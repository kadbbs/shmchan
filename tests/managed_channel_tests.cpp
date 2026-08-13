#include <shmchan/managed_channel.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <iostream>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

#include <cerrno>
#include <pthread.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {

using namespace std::chrono_literals;

#define CHECK(condition)                                                                         \
    do {                                                                                         \
        if (!(condition)) {                                                                      \
            throw std::runtime_error(std::string("check failed: ") + #condition + " at " +     \
                                     __FILE__ + ":" + std::to_string(__LINE__));                 \
        }                                                                                        \
    } while (false)

[[nodiscard]] std::string unique_name(std::string_view suffix) {
    static std::atomic<unsigned> sequence{0};
    return "shmchan-managed-test-" +
           std::to_string(static_cast<long long>(::getpid())) + "-" +
           std::to_string(sequence.fetch_add(1, std::memory_order_relaxed)) + "-" +
           std::string(suffix);
}

class unlink_guard {
public:
    explicit unlink_guard(std::string name) : name_(std::move(name)) {}
    ~unlink_guard() { (void)shmchan::managed_byte_channel::unlink(name_); }

    unlink_guard(const unlink_guard&) = delete;
    unlink_guard& operator=(const unlink_guard&) = delete;

private:
    std::string name_;
};

class child_guard {
public:
    explicit child_guard(pid_t child) noexcept : child_(child) {}
    ~child_guard() {
        if (child_ > 0) {
            (void)::kill(child_, SIGKILL);
            int status = 0;
            (void)::waitpid(child_, &status, 0);
        }
    }

    child_guard(const child_guard&) = delete;
    child_guard& operator=(const child_guard&) = delete;

    void release() noexcept { child_ = -1; }

private:
    pid_t child_;
};

[[nodiscard]] shmchan::managed_channel_options test_options() {
    shmchan::managed_channel_options options;
    options.message_capacity = 16;
    options.max_message_size = 256;
    options.heartbeat_interval = 10ms;
    options.participant_timeout = 250ms;
    options.reservation_timeout = 500ms;
    options.acknowledgment_timeout = 80ms;
    return options;
}

[[nodiscard]] shmchan::managed_open_options open_options(
    shmchan::protocol_descriptor protocol = {
        shmchan::protocol_id("shmchan.raw-bytes"), 1}) {
    shmchan::managed_open_options options;
    options.protocol = protocol;
    return options;
}

[[nodiscard]] std::string as_string(std::span<const std::byte> bytes) {
    return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

template <typename Predicate>
[[nodiscard]] bool wait_until(Predicate&& predicate, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(5ms);
    }
    return predicate();
}

void write_ready_byte(int descriptor) {
    const char value = '1';
    ssize_t result = 0;
    do {
        result = ::write(descriptor, &value, sizeof(value));
    } while (result < 0 && errno == EINTR);
    if (result != static_cast<ssize_t>(sizeof(value))) {
        throw std::system_error(errno, std::generic_category(), "write readiness byte");
    }
}

void read_ready_byte(int descriptor) {
    char value = 0;
    ssize_t result = 0;
    do {
        result = ::read(descriptor, &value, sizeof(value));
    } while (result < 0 && errno == EINTR);
    if (result != static_cast<ssize_t>(sizeof(value)) || value != '1') {
        throw std::runtime_error("failed to read child readiness byte");
    }
}

void write_status_code(int descriptor, shmchan::channel_status status) {
    const auto value = static_cast<std::uint32_t>(status);
    const auto* bytes = reinterpret_cast<const char*>(std::addressof(value));
    std::size_t written = 0;
    while (written < sizeof(value)) {
        const auto result = ::write(descriptor, bytes + written, sizeof(value) - written);
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            throw std::system_error(errno, std::generic_category(), "write status code");
        }
        written += static_cast<std::size_t>(result);
    }
}

[[nodiscard]] shmchan::channel_status read_status_code(int descriptor) {
    std::uint32_t value = 0;
    auto* bytes = reinterpret_cast<char*>(std::addressof(value));
    std::size_t received = 0;
    while (received < sizeof(value)) {
        const auto result = ::read(descriptor, bytes + received, sizeof(value) - received);
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            throw std::runtime_error("failed to read status code");
        }
        received += static_cast<std::size_t>(result);
    }
    return static_cast<shmchan::channel_status>(value);
}

void check_child_killed(pid_t child, int signal_number) {
    int status = 0;
    CHECK(::waitpid(child, &status, 0) == child);
    CHECK(WIFSIGNALED(status));
    CHECK(WTERMSIG(status) == signal_number);
}

void check_child_exited(pid_t child, int expected) {
    int status = 0;
    CHECK(::waitpid(child, &status, 0) == child);
    CHECK(WIFEXITED(status));
    CHECK(WEXITSTATUS(status) == expected);
}

void test_basic_ack_close_and_metrics() {
    const auto name = unique_name("basic");
    unlink_guard cleanup(name);
    auto options = test_options();
    options.message_capacity = 4;
    options.max_message_size = 32;
    options.role = shmchan::participant_role::producer;
    auto sender = shmchan::managed_byte_channel::create(name, options);

    auto receiver_options = open_options();
    receiver_options.role = shmchan::participant_role::consumer;
    auto receiver = shmchan::managed_byte_channel::open(name, receiver_options);

    CHECK(sender.name() == "/" + name);
    CHECK(sender.capacity() == 4);
    CHECK(sender.max_message_size() == 32);
    CHECK(sender.generation() == 1);
    CHECK(sender.state() == shmchan::managed_channel_state::healthy);
    CHECK(receiver.try_receive().code == shmchan::channel_status::would_block);
    CHECK(sender.try_send(std::string(33, 'x')) ==
          shmchan::channel_status::message_too_large);

    CHECK(sender.send("hello", 101) == shmchan::channel_status::success);
    CHECK(sender.try_send("duplicate", 101) ==
          shmchan::channel_status::duplicate_message);
    auto delivery = receiver.receive_for(1s);
    CHECK(delivery);
    CHECK(delivery->message_id() == 101);
    CHECK(delivery->generation() == 1);
    CHECK(delivery->attempt() == 1);
    CHECK(!delivery->redelivered());
    CHECK(!delivery->is_loaned());
    CHECK(as_string(delivery->bytes()) == "hello");

    const auto before_ack = sender.stats();
    CHECK(before_ack.sent_messages == 1);
    CHECK(before_ack.delivered_messages == 1);
    CHECK(before_ack.inflight_messages == 1);
    CHECK(before_ack.active_participants == 2);
    CHECK(delivery->ack() == shmchan::channel_status::success);
    CHECK(delivery->ack() == shmchan::channel_status::stale_delivery);

    const auto after_ack = sender.stats();
    CHECK(after_ack.acknowledged_messages == 1);
    CHECK(after_ack.free_slots == 4);
    const auto participants = sender.participants();
    CHECK(participants.size() == 2);
    CHECK(participants[0].observed_generation == 1);

    CHECK(sender.close());
    CHECK(!receiver.close());
    CHECK(sender.try_send("closed") == shmchan::channel_status::closed);
    CHECK(receiver.receive_for(100ms).code == shmchan::channel_status::closed);
}

void test_zero_copy_nack_timeout_and_reservation_cleanup() {
    const auto name = unique_name("zero-copy");
    unlink_guard cleanup(name);
    auto options = test_options();
    options.message_capacity = 3;
    options.max_message_size = 64;
    options.acknowledgment_timeout = 40ms;
    auto channel = shmchan::managed_byte_channel::create(name, options);

    auto reservation = channel.try_reserve(201);
    CHECK(reservation);
    CHECK(reservation->capacity() == 64);
    const std::string zero_copy_message = "written directly into shared memory";
    std::memcpy(
        reservation->buffer().data(), zero_copy_message.data(), zero_copy_message.size());
    CHECK(reservation->commit(zero_copy_message.size()) == shmchan::channel_status::success);

    auto first = channel.receive_loaned_for(1s);
    CHECK(first);
    CHECK(first->is_loaned());
    CHECK(as_string(first->bytes()) == zero_copy_message);
    CHECK(first->nack() == shmchan::channel_status::success);

    auto after_nack = channel.receive_loaned_for(1s);
    CHECK(after_nack);
    CHECK(after_nack->attempt() == 2);
    CHECK(after_nack->redelivered());
    CHECK(after_nack->ack() == shmchan::channel_status::success);

    CHECK(channel.send("lease", 202) == shmchan::channel_status::success);
    {
        auto unacknowledged = channel.receive_for(1s);
        CHECK(unacknowledged);
        CHECK(unacknowledged->attempt() == 1);
    }
    auto after_timeout = channel.receive_for(1s);
    CHECK(after_timeout);
    CHECK(after_timeout->message_id() == 202);
    CHECK(after_timeout->attempt() == 2);
    CHECK(after_timeout->ack() == shmchan::channel_status::success);

    {
        auto cancelled = channel.try_reserve(203);
        CHECK(cancelled);
    }
    auto stats = channel.stats();
    CHECK(stats.negatively_acknowledged_messages == 1);
    CHECK(stats.redelivered_messages >= 1);
    CHECK(stats.cancelled_reservations == 1);
    CHECK(stats.free_slots == 3);

    const std::array<std::string, 2> batch_payloads{"batch-a", "batch-b"};
    const std::array<shmchan::outbound_message, 2> batch{{
        {
            std::as_bytes(std::span<const char>{
                batch_payloads[0].data(), batch_payloads[0].size()}),
            205,
        },
        {
            std::as_bytes(std::span<const char>{
                batch_payloads[1].data(), batch_payloads[1].size()}),
            206,
        },
    }};
    const auto batch_result = channel.try_send_batch(batch);
    CHECK(batch_result.code == shmchan::channel_status::success);
    CHECK(batch_result.sent == 2);
    auto deliveries = channel.try_receive_batch(2);
    CHECK(deliveries.size() == 2);
    CHECK(as_string(deliveries[0].bytes()) == "batch-a");
    CHECK(as_string(deliveries[1].bytes()) == "batch-b");
    CHECK(deliveries[0].ack() == shmchan::channel_status::success);
    CHECK(deliveries[1].ack() == shmchan::channel_status::success);

    CHECK(channel.send(std::string_view{}, 207) == shmchan::channel_status::success);
    auto empty = channel.receive_for(1s);
    CHECK(empty);
    CHECK(empty->bytes().empty());
    CHECK(empty->ack() == shmchan::channel_status::success);

    auto pending = channel.try_reserve(204);
    CHECK(pending);
    CHECK(channel.close());
    CHECK(pending->commit(0) == shmchan::channel_status::closed);
    CHECK(channel.receive_for(200ms).code == shmchan::channel_status::closed);
    CHECK(channel.stats().cancelled_reservations == 2);
}

void test_protocol_and_participant_validation() {
    const auto name = unique_name("protocol");
    unlink_guard cleanup(name);
    auto options = test_options();
    options.protocol = {shmchan::protocol_id("example.document"), 7};
    options.max_participants = 1;
    auto only = shmchan::managed_byte_channel::create(name, options);

    bool participant_limit_detected = false;
    try {
        auto extra = shmchan::managed_byte_channel::open(name, open_options(options.protocol));
        (void)extra;
    } catch (const shmchan::managed_channel_error& error) {
        participant_limit_detected = error.code() == shmchan::channel_status::participant_limit;
    }
    CHECK(participant_limit_detected);

    bool protocol_mismatch_detected = false;
    try {
        const shmchan::protocol_descriptor wrong{
            shmchan::protocol_id("example.document"), 8};
        auto incompatible = shmchan::managed_byte_channel::open(name, open_options(wrong));
        (void)incompatible;
    } catch (const shmchan::managed_channel_error& error) {
        protocol_mismatch_detected =
            error.code() == shmchan::channel_status::protocol_mismatch;
    }
    CHECK(protocol_mismatch_detected);

    only = shmchan::managed_byte_channel{};
    auto reopened = shmchan::managed_byte_channel::open(name, open_options(options.protocol));
    CHECK(reopened.protocol() == options.protocol);
}

void test_unlink_fences_live_handles_and_wakes_waiters() {
    const auto name = unique_name("unlink");
    unlink_guard cleanup(name);
    auto channel = shmchan::managed_byte_channel::create(name, test_options());
    std::atomic<shmchan::channel_status> receive_status{shmchan::channel_status::success};
    std::thread waiter([&] {
        receive_status.store(channel.receive().code, std::memory_order_release);
    });
    std::this_thread::sleep_for(20ms);
    CHECK(shmchan::managed_byte_channel::unlink(name));
    waiter.join();
    CHECK(receive_status.load(std::memory_order_acquire) ==
          shmchan::channel_status::broken);
    CHECK(channel.try_send("after-unlink") == shmchan::channel_status::broken);
    CHECK(!shmchan::managed_byte_channel::unlink(name));
}

void test_generation_rebuild_and_replay() {
    const auto name = unique_name("generation");
    unlink_guard cleanup(name);
    auto options = test_options();
    options.message_capacity = 4;
    auto supervisor = shmchan::managed_byte_channel::create(name, options);
    auto peer = shmchan::managed_byte_channel::open(name);

    CHECK(supervisor.send("lost-old-generation", 301) ==
          shmchan::channel_status::success);
    auto old_reservation = peer.try_reserve(302);
    CHECK(old_reservation);
    CHECK(supervisor.mark_broken());
    CHECK(peer.try_send("blocked") == shmchan::channel_status::broken);

    const auto rebuilt = supervisor.rebuild_with_replay(
        [&](shmchan::managed_byte_channel& channel, std::uint64_t old_generation,
            std::uint64_t new_generation) {
            CHECK(old_generation == 1);
            CHECK(new_generation == 2);
            CHECK(channel.state() == shmchan::managed_channel_state::replaying);
            CHECK(peer.try_send("external-producer-is-gated", 999) ==
                  shmchan::channel_status::recovery_in_progress);
            CHECK(channel.send("from-upstream-outbox", 301) ==
                  shmchan::channel_status::success);
            auto during_replay = peer.receive_for(1s);
            CHECK(during_replay);
            CHECK(during_replay->message_id() == 301);
            CHECK(during_replay->ack() == shmchan::channel_status::success);
            CHECK(channel.send("after-replay-drain", 303) ==
                  shmchan::channel_status::success);
        });
    CHECK(rebuilt.code == shmchan::channel_status::success);
    CHECK(rebuilt.previous_generation == 1);
    CHECK(rebuilt.generation == 2);
    CHECK(peer.generation() == 2);
    CHECK(old_reservation->commit(0) == shmchan::channel_status::generation_changed);

    auto replayed = peer.receive_for(1s);
    CHECK(replayed);
    CHECK(replayed->generation() == 2);
    CHECK(replayed->message_id() == 303);
    CHECK(as_string(replayed->bytes()) == "after-replay-drain");
    CHECK(replayed->ack() == shmchan::channel_status::success);
    CHECK(supervisor.stats().rebuilt_generations == 1);

    CHECK(supervisor.mark_broken());
    bool replay_failed = false;
    try {
        (void)supervisor.rebuild_with_replay(
            [](shmchan::managed_byte_channel&, std::uint64_t, std::uint64_t) {
                throw std::runtime_error("synthetic replay failure");
            });
    } catch (const std::runtime_error&) {
        replay_failed = true;
    }
    CHECK(replay_failed);
    CHECK(supervisor.state() == shmchan::managed_channel_state::broken);
    CHECK(supervisor.reason() == shmchan::break_reason::replay_failed);
    CHECK(supervisor.stats().replay_failures == 1);
    const auto recovered = supervisor.rebuild();
    CHECK(recovered.code == shmchan::channel_status::success);
    CHECK(recovered.generation == 4);
}

void test_reservation_timeout_breaks_generation() {
    const auto name = unique_name("reservation-timeout");
    unlink_guard cleanup(name);
    auto options = test_options();
    options.reservation_timeout = 40ms;
    options.participant_timeout = 500ms;
    auto channel = shmchan::managed_byte_channel::create(name, options);
    auto reservation = channel.try_reserve(401);
    CHECK(reservation);

    CHECK(wait_until(
        [&] {
            (void)channel.supervise_once();
            return channel.state() == shmchan::managed_channel_state::broken;
        },
        1s));
    CHECK(channel.reason() == shmchan::break_reason::reservation_timeout);
    CHECK(channel.rebuild().code == shmchan::channel_status::success);
    CHECK(reservation->commit(0) == shmchan::channel_status::generation_changed);
}

void test_kill9_participant_and_upstream_replay() {
    const auto name = unique_name("kill9-participant");
    unlink_guard cleanup(name);
    auto options = test_options();
    options.message_capacity = 4;
    options.participant_timeout = 180ms;
    options.reservation_timeout = 2s;
    {
        auto creator = shmchan::managed_byte_channel::create(name, options);
    }

    std::array<int, 2> ready_pipe{};
    CHECK(::pipe(ready_pipe.data()) == 0);
    const pid_t child = ::fork();
    if (child < 0) {
        throw std::system_error(errno, std::generic_category(), "fork participant crash");
    }
    if (child == 0) {
        (void)::close(ready_pipe[0]);
        try {
            auto child_options = open_options();
            child_options.role = shmchan::participant_role::producer;
            child_options.monitor_peers = false;
            auto producer = shmchan::managed_byte_channel::open(name, child_options);
            auto reservation = producer.try_reserve(501);
            if (!reservation) {
                ::_exit(20);
            }
            const std::string payload = "unpublished-before-sigkill";
            std::memcpy(reservation->buffer().data(), payload.data(), payload.size());
            write_ready_byte(ready_pipe[1]);
            (void)::kill(::getpid(), SIGKILL);
            ::_exit(21);
        } catch (...) {
            ::_exit(22);
        }
    }
    (void)::close(ready_pipe[1]);
    read_ready_byte(ready_pipe[0]);
    (void)::close(ready_pipe[0]);
    check_child_killed(child, SIGKILL);

    auto supervisor_options = open_options();
    supervisor_options.role = shmchan::participant_role::supervisor;
    supervisor_options.monitor_peers = false;
    auto supervisor = shmchan::managed_byte_channel::open(name, supervisor_options);
    CHECK(wait_until(
        [&] {
            (void)supervisor.supervise_once();
            return supervisor.state() == shmchan::managed_channel_state::broken;
        },
        2s));
    CHECK(supervisor.reason() == shmchan::break_reason::participant_timeout);
    CHECK(supervisor.stats().failed_participant_session != 0);

    const auto rebuilt = supervisor.rebuild_with_replay(
        [](shmchan::managed_byte_channel& channel, std::uint64_t, std::uint64_t) {
            CHECK(channel.send("replayed-from-durable-outbox", 501) ==
                  shmchan::channel_status::success);
        });
    CHECK(rebuilt.code == shmchan::channel_status::success);
    CHECK(rebuilt.generation == 2);
    auto delivery = supervisor.receive_for(1s);
    CHECK(delivery);
    CHECK(delivery->message_id() == 501);
    CHECK(as_string(delivery->bytes()) == "replayed-from-durable-outbox");
    CHECK(delivery->ack() == shmchan::channel_status::success);
    CHECK(supervisor.stats().stale_participants == 0);
}

void test_kill9_during_replay_is_recoverable() {
    const auto name = unique_name("kill9-replay");
    unlink_guard cleanup(name);
    auto options = test_options();
    options.participant_timeout = 180ms;
    {
        auto creator = shmchan::managed_byte_channel::create(name, options);
        CHECK(creator.mark_broken());
    }

    std::array<int, 2> ready_pipe{};
    CHECK(::pipe(ready_pipe.data()) == 0);
    const pid_t child = ::fork();
    if (child < 0) {
        throw std::system_error(errno, std::generic_category(), "fork replay crash");
    }
    if (child == 0) {
        (void)::close(ready_pipe[0]);
        try {
            auto child_options = open_options();
            child_options.monitor_peers = false;
            child_options.role = shmchan::participant_role::supervisor;
            auto rebuilder = shmchan::managed_byte_channel::open(name, child_options);
            (void)rebuilder.rebuild_with_replay(
                [&](shmchan::managed_byte_channel& channel, std::uint64_t, std::uint64_t) {
                    if (channel.send("partial-replay", 551) !=
                        shmchan::channel_status::success) {
                        ::_exit(70);
                    }
                    write_ready_byte(ready_pipe[1]);
                    (void)::kill(::getpid(), SIGKILL);
                    ::_exit(71);
                });
            ::_exit(72);
        } catch (...) {
            ::_exit(73);
        }
    }
    child_guard replay_child_cleanup(child);

    (void)::close(ready_pipe[1]);
    read_ready_byte(ready_pipe[0]);
    (void)::close(ready_pipe[0]);
    check_child_killed(child, SIGKILL);
    replay_child_cleanup.release();

    auto supervisor_options = open_options();
    supervisor_options.monitor_peers = false;
    supervisor_options.role = shmchan::participant_role::supervisor;
    auto supervisor = shmchan::managed_byte_channel::open(name, supervisor_options);
    CHECK(supervisor.state() == shmchan::managed_channel_state::replaying);
    CHECK(wait_until(
        [&] {
            (void)supervisor.supervise_once();
            return supervisor.state() == shmchan::managed_channel_state::broken;
        },
        2s));
    CHECK(supervisor.generation() == 2);
    const auto recovered = supervisor.rebuild();
    CHECK(recovered.code == shmchan::channel_status::success);
    CHECK(recovered.generation == 3);
    CHECK(supervisor.try_receive().code == shmchan::channel_status::would_block);
}

void test_kill9_while_holding_robust_mutex() {
    const auto name = unique_name("kill9-mutex");
    unlink_guard cleanup(name);
    auto options = test_options();
    options.message_capacity = 4;
    options.max_message_size = 64;
    {
        auto creator = shmchan::managed_byte_channel::create(name, options);
    }

    std::array<int, 2> ready_pipe{};
    CHECK(::pipe(ready_pipe.data()) == 0);
    const pid_t child = ::fork();
    if (child < 0) {
        throw std::system_error(errno, std::generic_category(), "fork mutex crash");
    }
    if (child == 0) {
        (void)::close(ready_pipe[0]);
        try {
            auto control = shmchan::detail::open_managed_control(name, options.protocol);
            auto data = shmchan::detail::open_managed_data(
                control->base_name,
                1,
                options.message_capacity,
                options.max_message_size,
                options.protocol);
            if (::pthread_mutex_lock(std::addressof(data->header->mutex)) != 0) {
                ::_exit(30);
            }
            write_ready_byte(ready_pipe[1]);
            (void)::kill(::getpid(), SIGKILL);
            ::_exit(31);
        } catch (...) {
            ::_exit(32);
        }
    }

    (void)::close(ready_pipe[1]);
    read_ready_byte(ready_pipe[0]);
    (void)::close(ready_pipe[0]);
    check_child_killed(child, SIGKILL);

    auto no_monitor = open_options();
    no_monitor.monitor_peers = false;
    auto channel = shmchan::managed_byte_channel::open(name, no_monitor);
    CHECK(channel.try_send("detect-owner-death", 601) == shmchan::channel_status::broken);
    CHECK(channel.state() == shmchan::managed_channel_state::broken);
    CHECK(channel.reason() == shmchan::break_reason::robust_mutex_owner_died);
    CHECK(channel.rebuild().code == shmchan::channel_status::success);
    CHECK(channel.send("after-rebuild", 602) == shmchan::channel_status::success);
    auto delivery = channel.receive_for(1s);
    CHECK(delivery);
    CHECK(delivery->ack() == shmchan::channel_status::success);
}

void test_stopped_mutex_owner_times_out() {
    const auto name = unique_name("stopped-mutex");
    unlink_guard cleanup(name);
    auto options = test_options();
    options.message_capacity = 4;
    options.max_message_size = 64;
    options.participant_timeout = 180ms;
    {
        auto creator = shmchan::managed_byte_channel::create(name, options);
    }

    std::array<int, 2> ready_pipe{};
    CHECK(::pipe(ready_pipe.data()) == 0);
    const pid_t child = ::fork();
    if (child < 0) {
        throw std::system_error(errno, std::generic_category(), "fork stopped mutex owner");
    }
    if (child == 0) {
        (void)::close(ready_pipe[0]);
        try {
            auto control = shmchan::detail::open_managed_control(name, options.protocol);
            auto data = shmchan::detail::open_managed_data(
                control->base_name,
                1,
                options.message_capacity,
                options.max_message_size,
                options.protocol);
            if (::pthread_mutex_lock(std::addressof(data->header->mutex)) != 0) {
                ::_exit(60);
            }
            write_ready_byte(ready_pipe[1]);
            (void)::raise(SIGSTOP);
            ::_exit(61);
        } catch (...) {
            ::_exit(62);
        }
    }
    child_guard stopped_owner_cleanup(child);

    (void)::close(ready_pipe[1]);
    read_ready_byte(ready_pipe[0]);
    (void)::close(ready_pipe[0]);
    int stopped_status = 0;
    CHECK(::waitpid(child, &stopped_status, WUNTRACED) == child);
    CHECK(WIFSTOPPED(stopped_status));

    auto no_monitor = open_options();
    no_monitor.monitor_peers = false;
    auto channel = shmchan::managed_byte_channel::open(name, no_monitor);
    const auto started = std::chrono::steady_clock::now();
    CHECK(channel.try_send("lock-must-time-out", 603) == shmchan::channel_status::broken);
    const auto elapsed = std::chrono::steady_clock::now() - started;
    CHECK(elapsed >= 100ms);
    CHECK(elapsed < 2s);
    CHECK(channel.reason() == shmchan::break_reason::mutex_lock_timeout);

    CHECK(::kill(child, SIGKILL) == 0);
    check_child_killed(child, SIGKILL);
    stopped_owner_cleanup.release();
    CHECK(channel.rebuild().code == shmchan::channel_status::success);
}

void test_stopped_participant_is_fenced_after_rebuild() {
    const auto name = unique_name("stopped-fencing");
    unlink_guard cleanup(name);
    auto options = test_options();
    options.participant_timeout = 180ms;
    {
        auto creator = shmchan::managed_byte_channel::create(name, options);
    }

    std::array<int, 2> ready_pipe{};
    std::array<int, 2> result_pipe{};
    CHECK(::pipe(ready_pipe.data()) == 0);
    CHECK(::pipe(result_pipe.data()) == 0);
    const pid_t child = ::fork();
    if (child < 0) {
        throw std::system_error(errno, std::generic_category(), "fork stopped participant");
    }
    if (child == 0) {
        (void)::close(ready_pipe[0]);
        (void)::close(result_pipe[0]);
        try {
            auto child_options = open_options();
            child_options.monitor_peers = false;
            auto participant = shmchan::managed_byte_channel::open(name, child_options);
            write_ready_byte(ready_pipe[1]);
            (void)::raise(SIGSTOP);
            write_status_code(result_pipe[1], participant.try_send("must-be-fenced"));
            participant = shmchan::managed_byte_channel{};
            ::_exit(0);
        } catch (...) {
            ::_exit(50);
        }
    }
    child_guard stopped_child_cleanup(child);

    (void)::close(ready_pipe[1]);
    (void)::close(result_pipe[1]);
    read_ready_byte(ready_pipe[0]);
    (void)::close(ready_pipe[0]);
    int stopped_status = 0;
    CHECK(::waitpid(child, &stopped_status, WUNTRACED) == child);
    CHECK(WIFSTOPPED(stopped_status));
    CHECK(WSTOPSIG(stopped_status) == SIGSTOP);

    auto supervisor_options = open_options();
    supervisor_options.monitor_peers = false;
    supervisor_options.role = shmchan::participant_role::supervisor;
    auto supervisor = shmchan::managed_byte_channel::open(name, supervisor_options);
    CHECK(wait_until(
        [&] {
            (void)supervisor.supervise_once();
            return supervisor.state() == shmchan::managed_channel_state::broken;
        },
        2s));
    CHECK(supervisor.rebuild().code == shmchan::channel_status::success);
    CHECK(::kill(child, SIGCONT) == 0);
    CHECK(read_status_code(result_pipe[0]) == shmchan::channel_status::participant_expired);
    (void)::close(result_pipe[0]);
    check_child_exited(child, 0);
    stopped_child_cleanup.release();
    CHECK(supervisor.state() == shmchan::managed_channel_state::healthy);
}

void test_cross_process_send_receive_ack() {
    const auto name = unique_name("cross-process");
    unlink_guard cleanup(name);
    const auto options = test_options();
    {
        auto creator = shmchan::managed_byte_channel::create(name, options);
    }

    std::array<int, 2> ready_pipe{};
    CHECK(::pipe(ready_pipe.data()) == 0);
    const pid_t child = ::fork();
    if (child < 0) {
        throw std::system_error(errno, std::generic_category(), "fork consumer");
    }
    if (child == 0) {
        (void)::close(ready_pipe[0]);
        int exit_code = 0;
        try {
            auto consumer_options = open_options();
            consumer_options.role = shmchan::participant_role::consumer;
            {
                auto consumer =
                    shmchan::managed_byte_channel::open(name, consumer_options);
                write_ready_byte(ready_pipe[1]);
                auto delivery = consumer.receive_for(2s);
                if (!delivery || delivery->message_id() != 701 ||
                    as_string(delivery->bytes()) != "cross-process" ||
                    delivery->ack() != shmchan::channel_status::success) {
                    exit_code = 40;
                } else if (consumer.receive_for(2s).code !=
                           shmchan::channel_status::closed) {
                    exit_code = 41;
                }
            }
        } catch (...) {
            exit_code = 42;
        }
        ::_exit(exit_code);
    }

    (void)::close(ready_pipe[1]);
    read_ready_byte(ready_pipe[0]);
    (void)::close(ready_pipe[0]);
    auto producer_options = open_options();
    producer_options.role = shmchan::participant_role::producer;
    auto producer = shmchan::managed_byte_channel::open(name, producer_options);
    CHECK(producer.send("cross-process", 701) == shmchan::channel_status::success);
    CHECK(producer.close());
    check_child_exited(child, 0);
    CHECK(producer.stats().acknowledged_messages == 1);
}

void test_mpmc_threads() {
    constexpr std::size_t producer_count = 4;
    constexpr std::size_t consumer_count = 4;
    constexpr std::size_t messages_per_producer = 500;
    constexpr std::size_t message_count = producer_count * messages_per_producer;

    const auto name = unique_name("mpmc");
    unlink_guard cleanup(name);
    auto options = test_options();
    options.message_capacity = 64;
    options.max_message_size = sizeof(std::uint64_t);
    options.acknowledgment_timeout = 2s;
    options.participant_timeout = 2s;
    auto channel = shmchan::managed_byte_channel::create(name, options);

    std::vector<std::atomic<unsigned char>> seen(message_count);
    for (auto& count : seen) {
        count.store(0, std::memory_order_relaxed);
    }
    std::atomic<std::size_t> received{0};
    std::atomic<bool> failed{false};

    std::vector<std::thread> consumers;
    for (std::size_t index = 0; index < consumer_count; ++index) {
        consumers.emplace_back([&] {
            for (;;) {
                auto delivery = channel.receive();
                if (delivery.code == shmchan::channel_status::closed) {
                    return;
                }
                if (!delivery || delivery->bytes().size() != sizeof(std::uint64_t)) {
                    failed.store(true, std::memory_order_relaxed);
                    return;
                }
                std::uint64_t value = 0;
                std::memcpy(&value, delivery->bytes().data(), sizeof(value));
                if (value >= message_count ||
                    delivery->ack() != shmchan::channel_status::success) {
                    failed.store(true, std::memory_order_relaxed);
                    return;
                }
                seen[static_cast<std::size_t>(value)].fetch_add(
                    1, std::memory_order_relaxed);
                received.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    std::vector<std::thread> producers;
    for (std::size_t producer = 0; producer < producer_count; ++producer) {
        producers.emplace_back([&, producer] {
            const auto begin = producer * messages_per_producer;
            const auto end = begin + messages_per_producer;
            for (std::size_t value = begin; value < end; ++value) {
                const auto encoded = static_cast<std::uint64_t>(value);
                const auto bytes = std::as_bytes(
                    std::span<const std::uint64_t>{std::addressof(encoded), 1});
                if (channel.send(bytes, static_cast<std::uint64_t>(value + 1)) !=
                    shmchan::channel_status::success) {
                    failed.store(true, std::memory_order_relaxed);
                    return;
                }
            }
        });
    }

    for (auto& producer : producers) {
        producer.join();
    }
    CHECK(channel.close());
    for (auto& consumer : consumers) {
        consumer.join();
    }

    CHECK(!failed.load(std::memory_order_relaxed));
    CHECK(received.load(std::memory_order_relaxed) == message_count);
    for (const auto& count : seen) {
        CHECK(count.load(std::memory_order_relaxed) == 1);
    }
    const auto stats = channel.stats();
    CHECK(stats.sent_messages == message_count);
    CHECK(stats.acknowledged_messages == message_count);
}

} // namespace

int main() {
    try {
        test_basic_ack_close_and_metrics();
        test_zero_copy_nack_timeout_and_reservation_cleanup();
        test_protocol_and_participant_validation();
        test_unlink_fences_live_handles_and_wakes_waiters();
        test_generation_rebuild_and_replay();
        test_reservation_timeout_breaks_generation();
        test_kill9_participant_and_upstream_replay();
        test_kill9_during_replay_is_recoverable();
        test_kill9_while_holding_robust_mutex();
        test_stopped_mutex_owner_times_out();
        test_stopped_participant_is_fenced_after_rebuild();
        test_cross_process_send_receive_ack();
        test_mpmc_threads();
        std::cout << "managed channel tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
