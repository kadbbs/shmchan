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
#include <mutex>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unordered_set>
#include <vector>

#include <cerrno>
#include <fcntl.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {

using namespace std::chrono_literals;

#define CHECK(condition)                                                                     \
    do {                                                                                     \
        if (!(condition)) {                                                                  \
            throw std::runtime_error(std::string("check failed: ") + #condition + " at " + \
                                     __FILE__ + ":" + std::to_string(__LINE__));             \
        }                                                                                    \
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
    return options;
}

[[nodiscard]] std::string as_string(std::span<const std::byte> bytes) {
    return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

[[nodiscard]] std::string as_string(
    const shmchan::managed_byte_channel::buffer_type& bytes) {
    return as_string(std::span<const std::byte>{bytes.data(), bytes.size()});
}

void write_ready_byte(int descriptor) {
    const char value = '1';
    ssize_t result = 0;
    do {
        result = ::write(descriptor, std::addressof(value), sizeof(value));
    } while (result < 0 && errno == EINTR);
    if (result != static_cast<ssize_t>(sizeof(value))) {
        throw std::system_error(errno, std::generic_category(), "write readiness byte");
    }
}

void read_ready_byte(int descriptor) {
    char value = 0;
    ssize_t result = 0;
    do {
        result = ::read(descriptor, std::addressof(value), sizeof(value));
    } while (result < 0 && errno == EINTR);
    if (result != static_cast<ssize_t>(sizeof(value)) || value != '1') {
        throw std::runtime_error("failed to read child readiness byte");
    }
}

void check_child_killed(pid_t child, int signal_number) {
    int status = 0;
    CHECK(::waitpid(child, std::addressof(status), 0) == child);
    CHECK(WIFSIGNALED(status));
    CHECK(WTERMSIG(status) == signal_number);
}

void check_child_exited(pid_t child, int expected) {
    int status = 0;
    CHECK(::waitpid(child, std::addressof(status), 0) == child);
    CHECK(WIFEXITED(status));
    CHECK(WEXITSTATUS(status) == expected);
}

void test_basic_variable_messages_and_close() {
    const auto name = unique_name("basic");
    unlink_guard cleanup(name);
    auto options = test_options();
    options.message_capacity = 3;
    options.max_message_size = 32;
    auto channel = shmchan::managed_byte_channel::create(name, options);
    auto peer = shmchan::managed_byte_channel::open(name);

    CHECK(channel.name() == "/" + name);
    CHECK(channel.capacity() == 3);
    CHECK(channel.max_message_size() == 32);
    CHECK(channel.state() == shmchan::managed_channel_state::healthy);
    CHECK(peer.try_receive().code == shmchan::channel_status::would_block);
    CHECK(channel.try_send(std::string(33, 'x')) ==
          shmchan::channel_status::message_too_large);

    CHECK(channel.send("a") == shmchan::channel_status::success);
    CHECK(channel.send("a much longer message") ==
          shmchan::channel_status::success);
    CHECK(channel.send(std::string_view{}) == shmchan::channel_status::success);
    CHECK(channel.try_send("full") == shmchan::channel_status::would_block);

    auto first = peer.receive_for(1s);
    CHECK(first);
    CHECK(as_string(*first) == "a");

    auto second = peer.receive_for(1s);
    CHECK(second);
    CHECK(as_string(*second) == "a much longer message");

    auto third = peer.receive_for(1s);
    CHECK(third);
    CHECK(third->empty());

    const auto stats = channel.stats();
    CHECK(stats.sent_messages == 3);
    CHECK(stats.received_messages == 3);
    CHECK(stats.free_slots == 3);
    CHECK(stats.ready_messages == 0);

    CHECK(channel.send("drain-before-close") ==
          shmchan::channel_status::success);
    CHECK(channel.close());
    CHECK(!peer.close());
    CHECK(channel.send("closed") == shmchan::channel_status::closed);
    auto draining = peer.receive_for(1s);
    CHECK(draining);
    CHECK(as_string(*draining) == "drain-before-close");
    CHECK(peer.receive_for(100ms).code == shmchan::channel_status::closed);
}

void test_timeouts_and_slot_reuse() {
    const auto name = unique_name("timeouts");
    unlink_guard cleanup(name);
    auto options = test_options();
    options.message_capacity = 2;
    auto channel = shmchan::managed_byte_channel::create(name, options);

    CHECK(channel.receive_for(20ms).code == shmchan::channel_status::timed_out);
    CHECK(channel.send("one") == shmchan::channel_status::success);
    CHECK(channel.send("two") == shmchan::channel_status::success);
    CHECK(channel.send_for("three", 20ms) ==
          shmchan::channel_status::timed_out);

    auto one = channel.receive_for(1s);
    CHECK(one && as_string(*one) == "one");
    CHECK(channel.send("slot-reused-after-receive") ==
          shmchan::channel_status::success);

    auto two = channel.receive_for(1s);
    auto reused = channel.receive_for(1s);
    CHECK(two && as_string(*two) == "two");
    CHECK(reused && as_string(*reused) == "slot-reused-after-receive");

    const auto stats = channel.stats();
    CHECK(stats.send_timeouts == 1);
    CHECK(stats.receive_timeouts == 1);
}

void test_protocol_and_open_or_create() {
    const auto name = unique_name("protocol");
    unlink_guard cleanup(name);
    auto options = test_options();
    options.protocol = {shmchan::protocol_id("example.document"), 7};
    auto first = shmchan::managed_byte_channel::open_or_create(name, options);
    auto second = shmchan::managed_byte_channel::open_or_create(name, options);
    CHECK(first.protocol() == options.protocol);
    CHECK(second.capacity() == options.message_capacity);

    bool mismatch_detected = false;
    try {
        shmchan::managed_open_options wrong;
        wrong.protocol = {shmchan::protocol_id("example.document"), 8};
        auto incompatible = shmchan::managed_byte_channel::open(name, wrong);
        (void)incompatible;
    } catch (const shmchan::managed_channel_error& error) {
        mismatch_detected =
            error.code() == shmchan::channel_status::protocol_mismatch;
    }
    CHECK(mismatch_detected);

    bool layout_mismatch_detected = false;
    try {
        auto different = options;
        different.message_capacity += 1;
        auto incompatible =
            shmchan::managed_byte_channel::open_or_create(name, different);
        (void)incompatible;
    } catch (const std::invalid_argument&) {
        layout_mismatch_detected = true;
    }
    CHECK(layout_mismatch_detected);
}

void test_open_or_create_recovers_abandoned_initialization() {
    const auto name = unique_name("abandoned-init");
    unlink_guard cleanup(name);
    const auto object_name = shmchan::detail::managed_object_name(name);
    const int fd = ::shm_open(
        object_name.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    CHECK(fd >= 0);
    CHECK(::close(fd) == 0);

    auto channel =
        shmchan::managed_byte_channel::open_or_create(name, test_options());
    CHECK(channel.state() == shmchan::managed_channel_state::healthy);
    CHECK(channel.send("initialized") == shmchan::channel_status::success);
    auto received = channel.receive_for(1s);
    CHECK(received && as_string(*received) == "initialized");
}

void test_killed_incomplete_producer_is_recovered_in_place() {
    const auto name = unique_name("producer-crash");
    unlink_guard cleanup(name);
    auto options = test_options();
    options.message_capacity = 4;
    auto channel = shmchan::managed_byte_channel::create(name, options);
    CHECK(channel.send("published-before-crash") ==
          shmchan::channel_status::success);

    std::array<int, 2> ready_pipe{};
    CHECK(::pipe(ready_pipe.data()) == 0);
    const pid_t child = ::fork();
    if (child < 0) {
        throw std::system_error(errno, std::generic_category(), "fork producer crash");
    }
    if (child == 0) {
        (void)::close(ready_pipe[0]);
        try {
            auto mapping = shmchan::detail::open_managed_mapping(
                name, options.protocol);
            if (::pthread_mutex_lock(std::addressof(mapping->header->mutex)) != 0) {
                ::_exit(20);
            }
            auto& slot = mapping->slots[1];
            slot.state.store(
                static_cast<std::uint32_t>(
                    shmchan::detail::managed_slot_state::writing),
                std::memory_order_release);
            --mapping->header->free_slots;
            ++mapping->header->writing_messages;
            slot.sequence = mapping->header->next_sequence++;
            const std::string partial = "only-half-written";
            std::memcpy(
                mapping->payload(1).data(), partial.data(), partial.size() / 2);
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

    // The next normal operation repairs the abandoned WRITING slot in place.
    CHECK(channel.send("published-after-crash") ==
          shmchan::channel_status::success);
    CHECK(channel.state() == shmchan::managed_channel_state::healthy);

    auto before = channel.receive_for(1s);
    auto after = channel.receive_for(1s);
    CHECK(before && as_string(*before) == "published-before-crash");
    CHECK(after && as_string(*after) == "published-after-crash");
    CHECK(channel.try_receive().code == shmchan::channel_status::would_block);

    const auto stats = channel.stats();
    CHECK(stats.owner_death_recoveries >= 1);
    CHECK(stats.discarded_incomplete_writes == 1);
    CHECK(stats.free_slots == 4);
}

void test_killed_consumer_during_copy_keeps_ready_message() {
    const auto name = unique_name("consumer-crash");
    unlink_guard cleanup(name);
    auto options = test_options();
    options.message_capacity = 2;
    auto channel = shmchan::managed_byte_channel::create(name, options);
    CHECK(channel.send("must-survive-incomplete-receive") ==
          shmchan::channel_status::success);

    std::array<int, 2> ready_pipe{};
    CHECK(::pipe(ready_pipe.data()) == 0);
    const pid_t child = ::fork();
    if (child < 0) {
        throw std::system_error(errno, std::generic_category(), "fork consumer crash");
    }
    if (child == 0) {
        (void)::close(ready_pipe[0]);
        try {
            auto mapping = shmchan::detail::open_managed_mapping(
                name, options.protocol);
            if (::pthread_mutex_lock(std::addressof(mapping->header->mutex)) != 0) {
                ::_exit(30);
            }
            // receive() intentionally keeps the slot READY while copying. This models
            // death after a partial copy but before the slot is released.
            volatile std::byte copied = mapping->payload(0)[0];
            (void)copied;
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

    auto message = channel.receive_for(1s);
    CHECK(message);
    CHECK(as_string(*message) == "must-survive-incomplete-receive");
    CHECK(channel.state() == shmchan::managed_channel_state::healthy);
    CHECK(channel.stats().owner_death_recoveries >= 1);
}

void test_blocked_receiver_recovers_publish_without_wake() {
    const auto name = unique_name("publish-without-wake");
    unlink_guard cleanup(name);
    auto options = test_options();
    options.message_capacity = 2;
    auto channel = shmchan::managed_byte_channel::create(name, options);

    std::array<int, 2> child_ready{};
    std::array<int, 2> start_publish{};
    CHECK(::pipe(child_ready.data()) == 0);
    CHECK(::pipe(start_publish.data()) == 0);
    const pid_t child = ::fork();
    if (child < 0) {
        throw std::system_error(errno, std::generic_category(), "fork publish crash");
    }
    if (child == 0) {
        (void)::close(child_ready[0]);
        (void)::close(start_publish[1]);
        try {
            auto mapping = shmchan::detail::open_managed_mapping(
                name, options.protocol);
            write_ready_byte(child_ready[1]);
            read_ready_byte(start_publish[0]);
            if (::pthread_mutex_lock(std::addressof(mapping->header->mutex)) != 0) {
                ::_exit(35);
            }
            auto& slot = mapping->slots[0];
            slot.state.store(
                static_cast<std::uint32_t>(
                    shmchan::detail::managed_slot_state::writing),
                std::memory_order_release);
            --mapping->header->free_slots;
            ++mapping->header->writing_messages;
            slot.sequence = mapping->header->next_sequence++;
            const std::string payload = "published-before-owner-death";
            std::memcpy(
                mapping->payload(0).data(), payload.data(), payload.size());
            slot.payload_size = static_cast<std::uint32_t>(payload.size());
            slot.state.store(
                static_cast<std::uint32_t>(
                    shmchan::detail::managed_slot_state::ready),
                std::memory_order_release);
            // Die before repairing counts, unlocking, or incrementing event_epoch.
            (void)::kill(::getpid(), SIGKILL);
            ::_exit(36);
        } catch (...) {
            ::_exit(37);
        }
    }

    (void)::close(child_ready[1]);
    (void)::close(start_publish[0]);
    read_ready_byte(child_ready[0]);
    (void)::close(child_ready[0]);

    shmchan::receive_result<shmchan::managed_byte_channel::buffer_type> result;
    std::thread receiver([&] { result = channel.receive_for(2s); });
    std::this_thread::sleep_for(100ms);
    write_ready_byte(start_publish[1]);
    (void)::close(start_publish[1]);

    receiver.join();
    check_child_killed(child, SIGKILL);
    CHECK(result);
    CHECK(as_string(*result) == "published-before-owner-death");
    CHECK(channel.state() == shmchan::managed_channel_state::healthy);
}

void test_unrelated_process_death_does_not_affect_channel() {
    const auto name = unique_name("idle-crash");
    unlink_guard cleanup(name);
    auto channel =
        shmchan::managed_byte_channel::create(name, test_options());
    CHECK(channel.send("queued") == shmchan::channel_status::success);

    std::array<int, 2> ready_pipe{};
    CHECK(::pipe(ready_pipe.data()) == 0);
    const pid_t child = ::fork();
    if (child < 0) {
        throw std::system_error(errno, std::generic_category(), "fork idle crash");
    }
    if (child == 0) {
        (void)::close(ready_pipe[0]);
        try {
            auto peer = shmchan::managed_byte_channel::open(name);
            (void)peer;
            write_ready_byte(ready_pipe[1]);
            (void)::kill(::getpid(), SIGKILL);
            ::_exit(40);
        } catch (...) {
            ::_exit(41);
        }
    }

    (void)::close(ready_pipe[1]);
    read_ready_byte(ready_pipe[0]);
    (void)::close(ready_pipe[0]);
    check_child_killed(child, SIGKILL);

    CHECK(channel.state() == shmchan::managed_channel_state::healthy);
    auto queued = channel.receive_for(1s);
    CHECK(queued && as_string(*queued) == "queued");
    CHECK(channel.send("still-works") == shmchan::channel_status::success);
    auto next = channel.receive_for(1s);
    CHECK(next && as_string(*next) == "still-works");
}

void test_stopped_mutex_owner_times_out_without_destroying_channel() {
    const auto name = unique_name("stopped-owner");
    unlink_guard cleanup(name);
    auto options = test_options();
    auto channel = shmchan::managed_byte_channel::create(name, options);
    CHECK(channel.send("already-ready") == shmchan::channel_status::success);

    std::array<int, 2> ready_pipe{};
    CHECK(::pipe(ready_pipe.data()) == 0);
    const pid_t child = ::fork();
    if (child < 0) {
        throw std::system_error(errno, std::generic_category(), "fork stopped owner");
    }
    child_guard cleanup_child(child);
    if (child == 0) {
        (void)::close(ready_pipe[0]);
        try {
            auto mapping = shmchan::detail::open_managed_mapping(
                name, options.protocol);
            if (::pthread_mutex_lock(std::addressof(mapping->header->mutex)) != 0) {
                ::_exit(50);
            }
            write_ready_byte(ready_pipe[1]);
            (void)::raise(SIGSTOP);
            ::_exit(51);
        } catch (...) {
            ::_exit(52);
        }
    }

    (void)::close(ready_pipe[1]);
    read_ready_byte(ready_pipe[0]);
    (void)::close(ready_pipe[0]);
    int stopped_status = 0;
    CHECK(::waitpid(child, std::addressof(stopped_status), WUNTRACED) == child);
    CHECK(WIFSTOPPED(stopped_status));

    CHECK(channel.send_for("must-time-out", 40ms) ==
          shmchan::channel_status::timed_out);
    CHECK(channel.state() == shmchan::managed_channel_state::healthy);

    CHECK(::kill(child, SIGKILL) == 0);
    check_child_killed(child, SIGKILL);
    cleanup_child.release();

    CHECK(channel.send("after-owner-death") ==
          shmchan::channel_status::success);
    auto first = channel.receive_for(1s);
    auto second = channel.receive_for(1s);
    CHECK(first && as_string(*first) == "already-ready");
    CHECK(second && as_string(*second) == "after-owner-death");
    CHECK(channel.state() == shmchan::managed_channel_state::healthy);
}

void test_cross_process_send_receive() {
    const auto name = unique_name("cross-process");
    unlink_guard cleanup(name);
    auto channel =
        shmchan::managed_byte_channel::create(name, test_options());

    const pid_t child = ::fork();
    if (child < 0) {
        throw std::system_error(errno, std::generic_category(), "fork cross process");
    }
    if (child == 0) {
        try {
            auto producer = shmchan::managed_byte_channel::open(name);
            for (std::uint64_t id = 1; id <= 100; ++id) {
                const auto text = "child-" + std::to_string(id);
                if (producer.send_for(text, 2s) !=
                    shmchan::channel_status::success) {
                    ::_exit(60);
                }
            }
            ::_exit(0);
        } catch (...) {
            ::_exit(61);
        }
    }

    for (std::uint64_t id = 1; id <= 100; ++id) {
        auto message = channel.receive_for(2s);
        CHECK(message);
        CHECK(as_string(*message) == "child-" + std::to_string(id));
    }
    check_child_exited(child, 0);
}

void test_threaded_mpmc() {
    const auto name = unique_name("mpmc");
    unlink_guard cleanup(name);
    auto options = test_options();
    options.message_capacity = 64;
    options.max_message_size = 64;
    auto channel = shmchan::managed_byte_channel::create(name, options);

    constexpr std::size_t producer_count = 4;
    constexpr std::size_t consumer_count = 4;
    constexpr std::size_t messages_per_producer = 400;
    constexpr std::size_t total = producer_count * messages_per_producer;

    std::atomic<bool> failed{false};
    std::mutex received_mutex;
    std::unordered_set<std::string> received_messages;
    received_messages.reserve(total);

    std::vector<std::thread> consumers;
    for (std::size_t consumer = 0; consumer < consumer_count; ++consumer) {
        consumers.emplace_back([&] {
            for (;;) {
                auto result = channel.receive_for(1s);
                if (result) {
                    std::scoped_lock lock(received_mutex);
                    if (!received_messages.insert(as_string(*result)).second) {
                        failed.store(true, std::memory_order_release);
                    }
                    continue;
                }
                if (result.code == shmchan::channel_status::closed) {
                    return;
                }
                if (result.code != shmchan::channel_status::timed_out) {
                    failed.store(true, std::memory_order_release);
                    return;
                }
            }
        });
    }

    std::vector<std::thread> producers;
    for (std::size_t producer = 0; producer < producer_count; ++producer) {
        producers.emplace_back([&, producer] {
            for (std::size_t index = 0; index < messages_per_producer; ++index) {
                const auto id = static_cast<std::uint64_t>(
                    producer * messages_per_producer + index + 1);
                const auto payload = "message-" + std::to_string(id);
                if (channel.send_for(payload, 5s) !=
                    shmchan::channel_status::success) {
                    failed.store(true, std::memory_order_release);
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

    CHECK(!failed.load(std::memory_order_acquire));
    CHECK(received_messages.size() == total);
    const auto stats = channel.stats();
    CHECK(stats.sent_messages == total);
    CHECK(stats.received_messages == total);
    CHECK(stats.ready_messages == 0);
}

void test_unlink_wakes_waiters_and_fences_old_mapping() {
    const auto name = unique_name("unlink");
    auto channel =
        shmchan::managed_byte_channel::create(name, test_options());
    std::atomic<shmchan::channel_status> status{shmchan::channel_status::success};
    std::thread waiter([&] {
        status.store(channel.receive().code, std::memory_order_release);
    });
    std::this_thread::sleep_for(20ms);
    CHECK(shmchan::managed_byte_channel::unlink(name));
    waiter.join();
    CHECK(status.load(std::memory_order_acquire) == shmchan::channel_status::broken);
    CHECK(channel.try_send("old-mapping") == shmchan::channel_status::broken);
    CHECK(!shmchan::managed_byte_channel::unlink(name));

    auto replacement =
        shmchan::managed_byte_channel::open_or_create(name, test_options());
    unlink_guard cleanup(name);
    CHECK(replacement.send("new-object") == shmchan::channel_status::success);
    auto message = replacement.receive_for(1s);
    CHECK(message && as_string(*message) == "new-object");
}

} // namespace

int main() {
    try {
        test_basic_variable_messages_and_close();
        test_timeouts_and_slot_reuse();
        test_protocol_and_open_or_create();
        test_open_or_create_recovers_abandoned_initialization();
        test_killed_incomplete_producer_is_recovered_in_place();
        test_killed_consumer_during_copy_keeps_ready_message();
        test_blocked_receiver_recovers_publish_without_wake();
        test_unrelated_process_death_does_not_affect_channel();
        test_stopped_mutex_owner_times_out_without_destroying_channel();
        test_cross_process_send_receive();
        test_threaded_mpmc();
        test_unlink_wakes_waiters_and_fences_old_mapping();
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
    return 0;
}
