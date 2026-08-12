#include <shmchan/channel.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

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
    return "shmchan-test-" + std::to_string(static_cast<long long>(::getpid())) + "-" +
           std::to_string(sequence.fetch_add(1, std::memory_order_relaxed)) + "-" +
           std::string(suffix);
}

template <typename T>
class unlink_guard {
public:
    explicit unlink_guard(std::string name) : name_(std::move(name)) {}
    ~unlink_guard() { (void)shmchan::channel<T>::unlink(name_); }

    unlink_guard(const unlink_guard&) = delete;
    unlink_guard& operator=(const unlink_guard&) = delete;

private:
    std::string name_;
};

void test_basic_and_capacity_one() {
    const auto name = unique_name("basic");
    unlink_guard<int> cleanup(name);
    auto producer = shmchan::channel<int>::create(name, 1);
    auto consumer = shmchan::channel<int>::open(name);

    CHECK(producer.capacity() == 1);
    CHECK(producer.name() == "/" + name);
    CHECK(consumer.try_receive().code == shmchan::channel_status::would_block);
    CHECK(producer.try_send(41) == shmchan::channel_status::success);
    CHECK(producer.try_send(42) == shmchan::channel_status::would_block);

    auto first = consumer.receive();
    CHECK(first.code == shmchan::channel_status::success);
    CHECK(first.value == 41);

    CHECK(producer.send(42) == shmchan::channel_status::success);
    CHECK(producer.close());
    CHECK(!consumer.close());
    CHECK(producer.try_send(43) == shmchan::channel_status::closed);

    auto buffered = consumer.receive();
    CHECK(buffered.code == shmchan::channel_status::success);
    CHECK(buffered.value == 42);
    CHECK(consumer.receive().code == shmchan::channel_status::closed);
}

void test_timeouts_and_wake_on_close() {
    const auto name = unique_name("timeouts");
    unlink_guard<int> cleanup(name);
    auto channel = shmchan::channel<int>::create(name, 2);

    CHECK(channel.receive_for(2ms).code == shmchan::channel_status::timed_out);
    CHECK(channel.send(1) == shmchan::channel_status::success);
    CHECK(channel.send(2) == shmchan::channel_status::success);
    CHECK(channel.send_for(3, 2ms) == shmchan::channel_status::timed_out);

    CHECK(channel.receive().value == 1);
    CHECK(channel.receive().value == 2);

    std::atomic<shmchan::channel_status> result{shmchan::channel_status::success};
    std::thread waiter([&] { result.store(channel.receive().code, std::memory_order_release); });
    std::this_thread::sleep_for(5ms);
    CHECK(channel.close());
    waiter.join();
    CHECK(result.load(std::memory_order_acquire) == shmchan::channel_status::closed);
}

void test_close_wakes_blocked_sender() {
    const auto name = unique_name("sender-close");
    unlink_guard<int> cleanup(name);
    auto channel = shmchan::channel<int>::create(name, 1);
    CHECK(channel.send(1) == shmchan::channel_status::success);

    std::atomic<shmchan::channel_status> result{shmchan::channel_status::success};
    std::thread waiter([&] { result.store(channel.send(2), std::memory_order_release); });
    std::this_thread::sleep_for(5ms);
    CHECK(channel.close());
    waiter.join();

    CHECK(result.load(std::memory_order_acquire) == shmchan::channel_status::closed);
    CHECK(channel.receive().value == 1);
    CHECK(channel.receive().code == shmchan::channel_status::closed);
}

void test_type_validation_and_move() {
    const auto name = unique_name("type");
    unlink_guard<int> cleanup(name);
    auto original = shmchan::channel<int>::create(name, 4);
    auto moved = std::move(original);
    CHECK(!original.valid());
    CHECK(moved.valid());

    bool mismatch_detected = false;
    try {
        auto wrong = shmchan::channel<float>::open(name);
        (void)wrong;
    } catch (const std::runtime_error&) {
        mismatch_detected = true;
    }
    CHECK(mismatch_detected);
}

void test_mpmc_threads() {
    constexpr std::size_t producer_count = 4;
    constexpr std::size_t consumer_count = 4;
    constexpr std::size_t messages_per_producer = 10'000;
    constexpr std::size_t total_messages = producer_count * messages_per_producer;

    const auto name = unique_name("mpmc");
    unlink_guard<std::uint64_t> cleanup(name);
    auto channel = shmchan::channel<std::uint64_t>::create(name, 256);

    std::vector<std::atomic<unsigned char>> seen(total_messages);
    for (auto& count : seen) {
        count.store(0, std::memory_order_relaxed);
    }
    std::atomic<std::size_t> received{0};
    std::atomic<bool> failed{false};

    std::vector<std::thread> consumers;
    for (std::size_t index = 0; index < consumer_count; ++index) {
        consumers.emplace_back([&] {
            for (;;) {
                auto result = channel.receive();
                if (result.code == shmchan::channel_status::closed) {
                    return;
                }
                if (!result || *result >= total_messages) {
                    failed.store(true, std::memory_order_relaxed);
                    continue;
                }
                seen[static_cast<std::size_t>(*result)].fetch_add(1, std::memory_order_relaxed);
                received.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    std::vector<std::thread> producers;
    for (std::size_t producer = 0; producer < producer_count; ++producer) {
        producers.emplace_back([&, producer] {
            const std::size_t begin = producer * messages_per_producer;
            const std::size_t end = begin + messages_per_producer;
            for (std::size_t value = begin; value < end; ++value) {
                if (channel.send(static_cast<std::uint64_t>(value)) !=
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
    CHECK(received.load(std::memory_order_relaxed) == total_messages);
    for (const auto& count : seen) {
        CHECK(count.load(std::memory_order_relaxed) == 1);
    }
}

struct process_message {
    std::uint64_t sequence{};
    std::uint64_t checksum{};
};

void test_cross_process() {
    constexpr std::uint64_t message_count = 2'000;
    constexpr std::uint64_t checksum_mask = 0xa5a5a5a5a5a5a5a5ULL;

    const auto name = unique_name("process");
    unlink_guard<process_message> cleanup(name);
    auto producer = shmchan::channel<process_message>::create(name, 64);

    const pid_t child = ::fork();
    if (child < 0) {
        throw std::system_error(errno, std::generic_category(), "fork");
    }
    if (child == 0) {
        try {
            auto consumer = shmchan::channel<process_message>::open(name);
            for (std::uint64_t expected = 0; expected < message_count; ++expected) {
                const auto result = consumer.receive();
                if (!result || result->sequence != expected ||
                    result->checksum != (expected ^ checksum_mask)) {
                    ::_exit(10);
                }
            }
            if (consumer.receive().code != shmchan::channel_status::closed) {
                ::_exit(11);
            }
            ::_exit(0);
        } catch (...) {
            ::_exit(12);
        }
    }

    for (std::uint64_t sequence = 0; sequence < message_count; ++sequence) {
        const process_message value{
            .sequence = sequence,
            .checksum = sequence ^ checksum_mask,
        };
        CHECK(producer.send(value) == shmchan::channel_status::success);
    }
    CHECK(producer.close());

    int status = 0;
    CHECK(::waitpid(child, &status, 0) == child);
    CHECK(WIFEXITED(status));
    CHECK(WEXITSTATUS(status) == 0);
}

} // namespace

int main() {
    try {
        test_basic_and_capacity_one();
        test_timeouts_and_wake_on_close();
        test_close_wakes_blocked_sender();
        test_type_validation_and_move();
        test_mpmc_threads();
        test_cross_process();
        std::cout << "all shmchan tests passed\n";
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "test failure: " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
