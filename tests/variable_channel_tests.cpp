#include <shmchan/shmchan.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
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
    return "shmchan-variable-test-" +
           std::to_string(static_cast<long long>(::getpid())) + "-" +
           std::to_string(sequence.fetch_add(1, std::memory_order_relaxed)) + "-" +
           std::string(suffix);
}

class unlink_guard {
public:
    explicit unlink_guard(std::string name) : name_(std::move(name)) {}
    ~unlink_guard() { (void)shmchan::byte_channel::unlink(name_); }

    unlink_guard(const unlink_guard&) = delete;
    unlink_guard& operator=(const unlink_guard&) = delete;

private:
    std::string name_;
};

[[nodiscard]] std::vector<std::byte> make_message(std::uint64_t id, std::size_t size) {
    CHECK(size >= sizeof(id));
    std::vector<std::byte> value(size);
    std::memcpy(value.data(), &id, sizeof(id));
    for (std::size_t index = sizeof(id); index < size; ++index) {
        value[index] = static_cast<std::byte>((id * 131U + index * 17U) & 0xffU);
    }
    return value;
}

[[nodiscard]] bool valid_message(std::span<const std::byte> value, std::uint64_t& id) {
    if (value.size() < sizeof(id)) {
        return false;
    }
    std::memcpy(&id, value.data(), sizeof(id));
    for (std::size_t index = sizeof(id); index < value.size(); ++index) {
        if (value[index] != static_cast<std::byte>((id * 131U + index * 17U) & 0xffU)) {
            return false;
        }
    }
    return true;
}

void test_basic_wrap_and_limits() {
    const auto name = unique_name("basic");
    unlink_guard cleanup(name);
    auto sender = shmchan::byte_channel::create(name, 128);
    auto receiver = shmchan::byte_channel::open(name);

    CHECK(sender.capacity_bytes() == 128);
    CHECK(sender.max_message_size() == 96);
    CHECK(sender.name() == "/" + name);
    CHECK(receiver.try_receive().code == shmchan::channel_status::would_block);

    const std::string first(40, 'a'); // 96-byte record at offset 0.
    CHECK(sender.send(first) == shmchan::channel_status::success);
    auto first_result = receiver.receive();
    CHECK(first_result.code == shmchan::channel_status::success);
    CHECK(std::string(reinterpret_cast<const char*>(first_result->data()), first_result->size()) ==
          first);

    const std::string wrapped(50, 'w'); // Header starts at offset 96; payload wraps to offset 0.
    CHECK(sender.send(wrapped) == shmchan::channel_status::success);
    auto wrapped_result = receiver.receive();
    CHECK(std::string(reinterpret_cast<const char*>(wrapped_result->data()),
                      wrapped_result->size()) == wrapped);

    const std::string maximum(sender.max_message_size(), 'm');
    CHECK(sender.send(maximum) == shmchan::channel_status::success);
    CHECK(sender.try_send(std::string_view{}) == shmchan::channel_status::would_block);
    auto maximum_result = receiver.receive();
    CHECK(maximum_result->size() == maximum.size());
    CHECK(std::all_of(maximum_result->begin(), maximum_result->end(), [](std::byte value) {
        return value == std::byte{'m'};
    }));

    CHECK(sender.send(std::string_view{}) == shmchan::channel_status::success);
    const auto empty = receiver.receive();
    CHECK(empty.code == shmchan::channel_status::success);
    CHECK(empty->empty());

    const std::string oversized(sender.max_message_size() + 1, 'x');
    CHECK(sender.try_send(oversized) == shmchan::channel_status::message_too_large);
    CHECK(sender.send_for(oversized, 1ms) == shmchan::channel_status::message_too_large);

    CHECK(sender.close());
    CHECK(receiver.receive().code == shmchan::channel_status::closed);
}

void test_timeouts_and_close_wake() {
    const auto name = unique_name("timeouts");
    unlink_guard cleanup(name);
    auto channel = shmchan::byte_channel::create(name, 128);

    CHECK(channel.receive_for(2ms).code == shmchan::channel_status::timed_out);
    const std::string maximum(channel.max_message_size(), 'f');
    CHECK(channel.send(maximum) == shmchan::channel_status::success);
    CHECK(channel.send_for("blocked", 2ms) == shmchan::channel_status::timed_out);

    std::atomic<shmchan::channel_status> result{shmchan::channel_status::success};
    std::thread blocked([&] {
        result.store(channel.send("still blocked"), std::memory_order_release);
    });
    std::this_thread::sleep_for(5ms);
    CHECK(channel.close());
    blocked.join();
    CHECK(result.load(std::memory_order_acquire) == shmchan::channel_status::closed);

    CHECK(channel.receive().code == shmchan::channel_status::success);
    CHECK(channel.receive().code == shmchan::channel_status::closed);
}

struct document {
    std::uint32_t id{};
    std::string text{};

    friend bool operator==(const document&, const document&) = default;
};

struct document_codec {
    [[nodiscard]] std::vector<std::byte> encode(const document& value) const {
        std::vector<std::byte> encoded(sizeof(value.id) + value.text.size());
        std::memcpy(encoded.data(), &value.id, sizeof(value.id));
        std::memcpy(encoded.data() + sizeof(value.id), value.text.data(), value.text.size());
        return encoded;
    }

    [[nodiscard]] document decode(std::span<const std::byte> encoded) const {
        if (encoded.size() < sizeof(std::uint32_t)) {
            throw std::runtime_error("bad document payload");
        }
        document value;
        std::memcpy(&value.id, encoded.data(), sizeof(value.id));
        value.text.assign(
            reinterpret_cast<const char*>(encoded.data() + sizeof(value.id)),
            encoded.size() - sizeof(value.id));
        return value;
    }
};

void test_string_and_custom_codec() {
    {
        const auto name = unique_name("string");
        unlink_guard cleanup(name);
        auto sender = shmchan::string_channel::create(name, 1024);
        auto receiver = shmchan::string_channel::open(name);

        const std::string expected{"embedded\0nul\0bytes", 18};
        CHECK(sender.send(expected) == shmchan::channel_status::success);
        CHECK(receiver.receive().value == expected);
        CHECK(sender.send("string literal") == shmchan::channel_status::success);
        CHECK(receiver.receive().value == "string literal");
        CHECK(sender.close());
        CHECK(receiver.receive().code == shmchan::channel_status::closed);
    }

    {
        const auto name = unique_name("codec");
        unlink_guard cleanup(name);
        using document_channel = shmchan::serialized_channel<document, document_codec>;
        auto sender = document_channel::create(name, 4096);
        auto receiver = document_channel::open(name);

        const document expected{.id = 42, .text = std::string(777, 'd')};
        CHECK(sender.send(expected) == shmchan::channel_status::success);
        CHECK(receiver.receive().value == expected);
        CHECK(sender.close());
        CHECK(receiver.receive().code == shmchan::channel_status::closed);
    }
}

void test_mpmc_threads() {
    constexpr std::size_t producer_count = 4;
    constexpr std::size_t consumer_count = 4;
    constexpr std::size_t messages_per_producer = 3'000;
    constexpr std::size_t total_messages = producer_count * messages_per_producer;

    const auto name = unique_name("mpmc");
    unlink_guard cleanup(name);
    auto channel = shmchan::byte_channel::create(name, 32 * 1024);

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
                std::uint64_t id = 0;
                if (!result || !valid_message(*result, id) || id >= total_messages) {
                    failed.store(true, std::memory_order_relaxed);
                    continue;
                }
                seen[static_cast<std::size_t>(id)].fetch_add(1, std::memory_order_relaxed);
                received.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    std::vector<std::thread> producers;
    for (std::size_t producer = 0; producer < producer_count; ++producer) {
        producers.emplace_back([&, producer] {
            const std::size_t begin = producer * messages_per_producer;
            const std::size_t end = begin + messages_per_producer;
            for (std::size_t id = begin; id < end; ++id) {
                const auto message = make_message(id, sizeof(std::uint64_t) + (id * 37U) % 1024U);
                if (channel.send(message) != shmchan::channel_status::success) {
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

void test_cross_process() {
    constexpr std::uint64_t message_count = 2'000;
    const auto name = unique_name("process");
    unlink_guard cleanup(name);
    auto sender = shmchan::byte_channel::create(name, 16 * 1024);

    const pid_t child = ::fork();
    if (child < 0) {
        throw std::system_error(errno, std::generic_category(), "fork");
    }
    if (child == 0) {
        try {
            auto receiver = shmchan::byte_channel::open(name);
            for (std::uint64_t expected = 0; expected < message_count; ++expected) {
                const auto result = receiver.receive();
                std::uint64_t actual = 0;
                if (!result || !valid_message(*result, actual) || actual != expected) {
                    ::_exit(10);
                }
            }
            if (receiver.receive().code != shmchan::channel_status::closed) {
                ::_exit(11);
            }
            ::_exit(0);
        } catch (...) {
            ::_exit(12);
        }
    }

    for (std::uint64_t id = 0; id < message_count; ++id) {
        const auto size = sizeof(std::uint64_t) + static_cast<std::size_t>((id * 67U) % 2048U);
        const auto message = make_message(id, size);
        CHECK(sender.send(message) == shmchan::channel_status::success);
    }
    CHECK(sender.close());

    int status = 0;
    CHECK(::waitpid(child, &status, 0) == child);
    CHECK(WIFEXITED(status));
    CHECK(WEXITSTATUS(status) == 0);
}

} // namespace

int main() {
    try {
        test_basic_wrap_and_limits();
        test_timeouts_and_close_wake();
        test_string_and_custom_codec();
        test_mpmc_threads();
        test_cross_process();
        std::cout << "all variable-length shmchan tests passed\n";
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "test failure: " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
