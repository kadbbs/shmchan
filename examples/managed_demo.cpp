#include <shmchan/managed_channel.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {

using namespace std::chrono_literals;

void print_usage(const char* executable) {
    std::cerr << "用法:\n"
              << "  " << executable << " init <name>\n"
              << "  " << executable << " send <name> <message> [message-id]\n"
              << "  " << executable << " recv <name>\n"
              << "  " << executable << " status <name>\n"
              << "  " << executable << " break <name>\n"
              << "  " << executable << " rebuild <name>\n"
              << "  " << executable << " close <name>\n"
              << "  " << executable << " cleanup <name>\n";
}

[[nodiscard]] std::string as_string(std::span<const std::byte> bytes) {
    return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

void require_success(shmchan::channel_status status, std::string_view operation) {
    if (status != shmchan::channel_status::success) {
        throw std::runtime_error(
            std::string(operation) + " 失败: " + std::string(shmchan::to_string(status)));
    }
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        print_usage(argv[0]);
        return 2;
    }

    const std::string_view command = argv[1];
    const std::string_view name = argv[2];
    try {
        if (command == "init") {
            shmchan::managed_channel_options options;
            options.message_capacity = 1024;
            options.max_message_size = 64 * 1024;
            auto channel = shmchan::managed_byte_channel::create(name, options);
            std::cout << "已创建 " << channel.name() << "，generation="
                      << channel.generation() << '\n';
            return 0;
        }

        if (command == "cleanup") {
            const bool removed = shmchan::managed_byte_channel::unlink(name);
            std::cout << (removed ? "已删除共享内存对象" : "共享内存对象不存在") << '\n';
            return 0;
        }

        auto channel = shmchan::managed_byte_channel::open(name);
        if (command == "send") {
            if (argc < 4) {
                print_usage(argv[0]);
                return 2;
            }
            const auto message_id = argc >= 5 ? std::stoull(argv[4]) : 0ULL;
            require_success(channel.send(argv[3], message_id), "send");
            std::cout << "发送成功，generation=" << channel.generation() << '\n';
            return 0;
        }
        if (command == "recv") {
            auto delivery = channel.receive_for(5s);
            if (!delivery) {
                std::cout << "接收结束: " << shmchan::to_string(delivery.code) << '\n';
                return delivery.code == shmchan::channel_status::timed_out ? 3 : 1;
            }
            std::cout << "message_id=" << delivery->message_id()
                      << " generation=" << delivery->generation()
                      << " attempt=" << delivery->attempt()
                      << " payload=" << as_string(delivery->bytes()) << '\n';
            require_success(delivery->ack(), "ack");
            return 0;
        }
        if (command == "status") {
            const auto stats = channel.stats();
            std::cout << "state=" << shmchan::to_string(stats.state)
                      << " reason=" << shmchan::to_string(stats.reason)
                      << " generation=" << stats.generation
                      << " participants=" << stats.active_participants
                      << " ready=" << stats.ready_messages
                      << " inflight=" << stats.inflight_messages
                      << " sent=" << stats.sent_messages
                      << " acked=" << stats.acknowledged_messages
                      << " redelivered=" << stats.redelivered_messages << '\n';
            return 0;
        }
        if (command == "break") {
            std::cout << (channel.mark_broken() ? "已标记 broken" : "状态未改变") << '\n';
            return 0;
        }
        if (command == "rebuild") {
            const auto result = channel.rebuild();
            std::cout << "rebuild=" << shmchan::to_string(result.code)
                      << " old_generation=" << result.previous_generation
                      << " new_generation=" << result.generation << '\n';
            return result.code == shmchan::channel_status::success ? 0 : 1;
        }
        if (command == "close") {
            std::cout << (channel.close() ? "已关闭" : "状态未改变") << '\n';
            return 0;
        }

        print_usage(argv[0]);
        return 2;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
