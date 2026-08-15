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
              << "  " << executable << " send <name> <message>\n"
              << "  " << executable << " recv <name>\n"
              << "  " << executable << " status <name>\n"
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
            if (argc != 3) {
                print_usage(argv[0]);
                return 2;
            }
            shmchan::managed_channel_options options;
            options.message_capacity = 1024;
            options.max_message_size = 64 * 1024;
            auto channel = shmchan::managed_byte_channel::open_or_create(name, options);
            std::cout << "已打开 " << channel.name()
                      << "，capacity=" << channel.capacity()
                      << "，max_message_size=" << channel.max_message_size() << '\n';
            return 0;
        }

        if (command == "cleanup") {
            if (argc != 3) {
                print_usage(argv[0]);
                return 2;
            }
            const bool removed = shmchan::managed_byte_channel::unlink(name);
            std::cout << (removed ? "已删除共享内存对象" : "共享内存对象不存在") << '\n';
            return 0;
        }

        auto channel = shmchan::managed_byte_channel::open(name);
        if (command == "send") {
            if (argc != 4) {
                print_usage(argv[0]);
                return 2;
            }
            require_success(channel.send(argv[3]), "send");
            std::cout << "发送成功\n";
            return 0;
        }
        if (command == "recv") {
            if (argc != 3) {
                print_usage(argv[0]);
                return 2;
            }
            auto message = channel.receive_for(5s);
            if (!message) {
                std::cout << "接收结束: " << shmchan::to_string(message.code) << '\n';
                return message.code == shmchan::channel_status::timed_out ? 3 : 1;
            }
            std::cout << "payload="
                      << as_string(std::span<const std::byte>{
                             message->data(), message->size()})
                      << '\n';
            return 0;
        }
        if (command == "status") {
            if (argc != 3) {
                print_usage(argv[0]);
                return 2;
            }
            const auto stats = channel.stats();
            std::cout << "state=" << shmchan::to_string(stats.state)
                      << " reason=" << shmchan::to_string(stats.reason)
                      << " free=" << stats.free_slots
                      << " writing=" << stats.writing_messages
                      << " ready=" << stats.ready_messages
                      << " sent=" << stats.sent_messages
                      << " received=" << stats.received_messages
                      << " owner_death_recoveries=" << stats.owner_death_recoveries
                      << " discarded_incomplete_writes="
                      << stats.discarded_incomplete_writes << '\n';
            return 0;
        }
        if (command == "close") {
            if (argc != 3) {
                print_usage(argv[0]);
                return 2;
            }
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
