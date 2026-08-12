#include <shmchan/channel.hpp>

#include <cstdint>
#include <cstdio>
#include <iostream>
#include <string_view>

struct message {
    std::uint64_t id{};
    char text[56]{};
};

static_assert(std::is_trivially_copyable_v<message>);

int main(int argc, char** argv) {
    if (argc < 2 || argc > 3) {
        std::cerr << "usage: " << argv[0] << " <produce|consume> [channel-name]\n";
        return 2;
    }

    const std::string_view mode = argv[1];
    const std::string_view name = argc == 3 ? argv[2] : "shmchan-demo";

    try {
        if (mode == "produce") {
            auto channel = shmchan::channel<message>::create(name, 32);
            for (std::uint64_t id = 1; id <= 10; ++id) {
                message value{.id = id};
                std::snprintf(value.text, sizeof(value.text), "hello from producer #%llu",
                              static_cast<unsigned long long>(id));
                if (channel.send(value) != shmchan::channel_status::success) {
                    std::cerr << "channel closed while sending\n";
                    return 1;
                }
            }
            (void)channel.close();
            std::cout << "sent 10 messages to " << channel.name() << " and closed it\n";
            return 0;
        }

        if (mode == "consume") {
            auto channel = shmchan::channel<message>::open(name);
            for (;;) {
                auto result = channel.receive();
                if (result.code == shmchan::channel_status::closed) {
                    break;
                }
                std::cout << result->id << ": " << result->text << '\n';
            }
            (void)channel.unlink();
            return 0;
        }

        std::cerr << "mode must be 'produce' or 'consume'\n";
        return 2;
    } catch (const std::exception& error) {
        std::cerr << "shmchan_demo: " << error.what() << '\n';
        return 1;
    }
}
