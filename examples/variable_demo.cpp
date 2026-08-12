#include <shmchan/serialized_channel.hpp>

#include <iostream>
#include <string>
#include <string_view>

int main(int argc, char** argv) {
    if (argc < 2 || argc > 3) {
        std::cerr << "usage: " << argv[0] << " <produce|consume> [channel-name]\n";
        return 2;
    }

    const std::string_view mode = argv[1];
    const std::string_view name = argc == 3 ? argv[2] : "shmchan-variable-demo";

    try {
        if (mode == "produce") {
            auto channel = shmchan::string_channel::create(name, 64 * 1024);
            for (int index = 1; index <= 10; ++index) {
                std::string message = "variable message #" + std::to_string(index);
                message.append(static_cast<std::size_t>(index * index), '!');
                if (channel.send(message) != shmchan::channel_status::success) {
                    std::cerr << "channel closed while sending\n";
                    return 1;
                }
            }
            (void)channel.close();
            std::cout << "sent 10 variable-length strings to " << channel.name() << '\n';
            return 0;
        }

        if (mode == "consume") {
            auto channel = shmchan::string_channel::open(name);
            for (;;) {
                auto result = channel.receive();
                if (result.code == shmchan::channel_status::closed) {
                    break;
                }
                std::cout << result->size() << " bytes: " << *result << '\n';
            }
            (void)channel.unlink();
            return 0;
        }

        std::cerr << "mode must be 'produce' or 'consume'\n";
        return 2;
    } catch (const std::exception& error) {
        std::cerr << "shmchan_variable_demo: " << error.what() << '\n';
        return 1;
    }
}
