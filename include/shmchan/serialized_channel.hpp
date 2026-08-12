#pragma once

#include <shmchan/byte_channel.hpp>

#include <chrono>
#include <cstddef>
#include <iterator>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace shmchan {

template <typename T, typename Codec>
class serialized_channel {
public:
    using value_type = T;
    using codec_type = Codec;

    serialized_channel() noexcept(std::is_nothrow_default_constructible_v<Codec>) = default;

    [[nodiscard]] static serialized_channel create(
        std::string_view name,
        std::size_t capacity_bytes,
        Codec codec = {},
        ::mode_t permissions = 0600) {
        return serialized_channel(
            byte_channel::create(name, capacity_bytes, permissions), std::move(codec));
    }

    [[nodiscard]] static serialized_channel open(std::string_view name, Codec codec = {}) {
        return serialized_channel(byte_channel::open(name), std::move(codec));
    }

    [[nodiscard]] static bool unlink(std::string_view name) {
        return byte_channel::unlink(name);
    }

    [[nodiscard]] bool unlink() const { return channel_.unlink(); }
    [[nodiscard]] bool valid() const noexcept { return channel_.valid(); }
    [[nodiscard]] const std::string& name() const noexcept { return channel_.name(); }
    [[nodiscard]] std::size_t capacity_bytes() const { return channel_.capacity_bytes(); }
    [[nodiscard]] std::size_t max_message_size() const { return channel_.max_message_size(); }
    [[nodiscard]] bool is_closed() const { return channel_.is_closed(); }
    [[nodiscard]] bool close() { return channel_.close(); }

    template <typename U>
    [[nodiscard]] channel_status try_send(const U& value) {
        decltype(auto) encoded = codec_.encode(value);
        return channel_.try_send(encoded_bytes(encoded));
    }

    template <typename U>
    [[nodiscard]] channel_status send(const U& value) {
        decltype(auto) encoded = codec_.encode(value);
        return channel_.send(encoded_bytes(encoded));
    }

    template <typename U>
    [[nodiscard]] channel_status send_for(
        const U& value,
        std::chrono::nanoseconds timeout) {
        decltype(auto) encoded = codec_.encode(value);
        return channel_.send_for(encoded_bytes(encoded), timeout);
    }

    [[nodiscard]] receive_result<T> try_receive() {
        return decode(channel_.try_receive());
    }

    [[nodiscard]] receive_result<T> receive() {
        return decode(channel_.receive());
    }

    [[nodiscard]] receive_result<T> receive_for(std::chrono::nanoseconds timeout) {
        return decode(channel_.receive_for(timeout));
    }

    [[nodiscard]] byte_channel& raw_channel() noexcept { return channel_; }
    [[nodiscard]] const byte_channel& raw_channel() const noexcept { return channel_; }
    [[nodiscard]] Codec& codec() noexcept { return codec_; }
    [[nodiscard]] const Codec& codec() const noexcept { return codec_; }

private:
    serialized_channel(byte_channel channel, Codec codec)
        : channel_(std::move(channel)), codec_(std::move(codec)) {}

    template <typename Encoded>
    [[nodiscard]] static std::span<const std::byte> encoded_bytes(const Encoded& encoded) {
        using pointer_type = decltype(std::data(encoded));
        using element_type = std::remove_cv_t<std::remove_pointer_t<pointer_type>>;
        static_assert(sizeof(element_type) == 1,
                      "shmchan codecs must encode to a contiguous range of one-byte elements");

        return {
            reinterpret_cast<const std::byte*>(std::data(encoded)),
            static_cast<std::size_t>(std::size(encoded)),
        };
    }

    [[nodiscard]] receive_result<T> decode(receive_result<byte_channel::buffer_type> encoded) {
        if (!encoded) {
            return {encoded.code, std::nullopt};
        }

        const auto bytes = std::span<const std::byte>{encoded->data(), encoded->size()};
        return {channel_status::success, codec_.decode(bytes)};
    }

    byte_channel channel_{};
    [[no_unique_address]] Codec codec_{};
};

struct string_codec {
    [[nodiscard]] std::string_view encode(std::string_view value) const noexcept {
        return value;
    }

    [[nodiscard]] std::string decode(std::span<const std::byte> value) const {
        return {
            reinterpret_cast<const char*>(value.data()),
            value.size(),
        };
    }
};

using string_channel = serialized_channel<std::string, string_codec>;

} // namespace shmchan
