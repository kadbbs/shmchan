#pragma once

#include <shmchan/channel.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace shmchan {

class byte_channel {
    static_assert(std::atomic<std::uint32_t>::is_always_lock_free,
                  "shmchan requires lock-free 32-bit atomics");
    static_assert(std::atomic<std::uint64_t>::is_always_lock_free,
                  "shmchan requires lock-free 64-bit atomics");
    static_assert(std::atomic_ref<std::uint64_t>::is_always_lock_free,
                  "shmchan requires lock-free 64-bit atomic_ref");

    static constexpr std::uint64_t layout_magic = 0x53484d4348414e42ULL; // "SHMCHANB"
    static constexpr std::uint32_t layout_version = 2;
    static constexpr std::size_t record_alignment = 32;

    struct alignas(record_alignment) record_header {
        std::uint64_t state{};
        std::uint64_t total_size{};
        std::uint64_t payload_size{};
        std::uint64_t reserved{};
    };

    static_assert(sizeof(record_header) == record_alignment);

    struct alignas(detail::cache_line_size) shared_header {
        std::uint64_t magic{};
        std::uint32_t version{};
        std::uint32_t reserved{};
        std::uint64_t mapping_size{};
        std::uint64_t data_offset{};
        std::uint64_t capacity_bytes{};
        std::uint64_t header_size{};
        std::uint64_t record_header_size{};
        std::uint64_t record_alignment_value{};

        alignas(detail::cache_line_size) std::atomic<std::uint32_t> initialization{};
        alignas(detail::cache_line_size) std::atomic<std::uint32_t> sender_state{};

        alignas(detail::cache_line_size) std::atomic<std::uint64_t> write_position{};
        alignas(detail::cache_line_size) std::atomic<std::uint64_t> claim_position{};
        alignas(detail::cache_line_size) std::atomic<std::uint64_t> reclaim_position{};
        alignas(detail::cache_line_size) std::atomic<std::uint32_t> reclaim_lock{};
        alignas(detail::cache_line_size) std::atomic<std::uint32_t> readable_epoch{};
        alignas(detail::cache_line_size) std::atomic<std::uint32_t> writable_epoch{};
    };

    static constexpr std::size_t align_up(std::size_t value, std::size_t alignment) noexcept {
        return (value + alignment - 1) & ~(alignment - 1);
    }

    static constexpr std::size_t data_offset_value =
        (sizeof(shared_header) + record_alignment - 1) & ~(record_alignment - 1);

    class active_sender_guard {
    public:
        explicit active_sender_guard(shared_header& header) : header_(header) {
            auto current = header_.sender_state.load(std::memory_order_acquire);
            for (;;) {
                if ((current & detail::channel_closed_bit) != 0) {
                    return;
                }
                if ((current & detail::active_sender_mask) == detail::active_sender_mask) {
                    throw std::overflow_error("shmchan: too many concurrent senders");
                }
                if (header_.sender_state.compare_exchange_weak(
                        current, current + 1, std::memory_order_acq_rel, std::memory_order_relaxed)) {
                    registered_ = true;
                    return;
                }
            }
        }

        ~active_sender_guard() {
            if (registered_ &&
                header_.sender_state.fetch_sub(1, std::memory_order_acq_rel) ==
                    (detail::channel_closed_bit | 1U)) {
                header_.readable_epoch.fetch_add(1, std::memory_order_release);
                detail::futex_wake(header_.readable_epoch, INT_MAX);
            }
        }

        active_sender_guard(const active_sender_guard&) = delete;
        active_sender_guard& operator=(const active_sender_guard&) = delete;

        [[nodiscard]] bool registered() const noexcept { return registered_; }

    private:
        shared_header& header_;
        bool registered_{false};
    };

    class claimed_record_guard {
    public:
        claimed_record_guard(byte_channel& channel, record_header& record, std::uint64_t position)
            : channel_(channel), record_(record), position_(position) {}

        ~claimed_record_guard() {
            state_of(record_).store(consumed_token(position_), std::memory_order_release);
            channel_.reclaim_consumed();
        }

        claimed_record_guard(const claimed_record_guard&) = delete;
        claimed_record_guard& operator=(const claimed_record_guard&) = delete;

    private:
        byte_channel& channel_;
        record_header& record_;
        std::uint64_t position_;
    };

    class reclaim_lock_guard {
    public:
        explicit reclaim_lock_guard(shared_header& header) noexcept : header_(header) {
            for (;;) {
                std::uint32_t unlocked = 0;
                if (header_.reclaim_lock.compare_exchange_weak(
                        unlocked, 1, std::memory_order_acquire, std::memory_order_relaxed)) {
                    return;
                }
                for (unsigned spin = 0; spin < 32; ++spin) {
                    if (header_.reclaim_lock.load(std::memory_order_relaxed) == 0) {
                        break;
                    }
                    std::atomic_signal_fence(std::memory_order_seq_cst);
                }
                if (header_.reclaim_lock.load(std::memory_order_relaxed) != 0) {
                    auto* address = reinterpret_cast<std::uint32_t*>(
                        std::addressof(header_.reclaim_lock));
                    (void)::syscall(
                        SYS_futex, address, FUTEX_WAIT, 1U, nullptr, nullptr, 0);
                }
            }
        }

        ~reclaim_lock_guard() {
            header_.reclaim_lock.store(0, std::memory_order_release);
            detail::futex_wake(header_.reclaim_lock, 1);
        }

        reclaim_lock_guard(const reclaim_lock_guard&) = delete;
        reclaim_lock_guard& operator=(const reclaim_lock_guard&) = delete;

    private:
        shared_header& header_;
    };

public:
    using buffer_type = std::vector<std::byte>;

    byte_channel() noexcept = default;
    ~byte_channel() { release(); }

    byte_channel(const byte_channel&) = delete;
    byte_channel& operator=(const byte_channel&) = delete;

    byte_channel(byte_channel&& other) noexcept { move_from(std::move(other)); }

    byte_channel& operator=(byte_channel&& other) noexcept {
        if (this != &other) {
            release();
            move_from(std::move(other));
        }
        return *this;
    }

    [[nodiscard]] static byte_channel create(
        std::string_view name,
        std::size_t capacity_bytes,
        ::mode_t permissions = 0600) {
        auto normalized = detail::normalize_name(name);
        capacity_bytes = checked_capacity(capacity_bytes);
        const std::size_t mapping_size = checked_mapping_size(capacity_bytes);

        const int raw_fd =
            ::shm_open(normalized.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, permissions);
        if (raw_fd < 0) {
            detail::throw_errno("shm_open(create byte channel)", normalized);
        }
        detail::unique_fd fd(raw_fd);
        bool unlink_on_failure = true;

        try {
            detail::lock_fd(fd.get(), LOCK_EX);
            if (::ftruncate(fd.get(), static_cast<::off_t>(mapping_size)) != 0) {
                detail::throw_errno("ftruncate", normalized);
            }

            void* raw_mapping =
                ::mmap(nullptr, mapping_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd.get(), 0);
            if (raw_mapping == MAP_FAILED) {
                detail::throw_errno("mmap", normalized);
            }
            detail::mapped_region mapping(raw_mapping, mapping_size);

            auto* header = std::construct_at(static_cast<shared_header*>(raw_mapping));
            header->magic = layout_magic;
            header->version = layout_version;
            header->mapping_size = mapping_size;
            header->data_offset = data_offset_value;
            header->capacity_bytes = capacity_bytes;
            header->header_size = sizeof(shared_header);
            header->record_header_size = sizeof(record_header);
            header->record_alignment_value = record_alignment;
            header->initialization.store(0, std::memory_order_relaxed);
            header->sender_state.store(0, std::memory_order_relaxed);
            header->write_position.store(0, std::memory_order_relaxed);
            header->claim_position.store(0, std::memory_order_relaxed);
            header->reclaim_position.store(0, std::memory_order_relaxed);
            header->reclaim_lock.store(0, std::memory_order_relaxed);
            header->readable_epoch.store(0, std::memory_order_relaxed);
            header->writable_epoch.store(0, std::memory_order_relaxed);

            auto* data = data_address(raw_mapping);
            for (std::size_t offset = 0; offset < capacity_bytes; offset += record_alignment) {
                (void)std::construct_at(reinterpret_cast<record_header*>(data + offset));
            }
            header->initialization.store(
                detail::initialization_ready, std::memory_order_release);

            detail::lock_fd(fd.get(), LOCK_UN);
            unlink_on_failure = false;
            return byte_channel(mapping.release(), mapping_size, std::move(normalized));
        } catch (...) {
            if (unlink_on_failure) {
                (void)::shm_unlink(normalized.c_str());
            }
            throw;
        }
    }

    [[nodiscard]] static byte_channel open(std::string_view name) {
        auto normalized = detail::normalize_name(name);
        const int raw_fd = ::shm_open(normalized.c_str(), O_RDWR | O_CLOEXEC, 0);
        if (raw_fd < 0) {
            detail::throw_errno("shm_open(open byte channel)", normalized);
        }
        detail::unique_fd fd(raw_fd);
        detail::lock_fd(fd.get(), LOCK_SH);

        struct stat attributes {};
        if (::fstat(fd.get(), &attributes) != 0) {
            detail::throw_errno("fstat", normalized);
        }
        if (attributes.st_size < 0 ||
            static_cast<std::uintmax_t>(attributes.st_size) < sizeof(shared_header)) {
            throw std::runtime_error("shmchan: shared-memory object is incomplete: " + normalized);
        }
        if (static_cast<std::uintmax_t>(attributes.st_size) >
            std::numeric_limits<std::size_t>::max()) {
            throw std::runtime_error("shmchan: shared-memory object is too large: " + normalized);
        }
        const auto mapping_size = static_cast<std::size_t>(attributes.st_size);

        void* raw_mapping =
            ::mmap(nullptr, mapping_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd.get(), 0);
        if (raw_mapping == MAP_FAILED) {
            detail::throw_errno("mmap", normalized);
        }
        detail::mapped_region mapping(raw_mapping, mapping_size);
        validate_mapping(raw_mapping, mapping_size, normalized);
        detail::lock_fd(fd.get(), LOCK_UN);
        return byte_channel(mapping.release(), mapping_size, std::move(normalized));
    }

    [[nodiscard]] static bool unlink(std::string_view name) {
        const auto normalized = detail::normalize_name(name);
        if (::shm_unlink(normalized.c_str()) == 0) {
            return true;
        }
        if (errno == ENOENT) {
            return false;
        }
        detail::throw_errno("shm_unlink", normalized);
    }

    [[nodiscard]] bool unlink() const {
        require_valid();
        return unlink(name_);
    }

    [[nodiscard]] bool valid() const noexcept { return header_ != nullptr; }
    [[nodiscard]] const std::string& name() const noexcept { return name_; }

    [[nodiscard]] std::size_t capacity_bytes() const {
        require_valid();
        return static_cast<std::size_t>(header_->capacity_bytes);
    }

    [[nodiscard]] std::size_t max_message_size() const {
        require_valid();
        return static_cast<std::size_t>(header_->capacity_bytes) - sizeof(record_header);
    }

    [[nodiscard]] bool is_closed() const {
        require_valid();
        return is_closed_state();
    }

    [[nodiscard]] bool close() {
        require_valid();
        const auto previous =
            header_->sender_state.fetch_or(detail::channel_closed_bit, std::memory_order_acq_rel);
        if ((previous & detail::channel_closed_bit) != 0) {
            return false;
        }

        header_->readable_epoch.fetch_add(1, std::memory_order_release);
        header_->writable_epoch.fetch_add(1, std::memory_order_release);
        detail::futex_wake(header_->readable_epoch, INT_MAX);
        detail::futex_wake(header_->writable_epoch, INT_MAX);
        return true;
    }

    [[nodiscard]] channel_status try_send(std::span<const std::byte> message) {
        require_valid();
        if (message.size() > max_message_size()) {
            return channel_status::message_too_large;
        }

        active_sender_guard active(*header_);
        if (!active.registered() || is_closed_state()) {
            return channel_status::closed;
        }
        if (try_enqueue(message)) {
            return channel_status::success;
        }
        return is_closed_state() ? channel_status::closed : channel_status::would_block;
    }

    [[nodiscard]] channel_status try_send(std::string_view message) {
        return try_send(as_bytes(message));
    }

    [[nodiscard]] channel_status send(std::span<const std::byte> message) {
        require_valid();
        return send_impl(message, std::nullopt);
    }

    [[nodiscard]] channel_status send(std::string_view message) {
        return send(as_bytes(message));
    }

    [[nodiscard]] channel_status send_for(
        std::span<const std::byte> message,
        std::chrono::nanoseconds timeout) {
        require_valid();
        return send_impl(message, detail::make_deadline(timeout));
    }

    [[nodiscard]] channel_status send_for(
        std::string_view message,
        std::chrono::nanoseconds timeout) {
        return send_for(as_bytes(message), timeout);
    }

    [[nodiscard]] receive_result<buffer_type> try_receive() {
        require_valid();
        if (auto value = try_dequeue()) {
            return {channel_status::success, std::move(value)};
        }
        if (closed_and_senders_done()) {
            if (auto value = try_dequeue()) {
                return {channel_status::success, std::move(value)};
            }
            return {channel_status::closed, std::nullopt};
        }
        return {channel_status::would_block, std::nullopt};
    }

    [[nodiscard]] receive_result<buffer_type> receive() {
        require_valid();
        return receive_impl(std::nullopt);
    }

    [[nodiscard]] receive_result<buffer_type> receive_for(std::chrono::nanoseconds timeout) {
        require_valid();
        return receive_impl(detail::make_deadline(timeout));
    }

private:
    byte_channel(void* mapping, std::size_t mapping_size, std::string name) noexcept
        : mapping_(mapping),
          mapping_size_(mapping_size),
          header_(static_cast<shared_header*>(mapping)),
          data_(data_address(mapping)),
          name_(std::move(name)) {}

    [[nodiscard]] static std::span<const std::byte> as_bytes(std::string_view value) noexcept {
        return {
            reinterpret_cast<const std::byte*>(value.data()),
            value.size(),
        };
    }

    [[nodiscard]] static std::size_t checked_capacity(std::size_t capacity_bytes) {
        if (capacity_bytes <= sizeof(record_header)) {
            throw std::invalid_argument(
                "shmchan: byte channel capacity must be larger than the record header");
        }
        if (capacity_bytes > std::numeric_limits<std::size_t>::max() -
                                 (record_alignment - 1)) {
            throw std::length_error("shmchan: byte channel capacity is too large");
        }
        capacity_bytes = align_up(capacity_bytes, record_alignment);
        if (capacity_bytes > static_cast<std::size_t>(std::numeric_limits<std::int64_t>::max())) {
            throw std::length_error("shmchan: byte channel capacity is too large");
        }
        return capacity_bytes;
    }

    [[nodiscard]] static std::size_t checked_mapping_size(std::size_t capacity_bytes) {
        if (capacity_bytes >
            std::numeric_limits<std::size_t>::max() - data_offset_value) {
            throw std::length_error("shmchan: shared-memory mapping size overflows size_t");
        }
        const std::size_t size = data_offset_value + capacity_bytes;
        if (size > static_cast<std::uintmax_t>(std::numeric_limits<::off_t>::max())) {
            throw std::length_error("shmchan: shared-memory mapping size overflows off_t");
        }
        return size;
    }

    [[nodiscard]] static std::byte* data_address(void* mapping) noexcept {
        return static_cast<std::byte*>(mapping) + data_offset_value;
    }

    static void validate_mapping(
        void* mapping,
        std::size_t mapping_size,
        const std::string& name) {
        const auto* header = static_cast<const shared_header*>(mapping);
        if (header->initialization.load(std::memory_order_acquire) !=
            detail::initialization_ready) {
            throw std::runtime_error("shmchan: shared-memory object is not initialized: " + name);
        }
        if (header->magic != layout_magic || header->version != layout_version) {
            throw std::runtime_error("shmchan: incompatible byte-channel format: " + name);
        }
        if (header->capacity_bytes <= sizeof(record_header) ||
            header->capacity_bytes >
                static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max()) ||
            (header->capacity_bytes % record_alignment) != 0) {
            throw std::runtime_error("shmchan: invalid byte-channel capacity: " + name);
        }

        const auto capacity = static_cast<std::size_t>(header->capacity_bytes);
        std::size_t expected_size{};
        try {
            expected_size = checked_mapping_size(capacity);
        } catch (const std::exception&) {
            throw std::runtime_error("shmchan: invalid byte-channel mapping size: " + name);
        }

        if (header->mapping_size != mapping_size || expected_size != mapping_size ||
            header->data_offset != data_offset_value ||
            header->header_size != sizeof(shared_header) ||
            header->record_header_size != sizeof(record_header) ||
            header->record_alignment_value != record_alignment) {
            throw std::runtime_error("shmchan: byte-channel library ABI does not match: " + name);
        }
    }

    void require_valid() const {
        if (header_ == nullptr) {
            throw std::logic_error("shmchan: operation on an empty or moved-from byte channel");
        }
    }

    [[nodiscard]] static constexpr std::uint64_t published_token(
        std::uint64_t position) noexcept {
        return position + 1;
    }

    [[nodiscard]] static constexpr std::uint64_t claimed_token(
        std::uint64_t position) noexcept {
        return position + 2;
    }

    [[nodiscard]] static constexpr std::uint64_t consumed_token(
        std::uint64_t position) noexcept {
        return position + 3;
    }

    [[nodiscard]] static std::atomic_ref<std::uint64_t> state_of(
        record_header& record) noexcept {
        return std::atomic_ref<std::uint64_t>{record.state};
    }

    [[nodiscard]] static std::size_t record_size(std::size_t payload_size) noexcept {
        return align_up(sizeof(record_header) + payload_size, record_alignment);
    }

    [[nodiscard]] record_header* record_at(std::uint64_t position) const noexcept {
        const auto offset = static_cast<std::size_t>(position % header_->capacity_bytes);
        return reinterpret_cast<record_header*>(data_ + offset);
    }

    void copy_to_record(
        std::uint64_t position,
        std::span<const std::byte> message) noexcept {
        if (message.empty()) {
            return;
        }

        const auto capacity = static_cast<std::size_t>(header_->capacity_bytes);
        const auto record_offset = static_cast<std::size_t>(position % header_->capacity_bytes);
        const auto payload_offset = (record_offset + sizeof(record_header)) % capacity;
        const auto first_size = std::min(message.size(), capacity - payload_offset);
        std::memcpy(data_ + payload_offset, message.data(), first_size);
        if (first_size != message.size()) {
            std::memcpy(data_, message.data() + first_size, message.size() - first_size);
        }
    }

    void copy_from_record(
        std::uint64_t position,
        std::span<std::byte> destination) const noexcept {
        if (destination.empty()) {
            return;
        }

        const auto capacity = static_cast<std::size_t>(header_->capacity_bytes);
        const auto record_offset = static_cast<std::size_t>(position % header_->capacity_bytes);
        const auto payload_offset = (record_offset + sizeof(record_header)) % capacity;
        const auto first_size = std::min(destination.size(), capacity - payload_offset);
        std::memcpy(destination.data(), data_ + payload_offset, first_size);
        if (first_size != destination.size()) {
            std::memcpy(destination.data() + first_size, data_, destination.size() - first_size);
        }
    }

    [[nodiscard]] bool try_enqueue(std::span<const std::byte> message) {
        const auto required = static_cast<std::uint64_t>(record_size(message.size()));
        auto position = header_->write_position.load(std::memory_order_relaxed);

        for (;;) {
            const auto reclaimed = header_->reclaim_position.load(std::memory_order_acquire);
            const auto used = position - reclaimed;
            if (used > header_->capacity_bytes ||
                required > header_->capacity_bytes - used) {
                return false;
            }

            if (header_->write_position.compare_exchange_weak(
                    position,
                    position + required,
                    std::memory_order_relaxed,
                    std::memory_order_relaxed)) {
                auto* record = record_at(position);
                record->total_size = required;
                record->payload_size = message.size();
                record->reserved = 0;
                copy_to_record(position, message);
                state_of(*record).store(published_token(position), std::memory_order_release);
                header_->readable_epoch.fetch_add(1, std::memory_order_release);
                detail::futex_wake(header_->readable_epoch, 1);
                return true;
            }
        }
    }

    [[nodiscard]] std::optional<buffer_type> try_dequeue() {
        auto position = header_->claim_position.load(std::memory_order_relaxed);
        if (position == header_->write_position.load(std::memory_order_acquire)) {
            return std::nullopt;
        }

        record_header& record = *record_at(position);
        auto expected = published_token(position);
        if (!state_of(record).compare_exchange_strong(
                expected,
                claimed_token(position),
                std::memory_order_acquire,
                std::memory_order_relaxed)) {
            return std::nullopt;
        }

        const auto total_size = record.total_size;
        const auto payload_size = record.payload_size;
        if (total_size < sizeof(record_header) || total_size > header_->capacity_bytes ||
            (total_size % record_alignment) != 0 ||
            payload_size > total_size - sizeof(record_header)) {
            state_of(record).store(consumed_token(position), std::memory_order_release);
            reclaim_consumed();
            throw std::runtime_error("shmchan: corrupt byte-channel record in " + name_);
        }

        auto claim_expected = position;
        if (!header_->claim_position.compare_exchange_strong(
                claim_expected,
                position + total_size,
                std::memory_order_release,
                std::memory_order_relaxed)) {
            state_of(record).store(consumed_token(position), std::memory_order_release);
            reclaim_consumed();
            throw std::runtime_error("shmchan: inconsistent byte-channel claim position in " + name_);
        }

        header_->readable_epoch.fetch_add(1, std::memory_order_release);
        detail::futex_wake(header_->readable_epoch, 1);

        claimed_record_guard consumed(*this, record, position);
        buffer_type value(static_cast<std::size_t>(payload_size));
        copy_from_record(position, value);
        return value;
    }

    void reclaim_consumed() noexcept {
        reclaim_lock_guard lock(*header_);
        bool reclaimed_any = false;
        auto position = header_->reclaim_position.load(std::memory_order_relaxed);

        for (;;) {
            if (position == header_->claim_position.load(std::memory_order_acquire)) {
                break;
            }

            record_header& record = *record_at(position);
            if (state_of(record).load(std::memory_order_acquire) != consumed_token(position)) {
                break;
            }
            const auto total_size = record.total_size;
            if (total_size < sizeof(record_header) || total_size > header_->capacity_bytes ||
                (total_size % record_alignment) != 0) {
                break;
            }

            if (header_->reclaim_position.compare_exchange_weak(
                    position,
                    position + total_size,
                    std::memory_order_release,
                    std::memory_order_relaxed)) {
                reclaimed_any = true;
                position += total_size;
            }
        }

        if (reclaimed_any) {
            header_->writable_epoch.fetch_add(1, std::memory_order_release);
            detail::futex_wake(header_->writable_epoch, INT_MAX);
        }
    }

    [[nodiscard]] bool is_closed_state() const noexcept {
        return (header_->sender_state.load(std::memory_order_acquire) &
                detail::channel_closed_bit) != 0;
    }

    [[nodiscard]] bool closed_and_senders_done() const noexcept {
        return header_->sender_state.load(std::memory_order_acquire) ==
               detail::channel_closed_bit;
    }

    [[nodiscard]] channel_status send_impl(
        std::span<const std::byte> message,
        std::optional<detail::monotonic_clock::time_point> deadline) {
        if (message.size() > max_message_size()) {
            return channel_status::message_too_large;
        }

        active_sender_guard active(*header_);
        if (!active.registered()) {
            return channel_status::closed;
        }

        for (;;) {
            if (is_closed_state()) {
                return channel_status::closed;
            }

            const auto epoch = header_->writable_epoch.load(std::memory_order_acquire);
            if (try_enqueue(message)) {
                return channel_status::success;
            }
            if (is_closed_state()) {
                return channel_status::closed;
            }

            if (deadline.has_value()) {
                if (detail::futex_wait_until(header_->writable_epoch, epoch, *deadline) ==
                    detail::futex_wait_result::timed_out) {
                    if (is_closed_state()) {
                        return channel_status::closed;
                    }
                    if (try_enqueue(message)) {
                        return channel_status::success;
                    }
                    return is_closed_state() ? channel_status::closed
                                             : channel_status::timed_out;
                }
            } else {
                (void)detail::futex_wait(header_->writable_epoch, epoch, nullptr);
            }
        }
    }

    [[nodiscard]] receive_result<buffer_type> receive_impl(
        std::optional<detail::monotonic_clock::time_point> deadline) {
        for (;;) {
            const auto epoch = header_->readable_epoch.load(std::memory_order_acquire);
            if (auto value = try_dequeue()) {
                return {channel_status::success, std::move(value)};
            }
            if (closed_and_senders_done()) {
                if (auto value = try_dequeue()) {
                    return {channel_status::success, std::move(value)};
                }
                return {channel_status::closed, std::nullopt};
            }

            if (deadline.has_value()) {
                if (detail::futex_wait_until(header_->readable_epoch, epoch, *deadline) ==
                    detail::futex_wait_result::timed_out) {
                    if (auto value = try_dequeue()) {
                        return {channel_status::success, std::move(value)};
                    }
                    if (closed_and_senders_done()) {
                        return {channel_status::closed, std::nullopt};
                    }
                    return {channel_status::timed_out, std::nullopt};
                }
            } else {
                (void)detail::futex_wait(header_->readable_epoch, epoch, nullptr);
            }
        }
    }

    void release() noexcept {
        if (mapping_ != nullptr) {
            (void)::munmap(mapping_, mapping_size_);
        }
        mapping_ = nullptr;
        mapping_size_ = 0;
        header_ = nullptr;
        data_ = nullptr;
        name_.clear();
    }

    void move_from(byte_channel&& other) noexcept {
        mapping_ = std::exchange(other.mapping_, nullptr);
        mapping_size_ = std::exchange(other.mapping_size_, 0);
        header_ = std::exchange(other.header_, nullptr);
        data_ = std::exchange(other.data_, nullptr);
        name_ = std::move(other.name_);
        other.name_.clear();
    }

    void* mapping_{nullptr};
    std::size_t mapping_size_{0};
    shared_header* header_{nullptr};
    std::byte* data_{nullptr};
    std::string name_{};
};

} // namespace shmchan
