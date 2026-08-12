#pragma once

#if !defined(__linux__)
#error "shmchan currently supports Linux only"
#endif

#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>

#include <fcntl.h>
#include <linux/futex.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

namespace shmchan {

enum class channel_status {
    success,
    closed,
    would_block,
    timed_out,
    message_too_large,
};

[[nodiscard]] constexpr std::string_view to_string(channel_status status) noexcept {
    switch (status) {
    case channel_status::success:
        return "success";
    case channel_status::closed:
        return "closed";
    case channel_status::would_block:
        return "would_block";
    case channel_status::timed_out:
        return "timed_out";
    case channel_status::message_too_large:
        return "message_too_large";
    }
    return "unknown";
}

template <typename T>
struct receive_result {
    channel_status code{channel_status::would_block};
    std::optional<T> value{};

    [[nodiscard]] bool has_value() const noexcept { return value.has_value(); }
    [[nodiscard]] explicit operator bool() const noexcept { return has_value(); }

    [[nodiscard]] T& operator*() & { return *value; }
    [[nodiscard]] const T& operator*() const& { return *value; }
    [[nodiscard]] T&& operator*() && { return *std::move(value); }

    [[nodiscard]] T* operator->() { return std::addressof(*value); }
    [[nodiscard]] const T* operator->() const { return std::addressof(*value); }
};

namespace detail {

inline constexpr std::size_t cache_line_size = 64;
inline constexpr std::uint64_t layout_magic = 0x53484d4348414e31ULL; // "SHMCHAN1"
inline constexpr std::uint32_t layout_version = 1;
inline constexpr std::uint32_t initialization_ready = 1;
inline constexpr std::uint32_t channel_closed_bit = 0x80000000U;
inline constexpr std::uint32_t active_sender_mask = channel_closed_bit - 1U;

class unique_fd {
public:
    explicit unique_fd(int fd = -1) noexcept : fd_(fd) {}
    ~unique_fd() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    unique_fd(const unique_fd&) = delete;
    unique_fd& operator=(const unique_fd&) = delete;

    unique_fd(unique_fd&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
    unique_fd& operator=(unique_fd&& other) noexcept {
        if (this != &other) {
            if (fd_ >= 0) {
                ::close(fd_);
            }
            fd_ = std::exchange(other.fd_, -1);
        }
        return *this;
    }

    [[nodiscard]] int get() const noexcept { return fd_; }

private:
    int fd_;
};

class mapped_region {
public:
    mapped_region() noexcept = default;
    mapped_region(void* address, std::size_t size) noexcept : address_(address), size_(size) {}

    ~mapped_region() {
        if (address_ != nullptr) {
            ::munmap(address_, size_);
        }
    }

    mapped_region(const mapped_region&) = delete;
    mapped_region& operator=(const mapped_region&) = delete;

    mapped_region(mapped_region&& other) noexcept
        : address_(std::exchange(other.address_, nullptr)), size_(std::exchange(other.size_, 0)) {}

    mapped_region& operator=(mapped_region&& other) noexcept {
        if (this != &other) {
            if (address_ != nullptr) {
                ::munmap(address_, size_);
            }
            address_ = std::exchange(other.address_, nullptr);
            size_ = std::exchange(other.size_, 0);
        }
        return *this;
    }

    [[nodiscard]] void* get() const noexcept { return address_; }
    [[nodiscard]] void* release() noexcept {
        size_ = 0;
        return std::exchange(address_, nullptr);
    }

private:
    void* address_{nullptr};
    std::size_t size_{0};
};

[[noreturn]] inline void throw_errno(std::string_view operation, std::string_view name = {}) {
    const int error = errno;
    std::string message(operation);
    if (!name.empty()) {
        message += " [";
        message += name;
        message += ']';
    }
    throw std::system_error(error, std::generic_category(), message);
}

[[nodiscard]] inline std::string normalize_name(std::string_view name) {
    if (name.empty()) {
        throw std::invalid_argument("shmchan: the shared-memory name cannot be empty");
    }
    if (name.find('\0') != std::string_view::npos) {
        throw std::invalid_argument("shmchan: the shared-memory name cannot contain NUL");
    }

    std::string normalized;
    if (name.front() == '/') {
        normalized.assign(name);
    } else {
        normalized.reserve(name.size() + 1);
        normalized.push_back('/');
        normalized.append(name);
    }

    if (normalized.size() == 1 || normalized.find('/', 1) != std::string::npos) {
        throw std::invalid_argument(
            "shmchan: use a POSIX shared-memory name with no slash except the leading one");
    }
    return normalized;
}

inline void lock_fd(int fd, int operation) {
    while (::flock(fd, operation) != 0) {
        if (errno != EINTR) {
            throw_errno("flock");
        }
    }
}

enum class futex_wait_result { changed, timed_out };

[[nodiscard]] inline futex_wait_result futex_wait(
    std::atomic<std::uint32_t>& word,
    std::uint32_t expected,
    const ::timespec* timeout) {
    static_assert(sizeof(std::atomic<std::uint32_t>) == sizeof(std::uint32_t));
    static_assert(alignof(std::atomic<std::uint32_t>) >= alignof(std::uint32_t));

    auto* address = reinterpret_cast<std::uint32_t*>(std::addressof(word));
    const long result = ::syscall(SYS_futex, address, FUTEX_WAIT, expected, timeout, nullptr, 0);
    if (result == 0) {
        return futex_wait_result::changed;
    }

    switch (errno) {
    case EAGAIN:
    case EINTR:
        return futex_wait_result::changed;
    case ETIMEDOUT:
        return futex_wait_result::timed_out;
    default:
        throw_errno("futex(FUTEX_WAIT)");
    }
}

inline void futex_wake(std::atomic<std::uint32_t>& word, int count) noexcept {
    auto* address = reinterpret_cast<std::uint32_t*>(std::addressof(word));
    (void)::syscall(SYS_futex, address, FUTEX_WAKE, count, nullptr, nullptr, 0);
}

using monotonic_clock = std::chrono::steady_clock;

[[nodiscard]] inline monotonic_clock::time_point make_deadline(std::chrono::nanoseconds timeout) {
    const auto now = monotonic_clock::now();
    if (timeout <= std::chrono::nanoseconds::zero()) {
        return now;
    }

    const auto available = monotonic_clock::time_point::max() - now;
    const auto converted = std::chrono::duration_cast<monotonic_clock::duration>(timeout);
    if (converted >= available) {
        return monotonic_clock::time_point::max();
    }
    return now + converted;
}

[[nodiscard]] inline futex_wait_result futex_wait_until(
    std::atomic<std::uint32_t>& word,
    std::uint32_t expected,
    monotonic_clock::time_point deadline) {
    const auto now = monotonic_clock::now();
    if (now >= deadline) {
        return futex_wait_result::timed_out;
    }

    const auto remaining = deadline - now;
    const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(remaining);
    auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(remaining - seconds);
    if (seconds == std::chrono::seconds::zero() && nanoseconds == std::chrono::nanoseconds::zero()) {
        nanoseconds = std::chrono::nanoseconds{1};
    }

    const ::timespec timeout{
        static_cast<::time_t>(seconds.count()),
        static_cast<long>(nanoseconds.count()),
    };
    return futex_wait(word, expected, &timeout);
}

} // namespace detail

template <typename T>
class channel {
    static_assert(std::is_trivially_copyable_v<T>,
                  "shmchan messages must be trivially copyable");
    static_assert(std::is_copy_constructible_v<T>,
                  "shmchan messages must be copy constructible");
    static_assert(std::atomic<std::uint32_t>::is_always_lock_free,
                  "shmchan requires lock-free 32-bit atomics");
    static_assert(std::atomic<std::uint64_t>::is_always_lock_free,
                  "shmchan requires lock-free 64-bit atomics");

    struct alignas(detail::cache_line_size) slot {
        std::atomic<std::uint64_t> sequence{};
        std::array<std::byte, sizeof(T)> storage{};
    };

    struct alignas(detail::cache_line_size) shared_header {
        std::uint64_t magic{};
        std::uint32_t version{};
        std::uint32_t reserved{};
        std::uint64_t mapping_size{};
        std::uint64_t slots_offset{};
        std::uint64_t capacity{};
        std::uint64_t header_size{};
        std::uint64_t slot_size{};
        std::uint64_t slot_alignment{};
        std::uint64_t element_size{};
        std::uint64_t element_alignment{};
        std::uint64_t element_type_tag{};

        alignas(detail::cache_line_size) std::atomic<std::uint32_t> initialization{};
        alignas(detail::cache_line_size) std::atomic<std::uint32_t> sender_state{};

        alignas(detail::cache_line_size) std::atomic<std::uint64_t> enqueue_position{};
        alignas(detail::cache_line_size) std::atomic<std::uint64_t> dequeue_position{};
        alignas(detail::cache_line_size) std::atomic<std::uint32_t> readable_epoch{};
        alignas(detail::cache_line_size) std::atomic<std::uint32_t> writable_epoch{};
    };

    static constexpr std::size_t align_up(std::size_t value, std::size_t alignment) noexcept {
        return (value + alignment - 1) & ~(alignment - 1);
    }

    static constexpr std::size_t slots_offset_value =
        align_up(sizeof(shared_header), alignof(slot));

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
                    break;
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

public:
    channel() noexcept = default;

    ~channel() { release(); }

    channel(const channel&) = delete;
    channel& operator=(const channel&) = delete;

    channel(channel&& other) noexcept { move_from(std::move(other)); }

    channel& operator=(channel&& other) noexcept {
        if (this != &other) {
            release();
            move_from(std::move(other));
        }
        return *this;
    }

    [[nodiscard]] static channel create(
        std::string_view name,
        std::size_t capacity,
        ::mode_t permissions = 0600) {
        auto normalized = detail::normalize_name(name);
        const std::size_t mapping_size = checked_mapping_size(capacity);

        const int raw_fd =
            ::shm_open(normalized.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, permissions);
        if (raw_fd < 0) {
            detail::throw_errno("shm_open(create)", normalized);
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
            header->magic = detail::layout_magic;
            header->version = detail::layout_version;
            header->mapping_size = mapping_size;
            header->slots_offset = slots_offset_value;
            header->capacity = capacity;
            header->header_size = sizeof(shared_header);
            header->slot_size = sizeof(slot);
            header->slot_alignment = alignof(slot);
            header->element_size = sizeof(T);
            header->element_alignment = alignof(T);
            header->element_type_tag = type_tag();
            header->initialization.store(0, std::memory_order_relaxed);
            header->sender_state.store(0, std::memory_order_relaxed);
            header->enqueue_position.store(0, std::memory_order_relaxed);
            header->dequeue_position.store(0, std::memory_order_relaxed);
            header->readable_epoch.store(0, std::memory_order_relaxed);
            header->writable_epoch.store(0, std::memory_order_relaxed);

            auto* slots = slots_address(raw_mapping);
            for (std::size_t index = 0; index < capacity; ++index) {
                auto* current = std::construct_at(slots + index);
                current->sequence.store(index, std::memory_order_relaxed);
            }

            header->initialization.store(
                detail::initialization_ready, std::memory_order_release);

            detail::lock_fd(fd.get(), LOCK_UN);
            unlink_on_failure = false;
            return channel(mapping.release(), mapping_size, std::move(normalized));
        } catch (...) {
            if (unlink_on_failure) {
                (void)::shm_unlink(normalized.c_str());
            }
            throw;
        }
    }

    [[nodiscard]] static channel open(std::string_view name) {
        auto normalized = detail::normalize_name(name);
        const int raw_fd = ::shm_open(normalized.c_str(), O_RDWR | O_CLOEXEC, 0);
        if (raw_fd < 0) {
            detail::throw_errno("shm_open(open)", normalized);
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
        return channel(mapping.release(), mapping_size, std::move(normalized));
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

    [[nodiscard]] std::size_t capacity() const {
        require_valid();
        return static_cast<std::size_t>(header_->capacity);
    }

    [[nodiscard]] bool is_closed() const {
        require_valid();
        return (header_->sender_state.load(std::memory_order_acquire) &
                detail::channel_closed_bit) != 0;
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

    [[nodiscard]] channel_status try_send(const T& value) {
        require_valid();
        active_sender_guard active(*header_);
        if (!active.registered()) {
            return channel_status::closed;
        }
        if (is_closed_state()) {
            return channel_status::closed;
        }
        if (try_enqueue(value)) {
            return channel_status::success;
        }
        return is_closed_state()
                   ? channel_status::closed
                   : channel_status::would_block;
    }

    [[nodiscard]] channel_status send(const T& value) {
        require_valid();
        return send_impl(value, std::nullopt);
    }

    [[nodiscard]] channel_status send_for(
        const T& value,
        std::chrono::nanoseconds timeout) {
        require_valid();
        return send_impl(value, detail::make_deadline(timeout));
    }

    [[nodiscard]] receive_result<T> try_receive() {
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

    [[nodiscard]] receive_result<T> receive() {
        require_valid();
        return receive_impl(std::nullopt);
    }

    [[nodiscard]] receive_result<T> receive_for(std::chrono::nanoseconds timeout) {
        require_valid();
        return receive_impl(detail::make_deadline(timeout));
    }

private:
    channel(void* mapping, std::size_t mapping_size, std::string name) noexcept
        : mapping_(mapping),
          mapping_size_(mapping_size),
          header_(static_cast<shared_header*>(mapping)),
          slots_(slots_address(mapping)),
          name_(std::move(name)) {}

    [[nodiscard]] static std::size_t checked_mapping_size(std::size_t capacity) {
        if (capacity == 0) {
            throw std::invalid_argument("shmchan: capacity must be greater than zero");
        }
        if (capacity > static_cast<std::size_t>(std::numeric_limits<std::int64_t>::max())) {
            throw std::length_error("shmchan: capacity is too large");
        }
        if (capacity >
            (std::numeric_limits<std::size_t>::max() - slots_offset_value) / sizeof(slot)) {
            throw std::length_error("shmchan: shared-memory mapping size overflows size_t");
        }

        const std::size_t size = slots_offset_value + capacity * sizeof(slot);
        if (size > static_cast<std::uintmax_t>(std::numeric_limits<::off_t>::max())) {
            throw std::length_error("shmchan: shared-memory mapping size overflows off_t");
        }
        return size;
    }

    [[nodiscard]] static slot* slots_address(void* mapping) noexcept {
        auto* bytes = static_cast<std::byte*>(mapping);
        return reinterpret_cast<slot*>(bytes + slots_offset_value);
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
        if (header->magic != detail::layout_magic ||
            header->version != detail::layout_version) {
            throw std::runtime_error("shmchan: incompatible shared-memory format: " + name);
        }
        if (header->capacity == 0 ||
            header->capacity > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
            throw std::runtime_error("shmchan: invalid capacity in shared-memory object: " + name);
        }

        const auto capacity = static_cast<std::size_t>(header->capacity);
        std::size_t expected_size{};
        try {
            expected_size = checked_mapping_size(capacity);
        } catch (const std::exception&) {
            throw std::runtime_error("shmchan: invalid mapping size in shared-memory object: " + name);
        }

        if (header->mapping_size != mapping_size || expected_size != mapping_size ||
            header->slots_offset != slots_offset_value ||
            header->header_size != sizeof(shared_header) || header->slot_size != sizeof(slot) ||
            header->slot_alignment != alignof(slot) || header->element_size != sizeof(T) ||
            header->element_alignment != alignof(T) || header->element_type_tag != type_tag()) {
            throw std::runtime_error(
                "shmchan: channel type or library ABI does not match: " + name);
        }
    }

    void require_valid() const {
        if (header_ == nullptr) {
            throw std::logic_error("shmchan: operation on an empty or moved-from channel");
        }
    }

    [[nodiscard]] static consteval std::uint64_t type_tag() noexcept {
#if defined(__clang__) || defined(__GNUC__)
        constexpr std::string_view signature = __PRETTY_FUNCTION__;
#else
        constexpr std::string_view signature = "shmchan::channel<T>";
#endif
        std::uint64_t hash = 14695981039346656037ULL;
        for (const char character : signature) {
            hash ^= static_cast<unsigned char>(character);
            hash *= 1099511628211ULL;
        }
        return hash;
    }

    [[nodiscard]] bool try_enqueue(const T& value) {
        if (header_->capacity == 1) {
            std::uint64_t empty = 0;
            if (!slots_[0].sequence.compare_exchange_strong(
                    empty, 1, std::memory_order_acquire, std::memory_order_relaxed)) {
                return false;
            }
            std::memcpy(slots_[0].storage.data(), std::addressof(value), sizeof(T));
            slots_[0].sequence.store(2, std::memory_order_release);
            header_->readable_epoch.fetch_add(1, std::memory_order_release);
            detail::futex_wake(header_->readable_epoch, 1);
            return true;
        }

        auto position = header_->enqueue_position.load(std::memory_order_relaxed);
        for (;;) {
            slot& current = slots_[position % header_->capacity];
            const auto sequence = current.sequence.load(std::memory_order_acquire);
            const auto difference = static_cast<std::int64_t>(sequence - position);

            if (difference == 0) {
                if (header_->enqueue_position.compare_exchange_weak(
                        position,
                        position + 1,
                        std::memory_order_relaxed,
                        std::memory_order_relaxed)) {
                    std::memcpy(current.storage.data(), std::addressof(value), sizeof(T));
                    current.sequence.store(position + 1, std::memory_order_release);
                    header_->readable_epoch.fetch_add(1, std::memory_order_release);
                    detail::futex_wake(header_->readable_epoch, 1);
                    return true;
                }
            } else if (difference < 0) {
                return false;
            } else {
                position = header_->enqueue_position.load(std::memory_order_relaxed);
            }
        }
    }

    [[nodiscard]] std::optional<T> try_dequeue() {
        if (header_->capacity == 1) {
            std::uint64_t full = 2;
            if (!slots_[0].sequence.compare_exchange_strong(
                    full, 3, std::memory_order_acquire, std::memory_order_relaxed)) {
                return std::nullopt;
            }
            std::array<std::byte, sizeof(T)> bytes{};
            std::memcpy(bytes.data(), slots_[0].storage.data(), sizeof(T));
            T value = std::bit_cast<T>(bytes);
            slots_[0].sequence.store(0, std::memory_order_release);
            header_->writable_epoch.fetch_add(1, std::memory_order_release);
            detail::futex_wake(header_->writable_epoch, 1);
            return value;
        }

        auto position = header_->dequeue_position.load(std::memory_order_relaxed);
        for (;;) {
            slot& current = slots_[position % header_->capacity];
            const auto sequence = current.sequence.load(std::memory_order_acquire);
            const auto difference = static_cast<std::int64_t>(sequence - (position + 1));

            if (difference == 0) {
                if (header_->dequeue_position.compare_exchange_weak(
                        position,
                        position + 1,
                        std::memory_order_relaxed,
                        std::memory_order_relaxed)) {
                    std::array<std::byte, sizeof(T)> bytes{};
                    std::memcpy(bytes.data(), current.storage.data(), sizeof(T));
                    T value = std::bit_cast<T>(bytes);
                    current.sequence.store(position + header_->capacity, std::memory_order_release);
                    header_->writable_epoch.fetch_add(1, std::memory_order_release);
                    detail::futex_wake(header_->writable_epoch, 1);
                    return value;
                }
            } else if (difference < 0) {
                return std::nullopt;
            } else {
                position = header_->dequeue_position.load(std::memory_order_relaxed);
            }
        }
    }

    [[nodiscard]] bool closed_and_senders_done() const noexcept {
        return header_->sender_state.load(std::memory_order_acquire) ==
               detail::channel_closed_bit;
    }

    [[nodiscard]] bool is_closed_state() const noexcept {
        return (header_->sender_state.load(std::memory_order_acquire) &
                detail::channel_closed_bit) != 0;
    }

    [[nodiscard]] channel_status send_impl(
        const T& value,
        std::optional<detail::monotonic_clock::time_point> deadline) {
        active_sender_guard active(*header_);
        if (!active.registered()) {
            return channel_status::closed;
        }
        for (;;) {
            if (is_closed_state()) {
                return channel_status::closed;
            }

            const auto epoch = header_->writable_epoch.load(std::memory_order_acquire);
            if (try_enqueue(value)) {
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
                    if (try_enqueue(value)) {
                        return channel_status::success;
                    }
                    return is_closed_state()
                               ? channel_status::closed
                               : channel_status::timed_out;
                }
            } else {
                (void)detail::futex_wait(header_->writable_epoch, epoch, nullptr);
            }
        }
    }

    [[nodiscard]] receive_result<T> receive_impl(
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
        slots_ = nullptr;
        name_.clear();
    }

    void move_from(channel&& other) noexcept {
        mapping_ = std::exchange(other.mapping_, nullptr);
        mapping_size_ = std::exchange(other.mapping_size_, 0);
        header_ = std::exchange(other.header_, nullptr);
        slots_ = std::exchange(other.slots_, nullptr);
        name_ = std::move(other.name_);
        other.name_.clear();
    }

    void* mapping_{nullptr};
    std::size_t mapping_size_{0};
    shared_header* header_{nullptr};
    slot* slots_{nullptr};
    std::string name_{};
};

} // namespace shmchan
