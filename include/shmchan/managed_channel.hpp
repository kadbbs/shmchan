#pragma once

#include <shmchan/channel.hpp>

#include <algorithm>
#include <atomic>
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
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include <pthread.h>

namespace shmchan {

struct protocol_descriptor {
    std::uint64_t id{};
    std::uint32_t version{1};

    friend constexpr bool operator==(const protocol_descriptor&,
                                     const protocol_descriptor&) = default;
};

[[nodiscard]] constexpr std::uint64_t protocol_id(std::string_view name) noexcept {
    std::uint64_t hash = 14695981039346656037ULL;
    for (const char character : name) {
        hash ^= static_cast<unsigned char>(character);
        hash *= 1099511628211ULL;
    }
    return hash;
}

enum class managed_channel_state : std::uint32_t {
    initializing = 0,
    healthy = 1,
    closed = 2,
    broken = 3,
    destroying = 4,
};

enum class break_reason : std::uint32_t {
    none = 0,
    robust_mutex_not_recoverable = 1,
    corrupt_data = 2,
    sequence_exhausted = 3,
};

[[nodiscard]] constexpr std::string_view to_string(managed_channel_state state) noexcept {
    switch (state) {
    case managed_channel_state::initializing:
        return "initializing";
    case managed_channel_state::healthy:
        return "healthy";
    case managed_channel_state::closed:
        return "closed";
    case managed_channel_state::broken:
        return "broken";
    case managed_channel_state::destroying:
        return "destroying";
    }
    return "unknown";
}

[[nodiscard]] constexpr std::string_view to_string(break_reason reason) noexcept {
    switch (reason) {
    case break_reason::none:
        return "none";
    case break_reason::robust_mutex_not_recoverable:
        return "robust_mutex_not_recoverable";
    case break_reason::corrupt_data:
        return "corrupt_data";
    case break_reason::sequence_exhausted:
        return "sequence_exhausted";
    }
    return "unknown";
}

class managed_channel_error : public std::runtime_error {
public:
    managed_channel_error(channel_status code, std::string message)
        : std::runtime_error(std::move(message)), code_(code) {}

    [[nodiscard]] channel_status code() const noexcept { return code_; }

private:
    channel_status code_;
};

struct managed_channel_options {
    std::size_t message_capacity{1024};
    std::size_t max_message_size{4096};
    protocol_descriptor protocol{protocol_id("shmchan.raw-bytes"), 1};
    ::mode_t permissions{0600};
};

struct managed_open_options {
    protocol_descriptor protocol{protocol_id("shmchan.raw-bytes"), 1};
};

struct managed_channel_stats {
    managed_channel_state state{managed_channel_state::initializing};
    break_reason reason{break_reason::none};
    std::size_t message_capacity{};
    std::size_t max_message_size{};
    std::size_t free_slots{};
    std::size_t writing_messages{};
    std::size_t ready_messages{};
    std::size_t waiting_senders{};
    std::size_t waiting_receivers{};
    std::uint64_t sent_messages{};
    std::uint64_t sent_bytes{};
    std::uint64_t received_messages{};
    std::uint64_t received_bytes{};
    std::uint64_t owner_death_recoveries{};
    std::uint64_t discarded_incomplete_writes{};
    std::uint64_t send_timeouts{};
    std::uint64_t receive_timeouts{};
};

namespace detail {

inline constexpr std::uint64_t managed_simple_magic = 0x53484d4353494d50ULL; // SHMCSIMP
inline constexpr std::uint32_t managed_simple_version = 1;
inline constexpr std::size_t managed_cache_line = 64;
inline constexpr std::uint32_t managed_initialization_ready = 1;
inline constexpr auto managed_recovery_poll_interval = std::chrono::milliseconds{50};

static_assert(std::atomic<std::uint32_t>::is_always_lock_free);
static_assert(std::atomic<std::uint64_t>::is_always_lock_free);

enum class managed_slot_state : std::uint32_t {
    free = 0,
    writing = 1,
    ready = 2,
};

struct alignas(managed_cache_line) managed_message_slot {
    std::atomic<std::uint32_t> state{};
    std::uint32_t payload_size{};
    std::uint64_t sequence{};
    std::byte reserved[48]{};
};

static_assert(sizeof(managed_message_slot) == managed_cache_line);

struct alignas(managed_cache_line) managed_counters {
    std::atomic<std::uint64_t> sent_messages{};
    std::atomic<std::uint64_t> sent_bytes{};
    std::atomic<std::uint64_t> received_messages{};
    std::atomic<std::uint64_t> received_bytes{};
    std::atomic<std::uint64_t> owner_death_recoveries{};
    std::atomic<std::uint64_t> discarded_incomplete_writes{};
    std::atomic<std::uint64_t> send_timeouts{};
    std::atomic<std::uint64_t> receive_timeouts{};
    std::atomic<std::uint32_t> waiting_senders{};
    std::atomic<std::uint32_t> waiting_receivers{};
};

struct alignas(managed_cache_line) managed_shared_header {
    std::uint64_t magic{};
    std::uint32_t layout_version{};
    std::uint32_t header_size{};
    std::uint64_t mapping_size{};
    std::uint64_t protocol_id_value{};
    std::uint32_t protocol_version{};
    std::uint32_t reserved{};
    std::uint64_t message_capacity{};
    std::uint64_t max_message_size{};
    std::uint64_t slots_offset{};
    std::uint64_t payload_offset{};
    std::uint64_t payload_stride{};
    std::uint64_t slot_size{};
    std::uint64_t mutex_size{};
    std::uint64_t mutex_alignment{};

    alignas(managed_cache_line) std::atomic<std::uint32_t> initialization{};
    std::atomic<std::uint32_t> state{};
    std::atomic<std::uint32_t> reason{};
    std::atomic<std::uint32_t> event_epoch{};

    alignas(managed_cache_line) ::pthread_mutex_t mutex{};

    alignas(managed_cache_line) std::uint64_t next_sequence{1};
    std::uint64_t next_slot_hint{};
    std::uint64_t free_slots{};
    std::uint64_t writing_messages{};
    std::uint64_t ready_messages{};

    alignas(managed_cache_line) managed_counters counters{};
};

struct managed_layout {
    std::size_t mapping_size{};
    std::size_t slots_offset{};
    std::size_t payload_offset{};
    std::size_t payload_stride{};
};

[[nodiscard]] constexpr std::size_t managed_align_up(
    std::size_t value,
    std::size_t alignment) noexcept {
    return (value + alignment - 1) & ~(alignment - 1);
}

[[nodiscard]] inline managed_layout checked_managed_layout(
    std::size_t capacity,
    std::size_t max_message_size) {
    if (capacity == 0 || max_message_size == 0) {
        throw std::invalid_argument(
            "shmchan: managed channel capacity and max message size must be positive");
    }
    if (max_message_size > std::numeric_limits<std::uint32_t>::max()) {
        throw std::length_error("shmchan: managed message size exceeds uint32_t");
    }
    if (max_message_size >
        std::numeric_limits<std::size_t>::max() - (managed_cache_line - 1)) {
        throw std::length_error("shmchan: managed payload alignment overflows size_t");
    }

    managed_layout result{};
    result.payload_stride = managed_align_up(max_message_size, managed_cache_line);
    result.slots_offset =
        managed_align_up(sizeof(managed_shared_header), alignof(managed_message_slot));
    if (capacity >
        (std::numeric_limits<std::size_t>::max() - result.slots_offset) /
            sizeof(managed_message_slot)) {
        throw std::length_error("shmchan: managed descriptor area is too large");
    }
    result.payload_offset = managed_align_up(
        result.slots_offset + capacity * sizeof(managed_message_slot),
        managed_cache_line);
    if (capacity >
        (std::numeric_limits<std::size_t>::max() - result.payload_offset) /
            result.payload_stride) {
        throw std::length_error("shmchan: managed payload area is too large");
    }
    result.mapping_size = result.payload_offset + capacity * result.payload_stride;
    if (result.mapping_size >
        static_cast<std::uintmax_t>(std::numeric_limits<::off_t>::max())) {
        throw std::length_error("shmchan: managed mapping size overflows off_t");
    }
    return result;
}

[[nodiscard]] inline std::string managed_object_name(std::string_view base_name) {
    auto result = normalize_name(base_name);
    result += ".shmchan.managed";
    if (result.size() > 250) {
        throw std::invalid_argument("shmchan: managed channel name is too long");
    }
    return result;
}

struct managed_mapping {
    void* address{};
    std::size_t size{};
    managed_shared_header* header{};
    managed_message_slot* slots{};
    std::byte* payloads{};
    std::string base_name{};
    std::string object_name{};

    ~managed_mapping() {
        if (address != nullptr) {
            (void)::munmap(address, size);
        }
    }

    [[nodiscard]] std::span<std::byte> payload(std::size_t index) const noexcept {
        return {
            payloads + index * static_cast<std::size_t>(header->payload_stride),
            static_cast<std::size_t>(header->max_message_size),
        };
    }
};

inline void signal_managed_event(managed_shared_header& header) noexcept {
    header.event_epoch.fetch_add(1, std::memory_order_release);
    futex_wake(header.event_epoch, INT_MAX);
}

inline void mark_managed_broken(
    managed_shared_header& header,
    break_reason reason) noexcept {
    header.reason.store(static_cast<std::uint32_t>(reason), std::memory_order_release);
    header.state.store(
        static_cast<std::uint32_t>(managed_channel_state::broken),
        std::memory_order_release);
    signal_managed_event(header);
}

inline void reset_managed_slot(managed_message_slot& slot) noexcept {
    slot.payload_size = 0;
    slot.sequence = 0;
    slot.state.store(
        static_cast<std::uint32_t>(managed_slot_state::free),
        std::memory_order_release);
}

[[nodiscard]] inline bool recover_managed_slots(managed_mapping& mapping) noexcept {
    auto& header = *mapping.header;
    std::uint64_t free_count = 0;
    std::uint64_t writing_count = 0;
    std::uint64_t ready_count = 0;
    std::uint64_t discarded = 0;

    for (std::size_t index = 0; index < header.message_capacity; ++index) {
        auto& slot = mapping.slots[index];
        switch (static_cast<managed_slot_state>(
            slot.state.load(std::memory_order_acquire))) {
        case managed_slot_state::free:
            ++free_count;
            break;
        case managed_slot_state::writing:
            // send() keeps the robust mutex for the whole copy. Therefore a WRITING
            // slot seen after owner death can only belong to the dead mutex owner.
            reset_managed_slot(slot);
            ++free_count;
            ++discarded;
            break;
        case managed_slot_state::ready:
            if (slot.payload_size > header.max_message_size || slot.sequence == 0) {
                mark_managed_broken(header, break_reason::corrupt_data);
                return false;
            }
            ++ready_count;
            break;
        default:
            mark_managed_broken(header, break_reason::corrupt_data);
            return false;
        }
    }

    header.free_slots = free_count;
    header.writing_messages = writing_count;
    header.ready_messages = ready_count;
    header.counters.owner_death_recoveries.fetch_add(1, std::memory_order_relaxed);
    header.counters.discarded_incomplete_writes.fetch_add(
        discarded, std::memory_order_relaxed);
    signal_managed_event(header);
    return true;
}

[[nodiscard]] inline bool validate_managed_slots(managed_mapping& mapping) noexcept {
    auto& header = *mapping.header;
    std::uint64_t free_count = 0;
    std::uint64_t writing_count = 0;
    std::uint64_t ready_count = 0;
    for (std::size_t index = 0; index < header.message_capacity; ++index) {
        const auto& slot = mapping.slots[index];
        switch (static_cast<managed_slot_state>(
            slot.state.load(std::memory_order_acquire))) {
        case managed_slot_state::free:
            ++free_count;
            break;
        case managed_slot_state::writing:
            ++writing_count;
            break;
        case managed_slot_state::ready:
            if (slot.payload_size > header.max_message_size || slot.sequence == 0) {
                mark_managed_broken(header, break_reason::corrupt_data);
                return false;
            }
            ++ready_count;
            break;
        default:
            mark_managed_broken(header, break_reason::corrupt_data);
            return false;
        }
    }
    if (writing_count != 0 || free_count != header.free_slots ||
        writing_count != header.writing_messages ||
        ready_count != header.ready_messages ||
        free_count + writing_count + ready_count != header.message_capacity) {
        mark_managed_broken(header, break_reason::corrupt_data);
        return false;
    }
    return true;
}

enum class managed_lock_mode { blocking, nonblocking, timed };

class managed_mutex_guard {
public:
    managed_mutex_guard(
        managed_mapping& mapping,
        managed_lock_mode mode,
        std::optional<monotonic_clock::time_point> deadline = std::nullopt)
        : mapping_(std::addressof(mapping)) {
        unsigned contention_count = 0;
        for (;;) {
            const int result = mode == managed_lock_mode::blocking
                                   ? ::pthread_mutex_lock(
                                         std::addressof(mapping_->header->mutex))
                                   : ::pthread_mutex_trylock(
                                         std::addressof(mapping_->header->mutex));
            if (result == 0) {
                owns_ = true;
                status_ = channel_status::success;
                return;
            }
            if (result == EOWNERDEAD) {
                owns_ = true;
                const int consistent =
                    ::pthread_mutex_consistent(std::addressof(mapping_->header->mutex));
                if (consistent != 0) {
                    mark_managed_broken(
                        *mapping_->header, break_reason::robust_mutex_not_recoverable);
                    status_ = channel_status::broken;
                    return;
                }
                status_ = recover_managed_slots(*mapping_)
                              ? channel_status::success
                              : channel_status::broken;
                return;
            }
            if (result == ENOTRECOVERABLE) {
                mark_managed_broken(
                    *mapping_->header, break_reason::robust_mutex_not_recoverable);
                status_ = channel_status::broken;
                return;
            }
            if (result != EBUSY) {
                throw std::system_error(
                    result,
                    std::generic_category(),
                    "pthread_mutex_lock(managed channel)");
            }
            if (mode == managed_lock_mode::nonblocking) {
                status_ = channel_status::would_block;
                return;
            }
            if (mode == managed_lock_mode::timed &&
                deadline.has_value() && monotonic_clock::now() >= *deadline) {
                status_ = channel_status::timed_out;
                return;
            }
            if (contention_count++ < 64) {
                std::this_thread::yield();
            } else {
                const ::timespec pause{0, 50'000};
                (void)::nanosleep(std::addressof(pause), nullptr);
            }
        }
    }

    ~managed_mutex_guard() { unlock(); }

    managed_mutex_guard(const managed_mutex_guard&) = delete;
    managed_mutex_guard& operator=(const managed_mutex_guard&) = delete;

    [[nodiscard]] channel_status status() const noexcept { return status_; }
    [[nodiscard]] bool owns_lock() const noexcept { return owns_; }

    void unlock() noexcept {
        if (owns_) {
            owns_ = false;
            (void)::pthread_mutex_unlock(std::addressof(mapping_->header->mutex));
        }
    }

private:
    managed_mapping* mapping_{};
    channel_status status_{channel_status::would_block};
    bool owns_{};
};

[[nodiscard]] inline monotonic_clock::time_point managed_poll_deadline(
    std::optional<monotonic_clock::time_point> operation_deadline) noexcept {
    const auto poll_deadline = monotonic_clock::now() + managed_recovery_poll_interval;
    return operation_deadline.has_value() && *operation_deadline < poll_deadline
               ? *operation_deadline
               : poll_deadline;
}

[[nodiscard]] inline std::shared_ptr<managed_mapping> finish_managed_mapping(
    mapped_region& region,
    std::size_t mapping_size,
    std::string base_name,
    std::string object_name) {
    auto result = std::make_shared<managed_mapping>();
    result->address = region.release();
    result->size = mapping_size;
    result->header = static_cast<managed_shared_header*>(result->address);
    result->slots = reinterpret_cast<managed_message_slot*>(
        static_cast<std::byte*>(result->address) + result->header->slots_offset);
    result->payloads =
        static_cast<std::byte*>(result->address) + result->header->payload_offset;
    result->base_name = std::move(base_name);
    result->object_name = std::move(object_name);
    return result;
}

[[nodiscard]] inline std::shared_ptr<managed_mapping> initialize_managed_mapping_locked(
    int fd,
    std::string base_name,
    std::string object_name,
    const managed_channel_options& options) {
    const auto layout =
        checked_managed_layout(options.message_capacity, options.max_message_size);
    if (::ftruncate(fd, static_cast<::off_t>(layout.mapping_size)) != 0) {
        throw_errno("ftruncate(managed channel)", object_name);
    }
    void* address = ::mmap(
        nullptr,
        layout.mapping_size,
        PROT_READ | PROT_WRITE,
        MAP_SHARED,
        fd,
        0);
    if (address == MAP_FAILED) {
        throw_errno("mmap(create managed channel)", object_name);
    }
    mapped_region region(address, layout.mapping_size);
    auto* header = std::construct_at(static_cast<managed_shared_header*>(address));
    header->magic = managed_simple_magic;
    header->layout_version = managed_simple_version;
    header->header_size = sizeof(managed_shared_header);
    header->mapping_size = layout.mapping_size;
    header->protocol_id_value = options.protocol.id;
    header->protocol_version = options.protocol.version;
    header->message_capacity = options.message_capacity;
    header->max_message_size = options.max_message_size;
    header->slots_offset = layout.slots_offset;
    header->payload_offset = layout.payload_offset;
    header->payload_stride = layout.payload_stride;
    header->slot_size = sizeof(managed_message_slot);
    header->mutex_size = sizeof(::pthread_mutex_t);
    header->mutex_alignment = alignof(::pthread_mutex_t);
    header->initialization.store(0, std::memory_order_relaxed);
    header->state.store(
        static_cast<std::uint32_t>(managed_channel_state::initializing),
        std::memory_order_relaxed);
    header->reason.store(
        static_cast<std::uint32_t>(break_reason::none), std::memory_order_relaxed);
    header->event_epoch.store(0, std::memory_order_relaxed);
    header->next_sequence = 1;
    header->next_slot_hint = 0;
    header->free_slots = options.message_capacity;
    header->writing_messages = 0;
    header->ready_messages = 0;

    auto* slots = reinterpret_cast<managed_message_slot*>(
        static_cast<std::byte*>(address) + layout.slots_offset);
    for (std::size_t index = 0; index < options.message_capacity; ++index) {
        (void)std::construct_at(slots + index);
    }

    ::pthread_mutexattr_t attributes{};
    int result = ::pthread_mutexattr_init(std::addressof(attributes));
    if (result != 0) {
        throw std::system_error(
            result, std::generic_category(), "pthread_mutexattr_init");
    }
    try {
        result = ::pthread_mutexattr_setpshared(
            std::addressof(attributes), PTHREAD_PROCESS_SHARED);
        if (result != 0) {
            throw std::system_error(
                result, std::generic_category(), "pthread_mutexattr_setpshared");
        }
        result = ::pthread_mutexattr_setrobust(
            std::addressof(attributes), PTHREAD_MUTEX_ROBUST);
        if (result != 0) {
            throw std::system_error(
                result, std::generic_category(), "pthread_mutexattr_setrobust");
        }
        result = ::pthread_mutex_init(
            std::addressof(header->mutex), std::addressof(attributes));
        if (result != 0) {
            throw std::system_error(
                result, std::generic_category(), "pthread_mutex_init");
        }
    } catch (...) {
        (void)::pthread_mutexattr_destroy(std::addressof(attributes));
        throw;
    }
    (void)::pthread_mutexattr_destroy(std::addressof(attributes));

    header->state.store(
        static_cast<std::uint32_t>(managed_channel_state::healthy),
        std::memory_order_release);
    header->initialization.store(
        managed_initialization_ready, std::memory_order_release);
    return finish_managed_mapping(
        region, layout.mapping_size, std::move(base_name), std::move(object_name));
}

inline void validate_managed_mapping(
    managed_shared_header& header,
    std::size_t mapping_size,
    std::string_view object_name,
    protocol_descriptor protocol) {
    if (header.magic != managed_simple_magic ||
        header.layout_version != managed_simple_version ||
        header.header_size != sizeof(managed_shared_header) ||
        header.mapping_size != mapping_size ||
        header.slot_size != sizeof(managed_message_slot) ||
        header.mutex_size != sizeof(::pthread_mutex_t) ||
        header.mutex_alignment != alignof(::pthread_mutex_t) ||
        header.initialization.load(std::memory_order_acquire) !=
            managed_initialization_ready) {
        throw std::runtime_error(
            "shmchan: incompatible or incomplete managed channel: " +
            std::string(object_name));
    }
    const auto stored_state = header.state.load(std::memory_order_acquire);
    const auto stored_reason = header.reason.load(std::memory_order_acquire);
    if (stored_state > static_cast<std::uint32_t>(managed_channel_state::destroying) ||
        stored_reason > static_cast<std::uint32_t>(break_reason::sequence_exhausted)) {
        throw std::runtime_error(
            "shmchan: invalid managed channel state: " + std::string(object_name));
    }
    const auto expected = checked_managed_layout(
        static_cast<std::size_t>(header.message_capacity),
        static_cast<std::size_t>(header.max_message_size));
    if (expected.mapping_size != mapping_size ||
        expected.slots_offset != header.slots_offset ||
        expected.payload_offset != header.payload_offset ||
        expected.payload_stride != header.payload_stride) {
        throw std::runtime_error(
            "shmchan: corrupt managed channel layout: " + std::string(object_name));
    }
    if (header.protocol_id_value != protocol.id ||
        header.protocol_version != protocol.version) {
        throw managed_channel_error(
            channel_status::protocol_mismatch,
            "shmchan: managed channel protocol mismatch: " +
                std::string(object_name));
    }
}

[[nodiscard]] inline std::shared_ptr<managed_mapping> map_managed_fd_locked(
    int fd,
    std::string base_name,
    std::string object_name,
    protocol_descriptor protocol) {
    struct stat attributes {};
    if (::fstat(fd, std::addressof(attributes)) != 0) {
        throw_errno("fstat(managed channel)", object_name);
    }
    if (attributes.st_size < 0 ||
        static_cast<std::uintmax_t>(attributes.st_size) < sizeof(managed_shared_header) ||
        static_cast<std::uintmax_t>(attributes.st_size) >
            std::numeric_limits<std::size_t>::max()) {
        throw std::runtime_error(
            "shmchan: incomplete managed channel: " + object_name);
    }
    const auto mapping_size = static_cast<std::size_t>(attributes.st_size);
    void* address = ::mmap(
        nullptr,
        mapping_size,
        PROT_READ | PROT_WRITE,
        MAP_SHARED,
        fd,
        0);
    if (address == MAP_FAILED) {
        throw_errno("mmap(open managed channel)", object_name);
    }
    mapped_region region(address, mapping_size);
    auto* header = static_cast<managed_shared_header*>(address);
    validate_managed_mapping(*header, mapping_size, object_name, protocol);
    return finish_managed_mapping(
        region, mapping_size, std::move(base_name), std::move(object_name));
}

[[nodiscard]] inline std::shared_ptr<managed_mapping> create_managed_mapping(
    std::string_view name,
    const managed_channel_options& options) {
    auto base_name = normalize_name(name);
    auto object_name = managed_object_name(base_name);
    const int raw_fd = ::shm_open(
        object_name.c_str(),
        O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC,
        options.permissions);
    if (raw_fd < 0) {
        throw_errno("shm_open(create managed channel)", object_name);
    }
    unique_fd fd(raw_fd);
    bool unlink_on_failure = true;
    try {
        lock_fd(fd.get(), LOCK_EX);
        auto result = initialize_managed_mapping_locked(
            fd.get(), std::move(base_name), object_name, options);
        lock_fd(fd.get(), LOCK_UN);
        unlink_on_failure = false;
        return result;
    } catch (...) {
        if (unlink_on_failure) {
            (void)::shm_unlink(object_name.c_str());
        }
        throw;
    }
}

[[nodiscard]] inline std::shared_ptr<managed_mapping> open_managed_mapping(
    std::string_view name,
    protocol_descriptor protocol) {
    auto base_name = normalize_name(name);
    auto object_name = managed_object_name(base_name);
    const int raw_fd = ::shm_open(object_name.c_str(), O_RDWR | O_CLOEXEC, 0);
    if (raw_fd < 0) {
        throw_errno("shm_open(open managed channel)", object_name);
    }
    unique_fd fd(raw_fd);
    lock_fd(fd.get(), LOCK_SH);
    auto result = map_managed_fd_locked(
        fd.get(), std::move(base_name), std::move(object_name), protocol);
    lock_fd(fd.get(), LOCK_UN);
    return result;
}

[[nodiscard]] inline std::shared_ptr<managed_mapping> open_or_create_managed_mapping(
    std::string_view name,
    const managed_channel_options& options) {
    auto base_name = normalize_name(name);
    auto object_name = managed_object_name(base_name);
    const int raw_fd = ::shm_open(
        object_name.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, options.permissions);
    if (raw_fd < 0) {
        throw_errno("shm_open(open_or_create managed channel)", object_name);
    }
    unique_fd fd(raw_fd);
    lock_fd(fd.get(), LOCK_EX);

    struct stat attributes {};
    if (::fstat(fd.get(), std::addressof(attributes)) != 0) {
        throw_errno("fstat(open_or_create managed channel)", object_name);
    }

    std::shared_ptr<managed_mapping> result;
    bool needs_initialization = attributes.st_size == 0;
    if (!needs_initialization &&
        static_cast<std::uintmax_t>(attributes.st_size) >= sizeof(managed_shared_header)) {
        if (static_cast<std::uintmax_t>(attributes.st_size) >
            std::numeric_limits<std::size_t>::max()) {
            throw std::runtime_error(
                "shmchan: managed channel object is too large: " + object_name);
        }
        const auto current_size = static_cast<std::size_t>(attributes.st_size);
        void* probe = ::mmap(
            nullptr,
            current_size,
            PROT_READ | PROT_WRITE,
            MAP_SHARED,
            fd.get(),
            0);
        if (probe == MAP_FAILED) {
            throw_errno("mmap(probe managed channel)", object_name);
        }
        auto* header = static_cast<managed_shared_header*>(probe);
        needs_initialization =
            header->initialization.load(std::memory_order_acquire) !=
            managed_initialization_ready;
        (void)::munmap(probe, current_size);
    } else if (!needs_initialization) {
        needs_initialization = true;
    }

    if (needs_initialization) {
        if (::ftruncate(fd.get(), 0) != 0) {
            throw_errno("ftruncate(reset managed channel)", object_name);
        }
        result = initialize_managed_mapping_locked(
            fd.get(), std::move(base_name), object_name, options);
    } else {
        result = map_managed_fd_locked(
            fd.get(), std::move(base_name), object_name, options.protocol);
        if (result->header->message_capacity != options.message_capacity ||
            result->header->max_message_size != options.max_message_size) {
            throw std::invalid_argument(
                "shmchan: open_or_create options do not match the existing channel");
        }
    }
    lock_fd(fd.get(), LOCK_UN);
    return result;
}

} // namespace detail

class managed_byte_channel {
public:
    using buffer_type = std::vector<std::byte>;

    managed_byte_channel() noexcept = default;
    ~managed_byte_channel() = default;

    managed_byte_channel(const managed_byte_channel&) = delete;
    managed_byte_channel& operator=(const managed_byte_channel&) = delete;
    managed_byte_channel(managed_byte_channel&&) noexcept = default;
    managed_byte_channel& operator=(managed_byte_channel&&) noexcept = default;

    [[nodiscard]] static managed_byte_channel create(
        std::string_view name,
        managed_channel_options options = {}) {
        return managed_byte_channel(
            detail::create_managed_mapping(name, options));
    }

    [[nodiscard]] static managed_byte_channel open(
        std::string_view name,
        managed_open_options options = {}) {
        return managed_byte_channel(
            detail::open_managed_mapping(name, options.protocol));
    }

    [[nodiscard]] static managed_byte_channel open_or_create(
        std::string_view name,
        managed_channel_options options = {}) {
        return managed_byte_channel(
            detail::open_or_create_managed_mapping(name, options));
    }

    [[nodiscard]] static bool unlink(std::string_view name) {
        const auto object_name = detail::managed_object_name(name);
        const int raw_fd = ::shm_open(object_name.c_str(), O_RDWR | O_CLOEXEC, 0);
        if (raw_fd < 0) {
            if (errno == ENOENT) {
                return false;
            }
            detail::throw_errno("shm_open(unlink managed channel)", object_name);
        }
        detail::unique_fd fd(raw_fd);
        detail::lock_fd(fd.get(), LOCK_EX);

        struct stat attributes {};
        if (::fstat(fd.get(), std::addressof(attributes)) == 0 &&
            attributes.st_size >= static_cast<::off_t>(sizeof(detail::managed_shared_header)) &&
            static_cast<std::uintmax_t>(attributes.st_size) <=
                std::numeric_limits<std::size_t>::max()) {
            const auto size = static_cast<std::size_t>(attributes.st_size);
            void* address = ::mmap(
                nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd.get(), 0);
            if (address != MAP_FAILED) {
                auto* header = static_cast<detail::managed_shared_header*>(address);
                if (header->magic == detail::managed_simple_magic &&
                    header->mapping_size == size) {
                    header->state.store(
                        static_cast<std::uint32_t>(managed_channel_state::destroying),
                        std::memory_order_release);
                    detail::signal_managed_event(*header);
                }
                (void)::munmap(address, size);
            }
        }

        const int result = ::shm_unlink(object_name.c_str());
        const int saved_error = errno;
        detail::lock_fd(fd.get(), LOCK_UN);
        if (result == 0) {
            return true;
        }
        if (saved_error == ENOENT) {
            return false;
        }
        errno = saved_error;
        detail::throw_errno("shm_unlink(managed channel)", object_name);
    }

    [[nodiscard]] bool unlink() const {
        require_valid();
        return unlink(mapping_->base_name);
    }

    [[nodiscard]] bool valid() const noexcept { return mapping_ != nullptr; }

    [[nodiscard]] const std::string& name() const {
        require_valid();
        return mapping_->base_name;
    }

    [[nodiscard]] std::size_t capacity() const {
        require_valid();
        return static_cast<std::size_t>(mapping_->header->message_capacity);
    }

    [[nodiscard]] std::size_t max_message_size() const {
        require_valid();
        return static_cast<std::size_t>(mapping_->header->max_message_size);
    }

    [[nodiscard]] protocol_descriptor protocol() const {
        require_valid();
        return {
            mapping_->header->protocol_id_value,
            mapping_->header->protocol_version,
        };
    }

    [[nodiscard]] managed_channel_state state() const {
        require_valid();
        return static_cast<managed_channel_state>(
            mapping_->header->state.load(std::memory_order_acquire));
    }

    [[nodiscard]] break_reason reason() const {
        require_valid();
        return static_cast<break_reason>(
            mapping_->header->reason.load(std::memory_order_acquire));
    }

    [[nodiscard]] bool is_closed() const {
        return state() == managed_channel_state::closed;
    }

    [[nodiscard]] channel_status try_send(
        std::span<const std::byte> message) {
        return send_impl(
            message, detail::managed_lock_mode::nonblocking, std::nullopt);
    }

    [[nodiscard]] channel_status try_send(std::string_view message) {
        return try_send(as_bytes(message));
    }

    [[nodiscard]] channel_status send(
        std::span<const std::byte> message) {
        return send_impl(
            message, detail::managed_lock_mode::blocking, std::nullopt);
    }

    [[nodiscard]] channel_status send(std::string_view message) {
        return send(as_bytes(message));
    }

    [[nodiscard]] channel_status send_for(
        std::span<const std::byte> message,
        std::chrono::nanoseconds timeout) {
        return send_impl(
            message,
            detail::managed_lock_mode::timed,
            detail::make_deadline(timeout));
    }

    [[nodiscard]] channel_status send_for(
        std::string_view message,
        std::chrono::nanoseconds timeout) {
        return send_for(as_bytes(message), timeout);
    }

    [[nodiscard]] receive_result<buffer_type> try_receive() {
        return receive_impl(detail::managed_lock_mode::nonblocking, std::nullopt);
    }

    [[nodiscard]] receive_result<buffer_type> receive() {
        return receive_impl(detail::managed_lock_mode::blocking, std::nullopt);
    }

    [[nodiscard]] receive_result<buffer_type> receive_for(
        std::chrono::nanoseconds timeout) {
        return receive_impl(
            detail::managed_lock_mode::timed,
            detail::make_deadline(timeout));
    }

    [[nodiscard]] bool close() {
        require_valid();
        detail::managed_mutex_guard lock(
            *mapping_, detail::managed_lock_mode::blocking);
        if (lock.status() != channel_status::success) {
            return false;
        }
        auto& header = *mapping_->header;
        const auto current = static_cast<managed_channel_state>(
            header.state.load(std::memory_order_acquire));
        if (current != managed_channel_state::healthy) {
            return false;
        }
        header.state.store(
            static_cast<std::uint32_t>(managed_channel_state::closed),
            std::memory_order_release);
        lock.unlock();
        detail::signal_managed_event(header);
        return true;
    }

    [[nodiscard]] managed_channel_stats stats() const {
        require_valid();
        managed_channel_stats result{};
        auto& header = *mapping_->header;
        result.state = static_cast<managed_channel_state>(
            header.state.load(std::memory_order_acquire));
        result.reason = static_cast<break_reason>(
            header.reason.load(std::memory_order_acquire));
        result.message_capacity = static_cast<std::size_t>(header.message_capacity);
        result.max_message_size = static_cast<std::size_t>(header.max_message_size);

        detail::managed_mutex_guard lock(
            *mapping_, detail::managed_lock_mode::blocking);
        if (lock.status() == channel_status::success) {
            result.free_slots = static_cast<std::size_t>(header.free_slots);
            result.writing_messages = static_cast<std::size_t>(header.writing_messages);
            result.ready_messages = static_cast<std::size_t>(header.ready_messages);
        }
        result.waiting_senders =
            header.counters.waiting_senders.load(std::memory_order_relaxed);
        result.waiting_receivers =
            header.counters.waiting_receivers.load(std::memory_order_relaxed);
        result.sent_messages =
            header.counters.sent_messages.load(std::memory_order_relaxed);
        result.sent_bytes = header.counters.sent_bytes.load(std::memory_order_relaxed);
        result.received_messages =
            header.counters.received_messages.load(std::memory_order_relaxed);
        result.received_bytes =
            header.counters.received_bytes.load(std::memory_order_relaxed);
        result.owner_death_recoveries =
            header.counters.owner_death_recoveries.load(std::memory_order_relaxed);
        result.discarded_incomplete_writes =
            header.counters.discarded_incomplete_writes.load(std::memory_order_relaxed);
        result.send_timeouts =
            header.counters.send_timeouts.load(std::memory_order_relaxed);
        result.receive_timeouts =
            header.counters.receive_timeouts.load(std::memory_order_relaxed);
        return result;
    }

private:
    explicit managed_byte_channel(
        std::shared_ptr<detail::managed_mapping> mapping) noexcept
        : mapping_(std::move(mapping)) {}

    void require_valid() const {
        if (mapping_ == nullptr) {
            throw std::logic_error("shmchan: operation on an empty managed channel");
        }
    }

    [[nodiscard]] static std::span<const std::byte> as_bytes(
        std::string_view value) noexcept {
        return {
            reinterpret_cast<const std::byte*>(value.data()),
            value.size(),
        };
    }

    [[nodiscard]] channel_status operation_state() const noexcept {
        const auto current = static_cast<managed_channel_state>(
            mapping_->header->state.load(std::memory_order_acquire));
        if (current == managed_channel_state::healthy) {
            return channel_status::success;
        }
        if (current == managed_channel_state::closed) {
            return channel_status::closed;
        }
        return channel_status::broken;
    }

    [[nodiscard]] channel_status send_impl(
        std::span<const std::byte> message,
        detail::managed_lock_mode mode,
        std::optional<detail::monotonic_clock::time_point> deadline) {
        require_valid();
        auto& header = *mapping_->header;
        if (message.size() > header.max_message_size) {
            return channel_status::message_too_large;
        }

        for (;;) {
            const auto before_lock = operation_state();
            if (before_lock != channel_status::success) {
                return before_lock;
            }
            detail::managed_mutex_guard lock(*mapping_, mode, deadline);
            if (lock.status() != channel_status::success) {
                if (lock.status() == channel_status::timed_out) {
                    header.counters.send_timeouts.fetch_add(1, std::memory_order_relaxed);
                }
                return lock.status();
            }
            const auto locked_state = operation_state();
            if (locked_state != channel_status::success) {
                return locked_state;
            }
            if (!detail::validate_managed_slots(*mapping_)) {
                return channel_status::broken;
            }

            if (header.free_slots != 0) {
                if (header.next_sequence == std::numeric_limits<std::uint64_t>::max()) {
                    detail::mark_managed_broken(header, break_reason::sequence_exhausted);
                    return channel_status::broken;
                }

                const auto capacity = static_cast<std::size_t>(header.message_capacity);
                const auto start = static_cast<std::size_t>(header.next_slot_hint % capacity);
                std::size_t selected = capacity;
                for (std::size_t offset = 0; offset < capacity; ++offset) {
                    const auto index = (start + offset) % capacity;
                    if (mapping_->slots[index].state.load(std::memory_order_acquire) ==
                        static_cast<std::uint32_t>(detail::managed_slot_state::free)) {
                        selected = index;
                        break;
                    }
                }
                if (selected == capacity) {
                    detail::mark_managed_broken(header, break_reason::corrupt_data);
                    return channel_status::broken;
                }

                auto& slot = mapping_->slots[selected];
                slot.state.store(
                    static_cast<std::uint32_t>(detail::managed_slot_state::writing),
                    std::memory_order_release);
                --header.free_slots;
                ++header.writing_messages;
                slot.payload_size = 0;
                slot.sequence = header.next_sequence++;

                if (!message.empty()) {
                    std::memcpy(
                        mapping_->payload(selected).data(),
                        message.data(),
                        message.size());
                }
                slot.payload_size = static_cast<std::uint32_t>(message.size());

                // READY is the publication point. Every field and payload byte is complete
                // before this store while the process-shared mutex is still held.
                slot.state.store(
                    static_cast<std::uint32_t>(detail::managed_slot_state::ready),
                    std::memory_order_release);
                --header.writing_messages;
                ++header.ready_messages;
                header.next_slot_hint = (selected + 1) % capacity;
                header.counters.sent_messages.fetch_add(1, std::memory_order_relaxed);
                header.counters.sent_bytes.fetch_add(
                    message.size(), std::memory_order_relaxed);
                lock.unlock();
                detail::signal_managed_event(header);
                return operation_state() == channel_status::broken
                           ? channel_status::broken
                           : channel_status::success;
            }

            if (mode == detail::managed_lock_mode::nonblocking) {
                return channel_status::would_block;
            }
            const auto epoch = header.event_epoch.load(std::memory_order_acquire);
            lock.unlock();
            header.counters.waiting_senders.fetch_add(1, std::memory_order_relaxed);
            detail::futex_wait_result wait_result{};
            try {
                wait_result = detail::futex_wait_until(
                    header.event_epoch,
                    epoch,
                    detail::managed_poll_deadline(deadline));
            } catch (...) {
                header.counters.waiting_senders.fetch_sub(1, std::memory_order_relaxed);
                throw;
            }
            header.counters.waiting_senders.fetch_sub(1, std::memory_order_relaxed);
            if (wait_result == detail::futex_wait_result::timed_out) {
                if (deadline.has_value() && detail::monotonic_clock::now() >= *deadline) {
                    header.counters.send_timeouts.fetch_add(1, std::memory_order_relaxed);
                    return channel_status::timed_out;
                }
            }
        }
    }

    [[nodiscard]] receive_result<buffer_type> receive_impl(
        detail::managed_lock_mode mode,
        std::optional<detail::monotonic_clock::time_point> deadline) {
        require_valid();
        auto& header = *mapping_->header;

        for (;;) {
            const auto before_lock = operation_state();
            if (before_lock == channel_status::broken) {
                return {before_lock, std::nullopt};
            }
            detail::managed_mutex_guard lock(*mapping_, mode, deadline);
            if (lock.status() != channel_status::success) {
                if (lock.status() == channel_status::timed_out) {
                    header.counters.receive_timeouts.fetch_add(1, std::memory_order_relaxed);
                }
                return {lock.status(), std::nullopt};
            }
            if (!detail::validate_managed_slots(*mapping_)) {
                return {channel_status::broken, std::nullopt};
            }

            std::size_t selected = static_cast<std::size_t>(header.message_capacity);
            std::uint64_t selected_sequence = std::numeric_limits<std::uint64_t>::max();
            for (std::size_t index = 0; index < header.message_capacity; ++index) {
                const auto& slot = mapping_->slots[index];
                if (slot.state.load(std::memory_order_acquire) ==
                        static_cast<std::uint32_t>(detail::managed_slot_state::ready) &&
                    slot.sequence < selected_sequence) {
                    selected = index;
                    selected_sequence = slot.sequence;
                }
            }

            if (selected != header.message_capacity) {
                auto& slot = mapping_->slots[selected];
                if (slot.payload_size > header.max_message_size || slot.sequence == 0 ||
                    header.ready_messages == 0) {
                    detail::mark_managed_broken(header, break_reason::corrupt_data);
                    return {channel_status::broken, std::nullopt};
                }

                buffer_type payload(static_cast<std::size_t>(slot.payload_size));
                if (!payload.empty()) {
                    std::memcpy(
                        payload.data(),
                        mapping_->payload(selected).data(),
                        payload.size());
                }
                // The slot remains READY during allocation and memcpy. It becomes FREE only
                // after the complete message exists in process-private memory.
                detail::reset_managed_slot(slot);
                --header.ready_messages;
                ++header.free_slots;
                header.counters.received_messages.fetch_add(1, std::memory_order_relaxed);
                header.counters.received_bytes.fetch_add(
                    payload.size(), std::memory_order_relaxed);
                lock.unlock();
                detail::signal_managed_event(header);
                return {
                    channel_status::success,
                    std::move(payload),
                };
            }

            const auto locked_state = operation_state();
            if (locked_state == channel_status::closed) {
                return {channel_status::closed, std::nullopt};
            }
            if (locked_state == channel_status::broken) {
                return {channel_status::broken, std::nullopt};
            }
            if (mode == detail::managed_lock_mode::nonblocking) {
                return {channel_status::would_block, std::nullopt};
            }

            const auto epoch = header.event_epoch.load(std::memory_order_acquire);
            lock.unlock();
            header.counters.waiting_receivers.fetch_add(1, std::memory_order_relaxed);
            detail::futex_wait_result wait_result{};
            try {
                wait_result = detail::futex_wait_until(
                    header.event_epoch,
                    epoch,
                    detail::managed_poll_deadline(deadline));
            } catch (...) {
                header.counters.waiting_receivers.fetch_sub(1, std::memory_order_relaxed);
                throw;
            }
            header.counters.waiting_receivers.fetch_sub(1, std::memory_order_relaxed);
            if (wait_result == detail::futex_wait_result::timed_out) {
                if (deadline.has_value() && detail::monotonic_clock::now() >= *deadline) {
                    header.counters.receive_timeouts.fetch_add(1, std::memory_order_relaxed);
                    return {channel_status::timed_out, std::nullopt};
                }
            }
        }
    }

    std::shared_ptr<detail::managed_mapping> mapping_{};
};

} // namespace shmchan
