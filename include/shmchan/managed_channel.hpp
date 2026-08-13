#pragma once

#include <shmchan/channel.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <climits>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

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

enum class participant_role : std::uint32_t {
    producer = 1,
    consumer = 2,
    both = 3,
    supervisor = 7,
};

[[nodiscard]] constexpr bool valid_participant_role(participant_role role) noexcept {
    return role == participant_role::producer || role == participant_role::consumer ||
           role == participant_role::both || role == participant_role::supervisor;
}

enum class managed_channel_state : std::uint32_t {
    initializing = 0,
    healthy = 1,
    broken = 2,
    closed = 3,
    breaking = 4,
    destroying = 5,
    replaying = 6,
};

class managed_channel_error : public std::runtime_error {
public:
    managed_channel_error(channel_status code, std::string message)
        : std::runtime_error(std::move(message)), code_(code) {}

    [[nodiscard]] channel_status code() const noexcept { return code_; }

private:
    channel_status code_;
};

enum class break_reason : std::uint32_t {
    none = 0,
    participant_timeout = 1,
    reservation_timeout = 2,
    robust_mutex_owner_died = 3,
    robust_mutex_not_recoverable = 4,
    manual = 5,
    replay_failed = 6,
    corrupt_data = 7,
    sequence_exhausted = 8,
    mutex_lock_timeout = 9,
};

[[nodiscard]] constexpr std::string_view to_string(managed_channel_state state) noexcept {
    switch (state) {
    case managed_channel_state::initializing:
        return "initializing";
    case managed_channel_state::healthy:
        return "healthy";
    case managed_channel_state::broken:
        return "broken";
    case managed_channel_state::closed:
        return "closed";
    case managed_channel_state::breaking:
        return "breaking";
    case managed_channel_state::destroying:
        return "destroying";
    case managed_channel_state::replaying:
        return "replaying";
    }
    return "unknown";
}

[[nodiscard]] constexpr std::string_view to_string(break_reason reason) noexcept {
    switch (reason) {
    case break_reason::none:
        return "none";
    case break_reason::participant_timeout:
        return "participant_timeout";
    case break_reason::reservation_timeout:
        return "reservation_timeout";
    case break_reason::robust_mutex_owner_died:
        return "robust_mutex_owner_died";
    case break_reason::robust_mutex_not_recoverable:
        return "robust_mutex_not_recoverable";
    case break_reason::manual:
        return "manual";
    case break_reason::replay_failed:
        return "replay_failed";
    case break_reason::corrupt_data:
        return "corrupt_data";
    case break_reason::sequence_exhausted:
        return "sequence_exhausted";
    case break_reason::mutex_lock_timeout:
        return "mutex_lock_timeout";
    }
    return "unknown";
}

struct managed_channel_options {
    std::size_t message_capacity{1024};
    std::size_t max_message_size{4096};
    std::size_t max_participants{64};
    protocol_descriptor protocol{protocol_id("shmchan.raw-bytes"), 1};
    std::chrono::nanoseconds heartbeat_interval{std::chrono::milliseconds{250}};
    std::chrono::nanoseconds participant_timeout{std::chrono::seconds{3}};
    std::chrono::nanoseconds reservation_timeout{std::chrono::seconds{5}};
    std::chrono::nanoseconds acknowledgment_timeout{std::chrono::seconds{30}};
    participant_role role{participant_role::both};
    bool monitor_peers{true};
    ::mode_t permissions{0600};
};

struct managed_open_options {
    protocol_descriptor protocol{protocol_id("shmchan.raw-bytes"), 1};
    participant_role role{participant_role::both};
    bool monitor_peers{true};
};

struct managed_channel_stats {
    managed_channel_state state{managed_channel_state::initializing};
    break_reason reason{break_reason::none};
    std::uint64_t generation{};
    std::uint64_t replay_owner_session{};
    std::uint64_t failed_participant_session{};
    std::size_t message_capacity{};
    std::size_t max_message_size{};
    std::size_t free_slots{};
    std::size_t writing_messages{};
    std::size_t ready_messages{};
    std::size_t inflight_messages{};
    std::size_t active_participants{};
    std::size_t stale_participants{};
    std::size_t waiting_senders{};
    std::size_t waiting_receivers{};
    std::uint64_t sent_messages{};
    std::uint64_t sent_bytes{};
    std::uint64_t delivered_messages{};
    std::uint64_t delivered_bytes{};
    std::uint64_t acknowledged_messages{};
    std::uint64_t redelivered_messages{};
    std::uint64_t negatively_acknowledged_messages{};
    std::uint64_t cancelled_reservations{};
    std::uint64_t send_timeouts{};
    std::uint64_t receive_timeouts{};
    std::uint64_t broken_generations{};
    std::uint64_t rebuilt_generations{};
    std::uint64_t replay_failures{};
    std::chrono::nanoseconds oldest_message_age{};
};

struct managed_participant_info {
    std::size_t slot{};
    participant_role role{participant_role::both};
    ::pid_t pid{};
    std::uint64_t session{};
    std::uint64_t observed_generation{};
    bool stale{};
    std::chrono::nanoseconds registered_for{};
    std::chrono::nanoseconds heartbeat_age{};
};

struct outbound_message {
    std::span<const std::byte> payload{};
    std::uint64_t message_id{};
};

struct batch_send_result {
    channel_status code{channel_status::would_block};
    std::size_t sent{};
};

namespace detail {

inline constexpr std::uint64_t managed_control_magic = 0x53484d434d435431ULL; // SHMCMCT1
inline constexpr std::uint64_t managed_data_magic = 0x53484d434d444154ULL;    // SHMCMDAT
inline constexpr std::uint32_t managed_control_version = 1;
inline constexpr std::uint32_t managed_data_version = 1;
inline constexpr std::size_t managed_max_participants = 64;
inline constexpr std::size_t managed_cache_line = 64;
inline constexpr std::uint32_t participant_free = 0;
inline constexpr std::uint32_t participant_claiming = 1;
inline constexpr std::uint32_t participant_active = 2;
inline constexpr std::uint32_t participant_stale = 3;
inline constexpr std::uint32_t participant_releasing = 4;

static_assert(std::atomic<std::uint32_t>::is_always_lock_free);
static_assert(std::atomic<std::int32_t>::is_always_lock_free);
static_assert(std::atomic<std::uint64_t>::is_always_lock_free);

enum class managed_slot_state : std::uint32_t {
    free = 0,
    writing = 1,
    ready = 2,
    inflight = 3,
    acknowledged = 4,
};

[[nodiscard]] constexpr std::size_t managed_align_up(
    std::size_t value,
    std::size_t alignment) noexcept {
    return (value + alignment - 1) & ~(alignment - 1);
}

[[nodiscard]] inline std::uint64_t monotonic_now_ns() noexcept {
    ::timespec value{};
    if (::clock_gettime(CLOCK_MONOTONIC, &value) != 0) {
        return 0;
    }
    return static_cast<std::uint64_t>(value.tv_sec) * 1'000'000'000ULL +
           static_cast<std::uint64_t>(value.tv_nsec);
}

[[nodiscard]] constexpr std::uint64_t managed_deadline_after(
    std::uint64_t now,
    std::uint64_t duration) noexcept {
    return duration > std::numeric_limits<std::uint64_t>::max() - now
               ? std::numeric_limits<std::uint64_t>::max()
               : now + duration;
}

[[nodiscard]] constexpr std::chrono::nanoseconds managed_elapsed(
    std::uint64_t now,
    std::uint64_t then) noexcept {
    if (then == 0 || now < then) {
        return std::chrono::nanoseconds::zero();
    }
    const auto elapsed = now - then;
    const auto maximum =
        static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());
    return std::chrono::nanoseconds{
        static_cast<std::int64_t>(std::min(elapsed, maximum))};
}

[[nodiscard]] inline std::uint64_t duration_ns(
    std::chrono::nanoseconds value,
    std::string_view field) {
    if (value <= std::chrono::nanoseconds::zero()) {
        throw std::invalid_argument("shmchan: " + std::string(field) + " must be positive");
    }
    return static_cast<std::uint64_t>(value.count());
}

[[nodiscard]] inline std::uint64_t random_session_id() noexcept {
    std::uint64_t value = 0;
    const auto result = ::getrandom(std::addressof(value), sizeof(value), GRND_NONBLOCK);
    if (result == static_cast<ssize_t>(sizeof(value)) && value != 0) {
        return value;
    }
    static std::atomic<std::uint64_t> sequence{1};
    value = monotonic_now_ns() ^
            (static_cast<std::uint64_t>(static_cast<std::uint32_t>(::getpid())) << 32U) ^
            sequence.fetch_add(1, std::memory_order_relaxed);
    return value == 0 ? 1 : value;
}

[[nodiscard]] inline std::string managed_control_name(std::string_view base_name) {
    auto result = normalize_name(base_name);
    result += ".shmchan.control";
    if (result.size() > 250) {
        throw std::invalid_argument("shmchan: managed channel name is too long");
    }
    return result;
}

[[nodiscard]] inline std::string managed_data_name(
    std::string_view base_name,
    std::uint64_t generation) {
    auto result = normalize_name(base_name);
    std::array<char, 32> suffix{};
    const int length = std::snprintf(
        suffix.data(), suffix.size(), ".shmchan.g%016llx",
        static_cast<unsigned long long>(generation));
    if (length <= 0) {
        throw std::runtime_error("shmchan: failed to format generation name");
    }
    result.append(suffix.data(), static_cast<std::size_t>(length));
    if (result.size() > 250) {
        throw std::invalid_argument("shmchan: managed channel name is too long");
    }
    return result;
}

struct alignas(managed_cache_line) managed_participant_slot {
    std::atomic<std::uint32_t> state{};
    std::atomic<std::uint32_t> role{};
    std::atomic<std::int32_t> pid{};
    std::uint32_t reserved{};
    std::atomic<std::uint64_t> session{};
    std::atomic<std::uint64_t> process_start_ns{};
    std::atomic<std::uint64_t> heartbeat_ns{};
    std::atomic<std::uint64_t> observed_generation{};
    std::array<std::byte, 16> padding{};
};

static_assert(sizeof(managed_participant_slot) == managed_cache_line);

struct alignas(managed_cache_line) managed_counters {
    std::atomic<std::uint64_t> sent_messages{};
    std::atomic<std::uint64_t> sent_bytes{};
    std::atomic<std::uint64_t> delivered_messages{};
    std::atomic<std::uint64_t> delivered_bytes{};
    std::atomic<std::uint64_t> acknowledged_messages{};
    std::atomic<std::uint64_t> redelivered_messages{};
    std::atomic<std::uint64_t> negatively_acknowledged_messages{};
    std::atomic<std::uint64_t> cancelled_reservations{};
    std::atomic<std::uint64_t> send_timeouts{};
    std::atomic<std::uint64_t> receive_timeouts{};
    std::atomic<std::uint64_t> broken_generations{};
    std::atomic<std::uint64_t> rebuilt_generations{};
    std::atomic<std::uint64_t> replay_failures{};
    std::atomic<std::uint32_t> waiting_senders{};
    std::atomic<std::uint32_t> waiting_receivers{};
};

struct alignas(managed_cache_line) managed_control_header {
    std::uint64_t magic{};
    std::uint32_t layout_version{};
    std::uint32_t header_size{};
    std::uint64_t mapping_size{};
    std::uint64_t protocol_id_value{};
    std::uint32_t protocol_version{};
    std::uint32_t max_participants{};
    std::uint64_t message_capacity{};
    std::uint64_t max_message_size{};
    std::uint64_t heartbeat_interval_ns{};
    std::uint64_t participant_timeout_ns{};
    std::uint64_t reservation_timeout_ns{};
    std::uint64_t acknowledgment_timeout_ns{};
    std::uint64_t pthread_mutex_size{};
    std::uint64_t pthread_mutex_alignment{};

    alignas(managed_cache_line) std::atomic<std::uint32_t> initialization{};
    std::atomic<std::uint32_t> state{};
    std::atomic<std::uint32_t> reason{};
    std::atomic<std::uint32_t> event_epoch{};
    std::atomic<std::uint64_t> generation{};
    std::atomic<std::uint64_t> previous_generation{};
    std::atomic<std::uint64_t> building_generation{};
    std::atomic<std::uint64_t> replay_owner_session{};
    std::atomic<std::uint64_t> failed_participant_session{};

    alignas(managed_cache_line) managed_counters counters{};
    alignas(managed_cache_line)
        std::array<managed_participant_slot, managed_max_participants> participants{};
};

struct alignas(managed_cache_line) managed_message_slot {
    std::uint32_t state{};
    std::uint32_t payload_size{};
    std::uint64_t sequence{};
    std::uint64_t message_id{};
    std::uint64_t owner_session{};
    std::uint64_t created_ns{};
    std::uint64_t lease_deadline_ns{};
    std::uint32_t owner_participant{};
    std::uint32_t delivery_attempt{};
    std::uint32_t readers{};
    std::uint32_t acknowledgment_pending{};
};

static_assert(sizeof(managed_message_slot) == managed_cache_line);

struct alignas(managed_cache_line) managed_data_header {
    std::uint64_t magic{};
    std::uint32_t layout_version{};
    std::uint32_t header_size{};
    std::uint64_t mapping_size{};
    std::uint64_t generation{};
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
    alignas(managed_cache_line) ::pthread_mutex_t mutex{};

    alignas(managed_cache_line) std::uint64_t next_sequence{1};
    std::uint64_t next_message_id{1};
    std::uint64_t free_slots{};
    std::uint64_t writing_messages{};
    std::uint64_t ready_messages{};
    std::uint64_t inflight_messages{};
};

struct managed_control_mapping {
    void* address{};
    std::size_t size{};
    managed_control_header* header{};
    std::string base_name{};
    std::string object_name{};

    ~managed_control_mapping() {
        if (address != nullptr) {
            (void)::munmap(address, size);
        }
    }
};

struct managed_data_mapping {
    void* address{};
    std::size_t size{};
    managed_data_header* header{};
    managed_message_slot* slots{};
    std::byte* payloads{};
    std::string object_name{};

    ~managed_data_mapping() {
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

[[nodiscard]] inline std::size_t checked_managed_data_size(
    std::size_t capacity,
    std::size_t max_message_size,
    std::size_t& slots_offset,
    std::size_t& payload_offset,
    std::size_t& payload_stride) {
    if (capacity == 0 || max_message_size == 0) {
        throw std::invalid_argument(
            "shmchan: managed channel capacity and max message size must be positive");
    }
    if (max_message_size >
        std::numeric_limits<std::size_t>::max() - (managed_cache_line - 1)) {
        throw std::length_error("shmchan: managed payload alignment overflows size_t");
    }
    payload_stride = managed_align_up(max_message_size, managed_cache_line);
    slots_offset = managed_align_up(sizeof(managed_data_header), alignof(managed_message_slot));
    if (capacity >
        (std::numeric_limits<std::size_t>::max() - slots_offset) /
            sizeof(managed_message_slot)) {
        throw std::length_error("shmchan: managed channel descriptor area is too large");
    }
    payload_offset = managed_align_up(
        slots_offset + capacity * sizeof(managed_message_slot), managed_cache_line);
    if (capacity >
        (std::numeric_limits<std::size_t>::max() - payload_offset) / payload_stride) {
        throw std::length_error("shmchan: managed channel payload area is too large");
    }
    const auto mapping_size = payload_offset + capacity * payload_stride;
    if (mapping_size > static_cast<std::uintmax_t>(std::numeric_limits<::off_t>::max())) {
        throw std::length_error("shmchan: managed mapping size overflows off_t");
    }
    return mapping_size;
}

inline void managed_unlink_if_exists(const std::string& name) noexcept {
    if (::shm_unlink(name.c_str()) != 0 && errno != ENOENT) {
        return;
    }
}

[[nodiscard]] inline std::shared_ptr<managed_data_mapping> create_managed_data(
    const std::string& base_name,
    std::uint64_t generation,
    std::size_t capacity,
    std::size_t max_message_size,
    protocol_descriptor protocol,
    ::mode_t permissions) {
    std::size_t slots_offset{};
    std::size_t payload_offset{};
    std::size_t payload_stride{};
    const auto mapping_size = checked_managed_data_size(
        capacity, max_message_size, slots_offset, payload_offset, payload_stride);
    const auto object_name = managed_data_name(base_name, generation);

    const int raw_fd =
        ::shm_open(object_name.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, permissions);
    if (raw_fd < 0) {
        throw_errno("shm_open(create managed data)", object_name);
    }
    unique_fd fd(raw_fd);
    bool unlink_on_failure = true;

    try {
        lock_fd(fd.get(), LOCK_EX);
        if (::ftruncate(fd.get(), static_cast<::off_t>(mapping_size)) != 0) {
            throw_errno("ftruncate managed data", object_name);
        }
        void* address =
            ::mmap(nullptr, mapping_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd.get(), 0);
        if (address == MAP_FAILED) {
            throw_errno("mmap managed data", object_name);
        }
        mapped_region mapping(address, mapping_size);
        auto* header = std::construct_at(static_cast<managed_data_header*>(address));
        header->magic = managed_data_magic;
        header->layout_version = managed_data_version;
        header->header_size = sizeof(managed_data_header);
        header->mapping_size = mapping_size;
        header->generation = generation;
        header->protocol_id_value = protocol.id;
        header->protocol_version = protocol.version;
        header->message_capacity = capacity;
        header->max_message_size = max_message_size;
        header->slots_offset = slots_offset;
        header->payload_offset = payload_offset;
        header->payload_stride = payload_stride;
        header->slot_size = sizeof(managed_message_slot);
        header->mutex_size = sizeof(::pthread_mutex_t);
        header->mutex_alignment = alignof(::pthread_mutex_t);
        header->initialization.store(0, std::memory_order_relaxed);
        header->next_sequence = 1;
        header->next_message_id = 1;
        header->free_slots = capacity;

        ::pthread_mutexattr_t attributes{};
        int result = ::pthread_mutexattr_init(std::addressof(attributes));
        if (result != 0) {
            throw std::system_error(result, std::generic_category(), "pthread_mutexattr_init");
        }
        bool attributes_ready = true;
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
                throw std::system_error(result, std::generic_category(), "pthread_mutex_init");
            }
            (void)::pthread_mutexattr_destroy(std::addressof(attributes));
            attributes_ready = false;
        } catch (...) {
            if (attributes_ready) {
                (void)::pthread_mutexattr_destroy(std::addressof(attributes));
            }
            throw;
        }

        auto* bytes = static_cast<std::byte*>(address);
        auto* slots = reinterpret_cast<managed_message_slot*>(bytes + slots_offset);
        for (std::size_t index = 0; index < capacity; ++index) {
            (void)std::construct_at(slots + index);
        }
        header->initialization.store(initialization_ready, std::memory_order_release);

        lock_fd(fd.get(), LOCK_UN);
        unlink_on_failure = false;
        auto result_mapping = std::make_shared<managed_data_mapping>();
        result_mapping->address = mapping.release();
        result_mapping->size = mapping_size;
        result_mapping->header = header;
        result_mapping->slots = slots;
        result_mapping->payloads = bytes + payload_offset;
        result_mapping->object_name = object_name;
        return result_mapping;
    } catch (...) {
        if (unlink_on_failure) {
            managed_unlink_if_exists(object_name);
        }
        throw;
    }
}

[[nodiscard]] inline std::shared_ptr<managed_data_mapping> open_managed_data(
    const std::string& base_name,
    std::uint64_t generation,
    std::size_t capacity,
    std::size_t max_message_size,
    protocol_descriptor protocol) {
    const auto object_name = managed_data_name(base_name, generation);
    const int raw_fd = ::shm_open(object_name.c_str(), O_RDWR | O_CLOEXEC, 0);
    if (raw_fd < 0) {
        throw_errno("shm_open(open managed data)", object_name);
    }
    unique_fd fd(raw_fd);
    lock_fd(fd.get(), LOCK_SH);
    struct stat attributes {};
    if (::fstat(fd.get(), std::addressof(attributes)) != 0) {
        throw_errno("fstat managed data", object_name);
    }
    if (attributes.st_size <= 0 ||
        static_cast<std::uintmax_t>(attributes.st_size) >
            std::numeric_limits<std::size_t>::max()) {
        throw std::runtime_error("shmchan: invalid managed data size: " + object_name);
    }
    const auto mapping_size = static_cast<std::size_t>(attributes.st_size);
    void* address =
        ::mmap(nullptr, mapping_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd.get(), 0);
    if (address == MAP_FAILED) {
        throw_errno("mmap managed data", object_name);
    }
    mapped_region mapping(address, mapping_size);
    auto* header = static_cast<managed_data_header*>(address);
    if (mapping_size < sizeof(managed_data_header) ||
        header->initialization.load(std::memory_order_acquire) != initialization_ready ||
        header->magic != managed_data_magic ||
        header->layout_version != managed_data_version ||
        header->header_size != sizeof(managed_data_header) ||
        header->mapping_size != mapping_size || header->generation != generation ||
        header->protocol_id_value != protocol.id ||
        header->protocol_version != protocol.version ||
        header->message_capacity != capacity ||
        header->max_message_size != max_message_size ||
        header->slot_size != sizeof(managed_message_slot) ||
        header->mutex_size != sizeof(::pthread_mutex_t) ||
        header->mutex_alignment != alignof(::pthread_mutex_t)) {
        throw std::runtime_error("shmchan: incompatible managed data layout: " + object_name);
    }

    std::size_t expected_slots{};
    std::size_t expected_payload{};
    std::size_t expected_stride{};
    const auto expected_size = checked_managed_data_size(
        capacity,
        max_message_size,
        expected_slots,
        expected_payload,
        expected_stride);
    if (expected_size != mapping_size || header->slots_offset != expected_slots ||
        header->payload_offset != expected_payload || header->payload_stride != expected_stride) {
        throw std::runtime_error("shmchan: corrupt managed data offsets: " + object_name);
    }
    lock_fd(fd.get(), LOCK_UN);

    auto result = std::make_shared<managed_data_mapping>();
    auto* bytes = static_cast<std::byte*>(address);
    result->address = mapping.release();
    result->size = mapping_size;
    result->header = header;
    result->slots = reinterpret_cast<managed_message_slot*>(bytes + expected_slots);
    result->payloads = bytes + expected_payload;
    result->object_name = object_name;
    return result;
}

struct managed_local_state;

} // namespace detail

class managed_byte_channel;
class managed_send_reservation;
class managed_delivery;

} // namespace shmchan

namespace shmchan::detail {

[[nodiscard]] inline std::shared_ptr<managed_control_mapping> create_managed_control(
    std::string_view name,
    const managed_channel_options& options) {
    const auto base_name = normalize_name(name);
    const auto object_name = managed_control_name(base_name);
    if (options.max_participants == 0 ||
        options.max_participants > managed_max_participants) {
        throw std::invalid_argument("shmchan: max_participants must be in [1, 64]");
    }
    if (!valid_participant_role(options.role)) {
        throw std::invalid_argument("shmchan: invalid managed participant role");
    }
    if (options.message_capacity == 0 || options.max_message_size == 0) {
        throw std::invalid_argument(
            "shmchan: message_capacity and max_message_size must be positive");
    }
    if (options.max_message_size > std::numeric_limits<std::uint32_t>::max()) {
        throw std::invalid_argument(
            "shmchan: max_message_size must fit in a 32-bit payload length");
    }
    if (options.protocol.id == 0 || options.protocol.version == 0) {
        throw std::invalid_argument("shmchan: protocol id and version must be non-zero");
    }
    const auto heartbeat_interval = duration_ns(options.heartbeat_interval, "heartbeat_interval");
    const auto participant_timeout = duration_ns(options.participant_timeout, "participant_timeout");
    const auto reservation_timeout = duration_ns(options.reservation_timeout, "reservation_timeout");
    const auto acknowledgment_timeout =
        duration_ns(options.acknowledgment_timeout, "acknowledgment_timeout");
    if (participant_timeout <= heartbeat_interval ||
        participant_timeout - heartbeat_interval <= heartbeat_interval) {
        throw std::invalid_argument(
            "shmchan: participant_timeout must exceed twice the heartbeat interval");
    }

    const int raw_fd = ::shm_open(
        object_name.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, options.permissions);
    if (raw_fd < 0) {
        throw_errno("shm_open(create managed control)", object_name);
    }
    unique_fd fd(raw_fd);
    bool unlink_on_failure = true;

    try {
        lock_fd(fd.get(), LOCK_EX);
        constexpr auto mapping_size = sizeof(managed_control_header);
        if (::ftruncate(fd.get(), static_cast<::off_t>(mapping_size)) != 0) {
            throw_errno("ftruncate managed control", object_name);
        }
        void* address =
            ::mmap(nullptr, mapping_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd.get(), 0);
        if (address == MAP_FAILED) {
            throw_errno("mmap managed control", object_name);
        }
        mapped_region mapping(address, mapping_size);
        auto* header = std::construct_at(static_cast<managed_control_header*>(address));
        header->magic = managed_control_magic;
        header->layout_version = managed_control_version;
        header->header_size = sizeof(managed_control_header);
        header->mapping_size = mapping_size;
        header->protocol_id_value = options.protocol.id;
        header->protocol_version = options.protocol.version;
        header->max_participants = static_cast<std::uint32_t>(options.max_participants);
        header->message_capacity = options.message_capacity;
        header->max_message_size = options.max_message_size;
        header->heartbeat_interval_ns = heartbeat_interval;
        header->participant_timeout_ns = participant_timeout;
        header->reservation_timeout_ns = reservation_timeout;
        header->acknowledgment_timeout_ns = acknowledgment_timeout;
        header->pthread_mutex_size = sizeof(::pthread_mutex_t);
        header->pthread_mutex_alignment = alignof(::pthread_mutex_t);
        header->initialization.store(0, std::memory_order_relaxed);
        header->state.store(
            static_cast<std::uint32_t>(managed_channel_state::initializing),
            std::memory_order_relaxed);
        header->reason.store(
            static_cast<std::uint32_t>(break_reason::none), std::memory_order_relaxed);
        header->event_epoch.store(0, std::memory_order_relaxed);
        header->generation.store(1, std::memory_order_relaxed);
        header->previous_generation.store(0, std::memory_order_relaxed);
        header->building_generation.store(1, std::memory_order_relaxed);
        header->replay_owner_session.store(0, std::memory_order_relaxed);
        header->failed_participant_session.store(0, std::memory_order_relaxed);

        auto data = create_managed_data(
            base_name,
            1,
            options.message_capacity,
            options.max_message_size,
            options.protocol,
            options.permissions);
        (void)data;
        header->building_generation.store(0, std::memory_order_release);
        header->state.store(
            static_cast<std::uint32_t>(managed_channel_state::healthy),
            std::memory_order_release);
        header->initialization.store(initialization_ready, std::memory_order_release);
        lock_fd(fd.get(), LOCK_UN);
        unlink_on_failure = false;

        auto result = std::make_shared<managed_control_mapping>();
        result->address = mapping.release();
        result->size = mapping_size;
        result->header = header;
        result->base_name = base_name;
        result->object_name = object_name;
        return result;
    } catch (...) {
        if (unlink_on_failure) {
            managed_unlink_if_exists(managed_data_name(base_name, 1));
            managed_unlink_if_exists(object_name);
        }
        throw;
    }
}

[[nodiscard]] inline std::shared_ptr<managed_control_mapping> open_managed_control(
    std::string_view name,
    protocol_descriptor protocol) {
    if (protocol.id == 0 || protocol.version == 0) {
        throw std::invalid_argument("shmchan: protocol id and version must be non-zero");
    }
    const auto base_name = normalize_name(name);
    const auto object_name = managed_control_name(base_name);
    const int raw_fd = ::shm_open(object_name.c_str(), O_RDWR | O_CLOEXEC, 0);
    if (raw_fd < 0) {
        throw_errno("shm_open(open managed control)", object_name);
    }
    unique_fd fd(raw_fd);
    lock_fd(fd.get(), LOCK_SH);
    struct stat attributes {};
    if (::fstat(fd.get(), std::addressof(attributes)) != 0) {
        throw_errno("fstat managed control", object_name);
    }
    if (attributes.st_size != static_cast<::off_t>(sizeof(managed_control_header))) {
        throw std::runtime_error("shmchan: invalid managed control size: " + object_name);
    }
    void* address = ::mmap(
        nullptr,
        sizeof(managed_control_header),
        PROT_READ | PROT_WRITE,
        MAP_SHARED,
        fd.get(),
        0);
    if (address == MAP_FAILED) {
        throw_errno("mmap managed control", object_name);
    }
    mapped_region mapping(address, sizeof(managed_control_header));
    auto* header = static_cast<managed_control_header*>(address);
    const auto persisted_state = header->state.load(std::memory_order_acquire);
    if (header->initialization.load(std::memory_order_acquire) != initialization_ready ||
        header->magic != managed_control_magic ||
        header->layout_version != managed_control_version ||
        header->header_size != sizeof(managed_control_header) ||
        header->mapping_size != sizeof(managed_control_header) ||
        header->max_participants == 0 ||
        header->max_participants > managed_max_participants ||
        header->message_capacity == 0 || header->max_message_size == 0 ||
        header->max_message_size > std::numeric_limits<std::uint32_t>::max() ||
        header->protocol_id_value == 0 || header->protocol_version == 0 ||
        header->heartbeat_interval_ns == 0 || header->participant_timeout_ns == 0 ||
        header->reservation_timeout_ns == 0 || header->acknowledgment_timeout_ns == 0 ||
        header->heartbeat_interval_ns >
            static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) ||
        header->participant_timeout_ns >
            static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) ||
        header->reservation_timeout_ns >
            static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) ||
        header->acknowledgment_timeout_ns >
            static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) ||
        header->participant_timeout_ns <= header->heartbeat_interval_ns ||
        header->participant_timeout_ns - header->heartbeat_interval_ns <=
            header->heartbeat_interval_ns ||
        header->generation.load(std::memory_order_acquire) == 0 ||
        persisted_state > static_cast<std::uint32_t>(managed_channel_state::replaying) ||
        header->pthread_mutex_size != sizeof(::pthread_mutex_t) ||
        header->pthread_mutex_alignment != alignof(::pthread_mutex_t)) {
        throw std::runtime_error("shmchan: incompatible managed control layout: " + object_name);
    }
    if (header->protocol_id_value != protocol.id ||
        header->protocol_version != protocol.version) {
        throw managed_channel_error(
            channel_status::protocol_mismatch,
            "shmchan: managed channel protocol id/version does not match: " + object_name);
    }
    lock_fd(fd.get(), LOCK_UN);

    auto result = std::make_shared<managed_control_mapping>();
    result->address = mapping.release();
    result->size = sizeof(managed_control_header);
    result->header = header;
    result->base_name = base_name;
    result->object_name = object_name;
    return result;
}

inline void signal_managed_event(managed_control_header& control, int count = INT_MAX) noexcept {
    control.event_epoch.fetch_add(1, std::memory_order_release);
    futex_wake(control.event_epoch, count);
}

[[nodiscard]] inline bool finalize_control_breaking(
    managed_control_header& control) noexcept {
    auto expected = static_cast<std::uint32_t>(managed_channel_state::breaking);
    if (!control.state.compare_exchange_strong(
            expected,
            static_cast<std::uint32_t>(managed_channel_state::broken),
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        return false;
    }
    control.counters.broken_generations.fetch_add(1, std::memory_order_relaxed);
    signal_managed_event(control);
    return true;
}

[[nodiscard]] inline bool mark_control_broken(
    managed_control_header& control,
    break_reason reason,
    std::uint64_t participant_session = 0) noexcept {
    auto expected = control.state.load(std::memory_order_acquire);
    for (;;) {
        if (expected != static_cast<std::uint32_t>(managed_channel_state::healthy) &&
            expected != static_cast<std::uint32_t>(managed_channel_state::closed) &&
            expected != static_cast<std::uint32_t>(managed_channel_state::replaying)) {
            return false;
        }
        if (control.state.compare_exchange_weak(
                expected,
                static_cast<std::uint32_t>(managed_channel_state::breaking),
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            break;
        }
    }
    control.replay_owner_session.store(0, std::memory_order_release);
    control.reason.store(static_cast<std::uint32_t>(reason), std::memory_order_release);
    control.failed_participant_session.store(participant_session, std::memory_order_release);
    (void)finalize_control_breaking(control);
    return true;
}

struct managed_data_lock {
    managed_control_header* control{};
    managed_data_mapping* data{};
    bool owns{};
    bool usable{};

    managed_data_lock(
        managed_control_header& control_header,
        managed_data_mapping& mapping,
        bool wait = true)
        : control(std::addressof(control_header)), data(std::addressof(mapping)) {
        const auto initial_state = static_cast<managed_channel_state>(
            control->state.load(std::memory_order_acquire));
        if ((initial_state != managed_channel_state::healthy &&
             initial_state != managed_channel_state::closed &&
             initial_state != managed_channel_state::replaying) ||
            control->generation.load(std::memory_order_acquire) != data->header->generation) {
            return;
        }

        const auto deadline = managed_deadline_after(
            monotonic_now_ns(), control->participant_timeout_ns);
        unsigned contention_count = 0;
        for (;;) {
            const int result =
                ::pthread_mutex_trylock(std::addressof(data->header->mutex));
            if (result == 0) {
                owns = true;
                const auto locked_state = static_cast<managed_channel_state>(
                    control->state.load(std::memory_order_acquire));
                usable = (locked_state == managed_channel_state::healthy ||
                          locked_state == managed_channel_state::closed ||
                          locked_state == managed_channel_state::replaying) &&
                         control->generation.load(std::memory_order_acquire) ==
                             data->header->generation;
                return;
            }
            if (result == EOWNERDEAD) {
                owns = true;
                (void)mark_control_broken(
                    *control, break_reason::robust_mutex_owner_died);
                (void)::pthread_mutex_consistent(std::addressof(data->header->mutex));
                return;
            }
            if (result == ENOTRECOVERABLE) {
                (void)mark_control_broken(
                    *control, break_reason::robust_mutex_not_recoverable);
                return;
            }
            if (result != EBUSY) {
                throw std::system_error(
                    result, std::generic_category(), "pthread_mutex_trylock(managed data)");
            }
            if (!wait) {
                return;
            }

            const auto current_state = static_cast<managed_channel_state>(
                control->state.load(std::memory_order_acquire));
            if ((current_state != managed_channel_state::healthy &&
                 current_state != managed_channel_state::closed &&
                 current_state != managed_channel_state::replaying) ||
                control->generation.load(std::memory_order_acquire) !=
                    data->header->generation) {
                return;
            }
            if (monotonic_now_ns() >= deadline) {
                (void)mark_control_broken(
                    *control, break_reason::mutex_lock_timeout);
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

    ~managed_data_lock() {
        if (owns) {
            (void)::pthread_mutex_unlock(std::addressof(data->header->mutex));
        }
    }

    managed_data_lock(const managed_data_lock&) = delete;
    managed_data_lock& operator=(const managed_data_lock&) = delete;
};

struct managed_local_state {
    std::shared_ptr<managed_control_mapping> control{};
    std::mutex data_mutex{};
    std::shared_ptr<managed_data_mapping> data{};
    std::size_t participant_index{managed_max_participants};
    std::uint64_t session{};
    participant_role role{participant_role::both};
    bool monitor_peers{true};
    std::jthread heartbeat_thread{};
    std::mutex heartbeat_mutex{};
    std::condition_variable_any heartbeat_condition{};

    ~managed_local_state() {
        heartbeat_thread.request_stop();
        heartbeat_condition.notify_all();
        if (heartbeat_thread.joinable() &&
            heartbeat_thread.get_id() != std::this_thread::get_id()) {
            heartbeat_thread.join();
        }
        release_participant();
    }

    [[nodiscard]] managed_control_header& control_header() const noexcept {
        return *control->header;
    }

    [[nodiscard]] protocol_descriptor protocol() const noexcept {
        return {control->header->protocol_id_value, control->header->protocol_version};
    }

    static void clear_participant(managed_participant_slot& slot) noexcept {
        slot.heartbeat_ns.store(0, std::memory_order_relaxed);
        slot.observed_generation.store(0, std::memory_order_relaxed);
        slot.process_start_ns.store(0, std::memory_order_relaxed);
        slot.session.store(0, std::memory_order_relaxed);
        slot.pid.store(0, std::memory_order_relaxed);
        slot.role.store(0, std::memory_order_relaxed);
    }

    [[nodiscard]] bool participant_valid() const noexcept {
        if (control == nullptr || participant_index >= managed_max_participants) {
            return false;
        }
        const auto& slot = control->header->participants[participant_index];
        return slot.state.load(std::memory_order_acquire) == participant_active &&
               slot.session.load(std::memory_order_acquire) == session;
    }

    void claim_participant() {
        session = random_session_id();
        const auto now = monotonic_now_ns();
        const int raw_fd = ::shm_open(control->object_name.c_str(), O_RDWR | O_CLOEXEC, 0);
        if (raw_fd < 0) {
            throw_errno("shm_open(claim managed participant)", control->object_name);
        }
        unique_fd fd(raw_fd);
        lock_fd(fd.get(), LOCK_EX);
        try {
            auto& header = *control->header;
            for (std::size_t index = 0; index < header.max_participants; ++index) {
                auto& slot = header.participants[index];
                const auto slot_state = slot.state.load(std::memory_order_acquire);
                if (slot_state == participant_claiming ||
                    slot_state == participant_releasing) {
                    clear_participant(slot);
                    slot.state.store(participant_free, std::memory_order_release);
                }
            }

            for (std::size_t index = 0; index < header.max_participants; ++index) {
                auto& slot = header.participants[index];
                if (slot.state.load(std::memory_order_acquire) != participant_active) {
                    continue;
                }
                const auto heartbeat = slot.heartbeat_ns.load(std::memory_order_acquire);
                if (heartbeat == 0 || now < heartbeat ||
                    now - heartbeat <= header.participant_timeout_ns) {
                    continue;
                }
                const auto failed_session = slot.session.load(std::memory_order_acquire);
                auto expected = participant_active;
                if (slot.state.compare_exchange_strong(
                        expected,
                        participant_stale,
                        std::memory_order_acq_rel,
                        std::memory_order_relaxed)) {
                    (void)mark_control_broken(
                        header, break_reason::participant_timeout, failed_session);
                }
            }

            auto claim = [&](std::uint32_t expected_state) {
                for (std::size_t index = 0; index < header.max_participants; ++index) {
                    auto& slot = header.participants[index];
                    auto expected = expected_state;
                    if (!slot.state.compare_exchange_strong(
                            expected,
                            participant_claiming,
                            std::memory_order_acq_rel,
                            std::memory_order_relaxed)) {
                        continue;
                    }
                    slot.role.store(
                        static_cast<std::uint32_t>(role), std::memory_order_relaxed);
                    slot.pid.store(
                        static_cast<std::int32_t>(::getpid()), std::memory_order_relaxed);
                    slot.session.store(session, std::memory_order_relaxed);
                    slot.process_start_ns.store(now, std::memory_order_relaxed);
                    slot.heartbeat_ns.store(now, std::memory_order_relaxed);
                    slot.observed_generation.store(
                        header.generation.load(std::memory_order_acquire),
                        std::memory_order_relaxed);
                    participant_index = index;
                    slot.state.store(participant_active, std::memory_order_release);
                    return true;
                }
                return false;
            };

            if (!claim(participant_free) &&
                (header.state.load(std::memory_order_acquire) !=
                     static_cast<std::uint32_t>(managed_channel_state::broken) ||
                 !claim(participant_stale))) {
                throw managed_channel_error(
                    channel_status::participant_limit,
                    "shmchan: managed channel participant table is full");
            }
            lock_fd(fd.get(), LOCK_UN);
        } catch (...) {
            (void)::flock(fd.get(), LOCK_UN);
            throw;
        }
    }

    void release_participant() noexcept {
        if (participant_index >= managed_max_participants || control == nullptr) {
            return;
        }
        const int fd = ::shm_open(control->object_name.c_str(), O_RDWR | O_CLOEXEC, 0);
        bool locked = false;
        if (fd >= 0) {
            for (;;) {
                if (::flock(fd, LOCK_EX) == 0) {
                    locked = true;
                    break;
                }
                if (errno != EINTR) {
                    break;
                }
            }
        }
        auto& slot = control->header->participants[participant_index];
        auto expected = participant_active;
        if ((locked || fd < 0) &&
            slot.session.load(std::memory_order_acquire) == session &&
            slot.state.compare_exchange_strong(
                expected,
                participant_releasing,
                std::memory_order_acq_rel,
                std::memory_order_relaxed)) {
            clear_participant(slot);
            slot.state.store(participant_free, std::memory_order_release);
        }
        if (locked) {
            (void)::flock(fd, LOCK_UN);
        }
        if (fd >= 0) {
            (void)::close(fd);
        }
        participant_index = managed_max_participants;
    }

    void update_heartbeat() noexcept {
        if (participant_index >= managed_max_participants) {
            return;
        }
        auto& slot = control->header->participants[participant_index];
        if (slot.state.load(std::memory_order_acquire) == participant_active &&
            slot.session.load(std::memory_order_acquire) == session) {
            slot.heartbeat_ns.store(monotonic_now_ns(), std::memory_order_release);
        }
    }

    [[nodiscard]] std::shared_ptr<managed_data_mapping> current_data(
        bool throw_on_corruption = false) {
        for (;;) {
            if (!participant_valid()) {
                return {};
            }
            const auto state_value = static_cast<managed_channel_state>(
                control->header->state.load(std::memory_order_acquire));
            if (state_value != managed_channel_state::healthy &&
                state_value != managed_channel_state::closed &&
                state_value != managed_channel_state::replaying) {
                return {};
            }
            const auto wanted_generation =
                control->header->generation.load(std::memory_order_acquire);
            std::shared_ptr<managed_data_mapping> result;
            {
                std::scoped_lock lock(data_mutex);
                if (data == nullptr || data->header->generation != wanted_generation) {
                    try {
                        data = open_managed_data(
                            control->base_name,
                            wanted_generation,
                            static_cast<std::size_t>(control->header->message_capacity),
                            static_cast<std::size_t>(control->header->max_message_size),
                            protocol());
                    } catch (...) {
                        const auto current_state = static_cast<managed_channel_state>(
                            control->header->state.load(std::memory_order_acquire));
                        if (control->header->generation.load(std::memory_order_acquire) !=
                                wanted_generation ||
                            (current_state != managed_channel_state::healthy &&
                             current_state != managed_channel_state::closed &&
                             current_state != managed_channel_state::replaying)) {
                            continue;
                        }
                        (void)mark_control_broken(
                            *control->header, break_reason::corrupt_data);
                        if (throw_on_corruption) {
                            throw;
                        }
                        return {};
                    }
                }
                result = data;
            }
            const auto current_state = static_cast<managed_channel_state>(
                control->header->state.load(std::memory_order_acquire));
            if (control->header->generation.load(std::memory_order_acquire) !=
                    wanted_generation ||
                (current_state != managed_channel_state::healthy &&
                 current_state != managed_channel_state::closed &&
                 current_state != managed_channel_state::replaying)) {
                continue;
            }
            auto& slot = control->header->participants[participant_index];
            if (slot.state.load(std::memory_order_acquire) == participant_active &&
                slot.session.load(std::memory_order_acquire) == session) {
                slot.observed_generation.store(wanted_generation, std::memory_order_release);
                return result;
            }
            return {};
        }
    }

    void scan_participants() noexcept {
        auto& header = *control->header;
        const auto channel_state = static_cast<managed_channel_state>(
            header.state.load(std::memory_order_acquire));
        if (channel_state != managed_channel_state::healthy &&
            channel_state != managed_channel_state::closed &&
            channel_state != managed_channel_state::replaying) {
            return;
        }
        const auto now = monotonic_now_ns();
        for (std::size_t index = 0; index < header.max_participants; ++index) {
            if (index == participant_index) {
                continue;
            }
            auto& slot = header.participants[index];
            if (slot.state.load(std::memory_order_acquire) != participant_active) {
                continue;
            }
            const auto heartbeat = slot.heartbeat_ns.load(std::memory_order_acquire);
            if (heartbeat != 0 && now >= heartbeat &&
                now - heartbeat > header.participant_timeout_ns) {
                const auto failed_session = slot.session.load(std::memory_order_acquire);
                std::uint32_t expected = participant_active;
                if (slot.state.compare_exchange_strong(
                        expected,
                        participant_stale,
                        std::memory_order_acq_rel,
                        std::memory_order_relaxed)) {
                    (void)mark_control_broken(
                        header, break_reason::participant_timeout, failed_session);
                }
                return;
            }
        }
    }

    void scan_reservations() noexcept {
        auto& header = *control->header;
        const auto channel_state = static_cast<managed_channel_state>(
            header.state.load(std::memory_order_acquire));
        if (channel_state != managed_channel_state::healthy &&
            channel_state != managed_channel_state::closed &&
            channel_state != managed_channel_state::replaying) {
            return;
        }
        std::shared_ptr<managed_data_mapping> mapping;
        try {
            mapping = current_data();
            if (mapping == nullptr) {
                return;
            }
            managed_data_lock lock(header, *mapping, false);
            if (!lock.usable) {
                return;
            }
            const auto now = monotonic_now_ns();
            for (std::size_t index = 0; index < mapping->header->message_capacity; ++index) {
                const auto& slot = mapping->slots[index];
                if (slot.state == static_cast<std::uint32_t>(managed_slot_state::writing) &&
                    slot.lease_deadline_ns != 0 && now >= slot.lease_deadline_ns) {
                    (void)mark_control_broken(
                        header, break_reason::reservation_timeout, slot.owner_session);
                    return;
                }
            }
        } catch (...) {
            (void)mark_control_broken(header, break_reason::corrupt_data);
        }
    }

    void heartbeat_loop(std::stop_token stop) noexcept {
        while (!stop.stop_requested()) {
            update_heartbeat();
            if (monitor_peers && participant_valid()) {
                scan_participants();
                scan_reservations();
            }
            std::unique_lock lock(heartbeat_mutex);
            const auto interval = std::chrono::nanoseconds{
                static_cast<std::chrono::nanoseconds::rep>(
                    control->header->heartbeat_interval_ns)};
            try {
                (void)heartbeat_condition.wait_for(
                    lock, stop, interval, [] { return false; });
            } catch (...) {
                (void)mark_control_broken(
                    *control->header, break_reason::corrupt_data, session);
                return;
            }
        }
    }

    void start_heartbeat() {
        heartbeat_thread = std::jthread(
            [this](std::stop_token stop) { heartbeat_loop(stop); });
    }
};

[[nodiscard]] inline std::shared_ptr<managed_local_state> make_managed_local_state(
    std::shared_ptr<managed_control_mapping> control,
    participant_role role,
    bool monitor_peers) {
    if (!valid_participant_role(role)) {
        throw std::invalid_argument("shmchan: invalid managed participant role");
    }
    auto state = std::make_shared<managed_local_state>();
    state->control = std::move(control);
    state->role = role;
    state->monitor_peers = monitor_peers;
    state->claim_participant();
    try {
        if (state->control->header->state.load(std::memory_order_acquire) !=
            static_cast<std::uint32_t>(managed_channel_state::broken)) {
            (void)state->current_data(true);
        }
        state->start_heartbeat();
    } catch (...) {
        state->release_participant();
        throw;
    }
    return state;
}

inline void free_managed_slot(managed_data_mapping& data, managed_message_slot& slot) noexcept {
    const auto previous_state = static_cast<managed_slot_state>(slot.state);
    if (previous_state == managed_slot_state::writing && data.header->writing_messages != 0) {
        --data.header->writing_messages;
    } else if (previous_state == managed_slot_state::ready && data.header->ready_messages != 0) {
        --data.header->ready_messages;
    } else if ((previous_state == managed_slot_state::inflight ||
                previous_state == managed_slot_state::acknowledged) &&
               data.header->inflight_messages != 0) {
        --data.header->inflight_messages;
    }
    slot = managed_message_slot{};
    ++data.header->free_slots;
}

[[nodiscard]] inline bool managed_counts_valid(const managed_data_header& header) noexcept {
    const auto capacity = header.message_capacity;
    if (header.free_slots > capacity || header.writing_messages > capacity ||
        header.ready_messages > capacity || header.inflight_messages > capacity) {
        return false;
    }
    return header.free_slots + header.writing_messages + header.ready_messages +
               header.inflight_messages ==
           capacity;
}

[[nodiscard]] inline channel_status control_operation_status(
    const managed_local_state& local,
    bool sending) noexcept {
    const auto& control = local.control_header();
    const auto state = static_cast<managed_channel_state>(
        control.state.load(std::memory_order_acquire));
    if (!local.participant_valid()) {
        return channel_status::participant_expired;
    }
    if (state == managed_channel_state::healthy) {
        return channel_status::success;
    }
    if (state == managed_channel_state::replaying) {
        if (!sending ||
            control.replay_owner_session.load(std::memory_order_acquire) == local.session) {
            return channel_status::success;
        }
        return channel_status::recovery_in_progress;
    }
    if (state == managed_channel_state::closed) {
        return channel_status::closed;
    }
    return channel_status::broken;
}

} // namespace shmchan::detail

namespace shmchan {

class managed_send_reservation {
public:
    managed_send_reservation() noexcept = default;
    ~managed_send_reservation() { cancel(); }

    managed_send_reservation(const managed_send_reservation&) = delete;
    managed_send_reservation& operator=(const managed_send_reservation&) = delete;

    managed_send_reservation(managed_send_reservation&& other) noexcept {
        move_from(std::move(other));
    }

    managed_send_reservation& operator=(managed_send_reservation&& other) noexcept {
        if (this != &other) {
            cancel();
            move_from(std::move(other));
        }
        return *this;
    }

    [[nodiscard]] explicit operator bool() const noexcept { return active_; }

    [[nodiscard]] std::span<std::byte> buffer() const noexcept {
        if (!active_ || data_ == nullptr) {
            return {};
        }
        return data_->payload(slot_index_);
    }

    [[nodiscard]] std::uint64_t message_id() const noexcept { return message_id_; }
    [[nodiscard]] std::uint64_t generation() const noexcept { return generation_; }
    [[nodiscard]] std::size_t capacity() const noexcept {
        return data_ == nullptr ? 0 : static_cast<std::size_t>(data_->header->max_message_size);
    }

    [[nodiscard]] channel_status commit(std::size_t message_size) {
        if (!active_ || state_ == nullptr || data_ == nullptr) {
            return channel_status::stale_delivery;
        }
        if (message_size > data_->header->max_message_size) {
            return channel_status::message_too_large;
        }
        auto& control = state_->control_header();
        const auto operation_status = detail::control_operation_status(*state_, true);
        if (operation_status != channel_status::success) {
            if (operation_status == channel_status::closed) {
                cancel();
                return operation_status;
            }
            active_ = false;
            return operation_status;
        }
        if (control.generation.load(std::memory_order_acquire) != generation_) {
            active_ = false;
            return channel_status::generation_changed;
        }

        detail::managed_data_lock lock(control, *data_);
        if (!lock.usable) {
            active_ = false;
            return channel_status::broken;
        }
        auto& slot = data_->slots[slot_index_];
        const auto locked_state = static_cast<managed_channel_state>(
            control.state.load(std::memory_order_acquire));
        if (locked_state == managed_channel_state::closed) {
            if (slot.state ==
                    static_cast<std::uint32_t>(detail::managed_slot_state::writing) &&
                slot.sequence == sequence_ && slot.owner_session == state_->session) {
                detail::free_managed_slot(*data_, slot);
                control.counters.cancelled_reservations.fetch_add(
                    1, std::memory_order_relaxed);
                detail::signal_managed_event(control);
            }
            active_ = false;
            return channel_status::closed;
        }
        const bool replay_owner =
            locked_state == managed_channel_state::replaying &&
            control.replay_owner_session.load(std::memory_order_acquire) == state_->session;
        if (locked_state != managed_channel_state::healthy && !replay_owner) {
            active_ = false;
            return channel_status::broken;
        }
        if (!detail::managed_counts_valid(*data_->header)) {
            (void)detail::mark_control_broken(control, break_reason::corrupt_data);
            active_ = false;
            return channel_status::broken;
        }
        if (slot.state !=
                static_cast<std::uint32_t>(detail::managed_slot_state::writing) ||
            slot.sequence != sequence_ || slot.message_id != message_id_ ||
            slot.owner_session != state_->session ||
            data_->header->writing_messages == 0) {
            active_ = false;
            return channel_status::stale_delivery;
        }
        slot.payload_size = static_cast<std::uint32_t>(message_size);
        slot.owner_session = 0;
        slot.owner_participant = 0;
        slot.lease_deadline_ns = 0;
        slot.state = static_cast<std::uint32_t>(detail::managed_slot_state::ready);
        --data_->header->writing_messages;
        ++data_->header->ready_messages;
        control.counters.sent_messages.fetch_add(1, std::memory_order_relaxed);
        control.counters.sent_bytes.fetch_add(message_size, std::memory_order_relaxed);
        active_ = false;
        detail::signal_managed_event(control);

        if (control.generation.load(std::memory_order_acquire) != generation_) {
            return channel_status::generation_changed;
        }
        const auto completed_state = static_cast<managed_channel_state>(
            control.state.load(std::memory_order_acquire));
        return completed_state == managed_channel_state::healthy ||
                       completed_state == managed_channel_state::closed ||
                       (completed_state == managed_channel_state::replaying &&
                        control.replay_owner_session.load(std::memory_order_acquire) ==
                            state_->session)
                   ? channel_status::success
                   : channel_status::broken;
    }

    void cancel() noexcept {
        if (!active_ || state_ == nullptr || data_ == nullptr) {
            active_ = false;
            return;
        }
        auto& control = state_->control_header();
        const auto channel_state = static_cast<managed_channel_state>(
            control.state.load(std::memory_order_acquire));
        const bool replay_owner =
            channel_state == managed_channel_state::replaying &&
            control.replay_owner_session.load(std::memory_order_acquire) == state_->session;
        if (state_->participant_valid() &&
            control.generation.load(std::memory_order_acquire) == generation_ &&
            (channel_state == managed_channel_state::healthy ||
             channel_state == managed_channel_state::closed || replay_owner)) {
            try {
                detail::managed_data_lock lock(control, *data_);
                if (lock.usable) {
                    auto& slot = data_->slots[slot_index_];
                    if (slot.state ==
                            static_cast<std::uint32_t>(detail::managed_slot_state::writing) &&
                        slot.sequence == sequence_ &&
                        slot.owner_session == state_->session) {
                        detail::free_managed_slot(*data_, slot);
                        control.counters.cancelled_reservations.fetch_add(
                            1, std::memory_order_relaxed);
                        detail::signal_managed_event(control);
                    }
                }
            } catch (...) {
                (void)detail::mark_control_broken(control, break_reason::corrupt_data);
            }
        }
        active_ = false;
    }

private:
    friend class managed_byte_channel;

    managed_send_reservation(
        std::shared_ptr<detail::managed_local_state> state,
        std::shared_ptr<detail::managed_data_mapping> data,
        std::size_t slot_index,
        std::uint64_t generation,
        std::uint64_t sequence,
        std::uint64_t message_id) noexcept
        : state_(std::move(state)),
          data_(std::move(data)),
          slot_index_(slot_index),
          generation_(generation),
          sequence_(sequence),
          message_id_(message_id),
          active_(true) {}

    void move_from(managed_send_reservation&& other) noexcept {
        state_ = std::move(other.state_);
        data_ = std::move(other.data_);
        slot_index_ = other.slot_index_;
        generation_ = other.generation_;
        sequence_ = other.sequence_;
        message_id_ = other.message_id_;
        active_ = std::exchange(other.active_, false);
    }

    std::shared_ptr<detail::managed_local_state> state_{};
    std::shared_ptr<detail::managed_data_mapping> data_{};
    std::size_t slot_index_{};
    std::uint64_t generation_{};
    std::uint64_t sequence_{};
    std::uint64_t message_id_{};
    bool active_{};
};

struct managed_reservation_result {
    channel_status code{channel_status::would_block};
    std::optional<managed_send_reservation> value{};

    [[nodiscard]] bool has_value() const noexcept { return value.has_value(); }
    [[nodiscard]] explicit operator bool() const noexcept { return has_value(); }
    [[nodiscard]] managed_send_reservation& operator*() & { return *value; }
    [[nodiscard]] managed_send_reservation* operator->() {
        return std::addressof(*value);
    }
};

class managed_delivery {
public:
    managed_delivery() noexcept = default;
    ~managed_delivery() { release_reader(); }

    managed_delivery(const managed_delivery&) = delete;
    managed_delivery& operator=(const managed_delivery&) = delete;

    managed_delivery(managed_delivery&& other) noexcept { move_from(std::move(other)); }

    managed_delivery& operator=(managed_delivery&& other) noexcept {
        if (this != &other) {
            release_reader();
            move_from(std::move(other));
        }
        return *this;
    }

    [[nodiscard]] explicit operator bool() const noexcept { return active_; }
    [[nodiscard]] std::uint64_t message_id() const noexcept { return message_id_; }
    [[nodiscard]] std::uint64_t generation() const noexcept { return generation_; }
    [[nodiscard]] std::uint32_t attempt() const noexcept { return attempt_; }
    [[nodiscard]] bool redelivered() const noexcept { return attempt_ > 1; }
    [[nodiscard]] bool is_loaned() const noexcept { return loaned_; }

    [[nodiscard]] std::span<const std::byte> bytes() const noexcept {
        if (!active_) {
            return {};
        }
        if (loaned_ && data_ != nullptr) {
            return data_->payload(slot_index_).first(payload_size_);
        }
        return owned_payload_;
    }

    [[nodiscard]] channel_status ack() noexcept { return finish(true); }
    [[nodiscard]] channel_status nack() noexcept { return finish(false); }

private:
    friend class managed_byte_channel;

    managed_delivery(
        std::shared_ptr<detail::managed_local_state> state,
        std::shared_ptr<detail::managed_data_mapping> data,
        std::size_t slot_index,
        std::uint64_t generation,
        std::uint64_t sequence,
        std::uint64_t message_id,
        std::uint32_t attempt,
        std::size_t payload_size,
        bool loaned,
        std::vector<std::byte> owned_payload) noexcept
        : state_(std::move(state)),
          data_(std::move(data)),
          owned_payload_(std::move(owned_payload)),
          slot_index_(slot_index),
          generation_(generation),
          sequence_(sequence),
          message_id_(message_id),
          attempt_(attempt),
          payload_size_(payload_size),
          loaned_(loaned),
          reader_held_(loaned),
          active_(true) {}

    [[nodiscard]] channel_status finish(bool acknowledge) noexcept {
        if (!active_ || state_ == nullptr || data_ == nullptr) {
            return channel_status::stale_delivery;
        }
        auto& control = state_->control_header();
        if (control.generation.load(std::memory_order_acquire) != generation_) {
            reader_held_ = false;
            active_ = false;
            return channel_status::generation_changed;
        }
        const auto channel_state = static_cast<managed_channel_state>(
            control.state.load(std::memory_order_acquire));
        if (channel_state != managed_channel_state::healthy &&
            channel_state != managed_channel_state::closed &&
            channel_state != managed_channel_state::replaying) {
            reader_held_ = false;
            active_ = false;
            return channel_status::broken;
        }
        if (!state_->participant_valid()) {
            reader_held_ = false;
            active_ = false;
            return channel_status::participant_expired;
        }
        try {
            detail::managed_data_lock lock(control, *data_);
            if (!lock.usable) {
                reader_held_ = false;
                active_ = false;
                return channel_status::broken;
            }
            if (!detail::managed_counts_valid(*data_->header)) {
                (void)detail::mark_control_broken(control, break_reason::corrupt_data);
                reader_held_ = false;
                active_ = false;
                return channel_status::broken;
            }
            auto& slot = data_->slots[slot_index_];
            if (slot.sequence != sequence_ || slot.message_id != message_id_ ||
                (slot.state !=
                     static_cast<std::uint32_t>(detail::managed_slot_state::inflight) &&
                 slot.state !=
                     static_cast<std::uint32_t>(detail::managed_slot_state::acknowledged))) {
                reader_held_ = false;
                active_ = false;
                return channel_status::stale_delivery;
            }
            if (data_->header->inflight_messages == 0 ||
                slot.readers < static_cast<std::uint32_t>(reader_held_)) {
                (void)detail::mark_control_broken(control, break_reason::corrupt_data);
                reader_held_ = false;
                active_ = false;
                return channel_status::broken;
            }

            if (reader_held_ && slot.readers != 0) {
                --slot.readers;
                reader_held_ = false;
            }
            if (acknowledge) {
                if (slot.acknowledgment_pending == 0) {
                    slot.acknowledgment_pending = 1;
                    slot.state = static_cast<std::uint32_t>(
                        detail::managed_slot_state::acknowledged);
                    control.counters.acknowledged_messages.fetch_add(
                        1, std::memory_order_relaxed);
                }
                if (slot.readers == 0) {
                    detail::free_managed_slot(*data_, slot);
                }
            } else {
                if (slot.delivery_attempt != attempt_ ||
                    slot.acknowledgment_pending != 0) {
                    active_ = false;
                    return channel_status::stale_delivery;
                }
                slot.state = static_cast<std::uint32_t>(detail::managed_slot_state::ready);
                slot.owner_session = 0;
                slot.owner_participant = 0;
                slot.lease_deadline_ns = 0;
                --data_->header->inflight_messages;
                ++data_->header->ready_messages;
                control.counters.negatively_acknowledged_messages.fetch_add(
                    1, std::memory_order_relaxed);
            }
            active_ = false;
            detail::signal_managed_event(control);
            if (control.generation.load(std::memory_order_acquire) != generation_) {
                return channel_status::generation_changed;
            }
            const auto completed_state = static_cast<managed_channel_state>(
                control.state.load(std::memory_order_acquire));
            return completed_state == managed_channel_state::healthy ||
                           completed_state == managed_channel_state::closed ||
                           completed_state == managed_channel_state::replaying
                       ? channel_status::success
                       : channel_status::broken;
        } catch (...) {
            (void)detail::mark_control_broken(control, break_reason::corrupt_data);
            reader_held_ = false;
            active_ = false;
            return channel_status::broken;
        }
    }

    void release_reader() noexcept {
        if (!reader_held_ || state_ == nullptr || data_ == nullptr) {
            reader_held_ = false;
            return;
        }
        auto& control = state_->control_header();
        const auto channel_state = static_cast<managed_channel_state>(
            control.state.load(std::memory_order_acquire));
        if (!state_->participant_valid() ||
            control.generation.load(std::memory_order_acquire) != generation_ ||
            (channel_state != managed_channel_state::healthy &&
             channel_state != managed_channel_state::closed &&
             channel_state != managed_channel_state::replaying)) {
            reader_held_ = false;
            return;
        }
        try {
            detail::managed_data_lock lock(control, *data_);
            if (lock.usable) {
                auto& slot = data_->slots[slot_index_];
                if (slot.sequence == sequence_ && slot.message_id == message_id_ &&
                    slot.readers != 0) {
                    --slot.readers;
                    if (slot.readers == 0 && slot.acknowledgment_pending != 0) {
                        detail::free_managed_slot(*data_, slot);
                        detail::signal_managed_event(control);
                    }
                }
            }
        } catch (...) {
            (void)detail::mark_control_broken(control, break_reason::corrupt_data);
        }
        reader_held_ = false;
    }

    void move_from(managed_delivery&& other) noexcept {
        state_ = std::move(other.state_);
        data_ = std::move(other.data_);
        owned_payload_ = std::move(other.owned_payload_);
        slot_index_ = other.slot_index_;
        generation_ = other.generation_;
        sequence_ = other.sequence_;
        message_id_ = other.message_id_;
        attempt_ = other.attempt_;
        payload_size_ = other.payload_size_;
        loaned_ = other.loaned_;
        reader_held_ = std::exchange(other.reader_held_, false);
        active_ = std::exchange(other.active_, false);
    }

    std::shared_ptr<detail::managed_local_state> state_{};
    std::shared_ptr<detail::managed_data_mapping> data_{};
    std::vector<std::byte> owned_payload_{};
    std::size_t slot_index_{};
    std::uint64_t generation_{};
    std::uint64_t sequence_{};
    std::uint64_t message_id_{};
    std::uint32_t attempt_{};
    std::size_t payload_size_{};
    bool loaned_{};
    bool reader_held_{};
    bool active_{};
};

struct managed_delivery_result {
    channel_status code{channel_status::would_block};
    std::optional<managed_delivery> value{};

    [[nodiscard]] bool has_value() const noexcept { return value.has_value(); }
    [[nodiscard]] explicit operator bool() const noexcept { return has_value(); }
    [[nodiscard]] managed_delivery& operator*() & { return *value; }
    [[nodiscard]] const managed_delivery& operator*() const& { return *value; }
    [[nodiscard]] managed_delivery* operator->() { return std::addressof(*value); }
    [[nodiscard]] const managed_delivery* operator->() const {
        return std::addressof(*value);
    }
};

struct rebuild_result {
    channel_status code{channel_status::would_block};
    std::uint64_t previous_generation{};
    std::uint64_t generation{};
};

class managed_byte_channel {
public:
    managed_byte_channel() noexcept = default;
    ~managed_byte_channel() = default;

    managed_byte_channel(const managed_byte_channel&) = delete;
    managed_byte_channel& operator=(const managed_byte_channel&) = delete;
    managed_byte_channel(managed_byte_channel&&) noexcept = default;
    managed_byte_channel& operator=(managed_byte_channel&&) noexcept = default;

    [[nodiscard]] static managed_byte_channel create(
        std::string_view name,
        managed_channel_options options = {}) {
        auto control = detail::create_managed_control(name, options);
        auto state = detail::make_managed_local_state(
            std::move(control), options.role, options.monitor_peers);
        return managed_byte_channel(std::move(state));
    }

    [[nodiscard]] static managed_byte_channel open(
        std::string_view name,
        managed_open_options options = {}) {
        auto control = detail::open_managed_control(name, options.protocol);
        auto state = detail::make_managed_local_state(
            std::move(control), options.role, options.monitor_peers);
        return managed_byte_channel(std::move(state));
    }

    [[nodiscard]] bool valid() const noexcept { return state_ != nullptr; }

    [[nodiscard]] const std::string& name() const {
        require_valid();
        return state_->control->base_name;
    }

    [[nodiscard]] std::uint64_t generation() const {
        require_valid();
        return state_->control_header().generation.load(std::memory_order_acquire);
    }

    [[nodiscard]] managed_channel_state state() const {
        require_valid();
        return static_cast<managed_channel_state>(
            state_->control_header().state.load(std::memory_order_acquire));
    }

    [[nodiscard]] break_reason reason() const {
        require_valid();
        return static_cast<break_reason>(
            state_->control_header().reason.load(std::memory_order_acquire));
    }

    [[nodiscard]] std::size_t capacity() const {
        require_valid();
        return static_cast<std::size_t>(state_->control_header().message_capacity);
    }

    [[nodiscard]] std::size_t max_message_size() const {
        require_valid();
        return static_cast<std::size_t>(state_->control_header().max_message_size);
    }

    [[nodiscard]] protocol_descriptor protocol() const {
        require_valid();
        return state_->protocol();
    }

    [[nodiscard]] managed_reservation_result try_reserve(std::uint64_t message_id = 0) {
        require_valid();
        return try_reserve_impl(message_id);
    }

    [[nodiscard]] managed_reservation_result reserve() {
        return reserve_for_impl(0, std::nullopt);
    }

    [[nodiscard]] managed_reservation_result reserve(std::uint64_t message_id) {
        return reserve_for_impl(message_id, std::nullopt);
    }

    [[nodiscard]] managed_reservation_result reserve_for(
        std::chrono::nanoseconds timeout,
        std::uint64_t message_id = 0) {
        return reserve_for_impl(message_id, detail::make_deadline(timeout));
    }

    [[nodiscard]] channel_status try_send(
        std::span<const std::byte> message,
        std::uint64_t message_id = 0) {
        if (message.size() > max_message_size()) {
            return channel_status::message_too_large;
        }
        auto reservation = try_reserve(message_id);
        if (!reservation) {
            return reservation.code;
        }
        if (!message.empty()) {
            std::memcpy(reservation->buffer().data(), message.data(), message.size());
        }
        return reservation->commit(message.size());
    }

    [[nodiscard]] channel_status try_send(
        std::string_view message,
        std::uint64_t message_id = 0) {
        return try_send(as_bytes(message), message_id);
    }

    [[nodiscard]] channel_status send(
        std::span<const std::byte> message,
        std::uint64_t message_id = 0) {
        if (message.size() > max_message_size()) {
            return channel_status::message_too_large;
        }
        auto reservation = reserve(message_id);
        if (!reservation) {
            return reservation.code;
        }
        if (!message.empty()) {
            std::memcpy(reservation->buffer().data(), message.data(), message.size());
        }
        return reservation->commit(message.size());
    }

    [[nodiscard]] channel_status send(
        std::string_view message,
        std::uint64_t message_id = 0) {
        return send(as_bytes(message), message_id);
    }

    [[nodiscard]] channel_status send_for(
        std::span<const std::byte> message,
        std::chrono::nanoseconds timeout,
        std::uint64_t message_id = 0) {
        if (message.size() > max_message_size()) {
            return channel_status::message_too_large;
        }
        auto reservation = reserve_for(timeout, message_id);
        if (!reservation) {
            return reservation.code;
        }
        if (!message.empty()) {
            std::memcpy(reservation->buffer().data(), message.data(), message.size());
        }
        return reservation->commit(message.size());
    }

    [[nodiscard]] channel_status send_for(
        std::string_view message,
        std::chrono::nanoseconds timeout,
        std::uint64_t message_id = 0) {
        return send_for(as_bytes(message), timeout, message_id);
    }

    [[nodiscard]] batch_send_result try_send_batch(std::span<const outbound_message> messages) {
        batch_send_result result{channel_status::success, 0};
        for (const auto& message : messages) {
            result.code = try_send(message.payload, message.message_id);
            if (result.code != channel_status::success) {
                return result;
            }
            ++result.sent;
        }
        return result;
    }

    [[nodiscard]] managed_delivery_result try_receive() {
        require_valid();
        return try_receive_impl(false);
    }

    [[nodiscard]] managed_delivery_result try_receive_loaned() {
        require_valid();
        return try_receive_impl(true);
    }

    [[nodiscard]] managed_delivery_result receive() {
        return receive_for_impl(false, std::nullopt);
    }

    [[nodiscard]] managed_delivery_result receive_loaned() {
        return receive_for_impl(true, std::nullopt);
    }

    [[nodiscard]] managed_delivery_result receive_for(std::chrono::nanoseconds timeout) {
        return receive_for_impl(false, detail::make_deadline(timeout));
    }

    [[nodiscard]] managed_delivery_result receive_loaned_for(
        std::chrono::nanoseconds timeout) {
        return receive_for_impl(true, detail::make_deadline(timeout));
    }

    [[nodiscard]] std::vector<managed_delivery> try_receive_batch(std::size_t maximum) {
        std::vector<managed_delivery> result;
        result.reserve(maximum);
        while (result.size() < maximum) {
            auto delivery = try_receive();
            if (!delivery) {
                break;
            }
            result.push_back(std::move(*delivery));
        }
        return result;
    }

    [[nodiscard]] bool close() {
        require_valid();
        if (!state_->participant_valid()) {
            return false;
        }
        auto& control = state_->control_header();
        if (control.state.load(std::memory_order_acquire) !=
            static_cast<std::uint32_t>(managed_channel_state::healthy)) {
            return false;
        }
        auto data = state_->current_data();
        if (data == nullptr) {
            return false;
        }
        detail::managed_data_lock lock(control, *data);
        if (!lock.usable) {
            return false;
        }
        if (!detail::managed_counts_valid(*data->header)) {
            (void)detail::mark_control_broken(
                control, break_reason::corrupt_data, state_->session);
            return false;
        }
        auto expected = static_cast<std::uint32_t>(managed_channel_state::healthy);
        if (!control.state.compare_exchange_strong(
                expected,
                static_cast<std::uint32_t>(managed_channel_state::closed),
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            return false;
        }
        detail::signal_managed_event(control);
        return true;
    }

    [[nodiscard]] bool mark_broken(break_reason reason = break_reason::manual) {
        require_valid();
        if (!state_->participant_valid()) {
            return false;
        }
        return detail::mark_control_broken(state_->control_header(), reason, state_->session);
    }

    [[nodiscard]] bool supervise_once() {
        require_valid();
        if (!state_->participant_valid()) {
            return false;
        }
        const auto before = state();
        state_->scan_participants();
        state_->scan_reservations();
        return before != managed_channel_state::broken &&
               state() == managed_channel_state::broken;
    }

    [[nodiscard]] managed_channel_stats stats() const {
        require_valid();
        managed_channel_stats result{};
        auto& control = state_->control_header();
        result.state = static_cast<managed_channel_state>(
            control.state.load(std::memory_order_acquire));
        result.reason = static_cast<break_reason>(
            control.reason.load(std::memory_order_acquire));
        result.generation = control.generation.load(std::memory_order_acquire);
        result.replay_owner_session =
            control.replay_owner_session.load(std::memory_order_acquire);
        result.failed_participant_session =
            control.failed_participant_session.load(std::memory_order_acquire);
        result.message_capacity = static_cast<std::size_t>(control.message_capacity);
        result.max_message_size = static_cast<std::size_t>(control.max_message_size);
        result.waiting_senders = control.counters.waiting_senders.load(std::memory_order_relaxed);
        result.waiting_receivers =
            control.counters.waiting_receivers.load(std::memory_order_relaxed);
        result.sent_messages = control.counters.sent_messages.load(std::memory_order_relaxed);
        result.sent_bytes = control.counters.sent_bytes.load(std::memory_order_relaxed);
        result.delivered_messages =
            control.counters.delivered_messages.load(std::memory_order_relaxed);
        result.delivered_bytes =
            control.counters.delivered_bytes.load(std::memory_order_relaxed);
        result.acknowledged_messages =
            control.counters.acknowledged_messages.load(std::memory_order_relaxed);
        result.redelivered_messages =
            control.counters.redelivered_messages.load(std::memory_order_relaxed);
        result.negatively_acknowledged_messages =
            control.counters.negatively_acknowledged_messages.load(std::memory_order_relaxed);
        result.cancelled_reservations =
            control.counters.cancelled_reservations.load(std::memory_order_relaxed);
        result.send_timeouts = control.counters.send_timeouts.load(std::memory_order_relaxed);
        result.receive_timeouts =
            control.counters.receive_timeouts.load(std::memory_order_relaxed);
        result.broken_generations =
            control.counters.broken_generations.load(std::memory_order_relaxed);
        result.rebuilt_generations =
            control.counters.rebuilt_generations.load(std::memory_order_relaxed);
        result.replay_failures =
            control.counters.replay_failures.load(std::memory_order_relaxed);

        for (std::size_t index = 0; index < control.max_participants; ++index) {
            const auto participant_state =
                control.participants[index].state.load(std::memory_order_acquire);
            if (participant_state == detail::participant_active) {
                ++result.active_participants;
            } else if (participant_state == detail::participant_stale) {
                ++result.stale_participants;
            }
        }

        try {
            auto data = state_->current_data();
            if (data != nullptr) {
                detail::managed_data_lock lock(control, *data, false);
                if (lock.usable) {
                    result.free_slots = static_cast<std::size_t>(data->header->free_slots);
                    result.writing_messages =
                        static_cast<std::size_t>(data->header->writing_messages);
                    result.ready_messages =
                        static_cast<std::size_t>(data->header->ready_messages);
                    result.inflight_messages =
                        static_cast<std::size_t>(data->header->inflight_messages);
                    const auto now = detail::monotonic_now_ns();
                    std::uint64_t oldest = 0;
                    for (std::size_t index = 0; index < data->header->message_capacity; ++index) {
                        const auto& slot = data->slots[index];
                        if (slot.state !=
                                static_cast<std::uint32_t>(detail::managed_slot_state::free) &&
                            slot.created_ns != 0 && (oldest == 0 || slot.created_ns < oldest)) {
                            oldest = slot.created_ns;
                        }
                    }
                    if (oldest != 0 && now >= oldest) {
                        result.oldest_message_age = detail::managed_elapsed(now, oldest);
                    }
                }
            }
        } catch (...) {
        }
        return result;
    }

    [[nodiscard]] std::vector<managed_participant_info> participants() const {
        require_valid();
        const auto& control = state_->control_header();
        const auto now = detail::monotonic_now_ns();
        std::vector<managed_participant_info> result;
        result.reserve(control.max_participants);
        for (std::size_t index = 0; index < control.max_participants; ++index) {
            const auto& slot = control.participants[index];
            const auto slot_state = slot.state.load(std::memory_order_acquire);
            if (slot_state != detail::participant_active &&
                slot_state != detail::participant_stale) {
                continue;
            }
            managed_participant_info participant{};
            participant.slot = index;
            participant.role = static_cast<participant_role>(
                slot.role.load(std::memory_order_relaxed));
            participant.pid = static_cast<::pid_t>(slot.pid.load(std::memory_order_relaxed));
            participant.session = slot.session.load(std::memory_order_relaxed);
            participant.observed_generation =
                slot.observed_generation.load(std::memory_order_relaxed);
            participant.stale = slot_state == detail::participant_stale;
            participant.registered_for = detail::managed_elapsed(
                now, slot.process_start_ns.load(std::memory_order_relaxed));
            participant.heartbeat_age = detail::managed_elapsed(
                now, slot.heartbeat_ns.load(std::memory_order_relaxed));
            if (slot.state.load(std::memory_order_acquire) == slot_state) {
                result.push_back(participant);
            }
        }
        return result;
    }

    [[nodiscard]] rebuild_result rebuild() {
        return rebuild_with_replay([](managed_byte_channel&, std::uint64_t, std::uint64_t) {});
    }

    template <typename Replay>
    [[nodiscard]] rebuild_result rebuild_with_replay(Replay&& replay) {
        require_valid();
        auto& control = state_->control_header();
        if (!state_->participant_valid()) {
            const auto current = control.generation.load(std::memory_order_acquire);
            return {channel_status::participant_expired, current, current};
        }
        const auto initial_state = static_cast<managed_channel_state>(
            control.state.load(std::memory_order_acquire));
        if (initial_state != managed_channel_state::broken &&
            initial_state != managed_channel_state::breaking) {
            return {
                channel_status::would_block,
                control.generation.load(std::memory_order_acquire),
                control.generation.load(std::memory_order_acquire),
            };
        }

        const int raw_fd =
            ::shm_open(state_->control->object_name.c_str(), O_RDWR | O_CLOEXEC, 0);
        if (raw_fd < 0) {
            detail::throw_errno("shm_open(rebuild managed channel)", state_->control->object_name);
        }
        detail::unique_fd fd(raw_fd);
        if (::flock(fd.get(), LOCK_EX | LOCK_NB) != 0) {
            if (errno == EWOULDBLOCK) {
                return {
                    channel_status::would_block,
                    control.generation.load(std::memory_order_acquire),
                    control.generation.load(std::memory_order_acquire),
                };
            }
            detail::throw_errno("flock(rebuild managed channel)", state_->control->object_name);
        }

        const auto old_generation = control.generation.load(std::memory_order_acquire);
        if (!state_->participant_valid()) {
            detail::lock_fd(fd.get(), LOCK_UN);
            return {channel_status::participant_expired, old_generation, old_generation};
        }
        if (control.state.load(std::memory_order_acquire) ==
            static_cast<std::uint32_t>(managed_channel_state::breaking)) {
            auto expected_reason = static_cast<std::uint32_t>(break_reason::none);
            (void)control.reason.compare_exchange_strong(
                expected_reason,
                static_cast<std::uint32_t>(break_reason::corrupt_data),
                std::memory_order_acq_rel,
                std::memory_order_relaxed);
            (void)detail::finalize_control_breaking(control);
        }
        if (control.state.load(std::memory_order_acquire) !=
            static_cast<std::uint32_t>(managed_channel_state::broken)) {
            detail::lock_fd(fd.get(), LOCK_UN);
            return {channel_status::generation_changed, old_generation,
                    control.generation.load(std::memory_order_acquire)};
        }
        if (old_generation == std::numeric_limits<std::uint64_t>::max()) {
            detail::lock_fd(fd.get(), LOCK_UN);
            throw std::overflow_error("shmchan: managed channel generation exhausted");
        }
        const auto stale_previous_generation =
            control.previous_generation.load(std::memory_order_acquire);
        if (stale_previous_generation != 0 &&
            stale_previous_generation != old_generation) {
            detail::managed_unlink_if_exists(detail::managed_data_name(
                state_->control->base_name, stale_previous_generation));
        }
        struct stat control_attributes {};
        if (::fstat(fd.get(), std::addressof(control_attributes)) != 0) {
            (void)::flock(fd.get(), LOCK_UN);
            detail::throw_errno(
                "fstat(rebuild managed channel)", state_->control->object_name);
        }
        const auto permissions = static_cast<::mode_t>(control_attributes.st_mode & 0777U);
        const auto new_generation = old_generation + 1;
        control.building_generation.store(new_generation, std::memory_order_release);
        const auto new_name = detail::managed_data_name(state_->control->base_name, new_generation);
        detail::managed_unlink_if_exists(new_name);

        std::shared_ptr<detail::managed_data_mapping> new_data;
        try {
            new_data = detail::create_managed_data(
                state_->control->base_name,
                new_generation,
                static_cast<std::size_t>(control.message_capacity),
                static_cast<std::size_t>(control.max_message_size),
                state_->protocol(),
                permissions);
            const int data_fd = ::shm_open(new_name.c_str(), O_RDWR | O_CLOEXEC, 0);
            if (data_fd < 0) {
                detail::throw_errno("shm_open(set rebuilt permissions)", new_name);
            }
            detail::unique_fd rebuilt_fd(data_fd);
            if (::fchmod(rebuilt_fd.get(), permissions) != 0) {
                detail::throw_errno("fchmod(rebuilt managed data)", new_name);
            }
        } catch (...) {
            control.building_generation.store(0, std::memory_order_release);
            (void)::flock(fd.get(), LOCK_UN);
            detail::managed_unlink_if_exists(new_name);
            throw;
        }

        const auto now = detail::monotonic_now_ns();
        for (std::size_t index = 0; index < control.max_participants; ++index) {
            auto& participant = control.participants[index];
            auto participant_state = participant.state.load(std::memory_order_acquire);
            if (participant_state == detail::participant_claiming ||
                participant_state == detail::participant_releasing) {
                detail::managed_local_state::clear_participant(participant);
                participant.state.store(detail::participant_free, std::memory_order_release);
                continue;
            }
            if (participant_state == detail::participant_active) {
                const auto heartbeat =
                    participant.heartbeat_ns.load(std::memory_order_acquire);
                if (heartbeat != 0 && now >= heartbeat &&
                    now - heartbeat > control.participant_timeout_ns) {
                    auto expected = detail::participant_active;
                    if (participant.state.compare_exchange_strong(
                            expected,
                            detail::participant_stale,
                            std::memory_order_acq_rel,
                            std::memory_order_relaxed)) {
                        participant_state = detail::participant_stale;
                    }
                }
            }
            if (participant_state == detail::participant_stale) {
                detail::managed_local_state::clear_participant(participant);
                participant.state.store(detail::participant_free, std::memory_order_release);
            }
        }

        {
            std::scoped_lock lock(state_->data_mutex);
            state_->data = new_data;
        }
        control.previous_generation.store(old_generation, std::memory_order_relaxed);
        control.reason.store(
            static_cast<std::uint32_t>(break_reason::none), std::memory_order_relaxed);
        control.failed_participant_session.store(0, std::memory_order_relaxed);
        control.replay_owner_session.store(state_->session, std::memory_order_release);
        control.generation.store(new_generation, std::memory_order_release);
        control.building_generation.store(0, std::memory_order_release);
        control.state.store(
            static_cast<std::uint32_t>(managed_channel_state::replaying),
            std::memory_order_release);
        detail::signal_managed_event(control);
        (void)::flock(fd.get(), LOCK_UN);

        const auto old_name =
            detail::managed_data_name(state_->control->base_name, old_generation);
        detail::managed_unlink_if_exists(old_name);

        try {
            std::invoke(std::forward<Replay>(replay), *this, old_generation, new_generation);
        } catch (...) {
            control.counters.replay_failures.fetch_add(1, std::memory_order_relaxed);
            control.replay_owner_session.store(0, std::memory_order_release);
            (void)detail::mark_control_broken(control, break_reason::replay_failed);
            throw;
        }
        if (control.generation.load(std::memory_order_acquire) != new_generation) {
            return {
                channel_status::generation_changed,
                old_generation,
                control.generation.load(std::memory_order_acquire),
            };
        }
        if (control.replay_owner_session.load(std::memory_order_acquire) != state_->session) {
            return {channel_status::broken, old_generation, new_generation};
        }
        control.replay_owner_session.store(0, std::memory_order_release);
        auto expected_replaying =
            static_cast<std::uint32_t>(managed_channel_state::replaying);
        if (!control.state.compare_exchange_strong(
                expected_replaying,
                static_cast<std::uint32_t>(managed_channel_state::healthy),
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            return {channel_status::broken, old_generation, new_generation};
        }
        control.counters.rebuilt_generations.fetch_add(1, std::memory_order_relaxed);
        detail::signal_managed_event(control);
        return {channel_status::success, old_generation, new_generation};
    }

    [[nodiscard]] bool unlink() const {
        require_valid();
        if (!state_->participant_valid()) {
            return false;
        }
        return unlink(state_->control->base_name);
    }

    [[nodiscard]] static bool unlink(std::string_view name) {
        const auto base = detail::normalize_name(name);
        const auto control_name = detail::managed_control_name(base);
        const int raw_fd = ::shm_open(control_name.c_str(), O_RDWR | O_CLOEXEC, 0);
        if (raw_fd < 0) {
            if (errno == ENOENT) {
                return false;
            }
            detail::throw_errno("shm_open(unlink managed control)", control_name);
        }
        detail::unique_fd fd(raw_fd);
        detail::lock_fd(fd.get(), LOCK_EX);

        std::uint64_t current = 0;
        std::uint64_t previous = 0;
        std::uint64_t building = 0;
        struct stat attributes {};
        if (::fstat(fd.get(), std::addressof(attributes)) != 0) {
            detail::throw_errno("fstat(unlink managed control)", control_name);
        }
        if (attributes.st_size ==
            static_cast<::off_t>(sizeof(detail::managed_control_header))) {
            void* address = ::mmap(
                nullptr,
                sizeof(detail::managed_control_header),
                PROT_READ | PROT_WRITE,
                MAP_SHARED,
                fd.get(),
                0);
            if (address == MAP_FAILED) {
                detail::throw_errno("mmap(unlink managed control)", control_name);
            }
            auto* header = static_cast<detail::managed_control_header*>(address);
            if (header->magic == detail::managed_control_magic &&
                header->mapping_size == sizeof(detail::managed_control_header)) {
                current = header->generation.load(std::memory_order_acquire);
                previous = header->previous_generation.load(std::memory_order_acquire);
                building = header->building_generation.load(std::memory_order_acquire);
                header->state.store(
                    static_cast<std::uint32_t>(managed_channel_state::destroying),
                    std::memory_order_release);
                detail::signal_managed_event(*header);
            }
            (void)::munmap(address, sizeof(detail::managed_control_header));
        }

        if (current != 0) {
            detail::managed_unlink_if_exists(detail::managed_data_name(base, current));
        }
        if (previous != 0 && previous != current) {
            detail::managed_unlink_if_exists(detail::managed_data_name(base, previous));
        }
        if (building != 0 && building != current && building != previous) {
            detail::managed_unlink_if_exists(detail::managed_data_name(base, building));
        }
        if (::shm_unlink(control_name.c_str()) == 0) {
            detail::lock_fd(fd.get(), LOCK_UN);
            return true;
        }
        if (errno == ENOENT) {
            detail::lock_fd(fd.get(), LOCK_UN);
            return false;
        }
        detail::throw_errno("shm_unlink(managed control)", control_name);
    }

private:
    explicit managed_byte_channel(std::shared_ptr<detail::managed_local_state> state) noexcept
        : state_(std::move(state)) {}

    void require_valid() const {
        if (state_ == nullptr) {
            throw std::logic_error("shmchan: operation on an empty managed channel");
        }
    }

    [[nodiscard]] static std::span<const std::byte> as_bytes(std::string_view value) noexcept {
        return {
            reinterpret_cast<const std::byte*>(value.data()),
            value.size(),
        };
    }

    [[nodiscard]] managed_reservation_result try_reserve_impl(std::uint64_t requested_id) {
        auto& control = state_->control_header();
        const auto status = detail::control_operation_status(*state_, true);
        if (status != channel_status::success) {
            return {status, std::nullopt};
        }
        auto data = state_->current_data();
        if (data == nullptr) {
            const auto current_status = detail::control_operation_status(*state_, true);
            return {
                current_status == channel_status::success
                    ? channel_status::broken
                    : current_status,
                std::nullopt,
            };
        }
        const auto operation_generation = data->header->generation;
        detail::managed_data_lock lock(control, *data);
        if (!lock.usable) {
            return {channel_status::broken, std::nullopt};
        }
        const auto locked_state = static_cast<managed_channel_state>(
            control.state.load(std::memory_order_acquire));
        const bool replay_owner =
            locked_state == managed_channel_state::replaying &&
            control.replay_owner_session.load(std::memory_order_acquire) == state_->session;
        if (locked_state != managed_channel_state::healthy && !replay_owner) {
            return {
                locked_state == managed_channel_state::closed
                    ? channel_status::closed
                    : channel_status::broken,
                std::nullopt,
            };
        }
        if (!detail::managed_counts_valid(*data->header)) {
            (void)detail::mark_control_broken(
                control, break_reason::corrupt_data, state_->session);
            return {channel_status::broken, std::nullopt};
        }
        if (control.generation.load(std::memory_order_acquire) != operation_generation) {
            return {channel_status::generation_changed, std::nullopt};
        }
        if (data->header->next_sequence == std::numeric_limits<std::uint64_t>::max() ||
            (requested_id == 0 &&
             data->header->next_message_id == std::numeric_limits<std::uint64_t>::max())) {
            (void)detail::mark_control_broken(
                control, break_reason::sequence_exhausted, state_->session);
            return {channel_status::broken, std::nullopt};
        }
        if (requested_id != 0) {
            for (std::size_t index = 0; index < data->header->message_capacity; ++index) {
                const auto& existing = data->slots[index];
                if (existing.state >
                    static_cast<std::uint32_t>(detail::managed_slot_state::acknowledged)) {
                    (void)detail::mark_control_broken(
                        control, break_reason::corrupt_data, state_->session);
                    return {channel_status::broken, std::nullopt};
                }
                if (existing.state !=
                        static_cast<std::uint32_t>(detail::managed_slot_state::free) &&
                    existing.message_id == requested_id) {
                    return {channel_status::duplicate_message, std::nullopt};
                }
            }
        }
        if (data->header->free_slots == 0) {
            return {channel_status::would_block, std::nullopt};
        }
        const auto start = static_cast<std::size_t>(
            data->header->next_sequence % data->header->message_capacity);
        for (std::size_t offset = 0; offset < data->header->message_capacity; ++offset) {
            const auto index = (start + offset) % data->header->message_capacity;
            auto& slot = data->slots[index];
            if (slot.state >
                static_cast<std::uint32_t>(detail::managed_slot_state::acknowledged)) {
                (void)detail::mark_control_broken(
                    control, break_reason::corrupt_data, state_->session);
                return {channel_status::broken, std::nullopt};
            }
            if (slot.state !=
                static_cast<std::uint32_t>(detail::managed_slot_state::free)) {
                continue;
            }
            const auto now = detail::monotonic_now_ns();
            slot.state = static_cast<std::uint32_t>(detail::managed_slot_state::writing);
            slot.payload_size = 0;
            slot.sequence = data->header->next_sequence++;
            if (requested_id != 0) {
                slot.message_id = requested_id;
            } else {
                slot.message_id =
                    (operation_generation * 0x9e3779b97f4a7c15ULL) ^
                    data->header->next_message_id++;
                if (slot.message_id == 0) {
                    slot.message_id = data->header->next_message_id++;
                }
            }
            slot.owner_session = state_->session;
            slot.owner_participant = static_cast<std::uint32_t>(state_->participant_index);
            slot.created_ns = now;
            slot.lease_deadline_ns =
                detail::managed_deadline_after(now, control.reservation_timeout_ns);
            slot.delivery_attempt = 0;
            slot.readers = 0;
            slot.acknowledgment_pending = 0;
            --data->header->free_slots;
            ++data->header->writing_messages;
            return {
                channel_status::success,
                managed_send_reservation{
                    state_,
                    std::move(data),
                    index,
                    operation_generation,
                    slot.sequence,
                    slot.message_id},
            };
        }
        (void)detail::mark_control_broken(control, break_reason::corrupt_data);
        return {channel_status::broken, std::nullopt};
    }

    [[nodiscard]] managed_reservation_result reserve_for_impl(
        std::uint64_t message_id,
        std::optional<detail::monotonic_clock::time_point> deadline) {
        require_valid();
        auto& control = state_->control_header();
        for (;;) {
            const auto epoch = control.event_epoch.load(std::memory_order_acquire);
            auto result = try_reserve_impl(message_id);
            if (result.code != channel_status::would_block) {
                return result;
            }
            control.counters.waiting_senders.fetch_add(1, std::memory_order_relaxed);
            detail::futex_wait_result wait_result{};
            try {
                wait_result = deadline.has_value()
                                  ? detail::futex_wait_until(
                                        control.event_epoch, epoch, *deadline)
                                  : detail::futex_wait(
                                        control.event_epoch, epoch, nullptr);
            } catch (...) {
                control.counters.waiting_senders.fetch_sub(1, std::memory_order_relaxed);
                throw;
            }
            control.counters.waiting_senders.fetch_sub(1, std::memory_order_relaxed);
            if (wait_result == detail::futex_wait_result::timed_out) {
                result = try_reserve_impl(message_id);
                if (result.code != channel_status::would_block) {
                    return result;
                }
                control.counters.send_timeouts.fetch_add(1, std::memory_order_relaxed);
                return {channel_status::timed_out, std::nullopt};
            }
        }
    }

    [[nodiscard]] managed_delivery_result try_receive_impl(bool loaned) {
        auto& control = state_->control_header();
        auto operation_status = detail::control_operation_status(*state_, false);
        if (operation_status != channel_status::success &&
            operation_status != channel_status::closed) {
            return {operation_status, std::nullopt};
        }
        auto data = state_->current_data();
        if (data == nullptr) {
            const auto current_status = detail::control_operation_status(*state_, false);
            return {
                current_status == channel_status::success
                    ? channel_status::broken
                    : current_status,
                std::nullopt,
            };
        }
        const auto operation_generation = data->header->generation;
        std::size_t selected = static_cast<std::size_t>(data->header->message_capacity);
        bool redelivery = false;
        std::uint64_t selected_sequence = std::numeric_limits<std::uint64_t>::max();
        std::uint64_t earliest_writing = std::numeric_limits<std::uint64_t>::max();
        const auto now = detail::monotonic_now_ns();
        std::size_t payload_size = 0;
        std::uint64_t message_id = 0;
        std::uint64_t sequence = 0;
        std::uint32_t attempt = 0;

        {
            detail::managed_data_lock lock(control, *data);
            if (!lock.usable) {
                return {channel_status::broken, std::nullopt};
            }
            const auto locked_state = static_cast<managed_channel_state>(
                control.state.load(std::memory_order_acquire));
            operation_status = locked_state == managed_channel_state::closed
                                   ? channel_status::closed
                                   : channel_status::success;
            if (!detail::managed_counts_valid(*data->header)) {
                (void)detail::mark_control_broken(
                    control, break_reason::corrupt_data, state_->session);
                return {channel_status::broken, std::nullopt};
            }
            if (control.generation.load(std::memory_order_acquire) != operation_generation) {
                return {channel_status::generation_changed, std::nullopt};
            }
            for (std::size_t index = 0; index < data->header->message_capacity; ++index) {
                const auto& slot = data->slots[index];
                if (slot.state >
                    static_cast<std::uint32_t>(detail::managed_slot_state::acknowledged)) {
                    (void)detail::mark_control_broken(
                        control, break_reason::corrupt_data, state_->session);
                    return {channel_status::broken, std::nullopt};
                }
                if (slot.state ==
                        static_cast<std::uint32_t>(detail::managed_slot_state::inflight) &&
                    slot.acknowledgment_pending == 0 && slot.lease_deadline_ns != 0 &&
                    now >= slot.lease_deadline_ns && slot.sequence < selected_sequence) {
                    selected = index;
                    selected_sequence = slot.sequence;
                    redelivery = true;
                }
            }
            if (selected == data->header->message_capacity) {
                for (std::size_t index = 0; index < data->header->message_capacity; ++index) {
                    const auto& slot = data->slots[index];
                    if (slot.state ==
                            static_cast<std::uint32_t>(detail::managed_slot_state::writing) &&
                        slot.sequence < earliest_writing) {
                        earliest_writing = slot.sequence;
                    }
                    if (slot.state ==
                            static_cast<std::uint32_t>(detail::managed_slot_state::ready) &&
                        slot.sequence < selected_sequence) {
                        selected = index;
                        selected_sequence = slot.sequence;
                    }
                }
                if (selected != data->header->message_capacity &&
                    earliest_writing < selected_sequence) {
                    selected = static_cast<std::size_t>(data->header->message_capacity);
                }
            }

            if (selected == data->header->message_capacity) {
                if (operation_status == channel_status::closed &&
                    data->header->writing_messages == 0 && data->header->ready_messages == 0 &&
                    data->header->inflight_messages == 0) {
                    return {channel_status::closed, std::nullopt};
                }
                return {channel_status::would_block, std::nullopt};
            }

            auto& slot = data->slots[selected];
            const auto expected_slot_state = redelivery
                                                 ? detail::managed_slot_state::inflight
                                                 : detail::managed_slot_state::ready;
            if (slot.state != static_cast<std::uint32_t>(expected_slot_state) ||
                slot.payload_size > data->header->max_message_size ||
                slot.sequence == 0 || slot.message_id == 0 ||
                slot.readers == std::numeric_limits<std::uint32_t>::max() ||
                (!redelivery && data->header->ready_messages == 0) ||
                (redelivery && data->header->inflight_messages == 0)) {
                (void)detail::mark_control_broken(
                    control, break_reason::corrupt_data, state_->session);
                return {channel_status::broken, std::nullopt};
            }
            if (slot.delivery_attempt == std::numeric_limits<std::uint32_t>::max()) {
                (void)detail::mark_control_broken(
                    control, break_reason::sequence_exhausted, state_->session);
                return {channel_status::broken, std::nullopt};
            }
            if (!redelivery) {
                --data->header->ready_messages;
                ++data->header->inflight_messages;
            } else {
                control.counters.redelivered_messages.fetch_add(1, std::memory_order_relaxed);
            }
            slot.state = static_cast<std::uint32_t>(detail::managed_slot_state::inflight);
            slot.owner_session = state_->session;
            slot.owner_participant = static_cast<std::uint32_t>(state_->participant_index);
            slot.lease_deadline_ns =
                detail::managed_deadline_after(now, control.acknowledgment_timeout_ns);
            ++slot.delivery_attempt;
            ++slot.readers;
            payload_size = slot.payload_size;
            message_id = slot.message_id;
            sequence = slot.sequence;
            attempt = slot.delivery_attempt;
            control.counters.delivered_messages.fetch_add(1, std::memory_order_relaxed);
            control.counters.delivered_bytes.fetch_add(payload_size, std::memory_order_relaxed);
        }

        if (loaned) {
            return {
                channel_status::success,
                managed_delivery{
                    state_,
                    std::move(data),
                    selected,
                    operation_generation,
                    sequence,
                    message_id,
                    attempt,
                    payload_size,
                    true,
                    {}},
            };
        }

        const auto release_copy_reader = [&]() noexcept {
            try {
                detail::managed_data_lock lock(control, *data);
                if (!lock.usable) {
                    return channel_status::broken;
                }
                auto& slot = data->slots[selected];
                if (slot.sequence == sequence && slot.message_id == message_id &&
                    slot.readers != 0) {
                    --slot.readers;
                    if (slot.readers == 0 && slot.acknowledgment_pending != 0) {
                        detail::free_managed_slot(*data, slot);
                        detail::signal_managed_event(control);
                    }
                }
                return channel_status::success;
            } catch (...) {
                (void)detail::mark_control_broken(control, break_reason::corrupt_data);
                return channel_status::broken;
            }
        };

        std::vector<std::byte> payload;
        try {
            payload.resize(payload_size);
            if (payload_size != 0) {
                std::memcpy(payload.data(), data->payload(selected).data(), payload_size);
            }
        } catch (...) {
            (void)release_copy_reader();
            throw;
        }
        const auto release_status = release_copy_reader();
        if (release_status != channel_status::success) {
            return {release_status, std::nullopt};
        }
        return {
            channel_status::success,
            managed_delivery{
                state_,
                std::move(data),
                selected,
                operation_generation,
                sequence,
                message_id,
                attempt,
                payload_size,
                false,
                std::move(payload)},
        };
    }

    [[nodiscard]] managed_delivery_result receive_for_impl(
        bool loaned,
        std::optional<detail::monotonic_clock::time_point> deadline) {
        require_valid();
        auto& control = state_->control_header();
        for (;;) {
            const auto epoch = control.event_epoch.load(std::memory_order_acquire);
            auto result = try_receive_impl(loaned);
            if (result.code != channel_status::would_block) {
                return result;
            }
            control.counters.waiting_receivers.fetch_add(1, std::memory_order_relaxed);
            detail::futex_wait_result wait_result{};
            try {
                const auto poll_interval = std::chrono::nanoseconds{
                    static_cast<std::int64_t>(std::min(
                        control.heartbeat_interval_ns,
                        control.acknowledgment_timeout_ns))};
                auto poll_deadline = detail::monotonic_clock::now() + poll_interval;
                if (deadline.has_value() && *deadline < poll_deadline) {
                    poll_deadline = *deadline;
                }
                wait_result = detail::futex_wait_until(
                    control.event_epoch, epoch, poll_deadline);
            } catch (...) {
                control.counters.waiting_receivers.fetch_sub(1, std::memory_order_relaxed);
                throw;
            }
            control.counters.waiting_receivers.fetch_sub(1, std::memory_order_relaxed);
            if (wait_result == detail::futex_wait_result::timed_out) {
                result = try_receive_impl(loaned);
                if (result.code != channel_status::would_block) {
                    return result;
                }
                if (deadline.has_value() && detail::monotonic_clock::now() >= *deadline) {
                    control.counters.receive_timeouts.fetch_add(1, std::memory_order_relaxed);
                    return {channel_status::timed_out, std::nullopt};
                }
            }
        }
    }

    std::shared_ptr<detail::managed_local_state> state_{};
};

} // namespace shmchan
