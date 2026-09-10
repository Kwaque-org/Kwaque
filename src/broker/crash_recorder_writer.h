#pragma once

#include <sys/types.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>

namespace kwaque::broker::detail {

inline constexpr std::size_t crash_field_capacity = 4096;
inline constexpr std::size_t crash_serialization_capacity = 9216;
inline constexpr std::size_t crash_v1_header_size = 40;
inline constexpr std::size_t crash_header_size = 44;
inline constexpr std::size_t crash_version_capacity = 128;
inline constexpr std::size_t crash_architecture_capacity = 16;

// Wire v2: 8-byte magic, then little-endian milliseconds(u64), signal(u32),
// shard(u32), kind(u32), message/stack/version/architecture lengths(u32 each).
// The four byte strings follow, then a little-endian CRC32C of all preceding
// bytes. Retention also accepts v1, which has no architecture field or length.

[[nodiscard]] constexpr std::string_view crash_architecture() noexcept {
#if defined(__x86_64__)
    return "x86_64";
#elif defined(__aarch64__)
    return "aarch64";
#else
    return "unknown";
#endif
}

enum class crash_kind : std::uint32_t {
    startup_failure = 1,
    abort = 2,
    illegal_instruction = 3,
    segmentation_fault = 4,
};

struct crash_write_operations final {
    ssize_t (*write)(int, const void*, std::size_t) noexcept;
    int (*fsync)(int) noexcept;
};

// Owns all writable storage before publication to signal handlers. A successful
// claim transfers exclusive access to the prepared message and serialization.
class prepared_crash_writer final {
public:
    prepared_crash_writer();
    ~prepared_crash_writer();
    prepared_crash_writer(const prepared_crash_writer&) = delete;
    prepared_crash_writer& operator=(const prepared_crash_writer&) = delete;

    // Takes descriptor ownership only after successfully preparing storage.
    // Initialization and destruction require signal handlers to be detached.
    void initialize(int descriptor, std::string_view version);

    // Async-signal safe after initialize(). Returns false if consumed/released.
    [[nodiscard]] bool record(
      crash_kind kind,
      int signal,
      std::uint32_t shard,
      bool capture_backtrace = true) noexcept;

    // Claims an unused placeholder for removal, racing safely with record().
    [[nodiscard]] bool release() noexcept;
    void close() noexcept;

    [[nodiscard]] bool initialized() const noexcept;
    [[nodiscard]] bool ready() const noexcept;

    // Only for deterministic syscall-failure tests, before initialize().
    explicit prepared_crash_writer(crash_write_operations operations);

private:
    enum class state : std::uint8_t {
        uninitialized,
        initialized,
        filled,
        written,
        released
    };
    static_assert(std::atomic<state>::is_always_lock_free);
    struct storage;

    [[nodiscard]] bool write_record() noexcept;

    std::atomic<state> state_{state::uninitialized};
    std::unique_ptr<storage> storage_;
    crash_write_operations operations_;
    int descriptor_{-1};
};

// Validates the bounded record's version, field lengths and CRC32C trailer.
// Malformed, torn or corrupt reports have no trusted timestamp.
[[nodiscard]] bool crash_report_timestamp(
  std::span<const char> record,
  std::uint64_t file_size,
  std::int64_t& timestamp) noexcept;

} // namespace kwaque::broker::detail
