#ifndef KWAQUE_SRC_RUNTIME_FILE_H_
#define KWAQUE_SRC_RUNTIME_FILE_H_

#include "src/base/allocation.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/runtime/error.h"
#include "src/runtime/shard_affinity.h"

#include <seastar/core/chunked_vector.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/io_intent.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_future.hh>

#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <utility>

namespace kwaque::runtime {

class file_test_access;

inline constexpr std::size_t maximum_file_path_bytes = 4095;
inline constexpr std::size_t maximum_file_name_bytes = 255;
inline constexpr std::size_t maximum_directory_entries = 1U << 16U;
inline constexpr byte_count maximum_directory_name_bytes{16U * 1024U * 1024U};
inline constexpr byte_count maximum_file_io_bytes{64U * 1024U * 1024U};
inline constexpr std::uint32_t maximum_pending_file_reads = 96;
inline constexpr std::uint32_t maximum_pending_file_metadata_operations = 96;
inline constexpr std::uint32_t maximum_queued_file_writes = 96;

struct file_io_limits final {
    // Reads allocate independently, so both their count and requested bytes are
    // bounded before native dispatch. One active write is bounded by
    // maximum_file_io_bytes; these write limits bound only contenders retained
    // behind the native serializer.
    byte_count pending_read_bytes{maximum_file_io_bytes};
    std::uint32_t pending_reads{64};
    std::uint32_t pending_metadata_operations{64};
    byte_count queued_write_bytes{maximum_file_io_bytes};
    std::uint32_t queued_writes{64};

    [[nodiscard]] result<void> validate() const noexcept;

    bool operator==(const file_io_limits&) const = default;
};

struct directory_listing_limits final {
    item_count maximum_entries{4096};
    byte_count maximum_name_bytes{1024U * 1024U};

    [[nodiscard]] result<void> validate() const noexcept;

    bool operator==(const directory_listing_limits&) const = default;
};

class file_path final {
public:
    [[nodiscard]] static result<file_path> make(std::string value) noexcept;

    [[nodiscard]] const std::string& value() const noexcept { return value_; }

    bool operator==(const file_path&) const = default;

private:
    explicit file_path(std::string value) noexcept
      : value_(std::move(value)) {}

    std::string value_;
};

class file_name final {
public:
    [[nodiscard]] static result<file_name> make(std::string value) noexcept;

    [[nodiscard]] const std::string& value() const noexcept { return value_; }

    bool operator==(const file_name&) const = default;

private:
    explicit file_name(std::string value) noexcept
      : value_(std::move(value)) {}

    std::string value_;
};

enum class file_kind : std::uint8_t {
    regular,
    directory,
    other,
};

struct file_status final {
    file_kind kind;
    byte_count size;

    bool operator==(const file_status&) const = default;
};

struct directory_entry final {
    file_name name;
    file_kind kind;

    bool operator==(const directory_entry&) const = default;
};

class directory_listing final {
public:
    [[nodiscard]] static result<directory_listing> make(
      seastar::chunked_vector<directory_entry> entries,
      directory_listing_limits limits) noexcept;

    directory_listing(directory_listing&&) noexcept = default;
    directory_listing& operator=(directory_listing&&) noexcept = default;
    directory_listing(const directory_listing&) = delete;
    directory_listing& operator=(const directory_listing&) = delete;

    [[nodiscard]] const seastar::chunked_vector<directory_entry>&
    entries() const noexcept {
        return entries_;
    }
    [[nodiscard]] seastar::chunked_vector<directory_entry>
    take_entries() && noexcept {
        return std::move(entries_);
    }

private:
    explicit directory_listing(
      seastar::chunked_vector<directory_entry> entries) noexcept
      : entries_(std::move(entries)) {}

    seastar::chunked_vector<directory_entry> entries_;
};

enum class file_access : std::uint8_t {
    read_only,
    write_only,
    read_write,
};

struct file_open_options final {
    file_access access{file_access::read_only};
    bool create{false};
    bool exclusive{false};
    bool truncate{false};
    std::uint16_t permissions{0600U};

    [[nodiscard]] result<void> validate() const noexcept;

    bool operator==(const file_open_options&) const = default;
};

enum class file_state : std::uint8_t {
    open,
    closing,
    closed,
};

class file_position final {
public:
    using rep = std::uint64_t;

    constexpr file_position() noexcept = default;
    constexpr explicit file_position(rep value) noexcept
      : value_(value) {}

    [[nodiscard]] constexpr rep value() const noexcept { return value_; }

    [[nodiscard]] constexpr std::optional<file_position>
    checked_add(byte_count bytes) const noexcept {
        if (bytes.value() > std::numeric_limits<rep>::max() - value_) {
            return std::nullopt;
        }
        return file_position{value_ + bytes.value()};
    }

    auto operator<=>(const file_position&) const = default;

private:
    rep value_{0};
};

class file_read_result final {
public:
    [[nodiscard]] static result<file_read_result> make(
      bytes::fragmented_buffer data,
      bool eof,
      byte_count maximum_bytes) noexcept;

    file_read_result(file_read_result&&) noexcept = default;
    file_read_result& operator=(file_read_result&&) noexcept = default;
    file_read_result(const file_read_result&) = delete;
    file_read_result& operator=(const file_read_result&) = delete;

    [[nodiscard]] const bytes::fragmented_buffer& data() const noexcept {
        return data_;
    }
    [[nodiscard]] bytes::fragmented_buffer take_data() && noexcept {
        return std::move(data_);
    }
    [[nodiscard]] bool eof() const noexcept { return eof_; }

private:
    file_read_result(bytes::fragmented_buffer data, bool eof) noexcept
      : data_(std::move(data))
      , eof_(eof) {}

    bytes::fragmented_buffer data_;
    bool eof_;
};

[[nodiscard]] inline result<void> validate_file_read_request(
  file_position position, byte_count maximum_bytes) noexcept {
    if (maximum_bytes.value() == 0) {
        return failure(
          operation_error{errc::invalid_argument, operation_kind::file});
    }
    if (maximum_bytes > maximum_file_io_bytes) {
        return failure(
          operation_error{errc::out_of_range, operation_kind::file});
    }
    if (!position.checked_add(maximum_bytes)) {
        return failure(
          operation_error{errc::out_of_range, operation_kind::file});
    }
    return {};
}

[[nodiscard]] inline result<void> validate_file_write_request(
  file_position position, const bytes::fragmented_buffer& data) noexcept {
    if (data.empty()) {
        return failure(
          operation_error{errc::invalid_argument, operation_kind::file});
    }
    if (
      data.size() > maximum_file_io_bytes
      || data.retained_bytes() > maximum_file_io_bytes
      || !position.checked_add(data.size())) {
        return failure(
          operation_error{errc::out_of_range, operation_kind::file});
    }
    return {};
}

// Owns one native file on one shard. The type removes seastar::file's copyable
// shared ownership and adds explicit drain/close state, while all file
// operations still dispatch through the native Seastar implementation.
class file final {
public:
    explicit file(seastar::file&& native_file, file_io_limits limits = {});
    file(file&& other) noexcept;
    file& operator=(file&&) = delete;
    file(const file&) = delete;
    file& operator=(const file&) = delete;
    ~file();

    void request_abort();
    // Cancellation is owner-scoped: request_abort() rejects new work and
    // cancels every accepted native read/write request through this file's
    // io_intent. Reads and writes therefore need no second per-operation
    // cancellation mechanism or adapter object.
    [[nodiscard]] seastar::future<result<file_read_result>>
    read(file_position position, byte_count maximum_bytes);
    [[nodiscard]] seastar::future<result<byte_count>>
    write(file_position position, bytes::fragmented_buffer data);
    [[nodiscard]] seastar::future<result<void>> flush();
    [[nodiscard]] seastar::future<result<void>> truncate(std::uint64_t size);
    [[nodiscard]] seastar::future<result<std::uint64_t>> size();
    [[nodiscard]] seastar::future<result<void>> close();

    [[nodiscard]] file_state state() const;
    [[nodiscard]] bool abort_requested() const;
    [[nodiscard]] const file_io_limits& limits() const noexcept {
        owner_.assert_current();
        return limits_;
    }
    [[nodiscard]] std::uint32_t pending_reads() const;
    [[nodiscard]] byte_count pending_read_bytes() const;
    [[nodiscard]] std::uint32_t pending_metadata_operations() const;
    [[nodiscard]] std::uint32_t queued_writes() const;
    [[nodiscard]] byte_count queued_write_bytes() const;
    [[nodiscard]] owner_shard owner() const noexcept { return owner_; }

private:
    friend class file_test_access;

    class writer;
    class admission_reservation final {
    public:
        admission_reservation(
          seastar::semaphore_units<> operation,
          seastar::semaphore_units<> bytes) noexcept
          : operation_(std::move(operation))
          , bytes_(std::move(bytes)) {}

        admission_reservation(admission_reservation&&) noexcept = default;
        admission_reservation&
        operator=(admission_reservation&&) noexcept = default;
        admission_reservation(const admission_reservation&) = delete;
        admission_reservation& operator=(const admission_reservation&) = delete;

    private:
        seastar::semaphore_units<> operation_;
        seastar::semaphore_units<> bytes_;
    };

    [[nodiscard]] static bool move_is_idle(const file& other) noexcept;
    [[nodiscard]] static owner_shard prepare_move(file& other) noexcept;
    [[nodiscard]] std::optional<operation_error> operation_rejection() const;
    [[nodiscard]] std::optional<admission_reservation>
    try_acquire_read(byte_count bytes) noexcept;
    [[nodiscard]] std::optional<seastar::semaphore_units<>>
    try_acquire_metadata() noexcept;
    [[nodiscard]] std::optional<admission_reservation>
    try_acquire_queued_write(byte_count bytes) noexcept;
    [[nodiscard]] seastar::future<result<file_read_result>> read_chunked(
      file_position position,
      byte_count maximum_bytes,
      admission_reservation admission,
      seastar::gate::holder holder);
    [[nodiscard]] seastar::future<result<void>> close_once();

    owner_shard owner_;
    file_io_limits limits_;
    seastar::file native_file_;
    seastar::io_intent io_intent_;
    seastar::gate operations_;
    seastar::semaphore read_operation_units_;
    seastar::semaphore read_byte_units_;
    seastar::semaphore metadata_operation_units_;
    seastar::semaphore queued_write_operation_units_;
    seastar::semaphore queued_write_byte_units_;
    seastar::semaphore write_serialization_{1};
    std::uint64_t memory_dma_alignment_;
    std::uint64_t disk_read_dma_alignment_;
    std::uint64_t disk_write_dma_alignment_;
    std::uint64_t disk_overwrite_dma_alignment_;
    std::uint64_t native_write_max_length_;
    std::uint64_t append_chunk_limit_;
    std::optional<seastar::shared_promise<result<void>>> close_done_;
    file_state state_{file_state::open};
    bool abort_requested_{false};
    bool moved_from_{false};
};

template<typename FileSystem>
concept file_system_backend = requires(
  FileSystem& file_system,
  file_path path,
  file_path destination,
  file_open_options options,
  directory_listing_limits limits) {
    {
        file_system.open(std::move(path), options)
    } -> std::same_as<seastar::future<result<file>>>;
    {
        file_system.exists(std::move(path))
    } -> std::same_as<seastar::future<result<bool>>>;
    {
        file_system.stat(std::move(path))
    } -> std::same_as<seastar::future<result<file_status>>>;
    {
        file_system.list(std::move(path), limits)
    } -> std::same_as<seastar::future<result<directory_listing>>>;
    {
        file_system.create_directories(std::move(path))
    } -> std::same_as<seastar::future<result<void>>>;
    {
        file_system.remove_file(std::move(path))
    } -> std::same_as<seastar::future<result<void>>>;
    {
        file_system.remove_directory(std::move(path))
    } -> std::same_as<seastar::future<result<void>>>;
    {
        file_system.rename(std::move(path), std::move(destination))
    } -> std::same_as<seastar::future<result<void>>>;
    {
        file_system.sync_directory(std::move(path))
    } -> std::same_as<seastar::future<result<void>>>;
};

} // namespace kwaque::runtime

#endif // KWAQUE_SRC_RUNTIME_FILE_H_
