#ifndef KWAQUE_SRC_RUNTIME_FILE_H_
#define KWAQUE_SRC_RUNTIME_FILE_H_

#include "src/base/allocation.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/runtime/error.h"
#include "src/runtime/file_error.h"
#include "src/runtime/file_position.h"
#include "src/runtime/first_failure.h"
#include "src/runtime/operation_statistics.h"
#include "src/runtime/shard_affinity.h"

#include <seastar/core/chunked_vector.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/io_intent.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>

#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace kwaque::simulation {
class fake_directory_cursor;
class fake_file_system;
} // namespace kwaque::simulation

namespace kwaque::runtime {

namespace production {
class directory_cursor;
}
namespace detail {
struct directory_cursor_memory;
}

class file_test_access;

inline constexpr std::size_t maximum_file_path_bytes = 4095;
inline constexpr std::size_t maximum_file_name_bytes = 255;
inline constexpr std::size_t maximum_directory_entries = 1U << 16U;
inline constexpr byte_count maximum_directory_name_bytes{16U * 1024U * 1024U};
inline constexpr std::size_t maximum_directory_page_entries = 1024;
inline constexpr byte_count maximum_directory_page_name_bytes{256U * 1024U};
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

// An owning snapshot of an open file's DMA requirements. Native maximum
// lengths are recommendations; the runtime operation/allocation caps are hard.
// A snapshot carries no handle, lifetime lease or permission to perform I/O.
class file_geometry final {
public:
    [[nodiscard]] byte_count memory_alignment() const noexcept {
        return memory_;
    }
    [[nodiscard]] byte_count read_alignment() const noexcept { return read_; }
    [[nodiscard]] byte_count write_alignment() const noexcept { return write_; }
    [[nodiscard]] byte_count overwrite_alignment() const noexcept {
        return overwrite_;
    }
    [[nodiscard]] byte_count native_read_max_length() const noexcept {
        return read_max_;
    }
    [[nodiscard]] byte_count native_write_max_length() const noexcept {
        return write_max_;
    }
    [[nodiscard]] byte_count append_chunk_bytes() const noexcept {
        return append_chunk_;
    }
    [[nodiscard]] byte_count read_operation_limit() const noexcept {
        return read_operation_limit_;
    }
    [[nodiscard]] static constexpr byte_count allocation_limit() noexcept {
        return byte_count{maximum_contiguous_allocation_bytes};
    }
    [[nodiscard]] static constexpr byte_count operation_limit() noexcept {
        return maximum_file_io_bytes;
    }
    // The format owner separately checks its supported alignment range.
    [[nodiscard]] bool
    supports_disk_alignment(byte_count alignment) const noexcept;
    bool operator==(const file_geometry&) const = default;

private:
    friend class file;
    file_geometry(
      byte_count memory,
      byte_count read,
      byte_count write,
      byte_count overwrite,
      byte_count read_max,
      byte_count write_max,
      byte_count append_chunk,
      byte_count read_operation_limit) noexcept
      : memory_(memory)
      , read_(read)
      , write_(write)
      , overwrite_(overwrite)
      , read_max_(read_max)
      , write_max_(write_max)
      , append_chunk_(append_chunk)
      , read_operation_limit_(read_operation_limit) {}
    byte_count memory_, read_, write_, overwrite_, read_max_, write_max_,
      append_chunk_, read_operation_limit_;
};

// Advisory filesystem-wide sample; neither a reservation nor write permission.
class file_system_space final {
public:
    [[nodiscard]] static result<file_system_space> make(
      byte_count capacity,
      byte_count free,
      byte_count available,
      bool read_only) noexcept;
    [[nodiscard]] static result<file_system_space> from_blocks(
      std::uint64_t fragment_bytes,
      std::uint64_t blocks,
      std::uint64_t free_blocks,
      std::uint64_t available_blocks,
      bool read_only) noexcept;
    [[nodiscard]] byte_count capacity() const noexcept { return capacity_; }
    [[nodiscard]] byte_count free() const noexcept { return free_; }
    [[nodiscard]] byte_count available() const noexcept { return available_; }
    [[nodiscard]] bool read_only() const noexcept { return read_only_; }
    bool operator==(const file_system_space&) const = default;

private:
    file_system_space(
      byte_count capacity,
      byte_count free,
      byte_count available,
      bool read_only) noexcept
      : capacity_(capacity)
      , free_(free)
      , available_(available)
      , read_only_(read_only) {}
    byte_count capacity_, free_, available_;
    bool read_only_;
};

struct directory_listing_limits final {
    item_count maximum_entries{4096};
    byte_count maximum_name_bytes{1024U * 1024U};

    [[nodiscard]] result<void> validate() const noexcept;

    bool operator==(const directory_listing_limits&) const = default;
};

// One outstanding page per cursor. Returned pages retain admission until they
// are destroyed, including after cursor close. Exhaustion is explicit: a full
// page need not be the last page. Entries have no ordering/snapshot guarantee.
// An empty page without end may have advanced through deleted entries.
struct directory_page_limits final {
    item_count maximum_entries{128};
    byte_count maximum_name_bytes{32U * 1024U};

    [[nodiscard]] result<void> validate() const noexcept;
    bool operator==(const directory_page_limits&) const = default;
};

class file_path final {
public:
    [[nodiscard]] static result<file_path> make(std::string_view value);

    [[nodiscard]] const std::string& value() const noexcept { return value_; }

    bool operator==(const file_path&) const = default;

private:
    explicit file_path(std::string value) noexcept
      : value_(std::move(value)) {}

    std::string value_;
};

class file_name final {
public:
    [[nodiscard]] static result<file_name> make(std::string_view value);

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
    // Validate before allocating fresh descriptor storage. Incoming spare
    // capacity is not retained; allocation failures remain exceptional.
    [[nodiscard]] static result<directory_listing> make(
      seastar::chunked_vector<directory_entry> entries,
      directory_listing_limits limits);

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

class directory_page final {
public:
    directory_page(directory_page&&) noexcept;
    directory_page& operator=(directory_page&&) noexcept;
    directory_page(const directory_page&) = delete;
    directory_page& operator=(const directory_page&) = delete;
    ~directory_page();

    [[nodiscard]] const seastar::chunked_vector<directory_entry>&
    entries() const noexcept {
        return listing_.entries();
    }
    [[nodiscard]] bool end() const noexcept { return end_; }

private:
    friend class production::directory_cursor;
    friend class kwaque::simulation::fake_directory_cursor;
    friend class kwaque::simulation::fake_file_system;
    directory_page(
      directory_listing listing,
      bool end,
      seastar::lw_shared_ptr<detail::directory_cursor_memory> memory,
      seastar::semaphore_units<> reservation) noexcept;

    // Destroy page storage and its reservation before the semaphore owner.
    seastar::lw_shared_ptr<detail::directory_cursor_memory> memory_;
    seastar::semaphore_units<> reservation_;
    directory_listing listing_;
    bool end_;
};

template<typename Cursor>
concept directory_cursor_contract
  = std::movable<Cursor> && !std::copy_constructible<Cursor>
    && requires(Cursor& cursor, directory_page_limits limits) {
           {
               cursor.next(limits)
           } -> std::same_as<seastar::future<result<directory_page>>>;
           { cursor.sync() } -> std::same_as<seastar::future<result<void>>>;
           { cursor.close() } -> std::same_as<seastar::future<result<void>>>;
       };

enum class file_rename_policy : std::uint8_t { replace, no_replace };

enum class file_access : std::uint8_t {
    read_only,
    write_only,
    read_write,
};

enum class file_close_policy : std::uint8_t { legacy, checked };

struct file_open_options final {
    file_access access{file_access::read_only};
    bool create{false};
    bool exclusive{false};
    bool truncate{false};
    std::uint16_t permissions{0600U};
    file_close_policy close_policy{file_close_policy::legacy};

    [[nodiscard]] result<void> validate() const noexcept;

    bool operator==(const file_open_options&) const = default;
};

enum class file_state : std::uint8_t {
    open,
    closing,
    closed,
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
// A qualified write/flush/truncate failure fences further mutations. Reads can
// still inspect the file, and default close still releases its native handle.
class file final {
public:
    // Holds a real metadata-admission unit for a later barrier. The file must
    // outlive the reservation; release it before closing or moving the file.
    // A reservation admits at most one flush at a time and cannot be moved or
    // released until that flush completes.
    class metadata_reservation final {
    public:
        metadata_reservation(metadata_reservation&&) noexcept;
        metadata_reservation& operator=(metadata_reservation&&) noexcept;
        metadata_reservation(const metadata_reservation&) = delete;
        metadata_reservation& operator=(const metadata_reservation&) = delete;
        ~metadata_reservation();
        void release() noexcept;

    private:
        friend class file;
        metadata_reservation(file&, seastar::semaphore_units<>) noexcept;
        file* owner_;
        seastar::semaphore_units<> units_;
        bool active_{false};
    };

    explicit file(
      seastar::file&& native_file,
      file_io_limits limits = {},
      operation_statistics_owner statistics = {},
      file_close_policy close_policy = file_close_policy::legacy);
    file(file&& other) noexcept;
    file& operator=(file&&) = delete;
    file(const file&) = delete;
    file& operator=(const file&) = delete;
    ~file();

    void request_abort();
    [[nodiscard]] result<metadata_reservation> try_reserve_metadata();
    [[nodiscard]] seastar::future<result<void>>
    flush(metadata_reservation& reservation);
    // Cancellation is owner-scoped: request_abort() rejects new work and
    // cancels every accepted native read/write request through this file's
    // io_intent. Reads and writes therefore need no second per-operation
    // cancellation mechanism or adapter object.
    [[nodiscard, gnu::always_inline]] seastar::future<result<file_read_result>>
    read(file_position position, byte_count maximum_bytes) {
        owner_.assert_current();
        if (
          auto valid = validate_file_read_request(position, maximum_bytes);
          !valid) {
            statistics_->reject();
            result<file_read_result> outcome = failure(valid.error());
            return seastar::make_ready_future<result<file_read_result>>(
              std::move(outcome));
        }
        if (auto rejected = operation_rejection()) {
            statistics_->reject();
            result<file_read_result> outcome = failure(std::move(*rejected));
            return seastar::make_ready_future<result<file_read_result>>(
              std::move(outcome));
        }
        if (maximum_bytes > limits_.pending_read_bytes) {
            statistics_->reject();
            result<file_read_result> outcome = failure(
              operation_error{errc::out_of_range, operation_kind::file});
            return seastar::make_ready_future<result<file_read_result>>(
              std::move(outcome));
        }
        return read_validated(position, maximum_bytes);
    }
    [[nodiscard, gnu::always_inline]] seastar::future<result<byte_count>>
    write(file_position position, bytes::fragmented_buffer data) {
        owner_.assert_current();
        if (auto valid = validate_file_write_request(position, data); !valid) {
            statistics_->reject();
            result<byte_count> outcome = failure(valid.error());
            return seastar::make_ready_future<result<byte_count>>(
              std::move(outcome));
        }
        if (auto exception = first_failure_.exception()) {
            statistics_->reject();
            if (auto rejected = operation_rejection())
                return seastar::make_ready_future<result<byte_count>>(
                  failure(std::move(*rejected)));
            return seastar::make_exception_future<result<byte_count>>(
              exception);
        }
        if (auto rejected = mutation_rejection()) {
            statistics_->reject();
            result<byte_count> outcome = failure(std::move(*rejected));
            return seastar::make_ready_future<result<byte_count>>(
              std::move(outcome));
        }
        return write_validated(position, std::move(data));
    }
    [[nodiscard]] seastar::future<result<void>> flush();
    [[nodiscard]] seastar::future<result<void>> truncate(std::uint64_t size);
    [[nodiscard]] seastar::future<result<std::uint64_t>> size();
    // Checked close drains accepted work without initiating abort. There is one
    // closing caller; concurrent checked close rejects with queue_full. After
    // completion, calls return the cached first outcome without another I/O.
    [[nodiscard]] seastar::future<result<void>> close();

    [[nodiscard]] file_state state() const;
    [[nodiscard]] result<file_geometry> geometry() const noexcept;
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
    [[nodiscard]] operation_statistics_snapshot statistics() const noexcept {
        owner_.assert_current();
        return statistics_->snapshot();
    }

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
    [[nodiscard]] std::optional<operation_error> operation_rejection() const {
        if (moved_from_ || state_ != file_state::open) {
            return operation_error{errc::closed, operation_kind::file};
        }
        if (abort_requested_) {
            return operation_error{errc::aborted, operation_kind::file};
        }
        return std::nullopt;
    }
    [[nodiscard]] std::optional<operation_error>
    mutation_rejection(bool admitted = false) const {
        if (!(admitted && close_policy_ == file_close_policy::checked
              && state_ == file_state::closing && !abort_requested_)) {
            if (auto rejected = operation_rejection()) return rejected;
        }
        if (first_failure_.exception())
            std::rethrow_exception(first_failure_.exception());
        return first_failure_.error();
    }
    [[nodiscard]] operation_error remember_error(operation_error error);
    [[nodiscard]] operation_error
    remember_io_failure(std::exception_ptr exception);
    [[nodiscard]] std::optional<admission_reservation>
    try_acquire_read(byte_count bytes) noexcept;
    [[nodiscard]] std::optional<seastar::semaphore_units<>>
    try_acquire_metadata() noexcept;
    [[nodiscard]] std::optional<admission_reservation>
    try_acquire_queued_write(byte_count bytes) noexcept;
    [[nodiscard]] seastar::future<result<file_read_result>>
    read_validated(file_position position, byte_count maximum_bytes);
    // These helpers do not suspend; pending writes must retain their payload
    // in an owning continuation or writer before returning.
    [[nodiscard]] seastar::future<result<byte_count>>
    write_validated(file_position position, bytes::fragmented_buffer&& data);
    [[nodiscard]] seastar::future<result<byte_count>> write_general(
      file_position position,
      bytes::fragmented_buffer&& data,
      std::optional<seastar::semaphore_units<>> serialization);
    [[nodiscard]] seastar::future<result<file_read_result>> read_chunked(
      file_position position,
      byte_count maximum_bytes,
      admission_reservation admission,
      seastar::gate::holder holder,
      operation_statistics::reservation metric);
    [[nodiscard]] seastar::future<result<void>> close_once();
    [[nodiscard]] seastar::future<result<void>> close_checked_once();

    owner_shard owner_;
    operation_statistics_owner statistics_owner_;
    operation_statistics* statistics_;
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
    first_failure first_failure_;
    file_close_policy close_policy_;
    file_state state_{file_state::open};
    bool abort_requested_{false};
    bool moved_from_{false};
};

// Preallocation is intentionally absent. A future optional hint must exclude
// existing/reserved/in-flight ranges and reconcile EOF separately. A successful
// hint cannot imply reserved capacity, preserved overlap or durable contents.
// remove_file unlinks a non-directory entry, including a symlink.
// remove_directory requires an empty directory. A kind mismatch rejects
// without removing the target; durability still requires directory sync.
template<typename FileSystem>
concept file_system_backend
  = requires { typename FileSystem::directory_cursor_type; }
    && directory_cursor_contract<typename FileSystem::directory_cursor_type>
    && requires(
      FileSystem& file_system,
      file_path path,
      file_path destination,
      file_open_options options,
      directory_listing_limits limits) {
           {
               file_system.open_directory(
                 std::move(path), file_close_policy::checked)
           } -> std::same_as<seastar::future<
             result<typename FileSystem::directory_cursor_type>>>;
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
               file_system.space(std::move(path))
           } -> std::same_as<seastar::future<result<file_system_space>>>;
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
               file_system.rename(
                 std::move(path),
                 std::move(destination),
                 file_rename_policy::no_replace)
           } -> std::same_as<seastar::future<result<void>>>;
           {
               file_system.sync_directory(
                 std::move(path), file_close_policy::checked)
           } -> std::same_as<seastar::future<result<void>>>;
       };

} // namespace kwaque::runtime

#endif // KWAQUE_SRC_RUNTIME_FILE_H_
