#include "src/runtime/file.h"

#include "src/base/invariant.h"
#include "src/runtime/fragmented_buffer_internal.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/temporary_buffer.hh>

#include <algorithm>
#include <bit>
#include <cstdint>
#include <cstring>
#include <limits>
#include <new>
#include <span>
#include <stdexcept>
#include <system_error>
#include <utility>

namespace kwaque::runtime {

namespace {

constexpr invariant_id file_move_invariant{"KQ-FILE-MOVE-IDLE"};
constexpr invariant_id file_stopped_invariant{"KQ-FILE-CLOSED"};
constexpr invariant_id file_gate_invariant{"KQ-FILE-GATE-OPEN"};
constexpr invariant_id file_alignment_invariant{"KQ-FILE-DMA-ALIGNMENT"};
constexpr invariant_id file_consumption_invariant{"KQ-FILE-WRITE-CONSUMED"};

operation_error file_error(errc code) noexcept {
    return operation_error{code, operation_kind::file};
}

bool contains_nul(const std::string& value) noexcept {
    return std::find(value.begin(), value.end(), '\0') != value.end();
}

file_io_limits validated_file_io_limits(file_io_limits limits) {
    if (!limits.validate()) {
        throw std::invalid_argument("invalid file I/O limits");
    }
    return limits;
}

errc map_system_error(const std::error_code& error) noexcept {
    if (error == std::errc::no_such_file_or_directory) {
        return errc::not_found;
    }
    if (error == std::errc::file_exists) {
        return errc::already_exists;
    }
    if (
      error == std::errc::permission_denied
      || error == std::errc::operation_not_permitted
      || error == std::errc::read_only_file_system) {
        return errc::permission_denied;
    }
    if (error == std::errc::directory_not_empty) {
        return errc::directory_not_empty;
    }
    if (error == std::errc::operation_canceled) {
        return errc::aborted;
    }
    if (error == std::errc::timed_out) {
        return errc::timed_out;
    }
    if (
      error == std::errc::no_space_on_device
      || error == std::errc::too_many_files_open
      || error == std::errc::too_many_files_open_in_system) {
        return errc::resource_exhausted;
    }
    if (error == std::errc::file_too_large) {
        return errc::out_of_range;
    }
    if (error == std::errc::invalid_argument) {
        return errc::invalid_argument;
    }
    if (error == std::errc::is_a_directory) {
        return errc::is_a_directory;
    }
    if (error == std::errc::not_a_directory) {
        return errc::not_a_directory;
    }
    return errc::io_failure;
}

operation_error file_error_from_exception(std::exception_ptr exception) {
    try {
        std::rethrow_exception(std::move(exception));
    } catch (const std::bad_alloc&) {
        throw;
    } catch (const seastar::cancelled_error&) {
        return file_error(errc::aborted);
    } catch (const std::system_error& error) {
        return file_error(map_system_error(error.code()));
    } catch (...) {
        return file_error(errc::io_failure);
    }
}

seastar::file require_open_file(seastar::file&& native_file) {
    if (!native_file) {
        throw std::invalid_argument("file requires an open native handle");
    }
    return std::move(native_file);
}

[[nodiscard]] constexpr std::uint64_t
round_down(std::uint64_t value, std::uint64_t alignment) noexcept {
    return value & ~(alignment - 1U);
}

[[nodiscard]] constexpr std::uint64_t
round_up(std::uint64_t value, std::uint64_t alignment) noexcept {
    return round_down(value + alignment - 1U, alignment);
}

[[nodiscard]] constexpr bool
is_aligned(std::uint64_t value, std::uint64_t alignment) noexcept {
    return (value & (alignment - 1U)) == 0;
}

[[nodiscard]] bool
is_aligned(const void* address, std::uint64_t alignment) noexcept {
    return is_aligned(
      static_cast<std::uint64_t>(reinterpret_cast<std::uintptr_t>(address)),
      alignment);
}

struct native_bulk_read_request final {
    std::size_t bytes;
    byte_count retained_bytes;
};

[[nodiscard]] native_bulk_read_request bounded_native_bulk_read(
  std::uint64_t position,
  std::uint64_t remaining,
  std::uint64_t read_alignment) noexcept {
    const auto front = position & (read_alignment - 1U);
    const auto maximum_payload = maximum_contiguous_allocation_bytes - front;
    const auto requested = std::min(remaining, maximum_payload);
    const auto retained = round_up(requested + front, read_alignment);
    return native_bulk_read_request{
      .bytes = static_cast<std::size_t>(requested),
      .retained_bytes = byte_count{retained},
    };
}

} // namespace

class file::writer final {
public:
    writer(
      file& owner,
      file_position position,
      bytes::fragmented_buffer data,
      seastar::gate::holder holder,
      operation_statistics::reservation metric)
      : owner_(owner)
      , data_(std::move(data))
      , initial_position_(position.value())
      , total_bytes_(data_.size().value())
      , holder_(std::move(holder))
      , metric_(std::move(metric)) {}

    writer(writer&&) noexcept = default;
    writer& operator=(writer&&) = delete;
    writer(const writer&) = delete;
    writer& operator=(const writer&) = delete;

    [[nodiscard]] static seastar::future<result<byte_count>> run(
      writer self,
      std::optional<seastar::semaphore_units<>> serialization,
      std::optional<admission_reservation> queued) {
        static_assert(
          std::numeric_limits<std::size_t>::digits
          >= std::numeric_limits<std::uint64_t>::digits);

        try {
            if (!serialization) {
                serialization.emplace(
                  co_await seastar::coroutine::without_preemption_check(
                    seastar::get_units(self.owner_.write_serialization_, 1)));
            }
            queued.reset();
            static_cast<void>(*serialization);
            static_cast<void>(self.holder_);
            if (auto rejected = self.owner_.operation_rejection()) {
                co_return failure(std::move(*rejected));
            }

            const auto logical_end = self.initial_position_ + self.total_bytes_;

            const auto memory_alignment = self.owner_.memory_dma_alignment_;
            const auto read_alignment = self.owner_.disk_read_dma_alignment_;
            const auto append_alignment = self.owner_.disk_write_dma_alignment_;
            const auto overwrite_alignment
              = self.owner_.disk_overwrite_dma_alignment_;

            self.memory_alignment_ = memory_alignment;
            self.write_alignment_ = append_alignment;
            if (
              !is_aligned(self.initial_position_, append_alignment)
              || !is_aligned(self.total_bytes_, append_alignment)) {
                self.original_size_ = co_await self.owner_.native_file_.size();
                self.size_known_ = true;
                self.final_size_ = std::max(self.original_size_, logical_end);
                self.write_alignment_ = logical_end <= self.original_size_
                                          ? overwrite_alignment
                                          : append_alignment;
            }
            self.rmw_alignment_ = std::max(
              {self.memory_alignment_, read_alignment, self.write_alignment_});
            if (
              (!is_aligned(self.initial_position_, self.write_alignment_)
               || !is_aligned(self.total_bytes_, self.write_alignment_))) {
                const auto final_block = round_down(
                  logical_end - 1U, self.rmw_alignment_);
                if (
                  self.rmw_alignment_
                  > std::numeric_limits<std::uint64_t>::max() - final_block) {
                    co_return failure(file_error(errc::out_of_range));
                }
            }

            auto source = detail::fragmented_buffer_io_access::consume(
              self.data_);

            const auto recommended = self.owner_.native_write_max_length_;
            self.native_chunk_bytes_ = round_down(
              recommended, self.write_alignment_);
            if (self.native_chunk_bytes_ == 0) {
                self.native_chunk_bytes_ = self.write_alignment_;
            }

            std::uint64_t position = self.initial_position_;
            std::uint64_t remaining = self.total_bytes_;
            while (remaining != 0) {
                if (
                  !is_aligned(position, self.write_alignment_)
                  || remaining < self.write_alignment_) {
                    const auto written = co_await self.write_partial_block(
                      source, position, remaining);
                    position += written;
                    remaining -= written;
                    continue;
                }

                const auto transferable = round_down(
                  std::min(remaining, self.native_chunk_bytes_),
                  self.write_alignment_);
                const auto front = source.front();
                const auto direct_bytes = round_down(
                  std::min<std::uint64_t>(front.size(), transferable),
                  self.write_alignment_);
                if (
                  direct_bytes != 0
                  && is_aligned(front.data(), self.memory_alignment_)) {
                    auto fragment = source.take_front(
                      static_cast<std::size_t>(direct_bytes));
                    const auto written
                      = co_await self.owner_.native_file_.dma_write(
                        position,
                        fragment.get(),
                        fragment.size(),
                        &self.owner_.io_intent_);
                    if (written == 0 || written > fragment.size()) {
                        throw std::system_error(
                          std::make_error_code(std::errc::io_error));
                    }
                    if (written != fragment.size()) {
                        co_await recover_short_write(
                          self.owner_,
                          position,
                          fragment.get(),
                          fragment.size(),
                          written,
                          self.write_alignment_,
                          self.memory_alignment_);
                    }
                    position += direct_bytes;
                    remaining -= direct_bytes;
                    self.physical_end_ = std::max(self.physical_end_, position);
                    continue;
                }

                const auto allocation = round_up(
                  transferable, self.memory_alignment_);
                auto staging = seastar::temporary_buffer<char>::aligned(
                  static_cast<std::size_t>(self.memory_alignment_),
                  static_cast<std::size_t>(allocation));
                const auto staged = source.copy_front_to(
                  std::span<char>{
                    staging.get_write(),
                    static_cast<std::size_t>(transferable)});
                KWAQUE_INVARIANT(
                  file_consumption_invariant,
                  staged == transferable,
                  "fragmented write ended before its declared size");
                const auto staged_size = static_cast<std::size_t>(transferable);
                const auto written
                  = co_await self.owner_.native_file_.dma_write(
                    position,
                    staging.get(),
                    staged_size,
                    &self.owner_.io_intent_);
                if (written == 0 || written > staged_size) {
                    throw std::system_error(
                      std::make_error_code(std::errc::io_error));
                }
                if (written != staged_size) {
                    co_await recover_short_write(
                      self.owner_,
                      position,
                      staging.get(),
                      staged_size,
                      written,
                      self.write_alignment_,
                      self.memory_alignment_);
                }
                position += transferable;
                remaining -= transferable;
                self.physical_end_ = std::max(self.physical_end_, position);
            }

            KWAQUE_INVARIANT(
              file_consumption_invariant,
              self.data_.empty(),
              "fragmented write retained bytes after completion");
            if (self.size_known_ && self.physical_end_ > self.final_size_) {
                co_await self.owner_.native_file_.truncate(self.final_size_);
            }
            self.metric_.add_completed_bytes(self.total_bytes_);
            co_return byte_count{self.total_bytes_};
        } catch (const std::bad_alloc&) {
            throw;
        } catch (...) {
            co_return failure(
              file_error_from_exception(std::current_exception()));
        }
    }

    [[nodiscard]] static seastar::future<result<byte_count>> finish_direct(
      file& owner,
      std::uint64_t position,
      bytes::fragmented_buffer data,
      seastar::temporary_buffer<char> fragment,
      seastar::semaphore_units<> serialization,
      std::size_t completed,
      std::uint64_t write_alignment,
      std::uint64_t memory_alignment,
      operation_statistics::reservation metric) {
        static_cast<void>(data);
        static_cast<void>(serialization);
        const auto size = fragment.size();
        try {
            co_await recover_short_write(
              owner,
              position,
              fragment.get(),
              size,
              completed,
              write_alignment,
              memory_alignment);
            metric.add_completed_bytes(static_cast<std::uint64_t>(size));
            co_return byte_count{static_cast<std::uint64_t>(size)};
        } catch (const std::bad_alloc&) {
            throw;
        } catch (...) {
            co_return failure(
              file_error_from_exception(std::current_exception()));
        }
    }

private:
    [[nodiscard]] static seastar::future<> recover_short_write(
      file& owner,
      std::uint64_t position,
      const char* data,
      std::size_t size,
      std::size_t completed,
      std::uint64_t write_alignment,
      std::uint64_t memory_alignment) {
        if (completed > size) {
            throw std::system_error(std::make_error_code(std::errc::io_error));
        }
        while (completed < size) {
            const auto current_position
              = position + static_cast<std::uint64_t>(completed);
            const auto remaining = size - completed;
            if (
              !is_aligned(current_position, write_alignment)
              || !is_aligned(
                static_cast<std::uint64_t>(remaining), write_alignment)) {
                throw std::system_error(
                  std::make_error_code(std::errc::io_error));
            }

            const char* submitted = data + completed;
            std::optional<seastar::temporary_buffer<char>> realigned;
            if (!is_aligned(submitted, memory_alignment)) {
                const auto allocation = round_up(
                  static_cast<std::uint64_t>(remaining), memory_alignment);
                realigned.emplace(
                  seastar::temporary_buffer<char>::aligned(
                    static_cast<std::size_t>(memory_alignment),
                    static_cast<std::size_t>(allocation)));
                std::memcpy(realigned->get_write(), submitted, remaining);
                submitted = realigned->get();
            }

            const auto written = co_await owner.native_file_.dma_write(
              current_position, submitted, remaining, &owner.io_intent_);
            if (written == 0 || written > remaining) {
                throw std::system_error(
                  std::make_error_code(std::errc::io_error));
            }
            completed += written;
            if (
              completed < size
              && !is_aligned(
                static_cast<std::uint64_t>(completed), write_alignment)) {
                throw std::system_error(
                  std::make_error_code(std::errc::io_error));
            }
        }
    }

    [[nodiscard]] seastar::future<std::uint64_t> write_partial_block(
      detail::fragmented_buffer_io_access::consumer& source,
      std::uint64_t position,
      std::uint64_t remaining) {
        const auto block_start = round_down(position, rmw_alignment_);
        const auto offset = position - block_start;
        const auto count = std::min(remaining, rmw_alignment_ - offset);
        auto block = seastar::temporary_buffer<char>::aligned(
          static_cast<std::size_t>(memory_alignment_),
          static_cast<std::size_t>(rmw_alignment_));
        std::memset(block.get_write(), 0, block.size());

        if (block_start < original_size_) {
            const auto existing = std::min(
              rmw_alignment_, original_size_ - block_start);
            const auto read = co_await owner_.native_file_.dma_read(
              block_start, block.get_write(), block.size(), &owner_.io_intent_);
            if (read < existing || read > block.size()) {
                throw std::system_error(
                  std::make_error_code(std::errc::io_error));
            }
        }

        const auto copied = source.copy_front_to(
          std::span<char>{
            block.get_write() + static_cast<std::size_t>(offset),
            static_cast<std::size_t>(count)});
        KWAQUE_INVARIANT(
          file_consumption_invariant,
          copied == count,
          "fragmented write ended during read-modify-write");
        const auto written = co_await owner_.native_file_.dma_write(
          block_start, block.get(), block.size(), &owner_.io_intent_);
        if (written == 0 || written > block.size()) {
            throw std::system_error(std::make_error_code(std::errc::io_error));
        }
        if (written != block.size()) {
            co_await recover_short_write(
              owner_,
              block_start,
              block.get(),
              block.size(),
              written,
              write_alignment_,
              memory_alignment_);
        }
        physical_end_ = std::max(physical_end_, block_start + rmw_alignment_);
        co_return count;
    }

    file& owner_;
    bytes::fragmented_buffer data_;
    std::uint64_t initial_position_;
    std::uint64_t total_bytes_;
    seastar::gate::holder holder_;
    operation_statistics::reservation metric_;
    std::uint64_t original_size_{0};
    std::uint64_t final_size_{0};
    std::uint64_t physical_end_{0};
    std::uint64_t memory_alignment_{0};
    std::uint64_t write_alignment_{0};
    std::uint64_t rmw_alignment_{0};
    std::uint64_t native_chunk_bytes_{0};
    bool size_known_{false};
};

result<file_path> file_path::make(std::string value) noexcept {
    if (value.empty() || contains_nul(value)) {
        return failure(file_error(errc::invalid_argument));
    }
    if (value.size() > maximum_file_path_bytes) {
        return failure(file_error(errc::out_of_range));
    }
    return file_path{std::move(value)};
}

result<file_name> file_name::make(std::string value) noexcept {
    if (
      value.empty() || value == "." || value == ".."
      || value.find('/') != std::string::npos || contains_nul(value)) {
        return failure(file_error(errc::invalid_argument));
    }
    if (value.size() > maximum_file_name_bytes) {
        return failure(file_error(errc::out_of_range));
    }
    return file_name{std::move(value)};
}

result<void> directory_listing_limits::validate() const noexcept {
    if (maximum_entries.value() == 0 || maximum_name_bytes.value() == 0) {
        return failure(file_error(errc::invalid_argument));
    }
    if (
      maximum_entries.value() > maximum_directory_entries
      || maximum_name_bytes > kwaque::runtime::maximum_directory_name_bytes) {
        return failure(file_error(errc::out_of_range));
    }
    return {};
}

result<directory_listing> directory_listing::make(
  seastar::chunked_vector<directory_entry> entries,
  directory_listing_limits limits) noexcept {
    if (auto valid = limits.validate(); !valid) {
        return failure(valid.error());
    }
    if (entries.size() > limits.maximum_entries.value()) {
        return failure(file_error(errc::resource_exhausted));
    }
    std::uint64_t name_bytes = 0;
    for (const auto& entry : entries) {
        const auto size = static_cast<std::uint64_t>(entry.name.value().size());
        if (size > limits.maximum_name_bytes.value() - name_bytes) {
            return failure(file_error(errc::resource_exhausted));
        }
        name_bytes += size;
    }
    return directory_listing{std::move(entries)};
}

result<void> file_open_options::validate() const noexcept {
    const auto access_value = static_cast<std::uint8_t>(access);
    if (
      access_value > static_cast<std::uint8_t>(file_access::read_write)
      || (exclusive && !create)
      || (truncate && access == file_access::read_only)
      || permissions > 0777U) {
        return failure(file_error(errc::invalid_argument));
    }
    return {};
}

result<file_read_result> file_read_result::make(
  bytes::fragmented_buffer data, bool eof, byte_count maximum_bytes) noexcept {
    if (maximum_bytes.value() == 0) {
        return failure(file_error(errc::invalid_argument));
    }
    if (maximum_bytes > maximum_file_io_bytes) {
        return failure(file_error(errc::out_of_range));
    }
    if (data.size() > maximum_bytes) {
        return failure(file_error(errc::out_of_range));
    }
    if (data.size() < maximum_bytes && !eof) {
        return failure(file_error(errc::invalid_argument));
    }
    return file_read_result{std::move(data), eof};
}

result<void> file_io_limits::validate() const noexcept {
    if (
      pending_read_bytes.value() == 0 || pending_reads == 0
      || pending_metadata_operations == 0 || queued_write_bytes.value() == 0
      || queued_writes == 0) {
        return failure(file_error(errc::invalid_argument));
    }
    if (
      pending_read_bytes > maximum_file_io_bytes
      || queued_write_bytes > maximum_file_io_bytes
      || pending_reads > maximum_pending_file_reads
      || pending_metadata_operations > maximum_pending_file_metadata_operations
      || queued_writes > maximum_queued_file_writes) {
        return failure(file_error(errc::out_of_range));
    }
    return {};
}

file::file(
  seastar::file&& native_file,
  file_io_limits limits,
  operation_statistics_owner statistics)
  : statistics_owner_(std::move(statistics))
  , statistics_(&statistics_owner_.get())
  , limits_(validated_file_io_limits(limits))
  , native_file_(require_open_file(std::move(native_file)))
  , read_operation_units_(limits_.pending_reads)
  , read_byte_units_(limits_.pending_read_bytes.value())
  , metadata_operation_units_(limits_.pending_metadata_operations)
  , queued_write_operation_units_(limits_.queued_writes)
  , queued_write_byte_units_(limits_.queued_write_bytes.value())
  , memory_dma_alignment_(
      std::max<std::uint64_t>(
        native_file_.memory_dma_alignment(), sizeof(void*)))
  , disk_read_dma_alignment_(native_file_.disk_read_dma_alignment())
  , disk_write_dma_alignment_(native_file_.disk_write_dma_alignment())
  , disk_overwrite_dma_alignment_(native_file_.disk_overwrite_dma_alignment())
  , native_write_max_length_(
      std::min<std::uint64_t>(
        native_file_.disk_write_max_length(),
        maximum_contiguous_allocation_bytes))
  , append_chunk_limit_(
      round_down(native_write_max_length_, disk_write_dma_alignment_)) {
    KWAQUE_INVARIANT(
      file_alignment_invariant,
      std::has_single_bit(native_file_.memory_dma_alignment())
        && std::has_single_bit(memory_dma_alignment_)
        && std::has_single_bit(disk_read_dma_alignment_)
        && std::has_single_bit(disk_write_dma_alignment_)
        && std::has_single_bit(disk_overwrite_dma_alignment_)
        && std::max(
             {memory_dma_alignment_,
              disk_read_dma_alignment_,
              disk_write_dma_alignment_,
              disk_overwrite_dma_alignment_})
             <= maximum_contiguous_allocation_bytes,
      "native file reported an invalid or oversized DMA alignment");
    if (append_chunk_limit_ == 0) {
        append_chunk_limit_ = disk_write_dma_alignment_;
    }
}

bool file::move_is_idle(const file& other) noexcept {
    return other.operations_.get_count() == 0
           && other.state_ != file_state::closing
           && other.write_serialization_.current() == 1
           && other.write_serialization_.waiters() == 0
           && other.read_operation_units_.current()
                == other.limits_.pending_reads
           && other.read_byte_units_.current()
                == other.limits_.pending_read_bytes.value()
           && other.metadata_operation_units_.current()
                == other.limits_.pending_metadata_operations
           && other.queued_write_operation_units_.current()
                == other.limits_.queued_writes
           && other.queued_write_byte_units_.current()
                == other.limits_.queued_write_bytes.value();
}

owner_shard file::prepare_move(file& other) noexcept {
    other.owner_.assert_current();
    KWAQUE_INVARIANT(
      file_move_invariant,
      move_is_idle(other),
      "file moved with an operation in progress");
    return other.owner_;
}

file::file(file&& other) noexcept
  : owner_(prepare_move(other))
  , statistics_owner_(std::move(other.statistics_owner_))
  , statistics_(&statistics_owner_.get())
  , limits_(other.limits_)
  , native_file_(std::move(other.native_file_))
  , io_intent_(std::move(other.io_intent_))
  , operations_(std::move(other.operations_))
  , read_operation_units_(std::move(other.read_operation_units_))
  , read_byte_units_(std::move(other.read_byte_units_))
  , metadata_operation_units_(std::move(other.metadata_operation_units_))
  , queued_write_operation_units_(
      std::move(other.queued_write_operation_units_))
  , queued_write_byte_units_(std::move(other.queued_write_byte_units_))
  , write_serialization_(std::move(other.write_serialization_))
  , memory_dma_alignment_(other.memory_dma_alignment_)
  , disk_read_dma_alignment_(other.disk_read_dma_alignment_)
  , disk_write_dma_alignment_(other.disk_write_dma_alignment_)
  , disk_overwrite_dma_alignment_(other.disk_overwrite_dma_alignment_)
  , native_write_max_length_(other.native_write_max_length_)
  , append_chunk_limit_(other.append_chunk_limit_)
  , close_done_(std::move(other.close_done_))
  , state_(other.state_)
  , abort_requested_(other.abort_requested_) {
    other.state_ = file_state::closed;
    other.abort_requested_ = true;
    other.moved_from_ = true;
}

file::~file() {
    owner_.assert_current();
    KWAQUE_INVARIANT(
      file_stopped_invariant,
      moved_from_
        || (state_ == file_state::closed && operations_.get_count() == 0
            && write_serialization_.current() == 1
            && write_serialization_.waiters() == 0
            && read_operation_units_.current() == limits_.pending_reads
            && read_byte_units_.current()
                 == limits_.pending_read_bytes.value()
            && metadata_operation_units_.current()
                 == limits_.pending_metadata_operations
            && queued_write_operation_units_.current()
                 == limits_.queued_writes
            && queued_write_byte_units_.current()
                 == limits_.queued_write_bytes.value()),
      "file destroyed before close completed");
}

std::optional<file::admission_reservation>
file::try_acquire_read(byte_count bytes) noexcept {
    auto operation = seastar::try_get_units(read_operation_units_, 1);
    if (!operation) {
        return std::nullopt;
    }
    auto byte_reservation = seastar::try_get_units(
      read_byte_units_, bytes.value());
    if (!byte_reservation) {
        return std::nullopt;
    }
    return admission_reservation{
      std::move(*operation), std::move(*byte_reservation)};
}

std::optional<file::admission_reservation>
file::try_acquire_queued_write(byte_count bytes) noexcept {
    auto operation = seastar::try_get_units(queued_write_operation_units_, 1);
    if (!operation) {
        return std::nullopt;
    }
    auto byte_reservation = seastar::try_get_units(
      queued_write_byte_units_, bytes.value());
    if (!byte_reservation) {
        return std::nullopt;
    }
    return admission_reservation{
      std::move(*operation), std::move(*byte_reservation)};
}

std::optional<seastar::semaphore_units<>>
file::try_acquire_metadata() noexcept {
    return seastar::try_get_units(metadata_operation_units_, 1);
}

void file::request_abort() {
    owner_.assert_current();
    if (moved_from_ || state_ == file_state::closed || abort_requested_) {
        return;
    }
    abort_requested_ = true;
    io_intent_.cancel();
}

std::optional<operation_error> file::operation_rejection() const {
    if (moved_from_ || state_ != file_state::open) {
        return file_error(errc::closed);
    }
    if (abort_requested_) {
        return file_error(errc::aborted);
    }
    return std::nullopt;
}

seastar::future<result<void>> file::flush() {
    owner_.assert_current();
    if (auto rejected = operation_rejection()) {
        statistics_->reject();
        co_return failure(std::move(*rejected));
    }
    auto admission = try_acquire_metadata();
    if (!admission) {
        statistics_->reject();
        co_return failure(file_error(errc::queue_full));
    }
    static_cast<void>(*admission);
    auto holder = operations_.try_hold();
    KWAQUE_INVARIANT(
      file_gate_invariant,
      holder.has_value(),
      "open file rejected operation gate entry");
    [[maybe_unused]] auto metric = statistics_->accept();
    try {
        co_await native_file_.flush();
        co_return result<void>{};
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        co_return failure(file_error_from_exception(std::current_exception()));
    }
}

seastar::future<result<file_read_result>>
file::read(file_position position, byte_count maximum_bytes) {
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
          file_error(errc::out_of_range));
        return seastar::make_ready_future<result<file_read_result>>(
          std::move(outcome));
    }
    auto admission = try_acquire_read(maximum_bytes);
    if (!admission) {
        statistics_->reject();
        result<file_read_result> outcome = failure(
          file_error(errc::queue_full));
        return seastar::make_ready_future<result<file_read_result>>(
          std::move(outcome));
    }
    auto holder = operations_.try_hold();
    KWAQUE_INVARIANT(
      file_gate_invariant,
      holder.has_value(),
      "open file rejected operation gate entry");
    auto metric = statistics_->accept();
    const auto native_request = bounded_native_bulk_read(
      position.value(), maximum_bytes.value(), disk_read_dma_alignment_);
    if (native_request.bytes != maximum_bytes.value()) {
        return read_chunked(
          position,
          maximum_bytes,
          std::move(*admission),
          std::move(*holder),
          std::move(metric));
    }
    return native_file_
      .dma_read_bulk<char>(position.value(), native_request.bytes, &io_intent_)
      .then_wrapped(
        [admission = std::move(*admission),
         holder = std::move(*holder),
         metric = std::move(metric),
         maximum_bytes,
         retained_bytes = native_request.retained_bytes](
          seastar::future<seastar::temporary_buffer<char>> completed) mutable
          -> result<file_read_result> {
            static_cast<void>(admission);
            static_cast<void>(holder);
            try {
                auto native = completed.get();
                if (native.size() > maximum_bytes.value()) {
                    native.trim(
                      static_cast<std::size_t>(maximum_bytes.value()));
                }
                const bool eof = native.size() < maximum_bytes.value();
                const auto retained = native.empty() ? byte_count{}
                                                     : retained_bytes;
                auto data = detail::fragmented_buffer_io_access::adopt(
                  std::move(native), retained);
                metric.add_completed_bytes(data.size().value());
                return file_read_result::make(
                  std::move(data), eof, maximum_bytes);
            } catch (const std::bad_alloc&) {
                throw;
            } catch (...) {
                return failure(
                  file_error_from_exception(std::current_exception()));
            }
        });
}

seastar::future<result<file_read_result>> file::read_chunked(
  file_position position,
  byte_count maximum_bytes,
  admission_reservation admission,
  seastar::gate::holder holder,
  operation_statistics::reservation metric) {
    static_cast<void>(admission);
    static_cast<void>(holder);
    bytes::fragmented_buffer data;
    auto current = position.value();
    auto remaining = maximum_bytes.value();
    bool eof = false;
    try {
        while (remaining != 0) {
            const auto native_request = bounded_native_bulk_read(
              current, remaining, disk_read_dma_alignment_);
            const auto requested = native_request.bytes;
            auto native = co_await native_file_.dma_read_bulk<char>(
              current, requested, &io_intent_);
            if (native.size() > requested) {
                native.trim(requested);
            }
            const auto received = native.size();
            if (received != 0) {
                const auto appended
                  = detail::fragmented_buffer_io_access::append_adopted(
                    data, std::move(native), native_request.retained_bytes);
                KWAQUE_INVARIANT(
                  file_consumption_invariant,
                  appended.has_value(),
                  "bounded file read could not adopt a native chunk");
                current += received;
                remaining -= received;
            }
            if (received < requested) {
                eof = true;
                break;
            }
        }
        metric.add_completed_bytes(data.size().value());
        co_return file_read_result::make(std::move(data), eof, maximum_bytes);
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        co_return failure(file_error_from_exception(std::current_exception()));
    }
}

seastar::future<result<byte_count>>
file::write(file_position position, bytes::fragmented_buffer data) {
    owner_.assert_current();
    if (auto valid = validate_file_write_request(position, data); !valid) {
        statistics_->reject();
        result<byte_count> outcome = failure(valid.error());
        return seastar::make_ready_future<result<byte_count>>(
          std::move(outcome));
    }
    if (auto rejected = operation_rejection()) {
        statistics_->reject();
        result<byte_count> outcome = failure(std::move(*rejected));
        return seastar::make_ready_future<result<byte_count>>(
          std::move(outcome));
    }
    const auto append_alignment = disk_write_dma_alignment_;
    const auto memory_alignment = memory_dma_alignment_;
    const auto data_size = data.size().value();
    const bool has_one_fragment = data.fragment_count() == 1;
    const auto only_fragment = has_one_fragment ? *data.begin()
                                                : bytes::fragment_view{};
    auto serialization = seastar::try_get_units(write_serialization_, 1);
    if (
      serialization && has_one_fragment
      && is_aligned(position.value(), append_alignment)
      && is_aligned(data_size, append_alignment)
      && data_size <= append_chunk_limit_
      && is_aligned(only_fragment.data(), memory_alignment)) {
        auto metric = statistics_->accept();
        auto consumer = detail::fragmented_buffer_io_access::consume(data);
        auto fragment = consumer.take_front();
        const auto expected = fragment.size();
        return native_file_
          .dma_write(position.value(), fragment.get(), expected, &io_intent_)
          .then_wrapped(
            [this,
             position = position.value(),
             data = std::move(data),
             fragment = std::move(fragment),
             serialization = std::move(*serialization),
             metric = std::move(metric),
             expected,
             append_alignment,
             memory_alignment](seastar::future<std::size_t> completed) mutable
              -> seastar::future<result<byte_count>> {
                try {
                    const auto written = completed.get();
                    if (written == 0 || written > expected) {
                        result<byte_count> outcome = failure(
                          file_error(errc::io_failure));
                        return seastar::make_ready_future<result<byte_count>>(
                          std::move(outcome));
                    }
                    if (written == expected) {
                        metric.add_completed_bytes(
                          static_cast<std::uint64_t>(expected));
                        result<byte_count> outcome = byte_count{
                          static_cast<std::uint64_t>(expected)};
                        return seastar::make_ready_future<result<byte_count>>(
                          std::move(outcome));
                    }
                    return writer::finish_direct(
                      *this,
                      position,
                      std::move(data),
                      std::move(fragment),
                      std::move(serialization),
                      written,
                      append_alignment,
                      memory_alignment,
                      std::move(metric));
                } catch (const std::bad_alloc&) {
                    return seastar::current_exception_as_future<
                      result<byte_count>>();
                } catch (...) {
                    result<byte_count> outcome = failure(
                      file_error_from_exception(std::current_exception()));
                    return seastar::make_ready_future<result<byte_count>>(
                      std::move(outcome));
                }
            });
    }
    std::optional<admission_reservation> queued;
    if (!serialization) {
        queued = try_acquire_queued_write(data.retained_bytes());
        if (!queued) {
            statistics_->reject();
            result<byte_count> outcome = failure(file_error(errc::queue_full));
            return seastar::make_ready_future<result<byte_count>>(
              std::move(outcome));
        }
    }
    auto holder = operations_.try_hold();
    KWAQUE_INVARIANT(
      file_gate_invariant,
      holder.has_value(),
      "open file rejected operation gate entry");
    auto metric = statistics_->accept();
    return writer::run(
      writer{
        *this,
        position,
        std::move(data),
        std::move(*holder),
        std::move(metric)},
      std::move(serialization),
      std::move(queued));
}

seastar::future<result<void>> file::truncate(std::uint64_t size) {
    owner_.assert_current();
    if (auto rejected = operation_rejection()) {
        statistics_->reject();
        co_return failure(std::move(*rejected));
    }
    auto serialization = seastar::try_get_units(write_serialization_, 1);
    std::optional<admission_reservation> queued;
    if (!serialization) {
        queued = try_acquire_queued_write(byte_count{});
        if (!queued) {
            statistics_->reject();
            co_return failure(file_error(errc::queue_full));
        }
    }
    auto holder = operations_.try_hold();
    KWAQUE_INVARIANT(
      file_gate_invariant,
      holder.has_value(),
      "open file rejected operation gate entry");
    [[maybe_unused]] auto metric = statistics_->accept();
    try {
        if (!serialization) {
            serialization.emplace(
              co_await seastar::coroutine::without_preemption_check(
                seastar::get_units(write_serialization_, 1)));
        }
        queued.reset();
        static_cast<void>(*serialization);
        if (auto rejected = operation_rejection()) {
            co_return failure(std::move(*rejected));
        }
        co_await native_file_.truncate(size);
        co_return result<void>{};
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        co_return failure(file_error_from_exception(std::current_exception()));
    }
}

seastar::future<result<std::uint64_t>> file::size() {
    owner_.assert_current();
    if (auto rejected = operation_rejection()) {
        statistics_->reject();
        co_return failure(std::move(*rejected));
    }
    auto admission = try_acquire_metadata();
    if (!admission) {
        statistics_->reject();
        co_return failure(file_error(errc::queue_full));
    }
    static_cast<void>(*admission);
    auto holder = operations_.try_hold();
    KWAQUE_INVARIANT(
      file_gate_invariant,
      holder.has_value(),
      "open file rejected operation gate entry");
    [[maybe_unused]] auto metric = statistics_->accept();
    try {
        co_return co_await native_file_.size();
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        co_return failure(file_error_from_exception(std::current_exception()));
    }
}

seastar::future<result<void>> file::close() {
    owner_.assert_current();
    if (moved_from_) {
        return seastar::make_ready_future<result<void>>(result<void>{});
    }
    if (state_ == file_state::closing) {
        return close_done_->get_shared_future();
    }
    if (state_ == file_state::closed) {
        return close_done_ && close_done_->available()
                 ? close_done_->get_shared_future()
                 : seastar::make_ready_future<result<void>>(result<void>{});
    }

    try {
        close_done_.emplace();
    } catch (...) {
        return seastar::current_exception_as_future<result<void>>();
    }
    state_ = file_state::closing;
    abort_requested_ = true;
    io_intent_.cancel();
    auto metric = statistics_->accept();
    auto completion = close_once().then_wrapped(
      [this, metric = std::move(metric)](
        seastar::future<result<void>> closed) mutable {
          static_cast<void>(metric);
          state_ = file_state::closed;
          try {
              close_done_->set_value(closed.get());
          } catch (...) {
              close_done_->set_exception(std::current_exception());
          }
      });
    static_cast<void>(completion);
    return close_done_->get_shared_future();
}

seastar::future<result<void>> file::close_once() {
    co_await operations_.close();
    try {
        auto serialization
          = co_await seastar::coroutine::without_preemption_check(
            seastar::get_units(write_serialization_, 1));
        static_cast<void>(serialization);
        co_await native_file_.close();
        co_return result<void>{};
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        co_return failure(file_error_from_exception(std::current_exception()));
    }
}

file_state file::state() const {
    owner_.assert_current();
    return state_;
}

bool file::abort_requested() const {
    owner_.assert_current();
    return abort_requested_;
}

std::uint32_t file::pending_reads() const {
    owner_.assert_current();
    return static_cast<std::uint32_t>(
      limits_.pending_reads - read_operation_units_.current());
}

byte_count file::pending_read_bytes() const {
    owner_.assert_current();
    return byte_count{
      limits_.pending_read_bytes.value() - read_byte_units_.current()};
}

std::uint32_t file::pending_metadata_operations() const {
    owner_.assert_current();
    return static_cast<std::uint32_t>(
      limits_.pending_metadata_operations
      - metadata_operation_units_.current());
}

std::uint32_t file::queued_writes() const {
    owner_.assert_current();
    return static_cast<std::uint32_t>(
      limits_.queued_writes - queued_write_operation_units_.current());
}

byte_count file::queued_write_bytes() const {
    owner_.assert_current();
    return byte_count{
      limits_.queued_write_bytes.value() - queued_write_byte_units_.current()};
}

} // namespace kwaque::runtime
