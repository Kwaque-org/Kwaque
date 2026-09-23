#include "src/runtime/file.h"

#include "src/base/invariant.h"
#include "src/runtime/directory_cursor_internal.h"
#include "src/runtime/file_error_internal.h"
#include "src/runtime/fragmented_buffer_internal.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/defer.hh>

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

result<void> directory_page_limits::validate() const noexcept {
    if (
      maximum_entries.value() == 0
      || maximum_entries.value() > maximum_directory_page_entries
      || maximum_name_bytes.value() == 0
      || maximum_name_bytes > maximum_directory_page_name_bytes) {
        return failure(
          operation_error{errc::invalid_argument, operation_kind::file});
    }
    return {};
}

directory_page::directory_page(
  directory_listing listing,
  bool end,
  seastar::lw_shared_ptr<detail::directory_cursor_memory> memory,
  seastar::semaphore_units<> reservation) noexcept
  : memory_(std::move(memory))
  , reservation_(std::move(reservation))
  , listing_(std::move(listing))
  , end_(end) {}
directory_page::directory_page(directory_page&&) noexcept = default;
directory_page::~directory_page() {
    if (memory_) memory_->assert_current();
}
directory_page& directory_page::operator=(directory_page&& other) noexcept {
    if (this != &other) {
        directory_page previous(std::move(other));
        using std::swap;
        swap(memory_, previous.memory_);
        swap(reservation_, previous.reservation_);
        swap(listing_, previous.listing_);
        swap(end_, previous.end_);
    }
    return *this;
}

namespace {

constexpr invariant_id file_move_invariant{"KQ-FILE-MOVE-IDLE"};
constexpr invariant_id file_stopped_invariant{"KQ-FILE-CLOSED"};
constexpr invariant_id file_gate_invariant{"KQ-FILE-GATE-OPEN"};
constexpr invariant_id file_alignment_invariant{"KQ-FILE-DMA-ALIGNMENT"};
constexpr invariant_id file_consumption_invariant{"KQ-FILE-WRITE-CONSUMED"};

operation_error file_error(errc code) noexcept {
    return operation_error{code, operation_kind::file};
}

bool contains_nul(std::string_view value) noexcept {
    return std::find(value.begin(), value.end(), '\0') != value.end();
}

file_io_limits validated_file_io_limits(file_io_limits limits) {
    if (!limits.validate()) {
        throw std::invalid_argument("invalid file I/O limits");
    }
    return limits;
}

operation_error file_error_from_exception(std::exception_ptr exception) {
    try {
        std::rethrow_exception(std::move(exception));
    } catch (const kwaque::runtime::detail::file_operation_exception& error) {
        return error.error();
    } catch (const seastar::cancelled_error&) {
        return file_error(errc::aborted);
    } catch (const std::system_error& error) {
        return detail::map_file_operation_error(error.code());
    }
}

seastar::file
require_open_file(seastar::file&& native_file, file_close_policy policy) {
    if (!native_file) {
        throw std::invalid_argument("file requires an open native handle");
    }
    if (
      policy != file_close_policy::legacy
      && policy != file_close_policy::checked)
        throw std::invalid_argument("invalid file close policy");
    if (
      policy == file_close_policy::checked
      && !native_file.supports_checked_close())
        throw detail::file_operation_exception{file_error(errc::unavailable)};
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

[[nodiscard, gnu::noinline]] seastar::temporary_buffer<char>
stage_aligned_write(
  bytes::fragmented_buffer& data,
  std::uint64_t data_size,
  std::uint64_t memory_alignment) {
    const auto allocation = round_up(data_size, memory_alignment);
    KWAQUE_INVARIANT(
      file_alignment_invariant,
      allocation <= maximum_contiguous_allocation_bytes,
      "file staging exceeded its contiguous allocation limit");
    auto fragment = seastar::temporary_buffer<char>::aligned(
      static_cast<std::size_t>(memory_alignment),
      static_cast<std::size_t>(allocation));
    auto consumer = detail::fragmented_buffer_io_access::consume(data);
    const auto staged = consumer.copy_front_to(
      std::span<char>{
        fragment.get_write(), static_cast<std::size_t>(data_size)});
    KWAQUE_INVARIANT(
      file_consumption_invariant,
      staged == data_size && data.empty(),
      "fragmented write ended before its declared size");
    fragment.trim(static_cast<std::size_t>(data_size));
    return fragment;
}

} // namespace

bool file_geometry::supports_disk_alignment(
  byte_count alignment) const noexcept {
    const auto value = alignment.value();
    return std::has_single_bit(value) && value % read_.value() == 0
           && value % write_.value() == 0 && value % overwrite_.value() == 0;
}

result<file_system_space> file_system_space::make(
  byte_count capacity,
  byte_count free,
  byte_count available,
  bool read_only) noexcept {
    const auto sentinel = std::numeric_limits<std::uint64_t>::max();
    if (
      capacity.value() == sentinel || free.value() == sentinel
      || available.value() == sentinel || available > free || free > capacity)
        return failure(file_error(errc::invalid_argument));
    return file_system_space{capacity, free, available, read_only};
}

result<file_system_space> file_system_space::from_blocks(
  std::uint64_t fragment_bytes,
  std::uint64_t blocks,
  std::uint64_t free_blocks,
  std::uint64_t available_blocks,
  bool read_only) noexcept {
    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    if (
      fragment_bytes == 0 || fragment_bytes == maximum || blocks == maximum
      || free_blocks == maximum || available_blocks == maximum
      || available_blocks > free_blocks || free_blocks > blocks)
        return failure(file_error(errc::invalid_argument));
    if (blocks > maximum / fragment_bytes)
        return failure(file_error(errc::out_of_range));
    return make(
      byte_count{blocks * fragment_bytes},
      byte_count{free_blocks * fragment_bytes},
      byte_count{available_blocks * fragment_bytes},
      read_only);
}

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
            if (auto rejected = self.owner_.mutation_rejection(true)) {
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
                        throw detail::file_operation_exception{make_file_error(
                          errc::io_failure, file_failure_detail::unknown)};
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
                    throw detail::file_operation_exception{make_file_error(
                      errc::io_failure, file_failure_detail::unknown)};
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
        } catch (...) {
            co_return failure(
              self.owner_.remember_io_failure(std::current_exception()));
        }
    }

    [[nodiscard]] static seastar::future<result<byte_count>> finish_direct(
      file& owner,
      seastar::gate::holder holder,
      std::uint64_t position,
      bytes::fragmented_buffer data,
      seastar::temporary_buffer<char> fragment,
      seastar::semaphore_units<> serialization,
      std::size_t completed,
      std::uint64_t write_alignment,
      std::uint64_t memory_alignment,
      operation_statistics::reservation metric) {
        static_cast<void>(holder);
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
        } catch (...) {
            co_return failure(
              owner.remember_io_failure(std::current_exception()));
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
            throw detail::file_operation_exception{
              make_file_error(errc::io_failure, file_failure_detail::unknown)};
        }
        while (completed < size) {
            const auto current_position
              = position + static_cast<std::uint64_t>(completed);
            const auto remaining = size - completed;
            if (
              !is_aligned(current_position, write_alignment)
              || !is_aligned(
                static_cast<std::uint64_t>(remaining), write_alignment)) {
                throw detail::file_operation_exception{make_file_error(
                  errc::io_failure, file_failure_detail::unknown)};
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
                throw detail::file_operation_exception{make_file_error(
                  errc::io_failure, file_failure_detail::unknown)};
            }
            completed += written;
            if (
              completed < size
              && !is_aligned(
                static_cast<std::uint64_t>(completed), write_alignment)) {
                throw detail::file_operation_exception{make_file_error(
                  errc::io_failure, file_failure_detail::unknown)};
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
                throw detail::file_operation_exception{make_file_error(
                  errc::io_failure, file_failure_detail::unknown)};
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
            throw detail::file_operation_exception{
              make_file_error(errc::io_failure, file_failure_detail::unknown)};
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

result<file_path> file_path::make(std::string_view value) {
    if (value.size() > maximum_file_path_bytes)
        return failure(file_error(errc::out_of_range));
    if (value.empty() || contains_nul(value))
        return failure(file_error(errc::invalid_argument));
    return file_path{std::string{value}};
}

result<file_name> file_name::make(std::string_view value) {
    if (value.size() > maximum_file_name_bytes)
        return failure(file_error(errc::out_of_range));
    if (
      value.empty() || value == "." || value == ".."
      || value.find('/') != std::string_view::npos || contains_nul(value)) {
        return failure(file_error(errc::invalid_argument));
    }
    return file_name{std::string{value}};
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
  directory_listing_limits limits) {
    if (auto valid = limits.validate(); !valid) {
        return failure(valid.error());
    }
    if (entries.size() > limits.maximum_entries.value()) {
        return failure(file_error(errc::resource_exhausted));
    }
    std::uint64_t name_bytes = 0;
    for (const auto& entry : entries) {
        if (
          static_cast<std::uint8_t>(entry.kind)
          > static_cast<std::uint8_t>(file_kind::other)) {
            return failure(file_error(errc::invalid_argument));
        }
        const auto size = static_cast<std::uint64_t>(entry.name.value().size());
        if (size > limits.maximum_name_bytes.value() - name_bytes) {
            return failure(file_error(errc::resource_exhausted));
        }
        name_bytes += size;
    }
    // Repeated pops can retain native outer descriptor capacity even
    // when capacity() is zero. Transfer values into fresh bounded storage so
    // caller allocation history cannot escape inside this validated result.
    seastar::chunked_vector<directory_entry> bounded;
    bounded.reserve(entries.size());
    for (auto& entry : entries)
        bounded.push_back(std::move(entry));
    return directory_listing{std::move(bounded)};
}

result<void> file_open_options::validate() const noexcept {
    const auto access_value = static_cast<std::uint8_t>(access);
    if (
      access_value > static_cast<std::uint8_t>(file_access::read_write)
      || (exclusive && !create)
      || (truncate && access == file_access::read_only) || permissions > 0777U
      || static_cast<std::uint8_t>(close_policy)
           > static_cast<std::uint8_t>(file_close_policy::checked)) {
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
  operation_statistics_owner statistics,
  file_close_policy close_policy)
  : statistics_owner_(std::move(statistics))
  , statistics_(&statistics_owner_.get())
  , limits_(validated_file_io_limits(limits))
  , native_file_(require_open_file(std::move(native_file), close_policy))
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
      round_down(native_write_max_length_, disk_write_dma_alignment_))
  , close_policy_(close_policy) {
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
  , first_failure_(other.first_failure_)
  , close_policy_(other.close_policy_)
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

file::metadata_reservation::metadata_reservation(
  file& owner, seastar::semaphore_units<> units) noexcept
  : owner_(&owner)
  , units_(std::move(units)) {}

file::metadata_reservation::metadata_reservation(
  metadata_reservation&& other) noexcept
  : owner_(other.owner_) {
    if (owner_) owner_->owner_.assert_current();
    KWAQUE_INVARIANT(
      file_move_invariant,
      !other.active_,
      "metadata reservation moved during a flush");
    units_ = std::move(other.units_);
    other.owner_ = nullptr;
}

file::metadata_reservation&
file::metadata_reservation::operator=(metadata_reservation&& other) noexcept {
    if (this != &other) {
        release();
        if (other.owner_) other.owner_->owner_.assert_current();
        KWAQUE_INVARIANT(
          file_move_invariant,
          !other.active_,
          "metadata reservation moved during a flush");
        owner_ = std::exchange(other.owner_, nullptr);
        units_ = std::move(other.units_);
    }
    return *this;
}

file::metadata_reservation::~metadata_reservation() { release(); }

void file::metadata_reservation::release() noexcept {
    if (owner_) owner_->owner_.assert_current();
    KWAQUE_INVARIANT(
      file_stopped_invariant,
      !active_,
      "metadata reservation released during a flush");
    units_.return_all();
    owner_ = nullptr;
}

result<file::metadata_reservation> file::try_reserve_metadata() {
    owner_.assert_current();
    if (auto rejected = operation_rejection()) {
        return failure(*rejected);
    }
    auto admission = try_acquire_metadata();
    if (!admission) {
        return failure(make_file_error(
          errc::queue_full, file_failure_detail::admission_not_dispatched));
    }
    return metadata_reservation{*this, std::move(*admission)};
}

seastar::future<result<void>> file::flush(metadata_reservation& reservation) {
    owner_.assert_current();
    if (reservation.owner_ != this || reservation.units_.count() != 1) {
        statistics_->reject();
        co_return failure(make_file_error(
          errc::invalid_argument,
          file_failure_detail::admission_not_dispatched));
    }
    if (auto rejected = mutation_rejection()) {
        statistics_->reject();
        co_return failure(*rejected);
    }
    if (reservation.active_) {
        statistics_->reject();
        co_return failure(make_file_error(
          errc::queue_full, file_failure_detail::admission_not_dispatched));
    }
    auto holder = operations_.hold();
    reservation.active_ = true;
    auto active = seastar::defer(
      [&reservation] noexcept { reservation.active_ = false; });
    [[maybe_unused]] auto metric = statistics_->accept();
    try {
        co_await native_file_.flush();
        if (first_failure_.failed()) co_return first_failure_.outcome();
        co_return result<void>{};
    } catch (...) {
        co_return failure(remember_io_failure(std::current_exception()));
    }
}

operation_error file::remember_error(operation_error error) {
    const auto detail = file_detail(error);
    bool qualified = false;
    for (std::size_t index = 0; index < error.context_size(); ++index)
        qualified |= error.context_at(index)->key
                     == operation_context_key::detail;
    if (
      qualified && detail
      && *detail != file_failure_detail::admission_not_dispatched
      && error.code() != errc::aborted && !first_failure_.failed())
        first_failure_.observe(error);
    if (
      close_policy_ == file_close_policy::checked
      && (!detail || *detail != file_failure_detail::admission_not_dispatched)) {
        first_failure_.observe(error);
        if (first_failure_.exception())
            std::rethrow_exception(first_failure_.exception());
        return *first_failure_.error();
    }
    return first_failure_.error().value_or(error);
}

operation_error file::remember_io_failure(std::exception_ptr exception) {
    try {
        return remember_error(file_error_from_exception(exception));
    } catch (...) {
        if (close_policy_ == file_close_policy::checked)
            first_failure_.observe(exception);
        throw;
    }
}

seastar::future<result<void>> file::flush() {
    owner_.assert_current();
    if (auto rejected = mutation_rejection()) {
        statistics_->reject();
        co_return failure(std::move(*rejected));
    }
    auto admission = try_acquire_metadata();
    if (!admission) {
        statistics_->reject();
        co_return failure(make_file_error(
          errc::queue_full, file_failure_detail::admission_not_dispatched));
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
        if (first_failure_.failed()) co_return first_failure_.outcome();
        co_return result<void>{};
    } catch (...) {
        co_return failure(remember_io_failure(std::current_exception()));
    }
}

seastar::future<result<file_read_result>>
file::read_validated(file_position position, byte_count maximum_bytes) {
    auto admission = try_acquire_read(maximum_bytes);
    if (!admission) {
        statistics_->reject();
        result<file_read_result> outcome = failure(make_file_error(
          errc::queue_full, file_failure_detail::admission_not_dispatched));
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
file::write_validated(file_position position, bytes::fragmented_buffer&& data) {
    try {
        const auto append_alignment = disk_write_dma_alignment_;
        const auto memory_alignment = memory_dma_alignment_;
        const auto data_size = data.size().value();
        const bool has_one_fragment = data.fragment_count() == 1;
        const auto front_fragment = *data.begin();
        const bool aligned_front = is_aligned(
          front_fragment.data(), memory_alignment);
        auto serialization = seastar::try_get_units(write_serialization_, 1);
        if (
          serialization && is_aligned(position.value(), append_alignment)
          && is_aligned(data_size, append_alignment)
          && data_size <= append_chunk_limit_
          && (has_one_fragment || front_fragment.size() < append_alignment || !aligned_front)) {
            seastar::gate::holder holder;
            if (close_policy_ == file_close_policy::checked) {
                holder = seastar::gate::holder{operations_};
            }
            auto metric = statistics_->accept();
            seastar::temporary_buffer<char> fragment;
            if (has_one_fragment && aligned_front) {
                auto consumer = detail::fragmented_buffer_io_access::consume(
                  data);
                fragment = consumer.take_front();
            } else {
                fragment = stage_aligned_write(
                  data, data_size, memory_alignment);
            }
            if (auto rejected = mutation_rejection()) {
                return seastar::make_ready_future<result<byte_count>>(
                  failure(std::move(*rejected)));
            }
            const auto expected = fragment.size();
            auto completed = native_file_.dma_write(
              position.value(), fragment.get(), expected, &io_intent_);
#ifndef SEASTAR_DEBUG
            // Match native ready completion without constructing pending state.
            if (completed.available()) {
                const auto written = completed.get();
                if (written == 0 || written > expected) {
                    return seastar::make_ready_future<result<byte_count>>(
                      failure(remember_error(file_error(errc::io_failure))));
                }
                if (written == expected) {
                    metric.add_completed_bytes(
                      static_cast<std::uint64_t>(expected));
                    return seastar::make_ready_future<result<byte_count>>(
                      byte_count{static_cast<std::uint64_t>(expected)});
                }
                return writer::finish_direct(
                  *this,
                  std::move(holder),
                  position.value(),
                  std::move(data),
                  std::move(fragment),
                  std::move(*serialization),
                  written,
                  append_alignment,
                  memory_alignment,
                  std::move(metric));
            }
#endif
            return std::move(completed).then_wrapped(
              [this,
               holder = std::move(holder),
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
                            remember_error(file_error(errc::io_failure)));
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
                        std::move(holder),
                        position,
                        std::move(data),
                        std::move(fragment),
                        std::move(serialization),
                        written,
                        append_alignment,
                        memory_alignment,
                        std::move(metric));
                  } catch (...) {
                      result<byte_count> outcome = failure(
                        remember_io_failure(std::current_exception()));
                      return seastar::make_ready_future<result<byte_count>>(
                        std::move(outcome));
                  }
              });
        }
        return write_general(
          position, std::move(data), std::move(serialization));
    } catch (...) {
        return seastar::futurize_invoke(
          [this, exception = std::current_exception()] -> result<byte_count> {
              return failure(remember_io_failure(exception));
          });
    }
}

seastar::future<result<byte_count>> file::write_general(
  file_position position,
  bytes::fragmented_buffer&& data,
  std::optional<seastar::semaphore_units<>> serialization) {
    std::optional<admission_reservation> queued;
    if (!serialization) {
        queued = try_acquire_queued_write(data.retained_bytes());
        if (!queued) {
            statistics_->reject();
            result<byte_count> outcome = failure(make_file_error(
              errc::queue_full, file_failure_detail::admission_not_dispatched));
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
    if (auto rejected = mutation_rejection()) {
        statistics_->reject();
        co_return failure(std::move(*rejected));
    }
    auto serialization = seastar::try_get_units(write_serialization_, 1);
    std::optional<admission_reservation> queued;
    if (!serialization) {
        queued = try_acquire_queued_write(byte_count{});
        if (!queued) {
            statistics_->reject();
            co_return failure(make_file_error(
              errc::queue_full, file_failure_detail::admission_not_dispatched));
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
        if (auto rejected = mutation_rejection(true)) {
            co_return failure(std::move(*rejected));
        }
        co_await native_file_.truncate(size);
        co_return result<void>{};
    } catch (...) {
        co_return failure(remember_io_failure(std::current_exception()));
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
        co_return failure(make_file_error(
          errc::queue_full, file_failure_detail::admission_not_dispatched));
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
    if (close_policy_ == file_close_policy::checked) {
        if (state_ == file_state::closing)
            return seastar::make_ready_future<result<void>>(
              failure(make_file_error(
                errc::queue_full,
                file_failure_detail::admission_not_dispatched)));
        if (state_ == file_state::closed) {
            if (first_failure_.exception())
                return seastar::make_exception_future<result<void>>(
                  first_failure_.exception());
            return seastar::make_ready_future<result<void>>(
              first_failure_.outcome());
        }
        return close_checked_once();
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

seastar::future<result<void>> file::close_checked_once() {
    // The coroutine frame is obtained before admission changes. No shared
    // promise or unbounded close-waiter list is created during shutdown.
    state_ = file_state::closing;
    [[maybe_unused]] auto metric = statistics_->accept();
    co_await operations_.close();
    // Checked direct writes retain the gate through short-write recovery too.
    // Draining it leaves the serializer free without allocating a close waiter.
    auto serialization = seastar::try_get_units(write_serialization_, 1);
    KWAQUE_INVARIANT(
      file_gate_invariant,
      serialization.has_value(),
      "file close drained its gate with a writer still active");
    try {
        co_await native_file_.close_checked();
    } catch (...) {
        if (!first_failure_.failed()) {
            try {
                first_failure_.observe(
                  file_error_from_exception(std::current_exception()));
            } catch (...) {
                first_failure_.observe(std::current_exception());
            }
        }
    }
    state_ = file_state::closed;
    co_return first_failure_.outcome();
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

result<file_geometry> file::geometry() const noexcept {
    owner_.assert_current();
    if (auto rejected = operation_rejection()) return failure(*rejected);
    const auto memory = native_file_.memory_dma_alignment();
    const auto read_max = native_file_.disk_read_max_length();
    const auto write_max = native_file_.disk_write_max_length();
    // Do not advertise a recommendation too small for even one aligned request,
    // or the writer's fallback chunk as though it respected that
    // recommendation.
    if (
      read_max < disk_read_dma_alignment_
      || write_max
           < std::max(disk_write_dma_alignment_, disk_overwrite_dma_alignment_)
      || append_chunk_limit_ > write_max)
        return failure(file_error(errc::invalid_argument));
    return file_geometry{
      byte_count{memory},
      byte_count{disk_read_dma_alignment_},
      byte_count{disk_write_dma_alignment_},
      byte_count{disk_overwrite_dma_alignment_},
      byte_count{read_max},
      byte_count{write_max},
      byte_count{append_chunk_limit_},
      limits_.pending_read_bytes};
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
