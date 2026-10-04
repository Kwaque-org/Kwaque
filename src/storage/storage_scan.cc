#include "src/storage/storage_scan.h"

#include "src/base/invariant.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/envelope_integrity.h"
#include "src/storage/local_paths.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <exception>
#include <utility>

namespace kwaque::storage {
namespace {
using detail::path_error;

bool all_zero(const codec::encoded_envelope_prefix& prefix) noexcept {
    return std::all_of(
      prefix.begin(), prefix.end(), [](char byte) { return byte == 0; });
}
// A failed extent check on these fields leaves header_bytes trustworthy, so
// the header checksum still decides between corruption and malformed bytes.
bool body_extent_field(const codec::error& error) noexcept {
    return error.field()
             == static_cast<std::uint16_t>(codec::envelope_field::body_bytes)
           || error.field()
                == static_cast<std::uint16_t>(
                  codec::envelope_field::encoded_bytes);
}
} // namespace

runtime::result<void> scan_reader_limits::validate() const noexcept {
    if (
      window_bytes.value() == 0 || window_bytes > runtime::maximum_file_io_bytes
      || read_ahead > maximum_scan_read_ahead
      || maximum_object_bytes < byte_count{codec::envelope_prefix_bytes})
        return runtime::failure(path_error(errc::invalid_argument));
    return {};
}

runtime::result<std::unique_ptr<scan_reader>> scan_reader::make(
  runtime::file& file,
  workload_budget& budget,
  runtime::file_position begin,
  runtime::file_position end,
  scan_reader_limits limits) {
    if (auto valid = limits.validate(); !valid)
        return runtime::failure(valid.error());
    if (end < begin)
        return runtime::failure(path_error(errc::invalid_argument));
    // In flight: the read-ahead plus the window being parsed. Buffered: an
    // object no larger than a window can straddle two more. Larger objects
    // add their own reservation while they are examined.
    const auto windows = limits.window_bytes.checked_mul(limits.read_ahead + 3);
    const auto bytes = windows ? windows->checked_add(limits.execution_bytes)
                               : std::nullopt;
    if (!bytes) return runtime::failure(path_error(errc::out_of_range));
    auto reservation = budget.try_reserve(*bytes);
    if (!reservation) return runtime::failure(reservation.error());
    return std::unique_ptr<scan_reader>(new scan_reader(
      file, budget, std::move(*reservation), begin, end, limits));
}

scan_reader::scan_reader(
  runtime::file& file,
  workload_budget& budget,
  workload_reservation reservation,
  runtime::file_position begin,
  runtime::file_position end,
  scan_reader_limits limits) noexcept
  : file_(file)
  , budget_(budget)
  , reservation_(std::move(reservation))
  , limits_(limits)
  , cursor_(begin)
  , end_(end)
  , buffered_begin_(begin)
  , buffered_end_(begin)
  , next_read_(begin) {}

scan_reader::~scan_reader() {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-SCAN-JOINED"},
      count_ == 0,
      "scan reader destroyed with reads in flight");
}

// Keeps the read-ahead full. Each window ends at the next multiple of the
// window size, so after the first one every read is aligned.
void scan_reader::issue() {
    const auto window = limits_.window_bytes.value();
    while (count_ < limits_.read_ahead + 1U && next_read_ < end_) {
        const auto at = next_read_;
        const auto aligned = window - (at.value() % window);
        const byte_count length{
          std::min<std::uint64_t>(aligned, end_.value() - at.value())};
        auto& slot = reads_[(head_ + count_) % read_slots];
        slot.emplace(pending_read{at, length, file_.read(at, length)});
        ++count_;
        next_read_ = runtime::file_position{at.value() + length.value()};
    }
}

seastar::future<runtime::result<void>> scan_reader::fill(
  runtime::file_position through, codec::cooperative_work& work) {
    while (buffered_end_ < through) {
        issue();
        KWAQUE_INVARIANT(
          invariant_id{"KQ-SCAN-FILL"},
          count_ != 0,
          "scan fill requested bytes past its end");
        auto pending = std::move(*reads_[head_]);
        reads_[head_].reset();
        head_ = (head_ + 1) % read_slots;
        --count_;
        // Any failure leaves a hole in the window sequence, so the reader
        // stays failed rather than continue past it.
        broken_ = true;
        auto read = co_await std::move(pending.read);
        if (!read) co_return runtime::failure(read.error());
        // The scan end was observed before scanning; a shorter read means the
        // file changed underneath, which a scan cannot classify.
        if (read->data().size() != pending.length)
            co_return runtime::failure(path_error(errc::wrong_context));
        auto data = std::move(*read).take_data();
        if (buffered_.empty()) {
            buffered_ = std::move(data);
            buffered_begin_ = pending.at;
        } else {
            bytes::fragmented_buffer_builder joined;
            if (
              !joined.append_buffer(std::move(buffered_))
              || !joined.append_buffer(std::move(data)))
                co_return runtime::failure(
                  path_error(errc::resource_exhausted));
            auto finished = joined.finish();
            if (!finished)
                co_return runtime::failure(
                  path_error(errc::resource_exhausted));
            buffered_ = std::move(*finished);
        }
        broken_ = false;
        buffered_end_ = runtime::file_position{
          pending.at.value() + pending.length.value()};
        issue();
        if (auto ready = work.poll(); !ready)
            co_return runtime::failure(path_error(ready.error().code()));
    }
    co_return runtime::result<void>{};
}

runtime::result<bytes::fragmented_buffer>
scan_reader::share(runtime::file_position at, byte_count length) {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-SCAN-SHARE"},
      at >= buffered_begin_
        && at.value() + length.value() <= buffered_end_.value(),
      "scan share outside its buffered bytes");
    auto shared = buffered_.share(
      byte_count{at.value() - buffered_begin_.value()}, length);
    if (!shared) return runtime::failure(path_error(errc::resource_exhausted));
    return std::move(*shared);
}

seastar::future<runtime::result<scan_slot>>
scan_reader::next(codec::cooperative_work& work) {
    if (closed_) co_return runtime::failure(path_error(errc::closed));
    if (broken_) co_return runtime::failure(path_error(errc::io_failure));
    if (current_) co_return *current_;
    if (auto ready = co_await detail::path_checkpoint(work); !ready)
        co_return runtime::failure(ready.error());
    auto slot = co_await classify(work);
    if (!slot) co_return slot;
    current_ = *slot;
    co_return slot;
}

seastar::future<runtime::result<scan_slot>>
scan_reader::classify(codec::cooperative_work& work) {
    object_reservation_.reset();
    const auto at = cursor_;
    const auto slot = [at](
                        scan_slot_kind kind,
                        std::optional<codec::error> cause = std::nullopt) {
        return scan_slot{kind, at, std::nullopt, cause};
    };
    const auto remaining = end_.value() - at.value();
    if (remaining == 0) co_return slot(scan_slot_kind::end);
    if (remaining < codec::envelope_prefix_bytes)
        co_return slot(scan_slot_kind::torn);
    const auto prefix_end = runtime::file_position{
      at.value() + codec::envelope_prefix_bytes};
    if (auto filled = co_await fill(prefix_end, work); !filled)
        co_return runtime::failure(filled.error());
    codec::encoded_envelope_prefix raw{};
    std::optional<codec::result<codec::unverified_envelope_prefix>> prefix;
    {
        auto bytes = share(at, byte_count{codec::envelope_prefix_bytes});
        if (!bytes) co_return runtime::failure(bytes.error());
        bytes::fragmented_buffer_parser input{std::move(*bytes)};
        if (!input.peek_to(raw))
            co_return runtime::failure(path_error(errc::invariant_violation));
        if (all_zero(raw)) co_return slot(scan_slot_kind::zero);
        const codec::field_context context{.origin = at.value()};
        prefix.emplace(
          codec::peek_envelope_prefix(
            input,
            work.policy(),
            {limits_.maximum_object_bytes, limits_.maximum_object_bytes},
            context,
            codec::input_boundary::complete));
    }
    if (!*prefix && !body_extent_field(prefix->error()))
        co_return slot(scan_slot_kind::malformed, prefix->error());
    // Header extents passed, so header_bytes is usable even when a body
    // extent was refused: its checksum decides what the refusal means.
    const auto header_bytes = static_cast<std::uint16_t>(
      static_cast<std::uint8_t>(raw[10])
      | (static_cast<std::uint16_t>(static_cast<std::uint8_t>(raw[11])) << 8U));
    if (header_bytes > remaining) co_return slot(scan_slot_kind::torn);
    const auto header_end = runtime::file_position{at.value() + header_bytes};
    if (auto filled = co_await fill(header_end, work); !filled)
        co_return runtime::failure(filled.error());
    {
        auto header = share(at, byte_count{header_bytes});
        if (!header) co_return runtime::failure(header.error());
        auto verified = co_await codec::verify_envelope_header_crc(
          *header, work, codec::field_context{.origin = at.value()});
        if (!verified) {
            if (verified.error().code() == errc::corrupt_data)
                co_return slot(scan_slot_kind::corrupt, verified.error());
            co_return runtime::failure(path_error(verified.error().code()));
        }
    }
    if (!*prefix) {
        // Its checksum proves the declared size intact: a size only the
        // limits refuse is a resource limit, never damage.
        if (prefix->error().code() == errc::resource_exhausted)
            co_return runtime::failure(path_error(errc::resource_exhausted));
        co_return slot(scan_slot_kind::malformed, prefix->error());
    }
    const auto length = (*prefix)->encoded_bytes();
    if (length.value() > remaining) co_return slot(scan_slot_kind::torn);
    if (length > limits_.window_bytes) {
        auto extra = budget_.try_reserve(length);
        if (!extra) co_return runtime::failure(extra.error());
        object_reservation_.emplace(std::move(*extra));
    }
    const auto object_end = runtime::file_position{at.value() + length.value()};
    if (auto filled = co_await fill(object_end, work); !filled)
        co_return runtime::failure(filled.error());
    co_return scan_slot{scan_slot_kind::object, at, **prefix, std::nullopt};
}

runtime::result<bytes::fragmented_buffer> scan_reader::object() {
    if (closed_ || !current_ || current_->kind != scan_slot_kind::object)
        return runtime::failure(path_error(errc::invalid_argument));
    return share(cursor_, current_->prefix->encoded_bytes());
}

runtime::result<void> scan_reader::advance() {
    if (closed_ || !current_ || current_->kind != scan_slot_kind::object)
        return runtime::failure(path_error(errc::invalid_argument));
    const auto length = current_->prefix->encoded_bytes();
    cursor_ = runtime::file_position{cursor_.value() + length.value()};
    if (!buffered_.trim_front(
          byte_count{cursor_.value() - buffered_begin_.value()}))
        return runtime::failure(path_error(errc::invariant_violation));
    buffered_begin_ = cursor_;
    current_.reset();
    object_reservation_.reset();
    return {};
}

seastar::future<> scan_reader::close() noexcept {
    closed_ = true;
    current_.reset();
    while (count_ != 0) {
        auto pending = std::move(*reads_[head_]);
        reads_[head_].reset();
        head_ = (head_ + 1) % read_slots;
        --count_;
        try {
            static_cast<void>(co_await std::move(pending.read));
        } catch (...) {
        }
    }
    buffered_ = bytes::fragmented_buffer{};
    object_reservation_.reset();
}

} // namespace kwaque::storage
