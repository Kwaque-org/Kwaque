#include "src/storage/segment_scan.h"

#include "src/base/invariant.h"
#include "src/codec/transaction.h"
#include "src/storage/page_internal.h"

#include <algorithm>
#include <limits>

namespace kwaque::storage {
namespace {
using detail::path_error;

constexpr auto block_family = static_cast<std::uint16_t>(
  codec::format_family::segment_batch_block);
constexpr auto footer_family = static_cast<std::uint16_t>(
  codec::format_family::durable_boundary_footer);

segment_scan_stop slot_stop(scan_slot_kind kind) noexcept {
    switch (kind) {
    case scan_slot_kind::zero:
        return segment_scan_stop::unwritten;
    case scan_slot_kind::torn:
        return segment_scan_stop::torn;
    case scan_slot_kind::corrupt:
        return segment_scan_stop::corrupt;
    case scan_slot_kind::malformed:
        return segment_scan_stop::malformed;
    case scan_slot_kind::end:
    case scan_slot_kind::object:
        break;
    }
    return segment_scan_stop::end;
}

// Everything a data file of this size could hold, from the origins: the walk
// discovers its extent, so only the file bounds the bytes it may accept.
runtime::result<storage::coverage>
open_extent(const segment_history_context& history, std::uint64_t size) {
    constexpr auto top = std::numeric_limits<std::uint64_t>::max();
    const auto unit = history.alignment.bytes().value();
    const runtime::file_position aligned{size - size % unit};
    const auto logical = history.logical_origin;
    const auto physical = history.physical_origin;
    if (aligned <= history.data_start)
        return storage::coverage{
          model::range_logical_span::make(logical, logical).value(),
          model::segment_relative_span::make(physical, physical).value(),
          model::file_byte_span::make(history.data_start, history.data_start)
            .value()};
    // Physical records never outnumber logical ones.
    const auto room = std::min(top - logical.value(), top - physical.value());
    return storage::coverage{
      model::range_logical_span::make(logical, model::range_logical_end{top})
        .value(),
      model::segment_relative_span::make(
        physical, model::segment_relative_end{physical.value() + room})
        .value(),
      model::file_byte_span::make(history.data_start, aligned).value()};
}
} // namespace

namespace detail {
std::optional<segment_scan_stop>
segment_decode_stop(const codec::error& error) noexcept {
    switch (error.code()) {
    case errc::corrupt_data:
        return segment_scan_stop::corrupt;
    case errc::wrong_context:
        return segment_scan_stop::foreign;
    case errc::unsupported_format:
    case errc::resource_exhausted:
    case errc::aborted:
    case errc::invalid_argument:
    case errc::closed:
        return std::nullopt;
    default:
        return segment_scan_stop::malformed;
    }
}

runtime::operation_error
segment_short_walk(const segment_scan_report& report) noexcept {
    if (!report.complete) return path_error(errc::aborted);
    switch (report.stop) {
    case segment_scan_stop::corrupt:
        return path_error(errc::corrupt_data);
    case segment_scan_stop::malformed:
        return path_error(errc::malformed_data);
    case segment_scan_stop::foreign:
        return path_error(errc::wrong_context);
    case segment_scan_stop::end:
    case segment_scan_stop::unwritten:
    case segment_scan_stop::torn:
        break;
    }
    return path_error(errc::truncated_data);
}
} // namespace detail

runtime::result<void> segment_scan_limits::validate() const noexcept {
    if (auto valid = metadata.validate(); !valid) return valid;
    if (auto valid = reader.validate(); !valid) return valid;
    if (
      working_bytes.value() == 0 || decode_metadata_bytes.value() == 0
      || !working_bytes.checked_add(reader.maximum_object_bytes))
        return runtime::failure(path_error(errc::invalid_argument));
    return {};
}

segment_scanner::segment_scanner(
  runtime::file& data,
  workload_budget& budget,
  segment_history_context history,
  segment_scan_limits limits,
  workload_reservation working) noexcept
  : data_(data)
  , budget_(budget)
  , history_(history)
  , limits_(limits)
  , working_(std::move(working))
  , physical_(history.physical_origin) {}

segment_scanner::~segment_scanner() {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-SEGMENT-SCAN-JOINED"},
      !reader_,
      "segment scanner destroyed before close");
}

seastar::future<runtime::result<std::unique_ptr<segment_scanner>>>
segment_scanner::make(
  runtime::file& data,
  workload_budget& budget,
  segment_history_context history,
  std::optional<local_footer_reference> pin,
  segment_scan_limits limits,
  codec::cooperative_work& work,
  std::optional<runtime::file_position> end,
  extent_integrity integrity) {
    if (auto valid = limits.validate(); !valid)
        co_return runtime::failure(valid.error());
    if (
      pin
      && (pin->family() != footer_family || !pin->validate_alignment(history.alignment)))
        co_return runtime::failure(path_error(errc::invalid_argument));
    // The pinned prefix is never read again, so a resumed walk cannot hash it.
    if (
      (integrity != extent_integrity::crc32c
       && (integrity != extent_integrity::crc32c_and_digest || pin))
      || (end && (*end < history.data_start || !history.alignment.aligned(*end))))
        co_return runtime::failure(path_error(errc::invalid_argument));
    auto working = budget.try_reserve(
      byte_count{
        limits.working_bytes.value() + limits.decode_metadata_bytes.value()});
    if (!working) co_return runtime::failure(working.error());
    auto output = std::unique_ptr<segment_scanner>(
      new segment_scanner(data, budget, history, limits, std::move(*working)));
    output->end_ = end;
    auto size = co_await data.size();
    if (!size) co_return runtime::failure(size.error());
    // A bounded walk sees the file as ending at its bound, and its verifier
    // admits objects up to that bound even past the current file end: a
    // recovered seal substitutes objects there.
    const auto bound = end ? std::min(*size, end->value()) : *size;
    auto expected = open_extent(history, end ? end->value() : *size);
    if (!expected) co_return runtime::failure(expected.error());
    auto& report = output->report_;
    report.file_bytes = *size;
    report.pin = pin;
    const auto start = pin ? pin->position() : history.data_start;
    report.content_end = start;
    if (start.value() > bound) {
        // The certified footer, or the data itself, lies past the file end.
        output->stop(segment_scan_stop::torn, std::nullopt);
        if (pin) report.verdict = segment_scan_verdict::corrupt;
        co_return std::move(output);
    }
    auto reader = scan_reader::make(
      data, budget, start, runtime::file_position{bound}, limits.reader);
    if (!reader) co_return runtime::failure(reader.error());
    output->reader_ = std::move(*reader);
    // From here an unclosed scanner may not be destroyed, whatever throws.
    runtime::result<void> ready;
    std::exception_ptr thrown;
    try {
        if (pin) {
            ready = co_await output->resume(*pin, *expected, work);
        } else {
            auto made = extent_verifier::make(
              history,
              *expected,
              work.policy(),
              extent_layout_kind::initial_append,
              {},
              integrity);
            if (made)
                output->verifier_.emplace(std::move(*made));
            else
                ready = runtime::failure(path_error(made.error().code()));
        }
    } catch (...) {
        thrown = std::current_exception();
    }
    if (thrown || !ready) {
        co_await output->close();
        if (thrown) std::rethrow_exception(thrown);
        co_return runtime::failure(ready.error());
    }
    co_return std::move(output);
}

runtime::result<codec::decode_budget> segment_scanner::budget_for(
  const bytes::fragmented_buffer_parser& input,
  byte_count length,
  const codec::limits& policy) const {
    auto memory = codec::reserve_decode_input(
      input,
      policy,
      {byte_count{length.value() + limits_.working_bytes.value()},
       limits_.decode_metadata_bytes,
       limits_.metadata.charge},
      {},
      codec::input_boundary::complete);
    if (!memory) return runtime::failure(path_error(memory.error().code()));
    return *memory;
}

void segment_scanner::stop(
  segment_scan_stop kind, std::optional<codec::error> cause, bool intact) {
    stopped_ = true;
    report_.stop = kind;
    report_.cause = cause;
    report_.intact = intact;
    report_.verdict = kind == segment_scan_stop::end
                        ? segment_scan_verdict::clean
                        : segment_scan_verdict::uncertified_tail;
}

seastar::future<runtime::result<void>> segment_scanner::resume(
  local_footer_reference pin,
  storage::coverage expected,
  codec::cooperative_work& work) {
    auto slot = co_await reader_->next(work);
    if (!slot) co_return runtime::failure(slot.error());
    // Anything but the pinned footer's exact object here is damage to
    // certified bytes.
    const auto corrupt =
      [this](segment_scan_stop kind, std::optional<codec::error> cause) {
          stop(kind, cause);
          report_.verdict = segment_scan_verdict::corrupt;
      };
    if (slot->kind != scan_slot_kind::object) {
        corrupt(
          slot->kind == scan_slot_kind::end ? segment_scan_stop::torn
                                            : slot_stop(slot->kind),
          slot->cause);
        co_return runtime::result<void>{};
    }
    if (slot->prefix->family != footer_family) {
        corrupt(segment_scan_stop::foreign, std::nullopt);
        co_return runtime::result<void>{};
    }
    if (slot->prefix->encoded_bytes() != pin.bytes()) {
        corrupt(segment_scan_stop::malformed, std::nullopt);
        co_return runtime::result<void>{};
    }
    auto bytes = reader_->object();
    if (!bytes) co_return runtime::failure(bytes.error());
    bytes::fragmented_buffer_parser input{bytes->share()};
    auto memory = budget_for(input, pin.bytes(), work.policy());
    if (!memory) co_return runtime::failure(memory.error());
    auto resumed = co_await extent_verifier::resume(
      {history_, pin.position()},
      pin.digest(),
      expected,
      std::move(*bytes),
      *memory,
      work,
      {.origin = pin.position().value()});
    if (!resumed) {
        const auto kind = detail::segment_decode_stop(resumed.error());
        if (!kind)
            co_return runtime::failure(path_error(resumed.error().code()));
        corrupt(*kind, resumed.error());
        co_return runtime::result<void>{};
    }
    report_.boundary = segment_scan_boundary{pin, resumed->footer.boundary()};
    physical_ = resumed->footer.boundary().coverage.physical().end();
    verifier_.emplace(std::move(resumed->verifier));
    report_.content_end = runtime::file_position{
      pin.position().value() + pin.bytes().value()};
    pending_advance_ = true;
    co_return runtime::result<void>{};
}

seastar::future<runtime::result<std::optional<segment_scanned_object>>>
segment_scanner::next(codec::cooperative_work& work) {
    using output = std::optional<segment_scanned_object>;
    if (closed_) co_return runtime::failure(path_error(errc::closed));
    // The borrowed object dies before the reader releases or refills the
    // windows that hold its bytes.
    block_.reset();
    if (pending_advance_) {
        pending_advance_ = false;
        if (auto moved = reader_->advance(); !moved)
            co_return runtime::failure(moved.error());
    }
    if (stopped_) co_return output{};
    auto slot = co_await reader_->next(work);
    if (!slot) co_return runtime::failure(slot.error());
    if (slot->kind != scan_slot_kind::object) {
        stop(slot_stop(slot->kind), slot->cause);
        co_return output{};
    }
    if (slot->prefix->family == block_family)
        co_return co_await take_block(*slot, work);
    if (slot->prefix->family == footer_family)
        co_return co_await take_footer(*slot, work);
    stop(
      segment_scan_stop::foreign,
      codec::error{
        errc::wrong_context, slot->prefix->family, 0, slot->position.value()},
      true);
    co_return output{};
}

seastar::future<runtime::result<std::optional<segment_scanned_object>>>
segment_scanner::take_block(scan_slot slot, codec::cooperative_work& work) {
    using output = std::optional<segment_scanned_object>;
    const auto length = slot.prefix->encoded_bytes();
    // Only a checksum failure while decoding is damage; anything the decoded
    // object fails is an intact object the prefix rejects.
    const auto damage = [this](
                          const codec::error& error,
                          bool decoded = false) -> runtime::result<output> {
        const auto kind = detail::segment_decode_stop(error);
        if (!kind) return runtime::failure(path_error(error.code()));
        stop(*kind, error, decoded || *kind != segment_scan_stop::corrupt);
        return output{};
    };
    auto location = segment_write_context::make(
      history_.segment, history_.alignment, physical_, slot.position);
    if (!location) co_return damage(codec::error{errc::malformed_data});
    auto bytes = reader_->object();
    if (!bytes) co_return runtime::failure(bytes.error());
    bytes::fragmented_buffer_parser input{std::move(*bytes)};
    auto memory = budget_for(input, length, work.policy());
    if (!memory) co_return runtime::failure(memory.error());
    const model::batch_decode_expectation batch{
      history_.segment.topic(), history_.segment.range()};
    const codec::field_context context{.origin = slot.position.value()};
    auto decoded = co_await decode_segment_block(
      input,
      {*location, history_.data_start, batch, history_.profile},
      *memory,
      work,
      context,
      codec::input_boundary::complete);
    if (!decoded) co_return damage(decoded.error());
    // The verifier reuses the codec's validation and extends the prefix.
    auto added = co_await verifier_->add_block(
      decoded->value, batch, decoded->remaining, work, context);
    if (!added) co_return damage(added.error(), true);
    physical_ = decoded->value.descriptor().coverage().physical().end();
    ++report_.suffix_blocks;
    report_.content_end = runtime::file_position{
      slot.position.value() + length.value()};
    block_.emplace(std::move(decoded->value));
    pending_advance_ = true;
    co_return output{segment_scanned_object{&*block_, nullptr}};
}

seastar::future<runtime::result<std::optional<segment_scanned_object>>>
segment_scanner::take_footer(scan_slot slot, codec::cooperative_work& work) {
    using output = std::optional<segment_scanned_object>;
    const auto length = slot.prefix->encoded_bytes();
    const auto damage = [this](
                          const codec::error& error,
                          bool decoded = false) -> runtime::result<output> {
        const auto kind = detail::segment_decode_stop(error);
        if (!kind) return runtime::failure(path_error(error.code()));
        stop(*kind, error, decoded || *kind != segment_scan_stop::corrupt);
        return output{};
    };
    auto bytes = reader_->object();
    if (!bytes) co_return runtime::failure(bytes.error());
    const codec::field_context context{.origin = slot.position.value()};
    std::optional<codec::decode_budget> memory;
    std::optional<codec::result<durable_footer>> footer;
    {
        bytes::fragmented_buffer_parser input{bytes->share()};
        auto admitted = budget_for(input, length, work.policy());
        if (!admitted) co_return runtime::failure(admitted.error());
        memory = *admitted;
        footer.emplace(
          co_await decode_durable_footer(
            input,
            {history_, slot.position},
            *memory,
            work,
            context,
            codec::input_boundary::complete));
        if (*footer && !input.at_end())
            co_return damage(codec::error{errc::malformed_data});
    }
    if (!*footer) co_return damage(footer->error());
    // Only a footer that names exactly the prefix scanned so far verifies its
    // covered history; any other is damage, not a boundary.
    auto prefix = verifier_->checkpoint(work, context);
    if (!prefix) co_return runtime::failure(path_error(prefix.error().code()));
    if (
      auto valid = validate_durable_footer(**footer, *prefix, context); !valid)
        co_return damage(valid.error(), true);
    auto digest = co_await detail::hash_exact(*bytes, work, context);
    if (!digest) co_return runtime::failure(path_error(digest.error().code()));
    auto reference = local_footer_reference::make(
      slot.position, length, footer_family, *digest);
    if (!reference)
        co_return runtime::failure(path_error(errc::invariant_violation));
    auto added = co_await verifier_->add_footer(
      std::move(*bytes), *memory, work, std::nullopt, context);
    if (!added) co_return damage(added.error(), true);
    report_.boundary = segment_scan_boundary{*reference, (*footer)->boundary()};
    report_.suffix_blocks = 0;
    report_.content_end = runtime::file_position{
      slot.position.value() + length.value()};
    pending_advance_ = true;
    co_return output{segment_scanned_object{nullptr, &*report_.boundary}};
}

runtime::result<verified_extent>
segment_scanner::finish(codec::cooperative_work& work) {
    if (
      closed_ || !stopped_ || report_.stop != segment_scan_stop::end
      || !verifier_)
        return runtime::failure(path_error(errc::invalid_argument));
    block_.reset();
    auto finished = verifier_->finish_prefix(work);
    verifier_.reset();
    if (!finished) return runtime::failure(path_error(finished.error().code()));
    return std::move(*finished);
}

runtime::result<verified_extent>
segment_scanner::prefix(codec::cooperative_work& work) {
    if (closed_ || !verifier_ || verifier_->closed())
        return runtime::failure(path_error(errc::invalid_argument));
    auto evidence = verifier_->checkpoint(work);
    if (!evidence) return runtime::failure(path_error(evidence.error().code()));
    return std::move(*evidence);
}

runtime::result<void> segment_scanner::substitutable() const noexcept {
    // Certified bytes are never replaced, and only a slot the walk could not
    // read takes a reconstructed object.
    if (
      closed_ || !stopped_ || !verifier_ || verifier_->closed()
      || report_.verdict == segment_scan_verdict::corrupt)
        return runtime::failure(path_error(errc::invalid_argument));
    return {};
}

seastar::future<runtime::result<void>>
segment_scanner::restart(runtime::file_position at) {
    if (reader_) {
        co_await reader_->close();
        reader_.reset();
    }
    // Stopped until a reader is installed, so a failed restart reads nothing.
    stopped_ = true;
    pending_advance_ = false;
    report_.stop = segment_scan_stop::end;
    report_.cause.reset();
    report_.intact = false;
    report_.verdict = segment_scan_verdict::clean;
    report_.content_end = at;
    auto size = co_await data_.size();
    if (!size) co_return runtime::failure(size.error());
    report_.file_bytes = *size;
    const auto bound = end_ ? std::min(*size, end_->value()) : *size;
    // Nothing of the file remains after the object.
    if (at.value() >= bound) {
        stop(segment_scan_stop::end, std::nullopt);
        co_return runtime::result<void>{};
    }
    auto reader = scan_reader::make(
      data_, budget_, at, runtime::file_position{bound}, limits_.reader);
    if (!reader) co_return runtime::failure(reader.error());
    reader_ = std::move(*reader);
    stopped_ = false;
    co_return runtime::result<void>{};
}

seastar::future<runtime::result<segment_block>> segment_scanner::substitute(
  segment_block block, codec::cooperative_work& work) {
    if (auto valid = substitutable(); !valid)
        co_return runtime::failure(valid.error());
    block_.reset();
    const auto at = report_.content_end;
    const auto covered = block.descriptor().coverage();
    if (
      covered.bytes().begin() != at || covered.physical().begin() != physical_)
        co_return runtime::failure(path_error(errc::wrong_context));
    const model::batch_decode_expectation batch{
      history_.segment.topic(), history_.segment.range()};
    auto added = co_await verifier_->add_block(
      block,
      batch,
      {limits_.working_bytes,
       limits_.decode_metadata_bytes,
       limits_.metadata.charge},
      work,
      {.origin = at.value()});
    if (!added) co_return runtime::failure(path_error(added.error().code()));
    physical_ = covered.physical().end();
    ++report_.suffix_blocks;
    if (auto restarted = co_await restart(covered.bytes().end()); !restarted)
        co_return runtime::failure(restarted.error());
    co_return std::move(block);
}

seastar::future<runtime::result<encoded_durable_footer>>
segment_scanner::substitute(
  encoded_durable_footer footer, codec::cooperative_work& work) {
    if (auto valid = substitutable(); !valid)
        co_return runtime::failure(valid.error());
    block_.reset();
    const auto at = report_.content_end;
    const auto location = footer.descriptor().location();
    if (location.history != history_ || location.position != at)
        co_return runtime::failure(path_error(errc::wrong_context));
    const codec::field_context context{.origin = at.value()};
    auto prefix = verifier_->checkpoint(work, context);
    if (!prefix) co_return runtime::failure(path_error(prefix.error().code()));
    if (
      auto valid = validate_durable_footer(
        footer.descriptor(), *prefix, context);
      !valid)
        co_return runtime::failure(path_error(valid.error().code()));
    const auto length = footer.bytes().size();
    auto digest = co_await detail::hash_exact(footer.bytes(), work, context);
    if (!digest) co_return runtime::failure(path_error(digest.error().code()));
    auto reference = local_footer_reference::make(
      at, length, footer_family, *digest);
    if (!reference)
        co_return runtime::failure(path_error(errc::invariant_violation));
    auto added = co_await verifier_->add_footer(
      footer,
      {limits_.working_bytes,
       limits_.decode_metadata_bytes,
       limits_.metadata.charge},
      work,
      *prefix,
      context);
    if (!added) co_return runtime::failure(path_error(added.error().code()));
    report_.boundary = segment_scan_boundary{
      *reference, footer.descriptor().boundary()};
    report_.suffix_blocks = 0;
    if (
      auto restarted = co_await restart(
        runtime::file_position{at.value() + length.value()});
      !restarted)
        co_return runtime::failure(restarted.error());
    co_return std::move(footer);
}

seastar::future<> segment_scanner::close() noexcept {
    closed_ = true;
    block_.reset();
    if (verifier_) {
        verifier_->close();
        verifier_.reset();
    }
    if (reader_) {
        co_await reader_->close();
        reader_.reset();
    }
}

} // namespace kwaque::storage
