#include "src/storage/segment_run.h"

#include "src/base/allocation.h"
#include "src/base/invariant.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/envelope.h"
#include "src/codec/envelope_integrity.h"
#include "src/codec/error.h"
#include "src/codec/format_registry.h"
#include "src/codec/transaction.h"
#include "src/runtime/file_error.h"
#include "src/storage/format_context.h"
#include "src/storage/segment_format.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <algorithm>
#include <array>
#include <limits>
#include <new>
#include <optional>
#include <system_error>
#include <utility>

namespace kwaque::storage {
namespace {
runtime::operation_error run_error(errc code) noexcept {
    return runtime::operation_error{code, runtime::operation_kind::file};
}
// Why the buffer refused a share of itself.
errc share_code(std::error_code source) noexcept {
    for (const auto code :
         {errc::resource_exhausted, errc::out_of_range, errc::invalid_argument})
        if (source == code) return code;
    return errc::invariant_violation;
}
constexpr auto block_family = static_cast<std::uint16_t>(
  codec::format_family::segment_batch_block);
constexpr auto footer_family = static_cast<std::uint16_t>(
  codec::format_family::durable_boundary_footer);

// What a failed check of one object means for the run. A failure that says
// nothing about the bytes is itself. A checksum that does not match is
// damage wherever it is. An object that is whole and is not the one named
// says, at the run's first object, that what led there was wrong: only an
// object whose own checksums hold can say that. Anything else the bytes fail
// is the data's, at the first object as at any other.
runtime::operation_error object_error(errc code, bool first) noexcept {
    switch (code) {
    case errc::aborted:
    case errc::closed:
    case errc::queue_full:
    case errc::resource_exhausted:
    case errc::invariant_violation:
    case errc::corrupt_data:
    case errc::unsupported_format:
        return run_error(code);
    case errc::wrong_context:
        return run_error(first ? errc::wrong_context : errc::malformed_data);
    default:
        return run_error(errc::malformed_data);
    }
}
// Whether a fed fragment is shared rather than copied: its bytes fill at
// least three quarters of the allocation it keeps alive, so that what a run
// keeps is never more than 4/3 of what it presents. A sparse one, a small
// batch in a large frame, is copied.
bool dense(const bytes::fragmented_buffer& bytes, std::size_t at) noexcept {
    const auto presented = bytes.fragment_at(at);
    const auto backing = bytes.fragment_retained_bytes(at);
    return presented && backing
           && 4 * std::uint64_t{presented->size()} >= 3 * backing->value();
}
// `bytes`, sharing each stretch of dense fragments and copying each stretch
// of the others into allocations of its own. Nothing when it cannot be made.
std::optional<bytes::fragmented_buffer>
shared_or_copied(bytes::fragmented_buffer bytes) {
    if (bytes.empty()) return bytes;
    bytes::fragmented_buffer_builder joined{
      {.initial_fragment_bytes = byte_count{1},
       .max_fragment_bytes = byte_count{1},
       .max_total_bytes = bytes.size(),
       .max_retained_bytes = bytes::max_builder_total_bytes}};
    while (!bytes.empty()) {
        std::size_t count = 0;
        while (count < bytes.fragment_count() && dense(bytes, count))
            ++count;
        if (count != 0) {
            if (!joined.append_fragments(bytes, item_count{count}))
                return std::nullopt;
            continue;
        }
        std::uint64_t length = 0;
        while (count < bytes.fragment_count() && !dense(bytes, count))
            length += bytes.fragment_at(count++)->size();
        auto stretch = bytes.share(byte_count{0}, byte_count{length});
        if (!stretch) return std::nullopt;
        auto copied = stretch->copy();
        if (
          !bytes.trim_front(byte_count{length})
          || !joined.append_fragments(
            copied, item_count{copied.fragment_count()}))
            return std::nullopt;
    }
    auto made = joined.finish();
    if (!made) return std::nullopt;
    return std::move(*made);
}
// Whether a read failed because its device did: an I/O error, a read-only
// filesystem, a device that is gone.
bool device_failed(const runtime::operation_error& why) noexcept {
    const auto detail = runtime::file_detail(why);
    if (!detail) return false;
    switch (*detail) {
    case runtime::file_failure_detail::device_io:
    case runtime::file_failure_detail::read_only:
    case runtime::file_failure_detail::device_gone:
        return true;
    case runtime::file_failure_detail::unknown:
    case runtime::file_failure_detail::no_space:
    case runtime::file_failure_detail::descriptor_limit:
    case runtime::file_failure_detail::permission:
    case runtime::file_failure_detail::admission_not_dispatched:
    case runtime::file_failure_detail::quota:
        return false;
    }
    return false;
}
// Whether the cache answered that it does not keep a run: there was no room
// for it, or it is more than the cache keeps in one value.
bool unkept(errc code) noexcept {
    return code == errc::queue_full || code == errc::out_of_range;
}
// A failed extent check on these fields leaves the header's length usable,
// so the header's checksum still decides between damage and a limit.
bool body_extent_field(const codec::error& error) noexcept {
    return error.field()
             == static_cast<std::uint16_t>(codec::envelope_field::body_bytes)
           || error.field()
                == static_cast<std::uint16_t>(
                  codec::envelope_field::encoded_bytes);
}
} // namespace

runtime::result<void> segment_run_limits::validate() const noexcept {
    if (
      charge == nullptr || run_bytes.value() == 0 || working_bytes.value() == 0
      || metadata_bytes.value() == 0
      || run_bytes.value() > std::numeric_limits<std::uint32_t>::max())
        return runtime::failure(run_error(errc::invalid_argument));
    return {};
}

std::optional<segment_run_start> segment_run_at(
  const segment_history_context& history,
  const sparse_index_entry& anchor) noexcept {
    const auto base = anchor.logical_anchor().value();
    const auto origin = history.logical_origin.value();
    if (base < origin) return std::nullopt;
    const auto physical = history.physical_origin.checked_add(
      model::segment_record_count{base - origin});
    if (!physical) return std::nullopt;
    return segment_run_start{
      anchor.block_position(), anchor.logical_anchor(), *physical};
}

std::size_t
segment_run::seek(model::range_logical_offset target) const noexcept {
    const auto found = std::ranges::partition_point(
      blocks_, [target](const segment_run_block& block) {
          return block.logical.end().value() <= target.value();
      });
    return static_cast<std::size_t>(found - blocks_.begin());
}

segment_run_start segment_run::at(std::size_t at) const noexcept {
    const auto& block = blocks_[at];
    return segment_run_start{
      runtime::file_position{begin_.value() + block.offset},
      // A block's first record has an offset, so its begin is one.
      model::range_logical_offset::make(block.logical.begin().value()).value(),
      block.physical.begin()};
}

std::optional<segment_run_start>
segment_run::after(std::size_t at) const noexcept {
    if (at + 1 < blocks_.size()) return this->at(at + 1);
    const auto& last = blocks_[at];
    const auto base = model::range_logical_offset::make(
      last.logical.end().value());
    if (!base) return std::nullopt;
    return segment_run_start{end(), *base, last.physical.end()};
}

runtime::result<bytes::fragmented_buffer>
segment_run::batch(std::size_t at) const {
    const auto& block = blocks_[at];
    const auto& held = parts_[block.part];
    auto shared = held.bytes.share(
      byte_count{block.offset - held.offset + block.batch_offset},
      byte_count{block.batch_bytes});
    if (!shared) return runtime::failure(run_error(share_code(shared.error())));
    return std::move(*shared);
}

runtime::result<void> segment_run_builder::price() noexcept {
    std::uint64_t total = buffers_;
    for (const auto bytes :
         {run_.blocks_.capacity() * sizeof(segment_run_block),
          run_.parts_.capacity() * sizeof(segment_run::part)}) {
        if (bytes == 0) continue;
        const auto charged = budget_->allocation_charge(byte_count{bytes});
        if (!charged) return runtime::failure(charged.error());
        total += charged->value();
    }
    run_.charge_ = total;
    return {};
}

runtime::result<void>
segment_run_builder::part(bytes::fragmented_buffer bytes) {
    auto& parts = run_.parts_;
    if (parts.size() == maximum_run_parts)
        return runtime::failure(run_error(errc::out_of_range));
    const auto cost = budget_->buffer_charge(bytes);
    if (!cost) return runtime::failure(cost.error());
    std::uint64_t offset = 0;
    if (!parts.empty())
        offset = parts.back().offset + parts.back().bytes.size().value();
    try {
        if (parts.size() == parts.capacity())
            parts.reserve(
              std::min<std::size_t>(
                maximum_run_parts,
                std::max<std::size_t>(1, 2 * parts.capacity())));
    } catch (const std::bad_alloc&) {
        return runtime::failure(run_error(errc::resource_exhausted));
    }
    const auto size = bytes.size().value();
    parts.push_back(segment_run::part{offset, std::move(bytes)});
    buffers_ += cost->value();
    run_.size_ = offset + size;
    return price();
}

runtime::result<void> segment_run_builder::block(segment_run_block described) {
    auto& blocks = run_.blocks_;
    if (run_.parts_.empty() || blocks.size() == maximum_run_blocks)
        return runtime::failure(run_error(errc::out_of_range));
    try {
        if (blocks.size() == blocks.capacity())
            blocks.reserve(
              std::min<std::size_t>(
                maximum_run_blocks,
                std::max<std::size_t>(4, 2 * blocks.capacity())));
    } catch (const std::bad_alloc&) {
        return runtime::failure(run_error(errc::resource_exhausted));
    }
    described.part = static_cast<std::uint32_t>(run_.parts_.size() - 1);
    blocks.push_back(described);
    return price();
}

seastar::future<runtime::result<checked_segment_run>> check_segment_run(
  bytes::fragmented_buffer bytes,
  segment_history_context history,
  segment_run_start start,
  bool whole,
  const segment_run_limits& limits,
  const workload_budget& budget,
  codec::cooperative_work& work) {
    using output = runtime::result<checked_segment_run>;
    // Blocks are packed and only a footer pads, to its own end: a run starts
    // at any block and its objects have any length.
    const auto total = bytes.size().value();
    if (total == 0 || start.position < history.data_start)
        co_return output{runtime::failure(run_error(errc::invalid_argument))};
    // Every alias of the bytes is priced against what the bytes cost whole.
    const auto backing = budget.buffer_charge(bytes);
    if (!backing) co_return output{runtime::failure(backing.error())};
    const auto config = work.policy().config();
    const byte_count ceiling{
      config.max_encoded_body_bytes.value() + config.max_header_bytes.value()};
    const codec::decode_budget allowance{
      byte_count{backing->value() + limits.working_bytes.value()},
      limits.metadata_bytes,
      limits.charge};
    const model::batch_decode_expectation batch{
      history.segment.topic(), history.segment.range()};

    std::vector<segment_run_block> blocks;
    std::optional<byte_count> cut;
    std::uint64_t offset = 0;
    auto logical = start.base.value();
    auto physical = start.physical;
    while (offset < total) {
        const bool first = blocks.empty();
        const auto remaining = total - offset;
        const runtime::file_position at{start.position.value() + offset};
        const codec::field_context context{.origin = at.value()};
        const auto failed = [first](errc code) {
            return output{runtime::failure(object_error(code, first))};
        };
        if (auto ready = co_await work.checkpoint(); !ready)
            co_return failed(ready.error().code());
        codec::encoded_envelope_prefix raw{};
        std::optional<codec::result<codec::unverified_envelope_prefix>> prefix;
        try {
            auto head = bytes.share(
              byte_count{offset}, byte_count{codec::envelope_prefix_bytes});
            if (!head) co_return failed(share_code(head.error()));
            const bytes::fragmented_buffer_parser peek{std::move(*head)};
            if (!peek.peek_to(raw)) co_return failed(errc::malformed_data);
            prefix.emplace(
              codec::peek_envelope_prefix(
                peek,
                work.policy(),
                {config.max_encoded_body_bytes, ceiling},
                context,
                codec::input_boundary::complete));
        } catch (const std::bad_alloc&) {
            co_return failed(errc::resource_exhausted);
        }
        // Zeroes below a durable position are bytes nobody wrote there.
        if (std::ranges::all_of(raw, [](char byte) { return byte == 0; }))
            co_return failed(errc::malformed_data);
        if (!*prefix && !body_extent_field(prefix->error()))
            co_return failed(errc::malformed_data);
        // What the prefix alone says of the object. A block is checked
        // whole below, header first. Anything else is believed, or held
        // against the data, only once the header's own checksum holds: its
        // extents passed, so its length is usable even when a body extent
        // was refused.
        std::optional<errc> refusal;
        std::uint64_t length = 0;
        bool footer = false;
        if (!*prefix) {
            refusal = prefix->error().code();
        } else {
            length = (*prefix)->encoded_bytes().value();
            if (length == 0) {
                refusal = errc::malformed_data;
            } else if (length > remaining) {
                if (!whole) {
                    cut.emplace(length);
                    break;
                }
                refusal = errc::malformed_data;
            } else if ((*prefix)->family == footer_family) {
                footer = true;
            } else if ((*prefix)->family != block_family) {
                // Whole, and not a block.
                refusal = errc::wrong_context;
            }
        }
        if (refusal || footer) {
            const auto header_bytes = static_cast<std::uint16_t>(
              static_cast<std::uint8_t>(raw[10])
              | (static_cast<std::uint16_t>(static_cast<std::uint8_t>(raw[11])) << 8U));
            if (header_bytes > remaining)
                co_return failed(errc::malformed_data);
            std::optional<codec::result<void>> verified;
            try {
                auto header = bytes.share(
                  byte_count{offset}, byte_count{header_bytes});
                if (!header) co_return failed(share_code(header.error()));
                verified.emplace(
                  co_await codec::verify_envelope_header_crc(
                    *header, work, context));
            } catch (const std::bad_alloc&) {
                co_return failed(errc::resource_exhausted);
            }
            if (!*verified) co_return failed(verified->error().code());
            // A size its checksum proves and only the limits refuse is a
            // limit, never damage; a length or a kind it proves is what
            // was written there.
            if (refusal) co_return failed(*refusal);
            // A run the cache keeps begins with a block: a footer where a
            // block was named is not what was named. Anywhere else it is
            // passed over.
            if (first && whole) co_return failed(errc::wrong_context);
            offset += length;
            continue;
        }
        if (blocks.size() == maximum_run_blocks) {
            if (whole)
                co_return output{
                  runtime::failure(run_error(errc::out_of_range))};
            break;
        }
        // Where the start names it, or where the block before it ends: a
        // place no block of this segment can lie says the same of both.
        const auto location = segment_write_context::make(
          history.segment, history.alignment, physical, at);
        if (!location) co_return failed(errc::wrong_context);
        std::optional<codec::result<checked_segment_block>> checked;
        try {
            auto object = bytes.share(byte_count{offset}, byte_count{length});
            if (!object) co_return failed(share_code(object.error()));
            bytes::fragmented_buffer_parser input{std::move(*object)};
            const auto memory = codec::reserve_decode_input(
              input,
              work.policy(),
              allowance,
              context,
              codec::input_boundary::complete);
            if (!memory) co_return failed(memory.error().code());
            checked.emplace(
              co_await check_segment_block(
                input,
                {*location, history.data_start, batch, history.profile},
                *memory,
                work,
                context,
                codec::input_boundary::complete));
            if (blocks.size() == blocks.capacity())
                blocks.reserve(
                  std::min<std::size_t>(
                    maximum_run_blocks,
                    std::max<std::size_t>(4, 2 * blocks.capacity())));
        } catch (const std::bad_alloc&) {
            co_return failed(errc::resource_exhausted);
        }
        if (!*checked) co_return failed(checked->error().code());
        const auto covered = (*checked)->coverage;
        const auto begins = covered.logical().begin().value();
        // The first block is the one the start names. A later one begins at
        // or after the end of the one before it. Its checksums held: it is
        // whole, and it is another block.
        if (first ? begins != logical : begins < logical)
            co_return failed(errc::wrong_context);
        blocks.push_back(
          segment_run_block{
            .logical = covered.logical(),
            .physical = covered.physical(),
            .offset = offset,
            .bytes = static_cast<std::uint32_t>(length),
            .batch_offset = static_cast<std::uint32_t>(
              (*checked)->child_offset.value()),
            .batch_bytes = static_cast<std::uint32_t>(
              (*checked)->child_bytes.value()),
            .part = 0});
        logical = covered.logical().end().value();
        physical = covered.physical().end();
        offset += length;
    }
    if (auto ready = work.poll(); !ready)
        co_return output{runtime::failure(run_error(ready.error().code()))};
    segment_run_builder builder{start.position, budget};
    if (auto added = builder.part(std::move(bytes)); !added)
        co_return output{runtime::failure(added.error())};
    for (const auto& block : blocks)
        if (auto added = builder.block(block); !added)
            co_return output{runtime::failure(added.error())};
    builder.end_at(offset);
    co_return output{checked_segment_run{std::move(builder).finish(), cut}};
}

runtime::result<segment_data_source> segment_data_source::share() const {
    if (const auto* sealed = std::get_if<local_generation_pin>(&pin_)) {
        auto shared = sealed->share();
        if (!shared) return runtime::failure(shared.error());
        return segment_data_source{std::move(*shared)};
    }
    return segment_data_source{std::get<segment_data_pin>(pin_).share()};
}

seastar::future<runtime::result<bytes::fragmented_buffer>>
segment_data_source::read(
  runtime::file_position position,
  byte_count length,
  runtime::file_position bound) const {
    if (const auto* sealed = std::get_if<local_generation_pin>(&pin_))
        return sealed->read_data(position, length, bound);
    return std::get<segment_data_pin>(pin_).read(position, length, bound);
}

runtime::result<std::unique_ptr<segment_runs>> segment_runs::make(
  page_cache& cache,
  workload_budget& budget,
  segment_history_context history,
  segment_runs_config config) {
    if (auto valid = config.limits.validate(); !valid)
        return runtime::failure(valid.error());
    // A stretch longer than one read is read in parts of that many bytes,
    // and each part is whole objects.
    if (
      config.feed_bytes.value() == 0 || history.alignment.bytes().value() == 0
      || config.limits.run_bytes.value() % history.alignment.bytes().value()
           != 0)
        return runtime::failure(run_error(errc::invalid_argument));
    try {
        std::unique_ptr<segment_runs> made{
          new segment_runs{cache, budget, history, std::move(config)}};
        // Sized once, so that the writer's path never allocates for them.
        made->waiting_.reserve(maximum_fed_groups);
        made->pending_.reserve(maximum_fed_runs);
        made->copier_.emplace(
          seastar::with_scheduling_group(
            budget.scheduling_group(),
            [runs = made.get()] { return runs->copy(); }));
        return made;
    } catch (const std::bad_alloc&) {
        return runtime::failure(run_error(errc::resource_exhausted));
    }
}

segment_runs::segment_runs(
  page_cache& cache,
  workload_budget& budget,
  segment_history_context history,
  segment_runs_config config) noexcept
  : cache_(cache)
  , budget_(budget)
  , history_(history)
  , config_(std::move(config)) {}

segment_runs::~segment_runs() {
    assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-SEGMENT-RUNS-CLOSED"},
      closed_ || !copier_,
      "segment runs destroyed before joined close");
}

runtime::result<seastar::gate::holder> segment_runs::enter() noexcept {
    assert_current();
    auto held = loads_.try_hold();
    if (closing_ || !held) return runtime::failure(run_error(errc::closed));
    return std::move(*held);
}

void segment_runs::touch() noexcept {
    assert_current();
    if (closing_ || sealed_ || !config_.now) return;
    read_ = config_.now();
    wanted_ = true;
}

void segment_runs::read_failed(const runtime::operation_error& why) noexcept {
    assert_current();
    if (!config_.failed) return;
    // A checksum that fails, or bytes that are not what they say, where a
    // barrier made them durable.
    const bool damage = why.code() == errc::corrupt_data
                        || why.code() == errc::malformed_data;
    const bool device = !damage && device_failed(why);
    if (!damage && !device) return;
    auto& told = damage ? damage_told_ : device_told_;
    if (told) return;
    told = true;
    config_.failed(why);
}

void segment_runs::establish(std::uint32_t readers) noexcept {
    assert_current();
    established_ = readers;
    if (closing_ || sealed_) return;
    if (readers != 0) {
        wanted_ = true;
    } else if (!config_.now && wanted_) {
        // What is written from here on is not kept, so the run that was
        // filling will never be whole.
        wanted_ = false;
        gap_ = true;
        lapsed_ = true;
        stir();
    }
}

void segment_runs::stir() noexcept {
    stirring_ = true;
    wake_.signal();
}

seastar::future<> segment_runs::settled() {
    assert_current();
    while (!closing_ && (stirring_ || !waiting_.empty()))
        co_await idle_.when();
}

std::optional<segment_tail>
segment_runs::tail(model::range_logical_offset target) noexcept {
    assert_current();
    for (auto at = pending_.size(); at-- != 0;) {
        auto& run = *pending_[at];
        if (target < run.base) continue;
        return segment_tail{&run.building.run(), run.load.hold(), readable_};
    }
    return std::nullopt;
}

runtime::result<byte_count>
segment_runs::estimate(std::uint64_t length) const noexcept {
    // A read returns one buffer for each contiguous allocation's worth, and a
    // copy is made the same way. Their bookkeeping and the run's own tables
    // are small beside them; the run is charged exactly once it is there.
    const auto extra = budget_.allocation_charge(byte_count{1_KiB});
    if (!extra) return runtime::failure(extra.error());
    auto total = extra->value();
    if (const auto chunks = length / maximum_contiguous_allocation_bytes) {
        // A charge profile that cannot price a whole one refuses here, as
        // it would refuse the buffer.
        const auto whole = budget_.allocation_charge(
          byte_count{maximum_contiguous_allocation_bytes});
        if (!whole) return runtime::failure(whole.error());
        if (
          chunks > (std::numeric_limits<std::uint64_t>::max() - total)
                     / whole->value())
            return runtime::failure(run_error(errc::out_of_range));
        total += chunks * whole->value();
    }
    if (const auto rest = length % maximum_contiguous_allocation_bytes) {
        const auto last = budget_.allocation_charge(byte_count{rest});
        if (!last) return runtime::failure(last.error());
        total += last->value();
    }
    return byte_count{total};
}

seastar::future<runtime::result<segment_run>> segment_runs::load(
  const segment_data_source& source,
  segment_run_start start,
  runtime::file_position end,
  runtime::file_position bound,
  seastar::abort_source& stop) {
    using output = runtime::result<segment_run>;
    // Whoever asked for this was let in before any drain began, and holds
    // the drain's gate: the read ends, and the drain waits for it. A drain
    // turns readers away where they come in, not here, where a load the
    // cache has already started and counted would be given up half way.
    const auto& limits = config_.limits;
    codec::cooperative_work work{limits.policy, stop};
    // What the check takes beyond the run, for as long as it runs.
    const auto working = budget_.try_reserve(
      byte_count{limits.working_bytes.value() + limits.metadata_bytes.value()});
    if (!working) co_return output{runtime::failure(working.error())};
    device_read(start.position);
    auto read = co_await source.read(
      start.position, byte_count{end.value() - start.position.value()}, bound);
    if (!read) co_return output{runtime::failure(read.error())};
    auto checked = co_await check_segment_run(
      std::move(*read), history_, start, true, limits, budget_, work);
    if (!checked) co_return output{runtime::failure(checked.error())};
    co_return output{std::move(checked->run)};
}

seastar::future<runtime::result<cache_ref<segment_run>>> segment_runs::fetch(
  segment_run_start start,
  runtime::file_position end,
  const segment_data_source& source,
  runtime::file_position bound,
  seastar::abort_source& caller,
  std::uint32_t readers,
  workload_budget* lender) {
    using output = runtime::result<cache_ref<segment_run>>;
    assert_current();
    if (closing_) co_return output{runtime::failure(run_error(errc::closed))};
    if (auto hit = runs_.get(start.base)) co_return output{std::move(hit)};
    if (end <= start.position || bound < end)
        co_return output{runtime::failure(run_error(errc::out_of_range))};
    const auto length = end.value() - start.position.value();
    if (length > config_.limits.run_bytes.value())
        co_return output{runtime::failure(run_error(errc::out_of_range))};
    // A run the writer is feeding is not waited for: it answers when it is
    // durable, which may be never for a reader whose bound is already taken.
    for (const auto& fed : pending_)
        if (fed->base == start.base)
            co_return output{runtime::failure(run_error(errc::queue_full))};
    // The load in flight takes one more: nothing of the source is needed
    // to wait for it.
    if (runs_.loading(start.base))
        co_return co_await runs_.join(start.base, caller);
    const auto admitted = estimate(enclosing(start.position, length));
    if (!admitted) co_return output{runtime::failure(admitted.error())};
    // What reads the run owns its own right to the source: the read may
    // still be running when its caller has left.
    const auto loader = [this, start, end, bound](segment_data_source from) {
        return [this,
                source = std::move(from),
                start,
                end,
                bound,
                joined = loads_.hold()](seastar::abort_source& stop) {
            return load(source, start, end, bound, stop);
        };
    };
    auto shared = source.share();
    if (!shared) co_return output{runtime::failure(shared.error())};
    auto loaded = co_await runs_.fetch(
      cache_,
      start.base,
      *admitted,
      loader(std::move(*shared)),
      caller,
      nullptr,
      readers);
    if (
      loaded || lender == nullptr || closing_ || !unkept(loaded.error().code()))
        co_return loaded;
    // No room for it as the cache's own: under its reader's reservation,
    // which holds what the run is estimated at and what its buffers'
    // bookkeeping and its entry turn out to cost.
    std::optional<runtime::result<workload_reservation>> funding;
    try {
        funding.emplace(lender->try_reserve(
          byte_count{admitted->value() + admitted->value() / 8 + 4_KiB}));
    } catch (const std::bad_alloc&) {
        co_return loaded;
    }
    if (!*funding) co_return loaded;
    auto again = source.share();
    if (!again) co_return loaded;
    co_return co_await runs_.fetch(
      cache_,
      start.base,
      *admitted,
      loader(std::move(*again)),
      caller,
      &**funding,
      0);
}

seastar::future<runtime::result<cache_ref<segment_run>>>
segment_runs::read_alone(
  segment_run_start start,
  runtime::file_position end,
  const segment_data_source& source,
  runtime::file_position bound,
  workload_budget& budget,
  seastar::abort_source& caller) {
    using output = runtime::result<cache_ref<segment_run>>;
    assert_current();
    if (closing_) co_return output{runtime::failure(run_error(errc::closed))};
    if (caller.abort_requested())
        co_return output{runtime::failure(run_error(errc::aborted))};
    if (end <= start.position || bound < end)
        co_return output{runtime::failure(run_error(errc::out_of_range))};
    const auto joined = loads_.hold();
    const auto& limits = config_.limits;
    codec::cooperative_work work{limits.policy, caller};
    const auto working = budget.try_reserve(
      byte_count{limits.working_bytes.value() + limits.metadata_bytes.value()});
    if (!working) co_return output{runtime::failure(working.error())};
    auto wanted = std::min(
      end.value() - start.position.value(), limits.run_bytes.value());
    // Once more, with exactly its length, for a first block that one read's
    // worth does not hold.
    for (int attempt = 0; attempt < 2; ++attempt) {
        const auto admitted = estimate(enclosing(start.position, wanted));
        if (!admitted) co_return output{runtime::failure(admitted.error())};
        // The run's own, for as long as anything holds it: what it is
        // estimated at, and room for what its buffers' bookkeeping and its
        // entry turn out to cost.
        auto kept = budget.try_reserve(
          byte_count{admitted->value() + admitted->value() / 8 + 4_KiB});
        if (!kept) co_return output{runtime::failure(kept.error())};
        cache_slot<segment_run> place;
        auto led = place.lead(cache_, *admitted, std::move(*kept));
        if (!led) co_return output{runtime::failure(led.error())};
        device_read(start.position);
        auto read = co_await source.read(
          start.position, byte_count{wanted}, bound);
        if (!read) {
            std::move(*led).fail(read.error());
            co_return output{runtime::failure(read.error())};
        }
        auto checked = co_await check_segment_run(
          std::move(*read), history_, start, false, limits, budget, work);
        if (!checked) {
            std::move(*led).fail(checked.error());
            co_return output{runtime::failure(checked.error())};
        }
        // Nothing fit: the first object is longer than one read's worth.
        if (checked->run.end() == start.position) {
            const auto whole = checked->cut.value_or(byte_count{});
            std::move(*led).fail(run_error(errc::out_of_range));
            if (
              attempt != 0 || whole.value() <= wanted
              || whole.value() > end.value() - start.position.value())
                co_return output{
                  runtime::failure(run_error(errc::malformed_data))};
            wanted = whole.value();
            continue;
        }
        if (auto sized = led->resize(checked->run.charge()); !sized) {
            std::move(*led).fail(sized.error());
            co_return output{runtime::failure(sized.error())};
        }
        // Kept by nothing: it lives with whoever holds it from here.
        co_return output{std::move(*led).publish_held(std::move(checked->run))};
    }
    co_return output{runtime::failure(run_error(errc::malformed_data))};
}

seastar::future<runtime::result<cache_ref<segment_run>>>
segment_runs::read_held(
  segment_run_start start,
  runtime::file_position end,
  const segment_read_held& held,
  workload_budget& budget,
  seastar::abort_source& caller) {
    using output = runtime::result<cache_ref<segment_run>>;
    assert_current();
    if (closing_) co_return output{runtime::failure(run_error(errc::closed))};
    if (caller.abort_requested())
        co_return output{runtime::failure(run_error(errc::aborted))};
    if (end <= start.position)
        co_return output{runtime::failure(run_error(errc::out_of_range))};
    const auto joined = loads_.hold();
    const auto& limits = config_.limits;
    codec::cooperative_work work{limits.policy, caller};
    const auto working = budget.try_reserve(
      byte_count{limits.working_bytes.value() + limits.metadata_bytes.value()});
    if (!working) co_return output{runtime::failure(working.error())};
    auto wanted = std::min(
      end.value() - start.position.value(), limits.run_bytes.value());
    // Once more, with exactly its length, for a first block that one read's
    // worth does not hold.
    for (int attempt = 0; attempt < 2; ++attempt) {
        // A seal that moved the session meanwhile let go of the writer:
        // everything is in the file.
        if (!held)
            co_return output{runtime::failure(run_error(errc::out_of_range))};
        auto bytes = held(
          start.position,
          runtime::file_position{start.position.value() + wanted});
        if (!bytes) co_return output{runtime::failure(bytes.error())};
        // The run's own, for as long as anything holds it: what the shares
        // keep alive, and room for its entry and its tables.
        const auto admitted = budget.buffer_charge(*bytes);
        if (!admitted) co_return output{runtime::failure(admitted.error())};
        auto kept = budget.try_reserve(
          byte_count{admitted->value() + admitted->value() / 8 + 4_KiB});
        if (!kept) co_return output{runtime::failure(kept.error())};
        cache_slot<segment_run> place;
        auto led = place.lead(cache_, *admitted, std::move(*kept));
        if (!led) co_return output{runtime::failure(led.error())};
        auto checked = co_await check_segment_run(
          std::move(*bytes), history_, start, false, limits, budget, work);
        if (!checked) {
            std::move(*led).fail(checked.error());
            co_return output{runtime::failure(checked.error())};
        }
        // Nothing fit: the first object is longer than one read's worth.
        if (checked->run.end() == start.position) {
            const auto whole = checked->cut.value_or(byte_count{});
            std::move(*led).fail(run_error(errc::out_of_range));
            if (
              attempt != 0 || whole.value() <= wanted
              || whole.value() > end.value() - start.position.value())
                co_return output{
                  runtime::failure(run_error(errc::malformed_data))};
            wanted = whole.value();
            continue;
        }
        if (auto sized = led->resize(checked->run.charge()); !sized) {
            std::move(*led).fail(sized.error());
            co_return output{runtime::failure(sized.error())};
        }
        co_return output{std::move(*led).publish_held(std::move(checked->run))};
    }
    co_return output{runtime::failure(run_error(errc::malformed_data))};
}

bool segment_runs::prefetch(
  segment_run_start start,
  runtime::file_position end,
  const segment_data_source& source,
  runtime::file_position bound,
  std::uint32_t readers) {
    assert_current();
    if (closing_ || end <= start.position || bound < end) return false;
    const auto length = end.value() - start.position.value();
    // A duplicate, a run the feed is filling, or one the cache does not
    // keep: nothing is asked for.
    if (length > config_.limits.run_bytes.value() || runs_.holds(start.base))
        return false;
    const auto admitted = estimate(enclosing(start.position, length));
    if (!admitted) return false;
    auto shared = source.share();
    if (!shared) return false;
    const bool started = runs_.prefetch(
      cache_,
      start.base,
      *admitted,
      [this,
       source = std::move(*shared),
       start,
       end,
       bound,
       joined = loads_.hold()](seastar::abort_source& stop) {
          return load(source, start, end, bound, stop);
      },
      readers);
    if (started) ++prefetched_;
    return started;
}

void segment_runs::seal() noexcept {
    assert_current();
    if (closing_ || sealed_) return;
    sealed_ = true;
    wanted_ = false;
    // All of a sealed segment is in its file, wherever its writer's bytes
    // had reached: a seal while the shard runs writes through a handle of
    // its own, of which the writer's observer is never told.
    written_.reset();
    stir();
}

segment_block_observer
segment_runs::observer(active_sparse_index& index) noexcept {
    // Nothing of its writer's is in the file yet.
    written_ = history_.data_start;
    segment_block_observer told{
      [this, &index](std::span<const segment_block_layout> blocks) noexcept {
          note(blocks, index);
      },
      [this, &index](runtime::file_position end) noexcept {
          durable_ = end;
          readable(index);
      },
      [this](
        runtime::file_position begin,
        bytes::fragmented_buffer& bytes,
        const workload_reservation& paid) noexcept {
          feed(begin, bytes, paid);
      },
      &wanted_};
    told.written = [this](runtime::file_position end) noexcept {
        written_ = end;
    };
    told.reported = [this, &index](runtime::file_position end) noexcept {
        reported_ = end;
        readable(index);
    };
    told.admit = [&index](std::uint32_t blocks) { return index.admit(blocks); };
    return told;
}

void segment_runs::note(
  std::span<const segment_block_layout> blocks,
  active_sparse_index& index) noexcept {
    assert_current();
    noted_.clear();
    torn_ = false;
    // A group may have no blocks: the footer that closes a held run.
    noted_group_ = true;
    for (const auto& block : blocks) {
        const bool chosen = index.written(block.records);
        if (!wanted_ || torn_) continue;
        try {
            noted_.push_back(
              noted_block{block.records, block.envelope, chosen});
        } catch (...) {
            torn_ = true;
        }
    }
}

void segment_runs::drop() noexcept {
    gap_ = true;
    ++dropped_;
    noted_.clear();
    noted_group_ = false;
}

void segment_runs::feed(
  runtime::file_position begin,
  bytes::fragmented_buffer& bytes,
  const workload_reservation& paid) noexcept {
    assert_current();
    // The time is looked at here, once a group: a segment nobody reads any
    // more stops being fed with the next thing written to it.
    if (established_ == 0) {
        const auto idle = config_.now
                            ? config_.now().checked_elapsed_since(read_)
                            : std::nullopt;
        if (!idle || idle->nanoseconds() > config_.feed_linger_nanoseconds) {
            wanted_ = false;
            lapsed_ = true;
            drop();
            stir();
            return;
        }
    }
    const auto size = bytes.size().value();
    if (
      torn_ || !noted_group_ || waiting_.size() >= maximum_fed_groups
      || size > config_.limits.run_bytes.value()
      || size > config_.feed_bytes.value() - waiting_bytes_) {
        drop();
        return;
    }
    try {
        waiting_.push_back(
          fed_group{
            begin, paid.share(), bytes.share(), std::move(noted_), gap_});
    } catch (...) {
        drop();
        return;
    }
    noted_.clear();
    noted_group_ = false;
    gap_ = false;
    waiting_bytes_ += size;
    stir();
}

void segment_runs::readable(active_sparse_index& index) noexcept {
    assert_current();
    const auto end = std::max(durable_, reported_);
    if (end <= readable_) return;
    readable_ = end;
    // Where a group ends, whichever told it: no block lies across it.
    index.durable(end);
    if (!pending_.empty()) stir();
}

void segment_runs::device_read(runtime::file_position at) noexcept {
    if (!pending_.empty() && at >= pending_.front()->building.run().begin())
        ++tail_device_reads_;
}

void segment_runs::abandon() noexcept {
    skipping_ = true;
    if (pending_.empty() || pending_.back()->closed) return;
    // Not kept, which is what a reader is told of a run that is being fed.
    std::move(pending_.back()->load).fail(run_error(errc::queue_full));
    pending_.pop_back();
}

void segment_runs::publish() noexcept {
    while (!pending_.empty()) {
        auto& oldest = *pending_.front();
        if (!oldest.closed || readable_ < oldest.building.run().end()) return;
        std::move(oldest.load).publish(std::move(oldest.building).finish());
        pending_.pop_front();
    }
}

void segment_runs::begin_run(const noted_block& block) {
    skipping_ = true;
    const auto base = model::range_logical_offset::make(
      block.records.logical().begin().value());
    if (!base || pending_.size() >= maximum_fed_runs) {
        ++dropped_;
        return;
    }
    // Kept for those the segment's owner says read it, and for one reader
    // when nobody says.
    auto led = runs_.lead(
      cache_,
      *base,
      byte_count{},
      std::nullopt,
      established_ != 0 ? established_ : 1U);
    if (!led) {
        ++dropped_;
        return;
    }
    try {
        pending_.push_back(
          std::make_unique<fed_run>(
            std::move(*led), *base, block.records.bytes().begin(), budget_));
    } catch (const std::bad_alloc&) {
        // The load is let go of with its entry.
        ++dropped_;
        return;
    }
    skipping_ = false;
}

bool segment_runs::append(
  fed_group& group,
  std::size_t first,
  std::size_t count,
  std::uint64_t from,
  std::uint64_t until) {
    auto& filling = *pending_.back();
    const auto& run = filling.building.run();
    const auto length = until - from;
    if (
      run.end().value() - run.begin().value() + length
      > config_.limits.run_bytes.value())
        return false;
    try {
        auto slice = group.bytes.share(byte_count{from}, byte_count{length});
        if (!slice) return false;
        // What the part keeps alive is taken before it is made, and made
        // exact once it is there: the allocations of the fragments it
        // shares, and copies of the others.
        std::uint64_t room = 0, copying = 0;
        for (std::size_t at = 0; at < slice->fragment_count(); ++at) {
            if (config_.copy_fed || !dense(*slice, at)) {
                copying += slice->fragment_at(at)->size();
                continue;
            }
            const auto cost = slice->allocation_cost(
              at, 1, config_.limits.charge);
            if (!cost) return false;
            room += cost->backing.value() + cost->share_controls.value();
        }
        const auto copies = estimate(copying);
        if (!copies) return false;
        if (!filling.load.resize(
              byte_count{run.charge().value() + room + copies->value()}))
            return false;
        auto part = config_.copy_fed ? std::optional{slice->copy()}
                                     : shared_or_copied(std::move(*slice));
        if (!part || !filling.building.part(std::move(*part))) return false;
        copied_ += copying;
    } catch (const std::bad_alloc&) {
        return false;
    }
    for (auto at = first; at < first + count; ++at) {
        const auto& block = group.blocks[at];
        const auto bytes = block.records.bytes();
        const auto header = block.envelope.header_bytes().value()
                            + segment_block_fixed_bytes.value();
        if (!filling.building.block(
              segment_run_block{
                .logical = block.records.logical(),
                .physical = block.records.physical(),
                .offset = bytes.begin().value() - run.begin().value(),
                .bytes = static_cast<std::uint32_t>(bytes.size().value()),
                .batch_offset = static_cast<std::uint32_t>(header),
                .batch_bytes = static_cast<std::uint32_t>(
                  block.envelope.body_bytes().value()
                  - segment_block_fixed_bytes.value()
                  - block.envelope.padding_bytes().value()),
                .part = 0}))
            return false;
    }
    return filling.load.resize(run.charge()).has_value();
}

void segment_runs::take(fed_group& group) {
    if (group.after_gap) abandon();
    const auto size = group.bytes.size().value();
    // What of this group goes to the run that is filling: its bytes from
    // `from`, which hold its blocks from `first`.
    std::size_t first = 0;
    std::uint64_t from = 0;
    for (std::size_t at = 0; at < group.blocks.size(); ++at) {
        const auto& block = group.blocks[at];
        if (!block.chosen) continue;
        // The index chose this block: the run before it ends where it
        // begins, and the next begins with it.
        const auto begins = block.records.bytes().begin().value()
                            - group.begin.value();
        if (!skipping_) {
            if (
              begins > from && !append(group, first, at - first, from, begins))
                abandon();
            else
                pending_.back()->closed = true;
        }
        begin_run(block);
        first = at;
        from = begins;
    }
    if (
      !skipping_
      && !append(group, first, group.blocks.size() - first, from, size))
        abandon();
}

seastar::future<> segment_runs::copy() {
    while (!closing_) {
        if (lapsed_) {
            lapsed_ = false;
            abandon();
        }
        if (waiting_.empty()) {
            if (sealed_ && !finished_) {
                // Everything written was handed over, or it was not: the
                // run that was filling is whole, or is given up.
                finished_ = true;
                if (gap_ || skipping_)
                    abandon();
                else if (!pending_.empty())
                    pending_.back()->closed = true;
            }
            publish();
            stirring_ = false;
            idle_.broadcast();
            co_await wake_.when();
            continue;
        }
        take_front();
        co_await seastar::coroutine::maybe_yield();
    }
}

void segment_runs::take_front() noexcept {
    auto group = std::move(waiting_.front());
    waiting_.pop_front();
    waiting_bytes_ -= group.bytes.size().value();
    try {
        take(group);
    } catch (...) {
        // Whatever kept a group from being taken, its run is read from the
        // device instead.
        abandon();
    }
    publish();
}

bool segment_runs::catch_up(model::range_logical_offset target) noexcept {
    assert_current();
    if (closing_ || finished_ || lapsed_) return false;
    // The first group waiting whose blocks reach past `target`: it holds
    // `target` unless that lies before it, in a group already taken or one
    // that was not kept.
    std::size_t holder = 0;
    while (holder < waiting_.size()
           && (waiting_[holder].blocks.empty()
               || waiting_[holder].blocks.back().records.logical().end().value()
                    <= target.value()))
        ++holder;
    if (
      holder == waiting_.size()
      || waiting_[holder].blocks.front().records.logical().begin().value()
           > target.value())
        return false;
    const auto copied = copied_;
    bool took = false;
    for (std::size_t taken = 0; taken <= holder; ++taken) {
        if (
          waiting_.front().after_gap || lapsed_
          || copied_ - copied >= config_.limits.run_bytes.value())
            break;
        take_front();
        took = true;
    }
    return took;
}

seastar::future<runtime::result<void>> segment_runs::close() {
    assert_current();
    if (closed_) co_return debt_.outcome();
    // One drain at a time: a second caller is not told it ended.
    if (closing_) co_return runtime::failure(run_error(errc::queue_full));
    closing_ = true;
    wanted_ = false;
    wake_.signal();
    idle_.broadcast();
    if (copier_) {
        // However the feed's own work ended, the rest goes on.
        try {
            co_await std::move(*copier_);
        } catch (...) {
            debt_.observe(std::current_exception());
        }
        copier_.reset();
    }
    // What is durable and complete is the cache's; the rest is given up.
    publish();
    while (!pending_.empty()) {
        std::move(pending_.back()->load).fail(run_error(errc::closed));
        pending_.pop_back();
    }
    waiting_.clear();
    waiting_bytes_ = 0;
    co_await loads_.close();
    co_await runs_.clear_gently();
    closed_ = true;
    co_return debt_.outcome();
}

} // namespace kwaque::storage
