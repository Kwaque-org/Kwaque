#include "src/model/batch_rewrite.h"

#include "src/base/invariant.h"
#include "src/model/batch_staging.h"
#include "src/model/record_scan.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <optional>
#include <utility>

namespace kwaque::model {
namespace {
using bytes::fragmented_buffer;
using bytes::fragmented_buffer_parser;
codec::error
at(errc code, codec::field_context context, byte_count position = {}) {
    return codec::error{
      code, context.family, context.field, context.origin + position.value()};
}
} // namespace

namespace detail {

// The concrete rewrite owner keeps all partial native owners reachable by its
// parent coroutine's allocation-free cleanup, even when a child throws.
class batch_rewriter final {
public:
    fragmented_buffer_parser input;
    fragmented_buffer source;
    std::optional<bytes::fragmented_buffer_builder> output;
    fragmented_buffer produced;
    item_count headers;

    [[nodiscard]] assigned_batch finish(
      assigned_batch_context context,
      codec::semantic_batch_digest digest) && noexcept {
        return assigned_batch{context, digest, headers, std::move(produced)};
    }

    seastar::future<codec::result<void>> run(
      assigned_batch_context context,
      item_count expected_headers,
      std::span<const range_logical_count> selected,
      codec::decode_budget memory,
      codec::cooperative_work& work,
      codec::field_context coordinates) {
        const auto anchor = at(errc::success, coordinates);
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        const auto start = codec::detail::integer_read_start(
          input, coordinates, codec::input_boundary::complete);
        if (!start) co_return codec::failure(start.error());
        if (
          input.at_end() || selected.empty()
          || selected.size() > context.retained_count().value())
            co_return codec::failure(at(errc::invalid_argument, coordinates));
        const auto policy = work.policy();
        const auto submitted = context.submitted();
        const bool keep_all = selected.size()
                              == context.retained_count().value();
        const auto counts = policy.validate_batch_counts(
          item_count{submitted.original_count().value()},
          context.retained_count(),
          expected_headers);
        if (!counts)
            co_return codec::failure(
              codec::detail::allocation_cost_error(
                counts.error(), coordinates, *start));
        if (
          input.total_bytes() > policy.config().max_expanded_batch_bytes
          || work.byte_quantum().value() < 4U * sizeof(record_layout)
          || work.item_quantum().value() < 64)
            co_return codec::failure(at(errc::resource_exhausted, coordinates));
        if (context.retained_count().value() > input.total_bytes().value() / 7U)
            co_return codec::failure(at(errc::malformed_data, coordinates));
        std::optional<range_logical_count> previous;
        for (const auto delta : selected) {
            if (
              auto ready = co_await work.admit(
                byte_count{}, item_count{4}, anchor);
              !ready)
                co_return codec::failure(ready.error());
            if (auto ready = work.poll(anchor); !ready)
                co_return codec::failure(ready.error());
            if (
              delta >= submitted.original_count()
              || (previous && delta <= *previous))
                co_return codec::failure(
                  at(errc::invalid_argument, coordinates));
            previous = delta;
        }
        if (!keep_all) {
            const auto geometry = make_batch_staging(
              policy, memory.charge, anchor);
            if (!geometry) co_return codec::failure(geometry.error());
            shape_ = *geometry;
        }
        if (auto ready = co_await work.checkpoint(anchor); !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        const auto admitted = codec::reserve_decode_input(
          input, policy, memory, coordinates, codec::input_boundary::complete);
        if (!admitted) co_return codec::failure(admitted.error());
        if (auto ready = co_await work.checkpoint(anchor); !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        const auto cost = input.next_buffer_allocation_cost(
          input.total_bytes(), memory.charge);
        if (!cost)
            co_return codec::failure(
              codec::detail::allocation_cost_error(
                cost.error(), coordinates, *start));
        const auto valid = codec::detail::validate_decode_cost(
          input.total_bytes(),
          policy.config().max_expanded_batch_bytes,
          *cost,
          policy,
          coordinates,
          *start);
        if (!valid) co_return codec::failure(valid.error());
        const auto reserved = codec::detail::consume_decode_budget(
          policy,
          *admitted,
          byte_count{},
          cost->descriptors,
          coordinates,
          *start);
        if (!reserved) co_return codec::failure(reserved.error());
        memory_ = *reserved;
        if (auto ready = co_await work.checkpoint(anchor); !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        auto shared = input.peek_buffer(input.total_bytes());
        if (!shared)
            co_return codec::failure(
              codec::detail::allocation_cost_error(
                shared.error(), coordinates, *start));
        source = std::move(*shared);
        cursor_ = source.begin();
        if (auto ready = co_await work.checkpoint(anchor); !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());

        std::size_t selection = 0;
        std::uint64_t header_count = 0;
        previous.reset();
        const bool dense = context.retained_count().value()
                           == submitted.original_count().value();
        for (std::uint64_t index = 0; index < context.retained_count().value();
             ++index) {
            auto child_memory = memory_;
            if (output) {
                const auto remaining = admit_batch_staging(
                  shape_,
                  output->size(),
                  policy,
                  memory_.operation_remaining,
                  memory_.charge,
                  anchor);
                if (!remaining) co_return codec::failure(remaining.error());
                child_memory.operation_remaining = *remaining;
            }
            if (
              auto ready = co_await work.admit(
                byte_count{4U * sizeof(record_layout)}, item_count{64}, anchor);
              !ready)
                co_return codec::failure(ready.error());
            if (auto ready = work.poll(anchor); !ready)
                co_return codec::failure(ready.error());
            const auto current = co_await scan_record(
              input,
              {submitted.original_timestamp_base(),
               submitted.original_count(),
               item_count{expected_headers.value() - header_count}},
              child_memory,
              work,
              coordinates);
            if (!current) co_return codec::failure(current.error());
            if (auto ready = work.poll(anchor); !ready)
                co_return codec::failure(ready.error());
            const auto delta = current->fields.logical_delta;
            if ((previous && delta <= *previous)
                || (dense && (delta.value() != index || (index == 0 && current->fields.timestamp_delta != 0))))
                co_return codec::failure(at(
                  errc::malformed_data, coordinates, current->encoded.offset));
            previous = delta;
            header_count += current->headers().size();
            if (selection == selected.size()) continue;
            if (selected[selection] < delta)
                co_return codec::failure(at(
                  errc::invalid_argument,
                  coordinates,
                  current->encoded.offset));
            if (selected[selection] != delta) continue;
            if (!keep_all) {
                const auto appended = co_await append_extent(
                  current->encoded, work, coordinates);
                if (!appended) co_return codec::failure(appended.error());
                if (auto ready = work.poll(anchor); !ready)
                    co_return codec::failure(ready.error());
            }
            // Count the match only after any required copy has completed.
            headers = item_count{headers.value() + current->headers().size()};
            ++selection;
        }
        if (!input.at_end() || header_count != expected_headers.value())
            co_return codec::failure(
              at(errc::malformed_data, coordinates, input.bytes_consumed()));
        if (selection != selected.size())
            co_return codec::failure(at(errc::invalid_argument, coordinates));
        // Count equality alone is insufficient: reuse backing only after every
        // requested delta matched the actual current survivors and exhaustion.
        if (keep_all) {
            produced = std::move(source);
            co_return codec::result<void>{};
        }
        if (auto ready = co_await work.checkpoint(anchor); !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        KWAQUE_INVARIANT(
          invariant_id{"KQ-BATCH-REWRITE-OUTPUT"},
          output.has_value() && !output->empty(),
          "nonempty matched selection has no encoded record bytes");
        const auto size = output->size();
        auto complete = output->finish();
        if (!complete)
            co_return codec::failure(
              codec::detail::allocation_cost_error(
                complete.error(), coordinates, coordinates.origin));
        produced = std::move(*complete);
        KWAQUE_INVARIANT(
          invariant_id{"KQ-BATCH-REWRITE-SIZE"},
          produced.size() == size,
          "published survivor bytes differ from completed record appends");
        co_return codec::result<void>{};
    }

private:
    batch_staging_shape shape_;
    codec::decode_budget memory_;
    fragmented_buffer::const_iterator cursor_;
    std::size_t fragment_offset_{0};
    std::uint64_t position_{0};

    void advance(std::size_t size, bytes::fragment_view fragment) noexcept {
        position_ += size;
        fragment_offset_ += size;
        if (fragment_offset_ == fragment.size()) {
            ++cursor_;
            fragment_offset_ = 0;
        }
    }

    seastar::future<codec::result<void>> append_extent(
      record_byte_range extent,
      codec::cooperative_work& work,
      codec::field_context coordinates) {
        const auto anchor = at(errc::success, coordinates, extent.offset);
        const auto end = extent.offset.checked_add(extent.length);
        KWAQUE_INVARIANT(
          invariant_id{"KQ-BATCH-REWRITE-EXTENT"},
          end && *end <= source.size() && position_ <= extent.offset.value(),
          "record selection did not advance within its validated region");
        const auto total
          = (output ? output->size() : byte_count{}).checked_add(extent.length);
        if (!total)
            co_return codec::failure(
              at(errc::out_of_range, coordinates, extent.offset));
        const auto admitted = admit_batch_staging(
          shape_,
          *total,
          work.policy(),
          memory_.operation_remaining,
          memory_.charge,
          anchor);
        if (!admitted) co_return codec::failure(admitted.error());
        if (auto ready = co_await work.checkpoint(anchor); !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        if (!output) {
            output.emplace(shape_.config);
            const auto reserved = output->reserve_fragments(
              item_count{shape_.config.max_fragments});
            KWAQUE_INVARIANT(
              invariant_id{"KQ-BATCH-REWRITE-RESERVE"},
              reserved.has_value(),
              "admitted survivor descriptor reservation failed");
        }
        // One forward fragment cursor skips holes and copies complete canonical
        // extents; no re-encoding, per-record share or selected-record index.
        while (position_ < end->value()) {
            KWAQUE_INVARIANT(
              invariant_id{"KQ-BATCH-REWRITE-CURSOR"},
              cursor_ != source.end(),
              "survivor cursor exhausted its validated source early");
            const auto fragment = *cursor_;
            const bool copy = position_ >= extent.offset.value();
            const auto bound = copy ? end->value() : extent.offset.value();
            const auto size = std::min(
              {static_cast<std::uint64_t>(fragment.size() - fragment_offset_),
               bound - position_,
               copy ? work.byte_quantum().value() / 2U : bound - position_});
            if (
              auto ready = co_await work.admit(
                byte_count{copy ? 2U * size : 0U}, item_count{8}, anchor);
              !ready)
                co_return codec::failure(ready.error());
            if (auto ready = work.poll(anchor); !ready)
                co_return codec::failure(ready.error());
            if (copy) {
                const auto appended = output->append(
                  std::span<const char>{
                    fragment.data() + fragment_offset_,
                    static_cast<std::size_t>(size)});
                if (!appended)
                    co_return codec::failure(
                      codec::detail::allocation_cost_error(
                        appended.error(),
                        coordinates,
                        coordinates.origin + position_));
            }
            advance(static_cast<std::size_t>(size), fragment);
        }
        KWAQUE_INVARIANT(
          invariant_id{"KQ-BATCH-REWRITE-APPEND"},
          output->size() == *total,
          "selected record copy differs from its validated extent");
        co_return codec::result<void>{};
    }
};
} // namespace detail

seastar::future<codec::result<removed_batch_coverage>>
remove_all_records(assigned_batch&& source, codec::cooperative_work& work) {
    const auto original = source.context();
    const auto digest = source.fingerprint();
    auto records = std::move(source).release_records();
    constexpr codec::error anchor{errc::success, 2};
    std::optional<codec::error> failed;
    if (auto ready = work.poll(anchor); !ready)
        failed = ready.error();
    else if (records.empty())
        failed = codec::error{errc::invalid_argument, 2};

    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    records = fragmented_buffer{};
    if (failed) co_return codec::failure(*failed);
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    co_return removed_batch_coverage{
      original.submitted(), original.logical_span(), digest};
}

seastar::future<codec::result<assigned_batch>> rewrite_assigned_batch(
  assigned_batch&& source,
  std::span<const range_logical_count> selected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context) {
    const auto original = source.context();
    const auto digest = source.fingerprint();
    const auto headers = source.header_count();
    detail::batch_rewriter owner;
    owner.input = fragmented_buffer_parser{std::move(source).release_records()};
    context.family = 2;
    std::optional<codec::result<void>> outcome;
    std::exception_ptr exception;
    try {
        outcome.emplace(
          co_await owner.run(
            original, headers, selected, memory, work, context));
    } catch (...) {
        exception = std::current_exception();
    }
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    owner.output.reset();
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    owner.input = fragmented_buffer_parser{};
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    owner.source = fragmented_buffer{};
    if (!exception && outcome->has_value()) {
        if (auto ready = work.poll(at(errc::success, context)); !ready)
            *outcome = codec::failure(ready.error());
    }
    if (exception || !outcome->has_value()) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        owner.produced = fragmented_buffer{};
        if (exception) std::rethrow_exception(exception);
        co_return codec::failure(outcome->error());
    }
    const auto narrowed = original.with_retained_count(
      item_count{selected.size()});
    KWAQUE_INVARIANT(
      invariant_id{"KQ-BATCH-REWRITE-COUNT"},
      narrowed.has_value(),
      "validated nonempty survivor selection could not preserve its original "
      "span");
    co_return std::move(owner).finish(*narrowed, digest);
}

} // namespace kwaque::model
