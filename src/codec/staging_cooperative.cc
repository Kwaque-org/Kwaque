#include "src/codec/staging_cooperative.h"

#include "src/base/invariant.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/codec/transaction.h"

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <algorithm>
#include <array>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <optional>
#include <utility>

namespace kwaque::codec {
namespace {

using bytes::fragmented_buffer;
using bytes::fragmented_buffer_builder;

error at(errc code, field_context context) noexcept {
    return error{code, context.family, context.field, context.origin};
}

result<void> add(byte_count& target, byte_count amount, field_context context) {
    const auto next = target.checked_add(amount);
    if (!next) {
        return codec::failure(at(errc::out_of_range, context));
    }
    target = *next;
    return {};
}

result<byte_count>
multiply(byte_count amount, std::uint64_t count, field_context context) {
    if (
      count != 0
      && amount.value() > std::numeric_limits<std::uint64_t>::max() / count) {
        return codec::failure(at(errc::out_of_range, context));
    }
    return byte_count{amount.value() * count};
}

// The native descriptor query is divided before it enters its synchronous
// loop. Descriptor history is charged once by the range starting at zero.
seastar::future<result<bytes::buffer_allocation_cost>> input_cost(
  const fragmented_buffer& input,
  cooperative_work& work,
  bytes::allocation_charge_fn charge,
  field_context context) {
    const auto anchor = at(errc::success, context);
    bytes::buffer_allocation_cost total;
    byte_count aggregate;
    const auto quantum = work.item_quantum().value();
    if (!input.empty() && quantum < 6) {
        co_return codec::failure(at(errc::resource_exhausted, context));
    }
    const auto batch = quantum < 6 ? 1U : (quantum - 2U) / 4U;
    std::size_t first = 0;
    do {
        const auto count = std::min<std::size_t>(
          input.fragment_count() - first, batch);
        auto admitted = co_await work.admit(
          byte_count{}, item_count{count == 0 ? 1U : 4U * count + 2U}, anchor);
        if (!admitted) {
            co_return codec::failure(admitted.error());
        }
        if (auto ready = work.poll(anchor); !ready) {
            co_return codec::failure(ready.error());
        }
        const auto part = input.allocation_cost(first, count, charge);
        if (!part) {
            co_return codec::failure(
              detail::allocation_cost_error(
                part.error(), context, context.origin));
        }
        for (const auto amount :
             {part->backing, part->descriptors, part->share_controls}) {
            if (auto summed = add(aggregate, amount, context); !summed) {
                co_return codec::failure(summed.error());
            }
        }
        // Each category is bounded by the already checked aggregate.
        total.backing = byte_count{
          total.backing.value() + part->backing.value()};
        total.descriptors = byte_count{
          total.descriptors.value() + part->descriptors.value()};
        total.share_controls = byte_count{
          total.share_controls.value() + part->share_controls.value()};
        total.largest_allocation = std::max(
          total.largest_allocation, part->largest_allocation);
        first += count;
    } while (first != input.fragment_count());
    total.fragments = item_count{input.fragment_count()};
    co_return total;
}

struct assembly_shape {
    std::uint64_t ceiling;
    std::uint64_t nodes;
};

seastar::future<result<assembly_shape>> choose_shape(
  const fragmented_buffer& prefix,
  const fragmented_buffer& payload,
  cooperative_work& work,
  byte_count total,
  byte_count input_backing,
  operation_usage live,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  field_context context) {
    const auto policy = work.policy();
    const auto config = policy.config();
    const auto anchor = at(errc::success, context);
    const auto available = policy.remaining_operation_bytes(
      live, parent_remaining);
    if (!available) {
        co_return codec::failure(
          detail::allocation_cost_error(
            available.error(), context, context.origin));
    }
    // One splice includes its preflight, transfer, trim and temporary cleanup.
    if (work.item_quantum().value() < 8) {
        co_return codec::failure(at(errc::resource_exhausted, context));
    }
    auto ceiling = std::min(
      {total.value(), config.max_allocation_bytes.value(), available->value()});
    while (ceiling != 0) {
        std::uint64_t nodes = 0;
        std::uint64_t copied_pieces = 0;
        bool copy_fits = true;
        for (const auto* source : {&prefix, &payload}) {
            for (const auto fragment : *source) {
                const auto admitted = co_await work.admit(
                  byte_count{}, item_count{1}, anchor);
                if (!admitted) {
                    co_return codec::failure(admitted.error());
                }
                if (auto ready = work.poll(anchor); !ready) {
                    co_return codec::failure(ready.error());
                }
                const auto length = static_cast<std::uint64_t>(fragment.size());
                const bool copied
                  = length
                      <= fragmented_buffer_builder::pack_copy_threshold.value()
                    && length <= ceiling;
                if (copied) {
                    const auto slice = work.byte_quantum().value() / 2U;
                    if (slice == 0) {
                        copy_fits = false;
                        continue;
                    }
                    const auto pieces = 1U + (length - 1U) / slice;
                    nodes += pieces;
                    copied_pieces += pieces;
                } else {
                    ++nodes;
                }
            }
        }
        if (!copy_fits || nodes > config.max_buffer_fragments.value()) {
            // A narrower tail ceiling can turn packing into a no-copy splice.
            ceiling /= 2U;
            continue;
        }
        const byte_count descriptor_request{
          nodes * fragmented_buffer::fragment_descriptor_size()};
        const byte_count slice_request{
          2U * fragmented_buffer::fragment_descriptor_size()};
        const auto descriptors = charge(descriptor_request);
        const auto slice_descriptors = charge(slice_request);
        const auto tail = copied_pieces == 0 ? byte_count{}
                                             : charge(byte_count{ceiling});
        if (
          descriptors < descriptor_request || slice_descriptors < slice_request
          || (copied_pieces != 0 && tail < byte_count{ceiling})) {
            co_return codec::failure(at(errc::invalid_argument, context));
        }
        const auto backing = multiply(tail, copied_pieces, context);
        const auto descriptor_peak = multiply(descriptors, 2, context);
        const auto slice_peak = multiply(slice_descriptors, 2, context);
        if (!backing || !descriptor_peak || !slice_peak) {
            co_return codec::failure(at(errc::out_of_range, context));
        }
        auto projected = live;
        for (const auto cost : {*descriptor_peak, *slice_peak}) {
            if (
              auto added = add(projected.payload_bookkeeping, cost, context);
              !added) {
                co_return codec::failure(added.error());
            }
        }
        if (
          auto added = add(projected.staged_output, *backing, context);
          !added) {
            co_return codec::failure(added.error());
        }
        auto retained = input_backing;
        if (auto added = add(retained, *backing, context); !added) {
            co_return codec::failure(added.error());
        }
        const auto fits = policy.remaining_operation_bytes(
          projected, parent_remaining);
        if (!fits && fits.error() != errc::resource_exhausted) {
            co_return codec::failure(
              detail::allocation_cost_error(
                fits.error(), context, context.origin));
        }
        if (
          descriptors <= config.max_allocation_bytes
          && slice_descriptors <= config.max_allocation_bytes
          && tail <= config.max_allocation_bytes
          && retained <= config.max_retained_bytes && fits) {
            co_return assembly_shape{ceiling, nodes};
        }
        ceiling /= 2U;
    }
    co_return codec::failure(at(errc::resource_exhausted, context));
}

seastar::future<result<void>> splice_input(
  fragmented_buffer& source,
  fragmented_buffer_builder& output,
  const assembly_shape shape,
  cooperative_work& work,
  field_context context) {
    const auto anchor = at(errc::success, context);
    while (!source.empty()) {
        const auto fragment = source.fragment_at(0).value();
        const auto length = static_cast<std::uint64_t>(fragment.size());
        const bool copied
          = length <= fragmented_buffer_builder::pack_copy_threshold.value()
            && length <= shape.ceiling;
        const auto slice_size = copied
                                  ? std::min(
                                      length, work.byte_quantum().value() / 2U)
                                  : length;
        auto before_share = co_await work.checkpoint(anchor);
        if (!before_share) {
            co_return codec::failure(before_share.error());
        }
        if (auto ready = work.poll(anchor); !ready) {
            co_return codec::failure(ready.error());
        }
        auto slice = source.share(byte_count{}, byte_count{slice_size});
        if (!slice) {
            co_return codec::failure(
              detail::allocation_cost_error(
                slice.error(), context, context.origin));
        }
        const auto after_share = co_await work.checkpoint(anchor);
        if (!after_share) {
            co_return codec::failure(after_share.error());
        }
        const auto admitted = co_await work.admit(
          byte_count{copied ? 2U * slice_size : 0U}, item_count{8}, anchor);
        if (!admitted) {
            co_return codec::failure(admitted.error());
        }
        if (auto ready = work.poll(anchor); !ready) {
            co_return codec::failure(ready.error());
        }
        const auto appended = output.append_buffer(std::move(*slice));
        if (!appended) {
            co_return codec::failure(
              detail::allocation_cost_error(
                appended.error(), context, context.origin));
        }
        const auto trimmed = source.trim_front(byte_count{slice_size});
        KWAQUE_INVARIANT(
          invariant_id{"KQ-CODEC-STAGING-TRIM"},
          trimmed.has_value(),
          "admitted source slice could not be consumed");
    }
    co_return result<void>{};
}

seastar::future<result<fragmented_buffer>> assemble_owned(
  fragmented_buffer& prefix,
  fragmented_buffer& payload,
  std::optional<fragmented_buffer_builder>& output,
  cooperative_work& work,
  byte_count logical_cap,
  operation_usage live,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  field_context context) {
    const auto anchor = at(errc::success, context);
    const auto entered = co_await work.admit(
      byte_count{}, item_count{1}, anchor);
    if (!entered) {
        co_return codec::failure(entered.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    if (charge == nullptr) {
        co_return codec::failure(at(errc::invalid_argument, context));
    }
    const auto total = prefix.size().checked_add(payload.size());
    if (
      !total
      || total->value()
           > std::numeric_limits<std::uint64_t>::max() - context.origin) {
        co_return codec::failure(at(errc::invalid_argument, context));
    }
    if (*total > logical_cap) {
        co_return codec::failure(at(errc::resource_exhausted, context));
    }
    const auto policy = work.policy();
    byte_count input_backing;
    for (const auto* source : {&prefix, &payload}) {
        const auto cost = co_await input_cost(*source, work, charge, context);
        if (!cost) {
            co_return codec::failure(cost.error());
        }
        if (auto ready = work.poll(anchor); !ready) {
            co_return codec::failure(ready.error());
        }
        const auto valid = policy.validate_buffer(
          source->size(), cost->backing, cost->fragments, logical_cap);
        if (!valid) {
            co_return codec::failure(
              detail::allocation_cost_error(
                valid.error(), context, context.origin));
        }
        for (const auto amount : {cost->descriptors, cost->share_controls}) {
            if (
              auto added = add(live.payload_bookkeeping, amount, context);
              !added) {
                co_return codec::failure(added.error());
            }
        }
        if (
          auto added = add(live.retained_input, cost->backing, context);
          !added) {
            co_return codec::failure(added.error());
        }
        if (auto added = add(input_backing, cost->backing, context); !added) {
            co_return codec::failure(added.error());
        }
    }
    const auto admitted = policy.remaining_operation_bytes(
      live, parent_remaining);
    if (!admitted) {
        co_return codec::failure(
          detail::allocation_cost_error(
            admitted.error(), context, context.origin));
    }
    if (total->value() == 0) {
        co_return fragmented_buffer{};
    }
    const auto shape = co_await choose_shape(
      prefix,
      payload,
      work,
      *total,
      input_backing,
      live,
      parent_remaining,
      charge,
      context);
    if (!shape) {
        co_return codec::failure(shape.error());
    }
    const auto setup = co_await work.admit(byte_count{}, item_count{1}, anchor);
    if (!setup) {
        co_return codec::failure(setup.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    bytes::fragmented_buffer_builder_config config;
    config.initial_fragment_bytes = byte_count{
      std::min(shape->ceiling, std::uint64_t{512})};
    config.max_fragment_bytes = byte_count{shape->ceiling};
    config.max_total_bytes = *total;
    config.max_retained_bytes = policy.config().max_retained_bytes;
    config.max_fragments = static_cast<std::size_t>(shape->nodes);
    output.emplace(config);
    const auto reserved = output->reserve_fragments(item_count{shape->nodes});
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CODEC-STAGING-RESERVE"},
      reserved.has_value(),
      "admitted descriptor reservation failed");
    for (auto* source : {&prefix, &payload}) {
        const auto appended = co_await splice_input(
          *source, *output, *shape, work, context);
        if (!appended) {
            co_return codec::failure(appended.error());
        }
    }
    const auto before_finish = co_await work.checkpoint(anchor);
    if (!before_finish) {
        co_return codec::failure(before_finish.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    auto finished = output->finish();
    if (!finished) {
        co_return codec::failure(
          detail::allocation_cost_error(
            finished.error(), context, context.origin));
    }
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CODEC-STAGING-SIZE"},
      finished->size() == *total,
      "published buffer size differs from admitted size");
    // This is still private staging. The outer owner checks again after all
    // input and builder teardown and before transferring the result.
    co_return std::move(*finished);
}

} // namespace

seastar::future<result<bytes::fragmented_buffer>> assemble_buffer_cooperatively(
  bytes::fragmented_buffer&& prefix,
  bytes::fragmented_buffer&& payload,
  cooperative_work& work,
  byte_count logical_cap,
  operation_usage other_live,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  field_context context) {
    if (std::addressof(prefix) == std::addressof(payload)) {
        co_return codec::failure(at(errc::invalid_argument, context));
    }
    auto owned_prefix = std::move(prefix);
    auto owned_payload = std::move(payload);
    std::optional<fragmented_buffer_builder> output;
    std::optional<result<fragmented_buffer>> produced;
    std::exception_ptr exception;
    try {
        produced.emplace(
          co_await assemble_owned(
            owned_prefix,
            owned_payload,
            output,
            work,
            logical_cap,
            other_live,
            parent_remaining,
            charge,
            context));
    } catch (...) {
        exception = std::current_exception();
    }
    // Cleanup stays in this owning frame. The native awaiter schedules this
    // frame directly, so allocation failure cannot prevent bounded teardown.
    // Nonempty foundation owners have at most 1024 descriptors; reserve an
    // entire quantum around that explicit indivisible release. Empty owners
    // retain a positive cumulative cost instead of resetting child budgets.
    if (output) {
        const bool nonempty = output->fragment_count() != 0;
        co_await work.drain_inline(
          nonempty ? work.byte_quantum() : byte_count{},
          nonempty ? work.item_quantum() : item_count{1});
        output.reset();
    }
    for (auto* input : {&owned_prefix, &owned_payload}) {
        const bool nonempty = !input->empty();
        co_await work.drain_inline(
          nonempty ? work.byte_quantum() : byte_count{},
          nonempty ? work.item_quantum() : item_count{1});
        *input = fragmented_buffer{};
    }
    if (exception) {
        std::rethrow_exception(exception);
    }
    if (!produced->has_value()) {
        co_return codec::failure(produced->error());
    }
    if (auto ready = work.poll(at(errc::success, context)); !ready) {
        const bool nonempty = !(**produced).empty();
        co_await work.drain_inline(
          nonempty ? work.byte_quantum() : byte_count{},
          nonempty ? work.item_quantum() : item_count{1});
        produced.reset();
        co_return codec::failure(ready.error());
    }
    co_return std::move(**produced);
}

} // namespace kwaque::codec
