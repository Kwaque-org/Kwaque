#include "src/codec/staging.h"

#include "src/base/error.h"
#include "src/base/invariant.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/codec/error.h"
#include "src/codec/limits.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <system_error>
#include <utility>

namespace kwaque::codec {
namespace {

error staging_error(errc code, field_context context) noexcept {
    return error{code, context.family, context.field, context.origin};
}

errc accounting_error(const std::error_code& code) noexcept {
    if (code == errc::invalid_argument) {
        return errc::invalid_argument;
    }
    if (code == errc::out_of_range) {
        return errc::out_of_range;
    }
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CODEC-STAGING-COST"},
      code == errc::resource_exhausted,
      "staging accounting returned an unexpected failure");
    return errc::resource_exhausted;
}

result<void>
add_charge(byte_count& total, byte_count cost, field_context context) noexcept {
    const auto sum = total.checked_add(cost);
    if (!sum) {
        return codec::failure(staging_error(errc::out_of_range, context));
    }
    total = *sum;
    return {};
}

result<byte_count> multiply_charge(
  byte_count cost, std::uint64_t count, field_context context) noexcept {
    if (
      count != 0
      && cost.value() > std::numeric_limits<std::uint64_t>::max() / count) {
        return codec::failure(staging_error(errc::out_of_range, context));
    }
    return byte_count{cost.value() * count};
}

} // namespace

result<bytes::fragmented_buffer> assemble_buffer(
  std::span<const char> prefix,
  bytes::fragmented_buffer payload,
  const limits& policy,
  byte_count logical_cap,
  const operation_usage& other_live,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  field_context context) {
    if (charge == nullptr) {
        return codec::failure(staging_error(errc::invalid_argument, context));
    }
    const auto total = payload.size().checked_add(byte_count{prefix.size()});
    if (
      !total
      || total->value()
           > std::numeric_limits<std::uint64_t>::max() - context.origin) {
        return codec::failure(staging_error(errc::invalid_argument, context));
    }
    const auto config = policy.config();
    const auto work_bytes = std::min(
      config.max_work_bytes.value(), std::uint64_t{65536});
    const auto work_items = std::min(
      config.max_work_items.value(), std::uint64_t{256});
    const auto fragments = static_cast<std::uint64_t>(payload.fragment_count());
    // Covers our cost/packing scans, the builder's preflight and transfer, and
    // descriptor migration. Final publication is its separate bounded leaf.
    if (
      *total > logical_cap || prefix.size() > work_bytes
      || fragments > work_items / 8U) {
        return codec::failure(staging_error(errc::resource_exhausted, context));
    }
    const auto input_cost = payload.allocation_cost(charge);
    if (!input_cost) {
        return codec::failure(
          staging_error(accounting_error(input_cost.error()), context));
    }
    const auto valid_input = policy.validate_buffer(
      payload.size(), input_cost->backing, input_cost->fragments, logical_cap);
    if (!valid_input) {
        return codec::failure(
          staging_error(accounting_error(valid_input.error()), context));
    }
    auto live = other_live;
    for (const auto cost :
         {input_cost->descriptors, input_cost->share_controls}) {
        if (
          auto added = add_charge(live.payload_bookkeeping, cost, context);
          !added) {
            return codec::failure(added.error());
        }
    }
    if (
      auto added = add_charge(
        live.retained_input, input_cost->backing, context);
      !added) {
        return codec::failure(added.error());
    }
    const auto remaining = policy.remaining_operation_bytes(
      live, parent_remaining);
    if (!remaining) {
        return codec::failure(
          staging_error(accounting_error(remaining.error()), context));
    }
    if (total->value() == 0) {
        return bytes::fragmented_buffer{};
    }

    // A verified monotone charge bound lets a ceiling bound every smaller
    // request the existing builder's growth policy can make. Reduce it when
    // served rounding or the live aggregate would exceed an allowance.
    auto ceiling = std::min(
      {total->value(),
       config.max_allocation_bytes.value(),
       remaining->value()});
    auto work_spent = fragments;
    while (ceiling != 0) {
        const auto served_tail = charge(byte_count{ceiling});
        if (served_tail < byte_count{ceiling}) {
            return codec::failure(
              staging_error(errc::invalid_argument, context));
        }
        const auto prefix_fragments = prefix.empty()
                                        ? 0U
                                        : 1U + (prefix.size() - 1U) / ceiling;
        const auto nodes = fragments + prefix_fragments;
        if (
          nodes > config.max_buffer_fragments.value()
          || nodes > work_items / 8U) {
            // Decreasing the ceiling can only increase this work/count bound.
            return codec::failure(
              staging_error(errc::resource_exhausted, context));
        }
        if (fragments > work_items - work_spent) {
            return codec::failure(
              staging_error(errc::resource_exhausted, context));
        }
        work_spent += fragments;
        if (6U * nodes > work_items - work_spent) {
            return codec::failure(
              staging_error(errc::resource_exhausted, context));
        }
        std::uint64_t packing_fragments = 0;
        std::uint64_t copied = prefix.size();
        for (const auto fragment : payload) {
            if (
              fragment.size()
                <= bytes::fragmented_buffer_builder::pack_copy_threshold.value()
              && fragment.size() <= ceiling) {
                ++packing_fragments;
                copied += fragment.size();
            }
        }
        if (copied > work_bytes) {
            // Smaller ceilings can convert a small donation into a no-copy
            // splice, so continue searching within the same finite bound.
            ceiling /= 2U;
            continue;
        }
        const auto new_tails = prefix_fragments + packing_fragments;
        const auto backing = multiply_charge(served_tail, new_tails, context);
        const byte_count descriptor_request{
          2U * nodes * bytes::fragmented_buffer::fragment_descriptor_size()};
        const auto descriptor = charge(descriptor_request);
        if (descriptor < descriptor_request) {
            return codec::failure(
              staging_error(errc::invalid_argument, context));
        }
        const auto descriptor_peak = multiply_charge(descriptor, 2, context);
        if (!backing || !descriptor_peak) {
            return codec::failure(staging_error(errc::out_of_range, context));
        }
        const auto output_backing = input_cost->backing.checked_add(*backing);
        if (!output_backing) {
            return codec::failure(staging_error(errc::out_of_range, context));
        }
        auto projected = live;
        if (
          auto added = add_charge(projected.staged_output, *backing, context);
          !added) {
            return codec::failure(added.error());
        }
        if (
          auto added = add_charge(
            projected.payload_bookkeeping, *descriptor_peak, context);
          !added) {
            return codec::failure(added.error());
        }
        const auto admitted = policy.remaining_operation_bytes(
          projected, parent_remaining);
        if (!admitted && admitted.error() != errc::resource_exhausted) {
            return codec::failure(
              staging_error(accounting_error(admitted.error()), context));
        }
        if (
          (new_tails == 0 || served_tail <= config.max_allocation_bytes)
          && descriptor <= config.max_allocation_bytes
          && *output_backing <= config.max_retained_bytes && admitted) {
            bytes::fragmented_buffer_builder_config builder_config;
            builder_config.initial_fragment_bytes = byte_count{
              std::min(ceiling, std::uint64_t{512})};
            builder_config.max_fragment_bytes = byte_count{ceiling};
            builder_config.max_total_bytes = *total;
            builder_config.max_retained_bytes = config.max_retained_bytes;
            builder_config.max_fragments = static_cast<std::size_t>(
              config.max_buffer_fragments.value());
            bytes::fragmented_buffer_builder output{builder_config};
            const auto copied_prefix = output.append(prefix);
            KWAQUE_INVARIANT(
              invariant_id{"KQ-CODEC-STAGING-PREFIX"},
              copied_prefix.has_value(),
              "admitted prefix append failed");
            const auto spliced = output.append_buffer(std::move(payload));
            KWAQUE_INVARIANT(
              invariant_id{"KQ-CODEC-STAGING-SPLICE"},
              spliced.has_value(),
              "admitted payload splice failed");
            auto finished = output.finish();
            KWAQUE_INVARIANT(
              invariant_id{"KQ-CODEC-STAGING-FINISH"},
              finished.has_value(),
              "admitted private publication failed");
            KWAQUE_INVARIANT(
              invariant_id{"KQ-CODEC-STAGING-SIZE"},
              finished->size() == *total,
              "published buffer size differs from admitted size");
            return std::move(*finished);
        }
        ceiling /= 2U;
    }
    return codec::failure(staging_error(errc::resource_exhausted, context));
}

} // namespace kwaque::codec
