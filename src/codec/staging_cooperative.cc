#include "src/codec/staging_cooperative.h"

#include "src/base/invariant.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/codec/buffer_cost_internal.h"
#include "src/codec/transaction.h"

#include <seastar/core/coroutine.hh>

#include <array>
#include <cstdint>
#include <exception>
#include <limits>
#include <optional>
#include <span>
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

// Sources are published owners, so assembly transfers whole fragments.
// Only the preallocated output descriptors are new.
result<item_count> admit_assembly_descriptors(
  std::uint64_t nodes,
  const limits& policy,
  operation_usage live,
  byte_count input_backing,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  field_context context) {
    const auto config = policy.config();
    if (
      nodes > config.max_buffer_fragments.value()
      || input_backing > config.max_retained_bytes) {
        return codec::failure(at(errc::resource_exhausted, context));
    }
    const byte_count descriptor_request{
      nodes * fragmented_buffer::fragment_descriptor_size()};
    const auto descriptors = charge(descriptor_request);
    if (descriptors < descriptor_request) {
        return codec::failure(at(errc::invalid_argument, context));
    }
    if (
      auto added = add(live.payload_bookkeeping, descriptors, context); !added)
        return codec::failure(added.error());
    const auto fits = policy.remaining_operation_bytes(live, parent_remaining);
    if (!fits) {
        return codec::failure(
          detail::allocation_cost_error(fits.error(), context, context.origin));
    }
    if (descriptors > config.max_allocation_bytes) {
        return codec::failure(at(errc::resource_exhausted, context));
    }
    return item_count{nodes};
}

seastar::future<result<void>> splice_input(
  fragmented_buffer& source,
  fragmented_buffer_builder& output,
  cooperative_work& work,
  field_context context) {
    const auto anchor = at(errc::success, context);
    while (!source.empty()) {
        const auto count = std::min<std::uint64_t>(
          source.fragment_count(), work.item_quantum().value() / 8U);
        const auto admitted = co_await work.admit(
          byte_count{}, item_count{8U * count}, anchor);
        if (!admitted) {
            co_return codec::failure(admitted.error());
        }
        if (auto ready = work.poll(anchor); !ready) {
            co_return codec::failure(ready.error());
        }
        const auto appended = output.append_fragments(
          source, item_count{count});
        if (!appended) {
            co_return codec::failure(
              detail::allocation_cost_error(
                appended.error(), context, context.origin));
        }
    }
    co_return result<void>{};
}

seastar::future<result<fragmented_buffer>> assemble_owned(
  std::span<fragmented_buffer> inputs,
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
    std::optional<byte_count> total{byte_count{}};
    std::size_t fragments = 0;
    for (const auto& input : inputs) {
        if (total) total = total->checked_add(input.size());
        fragments += input.fragment_count();
    }
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
    for (const auto& input : inputs) {
        const auto* source = &input;
        const auto cost = co_await detail::buffer_input_cost(
          *source, work, charge, context);
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
    if (work.item_quantum().value() < 8) {
        co_return codec::failure(at(errc::resource_exhausted, context));
    }
    const auto nodes = admit_assembly_descriptors(
      fragments,
      policy,
      live,
      input_backing,
      parent_remaining,
      charge,
      context);
    if (!nodes) co_return codec::failure(nodes.error());
    const auto setup = co_await work.admit(byte_count{}, item_count{1}, anchor);
    if (!setup) {
        co_return codec::failure(setup.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    bytes::fragmented_buffer_builder_config config;
    config.initial_fragment_bytes = byte_count{1};
    config.max_fragment_bytes = byte_count{1};
    config.max_total_bytes = *total;
    config.max_retained_bytes = policy.config().max_retained_bytes;
    config.max_fragments = static_cast<std::size_t>(nodes->value());
    output.emplace(config);
    const auto reserved = output->reserve_fragments(*nodes);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CODEC-STAGING-RESERVE"},
      reserved.has_value(),
      "admitted descriptor reservation failed");
    for (auto& source : inputs) {
        const auto appended = co_await splice_input(
          source, *output, work, context);
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

template<std::size_t N>
seastar::future<result<fragmented_buffer>> assemble_inputs(
  std::array<fragmented_buffer*, N> sources,
  cooperative_work& work,
  byte_count logical_cap,
  operation_usage other_live,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  field_context context) {
    static_assert(N >= 2 && N <= 4);
    for (std::size_t i = 0; i < N; ++i)
        for (std::size_t j = 0; j < i; ++j)
            if (sources[i] == sources[j])
                co_return codec::failure(at(errc::invalid_argument, context));
    // Native initial_suspend is suspend_never: references to caller inputs are
    // consumed into this frame before any suspension. Frame-allocation failure
    // precedes these moves and leaves the caller's owners intact.
    std::array<fragmented_buffer, N> owned;
    for (std::size_t i = 0; i < N; ++i)
        owned[i] = std::move(*sources[i]);
    std::optional<fragmented_buffer_builder> output;
    std::optional<result<fragmented_buffer>> produced;
    std::exception_ptr exception;
    try {
        produced.emplace(
          co_await assemble_owned(
            owned,
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
    for (auto& input : owned) {
        const bool nonempty = !input.empty();
        co_await work.drain_inline(
          nonempty ? work.byte_quantum() : byte_count{},
          nonempty ? work.item_quantum() : item_count{1});
        input = fragmented_buffer{};
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
    return assemble_inputs(
      std::array{&prefix, &payload},
      work,
      logical_cap,
      other_live,
      parent_remaining,
      charge,
      context);
}

seastar::future<result<bytes::fragmented_buffer>> assemble_buffer_cooperatively(
  bytes::fragmented_buffer&& prefix,
  bytes::fragmented_buffer&& payload,
  bytes::fragmented_buffer&& suffix,
  cooperative_work& work,
  byte_count logical_cap,
  operation_usage other_live,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  field_context context) {
    return assemble_inputs(
      std::array{&prefix, &payload, &suffix},
      work,
      logical_cap,
      other_live,
      parent_remaining,
      charge,
      context);
}
seastar::future<result<bytes::fragmented_buffer>> assemble_buffer_cooperatively(
  bytes::fragmented_buffer&& prefix,
  bytes::fragmented_buffer&& fixed,
  bytes::fragmented_buffer&& payload,
  bytes::fragmented_buffer&& suffix,
  cooperative_work& work,
  byte_count logical_cap,
  operation_usage other_live,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  field_context context) {
    return assemble_inputs(
      std::array{&prefix, &fixed, &payload, &suffix},
      work,
      logical_cap,
      other_live,
      parent_remaining,
      charge,
      context);
}

} // namespace kwaque::codec
