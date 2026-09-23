#pragma once

#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/codec/cooperative.h"
#include "src/codec/error.h"
#include "src/codec/integer.h"
#include "src/codec/transaction.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <algorithm>
#include <cstddef>

namespace kwaque::codec::detail {

// Borrow the owner's immutable buffer for this joined scan. Divide native
// descriptor queries before their synchronous loops; charge descriptor history
// once, including an empty buffer that retains descriptor storage. Keeping the
// coroutine body visible lets each owning caller retain allocation elision.
[[nodiscard]] inline seastar::future<result<bytes::buffer_allocation_cost>>
buffer_input_cost(
  const bytes::fragmented_buffer& input,
  cooperative_work& work,
  bytes::allocation_charge_fn charge,
  field_context context) {
    const error anchor{
      errc::success, context.family, context.field, context.origin};
    bytes::buffer_allocation_cost total;
    byte_count aggregate;
    const auto quantum = work.item_quantum().value();
    if (!input.empty() && quantum < 6) {
        co_return codec::failure(
          error{
            errc::resource_exhausted,
            context.family,
            context.field,
            context.origin});
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
              allocation_cost_error(part.error(), context, context.origin));
        }
        for (const auto amount :
             {part->backing, part->descriptors, part->share_controls}) {
            const auto next = aggregate.checked_add(amount);
            if (!next) {
                co_return codec::failure(
                  error{
                    errc::out_of_range,
                    context.family,
                    context.field,
                    context.origin});
            }
            aggregate = *next;
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

} // namespace kwaque::codec::detail
