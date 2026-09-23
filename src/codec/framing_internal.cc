#include "src/codec/framing_internal.h"

#include "src/base/error.h"
#include "src/base/invariant.h"
#include "src/codec/transaction.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/deleter.hh>

#include <array>
#include <cstddef>
#include <cstdint>

namespace kwaque::codec::detail::framing {

using bytes::fragmented_buffer;

error encode_error(errc reason, field_context context) noexcept {
    return error{reason, context.family, context.field, context.origin};
}

result<void> add_charge(
  byte_count& target, byte_count amount, field_context context) noexcept {
    const auto sum = target.checked_add(amount);
    if (!sum) {
        return codec::failure(encode_error(errc::out_of_range, context));
    }
    target = *sum;
    return {};
}

result<void> admit_usage(
  const operation_usage& live,
  const limits& policy,
  byte_count parent_remaining,
  field_context context) {
    const auto remaining = policy.remaining_operation_bytes(
      live, parent_remaining);
    if (!remaining) {
        return codec::failure(
          detail::allocation_cost_error(
            remaining.error(), context, context.origin));
    }
    return {};
}

// A slice does not allocate its backing again. Gate the same conservative
// descriptor request used by slice_allocation_cost, plus native promotion;
// its aggregate descriptor peak is charged separately to operation usage.
result<byte_count> admit_alias_allocations(
  const bytes::buffer_allocation_cost& cost,
  const limits& policy,
  bytes::allocation_charge_fn charge,
  field_context context) {
    if (cost.fragments.value() == 0) {
        return byte_count{};
    }
    KWAQUE_INVARIANT(
      invariant_id{"KQ-FRAMING-WRITER-FRAGMENTS"},
      cost.fragments.value() <= bytes::max_buffer_fragments,
      "checksum alias exceeds the substrate fragment ceiling");
    const std::array requests{
      byte_count{
        2U * cost.fragments.value()
        * fragmented_buffer::fragment_descriptor_size()},
      byte_count{sizeof(seastar::free_deleter_impl)}};
    std::array<byte_count, requests.size()> served{};
    for (std::size_t index = 0; index < requests.size(); ++index) {
        served[index] = charge(requests[index]);
        if (served[index] < requests[index]) {
            return codec::failure(
              encode_error(errc::invalid_argument, context));
        }
        if (auto valid = policy.validate_allocation(served[index]); !valid) {
            return codec::failure(
              detail::allocation_cost_error(
                valid.error(), context, context.origin));
        }
    }
    const auto peak = served[0].checked_add(served[0]);
    if (!peak) {
        return codec::failure(encode_error(errc::out_of_range, context));
    }
    return *peak;
}

// copy_of(prefix_bytes) uses one native temporary_buffer allocation and one
// exact fresh descriptor reservation. Its later sharing uses the native
// free-deleter control already included by the substrate's allocation-cost
// contract.
result<void> admit_header_owner(
  byte_count prefix_bytes,
  const bytes::buffer_allocation_cost& body_cost,
  operation_usage live,
  const limits& policy,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  field_context context) {
    const std::array requests{
      prefix_bytes,
      byte_count{fragmented_buffer::fragment_descriptor_size()},
      byte_count{sizeof(seastar::free_deleter_impl)}};
    std::array<byte_count, requests.size()> charges{};
    for (std::size_t index = 0; index < requests.size(); ++index) {
        charges[index] = charge(requests[index]);
        if (charges[index] < requests[index]) {
            return codec::failure(
              encode_error(errc::invalid_argument, context));
        }
        if (
          const auto valid = policy.validate_allocation(charges[index]);
          !valid) {
            return codec::failure(
              detail::allocation_cost_error(
                valid.error(), context, context.origin));
        }
    }
    const auto retained = body_cost.backing.checked_add(charges[0]);
    if (!retained) {
        return codec::failure(encode_error(errc::out_of_range, context));
    }
    if (*retained > policy.config().max_retained_bytes) {
        return codec::failure(encode_error(errc::resource_exhausted, context));
    }
    if (
      auto added = add_charge(live.retained_input, charges[0], context);
      !added) {
        return added;
    }
    for (const auto cost : {charges[1], charges[2]}) {
        if (
          auto added = add_charge(live.payload_bookkeeping, cost, context);
          !added) {
            return added;
        }
    }
    return admit_usage(live, policy, parent_remaining, context);
}

} // namespace kwaque::codec::detail::framing
