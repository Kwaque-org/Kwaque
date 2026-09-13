#include "src/codec/envelope_encode.h"

#include "src/base/error.h"
#include "src/base/invariant.h"
#include "src/codec/crc32c.h"
#include "src/codec/crc32c_cooperative.h"
#include "src/codec/staging_cooperative.h"
#include "src/codec/transaction.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/deleter.hh>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <optional>
#include <span>
#include <utility>

namespace kwaque::codec {
namespace {

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
      invariant_id{"KQ-ENVELOPE-WRITER-FRAGMENTS"},
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

// Keep the existing staging cost scan's bounded ranges and charge order. The
// first range includes retained descriptor capacity, even for an empty body.
seastar::future<result<bytes::buffer_allocation_cost>> body_input_cost(
  const fragmented_buffer& body,
  cooperative_work& work,
  bytes::allocation_charge_fn charge,
  field_context context) {
    const auto anchor = encode_error(errc::success, context);
    bytes::buffer_allocation_cost total;
    byte_count aggregate;
    const auto quantum = work.item_quantum().value();
    if (!body.empty() && quantum < 6) {
        co_return codec::failure(
          encode_error(errc::resource_exhausted, context));
    }
    const auto batch = quantum < 6 ? 1U : (quantum - 2U) / 4U;
    std::size_t first = 0;
    do {
        const auto count = std::min<std::size_t>(
          body.fragment_count() - first, batch);
        auto admitted = co_await work.admit(
          byte_count{}, item_count{count == 0 ? 1U : 4U * count + 2U}, anchor);
        if (!admitted) {
            co_return codec::failure(admitted.error());
        }
        if (auto ready = work.poll(anchor); !ready) {
            co_return codec::failure(ready.error());
        }
        const auto part = body.allocation_cost(first, count, charge);
        if (!part) {
            co_return codec::failure(
              detail::allocation_cost_error(
                part.error(), context, context.origin));
        }
        for (const auto amount :
             {part->backing, part->descriptors, part->share_controls}) {
            if (auto summed = add_charge(aggregate, amount, context); !summed) {
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
    } while (first != body.fragment_count());
    total.fragments = item_count{body.fragment_count()};
    co_return total;
}

// copy_of(32 bytes) uses one native temporary_buffer allocation and one exact
// fresh descriptor reservation. Its later sharing uses the native free-deleter
// control already included by the substrate's allocation-cost contract.
result<void> admit_header_owner(
  const bytes::buffer_allocation_cost& body_cost,
  operation_usage live,
  const limits& policy,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  field_context context) {
    constexpr std::array requests{
      byte_count{envelope_prefix_bytes},
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

seastar::future<result<fragmented_buffer>> encode_owned(
  fragmented_buffer& body,
  std::optional<fragmented_buffer>& header,
  format_family family,
  cooperative_work& work,
  envelope_extent_limits owner_limits,
  operation_usage other_live,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  field_context context) {
    const auto anchor = encode_error(errc::success, context);
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    if (charge == nullptr) {
        co_return codec::failure(encode_error(errc::invalid_argument, context));
    }
    const auto policy = work.policy();
    if (
      auto admitted = co_await work.admit(
        envelope_prefix_work_bytes, envelope_prefix_work_items, anchor);
      !admitted) {
        co_return codec::failure(admitted.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    // Preflight the existing writer profile and construct only private inline
    // placeholders. Neither checksum slot is complete or published yet.
    auto prefix = encode_envelope_prefix(
      envelope_prefix_fields{family, body.size(), 0, 0},
      policy,
      owner_limits,
      context);
    if (!prefix) {
        co_return codec::failure(prefix.error());
    }
    const auto total = body.size().checked_add(
      byte_count{envelope_prefix_bytes});
    KWAQUE_INVARIANT(
      invariant_id{"KQ-ENVELOPE-WRITER-EXTENT"},
      total.has_value(),
      "validated envelope size no longer fits its coordinate domain");
    if (auto ready = co_await work.checkpoint(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    const auto body_cost = co_await body_input_cost(
      body, work, charge, context);
    if (!body_cost) {
        co_return codec::failure(body_cost.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    if (
      auto valid = policy.validate_buffer(
        body.size(),
        body_cost->backing,
        body_cost->fragments,
        std::min(
          policy.config().max_encoded_body_bytes, owner_limits.max_body_bytes));
      !valid) {
        co_return codec::failure(
          detail::allocation_cost_error(
            valid.error(), context, context.origin));
    }
    auto body_live = other_live;
    if (
      auto added = add_charge(
        body_live.retained_input, body_cost->backing, context);
      !added) {
        co_return codec::failure(added.error());
    }
    for (const auto amount :
         {body_cost->descriptors, body_cost->share_controls}) {
        if (
          auto added = add_charge(
            body_live.payload_bookkeeping, amount, context);
          !added) {
            co_return codec::failure(added.error());
        }
    }
    if (
      auto admitted = admit_usage(body_live, policy, parent_remaining, context);
      !admitted) {
        co_return codec::failure(admitted.error());
    }
    if (auto ready = co_await work.checkpoint(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    const auto alias_descriptors = admit_alias_allocations(
      *body_cost, policy, charge, context);
    if (!alias_descriptors) {
        co_return codec::failure(alias_descriptors.error());
    }
    auto checksum_live = body_live;
    // The alias's backing and possible native promotion are already covered
    // by the original body reservation. Only additional descriptors overlap.
    if (
      auto added = add_charge(
        checksum_live.payload_bookkeeping, *alias_descriptors, context);
      !added) {
        co_return codec::failure(added.error());
    }
    if (
      auto admitted = admit_usage(
        checksum_live, policy, parent_remaining, context);
      !admitted) {
        co_return codec::failure(admitted.error());
    }
    if (auto ready = co_await work.checkpoint(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    auto checksum_input = body.share(byte_count{}, body.size());
    if (!checksum_input) {
        co_return codec::failure(
          detail::allocation_cost_error(
            checksum_input.error(), context, context.origin));
    }
    const error body_anchor{
      errc::success,
      context.family,
      static_cast<std::uint16_t>(envelope_field::body_crc32c),
      context.origin + 24};
    const auto body_checksum = co_await crc32c_cooperatively(
      std::move(*checksum_input), work, 0, body_anchor);
    if (!body_checksum) {
        co_return codec::failure(body_checksum.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    if (
      auto admitted = co_await work.admit(
        envelope_prefix_work_bytes, envelope_prefix_work_items, anchor);
      !admitted) {
        co_return codec::failure(admitted.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    // Retain native placeholder completion on PRIVATE inline bytes. The body
    // CRC is part of the header checksum; its own slot is still four zeros.
    seastar::write_le(prefix->data() + 24, *body_checksum);
    crc32c header_checksum;
    header_checksum.extend(std::span<const char>{*prefix});
    seastar::write_le(prefix->data() + 28, header_checksum.value());
    if (
      auto admitted = admit_header_owner(
        *body_cost, body_live, policy, parent_remaining, charge, context);
      !admitted) {
        co_return codec::failure(admitted.error());
    }
    if (
      auto admitted = co_await work.admit(
        envelope_prefix_work_bytes, envelope_prefix_work_items, anchor);
      !admitted) {
        co_return codec::failure(admitted.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    auto frozen_header = fragmented_buffer::copy_of(
      std::span<const char>{*prefix});
    if (!frozen_header) {
        co_return codec::failure(
          detail::allocation_cost_error(
            frozen_header.error(), context, context.origin));
    }
    header.emplace(std::move(*frozen_header));
    // Checksum-only aliases are gone. Assembly admits the actual header/body
    // input owners and its own output/slice/migration peaks, using the ORIGINAL
    // parent allowance, not a second debit of the stages already released.
    co_return co_await assemble_buffer_cooperatively(
      std::move(*header),
      std::move(body),
      work,
      *total,
      other_live,
      parent_remaining,
      charge,
      context);
}

} // namespace

seastar::future<result<bytes::fragmented_buffer>> encode_envelope(
  bytes::fragmented_buffer&& body,
  format_family family,
  cooperative_work& work,
  envelope_extent_limits owner_limits,
  operation_usage other_live,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  field_context context) {
    auto owned_body = std::move(body);
    std::optional<fragmented_buffer> header;
    std::optional<result<fragmented_buffer>> produced;
    std::exception_ptr exception;
    try {
        produced.emplace(
          co_await encode_owned(
            owned_body,
            header,
            family,
            work,
            owner_limits,
            other_live,
            parent_remaining,
            charge,
            context));
    } catch (...) {
        exception = std::current_exception();
    }
    if (header) {
        const bool nonempty = header->fragment_count() != 0;
        co_await work.drain_inline(
          nonempty ? work.byte_quantum() : byte_count{},
          nonempty ? work.item_quantum() : item_count{1});
        header.reset();
    }
    const bool nonempty = owned_body.fragment_count() != 0;
    co_await work.drain_inline(
      nonempty ? work.byte_quantum() : byte_count{},
      nonempty ? work.item_quantum() : item_count{1});
    owned_body = fragmented_buffer{};
    if (exception) {
        std::rethrow_exception(exception);
    }
    KWAQUE_INVARIANT(
      invariant_id{"KQ-ENVELOPE-WRITER-OUTCOME"},
      produced.has_value(),
      "envelope writer completed without a result or exception");
    if (!produced->has_value()) {
        co_return codec::failure(produced->error());
    }
    if (auto ready = work.poll(encode_error(errc::success, context)); !ready) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        produced.reset();
        co_return codec::failure(ready.error());
    }
    // Completed output moves only after every temporary owner has been drained.
    // There is no suspension between the final abort observation and transfer.
    co_return std::move(**produced);
}

} // namespace kwaque::codec
