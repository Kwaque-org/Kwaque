#include "src/codec/envelope_encode.h"

#include "src/base/error.h"
#include "src/base/invariant.h"
#include "src/codec/buffer_cost_internal.h"
#include "src/codec/crc32c.h"
#include "src/codec/crc32c_cooperative.h"
#include "src/codec/framing_internal.h"
#include "src/codec/staging_cooperative.h"
#include "src/codec/transaction.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <cstdint>
#include <exception>
#include <optional>
#include <span>
#include <utility>

namespace kwaque::codec {
namespace {

using bytes::fragmented_buffer;

using detail::buffer_input_cost;
using detail::framing::add_charge;
using detail::framing::admit_header_owner;
using detail::framing::admit_usage;
using detail::framing::encode_error;

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
    const auto body_cost = co_await buffer_input_cost(
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
    const error body_anchor{
      errc::success,
      context.family,
      static_cast<std::uint16_t>(envelope_field::body_crc32c),
      context.origin + 24};
    const auto body_checksum = co_await crc32c_borrowed(
      body, work, 0, body_anchor);
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
        byte_count{envelope_prefix_bytes},
        *body_cost,
        body_live,
        policy,
        parent_remaining,
        charge,
        context);
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
    // Assembly admits the actual header/body
    // input owners and its pre-reserved output descriptors, using the ORIGINAL
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
