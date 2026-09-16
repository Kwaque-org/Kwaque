#include "src/base/error.h"
#include "src/base/invariant.h"
#include "src/codec/crc32c.h"
#include "src/codec/crc32c_cooperative.h"
#include "src/codec/framing_internal.h"
#include "src/codec/staging_cooperative.h"
#include "src/codec/transaction.h"
#include "src/protocol/frame_codec.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <cstdint>
#include <exception>
#include <optional>
#include <span>
#include <utility>

namespace kwaque::protocol {
using codec::assemble_buffer_cooperatively;
using codec::cooperative_work;
using codec::crc32c;
using codec::crc32c_cooperatively;
using codec::error;
using codec::field_context;
using codec::operation_usage;
using codec::result;
namespace {

using bytes::fragmented_buffer;

using codec::detail::framing::add_charge;
using codec::detail::framing::admit_alias_allocations;
using codec::detail::framing::admit_header_owner;
using codec::detail::framing::admit_usage;
using codec::detail::framing::body_input_cost;
using codec::detail::framing::encode_error;

seastar::future<result<fragmented_buffer>> encode_owned(
  fragmented_buffer& body,
  std::optional<fragmented_buffer>& header,
  frame_metadata metadata,
  cooperative_work& work,
  frame_extent_limits owner_limits,
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
        frame_prefix_work_bytes, frame_prefix_work_items, anchor);
      !admitted) {
        co_return codec::failure(admitted.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    // Preflight the existing writer profile and construct only private inline
    // placeholders. Neither checksum slot is complete or published yet.
    auto prefix = encode_frame_prefix(
      frame_prefix_fields{metadata, body.size(), 0, 0},
      policy,
      owner_limits,
      context);
    if (!prefix) {
        co_return codec::failure(prefix.error());
    }
    const auto total = body.size().checked_add(byte_count{frame_prefix_bytes});
    KWAQUE_INVARIANT(
      invariant_id{"KQ-FRAME-WRITER-EXTENT"},
      total.has_value(),
      "validated frame size no longer fits its coordinate domain");
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
          policy.config().max_encoded_body_bytes,
          owner_limits.max_payload_bytes));
      !valid) {
        co_return codec::failure(
          codec::detail::allocation_cost_error(
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
          codec::detail::allocation_cost_error(
            checksum_input.error(), context, context.origin));
    }
    const error body_anchor{
      errc::success,
      context.family,
      static_cast<std::uint16_t>(frame_field::payload_crc32c),
      context.origin + 44};
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
        frame_prefix_work_bytes, frame_prefix_work_items, anchor);
      !admitted) {
        co_return codec::failure(admitted.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    // Retain native placeholder completion on PRIVATE inline bytes. The body
    // CRC is part of the header checksum; its own slot is still four zeros.
    seastar::write_le(prefix->data() + 44, *body_checksum);
    crc32c header_checksum;
    header_checksum.extend(std::span<const char>{*prefix});
    seastar::write_le(prefix->data() + 40, header_checksum.value());
    if (
      auto admitted = admit_header_owner(
        byte_count{frame_prefix_bytes},
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
        frame_prefix_work_bytes, frame_prefix_work_items, anchor);
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
          codec::detail::allocation_cost_error(
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

seastar::future<result<bytes::fragmented_buffer>> encode_frame(
  bytes::fragmented_buffer&& body,
  frame_metadata metadata,
  cooperative_work& work,
  frame_extent_limits owner_limits,
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
            metadata,
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
      invariant_id{"KQ-FRAME-WRITER-OUTCOME"},
      produced.has_value(),
      "frame writer completed without a result or exception");
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

} // namespace kwaque::protocol
