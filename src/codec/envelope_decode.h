#pragma once

#include "src/base/invariant.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/cooperative.h"
#include "src/codec/envelope.h"
#include "src/codec/error.h"
#include "src/codec/format_registry.h"
#include "src/codec/integer.h"
#include "src/codec/transaction.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <concepts>
#include <exception>
#include <functional>
#include <optional>
#include <type_traits>
#include <utility>

namespace kwaque::codec {
namespace detail {

// Internal framing result, not a decoded body or a transferable validation
// capability. Its producer leaves the caller's transaction at the body start.
struct envelope_body_extent final {
    byte_count bytes;
    byte_count encoded_bytes;
    field_context context;
};

[[nodiscard]] seastar::future<result<envelope_body_extent>>
inspect_envelope_in_transaction(
  bytes::fragmented_buffer_parser& input,
  format_family expected_family,
  envelope_extent_limits owner_limits,
  decode_budget memory,
  cooperative_work& work,
  field_context context,
  input_boundary boundary);

// The parent is already reserved. Only additional child descriptors are
// debited; reported backing is checked without charging its known alias twice.
[[nodiscard]] result<decode_budget> admit_envelope_child(
  const bytes::fragmented_buffer_parser& input,
  byte_count bytes,
  byte_count logical_limit,
  decode_budget memory,
  const limits& policy,
  field_context context);

template<typename T, typename Decoder>
concept envelope_body_decoder
  = owning_decode_result<result<T>>::value
    && std::constructible_from<std::decay_t<Decoder>, Decoder&&>
    && std::is_nothrow_destructible_v<std::decay_t<Decoder>>
    && std::invocable<
      std::decay_t<Decoder>&,
      bytes::fragmented_buffer_parser&,
      field_context,
      input_boundary,
      decode_budget,
      cooperative_work&>
    && std::same_as<
      std::invoke_result_t<
        std::decay_t<Decoder>&,
        bytes::fragmented_buffer_parser&,
        field_context,
        input_boundary,
        decode_budget,
        cooperative_work&>,
      seastar::future<result<T>>>;

} // namespace detail

// Decode one envelope under a single parent transaction. The static body
// decoder is invoked only after header/body integrity, compatibility and the
// independently requested family pass. It receives an exact complete body,
// absolute coordinates and the remaining shared memory/work allowances.
// Successful decoding consumes precisely this envelope; rejection, exceptions
// and observed cancellation restore the original parent position and marks.
// The body decoder must leave its child extent unchanged and marks balanced.
// It may not access the parent through another alias or publish partial state.
// It owns/drains its private staging on failure and returns an owning value;
// direct-borrow checks do not inspect arbitrary aggregate members.
//
// Before invoking this coroutine, reserve_decode_input must have reserved this
// parent's backing/descriptors/potential share controls ONCE for its lifetime.
// memory is the residual after that reservation and verified native CRC,
// coroutine-frame, callback and other live costs. The decoder admits only its
// additional aliases and gives the body callback the reduced residual. The
// caller retains reservations for returned owners and for persistent parent
// share promotion, including on failure. No allocator/frame introspection or
// implicit refund is performed. Parent, work, abort source and all explicitly
// borrowed callback state stay alive, unmoved and exclusive through completion.
//
// The native coroutine starts synchronously. The forwarding-reference argument
// is used only to construct frame-owned callback storage before the first
// await; it is never accessed afterward. That storage and temporary child
// ownership are destroyed before the final abort poll/commit. Callback/result
// destruction must have reviewed bounded work; moving the final result must
// leave an inert source. Native bounded slice/publication/release operations
// retain the substrate's explicit fragment ceiling and are bracketed by
// shared-work checkpoints.
template<typename T, typename Decoder>
requires detail::envelope_body_decoder<T, Decoder>
[[nodiscard]] seastar::future<result<T>> decode_envelope(
  bytes::fragmented_buffer_parser& input,
  format_family expected_family,
  envelope_extent_limits owner_limits,
  decode_budget memory,
  cooperative_work& work,
  Decoder&& decoder,
  field_context context = {},
  input_boundary boundary = input_boundary::open) {
    const auto start = detail::integer_read_start(input, context, boundary);
    if (!start) {
        co_return codec::failure(start.error());
    }
    const error anchor{errc::success, context.family, context.field, *start};
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    if (
      memory.charge == nullptr
      || !lookup_format(static_cast<std::uint16_t>(expected_family))) {
        co_return codec::failure(
          error{errc::invalid_argument, context.family, context.field, *start});
    }

    // No reference to the constructor argument is used across suspension.
    std::optional<std::decay_t<Decoder>> callback{
      std::in_place, std::forward<Decoder>(decoder)};
    const auto entry_depth = input.checkpoint_depth();
    const auto entry_position = input.bytes_consumed();
    const auto entry_extent = input.total_bytes();
    if (auto marked = input.push_checkpoint(); !marked) {
        co_return codec::failure(
          detail::allocation_cost_error(marked.error(), context, *start));
    }
    detail::parser_transaction_guard transaction{input, entry_depth};
    std::optional<bytes::fragmented_buffer_parser> child;
    std::optional<result<T>> outcome;
    std::optional<error> failed;
    std::exception_ptr exception;
    byte_count encoded_bytes;
    byte_count body_bytes;
    byte_count parent_body_position;
    field_context body_context;

    try {
        do {
            const auto framing
              = co_await detail::inspect_envelope_in_transaction(
                input,
                expected_family,
                owner_limits,
                memory,
                work,
                context,
                boundary);
            if (!framing) {
                failed = framing.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            encoded_bytes = framing->encoded_bytes;
            body_bytes = framing->bytes;
            body_context = framing->context;
            parent_body_position = input.bytes_consumed();
            // Cost lookup and native slice construction are bounded substrate
            // leaves. No child allocation occurs before this admission.
            if (auto ready = co_await work.checkpoint(anchor); !ready) {
                failed = ready.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            const auto remaining = detail::admit_envelope_child(
              input, body_bytes, body_bytes, memory, work.policy(), context);
            if (!remaining) {
                failed = remaining.error();
                break;
            }
            auto shared = input.peek_buffer(body_bytes);
            if (!shared) {
                failed = detail::allocation_cost_error(
                  shared.error(), body_context, body_context.origin);
                break;
            }
            child.emplace(std::move(*shared));
            if (auto ready = co_await work.checkpoint(anchor); !ready) {
                failed = ready.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            outcome.emplace(
              co_await std::invoke(
                *callback,
                *child,
                body_context,
                input_boundary::complete,
                *remaining,
                work));
            KWAQUE_INVARIANT(
              invariant_id{"KQ-ENVELOPE-CHILD-EXTENT"},
              child->total_bytes() == body_bytes,
              "body decoder replaced its exact child extent");
            KWAQUE_INVARIANT(
              invariant_id{"KQ-ENVELOPE-CHILD-MARKS"},
              child->checkpoint_depth() == 0,
              "body decoder left unresolved child marks");
            KWAQUE_INVARIANT(
              invariant_id{"KQ-ENVELOPE-PARENT"},
              input.bytes_consumed() == parent_body_position
                && input.total_bytes() == entry_extent
                && input.checkpoint_depth() == entry_depth + 1U,
              "body decoder changed its enclosing parser");
            if (!outcome->has_value()) {
                const auto failure = outcome->error();
                failed = failure.code() == errc::truncated_data
                           ? error{
                               errc::malformed_data,
                               failure.family(),
                               failure.field(),
                               failure.byte_offset()}
                           : failure;
            } else if (!child->at_end()) {
                failed = error{
                  errc::malformed_data,
                  context.family,
                  static_cast<std::uint16_t>(envelope_field::body_bytes),
                  body_context.origin + child->bytes_consumed().value()};
            }
        } while (false);
    } catch (...) {
        exception = std::current_exception();
    }

    if (child) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        child.reset();
    }
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    callback.reset();
    if (!failed && !exception) {
        if (auto ready = work.poll(anchor); !ready) {
            failed = ready.error();
        }
    }
    if (failed || exception) {
        if (outcome) {
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            outcome.reset();
        }
        if (exception) {
            std::rethrow_exception(exception);
        }
        co_return codec::failure(*failed);
    }
    KWAQUE_INVARIANT(
      invariant_id{"KQ-ENVELOPE-OUTCOME"},
      outcome.has_value() && outcome->has_value(),
      "envelope completed without a decoded result");
    KWAQUE_INVARIANT(
      invariant_id{"KQ-ENVELOPE-FINAL-PARENT"},
      input.total_bytes() == entry_extent
        && input.bytes_consumed() == parent_body_position
        && input.checkpoint_depth() == entry_depth + 1U,
      "temporary decoder cleanup changed its enclosing parser");
    // The final poll above, advancement, commit and result transfer have no
    // intervening suspension. Only inert moved-from state remains afterward.
    const auto advanced = input.skip(body_bytes);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-ENVELOPE-COMMIT"},
      advanced.has_value()
        && input.bytes_consumed().value() - entry_position.value()
             == encoded_bytes.value(),
      "validated envelope could not commit its exact extent");
    transaction.commit();
    co_return std::move(*outcome);
}

} // namespace kwaque::codec
