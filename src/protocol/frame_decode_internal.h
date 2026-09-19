#pragma once

#include "src/base/invariant.h"
#include "src/codec/envelope_decode.h"
#include "src/protocol/frame_codec.h"

#include <seastar/core/coroutine.hh>

#include <concepts>
#include <exception>
#include <functional>
#include <optional>
#include <type_traits>
#include <utility>

namespace kwaque::protocol::detail {

[[nodiscard]] inline codec::error frame_error(
  errc reason,
  codec::field_context context,
  frame_field field,
  std::uint64_t offset) noexcept {
    return codec::error{
      reason, context.family, static_cast<std::uint16_t>(field), offset};
}

// Caller holds one transaction. Complete header success leaves input at the
// payload start; shortages retain no payload owner and never run its checksum.
[[nodiscard]] seastar::future<frame_read_result<frame_header>>
inspect_frame_header_in_transaction(
  bytes::fragmented_buffer_parser& input,
  frame_extent_limits owner_limits,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context,
  codec::input_boundary boundary,
  std::uint64_t start);

[[nodiscard]] seastar::future<frame_read_result<frame_header>>
inspect_frame_in_transaction(
  bytes::fragmented_buffer_parser& input,
  std::optional<frame_kind> expected_kind,
  frame_extent_limits owner_limits,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context,
  codec::input_boundary boundary,
  std::uint64_t start);

// Called only after the temporary payload parser has died. Both remainders
// were derived from the same normalized input budget. Restore its descriptors,
// preserving every charge still held by the result and original input owner.
[[nodiscard]] codec::decode_budget release_payload_parser_charge(
  codec::decode_budget original,
  codec::decode_budget child,
  codec::decode_budget result) noexcept;

template<typename T, typename Decoder>
concept frame_payload_decoder = requires(
  T& value,
  std::decay_t<Decoder>& decoder,
  bytes::fragmented_buffer_parser& input,
  const frame_header& header,
  codec::field_context context,
  codec::decode_budget memory,
  codec::cooperative_work& work) {
    requires codec::detail::owning_decode_result<codec::result<T>>::value;
    requires std::constructible_from<std::decay_t<Decoder>, Decoder&&>;
    requires std::is_nothrow_destructible_v<std::decay_t<Decoder>>;
    { value.remaining } -> std::same_as<codec::decode_budget&>;
    {
        std::invoke(decoder, input, header, context, memory, work)
    } -> std::same_as<seastar::future<codec::result<T>>>;
};

// Private static composition only: one parent transaction, an exact child,
// frame-owned callback, joined cleanup, then final poll/commit. Callbacks must
// leave child extent/marks intact, consume it exactly on success, and report
// persistent result charges relative to the supplied residual. No callback may
// access the parent or publish state. Direct type checks cannot inspect borrows
// hidden inside aggregates; owning consumers are reviewed with their adapters.
template<typename T, typename Decoder>
requires frame_payload_decoder<T, Decoder>
[[nodiscard]] seastar::future<frame_read_result<T>> decode_frame_payload(
  bytes::fragmented_buffer_parser& input,
  std::optional<frame_kind> expected_kind,
  frame_extent_limits owner_limits,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  Decoder&& decoder,
  codec::field_context context,
  codec::input_boundary boundary) {
    const auto start = codec::detail::integer_read_start(
      input, context, boundary);
    if (!start) co_return codec::failure(start.error());
    const auto anchor = frame_error(
      errc::success, context, frame_field::magic, *start);
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (memory.charge == nullptr)
        co_return codec::failure(frame_error(
          errc::invalid_argument, context, frame_field::magic, *start));
    const auto normalized = codec::detail::consume_decode_budget(
      work.policy(), memory, byte_count{}, byte_count{}, context, *start);
    if (!normalized) co_return codec::failure(normalized.error());
    memory = *normalized;
    std::optional<std::decay_t<Decoder>> callback{
      std::in_place, std::forward<Decoder>(decoder)};
    const auto entry_depth = input.checkpoint_depth();
    const auto entry_position = input.bytes_consumed();
    const auto entry_extent = input.total_bytes();
    if (auto marked = input.push_checkpoint(); !marked)
        co_return codec::failure(
          codec::detail::allocation_cost_error(
            marked.error(), context, *start));
    codec::detail::parser_transaction_guard transaction{input, entry_depth};
    std::optional<bytes::fragmented_buffer_parser> child;
    std::optional<codec::result<T>> outcome;
    std::optional<codec::error> failed;
    std::optional<need_more> incomplete;
    std::exception_ptr exception;
    byte_count encoded_bytes;
    byte_count body_bytes;
    byte_count parent_body_position;
    codec::field_context body_context;
    codec::decode_budget child_memory;

    try {
        do {
            const auto inspected = co_await inspect_frame_in_transaction(
              input,
              expected_kind,
              owner_limits,
              memory,
              work,
              context,
              boundary,
              *start);
            if (!inspected) {
                failed = inspected.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            if (const auto* more = std::get_if<need_more>(&*inspected)) {
                incomplete = *more;
                break;
            }
            const auto& header = std::get<frame_header>(*inspected);
            body_bytes = header.payload_bytes;
            encoded_bytes = byte_count{
              header.header_bytes.value() + body_bytes.value()};
            body_context = {
              .origin = *start + header.header_bytes.value(),
              .family = context.family,
              .field = static_cast<std::uint16_t>(frame_field::payload_bytes)};
            parent_body_position = input.bytes_consumed();
            if (auto ready = co_await work.checkpoint(anchor); !ready) {
                failed = ready.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            const auto remaining = codec::detail::admit_envelope_child(
              input, body_bytes, body_bytes, memory, work.policy(), context);
            if (!remaining) {
                failed = remaining.error();
                break;
            }
            child_memory = *remaining;
            auto shared = input.peek_buffer(body_bytes);
            if (!shared) {
                failed = codec::detail::allocation_cost_error(
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
                *callback, *child, header, body_context, child_memory, work));
            KWAQUE_INVARIANT(
              invariant_id{"KQ-FRAME-CHILD-EXTENT"},
              child->total_bytes() == body_bytes,
              "payload decoder replaced its exact extent");
            KWAQUE_INVARIANT(
              invariant_id{"KQ-FRAME-CHILD-MARKS"},
              child->checkpoint_depth() == 0,
              "payload decoder left unresolved marks");
            KWAQUE_INVARIANT(
              invariant_id{"KQ-FRAME-PARENT"},
              input.bytes_consumed() == parent_body_position
                && input.total_bytes() == entry_extent
                && input.checkpoint_depth() == entry_depth + 1U,
              "payload decoder changed its enclosing parser");
            if (!outcome->has_value()) {
                const auto error = outcome->error();
                failed = error.code() == errc::truncated_data
                  ? codec::error{errc::malformed_data, error.family(), error.field(), error.byte_offset()}
                  : error;
            } else if (!child->at_end()) {
                failed = frame_error(
                  errc::malformed_data,
                  context,
                  frame_field::payload_bytes,
                  body_context.origin + child->bytes_consumed().value());
            }
        } while (false);
    } catch (...) {
        exception = std::current_exception();
    }

    if (child) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        child.reset();
        if (outcome && outcome->has_value())
            (**outcome).remaining = release_payload_parser_charge(
              memory, child_memory, (**outcome).remaining);
    }
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    callback.reset();
    if (!failed && !exception) {
        if (auto ready = work.poll(anchor); !ready) failed = ready.error();
    }
    if (failed || exception) {
        if (outcome) {
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            outcome.reset();
        }
        if (exception) std::rethrow_exception(exception);
        co_return codec::failure(*failed);
    }
    if (incomplete) co_return *incomplete;
    KWAQUE_INVARIANT(
      invariant_id{"KQ-FRAME-OUTCOME"},
      outcome.has_value() && outcome->has_value(),
      "frame completed without a decoded result");
    KWAQUE_INVARIANT(
      invariant_id{"KQ-FRAME-FINAL-PARENT"},
      input.total_bytes() == entry_extent
        && input.bytes_consumed() == parent_body_position
        && input.checkpoint_depth() == entry_depth + 1U,
      "temporary decoder cleanup changed its enclosing parser");
    const auto advanced = input.skip(body_bytes);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-FRAME-COMMIT"},
      advanced.has_value()
        && input.bytes_consumed().value() - entry_position.value()
             == encoded_bytes.value(),
      "validated frame could not commit its exact extent");
    transaction.commit();
    co_return std::move(**outcome);
}

} // namespace kwaque::protocol::detail
