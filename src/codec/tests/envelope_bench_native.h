#pragma once

#include "src/base/error.h"
#include "src/base/invariant.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/envelope.h"
#include "src/codec/envelope_decode.h"
#include "src/codec/envelope_encode.h"
#include "src/codec/header_extensions.h"
#include "src/codec/staging_cooperative.h"
#include "src/codec/tests/codec_bench_reference.h"
#include "src/codec/transaction.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/deleter.hh>

#include <crc32c/crc32c.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <limits>
#include <optional>
#include <span>
#include <utility>

namespace kwaque::codec::bench::checked {

using codec::detail::admit_envelope_child;
using codec::detail::allocation_cost_error;
using codec::detail::envelope_body_extent;
using codec::detail::integer_read_start;
using codec::detail::integer_shortage;
using codec::detail::validate_sender_version_values;

template<typename T>
T native_load(const char* input) noexcept {
    T value;
    std::memcpy(&value, input, sizeof(value));
    return seastar::le_to_cpu(value);
}

template<typename T>
void native_store(char* output, T value) noexcept {
    value = seastar::cpu_to_le(value);
    std::memcpy(output, &value, sizeof(value));
}

class comparison_crc final {
public:
    explicit comparison_crc(std::uint32_t seed = 0) noexcept
      : value_(seed) {}
    std::uint32_t value() const noexcept { return value_; }
    void extend(std::span<const char> data) {
        if (!data.empty()) {
            value_ = ::crc32c::Extend(
              value_,
              reinterpret_cast<const std::uint8_t*>(data.data()),
              data.size());
        }
    }

private:
    std::uint32_t value_;
};
result<unverified_envelope_prefix> native_prefix_read(
  const bytes::fragmented_buffer_parser&,
  const limits&,
  envelope_extent_limits,
  field_context,
  input_boundary);
result<encoded_envelope_prefix> native_prefix_write(
  envelope_prefix_fields, const limits&, envelope_extent_limits, field_context);
seastar::future<result<item_count>> native_extensions(
  bytes::fragmented_buffer_parser&,
  byte_count,
  byte_count,
  cooperative_work&,
  field_context);
seastar::future<result<void>> native_header_crc(
  const bytes::fragmented_buffer&, cooperative_work&, field_context);
seastar::future<result<envelope_body_extent>> native_inspect(
  bytes::fragmented_buffer_parser&,
  format_family,
  envelope_extent_limits,
  decode_budget,
  cooperative_work&,
  field_context,
  input_boundary);
seastar::future<result<bytes::fragmented_buffer>> native_encode(
  bytes::fragmented_buffer&&,
  format_family,
  cooperative_work&,
  envelope_extent_limits,
  operation_usage,
  byte_count,
  bytes::allocation_charge_fn,
  field_context);

template<typename T, typename Decoder>
requires detail::envelope_body_decoder<T, Decoder>
[[nodiscard]] seastar::future<result<T>> native_decode(
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
            const auto framing = co_await native_inspect(
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

} // namespace kwaque::codec::bench::checked
