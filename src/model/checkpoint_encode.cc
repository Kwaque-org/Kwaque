#include "src/base/invariant.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/codec/envelope_encode.h"
#include "src/model/checkpoint_codec.h"
#include "src/model/checkpoint_wire.h"
#include "src/model/fingerprint.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <cstdint>
#include <exception>
#include <limits>
#include <optional>
#include <span>
#include <utility>

namespace kwaque::model {
namespace {
using bytes::fragmented_buffer;
using bytes::fragmented_buffer_builder;
using detail::checkpoint_error;

// Uniform tails give an exact backing bound. Descriptor storage is reserved
// once before appending; no per-cursor allocation or growing metadata array.
codec::result<bytes::fragmented_buffer_builder_config> body_staging(
  byte_count body,
  const codec::limits& policy,
  byte_count remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context context) {
    const auto cap = policy.config();
    auto width = std::min(
      {std::uint64_t{65536}, body.value(), cap.max_allocation_bytes.value()});
    byte_count tail;
    while (width != 0) {
        tail = charge(byte_count{width});
        if (tail < byte_count{width})
            return codec::failure(
              checkpoint_error(errc::invalid_argument, context));
        if (policy.validate_allocation(tail)) break;
        width /= 2U;
    }
    if (width == 0)
        return codec::failure(
          checkpoint_error(errc::resource_exhausted, context));
    const auto count = 1U + (body.value() - 1U) / width;
    if (count > cap.max_buffer_fragments.value())
        return codec::failure(
          checkpoint_error(errc::resource_exhausted, context));
    const byte_count request{
      count * fragmented_buffer::fragment_descriptor_size()};
    const auto descriptors = charge(request);
    if (descriptors < request)
        return codec::failure(
          checkpoint_error(errc::invalid_argument, context));
    const byte_count backing{count * tail.value()};
    if (
      !policy.validate_allocation(descriptors)
      || !policy.validate_buffer(
        body, backing, item_count{count}, cap.max_encoded_body_bytes)
      || !policy.remaining_operation_bytes(
        {.staged_output = backing, .payload_bookkeeping = descriptors},
        remaining))
        return codec::failure(
          checkpoint_error(errc::resource_exhausted, context));
    bytes::fragmented_buffer_builder_config config;
    config.initial_fragment_bytes = byte_count{width};
    config.max_fragment_bytes = byte_count{width};
    config.max_total_bytes = body;
    config.max_retained_bytes = byte_count{count * width};
    config.max_fragments = count;
    if (!config.validate())
        return codec::failure(
          checkpoint_error(errc::resource_exhausted, context));
    return config;
}

seastar::future<codec::result<encoded_read_checkpoint>> encode(
  const read_checkpoint& value,
  std::optional<fragmented_buffer_builder>& staging,
  std::optional<fragmented_buffer>& body,
  codec::cooperative_work& work,
  byte_count remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context context) {
    const auto anchor = checkpoint_error(errc::success, context);
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (charge == nullptr)
        co_return codec::failure(
          checkpoint_error(errc::invalid_argument, context));
    const auto size = detail::checkpoint_body_size(
      value.cursors().size(), work.policy(), context);
    if (!size) co_return codec::failure(size.error());
    const byte_count encoded_size{size->value() + codec::envelope_prefix_bytes};
    if (
      encoded_size.value()
      > std::numeric_limits<std::uint64_t>::max() - context.origin)
        co_return codec::failure(
          checkpoint_error(errc::invalid_argument, context));
    const auto fingerprint = co_await compute_checkpoint_fingerprint(
      value, work, anchor);
    if (!fingerprint) co_return codec::failure(fingerprint.error());
    if (auto ready = co_await work.checkpoint(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    const auto shape = body_staging(
      *size, work.policy(), remaining, charge, context);
    if (!shape) co_return codec::failure(shape.error());
    staging.emplace(*shape);
    const auto reserved = staging->reserve_fragments(
      item_count{shape->max_fragments});
    if (!reserved)
        co_return codec::failure(
          codec::detail::allocation_cost_error(
            reserved.error(), context, context.origin));
    if (
      auto ready = co_await work.admit(byte_count{256}, item_count{64}, anchor);
      !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    const auto prefix = detail::encode_checkpoint_prefix(
      value.topic(), static_cast<std::uint32_t>(value.cursors().size()));
    if (auto copied = staging->append(std::span<const char>{prefix}); !copied)
        co_return codec::failure(
          codec::detail::allocation_cost_error(
            copied.error(), context, context.origin));
    for (const auto& cursor : value.cursors()) {
        if (
          auto ready = co_await work.admit(
            byte_count{256}, item_count{64}, anchor);
          !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        const auto entry = detail::encode_checkpoint_cursor(cursor);
        if (
          auto copied = staging->append(std::span<const char>{entry}); !copied)
            co_return codec::failure(
              codec::detail::allocation_cost_error(
                copied.error(), context, context.origin));
    }
    if (auto ready = co_await work.checkpoint(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    auto finished = staging->finish();
    if (!finished)
        co_return codec::failure(
          codec::detail::allocation_cost_error(
            finished.error(), context, context.origin));
    body.emplace(std::move(*finished));
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CHECKPOINT-BODY"},
      body->size() == *size,
      "checkpoint writer differs from its fixed count and cursor extent");
    auto framed = co_await codec::encode_envelope(
      std::move(*body),
      codec::format_family::read_checkpoint,
      work,
      {*size, encoded_size},
      {},
      remaining,
      charge,
      context);
    if (!framed) co_return codec::failure(framed.error());
    co_return encoded_read_checkpoint{std::move(*framed), *fingerprint};
}
} // namespace

seastar::future<codec::result<encoded_read_checkpoint>> encode_read_checkpoint(
  const read_checkpoint& value,
  codec::cooperative_work& work,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context context) {
    context.family = static_cast<std::uint16_t>(
      codec::format_family::read_checkpoint);
    std::optional<fragmented_buffer_builder> staging;
    std::optional<fragmented_buffer> body;
    std::optional<codec::result<encoded_read_checkpoint>> outcome;
    std::exception_ptr exception;
    try {
        outcome.emplace(
          co_await encode(
            value, staging, body, work, parent_remaining, charge, context));
    } catch (...) {
        exception = std::current_exception();
    }
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    staging.reset();
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    body.reset();
    if (exception) std::rethrow_exception(exception);
    if (!outcome->has_value()) co_return codec::failure(outcome->error());
    if (
      auto ready = work.poll(detail::checkpoint_error(errc::success, context));
      !ready) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        outcome.reset();
        co_return codec::failure(ready.error());
    }
    co_return std::move(**outcome);
}

} // namespace kwaque::model
