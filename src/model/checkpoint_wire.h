#pragma once

#include "src/codec/envelope.h"
#include "src/codec/error.h"
#include "src/codec/integer.h"
#include "src/codec/limits.h"
#include "src/model/checkpoint.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>

namespace kwaque::model {

inline constexpr byte_count checkpoint_fixed_bytes{20};
inline constexpr byte_count checkpoint_cursor_bytes{24};

namespace detail {

inline codec::error checkpoint_error(
  errc code, codec::field_context context, std::uint64_t offset = 0) noexcept {
    return codec::error{
      code, context.family, context.field, context.origin + offset};
}

// Counts are checked before arithmetic, native allocation or narrowing. Value
// construction uses the minimum header; framing checks its actual header too.
inline codec::result<byte_count> checkpoint_body_size(
  std::uint64_t count,
  const codec::limits& policy,
  codec::field_context context) {
    if (count == 0)
        return codec::failure(
          checkpoint_error(errc::invalid_argument, context));
    const auto limits = policy.config();
    if (
      count > std::min(
        {std::uint64_t{read_checkpoint::maximum_cursors},
         limits.max_checkpoint_cursors.value(),
         limits.max_object_entries.value()}))
        return codec::failure(
          checkpoint_error(errc::resource_exhausted, context));
    const byte_count body{
      checkpoint_fixed_bytes.value() + checkpoint_cursor_bytes.value() * count};
    if (
      body > limits.max_encoded_body_bytes
      || body.value() + codec::envelope_prefix_bytes
           > limits.max_checkpoint_bytes.value())
        return codec::failure(
          checkpoint_error(errc::resource_exhausted, context));
    return body;
}

inline std::array<char, checkpoint_fixed_bytes.value()>
encode_checkpoint_prefix(topic_id topic, std::uint32_t count) noexcept {
    std::array<char, checkpoint_fixed_bytes.value()> out{};
    std::copy(topic.bytes().begin(), topic.bytes().end(), out.begin());
    const auto encoded = std::bit_cast<std::array<char, 4>>(
      seastar::cpu_to_le(count));
    std::copy(encoded.begin(), encoded.end(), out.begin() + 16);
    return out;
}

inline std::array<char, checkpoint_cursor_bytes.value()>
encode_checkpoint_cursor(range_cursor cursor) noexcept {
    std::array<char, checkpoint_cursor_bytes.value()> out{};
    const auto range = cursor.range();
    std::copy(range.bytes().begin(), range.bytes().end(), out.begin());
    const auto encoded = std::bit_cast<std::array<char, 8>>(
      seastar::cpu_to_le(cursor.next().value()));
    std::copy(encoded.begin(), encoded.end(), out.begin() + 16);
    return out;
}

} // namespace detail
} // namespace kwaque::model
