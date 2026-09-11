#pragma once

#include "src/base/error.h"
#include "src/base/invariant.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/error.h"

#include <seastar/core/byteorder.hh>

#include <array>
#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <type_traits>

namespace kwaque::codec {

// The current parser/builder starts at origin in the outer input/output.
// Family and field are trusted diagnostic identifiers, not incoming wire tags.
struct field_context final {
    std::uint64_t origin{0};
    std::uint16_t family{0};
    std::uint16_t field{0};
};

enum class input_boundary : std::uint8_t {
    open = 0,
    complete = 1,
};

template<typename T>
concept fixed_width_integer = bytes::fixed_width_unsigned_integer<T>
                              || std::same_as<T, std::int32_t>
                              || std::same_as<T, std::int64_t>;

template<typename T>
concept varuint_integer = std::same_as<T, std::uint32_t>
                          || std::same_as<T, std::uint64_t>;

template<typename T>
concept varint_integer = std::same_as<T, std::int32_t>
                         || std::same_as<T, std::int64_t>;

template<varint_integer T>
[[nodiscard]] constexpr std::make_unsigned_t<T>
encode_zigzag(T value) noexcept {
    using unsigned_type = std::make_unsigned_t<T>;
    constexpr auto sign_bit = std::numeric_limits<unsigned_type>::digits - 1;
    const auto bits = std::bit_cast<unsigned_type>(value);
    const auto sign_mask = unsigned_type{0} - (bits >> sign_bit);
    return (bits << 1U) ^ sign_mask;
}

template<varuint_integer T>
[[nodiscard]] constexpr std::make_signed_t<T> decode_zigzag(T bits) noexcept {
    const auto sign_mask = T{0} - (bits & T{1});
    return std::bit_cast<std::make_signed_t<T>>((bits >> 1U) ^ sign_mask);
}

namespace detail {

// Validate the whole supplied coordinate extent before a read can commit.
[[nodiscard]] result<std::uint64_t> integer_read_start(
  const bytes::fragmented_buffer_parser& input,
  field_context context,
  input_boundary boundary) noexcept;

[[nodiscard]] constexpr error integer_shortage(
  field_context context,
  input_boundary boundary,
  std::uint64_t first_missing) noexcept {
    return error{
      boundary == input_boundary::complete ? errc::malformed_data
                                           : errc::truncated_data,
      context.family,
      context.field,
      first_missing};
}

// One scalar is appended atomically to the owner's private staging builder.
// The owner configures its object/fragment limits and accounts actual backing;
// this helper neither publishes a complete object nor grants a fresh budget.
[[nodiscard]] result<void> append_integer(
  bytes::fragmented_buffer_builder& output,
  std::span<const char> encoded,
  field_context context);

[[nodiscard]] result<std::uint32_t> read_varuint32(
  bytes::fragmented_buffer_parser& input,
  field_context context,
  input_boundary boundary);
[[nodiscard]] result<std::uint64_t> read_varuint64(
  bytes::fragmented_buffer_parser& input,
  field_context context,
  input_boundary boundary);
[[nodiscard]] result<void> write_varuint_value(
  bytes::fragmented_buffer_builder& output,
  std::uint64_t value,
  field_context context);

template<fixed_width_integer T, std::endian Order>
[[nodiscard]] result<T> read_fixed(
  bytes::fragmented_buffer_parser& input,
  field_context context,
  input_boundary boundary) {
    const auto start = integer_read_start(input, context, boundary);
    if (!start) {
        return codec::failure(start.error());
    }
    if (input.bytes_remaining().value() < sizeof(T)) {
        return codec::failure(integer_shortage(
          context, boundary, context.origin + input.total_bytes().value()));
    }
    using unsigned_type = std::make_unsigned_t<T>;
    const auto value = [&input] {
        if constexpr (Order == std::endian::little) {
            return input.read_le<unsigned_type>();
        } else {
            return input.read_be<unsigned_type>();
        }
    }();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CODEC-FIXED-READ"},
      value.has_value(),
      "bounded fixed-width read failed without intervening mutation");
    return std::bit_cast<T>(*value);
}

template<fixed_width_integer T, std::endian Order>
[[nodiscard]] result<void> write_fixed(
  bytes::fragmented_buffer_builder& output, T value, field_context context) {
    using unsigned_type = std::make_unsigned_t<T>;
    const auto bits = std::bit_cast<unsigned_type>(value);
    const auto ordered = Order == std::endian::little
                           ? seastar::cpu_to_le(bits)
                           : seastar::cpu_to_be(bits);
    const auto encoded = std::bit_cast<std::array<char, sizeof(T)>>(ordered);
    return append_integer(output, encoded, context);
}

} // namespace detail

// Atomic scalar reads. Shortage is incomplete at an open input boundary and
// malformed inside a complete declared parent. Diagnostics use outer input
// coordinates. These leaves neither share storage nor use parser checkpoints.
template<fixed_width_integer T>
[[nodiscard]] result<T> read_le(
  bytes::fragmented_buffer_parser& input,
  field_context context = {},
  input_boundary boundary = input_boundary::open) {
    return detail::read_fixed<T, std::endian::little>(input, context, boundary);
}

template<fixed_width_integer T>
[[nodiscard]] result<T> read_be(
  bytes::fragmented_buffer_parser& input,
  field_context context = {},
  input_boundary boundary = input_boundary::open) {
    return detail::read_fixed<T, std::endian::big>(input, context, boundary);
}

// Writes append one scalar to private staging. Allocation exceptions propagate;
// the existing builder preserves prior logical bytes on a failed scalar append.
template<fixed_width_integer T>
[[nodiscard]] result<void> write_le(
  bytes::fragmented_buffer_builder& output,
  T value,
  field_context context = {}) {
    return detail::write_fixed<T, std::endian::little>(output, value, context);
}

template<fixed_width_integer T>
[[nodiscard]] result<void> write_be(
  bytes::fragmented_buffer_builder& output,
  T value,
  field_context context = {}) {
    return detail::write_fixed<T, std::endian::big>(output, value, context);
}

// Minimal unsigned base-128 only; this is not the protobuf varint profile.
// The probe reads at most five/ten bytes and commits only a complete valid
// value.
template<varuint_integer T>
[[nodiscard]] result<T> read_varuint(
  bytes::fragmented_buffer_parser& input,
  field_context context = {},
  input_boundary boundary = input_boundary::open) {
    if constexpr (std::same_as<T, std::uint32_t>) {
        return detail::read_varuint32(input, context, boundary);
    } else {
        return detail::read_varuint64(input, context, boundary);
    }
}

template<varuint_integer T>
[[nodiscard]] result<void> write_varuint(
  bytes::fragmented_buffer_builder& output,
  T value,
  field_context context = {}) {
    return detail::write_varuint_value(output, value, context);
}

template<varint_integer T>
[[nodiscard]] result<T> read_varint(
  bytes::fragmented_buffer_parser& input,
  field_context context = {},
  input_boundary boundary = input_boundary::open) {
    const auto value = read_varuint<std::make_unsigned_t<T>>(
      input, context, boundary);
    if (!value) {
        return codec::failure(value.error());
    }
    return decode_zigzag(*value);
}

template<varint_integer T>
[[nodiscard]] result<void> write_varint(
  bytes::fragmented_buffer_builder& output,
  T value,
  field_context context = {}) {
    return write_varuint(output, encode_zigzag(value), context);
}

// The allowance applies to the field's payload, so zero permits null/empty.
// A successful read consumes only the prefix, after validating payload bounds.
// No slice, payload copy or parser checkpoint is needed for these leaves.
[[nodiscard]] result<std::optional<byte_count>> read_nullable_length(
  bytes::fragmented_buffer_parser& input,
  byte_count maximum_length,
  field_context context = {},
  input_boundary boundary = input_boundary::open);

[[nodiscard]] result<void> write_nullable_length(
  bytes::fragmented_buffer_builder& output,
  std::optional<byte_count> length,
  byte_count maximum_length,
  field_context context = {});

} // namespace kwaque::codec
