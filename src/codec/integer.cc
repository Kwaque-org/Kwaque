#include "src/codec/integer.h"

#include "src/base/error.h"
#include "src/base/invariant.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/error.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>

namespace kwaque::codec::detail {

result<std::uint64_t> integer_read_start(
  const bytes::fragmented_buffer_parser& input,
  field_context context,
  input_boundary boundary) noexcept {
    if (
      (boundary != input_boundary::open && boundary != input_boundary::complete)
      || context.origin > std::numeric_limits<std::uint64_t>::max()
                            - input.total_bytes().value()) {
        return codec::failure(
          error{
            errc::invalid_argument,
            context.family,
            context.field,
            context.origin});
    }
    return context.origin + input.bytes_consumed().value();
}

result<void> append_integer(
  bytes::fragmented_buffer_builder& output,
  std::span<const char> encoded,
  field_context context) {
    const auto size = output.size().value();
    constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
    if (context.origin > maximum - size) {
        return codec::failure(
          error{
            errc::invalid_argument,
            context.family,
            context.field,
            context.origin});
    }
    const auto start = context.origin + size;
    if (encoded.size() > maximum - start) {
        return codec::failure(
          error{errc::invalid_argument, context.family, context.field, start});
    }
    if (output.finished()) {
        return codec::failure(
          error{errc::closed, context.family, context.field, start});
    }
    const auto appended = output.append(encoded);
    if (!appended) {
        KWAQUE_INVARIANT(
          invariant_id{"KQ-CODEC-SCALAR-APPEND"},
          appended.error() == errc::resource_exhausted,
          "open bounded scalar append returned an unexpected error");
        return codec::failure(
          error{
            errc::resource_exhausted, context.family, context.field, start});
    }
    return {};
}

namespace {

template<varuint_integer T>
struct varuint_probe final {
    T value;
    byte_count size;
};

template<varuint_integer T>
result<varuint_probe<T>> probe_varuint_value(
  const bytes::fragmented_buffer_parser& input,
  field_context context,
  input_boundary boundary) {
    const auto start = integer_read_start(input, context, boundary);
    if (!start) {
        return codec::failure(start.error());
    }
    constexpr auto bits = static_cast<unsigned>(std::numeric_limits<T>::digits);
    constexpr std::size_t max_bytes = (bits + 6U) / 7U;
    std::array<char, max_bytes> encoded{};
    const auto available = static_cast<std::size_t>(
      std::min<std::uint64_t>(input.bytes_remaining().value(), max_bytes));
    const auto peeked = input.peek_to(
      std::span<char>{encoded}.first(available));
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CODEC-VARUINT-PEEK"},
      peeked.has_value(),
      "bounded varuint peek failed without intervening mutation");

    T value{0};
    unsigned bits_read = 0;
    for (std::size_t index = 0; index < available; ++index) {
        const auto octet = std::bit_cast<std::uint8_t>(encoded[index]);
        if (octet == 0 && bits_read != 0) {
            return codec::failure(
              error{
                errc::malformed_data,
                context.family,
                context.field,
                *start + index});
        }
        const auto remaining_bits = bits - bits_read;
        // Check the whole final byte, including its continuation bit, before
        // shifting. The largest shifts are 28 for u32 and 63 for u64.
        if (remaining_bits <= 7U && (octet >> remaining_bits) != 0) {
            return codec::failure(
              error{
                errc::malformed_data,
                context.family,
                context.field,
                *start + index});
        }
        value |= static_cast<T>(octet & 0x7fU) << bits_read;
        if ((octet & 0x80U) == 0) {
            return varuint_probe<T>{value, byte_count{index + 1U}};
        }
        bits_read += 7U;
    }
    return codec::failure(integer_shortage(
      context, boundary, context.origin + input.total_bytes().value()));
}

void commit_varuint(bytes::fragmented_buffer_parser& input, byte_count size) {
    const auto committed = input.skip(size);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CODEC-VARUINT-COMMIT"},
      committed.has_value(),
      "validated varuint skip failed without intervening mutation");
}

template<varuint_integer T>
result<T> read_varuint_value(
  bytes::fragmented_buffer_parser& input,
  field_context context,
  input_boundary boundary) {
    const auto value = probe_varuint_value<T>(input, context, boundary);
    if (!value) {
        return codec::failure(value.error());
    }
    commit_varuint(input, value->size);
    return value->value;
}

} // namespace

result<std::uint32_t> read_varuint32(
  bytes::fragmented_buffer_parser& input,
  field_context context,
  input_boundary boundary) {
    return read_varuint_value<std::uint32_t>(input, context, boundary);
}

result<std::uint64_t> read_varuint64(
  bytes::fragmented_buffer_parser& input,
  field_context context,
  input_boundary boundary) {
    return read_varuint_value<std::uint64_t>(input, context, boundary);
}

result<void> write_varuint_value(
  bytes::fragmented_buffer_builder& output,
  std::uint64_t value,
  field_context context) {
    std::array<char, 10> encoded{};
    if (value < 0x80U) {
        encoded[0] = std::bit_cast<char>(static_cast<std::uint8_t>(value));
        return append_integer(
          output, std::span<const char>{encoded}.first(1), context);
    }
    std::size_t count = 0;
    while (value >= 0x80U) {
        encoded[count++] = std::bit_cast<char>(
          static_cast<std::uint8_t>((value & 0x7fU) | 0x80U));
        value >>= 7U;
    }
    encoded[count++] = std::bit_cast<char>(static_cast<std::uint8_t>(value));
    return append_integer(
      output, std::span<const char>{encoded}.first(count), context);
}

} // namespace kwaque::codec::detail

namespace kwaque::codec {

result<std::optional<byte_count>> read_nullable_length(
  bytes::fragmented_buffer_parser& input,
  byte_count maximum_length,
  field_context context,
  input_boundary boundary) {
    const auto probe = detail::probe_varuint_value<std::uint32_t>(
      input, context, boundary);
    if (!probe) {
        return codec::failure(probe.error());
    }
    const auto start = context.origin + input.bytes_consumed().value();
    const auto decoded = decode_zigzag(probe->value);
    if (decoded < -1) {
        return codec::failure(
          error{errc::malformed_data, context.family, context.field, start});
    }
    if (decoded == -1) {
        detail::commit_varuint(input, probe->size);
        return std::optional<byte_count>{};
    }
    const byte_count length{static_cast<std::uint64_t>(decoded)};
    if (length > maximum_length) {
        return codec::failure(
          error{
            errc::resource_exhausted, context.family, context.field, start});
    }
    if (
      length.value() > input.bytes_remaining().value() - probe->size.value()) {
        return codec::failure(
          detail::integer_shortage(
            context, boundary, context.origin + input.total_bytes().value()));
    }
    detail::commit_varuint(input, probe->size);
    return std::optional<byte_count>{length};
}

result<void> write_nullable_length(
  bytes::fragmented_buffer_builder& output,
  std::optional<byte_count> length,
  byte_count maximum_length,
  field_context context) {
    constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
    if (context.origin > maximum - output.size().value()) {
        return codec::failure(
          error{
            errc::invalid_argument,
            context.family,
            context.field,
            context.origin});
    }
    const auto start = context.origin + output.size().value();
    if (length) {
        if (
          length->value() > static_cast<std::uint64_t>(
            std::numeric_limits<std::int32_t>::max())) {
            return codec::failure(
              error{
                errc::invalid_argument, context.family, context.field, start});
        }
    }
    const auto decoded = length ? static_cast<std::int32_t>(length->value())
                                : std::int32_t{-1};
    const auto prefix_size = std::max(
      1U,
      (static_cast<unsigned>(std::bit_width(encode_zigzag(decoded))) + 6U)
        / 7U);
    if (prefix_size > maximum - start) {
        return codec::failure(
          error{errc::invalid_argument, context.family, context.field, start});
    }
    if (length && *length > maximum_length) {
        return codec::failure(
          error{
            errc::resource_exhausted, context.family, context.field, start});
    }
    return write_varint(output, decoded, context);
}

} // namespace kwaque::codec
