#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/collection.h"
#include "src/codec/cooperative.h"
#include "src/codec/crc32c.h"
#include "src/codec/error.h"
#include "src/codec/integer.h"
#include "src/codec/limits.h"
#include "src/codec/transaction.h"
#include "src/runtime/testing/seastar_fuzz.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/temporary_buffer.hh>

#include <crc32c/crc32c.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

namespace codec = kwaque::codec;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::item_count;
using kwaque::bytes::fragmented_buffer;
using kwaque::bytes::fragmented_buffer_builder;
using kwaque::bytes::fragmented_buffer_parser;

constexpr std::size_t max_input_size = 16U * 1024U;
constexpr codec::field_context context{.origin = 1024, .family = 3, .field = 7};
constexpr std::uint64_t wire_origin = context.origin + 2U;
constexpr codec::error injected_result{errc::wrong_context, 91, 92, 93};

void require(bool condition) {
    if (!condition) {
        __builtin_trap();
    }
}

class operands final {
public:
    explicit operands(std::span<const std::uint8_t> bytes) noexcept
      : bytes_(bytes) {}
    std::uint8_t byte() noexcept {
        return at_ < bytes_.size() ? bytes_[at_++] : 0;
    }
    std::uint64_t word() noexcept {
        std::uint64_t value = 0;
        for (unsigned index = 0; index < 8; ++index) {
            value |= static_cast<std::uint64_t>(byte()) << (8U * index);
        }
        return value;
    }
    std::span<const std::uint8_t> remaining() const noexcept {
        return bytes_.subspan(at_);
    }

private:
    std::span<const std::uint8_t> bytes_;
    std::size_t at_{0};
};

std::span<const char> chars(std::span<const std::uint8_t> bytes) noexcept {
    return bytes.empty()
             ? std::span<const char>{}
             : std::span<const char>{
                 reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

byte_count allocation_charge(byte_count requested) noexcept {
    if (requested.value() == 0) {
        return {};
    }
    const auto value = std::max(requested.value(), std::uint64_t{16});
    if (value > (std::uint64_t{1} << 62U)) {
        return byte_count{std::numeric_limits<std::uint64_t>::max()};
    }
    return byte_count{2U * std::bit_ceil(value)};
}

codec::decode_budget memory_budget() noexcept {
    // Residuals leave ample separate reservations for the bounded harness,
    // callback/frame storage and any borrowed input owners.
    return {
      byte_count{8U * 1024U * 1024U},
      byte_count{128U * 1024U},
      allocation_charge};
}

fragmented_buffer_parser
parser_for(std::span<const std::uint8_t> wire, bool split, std::size_t depth) {
    std::vector<std::uint8_t> input{0xa5, 0x5a};
    input.insert(input.end(), wire.begin(), wire.end());
    fragmented_buffer buffer;
    if (!split) {
        buffer = fragmented_buffer::copy_of(chars(input)).value();
    } else {
        std::vector<seastar::temporary_buffer<char>> fragments;
        const auto chunk = std::max<std::size_t>(1, (input.size() + 31U) / 32U);
        for (std::size_t offset = 0; offset < input.size();) {
            const auto size = std::min(
              input.size() - offset, offset < 16 ? std::size_t{1} : chunk);
            seastar::temporary_buffer<char> fragment{size};
            std::ranges::copy(
              chars(std::span<const std::uint8_t>{input}.subspan(offset, size)),
              fragment.get_write());
            fragments.push_back(std::move(fragment));
            offset += size;
        }
        require(fragments.size() <= 48);
        buffer = fragmented_buffer::copy_from_fragments(fragments).value();
    }
    fragmented_buffer_parser parser{std::move(buffer)};
    parser.skip(byte_count{2}).value();
    for (std::size_t index = 0; index < depth; ++index) {
        parser.push_checkpoint().value();
    }
    return parser;
}

void verify_cursor(
  const fragmented_buffer_parser& parser,
  std::span<const std::uint8_t> wire,
  std::size_t used,
  std::size_t depth) {
    require(used <= wire.size());
    require(parser.bytes_consumed() == byte_count{2U + used});
    require(parser.total_bytes() == byte_count{2U + wire.size()});
    require(parser.checkpoint_depth() == depth);
    const auto remaining = wire.subspan(used);
    std::string actual(remaining.size(), '\0');
    require(parser.peek_to(std::span<char>{actual}).has_value());
    require(std::ranges::equal(actual, chars(remaining)));
}

codec::error
failure_at(errc reason, std::uint64_t origin, std::size_t offset) noexcept {
    return codec::error{reason, context.family, context.field, origin + offset};
}

codec::error shortage(
  codec::input_boundary boundary,
  std::uint64_t origin,
  std::size_t size) noexcept {
    return failure_at(
      boundary == codec::input_boundary::open ? errc::truncated_data
                                              : errc::malformed_data,
      origin,
      size);
}

struct scalar_oracle final {
    std::uint64_t value{0};
    std::size_t used{0};
    std::optional<codec::error> failed;
};

// Arithmetic base-128 grammar oracle, independently checking multiplication,
// addition and minimal digit count instead of the decoder's terminal-bit test.
scalar_oracle varuint_oracle(
  std::span<const std::uint8_t> bytes,
  unsigned width,
  codec::input_boundary boundary,
  std::uint64_t origin = wire_origin) {
    const auto maximum = width == 32
                           ? std::uint64_t{0xffffffffU}
                           : std::numeric_limits<std::uint64_t>::max();
    const auto digits = (width + 6U) / 7U;
    std::uint64_t value = 0;
    std::uint64_t place = 1;
    for (std::size_t index = 0; index < bytes.size() && index < digits;
         ++index) {
        const auto digit = static_cast<std::uint64_t>(bytes[index] % 128U);
        if (digit > (maximum - value) / place) {
            return {.failed = failure_at(errc::malformed_data, origin, index)};
        }
        value += digit * place;
        if (bytes[index] < 128U) {
            if (index != 0 && value < place) {
                return {
                  .failed = failure_at(errc::malformed_data, origin, index)};
            }
            return {.value = value, .used = index + 1U};
        }
        if (index + 1U == digits) {
            return {.failed = failure_at(errc::malformed_data, origin, index)};
        }
        place *= 128U;
    }
    return {.failed = shortage(boundary, origin, bytes.size())};
}

std::int64_t zigzag_oracle(std::uint64_t value) noexcept {
    const auto magnitude = static_cast<std::int64_t>(value / 2U);
    return value % 2U == 0 ? magnitude : -magnitude - 1;
}

std::vector<std::uint8_t> varuint_bytes(std::uint64_t value) {
    std::vector<std::uint8_t> encoded;
    do {
        auto digit = static_cast<std::uint8_t>(value % 128U);
        value /= 128U;
        if (value != 0) {
            digit = static_cast<std::uint8_t>(digit + 128U);
        }
        encoded.push_back(digit);
    } while (value != 0);
    return encoded;
}

std::vector<std::uint8_t> published_bytes(fragmented_buffer_builder& builder) {
    auto buffer = builder.finish().value();
    std::vector<std::uint8_t> bytes(buffer.size().value());
    const auto copied = buffer.copy_to(
      std::span<char>{reinterpret_cast<char*>(bytes.data()), bytes.size()});
    require(copied.has_value() && copied->value() == bytes.size());
    return bytes;
}

void mutate(std::vector<std::uint8_t>& wire, unsigned kind, std::size_t index) {
    switch (kind % 5U) {
    case 1:
        if (!wire.empty()) {
            wire[index % wire.size()] ^= 0x80U;
        }
        break;
    case 2:
        wire.resize(index % (wire.size() + 1U));
        break;
    case 3:
        if (!wire.empty()) {
            wire.back() |= 0x80U;
        }
        wire.push_back(0);
        break;
    case 4:
        wire.push_back(0);
        break;
    default:
        break;
    }
}

template<typename T>
void verify_varint(
  std::span<const std::uint8_t> wire,
  codec::input_boundary boundary,
  bool split,
  std::size_t depth) {
    auto input = parser_for(wire, split, depth);
    const auto expected = varuint_oracle(wire, sizeof(T) * 8U, boundary);
    const auto actual = [&] {
        if constexpr (std::is_signed_v<T>) {
            return codec::read_varint<T>(input, context, boundary);
        } else {
            return codec::read_varuint<T>(input, context, boundary);
        }
    }();
    require(actual.has_value() == !expected.failed);
    if (expected.failed) {
        require(actual.error() == *expected.failed);
    } else if constexpr (std::is_signed_v<T>) {
        require(*actual == static_cast<T>(zigzag_oracle(expected.value)));
    } else {
        require(*actual == static_cast<T>(expected.value));
    }
    verify_cursor(input, wire, expected.failed ? 0 : expected.used, depth);
}

template<typename T>
std::vector<std::uint8_t> structured_varint(std::uint64_t bits) {
    using unsigned_type = std::make_unsigned_t<T>;
    const auto value = std::bit_cast<T>(static_cast<unsigned_type>(bits));
    std::uint64_t expected_bits;
    fragmented_buffer_builder output;
    if constexpr (std::is_signed_v<T>) {
        const auto signed_value = static_cast<std::int64_t>(value);
        expected_bits = signed_value >= 0
                          ? static_cast<std::uint64_t>(signed_value) * 2U
                          : static_cast<std::uint64_t>(-(signed_value + 1)) * 2U
                              + 1U;
        require(codec::write_varint(output, value, context).has_value());
    } else {
        expected_bits = value;
        require(codec::write_varuint(output, value, context).has_value());
    }
    auto wire = published_bytes(output);
    require(wire == varuint_bytes(expected_bits));
    return wire;
}

template<typename T>
void verify_fixed(
  std::span<const std::uint8_t> wire,
  bool big,
  codec::input_boundary boundary,
  bool split,
  std::size_t depth) {
    auto input = parser_for(wire, split, depth);
    const auto actual = big ? codec::read_be<T>(input, context, boundary)
                            : codec::read_le<T>(input, context, boundary);
    if (wire.size() < sizeof(T)) {
        require(!actual.has_value());
        require(actual.error() == shortage(boundary, wire_origin, wire.size()));
        verify_cursor(input, wire, 0, depth);
        return;
    }
    std::uint64_t bits = 0;
    for (std::size_t index = 0; index < sizeof(T); ++index) {
        const auto position = big ? index : sizeof(T) - 1U - index;
        bits = bits * 256U + wire[position];
    }
    require(actual.has_value());
    require(std::bit_cast<std::make_unsigned_t<T>>(*actual) == bits);
    fragmented_buffer_builder output;
    require((big ? codec::write_be(output, *actual, context)
                 : codec::write_le(output, *actual, context))
              .has_value());
    const auto encoded = published_bytes(output);
    require(std::ranges::equal(encoded, wire.first(sizeof(T))));
    verify_cursor(input, wire, sizeof(T), depth);
}

template<typename T>
std::vector<std::uint8_t> structured_fixed(std::uint64_t bits, bool big) {
    const auto value = static_cast<T>(bits);
    fragmented_buffer_builder output;
    require((big ? codec::write_be(output, value, context)
                 : codec::write_le(output, value, context))
              .has_value());
    auto wire = published_bytes(output);
    std::vector<std::uint8_t> expected(sizeof(T));
    for (std::size_t index = 0; index < sizeof(T); ++index) {
        const auto destination = big ? sizeof(T) - 1U - index : index;
        expected[destination] = static_cast<std::uint8_t>(bits & 0xffU);
        bits /= 256U;
    }
    require(wire == expected);
    return wire;
}

struct nullable_oracle final {
    std::optional<std::uint64_t> length;
    std::size_t used{0};
    std::optional<codec::error> failed;
};

nullable_oracle nullable(
  std::span<const std::uint8_t> wire,
  std::uint64_t allowance,
  codec::input_boundary boundary,
  std::uint64_t origin = wire_origin) {
    const auto raw = varuint_oracle(wire, 32, boundary, origin);
    if (raw.failed) {
        return {.failed = raw.failed};
    }
    const auto length = zigzag_oracle(raw.value);
    if (length < -1) {
        return {.failed = failure_at(errc::malformed_data, origin, 0)};
    }
    if (length == -1) {
        return {.used = raw.used};
    }
    if (static_cast<std::uint64_t>(length) > allowance) {
        return {.failed = failure_at(errc::resource_exhausted, origin, 0)};
    }
    if (static_cast<std::uint64_t>(length) > wire.size() - raw.used) {
        return {.failed = shortage(boundary, origin, wire.size())};
    }
    return {.length = static_cast<std::uint64_t>(length), .used = raw.used};
}

std::vector<std::uint8_t> structured_nullable(operands& input) {
    const auto length_byte = input.byte();
    const auto length = length_byte == 255
                          ? std::optional<byte_count>{}
                          : std::optional<byte_count>{byte_count{length_byte}};
    fragmented_buffer_builder output;
    require(
      codec::write_nullable_length(output, length, byte_count{255}, context)
        .has_value());
    auto expected = varuint_bytes(length ? 2U * length->value() : 1U);
    std::vector<std::uint8_t> payload;
    for (std::uint64_t index = 0; length && index < length->value(); ++index) {
        payload.push_back(input.byte());
    }
    require(output.append(chars(payload)).has_value());
    expected.insert(expected.end(), payload.begin(), payload.end());
    auto wire = published_bytes(output);
    require(wire == expected);
    return wire;
}

void verify_nullable(
  std::span<const std::uint8_t> wire,
  std::uint64_t allowance,
  codec::input_boundary boundary,
  bool split,
  std::size_t depth) {
    const auto expected = nullable(wire, allowance, boundary);
    auto input = parser_for(wire, split, depth);
    const auto actual = codec::read_nullable_length(
      input, byte_count{allowance}, context, boundary);
    require(actual.has_value() == !expected.failed);
    if (expected.failed) {
        require(actual.error() == *expected.failed);
    } else {
        require(actual->has_value() == expected.length.has_value());
        if (expected.length) {
            require((**actual).value() == *expected.length);
        }
    }
    verify_cursor(input, wire, expected.failed ? 0 : expected.used, depth);
}

void verify_exact(
  std::span<const std::uint8_t> wire,
  std::size_t declared,
  std::size_t cap,
  std::uint64_t allowance,
  codec::input_boundary boundary,
  bool split,
  std::size_t depth) {
    scalar_oracle expected;
    if (declared > cap) {
        expected.failed = failure_at(errc::resource_exhausted, wire_origin, 0);
    } else if (declared > wire.size()) {
        expected.failed = shortage(boundary, wire_origin, wire.size());
    } else {
        const auto child = wire.first(declared);
        expected = varuint_oracle(child, 32, codec::input_boundary::complete);
        if (!expected.failed) {
            const auto length = nullable(
              child.subspan(expected.used),
              allowance,
              codec::input_boundary::complete,
              wire_origin + expected.used);
            if (length.failed) {
                expected.failed = length.failed;
            } else {
                expected.used += length.used + length.length.value_or(0);
                if (expected.used != declared) {
                    expected.failed = failure_at(
                      errc::malformed_data, wire_origin, expected.used);
                }
            }
        }
    }
    auto input = parser_for(wire, split, depth);
    auto budget
      = codec::reserve_decode_input(
          input, codec::limits::defaults(), memory_budget(), context, boundary)
          .value();
    const auto actual = codec::decode_exact(
      input,
      byte_count{declared},
      byte_count{cap},
      codec::limits::defaults(),
      budget,
      context,
      boundary,
      [allowance](
        auto& child,
        codec::field_context field,
        codec::input_boundary end,
        codec::decode_budget) -> codec::result<std::uint32_t> {
          const auto first = codec::read_varuint<std::uint32_t>(
            child, field, end);
          if (!first) {
              return codec::failure(first.error());
          }
          const auto length = codec::read_nullable_length(
            child, byte_count{allowance}, field, end);
          if (!length) {
              return codec::failure(length.error());
          }
          if (*length) {
              require(child.skip(**length).has_value());
          }
          return *first;
      });
    require(actual.has_value() == !expected.failed);
    if (expected.failed) {
        require(actual.error() == *expected.failed);
    } else {
        require(*actual == expected.value);
    }
    verify_cursor(input, wire, expected.failed ? 0 : declared, depth);
}

struct transaction_exception {};

void verify_transaction(
  std::span<const std::uint8_t> wire,
  unsigned action,
  codec::input_boundary boundary,
  bool split,
  std::size_t depth) {
    auto expected = varuint_oracle(wire, 64, boundary);
    if (depth == 8) {
        expected.failed = failure_at(errc::resource_exhausted, wire_origin, 0);
    }
    const bool throws = !expected.failed && action % 3U == 2;
    if (!expected.failed && action % 3U == 1) {
        expected.failed = injected_result;
    }
    auto input = parser_for(wire, split, depth);
    std::optional<codec::result<std::uint64_t>> actual;
    bool threw = false;
    try {
        actual.emplace(
          codec::with_transaction(
            input, context, [&](auto& child) -> codec::result<std::uint64_t> {
                const auto value = codec::read_varuint<std::uint64_t>(
                  child, context, boundary);
                if (!value) {
                    return codec::failure(value.error());
                }
                if (action % 3U == 1) {
                    return codec::failure(injected_result);
                }
                if (action % 3U == 2) {
                    throw transaction_exception{};
                }
                return *value;
            }));
    } catch (const transaction_exception&) {
        threw = true;
    }
    require(threw == throws);
    if (throws) {
        require(!actual);
    } else {
        require(actual.has_value());
        require(actual->has_value() == !expected.failed);
        if (expected.failed) {
            require(actual->error() == *expected.failed);
        } else {
            require(**actual == expected.value);
        }
    }
    verify_cursor(
      input, wire, expected.failed || throws ? 0 : expected.used, depth);
}

struct pair_less {
    bool operator()(std::uint8_t left, std::uint8_t right) const noexcept {
        return left < right;
    }
};

struct map_oracle final {
    std::array<std::pair<std::uint8_t, std::uint8_t>, 16> entries{};
    std::size_t staged{0};
    std::size_t keys{0};
    std::size_t values{0};
    std::size_t inserts{0};
    std::size_t used{0};
    std::optional<codec::error> failed;
};

map_oracle ordered_oracle(
  std::span<const std::uint8_t> wire,
  std::size_t maximum_count,
  std::size_t reject_insert,
  codec::input_boundary boundary,
  std::size_t depth) {
    map_oracle result;
    if (depth == 8) {
        result.failed = failure_at(errc::resource_exhausted, wire_origin, 0);
        return result;
    }
    const auto count = varuint_oracle(wire, 32, boundary);
    if (count.failed) {
        result.failed = count.failed;
        return result;
    }
    if (count.value > maximum_count) {
        result.failed = failure_at(errc::resource_exhausted, wire_origin, 0);
        return result;
    }
    result.used = count.used;
    std::optional<std::pair<std::uint8_t, std::uint8_t>> pending;
    const auto insert = [&] {
        ++result.inserts;
        if (reject_insert != 0 && result.inserts == reject_insert) {
            result.failed = injected_result;
            return false;
        }
        require(result.staged < result.entries.size());
        result.entries[result.staged++] = *pending;
        return true;
    };
    for (std::uint64_t index = 0; index < count.value; ++index) {
        ++result.keys;
        if (result.used == wire.size()) {
            result.failed = shortage(boundary, wire_origin, wire.size());
            return result;
        }
        const auto key_at = result.used;
        const auto key = wire[result.used++];
        if (pending && pending->first >= key) {
            result.failed = failure_at(
              errc::malformed_data, wire_origin, key_at);
            return result;
        }
        ++result.values;
        if (result.used == wire.size()) {
            result.failed = shortage(boundary, wire_origin, wire.size());
            return result;
        }
        const auto value = wire[result.used++];
        if (pending && !insert()) {
            return result;
        }
        pending = std::pair{key, value};
    }
    if (pending) {
        static_cast<void>(insert());
    }
    return result;
}

std::vector<std::uint8_t> structured_map(operands& input) {
    const auto count = static_cast<std::size_t>(input.byte() % 17U);
    std::array<std::pair<std::uint8_t, std::uint8_t>, 16> pairs{};
    std::vector<std::uint8_t> expected{static_cast<std::uint8_t>(count)};
    for (std::size_t index = 0; index < count; ++index) {
        pairs[index] = {
          static_cast<std::uint8_t>(2U * index + 1U), input.byte()};
        expected.push_back(pairs[index].first);
        expected.push_back(pairs[index].second);
    }
    const auto values
      = std::span<const std::pair<std::uint8_t, std::uint8_t>>{pairs}.first(
        count);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto memory = memory_budget();
    fragmented_buffer_builder output;
    require(output.reserve(byte_count{64}).has_value());
    const auto write = [](
                         auto& builder,
                         const std::uint8_t& value,
                         codec::field_context field,
                         codec::decode_budget&,
                         codec::cooperative_work&) {
        return codec::write_le(builder, value, field);
    };
    require(
      codec::write_ordered_map(
        output,
        values,
        item_count{16},
        memory,
        work,
        write,
        write,
        pair_less{},
        byte_count{16},
        item_count{16},
        context)
        .get()
        .has_value());
    auto wire = published_bytes(output);
    require(wire == expected);
    return wire;
}

void verify_ordered(
  std::span<const std::uint8_t> wire,
  std::size_t maximum_count,
  std::size_t reject_insert,
  codec::input_boundary boundary,
  bool split,
  std::size_t depth) {
    const auto expected = ordered_oracle(
      wire, maximum_count, reject_insert, boundary, depth);
    auto input = parser_for(wire, split, depth);
    auto memory
      = codec::reserve_decode_input(
          input, codec::limits::defaults(), memory_budget(), context, boundary)
          .value();
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    map_oracle observed;
    const auto actual
      = codec::read_ordered_map<std::uint8_t, std::uint8_t>(
          input,
          item_count{maximum_count},
          memory,
          work,
          [&](
            auto& parser,
            codec::field_context field,
            codec::input_boundary end,
            codec::decode_budget&,
            codec::cooperative_work&) {
              ++observed.keys;
              return codec::read_le<std::uint8_t>(parser, field, end);
          },
          [&](
            auto& parser,
            codec::field_context field,
            codec::input_boundary end,
            codec::decode_budget&,
            codec::cooperative_work&) {
              ++observed.values;
              return codec::read_le<std::uint8_t>(parser, field, end);
          },
          [&](
            std::uint8_t&& key,
            std::uint8_t&& value,
            codec::decode_budget&,
            codec::cooperative_work&) -> codec::result<void> {
              ++observed.inserts;
              if (reject_insert != 0 && observed.inserts == reject_insert) {
                  return codec::failure(injected_result);
              }
              require(observed.staged < observed.entries.size());
              observed.entries[observed.staged++] = {key, value};
              return {};
          },
          pair_less{},
          byte_count{16},
          item_count{16},
          context,
          boundary)
          .get();
    require(actual.has_value() == !expected.failed);
    if (expected.failed) {
        require(actual.error() == *expected.failed);
    } else {
        require(actual->value() == expected.staged);
    }
    require(
      observed.keys == expected.keys && observed.values == expected.values
      && observed.inserts == expected.inserts);
    require(observed.staged == expected.staged);
    require(
      std::ranges::equal(
        std::span{observed.entries}.first(observed.staged),
        std::span{expected.entries}.first(expected.staged)));
    verify_cursor(input, wire, expected.failed ? 0 : expected.used, depth);
}

std::uint32_t
bitwise_crc(std::uint32_t seed, std::span<const std::uint8_t> bytes) noexcept {
    auto value = ~seed;
    for (const auto byte : bytes) {
        value ^= byte;
        for (unsigned bit = 0; bit < 8; ++bit) {
            value = (value >> 1U) ^ ((value & 1U) != 0 ? 0x82f63b78U : 0U);
        }
    }
    return ~value;
}

void verify_crc(
  std::span<const std::uint8_t> body, std::uint32_t seed, std::size_t stride) {
    const auto expected = bitwise_crc(seed, body);
    codec::crc32c whole{seed};
    whole.extend(chars(body));
    require(whole.value() == expected);
    const auto native = body.empty()
                          ? seed
                          : ::crc32c::Extend(seed, body.data(), body.size());
    require(native == expected);
    codec::crc32c incremental{seed};
    auto comparison = seed;
    for (std::size_t offset = 0; offset < body.size();) {
        const auto count = std::min(stride, body.size() - offset);
        const auto part = body.subspan(offset, count);
        incremental.extend(std::span<const char>{});
        incremental.extend(chars(part));
        comparison = ::crc32c::Extend(comparison, part.data(), part.size());
        offset += count;
    }
    incremental.extend(std::span<const char>{});
    require(incremental.value() == expected && comparison == expected);
}

void exercise(std::span<const std::uint8_t> bytes) {
    // Header: mode, flags, and three mode operands. Structured bodies add a
    // mutation selector/index before their bounded values; raw bodies do not.
    operands input{bytes};
    const auto mode = input.byte() % 6U;
    const auto flags = input.byte();
    const auto a = input.byte();
    const auto b = input.byte();
    const auto c = input.byte();
    const auto body = input.remaining();
    const auto seed = (static_cast<std::uint32_t>(flags) << 24U)
                      | (static_cast<std::uint32_t>(a) << 16U)
                      | (static_cast<std::uint32_t>(b) << 8U) | c;
    verify_crc(body, seed, 1U + a % 64U);
    const auto boundary = (flags & 1U) != 0 ? codec::input_boundary::complete
                                            : codec::input_boundary::open;
    const bool split = (flags & 2U) != 0;
    const auto depth = static_cast<std::size_t>((flags >> 2U) % 16U % 9U);
    const bool structured = (flags & 0x80U) != 0;
    unsigned mutation = 0;
    std::size_t index = 0;
    if (structured) {
        mutation = input.byte();
        index = input.byte();
    }
    std::vector<std::uint8_t> wire(body.begin(), body.end());
    std::size_t declared = a;
    if (structured) {
        if (mode == 0) {
            const auto value = input.word();
            switch (a % 4U) {
            case 0:
                wire = structured_varint<std::uint32_t>(value);
                break;
            case 1:
                wire = structured_varint<std::uint64_t>(value);
                break;
            case 2:
                wire = structured_varint<std::int32_t>(value);
                break;
            default:
                wire = structured_varint<std::int64_t>(value);
                break;
            }
        } else if (mode == 1) {
            const auto value = input.word();
            const bool big = (a & 4U) != 0;
            switch (a % 4U) {
            case 0:
                wire = structured_fixed<std::uint8_t>(value, big);
                break;
            case 1:
                wire = structured_fixed<std::uint16_t>(value, big);
                break;
            case 2:
                wire = structured_fixed<std::uint32_t>(value, big);
                break;
            default:
                wire = structured_fixed<std::uint64_t>(value, big);
                break;
            }
        } else if (mode == 2) {
            wire = structured_nullable(input);
        } else if (mode == 3) {
            wire = structured_varint<std::uint32_t>(input.word());
            auto tail = structured_nullable(input);
            wire.insert(wire.end(), tail.begin(), tail.end());
        } else if (mode == 4) {
            wire = structured_varint<std::uint64_t>(input.word());
        } else {
            wire = structured_map(input);
        }
        if (mode == 3 && a == 0) {
            declared = wire.size();
        }
        mutate(wire, mutation, index);
    }
    switch (mode) {
    case 0:
        switch (a % 4U) {
        case 0:
            verify_varint<std::uint32_t>(wire, boundary, split, depth);
            break;
        case 1:
            verify_varint<std::uint64_t>(wire, boundary, split, depth);
            break;
        case 2:
            verify_varint<std::int32_t>(wire, boundary, split, depth);
            break;
        default:
            verify_varint<std::int64_t>(wire, boundary, split, depth);
            break;
        }
        break;
    case 1: {
        const bool big = (a & 4U) != 0;
        switch (a % 4U) {
        case 0:
            verify_fixed<std::uint8_t>(wire, big, boundary, split, depth);
            break;
        case 1:
            verify_fixed<std::uint16_t>(wire, big, boundary, split, depth);
            break;
        case 2:
            if ((a & 8U) != 0) {
                verify_fixed<std::int32_t>(wire, big, boundary, split, depth);
            } else {
                verify_fixed<std::uint32_t>(wire, big, boundary, split, depth);
            }
            break;
        default:
            if ((a & 8U) != 0) {
                verify_fixed<std::int64_t>(wire, big, boundary, split, depth);
            } else {
                verify_fixed<std::uint64_t>(wire, big, boundary, split, depth);
            }
            break;
        }
        break;
    }
    case 2:
        verify_nullable(wire, a, boundary, split, depth);
        break;
    case 3:
        verify_exact(wire, declared, b, c, boundary, split, depth);
        break;
    case 4:
        verify_transaction(wire, b, boundary, split, depth);
        break;
    default:
        verify_ordered(wire, a % 17U, b % 18U, boundary, split, depth);
        break;
    }
}

} // namespace

extern "C" int
LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    if (size > max_input_size) {
        return 0;
    }
    std::vector<std::uint8_t> owned;
    if (size != 0) {
        owned.assign(data, data + size);
    }
    kwaque::runtime::testing::run_fuzz_input(
      [owned = std::move(owned)] { exercise(owned); });
    return 0;
}
