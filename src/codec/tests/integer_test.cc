#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/error.h"
#include "src/codec/integer.h"

#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using namespace std::literals;
namespace codec = kwaque::codec;
using codec::field_context;
using codec::input_boundary;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::bytes::fragmented_buffer;
using kwaque::bytes::fragmented_buffer_builder;
using kwaque::bytes::fragmented_buffer_builder_config;
using kwaque::bytes::fragmented_buffer_parser;

constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
constexpr field_context context{.origin = 100, .family = 7, .field = 11};

template<typename T>
concept fixed_api = requires(
  fragmented_buffer_parser& input, fragmented_buffer_builder& output, T value) {
    { codec::read_le<T>(input) } -> std::same_as<codec::result<T>>;
    { codec::read_be<T>(input) } -> std::same_as<codec::result<T>>;
    { codec::write_le(output, value) } -> std::same_as<codec::result<void>>;
    { codec::write_be(output, value) } -> std::same_as<codec::result<void>>;
};

template<typename T>
concept varuint_api = requires(
  fragmented_buffer_parser& input, fragmented_buffer_builder& output, T value) {
    { codec::read_varuint<T>(input) } -> std::same_as<codec::result<T>>;
    {
        codec::write_varuint(output, value)
    } -> std::same_as<codec::result<void>>;
};

template<typename T>
concept rejected_fixed_api = !requires(fragmented_buffer_parser& input) {
    codec::read_le<T>(input);
} && !requires(fragmented_buffer_parser& input) {
    codec::read_be<T>(input);
} && !requires(fragmented_buffer_builder& output, T value) {
    codec::write_le(output, value);
} && !requires(fragmented_buffer_builder& output, T value) {
    codec::write_be(output, value);
};

template<typename T>
concept rejected_varuint_api = !requires(fragmented_buffer_parser& input) {
    codec::read_varuint<T>(input);
} && !requires(fragmented_buffer_builder& output, T value) {
    codec::write_varuint(output, value);
};

struct converted_integer {
    operator std::uint32_t() const noexcept;
};

static_assert(fixed_api<std::uint8_t>);
static_assert(fixed_api<std::uint16_t>);
static_assert(fixed_api<std::uint32_t>);
static_assert(fixed_api<std::uint64_t>);
static_assert(fixed_api<std::int32_t>);
static_assert(fixed_api<std::int64_t>);
static_assert(rejected_fixed_api<std::int8_t>);
static_assert(rejected_fixed_api<std::int16_t>);
static_assert(rejected_fixed_api<bool>);
static_assert(rejected_fixed_api<float>);
static_assert(rejected_fixed_api<double>);
static_assert(rejected_fixed_api<converted_integer>);
static_assert(varuint_api<std::uint32_t>);
static_assert(varuint_api<std::uint64_t>);
static_assert(rejected_varuint_api<std::uint8_t>);
static_assert(rejected_varuint_api<std::uint16_t>);
static_assert(rejected_varuint_api<std::int32_t>);
static_assert(rejected_varuint_api<std::int64_t>);
static_assert(rejected_varuint_api<bool>);
static_assert(rejected_varuint_api<float>);
static_assert(rejected_varuint_api<double>);
static_assert(rejected_varuint_api<converted_integer>);

seastar::temporary_buffer<char> fragment_of(std::string_view bytes) {
    seastar::temporary_buffer<char> fragment{bytes.size()};
    std::ranges::copy(bytes, fragment.get_write());
    return fragment;
}

// Exact cuts avoid the builder's small-fragment packing policy.
fragmented_buffer
split_at(std::string_view bytes, const std::vector<std::size_t>& cuts = {}) {
    std::vector<seastar::temporary_buffer<char>> fragments;
    std::size_t previous = 0;
    for (const auto cut : cuts) {
        fragments.push_back(
          fragment_of(bytes.substr(previous, cut - previous)));
        previous = cut;
    }
    fragments.push_back(fragment_of(bytes.substr(previous)));
    return fragmented_buffer::copy_from_fragments(fragments).value();
}

std::vector<std::size_t> single_byte_cuts(std::size_t size) {
    std::vector<std::size_t> cuts;
    for (std::size_t cut = 1; cut < size; ++cut) {
        cuts.push_back(cut);
    }
    return cuts;
}

std::string unread_bytes(const fragmented_buffer_parser& input) {
    std::string bytes(input.bytes_remaining().value(), '\0');
    input.peek_to(std::span<char>{bytes}).value();
    return bytes;
}

void expect_error(
  const auto& value,
  errc code,
  std::uint64_t offset,
  field_context diagnostic = context) {
    ASSERT_FALSE(value.has_value());
    EXPECT_EQ(value.error().code(), code);
    EXPECT_EQ(value.error().family(), diagnostic.family);
    EXPECT_EQ(value.error().field(), diagnostic.field);
    EXPECT_EQ(value.error().byte_offset(), offset);
}

template<typename T, bool BigEndian>
void verify_fixed_value(T expected, std::string_view encoded) {
    ASSERT_EQ(encoded.size(), sizeof(T));
    fragmented_buffer_builder output;
    ASSERT_TRUE(output.append("pre"sv).has_value());
    const auto written = [&] {
        if constexpr (BigEndian) {
            return codec::write_be(output, expected, context);
        } else {
            return codec::write_le(output, expected, context);
        }
    }();
    ASSERT_TRUE(written.has_value());
    const auto published = output.finish();
    ASSERT_TRUE(published.has_value());
    EXPECT_TRUE(
      published->content_equals(std::string{"pre"} + std::string{encoded}));

    const auto bytes = std::string{"p"} + std::string{encoded} + "*";
    for (std::size_t cut = 0; cut <= bytes.size(); ++cut) {
        SCOPED_TRACE(cut);
        fragmented_buffer_parser input{split_at(bytes, {cut})};
        ASSERT_TRUE(input.skip(byte_count{1}).has_value());
        const auto value = [&] {
            if constexpr (BigEndian) {
                return codec::read_be<T>(input, context);
            } else {
                return codec::read_le<T>(input, context);
            }
        }();
        ASSERT_TRUE(value.has_value());
        EXPECT_EQ(*value, expected);
        EXPECT_EQ(input.bytes_consumed(), byte_count{1 + sizeof(T)});
        EXPECT_EQ(input.checkpoint_depth(), 0U);
        const auto following = codec::read_le<std::uint8_t>(input);
        ASSERT_TRUE(following.has_value());
        EXPECT_EQ(*following, 42U);
        EXPECT_TRUE(input.at_end());
    }
}

template<typename T>
void verify_fixed_orders(
  T expected, std::string_view little, std::string_view big) {
    verify_fixed_value<T, false>(expected, little);
    verify_fixed_value<T, true>(expected, big);
}

TEST(CodecIntegerTest, FixedWidthsUseLiteralEndianBytesAcrossEverySingleSplit) {
    verify_fixed_orders<std::uint8_t>(0xa5, "\xa5"sv, "\xa5"sv);
    verify_fixed_orders<std::uint16_t>(0x1234, "\x34\x12"sv, "\x12\x34"sv);
    verify_fixed_orders<std::uint32_t>(
      0x12345678, "\x78\x56\x34\x12"sv, "\x12\x34\x56\x78"sv);
    verify_fixed_orders<std::uint64_t>(
      0x0102030405060708,
      "\x08\x07\x06\x05\x04\x03\x02\x01"sv,
      "\x01\x02\x03\x04\x05\x06\x07\x08"sv);
    verify_fixed_orders<std::uint32_t>(
      0, "\x00\x00\x00\x00"sv, "\x00\x00\x00\x00"sv);
    verify_fixed_orders<std::uint64_t>(
      maximum,
      "\xff\xff\xff\xff\xff\xff\xff\xff"sv,
      "\xff\xff\xff\xff\xff\xff\xff\xff"sv);
}

TEST(CodecIntegerTest, SignedFixedWidthsPreserveLiteralTwosComplementExtremes) {
    verify_fixed_orders<std::int32_t>(
      -1, "\xff\xff\xff\xff"sv, "\xff\xff\xff\xff"sv);
    verify_fixed_orders<std::int32_t>(
      std::numeric_limits<std::int32_t>::min(),
      "\x00\x00\x00\x80"sv,
      "\x80\x00\x00\x00"sv);
    verify_fixed_orders<std::int32_t>(
      std::numeric_limits<std::int32_t>::max(),
      "\xff\xff\xff\x7f"sv,
      "\x7f\xff\xff\xff"sv);
    verify_fixed_orders<std::int64_t>(
      -1,
      "\xff\xff\xff\xff\xff\xff\xff\xff"sv,
      "\xff\xff\xff\xff\xff\xff\xff\xff"sv);
    verify_fixed_orders<std::int64_t>(
      std::numeric_limits<std::int64_t>::min(),
      "\x00\x00\x00\x00\x00\x00\x00\x80"sv,
      "\x80\x00\x00\x00\x00\x00\x00\x00"sv);
    verify_fixed_orders<std::int64_t>(
      std::numeric_limits<std::int64_t>::max(),
      "\xff\xff\xff\xff\xff\xff\xff\x7f"sv,
      "\x7f\xff\xff\xff\xff\xff\xff\xff"sv);
}

template<typename T>
void verify_fixed_truncations() {
    for (const auto boundary :
         {input_boundary::open, input_boundary::complete}) {
        const auto code = boundary == input_boundary::open
                            ? errc::truncated_data
                            : errc::malformed_data;
        for (std::size_t available = 0; available < sizeof(T); ++available) {
            const auto bytes = std::string{"xy"} + std::string(available, 'z');
            for (const bool big : {false, true}) {
                fragmented_buffer_parser input{
                  split_at(bytes, single_byte_cuts(bytes.size()))};
                ASSERT_TRUE(input.skip(byte_count{2}).has_value());
                const auto before = unread_bytes(input);
                const auto value
                  = big ? codec::read_be<T>(input, context, boundary)
                        : codec::read_le<T>(input, context, boundary);
                expect_error(value, code, 102U + available);
                EXPECT_EQ(input.bytes_consumed(), byte_count{2});
                EXPECT_EQ(input.checkpoint_depth(), 0U);
                EXPECT_EQ(unread_bytes(input), before);
            }
        }
    }
}

TEST(CodecIntegerTest, EveryFixedWidthTruncationReportsFirstMissingOuterByte) {
    verify_fixed_truncations<std::uint8_t>();
    verify_fixed_truncations<std::uint16_t>();
    verify_fixed_truncations<std::uint32_t>();
    verify_fixed_truncations<std::uint64_t>();
    verify_fixed_truncations<std::int32_t>();
    verify_fixed_truncations<std::int64_t>();
}

struct varuint_case {
    std::uint64_t value;
    std::string_view encoded;
};

constexpr std::array varuint_cases{
  varuint_case{0, "\x00"sv},
  varuint_case{1, "\x01"sv},
  varuint_case{63, "\x3f"sv},
  varuint_case{64, "\x40"sv},
  varuint_case{127, "\x7f"sv},
  varuint_case{128, "\x80\x01"sv},
  varuint_case{129, "\x81\x01"sv},
  varuint_case{255, "\xff\x01"sv},
  varuint_case{256, "\x80\x02"sv},
  varuint_case{8191, "\xff\x3f"sv},
  varuint_case{8192, "\x80\x40"sv},
  varuint_case{16383, "\xff\x7f"sv},
  varuint_case{16384, "\x80\x80\x01"sv},
  varuint_case{131071, "\xff\xff\x07"sv},
  varuint_case{1048575, "\xff\xff\x3f"sv},
  varuint_case{1048576, "\x80\x80\x40"sv},
  varuint_case{2097152, "\x80\x80\x80\x01"sv},
  varuint_case{16777215, "\xff\xff\xff\x07"sv},
  varuint_case{268435455, "\xff\xff\xff\x7f"sv},
  varuint_case{268435456, "\x80\x80\x80\x80\x01"sv},
  varuint_case{2147483647, "\xff\xff\xff\xff\x07"sv},
  varuint_case{2147483648, "\x80\x80\x80\x80\x08"sv},
  varuint_case{4294967232, "\xc0\xff\xff\xff\x0f"sv},
  varuint_case{4294967295, "\xff\xff\xff\xff\x0f"sv},
  varuint_case{8589934591, "\xff\xff\xff\xff\x1f"sv},
  varuint_case{34359738368, "\x80\x80\x80\x80\x80\x01"sv},
  varuint_case{4398046511104, "\x80\x80\x80\x80\x80\x80\x01"sv},
  varuint_case{562949953421312, "\x80\x80\x80\x80\x80\x80\x80\x01"sv},
  varuint_case{72057594037927936, "\x80\x80\x80\x80\x80\x80\x80\x80\x01"sv},
  varuint_case{
    9223372036854775808ULL, "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x01"sv},
  varuint_case{maximum, "\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01"sv}};

template<typename T>
class CodecVaruintTest : public ::testing::Test {};
using varuint_types = ::testing::Types<std::uint32_t, std::uint64_t>;
TYPED_TEST_SUITE(CodecVaruintTest, varuint_types);

TYPED_TEST(
  CodecVaruintTest, LiteralEncodingsAndFollowingValuesAtEverySingleSplit) {
    for (const auto& input_case : varuint_cases) {
        if (input_case.value > std::numeric_limits<TypeParam>::max()) {
            continue;
        }
        SCOPED_TRACE(input_case.value);
        fragmented_buffer_builder output;
        ASSERT_TRUE(output.append("p"sv).has_value());
        ASSERT_TRUE(
          codec::write_varuint(
            output, static_cast<TypeParam>(input_case.value), context)
            .has_value());
        const auto published = output.finish();
        ASSERT_TRUE(published.has_value());
        EXPECT_TRUE(published->content_equals(
          std::string{"p"} + std::string{input_case.encoded}));

        const auto bytes = std::string{"p"} + std::string{input_case.encoded}
                           + "*";
        for (std::size_t cut = 0; cut <= bytes.size(); ++cut) {
            fragmented_buffer_parser input{split_at(bytes, {cut})};
            ASSERT_TRUE(input.skip(byte_count{1}).has_value());
            const auto value = codec::read_varuint<TypeParam>(input, context);
            ASSERT_TRUE(value.has_value()) << cut;
            EXPECT_EQ(*value, input_case.value);
            EXPECT_EQ(
              input.bytes_consumed().value(), 1U + input_case.encoded.size());
            const auto following = codec::read_varuint<TypeParam>(
              input, context);
            ASSERT_TRUE(following.has_value());
            EXPECT_EQ(*following, 42U);
            EXPECT_TRUE(input.at_end());
        }
    }
}

TYPED_TEST(CodecVaruintTest, EveryLiteralTruncationPreservesCursorAndInput) {
    for (const auto& input_case : varuint_cases) {
        if (input_case.value > std::numeric_limits<TypeParam>::max()) {
            continue;
        }
        for (std::size_t length = 0; length < input_case.encoded.size();
             ++length) {
            const auto bytes = std::string{"xy"}
                               + std::string{
                                 input_case.encoded.substr(0, length)};
            for (const auto boundary :
                 {input_boundary::open, input_boundary::complete}) {
                fragmented_buffer_parser input{
                  split_at(bytes, single_byte_cuts(bytes.size()))};
                ASSERT_TRUE(input.skip(byte_count{2}).has_value());
                const auto before = unread_bytes(input);
                const auto value = codec::read_varuint<TypeParam>(
                  input, context, boundary);
                expect_error(
                  value,
                  boundary == input_boundary::open ? errc::truncated_data
                                                   : errc::malformed_data,
                  102U + length);
                EXPECT_EQ(input.bytes_consumed(), byte_count{2});
                EXPECT_EQ(input.checkpoint_depth(), 0U);
                EXPECT_EQ(unread_bytes(input), before);
            }
        }
    }
}

TYPED_TEST(
  CodecVaruintTest,
  EveryFinalByteDistinguishesCanonicalPayloadAndMalformedBits) {
    constexpr bool narrow = std::same_as<TypeParam, std::uint32_t>;
    constexpr std::size_t prefix_size = narrow ? 4U : 9U;
    constexpr std::uint64_t terminal_max = narrow ? 15U : 1U;
    constexpr std::uint64_t place = narrow ? 0x10000000ULL
                                           : 0x8000000000000000ULL;
    for (const std::uint8_t prefix_octet :
         {std::uint8_t{0x80}, std::uint8_t{0xff}}) {
        for (std::uint64_t terminal = 0; terminal < 256; ++terminal) {
            SCOPED_TRACE(
              ::testing::Message()
              << unsigned(prefix_octet) << ':' << terminal);
            std::string encoded(prefix_size, std::bit_cast<char>(prefix_octet));
            encoded.push_back(
              std::bit_cast<char>(static_cast<std::uint8_t>(terminal)));
            for (const bool following_present : {false, true}) {
                const auto bytes = std::string{"xy"} + encoded
                                   + (following_present ? "*" : "");
                for (const auto boundary :
                     {input_boundary::open, input_boundary::complete}) {
                    fragmented_buffer_parser input{
                      split_at(bytes, {2, 2 + prefix_size})};
                    ASSERT_TRUE(input.skip(byte_count{2}).has_value());
                    const auto before = unread_bytes(input);
                    const auto value = codec::read_varuint<TypeParam>(
                      input, context, boundary);
                    if (terminal >= 1U && terminal <= terminal_max) {
                        ASSERT_TRUE(value.has_value());
                        const auto low = prefix_octet == 0xff ? place - 1U : 0U;
                        EXPECT_EQ(*value, terminal * place + low);
                        EXPECT_EQ(
                          input.bytes_consumed().value(), 3U + prefix_size);
                        if (following_present) {
                            const auto next = codec::read_varuint<TypeParam>(
                              input);
                            ASSERT_TRUE(next.has_value());
                            EXPECT_EQ(*next, 42U);
                        }
                        EXPECT_TRUE(input.at_end());
                    } else {
                        expect_error(
                          value, errc::malformed_data, 102U + prefix_size);
                        EXPECT_EQ(input.bytes_consumed(), byte_count{2});
                        EXPECT_EQ(unread_bytes(input), before);
                    }
                    EXPECT_EQ(input.checkpoint_depth(), 0U);
                }
            }
        }
    }
}

TYPED_TEST(CodecVaruintTest, RedundantZeroGroupsRejectAtTheirTerminalByte) {
    for (const auto encoded :
         {"\x80\x00"sv, "\xff\x00"sv, "\x81\x80\x00"sv, "\xff\xff\x00"sv}) {
        const auto bytes = std::string{"p"} + std::string{encoded} + "*";
        fragmented_buffer_parser input{
          split_at(bytes, single_byte_cuts(bytes.size()))};
        ASSERT_TRUE(input.skip(byte_count{1}).has_value());
        const auto before = unread_bytes(input);
        const auto value = codec::read_varuint<TypeParam>(input, context);
        expect_error(value, errc::malformed_data, 100U + encoded.size());
        EXPECT_EQ(input.bytes_consumed(), byte_count{1});
        EXPECT_EQ(unread_bytes(input), before);
    }
}

TEST(CodecIntegerTest, InvalidBoundaryAndWholeExtentRejectBeforeAnyRead) {
    const auto verify = [](auto read) {
        const auto bytes = "\x01\x2a"sv;
        for (const auto boundary :
             {input_boundary::open, input_boundary::complete}) {
            fragmented_buffer_parser input{split_at(bytes, {1})};
            ASSERT_TRUE(input.skip(byte_count{1}).has_value());
            const field_context overflowing{
              .origin = maximum - 1U, .family = 65535, .field = 65535};
            const auto value = read(input, overflowing, boundary);
            expect_error(
              value, errc::invalid_argument, maximum - 1U, overflowing);
            EXPECT_EQ(input.bytes_consumed(), byte_count{1});
            EXPECT_EQ(unread_bytes(input), "*"sv);
        }
        for (const auto raw : {std::uint8_t{2}, std::uint8_t{255}}) {
            fragmented_buffer_parser input{split_at(bytes)};
            const auto value = read(
              input, context, static_cast<input_boundary>(raw));
            expect_error(value, errc::invalid_argument, context.origin);
            EXPECT_EQ(input.bytes_consumed(), byte_count{});
            EXPECT_EQ(unread_bytes(input), bytes);
        }
    };
    verify(codec::read_le<std::uint8_t>);
    verify(codec::read_be<std::uint32_t>);
    verify(codec::read_le<std::int64_t>);
    verify(codec::read_varuint<std::uint32_t>);
    verify(codec::read_varuint<std::uint64_t>);
}

TEST(
  CodecIntegerTest, ValidOriginsAtMaximumPreserveSuccessAndFailureCoordinates) {
    const field_context final_byte{
      .origin = maximum - 1U, .family = 17, .field = 31};
    fragmented_buffer_parser valid{split_at("\x01"sv)};
    const auto value = codec::read_varuint<std::uint64_t>(valid, final_byte);
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(*value, 1U);
    EXPECT_TRUE(valid.at_end());

    const field_context edge{.origin = maximum - 2U, .family = 17, .field = 31};
    for (const auto boundary :
         {input_boundary::open, input_boundary::complete}) {
        fragmented_buffer_parser input{split_at("x\x80"sv, {1})};
        ASSERT_TRUE(input.skip(byte_count{1}).has_value());
        const auto code = boundary == input_boundary::open
                            ? errc::truncated_data
                            : errc::malformed_data;
        expect_error(
          codec::read_varuint<std::uint32_t>(input, edge, boundary),
          code,
          maximum,
          edge);
        expect_error(
          codec::read_be<std::uint16_t>(input, edge, boundary),
          code,
          maximum,
          edge);
        EXPECT_EQ(input.bytes_consumed(), byte_count{1});
        EXPECT_EQ(unread_bytes(input), "\x80"sv);
    }
    fragmented_buffer_parser malformed{split_at("\x80\x00"sv, {1})};
    expect_error(
      codec::read_varuint<std::uint64_t>(malformed, edge),
      errc::malformed_data,
      maximum - 1U,
      edge);
    EXPECT_EQ(malformed.bytes_consumed(), byte_count{});
    fragmented_buffer_parser empty;
    const field_context at_end{.origin = maximum, .family = 17, .field = 31};
    expect_error(
      codec::read_le<std::uint8_t>(empty, at_end),
      errc::truncated_data,
      maximum,
      at_end);
}

TEST(
  CodecIntegerTest, ScalarsWorkAtFullCheckpointDepthWithoutTakingAnotherSlot) {
    const auto bytes = "\x78\x56\x34\x12\x80\x01\x80"sv;
    fragmented_buffer_parser input{
      split_at(bytes, single_byte_cuts(bytes.size()))};
    for (std::size_t depth = 0; depth < 8; ++depth) {
        ASSERT_TRUE(input.push_checkpoint().has_value());
    }
    const auto fixed = codec::read_le<std::uint32_t>(input, context);
    ASSERT_TRUE(fixed.has_value());
    EXPECT_EQ(*fixed, 0x12345678U);
    EXPECT_EQ(input.checkpoint_depth(), 8U);
    const auto varuint = codec::read_varuint<std::uint64_t>(input, context);
    ASSERT_TRUE(varuint.has_value());
    EXPECT_EQ(*varuint, 128U);
    EXPECT_EQ(input.checkpoint_depth(), 8U);
    expect_error(
      codec::read_varuint<std::uint32_t>(input, context),
      errc::truncated_data,
      107);
    EXPECT_EQ(input.bytes_consumed(), byte_count{6});
    EXPECT_EQ(input.checkpoint_depth(), 8U);
    EXPECT_EQ(unread_bytes(input), "\x80"sv);
    for (std::size_t depth = 8; depth != 0; --depth) {
        ASSERT_TRUE(input.rollback().has_value());
        EXPECT_EQ(input.checkpoint_depth(), depth - 1U);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
    }
    EXPECT_EQ(unread_bytes(input), bytes);
}

TEST(CodecIntegerTest, ScalarReadsNeedNoAllocationOnSuccessOrTypedFailure) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation failure injection is not enabled";
#else
    struct read_case {
        const char* name;
        std::string_view encoded;
        codec::result<std::uint64_t> (*read)(
          fragmented_buffer_parser&, field_context, input_boundary);
        std::optional<std::uint64_t> expected;
        errc code;
        std::uint64_t offset;
    };
    const std::array cases{
      read_case{
        "little endian",
        "\x08\x07\x06\x05\x04\x03\x02\x01"sv,
        codec::read_le<std::uint64_t>,
        std::uint64_t{0x0102030405060708},
        errc::success,
        0},
      read_case{
        "big endian",
        "\x01\x02\x03\x04\x05\x06\x07\x08"sv,
        codec::read_be<std::uint64_t>,
        std::uint64_t{0x0102030405060708},
        errc::success,
        0},
      read_case{
        "fixed shortage",
        "\x01"sv,
        codec::read_le<std::uint64_t>,
        std::nullopt,
        errc::truncated_data,
        102},
      read_case{
        "varuint",
        "\x80\x01"sv,
        codec::read_varuint<std::uint64_t>,
        std::uint64_t{128},
        errc::success,
        0},
      read_case{
        "malformed varuint",
        "\x80\x00"sv,
        codec::read_varuint<std::uint64_t>,
        std::nullopt,
        errc::malformed_data,
        102},
      read_case{
        "truncated varuint",
        "\x80"sv,
        codec::read_varuint<std::uint64_t>,
        std::nullopt,
        errc::truncated_data,
        102}};
    for (const auto& input_case : cases) {
        SCOPED_TRACE(input_case.name);
        const auto bytes = std::string{"p"} + std::string{input_case.encoded};
        fragmented_buffer_parser input{
          split_at(bytes, single_byte_cuts(bytes.size()))};
        ASSERT_TRUE(input.skip(byte_count{1}).has_value());
        std::optional<codec::result<std::uint64_t>> value;
        bool threw = false;
        auto& injector = seastar::memory::local_failure_injector();
        const auto allocations = injector.alloc_count();
        injector.fail_after(0);
        try {
            value.emplace(
              input_case.read(input, context, input_boundary::open));
        } catch (const std::bad_alloc&) {
            threw = true;
        } catch (...) {
            injector.cancel();
            throw;
        }
        const bool injected = injector.failed();
        const auto allocations_after = injector.alloc_count();
        injector.cancel();
        ASSERT_FALSE(threw);
        EXPECT_FALSE(injected);
        EXPECT_EQ(allocations_after, allocations);
        ASSERT_TRUE(value.has_value());
        if (input_case.expected) {
            ASSERT_TRUE(value->has_value());
            EXPECT_EQ(**value, *input_case.expected);
            EXPECT_TRUE(input.at_end());
        } else {
            expect_error(*value, input_case.code, input_case.offset);
            EXPECT_EQ(input.bytes_consumed(), byte_count{1});
            EXPECT_EQ(unread_bytes(input), input_case.encoded);
        }
        EXPECT_EQ(input.checkpoint_depth(), 0U);
    }
#endif
}

struct writer_case {
    const char* name;
    codec::result<void> (*write)(fragmented_buffer_builder&, field_context);
    std::string_view encoded;
};

const std::array writers{
  writer_case{
    "little endian",
    [](fragmented_buffer_builder& output, field_context diagnostic) {
        return codec::write_le(
          output, std::uint64_t{0x0102030405060708}, diagnostic);
    },
    "\x08\x07\x06\x05\x04\x03\x02\x01"sv},
  writer_case{
    "big endian signed",
    [](fragmented_buffer_builder& output, field_context diagnostic) {
        return codec::write_be(
          output, std::numeric_limits<std::int64_t>::min(), diagnostic);
    },
    "\x80\x00\x00\x00\x00\x00\x00\x00"sv},
  writer_case{
    "varuint",
    [](fragmented_buffer_builder& output, field_context diagnostic) {
        return codec::write_varuint(output, maximum, diagnostic);
    },
    "\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01"sv}};

TEST(CodecIntegerTest, WriterCoordinateChecksUseActualEncodedLength) {
    for (const auto& input : varuint_cases) {
        fragmented_buffer_builder output;
        ASSERT_TRUE(output.append("p"sv).has_value());
        const field_context edge{
          .origin = maximum - 1U - input.encoded.size(),
          .family = 17,
          .field = 31};
        ASSERT_TRUE(
          codec::write_varuint(output, input.value, edge).has_value());
        expect_error(
          codec::write_varuint(output, std::uint32_t{0}, edge),
          errc::invalid_argument,
          maximum,
          edge);
        const auto published = output.finish();
        ASSERT_TRUE(published.has_value());
        EXPECT_TRUE(published->content_equals(
          std::string{"p"} + std::string{input.encoded}));
    }
    for (const auto& writer : writers) {
        fragmented_buffer_builder output;
        ASSERT_TRUE(output.append("p"sv).has_value());
        const field_context edge{
          .origin = maximum - 1U - writer.encoded.size(),
          .family = 17,
          .field = 31};
        ASSERT_TRUE(writer.write(output, edge).has_value());
        expect_error(
          codec::write_le(output, std::uint8_t{0}, edge),
          errc::invalid_argument,
          maximum,
          edge);
        const auto published = output.finish();
        ASSERT_TRUE(published.has_value());
        EXPECT_TRUE(published->content_equals(
          std::string{"p"} + std::string{writer.encoded}));
    }
}

TEST(
  CodecIntegerTest, InvalidWriterOriginsAndClosedBuildersPreservePriorOutput) {
    for (const auto& writer : writers) {
        SCOPED_TRACE(writer.name);
        fragmented_buffer_builder output;
        ASSERT_TRUE(output.append("ab"sv).has_value());
        const auto retained = output.retained_bytes();
        const auto fragments = output.fragment_count();
        const auto tail = output.tail_capacity();
        const field_context invalid_origin{
          .origin = maximum - 1U, .family = 17, .field = 31};
        expect_error(
          writer.write(output, invalid_origin),
          errc::invalid_argument,
          maximum - 1U,
          invalid_origin);
        const field_context invalid_end{
          .origin = maximum - 2U, .family = 17, .field = 31};
        expect_error(
          writer.write(output, invalid_end),
          errc::invalid_argument,
          maximum,
          invalid_end);
        EXPECT_EQ(output.size(), byte_count{2});
        EXPECT_EQ(output.retained_bytes(), retained);
        EXPECT_EQ(output.fragment_count(), fragments);
        EXPECT_EQ(output.tail_capacity(), tail);
        const auto published = output.finish();
        ASSERT_TRUE(published.has_value());
        EXPECT_TRUE(published->content_equals("ab"sv));
        expect_error(
          writer.write(output, context), errc::closed, context.origin);
        EXPECT_TRUE(output.finished());
        EXPECT_TRUE(published->content_equals("ab"sv));
    }
}

TEST(CodecIntegerTest, WriterCapRejectionIsAtomicAndLeavesTheTailUsable) {
    for (const auto& writer : writers) {
        SCOPED_TRACE(writer.name);
        fragmented_buffer_builder_config config;
        config.initial_fragment_bytes = byte_count{5};
        config.max_fragment_bytes = byte_count{5};
        config.max_total_bytes = byte_count{5};
        fragmented_buffer_builder output{config};
        ASSERT_TRUE(output.append("head"sv).has_value());
        const auto retained = output.retained_bytes();
        const auto fragments = output.fragment_count();
        const auto tail = output.tail_capacity();
        expect_error(
          writer.write(output, context), errc::resource_exhausted, 104);
        EXPECT_EQ(output.size(), byte_count{4});
        EXPECT_EQ(output.retained_bytes(), retained);
        EXPECT_EQ(output.fragment_count(), fragments);
        EXPECT_EQ(output.tail_capacity(), tail);
        ASSERT_TRUE(
          codec::write_le(output, std::uint8_t{'!'}, context).has_value());
        const auto published = output.finish();
        ASSERT_TRUE(published.has_value());
        EXPECT_TRUE(published->content_equals("head!"sv));
    }
}

TEST(
  CodecIntegerTest, ScalarAllocationFailuresPropagateAndRestoreExistingBytes) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation failure injection is not enabled";
#else
    for (const auto& writer : writers) {
        SCOPED_TRACE(writer.name);
        std::size_t failures = 0;
        bool completed = false;
        for (std::uint64_t fail_after = 0; fail_after < 16; ++fail_after) {
            fragmented_buffer_builder_config config;
            config.initial_fragment_bytes = byte_count{8};
            config.max_fragment_bytes = byte_count{8};
            fragmented_buffer_builder output{config};
            ASSERT_TRUE(output.append("head"sv).has_value());
            const auto retained = output.retained_bytes();
            const auto fragments = output.fragment_count();
            const auto tail = output.tail_capacity();
            std::optional<codec::result<void>> appended;
            bool threw = false;
            auto& injector = seastar::memory::local_failure_injector();
            injector.fail_after(fail_after);
            try {
                appended.emplace(writer.write(output, context));
            } catch (const std::bad_alloc&) {
                threw = true;
            } catch (...) {
                injector.cancel();
                throw;
            }
            const bool injected = injector.failed();
            injector.cancel();
            if (injected) {
                ++failures;
                EXPECT_TRUE(threw);
                EXPECT_FALSE(appended.has_value());
                EXPECT_EQ(output.size(), byte_count{4});
                EXPECT_EQ(output.retained_bytes(), retained);
                EXPECT_EQ(output.fragment_count(), fragments);
                EXPECT_EQ(output.tail_capacity(), tail);
                ASSERT_TRUE(output.append("ok"sv).has_value());
                const auto published = output.finish();
                ASSERT_TRUE(published.has_value());
                EXPECT_TRUE(published->content_equals("headok"sv));
            } else {
                EXPECT_FALSE(threw);
                ASSERT_TRUE(appended.has_value());
                ASSERT_TRUE(appended->has_value());
                const auto published = output.finish();
                ASSERT_TRUE(published.has_value());
                EXPECT_TRUE(published->content_equals(
                  std::string{"head"} + std::string{writer.encoded}));
                completed = true;
                break;
            }
        }
        EXPECT_GT(failures, 0U);
        EXPECT_TRUE(completed);
    }
#endif
}

} // namespace
