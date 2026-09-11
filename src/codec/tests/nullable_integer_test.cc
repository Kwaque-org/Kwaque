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
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

using namespace std::literals;
namespace codec = kwaque::codec;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::bytes::fragmented_buffer;
using kwaque::bytes::fragmented_buffer_builder;
using kwaque::bytes::fragmented_buffer_builder_config;
using kwaque::bytes::fragmented_buffer_parser;

constexpr codec::field_context context{.origin = 100, .family = 7, .field = 11};
constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();

template<typename T>
concept varint_api = requires(
  fragmented_buffer_parser& input, fragmented_buffer_builder& output, T value) {
    { codec::read_varint<T>(input) } -> std::same_as<codec::result<T>>;
    { codec::write_varint(output, value) } -> std::same_as<codec::result<void>>;
    codec::encode_zigzag(value);
};

template<typename T>
concept rejected_varint_api = !requires(fragmented_buffer_parser& input) {
    codec::read_varint<T>(input);
} && !requires(fragmented_buffer_builder& output, T value) {
    codec::write_varint(output, value);
} && !requires(T value) { codec::encode_zigzag(value); };

struct converted_integer {
    operator std::int32_t() const noexcept;
};

static_assert(varint_api<std::int32_t>);
static_assert(varint_api<std::int64_t>);
static_assert(rejected_varint_api<std::uint32_t>);
static_assert(rejected_varint_api<std::uint64_t>);
static_assert(rejected_varint_api<std::int8_t>);
static_assert(rejected_varint_api<std::int16_t>);
static_assert(rejected_varint_api<bool>);
static_assert(rejected_varint_api<float>);
static_assert(rejected_varint_api<converted_integer>);
static_assert(
  std::same_as<decltype(codec::encode_zigzag(std::int32_t{})), std::uint32_t>);
static_assert(
  std::same_as<decltype(codec::encode_zigzag(std::int64_t{})), std::uint64_t>);
static_assert(
  std::same_as<decltype(codec::decode_zigzag(std::uint32_t{})), std::int32_t>);
static_assert(
  std::same_as<decltype(codec::decode_zigzag(std::uint64_t{})), std::int64_t>);
static_assert(noexcept(codec::encode_zigzag(std::int64_t{})));
static_assert(noexcept(codec::decode_zigzag(std::uint64_t{})));
static_assert(codec::encode_zigzag(std::int32_t{-1}) == 1U);
static_assert(codec::encode_zigzag(std::int64_t{-2}) == 3U);
static_assert(
  codec::encode_zigzag(std::numeric_limits<std::int32_t>::min())
  == std::numeric_limits<std::uint32_t>::max());
static_assert(
  codec::encode_zigzag(std::numeric_limits<std::int64_t>::min()) == maximum);
static_assert(
  codec::decode_zigzag(std::uint32_t{0xffffffff})
  == std::numeric_limits<std::int32_t>::min());
static_assert(
  codec::decode_zigzag(maximum) == std::numeric_limits<std::int64_t>::min());
static_assert(std::same_as<
              decltype(codec::read_nullable_length(
                std::declval<fragmented_buffer_parser&>(), byte_count{})),
              codec::result<std::optional<byte_count>>>);
static_assert(std::same_as<
              decltype(codec::write_nullable_length(
                std::declval<fragmented_buffer_builder&>(),
                std::optional<byte_count>{},
                byte_count{})),
              codec::result<void>>);

fragmented_buffer byte_fragments(std::string_view bytes) {
    std::vector<seastar::temporary_buffer<char>> fragments;
    for (const auto value : bytes) {
        seastar::temporary_buffer<char> fragment{1};
        fragment.get_write()[0] = value;
        fragments.push_back(std::move(fragment));
    }
    return fragmented_buffer::copy_from_fragments(fragments).value();
}

std::string unread_bytes(const fragmented_buffer_parser& input) {
    std::string bytes(input.bytes_remaining().value(), '\0');
    input.peek_to(std::span<char>{bytes}).value();
    return bytes;
}

void expect_error(
  const auto& result,
  errc reason,
  std::uint64_t offset,
  codec::field_context diagnostic = context) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), reason);
    EXPECT_EQ(result.error().family(), diagnostic.family);
    EXPECT_EQ(result.error().field(), diagnostic.field);
    EXPECT_EQ(result.error().byte_offset(), offset);
}

template<typename T>
struct signed_case {
    T value;
    std::string_view encoded;
};

constexpr std::array signed32_cases{
  signed_case<std::int32_t>{0, "\x00"sv},
  signed_case<std::int32_t>{-1, "\x01"sv},
  signed_case<std::int32_t>{1, "\x02"sv},
  signed_case<std::int32_t>{-2, "\x03"sv},
  signed_case<std::int32_t>{63, "\x7e"sv},
  signed_case<std::int32_t>{-64, "\x7f"sv},
  signed_case<std::int32_t>{64, "\x80\x01"sv},
  signed_case<std::int32_t>{-65, "\x81\x01"sv},
  signed_case<std::int32_t>{8192, "\x80\x80\x01"sv},
  signed_case<std::int32_t>{-8193, "\x81\x80\x01"sv},
  signed_case<std::int32_t>{
    std::numeric_limits<std::int32_t>::max(), "\xfe\xff\xff\xff\x0f"sv},
  signed_case<std::int32_t>{
    std::numeric_limits<std::int32_t>::min(), "\xff\xff\xff\xff\x0f"sv}};

constexpr std::array signed64_cases{
  signed_case<std::int64_t>{0, "\x00"sv},
  signed_case<std::int64_t>{-1, "\x01"sv},
  signed_case<std::int64_t>{1, "\x02"sv},
  signed_case<std::int64_t>{-2, "\x03"sv},
  signed_case<std::int64_t>{64, "\x80\x01"sv},
  signed_case<std::int64_t>{-65, "\x81\x01"sv},
  signed_case<std::int64_t>{8192, "\x80\x80\x01"sv},
  signed_case<std::int64_t>{-8193, "\x81\x80\x01"sv},
  signed_case<std::int64_t>{17179869184, "\x80\x80\x80\x80\x80\x01"sv},
  signed_case<std::int64_t>{-17179869185, "\x81\x80\x80\x80\x80\x01"sv},
  signed_case<std::int64_t>{
    std::numeric_limits<std::int64_t>::max(),
    "\xfe\xff\xff\xff\xff\xff\xff\xff\xff\x01"sv},
  signed_case<std::int64_t>{
    std::numeric_limits<std::int64_t>::min(),
    "\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01"sv}};

template<typename T, std::size_t Size>
void verify_signed_cases(const std::array<signed_case<T>, Size>& cases) {
    for (const auto& value : cases) {
        SCOPED_TRACE(value.value);
        fragmented_buffer_builder output;
        ASSERT_TRUE(output.append("p"sv).has_value());
        ASSERT_TRUE(
          codec::write_varint(output, value.value, context).has_value());
        auto encoded = output.finish();
        ASSERT_TRUE(encoded.has_value());
        EXPECT_TRUE(encoded->content_equals(
          std::string{"p"} + std::string{value.encoded}));

        const auto bytes = std::string{"p"} + std::string{value.encoded} + "!";
        fragmented_buffer_parser input{byte_fragments(bytes)};
        ASSERT_TRUE(input.skip(byte_count{1}).has_value());
        const auto decoded = codec::read_varint<T>(input, context);
        ASSERT_TRUE(decoded.has_value());
        EXPECT_EQ(*decoded, value.value);
        EXPECT_EQ(input.bytes_consumed(), byte_count{1 + value.encoded.size()});
        EXPECT_EQ(unread_bytes(input), "!"sv);
        EXPECT_EQ(input.checkpoint_depth(), 0U);

        for (const auto boundary :
             {codec::input_boundary::open, codec::input_boundary::complete}) {
            for (std::size_t cut = 0; cut < value.encoded.size(); ++cut) {
                const auto prefix = std::string{"p"}
                                    + std::string{value.encoded.substr(0, cut)};
                fragmented_buffer_parser truncated{byte_fragments(prefix)};
                ASSERT_TRUE(truncated.skip(byte_count{1}).has_value());
                expect_error(
                  codec::read_varint<T>(truncated, context, boundary),
                  boundary == codec::input_boundary::open
                    ? errc::truncated_data
                    : errc::malformed_data,
                  101 + cut);
                EXPECT_EQ(truncated.bytes_consumed(), byte_count{1});
                EXPECT_EQ(
                  unread_bytes(truncated), value.encoded.substr(0, cut));
            }
        }
    }
}

TEST(CodecNullableIntegerTest, Signed32UsesIndependentLiteralBytes) {
    verify_signed_cases(signed32_cases);
}

TEST(CodecNullableIntegerTest, Signed64UsesIndependentLiteralBytes) {
    verify_signed_cases(signed64_cases);
}

TEST(CodecNullableIntegerTest, NullEmptyAndPayloadLengthsCommitOnlyThePrefix) {
    struct length_case {
        std::string_view encoded;
        std::string_view payload;
        std::optional<byte_count> expected;
        byte_count allowance;
    };
    const std::array cases{
      length_case{"\x01"sv, "!"sv, std::nullopt, byte_count{}},
      length_case{"\x00"sv, "!"sv, byte_count{}, byte_count{}},
      length_case{"\x04"sv, "AB!"sv, byte_count{2}, byte_count{2}},
      length_case{"\x06"sv, "ABC"sv, byte_count{3}, byte_count{9}}};
    for (const auto& value : cases) {
        const auto bytes = std::string{"p"} + std::string{value.encoded}
                           + std::string{value.payload};
        fragmented_buffer_parser input{byte_fragments(bytes)};
        ASSERT_TRUE(input.skip(byte_count{1}).has_value());
        const auto decoded = codec::read_nullable_length(
          input, value.allowance, context, codec::input_boundary::complete);
        ASSERT_TRUE(decoded.has_value());
        EXPECT_EQ(*decoded, value.expected);
        EXPECT_EQ(input.bytes_consumed(), byte_count{1 + value.encoded.size()});
        EXPECT_EQ(unread_bytes(input), value.payload);
        fragmented_buffer_builder output;
        ASSERT_TRUE(
          codec::write_nullable_length(
            output, value.expected, value.allowance, context)
            .has_value());
        const auto encoded = output.finish();
        ASSERT_TRUE(encoded.has_value());
        EXPECT_TRUE(encoded->content_equals(value.encoded));
    }
}

TEST(CodecNullableIntegerTest, InvalidLengthsAndCapsRejectBeforeConsumption) {
    struct invalid_case {
        std::string_view encoded;
        byte_count allowance;
        errc reason;
        std::uint64_t offset;
    };
    const std::array cases{
      invalid_case{"\x03"sv, byte_count{}, errc::malformed_data, 101},
      invalid_case{
        "\xff\xff\xff\xff\x0f"sv,
        byte_count{maximum},
        errc::malformed_data,
        101},
      invalid_case{"\x80\x00"sv, byte_count{}, errc::malformed_data, 102},
      invalid_case{
        "\xff\xff\xff\xff\x1f"sv,
        byte_count{maximum},
        errc::malformed_data,
        105},
      invalid_case{
        "\x80\x80\x80\x80\x80"sv,
        byte_count{maximum},
        errc::malformed_data,
        105},
      invalid_case{"\x08"sv, byte_count{3}, errc::resource_exhausted, 101},
      invalid_case{"\x02X"sv, byte_count{}, errc::resource_exhausted, 101},
      invalid_case{
        "\xfe\xff\xff\xff\x0f"sv,
        byte_count{1},
        errc::resource_exhausted,
        101}};
    for (const auto& value : cases) {
        for (const auto boundary :
             {codec::input_boundary::open, codec::input_boundary::complete}) {
            const auto bytes = std::string{"p"} + std::string{value.encoded};
            fragmented_buffer_parser input{byte_fragments(bytes)};
            ASSERT_TRUE(input.skip(byte_count{1}).has_value());
            expect_error(
              codec::read_nullable_length(
                input, value.allowance, context, boundary),
              value.reason,
              value.offset);
            EXPECT_EQ(input.bytes_consumed(), byte_count{1});
            EXPECT_EQ(unread_bytes(input), value.encoded);
            EXPECT_EQ(input.checkpoint_depth(), 0U);
        }
    }
}

TEST(
  CodecNullableIntegerTest, PrefixAndPayloadShortagesUseTheOuterMissingByte) {
    for (const auto encoded :
         {""sv,
          "\x80"sv,
          "\x08"sv,
          "\x08"
          "AB"sv,
          "\xfe\xff\xff\xff\x0f"sv}) {
        for (const auto boundary :
             {codec::input_boundary::open, codec::input_boundary::complete}) {
            fragmented_buffer_parser input{
              byte_fragments(std::string{"p"} + std::string{encoded})};
            ASSERT_TRUE(input.skip(byte_count{1}).has_value());
            expect_error(
              codec::read_nullable_length(
                input, byte_count{maximum}, context, boundary),
              boundary == codec::input_boundary::open ? errc::truncated_data
                                                      : errc::malformed_data,
              101 + encoded.size());
            EXPECT_EQ(input.bytes_consumed(), byte_count{1});
            EXPECT_EQ(unread_bytes(input), encoded);
        }
    }
}

TEST(CodecNullableIntegerTest, NullableAndSignedReadsDoNotUseCallerMarks) {
    const auto bytes = "\x01\x00\x04"
                       "AB\x03"sv;
    fragmented_buffer_parser input{byte_fragments(bytes)};
    for (std::size_t depth = 0; depth < 8; ++depth) {
        ASSERT_TRUE(input.push_checkpoint().has_value());
    }
    const auto signed_value = codec::read_varint<std::int64_t>(input, context);
    ASSERT_TRUE(signed_value.has_value());
    EXPECT_EQ(*signed_value, -1);
    const auto empty = codec::read_nullable_length(
      input, byte_count{}, context);
    ASSERT_TRUE(empty.has_value());
    EXPECT_EQ(*empty, std::optional<byte_count>{byte_count{}});
    const auto present = codec::read_nullable_length(
      input, byte_count{2}, context);
    ASSERT_TRUE(present.has_value());
    EXPECT_EQ(*present, std::optional<byte_count>{byte_count{2}});
    EXPECT_EQ(input.bytes_consumed(), byte_count{3});
    ASSERT_TRUE(input.skip(byte_count{2}).has_value());
    expect_error(
      codec::read_nullable_length(input, byte_count{2}, context),
      errc::malformed_data,
      105);
    EXPECT_EQ(input.bytes_consumed(), byte_count{5});
    EXPECT_EQ(input.checkpoint_depth(), 8U);
    for (std::size_t depth = 8; depth != 0; --depth) {
        ASSERT_TRUE(input.rollback().has_value());
        EXPECT_EQ(input.checkpoint_depth(), depth - 1U);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
    }
    EXPECT_EQ(unread_bytes(input), bytes);
}

TEST(CodecNullableIntegerTest, InvalidCoordinatesRejectNullBeforeItsCommit) {
    fragmented_buffer_parser input{byte_fragments("p\x01"sv)};
    ASSERT_TRUE(input.skip(byte_count{1}).has_value());
    const codec::field_context overflow{
      .origin = maximum - 1U, .family = 17, .field = 31};
    expect_error(
      codec::read_nullable_length(input, byte_count{}, overflow),
      errc::invalid_argument,
      maximum - 1U,
      overflow);
    expect_error(
      codec::read_nullable_length(
        input, byte_count{}, context, static_cast<codec::input_boundary>(2)),
      errc::invalid_argument,
      context.origin);
    EXPECT_EQ(input.bytes_consumed(), byte_count{1});
    EXPECT_EQ(unread_bytes(input), "\x01"sv);
}

TEST(
  CodecNullableIntegerTest, WriterChecksSignedDomainThenAllowanceAtomically) {
    fragmented_buffer_builder output;
    ASSERT_TRUE(output.append("p"sv).has_value());
    for (const auto invalid : {std::uint64_t{2147483648}, maximum}) {
        expect_error(
          codec::write_nullable_length(
            output, byte_count{invalid}, byte_count{}, context),
          errc::invalid_argument,
          101);
    }
    expect_error(
      codec::write_nullable_length(
        output, byte_count{4}, byte_count{3}, context),
      errc::resource_exhausted,
      101);
    EXPECT_EQ(output.size(), byte_count{1});
    ASSERT_TRUE(
      codec::write_nullable_length(
        output, byte_count{2147483647}, byte_count{2147483647}, context)
        .has_value());
    const auto encoded = output.finish();
    ASSERT_TRUE(encoded.has_value());
    EXPECT_TRUE(encoded->content_equals("p\xfe\xff\xff\xff\x0f"sv));
}

TEST(CodecNullableIntegerTest, WriterPreservesCoordinateAndBuilderLimits) {
    fragmented_buffer_builder_config config;
    config.initial_fragment_bytes = byte_count{2};
    config.max_fragment_bytes = byte_count{2};
    config.max_total_bytes = byte_count{2};
    fragmented_buffer_builder output{config};
    ASSERT_TRUE(output.append("p"sv).has_value());
    const codec::field_context overflow{
      .origin = maximum, .family = 17, .field = 31};
    expect_error(
      codec::write_nullable_length(
        output, std::nullopt, byte_count{}, overflow),
      errc::invalid_argument,
      maximum,
      overflow);
    const codec::field_context short_extent{
      .origin = maximum - 2U, .family = 17, .field = 31};
    expect_error(
      codec::write_nullable_length(
        output, byte_count{64}, byte_count{}, short_extent),
      errc::invalid_argument,
      maximum - 1U,
      short_extent);
    expect_error(
      codec::write_nullable_length(
        output, byte_count{64}, byte_count{64}, context),
      errc::resource_exhausted,
      101);
    EXPECT_EQ(output.size(), byte_count{1});
    ASSERT_TRUE(
      codec::write_nullable_length(output, std::nullopt, byte_count{}, context)
        .has_value());
    const auto encoded = output.finish();
    ASSERT_TRUE(encoded.has_value());
    EXPECT_TRUE(encoded->content_equals("p\x01"sv));
    expect_error(
      codec::write_nullable_length(output, std::nullopt, byte_count{}, context),
      errc::closed,
      100);
}

TEST(
  CodecNullableIntegerTest, NullableReadsAvoidAllocationOnSuccessAndFailure) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation failure injection is not enabled";
#else
    struct read_case {
        std::string_view bytes;
        bool success;
    };
    constexpr std::array cases{
      read_case{"\x01"sv, true},
      read_case{"\x00"sv, true},
      read_case{
        "\x04"
        "AB"sv,
        true},
      read_case{"\x03"sv, false},
      read_case{"\x08"sv, false},
      read_case{"\x0a"sv, false}};
    for (const auto& test_case : cases) {
        fragmented_buffer_parser input{byte_fragments(test_case.bytes)};
        std::optional<codec::result<std::optional<byte_count>>> value;
        auto& injector = seastar::memory::local_failure_injector();
        const auto before = injector.alloc_count();
        injector.fail_after(0);
        try {
            value.emplace(
              codec::read_nullable_length(input, byte_count{4}, context));
        } catch (...) {
            injector.cancel();
            throw;
        }
        const bool injected = injector.failed();
        const auto after = injector.alloc_count();
        injector.cancel();
        EXPECT_FALSE(injected);
        EXPECT_EQ(after, before);
        ASSERT_TRUE(value.has_value());
        EXPECT_EQ(value->has_value(), test_case.success);
    }
#endif
}

} // namespace
