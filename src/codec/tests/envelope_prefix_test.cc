#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/envelope.h"
#include "src/codec/format_registry.h"
#include "src/codec/limits.h"

#include <seastar/core/memory.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
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
using kwaque::byte_count;
using kwaque::errc;
using kwaque::item_count;
using kwaque::bytes::fragmented_buffer;
using kwaque::bytes::fragmented_buffer_parser;

constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
constexpr codec::field_context context{.origin = 100, .family = 9, .field = 73};
constexpr codec::envelope_extent_limits owner_limits{
  .max_body_bytes = byte_count{16'777'216},
  .max_encoded_bytes = byte_count{33'554'432}};

// Body "123456789" has finalized CRC32C e3069283. The complete header's
// finalized CRC32C, with the last four octets zeroed, is bb045503.
constexpr auto golden
  = "\x4b\x51\x42\x46\x01\x00\x01\x00\x01\x00\x20\x00\x09\x00\x00\x00"
    "\x00\x00\x00\x00\x00\x00\x00\x00\x83\x92\x06\xe3\x03\x55\x04\xbb"sv;

// Distinct raw values detect equal-width swaps. This is framing input only:
// its versions/features/checksums deliberately make no validity claim.
constexpr auto distinct
  = "\x4b\x51\x42\x46\x34\x12\x45\x23\x56\x34\x45\x03\x03\x02\x01\x00"
    "\xef\xcd\xab\x89\x67\x45\x23\x01\xef\xcd\xab\x89\x10\x32\x54\x76"sv;
static_assert(golden.size() == codec::envelope_prefix_bytes);
static_assert(distinct.size() == codec::envelope_prefix_bytes);

fragmented_buffer split_at(std::string_view bytes, std::size_t cut) {
    std::vector<seastar::temporary_buffer<char>> fragments;
    for (const auto part : {bytes.substr(0, cut), bytes.substr(cut)}) {
        seastar::temporary_buffer<char> fragment{part.size()};
        std::ranges::copy(part, fragment.get_write());
        fragments.push_back(std::move(fragment));
    }
    return fragmented_buffer::copy_from_fragments(fragments).value();
}

fragmented_buffer one_byte_fragments(std::string_view bytes) {
    std::vector<seastar::temporary_buffer<char>> fragments;
    for (const auto value : bytes) {
        seastar::temporary_buffer<char> fragment{1};
        fragment.get_write()[0] = value;
        fragments.push_back(std::move(fragment));
    }
    return fragmented_buffer::copy_from_fragments(fragments).value();
}

void put_u16(std::string& encoded, std::size_t offset, std::uint16_t value) {
    encoded[offset] = static_cast<char>(value & 0xffU);
    encoded[offset + 1] = static_cast<char>(value >> 8U);
}

void put_u32(std::string& encoded, std::size_t offset, std::uint32_t value) {
    for (std::size_t index = 0; index < 4; ++index) {
        encoded[offset + index] = static_cast<char>(value & 0xffU);
        value >>= 8U;
    }
}

void expect_error(
  const auto& value,
  errc code,
  codec::envelope_field field,
  std::uint64_t offset,
  codec::field_context diagnostic = context) {
    ASSERT_FALSE(value.has_value());
    EXPECT_EQ(value.error().code(), code);
    EXPECT_EQ(value.error().family(), diagnostic.family);
    EXPECT_EQ(value.error().field(), static_cast<std::uint16_t>(field));
    EXPECT_EQ(value.error().byte_offset(), offset);
}

codec::envelope_prefix_fields fields(byte_count body = byte_count{9}) {
    return codec::envelope_prefix_fields{
      .family = codec::format_family::submitted_batch,
      .body_bytes = body,
      .body_crc32c = 0xe3069283U,
      .header_crc32c = 0xbb045503U};
}

TEST(EnvelopePrefixTest, EncodesIndependentExactBytesForCurrentWriter) {
    const auto encoded = codec::encode_envelope_prefix(
      fields(), codec::limits::defaults(), owner_limits, context);
    ASSERT_TRUE(encoded.has_value());
    EXPECT_EQ(std::string_view(encoded->data(), encoded->size()), golden);
    for (std::uint16_t family = 1; family <= 11; ++family) {
        auto supplied = fields();
        supplied.family = static_cast<codec::format_family>(family);
        auto expected = std::string{golden};
        expected[4] = static_cast<char>(family);
        const auto result = codec::encode_envelope_prefix(
          supplied, codec::limits::defaults(), owner_limits);
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(std::string_view(result->data(), result->size()), expected);
    }
}

TEST(EnvelopePrefixTest, ReadsEveryFieldAcrossEverySplitWithoutConsuming) {
    const auto bytes = std::string{"pre"} + std::string{distinct} + "next";
    for (std::size_t cut = 0; cut <= bytes.size(); ++cut) {
        SCOPED_TRACE(cut);
        fragmented_buffer_parser input{split_at(bytes, cut)};
        ASSERT_TRUE(input.skip(byte_count{3}).has_value());
        const auto prefix = codec::peek_envelope_prefix(
          input, codec::limits::defaults(), owner_limits, context);
        ASSERT_TRUE(prefix.has_value());
        EXPECT_EQ(prefix->family, 0x1234U);
        EXPECT_EQ(prefix->writer_version, 0x2345U);
        EXPECT_EQ(prefix->minimum_reader_version, 0x3456U);
        EXPECT_EQ(prefix->header_bytes, 0x0345U);
        EXPECT_EQ(prefix->body_bytes, 0x00010203U);
        EXPECT_EQ(prefix->required_features, UINT64_C(0x0123456789abcdef));
        EXPECT_EQ(prefix->body_crc32c, 0x89abcdefU);
        EXPECT_EQ(prefix->header_crc32c, 0x76543210U);
        EXPECT_EQ(prefix->encoded_bytes(), byte_count{66'888});
        EXPECT_EQ(input.bytes_consumed(), byte_count{3});
        EXPECT_EQ(input.checkpoint_depth(), 0U);
        std::array<char, 36> unread{};
        ASSERT_TRUE(input.peek_to(unread).has_value());
        EXPECT_EQ(
          std::string_view(unread.data(), unread.size()),
          std::string{distinct} + "next");
    }
}

TEST(EnvelopePrefixTest, EveryIncompleteCutHasAbsoluteFirstMissingByte) {
    for (std::size_t cut = 0; cut < codec::envelope_prefix_bytes; ++cut) {
        SCOPED_TRACE(cut);
        const auto bytes = std::string{"pre"}
                           + std::string{golden.substr(0, cut)};
        fragmented_buffer_parser input{one_byte_fragments(bytes)};
        ASSERT_TRUE(input.skip(byte_count{3}).has_value());
        for (const auto boundary :
             {codec::input_boundary::open, codec::input_boundary::complete}) {
            const auto prefix = codec::peek_envelope_prefix(
              input,
              codec::limits::defaults(),
              owner_limits,
              context,
              boundary);
            expect_error(
              prefix,
              boundary == codec::input_boundary::open ? errc::truncated_data
                                                      : errc::malformed_data,
              codec::envelope_field::magic,
              103 + cut);
            EXPECT_EQ(input.bytes_consumed(), byte_count{3});
            EXPECT_EQ(input.checkpoint_depth(), 0U);
        }
    }
    fragmented_buffer_parser short_bad_magic{split_at("bad!"sv, 2)};
    expect_error(
      codec::peek_envelope_prefix(
        short_bad_magic, codec::limits::defaults(), owner_limits, context),
      errc::truncated_data,
      codec::envelope_field::magic,
      104);
}

TEST(EnvelopePrefixTest, CallerCheckpointStackSurvivesSuccessAndFailure) {
    for (const bool malformed : {false, true}) {
        auto bytes = std::string{"p"} + std::string{golden};
        if (malformed) {
            bytes[1] = '?';
        }
        fragmented_buffer_parser input{one_byte_fragments(bytes)};
        for (std::size_t depth = 0;
             depth < kwaque::bytes::max_parser_checkpoints;
             ++depth) {
            ASSERT_TRUE(input.push_checkpoint().has_value());
        }
        ASSERT_TRUE(input.skip(byte_count{1}).has_value());
        const auto prefix = codec::peek_envelope_prefix(
          input, codec::limits::defaults(), owner_limits, context);
        EXPECT_EQ(prefix.has_value(), !malformed);
        EXPECT_EQ(input.bytes_consumed(), byte_count{1});
        EXPECT_EQ(
          input.checkpoint_depth(), kwaque::bytes::max_parser_checkpoints);
        ASSERT_TRUE(input.rollback().has_value());
        EXPECT_EQ(input.bytes_consumed(), byte_count{0});
        while (input.checkpoint_depth() != 0) {
            ASSERT_TRUE(input.rollback().has_value());
        }
    }
}

TEST(EnvelopePrefixTest, FramingChecksPrecedeUnavailableVariableExtents) {
    auto encoded = std::string{golden};
    struct header_case {
        std::uint16_t bytes;
        errc error;
    };
    for (const auto test : std::array{
           header_case{0, errc::malformed_data},
           header_case{31, errc::malformed_data},
           header_case{4097, errc::resource_exhausted},
           header_case{UINT16_MAX, errc::resource_exhausted}}) {
        put_u16(encoded, 10, test.bytes);
        fragmented_buffer_parser input{split_at(encoded, 11)};
        expect_error(
          codec::peek_envelope_prefix(
            input, codec::limits::defaults(), owner_limits, context),
          test.error,
          codec::envelope_field::header_bytes,
          110);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
    }
    for (const std::uint16_t header :
         {std::uint16_t{32}, std::uint16_t{4096}}) {
        put_u16(encoded, 10, header);
        fragmented_buffer_parser input{split_at(encoded, 11)};
        EXPECT_TRUE(
          codec::peek_envelope_prefix(
            input, codec::limits::defaults(), owner_limits)
            .has_value());
    }
    put_u16(encoded, 10, 31);
    put_u32(encoded, 12, UINT32_MAX);
    encoded[0] = 0;
    fragmented_buffer_parser input{split_at(encoded, 12)};
    expect_error(
      codec::peek_envelope_prefix(
        input, codec::limits::defaults(), owner_limits, context),
      errc::malformed_data,
      codec::envelope_field::magic,
      100);
}

TEST(EnvelopePrefixTest, BodyAndTotalCapsAreIndependentAndIntersectPolicy) {
    auto encoded = std::string{golden};
    for (const std::uint32_t body :
         {0U, 1U, 16'777'216U, 16'777'217U, UINT32_MAX}) {
        put_u32(encoded, 12, body);
        fragmented_buffer_parser input{split_at(encoded, 13)};
        const auto prefix = codec::peek_envelope_prefix(
          input,
          codec::limits::defaults(),
          {.max_body_bytes = byte_count{maximum},
           .max_encoded_bytes = byte_count{maximum}},
          context);
        if (body <= 16'777'216U) {
            ASSERT_TRUE(prefix.has_value());
            EXPECT_EQ(prefix->body_bytes, body);
        } else {
            expect_error(
              prefix,
              errc::resource_exhausted,
              codec::envelope_field::body_bytes,
              112);
        }
    }
    put_u32(encoded, 12, 9);
    for (const auto cap : std::array{
           codec::envelope_extent_limits{byte_count{9}, byte_count{41}},
           codec::envelope_extent_limits{byte_count{8}, byte_count{41}},
           codec::envelope_extent_limits{byte_count{9}, byte_count{40}},
           codec::envelope_extent_limits{byte_count{0}, byte_count{0}}}) {
        fragmented_buffer_parser input{split_at(encoded, 16)};
        const auto prefix = codec::peek_envelope_prefix(
          input, codec::limits::defaults(), cap, context);
        if (
          cap.max_body_bytes.value() == 9
          && cap.max_encoded_bytes.value() == 41) {
            EXPECT_TRUE(prefix.has_value());
        } else {
            expect_error(
              prefix,
              errc::resource_exhausted,
              cap.max_body_bytes.value() < 9
                ? codec::envelope_field::body_bytes
                : codec::envelope_field::encoded_bytes,
              112);
        }
    }
}

TEST(
  EnvelopePrefixTest, LeavesUntrustedFamilyVersionsFeaturesAndCrcsUnvalidated) {
    for (const std::uint16_t value :
         {std::uint16_t{0}, std::uint16_t{UINT16_MAX}}) {
        auto encoded = std::string{golden};
        for (const auto offset : {4U, 6U, 8U}) {
            put_u16(encoded, offset, value);
        }
        std::fill(encoded.begin() + 16, encoded.end(), static_cast<char>(0xff));
        fragmented_buffer_parser input{one_byte_fragments(encoded)};
        const auto prefix = codec::peek_envelope_prefix(
          input, codec::limits::defaults(), owner_limits);
        ASSERT_TRUE(prefix.has_value());
        EXPECT_EQ(prefix->family, value);
        EXPECT_EQ(prefix->writer_version, value);
        EXPECT_EQ(prefix->minimum_reader_version, value);
        EXPECT_EQ(prefix->required_features, maximum);
        EXPECT_EQ(prefix->body_crc32c, UINT32_MAX);
        EXPECT_EQ(prefix->header_crc32c, UINT32_MAX);
    }
}

TEST(EnvelopePrefixTest, CheckedCoordinatesSeparateCallerAndDeclaredOverflow) {
    fragmented_buffer_parser input{split_at(golden, 28)};
    auto diagnostic = context;
    diagnostic.origin = maximum - 41;
    EXPECT_TRUE(
      codec::peek_envelope_prefix(
        input, codec::limits::defaults(), owner_limits, diagnostic)
        .has_value());
    diagnostic.origin = maximum - 40;
    expect_error(
      codec::peek_envelope_prefix(
        input, codec::limits::defaults(), owner_limits, diagnostic),
      errc::malformed_data,
      codec::envelope_field::encoded_bytes,
      maximum - 28,
      diagnostic);
    diagnostic.origin = maximum - 31;
    const auto bad_origin = codec::peek_envelope_prefix(
      input, codec::limits::defaults(), owner_limits, diagnostic);
    ASSERT_FALSE(bad_origin.has_value());
    EXPECT_EQ(bad_origin.error().code(), errc::invalid_argument);
    EXPECT_EQ(bad_origin.error().byte_offset(), diagnostic.origin);
    const auto bad_boundary = codec::peek_envelope_prefix(
      input,
      codec::limits::defaults(),
      owner_limits,
      context,
      static_cast<codec::input_boundary>(2));
    ASSERT_FALSE(bad_boundary.has_value());
    EXPECT_EQ(bad_boundary.error().code(), errc::invalid_argument);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
}

TEST(EnvelopePrefixTest, WriterRejectsUnknownFamiliesAndNarrowingBeforeOutput) {
    for (const std::uint16_t family :
         {std::uint16_t{0}, std::uint16_t{12}, std::uint16_t{UINT16_MAX}}) {
        auto supplied = fields();
        supplied.family = static_cast<codec::format_family>(family);
        expect_error(
          codec::encode_envelope_prefix(
            supplied, codec::limits::defaults(), owner_limits, context),
          errc::invalid_argument,
          codec::envelope_field::family,
          104);
    }
    expect_error(
      codec::encode_envelope_prefix(
        fields(byte_count{UINT32_MAX}),
        codec::limits::defaults(),
        owner_limits,
        context),
      errc::resource_exhausted,
      codec::envelope_field::body_bytes,
      112);
    expect_error(
      codec::encode_envelope_prefix(
        fields(byte_count{std::uint64_t{UINT32_MAX} + 1}),
        codec::limits::defaults(),
        owner_limits,
        context),
      errc::invalid_argument,
      codec::envelope_field::body_bytes,
      112);
    auto diagnostic = context;
    diagnostic.origin = maximum - 40;
    expect_error(
      codec::encode_envelope_prefix(
        fields(), codec::limits::defaults(), owner_limits, diagnostic),
      errc::invalid_argument,
      codec::envelope_field::encoded_bytes,
      maximum - 28,
      diagnostic);
    const auto empty = codec::encode_envelope_prefix(
      fields(byte_count{}),
      codec::limits::defaults(),
      {.max_body_bytes = byte_count{}, .max_encoded_bytes = byte_count{32}});
    EXPECT_TRUE(empty.has_value());
}

TEST(EnvelopePrefixTest, FixedWorkQuantumIsExplicitAndCannotBeEnlarged) {
    for (const bool bytes_short : {false, true}) {
        auto config = codec::limits::defaults().config();
        config.max_work_bytes = byte_count{
          codec::envelope_prefix_work_bytes.value() - (bytes_short ? 1 : 0)};
        config.max_work_items = item_count{
          codec::envelope_prefix_work_items.value() - (bytes_short ? 0 : 1)};
        const auto policy = codec::limits::make(config);
        ASSERT_TRUE(policy.has_value());
        fragmented_buffer_parser input{split_at(golden, 7)};
        expect_error(
          codec::peek_envelope_prefix(input, *policy, owner_limits, context),
          errc::resource_exhausted,
          codec::envelope_field::magic,
          100);
        expect_error(
          codec::encode_envelope_prefix(
            fields(), *policy, owner_limits, context),
          errc::resource_exhausted,
          codec::envelope_field::magic,
          100);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
    }
    auto config = codec::limits::defaults().config();
    config.max_work_bytes = codec::envelope_prefix_work_bytes;
    config.max_work_items = codec::envelope_prefix_work_items;
    const auto policy = codec::limits::make(config);
    ASSERT_TRUE(policy.has_value());
    fragmented_buffer_parser input{one_byte_fragments(golden)};
    EXPECT_TRUE(
      codec::peek_envelope_prefix(input, *policy, owner_limits).has_value());
    EXPECT_TRUE(
      codec::encode_envelope_prefix(fields(), *policy, owner_limits)
        .has_value());
}

TEST(EnvelopePrefixTest, OwnerCannotWidenNarrowHeaderOrTotalPolicy) {
    for (const bool header_short : {false, true}) {
        auto config = codec::limits::defaults().config();
        config.max_header_bytes = byte_count{header_short ? 31U : 32U};
        config.max_retained_bytes = byte_count{header_short ? 41U : 40U};
        const auto policy = codec::limits::make(config);
        ASSERT_TRUE(policy.has_value());
        fragmented_buffer_parser input{split_at(golden, 12)};
        const auto field = header_short ? codec::envelope_field::header_bytes
                                        : codec::envelope_field::encoded_bytes;
        const auto offset = header_short ? 110U : 112U;
        expect_error(
          codec::peek_envelope_prefix(input, *policy, owner_limits, context),
          errc::resource_exhausted,
          field,
          offset);
        expect_error(
          codec::encode_envelope_prefix(
            fields(), *policy, owner_limits, context),
          errc::resource_exhausted,
          field,
          offset);
    }
}

TEST(EnvelopePrefixTest, FragmentedPeekAndInlineEncodingAllocateNothing) {
    fragmented_buffer_parser input{one_byte_fragments(golden)};
    std::optional<codec::result<codec::unverified_envelope_prefix>> decoded;
    std::optional<codec::result<codec::encoded_envelope_prefix>> encoded;
    auto& injector = seastar::memory::local_failure_injector();
    const auto before = injector.alloc_count();
    const auto native_before = seastar::memory::stats().mallocs();
    injector.fail_after(0);
    bool threw = false;
    try {
        decoded.emplace(
          codec::peek_envelope_prefix(
            input, codec::limits::defaults(), owner_limits));
        encoded.emplace(
          codec::encode_envelope_prefix(
            fields(), codec::limits::defaults(), owner_limits));
    } catch (const std::bad_alloc&) {
        threw = true;
    } catch (...) {
        injector.cancel();
        throw;
    }
    const auto after = injector.alloc_count();
    const auto native_after = seastar::memory::stats().mallocs();
    const bool injected = injector.failed();
    injector.cancel();
    EXPECT_FALSE(threw);
    EXPECT_FALSE(injected);
    EXPECT_EQ(after, before);
    EXPECT_EQ(native_after, native_before);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(decoded->has_value());
    ASSERT_TRUE(encoded.has_value());
    EXPECT_TRUE(encoded->has_value());
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
}

} // namespace
