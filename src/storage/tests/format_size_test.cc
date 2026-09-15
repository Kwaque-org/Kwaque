#include "src/storage/format_size.h"

#include <gtest/gtest.h>

#include <array>
#include <concepts>
#include <cstdint>
#include <limits>
#include <span>
#include <type_traits>

namespace {
namespace codec = kwaque::codec;
namespace storage = kwaque::storage;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::runtime::file_position;
using storage::aligned_envelope_layout;
using storage::storage_alignment;
constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
constexpr std::uint64_t body_cap = 16U << 20U;
constexpr codec::envelope_extent_limits ordinary{
  byte_count{body_cap}, byte_count{body_cap + 4096}};
constexpr codec::envelope_extent_limits page{
  byte_count{65536}, byte_count{65536}};
static_assert(!std::default_initializable<aligned_envelope_layout>);
static_assert(!std::is_aggregate_v<aligned_envelope_layout>);
static_assert(std::is_trivially_copyable_v<aligned_envelope_layout>);
static_assert(std::is_nothrow_move_constructible_v<aligned_envelope_layout>);

storage_alignment alignment(std::uint64_t bytes) {
    return storage_alignment::make(byte_count{bytes}).value();
}
auto layout(
  std::uint64_t header,
  std::uint64_t fixed,
  std::uint64_t tail,
  std::uint64_t align = 512,
  codec::envelope_extent_limits owner = ordinary,
  codec::limits policy = codec::limits::defaults()) {
    return aligned_envelope_layout::make(
      {byte_count{header}, byte_count{fixed}, byte_count{tail}},
      alignment(align),
      policy,
      owner);
}
void expect_error(const auto& result, errc error) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), error);
}

TEST(StorageSizeTest, IndependentSmallObjectGeometryIncludesEveryHeader) {
    struct example {
        std::uint64_t header, fixed, tail, padding, encoded;
    };
    constexpr std::array cases{
      example{32, 96, 0, 384, 512},
      example{32, 120, 223, 137, 512},
      example{32, 136, 223, 121, 512},
      example{32, 192, 0, 288, 512},
      example{32, 232, 48, 200, 512},
      example{32, 100, 160, 220, 512},
      example{40, 120, 223, 129, 512},
      example{4096, 120, 223, 169, 4608}};
    for (const auto& input : cases) {
        const auto result = layout(input.header, input.fixed, input.tail);
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result->header_bytes(), byte_count{input.header});
        EXPECT_EQ(result->padding_bytes(), byte_count{input.padding});
        EXPECT_EQ(result->encoded_bytes(), byte_count{input.encoded});
        EXPECT_EQ(
          result->body_bytes(), byte_count{input.encoded - input.header});
        EXPECT_EQ(result->alignment(), alignment(512));
        EXPECT_EQ(
          result->at(file_position{512}).value().end(),
          file_position{512 + input.encoded});
    }
}

TEST(StorageSizeTest, MinimumPaddingMatchesIndependentCeilingArithmetic) {
    for (unsigned shift = 9; shift <= 16; ++shift) {
        const auto multiple = std::uint64_t{1} << shift;
        const auto align = alignment(multiple);
        for (std::uint64_t bytes = 0; bytes <= 2 * multiple; ++bytes) {
            const auto expected = ((bytes + multiple - 1U) / multiple)
                                    * multiple
                                  - bytes;
            EXPECT_EQ(
              storage::minimum_padding(byte_count{bytes}, align),
              byte_count{expected});
        }
        EXPECT_EQ(
          storage::minimum_padding(byte_count{maximum}, align), byte_count{1});
        EXPECT_EQ(
          storage::minimum_padding(byte_count{maximum - multiple + 1U}, align),
          byte_count{});
    }
}

TEST(StorageSizeTest, ExactAndOneOverUnpaddedExtentsMoveToTheNextUnit) {
    for (unsigned shift = 9; shift <= 16; ++shift) {
        const auto multiple = std::uint64_t{1} << shift;
        const auto exact = layout(32, 120, multiple - 152U, multiple).value();
        const auto short_by_one
          = layout(32, 120, multiple - 153U, multiple).value();
        const auto long_by_one
          = layout(32, 120, multiple - 151U, multiple).value();
        EXPECT_EQ(exact.padding_bytes(), byte_count{});
        EXPECT_EQ(short_by_one.padding_bytes(), byte_count{1});
        EXPECT_EQ(long_by_one.padding_bytes(), byte_count{multiple - 1U});
        EXPECT_EQ(exact.encoded_bytes(), byte_count{multiple});
        EXPECT_EQ(short_by_one.encoded_bytes(), byte_count{multiple});
        EXPECT_EQ(long_by_one.encoded_bytes(), byte_count{2 * multiple});
    }
}

TEST(
  StorageSizeTest, BodyAndWholeEnvelopeLimitsAreIndependentAndIncludePadding) {
    EXPECT_TRUE(layout(32, 120, 223, 512, {byte_count{480}, byte_count{512}}));
    expect_error(
      layout(32, 120, 223, 512, {byte_count{479}, byte_count{512}}),
      errc::resource_exhausted);
    expect_error(
      layout(32, 120, 223, 512, {byte_count{480}, byte_count{511}}),
      errc::resource_exhausted);
    expect_error(
      layout(32, 120, 223, 512, {byte_count{}, byte_count{512}}),
      errc::resource_exhausted);
    expect_error(
      layout(32, 120, 223, 512, {byte_count{480}, byte_count{}}),
      errc::resource_exhausted);
    EXPECT_TRUE(layout(512, 0, 0, 512, {byte_count{}, byte_count{512}}));
    const codec::envelope_extent_limits oversized{
      byte_count{maximum}, byte_count{maximum}};
    expect_error(
      layout(32, body_cap, 1, 512, oversized), errc::resource_exhausted);
    auto config = codec::limits_config{};
    config.max_encoded_body_bytes = byte_count{479};
    expect_error(
      layout(32, 120, 223, 512, ordinary, codec::limits::make(config).value()),
      errc::resource_exhausted);
}

TEST(StorageSizeTest, RetryPageCapacityCountsHeaderExtensionsAndAlignment) {
    for (unsigned shift = 9; shift <= 16; ++shift) {
        const auto multiple = std::uint64_t{1} << shift;
        EXPECT_EQ(
          layout(32, 100, 408U * 160U, multiple, page).value().encoded_bytes(),
          byte_count{65536});
        expect_error(
          layout(32, 100, 409U * 160U, multiple, page),
          errc::resource_exhausted);
        EXPECT_EQ(
          layout(4096, 100, 383U * 160U, multiple, page)
            .value()
            .encoded_bytes(),
          byte_count{65536});
        expect_error(
          layout(4096, 100, 384U * 160U, multiple, page),
          errc::resource_exhausted);
    }
    // The same contents fit a larger owner; the page's whole-object cap is
    // independently restrictive.
    EXPECT_TRUE(layout(4096, 100, 408U * 160U, 512, ordinary));
}

TEST(StorageSizeTest, LargestChildFitsAndOneAdditionalByteFails) {
    for (unsigned shift = 9; shift <= 16; ++shift) {
        const auto multiple = std::uint64_t{1} << shift;
        for (const std::uint64_t header : {32U, 4096U}) {
            for (const std::uint64_t fixed : {120U, 136U}) {
                for (const auto owner :
                     {ordinary,
                      page,
                      codec::envelope_extent_limits{
                        byte_count{8192}, byte_count{7000}}}) {
                    const auto max_child = storage::max_child_bytes(
                      byte_count{header},
                      byte_count{fixed},
                      alignment(multiple),
                      codec::limits::defaults(),
                      owner);
                    if (!max_child) {
                        EXPECT_EQ(max_child.error(), errc::resource_exhausted);
                        expect_error(
                          layout(header, fixed, 1, multiple, owner),
                          errc::resource_exhausted);
                        continue;
                    }
                    EXPECT_TRUE(layout(
                      header, fixed, max_child->value(), multiple, owner));
                    expect_error(
                      layout(
                        header,
                        fixed,
                        max_child->value() + 1U,
                        multiple,
                        owner),
                      errc::resource_exhausted);
                }
            }
        }
    }
    EXPECT_EQ(
      storage::max_child_bytes(
        byte_count{32},
        byte_count{120},
        alignment(512),
        codec::limits::defaults(),
        ordinary)
        .value(),
      byte_count{16777064});
    EXPECT_EQ(
      storage::max_child_bytes(
        byte_count{32},
        byte_count{136},
        alignment(65536),
        codec::limits::defaults(),
        ordinary)
        .value(),
      byte_count{16777048});
    expect_error(
      storage::max_child_bytes(
        byte_count{32},
        byte_count{480},
        alignment(512),
        codec::limits::defaults(),
        {byte_count{480}, byte_count{512}}),
      errc::resource_exhausted);
}

TEST(
  StorageSizeTest, MaximumSelectedWriterFitsButStandaloneCapIsNotFreeOverhead) {
    // Complete assigned envelope: maximum header + fixed context + selected
    // writer's largest record frame. The outer header has its own overhead.
    constexpr std::uint64_t child = 4096U + 184U + 8389659U;
    for (const std::uint64_t fixed : {120U, 136U}) {
        const auto result = layout(4096, fixed, child, 65536);
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result->encoded_bytes(), byte_count{8454144});
        expect_error(
          layout(32, fixed, body_cap + 32U), errc::resource_exhausted);
    }
}

TEST(StorageSizeTest, CheckedSumsAndPlacementRejectOverflowBeforeWrapping) {
    expect_error(layout(32, maximum, 1), errc::out_of_range);
    expect_error(layout(32, maximum - 31U, 0), errc::out_of_range);
    expect_error(layout(32, maximum - 32U, 0), errc::out_of_range);
    expect_error(
      storage::max_child_bytes(
        byte_count{32},
        byte_count{maximum},
        alignment(512),
        codec::limits::defaults(),
        ordinary),
      errc::out_of_range);
    const auto result = layout(32, 96, 0).value();
    const auto highest_aligned = maximum - 511U;
    const auto last_fit = result.at(file_position{highest_aligned - 512U});
    ASSERT_TRUE(last_fit.has_value());
    EXPECT_EQ(last_fit->begin(), file_position{highest_aligned - 512U});
    EXPECT_EQ(last_fit->end(), file_position{highest_aligned});
    expect_error(result.at(file_position{highest_aligned}), errc::out_of_range);
    expect_error(result.at(file_position{1}), errc::invalid_argument);
    EXPECT_EQ(result.at(file_position{}).value().end(), file_position{512});
}

TEST(StorageSizeTest, HeaderBoundsApplyToBothSizingOperations) {
    for (const std::uint64_t header : {0U, 31U}) {
        expect_error(layout(header, 120, 223), errc::invalid_argument);
        expect_error(
          storage::max_child_bytes(
            byte_count{header},
            byte_count{120},
            alignment(512),
            codec::limits::defaults(),
            ordinary),
          errc::invalid_argument);
    }
    for (const auto header :
         std::array<std::uint64_t, 3>{4097, 65536, maximum}) {
        expect_error(layout(header, 120, 223), errc::resource_exhausted);
        expect_error(
          storage::max_child_bytes(
            byte_count{header},
            byte_count{120},
            alignment(512),
            codec::limits::defaults(),
            ordinary),
          errc::resource_exhausted);
    }
    auto config = codec::limits_config{};
    config.max_header_bytes = byte_count{32};
    const auto policy = codec::limits::make(config).value();
    expect_error(
      layout(40, 120, 223, 512, ordinary, policy), errc::resource_exhausted);
    expect_error(
      storage::max_child_bytes(
        byte_count{40}, byte_count{120}, alignment(512), policy, ordinary),
      errc::resource_exhausted);
}

TEST(
  StorageSizeTest,
  PaddingChecksEveryByteAndRejectsOverBudgetPiecesBeforeReading) {
    std::array<char, 65536> bytes{};
    const auto policy = codec::limits::defaults();
    EXPECT_TRUE(storage::validate_zero_padding({}, policy));
    EXPECT_TRUE(storage::validate_zero_padding(bytes, policy));
    for (const std::size_t position : {0U, 32768U, 65535U}) {
        bytes[position] = '\x01';
        expect_error(
          storage::validate_zero_padding(bytes, policy), errc::malformed_data);
        bytes[position] = '\0';
    }
    auto config = codec::limits_config{};
    config.max_work_bytes = byte_count{32768};
    const auto narrowed = codec::limits::make(config).value();
    bytes.back() = '\x01';
    expect_error(
      storage::validate_zero_padding(bytes, narrowed),
      errc::resource_exhausted);
    EXPECT_TRUE(
      storage::validate_zero_padding(
        std::span<const char>{bytes}.first(32768), narrowed));
    expect_error(
      storage::validate_zero_padding(
        std::span<const char>{bytes}.subspan(32768), narrowed),
      errc::malformed_data);
}
} // namespace
