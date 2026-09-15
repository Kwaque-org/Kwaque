#include "src/compression/lz4.h"
#include "src/compression/tests/lz4_test_support.h"
#include "src/compression/tests/test_support.h"

#include <seastar/core/abort_source.hh>

#include <array>
#include <string_view>

namespace kwaque::compression {
namespace {
using namespace testing;

struct header_case final {
    std::string_view hex;
    std::uint64_t expanded;
    errc expected;
};

TEST(Lz4HeaderTest, WireProfilesAndChecksumValidationBeforePayloadScratch) {
    constexpr std::array cases{
      header_case{"04224d187c40030000000000000074", 3, errc::success},
      header_case{"04224d187440bd", 0, errc::success},
      header_case{"04224d187c400000000000000000c8", 0, errc::success},
      header_case{
        "04224d187d400300000000000000000000001d", 3, errc::unsupported_format},
      header_case{
        "04224d187d400300000000000000010000007b", 3, errc::unsupported_format},
      header_case{
        "04224d185c400300000000000000a2", 3, errc::unsupported_format},
      header_case{
        "04224d187c70030000000000000099", 3, errc::unsupported_format},
      header_case{
        "04224d186c40030000000000000029", 3, errc::unsupported_format},
      header_case{
        "04224d1878400300000000000000f0", 3, errc::unsupported_format},
      header_case{"04224d187440bd", 3, errc::unsupported_format},
      header_case{"04224d187c4004000000000000001f", 3, errc::malformed_data},
      header_case{"04224d187e40030000000000000084", 3, errc::malformed_data},
      header_case{"04224d187c410300000000000000f6", 3, errc::malformed_data},
      header_case{"04224d18bc400300000000000000cc", 3, errc::malformed_data},
      header_case{"04224d187c40030000000000000075", 3, errc::corrupt_data},
      header_case{"502a4d1800000000", 0, errc::unsupported_format},
    };
    for (const auto& value : cases) {
        SCOPED_TRACE(value.hex);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto plan = detail::admit_lz4(
                            detail::lz4_direction::decompress,
                            byte_count{value.expanded},
                            byte_count{1024},
                            work,
                            budget())
                            .value();
        detail::lz4_context owner{plan};
        ASSERT_TRUE(owner.memory().status());
        const auto initial = owner.memory().live_bytes();
        const auto header = from_hex(value.hex);
        const auto result = owner.read_header(
          std::span{header.data(), header.size()},
          byte_count{value.expanded},
          context);
        if (value.expected == errc::success)
            EXPECT_TRUE(result.has_value());
        else
            expect_error(result, value.expected);
        EXPECT_EQ(owner.memory().live_bytes(), initial);
        EXPECT_EQ(
          std::ranges::count_if(
            owner.memory().allocations(),
            [](const auto& allocation) {
                return allocation.address != nullptr;
            }),
          1);
        // A second getFrameInfo must never enter native decompression/init.
        expect_error(
          owner.read_header(
            std::span{header.data(), header.size()},
            byte_count{value.expanded},
            context),
          errc::closed);
        EXPECT_EQ(owner.memory().live_bytes(), initial);
    }
}

TEST(Lz4HeaderTest, EveryPartialHeaderRejectsOnAFreshContext) {
    for (const auto hex :
         {std::string_view{"04224d187440bd"},
          std::string_view{"04224d187c40030000000000000074"},
          std::string_view{"04224d187d400300000000000000000000001d"}}) {
        const auto header = from_hex(hex);
        for (std::size_t length = 0; length < header.size(); ++length) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            const auto plan = detail::admit_lz4(
                                detail::lz4_direction::decompress,
                                byte_count{3},
                                byte_count{1024},
                                work,
                                budget())
                                .value();
            detail::lz4_context owner{plan};
            ASSERT_TRUE(owner.memory().status());
            const auto initial = owner.memory().live_bytes();
            expect_error(
              owner.read_header(
                std::span{header.data(), length}, byte_count{3}, context),
              errc::malformed_data);
            EXPECT_EQ(owner.memory().live_bytes(), initial);
        }
    }
}

TEST(Lz4HeaderTest, FragmentedHeadersUseTheSameStrictProfile) {
    const auto raw = from_hex(raw_abc_hex);
    for (std::size_t width = 1; width <= 19; ++width) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto input = layout(raw, width);
        const auto decoded
          = decompress_lz4(
              std::move(input), byte_count{3}, work, budget(), context)
              .get();
        ASSERT_TRUE(decoded.has_value());
        EXPECT_TRUE(decoded->value.content_equals("abc"));
        const auto dict = from_hex("04224d187d400300000000000000000000001d");
        auto unsupported = layout(dict, width);
        expect_error(
          decompress_lz4(
            std::move(unsupported), byte_count{3}, work, budget(), context)
            .get(),
          errc::unsupported_format);
    }
}

} // namespace
} // namespace kwaque::compression
