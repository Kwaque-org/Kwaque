#include "src/bytes/test_allocation_profile.h"
#include "src/codec/tests/envelope_decode_test_support.h"
#include "src/codec/tests/format_fixture.h"
#include "src/compression/compression.h"

#include <seastar/core/abort_source.hh>

#include <gtest/gtest.h>

namespace {
using namespace kwaque;
TEST(CompressionFormatFixtureTest, IndependentRawBlockIncludesAllChecksums) {
    const auto wire = codec::testing::format_fixture::read("lz4_raw_abc");
    for (const std::size_t width : {1U, 7U, 67U}) {
        auto input = codec::testing::envelope_fixture::fragmented(wire, width);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto cost = input.allocation_cost(bytes::testing::charge).value();
        const auto memory
          = codec::detail::consume_decode_budget(
              work.policy(),
              {byte_count{32U << 20U},
               byte_count{1U << 20U},
               bytes::testing::charge},
              cost.backing,
              cost.descriptors.checked_add(cost.share_controls).value(),
              {},
              0)
              .value();
        auto decoded = compression::decompress_lz4(
                         std::move(input), byte_count{3}, work, memory)
                         .get();
        ASSERT_TRUE(decoded.has_value());
        EXPECT_TRUE(decoded->value.content_equals("abc"));
    }
}
} // namespace
