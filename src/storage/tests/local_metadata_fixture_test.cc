#include "src/storage/local_metadata.h"
#include "src/storage/tests/local_metadata_fixture.h"
#include "src/storage/tests/local_metadata_fixture_cases.h"
#include "src/storage/tests/local_metadata_fixture_expectation.h"
#include "src/storage/tests/retry_test_support.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <string>

namespace kwaque::storage {
namespace {
using namespace testing;
using local_fixture::expectation;
using local_fixture::literal_digest;

TEST(LocalMetadataFixtureTest, IndependentFieldsPinsAndMalformedVerdicts) {
    for (const auto& test : local_fixture_cases) {
        SCOPED_TRACE(test.name);
        const auto wire = local_fixture::read(test.name);
        ASSERT_EQ(wire.size(), test.bytes);
        EXPECT_EQ(exact_digest(wire), literal_digest(test.digest).bytes());
        for (const std::size_t width : {67U, 4096U}) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            {
                bytes::fragmented_buffer_parser discovery{buffer(wire, width)};
                auto probed = probe_local_metadata(
                                discovery,
                                alignment(4096),
                                reserve(discovery, work),
                                work,
                                {},
                                codec::input_boundary::complete)
                                .get();
                // A device claim can be structurally valid without matching
                // the independently configured owner used by contextual decode.
                const auto expected_probe = test.name == "foreign_device"
                                              ? errc::success
                                              : test.expected;
                if (expected_probe == errc::success) {
                    ASSERT_TRUE(probed);
                    EXPECT_TRUE(discovery.at_end());
                } else {
                    ASSERT_FALSE(probed);
                    EXPECT_EQ(probed.error().code(), expected_probe);
                    EXPECT_EQ(discovery.bytes_consumed().value(), 0U);
                }
                EXPECT_EQ(discovery.checkpoint_depth(), 0U);
            }
            bytes::fragmented_buffer_parser input{
              buffer("prefix" + wire, width)};
            ASSERT_TRUE(input.skip(byte_count{6}));
            ASSERT_TRUE(input.push_checkpoint());
            auto decoded = decode_local_metadata(
                             input,
                             expectation(test),
                             reserve(input, work),
                             work,
                             {},
                             codec::input_boundary::complete)
                             .get();
            if (test.expected != errc::success) {
                ASSERT_FALSE(decoded);
                EXPECT_EQ(decoded.error().code(), test.expected);
                EXPECT_EQ(input.bytes_consumed().value(), 6U);
                EXPECT_EQ(input.checkpoint_depth(), 1U);
                ASSERT_TRUE(input.rollback());
                continue;
            }
            ASSERT_TRUE(decoded);
            EXPECT_TRUE(input.at_end());
            EXPECT_EQ(input.checkpoint_depth(), 1U);
            const auto e = expectation(test);
            auto encoded = encode_local_metadata(
                             {e.header,
                              e.alignment,
                              e.segment_alignment,
                              e.data_metadata_alignment},
                             decoded->value.payload(),
                             work,
                             decoded->remaining.operation_remaining,
                             charge)
                             .get();
            if (test.reserved_disposition) {
                ASSERT_FALSE(encoded);
                EXPECT_EQ(encoded.error().code(), errc::unsupported_format);
            } else {
                ASSERT_TRUE(encoded);
                EXPECT_EQ(
                  flat(encoded->bytes), local_fixture::read(test.canonical));
            }
            ASSERT_TRUE(input.commit());
        }
    }
}
TEST(
  LocalMetadataFixtureTest,
  FollowingReferencesChecksActualFamilyAndPageDigest) {
    for (const auto name : {"wrong_footer_family", "root_page_digest"}) {
        const auto found = std::find_if(
          std::begin(local_fixture_cases),
          std::end(local_fixture_cases),
          [name](const auto& test) { return test.name == name; });
        ASSERT_NE(found, std::end(local_fixture_cases));
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        bytes::fragmented_buffer_parser input{
          buffer(local_fixture::read(name), 67)};
        auto decoded = decode_local_metadata(
                         input, expectation(*found), reserve(input, work), work)
                         .get();
        ASSERT_TRUE(decoded);
        if (found->kind == 10) {
            const auto& proof = std::get<local_boundary_evidence>(
              decoded->value.payload());
            const auto footer = local_fixture::read("footer_a");
            EXPECT_NE(proof.footer.family(), get(footer, 4, 2));
            EXPECT_EQ(proof.footer.digest().bytes(), exact_digest(footer));
        } else {
            const auto& root = std::get<local_completed_retry_root>(
              decoded->value.payload());
            ASSERT_EQ(root.pages.size(), 1U);
            const auto page_case = std::find_if(
              std::begin(local_fixture_cases),
              std::end(local_fixture_cases),
              [](const auto& test) { return test.name == "snapshot_page"; });
            ASSERT_NE(page_case, std::end(local_fixture_cases));
            auto expected = expectation(*page_case);
            expected.digest = root.pages[0].digest();
            expected.page = root.pages[0];
            bytes::fragmented_buffer_parser page{
              buffer(local_fixture::read("snapshot_page"), 67)};
            auto rejected = decode_local_metadata(
                              page, expected, reserve(page, work), work)
                              .get();
            ASSERT_FALSE(rejected);
            EXPECT_EQ(rejected.error().code(), errc::corrupt_data);
            EXPECT_EQ(page.bytes_consumed().value(), 0U);
        }
    }
}
} // namespace
} // namespace kwaque::storage
