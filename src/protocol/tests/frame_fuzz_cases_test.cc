#include "src/model/tests/record_fuzz_cases.h"
#include "src/model/tests/record_fuzz_oracle.h"
#include "src/protocol/tests/batch_frame_test_support.h"
#include "src/protocol/tests/frame_fuzz_cases.h"
#include "src/protocol/tests/frame_fuzz_oracle.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <vector>

namespace {
using namespace kwaque;
namespace test = protocol::testing;
namespace fixture = test::frame_fixture;

TEST(FrameFuzzCasesTest, IndependentOracleChecksBoundariesAndFailurePriority) {
    const auto wire = fixture::wire("abc", fixture::extension());
    for (std::size_t cut = 0; cut <= wire.size(); ++cut) {
        const auto open = test::probe_frame(
          std::string_view{wire}.substr(0, cut));
        const auto complete = test::probe_frame(
          std::string_view{wire}.substr(0, cut), 0, test::complete_input);
        if (cut < wire.size()) {
            EXPECT_EQ(open.error, errc::success);
            EXPECT_EQ(
              open.needed,
              (cut < 48   ? 48
               : cut < 57 ? 57
                          : wire.size())
                - cut);
            EXPECT_EQ(complete.error, errc::malformed_data);
        } else {
            EXPECT_EQ(open.used, wire.size());
            EXPECT_EQ(complete.used, wire.size());
        }
    }
    auto corrupt = wire;
    corrupt[4] ^= 1;
    EXPECT_EQ(test::probe_frame(corrupt).error, errc::corrupt_data);
    EXPECT_EQ(
      test::probe_frame(corrupt, 0, test::deny_metadata).error,
      errc::resource_exhausted);
    EXPECT_EQ(
      test::probe_frame(corrupt, 16, test::nil_topic | test::deny_metadata)
        .error,
      errc::invalid_argument);
    EXPECT_EQ(
      test::probe_frame(corrupt, 16, test::nil_topic | test::initial_abort, 8)
        .error,
      errc::aborted);
    EXPECT_EQ(
      test::probe_frame("", 0, test::deny_work).error,
      errc::resource_exhausted);
}

TEST(FrameFuzzCasesTest, StructuredMutationsReachGenericAndBothBatchKinds) {
    for (unsigned shape = 1; shape <= 4; ++shape) {
        for (unsigned mutation = 0; mutation < 32; ++mutation) {
            for (unsigned header = 0; header < 3; ++header) {
                for (unsigned encoding = 0; encoding < 2; ++encoding) {
                    std::array<std::uint8_t, 9> input{};
                    input[0] = static_cast<std::uint8_t>(shape);
                    input[1] = static_cast<std::uint8_t>(mutation);
                    input[2] = static_cast<std::uint8_t>(encoding);
                    input[3] = static_cast<std::uint8_t>(header);
                    input[4] = 1;
                    input[5] = test::complete_input;
                    input[7] = 31;
                    input[8] = 0x80;
                    test::exercise_frame_case(input);
                }
            }
        }
    }
}

TEST(FrameFuzzCasesTest, MixedDenialsDoNotAcceptSuccessOrHideOtherFailures) {
    for (unsigned shape : {1U, 2U, 4U}) {
        for (unsigned flags = 0; flags < 128; ++flags) {
            for (const unsigned depth : {0U, 8U}) {
                std::array<std::uint8_t, 8> input{};
                input[0] = static_cast<std::uint8_t>(shape);
                input[2] = 1;
                input[4] = 1;
                input[5] = static_cast<std::uint8_t>(flags);
                input[6] = static_cast<std::uint8_t>(depth);
                test::exercise_frame_case(input);
            }
        }
    }
}

TEST(FrameFuzzCasesTest, DamagedCompressedBlockRejectsAcrossInputLayouts) {
    // The block declares 21 raw bytes inside a 20-byte content-size frame.
    // Fragmented input may expose the output overflow before its bad checksum.
    std::array<std::uint8_t, 8> input{
      0xf9, 0x1e, 0x01, 0x0b, 0x01, 0x1d, 0x01, 0x2d};
    test::exercise_frame_case(input);
    for (std::uint8_t layout = 0; layout < 4; ++layout) {
        for (std::uint8_t header = 0; header < 3; ++header) {
            for (const std::uint8_t flags :
                 {std::uint8_t{1}, std::uint8_t{0x1d}}) {
                input[3] = header;
                input[4] = layout;
                input[5] = flags;
                test::exercise_frame_case(input);
            }
        }
    }
}

TEST(FrameFuzzCasesTest, CompressionBodyErrorDoesNotRelaxEarlierValidation) {
    auto batch = test::batch_frame_fixture::batch_wire(true, true, true);
    const auto header = static_cast<std::size_t>(
      model::testing::little(batch, 10, 2));
    const auto block = header + 184 + 15;
    ASSERT_EQ(model::testing::little(batch, block, 4), 0x80000014U);
    model::testing::put(batch, block, 0x80000015U, 4);
    model::testing::repair_crc(batch);
    auto wire = test::batch_frame_fixture::frame(batch, true);
    const auto damage = test::probe_frame(wire, 17, test::complete_input);
    ASSERT_EQ(damage.error, errc::corrupt_data);
    ASSERT_TRUE(damage.batch.compression_body_error);
    EXPECT_TRUE(damage.batch.matches_error(errc::corrupt_data));
    EXPECT_TRUE(damage.batch.matches_error(errc::malformed_data));
    for (const auto code :
         {errc::success,
          errc::resource_exhausted,
          errc::unsupported_format,
          errc::wrong_context,
          errc::aborted,
          errc::invalid_argument,
          errc::truncated_data}) {
        EXPECT_FALSE(damage.batch.matches_error(code));
    }
    // The nested batch oracle is shared with the raw model fuzzer.
    const auto extended = model::testing::frame(
      batch.substr(header), true, true);
    for (const auto& nested : {batch, extended}) {
        for (std::uint8_t layout = 0; layout < 3; ++layout) {
            std::vector<std::uint8_t> raw{3, 0, 0, layout, 8, 0, 0, 0};
            for (const char byte : nested)
                raw.push_back(static_cast<std::uint8_t>(byte));
            model::testing::exercise_record_case(raw);
        }
    }

    const auto wrong = test::probe_frame(
      wire, 17, test::complete_input | test::wrong_topic);
    EXPECT_EQ(wrong.error, errc::wrong_context);
    EXPECT_FALSE(wrong.batch.compression_body_error);
    const auto denied = test::probe_frame(wire, 17, test::deny_metadata);
    EXPECT_EQ(denied.error, errc::resource_exhausted);
    EXPECT_FALSE(denied.batch.compression_body_error);

    // Inner-envelope integrity still precedes compression.
    auto bad_envelope = batch;
    bad_envelope.back() ^= 1;
    const auto inner = test::probe_frame(
      test::batch_frame_fixture::frame(bad_envelope, true), 17);
    EXPECT_EQ(inner.error, errc::corrupt_data);
    EXPECT_FALSE(inner.batch.compression_body_error);
    EXPECT_FALSE(inner.batch.matches_error(errc::malformed_data));

    // Repair the enclosing checksums after corrupting the LZ4 header checksum.
    auto bad_header = batch;
    bad_header[header + 184 + 14] ^= 1;
    model::testing::repair_crc(bad_header);
    const auto compressed_header = test::probe_frame(
      test::batch_frame_fixture::frame(bad_header, true), 17);
    EXPECT_EQ(compressed_header.error, errc::corrupt_data);
    EXPECT_FALSE(compressed_header.batch.compression_body_error);
    EXPECT_FALSE(compressed_header.batch.matches_error(errc::malformed_data));

    wire[40] ^= 1;
    const auto outer = test::probe_frame(wire, 17);
    EXPECT_EQ(outer.error, errc::corrupt_data);
    EXPECT_FALSE(outer.batch.compression_body_error);
}

TEST(FrameFuzzCasesTest, RawInputsUseGenericAndTypedParsingWithNestedRepair) {
    for (const bool assigned : {false, true}) {
        const auto wire = test::batch_frame_fixture::frame(
          test::batch_frame_fixture::batch_wire(assigned, true, assigned),
          assigned);
        for (unsigned repairs = 0; repairs < 4; ++repairs) {
            for (std::size_t cut :
                 {std::size_t{0},
                  std::size_t{47},
                  wire.size() - 1,
                  wire.size()}) {
                std::vector<std::uint8_t> input(8);
                input[0] = assigned ? 10 : 5;
                input[1] = static_cast<std::uint8_t>(repairs);
                input[4] = 1;
                for (char c : std::string_view{wire}.substr(0, cut))
                    input.push_back(static_cast<std::uint8_t>(c));
                test::exercise_frame_case(input);
            }
        }
    }
    std::array<std::uint8_t, 8> empty{};
    test::exercise_frame_case(empty);
}

} // namespace
