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
