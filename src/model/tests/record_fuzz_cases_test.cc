#include "src/model/tests/record_fuzz_cases.h"
#include "src/model/tests/record_fuzz_oracle.h"

#include <seastar/core/thread.hh>

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <vector>

TEST(RecordFuzzCasesTest, IndependentOracleLiterals) {
    kwaque::model::testing::verify_record_oracle();
}
TEST(RecordFuzzCasesTest, BatchHeaderFamilyAndVersionPrecedence) {
    using namespace kwaque::model::testing;
    using kwaque::errc;
    const std::array<std::uint8_t, 5> zero_family{5, 3, 0, 0, 1};
    exercise_record_case(zero_family);

    struct header_case final {
        std::uint16_t family, writer, reader;
        errc error;
    };
    for (const bool assigned : {false, true}) {
        for (const auto test : std::array{
               header_case{0, 1, 1, errc::malformed_data},
               header_case{0, 2, 2, errc::malformed_data},
               header_case{0, 1, 0, errc::unsupported_format},
               header_case{0, 0, 0, errc::unsupported_format},
               header_case{0, 0, 1, errc::malformed_data},
               header_case{11, 1, 1, errc::wrong_context},
               header_case{12, 1, 1, errc::unsupported_format},
               header_case{65535, 1, 1, errc::unsupported_format},
               header_case{11, 1, 2, errc::malformed_data},
               header_case{12, 1, 2, errc::malformed_data}}) {
            SCOPED_TRACE(
              ::testing::Message() << assigned << ':' << test.family << ':'
                                   << test.writer << ':' << test.reader);
            auto wire = frame({}, assigned, false);
            put(wire, 4, test.family, 2);
            put(wire, 6, test.writer, 2);
            put(wire, 8, test.reader, 2);
            repair_crc(wire);
            EXPECT_EQ(
              probe_batch(wire, assigned, true, false).error, test.error);
            for (const std::uint8_t layout :
                 std::array<std::uint8_t, 3>{0, 1, 2}) {
                std::vector<std::uint8_t> input{
                  assigned ? std::uint8_t{3} : std::uint8_t{2},
                  0,
                  0,
                  layout,
                  8,
                  0,
                  0,
                  0};
                for (const char byte : wire)
                    input.push_back(static_cast<std::uint8_t>(byte));
                exercise_record_case(input);
            }
        }
    }
}
TEST(RecordFuzzCasesTest, GrammarMutationAndOwnershipControls) {
    for (std::uint8_t mode = 0; mode < 7; ++mode) {
        for (std::uint8_t mutation = 0; mutation < 32; ++mutation) {
            for (const std::uint8_t depth :
                 std::array<std::uint8_t, 3>{0, 7, 8}) {
                std::array<std::uint8_t, 24> input{
                  mode,
                  mutation,
                  4,
                  1,
                  1,
                  depth,
                  0x0a,
                  2,
                  0x08,
                  0xff,
                  0x02,
                  0x00,
                  0x01,
                  0x61,
                  0x01};
                kwaque::model::testing::exercise_record_case(input);
                seastar::thread::maybe_yield();
            }
        }
    }
}
TEST(RecordFuzzCasesTest, BudgetsBoundariesAbortAndMaximumEngineInput) {
    for (const std::uint8_t mode : std::array<std::uint8_t, 4>{1, 4, 5, 6}) {
        for (const std::uint8_t flags :
             std::array<std::uint8_t, 9>{0, 1, 3, 4, 8, 16, 32, 64, 128}) {
            for (const std::uint8_t work :
                 std::array<std::uint8_t, 8>{0, 1, 2, 3, 4, 5, 8, 9}) {
                std::array<std::uint8_t, 12> input{
                  mode, 0, 7, 2, flags, 0, 0x55, work, 0xaa, 2, 4, 3};
                kwaque::model::testing::exercise_record_case(input);
                seastar::thread::maybe_yield();
            }
        }
    }
    std::vector<std::uint8_t> maximum(
      kwaque::model::testing::record_fuzz_max_input, 0);
    kwaque::model::testing::exercise_record_case(maximum);
    maximum[0] = 5;
    maximum[2] = 7;
    maximum[3] = 1;
    maximum[4] = 1;
    kwaque::model::testing::exercise_record_case(maximum);
}

TEST(
  RecordFuzzCasesTest,
  CompressedGrammarUsesIndependentExpansionAndRawSemantics) {
    for (const std::uint8_t mode : std::array<std::uint8_t, 2>{4, 5}) {
        for (std::uint8_t mutation = 0; mutation < 32; ++mutation) {
            for (const std::uint8_t work :
                 std::array<std::uint8_t, 4>{16, 17, 18, 20}) {
                std::array<std::uint8_t, 12> input{
                  mode, mutation, 4, 1, 1, 0, 0x55, work, 1, 2, 3, 4};
                kwaque::model::testing::exercise_record_case(input);
                seastar::thread::maybe_yield();
            }
        }
    }
}
