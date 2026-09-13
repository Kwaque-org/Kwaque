#include "src/model/tests/record_fuzz_cases.h"

#include <seastar/core/thread.hh>

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <vector>

TEST(RecordFuzzCasesTest, IndependentOracleLiterals) {
    kwaque::model::testing::verify_record_oracle();
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
