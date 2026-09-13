#include "src/codec/tests/envelope_decode_test_support.h"
#include "src/codec/tests/envelope_fuzz_cases.h"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace {

namespace cases = kwaque::codec::testing;
namespace fixture = cases::envelope_fixture;

std::vector<std::uint8_t>
raw_case(std::string_view wire, std::uint8_t flags = 0) {
    std::vector<std::uint8_t> input{flags, 0, 0, 0, 0, 0};
    for (const auto octet : wire) {
        input.push_back(static_cast<std::uint8_t>(octet));
    }
    return input;
}

TEST(EnvelopeFuzzCasesTest, OracleMatchesIndependentLiteralAndErrorFacts) {
    cases::verify_envelope_fuzz_oracle();
}

TEST(
  EnvelopeFuzzCasesTest, IndependentLiteralAndEveryTruncationReachTheOracle) {
    const auto literal = std::string{fixture::fixed_prefix}
                         + std::string{fixture::fixed_body};
    EXPECT_EQ(fixture::crc32c(fixture::fixed_body), 0xdd0949a2U);
    for (std::size_t length = 0; length <= literal.size(); ++length) {
        for (const auto flags : std::array<std::uint8_t, 2>{0, 8}) {
            cases::exercise_envelope_case(
              raw_case(std::string_view{literal}.substr(0, length), flags));
        }
    }
}

TEST(
  EnvelopeFuzzCasesTest, StructuredMutationsExerciseRawAndRepairedIntegrity) {
    for (const auto flags : std::array<std::uint8_t, 3>{1, 3, 7}) {
        for (std::uint8_t mutation = 0; mutation < 32; ++mutation) {
            for (const auto shape : std::array<std::uint8_t, 3>{0, 1, 2}) {
                const std::array<std::uint8_t, 9> input{
                  flags, mutation, 17, shape, 0, 0, 'a', 'b', 'c'};
                cases::exercise_envelope_case(input);
            }
        }
    }
}

TEST(EnvelopeFuzzCasesTest, MaximumHeadersCountsAndExistingMarksRemainBounded) {
    for (const auto shape : std::array<std::uint8_t, 3>{2, 3, 4}) {
        for (const auto depth : std::array<std::uint8_t, 3>{0, 7, 8}) {
            const std::array<std::uint8_t, 7> input{
              23, 0, 0, shape, 0, depth, 'x'};
            cases::exercise_envelope_case(input);
        }
    }
    std::vector<std::uint8_t> maximum(16U * 1024U, 'x');
    maximum[0] = 7;
    maximum[1] = 0;
    maximum[2] = 0;
    maximum[3] = 3;
    maximum[4] = 0;
    maximum[5] = 0;
    cases::exercise_envelope_case(maximum);
}

TEST(
  EnvelopeFuzzCasesTest,
  IndependentContextChangesAndAdjacentBytesStayDistinct) {
    for (const auto mismatch : std::array<std::uint8_t, 4>{1, 2, 4, 8}) {
        const std::array<std::uint8_t, 9> input{
          23, 0, 0, 1, mismatch, 7, 'a', 'b', 'c'};
        cases::exercise_envelope_case(input);
    }
}

TEST(EnvelopeFuzzCasesTest, WorkAndResidualAdmissionHaveIndependentOutcomes) {
    const std::array<std::uint8_t, 9> input{7, 0, 0, 1, 0, 0, 'a', 'b', 'c'};
    cases::exercise_envelope_case(input, {.exhaust_operation = true});
    cases::exercise_envelope_case(input, {.exhaust_metadata = true});
    cases::exercise_envelope_case(input, {.work_bytes = 127});
    cases::exercise_envelope_case(input, {.work_items = 63});
    cases::exercise_envelope_case(input, {.work_bytes = 128, .work_items = 64});
}

TEST(
  EnvelopeFuzzCasesTest, CancellationAndOwnedCleanupPreserveEarlierFailures) {
    for (const auto mutation : std::array<std::uint8_t, 5>{0, 8, 16, 18, 26}) {
        const std::array<std::uint8_t, 9> input{
          1, mutation, 0, 1, 0, 0, 'a', 'b', 'c'};
        cases::exercise_envelope_case(input, {.initially_aborted = true});
        cases::exercise_envelope_case(input, {.queued_abort = true});
        cases::exercise_envelope_case(input, {.body_abort = true});
        cases::exercise_envelope_case(input, {.cleanup_abort = true});
        cases::exercise_envelope_case(
          input,
          {.queued_abort = true, .body_abort = true, .cleanup_abort = true});
    }
}

} // namespace
