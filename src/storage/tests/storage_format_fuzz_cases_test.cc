#include "src/storage/tests/storage_format_fixture.h"
#include "src/storage/tests/storage_format_fuzz_cases.h"

#include <gtest/gtest.h>

namespace kwaque::storage::testing {
TEST(StorageFuzzCasesTest, FamiliesMutationsAndCompatibleNestedProfiles) {
    for (std::uint8_t kind = 0; kind < 7; ++kind) {
        for (std::uint8_t mutation = 0; mutation < 9; ++mutation) {
            for (const auto variant :
                 std::array<std::uint8_t, 4>{0, 1, 2, 255}) {
                const std::array<std::uint8_t, 8> script{
                  kind, mutation, 0, mutation, 31, 0, 7, variant};
                exercise_storage_case(script);
                seastar::thread::maybe_yield();
            }
        }
    }
}
TEST(StorageFuzzCasesTest, ResourceFailuresCancellationMarksAndTerminalOrigin) {
    for (std::uint8_t kind = 0; kind < 6; ++kind) {
        for (const auto flags :
             std::array<std::uint8_t, 6>{2, 4, 8, 16, 32, 62}) {
            const std::array<std::uint8_t, 8> script{
              kind, 0, flags, 0, 0, 0, 0, 0};
            exercise_storage_case(script);
        }
    }
}
TEST(StorageFuzzCasesTest, RawGoldenObjectsAndMalformedMaximumInput) {
    for (std::uint8_t kind = 0; kind < 6; ++kind) {
        const storage_fixture fixture{static_cast<storage_case>(kind)};
        std::vector<std::uint8_t> script{kind, 0, 1, 0, 0, 0, 0, 0};
        script.insert(script.end(), fixture.wire.begin(), fixture.wire.end());
        exercise_storage_case(script);
        script.resize(storage_fuzz_max_input, 0xff);
        exercise_storage_case(script);
    }
    exercise_storage_case({});
    const std::vector<std::uint8_t> oversized(storage_fuzz_max_input + 1, 255);
    exercise_storage_case(oversized);
}
} // namespace kwaque::storage::testing
