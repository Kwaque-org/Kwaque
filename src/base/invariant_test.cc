#include "src/base/invariant.h"

#include <gtest/gtest.h>

#include <string_view>
#include <type_traits>

namespace {

static_assert(!std::is_convertible_v<std::string_view, kwaque::invariant_id>);
static_assert(std::is_constructible_v<kwaque::invariant_id, std::string_view>);
static_assert(kwaque::invariant_id{"KQ-INVARIANT-TEST-001"}.valid());
static_assert(!kwaque::invariant_id{""}.valid());
static_assert(!kwaque::invariant_id{"not-stable"}.valid());

TEST(InvariantTest, DebugAssertionEvaluationMatchesBuildMode) {
    int evaluations = 0;
    KWAQUE_DEBUG_ASSERT(
      kwaque::invariant_id{"KQ-DEBUG-ASSERT-TEST"},
      (++evaluations, true),
      "debug evaluation probe");

#if KWAQUE_ENABLE_DEBUG_ASSERTIONS
    EXPECT_EQ(evaluations, 1);
#else
    EXPECT_EQ(evaluations, 0);
#endif
}

TEST(InvariantTest, SuccessfulInvariantDoesNotEvaluateFailureMetadata) {
    int id_evaluations = 0;
    int context_evaluations = 0;
    KWAQUE_INVARIANT(
      (++id_evaluations, kwaque::invariant_id{"KQ-INVARIANT-COLD-METADATA"}),
      true,
      (++context_evaluations, std::string_view{"unused"}));

    EXPECT_EQ(id_evaluations, 0);
    EXPECT_EQ(context_evaluations, 0);
}

TEST(InvariantDeathTest, AlwaysOnInvariantEmitsStableSingleLineIdentity) {
    EXPECT_DEATH(
      KWAQUE_INVARIANT(
        kwaque::invariant_id{"KQ-INVARIANT-TEST-001"},
        false,
        "first line\nsecond line"),
      "id=KQ-INVARIANT-TEST-001.*expression=false.*context=first "
      "line\\\\nsecond "
      "line.*source=invariant_test.cc:[0-9]+");
}

} // namespace
