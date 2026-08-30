#include "src/simulation/virtual_time.h"

#include <gtest/gtest.h>

TEST(VirtualTimeInvariantDeathTest, StaticClockRejectsMissingBinding) {
    EXPECT_DEATH(
      { static_cast<void>(kwaque::simulation::monotonic_clock::now()); },
      "id=KQ-CLOCK-BOUND");
}
