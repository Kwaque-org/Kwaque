#include <gtest/gtest.h>

#include <seastar/core/shard_id.hh>
#include <seastar/core/smp.hh>

TEST(SeastarGtest, RunsOnTheConfiguredReactor) {
  EXPECT_EQ(seastar::this_shard_id(), 0U);
  EXPECT_EQ(seastar::this_smp_shard_count(), 2U);
}
