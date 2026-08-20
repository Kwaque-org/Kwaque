#include <seastar/core/shard_id.hh>
#include <seastar/core/smp.hh>

#include <gtest/gtest.h>

TEST(SeastarGtest, RunsOnTheConfiguredReactor) {
    EXPECT_EQ(seastar::this_shard_id(), 0U);
    EXPECT_EQ(seastar::this_smp_shard_count(), 2U);
}
