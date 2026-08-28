#include "src/resource/workload_class.h"

#include <gtest/gtest.h>

#include <concepts>
#include <cstdint>
#include <set>
#include <stdexcept>
#include <string>

namespace kwaque::resource {

static_assert(!std::convertible_to<unsigned, workload_class>);
static_assert(!std::convertible_to<workload_class, unsigned>);

TEST(workload_class_test, descriptors_are_exhaustive_unique_and_bounded) {
    ASSERT_EQ(all_workload_classes.size(), 8U);
    ASSERT_EQ(workload_descriptors().size(), all_workload_classes.size());

    const std::array<std::string_view, workload_class_count> expected_names{
      "foreground_protocol",
      "consensus_critical",
      "replication",
      "metadata",
      "repair",
      "compaction",
      "offload",
      "maintenance",
    };
    std::set<std::string> names;
    std::uint64_t share_sum = 0;
    std::uint64_t memory_weight_sum = 0;
    for (std::size_t index = 0; index < workload_class_count; ++index) {
        const auto classification = all_workload_classes[index];
        const auto& descriptor = descriptor_for(classification);
        EXPECT_EQ(workload_index(classification), index);
        EXPECT_EQ(descriptor.classification, classification);
        EXPECT_EQ(descriptor.metric_name, expected_names[index]);
        EXPECT_EQ(to_string(classification), expected_names[index]);
        EXPECT_LE(descriptor.metric_name.size(), 32U);
        EXPECT_GT(descriptor.scheduling_shares, 0U);
        EXPECT_LE(descriptor.scheduling_shares, 2000U);
        EXPECT_GT(descriptor.max_nonlocal_requests, 0U);
        EXPECT_GT(descriptor.memory_weight, 0U);
        EXPECT_TRUE(names.emplace(descriptor.metric_name).second);
        share_sum += descriptor.scheduling_shares;
        memory_weight_sum += descriptor.memory_weight;
    }

    EXPECT_GT(share_sum, 0U);
    EXPECT_EQ(memory_weight_sum, 100U);
    const auto foreground
      = descriptor_for(workload_class::foreground_protocol).scheduling_shares;
    const auto replication
      = descriptor_for(workload_class::replication).scheduling_shares;
    const auto consensus
      = descriptor_for(workload_class::consensus_critical).scheduling_shares;
    EXPECT_GT(consensus, foreground);
    EXPECT_GT(consensus, replication);
    for (const auto background : {
           workload_class::repair,
           workload_class::compaction,
           workload_class::offload,
           workload_class::maintenance,
         }) {
        EXPECT_GE(foreground, descriptor_for(background).scheduling_shares);
        EXPECT_GE(replication, descriptor_for(background).scheduling_shares);
    }
}

TEST(workload_class_test, invalid_values_are_rejected_by_descriptor_lookup) {
    const auto invalid = static_cast<workload_class>(workload_class_count);
    EXPECT_EQ(to_string(invalid), "unknown");
    EXPECT_THROW(static_cast<void>(descriptor_for(invalid)), std::out_of_range);
}

} // namespace kwaque::resource
