#include "src/base/metric_schema.h"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace {

std::uint64_t schema_fingerprint() {
    std::uint64_t value = UINT64_C(14695981039346656037);
    const auto mix = [&value](std::uint8_t byte) {
        value ^= byte;
        value *= UINT64_C(1099511628211);
    };
    const auto mix_text = [&mix](std::string_view text) {
        for (const auto byte : text) {
            mix(static_cast<std::uint8_t>(static_cast<unsigned char>(byte)));
        }
        mix(UINT8_C(255));
    };
    for (const auto& descriptor : kwaque::metric_descriptors()) {
        const auto id = static_cast<std::uint16_t>(descriptor.id);
        mix(static_cast<std::uint8_t>(id));
        mix(static_cast<std::uint8_t>(id >> 8U));
        mix_text(descriptor.group);
        mix_text(descriptor.name);
        mix_text(descriptor.help);
        mix(static_cast<std::uint8_t>(descriptor.kind));
        mix(static_cast<std::uint8_t>(descriptor.labels));
        mix(static_cast<std::uint8_t>(descriptor.aggregate_shard));
    }
    return value;
}

static_assert(kwaque::metric_inventory_size == 81U);
static_assert(kwaque::metric_workload_values == 8U);
static_assert(kwaque::metric_series_per_shard == 109U);

TEST(MetricSchemaTest, InventoryIsAppendOnlyBoundedAndLowCardinality) {
    const auto descriptors = kwaque::metric_descriptors();
    ASSERT_EQ(descriptors.size(), kwaque::metric_inventory_size);
    std::size_t series = 0;
    for (std::size_t index = 0; index < descriptors.size(); ++index) {
        const auto& descriptor = descriptors[index];
        EXPECT_EQ(static_cast<std::uint16_t>(descriptor.id), index + 1U);
        EXPECT_EQ(kwaque::descriptor_for(descriptor.id), &descriptor);
        EXPECT_FALSE(descriptor.group.empty());
        EXPECT_FALSE(descriptor.name.empty());
        EXPECT_FALSE(descriptor.help.empty());
        EXPECT_TRUE(descriptor.aggregate_shard);
        EXPECT_TRUE(
          descriptor.labels == kwaque::metric_label_domain::none
          || descriptor.labels == kwaque::metric_label_domain::workload);
        series += kwaque::metric_series_count(descriptor.labels);

        for (const auto forbidden : std::array{
               std::string_view{"path"},
               std::string_view{"host"},
               std::string_view{"seed"},
               std::string_view{"object"},
               std::string_view{"stable_id"},
               std::string_view{"occurrence"},
               std::string_view{"token"},
               std::string_view{"credential"}}) {
            EXPECT_EQ(descriptor.name.find(forbidden), std::string_view::npos);
            EXPECT_EQ(descriptor.group.find(forbidden), std::string_view::npos);
        }
    }
    EXPECT_EQ(series, kwaque::metric_series_per_shard);
    EXPECT_EQ(series, 109U);
    EXPECT_EQ(schema_fingerprint(), UINT64_C(0xd4b64689fc2d4d3c));
    EXPECT_EQ(
      kwaque::metric_series_count(kwaque::metric_label_domain::none), 1U);
    EXPECT_EQ(
      kwaque::metric_series_count(kwaque::metric_label_domain::workload), 8U);
    EXPECT_EQ(
      kwaque::metric_series_count(
        static_cast<kwaque::metric_label_domain>(255)),
      0U);
    EXPECT_EQ(
      kwaque::descriptor_for(static_cast<kwaque::metric_id>(0)), nullptr);
    EXPECT_EQ(
      kwaque::descriptor_for(static_cast<kwaque::metric_id>(255)), nullptr);
}

TEST(MetricSchemaTest, WorkloadLabelHasOneExactClosedValueSet) {
    constexpr std::array<std::string_view, 8> expected{
      "foreground_protocol",
      "consensus_critical",
      "replication",
      "metadata",
      "repair",
      "compaction",
      "offload",
      "maintenance",
    };
    EXPECT_EQ(kwaque::metric_workload_label, "workload");
    EXPECT_EQ(kwaque::metric_workload_label_values, expected);
}

TEST(MetricSchemaTest, DeviceChainStepAndRefusalLabelsHaveClosedValueSets) {
    EXPECT_EQ(kwaque::metric_device_label, "device");
    EXPECT_EQ(
      kwaque::metric_device_label_values,
      (std::array<std::string_view, 8>{
        "0", "1", "2", "3", "4", "5", "6", "7"}));
    EXPECT_EQ(kwaque::metric_chain_step_label, "chain_step");
    EXPECT_EQ(
      kwaque::metric_chain_step_label_values,
      (std::array<std::string_view, 7>{
        "envelope_size",
        "ingress_memory",
        "stream_window",
        "range_queue",
        "shard_capacity",
        "forwarded_traffic",
        "storage_pressure"}));
    EXPECT_EQ(kwaque::metric_refusal_label, "refusal");
    // The definite append refusals, codes 203 to 223, in code order.
    EXPECT_EQ(
      kwaque::metric_refusal_label_values,
      (std::array<std::string_view, 21>{
        "batch_id_conflict",
        "stale",
        "sequence_gap",
        "window_exceeded",
        "unknown_stream",
        "stream_sealed",
        "stream_failed",
        "producer_fenced",
        "invalid_ack_floor",
        "stream_limit",
        "topic_sealed",
        "not_found",
        "misrouted_key",
        "stale_route",
        "no_space",
        "too_large",
        "invalid_batch",
        "timed_out_before_acceptance",
        "aborted_before_acceptance",
        "closed",
        "unknown_producer"}));
    EXPECT_EQ(
      kwaque::metric_series_count(kwaque::metric_label_domain::device), 8U);
    EXPECT_EQ(
      kwaque::metric_series_count(kwaque::metric_label_domain::chain_step), 7U);
    EXPECT_EQ(
      kwaque::metric_series_count(kwaque::metric_label_domain::refusal), 21U);
}

TEST(MetricSchemaTest, OnlyResourceMetricsUseTheWorkloadLabel) {
    for (const auto& descriptor : kwaque::metric_descriptors()) {
        if (descriptor.labels == kwaque::metric_label_domain::workload) {
            EXPECT_EQ(descriptor.group, "resource_manager");
            EXPECT_GE(
              static_cast<std::uint16_t>(descriptor.id),
              static_cast<std::uint16_t>(
                kwaque::metric_id::memory_configured_bytes));
            EXPECT_LE(
              static_cast<std::uint16_t>(descriptor.id),
              static_cast<std::uint16_t>(kwaque::metric_id::memory_waiters));
        }
    }
}

} // namespace
