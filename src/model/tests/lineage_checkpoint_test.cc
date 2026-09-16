#include "src/bytes/test_allocation_profile.h"
#include "src/model/checkpoint_codec.h"
#include "src/model/tests/lineage_test_support.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/temporary_buffer.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {
namespace model = kwaque::model;
namespace codec = kwaque::codec;
using namespace kwaque::model::testing;
using namespace kwaque::model::testing::fixtures;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::bytes::fragmented_buffer;
using kwaque::bytes::fragmented_buffer_parser;
using kwaque::bytes::testing::charge;
constexpr codec::field_context context{.origin = 71, .family = 10};

codec::decode_budget memory() {
    // The other half reserves fixtures, small model snapshots, native SHA/CRC
    // state and coroutine frames. Each codec call carries its actual residual.
    return {byte_count{32U << 20U}, byte_count{1U << 20U}, charge};
}

std::string save(const lineage_frontier& before) {
    std::vector<model::range_cursor> cursors;
    cursors.reserve(before.size());
    for (std::size_t i = 0; i < before.size(); ++i)
        cursors.push_back(before.at(i).value());
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto built_result = model::make_read_checkpoint(
                          before.topic(), cursors, memory(), work, context)
                          .get();
    const auto& built = built_result.value();
    auto encoded_result = model::encode_read_checkpoint(
                            built.value,
                            work,
                            built.remaining.operation_remaining,
                            charge,
                            context)
                            .get();
    const auto& encoded = encoded_result.value();
    std::string wire;
    wire.reserve(encoded.bytes.size().value());
    for (const auto part : encoded.bytes)
        wire.append(part.data(), part.size());
    return wire;
} // All writer/model-codec owners end here; only copied bytes leave the call.

codec::result<model::decoded_read_checkpoint> decode_saved(
  std::string_view wire,
  std::size_t width = 7,
  model::topic_id expected = topic()) {
    std::vector<seastar::temporary_buffer<char>> fragments;
    fragments.reserve((wire.size() + width - 1U) / width);
    while (!wire.empty()) {
        const auto length = std::min(width, wire.size());
        fragments.emplace_back(wire.data(), length);
        wire.remove_prefix(length);
    }
    fragmented_buffer_parser input{
      fragmented_buffer::copy_from_fragments(fragments).value()};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto reserved = codec::reserve_decode_input(
      input, work.policy(), memory(), context);
    if (!reserved) return codec::failure(reserved.error());
    auto decoded = model::decode_read_checkpoint(
                     input,
                     expected,
                     *reserved,
                     work,
                     context,
                     codec::input_boundary::complete)
                     .get();
    if (decoded)
        EXPECT_TRUE(input.at_end());
    else
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
    return decoded;
}

kwaque::result<lineage_frontier>
restore(std::string_view wire, std::size_t width = 7) {
    auto decoded = decode_saved(wire, width);
    if (!decoded) return kwaque::failure(decoded.error().code());
    return lineage_frontier::make(
      decoded->value.topic(), decoded->value.cursors());
} // The decoded vector ends too; the restored model owns fixed scalar slots.

TEST(
  LineageCheckpointTest,
  RestartBetweenEitherMergeCompletionMatchesUninterruptedRun) {
    constexpr std::array<std::uint8_t, 2> parents{0xe0, 0x10};
    constexpr std::array<std::uint64_t, 2> ends{3, 7};
    for (std::size_t first = 0; first < parents.size(); ++first) {
        const auto second = 1U - first;
        std::string durable;
        std::optional<lineage_frontier> expected;
        {
            const auto original = history{}.make();
            const auto children
              = original.split(frontier(cursor(0x40, 5)), range(0x40)).value();
            const auto paused
              = original.advance(children, cursor(parents[first], ends[first]))
                  .value();
            durable = save(paused);
            const auto complete
              = original.advance(paused, cursor(parents[second], ends[second]))
                  .value();
            expected = original.merge(complete, range(0x20)).value();
        }
        // Reload equivalent committed facts in another enumeration. No reader
        // object, in-memory completion set or old frontier enters recovery.
        auto metadata = history{};
        std::reverse(metadata.ranges.begin(), metadata.ranges.end());
        std::reverse(metadata.edges.begin(), metadata.edges.end());
        const auto restarted = metadata.make();
        for (const auto width : {1U, 7U, 67U}) {
            const auto recovered = restore(durable, width);
            ASSERT_TRUE(recovered);
            ASSERT_TRUE(restarted.validate_frontier(*recovered));
            EXPECT_EQ(
              *recovered,
              frontier(
                cursor(parents[first], ends[first]),
                cursor(parents[second], 0)));
            EXPECT_EQ(save(*recovered), durable);
            const auto repeated
              = restarted
                  .advance(*recovered, cursor(parents[first], ends[first]))
                  .value();
            EXPECT_EQ(repeated, *recovered);
            const auto waiting = restarted.merge(repeated, range(0x20));
            ASSERT_FALSE(waiting);
            EXPECT_EQ(waiting.error(), errc::unavailable);
            const auto complete
              = restarted
                  .advance(repeated, cursor(parents[second], ends[second]))
                  .value();
            EXPECT_TRUE(restarted.validate_advance(repeated, complete));
            const auto resumed = restarted.merge(complete, range(0x20));
            ASSERT_TRUE(resumed);
            EXPECT_TRUE(restarted.validate_advance(complete, *resumed));
            EXPECT_EQ(*resumed, *expected);
        }
    }
}

TEST(
  LineageCheckpointTest,
  RestartAfterReplacementNeverResetsAnAdvancedSuccessor) {
    std::array<std::string, 2> durable;
    std::array<std::optional<lineage_frontier>, 2> expected;
    {
        const auto graph = history{}.make();
        const auto children
          = graph.split(frontier(cursor(0x40, 5)), range(0x40)).value();
        const auto merged
          = graph.merge(frontier(cursor(0xe0, 3), cursor(0x10, 7)), range(0x20))
              .value();
        expected[0] = graph.advance(children, cursor(0xe0, 2)).value();
        expected[1] = graph.advance(merged, cursor(0x20, 9)).value();
        for (std::size_t i = 0; i < durable.size(); ++i)
            durable[i] = save(*expected[i]);
    }
    const auto restarted = history{}.make();
    for (std::size_t i = 0; i < durable.size(); ++i) {
        const auto recovered = restore(durable[i]);
        ASSERT_TRUE(recovered);
        EXPECT_EQ(*recovered, *expected[i]);
        const auto repeated = i == 0 ? restarted.split(*recovered, range(0x40))
                                     : restarted.merge(*recovered, range(0x20));
        ASSERT_FALSE(repeated);
        EXPECT_EQ(repeated.error(), errc::not_found);
        EXPECT_EQ(save(*recovered), durable[i]);
    }
}

TEST(
  LineageCheckpointTest,
  StructuralDecodeLeavesTopologyAndAdvancementToTheModel) {
    const auto graph = split_merge_split_history{}.make();
    for (const auto& invalid :
         {frontier(cursor(0xe0, 1), cursor(0x90, 1)),
          frontier(cursor(0x99, 0))}) {
        const auto decoded = decode_saved(save(invalid));
        ASSERT_TRUE(decoded);
        const auto snapshot = lineage_frontier::make(
          decoded->value.topic(), decoded->value.cursors());
        ASSERT_TRUE(snapshot);
        EXPECT_EQ(*snapshot, invalid);
        EXPECT_FALSE(graph.validate_frontier(*snapshot));
    }
    const auto early_children = restore(
      save(frontier(cursor(0xe0, 0), cursor(0x10, 0))));
    ASSERT_TRUE(early_children);
    EXPECT_TRUE(graph.validate_frontier(*early_children));
    EXPECT_FALSE(
      graph.validate_advance(frontier(cursor(0x40, 4)), *early_children));
    EXPECT_TRUE(
      graph.validate_advance(frontier(cursor(0x40, 5)), *early_children));
    const auto wrong_topic = decode_saved(
      save(*early_children), 7, id<model::topic_id>(2));
    ASSERT_FALSE(wrong_topic);
    EXPECT_EQ(wrong_topic.error().code(), errc::wrong_context);
}

TEST(
  LineageCheckpointTest,
  RestartPreservesCompletedZeroParentAndMaximumLogicalBoundary) {
    history metadata;
    metadata.ranges[0].sealed_end = model::range_logical_end{0};
    metadata.ranges[1].sealed_end = model::range_logical_end{0};
    metadata.ranges[2].sealed_end = model::range_logical_end{maximum};
    const auto graph = metadata.make();
    const auto children
      = graph.split(frontier(cursor(0x40, 0)), range(0x40)).value();
    const auto recovered = restore(save(children), 1);
    ASSERT_TRUE(recovered);
    EXPECT_FALSE(graph.merge(*recovered, range(0x20)));
    const auto complete
      = graph.advance(*recovered, cursor(0x10, maximum)).value();
    const auto restored_complete = restore(save(complete), 1);
    ASSERT_TRUE(restored_complete);
    EXPECT_EQ(*restored_complete, complete);
    const auto merged = graph.merge(*restored_complete, range(0x20)).value();
    EXPECT_EQ(merged, frontier(cursor(0x20, 0)));
    const std::array only_root{
      node(1, model::keyspace_interval::root(), model::range_logical_end{0})};
    const auto empty_topic
      = lineage_model::make(topic(), only_root, {}).value();
    const auto initial = restore(save(frontier(cursor(1, 0))));
    ASSERT_TRUE(initial);
    EXPECT_TRUE(empty_topic.validate_frontier(*initial));
    EXPECT_EQ(initial->size(), 1);
}

TEST(
  LineageCheckpointTest,
  RestartPreservesIndependentSiblingProgressAcrossMerge) {
    std::string durable;
    std::optional<lineage_frontier> expected;
    {
        const auto graph = independent_history{}.make();
        const auto split
          = graph.split(frontier(cursor(2, 3), cursor(3, 1)), range(2)).value();
        const auto first = graph.advance(split, cursor(4, 2)).value();
        const auto paused = graph.advance(first, cursor(5, 4)).value();
        durable = save(paused);
        expected
          = graph.merge(graph.advance(paused, cursor(5, 5)).value(), range(8))
              .value();
    }
    const auto restarted = independent_history{}.make();
    const auto recovered = restore(durable);
    ASSERT_TRUE(recovered);
    EXPECT_EQ(*recovered, frontier(cursor(3, 1), cursor(4, 2), cursor(5, 4)));
    EXPECT_FALSE(restarted.merge(*recovered, range(8)));
    const auto completed = restarted.advance(*recovered, cursor(5, 5)).value();
    const auto merged = restarted.merge(completed, range(8)).value();
    EXPECT_EQ(merged, *expected);
    EXPECT_EQ(merged, frontier(cursor(3, 1), cursor(8, 0)));
}

TEST(
  LineageCheckpointTest,
  RepeatedFreshRangeHistorySurvivesEachMergeGateRestart) {
    const cycling_history metadata{model::range_logical_end{1}};
    auto current = frontier(cursor(1, 0));
    for (std::uint8_t parent = 1; parent < 16;
         parent = static_cast<std::uint8_t>(parent + 3U)) {
        const auto left = static_cast<std::uint8_t>(parent + 1U);
        const auto right = static_cast<std::uint8_t>(parent + 2U);
        const auto merged = static_cast<std::uint8_t>(parent + 3U);
        std::string durable;
        std::optional<lineage_frontier> expected;
        {
            const auto graph = metadata.make();
            const auto ended
              = graph.advance(current, cursor(parent, 1)).value();
            const auto children = graph.split(ended, range(parent)).value();
            const auto paused
              = graph.advance(children, cursor(left, 1)).value();
            durable = save(paused);
            expected = graph
                         .merge(
                           graph.advance(paused, cursor(right, 1)).value(),
                           range(merged))
                         .value();
        }
        const auto recovered = restore(durable);
        ASSERT_TRUE(recovered);
        const auto restarted = metadata.make();
        ASSERT_TRUE(restarted.validate_frontier(*recovered));
        EXPECT_FALSE(restarted.merge(*recovered, range(merged)));
        const auto completed
          = restarted.advance(*recovered, cursor(right, 1)).value();
        current = restarted.merge(completed, range(merged)).value();
        EXPECT_EQ(current, *expected);
        EXPECT_EQ(current, frontier(cursor(merged, 0)));
        EXPECT_EQ(restore(save(current)).value(), current);
    }
    EXPECT_EQ(current, frontier(cursor(16, 0)));
}

} // namespace
