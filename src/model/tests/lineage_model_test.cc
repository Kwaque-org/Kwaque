#include "src/base/error.h"
#include "src/model/tests/lineage_model.h"
#include "src/model/tests/lineage_test_support.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

namespace {
namespace model = kwaque::model;
using namespace kwaque::model::testing;
using kwaque::errc;
using kwaque::item_count;
using namespace kwaque::model::testing::fixtures;

static_assert(!std::is_default_constructible_v<lineage_model>);
static_assert(!std::is_default_constructible_v<lineage_frontier>);
static_assert(std::is_trivially_copyable_v<lineage_model>);
static_assert(std::is_trivially_copyable_v<lineage_frontier>);
static_assert(std::is_nothrow_copy_constructible_v<lineage_model>);
static_assert(noexcept(lineage_model::make({}, {}, {})));
template<typename T>
concept temporary_range_view = requires(T&& value) {
    std::move(value).ranges();
};
static_assert(!temporary_range_view<lineage_model>);

TEST(LineageModelTest, OwnsCommittedFactsAndExplicitTransitiveRelations) {
    history source;
    const auto graph = source.make();
    EXPECT_EQ(graph.root(), range(0x40));
    EXPECT_EQ(graph.ranges().size(), 4);
    EXPECT_EQ(graph.edges().size(), 4);
    EXPECT_TRUE(graph.precedes(range(0x40), range(0x20)).value());
    EXPECT_TRUE(graph.precedes(range(0xe0), range(0x20)).value());
    EXPECT_FALSE(graph.precedes(range(0x10), range(0xe0)).value());
    EXPECT_FALSE(graph.precedes(range(0x20), range(0x40)).value());
    EXPECT_FALSE(graph.precedes(range(0x40), range(0x40)).value());
    const auto unknown = graph.precedes(range(0x99), range(0x40));
    ASSERT_FALSE(unknown);
    EXPECT_EQ(unknown.error(), errc::not_found);
    source.ranges[0].sealed_end.reset();
    source.edges[0] = edge(0x99, 0x20);
    EXPECT_EQ(graph.ranges()[0].sealed_end, model::range_logical_end{5});
    EXPECT_EQ(graph.edges()[0], edge(0x40, 0xe0));
}

TEST(LineageModelTest, RootOnlyViewDistinguishesUnknownAndZeroSeal) {
    for (const auto& seal :
         {std::optional<model::range_logical_end>{},
          std::optional{model::range_logical_end{0}},
          std::optional{model::range_logical_end{maximum}}}) {
        const std::array ranges{
          node(1, model::keyspace_interval::root(), seal)};
        const auto graph = lineage_model::make(topic(), ranges, {});
        ASSERT_TRUE(graph);
        EXPECT_EQ(graph->ranges()[0].sealed_end, seal);
        const auto before = frontier(cursor(1, 0));
        const auto attempted = graph->split(before, range(1));
        ASSERT_FALSE(attempted);
        EXPECT_EQ(attempted.error(), errc::invalid_argument);
        EXPECT_EQ(before.size(), 1);
    }
}

TEST(LineageModelTest, RejectsInvalidIdentityTopicRootAndSeals) {
    const auto empty = lineage_model::make(topic(), {}, {});
    ASSERT_FALSE(empty);
    EXPECT_EQ(empty.error(), errc::invalid_argument);
    history source;
    EXPECT_FALSE(lineage_model::make({}, source.ranges, source.edges));
    for (unsigned mutation = 0; mutation < 8; ++mutation) {
        SCOPED_TRACE(mutation);
        auto modified = source;
        if (mutation == 0) modified.ranges[0].id = {};
        if (mutation == 1) modified.ranges[1].id = modified.ranges[0].id;
        if (mutation == 2) modified.ranges[1].topic = {};
        if (mutation == 3) modified.ranges[1].topic = id<model::topic_id>(2);
        if (mutation == 4) modified.ranges[0].interval = interval(0, 1);
        if (mutation == 5) modified.ranges[0].sealed_end.reset();
        if (mutation == 6) modified.ranges[1].sealed_end.reset();
        if (mutation == 7) modified.ranges[2].sealed_end.reset();
        const auto result = lineage_model::make(
          topic(), modified.ranges, modified.edges);
        ASSERT_FALSE(result);
        EXPECT_EQ(
          result.error(),
          mutation == 3 ? errc::wrong_context : errc::invalid_argument);
    }
}

TEST(LineageModelTest, RejectsUnknownDuplicateSelfAndMissingEdges) {
    const history source;
    for (unsigned mutation = 0; mutation < 6; ++mutation) {
        SCOPED_TRACE(mutation);
        auto edges = source.edges;
        if (mutation == 0) edges[0].predecessor = range(0x99);
        if (mutation == 1) edges[0].successor = range(0x99);
        if (mutation == 2) edges[0].successor = edges[0].predecessor;
        if (mutation == 3) edges[1] = edges[0];
        if (mutation == 4) edges[3] = edges[2];
        const auto supplied = mutation == 5 ? std::span{edges}.first(3)
                                            : std::span{edges};
        const auto result = lineage_model::make(
          topic(), source.ranges, supplied);
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), errc::invalid_argument);
    }
    const auto missing_split_child = lineage_model::make(
      topic(),
      std::span{source.ranges}.first(2),
      std::span{source.edges}.first(1));
    EXPECT_FALSE(missing_split_child);
}

TEST(LineageModelTest, RejectsCyclesAndDisconnectedHistory) {
    const std::array ranges{
      node(1, model::keyspace_interval::root(), model::range_logical_end{}),
      node(2, interval(0, 1)),
      node(3, interval(half, 1)),
      node(4, interval(0, 1), model::range_logical_end{}),
      node(5, interval(0, 1), model::range_logical_end{})};
    const std::array edges{edge(1, 2), edge(1, 3), edge(4, 5), edge(5, 4)};
    EXPECT_FALSE(lineage_model::make(topic(), ranges, edges));
    // One connected root plus a second isolated root is not a complete view.
    EXPECT_FALSE(
      lineage_model::make(
        topic(), std::span{ranges}.first(4), std::span{edges}.first(2)));
    const std::array cycle{edge(1, 4), edge(4, 5), edge(5, 1)};
    const std::array cyclic_ranges{ranges[0], ranges[3], ranges[4]};
    EXPECT_FALSE(lineage_model::make(topic(), cyclic_ranges, cycle));
}

TEST(LineageModelTest, RejectsIncompleteOrConflictingSplitMergeGeometry) {
    for (unsigned mutation = 0; mutation < 5; ++mutation) {
        history source;
        if (mutation == 0)
            source.ranges[2].interval = source.ranges[1].interval;
        if (mutation == 1) {
            source.ranges[1].interval = interval(0, 2);
            source.ranges[2].interval = interval(quarter, 2);
        }
        if (mutation == 2) {
            source.ranges[1].interval = interval(quarter, 2);
            source.ranges[2].interval = interval(half, 2);
        }
        if (mutation == 3) source.ranges[3].interval = interval(0, 1);
        if (mutation == 4) {
            source.ranges[1].interval = interval(maximum - 1U, 64);
            source.ranges[2].interval = interval(maximum, 64);
        }
        EXPECT_FALSE(lineage_model::make(topic(), source.ranges, source.edges));
    }
    history source;
    const std::array ranges{
      source.ranges[0],
      source.ranges[1],
      source.ranges[2],
      source.ranges[3],
      node(0x99, interval(0, 2))};
    const std::array edges{
      source.edges[0],
      source.edges[1],
      source.edges[2],
      source.edges[3],
      edge(0xe0, 0x99)};
    // A parent cannot feed a merge while also feeding a different transition.
    EXPECT_FALSE(lineage_model::make(topic(), ranges, edges));
    auto three_children = edges;
    three_children.back() = edge(0x40, 0x99);
    EXPECT_FALSE(lineage_model::make(topic(), ranges, three_children));
}

TEST(LineageModelTest, RangeCapIncludesRetiredHistoryEvenWhenOneRangeRemains) {
    const cycling_history source;
    std::array<lineage_range, 17> ranges{};
    std::copy(source.ranges.begin(), source.ranges.end(), ranges.begin());
    const auto& edges = source.edges;
    const auto graph = lineage_model::make(
      topic(), std::span{ranges}.first(16), edges);
    ASSERT_TRUE(graph);
    EXPECT_TRUE(graph->precedes(range(1), range(16)).value());
    ranges[16] = node(17, model::keyspace_interval::root());
    const auto overflow = lineage_model::make(topic(), ranges, edges);
    ASSERT_FALSE(overflow);
    EXPECT_EQ(overflow.error(), errc::resource_exhausted);
    std::array<lineage_edge, 33> excessive{};
    excessive.fill(edge(1, 2));
    const auto too_many_edges = lineage_model::make(
      topic(), std::span{ranges}.first(16), excessive);
    ASSERT_FALSE(too_many_edges);
    EXPECT_EQ(too_many_edges.error(), errc::resource_exhausted);
}

TEST(
  LineageModelTest, FrontierChecksStructuralOrderAndReturnsIndependentScalars) {
    EXPECT_FALSE(lineage_frontier::make(topic(), {}));
    const std::array one{cursor(0x40, 0)};
    EXPECT_FALSE(lineage_frontier::make({}, one));
    const std::array descending{cursor(0xe0, 3), cursor(0x10, 7)};
    EXPECT_FALSE(lineage_frontier::make(topic(), descending));
    for (const auto next : {std::uint64_t{3}, std::uint64_t{7}}) {
        const std::array duplicates{cursor(0xe0, 3), cursor(0xe0, next)};
        EXPECT_FALSE(lineage_frontier::make(topic(), duplicates));
    }
    const auto value = frontier(cursor(0xe0, 3), cursor(0x10, maximum));
    EXPECT_EQ(value.at(0).value(), cursor(0x10, maximum));
    EXPECT_EQ(value.at(1).value(), cursor(0xe0, 3));
    const auto out_of_bounds = value.at(2);
    ASSERT_FALSE(out_of_bounds);
    EXPECT_EQ(out_of_bounds.error(), errc::out_of_range);
    EXPECT_FALSE(value.find(range(0x99)));
    const std::vector excessive(maximum_lineage_ranges + 1U, cursor(1, 0));
    const auto too_many = lineage_frontier::make(topic(), excessive);
    ASSERT_FALSE(too_many);
    EXPECT_EQ(too_many.error(), errc::resource_exhausted);
}

TEST(LineageModelTest, SplitWaitsForExactEndAndPublishesBothChildrenAtZero) {
    const auto graph = history{}.make();
    const auto partial = frontier(cursor(0x40, 4));
    const auto waiting = graph.split(partial, range(0x40));
    ASSERT_FALSE(waiting);
    EXPECT_EQ(waiting.error(), errc::unavailable);
    EXPECT_EQ(partial, frontier(cursor(0x40, 4)));
    const auto complete = frontier(cursor(0x40, 5));
    const auto result = graph.split(complete, range(0x40));
    ASSERT_TRUE(result);
    EXPECT_EQ(*result, frontier(cursor(0xe0, 0), cursor(0x10, 0)));
    EXPECT_FALSE(result->find(range(0x40)));
    EXPECT_EQ(complete, frontier(cursor(0x40, 5)));
    const auto repeated = graph.split(*result, range(0x40));
    ASSERT_FALSE(repeated);
    EXPECT_EQ(repeated.error(), errc::not_found);
    const auto advanced = frontier(cursor(0xe0, 2), cursor(0x10, 4));
    EXPECT_FALSE(graph.split(advanced, range(0x40)));
    EXPECT_EQ(advanced, frontier(cursor(0xe0, 2), cursor(0x10, 4)));
}

TEST(LineageModelTest, SplitPreservesSiblingProgressAndCapFailureIsAtomic) {
    const std::array ranges{
      node(1, model::keyspace_interval::root(), model::range_logical_end{5}),
      node(2, interval(0, 1), model::range_logical_end{3}),
      node(3, interval(half, 1)),
      node(4, interval(0, 2)),
      node(5, interval(quarter, 2))};
    const std::array edges{edge(1, 2), edge(1, 3), edge(2, 4), edge(2, 5)};
    const auto graph = lineage_model::make(topic(), ranges, edges);
    ASSERT_TRUE(graph);
    const auto before = frontier(cursor(2, 3), cursor(3, maximum));
    for (const auto cap : {0U, 1U, 2U}) {
        const auto rejected = graph->split(before, range(2), item_count{cap});
        ASSERT_FALSE(rejected);
        EXPECT_EQ(rejected.error(), errc::resource_exhausted);
        EXPECT_EQ(before, frontier(cursor(2, 3), cursor(3, maximum)));
    }
    const auto after = graph->split(before, range(2), item_count{3});
    ASSERT_TRUE(after);
    EXPECT_EQ(*after, frontier(cursor(3, maximum), cursor(4, 0), cursor(5, 0)));
}

TEST(LineageModelTest, MergeRetainsCompletedParentsInEitherCompletionOrder) {
    const auto graph = history{}.make();
    for (const bool left_first : {false, true}) {
        const auto before = left_first
                              ? frontier(cursor(0xe0, 3), cursor(0x10, 6))
                              : frontier(cursor(0xe0, 2), cursor(0x10, 7));
        for (unsigned attempt = 0; attempt < 3; ++attempt) {
            const auto waiting = graph.merge(before, range(0x20));
            ASSERT_FALSE(waiting);
            EXPECT_EQ(waiting.error(), errc::unavailable);
            EXPECT_EQ(
              before.find(range(0xe0))->next().value(), left_first ? 3U : 2U);
            EXPECT_EQ(
              before.find(range(0x10))->next().value(), left_first ? 6U : 7U);
            EXPECT_FALSE(before.find(range(0x20)));
        }
        const auto completed = frontier(cursor(0xe0, 3), cursor(0x10, 7));
        const auto after = graph.merge(completed, range(0x20), item_count{1});
        ASSERT_TRUE(after);
        EXPECT_EQ(*after, frontier(cursor(0x20, 0)));
        EXPECT_EQ(completed, frontier(cursor(0xe0, 3), cursor(0x10, 7)));
    }
}

TEST(
  LineageModelTest, MissingOrRepeatedParentCannotSatisfyMergeOrResetSuccessor) {
    const auto graph = history{}.make();
    const auto missing = frontier(cursor(0xe0, 3));
    EXPECT_FALSE(graph.merge(missing, range(0x20)));
    const auto earlier = frontier(cursor(0x40, 0));
    const auto absent = graph.merge(earlier, range(0x20));
    ASSERT_FALSE(absent);
    EXPECT_EQ(absent.error(), errc::not_found);
    const auto progressed = frontier(cursor(0x20, maximum));
    const auto repeated = graph.merge(progressed, range(0x20));
    ASSERT_FALSE(repeated);
    EXPECT_EQ(repeated.error(), errc::not_found);
    EXPECT_EQ(progressed, frontier(cursor(0x20, maximum)));
    const auto completed = frontier(cursor(0xe0, 3), cursor(0x10, 7));
    const auto denied = graph.merge(completed, range(0x20), item_count{});
    ASSERT_FALSE(denied);
    EXPECT_EQ(denied.error(), errc::resource_exhausted);
    EXPECT_EQ(completed, frontier(cursor(0xe0, 3), cursor(0x10, 7)));
}

TEST(LineageModelTest, MergePreservesUnrelatedSiblingAndSealedTerminalRange) {
    const std::array ranges{
      node(1, model::keyspace_interval::root(), model::range_logical_end{5}),
      node(2, interval(0, 1), model::range_logical_end{3}),
      node(3, interval(half, 1)),
      node(4, interval(0, 2), model::range_logical_end{2}),
      node(5, interval(quarter, 2), model::range_logical_end{4}),
      node(6, interval(0, 1), model::range_logical_end{0})};
    const std::array edges{
      edge(1, 2), edge(1, 3), edge(2, 4), edge(2, 5), edge(4, 6), edge(5, 6)};
    const auto graph = lineage_model::make(topic(), ranges, edges);
    ASSERT_TRUE(graph);
    const auto before = frontier(
      cursor(3, maximum), cursor(4, 2), cursor(5, 4));
    const auto denied = graph->merge(before, range(6), item_count{1});
    ASSERT_FALSE(denied);
    EXPECT_EQ(denied.error(), errc::resource_exhausted);
    const auto after = graph->merge(before, range(6), item_count{2});
    ASSERT_TRUE(after);
    EXPECT_EQ(*after, frontier(cursor(3, maximum), cursor(6, 0)));
    EXPECT_EQ(before, frontier(cursor(3, maximum), cursor(4, 2), cursor(5, 4)));
    EXPECT_FALSE(graph->split(*after, range(6)));
    EXPECT_TRUE(after->find(range(6)));
}

TEST(LineageModelTest, ZeroAndMaximumSealsAreExactLogicalBoundaries) {
    for (const auto end : {std::uint64_t{0}, maximum}) {
        history source;
        source.ranges[0].sealed_end = model::range_logical_end{end};
        source.ranges[1].sealed_end = model::range_logical_end{0};
        source.ranges[2].sealed_end = model::range_logical_end{maximum};
        const auto graph = source.make();
        const auto children = graph.split(
          frontier(cursor(0x40, end)), range(0x40));
        ASSERT_TRUE(children);
        EXPECT_EQ(*children, frontier(cursor(0xe0, 0), cursor(0x10, 0)));
        const auto waiting = graph.merge(
          frontier(cursor(0xe0, 0), cursor(0x10, maximum - 1U)), range(0x20));
        ASSERT_FALSE(waiting);
        EXPECT_EQ(waiting.error(), errc::unavailable);
        const auto after = graph.merge(
          frontier(cursor(0xe0, 0), cursor(0x10, maximum)), range(0x20));
        ASSERT_TRUE(after);
        EXPECT_EQ(*after, frontier(cursor(0x20, 0)));
    }
}

TEST(LineageModelTest, TransitionsRejectWrongTopicUnknownRangeAndPastEnd) {
    const auto graph = history{}.make();
    const std::array root{cursor(0x40, 5)};
    const auto wrong_topic
      = lineage_frontier::make(id<model::topic_id>(2), root).value();
    const auto wrong = graph.split(wrong_topic, range(0x40));
    ASSERT_FALSE(wrong);
    EXPECT_EQ(wrong.error(), errc::wrong_context);
    const auto unknown = graph.split(frontier(cursor(0x99, 0)), range(0x40));
    ASSERT_FALSE(unknown);
    EXPECT_EQ(unknown.error(), errc::not_found);
    const auto past_end = graph.split(frontier(cursor(0x40, 6)), range(0x40));
    ASSERT_FALSE(past_end);
    EXPECT_EQ(past_end.error(), errc::out_of_range);
    EXPECT_FALSE(
      graph.merge(frontier(cursor(0xe0, 4), cursor(0x10, 7)), range(0x20)));
    EXPECT_FALSE(
      graph.split(frontier(cursor(0x40, 5), cursor(0xe0, 0)), range(0x40)));
    EXPECT_FALSE(graph.split(frontier(cursor(0x40, 5)), range(0x99)));
}

TEST(
  LineageModelTest, StructuralAndGeometricValidityDoNotProveACausalFrontier) {
    const split_merge_split_history source;
    const auto graph = source.make();
    const std::array disjoint{
      source.ranges[1].interval, source.ranges[5].interval};
    ASSERT_TRUE(
      model::validate_keyspace_coverage(
        disjoint, model::keyspace_interval::root()));
    const auto invalid = frontier(cursor(0xe0, 1), cursor(0x90, 1));
    ASSERT_TRUE(graph.precedes(range(0xe0), range(0x90)).value());
    const auto rejected = graph.validate_frontier(invalid);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error(), errc::invalid_argument);
    EXPECT_FALSE(
      graph.validate_frontier(frontier(cursor(0x70, 1), cursor(0x10, 1))));
    for (const auto& valid :
         {frontier(cursor(0x40, 0)),
          frontier(cursor(0xe0, 1), cursor(0x10, 2)),
          frontier(cursor(0x20, 5)),
          frontier(cursor(0x70, 1), cursor(0x90, 2))}) {
        EXPECT_TRUE(graph.validate_frontier(valid));
        EXPECT_FALSE(graph.validate_advance(invalid, valid));
        EXPECT_FALSE(graph.validate_advance(valid, invalid));
    }
}

TEST(LineageModelTest, ValidDestinationStillRequiresOneEnabledTransition) {
    const auto graph = split_merge_split_history{}.make();
    const auto root_partial = frontier(cursor(0x40, 4));
    const auto root_end = frontier(cursor(0x40, 5));
    const auto children = frontier(cursor(0xe0, 0), cursor(0x10, 0));
    const auto merged = frontier(cursor(0x20, 0));
    const auto grandchildren = frontier(cursor(0x70, 0), cursor(0x90, 0));
    ASSERT_TRUE(graph.validate_frontier(children));
    ASSERT_TRUE(graph.validate_frontier(merged));
    ASSERT_TRUE(graph.validate_frontier(grandchildren));
    const auto too_early = graph.validate_advance(root_partial, children);
    ASSERT_FALSE(too_early);
    EXPECT_EQ(too_early.error(), errc::unavailable);
    EXPECT_TRUE(graph.validate_advance(root_end, children));
    EXPECT_FALSE(graph.validate_advance(root_end, merged));
    EXPECT_FALSE(graph.validate_advance(root_end, grandchildren));
    EXPECT_FALSE(graph.validate_advance(
      root_end, frontier(cursor(0xe0, 1), cursor(0x10, 0))));
    EXPECT_FALSE(graph.validate_advance(
      frontier(cursor(0xe0, 3), cursor(0x10, 6)), merged));
    const auto parents_end = frontier(cursor(0xe0, 3), cursor(0x10, 7));
    EXPECT_TRUE(graph.validate_advance(parents_end, merged));
    EXPECT_FALSE(
      graph.validate_advance(parents_end, frontier(cursor(0x20, 1))));
    EXPECT_TRUE(
      graph.validate_advance(frontier(cursor(0x20, 11)), grandchildren));
    EXPECT_FALSE(graph.validate_advance(merged, parents_end));
}

TEST(LineageModelTest, OneCursorAdvancesMonotonicallyWithoutTouchingSiblings) {
    const auto graph = history{}.make();
    const auto before = frontier(cursor(0xe0, 1), cursor(0x10, 2));
    const auto unchanged = graph.advance(before, cursor(0xe0, 1));
    ASSERT_TRUE(unchanged);
    EXPECT_EQ(*unchanged, before);
    EXPECT_TRUE(graph.validate_advance(before, before));
    const auto after = graph.advance(before, cursor(0xe0, 3));
    ASSERT_TRUE(after);
    EXPECT_EQ(*after, frontier(cursor(0xe0, 3), cursor(0x10, 2)));
    EXPECT_TRUE(graph.validate_advance(before, *after));
    EXPECT_FALSE(graph.validate_advance(*after, before));
    EXPECT_EQ(before, frontier(cursor(0xe0, 1), cursor(0x10, 2)));
    const auto backwards = graph.advance(before, cursor(0xe0, 0));
    ASSERT_FALSE(backwards);
    EXPECT_EQ(backwards.error(), errc::invalid_argument);
    const auto past_end = graph.advance(before, cursor(0xe0, 4));
    ASSERT_FALSE(past_end);
    EXPECT_EQ(past_end.error(), errc::out_of_range);
    const auto absent = graph.advance(before, cursor(0x20, 0));
    ASSERT_FALSE(absent);
    EXPECT_EQ(absent.error(), errc::not_found);
    EXPECT_FALSE(graph.advance(before, cursor(0x99, 0)));
    const auto two_updates = frontier(cursor(0xe0, 3), cursor(0x10, 7));
    ASSERT_TRUE(graph.validate_frontier(two_updates));
    EXPECT_FALSE(graph.validate_advance(before, two_updates));
}

TEST(
  LineageModelTest,
  FrontierValidationDistinguishesContextMembershipAndCoverage) {
    const auto graph = history{}.make();
    const std::array root{cursor(0x40, 0)};
    const auto wrong_topic
      = lineage_frontier::make(id<model::topic_id>(2), root).value();
    const auto wrong = graph.validate_frontier(wrong_topic);
    ASSERT_FALSE(wrong);
    EXPECT_EQ(wrong.error(), errc::wrong_context);
    const auto missing = graph.validate_frontier(frontier(cursor(0x99, 0)));
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error(), errc::not_found);
    EXPECT_FALSE(graph.validate_frontier(frontier(cursor(0xe0, 0))));
    EXPECT_FALSE(
      graph.validate_frontier(frontier(cursor(0x40, 0), cursor(0xe0, 0))));
    EXPECT_FALSE(
      graph.validate_frontier(frontier(cursor(0xe0, 4), cursor(0x10, 0))));
    EXPECT_FALSE(graph.advance(wrong_topic, cursor(0x40, 1)));
    EXPECT_FALSE(
      graph.validate_advance(wrong_topic, frontier(cursor(0x40, 1))));
    EXPECT_FALSE(
      graph.validate_advance(frontier(cursor(0x40, 1)), wrong_topic));
}

TEST(LineageModelTest, HolesUnknownEndsAndZeroSealsRetainTheirBoundaryMeaning) {
    const std::array unsealed{node(1, model::keyspace_interval::root())};
    const auto graph = lineage_model::make(topic(), unsealed, {}).value();
    const auto start = frontier(cursor(1, 0));
    const auto terminal = graph.advance(start, cursor(1, maximum));
    ASSERT_TRUE(terminal);
    EXPECT_TRUE(graph.validate_frontier(*terminal));
    EXPECT_TRUE(graph.validate_advance(start, *terminal));
    EXPECT_EQ(graph.advance(*terminal, cursor(1, maximum)).value(), *terminal);
    EXPECT_FALSE(graph.validate_advance(*terminal, start));
    const std::array sealed{
      node(1, model::keyspace_interval::root(), model::range_logical_end{0})};
    const auto empty = lineage_model::make(topic(), sealed, {}).value();
    EXPECT_TRUE(empty.validate_frontier(start));
    EXPECT_TRUE(empty.validate_advance(start, start));
    EXPECT_EQ(empty.advance(start, cursor(1, 0)).value(), start);
    EXPECT_FALSE(empty.advance(start, cursor(1, 1)));
    EXPECT_EQ(start.size(), 1);
}

TEST(
  LineageModelTest,
  IndependentBranchProgressCommutesWithoutNormalizingOtherCursors) {
    const auto graph = independent_history{}.make();
    const auto initial = frontier(cursor(2, 0), cursor(3, 0));
    const auto left_then_right = graph.advance(
      graph.advance(initial, cursor(2, 3)).value(), cursor(3, 2));
    const auto right_then_left = graph.advance(
      graph.advance(initial, cursor(3, 2)).value(), cursor(2, 3));
    ASSERT_TRUE(left_then_right);
    ASSERT_TRUE(right_then_left);
    EXPECT_EQ(*left_then_right, *right_then_left);
    const auto left_split = graph.split(*left_then_right, range(2)).value();
    EXPECT_TRUE(graph.validate_frontier(left_split));
    EXPECT_TRUE(graph.validate_advance(*left_then_right, left_split));
    EXPECT_EQ(left_split, frontier(cursor(3, 2), cursor(4, 0), cursor(5, 0)));
    const auto both_ends = frontier(cursor(2, 3), cursor(3, 7));
    const auto split_left_first
      = graph.split(graph.split(both_ends, range(2)).value(), range(3)).value();
    const auto split_right_first
      = graph.split(graph.split(both_ends, range(3)).value(), range(2)).value();
    EXPECT_EQ(split_left_first, split_right_first);
    EXPECT_FALSE(graph.validate_advance(both_ends, split_left_first));
    const auto ready = frontier(cursor(3, 2), cursor(4, 2), cursor(5, 5));
    const auto merged = graph.merge(ready, range(8)).value();
    EXPECT_TRUE(graph.validate_frontier(merged));
    EXPECT_TRUE(graph.validate_advance(ready, merged));
    EXPECT_EQ(merged, frontier(cursor(3, 2), cursor(8, 0)));
    EXPECT_FALSE(
      graph.validate_advance(ready, frontier(cursor(3, 3), cursor(8, 0))));
}

TEST(
  LineageModelTest,
  RepeatedSplitMergeUsesFreshIdentitiesAndResetsOnlyNewRanges) {
    const auto graph = cycling_history{model::range_logical_end{1}}.make();
    auto current = frontier(cursor(1, 0));
    for (std::uint8_t parent = 1; parent < 16;
         parent = static_cast<std::uint8_t>(parent + 3U)) {
        const auto left = static_cast<std::uint8_t>(parent + 1U);
        const auto right = static_cast<std::uint8_t>(parent + 2U);
        const auto merged = static_cast<std::uint8_t>(parent + 3U);
        const auto ended = graph.advance(current, cursor(parent, 1)).value();
        ASSERT_TRUE(graph.validate_advance(current, ended));
        const auto children = graph.split(ended, range(parent)).value();
        ASSERT_TRUE(graph.validate_advance(ended, children));
        EXPECT_EQ(children, frontier(cursor(left, 0), cursor(right, 0)));
        const auto first = graph.advance(children, cursor(left, 1)).value();
        EXPECT_TRUE(graph.validate_advance(children, first));
        EXPECT_FALSE(graph.merge(first, range(merged)));
        const auto second = graph.advance(first, cursor(right, 1)).value();
        EXPECT_TRUE(graph.validate_advance(first, second));
        const auto next = graph.merge(second, range(merged)).value();
        ASSERT_TRUE(graph.validate_advance(second, next));
        EXPECT_EQ(next, frontier(cursor(merged, 0)));
        EXPECT_FALSE(graph.validate_advance(current, next));
        EXPECT_FALSE(graph.merge(next, range(merged)));
        current = next;
    }
    EXPECT_EQ(current, frontier(cursor(16, 0)));
}

TEST(
  LineageModelTest,
  EveryMetadataEnumerationPreservesAncestryAndTransitionResults) {
    const split_merge_split_history source;
    const auto before = frontier(cursor(0x40, 5));
    const auto expected = frontier(cursor(0xe0, 0), cursor(0x10, 0));
    const auto invalid = frontier(cursor(0xe0, 1), cursor(0x90, 1));
    for (const bool permute_edges : {false, true}) {
        std::array<std::size_t, 6> order{0, 1, 2, 3, 4, 5};
        std::size_t cases = 0;
        do {
            auto ranges = source.ranges;
            auto edges = source.edges;
            for (std::size_t i = 0; i < order.size(); ++i) {
                if (permute_edges)
                    edges[i] = source.edges[order[i]];
                else
                    ranges[i] = source.ranges[order[i]];
            }
            const auto graph = lineage_model::make(topic(), ranges, edges);
            ASSERT_TRUE(graph);
            EXPECT_EQ(graph->root(), range(0x40));
            EXPECT_TRUE(graph->precedes(range(0xe0), range(0x90)).value());
            EXPECT_FALSE(graph->validate_frontier(invalid));
            EXPECT_EQ(graph->split(before, range(0x40)).value(), expected);
            EXPECT_TRUE(graph->validate_advance(before, expected));
            ++cases;
        } while (std::next_permutation(order.begin(), order.end()));
        EXPECT_EQ(cases, 720);
    }
}

TEST(
  LineageModelTest,
  EveryOpaqueIdentityAssignmentPreservesTheSameLogicalHistory) {
    const split_merge_split_history source;
    std::array<std::uint8_t, 6> markers{0x11, 0x40, 0x70, 0x80, 0xe0, 0xf0};
    std::size_t cases = 0;
    do {
        auto ranges = source.ranges;
        auto edges = source.edges;
        for (std::size_t i = 0; i < ranges.size(); ++i) {
            ranges[i].id = range(markers[i]);
            for (std::size_t e = 0; e < edges.size(); ++e) {
                if (source.edges[e].predecessor == source.ranges[i].id)
                    edges[e].predecessor = ranges[i].id;
                if (source.edges[e].successor == source.ranges[i].id)
                    edges[e].successor = ranges[i].id;
            }
        }
        const auto graph = lineage_model::make(topic(), ranges, edges);
        ASSERT_TRUE(graph);
        const auto root = frontier(cursor(markers[0], 5));
        const auto children = frontier(
          cursor(markers[1], 0), cursor(markers[2], 0));
        EXPECT_EQ(graph->root(), ranges[0].id);
        EXPECT_TRUE(graph->precedes(ranges[1].id, ranges[5].id).value());
        EXPECT_FALSE(graph->validate_frontier(
          frontier(cursor(markers[1], 1), cursor(markers[5], 1))));
        EXPECT_EQ(graph->split(root, ranges[0].id).value(), children);
        EXPECT_TRUE(graph->validate_advance(root, children));
        const auto completed = frontier(
          cursor(markers[1], 3), cursor(markers[2], 7));
        const auto merged = frontier(cursor(markers[3], 0));
        EXPECT_EQ(graph->merge(completed, ranges[3].id).value(), merged);
        EXPECT_TRUE(graph->validate_advance(completed, merged));
        ++cases;
    } while (std::next_permutation(markers.begin(), markers.end()));
    EXPECT_EQ(cases, 720);
}

} // namespace
