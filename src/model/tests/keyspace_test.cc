#include "src/base/error.h"
#include "src/model/keyspace.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bitset>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

using kwaque::model::keyspace_boundary;
using kwaque::model::keyspace_interval;
using kwaque::model::max_unordered_keyspace_intervals;
using kwaque::model::ordered_keyspace_coverage;
using kwaque::model::validate_keyspace_coverage;

constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
constexpr auto half = std::uint64_t{1} << 63U;
constexpr auto quarter = std::uint64_t{1} << 62U;

template<typename Value>
concept ordering_expression = requires(const Value& lhs, const Value& rhs) {
    lhs < rhs;
} || requires(const Value& lhs, const Value& rhs) { lhs <=> rhs; };

template<typename Value>
concept size_expression = requires(const Value& value) { value.size(); };

template<typename Point>
concept point_containment_expression = requires(
  const keyspace_interval& interval, Point point) {
    interval.contains(std::forward<Point>(point));
};

struct implicitly_converted_point {
    operator std::uint64_t() const noexcept;
};

static_assert(sizeof(keyspace_interval) == 16);
static_assert(std::is_standard_layout_v<keyspace_interval>);
static_assert(std::is_trivially_copyable_v<keyspace_interval>);
static_assert(std::is_nothrow_copy_constructible_v<keyspace_interval>);
static_assert(std::is_nothrow_move_constructible_v<keyspace_interval>);
static_assert(std::is_nothrow_destructible_v<keyspace_interval>);
static_assert(!std::default_initializable<keyspace_interval>);
static_assert(
  !std::constructible_from<keyspace_interval, std::uint64_t, std::uint8_t>);
static_assert(!ordering_expression<keyspace_interval>);
static_assert(!size_expression<keyspace_interval>);
static_assert(!std::default_initializable<keyspace_boundary>);
static_assert(std::is_trivially_copyable_v<keyspace_boundary>);
static_assert(!std::constructible_from<keyspace_boundary, std::uint64_t, bool>);
static_assert(!std::convertible_to<keyspace_boundary, std::uint64_t>);
static_assert(point_containment_expression<std::uint64_t>);
static_assert(point_containment_expression<std::uint64_t&>);
static_assert(point_containment_expression<const std::uint64_t&>);
static_assert(point_containment_expression<keyspace_interval>);
static_assert(!point_containment_expression<int>);
static_assert(!point_containment_expression<std::int64_t>);
static_assert(!point_containment_expression<std::uint32_t>);
static_assert(!point_containment_expression<bool>);
static_assert(!point_containment_expression<double>);
static_assert(!point_containment_expression<implicitly_converted_point>);
static_assert(!point_containment_expression<keyspace_boundary>);
static_assert(std::same_as<
              decltype(std::declval<keyspace_interval>().begin()),
              keyspace_boundary>);
static_assert(std::same_as<
              decltype(std::declval<const keyspace_interval>().end()),
              keyspace_boundary>);
static_assert(std::same_as<
              decltype(std::declval<const keyspace_interval&>().depth()),
              std::uint8_t>);
static_assert(std::same_as<
              decltype(std::declval<const keyspace_boundary&>().value()),
              std::optional<std::uint64_t>>);
static_assert(keyspace_interval::root().prefix() == 0U);
static_assert(keyspace_interval::root().depth() == 0U);
static_assert(keyspace_interval::root().contains(maximum));
static_assert(keyspace_interval::root().contains(std::uint64_t{0}));
static_assert(noexcept(keyspace_interval::root().contains(std::uint64_t{0})));
static_assert(keyspace_interval::root().end() == keyspace_boundary::full_end());
static_assert(
  keyspace_boundary::ordinary(maximum) < keyspace_boundary::full_end());
static_assert(!keyspace_boundary::full_end().value().has_value());
static_assert(noexcept(keyspace_interval::make(0, 0)));
static_assert(
  noexcept(validate_keyspace_coverage({}, keyspace_interval::root())));
static_assert(max_unordered_keyspace_intervals == 64U);

keyspace_interval interval(std::uint64_t prefix, std::uint64_t depth) {
    return keyspace_interval::make(prefix, depth).value();
}

TEST(KeyspaceTest, FullEndpointIsDistinctFromEveryOrdinaryBoundary) {
    const auto full = keyspace_boundary::full_end();
    EXPECT_TRUE(full.is_full());
    EXPECT_FALSE(full.value().has_value());
    EXPECT_EQ(full, keyspace_boundary::full_end());
    for (const auto point :
         {std::uint64_t{0}, std::uint64_t{1}, half, maximum}) {
        const auto ordinary = keyspace_boundary::ordinary(point);
        ASSERT_TRUE(ordinary.value().has_value());
        EXPECT_EQ(*ordinary.value(), point);
        EXPECT_FALSE(ordinary.is_full());
        EXPECT_LT(ordinary, full);
        EXPECT_GT(full, ordinary);
        EXPECT_NE(ordinary, full);
    }
    EXPECT_LT(
      keyspace_boundary::ordinary(half - 1U),
      keyspace_boundary::ordinary(half));
    EXPECT_LT(keyspace_boundary::ordinary(1), keyspace_boundary::ordinary(256));
}

TEST(KeyspaceTest, ValidatesDepthBeforeNarrowingAndRejectsUnusedSuffixBits) {
    for (const auto depth :
         {std::uint64_t{65}, std::uint64_t{255}, std::uint64_t{256}, maximum}) {
        const auto invalid = keyspace_interval::make(0, depth);
        ASSERT_FALSE(invalid.has_value()) << depth;
        EXPECT_EQ(invalid.error(), kwaque::errc::invalid_argument);
    }
    for (const auto& [prefix, depth] : std::array{
           std::pair{std::uint64_t{1}, std::uint64_t{0}},
           std::pair{maximum, std::uint64_t{0}},
           std::pair{half + 1U, std::uint64_t{1}},
           std::pair{quarter + 1U, std::uint64_t{2}},
           std::pair{std::uint64_t{0x8000000000000001}, std::uint64_t{8}},
           std::pair{std::uint64_t{0xabc0000000000001}, std::uint64_t{12}},
           std::pair{std::uint64_t{0x1234000000000001}, std::uint64_t{16}},
           std::pair{std::uint64_t{0x1234560000000001}, std::uint64_t{23}},
           std::pair{std::uint64_t{0x1234560000000001}, std::uint64_t{24}},
           std::pair{std::uint64_t{0x1234567800000001}, std::uint64_t{32}},
           std::pair{maximum, std::uint64_t{63}}}) {
        const auto invalid = keyspace_interval::make(prefix, depth);
        ASSERT_FALSE(invalid.has_value()) << prefix << ':' << depth;
        EXPECT_EQ(invalid.error(), kwaque::errc::invalid_argument);
    }
    const auto explicit_root = keyspace_interval::make(0, 0);
    ASSERT_TRUE(explicit_root.has_value());
    EXPECT_EQ(*explicit_root, keyspace_interval::root());
}

TEST(KeyspaceTest, LiteralIntervalsPreserveRootHalvesAndTerminalSingletons) {
    struct interval_case {
        std::uint64_t prefix;
        std::uint64_t depth;
        keyspace_boundary end;
    };
    const std::array cases{
      interval_case{0, 0, keyspace_boundary::full_end()},
      interval_case{0, 1, keyspace_boundary::ordinary(half)},
      interval_case{half, 1, keyspace_boundary::full_end()},
      interval_case{quarter, 2, keyspace_boundary::ordinary(half)},
      interval_case{
        0x8000000000000000, 8, keyspace_boundary::ordinary(0x8100000000000000)},
      interval_case{
        0xabc0000000000000,
        12,
        keyspace_boundary::ordinary(0xabd0000000000000)},
      interval_case{
        0x1234000000000000,
        16,
        keyspace_boundary::ordinary(0x1235000000000000)},
      interval_case{
        0x1234560000000000,
        23,
        keyspace_boundary::ordinary(0x1234580000000000)},
      interval_case{
        0x1234560000000000,
        24,
        keyspace_boundary::ordinary(0x1234570000000000)},
      interval_case{
        0x1234567800000000,
        32,
        keyspace_boundary::ordinary(0x1234567900000000)},
      interval_case{0, 63, keyspace_boundary::ordinary(2)},
      interval_case{maximum - 1U, 63, keyspace_boundary::full_end()},
      interval_case{0, 64, keyspace_boundary::ordinary(1)},
      interval_case{maximum - 1U, 64, keyspace_boundary::ordinary(maximum)},
      interval_case{maximum, 64, keyspace_boundary::full_end()}};
    for (const auto& input : cases) {
        const auto value = keyspace_interval::make(input.prefix, input.depth);
        ASSERT_TRUE(value.has_value()) << input.prefix << ':' << input.depth;
        EXPECT_EQ(value->prefix(), input.prefix);
        EXPECT_EQ(value->depth(), input.depth);
        EXPECT_EQ(value->begin(), keyspace_boundary::ordinary(input.prefix));
        EXPECT_EQ(value->end(), input.end);
        EXPECT_TRUE(value->contains(input.prefix));
        EXPECT_TRUE(value->contains(*value));
        if (const auto ordinary_end = input.end.value()) {
            EXPECT_TRUE(value->contains(*ordinary_end - 1U));
            EXPECT_FALSE(value->contains(*ordinary_end));
        } else {
            EXPECT_TRUE(value->contains(maximum));
        }
        if (input.prefix != 0) {
            EXPECT_FALSE(value->contains(input.prefix - 1U));
        }
    }
}

TEST(KeyspaceTest, AdjacencyDoesNotImplyBuddyEligibility) {
    const auto left = interval(0, 1);
    const auto right = interval(half, 1);
    EXPECT_TRUE(left.is_buddy_of(right));
    EXPECT_TRUE(right.is_buddy_of(left));
    EXPECT_TRUE(left.adjacent_to(right));
    EXPECT_FALSE(left.overlaps(right));
    EXPECT_FALSE(left.is_buddy_of(left));
    EXPECT_FALSE(
      keyspace_interval::root().is_buddy_of(keyspace_interval::root()));

    const auto right_quarter = interval(half, 2);
    EXPECT_TRUE(left.adjacent_to(right_quarter));
    EXPECT_TRUE(right_quarter.adjacent_to(left));
    EXPECT_FALSE(left.is_buddy_of(right_quarter));
    EXPECT_FALSE(right_quarter.is_buddy_of(left));

    const auto one = interval(1, 64);
    const auto two = interval(2, 64);
    EXPECT_TRUE(one.adjacent_to(two));
    EXPECT_FALSE(one.is_buddy_of(two));
    EXPECT_FALSE(two.is_buddy_of(one));
    EXPECT_TRUE(interval(maximum - 1U, 64).is_buddy_of(interval(maximum, 64)));
    EXPECT_FALSE(interval(maximum, 64).adjacent_to(interval(0, 64)));
}

TEST(
  KeyspaceCoverageTest,
  EarlyFinishIsNonConsumingAndExtraInputPoisonsCompletion) {
    const auto left = interval(0, 1);
    const auto right = interval(half, 1);
    ordered_keyspace_coverage coverage{keyspace_interval::root()};
    const auto empty = coverage.finish();
    ASSERT_FALSE(empty.has_value());
    EXPECT_EQ(empty.error(), kwaque::errc::invalid_argument);
    EXPECT_FALSE(coverage.finish().has_value());
    ASSERT_TRUE(coverage.append(left).has_value());
    EXPECT_FALSE(coverage.finish().has_value());
    ASSERT_TRUE(coverage.append(right).has_value());
    EXPECT_TRUE(coverage.finish().has_value());
    EXPECT_TRUE(coverage.finish().has_value());
    const auto extra = coverage.append(left);
    ASSERT_FALSE(extra.has_value());
    EXPECT_EQ(extra.error(), kwaque::errc::invalid_argument);
    EXPECT_FALSE(coverage.finish().has_value());
}

TEST(KeyspaceCoverageTest, GapOverlapDuplicateAndDescendingFailuresStayFailed) {
    const auto left = interval(0, 1);
    const auto right = interval(half, 1);
    for (const auto bad : std::array{
           left,
           keyspace_interval::root(),
           interval(3U * quarter, 2),
           interval(0, 2)}) {
        ordered_keyspace_coverage coverage{keyspace_interval::root()};
        ASSERT_TRUE(coverage.append(left).has_value());
        const auto rejected = coverage.append(bad);
        ASSERT_FALSE(rejected.has_value());
        EXPECT_EQ(rejected.error(), kwaque::errc::invalid_argument);
        EXPECT_FALSE(coverage.finish().has_value());
        EXPECT_FALSE(coverage.append(right).has_value());
        EXPECT_FALSE(coverage.finish().has_value());
    }
    ordered_keyspace_coverage missing_first{keyspace_interval::root()};
    EXPECT_FALSE(missing_first.append(right).has_value());
    EXPECT_FALSE(missing_first.append(keyspace_interval::root()).has_value());
    EXPECT_FALSE(missing_first.finish().has_value());
}

TEST(
  KeyspaceCoverageTest, ChecksTheSuppliedTargetAndSupportsTerminalSubranges) {
    const auto left = interval(0, 1);
    ordered_keyspace_coverage outside{left};
    const auto rejected = outside.append(keyspace_interval::root());
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), kwaque::errc::invalid_argument);
    EXPECT_FALSE(outside.append(left).has_value());
    EXPECT_FALSE(outside.finish().has_value());

    ordered_keyspace_coverage right{interval(half, 1)};
    ASSERT_TRUE(right.append(interval(half, 2)).has_value());
    EXPECT_FALSE(right.finish().has_value());
    ASSERT_TRUE(right.append(interval(3U * quarter, 2)).has_value());
    EXPECT_TRUE(right.finish().has_value());

    const auto last_point = interval(maximum, 64);
    ordered_keyspace_coverage terminal{last_point};
    ASSERT_TRUE(terminal.append(last_point).has_value());
    EXPECT_TRUE(terminal.finish().has_value());
}

TEST(
  KeyspaceCoverageTest,
  UnorderedPermutationsPreserveInputAndRejectInvalidCoverage) {
    std::array pieces{
      interval(0, 2),
      interval(quarter, 2),
      interval(half, 2),
      interval(3U * quarter, 2)};
    std::size_t permutations = 0;
    do {
        const auto saved = pieces;
        EXPECT_TRUE(
          validate_keyspace_coverage(pieces, keyspace_interval::root())
            .has_value());
        EXPECT_EQ(pieces, saved);
        ++permutations;
    } while (std::next_permutation(
      pieces.begin(), pieces.end(), [](const auto& left, const auto& right) {
          return left.prefix() < right.prefix();
      }));
    EXPECT_EQ(permutations, 24U);

    const std::array duplicate{
      interval(half, 1), interval(0, 1), interval(0, 1)};
    const auto saved = duplicate;
    const auto rejected = validate_keyspace_coverage(
      duplicate, keyspace_interval::root());
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), kwaque::errc::invalid_argument);
    EXPECT_EQ(duplicate, saved);
    const std::array whole{keyspace_interval::root()};
    EXPECT_TRUE(
      validate_keyspace_coverage(whole, keyspace_interval::root()).has_value());
    const std::array terminal{interval(maximum, 64)};
    EXPECT_TRUE(
      validate_keyspace_coverage(terminal, terminal.front()).has_value());
    const auto empty = validate_keyspace_coverage(
      {}, keyspace_interval::root());
    ASSERT_FALSE(empty.has_value());
    EXPECT_EQ(empty.error(), kwaque::errc::invalid_argument);
}

TEST(
  KeyspaceCoverageTest, MixedDepthAndSharedStartPermutationsAgreeAtFullWidth) {
    struct coverage_case {
        std::array<keyspace_interval, 3> pieces;
        keyspace_interval target;
        bool accepted;
    };
    const auto root = keyspace_interval::root();
    const std::array cases{
      coverage_case{
        {interval(0, 1), interval(half, 2), interval(3U * quarter, 2)},
        root,
        true},
      coverage_case{
        {interval(0, 2), interval(quarter, 2), interval(half, 1)}, root, true},
      coverage_case{
        {interval(maximum - 3U, 63),
         interval(maximum - 1U, 64),
         interval(maximum, 64)},
        interval(maximum - 3U, 62),
        true},
      coverage_case{
        {interval(0, 1), interval(0, 2), interval(half, 1)}, root, false},
      coverage_case{
        {interval(0, 2), interval(half, 2), interval(3U * quarter, 2)},
        root,
        false}};
    const auto before = [](const auto& left, const auto& right) {
        if (left.prefix() != right.prefix()) {
            return left.prefix() < right.prefix();
        }
        return left.depth() < right.depth();
    };
    for (const auto& input : cases) {
        auto pieces = input.pieces;
        std::ranges::sort(pieces, before);
        std::size_t permutations = 0;
        do {
            const auto saved = pieces;
            const auto result = validate_keyspace_coverage(
              pieces, input.target);
            ASSERT_EQ(result.has_value(), input.accepted)
              << input.target.prefix() << ':'
              << static_cast<unsigned>(input.target.depth());
            if (!result) {
                EXPECT_EQ(result.error(), kwaque::errc::invalid_argument);
            }
            EXPECT_EQ(pieces, saved);
            ++permutations;
        } while (std::next_permutation(pieces.begin(), pieces.end(), before));
        EXPECT_EQ(permutations, 6U);
    }
}

TEST(KeyspaceCoverageTest, Accepts64IntervalsAndRejects65BeforeGeometry) {
    std::vector<keyspace_interval> pieces;
    pieces.reserve(64);
    constexpr auto width = std::uint64_t{1} << 58U;
    for (std::uint64_t index = 0; index < 64; ++index) {
        pieces.push_back(interval(index * width, 6));
    }
    std::ranges::reverse(pieces);
    const auto saved = pieces;
    EXPECT_TRUE(validate_keyspace_coverage(pieces, keyspace_interval::root())
                  .has_value());
    EXPECT_EQ(pieces, saved);

    const std::vector<keyspace_interval> oversized(
      65, keyspace_interval::root());
    const auto oversized_saved = oversized;
    const auto rejected = validate_keyspace_coverage(
      oversized, keyspace_interval::root());
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), kwaque::errc::resource_exhausted);
    EXPECT_EQ(oversized, oversized_saved);
}

constexpr std::size_t oracle_cells = 16;
constexpr auto oracle_cell_width = std::uint64_t{1} << 60U;

struct cell_interval {
    keyspace_interval value;
    std::size_t first;
    std::size_t count;
    std::bitset<oracle_cells> cells;
};

std::vector<cell_interval> cell_intervals() {
    std::vector<cell_interval> values;
    values.reserve(31);
    std::uint64_t depth = 0;
    for (std::size_t count = oracle_cells; count != 0; count /= 2, ++depth) {
        for (std::size_t first = 0; first < oracle_cells; first += count) {
            std::bitset<oracle_cells> cells;
            for (std::size_t cell = first; cell < first + count; ++cell) {
                cells.set(cell);
            }
            values.push_back(
              {interval(first * oracle_cell_width, depth),
               first,
               count,
               cells});
        }
    }
    return values;
}

TEST(KeyspaceOracleTest, GeometryMatchesIndependent16CellMembership) {
    const auto values = cell_intervals();
    ASSERT_EQ(values.size(), 31U);
    for (const auto& left : values) {
        for (std::size_t cell = 0; cell < oracle_cells; ++cell) {
            const auto first = cell * oracle_cell_width;
            EXPECT_EQ(left.value.contains(first), left.cells.test(cell));
            EXPECT_EQ(
              left.value.contains(first + (oracle_cell_width - 1U)),
              left.cells.test(cell));
        }
        for (const auto& right : values) {
            SCOPED_TRACE(
              ::testing::Message() << left.first << ':' << left.count << '/'
                                   << right.first << ':' << right.count);
            const auto intersection = left.cells & right.cells;
            const bool adjacent = left.first + left.count == right.first
                                  || right.first + right.count == left.first;
            const bool buddies = adjacent && left.count == right.count
                                 && left.first / (2U * left.count)
                                      == right.first / (2U * right.count);
            EXPECT_EQ(
              left.value.contains(right.value), intersection == right.cells);
            EXPECT_EQ(left.value.overlaps(right.value), intersection.any());
            EXPECT_EQ(left.value.adjacent_to(right.value), adjacent);
            EXPECT_EQ(left.value.is_buddy_of(right.value), buddies);
        }
    }
}

TEST(
  KeyspaceOracleTest, TwoIntervalCoverageMatchesIndependentCellMultiplicity) {
    const auto values = cell_intervals();
    for (const auto target_index :
         {std::size_t{0}, std::size_t{1}, std::size_t{2}}) {
        const auto& target = values[target_index];
        for (const auto& left : values) {
            for (const auto& right : values) {
                bool exact = true;
                for (std::size_t cell = 0; cell < oracle_cells; ++cell) {
                    const auto multiplicity
                      = static_cast<unsigned>(left.cells.test(cell))
                        + static_cast<unsigned>(right.cells.test(cell));
                    exact = exact
                            && multiplicity
                                 == static_cast<unsigned>(
                                   target.cells.test(cell));
                }
                const std::array input{left.value, right.value};
                SCOPED_TRACE(
                  ::testing::Message()
                  << target.first << ':' << target.count << '/' << left.first
                  << ':' << left.count << '/' << right.first << ':'
                  << right.count);
                EXPECT_EQ(
                  validate_keyspace_coverage(input, target.value).has_value(),
                  exact);
            }
        }
    }
}

TEST(KeyspaceOracleTest, MixedDepthMultisetsMatchIndependent16CellCoverage) {
    const auto values = cell_intervals();
    const std::array selected_ranges{
      std::pair{0U, 8U},
      std::pair{8U, 8U},
      std::pair{0U, 4U},
      std::pair{4U, 4U},
      std::pair{8U, 4U},
      std::pair{12U, 4U},
      std::pair{8U, 2U},
      std::pair{10U, 2U},
      std::pair{12U, 2U},
      std::pair{14U, 2U},
      std::pair{10U, 1U},
      std::pair{11U, 1U},
      std::pair{14U, 1U},
      std::pair{15U, 1U}};
    std::array<const cell_interval*, 14> selected{};
    for (std::size_t index = 0; index < selected_ranges.size(); ++index) {
        const auto [first, count] = selected_ranges[index];
        const auto found = std::ranges::find_if(
          values, [first, count](const auto& value) {
              return value.first == first && value.count == count;
          });
        ASSERT_TRUE(found != values.end()) << first << ':' << count;
        selected[index] = &*found;
    }

    struct target_case {
        keyspace_interval value;
        std::bitset<oracle_cells> cells;
    };
    const std::array targets{
      target_case{keyspace_interval::root(), std::bitset<oracle_cells>{0xffff}},
      target_case{interval(half, 2), std::bitset<oracle_cells>{0x0f00}},
      target_case{
        interval(3U * quarter, 2), std::bitset<oracle_cells>{0xf000}}};

    const auto verify = [&](
                          const std::array<std::size_t, 4>& indices,
                          std::size_t count) -> ::testing::AssertionResult {
        const std::array pieces{
          selected[indices[0]]->value,
          selected[indices[1]]->value,
          selected[indices[2]]->value,
          selected[indices[3]]->value};
        const auto input = std::span<const keyspace_interval>{pieces}.first(
          count);
        std::bitset<oracle_cells> seen;
        std::bitset<oracle_cells> repeated;
        for (std::size_t index = 0; index < count; ++index) {
            const auto& cells = selected[indices[index]]->cells;
            repeated |= seen & cells;
            seen |= cells;
        }
        for (const auto& target : targets) {
            const bool exact = repeated.none() && seen == target.cells;
            const auto result = validate_keyspace_coverage(input, target.value);
            if (result.has_value() != exact) {
                return ::testing::AssertionFailure()
                       << "piece count " << count << ", indices " << indices[0]
                       << ',' << indices[1] << ',' << indices[2] << ','
                       << indices[3] << ", target cells " << target.cells;
            }
            if (!result && result.error() != kwaque::errc::invalid_argument) {
                return ::testing::AssertionFailure()
                       << "unexpected coverage error " << result.error();
            }
        }
        return ::testing::AssertionSuccess();
    };

    std::size_t checked = 0;
    for (std::size_t first = 0; first < selected.size(); ++first) {
        for (std::size_t second = first; second < selected.size(); ++second) {
            for (std::size_t third = second; third < selected.size(); ++third) {
                ASSERT_TRUE(verify({first, second, third, third}, 3));
                ++checked;
                for (std::size_t fourth = third; fourth < selected.size();
                     ++fourth) {
                    ASSERT_TRUE(verify({first, second, third, fourth}, 4));
                    ++checked;
                }
            }
        }
    }
    EXPECT_EQ(checked, 2940U);
}

} // namespace
