#pragma once

#include "src/model/tests/lineage_model.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>

namespace kwaque::model::testing::fixtures {

constexpr std::uint64_t half = std::uint64_t{1} << 63U;
constexpr std::uint64_t quarter = std::uint64_t{1} << 62U;
constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();

template<typename Id>
Id id(std::uint8_t marker) {
    std::array<std::uint8_t, 16> raw{};
    raw.back() = marker;
    return Id::make(raw).value();
}
inline topic_id topic() { return id<topic_id>(1); }
inline range_id range(std::uint8_t marker) { return id<range_id>(marker); }
inline keyspace_interval interval(std::uint64_t prefix, std::uint64_t depth) {
    return keyspace_interval::make(prefix, depth).value();
}
inline lineage_range node(
  std::uint8_t marker,
  keyspace_interval geometry,
  std::optional<range_logical_end> seal = std::nullopt) {
    return {range(marker), topic(), geometry, seal};
}
inline lineage_edge edge(std::uint8_t parent, std::uint8_t child) {
    return {range(parent), range(child)};
}
inline range_cursor cursor(std::uint8_t marker, std::uint64_t next) {
    return range_cursor::make(range(marker), range_logical_end{next}).value();
}
template<typename... Cursors>
lineage_frontier frontier(Cursors... cursors) {
    std::array sorted{cursors...};
    std::ranges::sort(sorted, range_cursor_less{});
    return lineage_frontier::make(topic(), sorted).value();
}

// Deliberately different identity and keyspace orders. The fourth range's
// geometry equals the historical root, but it has a fresh identity.
struct history final {
    std::array<lineage_range, 4> ranges{
      node(0x40, keyspace_interval::root(), range_logical_end{5}),
      node(0xe0, interval(0, 1), range_logical_end{3}),
      node(0x10, interval(half, 1), range_logical_end{7}),
      node(0x20, keyspace_interval::root())};
    std::array<lineage_edge, 4> edges{
      edge(0x40, 0xe0), edge(0x40, 0x10), edge(0xe0, 0x20), edge(0x10, 0x20)};
    lineage_model make() const {
        return lineage_model::make(topic(), ranges, edges).value();
    }
};

// R -> (A,B) -> C -> (D,E). Old/new halves can cover the keyspace
// geometrically while remaining causally related through C.
struct split_merge_split_history final {
    std::array<lineage_range, 6> ranges{
      node(0x40, keyspace_interval::root(), range_logical_end{5}),
      node(0xe0, interval(0, 1), range_logical_end{3}),
      node(0x10, interval(half, 1), range_logical_end{7}),
      node(0x20, keyspace_interval::root(), range_logical_end{11}),
      node(0x70, interval(0, 1)),
      node(0x90, interval(half, 1))};
    std::array<lineage_edge, 6> edges{
      edge(0x40, 0xe0),
      edge(0x40, 0x10),
      edge(0xe0, 0x20),
      edge(0x10, 0x20),
      edge(0x20, 0x70),
      edge(0x20, 0x90)};
    lineage_model make() const {
        return lineage_model::make(topic(), ranges, edges).value();
    }
};

// The left branch splits and merges independently of the right branch.
struct independent_history final {
    std::array<lineage_range, 8> ranges{
      node(1, keyspace_interval::root(), range_logical_end{4}),
      node(2, interval(0, 1), range_logical_end{3}),
      node(3, interval(half, 1), range_logical_end{7}),
      node(4, interval(0, 2), range_logical_end{2}),
      node(5, interval(quarter, 2), range_logical_end{5}),
      node(6, interval(half, 2)),
      node(7, interval(half + quarter, 2)),
      node(8, interval(0, 1))};
    std::array<lineage_edge, 8> edges{
      edge(1, 2),
      edge(1, 3),
      edge(2, 4),
      edge(2, 5),
      edge(3, 6),
      edge(3, 7),
      edge(4, 8),
      edge(5, 8)};
    lineage_model make() const {
        return lineage_model::make(topic(), ranges, edges).value();
    }
};

struct cycling_history final {
    std::array<lineage_range, 16> ranges{};
    std::array<lineage_edge, 20> edges{};

    explicit cycling_history(range_logical_end seal = range_logical_end{}) {
        ranges[0] = node(1, keyspace_interval::root(), seal);
        for (std::size_t step = 0; step < 5; ++step) {
            const auto parent = static_cast<std::uint8_t>(1U + 3U * step);
            const auto left = static_cast<std::uint8_t>(parent + 1U);
            const auto right = static_cast<std::uint8_t>(parent + 2U);
            const auto merged = static_cast<std::uint8_t>(parent + 3U);
            ranges[left - 1U] = node(left, interval(0, 1), seal);
            ranges[right - 1U] = node(right, interval(half, 1), seal);
            ranges[merged - 1U] = node(merged, keyspace_interval::root(), seal);
            edges[4U * step] = edge(parent, left);
            edges[4U * step + 1U] = edge(parent, right);
            edges[4U * step + 2U] = edge(left, merged);
            edges[4U * step + 3U] = edge(right, merged);
        }
    }
    lineage_model make() const {
        return lineage_model::make(topic(), ranges, edges).value();
    }
};

} // namespace kwaque::model::testing::fixtures
