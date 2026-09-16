#pragma once

#include "src/base/result.h"
#include "src/base/units.h"
#include "src/model/checkpoint.h"
#include "src/model/identity.h"
#include "src/model/keyspace.h"
#include "src/model/position.h"

#include <array>
#include <cstddef>
#include <optional>
#include <span>

namespace kwaque::model::testing {

inline constexpr std::size_t maximum_lineage_ranges = 16;
inline constexpr std::size_t maximum_lineage_edges = 32;

// Supplied committed facts. A missing seal is unknown, whereas a known zero
// seal means the range has no logical slots. Identity never follows geometry.
struct lineage_range final {
    range_id id;
    topic_id topic;
    keyspace_interval interval{keyspace_interval::root()};
    std::optional<range_logical_end> sealed_end;

    bool operator==(const lineage_range&) const noexcept = default;
};

struct lineage_edge final {
    range_id predecessor;
    range_id successor;

    bool operator==(const lineage_edge&) const noexcept = default;
};

class lineage_model;

// Small owning position snapshot for the model. Construction checks topic,
// count and strict RangeID order only; it does not certify topology or prior
// processing. Scalar copies are independent; no storage or wire codec is used.
class lineage_frontier final {
public:
    [[nodiscard]] static result<lineage_frontier>
    make(topic_id topic, std::span<const range_cursor> sorted) noexcept;

    [[nodiscard]] topic_id topic() const noexcept { return topic_; }
    [[nodiscard]] std::size_t size() const noexcept { return size_; }
    [[nodiscard]] result<range_cursor> at(std::size_t index) const noexcept;
    [[nodiscard]] std::optional<range_cursor>
    find(range_id range) const noexcept;
    bool operator==(const lineage_frontier&) const noexcept = default;

private:
    friend class lineage_model;
    explicit lineage_frontier(topic_id topic) noexcept
      : topic_(topic) {}

    topic_id topic_;
    // Disengaged slots are private unused capacity, never nil cursor values.
    std::array<std::optional<range_cursor>, maximum_lineage_ranges> cursors_{};
    std::size_t size_{0};
};

// Immutable, self-contained test view of one complete committed history.
// The caps include historical ranges/edges. Construction checks one full-space
// root, reachability, cycles, known seals and reciprocal split/merge geometry.
// All work is synchronous and bounded: at most 16^3 closure steps and fixed
// arrays below 8 KiB. No allocation, reactor, external lifetime, cancellation
// source or mutable completion ledger is involved.
// Borrowed fact views require a stable, unchanged owner; copies own their
// facts.
class lineage_model final {
public:
    [[nodiscard]] static result<lineage_model> make(
      topic_id topic,
      std::span<const lineage_range> ranges,
      std::span<const lineage_edge> edges) noexcept;

    [[nodiscard]] topic_id topic() const noexcept { return topic_; }
    [[nodiscard]] range_id root() const noexcept { return ranges_[root_].id; }
    [[nodiscard]] std::span<const lineage_range> ranges() const& noexcept {
        return std::span{ranges_}.first(range_count_);
    }
    std::span<const lineage_range> ranges() const&& = delete;
    [[nodiscard]] std::span<const lineage_edge> edges() const& noexcept {
        return std::span{edges_}.first(edge_count_);
    }
    std::span<const lineage_edge> edges() const&& = delete;
    [[nodiscard]] result<bool>
    precedes(range_id ancestor, range_id descendant) const noexcept;

    // Topology validity is separate from structural construction and from
    // advancement. Require topic membership, full coverage, a transitive
    // antichain and positions within known seals. Unknown unsealed ends do
    // not invent a high-watermark; logical holes remain valid positions.
    [[nodiscard]] result<void>
    validate_frontier(const lineage_frontier& frontier) const noexcept;

    // Advance one present range monotonically, leaving every sibling intact.
    // An equal position is a no-op. This models a supplied read boundary; it
    // does not prove that records were processed or authorize consumption.
    [[nodiscard]] result<lineage_frontier> advance(
      const lineage_frontier& before, range_cursor position) const noexcept;

    // Accept equality or exactly one elementary action: advance one existing
    // cursor, perform one enabled split, or perform one enabled merge. Both
    // snapshots must be topology-valid. This is not a general progress order:
    // combined updates, traversal over several edges and topology replacement
    // with already-advanced children reject. Existing transition gates decide
    // readiness, and equality with their full result protects siblings.
    [[nodiscard]] result<void> validate_advance(
      const lineage_frontier& before,
      const lineage_frontier& after) const noexcept;

    // Check the supplied frontier's topic, membership, coverage, transitive
    // ancestry and known-end bounds before applying the selected transition.
    // Split replaces one parent exactly at its seal with both children at zero.
    // Merge names the successor and requires both distinct parents at their
    // own seals. A partial/absent parent never counts as complete. Sealed
    // terminal leaves remain in the snapshot.
    //
    // After frontier admission, an incomplete predecessor returns unavailable;
    // an absent selected parent returns not_found. Invalid inputs reject.
    // Replacement uses private fixed storage; every result leaves before
    // untouched. The supplied ceiling can only narrow the 16-cursor cap.
    [[nodiscard]] result<lineage_frontier> split(
      const lineage_frontier& before,
      range_id parent,
      item_count maximum_cursors = item_count{
        maximum_lineage_ranges}) const noexcept;
    [[nodiscard]] result<lineage_frontier> merge(
      const lineage_frontier& before,
      range_id successor,
      item_count maximum_cursors = item_count{
        maximum_lineage_ranges}) const noexcept;

private:
    struct links final {
        std::array<std::size_t, 2> predecessors{};
        std::array<std::size_t, 2> successors{};
        std::size_t predecessor_count{0};
        std::size_t successor_count{0};
    };

    explicit lineage_model(topic_id topic) noexcept
      : topic_(topic) {}
    [[nodiscard]] std::optional<std::size_t>
    index_of(range_id id) const noexcept;
    [[nodiscard]] result<lineage_frontier> replace(
      const lineage_frontier& before,
      std::span<const std::size_t> parents,
      std::span<const std::size_t> children,
      item_count maximum_cursors) const noexcept;

    topic_id topic_;
    std::array<lineage_range, maximum_lineage_ranges> ranges_{};
    std::array<lineage_edge, maximum_lineage_edges> edges_{};
    std::array<links, maximum_lineage_ranges> links_{};
    std::array<std::array<bool, maximum_lineage_ranges>, maximum_lineage_ranges>
      reachable_{};
    std::size_t range_count_{0};
    std::size_t edge_count_{0};
    std::size_t root_{0};
};

static_assert(sizeof(lineage_model) + sizeof(lineage_frontier) < 8U * 1024U);

} // namespace kwaque::model::testing
