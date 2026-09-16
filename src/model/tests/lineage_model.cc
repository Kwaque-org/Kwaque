#include "src/model/tests/lineage_model.h"

#include "src/base/error.h"

#include <algorithm>
#include <array>
#include <cstdint>

namespace kwaque::model::testing {

result<lineage_frontier> lineage_frontier::make(
  topic_id topic, std::span<const range_cursor> sorted) noexcept {
    if (topic.is_nil() || sorted.empty())
        return failure(errc::invalid_argument);
    if (sorted.size() > maximum_lineage_ranges)
        return failure(errc::resource_exhausted);
    lineage_frontier value{topic};
    for (std::size_t i = 0; i < sorted.size(); ++i) {
        if (i != 0 && !range_cursor_less{}(sorted[i - 1], sorted[i]))
            return failure(errc::invalid_argument);
        value.cursors_[i] = sorted[i];
    }
    value.size_ = sorted.size();
    return value;
}

result<range_cursor> lineage_frontier::at(std::size_t index) const noexcept {
    if (index >= size_) return failure(errc::out_of_range);
    return *cursors_[index];
}

std::optional<range_cursor>
lineage_frontier::find(range_id range) const noexcept {
    for (std::size_t i = 0; i < size_; ++i)
        if (cursors_[i]->range() == range) return cursors_[i];
    return std::nullopt;
}

std::optional<std::size_t> lineage_model::index_of(range_id id) const noexcept {
    for (std::size_t i = 0; i < range_count_; ++i)
        if (ranges_[i].id == id) return i;
    return std::nullopt;
}

result<lineage_model> lineage_model::make(
  topic_id topic,
  std::span<const lineage_range> ranges,
  std::span<const lineage_edge> edges) noexcept {
    if (topic.is_nil() || ranges.empty())
        return failure(errc::invalid_argument);
    if (
      ranges.size() > maximum_lineage_ranges
      || edges.size() > maximum_lineage_edges)
        return failure(errc::resource_exhausted);
    lineage_model value{topic};
    for (const auto& range : ranges) {
        if (
          range.id.is_nil() || range.topic.is_nil() || value.index_of(range.id))
            return failure(errc::invalid_argument);
        if (range.topic != topic) return failure(errc::wrong_context);
        value.ranges_[value.range_count_++] = range;
    }
    // Build both directions from the supplied successor/predecessor relation.
    // No borrowed pointer or numeric identity ordering becomes topology.
    for (const auto& edge : edges) {
        const auto parent = value.index_of(edge.predecessor);
        const auto child = value.index_of(edge.successor);
        if (!parent || !child || parent == child)
            return failure(errc::invalid_argument);
        auto& outgoing = value.links_[*parent];
        auto& incoming = value.links_[*child];
        if (
          outgoing.successor_count == 2 || incoming.predecessor_count == 2
          || value.reachable_[*parent][*child])
            return failure(errc::invalid_argument);
        outgoing.successors[outgoing.successor_count++] = *child;
        incoming.predecessors[incoming.predecessor_count++] = *parent;
        value.reachable_[*parent][*child] = true;
        value.edges_[value.edge_count_++] = edge;
    }
    std::optional<std::size_t> root;
    for (std::size_t i = 0; i < value.range_count_; ++i) {
        const auto& links = value.links_[i];
        if (links.predecessor_count == 0) {
            if (root) return failure(errc::invalid_argument);
            root = i;
        }
        if (links.successor_count != 0 && !value.ranges_[i].sealed_end)
            return failure(errc::invalid_argument);
    }
    if (!root || value.ranges_[*root].interval != keyspace_interval::root())
        return failure(errc::invalid_argument);
    value.root_ = *root;
    // Bounded transitive closure also detects cycles in disconnected pieces.
    for (std::size_t through = 0; through < value.range_count_; ++through)
        for (std::size_t from = 0; from < value.range_count_; ++from)
            for (std::size_t to = 0; to < value.range_count_; ++to)
                value.reachable_[from][to]
                  = value.reachable_[from][to]
                    || (value.reachable_[from][through] && value.reachable_[through][to]);
    for (std::size_t i = 0; i < value.range_count_; ++i) {
        if (
          value.reachable_[i][i] || (i != *root && !value.reachable_[*root][i]))
            return failure(errc::invalid_argument);
        const auto& links = value.links_[i];
        if (links.successor_count == 2) {
            const auto left = links.successors[0];
            const auto right = links.successors[1];
            const auto interval = value.ranges_[i].interval;
            const std::array children{
              value.ranges_[left].interval, value.ranges_[right].interval};
            if (
              value.links_[left].predecessor_count != 1
              || value.links_[right].predecessor_count != 1
              || !children[0].is_buddy_of(children[1])
              || std::uint64_t{children[0].depth()}
                   != std::uint64_t{interval.depth()} + 1U
              || !validate_keyspace_coverage(children, interval))
                return failure(errc::invalid_argument);
        } else if (
          links.successor_count == 1
          && value.links_[links.successors[0]].predecessor_count != 2) {
            return failure(errc::invalid_argument);
        }
        if (links.predecessor_count == 1) {
            if (value.links_[links.predecessors[0]].successor_count != 2)
                return failure(errc::invalid_argument);
        } else if (links.predecessor_count == 2) {
            const auto left = links.predecessors[0];
            const auto right = links.predecessors[1];
            const std::array parents{
              value.ranges_[left].interval, value.ranges_[right].interval};
            if (
              value.links_[left].successor_count != 1
              || value.links_[right].successor_count != 1
              || !parents[0].is_buddy_of(parents[1])
              || !validate_keyspace_coverage(
                parents, value.ranges_[i].interval))
                return failure(errc::invalid_argument);
        }
    }
    return value;
}

result<bool>
lineage_model::precedes(range_id ancestor, range_id descendant) const noexcept {
    const auto from = index_of(ancestor);
    const auto to = index_of(descendant);
    if (!from || !to) return failure(errc::not_found);
    return reachable_[*from][*to];
}

result<void> lineage_model::validate_frontier(
  const lineage_frontier& before) const noexcept {
    if (before.topic() != topic_) return failure(errc::wrong_context);
    std::array<std::size_t, maximum_lineage_ranges> indices{};
    std::array<const keyspace_interval*, maximum_lineage_ranges> intervals{};
    for (std::size_t i = 0; i < before.size_; ++i) {
        const auto& cursor = *before.cursors_[i];
        const auto index = index_of(cursor.range());
        if (!index) return failure(errc::not_found);
        const auto& range = ranges_[*index];
        if (range.sealed_end && cursor.next() > *range.sealed_end)
            return failure(errc::out_of_range);
        indices[i] = *index;
        intervals[i] = &range.interval;
        for (std::size_t previous = 0; previous < i; ++previous)
            if (
              reachable_[*index][indices[previous]]
              || reachable_[indices[previous]][*index])
                return failure(errc::invalid_argument);
    }
    auto sorted = std::span{intervals}.first(before.size_);
    std::ranges::sort(
      sorted,
      [](
        const keyspace_interval* left,
        const keyspace_interval* right) noexcept {
          if (left->prefix() != right->prefix())
              return left->prefix() < right->prefix();
          return left->depth() < right->depth();
      });
    ordered_keyspace_coverage coverage{keyspace_interval::root()};
    for (const auto* interval : sorted) {
        if (auto added = coverage.append(*interval); !added) return added;
    }
    return coverage.finish();
}

result<lineage_frontier> lineage_model::replace(
  const lineage_frontier& before,
  std::span<const std::size_t> parents,
  std::span<const std::size_t> children,
  item_count maximum_cursors) const noexcept {
    if (auto valid = validate_frontier(before); !valid)
        return failure(valid.error());
    // Every distinct immediate predecessor must still be present at its known
    // end. Do not remove a completed merge parent while the other is partial.
    for (const auto parent : parents) {
        const auto cursor = before.find(ranges_[parent].id);
        if (!cursor) return failure(errc::not_found);
        if (cursor->next() != *ranges_[parent].sealed_end)
            return failure(errc::unavailable);
    }
    const auto count = before.size_ - parents.size() + children.size();
    if (
      count > std::min<std::uint64_t>(
        maximum_lineage_ranges, maximum_cursors.value()))
        return failure(errc::resource_exhausted);
    lineage_frontier after{topic_};
    for (std::size_t i = 0; i < before.size_; ++i) {
        const auto& cursor = *before.cursors_[i];
        const auto removed = std::ranges::any_of(
          parents, [&](std::size_t parent) noexcept {
              return cursor.range() == ranges_[parent].id;
          });
        if (!removed) after.cursors_[after.size_++] = cursor;
    }
    for (const auto child : children)
        after.cursors_[after.size_++]
          = range_cursor::make(ranges_[child].id, range_logical_end{}).value();
    // The entire model has at most 16 scalar positions. Sorting this private
    // replacement restores wire key order without assigning it causal meaning.
    std::ranges::sort(
      std::span{after.cursors_}.first(after.size_),
      [](const auto& left, const auto& right) noexcept {
          return range_cursor_less{}(*left, *right);
      });
    return after;
}

result<lineage_frontier> lineage_model::split(
  const lineage_frontier& before,
  range_id parent,
  item_count maximum_cursors) const noexcept {
    const auto index = index_of(parent);
    if (!index) return failure(errc::not_found);
    const auto& links = links_[*index];
    if (links.successor_count != 2) return failure(errc::invalid_argument);
    const std::array parents{*index};
    return replace(before, parents, links.successors, maximum_cursors);
}

result<lineage_frontier> lineage_model::merge(
  const lineage_frontier& before,
  range_id successor,
  item_count maximum_cursors) const noexcept {
    const auto index = index_of(successor);
    if (!index) return failure(errc::not_found);
    const auto& links = links_[*index];
    if (links.predecessor_count != 2) return failure(errc::invalid_argument);
    const std::array children{*index};
    return replace(before, links.predecessors, children, maximum_cursors);
}

result<lineage_frontier> lineage_model::advance(
  const lineage_frontier& before, range_cursor position) const noexcept {
    if (auto valid = validate_frontier(before); !valid)
        return failure(valid.error());
    for (std::size_t i = 0; i < before.size_; ++i) {
        const auto& current = *before.cursors_[i];
        if (current.range() != position.range()) continue;
        if (position.next() < current.next())
            return failure(errc::invalid_argument);
        const auto& range = ranges_[*index_of(position.range())];
        if (range.sealed_end && position.next() > *range.sealed_end)
            return failure(errc::out_of_range);
        auto after = before;
        after.cursors_[i] = position;
        return after;
    }
    return failure(errc::not_found);
}

result<void> lineage_model::validate_advance(
  const lineage_frontier& before,
  const lineage_frontier& after) const noexcept {
    if (auto valid = validate_frontier(before); !valid)
        return failure(valid.error());
    if (auto valid = validate_frontier(after); !valid)
        return failure(valid.error());
    std::size_t removed = 0;
    std::size_t added = 0;
    range_id removed_id;
    range_id added_id;
    std::optional<range_cursor> changed;
    for (std::size_t i = 0; i < before.size_; ++i) {
        const auto& current = *before.cursors_[i];
        const auto next = after.find(current.range());
        if (!next) {
            ++removed;
            removed_id = current.range();
        } else if (*next != current) {
            if (changed) return failure(errc::invalid_argument);
            changed = *next;
        }
    }
    for (std::size_t i = 0; i < after.size_; ++i) {
        const auto& next = *after.cursors_[i];
        if (!before.find(next.range())) {
            ++added;
            added_id = next.range();
        }
    }
    if (removed == 0 && added == 0 && !changed) return {};
    const auto expected = [&] -> result<lineage_frontier> {
        if (removed == 0 && added == 0) return advance(before, *changed);
        if (changed) return failure(errc::invalid_argument);
        if (removed == 1 && added == 2) return split(before, removed_id);
        if (removed == 2 && added == 1) return merge(before, added_id);
        return failure(errc::invalid_argument);
    }();
    if (!expected) return failure(expected.error());
    if (*expected != after) return failure(errc::invalid_argument);
    return {};
}

} // namespace kwaque::model::testing
