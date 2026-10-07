#pragma once

#include "src/storage/local_metadata.h"

#include <cstdint>
#include <optional>
#include <span>
#include <vector>

namespace kwaque::storage::testing {

// What a test driver saw of the PREPAREs it caused, from its own inputs and
// the receipts it was handed: where each ends in the WAL, the segment it
// names and where its block ends in that segment's data file. No engine
// cursor, pin table or scan supplies these. After a checkpoint drops the WAL
// below its cutoff, every PREPARE below it must still be accounted for by
// something durable, and this says which one is not.
class checkpoint_coverage_oracle final {
public:
    struct prepare final {
        local_wal_cursor end;
        segment_context segment;
        runtime::file_position block_end;
        // Where it begins, when the driver knows: a discard names that.
        std::optional<local_wal_cursor> begin;
    };
    struct discard final {
        local_wal_cursor prepare;
        std::uint64_t decision;
    };

    void appended(
      local_wal_cursor end,
      segment_context segment,
      runtime::file_position block_end,
      std::optional<local_wal_cursor> begin = std::nullopt) {
        prepares_.push_back({end, segment, block_end, begin});
    }
    // A durable decision with this sequence discards the PREPARE that begins
    // here.
    void discarded(local_wal_cursor prepare, std::uint64_t decision) {
        discards_.push_back({prepare, decision});
    }
    [[nodiscard]] std::size_t prepares() const noexcept {
        return prepares_.size();
    }

    // The first PREPARE that ends at or below `cutoff` and that nothing
    // covers, or nothing when each is covered by one of:
    // - a boundary entry of its segment whose pinned footer lies at or after
    //   the end of its block;
    // - its segment's own publication: sealed or deleting, or pinning a
    //   boundary at or after the end of its block;
    // - a discard entry naming the decision that discards it.
    // Positions that cannot be compared count as uncovered.
    template<typename Table>
    [[nodiscard]] std::optional<std::size_t> uncovered(
      const local_store_context& owner,
      local_wal_cursor cutoff,
      const Table& table,
      std::span<const local_object_publication> publications) const {
        for (std::size_t i = 0; i != prepares_.size(); ++i) {
            const auto& seen = prepares_[i];
            const auto order = seen.end.compare(owner, cutoff, owner);
            if (!order) return i;
            if (*order > 0) continue;
            if (!covered(seen, table, publications)) return i;
        }
        return std::nullopt;
    }

private:
    template<typename Table>
    [[nodiscard]] bool covered(
      const prepare& seen,
      const Table& table,
      std::span<const local_object_publication> publications) const {
        for (const auto& entry : table) {
            if (entry.segment != seen.segment) continue;
            if (
              entry.disposition
                == local_checkpoint_disposition::segment_boundary
              && entry.locator >= seen.block_end.value())
                return true;
            if (
              entry.disposition
                == local_checkpoint_disposition::authorized_discard
              && seen.begin) {
                for (const auto& decided : discards_)
                    if (
                      decided.decision == entry.locator
                      && decided.prepare == *seen.begin)
                        return true;
            }
        }
        for (const auto& publication : publications) {
            if (publication.segment != seen.segment) continue;
            if (
              publication.state == local_object_state::sealed
              || publication.state == local_object_state::deleting)
                return true;
            if (
              publication.boundary
              && publication.boundary->position().value()
                   >= seen.block_end.value())
                return true;
        }
        return false;
    }

    std::vector<prepare> prepares_;
    std::vector<discard> discards_;
};

} // namespace kwaque::storage::testing
