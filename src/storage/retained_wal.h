#pragma once

#include "src/runtime/error.h"
#include "src/storage/local_types.h"

#include <array>
#include <cstdint>
#include <optional>

namespace kwaque::storage {

// The most WAL files a shard can hold, which is also the highest limit that
// can be configured, and the limit it has unless configured otherwise.
inline constexpr std::uint32_t maximum_retained_wal_files = 64;
inline constexpr std::uint32_t default_retained_wal_files = 8;

// The WAL files a shard holds: its chain from the oldest file not yet removed
// to the head. A file whose removal failed still counts.
struct wal_retention final {
    std::uint32_t files{0};
    // The most a rotation may leave it holding; zero sets no limit below the
    // most a shard can hold.
    std::uint32_t limit{0};
    // A rotation would add a file past the limit, or past the most a shard
    // can hold.
    [[nodiscard]] bool full() const noexcept { return files >= most(); }
    // The most a rotation may leave it holding, whichever bound applies.
    [[nodiscard]] std::uint32_t most() const noexcept {
        return limit != 0 ? limit : maximum_retained_wal_files;
    }
    bool operator==(const wal_retention&) const noexcept = default;
};

// The names of those files, oldest first. Whoever makes a file the head
// names it here, and a restart names the files it found, so the names are
// never read back from the device. A file leaves from the oldest end, and
// only once it is gone: the names never have a hole, and a file whose removal
// failed stays as debt. The table is fixed at the most a shard can hold.
class retained_wal final {
public:
    [[nodiscard]] std::uint32_t files() const noexcept { return count_; }
    [[nodiscard]] bool empty() const noexcept { return count_ == 0; }
    [[nodiscard]] bool full() const noexcept { return count_ == ring_.size(); }
    [[nodiscard]] std::optional<model::wal_incarnation_id>
    oldest() const noexcept {
        if (count_ == 0) return std::nullopt;
        return ring_[first_];
    }
    [[nodiscard]] std::optional<model::wal_incarnation_id>
    newest() const noexcept {
        if (count_ == 0) return std::nullopt;
        return ring_[(first_ + count_ - 1) % ring_.size()];
    }
    // Files that lie wholly below `cutoff`: every one before the file it
    // names. That file is kept, and so is the newest, which is the head.
    [[nodiscard]] std::uint32_t
    below(const local_wal_cursor& cutoff) const noexcept;

    // The file that follows the newest in the chain. Incarnations only grow
    // along a chain; anything else is refused, and so is a full table.
    [[nodiscard]] runtime::result<void>
    extend(model::wal_incarnation_id file) noexcept;
    // The oldest file is gone. The newest never leaves this way.
    void drop_oldest() noexcept;

private:
    std::array<model::wal_incarnation_id, maximum_retained_wal_files> ring_{};
    std::uint32_t first_{0}, count_{0};
};

} // namespace kwaque::storage
