#pragma once

#include "src/storage/local_append_verdict.h"

#include <array>
#include <cstddef>
#include <cstdint>

namespace kwaque::storage {

// Why a run of held WAL-placed groups was closed, and so written: the
// footer that closes it is what a barrier of its segment makes durable.
enum class segment_run_close : std::uint8_t {
    // It reached its hold with the held group whose footer closes it.
    filled,
    // A group that wants its barrier now was frozen after it.
    wanted,
    // The group frozen after it would have taken its write past one file
    // write.
    limited,
    // A barrier was asked through it.
    synced,
    // Its memory was wanted for appends.
    pressed,
};
inline constexpr std::size_t segment_run_closes = 5;

// What one append owner placed, held and synced since it was made. Every
// count only grows; the last four say what stands at the call.
struct local_append_statistics final {
    // Portions by local_append_placement, and the bytes their freezes took
    // in their segments.
    std::array<std::uint64_t, local_append_placements> portions{},
      portion_bytes{};
    // WAL-placed portions that joined a held run rather than being written
    // at once, and their bytes.
    std::uint64_t held_portions{0}, held_portion_bytes{0};
    // Held runs closed and so written, by segment_run_close.
    std::array<std::uint64_t, segment_run_closes> runs{};
    // Segment barriers run because a segment-placed portion's result waited
    // for one, and those run because a sync was asked.
    std::uint64_t barriers{0}, syncs{0};
    // Rounds that asked a sync of every segment whose groups a WAL file
    // behind the head still holds, at a rotation or at the retained limit,
    // and the syncs they asked.
    std::uint64_t sync_rounds{0}, round_syncs{0};
    // Bytes in runs held open, and how many runs; bytes the segments'
    // reported prefixes cover past their newest durable footers, which only
    // the WAL holds durable; and the shard's WAL files from the oldest one
    // holding a group not yet durable in its segment through the head, none
    // when there is no such group.
    std::uint64_t held_bytes{0}, held_runs{0}, unsynced_bytes{0},
      unsynced_files{0};
};

} // namespace kwaque::storage
