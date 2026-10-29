#pragma once

#include <cstdint>
#include <span>
#include <string_view>

namespace kwaque::storage {

// Where a group makes one segment's requests durable before they are
// reported: in the WAL, whose PREPAREs carry them until a barrier of their
// segment covers them, or in the segment alone, whose footer describes
// itself.
enum class local_append_placement : std::uint8_t { wal, segment };
inline constexpr std::size_t local_append_placements = 2;

// The WAL side of one segment group when its result is decided.
enum class local_wal_state : std::uint8_t {
    // Segment-placed, or never frozen: none of its members was offered.
    none,
    // Offered; the WAL has not answered the group's submission.
    deciding,
    // The WAL took none of the group, and that is proven.
    refused,
    // The submission ended without that proof: a PREPARE may exist.
    unknown,
    // Accepted; its barrier has not settled.
    pending,
    // A barrier covered its PREPAREs.
    durable,
    // Accepted, and its barrier failed.
    failed,
};
inline constexpr std::size_t local_wal_states = 7;

// The segment side of one segment group.
enum class local_segment_state : std::uint8_t {
    // It never froze: it has no place in the file.
    unfrozen,
    // Frozen in file order, and neither submitted nor dropped yet.
    frozen,
    // Never submitted: none of its bytes reaches the file.
    dropped,
    // Submitted; no barrier has settled through its footer.
    submitted,
    // A successful barrier's cut covers its footer.
    durable,
    // A failed barrier's cut covers its footer.
    failed,
};
inline constexpr std::size_t local_segment_states = 6;

// How the earlier groups of its segment were published. A group's result is
// decided only once every earlier one is published, in file order.
enum class local_earlier_state : std::uint8_t {
    // Every one durable, or there is none.
    durable,
    // One as a failure: no later one can be durable, since restart could not
    // fill the place before it.
    failed,
};
inline constexpr std::size_t local_earlier_states = 2;

enum class local_append_verdict : std::uint8_t {
    // Not yet decided: its result waits for a barrier or an answer.
    wait,
    durable,
    not_written,
    uncertain,
    // A state the owner never produces.
    impossible,
};

// One row: the sets of values it covers, one bit per value of each.
struct local_verdict_case final {
    std::string_view label;
    local_append_verdict verdict;
    std::uint8_t placements, wal, segment, earlier;

    [[nodiscard]] bool matches(
      local_append_placement,
      local_wal_state,
      local_segment_state,
      local_earlier_state) const noexcept;
};

// Every combination of the four matches exactly one row.
[[nodiscard]] std::span<const local_verdict_case>
local_verdict_cases() noexcept;

// The one row these match. The owner never produces an impossible state;
// doing so is an invariant violation.
[[nodiscard]] const local_verdict_case& classify_local_append(
  local_append_placement,
  local_wal_state,
  local_segment_state,
  local_earlier_state);

} // namespace kwaque::storage
