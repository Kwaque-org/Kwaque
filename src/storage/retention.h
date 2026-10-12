#pragma once

#include "src/base/units.h"
#include "src/model/position.h"
#include "src/runtime/time.h"
#include "src/storage/format_context.h"
#include "src/storage/segment_immutable.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>

namespace kwaque::storage {

// One segment as whoever owns the segments supplies it for a walk. The
// segments of a range are supplied oldest first.
struct retention_segment final {
    segment_context segment;
    // Where it ends in its range: nothing that ends past the walk's bound
    // may go.
    model::range_logical_end end;
    // When it was sealed, as its owner of segments recorded it once for
    // every replica. None when no time is known for it: it can then go by
    // size, and never by age.
    std::optional<runtime::wall_time> sealed;
};

// Whether anything on this shard still needs a segment, which only the
// shard knows.
enum class retention_need : std::uint8_t {
    // Nothing: it may go.
    none,
    // It is not sealed here: appended to, sealing, failed or found by a
    // restart. What pins the log and the checkpoint is of such segments.
    unsealed,
    // A reader has a session on it, or a transition of it runs.
    read,
    // The shard has no owner for it and was given nothing to know it by.
    unknown,
};
// What only this shard knows of one supplied segment.
struct retention_local final {
    // What its data file holds on its device.
    byte_count bytes;
    retention_need need{retention_need::unknown};
    // It is being removed already. It counts among those that may go
    // whatever a rule or the bound says of it, so that whoever asked for
    // its removal is offered it again and asks again.
    bool going{false};
};

// Why a walk ended where it did.
enum class retention_stop : std::uint8_t {
    // Every supplied segment may go.
    exhausted,
    // A rule selects the next segment, and it ends past the bound.
    bound,
    // The next segment is selected by neither rule: it is not older than
    // the cutoff, and removing it would take the range below its target.
    // Nothing holds retention back here.
    kept,
    // A rule selects the next segment, and something on this shard still
    // needs it; or the shard does not know the segment, and so not what a
    // rule would say of it.
    needed,
};
struct retention_request final {
    // A range's segments, oldest first.
    std::span<const retention_segment> segments;
    // Segments sealed before this may go by age. None: no age rule.
    std::optional<runtime::wall_time> cutoff;
    // The bytes the range should hold on this broker. While it holds
    // more, its oldest segments may go, as long as it does not fall below
    // this. None: no size rule.
    std::optional<byte_count> target;
    // Nothing that ends past this may go, whatever the rules say.
    model::range_logical_end bound;
};
struct retention_candidates final {
    // The oldest `count` supplied segments may go; `bytes` is what they
    // hold together.
    std::size_t count{0};
    byte_count bytes;
    retention_stop stop{retention_stop::exhausted};
    // What still needs the segment the walk stopped at.
    retention_need need{retention_need::none};
    bool operator==(const retention_candidates&) const = default;
};

// Selects the segments of one range that may go, as a prefix of what was
// supplied: oldest first, stopping at the first that may not. A segment
// may go by age when it was sealed before the cutoff, and by size when the
// range holds its target or more and would still hold it without the
// segment. It does not go when it ends past the bound or when something
// local still needs it. The rules are asked first, so the reason a walk
// ended says whether retention is satisfied there or held back.
//
// `local` holds what the shard knows of each supplied segment, in the same
// order. Nothing is read, no clock among them, and nothing is removed: a
// candidate authorizes nothing.
[[nodiscard]] inline retention_candidates select_retention(
  const retention_request& request,
  std::span<const retention_local> local) noexcept {
    retention_candidates selected;
    const auto known = std::min(request.segments.size(), local.size());
    std::uint64_t held = 0;
    for (std::size_t i = 0; i != known; ++i) {
        const auto more = local[i].bytes.value();
        held = more > UINT64_MAX - held ? UINT64_MAX : held + more;
    }
    // Nothing goes by size while the range holds less than its target.
    // Beyond that, what it holds over the target is the most the size rule
    // frees.
    const bool sized = request.target && held >= request.target->value();
    auto excess = sized ? held - request.target->value() : std::uint64_t{0};
    for (std::size_t i = 0; i != request.segments.size(); ++i) {
        const auto& candidate = request.segments[i];
        // What was not supplied is not known.
        const auto facts = i < local.size() ? local[i] : retention_local{};
        if (facts.going) {
            ++selected.count;
            continue;
        }
        const auto bytes = facts.bytes.value();
        const bool aged = request.cutoff && candidate.sealed
                          && *candidate.sealed < *request.cutoff;
        const bool over = sized && bytes <= excess;
        // A segment the shard does not know has no bytes to ask the size
        // rule with.
        if (facts.need != retention_need::unknown && !aged && !over) {
            selected.stop = retention_stop::kept;
            return selected;
        }
        if (candidate.end > request.bound) {
            selected.stop = retention_stop::bound;
            return selected;
        }
        if (facts.need != retention_need::none) {
            selected.stop = retention_stop::needed;
            selected.need = facts.need;
            return selected;
        }
        excess -= std::min(excess, bytes);
        ++selected.count;
        selected.bytes = selected.bytes.checked_add(facts.bytes)
                           .value_or(byte_count{UINT64_MAX});
    }
    return selected;
}

// What a segment's owner holds it to be when it authorizes its removal:
// the segment, and either the sealed extent it was published as or that it
// was never sealed and is to be discarded. The local publication's
// generation is no part of it: that moves when an index is published, and
// no owner outside sees it.
struct deletion_authorization final {
    segment_context segment;
    std::optional<segment_immutable_expectation> sealed;
    bool operator==(const deletion_authorization&) const = default;
};

// What this shard holds a segment to be.
enum class segment_holding : std::uint8_t {
    // No owner here, and nothing supplied to know it by.
    unknown,
    // Still written here: in creation, appended to or sealing.
    attached,
    // Not sealed and written no more: failed, or found by a restart.
    unsealed,
    // Sealed, with `extent`.
    sealed,
    // Being removed under `accepted`.
    deleting,
};
struct segment_held final {
    segment_context segment;
    segment_holding holding{segment_holding::unknown};
    std::optional<segment_immutable_expectation> extent;
    // The authorization a segment that is being removed was accepted with.
    // None for one a restart found under its deleting name with nothing
    // said of why.
    std::optional<deletion_authorization> accepted;
};

enum class authorization_verdict : std::uint8_t {
    // It names what the shard holds: the segment may be removed.
    authorized,
    // The segment is being removed under this same authorization: the
    // first answer again.
    repeated,
    // It names another segment than the one it was checked against.
    wrong_context,
    // It names something the shard does not hold: another sealed extent, a
    // sealed extent for a segment that is not sealed here, or anything but
    // what a segment being removed was accepted with.
    wrong_identity,
    // It names as unsealed a segment that is sealed here.
    sealed_here,
    // The segment is still written here.
    attached,
    // The shard does not know the segment, or does not know what its
    // device holds of it: its seal failed after it began to publish.
    unknown,
};

// Checks a supplied authorization against the segment it names. Nothing is
// changed by it, whatever the answer.
[[nodiscard]] inline authorization_verdict authorize_deletion(
  const deletion_authorization& supplied, const segment_held& held) noexcept {
    if (supplied.segment != held.segment)
        return authorization_verdict::wrong_context;
    switch (held.holding) {
    case segment_holding::unknown:
        return authorization_verdict::unknown;
    case segment_holding::attached:
        return authorization_verdict::attached;
    case segment_holding::unsealed:
        return supplied.sealed ? authorization_verdict::wrong_identity
                               : authorization_verdict::authorized;
    case segment_holding::sealed:
        if (!supplied.sealed) return authorization_verdict::sealed_here;
        return held.extent && *supplied.sealed == *held.extent
                 ? authorization_verdict::authorized
                 : authorization_verdict::wrong_identity;
    case segment_holding::deleting:
        // With nothing accepted its name on the device is what decided,
        // and the first authorization that names it is taken.
        if (!held.accepted) return authorization_verdict::authorized;
        return *held.accepted == supplied
                 ? authorization_verdict::repeated
                 : authorization_verdict::wrong_identity;
    }
    return authorization_verdict::unknown;
}

} // namespace kwaque::storage
