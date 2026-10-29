#include "src/storage/local_append_verdict.h"

#include "src/base/invariant.h"

#include <array>

namespace kwaque::storage {
namespace {
template<typename... Values>
constexpr std::uint8_t of(Values... values) noexcept {
    return static_cast<std::uint8_t>(
      ((1U << static_cast<unsigned>(values)) | ...));
}
constexpr std::uint8_t every = 0xff;

using enum local_append_verdict;
using P = local_append_placement;
using W = local_wal_state;
using S = local_segment_state;
using E = local_earlier_state;
constexpr auto accepted = of(W::pending, W::durable, W::failed);
constexpr auto offered = of(
  W::deciding, W::refused, W::unknown, W::pending, W::durable, W::failed);
constexpr auto in_order = of(
  S::frozen, S::dropped, S::submitted, S::durable, S::failed);

// clang-format off
constexpr std::array cases{
    // A group that never froze has no place in the file and nothing in the
    // WAL: no PREPARE and no segment byte.
    local_verdict_case{"@unfrozen",             not_written, every,        of(W::none), of(S::unfrozen), every},
    local_verdict_case{"@unfrozen_offered",     impossible,  every,        offered,     of(S::unfrozen), every},
    // Only a WAL-placed group offers members, and every frozen one does. Its
    // segment group is submitted or dropped only once the WAL answered, and
    // submitted only if the WAL accepted it.
    local_verdict_case{"@segment_offered",      impossible,  of(P::segment), offered,   in_order,        every},
    local_verdict_case{"@wal_unoffered",        impossible,  of(P::wal),  of(W::none),  in_order,        every},
    local_verdict_case{"@unanswered_settled",   impossible,  of(P::wal),  of(W::deciding),
                                                 of(S::dropped, S::submitted, S::durable, S::failed),      every},
    local_verdict_case{"@refused_submitted",    impossible,  of(P::wal),  of(W::refused, W::unknown),
                                                 of(S::submitted, S::durable, S::failed),                  every},
    // Segment-placed: durable only by its own segment's barrier.
    local_verdict_case{"@segment_frozen",       wait,        of(P::segment), of(W::none), of(S::frozen),  every},
    local_verdict_case{"@segment_unsynced",     wait,        of(P::segment), of(W::none), of(S::submitted), of(E::durable)},
    local_verdict_case{"@segment_dropped",      not_written, of(P::segment), of(W::none), of(S::dropped), every},
    local_verdict_case{"@segment_durable",      durable,     of(P::segment), of(W::none), of(S::durable), of(E::durable)},
    local_verdict_case{"@segment_failed",       uncertain,   of(P::segment), of(W::none), of(S::failed),  every},
    local_verdict_case{"@segment_after_failure",uncertain,   of(P::segment), of(W::none),
                                                 of(S::submitted, S::durable),                             of(E::failed)},
    // WAL-placed, before or without the WAL's acceptance.
    local_verdict_case{"@wal_unanswered",       wait,        of(P::wal),  of(W::deciding, W::refused, W::unknown),
                                                 of(S::frozen),                                            every},
    local_verdict_case{"@wal_refused",          not_written, of(P::wal),  of(W::refused), of(S::dropped), every},
    local_verdict_case{"@wal_unknown",          uncertain,   of(P::wal),  of(W::unknown), of(S::dropped), every},
    // WAL-placed and accepted: durable by its WAL barrier, or by a barrier
    // of its segment through its footer, whichever is first.
    local_verdict_case{"@wal_durable",          durable,     of(P::wal),  of(W::durable), in_order,       of(E::durable)},
    local_verdict_case{"@wal_covered",          durable,     of(P::wal),  of(W::pending, W::failed),
                                                 of(S::durable),                                           of(E::durable)},
    local_verdict_case{"@wal_waiting",          wait,        of(P::wal),  of(W::pending, W::failed),
                                                 of(S::frozen, S::submitted),                              of(E::durable)},
    local_verdict_case{"@wal_unsynced",         wait,        of(P::wal),  of(W::pending),
                                                 of(S::dropped, S::failed),                                of(E::durable)},
    local_verdict_case{"@wal_failed",           uncertain,   of(P::wal),  of(W::failed),
                                                 of(S::dropped, S::failed),                                of(E::durable)},
    local_verdict_case{"@wal_after_failure",    uncertain,   of(P::wal),  accepted,   in_order,          of(E::failed)},
};
// clang-format on

constexpr bool in(std::uint8_t set, auto value) noexcept {
    return ((set >> static_cast<unsigned>(value)) & 1U) != 0;
}
} // namespace

bool local_verdict_case::matches(
  local_append_placement placement,
  local_wal_state wal_state,
  local_segment_state segment_state,
  local_earlier_state earlier_state) const noexcept {
    return in(placements, placement) && in(wal, wal_state)
           && in(segment, segment_state) && in(earlier, earlier_state);
}

std::span<const local_verdict_case> local_verdict_cases() noexcept {
    return cases;
}

const local_verdict_case& classify_local_append(
  local_append_placement placement,
  local_wal_state wal,
  local_segment_state segment,
  local_earlier_state earlier) {
    const local_verdict_case* found = nullptr;
    for (const auto& candidate : cases) {
        if (!candidate.matches(placement, wal, segment, earlier)) continue;
        KWAQUE_INVARIANT(
          invariant_id{"KQ-LOCAL-APPEND-VERDICT-UNIQUE"},
          found == nullptr,
          "a segment group's state matches two cases");
        found = &candidate;
    }
    KWAQUE_INVARIANT(
      invariant_id{"KQ-LOCAL-APPEND-VERDICT-EXHAUSTIVE"},
      found != nullptr,
      "a segment group's state matches no case");
    KWAQUE_INVARIANT(
      invariant_id{"KQ-LOCAL-APPEND-VERDICT-POSSIBLE"},
      found->verdict != local_append_verdict::impossible,
      "a segment group's state is impossible");
    return *found;
}

} // namespace kwaque::storage
