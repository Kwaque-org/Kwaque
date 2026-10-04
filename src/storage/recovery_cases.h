#pragma once

#include "src/storage/segment_format.h"
#include "src/storage/wal_format.h"

#include <array>
#include <cstdint>
#include <span>
#include <string_view>

namespace kwaque::storage {

// Evidence about one item: a batch slot, where a PREPARE in the WAL and a
// block in its segment may both name the same target position, or a byte
// region of one file. Matching order matters: each predicate precedes the
// ones it implies.
enum class recovery_predicate : std::uint8_t {
    // The item is a byte region of one file, not a batch slot.
    region,
    // A resolved PREPARE in the WAL's classified content names the slot.
    prepare,
    // A complete block starts at the slot, scanned before the segment's first
    // damage.
    block,
    // The slot lies inside pinned evidence, which is not scanned again.
    pinned,
    // The slot lies at or below the recovered boundary, or inside pinned
    // evidence.
    covered,
    // The PREPARE's target strictly increases within its segment in WAL
    // order, as formation submits groups.
    ordered,
    // Both copies agree: target, batch identity, original binding, length,
    // and the child bytes, directly or by the XXH3-128 identity of each
    // copy's exact child.
    identical,
    // The claimed slot fits the layout: aligned, at or after the data start,
    // and never inside another object or a certified footer.
    placed,
    // The region lies beyond a rotated WAL file's sealed end.
    slack,
    // The region starts at its file's first damage.
    damage,
    // The region lies below pinned certification: a rotated WAL file's sealed
    // prefix, a checkpoint, a published or recovering boundary, or a sealed
    // root.
    certified,
};
inline constexpr std::size_t recovery_predicate_count = 11;
using recovery_parameters = std::array<bool, recovery_predicate_count>;

enum class recovery_matcher : std::uint8_t {
    any,
    is_false,
    is_true,
    // A mismatch is an impossible state, never a non-match.
    assert_is_false,
    assert_is_true,
};

// What the evidence establishes. Classification grants no visibility, serves
// nothing and changes no byte; actions on it are supplied separately.
enum class recovery_classification : std::uint8_t {
    // Both copies agree under the recovered boundary, or the slot lies in
    // pinned evidence: the local data obligation is met.
    satisfied,
    // A footer-covered block whose PREPARE did not survive in the WAL's
    // classified content. Overlapped barriers make this ordinary.
    footer_only,
    // A PREPARE with no block at or below the recovered boundary: preserve
    // it until a supplied resolution.
    candidate,
    // A complete block after the recovered boundary: unresolved, never
    // readable merely because its checksums pass.
    suffix,
    // Differing copies of one slot, a target out of WAL order, or a target
    // that cannot be placed: stop affected recovery and retain evidence.
    conflict,
    // Verified content before a file's first damage.
    content,
    // The first damage and every later byte, beyond all pinned
    // certification. Retained; later surviving envelopes are not evidence.
    uncertified_tail,
    // Damage below pinned certification: stop affected recovery.
    corruption,
    // Bytes after a rotated WAL file's sealed end: never content.
    slack,
};

struct recovery_case final {
    std::string_view label;
    recovery_classification classification;
    std::array<recovery_matcher, recovery_predicate_count> pattern;

    // In predicate order: false at the first mismatching is_true/is_false,
    // an invariant_violation error at the first failing assertion.
    [[nodiscard]] result<bool> check(const recovery_parameters&) const noexcept;
};

// Every parameter combination matches exactly one case, or matches none and
// fails an assertion of some case: an impossible state.
[[nodiscard]] std::span<const recovery_case> recovery_cases() noexcept;

// The one case these parameters match. The evidence builders never produce
// impossible parameters; doing so is an invariant violation.
[[nodiscard]] const recovery_case&
classify_recovery(const recovery_parameters& parameters);

// The first field in which two copies of one slot differ.
enum class recovery_copy_difference : std::uint8_t {
    none,
    // Target segment, position or physical begin.
    target,
    batch_id,
    original_binding,
    length,
    child,
};

// Compares a PREPARE's exact child with the child a block carries, both
// already validated by their codecs. No copy is made.
[[nodiscard]] result<recovery_copy_difference>
compare_recovery_copies(const wal_prepare& prepare, const segment_block& block);

// What compare_recovery_copies reads from a PREPARE, kept once its copy is
// gone: the claims, and the XXH3-128 identity of its exact child.
struct recovery_copy_identity final {
    segment_write_context target;
    model::batch_id id;
    model::producer_stream_binding binding;
    byte_count child_bytes;
    codec::immutable_object_digest child;
};
[[nodiscard]] recovery_copy_identity recovery_identity(
  const wal_prepare& prepare, codec::immutable_object_digest child) noexcept;
// compare_recovery_copies up to the child bytes, in the same order: none means
// every claim and the length agree, and the children's identities decide.
[[nodiscard]] result<recovery_copy_difference> compare_recovery_claims(
  const recovery_copy_identity& prepare, const segment_block& block);

// One batch slot as the merge of WAL and segment evidence found it.
struct recovery_slot final {
    const wal_prepare* prepare{nullptr};
    const segment_block* block{nullptr};
    bool pinned{false};
    bool covered{false};
    // Meaningful only with a PREPARE.
    bool ordered{false};
    bool placed{false};
    // Meaningful only with both copies.
    recovery_copy_difference difference{recovery_copy_difference::none};
};

// The same evidence once the copies themselves are gone: a merge classifies a
// slot only when a footer covers it or its segment's walk ends.
struct recovery_slot_evidence final {
    bool prepare{false};
    bool block{false};
    bool pinned{false};
    bool covered{false};
    bool ordered{false};
    bool placed{false};
    recovery_copy_difference difference{recovery_copy_difference::none};
};

// Predicates that depend on an absent copy are false.
[[nodiscard]] recovery_parameters
batch_parameters(const recovery_slot& slot) noexcept;
[[nodiscard]] recovery_parameters
batch_parameters(const recovery_slot_evidence& slot) noexcept;
[[nodiscard]] recovery_parameters
region_parameters(bool slack, bool damage, bool certified) noexcept;

} // namespace kwaque::storage
