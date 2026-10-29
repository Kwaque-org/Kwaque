#include "src/storage/recovery_cases.h"

#include "src/base/invariant.h"

namespace kwaque::storage {
namespace {
using enum recovery_matcher;
constexpr auto xx = any;
constexpr auto f0 = is_false;
constexpr auto t1 = is_true;
constexpr auto a0 = assert_is_false;
constexpr auto a1 = assert_is_true;
using pattern = std::array<recovery_matcher, recovery_predicate_count>;

// A batch slot: the region predicates are false.
constexpr pattern batch(
  recovery_matcher prepare,
  recovery_matcher block,
  recovery_matcher pinned,
  recovery_matcher covered,
  recovery_matcher ordered,
  recovery_matcher identical,
  recovery_matcher placed,
  recovery_matcher contiguous) noexcept {
    return {
      f0,
      prepare,
      block,
      pinned,
      covered,
      ordered,
      identical,
      placed,
      contiguous,
      a0,
      a0,
      a0,
      a0};
}
// A byte region: the batch predicates are false.
constexpr pattern region(
  recovery_matcher slack,
  recovery_matcher damage,
  recovery_matcher certified,
  recovery_matcher filled) noexcept {
    return {
      t1, a0, a0, a0, a0, a0, a0, a0, a0, slack, damage, certified, filled};
}

using enum recovery_classification;
// clang-format off
constexpr std::array cases{
    //                                                   prep blk pin cov ord ident place contig
    // Both copies. A scanned block never lies inside pinned evidence, and a
    // block at the slot proves the slot is placed. One found after the
    // recovered boundary is taken when a footer the walk reaches covers it,
    // and otherwise waits as a candidate: its group was never certified.
    recovery_case{"@satisfied",           satisfied,   batch(t1, t1, a0, t1, t1, t1, a1, xx)},
    recovery_case{"@adopt",               materialize, batch(t1, t1, a0, f0, t1, t1, a1, t1)},
    recovery_case{"@found_candidate",     candidate,   batch(t1, t1, a0, f0, t1, t1, a1, f0)},
    recovery_case{"@differing_copies",    conflict,    batch(t1, t1, a0, xx, t1, f0, a1, xx)},
    recovery_case{"@block_out_of_order",  conflict,    batch(t1, t1, a0, xx, f0, xx, a1, xx)},
    // A PREPARE whose slot lies in pinned evidence gets a layout check only.
    recovery_case{"@satisfied_pinned",    satisfied,   batch(t1, f0, t1, a1, t1, a0, t1, xx)},
    recovery_case{"@pinned_misplaced",    conflict,    batch(t1, f0, t1, a1, t1, a0, f0, xx)},
    recovery_case{"@pinned_out_of_order", conflict,    batch(t1, f0, t1, a1, f0, a0, xx, xx)},
    // Scanned history up to the boundary has a block at every placed slot.
    recovery_case{"@covered_misplaced",   conflict,    batch(t1, f0, f0, t1, xx, a0, a0, xx)},
    // After the boundary, including the damaged tail: the WAL fills the
    // slot when every slot before it is filled, and otherwise it waits.
    recovery_case{"@materialize",         materialize, batch(t1, f0, f0, f0, t1, a0, t1, t1)},
    recovery_case{"@candidate",           candidate,   batch(t1, f0, f0, f0, t1, a0, t1, f0)},
    recovery_case{"@tail_misplaced",      conflict,    batch(t1, f0, f0, f0, t1, a0, f0, xx)},
    recovery_case{"@tail_out_of_order",   conflict,    batch(t1, f0, f0, f0, f0, a0, xx, xx)},
    // A block without a surviving PREPARE: ordinary for a segment-placed
    // block under its footer, and never adopted after the boundary.
    recovery_case{"@footer_only",         footer_only, batch(f0, a1, a0, t1, a0, a0, a0, a0)},
    recovery_case{"@suffix_unsourced",    suffix,      batch(f0, a1, a0, f0, a0, a0, a0, a0)},
    //                                                          slack damage certified filled
    recovery_case{"@slack",               slack,            region(t1, xx, a0, a0)},
    recovery_case{"@content",             content,          region(f0, f0, xx, a0)},
    recovery_case{"@materialized",        materialized,     region(f0, t1, f0, t1)},
    recovery_case{"@repaired",            repaired,         region(f0, t1, t1, t1)},
    recovery_case{"@uncertified_tail",    uncertified_tail, region(f0, t1, f0, f0)},
    recovery_case{"@corruption",          corruption,       region(f0, t1, t1, f0)},
};
// clang-format on

constexpr std::size_t at(recovery_predicate predicate) noexcept {
    return static_cast<std::size_t>(predicate);
}
} // namespace

result<bool>
recovery_case::check(const recovery_parameters& parameters) const noexcept {
    for (std::size_t i = 0; i < recovery_predicate_count; ++i) {
        switch (pattern[i]) {
        case any:
            break;
        case is_false:
            if (parameters[i]) return false;
            break;
        case is_true:
            if (!parameters[i]) return false;
            break;
        case assert_is_false:
            if (parameters[i]) return failure(errc::invariant_violation);
            break;
        case assert_is_true:
            if (!parameters[i]) return failure(errc::invariant_violation);
            break;
        }
    }
    return true;
}

std::span<const recovery_case> recovery_cases() noexcept { return cases; }

const recovery_case& classify_recovery(const recovery_parameters& parameters) {
    const recovery_case* found = nullptr;
    for (const auto& candidate : cases) {
        const auto matched = candidate.check(parameters);
        KWAQUE_INVARIANT(
          invariant_id{"KQ-RECOVERY-CASE-POSSIBLE"},
          matched.has_value(),
          "recovery evidence is an impossible state");
        if (!*matched) continue;
        KWAQUE_INVARIANT(
          invariant_id{"KQ-RECOVERY-CASE-UNIQUE"},
          found == nullptr,
          "recovery evidence matches two cases");
        found = &candidate;
    }
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RECOVERY-CASE-EXHAUSTIVE"},
      found != nullptr,
      "recovery evidence matches no case");
    return *found;
}

result<recovery_copy_difference> compare_recovery_copies(
  const wal_prepare& prepare, const segment_block& block) {
    const auto target = prepare.target();
    const auto descriptor = block.descriptor();
    const auto placed = descriptor.coverage();
    if (
      target.segment() != descriptor.context()
      || target.position() != placed.bytes().begin()
      || target.physical_begin() != placed.physical().begin())
        return recovery_copy_difference::target;
    const auto wal_info = prepare.batch().info();
    const auto wal = wal_info.context.submitted();
    const auto info = descriptor.batch();
    const auto data = info.context.submitted();
    if (wal.id() != data.id()) return recovery_copy_difference::batch_id;
    if (wal.binding() != data.binding())
        return recovery_copy_difference::original_binding;
    const auto stamps = prepare.stamps();
    if (
      segment_block_stamps{stamps.append_time, stamps.target_certified_end}
        != descriptor.stamps()
      || wal_info.max_record_time != info.max_record_time)
        return recovery_copy_difference::stamps;
    auto child = block.child();
    if (!child) return failure(child.error());
    const auto& exact = prepare.batch().bytes();
    if (exact.size() != child->size()) return recovery_copy_difference::length;
    if (!exact.content_equals(*child)) return recovery_copy_difference::child;
    return recovery_copy_difference::none;
}

recovery_copy_identity recovery_identity(
  const wal_prepare& prepare, codec::immutable_object_digest child) noexcept {
    const auto info = prepare.batch().info();
    const auto batch = info.context.submitted();
    const auto stamps = prepare.stamps();
    return {
      prepare.target(),
      batch.id(),
      batch.binding(),
      {stamps.append_time, stamps.target_certified_end},
      info.max_record_time,
      prepare.batch().bytes().size(),
      child};
}

result<recovery_copy_difference> compare_recovery_claims(
  const recovery_copy_identity& prepare, const segment_block& block) {
    const auto descriptor = block.descriptor();
    const auto placed = descriptor.coverage();
    if (
      prepare.target.segment() != descriptor.context()
      || prepare.target.position() != placed.bytes().begin()
      || prepare.target.physical_begin() != placed.physical().begin())
        return recovery_copy_difference::target;
    const auto info = descriptor.batch();
    const auto data = info.context.submitted();
    if (prepare.id != data.id()) return recovery_copy_difference::batch_id;
    if (prepare.binding != data.binding())
        return recovery_copy_difference::original_binding;
    if (
      prepare.stamps != descriptor.stamps()
      || prepare.max_record_time != info.max_record_time)
        return recovery_copy_difference::stamps;
    auto child = block.child();
    if (!child) return failure(child.error());
    if (prepare.child_bytes != child->size())
        return recovery_copy_difference::length;
    return recovery_copy_difference::none;
}

recovery_parameters batch_parameters(const recovery_slot& slot) noexcept {
    return batch_parameters(
      recovery_slot_evidence{
        slot.prepare != nullptr,
        slot.block != nullptr,
        slot.pinned,
        slot.covered,
        slot.ordered,
        slot.placed,
        slot.contiguous,
        slot.difference});
}

recovery_parameters
batch_parameters(const recovery_slot_evidence& slot) noexcept {
    recovery_parameters output{};
    output[at(recovery_predicate::prepare)] = slot.prepare;
    output[at(recovery_predicate::block)] = slot.block;
    output[at(recovery_predicate::pinned)] = slot.pinned;
    output[at(recovery_predicate::covered)] = slot.covered;
    output[at(recovery_predicate::ordered)] = slot.prepare && slot.ordered;
    output[at(recovery_predicate::identical)]
      = slot.prepare && slot.block
        && slot.difference == recovery_copy_difference::none;
    output[at(recovery_predicate::placed)] = slot.prepare && slot.placed;
    output[at(recovery_predicate::contiguous)] = slot.prepare
                                                 && slot.contiguous;
    return output;
}

recovery_parameters region_parameters(
  bool slack, bool damage, bool certified, bool filled) noexcept {
    recovery_parameters output{};
    output[at(recovery_predicate::region)] = true;
    output[at(recovery_predicate::slack)] = slack;
    output[at(recovery_predicate::damage)] = damage;
    output[at(recovery_predicate::certified)] = certified;
    output[at(recovery_predicate::filled)] = filled;
    return output;
}

} // namespace kwaque::storage
