#include "src/base/allocation.h"
#include "src/base/units.h"
#include "src/bytes/test_allocation_profile.h"
#include "src/storage/recovery_cases.h"
#include "src/storage/recovery_decision.h"
#include "src/storage/recovery_inventory.h"
#include "src/storage/recovery_pins.h"
#include "src/storage/recovery_plan.h"
#include "src/storage/recovery_publication.h"
#include "src/storage/tests/footer_test_support.h"
#include "src/storage/tests/wal_test_support.h"

#include <gtest/gtest.h>

#include <map>
#include <string>

namespace kwaque::storage {
namespace {
using namespace testing;
using bytes::fragmented_buffer_parser;

recovery_parameters pattern(unsigned bits) {
    recovery_parameters output{};
    for (std::size_t i = 0; i < output.size(); ++i)
        output[i] = ((bits >> i) & 1U) != 0;
    return output;
}

// Every pattern matches exactly one case and fails no assertion, or matches
// none and fails an assertion of some case.
TEST(RecoveryCasesTest, EveryPatternMatchesOneCaseOrIsImpossible) {
    std::map<std::string_view, unsigned> hits;
    unsigned impossible = 0;
    for (unsigned bits = 0; bits < (1U << recovery_predicate_count); ++bits) {
        const auto parameters = pattern(bits);
        bool failed = false;
        const recovery_case* matched = nullptr;
        for (const auto& candidate : recovery_cases()) {
            const auto checked = candidate.check(parameters);
            if (!checked) {
                ASSERT_EQ(matched, nullptr) << bits;
                failed = true;
                continue;
            }
            if (*checked) {
                ASSERT_FALSE(failed) << bits;
                ASSERT_EQ(matched, nullptr) << bits;
                matched = &candidate;
            }
        }
        ASSERT_EQ(failed, matched == nullptr) << bits;
        if (matched)
            ++hits[matched->label];
        else
            ++impossible;
    }
    unsigned possible = 0;
    for (const auto& candidate : recovery_cases()) {
        EXPECT_GT(hits[candidate.label], 0U) << candidate.label;
        possible += hits[candidate.label];
    }
    EXPECT_EQ(possible, 26U);
    EXPECT_EQ(impossible, 2022U);
}

struct copies final {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};

    wal_prepare prepare(std::string child = assigned_wire()) {
        fragmented_buffer_parser input{buffer(wal_wire(std::move(child)))};
        return decode_wal_prepare(
                 input, wal_expected(), reserve(input, work), work)
          .get()
          .value()
          .value;
    }
    segment_block block(std::string wire, std::uint64_t position = 512) {
        fragmented_buffer_parser input{buffer(wire)};
        return decode_segment_block(
                 input,
                 block_expected(0x30, 1, position, 0, 512),
                 reserve(input, work),
                 work)
          .get()
          .value()
          .value;
    }
};

TEST(RecoveryCasesTest, ExactCopiesAreComparedFieldByField) {
    copies make;
    const auto prepare = make.prepare();
    EXPECT_EQ(
      compare_recovery_copies(prepare, make.block(block_wire())).value(),
      recovery_copy_difference::none);
    EXPECT_EQ(
      compare_recovery_copies(
        prepare,
        make.block(
          block_wire(assigned_wire(), block_expected(0x30, 1, 1024, 0, 512)),
          1024))
        .value(),
      recovery_copy_difference::target);
    // Another request at the same slot.
    EXPECT_EQ(
      compare_recovery_copies(prepare, make.block(data_block(101, 0, 512)))
        .value(),
      recovery_copy_difference::batch_id);
    // The same request, encoded differently.
    EXPECT_EQ(
      compare_recovery_copies(
        prepare, make.block(block_wire(assigned_wire(true))))
        .value(),
      recovery_copy_difference::length);
}

TEST(RecoveryCasesTest, MatrixRowsClassifyFromTheirEvidence) {
    copies make;
    const auto prepare = make.prepare();
    const auto block = make.block(block_wire());
    const auto label = [](const recovery_slot& slot) {
        return std::string{classify_recovery(batch_parameters(slot)).label};
    };
    EXPECT_EQ(label({&prepare, &block, false, true, true, true}), "@satisfied");
    EXPECT_EQ(
      label({&prepare, nullptr, true, true, true, true}), "@satisfied_pinned");
    EXPECT_EQ(label({nullptr, &block, false, true}), "@footer_only");
    EXPECT_EQ(
      label({&prepare, nullptr, false, false, true, true}), "@candidate");
    EXPECT_EQ(
      label({&prepare, &block, false, false, true, true}), "@suffix_sourced");
    EXPECT_EQ(label({nullptr, &block, false, false}), "@suffix_unsourced");
    EXPECT_EQ(
      label(
        {&prepare,
         &block,
         false,
         true,
         true,
         true,
         recovery_copy_difference::child}),
      "@differing_copies");
    EXPECT_EQ(
      label({&prepare, nullptr, false, false, false, true}),
      "@tail_out_of_order");
    EXPECT_EQ(
      label({&prepare, nullptr, false, true, true, false}),
      "@covered_misplaced");
    const auto region = [](bool slack, bool damage, bool certified) {
        return classify_recovery(region_parameters(slack, damage, certified))
          .classification;
    };
    EXPECT_EQ(region(false, false, true), recovery_classification::content);
    EXPECT_EQ(
      region(false, true, false), recovery_classification::uncertified_tail);
    EXPECT_EQ(region(false, true, true), recovery_classification::corruption);
    EXPECT_EQ(region(true, true, false), recovery_classification::slack);
}

TEST(RecoveryCasesTest, PredicatesOfAnAbsentCopyAreFalse) {
    copies make;
    const auto prepare = make.prepare();
    const auto block = make.block(block_wire());
    const auto at = [](recovery_predicate predicate) {
        return static_cast<std::size_t>(predicate);
    };
    const auto orphan = batch_parameters(
      {nullptr, &block, false, true, true, true});
    EXPECT_FALSE(orphan[at(recovery_predicate::ordered)]);
    EXPECT_FALSE(orphan[at(recovery_predicate::placed)]);
    EXPECT_FALSE(orphan[at(recovery_predicate::identical)]);
    const auto alone = batch_parameters(
      {&prepare, nullptr, false, false, true, true});
    EXPECT_TRUE(alone[at(recovery_predicate::ordered)]);
    EXPECT_FALSE(alone[at(recovery_predicate::identical)]);
    EXPECT_FALSE(alone[at(recovery_predicate::region)]);
    const auto tail = region_parameters(false, true, false);
    EXPECT_TRUE(tail[at(recovery_predicate::region)]);
    EXPECT_FALSE(tail[at(recovery_predicate::prepare)]);
    EXPECT_FALSE(tail[at(recovery_predicate::block)]);
}

codec::immutable_object_digest identity(std::string_view bytes) {
    codec::xxh3_128_hasher hash;
    hash.update(bytes.data(), bytes.size());
    return codec::immutable_object_digest{std::move(hash).final()};
}
local_footer_reference footer_at(std::uint64_t position, std::uint16_t family) {
    return local_footer_reference::make(
             runtime::file_position{position},
             byte_count{4_KiB},
             family,
             identity("footer"))
      .value();
}
local_root_reference root_of(local_root_kind kind, std::uint64_t sequence) {
    return local_root_reference::make(
             kind,
             local_object_sequence::make(sequence).value(),
             runtime::file_position{
               kind == local_root_kind::sealed_retry ? 16384U : 0U},
             byte_count{4_KiB},
             page_count::make(1).value(),
             identity("root"))
      .value();
}

TEST(RecoveryPinsTest, PublicationStateAndPinsAgree) {
    const auto valid = [](
                         local_object_state state,
                         std::optional<local_footer_reference> boundary,
                         std::vector<local_root_reference> roots) {
        return detail::validate_recovery_publication(
                 {sc(), state, boundary, std::move(roots)})
          .has_value();
    };
    using enum local_object_state;
    const auto footer = footer_at(12288, 6);
    const auto root = footer_at(16384, 7);
    const auto index = root_of(local_root_kind::index, 42);
    const auto retry = root_of(local_root_kind::sealed_retry, 41);
    const auto snapshot = root_of(
      local_root_kind::completed_retry_snapshot, 40);
    const auto checkpoint = root_of(local_root_kind::checkpoint, 43);
    EXPECT_TRUE(valid(active, std::nullopt, {}));
    EXPECT_TRUE(valid(active, footer, {snapshot}));
    EXPECT_TRUE(valid(recovering, footer, {}));
    // A snapshot root names a footer the publication must pin too.
    EXPECT_FALSE(valid(active, std::nullopt, {snapshot}));
    EXPECT_FALSE(valid(active, root, {}));
    EXPECT_FALSE(valid(recovering, footer, {index}));
    EXPECT_FALSE(valid(active, footer, {retry}));
    EXPECT_TRUE(valid(sealed, root, {retry}));
    EXPECT_TRUE(valid(sealed, root, {index, retry, snapshot}));
    EXPECT_FALSE(valid(sealed, root, {index, snapshot}));
    EXPECT_FALSE(valid(sealed, footer, {retry}));
    EXPECT_FALSE(valid(sealed, std::nullopt, {retry}));
    EXPECT_FALSE(valid(sealed, root, {retry, retry}));
    EXPECT_FALSE(valid(sealed, root, {retry, checkpoint}));
    EXPECT_TRUE(valid(deleting, std::nullopt, {}));
}

TEST(RecoveryCasesTest, ScalarEvidenceMatchesTheCopies) {
    copies make;
    const auto prepare = make.prepare();
    const auto block = make.block(block_wire());
    for (unsigned bits = 0; bits < 64; ++bits) {
        const auto bit = [bits](unsigned i) { return ((bits >> i) & 1U) != 0; };
        const recovery_slot slot{
          bit(0) ? &prepare : nullptr,
          bit(1) ? &block : nullptr,
          bit(2),
          bit(3),
          bit(4),
          bit(5),
          recovery_copy_difference::length};
        const recovery_slot_evidence evidence{
          bit(0),
          bit(1),
          bit(2),
          bit(3),
          bit(4),
          bit(5),
          recovery_copy_difference::length};
        EXPECT_EQ(batch_parameters(slot), batch_parameters(evidence)) << bits;
    }
}

local_segment_descriptor descriptor_of(segment_context context) {
    return {
      context,
      model::range_logical_end{100},
      model::segment_relative_end{},
      alignment(),
      storage_profile::v1,
      1,
      local_layout_kind::initial,
      byte_count{64_MiB},
      runtime::monotonic_duration{3600000000000ULL}};
}
recovery_target target(
  segment_context context,
  local_object_state state,
  std::optional<local_footer_reference> pin = std::nullopt,
  std::uint64_t generation = 1) {
    return {
      descriptor_of(context),
      segment_header::make(context, model::range_logical_end{100}, alignment())
        .value(),
      state,
      local_publication_generation::make(generation).value(),
      pin};
}

TEST(RecoveryMergeTest, TargetsAreIndependentAndConsistent) {
    using enum local_object_state;
    const auto valid = [](std::vector<recovery_target> targets) {
        return detail::validate_recovery_targets(targets).has_value();
    };
    const auto footer = footer_at(12288, 6);
    const auto root = footer_at(16384, 7);
    EXPECT_TRUE(
      valid({target(sc(), active), target(sc(0x31), recovering, footer)}));
    EXPECT_TRUE(valid({target(sc(), sealed, root)}));
    EXPECT_FALSE(valid({target(sc(), active), target(sc(), recovering)}));
    EXPECT_FALSE(valid({target(sc(), sealed)}));
    EXPECT_FALSE(valid({target(sc(), sealed, footer)}));
    EXPECT_FALSE(valid({target(sc(), active, root)}));
    EXPECT_FALSE(valid({target(sc(), deleting)}));
    auto mismatched = target(sc(), active);
    mismatched.header = segment_header::make(
                          sc(0x31), model::range_logical_end{100}, alignment())
                          .value();
    EXPECT_FALSE(valid({mismatched}));
}

TEST(RecoveryMergeTest, ResumeBindingCoversEveryInput) {
    using enum local_object_state;
    local_shard_control control{
      local_wal_high{}.checked_advance(1).value(),
      local_object_high{},
      local_decision_high{},
      local_deletion_high{},
      {},
      local_wal_head{id<model::wal_incarnation_id>(0x70), identity("header")}};
    const std::vector targets{
      target(sc(), active, footer_at(12288, 6)), target(sc(0x31), active)};
    const auto base = detail::recovery_binding(
      control, std::nullopt, targets, 128);
    EXPECT_EQ(
      base, detail::recovery_binding(control, std::nullopt, targets, 128));
    auto other_head = control;
    other_head.wal_head->header_digest = identity("other header");
    EXPECT_NE(
      base, detail::recovery_binding(other_head, std::nullopt, targets, 128));
    const auto cutoff = local_wal_cursor::make(
      id<model::wal_incarnation_id>(0x70), runtime::file_position{4096});
    EXPECT_NE(base, detail::recovery_binding(control, *cutoff, targets, 128));
    // The queue bound decides where a full queue's slots fall.
    EXPECT_NE(
      base, detail::recovery_binding(control, std::nullopt, targets, 64));
    for (std::size_t field = 0; field < 4; ++field) {
        auto changed = targets;
        if (field == 0) changed[0].state = recovering;
        if (field == 1)
            changed[0].generation
              = local_publication_generation::make(2).value();
        if (field == 2) changed[0].pin = footer_at(16384, 6);
        if (field == 3) changed[1].pin = footer_at(12288, 6);
        EXPECT_NE(
          base, detail::recovery_binding(control, std::nullopt, changed, 128))
          << field;
    }
}

recovery_segment_report report_of(
  local_object_state state,
  std::optional<local_footer_reference> pin,
  std::optional<std::uint64_t> boundary) {
    recovery_segment_report report{sc(), state, {}, pin};
    if (boundary)
        report.boundary = segment_scan_boundary{
          footer_at(*boundary, state == local_object_state::sealed ? 7 : 6),
          {scope(100, 102, 0, 2, 512, *boundary), 2, std::nullopt, 0}};
    report.scanned = state != local_object_state::sealed;
    return report;
}

// Startup plans never remove bytes: damaged certified history and conflicts
// stop their scope, a tail keeps its candidates and suffix for a supplied
// decision, and a recovering publication is never lowered.
TEST(RecoveryPlanTest, DamageStopsAndTailsArePublishedNeverTruncated) {
    using enum local_object_state;
    using enum recovery_plan_action;
    const auto pin = footer_at(12288, 6);
    const auto planned = [](const recovery_segment_report& report) {
        return detail::plan_recovery_segment(report).value();
    };
    auto corrupt = report_of(active, pin, std::nullopt);
    corrupt.verdict = segment_scan_verdict::corrupt;
    corrupt.stop = segment_scan_stop::corrupt;
    EXPECT_EQ(planned(corrupt).action, stop);
    EXPECT_EQ(planned(corrupt).reason, recovery_plan_reason::corruption);
    auto conflict = report_of(active, pin, 16384);
    conflict.conflicts = 1;
    EXPECT_EQ(planned(conflict).reason, recovery_plan_reason::conflict);
    auto misplaced = report_of(recovering, pin, 12288);
    misplaced.misplaced = 1;
    EXPECT_EQ(planned(misplaced).reason, recovery_plan_reason::misplaced);

    auto tail = report_of(active, pin, 16384);
    tail.verdict = segment_scan_verdict::uncertified_tail;
    tail.stop = segment_scan_stop::unwritten;
    tail.candidates = 2;
    tail.suffix = 1;
    const auto kept = planned(tail);
    EXPECT_EQ(kept.action, publish_recovering);
    EXPECT_EQ(kept.boundary, footer_at(16384, 6));
    EXPECT_EQ(kept.candidates, 2U);
    EXPECT_EQ(kept.suffix, 1U);
    const auto empty = planned(report_of(active, std::nullopt, std::nullopt));
    EXPECT_EQ(empty.action, publish_recovering);
    EXPECT_FALSE(empty.boundary);
    // Already pinned there: nothing to publish. Higher: published.
    EXPECT_EQ(planned(report_of(recovering, pin, 12288)).action, retain);
    EXPECT_EQ(
      planned(report_of(recovering, pin, 16384)).action, publish_recovering);
    EXPECT_EQ(
      planned(report_of(active, pin, 12288)).action, publish_recovering);
    EXPECT_EQ(
      planned(report_of(sealed, footer_at(16384, 7), std::nullopt)).action,
      retain);
    // A boundary below its pin cannot come from a walk that starts there.
    const auto lowered = detail::plan_recovery_segment(
      report_of(recovering, footer_at(16384, 6), 12288));
    ASSERT_FALSE(lowered);
    EXPECT_EQ(lowered.error().code(), errc::invariant_violation);

    wal_scan_result scan;
    scan.complete = true;
    EXPECT_EQ(detail::plan_recovery_wal(scan, false).action, retain);
    scan.content_end = local_wal_cursor::make(
                         id<model::wal_incarnation_id>(0x70),
                         runtime::file_position{8192})
                         .value();
    const auto successor = detail::plan_recovery_wal(scan, false);
    EXPECT_EQ(successor.action, activate_successor);
    EXPECT_EQ(successor.predecessor, scan.content_end);
    EXPECT_EQ(detail::plan_recovery_wal(scan, true).action, stop);
    scan.verdict = wal_scan_verdict::corrupt;
    EXPECT_EQ(
      detail::plan_recovery_wal(scan, false).reason,
      recovery_plan_reason::corruption);
}

// A segment's suffix plan identity is the canonical framing of every PREPARE
// that names it, in WAL order, under the decided end, and of nothing else.
TEST(RecoveryDecisionTest, SuffixIdentityFramesThePlanCanonically) {
    const auto wal = id<model::wal_incarnation_id>(0x70);
    const auto at = [&wal](std::uint64_t position) {
        return local_wal_cursor::make(wal, runtime::file_position{position})
          .value();
    };
    const std::array prepares{
      recovery_suffix_prepare{
        at(4096),
        at(8192),
        runtime::file_position{512},
        identity("first"),
        nullptr},
      recovery_suffix_prepare{
        at(8192),
        at(12288),
        runtime::file_position{1024},
        identity("second"),
        nullptr}};
    const auto plan = [](
                        const segment_context& segment,
                        std::uint64_t end,
                        std::span<const recovery_suffix_prepare> entries) {
        recovery_suffix_identity hash{segment, runtime::file_position{end}};
        for (const auto& entry : entries)
            hash.add(entry);
        return std::move(hash).finish();
    };
    std::string framed{"KQ/RECOVERY-SUFFIX-PLAN/1"};
    const auto le = [&framed](std::uint64_t value) {
        for (std::size_t i = 0; i < 8; ++i)
            framed.push_back(static_cast<char>(value >> (8U * i)));
    };
    const auto raw = [&framed](const auto& bytes) {
        for (const auto byte : bytes)
            framed.push_back(static_cast<char>(byte));
    };
    const auto segment = sc();
    const auto cluster = segment.cluster();
    const auto topic = segment.topic();
    const auto range = segment.range();
    const auto number = segment.segment();
    raw(cluster.bytes());
    raw(topic.bytes());
    raw(range.bytes());
    raw(number.bytes());
    le(1);
    le(1536);
    for (const auto& entry : prepares) {
        raw(wal.bytes());
        le(entry.begin.position().value());
        le(entry.end.position().value());
        le(entry.target.value());
        raw(entry.digest.bytes());
    }
    le(2);
    const auto base = plan(sc(), 1536, prepares);
    EXPECT_EQ(base, identity(framed));
    EXPECT_NE(base, plan(sc(), 2048, prepares));
    EXPECT_NE(base, plan(sc(0x31), 1536, prepares));
    EXPECT_NE(base, plan(sc(), 1536, std::span{prepares}.first(1)));
    const std::array reversed{prepares[1], prepares[0]};
    EXPECT_NE(base, plan(sc(), 1536, reversed));
    auto other = prepares;
    other[1].digest = identity("another second");
    EXPECT_NE(base, plan(sc(), 1536, other));
}

// One seal resolves every candidate of its segment: those below the end are
// reconstructed and the rest discarded; preserving one contradicts it.
TEST(RecoveryDecisionTest, CandidatesAgreeWithTheirSegmentSeal) {
    local_recovery_decision candidate{
      local_wal_cursor::make(
        id<model::wal_incarnation_id>(0x70), runtime::file_position{4096})
        .value(),
      identity("prepare"),
      sc(),
      runtime::file_position{4096},
      local_recovery_action::reconstruct,
      1};
    const auto agrees = [&candidate](std::uint64_t end) {
        return detail::recovery_decisions_agree(
          candidate, runtime::file_position{end});
    };
    EXPECT_TRUE(agrees(8192));
    EXPECT_FALSE(agrees(4096));
    candidate.action = local_recovery_action::discard;
    EXPECT_TRUE(agrees(4096));
    EXPECT_FALSE(agrees(8192));
    candidate.action = local_recovery_action::preserve;
    EXPECT_FALSE(agrees(4096));
    EXPECT_FALSE(agrees(8192));
}

// A failed read of authoritative metadata says what restart may conclude; a
// failure of the operation itself concludes nothing. A store that must exist
// and is pristine is lost, never empty.
TEST(RecoveryInventoryTest, VerdictsClassifyFailuresAndStates) {
    EXPECT_EQ(
      recovery_failure_verdict(errc::unsupported_format),
      recovery_store_verdict::unsupported);
    for (const auto code :
         {errc::malformed_data,
          errc::corrupt_data,
          errc::wrong_context,
          errc::truncated_data})
        EXPECT_EQ(
          recovery_failure_verdict(code), recovery_store_verdict::corrupt);
    EXPECT_EQ(
      recovery_failure_verdict(errc::not_found), recovery_store_verdict::lost);
    for (const auto code :
         {errc::io_failure, errc::unavailable, errc::permission_denied})
        EXPECT_EQ(
          recovery_failure_verdict(code), recovery_store_verdict::unavailable);
    for (const auto code :
         {errc::resource_exhausted,
          errc::aborted,
          errc::invalid_argument,
          errc::closed,
          errc::queue_full})
        EXPECT_FALSE(recovery_failure_verdict(code)) << static_cast<int>(code);
    EXPECT_EQ(
      recovery_state_verdict(local_store_state::existing),
      recovery_store_verdict::ready);
    EXPECT_EQ(
      recovery_state_verdict(local_store_state::pristine),
      recovery_store_verdict::lost);
    EXPECT_EQ(
      recovery_state_verdict(local_store_state::incomplete),
      recovery_store_verdict::incomplete);
    EXPECT_EQ(
      recovery_state_verdict(local_store_state::unsupported),
      recovery_store_verdict::unsupported);
    EXPECT_EQ(
      recovery_state_verdict(local_store_state::corrupt),
      recovery_store_verdict::corrupt);
    // The set takes its most severe device.
    EXPECT_LT(
      recovery_store_verdict::ready, recovery_store_verdict::unavailable);
    EXPECT_LT(
      recovery_store_verdict::unavailable, recovery_store_verdict::incomplete);
    EXPECT_LT(recovery_store_verdict::incomplete, recovery_store_verdict::lost);
    EXPECT_LT(
      recovery_store_verdict::lost, recovery_store_verdict::unsupported);
    EXPECT_LT(
      recovery_store_verdict::unsupported, recovery_store_verdict::corrupt);
}

// A durable seal decision is pending until its segment is sealed at its end:
// another segment's seal or a candidate decision is not one, the same
// decision twice resumes from its first record, and two different seals of
// one segment, or a seal somewhere else, contradict it.
TEST(RecoveryInventoryTest, PendingSealIsTheDecisionNotYetCarriedOut) {
    const auto seal = [](
                        std::uint64_t sequence,
                        std::uint64_t end,
                        std::uint64_t owner = 7,
                        segment_context segment = sc()) {
        return recovery_decision_record{
          {local_decision_sequence::make(sequence).value(),
           identity("record"),
           byte_count{4096}},
          {local_wal_cursor::make(
             id<model::wal_incarnation_id>(0x70), runtime::file_position{8192})
             .value(),
           identity("plan"),
           segment,
           runtime::file_position{end},
           local_recovery_action::seal_at,
           owner}};
    };
    local_object_publication publication{
      sc(), local_object_state::active, std::nullopt, {}};
    const auto pending =
      [&publication](std::span<const recovery_decision_record> held) {
          return detail::pending_recovery_seal(publication, held);
      };
    EXPECT_EQ(pending({}).value(), std::nullopt);
    auto candidate = seal(3, 4096);
    candidate.value.action = local_recovery_action::reconstruct;
    const std::array unrelated{seal(2, 4096, 7, sc(0x31)), candidate};
    EXPECT_EQ(pending(unrelated).value(), std::nullopt);
    const std::array twice{seal(9, 8192), seal(4, 8192)};
    EXPECT_EQ(pending(twice).value(), twice[1].pin);
    publication.state = local_object_state::recovering;
    EXPECT_EQ(pending(twice).value(), twice[1].pin);
    for (const auto& other : {seal(5, 4096), seal(5, 8192, 8)}) {
        const std::array conflicting{seal(4, 8192), other};
        const auto refused = pending(conflicting);
        ASSERT_FALSE(refused);
        EXPECT_EQ(refused.error().code(), errc::wrong_context);
    }
    publication.state = local_object_state::sealed;
    publication.boundary
      = local_footer_reference::make(
          runtime::file_position{8192}, byte_count{4096}, 7, identity("root"))
          .value();
    EXPECT_EQ(pending(twice).value(), std::nullopt);
    publication.boundary
      = local_footer_reference::make(
          runtime::file_position{4096}, byte_count{4096}, 7, identity("root"))
          .value();
    const auto elsewhere = pending(twice);
    ASSERT_FALSE(elsewhere);
    EXPECT_EQ(elsewhere.error().code(), errc::wrong_context);
    publication.state = local_object_state::deleting;
    publication.boundary.reset();
    EXPECT_EQ(pending(twice).value(), std::nullopt);
}

// Reads stop at the smaller of the supplied visible end and the recovered
// boundary, never before the origin; without a supplied end nothing is
// readable. A visible end past the boundary is reported, not lowered.
TEST(RecoveryPublicationTest, ReadsHonourTheSuppliedVisibleEnd) {
    using end = model::range_logical_end;
    const end origin{100}, recovered{102};
    const auto bound = [](end value, bool short_of) {
        return recovery_read_bound{value, short_of};
    };
    EXPECT_EQ(
      bound_recovered_reads(origin, recovered, std::nullopt),
      bound(origin, false));
    EXPECT_EQ(
      bound_recovered_reads(origin, recovered, end{101}),
      bound(end{101}, false));
    EXPECT_EQ(
      bound_recovered_reads(origin, recovered, recovered),
      bound(recovered, false));
    EXPECT_EQ(
      bound_recovered_reads(origin, recovered, end{105}),
      bound(recovered, true));
    EXPECT_EQ(
      bound_recovered_reads(origin, recovered, end{90}), bound(origin, false));
    // Without a boundary nothing past the origin is certified.
    EXPECT_EQ(
      bound_recovered_reads(origin, origin, end{101}), bound(origin, true));
    EXPECT_TRUE(recovered_publication_limits{}.validate());
    EXPECT_TRUE(recovered_publication_limits{maximum_recovery_jobs}.validate());
    EXPECT_FALSE(recovered_publication_limits{0}.validate());
    EXPECT_FALSE(
      recovered_publication_limits{maximum_recovery_jobs + 1}.validate());
}

// Open segment walks, slots waiting for coverage and obligation rows each
// have a bound, never none and never zero.
TEST(RecoveryMergeTest, LimitsBoundWalksAndHeldState) {
    const auto bounded = [] {
        recovery_merge_limits limits;
        limits.wal.metadata.charge = &bytes::testing::charge;
        limits.segment.metadata.charge = &bytes::testing::charge;
        return limits;
    };
    EXPECT_TRUE(bounded().validate());
    for (const std::uint32_t cursors : {1U, maximum_recovery_cursors}) {
        auto limits = bounded();
        limits.cursors = cursors;
        EXPECT_TRUE(limits.validate()) << cursors;
    }
    for (const std::uint32_t cursors : {0U, maximum_recovery_cursors + 1}) {
        auto limits = bounded();
        limits.cursors = cursors;
        EXPECT_FALSE(limits.validate()) << cursors;
    }
    auto limits = bounded();
    limits.pending = 0;
    EXPECT_FALSE(limits.validate());
    limits = bounded();
    limits.obligations = 0;
    EXPECT_FALSE(limits.validate());
    // Each fixed table is one contiguous allocation: the largest that fits is
    // valid and one row more is not.
    limits = bounded();
    limits.obligations = static_cast<std::uint32_t>(
      maximum_contiguous_allocation_bytes
      / sizeof(detail::recovery_obligation_row));
    EXPECT_TRUE(limits.validate());
    ++limits.obligations;
    EXPECT_FALSE(limits.validate());
    limits = bounded();
    limits.pending = static_cast<std::uint32_t>(
      maximum_contiguous_allocation_bytes / sizeof(detail::recovery_queued));
    EXPECT_TRUE(limits.validate());
    ++limits.pending;
    EXPECT_FALSE(limits.validate());
}

} // namespace
} // namespace kwaque::storage
