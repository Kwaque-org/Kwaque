#pragma once

#include "src/storage/recovery_inventory.h"
#include "src/storage/recovery_plan.h"
#include "src/storage/recovery_publication.h"
#include "src/storage/recovery_seal.h"
#include "src/storage/tests/retry_test_support.h"
#include "src/storage/tests/segment_scan_contract.h"
#include "src/storage/tests/wal_test_support.h"

#include <seastar/util/alloc_failure_injector.hh>

#include <algorithm>
#include <bit>
#include <limits>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace kwaque::storage::testing::recovery_contract {
using installation_contract::descriptor;
using installation_contract::segment;
using installation_contract::segment_head;
using reader_contract::active_boundary;
using reader_contract::chain_control;
using reader_contract::put_wal;
using reader_contract::sealed_boundary;
using reader_contract::wal;
using scan_contract::descriptor_b;
using scan_contract::head_control;
using scan_contract::interleaved;
using scan_contract::seed_targets;
using scan_contract::segment_b;
using scan_contract::segment_head_b;
using segment_scan_contract::active_a;
using segment_scan_contract::block_a;
using segment_scan_contract::data_path;
using store_contract::limits;
using store_contract::read_bytes;
using store_contract::require;
using store_contract::take;
using store_contract::write_bytes;

inline recovery_target target_a(
  std::optional<local_footer_reference> pin = std::nullopt,
  local_object_state state = local_object_state::active,
  std::uint64_t generation = 1) {
    return {
      descriptor(),
      segment_head(),
      state,
      local_publication_generation::make(generation).value(),
      pin};
}
inline recovery_target target_b() {
    return {
      descriptor_b(),
      segment_head_b(),
      local_object_state::active,
      local_publication_generation::make(1).value(),
      std::nullopt};
}
inline recovery_merge_limits merge_limits(std::uint32_t cursors = 2) {
    return {
      scan_contract::scan_limits(),
      segment_scan_contract::scan_limits(),
      cursors,
      16,
      8};
}
inline std::string segment_b_bytes() {
    return local_fixture::read("data_header_b")
           + local_fixture::read("block_b1") + local_fixture::read("footer_b");
}
// A PREPARE of the head file wal(1), independently framed as the fixtures
// are.
inline std::string prepare_at(
  std::string child,
  std::uint64_t wal_position,
  segment_context target,
  std::uint64_t physical,
  std::uint64_t position) {
    return wal_wire(
      std::move(child),
      {wal_write_context::make(
         wal(1), alignment(4096), runtime::file_position{wal_position})
         .value(),
       segment_write_context::make(
         target,
         alignment(4096),
         model::segment_relative_end{physical},
         runtime::file_position{position})
         .value(),
       runtime::file_position{4096},
       model::range_routing_epoch::make(1).value(),
       batch_expected()});
}

inline std::string name(const segment_context& value) {
    if (value == segment()) return "A";
    if (value == segment_b()) return "B";
    return "?";
}
inline std::string number(std::uint64_t value) { return std::to_string(value); }
inline std::string stop_name(segment_scan_stop stop) {
    switch (stop) {
    case segment_scan_stop::end:
        return "end";
    case segment_scan_stop::unwritten:
        return "unwritten";
    case segment_scan_stop::torn:
        return "torn";
    case segment_scan_stop::corrupt:
        return "corrupt";
    case segment_scan_stop::malformed:
        return "malformed";
    case segment_scan_stop::foreign:
        return "foreign";
    }
    return "?";
}
inline std::string verdict_name(segment_scan_verdict verdict) {
    switch (verdict) {
    case segment_scan_verdict::clean:
        return "clean";
    case segment_scan_verdict::uncertified_tail:
        return "tail";
    case segment_scan_verdict::corrupt:
        return "corrupt";
    }
    return "?";
}
// One line per item, so whole enumerations compare as text.
inline std::string describe(const recovery_item& item) {
    return std::visit(
      [](const auto& value) -> std::string {
          using T = std::remove_cvref_t<decltype(value)>;
          if constexpr (std::is_same_v<T, recovery_slot_report>) {
              auto out = "slot " + name(value.segment) + " "
                         + number(value.position.value()) + " "
                         + std::string{value.matched->label};
              if (value.begin)
                  out += " w" + number(value.begin->position().value());
              return out;
          } else if constexpr (std::is_same_v<T, recovery_unresolved_report>) {
              return "unresolved w" + number(value.begin.position().value())
                     + (value.resolution == wal_target_resolution::unknown ? " unknown" : " misplaced");
          } else if constexpr (std::is_same_v<T, recovery_region_report>) {
              return "region "
                     + (value.wal ? "wal" + number(value.wal->bytes()[15])
                                  : name(*value.segment))
                     + " " + number(value.begin.value()) + "-"
                     + number(value.end.value()) + " "
                     + std::string{value.matched->label};
          } else if constexpr (std::is_same_v<T, recovery_segment_report>) {
              return "segment " + name(value.segment) + " boundary="
                     + (value.boundary
                          ? number(value.boundary->footer.position().value())
                          : std::string{"-"})
                     + " " + (value.scanned ? stop_name(value.stop) : "cold")
                     + " " + verdict_name(value.verdict)
                     + " sat=" + number(value.satisfied)
                     + " fo=" + number(value.footer_only)
                     + " cand=" + number(value.candidates)
                     + " suf=" + number(value.suffix)
                     + " conf=" + number(value.conflicts)
                     + " unres=" + number(value.unresolved);
          } else {
              return "obligation " + name(value.segment)
                     + " sat=" + number(value.satisfied)
                     + " pinned=" + number(value.pinned)
                     + (value.first_pinned
                          ? " first=w"
                              + number(value.first_pinned->position().value())
                          : std::string{});
          }
      },
      item);
}

// Records every item, observes it in a planner when one is supplied, and
// stops after `stop_after` items.
struct record final {
    std::vector<std::string>* out;
    recovery_planner* planner{nullptr};
    std::size_t stop_after{std::numeric_limits<std::size_t>::max()};
    std::size_t* seen;
    seastar::future<runtime::result<bool>>
    operator()(const recovery_item& item) const {
        out->push_back(describe(item));
        if (planner)
            if (auto observed = planner->observe(item); !observed)
                return seastar::make_ready_future<runtime::result<bool>>(
                  runtime::failure(observed.error()));
        ++*seen;
        return seastar::make_ready_future<runtime::result<bool>>(
          *seen < stop_after);
    }
};

template<typename Backend, typename Owner, typename Driver>
seastar::future<runtime::result<recovery_merge_result>> merge(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  std::span<const recovery_target> targets,
  std::vector<std::string>& out,
  Driver drive,
  std::uint32_t cursors = 2,
  std::optional<recovery_resume> resume = std::nullopt,
  std::size_t stop_after = std::numeric_limits<std::size_t>::max(),
  const local_shard_control& control = head_control(),
  recovery_planner* planner = nullptr) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array specs{spec};
    std::size_t seen = 0;
    co_return co_await drive.lifecycle(reconcile_local_recovery(
      files,
      owner,
      std::span<const local_device_spec>{specs},
      spec,
      0,
      control,
      std::nullopt,
      targets,
      budget,
      merge_limits(cursors),
      work,
      record{&out, planner, stop_after, &seen},
      resume));
}

// A3's PREPARE, with the later fixtures.
inline std::string prepare_a3();

// The store merge_pass reads: segments A and B interleaved in one head.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> seed_interleaved(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await seed_targets(files, owner, spec, budget, drive);
    co_await write_bytes(files, data_path(spec, segment()), active_a(), drive);
    co_await write_bytes(
      files, data_path(spec, segment_b()), segment_b_bytes(), drive);
    co_await put_wal(files, spec, wal(1), interleaved(), drive);
}

// A read that fails is unavailability, never an end: the merge of
// seed_interleaved's store fails with the read's own error once a read of the
// file whose regions are `regions` fails, and nothing classifies its bytes.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> failed_read(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  std::string_view regions,
  Driver drive) {
    const std::array targets{target_a(), target_b()};
    std::vector<std::string> out;
    auto merged = co_await merge(
      files, owner, spec, budget, targets, out, drive);
    require(
      !merged && merged.error().code() == errc::io_failure
        && recovery_failure_verdict(merged.error().code())
             == recovery_store_verdict::unavailable
        && std::none_of(
          out.begin(),
          out.end(),
          [regions](const auto& line) { return line.starts_with(regions); }),
      "a failed read ended a file's content");
}

// One pass over the WAL: every copy is joined, obligation rows are rebuilt,
// queued PREPAREs read each segment once however few walks are open, eviction
// changes nothing but the reload count, and a stopped enumeration resumes
// exactly where it stopped while its inputs are unchanged.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> merge_pass(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await seed_targets(files, owner, spec, budget, drive);
    require(
      prepare_at(local_fixture::read("child_a1"), 4096, segment(), 0, 4096)
          == local_fixture::read("prepare_a1")
        && prepare_at(
             local_fixture::read("child_a2"), 12288, segment(), 1, 8192)
             == local_fixture::read("prepare_a2"),
      "PREPAREs are not framed as the fixtures");
    co_await write_bytes(files, data_path(spec, segment()), active_a(), drive);
    co_await write_bytes(
      files, data_path(spec, segment_b()), segment_b_bytes(), drive);
    co_await put_wal(files, spec, wal(1), interleaved(), drive);
    const std::array targets{target_a(), target_b()};
    const std::vector<std::string> expected{
      "region wal1 4096-16384 @content",
      "slot A 4096 @satisfied w4096",
      "slot A 8192 @satisfied w12288",
      "region A 4096-16384 @content",
      "segment A boundary=12288 end clean sat=2 fo=0 cand=0 suf=0 conf=0 "
      "unres=0",
      "slot B 4096 @satisfied w8192",
      "region B 4096-12288 @content",
      "segment B boundary=8192 end clean sat=1 fo=0 cand=0 suf=0 conf=0 "
      "unres=0",
      "obligation A sat=2 pinned=0",
      "obligation B sat=1 pinned=0"};
    std::vector<std::string> out;
    auto result = take(
      co_await merge(files, owner, spec, budget, targets, out, drive));
    require(
      out == expected && !result.resume && result.delivered == expected.size()
        && result.reloads == 0 && result.wal.prepares == 3
        && result.wal.content_end
        && result.wal.content_end->position().value() == 16384,
      "one merge pass did not join every copy");
    // One open walk: each segment's PREPAREs wait in its queue, so the
    // interleaved WAL still reads each segment once, in one run.
    out.clear();
    result = take(
      co_await merge(files, owner, spec, budget, targets, out, drive, 1));
    require(
      out == expected && result.reloads == 0,
      "one walk changed the merge or reread a segment");

    // A stopped enumeration resumes after its last delivered item.
    out.clear();
    result = take(
      co_await merge(
        files, owner, spec, budget, targets, out, drive, 1, std::nullopt, 3));
    require(
      out.size() == 3 && result.resume && result.resume->items == 3
        && result.delivered == 3,
      "a stopped enumeration did not return its resume point");
    const auto token = *result.resume;
    result = take(
      co_await merge(
        files, owner, spec, budget, targets, out, drive, 2, token));
    require(
      out == expected && !result.resume
        && result.delivered == expected.size() - 3,
      "a resumed enumeration repeated or lost items");
    // A token binds every object and generation it was derived from.
    const std::array republished{
      target_a(std::nullopt, local_object_state::active, 2), target_b()};
    auto stale = co_await merge(
      files, owner, spec, budget, republished, out, drive, 2, token);
    require(
      !stale && stale.error().code() == errc::wrong_context,
      "a token survived a new publication generation");
    auto other_head = head_control();
    other_head.wal_head->header_digest = codec::immutable_object_digest{
      exact_digest("another header")};
    stale = co_await merge(
      files,
      owner,
      spec,
      budget,
      targets,
      out,
      drive,
      2,
      token,
      std::numeric_limits<std::size_t>::max(),
      other_head);
    require(
      !stale && stale.error().code() == errc::wrong_context,
      "a token survived another WAL head");
    require(
      (co_await read_bytes(files, data_path(spec, segment()), drive))
        == active_a(),
      "merging changed segment bytes");

    // A full queue runs its walk during the scan. With B first in target
    // order and one walk open, B's walk closes A's, and A's reopens at its
    // last verified footer: only the reload count differs.
    co_await write_bytes(
      files,
      data_path(spec, segment()),
      active_a() + std::string(4096, '\0'),
      drive);
    co_await put_wal(files, spec, wal(1), interleaved() + prepare_a3(), drive);
    const std::array reordered{target_b(), target_a()};
    const auto control = head_control();
    const std::array specs{spec};
    std::array<std::vector<std::string>, 2> served;
    std::array<std::uint32_t, 2> reloads{};
    for (const std::uint32_t cursors : {1U, 2U}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto limits = merge_limits(cursors);
        limits.pending = 2;
        std::size_t seen = 0;
        auto& out = served[cursors - 1];
        reloads[cursors - 1] = take(
                                 co_await drive.lifecycle(
                                   reconcile_local_recovery(
                                     files,
                                     owner,
                                     std::span<const local_device_spec>{specs},
                                     spec,
                                     0,
                                     control,
                                     std::nullopt,
                                     reordered,
                                     budget,
                                     limits,
                                     work,
                                     record{
                                       &out,
                                       nullptr,
                                       std::numeric_limits<std::size_t>::max(),
                                       &seen})))
                                 .reloads;
    }
    require(
      served[0] == served[1] && reloads[0] == 1 && reloads[1] == 0
        && std::count_if(
             served[0].begin(),
             served[0].end(),
             [](const auto& line) { return line.starts_with("slot A 16384 "); })
             == 1,
      "a walk closed by another changed the merge or was not reopened");
}

// Per-segment order, pinned evidence, sealed extents, candidates, suffixes
// and damage, each from scanned bytes.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> merge_cases(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await seed_targets(files, owner, spec, budget, drive);
    const auto a = data_path(spec, segment());
    co_await write_bytes(
      files, data_path(spec, segment_b()), segment_b_bytes(), drive);
    const auto header = local_fixture::read("wal_header");
    const auto header_a = local_fixture::read("data_header_a");
    struct merge_case final {
        std::string segment_a, wal;
        std::optional<local_footer_reference> pin;
        local_object_state state;
        std::vector<std::string> a_items;
        const char* message;
    };
    const std::array cases{
      // A2's PREPARE before A1's: the earlier target is out of WAL order.
      merge_case{
        active_a(),
        header
          + prepare_at(
            local_fixture::read("child_a2"), 4096, segment(), 1, 8192)
          + prepare_at(
            local_fixture::read("child_a1"), 8192, segment(), 0, 4096),
        std::nullopt,
        local_object_state::active,
        {"slot A 4096 @block_out_of_order w8192",
         "slot A 4096 @footer_only",
         "slot A 8192 @satisfied w4096",
         "region A 4096-16384 @content",
         "segment A boundary=12288 end clean sat=1 fo=1 cand=0 suf=0 conf=1 "
         "unres=0"},
        "a non-increasing target was not a conflict"},
      // Slots inside the pinned footer's history get a layout check only.
      merge_case{
        active_a(),
        interleaved(),
        active_boundary(),
        local_object_state::active,
        {"slot A 4096 @satisfied_pinned w4096",
         "slot A 8192 @satisfied_pinned w12288",
         "region A 4096-16384 @content",
         "segment A boundary=12288 end clean sat=2 fo=0 cand=0 suf=0 conf=0 "
         "unres=0"},
        "pinned evidence was scanned again"},
      // A sealed extent is cold: its root pins every slot below it.
      merge_case{
        active_a() + local_fixture::read("sealed_a"),
        interleaved(),
        sealed_boundary(),
        local_object_state::sealed,
        {"slot A 4096 @satisfied_pinned w4096",
         "slot A 8192 @satisfied_pinned w12288",
         "segment A boundary=- cold clean sat=2 fo=0 cand=0 suf=0 conf=0 "
         "unres=0"},
        "a sealed extent was read or its slots lost"},
      // No surviving block: both PREPAREs are candidates.
      merge_case{
        header_a,
        interleaved(),
        std::nullopt,
        local_object_state::active,
        {"slot A 4096 @candidate w4096",
         "slot A 8192 @candidate w12288",
         "segment A boundary=- end clean sat=0 fo=0 cand=2 suf=0 conf=0 "
         "unres=0"},
        "PREPAREs without blocks were not candidates"},
      // A1's block survives without a footer, then zeros: an unresolved
      // suffix with its source, a candidate and an uncertified tail.
      merge_case{
        header_a + local_fixture::read("block_a1") + std::string(4096, '\0'),
        interleaved(),
        std::nullopt,
        local_object_state::active,
        {"slot A 4096 @suffix_sourced w4096",
         "slot A 8192 @candidate w12288",
         "region A 4096-8192 @content",
         "region A 8192-12288 @uncertified_tail",
         "segment A boundary=- unwritten tail sat=0 fo=0 cand=1 suf=1 conf=0 "
         "unres=0"},
        "a suffix before damage was not classified"},
      // A PREPARE naming the slot a verified footer occupies.
      merge_case{
        active_a(),
        header + local_fixture::read("prepare_a1")
          + prepare_at(
            local_fixture::read("child_a2"), 8192, segment(), 1, 12288),
        std::nullopt,
        local_object_state::active,
        {"slot A 4096 @satisfied w4096",
         "slot A 8192 @footer_only",
         "slot A 12288 @covered_misplaced w8192",
         "region A 4096-16384 @content",
         "segment A boundary=12288 end clean sat=1 fo=1 cand=0 suf=0 conf=1 "
         "unres=0"},
        "a PREPARE naming a footer's slot was not a conflict"},
    };
    for (const auto& test : cases) {
        co_await write_bytes(files, a, test.segment_a, drive);
        co_await put_wal(files, spec, wal(1), test.wal, drive);
        const std::array targets{target_a(test.pin, test.state), target_b()};
        std::vector<std::string> out;
        take(co_await merge(files, owner, spec, budget, targets, out, drive));
        std::vector<std::string> a_items;
        for (const auto& line : out)
            if (
              line.starts_with("slot A") || line.starts_with("region A")
              || line.starts_with("segment A"))
                a_items.push_back(line);
        require(a_items == test.a_items, test.message);
        require(
          (co_await read_bytes(files, a, drive)) == test.segment_a,
          "merging changed segment bytes");
    }
    // The same request encoded differently at A2's slot: the merge compares
    // the two copies' exact children and reports them differing.
    co_await write_bytes(
      files,
      a,
      header_a + local_fixture::read("block_a1") + block_a(101, 1, 8192, true)
        + local_fixture::read("footer_a"),
      drive);
    co_await put_wal(files, spec, wal(1), interleaved(), drive);
    const std::array targets{target_a(), target_b()};
    std::vector<std::string> out;
    take(co_await merge(files, owner, spec, budget, targets, out, drive));
    require(
      std::ranges::find(out, "slot A 8192 @differing_copies w12288")
        != out.end(),
      "differing copies of one slot were not compared");
}

template<typename Backend, typename Owner, typename Driver>
seastar::future<recovery_planner> plan(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  std::span<const recovery_target> targets,
  Driver drive,
  const local_shard_control& control = head_control()) {
    auto planner = take(recovery_planner::make(targets, budget));
    std::vector<std::string> out;
    auto merged = take(
      co_await merge(
        files,
        owner,
        spec,
        budget,
        targets,
        out,
        drive,
        2,
        std::nullopt,
        std::numeric_limits<std::size_t>::max(),
        control,
        &planner));
    take(planner.finish(merged));
    co_return std::move(planner);
}

// Restart plans from damaged bytes. Where a log repair truncates at the first
// bad record and accepts the shorter log, the plan keeps every byte: the
// head's content ends at its first damage and a successor continues after it,
// later intact PREPAREs stay uncertified survivals, and damage below
// certification stops instead of becoming a shorter history.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> plan_cases(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await seed_targets(files, owner, spec, budget, drive);
    const auto a = data_path(spec, segment());
    const auto wal_path = take(
      take(local_paths::make(spec.root)).wal(0, wal(1)));
    co_await write_bytes(files, a, active_a(), drive);
    co_await write_bytes(
      files, data_path(spec, segment_b()), segment_b_bytes(), drive);
    const std::array targets{target_a(), target_b()};
    const auto expect = [&](
                          const recovery_planner& planned,
                          recovery_plan_action wal_action,
                          std::optional<std::uint64_t> predecessor,
                          recovery_plan_action a_action,
                          bool ready,
                          const char* message) {
        const auto& wal_plan = planned.wal();
        const auto segments = planned.segments();
        require(
          wal_plan.action == wal_action
            && (predecessor
                  ? wal_plan.predecessor
                      && wal_plan.predecessor->position().value() == *predecessor
                  : !wal_plan.predecessor)
            && segments.size() == 2 && segments[0].action == a_action
            && planned.ready() == ready,
          message);
    };

    // A cut-off final PREPARE: the head ends before it.
    auto truncated = interleaved();
    truncated.resize(truncated.size() - 4);
    co_await put_wal(files, spec, wal(1), truncated, drive);
    auto planned = co_await plan(files, owner, spec, budget, targets, drive);
    expect(
      planned,
      recovery_plan_action::activate_successor,
      12288,
      recovery_plan_action::publish_recovering,
      true,
      "a torn final PREPARE was not closed by a successor");
    require(
      planned.segments()[0].boundary == active_boundary()
        && planned.segments()[0].candidates == 0,
      "a footer-covered block became a candidate after its PREPARE tore");
    require(
      (co_await read_bytes(files, wal_path, drive)) == truncated,
      "planning changed the head");

    // A PREPARE's checksummed bytes end where its child does; the rest of its
    // aligned slot is zero padding. A tear must land before that end.
    const auto payload = [](std::string_view prepare) {
        return 32 + 136 + get(prepare, 32 + 128, 4);
    };
    const auto a2_payload = payload(local_fixture::read("prepare_a2"));
    const auto b1_payload = payload(local_fixture::read("prepare_b1"));
    require(
      a2_payload > 256 && b1_payload > 256,
      "a tear at 256 would miss the PREPARE payloads");
    // A final PREPARE torn mid-write: zeros from inside its payload to the
    // end of the file.
    auto torn = interleaved();
    std::fill(torn.begin() + 12288 + 256, torn.end(), '\0');
    co_await put_wal(files, spec, wal(1), torn, drive);
    planned = co_await plan(files, owner, spec, budget, targets, drive);
    expect(
      planned,
      recovery_plan_action::activate_successor,
      12288,
      recovery_plan_action::publish_recovering,
      true,
      "a torn final record was not closed by a successor");

    // A zeroed span inside the second PREPARE's payload, with an intact
    // third: the head ends at the damage, and nothing after it is dropped or
    // truncated.
    auto middle = interleaved();
    std::fill(middle.begin() + 8192 + 256, middle.begin() + 8192 + 512, '\0');
    co_await put_wal(files, spec, wal(1), middle, drive);
    std::vector<std::string> out;
    auto merged = take(
      co_await merge(files, owner, spec, budget, targets, out, drive));
    require(
      merged.wal.content_end
        && merged.wal.content_end->position().value() == 8192
        && std::find(out.begin(), out.end(), "slot A 8192 @footer_only")
             != out.end()
        && std::find(out.begin(), out.end(), "slot B 4096 @footer_only")
             != out.end()
        && std::find(
             out.begin(), out.end(), "region wal1 8192-16384 @uncertified_tail")
             != out.end(),
      "a PREPARE after the head's first damage became evidence");
    planned = co_await plan(files, owner, spec, budget, targets, drive);
    expect(
      planned,
      recovery_plan_action::activate_successor,
      8192,
      recovery_plan_action::publish_recovering,
      true,
      "interior head damage dropped later evidence");
    require(
      (co_await read_bytes(files, wal_path, drive)) == middle,
      "planning changed the damaged head");

    // The same damage inside a rotated file's sealed prefix is proven
    // corruption: nothing may change.
    co_await put_wal(
      files, spec, wal(9), local_fixture::read("wal_successor_gap"), drive);
    planned = co_await plan(
      files, owner, spec, budget, targets, drive, chain_control());
    require(
      planned.wal().action == recovery_plan_action::stop
        && planned.wal().reason == recovery_plan_reason::corruption
        && !planned.ready(),
      "damage in a sealed prefix became a plan");
    co_await put_wal(files, spec, wal(1), interleaved(), drive);

    // Damage to the pinned footer is proven corruption of that segment.
    auto pinned = active_a();
    pinned[12288 + 40] ^= 1;
    co_await write_bytes(files, a, pinned, drive);
    const std::array pinned_targets{target_a(active_boundary()), target_b()};
    planned = co_await plan(files, owner, spec, budget, pinned_targets, drive);
    require(
      planned.segments()[0].action == recovery_plan_action::stop
        && planned.segments()[0].reason == recovery_plan_reason::corruption
        && planned.segments()[1].action
             == recovery_plan_action::publish_recovering
        && !planned.ready(),
      "damage below a pin did not stop its segment");
    require(
      (co_await read_bytes(files, a, drive)) == pinned,
      "planning changed a damaged segment");

    // An uncertified segment tail keeps its candidates; the plan publishes the
    // recovered boundary and never truncates the suffix.
    const auto tail = local_fixture::read("data_header_a")
                      + local_fixture::read("block_a1")
                      + std::string(4096, '\0');
    co_await write_bytes(files, a, tail, drive);
    planned = co_await plan(files, owner, spec, budget, targets, drive);
    require(
      planned.segments()[0].action == recovery_plan_action::publish_recovering
        && !planned.segments()[0].boundary
        && planned.segments()[0].candidates == 1
        && planned.segments()[0].suffix == 1 && planned.ready(),
      "an uncertified tail was not kept for resolution");
    // A recovering segment already pinned at its recovered boundary needs no
    // new publication.
    co_await write_bytes(files, a, active_a(), drive);
    const std::array recovering{
      target_a(active_boundary(), local_object_state::recovering), target_b()};
    planned = co_await plan(files, owner, spec, budget, recovering, drive);
    require(
      planned.segments()[0].action == recovery_plan_action::retain
        && planned.segments()[0].boundary == active_boundary(),
      "a recovering pin was republished or lowered");

    // Unsupported bytes are never planned over.
    auto unsupported = local_fixture::read("prepare_b1");
    put(unsupported, 32 + 124, 2, 2);
    repair(unsupported);
    co_await put_wal(
      files,
      spec,
      wal(1),
      local_fixture::read("wal_header") + local_fixture::read("prepare_a1")
        + unsupported,
      drive);
    auto planner = take(recovery_planner::make(targets, budget));
    out.clear();
    auto rejected = co_await merge(
      files,
      owner,
      spec,
      budget,
      targets,
      out,
      drive,
      2,
      std::nullopt,
      std::numeric_limits<std::size_t>::max(),
      head_control(),
      &planner);
    require(
      !rejected && rejected.error().code() == errc::unsupported_format,
      "an unsupported PREPARE was planned over");
}

// A recovered seal of these tests seals no completion facts.
struct no_facts final {
    seastar::future<runtime::result<std::vector<completed_retry>>>
    read(std::uint32_t, std::uint32_t, codec::cooperative_work&) {
        return seastar::make_ready_future<
          runtime::result<std::vector<completed_retry>>>(
          runtime::failure(detail::path_error(errc::out_of_range)));
    }
};
inline segment_writer_config seal_config(std::uint64_t retry) {
    return {
      local_object_sequence::make(retry).value(),
      installation_contract::limits()};
}
inline recovery_decision_pin
decision_pin(std::uint64_t sequence, std::string_view record) {
    return {
      local_decision_sequence::make(sequence).value(),
      codec::immutable_object_digest{exact_digest(record)},
      byte_count{record.size()}};
}
inline local_wal_cursor cursor(std::uint64_t position) {
    return take(
      local_wal_cursor::make(wal(1), runtime::file_position{position}));
}
// The sealed root an independent builder places at `end` over the blocks and
// footers before it.
inline std::string sealed_root(
  std::uint64_t end,
  std::string_view extent,
  storage::coverage covered,
  std::uint32_t blocks,
  storage::coverage last) {
    return sealed_wire(
      {covered, blocks, last, 0},
      exact_digest(extent),
      {},
      {segment_scan_contract::history_a(), runtime::file_position{end}});
}

// A4 at 24576, after the footer naming A3; PREPAREs for A3 and A4 follow the
// interleaved head at 16384 and 20480.
inline std::string block_a4() { return block_a(103, 3, 24576); }
inline std::string prepare_a3() {
    return prepare_at(data_child(102), 16384, segment(), 2, 16384);
}
inline std::string prepare_a4(std::uint64_t physical = 3) {
    return prepare_at(data_child(103), 20480, segment(), physical, 24576);
}

// One PREPARE of a segment's suffix plan: its source in wal(1), its slot and
// its exact bytes.
struct planned_prepare final {
    std::uint64_t begin, end, target;
    std::string bytes;
};
inline std::vector<planned_prepare> planned_a(std::size_t count) {
    std::vector<planned_prepare> all{
      {4096, 8192, 4096, local_fixture::read("prepare_a1")},
      {12288, 16384, 8192, local_fixture::read("prepare_a2")},
      {16384, 20480, 16384, prepare_a3()},
      {20480, 24576, 24576, prepare_a4()}};
    all.resize(count);
    return all;
}
// The canonical suffix plan's identity, framed independently: the domain,
// the segment, the end, then each PREPARE's source, slot and exact digest in
// WAL order, then their count, every integer as eight little-endian bytes.
inline codec::content_digest suffix_identity(
  const segment_context& target,
  std::uint64_t end,
  const std::vector<planned_prepare>& entries) {
    std::string framed{"KQ/RECOVERY-SUFFIX-PLAN/1"};
    const auto le = [&framed](std::uint64_t value) {
        for (std::size_t i = 0; i < 8; ++i)
            framed.push_back(static_cast<char>(value >> (8U * i)));
    };
    const auto raw = [&framed](const auto& bytes) {
        for (const auto byte : bytes)
            framed.push_back(static_cast<char>(byte));
    };
    const auto cluster = target.cluster();
    const auto topic = target.topic();
    const auto range = target.range();
    const auto id = target.segment();
    const auto incarnation = wal(1);
    raw(cluster.bytes());
    raw(topic.bytes());
    raw(range.bytes());
    raw(id.bytes());
    le(target.generation().value());
    le(end);
    for (const auto& entry : entries) {
        raw(incarnation.bytes());
        le(entry.begin);
        le(entry.end);
        le(entry.target);
        raw(exact_digest(entry.bytes));
    }
    le(entries.size());
    return exact_digest(framed);
}

// A decision record for segment A, encoded by the production codec as an
// input only; the codec itself is pinned by its independent fixtures.
template<typename Backend, typename Driver>
seastar::future<recovery_decision_pin> put_decision(
  Backend& files,
  const local_device_spec& spec,
  std::uint64_t sequence,
  local_recovery_action action,
  std::uint64_t end,
  codec::content_digest plan,
  std::uint64_t content_end,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const local_recovery_decision value{
      cursor(content_end),
      codec::immutable_object_digest{plan},
      segment(),
      runtime::file_position{end},
      action,
      905};
    const auto header = local_metadata_header::make(
                          local_metadata_kind::recovery_decision,
                          spec.shard_owner(0).value(),
                          local_publication_generation::make(sequence).value())
                          .value();
    auto encoded = co_await drive.lifecycle(encode_local_metadata(
      {header, alignment(4096), alignment(4096)},
      local_metadata_payload{value},
      work,
      installation_contract::limits().operation_bytes,
      installation_contract::limits().charge));
    require(encoded.has_value(), "decision record not encoded");
    const auto bytes = flat(encoded->bytes);
    const auto paths = take(local_paths::make(spec.root));
    co_await write_bytes(
      files,
      take(paths.sequence_file(0, local_sequence_file::decision, sequence)),
      bytes,
      drive);
    co_return decision_pin(sequence, bytes);
}
// A segment's current publication (A's by default), encoded by the
// production codec as an input only.
template<typename Backend, typename Driver>
seastar::future<std::string> put_publication(
  Backend& files,
  const local_device_spec& spec,
  local_object_state state,
  std::optional<local_footer_reference> boundary,
  std::uint64_t generation,
  Driver drive,
  segment_context target = segment()) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const local_object_publication value{target, state, boundary, {}};
    const auto header
      = local_metadata_header::make(
          local_metadata_kind::object_publication,
          spec.shard_owner(0).value(),
          local_publication_generation::make(generation).value())
          .value();
    auto encoded = co_await drive.lifecycle(encode_local_metadata(
      {header, alignment(4096), alignment(4096)},
      local_metadata_payload{value},
      work,
      installation_contract::limits().operation_bytes,
      installation_contract::limits().charge));
    require(encoded.has_value(), "publication not encoded");
    const auto bytes = flat(encoded->bytes);
    const auto paths = take(local_paths::make(spec.root));
    co_await write_bytes(
      files,
      take(paths.segment_file(
        0,
        {target.segment(), target.generation()},
        local_segment_file::published)),
      bytes,
      drive);
    co_return bytes;
}
template<typename Backend, typename Driver>
seastar::future<bool>
present(Backend& files, const runtime::file_path& path, Driver drive) {
    auto status = co_await drive.lifecycle(files.stat(path));
    co_return status.has_value();
}
inline recovery_suffix_source suffix_source(
  std::span<const local_device_spec> devices,
  recovery_target target = target_a()) {
    return {
      devices,
      head_control(),
      std::nullopt,
      std::move(target),
      scan_contract::scan_limits()};
}
// Executes segment A's recovered seal under a decision's pin.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<runtime::result<recovered_seal_outcome>> execute_seal(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  recovery_decision_pin decision,
  std::uint64_t retry,
  Driver drive,
  local_object_state state = local_object_state::active) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array specs{spec};
    const auto source = suffix_source(specs, target_a(std::nullopt, state));
    co_return co_await drive.lifecycle(
      execute_recovered_seal<Clock>(
        files,
        owner,
        spec,
        spec,
        0,
        source,
        decision,
        budget,
        seal_config(retry),
        segment_scan_contract::scan_limits(),
        no_facts{},
        0,
        0,
        work));
}

// Resolves a supplied seal on the single device `spec` whose end the store
// proves, and returns its decision.
template<typename Backend, typename Owner, typename Driver>
seastar::future<recovery_decision_outcome> decide_seal(
  recovery_decision_log<Backend, Owner>& log,
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  const recovery_suffix_source& source,
  recovery_seal_resolution resolution,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto resolved = take(
      co_await drive.lifecycle(resolve_recovered_seal(
        log,
        files,
        owner,
        spec,
        spec,
        0,
        source,
        resolution,
        budget,
        segment_scan_contract::scan_limits(),
        work)));
    require(resolved.decided.has_value(), "a provable seal was not decided");
    co_return std::move(*resolved.decided);
}

// Resolutions supplied by an owner become durable decisions before anything
// they authorize runs. A candidate decision pins the exact PREPARE; a segment
// decision pins the WAL content end and its suffix plan, and resolves every
// candidate of the segment. The same resolution replays, a contradicting one
// is refused, and nothing is decided locally.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> decisions(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    using control_type = local_control_owner<Backend, Owner>;
    using allocator_type = local_id_allocator<Backend, Owner>;
    using log_type = recovery_decision_log<Backend, Owner>;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await seed_targets(files, owner, spec, budget, drive);
    const auto paths = take(local_paths::make(spec.root));
    co_await write_bytes(
      files,
      data_path(spec, segment()),
      active_a() + std::string(4096, '\0'),
      drive);
    // A seal is proved from the segment's publication, as it executes.
    co_await put_publication(
      files, spec, local_object_state::active, active_boundary(), 1, drive);
    co_await write_bytes(
      files, data_path(spec, segment_b()), segment_b_bytes(), drive);
    co_await put_wal(files, spec, wal(1), interleaved() + prepare_a3(), drive);
    co_await write_bytes(
      files,
      take(paths.control(0)),
      local_fixture::read("control_head"),
      drive);
    const auto record_path = [&](std::uint64_t sequence) {
        return take(
          paths.sequence_file(0, local_sequence_file::decision, sequence));
    };
    auto control = take(
      co_await drive.lifecycle(
        control_type::open(
          files, owner, spec, 0, true, budget, limits(), work)));
    std::unique_ptr<allocator_type> ids;
    std::unique_ptr<log_type> log;
    runtime::first_failure failed;
    try {
        ids = take(allocator_type::make(*control, budget, 4));
        log = take(
          log_type::make(files, owner, spec, 0, *ids, budget, {limits(), 8}));
        const std::array specs{spec};
        const auto source_a = suffix_source(specs, target_a(active_boundary()));
        const auto source_b = suffix_source(specs, target_b());

        // A candidate decision pins its exact PREPARE, and is durable at its
        // fixed path under the pin it returns.
        const recovery_candidate_resolution a3{
          cursor(16384),
          segment(),
          runtime::file_position{16384},
          local_recovery_action::reconstruct,
          901};
        // A resolution must name a PREPARE the WAL classifies at that slot;
        // nothing is invented for one that does not.
        auto misplaced = a3;
        misplaced.target = runtime::file_position{20480};
        auto missing = co_await drive.lifecycle(
          log->resolve(misplaced, source_a, work));
        require(
          !missing && missing.error().code() == errc::not_found,
          "a decision named a slot its PREPARE does not");
        misplaced = a3;
        misplaced.prepare = cursor(20480);
        missing = co_await drive.lifecycle(
          log->resolve(misplaced, source_a, work));
        require(
          !missing && missing.error().code() == errc::not_found,
          "a decision named a PREPARE the WAL does not hold");

        const auto created = take(
          co_await drive.lifecycle(log->resolve(a3, source_a, work)));
        const local_recovery_decision a3_value{
          cursor(16384),
          codec::immutable_object_digest{exact_digest(prepare_a3())},
          segment(),
          runtime::file_position{16384},
          local_recovery_action::reconstruct,
          901};
        require(
          created.status == recovery_decision_status::created
            && created.record.value == a3_value
            && created.record.pin.sequence.value() == 1,
          "a candidate decision did not pin its exact PREPARE");
        require(
          take(
            co_await drive.lifecycle(
              detail::load_recovery_decision(
                files,
                owner,
                spec,
                0,
                created.record.pin,
                descriptor(),
                budget,
                limits(),
                work)))
            == a3_value,
          "a candidate decision is not durable under its pin");

        // The same resolution replays the stored record; another action for
        // the same PREPARE is refused. Neither writes a record.
        auto replayed = take(
          co_await drive.lifecycle(log->resolve(a3, source_a, work)));
        require(
          replayed.status == recovery_decision_status::exists
            && replayed.record.pin == created.record.pin,
          "a repeated candidate decision did not replay");
        auto other = a3;
        other.action = local_recovery_action::discard;
        auto refused = take(
          co_await drive.lifecycle(log->resolve(other, source_a, work)));
        require(
          refused.status == recovery_decision_status::conflict
            && refused.record.pin == created.record.pin,
          "a contradicting candidate decision was not refused");
        require(
          !(co_await present(files, record_path(2), drive)),
          "a replayed or refused decision wrote a record");

        // A seal whose layout cannot be proved is answered with where the
        // proof stopped, and nothing is persisted: no PREPARE names the slot
        // after A3, so the owner may still decide another end.
        auto unproved = take(
          co_await drive.lifecycle(resolve_recovered_seal(
            *log,
            files,
            owner,
            spec,
            spec,
            0,
            source_a,
            recovery_seal_resolution{
              segment(), runtime::file_position{24576}, 902},
            budget,
            segment_scan_contract::scan_limits(),
            work)));
        require(
          !unproved.decided && unproved.gap == recovered_seal_gap::unproved
            && unproved.at == runtime::file_position{20480}
            && !(co_await present(files, record_path(2), drive)),
          "a seal whose layout was not proved was persisted");

        // One segment decision pins the WAL content end and the canonical
        // suffix plan it proved, and agrees with the candidate decision held
        // for it.
        const recovery_seal_resolution seal{
          segment(), runtime::file_position{20480}, 902};
        const auto sealed = co_await decide_seal(
          *log, files, owner, spec, budget, source_a, seal, drive);
        require(
          sealed.status == recovery_decision_status::created
            && sealed.record.value
                 == local_recovery_decision{cursor(20480), codec::immutable_object_digest{suffix_identity(segment(), 20480, planned_a(3))}, segment(), runtime::file_position{20480}, local_recovery_action::seal_at, 902}
            && sealed.record.pin.sequence.value() == 2,
          "a segment decision did not pin its suffix plan");
        auto again = co_await decide_seal(
          *log, files, owner, spec, budget, source_a, seal, drive);
        require(
          again.status == recovery_decision_status::exists
            && again.record.pin == sealed.record.pin,
          "a repeated segment decision did not replay");
        auto lower = seal;
        lower.end = runtime::file_position{16384};
        again = co_await decide_seal(
          *log, files, owner, spec, budget, source_a, lower, drive);
        require(
          again.status == recovery_decision_status::conflict
            && again.record.pin == sealed.record.pin,
          "a second end for one segment was not refused");

        // That record resolves every candidate of the segment: one below the
        // end is kept, so discarding it contradicts the seal.
        recovery_candidate_resolution a2{
          cursor(12288),
          segment(),
          runtime::file_position{8192},
          local_recovery_action::reconstruct,
          903};
        auto resolved = take(
          co_await drive.lifecycle(log->resolve(a2, source_a, work)));
        require(
          resolved.status == recovery_decision_status::resolved
            && resolved.record.pin == sealed.record.pin,
          "a segment decision did not resolve its candidates");
        a2.action = local_recovery_action::discard;
        resolved = take(
          co_await drive.lifecycle(log->resolve(a2, source_a, work)));
        require(
          resolved.status == recovery_decision_status::conflict,
          "a candidate decision contradicting the seal was accepted");

        // A held candidate decision that a seal would contradict blocks it.
        const recovery_candidate_resolution b1{
          cursor(8192),
          segment_b(),
          runtime::file_position{4096},
          local_recovery_action::discard,
          904};
        const auto discarded = take(
          co_await drive.lifecycle(log->resolve(b1, source_b, work)));
        require(
          discarded.status == recovery_decision_status::created
            && discarded.record.pin.sequence.value() == 3,
          "a discard decision was not persisted");
        auto blocked = co_await decide_seal(
          *log,
          files,
          owner,
          spec,
          budget,
          source_b,
          recovery_seal_resolution{
            segment_b(), runtime::file_position{8192}, 906},
          drive);
        require(
          blocked.status == recovery_decision_status::conflict
            && blocked.record.pin == discarded.record.pin
            && !(co_await present(files, record_path(4), drive)),
          "a seal contradicting a held candidate decision was persisted");

        // After a restart the pins are held again and still answer.
        failed.observe(co_await drive.lifecycle(log->close()));
        log.reset();
        log = take(
          log_type::make(files, owner, spec, 0, *ids, budget, {limits(), 8}));
        for (const auto& [pin, held] : std::array<
               std::pair<recovery_decision_pin, local_segment_descriptor>,
               3>{
               {{created.record.pin, descriptor()},
                {sealed.record.pin, descriptor()},
                {discarded.record.pin, descriptor_b()}}})
            take(co_await drive.lifecycle(log->adopt(pin, held, work)));
        again = co_await decide_seal(
          *log, files, owner, spec, budget, source_a, seal, drive);
        require(
          again.status == recovery_decision_status::exists
            && again.record.pin == sealed.record.pin
            && log->decisions().size() == 3,
          "adopted decisions did not replay after a restart");
        auto forged = sealed.record.pin;
        forged.digest = created.record.pin.digest;
        auto rejected = co_await drive.lifecycle(
          log->adopt(forged, descriptor(), work));
        require(!rejected, "a pin with another identity was adopted");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (log) failed.observe(co_await drive.lifecycle(log->close()));
    log.reset();
    if (ids) failed.observe(co_await drive.lifecycle(ids->close()));
    ids.reset();
    failed.observe(co_await drive.lifecycle(control->close()));
    control.reset();
    take(failed.outcome());
}

// A segment tail leaves only inside a recovered seal that executes a durable
// decision whose suffix plan the WAL still classifies: decision, sealed root,
// truncation, flush, then the sealed publication. Every refusal changes
// nothing, the WAL is never touched, and a crash after any step resumes from
// the same decision.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> recovered_seal(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await seed_targets(files, owner, spec, budget, drive);
    const auto paths = take(local_paths::make(spec.root));
    const local_segment_name name{segment().segment(), segment().generation()};
    const auto data = data_path(spec, segment());
    const auto published = take(
      paths.segment_file(0, name, local_segment_file::published));
    const auto wal_path = take(paths.wal(0, wal(1)));
    co_await put_wal(files, spec, wal(1), interleaved(), drive);
    const auto header = local_fixture::read("data_header_a");
    const auto a1 = local_fixture::read("block_a1");
    const auto a2 = local_fixture::read("block_a2");
    const auto footer_a = local_fixture::read("footer_a");
    const auto boundary = co_await put_publication(
      files, spec, local_object_state::active, active_boundary(), 1, drive);
    const auto execute =
      [&](
        recovery_decision_pin decision,
        std::uint64_t retry,
        local_object_state state = local_object_state::active) {
          return execute_seal<Clock>(
            files, owner, spec, budget, decision, retry, drive, state);
      };
    const auto unchanged =
      [&](const std::string& bytes, const char* message) -> seastar::future<> {
        require(
          (co_await read_bytes(files, data, drive)) == bytes
            && (co_await read_bytes(files, published, drive)) == boundary
            && (co_await read_bytes(files, wal_path, drive)) == interleaved(),
          message);
    };
    const auto plan = [](std::uint64_t end) {
        return suffix_identity(segment(), end, planned_a(2));
    };
    const auto suffix = active_a() + segment_scan_contract::block_a3()
                        + std::string(4096, '\0');
    co_await write_bytes(files, data, suffix, drive);

    // No durable decision, or one that is not exactly this segment's seal of
    // the plan the WAL classifies now, changes nothing.
    const auto at_footer_record = std::string(4096, 'x');
    auto refused = co_await execute(decision_pin(64, at_footer_record), 50);
    require(!refused, "a seal ran without a durable decision");
    co_await unchanged(suffix, "a refused seal changed bytes");
    const auto at_footer = co_await put_decision(
      files,
      spec,
      64,
      local_recovery_action::seal_at,
      16384,
      plan(16384),
      16384,
      drive);
    refused = co_await execute(decision_pin(64, at_footer_record), 50);
    require(
      !refused && refused.error().code() == errc::corrupt_data,
      "a decision with another identity authorized a seal");
    const auto discard = co_await put_decision(
      files,
      spec,
      66,
      local_recovery_action::discard,
      16384,
      plan(16384),
      16384,
      drive);
    refused = co_await execute(discard, 50);
    require(
      !refused && refused.error().code() == errc::wrong_context,
      "a candidate decision authorized a segment seal");
    const auto stale = co_await put_decision(
      files,
      spec,
      68,
      local_recovery_action::seal_at,
      16384,
      exact_digest("another plan"),
      16384,
      drive);
    refused = co_await execute(stale, 50);
    require(
      !refused && refused.error().code() == errc::wrong_context,
      "a decision naming another suffix plan authorized a seal");
    // Past the surviving objects, with no candidate to fill the gap: the
    // layout is not proved and nothing changes.
    const auto beyond = co_await put_decision(
      files,
      spec,
      67,
      local_recovery_action::seal_at,
      24576,
      plan(24576),
      16384,
      drive);
    auto gap = take(co_await execute(beyond, 50));
    require(
      gap.unresolved == recovered_seal_gap::unproved && gap.at
        && gap.at->value() == 20480 && !gap.seal.boundary,
      "a seal past the surviving objects was not left unresolved");
    co_await unchanged(suffix, "an unresolved seal changed bytes");

    // Sealed at the decided end: the suffix after it is removed, the WAL is
    // not touched, and the sealed publication pins the new root.
    const auto root = sealed_root(
      16384,
      a1 + a2 + footer_a,
      scope(100, 102, 0, 2, 4096, 16384),
      2,
      scope(101, 102, 1, 2, 8192, 12288));
    const auto sealed_bytes = active_a() + root;
    const auto expect_sealed = [&](const char* message) -> seastar::future<> {
        require(
          (co_await read_bytes(files, data, drive)) == sealed_bytes
            && (co_await read_bytes(files, wal_path, drive)) == interleaved(),
          message);
        auto generation = co_await segment_scan_contract::open_generation(
          files, owner, spec, budget, drive);
        require(
          generation.publication.state == local_object_state::sealed
            && generation.publication.boundary
            && generation.publication.boundary->position().value() == 16384
            && generation.publication.boundary->digest().bytes()
                 == exact_digest(root),
          message);
        co_await segment_scan_contract::close_generation(generation, drive);
    };
    auto sealed = take(co_await execute(at_footer, 50));
    require(
      !sealed.already_sealed && sealed.seal.boundary
        && sealed.seal.boundary->position().value() == 16384
        && sealed.seal.retry && sealed.seal.retry->sequence().value() == 50
        && sealed.blocks == 0 && sealed.footers == 0,
      "the recovered seal did not report its root");
    co_await expect_sealed("the recovered seal left a tail or wrong root");
    require(
      (co_await read_bytes(
         files,
         take(paths.object(0, name, sealed.seal.retry->sequence())),
         drive))
        .empty(),
      "the retry bundle of an empty completion set is not empty");
    // Repeating the decision finds it done.
    sealed = take(co_await execute(at_footer, 51));
    require(sealed.already_sealed, "a completed seal ran again");
    co_await expect_sealed("a completed seal changed bytes");

    // A crash after the root but before truncation, or after truncation but
    // before publication, resumes from the decision to the same state.
    for (const auto& [bytes, retry] :
         std::array<std::pair<std::string, std::uint64_t>, 2>{
           {{active_a() + root + std::string(4096, '\0'), 52},
            {sealed_bytes, 53}}}) {
        co_await write_bytes(files, data, bytes, drive);
        co_await write_bytes(files, published, boundary, drive);
        sealed = take(co_await execute(at_footer, retry));
        require(!sealed.already_sealed, "an interrupted seal was not resumed");
        co_await expect_sealed("a resumed seal reached another state");
    }

    // An owner's end below the recovered boundary: the root replaces the
    // pinned footer, and a seal interrupted there still resumes without it.
    const auto lower = co_await put_decision(
      files,
      spec,
      65,
      local_recovery_action::seal_at,
      12288,
      plan(12288),
      16384,
      drive);
    const auto lower_root = sealed_root(
      12288,
      a1 + a2,
      scope(100, 102, 0, 2, 4096, 12288),
      2,
      scope(101, 102, 1, 2, 8192, 12288));
    for (const auto& [bytes, retry] :
         std::array<std::pair<std::string, std::uint64_t>, 2>{
           {{active_a() + segment_scan_contract::block_a3(), 54},
            {header + a1 + a2 + lower_root + segment_scan_contract::block_a3(),
             55}}}) {
        co_await write_bytes(files, data, bytes, drive);
        co_await write_bytes(files, published, boundary, drive);
        sealed = take(co_await execute(lower, retry));
        require(
          sealed.seal.boundary
            && sealed.seal.boundary->position().value() == 12288
            && (co_await read_bytes(files, data, drive))
                 == header + a1 + a2 + lower_root,
          "a seal below the recovered boundary did not end at its decision");
    }

    // A recovering publication opens metadata-only and roll-required; only
    // the recovered seal changes it.
    co_await write_bytes(files, data, active_a(), drive);
    const local_object_publication recovering{
      segment(), local_object_state::recovering, active_boundary(), {}};
    co_await put_publication(
      files, spec, local_object_state::recovering, active_boundary(), 2, drive);
    {
        using writer_type = segment_writer<Backend, Owner, Clock>;
        auto writer = take(
          co_await drive.lifecycle(
            writer_type::open_existing(
              files,
              owner,
              spec,
              0,
              {local_publication_generation::make(2).value(),
               recovering,
               descriptor(),
               {}},
              budget,
              seal_config(56),
              work)));
        runtime::first_failure failed;
        try {
            require(
              writer->recovered() && take(writer->roll_required())
                && writer->append_state() == model::append_state::active
                && !writer->capture(),
              "a recovering publication reopened for append");
            auto ordinary = co_await drive.lifecycle(
              writer->seal(no_facts{}, 0, 0, work));
            require(
              ordinary.failure.failed() && !ordinary.boundary,
              "an ordinary seal ran on a recovered segment");
            auto written = co_await drive.lifecycle(writer->write_recovered(
              runtime::file_position{16384},
              co_await installation_contract::buffer_async(
                segment_scan_contract::block_a3()),
              work));
            require(
              !written && written.error().code() == errc::closed,
              "a recovered segment was written outside a decided seal");
            // Nor may it seal a verified extent: only an owner opened to
            // execute a durable decision truncates.
            auto extent = take(
              co_await drive.lifecycle(verify_local_segment_extent(
                files,
                owner,
                spec,
                0,
                segment_scan_contract::history_a(),
                runtime::file_position{16384},
                budget,
                segment_scan_contract::scan_limits(),
                work,
                [](const segment_scanned_object&) {
                    return seastar::make_ready_future<runtime::result<bool>>(
                      true);
                })));
            auto unauthorized = co_await drive.lifecycle(writer->seal_recovered(
              std::move(extent), no_facts{}, 0, 0, work));
            require(
              unauthorized.failure.failed() && !unauthorized.boundary,
              "a recovered segment was sealed without a decision");
        } catch (...) {
            failed.observe(std::current_exception());
        }
        failed.observe(co_await drive.lifecycle(writer->close()));
        take(failed.outcome());
    }
    require(
      (co_await read_bytes(files, data, drive)) == active_a(),
      "a recovering open changed bytes");
    sealed = take(
      co_await execute(at_footer, 57, local_object_state::recovering));
    require(
      sealed.seal.boundary
        && (co_await read_bytes(files, data, drive)) == sealed_bytes,
      "a recovering segment did not seal at its decision");
}

// A completed recovered seal is durable as a whole: one flush of the data
// file covers the root and the truncation before the sealed publication, so
// a crash just after it keeps exactly the sealed extent and publication.
// flushes(path) counts a file's flushes, or is empty where they cannot be
// observed; crash() drops everything a flush did not cover.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Flushes,
  typename Crash>
seastar::future<> crash_after_seal(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Flushes flushes,
  Crash crash) {
    co_await seed_targets(files, owner, spec, budget, drive);
    const auto data = data_path(spec, segment());
    co_await put_wal(files, spec, wal(1), interleaved(), drive);
    co_await put_publication(
      files, spec, local_object_state::active, active_boundary(), 1, drive);
    co_await write_bytes(
      files,
      data,
      active_a() + segment_scan_contract::block_a3() + std::string(4096, '\0'),
      drive);
    const auto decision = co_await put_decision(
      files,
      spec,
      64,
      local_recovery_action::seal_at,
      16384,
      suffix_identity(segment(), 16384, planned_a(2)),
      16384,
      drive);
    const auto root = sealed_root(
      16384,
      local_fixture::read("block_a1") + local_fixture::read("block_a2")
        + local_fixture::read("footer_a"),
      scope(100, 102, 0, 2, 4096, 16384),
      2,
      scope(101, 102, 1, 2, 8192, 12288));
    const auto before = flushes(data);
    auto sealed = take(
      co_await execute_seal<Clock>(
        files, owner, spec, budget, decision, 50, drive));
    require(
      sealed.seal.boundary && sealed.seal.boundary->position().value() == 16384,
      "the recovered seal did not seal at its decision");
    if (before)
        require(
          flushes(data) == *before + 1,
          "the recovered seal did not flush its data file exactly once");
    crash();
    require(
      (co_await read_bytes(files, data, drive)) == active_a() + root,
      "a crash after a recovered seal lost its root or truncation");
    auto generation = co_await segment_scan_contract::open_generation(
      files, owner, spec, budget, drive);
    const bool kept = generation.publication.state == local_object_state::sealed
                      && generation.publication.boundary
                      && generation.publication.boundary->digest().bytes()
                           == exact_digest(root);
    co_await segment_scan_contract::close_generation(generation, drive);
    require(kept, "a crash after a recovered seal lost its publication");
}

// Reconstruction happens only inside the recovered seal, slot by slot in
// file order: each slot below the decided end holds its verified surviving
// object or the block of the PREPARE that names it, and a footer is placed
// only in a gap proved to be one between groups. A surviving later footer
// must name the reconstructed prefix. Whatever cannot be proved leaves the
// segment and the WAL untouched, and an interrupted reconstruction resumes
// from the same decision.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> reconstruction(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await seed_targets(files, owner, spec, budget, drive);
    const auto paths = take(local_paths::make(spec.root));
    const local_segment_name name{segment().segment(), segment().generation()};
    const auto data = data_path(spec, segment());
    const auto published = take(
      paths.segment_file(0, name, local_segment_file::published));
    const auto wal_path = take(paths.wal(0, wal(1)));
    const auto a1 = local_fixture::read("block_a1");
    const auto a2 = local_fixture::read("block_a2");
    const auto footer_a = local_fixture::read("footer_a");
    const auto a3 = segment_scan_contract::block_a3();
    const auto footer_a3 = segment_scan_contract::footer_a3();
    const auto a4 = block_a4();
    const auto full_wal = interleaved() + prepare_a3() + prepare_a4();
    std::uint64_t sequence = 70, retry = 70;
    struct attempt final {
        recovered_seal_outcome outcome;
        std::string bytes;
    };
    // Resets the segment, its publication and the WAL, then executes a seal
    // at `end` of the plan that WAL classifies.
    const auto run = [&](
                       std::string segment_bytes,
                       std::string wal_bytes,
                       std::uint64_t end,
                       std::size_t planned,
                       std::uint64_t content_end)
      -> seastar::future<runtime::result<attempt>> {
        co_await write_bytes(files, data, segment_bytes, drive);
        co_await put_wal(files, spec, wal(1), wal_bytes, drive);
        co_await put_publication(
          files, spec, local_object_state::active, active_boundary(), 1, drive);
        const auto decision = co_await put_decision(
          files,
          spec,
          sequence++,
          local_recovery_action::seal_at,
          end,
          suffix_identity(segment(), end, planned_a(planned)),
          content_end,
          drive);
        auto outcome = co_await execute_seal<Clock>(
          files, owner, spec, budget, decision, retry++, drive);
        if (!outcome) co_return runtime::failure(outcome.error());
        require(
          (co_await read_bytes(files, wal_path, drive)) == wal_bytes,
          "a recovered seal changed the WAL");
        co_return attempt{*outcome, co_await read_bytes(files, data, drive)};
    };
    const auto sealed_at = [&](const attempt& done, std::uint64_t end) {
        return done.outcome.unresolved == recovered_seal_gap::none
               && done.outcome.seal.boundary
               && done.outcome.seal.boundary->position().value() == end;
    };

    // Two groups of candidates after the pinned footer: A3, then the footer
    // that ends its group in a gap proved by A4, then A4. Every byte placed
    // is exactly what the writer would have written.
    const auto sealed_extent = active_a() + a3 + footer_a3 + a4;
    const auto root = sealed_root(
      28672,
      a1 + a2 + footer_a + a3 + footer_a3 + a4,
      scope(100, 104, 0, 4, 4096, 28672),
      4,
      scope(103, 104, 3, 4, 24576, 28672));
    auto done = take(
      co_await run(
        active_a() + std::string(4096, '\0'), full_wal, 28672, 4, 24576));
    require(
      sealed_at(done, 28672) && done.outcome.blocks == 2
        && done.outcome.footers == 1 && done.bytes == sealed_extent + root,
      "candidates and their group footer were not reconstructed exactly");

    // Interrupted after A3 was written, or after every object but before the
    // root: the same decision reaches the same state.
    for (const auto& [bytes, blocks, footers] :
         std::array<std::tuple<std::string, std::uint32_t, std::uint32_t>, 2>{
           {{active_a() + a3 + std::string(4096, '\0'), 1, 1},
            {sealed_extent, 0, 0}}}) {
        done = take(co_await run(bytes, full_wal, 28672, 4, 24576));
        require(
          sealed_at(done, 28672) && done.outcome.blocks == blocks
            && done.outcome.footers == footers
            && done.bytes == sealed_extent + root,
          "an interrupted reconstruction did not resume to the same state");
    }

    // A group whose later member's PREPARE did not survive: the owner seals
    // after A3, and the root, not an invented footer, ends the extent.
    const auto partial_root = sealed_root(
      20480,
      a1 + a2 + footer_a + a3,
      scope(100, 103, 0, 3, 4096, 20480),
      3,
      scope(102, 103, 2, 3, 16384, 20480));
    const auto partial_wal = interleaved() + prepare_a3();
    done = take(
      co_await run(
        active_a() + std::string(8192, '\0'), partial_wal, 20480, 3, 20480));
    require(
      sealed_at(done, 20480) && done.outcome.blocks == 1
        && done.outcome.footers == 0
        && done.bytes == active_a() + a3 + partial_root,
      "a partial group was not sealed after its surviving candidate");

    // A surviving later footer must name the reconstructed prefix.
    const auto later_root = sealed_root(
      24576,
      a1 + a2 + footer_a + a3 + footer_a3,
      scope(100, 103, 0, 3, 4096, 24576),
      3,
      scope(102, 103, 2, 3, 16384, 20480));
    done = take(
      co_await run(
        active_a() + std::string(4096, '\0') + footer_a3,
        partial_wal,
        24576,
        3,
        20480));
    require(
      sealed_at(done, 24576) && done.outcome.blocks == 1
        && done.outcome.footers == 0
        && done.bytes == active_a() + a3 + footer_a3 + later_root,
      "a surviving footer did not verify the reconstruction");

    // Nothing that cannot be proved changes a byte.
    struct unproved_case final {
        std::string segment_bytes, wal_bytes;
        std::uint64_t end;
        std::size_t planned;
        std::uint64_t content_end;
        recovered_seal_gap gap;
        std::uint64_t at;
        const char* message;
    };
    const auto foreign_a3 = block_a(102, 2, 16384, true);
    const std::array cases{
      // An intact block other than the candidate holds its slot.
      unproved_case{
        active_a() + foreign_a3,
        partial_wal,
        20480,
        3,
        20480,
        recovered_seal_gap::conflict,
        16384,
        "a different intact block was overwritten"},
      // A surviving footer that does not name the reconstructed prefix.
      unproved_case{
        active_a() + std::string(4096, '\0')
          + segment_scan_contract::footer_a3(1),
        partial_wal,
        24576,
        3,
        20480,
        recovered_seal_gap::conflict,
        20480,
        "a footer contradicting the reconstruction was kept or replaced"},
      // A gap after the last candidate is not a group boundary anyone proved.
      unproved_case{
        active_a() + std::string(8192, '\0'),
        partial_wal,
        24576,
        3,
        20480,
        recovered_seal_gap::unproved,
        20480,
        "a footer was invented at the end of a group"},
      // A4 skips a record, so the gap before it is not a footer's.
      unproved_case{
        active_a() + std::string(4096, '\0'),
        interleaved() + prepare_a3() + prepare_a4(4),
        28672,
        0,
        24576,
        recovered_seal_gap::unproved,
        20480,
        "a gap before a discontinuous candidate became a footer"},
    };
    for (const auto& test : cases) {
        auto plan = test.planned != 0 ? planned_a(test.planned)
                                      : std::vector<planned_prepare>{
                                          planned_a(3)[0],
                                          planned_a(3)[1],
                                          planned_a(3)[2],
                                          {20480, 24576, 24576, prepare_a4(4)}};
        co_await write_bytes(files, data, test.segment_bytes, drive);
        co_await put_wal(files, spec, wal(1), test.wal_bytes, drive);
        const auto boundary = co_await put_publication(
          files, spec, local_object_state::active, active_boundary(), 1, drive);
        const auto decision = co_await put_decision(
          files,
          spec,
          sequence++,
          local_recovery_action::seal_at,
          test.end,
          suffix_identity(segment(), test.end, plan),
          test.content_end,
          drive);
        auto outcome = take(
          co_await execute_seal<Clock>(
            files, owner, spec, budget, decision, retry++, drive));
        require(
          outcome.unresolved == test.gap && outcome.at
            && outcome.at->value() == test.at && !outcome.seal.boundary,
          test.message);
        require(
          (co_await read_bytes(files, data, drive)) == test.segment_bytes
            && (co_await read_bytes(files, published, drive)) == boundary
            && (co_await read_bytes(files, wal_path, drive)) == test.wal_bytes,
          test.message);
    }

    // The plan binds the WAL: a decision classified before A4 arrived names
    // another plan.
    co_await write_bytes(
      files, data, active_a() + std::string(4096, '\0'), drive);
    co_await put_wal(files, spec, wal(1), full_wal, drive);
    co_await put_publication(
      files, spec, local_object_state::active, active_boundary(), 1, drive);
    auto earlier = co_await put_decision(
      files,
      spec,
      sequence++,
      local_recovery_action::seal_at,
      20480,
      suffix_identity(segment(), 20480, planned_a(3)),
      20480,
      drive);
    auto stale = co_await execute_seal<Clock>(
      files, owner, spec, budget, earlier, retry++, drive);
    require(
      !stale && stale.error().code() == errc::wrong_context,
      "a decision survived a change of its suffix plan");

    // Damage to the pinned footer is proven corruption: nothing is rebuilt.
    auto damaged = active_a() + std::string(4096, '\0');
    damaged[12288 + 40] ^= 1;
    auto corrupt = co_await run(damaged, full_wal, 28672, 4, 24576);
    require(
      !corrupt && corrupt.error().code() == errc::corrupt_data
        && (co_await read_bytes(files, data, drive)) == damaged,
      "damage below the pin was reconstructed over");
}

// The directory a file's path names.
inline runtime::file_path parent_of(const runtime::file_path& path) {
    return take(
      runtime::file_path::make(
        path.value().substr(0, path.value().rfind('/'))));
}
// A publication temporary of `path`, as the publisher names its first
// attempt at `generation`.
inline runtime::file_path
temporary_of(const runtime::file_path& path, std::uint64_t generation) {
    const auto split = path.value().rfind('/');
    return take(local_child_path(
      parent_of(path),
      take(local_temporary_name(
        take(runtime::file_name::make(path.value().substr(split + 1))),
        take(local_publication_generation::make(generation)),
        0))));
}
struct decisions_seen final {
    std::vector<recovery_decision_record>* out;
    seastar::future<runtime::result<bool>>
    operator()(const recovery_decision_record& value) const {
        out->push_back(value);
        return seastar::make_ready_future<runtime::result<bool>>(true);
    }
};
template<typename Backend, typename Owner, typename Driver>
seastar::future<runtime::result<recovery_decision_discovery>> discover(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  const local_shard_control& fields,
  std::span<const local_segment_descriptor> catalog,
  std::vector<recovery_decision_record>& out,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_return co_await drive.lifecycle(discover_recovery_decisions(
      files,
      owner,
      spec,
      0,
      fields,
      catalog,
      budget,
      {limits(), 16},
      work,
      decisions_seen{&out}));
}
template<typename Backend, typename Owner, typename Driver>
seastar::future<runtime::result<recovery_inventory>> inventory(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  std::span<const recovery_catalog_entry> catalog,
  std::span<const recovery_decision_record> decisions,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array specs{spec};
    co_return co_await drive.lifecycle(open_recovery_inventory(
      files,
      owner,
      std::span<const local_device_spec>{specs},
      0,
      catalog,
      decisions,
      budget,
      limits(),
      work));
}
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<runtime::result<recovered_publications>> publish(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  const recovery_planner& planned,
  const recovery_inventory& found,
  std::span<const recovery_visibility> visible,
  std::uint64_t retry,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array specs{spec};
    co_return co_await drive.lifecycle(
      publish_recovered_segments<Clock>(
        files,
        owner,
        std::span<const local_device_spec>{specs},
        0,
        planned,
        found,
        visible,
        budget,
        seal_config(retry),
        {},
        work));
}

// Restart settles interrupted operations from durable records and exact
// files before the merge reads anything. A recovered seal whose root already
// replaced the pinned footer reads as damage below the pin to the merge
// alone; its durable decision is found first, behind a fresh directory
// barrier, and the seal resumes to the decided end. A deleting segment is
// left to its deletion. Leftovers of interrupted publications, rotations and
// seal attempts are recognised by their exact names, never read as records
// and never changed.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> interruptions(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await seed_targets(files, owner, spec, budget, drive);
    const auto paths = take(local_paths::make(spec.root));
    const local_segment_name name{segment().segment(), segment().generation()};
    const auto data = data_path(spec, segment());
    const auto published = take(
      paths.segment_file(0, name, local_segment_file::published));
    const auto wal_path = take(paths.wal(0, wal(1)));
    co_await put_wal(files, spec, wal(1), interleaved(), drive);
    const auto header = local_fixture::read("data_header_a");
    const auto a1 = local_fixture::read("block_a1");
    const auto a2 = local_fixture::read("block_a2");
    const auto a3 = segment_scan_contract::block_a3();
    co_await put_publication(
      files, spec, local_object_state::active, active_boundary(), 1, drive);
    const auto decision = co_await put_decision(
      files,
      spec,
      65,
      local_recovery_action::seal_at,
      12288,
      suffix_identity(segment(), 12288, planned_a(2)),
      16384,
      drive);
    const auto root = sealed_root(
      12288,
      a1 + a2,
      scope(100, 102, 0, 2, 4096, 12288),
      2,
      scope(101, 102, 1, 2, 8192, 12288));
    // The seal's root replaced the pinned footer; the process stopped before
    // the truncation and the sealed publication.
    co_await write_bytes(files, data, header + a1 + a2 + root + a3, drive);
    // A decision's and a publication's temporaries, and a successor header
    // the head chain never reached.
    const auto pending = temporary_of(
      take(paths.sequence_file(0, local_sequence_file::decision, 70)), 70);
    co_await write_bytes(files, pending, std::string(4096, 'x'), drive);
    const auto replacing = temporary_of(published, 2);
    co_await write_bytes(files, replacing, std::string(4096, 'y'), drive);
    const auto orphan = take(paths.wal(0, wal(9)));
    co_await put_wal(
      files, spec, wal(9), local_fixture::read("wal_successor_gap"), drive);
    // An earlier seal attempt's retry bundle that no publication names.
    const auto abandoned = take(
      paths.object(0, name, take(local_object_sequence::make(57))));
    take(
      co_await drive.lifecycle(files.create_directories(parent_of(abandoned))));
    co_await write_bytes(files, abandoned, std::string(4096, 'z'), drive);
    // Segment B is being deleted.
    const auto deleting = co_await put_publication(
      files,
      spec,
      local_object_state::deleting,
      std::nullopt,
      3,
      drive,
      segment_b());
    auto fields = head_control();
    fields.decision_high = local_decision_high{128};
    const std::array descriptors{descriptor(), descriptor_b()};
    const std::array catalog{
      recovery_catalog_entry{descriptor(), segment_head()},
      recovery_catalog_entry{descriptor_b(), segment_head_b()}};

    {
        const std::array targets{target_a(active_boundary())};
        auto planned = co_await plan(
          files, owner, spec, budget, targets, drive, fields);
        require(
          !planned.ready()
            && planned.segments()[0].action == recovery_plan_action::stop
            && planned.segments()[0].reason == recovery_plan_reason::corruption,
          "the merge alone did not read the replaced footer as damage");
    }

    std::vector<recovery_decision_record> found;
    {
        // A sequence above the decision high mark was never allocated.
        auto low = fields;
        low.decision_high = local_decision_high{64};
        auto refused = co_await discover(
          files, owner, spec, budget, low, descriptors, found, drive);
        require(
          !refused && refused.error().code() == errc::wrong_context,
          "a decision above the high mark was read");
        found.clear();
        // Outside the catalog a decision is retained, never acted on.
        auto outside = take(
          co_await discover(
            files,
            owner,
            spec,
            budget,
            fields,
            std::span<const local_segment_descriptor>{},
            found,
            drive));
        require(
          outside.complete && outside.unmatched == 1 && outside.decisions == 0
            && outside.temporaries == 1 && found.empty(),
          "a decision outside the catalog was visited");
    }
    auto discovered = take(
      co_await discover(
        files, owner, spec, budget, fields, descriptors, found, drive));
    require(
      discovered.complete && discovered.decisions == 1
        && discovered.temporaries == 1 && discovered.unmatched == 0
        && found.size() == 1 && found[0].pin == decision
        && found[0].value.action == local_recovery_action::seal_at
        && found[0].value.target_position.value() == 12288,
      "the durable seal decision was not discovered under its pin");

    // A restarted log holds what discovery found before it takes any
    // resolution: a seal of the same segment at another end conflicts.
    {
        using control_type = local_control_owner<Backend, Owner>;
        using allocator_type = local_id_allocator<Backend, Owner>;
        using log_type = recovery_decision_log<Backend, Owner>;
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        co_await write_bytes(
          files,
          take(paths.control(0)),
          local_fixture::read("control_head"),
          drive);
        auto control = take(
          co_await drive.lifecycle(
            control_type::open(
              files, owner, spec, 0, true, budget, limits(), work)));
        std::unique_ptr<allocator_type> ids;
        std::unique_ptr<log_type> log;
        runtime::first_failure failed;
        try {
            ids = take(allocator_type::make(*control, budget, 4));
            log = take(
              log_type::make(
                files, owner, spec, 0, *ids, budget, {limits(), 8}));
            const auto held = take(
              co_await drive.lifecycle(
                log->adopt(found[0].pin, descriptor(), work)));
            const std::array specs{spec};
            auto other = co_await decide_seal(
              *log,
              files,
              owner,
              spec,
              budget,
              suffix_source(specs, target_a(active_boundary())),
              recovery_seal_resolution{
                segment(), runtime::file_position{16384}, 906},
              drive);
            require(
              held.value == found[0].value
                && other.status == recovery_decision_status::conflict
                && other.record.pin == decision,
              "a restarted log accepted a seal contradicting a durable one");
        } catch (...) {
            failed.observe(std::current_exception());
        }
        if (log) failed.observe(co_await drive.lifecycle(log->close()));
        log.reset();
        if (ids) failed.observe(co_await drive.lifecycle(ids->close()));
        ids.reset();
        failed.observe(co_await drive.lifecycle(control->close()));
        control.reset();
        take(failed.outcome());
    }

    {
        auto opened = take(
          co_await inventory(
            files, owner, spec, budget, catalog, found, drive));
        require(
          opened.entries.size() == 2
            && opened.entries[0].state == recovery_entry_state::sealing
            && opened.entries[0].pending_seal == decision
            && opened.entries[1].state == recovery_entry_state::deleting
            && opened.targets.empty(),
          "a pending seal or a deleting segment was handed to the merge");
        auto sealed = take(
          co_await execute_seal<Clock>(
            files,
            owner,
            spec,
            budget,
            *opened.entries[0].pending_seal,
            58,
            drive));
        require(
          sealed.seal.boundary
            && sealed.seal.boundary->position().value() == 12288
            && (co_await read_bytes(files, data, drive))
                 == header + a1 + a2 + root,
          "the resumed seal did not end at its decision");
    }
    auto opened = take(
      co_await inventory(files, owner, spec, budget, catalog, found, drive));
    require(
      opened.entries.size() == 2
        && opened.entries[0].state == recovery_entry_state::target
        && !opened.entries[0].pending_seal && opened.targets.size() == 1
        && opened.targets[0].state == local_object_state::sealed
        && opened.targets[0].pin
        && opened.targets[0].pin->position().value() == 12288
        && opened.entries[1].state == recovery_entry_state::deleting,
      "the seal decision was not consumed by the sealed publication");
    auto planned = co_await plan(
      files, owner, spec, budget, opened.targets, drive, fields);
    require(
      planned.ready()
        && planned.segments()[0].action == recovery_plan_action::retain
        && planned.wal().action == recovery_plan_action::activate_successor
        && planned.wal().unresolved == 1,
      "the resumed seal did not leave a ready plan");
    require(
      (co_await read_bytes(files, pending, drive)) == std::string(4096, 'x')
        && (co_await read_bytes(files, replacing, drive))
             == std::string(4096, 'y')
        && (co_await read_bytes(files, orphan, drive))
             == local_fixture::read("wal_successor_gap")
        && (co_await read_bytes(files, abandoned, drive))
             == std::string(4096, 'z')
        && (co_await read_bytes(files, wal_path, drive)) == interleaved()
        && (co_await read_bytes(
             files,
             take(paths.segment_file(
               0,
               {segment_b().segment(), segment_b().generation()},
               local_segment_file::published)),
             drive))
             == deleting,
      "restart changed a leftover, the WAL or a deleting segment");
}

// Before restart changes anything every configured device must be an
// existing store this configuration owns. A device found empty is lost, never
// a store to start again; a newer format is unsupported; corrupt bytes, or
// another device's identity, are corrupt; missing metadata is incomplete.
// Every device is reported, the set takes the most severe, and nothing is
// written. A segment creation interrupted before its publication is counted,
// never a gap in the store. `specs` are an all-role device, then a data
// device.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> device_gate(
  Backend& files,
  Owner& owner,
  std::span<const local_device_spec> specs,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    // The configured set is initialized together: it has one control device.
    const auto initialize = [&] {
        return drive.lifecycle(initialize_local_stores(
          files,
          owner,
          specs,
          {local_store_intent::create_or_resume, false},
          budget,
          limits(),
          work));
    };
    take((co_await initialize()).failure.outcome());
    const auto inspect = [&] {
        return drive.lifecycle(
          inspect_local_recovery(files, owner, specs, budget, limits(), work));
    };
    const auto verdicts = [](
                            const recovery_store_report& report,
                            recovery_store_verdict all,
                            recovery_store_verdict data) {
        return report.inspected == 2 && report.verdict == std::max(all, data)
               && report.devices[0].verdict == all
               && report.devices[1].verdict == data;
    };
    auto report = take(co_await inspect());
    require(
      verdicts(
        report, recovery_store_verdict::ready, recovery_store_verdict::ready)
        && report.devices[1].store
        && report.devices[1].store->interrupted_creations == 0,
      "bootstrapped devices were not ready");

    const auto data = specs[1];
    const auto data_paths = take(local_paths::make(data.root));
    const auto marker = take(data_paths.store());
    const auto identity = co_await read_bytes(files, marker, drive);
    const auto other = co_await read_bytes(
      files, take(take(local_paths::make(specs[0].root)).store()), drive);
    for (const auto& [bytes, expected] :
         std::array<std::pair<std::string, recovery_store_verdict>, 3>{
           {{local_fixture::read("future_minimum"),
             recovery_store_verdict::unsupported},
            {local_fixture::read("reserved_common"),
             recovery_store_verdict::corrupt},
            {other, recovery_store_verdict::corrupt}}}) {
        co_await write_bytes(files, marker, bytes, drive);
        report = take(co_await inspect());
        require(
          verdicts(report, recovery_store_verdict::ready, expected)
            && report.devices[1].error
            && (co_await read_bytes(files, marker, drive)) == bytes,
          "a device's identity was not refused as it stands");
    }
    co_await write_bytes(files, marker, identity, drive);

    const auto control = take(
      take(local_paths::make(specs[0].root)).control(1));
    const auto fields = co_await read_bytes(files, control, drive);
    take(co_await drive.lifecycle(files.remove_file(control)));
    report = take(co_await inspect());
    require(
      verdicts(
        report,
        recovery_store_verdict::incomplete,
        recovery_store_verdict::ready)
        && !take(co_await drive.lifecycle(files.exists(control))),
      "a missing control was not incomplete");
    co_await write_bytes(files, control, fields, drive);

    // The data device's disk is replaced by an empty one.
    for (std::uint32_t shard = 0; shard < data.identity.shard_count; ++shard) {
        const auto segments = take(
          data_paths.buckets(shard, local_bucket_kind::segment));
        take(co_await drive.lifecycle(files.remove_directory(segments)));
        take(
          co_await drive.lifecycle(
            files.remove_directory(parent_of(segments))));
    }
    take(
      co_await drive.lifecycle(files.remove_directory(parent_of(
        parent_of(take(data_paths.buckets(0, local_bucket_kind::segment)))))));
    take(co_await drive.lifecycle(files.remove_file(marker)));
    report = take(co_await inspect());
    require(
      verdicts(
        report, recovery_store_verdict::ready, recovery_store_verdict::lost)
        && !take(co_await drive.lifecycle(files.exists(marker))),
      "an empty device was not lost, or was started again");
    take((co_await initialize()).failure.outcome());

    // Creation stopped after the descriptor and the data file's temporary.
    take((co_await drive.lifecycle(publish_local_segment_descriptor(
            files,
            owner,
            data,
            0,
            descriptor(),
            segment_head(),
            budget,
            limits(),
            work)))
           .failure.outcome());
    const auto temporary = temporary_of(
      take(data_paths.segment_file(
        0,
        {segment().segment(), segment().generation()},
        local_segment_file::data)),
      1);
    co_await write_bytes(
      files, temporary, local_fixture::read("data_header_a"), drive);
    report = take(co_await inspect());
    require(
      verdicts(
        report, recovery_store_verdict::ready, recovery_store_verdict::ready)
        && report.devices[1].store
        && report.devices[1].store->interrupted_creations == 1
        && (co_await read_bytes(files, temporary, drive))
             == local_fixture::read("data_header_a"),
      "an interrupted segment creation was a gap in the store");
}

// A recovered segment's classified prefix seeds evidence only after one new
// flush of its data file; only then does a recovering publication pin its
// recovered boundary, never below its pin. Reads stop at the smaller of the
// supplied visible end and that boundary, so the suffix after it stays
// outside them. A plan that has not finished publishes nothing, and the next
// restart leaves an unchanged recovering segment alone.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> recovering_publication(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await seed_targets(files, owner, spec, budget, drive);
    const auto paths = take(local_paths::make(spec.root));
    const local_segment_name name{segment().segment(), segment().generation()};
    const auto data = data_path(spec, segment());
    const auto published = take(
      paths.segment_file(0, name, local_segment_file::published));
    co_await put_wal(files, spec, wal(1), interleaved(), drive);
    // A3 after footer_a is a suffix no PREPARE names.
    const auto bytes = segment_scan_contract::active_a()
                       + segment_scan_contract::block_a3();
    co_await write_bytes(files, data, bytes, drive);
    const auto initial = co_await put_publication(
      files, spec, local_object_state::active, std::nullopt, 1, drive);
    const std::array catalog{
      recovery_catalog_entry{descriptor(), segment_head()}};
    const std::array beyond{
      recovery_visibility{segment(), model::range_logical_end{103}}};

    auto found = take(
      co_await inventory(
        files,
        owner,
        spec,
        budget,
        catalog,
        std::span<const recovery_decision_record>{},
        drive));
    require(
      found.targets.size() == 1
        && found.targets[0].state == local_object_state::active
        && !found.targets[0].pin,
      "the active segment was not a target");
    {
        // A plan that has not finished authorizes nothing.
        auto unfinished = take(recovery_planner::make(found.targets, budget));
        auto refused = co_await publish<Clock>(
          files, owner, spec, budget, unfinished, found, beyond, 70, drive);
        require(
          !refused && refused.error().code() == errc::wrong_context
            && (co_await read_bytes(files, published, drive)) == initial,
          "a plan that had not finished published a segment");
    }
    {
        auto planned = co_await plan(
          files, owner, spec, budget, found.targets, drive);
        require(
          planned.ready()
            && planned.segments()[0].action
                 == recovery_plan_action::publish_recovering
            && planned.segments()[0].boundary == active_boundary()
            && planned.segments()[0].recovered_end.value() == 102
            && planned.segments()[0].suffix == 1,
          "the restart did not plan a recovering publication");
        auto outcome = take(
          co_await publish<Clock>(
            files, owner, spec, budget, planned, found, beyond, 71, drive));
        require(
          outcome.complete && outcome.segments.size() == 1
            && outcome.segments[0].published && !outcome.segments[0].failure
            && outcome.segments[0].reads
                 == recovery_read_bound{model::range_logical_end{102}, true},
          "the recovering publication did not complete");
    }
    {
        auto generation = co_await segment_scan_contract::open_generation(
          files, owner, spec, budget, drive);
        require(
          generation.generation.value() == 2
            && generation.publication.state == local_object_state::recovering
            && generation.publication.boundary == active_boundary()
            && generation.publication.roots.empty(),
          "the recovering publication does not pin the recovered boundary");
        co_await segment_scan_contract::close_generation(generation, drive);
    }
    require(
      (co_await read_bytes(files, data, drive)) == bytes
        && (co_await read_bytes(files, take(paths.wal(0, wal(1))), drive))
             == interleaved(),
      "a recovering publication changed the segment or the WAL");
    const auto recovering = co_await read_bytes(files, published, drive);

    // The pin is never lowered, and an unchanged publication not repeated.
    {
        using writer_type = segment_writer<Backend, Owner, Clock>;
        const local_object_publication current{
          segment(), local_object_state::recovering, active_boundary(), {}};
        auto writer = take(
          co_await drive.lifecycle(
            writer_type::open_existing(
              files,
              owner,
              spec,
              0,
              {local_publication_generation::make(2).value(),
               current,
               descriptor(),
               {}},
              budget,
              seal_config(72),
              work)));
        runtime::first_failure failed;
        try {
            const auto other = take(
              local_footer_reference::make(
                runtime::file_position{12288},
                byte_count{4_KiB},
                6,
                codec::immutable_object_digest{exact_digest("other")}));
            for (const auto& [boundary, code] : std::array<
                   std::pair<std::optional<local_footer_reference>, errc>,
                   3>{
                   {{std::nullopt, errc::wrong_context},
                    {other, errc::wrong_context},
                    {active_boundary(), errc::invalid_argument}}}) {
                auto refused = co_await drive.lifecycle(
                  writer->publish_recovering(boundary, work));
                require(
                  !refused && refused.error().code() == code,
                  "a recovering publication lowered or repeated its pin");
            }
            require(
              take(writer->roll_required()),
              "a recovering segment accepted appends");
        } catch (...) {
            failed.observe(std::current_exception());
        }
        failed.observe(co_await drive.lifecycle(writer->close()));
        take(failed.outcome());
    }
    require(
      (co_await read_bytes(files, published, drive)) == recovering,
      "a refused recovering publication changed the pointer");

    // The next restart finds it recovering at its boundary and keeps it.
    auto again = take(
      co_await inventory(
        files,
        owner,
        spec,
        budget,
        catalog,
        std::span<const recovery_decision_record>{},
        drive));
    require(
      again.targets.size() == 1
        && again.targets[0].state == local_object_state::recovering
        && again.targets[0].pin == active_boundary(),
      "the recovering segment did not resume at its pin");
    auto replanned = co_await plan(
      files, owner, spec, budget, again.targets, drive);
    const std::array below{
      recovery_visibility{segment(), model::range_logical_end{101}}};
    auto kept = take(
      co_await publish<Clock>(
        files, owner, spec, budget, replanned, again, below, 73, drive));
    require(
      replanned.ready()
        && replanned.segments()[0].action == recovery_plan_action::retain
        && kept.complete && kept.segments.size() == 1
        && !kept.segments[0].published
        && kept.segments[0].action == recovery_plan_action::retain
        && kept.segments[0].reads
             == recovery_read_bound{model::range_logical_end{101}, false}
        && (co_await read_bytes(files, published, drive)) == recovering,
      "an unchanged recovering segment was flushed or published again");
}

// Damage to one 4-KiB slot of a file image, each a shape a log reader meets.
enum class slot_damage : std::uint8_t {
    // Never written, or written with zeros.
    zeros,
    // Only the header's first bytes are zero.
    zeroed_header,
    // One checksum byte differs.
    checksum,
    // Another family's integrity-valid envelope, its checksums repaired.
    family,
    // A longer body length, its checksums repaired.
    length,
    // Bytes that were never an envelope.
    garbage,
    // A valid PREPARE written for another position.
    stale,
};
inline std::string
damage_slot(std::string image, std::size_t at, slot_damage kind) {
    auto slot = image.substr(at, 4096);
    switch (kind) {
    case slot_damage::zeros:
        slot.assign(4096, '\0');
        break;
    case slot_damage::zeroed_header:
        std::fill_n(slot.begin(), 32, '\0');
        break;
    case slot_damage::checksum:
        slot[24] ^= 1;
        break;
    case slot_damage::family:
        put(slot, 4, 6, 2);
        repair(slot);
        break;
    case slot_damage::length:
        put(slot, 12, get(slot, 12, 4) + 4096, 4);
        repair(slot);
        break;
    case slot_damage::garbage:
        slot.assign(4096, 'x');
        break;
    case slot_damage::stale:
        slot = local_fixture::read("prepare_a1");
        break;
    }
    image.replace(at, 4096, slot);
    return image;
}

// Every damage shape a log reader's own tests construct, with the outcome
// recovery owes for it. A head ends its content at its first damage and a
// successor continues after it, whatever lies beyond: zeros, a torn or cut
// record, a checksum mismatch, another family or length behind a repaired
// checksum, garbage or a stale record. Records after the damage are never
// resynced to, joined or accepted; they stay uncertified survivals, and the
// blocks they name stay covered by their footers. The same damage below a
// rotated file's sealed end is proven corruption: recovery stops, every
// later byte stays and nothing is created. In a segment the recovered
// boundary is the last footer before the damage, an interior gap is damage
// rather than an end, and the PREPAREs after it stay enumerated. No damage
// is truncated, repaired or deleted.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> source_cases(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await seed_targets(files, owner, spec, budget, drive);
    const auto paths = take(local_paths::make(spec.root));
    const auto a = data_path(spec, segment());
    const auto head_path = take(paths.wal(0, wal(1)));
    const auto successor_path = take(paths.wal(0, wal(9)));
    co_await write_bytes(files, a, active_a(), drive);
    co_await write_bytes(
      files, data_path(spec, segment_b()), segment_b_bytes(), drive);
    const std::array targets{target_a(), target_b()};
    const auto contains =
      [](const std::vector<std::string>& out, std::string_view line) {
          return std::find(out.begin(), out.end(), line) != out.end();
      };
    // The head's content ends at `end`; every byte stays.
    const auto head_ends = [&](
                             std::string bytes,
                             std::uint64_t end,
                             std::string region,
                             const char* message) -> seastar::future<> {
        co_await put_wal(files, spec, wal(1), bytes, drive);
        std::vector<std::string> out;
        auto merged = take(
          co_await merge(files, owner, spec, budget, targets, out, drive));
        require(
          merged.wal.content_end
            && merged.wal.content_end->position().value() == end
            && contains(out, region),
          message);
        auto planned = co_await plan(
          files, owner, spec, budget, targets, drive);
        require(
          planned.ready()
            && planned.wal().action == recovery_plan_action::activate_successor
            && planned.wal().predecessor
            && planned.wal().predecessor->position().value() == end
            && planned.segments()[0].boundary == active_boundary()
            && planned.segments()[0].candidates == 0,
          message);
        require(
          (co_await read_bytes(files, head_path, drive)) == bytes
            && (co_await read_bytes(files, a, drive)) == active_a(),
          message);
    };

    // Interior damage to the second PREPARE: the head ends there, and the
    // intact third PREPARE after it never becomes evidence.
    for (const auto kind :
         {slot_damage::zeros,
          slot_damage::zeroed_header,
          slot_damage::checksum,
          slot_damage::family,
          slot_damage::length,
          slot_damage::garbage,
          slot_damage::stale}) {
        const auto bytes = damage_slot(interleaved(), 8192, kind);
        co_await head_ends(
          bytes,
          8192,
          "region wal1 8192-16384 @uncertified_tail",
          "interior head damage was read past or accepted");
        std::vector<std::string> out;
        take(co_await merge(files, owner, spec, budget, targets, out, drive));
        require(
          contains(out, "slot A 8192 @footer_only")
            && contains(out, "slot B 4096 @footer_only"),
          "a PREPARE after the head's damage became evidence");
    }
    // A final PREPARE whose repaired length runs past the end of the file,
    // and final PREPAREs cut at every depth: the head ends before them.
    {
        auto past = interleaved();
        auto last = past.substr(12288);
        put(last, 12, get(last, 12, 4) + 8192, 4);
        repair(last);
        past.replace(12288, 4096, last);
        co_await head_ends(
          past,
          12288,
          "region wal1 12288-16384 @uncertified_tail",
          "a length past the end of the file was read as an end or data");
    }
    for (const std::size_t kept : {1U, 31U, 32U, 2048U, 4092U}) {
        const auto cut = interleaved().substr(0, 12288 + kept);
        co_await head_ends(
          cut,
          12288,
          "region wal1 12288-" + number(12288 + kept) + " @uncertified_tail",
          "a cut final PREPARE was not ended before");
    }

    // The same damage below a rotated file's sealed end is proven
    // corruption: recovery stops and changes nothing.
    const auto successor = local_fixture::read("wal_successor_gap");
    co_await put_wal(files, spec, wal(9), successor, drive);
    const auto rotated_stops =
      [&](std::string bytes, const char* message) -> seastar::future<> {
        co_await put_wal(files, spec, wal(1), bytes, drive);
        auto planned = co_await plan(
          files, owner, spec, budget, targets, drive, chain_control());
        require(
          !planned.ready() && planned.wal().action == recovery_plan_action::stop
            && planned.wal().reason == recovery_plan_reason::corruption,
          message);
        require(
          (co_await read_bytes(files, head_path, drive)) == bytes
            && (co_await read_bytes(files, successor_path, drive)) == successor,
          message);
    };
    for (const auto kind :
         {slot_damage::zeros,
          slot_damage::zeroed_header,
          slot_damage::checksum,
          slot_damage::family,
          slot_damage::length,
          slot_damage::garbage,
          slot_damage::stale})
        co_await rotated_stops(
          damage_slot(interleaved(), 8192, kind),
          "damage in a sealed prefix did not stop recovery");
    // A rotated file shorter than its sealed end is missing certified bytes:
    // the chain is refused as corrupt before any content is read.
    {
        const auto cut = interleaved().substr(0, 12288 + 100);
        co_await put_wal(files, spec, wal(1), cut, drive);
        std::vector<std::string> out;
        auto refused = co_await merge(
          files,
          owner,
          spec,
          budget,
          targets,
          out,
          drive,
          2,
          std::nullopt,
          std::numeric_limits<std::size_t>::max(),
          chain_control());
        require(
          !refused
            && recovery_failure_verdict(refused.error().code())
                 == recovery_store_verdict::corrupt
            && out.empty(),
          "a rotated file shorter than its sealed end was not refused");
        require(
          (co_await read_bytes(files, head_path, drive)) == cut
            && (co_await read_bytes(files, successor_path, drive)) == successor,
          "a refused rotated file was changed");
    }
    // A cut record at the end of the head, after an intact rotated file,
    // keeps the rotated content and ends the head before it.
    co_await put_wal(files, spec, wal(1), interleaved(), drive);
    {
        const auto torn = successor
                          + local_fixture::read("prepare_a1").substr(0, 2048);
        co_await put_wal(files, spec, wal(9), torn, drive);
        auto planned = co_await plan(
          files, owner, spec, budget, targets, drive, chain_control());
        require(
          planned.ready()
            && planned.wal().action == recovery_plan_action::activate_successor
            && planned.wal().predecessor
            && planned.wal().predecessor->incarnation() == wal(9)
            && planned.wal().predecessor->position().value() == 4096
            && (co_await read_bytes(files, successor_path, drive)) == torn,
          "a torn final record of the head lost the rotated content");
        co_await put_wal(files, spec, wal(9), successor, drive);
    }
    take(co_await drive.lifecycle(files.remove_file(successor_path)));
    co_await put_wal(files, spec, wal(1), interleaved(), drive);

    // Segments. The recovered boundary is the last footer before the first
    // damage; the PREPAREs after it stay candidates or a sourced suffix.
    const auto a1 = local_fixture::read("block_a1");
    const auto a3 = segment_scan_contract::block_a3();
    const auto segment_plan = [&](std::string bytes, const char* message)
      -> seastar::future<recovery_segment_plan> {
        co_await write_bytes(files, a, bytes, drive);
        auto planned = co_await plan(
          files, owner, spec, budget, targets, drive);
        require(
          planned.ready()
            && planned.segments()[0].action
                 == recovery_plan_action::publish_recovering
            && (co_await read_bytes(files, a, drive)) == bytes,
          message);
        co_return planned.segments()[0];
    };
    // A single block, damaged: nothing is recovered and nothing truncated;
    // both PREPAREs stay candidates.
    {
        auto bad = a1;
        bad[24] ^= 1;
        const auto planned = co_await segment_plan(
          local_fixture::read("data_header_a") + bad,
          "a single damaged block was truncated away");
        require(
          !planned.boundary && planned.candidates == 2,
          "a single damaged block lost its candidates");
    }
    // A damaged last block after an intact group: the boundary is before it.
    {
        auto bad = a3;
        bad[24] ^= 1;
        const auto planned = co_await segment_plan(
          active_a() + bad, "a damaged last block was not ended before");
        require(
          planned.boundary == active_boundary() && planned.candidates == 0,
          "a damaged last block moved the boundary");
    }
    // Nonsense from inside any object to the end of the file.
    const auto tail = active_a() + a3 + segment_scan_contract::footer_a3();
    for (const std::uint64_t object : {4096U, 8192U, 12288U, 16384U})
        for (const std::uint64_t inside : {0U, 7U, 14U}) {
            auto bytes = tail;
            std::fill(
              bytes.begin() + static_cast<std::ptrdiff_t>(object + inside),
              bytes.end(),
              'x');
            const auto planned = co_await segment_plan(
              bytes, "nonsense to the end changed the segment");
            if (object >= 16384)
                require(
                  planned.boundary == active_boundary()
                    && planned.candidates == 0 && planned.suffix == 0,
                  "nonsense after a footer moved the boundary");
            else {
                // Each intact block before the damage stays a sourced
                // suffix; each PREPARE whose block is gone a candidate.
                const auto candidates = (12288 - object) / 4096;
                require(
                  !planned.boundary && planned.candidates == candidates
                    && planned.suffix == 2 - candidates,
                  "nonsense inside the first group dropped a PREPARE");
            }
        }
    // Interior damage with later intact objects: no footer after the damage
    // becomes the boundary, and nothing after it is read as an end.
    for (const auto kind :
         {slot_damage::zeros,
          slot_damage::zeroed_header,
          slot_damage::garbage}) {
        const auto planned = co_await segment_plan(
          damage_slot(tail, 8192, kind),
          "interior segment damage changed the segment");
        require(
          !planned.boundary && planned.candidates == 1 && planned.suffix == 1,
          "a footer after interior damage became the boundary");
    }
    // Garbage after the header.
    {
        const auto planned = co_await segment_plan(
          local_fixture::read("data_header_a") + std::string(8192, 'x'),
          "a garbage segment was changed");
        require(
          !planned.boundary && planned.candidates == 2,
          "a garbage segment lost its candidates");
    }
}

// A seeded stream of damage choices, so one seed replays one image exactly.
class damage_random final {
public:
    explicit damage_random(std::uint64_t seed) noexcept
      : state_(seed) {}
    // Uniform in [0, bound).
    std::uint64_t uniform(std::uint64_t bound) noexcept {
        state_ += 0x9e3779b97f4a7c15ULL;
        auto z = state_;
        z = (z ^ (z >> 30U)) * 0xbf58476d1ce4e5b9ULL;
        z = (z ^ (z >> 27U)) * 0x94d049bb133111ebULL;
        return (z ^ (z >> 31U)) % bound;
    }

private:
    std::uint64_t state_;
};
// The worst a crash may do to one 4-KiB page of a write no barrier covered.
enum class page_damage : std::uint8_t { none, drop, corrupt };

// What a crash leaves of `written`, laid over `before` at `offset`, when no
// barrier covered the write. Each 4-KiB page draws its damage up to `most`:
// kept whole, dropped, or torn. In a torn page each 512-byte sector is kept
// with probability 1/4; otherwise with probability 2/3 it is bad on its right
// side, its left side or entirely, the bad bytes garbage with probability 1/2
// (always, when the whole sector is bad) or else still the old bytes; and
// otherwise it is dropped. The file ends at its old end or at the last byte
// that survived, whichever is later, so dropped sectors past the old end
// leave holes or a shorter file.
inline std::string torn(
  std::string_view before,
  std::string_view written,
  std::size_t offset,
  std::uint64_t seed,
  page_damage most = page_damage::corrupt) {
    damage_random random{seed};
    std::string image{before};
    if (image.size() < offset + written.size())
        image.resize(offset + written.size(), '\0');
    std::size_t end = before.size();
    const auto keep = [&](std::size_t from, std::size_t to) {
        std::copy(
          written.begin() + static_cast<std::ptrdiff_t>(from),
          written.begin() + static_cast<std::ptrdiff_t>(to),
          image.begin() + static_cast<std::ptrdiff_t>(offset + from));
        end = std::max(end, offset + to);
    };
    for (std::size_t page = 0; page < written.size(); page += 4096) {
        const auto mode = static_cast<page_damage>(
          random.uniform(static_cast<std::uint64_t>(most) + 1));
        const auto page_end = std::min(page + 4096, written.size());
        for (std::size_t sector = page; sector < page_end; sector += 512) {
            const auto length = std::min<std::size_t>(512, page_end - sector);
            if (
              mode == page_damage::none
              || (mode == page_damage::corrupt && random.uniform(4) == 0)) {
                keep(sector, sector + length);
                continue;
            }
            if (mode != page_damage::corrupt || random.uniform(3) == 0)
                continue;
            const auto side = random.uniform(3);
            const bool garbage = side == 2 || random.uniform(2) == 0;
            std::size_t good_start = 0, good_end = length, bad_start = 0,
                        bad_end = length;
            if (side == 0) {
                good_end = random.uniform(length);
                bad_start = good_end;
            } else if (side == 1) {
                bad_end = random.uniform(length);
                good_start = bad_end;
            } else {
                good_end = 0;
            }
            if (garbage && bad_start != bad_end) {
                keep(sector, sector + length);
                for (auto i = bad_start; i < bad_end; ++i)
                    image[offset + sector + i] = std::bit_cast<char>(
                      static_cast<std::uint8_t>(random.uniform(256)));
            } else if (good_start != good_end) {
                keep(sector + good_start, sector + good_end);
            }
        }
    }
    image.resize(end);
    return image;
}

// A crash during a recovered seal leaves whatever unflushed writes the device
// kept: pages whole, dropped or torn sector by sector, and an end of file
// that may revert. The seal resumed over any of them reaches exactly the
// state the uninterrupted seal reaches, including one whose root replaced
// the pinned footer.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> torn_seals(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await seed_targets(files, owner, spec, budget, drive);
    const auto data = data_path(spec, segment());
    const auto header = local_fixture::read("data_header_a");
    const auto a1 = local_fixture::read("block_a1");
    const auto a2 = local_fixture::read("block_a2");
    const auto footer_a = local_fixture::read("footer_a");
    const auto a3 = segment_scan_contract::block_a3();
    const auto footer_a3 = segment_scan_contract::footer_a3();
    const auto a4 = block_a4();
    co_await write_bytes(
      files, data_path(spec, segment_b()), segment_b_bytes(), drive);

    // The images span every shape: kept, dropped with a reverted end, torn
    // with old bytes, and torn with garbage; one seed replays one image.
    {
        const std::string before(8192, 'o');
        const std::string written(12288, 'n');
        bool whole = false, reverted = false, mixed = false, garbage = false;
        for (std::uint64_t seed = 1; seed <= 256; ++seed) {
            const auto image = torn(before, written, 4096, seed);
            require(
              image == torn(before, written, 4096, seed)
                && image.size() >= before.size()
                && image.size() <= 4096 + written.size()
                && image.substr(0, 4096) == before.substr(0, 4096),
              "a torn image was not reproducible or touched earlier bytes");
            whole = whole || image == before.substr(0, 4096) + written;
            reverted = reverted || image.size() < 4096 + written.size();
            garbage = garbage
                      || std::any_of(image.begin(), image.end(), [](char c) {
                             return c != 'o' && c != 'n' && c != '\0';
                         });
            for (std::size_t sector = 4096; sector + 512 <= image.size();
                 sector += 512) {
                const auto view = std::string_view{image}.substr(sector, 512);
                mixed
                  = mixed
                    || (view.find('n') != std::string_view::npos && view.find_first_not_of('n') != std::string_view::npos);
            }
        }
        require(
          whole && reverted && mixed && garbage,
          "torn images do not cover every damage shape");
    }

    std::uint64_t sequence = 70, retry = 70;
    struct attempt final {
        recovered_seal_outcome outcome;
        std::string bytes;
    };
    // As the reconstruction case: resets the segment, its publication and
    // the WAL, then executes a seal at `end` of the plan that WAL classifies.
    const auto run = [&](
                       std::string segment_bytes,
                       std::string wal_bytes,
                       std::uint64_t end,
                       std::size_t planned,
                       std::uint64_t content_end)
      -> seastar::future<runtime::result<attempt>> {
        co_await write_bytes(files, data, segment_bytes, drive);
        co_await put_wal(files, spec, wal(1), wal_bytes, drive);
        co_await put_publication(
          files, spec, local_object_state::active, active_boundary(), 1, drive);
        const auto decision = co_await put_decision(
          files,
          spec,
          sequence++,
          local_recovery_action::seal_at,
          end,
          suffix_identity(segment(), end, planned_a(planned)),
          content_end,
          drive);
        auto outcome = co_await execute_seal<Clock>(
          files, owner, spec, budget, decision, retry++, drive);
        if (!outcome) co_return runtime::failure(outcome.error());
        co_return attempt{*outcome, co_await read_bytes(files, data, drive)};
    };

    // A reconstruction torn before the seal's one flush resumes to the same
    // sealed extent.
    {
        const auto root = sealed_root(
          28672,
          a1 + a2 + footer_a + a3 + footer_a3 + a4,
          scope(100, 104, 0, 4, 4096, 28672),
          4,
          scope(103, 104, 3, 4, 24576, 28672));
        const auto before = active_a() + std::string(4096, '\0');
        const auto written = a3 + footer_a3 + a4 + root;
        const auto full_wal = interleaved() + prepare_a3() + prepare_a4();
        for (std::uint64_t seed = 1; seed <= 16; ++seed) {
            auto done = take(
              co_await run(
                torn(before, written, 16384, seed), full_wal, 28672, 4, 24576));
            require(
              done.outcome.unresolved == recovered_seal_gap::none
                && done.outcome.seal.boundary
                && done.outcome.seal.boundary->position().value() == 28672
                && done.bytes == active_a() + written,
              "a torn reconstruction did not resume to the sealed extent");
        }
    }
    // A seal below the pinned footer, torn after its root replaced that
    // footer, with or without its truncation: the same decision ends there.
    {
        const auto lower_root = sealed_root(
          12288,
          a1 + a2,
          scope(100, 102, 0, 2, 4096, 12288),
          2,
          scope(101, 102, 1, 2, 8192, 12288));
        const auto before = active_a() + a3;
        for (std::uint64_t seed = 1; seed <= 8; ++seed) {
            auto image = torn(before, lower_root, 12288, seed);
            if (seed % 2 == 0)
                image.resize(std::min(image.size(), 12288 + lower_root.size()));
            auto done = take(
              co_await run(image, interleaved(), 12288, 2, 16384));
            require(
              done.outcome.seal.boundary
                && done.outcome.seal.boundary->position().value() == 12288
                && done.bytes == header + a1 + a2 + lower_root,
              "a torn seal below the pin did not end at its decision");
        }
    }
}

// A restart over appends a crash tore never certifies less than the last
// flushed footer and never loses a PREPARE: each stays satisfied, a sourced
// suffix or a candidate. A torn WAL tail never ends the head before its
// certified prefix.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> torn_tails(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await seed_targets(files, owner, spec, budget, drive);
    const auto data = data_path(spec, segment());
    const auto wal_path = take(
      take(local_paths::make(spec.root)).wal(0, wal(1)));
    const auto a3 = segment_scan_contract::block_a3();
    const auto footer_a3 = segment_scan_contract::footer_a3();
    const auto a4 = block_a4();
    co_await write_bytes(
      files, data_path(spec, segment_b()), segment_b_bytes(), drive);
    const std::array targets{target_a(), target_b()};
    const auto label = [](
                         const std::vector<std::string>& out,
                         std::string_view slot) -> std::string {
        std::string found;
        std::size_t matches = 0;
        for (const auto& line : out)
            if (line.starts_with(slot)) {
                ++matches;
                const auto at = line.find('@');
                found = line.substr(at, line.find(' ', at) - at);
            }
        return matches == 1 ? found : std::string{};
    };
    const auto footer_a3_ref = take(
      local_footer_reference::make(
        runtime::file_position{20480},
        byte_count{4096},
        6,
        codec::immutable_object_digest{exact_digest(footer_a3)}));
    // Appends torn after the last flushed footer: the boundary is that footer
    // or a later one that survived whole, and every PREPARE stays.
    co_await put_wal(
      files, spec, wal(1), interleaved() + prepare_a3() + prepare_a4(), drive);
    for (std::uint64_t seed = 1; seed <= 16; ++seed) {
        const auto image = torn(active_a(), a3 + footer_a3 + a4, 16384, seed);
        co_await write_bytes(files, data, image, drive);
        std::vector<std::string> out;
        take(co_await merge(files, owner, spec, budget, targets, out, drive));
        auto planned = co_await plan(
          files, owner, spec, budget, targets, drive);
        const auto& a_plan = planned.segments()[0];
        const auto third = label(out, "slot A 16384 ");
        const auto fourth = label(out, "slot A 24576 ");
        const bool later = a_plan.boundary == footer_a3_ref;
        require(
          planned.ready()
            && a_plan.action == recovery_plan_action::publish_recovering
            && (later || a_plan.boundary == active_boundary())
            && (later ? third == "@satisfied" : third == "@suffix_sourced" || third == "@candidate")
            && (fourth == "@suffix_sourced" || fourth == "@candidate")
            && (co_await read_bytes(files, data, drive)) == image,
          "torn appends lowered the boundary or lost a PREPARE");
    }
    // A WAL tail torn after its certified prefix: the head ends no earlier
    // than that prefix, and the segment's copies stay covered.
    co_await write_bytes(files, data, active_a() + a3 + footer_a3 + a4, drive);
    for (std::uint64_t seed = 1; seed <= 16; ++seed) {
        const auto image = torn(
          interleaved(), prepare_a3() + prepare_a4(), 16384, seed);
        co_await put_wal(files, spec, wal(1), image, drive);
        std::vector<std::string> out;
        take(co_await merge(files, owner, spec, budget, targets, out, drive));
        auto planned = co_await plan(
          files, owner, spec, budget, targets, drive);
        const auto end = planned.wal().predecessor;
        const auto third = label(out, "slot A 16384 ");
        const auto fourth = label(out, "slot A 24576 ");
        require(
          planned.ready() && end && end->position().value() >= 16384
            && planned.segments()[0].boundary == footer_a3_ref
            && third
                 == (end->position().value() >= 20480 ? "@satisfied" : "@footer_only")
            && fourth
                 == (end->position().value() >= 24576 ? "@suffix_sourced" : "@suffix_unsourced")
            && (co_await read_bytes(files, wal_path, drive)) == image,
          "a torn WAL tail ended the head before its certified prefix");
    }
}

// Records every item, and cancels its merge's work once `after` items were
// delivered or, with `first`, signals another task after the first item and
// waits until that task has acted. Notes the most handles held while any
// item is visited.
struct observed_merge final {
    std::vector<std::string>* out;
    workload_budget* budget;
    std::uint64_t* handles;
    seastar::abort_source* abort{nullptr};
    std::size_t after{0};
    seastar::promise<>* first{nullptr};
    seastar::future<>* acted{nullptr};
    seastar::future<runtime::result<bool>>
    operator()(const recovery_item& item) const {
        out->push_back(describe(item));
        *handles = std::max(*handles, budget->snapshot().handles);
        if (abort && out->size() == after) abort->request_abort();
        if (first && out->size() == 1) {
            first->set_value();
            return std::move(*acted).then(
              [] { return runtime::result<bool>{true}; });
        }
        return seastar::make_ready_future<runtime::result<bool>>(true);
    }
};

// Keeps every item a merge delivers.
struct kept_items final {
    std::vector<recovery_item>* out;
    seastar::future<runtime::result<bool>>
    operator()(const recovery_item& item) const {
        out->push_back(item);
        return seastar::make_ready_future<runtime::result<bool>>(true);
    }
};

// Scans stay bounded and give way to other work. Cancellation after any
// delivered item, or from another task while the merge runs, ends it as
// aborted with a prefix of its enumeration: nothing is written, and every
// charge and handle comes back. Under any shortage of tasks, bytes or handles
// the merge delivers exactly its unconstrained enumeration or fails cleanly
// with a resource error, never part of a plan. One segment walk open at a
// time gives the same enumeration, each segment read in one run, and no more
// handles are held than open walks plus the WAL file.
// Obligation rows stop at their bound instead of growing, and a queue of one
// classifies the same items. An allocation failure anywhere in planning
// leaves nothing charged.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> bounded_scanning(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await seed_targets(files, owner, spec, budget, drive);
    const auto a = data_path(spec, segment());
    const auto wal_path = take(
      take(local_paths::make(spec.root)).wal(0, wal(1)));
    co_await write_bytes(files, a, active_a(), drive);
    co_await write_bytes(
      files, data_path(spec, segment_b()), segment_b_bytes(), drive);
    co_await put_wal(files, spec, wal(1), interleaved(), drive);
    const std::array specs{spec};
    const std::array targets{target_a(), target_b()};
    // Borrowed by every merge until it completes.
    const auto control = head_control();
    const auto baseline = budget.snapshot();
    const auto unchanged = [&](const char* message) -> seastar::future<> {
        const auto now = budget.snapshot();
        require(
          now.tasks == baseline.tasks && now.bytes == baseline.bytes
            && now.handles == baseline.handles
            && (co_await read_bytes(files, wal_path, drive)) == interleaved()
            && (co_await read_bytes(files, a, drive)) == active_a(),
          message);
    };
    const auto run = [&](
                       codec::cooperative_work& work,
                       recovery_merge_limits limits,
                       observed_merge visit) {
        return drive.lifecycle(reconcile_local_recovery(
          files,
          owner,
          std::span<const local_device_spec>{specs},
          spec,
          0,
          control,
          std::nullopt,
          targets,
          budget,
          limits,
          work,
          visit));
    };
    const auto prefix = [](
                          const std::vector<std::string>& part,
                          const std::vector<std::string>& whole) {
        return part.size() <= whole.size()
               && std::equal(part.begin(), part.end(), whole.begin());
    };

    std::vector<std::string> expected;
    std::uint64_t handles = 0;
    {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        take(
          co_await run(
            work,
            merge_limits(),
            observed_merge{&expected, &budget, &handles}));
    }
    co_await unchanged("an unconstrained merge kept a charge");

    // Cancelled after each delivered item.
    for (std::size_t after = 1; after <= expected.size(); ++after) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        std::vector<std::string> out;
        auto merged = co_await run(
          work,
          merge_limits(),
          observed_merge{&out, &budget, &handles, &abort, after});
        require(
          prefix(out, expected)
            && (merged ? out == expected
                       : merged.error().code() == errc::aborted
                           && out.size() < expected.size())
            && (after != 1 || !merged),
          "a cancelled merge went on, failed otherwise or delivered other "
          "items");
        co_await unchanged("a cancelled merge kept a charge or wrote");
    }
    // Cancelled from another task while the merge is suspended in its
    // visitor; the visitor resumes only once that task has cancelled.
    {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        std::vector<std::string> out;
        seastar::promise<> first;
        auto cancelled = first.get_future().then(
          [&abort] { abort.request_abort(); });
        auto merged = co_await run(
          work,
          merge_limits(),
          observed_merge{
            &out, &budget, &handles, nullptr, 0, &first, &cancelled});
        require(
          !merged && merged.error().code() == errc::aborted
            && prefix(out, expected) && out.size() < expected.size(),
          "a merge did not give way to a task cancelling it");
        co_await unchanged("a merge cancelled from elsewhere kept a charge");
    }

    // Shortages: the same enumeration, or a clean resource error.
    const auto limits = budget.limits();
    const auto squeezed =
      [&](std::uint32_t tasks, byte_count bytes, std::uint32_t free_handles)
      -> seastar::future<bool> {
        std::vector<workload_reservation> held;
        held.reserve(limits.tasks);
        for (std::uint32_t i = 0; i + tasks < limits.tasks; ++i)
            held.push_back(take(budget.try_reserve(byte_count{1})));
        if (bytes < limits.bytes) {
            auto room = budget.try_reserve(
              byte_count{limits.bytes.value() - bytes.value()});
            if (room) held.push_back(std::move(*room));
        }
        if (free_handles < limits.handles) {
            if (held.empty())
                held.push_back(take(budget.try_reserve(byte_count{1})));
            take(
              held.back().try_acquire_handles(limits.handles - free_handles));
        }
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        std::vector<std::string> out;
        std::uint64_t peak = 0;
        auto merged = co_await run(
          work, merge_limits(), observed_merge{&out, &budget, &peak});
        held.clear();
        require(
          merged ? out == expected
                 : merged.error().code() == errc::resource_exhausted
                     || merged.error().code() == errc::queue_full,
          "a merge under a shortage failed otherwise or delivered a part");
        co_await unchanged("a merge under a shortage kept a charge");
        co_return merged.has_value();
    };
    bool refused = false, completed = false;
    for (std::uint32_t tasks = 1; tasks <= limits.tasks; tasks += 3) {
        const bool done = co_await squeezed(
          tasks, limits.bytes, limits.handles);
        refused = refused || !done;
        completed = completed || done;
    }
    require(refused && completed, "the task shortages did not span the merge");
    refused = completed = false;
    for (const auto bytes : {64_KiB, 512_KiB, 2_MiB, 8_MiB}) {
        const bool done = co_await squeezed(
          limits.tasks - 2, byte_count{bytes}, limits.handles);
        refused = refused || !done;
        completed = completed || done;
    }
    require(refused && completed, "the byte shortages did not span the merge");
    refused = completed = false;
    for (std::uint32_t free_handles = 0; free_handles <= 8; free_handles += 2) {
        const bool done = co_await squeezed(
          limits.tasks - 1, limits.bytes, free_handles);
        refused = refused || !done;
        completed = completed || done;
    }
    require(
      refused && completed, "the handle shortages did not span the merge");

    // One walk open at a time: the same enumeration, each segment read in one
    // run, and never more handles than open walks plus the WAL file.
    for (const std::uint32_t cursors : {1U, 2U}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        std::vector<std::string> out;
        std::uint64_t peak = 0;
        auto merged = take(
          co_await run(
            work, merge_limits(cursors), observed_merge{&out, &budget, &peak}));
        require(
          out == expected && peak <= cursors + 1U && merged.reloads == 0,
          "bounded walks changed the enumeration or held more handles");
    }
    co_await unchanged("bounded walks kept a charge");
    // Obligation rows stop at their bound. A queue of one runs each walk at
    // once: the same items, in another order.
    for (const bool rows : {false, true}) {
        auto bounded = merge_limits();
        if (rows)
            bounded.obligations = 1;
        else
            bounded.pending = 1;
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        std::vector<std::string> out;
        std::uint64_t peak = 0;
        auto merged = co_await run(
          work, bounded, observed_merge{&out, &budget, &peak});
        if (rows)
            require(
              !merged && merged.error().code() == errc::resource_exhausted,
              "a merge grew past its obligation bound");
        else {
            auto sorted = out;
            auto whole = expected;
            std::ranges::sort(sorted);
            std::ranges::sort(whole);
            require(
              merged && sorted == whole,
              "a queue of one changed what the merge classifies");
        }
        co_await unchanged("a bounded merge kept a charge");
    }
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION) && !defined(SEASTAR_DEBUG)
    // Planning, the merge's synchronous consumer, failing at its at-th
    // allocation: it throws, nothing stays charged, and once no failure is
    // reached it plans as before.
    {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        std::vector<recovery_item> items;
        auto merged = take(
          co_await drive.lifecycle(reconcile_local_recovery(
            files,
            owner,
            std::span<const local_device_spec>{specs},
            spec,
            0,
            control,
            std::nullopt,
            targets,
            budget,
            merge_limits(),
            work,
            kept_items{&items})));
        auto& injector = seastar::memory::local_failure_injector();
        bool reached = false;
        for (std::size_t at = 0; at != 1024; ++at) {
            bool injected = false, ready = false;
            try {
                injector.fail_after(at);
                auto planner = take(recovery_planner::make(targets, budget));
                for (const auto& item : items)
                    take(planner.observe(item));
                take(planner.finish(merged));
                ready = planner.ready();
            } catch (const std::bad_alloc&) {
            }
            injected = injector.failed();
            injector.cancel();
            co_await unchanged(
              "planning that failed to allocate kept a charge");
            if (!injected) {
                require(ready, "planning did not recover after the cuts");
                reached = true;
                break;
            }
        }
        require(reached, "planning allocations were never all passed");
    }
#endif
}

} // namespace kwaque::storage::testing::recovery_contract
