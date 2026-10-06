#pragma once

#include "src/base/units.h"
#include "src/storage/checkpoint.h"
#include "src/storage/recovery_inventory.h"
#include "src/storage/recovery_publication.h"
#include "src/storage/recovery_successor.h"
#include "src/storage/retry_lookup.h"
#include "src/storage/retry_snapshot.h"
#include "src/storage/tests/checkpoint_coverage_oracle.h"
#include "src/storage/tests/local_append_contract.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace kwaque::storage::testing::recovery_successor_contract {
using store_contract::require;
using store_contract::take;
namespace append_contract = local_append_contract;
namespace installation = installation_contract;

// Feeds every merged item to the planner and keeps the obligation rows.
struct restart_view final {
    recovery_planner* planner;
    std::vector<recovery_obligation>* rows;
    seastar::future<runtime::result<bool>>
    operator()(const recovery_item& item) const {
        if (auto observed = planner->observe(item); !observed)
            return seastar::make_ready_future<runtime::result<bool>>(
              runtime::failure(observed.error()));
        if (const auto* row = std::get_if<recovery_obligation>(&item))
            rows->push_back(*row);
        return seastar::make_ready_future<runtime::result<bool>>(true);
    }
};
inline local_segment_descriptor described(std::uint64_t generation) {
    auto value = segment_writer_contract::descriptor();
    value.segment = append_contract::context(generation);
    return value;
}
inline segment_writer_config segment_configuration(std::uint64_t retry) {
    auto config = segment_writer_contract::configuration();
    config.retry_object = local_object_sequence::make(retry).value();
    config.admission.working_bytes = byte_count{1_MiB};
    return config;
}
inline recovery_merge_limits merge_limits() {
    const scan_reader_limits reader{};
    return {
      {store_contract::limits(), reader, byte_count{4_MiB}, byte_count{64_KiB}},
      {store_contract::limits(), reader, byte_count{4_MiB}, byte_count{64_KiB}},
      2,
      16,
      8};
}
struct chain_link final {
    model::wal_incarnation_id incarnation;
    std::optional<runtime::file_position> sealed_end;
};

// A restart reads everything before it changes anything: the device, every
// durable decision and the catalog's segments. Then the recovered segment
// gets one fresh flush and only after it a recovering publication at its
// recovered boundary. The WAL head is closed, never resumed: one fresh flush
// of the head, then a successor whose predecessor cursor is its classified
// content end, and only then the control's new head. The head's bytes,
// including its uncertified tail, never change. The obligations recovery
// rebuilt pin their WAL file in the append path's own table, older than every
// new group and never expiring, and a table without room for them is
// refused. The recovered segment stays roll-required, so new appends use a
// new generation and the new head. flushes(path) counts a file's flushes
// where the backend can tell.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer,
  typename Flushes>
seastar::future<> recovered_successor(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer,
  Flushes flushes) {
    using control_type = local_control_owner<Backend, Owner>;
    using allocator_type = local_id_allocator<Backend, Owner>;
    using writer_type = wal_writer<Backend, Owner>;
    using segment_type = segment_writer<Backend, Owner, Clock>;
    using append_type = local_append<Backend, Owner, Clock>;
    struct appended final {
        local_wal_cursor begin, end;
        segment_block_layout block;
    };
    std::optional<local_wal_head> old_head;
    std::vector<appended> before;

    // Before the crash: two requests, each its own group with its footer.
    co_await append_contract::with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {},
      [&](auto& append, auto& writer, auto&, auto& targets, auto& work)
        -> seastar::future<> {
          old_head = writer.prepared_head();
          for (const std::uint64_t logical : {100U, 101U}) {
              const auto start = writer.progress()->reserved;
              auto outcome = co_await append_contract::settle(
                append.append(
                  *targets[0],
                  co_await append_contract::request(
                    append_contract::child_wire(logical), budget, work),
                  work),
                drive);
              const auto& receipt = append_contract::durable(outcome);
              before.push_back(
                {start, receipt.wal.boundary().cursor(), receipt.block});
          }
      });
    require(
      old_head && before.size() == 2
        && before[1].begin.incarnation() == old_head->incarnation,
      "the requests before the crash were not durable in one WAL file");
    const auto old_path = append_contract::wal_path(
      spec, old_head->incarnation);
    const auto old_segment = append_contract::segment_path(spec, 1);

    // The crash loses the second group's block and footer: its PREPARE is a
    // candidate.
    auto damaged = co_await append_contract::read_segment(
      files, spec, 1, drive);
    const auto lost = before[1].block.records.bytes().begin().value();
    std::fill(
      damaged.begin() + static_cast<std::ptrdiff_t>(lost), damaged.end(), '\0');
    co_await store_contract::write_bytes(files, old_segment, damaged, drive);
    const auto old_bytes = co_await append_contract::read_wal(
      files, spec, old_head->incarnation, drive);

    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array specs{spec};
    // The device first: an existing store this configuration owns.
    require(
      take(
        co_await drive.lifecycle(inspect_local_recovery(
          files,
          owner,
          std::span<const local_device_spec>{specs},
          budget,
          store_contract::limits(),
          work)))
          .verdict
        == recovery_store_verdict::ready,
      "the restarted store was not ready to recover");
    auto control = take(
      co_await drive.lifecycle(
        control_type::open(
          files,
          owner,
          spec,
          0,
          true,
          budget,
          store_contract::limits(),
          work)));
    std::unique_ptr<allocator_type> ids;
    std::unique_ptr<writer_type> writer;
    std::unique_ptr<wal_group_commit> commit;
    std::unique_ptr<segment_type> recovered, fresh;
    std::unique_ptr<append_type> append;
    std::unique_ptr<local_checkpoint<Backend, Owner, Clock>> checkpoint;
    runtime::first_failure failed;
    try {
        // Every durable decision, then the catalog's segments, then
        // classification and the read-only plan.
        const auto fields = take(control->snapshot()).fields;
        const std::array descriptors{described(1)};
        std::vector<recovery_decision_record> decided;
        auto discovered = take(
          co_await drive.lifecycle(discover_recovery_decisions(
            files,
            owner,
            spec,
            0,
            fields,
            descriptors,
            budget,
            {store_contract::limits(), 16},
            work,
            [&decided](const recovery_decision_record& value) {
                decided.push_back(value);
                return seastar::make_ready_future<runtime::result<bool>>(true);
            })));
        require(
          discovered.complete && discovered.decisions == 0 && decided.empty(),
          "a restart without decisions found one");
        const std::array catalog{recovery_catalog_entry{
          described(1),
          segment_header::make(
            append_contract::context(1),
            model::range_logical_end{100},
            alignment(8192))
            .value()}};
        auto found = take(
          co_await drive.lifecycle(open_recovery_inventory(
            files,
            owner,
            std::span<const local_device_spec>{specs},
            0,
            catalog,
            decided,
            budget,
            store_contract::limits(),
            work)));
        const auto& targets = found.targets;
        require(
          targets.size() == 1 && targets[0].state == local_object_state::active
            && targets[0].generation.value() == 1 && !targets[0].pin,
          "the catalog's segment was not opened as written");
        auto planner = take(recovery_planner::make(targets, budget));
        std::vector<recovery_obligation> rows;
        auto merged = take(
          co_await drive.lifecycle(reconcile_local_recovery(
            files,
            owner,
            std::span<const local_device_spec>{specs},
            spec,
            0,
            fields,
            std::nullopt,
            targets,
            budget,
            merge_limits(),
            work,
            restart_view{&planner, &rows})));
        take(planner.finish(merged));
        const auto content_end = before[1].end;
        require(
          planner.ready()
            && planner.wal().action == recovery_plan_action::activate_successor
            && planner.wal().predecessor == content_end
            && planner.segments()[0].action
                 == recovery_plan_action::publish_recovering
            && planner.segments()[0].candidates == 1,
          "the restart did not plan a successor after the head's content");
        require(
          rows.size() == 1 && rows[0].wal == old_head->incarnation
            && rows[0].satisfied == 1 && rows[0].pinned == 1
            && rows[0].first_pinned == before[1].begin,
          "the candidate's obligation was not rebuilt");

        // One fresh flush of the recovered segment, then its recovering
        // publication at the recovered boundary.
        const auto boundary = planner.segments()[0].boundary;
        const std::optional<std::uint64_t> segment_flushed = flushes(
          old_segment);
        auto published = take(
          co_await drive.lifecycle(
            publish_recovered_segments<Clock>(
              files,
              owner,
              std::span<const local_device_spec>{specs},
              0,
              planner,
              found,
              std::span<const recovery_visibility>{},
              budget,
              segment_configuration(45),
              {},
              work)));
        require(
          boundary && published.complete && published.segments.size() == 1
            && published.segments[0].published,
          "the recovered segment was not published as recovering");
        if (segment_flushed)
            require(
              flushes(old_segment) == *segment_flushed + 1,
              "the recovered segment was not flushed once before publication");

        ids = take(allocator_type::make(*control, budget, 4));
        const auto config = wal_writer_contract::configuration();
        // An owner for a fresh store cannot take a recovered head.
        auto refused = writer_type::make(
          *control, *ids, budget, config, wal_start_intent::known_unactivated);
        require(
          !refused && refused.error().code() == errc::wrong_context,
          "a recovered head was bootstrapped over");

        // A cursor the head's bytes do not hold activates nothing and
        // changes nothing; nor does a plan that is not ready.
        {
            auto attempt = take(
              writer_type::make(
                *control,
                *ids,
                budget,
                config,
                wal_start_intent::recovered_head));
            auto bootstrap = co_await drive.lifecycle(attempt->bootstrap(work));
            require(
              !bootstrap && bootstrap.error().code() == errc::invalid_argument,
              "a recovered head was bootstrapped");
            retained_wal found;
            take(found.extend(old_head->incarnation));
            const auto past = take(
              local_wal_cursor::make(
                old_head->incarnation,
                runtime::file_position{old_bytes.size() + 8192}));
            // The chain it is handed ends at the head it recovers.
            retained_wal other;
            take(other.extend(
              local_wal_high{}.checked_advance(0xee)->incarnation().value()));
            auto unnamed = co_await drive.lifecycle(
              attempt->activate_recovered(past, other, work));
            auto empty = co_await drive.lifecycle(
              attempt->activate_recovered(past, retained_wal{}, work));
            require(
              !unnamed && unnamed.error().code() == errc::invalid_argument
                && !empty && empty.error().code() == errc::invalid_argument,
              "a successor was activated for a chain that does not end at "
              "the recovered head");
            auto beyond = co_await drive.lifecycle(
              attempt->activate_recovered(past, found, work));
            require(
              !beyond && beyond.error().code() == errc::wrong_context,
              "a successor was activated past the head's bytes");
            static_cast<void>(co_await drive.lifecycle(attempt->close()));
        }
        require(
          take(control->snapshot()).fields.wal_head == *old_head,
          "a refused activation changed the control");
        writer = take(
          writer_type::make(
            *control, *ids, budget, config, wal_start_intent::recovered_head));
        {
            auto unplanned = take(recovery_planner::make(targets, budget));
            auto refused_plan = co_await drive.lifecycle(
              activate_recovered_wal(*writer, unplanned, work));
            require(
              !refused_plan
                && refused_plan.error().code() == errc::wrong_context,
              "a plan that was not ready activated a successor");
        }

        // The fresh barrier, then the successor, then the new head.
        const std::optional<std::uint64_t> flushed = flushes(old_path);
        take(
          co_await drive.lifecycle(
            activate_recovered_wal(*writer, planner, work)));
        const auto head = writer->prepared_head();
        const auto after = take(control->snapshot()).fields;
        require(
          head && head->incarnation != old_head->incarnation
            && after.wal_head == head
            && writer->progress()->reserved.incarnation() == head->incarnation,
          "the successor did not become the control's head");
        if (flushed)
            require(
              flushes(old_path) == *flushed + 1,
              "the recovered head was not flushed once before its successor");
        std::vector<chain_link> chain;
        auto visit_chain = [&chain](const local_wal_chain_entry& entry) {
            chain.push_back(
              {std::get<local_wal_descriptor>(entry.record.value.payload())
                 .incarnation,
               entry.sealed_end});
            return seastar::make_ready_future<runtime::result<bool>>(true);
        };
        auto walked = take(
          co_await drive.lifecycle(walk_local_wal_chain(
            files,
            owner,
            spec,
            0,
            after,
            std::nullopt,
            false,
            budget,
            store_contract::limits(),
            work,
            visit_chain)));
        require(
          walked.complete && chain.size() == 2
            && chain[0].incarnation == head->incarnation && !chain[0].sealed_end
            && chain[1].incarnation == old_head->incarnation
            && chain[1].sealed_end == content_end.position(),
          "the successor's predecessor cursor is not the head's content end");
        require(
          (co_await append_contract::read_wal(
            files, spec, old_head->incarnation, drive))
            == old_bytes,
          "activating a successor wrote the recovered head");

        wal_group_commit_config cohort;
        cohort.outstanding_groups = 2;
        cohort.maximum_wait = runtime::monotonic_duration{0};
        commit = take(wal_group_commit::make(*writer, budget, cohort));
        take(commit->template start<Clock>(timer));

        // Restored obligations take table rows; a table without room for
        // them all takes none.
        const auto pinned =
          [&](std::uint64_t generation, local_wal_cursor first) {
              return local_pinned_obligation{
                old_head->incarnation,
                append_contract::context(generation),
                1,
                first};
          };
        {
            auto small = take(
              append_type::make(
                budget,
                *commit,
                *writer,
                {.maximum_segments = 1, .maximum_requests = 1}));
            std::uint32_t visited = 0;
            const auto count = [&visited](const local_obligation&) {
                ++visited;
            };
            const std::array three{
              pinned(1, before[1].begin),
              pinned(3, before[0].begin),
              pinned(4, before[1].begin)};
            auto full = small->restore_pinned(three);
            require(
              !full && full.error().code() == errc::resource_exhausted
                && take(small->obligations(count)).obligations == 0,
              "obligations without room were partly restored");
            take(small->restore_pinned(std::span{three}.first(2)));
            const auto snapshot = take(small->obligations(count));
            require(
              snapshot.obligations == 2
                && snapshot.discharged == before[0].begin,
              "restored obligations lost their WAL order");
            // Only a segment's own sealed or deleting publication releases
            // all its pins, and only that segment's.
            const auto publication =
              [](std::uint64_t generation, local_object_state state) {
                  return local_object_publication{
                    append_contract::context(generation), state, {}, {}};
              };
            const auto active = small->discharge_pinned(
              publication(3, local_object_state::active));
            const auto absent = small->discharge_pinned(
              publication(4, local_object_state::sealed));
            require(
              !active && active.error().code() == errc::invalid_argument
                && !absent && absent.error().code() == errc::not_found
                && take(small->obligations(count)).obligations == 2,
              "a discharge without a resolving publication released a pin");
            take(small->discharge_pinned(
              publication(3, local_object_state::sealed)));
            const auto released = take(small->obligations(count));
            const auto repeated = small->discharge_pinned(
              publication(3, local_object_state::sealed));
            require(
              released.obligations == 1
                && released.discharged == before[1].begin && !repeated
                && repeated.error().code() == errc::not_found,
              "a sealed publication did not release exactly its pins");
            failed.observe(co_await drive.lifecycle(small->close()));
        }
        // A discard decision names one PREPARE and releases that one. With
        // two candidates of a segment pinned, discarding the older leaves
        // the other holding the WAL where it begins; one decision never
        // releases both.
        {
            auto pair = take(
              append_type::make(
                budget,
                *commit,
                *writer,
                {.maximum_segments = 1, .maximum_requests = 1}));
            const auto segment = append_contract::context(5);
            const std::array two{local_pinned_obligation{
              old_head->incarnation, segment, 2, before[0].begin}};
            take(pair->restore_pinned(two));
            const auto discard_of = [&segment](local_wal_cursor prepare) {
                return local_recovery_decision{
                  prepare,
                  codec::immutable_object_digest{codec::content_digest{}},
                  segment,
                  runtime::file_position{},
                  local_recovery_action::discard,
                  9};
            };
            const auto none = [](const local_obligation&) {};
            const auto both = pair->discharge_pinned(
              discard_of(before[0].begin), 0, std::nullopt);
            const auto neither = pair->discharge_pinned(
              discard_of(before[0].begin), 2, before[0].end);
            require(
              !both && both.error().code() == errc::wrong_context && !neither
                && neither.error().code() == errc::wrong_context
                && take(pair->obligations(none)).discharged == before[0].begin,
              "one discard decision released more than its PREPARE, or none");
            // Discarding the later candidate leaves the older one pinned
            // where it begins: no decision moves a PREPARE it does not name.
            const auto skipping = pair->discharge_pinned(
              discard_of(before[1].begin), 1, before[1].end);
            require(
              !skipping && skipping.error().code() == errc::wrong_context
                && take(pair->obligations(none)).discharged == before[0].begin,
              "a discard of a later PREPARE released the oldest one");
            take(pair->discharge_pinned(
              discard_of(before[0].begin), 1, before[1].begin));
            const auto one_left = take(pair->obligations(none));
            require(
              one_left.obligations == 1
                && one_left.discharged == before[1].begin,
              "a discard released a PREPARE it did not name");
            take(pair->discharge_pinned(
              discard_of(before[1].begin), 0, std::nullopt));
            require(
              take(pair->obligations(none)).obligations == 0,
              "the last candidate's discard did not release it");
            failed.observe(co_await drive.lifecycle(pair->close()));
        }

        append = take(append_type::make(budget, *commit, *writer, {}));
        const std::array restored{local_pinned_obligation{
          rows[0].wal, rows[0].segment, rows[0].pinned, *rows[0].first_pinned}};
        take(append->restore_pinned(restored));
        const auto pins_wal = [&]() {
            std::vector<local_obligation> seen;
            const auto snapshot = take(append->obligations(
              [&seen](const local_obligation& row) { seen.push_back(row); }));
            return snapshot.discharged == before[1].begin && seen.size() == 1
                   && seen[0].wal == old_head->incarnation
                   && seen[0].segment == append_contract::context(1)
                   && seen[0].pinned == 1
                   && !append->reclaimable(old_head->incarnation);
        };
        require(pins_wal(), "a restored candidate did not pin its WAL file");

        // The recovered generation is roll-required; appends use a new one.
        recovered = take(
          co_await drive.lifecycle(
            segment_type::open_existing(
              files,
              owner,
              spec,
              0,
              {local_publication_generation::make(2).value(),
               {append_contract::context(1),
                local_object_state::recovering,
                boundary,
                {}},
               described(1),
               {}},
              budget,
              segment_configuration(46),
              work)));
        require(
          take(recovered->roll_required()) && !append->attach(*recovered),
          "a recovered segment accepted appends");
        fresh = take(
          segment_type::make_new(
            files,
            owner,
            spec,
            0,
            described(2),
            budget,
            segment_configuration(47)));
        take(co_await drive.lifecycle(fresh->create_new(work)));
        const auto target = take(append->attach(*fresh));
        auto outcome = co_await append_contract::settle(
          append->append(
            target,
            co_await append_contract::request(
              append_contract::child_wire(100, 2), budget, work),
            work),
          drive);
        const auto& receipt = append_contract::durable(outcome);
        require(
          receipt.wal.boundary().cursor().incarnation() == head->incarnation,
          "a new append did not use the successor");
        auto again = append->restore_pinned(restored);
        require(
          !again && again.error().code() == errc::invalid_argument,
          "obligations were restored after appends began");

        // Time passes: nothing expires the candidate's obligation.
        co_await append_contract::probe(
          drive,
          timer,
          Clock::now()
            .checked_add(runtime::monotonic_duration{10'000'000'000})
            .value());
        require(pins_wal(), "a restored obligation expired");
        require(
          (co_await append_contract::read_wal(
            files, spec, old_head->incarnation, drive))
              == old_bytes
            && (co_await append_contract::read_segment(files, spec, 1, drive))
                 == damaged,
          "the recovered head or segment was written outside a decided seal");

        // The rebuilt rows fold to the one pin per segment that was restored.
        const auto shard = writer->progress()->owner;
        const auto folded = take(fold_pinned_obligations(rows, shard));
        require(
          folded.size() == 1 && folded[0].wal == restored[0].wal
            && folded[0].segment == restored[0].segment
            && folded[0].pinned == restored[0].pinned
            && folded[0].first == restored[0].first,
          "rebuilt rows did not fold to one pin per segment");
        // The candidate holds the cutoff at its PREPARE, whatever became
        // durable after it.
        const auto obligated = [&] {
            return take(append->obligations([](const local_obligation&) {}));
        };
        const auto held = take(
          checkpoint_cutoff(shard, obligated(), std::nullopt, std::nullopt));
        require(
          held == before[1].begin,
          "the cutoff passed an unresolved candidate's PREPARE");

        // A checkpoint run while the candidate is unresolved ends at its
        // PREPARE and no further, and the WAL file that holds it stays: no
        // run, however often it is asked for, drops a candidate. The WAL's
        // unreclaimed part begins where the recovered head's first PREPARE
        // did.
        checkpoint = take(
          local_checkpoint<Backend, Owner, Clock>::make(
            files,
            owner,
            std::span<const local_device_spec>{specs},
            spec,
            0,
            *control,
            *ids,
            *append,
            budget,
            {.pins = {}, .metadata = store_contract::limits()},
            codec::limits::defaults(),
            before[0].begin));
        require(
          !take(control->snapshot()).fields.checkpoint && !checkpoint->end(),
          "a store that never checkpointed has a checkpoint head");
        // A restarted shard cuts nothing before recovery's publications are
        // handed over: they are what covers the WAL the restart read.
        {
            const auto early = co_await drive.lifecycle(checkpoint->request());
            published.complete = false;
            const auto incomplete = checkpoint->recovered(published);
            published.complete = true;
            require(
              !early && early.error().code() == errc::wrong_context
                && !incomplete
                && incomplete.error().code() == errc::wrong_context
                && !take(control->snapshot()).fields.checkpoint && pins_wal(),
              "a restarted shard took a checkpoint before its recovering "
              "publications were durable");
        }
        take(checkpoint->recovered(published));
        const auto pinned_run = take(
          co_await drive.lifecycle(checkpoint->request()));
        const auto pinned_again = take(
          co_await drive.lifecycle(checkpoint->request()));
        require(
          pinned_run.published && pinned_run.end == held
            && pinned_run.entries == 1 && pinned_run.wal_removed == 0
            && pinned_run.wal_owed == 0 && !pinned_run.wal_failed
            && !pinned_again.published && pinned_again.end == held
            && pinned_again.wal_removed == 0 && checkpoint->end() == held
            && append->retained_wal().files() == 2 && pins_wal()
            && take(co_await drive.lifecycle(files.exists(old_path))),
          "a checkpoint passed an unresolved candidate or dropped its WAL");

        // The new segment's newest durable footer is pinned from its bytes
        // on the device, read once, and then carried without reading.
        std::vector<local_durable_boundary> durable;
        append->durable_boundaries(
          [&durable](const local_durable_boundary& one) {
              durable.push_back(one);
          });
        const auto& cut = receipt.segment.boundary();
        require(
          durable.size() == 1 && durable[0].device == spec.owner.device()
            && durable[0].history == cut.history()
            && durable[0].covered == cut.covered()
            && durable[0].footer == *cut.footer(),
          "the attachment's newest durable footer was not reported");
        auto pins = take(checkpoint_pins::make(budget, {}));
        take(pins.observe(durable[0]));
        auto first = take(pins.capture(shard, held));
        require(
          first.entries().empty() && first.refresh().size() == 1
            && !first.complete(),
          "a segment's first footer was not cut for reading");
        take(
          co_await drive.lifecycle(pin_durable_boundaries(
            files,
            owner,
            std::span<const local_device_spec>{specs},
            0,
            first,
            budget,
            store_contract::limits(),
            work)));
        const auto stored = co_await append_contract::read_segment(
          files, spec, 2, drive);
        const auto footer = stored.substr(
          cut.footer()->begin().value(), cut.footer()->size().value());
        const local_checkpoint_entry pinned_footer{
          append_contract::context(2),
          local_checkpoint_disposition::segment_boundary,
          spec.owner.device(),
          cut.footer()->begin().value(),
          cut.footer()->size(),
          6,
          codec::immutable_object_digest{
            codec::xxh3_128(footer.data(), footer.size())}};
        require(
          first.complete() && first.entries().size() == 1
            && first.entries()[0] == pinned_footer,
          "the boundary pin is not the footer the device holds");
        take(pins.adopt(std::move(first)));
        take(pins.observe(durable[0]));
        auto carried = take(pins.capture(shard, held));
        require(
          carried.complete() && carried.refresh().empty()
            && carried.entries().size() == 1
            && carried.entries()[0] == pinned_footer,
          "an unchanged entry was not carried forward");
        take(pins.adopt(std::move(carried)));
        // A footer on a device the store does not have is not read.
        auto elsewhere = durable[0];
        elsewhere.device
          = device_store_id::make(std::array<std::uint8_t, 16>{0x7f}).value();
        const auto unread = co_await drive.lifecycle(read_durable_boundary(
          files,
          owner,
          std::span<const local_device_spec>{specs},
          0,
          elsewhere,
          budget,
          store_contract::limits(),
          work));
        require(
          !unread && unread.error().code() == errc::wrong_context,
          "a footer was read from a device that was not named");

        // A discharge needs the durable decision that resolves the PREPARE
        // and the count it leaves; anything else releases nothing.
        const local_recovery_decision discard{
          before[1].begin,
          codec::immutable_object_digest{codec::content_digest{}},
          append_contract::context(1),
          runtime::file_position{lost},
          local_recovery_action::discard,
          7};
        const auto rejects = [&](
                               local_recovery_decision decision,
                               std::uint32_t remaining,
                               std::optional<local_wal_cursor> next,
                               errc expected) {
            const auto tried = append->discharge_pinned(
              decision, remaining, next);
            return !tried && tried.error().code() == expected;
        };
        auto preserve = discard;
        preserve.action = local_recovery_action::preserve;
        auto earlier = discard;
        earlier.prepare = before[0].begin;
        auto stranger = discard;
        stranger.segment = append_contract::context(9);
        require(
          rejects(preserve, 0, std::nullopt, errc::invalid_argument)
            && rejects(discard, 0, before[1].begin, errc::invalid_argument)
            && rejects(discard, 1, std::nullopt, errc::invalid_argument)
            && rejects(discard, 1, before[1].end, errc::wrong_context)
            && rejects(earlier, 0, std::nullopt, errc::wrong_context)
            && rejects(stranger, 0, std::nullopt, errc::not_found)
            && pins_wal(),
          "an unmatched discharge released a pinned PREPARE");
        // The checkpoint owner holds the decision before it releases
        // anything: a cut that passes the PREPARE must carry it.
        const recovery_decision_record record{
          {local_decision_sequence::make(1).value(),
           discard.prepare_digest,
           byte_count{4096}},
          discard};
        require(
          rejects(discard, 0, std::nullopt, errc::wrong_context) && pins_wal(),
          "a decision the checkpoint owner does not hold released a PREPARE");
        take(checkpoint->hold(record));
        take(append->discharge_pinned(discard, 0, std::nullopt));
        const auto settled = obligated();
        require(
          settled.obligations == 0
            && settled.discharged == writer->progress()->reserved
            && append->reclaimable(old_head->incarnation)
            && rejects(discard, 0, std::nullopt, errc::not_found),
          "a durable discard did not release its PREPARE exactly once");
        // The cutoff moves on to the WAL-durable end; a hold keeps it.
        const auto moved = take(
          checkpoint_cutoff(shard, settled, std::nullopt, held));
        require(
          moved == settled.wal_durable
            && take(checkpoint_cutoff(shard, settled, held, held)) == held,
          "the cutoff did not follow the discharged prefix");
        // The discard is carried once the cutoff has passed its PREPARE, and
        // leaves with the segment's sealed publication.
        take(pins.hold(record));
        auto early = take(pins.capture(shard, held));
        require(
          early.entries().size() == 1,
          "a discard was carried before the cutoff passed its PREPARE");
        take(pins.adopt(std::move(early)));
        auto late = take(pins.capture(shard, moved));
        require(
          late.complete() && late.entries().size() == 2
            && late.entries()[0].segment == append_contract::context(1)
            && late.entries()[0].disposition
                 == local_checkpoint_disposition::authorized_discard
            && late.entries()[0].locator == 1
            && late.entries()[1] == pinned_footer,
          "a passed discard was not carried in segment order");
        take(pins.adopt(std::move(late)));
        take(pins.cover(
          local_object_publication{
            append_contract::context(1), local_object_state::sealed, {}, {}}));
        require(
          pins.entries().size() == 1 && pins.entries()[0] == pinned_footer,
          "a sealed publication did not release its segment's discard");

        // With the candidate discarded, a run publishes the table as one
        // bundle and only then moves the control's head past its PREPARE.
        const auto head_of = [&] {
            return take(control->snapshot()).fields.checkpoint;
        };
        const auto bundle_path = [&](const local_root_reference& head) {
            return take(
              take(local_paths::make(spec.root))
                .sequence_file(
                  0, local_sequence_file::checkpoint, head.sequence().value()));
        };
        const auto root_of = [&](const local_root_reference& head)
          -> seastar::future<local_checkpoint_root> {
            auto loaded = take(
              co_await drive.lifecycle(load_local_checkpoint_root(
                files, spec, 0, head, budget, store_contract::limits(), work)));
            co_return std::get<local_checkpoint_root>(loaded.value.payload());
        };
        take(checkpoint->hold(record));
        require(
          head_of() && checkpoint->end() == held,
          "the head moved before a run that could pass the candidate");
        const auto first_run = take(
          co_await drive.lifecycle(checkpoint->request()));
        const auto head1 = head_of();
        require(
          first_run.published && first_run.end == settled.wal_durable
            && first_run.entries == 2 && first_run.unretired == 0 && head1
            && checkpoint->end() == first_run.end
            && control->checkpoint_end() == first_run.end
            && !checkpoint->running(),
          "a checkpoint did not move the head to the cutoff");
        // Only now does the WAL below the cutoff's file go: the recovered
        // head, oldest first. The file the cutoff lies in is the chain's
        // start from here on, and it is the head.
        require(
          first_run.wal_removed == 1 && first_run.wal_owed == 0
            && !first_run.wal_failed
            && !take(co_await drive.lifecycle(files.exists(old_path)))
            && take(
              co_await drive.lifecycle(files.exists(
                append_contract::wal_path(spec, head->incarnation))))
            && append->retained_wal().files() == 1
            && append->retained_wal().newest() == head->incarnation,
          "the WAL below the cutoff's file was not removed as a prefix");
        const auto first_root = co_await root_of(*head1);
        const auto first_file = co_await store_contract::read_all_bytes(
          files, bundle_path(*head1), drive);
        const auto first_page = first_file.substr(head1->bytes().value());
        require(
          first_root.begin == held && first_root.end == first_run.end
            && first_root.entry_count == 2 && first_root.pages.size() == 1
            && first_page.size() == first_root.pages[0].encoded_bytes().value()
            && codec::immutable_object_digest{codec::xxh3_128(
                 first_page.data(), first_page.size())}
                 == first_root.pages[0].digest(),
          "the bundle is not a root over the page it pins");
        const auto& table = checkpoint->pins().entries();
        require(
          table.size() == 2 && table[0].segment == append_contract::context(1)
            && table[0].disposition
                 == local_checkpoint_disposition::authorized_discard
            && table[1] == pinned_footer,
          "the durable checkpoint's table was not adopted");
        // With the cutoff where it was, nothing is written.
        const auto idle = take(co_await drive.lifecycle(checkpoint->request()));
        require(
          !idle.published && idle.end == first_run.end && head_of() == head1
            && idle.wal_removed == 0 && idle.wal_owed == 0,
          "a checkpoint was published although the cutoff stood still");

        auto more = co_await append_contract::settle(
          append->append(
            target,
            co_await append_contract::request(
              append_contract::child_wire(101, 2), budget, work),
            work),
          drive);
        const auto& grown = append_contract::durable(more).segment.boundary();
        // A hold keeps the cutoff, as for a scan that reads from it.
        {
            const auto hold = take(checkpoint->hold_cutoff());
            const auto held_run = take(
              co_await drive.lifecycle(checkpoint->request()));
            require(
              hold.from() == first_run.end && checkpoint->holds() == 1
                && !held_run.published && held_run.end == first_run.end
                && head_of() == head1,
              "a checkpoint moved the cutoff past a hold");
        }
        require(checkpoint->holds() == 0, "a released hold was kept");
        // Requests made while a run is in flight are served together by
        // the one run after it.
        auto leading = checkpoint->request();
        auto second = checkpoint->request();
        auto third = checkpoint->request();
        require(checkpoint->running(), "a checkpoint run did not start");
        const auto second_run = take(
          co_await drive.lifecycle(std::move(leading)));
        const auto joined = take(co_await drive.lifecycle(std::move(second)));
        const auto also = take(co_await drive.lifecycle(std::move(third)));
        const auto head2 = head_of();
        require(
          second_run.published && second_run.entries == 2
            && second_run.unretired == 0
            && second_run.end == writer->progress()->durable
            && !joined.published && !also.published
            && joined.end == second_run.end && also.end == second_run.end
            && head2 && head2->sequence() > head1->sequence()
            && checkpoint->end() == second_run.end && !checkpoint->running(),
          "requests during a run were not served by one following run");
        const auto second_root = co_await root_of(*head2);
        require(
          second_root.begin == first_run.end
            && second_root.end == second_run.end
            && second_root.entry_count == 2,
          "a checkpoint did not continue the one before it");
        // The segment's refreshed footer is pinned, and the bundle only the
        // previous head referenced is gone now that the new head is durable.
        require(
          checkpoint->pins().entries()[1].locator
              == grown.footer()->begin().value()
            && !take(
              co_await drive.lifecycle(files.exists(bundle_path(*head1))))
            && take(
              co_await drive.lifecycle(files.exists(bundle_path(*head2)))),
          "the previous checkpoint was not retired after the new head");

        // A hold taken while a run is in flight wins: the head stays, and
        // nothing the durable checkpoint keeps is released or replaced.
        static_cast<void>(append_contract::durable(
          co_await append_contract::settle(
            append->append(
              target,
              co_await append_contract::request(
                append_contract::child_wire(102, 2), budget, work),
              work),
            drive)));
        {
            auto racing = checkpoint->request();
            const auto hold = take(checkpoint->hold_cutoff());
            const auto lost = co_await drive.lifecycle(std::move(racing));
            require(
              !lost && lost.error().code() == errc::queue_full
                && head_of() == head2 && checkpoint->end() == second_run.end
                && !checkpoint->failure().failed()
                && checkpoint->pins().entries().size() == 2
                && checkpoint->pins().entries()[1].locator
                     == grown.footer()->begin().value()
                && take(
                  co_await drive.lifecycle(files.exists(bundle_path(*head2)))),
              "a checkpoint moved the head past a hold taken during its run");
        }
        const auto third_run = take(
          co_await drive.lifecycle(checkpoint->request()));
        const auto head3 = head_of();
        require(
          third_run.published && third_run.unretired == 0
            && third_run.end == writer->progress()->durable && head3
            && head3->sequence() > head2->sequence()
            && !take(
              co_await drive.lifecycle(files.exists(bundle_path(*head2)))),
          "a checkpoint did not resume once its hold was released");

        // A durable bundle that does not continue the durable checkpoint is
        // never installed, whatever its sequence.
        {
            const auto sequence = take(
              co_await drive.lifecycle(ids->allocate_object(work)));
            const seastar::chunked_vector<local_checkpoint_entry> none;
            detail::checkpoint_pages pages{
              &none,
              shard,
              spec.identity.metadata_alignment,
              sequence,
              467,
              store_contract::limits()};
            auto stale = take(
              co_await drive.lifecycle(
                detail::encode_checkpoint_root(
                  pages, before[0].begin, first_run.end, work)));
            const auto reference = stale.reference;
            auto bundle = take(
              co_await drive.lifecycle(
                local_bundle::make(
                  reference,
                  std::move(stale.expected),
                  std::move(stale.bytes),
                  budget,
                  store_contract::limits(),
                  work)));
            const auto ready = [](codec::cooperative_work&) {
                return seastar::make_ready_future<runtime::result<void>>(
                  runtime::result<void>{});
            };
            const auto published = co_await drive.lifecycle(
              publish_local_bundle(
                files,
                owner,
                spec,
                0,
                std::move(bundle),
                pages,
                ready,
                budget,
                work));
            require(
              published.reference == reference,
              "a well-formed bundle was not published");
            const auto installed = co_await drive.lifecycle(control->update(
              [reference](
                local_shard_control& fields) -> runtime::result<void> {
                  fields.checkpoint = reference;
                  return {};
              },
              [](const auto&, const auto&, auto&) {
                  return seastar::make_ready_future<runtime::result<void>>(
                    runtime::result<void>{});
              },
              work));
            require(
              installed.failure.error()
                && installed.failure.error()->code() == errc::wrong_context
                && head_of() == head3
                && control->checkpoint_end() == third_run.end
                && !control->fenced(),
              "a checkpoint that moves the cutoff back was installed");
        }

        // Completed-retry facts become durable only through a cut: one
        // immutable snapshot beside the segment, then the segment's own
        // publication pointing at it.
        {
            auto retries = take(
              retry_snapshot<Backend, Owner, Clock>::make(
                files,
                owner,
                std::span<const local_device_spec>{specs},
                0,
                *append,
                target,
                *fresh,
                *ids,
                budget,
                store_contract::limits(),
                codec::limits::defaults()));
            runtime::first_failure cutting;
            try {
                const auto demand = [&](bool forced) {
                    return drive.lifecycle(retries->request(forced));
                };
                const auto object_path = [&](const local_root_reference& root) {
                    return take(take(local_paths::make(spec.root))
                                  .object(
                                    0,
                                    {append_contract::context(2).segment(),
                                     append_contract::context(2).generation()},
                                    root.sequence()));
                };
                const auto published =
                  [&]() -> seastar::future<local_object_publication> {
                    auto selected = take(
                      co_await drive.lifecycle(
                        storage::detail::select_recovery_publication(
                          files,
                          spec,
                          0,
                          described(2),
                          budget,
                          store_contract::limits(),
                          work)));
                    co_return std::move(selected.second);
                };
                // With nothing recorded even a forced demand writes nothing.
                const auto none = take(co_await demand(true));
                require(
                  !none.cut && none.durable == 0 && none.unsaved == 0
                    && !none.snapshot && retries->durable() == 0
                    && (co_await published()).roots.empty(),
                  "a cut was written without a recorded fact");
                const auto one = append_contract::completed_fact(receipt);
                const auto two = append_contract::completed_fact(
                  append_contract::durable(more));
                take(append->record_completed_retry(target, one));
                // Recording alone makes nothing durable.
                require(
                  retries->durable() == 0
                    && (co_await published()).roots.empty(),
                  "a recorded fact was durable before any cut");
                const auto first_cut = take(co_await demand(false));
                require(
                  first_cut.cut && first_cut.durable == 1
                    && first_cut.unsaved == 0 && first_cut.written == 1
                    && first_cut.snapshot && first_cut.boundary
                    && !first_cut.unretired && retries->durable() == 1
                    && take(
                      co_await drive.lifecycle(
                        files.exists(object_path(*first_cut.snapshot)))),
                  "the first fact was not cut into a durable snapshot");
                const auto pointed = co_await published();
                require(
                  pointed.state == local_object_state::active
                    && pointed.boundary == first_cut.boundary
                    && pointed.roots.size() == 1
                    && pointed.roots[0] == *first_cut.snapshot
                    && first_cut.snapshot->kind()
                         == local_root_kind::completed_retry_snapshot,
                  "the segment's publication does not point at the snapshot");
                // The unsaved facts are as many as the saved: a cut is due.
                take(append->record_completed_retry(target, two));
                const auto second_cut = take(co_await demand(false));
                require(
                  second_cut.cut && second_cut.durable == 2
                    && second_cut.written == 2 && second_cut.unsaved == 0
                    && second_cut.snapshot != first_cut.snapshot
                    && !second_cut.unretired
                    && !take(
                      co_await drive.lifecycle(
                        files.exists(object_path(*first_cut.snapshot))))
                    && take(
                      co_await drive.lifecycle(
                        files.exists(object_path(*second_cut.snapshot)))),
                  "a doubled fact set was not cut, or kept the old snapshot");
                // One unsaved fact against two saved is not due unless
                // forced.
                auto extra = co_await append_contract::settle(
                  append->append(
                    target,
                    co_await append_contract::request(
                      append_contract::child_wire(103, 2), budget, work),
                    work),
                  drive);
                const auto three = append_contract::completed_fact(
                  append_contract::durable(extra));
                take(append->record_completed_retry(target, three));
                const auto waiting = take(co_await demand(false));
                require(
                  !waiting.cut && waiting.durable == 2 && waiting.unsaved == 1
                    && waiting.written == 0
                    && waiting.snapshot == second_cut.snapshot
                    && (co_await published()).roots[0] == *second_cut.snapshot,
                  "a demand that was not due cut a snapshot");
                // Demands made while a cut is in flight are served together
                // by the one cut after it.
                auto leading = retries->request(true);
                auto joining = retries->request(false);
                require(retries->running(), "a forced cut did not start");
                const auto third_cut = take(
                  co_await drive.lifecycle(std::move(leading)));
                const auto joined = take(
                  co_await drive.lifecycle(std::move(joining)));
                const auto final_pointer = co_await published();
                require(
                  third_cut.cut && third_cut.durable == 3
                    && third_cut.written == 3 && third_cut.unsaved == 0
                    && !joined.cut && joined.durable == 3 && joined.written == 0
                    && joined.snapshot == third_cut.snapshot
                    && !retries->running()
                    && final_pointer.boundary == third_cut.boundary
                    && final_pointer.roots.size() == 1
                    && final_pointer.roots[0] == *third_cut.snapshot
                    && third_cut.boundary->position()
                         >= second_cut.boundary->position(),
                  "a forced cut did not save every recorded fact");
                take(co_await drive.lifecycle(retries->close()));

                // At seal the snapshot leaves the publication, so the seal's
                // source must hold every fact it does: one with fewer is
                // refused before anything is written.
                struct no_facts final {
                    seastar::future<
                      runtime::result<std::vector<completed_retry>>>
                    read(std::uint32_t, std::uint32_t, codec::cooperative_work&)
                      const {
                        return seastar::make_ready_future<
                          runtime::result<std::vector<completed_retry>>>(
                          std::vector<completed_retry>{});
                    }
                };
                const auto reserved = fresh->progress()->reserved.retry_entries;
                const auto dropped = co_await drive.lifecycle(
                  fresh->seal(no_facts{}, 2, reserved - 2, work));
                require(
                  dropped.failure.error()
                    && dropped.failure.error()->code() == errc::invalid_argument
                    && (co_await published()).roots[0] == *third_cut.snapshot,
                  "a seal source that lacks durable facts was accepted");
                auto detached = take(append->detach(target));
                const auto completed = detached.retry.completed();
                const auto sealed = co_await drive.lifecycle(fresh->seal(
                  std::move(detached.retry),
                  completed,
                  reserved - completed,
                  work));
                require(
                  !sealed.failure.failed() && completed == 3 && sealed.retry
                    && sealed.boundary,
                  "the segment did not seal with its recorded facts");
                // The sealed summary holds the facts now; the snapshot left
                // the publication and went only after it was durable.
                const auto summarized = co_await published();
                require(
                  summarized.state == local_object_state::sealed
                    && summarized.boundary == sealed.boundary
                    && summarized.roots.size() == 1
                    && summarized.roots[0] == *sealed.retry
                    && !take(
                      co_await drive.lifecycle(
                        files.exists(object_path(*third_cut.snapshot)))),
                  "the seal did not supersede the snapshot");

                // A fact supplied after the seal goes to a snapshot beside
                // the summary that holds only what the summary lacks. Lookup
                // asks that snapshot first, then the sealed summary.
                const auto late_fact = [&one](std::uint64_t offset) {
                    const auto id = one.id();
                    return completed_retry::make(
                             model::batch_id::make(
                               id.producer(),
                               id.epoch(),
                               id.stream(),
                               model::batch_sequence{
                                 id.sequence().value() + offset})
                               .value(),
                             one.submitted_digest(),
                             one.original_binding(),
                             one.returned_span(),
                             one.ack_generation())
                      .value();
                };
                const auto fourth = late_fact(1000), fifth = late_fact(2000);
                // The descriptor is borrowed until the open is joined, so it
                // outlives the call that starts it.
                const auto sealed_descriptor = described(2);
                const auto generation_of = [&] {
                    return drive.lifecycle(open_recovery_generation(
                      files,
                      owner,
                      spec,
                      0,
                      sealed_descriptor,
                      budget,
                      store_contract::limits(),
                      work));
                };
                std::optional<local_recovery_generation> reopened;
                std::unique_ptr<late_retry_snapshot<Backend, Owner, Clock>>
                  late;
                runtime::first_failure reading;
                const auto release = [&]() -> seastar::future<> {
                    if (late) {
                        reading.observe(
                          co_await drive.lifecycle(late->close()));
                        late.reset();
                    }
                    if (reopened && reopened->owner) {
                        reopened->owner->retire();
                        reading.observe(
                          co_await drive.lifecycle(reopened->owner->close()));
                    }
                    reopened.reset();
                };
                try {
                    reopened.emplace(take(co_await generation_of()));
                    require(
                      reopened->owner
                        && reopened->publication.state
                             == local_object_state::sealed,
                      "the sealed segment did not reopen under its pins");
                    {
                        const auto pin = take(reopened->owner->pin());
                        auto lookup = take(
                          co_await drive.lifecycle(
                            completed_retry_lookup::open(pin, budget, work)));
                        const auto cold = lookup.pages_read();
                        const auto found = take(
                          co_await drive.lifecycle(
                            lookup.find(two.id(), work)));
                        const auto absent = take(
                          co_await drive.lifecycle(
                            lookup.find(fourth.id(), work)));
                        std::vector<completed_retry> walked;
                        const bool whole = take(
                          co_await drive.lifecycle(lookup.enumerate(
                            [&walked](
                              const completed_retry& fact, retry_source) {
                                walked.push_back(fact);
                                return true;
                            },
                            work)));
                        require(
                          !lookup.snapshot() && lookup.summary()
                            && lookup.facts() == 3 && cold == 1 && found
                            && found->fact == two
                            && found->source == retry_source::summary && !absent
                            && whole && walked.size() == 3
                            && walked[0].id().canonical_less(walked[1].id())
                            && walked[1].id().canonical_less(walked[2].id()),
                          "the sealed summary did not return its originals");
                        auto summary = take(
                          co_await drive.lifecycle(
                            retry_root_reader::open(
                              take(pin.root(local_root_kind::sealed_retry)),
                              budget,
                              work)));
                        const auto current = reopened->publication;
                        late = take(
                          co_await drive.lifecycle(
                            late_retry_snapshot<Backend, Owner, Clock>::open(
                              files,
                              owner,
                              std::span<const local_device_spec>{specs},
                              0,
                              *fresh,
                              described(2),
                              current,
                              std::move(summary),
                              *ids,
                              budget,
                              store_contract::limits(),
                              codec::limits::defaults(),
                              8)));
                    }
                    const auto record = [&](const completed_retry& fact) {
                        return drive.lifecycle(late->record(fact, work));
                    };
                    // What the summary holds is not kept again, and a
                    // differing fact names its first differing field.
                    const auto known = take(co_await record(two));
                    auto digest = two.submitted_digest().bytes();
                    digest[0] ^= 1U;
                    const auto differing = take(
                      co_await record(
                        completed_retry::make(
                          two.id(),
                          codec::semantic_batch_digest{digest},
                          two.original_binding(),
                          two.returned_span(),
                          two.ack_generation())
                          .value()));
                    require(
                      known.status == completed_retry_status::exists
                        && known.stored == two
                        && differing.status
                             == completed_retry_status::
                               exists_with_different_submitted_digest
                        && differing.stored == two && late->unsaved() == 0,
                      "a fact the sealed summary holds was kept again");
                    const auto created = take(co_await record(fourth));
                    const auto repeated = take(co_await record(fourth));
                    require(
                      created.status == completed_retry_status::created
                        && repeated.status == completed_retry_status::exists
                        && late->unsaved() == 1 && late->durable() == 0
                        && (co_await published()).roots.size() == 1,
                      "a late fact was not kept apart from what is durable");
                    const auto late_cut = take(
                      co_await drive.lifecycle(late->request(false)));
                    const auto beside = co_await published();
                    require(
                      late_cut.cut && late_cut.durable == 1
                        && late_cut.written == 1 && late_cut.unsaved == 0
                        && late_cut.snapshot
                        && late_cut.boundary == sealed.boundary
                        && beside.state == local_object_state::sealed
                        && beside.boundary == sealed.boundary
                        && beside.roots.size() == 2
                        && beside.roots[0] == *sealed.retry
                        && beside.roots[1] == *late_cut.snapshot,
                      "a late snapshot did not carry the sealed roots");
                    // Durable now: found in the snapshot, not kept again. The
                    // next cut merges the snapshot with what was kept since.
                    const auto saved = take(co_await record(fourth));
                    const auto second = take(co_await record(fifth));
                    const auto merged_cut = take(
                      co_await drive.lifecycle(late->request(false)));
                    const auto replaced = co_await published();
                    require(
                      saved.status == completed_retry_status::exists
                        && saved.stored == fourth
                        && second.status == completed_retry_status::created
                        && merged_cut.cut && merged_cut.durable == 2
                        && merged_cut.written == 2 && merged_cut.unsaved == 0
                        && merged_cut.snapshot != late_cut.snapshot
                        && !merged_cut.unretired && replaced.roots.size() == 2
                        && replaced.roots[0] == *sealed.retry
                        && replaced.roots[1] == *merged_cut.snapshot
                        && !take(
                          co_await drive.lifecycle(
                            files.exists(object_path(*late_cut.snapshot))))
                        && take(
                          co_await drive.lifecycle(
                            files.exists(object_path(*merged_cut.snapshot)))),
                      "a late cut did not merge the snapshot it replaced");
                    co_await release();
                    // Under the new publication a lookup finds late facts in
                    // the snapshot and the others in the summary.
                    reopened.emplace(take(co_await generation_of()));
                    {
                        const auto pin = take(reopened->owner->pin());
                        auto lookup = take(
                          co_await drive.lifecycle(
                            completed_retry_lookup::open(pin, budget, work)));
                        const auto early = take(
                          co_await drive.lifecycle(
                            lookup.find(one.id(), work)));
                        const auto newest = take(
                          co_await drive.lifecycle(
                            lookup.find(fifth.id(), work)));
                        const auto never = take(
                          co_await drive.lifecycle(
                            lookup.find(late_fact(3000).id(), work)));
                        require(
                          lookup.snapshot() && lookup.summary()
                            && lookup.facts() == 5 && early
                            && early->fact == one
                            && early->source == retry_source::summary && newest
                            && newest->fact == fifth
                            && newest->source == retry_source::snapshot
                            && !never,
                          "a lookup did not ask the snapshot, then the "
                          "summary");
                    }
                    // Nothing in the segment's directory is unreferenced.
                    require(
                      take(
                        co_await drive.lifecycle(remove_unreferenced_objects(
                          files, owner, spec, 0, reopened->publication, work)))
                        == 0,
                      "a referenced bundle was taken for debris");
                } catch (...) {
                    reading.observe(std::current_exception());
                }
                co_await release();
                take(reading.outcome());
            } catch (...) {
                cutting.observe(std::current_exception());
            }
            cutting.observe(co_await drive.lifecycle(retries->close()));
            take(cutting.outcome());
        }

        // A reopen reads the head's bundle whole, under the control's
        // reference and the root's page pins, and takes the cutoff and the
        // table from it.
        {
            const auto now = take(control->snapshot()).fields;
            auto loaded = take(
              co_await drive.lifecycle(load_checkpoint(
                files,
                owner,
                spec,
                0,
                now,
                budget,
                store_contract::limits(),
                work)));
            const auto& carried = checkpoint->pins().entries();
            require(
              loaded && loaded->reference == *head3
                && loaded->end == third_run.end
                && loaded->begin == second_run.end
                && loaded->entries.size() == carried.size()
                && std::equal(
                  loaded->entries.begin(),
                  loaded->entries.end(),
                  carried.begin()),
              "the reopened checkpoint is not the durable one");
            // The discard entry needs its durable decision, and the boundary
            // entry its segment in the catalog.
            seastar::chunked_vector<local_checkpoint_entry> discards;
            discards.push_back(loaded->entries[0]);
            const std::array known{record};
            const auto resumed = take(
              resume_from_checkpoint(discards, found, known));
            const auto undecided = resume_from_checkpoint(discards, found, {});
            const auto unknown = resume_from_checkpoint(
              loaded->entries, found, known);
            require(
              resumed.discards == 1 && resumed.raised == 0
                && resumed.superseded == 0 && !undecided
                && undecided.error().code() == errc::not_found && !unknown
                && unknown.error().code() == errc::not_found,
              "a checkpoint entry resumed without what it pins");
            // Every bundle but the head's is unreferenced, whichever side
            // of the head its sequence lies on.
            const auto stale = take(
              co_await drive.lifecycle(remove_stale_checkpoints(
                files, owner, spec, 0, *control, work)));
            const auto again = take(
              co_await drive.lifecycle(remove_stale_checkpoints(
                files, owner, spec, 0, *control, work)));
            require(
              stale == 1 && again == 0
                && take(
                  co_await drive.lifecycle(files.exists(bundle_path(*head3)))),
              "a reopen did not remove exactly the unreferenced bundles");
            // Nothing below the cutoff's file is left to remove, and the
            // file it lies in stays.
            const auto below = take(
              co_await drive.lifecycle(
                remove_wal_below(files, owner, spec, 0, *control, work)));
            require(
              below.removed == 0 && below.kept == 1
                && take(
                  co_await drive.lifecycle(files.exists(
                    append_contract::wal_path(spec, head->incarnation)))),
              "a reopen removed a WAL file at or above the cutoff");
            // A name below the cutoff that came back, as after a crash that
            // undid its removal, goes again without being read: what it
            // holds is not a WAL file at all.
            const auto inspected = [&]() -> seastar::future<int> {
                const auto report = co_await drive.lifecycle(
                  inspect_local_recovery(
                    files,
                    owner,
                    std::span<const local_device_spec>{specs},
                    budget,
                    store_contract::limits(),
                    work));
                co_return report ? static_cast<int>(report->verdict)
                                 : -1 - static_cast<int>(report.error().code());
            };
            const auto clean = co_await inspected();
            co_await store_contract::write_bytes(
              files, old_path, std::string(4096, 'x'), drive);
            // The inspection that precedes a restart does not open it either.
            require(
              co_await inspected() == clean,
              "a WAL name below the cutoff changed what a restart finds");
            const auto undone = take(
              co_await drive.lifecycle(
                remove_wal_below(files, owner, spec, 0, *control, work)));
            require(
              undone.removed == 1 && undone.kept == 1
                && !take(co_await drive.lifecycle(files.exists(old_path)))
                && take(
                  co_await drive.lifecycle(files.exists(
                    append_contract::wal_path(spec, head->incarnation)))),
              "a WAL name below the cutoff survived a reopen");
            // A table continues only from the head it was loaded under.
            auto restored = take(
              checkpoint_pins::make(budget, checkpoint_pin_limits{}));
            const auto count = loaded->entries.size();
            take(restored.restore(std::move(*loaded)));
            require(
              restored.entries().size() == count
                && std::equal(
                  restored.entries().begin(),
                  restored.entries().end(),
                  carried.begin()),
              "the durable table was not restored as it was carried");
            // The owner's second lifetime, as after a reopen. A new owner
            // over a head it did not write cuts nothing until it has that
            // head's table: a checkpoint from an empty table would retire
            // the bundle that carries every earlier entry.
            take(co_await drive.lifecycle(checkpoint->close()));
            checkpoint.reset();
            checkpoint = take(
              local_checkpoint<Backend, Owner, Clock>::make(
                files,
                owner,
                std::span<const local_device_spec>{specs},
                spec,
                0,
                *control,
                *ids,
                *append,
                budget,
                {.pins = {}, .metadata = store_contract::limits()},
                codec::limits::defaults(),
                before[0].begin));
            take(checkpoint->recovered(published));
            const auto unrestored = co_await drive.lifecycle(
              checkpoint->request());
            require(
              !unrestored && unrestored.error().code() == errc::wrong_context
                && head_of() == head3 && checkpoint->pins().entries().empty(),
              "an owner that did not restore the head's table took a "
              "checkpoint");
            auto reloaded = take(
              co_await drive.lifecycle(load_checkpoint(
                files,
                owner,
                spec,
                0,
                take(control->snapshot()).fields,
                budget,
                store_contract::limits(),
                work)));
            require(reloaded.has_value(), "the head's checkpoint did not load");
            take(checkpoint->restore(std::move(*reloaded)));
            const auto resumed_run = take(
              co_await drive.lifecycle(checkpoint->request()));
            // It continues the durable checkpoint: a cut that moved begins
            // where that one ended and carries its entries forward.
            if (resumed_run.published) {
                const auto resumed_head = head_of();
                require(
                  resumed_head && !(*resumed_head == *head3)
                    && resumed_run.entries == count
                    && (co_await root_of(*resumed_head)).begin == third_run.end,
                  "a restored owner did not continue the head it restored");
            } else {
                require(
                  resumed_run.end == third_run.end && head_of() == head3,
                  "a restored owner moved a cutoff that had not moved");
            }
            require(
              checkpoint->pins().entries().size() == count
                && !checkpoint->failure().failed(),
              "a restored owner lost an entry it carried");
            // The first segment's sealed publication takes over from its
            // entries: only the other segment's pin is left.
            take(checkpoint->cover(
              local_object_publication{
                append_contract::context(1),
                local_object_state::sealed,
                {},
                {}}));
            require(
              checkpoint->pins().entries().size() + 1 == count,
              "a sealed publication did not release its segment's entries");
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    const auto close = [&](auto operation) -> seastar::future<> {
        try {
            failed.observe(co_await drive.lifecycle(std::move(operation)));
        } catch (...) {
            failed.observe(std::current_exception());
        }
    };
    if (checkpoint) co_await close(checkpoint->close());
    checkpoint.reset();
    if (append) co_await close(append->close());
    append.reset();
    if (fresh) co_await close(fresh->close());
    fresh.reset();
    if (recovered) co_await close(recovered->close());
    recovered.reset();
    if (commit) co_await close(commit->close());
    commit.reset();
    if (writer) co_await close(writer->close());
    writer.reset();
    if (ids) co_await close(ids->close());
    ids.reset();
    co_await close(control->close());
    control.reset();
    take(failed.outcome());
}

// Restart after restart, the device crashing between them where the backend
// can. Every restart reads the device, its decisions and the catalog before
// changing anything. The recovered segment is published as recovering once,
// and its pin never lowers or moves; its candidate, and the obligation that
// pins the original WAL file, survive every restart. Each restart closes its
// head with one successor, and the original head's bytes and the segment's
// bytes never change.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> repeated_restarts(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    using control_type = local_control_owner<Backend, Owner>;
    using allocator_type = local_id_allocator<Backend, Owner>;
    using writer_type = wal_writer<Backend, Owner>;
    constexpr bool crashable = requires(Backend& backend) { backend.crash(); };
    struct appended final {
        local_wal_cursor begin, end;
        segment_block_layout block;
    };
    std::optional<local_wal_head> old_head;
    std::vector<appended> before;
    co_await append_contract::with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {},
      [&](auto& append, auto& writer, auto&, auto& targets, auto& work)
        -> seastar::future<> {
          old_head = writer.prepared_head();
          for (const std::uint64_t logical : {100U, 101U}) {
              const auto start = writer.progress()->reserved;
              auto outcome = co_await append_contract::settle(
                append.append(
                  *targets[0],
                  co_await append_contract::request(
                    append_contract::child_wire(logical), budget, work),
                  work),
                drive);
              const auto& receipt = append_contract::durable(outcome);
              before.push_back(
                {start, receipt.wal.boundary().cursor(), receipt.block});
          }
      });
    require(
      old_head && before.size() == 2
        && before[1].begin.incarnation() == old_head->incarnation,
      "the requests before the crash were not durable in one WAL file");
    // The crash loses the second group's block and footer.
    auto damaged = co_await append_contract::read_segment(
      files, spec, 1, drive);
    const auto lost = before[1].block.records.bytes().begin().value();
    std::fill(
      damaged.begin() + static_cast<std::ptrdiff_t>(lost), damaged.end(), '\0');
    co_await store_contract::write_bytes(
      files, append_contract::segment_path(spec, 1), damaged, drive);
    const auto old_bytes = co_await append_contract::read_wal(
      files, spec, old_head->incarnation, drive);

    const std::array specs{spec};
    const std::array descriptors{described(1)};
    const std::array catalog{recovery_catalog_entry{
      described(1),
      segment_header::make(
        append_contract::context(1),
        model::range_logical_end{100},
        alignment(8192))
        .value()}};
    std::optional<local_footer_reference> pinned;
    auto head = old_head;
    for (std::uint32_t restart = 0; restart != 3; ++restart) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        require(
          take(
            co_await drive.lifecycle(inspect_local_recovery(
              files,
              owner,
              std::span<const local_device_spec>{specs},
              budget,
              store_contract::limits(),
              work)))
              .verdict
            == recovery_store_verdict::ready,
          "a restarted store was not ready to recover");
        auto control = take(
          co_await drive.lifecycle(
            control_type::open(
              files,
              owner,
              spec,
              0,
              true,
              budget,
              store_contract::limits(),
              work)));
        std::unique_ptr<allocator_type> ids;
        std::unique_ptr<writer_type> writer;
        runtime::first_failure failed;
        try {
            const auto fields = take(control->snapshot()).fields;
            require(
              fields.wal_head == head,
              "a restart found another head than the last one activated");
            std::vector<recovery_decision_record> decided;
            auto discovered = take(
              co_await drive.lifecycle(discover_recovery_decisions(
                files,
                owner,
                spec,
                0,
                fields,
                descriptors,
                budget,
                {store_contract::limits(), 16},
                work,
                [&decided](const recovery_decision_record& value) {
                    decided.push_back(value);
                    return seastar::make_ready_future<runtime::result<bool>>(
                      true);
                })));
            require(
              discovered.complete && discovered.decisions == 0,
              "a restart without decisions found one");
            auto found = take(
              co_await drive.lifecycle(open_recovery_inventory(
                files,
                owner,
                std::span<const local_device_spec>{specs},
                0,
                catalog,
                decided,
                budget,
                store_contract::limits(),
                work)));
            const auto& targets = found.targets;
            require(
              targets.size() == 1 && targets[0].pin == pinned
                && targets[0].state
                     == (pinned ? local_object_state::recovering : local_object_state::active),
              "the recovered segment's pin moved across a restart");
            auto planner = take(recovery_planner::make(targets, budget));
            std::vector<recovery_obligation> rows;
            auto merged = take(
              co_await drive.lifecycle(reconcile_local_recovery(
                files,
                owner,
                std::span<const local_device_spec>{specs},
                spec,
                0,
                fields,
                std::nullopt,
                targets,
                budget,
                merge_limits(),
                work,
                restart_view{&planner, &rows})));
            take(planner.finish(merged));
            const auto& planned = planner.segments()[0];
            require(
              planner.ready()
                && planner.wal().action
                     == recovery_plan_action::activate_successor
                && planned.candidates == 1 && planned.boundary
                && (pinned ? planned.action == recovery_plan_action::retain
                               && planned.boundary == pinned
                           : planned.action
                               == recovery_plan_action::publish_recovering),
              "a restart lowered, moved or republished the recovered pin");
            require(
              rows.size() == 1 && rows[0].wal == old_head->incarnation
                && rows[0].pinned == 1
                && rows[0].first_pinned == before[1].begin,
              "a restart dropped the candidate's obligation");
            auto published = take(
              co_await drive.lifecycle(
                publish_recovered_segments<Clock>(
                  files,
                  owner,
                  std::span<const local_device_spec>{specs},
                  0,
                  planner,
                  found,
                  std::span<const recovery_visibility>{},
                  budget,
                  segment_configuration(45 + restart),
                  {},
                  work)));
            require(
              published.complete && published.segments.size() == 1
                && published.segments[0].published == !pinned,
              "the recovering publication was not made exactly once");
            pinned = planned.boundary;
            ids = take(allocator_type::make(*control, budget, 4));
            writer = take(
              writer_type::make(
                *control,
                *ids,
                budget,
                wal_writer_contract::configuration(),
                wal_start_intent::recovered_head));
            take(
              co_await drive.lifecycle(
                activate_recovered_wal(*writer, planner, work)));
            head = writer->prepared_head();
            require(
              head && head != fields.wal_head
                && take(control->snapshot()).fields.wal_head == head,
              "a restart did not close its head with a successor");
        } catch (...) {
            failed.observe(std::current_exception());
        }
        const auto close = [&](auto operation) -> seastar::future<> {
            try {
                failed.observe(co_await drive.lifecycle(std::move(operation)));
            } catch (...) {
                failed.observe(std::current_exception());
            }
        };
        if (writer) co_await close(writer->close());
        writer.reset();
        if (ids) co_await close(ids->close());
        ids.reset();
        co_await close(control->close());
        control.reset();
        take(failed.outcome());
        require(
          (co_await append_contract::read_wal(
            files, spec, old_head->incarnation, drive))
              == old_bytes
            && (co_await append_contract::read_segment(files, spec, 1, drive))
                 == damaged,
          "a restart wrote the recovered head or segment");
        if constexpr (crashable) take(co_await drive.lifecycle(files.crash()));
    }
}

// One crash history of a checkpoint run. A first checkpoint completes, so
// the run under test replaces a head, removes WAL files and retires a bundle.
// That run is then cut after `cut` scheduler events: the device crashes with
// the run wherever it stood, between or inside the bundle's temporary, its
// rename, the head's rename and sync, each WAL unlink and the old bundle's.
// step(pending) advances one event and says whether it did. Then the store is
// reopened twice, a crash before each. Every reopen finds the old head or the
// new one, never anything else, and a run that returned before the crash is
// never lost. Its bundle reads whole; every WAL file from the head down to the
// cutoff's file is there once the names below the cutoff are removed again;
// the bundles no head names go; and the temporaries an interrupted run left
// do not grow from one restart to the next. Returns whether the run finished
// before the cut, which ends a sweep over cuts.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer,
  typename Step>
seastar::future<bool> checkpoint_crash(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer,
  std::uint32_t cut,
  Step step) {
    using control_type = local_control_owner<Backend, Owner>;
    const std::array specs{spec};
    const auto shard = spec.shard_owner(0).value();
    // Every PREPARE the driver caused, from the receipts it was handed.
    checkpoint_coverage_oracle seen;
    std::optional<local_wal_cursor> before, after;
    bool finished = false;
    co_await append_contract::with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.wal_capacity = byte_count{24_KiB}, .fenced = 1, .tolerate_close = true},
      [&](
        auto& append,
        auto& writer,
        auto&,
        auto& targets,
        auto& work,
        auto& control,
        auto& ids) -> seastar::future<> {
          const auto origin = writer.progress()->reserved;
          // Four requests fill two WAL files of this size.
          const auto fill = [&](std::uint64_t first) -> seastar::future<> {
              for (std::uint64_t logical = first; logical != first + 4;
                   ++logical) {
                  const auto outcome = co_await append_contract::settle(
                    append.append(
                      *targets[0],
                      co_await append_contract::request(
                        append_contract::child_wire(logical), budget, work),
                      work),
                    drive);
                  const auto& receipt = append_contract::durable(outcome);
                  seen.appended(
                    receipt.wal.boundary().cursor(),
                    append_contract::context(1),
                    receipt.block.records.bytes().end());
              }
          };
          co_await fill(100);
          auto checkpoint = take(
            local_checkpoint<Backend, Owner, Clock>::make(
              files,
              owner,
              std::span<const local_device_spec>{specs},
              spec,
              0,
              control,
              ids,
              append,
              budget,
              {.pins = {}, .metadata = store_contract::limits()},
              codec::limits::defaults(),
              origin));
          runtime::first_failure failed;
          try {
              const auto first = take(
                co_await drive.lifecycle(checkpoint->request()));
              require(
                first.published && first.wal_removed != 0
                  && first.wal_owed == 0,
                "the first checkpoint did not reclaim its WAL prefix");
              before = first.end;
              co_await fill(104);
              after = writer.progress()->durable;
              require(
                !(after->incarnation() == before->incarnation()),
                "the run under test has no WAL file to remove");
              auto pending = checkpoint->request();
              std::uint32_t steps = 0;
              while (steps != cut && co_await step(pending))
                  ++steps;
              finished = pending.available();
              require(
                finished || steps == cut,
                "a checkpoint run stopped without a result");
              take(co_await drive.lifecycle(files.crash()));
              try {
                  // Joined before anything closes; what it returns after
                  // the crash is not a result anyone received.
                  static_cast<void>(
                    co_await drive.lifecycle(std::move(pending)));
              } catch (...) {
                  // A run the crash broke may end in an exception.
                  static_cast<void>(std::current_exception());
              }
          } catch (...) {
              failed.observe(std::current_exception());
          }
          static_cast<void>(co_await drive.lifecycle(checkpoint->close()));
          checkpoint.reset();
          take(failed.outcome());
      });
    require(before && after, "the history did not reach its checkpoint run");

    std::optional<local_wal_cursor> reopened;
    std::uint64_t leftover = 0;
    for (unsigned restart = 0; restart != 2; ++restart) {
        // The owners above kept running after the first crash; only what
        // survives another one is what a restart sees.
        take(co_await drive.lifecycle(files.crash()));
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto control = take(
          co_await drive.lifecycle(
            control_type::open(
              files,
              owner,
              spec,
              0,
              true,
              budget,
              store_contract::limits(),
              work)));
        runtime::first_failure failed;
        try {
            const auto fields = take(control->snapshot()).fields;
            const auto cutoff = control->checkpoint_end();
            require(
              cutoff && (*cutoff == *before || *cutoff == *after),
              "a crash left a head that is neither the old nor the new one");
            require(
              !finished || *cutoff == *after,
              "a checkpoint that returned before the crash was lost");
            require(
              !reopened || *reopened == *cutoff,
              "a second crash changed the checkpoint head");
            reopened = *cutoff;
            auto loaded = take(
              co_await drive.lifecycle(load_checkpoint(
                files,
                owner,
                spec,
                0,
                fields,
                budget,
                store_contract::limits(),
                work)));
            require(
              loaded && loaded->end == *cutoff
                && fields.checkpoint == loaded->reference,
              "the head's bundle did not read whole after a crash");
            // Whichever head survived, its pins or the segment's own
            // publication cover every PREPARE below its cutoff.
            const auto descriptor = described(1);
            auto selected = take(
              co_await drive.lifecycle(
                storage::detail::select_recovery_publication(
                  files,
                  spec,
                  0,
                  descriptor,
                  budget,
                  store_contract::limits(),
                  work)));
            const std::array publications{std::move(selected.second)};
            require(
              seen.prepares() == 8
                && !seen.uncovered(
                  shard, *cutoff, loaded->entries, publications),
              "a crash left a PREPARE below the cutoff that nothing covers");
            // Names below the cutoff go again, unread; then every file the
            // cutoff still needs must be there.
            const auto below = take(
              co_await drive.lifecycle(
                remove_wal_below(files, owner, spec, 0, *control, work)));
            std::uint32_t chain = 0;
            const auto walked = take(
              co_await drive.lifecycle(walk_local_wal_chain(
                files,
                owner,
                spec,
                0,
                fields,
                cutoff,
                false,
                budget,
                store_contract::limits(),
                work,
                [&chain](const local_wal_chain_entry&) {
                    ++chain;
                    return seastar::make_ready_future<runtime::result<bool>>(
                      true);
                })));
            const auto none = take(
              co_await drive.lifecycle(
                remove_wal_below(files, owner, spec, 0, *control, work)));
            require(
              walked.complete && chain != 0 && below.kept >= chain
                && none.removed == 0 && none.kept == below.kept,
              "the WAL at or above the cutoff is not whole after a crash");
            // At most the replaced bundle, whose removal the crash undid,
            // and one whose head never became durable.
            const auto stale = take(
              co_await drive.lifecycle(remove_stale_checkpoints(
                files, owner, spec, 0, *control, work)));
            const auto clean = take(
              co_await drive.lifecycle(remove_stale_checkpoints(
                files, owner, spec, 0, *control, work)));
            require(
              stale <= 2 && clean == 0,
              "a crash left more bundles than one run can");
            std::uint64_t temporaries = 0;
            const auto names = take(
              co_await drive.lifecycle(walk_local_namespace(
                files,
                owner,
                spec,
                budget,
                store_contract::limits(),
                work,
                [&temporaries](const local_namespace_entry& entry) {
                    if (entry.kind == local_entry_kind::temporary)
                        ++temporaries;
                    return seastar::make_ready_future<runtime::result<bool>>(
                      true);
                })));
            require(
              names.complete && temporaries <= 4
                && (restart == 0 || temporaries <= leftover),
              "an interrupted checkpoint's temporaries grew");
            leftover = temporaries;
        } catch (...) {
            failed.observe(std::current_exception());
        }
        failed.observe(co_await drive.lifecycle(control->close()));
        take(failed.outcome());
    }
    co_return finished;
}

// One crash history of a completed-retry snapshot cut. A first cut completes,
// so the cut under test replaces a snapshot. It is cut after `cut` scheduler
// events and the device crashes: before, inside or after the bundle's
// publication, the pointer's, and the removal of the snapshot it replaces.
// Then the segment is reopened twice, a crash before each. Its publication
// names the old snapshot or the new one, whole and readable through the pins
// it carries, and a cut that returned before the crash is never lost. A
// bundle the publication does not name is debris and is removed; nothing it
// names is. Returns whether the cut finished before the crash.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer,
  typename Step>
seastar::future<bool> retry_snapshot_crash(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer,
  std::uint32_t cut,
  Step step) {
    const std::array specs{spec};
    std::optional<local_root_reference> before;
    bool finished = false;
    co_await append_contract::with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.fenced = 1, .tolerate_close = true},
      [&](
        auto& append,
        auto&,
        auto& segments,
        auto& targets,
        auto& work,
        auto&,
        auto& ids) -> seastar::future<> {
          std::array<std::optional<completed_retry>, 2> facts;
          for (std::size_t i = 0; i != facts.size(); ++i)
              facts[i] = append_contract::completed_fact(
                append_contract::durable(
                  co_await append_contract::settle(
                    append.append(
                      *targets[0],
                      co_await append_contract::request(
                        append_contract::child_wire(100 + i), budget, work),
                      work),
                    drive)));
          auto retries = take(
            retry_snapshot<Backend, Owner, Clock>::make(
              files,
              owner,
              std::span<const local_device_spec>{specs},
              0,
              append,
              *targets[0],
              *segments[0],
              ids,
              budget,
              store_contract::limits(),
              codec::limits::defaults()));
          runtime::first_failure failed;
          try {
              take(append.record_completed_retry(*targets[0], *facts[0]));
              const auto first = take(
                co_await drive.lifecycle(retries->request(true)));
              require(
                first.cut && first.durable == 1 && first.snapshot,
                "the first snapshot was not cut");
              before = first.snapshot;
              take(append.record_completed_retry(*targets[0], *facts[1]));
              auto pending = retries->request(false);
              std::uint32_t steps = 0;
              while (steps != cut && co_await step(pending))
                  ++steps;
              finished = pending.available();
              require(
                finished || steps == cut, "a cut stopped without a result");
              take(co_await drive.lifecycle(files.crash()));
              try {
                  // Joined before anything closes; what it returns after
                  // the crash is not a result anyone received.
                  static_cast<void>(
                    co_await drive.lifecycle(std::move(pending)));
              } catch (...) {
                  // A cut the crash broke may end in an exception.
                  static_cast<void>(std::current_exception());
              }
          } catch (...) {
              failed.observe(std::current_exception());
          }
          static_cast<void>(co_await drive.lifecycle(retries->close()));
          retries.reset();
          take(failed.outcome());
      });
    require(before.has_value(), "the history did not reach its cut");

    std::optional<local_root_reference> reopened;
    for (unsigned restart = 0; restart != 2; ++restart) {
        // The owners above kept running after the first crash; only what
        // survives another one is what a restart sees.
        take(co_await drive.lifecycle(files.crash()));
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto generation = take(
          co_await drive.lifecycle(open_recovery_generation(
            files,
            owner,
            spec,
            0,
            described(1),
            budget,
            store_contract::limits(),
            work)));
        runtime::first_failure failed;
        try {
            const auto& publication = generation.publication;
            require(
              generation.owner
                && publication.state == local_object_state::active
                && publication.boundary && publication.roots.size() == 1
                && publication.roots[0].kind()
                     == local_root_kind::completed_retry_snapshot
                && generation.resume == publication.boundary,
              "a crash left a publication that pins no whole snapshot");
            const auto current = publication.roots[0];
            {
                const auto pin = take(generation.owner->pin());
                auto lookup = take(
                  co_await drive.lifecycle(
                    completed_retry_lookup::open(pin, budget, work)));
                // The old snapshot holds one fact and the new one both.
                require(
                  lookup.snapshot() && !lookup.summary()
                    && (current == *before ? lookup.facts() == 1 : lookup.facts() == 2),
                  "a crash left a snapshot that is neither the old nor the "
                  "new one");
            }
            require(
              !finished || !(current == *before),
              "a cut that returned before the crash was lost");
            require(
              !reopened || *reopened == current,
              "a second crash changed the segment's snapshot");
            reopened = current;
            // At most the replaced snapshot, whose removal the crash undid,
            // and one whose pointer never moved.
            const auto debris = take(
              co_await drive.lifecycle(remove_unreferenced_objects(
                files, owner, spec, 0, publication, work)));
            const auto clean = take(
              co_await drive.lifecycle(remove_unreferenced_objects(
                files, owner, spec, 0, publication, work)));
            require(
              debris <= 2 && clean == 0
                && take(
                  co_await drive.lifecycle(
                    files.exists(take(take(local_paths::make(spec.root))
                                        .object(
                                          0,
                                          {publication.segment.segment(),
                                           publication.segment.generation()},
                                          current.sequence()))))),
              "a crash left more bundles than one cut can, or lost the one "
              "the publication names");
        } catch (...) {
            failed.observe(std::current_exception());
        }
        if (generation.owner) {
            generation.owner->retire();
            failed.observe(co_await drive.lifecycle(generation.owner->close()));
        }
        take(failed.outcome());
    }
    co_return finished;
}

// What a checkpoint must never do, on one store, and what its durable head
// guarantees afterwards.
//
// A run that cannot finish enables nothing: with the data device missing,
// with a pin table too small for the segments that need a pin, and with its
// bundle unable to take its name, the head stays absent and every WAL file
// stays. Only the run that publishes moves the head, and only then does the
// WAL below the cutoff go. The segment has no index, and none is asked for.
//
// After a reopen the loaded head is the guard: a durable bundle that does not
// continue it is refused. Every PREPARE the driver saw below the cutoff is
// covered by the pin, and by nothing else here, so the pin is what replaced
// the WAL. Then the pinned footer is damaged. With the WAL that covered it
// gone, a restart that resumed from the segment's publication alone would
// read a shorter history; resuming from the checkpoint's pin, it reports
// proven corruption and plans nothing for the segment.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> checkpoint_regressions(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    using control_type = local_control_owner<Backend, Owner>;
    using allocator_type = local_id_allocator<Backend, Owner>;
    using checkpoint_type = local_checkpoint<Backend, Owner, Clock>;
    const std::array specs{spec};
    const auto shard = spec.shard_owner(0).value();
    checkpoint_coverage_oracle seen;
    std::optional<local_wal_cursor> origin, cutoff;
    std::optional<local_checkpoint_entry> pinned;
    std::optional<runtime::file_path> first_path;
    co_await append_contract::with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.wal_capacity = byte_count{24_KiB}},
      [&](
        auto& append,
        auto& writer,
        auto&,
        auto& targets,
        auto& work,
        auto& control,
        auto& ids) -> seastar::future<> {
          origin = writer.progress()->reserved;
          first_path = append_contract::wal_path(spec, origin->incarnation());
          // Four requests fill two WAL files of this size.
          const auto request = [&](std::uint64_t logical) -> seastar::future<> {
              const auto outcome = co_await append_contract::settle(
                append.append(
                  *targets[0],
                  co_await append_contract::request(
                    append_contract::child_wire(logical), budget, work),
                  work),
                drive);
              const auto& receipt = append_contract::durable(outcome);
              seen.appended(
                receipt.wal.boundary().cursor(),
                append_contract::context(1),
                receipt.block.records.bytes().end());
          };
          for (std::uint64_t logical = 100; logical != 104; ++logical)
              co_await request(logical);
          require(
            writer.statistics().rotations != 0,
            "the history has no WAL file below its cutoff");
          const auto unchanged = [&]() -> seastar::future<bool> {
              co_return !take(control.snapshot()).fields.checkpoint
                && !control.checkpoint_end()
                && take(co_await drive.lifecycle(files.exists(*first_path)));
          };
          const auto make = [&](
                              std::span<const local_device_spec> devices,
                              checkpoint_pin_limits pins) {
              return take(
                checkpoint_type::make(
                  files,
                  owner,
                  devices,
                  spec,
                  0,
                  control,
                  ids,
                  append,
                  budget,
                  {.pins = pins, .metadata = store_contract::limits()},
                  codec::limits::defaults(),
                  *origin));
          };
          // One run of a freshly made owner, which is then closed. It may
          // first be handed the final footer of a segment that detached
          // earlier, which it must keep pinned beside the attached one's.
          const auto attempt = [&](
                                 std::span<const local_device_spec> devices,
                                 checkpoint_pin_limits pins,
                                 bool detached)
            -> seastar::future<typename checkpoint_type::output> {
              auto checkpoint = make(devices, pins);
              runtime::result<void> handed{};
              if (detached) {
                  std::optional<local_durable_boundary> other;
                  append.durable_boundaries(
                    [&other](const local_durable_boundary& attached) {
                        other.emplace(attached);
                    });
                  require(
                    other.has_value(), "the attached segment has no footer");
                  other->history.segment = append_contract::context(9);
                  handed = checkpoint->observe(*other);
              }
              auto outcome = co_await drive.lifecycle(checkpoint->request());
              take(co_await drive.lifecycle(checkpoint->close()));
              take(handed);
              co_return outcome;
          };
          // The device that holds the pinned segment is not there.
          const auto lost = co_await attempt({}, {}, false);
          require(
            !lost && lost.error().code() == errc::wrong_context
              && co_await unchanged(),
            "a checkpoint without its data device moved the head or removed "
            "WAL");
          // The table has room for one segment and two need a pin.
          const auto crowded = co_await attempt(
            specs, {.segments = 1, .decisions = 1}, true);
          require(
            !crowded && crowded.error().code() == errc::resource_exhausted
              && co_await unchanged(),
            "a checkpoint whose table does not fit moved the head or removed "
            "WAL");
          auto checkpoint = make(specs, {});
          runtime::first_failure failed;
          try {
              // Its bundle cannot take its name: the next sequence is in use.
              const auto before = take(
                co_await drive.lifecycle(ids.allocate_object(work)));
              const auto blocking = take(
                take(local_paths::make(spec.root))
                  .sequence_file(
                    0, local_sequence_file::checkpoint, before.value() + 1));
              co_await store_contract::write_bytes(
                files, blocking, std::string(4096, 'x'), drive);
              const auto collided = co_await drive.lifecycle(
                checkpoint->request());
              require(
                !collided && co_await unchanged()
                  && checkpoint->pins().entries().empty()
                  && !checkpoint->failure().failed(),
                "a checkpoint whose bundle was not published moved the head "
                "or removed WAL");
              take(co_await drive.lifecycle(files.remove_file(blocking)));
              // The same owner then publishes, and only now does WAL go. A
              // close asked for meanwhile waits for that run and loses
              // nothing of it; afterwards the owner takes no request.
              auto publishing = checkpoint->request();
              auto closing = checkpoint->close();
              require(
                checkpoint->running() && !closing.available(),
                "close did not wait for the checkpoint in flight");
              const auto done = take(
                co_await drive.lifecycle(std::move(publishing)));
              take(co_await drive.lifecycle(std::move(closing)));
              const auto late = co_await drive.lifecycle(checkpoint->request());
              require(
                !late && late.error().code() == errc::closed,
                "a closed checkpoint owner started a run");
              require(
                done.published && done.entries == 1 && done.wal_removed != 0
                  && done.wal_owed == 0 && !done.wal_failed
                  && done.end == writer.progress()->durable
                  && control.checkpoint_end() == done.end
                  && !take(co_await drive.lifecycle(files.exists(*first_path))),
                "the run that published did not move the head and reclaim");
              cutoff = done.end;
              for (const auto& entry : checkpoint->pins().entries())
                  if (entry.segment == append_contract::context(1))
                      pinned = entry;
          } catch (...) {
              failed.observe(std::current_exception());
          }
          failed.observe(co_await drive.lifecycle(checkpoint->close()));
          take(failed.outcome());
          // One more request, above the cutoff: the only PREPARE a restart
          // has to read.
          co_await request(104);
      });
    require(
      origin && cutoff && pinned
        && pinned->disposition
             == local_checkpoint_disposition::segment_boundary,
      "the history did not reach its durable checkpoint");

    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto control = take(
      co_await drive.lifecycle(
        control_type::open(
          files,
          owner,
          spec,
          0,
          true,
          budget,
          store_contract::limits(),
          work)));
    std::unique_ptr<allocator_type> ids;
    runtime::first_failure failed;
    try {
        const auto fields = take(control->snapshot()).fields;
        auto loaded = take(
          co_await drive.lifecycle(load_checkpoint(
            files,
            owner,
            spec,
            0,
            fields,
            budget,
            store_contract::limits(),
            work)));
        require(
          loaded && loaded->end == *cutoff && loaded->begin == *origin
            && loaded->entries.size() == 1
            && control->checkpoint_end() == *cutoff,
          "the reopened head is not the checkpoint that was published");
        // The oracle: nothing but the pins covers what the WAL held.
        const auto descriptor = described(1);
        auto selected = take(
          co_await drive.lifecycle(
            storage::detail::select_recovery_publication(
              files,
              spec,
              0,
              descriptor,
              budget,
              store_contract::limits(),
              work)));
        const std::array publications{std::move(selected.second)};
        const seastar::chunked_vector<local_checkpoint_entry> nothing;
        const bool indexed = std::any_of(
          publications.begin(), publications.end(), [](const auto& published) {
              return !published.roots.empty();
          });
        require(
          seen.prepares() == 5
            && !seen.uncovered(
              shard, loaded->end, loaded->entries, publications)
            && seen.uncovered(shard, loaded->end, nothing, publications) == 0U
            && !indexed,
          "a PREPARE below the cutoff is covered by no pin, or by something "
          "that is not one");

        // The loaded end is the guard a reopened control starts from: a
        // durable bundle that begins anywhere else is never installed.
        ids = take(allocator_type::make(*control, budget));
        {
            const auto sequence = take(
              co_await drive.lifecycle(ids->allocate_object(work)));
            storage::detail::checkpoint_pages pages{
              &nothing,
              shard,
              spec.identity.metadata_alignment,
              sequence,
              1,
              store_contract::limits()};
            auto stale = take(
              co_await drive.lifecycle(
                storage::detail::encode_checkpoint_root(
                  pages, *origin, *cutoff, work)));
            const auto reference = stale.reference;
            auto bundle = take(
              co_await drive.lifecycle(
                local_bundle::make(
                  reference,
                  std::move(stale.expected),
                  std::move(stale.bytes),
                  budget,
                  store_contract::limits(),
                  work)));
            const auto published = co_await drive.lifecycle(
              publish_local_bundle(
                files,
                owner,
                spec,
                0,
                std::move(bundle),
                pages,
                [](codec::cooperative_work&) {
                    return seastar::make_ready_future<runtime::result<void>>(
                      runtime::result<void>{});
                },
                budget,
                work));
            require(
              published.reference == reference,
              "a well-formed bundle was not published");
            const auto installed = co_await drive.lifecycle(control->update(
              [reference](
                local_shard_control& candidate) -> runtime::result<void> {
                  candidate.checkpoint = reference;
                  return {};
              },
              [](const auto&, const auto&, auto&) {
                  return seastar::make_ready_future<runtime::result<void>>(
                    runtime::result<void>{});
              },
              work));
            require(
              installed.failure.error()
                && installed.failure.error()->code() == errc::wrong_context
                && take(control->snapshot()).fields.checkpoint
                     == fields.checkpoint
                && control->checkpoint_end() == *cutoff && !control->fenced(),
              "a reopened control installed a checkpoint that moves back");
        }

        // The pinned footer is damaged after the WAL that covered it went.
        auto data = co_await append_contract::read_segment(
          files, spec, 1, drive);
        const auto damaged = pinned->locator + 40;
        require(
          pinned->bytes.value() > 40 && damaged < data.size(),
          "the pinned footer is not in the segment's file");
        data[damaged] ^= 1;
        co_await store_contract::write_bytes(
          files, append_contract::segment_path(spec, 1), data, drive);
        const std::array catalog{recovery_catalog_entry{
          descriptor,
          segment_header::make(
            append_contract::context(1),
            model::range_logical_end{100},
            alignment(8192))
            .value()}};
        // One restart's read-only plan, resuming from the checkpoint's pin
        // or from the publication alone, and the PREPAREs it read.
        struct planned final {
            recovery_segment_plan segment;
            std::uint64_t prepares;
        };
        const auto plan = [&](bool resume) -> seastar::future<planned> {
            const auto current = take(control->snapshot()).fields;
            auto found = take(
              co_await drive.lifecycle(open_recovery_inventory(
                files,
                owner,
                std::span<const local_device_spec>{specs},
                0,
                catalog,
                std::span<const recovery_decision_record>{},
                budget,
                store_contract::limits(),
                work)));
            require(
              found.targets.size() == 1 && !found.targets[0].pin,
              "an active publication pinned a boundary of its own");
            if (resume) {
                const auto resumed = take(
                  resume_from_checkpoint(loaded->entries, found, {}));
                require(
                  resumed.raised == 1 && resumed.superseded == 0
                    && found.targets[0].pin
                    && found.targets[0].pin->position().value()
                         == pinned->locator,
                  "the checkpoint's pin was not handed to the restart");
            }
            auto planner = take(recovery_planner::make(found.targets, budget));
            std::vector<recovery_obligation> rows;
            auto merged = take(
              co_await drive.lifecycle(reconcile_local_recovery(
                files,
                owner,
                std::span<const local_device_spec>{specs},
                spec,
                0,
                current,
                loaded->end,
                found.targets,
                budget,
                merge_limits(),
                work,
                restart_view{&planner, &rows})));
            take(planner.finish(merged));
            co_return planned{planner.segments()[0], merged.wal.prepares};
        };
        const auto unpinned = co_await plan(false);
        const auto resumed = co_await plan(true);
        // The scan starts at the cutoff: of five PREPAREs it reads the one
        // above it, though the cutoff's own file still holds two below.
        require(
          unpinned.prepares == 1 && resumed.prepares == 1,
          "a restart read PREPAREs below the durable cutoff");
        require(
          unpinned.segment.reason != recovery_plan_reason::corruption
            && resumed.segment.action == recovery_plan_action::stop
            && resumed.segment.reason == recovery_plan_reason::corruption,
          "a pinned footer damaged after reclamation was not reported as "
          "corruption of its segment");
        require(
          (co_await append_contract::read_segment(files, spec, 1, drive))
            == data,
          "planning changed a damaged segment");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (ids) failed.observe(co_await drive.lifecycle(ids->close()));
    failed.observe(co_await drive.lifecycle(control->close()));
    take(failed.outcome());
}

namespace detail {
// A request the retained limit refused: nothing written, and its failure
// carries the limit and the files held.
inline bool retention_refused(
  const local_append_outcome& outcome,
  std::uint32_t limit,
  std::uint32_t held) {
    if (
      outcome.status != local_append_status::not_written || outcome.receipt
      || !outcome.failure.error())
        return false;
    const auto& error = *outcome.failure.error();
    if (error.code() != errc::resource_exhausted || error.context_size() != 2)
        return false;
    const auto first = *error.context_at(0);
    const auto second = *error.context_at(1);
    return first.key == runtime::operation_context_key::limit
           && first.value == limit
           && second.key == runtime::operation_context_key::actual
           && second.value == held;
}
} // namespace detail

// A shard whose WAL nothing pins stays within its retained limit on its own.
// The checkpoint owner is bound as the reclaimer of the append owner's WAL,
// and from then on nobody asks for a run: each rotation leaves a closed file
// behind, a run starts once no obligation holds it, and the file goes. Six
// WAL files are filled under a limit of two and no request is refused.
//
// The owner that reclaims has a budget of its own. One made on the append
// owner's budget is refused the binding, and with every handle credit of the
// append budget taken, a checkpoint still publishes.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> retained_wal_reclaimed(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  workload_budget& reserve,
  Driver drive,
  Timer& timer) {
    using checkpoint_type = local_checkpoint<Backend, Owner, Clock>;
    const std::array specs{spec};
    std::vector<local_retention_pressure> reports;
    co_await append_contract::with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.wal_capacity = byte_count{24_KiB},
       .retained_files = 2,
       .pressure = &reports,
       .control_budget = &reserve},
      [&](
        auto& append,
        auto& writer,
        auto&,
        auto& targets,
        auto& work,
        auto& control,
        auto& ids) -> seastar::future<> {
          const auto origin = writer.progress()->reserved;
          const auto first_path = append_contract::wal_path(
            spec, origin.incarnation());
          const auto make = [&](workload_budget& funded) {
              return take(
                checkpoint_type::make(
                  files,
                  owner,
                  std::span<const local_device_spec>{specs},
                  spec,
                  0,
                  control,
                  ids,
                  append,
                  funded,
                  {.pins = {}, .metadata = store_contract::limits()},
                  codec::limits::defaults(),
                  origin));
          };
          // Two requests fill a WAL file of this size.
          const auto request = [&](std::uint64_t logical) -> seastar::future<> {
              const auto outcome = co_await append_contract::settle(
                append.append(
                  *targets[0],
                  co_await append_contract::request(
                    append_contract::child_wire(logical), budget, work),
                  work),
                drive);
              static_cast<void>(append_contract::durable(outcome));
          };
          {
              auto shared = make(budget);
              const auto bound = shared->bind_retention();
              take(co_await drive.lifecycle(shared->close()));
              require(
                !bound && bound.error().code() == errc::invalid_argument,
                "a reclaimer funded by the appends it relieves was bound");
          }
          auto checkpoint = make(reserve);
          runtime::first_failure failed;
          try {
              take(checkpoint->bind_retention());
              const auto twice = checkpoint->bind_retention();
              require(
                !twice && twice.error().code() == errc::already_exists
                  && append.retention() == wal_retention{1, 2},
                "the reclaimer was bound twice, or the head is not the one "
                "file held");
              for (std::uint64_t logical = 100; logical != 104; ++logical)
                  co_await request(logical);
              require(
                writer.statistics().rotations == 1,
                "the history did not close a WAL file");
              // With every handle credit of the append budget taken, an
              // explicit run still publishes. It finds the closed file gone:
              // the run nobody asked for removed it.
              {
                  auto taken = take(budget.try_reserve(byte_count{4_KiB}));
                  const auto free = budget.limits().handles
                                    - budget.snapshot().handles;
                  if (free != 0)
                      take(taken.try_acquire_handles(
                        static_cast<std::uint32_t>(free)));
                  const auto ran = take(
                    co_await drive.lifecycle(checkpoint->request()));
                  require(
                    ran.published && ran.wal_removed == 0 && ran.wal_owed == 0
                      && !ran.wal_failed
                      && ran.end.incarnation()
                           == writer.progress()->reserved.incarnation()
                      && !take(
                        co_await drive.lifecycle(files.exists(first_path)))
                      && append.retention() == wal_retention{1, 2},
                    "a closed WAL file waited for a checkpoint to be asked "
                    "for, or the reclaimer needed the append budget");
              }
              for (std::uint64_t logical = 104; logical != 112; ++logical)
                  co_await request(logical);
              const auto last = take(
                co_await drive.lifecycle(checkpoint->request()));
              require(
                reports.empty() && !append.retention_pressure()
                  && writer.statistics().rotations == 5
                  && append.retention() == wal_retention{1, 2}
                  && append.retained_wal().files() == 1 && last.wal_owed == 0
                  && last.end == writer.progress()->durable
                  && !checkpoint->failure().failed(),
                "a shard nothing pins met its retained limit, or kept a "
                "closed WAL file");
          } catch (...) {
              failed.observe(std::current_exception());
          }
          failed.observe(co_await drive.lifecycle(checkpoint->close()));
          take(failed.outcome());
      });
}

// A candidate nobody resolves holds the WAL, and the retained limit turns
// that into admission pressure, never into a lost file.
//
// One PREPARE in the first WAL file is pinned as a restart would pin it. No
// checkpoint can pass it, so at the limit a rotation first has a checkpoint
// run and is then refused. The requests forming are rejected unwritten with
// the limit and the files held, the group already in flight becomes durable,
// and nothing is latched. Every refusal names the pinning segment and where
// its PREPARE begins, not only the first, and the file that holds it stays.
// Once its segment's sealed publication resolves the candidate, the same
// request goes through: the checkpoint the rotation waited for removes the
// file.
//
// A held cutoff bounds the WAL the same way, with no segment to name. And a
// request waiting at the limit does not keep the checkpoint owner from
// closing: both finish, and the request reports that nothing was written.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> retained_wal_pressure(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  workload_budget& reserve,
  Driver drive,
  Timer& timer) {
    using checkpoint_type = local_checkpoint<Backend, Owner, Clock>;
    const std::array specs{spec};
    const auto candidate = append_contract::context(9);
    std::vector<local_retention_pressure> reports;
    co_await append_contract::with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.wal_capacity = byte_count{24_KiB},
       .retained_files = 2,
       .pressure = &reports,
       .control_budget = &reserve},
      [&](
        auto& append,
        auto& writer,
        auto&,
        auto& targets,
        auto& work,
        auto& control,
        auto& ids) -> seastar::future<> {
          const auto origin = writer.progress()->reserved;
          const auto first_path = append_contract::wal_path(
            spec, origin.incarnation());
          const std::array pinned{local_pinned_obligation{
            origin.incarnation(), candidate, 1, origin}};
          take(append.restore_pinned(pinned));
          const auto offer = [&](std::uint64_t logical) {
              return append_contract::request(
                append_contract::child_wire(logical), budget, work);
          };
          const auto settled =
            [&](
              std::uint64_t logical) -> seastar::future<local_append_outcome> {
              co_return co_await append_contract::settle(
                append.append(*targets[0], co_await offer(logical), work),
                drive);
          };
          const auto request = [&](std::uint64_t logical) -> seastar::future<> {
              const auto outcome = co_await settled(logical);
              static_cast<void>(append_contract::durable(outcome));
          };
          const auto held_here = [&]() -> seastar::future<bool> {
              co_return take(
                co_await drive.lifecycle(files.exists(first_path)));
          };
          auto checkpoint = take(
            checkpoint_type::make(
              files,
              owner,
              std::span<const local_device_spec>{specs},
              spec,
              0,
              control,
              ids,
              append,
              reserve,
              {.pins = {}, .metadata = store_contract::limits()},
              codec::limits::defaults(),
              origin));
          runtime::first_failure failed;
          try {
              take(checkpoint->bind_retention());
              // Three requests: the first file is full and the second holds
              // one. The shard is at its limit of two files.
              for (std::uint64_t logical = 100; logical != 103; ++logical)
                  co_await request(logical);
              require(
                append.retention() == wal_retention{2, 2} && reports.empty(),
                "the history is not at its retained limit");
              // The fourth fits the second file and is accepted before
              // the fifth, which needs a third file, is refused. It becomes
              // durable all the same.
              auto fitting = append.append(
                *targets[0], co_await offer(103), work);
              const auto fitted = co_await drive.lifecycle(
                std::move(fitting.accepted));
              require(fitted.accepted(), "a request that fits was rejected");
              const auto first_refusal = co_await settled(104);
              require(
                detail::retention_refused(first_refusal, 2, 2),
                "a rotation past the retained limit was not refused");
              static_cast<void>(append_contract::durable(
                co_await drive.lifecycle(std::move(fitting.result))));
              const auto second_refusal = co_await settled(104);
              require(
                detail::retention_refused(second_refusal, 2, 2),
                "the retained limit refused only once");
              require(
                reports.size() == 2 && append.retention_pressure()
                  && !append.failure().failed() && !append.storage_failure()
                  && !writer.failure().failed()
                  && writer.statistics().rotations == 1
                  && !take(control.snapshot()).fields.checkpoint
                  && !checkpoint->end() && co_await held_here(),
                "retention pressure latched a failure, moved the cutoff past "
                "a candidate or lost its WAL file");
              for (const auto& report : reports)
                  require(
                    report.retained == 2 && report.limit == 2
                      && report.segment == candidate && report.first == origin
                      && !report.reclaim,
                    "a refusal did not name the pinning segment and where "
                    "its PREPARE begins");
              // Its segment's sealed publication resolves the candidate. The
              // same request then waits for one checkpoint, which removes
              // the first file, and is written.
              take(append.discharge_pinned(
                local_object_publication{
                  candidate, local_object_state::sealed, std::nullopt, {}}));
              co_await request(104);
              const auto relieved = take(
                co_await drive.lifecycle(checkpoint->request()));
              require(
                reports.size() == 2 && !append.retention_pressure()
                  && writer.statistics().rotations == 2 && !co_await held_here()
                  && relieved.wal_owed == 0
                  && relieved.end == writer.progress()->durable
                  && append.retention() == wal_retention{1, 2},
                "a resolved candidate still held its WAL file or the "
                "request it had held back");
              {
                  // A held cutoff: the file it lies in stays, and at the
                  // limit there is no segment to name.
                  const auto hold = take(checkpoint->hold_cutoff());
                  for (std::uint64_t logical = 105; logical != 108; ++logical)
                      co_await request(logical);
                  const auto held_back = co_await settled(108);
                  require(
                    detail::retention_refused(held_back, 2, 2)
                      && hold.from() == relieved.end && reports.size() == 3
                      && !reports.back().segment && !reports.back().first
                      && !reports.back().reclaim && reports.back().retained == 2
                      && checkpoint->end() == relieved.end,
                    "a held cutoff did not bound the WAL as pressure");
                  // The reclaimer closes while a request is at the limit.
                  // The close completes and the request ends unwritten
                  // either way. When its rotation had already asked for a
                  // run, the close joins that run: it ran against the held
                  // cutoff, so nothing failed and nothing is named. When it
                  // had not, nobody is left to ask.
                  auto waiting = append.append(
                    *targets[0], co_await offer(108), work);
                  const bool asked = checkpoint->running();
                  auto closing = checkpoint->close();
                  take(co_await drive.lifecycle(std::move(closing)));
                  const auto accepted = co_await drive.lifecycle(
                    std::move(waiting.accepted));
                  const auto last = co_await drive.lifecycle(
                    std::move(waiting.result));
                  require(
                    accepted.accepted() && detail::retention_refused(last, 2, 2)
                      && reports.size() == 4 && !reports.back().segment
                      && !append.failure().failed()
                      && (asked
                            ? !reports.back().reclaim
                            : reports.back().reclaim
                                && reports.back().reclaim->code()
                                     == errc::closed),
                    "a request at the retained limit did not end unwritten "
                    "once the reclaimer closed");
                  // With the reclaimer gone nothing can remove a file, and
                  // every further refusal says so.
                  const auto unreclaimed = co_await settled(108);
                  require(
                    detail::retention_refused(unreclaimed, 2, 2)
                      && reports.size() == 5 && !reports.back().segment
                      && reports.back().reclaim
                      && reports.back().reclaim->code() == errc::closed
                      && !append.failure().failed(),
                    "a refusal with nobody to reclaim the WAL did not say "
                    "so");
              }
          } catch (...) {
              failed.observe(std::current_exception());
          }
          failed.observe(co_await drive.lifecycle(checkpoint->close()));
          take(failed.outcome());
      });
}

// A segment that detaches leaves its newest durable footer with the shard's
// checkpoint owner in the detach itself, so the next checkpoint pins it
// beside the attached segment's with no hand-off by the caller. When the pin
// table has no room for it the detach is refused and the segment stays
// attached: a checkpoint then fails as for any attached segment it cannot
// pin, and never passes that segment's PREPAREs with nothing covering them.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> detached_boundary_pinned(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    using checkpoint_type = local_checkpoint<Backend, Owner, Clock>;
    const std::array specs{spec};
    co_await append_contract::with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.segments = 2},
      [&](
        auto& append,
        auto& writer,
        auto&,
        auto& targets,
        auto& work,
        auto& control,
        auto& ids) -> seastar::future<> {
          const auto origin = writer.progress()->reserved;
          const auto make = [&](checkpoint_pin_limits pins) {
              return take(
                checkpoint_type::make(
                  files,
                  owner,
                  std::span<const local_device_spec>{specs},
                  spec,
                  0,
                  control,
                  ids,
                  append,
                  budget,
                  {.pins = pins, .metadata = store_contract::limits()},
                  codec::limits::defaults(),
                  origin));
          };
          const auto request = [&](
                                 std::uint32_t segment,
                                 std::uint64_t logical) -> seastar::future<> {
              const auto outcome = co_await append_contract::settle(
                append.append(
                  *targets[segment],
                  co_await append_contract::request(
                    append_contract::child_wire(logical, segment + 1),
                    budget,
                    work),
                  work),
                drive);
              static_cast<void>(append_contract::durable(outcome));
          };
          co_await request(0, 100);
          co_await request(1, 100);
          const auto head_of = [&] {
              return take(control.snapshot()).fields.checkpoint;
          };
          {
              // A table with room for one segment. A run takes the first
              // attached segment's footer and cannot take the second's.
              auto crowded = make({.segments = 1, .decisions = 1});
              runtime::first_failure failed;
              try {
                  const auto ran = co_await drive.lifecycle(crowded->request());
                  const auto refused = append.detach(*targets[1]);
                  require(
                    !ran && ran.error().code() == errc::resource_exhausted
                      && !refused
                      && refused.error().code() == errc::resource_exhausted
                      && !head_of(),
                    "a segment whose footer the pin table cannot take was "
                    "detached, or a checkpoint passed it");
                  // It is still attached, and still takes appends.
                  co_await request(1, 101);
              } catch (...) {
                  failed.observe(std::current_exception());
              }
              failed.observe(co_await drive.lifecycle(crowded->close()));
              take(failed.outcome());
          }
          auto checkpoint = make({});
          runtime::first_failure failed;
          try {
              const auto detached = take(append.detach(*targets[0]));
              require(
                detached.boundary.has_value(),
                "a segment with a durable request detached without a footer");
              const auto ran = take(
                co_await drive.lifecycle(checkpoint->request()));
              const auto& table = checkpoint->pins().entries();
              const auto pins = [&table](std::uint64_t generation) {
                  return std::ranges::any_of(
                    table, [generation](const local_checkpoint_entry& entry) {
                        return entry.segment
                                 == append_contract::context(generation)
                               && entry.disposition
                                    == local_checkpoint_disposition::
                                      segment_boundary;
                    });
              };
              require(
                ran.published && ran.entries == 2 && head_of()
                  && ran.end == writer.progress()->durable && table.size() == 2
                  && pins(1) && pins(2),
                "a checkpoint did not pin a detached segment's last footer "
                "beside the attached one's");
              const auto pinned = std::ranges::find_if(
                table, [](const local_checkpoint_entry& entry) {
                    return entry.segment == append_contract::context(1);
                });
              require(
                pinned->locator == detached.boundary->footer.begin().value()
                  && pinned->bytes.value()
                       == detached.boundary->footer.end().value()
                            - detached.boundary->footer.begin().value(),
                "the pin is not the footer the segment detached with");
          } catch (...) {
              failed.observe(std::current_exception());
          }
          failed.observe(co_await drive.lifecycle(checkpoint->close()));
          take(failed.outcome());
      });
}

} // namespace kwaque::storage::testing::recovery_successor_contract
