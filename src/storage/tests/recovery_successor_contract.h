#pragma once

#include "src/base/units.h"
#include "src/storage/recovery_inventory.h"
#include "src/storage/recovery_publication.h"
#include "src/storage/recovery_successor.h"
#include "src/storage/tests/local_append_contract.h"

#include <seastar/core/coroutine.hh>

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
            auto beyond = co_await drive.lifecycle(attempt->activate_recovered(
              take(
                local_wal_cursor::make(
                  old_head->incarnation,
                  runtime::file_position{old_bytes.size() + 8192})),
              work));
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
            failed.observe(co_await drive.lifecycle(small->close()));
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

} // namespace kwaque::storage::testing::recovery_successor_contract
