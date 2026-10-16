#pragma once

#include "src/base/units.h"
#include "src/codec/cooperative.h"
#include "src/codec/limits.h"
#include "src/runtime/time.h"
#include "src/storage/device_placement.h"
#include "src/storage/local_engine.h"
#include "src/storage/local_paths.h"
#include "src/storage/local_store_config.h"
#include "src/storage/recovery_inventory.h"
#include "src/storage/retention.h"
#include "src/storage/segment_format.h"
#include "src/storage/segment_lifecycle.h"
#include "src/storage/tests/local_append_contract.h"
#include "src/storage/tests/local_installation_contract.h"
#include "src/storage/tests/local_store_contract.h"
#include "src/storage/tests/recovery_successor_contract.h"
#include "src/storage/tests/segment_lifecycle_contract.h"
#include "src/storage/tests/segment_writer_contract.h"
#include "src/storage/tests/wal_writer_contract.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>

namespace kwaque::storage::testing::local_engine_contract {
using store_contract::require;
using store_contract::take;
namespace appends = local_append_contract;
namespace lifecycle = segment_lifecycle_contract;
namespace segments = segment_writer_contract;

// An engine's limits under the tests' allocation profile, for two writable
// segments.
inline local_engine_config configuration() {
    local_engine_config config{.wal = wal_writer_contract::configuration()};
    config.commit.outstanding_groups = 2;
    config.commit.maximum_wait = runtime::monotonic_duration{0};
    config.checkpoint = {.pins = {}, .metadata = store_contract::limits()};
    config.table = lifecycle::configuration();
    config.table.maximum_writable = 2;
    config.table.maximum_creations = 1;
    config.cache = {.capacity = byte_count{2_MiB}};
    config.writer = segments::configuration();
    config.writer.admission.working_bytes = byte_count{1_MiB};
    config.metadata = store_contract::limits();
    config.decisions = {store_contract::limits(), 16};
    config.merge = recovery_successor_contract::merge_limits();
    config.id_block = 4;
    config.reserved_append_handles = 4;
    return config;
}
// One catalog entry as a segment's owner of segments lists it.
inline recovery_catalog_entry listed(
  std::uint64_t generation,
  std::optional<deletion_authorization> expiring = std::nullopt) {
    auto described = segments::descriptor();
    described.segment = appends::context(generation);
    const auto header = segment_header::make(
                          described.segment,
                          described.logical_origin,
                          described.alignment,
                          described.profile)
                          .value();
    return {described, header, expiring};
}

// One shard's storage through three lives of its process. A store that was
// never used starts ready and takes a segment and an append; stopped, it
// closes that segment unsealed. Started again with the segment in its
// owner's catalog, it restarts: the segment is told as recovered and takes
// no append, a new one does, and the recovered one is removed as what it is,
// not sealed, which the durable checkpoint has to let go of first. A third
// segment is sealed there after a checkpoint that names it, and the stop
// takes the checkpoint that lets go of it. Started a third time with the
// removed segment listed as expiring and the sealed one not listed at all,
// the first is reported gone and the unsealed one is recovered: a start
// does not fail on a segment that was sealed before a clean stop. No start
// writes a flag that a stop was clean.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> started_and_restarted(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  workload_budget& reserve,
  Driver drive,
  Timer& timer) {
    using engine_type = local_engine<Backend, Owner, Clock>;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation_contract::bootstrap(
      files, owner, spec, budget, work, drive);
    const std::array devices{spec};
    const auto a = appends::context(1);
    const auto b = appends::context(2);
    const auto make = [&] {
        return take(
          engine_type::make(
            {files, owner, devices, spec, 0, budget, budget, reserve, budget},
            configuration()));
    };
    // The sampler's part: the shard may promise this much of its one pool.
    const auto share = [](engine_type& engine) {
        const std::array pools{
          device_pool_share{true, false, byte_count{1_GiB}}};
        engine.placement()->sampled(pools, Clock::now());
    };
    const auto begin = [&](
                         engine_type& engine,
                         lifecycle::told& heard,
                         std::span<const recovery_catalog_entry> catalog) {
        return drive.lifecycle(
          engine.start(timer, catalog, {heard.sink(), {}, {}}));
    };

    {
        lifecycle::told heard;
        auto engine = make();
        runtime::first_failure failed;
        try {
            const auto began = co_await begin(*engine, heard, {});
            take(began.failure.outcome());
            require(
              began.state == engine_state::ready && !began.restarted
                && began.recovered == 0 && began.resumed == 0
                && engine->state() == engine_state::ready && engine->table()
                && engine->append() && engine->checkpoint(),
              "a store that was never used did not start ready");
            share(*engine);
            const auto target = take(
              co_await drive.lifecycle(
                engine->table()->create(lifecycle::creation(spec, 1), work)));
            co_await lifecycle::append_one(
              *engine->append(), target, 100, 1, budget, work, drive);
        } catch (...) {
            failed.observe(std::current_exception());
        }
        failed.observe(co_await drive.lifecycle(engine->stop()));
        require(
          engine->state() == engine_state::stopped && !engine->table(),
          "a stopped shard still serves");
        take(failed.outcome());
    }
    {
        lifecycle::told heard;
        auto engine = make();
        runtime::first_failure failed;
        try {
            const std::array catalog{listed(1)};
            const auto began = co_await begin(*engine, heard, catalog);
            take(began.failure.outcome());
            require(
              began.state == engine_state::ready && began.restarted
                && began.recovered == 1 && began.resumed == 0 && began.gone == 0
                && began.refused == 0 && began.sealing == 0
                && began.unresolved == 0
                && engine->table()->life(a) == segment_life::recovering
                && !engine->table()->target(a),
              "a restart did not take in the segment its catalog lists as "
              "recovered");
            require(
              co_await lifecycle::eventually<Clock>(
                drive,
                timer,
                [&] {
                    return heard.count(a, segment_roll_reason::recovered) == 1;
                }),
              "a recovered segment was not told to its owner");
            // A new segment takes appends on the recovered store.
            share(*engine);
            const auto next = take(
              co_await drive.lifecycle(
                engine->table()->create(lifecycle::creation(spec, 2), work)));
            co_await lifecycle::append_one(
              *engine->append(), next, 100, 2, budget, work, drive);
            // The recovered one goes as what it is. The durable checkpoint
            // pins its last footer: the removal has it let go first.
            const auto gone = co_await drive.lifecycle(
              engine->table()->remove({a, std::nullopt}));
            take(gone.failure.outcome());
            require(
              gone.verdict == authorization_verdict::authorized && gone.gone
                && !gone.settling && gone.removed >= 3
                && !engine->table()->life(a),
              "a recovered segment was not removed as not sealed");
            // A third segment is sealed after a checkpoint that names it.
            // Until a later checkpoint is durable a restart would still
            // look for it, and the table says so. Its seal answers with
            // what it is opened against. The stop below takes that later
            // checkpoint, so the next catalog need not list the segment.
            const auto c = appends::context(3);
            share(*engine);
            const auto third = take(
              co_await drive.lifecycle(
                engine->table()->create(lifecycle::creation(spec, 3), work)));
            co_await lifecycle::append_one(
              *engine->append(), third, 100, 3, budget, work, drive);
            const auto cut = co_await drive.lifecycle(
              engine->checkpoint()->request());
            const auto sealed = co_await drive.lifecycle(
              engine->table()->seal(c));
            take(sealed.failure.outcome());
            require(
              sealed.extent && sealed.expected
                && sealed.expected->extent == *sealed.extent
                && sealed.expected->publication.segment == c
                && (!cut || !cut->published || engine->table()->restart_names(c)),
              "a seal did not answer with what the sealed segment is "
              "opened against, or a sealed segment the durable checkpoint "
              "names is not said to be looked for");
        } catch (...) {
            failed.observe(std::current_exception());
        }
        failed.observe(co_await drive.lifecycle(engine->stop()));
        take(failed.outcome());
    }
    {
        lifecycle::told heard;
        auto engine = make();
        runtime::first_failure failed;
        try {
            const std::array catalog{
              listed(1, deletion_authorization{a, std::nullopt}), listed(2)};
            const auto began = co_await begin(*engine, heard, catalog);
            take(began.failure.outcome());
            require(
              began.state == engine_state::ready && began.restarted
                && began.gone == 1 && began.recovered == 1 && began.resumed == 0
                && began.unresolved == 0 && !engine->table()->life(a)
                && engine->table()->life(b) == segment_life::recovering,
              "a removal that finished was not reported gone, or the "
              "segment beside it was not recovered");
        } catch (...) {
            failed.observe(std::current_exception());
        }
        failed.observe(co_await drive.lifecycle(engine->stop()));
        take(failed.outcome());
    }
}

// A control record older than its store is refused before anything is
// written. The record a store was made with, put back after a start made
// the first WAL file, says the store was never used, beside a WAL file it
// never handed out and with no segment listed. The start is refused and the
// record is as it was put back; the newer record put back again restarts
// the store as it was.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> stale_control_record_refused(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  workload_budget& reserve,
  Driver drive,
  Timer& timer) {
    using engine_type = local_engine<Backend, Owner, Clock>;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation_contract::bootstrap(
      files, owner, spec, budget, work, drive);
    const std::array devices{spec};
    const auto make = [&] {
        return take(
          engine_type::make(
            {files, owner, devices, spec, 0, budget, budget, reserve, budget},
            configuration()));
    };
    const auto control = take(take(local_paths::make(spec.root)).control(0));
    const auto made = co_await store_contract::read_all_bytes(
      files, control, drive);
    // Started on what the record says, and refused on it.
    const auto life = [&]() -> seastar::future<engine_start> {
        lifecycle::told heard;
        auto engine = make();
        runtime::first_failure failed;
        engine_start began;
        try {
            began = co_await drive.lifecycle(
              engine->start(timer, {}, {heard.sink(), {}, {}}));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        failed.observe(co_await drive.lifecycle(engine->stop()));
        take(failed.outcome());
        co_return began;
    };

    const auto first = co_await life();
    take(first.failure.outcome());
    require(
      first.state == engine_state::ready && !first.restarted,
      "a store that was never used did not start");
    const auto used = co_await store_contract::read_all_bytes(
      files, control, drive);
    require(used != made, "a start left the record it was made with");

    co_await store_contract::write_bytes(files, control, made, drive);
    const auto refused = co_await life();
    require(
      refused.failure.failed() && refused.state == engine_state::stopped
        && !refused.restarted,
      "a store with a WAL its record never handed out was taken as unused");
    const auto kept = refused.failure.outcome();
    require(
      !kept && kept.error().code() == errc::wrong_context,
      "a record older than its store was refused for another reason");
    require(
      co_await store_contract::read_all_bytes(files, control, drive) == made,
      "a refused start wrote over the record");

    co_await store_contract::write_bytes(files, control, used, drive);
    const auto again = co_await life();
    take(again.failure.outcome());
    require(
      again.state == engine_state::ready && again.restarted,
      "the store a refused start was given did not restart");
}

// One crash history of a removal. A store takes a segment, seals it and lets
// its owner go; the segment's removal is then cut after `cut` scheduler
// events and the device crashes with the removal wherever it stood: before,
// inside or after the rename, its sync, the checkpoint that lets go of the
// segment, each unlink, the directory's removal and the last sync. step(
// pending) advances one event and says whether it did.
//
// Then the store is started twice more, a crash before each, with the
// segment in its owner's catalog as expiring. Before each start the device
// holds the segment under its own name with every file, or under its
// deleting name, or under neither, and never under both; a removal that had
// returned before the crash is under neither. Each start is ready: it goes
// on with the removal it finds, or reports the segment gone, and leaves no
// name behind; a segment reported gone has no PREPARE left that a restart
// would ask about. Returns whether the removal finished before the cut,
// which ends a sweep over cuts.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer,
  typename Step>
seastar::future<bool> removal_crash(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  workload_budget& reserve,
  Driver drive,
  Timer& timer,
  std::uint32_t cut,
  Step step) {
    using engine_type = local_engine<Backend, Owner, Clock>;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation_contract::bootstrap(
      files, owner, spec, budget, work, drive);
    const std::array devices{spec};
    const auto a = appends::context(1);
    const auto paths = take(local_paths::make(spec.root));
    const local_segment_name name{a.segment(), a.generation()};
    const auto live = take(paths.segment(0, name));
    const auto marked = take(paths.deleting(0, name));
    const auto there = [&](const runtime::file_path& path) {
        return drive.lifecycle(files.exists(path));
    };
    const auto make = [&] {
        return take(
          engine_type::make(
            {files, owner, devices, spec, 0, budget, budget, reserve, budget},
            configuration()));
    };
    const auto begin = [&](
                         engine_type& engine,
                         lifecycle::told& heard,
                         std::span<const recovery_catalog_entry> catalog) {
        return drive.lifecycle(
          engine.start(timer, catalog, {heard.sink(), {}, {}}));
    };

    std::optional<segment_immutable_expectation> extent;
    bool finished = false, returned = false;
    {
        lifecycle::told heard;
        auto engine = make();
        runtime::first_failure failed;
        try {
            const auto began = co_await begin(*engine, heard, {});
            take(began.failure.outcome());
            const std::array pools{
              device_pool_share{true, false, byte_count{1_GiB}}};
            engine->placement()->sampled(pools, Clock::now());
            const auto target = take(
              co_await drive.lifecycle(
                engine->table()->create(lifecycle::creation(spec, 1), work)));
            for (const std::uint64_t logical : {100U, 101U})
                co_await lifecycle::append_one(
                  *engine->append(), target, logical, 1, budget, work, drive);
            const auto sealed = co_await drive.lifecycle(
              engine->table()->seal(a));
            take(sealed.failure.outcome());
            const auto kept = engine->table()->expectation(a);
            require(
              sealed.extent && kept,
              "a sealed segment does not say what it was published as");
            extent = *sealed.extent;
            // Nothing of it is open when its removal is cut.
            take(co_await drive.lifecycle(engine->table()->close_segment(a)));
            auto pending = engine->table()->remove({a, *extent}, &*kept);
            std::uint32_t steps = 0;
            while (steps != cut && co_await step(pending))
                ++steps;
            finished = pending.available();
            require(
              finished || steps == cut, "a removal stopped without a result");
            take(co_await drive.lifecycle(files.crash()));
            try {
                // Joined before anything closes. Only one that had returned
                // before the crash told anyone anything.
                const auto ended = co_await drive.lifecycle(std::move(pending));
                returned = finished && ended.gone;
            } catch (...) {
                // A removal the crash broke may end in an exception.
                static_cast<void>(std::current_exception());
            }
            require(
              !finished || returned,
              "a removal that ran to its end did not report the segment gone");
        } catch (...) {
            failed.observe(std::current_exception());
        }
        // What closes after a crash may fail.
        static_cast<void>(co_await drive.lifecycle(engine->stop()));
        take(failed.outcome());
    }
    require(extent.has_value(), "the history did not reach its removal");

    const std::array catalog{listed(1, deletion_authorization{a, *extent})};
    bool reported = false;
    for (unsigned restart = 0; restart != 2; ++restart) {
        // The life before kept running after the first crash; only what
        // survives another one is what a start sees.
        take(co_await drive.lifecycle(files.crash()));
        const bool own = take(co_await there(live));
        const bool mark = take(co_await there(marked));
        require(!(own && mark), "a crash left the segment under both names");
        if (own) {
            bool whole = true;
            for (const auto file :
                 {local_segment_file::descriptor,
                  local_segment_file::data,
                  local_segment_file::published})
                whole = whole
                        && take(
                          co_await there(
                            take(paths.segment_file(0, name, file))));
            require(
              whole,
              "a crash left a segment under its own name with a file gone");
        }
        require(
          !returned || (!own && !mark),
          "a removal that returned before the crash was lost");
        require(
          !reported || (!own && !mark),
          "a crash brought back a segment a start had removed");

        lifecycle::told heard;
        auto engine = make();
        runtime::first_failure failed;
        try {
            const auto began = co_await begin(*engine, heard, catalog);
            take(began.failure.outcome());
            require(
              began.state == engine_state::ready && began.restarted
                && began.refused == 0 && began.recovered == 0
                && began.sealing == 0 && began.resumed + began.gone == 1
                && (began.gone == 1) == (!own && !mark) && began.owed == 0,
              "a start did not go on with the removal it found, or did not "
              "report the one that had finished");
            require(
              began.gone == 0 || began.unresolved == 0,
              "a segment was reported gone while the WAL still asks for it");
            require(
              !take(co_await there(live)) && !take(co_await there(marked)),
              "a start left a name of a segment it had removed");
            reported = true;
        } catch (...) {
            failed.observe(std::current_exception());
        }
        failed.observe(co_await drive.lifecycle(engine->stop()));
        take(failed.outcome());
    }
    co_return finished;
}

} // namespace kwaque::storage::testing::local_engine_contract
