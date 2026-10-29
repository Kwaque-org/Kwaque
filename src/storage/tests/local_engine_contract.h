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
#include "src/storage/tests/segment_test_support.h"
#include "src/storage/tests/segment_writer_contract.h"
#include "src/storage/tests/wal_writer_contract.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <array>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

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
    // The bounds these contracts were written for, every portion placed in
    // its segment among them.
    config.append.maximum_segments = 64;
    config.append.maximum_requests = 64;
    config.append.segment_placement_bytes = byte_count{0};
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
                          described.alignment,
                          described.profile)
                          .value();
    return {described, header, appends::segment_origin(), expiring};
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
            co_await lifecycle::append_one<Clock>(
              *engine->append(), target, 100, 1, budget, work, drive, timer);
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
            co_await lifecycle::append_one<Clock>(
              *engine->append(), next, 100, 2, budget, work, drive, timer);
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
            co_await lifecycle::append_one<Clock>(
              *engine->append(), third, 100, 3, budget, work, drive, timer);
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

// A spare is no WAL file a control record hands out: the check that refuses
// a record older than its store passes it over. The record a store was made
// with, put back once the first WAL file the start made lies among the
// WAL's spares under its incarnation, which the record never handed out,
// starts the store as never used, and the spare is left as it was. Restart's
// inspection takes the store with its spares.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> stale_control_check_passes_spares(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  workload_budget& reserve,
  Driver drive,
  Timer& timer) {
    using engine_type = local_engine<Backend, Owner, Clock>;
    using control_type = local_control_owner<Backend, Owner>;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation_contract::bootstrap(
      files, owner, spec, budget, work, drive);
    const std::array devices{spec};
    const auto paths = take(local_paths::make(spec.root));
    const auto control = take(paths.control(0));
    const auto made = co_await store_contract::read_all_bytes(
      files, control, drive);
    const auto life = [&]() -> seastar::future<engine_start> {
        lifecycle::told heard;
        auto engine = take(
          engine_type::make(
            {files, owner, devices, spec, 0, budget, budget, reserve, budget},
            configuration()));
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

    take((co_await life()).failure.outcome());
    std::optional<model::wal_incarnation_id> head;
    {
        auto opened = take(
          co_await drive.lifecycle(
            control_type::open(
              files,
              owner,
              spec,
              0,
              false,
              budget,
              store_contract::limits(),
              work)));
        const auto fields = take(opened->snapshot()).fields;
        if (fields.wal_head) head = fields.wal_head->incarnation;
        take(co_await drive.lifecycle(opened->close()));
    }
    require(head.has_value(), "a store's first start made no WAL file");
    const auto spare = take(paths.spare(0, *head));
    const auto kept = co_await store_contract::read_all_bytes(
      files, take(paths.wal(0, *head)), drive);
    take(
      co_await drive.lifecycle(files.create_directories(
        take(paths.spares(0, local_spare_kind::wal)))));
    take(
      co_await drive.lifecycle(files.rename(
        take(paths.wal(0, *head)),
        spare,
        runtime::file_rename_policy::no_replace)));
    co_await store_contract::write_bytes(files, control, made, drive);

    const auto began = co_await life();
    take(began.failure.outcome());
    require(
      began.state == engine_state::ready && !began.restarted,
      "a store whose only WAL file is a spare was refused or restarted");
    require(
      co_await store_contract::read_all_bytes(files, spare, drive) == kept,
      "a start changed a spare");
    // Restart's inspection takes a store that keeps spares as it is.
    const auto inspected = take(
      co_await drive.lifecycle(inspect_local_recovery(
        files, owner, devices, budget, store_contract::limits(), work)));
    require(
      inspected.inspected == 1
        && inspected.verdict == recovery_store_verdict::ready,
      "a store that keeps spares did not pass restart's inspection");
}

// A segment prepared and never activated is listed with no origin. A
// restart takes it in as prepared, and it is activated at the origin its
// owner then gives and takes appends. Listed so again once it was written,
// as though its activation record was lost, it is a conflict: the shard
// stays recovering and nothing is changed.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> prepared_restarted(
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
    const auto share = [](engine_type& engine) {
        const std::array pools{
          device_pool_share{true, false, byte_count{1_GiB}}};
        engine.placement()->sampled(pools, Clock::now());
    };
    auto prepared = listed(1);
    prepared.logical_origin.reset();
    const std::array catalog{prepared};
    // One life of the shard: started on `catalogued`, then `body`, then
    // stopped.
    const auto life = [&](
                        std::span<const recovery_catalog_entry> catalogued,
                        auto body) -> seastar::future<> {
        lifecycle::told heard;
        auto engine = take(
          engine_type::make(
            {files, owner, devices, spec, 0, budget, budget, reserve, budget},
            configuration()));
        runtime::first_failure failed;
        try {
            const auto began = co_await drive.lifecycle(
              engine->start(timer, catalogued, {heard.sink(), {}, {}}));
            take(began.failure.outcome());
            co_await body(*engine, began);
        } catch (...) {
            failed.observe(std::current_exception());
        }
        failed.observe(co_await drive.lifecycle(engine->stop()));
        take(failed.outcome());
    };

    co_await life(
      {}, [&](engine_type& engine, const engine_start&) -> seastar::future<> {
          share(engine);
          take(
            co_await drive.lifecycle(
              engine.table()->prepare(lifecycle::creation(spec, 1), work)));
          require(
            engine.table()->life(a) == segment_life::prepared,
            "a prepared segment was not held as prepared");
      });
    co_await life(
      catalog,
      [&](engine_type& engine, const engine_start& began) -> seastar::future<> {
          require(
            began.state == engine_state::ready && began.prepared == 1
              && began.conflicts == 0
              && engine.table()->life(a) == segment_life::prepared,
            "a restart did not take in a segment prepared and never "
            "activated");
          share(engine);
          const auto target = take(
            co_await drive.lifecycle(engine.table()->activate(
              a, {.logical_origin = appends::segment_origin()}, work)));
          co_await lifecycle::append_one<Clock>(
            *engine.append(), target, 100, 1, budget, work, drive, timer);
      });
    co_await life(
      catalog,
      [&](engine_type&, const engine_start& began) -> seastar::future<> {
          require(
            began.state == engine_state::recovering && began.prepared == 1
              && began.conflicts == 1,
            "a segment listed as never activated that was written was taken "
            "in as prepared");
          co_return;
      });
}

// A shard whose appends' budget holds fewer writable segments than it is
// configured for takes as many as the budget holds, and reports that many:
// each of them can be made, and one more is refused as over the bound.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> derived_segments(
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
    auto config = configuration();
    config.table.maximum_writable = 64;
    // The handle credits left after the WAL's, every seal slot's and each
    // creation's, at an idle segment's for each.
    const auto credits
      = (budget.limits().handles - config.reserved_append_handles
         - config.table.maximum_seals * segment_seal_handles
         - config.table.maximum_creations * segment_creation_handles)
        / segment_writer_handles(config.writer);
    lifecycle::told heard;
    auto engine = take(
      engine_type::make(
        {files, owner, devices, spec, 0, budget, budget, reserve, budget},
        config));
    runtime::first_failure failed;
    try {
        const auto began = co_await drive.lifecycle(
          engine->start(timer, {}, {heard.sink(), {}, {}}));
        take(began.failure.outcome());
        require(
          began.state == engine_state::ready && began.segments != 0
            && began.segments <= credits && began.segments < 64,
          "a shard did not take the writable segments its budget holds");
        const std::array pools{
          device_pool_share{true, false, byte_count{1_GiB}}};
        engine->placement()->sampled(pools, Clock::now());
        for (std::uint32_t generation = 1; generation <= began.segments;
             ++generation)
            static_cast<void>(take(
              co_await drive.lifecycle(engine->table()->create(
                lifecycle::creation(spec, generation), work))));
        const auto over = co_await drive.lifecycle(engine->table()->create(
          lifecycle::creation(spec, began.segments + 1), work));
        require(
          !over && over.error().code() == errc::queue_full,
          "a shard made more writable segments than it reported");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    failed.observe(co_await drive.lifecycle(engine->stop()));
    take(failed.outcome());
}

// A shard at its default width. It takes the 1,024 writable segments it is
// configured for, makes them and 1,024 more prepared, appends to each and
// stops cleanly, its checkpoint pinning every one. Started again on a
// catalog that lists them all, it takes each in as recovered or prepared.
// A catalog one longer than a restart reads is refused before anything is
// read.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> wide_shard(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  workload_budget& reserve,
  Driver drive,
  Timer& timer) {
    using engine_type = local_engine<Backend, Owner, Clock>;
    constexpr std::uint32_t width = 1024;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation_contract::bootstrap(
      files, owner, spec, budget, work, drive);
    const std::array devices{spec};
    auto config = configuration();
    config.table.maximum_writable = width;
    config.table.maximum_prepared = width;
    config.append.maximum_segments = width;
    // Room on the device for every segment's whole bound. A share is
    // trusted for a few seconds, and making 2,048 segments takes longer:
    // it is handed before each creation, as the process's sampler keeps it
    // fresh.
    const auto share = [](engine_type& engine) {
        const std::array pools{
          device_pool_share{true, false, byte_count{std::uint64_t{1} << 50U}}};
        engine.placement()->sampled(pools, Clock::now());
    };
    // One life of the shard: started on `catalogued`, then `body`, which
    // judges the start, then stopped.
    const auto life = [&](
                        std::span<const recovery_catalog_entry> catalogued,
                        auto body) -> seastar::future<> {
        lifecycle::told heard;
        auto engine = take(
          engine_type::make(
            {files, owner, devices, spec, 0, budget, budget, reserve, budget},
            config));
        runtime::first_failure failed;
        try {
            const auto began = co_await drive.lifecycle(
              engine->start(timer, catalogued, {heard.sink(), {}, {}}));
            co_await body(*engine, began);
        } catch (...) {
            failed.observe(std::current_exception());
        }
        failed.observe(co_await drive.lifecycle(engine->stop()));
        take(failed.outcome());
    };

    co_await life(
      {},
      [&](engine_type& engine, const engine_start& began) -> seastar::future<> {
          take(began.failure.outcome());
          require(
            began.state == engine_state::ready && began.segments == width,
            "a shard did not take the writable segments it is configured "
            "for");
          for (std::uint32_t generation = 1; generation <= width;
               ++generation) {
              share(engine);
              const auto target = take(
                co_await drive.lifecycle(engine.table()->create(
                  lifecycle::creation(spec, generation), work)));
              co_await lifecycle::append_one<Clock>(
                *engine.append(),
                target,
                100,
                generation,
                budget,
                work,
                drive,
                timer);
          }
          for (std::uint32_t generation = width + 1; generation <= 2 * width;
               ++generation) {
              share(engine);
              take(
                co_await drive.lifecycle(engine.table()->prepare(
                  lifecycle::creation(spec, generation), work)));
          }
      });

    std::vector<recovery_catalog_entry> catalog;
    catalog.reserve(maximum_recovery_catalog + 1);
    for (std::uint32_t generation = 1; generation <= width; ++generation)
        catalog.push_back(listed(generation));
    for (std::uint32_t generation = width + 1; generation <= 2 * width;
         ++generation) {
        auto prepared = listed(generation);
        prepared.logical_origin.reset();
        catalog.push_back(std::move(prepared));
    }
    co_await life(
      catalog,
      [&](engine_type&, const engine_start& began) -> seastar::future<> {
          take(began.failure.outcome());
          require(
            began.state == engine_state::ready && began.restarted
              && began.recovered == width && began.unadopted == 0
              && began.prepared == width && began.conflicts == 0,
            "a restart did not take in every segment its catalog lists");
          co_return;
      });

    while (catalog.size() <= maximum_recovery_catalog)
        catalog.push_back(listed(catalog.size() + 1));
    co_await life(
      catalog,
      [&](engine_type&, const engine_start& began) -> seastar::future<> {
          const auto refused = began.failure.outcome();
          require(
            began.state != engine_state::ready && !refused
              && refused.error().code() == errc::out_of_range,
            "a catalog longer than one restart reads was not refused");
          co_return;
      });
}

// The append owner's own placement, as a shard runs it. A small request's
// portion is placed in the WAL and held in its segment's run: its WAL
// barrier publishes it while the segment holds nothing of it, and a session
// is given it all the same. A clean stop syncs the run, so the next start
// finds every reported block where its receipt said, and writes nothing.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> placed_by_default(
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
    auto config = configuration();
    const local_append_config product;
    config.append.segment_placement_bytes = product.segment_placement_bytes;
    config.append.held_run_bytes = product.held_run_bytes;
    const auto make = [&] {
        return take(
          engine_type::make(
            {files, owner, devices, spec, 0, budget, budget, reserve, budget},
            config));
    };
    std::vector<std::string> wires;
    std::vector<local_append_receipt> receipts;
    {
        lifecycle::told heard;
        auto engine = make();
        runtime::first_failure failed;
        try {
            const auto began = co_await drive.lifecycle(
              engine->start(timer, {}, {heard.sink(), {}, {}}));
            take(began.failure.outcome());
            const std::array pools{
              device_pool_share{true, false, byte_count{1_GiB}}};
            engine->placement()->sampled(pools, Clock::now());
            auto& table = *engine->table();
            auto& append = *engine->append();
            const auto target = take(
              co_await drive.lifecycle(
                table.create(lifecycle::creation(spec, 1), work)));
            for (std::uint64_t logical = 100; logical != 104; ++logical) {
                wires.push_back(appends::child_wire(logical, 1));
                const auto outcome = co_await appends::settle(
                  append.append(
                    target,
                    co_await appends::request(wires.back(), budget, work),
                    work),
                  drive);
                const auto& receipt = appends::durable(outcome);
                require(
                  receipt.placement == local_append_placement::wal
                    && receipt.wal && !receipt.segment
                    && !receipt.boundary.footer(),
                  "a small request was not placed in the WAL and held");
                receipts.push_back(receipt);
            }
            const auto counted = append.statistics();
            require(
              counted.held_portions == 4 && counted.held_runs == 1
                && counted.held_bytes != 0 && counted.barriers == 0
                && counted.unsynced_bytes != 0,
              "the requests' run was not held unwritten");
            auto session = take(
              co_await drive.lifecycle(table.open_session(
                a, lifecycle::sight(1U << 20U), budget, work)));
            const auto read = co_await lifecycle::read_from(
              session,
              {model::range_logical_end{100}, std::nullopt},
              byte_count{1_MiB},
              drive);
            require(
              read.batches.size() == wires.size(),
              "a session was not given the held requests");
            for (std::size_t i = 0; i != wires.size(); ++i)
                require(
                  read.batches[i].logical.begin().value() == 100 + i
                    && flat(read.batches[i].bytes) == wires[i],
                  "a session was given a held request other than its own");
        } catch (...) {
            failed.observe(std::current_exception());
        }
        failed.observe(co_await drive.lifecycle(engine->stop()));
        take(failed.outcome());
    }
    lifecycle::told heard;
    auto engine = make();
    runtime::first_failure failed;
    try {
        const std::array catalog{listed(1)};
        const auto began = co_await drive.lifecycle(
          engine->start(timer, catalog, {heard.sink(), {}, {}}));
        take(began.failure.outcome());
        require(
          began.state == engine_state::ready && began.recovered == 1
            && began.materialized == 0 && began.candidates == 0,
          "a clean stop left its held run for the start to write");
        const auto data = co_await appends::read_segment(files, spec, 1, drive);
        for (std::size_t i = 0; i != receipts.size(); ++i) {
            const auto& block = receipts[i].block;
            const auto placed = appends::block_at(
              wires[i], 1, block, 8192, receipts[i].stamps);
            require(
              data.size()
                  >= block.records.bytes().begin().value() + placed.size()
                && data.substr(
                     block.records.bytes().begin().value(), placed.size())
                     == placed,
              "a reported request's block is not where its receipt said");
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    failed.observe(co_await drive.lifecycle(engine->stop()));
    take(failed.outcome());
}

// How a WAL head is damaged before the start that follows a crash.
enum class head_damage : std::uint8_t {
    // Four bytes of the oldest request's PREPARE zeroed.
    zeroed,
    // One bit of it flipped.
    flipped,
    // A unit of bytes that begin no PREPARE, after the newest one.
    junk,
    // A PREPARE's first bytes after the newest one, its header cut short:
    // a write the crash tore.
    torn_header,
    // A PREPARE cut inside its body after the newest one.
    torn_body,
};

// Small requests are placed in the WAL and held in their segment's run,
// each reported durable by its WAL barrier, so each later group's PREPARE
// stamps the WAL as certified past the earlier ones. `crash` crashes the
// device and stops the shard as the crash leaves it: the WAL keeps the
// PREPAREs and the run is lost. Damage the later stamps prove certified is
// corruption: the start reads everything, changes nothing and stays
// recovering. Damage after the newest PREPARE is a tail no stamp certifies:
// the start writes every reported request into its segment from the WAL
// and flushes it before it serves, is ready and takes appends, and the
// start after that finds nothing more to write.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer,
  typename Crash>
seastar::future<> damaged_head(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  workload_budget& reserve,
  Driver drive,
  Timer& timer,
  head_damage damage,
  Crash crash) {
    using engine_type = local_engine<Backend, Owner, Clock>;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation_contract::bootstrap(
      files, owner, spec, budget, work, drive);
    const std::array devices{spec};
    auto config = configuration();
    config.append.segment_placement_bytes = byte_count{
      std::numeric_limits<std::uint64_t>::max()};
    config.append.held_run_bytes = byte_count{128_KiB};
    const auto make = [&] {
        return take(
          engine_type::make(
            {files, owner, devices, spec, 0, budget, budget, reserve, budget},
            config));
    };
    const auto share = [](engine_type& engine) {
        const std::array pools{
          device_pool_share{true, false, byte_count{1_GiB}}};
        engine.placement()->sampled(pools, Clock::now());
    };
    std::vector<std::string> wires;
    std::vector<local_append_receipt> receipts;
    std::optional<runtime::file_path> head;
    {
        lifecycle::told heard;
        auto engine = make();
        const auto began = co_await drive.lifecycle(
          engine->start(timer, {}, {heard.sink(), {}, {}}));
        take(began.failure.outcome());
        share(*engine);
        const auto target = take(
          co_await drive.lifecycle(
            engine->table()->create(lifecycle::creation(spec, 1), work)));
        for (std::uint64_t logical = 100; logical != 104; ++logical) {
            wires.push_back(appends::child_wire(logical, 1));
            const auto outcome = co_await appends::settle(
              engine->append()->append(
                target,
                co_await appends::request(wires.back(), budget, work),
                work),
              drive);
            receipts.push_back(appends::durable(outcome));
            require(
              receipts.back().wal && !receipts.back().segment,
              "a request was not reported by its WAL barrier alone");
        }
        head = take(take(local_paths::make(spec.root))
                      .wal(0, take(engine->append()->wal_end()).incarnation()));
        co_await crash(*engine);
    }
    // The oldest request's PREPARE begins where the head's content does,
    // one unit in; past its envelope's prefix lies its body.
    constexpr std::size_t oldest = 8192 + 100;
    auto stored = co_await store_contract::read_all_bytes(files, *head, drive);
    const auto unit = [](std::string bytes) {
        bytes.resize((bytes.size() + 8191) / 8192 * 8192, '\0');
        return bytes;
    };
    const auto prepare = stored.substr(8192, 8192);
    switch (damage) {
    case head_damage::zeroed:
        stored.replace(oldest, 4, 4, '\0');
        break;
    case head_damage::flipped:
        stored[oldest] = static_cast<char>(stored[oldest] ^ 1);
        break;
    case head_damage::junk:
        stored += std::string(8192, 'J');
        break;
    case head_damage::torn_header:
        stored += unit(prepare.substr(0, 16));
        break;
    case head_damage::torn_body:
        stored += unit(prepare.substr(0, 300));
        break;
    }
    co_await store_contract::write_bytes(files, *head, stored, drive);
    const bool certified = damage == head_damage::zeroed
                           || damage == head_damage::flipped;
    const auto data = appends::segment_path(spec, 1);
    const auto before = co_await store_contract::read_all_bytes(
      files, data, drive);
    // One life on what the last left: started with the segments listed,
    // then `body`, then stopped.
    const auto life = [&](
                        std::span<const recovery_catalog_entry> catalog,
                        auto body) -> seastar::future<> {
        lifecycle::told heard;
        auto engine = make();
        runtime::first_failure failed;
        try {
            const auto began = co_await drive.lifecycle(
              engine->start(timer, catalog, {heard.sink(), {}, {}}));
            co_await body(*engine, began);
        } catch (...) {
            failed.observe(std::current_exception());
        }
        failed.observe(co_await drive.lifecycle(engine->stop()));
        take(failed.outcome());
    };
    const std::array first{listed(1)};
    co_await life(
      first,
      [&](engine_type& engine, const engine_start& began) -> seastar::future<> {
          if (certified) {
              require(
                began.state == engine_state::recovering
                  && (co_await store_contract::read_all_bytes(
                       files, *head, drive))
                       == stored
                  && (co_await store_contract::read_all_bytes(
                       files, data, drive))
                       == before,
                "a start went past damage the WAL proves certified, or "
                "changed the store");
              co_return;
          }
          take(began.failure.outcome());
          require(
            began.state == engine_state::ready && began.recovered == 1
              && began.materialized == receipts.size() && began.candidates == 0,
            "a start did not write the reported requests the WAL held");
          const auto written = co_await store_contract::read_all_bytes(
            files, data, drive);
          for (std::size_t i = 0; i != receipts.size(); ++i) {
              const auto& block = receipts[i].block;
              const auto placed = appends::block_at(
                wires[i], 1, block, 8192, receipts[i].stamps);
              const auto at = block.records.bytes().begin().value();
              require(
                written.size() >= at + placed.size()
                  && written.substr(at, placed.size()) == placed,
                "a reported request was not written where its receipt said");
          }
          share(engine);
          const auto target = take(
            co_await drive.lifecycle(
              engine.table()->create(lifecycle::creation(spec, 2), work)));
          co_await lifecycle::append_one<Clock>(
            *engine.append(), target, 100, 2, budget, work, drive, timer);
      });
    if (certified) co_return;
    const std::array both{listed(1), listed(2)};
    co_await life(
      both, [&](engine_type&, const engine_start& began) -> seastar::future<> {
          take(began.failure.outcome());
          require(
            began.state == engine_state::ready && began.recovered == 2
              && began.materialized == 0,
            "a start wrote again what the start before it had written");
          co_return;
      });
}

// Which block of a segment is damaged before the start after a crash.
enum class block_damage : std::uint8_t {
    // None: the start writes what only the WAL held.
    none,
    // The first block, placed in the WAL: its PREPARE writes it again.
    wal_placed,
    // The second, placed in its segment alone: nothing else holds it.
    segment_placed,
};

// One segment takes a small request, a large one and a small one, each
// reported before the next is offered: S1 is placed in the WAL and held,
// L2 in the segment alone, which writes S1's run before it and makes both
// durable there, and S3 in the WAL again, its PREPARE stamping the segment
// certified past L2. `crash` crashes the device and stops the shard as the
// crash leaves it, so S3's run is lost. Undamaged, the start writes S3 from
// its PREPARE. With S1 damaged, it writes S1 again from its PREPARE: a
// repair. With L2 damaged, S3's stamp proves the damage certified and no
// copy is left to write: the start reads everything, changes nothing and
// stays recovering.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer,
  typename Crash>
seastar::future<> damaged_certified_block(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  workload_budget& reserve,
  Driver drive,
  Timer& timer,
  block_damage damage,
  Crash crash) {
    using engine_type = local_engine<Backend, Owner, Clock>;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation_contract::bootstrap(
      files, owner, spec, budget, work, drive);
    const std::array devices{spec};
    auto config = configuration();
    config.append.segment_placement_bytes = appends::middle_placement;
    config.append.held_run_bytes = byte_count{128_KiB};
    const auto make = [&] {
        return take(
          engine_type::make(
            {files, owner, devices, spec, 0, budget, budget, reserve, budget},
            config));
    };
    const std::array wires{
      appends::child_wire(100, 1),
      appends::valued_wire(101, appends::large_value, 1),
      appends::child_wire(102, 1)};
    std::vector<local_append_receipt> receipts;
    {
        lifecycle::told heard;
        auto engine = make();
        const auto began = co_await drive.lifecycle(
          engine->start(timer, {}, {heard.sink(), {}, {}}));
        take(began.failure.outcome());
        const std::array pools{
          device_pool_share{true, false, byte_count{1_GiB}}};
        engine->placement()->sampled(pools, Clock::now());
        const auto target = take(
          co_await drive.lifecycle(
            engine->table()->create(lifecycle::creation(spec, 1), work)));
        for (const auto& wire : wires) {
            const auto outcome = co_await appends::settle(
              engine->append()->append(
                target,
                co_await appends::request(
                  wire, budget, work, appends::large_width),
                work),
              drive);
            receipts.push_back(appends::durable(outcome));
        }
        require(
          receipts[0].placement == local_append_placement::wal
            && receipts[1].placement == local_append_placement::segment
            && receipts[2].placement == local_append_placement::wal
            && receipts[2].stamps.certified_end
                 >= receipts[1].boundary.end().bytes,
          "the requests were not placed by size, or the last group's stamp "
          "does not certify the large one");
        co_await crash(*engine);
    }
    const auto data = appends::segment_path(spec, 1);
    auto stored = co_await store_contract::read_all_bytes(files, data, drive);
    if (damage != block_damage::none) {
        const auto& block
          = receipts[damage == block_damage::wal_placed ? 0 : 1].block;
        // A byte of the child, inside the block's checksummed body.
        const auto at = block.records.bytes().begin().value() + 32 + 144 + 8;
        require(at < stored.size(), "the damaged block was never written");
        stored[at] = static_cast<char>(stored[at] ^ 1);
        co_await store_contract::write_bytes(files, data, stored, drive);
    }
    lifecycle::told heard;
    auto engine = make();
    runtime::first_failure failed;
    try {
        const std::array catalog{listed(1)};
        const auto began = co_await drive.lifecycle(
          engine->start(timer, catalog, {heard.sink(), {}, {}}));
        if (damage == block_damage::segment_placed) {
            require(
              began.state == engine_state::recovering
                && (co_await store_contract::read_all_bytes(files, data, drive))
                     == stored,
              "a start went past certified damage nothing could repair, or "
              "changed the segment");
        } else {
            take(began.failure.outcome());
            require(
              began.state == engine_state::ready && began.recovered == 1
                && began.materialized != 0
                && began.repaired
                     == (damage == block_damage::wal_placed ? 1U : 0U),
              "a start did not write what the WAL held, or repaired what "
              "was intact");
            const auto written = co_await store_contract::read_all_bytes(
              files, data, drive);
            for (std::size_t i = 0; i != receipts.size(); ++i) {
                const auto& block = receipts[i].block;
                const auto placed = appends::block_at(
                  wires[i], 1, block, 8192, receipts[i].stamps);
                const auto at = block.records.bytes().begin().value();
                require(
                  written.size() >= at + placed.size()
                    && written.substr(at, placed.size()) == placed,
                  "a reported request is not where its receipt said");
            }
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    failed.observe(co_await drive.lifecycle(engine->stop()));
    take(failed.outcome());
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
                co_await lifecycle::append_one<Clock>(
                  *engine->append(),
                  target,
                  logical,
                  1,
                  budget,
                  work,
                  drive,
                  timer);
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
