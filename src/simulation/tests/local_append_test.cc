#include "src/base/units.h"
#include "src/simulation/fake_file_test_support.h"
#include "src/simulation/tests/local_store_fixture.h"
#include "src/simulation/virtual_time.h"
#include "src/storage/tests/local_append_contract.h"
#include "src/storage/tests/local_store_contract.h"
#include "src/storage/tests/recovery_successor_contract.h"

#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <optional>
#include <utility>

namespace {
using namespace kwaque;
using namespace kwaque::simulation;
using namespace kwaque::storage;
using namespace kwaque::storage::testing::store_contract;
using namespace kwaque::simulation::testing::local_store_fixture;
namespace contract = kwaque::storage::testing::local_append_contract;

// A file of the fixture and the flushes it has had once the fixture is
// attached. Creation order is deterministic, so a second environment with
// the same segments reproduces both.
struct flush_target final {
    std::uint64_t object{0};
    std::uint64_t flushes{0};
};
struct flush_targets final {
    flush_target wal;
    std::array<flush_target, 2> segments;
};
seastar::future<flush_targets> calibrate(std::uint32_t segments) {
    flush_targets found;
    co_await with_local_store(
      [&found, segments](
        auto& files,
        auto& owner,
        const auto& spec,
        auto& budget,
        auto drive,
        auto& timer) -> seastar::future<> {
          co_await contract::with_local_append<simulation::monotonic_clock>(
            files,
            owner,
            spec,
            budget,
            drive,
            timer,
            {.outstanding = 1, .segments = segments},
            [&](auto&, auto& writer, auto&, auto&, auto&) -> seastar::future<> {
                const auto target = [&](const runtime::file_path& file) {
                    const auto object = take(
                      fake_file_test_access::lookup(
                        files,
                        take(
                          fake_file_test_access::resolve(
                            files, file.value()))));
                    return flush_target{
                      object.value(),
                      take(
                        fake_file_test_access::occurrences(
                          files,
                          object,
                          runtime::builtin_fault_point::file_flush))};
                };
                found.wal = target(
                  contract::wal_path(
                    spec, writer.prepared_head()->incarnation));
                for (std::uint32_t i = 0; i != segments; ++i)
                    found.segments[i] = target(
                      contract::segment_path(spec, i + 1U));
                co_return;
            });
      });
    co_return found;
}

// The flush'th next flush of a calibrated file, with one decision.
fault_rule flush_fault(
  std::uint64_t id,
  flush_target target,
  std::uint64_t flush,
  runtime::fault_decision decision) {
    const auto occurrence = take(
      runtime::fault_occurrence::make(target.flushes + flush));
    return take(
      fault_rule::make(
        take(fault_rule_id::make(id)),
        runtime::builtin_fault_point::file_flush,
        runtime::fault_object_key::from_u64(target.object),
        occurrence,
        occurrence,
        fault_selector::once(),
        decision));
}
runtime::fault_decision failed_flush(runtime::fault_action when) {
    return take(
      runtime::fault_decision::make_file_failure(
        when, runtime::file_failure_detail::device_io));
}
runtime::fault_decision slow_flush() {
    return runtime::fault_decision::make_delay(
      runtime::monotonic_duration{1'000'000'000});
}

// The flushes the fake counted for one file.
struct flush_count final {
    fake_file_system* files;
    std::optional<std::uint64_t>
    operator()(const runtime::file_path& path) const {
        const auto object = take(
          fake_file_test_access::lookup(
            *files,
            take(fake_file_test_access::resolve(*files, path.value()))));
        return take(
          fake_file_test_access::occurrences(
            *files, object, runtime::builtin_fault_point::file_flush));
    }
};

} // namespace

SEASTAR_TEST_CASE(local_append_fake_durable_append) {
    co_await with_local_store([](auto&&... shared) {
        return contract::durable_append<simulation::monotonic_clock>(shared...);
    });
}

SEASTAR_TEST_CASE(local_append_fake_independent_alignments) {
    co_await with_local_store([](auto&&... shared) {
        return contract::durable_append<simulation::monotonic_clock>(
          shared..., 4096);
    });
}

SEASTAR_TEST_CASE(local_append_fake_occupancy_batching) {
    co_await with_local_store([](auto&&... shared) {
        return contract::occupancy_batching<simulation::monotonic_clock>(
          shared..., true);
    });
}

SEASTAR_TEST_CASE(local_append_fake_full_group_starts_next) {
    co_await with_local_store([](auto&&... shared) {
        return contract::full_group_starts_next<simulation::monotonic_clock>(
          shared...);
    });
}

SEASTAR_TEST_CASE(local_append_fake_multi_segment_group) {
    co_await with_local_store([](auto&&... shared) {
        return contract::multi_segment_group<simulation::monotonic_clock>(
          shared..., true);
    });
}

SEASTAR_TEST_CASE(local_append_fake_placement_by_size) {
    co_await with_local_store([](auto&&... shared) {
        return contract::placement_by_size<simulation::monotonic_clock>(
          shared..., true);
    });
}

// The same requests at three thresholds, each in a store of its own, with
// nothing held and with a hold, and the segments' flushes counted.
SEASTAR_TEST_CASE(local_append_fake_placement_ends) {
    for (const auto hold : {byte_count{}, byte_count{64_KiB}})
        co_await contract::placement_ends(
          [hold](byte_count placement)
            -> seastar::future<contract::placement_trace> {
              contract::placement_trace trace;
              co_await with_local_store(
                [&trace, placement, hold](auto& files, auto&&... shared) {
                    return contract::placement_run<simulation::monotonic_clock>(
                             files,
                             shared...,
                             placement,
                             hold,
                             flush_count{&files})
                      .then([&trace](contract::placement_trace run) {
                          trace = std::move(run);
                      });
                });
              co_return trace;
          },
          hold.value() != 0);
}

SEASTAR_TEST_CASE(local_append_fake_rejections_have_no_effect) {
    co_await with_local_store([](auto&&... shared) {
        return contract::rejections_have_no_effect<simulation::monotonic_clock>(
          shared...);
    });
}

SEASTAR_TEST_CASE(local_append_fake_pressure_before_freeze) {
    co_await with_local_store([](auto&&... shared) {
        return contract::pressure_before_freeze<simulation::monotonic_clock>(
          shared...);
    });
}

SEASTAR_TEST_CASE(local_append_fake_rotation_before_freeze) {
    co_await with_local_store([](auto&&... shared) {
        return contract::rotation_before_freeze<simulation::monotonic_clock>(
          shared...);
    });
}

SEASTAR_TEST_CASE(local_append_fake_segment_handle_budget) {
    co_await with_local_store([](auto&&... shared) {
        return contract::segment_handle_budget<simulation::monotonic_clock>(
          shared...);
    });
}

SEASTAR_TEST_CASE(local_recovered_successor_fake) {
    co_await with_local_store([](auto& files, auto&&... shared) {
        return storage::testing::recovery_successor_contract::
          recovered_successor<simulation::monotonic_clock>(
            files, shared..., flush_count{&files});
    });
}

SEASTAR_TEST_CASE(local_checkpoint_fake_regressions) {
    co_await with_local_store([](auto&&... shared) {
        return storage::testing::recovery_successor_contract::
          checkpoint_regressions<simulation::monotonic_clock>(shared...);
    });
}

SEASTAR_TEST_CASE(local_checkpoint_fake_detached_boundary_pinned) {
    co_await with_local_store([](auto&&... shared) {
        return storage::testing::recovery_successor_contract::
          detached_boundary_pinned<simulation::monotonic_clock>(shared...);
    });
}

SEASTAR_TEST_CASE(local_checkpoint_fake_reclaims_within_the_retained_limit) {
    co_await with_reserved_store([](auto&&... shared) {
        return storage::testing::recovery_successor_contract::
          retained_wal_reclaimed<simulation::monotonic_clock>(shared...);
    });
}

// Rotations sync what the closed files hold, held or not, so a shard that
// never asks for a sync stays within its retained limit.
SEASTAR_TEST_CASE(local_checkpoint_fake_retained_wal_synced) {
    for (const auto hold : {byte_count{}, byte_count{64_KiB}})
        co_await with_reserved_store([hold](auto&&... shared) {
            return storage::testing::recovery_successor_contract::
              retained_wal_synced<simulation::monotonic_clock>(shared..., hold);
        });
}

// The segment's first two syncs are slow: a rotation at the retained limit
// waits for them and goes on, and nothing deadlocks.
SEASTAR_TEST_CASE(local_checkpoint_fake_slow_sync_at_limit) {
    const auto targets = co_await calibrate(1);
    co_await with_store<true>(
      [](auto&&... shared) {
          return storage::testing::recovery_successor_contract::
            retained_wal_synced<simulation::monotonic_clock>(
              shared..., byte_count{64_KiB});
      },
      {flush_fault(1, targets.segments[0], 1, slow_flush()),
       flush_fault(2, targets.segments[0], 2, slow_flush())});
}

SEASTAR_TEST_CASE(local_checkpoint_fake_after_sync) {
    co_await with_reserved_store([](auto& files, auto&&... shared) {
        return storage::testing::recovery_successor_contract::
          checkpoint_after_sync<simulation::monotonic_clock>(
            files, shared..., flush_count{&files});
    });
}

SEASTAR_TEST_CASE(local_checkpoint_fake_retained_limit_pressure) {
    co_await with_reserved_store([](auto&&... shared) {
        return storage::testing::recovery_successor_contract::
          retained_wal_pressure<simulation::monotonic_clock>(shared...);
    });
}

// A checkpoint run cut after every number of scheduler events, once with
// unsynced names reverting at the crash and once with all of them surviving.
// Each cut is its own store, so no history depends on the one before it.
SEASTAR_TEST_CASE(local_checkpoint_fake_crash_at_every_cut) {
    for (const auto survive : {false, true}) {
        const auto crash = survive ? std::optional{fake_crash_policy{
                                       .namespace_percent = 100}}
                                   : std::nullopt;
        // Every cut where names revert, the ordinary crash; every second
        // one where they all survive.
        const std::uint32_t stride = survive ? 2 : 1;
        bool finished = false;
        std::uint32_t cut = 0;
        for (; !finished && cut < 2048; cut += stride)
            co_await with_local_store(
              [cut, &finished](
                auto& files,
                auto& owner,
                auto& spec,
                auto& budget,
                auto drive,
                auto& timer) -> seastar::future<> {
                  finished
                    = co_await storage::testing::recovery_successor_contract::
                      checkpoint_crash<simulation::monotonic_clock>(
                        files,
                        owner,
                        spec,
                        budget,
                        drive,
                        timer,
                        cut,
                        one_event{&drive.events});
              },
              {},
              crash);
        BOOST_REQUIRE(finished);
        // Several operations lie between the first and the last effect.
        BOOST_CHECK(cut > 8);
    }
}

SEASTAR_TEST_CASE(local_recovery_repeated_restarts_fake) {
    co_await with_local_store([](auto&&... shared) {
        return storage::testing::recovery_successor_contract::repeated_restarts<
          simulation::monotonic_clock>(shared...);
    });
}

// At every placement and hold.
SEASTAR_TEST_CASE(local_append_fake_close_drains_accepted) {
    for (const auto profile : contract::placement_profiles)
        co_await with_local_store([profile](auto&&... shared) {
            return contract::close_drains_accepted<simulation::monotonic_clock>(
              shared..., profile);
        });
}

// At every placement and hold.
SEASTAR_TEST_CASE(local_append_fake_quiesce_drains_one_attachment) {
    for (const auto profile : contract::placement_profiles)
        co_await with_local_store([profile](auto&&... shared) {
            return contract::quiesce_drains_one_attachment<
              simulation::monotonic_clock>(shared..., profile);
        });
}

SEASTAR_TEST_CASE(local_append_fake_barrier_coalescing) {
    co_await with_local_store([](auto&&... shared) {
        return contract::barrier_coalescing<simulation::monotonic_clock>(
          shared...);
    });
}

SEASTAR_TEST_CASE(local_append_fake_independent_segment_outcomes) {
    const auto targets = co_await calibrate(2);
    BOOST_REQUIRE_NE(targets.segments[1].object, 0U);
    co_await with_local_store(
      [](auto&&... shared) {
          return contract::independent_segment_outcomes<
            simulation::monotonic_clock>(shared...);
      },
      {flush_fault(
        1,
        targets.segments[1],
        1,
        failed_flush(runtime::fault_action::file_failure_before_effect))});
}

SEASTAR_TEST_CASE(local_append_fake_interest_ends_without_cancelling) {
    co_await with_local_store([](auto&&... shared) {
        return contract::interest_ends_without_cancelling<
          simulation::monotonic_clock>(shared..., true);
    });
}

SEASTAR_TEST_CASE(local_append_fake_deadline_detaches_waiter) {
    co_await with_local_store([](auto&&... shared) {
        return contract::deadline_detaches_waiter<simulation::monotonic_clock>(
          shared...);
    });
}

SEASTAR_TEST_CASE(local_append_fake_completed_retry_facts) {
    co_await with_local_store([](auto&&... shared) {
        return contract::completed_retry_facts<simulation::monotonic_clock>(
          shared...);
    });
}

// The limit is reached before any I/O, so the fake backend alone runs it.
SEASTAR_TEST_CASE(local_append_fake_stream_limit_rolls) {
    co_await with_local_store([](auto&&... shared) {
        return contract::stream_limit_rolls<simulation::monotonic_clock>(
          shared...);
    });
}

SEASTAR_TEST_CASE(local_append_fake_obligations_track_segments) {
    co_await with_local_store([](auto&&... shared) {
        return contract::obligations_track_segments<
          simulation::monotonic_clock>(shared..., true);
    });
}

SEASTAR_TEST_CASE(local_append_fake_shutdown_stops_admission) {
    co_await with_local_store([](auto&&... shared) {
        return contract::shutdown_stops_admission<simulation::monotonic_clock>(
          shared...);
    });
}

// The flush the placement does not wait for is the slow one.
SEASTAR_TEST_CASE(local_append_fake_barrier_order) {
    const auto targets = co_await calibrate(1);
    for (const bool segment_placed : {true, false})
        co_await with_local_store(
          [segment_placed](auto&&... shared) {
              return contract::barrier_order<simulation::monotonic_clock>(
                shared..., segment_placed);
          },
          {flush_fault(
            1,
            segment_placed ? targets.wal : targets.segments[0],
            1,
            slow_flush())});
}

// Results publish in file order whichever barrier finishes first, with the
// small groups written at once and held.
SEASTAR_TEST_CASE(local_append_fake_file_order_publication) {
    const auto targets = co_await calibrate(1);
    for (const auto hold : {byte_count{}, byte_count{64_KiB}})
        for (const bool wal_slow : {true, false})
            co_await with_local_store(
              [wal_slow, hold](auto&&... shared) {
                  return contract::file_order_publication<
                    simulation::monotonic_clock>(shared..., wal_slow, hold);
              },
              {flush_fault(
                1,
                wal_slow ? targets.wal : targets.segments[0],
                1,
                slow_flush())});
}

SEASTAR_TEST_CASE(local_append_fake_reordered_completions) {
    const auto targets = co_await calibrate(2);
    co_await with_local_store(
      [](auto&&... shared) {
          return contract::reordered_completions<simulation::monotonic_clock>(
            shared...);
      },
      {flush_fault(1, targets.segments[0], 1, slow_flush())});
}

SEASTAR_TEST_CASE(local_append_fake_wal_failure_after_segment) {
    const auto targets = co_await calibrate(1);
    for (const auto hold : {byte_count{}, byte_count{64_KiB}})
        co_await with_local_store(
          [hold](auto&&... shared) {
              return contract::wal_failure_after_segment<
                simulation::monotonic_clock>(shared..., hold);
          },
          {flush_fault(
            1,
            targets.wal,
            1,
            failed_flush(runtime::fault_action::file_failure_after_effect))});
}

SEASTAR_TEST_CASE(local_append_fake_held_until_synced) {
    co_await with_local_store([](auto& files, auto&&... shared) {
        return contract::held_until_synced<simulation::monotonic_clock>(
          files, shared..., flush_count{&files});
    });
}

SEASTAR_TEST_CASE(local_append_fake_rotation_syncs) {
    co_await with_local_store([](auto& files, auto&&... shared) {
        return contract::rotation_syncs<simulation::monotonic_clock>(
          files, shared..., flush_count{&files});
    });
}

// Each segment's first sync is slow.
SEASTAR_TEST_CASE(local_append_fake_sync_limit) {
    const auto targets = co_await calibrate(2);
    for (const std::uint32_t syncs : {1U, 2U})
        co_await with_local_store(
          [syncs](auto& files, auto&&... shared) {
              return contract::sync_limit<simulation::monotonic_clock>(
                files, shared..., syncs, flush_count{&files});
          },
          {flush_fault(1, targets.segments[0], 1, slow_flush()),
           flush_fault(2, targets.segments[1], 1, slow_flush())});
}

SEASTAR_TEST_CASE(local_append_fake_pressure_releases_holds) {
    co_await with_local_store([](auto&&... shared) {
        return contract::pressure_releases_holds<simulation::monotonic_clock>(
          shared...);
    });
}

SEASTAR_TEST_CASE(local_append_fake_every_failure_reported) {
    const auto targets = co_await calibrate(2);
    BOOST_REQUIRE_NE(targets.segments[1].object, 0U);
    co_await with_local_store(
      [](auto&&... shared) {
          return contract::every_failure_reported<simulation::monotonic_clock>(
            shared...);
      },
      {flush_fault(
         1,
         targets.segments[1],
         1,
         failed_flush(runtime::fault_action::file_failure_before_effect)),
       flush_fault(
         2,
         targets.wal,
         2,
         failed_flush(runtime::fault_action::file_failure_after_effect))});
}

// Each history at each placement: written at once, held, and placed in the
// segment alone, which no WAL flush decides.
SEASTAR_TEST_CASE(local_append_fake_crash_histories) {
    using history = contract::survival_history;
    using placement = contract::survival_placement;
    const auto targets = co_await calibrate(1);
    for (const auto placed :
         {placement::wal, placement::held, placement::segment})
        for (const auto chosen :
             {history::both,
              history::wal_only,
              history::footer_only,
              history::neither,
              history::lost_wal_completion}) {
            std::vector<fault_rule> faults;
            if (chosen == history::wal_only || chosen == history::neither)
                faults.push_back(
                  flush_fault(1, targets.segments[0], 1, slow_flush()));
            if (chosen == history::footer_only || chosen == history::neither)
                faults.push_back(flush_fault(2, targets.wal, 1, slow_flush()));
            if (chosen == history::lost_wal_completion)
                faults.push_back(flush_fault(
                  2,
                  targets.wal,
                  1,
                  runtime::fault_decision::make_drop_completion()));
            co_await with_local_store(
              [chosen, placed](auto&&... shared) {
                  return contract::crash_history<simulation::monotonic_clock>(
                    shared..., chosen, placed);
              },
              std::move(faults));
        }
}

// At every placement and hold.
SEASTAR_TEST_CASE(local_append_fake_concurrent_pipeline) {
    for (const auto profile : contract::placement_profiles)
        co_await with_local_store([profile](auto&&... shared) {
            return contract::concurrent_pipeline<simulation::monotonic_clock>(
              shared..., profile);
        });
}

SEASTAR_TEST_CASE(local_append_fake_close_during_slow_flush) {
    const auto targets = co_await calibrate(1);
    co_await with_local_store(
      [](auto&&... shared) {
          return contract::close_during_slow_flush<simulation::monotonic_clock>(
            shared...);
      },
      {flush_fault(1, targets.wal, 1, slow_flush())});
}

#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION) && !defined(SEASTAR_DEBUG)
SEASTAR_TEST_CASE(local_append_fake_acceptance_allocation_cuts) {
    std::size_t reached = 0;
    for (std::size_t at = 0; at != 256; ++at) {
        bool injected = false;
        co_await with_local_store(
          [at, &injected](auto&&... shared) -> seastar::future<> {
              injected = co_await contract::acceptance_allocation_cut<
                simulation::monotonic_clock>(shared..., at);
          });
        if (!injected) break;
        ++reached;
    }
    BOOST_REQUIRE_GT(reached, 0U);
    BOOST_REQUIRE_LT(reached, 256U);
}
#endif

SEASTAR_TEST_CASE(local_append_fake_wal_stops_before_acceptance) {
    co_await with_local_store([](auto&&... shared) {
        return contract::wal_stops_before_acceptance<
          simulation::monotonic_clock>(shared...);
    });
}

#if !defined(SEASTAR_DEBUG)
SEASTAR_TEST_CASE(local_append_fake_close_before_group_runs) {
    co_await with_local_store([](auto&&... shared) {
        return contract::close_before_group_runs<simulation::monotonic_clock>(
          shared...);
    });
}
#endif

SEASTAR_TEST_CASE(local_append_fake_identity_and_rejections) {
    co_await with_local_store([](auto&&... shared) {
        return contract::identity_and_rejections<simulation::monotonic_clock>(
          shared...);
    });
}
