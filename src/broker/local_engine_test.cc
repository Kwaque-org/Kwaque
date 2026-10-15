#include "src/broker/native_store_test_support.h"
#include "src/runtime/testing/test_directory.h"
#include "src/storage/tests/engine_fixture_contract.h"
#include "src/storage/tests/engine_history.h"
#include "src/storage/tests/engine_history_contract.h"
#include "src/storage/tests/engine_ledger.h"
#include "src/storage/tests/local_append_contract.h"
#include "src/storage/tests/local_engine_contract.h"
#include "src/storage/tests/recovery_successor_contract.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/tmp_file.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <cstdint>
#include <exception>
#include <utility>

// The storage contracts that need the shard's second budget, the engine
// itself, or more than one device, on the production file system.
//
// What still runs on the simulated file system only, and why:
//  - the engine's removal cut after every scheduler event, every crash
//    sweep, and every history with a crash in it: a native device cannot be
//    crashed or stepped one event at a time;
//  - the checkpoint and retry-snapshot runs cut at every event, and the
//    checkpoint regressions: the same;
//  - the append cases that fail or delay a chosen flush (pressure before a
//    freeze, coalesced barriers, independent segment outcomes, a detached
//    deadline, barrier order, reordered completions, a WAL failure after a
//    segment, a close during a slow flush, a WAL that stops before
//    acceptance): a native file system fails and delays nothing on request;
//  - what is open while a group is in flight, and two requests of a segment
//    under one footer: natively an I/O may overtake an acceptance, so the
//    cases below look only at what holds once the requests are durable;
//  - adding a device to a store, and the check of every device before any
//    is changed: both crash the device, and one holds two owners of a
//    directory at once, which a host lock refuses.
namespace {
using namespace kwaque;
namespace contract = storage::testing::local_append_contract;
namespace histories = storage::testing::engine_history_contract;
namespace image = storage::testing::engine_fixture_contract;
namespace successor = storage::testing::recovery_successor_contract;
using clock_type = broker::testing::native_clock;
using verdict = storage::testing::engine_ledger::verdict;

// The churn below: this many segments take appends at one time, and this
// many times the oldest of them gives way to a new one.
constexpr std::uint8_t churn_writable = 4;
constexpr unsigned churn_cycles = 16;

// One device in a directory of its own: the body takes the appends' budget
// and the reserve.
template<typename Func>
seastar::future<> with_reserved_store(Func function) {
    co_await seastar::tmp_dir::do_with(
      runtime::testing::test_directory_template(),
      seastar::coroutine::lambda(
        [&function](seastar::tmp_dir& directory) -> seastar::future<> {
            co_await broker::testing::with_native_store(
              directory.get_path() / "store", std::move(function));
        }));
}
// The same for a body that takes the appends' budget alone.
template<typename Func>
seastar::future<> with_local_store(Func function) {
    return with_reserved_store([function = std::move(function)](
                                 auto& files,
                                 auto& owner,
                                 const auto& spec,
                                 auto& budget,
                                 auto&,
                                 auto drive,
                                 auto& timer) mutable {
        return function(files, owner, spec, budget, drive, timer);
    });
}

// The checked-in store image on three directories, and the engine started
// on it.
seastar::future<> on_the_image(bool sealed) {
    co_await seastar::tmp_dir::do_with(
      runtime::testing::test_directory_template(),
      seastar::coroutine::lambda(
        [sealed](seastar::tmp_dir& directory) -> seastar::future<> {
            const auto& base = directory.get_path();
            co_await broker::testing::with_native_devices(
              std::array{base / "w", base / "a", base / "b"},
              image::devices,
              [sealed](
                auto& files,
                auto& owner,
                const auto& devices,
                auto& budget,
                auto& reserve,
                auto drive,
                auto& timer) {
                  return image::started_on_the_image<clock_type>(
                    files,
                    owner,
                    devices,
                    budget,
                    reserve,
                    drive,
                    timer,
                    sealed);
              });
        }));
}
} // namespace

SEASTAR_TEST_CASE(local_engine_native_started_and_restarted) {
    co_await with_reserved_store([](auto&&... shared) {
        return storage::testing::local_engine_contract::started_and_restarted<
          clock_type>(shared...);
    });
}

SEASTAR_TEST_CASE(local_checkpoint_native_detached_boundary_pinned) {
    co_await with_local_store([](auto&&... shared) {
        return successor::detached_boundary_pinned<clock_type>(shared...);
    });
}

SEASTAR_TEST_CASE(local_checkpoint_native_reclaims_within_the_retained_limit) {
    co_await with_reserved_store([](auto&&... shared) {
        return successor::retained_wal_reclaimed<clock_type>(shared...);
    });
}

SEASTAR_TEST_CASE(local_checkpoint_native_retained_limit_pressure) {
    co_await with_reserved_store([](auto&&... shared) {
        return successor::retained_wal_pressure<clock_type>(shared...);
    });
}

SEASTAR_TEST_CASE(local_append_native_multi_segment_group) {
    co_await with_local_store([](auto&&... shared) {
        return contract::multi_segment_group<clock_type>(shared..., false);
    });
}

SEASTAR_TEST_CASE(local_append_native_segment_handle_budget) {
    co_await with_local_store([](auto&&... shared) {
        return contract::segment_handle_budget<clock_type>(shared...);
    });
}

SEASTAR_TEST_CASE(local_append_native_obligations_track_segments) {
    co_await with_local_store([](auto&&... shared) {
        return contract::obligations_track_segments<clock_type>(
          shared..., false);
    });
}

// One life with everything a history can ask, a clean restart and a second
// life, with a ledger of what the driver did and was told: after the
// restart every segment is found under one of its names and read back.
SEASTAR_TEST_CASE(engine_history_native_a_life_is_read_back) {
    co_await with_reserved_store(
      [](
        auto& files,
        auto& owner,
        const auto& spec,
        auto& budget,
        auto& reserve,
        auto drive,
        auto& timer) -> seastar::future<> {
          const auto report = co_await histories::run_history<clock_type>(
            files,
            owner,
            spec,
            budget,
            reserve,
            drive,
            timer,
            broker::testing::one_turn{},
            storage::testing::whole_life_history(),
            {});
          BOOST_REQUIRE_MESSAGE(
            report.verdict == verdict::holds,
            "verdict " << storage::testing::verdict_name(report.verdict)
                       << " about " << report.about);
          BOOST_CHECK_EQUAL(report.lives, 2U);
      });
}

// Segments are created, sealed and removed in a cycle with as many taking
// appends as the engine is set to let: each time the oldest is sealed,
// another takes its place and its requests, a checkpoint is taken and the
// sealed one is removed. Beside the shard a task of the appends' own group
// does nothing but yield, and how long it waited for a turn is how long the
// shard's work kept the reactor from a request. Nothing is drained for it:
// the reactor is never idle. Its longest wait is told, and is held to no
// bound until a measured run has set one.
SEASTAR_TEST_CASE(
  engine_history_native_segments_churn_beside_a_foreground_task) {
    co_await with_reserved_store(
      [](
        auto& files,
        auto& owner,
        const auto& spec,
        auto& budget,
        auto& reserve,
        auto,
        auto& timer) -> seastar::future<> {
          histories::history_settings settings;
          settings.engine.table.maximum_writable = churn_writable;
          settings.segments = churn_writable + churn_cycles;
          broker::testing::foreground probe;
          auto probing = seastar::with_scheduling_group(
            budget.scheduling_group(),
            [&probe] { return broker::testing::foreground_loop(probe); });
          histories::history_report report;
          std::exception_ptr failed;
          try {
              report = co_await histories::run_history<clock_type>(
                files,
                owner,
                spec,
                budget,
                reserve,
                broker::testing::waiting_driver{},
                timer,
                broker::testing::one_yield{},
                storage::testing::churned_history(churn_writable, churn_cycles),
                {},
                {},
                std::move(settings));
          } catch (...) {
              failed = std::current_exception();
          }
          probe.stop = true;
          co_await std::move(probing);
          if (failed) std::rethrow_exception(failed);
          BOOST_REQUIRE_MESSAGE(
            report.verdict == verdict::holds,
            "verdict " << storage::testing::verdict_name(report.verdict)
                       << " about " << report.about);
          BOOST_CHECK_EQUAL(report.lives, 2U);
          BOOST_CHECK_EQUAL(report.writable, std::uint32_t{churn_writable});
          BOOST_CHECK_GT(probe.turns, 0U);
          BOOST_TEST_MESSAGE(
            "a foreground task took "
            << probe.turns << " turns and waited at most "
            << probe.longest_nanoseconds << " ns for one");
      });
}

// The engine starts on a store image it never wrote: every record, the WAL
// and both segments are the checked-in files, in three directories.
SEASTAR_TEST_CASE(engine_fixture_native_started_on_the_image) {
    co_await on_the_image(false);
}

// The same image with its first segment sealed: a reader is given the two
// requests the image holds, byte for byte.
SEASTAR_TEST_CASE(engine_fixture_native_sealed_segment_read_from_the_image) {
    co_await on_the_image(true);
}
