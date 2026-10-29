#include "src/runtime/testing/allocation_failures.h"
#include "src/simulation/tests/engine_history_fixture.h"
#include "src/storage/tests/engine_history.h"
#include "src/storage/tests/engine_history_contract.h"
#include "src/storage/tests/engine_ledger.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <string_view>
#include <utility>

// Allocation failure swept through the owners a shard's engine runs, one
// step of a history at a time: its start on a store never used, after a
// stop and after a crash; a creation; a reader of a segment that is written
// and of a sealed one; a seal, with the completed facts it derives; a
// checkpoint; a removal of a sealed segment and of a recovered one; and the
// stop.
//
// A round runs the history over a store of its own on the simulated file
// system, with one allocation of that one step failing, as the allocator's
// injector fails one and disarms. A step may answer the failure itself, and
// then the history goes on. A life in which an allocation failed may also
// end there, as a process that cannot allocate ends: the device crashes
// where it stands, and the next life starts on what it holds, reads back
// everything the ledger was told is durable and stops. Either way the
// ledger holds, the store leaves no budget charged and no file open, and
// the round leaves no more objects live than it found.
//
// One failure a round, not every allocation after the first: a process that
// cannot allocate ends at its first failure, and past it the exceptions that
// Seastar and the C++ runtime raise (a broken condition variable's, an abort
// source's) would have to come from the runtime's few hundred bytes of
// emergency memory, which a sweep cannot keep enough of.
//
// Not swept here: a roll, which no history asks for; and whatever the
// device, the scheduler and the driver themselves allocate inside a step is
// failed with the owners', since one reactor runs them all.
namespace {
using namespace kwaque;
namespace fixture = simulation::testing::engine_history_fixture;
namespace histories = storage::testing::engine_history_contract;
using histories::history_step;
using runtime::testing::allocation_window;
using storage::testing::engine_history;
using storage::testing::history_op;
using storage::testing::history_survival;
using verdict = storage::testing::engine_ledger::verdict;

// On every change a dozen points of each window are failed, spread over it;
// with the switch every point is.
std::uint64_t sampled_points() {
    return std::getenv("KWAQUE_FULL_SWEEPS") != nullptr ? 0 : 12;
}

// The place of the first operation of this kind at or after `from`.
std::size_t
first(const engine_history& history, history_op wanted, std::size_t from = 0) {
    for (auto at = from; at != history.operations.size(); ++at)
        if (history.operations[at].op == wanted) return at;
    ADD_FAILURE() << "the history has no such operation";
    return 0;
}

// One run of `history` with `window` open over `step` alone.
void round(
  const engine_history& history, history_step step, allocation_window& window) {
    fixture::around looked;
    looked.asking = [&window, step](const history_step& asked) {
        if (asked == step) window.open();
    };
    looked.ended = [&window, step](const history_step& asked, bool threw) {
        if (asked == step) window.close();
        // A life in which an allocation failed may end at any later step.
        return threw && asked.life == step.life && window.failures() != 0;
    };
    try {
        const auto report = fixture::run(history, {}, std::move(looked)).get();
        window.close();
        EXPECT_EQ(report.verdict, verdict::holds)
          << storage::testing::verdict_name(report.verdict) << " about "
          << report.about << ", failing allocation " << window.at();
    } catch (const std::exception& failed) {
        window.close();
        ADD_FAILURE() << "failing allocation " << window.at() << ": "
                      << failed.what();
    }
}

void sweep(
  std::string_view name, const engine_history& history, history_step step) {
    if (!runtime::testing::allocation_failures_available)
        GTEST_SKIP() << "this build cannot fail an allocation";
    const auto swept = runtime::testing::sweep_allocation_failures(
      sampled_points(), [&](allocation_window& window) {
          round(history, step, window);
          EXPECT_LE(window.failures(), 1U);
      });
    std::printf(
      "%.*s: %llu allocation points, %llu rounds, %llu with a failure\n",
      static_cast<int>(name.size()),
      name.data(),
      static_cast<unsigned long long>(swept.points),
      static_cast<unsigned long long>(swept.rounds),
      static_cast<unsigned long long>(swept.failed));
    std::fflush(stdout);
    // The step allocates, and some round met a failure.
    EXPECT_GT(swept.points, 0U);
    EXPECT_GT(swept.failed, 0U);
    EXPECT_EQ(swept.leaks, 0U);
}

// The fixed history with everything a history can ask of a store and a
// restart in it.
struct whole_life final {
    engine_history history{storage::testing::whole_life_history()};
    std::size_t restart{first(history, history_op::restart)};
    [[nodiscard]] history_step
    operation(history_op wanted, std::size_t from, std::uint32_t life) const {
        return {
          history_step::kind::operation, life, first(history, wanted, from)};
    }
};

// Two segments with requests in both, and then a crash that keeps
// everything: the start that follows scans a WAL whose every request is a
// candidate.
engine_history crashed_after_appends() {
    auto history = storage::testing::appended_history();
    history.header.survival = history_survival::all;
    history.operations.push_back({history_op::crash});
    return history;
}
} // namespace

// A build that can fail an allocation also counts what is live: the sweeps
// below look for both.
TEST(EngineAllocationTest, ABuildThatFailsAllocationsCountsLiveObjects) {
    EXPECT_TRUE(
      !runtime::testing::allocation_failures_available
      || runtime::testing::live_objects_counted);
}

// A life with nothing failing leaves no more objects live than it found,
// once a first life has made what the process makes once.
TEST(EngineAllocationTest, ALifeLeavesNothingLive) {
    if (!runtime::testing::live_objects_counted)
        GTEST_SKIP() << "this build's allocator does not count live objects";
    const auto history = storage::testing::whole_life_history();
    const auto life = [&history] {
        const auto report = fixture::run(history, {}).get();
        EXPECT_EQ(report.verdict, verdict::holds) << report.about;
    };
    life();
    life();
    const auto before = runtime::testing::live_objects();
    life();
    EXPECT_EQ(runtime::testing::live_objects(), before);
}

TEST(EngineAllocationTest, AStartOnAStoreNeverUsed) {
    const whole_life life;
    sweep("a first start", life.history, {history_step::kind::start, 0, 0});
}

TEST(EngineAllocationTest, ACreation) {
    const whole_life life;
    sweep("a creation", life.history, life.operation(history_op::create, 1, 0));
}

TEST(EngineAllocationTest, AReaderOfASegmentThatIsWritten) {
    const whole_life life;
    sweep(
      "a reader of a written segment",
      life.history,
      life.operation(history_op::read, 0, 0));
}

TEST(EngineAllocationTest, ASeal) {
    const whole_life life;
    sweep("a seal", life.history, life.operation(history_op::seal, 0, 0));
}

TEST(EngineAllocationTest, AReaderOfASealedSegment) {
    const whole_life life;
    sweep(
      "a reader of a sealed segment",
      life.history,
      life.operation(
        history_op::read, first(life.history, history_op::seal), 0));
}

TEST(EngineAllocationTest, ACheckpoint) {
    const whole_life life;
    sweep(
      "a checkpoint",
      life.history,
      life.operation(history_op::checkpoint, 0, 0));
}

TEST(EngineAllocationTest, AStop) {
    const whole_life life;
    sweep(
      "a stop", life.history, {history_step::kind::stop, 0, life.restart + 1});
}

TEST(EngineAllocationTest, AStartAfterAStop) {
    const whole_life life;
    sweep(
      "a start after a stop",
      life.history,
      {history_step::kind::start, 1, life.restart + 1});
}

TEST(EngineAllocationTest, ARemovalOfARecoveredSegment) {
    const whole_life life;
    sweep(
      "a removal of a recovered segment",
      life.history,
      life.operation(history_op::remove, life.restart, 1));
}

TEST(EngineAllocationTest, ARemovalOfASealedSegment) {
    const auto history = storage::testing::removed_history();
    sweep(
      "a removal of a sealed segment",
      history,
      {history_step::kind::operation, 0, first(history, history_op::remove)});
}

TEST(EngineAllocationTest, AStartAfterACrash) {
    const auto history = crashed_after_appends();
    sweep(
      "a start after a crash",
      history,
      {history_step::kind::start, 1, first(history, history_op::crash) + 1});
}
