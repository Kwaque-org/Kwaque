#include "src/base/allocation.h"
#include "src/runtime/testing/allocation_failures.h"
#include "src/simulation/tests/engine_history_fixture.h"
#include "src/storage/tests/engine_history.h"
#include "src/storage/tests/engine_history_contract.h"
#include "src/storage/tests/engine_ledger.h"

#include <seastar/core/memory.hh>

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <utility>

// One shard's storage with as many segments taking appends as its engine is
// set to let, and a cycle in which the oldest of them is sealed, another is
// created in its place and takes requests, a checkpoint is taken and the
// sealed one is removed. Four is the most the store's handle credits pay
// for. A restart follows, and everything is read back.
//
// Three things are held of it. The ledger holds. The store leaves no budget
// charged, no file open and no operation pending, which its fixture
// requires. And between the first start and the restart nothing allocates
// more than one allocation may be: not the engine, and not the device, the
// scheduler or the driver under it. The read-back that follows the restart
// reads a whole data file into one string and is outside that.
namespace {
using namespace kwaque;
namespace fixture = simulation::testing::engine_history_fixture;
namespace histories = storage::testing::engine_history_contract;
using histories::history_step;
using verdict = storage::testing::engine_ledger::verdict;

constexpr std::uint8_t writable = 4;
constexpr unsigned cycles = 24;
// The allocator's threshold is inclusive.
constexpr std::size_t above_the_ceiling = maximum_contiguous_allocation_bytes
                                          + 1;
} // namespace

TEST(EngineChurnTest, SegmentsChurnAtTheWritableBound) {
    const auto history = storage::testing::churned_history(writable, cycles);
    histories::history_settings settings;
    settings.engine.table.maximum_writable = writable;
    settings.segments = writable + cycles;

    const seastar::memory::scoped_large_allocation_warning_threshold counted{
      above_the_ceiling};
    std::uint64_t before = 0, large = 0;
    bool watched = false;
    fixture::around looked;
    looked.ended = [&before](const history_step& step, bool) {
        if (step.what == history_step::kind::start && step.life == 0)
            before = seastar::memory::stats().large_allocations();
        return false;
    };
    looked.asking = [&](const history_step& step) {
        if (step.what != history_step::kind::start || step.life != 1) return;
        large = seastar::memory::stats().large_allocations() - before;
        watched = true;
    };
    const auto report
      = fixture::run(history, {}, std::move(looked), true, settings).get();
    EXPECT_EQ(report.verdict, verdict::holds)
      << storage::testing::verdict_name(report.verdict) << " about "
      << report.about;
    EXPECT_EQ(report.lives, 2U);
    EXPECT_EQ(report.writable, std::uint32_t{writable});
    EXPECT_TRUE(watched);
    if (runtime::testing::live_objects_counted) {
        EXPECT_EQ(large, 0U)
          << "allocations above the ceiling in the first life";
    } else {
        std::printf(
          "not held: this build's allocator does not count allocations "
          "above the ceiling\n");
    }
}
