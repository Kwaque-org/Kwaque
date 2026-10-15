#include "src/base/units.h"
#include "src/simulation/environment.h"
#include "src/simulation/fake_file.h"
#include "src/simulation/fake_file_test_support.h"
#include "src/simulation/tests/engine_history_fixture.h"
#include "src/simulation/tests/local_store_fixture.h"
#include "src/simulation/virtual_time.h"
#include "src/storage/tests/engine_history.h"
#include "src/storage/tests/engine_history_contract.h"
#include "src/storage/tests/engine_ledger.h"
#include "src/storage/tests/local_append_contract.h"
#include "src/storage/tests/local_store_contract.h"

#include <seastar/core/future.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ios>
#include <map>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace {
using namespace kwaque;
using namespace kwaque::simulation;
using namespace kwaque::storage;
using namespace kwaque::storage::testing::store_contract;
using namespace kwaque::simulation::testing::local_store_fixture;
using simulation::testing::engine_history_fixture::image;
namespace histories = kwaque::storage::testing::engine_history_contract;
using storage::testing::engine_history;
using storage::testing::engine_ledger;
using storage::testing::history_op;
using storage::testing::history_survival;
using verdict = engine_ledger::verdict;

// What one run saw at its crashes: the names pending as each was taken, and
// the image each left; and the image the whole run left.
struct seen final {
    std::vector<std::size_t> pending;
    std::vector<std::uint64_t> images;
    std::uint64_t last{0};
};
// One run of a history over a store of its own. The device is down from
// each crash to the start after it: what the crash left running is stopped
// and changes nothing. With `cut_back`, the first segment's data file is cut
// back to that many bytes before each start that follows a crash, as a
// device that lost what was written past them would leave it.
seastar::future<histories::history_report> run_one(
  const engine_history& history,
  histories::history_cuts cuts,
  seen* crashes = nullptr,
  std::optional<std::uint64_t> cut_back = std::nullopt) {
    simulation::testing::engine_history_fixture::around looked;
    looked.crashing = [crashes](fake_file_system& files, std::uint32_t) {
        if (crashes)
            crashes->pending.push_back(
              fake_file_test_access::pending_namespace_groups(files));
    };
    looked.starting = [crashes, cut_back](
                        fake_file_system& files,
                        const local_device_spec& spec,
                        std::uint32_t) {
        if (crashes) crashes->images.push_back(image(files));
        if (cut_back) {
            const auto data
              = storage::testing::local_append_contract::segment_path(spec, 1);
            const auto held = take(
              fake_file_test_access::resolve(files, data.value()));
            BOOST_REQUIRE_GT(
              take(fake_file_test_access::visible_size(files, held)),
              *cut_back);
            take(fake_file_test_access::truncate(files, held, *cut_back));
        }
    };
    looked.over = [crashes](fake_file_system& files) {
        if (crashes) crashes->last = image(files);
    };
    co_return co_await simulation::testing::engine_history_fixture::run(
      history, cuts, std::move(looked));
}
void holds(const histories::history_report& report, const std::string& what) {
    BOOST_REQUIRE_MESSAGE(
      report.verdict == verdict::holds,
      what << ": verdict " << static_cast<unsigned>(report.verdict) << " about "
           << report.about);
}

// A history and where what it tests begins: the operations before `from`
// only set it up, and no crash is taken in them.
struct tested final {
    engine_history history;
    std::size_t from;
};
// Two segments, then a group of requests over both.
tested appended() { return {storage::testing::appended_history(), 2}; }
// A segment with requests, then another that follows it while it seals,
// and a checkpoint.
tested rolled() { return {storage::testing::rolled_history(), 3}; }
// A segment with requests, then its seal, a checkpoint and its removal.
tested removed() { return {storage::testing::removed_history(), 3}; }

// Every cut is taken, and a history is cut from its first operation, only
// when that is asked for: it takes many times what a sample does.
bool whole_sweeps() { return std::getenv("KWAQUE_FULL_SWEEPS") != nullptr; }

// One cut of a sweep: where it was taken, what the crash was taken with and
// left, and how many changes the start after it took to its stop.
struct taken final {
    std::uint64_t cut;
    std::size_t pending;
    std::uint64_t image;
    std::uint64_t after;
};
// A history's first life cut after its device's changes, from where what it
// tests begins to the end of its stop. Each cut is a store of its own: the
// crash, a start with the catalog its owner's knowledge makes, and
// everything read back. An uncut run first says how many changes there are
// to cut after. Every one of them is taken when the whole is asked for;
// otherwise `sample` of them, evenly apart or, with `leading`, the first.
seastar::future<std::vector<taken>> sweep(
  const engine_history& history,
  std::size_t from,
  std::uint64_t sample,
  bool leading = false) {
    histories::history_cuts cuts;
    cuts.from = whole_sweeps() ? 0 : from;
    const auto uncut = co_await run_one(history, cuts);
    holds(uncut, "the uncut history");
    BOOST_REQUIRE(!uncut.reached[0]);
    const bool every = whole_sweeps() || leading;
    const auto last = whole_sweeps() || !leading
                        ? uncut.changes[0]
                        : std::min(uncut.changes[0], sample - 1);
    const auto apart = every
                         ? 1
                         : std::max<std::uint64_t>(1, (last + sample) / sample);
    std::vector<taken> all;
    for (std::uint64_t cut = 0; cut <= last; cut += apart) {
        seen one;
        cuts.at[0] = cut;
        const auto report = co_await run_one(history, cuts, &one);
        holds(report, "cut " + std::to_string(cut));
        // A run that took fewer changes than the uncut one ends the sweep.
        if (!report.reached[0]) break;
        BOOST_REQUIRE(
          report.lives == 2 && one.pending.size() == 1
          && one.images.size() == 1);
        all.push_back({cut, one.pending[0], one.images[0], report.changes[1]});
    }
    co_return all;
}
} // namespace

// One life with everything a history can ask, a clean restart, and what the
// second life finds: nothing is cut. A sealed segment takes nothing more, a
// segment still written is not removed, and after the restart the sealed
// one is read through a session and the other is removed as what it is.
SEASTAR_TEST_CASE(engine_history_fake_a_life_is_read_back) {
    const auto report = co_await run_one(
      storage::testing::whole_life_history(), {});
    holds(report, "the uncut history");
    BOOST_CHECK_EQUAL(report.lives, 2U);
    BOOST_CHECK(!report.reached[0] && !report.reached[1]);
}

// What a run's ledger was told is enough to check what the run left, with
// nothing else of the run at hand: its lines are told to a ledger of
// another driver, which starts on the store, reads everything back and
// stops. Once with every answer heard, and once with the answers of the
// last two requests withheld, as their caller stood before they came. And
// told of one more request reported durable than the store ever took, the
// same check says it is lost.
SEASTAR_TEST_CASE(engine_history_fake_a_store_is_checked_from_what_was_told) {
    const engine_history history{
      {},
      {{history_op::create},
       {history_op::append, 0, 1},
       {history_op::settle},
       {history_op::create},
       {history_op::seal, 0},
       {history_op::checkpoint},
       {history_op::append, 0, 1},
       {history_op::settle}}};
    co_await with_reserved_store(
      [&](
        auto& files,
        auto& owner,
        auto& spec,
        auto& budget,
        auto& reserve,
        auto drive,
        auto& timer) -> seastar::future<> {
          using run_type = histories::history_run<
            simulation::monotonic_clock,
            std::remove_reference_t<decltype(files)>,
            std::remove_reference_t<decltype(owner)>,
            decltype(drive),
            std::remove_reference_t<decltype(timer)>,
            one_event>;
          const auto checked =
            [&](
              const std::vector<std::string>& lines,
              std::size_t answered) -> seastar::future<verdict> {
              run_type check{
                files,
                owner,
                spec,
                budget,
                reserve,
                drive,
                timer,
                one_event{&drive.events}};
              const auto report = co_await check.check(lines, answered);
              co_return report.verdict;
          };
          std::vector<std::string> told;
          // Where in `told` each acknowledgement came.
          std::vector<std::size_t> marks;
          {
              histories::history_hooks hooks;
              hooks.told = [&told](const std::string& text) {
                  told.push_back(text);
              };
              hooks.acknowledged = [&told, &marks] {
                  marks.push_back(told.size());
                  return seastar::make_ready_future<>();
              };
              run_type run{
                files,
                owner,
                spec,
                budget,
                reserve,
                drive,
                timer,
                one_event{&drive.events}};
              holds(
                co_await run.run(history, {}, std::move(hooks)),
                "the run that is told");
          }
          // Two creations, four requests, a seal and the stop.
          BOOST_REQUIRE_EQUAL(marks.size(), 8U);
          BOOST_REQUIRE_EQUAL(marks.back(), told.size());
          BOOST_CHECK(co_await checked(told, told.size()) == verdict::holds);
          // The seal was the fifth acknowledgement; the last two requests
          // were offered after it.
          BOOST_CHECK(co_await checked(told, marks[4]) == verdict::holds);
          // A fifth request, reported durable far past where the second
          // segment's file ends.
          auto more = told;
          more.emplace_back("offer 2 199 a5");
          more.emplace_back("accepted 4");
          more.emplace_back("durable 4 1099511627776 a5a5a5a5");
          BOOST_CHECK(co_await checked(more, more.size()) == verdict::lost);
      });
}

// A history's bytes are the history: the same run from its encoding, twice,
// to the same store.
SEASTAR_TEST_CASE(engine_history_fake_runs_from_its_bytes) {
    const auto bytes = storage::testing::encode_history(rolled().history);
    const auto decoded = storage::testing::decode_history(bytes);
    BOOST_REQUIRE(decoded == rolled().history);
    seen once, again;
    const auto first = co_await run_one(decoded, {}, &once);
    const auto second = co_await run_one(decoded, {}, &again);
    holds(first, "the decoded history");
    BOOST_CHECK_EQUAL(first.lives, second.lives);
    BOOST_CHECK_EQUAL(once.last, again.last);
}

// Three histories, each cut after the changes its device takes from where
// what it tests begins: with nothing unsynced surviving the crash, with
// everything surviving, and with about half of the data. After each crash
// the shard starts with the catalog its owner's knowledge makes, and every
// segment is found under one of its names and read back. A sample of the
// cuts by default; all of them, and from each history's first operation,
// when the whole is asked for.
SEASTAR_TEST_CASE(engine_history_fake_crash_at_every_cut) {
    for (const auto& one : {appended(), rolled(), removed()}) {
        const auto all = co_await sweep(one.history, one.from, 48);
        // Several changes lie between the first and the last.
        BOOST_CHECK_GT(all.size(), 4U);
        for (const auto kept :
             {history_survival::all, history_survival::data}) {
            auto history = one.history;
            history.header.survival = kept;
            co_await sweep(history, one.from, 6);
        }
    }
}

// The start after a crash is cut too. The crashes of a history's first life
// leave few different stores. For one cut that left each of them, the start
// that follows is cut after the changes it makes, crashed, started again and
// read back. A few stores and a sample of each start's cuts by default.
SEASTAR_TEST_CASE(engine_history_fake_restart_crash_once_an_image) {
    const auto one = appended();
    const auto all = co_await sweep(one.history, one.from, 48);
    std::map<std::uint64_t, taken> first;
    for (const auto& cut : all)
        first.try_emplace(cut.image, cut);
    BOOST_TEST_MESSAGE(
      all.size() << " cuts left " << first.size() << " stores");
    BOOST_REQUIRE_LE(first.size(), 256U);
    std::size_t stores = 0;
    for (const auto& [left, cut] : first) {
        static_cast<void>(left);
        if (!whole_sweeps() && stores++ == 6) break;
        const auto apart = whole_sweeps()
                             ? 1
                             : std::max<std::uint64_t>(1, (cut.after + 6) / 6);
        for (std::uint64_t again = 0; again <= cut.after; again += apart) {
            histories::history_cuts cuts;
            cuts.from = whole_sweeps() ? 0 : one.from;
            cuts.at[0] = cut.cut;
            cuts.at[1] = again;
            const auto report = co_await run_one(one.history, cuts);
            holds(
              report,
              "cut " + std::to_string(cut.cut) + " then "
                + std::to_string(again));
            if (!report.reached[1]) break;
            BOOST_REQUIRE_EQUAL(report.lives, 3U);
        }
    }
}

// Unsynced names survive one by one. A removal is cut after each of its
// changes; at each cut taken with names pending, every choice of which of
// them survive is run when they are at most six, and past that each alone
// and each alone missing.
SEASTAR_TEST_CASE(engine_history_fake_names_survive_one_by_one) {
    auto one = removed();
    one.history.header.survival = history_survival::names;
    // The removal alone, and its first cuts one after another: the ones
    // between its rename and the sync that follows are among them.
    const std::size_t from = one.history.operations.size() - 1;
    const auto none = co_await sweep(one.history, from, 16, true);
    std::size_t chosen = 0;
    for (const auto& cut : none) {
        const auto pending = std::min<std::size_t>(cut.pending, 56);
        if (pending == 0) continue;
        std::vector<std::uint64_t> masks;
        if (pending <= 6) {
            for (std::uint64_t mask = 1; mask != (1ULL << pending); ++mask)
                masks.push_back(mask);
        } else {
            const auto every = (1ULL << pending) - 1;
            masks.push_back(every);
            for (std::size_t bit = 0; bit != pending; ++bit) {
                masks.push_back(1ULL << bit);
                masks.push_back(every ^ (1ULL << bit));
            }
        }
        histories::history_cuts cuts;
        cuts.from = whole_sweeps() ? 0 : from;
        cuts.at[0] = cut.cut;
        for (const auto mask : masks) {
            if (!whole_sweeps() && chosen == 16) break;
            auto history = one.history;
            history.header.names = mask;
            const auto report = co_await run_one(history, cuts);
            holds(
              report,
              "cut " + std::to_string(cut.cut) + " with names "
                + std::to_string(mask));
            ++chosen;
        }
    }
    // Some cut of the removal is taken between a name and its sync.
    BOOST_CHECK_GT(chosen, 0U);
}

// The read-back reports a loss when there is one. A request is reported
// durable and the device crashes; before the next start the segment's data
// file is cut back to its header, as a device that lost the write would
// leave it. The start goes on, and the ledger says what is missing.
SEASTAR_TEST_CASE(engine_history_fake_a_lost_block_is_reported) {
    const engine_history history{
      {},
      {{history_op::create},
       {history_op::append, 0, 1},
       {history_op::settle},
       {history_op::crash}}};
    // Its header fills the file's first unit; the first block follows it.
    const auto report = co_await run_one(history, {}, nullptr, 8192);
    BOOST_CHECK(report.verdict == verdict::lost);
    BOOST_CHECK_EQUAL(report.lives, 2U);
}
