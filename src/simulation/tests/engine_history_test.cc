#include "src/base/units.h"
#include "src/simulation/environment.h"
#include "src/simulation/fake_file.h"
#include "src/simulation/fake_file_test_support.h"
#include "src/simulation/tests/engine_history_fixture.h"
#include "src/simulation/tests/local_store_fixture.h"
#include "src/simulation/virtual_time.h"
#include "src/storage/local_paths.h"
#include "src/storage/local_store_config.h"
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
#include <limits>
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
// A file whose end a device lost: the first segment's data file, or the
// WAL head, and the bytes left of it.
struct cut_back final {
    enum class file : std::uint8_t { segment, wal };
    file which{file::segment};
    std::uint64_t bytes{0};
};
// The one WAL file a store holds that never rotated.
canonical_fake_path
wal_head(fake_file_system& files, const local_device_spec& spec) {
    const auto buckets = take(
      take(local_paths::make(spec.root)).buckets(0, local_bucket_kind::wal));
    const auto under = take(
      fake_file_test_access::resolve(files, buckets.value()));
    std::optional<canonical_fake_path> found;
    for (const auto& bucket : take(fake_file_test_access::list(files, under))) {
        const auto directory = buckets.value() + "/" + bucket.name;
        const auto held = take(
          fake_file_test_access::resolve(files, directory));
        for (const auto& entry :
             take(fake_file_test_access::list(files, held))) {
            if (!entry.name.ends_with(".wal")) continue;
            BOOST_REQUIRE(!found);
            found = take(
              fake_file_test_access::resolve(
                files, directory + "/" + entry.name));
        }
    }
    BOOST_REQUIRE(found);
    return *found;
}
// One run of a history over a store of its own. The device is down from
// each crash to the start after it: what the crash left running is stopped
// and changes nothing. With `cut`, a file is cut back before each start
// that follows a crash, as a device that lost what was written past its
// bytes would leave it.
seastar::future<histories::history_report> run_one(
  const engine_history& history,
  histories::history_cuts cuts,
  seen* crashes = nullptr,
  std::optional<cut_back> cut = std::nullopt,
  histories::history_settings settings = {}) {
    simulation::testing::engine_history_fixture::around looked;
    looked.crashing = [crashes](fake_file_system& files, std::uint32_t) {
        if (crashes)
            crashes->pending.push_back(
              fake_file_test_access::pending_namespace_groups(files));
    };
    looked.starting =
      [crashes, cut](
        fake_file_system& files, const local_device_spec& spec, std::uint32_t) {
          if (crashes) crashes->images.push_back(image(files));
          if (!cut) return;
          const auto held
            = cut->which == cut_back::file::wal
                ? wal_head(files, spec)
                : take(
                    fake_file_test_access::resolve(
                      files,
                      storage::testing::local_append_contract::segment_path(
                        spec, 1)
                        .value()));
          BOOST_REQUIRE_GT(
            take(fake_file_test_access::visible_size(files, held)), cut->bytes);
          take(fake_file_test_access::truncate(files, held, cut->bytes));
      };
    looked.over = [crashes](fake_file_system& files) {
        if (crashes) crashes->last = image(files);
    };
    co_return co_await simulation::testing::engine_history_fixture::run(
      history, cuts, std::move(looked), false, std::move(settings));
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
// One segment's small, large and small request, each in its own place.
tested placed() { return {storage::testing::placed_history(), 1}; }
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
    for (const auto& one : {appended(), placed(), rolled(), removed()}) {
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
    for (const auto& one : {appended(), placed()}) {
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
            const auto apart = whole_sweeps() ? 1
                                              : std::max<std::uint64_t>(
                                                  1, (cut.after + 6) / 6);
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

// The read-back reports a loss when there is one, wherever a request was
// placed. A large request is placed in its segment alone and reported
// durable by its segment's barrier, and the device crashes; before the next
// start the segment's data file is cut back to its header, as a device that
// lost the write would leave it. Nothing else holds the request, so the
// start goes on and the ledger says what is missing.
SEASTAR_TEST_CASE(engine_history_fake_a_lost_segment_placed_block_is_reported) {
    const engine_history history{
      {},
      {{history_op::create},
       {history_op::append, 0, 1, 0x40},
       {history_op::settle},
       {history_op::crash}}};
    // Its header fills the file's first unit; the first block follows it.
    const auto report = co_await run_one(
      history, {}, nullptr, cut_back{cut_back::file::segment, 8192});
    BOOST_CHECK(report.verdict == verdict::lost);
    BOOST_CHECK_EQUAL(report.lives, 2U);
}

// Small requests are placed in the WAL, held in their segment's run and
// reported durable by their WAL barrier, and the crash loses the run before
// anything writes it. The start writes them from their WAL copies, so they
// are read back. With the WAL head cut back to its header as well, nothing
// holds them any more, and the ledger says they are missing.
SEASTAR_TEST_CASE(engine_history_fake_a_lost_wal_placed_block_is_reported) {
    const engine_history history{
      {},
      {{history_op::create},
       {history_op::append, 0, 1},
       {history_op::settle},
       {history_op::crash}}};
    holds(co_await run_one(history, {}), "the run lost at the crash");
    const auto report = co_await run_one(
      history, {}, nullptr, cut_back{cut_back::file::wal, 8192});
    BOOST_CHECK(report.verdict == verdict::lost);
    BOOST_CHECK_EQUAL(report.lives, 2U);
}

// Requests reported durable by their WAL barriers alone, in a WAL that moves
// to a new file every few of them, then a crash that loses every run their
// segments held. The start reads the WAL's files in order and writes each
// request into its segment where its receipt said before it serves: all of
// them are read back. Every request is large and placed in the WAL, held in
// runs of 256 KiB, and the WAL's files are 64 KiB. Then the same history cut
// after a sample of its changes.
SEASTAR_TEST_CASE(engine_history_fake_reported_across_wal_files) {
    const engine_history history{
      {},
      {{history_op::create},
       {history_op::append, 0, 3, 0x40},
       {history_op::settle},
       {history_op::append, 0, 3, 0x40},
       {history_op::settle},
       {history_op::crash}}};
    histories::history_settings settings;
    settings.engine.append.segment_placement_bytes = byte_count{
      std::numeric_limits<std::uint64_t>::max()};
    settings.engine.append.held_run_bytes = byte_count{256_KiB};
    settings.engine.wal.capacity_bytes = byte_count{64_KiB};
    const auto uncut = co_await run_one(
      history, {}, nullptr, std::nullopt, settings);
    holds(uncut, "the uncut history");
    BOOST_REQUIRE_EQUAL(uncut.lives, 2U);
    histories::history_cuts cuts;
    cuts.from = 1;
    // The same with no crash, for the changes there are to cut after.
    auto stopped = history;
    stopped.operations.pop_back();
    const auto changes = co_await run_one(
      stopped, cuts, nullptr, std::nullopt, settings);
    holds(changes, "the history with no crash");
    const auto apart = whole_sweeps() ? 1
                                      : std::max<std::uint64_t>(
                                          1, (changes.changes[0] + 12) / 12);
    for (std::uint64_t cut = 0; cut <= changes.changes[0]; cut += apart) {
        cuts.at[0] = cut;
        const auto report = co_await run_one(
          history, cuts, nullptr, std::nullopt, settings);
        holds(report, "cut " + std::to_string(cut));
        if (!report.reached[0]) break;
    }
}

// The WAL at its defaults, in files a few requests fill: each successor is
// made ahead under a temporary name and named at its rotation, and each file
// a checkpoint reclaims is kept as a spare that a later successor is made
// of. The history is cut after its changes from its first request, with
// nothing unsynced surviving the crash and with everything surviving, and
// every request reported durable is read back after each start: a body no
// rotation named is taken in as a spare and never as a WAL file, a named
// successor no control record pins is passed over as the highest file
// nothing references, and a spare whose rename was lost is back under its
// own name below the cutoff and taken in again. Some cuts are taken while a
// body waits under its temporary name and some while spares are kept. The
// first checkpoint's first cuts, one after another, are taken again with
// each choice of which pending names survive, as a removal's are. A sample
// of the cuts by default; all of them when the whole is asked for.
SEASTAR_TEST_CASE(engine_history_fake_wal_successors_cut) {
    const engine_history history{
      {},
      {{history_op::create},
       {history_op::append, 0, 3, 0x40},
       {history_op::settle},
       {history_op::checkpoint},
       {history_op::append, 0, 3, 0x40},
       {history_op::settle},
       {history_op::checkpoint},
       {history_op::append, 0, 3, 0x40},
       {history_op::settle}}};
    histories::history_settings settings;
    settings.engine.wal
      = storage::testing::wal_writer_contract::with_defaults();
    settings.engine.wal.capacity_bytes = byte_count{64_KiB};
    settings.engine.append.segment_placement_bytes = byte_count{
      std::numeric_limits<std::uint64_t>::max()};
    settings.engine.append.held_run_bytes = byte_count{256_KiB};
    // The names the WAL's directories hold as a crash is taken, and whether
    // a body is among them under a temporary name, and a spare.
    struct held final {
        std::uint64_t bodies{0}, spares{0};
        std::string names;
    };
    const auto look = [](fake_file_system& files, held& seen) {
        const auto paths = take(
          local_paths::make(take(runtime::file_path::make("/kwaque/store"))));
        const auto names = [&files](const std::string& directory) {
            std::vector<std::string> out;
            const auto at = fake_file_test_access::resolve(files, directory);
            if (!at) return out;
            const auto listed = fake_file_test_access::list(files, *at);
            if (!listed) return out;
            for (const auto& entry : *listed)
                out.push_back(entry.name);
            return out;
        };
        const auto root
          = take(paths.buckets(0, local_bucket_kind::wal)).value();
        seen.names.clear();
        bool body = false;
        for (const auto& bucket : names(root))
            for (const auto& name : names(root + "/" + bucket)) {
                body |= name.contains(".tmp-");
                seen.names += name + " ";
            }
        const auto spares = names(
          take(paths.spares(0, local_spare_kind::wal)).value());
        for (const auto& name : spares)
            seen.names += name + " ";
        seen.bodies += body ? 1 : 0;
        seen.spares += spares.empty() ? 0 : 1;
    };
    const auto cut_at =
      [&](const engine_history& run, histories::history_cuts cuts, held* seen) {
          simulation::testing::engine_history_fixture::around looked;
          looked.crashing = [&look,
                             seen](fake_file_system& files, std::uint32_t) {
              if (seen) look(files, *seen);
          };
          return simulation::testing::engine_history_fixture::run(
            run, cuts, std::move(looked), true, settings);
      };
    histories::history_cuts cuts;
    cuts.from = 1;
    const auto uncut = co_await cut_at(history, cuts, nullptr);
    holds(uncut, "the uncut history");
    for (const auto& [kept, sample] :
         {std::pair{history_survival::none, 48U},
          std::pair{history_survival::all, 12U}}) {
        auto survived = history;
        survived.header.survival = kept;
        const auto apart = whole_sweeps()
                             ? 1
                             : std::max<std::uint64_t>(
                                 1, (uncut.changes[0] + sample) / sample);
        held seen;
        for (std::uint64_t cut = 0; cut <= uncut.changes[0]; cut += apart) {
            cuts.at[0] = cut;
            const auto report = co_await cut_at(survived, cuts, &seen);
            holds(report, "cut " + std::to_string(cut));
            if (!report.reached[0]) break;
        }
        BOOST_CHECK_GT(seen.bodies, 0U);
        BOOST_CHECK_GT(seen.spares, 0U);
    }

    // From the first checkpoint, through the append after it, each cut
    // right after a change to the WAL's names while names are pending is
    // taken again with each choice of them surviving: a spare's rename before
    // its directory's sync, a body's making from a spare and its naming among
    // them. Every cut of the rest of the history too when the whole is asked
    // for.
    auto named = history;
    named.header.survival = history_survival::names;
    histories::history_cuts at_checkpoint;
    at_checkpoint.from = 3;
    std::size_t chosen = 0;
    std::string before;
    const std::uint64_t last = whole_sweeps()
                                 ? std::numeric_limits<std::uint64_t>::max()
                                 : 90;
    for (std::uint64_t cut = 0; cut != last; ++cut) {
        held seen;
        std::size_t pending = 0;
        at_checkpoint.at[0] = cut;
        {
            simulation::testing::engine_history_fixture::around looked;
            looked.crashing = [&look, &seen, &pending](
                                fake_file_system& files, std::uint32_t) {
                pending = fake_file_test_access::pending_namespace_groups(
                  files);
                look(files, seen);
            };
            const auto none
              = co_await simulation::testing::engine_history_fixture::run(
                named, at_checkpoint, std::move(looked), true, settings);
            holds(none, "cut " + std::to_string(cut) + " with no name");
            if (!none.reached[0]) break;
        }
        const bool changed = seen.names != std::exchange(before, seen.names);
        if (pending == 0 || !(changed || whole_sweeps())) continue;
        const auto names = std::min<std::size_t>(pending, 6);
        for (std::uint64_t mask = 1; mask < (1ULL << names); ++mask) {
            auto run = named;
            run.header.names = mask;
            holds(
              co_await cut_at(run, at_checkpoint, nullptr),
              "cut " + std::to_string(cut) + " with names "
                + std::to_string(mask));
            ++chosen;
        }
    }
    // Some cut of the checkpoint is taken between a name and its sync.
    BOOST_CHECK_GT(chosen, 0U);
}

// A segment is made, prepared then activated, and the device crashes before
// any group of it is durable: cut after each change its making takes. The
// start after each restarts it at the origin its owner lists, with nothing
// in it, and it is read back empty.
SEASTAR_TEST_CASE(engine_history_fake_crash_before_the_first_group) {
    const engine_history history{{}, {{history_op::create}}};
    histories::history_cuts cuts;
    const auto uncut = co_await run_one(history, cuts);
    holds(uncut, "the uncut history");
    for (std::uint64_t cut = 0; cut <= uncut.changes[0]; ++cut) {
        cuts.at[0] = cut;
        const auto report = co_await run_one(history, cuts);
        holds(report, "cut " + std::to_string(cut));
        if (!report.reached[0]) break;
        BOOST_REQUIRE_EQUAL(report.lives, 2U);
    }
    holds(
      co_await run_one({{}, {{history_op::create}, {history_op::crash}}}, {}),
      "a crash once it was made");
}

// A removal keeps its segment's data file as a spare while its device is
// healthy, as a sampler keeps it: the file moves from the deleting directory
// into the spare directory. The removal is cut after
// each of its changes, and the start after each cut is cut again after its
// own: no file is ever under two names, as each start finds the device and
// once the run is over, and every request is read back. A sample of the
// starts' cuts by default; all of them when the whole is asked for.
SEASTAR_TEST_CASE(engine_history_fake_removal_recycled_cut_twice) {
    const auto history = storage::testing::removed_history();
    const std::size_t removal = history.operations.size() - 1;
    const auto spares = [](fake_file_system& files) {
        const auto paths = take(
          local_paths::make(take(runtime::file_path::make("/kwaque/store"))));
        const auto at = fake_file_test_access::resolve(
          files, take(paths.spares(0, local_spare_kind::segment)).value());
        if (!at) return std::size_t{0};
        const auto listed = fake_file_test_access::list(files, *at);
        return listed ? listed->size() : std::size_t{0};
    };
    histories::history_settings settings;
    settings.sampled_each_operation = true;
    std::uint32_t most = 0;
    std::size_t kept = 0;
    simulation::testing::engine_history_fixture::around looked;
    looked.starting =
      [&most](
        fake_file_system& files, const local_device_spec&, std::uint32_t) {
          most = std::max(most, fake_file_test_access::most_file_links(files));
      };
    looked.over = [&most, &kept, &spares](fake_file_system& files) {
        most = std::max(most, fake_file_test_access::most_file_links(files));
        kept = spares(files);
    };
    histories::history_cuts cuts;
    cuts.from = removal;
    const auto uncut
      = co_await simulation::testing::engine_history_fixture::run(
        history, cuts, looked, false, settings);
    holds(uncut, "the uncut history");
    BOOST_REQUIRE_EQUAL(kept, 1U);
    for (std::uint64_t cut = 0; cut <= uncut.changes[0]; ++cut) {
        cuts.at[0] = cut;
        cuts.at[1] = std::nullopt;
        const auto once
          = co_await simulation::testing::engine_history_fixture::run(
            history, cuts, looked, false, settings);
        holds(once, "cut " + std::to_string(cut));
        if (!once.reached[0]) break;
        const auto apart = whole_sweeps() ? 1
                                          : std::max<std::uint64_t>(
                                              1, (once.changes[1] + 6) / 6);
        for (std::uint64_t again = 0; again <= once.changes[1];
             again += apart) {
            cuts.at[1] = again;
            const auto twice
              = co_await simulation::testing::engine_history_fixture::run(
                history, cuts, looked, false, settings);
            holds(
              twice,
              "cut " + std::to_string(cut) + " then " + std::to_string(again));
            if (!twice.reached[1]) break;
        }
    }
    BOOST_CHECK_LE(most, 1U);
}
