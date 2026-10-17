#pragma once

#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/codec/cooperative.h"
#include "src/codec/limits.h"
#include "src/model/position.h"
#include "src/runtime/file.h"
#include "src/runtime/file_error.h"
#include "src/runtime/file_flight.h"
#include "src/runtime/file_position.h"
#include "src/runtime/first_failure.h"
#include "src/runtime/time.h"
#include "src/storage/device_placement.h"
#include "src/storage/format_context.h"
#include "src/storage/local_append.h"
#include "src/storage/local_paths.h"
#include "src/storage/local_store_config.h"
#include "src/storage/page_cache.h"
#include "src/storage/segment_lifecycle.h"
#include "src/storage/segment_reader.h"
#include "src/storage/segment_writer.h"
#include "src/storage/segment_writer_state.h"
#include "src/storage/sparse_index.h"
#include "src/storage/sparse_index_owner.h"
#include "src/storage/tests/local_append_contract.h"
#include "src/storage/tests/local_store_contract.h"
#include "src/storage/tests/segment_reader_contract.h"
#include "src/storage/tests/segment_writer_contract.h"
#include "src/storage/tests/sparse_index_contract.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace kwaque::storage::testing::segment_lifecycle_contract {
using store_contract::require;
using store_contract::take;
namespace appends = local_append_contract;
namespace segments = segment_writer_contract;

// What may begin in each state, written out: one row a state, in the order
// seal, index publication, retry cut, deletion, close.
namespace rule {
struct row final {
    segment_life life;
    bool appends, readers;
    std::array<bool, 5> begins;
};
inline constexpr std::array<row, 7> table{{
  {segment_life::creating, false, false, {false, false, false, false, true}},
  {segment_life::writable, true, true, {true, false, true, false, true}},
  {segment_life::sealing, false, true, {false, false, false, false, true}},
  {segment_life::sealed, false, true, {false, true, false, true, true}},
  {segment_life::recovering, false, false, {false, false, false, true, true}},
  {segment_life::failed, false, false, {false, false, false, true, true}},
  {segment_life::deleting, false, false, {false, false, false, false, true}},
}};
inline constexpr std::array<segment_transition, 5> changes{
  segment_transition::seal,
  segment_transition::index_publication,
  segment_transition::retry_cut,
  segment_transition::deletion,
  segment_transition::close};
constexpr bool holds() noexcept {
    for (const auto& expected : table) {
        if (
          segment_appends(expected.life) != expected.appends
          || segment_admits_readers(expected.life) != expected.readers)
            return false;
        for (std::size_t change = 0; change != changes.size(); ++change)
            if (
              segment_admits(expected.life, changes[change])
              != expected.begins[change])
                return false;
    }
    return true;
}
static_assert(holds(), "the segment owner's rule is not the one written here");
} // namespace rule

// A table's limits under the tests' allocation profile.
inline segment_table_config configuration() {
    segment_table_config config;
    config.runs = segment_reader_contract::run_limits();
    config.metadata = store_contract::limits();
    config.scan = sparse_index_contract::scan_limits();
    return config;
}
inline segment_creation
creation(const local_device_spec& spec, std::uint64_t generation) {
    auto described = segments::descriptor();
    described.segment = appends::context(generation);
    auto writer = segments::configuration();
    writer.admission.working_bytes = byte_count{1_MiB};
    return {described, spec.owner.device(), writer};
}
inline segment_session_request sight(std::uint64_t visible) {
    return {.visible = model::range_logical_end{visible}};
}
inline bool
begins(const segment_read& read, std::initializer_list<std::uint64_t> at) {
    if (read.batches.size() != at.size()) return false;
    std::size_t i = 0;
    for (const auto expected : at)
        if (read.batches[i++].logical.begin().value() != expected) return false;
    return true;
}
inline bool ended(const segment_read& read) {
    return read.batches.empty() && read.stop == segment_read_stop::failed
           && read.error && read.error->code() == errc::closed;
}
template<typename Driver>
seastar::future<segment_read> read_from(
  segment_reader& session,
  segment_read_position from,
  byte_count limit,
  Driver drive) {
    seastar::abort_source asking;
    co_return co_await drive.lifecycle(
      session.read({from, limit, true}, asking));
}
template<typename Driver>
seastar::future<segment_read>
read_all(segment_reader& session, std::uint64_t from, Driver drive) {
    return read_from(
      session,
      {model::range_logical_end{from}, std::nullopt},
      byte_count{1_MiB},
      drive);
}
template<typename Append, typename Driver>
seastar::future<> append_one(
  Append& append,
  local_append_target target,
  std::uint64_t logical,
  std::uint64_t generation,
  workload_budget& budget,
  codec::cooperative_work& work,
  Driver drive) {
    static_cast<void>(appends::durable(
      co_await appends::settle(
        append.append(
          target,
          co_await appends::request(
            appends::child_wire(logical, generation), budget, work),
          work),
        drive)));
}

// A request its segment takes no more: decided before any effect.
template<typename Append, typename Driver>
seastar::future<local_append_acceptance> refused_one(
  Append& append,
  local_append_target target,
  std::uint64_t logical,
  std::uint64_t generation,
  workload_budget& budget,
  codec::cooperative_work& work,
  Driver drive) {
    auto stages = append.append(
      target,
      co_await appends::request(
        appends::child_wire(logical, generation), budget, work),
      work);
    const auto decided = co_await drive.lifecycle(std::move(stages.accepted));
    const auto outcome = co_await drive.lifecycle(std::move(stages.result));
    require(
      !decided.accepted() && outcome.status == local_append_status::not_written
        && !outcome.receipt,
      "a refused request had an effect");
    co_return decided;
}

// What a table told, in the order it told it.
struct told final {
    told() { reports.reserve(16); }
    [[nodiscard]] segment_roll_sink sink() {
        return [this](const segment_roll_report& report) noexcept {
            // Reserved, so that nothing here throws.
            if (reports.size() != reports.capacity()) reports.push_back(report);
        };
    }
    [[nodiscard]] std::size_t
    count(const segment_context& of, segment_roll_reason why) const noexcept {
        std::size_t found = 0;
        for (const auto& report : reports)
            if (report.segment == of && report.reasons.has(why)) ++found;
        return found;
    }
    std::vector<segment_roll_report> reports;
};
// Runs the environment in short steps until `done` holds, for a bounded
// while; and for a fixed number of steps, for what must not come.
template<
  runtime::monotonic_clock Clock,
  typename Driver,
  typename Timer,
  typename Done>
seastar::future<bool> eventually(Driver drive, Timer& timer, Done done) {
    constexpr runtime::monotonic_duration step{10'000'000};
    for (unsigned steps = 0; steps != 300 && !done(); ++steps)
        co_await appends::probe(
          drive, timer, Clock::now().checked_add(step).value());
    co_return done();
}
template<runtime::monotonic_clock Clock, typename Driver, typename Timer>
seastar::future<> pass(Driver drive, Timer& timer, unsigned steps) {
    constexpr runtime::monotonic_duration step{10'000'000};
    for (unsigned done = 0; done != steps; ++done)
        co_await appends::probe(
          drive, timer, Clock::now().checked_add(step).value());
}

// The append path of one shard with no segment yet, a cache, and a table of
// segment owners over them, closed while the append owner still runs.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer,
  typename Func>
seastar::future<> with_placed_table(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer,
  segment_table_config config,
  device_table* placement,
  Func body) {
    using table_type = segment_table<Backend, Owner, Clock>;
    co_await appends::with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 2, .segments = 0},
      [&](auto& append, auto&, auto&, auto&, auto& work, auto&, auto& ids)
        -> seastar::future<> {
          const std::array devices{spec};
          sparse_index_residency residency{4};
          sparse_index_rebuild_limit rebuilds;
          auto cache = take(
            page_cache::make(budget, {.capacity = byte_count{2_MiB}}));
          std::unique_ptr<table_type> table;
          runtime::first_failure failed;
          try {
              table = take(
                table_type::make(
                  {files,
                   owner,
                   devices,
                   0,
                   append,
                   nullptr,
                   ids,
                   *cache,
                   residency,
                   rebuilds,
                   budget,
                   budget,
                   budget,
                   placement},
                  config));
              co_await body(*table, append, work);
          } catch (...) {
              failed.observe(std::current_exception());
          }
          if (table) {
              try {
                  failed.observe(co_await drive.lifecycle(table->close()));
              } catch (...) {
                  failed.observe(std::current_exception());
              }
              table.reset();
          }
          try {
              co_await drive.lifecycle(cache->stop());
          } catch (...) {
              failed.observe(std::current_exception());
          }
          cache.reset();
          take(failed.outcome());
      });
}

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer,
  typename Func>
seastar::future<> with_segment_table(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer,
  segment_table_config config,
  Func body) {
    return with_placed_table<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      config,
      nullptr,
      std::move(body));
}

// One segment's life under its owner. It is created and found by its
// context alone; a second creation of it is refused. Its seal is one
// attempt: a second request joins it and a later one is given its outcome.
// While it seals its successor takes appends. The seal abandons nothing that
// was accepted, moves the session that was open, which reads on to the
// bound it took, and leaves the segment sealed with no attachment: its old
// target takes nothing, and a new session reads the sealed segment to its
// end. Closing the owner ends its sessions and takes it out of the table;
// what a session returned stays its holder's.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> sealed_under_readers(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await with_segment_table<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      configuration(),
      [&](auto& table, auto& append, auto& work) -> seastar::future<> {
          const auto a = appends::context(1);
          const auto b = appends::context(2);
          const auto first = segments::descriptor().logical_origin.value();
          require(first == 100, "the fixture's segments do not begin at 100");
          const auto target = take(
            co_await drive.lifecycle(table.create(creation(spec, 1), work)));
          {
              const auto twice = co_await drive.lifecycle(
                table.create(creation(spec, 1), work));
              require(
                table.life(a) == segment_life::writable
                  && table.target(a) == std::optional{target}
                  && table.size() == 1 && !twice
                  && twice.error().code() == errc::already_exists
                  && !table.life(b) && !table.target(b),
                "a created segment is not its context's one writable owner");
          }
          for (const std::uint64_t logical : {100U, 101U, 102U})
              co_await append_one(
                append, target, logical, 1, budget, work, drive);
          std::optional<segment_reader> tailing, late;
          std::optional<segment_read> early;
          runtime::first_failure failed;
          try {
              tailing.emplace(take(
                co_await drive.lifecycle(
                  table.open_session(a, sight(first + 64), budget, work))));
              early.emplace(
                co_await read_from(
                  *tailing,
                  {model::range_logical_end{first}, std::nullopt},
                  byte_count{1},
                  drive));
              require(
                begins(*early, {100}) && table.sessions(a) == 1,
                "a writable segment's session was not given its first batch");

              // Its successor exists and takes appends while it seals.
              const auto next = take(
                co_await drive.lifecycle(
                  table.create(creation(spec, 2), work)));
              auto sealing = table.seal(a);
              auto joined = table.seal(a);
              co_await append_one(append, next, 100, 2, budget, work, drive);
              const auto sealed = co_await drive.lifecycle(std::move(sealing));
              const auto same = co_await drive.lifecycle(std::move(joined));
              take(sealed.failure.outcome());
              const auto again = co_await drive.lifecycle(table.seal(a));
              require(
                sealed.extent && sealed.boundary && sealed.retry && sealed.index
                  && sealed.unresolved == 3 && same.boundary == sealed.boundary
                  && same.extent == sealed.extent && !again.failure.failed()
                  && again.boundary == sealed.boundary,
                "a seal did not publish the segment whole, or a repeated "
                "request was not given its outcome");
              require(
                table.life(a) == segment_life::sealed && !table.target(a)
                  && table.life(b) == segment_life::writable
                  && table.sessions(a) == 1 && !tailing->revoked()
                  && !table.failure(a).failed() && !table.debt(a).failed(),
                "a sealed segment kept its attachment, lost its session or "
                "owes something");
              {
                  // Its old place in the append owner names nothing.
                  auto stale = append.append(
                    target,
                    co_await appends::request(
                      appends::child_wire(103), budget, work),
                    work);
                  const auto refused = co_await drive.lifecycle(
                    std::move(stale.accepted));
                  static_cast<void>(
                    co_await drive.lifecycle(std::move(stale.result)));
                  require(
                    !refused.accepted() && refused.failure.error()
                      && refused.failure.error()->code() == errc::wrong_context,
                    "a sealed segment's old attachment took a request");
              }
              const auto rest = co_await read_from(
                *tailing, early->next, byte_count{1_MiB}, drive);
              late.emplace(take(
                co_await drive.lifecycle(
                  table.open_session(a, sight(first + 64), budget, work))));
              const auto whole = co_await read_all(*late, first, drive);
              require(
                begins(rest, {101, 102})
                  && rest.stop == segment_read_stop::short_of_visibility
                  && begins(whole, {100, 101, 102})
                  && whole.stop == segment_read_stop::end_of_segment
                  && table.sessions(a) == 2,
                "a session did not read across its segment's seal, or a new "
                "one did not read the sealed segment to its end");

              take(co_await drive.lifecycle(table.close_segment(a)));
              const auto after = co_await read_all(*tailing, first, drive);
              const auto unknown = co_await drive.lifecycle(table.seal(a));
              require(
                !table.life(a) && table.size() == 1 && tailing->revoked()
                  && late->revoked() && ended(after) && unknown.failure.error()
                  && unknown.failure.error()->code() == errc::not_found
                  && begins(*early, {100}),
                "a closed owner stayed in the table or left a session "
                "reading, or what was read changed");
          } catch (...) {
              failed.observe(std::current_exception());
          }
          early.reset();
          late.reset();
          tailing.reset();
          take(failed.outcome());
      });
}

// A sealed segment nothing reads has no owner, and one is made for a reader
// from what the segment's owner of segments supplies, which is what the
// seal left. Sealed owners are bounded: one more closes the one nothing
// uses, and is refused while every one of them is read. Without what its
// caller holds the segment to be it is not found, and with something else
// it is refused and leaves no owner behind.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> sealed_on_demand(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    auto bounded = configuration();
    bounded.maximum_open_sealed = 1;
    co_await with_segment_table<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      bounded,
      [&](auto& table, auto& append, auto& work) -> seastar::future<> {
          const auto a = appends::context(1);
          const auto b = appends::context(2);
          const auto first = segments::descriptor().logical_origin.value();
          std::optional<sealed_segment_expectation> kept_a, kept_b;
          {
              const auto target = take(
                co_await drive.lifecycle(
                  table.create(creation(spec, 1), work)));
              for (const std::uint64_t logical : {100U, 101U})
                  co_await append_one(
                    append, target, logical, 1, budget, work, drive);
              const auto sealed = co_await drive.lifecycle(table.seal(a));
              take(sealed.failure.outcome());
              kept_a = table.expectation(a);
              require(
                table.life(a) == segment_life::sealed && kept_a
                  && kept_a->extent == *sealed.extent
                  && kept_a->publication.boundary == sealed.boundary
                  && table.sessions(a) == 0 && table.open_sealed() == 1,
                "a sealed segment does not say what it was published as");
          }
          {
              const auto target = take(
                co_await drive.lifecycle(
                  table.create(creation(spec, 2), work)));
              co_await append_one(append, target, 100, 2, budget, work, drive);
              const auto sealed = co_await drive.lifecycle(table.seal(b));
              take(sealed.failure.outcome());
              kept_b = table.expectation(b);
              // The one nothing used made room for it.
              require(
                kept_b && !table.life(a)
                  && table.life(b) == segment_life::sealed
                  && table.open_sealed() == 1 && table.size() == 1,
                "the sealed owners were not kept to their bound");
          }
          std::optional<segment_reader> reader;
          runtime::first_failure failed;
          try {
              const auto unknown = co_await drive.lifecycle(
                table.open_session(a, sight(first + 64), budget, work));
              auto other = *kept_a;
              other.extent = kept_b->extent;
              const auto refused = co_await drive.lifecycle(
                table.open_session(a, sight(first + 64), budget, work, &other));
              require(
                !unknown && unknown.error().code() == errc::not_found
                  && !refused && refused.error().code() == errc::wrong_context
                  && !table.life(a) && table.size() == 0,
                "a sealed segment was opened without what its caller holds "
                "it to be, or a refused open left an owner");

              reader.emplace(take(
                co_await drive.lifecycle(table.open_session(
                  a, sight(first + 64), budget, work, &*kept_a))));
              const auto whole = co_await read_all(*reader, first, drive);
              require(
                begins(whole, {100, 101})
                  && whole.stop == segment_read_stop::end_of_segment
                  && table.life(a) == segment_life::sealed
                  && table.sessions(a) == 1 && table.open_sealed() == 1,
                "a sealed segment was not read through an owner made for "
                "its reader");
              // Every sealed owner is read: one more has no place.
              const auto full = co_await drive.lifecycle(table.open_session(
                b, sight(first + 64), budget, work, &*kept_b));
              require(
                !full && full.error().code() == errc::queue_full
                  && !table.life(b) && table.life(a) == segment_life::sealed
                  && !reader->revoked(),
                "a sealed owner in use was closed for another, or the "
                "bound was passed");
              // Its reader gone, it makes room.
              reader.reset();
              reader.emplace(take(
                co_await drive.lifecycle(table.open_session(
                  b, sight(first + 64), budget, work, &*kept_b))));
              const auto single = co_await read_all(*reader, first, drive);
              require(
                begins(single, {100})
                  && single.stop == segment_read_stop::end_of_segment
                  && !table.life(a) && table.life(b) == segment_life::sealed
                  && table.open_sealed() == 1,
                "the sealed owner nothing used did not make room");
          } catch (...) {
              failed.observe(std::current_exception());
          }
          reader.reset();
          take(failed.outcome());
      });
}

// The transitions of one segment run one at a time and each asks again when
// its turn comes. A cut of the completed-retry facts is durable before the
// seal that follows it, which then holds every fact; a cut asked for once
// the segment is sealed is refused. A segment that is told failed takes no
// seal, no cut and no new session, the session it had reads on, and its
// owner closes while the append owner still runs. No more writable segments
// are created than may be, and a table that closes refuses everything.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> one_transition_at_a_time(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    auto bounded = configuration();
    bounded.maximum_writable = 2;
    co_await with_segment_table<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      bounded,
      [&](auto& table, auto& append, auto& work) -> seastar::future<> {
          const auto a = appends::context(1);
          const auto b = appends::context(2);
          const auto first = segments::descriptor().logical_origin.value();
          const auto target = take(
            co_await drive.lifecycle(table.create(creation(spec, 1), work)));
          const auto other = take(
            co_await drive.lifecycle(table.create(creation(spec, 2), work)));
          {
              const auto third = co_await drive.lifecycle(
                table.create(creation(spec, 3), work));
              require(
                !third && third.error().code() == errc::queue_full
                  && table.size() == 2,
                "more writable segments were created than may be");
          }
          // One fact of two appends, cut, then sealed: the cut and the
          // seal are asked for together and run in that order.
          const auto written = co_await appends::settle(
            append.append(
              target,
              co_await appends::request(appends::child_wire(100), budget, work),
              work),
            drive);
          co_await append_one(append, target, 101, 1, budget, work, drive);
          const auto fact = appends::completed_fact(appends::durable(written));
          require(
            take(append.record_completed_retry(target, fact)).status
              == completed_retry_status::created,
            "a completed fact was not recorded");
          auto cutting = table.cut_retries(a, true);
          auto sealing = table.seal(a);
          const auto cut = take(co_await drive.lifecycle(std::move(cutting)));
          const auto sealed = co_await drive.lifecycle(std::move(sealing));
          take(sealed.failure.outcome());
          const auto late = co_await drive.lifecycle(
            table.cut_retries(a, true));
          require(
            cut.written == 1 && cut.snapshot && sealed.retry
              && sealed.unresolved == 1 && table.life(a) == segment_life::sealed
              && !late && late.error().code() == errc::closed,
            "a cut and a seal did not run one after the other, or a sealed "
            "segment was cut");

          // The other segment is told failed.
          co_await append_one(append, other, 100, 2, budget, work, drive);
          std::optional<segment_reader> reader;
          runtime::first_failure failed;
          try {
              reader.emplace(take(
                co_await drive.lifecycle(
                  table.open_session(b, sight(first + 64), budget, work))));
              runtime::first_failure why;
              why.observe(detail::path_error(errc::io_failure));
              table.fail(b, why);
              // A sealed segment is not failed by being told so.
              table.fail(a, why);
              const auto refused = co_await drive.lifecycle(table.seal(b));
              const auto uncut = co_await drive.lifecycle(
                table.cut_retries(b, true));
              const auto shut = co_await drive.lifecycle(
                table.open_session(b, sight(first + 64), budget, work));
              const auto read = co_await read_all(*reader, first, drive);
              require(
                table.life(b) == segment_life::failed && !table.target(b)
                  && table.failure(b).failed()
                  && table.life(a) == segment_life::sealed
                  && refused.failure.error()
                  && refused.failure.error()->code() == errc::io_failure
                  && !uncut && uncut.error().code() == errc::closed && !shut
                  && shut.error().code() == errc::closed,
                "a failed segment was sealed, cut or opened to a new "
                "session");
              require(
                begins(read, {100}) && !reader->revoked(),
                "the session of a segment that failed did not read on");
              // Its owner closes while the append owner runs: the
              // attachment drains and lets go.
              take(co_await drive.lifecycle(table.close_segment(b)));
              require(
                !table.life(b) && reader->revoked() && table.size() == 1,
                "a failed segment's owner did not close");
          } catch (...) {
              failed.observe(std::current_exception());
          }
          reader.reset();
          take(failed.outcome());

          // A closing table refuses everything.
          take(co_await drive.lifecycle(table.close()));
          const auto created = co_await drive.lifecycle(
            table.create(creation(spec, 3), work));
          const auto unsealed = co_await drive.lifecycle(table.seal(a));
          require(
            table.size() == 0 && !created
              && created.error().code() == errc::closed
              && unsealed.failure.failed(),
            "a closed table took a request");
      });
}

// What is left of a segment is what its next request finds. A segment says
// how many bytes of blocks it still takes after the footer that follows them
// and the root its seal writes: a block of exactly that size is taken, and
// with nothing left the next request is refused before any effect and stays
// refused. Whoever owns the segments is told why once, however often the
// refusal repeats, and once more for a device that is reported; a segment
// that is sealed is told of no more. Nothing is rolled: the full segment
// stays writable until it is sealed by request.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> roll_reported(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    told heard;
    co_await with_segment_table<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      configuration(),
      [&](auto& table, auto& append, auto& work) -> seastar::future<> {
          take(table.report_rolls(timer, heard.sink()));
          const auto a = appends::context(1);
          const auto b = appends::context(2);
          const auto size = segment_roll_reasons{segment_roll_reason::size};
          const auto target = take(
            co_await drive.lifecycle(table.create(creation(spec, 1), work)));
          const auto whole = take(table.left(a));
          require(
            whole.bytes.value() != 0 && whole.blocks != 0 && !whole.deadline,
            "an empty segment does not say what is left of it, or has an "
            "age");
          const auto written = co_await appends::settle(
            append.append(
              target,
              co_await appends::request(appends::child_wire(100), budget, work),
              work),
            drive);
          const auto& receipt = appends::durable(written);
          const auto block = receipt.block.records.bytes().size().value();
          require(
            receipt.segment.boundary().footer().has_value(),
            "a durable group has no footer");
          const auto footer
            = receipt.segment.boundary().footer()->size().value();
          const auto less = take(table.left(a));
          require(
            block != 0 && footer != 0
              && whole.bytes.value() - less.bytes.value() == block + footer
              && less.blocks == whole.blocks - 1 && less.deadline,
            "what is left did not shrink by the group that was taken, or "
            "an appended segment has no age");

          // A segment that holds two such groups and no more.
          auto small = creation(spec, 2);
          const auto room = 2 * block + footer;
          require(
            whole.bytes.value() > room,
            "the fixture's extent holds no two groups");
          small.descriptor.maximum_data_bytes = byte_count{
            small.descriptor.maximum_data_bytes.value()
            - (whole.bytes.value() - room)};
          const auto tight = take(
            co_await drive.lifecycle(table.create(small, work)));
          require(
            take(table.left(b)).bytes.value() == room,
            "a segment's extent is not what is left of it when empty");
          co_await append_one(append, tight, 100, 2, budget, work, drive);
          require(
            take(table.left(b)).bytes.value() == block,
            "what is left after one group is not one block");
          // An exact fit is taken.
          co_await append_one(append, tight, 101, 2, budget, work, drive);
          const auto none = take(table.left(b));
          require(
            none.bytes.value() == 0 && none.blocks == 0 && none.deadline,
            "a full segment says something is left of it");
          const auto first = co_await refused_one(
            append, tight, 102, 2, budget, work, drive);
          require(
            first.capacity == segment_capacity_decision::roll_required
              && !first.failure.failed(),
            "a request a full segment has no room for was not refused as "
            "roll-required");
          require(
            co_await eventually<Clock>(
              drive, timer, [&] { return !heard.reports.empty(); }),
            "nobody was told that a segment is full");
          const auto again = co_await refused_one(
            append, tight, 102, 2, budget, work, drive);
          co_await pass<Clock>(drive, timer, 3);
          require(
            again.capacity == segment_capacity_decision::roll_required
              && heard.reports.size() == 1 && heard.reports[0].segment == b
              && heard.reports[0].reasons == size
              && table.life(b) == segment_life::writable && table.target(b),
            "a full segment was told of twice or for another reason, or "
            "was rolled");

          // Its device is reported: each writable segment on it is told,
          // once.
          table.tell_device(spec.owner.device());
          table.tell_device(spec.owner.device());
          require(
            co_await eventually<Clock>(
              drive, timer, [&] { return heard.reports.size() >= 3; }),
            "the segments of a reported device were not told of");
          table.tell_device(spec.owner.device());
          co_await pass<Clock>(drive, timer, 3);
          require(
            heard.reports.size() == 3
              && heard.count(a, segment_roll_reason::device) == 1
              && heard.count(b, segment_roll_reason::device) == 1
              && heard.count(b, segment_roll_reason::size) == 1,
            "a reported device was told of twice for a segment");

          // Sealed by request: nothing more is told of it or said of its
          // room.
          const auto sealed = co_await drive.lifecycle(table.seal(b));
          take(sealed.failure.outcome());
          table.tell_device(spec.owner.device());
          co_await pass<Clock>(drive, timer, 3);
          const auto shut = table.left(b);
          const auto unknown = table.left(appends::context(3));
          require(
            heard.reports.size() == 3 && !shut
              && shut.error().code() == errc::closed && !unknown
              && unknown.error().code() == errc::not_found,
            "a sealed segment was told of, or says what is left of it");
      });
}

// An idle segment whose lifetime ends is told to whoever owns the segments,
// once, and is not sealed. A lifetime runs from the segment's first accepted
// group on the monotonic clock: an empty segment has none. The lifetimes are
// waited for in the order they end, whatever order they began in, so a
// segment with a short one is told while an older segment's long one still
// runs. A request to a segment past its lifetime is refused and nothing is
// told again; a segment sealed before its lifetime ends is never told.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> age_reported(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    told heard;
    co_await with_segment_table<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      configuration(),
      [&](auto& table, auto& append, auto& work) -> seastar::future<> {
          take(table.report_rolls(timer, heard.sink()));
          {
              const auto twice = table.report_rolls(timer, heard.sink());
              require(
                !twice && twice.error().code() == errc::wrong_context,
                "a table took a second receiver of its reports");
          }
          const auto a = appends::context(1);
          const auto b = appends::context(2);
          const auto d = appends::context(3);
          const auto age = segment_roll_reason::age;
          // The older segment's lifetime is long and begins first.
          const auto old = take(
            co_await drive.lifecycle(table.create(creation(spec, 1), work)));
          require(
            !take(table.left(a)).deadline && table.aging() == 0,
            "an empty segment has an age");
          co_await append_one(append, old, 100, 1, budget, work, drive);
          const auto late = take(table.left(a)).deadline;
          auto brief = creation(spec, 2);
          brief.descriptor.maximum_lifetime = runtime::monotonic_duration{
            150'000'000};
          const auto young = take(
            co_await drive.lifecycle(table.create(brief, work)));
          require(
            late && table.aging() == 1 && !take(table.left(b)).deadline,
            "a lifetime did not begin at the first accepted group");
          co_await append_one(append, young, 100, 2, budget, work, drive);
          const auto soon = take(table.left(b)).deadline;
          require(
            soon && *soon < *late && table.aging() == 2
              && (heard.count(b, age) == 0 || Clock::now() >= *soon),
            "a segment was told of before its lifetime ended");
          require(
            co_await eventually<Clock>(
              drive, timer, [&] { return heard.count(b, age) != 0; }),
            "nobody was told that an idle segment's lifetime ended");
          require(
            Clock::now() >= *soon && heard.reports.size() == 1
              && heard.reports[0].segment == b
              && heard.reports[0].reasons == segment_roll_reasons{age}
              && table.life(b) == segment_life::writable && table.target(b)
              && table.aging() == 1 && !table.age_failure().failed(),
            "an idle segment past its lifetime was told of early, twice or "
            "with another, or was sealed");
          // Told, not sealed: a request to it is refused for its age, and
          // that is not told again.
          const auto expired = co_await refused_one(
            append, young, 101, 2, budget, work, drive);
          co_await pass<Clock>(drive, timer, 3);
          require(
            expired.capacity == segment_capacity_decision::roll_required
              && !expired.failure.failed() && heard.reports.size() == 1,
            "a segment past its lifetime took a request, or was told of "
            "twice");
          take((co_await drive.lifecycle(table.seal(b))).failure.outcome());

          // Sealed before its lifetime ends: never told.
          auto passing = creation(spec, 3);
          passing.descriptor.maximum_lifetime = runtime::monotonic_duration{
            200'000'000};
          const auto third = take(
            co_await drive.lifecycle(table.create(passing, work)));
          co_await append_one(append, third, 100, 3, budget, work, drive);
          const auto ends = take(table.left(d)).deadline;
          require(
            ends && table.aging() == 2,
            "a third segment's lifetime did not begin");
          constexpr runtime::monotonic_duration margin{50'000'000};
          const bool early = Clock::now().checked_add(margin).value() < *ends;
          auto sealing = table.seal(d);
          take(
            (co_await drive.lifecycle(std::move(sealing))).failure.outcome());
          const auto past = ends->checked_add(margin).value();
          require(
            table.aging() == 1
              && co_await eventually<Clock>(
                drive, timer, [&] { return Clock::now() >= past; }),
            "a sealed segment kept its place among the lifetimes, or its "
            "lifetime never passed");
          require(
            (!early || heard.count(d, age) == 0) && heard.count(a, age) == 0
              && table.aging() == 1 && table.life(a) == segment_life::writable,
            "a segment sealed before its lifetime ended was told of, or "
            "one whose lifetime runs was");
      });
}

// Creating a segment and sealing one are apart. A successor is created while
// its predecessor takes appends, and both then take them; a predecessor is
// sealed by request while its successor takes appends, and the next segment
// is created meanwhile. Creations in progress are bounded apart from the
// writable segments and count among them, and a refused creation leaves
// nothing. A segment that was never used is sealed like any other, and its
// place among the writable is free again.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> created_ahead(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    auto bounded = configuration();
    bounded.maximum_writable = 2;
    bounded.maximum_creations = 1;
    {
        auto none = bounded;
        none.maximum_creations = 0;
        auto many = bounded;
        many.maximum_creations = maximum_concurrent_creations + 1;
        many.maximum_writable = maximum_concurrent_creations + 1;
        auto more = bounded;
        more.maximum_creations = bounded.maximum_writable + 1;
        const auto demand = bounded.handle_demand(4);
        auto crowd = bounded;
        crowd.maximum_writable = 64;
        require(
          bounded.validate() && !none.validate() && !many.validate()
            && !more.validate() && demand.segments == 2 && demand.creating == 1
            && demand.reserved == 4
            && validate_segment_handles(
              budget, creation(spec, 1).writer, bounded.handle_demand())
            && !validate_segment_handles(
              budget, creation(spec, 1).writer, crowd.handle_demand()),
          "the bound on creations is not checked, or a table's segments "
          "are not counted against their handle credits");
    }
    // A creation that runs beside appends has its own work account, which
    // is here until the table has closed.
    seastar::abort_source never;
    codec::cooperative_work apart{codec::limits::defaults(), never};
    co_await with_segment_table<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      bounded,
      [&](auto& table, auto& append, auto& work) -> seastar::future<> {
          const auto a = appends::context(1);
          const auto b = appends::context(2);
          const auto c = appends::context(3);
          // One creation at a time here: the second is refused while the
          // first runs, and leaves nothing.
          auto first = table.create(creation(spec, 1), work);
          const auto begun = table.life(a);
          const bool attached = table.target(a).has_value();
          const auto unsized = table.left(a);
          auto crowded = table.create(creation(spec, 2), apart);
          const auto over = co_await drive.lifecycle(std::move(crowded));
          const auto old = take(co_await drive.lifecycle(std::move(first)));
          require(
            begun == segment_life::creating && !attached && !unsized
              && unsized.error().code() == errc::closed && !over
              && over.error().code() == errc::queue_full && !table.life(b),
            "a segment in creation is appended to, or a creation over the "
            "bound was begun");

          // The successor is created while its predecessor takes appends.
          auto ahead = table.create(creation(spec, 2), apart);
          co_await append_one(append, old, 100, 1, budget, work, drive);
          const auto next = take(co_await drive.lifecycle(std::move(ahead)));
          co_await append_one(append, next, 100, 2, budget, work, drive);
          co_await append_one(append, old, 101, 1, budget, work, drive);
          {
              const auto third = co_await drive.lifecycle(
                table.create(creation(spec, 3), work));
              require(
                table.target(a) == std::optional{old}
                  && table.target(b) == std::optional{next} && !third
                  && third.error().code() == errc::queue_full
                  && table.size() == 2,
                "a segment and its successor do not both take appends, or "
                "more segments are writable than may be");
          }

          // The predecessor is sealed by request while its successor takes
          // appends; then the next one is created while that one does.
          auto sealing = table.seal(a);
          co_await append_one(append, next, 101, 2, budget, work, drive);
          take(
            (co_await drive.lifecycle(std::move(sealing))).failure.outcome());
          auto following = table.create(creation(spec, 3), apart);
          co_await append_one(append, next, 102, 2, budget, work, drive);
          const auto unused = take(
            co_await drive.lifecycle(std::move(following)));
          require(
            table.life(a) == segment_life::sealed
              && table.life(b) == segment_life::writable
              && table.target(c) == std::optional{unused}
              && !take(table.left(c)).deadline,
            "a seal or a creation held the other, or an unused segment "
            "has an age");

          // Never used, and sealed like any other.
          const auto sealed = co_await drive.lifecycle(table.seal(c));
          take(sealed.failure.outcome());
          const auto fourth = co_await drive.lifecycle(
            table.create(creation(spec, 4), work));
          require(
            sealed.extent && !sealed.index && sealed.unresolved == 0
              && table.life(c) == segment_life::sealed && !table.target(c)
              && fourth && table.size() == 4,
            "an unused segment was not sealed whole, or its place among "
            "the writable was not free again");
      });
}

// A watched device's operations in flight are kept in start order by the
// file system that owns them. An operation is in flight from the call that
// admits it until it ends: its start is a read meanwhile, and is what the
// device's stamp holds for a thread that is no shard's. An idle device has
// neither.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> device_watched(
  Backend& files,
  Owner&,
  const local_device_spec& spec,
  workload_budget&,
  Driver drive,
  Timer&) {
    // The file system writes through the stamp until it is destroyed, which
    // is after this case has returned: the stamp is the process's, as the
    // stamps a broker watches with are.
    static std::atomic<std::uint64_t> stamp{0};
    const runtime::file_flight_watch watching{
      []() noexcept { return Clock::now(); }, &stamp};
    take(files.watch_device(spec.root, watching));
    {
        const auto twice = files.watch_device(spec.root, watching);
        require(
          !twice && twice.error().code() == errc::already_exists
            && !files.oldest_operation(spec.root)
            && stamp.load() == runtime::no_file_operation_in_flight,
          "a device was watched twice, or an idle one has an operation in "
          "flight");
    }
    const auto path = take(
      runtime::file_path::make(spec.root.value() + "/watched"));
    auto file = take(
      co_await drive.lifecycle(files.open(
        path,
        {.access = runtime::file_access::read_write,
         .create = true,
         .truncate = true})));
    runtime::first_failure failed;
    try {
        require(
          !files.oldest_operation(spec.root)
            && stamp.load() == runtime::no_file_operation_in_flight,
          "an open that ended is still in flight");
        const std::string bytes(4096, 'k');
        auto data = take(
          kwaque::bytes::fragmented_buffer::copy_of(
            std::span<const char>{bytes}));
        const auto began = Clock::now();
        auto writing = file.write(runtime::file_position{}, std::move(data));
        const auto oldest = files.oldest_operation(spec.root);
        const auto shown = stamp.load();
        const auto written = co_await drive.lifecycle(std::move(writing));
        require(
          oldest && *oldest == began && shown == began.nanoseconds(),
          "an admitted write is not its device's oldest operation in "
          "flight, or its start is not what the stamp holds");
        take(written);
        require(
          !files.oldest_operation(spec.root)
            && stamp.load() == runtime::no_file_operation_in_flight,
          "a write that ended is still in flight");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        failed.observe(co_await drive.lifecycle(file.close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(failed.outcome());
}

// A new segment goes where the shard's device table places it, and only
// while the shard's share of the filesystem covers the segment's whole
// bound. With no sample no share is trusted; with too little the creation
// is refused for space and leaves nothing. A placed segment's bound is
// taken from the share at once, and what the shard still owes shrinks as a
// segment is written and ends when it is sealed. A device that fails takes
// its writable segments with it, their owner is told, they take no more
// appends, and what is sealed there stays.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> placed_by_share(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    using table_type = segment_table<Backend, Owner, Clock>;
    told heard;
    table_type* watched = nullptr;
    std::vector<device_report> changed;
    changed.reserve(8);
    const std::array devices{spec};
    const auto placement = take(
      device_table::make(
        devices, {}, [&](const device_report& report) noexcept {
            if (changed.size() != changed.capacity()) changed.push_back(report);
            if (watched) watched->device_changed(report);
        }));
    const auto device = spec.owner.device();
    const auto hand = [&](std::uint64_t share) {
        std::array<device_pool_share, device_table::maximum_devices> shares{};
        shares[0] = {true, false, byte_count{share}};
        placement->sampled(shares, Clock::now());
    };
    co_await with_placed_table<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      configuration(),
      placement.get(),
      [&](auto& table, auto& append, auto& work) -> seastar::future<> {
          watched = &table;
          take(table.report_rolls(timer, heard.sink()));
          const auto a = appends::context(1);
          const auto b = appends::context(2);
          const auto unnamed = [&](std::uint64_t generation) {
              auto made = creation(spec, generation);
              made.device.reset();
              return made;
          };
          const auto bound = take(
                               table_type::writer_type::disk_bound(
                                 unnamed(1).descriptor, unnamed(1).writer))
                               .value();
          require(
            bound > unnamed(1).descriptor.maximum_data_bytes.value(),
            "a segment's bound does not hold its extent and its retry "
            "bundle");
          {
              // No sample yet: no share is trusted.
              const auto early = co_await drive.lifecycle(
                table.create(unnamed(1), work));
              hand(bound - 1);
              const auto tight = co_await drive.lifecycle(
                table.create(unnamed(1), work));
              require(
                !early && early.error().code() == errc::unavailable && !tight
                  && tight.error().code() == errc::resource_exhausted
                  && table.size() == 0 && placement->writable(device) == 0,
                "a segment was created with no share to hold it, or a "
                "refused creation left something");
          }
          hand(2 * bound + 1);
          const auto first = take(
            co_await drive.lifecycle(table.create(unnamed(1), work)));
          require(
            table.life(a) == segment_life::writable
              && placement->writable(device) == 1
              && placement->share(device).value() == bound + 1,
            "a placed segment's bound was not taken from the share");
          const auto second = take(
            co_await drive.lifecycle(table.create(unnamed(2), work)));
          const auto third = co_await drive.lifecycle(
            table.create(unnamed(3), work));
          const auto owed = table.owed()[0].value();
          require(
            placement->writable(device) == 2
              && placement->share(device).value() == 1 && !third
              && third.error().code() == errc::resource_exhausted
              && table.size() == 2 && owed < 2 * bound
              && owed >= 2 * bound - 128_KiB,
            "the shard promised more than its share, or does not owe what "
            "its segments may still write");
          // What is owed shrinks as a segment is written, and ends when it
          // is sealed.
          co_await append_one(append, first, 100, 1, budget, work, drive);
          const auto less = table.owed()[0].value();
          take((co_await drive.lifecycle(table.seal(a))).failure.outcome());
          const auto one = table.owed()[0].value();
          require(
            less < owed && one < bound && one >= bound - 64_KiB
              && placement->writable(device) == 1,
            "what a shard owes did not follow what its segments wrote, or "
            "a sealed segment still counts");

          // The device fails: its writable segment fails, and its owner
          // is told why.
          placement->failed(device, runtime::file_failure_detail::device_io);
          require(
            co_await eventually<Clock>(
              drive,
              timer,
              [&] { return heard.count(b, segment_roll_reason::device) != 0; }),
            "the owner of a failed device's segment was not told");
          const auto after = co_await drive.lifecycle(
            table.create(unnamed(3), work));
          require(
            changed.size() == 1 && changed[0].state == device_state::failed
              && changed[0].cause == device_failure::io_error
              && table.life(b) == segment_life::failed && !table.target(b)
              && table.failure(b).error()
              && table.failure(b).error()->code() == errc::io_failure
              && table.life(a) == segment_life::sealed
              && placement->writable(device) == 0 && !after
              && after.error().code() == errc::unavailable
              && heard.count(b, segment_roll_reason::device) == 1
              && heard.count(a, segment_roll_reason::device) == 0,
            "a failed device kept a writable segment or took a new one, or "
            "what is sealed on it was touched");
          // The segment that failed with its device takes no more appends,
          // though its own writer has not failed.
          static_cast<void>(
            co_await refused_one(append, second, 100, 2, budget, work, drive));
      });
}

// What the shard knows of a segment for a retention walk, and whether an
// authorization to remove it names what the shard holds. A sealed segment
// nothing reads may go and says what it holds; one with a session, and one
// that is still appended to, may not. An authorization is accepted only
// for the extent a sealed segment was published as, and never while the
// segment is attached. A sealed segment the table has let go is known by
// what its caller holds it to be, as it is for a reader. Nothing is
// removed by any of it.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> kept_until_authorized(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await with_segment_table<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      configuration(),
      [&](auto& table, auto& append, auto& work) -> seastar::future<> {
          const auto a = appends::context(1);
          const auto b = appends::context(2);
          const auto c = appends::context(3);
          const auto first = segments::descriptor().logical_origin.value();
          const auto old = take(
            co_await drive.lifecycle(table.create(creation(spec, 1), work)));
          for (const std::uint64_t logical : {100U, 101U})
              co_await append_one(append, old, logical, 1, budget, work, drive);
          const auto sealed = co_await drive.lifecycle(table.seal(a));
          take(sealed.failure.outcome());
          const auto kept = table.expectation(a);
          const auto young = take(
            co_await drive.lifecycle(table.create(creation(spec, 2), work)));
          co_await append_one(append, young, 100, 2, budget, work, drive);
          require(
            sealed.extent && kept && kept->extent == *sealed.extent,
            "a sealed segment does not say what it was published as");
          const deletion_authorization exact{a, *sealed.extent};
          auto altered = *sealed.extent;
          altered.file_end = runtime::file_position{
            altered.file_end.value() + 1};
          {
              const auto whole = table.retained(a);
              const auto open = table.retained(b);
              const auto absent = table.retained(c);
              require(
                whole.need == retention_need::none
                  && whole.bytes.value() == sealed.extent->file_end.value()
                  && open.need == retention_need::unsealed
                  && open.bytes.value() != 0
                  && absent.need == retention_need::unknown,
                "the shard does not say what it holds of a segment, or "
                "whether it still needs it");
              require(
                table.check_deletion(exact) == authorization_verdict::authorized
                  && table.check_deletion({a, std::nullopt})
                       == authorization_verdict::sealed_here
                  && table.check_deletion({a, altered})
                       == authorization_verdict::wrong_identity
                  && table.check_deletion({b, std::nullopt})
                       == authorization_verdict::attached
                  && table.check_deletion({b, *sealed.extent})
                       == authorization_verdict::attached
                  && table.check_deletion({c, std::nullopt})
                       == authorization_verdict::unknown,
                "an authorization was accepted for what the shard does "
                "not hold, or for a segment that is still attached");
              // The walk over both: the sealed one may go, and it stops
              // at the one that is still appended to.
              const std::array supplied{
                retention_segment{
                  a, model::range_logical_end{first + 2}, std::nullopt},
                retention_segment{
                  b, model::range_logical_end{first + 3}, std::nullopt}};
              const std::array local{whole, open};
              const auto walked = select_retention(
                {supplied,
                 std::nullopt,
                 byte_count{},
                 model::range_logical_end{first + 64}},
                local);
              require(
                walked.count == 1 && walked.bytes == whole.bytes
                  && walked.stop == retention_stop::needed
                  && walked.need == retention_need::unsealed
                  && table.life(a) == segment_life::sealed
                  && table.life(b) == segment_life::writable,
                "a walk selected a segment that is appended to, or "
                "changed something");
          }
          std::optional<segment_reader> reader;
          runtime::first_failure failed;
          try {
              reader.emplace(take(
                co_await drive.lifecycle(
                  table.open_session(a, sight(first + 64), budget, work))));
              require(
                table.retained(a).need == retention_need::read
                  && table.check_deletion(exact)
                       == authorization_verdict::authorized,
                "a segment with a session is not needed here, or a reader "
                "decides what its owner authorized");
          } catch (...) {
              failed.observe(std::current_exception());
          }
          reader.reset();
          take(failed.outcome());
          require(
            table.retained(a).need == retention_need::none,
            "a segment whose session ended is still needed");
          // Let go by the table, it is known by what its caller holds.
          take(co_await drive.lifecycle(table.close_segment(a)));
          const auto cold = table.retained(a, &*kept);
          require(
            !table.life(a) && table.retained(a).need == retention_need::unknown
              && cold.need == retention_need::none
              && cold.bytes.value() == sealed.extent->file_end.value()
              && table.retained(c, &*kept).need == retention_need::unknown
              && table.check_deletion(exact) == authorization_verdict::unknown
              && table.check_deletion(exact, &*kept)
                   == authorization_verdict::authorized
              && table.check_deletion({a, altered}, &*kept)
                   == authorization_verdict::wrong_identity
              && table.check_deletion({c, *sealed.extent}, &*kept)
                   == authorization_verdict::unknown
              && !table.life(a),
            "a sealed segment the table let go is not known by what its "
            "caller holds it to be, or checking it made an owner");
      });
}

// A sealed segment is removed when its owner of segments authorizes exactly
// what the shard holds, and for nothing else. From that request on it takes
// no session, and the one it had is ended: its next read is refused, and
// what it was given stays its holder's. The directory takes its deleting
// name, its files go and then the directory, and the table lets its owner
// go: nothing is left under either name. The same request made again, for a
// segment the table has no owner for, finds it under neither name on any
// device and says again that it is gone, with or without what its caller
// holds it to be.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> removed_when_authorized(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await with_segment_table<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      configuration(),
      [&](auto& table, auto& append, auto& work) -> seastar::future<> {
          const auto a = appends::context(1);
          const auto first = segments::descriptor().logical_origin.value();
          const auto target = take(
            co_await drive.lifecycle(table.create(creation(spec, 1), work)));
          for (const std::uint64_t logical : {100U, 101U})
              co_await append_one(
                append, target, logical, 1, budget, work, drive);
          const auto sealed = co_await drive.lifecycle(table.seal(a));
          take(sealed.failure.outcome());
          const auto kept = table.expectation(a);
          require(
            sealed.extent && kept,
            "a sealed segment does not say what it was published as");
          const deletion_authorization exact{a, *sealed.extent};
          auto altered = *sealed.extent;
          altered.file_end = runtime::file_position{
            altered.file_end.value() + 1};
          const auto paths = take(local_paths::make(spec.root));
          const local_segment_name name{a.segment(), a.generation()};
          const auto live = take(paths.segment(0, name));
          const auto marked = take(paths.deleting(0, name));
          const auto there = [&](const runtime::file_path& path) {
              return drive.lifecycle(files.exists(path));
          };

          std::optional<segment_reader> reader;
          std::optional<segment_read> early;
          runtime::first_failure failed;
          try {
              reader.emplace(take(
                co_await drive.lifecycle(
                  table.open_session(a, sight(first + 64), budget, work))));
              early.emplace(
                co_await read_from(
                  *reader,
                  {model::range_logical_end{first}, std::nullopt},
                  byte_count{1},
                  drive));
              require(
                begins(*early, {100}) && table.sessions(a) == 1,
                "a sealed segment's session was not given its first batch");

              // Nothing but what the shard holds removes it.
              const auto wrong = co_await drive.lifecycle(
                table.remove({a, altered}));
              const auto unsealed = co_await drive.lifecycle(
                table.remove({a, std::nullopt}));
              require(
                wrong.verdict == authorization_verdict::wrong_identity
                  && !wrong.gone
                  && unsealed.verdict == authorization_verdict::sealed_here
                  && !unsealed.gone && table.life(a) == segment_life::sealed
                  && !reader->revoked() && take(co_await there(live))
                  && !take(co_await there(marked)),
                "a segment was touched for an authorization that does not "
                "name what the shard holds");

              const auto gone = co_await drive.lifecycle(table.remove(exact));
              take(gone.failure.outcome());
              require(
                gone.verdict == authorization_verdict::authorized && gone.gone
                  && !gone.settling && gone.removed >= 3 && gone.foreign == 0
                  && !gone.debt.failed() && !table.life(a),
                "an authorized removal did not take the segment away, or "
                "left its owner in the table");
              require(
                !take(co_await there(live)) && !take(co_await there(marked)),
                "a removed segment left a name behind");
              // Its reader was ended, and keeps what it was given.
              const auto after = co_await read_all(*reader, first, drive);
              require(
                reader->revoked() && ended(after) && begins(*early, {100})
                  && early->batches.front().bytes.size().value() != 0,
                "a removed segment's session reads on, or lost what it "
                "had returned");
          } catch (...) {
              failed.observe(std::current_exception());
          }
          early.reset();
          reader.reset();
          take(failed.outcome());

          const auto again = co_await drive.lifecycle(
            table.remove(exact, &*kept));
          const auto bare = co_await drive.lifecycle(table.remove(exact));
          require(
            again.verdict == authorization_verdict::repeated && again.gone
              && again.removed == 0 && !again.failure.failed()
              && bare.verdict == authorization_verdict::repeated && bare.gone
              && bare.removed == 0 && !bare.failure.failed() && !table.life(a),
            "a removal asked for again did not find the segment gone, or "
            "left an owner for it");
      });
}

// A segment that failed is removed as what it is: not sealed, and to be
// discarded. The removal lets go of its attachment to the append owner
// itself, closes its writer and takes its files away; its old place in the
// append owner then names nothing. While the removal has not been asked for
// the segment is kept, failed.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> failed_segment_discarded(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await with_segment_table<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      configuration(),
      [&](auto& table, auto& append, auto& work) -> seastar::future<> {
          const auto a = appends::context(1);
          const auto target = take(
            co_await drive.lifecycle(table.create(creation(spec, 1), work)));
          co_await append_one(append, target, 100, 1, budget, work, drive);
          const auto paths = take(local_paths::make(spec.root));
          const local_segment_name name{a.segment(), a.generation()};
          const auto live = take(paths.segment(0, name));
          const auto marked = take(paths.deleting(0, name));
          const auto there = [&](const runtime::file_path& path) {
              return drive.lifecycle(files.exists(path));
          };
          require(
            table.check_deletion({a, std::nullopt})
              == authorization_verdict::attached,
            "a segment that is appended to may be removed");

          runtime::first_failure why;
          why.observe(detail::path_error(errc::io_failure));
          table.fail(a, why);
          require(
            table.life(a) == segment_life::failed && !table.target(a)
              && table.check_deletion({a, std::nullopt})
                   == authorization_verdict::authorized
              && take(co_await there(live)),
            "a failed segment is not kept until its removal is asked "
            "for, or may not be removed as what it is");

          const auto gone = co_await drive.lifecycle(
            table.remove({a, std::nullopt}));
          take(gone.failure.outcome());
          require(
            gone.verdict == authorization_verdict::authorized && gone.gone
              && !gone.settling && gone.removed >= 3 && gone.foreign == 0
              && !table.life(a) && !take(co_await there(live))
              && !take(co_await there(marked)),
            "a failed segment was not discarded, or left a name behind");
          {
              // Its old place in the append owner names nothing.
              auto stale = append.append(
                target,
                co_await appends::request(
                  appends::child_wire(101), budget, work),
                work);
              const auto refused = co_await drive.lifecycle(
                std::move(stale.accepted));
              static_cast<void>(
                co_await drive.lifecycle(std::move(stale.result)));
              require(
                !refused.accepted() && refused.failure.error()
                  && refused.failure.error()->code() == errc::wrong_context,
                "a removed segment's old attachment took a request");
          }
      });
}

// A removal that a restart found goes on from wherever it stopped. A
// directory already under its deleting name is removed though its owner of
// segments still lists the segment as retained: the mark decides. One its
// owner marks expiring, found whole under its own name, is marked and then
// removed. An authorization for another segment, or a device the shard does
// not store data on, starts nothing.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> removal_resumed(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await with_segment_table<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      configuration(),
      [&](auto& table, auto& append, auto& work) -> seastar::future<> {
          const auto a = appends::context(1);
          const auto b = appends::context(2);
          const auto c = appends::context(3);
          const auto paths = take(local_paths::make(spec.root));
          const auto there = [&](const runtime::file_path& path) {
              return drive.lifecycle(files.exists(path));
          };
          const auto named = [&](const segment_context& of, bool deleting) {
              const local_segment_name name{of.segment(), of.generation()};
              return take(
                deleting ? paths.deleting(0, name) : paths.segment(0, name));
          };

          // Sealed, let go by the table, and then as a crash after the
          // mark left it.
          const auto first = take(
            co_await drive.lifecycle(table.create(creation(spec, 1), work)));
          for (const std::uint64_t logical : {100U, 101U})
              co_await append_one(
                append, first, logical, 1, budget, work, drive);
          take((co_await drive.lifecycle(table.seal(a))).failure.outcome());
          take(co_await drive.lifecycle(table.close_segment(a)));
          take(
            co_await drive.lifecycle(files.rename(
              named(a, false),
              named(a, true),
              runtime::file_rename_policy::no_replace)));
          const auto marked = co_await drive.lifecycle(
            table.resume_removal(a, spec.owner.device(), std::nullopt));
          take(marked.failure.outcome());
          require(
            marked.verdict == authorization_verdict::repeated && marked.gone
              && marked.removed >= 3 && marked.foreign == 0 && !table.life(a)
              && !take(co_await there(named(a, false)))
              && !take(co_await there(named(a, true))),
            "a removal found under its deleting name did not go on");

          // Not sealed, closed, whole under its own name, and expiring.
          const auto second = take(
            co_await drive.lifecycle(table.create(creation(spec, 2), work)));
          co_await append_one(append, second, 100, 2, budget, work, drive);
          take(co_await drive.lifecycle(table.close_segment(b)));
          require(
            !table.life(b) && take(co_await there(named(b, false))),
            "a closed segment left the device");
          const auto other = co_await drive.lifecycle(table.resume_removal(
            b, spec.owner.device(), deletion_authorization{a, std::nullopt}));
          const auto nowhere = co_await drive.lifecycle(table.resume_removal(
            c, device_store_id{}, deletion_authorization{c, std::nullopt}));
          require(
            other.verdict == authorization_verdict::wrong_context && !other.gone
              && !nowhere.gone && nowhere.failure.error()
              && nowhere.failure.error()->code() == errc::wrong_context
              && !table.life(b) && !table.life(c)
              && take(co_await there(named(b, false))),
            "a removal went on for another segment's authorization, or "
            "on a device the shard stores nothing on");
          const auto expiring = co_await drive.lifecycle(table.resume_removal(
            b, spec.owner.device(), deletion_authorization{b, std::nullopt}));
          take(expiring.failure.outcome());
          require(
            expiring.verdict == authorization_verdict::authorized
              && expiring.gone && expiring.removed >= 3 && expiring.foreign == 0
              && !table.life(b) && !take(co_await there(named(b, false)))
              && !take(co_await there(named(b, true))),
            "an expiring segment found whole was not marked and removed");

          // Asked of the table itself, for a segment it has no owner for:
          // the name on the device decides, as it does at a restart. Whole
          // under its own name and not sealed, the shard does not know it
          // and nothing is done; under its deleting name its removal goes
          // on, and leaves no owner behind.
          const auto third = take(
            co_await drive.lifecycle(table.create(creation(spec, 3), work)));
          co_await append_one(append, third, 100, 3, budget, work, drive);
          take(co_await drive.lifecycle(table.close_segment(c)));
          const deletion_authorization discard{c, std::nullopt};
          const auto whole = co_await drive.lifecycle(table.remove(discard));
          require(
            whole.verdict == authorization_verdict::unknown && !whole.gone
              && !whole.failure.failed() && !table.life(c)
              && take(co_await there(named(c, false))),
            "a segment the shard does not know was removed on a word that "
            "nothing held against the device");
          take(
            co_await drive.lifecycle(files.rename(
              named(c, false),
              named(c, true),
              runtime::file_rename_policy::no_replace)));
          const auto found = co_await drive.lifecycle(table.remove(discard));
          take(found.failure.outcome());
          require(
            found.verdict == authorization_verdict::repeated && found.gone
              && found.removed >= 3 && found.foreign == 0 && !table.life(c)
              && !take(co_await there(named(c, false)))
              && !take(co_await there(named(c, true))),
            "a removal asked for, of a segment under its deleting name, "
            "did not go on");
      });
}

// A removal against the transitions it may meet. While its segment seals it
// is refused whatever the authorization says, and nothing is touched;
// sealed, it is refused while the segment's close runs, and nothing is half
// done; closed, the segment is removed by what its caller holds it to be. A
// close asked for while a segment is being created takes effect once the
// creation is done: the segment is out of the table, and its place in the
// append owner takes nothing. One asked for while a creation runs that then
// makes nothing ends too, and nothing is left of the segment.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> removal_against_transitions(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await with_segment_table<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      configuration(),
      [&](auto& table, auto& append, auto& work) -> seastar::future<> {
          const auto a = appends::context(1);
          const auto b = appends::context(2);
          const auto paths = take(local_paths::make(spec.root));
          const local_segment_name name{a.segment(), a.generation()};
          const auto live = take(paths.segment(0, name));
          const auto marked = take(paths.deleting(0, name));
          const auto there = [&](const runtime::file_path& path) {
              return drive.lifecycle(files.exists(path));
          };
          const auto target = take(
            co_await drive.lifecycle(table.create(creation(spec, 1), work)));
          for (const std::uint64_t logical : {100U, 101U})
              co_await append_one(
                append, target, logical, 1, budget, work, drive);

          // Against its seal.
          auto sealing = table.seal(a);
          const auto early = co_await drive.lifecycle(
            table.remove({a, std::nullopt}));
          const auto sealed = co_await drive.lifecycle(std::move(sealing));
          take(sealed.failure.outcome());
          const auto kept = table.expectation(a);
          require(
            (early.verdict == authorization_verdict::attached
             || early.verdict == authorization_verdict::sealed_here)
              && !early.gone && !early.failure.failed() && sealed.extent && kept
              && table.life(a) == segment_life::sealed
              && take(co_await there(live)) && !take(co_await there(marked)),
            "a removal went on while its segment sealed, or the seal did "
            "not finish beside it");
          const deletion_authorization exact{a, *sealed.extent};

          // Against its close.
          auto closing = table.close_segment(a);
          const auto during = co_await drive.lifecycle(table.remove(exact));
          take(co_await drive.lifecycle(std::move(closing)));
          require(
            !during.gone
              && (during.verdict == authorization_verdict::unknown
                  || (during.failure.error()
                      && during.failure.error()->code() == errc::closed))
              && !table.life(a) && take(co_await there(live))
              && !take(co_await there(marked)),
            "a removal went on while its segment closed");
          const auto gone = co_await drive.lifecycle(
            table.remove(exact, &*kept));
          take(gone.failure.outcome());
          require(
            gone.verdict == authorization_verdict::authorized && gone.gone
              && !take(co_await there(live)) && !take(co_await there(marked)),
            "a closed segment was not removed by what its caller holds");

          // A close that overtakes a creation.
          auto creating = table.create(creation(spec, 2), work);
          auto overtaking = table.close_segment(b);
          const auto made = co_await drive.lifecycle(std::move(creating));
          const auto closed = co_await drive.lifecycle(std::move(overtaking));
          require(
            made.has_value() && closed.has_value() && !table.life(b),
            "a close asked for during a creation did not take the segment "
            "out once it was made");
          {
              auto stale = append.append(
                *made,
                co_await appends::request(
                  appends::child_wire(100, 2), budget, work),
                work);
              const auto refused = co_await drive.lifecycle(
                std::move(stale.accepted));
              static_cast<void>(
                co_await drive.lifecycle(std::move(stale.result)));
              require(
                !refused.accepted() && refused.failure.error()
                  && refused.failure.error()->code() == errc::wrong_context,
                "a segment closed during its creation took a request");
          }

          // A close that overtakes a creation that makes nothing. The
          // creation is refused before its writer exists; whichever of the
          // two ends first, nothing is left of the segment and the close
          // ends, with the owner or with none to find.
          const auto c = appends::context(3);
          auto unmade = creation(spec, 3);
          unmade.descriptor.layout = local_layout_kind::rewrite;
          auto failing = table.create(std::move(unmade), work);
          auto after = table.close_segment(c);
          const auto none = co_await drive.lifecycle(std::move(failing));
          const auto ended = co_await drive.lifecycle(std::move(after));
          require(
            !none && none.error().code() == errc::invalid_argument
              && (ended.has_value() || ended.error().code() == errc::not_found)
              && !table.life(c) && table.size() == 0,
            "a creation that made nothing left an owner, or took it from "
            "under the close that waited for it");
          // The same request may be made again, and makes the segment.
          const auto remade = co_await drive.lifecycle(
            table.create(creation(spec, 3), work));
          require(
            remade.has_value() && table.life(c) == segment_life::writable,
            "a segment whose creation made nothing cannot be created");
      });
}

// A reader does not hold a removal. A session whose read is in flight when
// its segment's removal is accepted is ended: that read returns, with whole
// batches or with none, the session's next read is refused, and the removal
// finishes while the session still exists. The removal waits for that read
// and for nothing else of the reader.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> revoked_in_flight(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await with_segment_table<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      configuration(),
      [&](auto& table, auto& append, auto& work) -> seastar::future<> {
          const auto a = appends::context(1);
          const auto first = segments::descriptor().logical_origin.value();
          const auto paths = take(local_paths::make(spec.root));
          const local_segment_name name{a.segment(), a.generation()};
          const auto live = take(paths.segment(0, name));
          const auto marked = take(paths.deleting(0, name));
          const auto there = [&](const runtime::file_path& path) {
              return drive.lifecycle(files.exists(path));
          };
          const auto target = take(
            co_await drive.lifecycle(table.create(creation(spec, 1), work)));
          for (const std::uint64_t logical : {100U, 101U})
              co_await append_one(
                append, target, logical, 1, budget, work, drive);
          const auto sealed = co_await drive.lifecycle(table.seal(a));
          take(sealed.failure.outcome());
          const auto kept = table.expectation(a);
          require(
            sealed.extent && kept,
            "a sealed segment does not say what it was published as");
          // Let go by the table: nothing of it is cached or open, so the
          // session's first read has to go to the device.
          take(co_await drive.lifecycle(table.close_segment(a)));

          std::optional<segment_reader> reader;
          std::optional<seastar::future<segment_read>> reading;
          seastar::abort_source asking;
          runtime::first_failure failed;
          try {
              reader.emplace(take(
                co_await drive.lifecycle(table.open_session(
                  a, sight(first + 64), budget, work, &*kept))));
              reading.emplace(reader->read(
                {{model::range_logical_end{first}, std::nullopt},
                 byte_count{1_MiB},
                 true},
                asking));
              const auto gone = co_await drive.lifecycle(
                table.remove({a, *sealed.extent}));
              const auto read = co_await drive.lifecycle(std::move(*reading));
              reading.reset();
              take(gone.failure.outcome());
              require(
                gone.verdict == authorization_verdict::authorized && gone.gone
                  && reader->revoked() && !table.life(a)
                  && !take(co_await there(live))
                  && !take(co_await there(marked)),
                "a session held its segment's removal, or outlived it "
                "unrevoked");
              const bool none = read.batches.empty()
                                && read.stop == segment_read_stop::failed
                                && read.error
                                && read.error->code() == errc::closed;
              const bool whole = !read.batches.empty()
                                 && read.batches.front().logical.begin().value()
                                      == 100
                                 && read.batches.front().bytes.size().value()
                                      != 0;
              require(
                none || whole,
                "a read that was in flight at its segment's removal did "
                "not end with whole batches or with none");
              const auto after = co_await read_all(*reader, first, drive);
              require(
                ended(after), "a session reads on after its segment is gone");
          } catch (...) {
              failed.observe(std::current_exception());
          }
          if (reading) {
              try {
                  static_cast<void>(
                    co_await drive.lifecycle(std::move(*reading)));
              } catch (...) {
                  failed.observe(std::current_exception());
              }
          }
          reader.reset();
          take(failed.outcome());
      });
}

// A removal whose call fails keeps its cause and its place, and loses
// nothing. With `faulted`, the unlink of the segment's data file fails
// once: the request says why and what is owed, the segment stays under its
// deleting name with that file still there, a new segment is created and
// appended to beside it, and the same request then finishes the removal.
// Without it the removal finishes at once.
//
// Where the place is kept depends on who had the segment. With `held` the
// table has its owner open when the removal is asked for: the owner stays,
// deleting, says what it owes, and answers the same authorization as
// repeated and another as not what it holds. Without it the segment was let
// go, and the removal's owner leaves with the attempt: the name on the
// device is the place, the table knows nothing of the segment meanwhile,
// and the same request finds it by that name. `found` is handed the data
// file's path before the removal, for a backend that injects the failure by
// the file.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer,
  typename Found>
seastar::future<> removal_retried(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer,
  bool faulted,
  bool held,
  Found found) {
    co_await with_segment_table<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      configuration(),
      [&](auto& table, auto& append, auto& work) -> seastar::future<> {
          const auto a = appends::context(1);
          const auto paths = take(local_paths::make(spec.root));
          const local_segment_name name{a.segment(), a.generation()};
          const auto live = take(paths.segment(0, name));
          const auto marked = take(paths.deleting(0, name));
          const auto there = [&](const runtime::file_path& path) {
              return drive.lifecycle(files.exists(path));
          };
          const auto target = take(
            co_await drive.lifecycle(table.create(creation(spec, 1), work)));
          for (const std::uint64_t logical : {100U, 101U})
              co_await append_one(
                append, target, logical, 1, budget, work, drive);
          const auto sealed = co_await drive.lifecycle(table.seal(a));
          take(sealed.failure.outcome());
          const auto kept = table.expectation(a);
          require(
            sealed.extent && kept,
            "a sealed segment does not say what it was published as");
          if (!held) take(co_await drive.lifecycle(table.close_segment(a)));
          const deletion_authorization exact{a, *sealed.extent};
          auto altered = *sealed.extent;
          altered.file_end = runtime::file_position{
            altered.file_end.value() + 1};
          found(take(paths.segment_file(0, name, local_segment_file::data)));

          const auto tried = co_await drive.lifecycle(
            table.remove(exact, &*kept));
          if (!faulted) {
              take(tried.failure.outcome());
              require(
                tried.gone && !take(co_await there(live))
                  && !take(co_await there(marked)),
                "a removal nothing failed did not finish");
              co_return;
          }
          const auto left = take(
            local_child_path(marked, take(runtime::file_name::make("data"))));
          require(
            tried.verdict == authorization_verdict::authorized && !tried.gone
              && !tried.settling && tried.failure.failed()
              && tried.debt.failed() && !take(co_await there(live))
              && take(co_await there(marked)) && take(co_await there(left)),
            "a removal whose unlink failed lost its cause or its place");
          if (held)
              require(
                table.life(a) == segment_life::deleting
                  && table.debt(a).failed()
                  && table.retained(a).need == retention_need::none
                  && table.retained(a).going
                  && table.check_deletion(exact)
                       == authorization_verdict::repeated
                  && table.check_deletion({a, altered})
                       == authorization_verdict::wrong_identity,
                "a segment the table held did not stay deleting with what "
                "it owes");
          else
              require(
                !table.life(a) && table.size() == 0
                  && table.check_deletion(exact)
                       == authorization_verdict::unknown,
                "an owner made for a removal outlived its attempt");

          // The shard goes on beside what it owes.
          const auto next = take(
            co_await drive.lifecycle(table.create(creation(spec, 2), work)));
          co_await append_one(append, next, 100, 2, budget, work, drive);

          const auto again = co_await drive.lifecycle(table.remove(exact));
          take(again.failure.outcome());
          require(
            again.verdict == authorization_verdict::repeated && again.gone
              && again.removed >= 1 && again.foreign == 0 && !table.life(a)
              && !take(co_await there(live)) && !take(co_await there(marked)),
            "the same request did not finish a removal that had failed");
      });
}

} // namespace kwaque::storage::testing::segment_lifecycle_contract
