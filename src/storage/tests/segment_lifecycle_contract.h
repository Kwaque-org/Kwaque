#pragma once

#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/test_allocation_profile.h"
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
#include "src/storage/recovery_decision.h"
#include "src/storage/segment_lifecycle.h"
#include "src/storage/segment_reader.h"
#include "src/storage/segment_writer.h"
#include "src/storage/segment_writer_state.h"
#include "src/storage/sparse_index.h"
#include "src/storage/sparse_index_owner.h"
#include "src/storage/tests/local_append_contract.h"
#include "src/storage/tests/local_store_contract.h"
#include "src/storage/tests/segment_reader_contract.h"
#include "src/storage/tests/segment_scan_contract.h"
#include "src/storage/tests/segment_writer_contract.h"
#include "src/storage/tests/sparse_index_contract.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <initializer_list>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace kwaque::storage::testing::segment_lifecycle_contract {
using store_contract::require;
using store_contract::take;
namespace appends = local_append_contract;
namespace segments = segment_writer_contract;

// What may begin in each state, written out: one row a state, in the order
// seal, index publication, deletion, close.
namespace rule {
struct row final {
    segment_life life;
    bool appends, readers;
    std::array<bool, 4> begins;
};
inline constexpr std::array<row, 7> table{{
  {segment_life::creating, false, false, {false, false, false, true}},
  {segment_life::writable, true, true, {true, false, false, true}},
  {segment_life::sealing, false, true, {false, false, false, true}},
  {segment_life::sealed, false, true, {false, true, true, true}},
  {segment_life::recovering, false, false, {false, false, true, true}},
  {segment_life::failed, false, false, {false, false, true, true}},
  {segment_life::deleting, false, false, {false, false, false, true}},
}};
inline constexpr std::array<segment_transition, 4> changes{
  segment_transition::seal,
  segment_transition::index_publication,
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
    // The bounds these contracts were written for; the defaults are a
    // shard's of 1,024 segments. Two seal slots, which the table reserves
    // when it is made, leave the fixtures' budgets what their segments
    // need.
    config.maximum_writable = 64;
    config.maximum_prepared = 64;
    config.maximum_seals = 2;
    // The WAL read as the segments are: in windows the fixtures' objects
    // fit, with the working allowance their decodes need.
    config.wal_scan = {
      config.scan.metadata,
      config.scan.reader,
      config.scan.working_bytes,
      config.scan.decode_metadata_bytes};
    return config;
}
inline segment_creation
creation(const local_device_spec& spec, std::uint64_t generation) {
    auto described = segments::descriptor();
    described.segment = appends::context(generation);
    auto writer = segments::configuration();
    writer.admission.working_bytes = byte_count{1_MiB};
    return {described, segments::logical_origin(), spec.owner.device(), writer};
}
// A request whose block is exactly `bytes` long.
inline std::string
sized_wire(std::uint64_t logical, std::size_t bytes, std::uint64_t generation) {
    for (std::size_t value = bytes - 420; value <= bytes - 390; ++value) {
        auto wire = appends::valued_wire(logical, value, generation);
        if (176 + wire.size() == bytes) return wire;
    }
    throw std::runtime_error("no request makes a block of that size");
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
// One request, durable; it returns once every barrier owed has run, so its
// group is readable and its footer is the newest durable one.
template<
  runtime::monotonic_clock Clock,
  typename Append,
  typename Driver,
  typename Timer>
seastar::future<> append_one(
  Append& append,
  local_append_target target,
  std::uint64_t logical,
  std::uint64_t generation,
  workload_budget& budget,
  codec::cooperative_work& work,
  Driver drive,
  Timer& timer) {
    static_cast<void>(appends::durable(
      co_await appends::settle(
        append.append(
          target,
          co_await appends::request(
            appends::child_wire(logical, generation), budget, work),
          work),
        drive)));
    co_await appends::covered<Clock>(append, drive, timer);
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
  Func body,
  byte_count placed_below
  = byte_count{std::numeric_limits<std::uint64_t>::max()},
  byte_count hold = {}) {
    using table_type = segment_table<Backend, Owner, Clock>;
    co_await appends::with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 2,
       .segments = 0,
       .placement = placed_below,
       .hold = hold},
      [&](
        auto& append, auto&, auto&, auto&, auto& work, auto& control, auto& ids)
        -> seastar::future<> {
          const std::array devices{spec};
          sparse_index_residency residency{4};
          sparse_index_rebuild_limit rebuilds;
          auto cache = take(
            page_cache::make(budget, {.capacity = byte_count{2_MiB}}));
          // The decisions a seal of a failed segment makes durable while
          // the shard runs, as an engine's table has them.
          auto decisions = take(
            recovery_decision_log<Backend, Owner>::make(
              files,
              owner,
              spec,
              0,
              ids,
              budget,
              {store_contract::limits(), 16}));
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
                   placement,
                   &control,
                   decisions.get()},
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
              failed.observe(co_await drive.lifecycle(decisions->close()));
          } catch (...) {
              failed.observe(std::current_exception());
          }
          decisions.reset();
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
// context alone, fed from its first group for the reader it was created
// with; a second creation of it is refused. Its seal is one attempt: a
// second request joins it and a later one is given its outcome. While it
// seals its successor takes appends. The seal abandons nothing that was
// accepted, moves the session that was open, which reads on to the
// segment's sealed end, and leaves the segment sealed with no attachment:
// its old target takes nothing, and a new session reads the sealed segment
// to its end. Closing the owner ends its sessions and takes it out of the
// table; what a session returned stays its holder's.
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
          const auto first = segments::logical_origin().value();
          require(first == 100, "the fixture's segments do not begin at 100");
          // Created with the reader its owner of segments counts on it: fed
          // before its first group.
          auto counted = creation(spec, 1);
          counted.readers = 1;
          const auto target = take(
            co_await drive.lifecycle(table.create(counted, work)));
          require(
            table.fed(a) && table.establish(a, 2) && !table.establish(b, 1)
              && !table.fed(b),
            "a segment created with a reader was not fed, or one the table "
            "holds no owner for was told of readers");
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
              co_await append_one<Clock>(
                append, target, logical, 1, budget, work, drive, timer);
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
              co_await append_one<Clock>(
                append, next, 100, 2, budget, work, drive, timer);
              const auto sealed = co_await drive.lifecycle(std::move(sealing));
              const auto same = co_await drive.lifecycle(std::move(joined));
              take(sealed.failure.outcome());
              const auto again = co_await drive.lifecycle(table.seal(a));
              require(
                sealed.extent && sealed.boundary && sealed.retry && sealed.index
                  && sealed.retry->pages().value() == 1
                  && same.boundary == sealed.boundary
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
                  && rest.stop == segment_read_stop::end_of_segment
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
          const auto first = segments::logical_origin().value();
          std::optional<sealed_segment_expectation> kept_a, kept_b;
          {
              const auto target = take(
                co_await drive.lifecycle(
                  table.create(creation(spec, 1), work)));
              for (const std::uint64_t logical : {100U, 101U})
                  co_await append_one<Clock>(
                    append, target, logical, 1, budget, work, drive, timer);
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
              co_await append_one<Clock>(
                append, target, 100, 2, budget, work, drive, timer);
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
// its turn comes. A seal publishes the completed facts of the batches the
// segment's appends published, which nobody supplied. A segment that is told
// failed takes no seal and no new session, the session it had reads on, and
// its owner closes while the append owner still runs. No more writable
// segments are created than may be, and a table that closes refuses
// everything.
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
          const auto first = segments::logical_origin().value();
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
          // Two appends, then sealed: its summary has their facts, one page
          // of them, with nothing supplied.
          static_cast<void>(appends::durable(
            co_await appends::settle(
              append.append(
                target,
                co_await appends::request(
                  appends::child_wire(100), budget, work),
                work),
              drive)));
          co_await append_one<Clock>(
            append, target, 101, 1, budget, work, drive, timer);
          const auto sealed = co_await drive.lifecycle(table.seal(a));
          take(sealed.failure.outcome());
          require(
            sealed.retry && sealed.retry->pages().value() == 1
              && table.life(a) == segment_life::sealed,
            "a seal did not publish the facts of its segment's appends");

          // The other segment is told failed.
          co_await append_one<Clock>(
            append, other, 100, 2, budget, work, drive, timer);
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
              const auto shut = co_await drive.lifecycle(
                table.open_session(b, sight(first + 64), budget, work));
              const auto read = co_await read_all(*reader, first, drive);
              require(
                table.life(b) == segment_life::failed && !table.target(b)
                  && table.failure(b).failed()
                  && table.life(a) == segment_life::sealed
                  && refused.failure.error()
                  && refused.failure.error()->code() == errc::io_failure
                  && !shut && shut.error().code() == errc::closed,
                "a failed segment was sealed or opened to a new session");
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
            receipt.boundary.footer().has_value(),
            "a durable group has no footer");
          const auto footer = receipt.boundary.footer()->size().value();
          const auto less = take(table.left(a));
          require(
            block != 0 && footer != 0
              && whole.bytes.value() - less.bytes.value() == block + footer
              && less.blocks == whole.blocks - 1 && less.deadline,
            "what is left did not shrink by the group that was taken, or "
            "an appended segment has no age");

          // The smallest segment: a block of exactly what is left of it is
          // taken, and then nothing is.
          auto small = creation(spec, 2);
          small.descriptor.maximum_data_bytes = byte_count{
            4 * small.descriptor.alignment.bytes().value()};
          const auto tight = take(
            co_await drive.lifecycle(table.create(small, work)));
          const auto room = take(table.left(b)).bytes.value();
          require(room != 0, "the smallest segment takes no block");
          static_cast<void>(appends::durable(
            co_await appends::settle(
              append.append(
                tight,
                co_await appends::request(
                  sized_wire(100, room, 2), budget, work, appends::large_width),
                work),
              drive)));
          co_await appends::covered<Clock>(append, drive, timer);
          const auto none = take(table.left(b));
          require(
            none.bytes.value() == 0 && none.blocks == 0 && none.deadline,
            "a full segment says something is left of it");
          const auto first = co_await refused_one(
            append, tight, 101, 2, budget, work, drive);
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
            append, tight, 101, 2, budget, work, drive);
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
          co_await append_one<Clock>(
            append, old, 100, 1, budget, work, drive, timer);
          const auto late = take(table.left(a)).deadline;
          auto brief = creation(spec, 2);
          brief.descriptor.maximum_lifetime = runtime::monotonic_duration{
            150'000'000};
          const auto young = take(
            co_await drive.lifecycle(table.create(brief, work)));
          require(
            late && table.aging() == 1 && !take(table.left(b)).deadline,
            "a lifetime did not begin at the first accepted group");
          co_await append_one<Clock>(
            append, young, 100, 2, budget, work, drive, timer);
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
          co_await append_one<Clock>(
            append, third, 100, 3, budget, work, drive, timer);
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
          co_await append_one<Clock>(
            append, old, 100, 1, budget, work, drive, timer);
          const auto next = take(co_await drive.lifecycle(std::move(ahead)));
          co_await append_one<Clock>(
            append, next, 100, 2, budget, work, drive, timer);
          co_await append_one<Clock>(
            append, old, 101, 1, budget, work, drive, timer);
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
          co_await append_one<Clock>(
            append, next, 101, 2, budget, work, drive, timer);
          take(
            (co_await drive.lifecycle(std::move(sealing))).failure.outcome());
          auto following = table.create(creation(spec, 3), apart);
          co_await append_one<Clock>(
            append, next, 102, 2, budget, work, drive, timer);
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
            sealed.extent && !sealed.index && sealed.retry
              && sealed.retry->pages().value() == 0
              && table.life(c) == segment_life::sealed && !table.target(c)
              && fourth && table.size() == 4,
            "an unused segment was not sealed whole, or its place among "
            "the writable was not free again");
      });
}

// A segment is prepared ahead of the request that activates it: its files
// are made and closed again, and it holds nothing of the budget, takes no
// append and opens no session. Prepared segments are bounded apart from the
// writable ones. Activation opens one at the origin its owner gives,
// writing and syncing nothing, and appends flow at once; while as many
// are writable as may be it is refused and stays prepared, and one that is
// not prepared is not activated. Two prepared at once and activated at
// different origins each begin at their own.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer,
  typename Mutations = store_contract::uncounted_mutations>
seastar::future<> prepared_ahead(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer,
  Mutations mutations = {}) {
    auto bounded = configuration();
    bounded.maximum_writable = 2;
    bounded.maximum_prepared = 2;
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
          const auto paths = take(local_paths::make(spec.root));
          const auto data = take(paths.segment_file(
            0, {b.segment(), b.generation()}, local_segment_file::data));
          const auto old = take(
            co_await drive.lifecycle(table.create(creation(spec, 1), work)));
          const auto before = budget.snapshot();
          take(
            co_await drive.lifecycle(table.prepare(creation(spec, 2), work)));
          take(
            co_await drive.lifecycle(table.prepare(creation(spec, 3), work)));
          const auto held = budget.snapshot();
          const auto over = co_await drive.lifecycle(
            table.prepare(creation(spec, 4), work));
          require(
            table.life(b) == segment_life::prepared
              && table.life(c) == segment_life::prepared && !table.target(b)
              && !table.target(c) && held.bytes == before.bytes
              && held.tasks == before.tasks && held.handles == before.handles
              && !over && over.error().code() == errc::queue_full
              && !table.life(appends::context(4)),
            "a prepared segment holds the budget or is attached, or more were "
            "prepared than may be");
          {
              const auto shut = co_await drive.lifecycle(
                table.open_session(b, sight(600), budget, work));
              const auto again = co_await drive.lifecycle(table.activate(
                a, {.logical_origin = model::range_logical_end{500}}, work));
              require(
                !shut && shut.error().code() == errc::closed && !again
                  && again.error().code() == errc::closed,
                "a prepared segment opened a session, or a writable one was "
                "activated");
          }
          co_await append_one<Clock>(
            append, old, 100, 1, budget, work, drive, timer);
          const auto made = co_await store_contract::read_all_bytes(
            files, data, drive);
          const auto quiet = mutations();
          const auto first = take(
            co_await drive.lifecycle(table.activate(
              b, {.logical_origin = model::range_logical_end{500}}, work)));
          const auto refused = co_await drive.lifecycle(table.activate(
            c, {.logical_origin = model::range_logical_end{900}}, work));
          // Where the backend counts them: nothing written or synced
          // anywhere, the files only opened.
          require(
            table.life(b) == segment_life::writable
              && table.target(b) == std::optional{first} && !refused
              && refused.error().code() == errc::queue_full
              && table.life(c) == segment_life::prepared && mutations() == quiet
              && (co_await store_contract::read_all_bytes(files, data, drive))
                   == made,
            "an activation wrote to or synced the store, or one past the "
            "writable bound was not refused and left prepared");
          co_await append_one<Clock>(
            append, first, 500, 2, budget, work, drive, timer);
          take((co_await drive.lifecycle(table.seal(a))).failure.outcome());
          const auto second = take(
            co_await drive.lifecycle(table.activate(
              c, {.logical_origin = model::range_logical_end{900}}, work)));
          co_await append_one<Clock>(
            append, second, 900, 3, budget, work, drive, timer);
          for (const auto& [of, origin] :
               {std::pair{b, std::uint64_t{500}},
                std::pair{c, std::uint64_t{900}}}) {
              auto session = take(
                co_await drive.lifecycle(
                  table.open_session(of, sight(origin + 64), budget, work)));
              const auto read = co_await read_all(session, origin, drive);
              require(
                begins(read, {origin}),
                "an activated segment did not begin at its own origin");
          }
      });
}

// Every seal runs in a seal slot whose grants the table reserved when it
// was made: one more seal than there are slots waits for a slot and
// completes, and none fails for want of the budget.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> sealed_in_slots(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    constexpr std::uint64_t segments = 5;
    auto slots = configuration();
    slots.maximum_seals = segments - 1;
    co_await with_segment_table<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      slots,
      [&](auto& table, auto& append, auto& work) -> seastar::future<> {
          for (std::uint64_t generation = 1; generation <= segments;
               ++generation) {
              const auto target = take(
                co_await drive.lifecycle(
                  table.create(creation(spec, generation), work)));
              co_await append_one<Clock>(
                append, target, 100, generation, budget, work, drive, timer);
          }
          std::vector<seastar::future<segment_sealed>> sealing;
          sealing.reserve(segments);
          for (std::uint64_t generation = 1; generation <= segments;
               ++generation)
              sealing.push_back(table.seal(appends::context(generation)));
          for (std::uint64_t generation = 1; generation <= segments;
               ++generation) {
              const auto sealed = co_await drive.lifecycle(
                std::move(sealing[generation - 1]));
              take(sealed.failure.outcome());
              require(
                sealed.extent && sealed.retry
                  && table.life(appends::context(generation))
                       == segment_life::sealed,
                "a seal beyond the slots failed or did not complete");
          }
      });
}

// An idle writable segment holds what every other does: its writer's
// instance, path, queue chunks, handle credits, lifetime token and
// completion reserve, its index's first chunk, its runs and its
// attachment, and of the handle credits only its writer's. Nothing it holds
// grows with the segments open beside it, and under the native allocator it
// is at most 128 KiB, also for a segment of 1 GiB, whose index may hold the
// most anchors: its first chunk is one page. One at the writer's defaults, with
// a zero-written window it extends, holds the same memory and one more
// credit, its synchronized handle's: the zero chunk and its staging are
// taken by each extension and let go after it. A group of 1 MiB is charged
// its staging and returns it: once it is durable the segment costs what it
// did after its first, small group.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> idle_cost(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    // Every portion is placed in its segment, the 1 MiB group's too.
    co_await with_placed_table<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      configuration(),
      nullptr,
      [&](auto& table, auto& append, auto& work) -> seastar::future<> {
          constexpr std::uint64_t segments = 4;
          const auto first = segments::logical_origin().value();
          static_cast<void>(take(
            co_await drive.lifecycle(table.create(creation(spec, 1), work))));
          const auto idle = budget.snapshot();
          workload_budget_snapshot each;
          for (std::uint64_t generation = 2; generation <= segments;
               ++generation) {
              static_cast<void>(take(
                co_await drive.lifecycle(
                  table.create(creation(spec, generation), work))));
              const auto now = budget.snapshot();
              const auto added = generation - 1;
              if (added == 1)
                  each = {
                    now.tasks - idle.tasks,
                    now.bytes - idle.bytes,
                    now.handles - idle.handles};
              require(
                each.handles == segment_writer_handles(creation(spec, 1).writer)
                  && now.tasks == idle.tasks + added * each.tasks
                  && now.bytes == idle.bytes + added * each.bytes
                  && now.handles == idle.handles + added * each.handles,
                "an idle segment's cost grew with the segments beside it, or "
                "it holds more handle credits than its writer's");
          }
          // The target is the native allocator's; the system allocator's
          // model prices every allocation above it.
          if (bytes::testing::native_charge_profile) {
              auto large = creation(spec, segments + 2);
              large.descriptor.maximum_data_bytes = byte_count{1_GiB};
              const auto before = budget.snapshot();
              static_cast<void>(
                take(co_await drive.lifecycle(table.create(large, work))));
              const auto after = budget.snapshot();
              require(
                each.bytes <= 128_KiB && after.bytes - before.bytes <= 128_KiB,
                "an idle segment costs more than its 128 KiB target");
          }
          auto defaults = creation(spec, segments + 1);
          defaults.writer = segments::with_defaults();
          const auto before = budget.snapshot();
          const auto target = take(
            co_await drive.lifecycle(table.create(defaults, work)));
          // Its window's first extension has run, and let go of its zeros.
          co_await pass<Clock>(drive, timer, 10);
          const auto extended = budget.snapshot();
          require(
            extended.bytes == before.bytes + each.bytes
              && extended.tasks == before.tasks + each.tasks
              && extended.handles == before.handles + each.handles + 1,
            "an idle segment at the writer's defaults held its window's zero "
            "chunk, or more than one segment more does");
          // A segment that took a group holds more than an idle one: what
          // its first group leaves is the measure of the next one.
          co_await append_one<Clock>(
            append, target, first, segments + 1, budget, work, drive, timer);
          co_await pass<Clock>(drive, timer, 10);
          const auto written = budget.snapshot();
          static_cast<void>(appends::durable(
            co_await appends::settle(
              append.append(
                target,
                co_await appends::large_request(
                  appends::valued_wire(first + 1, 1_MiB - 1_KiB, segments + 1),
                  budget,
                  work),
                work),
              drive)));
          co_await appends::covered<Clock>(append, drive, timer);
          // What the group's background work holds goes when it ends.
          require(
            co_await eventually<Clock>(
              drive,
              timer,
              [&] {
                  const auto after = budget.snapshot();
                  return after.bytes == written.bytes
                         && after.tasks == written.tasks
                         && after.handles == written.handles;
              }),
            "a 1 MiB group's staging was not returned with it");
      },
      byte_count{0});
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
          co_await append_one<Clock>(
            append, first, 100, 1, budget, work, drive, timer);
          const auto less = table.owed()[0].value();
          take((co_await drive.lifecycle(table.seal(a))).failure.outcome());
          const auto one = table.owed()[0].value();
          require(
            less < owed && one < bound && one >= bound - 64_KiB
              && placement->writable(device) == 1,
            "what a shard owes did not follow what its segments wrote, or "
            "a sealed segment still counts");
          // A segment made with a zero-written window already takes the
          // window on its device: it owes its bound less the window.
          {
              hand(2 * bound + 1);
              constexpr std::uint64_t window = 1_MiB;
              auto windowed = unnamed(4);
              windowed.writer.preallocation_bytes = byte_count{window};
              static_cast<void>(
                take(co_await drive.lifecycle(table.create(windowed, work))));
              const auto both = table.owed()[0].value();
              require(
                both > one && both - one < bound - window
                  && both - one >= bound - window - 64_KiB,
                "a zero-written window was not counted as already taken");
          }

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
          const auto first = segments::logical_origin().value();
          const auto old = take(
            co_await drive.lifecycle(table.create(creation(spec, 1), work)));
          for (const std::uint64_t logical : {100U, 101U})
              co_await append_one<Clock>(
                append, old, logical, 1, budget, work, drive, timer);
          const auto sealed = co_await drive.lifecycle(table.seal(a));
          take(sealed.failure.outcome());
          const auto kept = table.expectation(a);
          const auto young = take(
            co_await drive.lifecycle(table.create(creation(spec, 2), work)));
          co_await append_one<Clock>(
            append, young, 100, 2, budget, work, drive, timer);
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
          const auto first = segments::logical_origin().value();
          const auto target = take(
            co_await drive.lifecycle(table.create(creation(spec, 1), work)));
          for (const std::uint64_t logical : {100U, 101U})
              co_await append_one<Clock>(
                append, target, logical, 1, budget, work, drive, timer);
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

// A removed segment's data file is kept as a spare of its device while the
// device is healthy and the spares, with it, stay within the shard's share:
// it moves under the spare directory, its bytes unchanged. The next prepare
// there makes its data file of the oldest spare: the device table takes
// nothing for its files then, its activation places its bound less what
// the spare held, and its appends overwrite what the spare holds without
// growing the file. A device with no room left keeps no more spares than
// creations at once take, the oldest going first, and a removal there
// unlinks the file.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer,
  typename Watch = store_contract::unwatched_writes>
seastar::future<> recycled_on_removal(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer,
  Watch watch = {}) {
    using table_type = segment_table<Backend, Owner, Clock>;
    table_type* watched = nullptr;
    const std::array devices{spec};
    const auto placement = take(
      device_table::make(
        devices, {}, [&](const device_report& report) noexcept {
            if (watched) watched->device_changed(report);
        }));
    const auto device = spec.owner.device();
    const auto hand = [&](std::uint64_t share) {
        std::array<device_pool_share, device_table::maximum_devices> shares{};
        shares[0] = {true, false, byte_count{share}};
        placement->sampled(shares, Clock::now());
    };
    // A fresh sample of the share as it stands: the device stays healthy.
    const auto refresh = [&] { hand(placement->share(device).value()); };
    auto config = configuration();
    config.maximum_creations = 1;
    co_await with_placed_table<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      config,
      placement.get(),
      [&](auto& table, auto& append, auto& work) -> seastar::future<> {
          watched = &table;
          const auto unnamed = [&](std::uint64_t generation) {
              auto made = creation(spec, generation);
              made.device.reset();
              return made;
          };
          const auto bound = take(
                               table_type::writer_type::disk_bound(
                                 unnamed(1).descriptor, unnamed(1).writer))
                               .value();
          hand(8 * bound);
          const auto paths = take(local_paths::make(spec.root));
          const auto data = [&](std::uint64_t generation) {
              const auto of = appends::context(generation);
              return take(paths.segment_file(
                0, {of.segment(), of.generation()}, local_segment_file::data));
          };
          const auto spare = [&](std::uint64_t generation) {
              const auto of = appends::context(generation);
              return take(paths.spare(
                0, local_segment_name{of.segment(), of.generation()}));
          };
          const auto there = [&](const runtime::file_path& path) {
              return drive.lifecycle(files.exists(path));
          };
          const auto sealed = [&](std::uint64_t generation)
            -> seastar::future<deletion_authorization> {
              const auto of = appends::context(generation);
              const auto answer = co_await drive.lifecycle(table.seal(of));
              take(answer.failure.outcome());
              require(
                answer.extent.has_value(),
                "a sealed segment does not say what it was published as");
              co_return deletion_authorization{of, *answer.extent};
          };
          const auto made_and_sealed =
            [&](std::uint64_t generation, std::uint64_t requests = 1)
            -> seastar::future<deletion_authorization> {
              refresh();
              const auto target = take(
                co_await drive.lifecycle(
                  table.create(unnamed(generation), work)));
              for (std::uint64_t i = 0; i != requests; ++i)
                  co_await append_one<Clock>(
                    append,
                    target,
                    100 + i,
                    generation,
                    budget,
                    work,
                    drive,
                    timer);
              co_return co_await sealed(generation);
          };
          const auto removed =
            [&](deletion_authorization asked) -> seastar::future<> {
              refresh();
              const auto gone = co_await drive.lifecycle(table.remove(asked));
              take(gone.failure.outcome());
              require(
                gone.gone && gone.recycled == 1 && gone.removed >= 3
                  && !table.life(asked.segment),
                "an authorized removal did not take the segment away, or did "
                "not count its data file kept as a spare");
          };

          // Every segment is made before any is removed: a creation takes
          // a spare as a prepare does. Two removed while the device is
          // healthy: each data file stays, as a spare. The first wrote three
          // groups, so its later ones outlast what one group overwrites.
          const auto first = co_await made_and_sealed(1, 3);
          const auto third = co_await made_and_sealed(3);
          const auto last = co_await made_and_sealed(4);
          const auto kept = co_await store_contract::read_all_bytes(
            files, data(1), drive);
          co_await removed(first);
          co_await removed(third);
          require(
            table.spares(device) == 2
              && (co_await store_contract::read_all_bytes(
                   files, spare(1), drive))
                   == kept
              && take(co_await there(spare(3)))
              && !take(co_await there(data(1)))
              && !take(co_await there(data(3))),
            "a removed segment's data file was not kept as a spare, or "
            "changed");

          // The next prepare takes the oldest, and takes no space for it.
          refresh();
          const auto before = placement->share(device);
          take(co_await drive.lifecycle(table.prepare(unnamed(2), work)));
          const auto made = co_await store_contract::read_all_bytes(
            files, data(2), drive);
          require(
            table.life(appends::context(2)) == segment_life::prepared
              && table.spares(device) == 1 && !take(co_await there(spare(1)))
              && take(co_await there(spare(3))) && made.size() == kept.size()
              && made.substr(0, 8192) != kept.substr(0, 8192)
              && made.substr(8192) == kept.substr(8192)
              && placement->share(device) == before,
            "a prepare did not make its data file of the oldest spare, or "
            "took space for it");
          // Its activation places what the spare did not hold, and its
          // append overwrites what the spare holds.
          refresh();
          const auto placed = placement->share(device).value();
          const auto held = kept.size() / 8192 * 8192;
          const auto target = take(
            co_await drive.lifecycle(table.activate(
              appends::context(2),
              {.logical_origin = segments::logical_origin()},
              work)));
          require(
            placement->share(device).value() == placed - (bound - held),
            "an activation placed more than what the spare did not hold");
          watch.watch(data(2), kept.size());
          co_await append_one<Clock>(
            append, target, 100, 2, budget, work, drive, timer);
          const auto appended = co_await store_contract::read_all_bytes(
            files, data(2), drive);
          // Where the backend sees its writes: each lies within what the
          // spare held, an overwrite of blocks written already.
          require(
            appended.size() == kept.size() && watch.beyond().value_or(0) == 0,
            "an append to a segment made of a spare grew its file");
          {
              // Its file still holds the earlier segment's later groups past
              // its own, and a session reads its one batch and nothing of
              // them.
              auto session = take(
                co_await drive.lifecycle(table.open_session(
                  appends::context(2),
                  sight(segments::logical_origin().value() + 64),
                  budget,
                  work)));
              const auto read = co_await read_all(
                session, segments::logical_origin().value(), drive);
              const auto stale = 2 * 8192;
              require(
                begins(read, {100})
                  && read.stop == segment_read_stop::short_of_visibility
                  && !table.failure(appends::context(2)).failed()
                  && appended.substr(stale, 8192) == kept.substr(stale, 8192)
                  && appended.substr(8192, 8192) != kept.substr(8192, 8192),
                "a session on a segment made of a spare read past what it "
                "wrote, or the spare held nothing of its earlier life there");
          }
          co_await removed(co_await sealed(2));
          require(
            table.spares(device) == 2 && take(co_await there(spare(2))),
            "a removal did not keep its data file as a spare");

          // With no room left the oldest spares go, down to what creations
          // at once take, and a removal keeps none.
          hand(0);
          require(
            co_await eventually<Clock>(
              drive, timer, [&] { return table.spares(device) == 1; }),
            "a device with no room left kept more spares than creations at "
            "once take");
          require(
            !take(co_await there(spare(3))) && take(co_await there(spare(2))),
            "the spares that went were not the oldest");
          const auto gone = co_await drive.lifecycle(table.remove(last));
          take(gone.failure.outcome());
          require(
            gone.gone && gone.recycled == 0 && table.spares(device) == 1
              && !take(co_await there(spare(4)))
              && !take(co_await there(data(4))),
            "a removal on a device with no room left kept its data file");
          watched = nullptr;
      });
}

// One data file made into a hundred segments in turn, each removed and
// kept as a spare for the next, each writing a random number of requests
// of random sizes over what the ones before it left. Each time restart's
// walks find exactly its own blocks, its last footer the boundary and no
// damage proven, and a session reads exactly its own batches: nothing an
// earlier segment wrote is ever read as this one's.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> recycled_hundred_times(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    using table_type = segment_table<Backend, Owner, Clock>;
    table_type* watched = nullptr;
    const std::array devices{spec};
    const auto placement = take(
      device_table::make(
        devices, {}, [&](const device_report& report) noexcept {
            if (watched) watched->device_changed(report);
        }));
    const auto device = spec.owner.device();
    auto config = configuration();
    config.maximum_creations = 1;
    co_await with_placed_table<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      config,
      placement.get(),
      [&](auto& table, auto& append, auto& work) -> seastar::future<> {
          watched = &table;
          const auto unnamed = [&](std::uint64_t generation) {
              auto made = creation(spec, generation);
              made.device.reset();
              return made;
          };
          const auto bound = take(
                               table_type::writer_type::disk_bound(
                                 unnamed(1).descriptor, unnamed(1).writer))
                               .value();
          // Room for every segment and its spare, sampled afresh before
          // each creation and removal: the device stays healthy.
          const auto refresh = [&] {
              std::array<device_pool_share, device_table::maximum_devices>
                shares{};
              shares[0] = {true, false, byte_count{8 * bound}};
              placement->sampled(shares, Clock::now());
          };
          const auto first = segments::logical_origin().value();
          // A seeded stream of the case's own: a failing round is the same
          // round each run.
          std::uint64_t state = 0x6b77'6171'7565'0286ULL;
          const auto random = [&state](std::uint64_t bound) {
              state += 0x9e37'79b9'7f4a'7c15ULL;
              auto z = state;
              z = (z ^ (z >> 30U)) * 0xbf58'476d'1ce4'e5b9ULL;
              z = (z ^ (z >> 27U)) * 0x94d0'49bb'1331'11ebULL;
              return (z ^ (z >> 31U)) % bound;
          };
          std::vector<segment_scan_contract::scanned> out;
          for (std::uint64_t generation = 1; generation <= 100; ++generation) {
              refresh();
              const auto made = unnamed(generation);
              const auto target = take(
                co_await drive.lifecycle(table.create(made, work)));
              require(
                table.spares(device) == 0,
                "a segment was not made of the spare the one before it left");
              const auto requests = 1 + random(8);
              for (std::uint64_t i = 0; i != requests; ++i)
                  static_cast<void>(appends::durable(
                    co_await appends::settle(
                      append.append(
                        target,
                        co_await appends::request(
                          sized_wire(
                            first + i, 512 + random(16384), generation),
                          budget,
                          work,
                          appends::large_width),
                        work),
                      drive)));
              co_await appends::covered<Clock>(append, drive, timer);

              const auto unit = made.descriptor.alignment.bytes().value();
              const segment_history_context history{
                made.descriptor.segment,
                made.descriptor.alignment,
                runtime::file_position{unit},
                made.logical_origin,
                made.descriptor.physical_origin};
              const auto walked = co_await segment_scan_contract::walk(
                files, owner, spec, budget, history, std::nullopt, out, drive);
              const auto proved = co_await segment_scan_contract::proving_walk(
                files, owner, spec, budget, std::nullopt, drive, history);
              const auto blocks = std::ranges::count_if(
                out, [](const auto& object) { return object.block; });
              require(
                walked.complete && walked.boundary
                  && static_cast<std::uint64_t>(blocks) == requests
                  && walked.suffix_blocks == 0
                  && walked.verdict != segment_scan_verdict::corrupt
                  && proved.verdict != segment_scan_verdict::corrupt
                  && proved.boundary
                  && proved.boundary->footer.position()
                       == walked.boundary->footer.position()
                  && proved.content_end == walked.content_end
                  && proved.proven_end <= walked.content_end,
                "a walk of a segment made of a spare read an earlier "
                "segment's bytes as its own");
              {
                  auto session = take(
                    co_await drive.lifecycle(table.open_session(
                      made.descriptor.segment,
                      sight(first + 64),
                      budget,
                      work)));
                  const auto read = co_await read_all(session, first, drive);
                  bool own = read.batches.size() == requests;
                  for (std::size_t i = 0; own && i != read.batches.size(); ++i)
                      own = read.batches[i].logical.begin().value()
                            == first + i;
                  require(
                    own && read.stop == segment_read_stop::short_of_visibility,
                    "a session on a segment made of a spare read other than "
                    "its own batches");
              }

              const auto answer = co_await drive.lifecycle(
                table.seal(made.descriptor.segment));
              take(answer.failure.outcome());
              require(
                answer.extent.has_value(),
                "a sealed segment does not say what it was published as");
              refresh();
              const auto gone = co_await drive.lifecycle(table.remove(
                deletion_authorization{
                  made.descriptor.segment, *answer.extent}));
              take(gone.failure.outcome());
              require(
                gone.gone && gone.recycled == 1 && table.spares(device) == 1,
                "a removed segment's data file was not kept as a spare");
          }
          watched = nullptr;
      });
}

// A segment's failure is of the kind its cause makes it: its device's for
// an I/O error, a file system turned read-only or a device gone; space for
// no space or no quota; its own for memory refused; internal for anything
// no operation expects. Whatever the kind, it leaves the append owner once
// what it accepted settled, with its reported prefix, its newest durable
// footer and where the WAL then ended, and its writer lets go of its
// handles: the budget holds the handle credits it held before the segment
// was made.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> failure_kinds(
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
          struct cause final {
              runtime::first_failure why;
              segment_failure_kind kind;
          };
          const auto file = [](runtime::file_failure_detail detail) {
              runtime::first_failure why;
              why.observe(runtime::make_file_error(errc::io_failure, detail));
              return why;
          };
          const auto thrown = [](std::exception_ptr error) {
              runtime::first_failure why;
              why.observe(std::move(error));
              return why;
          };
          using detail_kind = runtime::file_failure_detail;
          const std::array causes{
            cause{file(detail_kind::device_io), segment_failure_kind::device},
            cause{file(detail_kind::read_only), segment_failure_kind::device},
            cause{file(detail_kind::device_gone), segment_failure_kind::device},
            cause{file(detail_kind::no_space), segment_failure_kind::space},
            cause{file(detail_kind::quota), segment_failure_kind::space},
            cause{
              thrown(std::make_exception_ptr(std::bad_alloc{})),
              segment_failure_kind::own},
            cause{
              thrown(std::make_exception_ptr(std::runtime_error{"unknown"})),
              segment_failure_kind::internal}};
          std::uint64_t generation = 0;
          for (const auto& [why, kind] : causes) {
              ++generation;
              const auto of = appends::context(generation);
              const auto before = budget.snapshot();
              const auto target = take(
                co_await drive.lifecycle(
                  table.create(creation(spec, generation), work)));
              co_await append_one<Clock>(
                append, target, 100, generation, budget, work, drive, timer);
              const auto told = table.fail(of, why);
              require(
                told == std::optional{kind}
                  && table.life(of) == segment_life::failed
                  && !table.target(of),
                "a segment's failure was not of the kind its cause makes it");
              require(
                co_await eventually<Clock>(
                  drive,
                  timer,
                  [&] {
                      const auto left = table.failed_boundary(of);
                      return left && left->released;
                  }),
                "a failed segment did not leave the append owner, or its "
                "writer kept its handles");
              const auto left = *table.failed_boundary(of);
              require(
                left.kind == kind && left.reported && left.durable && left.quiet
                  && budget.snapshot().handles == before.handles,
                "a released segment did not say what it left, or kept its "
                "handle credits");
          }
      });
}

// A segment that failed is sealed while the shard runs, at an end no higher
// than its reported prefix, once it has left the append owner: its layout
// below that end is proved against the WAL as a restart proves it, and the
// decision to seal it there is made durable first. A failure of its own is
// sealed at once, in a seal slot on handles of its own: the segment is
// sealed and reads to its end. A failure of its device is only decided,
// and the next start seals it. A segment that has not failed, and an end
// above what it reported, are refused.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> running_seal(
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
          const auto first = segments::logical_origin().value();
          const auto left_at = [&](const segment_context& of)
            -> seastar::future<runtime::file_position> {
              require(
                co_await eventually<Clock>(
                  drive,
                  timer,
                  [&] {
                      const auto left = table.failed_boundary(of);
                      return left && left->released;
                  }),
                "a failed segment did not leave the append owner");
              const auto left = *table.failed_boundary(of);
              require(
                left.reported.has_value(),
                "a failed segment that took appends reported nothing");
              co_return left.reported->covered.bytes().end();
          };
          const auto target = take(
            co_await drive.lifecycle(table.create(creation(spec, 1), work)));
          for (const std::uint64_t logical : {100U, 101U})
              co_await append_one<Clock>(
                append, target, logical, 1, budget, work, drive, timer);
          {
              const auto early = co_await drive.lifecycle(
                table.seal_failed(a, runtime::file_position{}, 1));
              require(
                early.failure.failed() && !early.decided && !early.sealed,
                "a segment that has not failed was sealed as failed");
          }
          runtime::first_failure memory;
          memory.observe(std::make_exception_ptr(std::bad_alloc{}));
          require(
            table.fail(a, memory) == std::optional{segment_failure_kind::own},
            "memory refused is not a failure of the segment's own");
          const auto end = co_await left_at(a);
          {
              const auto above = co_await drive.lifecycle(table.seal_failed(
                a, runtime::file_position{end.value() + 8192}, 1));
              require(
                above.failure.error()
                  && above.failure.error()->code() == errc::invalid_argument
                  && !above.decided,
                "an end above the reported prefix was taken");
          }
          const auto sealed = co_await drive.lifecycle(
            table.seal_failed(a, end, 1));
          take(sealed.failure.outcome());
          require(
            sealed.decided
              && sealed.decided->status == recovery_decision_status::created
              && sealed.sealed && sealed.expected
              && table.life(a) == segment_life::sealed,
            "a segment failed on its own was not sealed at its reported end");
          {
              auto session = take(
                co_await drive.lifecycle(
                  table.open_session(a, sight(first + 64), budget, work)));
              const auto read = co_await read_all(session, first, drive);
              require(
                begins(read, {100, 101})
                  && read.stop == segment_read_stop::end_of_segment,
                "a segment sealed while the shard runs does not read to its "
                "end");
          }

          // Its device failed: only the decision is made.
          const auto other = take(
            co_await drive.lifecycle(table.create(creation(spec, 2), work)));
          co_await append_one<Clock>(
            append, other, 100, 2, budget, work, drive, timer);
          runtime::first_failure device;
          device.observe(
            runtime::make_file_error(
              errc::io_failure, runtime::file_failure_detail::device_io));
          static_cast<void>(table.fail(b, device));
          const auto decided = co_await drive.lifecycle(
            table.seal_failed(b, co_await left_at(b), 2));
          take(decided.failure.outcome());
          require(
            decided.decided
              && decided.decided->status == recovery_decision_status::created
              && !decided.sealed && !decided.expected
              && table.life(b) == segment_life::failed,
            "a segment whose device failed was written to, or its decision "
            "was not made");
      });
}

// A segment fails on its own with groups whose WAL-placed portions it held
// and never wrote: they are durable in the WAL alone, and its reported
// prefix ends inside their run, which has no footer yet. A seal keeping all
// of it ends where the footer closing the run would, and none above that is
// taken. As the segment is let go its writer writes the run; where that
// write fails, the seal while the shard runs writes each block from its
// PREPARE and the footer after them. Either way the sealed segment reads
// them to its end. `ready(data)` is asked before the run is written and
// says whether its write was set to fail.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer,
  typename Ready>
seastar::future<> running_seal_of_held(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer,
  Ready ready) {
    co_await with_placed_table<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      configuration(),
      nullptr,
      [&](auto& table, auto& append, auto& work) -> seastar::future<> {
          const auto a = appends::context(1);
          const auto first = segments::logical_origin().value();
          const auto target = take(
            co_await drive.lifecycle(table.create(creation(spec, 1), work)));
          // Reported durable by the WAL's barrier; no sync is asked for,
          // which would write the run they are held in.
          for (const std::uint64_t logical : {100U, 101U})
              static_cast<void>(appends::durable(
                co_await appends::settle(
                  append.append(
                    target,
                    co_await appends::request(
                      appends::child_wire(logical, 1), budget, work),
                    work),
                  drive)));
          const auto data = take(
            take(local_paths::make(spec.root))
              .segment_file(
                0, {a.segment(), a.generation()}, local_segment_file::data));
          const auto unit = segments::descriptor().alignment.bytes().value();
          require(
            (co_await store_contract::read_all_bytes(files, data, drive)).size()
              <= unit,
            "a held group was written to its segment");
          const bool armed = ready(data);
          runtime::first_failure memory;
          memory.observe(std::make_exception_ptr(std::bad_alloc{}));
          static_cast<void>(table.fail(a, memory));
          require(
            co_await eventually<Clock>(
              drive,
              timer,
              [&] {
                  const auto left = table.failed_boundary(a);
                  return left && left->released;
              }),
            "a failed segment did not leave the append owner");
          // Its reported prefix ends inside the run: a seal keeping all of
          // it ends where the footer closing the run would.
          const auto left = *table.failed_boundary(a);
          require(
            left.reported && left.sealable
              && left.reported->covered.bytes().end().value() > unit
              && left.reported->covered.bytes().end().value() < 2 * unit
              && left.sealable->value() == 2 * unit,
            "a failed segment's reported prefix inside a held run was not "
            "closed where its footer would end");
          const auto above = co_await drive.lifecycle(table.seal_failed(
            a, runtime::file_position{left.sealable->value() + unit}, 1));
          require(
            above.failure.error()
              && above.failure.error()->code() == errc::invalid_argument
              && !above.decided,
            "an end above where the held run closes was taken");
          const auto sealed = co_await drive.lifecycle(
            table.seal_failed(a, *left.sealable, 1));
          take(sealed.failure.outcome());
          require(
            sealed.sealed && sealed.sealed->blocks == (armed ? 2U : 0U)
              && sealed.sealed->footers == (armed ? 1U : 0U) && sealed.expected
              && table.life(a) == segment_life::sealed,
            "the held groups of a failed segment were not written from their "
            "PREPAREs as it was sealed");
          auto session = take(
            co_await drive.lifecycle(
              table.open_session(a, sight(first + 64), budget, work)));
          const auto read = co_await read_all(session, first, drive);
          require(
            begins(read, {100, 101})
              && read.stop == segment_read_stop::end_of_segment,
            "a segment sealed from its PREPAREs does not read to its end");
      },
      byte_count{std::numeric_limits<std::uint64_t>::max()},
      byte_count{64_KiB});
}

// A segment fails for want of space on its device, and is sealed there
// while the shard runs, at its reported prefix. A seal that meets no room is
// answered so, with nothing sealed, and the same decision seals it when
// asked again. `ready(data)` is asked before the segment's second group is
// written and says whether that write, and the seal's first after it, were
// set to fail for want of space; otherwise the second group is durable and
// only the failure is told.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer,
  typename Ready>
seastar::future<> running_seal_without_room(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer,
  Ready ready) {
    co_await with_placed_table<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      configuration(),
      nullptr,
      [&](auto& table, auto& append, auto& work) -> seastar::future<> {
          const auto a = appends::context(1);
          const auto first = segments::logical_origin().value();
          const auto target = take(
            co_await drive.lifecycle(table.create(creation(spec, 1), work)));
          co_await append_one<Clock>(
            append, target, 100, 1, budget, work, drive, timer);
          const auto data = take(
            take(local_paths::make(spec.root))
              .segment_file(
                0, {a.segment(), a.generation()}, local_segment_file::data));
          const bool armed = ready(data);
          const auto second = co_await appends::settle(
            append.append(
              target,
              co_await appends::request(
                appends::child_wire(101, 1), budget, work),
              work),
            drive);
          require(
            armed != (second.status == local_append_status::durable),
            "a group whose write met no space was durable, or one with room "
            "was not");
          // Its owner of segments fails it, as the append owner's failure
          // sink reports the write that met no space.
          runtime::first_failure space;
          space.observe(
            runtime::make_file_error(
              errc::io_failure, runtime::file_failure_detail::no_space));
          static_cast<void>(table.fail(a, space));
          require(
            co_await eventually<Clock>(
              drive,
              timer,
              [&] {
                  const auto left = table.failed_boundary(a);
                  return left && left->released;
              }),
            "a failed segment did not leave the append owner");
          const auto left = *table.failed_boundary(a);
          require(
            left.kind == segment_failure_kind::space && left.sealable,
            "a segment that met no space did not fail for want of it");
          auto sealed = co_await drive.lifecycle(
            table.seal_failed(a, *left.sealable, 1));
          if (armed) {
              require(
                sealed.room_wanted && sealed.failure.failed()
                  && table.life(a) == segment_life::failed,
                "a seal that met no space was not answered so");
              sealed = co_await drive.lifecycle(
                table.seal_failed(a, *left.sealable, 1));
          }
          take(sealed.failure.outcome());
          require(
            !sealed.room_wanted && sealed.sealed && sealed.expected
              && table.life(a) == segment_life::sealed,
            "a segment that failed for want of space was not sealed on its "
            "device");
          auto session = take(
            co_await drive.lifecycle(
              table.open_session(a, sight(first + 64), budget, work)));
          const auto read = co_await read_all(session, first, drive);
          require(
            (armed ? begins(read, {100}) : begins(read, {100, 101}))
              && read.stop == segment_read_stop::end_of_segment,
            "a segment sealed after it met no space does not read to its "
            "reported end");
      },
      byte_count{0});
}

// Sixty-four writable segments, as many as the table takes. Each holds two
// handle credits, its data file's and its completion's, and none of a
// seal's grants: every seal slot's were taken when the table was made, and
// a writable segment holds less memory than one slot's.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> writable_footprint(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    const auto config = configuration();
    const std::uint64_t writable = config.maximum_writable;
    co_await with_segment_table<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      config,
      [&](auto& table, auto&, auto& work) -> seastar::future<> {
          const auto before = budget.snapshot();
          for (std::uint64_t generation = 1; generation <= writable;
               ++generation)
              take(
                co_await drive.lifecycle(
                  table.create(creation(spec, generation), work)));
          const auto after = budget.snapshot();
          const auto limits = store_contract::limits();
          const auto slot = 3 * limits.operation_bytes.value()
                            + 2 * limits.execution_bytes.value() + 32_KiB;
          require(
            after.handles - before.handles == 2 * writable
              && (after.bytes - before.bytes) / writable < slot,
            "a writable segment held more than its data file's and its "
            "completion's credits, or a seal's memory");
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
          co_await append_one<Clock>(
            append, target, 100, 1, budget, work, drive, timer);
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

          // Requests still in flight when it fails, each with a work
          // account of its own kept until its acceptance is ready.
          struct offered final {
              seastar::future<local_append_acceptance> accepted;
              seastar::future<local_append_outcome> result;
          };
          seastar::abort_source abort;
          std::vector<offered> pending;
          pending.reserve(3);
          for (const std::uint64_t logical : {101U, 102U, 103U}) {
              auto own = std::make_unique<codec::cooperative_work>(
                codec::limits::defaults(), abort);
              auto stages = append.append(
                target,
                co_await appends::request(
                  appends::child_wire(logical), budget, *own),
                *own);
              pending.push_back(
                {std::move(stages.accepted)
                   .finally([kept = std::move(own)] noexcept {}),
                 std::move(stages.result)});
          }

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
          // Once it has left the append owner, a request it published lies
          // within the prefix it reports; one above that prefix is
          // uncertain or not written, never durable. Leaving settles what
          // it had accepted, so with no write failing they all end inside
          // it: a request above it needs a write that fails.
          require(
            co_await eventually<Clock>(
              drive,
              timer,
              [&] {
                  const auto left = table.failed_boundary(a);
                  return left && left->released;
              }),
            "a failed segment did not leave the append owner");
          const auto left = *table.failed_boundary(a);
          require(
            left.reported.has_value(),
            "a failed segment that took appends reported nothing");
          const auto reported_end = left.reported->covered.logical().end();
          for (auto& one : pending) {
              static_cast<void>(
                co_await drive.lifecycle(std::move(one.accepted)));
              const auto outcome = co_await drive.lifecycle(
                std::move(one.result));
              require(
                outcome.status == local_append_status::durable
                  ? outcome.receipt
                      && outcome.receipt->block.records.logical().end()
                           <= reported_end
                  : outcome.status == local_append_status::uncertain
                      || outcome.status == local_append_status::not_written,
                "a request above a failed segment's reported prefix was "
                "durable");
          }

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

// Requests in flight in their segment when its write fails. Placed in the
// segment alone, nothing else makes them durable, so once the failed
// segment has left the append owner one it published lies within the
// prefix it reports and the others are uncertain or not written, never
// durable. `ready` is told the segment's data file just before the
// requests are offered and answers whether its caller's environment fails
// that file's next write; with no failure armed every request ends inside
// the prefix.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer,
  typename Ready>
seastar::future<> failed_write_above_prefix(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer,
  Ready ready) {
    co_await with_placed_table<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      configuration(),
      nullptr,
      [&](auto& table, auto& append, auto& work) -> seastar::future<> {
          const auto a = appends::context(1);
          const auto target = take(
            co_await drive.lifecycle(table.create(creation(spec, 1), work)));
          co_await append_one<Clock>(
            append, target, 100, 1, budget, work, drive, timer);
          const auto paths = take(local_paths::make(spec.root));
          const bool armed = ready(take(paths.segment_file(
            0, {a.segment(), a.generation()}, local_segment_file::data)));

          // Each with a work account of its own, kept until its acceptance
          // is ready.
          struct offered final {
              seastar::future<local_append_acceptance> accepted;
              seastar::future<local_append_outcome> result;
          };
          seastar::abort_source abort;
          std::vector<offered> pending;
          pending.reserve(3);
          for (const std::uint64_t logical : {101U, 102U, 103U}) {
              auto own = std::make_unique<codec::cooperative_work>(
                codec::limits::defaults(), abort);
              auto stages = append.append(
                target,
                co_await appends::request(
                  appends::child_wire(logical), budget, *own),
                *own);
              pending.push_back(
                {std::move(stages.accepted)
                   .finally([kept = std::move(own)] noexcept {}),
                 std::move(stages.result)});
          }
          std::vector<local_append_outcome> outcomes;
          outcomes.reserve(pending.size());
          for (auto& one : pending) {
              static_cast<void>(
                co_await drive.lifecycle(std::move(one.accepted)));
              outcomes.push_back(
                co_await drive.lifecycle(std::move(one.result)));
          }

          // Its owner of segments fails it, unless it already has.
          if (table.life(a) == segment_life::writable) {
              runtime::first_failure why;
              why.observe(detail::path_error(errc::io_failure));
              table.fail(a, why);
          }
          require(
            co_await eventually<Clock>(
              drive,
              timer,
              [&] {
                  const auto left = table.failed_boundary(a);
                  return left && left->released;
              }),
            "a failed segment did not leave the append owner");
          const auto left = *table.failed_boundary(a);
          require(
            left.reported.has_value(),
            "a failed segment that took appends reported nothing");
          const auto reported_end = left.reported->covered.logical().end();
          std::size_t above = 0;
          for (const auto& outcome : outcomes) {
              require(
                outcome.status == local_append_status::durable
                  ? outcome.receipt
                      && outcome.receipt->block.records.logical().end()
                           <= reported_end
                  : outcome.status == local_append_status::uncertain
                      || outcome.status == local_append_status::not_written,
                "a request above a failed segment's reported prefix was "
                "durable");
              if (outcome.status != local_append_status::durable) ++above;
          }
          require(
            armed == (above != 0),
            armed ? "a failed write left no request above the prefix"
                  : "a request failed though no write did");
      },
      byte_count{0});
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
              co_await append_one<Clock>(
                append, first, logical, 1, budget, work, drive, timer);
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
          co_await append_one<Clock>(
            append, second, 100, 2, budget, work, drive, timer);
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
          co_await append_one<Clock>(
            append, third, 100, 3, budget, work, drive, timer);
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
              co_await append_one<Clock>(
                append, target, logical, 1, budget, work, drive, timer);

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
          const auto first = segments::logical_origin().value();
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
              co_await append_one<Clock>(
                append, target, logical, 1, budget, work, drive, timer);
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
              co_await append_one<Clock>(
                append, target, logical, 1, budget, work, drive, timer);
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
          co_await append_one<Clock>(
            append, next, 100, 2, budget, work, drive, timer);

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
