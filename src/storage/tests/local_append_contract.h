#pragma once

#include "src/base/units.h"
#include "src/codec/digest.h"
#include "src/codec/xxh3.h"
#include "src/storage/local_append.h"
#include "src/storage/tests/segment_writer_contract.h"
#include "src/storage/tests/wal_writer_contract.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/preempt.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/defer.hh>

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace kwaque::storage::testing::local_append_contract {
using store_contract::require;
using store_contract::take;
namespace installation = installation_contract;
namespace segment = segment_writer_contract;

// An independent assigned child bound to generation: the request sequence and
// logical base follow logical, and the semantic digest is recomputed over the
// bound submitted fields, as the footer fixtures do.
inline std::string
child_wire(std::uint64_t logical, std::uint64_t generation = 1) {
    auto child = assigned_wire();
    put(child, 32 + 96, generation, 8);
    put(child, 32 + 32, logical - 99U, 8);
    codec::xxh3_128_hasher hash;
    hash.update(
      codec::semantic_batch_domain.data(), codec::semantic_batch_domain.size());
    hash.update(child.data() + 32, 104);
    hash.update(child.data() + 32 + 136, 12);
    const auto original = hex("06000000010000");
    hash.update(original.data(), original.size());
    const auto digest = std::move(hash).final();
    for (std::size_t i = 0; i < digest.size(); ++i)
        child[32 + 104 + i] = std::bit_cast<char>(digest[i]);
    put(child, 32 + 168, logical, 8);
    put(child, 32 + 176, logical + 1U, 8);
    repair(child);
    return child;
}

inline segment_context context(std::uint64_t generation) {
    const auto base = installation::segment();
    return segment_context::make(
             base.cluster(),
             base.topic(),
             base.range(),
             base.segment(),
             model::segment_generation::make(generation).value())
      .value();
}

inline model::range_routing_epoch routing() {
    return model::range_routing_epoch::make(1).value();
}

template<typename Budget>
seastar::future<local_append_request>
request(std::string wire, Budget& resources, codec::cooperative_work& work) {
    auto raw = co_await installation::buffer_async(std::move(wire));
    auto batch = co_await validate_encoded_assigned_batch(
      std::move(raw), batch_expected(), budget(), work);
    require(batch.has_value(), "independent request child rejected");
    auto backing = take(resources.try_reserve_buffer(batch->bytes()));
    co_return local_append_request{
      std::move(*batch), std::move(backing), batch_expected(), routing(), {}};
}

struct options final {
    std::uint32_t outstanding{2};
    std::uint32_t segments{1};
    byte_count wal_capacity{1_MiB};
    // Segments a test fences on purpose: their close may report that failure.
    std::uint32_t fenced{0};
    // Records each storage failure the owner reports, from a sink that also
    // reads the owner back.
    std::vector<local_storage_failure>* failures{nullptr};
    // The test fails the WAL or crashes the device on purpose: every owner's
    // close may report that failure.
    bool tolerate_close{false};
    // Bound to the group commit's shutdown, so a test can stop the WAL first.
    seastar::abort_source* wal_shutdown{nullptr};
    // Every segment's alignment, independent of the WAL's 8 KiB.
    std::uint64_t segment_alignment{8192};
    // The most WAL files the shard may hold; zero does not bound them.
    std::uint32_t retained_files{0};
    // Records each rotation the retained limit refused, as the owner
    // reports it.
    std::vector<local_retention_pressure>* pressure{nullptr};
    // Funds the control owner and its allocator, apart from the appends.
    workload_budget* control_budget{nullptr};
    // Receives the handle credits the segments hold once all are created.
    std::uint64_t* segment_handles{nullptr};
};

// The durable local path: one WAL writer with its group commit, one or two
// segment writers and the local append owner, all closed in reverse order.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer,
  typename Func>
seastar::future<> with_local_append(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer,
  options chosen,
  Func body) {
    using writer_type = wal_writer<Backend, Owner>;
    using control_type = typename writer_type::control_type;
    using allocator_type = typename writer_type::allocator_type;
    using segment_type = segment_writer<Backend, Owner, Clock>;
    using append_type = local_append<Backend, Owner, Clock>;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    std::unique_ptr<control_type> control;
    std::unique_ptr<allocator_type> ids;
    std::unique_ptr<writer_type> writer;
    std::unique_ptr<wal_group_commit> commit;
    std::array<std::unique_ptr<segment_type>, 2> segments;
    std::unique_ptr<append_type> append;
    runtime::first_failure failed;
    try {
        co_await installation::bootstrap(
          files, owner, spec, budget, work, drive);
        auto& controlling = chosen.control_budget ? *chosen.control_budget
                                                  : budget;
        control = take(
          co_await drive.lifecycle(
            control_type::open(
              files,
              owner,
              spec,
              0,
              false,
              controlling,
              store_contract::limits(),
              work)));
        ids = take(allocator_type::make(*control, controlling, 4));
        auto wal = wal_writer_contract::configuration();
        wal.capacity_bytes = chosen.wal_capacity;
        wal.retained_files = chosen.retained_files;
        writer = take(
          writer_type::make(
            *control, *ids, budget, wal, wal_start_intent::known_unactivated));
        take(co_await drive.lifecycle(writer->bootstrap(work)));
        wal_group_commit_config cohort;
        cohort.outstanding_groups = chosen.outstanding;
        cohort.maximum_wait = runtime::monotonic_duration{0};
        commit = take(wal_group_commit::make(*writer, budget, cohort));
        if (chosen.wal_shutdown)
            take(commit->bind_shutdown(*chosen.wal_shutdown));
        take(commit->template start<Clock>(timer));
        // As a shard does before it starts: the budget must be able to hold
        // every configured segment open at once.
        take(validate_segment_handles(
          budget, segment::configuration(), {.segments = chosen.segments}));
        const auto handles_before = budget.snapshot().handles;
        for (std::uint32_t i = 0; i != chosen.segments; ++i) {
            auto config = segment::configuration();
            config.retry_object = local_object_sequence::make(45 + i).value();
            config.admission.working_bytes = byte_count{1_MiB};
            auto description = segment::descriptor();
            description.segment = context(i + 1);
            description.alignment = alignment(chosen.segment_alignment);
            segments[i] = take(
              segment_type::make_new(
                files, owner, spec, 0, description, budget, config));
            take(co_await drive.lifecycle(segments[i]->create_new(work)));
        }
        if (chosen.segment_handles)
            *chosen.segment_handles = budget.snapshot().handles
                                      - handles_before;
        local_failure_sink sink;
        if (chosen.failures) {
            chosen.failures->reserve(4);
            sink = [failures = chosen.failures,
                    &append](const local_storage_failure& failure) noexcept {
                // A reentrant read sees the failure already latched.
                if (append->storage_failure()) failures->push_back(failure);
            };
        }
        local_retention_sink refused;
        if (chosen.pressure) {
            chosen.pressure->reserve(8);
            refused = [reports = chosen.pressure](
                        const local_retention_pressure& report) noexcept {
                reports->push_back(report);
            };
        }
        append = take(
          append_type::make(
            budget, *commit, *writer, {}, std::move(sink), std::move(refused)));
        std::array<std::optional<local_append_target>, 2> targets;
        for (std::uint32_t i = 0; i != chosen.segments; ++i)
            targets[i] = take(append->attach(*segments[i]));
        // A body may also take the control owner the WAL writer updates,
        // and the allocator beside it.
        if constexpr (
          std::invocable<
            Func&,
            append_type&,
            writer_type&,
            decltype(segments)&,
            decltype(targets)&,
            codec::cooperative_work&,
            control_type&,
            allocator_type&>)
            co_await body(
              *append, *writer, segments, targets, work, *control, *ids);
        else if constexpr (
          std::invocable<
            Func&,
            append_type&,
            writer_type&,
            decltype(segments)&,
            decltype(targets)&,
            codec::cooperative_work&,
            control_type&>)
            co_await body(*append, *writer, segments, targets, work, *control);
        else
            co_await body(*append, *writer, segments, targets, work);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    const auto close = [&](auto operation) -> seastar::future<> {
        try {
            auto closed = co_await drive.lifecycle(std::move(operation));
            if (!chosen.tolerate_close) failed.observe(closed);
        } catch (...) {
            failed.observe(std::current_exception());
        }
    };
    if (append) co_await close(append->close());
    append.reset();
    for (std::uint32_t i = 0; i != segments.size(); ++i) {
        auto& owned = segments[i];
        if (!owned) continue;
        if ((chosen.fenced & (1U << i)) != 0) {
            // Its latched failure is the fence the test injected.
            try {
                static_cast<void>(co_await drive.lifecycle(owned->close()));
            } catch (...) {
                failed.observe(std::current_exception());
            }
        } else {
            co_await close(owned->close());
        }
        owned.reset();
    }
    if (commit) co_await close(commit->close());
    commit.reset();
    if (writer) co_await close(writer->close());
    writer.reset();
    if (ids) co_await close(ids->close());
    ids.reset();
    if (control) co_await close(control->close());
    control.reset();
    take(failed.outcome());
}

// What a shard checks before it starts, and the count that check rests on.
// Two created segments hold exactly the handle credits
// segment_writer_handles() gives for their configuration, so the count is
// the code's and not an estimate. The budget is then accepted for every
// number of segments it can hold beside a reserve and what those in creation
// need, and refused for one more with its limit and what was expected of it.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> segment_handle_budget(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    std::uint64_t held = 0;
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.segments = 2, .segment_handles = &held},
      [](auto&, auto&, auto&, auto&, auto&) -> seastar::future<> {
          co_return;
      });
    const auto config = segment::configuration();
    const auto each = segment_writer_handles(config);
    auto zero_written = config;
    zero_written.preallocation_bytes = byte_count{64_KiB};
    require(
      held == 2 * std::uint64_t{each}
        && segment_writer_handles(zero_written) == each + 1,
      "created segments do not hold the handle credits their "
      "configuration is counted for");
    const auto limit = budget.limits().handles;
    const auto creating = segment_creation_handles;
    require(
      limit >= creating + 2 * each, "the handle budget holds no two segments");
    const auto most = (limit - creating) / each;
    const auto spare = (limit - creating) % each;
    const auto refused = [&](segment_handle_demand demand) {
        const auto checked = validate_segment_handles(budget, config, demand);
        if (
          checked || checked.error().code() != errc::resource_exhausted
          || checked.error().context_size() != 2)
            return false;
        const auto has = *checked.error().context_at(0);
        const auto wants = *checked.error().context_at(1);
        return has.key == runtime::operation_context_key::limit
               && has.value == limit
               && wants.key == runtime::operation_context_key::expected
               && wants.value
                    == std::uint64_t{demand.segments} * each
                         + std::uint64_t{std::min(
                             demand.creating, demand.segments)}
                             * creating
                         + demand.reserved;
    };
    require(
      validate_segment_handles(budget, config, {.reserved = limit})
        && validate_segment_handles(
          budget, config, {.segments = most, .reserved = spare}),
      "a segment count the handle budget holds was refused");
    const auto all = std::numeric_limits<std::uint32_t>::max();
    require(
      refused({.segments = most + 1}) && refused({.reserved = limit + 1})
        && refused({.segments = most, .reserved = spare + 1})
        && refused({.segments = most, .creating = 2, .reserved = spare})
        && refused({.segments = all, .creating = all, .reserved = all}),
      "a segment count the handle budget cannot hold was accepted");
}

template<typename Driver>
seastar::future<local_append_outcome>
settle(local_append_stages stages, Driver drive) {
    auto accepted = co_await drive.lifecycle(std::move(stages.accepted));
    require(accepted.accepted(), "valid request was not accepted");
    co_return co_await drive.lifecycle(std::move(stages.result));
}
inline const local_append_receipt&
durable(const local_append_outcome& outcome) {
    require(
      outcome.status == local_append_status::durable && outcome.receipt
        && !outcome.failure.failed(),
      "accepted request did not become locally durable");
    return *outcome.receipt;
}

// The expected PREPARE, built independently at a WAL position and naming the
// member's final block. The WAL is 8-KiB aligned; a segment's header fills its
// first alignment unit.
inline std::string prepare_at(
  const std::string& child,
  model::wal_incarnation_id incarnation,
  std::uint64_t wal_position,
  std::uint64_t generation,
  const segment_block_layout& block,
  std::uint64_t segment_alignment = 8192) {
    return wal_wire(
      child,
      {wal_write_context::make(
         incarnation, alignment(8192), runtime::file_position{wal_position})
         .value(),
       segment_write_context::make(
         context(generation),
         alignment(segment_alignment),
         block.records.physical().begin(),
         block.records.bytes().begin())
         .value(),
       runtime::file_position{segment_alignment},
       routing(),
       batch_expected()});
}
inline std::string block_at(
  const std::string& child,
  std::uint64_t generation,
  const segment_block_layout& block,
  std::uint64_t segment_alignment = 8192) {
    return block_wire(
      child,
      {segment_write_context::make(
         context(generation),
         alignment(segment_alignment),
         block.records.physical().begin(),
         block.records.bytes().begin())
         .value(),
       runtime::file_position{segment_alignment},
       batch_expected()});
}

inline runtime::file_path
wal_path(const local_device_spec& spec, model::wal_incarnation_id incarnation) {
    return take(take(local_paths::make(spec.root)).wal(0, incarnation));
}
template<typename Backend, typename Driver>
seastar::future<std::string> read_wal(
  Backend& files,
  const local_device_spec& spec,
  model::wal_incarnation_id incarnation,
  Driver drive) {
    co_return co_await store_contract::read_all_bytes(
      files, wal_path(spec, incarnation), drive);
}

// Runs the environment until a virtual time, observing state in between.
template<typename Driver, typename Timer>
seastar::future<>
probe(Driver drive, Timer& timer, runtime::monotonic_time until) {
    seastar::abort_source never;
    take(co_await drive.lifecycle(timer.sleep_until(until, never)));
}

// Settled requests keep charges until background work ends: the append
// owner's group task releases its members after publishing them, and the
// segment writer's deferred digest releases a written group's memory. Waits
// for both, so a charge comparison starts from an idle owner.
template<
  runtime::monotonic_clock Clock,
  typename Append,
  typename Segment,
  typename Driver,
  typename Timer>
seastar::future<>
retired(Append& append, Segment& segment, Driver drive, Timer& timer) {
    constexpr runtime::monotonic_duration poll{10'000'000};
    while (append.groups() != 0)
        co_await probe(drive, timer, Clock::now().checked_add(poll).value());
    take(co_await drive.lifecycle(segment.digest_caught_up()));
}
// The data file of the fixture segment with this generation.
inline runtime::file_path
segment_path(const local_device_spec& spec, std::uint64_t generation) {
    return take(take(local_paths::make(spec.root))
                  .segment_file(
                    0,
                    {installation::segment().segment(),
                     model::segment_generation::make(generation).value()},
                    local_segment_file::data));
}
template<typename Backend, typename Driver>
seastar::future<std::string> read_segment(
  Backend& files,
  const local_device_spec& spec,
  std::uint64_t generation,
  Driver drive) {
    co_return co_await store_contract::read_all_bytes(
      files, segment_path(spec, generation), drive);
}

// One request becomes locally durable: its PREPARE names the frozen block
// position, the block and footer are written there, and both receipts cover
// it only after both barriers. The segment's alignment is independent of the
// WAL's; a different one shows each is used where it belongs.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> durable_append(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer,
  std::uint64_t segment_alignment = 8192) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.segment_alignment = segment_alignment},
      [&](auto& append, auto& writer, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          const auto empty = segments[0]->progress()->reserved;
          const auto wal_start = writer.progress()->reserved;
          const auto wire = child_wire(100);
          auto outcome = co_await settle(
            append.append(
              *targets[0], co_await request(wire, budget, work), work),
            drive);
          const auto& receipt = durable(outcome);
          const auto& block = receipt.block.records;
          const auto cut = receipt.segment.boundary();
          require(
            block.bytes().begin() == empty.bytes
              && block.physical().begin() == empty.physical && cut.footer()
              && cut.footer()->begin() == block.bytes().end()
              && cut.end().blocks == 1 && cut.end().footers == 1
              && segments[0]->progress()->durable == cut.end(),
            "request was not written at the frozen block with its footer");
          require(
            receipt.wal.boundary().cursor().position() > wal_start.position()
              && writer.progress()->durable.position()
                   >= receipt.wal.boundary().cursor().position(),
            "WAL receipt does not cover the request's PREPARE");
          require(
            receipt.batch.context.logical_span().begin()
              == model::range_logical_end{100},
            "result lost the original logical span");
          const auto incarnation = writer.prepared_head()->incarnation;
          const auto stored = co_await read_wal(
            files, spec, incarnation, drive);
          const auto expected = prepare_at(
            wire,
            incarnation,
            wal_start.position().value(),
            1,
            receipt.block,
            segment_alignment);
          require(
            stored.substr(wal_start.position().value(), expected.size())
              == expected,
            "PREPARE differs from the independent envelope at its final "
            "target");
          const auto data = co_await read_segment(files, spec, 1, drive);
          const auto placed = block_at(
            wire, 1, receipt.block, segment_alignment);
          require(
            data.substr(block.bytes().begin().value(), placed.size()) == placed,
            "segment block differs from the independent envelope");
      });
}

// A group forms when the WAL can take it: requests accepted while the only
// outstanding group is in flight form the next group together, under one
// footer. deterministic is false where I/O may overtake acceptance.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> occupancy_batching(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer,
  bool deterministic) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 1},
      [&](auto& append, auto&, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          auto first = append.append(
            *targets[0], co_await request(child_wire(100), budget, work), work);
          require(
            (co_await drive.lifecycle(std::move(first.accepted))).accepted(),
            "first request rejected");
          std::vector<local_append_stages> later;
          for (std::uint64_t logical : {101U, 102U})
              later.push_back(append.append(
                *targets[0],
                co_await request(child_wire(logical), budget, work),
                work));
          for (auto& stages : later)
              require(
                (co_await drive.lifecycle(std::move(stages.accepted)))
                  .accepted(),
                "later request rejected");
          const auto head = durable(
            co_await drive.lifecycle(std::move(first.result)));
          std::vector<local_append_outcome> tail;
          for (auto& stages : later)
              tail.push_back(
                co_await drive.lifecycle(std::move(stages.result)));
          const auto& second = durable(tail[0]);
          const auto& third = durable(tail[1]);
          require(
            second.block.records.bytes().begin()
                == head.segment.boundary().end().bytes
              && third.block.records.bytes().begin()
                   >= second.block.records.bytes().end()
              && segments[0]->progress()->durable.blocks == 3,
            "grouped requests lost order or position");
          if (deterministic)
              require(
                second.segment.boundary() == third.segment.boundary()
                  && third.block.records.bytes().begin()
                       == second.block.records.bytes().end()
                  && segments[0]->progress()->durable.footers == 2,
                "requests waiting on the in-flight group did not share a "
                "footer");
      });
}

// Members of one group across two segments keep acceptance order in the WAL
// (A1, B1, A2); each segment freezes its own contiguous blocks and footer.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> multi_segment_group(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 1, .segments = 2},
      [&](auto& append, auto& writer, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          const auto other = segments[1]->progress()->reserved;
          auto blocker = append.append(
            *targets[0], co_await request(child_wire(100), budget, work), work);
          require(
            (co_await drive.lifecycle(std::move(blocker.accepted))).accepted(),
            "blocking request rejected");
          const std::array wires{
            child_wire(101), child_wire(100, 2), child_wire(102)};
          const std::array<std::uint32_t, 3> owners{0, 1, 0};
          // Requests join in call order whatever each one's validation
          // costs; the WAL order below checks it.
          std::vector<local_append_stages> grouped;
          for (std::size_t i = 0; i != wires.size(); ++i)
              grouped.push_back(append.append(
                *targets[owners[i]],
                co_await request(wires[i], budget, work),
                work));
          for (auto& stages : grouped)
              require(
                (co_await drive.lifecycle(std::move(stages.accepted)))
                  .accepted(),
                "grouped request rejected");
          const auto first = durable(
            co_await drive.lifecycle(std::move(blocker.result)));
          std::vector<local_append_outcome> results;
          for (auto& stages : grouped)
              results.push_back(
                co_await drive.lifecycle(std::move(stages.result)));
          const auto& a1 = durable(results[0]);
          const auto& b1 = durable(results[1]);
          const auto& a2 = durable(results[2]);
          require(
            a1.segment.boundary() == a2.segment.boundary()
              && a2.block.records.bytes().begin()
                   == a1.block.records.bytes().end()
              && a1.block.records.bytes().begin()
                   == first.segment.boundary().end().bytes
              && b1.block.records.bytes().begin() == other.bytes
              && b1.segment.boundary().end().footers == 1
              && segments[1]->progress()->durable
                   == b1.segment.boundary().end(),
            "a segment's members did not freeze contiguously under one "
            "footer");
          // After the blocker's PREPARE: A1, B1, A2 in acceptance order.
          const auto incarnation = writer.prepared_head()->incarnation;
          const auto stored = co_await read_wal(
            files, spec, incarnation, drive);
          const auto blocked = prepare_at(
            child_wire(100), incarnation, 8192, 1, first.block);
          auto at = 8192 + blocked.size();
          const std::array<const local_append_receipt*, 3> ordered{
            &a1, &b1, &a2};
          const std::array<std::uint64_t, 3> generations{1, 2, 1};
          const std::array<const char*, 3> misplaced{
            "WAL member A1 is not first after the blocker or lost its target",
            "WAL member B1 is not second or lost its final target",
            "WAL member A2 is not third or lost its final target"};
          for (std::size_t i = 0; i != 3; ++i) {
              const auto expected = prepare_at(
                wires[i], incarnation, at, generations[i], ordered[i]->block);
              require(
                stored.substr(at, expected.size()) == expected, misplaced[i]);
              at += expected.size();
          }
      });
}

// Rejections before acceptance leave no PREPARE, segment byte or charge; the
// next valid request is written where the rejected ones would have been.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> rejections_have_no_effect(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {},
      [&](auto& append, auto& writer, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          const auto segment_before = segments[0]->progress()->reserved;
          const auto wal_before = writer.progress()->reserved;
          const auto charged = budget.snapshot();
          const auto rejected =
            [&](local_append_stages stages, errc code) -> seastar::future<> {
              auto accepted = co_await drive.lifecycle(
                std::move(stages.accepted));
              auto outcome = co_await drive.lifecycle(std::move(stages.result));
              require(
                !accepted.accepted() && accepted.failure.error()
                  && accepted.failure.error()->code() == code
                  && outcome.status == local_append_status::not_written
                  && !outcome.receipt,
                "invalid request was accepted or changed its outcome");
          };
          {
              auto wrong = co_await request(child_wire(100), budget, work);
              wrong.physical_begin = model::segment_relative_end{7};
              co_await rejected(
                append.append(*targets[0], std::move(wrong), work),
                errc::wrong_context);
          }
          co_await rejected(
            append.append(
              *targets[0],
              co_await request(child_wire(105), budget, work),
              work),
            errc::wrong_context);
          {
              seastar::abort_source stop;
              stop.request_abort();
              codec::cooperative_work cancelled{
                codec::limits::defaults(), stop};
              auto input = co_await request(child_wire(100), budget, work);
              co_await rejected(
                append.append(*targets[0], std::move(input), cancelled),
                errc::aborted);
          }
          {
              auto config = codec::limits::defaults().config();
              config.max_buffer_fragments = item_count{4};
              seastar::abort_source stop;
              codec::cooperative_work narrow{
                codec::limits::make(config).value(), stop};
              auto input = co_await request(child_wire(100), budget, work);
              co_await rejected(
                append.append(*targets[0], std::move(input), narrow),
                errc::invalid_argument);
          }
          require(
            segments[0]->progress()->reserved == segment_before
              && writer.progress()->reserved == wal_before
              && budget.snapshot().bytes == charged.bytes
              && budget.snapshot().tasks == charged.tasks
              && !segments[0]->failure().failed() && append.forming() == 0,
            "a rejected request left positions, bytes or charges behind");
          auto outcome = co_await settle(
            append.append(
              *targets[0],
              co_await request(child_wire(100), budget, work),
              work),
            drive);
          require(
            durable(outcome).block.records.bytes().begin()
              == segment_before.bytes,
            "rejections moved the next request's position");
          // An idle attachment detaches; its handle then names nothing.
          const auto stale = *targets[0];
          take(append.detach(stale));
          targets[0] = take(append.attach(*segments[0]));
          require(!(*targets[0] == stale), "reattachment reused a handle");
          co_await rejected(
            append.append(
              stale, co_await request(child_wire(101), budget, work), work),
            errc::wrong_context);
      });
}

// WAL-side pressure after acceptance is found before any segment position
// freezes: the request completes not_written, the segment is not fenced and
// the next request is written with no hole. Needs I/O that acceptance cannot
// overtake.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> pressure_before_freeze(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 1},
      [&](auto& append, auto& writer, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          auto first = append.append(
            *targets[0], co_await request(child_wire(100), budget, work), work);
          require(
            (co_await drive.lifecycle(std::move(first.accepted))).accepted(),
            "first request rejected");
          auto waiting = append.append(
            *targets[0], co_await request(child_wire(101), budget, work), work);
          require(
            (co_await drive.lifecycle(std::move(waiting.accepted))).accepted(),
            "forming request rejected");
          // Fill the budget in a few reservations. What the in-flight group
          // returns stays below the WAL writer's validation working set.
          std::vector<workload_reservation> squeeze;
          auto amount = budget.limits().bytes.value();
          while (amount >= 65536 && squeeze.size() != 16) {
              auto held = budget.try_reserve(byte_count{amount});
              if (held)
                  squeeze.push_back(std::move(*held));
              else
                  amount /= 2;
          }
          require(
            !budget.try_reserve(
              byte_count{writer.child_limits().working_bytes.value() / 2}),
            "fixture could not create WAL-side pressure");
          const auto head = durable(
            co_await drive.lifecycle(std::move(first.result)));
          auto pressured = co_await drive.lifecycle(std::move(waiting.result));
          require(
            pressured.status == local_append_status::not_written
              && pressured.failure.error()
              && pressured.failure.error()->code() == errc::queue_full
              && !segments[0]->failure().failed()
              && segments[0]->progress()->reserved
                   == head.segment.boundary().end(),
            "WAL pressure after acceptance froze or fenced the segment");
          squeeze.clear();
          auto next = co_await settle(
            append.append(
              *targets[0],
              co_await request(child_wire(101), budget, work),
              work),
            drive);
          require(
            durable(next).block.records.bytes().begin()
              == head.segment.boundary().end().bytes,
            "pressure left a hole or fenced later appends");
      });
}

// A group that fits only a fresh WAL file rotates before any position
// freezes; every request stays durable and no segment is fenced.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> rotation_before_freeze(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.wal_capacity = byte_count{24_KiB}},
      [&](
        auto& append,
        auto& writer,
        auto& segments,
        auto& targets,
        auto& work,
        auto& control) -> seastar::future<> {
          const auto initial = writer.progress()->reserved.incarnation();
          std::optional<local_append_receipt> last;
          for (std::uint64_t logical = 100; logical != 104; ++logical) {
              auto outcome = co_await settle(
                append.append(
                  *targets[0],
                  co_await request(child_wire(logical), budget, work),
                  work),
                drive);
              const auto& receipt = durable(outcome);
              if (last)
                  require(
                    receipt.block.records.bytes().begin()
                      == last->segment.boundary().end().bytes,
                    "rotation left a segment hole");
              last = receipt;
          }
          require(
            writer.statistics().rotations != 0
              && !segments[0]->failure().failed()
              && segments[0]->progress()->durable.blocks == 4,
            "a full WAL file did not rotate before freezing");
          // The file left behind holds no open obligation; the current one is
          // still being written.
          const auto current = writer.progress()->reserved.incarnation();
          require(
            !(current == initial) && append.reclaimable(initial)
              && !append.reclaimable(current),
            "WAL reclamation ignored the file's state");
          // With every group durable, the discharged prefix and the durable
          // end are positions in the current file: nothing still names the
          // rotated file's end.
          const auto snapshot = take(
            append.obligations([](const local_obligation&) {}));
          require(
            snapshot.obligations == 0
              && snapshot.discharged == writer.progress()->reserved
              && snapshot.wal_durable == writer.progress()->durable
              && snapshot.wal_durable.incarnation() == current,
            "the discharged prefix stayed in a rotated WAL file");
          // The segment's newest durable footer is the last group's.
          std::vector<local_durable_boundary> newest;
          append.durable_boundaries(
            [&newest](const local_durable_boundary& one) {
                newest.push_back(one);
            });
          const auto& cut = last->segment.boundary();
          require(
            newest.size() == 1 && newest[0].device == spec.owner.device()
              && newest[0].history == cut.history()
              && newest[0].covered == cut.covered()
              && newest[0].footer == *cut.footer()
              && newest[0].footer.end()
                   == segments[0]->progress()->durable.bytes,
            "the newest durable footer was not the last group's");

          // A rotation that is only refused for pressure is retried, never
          // latched. Every turn of the control is taken: one edit in flight
          // and as many waiting as may wait. The request that needs the next
          // WAL file is then refused with no effect, and the owner stays
          // healthy.
          seastar::promise<runtime::result<void>> proceed;
          std::array<
            std::optional<seastar::future<local_publication_outcome>>,
            1 + maximum_control_waiters>
            edits;
          edits[0].emplace(control.update(
            [](local_shard_control&) -> runtime::result<void> { return {}; },
            [&proceed](const auto&, const auto&, auto&) {
                return proceed.get_future();
            },
            work));
          for (std::size_t i = 1; i != edits.size(); ++i)
              edits[i].emplace(control.update(
                [](local_shard_control&) -> runtime::result<void> {
                    return {};
                },
                work));
          runtime::first_failure failed;
          std::uint64_t logical = 104;
          try {
              require(
                control.waiting() == maximum_control_waiters,
                "control edits did not wait their turn");
              std::optional<local_append_outcome> refused;
              for (; logical != 112 && !refused; ++logical) {
                  auto outcome = co_await settle(
                    append.append(
                      *targets[0],
                      co_await request(child_wire(logical), budget, work),
                      work),
                    drive);
                  if (outcome.status == local_append_status::durable)
                      last = *outcome.receipt;
                  else
                      refused.emplace(std::move(outcome));
              }
              require(
                refused && refused->status == local_append_status::not_written
                  && refused->failure.error()
                  && refused->failure.error()->code() == errc::queue_full
                  && !append.failure().failed() && !writer.failure().failed()
                  && !append.storage_failure(),
                "a rotation refused for pressure failed the append owner");
          } catch (...) {
              failed.observe(std::current_exception());
          }
          proceed.set_value(runtime::result<void>{});
          for (auto& edit : edits) {
              try {
                  auto done = co_await drive.lifecycle(std::move(*edit));
                  failed.observe(done.failure.outcome());
              } catch (...) {
                  failed.observe(std::current_exception());
              }
              edit.reset();
          }
          take(failed.outcome());
          // With the control free again the same rotation resumes: the
          // refused batch is appended after the last durable one, in a new
          // WAL file.
          const auto rotations = writer.statistics().rotations;
          auto resumed = co_await settle(
            append.append(
              *targets[0],
              co_await request(child_wire(logical - 1), budget, work),
              work),
            drive);
          const auto& receipt = durable(resumed);
          require(
            receipt.block.records.bytes().begin()
                == last->segment.boundary().end().bytes
              && writer.statistics().rotations == rotations + 1
              && !append.failure().failed(),
            "a rotation refused for pressure did not resume");
      });
}

// Close stops acceptance and drains accepted requests through both barriers.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> close_drains_accepted(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 1},
      [&](auto& append, auto&, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          std::vector<local_append_stages> pending;
          for (std::uint64_t logical : {100U, 101U})
              pending.push_back(append.append(
                *targets[0],
                co_await request(child_wire(logical), budget, work),
                work));
          for (auto& stages : pending)
              require(
                (co_await drive.lifecycle(std::move(stages.accepted)))
                  .accepted(),
                "request rejected before close");
          auto closing = append.close();
          auto late = append.append(
            *targets[0], co_await request(child_wire(102), budget, work), work);
          const auto refused = co_await drive.lifecycle(
            std::move(late.accepted));
          take(co_await drive.lifecycle(std::move(closing)));
          for (auto& stages : pending)
              static_cast<void>(
                durable(co_await drive.lifecycle(std::move(stages.result))));
          static_cast<void>(co_await drive.lifecycle(std::move(late.result)));
          require(
            !refused.accepted() && refused.failure.error()
              && refused.failure.error()->code() == errc::closed
              && segments[0]->progress()->durable.blocks == 2,
            "close accepted new work or did not drain accepted requests");
      });
}

// Groups submitted while a segment's barrier runs are covered together by
// its next barrier: one receipt, while each group keeps its own footer.
// Needs I/O that acceptance cannot overtake.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> barrier_coalescing(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 3},
      [&](auto& append, auto&, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          // Each request forms its own group before the next arrives.
          std::vector<local_append_stages> staged;
          for (std::uint64_t logical : {100U, 101U, 102U}) {
              staged.push_back(append.append(
                *targets[0],
                co_await request(child_wire(logical), budget, work),
                work));
              require(
                (co_await drive.lifecycle(std::move(staged.back().accepted)))
                  .accepted(),
                "request rejected");
          }
          std::vector<local_append_outcome> outcomes;
          for (auto& stages : staged)
              outcomes.push_back(
                co_await drive.lifecycle(std::move(stages.result)));
          const auto& first = durable(outcomes[0]);
          const auto& second = durable(outcomes[1]);
          const auto& third = durable(outcomes[2]);
          require(
            second.block.records.bytes().begin()
                == first.segment.boundary().end().bytes
              && third.block.records.bytes().begin()
                   > second.block.records.bytes().end()
              && !(first.segment.boundary() == second.segment.boundary())
              && second.segment.boundary() == third.segment.boundary()
              && third.segment.boundary().end().blocks == 3
              && third.segment.boundary().end().footers == 3
              && segments[0]->progress()->durable
                   == third.segment.boundary().end(),
            "groups submitted during a barrier were not covered by one "
            "barrier");
      });
}

// One WAL group across two segments whose second segment's barrier flush
// fails: the first segment's members stay durable, the failed one is
// uncertain and fenced, and later requests there are refused. The failure is
// reported once, classified, and its obligation stays pinned in the WAL file.
// The caller's environment fails that segment's next flush.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> independent_segment_outcomes(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    std::vector<local_storage_failure> failures;
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 1, .segments = 2, .fenced = 2, .failures = &failures},
      [&](auto& append, auto& writer, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          auto blocker = append.append(
            *targets[0], co_await request(child_wire(100), budget, work), work);
          require(
            (co_await drive.lifecycle(std::move(blocker.accepted))).accepted(),
            "blocking request rejected");
          // A1 and B1 form one group behind the blocker.
          auto a1 = append.append(
            *targets[0], co_await request(child_wire(101), budget, work), work);
          auto b1 = append.append(
            *targets[1],
            co_await request(child_wire(100, 2), budget, work),
            work);
          for (auto* stages : {&a1, &b1})
              require(
                (co_await drive.lifecycle(std::move(stages->accepted)))
                  .accepted(),
                "grouped request rejected");
          const auto head = durable(
            co_await drive.lifecycle(std::move(blocker.result)));
          const auto kept = co_await drive.lifecycle(std::move(a1.result));
          const auto lost = co_await drive.lifecycle(std::move(b1.result));
          const auto& a = durable(kept);
          require(
            a.block.records.bytes().begin()
                == head.segment.boundary().end().bytes
              && a.wal.boundary().cursor().position()
                   <= writer.progress()->durable.position()
              && !segments[0]->failure().failed(),
            "a failed segment revoked another segment's durable member");
          require(
            lost.status == local_append_status::uncertain && !lost.receipt
              && lost.failure.failed() && segments[1]->failure().failed(),
            "a failed segment barrier did not leave its member uncertain");
          // The failure is sticky for that segment only.
          const auto later = co_await settle(
            append.append(
              *targets[0],
              co_await request(child_wire(102), budget, work),
              work),
            drive);
          require(
            durable(later).block.records.bytes().begin()
              == a.segment.boundary().end().bytes,
            "the healthy segment did not continue after the failure");
          auto refused = append.append(
            *targets[1],
            co_await request(child_wire(101, 2), budget, work),
            work);
          const auto decision = co_await drive.lifecycle(
            std::move(refused.accepted));
          const auto outcome = co_await drive.lifecycle(
            std::move(refused.result));
          require(
            !decision.accepted()
              && outcome.status == local_append_status::not_written,
            "the fenced segment accepted a later request");
          const auto& failed = append.storage_failure();
          require(
            failures.size() == 1 && failed
              && failures[0].path == local_storage_path::segment
              && failures[0].segment == context(2)
              && failures[0].detail == runtime::file_failure_detail::device_io
              && failed->failure.error()
              && failed->failure.error()->code() == errc::io_failure,
            "the segment failure was not reported once and classified");
          // Only the failed segment group keeps the WAL file obligated: the
          // discharged prefix stops where that group's PREPAREs begin.
          std::vector<local_obligation> open;
          const auto obligated = take(append.obligations(
            [&open](const local_obligation& one) { open.push_back(one); }));
          const auto file = writer.progress()->reserved.incarnation();
          require(
            open.size() == 1 && obligated.obligations == 1
              && open[0].segment == context(2) && open[0].wal == file
              && open[0].pending == 0 && open[0].pinned == 1
              && obligated.discharged == head.wal.boundary().cursor()
              && !append.reclaimable(file),
            "the failed segment group's obligation was not pinned");
      });
}

// Ending the caller's interest only detaches it. Before acceptance an ended
// interest rejects with no effect; after it, the result reports aborted and
// the append continues, as it does when the result is dropped. deterministic
// is false where I/O may overtake the abort.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> interest_ends_without_cancelling(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer,
  bool deterministic) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 1},
      [&](auto& append, auto&, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          using interest = local_append_interest<Timer>;
          const auto empty = segments[0]->progress()->reserved;
          const auto charged = budget.snapshot();
          const auto rejected = [&](
                                  local_append_interest_stages stages,
                                  errc code) -> seastar::future<> {
              auto accepted = co_await drive.lifecycle(
                std::move(stages.accepted));
              auto outcome = co_await drive.lifecycle(std::move(stages.result));
              require(
                !accepted.accepted() && accepted.failure.error()
                  && accepted.failure.error()->code() == code && outcome
                  && outcome->status == local_append_status::not_written,
                "an ended interest was accepted");
          };
          {
              seastar::abort_source gone;
              gone.request_abort();
              co_await rejected(
                append.append(
                  *targets[0],
                  co_await request(child_wire(100), budget, work),
                  work,
                  interest{timer, gone, std::nullopt}),
                errc::aborted);
          }
          {
              seastar::abort_source caller;
              co_await rejected(
                append.append(
                  *targets[0],
                  co_await request(child_wire(100), budget, work),
                  work,
                  interest{timer, caller, Clock::now()}),
                errc::timed_out);
          }
          require(
            segments[0]->progress()->reserved == empty
              && budget.snapshot().bytes == charged.bytes
              && budget.snapshot().tasks == charged.tasks,
            "a rejected interest left positions or charges behind");
          seastar::abort_source caller;
          auto watched = append.append(
            *targets[0],
            co_await request(child_wire(100), budget, work),
            work,
            interest{timer, caller, std::nullopt});
          require(
            (co_await drive.lifecycle(std::move(watched.accepted))).accepted(),
            "watched request rejected");
          caller.request_abort();
          const auto ended = co_await drive.lifecycle(
            std::move(watched.result));
          if (deterministic || !ended)
              require(
                !ended && ended.error().code() == errc::aborted,
                "an aborted interest did not end with aborted");
          else
              static_cast<void>(durable(*ended));
          {
              auto dropped = append.append(
                *targets[0],
                co_await request(child_wire(101), budget, work),
                work);
              require(
                (co_await drive.lifecycle(std::move(dropped.accepted)))
                  .accepted(),
                "dropped request rejected");
          }
          const auto next = co_await settle(
            append.append(
              *targets[0],
              co_await request(child_wire(102), budget, work),
              work),
            drive);
          const auto& receipt = durable(next);
          require(
            receipt.batch.context.logical_span().begin()
                == model::range_logical_end{102}
              && receipt.block.records.bytes().begin() > empty.bytes
              && segments[0]->progress()->durable.blocks == 3
              && !segments[0]->failure().failed(),
            "an ended interest cancelled its append");
      });
}

// A deadline reached while the request is in flight ends the caller's
// interest with timed_out; the append still becomes durable. Needs I/O that
// cannot complete before the deadline.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> deadline_detaches_waiter(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 1},
      [&](auto& append, auto&, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          auto blocker = append.append(
            *targets[0], co_await request(child_wire(100), budget, work), work);
          require(
            (co_await drive.lifecycle(std::move(blocker.accepted))).accepted(),
            "blocking request rejected");
          seastar::abort_source caller;
          const auto deadline = Clock::now().checked_add(
            runtime::monotonic_duration{1});
          require(deadline.has_value(), "deadline overflow");
          auto timed = append.append(
            *targets[0],
            co_await request(child_wire(101), budget, work),
            work,
            local_append_interest<Timer>{timer, caller, *deadline});
          require(
            (co_await drive.lifecycle(std::move(timed.accepted))).accepted(),
            "timed request rejected");
          const auto ended = co_await drive.lifecycle(std::move(timed.result));
          require(
            !ended && ended.error().code() == errc::timed_out,
            "the deadline did not end the caller's interest");
          const auto head = durable(
            co_await drive.lifecycle(std::move(blocker.result)));
          const auto next = co_await settle(
            append.append(
              *targets[0],
              co_await request(child_wire(102), budget, work),
              work),
            drive);
          require(
            durable(next).block.records.bytes().begin()
                > head.segment.boundary().end().bytes
              && segments[0]->progress()->durable.blocks == 3,
            "a timed-out waiter cancelled its append");
      });
}

// The completed fact of a durable request, as the completed-request owner
// would supply it.
inline completed_retry completed_fact(const local_append_receipt& receipt) {
    const auto original = receipt.batch.context.submitted();
    return completed_retry::make(
             original.id(),
             receipt.batch.fingerprint,
             original.binding(),
             receipt.batch.context.logical_span(),
             original.binding().generation())
      .value();
}

// Completed-retry facts: identical facts return the stored original, a
// conflicting one names its first differing field, scope and the reserved
// capacity are enforced, and detaching hands the facts to the segment's seal
// in canonical order.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> completed_retry_facts(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {},
      [&](auto& append, auto&, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          const auto first = co_await settle(
            append.append(
              *targets[0],
              co_await request(child_wire(100), budget, work),
              work),
            drive);
          const auto second = co_await settle(
            append.append(
              *targets[0],
              co_await request(child_wire(101), budget, work),
              work),
            drive);
          const auto one = completed_fact(durable(first));
          const auto two = completed_fact(durable(second));
          const auto record = [&](const completed_retry& fact) {
              return append.record_completed_retry(*targets[0], fact);
          };
          // Recorded out of order; an identical fact returns the original.
          const auto created = take(record(two));
          require(
            created.status == completed_retry_status::created
              && take(record(one)).status == completed_retry_status::created,
            "a new completed fact was not recorded");
          const auto again = take(record(one));
          require(
            again.status == completed_retry_status::exists
              && again.stored == one,
            "an identical fact did not return the stored original");
          // A conflict names the first differing field and keeps the original.
          auto digest = one.submitted_digest().bytes();
          digest[0] ^= 1;
          const auto other_digest = take(record(
            completed_retry::make(
              one.id(),
              codec::semantic_batch_digest{digest},
              one.original_binding(),
              two.returned_span(),
              one.ack_generation())
              .value()));
          const auto other_span = take(record(
            completed_retry::make(
              one.id(),
              one.submitted_digest(),
              one.original_binding(),
              two.returned_span(),
              one.ack_generation())
              .value()));
          require(
            other_digest.status
                == completed_retry_status::
                  exists_with_different_submitted_digest
              && other_span.status
                   == completed_retry_status::
                     exists_with_different_returned_span
              && other_digest.stored == one && other_span.stored == one,
            "a conflicting fact did not name its first differing field");
          // Another generation of the segment is out of scope.
          const auto binding = one.original_binding();
          const auto elsewhere = record(
            completed_retry::make(
              one.id(),
              one.submitted_digest(),
              model::producer_stream_binding::make(
                binding.topic(),
                binding.range(),
                binding.routing_epoch(),
                binding.segment(),
                model::segment_generation::make(2).value())
                .value(),
              one.returned_span(),
              model::segment_generation::make(2).value())
              .value());
          require(
            !elsewhere && elsewhere.error().code() == errc::wrong_context,
            "an out-of-scope fact was recorded");
          // Two appends reserved two entries: a third identity rejects.
          auto unappended = co_await request(child_wire(102), budget, work);
          const auto info = unappended.batch.info();
          const auto beyond = record(
            completed_retry::make(
              info.context.submitted().id(),
              info.fingerprint,
              info.context.submitted().binding(),
              info.context.logical_span(),
              info.context.submitted().binding().generation())
              .value());
          require(
            !beyond && beyond.error().code() == errc::resource_exhausted,
            "a fact beyond the reserved capacity was recorded");
          // A completion fact is no WAL obligation.
          require(
            take(append.obligations([](const local_obligation&) {})).obligations
              == 0,
            "a completed-retry fact opened a WAL obligation");
          auto detached = take(append.detach(*targets[0]));
          // The attachment's newest durable footer leaves with it.
          const auto& last = durable(second).segment.boundary();
          require(
            detached.boundary
              && detached.boundary->device == spec.owner.device()
              && detached.boundary->history == last.history()
              && detached.boundary->covered == last.covered()
              && detached.boundary->footer == *last.footer(),
            "detach lost the attachment's newest durable footer");
          auto& snapshot = detached.retry;
          const auto& facts = snapshot.facts();
          const auto slice = take(
            co_await drive.lifecycle(snapshot.read(0, 2, work)));
          require(
            snapshot.completed() == 2 && snapshot.reserved() == 2
              && facts[0].id().canonical_less(facts[1].id())
              && (facts[0] == one || facts[0] == two)
              && (facts[1] == one || facts[1] == two) && slice.size() == 2
              && slice[0] == facts[0] && slice[1] == facts[1]
              && !(co_await drive.lifecycle(snapshot.read(1, 2, work))),
            "detach did not hand over the facts in canonical order");
          const auto completed = snapshot.completed();
          const auto sealed = co_await drive.lifecycle(
            segments[0]->seal(
              std::move(snapshot),
              completed,
              segments[0]->progress()->reserved.retry_entries - completed,
              work));
          require(
            !sealed.failure.failed() && sealed.retry && sealed.unresolved == 0,
            "the segment did not seal with the recorded facts");
      });
}

// Each WAL file's open obligations toward each segment, and the discharged
// prefix: open while a group's segment barrier is pending, gone once every
// group is segment-durable. Needs I/O that acceptance cannot overtake.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> obligations_track_segments(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 1, .segments = 2},
      [&](auto& append, auto& writer, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          const auto start = writer.progress()->reserved;
          const auto file = start.incarnation();
          const auto collect = [&] {
              std::vector<local_obligation> open;
              auto snapshot = take(append.obligations(
                [&open](const local_obligation& one) { open.push_back(one); }));
              return std::pair{std::move(open), snapshot};
          };
          auto blocker = append.append(
            *targets[0], co_await request(child_wire(100), budget, work), work);
          require(
            (co_await drive.lifecycle(std::move(blocker.accepted))).accepted(),
            "blocking request rejected");
          auto a1 = append.append(
            *targets[0], co_await request(child_wire(101), budget, work), work);
          auto b1 = append.append(
            *targets[1],
            co_await request(child_wire(100, 2), budget, work),
            work);
          for (auto* stages : {&a1, &b1})
              require(
                (co_await drive.lifecycle(std::move(stages->accepted)))
                  .accepted(),
                "grouped request rejected");
          // Only the blocker's group reached the WAL so far.
          const auto [early, during] = collect();
          require(
            early.size() == 1 && early[0].wal == file
              && early[0].segment == context(1) && early[0].pending == 1
              && early[0].pinned == 0 && during.discharged == start
              && !append.reclaimable(file),
            "an in-flight group's obligation was not open");
          // No barrier has covered a group yet: nothing to pin.
          std::uint32_t reported = 0;
          append.durable_boundaries(
            [&reported](const local_durable_boundary&) { ++reported; });
          require(reported == 0, "a footer was reported before its barrier");
          static_cast<void>(
            durable(co_await drive.lifecycle(std::move(blocker.result))));
          static_cast<void>(
            durable(co_await drive.lifecycle(std::move(a1.result))));
          static_cast<void>(
            durable(co_await drive.lifecycle(std::move(b1.result))));
          const auto [late, after] = collect();
          require(
            late.empty() && after.obligations == 0
              && after.discharged == writer.progress()->reserved
              && after.wal_durable == writer.progress()->durable,
            "segment-durable groups left obligations open");
          // Each attachment reports its own newest durable footer.
          std::vector<local_durable_boundary> newest;
          append.durable_boundaries(
            [&newest](const local_durable_boundary& one) {
                newest.push_back(one);
            });
          require(newest.size() == 2, "an attachment's footer is missing");
          for (std::uint32_t i = 0; i != 2; ++i)
              require(
                newest[i].history.segment == context(i + 1)
                  && newest[i].device == spec.owner.device()
                  && newest[i].footer.end()
                       == segments[i]->progress()->durable.bytes,
                "a reported footer is not its segment's durable end");
      });
}

// The environment's stop notification closes admission without failing
// accepted work, which drains through close.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> shutdown_stops_admission(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 1},
      [&](auto& append, auto&, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          seastar::abort_source environment;
          take(append.bind_shutdown(environment));
          auto accepted = append.append(
            *targets[0], co_await request(child_wire(100), budget, work), work);
          require(
            (co_await drive.lifecycle(std::move(accepted.accepted))).accepted(),
            "request rejected before shutdown");
          environment.request_abort();
          auto late = append.append(
            *targets[0], co_await request(child_wire(101), budget, work), work);
          const auto refused = co_await drive.lifecycle(
            std::move(late.accepted));
          const auto unwritten = co_await drive.lifecycle(
            std::move(late.result));
          static_cast<void>(
            durable(co_await drive.lifecycle(std::move(accepted.result))));
          seastar::abort_source again;
          require(
            !refused.accepted() && refused.failure.error()
              && refused.failure.error()->code() == errc::closed
              && unwritten.status == local_append_status::not_written
              && segments[0]->progress()->durable.blocks == 1
              && !append.bind_shutdown(again),
            "shutdown accepted new work or failed accepted work");
      });
}

inline constexpr runtime::monotonic_duration half_second{500'000'000};

// Runs operation with native preemption suppressed, so the call's synchronous
// path (acceptance, then formation up to the group's task) completes inside it.
template<typename Func>
auto uninterrupted(Func operation) {
    seastar::internal::preemption_monitor quiet{};
    const auto* previous = seastar::internal::get_need_preempt_var();
    auto restore = seastar::defer([previous] noexcept {
        seastar::internal::set_need_preempt_var(previous);
    });
    seastar::internal::set_need_preempt_var(&quiet);
    return operation();
}

// Either barrier may finish first; a request is published only after both,
// and its obligation follows the segment barrier alone. The caller's
// environment delays the slower flush by a second.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> barrier_order(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer,
  bool wal_slow) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 1},
      [&](auto& append, auto& writer, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          const auto start = writer.progress()->reserved;
          auto stages = append.append(
            *targets[0], co_await request(child_wire(100), budget, work), work);
          require(
            (co_await drive.lifecycle(std::move(stages.accepted))).accepted(),
            "request rejected");
          co_await probe(
            drive, timer, Clock::now().checked_add(half_second).value());
          std::vector<local_obligation> open;
          const auto snapshot = take(append.obligations(
            [&open](const local_obligation& one) { open.push_back(one); }));
          const auto wal = *writer.progress();
          require(
            !stages.result.available(),
            "a request was published before both barriers");
          // Segment-durable first: the obligation is discharged past the
          // group while the WAL-durable end is still before it, so the
          // discharged prefix alone is no bound for dropping WAL.
          if (wal_slow)
              require(
                segments[0]->progress()->durable.blocks == 1
                  && wal.durable == start && open.empty()
                  && snapshot.discharged == wal.reserved
                  && snapshot.wal_durable == start
                  && !(snapshot.discharged == start),
                "the segment barrier did not finish first or left its "
                "obligation open");
          else
              require(
                segments[0]->progress()->durable.blocks == 0
                  && wal.durable == wal.reserved && !(wal.durable == start)
                  && open.size() == 1 && open[0].pending == 1
                  && snapshot.discharged == start,
                "the WAL barrier did not finish first or discharged early");
          const auto outcome = co_await drive.lifecycle(
            std::move(stages.result));
          const auto& receipt = durable(outcome);
          require(
            receipt.wal.boundary().cursor().position()
                <= writer.progress()->durable.position()
              && segments[0]->progress()->durable.blocks == 1,
            "the result preceded one of its barriers");
      });
}

// A slow segment does not hold back another segment's request: completions
// arrive out of call order. The caller's environment delays segment A's
// first append flush.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> reordered_completions(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 1, .segments = 2},
      [&](auto& append, auto&, auto&, auto& targets, auto& work)
        -> seastar::future<> {
          auto a1 = append.append(
            *targets[0], co_await request(child_wire(100), budget, work), work);
          auto b1 = append.append(
            *targets[1],
            co_await request(child_wire(100, 2), budget, work),
            work);
          for (auto* stages : {&a1, &b1})
              require(
                (co_await drive.lifecycle(std::move(stages->accepted)))
                  .accepted(),
                "request rejected");
          co_await probe(
            drive, timer, Clock::now().checked_add(half_second).value());
          require(
            b1.result.available() && !a1.result.available(),
            "a slow segment held back another segment's result");
          static_cast<void>(
            durable(co_await drive.lifecycle(std::move(b1.result))));
          static_cast<void>(
            durable(co_await drive.lifecycle(std::move(a1.result))));
      });
}

// The WAL flush fails although the segment group became durable: the request
// is uncertain, never successful; its obligation is discharged because the
// segment holds it; the failure is reported once; later work is refused. The
// caller's environment fails the request's WAL flush.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> wal_failure_after_segment(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    std::vector<local_storage_failure> failures;
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 1, .failures = &failures, .tolerate_close = true},
      [&](auto& append, auto&, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          auto stages = append.append(
            *targets[0], co_await request(child_wire(100), budget, work), work);
          require(
            (co_await drive.lifecycle(std::move(stages.accepted))).accepted(),
            "request rejected");
          const auto outcome = co_await drive.lifecycle(
            std::move(stages.result));
          std::vector<local_obligation> open;
          static_cast<void>(take(append.obligations(
            [&open](const local_obligation& one) { open.push_back(one); })));
          require(
            outcome.status == local_append_status::uncertain && !outcome.receipt
              && outcome.failure.failed()
              && segments[0]->progress()->durable.blocks == 1 && open.empty(),
            "a failed WAL flush produced success or kept the segment's "
            "obligation");
          require(
            failures.size() == 1 && failures[0].path == local_storage_path::wal
              && failures[0].detail == runtime::file_failure_detail::device_io,
            "the WAL failure was not reported once and classified");
          const auto later = co_await drive.lifecycle(
            append
              .append(
                *targets[0],
                co_await request(child_wire(101), budget, work),
                work)
              .result);
          require(
            later.status == local_append_status::not_written
              && append.failure().failed(),
            "a failed WAL accepted later work or left the owner unlatched");
      });
}

// Which file's flush the history lets complete before the device crashes.
enum class survival_history : std::uint8_t {
    both,
    wal_only,
    footer_only,
    neither,
    // The WAL flush takes effect but its completion is lost.
    lost_wal_completion,
};

// One request and a crash: the surviving bytes follow which flushes took
// effect, and only a request reported durable has both. Surviving bytes never
// make a request successful. A second crash changes nothing durable. The
// caller's environment delays (or drops) the flushes the history names.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> crash_history(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer,
  survival_history history) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 1, .fenced = 1, .tolerate_close = true},
      [&](auto& append, auto& writer, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          const auto wal_start = writer.progress()->reserved.position().value();
          const auto data_start
            = segments[0]->progress()->reserved.bytes.value();
          const auto incarnation = writer.prepared_head()->incarnation;
          auto stages = append.append(
            *targets[0], co_await request(child_wire(100), budget, work), work);
          require(
            (co_await drive.lifecycle(std::move(stages.accepted))).accepted(),
            "request rejected");
          std::optional<local_append_outcome> outcome;
          if (history == survival_history::both)
              outcome.emplace(
                co_await drive.lifecycle(std::move(stages.result)));
          else {
              co_await probe(
                drive, timer, Clock::now().checked_add(half_second).value());
              require(
                !stages.result.available(),
                "a request was published before both barriers");
          }
          // What the request wrote; the crash keeps only what was flushed.
          const auto wal_written = co_await read_wal(
            files, spec, incarnation, drive);
          const auto data_written = co_await read_segment(
            files, spec, 1, drive);
          take(co_await drive.lifecycle(files.crash()));
          if (!outcome)
              outcome.emplace(
                co_await drive.lifecycle(std::move(stages.result)));
          const auto wal_image = co_await read_wal(
            files, spec, incarnation, drive);
          const auto data_image = co_await read_segment(files, spec, 1, drive);
          const auto survived = [](
                                  const std::string& image,
                                  const std::string& written,
                                  std::uint64_t begin,
                                  std::uint64_t end) {
              return begin < end && end <= written.size() && image.size() >= end
                     && image.compare(
                          begin, end - begin, written, begin, end - begin)
                          == 0;
          };
          const bool prepare = survived(
            wal_image, wal_written, wal_start, wal_start + 8192);
          const bool block = survived(
            data_image, data_written, data_start, data_written.size());
          const bool wal_flushed
            = history == survival_history::both
              || history == survival_history::wal_only
              || history == survival_history::lost_wal_completion;
          const bool segment_flushed
            = history == survival_history::both
              || history == survival_history::footer_only
              || history == survival_history::lost_wal_completion;
          require(
            prepare == wal_flushed && block == segment_flushed,
            "the crash image does not follow the flushes that took effect");
          require(
            (outcome->status == local_append_status::durable)
              == (history == survival_history::both),
            "surviving bytes made a request successful, or a durable "
            "request lost them");
          take(co_await drive.lifecycle(files.crash()));
          require(
            co_await read_wal(files, spec, incarnation, drive) == wal_image
              && co_await read_segment(files, spec, 1, drive) == data_image,
            "a second crash changed the durable image");
      });
}

// Many concurrent requests across two segments, with a window of them
// outstanding: every one becomes durable, and each segment's blocks and WAL
// receipts follow call order. Acceptance rejects under budget pressure rather
// than waiting, so the caller bounds what is outstanding to what the
// fixture's budget admits.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> concurrent_pipeline(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.segments = 2},
      [&](auto& append, auto&, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          constexpr std::uint32_t requests = 24;
          constexpr std::uint32_t window = 12;
          std::vector<local_append_stages> staged;
          staged.reserve(requests);
          std::vector<local_append_outcome> outcomes;
          outcomes.reserve(requests);
          for (std::uint32_t i = 0; i != requests; ++i) {
              if (i >= window)
                  outcomes.push_back(
                    co_await settle(std::move(staged[i - window]), drive));
              const auto logical = std::uint64_t{100} + i / 2;
              staged.push_back(append.append(
                *targets[i % 2],
                co_await request(
                  i % 2 == 0 ? child_wire(logical) : child_wire(logical, 2),
                  budget,
                  work),
                work));
          }
          for (std::uint32_t i = requests - window; i != requests; ++i)
              outcomes.push_back(co_await settle(std::move(staged[i]), drive));
          for (std::uint32_t i = 0; i != requests; ++i) {
              const auto& receipt = durable(outcomes[i]);
              require(
                receipt.batch.context.logical_span().begin().value()
                  == 100 + i / 2,
                "a request lost its logical position");
              if (i < 2) continue;
              const auto& previous = *outcomes[i - 2].receipt;
              require(
                receipt.block.records.bytes().begin()
                    >= previous.block.records.bytes().end()
                  && receipt.wal.boundary().cursor().position()
                       >= previous.wal.boundary().cursor().position(),
                "concurrent requests lost call order");
          }
          require(
            segments[0]->progress()->durable.blocks == requests / 2
              && segments[1]->progress()->durable.blocks == requests / 2,
            "concurrent requests were not all written");
      });
}

// Close while accepted requests wait for a slow WAL flush: close waits for
// them, every one becomes durable and close succeeds. The caller's
// environment delays the first WAL flush.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> close_during_slow_flush(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {},
      [&](auto& append, auto&, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          std::vector<local_append_stages> pending;
          for (std::uint64_t logical = 100; logical != 104; ++logical)
              pending.push_back(append.append(
                *targets[0],
                co_await request(child_wire(logical), budget, work),
                work));
          for (auto& stages : pending)
              require(
                (co_await drive.lifecycle(std::move(stages.accepted)))
                  .accepted(),
                "request rejected before close");
          take(co_await drive.lifecycle(append.close()));
          for (auto& stages : pending)
              static_cast<void>(
                durable(co_await drive.lifecycle(std::move(stages.result))));
          require(
            segments[0]->progress()->durable.blocks == 4,
            "close did not wait for requests blocked on a slow flush");
      });
}

#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION) && !defined(SEASTAR_DEBUG)
// One allocation failure at the at-th allocation of a request's synchronous
// path: acceptance, then formation up to the group's task. A rejected request
// has no effect; an accepted one settles; the owner continues. Returns whether
// the cut was reached.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<bool> acceptance_allocation_cut(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer,
  std::size_t at) {
    bool injected = false;
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 1},
      [&](auto& append, auto&, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          const auto empty = segments[0]->progress()->reserved;
          auto input = co_await request(child_wire(100), budget, work);
          auto& injector = seastar::memory::local_failure_injector();
          auto cancel = seastar::defer(
            [&injector] noexcept { injector.cancel(); });
          auto stages = uninterrupted([&] {
              injector.fail_after(at);
              auto output = append.append(*targets[0], std::move(input), work);
              injected = injector.failed();
              injector.cancel();
              return output;
          });
          const auto decision = co_await drive.lifecycle(
            std::move(stages.accepted));
          const auto outcome = co_await drive.lifecycle(
            std::move(stages.result));
          const bool written = outcome.status == local_append_status::durable;
          if (decision.accepted())
              require(
                written || outcome.status == local_append_status::not_written,
                "an accepted request did not settle");
          else
              require(
                outcome.status == local_append_status::not_written
                  && segments[0]->progress()->reserved == empty,
                "a rejected request left an effect");
          const auto next = co_await settle(
            append.append(
              *targets[0],
              co_await request(child_wire(written ? 101 : 100), budget, work),
              work),
            drive);
          require(
            durable(next).block.records.bytes().begin()
              == (written ? outcome.receipt->segment.boundary().end().bytes
                          : empty.bytes),
            "an allocation failure left a hole or stopped the owner");
      });
    co_return injected;
}
#endif

// The environment's stop reaches the WAL owner after a group's positions are
// admitted but before the WAL accepts it: the group's frozen segment group is
// dropped unwritten, its requests are not_written with no PREPARE, the
// segment is fenced as dropped frozen groups require, and the owner refuses
// later work.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> wal_stops_before_acceptance(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
#if !defined(SEASTAR_DEBUG)
    // Native debug mode preempts at every await, so the synchronous window
    // this case targets does not exist there.
    seastar::abort_source wal_stop;
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 1,
       .fenced = 1,
       .tolerate_close = true,
       .wal_shutdown = &wal_stop},
      [&](auto& append, auto& writer, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          const auto wal_start = writer.progress()->reserved;
          auto input = co_await request(child_wire(100), budget, work);
          // Acceptance and formation complete; the group's task has not run.
          auto stages = uninterrupted(
            [&] { return append.append(*targets[0], std::move(input), work); });
          require(
            append.groups() == 1 && append.forming() == 0,
            "the request did not form its group synchronously");
          wal_stop.request_abort();
          const auto decision = co_await drive.lifecycle(
            std::move(stages.accepted));
          const auto outcome = co_await drive.lifecycle(
            std::move(stages.result));
          require(
            decision.accepted()
              && outcome.status == local_append_status::not_written
              && outcome.failure.error()
              && outcome.failure.error()->code() == errc::closed
              && writer.progress()->reserved == wal_start
              && segments[0]->failure().failed(),
            "a WAL stop before acceptance left a PREPARE or claimed success");
          const auto later = co_await drive.lifecycle(
            append
              .append(
                *targets[0],
                co_await request(child_wire(100), budget, work),
                work)
              .result);
          require(
            later.status == local_append_status::not_written,
            "the owner accepted work after its WAL stopped");
      });
#else
    static_cast<void>(files);
    static_cast<void>(owner);
    static_cast<void>(spec);
    static_cast<void>(budget);
    static_cast<void>(drive);
    static_cast<void>(timer);
    co_return;
#endif
}

#if !defined(SEASTAR_DEBUG)
// Close right after a request forms its group, before the group's task has
// frozen anything: close drains it through both barriers. Debug mode preempts
// at every await, so this synchronous window does not exist there.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> close_before_group_runs(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {},
      [&](auto& append, auto&, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          auto input = co_await request(child_wire(100), budget, work);
          auto stages = uninterrupted(
            [&] { return append.append(*targets[0], std::move(input), work); });
          require(
            append.groups() == 1 && append.forming() == 0,
            "the request did not form its group synchronously");
          auto closing = append.close();
          take(co_await drive.lifecycle(std::move(closing)));
          require(
            (co_await drive.lifecycle(std::move(stages.accepted))).accepted(),
            "request rejected");
          static_cast<void>(
            durable(co_await drive.lifecycle(std::move(stages.result))));
          require(
            segments[0]->progress()->durable.blocks == 1,
            "close dropped a formed group");
      });
}
#endif

// A normal append keeps the exact original identity and rejects sparse,
// relocated and wrongly routed children with no effect, while the requests
// around them stay contiguous.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> identity_and_rejections(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {},
      [&](auto& append, auto&, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          auto input = co_await request(child_wire(100), budget, work);
          const auto identity = input.batch.info();
          const auto first = co_await settle(
            append.append(*targets[0], std::move(input), work), drive);
          const auto& head = durable(first);
          require(
            head.batch == identity
              && head.block.records.logical()
                   == identity.context.logical_span(),
            "a durable result lost the batch's original identity");
          co_await retired<Clock>(append, *segments[0], drive, timer);
          const auto reserved = segments[0]->progress()->reserved;
          const auto charged = budget.snapshot();
          const auto rejected =
            [&](local_append_request offered, errc code) -> seastar::future<> {
              auto stages = append.append(
                *targets[0], std::move(offered), work);
              const auto decision = co_await drive.lifecycle(
                std::move(stages.accepted));
              const auto outcome = co_await drive.lifecycle(
                std::move(stages.result));
              require(
                !decision.accepted() && decision.failure.error()
                  && decision.failure.error()->code() == code
                  && outcome.status == local_append_status::not_written,
                "an unauthorized child was not rejected");
          };
          // Sparse data, even at its original placement.
          co_await rejected(
            co_await request(assigned_wire(false, 32, true), budget, work),
            errc::invalid_argument);
          // Bound to another generation: a relocation needs an installer.
          co_await rejected(
            co_await request(child_wire(101, 2), budget, work),
            errc::wrong_context);
          // A routing epoch the child was not bound under.
          auto rerouted = co_await request(child_wire(101), budget, work);
          rerouted.routing_epoch = model::range_routing_epoch::make(2).value();
          co_await rejected(std::move(rerouted), errc::wrong_context);
          require(
            segments[0]->progress()->reserved == reserved
              && budget.snapshot().bytes == charged.bytes
              && budget.snapshot().tasks == charged.tasks
              && !segments[0]->failure().failed(),
            "a rejected child left positions, charges or a fence behind");
          const auto next = co_await settle(
            append.append(
              *targets[0],
              co_await request(child_wire(101), budget, work),
              work),
            drive);
          require(
            durable(next).block.records.bytes().begin()
              == head.segment.boundary().end().bytes,
            "rejections moved the next request's position");
      });
}

} // namespace kwaque::storage::testing::local_append_contract
