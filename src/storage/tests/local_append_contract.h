#pragma once

#include "src/base/units.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/codec/digest.h"
#include "src/codec/xxh3.h"
#include "src/storage/completed_retry.h"
#include "src/storage/local_append.h"
#include "src/storage/placement_statistics.h"
#include "src/storage/retry_summary.h"
#include "src/storage/tests/footer_test_support.h"
#include "src/storage/tests/local_append_test_access.h"
#include "src/storage/tests/segment_writer_contract.h"
#include "src/storage/tests/wal_writer_contract.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/preempt.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/defer.hh>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace kwaque::storage::testing::local_append_contract {
using store_contract::require;
using store_contract::take;
namespace installation = installation_contract;
namespace segment = segment_writer_contract;

// An independent assigned child of `stream` bound to generation: the request
// sequence and logical base follow logical, and the semantic digest is
// recomputed over the bound submitted fields, as the footer fixtures do.
inline std::string child_wire(
  std::uint64_t logical,
  std::uint64_t generation = 1,
  std::uint64_t stream = 1) {
    auto child = assigned_wire();
    put(child, 32 + 96, generation, 8);
    put(child, 32 + 24, stream, 8);
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

// child_wire with one record of `value` bytes: a portion of a chosen size.
inline std::string valued_wire(
  std::uint64_t logical, std::size_t value, std::uint64_t generation = 1) {
    auto child = child_wire(logical, generation);
    replace_records(child, valued_record(value));
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

// The broker time every request carries, and the logical origin of every
// segment the fixture creates.
inline constexpr runtime::wall_time request_time{1};
inline model::range_logical_end segment_origin() {
    return model::range_logical_end{100};
}

// A child of several KiB takes larger fragments, so its WAL copy stays
// within the WAL's child metadata.
template<typename Budget>
seastar::future<local_append_request> request(
  std::string wire,
  Budget& resources,
  codec::cooperative_work& work,
  std::size_t width = 67) {
    auto raw = co_await installation::buffer_async(std::move(wire), width);
    auto batch = co_await validate_encoded_assigned_batch(
      std::move(raw), batch_expected(), budget(), work);
    require(batch.has_value(), "independent request child rejected");
    auto backing = take(resources.try_reserve_buffer(batch->bytes()));
    co_return local_append_request{
      std::move(*batch),
      std::move(backing),
      batch_expected(),
      routing(),
      request_time,
      {}};
}

// The threshold the placement contracts place by: a small request's portion
// takes less than it, and one large request's more.
inline constexpr byte_count middle_placement{32_KiB};
inline constexpr std::size_t large_value = 40'000;
// Fragments for a large request's child, so its WAL copy stays within the
// WAL's child metadata.
inline constexpr std::size_t large_width = 4096;

// A request larger than a metadata fixture may be, in fragments of the
// large width, built as the fixtures' own buffers are.
template<typename Budget>
seastar::future<local_append_request> large_request(
  std::string wire, Budget& resources, codec::cooperative_work& work) {
    const auto count = (wire.size() + large_width - 1U) / large_width;
    bytes::fragmented_buffer_builder builder{
      {.initial_fragment_bytes = byte_count{large_width},
       .max_fragment_bytes = byte_count{large_width},
       .max_total_bytes = byte_count{wire.size()},
       .max_retained_bytes = byte_count{count * large_width},
       .max_fragments = count}};
    builder.reserve_fragments(item_count{count}).value();
    for (std::string_view value{wire}; !value.empty();) {
        const auto size = std::min(value.size(), large_width);
        builder.append(std::span{value.data(), size}).value();
        value.remove_prefix(size);
        co_await seastar::coroutine::maybe_yield();
    }
    auto batch = co_await validate_encoded_assigned_batch(
      builder.finish().value(), batch_expected(), budget(), work);
    require(batch.has_value(), "independent request child rejected");
    auto backing = take(resources.try_reserve_buffer(batch->bytes()));
    co_return local_append_request{
      std::move(*batch),
      std::move(backing),
      batch_expected(),
      routing(),
      request_time,
      {}};
}

// Where a contract written for any placement puts its portions, and how
// long a WAL-placed one is held in its segment. Such a contract offers a
// large request among its small ones.
struct placement_profile final {
    byte_count placement{std::numeric_limits<std::uint64_t>::max()};
    byte_count hold{};
};
// Every portion in its segment; by size; every one in the WAL. Those that
// reach the WAL written at once, then held.
inline constexpr std::array placement_profiles{
  placement_profile{byte_count{0}, byte_count{}},
  placement_profile{middle_placement, byte_count{}},
  placement_profile{middle_placement, byte_count{64_KiB}},
  placement_profile{
    byte_count{std::numeric_limits<std::uint64_t>::max()}, byte_count{}},
  placement_profile{
    byte_count{std::numeric_limits<std::uint64_t>::max()}, byte_count{64_KiB}},
};

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
    // The placement threshold. The contracts written for the WAL's
    // mechanics place every portion there; the placement contracts choose.
    byte_count placement{std::numeric_limits<std::uint64_t>::max()};
    // How long a run of WAL-placed groups is held in its segment. The
    // contracts written for writes made at once hold nothing; the hold
    // contracts choose.
    byte_count hold{};
    // Syncs the owner runs at once.
    std::uint32_t syncs{16};
    // Every segment's retry depth.
    std::uint8_t retry_depth{default_retry_depth};
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
            description.retry_depth = chosen.retry_depth;
            segments[i] = take(
              segment_type::make_new(
                files,
                owner,
                spec,
                0,
                description,
                segment_origin(),
                budget,
                config));
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
        local_append_config placed;
        // The bounds these contracts were written for.
        placed.maximum_segments = 64;
        placed.maximum_requests = 64;
        placed.segment_placement_bytes = chosen.placement;
        placed.held_run_bytes = chosen.hold;
        placed.concurrent_syncs = chosen.syncs;
        append = take(
          append_type::make(
            budget,
            *commit,
            *writer,
            placed,
            std::move(sink),
            std::move(refused)));
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
    // A tolerated close may report the failure the test injected.
    const auto close = [&](
                         auto operation, bool tolerated) -> seastar::future<> {
        try {
            auto closed = co_await drive.lifecycle(std::move(operation));
            if (!tolerated) failed.observe(closed);
        } catch (...) {
            failed.observe(std::current_exception());
        }
    };
    const auto tolerated = chosen.tolerate_close;
    if (append) co_await close(append->close(), tolerated);
    append.reset();
    for (std::uint32_t i = 0; i != segments.size(); ++i) {
        auto& owned = segments[i];
        if (!owned) continue;
        co_await close(
          owned->close(), tolerated || (chosen.fenced & (1U << i)) != 0);
        owned.reset();
    }
    if (commit) co_await close(commit->close(), tolerated);
    commit.reset();
    if (writer) co_await close(writer->close(), tolerated);
    writer.reset();
    if (ids) co_await close(ids->close(), tolerated);
    ids.reset();
    if (control) co_await close(control->close(), tolerated);
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
                         + std::uint64_t{demand.seals} * segment_seal_handles
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
        && refused({.segments = most, .reserved = spare, .seals = 1})
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

// The completed fact of a durable request, as a seal of its segment keeps it.
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

// A case's caller of one append owner: it builds each independent child,
// charges it to the budget and submits it, and checks the stages it gets
// back. Nothing here awaits what the case would not await itself.
template<typename Append, typename Driver>
struct caller final {
    Append& append;
    workload_budget& budget;
    codec::cooperative_work& work;
    Driver drive;

    seastar::future<local_append_request>
    request(std::string wire, std::size_t width = 67) {
        return local_append_contract::request(
          std::move(wire), budget, work, width);
    }
    seastar::future<local_append_stages> submit(
      local_append_target target, std::string wire, std::size_t width = 67) {
        co_return append.append(
          target, co_await request(std::move(wire), width), work);
    }
    // Either kind of stages: with or without a caller's interest.
    template<typename Stages>
    seastar::future<> accepted(Stages& stages, const char* rejected) {
        require(
          (co_await drive.lifecycle(std::move(stages.accepted))).accepted(),
          rejected);
    }
    seastar::future<local_append_stages> accept(
      local_append_target target,
      std::string wire,
      const char* rejected,
      std::size_t width = 67) {
        auto stages = co_await submit(target, std::move(wire), width);
        co_await accepted(stages, rejected);
        co_return stages;
    }
    // Submitted, accepted and settled.
    seastar::future<local_append_outcome> settle(
      local_append_target target, std::string wire, std::size_t width = 67) {
        co_return co_await local_append_contract::settle(
          co_await submit(target, std::move(wire), width), drive);
    }
    // An accepted request's receipt; it must be durable.
    seastar::future<local_append_receipt> durable(local_append_stages& stages) {
        co_return local_append_contract::durable(
          co_await drive.lifecycle(std::move(stages.result)));
    }
    // Refused before acceptance with code, and nothing written.
    seastar::future<> refused(local_append_stages stages, errc code) {
        auto decision = co_await drive.lifecycle(std::move(stages.accepted));
        auto outcome = co_await drive.lifecycle(std::move(stages.result));
        require(
          !decision.accepted() && decision.failure.error()
            && decision.failure.error()->code() == code
            && outcome.status == local_append_status::not_written
            && !outcome.receipt,
          "a request that should have been refused was accepted or written");
    }
};

// The open WAL obligations, and the snapshot that counted them.
template<typename Append>
auto obligations(Append& append) {
    std::vector<local_obligation> open;
    auto snapshot = take(append.obligations(
      [&open](const local_obligation& one) { open.push_back(one); }));
    return std::pair{std::move(open), snapshot};
}
// Each attachment's newest durable footer.
template<typename Append>
std::vector<local_durable_boundary> newest_footers(Append& append) {
    std::vector<local_durable_boundary> newest;
    append.durable_boundaries(
      [&newest](const local_durable_boundary& one) { newest.push_back(one); });
    return newest;
}
// Nothing charged since charged was taken.
inline bool uncharged(
  const workload_budget& budget, const workload_budget_snapshot& charged) {
    const auto now = budget.snapshot();
    return now.bytes == charged.bytes && now.tasks == charged.tasks;
}

// The reported prefix of the fixture's attachment ending at a group's cut.
inline std::optional<local_reported_boundary> reported_at(
  const local_device_spec& spec, const segment_captured_boundary& cut) {
    const auto covered = cut.covered();
    return local_reported_boundary{
      spec.owner.device(),
      cut.history(),
      storage::coverage{
        covered.logical(),
        covered.physical(),
        model::file_byte_span::make(cut.history().data_start, cut.end().bytes)
          .value()}};
}

// Where a member's final block lies in the segment with this generation.
inline segment_write_context written_at(
  std::uint64_t generation,
  const segment_block_layout& block,
  std::uint64_t segment_alignment) {
    return segment_write_context::make(
             context(generation),
             alignment(segment_alignment),
             block.records.physical().begin(),
             block.records.bytes().begin())
      .value();
}

// The expected PREPARE, built independently at a WAL position and naming the
// member's final block. The WAL is 8-KiB aligned and its header fills its
// first 8 KiB; a segment's header fills its first alignment unit. Unless
// given, its stamps are those of the first group of both files: it ends its
// write, at the request's time, and both data starts certify nothing.
inline std::string prepare_at(
  const std::string& child,
  model::wal_incarnation_id incarnation,
  std::uint64_t wal_position,
  std::uint64_t generation,
  const segment_block_layout& block,
  std::uint64_t segment_alignment = 8192,
  std::optional<wal_prepare_stamps> stamps = std::nullopt) {
    const wal_prepare_expectation expected{
      wal_write_context::make(
        incarnation, alignment(8192), runtime::file_position{wal_position})
        .value(),
      runtime::file_position{8192},
      written_at(generation, block, segment_alignment),
      runtime::file_position{segment_alignment},
      routing(),
      batch_expected()};
    return wal_wire(child, expected, 32, stamps);
}
// What the writer stamped on the PREPARE that `stored` begins with.
inline wal_prepare_stamps stamps_of(std::string_view stored) {
    return {
      get(stored, 32 + 126, 2) != 0,
      runtime::wall_time{static_cast<std::int64_t>(get(stored, 32 + 128, 8))},
      runtime::file_position{get(stored, 32 + 144, 8)},
      runtime::file_position{get(stored, 32 + 152, 8)}};
}
// The block made of `child` where its receipt placed it, stamped as its
// receipt says, or, without stamps, as a first group is: at the request's
// time, certifying nothing.
inline std::string block_at(
  const std::string& child,
  std::uint64_t generation,
  const segment_block_layout& block,
  std::uint64_t segment_alignment = 8192,
  std::optional<segment_block_stamps> stamps = std::nullopt) {
    return block_wire(
      child,
      {written_at(generation, block, segment_alignment),
       runtime::file_position{segment_alignment},
       batch_expected()},
      32,
      stamps);
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

// A WAL-placed request is published by its WAL barrier, and its segment
// syncs only when asked. Asks every attachment for a sync, then waits until
// every group is released and no obligation is open, so every barrier owed
// has settled.
template<
  runtime::monotonic_clock Clock,
  typename Append,
  typename Driver,
  typename Timer>
seastar::future<> covered(Append& append, Driver drive, Timer& timer) {
    constexpr runtime::monotonic_duration poll{10'000'000};
    for (;;) {
        local_append_test_access::sync_all(append);
        bool open = append.groups() != 0;
        if (!open)
            static_cast<void>(
              take(append.obligations([&open](const local_obligation& one) {
                  open = open || one.pending != 0;
              })));
        if (!open) co_return;
        co_await probe(drive, timer, Clock::now().checked_add(poll).value());
    }
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
    co_await covered<Clock>(append, drive, timer);
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
// position, the block and footer are written there, and whichever barrier
// came first published it at its group's cut, where the reported prefix
// then ends. The segment's alignment is independent of the WAL's; a
// different one shows each is used where it belongs.
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
          caller client{append, budget, work, drive};
          const auto empty = segments[0]->progress()->reserved;
          const auto wal_start = writer.progress()->reserved;
          const auto wire = child_wire(100);
          auto outcome = co_await client.settle(*targets[0], wire);
          const auto& receipt = durable(outcome);
          const auto& block = receipt.block.records;
          const auto& cut = receipt.boundary;
          // Whichever barrier came first published it.
          require(
            receipt.placement == local_append_placement::wal
              && (receipt.wal || receipt.segment)
              && (!receipt.wal
                  || receipt.wal->boundary().cursor().position()
                       > wal_start.position())
              && (!receipt.segment
                  || receipt.segment->boundary().end() == cut.end())
              && take(append.reported(*targets[0])) == reported_at(spec, cut),
            "the receipt or the reported prefix does not cover the request");
          co_await covered<Clock>(append, drive, timer);
          require(
            block.bytes().begin() == empty.bytes
              && block.physical().begin() == empty.physical && cut.footer()
              && cut.footer()->begin() == block.bytes().end()
              && cut.end().blocks == 1 && cut.end().footers == 1
              && segments[0]->progress()->durable == cut.end(),
            "request was not written at the frozen block with its footer");
          require(
            writer.progress()->durable.position() > wal_start.position(),
            "the WAL barrier does not cover the request's PREPARE");
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
          caller client{append, budget, work, drive};
          auto first = co_await client.accept(
            *targets[0], child_wire(100), "first request rejected");
          std::vector<local_append_stages> later;
          for (std::uint64_t logical : {101U, 102U})
              later.push_back(
                co_await client.submit(*targets[0], child_wire(logical)));
          for (auto& stages : later)
              co_await client.accepted(stages, "later request rejected");
          const auto head = co_await client.durable(first);
          std::vector<local_append_outcome> tail;
          for (auto& stages : later)
              tail.push_back(
                co_await drive.lifecycle(std::move(stages.result)));
          const auto& second = durable(tail[0]);
          const auto& third = durable(tail[1]);
          co_await covered<Clock>(append, drive, timer);
          require(
            second.block.records.bytes().begin() == head.boundary.end().bytes
              && third.block.records.bytes().begin()
                   >= second.block.records.bytes().end()
              && segments[0]->progress()->durable.blocks == 3,
            "grouped requests lost order or position");
          if (deterministic)
              require(
                second.boundary == third.boundary
                  && third.block.records.bytes().begin()
                       == second.block.records.bytes().end()
                  && segments[0]->progress()->durable.footers == 2,
                "requests waiting on the in-flight group did not share a "
                "footer");
      });
}

// Members of one group across two segments keep acceptance order in the WAL
// (A1, B1, A2); each segment freezes its own contiguous blocks and footer.
// deterministic is false where I/O may overtake acceptance: the requests may
// then form more than one group, and a segment's two members lie in order
// with a footer between them.
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
  Timer& timer,
  bool deterministic) {
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
          caller client{append, budget, work, drive};
          const auto other = segments[1]->progress()->reserved;
          auto blocker = co_await client.accept(
            *targets[0], child_wire(100), "blocking request rejected");
          const std::array wires{
            child_wire(101), child_wire(100, 2), child_wire(102)};
          const std::array<std::uint32_t, 3> owners{0, 1, 0};
          // Requests join in call order whatever each one's validation
          // costs; the WAL order below checks it.
          std::vector<local_append_stages> grouped;
          for (std::size_t i = 0; i != wires.size(); ++i)
              grouped.push_back(
                co_await client.submit(*targets[owners[i]], wires[i]));
          for (auto& stages : grouped)
              co_await client.accepted(stages, "grouped request rejected");
          const auto first = co_await client.durable(blocker);
          std::vector<local_append_outcome> results;
          for (auto& stages : grouped)
              results.push_back(
                co_await drive.lifecycle(std::move(stages.result)));
          const auto& a1 = durable(results[0]);
          const auto& b1 = durable(results[1]);
          const auto& a2 = durable(results[2]);
          co_await covered<Clock>(append, drive, timer);
          require(
            a1.block.records.bytes().begin() == first.boundary.end().bytes
              && a2.block.records.bytes().begin()
                   >= a1.block.records.bytes().end()
              && b1.block.records.bytes().begin() == other.bytes
              && b1.boundary.end().footers == 1
              && segments[1]->progress()->durable == b1.boundary.end(),
            "a segment's members lost their order or their place");
          if (deterministic)
              require(
                a1.boundary == a2.boundary
                  && a2.block.records.bytes().begin()
                       == a1.block.records.bytes().end(),
                "a segment's members did not freeze contiguously under one "
                "footer");
          // After the blocker's PREPARE: A1, B1, A2 in acceptance order,
          // back to back within a group, whose last pads to the boundary.
          // Each states the certified ends the writers stamped, read back:
          // which barriers had completed depends on when each froze.
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
          const std::array data{
            co_await read_segment(files, spec, 1, drive),
            co_await read_segment(files, spec, 2, drive)};
          for (std::size_t i = 0; i != 3; ++i) {
              const auto stamps = stamps_of(
                std::string_view{stored}.substr(at));
              // The block made for it carries the same target stamp.
              const auto block_stamp = get(
                data[generations[i] - 1],
                ordered[i]->block.records.bytes().begin().value() + 32 + 128,
                8);
              const auto expected = prepare_at(
                wires[i],
                incarnation,
                at,
                generations[i],
                ordered[i]->block,
                8192,
                stamps);
              require(
                stored.substr(at, expected.size()) == expected
                  && stamps.target_certified_end.value() == block_stamp
                  && stamps.wal_certified_end.value() <= at
                  && (i != 2 || stamps.ends_write)
                  && (!deterministic || stamps.ends_write == (i == 2)),
                misplaced[i]);
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
          caller client{append, budget, work, drive};
          const auto segment_before = segments[0]->progress()->reserved;
          const auto wal_before = writer.progress()->reserved;
          const auto charged = budget.snapshot();
          {
              auto wrong = co_await client.request(child_wire(100));
              wrong.physical_begin = model::segment_relative_end{7};
              co_await client.refused(
                append.append(*targets[0], std::move(wrong), work),
                errc::wrong_context);
          }
          co_await client.refused(
            co_await client.submit(*targets[0], child_wire(105)),
            errc::wrong_context);
          {
              seastar::abort_source stop;
              stop.request_abort();
              codec::cooperative_work cancelled{
                codec::limits::defaults(), stop};
              auto input = co_await client.request(child_wire(100));
              co_await client.refused(
                append.append(*targets[0], std::move(input), cancelled),
                errc::aborted);
          }
          {
              auto config = codec::limits::defaults().config();
              config.max_buffer_fragments = item_count{4};
              seastar::abort_source stop;
              codec::cooperative_work narrow{
                codec::limits::make(config).value(), stop};
              auto input = co_await client.request(child_wire(100));
              co_await client.refused(
                append.append(*targets[0], std::move(input), narrow),
                errc::invalid_argument);
          }
          require(
            segments[0]->progress()->reserved == segment_before
              && writer.progress()->reserved == wal_before
              && uncharged(budget, charged) && !segments[0]->failure().failed()
              && append.forming() == 0,
            "a rejected request left positions, bytes or charges behind");
          auto outcome = co_await client.settle(*targets[0], child_wire(100));
          require(
            durable(outcome).block.records.bytes().begin()
              == segment_before.bytes,
            "rejections moved the next request's position");
          // An idle attachment detaches; its handle then names nothing.
          co_await covered<Clock>(append, drive, timer);
          const auto stale = *targets[0];
          take(append.detach(stale));
          targets[0] = take(append.attach(*segments[0]));
          require(!(*targets[0] == stale), "reattachment reused a handle");
          co_await client.refused(
            co_await client.submit(stale, child_wire(101)),
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
          caller client{append, budget, work, drive};
          auto first = co_await client.accept(
            *targets[0], child_wire(100), "first request rejected");
          auto waiting = co_await client.accept(
            *targets[0], child_wire(101), "forming request rejected");
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
          const auto head = co_await client.durable(first);
          auto pressured = co_await drive.lifecycle(std::move(waiting.result));
          require(
            pressured.status == local_append_status::not_written
              && pressured.failure.error()
              && pressured.failure.error()->code() == errc::queue_full
              && !segments[0]->failure().failed()
              && segments[0]->progress()->reserved == head.boundary.end(),
            "WAL pressure after acceptance froze or fenced the segment");
          squeeze.clear();
          auto next = co_await client.settle(*targets[0], child_wire(101));
          require(
            durable(next).block.records.bytes().begin()
              == head.boundary.end().bytes,
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
          caller client{append, budget, work, drive};
          const auto initial = writer.progress()->reserved.incarnation();
          std::optional<local_append_receipt> last;
          for (std::uint64_t logical = 100; logical != 104; ++logical) {
              auto outcome = co_await client.settle(
                *targets[0], child_wire(logical));
              const auto& receipt = durable(outcome);
              if (last)
                  require(
                    receipt.block.records.bytes().begin()
                      == last->boundary.end().bytes,
                    "rotation left a segment hole");
              last = receipt;
          }
          co_await covered<Clock>(append, drive, timer);
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
          const auto snapshot = obligations(append).second;
          require(
            snapshot.obligations == 0
              && snapshot.discharged == writer.progress()->reserved
              && snapshot.wal_durable == writer.progress()->durable
              && snapshot.wal_durable.incarnation() == current,
            "the discharged prefix stayed in a rotated WAL file");
          // The segment's newest durable footer is the last group's.
          const auto newest = newest_footers(append);
          const auto& cut = last->boundary;
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
                  auto outcome = co_await client.settle(
                    *targets[0], child_wire(logical));
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
          auto resumed = co_await client.settle(
            *targets[0], child_wire(logical - 1));
          const auto& receipt = durable(resumed);
          require(
            receipt.block.records.bytes().begin() == last->boundary.end().bytes
              && writer.statistics().rotations == rotations + 1
              && !append.failure().failed(),
            "a rotation refused for pressure did not resume");
      });
}

// Close stops acceptance, publishes accepted requests and makes every one
// durable in its segment: it runs the barriers they are owed and syncs what
// only the WAL holds, a held run written first.
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
  Timer& timer,
  placement_profile profile = {}) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 1, .placement = profile.placement, .hold = profile.hold},
      [&](auto& append, auto&, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          caller client{append, budget, work, drive};
          std::vector<local_append_stages> pending;
          pending.push_back(
            co_await client.submit(*targets[0], child_wire(100)));
          pending.push_back(
            co_await client.submit(
              *targets[0], valued_wire(101, large_value), large_width));
          for (auto& stages : pending)
              co_await client.accepted(stages, "request rejected before close");
          auto closing = append.close();
          auto late = co_await client.submit(*targets[0], child_wire(102));
          const auto refused = co_await drive.lifecycle(
            std::move(late.accepted));
          take(co_await drive.lifecycle(std::move(closing)));
          for (auto& stages : pending)
              static_cast<void>(co_await client.durable(stages));
          static_cast<void>(co_await drive.lifecycle(std::move(late.result)));
          require(
            !refused.accepted() && refused.failure.error()
              && refused.failure.error()->code() == errc::closed
              && segments[0]->progress()->durable.blocks == 2
              && append.statistics().held_runs == 0,
            "close accepted new work or did not drain accepted requests");
      });
}

// One attachment's admission closes while its requests are in flight. The
// wait ends only when what was accepted is published and a barrier of its
// segment covers it, and the attachment detaches at once; a request that
// arrives after is rejected as closed and reserves nothing; the other
// attachment's requests are accepted and durable meanwhile and afterwards; a
// second wait ends with the first, and a wait for the detached attachment is
// refused.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> quiesce_drains_one_attachment(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer,
  placement_profile profile = {}) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 1,
       .segments = 2,
       .placement = profile.placement,
       .hold = profile.hold},
      [&](auto& append, auto&, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          caller client{append, budget, work, drive};
          std::vector<local_append_stages> pending;
          pending.push_back(
            co_await client.submit(*targets[0], child_wire(100)));
          pending.push_back(
            co_await client.submit(
              *targets[0], valued_wire(101, large_value), large_width));
          for (auto& stages : pending)
              co_await client.accepted(
                stages, "request rejected before its attachment was quiesced");
          auto quiet = append.quiesce(*targets[0]);
          auto joined = append.quiesce(*targets[0]);
          auto late = co_await client.submit(*targets[0], child_wire(102));
          const auto refused = co_await drive.lifecycle(
            std::move(late.accepted));
          const auto meanwhile = co_await client.settle(
            *targets[1], child_wire(100, 2));
          take(co_await drive.lifecycle(std::move(quiet)));
          // Idle, so both accepted requests are behind their barriers.
          const auto drained = segments[0]->progress()->durable.blocks;
          take(co_await drive.lifecycle(std::move(joined)));
          const auto detached = take(append.detach(*targets[0]));
          const auto stale = co_await drive.lifecycle(
            append.quiesce(*targets[0]));
          for (auto& stages : pending)
              static_cast<void>(co_await client.durable(stages));
          static_cast<void>(co_await drive.lifecycle(std::move(late.result)));
          const auto afterwards = co_await client.settle(
            *targets[1], child_wire(101, 2));
          static_cast<void>(durable(meanwhile));
          static_cast<void>(durable(afterwards));
          require(
            drained == 2 && detached.boundary && detached.retry.facts() == 2,
            "the wait ended before what was accepted was durable, or the "
            "attachment did not detach whole");
          require(
            !refused.accepted() && refused.failure.error()
              && refused.failure.error()->code() == errc::closed && !stale
              && stale.error().code() == errc::wrong_context,
            "a quiesced attachment took a request, or a stale one was "
            "waited for");
      });
}

// Groups submitted while a segment's barrier runs are covered together by
// its next barrier: one receipt, while each group keeps its own footer.
// Segment-placed, so each is published by its segment's barrier. Needs I/O
// that acceptance cannot overtake.
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
      {.outstanding = 3, .placement = byte_count{0}},
      [&](auto& append, auto&, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          caller client{append, budget, work, drive};
          // Each request forms its own group before the next arrives.
          std::vector<local_append_stages> staged;
          for (std::uint64_t logical : {100U, 101U, 102U})
              staged.push_back(
                co_await client.accept(
                  *targets[0], child_wire(logical), "request rejected"));
          std::vector<local_append_outcome> outcomes;
          for (auto& stages : staged)
              outcomes.push_back(
                co_await drive.lifecycle(std::move(stages.result)));
          const auto& first = durable(outcomes[0]);
          const auto& second = durable(outcomes[1]);
          const auto& third = durable(outcomes[2]);
          require(
            first.segment && second.segment && third.segment && !first.wal
              && !second.wal && !third.wal
              && second.block.records.bytes().begin()
                   == first.boundary.end().bytes
              && third.block.records.bytes().begin()
                   > second.block.records.bytes().end()
              && first.segment->boundary() == first.boundary
              && second.segment->boundary() == third.segment->boundary()
              && third.segment->boundary() == third.boundary
              && !(second.boundary == third.boundary)
              && third.boundary.end().blocks == 3
              && third.boundary.end().footers == 3
              && segments[0]->progress()->durable == third.boundary.end(),
            "groups submitted during a barrier were not covered by one "
            "barrier");
      });
}

// One WAL group across two segments whose second segment's barrier flush
// fails: the first segment's members stay durable; the failed one's member
// is durable by its WAL barrier alone, its segment is fenced, and later
// requests there are refused. The failure is reported once, classified, and
// that obligation stays pinned in the WAL file, which alone holds the
// member. The caller's environment fails that segment's next flush.
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
          caller client{append, budget, work, drive};
          auto blocker = co_await client.accept(
            *targets[0], child_wire(100), "blocking request rejected");
          // A1 and B1 form one group behind the blocker.
          auto a1 = co_await client.submit(*targets[0], child_wire(101));
          auto b1 = co_await client.submit(*targets[1], child_wire(100, 2));
          for (auto* stages : {&a1, &b1})
              co_await client.accepted(*stages, "grouped request rejected");
          const auto head = co_await client.durable(blocker);
          const auto kept = co_await drive.lifecycle(std::move(a1.result));
          const auto logged = co_await drive.lifecycle(std::move(b1.result));
          const auto& a = durable(kept);
          require(
            a.block.records.bytes().begin() == head.boundary.end().bytes
              && (a.wal || a.segment) && !segments[0]->failure().failed(),
            "a failed segment revoked another segment's durable member");
          co_await covered<Clock>(append, drive, timer);
          const auto& b = durable(logged);
          require(
            b.wal && !b.segment && segments[1]->failure().failed(),
            "a member whose WAL barrier passed was not durable by it alone");
          // The failure is sticky for that segment only.
          const auto later = co_await client.settle(
            *targets[0], child_wire(102));
          require(
            durable(later).block.records.bytes().begin()
              == a.boundary.end().bytes,
            "the healthy segment did not continue after the failure");
          auto refused = co_await client.submit(
            *targets[1], child_wire(101, 2));
          const auto decision = co_await drive.lifecycle(
            std::move(refused.accepted));
          const auto outcome = co_await drive.lifecycle(
            std::move(refused.result));
          require(
            !decision.accepted()
              && outcome.status == local_append_status::not_written,
            "the fenced segment accepted a later request");
          // The failed segment holds the fact of what it published, and its
          // reported prefix ends there.
          const auto* published = take(append.completed_retries(*targets[1]));
          require(
            published->facts() == 1
              && published->find(completed_fact(b).id()) == completed_fact(b)
              && take(append.reported(*targets[1]))
                   == reported_at(spec, b.boundary),
            "a failed segment's facts do not end at its reported prefix");
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
          // discharged prefix stops where that group's PREPAREs begin, the
          // unit after the blocker's.
          co_await covered<Clock>(append, drive, timer);
          const auto [open, obligated] = obligations(append);
          const auto file = writer.progress()->reserved.incarnation();
          const auto blocked = prepare_at(
            child_wire(100), file, 8192, 1, head.block);
          require(
            open.size() == 1 && obligated.obligations == 1
              && open[0].segment == context(2) && open[0].wal == file
              && open[0].pending == 0 && open[0].pinned == 1
              && obligated.discharged.incarnation() == file
              && obligated.discharged.position().value()
                   == (8192 + blocked.size() + 8191) / 8192 * 8192
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
          caller client{append, budget, work, drive};
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
                  co_await client.request(child_wire(100)),
                  work,
                  interest{timer, gone, std::nullopt}),
                errc::aborted);
          }
          {
              seastar::abort_source waiting;
              co_await rejected(
                append.append(
                  *targets[0],
                  co_await client.request(child_wire(100)),
                  work,
                  interest{timer, waiting, Clock::now()}),
                errc::timed_out);
          }
          require(
            segments[0]->progress()->reserved == empty
              && uncharged(budget, charged),
            "a rejected interest left positions or charges behind");
          seastar::abort_source waiting;
          auto watched = append.append(
            *targets[0],
            co_await client.request(child_wire(100)),
            work,
            interest{timer, waiting, std::nullopt});
          co_await client.accepted(watched, "watched request rejected");
          waiting.request_abort();
          const auto ended = co_await drive.lifecycle(
            std::move(watched.result));
          if (deterministic || !ended)
              require(
                !ended && ended.error().code() == errc::aborted,
                "an aborted interest did not end with aborted");
          else
              static_cast<void>(durable(*ended));
          // Accepted, then its result dropped.
          static_cast<void>(co_await client.accept(
            *targets[0], child_wire(101), "dropped request rejected"));
          const auto next = co_await client.settle(
            *targets[0], child_wire(102));
          const auto& receipt = durable(next);
          co_await covered<Clock>(append, drive, timer);
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
          caller client{append, budget, work, drive};
          auto blocker = co_await client.accept(
            *targets[0], child_wire(100), "blocking request rejected");
          seastar::abort_source waiting;
          const auto deadline = Clock::now().checked_add(
            runtime::monotonic_duration{1});
          require(deadline.has_value(), "deadline overflow");
          auto timed = append.append(
            *targets[0],
            co_await client.request(child_wire(101)),
            work,
            local_append_interest<Timer>{timer, waiting, *deadline});
          co_await client.accepted(timed, "timed request rejected");
          const auto ended = co_await drive.lifecycle(std::move(timed.result));
          require(
            !ended && ended.error().code() == errc::timed_out,
            "the deadline did not end the caller's interest");
          const auto head = co_await client.durable(blocker);
          const auto next = co_await client.settle(
            *targets[0], child_wire(102));
          co_await covered<Clock>(append, drive, timer);
          require(
            durable(next).block.records.bytes().begin()
                > head.boundary.end().bytes
              && segments[0]->progress()->durable.blocks == 3,
            "a timed-out waiter cancelled its append");
      });
}

// Completed facts are derived, never supplied: each batch published durable
// becomes its stream's newest fact with its original digest, span and
// binding, a stream keeps its last R, and its later batches open no prefix
// and reserve no seal entry past R. Detaching hands the facts and the newest
// durable footer to the segment's seal, which takes them in batch identity
// order.
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
          caller client{append, budget, work, drive};
          const std::uint32_t depth = segments[0]->retry_depth();
          const auto count = depth + 2;
          std::vector<completed_retry> published;
          published.reserve(count);
          std::optional<local_append_receipt> newest;
          for (std::uint32_t i = 0; i != count; ++i) {
              const auto outcome = co_await client.settle(
                *targets[0], child_wire(100 + i));
              newest = durable(outcome);
              published.push_back(completed_fact(*newest));
              const auto* derived = take(append.completed_retries(*targets[0]));
              const auto& fact = published.back();
              require(
                derived->prefixes() == 1
                  && derived->facts() == std::min(i + 1, depth)
                  && derived->find(fact.id()) == fact
                  && derived->last(retry_prefix::of(fact.id())) == fact,
                "a published batch is not its stream's newest fact");
          }
          const auto* derived = take(append.completed_retries(*targets[0]));
          for (std::uint32_t i = 0; i != count; ++i)
              require(
                derived->find(published[i].id()).has_value()
                  == (i >= count - depth),
                "a stream did not keep exactly its last R facts");
          require(
            segments[0]->progress()->reserved.retry_entries == depth,
            "a stream's batches reserved seal entries past its depth");
          co_await covered<Clock>(append, drive, timer);
          auto detached = take(append.detach(*targets[0]));
          // The attachment's newest durable footer leaves with it.
          const auto& last = newest->boundary;
          require(
            detached.boundary
              && detached.boundary->device == spec.owner.device()
              && detached.boundary->history == last.history()
              && detached.boundary->covered == last.covered()
              && detached.boundary->footer == *last.footer(),
            "detach lost the attachment's newest durable footer");
          auto summary = take(
            co_await drive.lifecycle(
              retry_summary_builder::seal(std::move(detached.retry), work)));
          const auto slice = take(
            co_await drive.lifecycle(summary.read(0, depth, work)));
          require(
            summary.completed() == depth
              && std::equal(
                slice.begin(),
                slice.end(),
                published.end() - static_cast<std::ptrdiff_t>(depth),
                published.end()),
            "the summary did not hold the stream's last R facts in order");
          const auto sealed = co_await drive.lifecycle(
            segments[0]->seal(std::move(summary), depth, work));
          require(
            !sealed.failure.failed() && sealed.retry,
            "the segment did not seal with its derived facts");
      });
}

// A segment holds as many streams as its retry depth allows, ⌊65,536 / R⌋:
// 2,048 at R = 32. The next new stream is refused before anything of it is
// written or charged, its segment's owner is told to roll it for its retry
// state, and the streams it holds still append.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> stream_limit_rolls(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    constexpr std::uint8_t depth = 32;
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 1,
       .wal_capacity = byte_count{16_MiB},
       .retry_depth = depth},
      [&](auto& append, auto& writer, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          std::uint32_t refusals = 0;
          segment_roll_reasons reasons;
          take(
            segments[0]->observe_roll(
              segment_roll_observer{
                [](runtime::monotonic_time) noexcept {},
                [&](segment_roll_reasons why) noexcept {
                    ++refusals;
                    reasons = why;
                }}));
          caller client{append, budget, work, drive};
          const auto limit = retry_summary_builder::prefix_limit(depth);
          // Each request a stream of its own, submitted in bursts that the
          // WAL takes as few groups.
          constexpr std::uint32_t burst = 16;
          std::vector<local_append_stages> staged;
          staged.reserve(burst);
          for (std::uint32_t i = 0; i != limit; ++i) {
              staged.push_back(
                co_await client.submit(
                  *targets[0], child_wire(100 + i, 1, i + 1)));
              if (staged.size() != burst && i + 1 != limit) continue;
              for (auto& stages : staged)
                  static_cast<void>(
                    durable(co_await settle(std::move(stages), drive)));
              staged.clear();
          }
          co_await covered<Clock>(append, drive, timer);
          const auto segment_before = segments[0]->progress()->reserved;
          const auto wal_before = writer.progress()->reserved;
          const auto charged = budget.snapshot();
          const auto* derived = take(append.completed_retries(*targets[0]));
          require(
            derived->prefixes() == limit && derived->facts() == limit
              && segment_before.retry_entries == limit && refusals == 0,
            "a segment did not take as many streams as its depth allows");
          auto beyond = co_await client.submit(
            *targets[0], child_wire(100 + limit, 1, limit + 1));
          const auto decision = co_await drive.lifecycle(
            std::move(beyond.accepted));
          const auto outcome = co_await drive.lifecycle(
            std::move(beyond.result));
          require(
            !decision.accepted() && !decision.failure.failed()
              && decision.capacity == segment_capacity_decision::roll_required
              && outcome.status == local_append_status::not_written
              && !outcome.receipt && refusals == 1
              && reasons
                   == segment_roll_reasons{segment_roll_reason::retry_state},
            "a stream past the segment's limit was not refused for its "
            "retry state");
          require(
            segments[0]->progress()->reserved == segment_before
              && writer.progress()->reserved == wal_before
              && uncharged(budget, charged)
              && take(append.completed_retries(*targets[0]))->prefixes()
                   == limit,
            "a refused stream left positions, charges or a prefix behind");
          // Its logical position is still free for a stream the segment
          // holds.
          const auto kept = co_await client.settle(
            *targets[0], child_wire(100 + limit, 1, 1));
          const auto* after = take(append.completed_retries(*targets[0]));
          require(
            after->facts() == limit + 1
              && after->find(completed_fact(durable(kept)).id())
                   == completed_fact(durable(kept)),
            "a stream the segment holds stopped appending at the limit");
      });
}

// Each WAL file's open obligations toward each segment, and the discharged
// prefix: open while a group's segment barrier is pending, gone once every
// group is segment-durable. What is open while a group is in flight is
// looked at only with `ordered`, where no I/O overtakes an acceptance and no
// acceptance overtakes the group it formed; what is left once every group is
// segment-durable is looked at everywhere.
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
  Timer& timer,
  bool ordered) {
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
          caller client{append, budget, work, drive};
          const auto start = writer.progress()->reserved;
          const auto file = start.incarnation();
          auto blocker = co_await client.accept(
            *targets[0], child_wire(100), "blocking request rejected");
          auto a1 = co_await client.submit(*targets[0], child_wire(101));
          auto b1 = co_await client.submit(*targets[1], child_wire(100, 2));
          for (auto* stages : {&a1, &b1})
              co_await client.accepted(*stages, "grouped request rejected");
          if (ordered) {
              // Only the blocker's group reached the WAL so far.
              const auto [early, during] = obligations(append);
              require(
                early.size() == 1 && early[0].wal == file
                  && early[0].segment == context(1) && early[0].pending == 1
                  && early[0].pinned == 0 && during.discharged == start,
                "an in-flight group's obligation was not open");
              // No barrier has covered a group yet: nothing to pin.
              require(
                newest_footers(append).empty(),
                "a footer was reported before its barrier");
          }
          require(
            !append.reclaimable(file), "the WAL's head file was called free");
          for (auto* stages : {&blocker, &a1, &b1})
              static_cast<void>(co_await client.durable(*stages));
          // Published by a WAL barrier, a group is still open until a
          // barrier of its segment covers it.
          co_await covered<Clock>(append, drive, timer);
          const auto [late, after] = obligations(append);
          require(
            late.empty() && after.obligations == 0
              && after.discharged == writer.progress()->reserved
              && after.wal_durable == writer.progress()->durable,
            "segment-durable groups left obligations open");
          // Each attachment reports its own newest durable footer.
          const auto newest = newest_footers(append);
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
          caller client{append, budget, work, drive};
          seastar::abort_source environment;
          take(append.bind_shutdown(environment));
          auto accepted = co_await client.accept(
            *targets[0], child_wire(100), "request rejected before shutdown");
          environment.request_abort();
          auto late = co_await client.submit(*targets[0], child_wire(101));
          const auto refused = co_await drive.lifecycle(
            std::move(late.accepted));
          const auto unwritten = co_await drive.lifecycle(
            std::move(late.result));
          static_cast<void>(co_await client.durable(accepted));
          co_await covered<Clock>(append, drive, timer);
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

// Each placement waits for one barrier. A WAL-placed request is published
// by its WAL barrier; its segment's is not on its path and runs only when a
// sync asks, so its obligation stays open until then. A segment-placed one
// is published by its segment's barrier and writes nothing to the WAL. The
// caller's environment delays by a second the flush the placement does not
// wait for: the segment's for a WAL-placed request, the WAL's for a
// segment-placed one.
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
  bool segment_placed) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 1,
       .placement = segment_placed
                      ? byte_count{0}
                      : byte_count{std::numeric_limits<std::uint64_t>::max()}},
      [&](auto& append, auto& writer, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          caller client{append, budget, work, drive};
          const auto start = writer.progress()->reserved;
          auto stages = co_await client.accept(
            *targets[0], child_wire(100), "request rejected");
          co_await probe(
            drive, timer, Clock::now().checked_add(half_second).value());
          const auto [open, snapshot] = obligations(append);
          const auto wal = *writer.progress();
          require(
            stages.result.available(),
            "the barrier the placement waits for did not publish the request");
          const auto outcome = co_await drive.lifecycle(
            std::move(stages.result));
          const auto& receipt = durable(outcome);
          if (segment_placed)
              require(
                receipt.placement == local_append_placement::segment
                  && receipt.segment && !receipt.wal
                  && segments[0]->progress()->durable.blocks == 1
                  && wal.reserved == start && wal.durable == start
                  && open.empty() && snapshot.discharged == start,
                "a segment-placed request waited for the WAL or wrote to it");
          else
              require(
                receipt.placement == local_append_placement::wal && receipt.wal
                  && !receipt.segment
                  && segments[0]->progress()->durable.blocks == 0
                  && wal.durable == wal.reserved && !(wal.durable == start)
                  && open.size() == 1 && open[0].pending == 1
                  && snapshot.discharged == start,
                "a WAL-placed request was not published by its WAL barrier, "
                "or its obligation closed with no barrier of its segment");
          co_await covered<Clock>(append, drive, timer);
          require(
            segments[0]->progress()->durable.blocks == 1
              && obligations(append).first.empty(),
            "a sync did not cover the published request");
      });
}

// A slow segment does not hold back another segment's request: completions
// arrive out of call order. Segment-placed, so each request waits for its
// own segment's barrier. The caller's environment delays segment A's first
// append flush.
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
      {.outstanding = 1, .segments = 2, .placement = byte_count{0}},
      [&](auto& append, auto&, auto&, auto& targets, auto& work)
        -> seastar::future<> {
          caller client{append, budget, work, drive};
          auto a1 = co_await client.submit(*targets[0], child_wire(100));
          auto b1 = co_await client.submit(*targets[1], child_wire(100, 2));
          for (auto* stages : {&a1, &b1})
              co_await client.accepted(*stages, "request rejected");
          co_await probe(
            drive, timer, Clock::now().checked_add(half_second).value());
          require(
            b1.result.available() && !a1.result.available(),
            "a slow segment held back another segment's result");
          static_cast<void>(co_await client.durable(b1));
          static_cast<void>(co_await client.durable(a1));
      });
}

// The WAL flush fails after the request's PREPARE took effect: its failure
// asks the segment for a sync before it is reported, and the segment
// barrier rescues the request, held or not; its obligation is discharged
// because the segment holds it; the failure is reported once; later work is
// refused. The caller's environment fails the request's WAL flush.
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
  Timer& timer,
  byte_count hold = {}) {
    std::vector<local_storage_failure> failures;
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 1,
       .failures = &failures,
       .tolerate_close = true,
       .hold = hold},
      [&](auto& append, auto&, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          caller client{append, budget, work, drive};
          auto stages = co_await client.accept(
            *targets[0], child_wire(100), "request rejected");
          const auto outcome = co_await drive.lifecycle(
            std::move(stages.result));
          co_await covered<Clock>(append, drive, timer);
          const auto open = obligations(append).first;
          const auto& receipt = durable(outcome);
          require(
            receipt.segment && !receipt.wal
              && segments[0]->progress()->durable.blocks == 1 && open.empty(),
            "the segment barrier did not rescue a request whose WAL flush "
            "failed, or kept its obligation");
          require(
            failures.size() == 1 && failures[0].path == local_storage_path::wal
              && failures[0].detail == runtime::file_failure_detail::device_io,
            "the WAL failure was not reported once and classified");
          const auto later = co_await drive.lifecycle(
            (co_await client.submit(*targets[0], child_wire(101))).result);
          require(
            later.status == local_append_status::not_written
              && append.failure().failed(),
            "a failed WAL accepted later work or left the owner unlatched");
      });
}

// A segment's barrier flush fails, and then the WAL's: each failure is
// reported, though the owner had already reported one. The first reported
// stays the owner's. Each request still has one durable place: the first
// its WAL barrier, the second its segment's, whose sync the WAL's failure
// asked for before it was reported. The caller's environment fails the
// second segment's next flush and the WAL's second.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> every_failure_reported(
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
      {.outstanding = 1,
       .segments = 2,
       .failures = &failures,
       .tolerate_close = true},
      [&](auto& append, auto&, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          caller client{append, budget, work, drive};
          auto logged = co_await client.accept(
            *targets[1], child_wire(100, 2), "request rejected");
          const auto first = co_await drive.lifecycle(std::move(logged.result));
          co_await covered<Clock>(append, drive, timer);
          require(
            durable(first).wal && !durable(first).segment
              && segments[1]->failure().failed() && failures.size() == 1
              && failures[0].path == local_storage_path::segment
              && failures[0].segment == context(2),
            "the segment failure was not reported");
          // Its sync failed after the request was published: only the WAL
          // holds what was reported, and the report says how far.
          require(
            failures[0].reported
              && *failures[0].reported
                   == reported_at(spec, durable(first).boundary)
              && newest_footers(append).size() == 0,
            "a failed sync's report did not carry the reported prefix past "
            "the durable footer");
          auto later = co_await client.accept(
            *targets[0],
            child_wire(100),
            "a healthy segment refused a request");
          const auto second = co_await drive.lifecycle(std::move(later.result));
          co_await covered<Clock>(append, drive, timer);
          require(
            durable(second).segment && !durable(second).wal
              && failures.size() == 2
              && failures[1].path == local_storage_path::wal
              && failures[1].detail == runtime::file_failure_detail::device_io
              && append.storage_failure()
              && append.storage_failure()->path == local_storage_path::segment,
            "a WAL failure after a segment failure was not reported");
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
// Where the history's request is placed: in the WAL and written to its
// segment at once; in the WAL and held in its segment's run, which only a
// sync writes; or in its segment alone, with nothing in the WAL.
enum class survival_placement : std::uint8_t { wal, held, segment };

// One request and a crash: the surviving bytes follow which flushes took
// effect. Its segment is asked to cover it at once, as a seal asks, so every
// barrier it can have runs: a held run is written first. Its first barrier
// to complete publishes it durable, so it keeps that copy: its WAL barrier,
// unless it has no PREPARE, or its segment's. With none complete it is not
// published, and surviving bytes never make it successful. A second crash
// changes nothing durable. The caller's environment delays (or drops) the
// flushes the history names; a WAL flush a request with no PREPARE does not
// wait for changes nothing of it.
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
  survival_history history,
  survival_placement placed = survival_placement::wal) {
    const bool logged = placed != survival_placement::segment;
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
       .placement = logged
                      ? byte_count{std::numeric_limits<std::uint64_t>::max()}
                      : byte_count{0},
       .hold = placed == survival_placement::held ? byte_count{64_KiB}
                                                  : byte_count{}},
      [&](auto& append, auto& writer, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          const auto wal_start = writer.progress()->reserved;
          const auto data_start
            = segments[0]->progress()->reserved.bytes.value();
          const auto incarnation = writer.prepared_head()->incarnation;
          const bool wal_flushed
            = logged
              && (history == survival_history::both || history == survival_history::wal_only || history == survival_history::lost_wal_completion);
          const bool segment_flushed
            = history == survival_history::both
              || history == survival_history::footer_only
              || history == survival_history::lost_wal_completion;
          const bool published = wal_flushed || segment_flushed;
          caller client{append, budget, work, drive};
          auto stages = co_await client.accept(
            *targets[0], child_wire(100), "request rejected");
          auto draining = append.quiesce(*targets[0]);
          std::optional<local_append_outcome> outcome;
          if (history == survival_history::both) {
              outcome.emplace(
                co_await drive.lifecycle(std::move(stages.result)));
              co_await covered<Clock>(append, drive, timer);
          } else {
              co_await probe(
                drive, timer, Clock::now().checked_add(half_second).value());
              require(
                stages.result.available() == published,
                "a request was published with no barrier complete, or not "
                "by the first to complete");
              if (published)
                  outcome.emplace(
                    co_await drive.lifecycle(std::move(stages.result)));
          }
          require(
            logged || writer.progress()->reserved == wal_start,
            "a request placed in its segment reached the WAL");
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
          const auto at = wal_start.position().value();
          const bool prepare = logged
                               && survived(
                                 wal_image, wal_written, at, at + 8192);
          const bool block = survived(
            data_image, data_written, data_start, data_written.size());
          require(
            prepare == wal_flushed && block == segment_flushed,
            "the crash image does not follow the flushes that took effect");
          require(
            (outcome->status == local_append_status::durable) == published
              && (!published || prepare || block),
            "surviving bytes made a request successful, or a durable "
            "request lost them");
          require(
            !outcome->receipt
              || (outcome->receipt->placement == local_append_placement::wal)
                   == logged,
            "a request was published with another placement than its own");
          require(
            take(append.completed_retries(*targets[0]))->facts()
              == (published ? 1U : 0U),
            "a request that was not published durable left a fact");
          take(co_await drive.lifecycle(files.crash()));
          require(
            co_await read_wal(files, spec, incarnation, drive) == wal_image
              && co_await read_segment(files, spec, 1, drive) == data_image,
            "a second crash changed the durable image");
          // The crash may fail the barrier the quiesce waited for.
          static_cast<void>(co_await drive.lifecycle(std::move(draining)));
      });
}

// Many concurrent requests across two segments, with a window of them
// outstanding: every one becomes durable, and each segment's blocks, cuts and
// WAL receipts follow call order. Acceptance rejects under budget pressure
// rather than waiting, so the caller bounds what is outstanding to what the
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
  Timer& timer,
  placement_profile profile = {}) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.segments = 2, .placement = profile.placement, .hold = profile.hold},
      [&](auto& append, auto&, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          constexpr std::uint32_t requests = 24;
          constexpr std::uint32_t window = 12;
          std::vector<local_append_stages> staged;
          staged.reserve(requests);
          std::vector<local_append_outcome> outcomes;
          outcomes.reserve(requests);
          caller client{append, budget, work, drive};
          for (std::uint32_t i = 0; i != requests; ++i) {
              if (i >= window)
                  outcomes.push_back(
                    co_await settle(std::move(staged[i - window]), drive));
              const auto logical = std::uint64_t{100} + i / 2;
              // Every fifth one is large.
              staged.push_back(
                co_await client.submit(
                  *targets[i % 2],
                  i % 5 == 4 ? valued_wire(logical, large_value, 1 + i % 2)
                             : child_wire(logical, 1 + i % 2),
                  large_width));
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
                  && receipt.boundary.end().bytes
                       >= previous.boundary.end().bytes
                  && (!receipt.wal || !previous.wal
                      || receipt.wal->boundary().cursor().position()
                           >= previous.wal->boundary().cursor().position()),
                "concurrent requests lost call order");
          }
          co_await covered<Clock>(append, drive, timer);
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
          caller client{append, budget, work, drive};
          std::vector<local_append_stages> pending;
          for (std::uint64_t logical = 100; logical != 104; ++logical)
              pending.push_back(
                co_await client.submit(*targets[0], child_wire(logical)));
          for (auto& stages : pending)
              co_await client.accepted(stages, "request rejected before close");
          take(co_await drive.lifecycle(append.close()));
          for (auto& stages : pending)
              static_cast<void>(co_await client.durable(stages));
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
      [&](
        auto& append,
        auto&,
        auto& segments,
        auto& targets,
        auto& work) -> seastar::
                      future<> {
                          caller client{append, budget, work, drive};
                          const auto empty = segments[0]->progress()->reserved;
                          auto input = co_await client.request(child_wire(100));
                          auto& injector
                            = seastar::memory::local_failure_injector();
                          auto cancel = seastar::defer(
                            [&injector] noexcept { injector.cancel(); });
                          auto stages = uninterrupted([&] {
                              injector.fail_after(at);
                              auto output = append.append(
                                *targets[0], std::move(input), work);
                              injected = injector.failed();
                              injector.cancel();
                              return output;
                          });
                          const auto decision = co_await drive.lifecycle(
                            std::move(stages.accepted));
                          const auto outcome = co_await drive.lifecycle(
                            std::move(stages.result));
                          const bool written = outcome.status
                                               == local_append_status::durable;
                          if (decision.accepted())
                              require(
                                written
                                  || outcome.status
                                       == local_append_status::not_written,
                                "an accepted request did not settle");
                          else
                              require(
                                outcome.status
                                    == local_append_status::not_written
                                  && segments[0]->progress()->reserved == empty,
                                "a rejected request left an effect");
                          const auto next = co_await client.settle(
                            *targets[0], child_wire(written ? 101 : 100));
                          require(
            durable(next).block.records.bytes().begin()
              == (written ? outcome.receipt->boundary.end().bytes
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
          caller client{append, budget, work, drive};
          const auto wal_start = writer.progress()->reserved;
          auto input = co_await client.request(child_wire(100));
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
            (co_await client.submit(*targets[0], child_wire(100))).result);
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
// frozen anything: close publishes it and runs the barrier it is owed. Debug
// mode preempts at every await, so this synchronous window does not exist
// there.
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
          caller client{append, budget, work, drive};
          auto input = co_await client.request(child_wire(100));
          auto stages = uninterrupted(
            [&] { return append.append(*targets[0], std::move(input), work); });
          require(
            append.groups() == 1 && append.forming() == 0,
            "the request did not form its group synchronously");
          auto closing = append.close();
          take(co_await drive.lifecycle(std::move(closing)));
          co_await client.accepted(stages, "request rejected");
          static_cast<void>(co_await client.durable(stages));
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
          caller client{append, budget, work, drive};
          auto input = co_await client.request(child_wire(100));
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
          // Sparse data, even at its original placement.
          co_await client.refused(
            co_await client.submit(*targets[0], assigned_wire(false, 32, true)),
            errc::invalid_argument);
          // Bound to another generation: a relocation needs an installer.
          co_await client.refused(
            co_await client.submit(*targets[0], child_wire(101, 2)),
            errc::wrong_context);
          // A routing epoch the child was not bound under.
          auto rerouted = co_await client.request(child_wire(101));
          rerouted.routing_epoch = model::range_routing_epoch::make(2).value();
          co_await client.refused(
            append.append(*targets[0], std::move(rerouted), work),
            errc::wrong_context);
          require(
            segments[0]->progress()->reserved == reserved
              && uncharged(budget, charged) && !segments[0]->failure().failed(),
            "a rejected child left positions, charges or a fence behind");
          const auto next = co_await client.settle(
            *targets[0], child_wire(101));
          require(
            durable(next).block.records.bytes().begin()
              == head.boundary.end().bytes,
            "rejections moved the next request's position");
      });
}

// A request that would take its segment's forming members past one WAL
// group waits until a formation takes them, then starts the next group:
// nothing is refused, and call order holds. A 64-KiB WAL file holds seven
// of these PREPAREs after its header, and ten are offered while formation
// is held.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> full_group_starts_next(
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
      {.outstanding = 1, .wal_capacity = byte_count{64_KiB}},
      [&](auto& append, auto&, auto&, auto& targets, auto& work)
        -> seastar::future<> {
          caller client{append, budget, work, drive};
          std::vector<local_append_stages> offered;
          {
              const auto held
                = co_await local_append_test_access::hold_formation(append);
              for (std::uint64_t logical = 100; logical != 110; ++logical)
                  offered.push_back(
                    co_await client.submit(
                      *targets[0], valued_wire(logical, 7'000), large_width));
          }
          std::vector<local_append_receipt> receipts;
          for (auto& stages : offered) {
              co_await client.accepted(
                stages, "a request beside a full group was refused");
              receipts.push_back(co_await client.durable(stages));
          }
          for (std::size_t i = 1; i != receipts.size(); ++i)
              require(
                receipts[i].block.records.bytes().begin()
                  >= receipts[i - 1].block.records.bytes().end(),
                "requests of consecutive groups lost call order");
          require(
            receipts[6].boundary == receipts[0].boundary
              && !(receipts[7].boundary == receipts[6].boundary),
            "the full group did not end at seven requests");
      });
}

// Each segment's portion of a group is placed whole, by its size: a mixed
// group offers the WAL exactly its small portion's members, in acceptance
// order, and nothing of its large one, which its segment's barrier alone
// publishes; a group of large portions writes nothing to the WAL; and a
// portion holding a large batch with a small one is placed in its segment
// whole. deterministic is false where I/O may overtake acceptance: then the
// requests may form more than one group.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> placement_by_size(
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
      {.outstanding = 1, .segments = 2, .placement = middle_placement},
      [&](auto& append, auto& writer, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          caller client{append, budget, work, drive};
          const auto other = segments[1]->progress()->reserved;
          // A blocker holds the WAL, so the next three form one group.
          auto blocker = co_await client.accept(
            *targets[0], child_wire(100), "blocking request rejected");
          const std::array wires{
            child_wire(101), valued_wire(100, large_value, 2), child_wire(102)};
          const std::array<std::uint32_t, 3> owners{0, 1, 0};
          std::vector<local_append_stages> grouped;
          for (std::size_t i = 0; i != wires.size(); ++i)
              grouped.push_back(
                co_await client.submit(
                  *targets[owners[i]], wires[i], large_width));
          for (auto& stages : grouped)
              co_await client.accepted(stages, "grouped request rejected");
          const auto first = co_await client.durable(blocker);
          std::vector<local_append_outcome> results;
          for (auto& stages : grouped)
              results.push_back(
                co_await drive.lifecycle(std::move(stages.result)));
          const auto& a1 = durable(results[0]);
          const auto& b1 = durable(results[1]);
          const auto& a2 = durable(results[2]);
          require(
            first.placement == local_append_placement::wal
              && a1.placement == local_append_placement::wal
              && a2.placement == local_append_placement::wal
              && b1.placement == local_append_placement::segment && !b1.wal
              && b1.segment && b1.block.records.bytes().begin() == other.bytes
              && b1.segment->boundary().end().bytes >= b1.boundary.end().bytes,
            "a group's portions were not placed by their size");
          co_await covered<Clock>(append, drive, timer);
          // After the blocker's PREPARE only A1 and A2, back to back, and
          // nothing names segment 2.
          const auto incarnation = writer.prepared_head()->incarnation;
          const auto stored = co_await read_wal(
            files, spec, incarnation, drive);
          const auto blocked = prepare_at(
            child_wire(100), incarnation, 8192, 1, first.block);
          auto at = 8192 + blocked.size();
          const std::array<const local_append_receipt*, 2> logged{&a1, &a2};
          const std::array logged_wires{wires[0], wires[2]};
          for (std::size_t i = 0; i != logged.size(); ++i) {
              const auto stamps = stamps_of(
                std::string_view{stored}.substr(at));
              const auto expected = prepare_at(
                logged_wires[i],
                incarnation,
                at,
                1,
                logged[i]->block,
                8192,
                stamps);
              require(
                stored.substr(at, expected.size()) == expected
                  && (!deterministic || stamps.ends_write == (i == 1)),
                "the WAL did not hold exactly the small portion's members in "
                "acceptance order");
              at += expected.size();
          }
          if (deterministic)
              require(
                writer.progress()->reserved.position().value() == at,
                "the WAL holds more than the small portion's members");

          // A group of large portions only: nothing reaches the WAL.
          const auto groups = writer.statistics().accepted_groups;
          const auto reserved = writer.progress()->reserved;
          const auto large = co_await client.settle(
            *targets[1], valued_wire(101, large_value, 2), large_width);
          const auto& alone = durable(large);
          require(
            alone.placement == local_append_placement::segment && !alone.wal
              && alone.segment && writer.statistics().accepted_groups == groups
              && writer.progress()->reserved == reserved,
            "a group of large portions reached the WAL");

          // A small batch and a large one in one forming set, held behind
          // a blocker: its portion is placed in its segment whole.
          auto holding = co_await client.accept(
            *targets[0], child_wire(103), "blocking request rejected");
          auto small = co_await client.submit(*targets[1], child_wire(102, 2));
          auto big = co_await client.submit(
            *targets[1], valued_wire(103, large_value, 2), large_width);
          for (auto* stages : {&small, &big})
              co_await client.accepted(*stages, "forming request rejected");
          static_cast<void>(co_await client.durable(holding));
          const auto& together = co_await client.durable(small);
          const auto& whole = co_await client.durable(big);
          require(
            whole.placement == local_append_placement::segment && !whole.wal
              && (!deterministic
                  || (together.placement == local_append_placement::segment
                      && !together.wal && together.boundary == whole.boundary
                      && writer.statistics().accepted_groups == groups + 1)),
            "a portion with a large batch was not placed in its segment "
            "whole");
      });
}

// What one threshold made of the same requests: each result's placement,
// each segment's reported prefix, both segments' bytes, and the WAL groups
// accepted; what the owner counted, and the flushes of both segments' data
// files while the requests ran, where the backend counts them.
struct placement_trace final {
    std::vector<local_append_placement> placements;
    std::array<std::optional<local_reported_boundary>, 2> reported;
    std::array<std::string, 2> data;
    std::uint64_t wal_groups{0};
    bool wal_written{false};
    local_append_statistics statistics;
    std::optional<std::uint64_t> flushes;
};
// A backend that does not count a file's flushes.
struct uncounted_flushes final {
    std::optional<std::uint64_t> operator()(const runtime::file_path&) const {
        return std::nullopt;
    }
};

// The same requests at a threshold: small and large ones in both segments,
// each waited for until every barrier it is owed has run. Results, reported
// prefixes and segment bytes must not depend on the threshold, which only
// chooses each portion's place.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer,
  typename Count = uncounted_flushes>
seastar::future<placement_trace> placement_run(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer,
  byte_count placement,
  byte_count hold = {},
  Count flushes = {}) {
    placement_trace trace;
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 1, .segments = 2, .placement = placement, .hold = hold},
      [&](auto& append, auto& writer, auto&, auto& targets, auto& work)
        -> seastar::future<> {
          caller client{append, budget, work, drive};
          const auto start = writer.progress()->reserved;
          const auto flushed = [&]() -> std::optional<std::uint64_t> {
              const auto first = flushes(segment_path(spec, 1));
              const auto second = flushes(segment_path(spec, 2));
              if (!first || !second) return std::nullopt;
              return *first + *second;
          };
          const auto before = flushed();
          for (std::uint64_t i = 0; i != 6; ++i) {
              const auto segment = i % 2;
              const auto logical = 100 + i / 2;
              const bool large = (i / 2 + segment) % 2 == 1;
              auto outcome = co_await client.settle(
                *targets[segment],
                large ? valued_wire(logical, large_value, segment + 1)
                      : child_wire(logical, segment + 1),
                large_width);
              trace.placements.push_back(durable(outcome).placement);
              co_await covered<Clock>(append, drive, timer);
          }
          for (std::uint32_t i = 0; i != 2; ++i)
              trace.reported[i] = take(append.reported(*targets[i]));
          trace.wal_groups = writer.statistics().accepted_groups;
          trace.wal_written = !(writer.progress()->reserved == start);
          trace.statistics = append.statistics();
          if (const auto after = flushed(); before && after)
              trace.flushes = *after - *before;
      });
    for (std::uint32_t i = 0; i != 2; ++i)
        trace.data[i] = co_await read_segment(files, spec, i + 1, drive);
    co_return trace;
}

// Runs one of placement_run's requests at three thresholds, each through
// run(threshold): every portion in its segment, by size, every one in the
// WAL. They agree on everything but placement, and only the WAL-placed
// portions reach the WAL. With nothing held they agree on every byte too;
// with a hold, the held portions share footers, so they agree on the
// records each reported prefix covers.
template<typename Run>
seastar::future<> placement_ends(Run run, bool held = false) {
    const auto segment = co_await run(byte_count{0});
    const auto middle = co_await run(middle_placement);
    const auto logged = co_await run(
      byte_count{std::numeric_limits<std::uint64_t>::max()});
    const auto records = [](const placement_trace& trace, std::uint32_t i) {
        const auto& covered = trace.reported[i]->covered;
        return std::pair{covered.logical(), covered.physical()};
    };
    for (const auto* trace : {&segment, &middle, &logged})
        require(
          trace->placements.size() == 6 && segment.reported[0]
            && segment.reported[1] && trace->reported[0] && trace->reported[1]
            && (held ? records(*trace, 0) == records(segment, 0)
                         && records(*trace, 1) == records(segment, 1)
                     : trace->data == segment.data
                         && trace->reported == segment.reported),
          "the threshold changed a result, a reported prefix or a segment's "
          "bytes");
    for (std::size_t i = 0; i != 6; ++i) {
        const bool large = (i / 2 + i % 2) % 2 == 1;
        require(
          segment.placements[i] == local_append_placement::segment
            && logged.placements[i] == local_append_placement::wal
            && middle.placements[i]
                 == (large ? local_append_placement::segment : local_append_placement::wal),
          "a portion was not placed as its size and the threshold say");
    }
    require(
      segment.wal_groups == 0 && !segment.wal_written && middle.wal_groups == 3
        && logged.wal_groups == 6,
      "a portion placed in its segment reached the WAL");
    // Each portion became durable in its segment by one barrier: the one its
    // result waited for when it was placed there, a sync when it was placed
    // in the WAL. So every threshold flushes the segments as often, and none
    // but zero runs a barrier its result waits for.
    constexpr auto wal = static_cast<std::size_t>(local_append_placement::wal);
    constexpr auto in_segment = static_cast<std::size_t>(
      local_append_placement::segment);
    for (const auto* trace : {&segment, &middle, &logged}) {
        const auto& counted = trace->statistics;
        const auto placed = static_cast<std::uint64_t>(std::ranges::count(
          trace->placements, local_append_placement::segment));
        const auto synced
          = counted.runs[static_cast<std::size_t>(segment_run_close::synced)];
        require(
          counted.portions[in_segment] == placed
            && counted.portions[wal] == 6 - placed && counted.barriers == placed
            && counted.syncs == 6 - placed
            && (!trace->flushes || *trace->flushes == 6)
            && counted.held_portions == (held ? 6 - placed : 0)
            && synced == counted.held_portions
            && counted.runs[static_cast<std::size_t>(segment_run_close::filled)]
                 == 0
            && counted.held_bytes == 0 && counted.held_runs == 0
            && counted.unsynced_bytes == 0 && counted.unsynced_files == 0,
          "the owner's counts do not follow what each portion did");
    }
}

// Results publish in their segment's file order, whichever barrier makes
// each durable. S1 (small), L (large) and S2 (small) are each their own
// group in one segment. With the WAL slow, L's segment barrier also covers
// S1 and publishes it while S1's WAL barrier is still pending; S2, which
// waits for the WAL, is not published. With the segment slow, S1 is
// published by its WAL barrier, and S2 waits behind L although its own WAL
// barrier has passed. The caller's environment delays the slow file's
// first flush by a second. With a hold, S1 is held in its segment, and L is
// frozen after a footer of S1's run's own, which L's write and barrier
// carry.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> file_order_publication(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer,
  bool wal_slow,
  byte_count hold = {}) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 1, .placement = middle_placement, .hold = hold},
      [&](
        auto& append,
        auto& writer,
        auto&,
        auto& targets,
        auto& work) -> seastar::
                      future<> {
                          caller client{append, budget, work, drive};
                          // Each forms its own group before the next arrives.
                          const auto formed = [&]() -> seastar::future<> {
                              constexpr runtime::monotonic_duration poll{
                                1'000'000};
                              while (append.forming() != 0)
                                  co_await probe(
                                    drive,
                                    timer,
                                    Clock::now().checked_add(poll).value());
                          };
                          auto s1 = co_await client.accept(
                            *targets[0], child_wire(100), "S1 rejected");
                          co_await formed();
                          auto l = co_await client.accept(
                            *targets[0],
                            valued_wire(101, large_value),
                            "L rejected",
                            large_width);
                          co_await formed();
                          auto s2 = co_await client.accept(
                            *targets[0], child_wire(102), "S2 rejected");
                          co_await probe(
                            drive,
                            timer,
                            Clock::now().checked_add(half_second).value());
                          const auto wal = *writer.progress();
                          const bool first = s1.result.available();
                          const bool second = l.result.available();
                          const bool third = s2.result.available();
                          const auto* builder = take(
                            append.completed_retries(*targets[0]));
                          const auto derived = builder->facts();
                          const auto one = co_await drive.lifecycle(
                            std::move(s1.result));
                          if (wal_slow) {
                              require(
                                first && second && !third
                                  && durable(one).segment && !durable(one).wal,
                                "a segment barrier did not publish the "
                                "WAL-placed group "
                                "before it, or published one waiting for the "
                                "WAL");
                          } else {
                              require(
                                first && !second && !third && durable(one).wal
                                  && !durable(one).segment
                                  && wal.durable == wal.reserved,
                                "a group was published past one whose barrier "
                                "had not "
                                "completed");
                          }
                          // A fact is derived as its branch is published,
                          // never past one still undecided.
                          require(
                            derived
                              == static_cast<std::uint32_t>(first)
                                   + static_cast<std::uint32_t>(second)
                                   + static_cast<std::uint32_t>(third),
                            "a batch's fact was derived before its branch "
                            "was published");
                          const auto two = co_await drive.lifecycle(
                            std::move(l.result));
                          const auto three = co_await drive.lifecycle(
                            std::move(s2.result));
                          const auto& a = durable(one);
                          const auto& b = durable(two);
                          const auto& c = durable(three);
                          // Held, S1 ends with no footer, and its run's own
                          // comes before L.
                          const auto closed = (a.boundary.end().bytes.value()
                                               + 248 + 8191)
                                              / 8192 * 8192;
                          require(
            a.placement == local_append_placement::wal
              && b.placement == local_append_placement::segment
              && c.placement == local_append_placement::wal
              && a.boundary.footer().has_value() == (hold.value() == 0)
              && b.block.records.bytes().begin().value()
                   == (hold.value() == 0 ? a.boundary.end().bytes.value()
                                         : closed)
              && c.block.records.bytes().begin() == b.boundary.end().bytes,
            "the groups were not placed and ordered as written");
                          co_await covered<Clock>(append, drive, timer);
                          require(
                            take(append.reported(*targets[0]))
                              == reported_at(spec, c.boundary),
                            "the reported prefix did not reach the last group");
                          const auto* facts = take(
                            append.completed_retries(*targets[0]));
                          require(
                            facts->facts() == 3
                              && facts->find(completed_fact(a).id())
                                   == completed_fact(a)
                              && facts->find(completed_fact(b).id())
                                   == completed_fact(b)
                              && facts->find(completed_fact(c).id())
                                   == completed_fact(c),
                            "a published batch's fact was lost");
                      });
}

// WAL-placed groups held in their segment. Each request is published by its
// WAL barrier while its segment writes and syncs nothing: the reported
// prefix advances, readers are told so, and the newest durable footer
// stays. A sync asked of the attachment closes the run with one footer,
// writes it and syncs it once: that footer then covers every reported
// request, whose blocks lie back to back from the data start, and the
// obligations close. Requests held again lie back to back from that
// footer's end, are synced by a quiesce, and the attachment detaches with a
// footer covering everything reported.
// `flushes` counts a file's flushes where the backend can.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer,
  typename Count>
seastar::future<> held_until_synced(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer,
  Count flushes) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 1, .hold = byte_count{64_KiB}},
      [&](auto& append, auto&, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          caller client{append, budget, work, drive};
          auto& segment = *segments[0];
          const auto empty = segment.progress()->reserved;
          const auto path = segment_path(spec, 1);
          const auto before = flushes(path);
          std::vector<std::string> wires;
          std::vector<local_append_receipt> receipts;
          // Where the next held block begins, once a block was held.
          std::optional<runtime::file_position> follows;
          const auto held = [&](std::uint64_t logical) -> seastar::future<> {
              wires.push_back(child_wire(logical));
              auto outcome = co_await client.settle(*targets[0], wires.back());
              const auto& receipt = durable(outcome);
              const auto begins = receipt.block.records.bytes().begin();
              require(
                receipt.wal && !receipt.segment && !receipt.boundary.footer()
                  && (!follows || begins == *follows)
                  && take(append.reported(*targets[0]))
                       == reported_at(spec, receipt.boundary)
                  && segment.progress()->reported.bytes
                       == receipt.boundary.end().bytes,
                "a WAL-placed request was not held: published by its WAL "
                "barrier, back to back and reported");
              receipts.push_back(receipt);
              follows = receipt.boundary.end().bytes;
          };
          for (std::uint64_t logical = 100; logical != 104; ++logical)
              co_await held(logical);
          {
              const auto [open, snapshot] = obligations(append);
              require(
                segment.progress()->written == empty
                  && segment.progress()->durable == empty
                  && newest_footers(append).empty() && flushes(path) == before
                  && open.size() == 1 && open[0].pending == 4,
                "a held run was written, synced or discharged with no sync "
                "asked for");
              // One run holds the four, reported past every durable footer
              // and kept in the head alone.
              const auto counted = append.statistics();
              require(
                counted.held_portions == 4 && counted.held_runs == 1
                  && counted.held_bytes == segment.held().value()
                  && counted.held_bytes != 0 && counted.unsynced_bytes != 0
                  && counted.unsynced_files == 1 && counted.syncs == 0,
                "the owner's counts disagree with the run it holds");
          }
          take(co_await drive.lifecycle(append.sync(*targets[0])));
          {
              const auto newest = newest_footers(append);
              const auto& last = receipts.back().boundary;
              require(
                newest.size() == 1
                  && newest[0].footer.begin() == last.end().bytes
                  && segment.progress()->durable.bytes == newest[0].footer.end()
                  && segment.progress()->written == segment.progress()->durable
                  && obligations(append).first.empty()
                  && take(append.reported(*targets[0]))
                       == reported_at(spec, last)
                  && (!before || flushes(path) == *before + 1),
                "a sync did not close, write and sync the held run once");
              const auto counted = append.statistics();
              require(
                counted
                      .runs[static_cast<std::size_t>(segment_run_close::synced)]
                    == 1
                  && counted.syncs == 1 && counted.held_runs == 0
                  && counted.held_bytes == 0 && counted.unsynced_bytes == 0
                  && counted.unsynced_files == 0,
                "the owner's counts disagree with the sync that wrote the "
                "run");
              const auto data = co_await read_segment(files, spec, 1, drive);
              for (std::size_t i = 0; i != receipts.size(); ++i) {
                  const auto& block = receipts[i].block;
                  const auto placed = block_at(wires[i], 1, block);
                  require(
                    data.substr(
                      block.records.bytes().begin().value(), placed.size())
                      == placed,
                    "a held block was not written where it was frozen");
              }
              follows = newest[0].footer.end();
          }
          for (std::uint64_t logical = 104; logical != 106; ++logical)
              co_await held(logical);
          take(co_await drive.lifecycle(append.quiesce(*targets[0])));
          const auto detached = take(append.detach(*targets[0]));
          const auto reported_end = receipts.back().boundary.end().bytes;
          require(
            detached.boundary
              && detached.boundary->footer.begin() == reported_end
              && segment.progress()->durable.bytes
                   == detached.boundary->footer.end()
              && obligations(append).first.empty(),
            "a quiesced attachment detached with reported requests its "
            "footer does not cover");
      });
}

// The WAL moving to a new file syncs exactly the segments whose WAL-placed
// groups the file left behind still holds, once each; one that only wrote
// into the new head is not synced.
// Two PREPAREs fill a file here. `flushes` counts a file's flushes where the
// backend can.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer,
  typename Count>
seastar::future<> rotation_syncs(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer,
  Count flushes) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 1, .segments = 2, .wal_capacity = byte_count{24_KiB}},
      [&](auto& append, auto& writer, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          caller client{append, budget, work, drive};
          const std::array paths{segment_path(spec, 1), segment_path(spec, 2)};
          const std::array before{flushes(paths[0]), flushes(paths[1])};
          const auto synced = [&](std::uint32_t i) {
              return !before[i]
                       ? std::nullopt
                       : std::optional{*flushes(paths[i]) - *before[i]};
          };
          // Settles until every sync asked for has run.
          const auto settled = [&]() -> seastar::future<> {
              constexpr runtime::monotonic_duration poll{10'000'000};
              for (int i = 0; i != 100; ++i)
                  co_await probe(
                    drive, timer, Clock::now().checked_add(poll).value());
          };
          const auto first = writer.progress()->reserved.incarnation();
          std::optional<local_append_receipt> a;
          for (std::uint64_t logical : {100U, 101U})
              a = durable(
                co_await client.settle(*targets[0], child_wire(logical)));
          const auto b = durable(
            co_await client.settle(*targets[1], child_wire(100, 2)));
          co_await settled();
          const auto second = writer.progress()->reserved.incarnation();
          {
              const auto [open, snapshot] = obligations(append);
              require(
                !(second == first) && writer.statistics().rotations == 1
                  && segments[0]->progress()->durable == a->boundary.end()
                  && segments[1]->progress()->durable.blocks == 0
                  && (!synced(0) || *synced(0) == 1)
                  && (!synced(1) || *synced(1) == 0)
                  && append.reclaimable(first) && open.size() == 1
                  && open[0].segment == context(2) && open[0].wal == second,
                "a rotation did not sync exactly the segment the closed file "
                "held, once");
              // One round asked one sync, and the barrier it ran was that
              // sync: no result waited for a barrier.
              const auto counted = append.statistics();
              require(
                counted.sync_rounds == 1 && counted.round_syncs == 1
                  && counted.syncs == 1 && counted.barriers == 0,
                "the owner's counts disagree with the rotation's one sync");
          }
          // Both segments write into the second file; the third syncs both.
          const auto a3 = durable(
            co_await client.settle(*targets[0], child_wire(102)));
          static_cast<void>(
            durable(co_await client.settle(*targets[1], child_wire(101, 2))));
          co_await settled();
          require(
            writer.statistics().rotations == 2
              && segments[0]->progress()->durable == a3.boundary.end()
              && segments[1]->progress()->durable == b.boundary.end()
              && (!synced(0) || *synced(0) == 2)
              && (!synced(1) || *synced(1) == 1) && append.reclaimable(second),
            "a rotation did not sync each segment the closed file held once");
          // B's last group went to the head, which no rotation has left:
          // it alone keeps a WAL file, the head.
          const auto counted = append.statistics();
          require(
            counted.sync_rounds == 2 && counted.round_syncs == 3
              && counted.syncs == 3 && counted.barriers == 0
              && counted.unsynced_files == 1,
            "the owner's counts disagree with the rotations' syncs");
      });
}

// Held runs give their memory back under pressure instead of refusing for
// good. A request refused for memory while a run is held has the held runs
// written, a sync then settles through the footer that closed them, and the
// next one finds the memory; with nothing held the refusal stands. The run
// holds up to 4 MiB, which its opening reserves.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> pressure_releases_holds(
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
      {.outstanding = 1, .hold = byte_count{4_MiB}},
      [&](auto& append, auto&, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          caller client{append, budget, work, drive};
          auto& segment = *segments[0];
          static_cast<void>(
            durable(co_await client.settle(*targets[0], child_wire(100))));
          require(
            segment.held().value() != 0, "the first request was not held");
          // The children of the next requests are charged before memory is
          // squeezed.
          std::vector<local_append_request> requests;
          for (std::uint64_t logical : {101U, 101U, 102U, 102U})
              requests.push_back(co_await client.request(child_wire(logical)));
          std::vector<workload_reservation> squeeze;
          const auto squeezed = [&] {
              auto amount = budget.limits().bytes.value();
              while (amount >= 4096) {
                  auto held = budget.try_reserve(byte_count{amount});
                  if (held)
                      squeeze.push_back(std::move(*held));
                  else
                      amount /= 2;
              }
          };
          squeezed();
          const auto refused =
            [&](local_append_stages stages) -> seastar::future<> {
              auto decision = co_await drive.lifecycle(
                std::move(stages.accepted));
              auto outcome = co_await drive.lifecycle(std::move(stages.result));
              require(
                !decision.accepted() && decision.failure.error()
                  && decision.failure.error()->code() == errc::queue_full
                  && outcome.status == local_append_status::not_written,
                "a request was not refused for memory");
          };
          co_await refused(
            append.append(*targets[0], std::move(requests[0]), work));
          // The refusal had the run written; its memory returns once hashed.
          for (int i = 0;
               i != 100
               && !(
                 segment.progress()->written == segment.progress()->reserved);
               ++i)
              co_await probe(
                drive,
                timer,
                Clock::now()
                  .checked_add(runtime::monotonic_duration{1'000'000})
                  .value());
          take(co_await drive.lifecycle(segment.digest_caught_up()));
          require(
            segment.held().value() == 0
              && segment.progress()->written == segment.progress()->reserved,
            "a request refused for memory did not have the held run written");
          // The footer written for it is what a barrier takes, with no run
          // held to close.
          take(co_await drive.lifecycle(append.sync(*targets[0])));
          static_cast<void>(durable(
            co_await settle(
              append.append(*targets[0], std::move(requests[1]), work),
              drive)));
          // Nothing left to release: the memory is taken again and the
          // refusal stands.
          take(co_await drive.lifecycle(append.sync(*targets[0])));
          take(co_await drive.lifecycle(segment.digest_caught_up()));
          squeezed();
          co_await refused(
            append.append(*targets[0], std::move(requests[2]), work));
          co_await refused(
            append.append(*targets[0], std::move(requests[3]), work));
          squeeze.clear();
      });
}

// A rotation syncs as many segments at once as the owner lets. Both
// segments wrote into the first file; the second request of the first
// segment moves the WAL on, which asks both for a sync. The caller's
// environment delays each segment's first flush by a second, so half a
// second later exactly as many syncs have started as the owner lets and
// none has finished, whichever segment took the first turn; a sync that
// waited for its turn runs after. Two PREPAREs fill a file. `flushes`
// counts a file's flushes.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer,
  typename Count>
seastar::future<> sync_limit(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer,
  std::uint32_t syncs,
  Count flushes) {
    co_await with_local_append<Clock>(
      files,
      owner,
      spec,
      budget,
      drive,
      timer,
      {.outstanding = 1,
       .segments = 2,
       .wal_capacity = byte_count{24_KiB},
       .syncs = syncs},
      [&](auto& append, auto& writer, auto& segments, auto& targets, auto& work)
        -> seastar::future<> {
          caller client{append, budget, work, drive};
          // The flushes both segments have started.
          const auto started = [&] {
              return flushes(segment_path(spec, 1)).value()
                     + flushes(segment_path(spec, 2)).value();
          };
          static_cast<void>(
            durable(co_await client.settle(*targets[0], child_wire(100))));
          static_cast<void>(
            durable(co_await client.settle(*targets[1], child_wire(100, 2))));
          const auto before = started();
          static_cast<void>(
            durable(co_await client.settle(*targets[0], child_wire(101))));
          co_await probe(
            drive, timer, Clock::now().checked_add(half_second).value());
          require(
            writer.statistics().rotations == 1
              && started() - before == std::min<std::uint64_t>(syncs, 2)
              && segments[0]->progress()->durable.blocks == 0
              && segments[1]->progress()->durable.blocks == 0,
            "the owner ran more syncs at once than it lets, or fewer");
          co_await covered<Clock>(append, drive, timer);
          require(
            segments[0]->progress()->durable.blocks == 2
              && segments[1]->progress()->durable.blocks == 1,
            "a sync that waited for its turn did not run");
      });
}

} // namespace kwaque::storage::testing::local_append_contract
