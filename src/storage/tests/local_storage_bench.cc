#include "src/base/units.h"
#include "src/codec/tests/benchmark_buffer.h"
#include "src/codec/tests/memory_qualification_support.h"
#include "src/codec/xxh3.h"
#include "src/resource/resource_registry.h"
#include "src/runtime/production/clocks.h"
#include "src/runtime/production/timer.h"
#include "src/runtime/testing/test_directory.h"
#include "src/storage/local_append.h"
#include "src/storage/local_paths.h"
#include "src/storage/tests/local_append_test_access.h"
#include "src/storage/tests/local_storage_bench_file.h"
#include "src/storage/tests/segment_bench_fixture.h"
#include "src/storage/tests/segment_writer_contract.h"
#include "src/storage/tests/wal_writer_contract.h"
#include "src/storage/wal_format.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread_cputime_clock.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/later.hh>
#include <seastar/util/tmp_file.hh>

#include <algorithm>
#include <array>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace kwaque::storage::testing::local_storage_bench_support {
namespace {
using bytes::testing::charge;
using store_contract::require;
using store_contract::take;
namespace fixture = segment_bench_support;
namespace installation = installation_contract;
using clock = runtime::production::monotonic_clock;
using owner_type = store_contract::ownership_input;
using wal_type = wal_writer<file_system, owner_type>;
using control_type = wal_type::control_type;
using allocator_type = wal_type::allocator_type;
using segment_type = segment_writer<file_system, owner_type, clock>;
using append_type = local_append<file_system, owner_type, clock>;
#if defined(KWAQUE_LOCAL_STORAGE_TIMING_ONLY)
constexpr bool timing_only = true;
#else
constexpr bool timing_only = false;
#endif
constexpr std::size_t flush_records = 1024;

// Who runs the append: the local append owner, or the calls it makes composed
// in a straight line, either overlapped as the owner overlaps them or with
// each segment write held back until the WAL barrier. The sequential
// composition exists only here, to measure what the overlap is worth.
//
// Two more compositions store the same segment bytes with one barrier per
// group instead of two, to measure what each durable place would cost before
// the append owner is changed to choose one:
// - segment_placed: no WAL record; a group is acknowledged when its segment
//   barriers succeed.
// - wal_placed: a group is acknowledged when its WAL barrier succeeds; its
//   segment groups are written at once and made durable by one barrier per
//   segment after the last group, inside the timed region.
// - wal_held: acknowledged the same way, but its segment groups are frozen
//   and held unwritten until every group is acknowledged; then they are
//   written together, gathered as the writer gathers queued groups, and made
//   durable by one barrier per segment.
enum class scope : std::uint8_t {
    candidate,
    baseline,
    sequential,
    segment_placed,
    wal_placed,
    wal_held
};
const char* scope_name(scope value) noexcept {
    switch (value) {
    case scope::candidate:
        return "candidate";
    case scope::baseline:
        return "baseline";
    case scope::sequential:
        return "sequential";
    case scope::segment_placed:
        return "segment_placed";
    case scope::wal_placed:
        return "wal_placed";
    case scope::wal_held:
        return "wal_held";
    }
    std::abort();
}
// held: the whole burst is accepted while the entrance is held, so each
// segment's requests form one group, as they do behind a busy pipeline.
// serial: each request completes before the next starts; one group each.
// isolated: serial, at planned arrivals 2 ms apart.
// burst: every request at once; groups follow pipeline occupancy.
// paced: planned arrivals at a fixed rate, without waiting for results.
enum class arrival : std::uint8_t { held, serial, isolated, burst, paced };
const char* arrival_name(arrival value) noexcept {
    switch (value) {
    case arrival::held:
        return "held";
    case arrival::serial:
        return "serial";
    case arrival::isolated:
        return "isolated";
    case arrival::burst:
        return "burst";
    case arrival::paced:
        return "paced";
    }
    std::abort();
}

// Request i targets segment i % segments.
struct case_shape final {
    const char* name;
    std::size_t payload_bytes{0};
    bool fragmented{false};
    std::uint32_t requests{32}, segments{1};
    arrival profile{arrival::held};
    std::uint64_t spacing_ns{0};
    // The WAL group commit's deliberate wait for more groups.
    std::uint64_t wait_ns{0};
    [[nodiscard]] std::uint32_t per_segment() const noexcept {
        return requests / segments;
    }
    // Whether the group cut is fixed in advance, so a composed path can
    // repeat it and the stored bytes are known before the run.
    [[nodiscard]] bool fixed() const noexcept {
        return profile == arrival::held || profile == arrival::serial
               || profile == arrival::isolated;
    }
    [[nodiscard]] bool observed() const noexcept {
        return profile == arrival::isolated || profile == arrival::burst
               || profile == arrival::paced;
    }
};
fixture::shape data_shape(const case_shape& selected) {
    fixture::shape result{.name = selected.name};
    result.payload_bytes = selected.payload_bytes;
    result.fragmented = selected.fragmented;
    result.window = 1;
    if (selected.profile == arrival::held) {
        result.groups = 1;
        result.blocks = selected.per_segment();
    } else {
        result.groups = selected.per_segment();
        result.blocks = 1;
    }
    return result;
}

struct native_driver final {
    template<typename T>
    seastar::future<T> lifecycle(seastar::future<T> value) const {
        return value;
    }
};
codec::decode_budget memory() {
    return {byte_count{32_MiB}, byte_count{1_MiB}, charge};
}
model::range_routing_epoch routing() {
    return model::range_routing_epoch::make(1).value();
}
// The broker time every request is assigned at, as the segment fixture
// stamps its blocks.
constexpr runtime::wall_time request_time{1};
template<std::size_t Octets>
std::array<char, 2 * Octets + 1>
hex_digest(std::array<unsigned char, Octets> digest) {
    std::array<char, 2 * Octets + 1> result{};
    constexpr char digits[] = "0123456789abcdef";
    for (std::size_t i = 0; i < digest.size(); ++i) {
        result[2 * i] = digits[digest[i] >> 4U];
        result[2 * i + 1] = digits[digest[i] & 15U];
    }
    return result;
}

// One request: its input until it is offered, a shared copy of its child for
// the exact-byte check, where its block was stored and the certified end its
// block and PREPARE state.
struct pending final {
    std::uint32_t segment{0};
    std::optional<local_append_request> request;
    std::optional<encoded_assigned_batch> alias;
    std::optional<segment_block_layout> block;
    std::optional<runtime::file_position> certified;
};
struct request_sample final {
    std::uint64_t planned{0}, started{0}, accepted{0}, terminal{0};
};

struct measurement final {
    // When the last request was acknowledged, on the timer that measures
    // `epoch`. A wal_placed run is still syncing its segments after it.
    std::uint64_t acknowledged{0};
    std::uint64_t epoch{0}, elapsed{0}, cpu{0}, allocations{0}, tasks{0},
      runtime_operations{0};
    std::uint64_t foreground_turns{0}, peak_admission{0};
    std::uint64_t wal_groups{0}, wal_flushes{0}, wal_writes{0}, footers{0},
      blocks{0};
    // WAL writes durable at completion: a WAL barrier covering only these
    // issues no flush.
    std::uint64_t wal_synchronized{0};
    wal_bench_support::reactor_io io;
    codec::testing::allocation_observation memory;
};

// The fixture and its owners predate this interval. Report new native
// allocation bounds separately from retained fixture/admission charges.
class measure_scope final {
public:
    measure_scope(file_system& files, measurement& output, bool allocations)
      : files_(files)
      , output_(output)
      , observe_allocations_(allocations)
      , before_allocations_(seastar::memory::stats().mallocs())
      , before_tasks_(seastar::engine().get_sched_stats().tasks_processed)
      , before_operations_(files.statistics().accepted)
      , before_io_(wal_bench_support::reactor_io::now()) {
        files_.sample.measuring = true;
#if !defined(KWAQUE_LOCAL_STORAGE_TIMING_ONLY)
        if (observe_allocations_)
            codec::testing::begin_allocation_observation();
#endif
        cpu_ = seastar::thread_cputime_clock::now();
        output_.epoch = measurement_time();
        perf_tests::start_measuring_time();
    }
    ~measure_scope() {
        perf_tests::stop_measuring_time();
        output_.elapsed = measurement_time() - output_.epoch;
        output_.cpu = static_cast<std::uint64_t>(
          (seastar::thread_cputime_clock::now() - cpu_).count());
        files_.sample.measuring = false;
#if !defined(KWAQUE_LOCAL_STORAGE_TIMING_ONLY)
        if (observe_allocations_)
            output_.memory = codec::testing::end_allocation_observation();
#endif
        output_.allocations = seastar::memory::stats().mallocs()
                              - before_allocations_;
        output_.tasks = seastar::engine().get_sched_stats().tasks_processed
                        - before_tasks_;
        output_.runtime_operations = files_.statistics().accepted
                                     - before_operations_;
        output_.io = wal_bench_support::reactor_io::now().since(before_io_);
    }
    measure_scope(const measure_scope&) = delete;
    measure_scope& operator=(const measure_scope&) = delete;

private:
    file_system& files_;
    measurement& output_;
    [[maybe_unused]] bool observe_allocations_;
    std::uint64_t before_allocations_, before_tasks_, before_operations_;
    wal_bench_support::reactor_io before_io_;
    seastar::thread_cputime_clock::time_point cpu_;
};

// The durable local path: the WAL writer with its group commit, the segment
// writers and, for the candidate, the local append owner over them.
struct stack final {
    file_system& files;
    owner_type& owner;
    const local_device_spec& wal_spec;
    const local_device_spec& data_spec;
    workload_budget& budget;
    codec::cooperative_work& work;
    runtime::production::timer& timer;
    std::unique_ptr<control_type> control;
    std::unique_ptr<allocator_type> ids;
    std::unique_ptr<wal_type> writer;
    std::unique_ptr<wal_group_commit> commit;
    std::vector<std::unique_ptr<segment_type>> segments;
    std::unique_ptr<append_type> append;
    std::vector<local_append_target> targets;
};

// A segment's workspace for validating a group's children: the contract's,
// or four times the child from 1 MiB, the ratio the WAL benchmark's 32 MiB
// gives 8 MiB children.
byte_count segment_working(const case_shape& selected) {
    if (selected.payload_bytes >= 4_MiB) return fixture::working_bytes;
    if (selected.payload_bytes >= 1_MiB) return byte_count{4_MiB};
    return byte_count{1_MiB};
}

// The zero-written range a synchronized run's writes fit inside: every child
// with its envelopes and padding, and every footer, rounded up to whole MiB
// and capped by the file's limit. It is written and made durable when the
// file is made, before the timed region, as a recycled file would already be.
byte_count synchronized_window(
  const case_shape& selected, std::uint32_t requests, byte_count limit) {
    const std::uint64_t child = std::max<std::uint64_t>(
      selected.payload_bytes, 4_KiB);
    const std::uint64_t wanted = std::uint64_t{requests} * (child + 16_KiB)
                                 + 1_MiB;
    const std::uint64_t rounded = (wanted + 1_MiB - 1) / 1_MiB * 1_MiB;
    return byte_count{std::min(rounded, limit.value())};
}

seastar::future<> open_stack(
  stack& s,
  const case_shape& selected,
  std::span<fixture::extent_input> inputs,
  scope chosen,
  bool synchronized) {
    s.control = take(
      co_await control_type::open(
        s.files,
        s.owner,
        s.wal_spec,
        0,
        false,
        s.budget,
        store_contract::limits(),
        s.work));
    s.ids = take(allocator_type::make(*s.control, s.budget, 4));
    // Each child's validation workspace: the contract's, or for children of
    // 4 MiB and more the WAL benchmark's.
    const bool large = selected.payload_bytes >= 4_MiB;
    auto wal = wal_writer_contract::configuration();
    wal.capacity_bytes = byte_count{128_MiB};
    if (large) wal.children.working_bytes = byte_count{16_MiB};
    if (synchronized)
        wal.preallocation_bytes = synchronized_window(
          selected, selected.requests, wal.capacity_bytes);
    s.writer = take(
      wal_type::make(
        *s.control,
        *s.ids,
        s.budget,
        wal,
        wal_start_intent::known_unactivated));
    // Each owner's first I/O creates its workload group's I/O class here, so
    // that one-time registration is setup, not measured work.
    take(
      co_await seastar::with_scheduling_group(
        s.budget.scheduling_group(),
        [&s] { return s.writer->bootstrap(s.work); }));
    wal_group_commit_config cohort;
    cohort.maximum_wait = runtime::monotonic_duration{selected.wait_ns};
    s.commit = take(wal_group_commit::make(*s.writer, s.budget, cohort));
    take(s.commit->template start<clock>(s.timer));
    s.segments.reserve(selected.segments);
    for (std::uint32_t i = 0; i < selected.segments; ++i) {
        auto config = segment_writer_contract::configuration();
        config.retry_object = local_object_sequence::make(45 + i).value();
        config.policy = s.work.policy();
        config.admission.working_bytes = segment_working(selected);
        if (synchronized)
            config.preallocation_bytes = synchronized_window(
              selected,
              selected.per_segment(),
              inputs[i].descriptor.maximum_data_bytes);
        s.segments.push_back(take(
          segment_type::make_new(
            s.files,
            s.owner,
            s.data_spec,
            0,
            inputs[i].descriptor,
            inputs[i].history.logical_origin,
            s.budget,
            config)));
        auto& created = *s.segments.back();
        take(
          co_await seastar::with_scheduling_group(
            s.budget.scheduling_group(),
            [&created, &s] { return created.create_new(s.work); }));
    }
    if (chosen != scope::candidate) co_return;
    local_append_config config;
    config.maximum_requests = 128;
    config.policy = s.work.policy();
    // Every group in the WAL and written at once, as the compositions it is
    // compared with.
    config.segment_placement_bytes = byte_count{
      std::numeric_limits<std::uint64_t>::max()};
    config.held_run_bytes = byte_count{};
    s.append = take(append_type::make(s.budget, *s.commit, *s.writer, config));
    s.targets.reserve(selected.segments);
    for (auto& segment : s.segments)
        s.targets.push_back(take(s.append->attach(*segment)));
}

template<typename Close>
seastar::future<> observe_close(runtime::first_failure& failed, Close close) {
    try {
        failed.observe(co_await close());
    } catch (...) {
        failed.observe(std::current_exception());
    }
}
// Every owner closes, in reverse, whatever failed before.
seastar::future<> close_stack(stack& s, runtime::first_failure& failed) {
    if (s.append)
        co_await observe_close(failed, [&s] { return s.append->close(); });
    s.append.reset();
    for (auto& segment : s.segments)
        if (segment)
            co_await observe_close(
              failed, [&segment] { return segment->close(); });
    s.segments.clear();
    if (s.commit)
        co_await observe_close(failed, [&s] { return s.commit->close(); });
    s.commit.reset();
    if (s.writer)
        co_await observe_close(failed, [&s] { return s.writer->close(); });
    s.writer.reset();
    if (s.ids) co_await observe_close(failed, [&s] { return s.ids->close(); });
    s.ids.reset();
    if (s.control)
        co_await observe_close(failed, [&s] { return s.control->close(); });
    s.control.reset();
}

// A wal_placed run's segment groups written but not yet durable: each
// segment's newest submitted boundary, and every write still to join. A
// wal_held run's groups are also kept frozen here, in order, until settled.
struct deferred_segments final {
    std::vector<std::optional<segment_captured_boundary>> cuts;
    std::vector<seastar::future<segment_write_completion>> written;
    std::vector<std::pair<std::size_t, segment_frozen_group>> held;
};

// Held groups are submitted first, in the order they were frozen. Then one
// barrier per segment covers every group written to it since the last, and
// every write is joined. A failed step still joins what was entered; a held
// group never submitted is abandoned, which fails its writer.
// A group published by its WAL barrier is synced in its segment only when
// asked. The interval asks and holds it, as it holds the compositions' last
// barriers.
seastar::future<> settle_owner(append_type& owner) {
    for (;;) {
        local_append_test_access::sync_all(owner);
        bool open = owner.groups() != 0;
        if (!open)
            static_cast<void>(
              take(owner.obligations([&open](const local_obligation& one) {
                  open = open || one.pending != 0;
              })));
        if (!open) co_return;
        co_await seastar::sleep(std::chrono::microseconds{10});
    }
}

seastar::future<> settle_deferred(
  stack& s,
  deferred_segments& deferred,
  std::deque<codec::cooperative_work>& branches) {
    runtime::first_failure failed;
    try {
        for (auto& [f, group] : deferred.held) {
            auto submission = take(
              s.segments[f]->submit(std::move(group), branches[f]));
            deferred.cuts[f].emplace(submission.boundary);
            deferred.written.push_back(std::move(submission.written));
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    deferred.held.clear();
    std::vector<std::optional<seastar::future<segment_barrier_outcome>>>
      barriers(deferred.cuts.size());
    for (std::size_t f = 0; f < deferred.cuts.size(); ++f) {
        if (!deferred.cuts[f]) continue;
        barriers[f].emplace(s.segments[f]->barrier(*deferred.cuts[f]));
        deferred.cuts[f].reset();
    }
    std::size_t expected = 0, receipts = 0;
    for (auto& waiting : barriers) {
        if (!waiting) continue;
        ++expected;
        try {
            const auto outcome = co_await std::move(*waiting);
            receipts += outcome.receipt.has_value() ? 1 : 0;
            failed.observe(outcome.failure.outcome());
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    for (auto& waiting : deferred.written) {
        try {
            failed.observe((co_await std::move(waiting)).failure.outcome());
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    deferred.written.clear();
    take(failed.outcome());
    require(
      receipts == expected, "a deferred segment barrier returned no receipt");
}

// The calls the local append owner makes for one group, composed in a
// straight line: validation and aliasing per member, one preparation per
// segment, the WAL admission, the freeze, the offer at final positions and
// the WAL acceptance. Each segment group submits once encoded, before the WAL
// barrier, or after it when sequential; the WAL result and one barrier per
// segment complete every member. Each segment group has its own work
// account, as concurrent branches must.
//
// segment_placed leaves out the WAL admission, offer and barrier: its members
// are complete when their segment barriers are. wal_placed leaves the segment
// barriers to settle_deferred: its members are complete when the WAL barrier
// is. Both keep the validation and aliasing every path shares.
seastar::future<> compose_group(
  stack& s,
  std::span<pending> members,
  scope chosen,
  std::deque<codec::cooperative_work>& branches,
  deferred_segments& deferred,
  measurement& observed) {
    const bool sequential = chosen == scope::sequential;
    const bool logged = chosen != scope::segment_placed;
    const bool held = chosen == scope::wal_held;
    const bool synced = chosen != scope::wal_placed && !held;
    const auto count = members.size();
    const auto segments = s.segments.size();
    std::vector<wal_prepared_children> prepared;
    std::vector<std::optional<segment_frozen_group>> frozen(segments);
    std::vector<std::optional<seastar::future<runtime::result<void>>>> encoded(
      segments);
    std::optional<seastar::future<wal_commit_result>> wal_result;
    std::vector<std::optional<segment_submission>> submitted(segments);
    std::vector<std::optional<seastar::future<segment_barrier_outcome>>>
      barriers(segments);
    runtime::first_failure failed;
    try {
        prepared.reserve(count);
        for (auto& member : members) {
            auto& segment = *s.segments[member.segment];
            const auto current = take(segment.capture());
            const auto history = current.history();
            auto request = std::move(*member.request);
            member.request.reset();
            const wal_child_expectation expected{
              segment_write_context::make(
                history.segment,
                history.alignment,
                current.end().physical,
                current.end().bytes)
                .value(),
              history.data_start,
              request.routing_epoch,
              request.expected,
              s.writer->profile(),
              history.profile};
            auto child = take(
              admitted_wal_batch::make(
                std::move(request.batch),
                std::move(request.backing),
                request.append_time,
                s.writer->child_limits().charge));
            prepared.push_back(take(
              co_await prepare_wal_children(
                std::move(child),
                expected,
                s.budget,
                s.writer->child_limits(),
                s.work)));
        }
        std::vector<std::optional<segment_prepared_group>> preparations(
          segments);
        for (std::size_t f = 0; f < segments; ++f) {
            std::
              array<const encoded_assigned_batch*, maximum_segment_group_blocks>
                children{};
            std::size_t n = 0;
            for (std::size_t i = 0; i < count; ++i)
                if (members[i].segment == f)
                    children[n++] = &prepared[i].segment.batch();
            if (n == 0) continue;
            auto plan = take(s.segments[f]->prepare_group(
              std::span{children}.first(n), s.work));
            require(
              plan.decision == segment_capacity_decision::fits
                && plan.prepared.has_value(),
              "a composed group does not fit its segment");
            preparations[f].emplace(std::move(*plan.prepared));
        }
        std::optional<wal_commit_admission> admitted;
        std::optional<wal_group> offer;
        if (logged) {
            std::array<wal_admission_member, maximum_wal_group_members>
              admission{};
            for (std::size_t i = 0; i < count; ++i)
                admission[i] = {
                  &prepared[i].wal.batch(), &prepared[i].expected};
            auto outcome = take(s.commit->template admit<clock>(
              *s.writer,
              std::span{admission}.first(count),
              s.work.policy(),
              clock::now()));
            require(
              outcome.admission.has_value(),
              "the WAL did not admit a composed group");
            admitted.emplace(std::move(*outcome.admission));
            offer.emplace(take(
              wal_group::make(s.budget, static_cast<std::uint32_t>(count))));
        }
        observed.peak_admission = std::max(
          observed.peak_admission, s.budget.snapshot().bytes);
        for (std::size_t f = 0; f < segments; ++f) {
            if (!preparations[f]) continue;
            std::vector<admitted_wal_batch> children;
            for (std::size_t i = 0; i < count; ++i)
                if (members[i].segment == f)
                    children.push_back(std::move(prepared[i].segment));
            frozen[f].emplace(take(
              co_await s.segments[f]->freeze_group(
                std::move(*preparations[f]), std::move(children), s.work)));
            const auto& layout = frozen[f]->layout();
            const auto history = layout.boundary().history();
            std::size_t k = 0;
            for (std::size_t i = 0; i < count; ++i) {
                if (members[i].segment != f) continue;
                const auto& block = layout.blocks()[k++];
                members[i].block.emplace(block);
                members[i].certified = layout.certified_end();
                prepared[i].expected.target
                  = segment_write_context::make(
                      history.segment,
                      history.alignment,
                      block.records.physical().begin(),
                      block.records.bytes().begin())
                      .value();
            }
            encoded[f].emplace(
              s.segments[f]->encode_group(*frozen[f], branches[f]));
        }
        if (logged) {
            for (std::size_t i = 0; i < count; ++i)
                take(offer->append(
                  std::move(prepared[i].wal),
                  prepared[i].expected,
                  *members[i].certified));
            auto ticket = take(
              co_await s.commit->template submit<clock>(
                *s.writer, std::move(*offer), std::move(*admitted), s.work));
            offer.reset();
            admitted.reset();
            wal_result.emplace(take(ticket.observe()));
        }
        std::optional<wal_commit_result> wal;
        if (sequential) {
            auto waiting = std::move(*wal_result);
            wal_result.reset();
            wal.emplace(co_await std::move(waiting));
        }
        for (std::size_t f = 0; f < segments; ++f) {
            if (!encoded[f]) continue;
            auto waiting = std::move(*encoded[f]);
            encoded[f].reset();
            take(co_await std::move(waiting));
            if (held) {
                deferred.held.emplace_back(f, std::move(*frozen[f]));
                frozen[f].reset();
                continue;
            }
            submitted[f].emplace(
              take(s.segments[f]->submit(std::move(*frozen[f]), branches[f])));
            frozen[f].reset();
        }
        for (std::size_t f = 0; f < segments; ++f) {
            if (!submitted[f]) continue;
            if (synced) {
                barriers[f].emplace(
                  s.segments[f]->barrier(submitted[f]->boundary));
                continue;
            }
            deferred.cuts[f].emplace(submitted[f]->boundary);
            deferred.written.push_back(std::move(submitted[f]->written));
            submitted[f].reset();
        }
        if (logged && !sequential) {
            auto waiting = std::move(*wal_result);
            wal_result.reset();
            wal.emplace(co_await std::move(waiting));
        }
        for (std::size_t f = 0; f < segments; ++f) {
            if (!barriers[f]) continue;
            auto waiting = std::move(*barriers[f]);
            barriers[f].reset();
            const auto outcome = co_await std::move(waiting);
            take(outcome.failure.outcome());
            require(
              outcome.receipt.has_value(),
              "a composed segment barrier returned no receipt");
        }
        if (logged) {
            take(wal->failure().outcome());
            require(
              wal->receipt().has_value(),
              "a composed WAL barrier returned no receipt");
        }
        observed.acknowledged = measurement_time();
    } catch (...) {
        failed.observe(std::current_exception());
    }
    // A failed step must not drop an entered future.
    for (auto& waiting : encoded)
        if (waiting) {
            try {
                failed.observe(co_await std::move(*waiting));
            } catch (...) {
                failed.observe(std::current_exception());
            }
        }
    if (wal_result) {
        try {
            const auto wal = co_await std::move(*wal_result);
            failed.observe(wal.failure().outcome());
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    for (auto& waiting : barriers)
        if (waiting) {
            try {
                failed.observe(
                  (co_await std::move(*waiting)).failure.outcome());
            } catch (...) {
                failed.observe(std::current_exception());
            }
        }
    for (auto& submission : submitted)
        if (submission) {
            try {
                failed.observe(
                  (co_await std::move(submission->written)).failure.outcome());
            } catch (...) {
                failed.observe(std::current_exception());
            }
        }
    take(failed.outcome());
}

// Where each segment's blocks start a new group: a footer follows the last
// block of each group, so the next block does not start at its end.
std::vector<std::uint32_t>
cuts_of(std::span<const pending> members, std::uint32_t segment) {
    std::vector<std::uint32_t> cuts;
    std::optional<runtime::file_position> end;
    for (const auto& member : members) {
        if (member.segment != segment) continue;
        const auto bytes = member.block->records.bytes();
        if (!end || bytes.begin() != *end) cuts.push_back(0);
        ++cuts.back();
        end = bytes.end();
    }
    return cuts;
}
std::vector<std::uint32_t> fixed_cuts(const case_shape& selected) {
    if (selected.profile == arrival::held) return {selected.per_segment()};
    return std::vector<std::uint32_t>(selected.per_segment(), 1);
}

// A little-endian field of `count` bytes stored at `at`.
template<typename File>
seastar::future<std::uint64_t>
stored_field(File& file, std::uint64_t at, std::uint64_t count) {
    auto read = take(
      co_await file.read(runtime::file_position{at}, byte_count{count}));
    require(read.data().size().value() == count, "a stored field was short");
    std::uint64_t value = 0, shift = 0;
    for (auto part : read.data())
        for (std::size_t i = 0; i < part.size(); ++i, shift += 8)
            value |= std::uint64_t{static_cast<unsigned char>(part.data()[i])}
                     << shift;
    co_return value;
}

// The data file holds exactly the expected extent: the children encoded
// under the cut that formed, which for a fixed case is the fixed one. Each
// block states where its file's certified prefix ended when its group
// formed, which depends on which barriers had completed then: the stamp is
// read back and held to its rule (on a boundary between the data start and
// the block, never decreasing, and the one its PREPARE states), and every
// other byte is compared exactly.
seastar::future<std::array<char, 33>> verify_data(
  stack& s,
  const case_shape& selected,
  std::uint32_t segment,
  fixture::extent_input& input,
  std::span<pending> members,
  codec::cooperative_work& work) {
    const auto cuts = cuts_of(members, segment);
    if (selected.fixed())
        require(
          cuts == fixed_cuts(selected),
          "the stored group cut differs from the fixed equal-work cut");
    const auto& history = input.history;
    const auto begin = history.data_start.value();
    const auto path = take(take(local_paths::make(s.data_spec.root))
                             .segment_file(
                               0,
                               {input.descriptor.segment.segment(),
                                input.descriptor.segment.generation()},
                               local_segment_file::data));
    auto file = take(co_await s.files.open(path, {}));
    runtime::first_failure checked;
    std::optional<verified_extent> proof;
    codec::xxh3_128_hasher actual;
    try {
        std::vector<encoded_assigned_batch> children;
        std::vector<runtime::file_position> certified;
        auto floor = history.data_start;
        for (auto& member : members) {
            if (member.segment != segment) continue;
            children.push_back(
              (co_await member.alias->share(memory(), work)).value());
            const auto at = member.block->records.bytes().begin();
            const runtime::file_position stamp{
              co_await stored_field(file, at.value() + 32 + 128, 8)};
            require(
              floor <= stamp && stamp <= at && history.alignment.aligned(stamp)
                && (!member.certified || *member.certified == stamp),
              "a block's certified end is off its boundary, past its block, "
              "decreasing, or not the one its PREPARE states");
            certified.push_back(stamp);
            floor = stamp;
        }
        proof.emplace(
          co_await fixture::encode_cut_extent(
            input, children, cuts, certified, work));
        const auto end = proof->boundary().coverage.bytes().end().value();
        require(
          s.segments[segment]->progress()->durable.bytes.value() == end,
          "the durable segment end differs from the expected extent");
        for (auto position = begin; position != end;) {
            auto read = take(
              co_await file.read(
                runtime::file_position{position},
                byte_count{std::min<std::uint64_t>(65536, end - position)}));
            require(
              read.data().size().value() != 0,
              "data verification reached an early end of file");
            for (auto part : read.data()) {
                actual.update(part.data(), part.size());
                co_await work.drain_inline(
                  byte_count{part.size()}, item_count{1});
            }
            position += read.data().size().value();
        }
    } catch (...) {
        checked.observe(std::current_exception());
    }
    checked.observe(co_await file.close());
    take(checked.outcome());
    const auto digest = std::move(actual).final();
    require(
      digest == proof->digest()->bytes(),
      "stored blocks and footers differ from the expected extent");
    co_return hex_digest(digest);
}

// The WAL holds each request's exact PREPARE, in call order, naming the
// block it was stored as, and nothing after them. Members of one group lie
// back to back and the last ends the write.
seastar::future<std::array<char, 33>> verify_wal(
  stack& s,
  const local_wal_cursor& start,
  std::span<pending> members,
  codec::cooperative_work& work) {
    const auto path = take(
      take(local_paths::make(s.wal_spec.root)).wal(0, start.incarnation()));
    auto file = take(co_await s.files.open(path, {}));
    runtime::first_failure checked;
    codec::xxh3_128_hasher expected_hash;
    try {
        auto position = start.position().value();
        for (auto& member : members) {
            const auto history
              = take(s.segments[member.segment]->capture()).history();
            const auto& block = *member.block;
            auto alias = (co_await member.alias->share(memory(), work)).value();
            // The run starts on a WAL file nothing was written to, so the
            // start is where its header ends.
            const wal_prepare_expectation expected{
              wal_write_context::make(
                start.incarnation(),
                alignment(8192),
                runtime::file_position{position})
                .value(),
              start.position(),
              segment_write_context::make(
                history.segment,
                history.alignment,
                block.records.physical().begin(),
                block.records.bytes().begin())
                .value(),
              history.data_start,
              routing(),
              {history.segment.topic(), history.segment.range()},
              s.writer->profile(),
              history.profile};
            // Whether it ends its write, and where each file's certified
            // prefix ended when its group formed, depend on how groups
            // formed and which barriers had completed: they are read back
            // and held to their rules, and every other byte is compared
            // exactly. Its block must state the same target stamp.
            const bool ends = co_await stored_field(
                                file, position + 32 + 126, 2)
                              != 0;
            const runtime::file_position target{
              co_await stored_field(file, position + 32 + 144, 8)};
            const runtime::file_position certified{
              co_await stored_field(file, position + 32 + 152, 8)};
            require(
              history.data_start <= target
                && target <= block.records.bytes().begin()
                && history.alignment.aligned(target)
                && (!member.certified || *member.certified == target)
                && start.position() <= certified
                && certified.value() <= position
                && certified.value() % 8192 == 0,
              "a PREPARE's certified ends are off their boundaries or past "
              "what they may claim");
            member.certified = target;
            auto encoded = (co_await encode_wal_prepare(
                              std::move(alias),
                              expected,
                              {ends, request_time, target, certified},
                              work,
                              byte_count{32_MiB},
                              charge))
                             .value();
            for (auto part : encoded)
                expected_hash.update(part.data(), part.size());
            std::uint64_t offset = 0;
            while (offset != encoded.size().value()) {
                const auto length = byte_count{std::min<std::uint64_t>(
                  65536, encoded.size().value() - offset)};
                auto read = take(
                  co_await file.read(
                    runtime::file_position{position + offset}, length));
                require(
                  read.data().size() == length, "WAL verification was short");
                require(
                  co_await codec::bench::buffers_equal(
                    read.data(),
                    encoded.share(byte_count{offset}, length).value(),
                    work),
                  "the WAL differs from the exact PREPAREs in call order");
                offset += length.value();
            }
            position += encoded.size().value();
        }
        require(
          take(s.writer->positions()).reserved.position().value() == position,
          "the WAL holds bytes after the measured PREPAREs");
    } catch (...) {
        checked.observe(std::current_exception());
    }
    checked.observe(co_await file.close());
    take(checked.outcome());
    co_return hex_digest(std::move(expected_hash).final());
}

struct verified final {
    std::array<char, 33> wal{};
    std::vector<std::array<char, 33>> segments;
};

struct native_settings final {
    std::string_view comparison;
    std::uint32_t flush_cap{0};
    const char* data_directory{nullptr};
    const char* fixture{nullptr};
    // Every record at time zero, so a format with coarser timestamps stores
    // the same values exactly.
    bool zero_timestamps{false};
    bool memory{false}, foreground{false};
    // Both files zero-written before the timed region, so writes inside them
    // overwrite and small ones go through a synchronized handle.
    bool synchronized{false};
};

wal_bench_support::barrier_kind
barrier_of(const native_settings& settings) noexcept {
    return settings.synchronized ? wal_bench_support::barrier_kind::synchronized
                                 : wal_bench_support::barrier_kind::extending;
}

// Only the observing build reports native work.
[[maybe_unused]] void
print_kind(const char* name, const kind_sample& sample, std::uint64_t epoch) {
    std::printf(
      ",\"%s\":{\"calls\":%" PRIu64 ",\"bytes\":%" PRIu64
      ",\"flushes\":%" PRIu64 ",\"depth\":%" PRIu64 ",\"flush_depth\":%" PRIu64
      ",\"write_service_ns\":%" PRIu64 ",\"write_busy_ns\":%" PRIu64
      ",\"flush_service_ns\":%" PRIu64 ",\"cap_waits\":%" PRIu64
      ",\"cap_wait_ns\":%" PRIu64 ",\"synchronized_calls\":%" PRIu64
      ",\"flush_times_complete\":%s"
      ",\"flush_times\":[",
      name,
      sample.calls,
      sample.bytes,
      sample.flushes,
      sample.depth,
      sample.flush_depth,
      sample.write_service_ns,
      sample.write_busy_ns,
      sample.flush_service_ns,
      sample.cap_waits,
      sample.cap_wait_ns,
      sample.synchronized_calls,
      sample.flush_times_overflowed ? "false" : "true");
    for (std::size_t i = 0; i < sample.flushes_recorded; ++i)
        std::printf(
          "%s[%" PRIu64 ",%" PRIu64 "]",
          i ? "," : "",
          sample.flush_times[i].begin - epoch,
          sample.flush_times[i].end - epoch);
    std::printf("]}");
}

void print_measurement(
  const case_shape& selected,
  scope chosen,
  const native_settings& settings,
  const measurement& value,
  const file_system& files,
  const verified& digests,
  std::span<const request_sample> samples,
  std::uint64_t child_bytes,
  byte_count fixture_bytes,
  std::uint64_t wal_device,
  std::uint64_t data_device,
  const std::set<unsigned>& affinity) {
    std::printf(
      "local_storage_bench_v1 {\"scope\":\"%s\",\"case\":\"%s\","
      "\"arrival\":\"%s\",\"observation\":\"%s\",\"comparison_id\":\"%.*s\","
      "\"topology\":\"%s\",\"fdatasync\":\"%s\",\"wal_digest\":\"%s\"",
      scope_name(chosen),
      selected.name,
      arrival_name(selected.profile),
      selected.observed() ? "requests" : "aggregate",
      static_cast<int>(settings.comparison.size()),
      settings.comparison.data(),
      settings.data_directory ? "separate" : "shared",
      seastar::engine().have_aio_fdatasync() ? "aio" : "thread",
      digests.wal.data());
    auto number = [](const char* name, std::uint64_t n) {
        std::printf(",\"%s\":%" PRIu64, name, n);
    };
    auto flag = [](const char* name, bool set) {
        std::printf(",\"%s\":%s", name, set ? "true" : "false");
    };
    number("payload_bytes", selected.payload_bytes);
    number("requests", selected.requests);
    number("segments", selected.segments);
    number("spacing_ns", selected.spacing_ns);
    number("wait_ns", selected.wait_ns);
    number("flush_cap", settings.flush_cap);
    number("wal_device", wal_device);
    number("data_device", data_device);
    number("child_bytes", child_bytes);
    number("elapsed_ns", value.elapsed);
    number(
      "ack_ns",
      value.acknowledged ? value.acknowledged - value.epoch : value.elapsed);
    number("reactor_cpu_ns", value.cpu);
    number("allocations", value.allocations);
    number("tasks", value.tasks);
    number("runtime_operations", value.runtime_operations);
    number("foreground_turns", value.foreground_turns);
    number("fixture_retained_bound", fixture_bytes.value());
    number("sampled_admission_peak", value.peak_admission);
    number("wal_groups", value.wal_groups);
    number("wal_flushes", value.wal_flushes);
    number("wal_writes", value.wal_writes);
    number("footers", value.footers);
    number("blocks", value.blocks);
    // Without synchronized writes neither file is preallocated: each barrier
    // follows writes that grew its file. With them both files are
    // zero-written, and small writes inside need no flush.
    std::printf(
      ",\"wal_barrier\":\"%s\",\"data_barrier\":\"%s\"",
      wal_bench_support::barrier_name(barrier_of(settings)),
      wal_bench_support::barrier_name(barrier_of(settings)));
    number("wal_synchronized_writes", value.wal_synchronized);
    number("reactor_flushes", value.io.flushes);
    number("reactor_writes", value.io.writes);
    number("reactor_write_bytes", value.io.write_bytes);
    number("reactor_reads", value.io.reads);
    number("reactor_read_bytes", value.io.read_bytes);
    flag("fragmented", selected.fragmented);
    flag("zero_timestamps", settings.zero_timestamps);
    flag("timing_only", timing_only);
#ifdef NDEBUG
    flag("ndebug", true);
#else
    flag("ndebug", false);
#endif
    flag(
      "libcxx_hardening_none",
      _LIBCPP_HARDENING_MODE == _LIBCPP_HARDENING_MODE_NONE);
    flag("foreground_probe", settings.foreground);
    flag("memory_observed", settings.memory);
    flag("complete", true);
    if (settings.memory) {
        number("memory_peak", value.memory.peak_upper_bound);
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)                            \
  && !defined(SEASTAR_DEFAULT_ALLOCATOR)
        flag("critical_observed", true);
        number("critical_peak", value.memory.critical_peak_upper_bound);
#else
        flag("critical_observed", false);
        std::printf(",\"critical_peak\":null");
#endif
        number("largest_allocation", value.memory.largest_allocation);
        flag("memory_complete", value.memory.complete);
    }
    if constexpr (!timing_only) {
        print_kind("wal", files.sample[file_kind::wal], value.epoch);
        print_kind("data", files.sample[file_kind::data], value.epoch);
        print_kind("other", files.sample[file_kind::other], value.epoch);
    }
    std::printf(",\"segment_digest\":[");
    for (std::size_t i = 0; i < digests.segments.size(); ++i)
        std::printf("%s\"%s\"", i ? "," : "", digests.segments[i].data());
    std::printf("],\"samples\":[");
    for (std::size_t i = 0; i < samples.size(); ++i) {
        const auto& r = samples[i];
        std::printf(
          "%s[%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 "]",
          i ? "," : "",
          r.planned - value.epoch,
          r.started - value.epoch,
          r.accepted - value.epoch,
          r.terminal - value.epoch);
    }
    std::printf("],\"affinity_cpus\":[");
    bool first = true;
    for (auto cpu : affinity) {
        std::printf("%s%u", first ? "" : ",", cpu);
        first = false;
    }
    std::puts("]}");
}

struct local_storage_bench {
    local_storage_bench() {
        static const bool initialized = [] {
            codec::testing::report_profile();
            return true;
        }();
        static_cast<void>(initialized);
    }

    seastar::future<std::size_t> run_case(case_shape selected, scope chosen) {
        require(
          selected.segments != 0
            && selected.segments <= fixture::maximum_extent_segments
            && selected.requests % selected.segments == 0
            && selected.requests <= maximum_wal_group_members * 4
            && selected.per_segment() <= fixture::maximum_batches,
          "local append benchmark shape exceeds its bound");
        require(
          chosen == scope::candidate || selected.profile == arrival::held
            || selected.profile == arrival::serial,
          "a composed path repeats a held or serial group cut");
        require(
          selected.profile != arrival::held
            || (selected.requests <= maximum_wal_group_members && selected.per_segment() <= fixture::maximum_group_blocks),
          "a held burst must fit one group");
        native_settings settings;
        const auto* supplied = std::getenv("KWAQUE_LOCAL_APPEND_COMPARISON_ID");
        settings.comparison = supplied ? supplied : "";
        require(
          settings.comparison.empty()
            || (settings.comparison.size() == 64 && settings.comparison.find_first_not_of("0123456789abcdef") == std::string_view::npos),
          "comparison ID must be a SHA-256 context digest");
        if (const auto* cap = std::getenv("KWAQUE_LOCAL_APPEND_FLUSH_CAP")) {
            const auto parsed = std::strtoul(cap, nullptr, 10);
            require(
              parsed <= fixture::maximum_extent_segments,
              "the flush cap exceeds the benchmark's segments");
            settings.flush_cap = static_cast<std::uint32_t>(parsed);
        }
        settings.data_directory = std::getenv(
          "KWAQUE_LOCAL_APPEND_DATA_DIRECTORY");
        settings.fixture = std::getenv("KWAQUE_LOCAL_APPEND_FIXTURE");
        settings.zero_timestamps
          = std::getenv("KWAQUE_LOCAL_APPEND_ZERO_TIMESTAMPS") != nullptr;
        settings.memory = std::getenv("KWAQUE_LOCAL_APPEND_OBSERVE_ALLOCATIONS")
                          != nullptr;
        settings.foreground
          = std::getenv("KWAQUE_LOCAL_APPEND_FOREGROUND_PROBE") != nullptr;
        settings.synchronized = std::getenv("KWAQUE_LOCAL_APPEND_SYNCHRONIZED")
                                != nullptr;
        require(
          !settings.memory || !timing_only,
          "native observation requires local_storage_bench");
        require(
          !settings.synchronized || !timing_only,
          "synchronized writes are counted only by local_storage_bench");
        require(
          !settings.fixture || (selected.segments == 1 && selected.fixed()),
          "fixture export requires one segment and a fixed group cut");
        const auto config = resource::resource_config::from_total_memory(
                              byte_count{
                                seastar::memory::stats().total_memory()})
                              .value();
        resource::resource_registry registry;
        co_await registry.start(config);
        resource::resource_manager manager{registry.handles()};
        runtime::production::timer timer;
        runtime::first_failure failed;
        try {
            co_await manager.start();
            co_await seastar::tmp_dir::do_with(
              runtime::testing::test_directory_template(),
              seastar::coroutine::lambda(
                [&](seastar::tmp_dir& directory) -> seastar::future<> {
                    if (!settings.data_directory) {
                        co_await run_in(
                          selected,
                          chosen,
                          settings,
                          manager,
                          timer,
                          directory.get_path(),
                          std::nullopt);
                        co_return;
                    }
                    co_await seastar::tmp_dir::do_with(
                      std::filesystem::path{settings.data_directory}
                        / "kwaque-local-append-XXXXXX",
                      seastar::coroutine::lambda(
                        [&](seastar::tmp_dir& data) -> seastar::future<> {
                            co_await run_in(
                              selected,
                              chosen,
                              settings,
                              manager,
                              timer,
                              directory.get_path(),
                              data.get_path());
                        }));
                }));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        try {
            failed.observe(co_await timer.stop());
        } catch (...) {
            failed.observe(std::current_exception());
        }
        try {
            co_await manager.stop();
        } catch (...) {
            failed.observe(std::current_exception());
        }
        try {
            co_await registry.stop();
        } catch (...) {
            failed.observe(std::current_exception());
        }
        take(failed.outcome());
        co_return selected.requests;
    }

private:
    seastar::future<> run_in(
      const case_shape& selected,
      scope chosen,
      const native_settings& settings,
      resource::resource_manager& manager,
      runtime::production::timer& timer,
      const std::filesystem::path& store,
      std::optional<std::filesystem::path> data_store) {
        const auto affinity = seastar::get_current_cpuset();
        file_system files{!timing_only, settings.flush_cap};
        const auto wal_root = take(
          runtime::file_path::make((store / "store").string()));
        take(co_await files.create_directories(wal_root));
        const auto wal_status = co_await seastar::file_stat(
          wal_root.value(), seastar::follow_symlink::no);
        const auto wal_spec = store_contract::specification(
          wal_root, {wal_status.device_id, wal_status.inode_number}, 68);
        // A separate data device holds the segments in its own store.
        std::optional<local_device_spec> separate;
        auto data_device = static_cast<std::uint64_t>(wal_status.device_id);
        if (data_store) {
            const auto root = take(
              runtime::file_path::make((*data_store / "store").string()));
            take(co_await files.create_directories(root));
            const auto status = co_await seastar::file_stat(
              root.value(), seastar::follow_symlink::no);
            separate.emplace(
              store_contract::specification(
                root, {status.device_id, status.inode_number}, 69));
            data_device = static_cast<std::uint64_t>(status.device_id);
        }
        const auto& data_spec = separate ? *separate : wal_spec;
        const std::array specs{wal_spec, data_spec};
        owner_type owner{specs};
        // Appends are produce-path work, admitted as the owner's tests admit
        // them. Each segment owner holds its data handle, write staging and
        // metadata workspace, about 8 MiB, for its lifetime. It also keeps its
        // creation publications and two prepared publishers, so it takes the
        // segment benchmark's 16 handles per owner. The WAL owners take the
        // WAL benchmark's 32. Each request's backing, and each segment's
        // workspace for validating a group, are charged on top. The whole
        // must stay within the foreground class's share of the reactor's
        // memory: about 282 MiB at the smoke test's 1 GiB.
        const std::uint64_t owners = selected.segments;
        workload_budget budget{
          manager.acquire_workload(
            resource::workload_class::foreground_protocol),
          {.tasks = 1024,
           .bytes
           = byte_count{(std::max<std::uint64_t>(96, 32 + 8 * owners) << 20U)
                        + std::uint64_t{selected.requests}
                            * selected.payload_bytes
                        + owners * segment_working(selected).value()},
           .handles = static_cast<std::uint32_t>(32 + 16 * owners)},
          charge};
        auto shape = data_shape(selected);
        shape.zero_timestamps = settings.zero_timestamps;
        seastar::abort_source abort;
        codec::cooperative_work work{fixture::policy(shape), abort};
        co_await installation::bootstrap(
          files, owner, wal_spec, budget, work, native_driver{});
        if (separate)
            co_await installation::bootstrap(
              files, owner, *separate, budget, work, native_driver{});

        std::vector<fixture::extent_input> inputs;
        inputs.reserve(selected.segments);
        byte_count fixture_bytes{};
        for (std::uint32_t f = 0; f < selected.segments; ++f) {
            inputs.push_back(co_await fixture::make_extent(shape, f, work));
            fixture_bytes = fixture_bytes
                              .checked_add(
                                fixture::retained_bound(inputs.back()))
                              .value();
        }
        if (settings.fixture) {
            fixture::export_extent(inputs.front(), shape, settings.fixture);
            co_await fixture::export_records(
              inputs.front(), shape, settings.fixture, work);
        }
        // Each request owns its child and backing; its shared copy checks
        // the stored bytes afterwards.
        std::vector<pending> members(selected.requests);
        std::uint64_t child_bytes = 0;
        for (std::uint32_t i = 0; i < selected.requests; ++i) {
            const auto f = i % selected.segments;
            const auto k = i / selected.segments;
            auto& child
              = inputs[f].groups[k / shape.blocks].children[k % shape.blocks];
            child_bytes += child.bytes().size().value();
            members[i].segment = f;
            members[i].alias.emplace(
              (co_await child.share(memory(), work)).value());
            auto backing = take(budget.try_reserve_buffer(child.bytes()));
            const auto history = inputs[f].history;
            members[i].request.emplace(
              local_append_request{
                std::move(child),
                std::move(backing),
                {history.segment.topic(), history.segment.range()},
                routing(),
                request_time,
                std::nullopt});
        }

        stack s{files, owner, wal_spec, data_spec, budget, work, timer};
        std::vector<request_sample> samples(
          selected.observed() ? selected.requests : 0);
        measurement result;
        verified digests;
        runtime::first_failure execution;
        bool stop_foreground = false;
        std::optional<seastar::future<>> foreground_work;
        auto foreground_loop = [&] -> seastar::future<> {
            while (!stop_foreground) {
                co_await seastar::yield();
                if (!stop_foreground) ++result.foreground_turns;
            }
        };
        std::optional<seastar::semaphore_units<>> hold;
        deferred_segments deferred;
        // Requests in flight together never share a work account: each has
        // its own, made before any interval and kept until the owners close.
        std::deque<codec::cooperative_work> accounts;
        try {
            co_await open_stack(
              s, selected, inputs, chosen, settings.synchronized);
            if (chosen == scope::candidate)
                for (std::uint32_t i = 0; i < selected.requests; ++i)
                    accounts.emplace_back(work.policy(), abort);
            std::deque<codec::cooperative_work> branches;
            for (std::uint32_t f = 0; f < selected.segments; ++f)
                branches.emplace_back(work.policy(), abort);
            deferred.cuts.resize(selected.segments);
            deferred.written.reserve(selected.requests + selected.segments);
            deferred.held.reserve(selected.requests + selected.segments);
            const auto start = take(s.writer->positions()).reserved;
            const auto wal_before = s.writer->statistics();
            std::vector<std::uint64_t> footers_before;
            for (const auto& segment : s.segments)
                footers_before.push_back(segment->progress()->durable.footers);
            if (chosen == scope::candidate && selected.profile == arrival::held)
                hold.emplace(
                  co_await local_append_test_access::hold_formation(*s.append));
            if constexpr (!timing_only)
                files.reserve_flush_times(flush_records);
            if (settings.foreground) foreground_work.emplace(foreground_loop());
            result.peak_admission = budget.snapshot().bytes;
            {
                measure_scope interval{files, result, settings.memory};
                if (chosen == scope::candidate) {
                    co_await run_candidate(
                      s, selected, members, accounts, samples, hold, result);
                    result.acknowledged = measurement_time();
                    co_await settle_owner(*s.append);
                } else if (selected.profile == arrival::held)
                    co_await compose_group(
                      s, members, chosen, branches, deferred, result);
                else
                    for (std::uint32_t i = 0; i < selected.requests; ++i)
                        co_await compose_group(
                          s,
                          std::span{members}.subspan(i, 1),
                          chosen,
                          branches,
                          deferred,
                          result);
                if (chosen == scope::wal_placed || chosen == scope::wal_held)
                    co_await settle_deferred(s, deferred, branches);
                require(
                  files.sample.active() == 0,
                  "timing stopped with native writes or flushes in flight");
            }
            stop_foreground = true;
            const auto wal_after = s.writer->statistics();
            result.wal_groups = wal_after.accepted_groups
                                - wal_before.accepted_groups;
            result.wal_flushes = wal_after.flush_calls - wal_before.flush_calls;
            result.wal_writes = wal_after.write_calls - wal_before.write_calls;
            result.wal_synchronized = wal_after.synchronized_writes
                                      - wal_before.synchronized_writes;
            // Each barrier claimed reached the device: the shard issued at
            // least the WAL's own flushes, and a synchronized run completed
            // synchronized writes as well, or flushed where it could not.
            std::uint64_t synchronized_writes = result.wal_synchronized;
            if constexpr (!timing_only)
                synchronized_writes
                  += files.sample[file_kind::data].synchronized_calls;
            require(
              wal_bench_support::barrier_evident(
                barrier_of(settings),
                result.io,
                result.io.flushes + synchronized_writes)
                && result.io.flushes >= result.wal_flushes,
              "claimed barrier reached no device");
            for (std::size_t f = 0; f < s.segments.size(); ++f) {
                const auto& durable = s.segments[f]->progress()->durable;
                result.footers += durable.footers - footers_before[f];
                result.blocks += durable.blocks;
            }
            require(
              result.blocks == selected.requests,
              "durable blocks differ from the offered requests");
            if (selected.fixed())
                require(
                  result.wal_groups
                    == (chosen == scope::segment_placed ? 0
                        : selected.profile == arrival::held
                          ? 1
                          : selected.requests),
                  "the WAL group count differs from the fixed cut");
            digests.wal = co_await verify_wal(
              s,
              start,
              chosen == scope::segment_placed ? std::span<pending>{}
                                              : std::span<pending>{members},
              work);
            for (std::uint32_t f = 0; f < selected.segments; ++f)
                digests.segments.push_back(
                  co_await verify_data(
                    s, selected, f, inputs[f], members, work));
        } catch (...) {
            execution.observe(std::current_exception());
        }
        hold.reset();
        // A failed run abandons the groups it still held, then joins every
        // deferred write it entered.
        deferred.held.clear();
        for (auto& waiting : deferred.written) {
            try {
                execution.observe(
                  (co_await std::move(waiting)).failure.outcome());
            } catch (...) {
                execution.observe(std::current_exception());
            }
        }
        deferred.written.clear();
        stop_foreground = true;
        if (foreground_work) {
            try {
                co_await std::move(*foreground_work);
            } catch (...) {
                execution.observe(std::current_exception());
            }
        }
        co_await close_stack(s, execution);
        take(execution.outcome());
        require(
          seastar::get_current_cpuset() == affinity,
          "benchmark CPU affinity changed during execution");
        if constexpr (!timing_only)
            for (const auto& kind : files.sample.kinds)
                require(
                  !kind.flush_times_overflowed,
                  "flush measurement exceeded its bound");
        if (settings.memory)
            require(
              result.memory.observed && result.memory.complete
                && result.memory.largest_allocation
                     <= maximum_contiguous_allocation_bytes,
              "allocation observation incomplete or exceeded the contiguous "
              "ceiling");
        print_measurement(
          selected,
          chosen,
          settings,
          result,
          files,
          digests,
          samples,
          child_bytes,
          fixture_bytes,
          static_cast<std::uint64_t>(wal_status.device_id),
          data_device,
          affinity);
    }

    // The local append owner. A held burst forms one group per segment once
    // every member is accepted; serial and isolated requests each form their
    // own; burst and paced requests group as pipeline occupancy decides.
    // Observed profiles time every request's two stages.
    static seastar::future<> run_candidate(
      stack& s,
      const case_shape& selected,
      std::span<pending> members,
      std::deque<codec::cooperative_work>& accounts,
      std::span<request_sample> samples,
      std::optional<seastar::semaphore_units<>>& hold,
      measurement& observed) {
        const auto count = members.size();
        std::vector<std::optional<local_append_outcome>> outcomes(count);
        const auto offer = [&](std::size_t i) {
            auto& member = members[i];
            auto request = std::move(*member.request);
            member.request.reset();
            auto stages = s.append->append(
              s.targets[member.segment], std::move(request), accounts[i]);
            observed.peak_admission = std::max(
              observed.peak_admission, s.budget.snapshot().bytes);
            return stages;
        };
        // A rejection names its typed pressure or failure. A held burst is
        // sized so that none occurs; arrivals that do not wait for their
        // result are held back at the owner's own bound, below.
        const auto admitted = [](const local_append_acceptance& acceptance) {
            take(acceptance.failure.outcome());
            require(
              acceptance.accepted(),
              "a request's segment group could not hold it");
        };
        // Joins both stages; a rejection still joins the result.
        const auto observe =
          [&](std::size_t i, local_append_stages stages) -> seastar::future<> {
            const auto acceptance = co_await std::move(stages.accepted);
            if (!samples.empty()) samples[i].accepted = measurement_time();
            outcomes[i].emplace(co_await std::move(stages.result));
            if (!samples.empty()) samples[i].terminal = measurement_time();
            admitted(acceptance);
        };
        switch (selected.profile) {
        case arrival::held: {
            std::vector<local_append_stages> stages;
            stages.reserve(count);
            for (std::size_t i = 0; i < count; ++i)
                stages.push_back(offer(i));
            std::vector<local_append_acceptance> acceptances;
            acceptances.reserve(count);
            for (auto& stage : stages)
                acceptances.push_back(co_await std::move(stage.accepted));
            hold.reset();
            for (std::size_t i = 0; i < count; ++i)
                outcomes[i].emplace(co_await std::move(stages[i].result));
            for (const auto& acceptance : acceptances)
                admitted(acceptance);
            break;
        }
        case arrival::serial:
            for (std::size_t i = 0; i < count; ++i)
                co_await observe(i, offer(i));
            break;
        case arrival::isolated:
        case arrival::paced:
        case arrival::burst: {
            const auto epoch = measurement_time();
            const auto arrival_start = std::chrono::steady_clock::now();
            std::vector<std::optional<seastar::future<>>> pending_results(
              count);
            // Requests offered whose result is not in. The owner refuses a
            // request while as many as one WAL group holds wait to form,
            // which a device slower than the arrivals brings about. A
            // producer waits then, and so does this one: an arrival is held
            // back until a result is in, and what it waited shows between
            // the time it was planned for and the time it started. No
            // request is dropped for a stall, and none is refused.
            std::size_t outstanding = 0;
            seastar::condition_variable drained;
            runtime::first_failure failed;
            try {
                for (std::size_t i = 0; i < count; ++i) {
                    const auto planned = i * selected.spacing_ns;
                    samples[i].planned = epoch + planned;
                    const auto due = arrival_start
                                     + std::chrono::nanoseconds{
                                       static_cast<std::int64_t>(planned)};
                    if (
                      const auto now = std::chrono::steady_clock::now();
                      due > now)
                        co_await seastar::sleep<std::chrono::steady_clock>(
                          due - now);
                    while (outstanding >= maximum_wal_group_members)
                        co_await drained.wait();
                    samples[i].started = measurement_time();
                    if (selected.profile == arrival::isolated) {
                        co_await observe(i, offer(i));
                        continue;
                    }
                    ++outstanding;
                    pending_results[i].emplace(
                      observe(i, offer(i)).finally([&outstanding, &drained] {
                          --outstanding;
                          drained.signal();
                      }));
                }
            } catch (...) {
                failed.observe(std::current_exception());
            }
            for (auto& waiting : pending_results)
                if (waiting) {
                    try {
                        co_await std::move(*waiting);
                    } catch (...) {
                        failed.observe(std::current_exception());
                    }
                }
            take(failed.outcome());
            break;
        }
        }
        for (std::size_t i = 0; i < count; ++i) {
            const auto& outcome = *outcomes[i];
            take(outcome.failure.outcome());
            require(
              outcome.status == local_append_status::durable
                && outcome.receipt.has_value(),
              "an accepted request did not become locally durable");
            require(
              outcome.receipt->batch == members[i].alias->info(),
              "a durable result changed the request's original identity");
            members[i].block.emplace(outcome.receipt->block);
        }
    }
};

struct candidate_append : local_storage_bench {};
struct baseline_append : local_storage_bench {};
struct sequential_append : local_storage_bench {};
struct local_append_latency : local_storage_bench {};

#define LOCAL_CASE(Name, ...)                                                  \
    case_shape { .name = #Name __VA_OPT__(, ) __VA_ARGS__ }
// The equal-work pairs: the owner against the same calls composed, and the
// composed overlap against the sequential composition, on one fixed cut.
#define LOCAL_PAIR(Name, ...)                                                  \
    PERF_TEST_F(candidate_append, Name) {                                      \
        return run_case(LOCAL_CASE(Name, __VA_ARGS__), scope::candidate);      \
    }                                                                          \
    PERF_TEST_F(baseline_append, Name) {                                       \
        return run_case(LOCAL_CASE(Name, __VA_ARGS__), scope::baseline);       \
    }                                                                          \
    PERF_TEST_F(sequential_append, Name) {                                     \
        return run_case(LOCAL_CASE(Name, __VA_ARGS__), scope::sequential);     \
    }
LOCAL_PAIR(tiny_group, .requests = 32)
LOCAL_PAIR(tiny_serial, .requests = 32, .profile = arrival::serial)
LOCAL_PAIR(aligned_group, .payload_bytes = 128_KiB, .requests = 16)
LOCAL_PAIR(
  aligned_serial,
  .payload_bytes = 128_KiB,
  .requests = 8,
  .profile = arrival::serial)
// A 257-byte-fragmented batch uses about half of a segment group's fragment
// bound, so one group holds at most two.
LOCAL_PAIR(
  fragmented_group, .payload_bytes = 128_KiB, .fragmented = true, .requests = 2)
LOCAL_PAIR(
  fragmented_serial,
  .payload_bytes = 128_KiB,
  .fragmented = true,
  .requests = 4,
  .profile = arrival::serial)
LOCAL_PAIR(
  maximum_serial,
  .payload_bytes = 8_MiB,
  .requests = 1,
  .profile = arrival::serial)
LOCAL_PAIR(many_segments, .requests = 32, .segments = 8)
// Wider than the 16-flush cap, so every cap of the sweep binds. Its 24
// segment owners need more memory than the paired driver's fixed profile.
LOCAL_PAIR(wide_segments, .requests = 48, .segments = 24)
#undef LOCAL_PAIR

struct two_barrier_placement : local_storage_bench {};
struct segment_placement : local_storage_bench {};
struct wal_placement : local_storage_bench {};
struct wal_held_placement : local_storage_bench {};

// One barrier per group against today's two, on one fixed cut: the same
// segment bytes stored by today's composition, with no WAL record, or
// acknowledged on the WAL with the segment barriers left to the end, the
// segment writes either issued at once or held until every group is
// acknowledged. A held
// case is one group across every segment; a serial case is one request a
// group. A child of `payload_bytes` carries about 1 KiB less record value.
// The widest cases are as wide as the budget's share of the smoke test's
// 1 GiB allows: 24 segments, or 16 with 1 MiB children.
#define LOCAL_PLACEMENT(Name, ...)                                             \
    PERF_TEST_F(two_barrier_placement, Name) {                                 \
        return run_case(LOCAL_CASE(Name, __VA_ARGS__), scope::baseline);       \
    }                                                                          \
    PERF_TEST_F(segment_placement, Name) {                                     \
        return run_case(LOCAL_CASE(Name, __VA_ARGS__), scope::segment_placed); \
    }                                                                          \
    PERF_TEST_F(wal_placement, Name) {                                         \
        return run_case(LOCAL_CASE(Name, __VA_ARGS__), scope::wal_placed);     \
    }                                                                          \
    PERF_TEST_F(wal_held_placement, Name) {                                    \
        return run_case(LOCAL_CASE(Name, __VA_ARGS__), scope::wal_held);       \
    }
LOCAL_PLACEMENT(small_one, .payload_bytes = 2_KiB, .requests = 64)
LOCAL_PLACEMENT(
  small_eight, .payload_bytes = 2_KiB, .requests = 64, .segments = 8)
LOCAL_PLACEMENT(
  small_wide, .payload_bytes = 2_KiB, .requests = 48, .segments = 24)
LOCAL_PLACEMENT(medium_one, .payload_bytes = 16_KiB, .requests = 64)
LOCAL_PLACEMENT(
  medium_eight, .payload_bytes = 16_KiB, .requests = 64, .segments = 8)
LOCAL_PLACEMENT(
  medium_wide, .payload_bytes = 16_KiB, .requests = 48, .segments = 24)
LOCAL_PLACEMENT(large_one, .payload_bytes = 1_MiB, .requests = 8)
LOCAL_PLACEMENT(
  large_eight, .payload_bytes = 1_MiB, .requests = 8, .segments = 8)
LOCAL_PLACEMENT(
  large_wide, .payload_bytes = 1_MiB, .requests = 16, .segments = 16)
LOCAL_PLACEMENT(
  small_serial,
  .payload_bytes = 2_KiB,
  .requests = 32,
  .profile = arrival::serial)
LOCAL_PLACEMENT(
  medium_serial,
  .payload_bytes = 16_KiB,
  .requests = 32,
  .profile = arrival::serial)
LOCAL_PLACEMENT(
  large_serial,
  .payload_bytes = 1_MiB,
  .requests = 8,
  .profile = arrival::serial)
#undef LOCAL_PLACEMENT

// Arrival-to-result latency of the owner, without and with the WAL group
// commit's 1-ms batching window.
#define LOCAL_LATENCY(Name, ...)                                               \
    PERF_TEST_F(local_append_latency, zero_##Name) {                           \
        return run_case(LOCAL_CASE(Name, __VA_ARGS__), scope::candidate);      \
    }                                                                          \
    PERF_TEST_F(local_append_latency, delayed_##Name) {                        \
        return run_case(                                                       \
          LOCAL_CASE(Name, __VA_ARGS__, .wait_ns = 1'000'000),                 \
          scope::candidate);                                                   \
    }
LOCAL_LATENCY(
  isolated,
  .requests = 64,
  .profile = arrival::isolated,
  .spacing_ns = 2'000'000)
LOCAL_LATENCY(burst, .requests = 64, .profile = arrival::burst)
LOCAL_LATENCY(
  paced, .requests = 128, .profile = arrival::paced, .spacing_ns = 1'000'000)
LOCAL_LATENCY(
  aligned_burst,
  .payload_bytes = 128_KiB,
  .requests = 32,
  .profile = arrival::burst)
LOCAL_LATENCY(
  many_burst, .requests = 64, .segments = 8, .profile = arrival::burst)
LOCAL_LATENCY(
  many_paced,
  .requests = 128,
  .segments = 8,
  .profile = arrival::paced,
  .spacing_ns = 1'000'000)
#undef LOCAL_LATENCY
#undef LOCAL_CASE
} // namespace
} // namespace kwaque::storage::testing::local_storage_bench_support
