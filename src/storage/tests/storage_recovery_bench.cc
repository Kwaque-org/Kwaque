#include "src/base/units.h"
#include "src/bytes/test_allocation_profile.h"
#include "src/codec/tests/allocation_observer.h"
#include "src/codec/tests/memory_qualification_support.h"
#include "src/resource/resource_registry.h"
#include "src/runtime/first_failure.h"
#include "src/runtime/production/clocks.h"
#include "src/runtime/production/timer.h"
#include "src/runtime/testing/test_directory.h"
#include "src/storage/local_append.h"
#include "src/storage/local_paths.h"
#include "src/storage/recovery_inventory.h"
#include "src/storage/recovery_publication.h"
#include "src/storage/recovery_successor.h"
#include "src/storage/segment_format.h"
#include "src/storage/tests/local_installation_contract.h"
#include "src/storage/tests/local_storage_bench_file.h"
#include "src/storage/tests/local_store_contract.h"
#include "src/storage/tests/segment_bench_fixture.h"
#include "src/storage/tests/segment_bench_observer.h"
#include "src/storage/tests/segment_test_support.h"
#include "src/storage/tests/segment_writer_contract.h"
#include "src/storage/tests/wal_writer_contract.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread_cputime_clock.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/tmp_file.hh>

#include <algorithm>
#include <array>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <variant>
#include <vector>

namespace kwaque::storage::testing::storage_recovery_bench_support {
namespace {
using bytes::testing::charge;
using local_storage_bench_support::file_kind;
using local_storage_bench_support::file_system;
using local_storage_bench_support::kind_sample;
using local_storage_bench_support::measurement_time;
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
#if defined(KWAQUE_STORAGE_RECOVERY_TIMING_ONLY)
constexpr bool timing_only = true;
#else
constexpr bool timing_only = false;
#endif

// What a store holds when it restarts. Every store is written by the local
// append path, one request at a time, and then stopped.
enum class store_state : std::uint8_t {
    // Stopped after its last durable append.
    stopped,
    // Damaged as log repair tests damage a log: the last PREPARE cut four
    // bytes short of its end, 512 zero bytes in the middle of one segment's
    // second block, and another segment zeroed from inside its last block to
    // its end.
    damaged,
    // Every segment lost the data after its header while the WAL kept every
    // PREPARE, so each one is a candidate.
    lost_data,
    // Recovered once and stopped again without appends: each segment resumes
    // at its recovering pin. No checkpoint exists yet, so the whole WAL chain
    // is still scanned.
    restarted,
};
const char* state_name(store_state value) noexcept {
    switch (value) {
    case store_state::stopped:
        return "stopped";
    case store_state::damaged:
        return "damaged";
    case store_state::lost_data:
        return "lost_data";
    case store_state::restarted:
        return "restarted";
    }
    std::abort();
}
const char* action_name(recovery_plan_action value) noexcept {
    switch (value) {
    case recovery_plan_action::stop:
        return "stop";
    case recovery_plan_action::retain:
        return "retain";
    case recovery_plan_action::publish_recovering:
        return "publish_recovering";
    case recovery_plan_action::activate_successor:
        return "activate_successor";
    }
    std::abort();
}
const char* verdict_name(recovery_store_verdict value) noexcept {
    switch (value) {
    case recovery_store_verdict::ready:
        return "ready";
    case recovery_store_verdict::unavailable:
        return "unavailable";
    case recovery_store_verdict::incomplete:
        return "incomplete";
    case recovery_store_verdict::lost:
        return "lost";
    case recovery_store_verdict::unsupported:
        return "unsupported";
    case recovery_store_verdict::corrupt:
        return "corrupt";
    }
    std::abort();
}
constexpr std::array<const char*, 9> classification_names{
  "satisfied",
  "footer_only",
  "candidate",
  "suffix",
  "conflict",
  "content",
  "uncertified_tail",
  "corruption",
  "slack"};

// Request i targets segment i % segments.
struct case_shape final {
    const char* name;
    std::size_t payload_bytes{0};
    std::uint32_t requests{0}, segments{0};
    store_state state{store_state::stopped};
    [[nodiscard]] std::uint32_t per_segment() const noexcept {
        return segments == 0 ? 0 : requests / segments;
    }
    [[nodiscard]] bool large() const noexcept { return payload_bytes >= 4_MiB; }
};

struct native_driver final {
    template<typename T>
    seastar::future<T> lifecycle(seastar::future<T> value) const {
        return value;
    }
};
model::range_routing_epoch routing() {
    return model::range_routing_epoch::make(1).value();
}
wal_writer_config wal_configuration(const case_shape& selected) {
    auto wal = wal_writer_contract::configuration();
    wal.capacity_bytes = byte_count{128_MiB};
    if (selected.large()) wal.children.working_bytes = byte_count{16_MiB};
    return wal;
}
// Each recovering publication opens its segment as the inventory found it.
segment_writer_config publication_configuration() {
    auto config = segment_writer_contract::configuration();
    config.admission.working_bytes = byte_count{1_MiB};
    return config;
}
// The largest aligned read window this allocator profile serves within one
// contiguous allocation. Under the native allocator it is the product's
// default window; the system allocator's per-allocation slack makes that one
// window too large for the ceiling.
byte_count admissible_window() {
    auto window = maximum_contiguous_allocation_bytes;
    while (charge(byte_count{window}).value()
           > maximum_contiguous_allocation_bytes)
        window -= 4_KiB;
    return byte_count{window};
}
struct native_settings final {
    bool memory{false};
    // Both scans' windows and read-ahead, swept by the restart cost model.
    scan_reader_limits reader{};
};
// The product's scan bounds; only the metadata reads take the benchmark's
// allocation charge.
recovery_merge_limits merge_limits(const native_settings& settings) {
    recovery_merge_limits limits;
    limits.wal.metadata = store_contract::limits();
    limits.segment.metadata = store_contract::limits();
    limits.wal.reader = settings.reader;
    limits.segment.reader = settings.reader;
    return limits;
}

// One durable request: its PREPARE and the block it names.
struct appended final {
    std::optional<local_wal_cursor> begin, end;
    // Its block's envelope in the data file: where the body begins and where
    // the content ends. The envelope's counted padding after it is zeros.
    std::uint64_t block_body{0}, block_end{0};
    std::optional<model::range_logical_end> logical_end;
    std::uint64_t child_bytes{0};
};
// What the local append path left on the device, and what an owner supplies
// to recovery independently of it: the segment catalog.
struct written_store final {
    std::vector<local_segment_descriptor> descriptors;
    std::vector<recovery_catalog_entry> catalog;
    std::vector<runtime::file_position> data_starts;
    std::vector<appended> requests;
    std::optional<local_wal_head> head;
    std::optional<local_wal_cursor> head_start;
    // WAL files in the chain, oldest first.
    std::vector<model::wal_incarnation_id> wal_files;
};

template<typename Close>
seastar::future<> observe_close(runtime::first_failure& failed, Close close) {
    try {
        failed.observe(co_await close());
    } catch (...) {
        failed.observe(std::current_exception());
    }
}

// Writes the store through the local append path: the WAL writer with its
// group commit, one segment writer per segment and the local append owner.
// Each request completes before the next starts, so each forms its own
// group and footer, and the WAL interleaves the segments.
seastar::future<written_store> write_store(
  file_system& files,
  owner_type& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  runtime::production::timer& timer,
  const case_shape& selected) {
    written_store store;
    fixture::shape shape{.name = selected.name};
    shape.payload_bytes = selected.payload_bytes;
    shape.window = 1;
    shape.groups = std::max<std::uint32_t>(1, selected.per_segment());
    shape.blocks = 1;
    seastar::abort_source abort;
    codec::cooperative_work work{fixture::policy(shape), abort};
    std::vector<fixture::extent_input> inputs;
    inputs.reserve(selected.segments);
    for (std::uint32_t f = 0; f < selected.segments; ++f) {
        inputs.push_back(co_await fixture::make_extent(shape, f, work));
        const auto& descriptor = inputs.back().descriptor;
        store.descriptors.push_back(descriptor);
        store.catalog.push_back(
          {descriptor,
           take(
             segment_header::make(
               descriptor.segment,
               descriptor.logical_origin,
               descriptor.alignment))});
        store.data_starts.push_back(inputs.back().history.data_start);
    }
    store.requests.resize(selected.requests);

    std::unique_ptr<control_type> control;
    std::unique_ptr<allocator_type> ids;
    std::unique_ptr<wal_type> writer;
    std::unique_ptr<wal_group_commit> commit;
    std::vector<std::unique_ptr<segment_type>> segments;
    std::unique_ptr<append_type> append;
    runtime::first_failure failed;
    try {
        control = take(
          co_await control_type::open(
            files,
            owner,
            spec,
            0,
            false,
            budget,
            store_contract::limits(),
            work));
        ids = take(allocator_type::make(*control, budget, 4));
        writer = take(
          wal_type::make(
            *control,
            *ids,
            budget,
            wal_configuration(selected),
            wal_start_intent::known_unactivated));
        take(
          co_await seastar::with_scheduling_group(
            budget.scheduling_group(),
            [&writer, &work] { return writer->bootstrap(work); }));
        store.head = writer->prepared_head();
        store.head_start = writer->progress()->reserved;
        store.wal_files.push_back(store.head->incarnation);
        wal_group_commit_config cohort;
        cohort.maximum_wait = runtime::monotonic_duration{0};
        commit = take(wal_group_commit::make(*writer, budget, cohort));
        take(commit->template start<clock>(timer));
        segments.reserve(selected.segments);
        for (std::uint32_t f = 0; f < selected.segments; ++f) {
            auto config = segment_writer_contract::configuration();
            config.retry_object = local_object_sequence::make(45 + f).value();
            config.policy = work.policy();
            config.admission.working_bytes = selected.large()
                                               ? fixture::working_bytes
                                               : byte_count{1_MiB};
            segments.push_back(take(
              segment_type::make_new(
                files, owner, spec, 0, inputs[f].descriptor, budget, config)));
            auto& created = *segments.back();
            take(
              co_await seastar::with_scheduling_group(
                budget.scheduling_group(),
                [&created, &work] { return created.create_new(work); }));
        }
        if (selected.requests != 0) {
            local_append_config config;
            config.maximum_requests = 128;
            config.policy = work.policy();
            append = take(append_type::make(budget, *commit, *writer, config));
            std::vector<local_append_target> targets;
            targets.reserve(selected.segments);
            for (auto& segment : segments)
                targets.push_back(take(append->attach(*segment)));
            // Each request holds its child's backing only while it is
            // appended, as a serial producer's would.
            for (std::uint32_t i = 0; i < selected.requests; ++i) {
                const auto f = i % selected.segments;
                auto& child
                  = inputs[f].groups[i / selected.segments].children[0];
                auto& record = store.requests[i];
                record.child_bytes = child.bytes().size().value();
                auto backing = take(budget.try_reserve_buffer(child.bytes()));
                const auto history = inputs[f].history;
                record.begin = writer->progress()->reserved;
                auto stages = append->append(
                  targets[f],
                  local_append_request{
                    std::move(child),
                    std::move(backing),
                    {history.segment.topic(), history.segment.range()},
                    routing(),
                    std::nullopt},
                  work);
                const auto acceptance = co_await std::move(stages.accepted);
                const auto outcome = co_await std::move(stages.result);
                take(acceptance.failure.outcome());
                require(
                  acceptance.accepted(),
                  "a store request's segment group could not hold it");
                take(outcome.failure.outcome());
                require(
                  outcome.status == local_append_status::durable
                    && outcome.receipt.has_value(),
                  "a store request did not become locally durable");
                const auto& receipt = *outcome.receipt;
                record.end = receipt.wal.boundary().cursor();
                const auto at = receipt.block.records.bytes().begin().value();
                const auto& envelope = receipt.block.envelope;
                record.block_body = at + envelope.header_bytes().value();
                record.block_end = at + envelope.encoded_bytes().value()
                                   - envelope.padding_bytes().value();
                record.logical_end = receipt.block.records.logical().end();
            }
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    // Every owner closes, in reverse, whatever failed before.
    if (append)
        co_await observe_close(failed, [&append] { return append->close(); });
    append.reset();
    for (auto& segment : segments)
        if (segment)
            co_await observe_close(
              failed, [&segment] { return segment->close(); });
    segments.clear();
    if (commit)
        co_await observe_close(failed, [&commit] { return commit->close(); });
    commit.reset();
    if (writer)
        co_await observe_close(failed, [&writer] { return writer->close(); });
    writer.reset();
    if (ids) co_await observe_close(failed, [&ids] { return ids->close(); });
    ids.reset();
    if (control)
        co_await observe_close(failed, [&control] { return control->close(); });
    control.reset();
    take(failed.outcome());
    for (const auto& record : store.requests)
        require(
          record.begin && record.end
            && record.begin->incarnation() == store.head->incarnation
            && record.end->incarnation() == store.head->incarnation,
          "a store's requests did not stay in one WAL file");
    co_return store;
}

runtime::file_path
data_path(const local_device_spec& spec, const local_segment_descriptor& d) {
    return take(take(local_paths::make(spec.root))
                  .segment_file(
                    0,
                    {d.segment.segment(), d.segment.generation()},
                    local_segment_file::data));
}
runtime::file_path
wal_path(const local_device_spec& spec, model::wal_incarnation_id incarnation) {
    return take(take(local_paths::make(spec.root)).wal(0, incarnation));
}

// Applies a store's damage after it stopped, as a crash and the device left
// it. Nothing here is measured.
seastar::future<> damage_store(
  file_system& files,
  const local_device_spec& spec,
  const written_store& store,
  const case_shape& selected) {
    const native_driver drive;
    if (selected.state == store_state::lost_data) {
        for (std::size_t f = 0; f < store.descriptors.size(); ++f) {
            const auto path = data_path(spec, store.descriptors[f]);
            auto bytes = co_await store_contract::read_all_bytes(
              files, path, drive);
            bytes.resize(store.data_starts[f].value());
            co_await store_contract::write_bytes(
              files, path, std::move(bytes), drive);
        }
        co_return;
    }
    if (selected.state != store_state::damaged) co_return;
    const auto per_segment = selected.per_segment();
    // The last PREPARE loses the last four bytes its envelope declares.
    {
        const auto& last = store.requests.back();
        const auto path = wal_path(spec, last.begin->incarnation());
        auto bytes = co_await store_contract::read_all_bytes(
          files, path, drive);
        const auto at = last.begin->position().value();
        require(
          at + 16 <= bytes.size(), "the last PREPARE is not on the device");
        const auto end = at + get(bytes, at + 10, 2) + get(bytes, at + 12, 4);
        require(end <= bytes.size(), "the last PREPARE is not on the device");
        bytes.resize(end - 4);
        co_await store_contract::write_bytes(
          files, path, std::move(bytes), drive);
    }
    // 512 zero bytes in the middle of segment 0's second block. A record is
    // its content, not the zero padding that aligns its envelope.
    {
        const auto& torn = store.requests[selected.segments];
        const auto path = data_path(spec, store.descriptors[0]);
        auto bytes = co_await store_contract::read_all_bytes(
          files, path, drive);
        const auto middle = (torn.block_body + torn.block_end) / 2;
        const auto from = std::max(torn.block_body, middle - 256);
        const auto to = std::min(torn.block_end, middle + 256);
        require(
          from < to && to <= bytes.size(),
          "the torn block is not on the device");
        std::fill(
          bytes.begin() + static_cast<std::ptrdiff_t>(from),
          bytes.begin() + static_cast<std::ptrdiff_t>(to),
          '\0');
        co_await store_contract::write_bytes(
          files, path, std::move(bytes), drive);
    }
    // Segment 1 zeroed from the middle of its last block's content to its
    // end.
    {
        const auto& torn
          = store.requests[(per_segment - 1) * selected.segments + 1];
        const auto path = data_path(spec, store.descriptors[1]);
        auto bytes = co_await store_contract::read_all_bytes(
          files, path, drive);
        const auto from = (torn.block_body + torn.block_end) / 2;
        require(
          from < torn.block_end && torn.block_end <= bytes.size(),
          "the last block is not on the device");
        std::fill(
          bytes.begin() + static_cast<std::ptrdiff_t>(from), bytes.end(), '\0');
        co_await store_contract::write_bytes(
          files, path, std::move(bytes), drive);
    }
}

// What the restart must classify, derived from the requests' receipts and
// the damage applied, never from the scan.
struct expected_segment final {
    recovery_plan_action action{recovery_plan_action::publish_recovering};
    // The logical end of the recovered boundary; absent without a boundary.
    std::optional<model::range_logical_end> boundary_end;
    std::uint32_t satisfied{0}, footer_only{0}, candidates{0};
};
struct expectation final {
    std::vector<expected_segment> segments;
    std::optional<local_wal_cursor> predecessor;
    std::uint64_t prepares{0};
    // Child bytes of the PREPAREs in the WAL's classified content: a lower
    // bound for the bytes whose checksums the scan verifies.
    std::uint64_t wal_child_bytes{0};
};
expectation expect(const written_store& store, const case_shape& selected) {
    expectation result;
    const auto n = selected.per_segment();
    const auto request = [&](std::uint32_t segment, std::uint32_t k) {
        return store.requests[k * selected.segments + segment];
    };
    for (std::uint32_t f = 0; f < selected.segments; ++f) {
        expected_segment segment;
        segment.boundary_end = request(f, n - 1).logical_end;
        segment.satisfied = n;
        switch (selected.state) {
        case store_state::stopped:
            break;
        case store_state::restarted:
            segment.action = recovery_plan_action::retain;
            break;
        case store_state::lost_data:
            segment.boundary_end.reset();
            segment.satisfied = 0;
            segment.candidates = n;
            break;
        case store_state::damaged:
            if (f == 0) {
                segment.boundary_end = request(0, 0).logical_end;
                segment.satisfied = 1;
                segment.candidates = n - 1;
            } else if (f == 1) {
                segment.boundary_end = request(1, n - 2).logical_end;
                segment.satisfied = n - 1;
                segment.candidates = 1;
            } else if (f == selected.segments - 1) {
                segment.satisfied = n - 1;
                segment.footer_only = 1;
            }
            break;
        }
        result.segments.push_back(segment);
    }
    const bool cut = selected.state == store_state::damaged;
    result.prepares = store.requests.size() - (cut ? 1 : 0);
    for (std::size_t i = 0; i < result.prepares; ++i)
        result.wal_child_bytes += store.requests[i].child_bytes;
    if (cut)
        result.predecessor = store.requests.back().begin;
    else if (selected.state == store_state::restarted)
        result.predecessor = store.head_start;
    else if (!store.requests.empty())
        result.predecessor = store.requests.back().end;
    else
        result.predecessor = store.head_start;
    return result;
}

// Native work at one instant of the restart.
struct mark final {
    std::uint64_t at{0}, reads{0}, read_bytes{0}, writes{0}, write_bytes{0},
      flushes{0}, opens{0}, directory_syncs{0}, crc_bulk_bytes{0},
      digest_bytes{0};
};
// The restart's steps, each ending at its mark.
constexpr std::array<const char*, 6> step_names{
  "inspect", "control", "decisions", "inventory", "merge", "durable"};
using marks = std::array<mark, step_names.size() + 1>;

struct measurement final {
    std::uint64_t epoch{0}, elapsed{0}, cpu{0}, allocations{0}, tasks{0},
      runtime_operations{0};
    marks steps{};
    codec::testing::allocation_observation memory;
    fixture::work_sample work;
};

mark capture(const file_system& files, const fixture::work_sample& work) {
    mark result;
    result.at = measurement_time();
    for (const auto& kind : files.sample.kinds) {
        result.reads += kind.reads;
        result.read_bytes += kind.read_bytes;
        result.writes += kind.calls;
        result.write_bytes += kind.bytes;
        result.flushes += kind.flushes;
        result.opens += kind.opens;
    }
    result.directory_syncs = files.sample.directory_syncs;
    result.crc_bulk_bytes = work.crc_bulk_bytes;
    result.digest_bytes = work.digest_bytes;
    return result;
}

// The store's restart up to ready, and nothing after it. Native work and
// allocations count only inside.
class measure_scope final {
public:
    measure_scope(file_system& files, measurement& output, bool allocations)
      : files_(files)
      , output_(output)
      , observe_allocations_(allocations)
      , before_allocations_(seastar::memory::stats().mallocs())
      , before_tasks_(seastar::engine().get_sched_stats().tasks_processed)
      , before_operations_(files.statistics().accepted) {
        files_.sample.measuring = true;
#if !defined(KWAQUE_STORAGE_RECOVERY_TIMING_ONLY)
        if (observe_allocations_)
            codec::testing::begin_allocation_observation();
        fixture::begin_work_observation(output_.work);
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
#if !defined(KWAQUE_STORAGE_RECOVERY_TIMING_ONLY)
        fixture::end_work_observation();
        if (observe_allocations_)
            output_.memory = codec::testing::end_allocation_observation();
#endif
        output_.allocations = seastar::memory::stats().mallocs()
                              - before_allocations_;
        output_.tasks = seastar::engine().get_sched_stats().tasks_processed
                        - before_tasks_;
        output_.runtime_operations = files_.statistics().accepted
                                     - before_operations_;
    }
    measure_scope(const measure_scope&) = delete;
    measure_scope& operator=(const measure_scope&) = delete;

private:
    file_system& files_;
    measurement& output_;
    [[maybe_unused]] bool observe_allocations_;
    std::uint64_t before_allocations_, before_tasks_, before_operations_;
    seastar::thread_cputime_clock::time_point cpu_;
};

// What one restart established, kept for verification after the interval.
struct restarted final {
    recovery_store_verdict verdict{recovery_store_verdict::ready};
    std::optional<recovery_planner> planner;
    recovery_merge_result merged;
    std::vector<recovery_segment_report> reports;
    std::array<std::uint64_t, classification_names.size()> slots{}, regions{};
    std::uint64_t unresolved{0}, obligations{0};
    std::uint64_t admission_peak{0}, handle_peak{0};
    std::uint32_t published{0};
    bool publications_complete{false};
    std::optional<local_wal_head> successor;
    std::optional<local_wal_cursor> successor_start;
};
// The owners a ready shard keeps for its new appends.
struct ready_owners final {
    std::unique_ptr<control_type> control;
    std::unique_ptr<allocator_type> ids;
    std::unique_ptr<wal_type> writer;
};
seastar::future<> close_owners(ready_owners& owners) {
    runtime::first_failure failed;
    if (owners.writer)
        co_await observe_close(
          failed, [&owners] { return owners.writer->close(); });
    owners.writer.reset();
    if (owners.ids)
        co_await observe_close(
          failed, [&owners] { return owners.ids->close(); });
    owners.ids.reset();
    if (owners.control)
        co_await observe_close(
          failed, [&owners] { return owners.control->close(); });
    owners.control.reset();
    take(failed.outcome());
}

std::size_t classification_index(const recovery_case* matched) {
    require(matched != nullptr, "a merged item matched no case");
    return static_cast<std::size_t>(matched->classification);
}
// Feeds every merged item to the planner and counts what it classifies,
// sampling the budget's admitted bytes and handles at each one.
struct restart_view final {
    recovery_planner* planner;
    restarted* out;
    const workload_budget* budget;
    seastar::future<runtime::result<bool>>
    operator()(const recovery_item& item) const {
        if (auto observed = planner->observe(item); !observed)
            return seastar::make_ready_future<runtime::result<bool>>(
              runtime::failure(observed.error()));
        const auto now = budget->snapshot();
        out->admission_peak = std::max(out->admission_peak, now.bytes);
        out->handle_peak = std::max(
          out->handle_peak, std::uint64_t{now.handles});
        if (const auto* slot = std::get_if<recovery_slot_report>(&item))
            ++out->slots[classification_index(slot->matched)];
        else if (
          const auto* region = std::get_if<recovery_region_report>(&item))
            ++out->regions[classification_index(region->matched)];
        else if (
          const auto* report = std::get_if<recovery_segment_report>(&item))
            out->reports.push_back(*report);
        else if (std::holds_alternative<recovery_unresolved_report>(item))
            ++out->unresolved;
        else
            ++out->obligations;
        return seastar::make_ready_future<runtime::result<bool>>(true);
    }
};

// The restart sequence, read-only until the plan is complete: the device's
// classification, the control and its head, durable decisions, the
// catalog's pinned evidence, one merge of the WAL chain with every target,
// the plan; then, at once, one fresh flush and a recovering publication per
// recovered segment, and the head closed by a successor. The shard is ready
// when this returns; the owners it keeps for new appends are not closed
// here.
seastar::future<> run_restart(
  file_system& files,
  owner_type& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  const written_store& store,
  const case_shape& selected,
  const native_settings& settings,
  restarted& out,
  ready_owners& owners,
  measurement* observed) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array specs{spec};
    std::size_t next = 0;
    const auto mark_step = [&] {
        if (observed) observed->steps[next] = capture(files, observed->work);
        ++next;
    };
    mark_step();
    auto report = take(
      co_await inspect_local_recovery(
        files,
        owner,
        std::span<const local_device_spec>{specs},
        budget,
        store_contract::limits(),
        work));
    out.verdict = report.verdict;
    require(
      report.verdict == recovery_store_verdict::ready,
      "a stopped store was not ready to recover");
    mark_step();
    owners.control = take(
      co_await control_type::open(
        files, owner, spec, 0, true, budget, store_contract::limits(), work));
    const auto fields = take(owners.control->snapshot()).fields;
    mark_step();
    std::vector<recovery_decision_record> decided;
    auto discovered = take(
      co_await discover_recovery_decisions(
        files,
        owner,
        spec,
        0,
        fields,
        store.descriptors,
        budget,
        {store_contract::limits(), 16},
        work,
        [&decided](const recovery_decision_record& value) {
            decided.push_back(value);
            return seastar::make_ready_future<runtime::result<bool>>(true);
        }));
    require(
      discovered.complete && discovered.decisions == 0,
      "a store without decisions found one");
    mark_step();
    auto found = take(
      co_await open_recovery_inventory(
        files,
        owner,
        std::span<const local_device_spec>{specs},
        0,
        store.catalog,
        decided,
        budget,
        store_contract::limits(),
        work));
    mark_step();
    out.planner.emplace(take(recovery_planner::make(found.targets, budget)));
    out.merged = take(
      co_await reconcile_local_recovery(
        files,
        owner,
        std::span<const local_device_spec>{specs},
        spec,
        0,
        fields,
        std::nullopt,
        found.targets,
        budget,
        merge_limits(settings),
        work,
        restart_view{&*out.planner, &out, &budget}));
    take(out.planner->finish(out.merged));
    require(out.planner->ready(), "a store's restart plan stopped");
    mark_step();
    owners.ids = take(allocator_type::make(*owners.control, budget, 4));
    owners.writer = take(
      wal_type::make(
        *owners.control,
        *owners.ids,
        budget,
        wal_configuration(selected),
        wal_start_intent::recovered_head));
    auto published = take(
      co_await establish_recovered_state<clock>(
        files,
        owner,
        std::span<const local_device_spec>{specs},
        0,
        *out.planner,
        found,
        std::span<const recovery_visibility>{},
        budget,
        publication_configuration(),
        {},
        *owners.writer,
        codec::limits::defaults(),
        abort));
    out.publications_complete = published.complete;
    for (const auto& segment : published.segments)
        if (segment.published) ++out.published;
    out.successor = owners.writer->prepared_head();
    out.successor_start = owners.writer->progress()->reserved;
    mark_step();
}

// The restart did all of its work: its classification is exactly the one the
// store's construction determines, and, where observed, the merge's own
// checksummed bytes reach every PREPARE child in the WAL's content. That
// byte count is a floor, not proof that each record was checked: per-record
// verification shows in the damaged store's classification.
void verify(
  const written_store& store,
  const case_shape& selected,
  const restarted& out,
  const file_system& files,
  const measurement& value) {
    const auto expected = expect(store, selected);
    const auto& plan = *out.planner;
    const auto& wal = plan.wal();
    require(
      wal.action == recovery_plan_action::activate_successor && wal.predecessor
        && expected.predecessor
        && wal.predecessor->incarnation() == expected.predecessor->incarnation()
        && wal.predecessor->position() == expected.predecessor->position()
        && wal.unresolved == 0 && wal.nonzero_slack == 0,
      "the restart closed the head at another content end");
    require(
      out.merged.wal.complete && out.merged.wal.prepares == expected.prepares
        && out.merged.wal.verdict == wal_scan_verdict::intact,
      "the restart scanned another WAL content");
    require(
      plan.segments().size() == expected.segments.size()
        && out.reports.size() == expected.segments.size(),
      "the restart planned another set of segments");
    std::uint32_t publishing = 0;
    for (std::size_t f = 0; f < expected.segments.size(); ++f) {
        const auto& want = expected.segments[f];
        const auto& segment = store.descriptors[f].segment;
        const auto& planned = plan.segments()[f];
        const auto report = std::find_if(
          out.reports.begin(), out.reports.end(), [&](const auto& r) {
              return r.segment == segment;
          });
        require(
          planned.segment == segment && report != out.reports.end(),
          "a segment's plan or report is missing");
        require(
          planned.action == want.action && planned.candidates == want.candidates
            && planned.suffix == 0 && report->satisfied == want.satisfied
            && report->footer_only == want.footer_only && report->conflicts == 0
            && report->unresolved == 0,
          "a segment was classified otherwise than its store determines");
        if (want.boundary_end)
            require(
              planned.boundary
                && planned.recovered_end.value() == want.boundary_end->value(),
              "a segment recovered another boundary");
        else
            require(
              !planned.boundary
                && planned.recovered_end.value()
                     == store.descriptors[f].logical_origin.value(),
              "a segment without a footer recovered a boundary");
        if (planned.action == recovery_plan_action::publish_recovering)
            ++publishing;
    }
    require(
      out.publications_complete && out.published == publishing,
      "a recovered segment was not flushed and published as recovering");
    require(
      out.successor && store.head
        && out.successor->incarnation != store.wal_files.back(),
      "the restart did not close its head with a successor");
    if constexpr (!timing_only) {
        const auto wal_reads = files.sample[file_kind::wal].reads;
        // The merge step alone: metadata read before it is checksummed too.
        const auto merged = value.steps[5].crc_bulk_bytes
                            - value.steps[4].crc_bulk_bytes;
        require(
          expected.prepares == 0 || merged != 0,
          "bulk CRC observation did not intercept the WAL scan");
        // Only a fragment's last 64 bytes or fewer can take the inline
        // checksum path that bulk observation does not see.
        require(
          merged + 64 * (2 * expected.prepares + wal_reads + 1)
            >= expected.wal_child_bytes,
          "the merge did not checksum every PREPARE's child");
    }
}

// A store file's size; a file that cannot be sized fails the run rather than
// reading as empty.
std::uint64_t file_bytes(const runtime::file_path& path) {
    return std::filesystem::file_size(path.value());
}

// Only the observing build reports native work.
[[maybe_unused]] void print_kind(const char* name, const kind_sample& sample) {
    std::printf(
      ",\"%s\":{\"reads\":%" PRIu64 ",\"read_bytes\":%" PRIu64
      ",\"read_minimum\":%" PRIu64 ",\"read_maximum\":%" PRIu64
      ",\"opens\":%" PRIu64 ",\"writes\":%" PRIu64 ",\"write_bytes\":%" PRIu64
      ",\"flushes\":%" PRIu64 "}",
      name,
      sample.reads,
      sample.read_bytes,
      sample.reads == 0 ? 0 : sample.read_minimum,
      sample.read_maximum,
      sample.opens,
      sample.calls,
      sample.bytes,
      sample.flushes);
}

void print_measurement(
  const case_shape& selected,
  const native_settings& settings,
  const measurement& value,
  const file_system& files,
  const written_store& store,
  const restarted& out,
  std::uint64_t wal_file_bytes,
  std::uint64_t data_file_bytes,
  const std::set<unsigned>& affinity) {
    std::printf(
      "storage_recovery_bench_v1 {\"case\":\"%s\",\"state\":\"%s\","
      "\"fdatasync\":\"%s\"",
      selected.name,
      state_name(selected.state),
      seastar::engine().have_aio_fdatasync() ? "aio" : "thread");
    auto number = [](const char* name, std::uint64_t n) {
        std::printf(",\"%s\":%" PRIu64, name, n);
    };
    auto flag = [](const char* name, bool set) {
        std::printf(",\"%s\":%s", name, set ? "true" : "false");
    };
    std::uint64_t child_bytes = 0;
    for (const auto& record : store.requests)
        child_bytes += record.child_bytes;
    number("window_bytes", settings.reader.window_bytes.value());
    number("read_ahead", settings.reader.read_ahead);
    number("payload_bytes", selected.payload_bytes);
    number("requests", selected.requests);
    number("segments", selected.segments);
    number("child_bytes", child_bytes);
    number("wal_files", store.wal_files.size());
    number("wal_file_bytes", wal_file_bytes);
    number("data_file_bytes", data_file_bytes);
    number("elapsed_ns", value.elapsed);
    number("reactor_cpu_ns", value.cpu);
    number("allocations", value.allocations);
    number("tasks", value.tasks);
    number("runtime_operations", value.runtime_operations);
    number("sampled_admission_peak", out.admission_peak);
    number("sampled_handle_peak", out.handle_peak);
    flag("timing_only", timing_only);
#ifdef NDEBUG
    flag("ndebug", true);
#else
    flag("ndebug", false);
#endif
    flag(
      "libcxx_hardening_none",
      _LIBCPP_HARDENING_MODE == _LIBCPP_HARDENING_MODE_NONE);
    flag("memory_observed", settings.memory);
    flag("complete", true);
    if (settings.memory) {
        number("memory_peak", value.memory.peak_upper_bound);
        number("largest_allocation", value.memory.largest_allocation);
        flag("memory_complete", value.memory.complete);
    }
    std::printf(",\"steps\":{");
    for (std::size_t p = 0; p < step_names.size(); ++p) {
        const auto& from = value.steps[p];
        const auto& to = value.steps[p + 1];
        std::printf(
          "%s\"%s\":{\"elapsed_ns\":%" PRIu64,
          p ? "," : "",
          step_names[p],
          to.at - from.at);
        if constexpr (!timing_only)
            std::printf(
              ",\"reads\":%" PRIu64 ",\"read_bytes\":%" PRIu64
              ",\"writes\":%" PRIu64 ",\"write_bytes\":%" PRIu64
              ",\"flushes\":%" PRIu64 ",\"opens\":%" PRIu64
              ",\"directory_syncs\":%" PRIu64 ",\"crc_bulk_bytes\":%" PRIu64
              ",\"digest_bytes\":%" PRIu64,
              to.reads - from.reads,
              to.read_bytes - from.read_bytes,
              to.writes - from.writes,
              to.write_bytes - from.write_bytes,
              to.flushes - from.flushes,
              to.opens - from.opens,
              to.directory_syncs - from.directory_syncs,
              to.crc_bulk_bytes - from.crc_bulk_bytes,
              to.digest_bytes - from.digest_bytes);
        std::printf("}");
    }
    std::printf("}");
    if constexpr (!timing_only) {
        print_kind("wal", files.sample[file_kind::wal]);
        print_kind("data", files.sample[file_kind::data]);
        print_kind("other", files.sample[file_kind::other]);
        number("directory_opens", files.sample.directory_opens);
        number("directory_syncs", files.sample.directory_syncs);
        number("listings", files.sample.listings);
        number("stats", files.sample.stats);
        number("crc_bulk_calls", value.work.crc_bulk_calls);
        number("crc_bulk_bytes", value.work.crc_bulk_bytes);
        number("digest_calls", value.work.digest_calls);
        number("digest_bytes", value.work.digest_bytes);
    }
    const auto& plan = *out.planner;
    std::array<std::uint64_t, 4> actions{};
    std::uint64_t candidates = 0, suffix = 0, boundaries = 0, scanned = 0;
    for (const auto& segment : plan.segments()) {
        ++actions[static_cast<std::size_t>(segment.action)];
        candidates += segment.candidates;
        suffix += segment.suffix;
        if (segment.boundary) ++boundaries;
    }
    for (const auto& report : out.reports)
        if (report.scanned) ++scanned;
    std::printf(
      ",\"classification\":{\"verdict\":\"%s\",\"wal_action\":\"%s\"",
      verdict_name(out.verdict),
      action_name(plan.wal().action));
    number("predecessor_position", plan.wal().predecessor->position().value());
    number("wal_scan_files", out.merged.wal.files);
    number("prepares", out.merged.wal.prepares);
    number("unresolved", out.unresolved);
    number("nonzero_slack", plan.wal().nonzero_slack);
    number("items", out.merged.delivered);
    number("reloads", out.merged.reloads);
    number("obligations", out.obligations);
    number("segments_scanned", scanned);
    number("boundaries", boundaries);
    number("candidates", candidates);
    number("suffix", suffix);
    number("published", out.published);
    std::printf(",\"segment_actions\":{");
    for (std::size_t a = 0; a < actions.size(); ++a)
        std::printf(
          "%s\"%s\":%" PRIu64,
          a ? "," : "",
          action_name(static_cast<recovery_plan_action>(a)),
          actions[a]);
    std::printf("},\"slots\":{");
    for (std::size_t c = 0; c < classification_names.size(); ++c)
        std::printf(
          "%s\"%s\":%" PRIu64,
          c ? "," : "",
          classification_names[c],
          out.slots[c]);
    std::printf("},\"regions\":{");
    for (std::size_t c = 0; c < classification_names.size(); ++c)
        std::printf(
          "%s\"%s\":%" PRIu64,
          c ? "," : "",
          classification_names[c],
          out.regions[c]);
    std::printf("}}");
    std::printf(",\"affinity_cpus\":[");
    bool first = true;
    for (auto cpu : affinity) {
        std::printf("%s%u", first ? "" : ",", cpu);
        first = false;
    }
    std::puts("]}");
}

// A scheduling group's I/O class is created, and its metrics registered, on
// the group's first read or write. That one-time registration is setup.
seastar::future<>
register_io_class(seastar::scheduling_group group, std::string path) {
    co_await seastar::with_scheduling_group(group, [path = std::move(path)] {
        return seastar::open_file_dma(path, seastar::open_flags::ro)
          .then([](seastar::file file) {
              return file.dma_read_bulk<char>(0, file.disk_read_dma_alignment())
                .discard_result()
                .finally([file]() mutable { return file.close(); });
          });
    });
}

struct storage_recovery_bench {
    storage_recovery_bench() {
        static const bool initialized = [] {
            codec::testing::report_profile();
            return true;
        }();
        static_cast<void>(initialized);
    }

    seastar::future<std::size_t> run_case(case_shape selected) {
        require(
          selected.segments <= fixture::maximum_extent_segments
            && (selected.segments == 0) == (selected.requests == 0)
            && (selected.segments == 0
                || (selected.requests % selected.segments == 0 && selected.per_segment() <= fixture::maximum_batches))
            && (selected.state == store_state::stopped || selected.segments >= 3)
            && (selected.state != store_state::damaged || selected.per_segment() >= 3),
          "restart benchmark shape exceeds its bound");
        native_settings settings;
        settings.memory = std::getenv(
                            "KWAQUE_STORAGE_RECOVERY_OBSERVE_ALLOCATIONS")
                          != nullptr;
        require(
          !settings.memory || !timing_only,
          "allocation observation requires storage_recovery_bench");
        const auto window = admissible_window();
        settings.reader.window_bytes = window;
        if (
          const auto* chosen = std::getenv(
            "KWAQUE_STORAGE_RECOVERY_WINDOW_BYTES")) {
            settings.reader.window_bytes = byte_count{
              std::strtoull(chosen, nullptr, 10)};
            require(
              settings.reader.window_bytes.value() >= 4_KiB
                && settings.reader.window_bytes.value() % 4_KiB == 0
                && settings.reader.window_bytes <= window,
              "a read window must be aligned and fit one allocation");
        }
        if (
          const auto* chosen = std::getenv(
            "KWAQUE_STORAGE_RECOVERY_READ_AHEAD")) {
            const auto depth = std::strtoul(chosen, nullptr, 10);
            require(
              depth <= maximum_scan_read_ahead,
              "read-ahead exceeds the scan reader's bound");
            settings.reader.read_ahead = static_cast<std::uint32_t>(depth);
        }
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
                    co_await run_in(
                      selected, settings, manager, timer, directory.get_path());
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
        co_return 1;
    }

private:
    static seastar::future<> run_in(
      const case_shape& selected,
      const native_settings& settings,
      resource::resource_manager& manager,
      runtime::production::timer& timer,
      const std::filesystem::path& directory) {
        const auto affinity = seastar::get_current_cpuset();
        file_system files{!timing_only, 0};
        const auto root = take(
          runtime::file_path::make((directory / "store").string()));
        take(co_await files.create_directories(root));
        const auto status = co_await seastar::file_stat(
          root.value(), seastar::follow_symlink::no);
        const auto spec = store_contract::specification(
          root, {status.device_id, status.inode_number}, 68);
        const std::array specs{spec};
        owner_type owner{specs};
        // Appends are produce-path work, admitted as the local append
        // benchmark admits them.
        const std::uint64_t owners = selected.segments;
        workload_budget writing{
          manager.acquire_workload(
            resource::workload_class::foreground_protocol),
          {.tasks = 1024,
           .bytes
           = byte_count{std::max<std::uint64_t>(96, 32 + 8 * owners) << 20U},
           .handles = static_cast<std::uint32_t>(32 + 16 * owners)},
          charge};
        // Restart is metadata work: up to two segment walks and the WAL
        // scan, each with its decode workspace and read-ahead, then eight
        // publications at a time beside the successor.
        workload_budget restarting{
          manager.acquire_workload(resource::workload_class::metadata),
          {.tasks = 256, .bytes = byte_count{64_MiB}, .handles = 64},
          charge};
        {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            co_await installation::bootstrap(
              files, owner, spec, writing, work, native_driver{});
        }
        auto store = co_await write_store(
          files, owner, spec, writing, timer, selected);
        co_await damage_store(files, spec, store, selected);
        co_await register_io_class(
          restarting.scheduling_group(),
          take(take(local_paths::make(spec.root)).control(0)).value());
        if (selected.state == store_state::restarted) {
            restarted first;
            ready_owners kept;
            runtime::first_failure setup;
            try {
                co_await seastar::with_scheduling_group(
                  restarting.scheduling_group(), [&] {
                      return run_restart(
                        files,
                        owner,
                        spec,
                        restarting,
                        store,
                        selected,
                        settings,
                        first,
                        kept,
                        nullptr);
                  });
            } catch (...) {
                setup.observe(std::current_exception());
            }
            try {
                co_await close_owners(kept);
            } catch (...) {
                setup.observe(std::current_exception());
            }
            take(setup.outcome());
            first.planner.reset();
            require(
              first.successor && first.successor_start,
              "the first restart did not activate a successor");
            store.wal_files.push_back(first.successor->incarnation);
            store.head = first.successor;
            store.head_start = first.successor_start;
        }

        // A restart is a new process: nothing the write phase or a first
        // restart verified or counted carries into the measured one.
        file_system restarting_files{!timing_only, 0};
        std::uint64_t wal_file_bytes = 0, data_file_bytes = 0;
        for (const auto& incarnation : store.wal_files)
            wal_file_bytes += file_bytes(wal_path(spec, incarnation));
        for (const auto& descriptor : store.descriptors)
            data_file_bytes += file_bytes(data_path(spec, descriptor));
        restarted out;
        out.reports.reserve(store.descriptors.size());
        ready_owners kept;
        measurement result;
        runtime::first_failure execution;
        try {
            {
                measure_scope interval{
                  restarting_files, result, settings.memory};
                co_await seastar::with_scheduling_group(
                  restarting.scheduling_group(), [&] {
                      return run_restart(
                        restarting_files,
                        owner,
                        spec,
                        restarting,
                        store,
                        selected,
                        settings,
                        out,
                        kept,
                        &result);
                  });
                require(
                  restarting_files.sample.active() == 0,
                  "timing stopped with native writes or flushes in flight");
            }
            verify(store, selected, out, restarting_files, result);
        } catch (...) {
            execution.observe(std::current_exception());
        }
        try {
            co_await close_owners(kept);
        } catch (...) {
            execution.observe(std::current_exception());
        }
        take(execution.outcome());
        require(
          seastar::get_current_cpuset() == affinity,
          "benchmark CPU affinity changed during execution");
        if (settings.memory)
            require(
              result.memory.observed && result.memory.complete
                && result.memory.largest_allocation
                     <= maximum_contiguous_allocation_bytes,
              "allocation observation incomplete or exceeded the contiguous "
              "ceiling");
        print_measurement(
          selected,
          settings,
          result,
          restarting_files,
          store,
          out,
          wal_file_bytes,
          data_file_bytes,
          affinity);
        out.planner.reset();
    }
};

struct restart : storage_recovery_bench {};

#define RESTART_CASE(Name, ...)                                                \
    PERF_TEST_F(restart, Name) {                                               \
        return run_case(case_shape{.name = #Name __VA_OPT__(, ) __VA_ARGS__}); \
    }
// An activated head with nothing appended.
RESTART_CASE(empty)
// Tiny requests, one group each, interleaved across eight segments.
RESTART_CASE(many_small, .requests = 128, .segments = 8)
// Requests at the largest benchmark payload, each its own group.
RESTART_CASE(large, .payload_bytes = 4_MiB, .requests = 8, .segments = 1)
RESTART_CASE(
  damaged, .requests = 128, .segments = 8, .state = store_state::damaged)
RESTART_CASE(
  candidate_heavy,
  .requests = 128,
  .segments = 8,
  .state = store_state::lost_data)
RESTART_CASE(
  clean_stop, .requests = 128, .segments = 8, .state = store_state::restarted)
#undef RESTART_CASE
} // namespace
} // namespace kwaque::storage::testing::storage_recovery_bench_support
