#include "src/base/units.h"
#include "src/bytes/test_allocation_profile.h"
#include "src/resource/resource_registry.h"
#include "src/runtime/first_failure.h"
#include "src/runtime/production/clocks.h"
#include "src/runtime/production/timer.h"
#include "src/runtime/testing/test_directory.h"
#include "src/storage/checkpoint.h"
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
#include "src/storage/tests/segment_test_support.h"
#include "src/storage/tests/segment_writer_contract.h"
#include "src/storage/tests/wal_writer_contract.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/tmp_file.hh>

#include <algorithm>
#include <array>
#include <cinttypes>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace kwaque::storage::testing::checkpoint_bench_support {
namespace {
using bytes::testing::charge;
using local_storage_bench_support::file_kind;
using local_storage_bench_support::file_system;
using local_storage_bench_support::io_sample;
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
using checkpoint_type = local_checkpoint<file_system, owner_type, clock>;

struct native_driver final {
    template<typename T>
    seastar::future<T> lifecycle(seastar::future<T> value) const {
        return value;
    }
};
model::range_routing_epoch routing() {
    return model::range_routing_epoch::make(1).value();
}

// Handle credits of the append budget for everything but open segments: the
// WAL writer and its rotation.
constexpr std::uint32_t other_handles = 32;

// A store of interleaved segments: request i targets segment i % segments,
// and each completes before the next starts, so each is its own group.
struct store_shape final {
    std::uint32_t segments{1};
    // Requests per segment.
    std::uint32_t rounds{1};
    // WAL file capacity in aligned units of the WAL's 8 KiB, its header
    // included; zero is one file that holds every request.
    std::uint32_t file_units{0};
    // The most WAL files the shard may hold; zero does not bound them.
    std::uint32_t retained_files{0};
    [[nodiscard]] std::uint32_t requests() const noexcept {
        return segments * rounds;
    }
};

// The durable local path and the owner of its checkpoints. The control
// owner, its allocator and the checkpoint owner are funded apart from the
// appends, as a shard that bounds its WAL must fund them.
struct stack final {
    std::unique_ptr<control_type> control;
    std::unique_ptr<allocator_type> ids;
    std::unique_ptr<wal_type> writer;
    std::unique_ptr<wal_group_commit> commit;
    std::vector<std::unique_ptr<segment_type>> segments;
    std::unique_ptr<append_type> append;
    std::vector<local_append_target> targets;
    std::unique_ptr<checkpoint_type> checkpoint;
    // Rotations the retained limit refused.
    std::vector<local_retention_pressure> refused;
};
// What the append path left for a restart, and what an owner supplies to
// recovery independently of it: the segment catalog.
struct written final {
    std::vector<fixture::extent_input> inputs;
    std::vector<local_segment_descriptor> descriptors;
    std::vector<recovery_catalog_entry> catalog;
    std::uint64_t wal_files{0};
};

template<typename Close>
seastar::future<> observe_close(runtime::first_failure& failed, Close close) {
    try {
        failed.observe(co_await close());
    } catch (...) {
        failed.observe(std::current_exception());
    }
}

seastar::future<written>
make_inputs(const store_shape& selected, codec::cooperative_work& work) {
    require(
      selected.segments != 0
        && selected.segments <= fixture::maximum_extent_segments
        && selected.rounds != 0 && selected.rounds <= fixture::maximum_batches,
      "checkpoint benchmark shape exceeds its bound");
    written store;
    fixture::shape shape{.name = "checkpoint"};
    shape.window = 1;
    shape.groups = selected.rounds;
    shape.blocks = 1;
    store.inputs.reserve(selected.segments);
    for (std::uint32_t f = 0; f < selected.segments; ++f) {
        store.inputs.push_back(co_await fixture::make_extent(shape, f, work));
        const auto& descriptor = store.inputs.back().descriptor;
        store.descriptors.push_back(descriptor);
        store.catalog.push_back(
          {descriptor,
           take(
             segment_header::make(
               descriptor.segment,
               descriptor.logical_origin,
               descriptor.alignment))});
    }
    co_return store;
}

seastar::future<> open_stack(
  stack& s,
  file_system& files,
  owner_type& owner,
  const local_device_spec& spec,
  std::span<const local_device_spec> specs,
  workload_budget& writing,
  workload_budget& reserve,
  runtime::production::timer& timer,
  const store_shape& selected,
  written& store,
  codec::cooperative_work& work) {
    s.control = take(
      co_await control_type::open(
        files, owner, spec, 0, false, reserve, store_contract::limits(), work));
    // One block of object sequences covers every checkpoint of a case, so a
    // measured run never also publishes the allocator's next block.
    s.ids = take(allocator_type::make(*s.control, reserve, 64));
    auto wal = wal_writer_contract::configuration();
    wal.capacity_bytes = selected.file_units == 0
                           ? byte_count{128_MiB}
                           : byte_count{
                               std::uint64_t{selected.file_units} * 8_KiB};
    wal.retained_files = selected.retained_files;
    s.writer = take(
      wal_type::make(
        *s.control, *s.ids, writing, wal, wal_start_intent::known_unactivated));
    take(
      co_await seastar::with_scheduling_group(
        writing.scheduling_group(),
        [&s, &work] { return s.writer->bootstrap(work); }));
    const auto origin = s.writer->progress()->reserved;
    wal_group_commit_config cohort;
    cohort.maximum_wait = runtime::monotonic_duration{0};
    s.commit = take(wal_group_commit::make(*s.writer, writing, cohort));
    take(s.commit->template start<clock>(timer));
    // As a shard does before it starts: the append budget must be able to
    // hold every segment open at once, beside what the WAL needs.
    take(validate_segment_handles(
      writing,
      segment_writer_contract::configuration(),
      {.segments = selected.segments, .reserved = other_handles}));
    const auto handles_before = writing.snapshot().handles;
    s.segments.reserve(selected.segments);
    for (std::uint32_t f = 0; f < selected.segments; ++f) {
        auto config = segment_writer_contract::configuration();
        config.retry_object = local_object_sequence::make(45 + f).value();
        config.policy = work.policy();
        config.admission.working_bytes = byte_count{1_MiB};
        s.segments.push_back(take(
          segment_type::make_new(
            files,
            owner,
            spec,
            0,
            store.inputs[f].descriptor,
            writing,
            config)));
        auto& created = *s.segments.back();
        take(
          co_await seastar::with_scheduling_group(
            writing.scheduling_group(),
            [&created, &work] { return created.create_new(work); }));
    }
    require(
      writing.snapshot().handles - handles_before
        == std::uint64_t{selected.segments}
             * segment_writer_handles(segment_writer_contract::configuration()),
      "open segments do not hold the handle credits the start check counts");
    local_append_config config;
    config.maximum_segments = selected.segments;
    config.maximum_requests = 128;
    config.policy = work.policy();
    s.refused.reserve(16);
    s.append = take(
      append_type::make(
        writing,
        *s.commit,
        *s.writer,
        config,
        {},
        [&s](const local_retention_pressure& report) noexcept {
            if (s.refused.size() != s.refused.capacity())
                s.refused.push_back(report);
        }));
    s.targets.reserve(selected.segments);
    for (auto& segment : s.segments)
        s.targets.push_back(take(s.append->attach(*segment)));
    s.checkpoint = take(
      checkpoint_type::make(
        files,
        owner,
        specs,
        spec,
        0,
        *s.control,
        *s.ids,
        *s.append,
        reserve,
        {.pins = {}, .metadata = store_contract::limits()},
        codec::limits::defaults(),
        origin));
}

// Every owner closes, in reverse, whatever failed before.
seastar::future<> close_stack(stack& s) {
    runtime::first_failure failed;
    if (s.checkpoint)
        co_await observe_close(failed, [&s] { return s.checkpoint->close(); });
    s.checkpoint.reset();
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
    take(failed.outcome());
}

// Request i, from acceptance to local durability. Its child's backing is
// held only while it is appended, as a serial producer's would be.
seastar::future<> append_request(
  stack& s,
  written& store,
  std::uint32_t i,
  workload_budget& writing,
  codec::cooperative_work& work) {
    const auto segments = static_cast<std::uint32_t>(store.inputs.size());
    const auto f = i % segments;
    auto& child = store.inputs[f].groups[i / segments].children[0];
    auto backing = take(writing.try_reserve_buffer(child.bytes()));
    const auto history = store.inputs[f].history;
    auto stages = s.append->append(
      s.targets[f],
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
      acceptance.accepted(), "a request's segment group could not hold it");
    take(outcome.failure.outcome());
    require(
      outcome.status == local_append_status::durable
        && outcome.receipt.has_value(),
      "a request did not become locally durable");
}

// Native work of one interval, by the kind of file it touched.
struct io_delta final {
    std::uint64_t elapsed{0};
    std::uint64_t flushes{0}, wal_flushes{0}, data_flushes{0};
    std::uint64_t directory_syncs{0}, listings{0};
    std::uint64_t wal_reads{0}, wal_read_bytes{0};
    std::uint64_t data_reads{0}, data_read_bytes{0};
    std::uint64_t writes{0}, write_bytes{0};
};
struct io_mark final {
    std::uint64_t at{0};
    io_sample sample;
};
io_mark mark(const file_system& files) {
    return {measurement_time(), files.sample};
}
io_delta since(const file_system& files, const io_mark& from) {
    const auto& now = files.sample;
    const auto& then = from.sample;
    io_delta out;
    out.elapsed = measurement_time() - from.at;
    for (std::size_t k = 0; k < now.kinds.size(); ++k) {
        out.flushes += now.kinds[k].flushes - then.kinds[k].flushes;
        out.writes += now.kinds[k].calls - then.kinds[k].calls;
        out.write_bytes += now.kinds[k].bytes - then.kinds[k].bytes;
    }
    out.wal_flushes = now[file_kind::wal].flushes
                      - then[file_kind::wal].flushes;
    out.data_flushes = now[file_kind::data].flushes
                       - then[file_kind::data].flushes;
    out.directory_syncs = now.directory_syncs - then.directory_syncs;
    out.listings = now.listings - then.listings;
    out.wal_reads = now[file_kind::wal].reads - then[file_kind::wal].reads;
    out.wal_read_bytes = now[file_kind::wal].read_bytes
                         - then[file_kind::wal].read_bytes;
    out.data_reads = now[file_kind::data].reads - then[file_kind::data].reads;
    out.data_read_bytes = now[file_kind::data].read_bytes
                          - then[file_kind::data].read_bytes;
    return out;
}

void print_io(const io_delta& io) {
    std::printf(
      ",\"elapsed_ns\":%" PRIu64 ",\"flushes\":%" PRIu64
      ",\"wal_flushes\":%" PRIu64 ",\"data_flushes\":%" PRIu64
      ",\"directory_syncs\":%" PRIu64 ",\"listings\":%" PRIu64
      ",\"wal_reads\":%" PRIu64 ",\"wal_read_bytes\":%" PRIu64
      ",\"data_reads\":%" PRIu64 ",\"data_read_bytes\":%" PRIu64
      ",\"writes\":%" PRIu64 ",\"write_bytes\":%" PRIu64,
      io.elapsed,
      io.flushes,
      io.wal_flushes,
      io.data_flushes,
      io.directory_syncs,
      io.listings,
      io.wal_reads,
      io.wal_read_bytes,
      io.data_reads,
      io.data_read_bytes,
      io.writes,
      io.write_bytes);
}

// The p-th percentile of sorted samples, by the nearest rank.
std::uint64_t percentile(const std::vector<std::uint64_t>& sorted, unsigned p) {
    if (sorted.empty()) return 0;
    const auto rank = (sorted.size() * p + 99) / 100;
    return sorted[std::max<std::size_t>(rank, 1) - 1];
}

// The largest aligned read window this allocator profile serves within one
// contiguous allocation.
byte_count admissible_window() {
    auto window = maximum_contiguous_allocation_bytes;
    while (charge(byte_count{window}).value()
           > maximum_contiguous_allocation_bytes)
        window -= 4_KiB;
    return byte_count{window};
}
recovery_merge_limits merge_limits() {
    recovery_merge_limits limits;
    limits.wal.metadata = store_contract::limits();
    limits.segment.metadata = store_contract::limits();
    limits.wal.reader.window_bytes = admissible_window();
    limits.segment.reader.window_bytes = admissible_window();
    return limits;
}
segment_writer_config publication_configuration() {
    auto config = segment_writer_contract::configuration();
    config.admission.working_bytes = byte_count{1_MiB};
    return config;
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

// What one restart read to become ready.
struct restart_sample final {
    io_delta io;
    std::uint64_t wal_scan_files{0}, prepares{0}, removed{0}, raised{0};
    bool checkpointed{false};
};
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
struct plan_view final {
    recovery_planner* planner;
    seastar::future<runtime::result<bool>>
    operator()(const recovery_item& item) const {
        if (auto observed = planner->observe(item); !observed)
            return seastar::make_ready_future<runtime::result<bool>>(
              runtime::failure(observed.error()));
        return seastar::make_ready_future<runtime::result<bool>>(true);
    }
};

// The restart of a stopped store up to ready. The durable checkpoint, when
// the store has one, is where the WAL scan starts and what each segment
// resumes from; every WAL name below its file goes unread.
seastar::future<> run_restart(
  file_system& files,
  owner_type& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  const written& store,
  restart_sample& out,
  ready_owners& owners) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array specs{spec};
    const auto report = take(
      co_await inspect_local_recovery(
        files,
        owner,
        std::span<const local_device_spec>{specs},
        budget,
        store_contract::limits(),
        work));
    require(
      report.verdict == recovery_store_verdict::ready,
      "a stopped store was not ready to recover");
    owners.control = take(
      co_await control_type::open(
        files, owner, spec, 0, true, budget, store_contract::limits(), work));
    const auto fields = take(owners.control->snapshot()).fields;
    auto loaded = take(
      co_await load_checkpoint(
        files, owner, spec, 0, fields, budget, store_contract::limits(), work));
    std::optional<local_wal_cursor> cutoff;
    if (loaded) {
        out.checkpointed = true;
        cutoff = loaded->end;
    }
    std::vector<recovery_decision_record> decided;
    const auto discovered = take(
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
    if (loaded)
        out.raised = take(
                       resume_from_checkpoint(loaded->entries, found, decided))
                       .raised;
    auto planner = take(recovery_planner::make(found.targets, budget));
    const auto merged = take(
      co_await reconcile_local_recovery(
        files,
        owner,
        std::span<const local_device_spec>{specs},
        spec,
        0,
        fields,
        cutoff,
        found.targets,
        budget,
        merge_limits(),
        work,
        plan_view{&planner}));
    take(planner.finish(merged));
    require(planner.ready(), "a store's restart plan stopped");
    // Only a plan that is ready removes anything: one that stops must find
    // the store as the crash left it.
    if (out.checkpointed) {
        const auto below = take(
          co_await remove_wal_below(
            files, owner, spec, 0, *owners.control, work));
        out.removed = below.removed;
        static_cast<void>(take(
          co_await remove_stale_checkpoints(
            files, owner, spec, 0, *owners.control, work)));
    }
    out.wal_scan_files = merged.wal.files;
    out.prepares = merged.wal.prepares;
    owners.ids = take(allocator_type::make(*owners.control, budget, 4));
    owners.writer = take(
      wal_type::make(
        *owners.control,
        *owners.ids,
        budget,
        wal_writer_contract::configuration(),
        wal_start_intent::recovered_head));
    const auto published = take(
      co_await establish_recovered_state<clock>(
        files,
        owner,
        std::span<const local_device_spec>{specs},
        0,
        planner,
        found,
        std::span<const recovery_visibility>{},
        budget,
        publication_configuration(),
        {},
        *owners.writer,
        codec::limits::defaults(),
        abort));
    require(
      published.complete,
      "a recovered segment was not flushed and published as recovering");
    // The successor joins the files the scan found.
    require(
      owners.writer->retention().files == merged.wal.chain_files + 1,
      "the restarted writer does not count the chain it was handed");
}

// One store directory, its two budgets and their owners, for one body.
struct environment final {
    file_system& files;
    owner_type& owner;
    const local_device_spec& spec;
    std::span<const local_device_spec> specs;
    workload_budget& writing;
    workload_budget& reserve;
    workload_budget& restarting;
    runtime::production::timer& timer;
};

struct checkpoint_bench {
    template<typename Body>
    seastar::future<std::size_t>
    run_stores(std::uint32_t segments, Body body) const {
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
                      segments, manager, timer, directory.get_path(), body);
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
    // Each store of a body is a fresh directory under the case's own.
    template<typename Body>
    static seastar::future<> run_in(
      std::uint32_t segments,
      resource::resource_manager& manager,
      runtime::production::timer& timer,
      const std::filesystem::path& directory,
      Body& body) {
        // Appends are produce-path work, admitted as the local append
        // benchmark admits them, within what their class has at this
        // reactor's memory.
        const std::uint64_t owners = segments;
        auto producing = manager.acquire_workload(
          resource::workload_class::foreground_protocol);
        const auto share = std::min<std::uint64_t>(
          std::max<std::uint64_t>(96, 32 + 8 * owners) << 20U,
          producing.hard_budget().value());
        workload_budget writing{
          std::move(producing),
          {.tasks = 1024,
           .bytes = byte_count{share},
           .handles = static_cast<std::uint32_t>(
             other_handles + segment_creation_handles
             + owners
                 * segment_writer_handles(
                   segment_writer_contract::configuration()))},
          charge};
        // Checkpoints, the control they replace and its allocator are
        // metadata work, funded apart from the appends and bounded here far
        // below them: every run of every case fits these tasks, bytes and
        // handle credits, whatever the number of segments.
        workload_budget reserve{
          manager.acquire_workload(resource::workload_class::metadata),
          {.tasks = 64, .bytes = byte_count{8_MiB}, .handles = 16},
          charge};
        // A restart: up to two segment walks and the WAL scan, each with its
        // decode workspace, then the publications beside the successor.
        workload_budget restarting{
          manager.acquire_workload(resource::workload_class::metadata),
          {.tasks = 256, .bytes = byte_count{64_MiB}, .handles = 64},
          charge};
        std::uint32_t stores = 0;
        const auto with_store = [&](auto use) -> seastar::future<> {
            file_system files{true, 0};
            const auto root = take(
              runtime::file_path::make(
                (directory / ("store" + std::to_string(stores++))).string()));
            take(co_await files.create_directories(root));
            const auto status = co_await seastar::file_stat(
              root.value(), seastar::follow_symlink::no);
            const auto spec = store_contract::specification(
              root, {status.device_id, status.inode_number}, 68);
            const std::array specs{spec};
            owner_type owner{specs};
            {
                seastar::abort_source abort;
                codec::cooperative_work work{codec::limits::defaults(), abort};
                co_await installation::bootstrap(
                  files, owner, spec, writing, work, native_driver{});
            }
            co_await register_io_class(
              reserve.scheduling_group(),
              take(take(local_paths::make(spec.root)).control(0)).value());
            co_await use(
              environment{
                files,
                owner,
                spec,
                std::span<const local_device_spec>{specs},
                writing,
                reserve,
                restarting,
                timer});
        };
        co_await body(with_store);
    }
};

// One checkpoint, measured alone, of a store whose every segment advanced
// since the checkpoint before it and whose WAL left a file behind.
struct cost_sample final {
    std::uint32_t segments{0};
    io_delta io;
    // What the checkpoint owner, the control and its allocator are admitted
    // against, and what that budget had refused by the end of the run.
    workload_budget_limits bound;
    std::uint64_t refused{0};
    std::optional<checkpoint_outcome> ran;
};
seastar::future<cost_sample>
checkpoint_cost(const environment& env, std::uint32_t segments) {
    // Four rounds over the segments; two fill one WAL file.
    const store_shape selected{
      .segments = segments, .rounds = 4, .file_units = 2 * segments + 1};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto store = co_await make_inputs(selected, work);
    stack s;
    cost_sample out{.segments = segments};
    runtime::first_failure failed;
    try {
        co_await open_stack(
          s,
          env.files,
          env.owner,
          env.spec,
          env.specs,
          env.writing,
          env.reserve,
          env.timer,
          selected,
          store,
          work);
        std::uint32_t i = 0;
        for (; i != segments; ++i)
            co_await append_request(s, store, i, env.writing, work);
        // The first checkpoint pins every segment once, unmeasured.
        const auto first = take(co_await s.checkpoint->request());
        require(
          first.published && first.entries == segments,
          "the first checkpoint did not pin every segment");
        for (; i != selected.requests(); ++i)
            co_await append_request(s, store, i, env.writing, work);
        require(
          s.writer->statistics().rotations != 0,
          "the store left no WAL file below its head");
        env.files.sample.measuring = true;
        const auto before = mark(env.files);
        perf_tests::start_measuring_time();
        out.ran.emplace(take(co_await s.checkpoint->request()));
        perf_tests::stop_measuring_time();
        out.io = since(env.files, before);
        env.files.sample.measuring = false;
        out.bound = env.reserve.limits();
        out.refused = env.reserve.snapshot().rejected;
        require(
          out.ran->published && out.ran->entries == segments
            && out.ran->wal_removed != 0 && out.ran->wal_owed == 0
            && !out.ran->wal_failed && out.ran->unretired == 0
            && out.ran->end == s.writer->progress()->durable,
          "the measured checkpoint did not pin every segment, move the "
          "cutoff to the durable end and remove the WAL below it");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        co_await close_stack(s);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(failed.outcome());
    co_return out;
}

// Whether a checkpoint is asked for around each measured request.
enum class overlap : std::uint8_t {
    // No checkpoint runs.
    quiet,
    // A run is asked for just before each request and joined after it, so
    // every request shares the device, and here the scheduling group too,
    // with a publishing checkpoint.
    every_request,
    // Nobody asks: the checkpoint owner reclaims on its own as the WAL
    // rotates under a retained limit.
    bound,
};
const char* overlap_name(overlap value) noexcept {
    switch (value) {
    case overlap::quiet:
        return "quiet";
    case overlap::every_request:
        return "every_request";
    case overlap::bound:
        return "bound";
    }
    std::abort();
}
struct latency_sample final {
    std::vector<std::uint64_t> sorted;
    std::uint64_t rotations{0}, checkpoints{0}, refused{0};
    wal_retention held;
    io_delta io;
};
seastar::future<latency_sample>
append_latency(const environment& env, std::uint32_t segments, overlap mode) {
    store_shape selected{.segments = segments, .rounds = 32};
    if (mode == overlap::bound) {
        // Four rounds a file, eight files, and at most four of them held.
        selected.file_units = 4 * segments + 1;
        selected.retained_files = 4;
    }
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto store = co_await make_inputs(selected, work);
    stack s;
    latency_sample out;
    out.sorted.reserve(selected.requests());
    runtime::first_failure failed;
    try {
        co_await open_stack(
          s,
          env.files,
          env.owner,
          env.spec,
          env.specs,
          env.writing,
          env.reserve,
          env.timer,
          selected,
          store,
          work);
        if (mode == overlap::bound) take(s.checkpoint->bind_retention());
        env.files.sample.measuring = true;
        const auto before = mark(env.files);
        perf_tests::start_measuring_time();
        for (std::uint32_t i = 0; i != selected.requests(); ++i) {
            std::optional<seastar::future<checkpoint_type::output>> running;
            if (mode == overlap::every_request)
                running.emplace(s.checkpoint->request());
            const auto started = measurement_time();
            co_await append_request(s, store, i, env.writing, work);
            out.sorted.push_back(measurement_time() - started);
            if (running && take(co_await std::move(*running)).published)
                ++out.checkpoints;
        }
        perf_tests::stop_measuring_time();
        out.io = since(env.files, before);
        env.files.sample.measuring = false;
        // A last run, also for the one a rotation may have left in flight.
        static_cast<void>(take(co_await s.checkpoint->request()));
        out.rotations = s.writer->statistics().rotations;
        out.refused = s.refused.size();
        out.held = s.append->retention();
        require(
          out.refused == 0 && !s.append->retention_pressure()
            && !s.checkpoint->failure().failed(),
          "a shard nothing pins met its retained limit");
        if (mode == overlap::bound)
            require(
              out.rotations != 0 && out.held.files == 1
                && s.append->retained_wal().files() == 1,
              "the bound shard kept a closed WAL file nobody asked it to "
              "remove");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        co_await close_stack(s);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(failed.outcome());
    std::sort(out.sorted.begin(), out.sorted.end());
    co_return out;
}

// A store written across `files` WAL files and stopped, with or without the
// checkpoint a graceful stop ends with, then restarted.
seastar::future<restart_sample> restart_after(
  const environment& env, std::uint32_t segments, bool stop_checkpoint) {
    // Eight rounds over the segments, two to a WAL file: four files.
    const store_shape selected{
      .segments = segments, .rounds = 8, .file_units = 2 * segments + 1};
    restart_sample out;
    written store;
    {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        store = co_await make_inputs(selected, work);
        stack s;
        runtime::first_failure failed;
        try {
            co_await open_stack(
              s,
              env.files,
              env.owner,
              env.spec,
              env.specs,
              env.writing,
              env.reserve,
              env.timer,
              selected,
              store,
              work);
            for (std::uint32_t i = 0; i != selected.requests(); ++i)
                co_await append_request(s, store, i, env.writing, work);
            store.wal_files = s.writer->statistics().rotations + 1;
            require(store.wal_files > 1, "the store's WAL stayed in one file");
            if (stop_checkpoint) {
                // After the appends drained: an ordinary checkpoint that
                // ends at the WAL-durable end.
                const auto last = take(co_await s.checkpoint->request());
                require(
                  last.published && last.end == s.writer->progress()->durable
                    && last.wal_removed == store.wal_files - 1,
                  "the stop checkpoint did not end at the durable end and "
                  "leave one WAL file");
            }
        } catch (...) {
            failed.observe(std::current_exception());
        }
        try {
            co_await close_stack(s);
        } catch (...) {
            failed.observe(std::current_exception());
        }
        take(failed.outcome());
    }
    // A restart is a new process: nothing the write phase counted carries
    // into it.
    file_system restarting{true, 0};
    ready_owners kept;
    runtime::first_failure failed;
    try {
        restarting.sample.measuring = true;
        const auto before = mark(restarting);
        perf_tests::start_measuring_time();
        co_await seastar::with_scheduling_group(
          env.restarting.scheduling_group(), [&] {
              return run_restart(
                restarting,
                env.owner,
                env.spec,
                env.restarting,
                store,
                out,
                kept);
          });
        perf_tests::stop_measuring_time();
        out.io = since(restarting, before);
        restarting.sample.measuring = false;
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        co_await close_owners(kept);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(failed.outcome());
    if (stop_checkpoint)
        require(
          out.checkpointed && out.prepares == 0 && out.wal_scan_files == 1
            && out.raised == segments,
          "a restart after a stop checkpoint read WAL below its cutoff");
    else
        require(
          !out.checkpointed && out.prepares == selected.requests()
            && out.wal_scan_files == store.wal_files,
          "a restart without a checkpoint did not scan the whole chain");
    co_return out;
}

struct qualification : checkpoint_bench {};

// A checkpoint flushes nothing of the WAL or of any segment, and its
// durable operations do not grow with the segments it pins: one bundle and
// one control replacement. It reads one footer per segment that advanced
// and, of the WAL, no more than the headers of the files new to it.
PERF_TEST_F(qualification, checkpoint_cost_by_segments) {
    constexpr std::array<std::uint32_t, 3> counts{
      1, 8, fixture::maximum_extent_segments};
    return run_stores(
      counts.back(), [counts](auto& with_store) -> seastar::future<> {
          std::vector<cost_sample> samples;
          for (const auto segments : counts)
              co_await with_store(
                [&samples, segments](environment env) -> seastar::future<> {
                    samples.push_back(co_await checkpoint_cost(env, segments));
                });
          for (const auto& sample : samples) {
              std::printf(
                "checkpoint_bench_v1 {\"case\":\"checkpoint_cost\","
                "\"segments\":%u,\"entries\":%u,\"wal_removed\":%u",
                sample.segments,
                sample.ran->entries,
                sample.ran->wal_removed);
              print_io(sample.io);
              std::printf(
                ",\"reserve_tasks\":%u,\"reserve_bytes\":%" PRIu64
                ",\"reserve_handles\":%u,\"reserve_refused\":%" PRIu64 "}\n",
                sample.bound.tasks,
                sample.bound.bytes.value(),
                sample.bound.handles,
                sample.refused);
              require(
                sample.io.wal_flushes == 0 && sample.io.data_flushes == 0
                  && sample.io.listings == 0,
                "a checkpoint flushed appended data or listed a directory");
              // The first sample has one segment: its reads are one footer's.
              require(
                sample.io.data_reads != 0
                  && sample.io.data_reads
                       == samples.front().io.data_reads * sample.segments,
                "a checkpoint's segment reads are not one footer per segment "
                "that advanced");
              require(
                sample.io.flushes == samples.front().io.flushes
                  && sample.io.directory_syncs
                       == samples.front().io.directory_syncs
                  && sample.io.wal_reads == samples.front().io.wal_reads
                  && sample.io.wal_read_bytes
                       == samples.front().io.wal_read_bytes,
                "a checkpoint's durable operations or WAL reads grew with its "
                "segments");
          }
      });
}

void print_latency(
  std::uint32_t segments, overlap mode, const latency_sample& sample) {
    std::printf(
      "checkpoint_bench_v1 {\"case\":\"append_latency\",\"segments\":%u,"
      "\"checkpoints\":\"%s\",\"requests\":%zu,\"p50_ns\":%" PRIu64
      ",\"p99_ns\":%" PRIu64 ",\"max_ns\":%" PRIu64 ",\"rotations\":%" PRIu64
      ",\"published\":%" PRIu64 ",\"refused\":%" PRIu64
      ",\"retained_files\":%u,\"retained_limit\":%u",
      segments,
      overlap_name(mode),
      sample.sorted.size(),
      percentile(sample.sorted, 50),
      percentile(sample.sorted, 99),
      sample.sorted.empty() ? 0 : sample.sorted.back(),
      sample.rotations,
      sample.checkpoints,
      sample.refused,
      sample.held.files,
      sample.held.limit);
    print_io(sample.io);
    std::puts("}");
}
// Append latency over eight interleaved segments: with no checkpoint, with
// one publishing beside every request, and with the shard bound to a
// retained limit so that it reclaims on its own. No append scans a
// directory in any of them.
PERF_TEST_F(qualification, append_latency_by_checkpoint_overlap) {
    constexpr std::uint32_t segments = 8;
    return run_stores(segments, [](auto& with_store) -> seastar::future<> {
        for (const auto mode :
             {overlap::quiet, overlap::every_request, overlap::bound})
            co_await with_store([mode](environment env) -> seastar::future<> {
                const auto sample = co_await append_latency(
                  env, segments, mode);
                print_latency(segments, mode, sample);
                require(
                  sample.io.listings == 0,
                  "an append or a checkpoint beside it listed a directory");
            });
    });
}

// Restart against the WAL a stop left: the whole chain when no checkpoint
// was taken, and nothing below the cutoff after the checkpoint a graceful
// stop ends with.
PERF_TEST_F(qualification, restart_by_retained_wal) {
    constexpr std::uint32_t segments = 8;
    return run_stores(segments, [](auto& with_store) -> seastar::future<> {
        for (const bool stop_checkpoint : {false, true})
            co_await with_store(
              [stop_checkpoint](environment env) -> seastar::future<> {
                  const auto sample = co_await restart_after(
                    env, segments, stop_checkpoint);
                  std::printf(
                    "checkpoint_bench_v1 {\"case\":\"restart\","
                    "\"segments\":%u,\"stop_checkpoint\":%s,"
                    "\"wal_scan_files\":%" PRIu64 ",\"prepares\":%" PRIu64
                    ",\"wal_names_removed\":%" PRIu64
                    ",\"pins_raised\":%" PRIu64,
                    segments,
                    stop_checkpoint ? "true" : "false",
                    sample.wal_scan_files,
                    sample.prepares,
                    sample.removed,
                    sample.raised);
                  print_io(sample.io);
                  std::puts("}");
              });
    });
}
} // namespace
} // namespace kwaque::storage::testing::checkpoint_bench_support
