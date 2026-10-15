#include "src/base/units.h"
#include "src/codec/tests/memory_qualification_support.h"
#include "src/resource/resource_manager.h"
#include "src/runtime/file.h"
#include "src/runtime/first_failure.h"
#include "src/runtime/production/timer.h"
#include "src/runtime/testing/test_directory.h"
#include "src/storage/local_append.h"
#include "src/storage/local_engine.h"
#include "src/storage/local_paths.h"
#include "src/storage/recovery_inventory.h"
#include "src/storage/segment_lifecycle.h"
#include "src/storage/tests/engine_bench_fixture.h"
#include "src/storage/tests/local_append_contract.h"
#include "src/storage/tests/local_engine_contract.h"
#include "src/storage/tests/local_storage_bench_file.h"
#include "src/storage/tests/local_store_contract.h"
#include "src/storage/tests/segment_bench_observer.h"
#include "src/storage/tests/wal_bench_file.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/posix.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread_cputime_clock.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/tmp_file.hh>

#include <array>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <optional>
#include <set>
#include <string>
#include <vector>

// A shard's engine started on a store it wrote and stopped, timed whole from
// the engine's construction until the shard is ready, over a table of stores
// that differ in what the owner of segments' catalog lists: sealed segments,
// segments left writable at the stop, and sealed segments the store retains
// that the catalog does not list.
namespace kwaque::storage::testing::engine_start_bench_support {
namespace {
namespace bench = engine_bench_support;
namespace measured = local_storage_bench_support;
namespace appends = local_append_contract;
namespace engines = local_engine_contract;
namespace observer = segment_bench_support;
using measured::file_kind;
using measured::kind_sample;
using measured::measurement_time;
using store_contract::require;
using store_contract::take;
using start_report = kwaque::storage::engine_start;
#if defined(KWAQUE_STORAGE_RECOVERY_TIMING_ONLY)
constexpr bool timing_only = true;
#else
constexpr bool timing_only = false;
#endif

// Segments the table may leave unsealed: the same bound in every case, so
// that every start makes the same owners.
constexpr std::uint32_t writable_bound = 4;
// Tiny requests appended to each segment, one at a time, before it is sealed
// or left writable.
constexpr std::uint32_t requests_per_segment = 4;
// The most sealed segments one restart's catalog lists beside every segment
// the shard may leave writable or being removed: the catalog's 122 entries
// less the 4 writable and the table's 16 deleting.
constexpr std::uint32_t top = 102;

// One store and the catalog its start is given. Segments are written in
// generation order: the sealed ones the catalog lists, then the sealed ones
// it does not, then those left writable at the stop, which it lists. Every
// segment is a generation of one segment identity, so the store holds them
// all in one bucket directory; a production store spreads them over many.
struct case_shape final {
    const char* name;
    std::uint32_t sealed{0}, unsealed{0}, unlisted{0};
    [[nodiscard]] std::uint32_t written() const noexcept {
        return sealed + unlisted + unsealed;
    }
    [[nodiscard]] std::uint32_t listed() const noexcept {
        return sealed + unsealed;
    }
};

const char* state_name(engine_state value) noexcept {
    switch (value) {
    case engine_state::constructed:
        return "constructed";
    case engine_state::starting:
        return "starting";
    case engine_state::recovering:
        return "recovering";
    case engine_state::ready:
        return "ready";
    case engine_state::stopping:
        return "stopping";
    case engine_state::stopped:
        return "stopped";
    }
    std::abort();
}

// Writes the store through the engine on a store never used before: each
// segment created through the table on the device the device table chooses,
// its requests appended and made durable one at a time, and sealed through
// the table before the next is created, except those left writable. The stop
// closes those unsealed and takes the checkpoint that lets go of the sealed
// ones. Nothing here is measured.
seastar::future<>
write_store(bench::bench_store& store, const case_shape& shape) {
    const auto began = co_await bench::start(store, {});
    take(began.failure.outcome());
    require(
      began.state == engine_state::ready && !began.restarted,
      "a store that was never used did not start ready");
    for (std::uint32_t generation = 1; generation <= shape.written();
         ++generation) {
        // The sampler's part: each creation sees a share sampled for it, as
        // one sampler period would hand it.
        bench::share(store, byte_count{1_GiB});
        const auto target = take(co_await bench::create(store, generation));
        for (std::uint32_t k = 0; k != requests_per_segment; ++k) {
            const auto outcome = co_await bench::append(
              store, target, 100 + k, generation);
            take(outcome.failure.outcome());
            require(
              outcome.status == local_append_status::durable
                && outcome.receipt.has_value(),
              "a store request did not become locally durable");
        }
        if (generation > shape.sealed + shape.unlisted) continue;
        const auto sealed = co_await store.engine->table()->seal(
          appends::context(generation));
        take(sealed.failure.outcome());
        require(sealed.extent.has_value(), "a store segment was not sealed");
    }
    co_await bench::stop(store);
}

// The catalog the owner of segments gives the start: the sealed segments the
// case lists and every segment left writable, which a start must find.
std::vector<recovery_catalog_entry> catalog(const case_shape& shape) {
    std::vector<recovery_catalog_entry> listed;
    listed.reserve(shape.listed());
    for (std::uint32_t generation = 1; generation <= shape.sealed; ++generation)
        listed.push_back(engines::listed(generation));
    for (std::uint32_t generation = shape.sealed + shape.unlisted + 1;
         generation <= shape.written();
         ++generation)
        listed.push_back(engines::listed(generation));
    return listed;
}

// The files the store holds when it is started, and their bytes, by kind.
struct stored final {
    std::uint64_t files{0}, bytes{0};
};
using store_bytes = std::array<stored, measured::file_kinds>;
store_bytes on_device(const std::filesystem::path& root) {
    store_bytes held{};
    for (const auto& entry :
         std::filesystem::recursive_directory_iterator(root)) {
        if (!entry.is_regular_file()) continue;
        const auto kind = measured::classify(
          take(runtime::file_path::make(entry.path().string())));
        auto& counted = held[static_cast<std::size_t>(kind)];
        ++counted.files;
        counted.bytes += entry.file_size();
    }
    return held;
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

struct measurement final {
    std::uint64_t elapsed{0}, cpu{0}, allocations{0}, tasks{0},
      runtime_operations{0};
    wal_bench_support::reactor_io io;
    observer::work_sample work;
};

// The start up to ready, and nothing after it. Native work, checksums and
// the reactor's I/O count only inside.
class measure_scope final {
public:
    measure_scope(measured::file_system& files, measurement& output)
      : files_(files)
      , output_(output)
      , before_allocations_(seastar::memory::stats().mallocs())
      , before_tasks_(seastar::engine().get_sched_stats().tasks_processed)
      , before_operations_(files.statistics().accepted)
      , before_io_(wal_bench_support::reactor_io::now()) {
        files_.sample.measuring = true;
#if !defined(KWAQUE_STORAGE_RECOVERY_TIMING_ONLY)
        observer::begin_work_observation(output_.work);
#endif
        cpu_ = seastar::thread_cputime_clock::now();
        epoch_ = measurement_time();
        perf_tests::start_measuring_time();
    }
    ~measure_scope() {
        perf_tests::stop_measuring_time();
        output_.elapsed = measurement_time() - epoch_;
        output_.cpu = static_cast<std::uint64_t>(
          (seastar::thread_cputime_clock::now() - cpu_).count());
        files_.sample.measuring = false;
#if !defined(KWAQUE_STORAGE_RECOVERY_TIMING_ONLY)
        observer::end_work_observation();
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
    measured::file_system& files_;
    measurement& output_;
    std::uint64_t before_allocations_, before_tasks_, before_operations_;
    wal_bench_support::reactor_io before_io_;
    seastar::thread_cputime_clock::time_point cpu_;
    std::uint64_t epoch_{0};
};

// The start found the store it was given: the shard is ready, every segment
// left writable is taken in as recovered, and nothing else was found to
// remove, resume, seal or resolve.
void verify(
  const start_report& began,
  const case_shape& shape,
  const bench::engine_type& engine) {
    take(began.failure.outcome());
    require(
      began.state == engine_state::ready && began.restarted
        && engine.state() == engine_state::ready,
      "the start did not leave the shard ready");
    require(
      began.recovered == shape.unsealed && began.unadopted == 0
        && began.resumed == 0 && began.marked_retained == 0 && began.gone == 0
        && began.owed == 0 && began.refused == 0 && began.sealing == 0
        && began.unresolved == 0 && began.wal_files != 0,
      "the start found another store than the one written");
    for (std::uint32_t generation = shape.sealed + shape.unlisted + 1;
         generation <= shape.written();
         ++generation)
        require(
          engine.table()->life(appends::context(generation))
            == segment_life::recovering,
          "a segment left writable was not taken in as recovered");
}

// Only the observing build reports native work.
[[maybe_unused]] void print_kind(const char* name, const kind_sample& sample) {
    std::printf(
      ",\"%s\":{\"reads\":%" PRIu64 ",\"read_bytes\":%" PRIu64
      ",\"read_minimum\":%" PRIu64 ",\"read_maximum\":%" PRIu64
      ",\"opens\":%" PRIu64 ",\"writes\":%" PRIu64 ",\"write_bytes\":%" PRIu64
      ",\"flushes\":%" PRIu64 ",\"unlinks\":%" PRIu64 "}",
      name,
      sample.reads,
      sample.read_bytes,
      sample.reads == 0 ? 0 : sample.read_minimum,
      sample.read_maximum,
      sample.opens,
      sample.calls,
      sample.bytes,
      sample.flushes,
      sample.unlinks);
}

void print_measurement(
  const case_shape& shape,
  const local_engine_config& config,
  const measurement& value,
  const start_report& began,
  const store_bytes& held,
  const measured::file_system& files,
  const std::set<unsigned>& affinity) {
    std::printf(
      "engine_start_v1 {\"case\":\"%s\",\"fdatasync\":\"%s\"",
      shape.name,
      seastar::engine().have_aio_fdatasync() ? "aio" : "thread");
    auto number = [](const char* name, std::uint64_t n) {
        std::printf(",\"%s\":%" PRIu64, name, n);
    };
    auto flag = [](const char* name, bool set) {
        std::printf(",\"%s\":%s", name, set ? "true" : "false");
    };
    number("sealed", shape.sealed);
    number("unsealed", shape.unsealed);
    number("unlisted", shape.unlisted);
    number("listed", shape.listed());
    number("requests_per_segment", requests_per_segment);
    number("request_bytes", appends::child_wire(100, 1).size());
    // Every segment is in one bucket directory.
    flag("one_bucket", true);
    number("catalog_bound", maximum_recovery_catalog);
    number("writable_bound", config.table.maximum_writable);
    number("deleting_bound", config.table.maximum_deleting);
    number("elapsed_ns", value.elapsed);
    number("reactor_cpu_ns", value.cpu);
    number("allocations", value.allocations);
    number("tasks", value.tasks);
    number("runtime_operations", value.runtime_operations);
    number("reactor_flushes", value.io.flushes);
    number("reactor_writes", value.io.writes);
    number("reactor_write_bytes", value.io.write_bytes);
    number("reactor_reads", value.io.reads);
    number("reactor_read_bytes", value.io.read_bytes);
    std::printf(
      ",\"start\":{\"state\":\"%s\",\"restarted\":%s",
      state_name(began.state),
      began.restarted ? "true" : "false");
    number("recovered", began.recovered);
    number("unadopted", began.unadopted);
    number("resumed", began.resumed);
    number("marked_retained", began.marked_retained);
    number("gone", began.gone);
    number("owed", began.owed);
    number("refused", began.refused);
    number("sealing", began.sealing);
    number("unresolved", began.unresolved);
    number("wal_files", began.wal_files);
    std::printf("},\"store\":{");
    constexpr std::array<const char*, measured::file_kinds> kinds{
      "wal", "data", "other"};
    for (std::size_t k = 0; k < kinds.size(); ++k)
        std::printf(
          "%s\"%s\":{\"files\":%" PRIu64 ",\"bytes\":%" PRIu64 "}",
          k ? "," : "",
          kinds[k],
          held[k].files,
          held[k].bytes);
    std::printf("}");
    flag("timing_only", timing_only);
#ifdef NDEBUG
    flag("ndebug", true);
#else
    flag("ndebug", false);
#endif
    flag(
      "libcxx_hardening_none",
      _LIBCPP_HARDENING_MODE == _LIBCPP_HARDENING_MODE_NONE);
    flag("complete", true);
    if constexpr (!timing_only) {
        print_kind("wal", files.sample[file_kind::wal]);
        print_kind("data", files.sample[file_kind::data]);
        print_kind("other", files.sample[file_kind::other]);
        number("directory_opens", files.sample.directory_opens);
        number("directory_syncs", files.sample.directory_syncs);
        number("listings", files.sample.listings);
        number("stats", files.sample.stats);
        number("renames", files.sample.renames);
        number("directory_removes", files.sample.directory_removes);
        number("crc_bulk_calls", value.work.crc_bulk_calls);
        number("crc_bulk_bytes", value.work.crc_bulk_bytes);
        number("digest_calls", value.work.digest_calls);
        number("digest_bytes", value.work.digest_bytes);
    }
    std::printf(",\"affinity_cpus\":[");
    bool first = true;
    for (auto cpu : affinity) {
        std::printf("%s%u", first ? "" : ",", cpu);
        first = false;
    }
    std::puts("]}");
}

// One case: the store written and stopped, then one start of a new engine
// on it, timed.
seastar::future<> run_in(
  const case_shape& shape,
  resource::resource_manager& manager,
  runtime::production::timer& timer,
  const std::filesystem::path& directory) {
    const auto affinity = seastar::get_current_cpuset();
    const auto root = directory / "store";
    co_await bench::with_bench_store(
      manager,
      timer,
      {root, {}},
      false,
      bench::configuration(writable_bound),
      [&](bench::bench_store& store) -> seastar::future<> {
          co_await write_store(store, shape);
          const auto& spec = store.devices.front();
          const auto control
            = take(take(local_paths::make(spec.root)).control(0)).value();
          const std::array groups{
            seastar::current_scheduling_group(),
            store.appends.scheduling_group(),
            store.reserve.scheduling_group()};
          for (const auto& group : groups)
              co_await register_io_class(group, control);
          const auto held = on_device(root);
          const auto listed = catalog(shape);

          // A start is a new process: nothing the store's writing verified
          // or counted carries into the measured one.
          measured::file_system files{!timing_only, 0};
          bench::bench_store restarted{
            files,
            store.owner,
            store.devices,
            store.appends,
            store.reserve,
            store.timer,
            store.abort,
            store.config};
          measurement result;
          std::optional<start_report> began;
          runtime::first_failure execution;
          try {
              {
                  measure_scope interval{files, result};
                  began.emplace(co_await bench::start(restarted, listed));
                  require(
                    files.sample.active() == 0,
                    "timing stopped with native writes or flushes in flight");
              }
              verify(*began, shape, *restarted.engine);
              // The head is closed with a successor, which is durable.
              require(
                result.io.flushes != 0 && result.io.writes != 0,
                "a start made nothing durable on the device");
          } catch (...) {
              execution.observe(std::current_exception());
          }
          try {
              co_await bench::stop(restarted);
          } catch (...) {
              execution.observe(std::current_exception());
          }
          take(execution.outcome());
          require(
            seastar::get_current_cpuset() == affinity,
            "benchmark CPU affinity changed during execution");
          print_measurement(
            shape, store.config, result, *began, held, files, affinity);
      });
}

struct engine_start_bench {
    engine_start_bench() {
        static const bool initialized = [] {
            codec::testing::report_profile();
            return true;
        }();
        static_cast<void>(initialized);
    }

    seastar::future<std::size_t> run_case(case_shape selected) {
        const auto config = bench::configuration(writable_bound);
        require(
          top + config.table.maximum_writable + config.table.maximum_deleting
            == maximum_recovery_catalog,
          "the largest case no longer lists what one catalog holds beside "
          "the writable and deleting bounds");
        require(
          selected.listed() <= top && selected.unlisted <= top
            && selected.unsealed <= writable_bound,
          "engine start case exceeds its bound");
        co_await bench::with_bench_resources(
          [&](
            resource::resource_manager& manager,
            runtime::production::timer& timer) -> seastar::future<> {
              co_await seastar::tmp_dir::do_with(
                runtime::testing::test_directory_template(),
                seastar::coroutine::lambda(
                  [&](seastar::tmp_dir& directory) -> seastar::future<> {
                      co_await run_in(
                        selected, manager, timer, directory.get_path());
                  }));
          });
        co_return 1;
    }
};

struct engine_start : engine_start_bench {};

// Each name carries the case's dimensions: sealed segments the catalog
// lists, segments left writable (all listed), and sealed segments retained
// and not listed.
#define ENGINE_START_CASE(Name, ...)                                           \
    PERF_TEST_F(engine_start, Name) {                                          \
        return run_case(case_shape{.name = #Name __VA_OPT__(, ) __VA_ARGS__}); \
    }
// A store that never took an append: the start's fixed cost.
ENGINE_START_CASE(no_segments)
ENGINE_START_CASE(sealed_8, .sealed = 8)
ENGINE_START_CASE(sealed_32, .sealed = 32)
// The most sealed segments one catalog lists.
ENGINE_START_CASE(sealed_102, .sealed = 102)
// As many listed, four of them left writable at the stop.
ENGINE_START_CASE(sealed_98_unsealed_4, .sealed = 98, .unsealed = 4)
// The stores of sealed_8 and sealed_102, started with nothing listed: what a
// store retains and its catalog does not list is read by no start.
ENGINE_START_CASE(unlisted_8, .unlisted = 8)
ENGINE_START_CASE(unlisted_102, .unlisted = 102)
#undef ENGINE_START_CASE
} // namespace
} // namespace kwaque::storage::testing::engine_start_bench_support
