#include "src/codec/tests/allocation_observer.h"
#include "src/codec/tests/benchmark_buffer.h"
#include "src/codec/tests/memory_qualification_support.h"
#include "src/model/batch_builder.h"
#include "src/model/record_codec.h"
#include "src/resource/resource_registry.h"
#include "src/runtime/testing/test_directory.h"
#include "src/storage/tests/storage_large_fixture.h"
#include "src/storage/tests/wal_append_contract.h"
#include "src/storage/tests/wal_bench_child.h"
#include "src/storage/tests/wal_bench_file.h"

#include <seastar/core/memory.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/later.hh>
#include <seastar/util/tmp_file.hh>

#include <bit>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <fstream>

namespace kwaque::storage::testing {
namespace {
using store_contract::require;
using store_contract::take;
#if defined(KWAQUE_WAL_TIMING_ONLY)
constexpr bool timing_only = true;
#else
constexpr bool timing_only = false;
#endif

// The allocator's threshold is inclusive. Exactly 128 KiB is permitted.
static_assert(std::has_single_bit(maximum_contiguous_allocation_bytes));
constexpr auto allocation_warning_threshold
  = maximum_contiguous_allocation_bytes + 1;

void check_rotation_allocation_bound(
  const seastar::memory::statistics& before,
  const seastar::memory::statistics& after) {
    require(
      after.mallocs() > before.mallocs(),
      "rotation allocation interval recorded no native work");
    require(
      after.large_allocations() == before.large_allocations(),
      "rotation exceeded contiguous allocation ceiling");
    require(
      after.failed_allocations() == before.failed_allocations(),
      "rotation encountered a native allocation failure");
    require(
      after.foreign_mallocs() == before.foreign_mallocs()
        && after.fallback_allocations() == before.fallback_allocations(),
      "rotation escaped native allocation accounting");
    require(
      seastar::memory::get_large_allocation_warning_threshold()
        == allocation_warning_threshold,
      "rotation replaced its allocation threshold");
}

struct native_driver final {
    template<typename T>
    seastar::future<T> lifecycle(seastar::future<T> operation) const {
        return operation;
    }
};

// Every pair starts with the same checked child and independent target, and
// ends at exact native write completion or one successful WAL flush. The
// codec/runtime control retains validation, layout, immutable encoding and the
// same file owner, omitting only the writer's admission/descriptor machinery.
// These are single-group costs; no segment barrier or producer ACK is measured.
struct wal_bench {
    wal_bench() { wal_bench_support::initialize(); }
    template<
      std::size_t Size,
      bool Fragmented,
      bool Noop,
      bool Barrier,
      bool Reference = false,
      bool Rotation = false,
      bool Preencoded = false>
    seastar::future<std::size_t> measure() {
        const auto resource_config
          = resource::resource_config::from_total_memory(
              byte_count{seastar::memory::stats().total_memory()})
              .value();
        resource::resource_registry registry;
        co_await registry.start(resource_config);
        resource::resource_manager manager{registry.handles()};
        std::exception_ptr failure;
        try {
            co_await manager.start();
            co_await seastar::tmp_dir::do_with(
              runtime::testing::test_directory_template(),
              seastar::coroutine::lambda(
                [&](seastar::tmp_dir& directory) -> seastar::future<> {
                    wal_bench_support::file_system files{!timing_only || Noop};
                    const auto root = take(
                      runtime::file_path::make(
                        (directory.get_path() / "store").string()));
                    take(co_await files.create_directories(root));
                    const auto status = co_await seastar::file_stat(
                      root.value(), seastar::follow_symlink::no);
                    const auto spec = store_contract::specification(
                      root, {status.device_id, status.inode_number});
                    const std::array specs{spec};
                    store_contract::ownership_input owner{specs};
                    workload_budget budget{
                      manager.acquire_workload(
                        resource::workload_class::metadata),
                      {.tasks = 128,
                       .bytes = byte_count{96U << 20U},
                       .handles = 32},
                      charge};
                    auto config = wal_writer_contract::configuration();
                    config.capacity_bytes = byte_count{128U << 20U};
                    config.children.working_bytes = byte_count{16U << 20U};
                    co_await wal_append_contract::with_writer(
                      files,
                      owner,
                      spec,
                      budget,
                      native_driver{},
                      config,
                      [&](auto& writer, auto& work) -> seastar::future<> {
                          auto child = co_await wal_bench_support::child(
                            Size, work);
                          if constexpr (Fragmented) {
                              auto fragmented
                                = co_await codec::bench::copy_layout(
                                  std::move(child).release_bytes(),
                                  257,
                                  work,
                                  byte_count{32U << 20U},
                                  1020);
                              child = (co_await validate_encoded_assigned_batch(
                                         std::move(fragmented),
                                         batch_expected(),
                                         {byte_count{32U << 20U},
                                          byte_count{1U << 20U},
                                          charge},
                                         work))
                                        .value();
                          }
                          const auto child_size = child.bytes().size();
                          const auto child_fragments
                            = child.bytes().fragment_count();
                          auto backing = take(
                            budget.try_reserve_buffer(child.bytes()));
                          const auto head = *writer.prepared_head();
                          const auto e = wal_writer_contract::child_context(
                            spec.owner.cluster());
                          const wal_prepare_expectation context{
                            wal_write_context::make(
                              head.incarnation,
                              alignment(8192),
                              runtime::file_position{8192})
                              .value(),
                            e.target,
                            e.target_data_start,
                            e.routing_epoch,
                            e.batch,
                            e.profile,
                            e.target_profile};
                          const auto layout
                            = preflight_wal_prepare(
                                child,
                                context,
                                work.policy(),
                                {runtime::maximum_file_io_bytes,
                                 runtime::maximum_file_io_bytes})
                                .value();
                          const auto encoded_bytes = layout.encoded_bytes();
                          // Independently owned comparison output is outside
                          // the timed/observed interval and remains separately
                          // charged.
                          auto alias = (co_await child.share(
                                          {byte_count{32U << 20U},
                                           byte_count{1U << 20U},
                                           charge},
                                          work))
                                         .value();
                          auto expected = (co_await encode_wal_prepare(
                                             std::move(alias),
                                             context,
                                             work,
                                             byte_count{32U << 20U},
                                             charge))
                                            .value();
                          auto expected_charge = take(
                            budget.try_reserve_buffer(expected));
                          if (
                            const auto* export_directory = std::getenv(
                              "KWAQUE_WAL_FIXTURE_DIRECTORY")) {
                              const auto name = std::string{export_directory}
                                                + "/wal-" + std::to_string(Size)
                                                + "-"
                                                + std::to_string(Fragmented)
                                                + ".bin";
                              auto output = take(
                                co_await files.open(
                                  take(runtime::file_path::make(name)),
                                  {.access = runtime::file_access::read_write,
                                   .create = true,
                                   .truncate = true,
                                   .close_policy
                                   = runtime::file_close_policy::checked}));
                              runtime::first_failure exported;
                              try {
                                  take(
                                    co_await output.write(
                                      {}, expected.share()));
                                  exported.observe(co_await output.flush());
                                  std::ofstream layout_file{name + ".layout"};
                                  for (const auto fragment : expected)
                                      layout_file << fragment.size() << '\n';
                                  layout_file.close();
                                  require(
                                    bool(layout_file),
                                    "cannot export WAL fragment layout");
                              } catch (...) {
                                  exported.observe(std::current_exception());
                              }
                              exported.observe(co_await output.close());
                              take(exported.outcome());
                          }
                          std::optional<bytes::fragmented_buffer> preencoded;
                          std::optional<workload_reservation> preencoded_charge;
                          if constexpr (Preencoded) {
                              preencoded_charge.emplace(
                                take(budget.try_reserve_buffer(expected)));
                              preencoded.emplace(expected.share());
                          }
                          auto admitted = take(
                            admitted_wal_batch::make(
                              std::move(child), std::move(backing), charge));
                          auto group = take(wal_group::make(budget, 1));
                          std::optional<admitted_wal_batch::contents> reference;
                          if constexpr (Reference)
                              reference.emplace(std::move(admitted).release());
                          else
                              take(group.append(std::move(admitted), e));
                          std::optional<runtime::file> direct;
                          std::optional<runtime::file::metadata_reservation>
                            metadata;
                          const auto path = take(take(local_paths::make(root))
                                                   .wal(0, head.incarnation));
                          runtime::first_failure execution_failure;
                          const bool foreground_probe
                            = std::getenv("KWAQUE_WAL_FOREGROUND_PROBE")
                              != nullptr;
                          const bool preallocate
                            = std::getenv("KWAQUE_WAL_PREALLOCATE") != nullptr;
                          require(
                            !preallocate || (!Noop && !Rotation),
                            "preallocation requires a disk append case");
                          const auto extent = ((8192 + encoded_bytes.value()
                                                + (2U << 20U) - 1)
                                               / (2U << 20U))
                                              * (2U << 20U);
                          bool stop_foreground = false;
                          std::uint64_t foreground_turns = 0;
                          auto foreground_work = [&] -> seastar::future<> {
                              while (!stop_foreground) {
                                  co_await seastar::yield();
                                  if (!stop_foreground) ++foreground_turns;
                              }
                          };
                          auto foreground = foreground_probe
                                              ? foreground_work()
                                              : seastar::make_ready_future<>();
                          try {
                              if constexpr (Reference) {
                                  take(co_await writer.close());
                                  direct.emplace(take(
                                    co_await files.open(
                                      path,
                                      {.access
                                       = runtime::file_access::read_write,
                                       .close_policy = runtime::
                                         file_close_policy::checked})));
                                  metadata.emplace(
                                    take(direct->try_reserve_metadata()));
                              }

                              std::optional<wal_captured_boundary> cut;
                              if constexpr (Rotation) {
                                  auto accepted = take(
                                    co_await writer.submit(
                                      std::move(group), work));
                                  cut.emplace(accepted.boundary);
                                  auto done = co_await std::move(
                                    accepted.written);
                                  take(done.failure.outcome());
                              }
                              if (preallocate)
                                  co_await files.prepare_extent(extent);
                              foreground_turns = 0;
                              const auto retained = budget.snapshot().bytes;
                              const auto tasks = seastar::engine()
                                                   .get_sched_stats()
                                                   .tasks_processed;
                              const auto allocations
                                = seastar::memory::stats().mallocs();
                              const auto metadata_operations
                                = files.statistics().accepted;
                              files.sample.noop = Noop;
                              files.sample.measuring = true;
#if defined(KWAQUE_WAL_TIMING_ONLY)
                              constexpr bool observe = false;
#else
                              const bool observe
                                = std::getenv("KWAQUE_WAL_OBSERVE_ALLOCATIONS")
                                  != nullptr;
#endif
                              const bool exact_observation = observe
                                                             && !Rotation;
                              std::optional<
                                seastar::memory::
                                  scoped_large_allocation_warning_threshold>
                                allocation_threshold;
                              std::optional<seastar::memory::statistics>
                                rotation_memory_before;
                              if constexpr (Rotation) {
                                  if (observe) {
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
                                      throw std::runtime_error(
                                        "rotation allocation qualification "
                                        "requires the native allocator");
#else
                                      // Directory creation may format an
                                      // expected ENOENT through libc, outside
                                      // linker wraps. Native counters cover
                                      // that allocation too; this check cannot
                                      // supply a byte peak.
                                      allocation_threshold.emplace(
                                        allocation_warning_threshold);
                                      rotation_memory_before.emplace(
                                        seastar::memory::stats());
#endif
                                  }
                              }
#if !defined(KWAQUE_WAL_TIMING_ONLY)
                              if (exact_observation)
                                  codec::testing::
                                    begin_allocation_observation();
#endif
                              bool measuring = true;
                              auto finish_observation = seastar::defer(
                                [&] noexcept {
                                    if (measuring) {
                                        perf_tests::stop_measuring_time();
                                        files.sample.measuring = false;
#if !defined(KWAQUE_WAL_TIMING_ONLY)
                                        if (exact_observation)
                                            static_cast<void>(
                                              codec::testing::
                                                end_allocation_observation());
#endif
                                    }
                                });
                              perf_tests::start_measuring_time();
                              if constexpr (Rotation) {
                                  take(
                                    co_await writer.rotate(
                                      *cut, encoded_bytes, work));
                              } else if constexpr (Reference) {
                                  bytes::fragmented_buffer output;
                                  if constexpr (Preencoded)
                                      output = std::move(*preencoded);
                                  else {
                                      (co_await reference->batch.validate(
                                         e.batch,
                                         {config.children.working_bytes,
                                          config.children.metadata_bytes,
                                          charge},
                                         work))
                                        .value();
                                      const auto checked
                                        = preflight_wal_prepare(
                                            reference->batch,
                                            context,
                                            work.policy(),
                                            {runtime::maximum_file_io_bytes,
                                             runtime::maximum_file_io_bytes})
                                            .value();
                                      require(
                                        checked.encoded_bytes()
                                          == encoded_bytes,
                                        "reference preflight changed extent");
                                      output = (co_await encode_wal_prepare(
                                                  std::move(reference->batch),
                                                  context,
                                                  work,
                                                  byte_count{32U << 20U},
                                                  charge))
                                                 .value();
                                  }
                                  const auto written = take(
                                    co_await direct->write(
                                      runtime::file_position{8192},
                                      std::move(output)));
                                  require(
                                    written == encoded_bytes,
                                    "runtime control returned short logical "
                                    "write");
                                  if constexpr (Barrier)
                                      take(co_await direct->flush(*metadata));
                              } else {
                                  auto accepted = take(
                                    co_await writer.submit(
                                      std::move(group), work));
                                  auto written = co_await std::move(
                                    accepted.written);
                                  take(written.failure.outcome());
                                  require(
                                    written.written == encoded_bytes,
                                    "writer returned short complete group");
                                  if constexpr (Barrier) {
                                      auto result = co_await writer.barrier(
                                        accepted.boundary);
                                      take(result.failure.outcome());
                                      require(
                                        result.receipt
                                          && result.receipt->boundary().cursor()
                                               == accepted.boundary.cursor(),
                                        "benchmark barrier changed its "
                                        "capture");
                                  }
                              }
                              perf_tests::stop_measuring_time();
                              stop_foreground = true;
                              files.sample.measuring = false;
#if defined(KWAQUE_WAL_TIMING_ONLY)
                              const codec::testing::allocation_observation
                                memory{};
#else
                              const auto memory
                                = exact_observation
                                    ? codec::testing::
                                        end_allocation_observation()
                                    : codec::testing::allocation_observation{};
#endif
                              measuring = false;
                              if constexpr (Rotation) {
                                  if (observe) {
                                      const auto after
                                        = seastar::memory::stats();
                                      check_rotation_allocation_bound(
                                        *rotation_memory_before, after);
                                      allocation_threshold.reset();
                                  }
                              }
                              if (exact_observation) {
                                  if (!memory.observed || !memory.complete) {
                                      std::printf(
                                        "wal_memory owner=%s storage=%s "
                                        "boundary=%s observed=%u complete=%u "
                                        "allocations=%" PRIu64
                                        " native_allocations=%" PRIu64
                                        " frees=%" PRIu64
                                        " native_frees=%" PRIu64 "\n",
                                        Preencoded  ? "runtime_preencoded"
                                        : Reference ? "codec_runtime"
                                                    : "writer",
                                        Noop ? "noop" : "disk",
                                        Rotation  ? "rotation"
                                        : Barrier ? "wal_barrier"
                                                  : "written",
                                        unsigned(memory.observed),
                                        unsigned(memory.complete),
                                        memory.allocations,
                                        memory.native_allocations,
                                        memory.frees,
                                        memory.native_frees);
                                      std::fflush(stdout);
                                  }
                                  require(
                                    memory.observed && memory.complete,
                                    "native allocation observation is "
                                    "incomplete");
                                  require(
                                    memory.largest_allocation
                                      <= maximum_contiguous_allocation_bytes,
                                    "writer interval exceeded contiguous "
                                    "allocation ceiling");
                              }
                              const auto elapsed_tasks = seastar::engine()
                                                           .get_sched_stats()
                                                           .tasks_processed
                                                         - tasks;
                              const auto mallocs
                                = seastar::memory::stats().mallocs()
                                  - allocations;
                              const auto sample = files.sample;
                              if constexpr (
                                !Rotation && (!timing_only || Noop)) {
                                  require(
                                    sample.bytes == encoded_bytes.value()
                                      && sample.depth >= 1
                                      && sample.depth
                                           <= runtime::file_io_limits{}
                                                .write_concurrency
                                      && sample.maximum
                                           <= maximum_contiguous_allocation_bytes
                                      && sample.flushes == (Barrier ? 1U : 0U),
                                    "native work differs from measured "
                                    "boundary");
                              }
                              static bool reported = false;
                              if (!reported) {
                                  std::printf(
                                    "wal_conditions foreground_probe=%d "
                                    "preallocated=%d setup_extent=%" PRIu64
                                    "\n",
                                    foreground_probe,
                                    preallocate,
                                    preallocate ? extent : 0);
                                  if constexpr (timing_only && !Noop) {
                                      std::printf(
                                        "wal_cost owner=%s storage=disk "
                                        "boundary=%s encoded_bytes=%" PRIu64
                                        " allocations=%" PRIu64
                                        " tasks=%" PRIu64
                                        " native_observed=0\n",
                                        Preencoded  ? "runtime_preencoded"
                                        : Reference ? "codec_runtime"
                                                    : "writer",
                                        Rotation  ? "rotation"
                                        : Barrier ? "wal_barrier"
                                                  : "written",
                                        encoded_bytes.value(),
                                        mallocs,
                                        elapsed_tasks);
                                  } else
                                      std::printf(
                                        "wal_cost owner=%s storage=%s "
                                        "boundary=%s "
                                        "child_bytes=%" PRIu64
                                        " child_fragments=%zu "
                                        "encoded_fragments=%zu "
                                        "encoded_bytes=%" PRIu64
                                        " native_calls=%" PRIu64
                                        " native_bytes=%" PRIu64
                                        " native_min=%" PRIu64
                                        " native_max=%" PRIu64
                                        " native_depth=%" PRIu64
                                        " flushes=%" PRIu64
                                        " allocations=%" PRIu64
                                        " tasks=%" PRIu64
                                        " retained_admission=%" PRIu64
                                        " staging_copy_upper_bound=%" PRIu64
                                        "\n",
                                        Preencoded  ? "runtime_preencoded"
                                        : Reference ? "codec_runtime"
                                                    : "writer",
                                        Noop ? "noop" : "disk",
                                        Rotation  ? "rotation"
                                        : Barrier ? "wal_barrier"
                                                  : "written",
                                        child_size.value(),
                                        child_fragments,
                                        expected.fragment_count(),
                                        encoded_bytes.value(),
                                        sample.calls,
                                        sample.bytes,
                                        sample.calls ? sample.minimum : 0,
                                        sample.maximum,
                                        sample.depth,
                                        sample.flushes,
                                        mallocs,
                                        elapsed_tasks,
                                        retained,
                                        sample.bytes);
                                  std::printf(
                                    "wal_cooperation foreground_turns=%" PRIu64
                                    " metadata_operations=%" PRIu64 "\n",
                                    foreground_turns,
                                    files.statistics().accepted
                                      - metadata_operations);
                                  if (observe && Rotation)
                                      std::printf(
                                        "wal_memory "
                                        "coverage=native_count_and_ceiling "
                                        "allocation_ceiling=%zu "
                                        "oversized_allocations=0 "
                                        "peak_observed=0 "
                                        "critical_peak_observed=0\n",
                                        maximum_contiguous_allocation_bytes);
                                  if (exact_observation)
                                      std::printf(
                                        "wal_memory complete=%u "
                                        "peak_upper_bound=%" PRIu64
                                        " largest_allocation=%" PRIu64
                                        " critical_peak_upper_bound=%" PRIu64
                                        "\n",
                                        unsigned(memory.complete),
                                        memory.peak_upper_bound,
                                        memory.largest_allocation,
                                        memory.critical_peak_upper_bound);
                                  reported = true;
                              }
                          } catch (...) {
                              execution_failure.observe(
                                std::current_exception());
                          }
                          stop_foreground = true;
                          co_await std::move(foreground);
                          if (preallocate) {
                              try {
                                  co_await files.finish_extent(
                                    8192 + encoded_bytes.value());
                              } catch (...) {
                                  execution_failure.observe(
                                    std::current_exception());
                              }
                          }
                          metadata.reset();
                          if (direct) {
                              try {
                                  execution_failure.observe(
                                    co_await direct->close());
                              } catch (...) {
                                  execution_failure.observe(
                                    std::current_exception());
                              }
                              direct.reset();
                          }
                          take(execution_failure.outcome());
                          if constexpr (!Noop) {
                              auto file = take(
                                co_await files.open(
                                  path,
                                  {.close_policy
                                   = runtime::file_close_policy::checked}));
                              runtime::first_failure check;
                              try {
                                  for (std::uint64_t offset = 0;
                                       offset < encoded_bytes.value();) {
                                      const auto length = byte_count{
                                        std::min<std::uint64_t>(
                                          65536,
                                          encoded_bytes.value() - offset)};
                                      auto actual = take(
                                        co_await file.read(
                                          runtime::file_position{8192 + offset},
                                          length));
                                      auto selected
                                        = expected
                                            .share(byte_count{offset}, length)
                                            .value();
                                      require(
                                        co_await codec::bench::buffers_equal(
                                          actual.data(), selected, work),
                                        "benchmark wrote different PREPARE "
                                        "bytes");
                                      offset += length.value();
                                  }
                              } catch (...) {
                                  check.observe(std::current_exception());
                              }
                              check.observe(co_await file.close());
                              take(check.outcome());
                          }
                      });
                    require(
                      budget.snapshot().tasks == 0
                        && budget.snapshot().bytes == 0
                        && budget.snapshot().handles == 0,
                      "benchmark retained storage admission");
                }));
        } catch (...) {
            failure = std::current_exception();
        }
        co_await manager.stop();
        co_await registry.stop();
        if (failure) std::rethrow_exception(failure);
        co_return 1;
    }
};
} // namespace

#define WAL_COST_CASE(Name, Size, Fragmented, Noop, Barrier)                   \
    PERF_TEST_F(wal_bench, writer_##Name) {                                    \
        return (measure<Size, Fragmented, Noop, Barrier>());                   \
    }                                                                          \
    PERF_TEST_F(wal_bench, codec_runtime_##Name) {                             \
        return (measure<Size, Fragmented, Noop, Barrier, true>());             \
    }
WAL_COST_CASE(tiny_noop_written, 0, false, true, false)
WAL_COST_CASE(tiny_disk_written, 0, false, false, false)
WAL_COST_CASE(tiny_disk_barrier, 0, false, false, true)
WAL_COST_CASE(k128_noop_written, 131072, false, true, false)
WAL_COST_CASE(k128_disk_written, 131072, false, false, false)
WAL_COST_CASE(k128_disk_barrier, 131072, false, false, true)
WAL_COST_CASE(m4_noop_written, 4U << 20U, false, true, false)
WAL_COST_CASE(m4_disk_written, 4U << 20U, false, false, false)
WAL_COST_CASE(m4_disk_barrier, 4U << 20U, false, false, true)
WAL_COST_CASE(maximum_noop_written, 8U << 20U, false, true, false)
WAL_COST_CASE(maximum_disk_written, 8U << 20U, false, false, false)
WAL_COST_CASE(maximum_disk_barrier, 8U << 20U, false, false, true)
WAL_COST_CASE(fragmented_noop_written, 131072, true, true, false)
WAL_COST_CASE(fragmented_disk_barrier, 131072, true, false, true)
#undef WAL_COST_CASE
PERF_TEST_F(wal_bench, writer_disk_rotation) {
    return (measure<131072, false, false, true, false, true>());
}

#define WAL_PREENCODED(Name, Size, Fragmented, Noop)                           \
    PERF_TEST_F(wal_bench, runtime_preencoded_##Name) {                        \
        return (measure<Size, Fragmented, Noop, true, true, false, true>());   \
    }
WAL_PREENCODED(tiny_disk_barrier, 0, false, false)
WAL_PREENCODED(k128_disk_barrier, 131072, false, false)
WAL_PREENCODED(m4_disk_barrier, 4U << 20U, false, false)
WAL_PREENCODED(maximum_disk_barrier, 8U << 20U, false, false)
WAL_PREENCODED(fragmented_disk_barrier, 131072, true, false)
WAL_PREENCODED(m4_noop_barrier, 4U << 20U, false, true)
#undef WAL_PREENCODED
} // namespace kwaque::storage::testing
