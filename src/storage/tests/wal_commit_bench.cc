#include "src/codec/sha256.h"
#include "src/resource/resource_registry.h"
#include "src/runtime/production/clocks.h"
#include "src/runtime/production/timer.h"
#include "src/runtime/testing/test_directory.h"
#include "src/storage/tests/wal_bench_child.h"
#include "src/storage/tests/wal_bench_file.h"
#include "src/storage/wal_group_commit.h"

#include <seastar/core/posix.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread_cputime_clock.hh>
#include <seastar/util/later.hh>
#include <seastar/util/tmp_file.hh>

#include <cinttypes>
#include <cstdio>
#include <fstream>
#include <span>
#include <string_view>

namespace kwaque::storage::testing {
namespace {
using store_contract::require;
using store_contract::take;
using clock = runtime::production::monotonic_clock;
using wal_bench_support::measurement_time;
#if defined(KWAQUE_WAL_TIMING_ONLY)
constexpr bool timing_only = true;
#else
constexpr bool timing_only = false;
#endif

enum class arrival_profile {
    fixed,
    burst,
    isolated,
    paced,
    retained,
    rotation,
    timeout
};
constexpr const char* profile_name(arrival_profile profile) {
    switch (profile) {
    case arrival_profile::fixed:
        return "fixed";
    case arrival_profile::burst:
        return "burst";
    case arrival_profile::isolated:
        return "isolated";
    case arrival_profile::paced:
        return "paced";
    case arrival_profile::retained:
        return "retained";
    case arrival_profile::rotation:
        return "rotation";
    case arrival_profile::timeout:
        return "timeout";
    }
    return "invalid";
}
struct request_sample final {
    std::uint64_t planned{0}, started{0}, admitted{0}, terminal{0};
    std::uint64_t begin{0}, end{0}, covering_end{0};
    std::uint32_t file{0};
    bool accepted{false}, timeout{false};
};

// The final cohort observer has completed. Verify every member's immutable
// result synchronously before releasing the retained window's admission.
void finish_aggregate(
  std::span<std::optional<wal_commit_ticket>> tickets,
  std::span<request_sample> requests,
  const wal_captured_boundary& cut) {
    require(tickets.size() == requests.size(), "aggregate window size changed");
    for (std::size_t i = 0; i != tickets.size(); ++i) {
        auto& ticket = *tickets[i];
        auto ready = take(ticket.observe());
        require(ready.available(), "cohort left a group unsettled");
        auto result = ready.get();
        take(result.failure().outcome());
        require(
          result.boundary() == ticket.boundary() && result.receipt()
            && result.receipt()->boundary() == cut,
          "aggregate cohort changed group certification");
        requests[i].covering_end = cut.cursor().position().value();
        tickets[i].reset();
    }
}
struct cohort_timer final {
    runtime::production::timer& timer;
    bool instrumented;
    std::uint64_t wait_ns{0};
    seastar::future<runtime::result<void>> sleep_until(
      runtime::monotonic_time deadline, seastar::abort_source& abort) {
        const auto before = instrumented ? measurement_time() : 0;
        auto done = seastar::defer([&] noexcept {
            if (instrumented) wait_ns += measurement_time() - before;
        });
        co_return co_await timer.sleep_until(deadline, abort);
    }
    void request_abort() noexcept { timer.request_abort(); }
    seastar::future<runtime::result<void>> stop() { return timer.stop(); }
};
struct commit_driver final {
    template<typename T>
    seastar::future<T> lifecycle(seastar::future<T> future) const {
        return future;
    }
};
std::array<char, 65> hex_digest(codec::sha256_digest digest) {
    std::array<char, 65> result{};
    constexpr char digits[] = "0123456789abcdef";
    for (std::size_t i = 0; i != digest.size(); ++i) {
        result[2 * i] = digits[digest[i] >> 4U];
        result[2 * i + 1] = digits[digest[i] & 15U];
    }
    return result;
}

struct wal_commit_bench {
    wal_commit_bench() { wal_bench_support::initialize(); }

    template<
      bool Coordinated,
      std::size_t Size,
      bool Fragmented,
      std::uint64_t Delay,
      arrival_profile Profile,
      std::uint32_t Target = 4,
      std::uint64_t TargetBytes = runtime::maximum_file_io_bytes.value(),
      std::uint32_t Capacity = 8>
    seastar::future<std::size_t> measure() {
        constexpr std::size_t count = Size >= (8U << 20U)                 ? 2
                                      : Size >= (4U << 20U)               ? 8
                                      : Profile == arrival_profile::paced ? 128
                                                                          : 32;
        constexpr std::size_t window = std::min<std::size_t>(4, count);
        constexpr bool fixed = Profile == arrival_profile::fixed
                               || Profile == arrival_profile::rotation;
        static_assert(Coordinated || fixed);
        const bool aggregate = std::getenv("KWAQUE_WAL_AGGREGATE_TIMING")
                               != nullptr;
        require(
          !aggregate || Profile == arrival_profile::fixed,
          "aggregate timing requires a fixed single-file workload");
        const auto affinity = seastar::get_current_cpuset();
        const char* comparison = std::getenv("KWAQUE_WAL_COMPARISON_ID");
        if (!comparison) comparison = "";
        const std::string_view comparison_id{comparison};
        require(
          comparison_id.empty()
            || (comparison_id.size() == 64 && comparison_id.find_first_not_of("0123456789abcdef") == std::string_view::npos),
          "comparison ID must be a SHA-256 context digest");
        const bool preallocate = std::getenv("KWAQUE_WAL_PREALLOCATE")
                                 != nullptr;
        require(
          !preallocate || Profile != arrival_profile::rotation,
          "rotation uses growing files");
        const bool foreground_probe = std::getenv("KWAQUE_WAL_FOREGROUND_PROBE")
                                      != nullptr;
        const auto configuration = resource::resource_config::from_total_memory(
                                     byte_count{
                                       seastar::memory::stats().total_memory()})
                                     .value();
        resource::resource_registry registry;
        co_await registry.start(configuration);
        resource::resource_manager manager{registry.handles()};
        runtime::production::timer timer;
        runtime::first_failure failed;
        try {
            co_await manager.start();
            co_await seastar::tmp_dir::do_with(
              runtime::testing::test_directory_template(),
              seastar::coroutine::lambda(
                [&](seastar::tmp_dir& directory) -> seastar::future<> {
                    wal_bench_support::file_system files{!timing_only};
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
                      {.tasks = 512,
                       .bytes = byte_count{96U << 20U},
                       .handles = 32},
                      charge};
                    auto writer_config = wal_writer_contract::configuration();
                    writer_config.capacity_bytes = byte_count{128U << 20U};
                    writer_config.children.working_bytes = byte_count{
                      16U << 20U};
                    co_await wal_append_contract::with_writer(
                      files,
                      owner,
                      spec,
                      budget,
                      commit_driver{},
                      writer_config,
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
                          const auto child_bytes = child.bytes().size().value();
                          const auto child_fragments
                            = child.bytes().fragment_count();
                          auto backing = take(
                            budget.try_reserve_buffer(child.bytes()));
                          std::optional<admitted_wal_batch> seed{take(
                            admitted_wal_batch::make(
                              std::move(child), std::move(backing), charge))};
                          const auto expected
                            = wal_writer_contract::child_context(
                              spec.owner.cluster());
                          const auto initial_head = *writer.prepared_head();
                          const wal_prepare_expectation initial{
                            wal_write_context::make(
                              initial_head.incarnation,
                              alignment(8192),
                              runtime::file_position{8192})
                              .value(),
                            expected.target,
                            expected.target_data_start,
                            expected.routing_epoch,
                            expected.batch,
                            expected.profile,
                            expected.target_profile};
                          const auto encoded_size
                            = preflight_wal_prepare(
                                seed->batch(),
                                initial,
                                work.policy(),
                                {runtime::maximum_file_io_bytes,
                                 runtime::maximum_file_io_bytes})
                                .value()
                                .encoded_bytes();
                          const auto setup_extent
                            = ((8192 + count * encoded_size.value()
                                + (2U << 20U) - 1)
                               / (2U << 20U))
                              * (2U << 20U);
                          std::array<std::optional<wal_group>, count> offers;
                          std::array<std::optional<workload_reservation>, count>
                            preparation;
                          for (std::size_t i = 0; i != count; ++i) {
                              auto pair = take(
                                co_await prepare_wal_children(
                                  std::move(*seed),
                                  expected,
                                  budget,
                                  writer_config.children,
                                  work));
                              seed.reset();
                              seed.emplace(std::move(pair.segment));
                              preparation[i].emplace(
                                std::move(pair.preparation));
                              offers[i].emplace(
                                take(wal_group::make(budget, 1)));
                              take(
                                offers[i]->append(
                                  std::move(pair.wal), expected));
                          }
                          auto verification_child = std::move(*seed).release();
                          seed.reset();
                          std::unique_ptr<wal_group_commit> groups;
                          wal_group_commit_config cfg;
                          cfg.outstanding_groups = Capacity;
                          cfg.target_members = fixed
                                                 ? static_cast<std::uint32_t>(
                                                     window)
                                                 : Target;
                          cfg.target_bytes = byte_count{TargetBytes};
                          cfg.maximum_wait = runtime::monotonic_duration{
                            fixed ? 3'600'000'000'000ULL : Delay};
                          cohort_timer batching{timer, !timing_only};
                          std::array<request_sample, count> requests{};
                          std::array<std::optional<local_wal_head>, count>
                            heads;
                          // Engaged slots own unconsumed futures. Clear each
                          // slot before awaiting so failure cleanup joins once.
                          std::array<std::optional<seastar::future<>>, count>
                            observing;
                          std::array<std::optional<wal_commit_result>, count>
                            retained;
                          std::array<std::optional<wal_submission>, window>
                            direct;
                          std::array<std::optional<wal_commit_ticket>, window>
                            aggregate_tickets;
                          std::array<bool, window> written_joined{};
                          std::array<seastar::abort_source, count> callers;
                          bool measuring = false, stop_foreground = false;
                          std::uint64_t foreground_turns = 0,
                                        peak_admission
                                        = budget.snapshot().bytes;
                          std::uint64_t elapsed = 0, reactor_cpu = 0,
                                        allocations = 0, tasks = 0,
                                        metadata_operations = 0;
                          codec::testing::allocation_observation memory;
                          bool observe_allocations = false;
#if !defined(KWAQUE_WAL_TIMING_ONLY)
                          observe_allocations
                            = std::getenv("KWAQUE_WAL_OBSERVE_ALLOCATIONS")
                              != nullptr;
#endif
                          require(
                            !observe_allocations
                              || Profile != arrival_profile::rotation,
                            "rotation needs the native count/ceiling "
                            "qualification profile");
                          auto foreground_work = [&] -> seastar::future<> {
                              while (!stop_foreground) {
                                  co_await seastar::yield();
                                  if (!stop_foreground) ++foreground_turns;
                              }
                          };
                          auto foreground = foreground_probe
                                              ? foreground_work()
                                              : seastar::make_ready_future<>();
                          [[maybe_unused]] auto observe_result =
                            [&](
                              std::size_t i,
                              seastar::future<wal_commit_result> pending)
                            -> seastar::future<> {
                              auto result = co_await std::move(pending);
                              requests[i].terminal = measurement_time();
                              take(result.failure().outcome());
                              require(
                                result.receipt().has_value(),
                                "benchmark completed without a WAL receipt");
                              requests[i].covering_end = result.receipt()
                                                           ->boundary()
                                                           .cursor()
                                                           .position()
                                                           .value();
                              if constexpr (
                                Profile == arrival_profile::retained)
                                  retained[i].emplace(std::move(result));
                          };
                          [[maybe_unused]] auto observe_deadline =
                            [&](
                              std::size_t i,
                              seastar::future<
                                runtime::result<wal_commit_result>> interest,
                              seastar::future<wal_commit_result> audit)
                            -> seastar::future<> {
                              auto result = co_await std::move(interest);
                              requests[i].terminal = measurement_time();
                              if (!result) {
                                  require(
                                    result.error().code() == errc::timed_out,
                                    "unexpected benchmark observer error");
                                  requests[i].timeout = true;
                              }
                              auto durable = co_await std::move(audit);
                              take(durable.failure().outcome());
                              require(
                                durable.receipt().has_value(),
                                "detached benchmark work did not drain");
                              requests[i].covering_end = durable.receipt()
                                                           ->boundary()
                                                           .cursor()
                                                           .position()
                                                           .value();
                          };
                          runtime::first_failure execution;
                          try {
                              if constexpr (Coordinated) {
                                  groups = take(
                                    wal_group_commit::make(
                                      writer, budget, cfg));
                                  if constexpr (!fixed)
                                      take(groups->template start<clock>(
                                        batching));
                              }
                              if (preallocate)
                                  co_await files.prepare_extent(setup_extent);
                              const auto before_allocations
                                = seastar::memory::stats().mallocs();
                              const auto before_tasks = seastar::engine()
                                                          .get_sched_stats()
                                                          .tasks_processed;
                              const auto before_metadata
                                = files.statistics().accepted;
                              peak_admission = budget.snapshot().bytes;
                              foreground_turns = 0;
                              files.sample.measuring = true;
                              files.sample.measure_service_time = !timing_only;
#if !defined(KWAQUE_WAL_TIMING_ONLY)
                              if (observe_allocations)
                                  codec::testing::
                                    begin_allocation_observation();
#endif
                              measuring = true;
                              const auto cpu_start
                                = seastar::thread_cputime_clock::now();
                              const auto epoch = measurement_time();
                              const auto arrival_start
                                = std::chrono::steady_clock::now();
                              perf_tests::start_measuring_time();
                              std::uint32_t file_index = 0;
                              for (std::size_t i = 0; i != count; ++i) {
                                  constexpr std::uint64_t spacing
                                    = Profile == arrival_profile::isolated
                                        ? 2'000'000
                                      : Profile == arrival_profile::paced
                                          || Profile
                                               == arrival_profile::retained
                                        ? 50'000
                                        : 0;
                                  if (!aggregate)
                                      requests[i].planned = epoch + i * spacing;
                                  if constexpr (spacing != 0) {
                                      const auto arrival
                                        = arrival_start
                                          + std::chrono::nanoseconds{
                                            static_cast<std::int64_t>(
                                              i * spacing)};
                                      const auto now
                                        = std::chrono::steady_clock::now();
                                      if (arrival > now)
                                          co_await seastar::sleep<
                                            std::chrono::steady_clock>(
                                            arrival - now);
                                  }
                                  if (!aggregate)
                                      requests[i].started = measurement_time();
                                  requests[i].file = file_index;
                                  heads[i] = *writer.prepared_head();
                                  if constexpr (Coordinated) {
                                      auto accepted
                                        = co_await groups
                                            ->template submit<clock>(
                                              writer,
                                              std::move(*offers[i]),
                                              work);
                                      if (!aggregate)
                                          requests[i].admitted
                                            = measurement_time();
                                      if (!accepted) {
                                          require(
                                            !fixed
                                              && accepted.error().code()
                                                   == errc::queue_full,
                                            "unexpected benchmark admission "
                                            "failure");
                                          requests[i].terminal
                                            = requests[i].admitted;
                                          continue;
                                      }
                                      requests[i].accepted = true;
                                      requests[i].end = accepted->boundary()
                                                          .cursor()
                                                          .position()
                                                          .value();
                                      require(
                                        accepted->encoded_bytes()
                                            == encoded_size
                                          && requests[i].end
                                               >= encoded_size.value(),
                                        "coordinator changed the prepared "
                                        "group extent");
                                      requests[i].begin
                                        = requests[i].end
                                          - encoded_size.value();
                                      if (aggregate) {
                                          aggregate_tickets[i % window].emplace(
                                            std::move(*accepted));
                                      } else if constexpr (
                                        Profile == arrival_profile::timeout) {
                                          const auto deadline
                                            = clock::now()
                                                .checked_add(
                                                  runtime::monotonic_duration{
                                                    10'000})
                                                .value();
                                          observing[i].emplace(observe_deadline(
                                            i,
                                            accepted->template observe<clock>(
                                              timer, callers[i], deadline),
                                            take(accepted->observe())));
                                      } else
                                          observing[i].emplace(observe_result(
                                            i, take(accepted->observe())));
                                  } else {
                                      auto accepted = take(
                                        co_await writer.submit(
                                          std::move(*offers[i]), work));
                                      require(
                                        accepted.extent.size() == encoded_size,
                                        "writer changed the prepared group "
                                        "extent");
                                      if (!aggregate)
                                          requests[i].admitted
                                            = measurement_time();
                                      requests[i].accepted = true;
                                      requests[i].begin
                                        = accepted.extent.begin().value();
                                      requests[i].end
                                        = accepted.extent.end().value();
                                      direct[i % window].emplace(
                                        std::move(accepted));
                                      written_joined[i % window] = false;
                                  }
                                  offers[i].reset();
                                  peak_admission = std::max(
                                    peak_admission, budget.snapshot().bytes);
                                  if constexpr (fixed) {
                                      if (i % window == window - 1) {
                                          if constexpr (Coordinated) {
                                              groups->force();
                                              auto capture = groups->capture(
                                                clock::now());
                                              require(
                                                capture.has_value(),
                                                "fixed benchmark capture "
                                                "missing");
                                              take(groups->flush(
                                                writer, *capture));
                                              if (aggregate) {
                                                  // One joined cohort observer,
                                                  // no per-request timing
                                                  // coroutine.
                                                  auto complete = co_await take(
                                                    aggregate_tickets.back()
                                                      ->observe());
                                                  take(complete.failure()
                                                         .outcome());
                                                  require(
                                                    complete.receipt()
                                                      && complete.receipt()
                                                             ->boundary()
                                                           == capture
                                                                ->boundary(),
                                                    "aggregate cohort lacks "
                                                    "its exact receipt");
                                                  finish_aggregate(
                                                    aggregate_tickets,
                                                    std::span{requests}.subspan(
                                                      i + 1 - window, window),
                                                    capture->boundary());
                                              } else
                                                  for (std::size_t j = i + 1
                                                                       - window;
                                                       j <= i;
                                                       ++j) {
                                                      auto pending = std::move(
                                                        *observing[j]);
                                                      observing[j].reset();
                                                      co_await std::move(
                                                        pending);
                                                  }
                                          } else {
                                              for (std::size_t j = 0;
                                                   j != window;
                                                   ++j) {
                                                  written_joined[j] = true;
                                                  auto written
                                                    = co_await std::move(
                                                      direct[j]->written);
                                                  take(
                                                    written.failure.outcome());
                                              }
                                              auto barrier
                                                = co_await writer.barrier(
                                                  direct.back()->boundary);
                                              take(barrier.failure.outcome());
                                              require(
                                                barrier.receipt.has_value(),
                                                "direct benchmark barrier "
                                                "failed");
                                              const auto terminal
                                                = aggregate
                                                    ? 0
                                                    : measurement_time();
                                              for (std::size_t j = i + 1
                                                                   - window;
                                                   j <= i;
                                                   ++j) {
                                                  requests[j].terminal
                                                    = terminal;
                                                  requests[j].covering_end
                                                    = barrier.receipt
                                                        ->boundary()
                                                        .cursor()
                                                        .position()
                                                        .value();
                                              }
                                              for (auto& item : direct)
                                                  item.reset();
                                          }
                                          if constexpr (
                                            Profile
                                            == arrival_profile::rotation) {
                                              if (i + 1 != count) {
                                                  if constexpr (Coordinated)
                                                      take(
                                                        co_await groups->rotate(
                                                          writer,
                                                          encoded_size,
                                                          work));
                                                  else
                                                      take(
                                                        co_await writer.rotate(
                                                          take(
                                                            writer.capture()),
                                                          encoded_size,
                                                          work));
                                                  ++file_index;
                                              }
                                          }
                                      }
                                  } else if constexpr (
                                    Profile == arrival_profile::isolated) {
                                      auto pending = std::move(*observing[i]);
                                      observing[i].reset();
                                      co_await std::move(pending);
                                  }
                              }
                              if constexpr (Coordinated && !fixed)
                                  groups->force();
                              for (auto& pending : observing)
                                  if (pending) {
                                      auto joined = std::move(*pending);
                                      pending.reset();
                                      co_await std::move(joined);
                                  }
                              perf_tests::stop_measuring_time();
                              elapsed = measurement_time() - epoch;
                              reactor_cpu = static_cast<std::uint64_t>(
                                (seastar::thread_cputime_clock::now()
                                 - cpu_start)
                                  .count());
                              measuring = false;
                              files.sample.measuring = false;
                              stop_foreground = true;
#if !defined(KWAQUE_WAL_TIMING_ONLY)
                              if (observe_allocations)
                                  memory = codec::testing::
                                    end_allocation_observation();
#endif
                              allocations = seastar::memory::stats().mallocs()
                                            - before_allocations;
                              tasks = seastar::engine()
                                        .get_sched_stats()
                                        .tasks_processed
                                      - before_tasks;
                              metadata_operations = files.statistics().accepted
                                                    - before_metadata;
                          } catch (...) {
                              execution.observe(std::current_exception());
                          }
                          if (measuring) {
                              perf_tests::stop_measuring_time();
                              files.sample.measuring = false;
#if !defined(KWAQUE_WAL_TIMING_ONLY)
                              if (observe_allocations)
                                  memory = codec::testing::
                                    end_allocation_observation();
#endif
                          }
                          stop_foreground = true;
                          co_await std::move(foreground);
                          if (groups) {
                              try {
                                  execution.observe(co_await groups->close());
                              } catch (...) {
                                  execution.observe(std::current_exception());
                              }
                          }
                          for (auto& pending : observing)
                              if (pending) {
                                  auto joined = std::move(*pending);
                                  pending.reset();
                                  try {
                                      co_await std::move(joined);
                                  } catch (...) {
                                      execution.observe(
                                        std::current_exception());
                                  }
                              }
                          for (std::size_t j = 0; j != window; ++j)
                              if (direct[j]) {
                                  auto& item = direct[j];
                                  if (!written_joined[j]) {
                                      try {
                                          auto written = co_await std::move(
                                            item->written);
                                          execution.observe(
                                            written.failure.outcome());
                                      } catch (...) {
                                          execution.observe(
                                            std::current_exception());
                                      }
                                  }
                                  item.reset();
                              }
                          for (auto& result : retained)
                              result.reset();
                          for (auto& ticket : aggregate_tickets)
                              ticket.reset();
                          groups.reset();
                          take(execution.outcome());
                          require(
                            seastar::get_current_cpuset() == affinity,
                            "benchmark CPU affinity changed during execution");
                          const auto stats = writer.statistics();
                          if (preallocate) {
                              std::uint64_t final_extent = 8192;
                              for (const auto& request : requests)
                                  if (request.accepted)
                                      final_extent = std::max(
                                        final_extent, request.end);
                              co_await files.finish_extent(final_extent);
                          }
                          take(co_await writer.close());
                          const auto accepted_count = static_cast<std::size_t>(
                            std::count_if(
                              requests.begin(), requests.end(), [](auto r) {
                                  return r.accepted;
                              }));
                          if constexpr (fixed)
                              require(
                                accepted_count == count
                                  && stats.flush_calls == count / window,
                                "fixed pair changed work or barrier count");
                          require(
                            stats.accepted_groups == accepted_count,
                            "offered work accounting lost an accepted group");
                          if (!timing_only)
                              require(
                                files.sample.bytes
                                    == accepted_count * encoded_size.value()
                                  && files.sample.flushes == stats.flush_calls
                                  && files.sample.active == 0,
                                "native work was not fully joined");
                          if (observe_allocations)
                              require(
                                memory.complete
                                  && memory.largest_allocation
                                       <= maximum_contiguous_allocation_bytes,
                                "allocation observation incomplete or exceeded "
                                "contiguous ceiling");
                          codec::sha256_hasher expected_hash, actual_hash;
                          std::optional<std::ofstream> fixture, layout;
                          if (
                            const char* destination = std::getenv(
                              "KWAQUE_WAL_COHORT_FIXTURE")) {
                              require(
                                fixed && Profile != arrival_profile::rotation,
                                "fixture export requires a fixed single-file "
                                "case");
                              fixture.emplace(
                                destination,
                                std::ios::binary | std::ios::trunc);
                              layout.emplace(
                                std::string{destination} + ".groups",
                                std::ios::trunc);
                              require(
                                bool(*fixture) && bool(*layout),
                                "fixture export failed");
                          }
                          for (std::size_t i = 0; i != count; ++i) {
                              if (!requests[i].accepted) continue;
                              auto alias
                                = co_await verification_child.batch.share(
                                  {byte_count{32U << 20U},
                                   byte_count{1U << 20U},
                                   charge},
                                  work);
                              auto bytes = co_await encode_wal_prepare(
                                std::move(alias).value(),
                                {wal_write_context::make(
                                   heads[i]->incarnation,
                                   alignment(8192),
                                   runtime::file_position{requests[i].begin})
                                   .value(),
                                 expected.target,
                                 expected.target_data_start,
                                 expected.routing_epoch,
                                 expected.batch,
                                 expected.profile,
                                 expected.target_profile},
                                work,
                                byte_count{32U << 20U},
                                charge);
                              auto encoded = std::move(bytes).value();
                              for (auto fragment : encoded) {
                                  expected_hash.update(
                                    fragment.data(), fragment.size());
                                  if (fixture)
                                      fixture->write(
                                        fragment.data(),
                                        static_cast<std::streamsize>(
                                          fragment.size()));
                              }
                              if (layout) {
                                  *layout << encoded.size().value() << ' '
                                          << requests[i].covering_end << ' '
                                          << encoded.fragment_count();
                                  for (auto fragment : encoded)
                                      *layout << ' ' << fragment.size();
                                  *layout << '\n';
                              }
                              const auto path = take(
                                take(local_paths::make(root))
                                  .wal(0, heads[i]->incarnation));
                              auto file = take(
                                co_await files.open(
                                  path,
                                  {.close_policy
                                   = runtime::file_close_policy::checked}));
                              runtime::first_failure checked;
                              try {
                                  std::uint64_t offset = 0;
                                  while (offset != encoded.size().value()) {
                                      const auto length = byte_count{
                                        std::min<std::uint64_t>(
                                          65536,
                                          encoded.size().value() - offset)};
                                      auto read = take(
                                        co_await file.read(
                                          runtime::file_position{
                                            requests[i].begin + offset},
                                          length));
                                      require(
                                        read.data().size() == length,
                                        "benchmark readback was short");
                                      for (auto fragment : read.data())
                                          actual_hash.update(
                                            fragment.data(), fragment.size());
                                      require(
                                        co_await codec::bench::buffers_equal(
                                          read.data(),
                                          encoded
                                            .share(byte_count{offset}, length)
                                            .value(),
                                          work),
                                        "benchmark changed exact PREPARE "
                                        "bytes");
                                      offset += length.value();
                                  }
                              } catch (...) {
                                  checked.observe(std::current_exception());
                              }
                              checked.observe(co_await file.close());
                              take(checked.outcome());
                          }
                          if (fixture) {
                              fixture->close();
                              layout->close();
                              require(
                                bool(*fixture) && bool(*layout),
                                "fixture export did not finish");
                          }
                          const auto digest = hex_digest(
                            std::move(expected_hash).final());
                          require(
                            digest
                              == hex_digest(std::move(actual_hash).final()),
                            "benchmark byte digest mismatch");
                          std::printf(
                            "wal_cohort_v1 "
                            "{\"owner\":\"%s\",\"case\":\"%s\",\"observation\":"
                            "\"%s\",\"size\":%zu,"
                            "\"fragmented\":%s,\"delay_ns\":%" PRIu64
                            ",\"timing_only\":%s,\"preallocated\":%s,"
                            "\"foreground_probe\":%s,\"offered\":%zu,"
                            "\"accepted\":%zu,\"elapsed_ns\":%" PRIu64
                            ",\"encoded_bytes\":%" PRIu64
                            ",\"child_bytes\":%" PRIu64
                            ",\"child_fragments\":%zu,\"sha256\":\"%s\","
                            "\"flushes\":%" PRIu64 ",\"native_calls\":%" PRIu64
                            ",\"allocations\":%" PRIu64 ",\"tasks\":%" PRIu64
                            ",\"sampled_retained_admission\":%" PRIu64
                            ",\"batch_wait_ns\":%" PRIu64
                            ",\"write_service_ns\":%" PRIu64
                            ",\"flush_service_ns\":%" PRIu64
                            ",\"foreground_turns\":%" PRIu64
                            ",\"metadata_operations\":%" PRIu64
                            ",\"memory_observed\":%s,\"memory_peak\":%" PRIu64
                            ",\"critical_peak\":%" PRIu64 ",\"requests\":[",
                            Coordinated ? "coordinator" : "writer",
                            profile_name(Profile),
                            aggregate ? "aggregate" : "requests",
                            Size,
                            Fragmented ? "true" : "false",
                            Delay,
                            timing_only ? "true" : "false",
                            preallocate ? "true" : "false",
                            foreground_probe ? "true" : "false",
                            count,
                            accepted_count,
                            elapsed,
                            accepted_count * encoded_size.value(),
                            child_bytes,
                            child_fragments,
                            digest.data(),
                            stats.flush_calls,
                            files.sample.calls,
                            allocations,
                            tasks,
                            peak_admission,
                            batching.wait_ns,
                            files.sample.write_service_ns,
                            files.sample.flush_service_ns,
                            foreground_turns,
                            metadata_operations,
                            observe_allocations ? "true" : "false",
                            memory.peak_upper_bound,
                            memory.critical_peak_upper_bound);
                          for (std::size_t i = 0; i != count; ++i) {
                              const auto& r = requests[i];
                              if (aggregate) {
                                  std::printf(
                                    "%s{\"accepted\":%s,\"timeout\":false,"
                                    "\"arrival_lateness_ns\":null,\"admission_"
                                    "ns\":null,"
                                    "\"latency_ns\":null,\"terminal\":null,"
                                    "\"file\":%u,\"end\":%" PRIu64
                                    ",\"covering_end\":%" PRIu64 "}",
                                    i ? "," : "",
                                    r.accepted ? "true" : "false",
                                    r.file,
                                    r.end,
                                    r.covering_end);
                                  continue;
                              }
                              std::printf(
                                "%s{\"accepted\":%s,\"timeout\":%s,\"arrival_"
                                "lateness_"
                                "ns\":%" PRIu64 ",\"admission_ns\":%" PRIu64
                                ",\"latency_ns\":%" PRIu64
                                ",\"terminal\":%" PRIu64
                                ",\"file\":%u,\"end\":%" PRIu64
                                ",\"covering_end\":%" PRIu64 "}",
                                i ? "," : "",
                                r.accepted ? "true" : "false",
                                r.timeout ? "true" : "false",
                                r.started > r.planned ? r.started - r.planned
                                                      : 0,
                                r.admitted - r.started,
                                r.terminal > r.planned ? r.terminal - r.planned
                                                       : 0,
                                r.terminal,
                                r.file,
                                r.end,
                                r.covering_end);
                          }
                          std::printf(
                            "],\"setup_extent\":%" PRIu64 ",\"device\":%" PRIu64
                            ",\"comparison_id\":\"%s\",\"target_members\":%u,"
                            "\"target_bytes\":%" PRIu64
                            ",\"group_capacity\":%u,\"native_depth\":%" PRIu64
                            ",\"logical_writes\":%" PRIu64 ",\"flush_times\":[",
                            preallocate ? setup_extent : 0,
                            static_cast<std::uint64_t>(status.device_id),
                            comparison,
                            cfg.target_members,
                            cfg.target_bytes.value(),
                            cfg.outstanding_groups,
                            files.sample.depth,
                            stats.write_calls);
                          if (!timing_only)
                              for (std::size_t i = 0; i != files.sample.flushes;
                                   ++i)
                                  std::printf(
                                    "%s[%" PRIu64 ",%" PRIu64 "]",
                                    i ? "," : "",
                                    files.sample.flush_times[i].begin,
                                    files.sample.flush_times[i].end);
                          std::printf("],\"affinity_cpus\":[");
                          bool first_cpu = true;
                          for (auto cpu : affinity) {
                              std::printf("%s%u", first_cpu ? "" : ",", cpu);
                              first_cpu = false;
                          }
                          std::printf(
                            "],\"reactor_cpu_ns\":%" PRIu64, reactor_cpu);
                          if constexpr (timing_only) {
                              std::puts(
                                ",\"write_busy_ns\":null,\"write_max_service_"
                                "ns\":null}");
                          } else {
                              std::printf(
                                ",\"write_busy_ns\":%" PRIu64
                                ",\"write_max_service_ns\":%" PRIu64 "}\n",
                                files.sample.write_busy_ns,
                                files.sample.write_max_service_ns);
                          }
                      });
                }));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        try {
            take(co_await timer.stop());
        } catch (...) {
            failed.observe(std::current_exception());
        }
        co_await manager.stop();
        co_await registry.stop();
        take(failed.outcome());
        co_return count;
    }
};
} // namespace

#define COMMIT_FIXED(Name, Size, Fragmented)                                   \
    PERF_TEST_F(wal_commit_bench, writer_##Name) {                             \
        return (                                                               \
          measure<false, Size, Fragmented, 0, arrival_profile::fixed>());      \
    }                                                                          \
    PERF_TEST_F(wal_commit_bench, coordinator_##Name) {                        \
        return (measure<true, Size, Fragmented, 0, arrival_profile::fixed>()); \
    }
COMMIT_FIXED(tiny, 0, false)
COMMIT_FIXED(aligned, 131072, false)
COMMIT_FIXED(fragmented, 131072, true)
COMMIT_FIXED(m4, 4U << 20U, false)
COMMIT_FIXED(maximum, 8U << 20U, false)
#undef COMMIT_FIXED
#define COMMIT_ARRIVAL(Name, Profile)                                          \
    PERF_TEST_F(wal_commit_bench, zero_##Name) {                               \
        return (measure<true, 0, false, 0, arrival_profile::Profile>());       \
    }                                                                          \
    PERF_TEST_F(wal_commit_bench, delayed_##Name) {                            \
        return (                                                               \
          measure<true, 0, false, 1'000'000, arrival_profile::Profile>());     \
    }
COMMIT_ARRIVAL(burst, burst)
COMMIT_ARRIVAL(isolated, isolated)
COMMIT_ARRIVAL(paced, paced)
COMMIT_ARRIVAL(retained, retained)
COMMIT_ARRIVAL(timeout, timeout)
#undef COMMIT_ARRIVAL
PERF_TEST_F(wal_commit_bench, writer_rotation) {
    return (measure<false, 131072, false, 0, arrival_profile::rotation>());
}
PERF_TEST_F(wal_commit_bench, coordinator_rotation) {
    return (measure<true, 131072, false, 0, arrival_profile::rotation>());
}
PERF_TEST_F(wal_commit_bench, default_burst) {
    return (measure<
            true,
            0,
            false,
            1'000'000,
            arrival_profile::burst,
            32,
            4U << 20U,
            2>());
}
PERF_TEST_F(wal_commit_bench, default_paced) {
    return (measure<
            true,
            0,
            false,
            1'000'000,
            arrival_profile::paced,
            32,
            4U << 20U,
            2>());
}
PERF_TEST_F(wal_commit_bench, byte_target_zero) {
    return (
      measure<true, 131072, false, 0, arrival_profile::burst, 32, 262144>());
}
PERF_TEST_F(wal_commit_bench, byte_target_delayed) {
    return (measure<
            true,
            131072,
            false,
            1'000'000,
            arrival_profile::burst,
            32,
            262144>());
}
} // namespace kwaque::storage::testing
