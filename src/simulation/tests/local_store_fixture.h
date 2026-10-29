#pragma once

#include "src/base/units.h"
#include "src/bytes/test_allocation_profile.h"
#include "src/resource/workload_class.h"
#include "src/runtime/file.h"
#include "src/runtime/testing/reactor_tasks.h"
#include "src/simulation/environment.h"
#include "src/simulation/fake_file.h"
#include "src/simulation/fake_file_test_support.h"
#include "src/simulation/scheduler.h"
#include "src/simulation/scheduler_driver.h"
#include "src/storage/tests/local_store_contract.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/future.hh>

#include <algorithm>
#include <array>
#include <cstdint>
#include <exception>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

// One shard's store on the simulated file system, for the storage contracts
// that run there: the environment's limits, a driver that keeps native
// continuations ahead of simulated completions, a step of one scheduler
// event, and the store itself with its budgets. A store that leaves a file
// open, an operation pending or a budget charged fails its body, and nothing
// here needs a test framework: a fuzz input runs the same store.
namespace kwaque::simulation::testing::local_store_fixture {
using namespace kwaque::storage;
using namespace kwaque::storage::testing::store_contract;

// With `long_run`, a run whose length its caller does not choose, such as a
// fuzz input's, ends on the scheduler's budget or the store's own and not on
// the fixture's: the trace has room for every event the scheduler may run,
// three entries an event, and the device has a handle for each one the two
// budgets may grant and for the harness's own. Every entry of a trace is
// allocated when it is made, so its room is what a run may need and no more.
inline environment_config config(
  std::vector<fault_rule> faults = {},
  std::optional<fake_crash_policy> crash = std::nullopt,
  bool long_run = false) {
    constexpr std::uint32_t total_events = 100000;
    environment_config_values values;
    values.file.crash_policy = crash;
    values.resource_total_memory = byte_count{256_MiB};
    values.scheduler.pending_events = 256;
    values.scheduler.events_per_pump = 64;
    values.scheduler.total_events = total_events;
    values.trace.entries = long_run ? 3U * total_events : 32768U;
    values.trace.encoded_bytes = long_run ? 128_MiB : 8_MiB;
    values.event_log.entries = 32;
    values.event_log.encoded_bytes = 32_KiB;
    values.file.maximum_objects = 256;
    values.file.maximum_open_handles = long_run ? 64 : 16;
    values.file.native_max_length = static_cast<std::uint32_t>(
      maximum_contiguous_allocation_bytes);
    // The WAL and segment writers run their physical windows together.
    values.file.maximum_pending_operations = 32;
    values.file.maximum_pending_reads = 8;
    values.file.maximum_pending_writes = 16;
    values.file.memory_dma_alignment = 4096;
    values.network.maximum_listeners = 2;
    values.network.maximum_connection_pairs = 2;
    values.network.maximum_pending_connects = 2;
    values.network.maximum_backlog_entries = 4;
    values.network.maximum_operations = 8;
    values.network.maximum_parked_operations = 4;
    values.network.maximum_packets = 16;
    values.network.maximum_direction_packets = 8;
    values.network.maximum_links = 8;
    values.network.maximum_address_entries = 8;
    values.network.maximum_active_flows = 4;
    values.network.maximum_controls = 8;
    values.network.stop_batch = 8;
    values.dns.maximum_records = 16;
    values.dns.maximum_answers = 32;
    values.dns.maximum_name_bytes = byte_count{8_KiB};
    values.dns.stop_batch = 8;
    values.dns.query_limits.maximum_waiters = 8;
    values.maximum_fault_rules = 16;
    for (auto& fault : faults)
        values.fault_rules.push_back(std::move(fault));
    return take(environment_config::make(std::move(values)));
}

// Drain native continuations before returning or advancing virtual time, so
// acceptance work never loses a race against simulated I/O completion. A
// future that is ready on entry still drains first: native preemption is
// timed, so an acceptance may complete inside the call, before the group
// formation it started.
struct ordered_driver final {
    scheduler& events;
    template<typename T>
    seastar::future<T> lifecycle(seastar::future<T> waiting) const {
        for (unsigned steps = 0;; ++steps) {
            co_await runtime::testing::drain_reactor_tasks();
            if (waiting.available()) break;
            if (steps == 100000 || events.pending_events() == 0)
                throw simulation::testing::scheduler_liveness_error{};
            simulation::testing::scheduler_driver_detail::run_next_batch(
              events, 1);
        }
        if constexpr (std::is_void_v<T>) {
            co_await std::move(waiting);
            co_return;
        } else {
            co_return co_await std::move(waiting);
        }
    }
};

// Advances a pending operation by one scheduler event; false once it has
// completed or nothing is left to run.
struct one_event final {
    scheduler* events;
    template<typename T>
    seastar::future<bool> operator()(seastar::future<T>& pending) const {
        co_await runtime::testing::drain_reactor_tasks();
        if (pending.available() || events->pending_events() == 0)
            co_return false;
        simulation::testing::scheduler_driver_detail::run_next_batch(
          *events, 1);
        co_return true;
    }
};

// Reserved: the body also takes a second budget, which funds what the
// appends must not be able to starve: the control owner and whoever reclaims
// the WAL.
template<bool Reserved, typename Func>
seastar::future<> with_store(
  Func function,
  std::vector<fault_rule> faults = {},
  std::optional<fake_crash_policy> crash = std::nullopt,
  bool long_run = false,
  std::uint32_t handles = 32) {
    auto target = take(
      environment::make(config(std::move(faults), crash, long_run)));
    ordered_driver drive{target->event_scheduler()};
    co_await target->start();
    std::exception_ptr first;
    try {
        workload_budget budget{
          target->resource_manager().acquire_workload(
            resource::workload_class::foreground_protocol),
          {.tasks = 128, .bytes = byte_count{32_MiB}, .handles = handles},
          bytes::testing::charge};
        auto& files = target->file_system();
        const auto root = take(runtime::file_path::make("/kwaque/store"));
        take(co_await drive.lifecycle(files.create_directories(root)));
        take(
          fake_file_test_access::sync_directory(
            files, take(fake_file_test_access::resolve(files, "/kwaque"))));
        const auto spec = specification(root, {1, 1}, 68);
        const std::array specs{spec};
        ownership_input owner{specs};
        if constexpr (Reserved) {
            auto lease = target->resource_manager().acquire_workload(
              resource::workload_class::metadata);
            const auto share = std::min<std::uint64_t>(
              lease.hard_budget().value(), 8_MiB);
            workload_budget reserve{
              std::move(lease),
              {.tasks = 64, .bytes = byte_count{share}, .handles = 16},
              bytes::testing::charge};
            co_await function(
              files, owner, spec, budget, reserve, drive, target->timer());
            require(
              reserve.snapshot().tasks == 0 && reserve.snapshot().bytes == 0,
              "a store left its reserve charged");
        } else {
            co_await function(
              files, owner, spec, budget, drive, target->timer());
        }
        require(
          fake_file_test_access::open_handles(target->file_system()) == 0
            && target->file_system().pending_operations() == 0,
          "a store left a file open or an operation pending");
        require(
          budget.snapshot().tasks == 0 && budget.snapshot().bytes == 0,
          "a store left its budget charged");
    } catch (...) {
        first = std::current_exception();
    }
    co_await drive.lifecycle(target->stop());
    if (first) std::rethrow_exception(first);
}
template<typename Func>
seastar::future<> with_local_store(
  Func function,
  std::vector<fault_rule> faults = {},
  std::optional<fake_crash_policy> crash = std::nullopt,
  bool long_run = false,
  std::uint32_t handles = 32) {
    return with_store<false, Func>(
      std::move(function), std::move(faults), crash, long_run, handles);
}
template<typename Func>
seastar::future<> with_reserved_store(
  Func function,
  std::optional<fake_crash_policy> crash = std::nullopt,
  bool long_run = false) {
    return with_store<true, Func>(std::move(function), {}, crash, long_run);
}

} // namespace kwaque::simulation::testing::local_store_fixture
