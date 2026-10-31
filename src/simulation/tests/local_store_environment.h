#pragma once

#include "src/base/allocation.h"
#include "src/base/units.h"
#include "src/bytes/test_allocation_profile.h"
#include "src/resource/workload_class.h"
#include "src/runtime/fault.h"
#include "src/runtime/file.h"
#include "src/runtime/testing/reactor_tasks.h"
#include "src/simulation/environment.h"
#include "src/simulation/fake_file.h"
#include "src/simulation/fake_file_test_support.h"
#include "src/simulation/fault_schedule.h"
#include "src/simulation/scheduler.h"
#include "src/simulation/scheduler_driver.h"
#include "src/storage/tests/local_store_contract.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/future.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <cstdint>
#include <exception>
#include <optional>
#include <utility>

// One shard's environment on the simulated file system for the storage
// contracts that check one component at a time: the environment's limits
// with up to two fault rules, a budget the body must leave as it found it,
// and the store on one device that most contracts run on.
namespace kwaque::simulation::testing::local_store_environment {
using namespace kwaque::storage;
using namespace kwaque::storage::testing::store_contract;

inline environment_config config(
  std::optional<runtime::builtin_fault_point> point = std::nullopt,
  runtime::fault_action action
  = runtime::fault_action::file_failure_before_effect,
  std::uint64_t occurrence = 1,
  std::optional<runtime::fault_object_key> object = std::nullopt,
  std::optional<fake_crash_policy> crash_policy = std::nullopt,
  std::optional<fault_rule> additional = std::nullopt,
  std::uint32_t native_max_length
  = static_cast<std::uint32_t>(maximum_contiguous_allocation_bytes),
  std::uint32_t maximum_open_handles = 16) {
    environment_config_values values;
    values.scheduler.pending_events = 256;
    values.scheduler.events_per_pump = 64;
    values.scheduler.total_events = 100000;
    values.trace.entries = 32768;
    values.trace.encoded_bytes = 8_MiB;
    values.event_log.entries = 32;
    values.event_log.encoded_bytes = 32_KiB;
    values.file.crash_policy = crash_policy;
    values.file.maximum_objects = 256;
    values.file.maximum_open_handles = maximum_open_handles;
    values.file.native_max_length = native_max_length;
    values.file.maximum_pending_operations = 8;
    values.file.maximum_pending_reads = 8;
    values.file.maximum_pending_writes = 8;
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
    if (point) {
        const auto decision
          = action == runtime::fault_action::drop_completion
              ? runtime::fault_decision::make_drop_completion()
            : action == runtime::fault_action::error
              ? runtime::fault_decision::make_error()
              : take(
                  runtime::fault_decision::make_file_failure(
                    action,
                    runtime::file_failure_detail::device_io,
                    byte_count{
                      action == runtime::fault_action::file_failure_after_prefix
                        ? 512U
                        : 0U}));
        values.fault_rules.push_back(take(
          fault_rule::make(
            take(fault_rule_id::make(1)),
            *point,
            object,
            take(runtime::fault_occurrence::make(occurrence)),
            take(runtime::fault_occurrence::make(occurrence)),
            fault_selector::once(),
            decision)));
    }
    if (additional) values.fault_rules.push_back(std::move(*additional));
    return take(environment_config::make(std::move(values)));
}

inline void stabilize(fake_file_system& files, const runtime::file_path& path) {
    take(
      fake_file_test_access::sync_directory(
        files, take(fake_file_test_access::resolve(files, path.value()))));
}

template<typename Func>
seastar::future<> with_store_environment(
  environment_config configuration,
  Func function,
  byte_count budget_bytes = byte_count{8_MiB},
  resource::workload_class classification = resource::workload_class::metadata,
  std::uint32_t budget_tasks = 16) {
    auto target = take(environment::make(std::move(configuration)));
    simulation::testing::scheduler_driver drive{target->event_scheduler()};
    co_await target->start();
    std::exception_ptr first;
    try {
        workload_budget budget{
          target->resource_manager().acquire_workload(classification),
          {.tasks = budget_tasks, .bytes = budget_bytes, .handles = 32},
          bytes::testing::charge};
        co_await function(*target, budget, drive);
        BOOST_CHECK_EQUAL(
          fake_file_test_access::open_handles(target->file_system()), 0U);
        BOOST_CHECK_EQUAL(target->file_system().pending_operations(), 0U);
        BOOST_CHECK_EQUAL(budget.snapshot().tasks, 0U);
        BOOST_CHECK_EQUAL(budget.snapshot().bytes, 0U);
    } catch (...) {
        first = std::current_exception();
    }
    co_await drive.lifecycle(target->stop());
    if (first) std::rethrow_exception(first);
}

// Drain runnable work in every scheduling group before advancing one backend
// event. Only reactor quiescence without an event is a parked completion.
enum class history_boundary { stepped, completed, parked };
template<typename T>
seastar::future<history_boundary>
history_step(scheduler& events, seastar::future<T>& operation) {
    if (operation.available()) co_return history_boundary::completed;
    co_await runtime::testing::drain_reactor_tasks();
    if (operation.available()) co_return history_boundary::completed;
    if (events.pending_events()) {
        if (!events.has_ready_events()) take(events.advance_to_next());
        require(take(events.step()), "history failed to advance its event");
        co_return history_boundary::stepped;
    }
    co_return history_boundary::parked;
}

// The body that makes the store root /kwaque/store durable on one device
// and runs a contract there, given the files, the ownership, the device,
// the budget and the driver.
template<typename Contract>
auto on_one_device(std::uint8_t device, Contract contract) {
    return
      [device, contract = std::move(contract)](
        this auto, auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, device);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await contract(files, owner, spec, budget, drive);
      };
}

} // namespace kwaque::simulation::testing::local_store_environment
