#pragma once

#include "src/base/units.h"
#include "src/broker/storage_directories.h"
#include "src/resource/resource_config.h"
#include "src/resource/resource_manager.h"
#include "src/resource/resource_registry.h"
#include "src/resource/workload_class.h"
#include "src/runtime/file.h"
#include "src/runtime/production/clocks.h"
#include "src/runtime/production/file.h"
#include "src/runtime/production/timer.h"
#include "src/runtime/testing/reactor_tasks.h"
#include "src/storage/local_store_config.h"
#include "src/storage/tests/local_store_contract.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/later.hh>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <type_traits>
#include <utility>
#include <vector>

// One shard's storage in directories of the production file system, for the
// storage contracts that take two budgets or more than one device, and for
// whatever else runs a store natively: the shard's resource owners, the
// directories taken and locked as a broker takes them, a budget for appends
// and one for what appends must not be able to starve, and a timer.
namespace kwaque::broker::testing {

// The clock a store on the production file system runs by.
using native_clock = runtime::production::monotonic_clock;

// Native continuations are run before an operation is waited for. Native
// preemption is timed, so an acceptance may complete inside the call that
// asked for it, before the group formation it started: a body that then
// offers its next request finds the first where the simulated driver would
// have let it get to.
struct draining_driver final {
    template<typename T>
    seastar::future<T> lifecycle(seastar::future<T> operation) const {
        co_await runtime::testing::drain_reactor_tasks();
        if constexpr (std::is_void_v<T>) {
            co_await std::move(operation);
            co_return;
        } else {
            co_return co_await std::move(operation);
        }
    }
};

// One turn of the reactor for a pending operation: what can run runs and
// what the kernel has finished is taken. False once the operation is done.
struct one_turn final {
    template<typename T>
    seastar::future<bool> operator()(seastar::future<T>& pending) const {
        if (pending.available()) co_return false;
        co_await runtime::testing::drain_reactor_tasks();
        co_return true;
    }
};

// An operation is waited for as it is, with nothing drained first: beside a
// task that never sleeps the reactor is never idle, and a drain would not
// return.
struct waiting_driver final {
    template<typename T>
    seastar::future<T> lifecycle(seastar::future<T> operation) const {
        return operation;
    }
};
// One yield for a pending operation. False once the operation is done.
struct one_yield final {
    template<typename T>
    seastar::future<bool> operator()(seastar::future<T>& pending) const {
        if (pending.available()) co_return false;
        co_await seastar::yield();
        co_return true;
    }
};

// A task that does nothing but yield, and what it saw: its turns and the
// longest wait between two of them, which is how long anything else kept
// the reactor from it.
struct foreground final {
    bool stop{false};
    std::uint64_t turns{0}, longest_nanoseconds{0};
};
inline seastar::future<> foreground_loop(foreground& probe) {
    auto last = native_clock::now().nanoseconds();
    while (!probe.stop) {
        co_await seastar::yield();
        const auto now = native_clock::now().nanoseconds();
        if (!probe.stop) {
            ++probe.turns;
            probe.longest_nanoseconds = std::max(
              probe.longest_nanoseconds, now - last);
        }
        last = now;
    }
}

namespace detail {
template<std::size_t N, std::size_t... I>
std::array<runtime::file_path, N> paths_of(
  const std::array<std::filesystem::path, N>& roots,
  std::index_sequence<I...>) {
    return {storage::testing::store_contract::take(
      runtime::file_path::make(roots[I].string()))...};
}
// One root, made if it is not there, and what the host says of its directory
// now.
inline seastar::future<storage::local_directory_identity>
made(std::filesystem::path root) {
    co_await seastar::recursive_touch_directory(root.string());
    const auto status = co_await seastar::file_stat(
      root.string(), seastar::follow_symlink::no);
    co_return storage::local_directory_identity{
      status.device_id, status.inode_number};
}
template<std::size_t N, std::size_t... I>
std::array<storage::local_directory_identity, N> identities_of(
  const std::vector<storage::local_directory_identity>& found,
  std::index_sequence<I...>) {
    return {found[I]...};
}
} // namespace detail

// What the appends' budget and the reserve grant, each in bytes at most its
// lease.
struct native_budgets final {
    storage::workload_budget_limits appends{
      .tasks = 128, .bytes = byte_count{48_MiB}, .handles = 32};
    storage::workload_budget_limits reserve{
      .tasks = 64, .bytes = byte_count{8_MiB}, .handles = 16};
};

// Runs `function(files, owner, devices, budget, reserve, drive, timer)` over
// the devices `specify(roots, directories)` says the directories are. Every
// root is made if it is not there, and each device's directory is what the
// host says of it now. A budget left charged is the body's failure.
template<std::size_t N, typename Specify, typename Func>
seastar::future<> with_native_devices(
  std::array<std::filesystem::path, N> roots,
  Specify specify,
  Func function,
  native_budgets limits = {}) {
    using storage::testing::store_contract::require;
    using storage::testing::store_contract::take;
    auto config = resource::resource_config::from_total_memory(
                    byte_count{seastar::memory::stats().total_memory()})
                    .value();
    resource::resource_registry registry;
    co_await registry.start(config);
    resource::resource_manager manager{registry.handles()};
    std::exception_ptr first;
    try {
        co_await manager.start();
        // Each directory's identity is whole when it is kept, and is kept
        // where a wait cannot reach it. An array filled an element a pass,
        // with a wait in every pass, is not kept across those waits by an
        // optimized build: it lies on the stack the wait gives up.
        std::vector<storage::local_directory_identity> directories;
        directories.reserve(N);
        for (const auto& root : roots)
            directories.push_back(co_await detail::made(root));
        const std::array<storage::local_device_spec, N> devices = specify(
          detail::paths_of(roots, std::make_index_sequence<N>{}),
          detail::identities_of<N>(directories, std::make_index_sequence<N>{}));
        auto ownership = take(
          co_await broker::storage_directories::acquire(devices));
        runtime::production::file_system files;
        auto appends = manager.acquire_workload(
          resource::workload_class::foreground_protocol);
        limits.appends.bytes = std::min(
          limits.appends.bytes, appends.hard_budget());
        storage::workload_budget budget{
          std::move(appends), limits.appends, bytes::testing::charge};
        auto lease = manager.acquire_workload(
          resource::workload_class::metadata);
        limits.reserve.bytes = std::min(
          limits.reserve.bytes, lease.hard_budget());
        storage::workload_budget reserve{
          std::move(lease), limits.reserve, bytes::testing::charge};
        runtime::production::timer timer;
        std::exception_ptr failure;
        try {
            co_await function(
              files,
              *ownership,
              devices,
              budget,
              reserve,
              draining_driver{},
              timer);
        } catch (...) {
            failure = std::current_exception();
        }
        take(co_await timer.stop());
        if (failure) std::rethrow_exception(failure);
        require(
          budget.snapshot().tasks == 0 && budget.snapshot().bytes == 0
            && budget.snapshot().handles == 0 && reserve.snapshot().tasks == 0
            && reserve.snapshot().bytes == 0,
          "a store left a budget charged");
    } catch (...) {
        first = std::current_exception();
    }
    co_await manager.stop();
    co_await registry.stop();
    if (first) std::rethrow_exception(first);
}

// One device that holds everything, in `root`:
// `function(files, owner, spec, budget, reserve, drive, timer)`.
template<typename Func>
seastar::future<> with_native_store(
  std::filesystem::path root, Func function, native_budgets limits = {}) {
    return with_native_devices(
      std::array{std::move(root)},
      [](
        std::array<runtime::file_path, 1> roots,
        const std::array<storage::local_directory_identity, 1>& directories) {
          return std::array{storage::testing::store_contract::specification(
            std::move(roots[0]), directories[0], 68)};
      },
      [function = std::move(function)](
        auto& files,
        auto& owner,
        const auto& devices,
        auto& budget,
        auto& reserve,
        auto drive,
        auto& timer) mutable {
          return function(
            files, owner, devices[0], budget, reserve, drive, timer);
      },
      limits);
}

} // namespace kwaque::broker::testing
