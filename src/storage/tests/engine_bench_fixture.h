#pragma once

#include "src/base/units.h"
#include "src/bytes/test_allocation_profile.h"
#include "src/codec/cooperative.h"
#include "src/resource/resource_config.h"
#include "src/resource/resource_manager.h"
#include "src/resource/resource_registry.h"
#include "src/resource/workload_class.h"
#include "src/runtime/file.h"
#include "src/runtime/first_failure.h"
#include "src/runtime/production/clocks.h"
#include "src/runtime/production/timer.h"
#include "src/storage/device_placement.h"
#include "src/storage/local_append.h"
#include "src/storage/local_engine.h"
#include "src/storage/local_store_bootstrap.h"
#include "src/storage/local_store_config.h"
#include "src/storage/recovery_inventory.h"
#include "src/storage/segment_lifecycle.h"
#include "src/storage/tests/local_append_contract.h"
#include "src/storage/tests/local_engine_contract.h"
#include "src/storage/tests/local_storage_bench_file.h"
#include "src/storage/tests/local_store_contract.h"
#include "src/storage/tests/segment_lifecycle_contract.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/seastar.hh>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <vector>

// One shard's storage composed as the engine composes it, on native files
// with real flushes, through the file system the storage benchmarks observe:
// the WAL and the control record on one device, segment data on the same
// one or on one or two others, a budget for appends and one for what appends
// must not be able to starve, and a timer. What a benchmark times is its
// own; this only builds, starts and stops.
namespace kwaque::storage::testing::engine_bench_support {
namespace measured = local_storage_bench_support;
namespace appends = local_append_contract;
namespace engines = local_engine_contract;
namespace lifecycle = segment_lifecycle_contract;
using store_contract::require;
using store_contract::take;
using clock = runtime::production::monotonic_clock;
using owner_type = store_contract::ownership_input;
using engine_type = local_engine<measured::file_system, owner_type, clock>;

// The engine's limits under the tests' allocation profile, for `writable`
// segments taking appends at once. Identities come from the control record
// in the product's blocks of 256: a block that runs out is replaced by a
// durable control publication, which the tests' block of 4 would put into
// one creation or seal in four.
inline local_engine_config configuration(std::uint32_t writable) {
    auto config = engines::configuration();
    config.table.maximum_writable = writable;
    config.id_block = 256;
    return config;
}

// Where one store lies: the directory that holds the WAL and the control
// record, and those that hold segment data. With no data directory the
// first holds everything, as one device does.
struct store_layout final {
    std::filesystem::path control;
    std::vector<std::filesystem::path> data;
};

// Operations run as they are, with nothing drained first.
struct direct_driver final {
    template<typename T>
    seastar::future<T> lifecycle(seastar::future<T> operation) const {
        return operation;
    }
};

// One store and its engine. The engine is made by each start and is let go
// by its stop, so a store can be started again by a new one, as a process
// would start it.
struct bench_store final {
    measured::file_system& files;
    owner_type& owner;
    std::span<const local_device_spec> devices;
    workload_budget& appends;
    workload_budget& reserve;
    runtime::production::timer& timer;
    seastar::abort_source& abort;
    local_engine_config config;
    std::unique_ptr<engine_type> engine;
    lifecycle::told heard;
};

// A new engine over the store, started with what its owner of segments
// lists. The report is the caller's to judge.
inline seastar::future<engine_start>
start(bench_store& store, std::span<const recovery_catalog_entry> catalog) {
    require(!store.engine, "a store is started by one engine at a time");
    store.engine = take(
      engine_type::make(
        {store.files,
         store.owner,
         store.devices,
         store.devices.front(),
         0,
         store.appends,
         store.appends,
         store.reserve,
         store.appends},
        store.config));
    co_return co_await store.engine->start(
      store.timer, catalog, {store.heard.sink(), {}, {}});
}
// The engine's joined stop. Its failure is the caller's.
inline seastar::future<> stop(bench_store& store) {
    if (!store.engine) co_return;
    auto stopped = co_await store.engine->stop();
    store.engine.reset();
    take(stopped);
}

// The sampler's part: every pool's share for this shard, as of now.
inline void share(bench_store& store, byte_count each) {
    auto* placement = store.engine->placement();
    require(placement != nullptr, "the shard is not ready");
    std::vector<device_pool_share> shares(
      placement->pools(), device_pool_share{true, false, each});
    placement->sampled(shares, clock::now());
}

// A segment of `generation`, created through the table on the device the
// shard's device table chooses, or on `device`.
inline seastar::future<runtime::result<local_append_target>> create(
  bench_store& store,
  std::uint64_t generation,
  std::optional<device_store_id> device = std::nullopt) {
    auto* table = store.engine->table();
    require(table != nullptr, "the shard is not ready");
    auto made = lifecycle::creation(store.devices.front(), generation);
    made.device = device;
    codec::cooperative_work work{store.config.policy, store.abort};
    co_return co_await table->create(std::move(made), work);
}

// One request of `generation` at `logical`, offered and joined, with a work
// account of its own.
inline seastar::future<local_append_outcome> append(
  bench_store& store,
  local_append_target target,
  std::uint64_t logical,
  std::uint64_t generation) {
    auto* owner = store.engine->append();
    require(owner != nullptr, "the shard is not ready");
    codec::cooperative_work work{store.config.policy, store.abort};
    auto stages = owner->append(
      target,
      co_await appends::request(
        appends::child_wire(logical, generation), store.appends, work),
      work);
    static_cast<void>(co_await std::move(stages.accepted));
    co_return co_await std::move(stages.result);
}

namespace detail {
// One directory, made if it is not there, and what the host says it is now.
inline seastar::future<local_directory_identity>
made(std::filesystem::path root) {
    co_await seastar::recursive_touch_directory(root.string());
    const auto status = co_await seastar::file_stat(
      root.string(), seastar::follow_symlink::no);
    co_return local_directory_identity{status.device_id, status.inode_number};
}
} // namespace detail

// Runs `function(store)` over a store laid out as `layout` says, with the
// shard's resource owners `manager` hands out. The device set is
// initialized first. An engine the function left running is stopped, and a
// budget left charged is the function's failure.
template<typename Func>
seastar::future<> with_bench_store(
  resource::resource_manager& manager,
  runtime::production::timer& timer,
  store_layout layout,
  bool observe,
  local_engine_config config,
  Func function) {
    // Each identity is kept whole across the waits that find it.
    std::vector<local_directory_identity> directories;
    directories.reserve(1 + layout.data.size());
    directories.push_back(co_await detail::made(layout.control));
    for (const auto& root : layout.data)
        directories.push_back(co_await detail::made(root));
    std::vector<local_device_spec> devices;
    devices.reserve(directories.size());
    const auto root_of = [](const std::filesystem::path& path) {
        return take(runtime::file_path::make(path.string()));
    };
    if (layout.data.empty()) {
        devices.push_back(
          store_contract::specification(
            root_of(layout.control), directories[0], 0x33));
    } else {
        devices.push_back(
          store_contract::specification(
            root_of(layout.control),
            directories[0],
            0x33,
            local_device_role::wal_control,
            1));
        for (std::size_t i = 0; i != layout.data.size(); ++i)
            devices.push_back(
              store_contract::specification(
                root_of(layout.data[i]),
                directories[i + 1],
                static_cast<std::uint8_t>(0x44 + 0x11 * i),
                local_device_role::data,
                1));
    }
    owner_type owner{devices};
    measured::file_system files{observe, 0};
    // Each writable segment's owner holds its data handle, write staging and
    // metadata workspace, about 8 MiB, for its lifetime, and 16 handles; the
    // WAL's owners take 32.
    const std::uint64_t writable = config.table.maximum_writable;
    workload_budget budget{
      manager.acquire_workload(resource::workload_class::foreground_protocol),
      {.tasks = 1024,
       .bytes
       = byte_count{std::max<std::uint64_t>(96, 32 + 8 * writable) << 20U},
       .handles = static_cast<std::uint32_t>(32 + 16 * writable)},
      bytes::testing::charge};
    auto lease = manager.acquire_workload(resource::workload_class::metadata);
    const auto held = std::min<std::uint64_t>(
      lease.hard_budget().value(), 32_MiB);
    workload_budget reserve{
      std::move(lease),
      {.tasks = 64, .bytes = byte_count{held}, .handles = 32},
      bytes::testing::charge};
    seastar::abort_source abort;
    {
        // The configured set is initialized together, as the shard's
        // devices are: it has one control device, and a data device alone
        // is no store.
        codec::cooperative_work work{config.policy, abort};
        const auto initialized = co_await initialize_local_stores(
          files,
          owner,
          std::span<const local_device_spec>{devices},
          {local_store_intent::create_or_resume, false},
          budget,
          store_contract::limits(),
          work);
        take(initialized.failure.outcome());
    }
    bench_store store{
      files, owner, devices, budget, reserve, timer, abort, std::move(config)};
    std::exception_ptr failure;
    try {
        co_await function(store);
    } catch (...) {
        failure = std::current_exception();
    }
    try {
        co_await stop(store);
    } catch (...) {
        if (!failure) failure = std::current_exception();
    }
    if (failure) std::rethrow_exception(failure);
    require(
      budget.snapshot().tasks == 0 && budget.snapshot().bytes == 0
        && budget.snapshot().handles == 0 && reserve.snapshot().tasks == 0
        && reserve.snapshot().bytes == 0,
      "a store left a budget charged");
}

// Runs `function(manager, timer)` with one shard's resource owners started
// from the reactor's memory, and stops them after it, whatever it did.
template<typename Func>
seastar::future<> with_bench_resources(Func function) {
    const auto config = resource::resource_config::from_total_memory(
                          byte_count{seastar::memory::stats().total_memory()})
                          .value();
    resource::resource_registry registry;
    co_await registry.start(config);
    resource::resource_manager manager{registry.handles()};
    runtime::production::timer timer;
    runtime::first_failure failed;
    try {
        co_await manager.start();
        co_await function(manager, timer);
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
}

} // namespace kwaque::storage::testing::engine_bench_support
