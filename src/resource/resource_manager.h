#pragma once

#include "src/base/compiler.h"
#include "src/base/invariant.h"
#include "src/base/units.h"
#include "src/resource/resource_registry.h"
#include "src/resource/workload_class.h"
#include "src/runtime/shard_affinity.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_future.hh>

#include <array>
#include <cstddef>
#include <optional>
#include <stdexcept>

namespace kwaque::resource {

class resource_manager_test_access;
class resource_manager;

// A component-owned lease for one workload's execution handles. Acquire it
// during component startup, cache it, drain the component's own work, and
// destroy it before stopping the resource manager. Holding the lease prevents
// manager and registry teardown. CPU/SMP handle use adds no per-operation
// Kwaque bookkeeping; memory admission retains the class budget's accounting.
class workload_handle final {
public:
    workload_handle(workload_handle&& other) noexcept;
    workload_handle& operator=(workload_handle&&) = delete;
    workload_handle(const workload_handle&) = delete;
    workload_handle& operator=(const workload_handle&) = delete;
    ~workload_handle();

    [[nodiscard]] seastar::scheduling_group scheduling_group() const;
    // A limited SMP slot remains occupied until the submitted future resolves.
    // Calls using one group must not nest through that group, and nested groups
    // must form an acyclic dependency graph. Long workflows should return an
    // owned dispatch result promptly and continue under a component gate after
    // releasing the limited slot.
    [[nodiscard]] seastar::smp_service_group smp_service_group() const;
    // Cache this native handle during component startup and use Seastar's
    // try_get_units/get_units directly. This lease must outlive the resulting
    // semaphore_units and pending waits. Each component must reject requests
    // above hard_budget() and independently bound the waits it can submit.
    [[nodiscard]] seastar::semaphore& memory_admission() const;

private:
    friend class resource_manager;

    workload_handle(
      resource_manager& manager,
      seastar::semaphore& memory,
      seastar::scheduling_group scheduling_group,
      seastar::smp_service_group smp_service_group,
      seastar::gate::holder lifetime) noexcept;
    void assert_live() const {
        owner_.assert_current();
        if (KWAQUE_UNLIKELY(manager_ == nullptr)) {
            throw std::logic_error("workload handle has been moved from");
        }
    }

    runtime::owner_shard owner_;
    resource_manager* manager_;
    seastar::semaphore* memory_;
    seastar::scheduling_group scheduling_group_;
    seastar::smp_service_group smp_service_group_;
    seastar::gate::holder lifetime_;
};

enum class resource_manager_state {
    constructed,
    starting,
    started,
    stopping,
    stopped,
};

// Shard-local resource access. Components cache one workload lease during
// startup, drain their own gates, release every memory reservation and pending
// acquisition, then destroy the lease before this manager stops. The lease
// holds the manager gate for that complete component lifetime.
class resource_manager final : public runtime::shard_affine {
public:
    explicit resource_manager(resource_handle_set handles) noexcept;
    ~resource_manager();

    [[nodiscard]] seastar::future<> start();
    void request_abort();
    [[nodiscard]] seastar::future<> stop();

    [[nodiscard]] bool ready() const;
    [[nodiscard]] resource_manager_state state() const;
    [[nodiscard]] byte_count hard_budget(workload_class classification) const;
    [[nodiscard]] byte_count memory_used(workload_class classification) const;
    [[nodiscard]] byte_count
    memory_available(workload_class classification) const;
    [[nodiscard]] workload_handle
    acquire_workload(workload_class classification);

private:
    friend class resource_manager_test_access;
    friend class workload_handle;

    [[nodiscard]] static std::size_t
    checked_index(workload_class classification) {
        const auto index = workload_index(classification);
        if (index >= workload_class_count) {
            throw std::out_of_range("unknown workload class");
        }
        return index;
    }
    void assert_ready() const;
    void register_metrics();
    void rollback_start();
    [[nodiscard]] seastar::future<> stop_once();

    resource_handle_set handles_;
    std::array<std::optional<seastar::semaphore>, workload_class_count>
      memory_admissions_{};
    seastar::gate work_;
    seastar::metrics::metric_groups metrics_;
    seastar::shared_promise<> stop_done_;
    resource_manager_state state_{resource_manager_state::constructed};
    std::optional<std::size_t> fail_before_start_point_;
    bool registry_lease_acquired_{false};
};

inline seastar::semaphore& workload_handle::memory_admission() const {
    assert_live();
    return *memory_;
}

} // namespace kwaque::resource
