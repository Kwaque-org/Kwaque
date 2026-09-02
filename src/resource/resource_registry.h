#pragma once

#include "src/resource/resource_config.h"
#include "src/resource/workload_class.h"
#include "src/runtime/shard_affinity.h"

#include <seastar/core/future.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/smp.hh>

#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <optional>
#include <utility>

namespace kwaque::resource {

class resource_registry_test_access;
class resource_manager;

class resource_handle_set final {
public:
    // Handles are copyable across shards but remain valid only until their
    // owning registry begins stop().
    resource_handle_set(const resource_handle_set&) = default;
    resource_handle_set& operator=(const resource_handle_set&) = default;
    resource_handle_set(resource_handle_set&&) noexcept = default;
    resource_handle_set& operator=(resource_handle_set&&) noexcept = default;

    [[nodiscard]] const resource_config& config() const noexcept {
        return config_;
    }
    [[nodiscard]] bool valid() const noexcept;

private:
    friend class resource_registry;
    friend class resource_manager;
    friend class resource_registry_test_access;

    resource_handle_set(
      resource_config config,
      std::uint64_t generation,
      std::array<seastar::scheduling_group, workload_class_count>
        scheduling_groups,
      std::array<seastar::smp_service_group, workload_class_count>
        smp_service_groups) noexcept;

    [[nodiscard]] bool try_acquire_manager_lease() const noexcept;
    void release_manager_lease() const noexcept;
    void assert_valid() const;

    resource_config config_;
    std::uint64_t generation_;
    std::array<seastar::scheduling_group, workload_class_count>
      scheduling_groups_;
    std::array<seastar::smp_service_group, workload_class_count>
      smp_service_groups_;
};

enum class resource_registry_state {
    constructed,
    starting,
    started,
    stopping,
    stopped,
    failed,
};

// Owns process-global scheduling resources. Construct, start, access, and stop
// this object on shard zero. Every shard-local manager must be fully stopped
// before stop() destroys the distributed handles.
class resource_registry final : public runtime::shard_affine {
public:
    resource_registry() noexcept = default;
    ~resource_registry();

    [[nodiscard]] seastar::future<> start(resource_config config);
    [[nodiscard]] seastar::future<> stop();

    [[nodiscard]] bool ready() const;
    [[nodiscard]] resource_registry_state state() const;
    [[nodiscard]] resource_handle_set handles() const;

private:
    friend class resource_registry_test_access;

    void assert_coordinator() const;
    template<typename Checkpoint>
    [[nodiscard]] seastar::future<>
    start_with(resource_config config, Checkpoint checkpoint);
    void prepare_start(resource_config config);
    [[nodiscard]] seastar::future<>
    create_scheduling_group(workload_class classification);
    [[nodiscard]] seastar::future<>
    create_smp_service_group(workload_class classification);
    [[nodiscard]] seastar::future<> rollback_start() noexcept;
    void finish_start();
    [[nodiscard]] seastar::future<> destroy_created_groups();
    [[nodiscard]] seastar::future<> stop_once();

    std::array<std::optional<seastar::scheduling_group>, workload_class_count>
      scheduling_groups_;
    std::array<std::optional<seastar::smp_service_group>, workload_class_count>
      smp_service_groups_;
    std::optional<resource_config> config_;
    seastar::shared_promise<> stop_done_;
    resource_registry_state state_{resource_registry_state::constructed};
    std::uint64_t generation_{0};
};

template<typename Checkpoint>
seastar::future<>
resource_registry::start_with(resource_config config, Checkpoint checkpoint) {
    prepare_start(std::move(config));

    std::exception_ptr startup_failure;
    try {
        std::size_t point = 0;
        for (const auto classification : all_workload_classes) {
            checkpoint(point++);
            co_await create_scheduling_group(classification);
            checkpoint(point++);
            co_await create_smp_service_group(classification);
        }
    } catch (...) {
        startup_failure = std::current_exception();
    }

    if (startup_failure) {
        co_await rollback_start();
        std::rethrow_exception(startup_failure);
    }
    finish_start();
}

} // namespace kwaque::resource
