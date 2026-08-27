#include "src/resource/resource_config.h"
#include "src/resource/resource_test_support.h"
#include "src/resource/workload_class.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace kwaque::resource {

namespace {

static_assert(std::copy_constructible<resource_handle_set>);
static_assert(std::is_copy_assignable_v<resource_handle_set>);
static_assert(!std::is_copy_constructible_v<resource_registry>);
static_assert(!std::is_move_constructible_v<resource_registry>);
static_assert(!std::is_copy_constructible_v<resource_manager>);
static_assert(!std::is_move_constructible_v<resource_manager>);
static_assert(!std::is_copy_constructible_v<workload_handle>);
static_assert(std::is_nothrow_move_constructible_v<workload_handle>);
static_assert(!std::is_move_assignable_v<workload_handle>);

resource_config test_config() {
    auto config = resource_config::from_total_memory(
      byte_count{
        static_cast<std::uint64_t>(seastar::memory::stats().total_memory())});
    if (!config) {
        throw std::runtime_error("test resource configuration was rejected");
    }
    return *config;
}

} // namespace

SEASTAR_TEST_CASE(
  resource_registry_gates_handles_and_enforces_one_active_owner) {
    resource_registry registry;
    BOOST_CHECK_THROW(static_cast<void>(registry.handles()), std::logic_error);

    co_await registry.start(test_config());
    BOOST_REQUIRE(registry.ready());
    const auto handles = registry.handles();
    BOOST_REQUIRE(handles.valid());
    BOOST_CHECK_EQUAL(
      handles.config().total_memory().value(),
      seastar::memory::stats().total_memory());
    resource_manager manager{handles};
    co_await manager.start();
    {
        auto original_workload = manager.acquire_workload(
          workload_class::metadata);
        auto moved_workload = std::move(original_workload);
        // A moved-from lease is deliberately invalid and must reject access.
        // NOLINTNEXTLINE(bugprone-use-after-move)
        BOOST_CHECK_THROW(
          static_cast<void>(original_workload.scheduling_group()),
          std::logic_error);
        BOOST_CHECK(!moved_workload.scheduling_group().is_main());
    }

    std::vector<seastar::scheduling_group> scheduling_groups;
    scheduling_groups.reserve(workload_class_count);
    for (const auto classification : all_workload_classes) {
        auto workload = manager.acquire_workload(classification);
        const auto group = workload.scheduling_group();
        scheduling_groups.push_back(group);
        BOOST_CHECK(!group.is_main());
        BOOST_CHECK_EQUAL(
          group.name(),
          "kwaque_" + std::string{descriptor_for(classification).metric_name});
    }
    for (std::size_t first = 0; first < workload_class_count; ++first) {
        for (std::size_t second = first + 1; second < workload_class_count;
             ++second) {
            BOOST_CHECK(scheduling_groups[first] != scheduling_groups[second]);
        }
    }

    resource_registry competing;
    bool rejected = false;
    try {
        co_await competing.start(test_config());
    } catch (const std::logic_error&) {
        rejected = true;
    }
    BOOST_CHECK(rejected);
    co_await competing.stop();

    bool active_manager_blocked_stop = false;
    try {
        co_await registry.stop();
    } catch (const std::logic_error&) {
        active_manager_blocked_stop = true;
    }
    BOOST_CHECK(active_manager_blocked_stop);
    BOOST_CHECK(registry.ready());
    BOOST_CHECK(handles.valid());
    co_await manager.stop();

    co_await registry.stop();
    co_await registry.stop();
    BOOST_CHECK(!registry.ready());
    BOOST_CHECK(!handles.valid());
    BOOST_CHECK_THROW(static_cast<void>(registry.handles()), std::logic_error);

    resource_manager stale_manager{handles};
    bool stale_manager_rejected = false;
    try {
        co_await stale_manager.start();
    } catch (const std::logic_error&) {
        stale_manager_rejected = true;
    }
    BOOST_CHECK(stale_manager_rejected);
    co_await stale_manager.stop();

    resource_registry replacement;
    co_await replacement.start(test_config());
    co_await replacement.stop();
}

SEASTAR_TEST_CASE(resource_registry_rolls_back_every_group_creation_point) {
    for (std::size_t point = 0;
         point < resource_registry_test_access::creation_point_count;
         ++point) {
        resource_registry registry;
        resource_registry_test_access::fail_before_creation(registry, point);
        bool injected = false;
        try {
            co_await registry.start(test_config());
        } catch (const std::runtime_error&) {
            injected = true;
        }
        BOOST_REQUIRE(injected);
        BOOST_CHECK(registry.state() == resource_registry_state::stopped);
        co_await registry.stop();

        resource_registry probe;
        co_await probe.start(test_config());
        co_await probe.stop();
    }
}

SEASTAR_TEST_CASE(resource_manager_rolls_back_every_local_start_point) {
    resource_registry registry;
    co_await registry.start(test_config());
    const auto handles = registry.handles();

    for (std::size_t point = 0;
         point < resource_manager_test_access::start_point_count;
         ++point) {
        resource_manager manager{handles};
        resource_manager_test_access::fail_before_start_point(manager, point);
        bool injected = false;
        try {
            co_await manager.start();
        } catch (const std::runtime_error&) {
            injected = true;
        }
        BOOST_REQUIRE(injected);
        BOOST_CHECK(manager.state() == resource_manager_state::stopped);
        co_await manager.stop();

        resource_manager probe{handles};
        co_await probe.start();
        co_await probe.stop();
    }

    co_await registry.stop();
}

} // namespace kwaque::resource
