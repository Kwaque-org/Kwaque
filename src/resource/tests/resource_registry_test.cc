#include "src/resource/resource_config.h"
#include "src/resource/resource_test_support.h"
#include "src/resource/workload_class.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/metrics_api.hh>
#include <seastar/core/scheduling_specific.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/critical_alloc_section.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <atomic>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
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
        BOOST_CHECK(
          moved_workload.hard_budget()
          == manager.hard_budget(workload_class::metadata));
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
        bool injected = false;
        try {
            co_await resource_registry_test_access::fail_before_creation(
              registry, test_config(), point);
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
        bool injected = false;
        try {
            co_await resource_manager_test_access::fail_before_start_point(
              manager, point);
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

namespace {
constexpr std::size_t probe_shards = 16;
std::array<std::atomic<unsigned>, probe_shards> live_group_specifics{};
std::atomic<unsigned> failed_group_shard{std::numeric_limits<unsigned>::max()};
std::atomic<unsigned> group_constructor_failures{0};

struct group_specific_lifetime {
    group_specific_lifetime() {
        ++live_group_specifics[seastar::this_shard_id()];
    }
    ~group_specific_lifetime() {
        --live_group_specifics[seastar::this_shard_id()];
    }
};

struct group_specific_failure {
    group_specific_failure() {
        if (seastar::this_shard_id() == failed_group_shard.load()) {
            failed_group_shard.store(std::numeric_limits<unsigned>::max());
            ++group_constructor_failures;
            throw std::runtime_error(
              "injected scheduling-specific construction");
        }
    }
};
} // namespace

SEASTAR_TEST_CASE(resource_registry_rolls_back_inside_native_group_creation) {
    const auto shards = seastar::this_smp_shard_count();
    BOOST_REQUIRE_LE(shards, probe_shards);
    // The first key must be constructed before the second throws, so rollback
    // must destroy real initialized state as well as release the group slot.
    co_await seastar::scheduling_group_key_create(
      seastar::make_scheduling_group_key_config<group_specific_lifetime>());
    co_await seastar::scheduling_group_key_create(
      seastar::make_scheduling_group_key_config<group_specific_failure>());
    std::array<unsigned, probe_shards> baseline{};
    for (unsigned shard = 0; shard < shards; ++shard) {
        baseline[shard] = live_group_specifics[shard].load();
    }
    for (unsigned failed_shard = 0; failed_shard < shards; ++failed_shard) {
        resource_registry registry;
        group_constructor_failures.store(0);
        failed_group_shard.store(failed_shard);
        bool failed = false;
        try {
            co_await registry.start(test_config());
        } catch (const std::runtime_error& error) {
            failed = std::string_view{error.what()}
                     == "injected scheduling-specific construction";
        }
        failed_group_shard.store(std::numeric_limits<unsigned>::max());
        BOOST_CHECK(failed);
        BOOST_CHECK_EQUAL(group_constructor_failures.load(), 1U);
        co_await registry.stop();
        for (unsigned shard = 0; shard < shards; ++shard) {
            BOOST_CHECK_EQUAL(
              live_group_specifics[shard].load(), baseline[shard]);
        }
        // Reusing the same workload names also checks that failed construction
        // left no scheduler metric registrations or occupied group slots.
        resource_registry replacement;
        co_await replacement.start(test_config());
        co_await replacement.stop();
        for (unsigned shard = 0; shard < shards; ++shard) {
            BOOST_CHECK_EQUAL(
              live_group_specifics[shard].load(), baseline[shard]);
        }
    }
    // A second creation may wait behind failed construction. It must observe
    // the fully restored shard set, not reuse a slot during remote teardown.
    failed_group_shard.store(shards - 1U);
    group_constructor_failures.store(0);
    auto first = seastar::create_scheduling_group("failed_parallel_group", 100);
    auto second = seastar::create_scheduling_group("next_parallel_group", 100);
    bool failed = false;
    std::optional<seastar::scheduling_group> unexpected;
    try {
        unexpected = co_await std::move(first);
    } catch (const std::runtime_error&) {
        failed = true;
    }
    failed_group_shard.store(std::numeric_limits<unsigned>::max());
    const auto replacement = co_await std::move(second);
    co_await seastar::destroy_scheduling_group(replacement);
    if (unexpected) {
        co_await seastar::destroy_scheduling_group(*unexpected);
    }
    BOOST_CHECK(failed);
    BOOST_CHECK_EQUAL(group_constructor_failures.load(), 1U);
    for (unsigned shard = 0; shard < shards; ++shard) {
        BOOST_CHECK_EQUAL(live_group_specifics[shard].load(), baseline[shard]);
    }
}

#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
namespace {
std::size_t registered_series() {
    std::size_t count = 0;
    for (const auto& [name, family] : seastar::metrics::impl::get_value_map()) {
        static_cast<void>(name);
        count += family.size();
    }
    return count;
}
} // namespace
#endif

SEASTAR_TEST_CASE(scheduling_group_dispatch_preserves_user_failure_injection) {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    const auto group = co_await seastar::create_scheduling_group(
      "dispatch_probe", 100);
    const bool needs_dispatch = !group.active();
    bool entered = false;
    bool ran_in_group = false;
    bool critical_in_callback = true;
    std::exception_ptr failure;
    auto& injector = seastar::memory::local_failure_injector();
    injector.fail_after(0);
    try {
        co_await seastar::with_scheduling_group(group, [&] {
            entered = true;
            ran_in_group = seastar::current_scheduling_group() == group;
            critical_in_callback = seastar::memory::is_critical_alloc_section();
            // The pending fault must reach user work, after task allocation
            // and enqueueing have completed under their fatal-OOM contract.
            seastar::memory::on_alloc_point();
        });
    } catch (...) {
        failure = std::current_exception();
    }
    const bool injected = injector.failed();
    injector.cancel();
    co_await seastar::destroy_scheduling_group(group);
    BOOST_CHECK(needs_dispatch);
    BOOST_CHECK(entered);
    BOOST_CHECK(ran_in_group);
    BOOST_CHECK(!critical_in_callback);
    BOOST_CHECK(injected);
    BOOST_REQUIRE(failure != nullptr);
    try {
        std::rethrow_exception(failure);
    } catch (const std::bad_alloc&) {
    }
#endif
    co_return;
}

SEASTAR_TEST_CASE(native_group_allocation_failure_restores_every_shard) {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    std::array<std::size_t, probe_shards> baseline{};
    const auto shards = seastar::this_smp_shard_count();
    BOOST_REQUIRE_LE(shards, probe_shards);
    for (unsigned shard = 0; shard < shards; ++shard) {
        baseline[shard] = co_await seastar::smp::submit_to(
          shard, &registered_series);
    }
    for (const bool with_short_name : {false, true}) {
        unsigned failures = 0;
        bool reached_success = false;
        for (std::size_t point = 0; point < 512; ++point) {
            std::optional<seastar::scheduling_group> group;
            // Allocate the names before arming injection. Both are longer than
            // the inline string capacity, so a copy in a noexcept forwarding
            // overload would terminate instead of returning a failed future.
            seastar::sstring name{"allocation_probe"};
            seastar::sstring short_name{"allocation_short_probe"};
            auto& injector = seastar::memory::local_failure_injector();
            std::exception_ptr failure;
            injector.fail_after(point);
            try {
                if (with_short_name) {
                    group = co_await seastar::create_scheduling_group(
                      std::move(name), std::move(short_name), 100);
                } else {
                    group = co_await seastar::create_scheduling_group(
                      std::move(name), 100);
                }
            } catch (...) {
                failure = std::current_exception();
            }
            const bool injected = injector.failed();
            injector.cancel();
            if (group) {
                co_await seastar::destroy_scheduling_group(*group);
            }
            for (unsigned shard = 0; shard < shards; ++shard) {
                const auto count = co_await seastar::smp::submit_to(
                  shard, &registered_series);
                BOOST_CHECK_EQUAL(count, baseline[shard]);
            }
            if (injected) {
                ++failures;
                BOOST_REQUIRE(failure != nullptr);
                try {
                    std::rethrow_exception(failure);
                } catch (const std::bad_alloc&) {
                } catch (const std::runtime_error& error) {
                    // Native C aligned_alloc reports nullptr; the specific-data
                    // owner translates that failure into this exception.
                    BOOST_CHECK(
                      std::string_view{error.what()}
                      == "memory allocation failed");
                }
            } else {
                BOOST_CHECK(failure == nullptr);
                reached_success = true;
                break;
            }
        }
        BOOST_CHECK_GT(failures, 0U);
        BOOST_CHECK(reached_success);
    }
#endif
    co_return;
}

} // namespace kwaque::resource
