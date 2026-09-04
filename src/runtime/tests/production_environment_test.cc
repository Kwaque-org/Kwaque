#include "src/base/units.h"
#include "src/observability/event_identity.h"
#include "src/resource/resource_config.h"
#include "src/resource/resource_registry.h"
#include "src/runtime/cross_shard.h"
#include "src/runtime/dns.h"
#include "src/runtime/file.h"
#include "src/runtime/network.h"
#include "src/runtime/production/environment.h"
#include "src/runtime/production/environment_test_support.h"
#include "src/runtime/testing/contracts/dns_test_server.h"
#include "src/runtime/testing/contracts/environment_contract.h"
#include "src/runtime/testing/contracts/environment_lifecycle_contract.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>
#include <seastar/util/tmp_file.hh>

#include <boost/test/unit_test.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace kwaque::runtime::test_types {

struct production_environment_observation final {
    owner_shard shard;
    std::uint64_t random_draw;
};

} // namespace kwaque::runtime::test_types

template<>
struct kwaque::runtime::enable_cross_shard_value<
  kwaque::runtime::test_types::production_environment_observation>
  : std::true_type {};

namespace {

kwaque::resource::resource_config resource_config() {
    auto result = kwaque::resource::resource_config::from_total_memory(
      kwaque::byte_count{std::uint64_t{128} * 1'024U * 1'024U});
    BOOST_REQUIRE(result.has_value());
    return *result;
}

kwaque::observability::event_sink_identity event_identity(std::uint64_t value) {
    auto epoch = kwaque::observability::event_sink_epoch::make(value);
    BOOST_REQUIRE(epoch.has_value());
    kwaque::observability::event_configuration_digest digest{};
    digest[0] = 0x61;
    return {
      .epoch = *epoch,
      .configuration_digest = digest,
    };
}

struct direct_driver final {
    template<typename T>
    seastar::future<T> lifecycle(seastar::future<T> waiting) const {
        constexpr auto watchdog = std::chrono::seconds{10};
        return seastar::with_timeout(
          seastar::lowres_clock::now() + watchdog, std::move(waiting));
    }

    template<typename T>
    seastar::future<T> operation(seastar::future<T> waiting) const {
        return lifecycle(std::move(waiting));
    }

    template<typename T, typename NativeOwner>
    seastar::future<T>
    operation(seastar::future<T> waiting, NativeOwner&) const {
        return lifecycle(std::move(waiting));
    }
};

kwaque::runtime::file_path file_path_of(const std::filesystem::path& path) {
    auto made = kwaque::runtime::file_path::make(path.string());
    BOOST_REQUIRE(made.has_value());
    return std::move(*made);
}

constexpr auto loopback = kwaque::runtime::network_address::ipv4(
  {std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}});

kwaque::runtime::test_types::production_environment_observation
inspect_started(kwaque::runtime::production::environment& environment) {
    const auto owner = environment.owner();
    if (
      environment.state() != kwaque::runtime::environment_state::started
      || !environment.resource_manager().ready()
      || owner.value() != seastar::this_shard_id()
      || environment.tasks().owner() != owner
      || environment.timer().owner() != owner
      || environment.file_system().owner() != owner
      || environment.network().owner() != owner
      || environment.dns().owner() != owner) {
        throw std::runtime_error(
          "shard-local production environment ownership is incomplete");
    }
    return {
      .shard = owner,
      .random_draw = environment.random().next_u64(),
    };
}

void require_aborted(kwaque::runtime::production::environment& environment) {
    if (
      !environment.abort_requested()
      || !environment.tasks().abort_requested()) {
        throw std::runtime_error(
          "shard-local production environment did not observe abort");
    }
}

seastar::logger& environment_logger() {
    static seastar::logger value{"kwaque-production-environment-test"};
    return value;
}

} // namespace

SEASTAR_TEST_CASE(production_environment_construction_propagates_allocation) {
    kwaque::resource::resource_registry registry;
    co_await registry.start(resource_config());

    std::unique_ptr<kwaque::runtime::production::environment> target;
    std::size_t attempts = 0;
    auto& injector = seastar::memory::local_failure_injector();
    while (target == nullptr) {
        injector.fail_after(attempts++);
        std::unique_ptr<kwaque::runtime::production::environment> candidate;
        try {
            candidate
              = std::make_unique<kwaque::runtime::production::environment>(
                kwaque::runtime::production::environment_dependencies{
                  registry.handles(),
                  environment_logger(),
                  event_identity(40)});
        } catch (...) {
            const bool injected = injector.failed();
            injector.cancel();
            if (!injected) {
                throw;
            }
            continue;
        }
        const bool injected = injector.failed();
        injector.cancel();
        if (!injected) {
            target = std::move(candidate);
            break;
        }
        co_await candidate->stop();
    }
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    BOOST_CHECK_GT(attempts, 1U);
#else
    BOOST_CHECK_EQUAL(attempts, 1U);
#endif
    BOOST_REQUIRE(target != nullptr);
    co_await target->stop();
    co_await registry.stop();
}

SEASTAR_TEST_CASE(production_environment_satisfies_shared_lifecycle_contract) {
    kwaque::resource::resource_registry registry;
    co_await registry.start(resource_config());
    {
        kwaque::runtime::production::environment target{
          kwaque::runtime::production::environment_dependencies{
            registry.handles(), environment_logger(), event_identity(1)}};
        co_await kwaque::runtime::testing::run_environment_lifecycle_contract(
          target, direct_driver{});
        BOOST_CHECK(
          target.resource_manager().state()
          == kwaque::resource::resource_manager_state::stopped);
        BOOST_CHECK(target.event_sink().stopped());
        BOOST_CHECK_EQUAL(target.event_sink().last_sequence(), 4U);
    }
    co_await registry.stop();
}

SEASTAR_TEST_CASE(production_environment_satisfies_component_contract) {
    seastar::tmp_dir directory;
    auto dns_listener = kwaque::runtime::testing::make_dns_listener();
    auto serving_dns = kwaque::runtime::testing::serve_dns_queries(
      dns_listener, {{{127, 0, 0, 42}, 7}}, true);
    kwaque::resource::resource_registry registry;
    bool registry_started = false;
    std::exception_ptr first_failure;

    try {
        co_await directory.create(
          std::filesystem::temp_directory_path()
          / "kwaque-environment-contract-XXXXXX");
        const auto component_directory = directory.get_path() / "component";
        co_await registry.start(resource_config());
        registry_started = true;
        {
            kwaque::runtime::production::environment target{
              kwaque::runtime::production::environment_dependencies{
                registry.handles(),
                environment_logger(),
                event_identity(50),
                kwaque::runtime::testing::dns_resolver_options(dns_listener)}};
            co_await kwaque::runtime::testing::run_environment_contract(
              target,
              kwaque::runtime::testing::environment_component_input{
                .root_path = file_path_of(component_directory),
                .listen_endpoint
                = kwaque::runtime::network_endpoint{loopback, 0},
                .dns = kwaque::runtime::testing::make_dns_query(
                  "environment.test"),
                .memory = kwaque::byte_count{4'096},
              },
              kwaque::runtime::testing::environment_contract_expectation{
                .dns_answers = {
                  {.endpoint = kwaque::runtime::network_endpoint{
                     kwaque::runtime::network_address::ipv4(
                       {std::byte{127},
                        std::byte{0},
                        std::byte{0},
                        std::byte{42}}),
                     33'145},
                   .ttl
                   = kwaque::runtime::monotonic_duration{7'000'000'000}},
                },
                .random_word = std::nullopt,
              },
              direct_driver{});
            if (co_await seastar::file_exists(component_directory.string())) {
                throw std::runtime_error(
                  "production environment component retained its path");
            }
            BOOST_CHECK(target.event_sink().stopped());
            BOOST_CHECK(
              kwaque::runtime::production::environment_test_access::
                components_released(target));
        }
    } catch (...) {
        first_failure = std::current_exception();
    }

    dns_listener.abort_accept();
    try {
        co_await direct_driver{}.lifecycle(std::move(serving_dns));
    } catch (...) {
        if (!first_failure) {
            first_failure = std::current_exception();
        }
    }
    if (registry_started) {
        try {
            co_await registry.stop();
        } catch (...) {
            if (!first_failure) {
                first_failure = std::current_exception();
            }
        }
    }
    try {
        co_await directory.remove();
    } catch (...) {
        if (!first_failure) {
            first_failure = std::current_exception();
        }
    }
    if (first_failure) {
        std::rethrow_exception(first_failure);
    }
}

SEASTAR_TEST_CASE(production_environment_stop_before_start_is_complete) {
    kwaque::resource::resource_registry registry;
    co_await registry.start(resource_config());
    {
        kwaque::runtime::production::environment target{
          kwaque::runtime::production::environment_dependencies{
            registry.handles(), environment_logger(), event_identity(2)}};
        BOOST_CHECK_THROW(static_cast<void>(target.random()), std::logic_error);
        co_await target.stop();
        co_await target.stop();
        BOOST_CHECK(
          target.state() == kwaque::runtime::environment_state::stopped);
        BOOST_CHECK(
          target.resource_manager().state()
          == kwaque::resource::resource_manager_state::stopped);
        BOOST_CHECK_THROW(static_cast<void>(target.random()), std::logic_error);
    }
    co_await registry.stop();
}

SEASTAR_TEST_CASE(production_environment_preabort_rolls_back_start) {
    kwaque::resource::resource_registry registry;
    co_await registry.start(resource_config());
    {
        kwaque::runtime::production::environment target{
          kwaque::runtime::production::environment_dependencies{
            registry.handles(), environment_logger(), event_identity(3)}};
        target.request_abort();
        bool aborted = false;
        try {
            co_await target.start();
        } catch (const seastar::abort_requested_exception&) {
            aborted = true;
        }
        BOOST_REQUIRE(aborted);
        BOOST_CHECK(
          target.state() == kwaque::runtime::environment_state::stopped);
        BOOST_CHECK(
          kwaque::runtime::production::environment_test_access::
            components_released(target));
        BOOST_CHECK_THROW(static_cast<void>(target.random()), std::logic_error);
        BOOST_CHECK_THROW(
          kwaque::runtime::basic_runtime{target}, std::logic_error);
    }
    co_await registry.stop();
}

SEASTAR_TEST_CASE(production_environment_stop_waits_for_retained_leases) {
    kwaque::resource::resource_registry registry;
    co_await registry.start(resource_config());
    {
        kwaque::runtime::production::environment target{
          kwaque::runtime::production::environment_dependencies{
            registry.handles(), environment_logger(), event_identity(4)}};
        co_await target.start();
        std::optional<seastar::future<>> stopping;
        {
            kwaque::runtime::basic_runtime root{target};
            auto acquired
              = root.view<kwaque::runtime::runtime_capability::random>();
            BOOST_REQUIRE(acquired.has_value());
            auto capability = std::move(*acquired);
            static_cast<void>(capability.random().next_u64());
            std::optional workload{target.resource_manager().acquire_workload(
              kwaque::resource::workload_class::maintenance)};

            stopping.emplace(target.stop());
            co_await seastar::yield();
            BOOST_CHECK(!stopping->available());
            static_cast<void>(capability.random().next_u64());
            workload.reset();
        }
        co_await std::move(*stopping);
        BOOST_CHECK(
          target.state() == kwaque::runtime::environment_state::stopped);
    }
    co_await registry.stop();
}

SEASTAR_TEST_CASE(production_environment_rolls_back_each_start_boundary) {
    kwaque::resource::resource_registry registry;
    co_await registry.start(resource_config());

    for (std::size_t point = 0; point < kwaque::runtime::production::
                                  environment_test_access::start_point_count;
         ++point) {
        kwaque::runtime::production::environment target{
          kwaque::runtime::production::environment_dependencies{
            registry.handles(),
            environment_logger(),
            event_identity(10 + point)}};
        bool failed = false;
        try {
            co_await kwaque::runtime::production::environment_test_access::
              fail_before_start_point(target, point);
        } catch (const std::runtime_error&) {
            failed = true;
        }
        BOOST_REQUIRE(failed);
        BOOST_CHECK(
          target.state() == kwaque::runtime::environment_state::stopped);
        BOOST_CHECK(
          kwaque::runtime::production::environment_test_access::
            components_released(target));
        BOOST_CHECK_THROW(static_cast<void>(target.random()), std::logic_error);
        BOOST_CHECK_THROW(
          kwaque::runtime::basic_runtime{target}, std::logic_error);
    }

    co_await registry.stop();
}

SEASTAR_TEST_CASE(production_environment_owner_starts_one_root_per_shard) {
    kwaque::resource::resource_registry registry;
    co_await registry.start(resource_config());
    kwaque::runtime::production::environment_owner environments{
      seastar::default_smp_service_group()};

    co_await environments.start(
      kwaque::runtime::production::environment_dependencies{
        registry.handles(), environment_logger(), event_identity(31)});
    const auto draws = co_await environments.invoke_on_all(&inspect_started);
    BOOST_REQUIRE_EQUAL(draws.size(), seastar::this_smp_shard_count());
    for (std::size_t shard = 0; shard < draws.size(); ++shard) {
        BOOST_CHECK_EQUAL(draws[shard].shard.value(), shard);
    }
    BOOST_CHECK_NE(draws[0].random_draw, draws[1].random_draw);
    co_await environments.request_abort();
    co_await environments.invoke_on_all(&require_aborted);
    co_await environments.stop();
    co_await registry.stop();
}
