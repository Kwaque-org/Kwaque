#include "src/runtime/environment.h"
#include "src/runtime/production/backend.h"
#include "src/runtime/production/backend_test_support.h"
#include "src/runtime/runtime_service.h"
#include "src/runtime/sharded_service.h"
#include "src/runtime/testing/contracts/dns_test_server.h"
#include "src/runtime/testing/contracts/real_backend_conformance.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <functional>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace kwaque::runtime::test_types {

struct backend_observation final {
    owner_shard shard;
    std::uint64_t random_draw;
};

} // namespace kwaque::runtime::test_types

template<>
struct kwaque::runtime::enable_cross_shard_value<
  kwaque::runtime::test_types::backend_observation> : std::true_type {};

namespace {

using kwaque::runtime::production::backend;

static_assert(!std::constructible_from<backend, seastar::abort_source&>);
static_assert(!std::copy_constructible<backend>);
static_assert(!std::move_constructible<backend>);

kwaque::runtime::test_types::backend_observation
inspect_backend(backend& local) {
    if (
      local.state() != kwaque::runtime::production::backend_state::started
      || local.timer().owner() != local.owner()
      || local.file_system().owner() != local.owner()
      || local.network().owner() != local.owner()
      || local.dns().owner() != local.owner()) {
        throw std::runtime_error("local backend ownership is incomplete");
    }

    kwaque::runtime::basic_runtime<backend> runtime{local};
    auto capabilities = runtime.view<
      kwaque::runtime::runtime_capability::timer,
      kwaque::runtime::runtime_capability::random,
      kwaque::runtime::runtime_capability::file_system,
      kwaque::runtime::runtime_capability::network,
      kwaque::runtime::runtime_capability::dns>();
    if (!capabilities) {
        throw std::runtime_error("backend capabilities were unavailable");
    }
    static_cast<void>(capabilities->timer());
    static_cast<void>(capabilities->file_system());
    static_cast<void>(capabilities->network());
    static_cast<void>(capabilities->dns());
    return {
      .shard = local.owner(),
      .random_draw = capabilities->random().next_u64(),
    };
}

void require_backend_aborted(backend& local) {
    if (!local.abort_requested()) {
        throw std::runtime_error("local backend did not observe parent abort");
    }
}

seastar::future<kwaque::runtime::owner_shard>
stop_waits_for_capability_leases(backend& local) {
    bool waited = false;
    std::optional<seastar::future<>> stopping;
    {
        kwaque::runtime::basic_runtime<backend> runtime{local};
        auto capability
          = runtime.view<kwaque::runtime::runtime_capability::random>();
        if (!capability) {
            throw std::runtime_error("random capability was unavailable");
        }
        stopping.emplace(local.stop());
        waited = !stopping->available();
    }
    co_await std::move(*stopping);
    if (
      !waited
      || local.state() != kwaque::runtime::production::backend_state::stopped) {
        throw std::runtime_error("backend stop did not wait for its leases");
    }
    co_return local.owner();
}

seastar::future<> run_backend_conformance(
  backend& local,
  std::optional<kwaque::runtime::testing::real_backend_dns_expectation>
    dns_expectation = std::nullopt) {
    const auto path
      = std::filesystem::temp_directory_path()
        / ("kwaque-backend-conformance-" + std::to_string(seastar::this_shard_id()));
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
    auto root = kwaque::runtime::file_path::make(path.string());
    if (!root) {
        throw std::runtime_error("conformance root path was rejected");
    }
    try {
        co_await kwaque::runtime::testing::run_real_backend_conformance(
          local, std::move(*root), std::move(dns_expectation));
    } catch (...) {
        std::filesystem::remove_all(path, ignored);
        throw;
    }
    if (std::filesystem::exists(path)) {
        std::filesystem::remove_all(path, ignored);
        throw std::runtime_error("conformance left temporary files behind");
    }
}

} // namespace

SEASTAR_TEST_CASE(production_backend_owns_every_local_capability) {
    BOOST_REQUIRE_GE(seastar::this_smp_shard_count(), 2U);
    kwaque::runtime::sharded_service<kwaque::runtime::runtime_service> runtimes{
      seastar::default_smp_service_group()};
    kwaque::runtime::production::backend_owner backends{
      seastar::default_smp_service_group()};

    co_await runtimes.start();
    co_await kwaque::runtime::production::start_backends(backends, runtimes);

    const auto observations = co_await backends.invoke_on_all(&inspect_backend);
    BOOST_REQUIRE_EQUAL(observations.size(), seastar::this_smp_shard_count());
    for (std::size_t index = 0; index < observations.size(); ++index) {
        BOOST_CHECK_EQUAL(observations[index].shard.value(), index);
    }
    BOOST_CHECK_NE(observations[0].random_draw, observations[1].random_draw);

    co_await runtimes.request_abort();
    co_await backends.invoke_on_all(&require_backend_aborted);

    co_await backends.stop();
    co_await runtimes.stop();
}

SEASTAR_TEST_CASE(production_backend_close_drains_capability_views_first) {
    kwaque::runtime::sharded_service<kwaque::runtime::runtime_service> runtimes{
      seastar::default_smp_service_group()};
    kwaque::runtime::production::backend_owner backends{
      seastar::default_smp_service_group()};

    co_await runtimes.start();
    co_await kwaque::runtime::production::start_backends(backends, runtimes);

    const auto stopped_shard = co_await backends.invoke_on_owner(
      kwaque::runtime::owner_shard{}, &stop_waits_for_capability_leases);
    BOOST_CHECK_EQUAL(stopped_shard.value(), 0U);

    co_await backends.stop();
    co_await runtimes.stop();
}

SEASTAR_TEST_CASE(production_backend_shared_real_conformance) {
    auto listener = kwaque::runtime::testing::make_dns_listener();
    auto serving = kwaque::runtime::testing::serve_dns_queries(
      listener,
      {
        {.address = {127, 0, 0, 42}, .ttl = 7},
        {.address = {127, 0, 0, 43}, .ttl = 11},
      },
      true);
    seastar::abort_source parent_abort;
    auto local = kwaque::runtime::production::backend_test_access::make(
      parent_abort, kwaque::runtime::testing::dns_resolver_options(listener));

    std::exception_ptr failure;
    try {
        co_await local->start();
        co_await run_backend_conformance(
          *local,
          kwaque::runtime::testing::real_backend_dns_expectation{
            .query = kwaque::runtime::testing::make_dns_query(
              "conformance.test"),
            .answers = {
              {.endpoint = kwaque::runtime::network_endpoint{
                 kwaque::runtime::network_address::ipv4(
                   {std::byte{127},
                    std::byte{0},
                    std::byte{0},
                    std::byte{42}}),
                 33145},
               .ttl = kwaque::runtime::monotonic_duration{7'000'000'000}},
              {.endpoint = kwaque::runtime::network_endpoint{
                 kwaque::runtime::network_address::ipv4(
                   {std::byte{127},
                    std::byte{0},
                    std::byte{0},
                    std::byte{43}}),
                 33145},
               .ttl = kwaque::runtime::monotonic_duration{11'000'000'000}},
            },
          });
    } catch (...) {
        failure = std::current_exception();
    }
    try {
        co_await local->stop();
    } catch (...) {
        if (!failure) {
            failure = std::current_exception();
        }
    }
    listener.abort_accept();
    try {
        co_await std::move(serving);
    } catch (...) {
        if (!failure) {
            failure = std::current_exception();
        }
    }
    if (failure) {
        std::rethrow_exception(failure);
    }
}

SEASTAR_TEST_CASE(production_backend_rolls_back_preaborted_start) {
    kwaque::runtime::sharded_service<kwaque::runtime::runtime_service> runtimes{
      seastar::default_smp_service_group()};
    kwaque::runtime::production::backend_owner backends{
      seastar::default_smp_service_group()};

    co_await runtimes.start();
    co_await runtimes.request_abort();

    bool rejected = false;
    try {
        co_await kwaque::runtime::production::start_backends(
          backends, runtimes);
    } catch (const seastar::abort_requested_exception&) {
        rejected = true;
    }
    BOOST_CHECK(rejected);
    BOOST_CHECK(
      backends.state() == kwaque::runtime::sharded_service_state::stopped);

    co_await backends.stop();
    co_await runtimes.stop();
}

SEASTAR_TEST_CASE(production_backend_rolls_back_each_component_start) {
    seastar::abort_source parent_abort;

    for (std::size_t point = 0;
         point
         < kwaque::runtime::production::backend_test_access::start_point_count;
         ++point) {
        auto local = kwaque::runtime::production::backend_test_access::make(
          parent_abort);
        bool injected = false;
        try {
            co_await kwaque::runtime::production::backend_test_access::
              fail_before_start_point(*local, point);
        } catch (const std::runtime_error&) {
            injected = true;
        }
        BOOST_REQUIRE(injected);
        BOOST_CHECK(
          local->state()
          == kwaque::runtime::production::backend_state::stopped);
        BOOST_CHECK(
          kwaque::runtime::production::backend_test_access::components_released(
            *local));
        co_await local->stop();
    }
}
