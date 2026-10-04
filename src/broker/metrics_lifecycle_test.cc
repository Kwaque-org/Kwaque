#include "src/admin/admin_limits.h"
#include "src/admin/admin_server.h"
#include "src/base/metric_schema.h"
#include "src/base/units.h"
#include "src/observability/event_identity.h"
#include "src/resource/resource_config.h"
#include "src/resource/resource_registry.h"
#include "src/runtime/production/environment.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/map_reduce.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_api.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/net/api.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

namespace {

const kwaque::metric_descriptor& metric_descriptor(kwaque::metric_id id) {
    const auto* descriptor = kwaque::descriptor_for(id);
    if (descriptor == nullptr) {
        throw std::logic_error("metric descriptor is missing");
    }
    return *descriptor;
}

seastar::sstring full_name(kwaque::metric_id id) {
    const auto& descriptor = metric_descriptor(id);
    auto result = seastar::sstring{std::string{descriptor.group}};
    result += seastar::sstring{"_"};
    result += seastar::sstring{std::string{descriptor.name}};
    return result;
}

seastar::future<unsigned> registration_count(seastar::sstring name) {
    co_return co_await seastar::map_reduce(
      seastar::this_smp_all_shards(),
      [name](unsigned shard) {
          return seastar::smp::submit_to(shard, [name] {
              return seastar::metrics::impl::get_value_map().contains(name)
                       ? 1U
                       : 0U;
          });
      },
      0U,
      std::plus<>{});
}

seastar::future<>
require_registration_count(seastar::sstring name, unsigned count) {
    const auto actual = co_await registration_count(std::move(name));
    BOOST_CHECK_EQUAL(actual, count);
}

seastar::future<> require_metric_range(
  kwaque::metric_id first, kwaque::metric_id last, unsigned count) {
    for (std::uint16_t value = static_cast<std::uint16_t>(first);
         value <= static_cast<std::uint16_t>(last);
         ++value) {
        co_await require_registration_count(
          full_name(static_cast<kwaque::metric_id>(value)), count);
    }
}

// Blocks the last process metric the admin owner registers on shard zero.
class admin_metric_blocker final {
public:
    admin_metric_blocker() {
        if (seastar::this_shard_id() == 0U) {
            metrics_.emplace();
            metrics_->add_group(
              "broker",
              {seastar::metrics::make_gauge(
                "start_time_seconds",
                [] { return 0U; },
                seastar::metrics::description(
                  "Administrative registration blocker"))});
        }
    }

    seastar::future<> stop() {
        metrics_.reset();
        return seastar::make_ready_future<>();
    }

private:
    std::optional<seastar::metrics::metric_groups> metrics_;
};

struct snapshot_usage final {
    std::size_t families{0};
    std::size_t series{0};
};

// The largest per-shard registry the bounded scrape must snapshot. Disabled
// series are not copied, so they do not count.
seastar::future<snapshot_usage> largest_snapshot_usage() {
    co_return co_await seastar::map_reduce(
      seastar::this_smp_all_shards(),
      [](unsigned shard) {
          return seastar::smp::submit_to(shard, [] {
              snapshot_usage usage;
              for (const auto& [name, family] :
                   seastar::metrics::impl::get_value_map()) {
                  ++usage.families;
                  for (const auto& [labels, metric] : family) {
                      if (metric && metric->is_enabled()) {
                          ++usage.series;
                      }
                  }
              }
              return usage;
          });
      },
      snapshot_usage{},
      [](snapshot_usage left, snapshot_usage right) {
          return snapshot_usage{
            .families = std::max(left.families, right.families),
            .series = std::max(left.series, right.series)};
      });
}

kwaque::resource::resource_config resource_config() {
    auto configured = kwaque::resource::resource_config::from_total_memory(
      kwaque::byte_count{std::uint64_t{128} * 1'024U * 1'024U});
    BOOST_REQUIRE(configured.has_value());
    return *configured;
}

kwaque::observability::event_sink_identity event_identity() {
    auto epoch = kwaque::observability::event_sink_epoch::make(1);
    BOOST_REQUIRE(epoch.has_value());
    return {
      .epoch = *epoch,
      .configuration_digest = {},
    };
}

seastar::logger& environment_logger() {
    static seastar::logger value{"kwaque-environment-metrics-test"};
    return value;
}

} // namespace

SEASTAR_TEST_CASE(runtime_and_admin_metrics_follow_endpoint_lifecycle) {
    BOOST_REQUIRE_EQUAL(seastar::this_smp_shard_count(), 2U);
    kwaque::resource::resource_registry registry;
    kwaque::runtime::production::environment_owner environments{
      seastar::default_smp_service_group()};
    kwaque::admin::admin_server admin;

    co_await registry.start(resource_config());
    co_await environments.start(
      kwaque::runtime::production::environment_dependencies{
        registry.handles(), environment_logger(), event_identity()});
    co_await require_metric_range(
      kwaque::metric_id::task_active,
      kwaque::metric_id::task_abort_requests_total,
      2U);
    co_await require_metric_range(
      kwaque::metric_id::timer_active,
      kwaque::metric_id::dns_rejected_total,
      2U);
    co_await require_metric_range(
      kwaque::metric_id::memory_configured_bytes,
      kwaque::metric_id::memory_waiters,
      2U);

    co_await admin.start("127.0.0.1", 0, seastar::this_smp_shard_count());
    co_await admin.mark_ready(std::chrono::steady_clock::duration::zero());
    co_await require_metric_range(
      kwaque::metric_id::broker_process_readiness,
      kwaque::metric_id::broker_start_time_seconds,
      1U);
    co_await require_registration_count("build_info", 1U);

    // The production composition must leave room under the scrape caps for
    // metrics later components add.
    const auto usage = co_await largest_snapshot_usage();
    std::cout << "per-shard metric families=" << usage.families
              << " enabled series=" << usage.series << '\n';
    BOOST_CHECK_LE(
      usage.families * 4U, kwaque::admin::metrics_snapshot_families * 3U);
    BOOST_CHECK_LE(
      usage.series * 4U, kwaque::admin::metrics_snapshot_series * 3U);

    co_await admin.stop();
    co_await require_metric_range(
      kwaque::metric_id::broker_process_readiness,
      kwaque::metric_id::broker_start_time_seconds,
      0U);
    co_await require_registration_count("build_info", 0U);
    co_await require_metric_range(
      kwaque::metric_id::task_active,
      kwaque::metric_id::task_abort_requests_total,
      2U);
    co_await require_metric_range(
      kwaque::metric_id::timer_active,
      kwaque::metric_id::dns_rejected_total,
      2U);

    co_await environments.stop();
    co_await require_metric_range(
      kwaque::metric_id::timer_active,
      kwaque::metric_id::dns_rejected_total,
      0U);
    co_await require_metric_range(
      kwaque::metric_id::task_active,
      kwaque::metric_id::task_abort_requests_total,
      0U);
    co_await require_metric_range(
      kwaque::metric_id::memory_configured_bytes,
      kwaque::metric_id::memory_waiters,
      0U);
    co_await registry.stop();
}

SEASTAR_TEST_CASE(admin_registration_failure_removes_partial_owner_metrics) {
    BOOST_REQUIRE_EQUAL(seastar::this_smp_shard_count(), 2U);
    seastar::sharded<admin_metric_blocker> blocker;
    co_await blocker.start();
    kwaque::admin::admin_server admin;
    std::exception_ptr failure;
    try {
        co_await admin.start("127.0.0.1", 0, 2);
    } catch (...) {
        failure = std::current_exception();
    }
    co_await admin.stop();
    // The admin group rolled back every metric it registered before the
    // blocked one; only the blocker remains.
    co_await require_registration_count("broker_process_readiness", 0U);
    co_await require_registration_count("broker_start_time_seconds", 1U);
    co_await blocker.stop();
    BOOST_REQUIRE(failure != nullptr);
    BOOST_CHECK_THROW(
      std::rethrow_exception(failure), seastar::metrics::double_registration);
    co_await require_registration_count("broker_start_time_seconds", 0U);
}

SEASTAR_TEST_CASE(admin_listener_failure_removes_routes_states_and_metrics) {
    BOOST_REQUIRE_EQUAL(seastar::this_smp_shard_count(), 2U);
    seastar::listen_options options;
    options.reuse_address = false;
    auto occupied = seastar::listen(
      seastar::socket_address{seastar::net::inet_address{"127.0.0.1"}, 0},
      options);
    const auto port = occupied.local_address().port();

    kwaque::admin::admin_server admin;
    std::exception_ptr failure;
    try {
        co_await admin.start("127.0.0.1", port, 2);
    } catch (...) {
        failure = std::current_exception();
    }
    co_await admin.stop();
    co_await require_registration_count("broker_process_readiness", 0U);
    co_await require_registration_count("build_info", 0U);
    occupied.abort_accept();
    BOOST_REQUIRE(failure != nullptr);
    BOOST_CHECK_THROW(std::rethrow_exception(failure), std::system_error);
}
