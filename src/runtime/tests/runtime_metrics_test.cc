#include "src/base/metric_schema.h"
#include "src/base/units.h"
#include "src/observability/event_identity.h"
#include "src/resource/resource_config.h"
#include "src/resource/resource_registry.h"
#include "src/runtime/production/environment.h"
#include "src/runtime/production/environment_test_support.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_api.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>

#include <boost/test/unit_test.hpp>

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>

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

bool registered(kwaque::metric_id id) {
    return seastar::metrics::impl::get_value_map().contains(full_name(id));
}

const seastar::metrics::impl::metric_family& family(kwaque::metric_id id) {
    const auto found = seastar::metrics::impl::get_value_map().find(
      full_name(id));
    if (found == seastar::metrics::impl::get_value_map().end()) {
        throw std::logic_error("metric family is not registered");
    }
    return found->second;
}

void require_exact_family(kwaque::metric_id id) {
    const auto& descriptor = metric_descriptor(id);
    const auto& registered_family = family(id);
    BOOST_CHECK(
      registered_family.info().type
      == (descriptor.kind == kwaque::metric_value_kind::gauge ? seastar::metrics::impl::data_type::GAUGE : seastar::metrics::impl::data_type::COUNTER));
    BOOST_CHECK(
      registered_family.info().d.str() == seastar::sstring{descriptor.help});
    BOOST_REQUIRE_EQUAL(registered_family.info().aggregate_labels.size(), 1U);
    BOOST_CHECK_EQUAL(
      registered_family.info().aggregate_labels.front(), "shard");
    BOOST_REQUIRE_EQUAL(registered_family.size(), 1U);
    const auto& labels = registered_family.begin()->first.labels();
    BOOST_REQUIRE_EQUAL(labels.size(), 1U);
    BOOST_CHECK(labels.contains("shard"));
}

std::uint64_t metric_value(kwaque::metric_id id) {
    const auto& registered_family = family(id);
    BOOST_REQUIRE_EQUAL(registered_family.size(), 1U);
    return registered_family.begin()->second->get_function()().ui();
}

kwaque::resource::resource_config resource_config() {
    auto configured = kwaque::resource::resource_config::from_total_memory(
      kwaque::byte_count{std::uint64_t{128} * 1'024U * 1'024U});
    BOOST_REQUIRE(configured.has_value());
    return *configured;
}

kwaque::observability::event_sink_identity event_identity(std::uint64_t value) {
    auto epoch = kwaque::observability::event_sink_epoch::make(value);
    BOOST_REQUIRE(epoch.has_value());
    return {
      .epoch = *epoch,
      .configuration_digest = {},
    };
}

seastar::logger& environment_logger() {
    static seastar::logger value{"kwaque-runtime-metrics-test"};
    return value;
}

} // namespace

SEASTAR_TEST_CASE(runtime_metric_inventory_registers_and_unregisters_by_owner) {
    kwaque::resource::resource_registry registry;
    co_await registry.start(resource_config());
    kwaque::runtime::production::environment environment{
      kwaque::runtime::production::environment_dependencies{
        registry.handles(), environment_logger(), event_identity(1)}};
    co_await environment.start();

    for (std::uint16_t value = static_cast<std::uint16_t>(
           kwaque::metric_id::task_active);
         value <= static_cast<std::uint16_t>(
           kwaque::metric_id::task_abort_requests_total);
         ++value) {
        require_exact_family(static_cast<kwaque::metric_id>(value));
    }
    for (std::uint16_t value = static_cast<std::uint16_t>(
           kwaque::metric_id::timer_active);
         value
         <= static_cast<std::uint16_t>(kwaque::metric_id::dns_rejected_total);
         ++value) {
        require_exact_family(static_cast<kwaque::metric_id>(value));
    }

    seastar::promise<> release;
    const auto accepted = environment.tasks().spawn(
      [&release] { return release.get_future(); });
    BOOST_REQUIRE(accepted.has_value());
    BOOST_CHECK_EQUAL(metric_value(kwaque::metric_id::task_active), 1U);
    BOOST_CHECK_EQUAL(metric_value(kwaque::metric_id::task_accepted_total), 1U);
    BOOST_CHECK_EQUAL(
      metric_value(kwaque::metric_id::task_completed_total), 0U);
    release.set_value();
    while (environment.tasks().task_count() != 0U) {
        co_await seastar::yield();
    }
    BOOST_CHECK_EQUAL(metric_value(kwaque::metric_id::task_active), 0U);
    BOOST_CHECK_EQUAL(
      metric_value(kwaque::metric_id::task_completed_total), 1U);

    environment.request_abort();
    BOOST_CHECK_EQUAL(
      metric_value(kwaque::metric_id::task_abort_requests_total), 1U);
    co_await environment.stop();
    for (std::uint16_t value = static_cast<std::uint16_t>(
           kwaque::metric_id::task_active);
         value
         <= static_cast<std::uint16_t>(kwaque::metric_id::dns_rejected_total);
         ++value) {
        BOOST_CHECK(!registered(static_cast<kwaque::metric_id>(value)));
    }
    co_await registry.stop();
}

SEASTAR_TEST_CASE(runtime_metric_partial_registration_rolls_back) {
    namespace metrics = seastar::metrics;
    const auto* blocked = kwaque::descriptor_for(
      kwaque::metric_id::file_accepted_total);
    std::optional<metrics::metric_groups> blocker;
    blocker.emplace();
    blocker->add_group(
      seastar::sstring{blocked->group},
      {metrics::make_counter(
        seastar::sstring{blocked->name},
        [] { return 0U; },
        metrics::description("Registration rollback blocker"))});

    kwaque::resource::resource_registry registry;
    co_await registry.start(resource_config());
    kwaque::runtime::production::environment environment{
      kwaque::runtime::production::environment_dependencies{
        registry.handles(), environment_logger(), event_identity(2)}};
    bool failed = false;
    try {
        co_await environment.start();
    } catch (const metrics::double_registration&) {
        failed = true;
    }
    BOOST_REQUIRE(failed);
    BOOST_CHECK(
      kwaque::runtime::production::environment_test_access::components_released(
        environment));
    for (std::uint16_t value = static_cast<std::uint16_t>(
           kwaque::metric_id::task_active);
         value
         <= static_cast<std::uint16_t>(kwaque::metric_id::dns_rejected_total);
         ++value) {
        const auto id = static_cast<kwaque::metric_id>(value);
        BOOST_CHECK(
          id == kwaque::metric_id::file_accepted_total || !registered(id));
    }

    co_await environment.stop();
    blocker.reset();
    BOOST_CHECK(!registered(kwaque::metric_id::file_accepted_total));
    co_await registry.stop();
}
