#include "src/base/metric_schema.h"
#include "src/runtime/fault.h"
#include "src/simulation/determinism_version.h"
#include "src/simulation/deterministic_random.h"
#include "src/simulation/event_trace.h"
#include "src/simulation/fake_dns.h"
#include "src/simulation/fake_file.h"
#include "src/simulation/fake_network.h"
#include "src/simulation/fault_schedule.h"
#include "src/simulation/metrics.h"
#include "src/simulation/scheduler.h"

#include <seastar/core/chunked_vector.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_api.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>

namespace {

constexpr std::uint64_t seed{47};

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
    const auto& registered = family(id);
    BOOST_CHECK(
      registered.info().type
      == (descriptor.kind == kwaque::metric_value_kind::gauge ? seastar::metrics::impl::data_type::GAUGE : seastar::metrics::impl::data_type::COUNTER));
    BOOST_CHECK(registered.info().d.str() == seastar::sstring{descriptor.help});
    BOOST_REQUIRE_EQUAL(registered.info().aggregate_labels.size(), 1U);
    BOOST_CHECK_EQUAL(registered.info().aggregate_labels.front(), "shard");
    BOOST_REQUIRE_EQUAL(registered.size(), 1U);
    const auto& labels = registered.begin()->first.labels();
    BOOST_REQUIRE_EQUAL(labels.size(), 1U);
    BOOST_CHECK(labels.contains("shard"));
}

std::uint64_t metric_value(kwaque::metric_id id) {
    const auto& registered = family(id);
    BOOST_REQUIRE_EQUAL(registered.size(), 1U);
    return registered.begin()->second->get_function()().ui();
}

kwaque::simulation::scheduler_limits scheduler_budget() {
    auto made = kwaque::simulation::scheduler_limits::make(
      kwaque::simulation::scheduler_limit_values{
        .pending_events = 512,
        .events_per_pump = 512,
        .total_events = 2'048,
        .maximum_deadline = kwaque::runtime::monotonic_time{1'000'000'000},
      });
    BOOST_REQUIRE(made.has_value());
    return *made;
}

kwaque::simulation::trace_limits trace_budget() {
    auto made = kwaque::simulation::trace_limits::make(
      kwaque::simulation::trace_limit_values{
        .entries = 512,
        .encoded_bytes = kwaque::simulation::canonical_header_encoded_size
                         + 512U
                             * kwaque::simulation::canonical_entry_encoded_size,
        .line_bytes = 1'024,
      });
    BOOST_REQUIRE(made.has_value());
    return *made;
}

kwaque::simulation::fake_network_config network_config() {
    auto config = kwaque::simulation::fake_network_config{};
    config.maximum_listeners = 2;
    config.maximum_connection_pairs = 2;
    config.maximum_pending_connects = 2;
    config.maximum_backlog_entries = 2;
    config.maximum_operations = 8;
    config.maximum_parked_operations = 2;
    config.maximum_direction_bytes = kwaque::byte_count{64 * 1'024U};
    config.maximum_packets = 16;
    config.maximum_packet_logical_bytes = kwaque::byte_count{1'024 * 1'024U};
    config.maximum_packet_retained_bytes = kwaque::byte_count{1'024 * 1'024U};
    config.maximum_direction_packets = 8;
    config.maximum_links = 4;
    config.maximum_address_entries = 8;
    config.maximum_active_flows = 4;
    config.maximum_controls = 4;
    config.stop_batch = 8;
    return config;
}

} // namespace

SEASTAR_TEST_CASE(simulation_metrics_register_exact_fixed_inventory) {
    const auto scheduler_limits = scheduler_budget();
    const auto trace_limits = trace_budget();
    kwaque::simulation::event_trace trace{
      kwaque::simulation::trace_header::current(
        seed,
        kwaque::simulation::deterministic_random_algorithm_version,
        kwaque::simulation::deterministic_random_coordinate_version,
        kwaque::simulation::trace_scheduler_budget{
          .pending_events = scheduler_limits.pending_events(),
          .events_per_pump = scheduler_limits.events_per_pump(),
          .total_events = scheduler_limits.total_events(),
          .maximum_deadline = scheduler_limits.maximum_deadline().nanoseconds(),
        },
        trace_limits,
        kwaque::simulation::trace_digest{},
        kwaque::simulation::trace_digest{}),
      trace_limits};
    kwaque::simulation::scheduler events{scheduler_limits, &trace};
    auto made_faults = kwaque::simulation::fault_schedule::make(
      events,
      trace,
      seed,
      seastar::chunked_vector<kwaque::simulation::fault_rule>{});
    BOOST_REQUIRE(made_faults.has_value());
    auto faults = std::move(*made_faults);
    auto made_files = kwaque::simulation::fake_file_system::make(
      {}, events, *faults);
    BOOST_REQUIRE(made_files.has_value());
    auto files = std::move(*made_files);
    auto made_network = kwaque::simulation::fake_network::make(
      network_config(), events, faults.get());
    BOOST_REQUIRE(made_network.has_value());
    auto network = std::move(*made_network);
    auto made_dns = kwaque::simulation::fake_dns::make(
      {}, events, faults.get());
    BOOST_REQUIRE(made_dns.has_value());
    auto dns = std::move(*made_dns);

    kwaque::simulation::simulation_metrics metrics{
      events, trace, *faults, *files, *network, *dns};
    metrics.start();
    BOOST_CHECK_THROW(metrics.start(), std::logic_error);
    for (std::uint16_t value = static_cast<std::uint16_t>(
           kwaque::metric_id::scheduler_pending_events);
         value
         <= static_cast<std::uint16_t>(kwaque::metric_id::fake_dns_active);
         ++value) {
        require_exact_family(static_cast<kwaque::metric_id>(value));
    }

    const auto occurrence = kwaque::runtime::fault_occurrence::make(1);
    BOOST_REQUIRE(occurrence.has_value());
    const auto prepared = faults->prepare(
      kwaque::runtime::fault_request{
        .point = kwaque::runtime::descriptor_for(
                   kwaque::runtime::builtin_fault_point::timer)
                   ->id,
        .occurrence = *occurrence,
        .object = kwaque::runtime::fault_object_key::none(),
      });
    BOOST_REQUIRE(prepared.has_value());
    BOOST_CHECK_EQUAL(faults->evaluations(), 1U);
    BOOST_CHECK_EQUAL(
      metric_value(kwaque::metric_id::fault_evaluations_total), 1U);
    BOOST_CHECK_EQUAL(
      metric_value(kwaque::metric_id::fault_decisions_applied_total), 0U);

    metrics.stop();
    metrics.stop();
    for (std::uint16_t value = static_cast<std::uint16_t>(
           kwaque::metric_id::scheduler_pending_events);
         value
         <= static_cast<std::uint16_t>(kwaque::metric_id::fake_dns_active);
         ++value) {
        BOOST_CHECK(!seastar::metrics::impl::get_value_map().contains(
          full_name(static_cast<kwaque::metric_id>(value))));
    }
    co_return;
}
