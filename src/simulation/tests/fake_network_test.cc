#include "src/runtime/testing/contracts/network_contract.h"
#include "src/simulation/determinism_version.h"
#include "src/simulation/event_trace.h"
#include "src/simulation/fake_network.h"
#include "src/simulation/fault_schedule.h"
#include "src/simulation/scheduler.h"
#include "src/simulation/tests/network_oracle.h"

#include <seastar/core/chunked_vector.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>

namespace {

kwaque::simulation::scheduler_limits scheduler_limits() {
    auto limits = kwaque::simulation::scheduler_limits::make(
      kwaque::simulation::scheduler_limit_values{
        .pending_events = 8'192,
        .events_per_pump = 4'096,
        .total_events = 100'000,
        .maximum_deadline = kwaque::runtime::monotonic_time{10'000'000'000ULL},
      });
    BOOST_REQUIRE(limits.has_value());
    return *limits;
}

kwaque::simulation::scheduler_limits bandwidth_scheduler_limits() {
    auto limits = kwaque::simulation::scheduler_limits::make(
      kwaque::simulation::scheduler_limit_values{
        .pending_events = 8'192,
        .events_per_pump = 4'096,
        .total_events = 100'000,
        .maximum_deadline = kwaque::runtime::monotonic_time{20'000'000'000ULL},
      });
    BOOST_REQUIRE(limits.has_value());
    return *limits;
}

kwaque::simulation::trace_limits network_trace_limits() {
    auto limits = kwaque::simulation::trace_limits::make(
      kwaque::simulation::trace_limit_values{
        .entries = 8'192,
        .encoded_bytes = kwaque::simulation::canonical_header_encoded_size
                         + 8'192U
                             * kwaque::simulation::canonical_entry_encoded_size,
        .line_bytes = 1'024,
      });
    BOOST_REQUIRE(limits.has_value());
    return *limits;
}

kwaque::simulation::trace_header network_trace_header(
  kwaque::simulation::scheduler_limits scheduler_budget,
  kwaque::simulation::trace_limits trace_budget) {
    return kwaque::simulation::trace_header::current(
      17,
      kwaque::simulation::deterministic_random_algorithm_version,
      kwaque::simulation::deterministic_random_coordinate_version,
      kwaque::simulation::trace_scheduler_budget{
        .pending_events = scheduler_budget.pending_events(),
        .events_per_pump = scheduler_budget.events_per_pump(),
        .total_events = scheduler_budget.total_events(),
        .maximum_deadline = scheduler_budget.maximum_deadline().nanoseconds(),
      },
      trace_budget,
      kwaque::simulation::trace_digest{},
      kwaque::simulation::trace_digest{});
}

kwaque::simulation::fault_rule network_write_rule(
  std::uint64_t id,
  std::uint64_t occurrence,
  kwaque::runtime::fault_decision decision) {
    auto rule_id = kwaque::simulation::fault_rule_id::make(id);
    auto selected = kwaque::runtime::fault_occurrence::make(occurrence);
    BOOST_REQUIRE(rule_id.has_value());
    BOOST_REQUIRE(selected.has_value());
    auto rule = kwaque::simulation::fault_rule::make(
      *rule_id,
      kwaque::runtime::builtin_fault_point::network_write,
      std::nullopt,
      *selected,
      *selected,
      kwaque::simulation::fault_selector::once(),
      decision);
    BOOST_REQUIRE(rule.has_value());
    return *rule;
}

template<typename T>
seastar::future<>
pump_until(kwaque::simulation::scheduler& events, seastar::future<T>& waiting) {
    while (!waiting.available()) {
        co_await seastar::yield();
        if (waiting.available()) {
            continue;
        }
        if (events.pending_events() == 0U) {
            continue;
        }
        if (!events.has_ready_events()) {
            const auto advanced = events.advance_to_next();
            BOOST_REQUIRE(advanced.has_value());
            BOOST_REQUIRE(advanced->has_value());
        }
        const auto ran = events.run_ready();
        BOOST_REQUIRE(ran.has_value());
    }
}

template<typename Future>
seastar::future<> require_ready_success(Future& waiting) {
    const auto result = co_await std::move(waiting);
    BOOST_REQUIRE(result.has_value());
}

template<typename Function>
seastar::future<> run_shared_case(Function function) {
    kwaque::simulation::scheduler events{scheduler_limits()};
    auto config = kwaque::simulation::fake_network_config{};
    config.egress_capacity = kwaque::simulation::bandwidth_capacity::finite(
      1U << 30U);
    auto made = kwaque::simulation::fake_network::make(config, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);
    auto contract = function(*network);
    co_await pump_until(events, contract);
    co_await std::move(contract);
}

seastar::future<> run_traced_round_trip(
  kwaque::simulation::event_trace& trace,
  kwaque::simulation::scheduler_limits limits) {
    kwaque::simulation::scheduler events{limits, &trace};
    auto made = kwaque::simulation::fake_network::make({}, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);
    auto contract
      = kwaque::runtime::testing::network_contract_detail::round_trip(*network);
    co_await pump_until(events, contract);
    co_await std::move(contract);
}

constexpr auto loopback = kwaque::runtime::network_address::ipv4(
  {std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}});
constexpr auto alternate = kwaque::runtime::network_address::ipv4(
  {std::byte{127}, std::byte{0}, std::byte{0}, std::byte{2}});
constexpr auto third = kwaque::runtime::network_address::ipv4(
  {std::byte{127}, std::byte{0}, std::byte{0}, std::byte{3}});
constexpr auto wildcard = kwaque::runtime::network_address::ipv4(
  {std::byte{}, std::byte{}, std::byte{}, std::byte{}});

} // namespace

SEASTAR_TEST_CASE(fake_network_configuration_checks_bounds_and_scheduler_fit) {
    kwaque::simulation::scheduler events{scheduler_limits()};
    auto valid = kwaque::simulation::fake_network::make({}, events);
    BOOST_REQUIRE(valid.has_value());
    valid->reset();

    auto invalid_range = kwaque::simulation::fake_network_config{};
    invalid_range.ephemeral_first = 0;
    BOOST_CHECK(!kwaque::simulation::fake_network::make(invalid_range, events)
                   .has_value());

    auto inverted_range = kwaque::simulation::fake_network_config{};
    inverted_range.ephemeral_first = 60'000;
    inverted_range.ephemeral_last = 50'000;
    BOOST_CHECK(!kwaque::simulation::fake_network::make(inverted_range, events)
                   .has_value());

    auto oversized = kwaque::simulation::fake_network_config{};
    oversized.maximum_listeners
      = kwaque::simulation::maximum_fake_network_listeners + 1U;
    BOOST_CHECK(
      !kwaque::simulation::fake_network::make(oversized, events).has_value());

    auto oversized_packets = kwaque::simulation::fake_network_config{};
    oversized_packets.maximum_packets
      = kwaque::simulation::maximum_fake_network_packets + 1U;
    BOOST_CHECK(
      !kwaque::simulation::fake_network::make(oversized_packets, events)
         .has_value());

    auto oversized_flows = kwaque::simulation::fake_network_config{};
    oversized_flows.maximum_active_flows
      = kwaque::simulation::maximum_bandwidth_flows + 1U;
    BOOST_CHECK(!kwaque::simulation::fake_network::make(oversized_flows, events)
                   .has_value());

    auto deadline_incoherent = kwaque::simulation::fake_network_config{};
    deadline_incoherent.egress_capacity
      = kwaque::simulation::bandwidth_capacity::finite(1);
    BOOST_CHECK(
      !kwaque::simulation::fake_network::make(deadline_incoherent, events)
         .has_value());
    co_return;
}

SEASTAR_TEST_CASE(
  fake_network_bind_registry_is_transactional_and_identity_safe) {
    kwaque::simulation::scheduler events{scheduler_limits()};
    auto made = kwaque::simulation::fake_network::make({}, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);

    auto binding = network->listen(
      kwaque::runtime::network_endpoint{wildcard, 31'000}, {});
    BOOST_CHECK(!binding.available());
    co_await pump_until(events, binding);
    auto bound = co_await std::move(binding);
    BOOST_REQUIRE(bound.has_value());
    auto listener = std::move(*bound);

    const auto duplicate = co_await network->listen(
      kwaque::runtime::network_endpoint{loopback, 31'000}, {});
    BOOST_REQUIRE(!duplicate.has_value());
    BOOST_CHECK(duplicate.error().code() == kwaque::errc::network_failure);

    auto closing = listener.close();
    BOOST_CHECK(!closing.available());
    co_await pump_until(events, closing);
    co_await require_ready_success(closing);

    auto rebinding = network->listen(
      kwaque::runtime::network_endpoint{loopback, 31'000}, {});
    co_await pump_until(events, rebinding);
    auto rebound = co_await std::move(rebinding);
    BOOST_REQUIRE(rebound.has_value());
    auto replacement = std::move(*rebound);

    auto other_binding = network->listen(
      kwaque::runtime::network_endpoint{alternate, 31'000}, {});
    co_await pump_until(events, other_binding);
    auto other_bound = co_await std::move(other_binding);
    BOOST_REQUIRE(other_bound.has_value());
    auto other_listener = std::move(*other_bound);

    auto replacement_close = replacement.close();
    auto other_close = other_listener.close();
    co_await pump_until(events, replacement_close);
    co_await pump_until(events, other_close);
    co_await require_ready_success(replacement_close);
    co_await require_ready_success(other_close);
    co_return;
}

SEASTAR_TEST_CASE(fake_network_connect_and_accept_are_separate_pumped_events) {
    kwaque::simulation::scheduler events{scheduler_limits()};
    auto made = kwaque::simulation::fake_network::make({}, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);
    auto binding = network->listen(
      kwaque::runtime::network_endpoint{loopback, 0}, {});
    co_await pump_until(events, binding);
    auto bound = co_await std::move(binding);
    BOOST_REQUIRE(bound.has_value());
    auto listener = std::move(*bound);

    seastar::abort_source accept_abort;
    seastar::abort_source connect_abort;
    auto accepting = listener.accept(accept_abort);
    auto connecting = network->connect(
      listener.local_endpoint(),
      std::nullopt,
      kwaque::runtime::network_connection_limits{},
      connect_abort);
    BOOST_CHECK(!accepting.available());
    BOOST_CHECK(!connecting.available());
    co_await pump_until(events, connecting);
    auto connected = co_await std::move(connecting);
    BOOST_REQUIRE(connected.has_value());
    co_await pump_until(events, accepting);
    auto accepted = co_await std::move(accepting);
    BOOST_REQUIRE(accepted.has_value());
    auto client = std::move(*connected);
    auto server = std::move(*accepted);
    BOOST_CHECK(client.remote_endpoint() == listener.local_endpoint());
    BOOST_CHECK(client.local_endpoint() == server.remote_endpoint());

    auto client_close = client.close();
    auto server_close = server.close();
    auto listener_close = listener.close();
    co_await pump_until(events, client_close);
    co_await pump_until(events, server_close);
    co_await pump_until(events, listener_close);
    co_await require_ready_success(client_close);
    co_await require_ready_success(server_close);
    co_await require_ready_success(listener_close);
    co_return;
}

SEASTAR_TEST_CASE(
  fake_network_shares_ephemeral_space_and_aborts_connect_promptly) {
    kwaque::simulation::scheduler events{scheduler_limits()};
    auto config = kwaque::simulation::fake_network_config{};
    config.ephemeral_first = 50'000;
    config.ephemeral_last = 50'000;
    auto made = kwaque::simulation::fake_network::make(config, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);
    auto binding = network->listen(
      kwaque::runtime::network_endpoint{loopback, 32'000}, {});
    co_await pump_until(events, binding);
    auto bound = co_await std::move(binding);
    BOOST_REQUIRE(bound.has_value());
    auto listener = std::move(*bound);

    seastar::abort_source accept_abort;
    seastar::abort_source connect_abort;
    auto accepting = listener.accept(accept_abort);
    auto connecting = network->connect(
      listener.local_endpoint(),
      std::nullopt,
      kwaque::runtime::network_connection_limits{},
      connect_abort);
    co_await pump_until(events, connecting);
    auto connected = co_await std::move(connecting);
    BOOST_REQUIRE(connected.has_value());
    co_await pump_until(events, accepting);
    auto accepted = co_await std::move(accepting);
    BOOST_REQUIRE(accepted.has_value());
    auto client = std::move(*connected);
    auto server = std::move(*accepted);
    BOOST_CHECK(client.local_endpoint().port() == 50'000U);

    seastar::abort_source exhausted_abort;
    const auto exhausted = co_await network->connect(
      listener.local_endpoint(),
      std::nullopt,
      kwaque::runtime::network_connection_limits{},
      exhausted_abort);
    BOOST_REQUIRE(!exhausted.has_value());
    BOOST_CHECK(exhausted.error().code() == kwaque::errc::resource_exhausted);

    auto client_close = client.close();
    auto server_close = server.close();
    co_await pump_until(events, client_close);
    co_await pump_until(events, server_close);
    co_await require_ready_success(client_close);
    co_await require_ready_success(server_close);

    seastar::abort_source canceled_source;
    auto canceled_connect = network->connect(
      listener.local_endpoint(),
      kwaque::runtime::network_endpoint{alternate, 40'000},
      kwaque::runtime::network_connection_limits{},
      canceled_source);
    BOOST_CHECK(!canceled_connect.available());
    canceled_source.request_abort();
    co_await pump_until(events, canceled_connect);
    const auto canceled = co_await std::move(canceled_connect);
    BOOST_REQUIRE(!canceled.has_value());
    BOOST_CHECK(canceled.error().code() == kwaque::errc::aborted);

    auto listener_close = listener.close();
    co_await pump_until(events, listener_close);
    co_await require_ready_success(listener_close);
    co_return;
}

SEASTAR_TEST_CASE(
  fake_network_close_rebind_does_not_retarget_captured_connect) {
    kwaque::simulation::scheduler events{scheduler_limits()};
    auto config = kwaque::simulation::fake_network_config{};
    config.connect_latency = kwaque::runtime::monotonic_duration{10};
    config.incoming_latency = kwaque::runtime::monotonic_duration{10};
    auto made = kwaque::simulation::fake_network::make(config, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);
    const kwaque::runtime::network_endpoint endpoint{loopback, 33'000};
    auto binding = network->listen(endpoint, {});
    co_await pump_until(events, binding);
    auto bound = co_await std::move(binding);
    BOOST_REQUIRE(bound.has_value());
    auto original = std::move(*bound);

    seastar::abort_source connect_abort;
    auto connecting = network->connect(
      endpoint,
      std::nullopt,
      kwaque::runtime::network_connection_limits{},
      connect_abort);
    auto closing = original.close();
    co_await pump_until(events, closing);
    co_await require_ready_success(closing);

    auto rebinding = network->listen(endpoint, {});
    co_await pump_until(events, rebinding);
    auto rebound = co_await std::move(rebinding);
    BOOST_REQUIRE(rebound.has_value());
    auto replacement = std::move(*rebound);

    co_await pump_until(events, connecting);
    const auto rejected = co_await std::move(connecting);
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == kwaque::errc::network_failure);

    auto replacement_close = replacement.close();
    co_await pump_until(events, replacement_close);
    co_await require_ready_success(replacement_close);
    co_return;
}

SEASTAR_TEST_CASE(fake_network_packet_ownership_outlives_write_admission) {
    kwaque::simulation::scheduler events{scheduler_limits()};
    auto config = kwaque::simulation::fake_network_config{};
    config.maximum_packets = 1;
    config.maximum_direction_packets = 1;
    config.maximum_active_flows = 1;
    config.latency_min = kwaque::runtime::monotonic_duration{10};
    config.latency_mean_parameter = config.latency_min;
    auto made = kwaque::simulation::fake_network::make(config, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);

    auto binding = network->listen(
      kwaque::runtime::network_endpoint{loopback, 0}, {});
    co_await pump_until(events, binding);
    auto bound = co_await std::move(binding);
    BOOST_REQUIRE(bound.has_value());
    auto listener = std::move(*bound);
    seastar::abort_source accept_abort;
    seastar::abort_source connect_abort;
    auto accepting = listener.accept(accept_abort);
    auto connecting = network->connect(
      listener.local_endpoint(),
      std::nullopt,
      kwaque::runtime::network_connection_limits{},
      connect_abort);
    co_await pump_until(events, connecting);
    auto connected = co_await std::move(connecting);
    BOOST_REQUIRE(connected.has_value());
    co_await pump_until(events, accepting);
    auto accepted = co_await std::move(accepting);
    BOOST_REQUIRE(accepted.has_value());
    auto client = std::move(*connected);
    auto server = std::move(*accepted);

    seastar::abort_source write_abort;
    auto first = client.write(
      kwaque::runtime::testing::network_contract_detail::make_bytes("first"),
      write_abort);
    co_await pump_until(events, first);
    co_await require_ready_success(first);

    seastar::abort_source rejected_abort;
    auto rejected = client.write(
      kwaque::runtime::testing::network_contract_detail::make_bytes("second"),
      rejected_abort);
    BOOST_REQUIRE(rejected.available());
    const auto saturated = rejected.get();
    BOOST_REQUIRE(!saturated.has_value());
    BOOST_CHECK(saturated.error().code() == kwaque::errc::queue_full);

    seastar::abort_source read_abort;
    auto reading = server.read(kwaque::byte_count{5}, read_abort);
    co_await pump_until(events, reading);
    auto received = co_await std::move(reading);
    BOOST_REQUIRE(received.has_value());
    BOOST_CHECK(received->data().content_equals("first"));

    auto third_write = client.write(
      kwaque::runtime::testing::network_contract_detail::make_bytes("third"),
      write_abort);
    BOOST_CHECK(!third_write.available());
    co_await pump_until(events, third_write);
    co_await require_ready_success(third_write);
    auto third_read = server.read(kwaque::byte_count{5}, read_abort);
    co_await pump_until(events, third_read);
    auto third_received = co_await std::move(third_read);
    BOOST_REQUIRE(third_received.has_value());
    BOOST_CHECK(third_received->data().content_equals("third"));

    auto client_close = client.close();
    auto server_close = server.close();
    auto listener_close = listener.close();
    co_await pump_until(events, client_close);
    co_await pump_until(events, server_close);
    co_await pump_until(events, listener_close);
    co_await require_ready_success(client_close);
    co_await require_ready_success(server_close);
    co_await require_ready_success(listener_close);
}

SEASTAR_TEST_CASE(
  fake_network_retained_backing_pressure_rejects_before_packet_ownership) {
    kwaque::simulation::scheduler events{scheduler_limits()};
    auto config = kwaque::simulation::fake_network_config{};
    config.maximum_packets = 4;
    config.maximum_direction_packets = 4;
    config.maximum_active_flows = 1;
    config.maximum_direction_bytes = kwaque::byte_count{64};
    config.maximum_packet_logical_bytes = kwaque::byte_count{64};
    config.maximum_packet_retained_bytes = kwaque::byte_count{16};
    config.latency_min = kwaque::runtime::monotonic_duration{20};
    config.latency_mean_parameter = config.latency_min;
    auto made = kwaque::simulation::fake_network::make(config, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);

    auto binding = network->listen(
      kwaque::runtime::network_endpoint{loopback, 0}, {});
    co_await pump_until(events, binding);
    auto bound = co_await std::move(binding);
    BOOST_REQUIRE(bound.has_value());
    auto listener = std::move(*bound);
    seastar::abort_source accept_abort;
    seastar::abort_source connect_abort;
    auto accepting = listener.accept(accept_abort);
    auto connecting = network->connect(
      listener.local_endpoint(),
      std::nullopt,
      kwaque::runtime::network_connection_limits{},
      connect_abort);
    co_await pump_until(events, connecting);
    co_await pump_until(events, accepting);
    auto connected = co_await std::move(connecting);
    auto accepted = co_await std::move(accepting);
    BOOST_REQUIRE(connected.has_value());
    BOOST_REQUIRE(accepted.has_value());
    auto client = std::move(*connected);
    auto server = std::move(*accepted);

    auto first_backing
      = kwaque::runtime::testing::network_contract_detail::make_bytes(
        std::string(16, 'a'));
    auto first_slice = first_backing.share(
      kwaque::byte_count{}, kwaque::byte_count{1});
    BOOST_REQUIRE(first_slice.has_value());
    BOOST_CHECK_EQUAL(first_slice->size().value(), 1U);
    BOOST_CHECK_EQUAL(first_slice->retained_bytes().value(), 16U);
    seastar::abort_source write_abort;
    auto first = client.write(std::move(*first_slice), write_abort);
    co_await pump_until(events, first);
    co_await require_ready_success(first);

    auto rejected_backing
      = kwaque::runtime::testing::network_contract_detail::make_bytes(
        std::string(16, 'b'));
    auto rejected_slice = rejected_backing.share(
      kwaque::byte_count{}, kwaque::byte_count{1});
    BOOST_REQUIRE(rejected_slice.has_value());
    auto rejected = client.write(std::move(*rejected_slice), write_abort);
    BOOST_REQUIRE(rejected.available());
    const auto saturated = rejected.get();
    BOOST_REQUIRE(!saturated.has_value());
    BOOST_CHECK(saturated.error().code() == kwaque::errc::queue_full);

    seastar::abort_source read_abort;
    auto reading = server.read(kwaque::byte_count{1}, read_abort);
    co_await pump_until(events, reading);
    auto received = co_await std::move(reading);
    BOOST_REQUIRE(received.has_value());
    BOOST_CHECK(received->data().content_equals("a"));

    auto admitted_backing
      = kwaque::runtime::testing::network_contract_detail::make_bytes(
        std::string(16, 'c'));
    auto admitted_slice = admitted_backing.share(
      kwaque::byte_count{}, kwaque::byte_count{1});
    BOOST_REQUIRE(admitted_slice.has_value());
    auto admitted = client.write(std::move(*admitted_slice), write_abort);
    BOOST_CHECK(!admitted.available());
    co_await pump_until(events, admitted);
    co_await require_ready_success(admitted);
    auto final_read = server.read(kwaque::byte_count{1}, read_abort);
    co_await pump_until(events, final_read);
    auto final_result = co_await std::move(final_read);
    BOOST_REQUIRE(final_result.has_value());
    BOOST_CHECK(final_result->data().content_equals("c"));

    auto client_close = client.close();
    auto server_close = server.close();
    auto listener_close = listener.close();
    co_await pump_until(events, client_close);
    co_await pump_until(events, server_close);
    co_await pump_until(events, listener_close);
    co_await require_ready_success(client_close);
    co_await require_ready_success(server_close);
    co_await require_ready_success(listener_close);
}

SEASTAR_TEST_CASE(fake_network_progressively_shares_source_egress) {
    kwaque::simulation::scheduler events{bandwidth_scheduler_limits()};
    auto config = kwaque::simulation::fake_network_config{};
    config.egress_capacity = kwaque::simulation::bandwidth_capacity::finite(
      1'000);
    config.maximum_active_flows = 2;
    config.maximum_direction_bytes = kwaque::byte_count{2'000};
    config.latency_min = kwaque::runtime::monotonic_duration{500'000'000};
    config.latency_mean_parameter = config.latency_min;
    auto made = kwaque::simulation::fake_network::make(config, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);

    auto binding_a = network->listen(
      kwaque::runtime::network_endpoint{alternate, 0}, {});
    auto binding_b = network->listen(
      kwaque::runtime::network_endpoint{third, 0}, {});
    co_await pump_until(events, binding_a);
    co_await pump_until(events, binding_b);
    auto bound_a = co_await std::move(binding_a);
    auto bound_b = co_await std::move(binding_b);
    BOOST_REQUIRE(bound_a.has_value());
    BOOST_REQUIRE(bound_b.has_value());
    auto listener_a = std::move(*bound_a);
    auto listener_b = std::move(*bound_b);

    seastar::abort_source accept_abort_a;
    seastar::abort_source accept_abort_b;
    seastar::abort_source connect_abort_a;
    seastar::abort_source connect_abort_b;
    auto accepting_a = listener_a.accept(accept_abort_a);
    auto accepting_b = listener_b.accept(accept_abort_b);
    auto connecting_a = network->connect(
      listener_a.local_endpoint(),
      std::nullopt,
      kwaque::runtime::network_connection_limits{},
      connect_abort_a);
    auto connecting_b = network->connect(
      listener_b.local_endpoint(),
      std::nullopt,
      kwaque::runtime::network_connection_limits{},
      connect_abort_b);
    co_await pump_until(events, connecting_a);
    co_await pump_until(events, connecting_b);
    co_await pump_until(events, accepting_a);
    co_await pump_until(events, accepting_b);
    auto connected_a = co_await std::move(connecting_a);
    auto connected_b = co_await std::move(connecting_b);
    auto accepted_a = co_await std::move(accepting_a);
    auto accepted_b = co_await std::move(accepting_b);
    BOOST_REQUIRE(connected_a.has_value());
    BOOST_REQUIRE(connected_b.has_value());
    BOOST_REQUIRE(accepted_a.has_value());
    BOOST_REQUIRE(accepted_b.has_value());
    auto client_a = std::move(*connected_a);
    auto client_b = std::move(*connected_b);
    auto server_a = std::move(*accepted_a);
    auto server_b = std::move(*accepted_b);

    seastar::abort_source write_abort_a;
    seastar::abort_source write_abort_b;
    const auto started = events.now();
    auto writing_a = client_a.write(
      kwaque::runtime::testing::network_contract_detail::repeated_bytes(
        1'000, 'a'),
      write_abort_a);
    auto writing_b = client_b.write(
      kwaque::runtime::testing::network_contract_detail::repeated_bytes(
        1'000, 'b'),
      write_abort_b);
    BOOST_CHECK(!writing_a.available());
    BOOST_CHECK(!writing_b.available());

    auto next = events.advance_to_next();
    BOOST_REQUIRE(next.has_value());
    BOOST_REQUIRE(next->has_value());
    auto elapsed = (**next).checked_elapsed_since(started);
    BOOST_REQUIRE(elapsed.has_value());
    BOOST_CHECK_EQUAL(elapsed->nanoseconds(), 2'000'000'000ULL);
    BOOST_REQUIRE(events.run_ready().has_value());
    co_await seastar::yield();
    BOOST_REQUIRE(writing_a.available());
    BOOST_REQUIRE(writing_b.available());
    co_await require_ready_success(writing_a);
    co_await require_ready_success(writing_b);

    seastar::abort_source read_abort_a;
    seastar::abort_source read_abort_b;
    auto reading_a = server_a.read(kwaque::byte_count{1'000}, read_abort_a);
    auto reading_b = server_b.read(kwaque::byte_count{1'000}, read_abort_b);
    BOOST_CHECK(!reading_a.available());
    BOOST_CHECK(!reading_b.available());
    next = events.advance_to_next();
    BOOST_REQUIRE(next.has_value());
    BOOST_REQUIRE(next->has_value());
    elapsed = (**next).checked_elapsed_since(started);
    BOOST_REQUIRE(elapsed.has_value());
    BOOST_CHECK_EQUAL(elapsed->nanoseconds(), 2'500'000'000ULL);
    BOOST_REQUIRE(events.run_ready().has_value());
    co_await seastar::yield();
    auto received_a = co_await std::move(reading_a);
    auto received_b = co_await std::move(reading_b);
    BOOST_REQUIRE(received_a.has_value());
    BOOST_REQUIRE(received_b.has_value());
    BOOST_CHECK(received_a->data().size() == kwaque::byte_count{1'000});
    BOOST_CHECK(received_b->data().size() == kwaque::byte_count{1'000});

    auto client_close_a = client_a.close();
    auto client_close_b = client_b.close();
    auto server_close_a = server_a.close();
    auto server_close_b = server_b.close();
    auto listener_close_a = listener_a.close();
    auto listener_close_b = listener_b.close();
    co_await pump_until(events, client_close_a);
    co_await pump_until(events, client_close_b);
    co_await pump_until(events, server_close_a);
    co_await pump_until(events, server_close_b);
    co_await pump_until(events, listener_close_a);
    co_await pump_until(events, listener_close_b);
    co_await require_ready_success(client_close_a);
    co_await require_ready_success(client_close_b);
    co_await require_ready_success(server_close_a);
    co_await require_ready_success(server_close_b);
    co_await require_ready_success(listener_close_a);
    co_await require_ready_success(listener_close_b);
}

SEASTAR_TEST_CASE(fake_network_interframe_gap_serializes_direction_fifo) {
    kwaque::simulation::scheduler events{bandwidth_scheduler_limits()};
    auto config = kwaque::simulation::fake_network_config{};
    config.egress_capacity = kwaque::simulation::bandwidth_capacity::finite(
      1'000);
    config.maximum_active_flows = 1;
    config.maximum_direction_bytes = kwaque::byte_count{2'000};
    config.latency_min = kwaque::runtime::monotonic_duration{};
    config.latency_mean_parameter = config.latency_min;
    config.interframe_gap = kwaque::runtime::monotonic_duration{1'000'000'000};
    auto made = kwaque::simulation::fake_network::make(config, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);
    auto binding = network->listen(
      kwaque::runtime::network_endpoint{alternate, 0}, {});
    co_await pump_until(events, binding);
    auto bound = co_await std::move(binding);
    BOOST_REQUIRE(bound.has_value());
    auto listener = std::move(*bound);
    seastar::abort_source accept_abort;
    seastar::abort_source connect_abort;
    auto accepting = listener.accept(accept_abort);
    auto connecting = network->connect(
      listener.local_endpoint(),
      std::nullopt,
      kwaque::runtime::network_connection_limits{},
      connect_abort);
    co_await pump_until(events, connecting);
    co_await pump_until(events, accepting);
    auto connected = co_await std::move(connecting);
    auto accepted = co_await std::move(accepting);
    BOOST_REQUIRE(connected.has_value());
    BOOST_REQUIRE(accepted.has_value());
    auto client = std::move(*connected);
    auto server = std::move(*accepted);

    seastar::abort_source write_abort;
    const auto started = events.now();
    auto first = client.write(
      kwaque::runtime::testing::network_contract_detail::repeated_bytes(
        1'000, 'a'),
      write_abort);
    auto second = client.write(
      kwaque::runtime::testing::network_contract_detail::repeated_bytes(
        1'000, 'b'),
      write_abort);
    auto next = events.advance_to_next();
    BOOST_REQUIRE(next.has_value() && next->has_value());
    BOOST_REQUIRE(events.run_ready().has_value());
    co_await seastar::yield();
    BOOST_REQUIRE(first.available());
    BOOST_CHECK(!second.available());
    auto elapsed = events.now().checked_elapsed_since(started);
    BOOST_REQUIRE(elapsed.has_value());
    BOOST_CHECK_EQUAL(elapsed->nanoseconds(), 1'000'000'000ULL);

    next = events.advance_to_next();
    BOOST_REQUIRE(next.has_value() && next->has_value());
    BOOST_REQUIRE(events.run_ready().has_value());
    co_await seastar::yield();
    BOOST_CHECK(!second.available());
    elapsed = events.now().checked_elapsed_since(started);
    BOOST_REQUIRE(elapsed.has_value());
    BOOST_CHECK_EQUAL(elapsed->nanoseconds(), 2'000'000'000ULL);

    next = events.advance_to_next();
    BOOST_REQUIRE(next.has_value() && next->has_value());
    BOOST_REQUIRE(events.run_ready().has_value());
    co_await seastar::yield();
    BOOST_REQUIRE(second.available());
    elapsed = events.now().checked_elapsed_since(started);
    BOOST_REQUIRE(elapsed.has_value());
    BOOST_CHECK_EQUAL(elapsed->nanoseconds(), 3'000'000'000ULL);
    co_await require_ready_success(first);
    co_await require_ready_success(second);

    seastar::abort_source read_abort;
    auto first_read = server.read(kwaque::byte_count{1'000}, read_abort);
    co_await pump_until(events, first_read);
    auto received_first = co_await std::move(first_read);
    BOOST_REQUIRE(received_first.has_value());
    BOOST_CHECK(received_first->data().content_equals(std::string(1'000, 'a')));
    auto second_read = server.read(kwaque::byte_count{1'000}, read_abort);
    co_await pump_until(events, second_read);
    auto received_second = co_await std::move(second_read);
    BOOST_REQUIRE(received_second.has_value());
    BOOST_CHECK(
      received_second->data().content_equals(std::string(1'000, 'b')));

    auto client_close = client.close();
    auto server_close = server.close();
    auto listener_close = listener.close();
    co_await pump_until(events, client_close);
    co_await pump_until(events, server_close);
    co_await pump_until(events, listener_close);
    co_await require_ready_success(client_close);
    co_await require_ready_success(server_close);
    co_await require_ready_success(listener_close);
}

SEASTAR_TEST_CASE(fake_network_write_faults_preserve_packet_ownership) {
    const auto scheduler_budget = bandwidth_scheduler_limits();
    const auto trace_budget = network_trace_limits();
    kwaque::simulation::event_trace trace{
      network_trace_header(scheduler_budget, trace_budget), trace_budget};
    kwaque::simulation::scheduler events{scheduler_budget, &trace};
    seastar::chunked_vector<kwaque::simulation::fault_rule> rules;
    rules.push_back(network_write_rule(
      1, 1, kwaque::runtime::fault_decision::make_duplicate()));
    rules.push_back(network_write_rule(
      2, 2, kwaque::runtime::fault_decision::make_corrupt()));
    rules.push_back(network_write_rule(
      3,
      3,
      kwaque::runtime::fault_decision::make_short_operation(
        kwaque::byte_count{2})));
    rules.push_back(
      network_write_rule(4, 4, kwaque::runtime::fault_decision::make_drop()));
    auto made_faults = kwaque::simulation::fault_schedule::make(
      events, trace, 17, std::move(rules));
    BOOST_REQUIRE(made_faults.has_value());
    auto faults = std::move(*made_faults);
    auto config = kwaque::simulation::fake_network_config{};
    config.maximum_packets = 2;
    config.maximum_direction_packets = 2;
    config.maximum_active_flows = 1;
    config.maximum_direction_bytes = kwaque::byte_count{6};
    config.maximum_packet_logical_bytes = kwaque::byte_count{6};
    config.maximum_packet_retained_bytes = kwaque::byte_count{1'024};
    config.latency_min = kwaque::runtime::monotonic_duration{};
    config.latency_mean_parameter = config.latency_min;
    auto made = kwaque::simulation::fake_network::make(
      config, events, faults.get());
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);

    auto binding = network->listen(
      kwaque::runtime::network_endpoint{alternate, 0}, {});
    co_await pump_until(events, binding);
    auto bound = co_await std::move(binding);
    BOOST_REQUIRE(bound.has_value());
    auto listener = std::move(*bound);
    seastar::abort_source accept_abort;
    seastar::abort_source connect_abort;
    auto accepting = listener.accept(accept_abort);
    auto connecting = network->connect(
      listener.local_endpoint(),
      std::nullopt,
      kwaque::runtime::network_connection_limits{},
      connect_abort);
    co_await pump_until(events, connecting);
    co_await pump_until(events, accepting);
    auto connected = co_await std::move(connecting);
    auto accepted = co_await std::move(accepting);
    BOOST_REQUIRE(connected.has_value());
    BOOST_REQUIRE(accepted.has_value());
    auto client = std::move(*connected);
    auto server = std::move(*accepted);
    seastar::abort_source write_abort;
    seastar::abort_source read_abort;

    auto duplicated = client.write(
      kwaque::runtime::testing::network_contract_detail::make_bytes("dup"),
      write_abort);
    co_await pump_until(events, duplicated);
    co_await require_ready_success(duplicated);

    auto saturated = client.write(
      kwaque::runtime::testing::network_contract_detail::make_bytes("x"),
      write_abort);
    BOOST_REQUIRE(saturated.available());
    const auto pressure = saturated.get();
    BOOST_REQUIRE(!pressure.has_value());
    BOOST_CHECK(pressure.error().code() == kwaque::errc::queue_full);

    auto duplicate_read
      = kwaque::runtime::testing::network_contract_detail::read_exactly(
        server, 6, read_abort);
    co_await pump_until(events, duplicate_read);
    const auto duplicate_bytes = co_await std::move(duplicate_read);
    BOOST_CHECK(duplicate_bytes == "dupdup");

    auto corrupted = client.write(
      kwaque::runtime::testing::network_contract_detail::make_bytes("a"),
      write_abort);
    co_await pump_until(events, corrupted);
    co_await require_ready_success(corrupted);
    auto corrupt_read = server.read(kwaque::byte_count{1}, read_abort);
    co_await pump_until(events, corrupt_read);
    auto corrupt_result = co_await std::move(corrupt_read);
    BOOST_REQUIRE(corrupt_result.has_value());
    BOOST_CHECK(!corrupt_result->data().content_equals("a"));

    auto shortened = client.write(
      kwaque::runtime::testing::network_contract_detail::make_bytes("abcd"),
      write_abort);
    co_await pump_until(events, shortened);
    co_await require_ready_success(shortened);
    auto short_read = server.read(kwaque::byte_count{4}, read_abort);
    co_await pump_until(events, short_read);
    auto short_result = co_await std::move(short_read);
    BOOST_REQUIRE(short_result.has_value());
    BOOST_CHECK(short_result->data().content_equals("ab"));

    auto dropped = client.write(
      kwaque::runtime::testing::network_contract_detail::make_bytes("gone"),
      write_abort);
    co_await pump_until(events, dropped);
    co_await require_ready_success(dropped);
    auto waiting_read = server.read(kwaque::byte_count{2}, read_abort);
    auto following = client.write(
      kwaque::runtime::testing::network_contract_detail::make_bytes("ok"),
      write_abort);
    co_await pump_until(events, following);
    co_await require_ready_success(following);
    co_await pump_until(events, waiting_read);
    auto following_result = co_await std::move(waiting_read);
    BOOST_REQUIRE(following_result.has_value());
    BOOST_CHECK(following_result->data().content_equals("ok"));

    auto client_close = client.close();
    auto server_close = server.close();
    auto listener_close = listener.close();
    co_await pump_until(events, client_close);
    co_await pump_until(events, server_close);
    co_await pump_until(events, listener_close);
    co_await require_ready_success(client_close);
    co_await require_ready_success(server_close);
    co_await require_ready_success(listener_close);

    std::array<bool, 23> actions{};
    bool canonical_rebalance = false;
    for (const auto& entry : trace.entries()) {
        actions[static_cast<std::size_t>(entry.action)] = true;
        if (
          entry.action
          == kwaque::simulation::trace_action::bandwidth_rebalanced) {
            canonical_rebalance
              = entry.context_size == 4
                && entry.context[0].key
                     == kwaque::simulation::trace_context_key::digest_word_0
                && entry.context[3].key
                     == kwaque::simulation::trace_context_key::digest_word_3;
        }
    }
    BOOST_CHECK(
      actions[static_cast<std::size_t>(
        kwaque::simulation::trace_action::fault_evaluated)]);
    BOOST_CHECK(
      actions[static_cast<std::size_t>(
        kwaque::simulation::trace_action::network_operation_applied)]);
    BOOST_CHECK(
      actions[static_cast<std::size_t>(
        kwaque::simulation::trace_action::flow_started)]);
    BOOST_CHECK(
      actions[static_cast<std::size_t>(
        kwaque::simulation::trace_action::bandwidth_rebalanced)]);
    BOOST_CHECK(
      actions[static_cast<std::size_t>(
        kwaque::simulation::trace_action::transfer_completed)]);
    BOOST_CHECK(
      actions[static_cast<std::size_t>(
        kwaque::simulation::trace_action::packet_delivered)]);
    BOOST_CHECK(
      actions[static_cast<std::size_t>(
        kwaque::simulation::trace_action::packet_dropped)]);
    BOOST_CHECK(
      actions[static_cast<std::size_t>(
        kwaque::simulation::trace_action::reset_applied)]);
    BOOST_CHECK(canonical_rebalance);
}

SEASTAR_TEST_CASE(
  fake_network_clog_partition_and_bandwidth_controls_are_directional) {
    const auto scheduler_budget = bandwidth_scheduler_limits();
    const auto trace_budget = network_trace_limits();
    kwaque::simulation::event_trace trace{
      network_trace_header(scheduler_budget, trace_budget), trace_budget};
    kwaque::simulation::scheduler events{scheduler_budget, &trace};
    auto config = kwaque::simulation::fake_network_config{};
    config.latency_min = kwaque::runtime::monotonic_duration{10};
    config.latency_mean_parameter = config.latency_min;
    auto made = kwaque::simulation::fake_network::make(config, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);
    auto binding = network->listen(
      kwaque::runtime::network_endpoint{alternate, 0}, {});
    co_await pump_until(events, binding);
    auto bound = co_await std::move(binding);
    BOOST_REQUIRE(bound.has_value());
    auto listener = std::move(*bound);
    seastar::abort_source accept_abort;
    seastar::abort_source connect_abort;
    auto accepting = listener.accept(accept_abort);
    auto connecting = network->connect(
      listener.local_endpoint(),
      std::nullopt,
      kwaque::runtime::network_connection_limits{},
      connect_abort);
    co_await pump_until(events, connecting);
    co_await pump_until(events, accepting);
    auto connected = co_await std::move(connecting);
    auto accepted = co_await std::move(accepting);
    BOOST_REQUIRE(connected.has_value());
    BOOST_REQUIRE(accepted.has_value());
    auto client = std::move(*connected);
    auto server = std::move(*accepted);

    auto clogging = network->clog(loopback, alternate);
    co_await pump_until(events, clogging);
    co_await require_ready_success(clogging);
    seastar::abort_source write_abort;
    seastar::abort_source read_abort;
    auto dropped_write = client.write(
      kwaque::runtime::testing::network_contract_detail::make_bytes("lost"),
      write_abort);
    co_await pump_until(events, dropped_write);
    co_await require_ready_success(dropped_write);
    auto reading = server.read(kwaque::byte_count{2}, read_abort);
    auto next = events.advance_to_next();
    BOOST_REQUIRE(next.has_value() && next->has_value());
    BOOST_REQUIRE(events.run_ready().has_value());
    co_await seastar::yield();
    BOOST_CHECK(!reading.available());

    auto partitioning = network->partition(loopback, alternate);
    co_await pump_until(events, partitioning);
    co_await require_ready_success(partitioning);
    auto unclogging = network->unclog(loopback, alternate);
    co_await pump_until(events, unclogging);
    co_await require_ready_success(unclogging);
    BOOST_CHECK(!reading.available());
    auto healing = network->heal(loopback, alternate);
    co_await pump_until(events, healing);
    co_await require_ready_success(healing);

    auto zeroing = network->set_link_capacity(
      loopback, alternate, kwaque::simulation::bandwidth_capacity::finite(0));
    co_await pump_until(events, zeroing);
    co_await require_ready_success(zeroing);
    auto following = client.write(
      kwaque::runtime::testing::network_contract_detail::make_bytes("ok"),
      write_abort);
    BOOST_CHECK(!following.available());
    auto unlimiting = network->set_link_capacity(
      loopback, alternate, kwaque::simulation::bandwidth_capacity::unlimited());
    co_await pump_until(events, unlimiting);
    co_await require_ready_success(unlimiting);
    co_await pump_until(events, following);
    co_await require_ready_success(following);
    co_await pump_until(events, reading);
    auto received = co_await std::move(reading);
    BOOST_REQUIRE(received.has_value());
    BOOST_CHECK(received->data().content_equals("ok"));

    auto client_close = client.close();
    auto server_close = server.close();
    auto listener_close = listener.close();
    co_await pump_until(events, client_close);
    co_await pump_until(events, server_close);
    co_await pump_until(events, listener_close);
    co_await require_ready_success(client_close);
    co_await require_ready_success(server_close);
    co_await require_ready_success(listener_close);
    BOOST_CHECK(std::ranges::any_of(trace.entries(), [](const auto& entry) {
        return entry.action
               == kwaque::simulation::trace_action::network_control_applied;
    }));
}

SEASTAR_TEST_CASE(
  fake_network_stop_batches_active_queued_and_parked_ownership) {
    const auto scheduler_budget = bandwidth_scheduler_limits();
    const auto trace_budget = network_trace_limits();
    kwaque::simulation::event_trace trace{
      network_trace_header(scheduler_budget, trace_budget), trace_budget};
    kwaque::simulation::scheduler events{scheduler_budget, &trace};
    auto config = kwaque::simulation::fake_network_config{};
    config.stop_batch = 1;
    auto made = kwaque::simulation::fake_network::make(config, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);

    auto binding = network->listen(
      kwaque::runtime::network_endpoint{alternate, 0}, {});
    co_await pump_until(events, binding);
    auto bound = co_await std::move(binding);
    BOOST_REQUIRE(bound.has_value());
    auto listener = std::move(*bound);
    seastar::abort_source accept_abort;
    seastar::abort_source connect_abort;
    auto accepting = listener.accept(accept_abort);
    auto connecting = network->connect(
      listener.local_endpoint(),
      std::nullopt,
      kwaque::runtime::network_connection_limits{},
      connect_abort);
    co_await pump_until(events, connecting);
    co_await pump_until(events, accepting);
    auto connected = co_await std::move(connecting);
    auto accepted = co_await std::move(accepting);
    BOOST_REQUIRE(connected.has_value());
    BOOST_REQUIRE(accepted.has_value());
    auto client = std::move(*connected);
    auto server = std::move(*accepted);

    auto zero = network->set_link_capacity(
      loopback, alternate, kwaque::simulation::bandwidth_capacity::finite(0));
    co_await pump_until(events, zero);
    co_await require_ready_success(zero);
    seastar::abort_source write_abort;
    seastar::abort_source read_abort;
    auto active = client.write(
      kwaque::runtime::testing::network_contract_detail::make_bytes("active"),
      write_abort);
    auto queued = client.write(
      kwaque::runtime::testing::network_contract_detail::make_bytes("queued"),
      write_abort);
    auto reading = server.read(kwaque::byte_count{16}, read_abort);
    auto controlling = network->partition(loopback, alternate);
    BOOST_CHECK(!active.available());
    BOOST_CHECK(!queued.available());
    BOOST_CHECK(!reading.available());
    BOOST_CHECK(!controlling.available());

    auto stopping = network->stop();
    BOOST_CHECK(!stopping.available());
    co_await pump_until(events, stopping);
    co_await require_ready_success(stopping);
    BOOST_CHECK(
      network->state() == kwaque::simulation::fake_network_state::stopped);

    co_await pump_until(events, active);
    co_await pump_until(events, queued);
    co_await pump_until(events, reading);
    co_await pump_until(events, controlling);
    const auto active_result = co_await std::move(active);
    const auto queued_result = co_await std::move(queued);
    const auto read_result = co_await std::move(reading);
    const auto control_result = co_await std::move(controlling);
    BOOST_REQUIRE(!active_result.has_value());
    BOOST_REQUIRE(!queued_result.has_value());
    BOOST_REQUIRE(!read_result.has_value());
    BOOST_REQUIRE(!control_result.has_value());
    BOOST_CHECK(active_result.error().code() == kwaque::errc::aborted);
    BOOST_CHECK(queued_result.error().code() == kwaque::errc::aborted);
    BOOST_CHECK(read_result.error().code() == kwaque::errc::aborted);
    BOOST_CHECK(control_result.error().code() == kwaque::errc::aborted);
    BOOST_REQUIRE((co_await network->stop()).has_value());
    BOOST_CHECK(std::ranges::any_of(trace.entries(), [](const auto& entry) {
        return entry.action == kwaque::simulation::trace_action::stop_terminal
               && entry.kind == kwaque::simulation::trace_event_kind::network;
    }));
}

SEASTAR_TEST_CASE(fake_network_stop_releases_clogged_ready_packet) {
    kwaque::simulation::scheduler events{bandwidth_scheduler_limits()};
    auto config = kwaque::simulation::fake_network_config{};
    config.stop_batch = 1;
    config.latency_min = kwaque::runtime::monotonic_duration{10};
    config.latency_mean_parameter = config.latency_min;
    auto made = kwaque::simulation::fake_network::make(config, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);
    auto binding = network->listen(
      kwaque::runtime::network_endpoint{alternate, 0}, {});
    co_await pump_until(events, binding);
    auto bound = co_await std::move(binding);
    BOOST_REQUIRE(bound.has_value());
    auto listener = std::move(*bound);
    seastar::abort_source accept_abort;
    seastar::abort_source connect_abort;
    auto accepting = listener.accept(accept_abort);
    auto connecting = network->connect(
      listener.local_endpoint(),
      std::nullopt,
      kwaque::runtime::network_connection_limits{},
      connect_abort);
    co_await pump_until(events, connecting);
    co_await pump_until(events, accepting);
    auto connected = co_await std::move(connecting);
    auto accepted = co_await std::move(accepting);
    BOOST_REQUIRE(connected.has_value());
    BOOST_REQUIRE(accepted.has_value());
    auto client = std::move(*connected);
    auto server = std::move(*accepted);
    auto clogging = network->clog(loopback, alternate);
    co_await pump_until(events, clogging);
    co_await require_ready_success(clogging);
    seastar::abort_source write_abort;
    auto writing = client.write(
      kwaque::runtime::testing::network_contract_detail::make_bytes("ready"),
      write_abort);
    co_await pump_until(events, writing);
    co_await require_ready_success(writing);
    const auto ready = events.advance_to_next();
    BOOST_REQUIRE(ready.has_value() && ready->has_value());
    BOOST_REQUIRE(events.run_ready().has_value());
    co_await seastar::yield();

    auto stopping = network->stop();
    co_await pump_until(events, stopping);
    co_await require_ready_success(stopping);
    BOOST_CHECK(
      network->state() == kwaque::simulation::fake_network_state::stopped);
    static_cast<void>(server);
}

SEASTAR_TEST_CASE(fake_network_stop_resolves_dropped_write_completion) {
    const auto scheduler_budget = bandwidth_scheduler_limits();
    const auto trace_budget = network_trace_limits();
    kwaque::simulation::event_trace trace{
      network_trace_header(scheduler_budget, trace_budget), trace_budget};
    kwaque::simulation::scheduler events{scheduler_budget, &trace};
    seastar::chunked_vector<kwaque::simulation::fault_rule> rules;
    rules.push_back(network_write_rule(
      1, 1, kwaque::runtime::fault_decision::make_drop_completion()));
    auto made_faults = kwaque::simulation::fault_schedule::make(
      events, trace, 17, std::move(rules));
    BOOST_REQUIRE(made_faults.has_value());
    auto faults = std::move(*made_faults);
    auto config = kwaque::simulation::fake_network_config{};
    config.stop_batch = 1;
    auto made = kwaque::simulation::fake_network::make(
      config, events, faults.get());
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);
    auto binding = network->listen(
      kwaque::runtime::network_endpoint{alternate, 0}, {});
    co_await pump_until(events, binding);
    auto bound = co_await std::move(binding);
    BOOST_REQUIRE(bound.has_value());
    auto listener = std::move(*bound);
    seastar::abort_source accept_abort;
    seastar::abort_source connect_abort;
    auto accepting = listener.accept(accept_abort);
    auto connecting = network->connect(
      listener.local_endpoint(),
      std::nullopt,
      kwaque::runtime::network_connection_limits{},
      connect_abort);
    co_await pump_until(events, connecting);
    co_await pump_until(events, accepting);
    auto connected = co_await std::move(connecting);
    auto accepted = co_await std::move(accepting);
    BOOST_REQUIRE(connected.has_value());
    BOOST_REQUIRE(accepted.has_value());
    auto client = std::move(*connected);
    auto server = std::move(*accepted);
    seastar::abort_source write_abort;
    auto writing = client.write(
      kwaque::runtime::testing::network_contract_detail::make_bytes("parked"),
      write_abort);
    while (events.pending_events() != 0U) {
        if (!events.has_ready_events()) {
            const auto advanced = events.advance_to_next();
            BOOST_REQUIRE(advanced.has_value() && advanced->has_value());
        }
        BOOST_REQUIRE(events.run_ready().has_value());
        co_await seastar::yield();
    }
    BOOST_CHECK(!writing.available());
    auto stopping = network->stop();
    co_await pump_until(events, stopping);
    co_await require_ready_success(stopping);
    co_await pump_until(events, writing);
    const auto stopped_write = co_await std::move(writing);
    BOOST_REQUIRE(!stopped_write.has_value());
    BOOST_CHECK(stopped_write.error().code() == kwaque::errc::aborted);
    BOOST_CHECK(std::ranges::any_of(trace.entries(), [](const auto& entry) {
        return entry.action
               == kwaque::simulation::trace_action::operation_parked;
    }));
    static_cast<void>(server);
}

SEASTAR_TEST_CASE(fake_network_shared_round_trip_contract) {
    co_await run_shared_case([](kwaque::simulation::fake_network& network) {
        return kwaque::runtime::testing::network_contract_detail::round_trip(
          network);
    });
}

SEASTAR_TEST_CASE(fake_network_schema_four_trace_replays_fresh) {
    const auto scheduler_budget = bandwidth_scheduler_limits();
    const auto trace_budget = network_trace_limits();
    const auto header = network_trace_header(scheduler_budget, trace_budget);
    kwaque::simulation::event_trace captured{header, trace_budget};
    co_await run_traced_round_trip(captured, scheduler_budget);
    auto artifact = captured.encode();
    BOOST_REQUIRE(artifact.has_value());
    auto decoded = kwaque::simulation::event_trace::decode(
      std::move(*artifact), trace_budget);
    BOOST_REQUIRE(decoded.has_value());
    auto replay = kwaque::simulation::event_trace::replay(
      header, trace_budget, std::move(*decoded));
    BOOST_REQUIRE(replay.has_value());
    co_await run_traced_round_trip(**replay, scheduler_budget);
    BOOST_REQUIRE((*replay)->finish_replay().has_value());
}

SEASTAR_TEST_CASE(fake_network_trace_poison_discards_bind_ownership) {
    const auto scheduler_budget = bandwidth_scheduler_limits();
    const auto trace_budget = network_trace_limits();
    const auto header = network_trace_header(scheduler_budget, trace_budget);
    kwaque::simulation::event_trace captured{header, trace_budget};
    {
        kwaque::simulation::scheduler events{scheduler_budget, &captured};
        auto made = kwaque::simulation::fake_network::make({}, events);
        BOOST_REQUIRE(made.has_value());
        auto network = std::move(*made);
        auto binding = network->listen(
          kwaque::runtime::network_endpoint{alternate, 0}, {});
        co_await pump_until(events, binding);
        auto bound = co_await std::move(binding);
        BOOST_REQUIRE(bound.has_value());
        auto listener = std::move(*bound);
        auto closing = listener.close();
        co_await pump_until(events, closing);
        co_await require_ready_success(closing);
    }
    auto artifact = captured.encode();
    BOOST_REQUIRE(artifact.has_value());
    auto decoded = kwaque::simulation::event_trace::decode(
      std::move(*artifact), trace_budget);
    BOOST_REQUIRE(decoded.has_value());
    bool mutated = false;
    for (auto& entry : decoded->entries) {
        if (
          entry.action
            == kwaque::simulation::trace_action::network_operation_applied
          && entry.domain
               == static_cast<std::uint32_t>(
                 kwaque::simulation::network_trace_phase::bind)) {
            ++entry.coordinate_b;
            mutated = true;
            break;
        }
    }
    BOOST_REQUIRE(mutated);
    auto replay = kwaque::simulation::event_trace::replay(
      header, trace_budget, std::move(*decoded));
    BOOST_REQUIRE(replay.has_value());
    kwaque::simulation::scheduler events{scheduler_budget, replay->get()};
    auto made = kwaque::simulation::fake_network::make({}, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);
    auto binding = network->listen(
      kwaque::runtime::network_endpoint{alternate, 0}, {});
    const auto advanced = events.advance_to_next();
    BOOST_REQUIRE(advanced.has_value() && advanced->has_value());
    const auto divergent = events.run_ready();
    BOOST_REQUIRE(!divergent.has_value());
    BOOST_CHECK(divergent.error().code() == kwaque::errc::replay_divergence);
    BOOST_REQUIRE(events.discard_failed());
    co_await seastar::yield();
    BOOST_REQUIRE(binding.available());
    const auto rejected = co_await std::move(binding);
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == kwaque::errc::replay_divergence);
}

SEASTAR_TEST_CASE(fake_network_matches_independent_directed_swizzle_oracle) {
    kwaque::simulation::scheduler events{bandwidth_scheduler_limits()};
    auto config = kwaque::simulation::fake_network_config{};
    config.latency_min = kwaque::runtime::monotonic_duration{10};
    config.latency_mean_parameter = config.latency_min;
    auto made = kwaque::simulation::fake_network::make(config, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);
    kwaque::simulation::testing::dense_network_oracle oracle;

    auto binding_a = network->listen(
      kwaque::runtime::network_endpoint{alternate, 0}, {});
    auto binding_b = network->listen(
      kwaque::runtime::network_endpoint{third, 0}, {});
    co_await pump_until(events, binding_a);
    co_await pump_until(events, binding_b);
    auto bound_a = co_await std::move(binding_a);
    auto bound_b = co_await std::move(binding_b);
    BOOST_REQUIRE(bound_a.has_value());
    BOOST_REQUIRE(bound_b.has_value());
    auto listener_a = std::move(*bound_a);
    auto listener_b = std::move(*bound_b);
    BOOST_REQUIRE(
      oracle
        .apply(
          kwaque::simulation::testing::oracle_step{
            .kind = kwaque::simulation::testing::oracle_step_kind::bind_exact,
            .source = 1,
            .port = listener_a.local_endpoint().port(),
          })
        .has_value());
    BOOST_REQUIRE(
      oracle
        .apply(
          kwaque::simulation::testing::oracle_step{
            .kind = kwaque::simulation::testing::oracle_step_kind::bind_exact,
            .source = 2,
            .port = listener_b.local_endpoint().port(),
          })
        .has_value());

    seastar::abort_source accept_abort_a;
    seastar::abort_source accept_abort_b;
    seastar::abort_source connect_abort_a;
    seastar::abort_source connect_abort_b;
    auto accepting_a = listener_a.accept(accept_abort_a);
    auto accepting_b = listener_b.accept(accept_abort_b);
    auto connecting_a = network->connect(
      listener_a.local_endpoint(),
      std::nullopt,
      kwaque::runtime::network_connection_limits{},
      connect_abort_a);
    auto connecting_b = network->connect(
      listener_b.local_endpoint(),
      std::nullopt,
      kwaque::runtime::network_connection_limits{},
      connect_abort_b);
    co_await pump_until(events, connecting_a);
    co_await pump_until(events, connecting_b);
    co_await pump_until(events, accepting_a);
    co_await pump_until(events, accepting_b);
    auto connected_a = co_await std::move(connecting_a);
    auto connected_b = co_await std::move(connecting_b);
    auto accepted_a = co_await std::move(accepting_a);
    auto accepted_b = co_await std::move(accepting_b);
    BOOST_REQUIRE(connected_a.has_value());
    BOOST_REQUIRE(connected_b.has_value());
    BOOST_REQUIRE(accepted_a.has_value());
    BOOST_REQUIRE(accepted_b.has_value());
    auto client_a = std::move(*connected_a);
    auto client_b = std::move(*connected_b);
    auto server_a = std::move(*accepted_a);
    auto server_b = std::move(*accepted_b);
    BOOST_REQUIRE(
      oracle
        .apply(
          kwaque::simulation::testing::oracle_step{
            .kind
            = kwaque::simulation::testing::oracle_step_kind::connect_implicit,
            .target = 1,
          })
        .has_value());
    BOOST_REQUIRE(
      oracle
        .apply(
          kwaque::simulation::testing::oracle_step{
            .kind
            = kwaque::simulation::testing::oracle_step_kind::connect_implicit,
            .target = 2,
          })
        .has_value());

    auto clog_a = network->clog(loopback, alternate);
    auto clog_b = network->clog(loopback, third);
    co_await pump_until(events, clog_a);
    co_await pump_until(events, clog_b);
    co_await require_ready_success(clog_a);
    co_await require_ready_success(clog_b);
    BOOST_REQUIRE(
      oracle
        .apply(
          kwaque::simulation::testing::oracle_step{
            .kind = kwaque::simulation::testing::oracle_step_kind::clog,
            .target = 1,
          })
        .has_value());
    BOOST_REQUIRE(
      oracle
        .apply(
          kwaque::simulation::testing::oracle_step{
            .kind = kwaque::simulation::testing::oracle_step_kind::clog,
            .target = 2,
          })
        .has_value());

    seastar::abort_source write_abort;
    auto writing_a = client_a.write(
      kwaque::runtime::testing::network_contract_detail::make_bytes("aaa"),
      write_abort);
    auto writing_b = client_b.write(
      kwaque::runtime::testing::network_contract_detail::make_bytes("aaa"),
      write_abort);
    co_await pump_until(events, writing_a);
    co_await pump_until(events, writing_b);
    co_await require_ready_success(writing_a);
    co_await require_ready_success(writing_b);
    BOOST_REQUIRE(
      oracle
        .apply(
          kwaque::simulation::testing::oracle_step{
            .kind = kwaque::simulation::testing::oracle_step_kind::write,
            .target = 1,
            .value = 3,
          })
        .has_value());
    BOOST_REQUIRE(
      oracle
        .apply(
          kwaque::simulation::testing::oracle_step{
            .kind = kwaque::simulation::testing::oracle_step_kind::write,
            .target = 2,
            .value = 3,
          })
        .has_value());
    auto ready = events.advance_to_next();
    BOOST_REQUIRE(ready.has_value() && ready->has_value());
    BOOST_REQUIRE(events.run_ready().has_value());
    co_await seastar::yield();

    auto partition = network->partition(loopback, alternate);
    co_await pump_until(events, partition);
    co_await require_ready_success(partition);
    BOOST_REQUIRE(
      oracle
        .apply(
          kwaque::simulation::testing::oracle_step{
            .kind = kwaque::simulation::testing::oracle_step_kind::partition,
            .target = 1,
          })
        .has_value());
    auto unclog_a = network->unclog(loopback, alternate);
    co_await pump_until(events, unclog_a);
    co_await require_ready_success(unclog_a);
    BOOST_REQUIRE(
      oracle
        .apply(
          kwaque::simulation::testing::oracle_step{
            .kind = kwaque::simulation::testing::oracle_step_kind::unclog,
            .target = 1,
          })
        .has_value());
    BOOST_CHECK(oracle.snapshot().visible[1].empty());

    auto unclog_b = network->unclog(loopback, third);
    co_await pump_until(events, unclog_b);
    co_await require_ready_success(unclog_b);
    BOOST_REQUIRE(
      oracle
        .apply(
          kwaque::simulation::testing::oracle_step{
            .kind = kwaque::simulation::testing::oracle_step_kind::unclog,
            .target = 2,
          })
        .has_value());
    seastar::abort_source read_abort;
    auto read_b = server_b.read(kwaque::byte_count{3}, read_abort);
    co_await pump_until(events, read_b);
    auto received_b = co_await std::move(read_b);
    BOOST_REQUIRE(received_b.has_value());
    BOOST_CHECK(
      received_b->data().content_equals(oracle.snapshot().visible[2]));

    auto heal = network->heal(loopback, alternate);
    auto reclog = network->clog(loopback, alternate);
    co_await pump_until(events, heal);
    co_await pump_until(events, reclog);
    co_await require_ready_success(heal);
    co_await require_ready_success(reclog);
    BOOST_REQUIRE(
      oracle
        .apply(
          kwaque::simulation::testing::oracle_step{
            .kind = kwaque::simulation::testing::oracle_step_kind::heal,
            .target = 1,
          })
        .has_value());
    BOOST_REQUIRE(
      oracle
        .apply(
          kwaque::simulation::testing::oracle_step{
            .kind = kwaque::simulation::testing::oracle_step_kind::clog,
            .target = 1,
          })
        .has_value());
    auto second_write = client_a.write(
      kwaque::runtime::testing::network_contract_detail::make_bytes("aaa"),
      write_abort);
    co_await pump_until(events, second_write);
    co_await require_ready_success(second_write);
    BOOST_REQUIRE(
      oracle
        .apply(
          kwaque::simulation::testing::oracle_step{
            .kind = kwaque::simulation::testing::oracle_step_kind::write,
            .target = 1,
            .value = 3,
          })
        .has_value());
    ready = events.advance_to_next();
    BOOST_REQUIRE(ready.has_value() && ready->has_value());
    BOOST_REQUIRE(events.run_ready().has_value());
    co_await seastar::yield();
    auto second_unclog = network->unclog(loopback, alternate);
    co_await pump_until(events, second_unclog);
    co_await require_ready_success(second_unclog);
    BOOST_REQUIRE(
      oracle
        .apply(
          kwaque::simulation::testing::oracle_step{
            .kind = kwaque::simulation::testing::oracle_step_kind::unclog,
            .target = 1,
          })
        .has_value());
    auto read_a = server_a.read(kwaque::byte_count{3}, read_abort);
    co_await pump_until(events, read_a);
    auto received_a = co_await std::move(read_a);
    BOOST_REQUIRE(received_a.has_value());
    BOOST_CHECK(
      received_a->data().content_equals(oracle.snapshot().visible[1]));

    auto client_close_a = client_a.close();
    auto client_close_b = client_b.close();
    auto server_close_a = server_a.close();
    auto server_close_b = server_b.close();
    auto listener_close_a = listener_a.close();
    auto listener_close_b = listener_b.close();
    co_await pump_until(events, client_close_a);
    co_await pump_until(events, client_close_b);
    co_await pump_until(events, server_close_a);
    co_await pump_until(events, server_close_b);
    co_await pump_until(events, listener_close_a);
    co_await pump_until(events, listener_close_b);
    co_await require_ready_success(client_close_a);
    co_await require_ready_success(client_close_b);
    co_await require_ready_success(server_close_a);
    co_await require_ready_success(server_close_b);
    co_await require_ready_success(listener_close_a);
    co_await require_ready_success(listener_close_b);
}

SEASTAR_TEST_CASE(fake_network_shared_connection_error_contract) {
    co_await run_shared_case([](kwaque::simulation::fake_network& network) {
        return kwaque::runtime::testing::network_contract_detail::
          connection_errors(network);
    });
}

SEASTAR_TEST_CASE(fake_network_shared_multiple_client_contract) {
    co_await run_shared_case([](kwaque::simulation::fake_network& network) {
        return kwaque::runtime::testing::network_contract_detail::
          multiple_clients(network);
    });
}

SEASTAR_TEST_CASE(fake_network_shared_long_stream_contract) {
    co_await run_shared_case([](kwaque::simulation::fake_network& network) {
        return kwaque::runtime::testing::network_contract_detail::long_stream(
          network);
    });
}

SEASTAR_TEST_CASE(fake_network_shared_active_read_abort_contract) {
    co_await run_shared_case([](kwaque::simulation::fake_network& network) {
        return kwaque::runtime::testing::network_contract_detail::
          active_read_abort(network);
    });
}

SEASTAR_TEST_CASE(fake_network_shared_saturation_abort_contract) {
    co_await run_shared_case([](kwaque::simulation::fake_network& network) {
        return kwaque::runtime::testing::network_contract_detail::
          saturation_and_abort(network);
    });
    co_return;
}
