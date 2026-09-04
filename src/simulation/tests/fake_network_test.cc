#include "src/runtime/random.h"
#include "src/runtime/testing/contracts/network_contract.h"
#include "src/simulation/determinism_version.h"
#include "src/simulation/deterministic_random.h"
#include "src/simulation/event_trace.h"
#include "src/simulation/fake_network.h"
#include "src/simulation/fake_network_test_support.h"
#include "src/simulation/fault_schedule.h"
#include "src/simulation/scheduler.h"
#include "src/simulation/scheduler_driver.h"
#include "src/simulation/tests/network_oracle.h"

#include <seastar/core/chunked_vector.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace {

using kwaque::simulation::testing::pump_until;

constexpr auto loopback = kwaque::runtime::network_address::ipv4(
  {std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}});
constexpr auto alternate = kwaque::runtime::network_address::ipv4(
  {std::byte{127}, std::byte{0}, std::byte{0}, std::byte{2}});
constexpr auto third = kwaque::runtime::network_address::ipv4(
  {std::byte{127}, std::byte{0}, std::byte{0}, std::byte{3}});
constexpr auto wildcard = kwaque::runtime::network_address::ipv4(
  {std::byte{}, std::byte{}, std::byte{}, std::byte{}});

constexpr kwaque::runtime::network_address numbered_address(std::uint8_t last) {
    return kwaque::runtime::network_address::ipv4(
      {std::byte{127}, std::byte{0}, std::byte{1}, std::byte{last}});
}

kwaque::simulation::scheduler_limits scheduler_limits() {
    auto limits = kwaque::simulation::scheduler_limits::make(
      kwaque::simulation::scheduler_limit_values{
        .pending_events = 65'536,
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
        .pending_events = 65'536,
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

kwaque::simulation::fault_rule network_rule(
  std::uint64_t id,
  kwaque::runtime::builtin_fault_point point,
  std::uint64_t occurrence,
  kwaque::runtime::fault_decision decision) {
    auto rule_id = kwaque::simulation::fault_rule_id::make(id);
    auto selected = kwaque::runtime::fault_occurrence::make(occurrence);
    BOOST_REQUIRE(rule_id.has_value());
    BOOST_REQUIRE(selected.has_value());
    auto rule = kwaque::simulation::fault_rule::make(
      *rule_id,
      point,
      std::nullopt,
      *selected,
      *selected,
      kwaque::simulation::fault_selector::once(),
      decision);
    BOOST_REQUIRE(rule.has_value());
    return *rule;
}

kwaque::simulation::fault_rule network_write_rule(
  std::uint64_t id,
  std::uint64_t occurrence,
  kwaque::runtime::fault_decision decision) {
    return network_rule(
      id,
      kwaque::runtime::builtin_fault_point::network_write,
      occurrence,
      decision);
}

template<typename Future>
seastar::future<> require_ready_success(Future& waiting) {
    const auto result = co_await std::move(waiting);
    BOOST_REQUIRE(result.has_value());
}

seastar::future<> stop_network(
  kwaque::simulation::scheduler& events,
  std::unique_ptr<kwaque::simulation::fake_network>& network) {
    auto stopping = network->stop();
    co_await pump_until(events, stopping);
    co_await require_ready_success(stopping);
}

seastar::future<> pump_all(kwaque::simulation::scheduler& events) {
    while (events.pending_events() != 0U) {
        if (!events.has_ready_events()) {
            const auto advanced = events.advance_to_next();
            BOOST_REQUIRE(advanced.has_value() && advanced->has_value());
        }
        BOOST_REQUIRE(events.run_ready().has_value());
        co_await seastar::yield();
    }
}

struct fault_environment final {
    kwaque::simulation::scheduler_limits scheduler_budget{
      bandwidth_scheduler_limits()};
    kwaque::simulation::trace_limits trace_budget{network_trace_limits()};
    kwaque::simulation::event_trace trace{
      network_trace_header(scheduler_budget, trace_budget), trace_budget};
    kwaque::simulation::scheduler events{scheduler_budget, &trace};
    std::unique_ptr<kwaque::simulation::fault_schedule> faults;
    std::unique_ptr<kwaque::simulation::fake_network> network;

    fault_environment(
      kwaque::runtime::builtin_fault_point point,
      kwaque::runtime::fault_decision decision,
      kwaque::simulation::fake_network_config config = {}) {
        seastar::chunked_vector<kwaque::simulation::fault_rule> rules;
        rules.push_back(network_rule(1, point, 1, decision));
        auto made_faults = kwaque::simulation::fault_schedule::make(
          events, trace, 17, std::move(rules));
        BOOST_REQUIRE(made_faults.has_value());
        faults = std::move(*made_faults);
        auto made_network = kwaque::simulation::fake_network::make(
          config, events, faults.get());
        BOOST_REQUIRE(made_network.has_value());
        network = std::move(*made_network);
    }
};

struct connection_fixture final {
    kwaque::simulation::fake_listener listener;
    kwaque::simulation::fake_connection client;
    kwaque::simulation::fake_connection server;
};

struct multi_connection_fixture final {
    kwaque::simulation::fake_listener listener;
    std::vector<kwaque::simulation::fake_connection> clients;
    std::vector<kwaque::simulation::fake_connection> servers;
};

seastar::future<connection_fixture> open_connection(
  kwaque::simulation::scheduler& events,
  kwaque::simulation::fake_network& network,
  kwaque::runtime::network_address address = alternate) {
    auto binding = network.listen(
      kwaque::runtime::network_endpoint{address, 0}, {});
    co_await pump_until(events, binding);
    auto bound = co_await std::move(binding);
    BOOST_REQUIRE(bound.has_value());
    auto listener = std::move(*bound);
    seastar::abort_source accept_abort;
    seastar::abort_source connect_abort;
    auto accepting = listener.accept(accept_abort);
    auto connecting = network.connect(
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
    co_return connection_fixture{
      .listener = std::move(listener),
      .client = std::move(*connected),
      .server = std::move(*accepted),
    };
}

seastar::future<connection_fixture>
open_connection(fault_environment& fixture) {
    co_return co_await open_connection(
      fixture.events, *fixture.network, alternate);
}

seastar::future<multi_connection_fixture> open_many_to_one(
  kwaque::simulation::scheduler& events,
  kwaque::simulation::fake_network& network,
  std::size_t count) {
    auto binding = network.listen(
      kwaque::runtime::network_endpoint{alternate, 0}, {});
    co_await pump_until(events, binding);
    auto bound = co_await std::move(binding);
    BOOST_REQUIRE(bound.has_value());
    multi_connection_fixture result{.listener = std::move(*bound)};
    result.clients.reserve(count);
    result.servers.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
        seastar::abort_source accept_abort;
        seastar::abort_source connect_abort;
        auto accepting = result.listener.accept(accept_abort);
        auto connecting = network.connect(
          result.listener.local_endpoint(),
          kwaque::runtime::network_endpoint{
            numbered_address(static_cast<std::uint8_t>(10U + index)), 0},
          kwaque::runtime::network_connection_limits{},
          connect_abort);
        co_await pump_until(events, connecting);
        co_await pump_until(events, accepting);
        auto connected = co_await std::move(connecting);
        auto accepted = co_await std::move(accepting);
        BOOST_REQUIRE(connected.has_value() && accepted.has_value());
        result.clients.push_back(std::move(*connected));
        result.servers.push_back(std::move(*accepted));
    }
    co_return result;
}

template<typename Future>
seastar::future<> drain_scheduler_until_idle(
  kwaque::simulation::scheduler& events, Future& future) {
    co_await pump_all(events);
    BOOST_CHECK(!future.available());
}

kwaque::runtime::fault_decision
decision_for(kwaque::runtime::fault_action action) {
    switch (action) {
    case kwaque::runtime::fault_action::error:
        return kwaque::runtime::fault_decision::make_error();
    case kwaque::runtime::fault_action::delay:
        return kwaque::runtime::fault_decision::make_delay(
          kwaque::runtime::monotonic_duration{25});
    case kwaque::runtime::fault_action::short_operation:
        return kwaque::runtime::fault_decision::make_short_operation(
          kwaque::byte_count{2});
    case kwaque::runtime::fault_action::drop:
        return kwaque::runtime::fault_decision::make_drop();
    case kwaque::runtime::fault_action::duplicate:
        return kwaque::runtime::fault_decision::make_duplicate();
    case kwaque::runtime::fault_action::reorder:
        return kwaque::runtime::fault_decision::make_reorder();
    case kwaque::runtime::fault_action::disconnect:
        return kwaque::runtime::fault_decision::make_disconnect();
    case kwaque::runtime::fault_action::corrupt:
        return kwaque::runtime::fault_decision::make_corrupt();
    case kwaque::runtime::fault_action::drop_completion:
        return kwaque::runtime::fault_decision::make_drop_completion();
    default:
        BOOST_FAIL("unsupported network fault action in contract matrix");
        return {};
    }
}

seastar::future<> capture_network_vocabulary(
  kwaque::simulation::event_trace& trace,
  kwaque::simulation::scheduler_limits scheduler_budget) {
    kwaque::simulation::scheduler events{scheduler_budget, &trace};
    seastar::chunked_vector<kwaque::simulation::fault_rule> rules;
    rules.push_back(
      network_write_rule(101, 2, kwaque::runtime::fault_decision::make_drop()));
    rules.push_back(network_write_rule(
      102, 3, kwaque::runtime::fault_decision::make_drop_completion()));
    rules.push_back(network_write_rule(
      103, 4, kwaque::runtime::fault_decision::make_disconnect()));
    auto made_faults = kwaque::simulation::fault_schedule::make(
      events, trace, 17, std::move(rules));
    BOOST_REQUIRE(made_faults.has_value());
    auto faults = std::move(*made_faults);
    auto config = kwaque::simulation::fake_network_config{};
    config.maximum_active_flows = 1;
    config.maximum_direction_bytes = kwaque::byte_count{64};
    config.latency_min = kwaque::runtime::monotonic_duration{};
    config.latency_mean_parameter = config.latency_min;
    auto made = kwaque::simulation::fake_network::make(
      config, events, faults.get());
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);
    auto first = co_await open_connection(events, *network, alternate);
    auto limiting = network->set_link_capacity(
      loopback,
      alternate,
      kwaque::simulation::bandwidth_capacity::finite(1'000));
    co_await pump_until(events, limiting);
    co_await require_ready_success(limiting);
    seastar::abort_source write_abort;
    seastar::abort_source read_abort;

    auto normal = first.client.write(
      kwaque::runtime::testing::network_contract_detail::make_bytes("normal"),
      write_abort);
    co_await pump_until(events, normal);
    co_await require_ready_success(normal);
    auto normal_read
      = kwaque::runtime::testing::network_contract_detail::read_exactly(
        first.server, 6, read_abort);
    co_await pump_until(events, normal_read);
    const auto normal_body = co_await std::move(normal_read);
    BOOST_REQUIRE(normal_body == "normal");

    auto dropped = first.client.write(
      kwaque::runtime::testing::network_contract_detail::make_bytes("drop"),
      write_abort);
    co_await pump_until(events, dropped);
    co_await require_ready_success(dropped);
    co_await pump_all(events);

    auto parked = first.client.write(
      kwaque::runtime::testing::network_contract_detail::make_bytes("park"),
      write_abort);
    co_await drain_scheduler_until_idle(events, parked);
    auto disconnected = first.client.write(
      kwaque::runtime::testing::network_contract_detail::make_bytes("reset"),
      write_abort);
    co_await pump_until(events, disconnected);
    const auto disconnected_result = co_await std::move(disconnected);
    BOOST_REQUIRE(!disconnected_result.has_value());
    BOOST_CHECK(
      disconnected_result.error().code() == kwaque::errc::network_failure);

    auto second = co_await open_connection(events, *network, third);
    auto final_write = second.client.write(
      kwaque::runtime::testing::network_contract_detail::make_bytes("fin"),
      write_abort);
    co_await pump_until(events, final_write);
    co_await require_ready_success(final_write);
    BOOST_REQUIRE(second.client.shutdown_output().has_value());
    auto body = kwaque::runtime::testing::network_contract_detail::read_exactly(
      second.server, 3, read_abort);
    co_await pump_until(events, body);
    const auto final_body = co_await std::move(body);
    BOOST_REQUIRE(final_body == "fin");
    auto eof = second.server.read(kwaque::byte_count{1}, read_abort);
    co_await pump_until(events, eof);
    auto eof_result = co_await std::move(eof);
    BOOST_REQUIRE(eof_result.has_value() && eof_result->eof());

    co_await stop_network(events, network);
    if (!parked.available()) {
        co_await pump_until(events, parked);
    }
    const auto parked_result = co_await std::move(parked);
    BOOST_REQUIRE(!parked_result.has_value());
    static_cast<void>(first);
    static_cast<void>(second);
}

enum class trace_mutation_kind : std::uint8_t {
    missing,
    extra,
    reordered,
};

enum class trace_field : std::uint8_t {
    sequence,
    time,
    deadline,
    action,
    kind,
    event_id,
    priority,
    domain,
    stable_id,
    coordinate_a,
    coordinate_b,
    value,
    result,
    context,
};

void mutate_trace_field(
  kwaque::simulation::trace_entry& entry, trace_field field) {
    switch (field) {
    case trace_field::sequence:
        entry.sequence ^= 1U;
        break;
    case trace_field::time:
        entry.time = kwaque::runtime::monotonic_time{
          entry.time.nanoseconds() + 1U};
        break;
    case trace_field::deadline:
        entry.deadline = kwaque::runtime::monotonic_time{
          entry.deadline.nanoseconds() + 1U};
        break;
    case trace_field::action:
        entry.action = entry.action
                           == kwaque::simulation::trace_action::scheduled
                         ? kwaque::simulation::trace_action::selected
                         : kwaque::simulation::trace_action::scheduled;
        break;
    case trace_field::kind:
        entry.kind = entry.kind == kwaque::simulation::trace_event_kind::network
                       ? kwaque::simulation::trace_event_kind::bandwidth
                       : kwaque::simulation::trace_event_kind::network;
        break;
    case trace_field::event_id:
        entry.event_id ^= 1U;
        break;
    case trace_field::priority:
        entry.priority ^= 1U;
        break;
    case trace_field::domain:
        entry.domain ^= 1U;
        break;
    case trace_field::stable_id:
        entry.stable_id ^= 1U;
        break;
    case trace_field::coordinate_a:
        entry.coordinate_a ^= 1U;
        break;
    case trace_field::coordinate_b:
        entry.coordinate_b ^= 1U;
        break;
    case trace_field::value:
        entry.value ^= 1U;
        break;
    case trace_field::result:
        entry.result ^= 1U;
        break;
    case trace_field::context:
        if (entry.context_size == 0U) {
            entry.context_size = 1U;
            entry.context[0] = kwaque::simulation::trace_context_field{
              .key = kwaque::simulation::trace_context_key::detail,
              .value = 1U,
            };
        } else {
            entry.context[0].value ^= 1U;
        }
        break;
    }
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
    co_await stop_network(events, network);
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
    co_await stop_network(events, network);
}

} // namespace

SEASTAR_TEST_CASE(fake_network_fault_table_rejects_inapplicable_actions) {
    struct invalid_row final {
        kwaque::runtime::builtin_fault_point point;
        kwaque::runtime::fault_decision decision;
    };
    const std::array rows{
      invalid_row{
        kwaque::runtime::builtin_fault_point::connect,
        kwaque::runtime::fault_decision::make_duplicate()},
      invalid_row{
        kwaque::runtime::builtin_fault_point::accept,
        kwaque::runtime::fault_decision::make_corrupt()},
      invalid_row{
        kwaque::runtime::builtin_fault_point::network_read,
        kwaque::runtime::fault_decision::make_duplicate()},
      invalid_row{
        kwaque::runtime::builtin_fault_point::network_write,
        kwaque::runtime::fault_decision::make_crash()},
      invalid_row{
        kwaque::runtime::builtin_fault_point::close,
        kwaque::runtime::fault_decision::make_disconnect()},
    };
    const auto occurrence = kwaque::runtime::fault_occurrence::make(1);
    BOOST_REQUIRE(occurrence.has_value());
    for (std::size_t index = 0; index < rows.size(); ++index) {
        const auto id = kwaque::simulation::fault_rule_id::make(index + 1U);
        BOOST_REQUIRE(id.has_value());
        const auto rejected = kwaque::simulation::fault_rule::make(
          *id,
          rows[index].point,
          std::nullopt,
          *occurrence,
          *occurrence,
          kwaque::simulation::fault_selector::once(),
          rows[index].decision);
        BOOST_REQUIRE(!rejected.has_value());
        BOOST_CHECK(rejected.error().code() == kwaque::errc::invalid_argument);
    }
    co_return;
}

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

    auto oversized_parked = kwaque::simulation::fake_network_config{};
    oversized_parked.maximum_parked_operations
      = kwaque::simulation::maximum_fake_network_parked_operations + 1U;
    BOOST_CHECK(
      !kwaque::simulation::fake_network::make(oversized_parked, events)
         .has_value());

    auto oversized_addresses = kwaque::simulation::fake_network_config{};
    oversized_addresses.maximum_address_entries
      = kwaque::simulation::maximum_fake_network_address_entries + 1U;
    BOOST_CHECK(
      !kwaque::simulation::fake_network::make(oversized_addresses, events)
         .has_value());

    auto deadline_incoherent = kwaque::simulation::fake_network_config{};
    deadline_incoherent.egress_capacity
      = kwaque::simulation::bandwidth_capacity::finite(1);
    BOOST_CHECK(
      !kwaque::simulation::fake_network::make(deadline_incoherent, events)
         .has_value());

    auto small_limits = kwaque::simulation::scheduler_limits::make(
      kwaque::simulation::scheduler_limit_values{
        .pending_events = 8'192,
        .events_per_pump = 4'096,
        .total_events = 100'000,
        .maximum_deadline = kwaque::runtime::monotonic_time{10'000'000'000ULL},
      });
    BOOST_REQUIRE(small_limits.has_value());
    kwaque::simulation::scheduler undersized{*small_limits};
    BOOST_CHECK(
      !kwaque::simulation::fake_network::make({}, undersized).has_value());
    co_return;
}

SEASTAR_TEST_CASE(fake_network_bounds_persistent_address_state) {
    kwaque::simulation::scheduler events{scheduler_limits()};
    auto config = kwaque::simulation::fake_network_config{};
    config.maximum_address_entries = 1;
    auto made = kwaque::simulation::fake_network::make(config, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);

    auto first = network->set_egress_capacity(
      loopback, kwaque::simulation::bandwidth_capacity::finite(1U << 30U));
    co_await pump_until(events, first);
    co_await require_ready_success(first);
    auto saturated = network->set_egress_capacity(
      alternate, kwaque::simulation::bandwidth_capacity::finite(1U << 30U));
    BOOST_REQUIRE(saturated.available());
    const auto rejected = saturated.get();
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == kwaque::errc::queue_full);

    auto released = network->set_egress_capacity(
      loopback, kwaque::simulation::bandwidth_capacity::unlimited());
    co_await pump_until(events, released);
    co_await require_ready_success(released);
    auto replacement = network->set_egress_capacity(
      alternate, kwaque::simulation::bandwidth_capacity::finite(1U << 29U));
    co_await pump_until(events, replacement);
    co_await require_ready_success(replacement);
    co_await stop_network(events, network);

    kwaque::simulation::scheduler port_events{scheduler_limits()};
    auto port_made = kwaque::simulation::fake_network::make(
      config, port_events);
    BOOST_REQUIRE(port_made.has_value());
    auto port_network = std::move(*port_made);
    auto binding = port_network->listen(
      kwaque::runtime::network_endpoint{loopback, 0}, {});
    co_await pump_until(port_events, binding);
    auto bound = co_await std::move(binding);
    BOOST_REQUIRE(bound.has_value());
    auto listener = std::move(*bound);
    auto second_address = port_network->listen(
      kwaque::runtime::network_endpoint{alternate, 0}, {});
    BOOST_REQUIRE(second_address.available());
    const auto address_rejected = second_address.get();
    BOOST_REQUIRE(!address_rejected.has_value());
    BOOST_CHECK(address_rejected.error().code() == kwaque::errc::queue_full);
    auto closing = listener.close();
    co_await pump_until(port_events, closing);
    co_await require_ready_success(closing);
    co_await stop_network(port_events, port_network);
}

SEASTAR_TEST_CASE(fake_network_bind_allocation_failure_is_transactional) {
    kwaque::simulation::scheduler events{scheduler_limits()};
    auto made = kwaque::simulation::fake_network::make({}, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);
    std::optional<seastar::future<
      kwaque::runtime::result<kwaque::simulation::fake_listener>>>
      accepted;
    bool unexpected_failure = false;
    seastar::memory::with_allocation_failures([&] {
        try {
            auto attempt = network->listen(
              kwaque::runtime::network_endpoint{alternate, 31'001}, {});
            if (!attempt.available()) {
                accepted.emplace(std::move(attempt));
                return;
            }
            const auto outcome = attempt.get();
            unexpected_failure = !outcome.has_value();
        } catch (const std::bad_alloc&) {
        }
    });
    BOOST_CHECK(!unexpected_failure);
    BOOST_REQUIRE(accepted.has_value());
    co_await pump_until(events, *accepted);
    auto bound = co_await std::move(*accepted);
    BOOST_REQUIRE(bound.has_value());
    auto listener = std::move(*bound);
    auto closing = listener.close();
    co_await pump_until(events, closing);
    co_await require_ready_success(closing);
    co_await stop_network(events, network);
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
    co_await stop_network(events, network);
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
    co_await stop_network(events, network);
    co_return;
}

SEASTAR_TEST_CASE(
  fake_network_scheduler_saturation_rejects_before_connect_admission) {
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

    seastar::chunked_vector<kwaque::simulation::event_id> blockers;
    while (true) {
        auto blocked = events.schedule(
          events.limits().maximum_deadline(),
          kwaque::simulation::event_priority::normal(),
          [] noexcept {});
        if (!blocked) {
            BOOST_CHECK(blocked.error().code() == kwaque::errc::queue_full);
            break;
        }
        blockers.push_back(*blocked);
    }
    seastar::abort_source connect_abort;
    auto rejected = network->connect(
      listener.local_endpoint(),
      std::nullopt,
      kwaque::runtime::network_connection_limits{},
      connect_abort);
    BOOST_REQUIRE(rejected.available());
    const auto outcome = rejected.get();
    BOOST_REQUIRE(!outcome.has_value());
    BOOST_CHECK(outcome.error().code() == kwaque::errc::queue_full);
    for (const auto id : blockers) {
        const auto canceled = events.cancel(id);
        BOOST_REQUIRE(canceled.has_value());
        BOOST_CHECK(*canceled);
    }
    auto closing = listener.close();
    co_await pump_until(events, closing);
    co_await require_ready_success(closing);
    co_await stop_network(events, network);
}

SEASTAR_TEST_CASE(
  fake_network_trace_saturation_rejects_before_connect_admission) {
    const auto scheduler_budget = bandwidth_scheduler_limits();
    const auto trace_budget = network_trace_limits();
    kwaque::simulation::event_trace trace{
      network_trace_header(scheduler_budget, trace_budget), trace_budget};
    kwaque::simulation::scheduler events{scheduler_budget, &trace};
    auto made = kwaque::simulation::fake_network::make({}, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);
    auto binding = network->listen(
      kwaque::runtime::network_endpoint{alternate, 0}, {});
    co_await pump_until(events, binding);
    auto bound = co_await std::move(binding);
    BOOST_REQUIRE(bound.has_value());
    auto listener = std::move(*bound);
    std::vector<kwaque::simulation::event_trace::reservation> blockers;
    while (true) {
        auto reserved = trace.reserve(
          1, kwaque::simulation::canonical_entry_encoded_size);
        if (!reserved) {
            BOOST_CHECK(
              reserved.error().code() == kwaque::errc::resource_exhausted);
            break;
        }
        blockers.push_back(std::move(*reserved));
    }
    seastar::abort_source connect_abort;
    auto connecting = network->connect(
      listener.local_endpoint(),
      std::nullopt,
      kwaque::runtime::network_connection_limits{},
      connect_abort);
    BOOST_REQUIRE(connecting.available());
    const auto rejected = connecting.get();
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == kwaque::errc::resource_exhausted);
    blockers.clear();
    auto closing = listener.close();
    co_await pump_until(events, closing);
    co_await require_ready_success(closing);
    co_await stop_network(events, network);
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
    co_await stop_network(events, network);
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
    co_await stop_network(events, network);
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
    co_await stop_network(events, network);
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
    co_await stop_network(events, network);
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
    co_await stop_network(events, network);
}

SEASTAR_TEST_CASE(fake_network_many_sources_share_receiver_ingress) {
    constexpr std::uint32_t connection_count = 4;
    kwaque::simulation::scheduler events{bandwidth_scheduler_limits()};
    auto config = kwaque::simulation::fake_network_config{};
    config.maximum_active_flows = connection_count;
    config.maximum_direction_bytes = kwaque::byte_count{1'000};
    config.latency_min = kwaque::runtime::monotonic_duration{};
    config.latency_mean_parameter = config.latency_min;
    auto made = kwaque::simulation::fake_network::make(config, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);
    auto connections = co_await open_many_to_one(
      events, *network, connection_count);
    auto limiting = network->set_ingress_capacity(
      alternate,
      kwaque::simulation::bandwidth_capacity::finite(
        connection_count * 1'000U));
    co_await pump_until(events, limiting);
    co_await require_ready_success(limiting);

    seastar::abort_source write_abort;
    std::vector<seastar::future<kwaque::runtime::result<void>>> writes;
    writes.reserve(connection_count);
    const auto started = events.now();
    for (auto& client : connections.clients) {
        writes.push_back(client.write(
          kwaque::runtime::testing::network_contract_detail::repeated_bytes(
            1'000, 'm'),
          write_abort));
    }
    auto next = events.advance_to_next();
    BOOST_REQUIRE(next.has_value() && next->has_value());
    const auto elapsed = (**next).checked_elapsed_since(started);
    BOOST_REQUIRE(elapsed.has_value());
    BOOST_CHECK_EQUAL(elapsed->nanoseconds(), 1'000'000'000ULL);
    BOOST_REQUIRE(events.run_ready().has_value());
    co_await seastar::yield();
    for (auto& writing : writes) {
        BOOST_REQUIRE(writing.available());
        co_await require_ready_success(writing);
    }
    seastar::abort_source read_abort;
    for (auto& server : connections.servers) {
        auto reading
          = kwaque::runtime::testing::network_contract_detail::read_exactly(
            server, 1'000, read_abort);
        co_await pump_until(events, reading);
        const auto received = co_await std::move(reading);
        BOOST_CHECK(received == std::string(1'000, 'm'));
    }
    co_await stop_network(events, network);
    static_cast<void>(connections);
}

SEASTAR_TEST_CASE(fake_network_one_source_shares_egress_across_destinations) {
    constexpr std::uint32_t connection_count = 4;
    kwaque::simulation::scheduler events{bandwidth_scheduler_limits()};
    auto config = kwaque::simulation::fake_network_config{};
    config.egress_capacity = kwaque::simulation::bandwidth_capacity::finite(
      connection_count * 1'000U);
    config.maximum_active_flows = connection_count;
    config.maximum_direction_bytes = kwaque::byte_count{1'000};
    config.latency_min = kwaque::runtime::monotonic_duration{};
    config.latency_mean_parameter = config.latency_min;
    auto made = kwaque::simulation::fake_network::make(config, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);
    std::vector<connection_fixture> connections;
    connections.reserve(connection_count);
    for (std::size_t index = 0; index < connection_count; ++index) {
        connections.push_back(
          co_await open_connection(
            events,
            *network,
            numbered_address(static_cast<std::uint8_t>(50U + index))));
    }

    seastar::abort_source write_abort;
    std::vector<seastar::future<kwaque::runtime::result<void>>> writes;
    writes.reserve(connection_count);
    const auto started = events.now();
    for (auto& connection : connections) {
        writes.push_back(connection.client.write(
          kwaque::runtime::testing::network_contract_detail::repeated_bytes(
            1'000, 'o'),
          write_abort));
    }
    auto next = events.advance_to_next();
    BOOST_REQUIRE(next.has_value() && next->has_value());
    const auto elapsed = (**next).checked_elapsed_since(started);
    BOOST_REQUIRE(elapsed.has_value());
    BOOST_CHECK_EQUAL(elapsed->nanoseconds(), 1'000'000'000ULL);
    BOOST_REQUIRE(events.run_ready().has_value());
    co_await seastar::yield();
    for (auto& writing : writes) {
        BOOST_REQUIRE(writing.available());
        co_await require_ready_success(writing);
    }
    seastar::abort_source read_abort;
    for (auto& connection : connections) {
        auto reading
          = kwaque::runtime::testing::network_contract_detail::read_exactly(
            connection.server, 1'000, read_abort);
        co_await pump_until(events, reading);
        const auto received = co_await std::move(reading);
        BOOST_CHECK(received == std::string(1'000, 'o'));
    }
    co_await stop_network(events, network);
    static_cast<void>(connections);
}

SEASTAR_TEST_CASE(
  fake_network_staggered_unequal_flows_rebalance_fractional_progress) {
    kwaque::simulation::scheduler events{bandwidth_scheduler_limits()};
    auto config = kwaque::simulation::fake_network_config{};
    config.egress_capacity = kwaque::simulation::bandwidth_capacity::finite(
      1'000);
    config.maximum_active_flows = 2;
    config.maximum_direction_bytes = kwaque::byte_count{2'000};
    config.latency_min = kwaque::runtime::monotonic_duration{};
    config.latency_mean_parameter = config.latency_min;
    auto made = kwaque::simulation::fake_network::make(config, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);
    auto first = co_await open_connection(events, *network, alternate);
    auto second = co_await open_connection(events, *network, third);
    seastar::abort_source write_abort;
    const auto started = events.now();
    auto first_write = first.client.write(
      kwaque::runtime::testing::network_contract_detail::repeated_bytes(
        1'000, 'a'),
      write_abort);
    const auto staggered = started.checked_add(
      kwaque::runtime::monotonic_duration{500'000'000});
    BOOST_REQUIRE(staggered.has_value());
    BOOST_REQUIRE(events.run_until(*staggered).has_value());
    auto second_write = second.client.write(
      kwaque::runtime::testing::network_contract_detail::repeated_bytes(
        1'500, 'b'),
      write_abort);

    const auto first_finish = started.checked_add(
      kwaque::runtime::monotonic_duration{1'500'000'000});
    BOOST_REQUIRE(first_finish.has_value());
    BOOST_REQUIRE(events.run_until(*first_finish).has_value());
    co_await seastar::yield();
    BOOST_REQUIRE(first_write.available());
    BOOST_CHECK(!second_write.available());
    co_await require_ready_success(first_write);

    const auto second_finish = started.checked_add(
      kwaque::runtime::monotonic_duration{2'500'000'000});
    BOOST_REQUIRE(second_finish.has_value());
    BOOST_REQUIRE(events.run_until(*second_finish).has_value());
    co_await seastar::yield();
    BOOST_REQUIRE(second_write.available());
    co_await require_ready_success(second_write);
    co_await stop_network(events, network);
    static_cast<void>(first);
    static_cast<void>(second);
}

SEASTAR_TEST_CASE(
  fake_network_fractional_carry_survives_repeated_real_rebalance) {
    kwaque::simulation::scheduler events{bandwidth_scheduler_limits()};
    auto config = kwaque::simulation::fake_network_config{};
    config.maximum_active_flows = 2;
    config.maximum_direction_bytes = kwaque::byte_count{10'000};
    config.latency_min = kwaque::runtime::monotonic_duration{};
    config.latency_mean_parameter = config.latency_min;
    auto made = kwaque::simulation::fake_network::make(config, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);
    auto connections = co_await open_many_to_one(events, *network, 2);
    auto limiting = network->set_ingress_capacity(
      alternate, kwaque::simulation::bandwidth_capacity::finite(30'000));
    co_await pump_until(events, limiting);
    co_await require_ready_success(limiting);
    seastar::abort_source write_abort;
    const auto started = events.now();
    auto small = connections.clients[0].write(
      kwaque::runtime::testing::network_contract_detail::make_bytes("s"),
      write_abort);
    auto large = connections.clients[1].write(
      kwaque::runtime::testing::network_contract_detail::repeated_bytes(
        10'000, 'l'),
      write_abort);
    auto next = events.advance_to_next();
    BOOST_REQUIRE(next.has_value() && next->has_value());
    auto elapsed = (**next).checked_elapsed_since(started);
    BOOST_REQUIRE(elapsed.has_value());
    BOOST_CHECK_EQUAL(elapsed->nanoseconds(), 66'667ULL);
    BOOST_REQUIRE(events.run_ready().has_value());
    co_await seastar::yield();
    BOOST_REQUIRE(small.available());
    BOOST_CHECK(!large.available());
    co_await require_ready_success(small);
    next = events.advance_to_next();
    BOOST_REQUIRE(next.has_value() && next->has_value());
    elapsed = (**next).checked_elapsed_since(started);
    BOOST_REQUIRE(elapsed.has_value());
    BOOST_CHECK_EQUAL(elapsed->nanoseconds(), 333'366'667ULL);
    BOOST_REQUIRE(events.run_ready().has_value());
    co_await seastar::yield();
    BOOST_REQUIRE(large.available());
    co_await require_ready_success(large);
    co_await stop_network(events, network);
    static_cast<void>(connections);
}

SEASTAR_TEST_CASE(
  fake_network_capacity_change_rebalances_active_and_new_transfers) {
    kwaque::simulation::scheduler events{bandwidth_scheduler_limits()};
    auto config = kwaque::simulation::fake_network_config{};
    config.egress_capacity = kwaque::simulation::bandwidth_capacity::finite(
      1'000);
    config.maximum_active_flows = 2;
    config.maximum_direction_bytes = kwaque::byte_count{2'000};
    config.latency_min = kwaque::runtime::monotonic_duration{};
    config.latency_mean_parameter = config.latency_min;
    auto made = kwaque::simulation::fake_network::make(config, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);
    auto first = co_await open_connection(events, *network, alternate);
    auto second = co_await open_connection(events, *network, third);
    seastar::abort_source write_abort;
    const auto started = events.now();
    auto first_write = first.client.write(
      kwaque::runtime::testing::network_contract_detail::repeated_bytes(
        1'000, 'a'),
      write_abort);
    const auto change_time = started.checked_add(
      kwaque::runtime::monotonic_duration{500'000'000});
    BOOST_REQUIRE(change_time.has_value());
    BOOST_REQUIRE(events.run_until(*change_time).has_value());
    auto increasing = network->set_egress_capacity(
      loopback, kwaque::simulation::bandwidth_capacity::finite(2'000));
    co_await pump_until(events, increasing);
    co_await require_ready_success(increasing);
    auto second_write = second.client.write(
      kwaque::runtime::testing::network_contract_detail::repeated_bytes(
        1'000, 'b'),
      write_abort);

    const auto first_finish = started.checked_add(
      kwaque::runtime::monotonic_duration{1'000'000'000});
    BOOST_REQUIRE(first_finish.has_value());
    BOOST_REQUIRE(events.run_until(*first_finish).has_value());
    co_await seastar::yield();
    BOOST_REQUIRE(first_write.available());
    BOOST_CHECK(!second_write.available());
    co_await require_ready_success(first_write);

    const auto second_finish = started.checked_add(
      kwaque::runtime::monotonic_duration{1'250'000'000});
    BOOST_REQUIRE(second_finish.has_value());
    BOOST_REQUIRE(events.run_until(*second_finish).has_value());
    co_await seastar::yield();
    BOOST_REQUIRE(second_write.available());
    co_await require_ready_success(second_write);
    co_await stop_network(events, network);
    static_cast<void>(first);
    static_cast<void>(second);
}

SEASTAR_TEST_CASE(fake_network_zero_endpoint_bandwidth_recovers_parked_flow) {
    for (const bool sender_limit : {true, false}) {
        kwaque::simulation::scheduler events{bandwidth_scheduler_limits()};
        auto config = kwaque::simulation::fake_network_config{};
        config.maximum_active_flows = 1;
        config.latency_min = kwaque::runtime::monotonic_duration{};
        config.latency_mean_parameter = config.latency_min;
        auto made = kwaque::simulation::fake_network::make(config, events);
        BOOST_REQUIRE(made.has_value());
        auto network = std::move(*made);
        auto connection = co_await open_connection(events, *network, alternate);
        auto zeroing = sender_limit
                         ? network->set_egress_capacity(
                             loopback,
                             kwaque::simulation::bandwidth_capacity::finite(0))
                         : network->set_ingress_capacity(
                             alternate,
                             kwaque::simulation::bandwidth_capacity::finite(0));
        co_await pump_until(events, zeroing);
        co_await require_ready_success(zeroing);
        seastar::abort_source write_abort;
        auto writing = connection.client.write(
          kwaque::runtime::testing::network_contract_detail::make_bytes(
            sender_limit ? "egress" : "ingress"),
          write_abort);
        BOOST_CHECK(!writing.available());
        BOOST_CHECK_EQUAL(events.pending_events(), 0U);
        auto releasing
          = sender_limit
              ? network->set_egress_capacity(
                  loopback, kwaque::simulation::bandwidth_capacity::unlimited())
              : network->set_ingress_capacity(
                  alternate,
                  kwaque::simulation::bandwidth_capacity::unlimited());
        co_await pump_until(events, releasing);
        co_await require_ready_success(releasing);
        co_await pump_until(events, writing);
        co_await require_ready_success(writing);
        co_await stop_network(events, network);
        static_cast<void>(connection);
    }
}

SEASTAR_TEST_CASE(fake_network_late_advance_drains_transfer_once) {
    kwaque::simulation::scheduler events{bandwidth_scheduler_limits()};
    auto config = kwaque::simulation::fake_network_config{};
    config.egress_capacity = kwaque::simulation::bandwidth_capacity::finite(
      1'000);
    config.maximum_active_flows = 1;
    config.maximum_direction_bytes = kwaque::byte_count{1'000};
    config.latency_min = kwaque::runtime::monotonic_duration{250'000'000};
    config.latency_mean_parameter = config.latency_min;
    auto made = kwaque::simulation::fake_network::make(config, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);
    auto connection = co_await open_connection(events, *network, alternate);
    seastar::abort_source write_abort;
    const auto started = events.now();
    auto writing = connection.client.write(
      kwaque::runtime::testing::network_contract_detail::repeated_bytes(
        1'000, 'l'),
      write_abort);
    const auto late = started.checked_add(
      kwaque::runtime::monotonic_duration{6'000'000'000});
    BOOST_REQUIRE(late.has_value());
    BOOST_REQUIRE(events.run_until(*late).has_value());
    co_await seastar::yield();
    BOOST_REQUIRE(writing.available());
    co_await require_ready_success(writing);
    seastar::abort_source read_abort;
    auto reading
      = kwaque::runtime::testing::network_contract_detail::read_exactly(
        connection.server, 1'000, read_abort);
    co_await pump_until(events, reading);
    const auto received = co_await std::move(reading);
    BOOST_CHECK(received == std::string(1'000, 'l'));
    BOOST_CHECK_EQUAL(events.pending_events(), 0U);
    const auto later = late->checked_add(
      kwaque::runtime::monotonic_duration{1'000'000'000});
    BOOST_REQUIRE(later.has_value());
    BOOST_REQUIRE(events.run_until(*later).has_value());
    BOOST_CHECK_EQUAL(events.pending_events(), 0U);
    const auto past = events.run_until(started);
    BOOST_REQUIRE(!past.has_value());
    BOOST_CHECK(past.error().code() == kwaque::errc::invalid_argument);
    co_await stop_network(events, network);
    static_cast<void>(connection);
}

SEASTAR_TEST_CASE(fake_network_huge_transfer_retains_one_bounded_wake) {
    auto limits = kwaque::simulation::scheduler_limits::make(
      kwaque::simulation::scheduler_limit_values{
        .pending_events = 65'536,
        .events_per_pump = 4'096,
        .total_events = 100'000,
        .maximum_deadline
        = kwaque::runtime::monotonic_time{20'000'000'000'000ULL},
      });
    BOOST_REQUIRE(limits.has_value());
    kwaque::simulation::scheduler events{*limits};
    auto config = kwaque::simulation::fake_network_config{};
    config.egress_capacity = kwaque::simulation::bandwidth_capacity::finite(1);
    config.maximum_active_flows = 1;
    config.maximum_direction_bytes = kwaque::byte_count{4'096};
    config.maximum_packet_logical_bytes = kwaque::byte_count{4'096};
    config.latency_min = kwaque::runtime::monotonic_duration{};
    config.latency_mean_parameter = config.latency_min;
    auto made = kwaque::simulation::fake_network::make(config, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);
    auto connection = co_await open_connection(events, *network, alternate);
    seastar::abort_source write_abort;
    const auto started = events.now();
    auto writing = connection.client.write(
      kwaque::runtime::testing::network_contract_detail::repeated_bytes(
        4'096, 'h'),
      write_abort);
    BOOST_CHECK(!writing.available());
    BOOST_CHECK_EQUAL(events.pending_events(), 1U);
    const auto next = events.advance_to_next();
    BOOST_REQUIRE(next.has_value() && next->has_value());
    const auto elapsed = (**next).checked_elapsed_since(started);
    BOOST_REQUIRE(elapsed.has_value());
    BOOST_CHECK_EQUAL(elapsed->nanoseconds(), 4'096'000'000'000ULL);
    auto stopping = network->stop();
    co_await pump_until(events, stopping);
    co_await require_ready_success(stopping);
    BOOST_REQUIRE(writing.available());
    const auto aborted = co_await std::move(writing);
    BOOST_REQUIRE(!aborted.has_value());
    BOOST_CHECK(aborted.error().code() == kwaque::errc::aborted);
    static_cast<void>(connection);
}

SEASTAR_TEST_CASE(
  fake_network_capacity_change_advances_old_rate_before_rebalance) {
    kwaque::simulation::scheduler events{bandwidth_scheduler_limits()};
    auto config = kwaque::simulation::fake_network_config{};
    config.link_capacity = kwaque::simulation::bandwidth_capacity::finite(
      1'000);
    config.maximum_active_flows = 1;
    config.maximum_direction_bytes = kwaque::byte_count{2'000};
    config.latency_min = kwaque::runtime::monotonic_duration{};
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
    BOOST_REQUIRE(connected.has_value() && accepted.has_value());
    auto client = std::move(*connected);
    auto server = std::move(*accepted);
    seastar::abort_source write_abort;
    auto writing = client.write(
      kwaque::runtime::testing::network_contract_detail::repeated_bytes(
        1'000, 'r'),
      write_abort);
    const auto started = events.now();
    const auto halfway_time = started.checked_add(
      kwaque::runtime::monotonic_duration{500'000'000});
    const auto finish_time = started.checked_add(
      kwaque::runtime::monotonic_duration{1'500'000'000});
    BOOST_REQUIRE(halfway_time.has_value() && finish_time.has_value());
    const auto halfway = events.run_until(*halfway_time);
    BOOST_REQUIRE(halfway.has_value());
    BOOST_CHECK(!writing.available());
    auto slowing = network->set_link_capacity(
      loopback, alternate, kwaque::simulation::bandwidth_capacity::finite(500));
    seastar::chunked_vector<kwaque::simulation::event_id> blockers;
    while (true) {
        auto blocked = events.schedule(
          events.limits().maximum_deadline(),
          kwaque::simulation::event_priority::normal(),
          [] noexcept {});
        if (!blocked) {
            BOOST_CHECK(blocked.error().code() == kwaque::errc::queue_full);
            break;
        }
        blockers.push_back(*blocked);
    }
    co_await pump_until(events, slowing);
    co_await require_ready_success(slowing);
    for (const auto id : blockers) {
        const auto canceled = events.cancel(id);
        BOOST_REQUIRE(canceled.has_value());
        BOOST_CHECK(*canceled);
    }
    const auto before_finish_time = finish_time->checked_sub(
      kwaque::runtime::monotonic_duration{1});
    BOOST_REQUIRE(before_finish_time.has_value());
    const auto before_finish = events.run_until(*before_finish_time);
    BOOST_REQUIRE(before_finish.has_value());
    BOOST_CHECK(!writing.available());
    const auto at_finish = events.run_until(*finish_time);
    BOOST_REQUIRE(at_finish.has_value());
    co_await seastar::yield();
    BOOST_REQUIRE(writing.available());
    co_await require_ready_success(writing);
    co_await stop_network(events, network);
    static_cast<void>(server);
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
    co_await stop_network(events, network);
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
    co_await stop_network(events, network);
}

SEASTAR_TEST_CASE(fake_network_connect_fault_terminal_matrix) {
    for (const auto action : {
           kwaque::runtime::fault_action::error,
           kwaque::runtime::fault_action::disconnect,
         }) {
        const auto decision
          = action == kwaque::runtime::fault_action::error
              ? kwaque::runtime::fault_decision::make_error()
              : kwaque::runtime::fault_decision::make_disconnect();
        fault_environment fixture{
          kwaque::runtime::builtin_fault_point::connect, decision};
        auto binding = fixture.network->listen(
          kwaque::runtime::network_endpoint{alternate, 0}, {});
        co_await pump_until(fixture.events, binding);
        auto bound = co_await std::move(binding);
        BOOST_REQUIRE(bound.has_value());
        auto listener = std::move(*bound);
        seastar::abort_source connect_abort;
        auto connecting = fixture.network->connect(
          listener.local_endpoint(),
          std::nullopt,
          kwaque::runtime::network_connection_limits{},
          connect_abort);
        co_await pump_until(fixture.events, connecting);
        const auto outcome = co_await std::move(connecting);
        BOOST_REQUIRE(!outcome.has_value());
        BOOST_CHECK(
          outcome.error().code()
          == (action == kwaque::runtime::fault_action::error ? kwaque::errc::fault_injected : kwaque::errc::network_failure));
        co_await stop_network(fixture.events, fixture.network);
    }

    fault_environment delayed{
      kwaque::runtime::builtin_fault_point::connect,
      kwaque::runtime::fault_decision::make_delay(
        kwaque::runtime::monotonic_duration{50})};
    auto delayed_connection = co_await open_connection(delayed);
    BOOST_CHECK(delayed.events.now().nanoseconds() >= 50U);
    co_await stop_network(delayed.events, delayed.network);
    static_cast<void>(delayed_connection);

    fault_environment parked{
      kwaque::runtime::builtin_fault_point::connect,
      kwaque::runtime::fault_decision::make_drop_completion()};
    auto binding = parked.network->listen(
      kwaque::runtime::network_endpoint{alternate, 0}, {});
    co_await pump_until(parked.events, binding);
    auto bound = co_await std::move(binding);
    BOOST_REQUIRE(bound.has_value());
    auto listener = std::move(*bound);
    seastar::abort_source accept_abort;
    seastar::abort_source connect_abort;
    auto accepting = listener.accept(accept_abort);
    auto connecting = parked.network->connect(
      listener.local_endpoint(),
      std::nullopt,
      kwaque::runtime::network_connection_limits{},
      connect_abort);
    co_await drain_scheduler_until_idle(parked.events, connecting);
    co_await stop_network(parked.events, parked.network);
    const auto stopped = co_await std::move(connecting);
    BOOST_REQUIRE(!stopped.has_value());
    BOOST_CHECK(stopped.error().code() == kwaque::errc::aborted);
    BOOST_CHECK(accepting.available());
    static_cast<void>(accepting);
}

SEASTAR_TEST_CASE(fake_network_accept_fault_terminal_matrix) {
    for (const auto action : {
           kwaque::runtime::fault_action::error,
           kwaque::runtime::fault_action::disconnect,
         }) {
        const auto decision
          = action == kwaque::runtime::fault_action::error
              ? kwaque::runtime::fault_decision::make_error()
              : kwaque::runtime::fault_decision::make_disconnect();
        fault_environment fixture{
          kwaque::runtime::builtin_fault_point::accept, decision};
        auto binding = fixture.network->listen(
          kwaque::runtime::network_endpoint{alternate, 0}, {});
        co_await pump_until(fixture.events, binding);
        auto bound = co_await std::move(binding);
        BOOST_REQUIRE(bound.has_value());
        auto listener = std::move(*bound);
        seastar::abort_source accept_abort;
        seastar::abort_source connect_abort;
        auto accepting = listener.accept(accept_abort);
        auto connecting = fixture.network->connect(
          listener.local_endpoint(),
          std::nullopt,
          kwaque::runtime::network_connection_limits{},
          connect_abort);
        co_await pump_until(fixture.events, accepting);
        const auto accepted = co_await std::move(accepting);
        BOOST_REQUIRE(!accepted.has_value());
        BOOST_CHECK(
          accepted.error().code()
          == (action == kwaque::runtime::fault_action::error ? kwaque::errc::fault_injected : kwaque::errc::network_failure));
        if (!connecting.available()) {
            co_await pump_until(fixture.events, connecting);
        }
        static_cast<void>(connecting);
        co_await stop_network(fixture.events, fixture.network);
    }

    fault_environment delayed{
      kwaque::runtime::builtin_fault_point::accept,
      kwaque::runtime::fault_decision::make_delay(
        kwaque::runtime::monotonic_duration{50})};
    auto delayed_connection = co_await open_connection(delayed);
    BOOST_CHECK(delayed.events.now().nanoseconds() >= 50U);
    co_await stop_network(delayed.events, delayed.network);
    static_cast<void>(delayed_connection);

    fault_environment parked{
      kwaque::runtime::builtin_fault_point::accept,
      kwaque::runtime::fault_decision::make_drop_completion()};
    auto binding = parked.network->listen(
      kwaque::runtime::network_endpoint{alternate, 0}, {});
    co_await pump_until(parked.events, binding);
    auto bound = co_await std::move(binding);
    BOOST_REQUIRE(bound.has_value());
    auto listener = std::move(*bound);
    seastar::abort_source accept_abort;
    seastar::abort_source connect_abort;
    auto accepting = listener.accept(accept_abort);
    auto connecting = parked.network->connect(
      listener.local_endpoint(),
      std::nullopt,
      kwaque::runtime::network_connection_limits{},
      connect_abort);
    co_await pump_until(parked.events, connecting);
    auto connected = co_await std::move(connecting);
    BOOST_REQUIRE(connected.has_value());
    co_await drain_scheduler_until_idle(parked.events, accepting);
    co_await stop_network(parked.events, parked.network);
    const auto stopped = co_await std::move(accepting);
    BOOST_REQUIRE(!stopped.has_value());
    BOOST_CHECK(stopped.error().code() == kwaque::errc::aborted);
    static_cast<void>(connected);
}

SEASTAR_TEST_CASE(fake_network_write_fault_terminal_matrix) {
    for (const auto action : {
           kwaque::runtime::fault_action::error,
           kwaque::runtime::fault_action::delay,
           kwaque::runtime::fault_action::short_operation,
           kwaque::runtime::fault_action::drop,
           kwaque::runtime::fault_action::duplicate,
           kwaque::runtime::fault_action::reorder,
           kwaque::runtime::fault_action::disconnect,
           kwaque::runtime::fault_action::corrupt,
           kwaque::runtime::fault_action::drop_completion,
         }) {
        fault_environment fixture{
          kwaque::runtime::builtin_fault_point::network_write,
          decision_for(action)};
        auto connection = co_await open_connection(fixture);
        seastar::abort_source write_abort;
        auto writing = connection.client.write(
          kwaque::runtime::testing::network_contract_detail::make_bytes("abcd"),
          write_abort);
        if (action == kwaque::runtime::fault_action::drop_completion) {
            co_await drain_scheduler_until_idle(fixture.events, writing);
            co_await stop_network(fixture.events, fixture.network);
            const auto stopped = co_await std::move(writing);
            BOOST_REQUIRE(!stopped.has_value());
            BOOST_CHECK(stopped.error().code() == kwaque::errc::aborted);
            continue;
        }
        co_await pump_until(fixture.events, writing);
        const auto written = co_await std::move(writing);
        if (
          action == kwaque::runtime::fault_action::error
          || action == kwaque::runtime::fault_action::disconnect) {
            BOOST_REQUIRE(!written.has_value());
            BOOST_CHECK(
              written.error().code()
              == (action == kwaque::runtime::fault_action::error ? kwaque::errc::fault_injected : kwaque::errc::network_failure));
        } else {
            BOOST_REQUIRE(written.has_value());
            if (action != kwaque::runtime::fault_action::drop) {
                seastar::abort_source read_abort;
                const auto expected_bytes
                  = action == kwaque::runtime::fault_action::duplicate ? 8U
                    : action == kwaque::runtime::fault_action::short_operation
                      ? 2U
                      : 4U;
                auto reading = kwaque::runtime::testing::
                  network_contract_detail::read_exactly(
                    connection.server, expected_bytes, read_abort);
                co_await pump_until(fixture.events, reading);
                const auto bytes = co_await std::move(reading);
                if (action == kwaque::runtime::fault_action::duplicate) {
                    BOOST_CHECK(bytes == "abcdabcd");
                } else if (
                  action == kwaque::runtime::fault_action::short_operation) {
                    BOOST_CHECK(bytes == "ab");
                } else if (action == kwaque::runtime::fault_action::corrupt) {
                    BOOST_CHECK(bytes.size() == 4U);
                    BOOST_CHECK(bytes != "abcd");
                } else {
                    BOOST_CHECK(bytes == "abcd");
                }
            }
        }
        co_await stop_network(fixture.events, fixture.network);
        static_cast<void>(connection);
    }
}

SEASTAR_TEST_CASE(fake_network_read_fault_terminal_matrix) {
    for (const auto action : {
           kwaque::runtime::fault_action::error,
           kwaque::runtime::fault_action::delay,
           kwaque::runtime::fault_action::short_operation,
           kwaque::runtime::fault_action::drop,
           kwaque::runtime::fault_action::disconnect,
           kwaque::runtime::fault_action::corrupt,
           kwaque::runtime::fault_action::drop_completion,
         }) {
        fault_environment fixture{
          kwaque::runtime::builtin_fault_point::network_read,
          decision_for(action)};
        auto connection = co_await open_connection(fixture);
        seastar::abort_source write_abort;
        auto writing = connection.client.write(
          kwaque::runtime::testing::network_contract_detail::make_bytes("abcd"),
          write_abort);
        co_await pump_until(fixture.events, writing);
        co_await require_ready_success(writing);
        seastar::abort_source read_abort;
        auto reading = connection.server.read(
          kwaque::byte_count{4}, read_abort);
        if (action == kwaque::runtime::fault_action::drop) {
            auto following = connection.client.write(
              kwaque::runtime::testing::network_contract_detail::make_bytes(
                "next"),
              write_abort);
            co_await pump_until(fixture.events, following);
            co_await require_ready_success(following);
        }
        if (action == kwaque::runtime::fault_action::drop_completion) {
            co_await drain_scheduler_until_idle(fixture.events, reading);
            co_await stop_network(fixture.events, fixture.network);
            const auto stopped = co_await std::move(reading);
            BOOST_REQUIRE(!stopped.has_value());
            BOOST_CHECK(stopped.error().code() == kwaque::errc::aborted);
            continue;
        }
        co_await pump_until(fixture.events, reading);
        auto result = co_await std::move(reading);
        if (
          action == kwaque::runtime::fault_action::error
          || action == kwaque::runtime::fault_action::disconnect) {
            BOOST_REQUIRE(!result.has_value());
            BOOST_CHECK(
              result.error().code()
              == (action == kwaque::runtime::fault_action::error ? kwaque::errc::fault_injected : kwaque::errc::network_failure));
        } else {
            BOOST_REQUIRE(result.has_value());
            if (action == kwaque::runtime::fault_action::short_operation) {
                BOOST_CHECK(result->data().content_equals("ab"));
            } else if (action == kwaque::runtime::fault_action::drop) {
                BOOST_CHECK(result->data().content_equals("next"));
            } else if (action == kwaque::runtime::fault_action::corrupt) {
                BOOST_CHECK(result->data().size() == kwaque::byte_count{4});
                BOOST_CHECK(!result->data().content_equals("abcd"));
            } else {
                BOOST_CHECK(result->data().content_equals("abcd"));
            }
        }
        co_await stop_network(fixture.events, fixture.network);
        static_cast<void>(connection);
    }
}

SEASTAR_TEST_CASE(fake_network_close_fault_terminal_matrix) {
    for (const auto action : {
           kwaque::runtime::fault_action::error,
           kwaque::runtime::fault_action::delay,
           kwaque::runtime::fault_action::drop_completion,
         }) {
        fault_environment fixture{
          kwaque::runtime::builtin_fault_point::close, decision_for(action)};
        auto connection = co_await open_connection(fixture);
        auto closing = connection.client.close();
        if (action == kwaque::runtime::fault_action::drop_completion) {
            co_await drain_scheduler_until_idle(fixture.events, closing);
            co_await stop_network(fixture.events, fixture.network);
            const auto stopped = co_await std::move(closing);
            BOOST_REQUIRE(!stopped.has_value());
            BOOST_CHECK(stopped.error().code() == kwaque::errc::aborted);
            continue;
        }
        co_await pump_until(fixture.events, closing);
        const auto result = co_await std::move(closing);
        if (action == kwaque::runtime::fault_action::error) {
            BOOST_REQUIRE(!result.has_value());
            BOOST_CHECK(result.error().code() == kwaque::errc::fault_injected);
        } else {
            BOOST_REQUIRE(result.has_value());
        }
        co_await stop_network(fixture.events, fixture.network);
        static_cast<void>(connection);
    }
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
    co_await stop_network(events, network);
}

SEASTAR_TEST_CASE(
  fake_network_stop_batches_active_queued_and_parked_ownership) {
    const auto scheduler_budget = bandwidth_scheduler_limits();
    const auto trace_budget = network_trace_limits();
    kwaque::simulation::event_trace trace{
      network_trace_header(scheduler_budget, trace_budget), trace_budget};
    kwaque::simulation::scheduler events{scheduler_budget, &trace};
    auto config = kwaque::simulation::fake_network_config{};
    config.maximum_listeners = 1;
    config.maximum_connection_pairs = 1;
    config.maximum_pending_connects = 1;
    config.maximum_backlog_entries = 1;
    config.maximum_operations = 4;
    config.maximum_parked_operations = 4;
    config.maximum_packets = 2;
    config.maximum_direction_packets = 2;
    config.maximum_links = 1;
    config.maximum_address_entries = 4;
    config.maximum_active_flows = 1;
    config.maximum_controls = 1;
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
    rules.push_back(network_write_rule(
      2, 2, kwaque::runtime::fault_decision::make_drop_completion()));
    auto made_faults = kwaque::simulation::fault_schedule::make(
      events, trace, 17, std::move(rules));
    BOOST_REQUIRE(made_faults.has_value());
    auto faults = std::move(*made_faults);
    auto config = kwaque::simulation::fake_network_config{};
    config.stop_batch = 1;
    config.maximum_parked_operations = 1;
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
    co_await pump_all(events);
    BOOST_CHECK(!writing.available());
    auto rejected = client.write(
      kwaque::runtime::testing::network_contract_detail::make_bytes("second"),
      write_abort);
    BOOST_REQUIRE(rejected.available());
    const auto parked_full = rejected.get();
    BOOST_REQUIRE(!parked_full.has_value());
    BOOST_CHECK(parked_full.error().code() == kwaque::errc::queue_full);
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

SEASTAR_TEST_CASE(fake_network_schema_five_trace_replays_fresh) {
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

SEASTAR_TEST_CASE(
  fake_network_trace_rejects_structural_and_every_field_mutation_at_boundaries) {
    struct required_boundary final {
        kwaque::simulation::trace_action action;
        kwaque::simulation::trace_event_kind kind;
        std::uint32_t domain;
    };
    constexpr std::array required_boundaries{
      required_boundary{
        kwaque::simulation::trace_action::network_operation_applied,
        kwaque::simulation::trace_event_kind::network,
        static_cast<std::uint32_t>(
          kwaque::simulation::network_trace_phase::write)},
      required_boundary{
        kwaque::simulation::trace_action::flow_started,
        kwaque::simulation::trace_event_kind::bandwidth,
        static_cast<std::uint32_t>(
          kwaque::simulation::bandwidth_trace_phase::flow_start)},
      required_boundary{
        kwaque::simulation::trace_action::bandwidth_rebalanced,
        kwaque::simulation::trace_event_kind::bandwidth,
        static_cast<std::uint32_t>(
          kwaque::simulation::bandwidth_trace_phase::rebalance)},
      required_boundary{
        kwaque::simulation::trace_action::transfer_completed,
        kwaque::simulation::trace_event_kind::bandwidth,
        static_cast<std::uint32_t>(
          kwaque::simulation::bandwidth_trace_phase::transfer_done)},
      required_boundary{
        kwaque::simulation::trace_action::packet_delivered,
        kwaque::simulation::trace_event_kind::network,
        static_cast<std::uint32_t>(
          kwaque::simulation::network_trace_phase::delivery)},
      required_boundary{
        kwaque::simulation::trace_action::packet_dropped,
        kwaque::simulation::trace_event_kind::network,
        static_cast<std::uint32_t>(
          kwaque::simulation::network_trace_phase::delivery)},
      required_boundary{
        kwaque::simulation::trace_action::network_operation_applied,
        kwaque::simulation::trace_event_kind::network,
        static_cast<std::uint32_t>(
          kwaque::simulation::network_trace_phase::sequence_release)},
      required_boundary{
        kwaque::simulation::trace_action::fin_delivered,
        kwaque::simulation::trace_event_kind::network,
        static_cast<std::uint32_t>(
          kwaque::simulation::network_trace_phase::fin)},
      required_boundary{
        kwaque::simulation::trace_action::reset_applied,
        kwaque::simulation::trace_event_kind::network,
        static_cast<std::uint32_t>(
          kwaque::simulation::network_trace_phase::reset)},
      required_boundary{
        kwaque::simulation::trace_action::network_control_applied,
        kwaque::simulation::trace_event_kind::network_control,
        static_cast<std::uint32_t>(
          kwaque::simulation::network_control_trace_phase::link_limit)},
      required_boundary{
        kwaque::simulation::trace_action::operation_parked,
        kwaque::simulation::trace_event_kind::network,
        static_cast<std::uint32_t>(
          kwaque::simulation::network_trace_phase::parked)},
      required_boundary{
        kwaque::simulation::trace_action::stop_terminal,
        kwaque::simulation::trace_event_kind::network,
        static_cast<std::uint32_t>(
          kwaque::simulation::network_trace_phase::stop)},
    };
    constexpr std::array structural_mutations{
      trace_mutation_kind::missing,
      trace_mutation_kind::extra,
      trace_mutation_kind::reordered,
    };
    constexpr std::array field_mutations{
      trace_field::sequence,
      trace_field::time,
      trace_field::deadline,
      trace_field::action,
      trace_field::kind,
      trace_field::event_id,
      trace_field::priority,
      trace_field::domain,
      trace_field::stable_id,
      trace_field::coordinate_a,
      trace_field::coordinate_b,
      trace_field::value,
      trace_field::result,
      trace_field::context,
    };
    const auto scheduler_budget = bandwidth_scheduler_limits();
    const auto trace_budget = network_trace_limits();
    const auto header = network_trace_header(scheduler_budget, trace_budget);
    kwaque::simulation::event_trace captured{header, trace_budget};
    co_await capture_network_vocabulary(captured, scheduler_budget);
    auto encoded = captured.encode();
    BOOST_REQUIRE(encoded.has_value());

    std::vector<std::size_t> boundaries;
    boundaries.reserve(required_boundaries.size());
    for (const auto boundary : required_boundaries) {
        const auto found = std::ranges::find_if(
          captured.entries(), [boundary](const auto& entry) {
              return entry.action == boundary.action
                     && entry.kind == boundary.kind
                     && entry.domain == boundary.domain;
          });
        BOOST_REQUIRE(found != captured.entries().end());
        boundaries.push_back(
          static_cast<std::size_t>(
            std::distance(captured.entries().begin(), found)));
    }

    auto require_first_divergence = [&](auto artifact, std::size_t boundary) {
        auto replay = kwaque::simulation::event_trace::replay(
          header, trace_budget, std::move(artifact));
        if (!replay) {
            return;
        }
        bool diverged = false;
        for (std::size_t index = 0; index <= boundary; ++index) {
            const auto observed = (*replay)->observe(captured.entries()[index]);
            if (index != boundary) {
                BOOST_REQUIRE(observed.has_value());
                continue;
            }
            BOOST_REQUIRE(!observed.has_value());
            BOOST_CHECK(
              observed.error().code() == kwaque::errc::replay_divergence);
            diverged = true;
        }
        BOOST_REQUIRE(diverged);
    };

    for (const auto boundary : boundaries) {
        for (const auto mutation : structural_mutations) {
            auto decoded = kwaque::simulation::event_trace::decode(
              *encoded, trace_budget);
            BOOST_REQUIRE(decoded.has_value());
            const bool reorder_from_predecessor
              = mutation == trace_mutation_kind::reordered
                && boundary + 1U == decoded->entries.size();
            if (reorder_from_predecessor) {
                BOOST_REQUIRE(boundary != 0U);
            }
            const auto mutation_boundary = reorder_from_predecessor
                                             ? boundary - 1U
                                             : boundary;
            seastar::chunked_vector<kwaque::simulation::trace_entry> changed;
            changed.reserve(
              decoded->entries.size()
              + static_cast<std::size_t>(mutation == trace_mutation_kind::extra)
              - static_cast<std::size_t>(
                mutation == trace_mutation_kind::missing));
            for (std::size_t index = 0; index < decoded->entries.size();
                 ++index) {
                if (
                  index == boundary
                  && mutation == trace_mutation_kind::missing) {
                    continue;
                }
                if (
                  index == boundary && mutation == trace_mutation_kind::extra) {
                    auto inserted = decoded->entries[index];
                    inserted.stable_id ^= 1U;
                    changed.push_back(inserted);
                }
                if (
                  index == mutation_boundary
                  && mutation == trace_mutation_kind::reordered) {
                    changed.push_back(decoded->entries[index + 1U]);
                    changed.push_back(decoded->entries[index]);
                    ++index;
                    continue;
                }
                changed.push_back(decoded->entries[index]);
            }
            for (std::size_t index = 0; index < changed.size(); ++index) {
                changed[index].sequence = index + 1U;
            }
            decoded->encoded_bytes
              = kwaque::simulation::canonical_header_encoded_size
                + changed.size()
                    * kwaque::simulation::canonical_entry_encoded_size;
            decoded->entries = std::move(changed);
            require_first_divergence(std::move(*decoded), mutation_boundary);
        }
        for (const auto field : field_mutations) {
            auto decoded = kwaque::simulation::event_trace::decode(
              *encoded, trace_budget);
            BOOST_REQUIRE(decoded.has_value());
            mutate_trace_field(decoded->entries[boundary], field);
            require_first_divergence(std::move(*decoded), boundary);
        }
        co_await seastar::maybe_yield();
    }
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
        co_await stop_network(events, network);
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

SEASTAR_TEST_CASE(
  fake_network_replay_diverges_before_rebalance_publication_or_wake_replacement) {
    const auto scheduler_budget = bandwidth_scheduler_limits();
    const auto trace_budget = network_trace_limits();
    const auto header = network_trace_header(scheduler_budget, trace_budget);
    auto config = kwaque::simulation::fake_network_config{};
    config.link_capacity = kwaque::simulation::bandwidth_capacity::finite(
      1'000);
    config.maximum_active_flows = 1;
    config.maximum_direction_bytes = kwaque::byte_count{2'000};
    config.latency_min = kwaque::runtime::monotonic_duration{};
    config.latency_mean_parameter = config.latency_min;
    std::uint64_t changed_deadline = 0;

    kwaque::simulation::event_trace captured{header, trace_budget};
    {
        kwaque::simulation::scheduler events{scheduler_budget, &captured};
        auto made = kwaque::simulation::fake_network::make(config, events);
        BOOST_REQUIRE(made.has_value());
        auto network = std::move(*made);
        auto connection = co_await open_connection(events, *network);
        seastar::abort_source write_abort;
        auto writing = connection.client.write(
          kwaque::runtime::testing::network_contract_detail::repeated_bytes(
            1'000, 'r'),
          write_abort);
        const auto halfway = events.now().checked_add(
          kwaque::runtime::monotonic_duration{500'000'000});
        const auto finish = events.now().checked_add(
          kwaque::runtime::monotonic_duration{1'500'000'000});
        BOOST_REQUIRE(halfway.has_value() && finish.has_value());
        changed_deadline = finish->nanoseconds();
        BOOST_REQUIRE(events.run_until(*halfway).has_value());
        auto slowing = network->set_link_capacity(
          loopback,
          alternate,
          kwaque::simulation::bandwidth_capacity::finite(500));
        co_await pump_until(events, slowing);
        co_await require_ready_success(slowing);
        co_await pump_until(events, writing);
        co_await require_ready_success(writing);
        co_await stop_network(events, network);
        static_cast<void>(connection);
    }

    auto artifact = captured.encode();
    BOOST_REQUIRE(artifact.has_value());
    auto decoded = kwaque::simulation::event_trace::decode(
      std::move(*artifact), trace_budget);
    BOOST_REQUIRE(decoded.has_value());
    bool mutated = false;
    for (auto& entry : decoded->entries) {
        if (
          entry.action == kwaque::simulation::trace_action::bandwidth_rebalanced
          && entry.value == changed_deadline && entry.context_size == 4U) {
            entry.context[0].value ^= 1U;
            mutated = true;
            break;
        }
    }
    BOOST_REQUIRE(mutated);
    auto replay = kwaque::simulation::event_trace::replay(
      header, trace_budget, std::move(*decoded));
    BOOST_REQUIRE(replay.has_value());

    kwaque::simulation::scheduler events{scheduler_budget, replay->get()};
    auto made = kwaque::simulation::fake_network::make(config, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);
    auto connection = co_await open_connection(events, *network);
    seastar::abort_source write_abort;
    auto writing = connection.client.write(
      kwaque::runtime::testing::network_contract_detail::repeated_bytes(
        1'000, 'r'),
      write_abort);
    const auto allocation_before = network->allocation_digest();
    const auto halfway = events.now().checked_add(
      kwaque::runtime::monotonic_duration{500'000'000});
    BOOST_REQUIRE(halfway.has_value());
    BOOST_REQUIRE(events.run_until(*halfway).has_value());
    auto slowing = network->set_link_capacity(
      loopback, alternate, kwaque::simulation::bandwidth_capacity::finite(500));
    BOOST_REQUIRE(events.has_ready_events());
    const auto divergent = events.run_ready();
    BOOST_REQUIRE(!divergent.has_value());
    BOOST_CHECK(divergent.error().code() == kwaque::errc::replay_divergence);
    BOOST_CHECK(network->allocation_digest() == allocation_before);
    BOOST_CHECK_EQUAL(events.pending_events(), 1U);
    BOOST_CHECK(!writing.available());
    co_await seastar::yield();
    BOOST_REQUIRE(slowing.available());
    const auto rejected = slowing.get();
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == kwaque::errc::replay_divergence);
    kwaque::simulation::fake_network_test_access::force_discard(
      *network, divergent.error());
    BOOST_CHECK(
      network->state() == kwaque::simulation::fake_network_state::stopped);
    BOOST_REQUIRE(writing.available());
    const auto aborted = co_await std::move(writing);
    BOOST_REQUIRE(!aborted.has_value());
    BOOST_CHECK(aborted.error().code() == kwaque::errc::replay_divergence);
    static_cast<void>(connection);
}

SEASTAR_TEST_CASE(
  fake_network_replay_diverges_before_packet_delivery_and_read_wake) {
    const auto scheduler_budget = bandwidth_scheduler_limits();
    const auto trace_budget = network_trace_limits();
    const auto header = network_trace_header(scheduler_budget, trace_budget);
    auto config = kwaque::simulation::fake_network_config{};
    config.latency_min = kwaque::runtime::monotonic_duration{10};
    config.latency_mean_parameter = config.latency_min;
    kwaque::simulation::event_trace captured{header, trace_budget};
    {
        kwaque::simulation::scheduler events{scheduler_budget, &captured};
        auto made = kwaque::simulation::fake_network::make(config, events);
        BOOST_REQUIRE(made.has_value());
        auto network = std::move(*made);
        auto connection = co_await open_connection(events, *network);
        seastar::abort_source write_abort;
        seastar::abort_source read_abort;
        auto writing = connection.client.write(
          kwaque::runtime::testing::network_contract_detail::make_bytes("p"),
          write_abort);
        co_await pump_until(events, writing);
        co_await require_ready_success(writing);
        auto reading = connection.server.read(
          kwaque::byte_count{1}, read_abort);
        co_await pump_until(events, reading);
        auto result = co_await std::move(reading);
        BOOST_REQUIRE(result.has_value());
        BOOST_CHECK(result->data().content_equals("p"));
        co_await stop_network(events, network);
        static_cast<void>(connection);
    }
    auto encoded = captured.encode();
    BOOST_REQUIRE(encoded.has_value());
    auto decoded = kwaque::simulation::event_trace::decode(
      std::move(*encoded), trace_budget);
    BOOST_REQUIRE(decoded.has_value());
    const auto boundary = std::ranges::find_if(
      decoded->entries, [](const auto& entry) {
          return entry.action
                 == kwaque::simulation::trace_action::packet_delivered;
      });
    BOOST_REQUIRE(boundary != decoded->entries.end());
    boundary->coordinate_a ^= 1U;
    auto replay = kwaque::simulation::event_trace::replay(
      header, trace_budget, std::move(*decoded));
    BOOST_REQUIRE(replay.has_value());

    kwaque::simulation::scheduler events{scheduler_budget, replay->get()};
    auto made = kwaque::simulation::fake_network::make(config, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);
    auto connection = co_await open_connection(events, *network);
    seastar::abort_source write_abort;
    seastar::abort_source read_abort;
    auto writing = connection.client.write(
      kwaque::runtime::testing::network_contract_detail::make_bytes("p"),
      write_abort);
    co_await pump_until(events, writing);
    co_await require_ready_success(writing);
    auto reading = connection.server.read(kwaque::byte_count{1}, read_abort);
    const auto advanced = events.advance_to_next();
    BOOST_REQUIRE(advanced.has_value() && advanced->has_value());
    const auto divergent = events.run_ready();
    BOOST_REQUIRE(!divergent.has_value());
    BOOST_CHECK(divergent.error().code() == kwaque::errc::replay_divergence);
    BOOST_CHECK(!reading.available());
    kwaque::simulation::fake_network_test_access::force_discard(
      *network, divergent.error());
    BOOST_REQUIRE(reading.available());
    const auto rejected = co_await std::move(reading);
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == kwaque::errc::replay_divergence);
    static_cast<void>(connection);
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
    co_await stop_network(events, network);
}

SEASTAR_TEST_CASE(fake_network_seeded_histories_reconcile_with_dense_oracle) {
    using kwaque::simulation::testing::dense_network_oracle;
    using kwaque::simulation::testing::oracle_step;
    using kwaque::simulation::testing::oracle_step_kind;

    for (std::uint64_t seed = 1; seed <= 128; ++seed) {
        auto source = kwaque::simulation::deterministic_random{seed}.stream(
          kwaque::simulation::random_domain::network_decision,
          UINT64_C(0x5245414c4e455431));
        BOOST_REQUIRE(source.has_value());
        const auto latency = *kwaque::runtime::uniform_u64(*source, 8);
        const bool wildcard_first = seed % 2U == 0U;
        const bool explicit_local = seed % 3U == 0U;
        std::string history = "seed=" + std::to_string(seed)
                              + " latency=" + std::to_string(latency)
                              + " wildcard=" + std::to_string(wildcard_first)
                              + " explicit=" + std::to_string(explicit_local);
        kwaque::simulation::scheduler events{bandwidth_scheduler_limits()};
        auto config = kwaque::simulation::fake_network_config{};
        config.maximum_listeners = 2;
        config.maximum_connection_pairs = 2;
        config.maximum_pending_connects = 2;
        config.maximum_backlog_entries = 2;
        config.maximum_operations = 8;
        config.maximum_parked_operations = 4;
        config.maximum_packets = 64;
        config.maximum_direction_packets = 64;
        config.maximum_direction_bytes = kwaque::byte_count{1'024};
        config.maximum_links = 2;
        config.maximum_address_entries = 8;
        config.maximum_active_flows = 2;
        config.maximum_controls = 4;
        config.stop_batch = 8;
        config.latency_seed = seed;
        config.latency_min = kwaque::runtime::monotonic_duration{latency};
        config.latency_mean_parameter = config.latency_min;
        auto made = kwaque::simulation::fake_network::make(config, events);
        BOOST_REQUIRE_MESSAGE(made.has_value(), history);
        auto network = std::move(*made);
        dense_network_oracle oracle;

        auto binding_a = network->listen(
          kwaque::runtime::network_endpoint{
            wildcard_first ? wildcard : alternate, 0},
          {});
        auto binding_b = network->listen(
          kwaque::runtime::network_endpoint{third, 0}, {});
        co_await pump_until(events, binding_a);
        co_await pump_until(events, binding_b);
        auto bound_a = co_await std::move(binding_a);
        auto bound_b = co_await std::move(binding_b);
        BOOST_REQUIRE_MESSAGE(
          bound_a.has_value() && bound_b.has_value(), history);
        auto listener_a = std::move(*bound_a);
        auto listener_b = std::move(*bound_b);
        BOOST_REQUIRE(
          oracle
            .apply(
              oracle_step{
                .kind = wildcard_first ? oracle_step_kind::bind_wildcard
                                       : oracle_step_kind::bind_exact,
                .source = 1,
                .port = listener_a.local_endpoint().port(),
              })
            .has_value());
        BOOST_REQUIRE(oracle
                        .apply(
                          oracle_step{
                            .kind = oracle_step_kind::bind_exact,
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
        const auto target_a = kwaque::runtime::network_endpoint{
          alternate, listener_a.local_endpoint().port()};
        const std::optional<kwaque::runtime::network_endpoint> explicit_source
          = explicit_local
              ? std::optional{kwaque::runtime::network_endpoint{loopback, 0}}
              : std::nullopt;
        auto connecting_a = network->connect(
          target_a,
          explicit_source,
          kwaque::runtime::network_connection_limits{},
          connect_abort_a);
        auto connecting_b = network->connect(
          listener_b.local_endpoint(),
          explicit_source,
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
        BOOST_REQUIRE_MESSAGE(
          connected_a.has_value() && connected_b.has_value()
            && accepted_a.has_value() && accepted_b.has_value(),
          history);
        auto client_a = std::move(*connected_a);
        auto client_b = std::move(*connected_b);
        auto server_a = std::move(*accepted_a);
        auto server_b = std::move(*accepted_b);
        BOOST_REQUIRE(
          oracle
            .apply(
              oracle_step{
                .kind = explicit_local ? oracle_step_kind::connect_explicit
                                       : oracle_step_kind::connect_implicit,
                .target = 1})
            .has_value());
        BOOST_REQUIRE(
          oracle
            .apply(
              oracle_step{
                .kind = explicit_local ? oracle_step_kind::connect_explicit
                                       : oracle_step_kind::connect_implicit,
                .target = 2})
            .has_value());
        seastar::abort_source write_abort;
        seastar::abort_source read_abort;
        std::array<std::size_t, 3> reconciled{};
        auto reconcile_visible = [&] -> seastar::future<> {
            const auto snapshot = oracle.snapshot();
            for (const std::uint8_t target :
                 {std::uint8_t{1}, std::uint8_t{2}}) {
                const auto expected = snapshot.visible[target].size()
                                      - reconciled[target];
                if (expected == 0U) {
                    continue;
                }
                auto& server = target == 1 ? server_a : server_b;
                auto reading = kwaque::runtime::testing::
                  network_contract_detail::read_exactly(
                    server, expected, read_abort);
                co_await pump_until(events, reading);
                const auto actual = co_await std::move(reading);
                BOOST_REQUIRE_MESSAGE(
                  actual == snapshot.visible[target].substr(reconciled[target]),
                  history);
                reconciled[target] = snapshot.visible[target].size();
            }
        };

        const auto zero_target = static_cast<std::uint8_t>(
          1U + *kwaque::runtime::uniform_u64(*source, 2));
        constexpr std::uint8_t oracle_source{0};
        const auto zero_dimension = *kwaque::runtime::uniform_u64(*source, 3);
        const auto zero_address = zero_target == 1 ? alternate : third;
        history.append(" zero=");
        history.append(std::to_string(zero_dimension));
        history.push_back(':');
        history.append(std::to_string(zero_target));
        oracle_step zero_step{
          .kind = zero_dimension == 0   ? oracle_step_kind::egress_zero
                  : zero_dimension == 1 ? oracle_step_kind::link_zero
                                        : oracle_step_kind::ingress_zero,
          .source = zero_dimension == 2 ? zero_target : oracle_source,
          .target = zero_target,
        };
        BOOST_REQUIRE_MESSAGE(oracle.apply(zero_step).has_value(), history);
        auto zeroing = zero_dimension == 0
                         ? network->set_egress_capacity(
                             loopback,
                             kwaque::simulation::bandwidth_capacity::finite(0))
                       : zero_dimension == 1
                         ? network->set_link_capacity(
                             loopback,
                             zero_address,
                             kwaque::simulation::bandwidth_capacity::finite(0))
                         : network->set_ingress_capacity(
                             zero_address,
                             kwaque::simulation::bandwidth_capacity::finite(0));
        co_await pump_until(events, zeroing);
        co_await require_ready_success(zeroing);
        const oracle_step zero_write{
          .kind = oracle_step_kind::write,
          .target = zero_target,
          .value = 1,
          .pattern = static_cast<std::uint8_t>('0' + zero_dimension),
        };
        BOOST_REQUIRE_MESSAGE(oracle.apply(zero_write).has_value(), history);
        auto& zero_client = zero_target == 1 ? client_a : client_b;
        auto zero_writing = zero_client.write(
          kwaque::runtime::testing::network_contract_detail::make_bytes(
            std::string(1, static_cast<char>(zero_write.pattern))),
          write_abort);
        BOOST_CHECK(!zero_writing.available());
        BOOST_CHECK_EQUAL(events.pending_events(), 0U);
        const oracle_step release_step{
          .kind = zero_dimension == 0   ? oracle_step_kind::egress_unlimited
                  : zero_dimension == 1 ? oracle_step_kind::link_unlimited
                                        : oracle_step_kind::ingress_unlimited,
          .source = zero_dimension == 2 ? zero_target : oracle_source,
          .target = zero_target,
        };
        BOOST_REQUIRE_MESSAGE(oracle.apply(release_step).has_value(), history);
        auto releasing
          = zero_dimension == 0
              ? network->set_egress_capacity(
                  loopback, kwaque::simulation::bandwidth_capacity::unlimited())
            : zero_dimension == 1
              ? network->set_link_capacity(
                  loopback,
                  zero_address,
                  kwaque::simulation::bandwidth_capacity::unlimited())
              : network->set_ingress_capacity(
                  zero_address,
                  kwaque::simulation::bandwidth_capacity::unlimited());
        co_await pump_until(events, releasing);
        co_await require_ready_success(releasing);
        co_await pump_until(events, zero_writing);
        co_await require_ready_success(zero_writing);
        co_await pump_all(events);
        co_await reconcile_visible();

        for (std::uint64_t index = 0; index < 32; ++index) {
            const auto target = static_cast<std::uint8_t>(
              1U + *kwaque::runtime::uniform_u64(*source, 2));
            const auto selected = *kwaque::runtime::uniform_u64(*source, 11);
            const auto capacity
              = 500U * (1U + *kwaque::runtime::uniform_u64(*source, 8));
            oracle_step step{
              .kind = selected == 0   ? oracle_step_kind::write
                      : selected == 1 ? oracle_step_kind::partition
                      : selected == 2 ? oracle_step_kind::heal
                      : selected == 3 ? oracle_step_kind::clog
                      : selected == 4 ? oracle_step_kind::unclog
                      : selected == 5 ? oracle_step_kind::egress_finite
                      : selected == 6 ? oracle_step_kind::egress_unlimited
                      : selected == 7 ? oracle_step_kind::link_finite
                      : selected == 8 ? oracle_step_kind::link_unlimited
                      : selected == 9 ? oracle_step_kind::ingress_finite
                                      : oracle_step_kind::ingress_unlimited,
              .source = selected >= 9U ? target : oracle_source,
              .target = target,
              .value = selected == 0
                         ? 1U + *kwaque::runtime::uniform_u64(*source, 16)
                         : capacity,
              .pattern = static_cast<std::uint8_t>('A' + index % 26U),
            };
            history.push_back(';');
            history.append(std::to_string(index));
            history.push_back(':');
            history.append(
              std::to_string(static_cast<std::uint8_t>(step.kind)));
            history.push_back(':');
            history.append(std::to_string(target));
            history.push_back(':');
            history.append(std::to_string(step.value));
            const auto expected = oracle.apply(step);
            BOOST_REQUIRE_MESSAGE(expected.has_value(), history);

            if (step.kind == oracle_step_kind::write) {
                auto& client = target == 1 ? client_a : client_b;
                auto writing = client.write(
                  kwaque::runtime::testing::network_contract_detail::make_bytes(
                    std::string(
                      static_cast<std::size_t>(step.value),
                      static_cast<char>(step.pattern))),
                  write_abort);
                co_await pump_until(events, writing);
                const auto result = co_await std::move(writing);
                BOOST_REQUIRE_MESSAGE(result.has_value(), history);
            } else {
                const auto target_address = target == 1 ? alternate : third;
                auto applying = [&] {
                    switch (step.kind) {
                    case oracle_step_kind::partition:
                        return network->partition(loopback, target_address);
                    case oracle_step_kind::heal:
                        return network->heal(loopback, target_address);
                    case oracle_step_kind::clog:
                        return network->clog(loopback, target_address);
                    case oracle_step_kind::unclog:
                        return network->unclog(loopback, target_address);
                    case oracle_step_kind::egress_finite:
                        return network->set_egress_capacity(
                          loopback,
                          kwaque::simulation::bandwidth_capacity::finite(
                            step.value));
                    case oracle_step_kind::egress_unlimited:
                        return network->set_egress_capacity(
                          loopback,
                          kwaque::simulation::bandwidth_capacity::unlimited());
                    case oracle_step_kind::link_finite:
                        return network->set_link_capacity(
                          loopback,
                          target_address,
                          kwaque::simulation::bandwidth_capacity::finite(
                            step.value));
                    case oracle_step_kind::link_unlimited:
                        return network->set_link_capacity(
                          loopback,
                          target_address,
                          kwaque::simulation::bandwidth_capacity::unlimited());
                    case oracle_step_kind::ingress_finite:
                        return network->set_ingress_capacity(
                          target_address,
                          kwaque::simulation::bandwidth_capacity::finite(
                            step.value));
                    case oracle_step_kind::ingress_unlimited:
                        return network->set_ingress_capacity(
                          target_address,
                          kwaque::simulation::bandwidth_capacity::unlimited());
                    default:
                        return seastar::make_ready_future<
                          kwaque::runtime::result<void>>(
                          kwaque::runtime::failure(
                            kwaque::runtime::operation_error{
                              kwaque::errc::invalid_argument,
                              kwaque::runtime::operation_kind::network}));
                    }
                }();
                co_await pump_until(events, applying);
                const auto result = co_await std::move(applying);
                BOOST_REQUIRE_MESSAGE(result.has_value(), history);
            }
            co_await pump_all(events);
            co_await reconcile_visible();
        }

        for (const std::uint8_t target : {std::uint8_t{1}, std::uint8_t{2}}) {
            const auto target_address = target == 1 ? alternate : third;
            for (const auto kind :
                 {oracle_step_kind::heal, oracle_step_kind::unclog}) {
                const oracle_step step{.kind = kind, .target = target};
                BOOST_REQUIRE(oracle.apply(step).has_value());
                auto applying = kind == oracle_step_kind::heal
                                  ? network->heal(loopback, target_address)
                                  : network->unclog(loopback, target_address);
                co_await pump_until(events, applying);
                co_await require_ready_success(applying);
                co_await pump_all(events);
            }
            const oracle_step final_write{
              .kind = oracle_step_kind::write, .target = target, .value = 1};
            const auto patterned_write = oracle_step{
              .kind = final_write.kind,
              .target = final_write.target,
              .value = final_write.value,
              .pattern = static_cast<std::uint8_t>('x' + target),
            };
            BOOST_REQUIRE(oracle.apply(patterned_write).has_value());
            auto& client = target == 1 ? client_a : client_b;
            auto writing = client.write(
              kwaque::runtime::testing::network_contract_detail::make_bytes(
                std::string(1, static_cast<char>(patterned_write.pattern))),
              write_abort);
            co_await pump_until(events, writing);
            co_await require_ready_success(writing);
            co_await pump_all(events);
            co_await reconcile_visible();
        }

        const auto snapshot = oracle.snapshot();
        BOOST_CHECK(reconciled[1] == snapshot.visible[1].size());
        BOOST_CHECK(reconciled[2] == snapshot.visible[2].size());
        seastar::abort_source exact_abort_a;
        seastar::abort_source exact_abort_b;
        auto extra_a = server_a.read(kwaque::byte_count{1}, exact_abort_a);
        auto extra_b = server_b.read(kwaque::byte_count{1}, exact_abort_b);
        co_await pump_all(events);
        BOOST_REQUIRE_MESSAGE(!extra_a.available(), history);
        BOOST_REQUIRE_MESSAGE(!extra_b.available(), history);
        BOOST_REQUIRE(
          oracle
            .apply(
              oracle_step{
                .kind = oracle_step_kind::shutdown_output, .source = 0})
            .has_value());
        BOOST_REQUIRE(client_a.shutdown_output().has_value());
        co_await pump_until(events, extra_a);
        auto eof_result = co_await std::move(extra_a);
        BOOST_REQUIRE(eof_result.has_value());
        BOOST_CHECK(eof_result->eof() && eof_result->data().empty());
        BOOST_CHECK(
          client_a.output_state()
          == kwaque::runtime::network_half_state::shut_down);
        BOOST_REQUIRE(
          oracle
            .apply(oracle_step{.kind = oracle_step_kind::reset, .source = 0})
            .has_value());
        server_b.request_abort();
        co_await pump_until(events, extra_b);
        const auto reset_result = co_await std::move(extra_b);
        BOOST_REQUIRE(!reset_result.has_value());
        BOOST_CHECK(reset_result.error().code() == kwaque::errc::aborted);
        BOOST_REQUIRE(
          oracle
            .apply(oracle_step{.kind = oracle_step_kind::close, .source = 0})
            .has_value());
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
        BOOST_CHECK_LT(history.size(), 4'096U);
        co_await stop_network(events, network);
    }
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
