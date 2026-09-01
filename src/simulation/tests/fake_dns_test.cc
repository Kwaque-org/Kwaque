#include "src/runtime/testing/contracts/dns_contract.h"
#include "src/simulation/determinism_version.h"
#include "src/simulation/event_trace.h"
#include "src/simulation/fake_dns.h"
#include "src/simulation/fault_schedule.h"
#include "src/simulation/scheduler.h"

#include <seastar/core/chunked_vector.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace {

kwaque::simulation::scheduler_limits dns_scheduler_limits() {
    auto limits = kwaque::simulation::scheduler_limits::make(
      kwaque::simulation::scheduler_limit_values{
        .pending_events = 1'024,
        .events_per_pump = 1'024,
        .total_events = 10'000,
        .maximum_deadline = kwaque::runtime::monotonic_time{1'000'000},
      });
    BOOST_REQUIRE(limits.has_value());
    return *limits;
}

kwaque::simulation::trace_limits dns_trace_limits() {
    auto limits = kwaque::simulation::trace_limits::make(
      kwaque::simulation::trace_limit_values{
        .entries = 1'024,
        .encoded_bytes = kwaque::simulation::canonical_header_encoded_size
                         + 1'024U
                             * kwaque::simulation::canonical_entry_encoded_size,
        .line_bytes = 1'024,
      });
    BOOST_REQUIRE(limits.has_value());
    return *limits;
}

kwaque::simulation::trace_header dns_trace_header(
  kwaque::simulation::scheduler_limits scheduler_budget,
  kwaque::simulation::trace_limits trace_budget) {
    return kwaque::simulation::trace_header::current(
      29,
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

kwaque::runtime::dns_query make_query(
  std::string host,
  std::uint16_t port = 9'988,
  kwaque::runtime::dns_address_family family
  = kwaque::runtime::dns_address_family::any) {
    auto name = kwaque::runtime::dns_name::make(std::move(host));
    BOOST_REQUIRE(name.has_value());
    return kwaque::runtime::dns_query{
      .host = std::move(*name), .port = port, .family = family};
}

kwaque::runtime::network_address address(std::uint8_t last) {
    return kwaque::runtime::network_address::ipv4(
      {std::byte{127}, std::byte{0}, std::byte{0}, std::byte{last}});
}

kwaque::runtime::dns_answer
answer(std::uint8_t last, std::uint16_t port, std::uint64_t ttl) {
    return kwaque::runtime::dns_answer{
      .endpoint = kwaque::runtime::network_endpoint{address(last), port},
      .ttl = kwaque::runtime::monotonic_duration{ttl},
    };
}

kwaque::runtime::dns_answer ipv6_answer(std::uint16_t port, std::uint64_t ttl) {
    auto selected = kwaque::runtime::network_address::try_parse_numeric("::1");
    BOOST_REQUIRE(selected.has_value());
    return kwaque::runtime::dns_answer{
      .endpoint = kwaque::runtime::network_endpoint{*selected, port},
      .ttl = kwaque::runtime::monotonic_duration{ttl},
    };
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

kwaque::simulation::fault_rule dns_rule(
  std::uint64_t id,
  std::uint64_t query_id,
  kwaque::runtime::fault_decision decision) {
    const auto rule_id = kwaque::simulation::fault_rule_id::make(id);
    BOOST_REQUIRE(rule_id.has_value());
    const auto occurrence = kwaque::runtime::fault_occurrence::make(1);
    BOOST_REQUIRE(occurrence.has_value());
    auto rule = kwaque::simulation::fault_rule::make(
      *rule_id,
      kwaque::runtime::builtin_fault_point::dns,
      kwaque::runtime::fault_object_key::from_u64(query_id),
      *occurrence,
      *occurrence,
      kwaque::simulation::fault_selector::once(),
      decision);
    BOOST_REQUIRE(rule.has_value());
    return *rule;
}

} // namespace

SEASTAR_TEST_CASE(fake_dns_records_are_canonical_ordered_and_transactional) {
    kwaque::simulation::scheduler events{dns_scheduler_limits()};
    auto made = kwaque::simulation::fake_dns::make({}, events);
    BOOST_REQUIRE(made.has_value());
    auto resolver = std::move(*made);

    auto key = make_query("Example.COM.");
    auto record = kwaque::simulation::fake_dns_record{
      .key = key,
      .answers = {
        answer(42, key.port, 7),
        ipv6_answer(key.port, 9),
        answer(43, key.port, 11),
      },
      .latency = kwaque::runtime::monotonic_duration{9},
    };
    const auto added_record = resolver->add_record(std::move(record));
    BOOST_REQUIRE(added_record.has_value());
    BOOST_CHECK_EQUAL(resolver->record_count(), 1U);
    BOOST_CHECK_EQUAL(resolver->answer_count(), 3U);
    BOOST_CHECK_EQUAL(
      resolver->retained_name_bytes().value(), key.host.value().size());

    auto duplicate = kwaque::simulation::fake_dns_record{
      .key = key, .answers = {answer(44, key.port, 1)}};
    const auto duplicated = resolver->add_record(std::move(duplicate));
    BOOST_REQUIRE(!duplicated.has_value());
    BOOST_CHECK(duplicated.error().code() == kwaque::errc::already_exists);

    seastar::abort_source abort_source;
    auto resolving = resolver->resolve(
      make_query("example.com", key.port), abort_source);
    BOOST_CHECK(!resolving.available());
    co_await pump_until(events, resolving);
    auto resolved = co_await std::move(resolving);
    BOOST_REQUIRE(resolved.has_value());
    BOOST_REQUIRE_EQUAL(resolved->answers().size(), 3U);
    BOOST_CHECK(resolved->answers()[0].endpoint.address() == address(42));
    BOOST_CHECK(
      resolved->answers()[1].endpoint.address().family()
      == kwaque::runtime::network_address_family::ipv6);
    BOOST_CHECK(resolved->answers()[2].endpoint.address() == address(43));
    BOOST_CHECK(
      resolved->answers()[0].ttl == kwaque::runtime::monotonic_duration{7});
    BOOST_CHECK(
      resolved->answers()[1].ttl == kwaque::runtime::monotonic_duration{9});
    BOOST_CHECK(
      resolved->answers()[2].ttl == kwaque::runtime::monotonic_duration{11});

    auto empty = kwaque::simulation::fake_dns_record{.key = key};
    const auto emptied_record = resolver->update_record(std::move(empty));
    BOOST_REQUIRE(emptied_record.has_value());
    auto empty_lookup = resolver->resolve(key, abort_source);
    co_await pump_until(events, empty_lookup);
    const auto empty_result = co_await std::move(empty_lookup);
    BOOST_REQUIRE(!empty_result.has_value());
    BOOST_CHECK(empty_result.error().code() == kwaque::errc::dns_failure);

    auto configured_error = kwaque::simulation::fake_dns_record{
      .key = key,
      .latency = kwaque::runtime::monotonic_duration{5},
      .error = kwaque::errc::timed_out,
    };
    const auto configured = resolver->update_record(
      std::move(configured_error));
    BOOST_REQUIRE(configured.has_value());
    auto failing = resolver->resolve(key, abort_source);
    co_await pump_until(events, failing);
    const auto failed = co_await std::move(failing);
    BOOST_REQUIRE(!failed.has_value());
    BOOST_CHECK(failed.error().code() == kwaque::errc::timed_out);

    BOOST_REQUIRE(resolver->remove_record(key).has_value());
    auto missing = resolver->resolve(key, abort_source);
    BOOST_CHECK(!missing.available());
    co_await pump_until(events, missing);
    const auto absent = co_await std::move(missing);
    BOOST_REQUIRE(!absent.has_value());
    BOOST_CHECK(absent.error().code() == kwaque::errc::dns_failure);

    auto numeric = resolver->resolve(
      make_query("127.0.0.51", 12'000), abort_source);
    BOOST_CHECK(!numeric.available());
    co_await pump_until(events, numeric);
    auto numeric_result = co_await std::move(numeric);
    BOOST_REQUIRE(numeric_result.has_value());
    BOOST_REQUIRE_EQUAL(numeric_result->answers().size(), 1U);
    BOOST_CHECK_EQUAL(numeric_result->answers()[0].endpoint.port(), 12'000U);
    BOOST_CHECK(
      numeric_result->answers()[0].ttl == kwaque::runtime::maximum_dns_ttl);
    BOOST_CHECK_EQUAL(resolver->record_count(), 0U);

    auto stopping = resolver->stop();
    co_await pump_until(events, stopping);
    co_await require_ready_success(stopping);
}

SEASTAR_TEST_CASE(fake_dns_validates_record_coherence_and_all_bounds) {
    kwaque::simulation::scheduler events{dns_scheduler_limits()};
    auto config = kwaque::simulation::fake_dns_config{};
    config.maximum_records = 2;
    config.maximum_answers = 2;
    config.maximum_name_bytes = kwaque::byte_count{18};
    auto made = kwaque::simulation::fake_dns::make(config, events);
    BOOST_REQUIRE(made.has_value());
    auto resolver = std::move(*made);
    const auto key = make_query(
      "bounded.test", 7'000, kwaque::runtime::dns_address_family::ipv4);

    auto wrong_port = kwaque::simulation::fake_dns_record{
      .key = key, .answers = {answer(1, 7'001, 1)}};
    const auto rejected_port = resolver->add_record(std::move(wrong_port));
    BOOST_CHECK(!rejected_port.has_value());

    auto ipv6 = kwaque::runtime::network_address::try_parse_numeric("::1");
    BOOST_REQUIRE(ipv6.has_value());
    auto wrong_family = kwaque::simulation::fake_dns_record{
      .key = key,
      .answers = {{
        .endpoint = kwaque::runtime::network_endpoint{*ipv6, key.port},
        .ttl = kwaque::runtime::monotonic_duration{1},
      }},
    };
    const auto rejected_family = resolver->add_record(std::move(wrong_family));
    BOOST_CHECK(!rejected_family.has_value());

    const auto excessive_ttl = kwaque::runtime::maximum_dns_ttl.checked_add(
      kwaque::runtime::monotonic_duration{1});
    BOOST_REQUIRE(excessive_ttl.has_value());
    auto bad_ttl = kwaque::simulation::fake_dns_record{
      .key = key,
      .answers = {{
        .endpoint = kwaque::runtime::network_endpoint{address(1), key.port},
        .ttl = *excessive_ttl,
      }},
    };
    const auto rejected_ttl = resolver->add_record(std::move(bad_ttl));
    BOOST_REQUIRE(!rejected_ttl.has_value());
    BOOST_CHECK(rejected_ttl.error().code() == kwaque::errc::out_of_range);

    auto valid = kwaque::simulation::fake_dns_record{
      .key = key,
      .answers = {answer(1, key.port, 1), answer(2, key.port, 2)},
    };
    const auto added_valid = resolver->add_record(std::move(valid));
    BOOST_REQUIRE(added_valid.has_value());
    auto answer_saturated = kwaque::simulation::fake_dns_record{
      .key = make_query("x.test", key.port),
      .answers = {answer(3, key.port, 3)},
    };
    const auto saturated_answers = resolver->add_record(
      std::move(answer_saturated));
    BOOST_REQUIRE(!saturated_answers.has_value());
    BOOST_CHECK(
      saturated_answers.error().code() == kwaque::errc::resource_exhausted);

    auto name_saturated = kwaque::simulation::fake_dns_record{
      .key = make_query("longname.test", key.port)};
    const auto saturated_name = resolver->add_record(std::move(name_saturated));
    BOOST_REQUIRE(!saturated_name.has_value());
    BOOST_CHECK(
      saturated_name.error().code() == kwaque::errc::resource_exhausted);

    auto second = kwaque::simulation::fake_dns_record{
      .key = make_query("x.test", key.port)};
    const auto added_second = resolver->add_record(std::move(second));
    BOOST_REQUIRE(added_second.has_value());
    auto third = kwaque::simulation::fake_dns_record{
      .key = make_query("z.test", key.port)};
    const auto saturated_records = resolver->add_record(std::move(third));
    BOOST_REQUIRE(!saturated_records.has_value());
    BOOST_CHECK(
      saturated_records.error().code() == kwaque::errc::resource_exhausted);
    BOOST_CHECK_EQUAL(resolver->record_count(), 2U);
    BOOST_CHECK_EQUAL(resolver->answer_count(), 2U);

    auto stopping = resolver->stop();
    BOOST_CHECK(!stopping.available());
    co_await pump_until(events, stopping);
    co_await require_ready_success(stopping);
    BOOST_CHECK_EQUAL(resolver->record_count(), 0U);
    BOOST_CHECK_EQUAL(resolver->answer_count(), 0U);
    BOOST_CHECK_EQUAL(resolver->retained_name_bytes().value(), 0U);
}

SEASTAR_TEST_CASE(fake_dns_serializes_named_queries_and_aborts_only_waiters) {
    kwaque::simulation::scheduler events{dns_scheduler_limits()};
    auto config = kwaque::simulation::fake_dns_config{};
    config.query_limits.maximum_waiters = 1;
    config.stop_batch = 1;
    auto made = kwaque::simulation::fake_dns::make(config, events);
    BOOST_REQUIRE(made.has_value());
    auto resolver = std::move(*made);
    const auto active_key = make_query("active.test");
    const auto waiting_key = make_query("waiting.test");
    const auto saturated_key = make_query("saturated.test");
    BOOST_REQUIRE(resolver
                    ->add_record(
                      kwaque::simulation::fake_dns_record{
                        .key = active_key,
                        .answers = {answer(1, active_key.port, 1)},
                        .latency = kwaque::runtime::monotonic_duration{20},
                      })
                    .has_value());
    BOOST_REQUIRE(resolver
                    ->add_record(
                      kwaque::simulation::fake_dns_record{
                        .key = waiting_key,
                        .answers = {answer(2, waiting_key.port, 2)},
                        .latency = kwaque::runtime::monotonic_duration{20},
                      })
                    .has_value());
    BOOST_REQUIRE(resolver
                    ->add_record(
                      kwaque::simulation::fake_dns_record{
                        .key = saturated_key,
                        .answers = {answer(3, saturated_key.port, 3)},
                      })
                    .has_value());

    seastar::abort_source active_abort;
    auto active = resolver->resolve(active_key, active_abort);
    BOOST_CHECK(resolver->active());
    active_abort.request_abort();

    seastar::abort_source waiting_abort;
    auto waiting = resolver->resolve(waiting_key, waiting_abort);
    BOOST_CHECK_EQUAL(resolver->waiting_queries(), 1U);

    seastar::abort_source saturated_abort;
    auto saturated = resolver->resolve(saturated_key, saturated_abort);
    BOOST_REQUIRE(saturated.available());
    const auto queue_full = saturated.get();
    BOOST_REQUIRE(!queue_full.has_value());
    BOOST_CHECK(queue_full.error().code() == kwaque::errc::queue_full);

    waiting_abort.request_abort();
    BOOST_CHECK(!waiting.available());
    co_await pump_until(events, waiting);
    const auto canceled = co_await std::move(waiting);
    BOOST_REQUIRE(!canceled.has_value());
    BOOST_CHECK(canceled.error().code() == kwaque::errc::aborted);
    BOOST_CHECK_EQUAL(resolver->waiting_queries(), 0U);
    BOOST_CHECK(!active.available());

    auto replacement = resolver->resolve(saturated_key, saturated_abort);
    BOOST_CHECK_EQUAL(resolver->waiting_queries(), 1U);
    auto stopping = resolver->stop();
    co_await pump_until(events, replacement);
    const auto stopped_waiter = co_await std::move(replacement);
    BOOST_REQUIRE(!stopped_waiter.has_value());
    BOOST_CHECK(stopped_waiter.error().code() == kwaque::errc::aborted);
    co_await pump_until(events, active);
    co_await require_ready_success(active);
    co_await pump_until(events, stopping);
    co_await require_ready_success(stopping);
}

SEASTAR_TEST_CASE(fake_dns_stops_the_maximum_waiter_set) {
    kwaque::simulation::scheduler events{dns_scheduler_limits()};
    auto config = kwaque::simulation::fake_dns_config{};
    config.query_limits.maximum_waiters = kwaque::runtime::maximum_dns_waiters;
    config.stop_batch = 1;
    auto made = kwaque::simulation::fake_dns::make(config, events);
    BOOST_REQUIRE(made.has_value());
    auto resolver = std::move(*made);
    const auto key = make_query("maximum-waiters.test");
    BOOST_REQUIRE(resolver
                    ->add_record(
                      kwaque::simulation::fake_dns_record{
                        .key = key,
                        .answers = {answer(9, key.port, 1)},
                        .latency = kwaque::runtime::monotonic_duration{100},
                      })
                    .has_value());

    seastar::abort_source abort_source;
    std::vector<
      seastar::future<kwaque::runtime::result<kwaque::runtime::dns_result>>>
      queries;
    queries.reserve(kwaque::runtime::maximum_dns_waiters + 1U);
    for (std::size_t index = 0; index <= kwaque::runtime::maximum_dns_waiters;
         ++index) {
        queries.push_back(resolver->resolve(key, abort_source));
    }
    BOOST_CHECK_EQUAL(
      resolver->waiting_queries(), kwaque::runtime::maximum_dns_waiters);

    auto stopping = resolver->stop();
    BOOST_CHECK(!stopping.available());
    co_await pump_until(events, stopping);
    co_await require_ready_success(stopping);

    for (std::size_t index = 0; index < queries.size(); ++index) {
        auto result = co_await std::move(queries[index]);
        if (index == 0) {
            BOOST_REQUIRE(result.has_value());
        } else {
            BOOST_REQUIRE(!result.has_value());
            BOOST_CHECK(result.error().code() == kwaque::errc::aborted);
        }
    }
    BOOST_CHECK_EQUAL(resolver->pending_queries(), 0U);
}

SEASTAR_TEST_CASE(fake_dns_faults_apply_error_delay_and_parked_completion) {
    const auto scheduler_budget = dns_scheduler_limits();
    const auto trace_budget = dns_trace_limits();
    kwaque::simulation::event_trace trace{
      dns_trace_header(scheduler_budget, trace_budget), trace_budget};
    kwaque::simulation::scheduler events{scheduler_budget, &trace};
    seastar::chunked_vector<kwaque::simulation::fault_rule> rules;
    rules.push_back(
      dns_rule(1, 1, kwaque::runtime::fault_decision::make_error()));
    rules.push_back(dns_rule(
      2,
      2,
      kwaque::runtime::fault_decision::make_delay(
        kwaque::runtime::monotonic_duration{7})));
    rules.push_back(
      dns_rule(3, 3, kwaque::runtime::fault_decision::make_drop_completion()));
    auto made_faults = kwaque::simulation::fault_schedule::make(
      events, trace, 29, std::move(rules));
    BOOST_REQUIRE(made_faults.has_value());
    auto faults = std::move(*made_faults);
    auto made = kwaque::simulation::fake_dns::make({}, events, faults.get());
    BOOST_REQUIRE(made.has_value());
    auto resolver = std::move(*made);
    const auto key = make_query("fault.test");
    BOOST_REQUIRE(resolver
                    ->add_record(
                      kwaque::simulation::fake_dns_record{
                        .key = key,
                        .answers = {answer(7, key.port, 13)},
                        .latency = kwaque::runtime::monotonic_duration{3},
                      })
                    .has_value());
    seastar::abort_source abort_source;

    auto injected = resolver->resolve(key, abort_source);
    co_await pump_until(events, injected);
    const auto injected_result = co_await std::move(injected);
    BOOST_REQUIRE(!injected_result.has_value());
    BOOST_CHECK(injected_result.error().code() == kwaque::errc::fault_injected);

    const auto before_delay = events.now();
    auto delayed = resolver->resolve(key, abort_source);
    co_await pump_until(events, delayed);
    co_await require_ready_success(delayed);
    const auto elapsed = events.now().checked_elapsed_since(before_delay);
    BOOST_REQUIRE(elapsed.has_value());
    BOOST_CHECK_EQUAL(elapsed->nanoseconds(), 10U);

    auto parked = resolver->resolve(key, abort_source);
    BOOST_CHECK(!parked.available());
    const auto advanced = events.advance_to_next();
    BOOST_REQUIRE(advanced.has_value());
    BOOST_REQUIRE(advanced->has_value());
    BOOST_REQUIRE(events.run_ready().has_value());
    co_await seastar::yield();
    BOOST_CHECK(!parked.available());
    BOOST_CHECK_EQUAL(resolver->pending_queries(), 1U);
    BOOST_CHECK(!resolver->active());

    auto stopping = resolver->stop();
    co_await pump_until(events, parked);
    const auto parked_result = co_await std::move(parked);
    BOOST_REQUIRE(!parked_result.has_value());
    BOOST_CHECK(parked_result.error().code() == kwaque::errc::aborted);
    co_await pump_until(events, stopping);
    co_await require_ready_success(stopping);

    std::array<bool, 23> actions{};
    for (const auto& entry : trace.entries()) {
        actions[static_cast<std::size_t>(entry.action)] = true;
    }
    BOOST_CHECK(
      actions[static_cast<std::size_t>(
        kwaque::simulation::trace_action::fault_evaluated)]);
    BOOST_CHECK(
      actions[static_cast<std::size_t>(
        kwaque::simulation::trace_action::dns_result_applied)]);
    BOOST_CHECK(
      actions[static_cast<std::size_t>(
        kwaque::simulation::trace_action::operation_parked)]);
    BOOST_CHECK(
      actions[static_cast<std::size_t>(
        kwaque::simulation::trace_action::stop_terminal)]);
}

SEASTAR_TEST_CASE(fake_dns_shared_runtime_contract) {
    kwaque::simulation::scheduler events{dns_scheduler_limits()};
    auto made = kwaque::simulation::fake_dns::make({}, events);
    BOOST_REQUIRE(made.has_value());
    auto resolver = std::move(*made);
    auto contract = kwaque::runtime::testing::run_dns_contract(*resolver);
    co_await pump_until(events, contract);
    co_await std::move(contract);
}
