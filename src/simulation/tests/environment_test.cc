#include "src/base/units.h"
#include "src/observability/event.h"
#include "src/observability/event_log.h"
#include "src/resource/workload_class.h"
#include "src/runtime/dns.h"
#include "src/runtime/file.h"
#include "src/runtime/network.h"
#include "src/runtime/testing/contracts/environment_contract.h"
#include "src/runtime/testing/contracts/environment_lifecycle_contract.h"
#include "src/simulation/environment.h"
#include "src/simulation/environment_test_support.h"
#include "src/simulation/scheduler_driver.h"
#include "src/simulation/sha256.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_api.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/sstring.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <boost/test/unit_test.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <system_error>
#include <utility>

namespace {

using kwaque::simulation::environment;
using kwaque::simulation::environment_config;
using kwaque::simulation::environment_config_values;
using kwaque::simulation::environment_test_access;
using kwaque::simulation::fault_rule;
using kwaque::simulation::fault_rule_id;
using kwaque::simulation::fault_selector;
using kwaque::simulation::scheduler;

fault_rule rule(
  std::uint64_t id,
  kwaque::runtime::builtin_fault_point point,
  std::uint64_t occurrence,
  kwaque::runtime::fault_decision decision) {
    const auto rule_id = fault_rule_id::make(id);
    const auto selected_occurrence = kwaque::runtime::fault_occurrence::make(
      occurrence);
    BOOST_REQUIRE(rule_id.has_value());
    BOOST_REQUIRE(selected_occurrence.has_value());
    auto result = fault_rule::make(
      *rule_id,
      point,
      std::nullopt,
      *selected_occurrence,
      *selected_occurrence,
      fault_selector::once(),
      decision);
    BOOST_REQUIRE(result.has_value());
    return *result;
}

environment_config
config(std::optional<fault_rule> selected_rule = std::nullopt) {
    environment_config_values values;
    values.master_seed = 71;
    values.runtime_stream_stable_id = 19;
    values.event_epoch = 23;
    values.configuration_digest[0] = 0x41;
    values.input_digest[0] = 0x73;
    values.scheduler.pending_events = 256;
    values.scheduler.events_per_pump = 64;
    values.scheduler.total_events = 2'000;
    values.trace.entries = 2'048;
    values.trace.encoded_bytes = 512U * 1'024U;
    values.event_log.entries = 32;
    values.event_log.encoded_bytes = 32U * 1'024U;
    values.maximum_fault_rules = 16;
    values.file.maximum_objects = 64;
    values.file.maximum_open_handles = 8;
    values.file.maximum_pending_operations = 8;
    values.file.maximum_pending_reads = 8;
    values.file.maximum_pending_writes = 8;
    values.network.maximum_listeners = 2;
    values.network.maximum_connection_pairs = 2;
    values.network.maximum_pending_connects = 2;
    values.network.maximum_backlog_entries = 4;
    values.network.maximum_operations = 8;
    values.network.maximum_parked_operations = 4;
    values.network.maximum_packets = 16;
    values.network.maximum_direction_packets = 8;
    values.network.maximum_links = 8;
    values.network.maximum_address_entries = 8;
    values.network.maximum_active_flows = 4;
    values.network.maximum_controls = 8;
    values.network.stop_batch = 8;
    values.dns.maximum_records = 16;
    values.dns.maximum_answers = 32;
    values.dns.maximum_name_bytes = kwaque::byte_count{8U * 1'024U};
    values.dns.stop_batch = 8;
    values.dns.query_limits.maximum_waiters = 8;
    if (selected_rule) {
        values.fault_rules.push_back(std::move(*selected_rule));
    }
    auto made = environment_config::make(std::move(values));
    BOOST_REQUIRE(made.has_value());
    return std::move(*made);
}

using kwaque::simulation::testing::pump_until;
using kwaque::simulation::testing::scheduler_driver;
using kwaque::simulation::testing::scheduler_liveness_error;

constexpr std::uint64_t scheduler_driver_test_event_count
  = kwaque::simulation::testing::scheduler_driver_batch_size + 1U;

bool contains_trace_action(
  const environment& target, kwaque::simulation::trace_action action) {
    for (const auto& entry : target.trace().entries()) {
        if (entry.action == action) {
            return true;
        }
    }
    return false;
}

kwaque::runtime::dns_query component_dns_query() {
    auto name = kwaque::runtime::dns_name::make("environment.test");
    BOOST_REQUIRE(name.has_value());
    return kwaque::runtime::dns_query{
      .host = std::move(*name),
      .port = 33'145,
      .family = kwaque::runtime::dns_address_family::ipv4,
    };
}

constexpr auto component_loopback = kwaque::runtime::network_address::ipv4(
  {std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}});
constexpr auto component_dns_address = kwaque::runtime::network_address::ipv4(
  {std::byte{127}, std::byte{0}, std::byte{0}, std::byte{42}});

kwaque::runtime::dns_answer component_dns_answer() {
    return kwaque::runtime::dns_answer{
      .endpoint
      = kwaque::runtime::network_endpoint{component_dns_address, 33'145},
      .ttl = kwaque::runtime::monotonic_duration{7'000'000'000},
    };
}

kwaque::runtime::file_path component_root_path() {
    auto path = kwaque::runtime::file_path::make(
      "/kwaque/environment-contract");
    BOOST_REQUIRE(path.has_value());
    return std::move(*path);
}

seastar::future<> expect_start_failure(environment& target) {
    auto starting = target.start();
    co_await pump_until(target.event_scheduler(), starting);
    bool failed = false;
    try {
        co_await std::move(starting);
    } catch (const std::system_error&) {
        failed = true;
    }
    BOOST_REQUIRE(failed);
    BOOST_CHECK(target.state() == kwaque::runtime::environment_state::stopped);
    BOOST_CHECK_EQUAL(target.event_scheduler().pending_events(), 0U);
}

bool metric_registered(const char* name) {
    return seastar::metrics::impl::get_value_map().contains(
      seastar::sstring{name});
}

} // namespace

SEASTAR_TEST_CASE(simulation_environment_validates_aggregate_configuration) {
    environment_config_values invalid_stream;
    invalid_stream.runtime_stream_stable_id = 0;
    auto rejected_stream = environment_config::make(std::move(invalid_stream));
    BOOST_REQUIRE(!rejected_stream.has_value());
    BOOST_CHECK(rejected_stream.error().code() == kwaque::errc::out_of_range);

    environment_config_values invalid_epoch;
    invalid_epoch.event_epoch = 0;
    auto rejected_epoch = environment_config::make(std::move(invalid_epoch));
    BOOST_REQUIRE(!rejected_epoch.has_value());
    BOOST_CHECK(
      rejected_epoch.error().code() == kwaque::errc::invalid_argument);

    environment_config_values invalid_memory;
    invalid_memory.resource_total_memory = kwaque::byte_count{1};
    auto rejected_memory = environment_config::make(std::move(invalid_memory));
    BOOST_REQUIRE(!rejected_memory.has_value());
    BOOST_CHECK(
      rejected_memory.error().code() == kwaque::errc::resource_exhausted);

    environment_config_values invalid_terminal_events;
    invalid_terminal_events.event_log.entries = 3;
    auto rejected_terminal_events = environment_config::make(
      std::move(invalid_terminal_events));
    BOOST_REQUIRE(!rejected_terminal_events.has_value());
    BOOST_CHECK(
      rejected_terminal_events.error().code()
      == kwaque::errc::resource_exhausted);

    environment_config_values invalid_terminal_bytes;
    invalid_terminal_bytes.event_log.encoded_bytes
      = kwaque::observability::canonical_event_log_header_encoded_size;
    auto rejected_terminal_bytes = environment_config::make(
      std::move(invalid_terminal_bytes));
    BOOST_REQUIRE(!rejected_terminal_bytes.has_value());
    BOOST_CHECK(
      rejected_terminal_bytes.error().code()
      == kwaque::errc::resource_exhausted);

    environment_config_values invalid_aggregate;
    invalid_aggregate.scheduler.pending_events = 128;
    auto rejected_aggregate = environment_config::make(
      std::move(invalid_aggregate));
    BOOST_REQUIRE(!rejected_aggregate.has_value());
    BOOST_CHECK(
      rejected_aggregate.error().code() == kwaque::errc::out_of_range);
    co_return;
}

SEASTAR_TEST_CASE(
  simulation_environment_construction_is_allocation_transactional) {
    // Digest providers initialize process-global state lazily. Complete that
    // one-time work before injecting failures into owner-local construction.
    kwaque::simulation::sha256_hasher digest_warmup;
    static_cast<void>(std::move(digest_warmup).final());

    std::unique_ptr<environment> target;
    std::size_t attempts = 0;
    auto& injector = seastar::memory::local_failure_injector();
    while (target == nullptr) {
        injector.fail_after(attempts++);
        std::unique_ptr<environment> candidate;
        try {
            auto made = environment::make(config());
            if (!made) {
                throw std::system_error(make_error_code(made.error().code()));
            }
            candidate = std::move(*made);
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
        auto stopping = candidate->stop();
        co_await pump_until(candidate->event_scheduler(), stopping);
        co_await std::move(stopping);
    }
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    BOOST_CHECK_GT(attempts, 1U);
#else
    BOOST_CHECK_EQUAL(attempts, 1U);
#endif
    BOOST_REQUIRE(target != nullptr);
    auto stopping = target->stop();
    co_await pump_until(target->event_scheduler(), stopping);
    co_await std::move(stopping);
}

SEASTAR_TEST_CASE(simulation_environment_satisfies_shared_lifecycle_contract) {
    auto made = environment::make(config());
    BOOST_REQUIRE(made.has_value());
    auto target = std::move(*made);
    co_await kwaque::runtime::testing::run_environment_lifecycle_contract(
      *target, scheduler_driver{target->event_scheduler()});

    BOOST_CHECK_EQUAL(target->event_sink().events().entries().size(), 4U);
    BOOST_CHECK_EQUAL(target->event_scheduler().pending_events(), 0U);
    BOOST_CHECK_EQUAL(target->time().pending_adjustments(), 0U);
    BOOST_CHECK(
      target->resource_manager().state()
      == kwaque::resource::resource_manager_state::stopped);
}

SEASTAR_TEST_CASE(simulation_environment_hides_runtime_while_starting) {
    auto made = environment::make(config(rule(
      5,
      kwaque::runtime::builtin_fault_point::environment_start,
      1,
      kwaque::runtime::fault_decision::make_delay(
        kwaque::runtime::monotonic_duration{37}))));
    BOOST_REQUIRE(made.has_value());
    auto target = std::move(*made);

    auto starting = target->start();
    BOOST_CHECK(!starting.available());
    BOOST_CHECK_THROW(static_cast<void>(target->random()), std::logic_error);
    BOOST_CHECK_THROW(
      kwaque::runtime::basic_runtime{*target}, std::logic_error);
    co_await pump_until(target->event_scheduler(), starting);
    co_await std::move(starting);

    auto stopping = target->stop();
    co_await pump_until(target->event_scheduler(), stopping);
    co_await std::move(stopping);
}

SEASTAR_TEST_CASE(simulation_environment_retained_view_drains_during_stop) {
    auto made = environment::make(config());
    BOOST_REQUIRE(made.has_value());
    auto target = std::move(*made);
    co_await target->start();

    auto runtime
      = std::make_unique<kwaque::runtime::basic_runtime<environment>>(*target);
    auto acquired
      = runtime->view<kwaque::runtime::runtime_capability::random>();
    BOOST_REQUIRE(acquired.has_value());
    std::optional capability{std::move(*acquired)};
    auto stopping = target->stop();
    BOOST_CHECK(!stopping.available());
    static_cast<void>(capability->random().next_u64());

    capability.reset();
    runtime.reset();
    co_await pump_until(target->event_scheduler(), stopping);
    co_await std::move(stopping);
}

SEASTAR_TEST_CASE(simulation_scheduler_driver_rejects_stalled_future) {
    auto made = environment::make(config());
    BOOST_REQUIRE(made.has_value());
    auto target = std::move(*made);
    seastar::promise<> stalled_promise;
    auto stalled = stalled_promise.get_future();

    bool rejected = false;
    try {
        co_await scheduler_driver{target->event_scheduler()}.operation(
          std::move(stalled));
    } catch (const scheduler_liveness_error&) {
        rejected = true;
    }
    BOOST_REQUIRE(rejected);

    auto stopping = target->stop();
    co_await pump_until(target->event_scheduler(), stopping);
    co_await std::move(stopping);
}

SEASTAR_TEST_CASE(simulation_scheduler_driver_yields_between_full_batches) {
    auto made = environment::make(config());
    BOOST_REQUIRE(made.has_value());
    auto target = std::move(*made);
    std::uint64_t completed = 0;
    seastar::promise<> completion;
    auto waiting = completion.get_future();

    for (std::uint64_t index = 0; index < scheduler_driver_test_event_count;
         ++index) {
        auto scheduled = target->event_scheduler().schedule(
          target->event_scheduler().now(),
          kwaque::simulation::event_priority::normal(),
          [&completed, &completion] noexcept {
              ++completed;
              if (completed == scheduler_driver_test_event_count) {
                  completion.set_value();
              }
          });
        BOOST_REQUIRE(scheduled.has_value());
    }
    co_await scheduler_driver{target->event_scheduler()}.operation(
      std::move(waiting));
    BOOST_CHECK_EQUAL(completed, scheduler_driver_test_event_count);
    BOOST_CHECK_EQUAL(target->event_scheduler().pending_events(), 0U);

    auto stopping = target->stop();
    co_await pump_until(target->event_scheduler(), stopping);
    co_await std::move(stopping);
}

SEASTAR_TEST_CASE(simulation_environment_satisfies_component_contract) {
    auto made = environment::make(config());
    BOOST_REQUIRE(made.has_value());
    auto target = std::move(*made);
    const auto expected_answer = component_dns_answer();
    auto record = environment_test_access::dns_owner(*target).add_record(
      kwaque::simulation::fake_dns_record{
        .key = component_dns_query(),
        .answers = {expected_answer},
        .latency = kwaque::runtime::monotonic_duration{3},
      });
    if (!record) {
        auto stopping = target->stop();
        co_await pump_until(target->event_scheduler(), stopping);
        co_await std::move(stopping);
        throw std::system_error(make_error_code(record.error().code()));
    }

    co_await kwaque::runtime::testing::run_environment_contract(
      *target,
      kwaque::runtime::testing::environment_component_input{
        .root_path = component_root_path(),
        .listen_endpoint
        = kwaque::runtime::network_endpoint{component_loopback, 0},
        .dns = component_dns_query(),
        .memory = kwaque::byte_count{4'096},
      },
      kwaque::runtime::testing::environment_contract_expectation{
        .dns_answers = {expected_answer},
        .random_word = UINT64_C(0xea975e34f614487d),
      },
      scheduler_driver{target->event_scheduler()});

    const auto& events = target->event_sink().events().entries();
    BOOST_REQUIRE_EQUAL(events.size(), 5U);
    BOOST_CHECK(
      events[0].kind()
      == kwaque::observability::event_kind::runtime_state_changed);
    BOOST_CHECK(
      events[1].kind()
      == kwaque::observability::event_kind::runtime_state_changed);
    BOOST_CHECK(
      events[2].kind() == kwaque::observability::event_kind::queue_admission);
    BOOST_CHECK(
      events[3].kind()
      == kwaque::observability::event_kind::runtime_state_changed);
    BOOST_CHECK(
      events[4].kind()
      == kwaque::observability::event_kind::runtime_state_changed);
    BOOST_CHECK(!target->trace().failed());
    BOOST_CHECK(!target->trace().entries().empty());
    BOOST_CHECK_GT(target->event_scheduler().executed_events(), 0U);
    BOOST_CHECK_EQUAL(target->event_scheduler().pending_events(), 0U);
    BOOST_CHECK_EQUAL(target->time().pending_adjustments(), 0U);
}

SEASTAR_TEST_CASE(
  simulation_environment_fault_capability_uses_logical_occurrences) {
    auto made = environment::make(config());
    BOOST_REQUIRE(made.has_value());
    auto target = std::move(*made);
    co_await target->start();

    const auto* descriptor = kwaque::runtime::descriptor_for(
      kwaque::runtime::builtin_fault_point::queue_admission);
    BOOST_REQUIRE(descriptor != nullptr);
    {
        kwaque::runtime::basic_runtime runtime{*target};
        auto acquired
          = runtime.view<kwaque::runtime::runtime_capability::fault>();
        BOOST_REQUIRE(acquired.has_value());
        auto faults = std::move(*acquired);

        auto second = kwaque::runtime::fault_occurrence::make(2);
        BOOST_REQUIRE(second.has_value());
        auto out_of_order = faults.evaluate_fault(
          kwaque::runtime::fault_request{
            .point = descriptor->id,
            .occurrence = *second,
            .object = kwaque::runtime::fault_object_key::none(),
          });
        BOOST_REQUIRE(!out_of_order.has_value());
        BOOST_CHECK(
          out_of_order.error().code() == kwaque::errc::invalid_argument);

        auto selected = faults.evaluate_fault(
          kwaque::runtime::fault_request{
            .point = descriptor->id,
            .occurrence = kwaque::runtime::fault_occurrence::first(),
            .object = kwaque::runtime::fault_object_key::none(),
          });
        BOOST_REQUIRE(selected.has_value());
        BOOST_CHECK(selected->action() == kwaque::runtime::fault_action::none);
    }
    const auto occurrences = target->failure_probe().occurrences(
      kwaque::runtime::builtin_fault_point::queue_admission);
    BOOST_REQUIRE(occurrences.has_value());
    BOOST_CHECK_EQUAL(*occurrences, 1U);

    auto stopping = target->stop();
    co_await pump_until(target->event_scheduler(), stopping);
    co_await std::move(stopping);
}

SEASTAR_TEST_CASE(simulation_environment_stop_before_start_is_complete) {
    auto made = environment::make(config());
    BOOST_REQUIRE(made.has_value());
    auto target = std::move(*made);
    BOOST_CHECK_THROW(static_cast<void>(target->random()), std::logic_error);

    auto stopping = target->stop();
    co_await pump_until(target->event_scheduler(), stopping);
    co_await std::move(stopping);
    co_await target->stop();

    BOOST_CHECK(target->state() == kwaque::runtime::environment_state::stopped);
    BOOST_CHECK_EQUAL(target->event_sink().events().entries().size(), 2U);
    BOOST_CHECK_EQUAL(target->event_scheduler().pending_events(), 0U);
    BOOST_CHECK_THROW(static_cast<void>(target->random()), std::logic_error);
}

SEASTAR_TEST_CASE(simulation_environment_rejects_a_second_live_owner) {
    auto first = environment::make(config());
    BOOST_REQUIRE(first.has_value());
    auto second = environment::make(config());
    BOOST_REQUIRE(!second.has_value());
    BOOST_CHECK(second.error().code() == kwaque::errc::unavailable);

    auto stopping = (*first)->stop();
    co_await pump_until((*first)->event_scheduler(), stopping);
    co_await std::move(stopping);
}

SEASTAR_TEST_CASE(simulation_environment_stop_cancels_wall_adjustments) {
    auto made = environment::make(config());
    BOOST_REQUIRE(made.has_value());
    auto target = std::move(*made);
    co_await target->start();
    BOOST_REQUIRE(target->time()
                    .schedule_wall_offset(
                      kwaque::runtime::monotonic_time{17},
                      kwaque::simulation::wall_offset{17})
                    .has_value());

    auto stopping = target->stop();
    BOOST_CHECK_EQUAL(target->time().pending_adjustments(), 0U);
    BOOST_CHECK_EQUAL(target->time().offset().nanoseconds(), 0);
    BOOST_CHECK(!contains_trace_action(
      *target, kwaque::simulation::trace_action::wall_adjusted));
    co_await pump_until(target->event_scheduler(), stopping);
    co_await std::move(stopping);
    BOOST_CHECK_EQUAL(target->time().pending_adjustments(), 0U);
    BOOST_CHECK_EQUAL(target->event_scheduler().pending_events(), 0U);
    BOOST_CHECK_EQUAL(target->time().offset().nanoseconds(), 0);
}

SEASTAR_TEST_CASE(simulation_environment_stop_drains_every_active_owner) {
    auto made = environment::make(config());
    BOOST_REQUIRE(made.has_value());
    auto target = std::move(*made);
    co_await target->start();

    const auto query = component_dns_query();
    BOOST_REQUIRE(target->dns()
                    .add_record(
                      kwaque::simulation::fake_dns_record{
                        .key = query,
                        .answers = {component_dns_answer()},
                        .latency = kwaque::runtime::monotonic_duration{100},
                      })
                    .has_value());
    BOOST_REQUIRE(target->time()
                    .schedule_wall_offset(
                      kwaque::runtime::monotonic_time{100},
                      kwaque::simulation::wall_offset{17})
                    .has_value());

    seastar::abort_source timer_abort;
    seastar::abort_source dns_abort;
    auto sleeping = target->timer().sleep_until(
      kwaque::runtime::monotonic_time{100}, timer_abort);
    auto creating = target->file_system().create_directories(
      component_root_path());
    auto listening = target->network().listen(
      kwaque::runtime::network_endpoint{component_loopback, 0}, {});
    auto resolving = target->dns().resolve(query, dns_abort);
    BOOST_CHECK(!sleeping.available());
    BOOST_CHECK(!creating.available());
    BOOST_CHECK(!listening.available());
    BOOST_CHECK(!resolving.available());

    auto stopping = target->stop();
    co_await pump_until(target->event_scheduler(), listening);
    const auto listen_result = co_await std::move(listening);
    BOOST_REQUIRE(listen_result.has_value());
    co_await pump_until(target->event_scheduler(), stopping);
    co_await std::move(stopping);

    const auto sleep_result = co_await std::move(sleeping);
    const auto create_result = co_await std::move(creating);
    const auto resolve_result = co_await std::move(resolving);
    BOOST_REQUIRE(!sleep_result.has_value());
    BOOST_CHECK(sleep_result.error().code() == kwaque::errc::aborted);
    BOOST_REQUIRE(create_result.has_value());
    BOOST_REQUIRE(resolve_result.has_value());
    BOOST_REQUIRE_EQUAL(resolve_result->answers().size(), 1U);
    BOOST_CHECK(resolve_result->answers().front() == component_dns_answer());
    BOOST_CHECK_EQUAL(target->time().pending_adjustments(), 0U);
    BOOST_CHECK_EQUAL(
      environment_test_access::timer_owner(*target).pending_waits(), 0U);
    BOOST_CHECK_EQUAL(
      environment_test_access::file_system_owner(*target).pending_operations(),
      0U);
    BOOST_CHECK_EQUAL(
      environment_test_access::network_owner(*target).active_operations(), 0U);
    BOOST_CHECK_EQUAL(
      environment_test_access::dns_owner(*target).pending_queries(), 0U);
    BOOST_CHECK_EQUAL(target->event_scheduler().pending_events(), 0U);
}

SEASTAR_TEST_CASE(simulation_environment_start_fault_rolls_back_every_owner) {
    auto made = environment::make(config(rule(
      1,
      kwaque::runtime::builtin_fault_point::environment_start,
      1,
      kwaque::runtime::fault_decision::make_error())));
    BOOST_REQUIRE(made.has_value());
    auto failed = std::move(*made);
    co_await expect_start_failure(*failed);
    BOOST_CHECK_THROW(
      kwaque::runtime::basic_runtime{*failed}, std::logic_error);
    BOOST_CHECK_EQUAL(failed->event_sink().events().entries().size(), 3U);

    auto replacement = environment::make(config());
    BOOST_REQUIRE(replacement.has_value());
    auto stopping = (*replacement)->stop();
    co_await pump_until((*replacement)->event_scheduler(), stopping);
    co_await std::move(stopping);
}

SEASTAR_TEST_CASE(simulation_environment_preabort_rolls_back_start) {
    auto made = environment::make(config());
    BOOST_REQUIRE(made.has_value());
    auto target = std::move(*made);
    target->request_abort();

    auto starting = target->start();
    co_await pump_until(target->event_scheduler(), starting);
    bool aborted = false;
    try {
        co_await std::move(starting);
    } catch (const seastar::abort_requested_exception&) {
        aborted = true;
    }
    BOOST_REQUIRE(aborted);
    BOOST_CHECK(target->state() == kwaque::runtime::environment_state::stopped);
    BOOST_CHECK_EQUAL(target->event_scheduler().pending_events(), 0U);
}

SEASTAR_TEST_CASE(simulation_environment_rolls_back_each_start_boundary) {
    for (std::size_t point = 0;
         point < kwaque::simulation::environment_test_access::start_point_count;
         ++point) {
        auto made = environment::make(config());
        BOOST_REQUIRE(made.has_value());
        auto target = std::move(*made);

        auto starting = kwaque::simulation::environment_test_access::
          fail_before_start_point(*target, point);
        co_await pump_until(target->event_scheduler(), starting);
        bool failed = false;
        try {
            co_await std::move(starting);
        } catch (const std::bad_alloc&) {
            failed = true;
        }
        BOOST_REQUIRE(failed);
        BOOST_CHECK(
          target->state() == kwaque::runtime::environment_state::stopped);
        BOOST_CHECK_EQUAL(target->event_scheduler().pending_events(), 0U);
    }
}

SEASTAR_TEST_CASE(simulation_environment_metric_failure_rolls_back_all_owners) {
    namespace metrics = seastar::metrics;
    std::optional<metrics::metric_groups> blocker;
    blocker.emplace();
    blocker->add_group(
      "simulation",
      {metrics::make_gauge(
        "trace_entries",
        [] { return 0U; },
        metrics::description("Registration rollback blocker"))});

    auto made = environment::make(config());
    BOOST_REQUIRE(made.has_value());
    auto target = std::move(*made);
    auto starting = target->start();
    co_await pump_until(target->event_scheduler(), starting);
    bool failed = false;
    try {
        co_await std::move(starting);
    } catch (const metrics::double_registration&) {
        failed = true;
    }
    BOOST_REQUIRE(failed);
    BOOST_CHECK(target->state() == kwaque::runtime::environment_state::stopped);
    BOOST_CHECK(metric_registered("simulation_trace_entries"));
    BOOST_CHECK(!metric_registered("simulation_scheduler_pending_events"));
    BOOST_CHECK(!metric_registered("simulation_fake_dns_active"));
    BOOST_CHECK(!metric_registered("resource_manager_memory_configured_bytes"));
    BOOST_CHECK_EQUAL(target->event_scheduler().pending_events(), 0U);

    blocker.reset();
    BOOST_CHECK(!metric_registered("simulation_trace_entries"));
    auto replacement = environment::make(config());
    BOOST_REQUIRE(replacement.has_value());
    co_await (*replacement)->start();
    auto stopping = (*replacement)->stop();
    co_await pump_until((*replacement)->event_scheduler(), stopping);
    co_await std::move(stopping);
}

SEASTAR_TEST_CASE(simulation_environment_resource_probe_rolls_back_registry) {
    constexpr auto creation_points
      = kwaque::resource::all_workload_classes.size() * 2U;
    for (std::uint64_t selected_occurrence = 1;
         selected_occurrence <= creation_points;
         ++selected_occurrence) {
        auto made = environment::make(config(rule(
          2,
          kwaque::runtime::builtin_fault_point::resource_group_create,
          selected_occurrence,
          kwaque::runtime::fault_decision::make_error())));
        BOOST_REQUIRE(made.has_value());
        auto target = std::move(*made);
        co_await expect_start_failure(*target);
        const auto occurrences = target->failure_probe().occurrences(
          kwaque::runtime::builtin_fault_point::resource_group_create);
        BOOST_REQUIRE(occurrences.has_value());
        BOOST_CHECK_EQUAL(*occurrences, selected_occurrence);
    }
}

SEASTAR_TEST_CASE(
  simulation_environment_stop_error_preserves_complete_cleanup) {
    auto made = environment::make(config(rule(
      3,
      kwaque::runtime::builtin_fault_point::environment_stop,
      1,
      kwaque::runtime::fault_decision::make_error())));
    BOOST_REQUIRE(made.has_value());
    auto target = std::move(*made);
    co_await target->start();
    target->request_abort();

    auto stopping = target->stop();
    co_await pump_until(target->event_scheduler(), stopping);
    bool failed = false;
    try {
        co_await std::move(stopping);
    } catch (const std::system_error&) {
        failed = true;
    }
    BOOST_REQUIRE(failed);

    bool repeated_failure = false;
    try {
        co_await target->stop();
    } catch (const std::system_error&) {
        repeated_failure = true;
    }
    BOOST_REQUIRE(repeated_failure);
    BOOST_CHECK(target->state() == kwaque::runtime::environment_state::stopped);
    BOOST_CHECK_EQUAL(target->event_scheduler().pending_events(), 0U);
    BOOST_CHECK_EQUAL(target->event_sink().events().entries().size(), 4U);
}

SEASTAR_TEST_CASE(simulation_environment_stop_delay_uses_virtual_control_time) {
    auto made = environment::make(config(rule(
      4,
      kwaque::runtime::builtin_fault_point::environment_stop,
      1,
      kwaque::runtime::fault_decision::make_delay(
        kwaque::runtime::monotonic_duration{37}))));
    BOOST_REQUIRE(made.has_value());
    auto target = std::move(*made);
    co_await target->start();
    target->request_abort();
    BOOST_REQUIRE(target->time()
                    .schedule_wall_offset(
                      kwaque::runtime::monotonic_time{17},
                      kwaque::simulation::wall_offset{17})
                    .has_value());

    auto stopping = target->stop();
    BOOST_CHECK(!stopping.available());
    BOOST_CHECK_EQUAL(target->time().pending_adjustments(), 0U);
    BOOST_CHECK_EQUAL(target->event_scheduler().pending_events(), 1U);
    BOOST_CHECK_EQUAL(target->time().offset().nanoseconds(), 0);
    BOOST_CHECK(!contains_trace_action(
      *target, kwaque::simulation::trace_action::wall_adjusted));
    co_await pump_until(target->event_scheduler(), stopping);
    co_await std::move(stopping);
    BOOST_CHECK_GE(target->event_scheduler().now().nanoseconds(), 37U);
    BOOST_CHECK_EQUAL(target->event_scheduler().pending_events(), 0U);
    BOOST_CHECK_EQUAL(target->time().offset().nanoseconds(), 0);
    BOOST_CHECK(!contains_trace_action(
      *target, kwaque::simulation::trace_action::wall_adjusted));
}
