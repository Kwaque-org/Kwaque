#include "src/runtime/testing/failure_probe/failure_probe.h"
#include "src/runtime/testing/failure_probe/failure_probe_test_support.h"
#include "src/simulation/determinism_version.h"
#include "src/simulation/deterministic_random.h"
#include "src/simulation/event_trace.h"
#include "src/simulation/fault_schedule.h"
#include "src/simulation/scheduler.h"
#include "src/simulation/timer.h"

#include <seastar/core/chunked_vector.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace {

using kwaque::runtime::builtin_fault_point;
using kwaque::runtime::fault_action;
using kwaque::runtime::fault_decision;
using kwaque::runtime::fault_object_key;
using kwaque::runtime::fault_occurrence;
using kwaque::runtime::fault_request;
using kwaque::runtime::monotonic_duration;
using kwaque::runtime::monotonic_time;
using kwaque::runtime::operation_error;
using kwaque::runtime::operation_kind;
using kwaque::runtime::result;
using kwaque::runtime::testing::apply_failure_evaluation;
using kwaque::runtime::testing::failure_evaluation;
using kwaque::runtime::testing::failure_probe;
using kwaque::runtime::testing::failure_probe_test_access;
using kwaque::runtime::testing::logical_failure_points;
using kwaque::simulation::event_trace;
using kwaque::simulation::fault_rule;
using kwaque::simulation::fault_rule_id;
using kwaque::simulation::fault_schedule;
using kwaque::simulation::fault_selector;
using kwaque::simulation::scheduler;
using kwaque::simulation::scheduler_limit_values;
using kwaque::simulation::scheduler_limits;
using kwaque::simulation::timer;
using kwaque::simulation::trace_action;
using kwaque::simulation::trace_digest;
using kwaque::simulation::trace_header;
using kwaque::simulation::trace_limit_values;
using kwaque::simulation::trace_limits;

constexpr std::uint64_t seed{53};

const kwaque::runtime::fault_point_descriptor&
fault_descriptor(builtin_fault_point point) {
    const auto* descriptor = kwaque::runtime::descriptor_for(point);
    if (descriptor == nullptr) {
        throw std::logic_error("fault descriptor is missing");
    }
    return *descriptor;
}

class fixed_injector final {
public:
    class preparation final {
    public:
        [[nodiscard]] fault_decision preview() const noexcept {
            return decision_;
        }
        [[nodiscard]] result<fault_decision> commit() noexcept {
            return decision_;
        }

    private:
        friend class fixed_injector;

        explicit preparation(fault_decision decision) noexcept
          : decision_(decision) {}

        fault_decision decision_;
    };

    explicit fixed_injector(fault_decision decision = {}) noexcept
      : decision_(decision) {}

    result<fault_decision> evaluate(const fault_request& request) noexcept {
        ++calls_;
        last_ = request;
        if (failure_) {
            return kwaque::runtime::failure(*failure_);
        }
        return decision_;
    }

    result<preparation> prepare(
      const fault_request& request, monotonic_time, monotonic_time) noexcept {
        ++calls_;
        last_ = request;
        if (failure_) {
            return kwaque::runtime::failure(*failure_);
        }
        return preparation{decision_};
    }

    void fail(kwaque::errc code) noexcept {
        failure_ = operation_error{code, operation_kind::fault};
    }
    [[nodiscard]] std::size_t calls() const noexcept { return calls_; }
    [[nodiscard]] const std::optional<fault_request>& last() const noexcept {
        return last_;
    }

private:
    fault_decision decision_;
    std::optional<operation_error> failure_;
    std::optional<fault_request> last_;
    std::size_t calls_{0};
};

class reentrant_injector final {
public:
    explicit reentrant_injector(failure_probe& probe) noexcept
      : probe_(&probe) {}

    result<fixed_injector::preparation> prepare(
      const fault_request& request,
      monotonic_time now,
      monotonic_time maximum_deadline) noexcept {
        const auto nested = probe_->evaluate(
          nested_,
          builtin_fault_point::environment_start,
          now,
          maximum_deadline);
        nested_error_ = nested ? kwaque::errc::success : nested.error().code();
        return outer_.prepare(request, now, maximum_deadline);
    }

    [[nodiscard]] std::optional<kwaque::errc> nested_error() const noexcept {
        return nested_error_;
    }
    [[nodiscard]] std::size_t nested_calls() const noexcept {
        return nested_.calls();
    }

private:
    failure_probe* probe_;
    fixed_injector outer_;
    fixed_injector nested_;
    std::optional<kwaque::errc> nested_error_;
};

class allocation_failing_timer final {
public:
    seastar::future<result<void>>
    sleep_until(monotonic_time deadline, seastar::abort_source&) {
        ++calls_;
        deadline_ = deadline;
        return seastar::make_exception_future<result<void>>(std::bad_alloc{});
    }
    void request_abort() noexcept {}
    seastar::future<result<void>> stop() {
        return seastar::make_ready_future<result<void>>(result<void>{});
    }

    [[nodiscard]] std::size_t calls() const noexcept { return calls_; }
    [[nodiscard]] std::optional<monotonic_time> deadline() const noexcept {
        return deadline_;
    }

private:
    std::optional<monotonic_time> deadline_;
    std::size_t calls_{0};
};

static_assert(kwaque::runtime::fault_injector<fixed_injector>);
static_assert(kwaque::runtime::testing::failure_probe_injector<fixed_injector>);
static_assert(
  kwaque::runtime::testing::failure_probe_injector<reentrant_injector>);
static_assert(kwaque::runtime::testing::failure_probe_injector<fault_schedule>);
static_assert(kwaque::runtime::timer_service<allocation_failing_timer>);
static_assert(!std::is_copy_constructible_v<failure_probe>);
static_assert(!std::is_move_constructible_v<failure_probe>);
static_assert(std::is_trivially_destructible_v<failure_probe>);
static_assert(std::is_copy_constructible_v<failure_evaluation>);
static_assert(std::is_trivially_destructible_v<failure_evaluation>);
static_assert(sizeof(failure_probe) <= 48U);
static_assert(sizeof(failure_evaluation) <= 96U);

scheduler_limits make_scheduler_limits() {
    auto made = scheduler_limits::make(
      scheduler_limit_values{
        .pending_events = 64,
        .events_per_pump = 64,
        .total_events = 256,
        .maximum_deadline = monotonic_time{100},
      });
    BOOST_REQUIRE(made.has_value());
    return *made;
}

trace_limits make_trace_limits() {
    auto made = trace_limits::make(
      trace_limit_values{
        .entries = 256,
        .encoded_bytes = kwaque::simulation::canonical_header_encoded_size
                         + 256U
                             * kwaque::simulation::canonical_entry_encoded_size,
        .line_bytes = 1'024,
      });
    BOOST_REQUIRE(made.has_value());
    return *made;
}

trace_header
make_header(scheduler_limits scheduler_budget, trace_limits trace_budget) {
    return trace_header::current(
      seed,
      kwaque::simulation::deterministic_random_algorithm_version,
      kwaque::simulation::deterministic_random_coordinate_version,
      kwaque::simulation::trace_scheduler_budget{
        .pending_events = scheduler_budget.pending_events(),
        .events_per_pump = scheduler_budget.events_per_pump(),
        .total_events = scheduler_budget.total_events(),
        .maximum_deadline = scheduler_budget.maximum_deadline().nanoseconds(),
      },
      trace_budget,
      trace_digest{},
      trace_digest{});
}

fault_rule rule(
  std::uint64_t id,
  builtin_fault_point point,
  fault_decision decision,
  std::uint64_t occurrence = 1) {
    auto rule_id = fault_rule_id::make(id);
    auto selected_occurrence = fault_occurrence::make(occurrence);
    BOOST_REQUIRE(rule_id.has_value());
    BOOST_REQUIRE(selected_occurrence.has_value());
    auto made = fault_rule::make(
      *rule_id,
      point,
      std::nullopt,
      *selected_occurrence,
      *selected_occurrence,
      fault_selector::once(),
      decision);
    BOOST_REQUIRE(made.has_value());
    return *made;
}

struct fixture final {
    scheduler_limits scheduler_budget{make_scheduler_limits()};
    trace_limits trace_budget{make_trace_limits()};
    event_trace trace{
      make_header(scheduler_budget, trace_budget), trace_budget};
    scheduler events{scheduler_budget, &trace};
    timer delays{events};
    std::unique_ptr<fault_schedule> faults;
    failure_probe probe;

    explicit fixture(seastar::chunked_vector<fault_rule> rules) {
        auto made = fault_schedule::make(events, trace, seed, std::move(rules));
        BOOST_REQUIRE(made.has_value());
        faults = std::move(*made);
    }
};

void run_next(scheduler& events) {
    if (!events.has_ready_events()) {
        const auto advanced = events.advance_to_next();
        BOOST_REQUIRE(advanced.has_value());
        BOOST_REQUIRE(advanced->has_value());
    }
    const auto ran = events.run_ready();
    BOOST_REQUIRE(ran.has_value());
    BOOST_REQUIRE(*ran != 0U);
}

std::size_t fault_entries(const event_trace& trace) {
    return static_cast<std::size_t>(
      std::ranges::count_if(trace.entries(), [](const auto& entry) {
          return entry.action == trace_action::fault_evaluated;
      }));
}

} // namespace

SEASTAR_TEST_CASE(failure_probe_assigns_exact_independent_occurrences) {
    failure_probe probe;
    fixed_injector injector;
    for (const auto point : logical_failure_points) {
        const auto& descriptor = fault_descriptor(point);
        for (std::uint64_t occurrence = 1; occurrence <= 3; ++occurrence) {
            const auto object = fault_object_key::from_u64(occurrence + 100U);
            const auto evaluated = probe.evaluate(
              injector, point, monotonic_time{7}, monotonic_time{100}, object);
            BOOST_REQUIRE(evaluated.has_value());
            BOOST_CHECK(evaluated->owner() == probe.owner());
            BOOST_CHECK(evaluated->request().point == descriptor.id);
            BOOST_CHECK_EQUAL(
              evaluated->request().occurrence.value(), occurrence);
            BOOST_CHECK(evaluated->request().object == object);
            BOOST_CHECK(evaluated->decision().action() == fault_action::none);
            BOOST_CHECK(!evaluated->deadline());
        }
        const auto count = probe.occurrences(point);
        BOOST_REQUIRE(count.has_value());
        BOOST_CHECK_EQUAL(*count, 3U);
    }
    BOOST_CHECK_EQUAL(injector.calls(), logical_failure_points.size() * 3U);

    const auto rejected = probe.evaluate(
      injector,
      builtin_fault_point::timer,
      monotonic_time{},
      monotonic_time{100});
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == kwaque::errc::invalid_argument);
    BOOST_CHECK_EQUAL(injector.calls(), logical_failure_points.size() * 3U);
    BOOST_CHECK(!probe.occurrences(builtin_fault_point::timer).has_value());
    co_return;
}

SEASTAR_TEST_CASE(failure_probe_rejects_same_point_reentry) {
    failure_probe probe;
    reentrant_injector injector{probe};
    const auto evaluated = probe.evaluate(
      injector,
      builtin_fault_point::environment_start,
      monotonic_time{},
      monotonic_time{100});
    BOOST_REQUIRE(evaluated.has_value());
    BOOST_REQUIRE(injector.nested_error().has_value());
    BOOST_CHECK(*injector.nested_error() == kwaque::errc::unavailable);
    BOOST_CHECK_EQUAL(injector.nested_calls(), 0U);
    const auto count = probe.occurrences(
      builtin_fault_point::environment_start);
    BOOST_REQUIRE(count.has_value());
    BOOST_CHECK_EQUAL(*count, 1U);
    co_return;
}

SEASTAR_TEST_CASE(failure_probe_accepts_exact_logical_action_matrix) {
    struct point_action final {
        builtin_fault_point point;
        fault_action action;
    };
    constexpr std::array combinations{
      point_action{builtin_fault_point::environment_start, fault_action::error},
      point_action{builtin_fault_point::environment_start, fault_action::delay},
      point_action{
        builtin_fault_point::resource_group_create, fault_action::error},
      point_action{builtin_fault_point::queue_admission, fault_action::error},
      point_action{builtin_fault_point::queue_admission, fault_action::delay},
      point_action{builtin_fault_point::environment_stop, fault_action::error},
      point_action{builtin_fault_point::environment_stop, fault_action::delay},
    };
    for (const auto [point, action] : combinations) {
        failure_probe probe;
        fixed_injector injector{
          action == fault_action::error
            ? fault_decision::make_error()
            : fault_decision::make_delay(monotonic_duration{10})};
        const auto evaluated = probe.evaluate(
          injector, point, monotonic_time{7}, monotonic_time{100});
        BOOST_REQUIRE(evaluated.has_value());
        BOOST_CHECK(evaluated->decision().action() == action);
        if (action == fault_action::delay) {
            BOOST_REQUIRE(evaluated->deadline().has_value());
            BOOST_CHECK(*evaluated->deadline() == monotonic_time{17});
        } else {
            BOOST_CHECK(!evaluated->deadline());
        }
    }

    failure_probe no_fault_probe;
    fixed_injector no_fault_injector;
    const auto no_fault = no_fault_probe.evaluate(
      no_fault_injector,
      builtin_fault_point::environment_start,
      monotonic_time{},
      monotonic_time{100});
    BOOST_REQUIRE(no_fault.has_value());
    allocation_failing_timer unused_timer;
    seastar::abort_source abort_source;
    const auto no_fault_outcome = co_await apply_failure_evaluation(
      *no_fault, unused_timer, abort_source);
    BOOST_REQUIRE(no_fault_outcome.has_value());
    BOOST_CHECK_EQUAL(unused_timer.calls(), 0U);
    co_return;
}

SEASTAR_TEST_CASE(failure_probe_rejects_before_counter_commit) {
    failure_probe probe;
    fixed_injector invalid_window;
    const auto invalid_time = probe.evaluate(
      invalid_window,
      builtin_fault_point::environment_start,
      monotonic_time{101},
      monotonic_time{100});
    BOOST_REQUIRE(!invalid_time.has_value());
    BOOST_CHECK(invalid_time.error().code() == kwaque::errc::invalid_argument);
    BOOST_CHECK_EQUAL(invalid_window.calls(), 0U);

    fixed_injector unavailable;
    unavailable.fail(kwaque::errc::unavailable);
    const auto injector_failure = probe.evaluate(
      unavailable,
      builtin_fault_point::environment_start,
      monotonic_time{},
      monotonic_time{100});
    BOOST_REQUIRE(!injector_failure.has_value());

    fixed_injector zero_delay{fault_decision::make_delay(monotonic_duration{})};
    const auto zero = probe.evaluate(
      zero_delay,
      builtin_fault_point::environment_start,
      monotonic_time{},
      monotonic_time{100});
    BOOST_REQUIRE(!zero.has_value());
    BOOST_CHECK(zero.error().code() == kwaque::errc::invalid_argument);

    fixed_injector unsupported{fault_decision::make_crash()};
    const auto wrong_action = probe.evaluate(
      unsupported,
      builtin_fault_point::environment_start,
      monotonic_time{},
      monotonic_time{100});
    BOOST_REQUIRE(!wrong_action.has_value());
    BOOST_CHECK(wrong_action.error().code() == kwaque::errc::invalid_argument);

    fixed_injector overflowing{
      fault_decision::make_delay(monotonic_duration{2})};
    const auto overflow = probe.evaluate(
      overflowing,
      builtin_fault_point::environment_start,
      monotonic_time{std::numeric_limits<std::uint64_t>::max() - 1U},
      monotonic_time::maximum());
    BOOST_REQUIRE(!overflow.has_value());
    BOOST_CHECK(overflow.error().code() == kwaque::errc::out_of_range);

    fixed_injector beyond_limit{
      fault_decision::make_delay(monotonic_duration{11})};
    const auto limited = probe.evaluate(
      beyond_limit,
      builtin_fault_point::environment_start,
      monotonic_time{90},
      monotonic_time{100});
    BOOST_REQUIRE(!limited.has_value());
    BOOST_CHECK(limited.error().code() == kwaque::errc::out_of_range);

    const auto count = probe.occurrences(
      builtin_fault_point::environment_start);
    BOOST_REQUIRE(count.has_value());
    BOOST_CHECK_EQUAL(*count, 0U);

    BOOST_REQUIRE(
      failure_probe_test_access::seed(
        probe,
        builtin_fault_point::environment_stop,
        std::numeric_limits<std::uint64_t>::max())
        .has_value());
    fixed_injector none;
    const auto exhausted = probe.evaluate(
      none,
      builtin_fault_point::environment_stop,
      monotonic_time{},
      monotonic_time{100});
    BOOST_REQUIRE(!exhausted.has_value());
    BOOST_CHECK(exhausted.error().code() == kwaque::errc::out_of_range);
    BOOST_CHECK_EQUAL(none.calls(), 0U);
    co_return;
}

SEASTAR_TEST_CASE(failure_probe_and_schedule_apply_error_and_virtual_delay) {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      1, builtin_fault_point::environment_start, fault_decision::make_error()));
    rules.push_back(rule(
      2,
      builtin_fault_point::queue_admission,
      fault_decision::make_delay(monotonic_duration{10})));
    rules.push_back(rule(
      3,
      builtin_fault_point::resource_group_create,
      fault_decision::make_error()));
    fixture environment{std::move(rules)};
    seastar::abort_source abort_source;
    bool owner_effect = false;

    const auto allocations_before = seastar::memory::stats().mallocs();
    const auto rejected = environment.probe.evaluate(
      *environment.faults,
      builtin_fault_point::environment_start,
      environment.events.now(),
      environment.scheduler_budget.maximum_deadline());
    const auto allocations_after = seastar::memory::stats().mallocs();
    BOOST_REQUIRE(rejected.has_value());
    BOOST_CHECK_EQUAL(allocations_after, allocations_before);
    auto error_application = apply_failure_evaluation(
      *rejected, environment.delays, abort_source);
    BOOST_CHECK(error_application.available());
    const auto error = co_await std::move(error_application);
    BOOST_REQUIRE(!error.has_value());
    BOOST_CHECK(error.error().code() == kwaque::errc::fault_injected);
    BOOST_CHECK(error.error().operation() == operation_kind::fault);
    BOOST_CHECK(!owner_effect);

    const auto resource_error = environment.probe.evaluate(
      *environment.faults,
      builtin_fault_point::resource_group_create,
      environment.events.now(),
      environment.scheduler_budget.maximum_deadline());
    BOOST_REQUIRE(resource_error.has_value());
    const auto resource_outcome = co_await apply_failure_evaluation(
      *resource_error, environment.delays, abort_source);
    BOOST_REQUIRE(!resource_outcome.has_value());
    BOOST_CHECK(
      resource_outcome.error().code() == kwaque::errc::fault_injected);

    const auto delayed = environment.probe.evaluate(
      *environment.faults,
      builtin_fault_point::queue_admission,
      environment.events.now(),
      environment.scheduler_budget.maximum_deadline());
    BOOST_REQUIRE(delayed.has_value());
    BOOST_REQUIRE(delayed->deadline().has_value());
    BOOST_CHECK(*delayed->deadline() == monotonic_time{10});
    auto waiting = apply_failure_evaluation(
      *delayed, environment.delays, abort_source);
    BOOST_CHECK(!waiting.available());
    run_next(environment.events);
    const auto completed = co_await std::move(waiting);
    BOOST_REQUIRE(completed.has_value());
    owner_effect = true;
    BOOST_CHECK(owner_effect);
    BOOST_CHECK(environment.events.now() == monotonic_time{10});
    BOOST_CHECK_EQUAL(fault_entries(environment.trace), 3U);
    BOOST_CHECK_EQUAL(environment.faults->evaluations(), 3U);
    BOOST_CHECK_EQUAL(environment.faults->applied_decisions(), 3U);

    const auto stopped = co_await environment.delays.stop();
    BOOST_REQUIRE(stopped.has_value());
}

SEASTAR_TEST_CASE(failure_probe_delay_stop_has_one_aborted_terminal) {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      3,
      builtin_fault_point::environment_stop,
      fault_decision::make_delay(monotonic_duration{10})));
    fixture environment{std::move(rules)};
    seastar::abort_source abort_source;

    const auto delayed = environment.probe.evaluate(
      *environment.faults,
      builtin_fault_point::environment_stop,
      environment.events.now(),
      environment.scheduler_budget.maximum_deadline());
    BOOST_REQUIRE(delayed.has_value());
    auto waiting = apply_failure_evaluation(
      *delayed, environment.delays, abort_source);
    auto stopping = environment.delays.stop();
    BOOST_CHECK(!waiting.available());
    run_next(environment.events);
    const auto outcome = co_await std::move(waiting);
    const auto stopped = co_await std::move(stopping);
    BOOST_REQUIRE(!outcome.has_value());
    BOOST_CHECK(outcome.error().code() == kwaque::errc::aborted);
    BOOST_REQUIRE(stopped.has_value());
    BOOST_CHECK_EQUAL(fault_entries(environment.trace), 1U);
    const auto count = environment.probe.occurrences(
      builtin_fault_point::environment_stop);
    BOOST_REQUIRE(count.has_value());
    BOOST_CHECK_EQUAL(*count, 1U);
}

SEASTAR_TEST_CASE(failure_probe_delay_allocation_failure_has_one_terminal) {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      6,
      builtin_fault_point::queue_admission,
      fault_decision::make_delay(monotonic_duration{10})));
    fixture environment{std::move(rules)};
    allocation_failing_timer failing_timer;
    seastar::abort_source abort_source;
    bool owner_effect = false;

    const auto delayed = environment.probe.evaluate(
      *environment.faults,
      builtin_fault_point::queue_admission,
      environment.events.now(),
      environment.scheduler_budget.maximum_deadline());
    BOOST_REQUIRE(delayed.has_value());
    bool allocation_failed = false;
    try {
        static_cast<void>(co_await apply_failure_evaluation(
          *delayed, failing_timer, abort_source));
    } catch (const std::bad_alloc&) {
        allocation_failed = true;
    }
    BOOST_REQUIRE(allocation_failed);
    BOOST_CHECK_EQUAL(failing_timer.calls(), 1U);
    BOOST_REQUIRE(failing_timer.deadline().has_value());
    BOOST_CHECK(*failing_timer.deadline() == monotonic_time{10});
    BOOST_CHECK_EQUAL(fault_entries(environment.trace), 1U);
    const auto count = environment.probe.occurrences(
      builtin_fault_point::queue_admission);
    BOOST_REQUIRE(count.has_value());
    BOOST_CHECK_EQUAL(*count, 1U);
    BOOST_CHECK(!owner_effect);
    co_return;
}

SEASTAR_TEST_CASE(failure_probe_trace_saturation_does_not_commit_occurrence) {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      7, builtin_fault_point::environment_start, fault_decision::make_error()));
    fixture environment{std::move(rules)};
    auto full_capacity = environment.trace.reserve(
      environment.trace_budget.entries(),
      static_cast<std::uint64_t>(environment.trace_budget.entries())
        * kwaque::simulation::canonical_entry_encoded_size);
    BOOST_REQUIRE(full_capacity.has_value());

    const auto evaluated = environment.probe.evaluate(
      *environment.faults,
      builtin_fault_point::environment_start,
      environment.events.now(),
      environment.scheduler_budget.maximum_deadline());
    BOOST_REQUIRE(!evaluated.has_value());
    BOOST_CHECK(evaluated.error().code() == kwaque::errc::resource_exhausted);
    BOOST_CHECK(environment.trace.entries().empty());
    BOOST_CHECK_EQUAL(environment.faults->evaluations(), 1U);
    BOOST_CHECK_EQUAL(environment.faults->applied_decisions(), 0U);
    const auto count = environment.probe.occurrences(
      builtin_fault_point::environment_start);
    BOOST_REQUIRE(count.has_value());
    BOOST_CHECK_EQUAL(*count, 0U);
    co_return;
}

SEASTAR_TEST_CASE(
  failure_probe_unschedulable_delay_precedes_trace_and_counter) {
    const auto zero_rule_id = fault_rule_id::make(99);
    const auto first_occurrence = fault_occurrence::make(1);
    BOOST_REQUIRE(zero_rule_id.has_value());
    BOOST_REQUIRE(first_occurrence.has_value());
    BOOST_CHECK(!fault_rule::make(
                   *zero_rule_id,
                   builtin_fault_point::environment_start,
                   std::nullopt,
                   *first_occurrence,
                   *first_occurrence,
                   fault_selector::once(),
                   fault_decision::make_delay(monotonic_duration{}))
                   .has_value());

    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      4,
      builtin_fault_point::queue_admission,
      fault_decision::make_delay(monotonic_duration{101})));
    fixture environment{std::move(rules)};

    const auto before = seastar::memory::stats().mallocs();
    const auto evaluated = environment.probe.evaluate(
      *environment.faults,
      builtin_fault_point::queue_admission,
      environment.events.now(),
      environment.scheduler_budget.maximum_deadline());
    const auto after = seastar::memory::stats().mallocs();
    BOOST_REQUIRE(!evaluated.has_value());
    BOOST_CHECK(evaluated.error().code() == kwaque::errc::out_of_range);
    BOOST_CHECK_EQUAL(after, before);
    BOOST_CHECK(environment.trace.entries().empty());
    BOOST_CHECK_EQUAL(environment.faults->evaluations(), 0U);
    BOOST_CHECK_EQUAL(environment.faults->applied_decisions(), 0U);
    const auto count = environment.probe.occurrences(
      builtin_fault_point::queue_admission);
    BOOST_REQUIRE(count.has_value());
    BOOST_CHECK_EQUAL(*count, 0U);

    const auto skipped_rule_id = fault_rule_id::make(100);
    const auto second_occurrence = fault_occurrence::make(2);
    const auto every_second = fault_selector::every_n(2);
    BOOST_REQUIRE(skipped_rule_id.has_value());
    BOOST_REQUIRE(second_occurrence.has_value());
    BOOST_REQUIRE(every_second.has_value());
    auto skipped_rule = fault_rule::make(
      *skipped_rule_id,
      builtin_fault_point::queue_admission,
      std::nullopt,
      *first_occurrence,
      *second_occurrence,
      *every_second,
      fault_decision::make_delay(monotonic_duration{101}));
    BOOST_REQUIRE(skipped_rule.has_value());
    seastar::chunked_vector<fault_rule> skipped_rules;
    skipped_rules.push_back(*skipped_rule);
    fixture skipped{std::move(skipped_rules)};
    BOOST_REQUIRE(
      failure_probe_test_access::seed(
        skipped.probe, builtin_fault_point::queue_admission, 1)
        .has_value());
    const auto skipped_evaluation = skipped.probe.evaluate(
      *skipped.faults,
      builtin_fault_point::queue_admission,
      skipped.events.now(),
      skipped.scheduler_budget.maximum_deadline());
    BOOST_REQUIRE(skipped_evaluation.has_value());
    BOOST_CHECK(skipped_evaluation->decision().action() == fault_action::none);
    BOOST_CHECK_EQUAL(fault_entries(skipped.trace), 1U);
    BOOST_CHECK_EQUAL(skipped.faults->evaluations(), 1U);
    BOOST_CHECK_EQUAL(skipped.faults->applied_decisions(), 0U);
    const auto skipped_count = skipped.probe.occurrences(
      builtin_fault_point::queue_admission);
    BOOST_REQUIRE(skipped_count.has_value());
    BOOST_CHECK_EQUAL(*skipped_count, 2U);
    co_return;
}

SEASTAR_TEST_CASE(
  failure_probe_uses_its_deadline_limit_before_schedule_commit) {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      105,
      builtin_fault_point::environment_start,
      fault_decision::make_delay(monotonic_duration{75})));
    fixture environment{std::move(rules)};

    const auto evaluated = environment.probe.evaluate(
      *environment.faults,
      builtin_fault_point::environment_start,
      environment.events.now(),
      monotonic_time{50});
    BOOST_REQUIRE(!evaluated.has_value());
    BOOST_CHECK(evaluated.error().code() == kwaque::errc::out_of_range);
    BOOST_CHECK(environment.trace.entries().empty());
    BOOST_CHECK_EQUAL(environment.faults->evaluations(), 0U);
    BOOST_CHECK_EQUAL(environment.faults->applied_decisions(), 0U);
    const auto count = environment.probe.occurrences(
      builtin_fault_point::environment_start);
    BOOST_REQUIRE(count.has_value());
    BOOST_CHECK_EQUAL(*count, 0U);
    co_return;
}

SEASTAR_TEST_CASE(failure_probe_replay_diverges_before_probe_commit_or_effect) {
    seastar::chunked_vector<fault_rule> captured_rules;
    captured_rules.push_back(rule(
      5, builtin_fault_point::environment_start, fault_decision::make_error()));
    fixture captured{std::move(captured_rules)};
    const auto evaluated = captured.probe.evaluate(
      *captured.faults,
      builtin_fault_point::environment_start,
      captured.events.now(),
      captured.scheduler_budget.maximum_deadline());
    BOOST_REQUIRE(evaluated.has_value());
    auto encoded = captured.trace.encode();
    BOOST_REQUIRE(encoded.has_value());
    auto decoded = event_trace::decode(*encoded, captured.trace_budget);
    BOOST_REQUIRE(decoded.has_value());
    BOOST_REQUIRE(!decoded->entries.empty());
    ++decoded->entries.front().stable_id;

    auto replay_trace = event_trace::replay(
      captured.trace.header(), captured.trace_budget, std::move(*decoded));
    BOOST_REQUIRE(replay_trace.has_value());
    scheduler replay_events{captured.scheduler_budget, replay_trace->get()};
    seastar::chunked_vector<fault_rule> replay_rules;
    replay_rules.push_back(rule(
      5, builtin_fault_point::environment_start, fault_decision::make_error()));
    auto replay_faults = fault_schedule::make(
      replay_events, **replay_trace, seed, std::move(replay_rules));
    BOOST_REQUIRE(replay_faults.has_value());
    failure_probe replay_probe;
    bool owner_effect = false;
    const auto divergence = replay_probe.evaluate(
      **replay_faults,
      builtin_fault_point::environment_start,
      replay_events.now(),
      captured.scheduler_budget.maximum_deadline());
    BOOST_REQUIRE(!divergence.has_value());
    BOOST_CHECK(divergence.error().code() == kwaque::errc::replay_divergence);
    const auto count = replay_probe.occurrences(
      builtin_fault_point::environment_start);
    BOOST_REQUIRE(count.has_value());
    BOOST_CHECK_EQUAL(*count, 0U);
    BOOST_CHECK(!owner_effect);
    BOOST_CHECK((*replay_trace)->failed());
    co_return;
}
