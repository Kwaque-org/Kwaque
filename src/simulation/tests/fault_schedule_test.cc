#include "src/simulation/fault_schedule.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <type_traits>
#include <utility>

namespace {

using kwaque::runtime::builtin_fault_point;
using kwaque::runtime::fault_action;
using kwaque::runtime::fault_decision;
using kwaque::runtime::fault_object_key;
using kwaque::runtime::fault_occurrence;
using kwaque::runtime::fault_request;
using kwaque::simulation::event_trace;
using kwaque::simulation::fault_rule;
using kwaque::simulation::fault_rule_id;
using kwaque::simulation::fault_schedule;
using kwaque::simulation::fault_schedule_limits;
using kwaque::simulation::fault_selector;
using kwaque::simulation::fault_trace_no_sample;
using kwaque::simulation::random_coordinate;
using kwaque::simulation::random_domain;
using kwaque::simulation::scheduler;
using kwaque::simulation::scheduler_limit_values;
using kwaque::simulation::scheduler_limits;
using kwaque::simulation::trace_action;
using kwaque::simulation::trace_digest;
using kwaque::simulation::trace_event_kind;
using kwaque::simulation::trace_header;
using kwaque::simulation::trace_limit_values;
using kwaque::simulation::trace_limits;
using kwaque::simulation::trace_scheduler_budget;

constexpr std::uint64_t master_seed{UINT64_C(0x123456789abcdef0)};

static_assert(!std::is_default_constructible_v<fault_rule_id>);
static_assert(
  !std::is_copy_constructible_v<kwaque::simulation::prepared_fault_evaluation>);
static_assert(
  std::is_move_constructible_v<kwaque::simulation::prepared_fault_evaluation>);

scheduler_limits test_scheduler_limits() {
    auto limits = scheduler_limits::make(
      scheduler_limit_values{
        .pending_events = 32,
        .events_per_pump = 64,
        .total_events = 128,
        .maximum_deadline = kwaque::runtime::monotonic_time{1'000},
      });
    BOOST_REQUIRE(limits.has_value());
    return *limits;
}

trace_limits test_trace_limits(std::uint32_t entries = 128) {
    auto limits = trace_limits::make(
      trace_limit_values{
        .entries = entries,
        .encoded_bytes = kwaque::simulation::canonical_header_encoded_size
                         + static_cast<std::uint64_t>(entries)
                             * kwaque::simulation::canonical_entry_encoded_size,
        .line_bytes = 1'024,
      });
    BOOST_REQUIRE(limits.has_value());
    return *limits;
}

trace_header
test_header(scheduler_limits scheduler_budget, trace_limits trace_budget) {
    return trace_header::current(
      master_seed,
      kwaque::simulation::deterministic_random_algorithm_version,
      kwaque::simulation::deterministic_random_coordinate_version,
      trace_scheduler_budget{
        .pending_events = scheduler_budget.pending_events(),
        .events_per_pump = scheduler_budget.events_per_pump(),
        .total_events = scheduler_budget.total_events(),
        .maximum_deadline = scheduler_budget.maximum_deadline().nanoseconds(),
      },
      trace_budget,
      trace_digest{},
      trace_digest{});
}

fault_rule make_rule(
  std::uint64_t id,
  builtin_fault_point point,
  std::optional<fault_object_key> object,
  std::uint64_t first,
  std::uint64_t last,
  fault_selector selector,
  fault_decision decision = fault_decision::make_error()) {
    const auto rule_id = fault_rule_id::make(id);
    const auto first_occurrence = fault_occurrence::make(first);
    const auto last_occurrence = fault_occurrence::make(last);
    BOOST_REQUIRE(rule_id.has_value());
    BOOST_REQUIRE(first_occurrence.has_value());
    BOOST_REQUIRE(last_occurrence.has_value());
    auto rule = fault_rule::make(
      *rule_id,
      point,
      object,
      *first_occurrence,
      *last_occurrence,
      selector,
      decision);
    BOOST_REQUIRE(rule.has_value());
    return *rule;
}

fault_request request(
  builtin_fault_point point,
  std::uint64_t occurrence,
  fault_object_key object = fault_object_key::none()) {
    const auto value = fault_occurrence::make(occurrence);
    BOOST_REQUIRE(value.has_value());
    return fault_request{
      .point = kwaque::runtime::descriptor_for(point)->id,
      .occurrence = *value,
      .object = object,
    };
}

struct fixture final {
    scheduler_limits scheduler_budget{test_scheduler_limits()};
    trace_limits trace_budget{test_trace_limits()};
    event_trace trace{
      test_header(scheduler_budget, trace_budget), trace_budget};
    scheduler events{scheduler_budget, &trace};

    std::unique_ptr<fault_schedule>
    schedule(seastar::chunked_vector<fault_rule> rules) {
        auto result = fault_schedule::make(
          events, trace, master_seed, std::move(rules));
        BOOST_REQUIRE(result.has_value());
        return std::move(*result);
    }
};

std::pair<std::uint64_t, std::uint64_t> rational_oracle(
  std::uint64_t rule_id, std::uint64_t occurrence, std::uint64_t denominator) {
    kwaque::simulation::deterministic_random random{master_seed};
    const auto coordinate = random_coordinate::make(
      random_domain::fault_decision, rule_id, occurrence);
    BOOST_REQUIRE(coordinate.has_value());
    std::uint64_t draw = 0;
    while (true) {
        const auto word = random.word_at(*coordinate, draw++);
        const auto product = static_cast<__uint128_t>(word) * denominator;
        const auto low = static_cast<std::uint64_t>(product);
        if (low >= -denominator % denominator) {
            return {
              static_cast<std::uint64_t>(product >> 64U),
              draw,
            };
        }
    }
}

kwaque::simulation::trace_entry evaluate_with_rule_noise(bool add_noise) {
    const auto ratio = kwaque::runtime::probability_ratio::make(3, 11);
    BOOST_REQUIRE(ratio.has_value());
    fixture environment;
    seastar::chunked_vector<fault_rule> rules;
    if (add_noise) {
        rules.push_back(make_rule(
          999,
          builtin_fault_point::network_write,
          std::nullopt,
          1,
          100,
          fault_selector::bounded_range()));
    }
    rules.push_back(make_rule(
      71,
      builtin_fault_point::file_read,
      fault_object_key::from_u64(8),
      1,
      100,
      fault_selector::rational(*ratio)));
    auto schedule = environment.schedule(std::move(rules));
    const auto outcome = schedule->evaluate(request(
      builtin_fault_point::file_read, 44, fault_object_key::from_u64(8)));
    BOOST_REQUIRE(outcome.has_value());
    BOOST_REQUIRE(environment.trace.entries().size() == 1U);
    return environment.trace.entries()[0];
}

seastar::chunked_vector<fault_rule> lifecycle_rules() {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(make_rule(
      101,
      builtin_fault_point::environment_start,
      std::nullopt,
      1,
      1,
      fault_selector::once(),
      fault_decision::make_delay(kwaque::runtime::monotonic_duration{7})));
    rules.push_back(make_rule(
      102,
      builtin_fault_point::resource_group_create,
      std::nullopt,
      1,
      1,
      fault_selector::once()));
    rules.push_back(make_rule(
      103,
      builtin_fault_point::queue_admission,
      std::nullopt,
      1,
      1,
      fault_selector::once(),
      fault_decision::make_delay(kwaque::runtime::monotonic_duration{11})));
    rules.push_back(make_rule(
      104,
      builtin_fault_point::environment_stop,
      std::nullopt,
      1,
      1,
      fault_selector::once()));
    return rules;
}

} // namespace

SEASTAR_TEST_CASE(fault_rule_validation_rejects_ambiguous_schedules) {
    BOOST_CHECK(!fault_rule_id::make(0).has_value());
    BOOST_CHECK(!fault_selector::every_n(0).has_value());
    BOOST_CHECK(!fault_schedule_limits::make(
                   kwaque::simulation::maximum_fault_schedule_rules + 1U)
                   .has_value());

    const auto id = fault_rule_id::make(1);
    const auto first = fault_occurrence::make(1);
    const auto second = fault_occurrence::make(2);
    BOOST_REQUIRE(id.has_value());
    BOOST_REQUIRE(first.has_value());
    BOOST_REQUIRE(second.has_value());
    BOOST_CHECK(!fault_rule::make(
                   *id,
                   builtin_fault_point::file_read,
                   std::nullopt,
                   *first,
                   *second,
                   fault_selector::once(),
                   fault_decision::make_error())
                   .has_value());
    BOOST_CHECK(!fault_rule::make(
                   *id,
                   builtin_fault_point::file_close,
                   std::nullopt,
                   *first,
                   *first,
                   fault_selector::once(),
                   fault_decision::make_error())
                   .has_value());
    BOOST_CHECK(
      fault_rule::make(
        *id,
        builtin_fault_point::file_truncate,
        std::nullopt,
        *first,
        *first,
        fault_selector::once(),
        fault_decision::make_partial_resize())
        .has_value());
    BOOST_CHECK(!fault_rule::make(
                   *id,
                   builtin_fault_point::file_write,
                   std::nullopt,
                   *first,
                   *first,
                   fault_selector::once(),
                   fault_decision::make_partial_resize())
                   .has_value());
    BOOST_CHECK(!fault_rule::make(
                   *id,
                   static_cast<builtin_fault_point>(255),
                   std::nullopt,
                   *first,
                   *first,
                   fault_selector::once(),
                   fault_decision::make_error())
                   .has_value());

    fixture environment;
    seastar::chunked_vector<fault_rule> duplicate_ids;
    duplicate_ids.push_back(make_rule(
      7,
      builtin_fault_point::file_read,
      std::nullopt,
      1,
      1,
      fault_selector::once()));
    duplicate_ids.push_back(make_rule(
      7,
      builtin_fault_point::file_write,
      std::nullopt,
      1,
      1,
      fault_selector::once()));
    const auto duplicate_schedule = fault_schedule::make(
      environment.events,
      environment.trace,
      master_seed,
      std::move(duplicate_ids));
    BOOST_CHECK(!duplicate_schedule.has_value());

    seastar::chunked_vector<fault_rule> overlapping;
    overlapping.push_back(make_rule(
      8,
      builtin_fault_point::file_read,
      std::nullopt,
      1,
      4,
      fault_selector::bounded_range()));
    overlapping.push_back(make_rule(
      9,
      builtin_fault_point::file_read,
      fault_object_key::from_u64(42),
      4,
      8,
      fault_selector::bounded_range()));
    const auto overlapping_schedule = fault_schedule::make(
      environment.events,
      environment.trace,
      master_seed,
      std::move(overlapping));
    BOOST_CHECK(!overlapping_schedule.has_value());

    seastar::chunked_vector<fault_rule> independent_objects;
    independent_objects.push_back(make_rule(
      10,
      builtin_fault_point::file_read,
      fault_object_key::from_u64(1),
      1,
      8,
      fault_selector::bounded_range()));
    independent_objects.push_back(make_rule(
      11,
      builtin_fault_point::file_read,
      fault_object_key::from_u64(2),
      1,
      8,
      fault_selector::bounded_range()));
    auto accepted = fault_schedule::make(
      environment.events,
      environment.trace,
      master_seed,
      std::move(independent_objects));
    BOOST_REQUIRE(accepted.has_value());
    co_return;
}

SEASTAR_TEST_CASE(fault_schedule_accepts_its_absolute_rule_limit) {
    const auto absolute = fault_schedule_limits::make(
      kwaque::simulation::maximum_fault_schedule_rules);
    BOOST_REQUIRE(absolute.has_value());
    fixture environment;
    seastar::chunked_vector<fault_rule> rules;
    rules.reserve(kwaque::simulation::maximum_fault_schedule_rules);
    for (std::uint64_t index = 1;
         index <= kwaque::simulation::maximum_fault_schedule_rules;
         ++index) {
        rules.push_back(make_rule(
          index,
          builtin_fault_point::file_read,
          fault_object_key::from_u64(index),
          1,
          1,
          fault_selector::once()));
    }
    auto schedule = fault_schedule::make(
      environment.events,
      environment.trace,
      master_seed,
      std::move(rules),
      *absolute);
    BOOST_REQUIRE(schedule.has_value());
    BOOST_CHECK(
      (*schedule)->rules().size()
      == kwaque::simulation::maximum_fault_schedule_rules);
    co_return;
}

SEASTAR_TEST_CASE(
  lifecycle_and_admission_fault_points_capture_and_replay_exactly) {
    const std::array points{
      builtin_fault_point::environment_start,
      builtin_fault_point::resource_group_create,
      builtin_fault_point::queue_admission,
      builtin_fault_point::environment_stop,
    };
    const std::array actions{
      fault_action::delay,
      fault_action::error,
      fault_action::delay,
      fault_action::error,
    };
    const std::array delay_nanoseconds{7U, 0U, 11U, 0U};
    const std::array rule_ids{101U, 102U, 103U, 104U};

    fixture captured;
    auto capture_schedule = captured.schedule(lifecycle_rules());
    for (std::size_t index = 0; index < points.size(); ++index) {
        const auto outcome = capture_schedule->evaluate(
          request(points[index], 1));
        BOOST_REQUIRE(outcome.has_value());
        BOOST_CHECK(outcome->action() == actions[index]);
        if (actions[index] == fault_action::delay) {
            BOOST_REQUIRE(outcome->delay().has_value());
            BOOST_CHECK(
              outcome->delay()->nanoseconds() == delay_nanoseconds[index]);
        } else {
            BOOST_CHECK(!outcome->delay().has_value());
        }
    }
    BOOST_REQUIRE(captured.trace.entries().size() == points.size());
    for (std::size_t index = 0; index < points.size(); ++index) {
        const auto& entry = captured.trace.entries()[index];
        BOOST_CHECK(entry.action == trace_action::fault_evaluated);
        BOOST_CHECK(entry.kind == trace_event_kind::fault);
        BOOST_CHECK(
          entry.domain
          == kwaque::runtime::descriptor_for(points[index])->id.value());
        BOOST_CHECK(entry.stable_id == rule_ids[index]);
        BOOST_CHECK(entry.coordinate_a == 1U);
        BOOST_CHECK(
          (entry.result & UINT32_C(0xff))
          == static_cast<std::uint8_t>(actions[index]));
    }

    auto encoded = captured.trace.encode();
    BOOST_REQUIRE(encoded.has_value());
    auto decoded = event_trace::decode(*encoded, captured.trace_budget);
    BOOST_REQUIRE(decoded.has_value());
    BOOST_CHECK(
      decoded->header.schema_version
      == kwaque::simulation::event_trace_schema_version);
    auto replay_trace = event_trace::replay(
      captured.trace.header(), captured.trace_budget, std::move(*decoded));
    BOOST_REQUIRE(replay_trace.has_value());
    scheduler replay_events{captured.scheduler_budget, replay_trace->get()};
    auto replay_schedule = fault_schedule::make(
      replay_events, **replay_trace, master_seed, lifecycle_rules());
    BOOST_REQUIRE(replay_schedule.has_value());
    for (std::size_t index = 0; index < points.size(); ++index) {
        const auto outcome
          = (*replay_schedule)->evaluate(request(points[index], 1));
        BOOST_REQUIRE(outcome.has_value());
        BOOST_CHECK(outcome->action() == actions[index]);
        if (actions[index] == fault_action::delay) {
            BOOST_REQUIRE(outcome->delay().has_value());
            BOOST_CHECK(
              outcome->delay()->nanoseconds() == delay_nanoseconds[index]);
        }
    }
    BOOST_CHECK((*replay_trace)->finish_replay().has_value());

    const auto rule_id = fault_rule_id::make(201);
    const auto occurrence = fault_occurrence::make(1);
    BOOST_REQUIRE(rule_id.has_value());
    BOOST_REQUIRE(occurrence.has_value());
    BOOST_CHECK(
      !fault_rule::make(
         *rule_id,
         builtin_fault_point::resource_group_create,
         std::nullopt,
         *occurrence,
         *occurrence,
         fault_selector::once(),
         fault_decision::make_delay(kwaque::runtime::monotonic_duration{1}))
         .has_value());
    BOOST_CHECK(!fault_rule::make(
                   *rule_id,
                   builtin_fault_point::environment_start,
                   std::nullopt,
                   *occurrence,
                   *occurrence,
                   fault_selector::once(),
                   fault_decision::make_crash())
                   .has_value());
    co_return;
}

SEASTAR_TEST_CASE(fault_selectors_have_exact_window_and_draw_semantics) {
    const auto ratio = kwaque::runtime::probability_ratio::make(2, 5);
    const auto zero = kwaque::runtime::probability_ratio::make(0, 7);
    const auto full = kwaque::runtime::probability_ratio::make(7, 7);
    const auto period = fault_selector::every_n(3);
    BOOST_REQUIRE(ratio.has_value());
    BOOST_REQUIRE(zero.has_value());
    BOOST_REQUIRE(full.has_value());
    BOOST_REQUIRE(period.has_value());

    fixture environment;
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(make_rule(
      1,
      builtin_fault_point::file_open,
      std::nullopt,
      3,
      3,
      fault_selector::once()));
    rules.push_back(make_rule(
      2,
      builtin_fault_point::file_exists,
      std::nullopt,
      5,
      7,
      fault_selector::bounded_range()));
    rules.push_back(make_rule(
      3, builtin_fault_point::file_stat, std::nullopt, 10, 19, *period));
    rules.push_back(make_rule(
      4,
      builtin_fault_point::file_list,
      std::nullopt,
      1,
      100,
      fault_selector::rational(*ratio)));
    rules.push_back(make_rule(
      5,
      builtin_fault_point::file_remove,
      std::nullopt,
      1,
      100,
      fault_selector::rational(*zero)));
    rules.push_back(make_rule(
      6,
      builtin_fault_point::directory_remove,
      std::nullopt,
      1,
      100,
      fault_selector::rational(*full)));
    rules.push_back(make_rule(
      7,
      builtin_fault_point::file_size,
      std::nullopt,
      std::numeric_limits<std::uint64_t>::max(),
      std::numeric_limits<std::uint64_t>::max(),
      *period));
    auto schedule = environment.schedule(std::move(rules));

    BOOST_CHECK(
      schedule->evaluate(request(builtin_fault_point::file_open, 2))->action()
      == fault_action::none);
    BOOST_CHECK(
      schedule->evaluate(request(builtin_fault_point::file_open, 3))->action()
      == fault_action::error);
    BOOST_CHECK(
      schedule->evaluate(request(builtin_fault_point::file_exists, 6))->action()
      == fault_action::error);
    for (std::uint64_t occurrence = 10; occurrence <= 19; ++occurrence) {
        const auto evaluated = schedule->evaluate(
          request(builtin_fault_point::file_stat, occurrence));
        BOOST_REQUIRE(evaluated.has_value());
        BOOST_CHECK(
          (evaluated->action() == fault_action::error)
          == ((occurrence - 10U) % 3U == 0));
    }

    constexpr std::uint64_t rational_occurrence{17};
    const auto [sample, draws] = rational_oracle(
      4, rational_occurrence, ratio->denominator());
    const auto rational = schedule->evaluate(
      request(builtin_fault_point::file_list, rational_occurrence));
    BOOST_REQUIRE(rational.has_value());
    BOOST_CHECK(
      (rational->action() == fault_action::error)
      == (sample < ratio->numerator()));

    const auto zero_result = schedule->evaluate(
      request(builtin_fault_point::file_remove, 9));
    const auto full_result = schedule->evaluate(
      request(builtin_fault_point::directory_remove, 9));
    BOOST_REQUIRE(zero_result.has_value());
    BOOST_REQUIRE(full_result.has_value());
    BOOST_CHECK(zero_result->action() == fault_action::none);
    BOOST_CHECK(full_result->action() == fault_action::error);
    const auto maximum_occurrence = schedule->evaluate(request(
      builtin_fault_point::file_size,
      std::numeric_limits<std::uint64_t>::max()));
    BOOST_REQUIRE(maximum_occurrence.has_value());
    BOOST_CHECK(maximum_occurrence->action() == fault_action::error);

    const auto& entries = environment.trace.entries();
    const auto rational_entry = std::ranges::find_if(
      entries, [](const auto& entry) { return entry.stable_id == 4; });
    BOOST_REQUIRE(rational_entry != entries.end());
    BOOST_CHECK(rational_entry->coordinate_b == draws);
    BOOST_CHECK(rational_entry->value == sample);
    const auto zero_entry = std::ranges::find_if(
      entries, [](const auto& entry) { return entry.stable_id == 5; });
    const auto full_entry = std::ranges::find_if(
      entries, [](const auto& entry) { return entry.stable_id == 6; });
    BOOST_REQUIRE(zero_entry != entries.end());
    BOOST_REQUIRE(full_entry != entries.end());
    BOOST_CHECK(zero_entry->coordinate_b == 0U);
    BOOST_CHECK(zero_entry->value == fault_trace_no_sample);
    BOOST_CHECK(
      (zero_entry->result & UINT32_C(0xff))
      == static_cast<std::uint8_t>(fault_action::error));
    BOOST_CHECK((zero_entry->result >> 8U) == 2U);
    BOOST_CHECK(full_entry->coordinate_b == 0U);
    BOOST_CHECK(full_entry->value == fault_trace_no_sample);
    BOOST_CHECK(
      (full_entry->result & UINT32_C(0xff))
      == static_cast<std::uint8_t>(fault_action::error));
    BOOST_CHECK((full_entry->result >> 8U) == 1U);
    const auto maximum_entry = std::ranges::find_if(
      entries, [](const auto& entry) { return entry.stable_id == 7; });
    BOOST_REQUIRE(maximum_entry != entries.end());
    BOOST_CHECK(
      maximum_entry->coordinate_a == std::numeric_limits<std::uint64_t>::max());
    co_return;
}

SEASTAR_TEST_CASE(fault_preparation_rolls_back_and_commits_exactly_once) {
    fixture environment;
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(make_rule(
      41,
      builtin_fault_point::file_write,
      fault_object_key::from_u64(9),
      1,
      1,
      fault_selector::once(),
      fault_decision::make_crash()));
    auto schedule = environment.schedule(std::move(rules));
    const auto selected = request(
      builtin_fault_point::file_write, 1, fault_object_key::from_u64(9));

    {
        auto prepared = schedule->prepare(selected);
        BOOST_REQUIRE(prepared.has_value());
        BOOST_CHECK(prepared->matched());
        BOOST_CHECK(prepared->applied());
        BOOST_CHECK(prepared->preview().action() == fault_action::crash);
    }
    BOOST_CHECK_EQUAL(schedule->evaluations(), 1U);
    BOOST_CHECK_EQUAL(schedule->applied_decisions(), 0U);
    BOOST_CHECK(environment.trace.entries().empty());
    {
        auto full_capacity = environment.trace.reserve(
          environment.trace_budget.entries(),
          static_cast<std::uint64_t>(environment.trace_budget.entries())
            * kwaque::simulation::canonical_entry_encoded_size);
        BOOST_REQUIRE(full_capacity.has_value());
    }

    auto prepared = schedule->prepare(selected);
    BOOST_REQUIRE(prepared.has_value());
    BOOST_CHECK_EQUAL(schedule->evaluations(), 2U);
    BOOST_CHECK_EQUAL(schedule->applied_decisions(), 0U);
    auto moved = std::move(*prepared);
    // The moved-from token must be inert and reject a second ownership action.
    // NOLINTNEXTLINE(bugprone-use-after-move)
    BOOST_CHECK(!prepared->commit().has_value());
    const auto committed = moved.commit();
    BOOST_REQUIRE(committed.has_value());
    BOOST_CHECK(committed->action() == fault_action::crash);
    BOOST_CHECK_EQUAL(schedule->applied_decisions(), 1U);
    BOOST_CHECK(!moved.commit().has_value());
    BOOST_CHECK_EQUAL(schedule->applied_decisions(), 1U);
    BOOST_CHECK(environment.trace.entries().size() == 1U);
    const auto& entry = environment.trace.entries()[0];
    BOOST_CHECK(entry.action == trace_action::fault_evaluated);
    BOOST_CHECK(entry.kind == trace_event_kind::fault);
    BOOST_CHECK(entry.domain == selected.point.value());
    BOOST_CHECK(entry.stable_id == 41U);
    BOOST_CHECK(entry.coordinate_a == 1U);
    BOOST_CHECK(
      (entry.result & UINT32_C(0xff))
      == static_cast<std::uint8_t>(fault_action::crash));
    BOOST_CHECK((entry.result >> 8U) == 1U);

    fixture direct_environment;
    seastar::chunked_vector<fault_rule> direct_rules;
    direct_rules.push_back(make_rule(
      41,
      builtin_fault_point::file_write,
      fault_object_key::from_u64(9),
      1,
      1,
      fault_selector::once(),
      fault_decision::make_crash()));
    auto direct_schedule = direct_environment.schedule(std::move(direct_rules));
    const auto direct = direct_schedule->evaluate(selected);
    BOOST_REQUIRE(direct.has_value());
    BOOST_CHECK(*direct == *committed);
    BOOST_REQUIRE(direct_environment.trace.entries().size() == 1U);
    BOOST_CHECK(direct_environment.trace.entries()[0] == entry);
    co_return;
}

SEASTAR_TEST_CASE(fault_coordinates_ignore_unrelated_rule_and_stream_noise) {
    kwaque::simulation::deterministic_random unrelated{master_seed};
    auto stream = unrelated.stream(random_domain::runtime_stream, 123, 456);
    BOOST_REQUIRE(stream.has_value());
    for (std::size_t index = 0; index < 1'000; ++index) {
        static_cast<void>(stream->next_u64());
    }
    const auto without_noise = evaluate_with_rule_noise(false);
    const auto with_noise = evaluate_with_rule_noise(true);
    BOOST_CHECK(without_noise == with_noise);
    co_return;
}

SEASTAR_TEST_CASE(fault_evaluation_allocates_nothing_after_construction) {
    fixture environment;
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(make_rule(
      91,
      builtin_fault_point::file_read,
      fault_object_key::from_u64(1),
      1,
      8,
      fault_selector::bounded_range()));
    auto schedule = environment.schedule(std::move(rules));

    const auto matched_request = request(
      builtin_fault_point::file_read, 3, fault_object_key::from_u64(1));
    const auto before_match = seastar::memory::stats().mallocs();
    const auto matched = schedule->evaluate(matched_request);
    const auto after_match = seastar::memory::stats().mallocs();
    BOOST_REQUIRE(matched.has_value());
    BOOST_CHECK(matched->action() == fault_action::error);
    BOOST_CHECK(after_match == before_match);

    const auto before_miss = seastar::memory::stats().mallocs();
    const auto missed = schedule->evaluate(request(
      builtin_fault_point::file_read, 3, fault_object_key::from_u64(2)));
    const auto after_miss = seastar::memory::stats().mallocs();
    BOOST_REQUIRE(missed.has_value());
    BOOST_CHECK(missed->action() == fault_action::none);
    BOOST_CHECK(after_miss == before_miss);
    co_return;
}

SEASTAR_TEST_CASE(fault_trace_crosses_storage_chunks_without_allocating) {
    constexpr std::uint32_t first_chunk_entries
      = kwaque::maximum_contiguous_allocation_bytes
        / sizeof(kwaque::simulation::trace_entry);
    const auto scheduler_budget = test_scheduler_limits();
    const auto trace_budget = test_trace_limits(first_chunk_entries + 1U);
    event_trace trace{
      test_header(scheduler_budget, trace_budget), trace_budget};
    scheduler events{scheduler_budget, &trace};
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(make_rule(
      92,
      builtin_fault_point::file_read,
      std::nullopt,
      1,
      first_chunk_entries + 1U,
      fault_selector::bounded_range()));
    auto schedule = fault_schedule::make(
      events, trace, master_seed, std::move(rules));
    BOOST_REQUIRE(schedule.has_value());
    for (std::uint64_t occurrence = 1; occurrence <= first_chunk_entries;
         ++occurrence) {
        BOOST_REQUIRE(
          (*schedule)
            ->evaluate(request(builtin_fault_point::file_read, occurrence))
            .has_value());
    }
    const auto before = seastar::memory::stats().mallocs();
    const auto boundary = (*schedule)->evaluate(
      request(builtin_fault_point::file_read, first_chunk_entries + 1U));
    const auto after = seastar::memory::stats().mallocs();
    BOOST_REQUIRE(boundary.has_value());
    BOOST_CHECK(after == before);
    BOOST_CHECK(trace.entries().size() == first_chunk_entries + 1U);
    co_return;
}

SEASTAR_TEST_CASE(fault_replay_compares_before_returning_the_decision) {
    fixture captured;
    seastar::chunked_vector<fault_rule> capture_rules;
    capture_rules.push_back(make_rule(
      77,
      builtin_fault_point::file_flush,
      std::nullopt,
      1,
      1,
      fault_selector::once()));
    auto capture_schedule = captured.schedule(std::move(capture_rules));
    BOOST_REQUIRE(
      capture_schedule->evaluate(request(builtin_fault_point::file_flush, 1))
        .has_value());
    auto encoded = captured.trace.encode();
    BOOST_REQUIRE(encoded.has_value());
    auto decoded = event_trace::decode(*encoded, captured.trace_budget);
    BOOST_REQUIRE(decoded.has_value());
    ++decoded->entries[0].stable_id;

    auto replay_trace = event_trace::replay(
      test_header(captured.scheduler_budget, captured.trace_budget),
      captured.trace_budget,
      std::move(*decoded));
    BOOST_REQUIRE(replay_trace.has_value());
    scheduler replay_events{captured.scheduler_budget, replay_trace->get()};
    seastar::chunked_vector<fault_rule> replay_rules;
    replay_rules.push_back(make_rule(
      77,
      builtin_fault_point::file_flush,
      std::nullopt,
      1,
      1,
      fault_selector::once()));
    auto replay_schedule = fault_schedule::make(
      replay_events, **replay_trace, master_seed, std::move(replay_rules));
    BOOST_REQUIRE(replay_schedule.has_value());
    const auto outcome = (*replay_schedule)
                           ->evaluate(
                             request(builtin_fault_point::file_flush, 1));
    BOOST_REQUIRE(!outcome.has_value());
    BOOST_CHECK(outcome.error().code() == kwaque::errc::replay_divergence);
    BOOST_CHECK((*replay_trace)->failed());
    co_return;
}
