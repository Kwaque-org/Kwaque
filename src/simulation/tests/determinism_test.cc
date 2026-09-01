#include "src/simulation/deterministic_random.h"
#include "src/simulation/event_trace.h"
#include "src/simulation/scheduler.h"
#include "src/simulation/sha256.h"
#include "src/simulation/virtual_time.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace {

using kwaque::simulation::deterministic_random;
using kwaque::simulation::event_priority;
using kwaque::simulation::event_trace;
using kwaque::simulation::random_coordinate;
using kwaque::simulation::random_domain;
using kwaque::simulation::scheduler;
using kwaque::simulation::scheduler_limit_values;
using kwaque::simulation::scheduler_limits;
using kwaque::simulation::sha256_digest;
using kwaque::simulation::sha256_hasher;
using kwaque::simulation::trace_action;
using kwaque::simulation::trace_artifact;
using kwaque::simulation::trace_digest;
using kwaque::simulation::trace_entry;
using kwaque::simulation::trace_event_descriptor;
using kwaque::simulation::trace_event_kind;
using kwaque::simulation::trace_header;
using kwaque::simulation::trace_limit_values;
using kwaque::simulation::trace_limits;
using kwaque::simulation::virtual_time;
using kwaque::simulation::virtual_time_config;
using kwaque::simulation::wall_offset;

scheduler_limits scenario_scheduler_limits() {
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

trace_limits scenario_trace_limits() {
    auto limits = trace_limits::make(
      trace_limit_values{
        .entries = 256,
        .encoded_bytes = 65'536,
        .line_bytes = 1'024,
      });
    BOOST_REQUIRE(limits.has_value());
    return *limits;
}

trace_digest digest(std::uint8_t base) {
    trace_digest result{};
    for (std::size_t index = 0; index < result.size(); ++index) {
        result[index] = static_cast<std::uint8_t>(base + index);
    }
    return result;
}

trace_header scenario_header(
  std::uint64_t seed,
  const scheduler_limits& scheduler_budget,
  trace_limits trace_budget) {
    return trace_header::current(
      seed,
      kwaque::simulation::deterministic_random_algorithm_version,
      kwaque::simulation::deterministic_random_coordinate_version,
      kwaque::simulation::trace_budget(scheduler_budget),
      trace_budget,
      digest(1),
      digest(101));
}

kwaque::runtime::result<void> run_scenario(
  event_trace& trace,
  const scheduler_limits& limits,
  std::uint64_t seed,
  std::uint64_t stable_id,
  std::uint64_t& state,
  std::int64_t* wall_offset_after_run = nullptr) {
    scheduler target{limits, &trace};
    const auto time_config = virtual_time_config::make(limits);
    if (!time_config) {
        return kwaque::runtime::failure(time_config.error());
    }
    virtual_time time{target, *time_config};
    kwaque::simulation::clock_binding binding{time};
    const deterministic_random random{seed};
    const auto coordinate = random_coordinate::make(
      random_domain::fault_decision, stable_id, 7);
    if (!coordinate) {
        return kwaque::runtime::failure(coordinate.error());
    }
    const auto decision = random.recorded_word_at(
      trace, target.now(), *coordinate, 0);
    if (!decision) {
        return kwaque::runtime::failure(decision.error());
    }
    state = *decision;

    auto wall = time.schedule_wall_offset(
      kwaque::runtime::monotonic_time{5}, wall_offset{20});
    if (!wall) {
        return kwaque::runtime::failure(wall.error());
    }
    auto applied = target.schedule(
      kwaque::runtime::monotonic_time{3},
      event_priority::normal(),
      [&state] noexcept { state ^= UINT64_C(0x55aa); },
      trace_event_descriptor{
        .kind = trace_event_kind::generic,
        .stable_id = stable_id,
        .value = UINT64_C(0x55aa),
      });
    if (!applied) {
        return kwaque::runtime::failure(applied.error());
    }
    auto dropped = target.schedule(
      kwaque::runtime::monotonic_time{4},
      event_priority::normal(),
      [&state] noexcept { state = 0; },
      trace_event_descriptor{
        .kind = trace_event_kind::generic,
        .stable_id = stable_id + 1,
      });
    if (!dropped) {
        return kwaque::runtime::failure(dropped.error());
    }
    auto canceled = target.cancel(*dropped);
    if (!canceled) {
        return kwaque::runtime::failure(canceled.error());
    }
    if (!*canceled) {
        return kwaque::runtime::failure(
          kwaque::runtime::operation_error{
            kwaque::errc::invariant_violation,
            kwaque::runtime::operation_kind::scheduler});
    }
    auto completed = target.run_until(kwaque::runtime::monotonic_time{10});
    if (wall_offset_after_run != nullptr) {
        *wall_offset_after_run = time.offset().nanoseconds();
    }
    if (!completed) {
        return kwaque::runtime::failure(completed.error());
    }
    return {};
}

std::optional<std::uint64_t> context_value(
  const kwaque::runtime::operation_error& error,
  kwaque::runtime::operation_context_key key) {
    for (std::size_t index = 0; index < error.context_size(); ++index) {
        const auto field = error.context_at(index);
        if (field && field->key == key) {
            return field->value;
        }
    }
    return std::nullopt;
}

sha256_digest sha256(const trace_artifact& artifact) {
    sha256_hasher hasher;
    for (const auto& chunk : artifact.chunks()) {
        hasher.update(chunk.data(), chunk.size());
    }
    return std::move(hasher).final();
}

} // namespace

SEASTAR_TEST_CASE(deterministic_scenario_is_byte_identical_under_noise) {
    const auto scheduler_budget = scenario_scheduler_limits();
    const auto trace_budget = scenario_trace_limits();
    const auto header = scenario_header(92, scheduler_budget, trace_budget);
    trace_artifact expected_encoding;
    sha256_digest expected_hash{};
    std::uint64_t expected_state = 0;

    for (std::size_t repetition = 0; repetition < 100; ++repetition) {
        std::vector<std::uint64_t> noise(repetition * 17 + 1, repetition);
        BOOST_TEST(noise.size() == repetition * 17 + 1);
        std::array<std::uint64_t, 4> caller_ids{41, 51, 61, 71};
        std::rotate(
          caller_ids.begin(),
          caller_ids.begin() + static_cast<std::ptrdiff_t>(repetition % 4),
          caller_ids.end());
        if ((repetition & 1U) != 0) {
            std::ranges::reverse(caller_ids);
        }
        std::vector<std::unique_ptr<std::uint64_t>> callers;
        callers.reserve(caller_ids.size());
        for (const auto id : caller_ids) {
            callers.push_back(std::make_unique<std::uint64_t>(id));
        }
        const auto caller = std::ranges::find_if(
          callers, [](const auto& id) { return *id == 41; });
        BOOST_REQUIRE(caller != callers.end());
        event_trace trace{header, trace_budget};
        std::uint64_t state = 0;
        const auto outcome = run_scenario(
          trace, scheduler_budget, 92, **caller, state);
        BOOST_REQUIRE(outcome.has_value());
        auto encoded = trace.encode();
        BOOST_REQUIRE(encoded.has_value());
        const auto hash = sha256(*encoded);
        if (repetition == 0) {
            expected_encoding = std::move(*encoded);
            expected_hash = hash;
            expected_state = state;
        } else {
            BOOST_CHECK(*encoded == expected_encoding);
            BOOST_TEST(hash == expected_hash, boost::test_tools::per_element());
            BOOST_TEST(state == expected_state);
        }
    }

    event_trace changed{header, trace_budget};
    std::uint64_t changed_state = 0;
    BOOST_REQUIRE(run_scenario(changed, scheduler_budget, 92, 42, changed_state)
                    .has_value());
    const auto changed_encoding = changed.encode();
    BOOST_REQUIRE(changed_encoding.has_value());
    BOOST_CHECK(*changed_encoding != expected_encoding);
    const auto expected_decoded = event_trace::decode(
      expected_encoding, trace_budget);
    const auto changed_decoded = event_trace::decode(
      *changed_encoding, trace_budget);
    BOOST_REQUIRE(expected_decoded.has_value());
    BOOST_REQUIRE(changed_decoded.has_value());
    std::size_t first_difference = 0;
    while (first_difference < expected_decoded->entries.size()
           && expected_decoded->entries[first_difference]
                == changed_decoded->entries[first_difference]) {
        ++first_difference;
    }
    BOOST_REQUIRE(first_difference < expected_decoded->entries.size());
    BOOST_TEST(first_difference == 0U);
    BOOST_CHECK(
      expected_decoded->entries[first_difference].action
      == trace_action::keyed_decision);
    co_return;
}

SEASTAR_TEST_CASE(trace_codec_yields_between_bounded_entry_batches) {
    constexpr std::uint32_t entry_count{10'001};
    const auto scheduler_budget = scenario_scheduler_limits();
    const auto trace_budget = trace_limits::make(
      trace_limit_values{
        .entries = entry_count,
        .encoded_bytes = kwaque::simulation::canonical_header_encoded_size
                         + static_cast<std::uint64_t>(entry_count)
                             * kwaque::simulation::canonical_entry_encoded_size,
        .line_bytes = 1'024,
      });
    BOOST_REQUIRE(trace_budget.has_value());
    event_trace captured{
      scenario_header(92, scheduler_budget, *trace_budget), *trace_budget};
    for (std::uint64_t value = 0; value < entry_count; ++value) {
        BOOST_REQUIRE(captured
                        .observe(
                          trace_entry{
                            .action = trace_action::keyed_decision,
                            .kind = trace_event_kind::keyed_random,
                            .domain = static_cast<std::uint32_t>(
                              random_domain::fault_decision),
                            .stable_id = 41,
                            .coordinate_a = value,
                            .value = value,
                          })
                        .has_value());
    }

    auto encoded = co_await captured.encode_cooperatively(17);
    BOOST_REQUIRE(encoded.has_value());
    BOOST_CHECK_GT(encoded->chunks().size(), 1U);
    auto decoded = co_await event_trace::decode_cooperatively(
      std::move(*encoded), *trace_budget, 19);
    BOOST_REQUIRE(decoded.has_value());
    BOOST_CHECK_EQUAL(decoded->entries.size(), entry_count);
    BOOST_CHECK(std::ranges::equal(decoded->entries, captured.entries()));
}

SEASTAR_TEST_CASE(scheduler_replay_compares_before_the_first_side_effect) {
    const auto scheduler_budget = scenario_scheduler_limits();
    const auto trace_budget = scenario_trace_limits();
    const auto header = scenario_header(92, scheduler_budget, trace_budget);
    event_trace captured{header, trace_budget};
    std::uint64_t captured_state = 0;
    BOOST_REQUIRE(
      run_scenario(captured, scheduler_budget, 92, 41, captured_state)
        .has_value());
    const auto encoded = captured.encode();
    BOOST_REQUIRE(encoded.has_value());

    auto decoded = event_trace::decode(*encoded, trace_budget);
    BOOST_REQUIRE(decoded.has_value());
    auto replay = event_trace::replay(
      header, trace_budget, std::move(*decoded));
    BOOST_REQUIRE(replay.has_value());
    std::uint64_t replayed_state = 0;
    BOOST_REQUIRE(
      run_scenario(**replay, scheduler_budget, 92, 41, replayed_state)
        .has_value());
    BOOST_REQUIRE((*replay)->finish_replay().has_value());
    BOOST_TEST(replayed_state == captured_state);

    auto divergent_decoded = event_trace::decode(*encoded, trace_budget);
    BOOST_REQUIRE(divergent_decoded.has_value());
    BOOST_REQUIRE(!divergent_decoded->entries.empty());
    BOOST_CHECK(
      divergent_decoded->entries.front().action
      == trace_action::keyed_decision);
    ++divergent_decoded->entries.front().value;
    auto divergent = event_trace::replay(
      header, trace_budget, std::move(*divergent_decoded));
    BOOST_REQUIRE(divergent.has_value());
    std::uint64_t untouched_state = 0;
    const auto divergence = run_scenario(
      **divergent, scheduler_budget, 92, 41, untouched_state);
    BOOST_REQUIRE(!divergence.has_value());
    BOOST_CHECK(divergence.error().code() == kwaque::errc::replay_divergence);
    BOOST_TEST(untouched_state == 0U);
    co_return;
}

SEASTAR_TEST_CASE(
  replay_rejects_a_changed_canceled_deadline_at_the_schedule_entry) {
    const auto scheduler_budget = scenario_scheduler_limits();
    const auto trace_budget = scenario_trace_limits();
    const auto header = scenario_header(92, scheduler_budget, trace_budget);
    event_trace captured{header, trace_budget};
    {
        scheduler target{scheduler_budget, &captured};
        const auto event = target.schedule(
          kwaque::runtime::monotonic_time{10},
          event_priority::normal(),
          [] noexcept {},
          trace_event_descriptor{
            .kind = trace_event_kind::generic,
            .stable_id = 77,
          });
        BOOST_REQUIRE(event.has_value());
        const auto canceled = target.cancel(*event);
        BOOST_REQUIRE(canceled.has_value());
        BOOST_REQUIRE(*canceled);
    }
    const auto encoded = captured.encode();
    BOOST_REQUIRE(encoded.has_value());
    auto decoded = event_trace::decode(*encoded, trace_budget);
    BOOST_REQUIRE(decoded.has_value());
    auto replay = event_trace::replay(
      header, trace_budget, std::move(*decoded));
    BOOST_REQUIRE(replay.has_value());
    {
        scheduler target{scheduler_budget, replay->get()};
        scheduler::callback retained{[] noexcept {}};
        const auto changed = target.schedule(
          kwaque::runtime::monotonic_time{20},
          event_priority::normal(),
          std::move(retained),
          trace_event_descriptor{
            .kind = trace_event_kind::generic,
            .stable_id = 77,
          });
        BOOST_REQUIRE(!changed.has_value());
        BOOST_CHECK(changed.error().code() == kwaque::errc::replay_divergence);
        // Replay rejected the schedule before callback ownership moved.
        // NOLINTNEXTLINE(bugprone-use-after-move)
        BOOST_CHECK(static_cast<bool>(retained));
        BOOST_CHECK(target.pending_events() == 0U);
    }
    co_return;
}

SEASTAR_TEST_CASE(
  replay_divergence_is_reported_before_every_state_change_and_drains_cleanly) {
    const auto scheduler_budget = scenario_scheduler_limits();
    const auto trace_budget = scenario_trace_limits();
    const auto header = scenario_header(92, scheduler_budget, trace_budget);
    event_trace captured{header, trace_budget};
    std::uint64_t captured_state = 0;
    BOOST_REQUIRE(
      run_scenario(captured, scheduler_budget, 92, 41, captured_state)
        .has_value());
    const auto encoded = captured.encode();
    BOOST_REQUIRE(encoded.has_value());

    const deterministic_random random{92};
    const auto coordinate = random_coordinate::make(
      random_domain::fault_decision, 41, 7);
    BOOST_REQUIRE(coordinate.has_value());
    const auto state_before_callbacks = random.word_at(*coordinate, 0);
    const std::array actions{
      trace_action::keyed_decision,
      trace_action::scheduled,
      trace_action::canceled,
      trace_action::time_advanced,
      trace_action::selected,
      trace_action::wall_adjusted,
    };

    for (const auto action : actions) {
        auto decoded = event_trace::decode(*encoded, trace_budget);
        BOOST_REQUIRE(decoded.has_value());
        const auto found = std::ranges::find_if(
          decoded->entries,
          [action](const auto& entry) { return entry.action == action; });
        BOOST_REQUIRE(found != decoded->entries.end());
        const auto expected_sequence = found->sequence;
        ++found->value;
        auto replay = event_trace::replay(
          header, trace_budget, std::move(*decoded));
        BOOST_REQUIRE(replay.has_value());

        std::uint64_t state = 0;
        std::int64_t observed_wall_offset = -1;
        const auto outcome = run_scenario(
          **replay, scheduler_budget, 92, 41, state, &observed_wall_offset);
        BOOST_REQUIRE(!outcome.has_value());
        BOOST_CHECK(outcome.error().code() == kwaque::errc::replay_divergence);
        const auto sequence = context_value(
          outcome.error(), kwaque::runtime::operation_context_key::sequence);
        BOOST_REQUIRE(sequence.has_value());
        BOOST_TEST(*sequence == expected_sequence);

        if (action == trace_action::keyed_decision) {
            BOOST_TEST(state == 0U);
        } else if (action == trace_action::wall_adjusted) {
            BOOST_TEST(state == (state_before_callbacks ^ UINT64_C(0x55aa)));
            BOOST_TEST(observed_wall_offset == 0);
        } else {
            BOOST_TEST(state == state_before_callbacks);
        }
    }
    co_return;
}
