#include "src/observability/event.h"
#include "src/runtime/cross_shard.h"
#include "src/runtime/random.h"
#include "src/runtime/shard_affinity.h"
#include "src/runtime/time.h"
#include "src/runtime/waiting.h"
#include "src/simulation/deterministic_random.h"
#include "src/simulation/environment.h"
#include "src/simulation/scheduler_driver.h"
#include "src/simulation/virtual_time.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/when_all.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

// A value a call may carry between shards.
struct shard_sum final {
    std::uint64_t value;
};

} // namespace

template<>
struct kwaque::runtime::enable_cross_shard_value<shard_sum> : std::true_type {};

namespace {

namespace runtime = kwaque::runtime;
namespace simulation = kwaque::simulation;
using runtime::monotonic_time;

constexpr std::uint64_t millisecond = 1'000'000;

std::unique_ptr<simulation::environment> make(
  std::uint16_t shards,
  std::uint64_t seed = 7,
  bool reorder = false,
  std::uint32_t units = 1'024,
  std::uint64_t incarnation = 0) {
    simulation::environment_config_values values;
    values.master_seed = seed;
    values.identity_incarnation = incarnation;
    values.virtual_shards = shards;
    values.cross_shard_calls.reorder = reorder;
    values.cross_shard_calls.units_per_pair = units;
    auto config = simulation::environment_config::make(std::move(values));
    BOOST_REQUIRE(config.has_value());
    auto made = simulation::environment::make(std::move(*config));
    BOOST_REQUIRE(made.has_value());
    return std::move(*made);
}

template<typename T>
seastar::future<T>
drive(simulation::environment& target, seastar::future<T> waiting) {
    co_await simulation::testing::pump_until(target.event_scheduler(), waiting);
    co_return co_await std::move(waiting);
}

seastar::future<> start(simulation::environment& target) {
    co_await drive(target, target.start());
}

seastar::future<> stop(simulation::environment& target) {
    co_await drive(target, target.stop());
}

unsigned virtual_index() {
    return runtime::current_shard() - seastar::this_shard_id();
}

// Suspends before reading its argument, so the call must keep the argument
// alive until the target's future resolves.
seastar::future<shard_sum> add_index_after_yield(const shard_sum& value) {
    co_await seastar::yield();
    co_return shard_sum{value.value + virtual_index()};
}

kwaque::observability::event_request refusal(monotonic_time at) {
    namespace observability = kwaque::observability;
    auto text = observability::event_text::make(
      observability::event_public_text::outcome_no_space);
    BOOST_REQUIRE(text.has_value());
    const std::array fields{observability::event_field{
      .key = observability::event_field_key::outcome,
      .value = observability::event_field_value::from_text(*text)}};
    auto made = observability::event_request::make(
      observability::event_request_context{
        .kind = observability::event_kind::append_refused,
        .severity = observability::event_severity::warning,
        .monotonic = at,
        .wall = kwaque::runtime::wall_time{0},
        .workload = kwaque::resource::workload_class::foreground_protocol,
      },
      fields);
    BOOST_REQUIRE(made.has_value());
    return *made;
}

} // namespace

SEASTAR_TEST_CASE(each_call_runs_as_its_target_and_answers_on_the_caller) {
    auto target = make(4);
    co_await start(*target);
    BOOST_CHECK_EQUAL(target->virtual_shards(), 4U);
    BOOST_CHECK(target->owner().is_current());
    for (unsigned shard = 0; shard < 4; ++shard) {
        const auto ran_as = co_await drive(
          *target,
          target->shards().invoke_on(
            shard, seastar::default_smp_service_group(), [&target] {
                // The backend's per-shard parts answer for this shard.
                BOOST_CHECK(target->owner().is_current());
                return virtual_index();
            }));
        BOOST_CHECK_EQUAL(ran_as, shard);
        // The answer arrives as the caller.
        BOOST_CHECK_EQUAL(virtual_index(), 0U);
    }
    co_await stop(*target);
}

SEASTAR_TEST_CASE(invoke_on_owner_runs_as_the_owner_and_keeps_its_arguments) {
    auto target = make(3);
    co_await start(*target);
    const auto remote = co_await drive(
      *target,
      target->shards().invoke_on(
        2, seastar::default_smp_service_group(), [&target] {
            return target->owner();
        }));
    auto answering = runtime::basic_invoke_on_owner(
      target->shards(),
      remote,
      seastar::default_smp_service_group(),
      &add_index_after_yield,
      shard_sum{40});
    const auto answered = co_await drive(*target, std::move(answering));
    BOOST_CHECK_EQUAL(answered.value, 42U);
    co_await stop(*target);
}

// Each virtual shard mints from its own stream, keyed by the environment,
// its incarnation and the shard (incarnation × 8 + shard), so no two shards
// or lives share an identity.
SEASTAR_TEST_CASE(each_shard_and_life_mints_its_own_identities) {
    using octets = std::array<std::uint8_t, runtime::identity_entropy_bytes>;
    std::set<octets> seen;
    for (const std::uint64_t incarnation : {0U, 1U}) {
        auto target = make(3, 7, false, 1'024, incarnation);
        co_await start(*target);
        for (unsigned shard = 0; shard < 3; ++shard) {
            const auto drawn = co_await drive(
              *target,
              target->shards().invoke_on(
                shard, seastar::default_smp_service_group(), [&target] {
                    octets minted{};
                    target->identity_entropy().fill(minted);
                    return minted;
                }));
            BOOST_CHECK(seen.insert(drawn).second);
            simulation::deterministic_random random{7};
            auto stream = random.stream(
              simulation::random_domain::object_id_mint,
              1,
              incarnation * 8 + shard);
            BOOST_REQUIRE(stream.has_value());
            simulation::identity_entropy expected{*stream};
            octets wanted{};
            expected.fill(wanted);
            BOOST_CHECK(drawn == wanted);
        }
        co_await stop(*target);
    }
    BOOST_CHECK_EQUAL(seen.size(), 6U);
}

SEASTAR_TEST_CASE(an_incarnation_past_the_identity_keys_is_refused) {
    simulation::environment_config_values values;
    values.identity_incarnation = std::numeric_limits<std::uint64_t>::max() / 8U
                                  + 1U;
    const auto config = simulation::environment_config::make(std::move(values));
    BOOST_REQUIRE(!config.has_value());
    BOOST_CHECK(config.error().code() == kwaque::errc::out_of_range);
    co_return;
}

SEASTAR_TEST_CASE(a_call_to_the_current_shard_runs_inline) {
    auto target = make(2);
    co_await start(*target);
    const auto pending_before = target->event_scheduler().pending_events();
    auto local = target->shards().invoke_on(
      0, seastar::default_smp_service_group(), [] { return 5; });
    BOOST_CHECK(local.available());
    BOOST_CHECK_EQUAL(
      target->event_scheduler().pending_events(), pending_before);
    const auto answered = co_await std::move(local);
    BOOST_CHECK_EQUAL(answered, 5);
    co_await stop(*target);
}

SEASTAR_TEST_CASE(an_owner_of_one_virtual_shard_is_not_current_on_another) {
    auto target = make(3);
    co_await start(*target);
    const auto remote = co_await drive(
      *target,
      target->shards().invoke_on(
        2, seastar::default_smp_service_group(), [&target] {
            return target->owner();
        }));
    BOOST_CHECK(!remote.is_current());
    BOOST_CHECK_EQUAL(remote.value(), seastar::this_shard_id() + 2U);
    co_await stop(*target);
}

SEASTAR_TEST_CASE(adding_shards_leaves_shard_zero_draws_unchanged) {
    std::vector<std::uint64_t> one_shard;
    std::vector<std::uint64_t> four_shards;
    for (const std::uint16_t shards : {std::uint16_t{1}, std::uint16_t{4}}) {
        auto target = make(shards, 11);
        co_await start(*target);
        auto& words = shards == 1 ? one_shard : four_shards;
        for (int draw = 0; draw < 8; ++draw) {
            words.push_back(target->random().next_u64());
        }
        if (shards == 4) {
            // Another shard draws its own words.
            const auto other = co_await drive(
              *target,
              target->shards().invoke_on(
                3, seastar::default_smp_service_group(), [&target] {
                    return target->random().next_u64();
                }));
            BOOST_CHECK_NE(other, words.front());
        }
        co_await stop(*target);
    }
    BOOST_CHECK(one_shard == four_shards);
}

SEASTAR_TEST_CASE(timers_and_deadlines_belong_to_their_shard) {
    auto target = make(4);
    co_await start(*target);
    const auto outcome = co_await drive(
      *target,
      target->shards().invoke_on(
        3,
        seastar::default_smp_service_group(),
        [&target]() -> seastar::future<int> {
            seastar::abort_source abort;
            const auto now = simulation::monotonic_clock::now().nanoseconds();
            const auto slept = co_await target->timer().sleep_until(
              monotonic_time{now + 5 * millisecond}, abort);
            BOOST_CHECK(slept.has_value());
            // The sleep resumed as this shard.
            BOOST_CHECK_EQUAL(virtual_index(), 3U);
            runtime::unit_queue queue{0};
            const auto waited = co_await queue.wait(
              1,
              nullptr,
              target->deadlines(),
              monotonic_time{now + 20 * millisecond});
            BOOST_CHECK_EQUAL(virtual_index(), 3U);
            co_return !waited.has_value()
              && waited.error().code() == kwaque::errc::timed_out;
        }));
    BOOST_CHECK(outcome);
    co_await stop(*target);
}

// A virtual shard numbers, rate-bounds and stamps its own events in its own
// log, as a shard's own sink does.
SEASTAR_TEST_CASE(each_shard_numbers_and_bounds_its_own_events) {
    auto target = make(2);
    co_await start(*target);
    const auto now = simulation::monotonic_clock::now();
    const auto before = target->event_sink().last_sequence();
    BOOST_REQUIRE(target->event_sink().emit(refusal(now)).has_value());
    BOOST_CHECK_EQUAL(target->event_sink().last_sequence(), before + 1U);
    const auto numbered = co_await drive(
      *target,
      target->shards().invoke_on(
        1, seastar::default_smp_service_group(), [&target, now] {
            auto& sink = target->event_sink();
            // The other shard's refusal at this time spent its own word; a
            // second one here is dropped.
            BOOST_CHECK(sink.emit(refusal(now)).has_value());
            BOOST_CHECK(sink.emit(refusal(now)).has_value());
            const auto& log = sink.events().entries();
            BOOST_REQUIRE_EQUAL(log.size(), 1U);
            BOOST_CHECK_EQUAL(
              log[0].shard().value(), seastar::this_shard_id() + 1U);
            return sink.last_sequence();
        }));
    BOOST_CHECK_EQUAL(numbered, 1U);
    BOOST_CHECK_EQUAL(target->event_sink().last_sequence(), before + 1U);
    co_await stop(*target);
}

// A connection's events would run as the shard that scheduled them, so the
// fake network serves only the first virtual shard.
SEASTAR_TEST_CASE(a_later_shard_cannot_listen_or_connect) {
    auto target = make(2);
    co_await start(*target);
    constexpr auto loopback = runtime::network_address::ipv4(
      {std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}});
    const auto refused = co_await drive(
      *target,
      target->shards().invoke_on(
        1,
        seastar::default_smp_service_group(),
        [&target, loopback]() -> seastar::future<bool> {
            auto listened = co_await target->network().listen(
              runtime::network_endpoint{loopback, 0}, {});
            seastar::abort_source abort;
            auto connected = co_await target->network().connect(
              runtime::network_endpoint{loopback, 9}, std::nullopt, {}, abort);
            co_return !listened && !connected
              && listened.error().code() == kwaque::errc::unavailable
              && connected.error().code() == kwaque::errc::unavailable;
        }));
    BOOST_CHECK(refused);
    co_await stop(*target);
}

SEASTAR_TEST_CASE(a_pair_runs_no_more_calls_at_once_than_it_has_units) {
    auto target = make(2, 3, false, 1);
    co_await start(*target);
    static int running = 0;
    static int most = 0;
    running = 0;
    most = 0;
    std::vector<seastar::future<>> calls;
    for (int call = 0; call < 4; ++call) {
        calls.push_back(target->shards().invoke_on(
          1, seastar::default_smp_service_group(), []() -> seastar::future<> {
              ++running;
              most = std::max(most, running);
              co_await seastar::yield();
              --running;
          }));
    }
    co_await drive(*target, seastar::when_all_succeed(std::move(calls)));
    BOOST_CHECK_EQUAL(most, 1);
    co_await stop(*target);
}

namespace {

// Two calls on one pair, admitted A then B: whether B ran first.
seastar::future<bool> second_runs_first(std::uint64_t seed, bool reorder) {
    auto target = make(2, seed, reorder);
    co_await start(*target);
    static std::vector<int> order;
    order.clear();
    auto first = target->shards().invoke_on(
      1, seastar::default_smp_service_group(), [] { order.push_back(1); });
    auto second = target->shards().invoke_on(
      1, seastar::default_smp_service_group(), [] { order.push_back(2); });
    std::vector<seastar::future<>> both;
    both.push_back(std::move(first));
    both.push_back(std::move(second));
    co_await drive(*target, seastar::when_all_succeed(std::move(both)));
    co_await stop(*target);
    BOOST_REQUIRE_EQUAL(order.size(), 2U);
    co_return order.front() == 2;
}

} // namespace

SEASTAR_TEST_CASE(a_pair_keeps_admission_order_unless_reordering_is_on) {
    unsigned reordered_without = 0;
    unsigned reordered_with = 0;
    for (std::uint64_t seed = 1; seed <= 48; ++seed) {
        reordered_without += co_await second_runs_first(seed, false) ? 1U : 0U;
        reordered_with += co_await second_runs_first(seed, true) ? 1U : 0U;
    }
    BOOST_CHECK_EQUAL(reordered_without, 0U);
    // Both orders occur across seeds.
    BOOST_CHECK_GT(reordered_with, 0U);
    BOOST_CHECK_LT(reordered_with, 48U);
}

SEASTAR_TEST_CASE(a_multi_shard_run_replays_exactly) {
    std::vector<std::uint64_t> runs[2];
    for (auto& run : runs) {
        auto target = make(8, 19, true);
        co_await start(*target);
        static std::vector<std::uint64_t>* record = nullptr;
        record = &run;
        std::vector<seastar::future<>> calls;
        for (unsigned shard = 1; shard < 8; ++shard) {
            for (int call = 0; call < 3; ++call) {
                calls.push_back(target->shards().invoke_on(
                  shard, seastar::default_smp_service_group(), [] {
                      record->push_back(
                        virtual_index() * 1'000'000'000ULL
                        + simulation::monotonic_clock::now().nanoseconds());
                  }));
            }
        }
        co_await drive(*target, seastar::when_all_succeed(std::move(calls)));
        run.push_back(target->trace().entries().size());
        co_await stop(*target);
    }
    BOOST_CHECK(runs[0] == runs[1]);
}
