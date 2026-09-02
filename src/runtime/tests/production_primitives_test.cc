#include "src/runtime/production/clocks.h"
#include "src/runtime/production/random.h"
#include "src/runtime/production/timer.h"
#include "src/runtime/random.h"
#include "src/runtime/time.h"
#include "src/runtime/timer.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <span>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace {

using kwaque::runtime::production::monotonic_clock;
using kwaque::runtime::production::random_source;
using kwaque::runtime::production::timer;
using kwaque::runtime::production::wall_clock;

class failing_entropy final {
public:
    std::uint32_t operator()() { throw std::runtime_error("entropy failed"); }
};

class allocation_failing_entropy final {
public:
    std::uint32_t operator()() { throw std::bad_alloc{}; }
};

static_assert(kwaque::runtime::monotonic_clock<monotonic_clock>);
static_assert(kwaque::runtime::wall_clock<wall_clock>);
static_assert(kwaque::runtime::timer_service<timer>);
static_assert(kwaque::runtime::random_source<random_source>);
static_assert(!std::is_copy_constructible_v<random_source>);
static_assert(!std::is_copy_assignable_v<random_source>);
static_assert(!std::is_convertible_v<
              decltype(monotonic_clock::now()),
              decltype(wall_clock::now())>);

} // namespace

SEASTAR_TEST_CASE(production_clocks_preserve_native_domains_and_order) {
    const auto monotonic_first = monotonic_clock::now();
    const auto monotonic_second = monotonic_clock::now();
    const auto wall_second = wall_clock::now();
    const auto native_wall_nanoseconds
      = std::chrono::duration_cast<
          std::chrono::duration<kwaque::runtime::wall_time::rep, std::nano>>(
          seastar::lowres_system_clock::now().time_since_epoch())
          .count();

    BOOST_CHECK(monotonic_second >= monotonic_first);
    BOOST_CHECK_EQUAL(
      monotonic_second.nanoseconds(),
      static_cast<std::uint64_t>(
        seastar::lowres_clock::now().time_since_epoch().count()));
    BOOST_CHECK_EQUAL(wall_second.unix_nanoseconds(), native_wall_nanoseconds);
    co_return;
}

SEASTAR_TEST_CASE(production_timer_allows_never_activated_rollback) {
    timer service;
    static_cast<void>(service);
    co_return;
}

SEASTAR_TEST_CASE(production_timer_keeps_past_and_now_completion_asynchronous) {
    timer service;
    seastar::abort_source caller_abort;
    const auto now = monotonic_clock::now();

    auto waiting = service.sleep_until(now, caller_abort);
    BOOST_CHECK(!waiting.available());
    const auto outcome = co_await std::move(waiting);
    const auto stopped = co_await service.stop();

    BOOST_REQUIRE(outcome.has_value());
    BOOST_REQUIRE(stopped.has_value());
}

SEASTAR_TEST_CASE(production_timer_handles_future_deadline_and_caller_abort) {
    timer completed_service;
    seastar::abort_source active;
    const auto deadline = monotonic_clock::now().checked_add(
      kwaque::runtime::monotonic_duration{1'000'000});
    BOOST_REQUIRE(deadline.has_value());
    const auto completed = co_await completed_service.sleep_until(
      *deadline, active);
    const auto completed_stop = co_await completed_service.stop();

    BOOST_REQUIRE(completed.has_value());
    BOOST_REQUIRE(completed_stop.has_value());
    BOOST_CHECK(
      completed_service.statistics()
      == (kwaque::runtime::operation_statistics_snapshot{
        .active = 0,
        .accepted = 1,
        .completed = 1,
      }));

    timer aborted_service;
    seastar::abort_source preaborted;
    preaborted.request_abort();
    const auto aborted = co_await aborted_service.sleep_until(
      monotonic_clock::now(), preaborted);
    const auto aborted_stop = co_await aborted_service.stop();

    BOOST_REQUIRE(!aborted.has_value());
    BOOST_CHECK(aborted.error().code() == kwaque::errc::aborted);
    BOOST_REQUIRE(aborted_stop.has_value());
    BOOST_CHECK(
      aborted_service.statistics()
      == (kwaque::runtime::operation_statistics_snapshot{
        .rejected = 1,
      }));
}

SEASTAR_TEST_CASE(production_timer_checks_native_signed_duration_boundary) {
    constexpr auto native_maximum = static_cast<std::uint64_t>(
      std::numeric_limits<seastar::lowres_clock::duration::rep>::max());
    const auto base = monotonic_clock::now();
    BOOST_REQUIRE_LE(base.nanoseconds(), native_maximum);
    const auto maximum_delta = native_maximum - base.nanoseconds();

    timer accepted_service;
    seastar::abort_source accepted_abort;
    const auto accepted_deadline = base.checked_add(
      kwaque::runtime::monotonic_duration{maximum_delta});
    BOOST_REQUIRE(accepted_deadline.has_value());
    auto accepted_wait = accepted_service.sleep_until(
      *accepted_deadline, accepted_abort);
    accepted_abort.request_abort();
    const auto accepted = co_await std::move(accepted_wait);
    const auto accepted_stop = co_await accepted_service.stop();

    BOOST_REQUIRE(!accepted.has_value());
    BOOST_CHECK(accepted.error().code() == kwaque::errc::aborted);
    BOOST_REQUIRE(accepted_stop.has_value());

    timer rejected_service;
    seastar::abort_source rejected_abort;
    const auto rejected_deadline = accepted_deadline->checked_add(
      kwaque::runtime::monotonic_duration{1});
    BOOST_REQUIRE(rejected_deadline.has_value());
    const auto rejected = co_await rejected_service.sleep_until(
      *rejected_deadline, rejected_abort);
    const auto rejected_stop = co_await rejected_service.stop();

    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == kwaque::errc::out_of_range);
    BOOST_REQUIRE(rejected_stop.has_value());
}

SEASTAR_TEST_CASE(production_timer_stop_aborts_drains_and_is_idempotent) {
    timer service;
    seastar::abort_source caller_abort;
    const auto deadline = monotonic_clock::now().checked_add(
      kwaque::runtime::monotonic_duration{60'000'000'000U});
    BOOST_REQUIRE(deadline.has_value());
    auto waiting = service.sleep_until(*deadline, caller_abort);

    auto first_stop = service.stop();
    auto second_stop = service.stop();
    const auto wait_outcome = co_await std::move(waiting);
    const auto first_outcome = co_await std::move(first_stop);
    const auto second_outcome = co_await std::move(second_stop);

    BOOST_REQUIRE(!wait_outcome.has_value());
    BOOST_CHECK(wait_outcome.error().code() == kwaque::errc::aborted);
    BOOST_REQUIRE(first_outcome.has_value());
    BOOST_REQUIRE(second_outcome.has_value());
    BOOST_CHECK(
      service.state() == kwaque::runtime::production::timer_state::stopped);
}

SEASTAR_TEST_CASE(production_random_matches_fixed_xoshiro_vectors) {
    random_source source{92};
    constexpr std::array<std::uint64_t, 8> expected{
      UINT64_C(0x25d991ca70736128),
      UINT64_C(0x3b532872acfccd46),
      UINT64_C(0xabcf431a1a81562e),
      UINT64_C(0xcbe21e640906d620),
      UINT64_C(0x4de790b1b79a78fb),
      UINT64_C(0xf9e42e0685dc493a),
      UINT64_C(0x4128ab554877c90f),
      UINT64_C(0x3909b00374bb4763),
    };
    for (const auto word : expected) {
        BOOST_CHECK_EQUAL(source.next_u64(), word);
    }
    co_return;
}

SEASTAR_TEST_CASE(production_random_seed_constructs_independent_local_state) {
    random_source first{7};
    random_source same{7};
    random_source different{8};
    for (std::size_t index = 0; index < 32; ++index) {
        const auto word = first.next_u64();
        BOOST_CHECK_EQUAL(word, same.next_u64());
        BOOST_CHECK_NE(word, different.next_u64());
    }
    co_return;
}

SEASTAR_TEST_CASE(
  production_random_translates_entropy_failure_only_at_startup) {
    failing_entropy unavailable;
    const auto failed = kwaque::runtime::production::detail::read_entropy_seed(
      unavailable);
    BOOST_REQUIRE(!failed.has_value());
    BOOST_CHECK(failed.error().code() == kwaque::errc::unavailable);

    allocation_failing_entropy allocation_failure;
    BOOST_CHECK_THROW(
      static_cast<void>(kwaque::runtime::production::detail::read_entropy_seed(
        allocation_failure)),
      std::bad_alloc);

    auto seeded = random_source::make();
    BOOST_REQUIRE(seeded.has_value());
    static_cast<void>(seeded->next_u64());
    co_return;
}

SEASTAR_TEST_CASE(production_random_uses_shared_bounded_and_fill_algorithms) {
    random_source source{92};
    const auto bounded = kwaque::runtime::uniform_u64(source, 17);
    BOOST_REQUIRE(bounded.has_value());
    BOOST_CHECK_LT(*bounded, 17U);

    const auto probability = kwaque::runtime::probability_ratio::make(1, 3);
    BOOST_REQUIRE(probability.has_value());
    static_cast<void>(kwaque::runtime::chance(source, *probability));

    std::array<std::byte, 13> bytes{};
    kwaque::runtime::fill_bytes(source, std::span<std::byte>{bytes});
    const std::array<std::byte, 13> empty{};
    BOOST_CHECK(bytes != empty);
    co_return;
}

SEASTAR_TEST_CASE(production_random_statistical_smoke_covers_every_bucket) {
    random_source source{92};
    std::array<std::size_t, 8> buckets{};
    constexpr std::size_t draws = 80'000;
    for (std::size_t index = 0; index < draws; ++index) {
        ++buckets[source.next_u64() & 7U];
    }
    for (const auto count : buckets) {
        BOOST_CHECK_GT(count, 9'000U);
        BOOST_CHECK_LT(count, 11'000U);
    }
    co_return;
}
