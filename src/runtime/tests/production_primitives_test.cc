#include "src/model/identity_mint.h"
#include "src/runtime/deadlines.h"
#include "src/runtime/production/alarm.h"
#include "src/runtime/production/clocks.h"
#include "src/runtime/production/random.h"
#include "src/runtime/production/timer.h"
#include "src/runtime/random.h"
#include "src/runtime/time.h"
#include "src/runtime/timer.h"
#include "src/runtime/waiting.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>
#include <sys/random.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <new>
#include <span>
#include <stdexcept>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

using kwaque::runtime::production::monotonic_clock;
using kwaque::runtime::production::random_source;
using kwaque::runtime::production::timer;
using kwaque::runtime::production::wall_clock;

class failing_entropy final {
public:
    std::uint32_t operator()() {
        throw std::system_error(std::make_error_code(std::errc::io_error));
    }
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

SEASTAR_TEST_CASE(production_random_preserves_unclassified_entropy_failures) {
    const std::array failures{
      std::make_exception_ptr(std::runtime_error("unexpected entropy failure")),
      std::make_exception_ptr(std::logic_error("invalid entropy state")),
    };
    for (const auto& failure : failures) {
        auto entropy = [&failure] -> std::uint32_t {
            std::rethrow_exception(failure);
        };
        std::exception_ptr observed;
        try {
            static_cast<void>(
              kwaque::runtime::production::detail::read_entropy_seed(entropy));
        } catch (...) {
            observed = std::current_exception();
        }
        BOOST_CHECK(observed == failure);
    }
    co_return;
}

SEASTAR_TEST_CASE(
  production_timer_preserves_unclassified_cancellation_failure) {
    timer service;
    seastar::abort_source caller_abort;
    const auto deadline = monotonic_clock::now().checked_add(
      kwaque::runtime::monotonic_duration{60'000'000'000U});
    BOOST_REQUIRE(deadline.has_value());
    auto waiting = service.sleep_until(*deadline, caller_abort);
    const auto failure = std::make_exception_ptr(
      std::logic_error("unexpected timer cancellation failure"));
    caller_abort.request_abort_ex(failure);
    std::exception_ptr observed;
    try {
        static_cast<void>(co_await std::move(waiting));
    } catch (...) {
        observed = std::current_exception();
    }
    const auto stopped = co_await service.stop();
    BOOST_CHECK(observed == failure);
    BOOST_REQUIRE(stopped.has_value());
    BOOST_CHECK_EQUAL(service.statistics().active, 0U);
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

namespace {

// Answers each kernel call from a script of {result, errno} steps and
// writes the requested bytes with a running counter.
class scripted_kernel_random final {
public:
    struct step {
        long result;
        int error;
    };

    explicit scripted_kernel_random(std::vector<step> steps) noexcept
      : steps_(std::move(steps)) {}

    long operator()(void* buffer, std::size_t size, unsigned flags) noexcept {
        flags_.push_back(flags);
        const auto current = steps_.at(calls_++);
        if (current.result < 0) {
            errno = current.error;
            return -1;
        }
        auto* bytes = static_cast<std::uint8_t*>(buffer);
        const auto written = std::min(
          size, static_cast<std::size_t>(current.result));
        for (std::size_t at = 0; at < written; ++at)
            bytes[at] = next_++;
        return static_cast<long>(written);
    }

    [[nodiscard]] std::size_t calls() const noexcept { return calls_; }
    [[nodiscard]] const std::vector<unsigned>& flags() const noexcept {
        return flags_;
    }

private:
    std::vector<step> steps_;
    std::vector<unsigned> flags_;
    std::size_t calls_{0};
    std::uint8_t next_{1};
};

static_assert(kwaque::runtime::identity_entropy_source<
              kwaque::runtime::production::identity_entropy>);
// The mint, which may not see the runtime's concept, takes it too.
static_assert(kwaque::model::object_id_entropy<
              kwaque::runtime::production::identity_entropy>);

} // namespace

SEASTAR_TEST_CASE(production_identity_entropy_refuses_an_unready_pool) {
    namespace detail = kwaque::runtime::production::detail;
    scripted_kernel_random unready{{{-1, EAGAIN}}};
    const auto refused = detail::probe_identity_entropy(unready);
    BOOST_REQUIRE(!refused.has_value());
    BOOST_CHECK(refused.error().code() == kwaque::errc::unavailable);
    BOOST_REQUIRE_EQUAL(unready.flags().size(), 1U);
    BOOST_CHECK_EQUAL(unready.flags().front(), unsigned{GRND_NONBLOCK});

    scripted_kernel_random missing{{{-1, ENOSYS}}};
    BOOST_CHECK(!detail::probe_identity_entropy(missing).has_value());

    scripted_kernel_random interrupted{{{-1, EINTR}, {16, 0}}};
    BOOST_CHECK(detail::probe_identity_entropy(interrupted).has_value());
    BOOST_CHECK_EQUAL(interrupted.calls(), 2U);

    scripted_kernel_random short_read{{{8, 0}}};
    BOOST_CHECK(!detail::probe_identity_entropy(short_read).has_value());
    co_return;
}

SEASTAR_TEST_CASE(
  production_identity_entropy_retries_interrupts_and_short_reads) {
    namespace detail = kwaque::runtime::production::detail;
    scripted_kernel_random kernel{{{5, 0}, {-1, EINTR}, {3, 0}, {16, 0}}};
    std::array<std::uint8_t, 16> octets{};
    detail::fill_identity_octets(kernel, octets);
    BOOST_CHECK_EQUAL(kernel.calls(), 4U);
    for (std::size_t at = 0; at < octets.size(); ++at)
        BOOST_CHECK_EQUAL(octets[at], static_cast<std::uint8_t>(at + 1));
    for (const auto flags : kernel.flags())
        BOOST_CHECK_EQUAL(flags, 0U);
    co_return;
}

SEASTAR_TEST_CASE(
  production_identity_entropy_fills_from_the_kernel_without_allocating) {
    auto source = kwaque::runtime::production::identity_entropy::make();
    BOOST_REQUIRE(source.has_value());
    std::array<std::uint8_t, 16> first{};
    std::array<std::uint8_t, 16> second{};
#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
    const auto before = seastar::memory::stats().mallocs();
#endif
    source->fill(first);
    source->fill(second);
#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
    BOOST_CHECK_EQUAL(seastar::memory::stats().mallocs() - before, 0U);
#endif
    BOOST_CHECK(first != second);
    const auto minted = kwaque::model::mint_object_id<kwaque::model::topic_id>(
      *source);
    BOOST_CHECK_EQUAL(minted.bytes()[6] >> 4U, 4U);
    BOOST_CHECK_EQUAL(minted.bytes()[8] >> 6U, 2U);
    co_return;
}

SEASTAR_TEST_CASE(production_alarm_rings_at_its_time_and_drives_deadlines) {
    using kwaque::runtime::production::monotonic_clock;
    bool rang = false;
    kwaque::runtime::production::alarm alarm{
      [&rang]() noexcept { rang = true; }};
    const auto armed_for = monotonic_clock::now().nanoseconds() + 3'000'000;
    alarm.arm(kwaque::runtime::monotonic_time{armed_for});
    while (!rang) {
        co_await seastar::sleep(std::chrono::milliseconds(1));
    }
    BOOST_CHECK_GE(monotonic_clock::now().nanoseconds(), armed_for);

    kwaque::runtime::
      basic_deadlines<kwaque::runtime::production::alarm, monotonic_clock>
        deadlines{kwaque::runtime::basic_deadlines<
          kwaque::runtime::production::alarm,
          monotonic_clock>::default_touches_per_task};
    kwaque::runtime::unit_queue queue{0};
    const kwaque::runtime::monotonic_time deadline{
      monotonic_clock::now().nanoseconds() + 5'000'000};
    const auto outcome = co_await queue.wait(1, nullptr, deadlines, deadline);
    BOOST_REQUIRE(!outcome.has_value());
    BOOST_CHECK(outcome.error().code() == kwaque::errc::timed_out);
    BOOST_CHECK_GE(
      monotonic_clock::now().nanoseconds(), deadline.nanoseconds());
    BOOST_CHECK(!deadlines.armed());
}
