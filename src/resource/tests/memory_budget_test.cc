#include "src/base/error.h"
#include "src/base/units.h"
#include "src/resource/memory_budget.h"
#include "src/resource/reclaimer_registry.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace kwaque::resource {

class reclaimer_registry_test_access final {
public:
    // Drives the registry's reclaim translation with a synthesized request.
    static seastar::memory::reclaiming_result
    bridge_reclaim(reclaimer_registry& registry, std::size_t bytes) {
        return registry.bridge_reclaim(
          seastar::memory::reclaimer::request{.bytes_to_reclaim = bytes});
    }
};

namespace {

using synchronous_callback = decltype([](byte_count) noexcept {
    return byte_count{};
});
using asynchronous_callback = decltype([](byte_count) {
    return seastar::make_ready_future<byte_count>(byte_count{});
});

static_assert(synchronous_reclaim_callback<synchronous_callback>);
static_assert(!synchronous_reclaim_callback<asynchronous_callback>);
static_assert(!std::is_copy_constructible_v<memory_units>);
static_assert(!std::is_copy_assignable_v<memory_units>);
static_assert(std::is_nothrow_move_constructible_v<memory_units>);
static_assert(std::is_nothrow_move_assignable_v<memory_units>);

class recording_trigger final : public memory_reclaim_trigger {
public:
    void request_reclaim(byte_count target) noexcept final {
        targets[count] = target;
        ++count;
    }

    std::array<byte_count, 4> targets{};
    std::size_t count{0};
};

memory_budget_config test_config(
  std::uint64_t capacity,
  std::uint64_t soft,
  std::uint64_t high,
  std::size_t max_waiters) {
    return memory_budget_config{
      .capacity = byte_count{capacity},
      .soft_watermark = byte_count{soft},
      .high_watermark = byte_count{high},
      .max_waiters = max_waiters,
    };
}

} // namespace

SEASTAR_TEST_CASE(memory_budget_validates_boundaries_and_raii_accounting) {
    BOOST_CHECK(
      !memory_budget_config::with_defaults(byte_count{}, 1).has_value());
    const auto defaults = memory_budget_config::with_defaults(
      byte_count{100}, 2);
    BOOST_REQUIRE(defaults.has_value());
    BOOST_CHECK_EQUAL(defaults->soft_watermark.value(), 50U);
    BOOST_CHECK_EQUAL(defaults->high_watermark.value(), 87U);
    BOOST_CHECK_EQUAL(defaults->max_waiters, 2U);
    BOOST_CHECK(!test_config(100, 80, 80, 1).validate().has_value());
    BOOST_CHECK(!test_config(100, 90, 80, 1).validate().has_value());
    BOOST_CHECK(!test_config(100, 50, 101, 1).validate().has_value());
    const memory_budget_config integer_max_config{
      .capacity = byte_count{std::numeric_limits<std::uint64_t>::max()},
      .soft_watermark = byte_count{1},
      .high_watermark = byte_count{2},
      .max_waiters = 1,
    };
    BOOST_CHECK(!integer_max_config.validate().has_value());

    recording_trigger trigger;
    memory_budget budget{test_config(100, 50, 80, 2), &trigger};
    const auto zero = budget.try_acquire(byte_count{});
    BOOST_REQUIRE(!zero.has_value());
    BOOST_CHECK(zero.error().code() == errc::invalid_argument);
    const auto oversized = budget.try_acquire(byte_count{101});
    BOOST_REQUIRE(!oversized.has_value());
    BOOST_CHECK(oversized.error().code() == errc::resource_exhausted);
    const auto integer_max = budget.try_acquire(
      byte_count{std::numeric_limits<std::uint64_t>::max()});
    BOOST_REQUIRE(!integer_max.has_value());
    BOOST_CHECK(integer_max.error().code() == errc::resource_exhausted);

    auto all = budget.try_acquire(byte_count{100});
    BOOST_REQUIRE(all.has_value());
    BOOST_CHECK_EQUAL(budget.used().value(), 100U);
    BOOST_CHECK_EQUAL(budget.available().value(), 0U);
    BOOST_CHECK(budget.under_pressure());
    BOOST_CHECK_EQUAL(trigger.count, 1U);
    BOOST_CHECK_EQUAL(trigger.targets[0].value(), 50U);

    auto split = all->split(byte_count{40});
    BOOST_REQUIRE(split.has_value());
    BOOST_CHECK_EQUAL(all->count().value(), 60U);
    BOOST_CHECK_EQUAL(split->count().value(), 40U);
    BOOST_REQUIRE(all->merge(std::move(*split)).has_value());
    BOOST_CHECK_EQUAL(all->count().value(), 100U);
    BOOST_CHECK(!static_cast<bool>(*split));

    memory_units moved{std::move(*all)};
    BOOST_CHECK(!static_cast<bool>(*all));
    BOOST_CHECK_EQUAL(moved.count().value(), 100U);
    BOOST_REQUIRE(budget.schedule_release(byte_count{40}).has_value());
    BOOST_CHECK_EQUAL(budget.scheduled_release().value(), 40U);
    BOOST_CHECK_EQUAL(budget.active().value(), 60U);
    BOOST_CHECK_EQUAL(budget.available().value(), 0U);
    BOOST_REQUIRE(budget.schedule_release(byte_count{10}).has_value());
    BOOST_CHECK(!budget.under_pressure());
    BOOST_CHECK_EQUAL(budget.available().value(), 0U);
    BOOST_CHECK_EQUAL(moved.release().value(), 100U);
    BOOST_CHECK_EQUAL(moved.release().value(), 0U);
    BOOST_CHECK_EQUAL(budget.used().value(), 0U);
    BOOST_CHECK_EQUAL(budget.scheduled_release().value(), 0U);
    BOOST_CHECK_EQUAL(budget.available().value(), 100U);

    try {
        auto exception_units = budget.try_acquire(byte_count{80});
        BOOST_REQUIRE(exception_units.has_value());
        throw std::runtime_error("synthetic early return");
    } catch (const std::runtime_error&) {
    }
    BOOST_CHECK_EQUAL(budget.available().value(), 100U);
    BOOST_CHECK_EQUAL(trigger.count, 2U);
    const auto counters = budget.counters();
    BOOST_CHECK_EQUAL(counters.admitted, 2U);
    BOOST_CHECK_EQUAL(counters.rejected, 3U);
    BOOST_CHECK_EQUAL(counters.high_transitions, 2U);
    BOOST_CHECK_EQUAL(counters.relief_transitions, 2U);
    co_return;
}

SEASTAR_TEST_CASE(memory_budget_bounds_waiters_and_translates_abort) {
    memory_budget budget{test_config(10, 5, 8, 1)};
    auto held = budget.try_acquire(byte_count{10});
    BOOST_REQUIRE(held.has_value());

    seastar::abort_source first_abort;
    auto first = budget.acquire(byte_count{6}, first_abort);
    co_await seastar::yield();
    BOOST_CHECK(!first.available());
    BOOST_CHECK_EQUAL(budget.waiting(), 1U);

    seastar::abort_source second_abort;
    auto excess = co_await budget.acquire(byte_count{1}, second_abort);
    BOOST_REQUIRE(!excess.has_value());
    BOOST_CHECK(excess.error().code() == errc::resource_exhausted);
    BOOST_CHECK_EQUAL(budget.waiting(), 1U);

    first_abort.request_abort();
    auto aborted = co_await std::move(first);
    BOOST_REQUIRE(!aborted.has_value());
    BOOST_CHECK(aborted.error().code() == errc::aborted);
    BOOST_CHECK_EQUAL(budget.waiting(), 0U);
    BOOST_CHECK_EQUAL(budget.used().value(), 10U);

    seastar::abort_source waiting_abort;
    auto waiting = budget.acquire(byte_count{10}, waiting_abort);
    co_await seastar::yield();
    BOOST_CHECK_EQUAL(budget.waiting(), 1U);
    BOOST_CHECK(budget.under_pressure());

    // Returning units readies the waiter and the admission counter deducts its
    // bytes at once, but the waiter only owns them once it resumes. Through
    // that window the bytes are committed to nobody, and because nothing became
    // available the budget must not read as relieved.
    static_cast<void>(held->release());
    BOOST_CHECK_EQUAL(budget.used().value(), 0U);
    BOOST_CHECK_EQUAL(budget.granted_pending().value(), 10U);
    BOOST_CHECK_EQUAL(budget.committed().value(), 10U);
    BOOST_CHECK_EQUAL(budget.available().value(), 0U);
    BOOST_CHECK(budget.under_pressure());

    auto admitted = co_await std::move(waiting);
    BOOST_REQUIRE(admitted.has_value());
    BOOST_CHECK_EQUAL(admitted->count().value(), 10U);
    BOOST_CHECK_EQUAL(budget.granted_pending().value(), 0U);
    BOOST_CHECK_EQUAL(budget.used().value(), 10U);
    BOOST_CHECK_EQUAL(budget.committed().value(), 10U);
    static_cast<void>(admitted->release());
    BOOST_CHECK_EQUAL(budget.committed().value(), 0U);
    BOOST_CHECK_EQUAL(budget.available().value(), 10U);
    BOOST_CHECK(!budget.under_pressure());

    seastar::abort_source preaborted;
    preaborted.request_abort();
    auto immediate = budget.acquire(byte_count{1}, preaborted);
    BOOST_CHECK(immediate.available());
    auto immediate_result = co_await std::move(immediate);
    BOOST_REQUIRE(!immediate_result.has_value());
    BOOST_CHECK(immediate_result.error().code() == errc::aborted);
}

SEASTAR_TEST_CASE(reclaimer_registry_orders_bounds_and_guards_reentry) {
    reclaimer_registry registry{3};
    registry.start();
    BOOST_CHECK(
      registry.bridge_scope() == seastar::memory::reclaimer_scope::async);

    reclaimer_registry competing{1};
    BOOST_CHECK_THROW(competing.start(), std::logic_error);
    competing.stop();

    std::array<unsigned, 8> order{};
    std::size_t next = 0;
    auto first = registry.register_reclaimer(
      10, [&order, &next](byte_count) noexcept {
          if (next < order.size()) {
              order[next] = 1;
          }
          ++next;
          return byte_count{4};
      });
    auto second = registry.register_reclaimer(
      10, [&order, &next](byte_count) noexcept {
          if (next < order.size()) {
              order[next] = 2;
          }
          ++next;
          return byte_count{3};
      });
    auto third = registry.register_reclaimer(
      5, [&order, &next](byte_count) noexcept {
          if (next < order.size()) {
              order[next] = 3;
          }
          ++next;
          return byte_count{20};
      });
    BOOST_REQUIRE(first.has_value());
    BOOST_REQUIRE(second.has_value());
    BOOST_REQUIRE(third.has_value());
    const auto full = registry.register_reclaimer(
      0, [](byte_count) noexcept { return byte_count{}; });
    BOOST_REQUIRE(!full.has_value());
    BOOST_CHECK(full.error() == make_error_code(errc::resource_exhausted));

    const auto reclaimed = registry.request_reclaim(byte_count{10});
    BOOST_CHECK_EQUAL(reclaimed.value(), 27U);
    BOOST_CHECK_EQUAL(next, 3U);
    BOOST_CHECK_EQUAL(order[0], 1U);
    BOOST_CHECK_EQUAL(order[1], 2U);
    BOOST_CHECK_EQUAL(order[2], 3U);
    auto counters = registry.counters();
    BOOST_CHECK_EQUAL(counters.attempts, 1U);
    BOOST_CHECK_EQUAL(counters.callbacks, 3U);
    BOOST_CHECK_EQUAL(counters.progress_bytes, 27U);
    BOOST_CHECK_GT(counters.last_allocator_total_bytes, 0U);
    BOOST_CHECK_LE(
      counters.last_allocator_free_bytes, counters.last_allocator_total_bytes);
    third->reset();
    auto recursive = registry.register_reclaimer(
      20, [&registry, &order, &next](byte_count) noexcept {
          if (next < order.size()) {
              order[next] = 4;
          }
          ++next;
          return registry.request_reclaim(byte_count{1});
      });
    BOOST_REQUIRE(recursive.has_value());
    const auto after_recursive = registry.request_reclaim(byte_count{1});
    BOOST_CHECK_EQUAL(after_recursive.value(), 4U);
    BOOST_CHECK_EQUAL(order[3], 4U);
    counters = registry.counters();
    BOOST_CHECK_EQUAL(counters.reentries, 1U);
    BOOST_CHECK(
      reclaimer_registry_test_access::bridge_reclaim(registry, 10)
      == seastar::memory::reclaiming_result::reclaimed_something);

    BOOST_CHECK_THROW(registry.stop(), std::logic_error);
    recursive->reset();
    second->reset();
    first->reset();
    auto zero_progress = registry.register_reclaimer(
      0, [](byte_count) noexcept { return byte_count{}; });
    BOOST_REQUIRE(zero_progress.has_value());
    BOOST_CHECK(
      reclaimer_registry_test_access::bridge_reclaim(registry, 1)
      == seastar::memory::reclaiming_result::reclaimed_nothing);
    zero_progress->reset();
    BOOST_CHECK_EQUAL(registry.size(), 0U);
    registry.stop();
    registry.stop();
    co_return;
}

} // namespace kwaque::resource
