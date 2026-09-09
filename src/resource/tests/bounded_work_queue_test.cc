#include "src/base/error.h"
#include "src/base/units.h"
#include "src/resource/bounded_work_queue.h"
#include "src/resource/resource_config.h"
#include "src/resource/resource_manager.h"
#include "src/resource/resource_registry.h"
#include "src/resource/workload_class.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/later.hh>
#include <seastar/util/noncopyable_function.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <new>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace kwaque::resource {

namespace {

static_assert(sizeof(queue_failure) < sizeof(runtime::operation_error));

class optional_item_failure final : public std::exception {};

bounded_work_queue_config queue_config(
  std::uint64_t items,
  std::uint64_t bytes,
  std::size_t producer_waiters,
  std::size_t manual_consumer_waiters = 64) {
    return bounded_work_queue_config{
      .maximum_items = item_count{items},
      .maximum_bytes = byte_count{bytes},
      .maximum_producer_waiters = producer_waiters,
      .maximum_manual_consumer_waiters = manual_consumer_waiters,
    };
}

resource_config manager_config() {
    auto config = resource_config::from_total_memory(
      byte_count{
        static_cast<std::uint64_t>(seastar::memory::stats().total_memory())});
    if (!config) {
        throw std::runtime_error("test resource configuration was rejected");
    }
    return *config;
}

[[gnu::noinline]] seastar::future<queue_result<int>> pop_for_allocation_check(
  bounded_work_queue<int>& queue, seastar::abort_source& abort_source) {
    return queue.pop(abort_source);
}

[[gnu::noinline]] seastar::future<queue_result<void>> push_for_allocation_check(
  bounded_work_queue<int>& queue, seastar::abort_source& abort_source) {
    return queue.push(7, byte_count{3}, abort_source);
}

class move_callback_item final {
public:
    move_callback_item(
      int initial_value,
      bool& armed,
      seastar::noncopyable_function<void() noexcept>& callback) noexcept
      : value(initial_value)
      , armed_(&armed)
      , callback_(&callback) {}

    move_callback_item(move_callback_item&& other) noexcept
      : value(other.value)
      , armed_(std::exchange(other.armed_, nullptr))
      , callback_(other.callback_) {
        if (armed_ != nullptr && std::exchange(*armed_, false)) {
            (*callback_)();
        }
    }
    move_callback_item(const move_callback_item&) = delete;
    move_callback_item& operator=(const move_callback_item&) = delete;
    move_callback_item& operator=(move_callback_item&&) = delete;

    int value;

private:
    bool* armed_;
    seastar::noncopyable_function<void() noexcept>* callback_;
};

} // namespace

SEASTAR_TEST_CASE(
  bounded_work_queue_ready_push_defers_close_until_publication_and_drain) {
    bounded_work_queue<move_callback_item> queue{queue_config(1, 10, 0)};
    seastar::abort_source abort_source;
    bool armed = true;
    bool called = false;
    bool unpublished = false;
    bool close_pending = false;
    std::exception_ptr callback_failure;
    std::optional<seastar::future<>> closing;
    seastar::noncopyable_function<void() noexcept> callback = [&] noexcept {
        called = true;
        unpublished = queue.size() == 0 && queue.bytes().value() == 0;
        try {
            closing.emplace(queue.close(queue_close_mode::drain));
            close_pending = !closing->available();
        } catch (...) {
            callback_failure = std::current_exception();
        }
    };
    const auto pushed = co_await queue.push(
      move_callback_item{7, armed, callback}, byte_count{3}, abort_source);
    BOOST_CHECK(called);
    BOOST_CHECK(unpublished);
    BOOST_CHECK(close_pending);
    BOOST_CHECK(!callback_failure);
    BOOST_REQUIRE(pushed.has_value());
    BOOST_CHECK_EQUAL(queue.size(), 1U);
    BOOST_CHECK_EQUAL(queue.bytes().value(), 3U);
    BOOST_CHECK(queue.state() == bounded_work_queue_state::draining);
    const auto popped = co_await queue.pop(abort_source);
    BOOST_REQUIRE(popped.has_value());
    BOOST_CHECK_EQUAL(popped->value, 7);
    if (closing) {
        co_await std::move(*closing);
    }
    co_await queue.close(queue_close_mode::drain);
    BOOST_CHECK(queue.state() == bounded_work_queue_state::closed);
    BOOST_CHECK_EQUAL(queue.bytes().value(), 0U);
}

SEASTAR_TEST_CASE(
  bounded_work_queue_push_move_abort_precedes_waiter_saturation) {
    bounded_work_queue<move_callback_item> queue{queue_config(1, 10, 1)};
    seastar::abort_source abort_source;
    seastar::abort_source newcomer_abort;
    bool armed = false;
    bool called = false;
    seastar::noncopyable_function<void() noexcept> callback = [&] noexcept {
        called = true;
        newcomer_abort.request_abort();
    };
    BOOST_REQUIRE(
      (co_await queue.push(
         move_callback_item{1, armed, callback}, byte_count{10}, abort_source))
        .has_value());
    auto head = queue.push(
      move_callback_item{2, armed, callback}, byte_count{1}, abort_source);
    BOOST_REQUIRE(!head.available());
    BOOST_CHECK_EQUAL(queue.waiting_producers(), 1U);
    BOOST_CHECK(!called);

    // The prvalue enters push directly. Its subsequent move into push_wait
    // aborts after public validation, with no queued-producer slot available.
    armed = true;
    const auto rejected = co_await queue.push(
      move_callback_item{3, armed, callback}, byte_count{1}, newcomer_abort);
    const auto pending_producers = queue.waiting_producers();
    auto closing = queue.close(queue_close_mode::abort);
    const auto interrupted_head = co_await std::move(head);
    co_await std::move(closing);

    BOOST_CHECK(called);
    BOOST_CHECK(!armed);
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().kind == queue_failure_kind::aborted);
    BOOST_CHECK_EQUAL(pending_producers, 1U);
    BOOST_REQUIRE(!interrupted_head.has_value());
    BOOST_CHECK(interrupted_head.error().kind == queue_failure_kind::closed);
    BOOST_CHECK_EQUAL(queue.waiting_producers(), 0U);
    BOOST_CHECK_EQUAL(queue.bytes().value(), 0U);
}

SEASTAR_TEST_CASE(
  bounded_work_queue_ready_consumer_prevents_reentrant_worker_start) {
    resource_registry registry;
    co_await registry.start(manager_config());
    resource_manager manager{registry.handles()};
    co_await manager.start();
    {
        bounded_work_queue<move_callback_item> queue{
          queue_config(1, 10, 0), manager, workload_class::maintenance};
        seastar::abort_source abort_source;
        bool armed = false;
        bool called = false;
        bool rejected_for_active_consumer = false;
        std::exception_ptr callback_failure;
        seastar::noncopyable_function<void() noexcept> callback = [&] noexcept {
            called = true;
            try {
                queue.start_workers(
                  bounded_queue_worker_config{
                    .workers = 1,
                    .maximum_error_reports = 0,
                  },
                  [](move_callback_item) {
                      return seastar::make_ready_future<>();
                  },
                  [](std::exception_ptr) noexcept {});
            } catch (const std::logic_error& error) {
                rejected_for_active_consumer
                  = std::string_view{error.what()}
                    == "queue has active manual consumers";
            } catch (...) {
                callback_failure = std::current_exception();
            }
        };
        BOOST_REQUIRE((co_await queue.push(
                         move_callback_item{7, armed, callback},
                         byte_count{3},
                         abort_source))
                        .has_value());
        BOOST_CHECK(!called);
        armed = true;
        const auto popped = co_await queue.pop(abort_source);
        BOOST_REQUIRE(popped.has_value());
        BOOST_CHECK(!armed);
        BOOST_CHECK_EQUAL(popped->value, 7);
        BOOST_CHECK(called);
        BOOST_CHECK(rejected_for_active_consumer);
        BOOST_CHECK(!callback_failure);
        BOOST_CHECK_EQUAL(queue.configured_workers(), 0U);
        BOOST_CHECK_EQUAL(queue.bytes().value(), 0U);
        co_await queue.close(queue_close_mode::abort);
    }
    co_await manager.stop();
    co_await registry.stop();
}

SEASTAR_TEST_CASE(bounded_work_queue_ready_push_avoids_coroutine_allocation) {
    bounded_work_queue<int> queue{queue_config(1, 10, 0)};
    seastar::abort_source abort_source;
    BOOST_REQUIRE(
      (co_await queue.push(0, byte_count{3}, abort_source)).has_value());
    BOOST_REQUIRE((co_await queue.pop(abort_source)).has_value());
#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
    const auto before = seastar::memory::stats().mallocs();
#endif
    auto pushing = push_for_allocation_check(queue, abort_source);
#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
    const auto after = seastar::memory::stats().mallocs();
    BOOST_CHECK_EQUAL(after, before);
#endif
    BOOST_REQUIRE(pushing.available());
    BOOST_REQUIRE(pushing.get().has_value());
    BOOST_CHECK_EQUAL(queue.accepted_pushes(), 2U);
    BOOST_CHECK_EQUAL(queue.waiting_producers(), 0U);
    const auto popped = co_await queue.pop(abort_source);
    BOOST_REQUIRE(popped.has_value());
    BOOST_CHECK_EQUAL(*popped, 7);
    BOOST_CHECK_EQUAL(queue.bytes().value(), 0U);
    co_await queue.close(queue_close_mode::drain);
}

SEASTAR_TEST_CASE(
  bounded_work_queue_ready_push_allocation_failure_is_exceptional_future) {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    // Keep this queue unwarmed: its first item insertion allocates storage.
    // The injector targets that push allocation. If item storage becomes
    // preallocated, this fixture needs another real push allocation site.
    bounded_work_queue<int> queue{queue_config(1, 10, 0)};
    seastar::abort_source abort_source;
    std::optional<seastar::future<queue_result<void>>> pushing;
    bool escaped = false;
    auto& injector = seastar::memory::local_failure_injector();
    injector.fail_after(0);
    try {
        pushing.emplace(queue.push(7, byte_count{3}, abort_source));
    } catch (...) {
        escaped = true;
    }
    const bool injected = injector.failed();
    injector.cancel();
    BOOST_CHECK_MESSAGE(
      injected,
      "first push did not allocate; review the unwarmed item-storage fixture");
    BOOST_CHECK(!escaped);
    if (pushing) {
        BOOST_REQUIRE(pushing->available());
        BOOST_CHECK_THROW(std::move(*pushing).get(), std::bad_alloc);
    }
    BOOST_CHECK_EQUAL(queue.size(), 0U);
    BOOST_CHECK_EQUAL(queue.bytes().value(), 0U);
    BOOST_CHECK_EQUAL(queue.accepted_pushes(), 0U);
    BOOST_CHECK_EQUAL(queue.rejected_pushes(), 0U);
    BOOST_REQUIRE(
      (co_await queue.push(8, byte_count{3}, abort_source)).has_value());
    const auto popped = co_await queue.pop(abort_source);
    BOOST_REQUIRE(popped.has_value());
    BOOST_CHECK_EQUAL(*popped, 8);
    co_await queue.close(queue_close_mode::drain);
#endif
    co_return;
}

SEASTAR_TEST_CASE(bounded_work_queue_ready_pop_avoids_coroutine_allocation) {
    bounded_work_queue<int> queue{queue_config(1, 10, 0)};
    seastar::abort_source abort_source;
    BOOST_REQUIRE(
      (co_await queue.push(7, byte_count{3}, abort_source)).has_value());
#if !defined(SEASTAR_DEBUG) && !defined(SEASTAR_DEFAULT_ALLOCATOR)
    const auto before = seastar::memory::stats().mallocs();
#endif
    auto popping = pop_for_allocation_check(queue, abort_source);
#if !defined(SEASTAR_DEBUG) && !defined(SEASTAR_DEFAULT_ALLOCATOR)
    const auto after = seastar::memory::stats().mallocs();
    BOOST_CHECK_EQUAL(after, before);
#endif
#if !defined(SEASTAR_DEBUG)
    BOOST_REQUIRE(popping.available());
#endif
    const auto popped = co_await std::move(popping);
    BOOST_REQUIRE(popped.has_value());
    BOOST_CHECK_EQUAL(*popped, 7);
    BOOST_CHECK_EQUAL(queue.size(), 0U);
    BOOST_CHECK_EQUAL(queue.bytes().value(), 0U);
    BOOST_CHECK_EQUAL(queue.waiting_consumers(), 0U);
    co_await queue.close(queue_close_mode::drain);
}

SEASTAR_TEST_CASE(
  bounded_work_queue_manual_pop_after_workers_bad_alloc_stays_in_future) {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    resource_registry registry;
    co_await registry.start(manager_config());
    resource_manager manager{registry.handles()};
    co_await manager.start();
    {
        bounded_work_queue<int> queue{
          queue_config(1, 10, 0), manager, workload_class::maintenance};
        seastar::abort_source abort_source;
        queue.start_workers(
          bounded_queue_worker_config{
            .workers = 1,
            .maximum_error_reports = 0,
          },
          [](int) { return seastar::make_ready_future<>(); },
          [](std::exception_ptr) noexcept {});
        BOOST_REQUIRE_EQUAL(queue.configured_workers(), 1U);

        // Manual pop is rejected after workers start. Exercise allocation
        // failure in that rejection path, before any consumer admission.
        std::optional<seastar::future<queue_result<int>>> popping;
        bool escaped = false;
        auto& injector = seastar::memory::local_failure_injector();
        injector.fail_after(0);
        try {
            popping.emplace(queue.pop(abort_source));
        } catch (...) {
            escaped = true;
        }
        const bool injected = injector.failed();
        injector.cancel();
        BOOST_CHECK(injected);
        BOOST_CHECK(!escaped);
        if (popping) {
            BOOST_REQUIRE(popping->available());
            BOOST_CHECK_THROW(std::move(*popping).get(), std::bad_alloc);
        }
        BOOST_CHECK_EQUAL(queue.size(), 0U);
        BOOST_CHECK_EQUAL(queue.bytes().value(), 0U);
        BOOST_CHECK_EQUAL(queue.configured_workers(), 1U);
        auto refused = queue.pop(abort_source);
        BOOST_REQUIRE(refused.available());
        BOOST_CHECK_THROW(refused.get(), std::logic_error);
        co_await queue.close(queue_close_mode::abort);
        BOOST_CHECK_EQUAL(queue.active_workers(), 0U);
        BOOST_CHECK_EQUAL(queue.waiting_consumers(), 0U);
    }
    co_await manager.stop();
    co_await registry.stop();
#endif
    co_return;
}

SEASTAR_TEST_CASE(
  bounded_work_queue_ready_pop_preserves_errors_and_queued_item) {
    bounded_work_queue<int> queue{queue_config(1, 10, 0)};
    seastar::abort_source abort_source;
    BOOST_REQUIRE(
      (co_await queue.push(7, byte_count{3}, abort_source)).has_value());
    seastar::abort_source aborted;
    aborted.request_abort();
    const auto rejected = co_await queue.pop(aborted);
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().kind == queue_failure_kind::aborted);
    BOOST_CHECK_EQUAL(queue.size(), 1U);
    BOOST_CHECK_EQUAL(queue.bytes().value(), 3U);
    const auto popped = co_await queue.pop(abort_source);
    BOOST_REQUIRE(popped.has_value());
    BOOST_CHECK_EQUAL(*popped, 7);
    co_await queue.close(queue_close_mode::drain);
    const auto closed = co_await queue.pop(aborted);
    BOOST_REQUIRE(!closed.has_value());
    BOOST_CHECK(closed.error().kind == queue_failure_kind::closed);
}

SEASTAR_TEST_CASE(
  bounded_work_queue_ready_item_does_not_bypass_waiting_consumer) {
    bounded_work_queue<int> queue{queue_config(3, 10, 0)};
    seastar::abort_source abort_source;
    auto first = queue.pop(abort_source);
    BOOST_REQUIRE(!first.available());
    auto push_first = queue.push(10, byte_count{1}, abort_source);
    BOOST_REQUIRE(push_first.available());
    BOOST_REQUIRE(push_first.get().has_value());

    // The first consumer owns the turn, but its wake-up has not run yet.
    auto newcomer = queue.pop(abort_source);
    BOOST_CHECK(!newcomer.available());
    auto push_second = queue.push(11, byte_count{1}, abort_source);
    BOOST_REQUIRE(push_second.available());
    BOOST_REQUIRE(push_second.get().has_value());

    const auto first_item = co_await std::move(first);
    const auto second_item = co_await std::move(newcomer);
    BOOST_REQUIRE(first_item.has_value());
    BOOST_REQUIRE(second_item.has_value());
    BOOST_CHECK_EQUAL(*first_item, 10);
    BOOST_CHECK_EQUAL(*second_item, 11);
    BOOST_CHECK_EQUAL(queue.bytes().value(), 0U);
    co_await queue.close(queue_close_mode::drain);
}

SEASTAR_TEST_CASE(
  bounded_work_queue_bounds_manual_consumers_and_reuses_capacity) {
    bounded_work_queue<int> queue{queue_config(1, 10, 0, 2)};
    seastar::abort_source first_abort;
    seastar::abort_source second_abort;
    seastar::abort_source next_abort;
    auto first = queue.pop(first_abort);
    auto second = queue.pop(second_abort);
    BOOST_CHECK(!first.available());
    BOOST_CHECK(!second.available());
    BOOST_CHECK_EQUAL(queue.waiting_consumers(), 2U);
    auto rejected = queue.pop(next_abort);
    BOOST_REQUIRE(rejected.available());
    const auto exhausted = rejected.get();
    BOOST_REQUIRE(!exhausted.has_value());
    BOOST_CHECK(
      exhausted.error().kind == queue_failure_kind::consumer_waiters_exhausted);
    BOOST_CHECK(exhausted.error().code() == errc::resource_exhausted);
    BOOST_CHECK_EQUAL(queue.waiting_consumers(), 2U);

    first_abort.request_abort();
    const auto canceled = co_await std::move(first);
    BOOST_REQUIRE(!canceled.has_value());
    BOOST_CHECK(canceled.error().kind == queue_failure_kind::aborted);
    BOOST_CHECK_EQUAL(queue.waiting_consumers(), 1U);
    auto next = queue.pop(next_abort);
    BOOST_CHECK(!next.available());
    BOOST_CHECK_EQUAL(queue.waiting_consumers(), 2U);
    auto closing = queue.close(queue_close_mode::abort);
    const auto second_result = co_await std::move(second);
    const auto next_result = co_await std::move(next);
    co_await std::move(closing);
    BOOST_REQUIRE(!second_result.has_value());
    BOOST_REQUIRE(!next_result.has_value());
    BOOST_CHECK(second_result.error().kind == queue_failure_kind::closed);
    BOOST_CHECK(next_result.error().kind == queue_failure_kind::closed);
    BOOST_CHECK_EQUAL(queue.waiting_consumers(), 0U);
    BOOST_CHECK_EQUAL(queue.bytes().value(), 0U);
}

SEASTAR_TEST_CASE(
  bounded_work_queue_zero_manual_waiters_preserves_ready_and_error_paths) {
    BOOST_CHECK(
      !queue_config(
         1,
         10,
         0,
         bounded_work_queue_config::maximum_supported_manual_consumer_waiters
           + 1)
         .validate()
         .has_value());
    bounded_work_queue<int> queue{queue_config(1, 10, 0, 0)};
    seastar::abort_source abort_source;
    const auto empty = co_await queue.pop(abort_source);
    BOOST_REQUIRE(!empty.has_value());
    BOOST_CHECK(
      empty.error().kind == queue_failure_kind::consumer_waiters_exhausted);
    BOOST_REQUIRE(
      (co_await queue.push(42, byte_count{1}, abort_source)).has_value());
    const auto ready = co_await queue.pop(abort_source);
    BOOST_REQUIRE(ready.has_value());
    BOOST_CHECK_EQUAL(*ready, 42);
    abort_source.request_abort();
    const auto aborted = co_await queue.pop(abort_source);
    BOOST_REQUIRE(!aborted.has_value());
    BOOST_CHECK(aborted.error().kind == queue_failure_kind::aborted);
    co_await queue.close(queue_close_mode::abort);
    const auto closed = co_await queue.pop(abort_source);
    BOOST_REQUIRE(!closed.has_value());
    BOOST_CHECK(closed.error().kind == queue_failure_kind::closed);
}

SEASTAR_TEST_CASE(bounded_work_queue_saturates_each_admission_dimension) {
    BOOST_CHECK(!queue_config(0, 10, 1).validate().has_value());
    BOOST_CHECK(!queue_config(1, 1, 1).validate().has_value());
    BOOST_CHECK(!queue_config(1, std::numeric_limits<std::uint64_t>::max(), 1)
                   .validate()
                   .has_value());
    BOOST_CHECK(
      !queue_config(
         1,
         10,
         bounded_work_queue_config::maximum_supported_producer_waiters + 1)
         .validate()
         .has_value());
    BOOST_CHECK((!bounded_queue_worker_config{
      .workers = bounded_queue_worker_config::maximum_supported_workers + 1,
      .maximum_error_reports = 0,
    }
                    .validate()
                    .has_value()));

    bounded_work_queue<int> queue{queue_config(1, 10, 1)};
    seastar::abort_source abort_source;
    auto invalid = co_await queue.push(0, byte_count{}, abort_source);
    BOOST_REQUIRE(!invalid.has_value());
    BOOST_CHECK(invalid.error().kind == queue_failure_kind::invalid_cost);
    BOOST_CHECK(invalid.error().code() == errc::invalid_argument);
    const auto invalid_detail = invalid.error().detail();
    BOOST_CHECK(invalid_detail.code() == errc::invalid_argument);
    BOOST_CHECK_EQUAL(invalid_detail.context_size(), 2U);
    auto oversized = co_await queue.push(0, byte_count{11}, abort_source);
    BOOST_REQUIRE(!oversized.has_value());
    BOOST_CHECK(oversized.error().kind == queue_failure_kind::oversized);

    BOOST_REQUIRE(
      (co_await queue.push(1, byte_count{10}, abort_source)).has_value());
    BOOST_CHECK_EQUAL(queue.size(), 1U);
    BOOST_CHECK_EQUAL(queue.bytes().value(), 10U);

    auto waiting = queue.push(2, byte_count{1}, abort_source);
    co_await seastar::yield();
    BOOST_CHECK(!waiting.available());
    BOOST_CHECK_EQUAL(queue.waiting_producers(), 1U);
    auto excess = co_await queue.push(3, byte_count{1}, abort_source);
    BOOST_REQUIRE(!excess.has_value());
    BOOST_CHECK(
      excess.error().kind == queue_failure_kind::producer_waiters_exhausted);
    BOOST_CHECK_EQUAL(queue.waiting_producers(), 1U);

    auto first = co_await queue.pop(abort_source);
    BOOST_REQUIRE(first.has_value());
    BOOST_CHECK_EQUAL(*first, 1);
    auto waiting_result = co_await std::move(waiting);
    BOOST_REQUIRE(waiting_result.has_value());
    auto second = co_await queue.pop(abort_source);
    BOOST_REQUIRE(second.has_value());
    BOOST_CHECK_EQUAL(*second, 2);
    BOOST_CHECK_EQUAL(queue.size(), 0U);
    BOOST_CHECK_EQUAL(queue.bytes().value(), 0U);
    BOOST_CHECK_EQUAL(queue.accepted_pushes(), 2U);
    BOOST_CHECK_EQUAL(queue.rejected_pushes(), 3U);
    co_await queue.close(queue_close_mode::drain);
}

SEASTAR_TEST_CASE(bounded_work_queue_saturates_items_and_bytes_independently) {
    seastar::abort_source abort_source;
    {
        bounded_work_queue<int> queue{queue_config(1, 10, 1)};
        BOOST_REQUIRE(
          (co_await queue.push(1, byte_count{1}, abort_source)).has_value());
        auto waiting = queue.push(2, byte_count{1}, abort_source);
        co_await seastar::yield();
        BOOST_CHECK(!waiting.available());
        BOOST_CHECK_EQUAL(queue.bytes().value(), 1U);
        BOOST_REQUIRE((co_await queue.pop(abort_source)).has_value());
        auto admitted = co_await std::move(waiting);
        BOOST_REQUIRE(admitted.has_value());
        BOOST_REQUIRE((co_await queue.pop(abort_source)).has_value());
        co_await queue.close(queue_close_mode::drain);
    }
    {
        bounded_work_queue<int> queue{queue_config(10, 10, 1)};
        BOOST_REQUIRE(
          (co_await queue.push(1, byte_count{10}, abort_source)).has_value());
        auto waiting = queue.push(2, byte_count{1}, abort_source);
        co_await seastar::yield();
        BOOST_CHECK(!waiting.available());
        BOOST_CHECK_EQUAL(queue.size(), 1U);
        BOOST_REQUIRE((co_await queue.pop(abort_source)).has_value());
        auto admitted = co_await std::move(waiting);
        BOOST_REQUIRE(admitted.has_value());
        BOOST_REQUIRE((co_await queue.pop(abort_source)).has_value());
        co_await queue.close(queue_close_mode::drain);
    }
}

SEASTAR_TEST_CASE(bounded_work_queue_preserves_fifo_and_aborts_waiters) {
    bounded_work_queue<int> queue{queue_config(1, 10, 3)};
    seastar::abort_source queue_abort;
    BOOST_REQUIRE(
      (co_await queue.push(0, byte_count{6}, queue_abort)).has_value());
    auto first_push = queue.push(1, byte_count{5}, queue_abort);
    auto second_push = queue.push(2, byte_count{4}, queue_abort);
    co_await seastar::yield();
    BOOST_CHECK_EQUAL(queue.waiting_producers(), 2U);

    auto zero = co_await queue.pop(queue_abort);
    BOOST_REQUIRE(zero.has_value());
    BOOST_CHECK_EQUAL(*zero, 0);
    auto first_push_result = co_await std::move(first_push);
    BOOST_REQUIRE(first_push_result.has_value());
    BOOST_CHECK(!second_push.available());
    auto one = co_await queue.pop(queue_abort);
    BOOST_REQUIRE(one.has_value());
    BOOST_CHECK_EQUAL(*one, 1);
    auto second_push_result = co_await std::move(second_push);
    BOOST_REQUIRE(second_push_result.has_value());
    auto two = co_await queue.pop(queue_abort);
    BOOST_REQUIRE(two.has_value());
    BOOST_CHECK_EQUAL(*two, 2);

    auto first_pop = queue.pop(queue_abort);
    auto second_pop = queue.pop(queue_abort);
    co_await seastar::yield();
    BOOST_CHECK_EQUAL(queue.waiting_consumers(), 2U);
    BOOST_REQUIRE(
      (co_await queue.push(10, byte_count{1}, queue_abort)).has_value());
    BOOST_REQUIRE(
      (co_await queue.push(11, byte_count{1}, queue_abort)).has_value());
    auto first_consumer = co_await std::move(first_pop);
    auto second_consumer = co_await std::move(second_pop);
    BOOST_REQUIRE(first_consumer.has_value());
    BOOST_REQUIRE(second_consumer.has_value());
    BOOST_CHECK_EQUAL(*first_consumer, 10);
    BOOST_CHECK_EQUAL(*second_consumer, 11);
    BOOST_CHECK_EQUAL(queue.waiting_consumers(), 0U);

    BOOST_REQUIRE(
      (co_await queue.push(3, byte_count{10}, queue_abort)).has_value());
    seastar::abort_source producer_abort;
    auto aborted_push = queue.push(4, byte_count{1}, producer_abort);
    co_await seastar::yield();
    producer_abort.request_abort();
    auto aborted = co_await std::move(aborted_push);
    BOOST_REQUIRE(!aborted.has_value());
    BOOST_CHECK(aborted.error().kind == queue_failure_kind::aborted);
    BOOST_CHECK_EQUAL(queue.waiting_producers(), 0U);
    auto three = co_await queue.pop(queue_abort);
    BOOST_REQUIRE(three.has_value());
    BOOST_CHECK_EQUAL(*three, 3);
    co_await queue.close(queue_close_mode::drain);
}

SEASTAR_TEST_CASE(bounded_work_queue_close_modes_release_every_unit) {
    {
        bounded_work_queue<int> queue{queue_config(3, 30, 2)};
        seastar::abort_source abort_source;
        BOOST_REQUIRE(
          (co_await queue.push(1, byte_count{10}, abort_source)).has_value());
        BOOST_REQUIRE(
          (co_await queue.push(2, byte_count{10}, abort_source)).has_value());
        auto closing = queue.close(queue_close_mode::drain);
        BOOST_CHECK(!closing.available());
        auto rejected = co_await queue.push(3, byte_count{1}, abort_source);
        BOOST_REQUIRE(!rejected.has_value());
        BOOST_CHECK(rejected.error().kind == queue_failure_kind::closed);
        auto first = co_await queue.pop(abort_source);
        auto second = co_await queue.pop(abort_source);
        BOOST_REQUIRE(first.has_value());
        BOOST_REQUIRE(second.has_value());
        BOOST_CHECK_EQUAL(*first, 1);
        BOOST_CHECK_EQUAL(*second, 2);
        co_await std::move(closing);
        auto closed = co_await queue.pop(abort_source);
        BOOST_REQUIRE(!closed.has_value());
        BOOST_CHECK(closed.error().kind == queue_failure_kind::closed);
        co_await queue.close(queue_close_mode::drain);
    }

    {
        bounded_work_queue<int> queue{queue_config(3, 30, 2)};
        seastar::abort_source abort_source;
        BOOST_REQUIRE(
          (co_await queue.push(1, byte_count{10}, abort_source)).has_value());
        BOOST_REQUIRE(
          (co_await queue.push(2, byte_count{10}, abort_source)).has_value());
        co_await queue.close(queue_close_mode::abort);
        BOOST_CHECK_EQUAL(queue.size(), 0U);
        BOOST_CHECK_EQUAL(queue.bytes().value(), 0U);
        BOOST_CHECK(queue.state() == bounded_work_queue_state::closed);
    }

    {
        bounded_work_queue<int> queue{queue_config(1, 10, 1)};
        seastar::abort_source consumer_abort;
        auto waiting = queue.pop(consumer_abort);
        co_await seastar::yield();
        BOOST_CHECK_EQUAL(queue.waiting_consumers(), 1U);
        consumer_abort.request_abort();
        auto aborted = co_await std::move(waiting);
        BOOST_REQUIRE(!aborted.has_value());
        BOOST_CHECK(aborted.error().kind == queue_failure_kind::aborted);
        BOOST_CHECK_EQUAL(queue.waiting_consumers(), 0U);
        co_await queue.close(queue_close_mode::abort);
    }

    {
        bounded_work_queue<int> queue{queue_config(1, 10, 1)};
        seastar::abort_source abort_source;
        BOOST_REQUIRE(
          (co_await queue.push(1, byte_count{10}, abort_source)).has_value());
        auto waiting = queue.push(2, byte_count{1}, abort_source);
        co_await seastar::yield();
        BOOST_CHECK_EQUAL(queue.waiting_producers(), 1U);
        auto closing = queue.close(queue_close_mode::abort);
        BOOST_CHECK(!closing.available());
        auto rejected = co_await std::move(waiting);
        BOOST_REQUIRE(!rejected.has_value());
        BOOST_CHECK(rejected.error().kind == queue_failure_kind::closed);
        BOOST_CHECK(queue.state() == bounded_work_queue_state::closed);
        co_await std::move(closing);
        BOOST_CHECK_EQUAL(queue.waiting_producers(), 0U);
        BOOST_CHECK_EQUAL(queue.size(), 0U);
        BOOST_CHECK_EQUAL(queue.bytes().value(), 0U);
    }

    {
        bounded_work_queue<int> queue{queue_config(1, 10, 1)};
        seastar::abort_source abort_source;
        BOOST_REQUIRE(
          (co_await queue.push(1, byte_count{10}, abort_source)).has_value());
        auto draining = queue.close(queue_close_mode::drain);
        BOOST_CHECK(!draining.available());
        auto aborting = queue.close(queue_close_mode::abort);
        co_await std::move(draining);
        co_await std::move(aborting);
        BOOST_CHECK(queue.state() == bounded_work_queue_state::closed);
        BOOST_CHECK_EQUAL(queue.size(), 0U);
        BOOST_CHECK_EQUAL(queue.bytes().value(), 0U);
    }

    {
        bounded_work_queue<int> queue{queue_config(1, 10, 1)};
        seastar::abort_source abort_source;
        auto waiting = queue.pop(abort_source);
        co_await seastar::yield();
        auto closing = queue.close(queue_close_mode::abort);
        auto closed = co_await std::move(waiting);
        BOOST_REQUIRE(!closed.has_value());
        BOOST_CHECK(closed.error().kind == queue_failure_kind::closed);
        co_await std::move(closing);
    }
}

SEASTAR_TEST_CASE(bounded_work_queue_bounds_both_producer_populations) {
    // A queue that permits no suspended producers rejects immediately rather
    // than suspending the producer that holds the admission turn.
    {
        bounded_work_queue<int> queue{queue_config(1, 10, 0)};
        seastar::abort_source abort_source;
        BOOST_REQUIRE(
          (co_await queue.push(1, byte_count{10}, abort_source)).has_value());
        auto refused = co_await queue.push(2, byte_count{10}, abort_source);
        BOOST_REQUIRE(!refused.has_value());
        BOOST_CHECK(
          refused.error().kind
          == queue_failure_kind::producer_waiters_exhausted);
        BOOST_CHECK_EQUAL(queue.waiting_producers(), 0U);
        auto first = co_await queue.pop(abort_source);
        BOOST_REQUIRE(first.has_value());
        co_await queue.close(queue_close_mode::drain);
    }

    // One slot is reserved for the turn holder, so the two populations together
    // never exceed the configured bound and the producer at the head of the
    // admission order is never asked to compete for a slot again.
    bounded_work_queue<int> queue{queue_config(1, 10, 2)};
    seastar::abort_source abort_source;
    BOOST_REQUIRE(
      (co_await queue.push(1, byte_count{10}, abort_source)).has_value());

    auto head = queue.push(2, byte_count{10}, abort_source);
    co_await seastar::yield();
    BOOST_CHECK_EQUAL(queue.admitting_producers(), 1U);
    BOOST_CHECK_EQUAL(queue.queued_producers(), 0U);

    auto queued = queue.push(3, byte_count{10}, abort_source);
    co_await seastar::yield();
    BOOST_CHECK_EQUAL(queue.admitting_producers(), 1U);
    BOOST_CHECK_EQUAL(queue.queued_producers(), 1U);
    BOOST_CHECK_EQUAL(queue.waiting_producers(), 2U);

    auto refused = co_await queue.push(4, byte_count{10}, abort_source);
    BOOST_REQUIRE(!refused.has_value());
    BOOST_CHECK(
      refused.error().kind == queue_failure_kind::producer_waiters_exhausted);
    BOOST_CHECK_EQUAL(queue.waiting_producers(), 2U);

    auto first = co_await queue.pop(abort_source);
    BOOST_REQUIRE(first.has_value());
    BOOST_CHECK_EQUAL(*first, 1);
    auto head_result = co_await std::move(head);
    BOOST_REQUIRE(head_result.has_value());

    // The queued producer inherits the turn and moves into the reserved slot
    // rather than taking a second one. Reaching that point takes several
    // reactor hops: the turn grant, the units continuation, and the resumed
    // push coroutine.
    while (queue.admitting_producers() != 1) {
        co_await seastar::yield();
    }
    BOOST_CHECK_EQUAL(queue.admitting_producers(), 1U);
    BOOST_CHECK_EQUAL(queue.queued_producers(), 0U);

    auto second = co_await queue.pop(abort_source);
    BOOST_REQUIRE(second.has_value());
    BOOST_CHECK_EQUAL(*second, 2);
    auto queued_result = co_await std::move(queued);
    BOOST_REQUIRE(queued_result.has_value());
    auto third = co_await queue.pop(abort_source);
    BOOST_REQUIRE(third.has_value());
    BOOST_CHECK_EQUAL(*third, 3);
    BOOST_CHECK_EQUAL(queue.waiting_producers(), 0U);
    BOOST_CHECK_EQUAL(queue.bytes().value(), 0U);
    co_await queue.close(queue_close_mode::drain);
}

SEASTAR_TEST_CASE(
  managed_workers_wait_when_manual_consumer_waiting_is_disabled) {
    resource_registry registry;
    co_await registry.start(manager_config());
    resource_manager manager{registry.handles()};
    co_await manager.start();
    {
        bounded_work_queue<int> queue{
          queue_config(2, 20, 0, 0), manager, workload_class::maintenance};
        queue.start_workers(
          bounded_queue_worker_config{.workers = 2, .maximum_error_reports = 0},
          [](int) { return seastar::make_ready_future<>(); },
          [](std::exception_ptr) noexcept {});
        const auto deadline = seastar::lowres_clock::now()
                              + std::chrono::seconds{10};
        while (queue.waiting_consumers() != 2
               && seastar::lowres_clock::now() < deadline) {
            co_await seastar::yield();
        }
        const auto waiting = queue.waiting_consumers();
        co_await queue.close(queue_close_mode::abort);
        BOOST_CHECK_EQUAL(waiting, 2U);
        BOOST_CHECK_EQUAL(queue.waiting_consumers(), 0U);
        BOOST_CHECK_EQUAL(queue.active_workers(), 0U);
    }
    co_await manager.stop();
    co_await registry.stop();
}

SEASTAR_TEST_CASE(bounded_work_queue_integrates_and_isolates_workers) {
    resource_registry registry;
    co_await registry.start(manager_config());
    resource_manager manager{registry.handles()};
    co_await manager.start();

    {
        bounded_work_queue<int> idle_queue{
          queue_config(1, 10, 0), manager, workload_class::maintenance};
        BOOST_CHECK_THROW(
          idle_queue.start_workers(
            bounded_queue_worker_config{
              .workers = 0,
              .maximum_error_reports = 0,
            },
            [](int) { return seastar::make_ready_future<>(); },
            [](std::exception_ptr) noexcept {}),
          std::invalid_argument);
        co_await idle_queue.close(queue_close_mode::abort);
    }

    bounded_work_queue<int> queue{
      queue_config(8, 80, 4), manager, workload_class::maintenance};
    seastar::abort_source admission_abort;
    for (int item = 1; item <= 6; ++item) {
        BOOST_REQUIRE(
          (co_await queue.push(item, byte_count{10}, admission_abort))
            .has_value());
    }

    seastar::shared_promise<> release;
    std::array<seastar::promise<>, 2> handler_started;
    auto first_handler_started = handler_started[0].get_future();
    auto second_handler_started = handler_started[1].get_future();
    std::vector<int> completed;
    completed.reserve(4);
    std::uint64_t reporter_calls = 0;
    bool scheduling_group_observed = true;
    std::optional<workload_handle> observed_workload{
      manager.acquire_workload(workload_class::maintenance)};
    const auto maintenance_group = observed_workload->scheduling_group();
    queue.start_workers(
      bounded_queue_worker_config{
        .workers = 2,
        .maximum_error_reports = 1,
      },
      [&release,
       &handler_started,
       &completed,
       maintenance_group,
       &scheduling_group_observed](int item) -> seastar::future<> {
          scheduling_group_observed = scheduling_group_observed
                                      && seastar::current_scheduling_group()
                                           == maintenance_group;
          if (item == 1 || item == 2) {
              handler_started[static_cast<std::size_t>(item - 1)].set_value();
          }
          co_await release.get_shared_future();
          if (item == 3 || item == 4) {
              throw optional_item_failure{};
          }
          completed.push_back(item);
      },
      [&reporter_calls](std::exception_ptr) noexcept { ++reporter_calls; },
      [](const std::exception_ptr& failure) noexcept {
          try {
              std::rethrow_exception(failure);
          } catch (const optional_item_failure&) {
              return true;
          } catch (...) {
              return false;
          }
      });
    BOOST_CHECK_THROW(
      queue.start_workers(
        bounded_queue_worker_config{
          .workers = 1,
          .maximum_error_reports = 0,
        },
        [](int) { return seastar::make_ready_future<>(); },
        [](std::exception_ptr) noexcept {}),
      std::logic_error);

    co_await std::move(first_handler_started);
    co_await std::move(second_handler_started);
    BOOST_CHECK_EQUAL(queue.configured_workers(), 2U);
    BOOST_CHECK_EQUAL(queue.active_workers(), 2U);
    BOOST_CHECK_EQUAL(queue.active_handlers(), 2U);
    BOOST_CHECK_EQUAL(queue.bytes().value(), 60U);
    BOOST_CHECK_EQUAL(
      manager.memory_used(workload_class::maintenance).value(), 60U);
    auto closing = queue.close(queue_close_mode::drain);
    BOOST_CHECK(!closing.available());
    release.set_value();
    co_await std::move(closing);

    std::ranges::sort(completed);
    BOOST_CHECK_EQUAL(completed.size(), 4U);
    BOOST_CHECK_EQUAL(completed[0], 1);
    BOOST_CHECK_EQUAL(completed[1], 2);
    BOOST_CHECK_EQUAL(completed[2], 5);
    BOOST_CHECK_EQUAL(completed[3], 6);
    BOOST_CHECK(scheduling_group_observed);
    BOOST_CHECK_EQUAL(queue.maximum_active_handlers(), 2U);
    BOOST_CHECK_EQUAL(queue.active_handlers(), 0U);
    BOOST_CHECK_EQUAL(queue.active_workers(), 0U);
    BOOST_CHECK_EQUAL(queue.reported_errors(), 1U);
    BOOST_CHECK_EQUAL(queue.suppressed_errors(), 1U);
    BOOST_CHECK_EQUAL(reporter_calls, 1U);
    BOOST_CHECK_EQUAL(queue.size(), 0U);
    BOOST_CHECK_EQUAL(queue.bytes().value(), 0U);

    co_await queue.close(queue_close_mode::drain);
    observed_workload.reset();
    co_await manager.stop();
    co_await registry.stop();
}

SEASTAR_TEST_CASE(managed_queue_aborts_native_memory_waiters) {
    resource_registry registry;
    co_await registry.start(manager_config());
    resource_manager manager{registry.handles()};
    co_await manager.start();

    const auto classification = workload_class::metadata;
    const auto budget = manager.hard_budget(classification);
    const auto direct_size = budget.checked_sub(byte_count{1});
    BOOST_REQUIRE(direct_size.has_value());
    std::optional<workload_handle> direct_workload{
      manager.acquire_workload(classification)};
    auto& admission = direct_workload->memory_admission();
    auto direct = seastar::try_get_units(admission, direct_size->value());
    BOOST_REQUIRE(direct.has_value());

    {
        bounded_work_queue<int> queue{
          queue_config(2, budget.value(), 1), manager, classification};
        seastar::abort_source caller_abort;
        auto waiting = queue.push(1, byte_count{2}, caller_abort);
        while (admission.waiters() != 1) {
            co_await seastar::yield();
        }
        caller_abort.request_abort();
        auto aborted = co_await std::move(waiting);
        BOOST_REQUIRE(!aborted.has_value());
        BOOST_CHECK(aborted.error().kind == queue_failure_kind::aborted);
        BOOST_CHECK_EQUAL(admission.waiters(), 0U);
        co_await queue.close(queue_close_mode::drain);
    }

    {
        bounded_work_queue<int> queue{
          queue_config(2, budget.value(), 1), manager, classification};
        seastar::abort_source caller_abort;
        auto waiting = queue.push(1, byte_count{2}, caller_abort);
        while (admission.waiters() != 1) {
            co_await seastar::yield();
        }
        auto closing = queue.close(queue_close_mode::abort);
        auto closed = co_await std::move(waiting);
        BOOST_REQUIRE(!closed.has_value());
        BOOST_CHECK(closed.error().kind == queue_failure_kind::closed);
        co_await std::move(closing);
        BOOST_CHECK_EQUAL(admission.waiters(), 0U);
    }

    direct->return_all();
    direct_workload.reset();
    co_await manager.stop();
    co_await registry.stop();
}

SEASTAR_TEST_CASE(queues_share_their_workload_class_memory_admission) {
    resource_registry registry;
    co_await registry.start(manager_config());
    resource_manager manager{registry.handles()};
    co_await manager.start();

    const auto classification = workload_class::metadata;
    const auto budget = manager.hard_budget(classification);
    const auto direct_size = budget.checked_sub(byte_count{2});
    BOOST_REQUIRE(direct_size.has_value());
    std::optional<workload_handle> direct_workload{
      manager.acquire_workload(classification)};
    auto direct = seastar::try_get_units(
      direct_workload->memory_admission(), direct_size->value());
    BOOST_REQUIRE(direct.has_value());

    bounded_work_queue<int> first{
      queue_config(2, budget.value(), 1), manager, classification};
    bounded_work_queue<int> second{
      queue_config(2, budget.value(), 1), manager, classification};
    seastar::abort_source abort_source;

    BOOST_REQUIRE(
      (co_await first.push(1, byte_count{1}, abort_source)).has_value());
    BOOST_CHECK_EQUAL(
      manager.memory_used(classification).value(), budget.value() - 1);

    auto waiting = second.push(2, byte_count{2}, abort_source);
    co_await seastar::yield();
    BOOST_CHECK(!waiting.available());
    BOOST_CHECK_EQUAL(second.waiting_producers(), 1U);

    auto first_item = co_await first.pop(abort_source);
    BOOST_REQUIRE(first_item.has_value());
    BOOST_CHECK_EQUAL(*first_item, 1);
    BOOST_CHECK_EQUAL(
      manager.memory_used(classification).value(), budget.value());
    auto admitted = co_await std::move(waiting);
    BOOST_REQUIRE(admitted.has_value());
    BOOST_CHECK_EQUAL(second.bytes().value(), 2U);
    BOOST_CHECK_EQUAL(
      manager.memory_used(classification).value(), budget.value());

    auto second_item = co_await second.pop(abort_source);
    BOOST_REQUIRE(second_item.has_value());
    BOOST_CHECK_EQUAL(*second_item, 2);
    direct->return_all();
    direct_workload.reset();
    BOOST_CHECK_EQUAL(manager.memory_used(classification).value(), 0U);

    co_await first.close(queue_close_mode::drain);
    co_await second.close(queue_close_mode::drain);
    co_await manager.stop();
    co_await registry.stop();
}

} // namespace kwaque::resource
