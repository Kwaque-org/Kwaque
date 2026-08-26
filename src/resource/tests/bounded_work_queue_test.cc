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
#include <seastar/core/memory.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

namespace kwaque::resource {

namespace {

bounded_work_queue_config queue_config(
  std::uint64_t items, std::uint64_t bytes, std::size_t producer_waiters) {
    return bounded_work_queue_config{
      .maximum_items = item_count{items},
      .maximum_bytes = byte_count{bytes},
      .maximum_producer_waiters = producer_waiters,
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

} // namespace

SEASTAR_TEST_CASE(bounded_work_queue_saturates_each_admission_dimension) {
    BOOST_CHECK(!queue_config(0, 10, 1).validate().has_value());
    BOOST_CHECK(!queue_config(1, 1, 1).validate().has_value());
    BOOST_CHECK(!queue_config(1, std::numeric_limits<std::uint64_t>::max(), 1)
                   .validate()
                   .has_value());

    bounded_work_queue<int> queue{queue_config(1, 10, 1)};
    seastar::abort_source abort_source;
    auto invalid = co_await queue.push(0, byte_count{}, abort_source);
    BOOST_REQUIRE(!invalid.has_value());
    BOOST_CHECK(invalid.error().kind == queue_failure_kind::invalid_cost);
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
    co_await queue.close(queue_close_mode::drain);
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
    for (std::size_t attempt = 0;
         attempt < 1024 && queue.admitting_producers() != 1;
         ++attempt) {
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

SEASTAR_TEST_CASE(bounded_queue_workers_limit_and_isolate_handlers) {
    resource_registry registry;
    co_await registry.start(manager_config());
    resource_manager manager{registry.handles()};
    co_await manager.start();

    {
        bounded_work_queue<int> idle_queue{queue_config(1, 10, 0)};
        bounded_queue_workers<int> idle_workers{
          idle_queue,
          manager,
          workload_class::maintenance,
          bounded_queue_worker_config{
            .workers = 1,
            .maximum_error_reports = 0,
          },
          [](int) { return seastar::make_ready_future<>(); },
          [](std::exception_ptr) noexcept {}};
        co_await idle_queue.close(queue_close_mode::abort);
    }

    bounded_work_queue<int> queue{queue_config(8, 80, 4)};
    seastar::abort_source admission_abort;
    for (int item = 1; item <= 6; ++item) {
        BOOST_REQUIRE(
          (co_await queue.push(item, byte_count{10}, admission_abort))
            .has_value());
    }

    seastar::shared_promise<> release;
    std::vector<int> completed;
    completed.reserve(4);
    std::uint64_t reporter_calls = 0;
    bool scheduling_group_observed = true;
    bounded_queue_workers<int> workers{
      queue,
      manager,
      workload_class::maintenance,
      bounded_queue_worker_config{
        .workers = 2,
        .maximum_error_reports = 1,
      },
      [&release, &completed, &manager, &scheduling_group_observed](
        int item) -> seastar::future<> {
          scheduling_group_observed = scheduling_group_observed
                                      && seastar::current_scheduling_group()
                                           == manager.scheduling_group(
                                             workload_class::maintenance);
          co_await release.get_shared_future();
          if (item == 3 || item == 4) {
              throw std::runtime_error("synthetic handler failure");
          }
          completed.push_back(item);
      },
      [&reporter_calls](std::exception_ptr) noexcept { ++reporter_calls; }};
    workers.start();

    for (std::size_t attempt = 0;
         attempt < 1024 && workers.active_handlers() != 2;
         ++attempt) {
        co_await seastar::yield();
    }
    BOOST_CHECK_EQUAL(workers.configured_workers(), 2U);
    BOOST_CHECK_EQUAL(workers.active_workers(), 2U);
    BOOST_CHECK_EQUAL(workers.active_handlers(), 2U);
    auto closing = workers.close(queue_close_mode::drain);
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
    BOOST_CHECK_EQUAL(workers.maximum_active_handlers(), 2U);
    BOOST_CHECK_EQUAL(workers.active_handlers(), 0U);
    BOOST_CHECK_EQUAL(workers.active_workers(), 0U);
    BOOST_CHECK_EQUAL(workers.reported_errors(), 1U);
    BOOST_CHECK_EQUAL(workers.suppressed_errors(), 1U);
    BOOST_CHECK_EQUAL(reporter_calls, 1U);
    BOOST_CHECK_EQUAL(queue.size(), 0U);
    BOOST_CHECK_EQUAL(queue.bytes().value(), 0U);

    co_await workers.close(queue_close_mode::drain);
    co_await manager.stop();
    co_await registry.stop();
}

} // namespace kwaque::resource
