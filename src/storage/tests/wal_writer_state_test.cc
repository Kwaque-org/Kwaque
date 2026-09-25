#include "src/bytes/test_allocation_profile.h"
#include "src/resource/resource_registry.h"
#include "src/storage/wal_writer_state.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <boost/test/unit_test.hpp>

namespace {
using namespace kwaque;
using namespace kwaque::storage;
constexpr auto workload = resource::workload_class::metadata;
template<typename Func>
seastar::future<> with_budget(Func body) {
    auto config = resource::resource_config::from_total_memory(
      byte_count{seastar::memory::stats().total_memory()});
    BOOST_REQUIRE(config.has_value());
    resource::resource_registry registry;
    co_await registry.start(*config);
    resource::resource_manager manager{registry.handles()};
    std::exception_ptr failure;
    try {
        co_await manager.start();
        co_await seastar::futurize_invoke(body, manager);
    } catch (...) {
        failure = std::current_exception();
    }
    co_await manager.stop();
    co_await registry.stop();
    if (failure) std::rethrow_exception(failure);
}

auto extent(std::uint64_t begin) {
    return model::file_byte_span::from_size(
             runtime::file_position{begin}, byte_count{4096})
      .value();
}
auto descriptor(workload_budget& budget, std::uint64_t begin) {
    return kwaque::storage::detail::wal_write_descriptor::make(
             budget, extent(begin), codec::limits::defaults(), byte_count{4096})
      .value();
}
void encoded(
  kwaque::storage::detail::wal_write_descriptor& node,
  workload_budget& budget,
  char value) {
    auto bytes
      = bytes::fragmented_buffer::copy_of(std::string(4096, value)).value();
    auto held = budget.try_reserve_buffer(bytes).value();
    const auto accepted = node.encoded(
      std::move(bytes), std::move(held), bytes::testing::charge);
    BOOST_REQUIRE(accepted);
}
} // namespace

SEASTAR_TEST_CASE(wal_descriptors_keep_execution_and_retire_only_the_front) {
    co_await with_budget([](auto& manager) -> seastar::future<> {
        workload_budget budget{
          manager.acquire_workload(workload),
          {.tasks = 16, .bytes = byte_count{1U << 20U}, .handles = 1},
          bytes::testing::charge};
        {
            kwaque::storage::detail::wal_descriptor_queue queue{
              2, byte_count{8192}};
            auto first = descriptor(budget, 8192);
            auto second = descriptor(budget, 12288);
            BOOST_REQUIRE(queue.push(first));
            BOOST_REQUIRE(queue.push(second));
            auto pending = first->observe();
            // Discarding interest does not cancel the independent execution.
            {
                auto detached = second->observe();
            }
            auto rejected = queue.push(descriptor(budget, 16384));
            BOOST_REQUIRE(!rejected);
            BOOST_CHECK(rejected.error().code() == errc::queue_full);
            encoded(*second, budget, 'b');
            second->dispatch();
            second->complete(byte_count{4096});
            BOOST_CHECK(!queue.retire_front());
            BOOST_CHECK(!pending.available());
            BOOST_CHECK(!second->execution_abort().abort_requested());
            encoded(*first, budget, 'a');
            first->dispatch();
            seastar::promise<> release;
            auto execute =
              [](auto node, seastar::future<> wait) -> seastar::future<> {
                co_await std::move(wait);
                node->complete(byte_count{4096});
            }(first, release.get_future());
            first = {};
            BOOST_CHECK(!execute.available());
            release.set_value();
            co_await std::move(execute);
            auto retired = queue.retire_front();
            BOOST_REQUIRE(retired);
            BOOST_CHECK_EQUAL(queue.size(), 1U);
            BOOST_CHECK(
              retired->payload().content_equals(std::string(4096, 'a')));
            auto result = co_await std::move(pending);
            BOOST_CHECK(!result.failure.failed());
            BOOST_CHECK_EQUAL(result.written.value(), 4096U);
            BOOST_REQUIRE(queue.retire_front());
            BOOST_CHECK(queue.empty());
            BOOST_CHECK_EQUAL(queue.bytes().value(), 0U);
            // Ready completion, with the observer installed after notification.
            auto ready = descriptor(budget, 16384);
            BOOST_REQUIRE(queue.push(ready));
            encoded(*ready, budget, 'c');
            ready->dispatch();
            ready->complete(byte_count{4096});
            BOOST_REQUIRE(queue.retire_front());
            auto observed = ready->observe();
            BOOST_CHECK(observed.available());
            const auto observed_result = co_await std::move(observed);
            BOOST_CHECK(!observed_result.failure.failed());
        }
        BOOST_CHECK_EQUAL(budget.snapshot().tasks, 0U);
        BOOST_CHECK_EQUAL(budget.snapshot().bytes, 0U);
    });
}

SEASTAR_TEST_CASE(wal_descriptors_preserve_short_write_and_exception_failures) {
    co_await with_budget([](auto& manager) -> seastar::future<> {
        workload_budget budget{
          manager.acquire_workload(workload),
          {.tasks = 16, .bytes = byte_count{1U << 20U}, .handles = 1},
          bytes::testing::charge};
        for (bool exceptional : {false, true}) {
            kwaque::storage::detail::wal_descriptor_queue queue{
              1, byte_count{4096}};
            auto node = descriptor(budget, 8192);
            BOOST_REQUIRE(queue.push(node));
            auto completion = node->observe();
            if (exceptional)
                node->fail(std::make_exception_ptr(std::bad_alloc{}));
            else {
                encoded(*node, budget, 'x');
                node->dispatch();
                node->complete(byte_count{4095});
            }
            node->fail(
              runtime::operation_error{
                errc::aborted, runtime::operation_kind::file});
            BOOST_REQUIRE(queue.retire_front());
            auto result = co_await std::move(completion);
            BOOST_CHECK_EQUAL(result.written.value(), 0U);
            if (exceptional)
                BOOST_CHECK_THROW(
                  static_cast<void>(result.failure.outcome()), std::bad_alloc);
            else
                BOOST_CHECK(result.failure.error()->code() == errc::io_failure);
        }
        BOOST_CHECK_EQUAL(budget.snapshot().tasks, 0U);
    });
}

SEASTAR_TEST_CASE(wal_gather_selects_only_a_bounded_adjacent_ready_prefix) {
    co_await with_budget([](auto& manager) {
        workload_budget budget{
          manager.acquire_workload(workload),
          {.tasks = 16, .bytes = byte_count{1U << 20U}, .handles = 1},
          bytes::testing::charge};
        {
            kwaque::storage::detail::wal_descriptor_queue queue{
              2, byte_count{8192}};
            auto first = descriptor(budget, 8192),
                 second = descriptor(budget, 12288);
            BOOST_REQUIRE(queue.push(first));
            BOOST_REQUIRE(queue.push(second));
            encoded(*second, budget, 'b');
            BOOST_CHECK_EQUAL(queue.ready_prefix().groups, 0U);
            encoded(*first, budget, 'a');
            BOOST_CHECK_EQUAL(queue.ready_prefix().groups, 2U);
            BOOST_CHECK_EQUAL(queue.ready_prefix(byte_count{4096}).groups, 1U);
            BOOST_CHECK_EQUAL(
              queue.ready_prefix(byte_count{8192}, 1).groups, 1U);
            BOOST_CHECK_EQUAL(queue.ready_prefix(byte_count{4095}).groups, 0U);
            first->dispatch();
            BOOST_CHECK_EQUAL(queue.ready_prefix().groups, 0U);
            first->complete(byte_count{4096});
            BOOST_REQUIRE(queue.retire_front());
            BOOST_CHECK_EQUAL(queue.ready_prefix().groups, 1U);
            second->dispatch();
            second->complete(byte_count{4096});
            BOOST_REQUIRE(queue.retire_front());
        }
        BOOST_CHECK_EQUAL(budget.snapshot().tasks, 0U);
    });
}

SEASTAR_TEST_CASE(wal_descriptor_construction_failure_leaves_no_admission) {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    co_await with_budget([](auto& manager) -> seastar::future<> {
        workload_budget budget{
          manager.acquire_workload(workload),
          {.tasks = 8, .bytes = byte_count{1U << 20U}, .handles = 1},
          bytes::testing::charge};
        for (std::size_t at = 0; at != 8; ++at) {
            auto& injector = seastar::memory::local_failure_injector();
            bool failed = false;
            injector.fail_after(at);
            try {
                auto node = descriptor(budget, 8192);
            } catch (const std::bad_alloc&) {
                failed = true;
            }
            const bool injected = injector.failed();
            injector.cancel();
            BOOST_CHECK_EQUAL(failed, injected);
            BOOST_CHECK_EQUAL(budget.snapshot().tasks, 0U);
            BOOST_CHECK_EQUAL(budget.snapshot().bytes, 0U);
        }
        co_return;
    });
#endif
    co_return;
}

SEASTAR_TEST_CASE(wal_failed_descriptor_link_has_no_queue_effect_or_observer) {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    co_await with_budget([](auto& manager) -> seastar::future<> {
        workload_budget budget{
          manager.acquire_workload(workload),
          {.tasks = 8, .bytes = byte_count{1U << 20U}, .handles = 1},
          bytes::testing::charge};
        {
            kwaque::storage::detail::wal_descriptor_queue queue{
              1, byte_count{4096}};
            auto node = descriptor(budget, 8192);
            const auto before = budget.snapshot();
            auto& injector = seastar::memory::local_failure_injector();
            bool failed = false;
            injector.fail_after(0);
            try {
                static_cast<void>(queue.push(node));
            } catch (const std::bad_alloc&) {
                failed = true;
            }
            const bool injected = injector.failed();
            injector.cancel();
            BOOST_CHECK(failed && injected);
            BOOST_CHECK(queue.empty());
            BOOST_CHECK_EQUAL(queue.bytes().value(), 0U);
            BOOST_CHECK_EQUAL(budget.snapshot().bytes, before.bytes);
            BOOST_REQUIRE(queue.push(node));
            auto observed = node->observe();
            encoded(*node, budget, 'x');
            node->dispatch();
            node->complete(byte_count{4096});
            BOOST_REQUIRE(queue.retire_front());
            auto result = co_await std::move(observed);
            BOOST_CHECK(!result.failure.failed());
        }
        BOOST_CHECK_EQUAL(budget.snapshot().tasks, 0U);
    });
#endif
    co_return;
}
