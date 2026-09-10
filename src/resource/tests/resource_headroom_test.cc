#include "src/base/units.h"
#include "src/resource/bounded_work_queue.h"
#include "src/resource/resource_config.h"
#include "src/resource/resource_manager.h"
#include "src/resource/resource_registry.h"
#include "src/resource/workload_class.h"
#include "src/runtime/task_scope.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <exception>
#include <optional>
#include <stdexcept>
#include <utility>

namespace kwaque::resource {

#ifndef SEASTAR_DEFAULT_ALLOCATOR
namespace {

using payload = seastar::temporary_buffer<char>;
using queue = bounded_work_queue<payload>;
constexpr std::uint64_t payload_chunk_bytes = 64ULL * 1024ULL;
constexpr std::size_t parked_tasks = 256;

// All retained payload is physically allocated and touched. Semaphore units
// alone cannot measure native allocator metadata, coroutine frames or queues.
class saturation_fixture final {
public:
    explicit saturation_fixture(resource_manager& manager)
      : manager_(manager) {}

    seastar::future<> fill() {
        for (const auto classification : all_workload_classes) {
            const auto index = workload_index(classification);
            const auto capacity = manager_.hard_budget(classification);
            const auto items = capacity.value() / payload_chunk_bytes
                               + (capacity.value() % payload_chunk_bytes != 0);
            producers_[index].emplace(
              bounded_work_queue_config{
                .maximum_items = item_count{items},
                .maximum_bytes = capacity,
                .maximum_producer_waiters
                = bounded_work_queue_config::maximum_supported_producer_waiters,
                .maximum_manual_consumer_waiters = 0},
              manager_,
              classification);
            auto remaining = capacity.value();
            while (remaining != 0) {
                const auto amount = std::min(remaining, payload_chunk_bytes);
                payload bytes{amount};
                std::memset(bytes.get_write(), 0x5a, bytes.size());
                const auto pushed = co_await producers_[index]->push(
                  std::move(bytes), byte_count{amount}, abort_);
                if (!pushed) {
                    throw std::runtime_error("payload admission failed");
                }
                charged_bytes_ += amount;
                remaining -= amount;
                co_await seastar::maybe_yield();
            }
            for (std::size_t waiter = 0; waiter < bounded_work_queue_config::
                                           maximum_supported_producer_waiters;
                 ++waiter) {
                pending_pushes_.push_back(
                  producers_[index]->push(payload{}, byte_count{1}, abort_));
            }

            consumers_[index].emplace(
              bounded_work_queue_config{
                .maximum_items = item_count{1},
                .maximum_bytes = byte_count{2},
                .maximum_producer_waiters = 0,
                .maximum_manual_consumer_waiters = bounded_work_queue_config::
                  maximum_supported_manual_consumer_waiters},
              manager_,
              classification);
            for (std::size_t waiter = 0;
                 waiter < bounded_work_queue_config::
                   maximum_supported_manual_consumer_waiters;
                 ++waiter) {
                pending_pops_.push_back(consumers_[index]->pop(abort_));
            }

            workers_[index].emplace(
              bounded_work_queue_config{
                .maximum_items = item_count{1},
                .maximum_bytes = byte_count{2},
                .maximum_producer_waiters = 0,
                .maximum_manual_consumer_waiters = 0},
              manager_,
              classification);
            workers_[index]->start_workers(
              bounded_queue_worker_config{
                .workers
                = bounded_queue_worker_config::maximum_supported_workers,
                .maximum_error_reports = 0},
              [](payload) { return seastar::make_ready_future<>(); },
              [](std::exception_ptr) noexcept {});
        }
        for (std::size_t task = 0; task < parked_tasks; ++task) {
            if (!tasks_.spawn(
                  [this] { return release_.get_shared_future(); })) {
                throw std::runtime_error("parked task admission failed");
            }
        }
        co_await seastar::yield();
        for (const auto classification : all_workload_classes) {
            const auto index = workload_index(classification);
            BOOST_CHECK_EQUAL(
              manager_.memory_available(classification).value(), 0U);
            BOOST_CHECK_EQUAL(
              producers_[index]->waiting_producers(),
              bounded_work_queue_config::maximum_supported_producer_waiters);
            BOOST_CHECK_EQUAL(
              consumers_[index]->waiting_consumers(),
              bounded_work_queue_config::
                maximum_supported_manual_consumer_waiters);
            BOOST_CHECK_EQUAL(
              workers_[index]->active_workers(),
              bounded_queue_worker_config::maximum_supported_workers);
        }
        BOOST_CHECK_EQUAL(tasks_.task_count(), parked_tasks);
    }

    seastar::future<> close() {
        release_.set_value();
        abort_.request_abort();
        for (auto* queues : {&producers_, &consumers_, &workers_}) {
            for (auto& item : *queues) {
                if (item) {
                    co_await item->close(queue_close_mode::abort);
                    item.reset();
                }
            }
        }
        for (auto& pending : pending_pushes_) {
            static_cast<void>(co_await std::move(pending));
        }
        for (auto& pending : pending_pops_) {
            static_cast<void>(co_await std::move(pending));
        }
        pending_pushes_.clear();
        pending_pops_.clear();
        co_await tasks_.close();
    }

    [[nodiscard]] std::uint64_t charged_bytes() const noexcept {
        return charged_bytes_;
    }

private:
    resource_manager& manager_;
    seastar::abort_source abort_;
    std::array<std::optional<queue>, workload_class_count> producers_;
    std::array<std::optional<queue>, workload_class_count> consumers_;
    std::array<std::optional<queue>, workload_class_count> workers_;
    std::deque<seastar::future<queue_result<void>>> pending_pushes_;
    std::deque<seastar::future<queue_result<payload>>> pending_pops_;
    runtime::task_scope tasks_;
    seastar::shared_promise<> release_;
    std::uint64_t charged_bytes_{0};
};

} // namespace
#endif

SEASTAR_TEST_CASE(
  resource_headroom_covers_native_payload_and_metadata_saturation) {
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    BOOST_TEST_MESSAGE(
      "native headroom measurement unavailable with system allocator");
    co_return;
#else
    const auto before = seastar::memory::stats();
    const auto configuration = resource_config::from_production_memory(
      byte_count{static_cast<std::uint64_t>(before.total_memory())},
      memory_reservations{
        .reactor_headroom = resource_config::default_reactor_headroom(),
        .admin_memory = byte_count{}});
    BOOST_REQUIRE(configuration.has_value());
    resource_registry registry;
    co_await registry.start(*configuration);
    resource_manager manager{registry.handles()};
    co_await manager.start();

    std::exception_ptr failure;
    std::uint64_t charged_bytes = 0;
    std::uint64_t allocated_bytes = 0;
    std::uint64_t native_unaccounted_bytes = 0;
    {
        saturation_fixture fixture{manager};
        try {
            co_await fixture.fill();
            const auto peak = seastar::memory::stats();
            charged_bytes = fixture.charged_bytes();
            allocated_bytes = peak.allocated_memory();
            BOOST_CHECK_GE(allocated_bytes, charged_bytes);
            native_unaccounted_bytes = allocated_bytes >= charged_bytes
                                         ? allocated_bytes - charged_bytes
                                         : 0;
            BOOST_CHECK_LE(
              native_unaccounted_bytes,
              configuration->reactor_headroom().value());
            BOOST_CHECK_EQUAL(
              peak.failed_allocations(), before.failed_allocations());
            BOOST_CHECK_EQUAL(
              peak.fallback_allocations(), before.fallback_allocations());
            auto control = manager.acquire_workload(
              workload_class::consensus_critical);
            bool progressed = false;
            co_await seastar::with_scheduling_group(
              control.scheduling_group(), [&progressed] { progressed = true; });
            BOOST_CHECK(progressed);
        } catch (...) {
            failure = std::current_exception();
        }
        co_await fixture.close();
    }
    co_await manager.stop();
    co_await registry.stop();
    const auto after = seastar::memory::stats();
    BOOST_CHECK_LE(
      after.allocated_memory(), configuration->reactor_headroom().value());
    BOOST_TEST_MESSAGE(
      "native headroom: before="
      << before.allocated_memory() << " peak=" << allocated_bytes << " charged="
      << charged_bytes << " uncharged=" << native_unaccounted_bytes
      << " after=" << after.allocated_memory()
      << " reserve=" << configuration->reactor_headroom().value());
    if (failure) {
        std::rethrow_exception(failure);
    }
#endif
}

} // namespace kwaque::resource
