#include "src/base/allocation.h"
#include "src/base/metric_schema.h"
#include "src/bytes/test_allocation_profile.h"
#include "src/resource/bounded_work_queue.h"
#include "src/resource/resource_registry.h"
#include "src/runtime/cross_shard.h"
#include "src/storage/metrics.h"
#include "src/storage/operation_scope.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/metrics_api.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <exception>
#include <limits>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {
using namespace kwaque;
using namespace kwaque::storage;
constexpr auto workload = resource::workload_class::metadata;
static_assert(!runtime::cross_shard_value<workload_reservation>);
static_assert(std::is_nothrow_move_constructible_v<workload_reservation>);

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
workload_budget_limits limits() { return {4, byte_count{64U * 1024U}, 2}; }

seastar::future<> exercise_task_capture_lifetime(workload_budget& budget) {
    seastar::promise<> entered, release;
    bool destroyed = false, completed = false;
    operation_scope scope{budget, {}};
    auto lifetime = seastar::defer([&destroyed] noexcept { destroyed = true; });
    const auto accepted = scope.spawn(
      byte_count{4096},
      [lifetime = std::move(lifetime), &entered, &release, &completed] mutable
        -> seastar::future<runtime::result<void>> {
          static_cast<void>(lifetime);
          // Copy the external address before suspension so even a premature
          // capture destruction is detected without dereferencing freed memory.
          auto* done = &completed;
          auto waiting = release.get_future();
          entered.set_value();
          co_await std::move(waiting);
          *done = true;
          co_return runtime::result<void>{};
      });
    bool retained = false;
    if (accepted) {
        co_await entered.get_future();
        retained = !destroyed;
        release.set_value();
    }
    const auto closed = co_await scope.close();
    BOOST_CHECK(accepted.has_value() && closed.has_value());
    BOOST_CHECK(retained && completed && destroyed);
    BOOST_CHECK_EQUAL(budget.snapshot().tasks, 0U);
    BOOST_CHECK_EQUAL(budget.snapshot().bytes, 0U);
}
} // namespace

SEASTAR_TEST_CASE(storage_scope_retains_coroutine_captures_across_suspension) {
    co_await with_budget([](auto& manager) -> seastar::future<> {
        workload_budget budget{
          manager.acquire_workload(workload), limits(), bytes::testing::charge};
        BOOST_CHECK(
          seastar::current_scheduling_group() != budget.scheduling_group());
        co_await exercise_task_capture_lifetime(budget);
        co_await seastar::with_scheduling_group(
          budget.scheduling_group(),
          [&budget] { return exercise_task_capture_lifetime(budget); });
    });
}

SEASTAR_TEST_CASE(
  storage_budget_rejects_impossible_requests_and_rolls_back_pressure) {
    co_await with_budget([](auto& manager) -> seastar::future<> {
        workload_budget budget{
          manager.acquire_workload(workload), limits(), bytes::testing::charge};
        auto over = budget.try_reserve(byte_count{UINT64_MAX});
        BOOST_REQUIRE(!over.has_value());
        BOOST_CHECK(over.error().code() == errc::out_of_range);
        {
            auto reservation = budget.try_reserve(byte_count{1});
            BOOST_REQUIRE(reservation.has_value());
            const auto handles = reservation->try_acquire_handles(3);
            BOOST_REQUIRE(!handles.has_value());
            BOOST_CHECK(handles.error().code() == errc::out_of_range);
        }
        auto observer = manager.acquire_workload(workload);
        const auto available = observer.memory_admission().current();
        {
            auto full = seastar::try_get_units(
              observer.memory_admission(), available);
            BOOST_REQUIRE(full.has_value());
            BOOST_CHECK(!budget.try_reserve(byte_count{1024}).has_value());
            BOOST_CHECK_EQUAL(budget.snapshot().tasks, 0U);
            BOOST_CHECK_EQUAL(budget.snapshot().bytes, 0U);
            BOOST_CHECK_EQUAL(observer.memory_admission().waiters(), 0U);
        }
        BOOST_CHECK_EQUAL(observer.memory_admission().current(), available);
        {
            auto first = budget.try_reserve(byte_count{100});
            BOOST_REQUIRE(first.has_value());
            BOOST_CHECK(first->try_acquire_handles(2).has_value());
            {
                auto second = budget.try_reserve(byte_count{100});
                BOOST_REQUIRE(second.has_value());
                BOOST_CHECK(!second->try_acquire_handles(1).has_value());
            }
            BOOST_CHECK_EQUAL(budget.snapshot().tasks, 1U);
            BOOST_CHECK_EQUAL(budget.snapshot().handles, 2U);
        }
        BOOST_CHECK_EQUAL(budget.snapshot().bytes, 0U);
        {
            std::array<std::optional<workload_reservation>, 4> tasks;
            for (auto& task : tasks) {
                auto admitted = budget.try_reserve(byte_count{1});
                BOOST_REQUIRE(admitted.has_value());
                task.emplace(std::move(*admitted));
            }
            const auto full = budget.try_reserve(byte_count{1});
            BOOST_REQUIRE(!full.has_value());
            BOOST_CHECK(full.error().code() == errc::queue_full);
            BOOST_CHECK_EQUAL(budget.snapshot().tasks, 4U);
        }
        BOOST_CHECK_EQUAL(budget.snapshot().bytes, 0U);
        budget.close_admission();
        BOOST_CHECK(!budget.try_reserve(byte_count{1}).has_value());
        co_return;
    });
}

SEASTAR_TEST_CASE(
  storage_queue_pop_and_output_alias_keep_workload_backing_charged) {
    co_await with_budget([](auto& manager) -> seastar::future<> {
        auto observer = manager.acquire_workload(workload);
        const auto available = observer.memory_admission().current();
        std::optional<workload_reservation> output;
        {
            workload_budget budget{
              manager.acquire_workload(workload),
              limits(),
              bytes::testing::charge};
            resource::bounded_work_queue<workload_reservation> queue{
              {item_count{1}, byte_count{1}}};
            seastar::abort_source abort;
            auto admitted = budget.try_reserve(byte_count{4096});
            BOOST_REQUIRE(admitted.has_value());
            BOOST_REQUIRE(admitted->try_acquire_handles(1).has_value());
            const auto charge = admitted->bytes().value();
            auto pushed = co_await queue.push(
              std::move(*admitted), byte_count{1}, abort);
            {
                auto popped = co_await queue.pop(abort);
                BOOST_REQUIRE(popped.has_value());
                output.emplace(popped->share());
                BOOST_CHECK_EQUAL(queue.bytes().value(), 0U);
                BOOST_CHECK_EQUAL(budget.snapshot().bytes, charge);
            }
            co_await queue.close(resource::queue_close_mode::drain);
            BOOST_CHECK(pushed.has_value());
            BOOST_CHECK_EQUAL(budget.snapshot().bytes, charge);
        }
        BOOST_CHECK(observer.memory_admission().current() < available);
        output.reset();
        BOOST_CHECK_EQUAL(observer.memory_admission().current(), available);
    });
}

SEASTAR_TEST_CASE(
  storage_budget_allocation_failure_refunds_every_acquired_unit) {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    co_await with_budget([](auto& manager) -> seastar::future<> {
        workload_budget budget{
          manager.acquire_workload(workload), limits(), bytes::testing::charge};
        auto observer = manager.acquire_workload(workload);
        const auto available = observer.memory_admission().current();
        auto& injector = seastar::memory::local_failure_injector();
        bool failed = false;
        injector.fail_after(0);
        try {
            static_cast<void>(budget.try_reserve(byte_count{4096}));
        } catch (const std::bad_alloc&) {
            failed = true;
        }
        const bool injected = injector.failed();
        injector.cancel();
        BOOST_CHECK(failed && injected);
        BOOST_CHECK_EQUAL(budget.snapshot().tasks, 0U);
        BOOST_CHECK_EQUAL(budget.snapshot().bytes, 0U);
        BOOST_CHECK_EQUAL(budget.snapshot().handles, 0U);
        BOOST_CHECK_EQUAL(observer.memory_admission().current(), available);
        co_return;
    });
#endif
    co_return;
}

SEASTAR_TEST_CASE(
  storage_scope_drains_before_cleanup_and_preserves_typed_first_failure) {
    co_await with_budget([](auto& manager) -> seastar::future<> {
        workload_budget budget{
          manager.acquire_workload(workload), limits(), bytes::testing::charge};
        seastar::promise<> release;
        bool finished = false, cleaned_after_work = false;
        runtime::operation_error first{
          errc::io_failure, runtime::operation_kind::file};
        operation_scope scope{
          budget, [&] -> seastar::future<runtime::result<void>> {
              cleaned_after_work = finished;
              co_return runtime::failure(
                runtime::operation_error{
                  errc::permission_denied, runtime::operation_kind::file});
          }};
        auto started = scope.spawn(
          byte_count{4096}, [&] -> seastar::future<runtime::result<void>> {
              co_await release.get_future();
              finished = true;
              co_return runtime::failure(first);
          });
        auto closing = scope.close();
        auto overlap = co_await scope.close();
        const bool pending = !closing.available();
        release.set_value();
        auto closed = co_await std::move(closing);
        auto again = co_await scope.close();
        BOOST_CHECK(started.has_value());
        BOOST_CHECK(pending && cleaned_after_work);
        BOOST_CHECK(!overlap.has_value());
        BOOST_REQUIRE(!closed.has_value());
        BOOST_REQUIRE(!again.has_value());
        BOOST_CHECK(closed.error() == first);
        BOOST_CHECK(again.error() == first);
        const auto stats = scope.statistics().snapshot();
        BOOST_CHECK_EQUAL(stats.operations.completed, 1U);
        BOOST_CHECK_EQUAL(stats.failed, 1U);
        BOOST_CHECK_EQUAL(stats.succeeded, 0U);
        BOOST_CHECK_EQUAL(stats.durable, 0U);
        BOOST_CHECK_EQUAL(budget.snapshot().tasks, 0U);
    });
}

SEASTAR_TEST_CASE(storage_scope_parent_abort_wakes_accepted_work_once) {
    co_await with_budget([](auto& manager) -> seastar::future<> {
        workload_budget budget{
          manager.acquire_workload(workload), limits(), bytes::testing::charge};
        seastar::abort_source parent;
        seastar::promise<> wake;
        unsigned wakes = 0, cleanups = 0;
        bool finished = false, right_group = false;
        operation_scope scope{
          budget, [&] -> seastar::future<runtime::result<void>> {
              ++cleanups;
              right_group = seastar::current_scheduling_group()
                            == budget.scheduling_group();
              co_return runtime::result<void>{};
          }};
        const auto bound = scope.bind_shutdown(parent, [&] noexcept {
            ++wakes;
            wake.set_value();
        });
        const auto duplicate = scope.bind_shutdown(parent, [] {});
        const auto started = scope.spawn(
          byte_count{4096}, [&] -> seastar::future<runtime::result<void>> {
              co_await wake.get_future();
              finished = true;
              co_return runtime::result<void>{};
          });
        parent.request_abort();
        parent.request_abort();
        const bool admission_closed = scope.admission_closed();
        const auto closed = co_await scope.close();
        const auto repeated = co_await scope.close();
        BOOST_CHECK(bound.has_value());
        BOOST_CHECK(
          !duplicate && duplicate.error().code() == errc::invalid_argument);
        BOOST_CHECK(started.has_value());
        BOOST_CHECK(admission_closed && finished && right_group);
        BOOST_CHECK(closed.has_value() && repeated.has_value());
        BOOST_CHECK_EQUAL(wakes, 1U);
        BOOST_CHECK_EQUAL(cleanups, 1U);
        BOOST_CHECK_EQUAL(budget.snapshot().tasks, 0U);
    });
}

SEASTAR_TEST_CASE(storage_scope_handles_already_aborted_and_destroyed_parents) {
    co_await with_budget([](auto& manager) -> seastar::future<> {
        for (const bool already_aborted : {false, true}) {
            workload_budget budget{
              manager.acquire_workload(workload),
              limits(),
              bytes::testing::charge};
            std::optional<seastar::abort_source> parent{std::in_place};
            unsigned wakes = 0;
            operation_scope scope{budget, {}};
            if (already_aborted) parent->request_abort();
            const auto bound = scope.bind_shutdown(
              *parent, [&] noexcept { ++wakes; });
            const bool initially_closed = scope.admission_closed();
            parent.reset();
            const auto closed = co_await scope.close();
            BOOST_CHECK(bound.has_value());
            BOOST_CHECK_EQUAL(initially_closed, already_aborted);
            BOOST_CHECK(closed.has_value());
            BOOST_CHECK_EQUAL(wakes, 1U);
        }
    });
}

SEASTAR_TEST_CASE(
  storage_scope_native_live_frames_fit_admitted_control_memory) {
#if defined(SEASTAR_HEAPPROF) && !defined(SEASTAR_DEFAULT_ALLOCATOR)
    co_await with_budget([](auto& manager) -> seastar::future<> {
        workload_budget budget{
          manager.acquire_workload(workload), limits(), bytes::testing::charge};
        seastar::promise<> entered, release;
        std::array<seastar::memory::allocation_site, 64> sites;
        static_assert(sizeof(sites) <= maximum_contiguous_allocation_bytes);
        std::size_t count = 0;
        std::uint64_t live_bytes = 0, admitted_bytes = 0;
        bool admitted = false;
        {
            seastar::memory::scoped_heap_profiling profiling{1};
            operation_scope scope{budget, {}};
            const auto accepted = scope.spawn(
              byte_count{16384}, [&] -> seastar::future<runtime::result<void>> {
                  entered.set_value();
                  co_await release.get_future();
                  co_return runtime::result<void>{};
              });
            admitted = accepted.has_value();
            if (admitted) {
                co_await entered.get_future();
                count = seastar::memory::sampled_memory_profile(
                  sites.data(), sites.size());
                for (std::size_t i = 0; i < count; ++i)
                    live_bytes += sites[i].size;
                admitted_bytes = budget.snapshot().bytes;
                release.set_value();
            }
            static_cast<void>(co_await scope.close());
        }
        BOOST_CHECK(admitted);
        BOOST_CHECK_LT(count, sites.size());
        BOOST_CHECK_LE(live_bytes, admitted_bytes);
        BOOST_CHECK_EQUAL(budget.snapshot().bytes, 0U);
        BOOST_TEST_MESSAGE(
          "live control bytes=" << live_bytes
                                << " admitted bytes=" << admitted_bytes);
    });
#else
    BOOST_TEST_MESSAGE(
      "Live-frame accounting requires the native heap-profiling build.");
#endif
    co_return;
}

SEASTAR_TEST_CASE(storage_metrics_register_bounded_families_and_rollback_oom) {
    co_await with_budget([](auto& manager) -> seastar::future<> {
        workload_budget budget{
          manager.acquire_workload(workload), limits(), bytes::testing::charge};
        storage_statistics statistics;
        const std::array sources{metric_source{&statistics, &budget}};
        storage_metrics metrics{sources};
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
        bool reached_success = false, saw_failure = false;
        for (unsigned allocation = 0; allocation < 1024 && !reached_success;
             ++allocation) {
            auto& injector = seastar::memory::local_failure_injector();
            injector.fail_after(allocation);
            try {
                metrics.start();
                reached_success = true;
            } catch (const std::bad_alloc&) {
                saw_failure = true;
            }
            injector.cancel();
            if (!reached_success) {
                BOOST_CHECK(!metrics.registered());
                const auto& map = seastar::metrics::impl::get_value_map();
                BOOST_CHECK(map.find("storage_active") == map.end());
            }
        }
        BOOST_CHECK(reached_success && saw_failure);
#else
        metrics.start();
#endif
        {
            auto success = statistics.accept();
            success.finish(storage_outcome::success);
            auto failed = statistics.accept();
            failed.finish(storage_outcome::error);
            auto uncertain = statistics.accept();
        }
        statistics.observe_durable();
        const auto snapshot = statistics.snapshot();
        BOOST_CHECK_EQUAL(snapshot.operations.completed, 3U);
        BOOST_CHECK_EQUAL(snapshot.succeeded, 1U);
        BOOST_CHECK_EQUAL(snapshot.failed, 1U);
        BOOST_CHECK_EQUAL(snapshot.uncertain, 1U);
        BOOST_CHECK_EQUAL(snapshot.durable, 1U);
        const auto& map = seastar::metrics::impl::get_value_map();
        unsigned families = 0;
        for (const auto& [name, family] : map) {
            if (!std::string_view{name.data(), name.size()}.starts_with(
                  "storage_"))
                continue;
            ++families;
            BOOST_CHECK_EQUAL(family.size(), 1U);
            for (const auto& value : family)
                BOOST_CHECK_EQUAL(value.first.labels().size(), 1U);
        }
        BOOST_CHECK_EQUAL(families, 11U);
        metrics.stop();
        BOOST_CHECK(map.find("storage_active") == map.end());
        co_return;
    });
}

SEASTAR_TEST_CASE(
  storage_buffer_cost_covers_backing_and_alias_descriptor_ownership) {
    co_await with_budget([](auto& manager) -> seastar::future<> {
        workload_budget budget{
          manager.acquire_workload(workload), limits(), bytes::testing::charge};
        std::optional<bytes::fragmented_buffer> output;
        std::optional<workload_reservation> output_charge;
        {
            const std::string payload(4096, 'a');
            auto made = bytes::fragmented_buffer::copy_of(
              std::span{payload.data(), payload.size()});
            BOOST_REQUIRE(made.has_value());
            auto input = std::move(*made);
            auto first = budget.try_reserve_buffer(input, byte_count{256});
            auto second = budget.try_reserve_buffer(input);
            BOOST_REQUIRE(first.has_value());
            BOOST_REQUIRE(second.has_value());
            BOOST_CHECK(first->bytes() > input.retained_bytes());
            output_charge.emplace(std::move(*second));
            output.emplace(input.share());
        }
        BOOST_REQUIRE(output.has_value());
        BOOST_CHECK_EQUAL(output->size().value(), 4096U);
        BOOST_CHECK_EQUAL(
          budget.snapshot().bytes, output_charge->bytes().value());
        output.reset();
        output_charge.reset();
        BOOST_CHECK_EQUAL(budget.snapshot().bytes, 0U);
        co_return;
    });
}

SEASTAR_TEST_CASE(
  storage_scope_preserves_exception_identity_through_cleanup_failure) {
    co_await with_budget([](auto& manager) -> seastar::future<> {
        workload_budget budget{
          manager.acquire_workload(workload), limits(), bytes::testing::charge};
        const auto first = std::make_exception_ptr(std::bad_alloc{});
        operation_scope scope{
          budget, [] -> seastar::future<runtime::result<void>> {
              co_return runtime::failure(
                runtime::operation_error{
                  errc::io_failure, runtime::operation_kind::file});
          }};
        auto started = scope.spawn(
          byte_count{4096}, [first] -> seastar::future<runtime::result<void>> {
              return seastar::make_exception_future<runtime::result<void>>(
                first);
          });
        for (unsigned attempt = 0; attempt < 2; ++attempt) {
            std::exception_ptr failure;
            try {
                static_cast<void>(co_await scope.close());
            } catch (...) {
                failure = std::current_exception();
            }
            BOOST_CHECK(failure == first);
        }
        BOOST_CHECK(started.has_value());
        BOOST_CHECK_EQUAL(scope.statistics().snapshot().uncertain, 1U);
        BOOST_CHECK_EQUAL(scope.statistics().snapshot().operations.active, 0U);
    });
}

SEASTAR_TEST_CASE(
  storage_buffer_cost_errors_preserve_cause_without_reserving_units) {
    co_await with_budget([](auto& manager) -> seastar::future<> {
        struct cost_case {
            bytes::allocation_charge_fn profile;
            errc expected;
        };
        const std::array cases{
          cost_case{
            +[](byte_count request) noexcept {
                return request.value() == 4096
                         ? byte_count{4095}
                         : bytes::testing::charge(request);
            },
            errc::invalid_argument},
          cost_case{
            +[](byte_count request) noexcept {
                return request.value() == 4096
                         ? byte_count{UINT64_MAX}
                         : bytes::testing::charge(request);
            },
            errc::out_of_range}};
        const std::string payload(4096, 'a');
        auto input = bytes::fragmented_buffer::copy_of(
          std::span{payload.data(), payload.size()});
        BOOST_REQUIRE(input.has_value());
        auto observer = manager.acquire_workload(workload);
        const auto available = observer.memory_admission().current();
        const auto retained = input->retained_bytes();
        for (const auto& test : cases) {
            workload_budget budget{
              manager.acquire_workload(workload), limits(), test.profile};
            const auto source = input->allocation_cost(test.profile);
            BOOST_REQUIRE(!source.has_value());
            BOOST_CHECK(source.error() == test.expected);
            const auto rejected = budget.try_reserve_buffer(*input);
            BOOST_REQUIRE(!rejected.has_value());
            BOOST_CHECK(rejected.error().code() == test.expected);
            BOOST_CHECK(
              rejected.error().operation()
              == runtime::operation_kind::resource);
            const auto state = budget.snapshot();
            BOOST_CHECK_EQUAL(state.accepted, 0U);
            BOOST_CHECK_EQUAL(state.rejected, 1U);
            BOOST_CHECK_EQUAL(state.tasks, 0U);
            BOOST_CHECK_EQUAL(state.bytes, 0U);
            BOOST_CHECK_EQUAL(state.handles, 0U);
            BOOST_CHECK_EQUAL(observer.memory_admission().current(), available);
            BOOST_CHECK(input->retained_bytes() == retained);
            BOOST_CHECK(input->content_equals(payload));
        }
        co_return;
    });
}
