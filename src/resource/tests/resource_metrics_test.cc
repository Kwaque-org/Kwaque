#include "src/base/metric_schema.h"
#include "src/base/units.h"
#include "src/resource/bounded_work_queue.h"
#include "src/resource/bounded_work_queue_metrics.h"
#include "src/resource/resource_config.h"
#include "src/resource/resource_manager.h"
#include "src/resource/resource_registry.h"
#include "src/resource/workload_class.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_api.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <cstdint>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>

namespace {

const kwaque::metric_descriptor& metric_descriptor(kwaque::metric_id id) {
    const auto* descriptor = kwaque::descriptor_for(id);
    if (descriptor == nullptr) {
        throw std::logic_error("metric descriptor is missing");
    }
    return *descriptor;
}

kwaque::resource::resource_config config() {
    auto made = kwaque::resource::resource_config::from_total_memory(
      kwaque::byte_count{
        static_cast<std::uint64_t>(seastar::memory::stats().total_memory())});
    if (!made) {
        throw std::runtime_error("resource metric configuration was rejected");
    }
    return *made;
}

seastar::sstring full_name(kwaque::metric_id id) {
    const auto& descriptor = metric_descriptor(id);
    auto result = seastar::sstring{std::string{descriptor.group}};
    result += seastar::sstring{"_"};
    result += seastar::sstring{std::string{descriptor.name}};
    return result;
}

const seastar::metrics::impl::metric_family& family(kwaque::metric_id id) {
    const auto found = seastar::metrics::impl::get_value_map().find(
      full_name(id));
    if (found == seastar::metrics::impl::get_value_map().end()) {
        throw std::logic_error("metric family is not registered");
    }
    return found->second;
}

void require_exact_family(kwaque::metric_id id, std::size_t instances) {
    const auto& descriptor = metric_descriptor(id);
    const auto& registered = family(id);
    BOOST_CHECK(
      registered.info().type
      == (descriptor.kind == kwaque::metric_value_kind::gauge ? seastar::metrics::impl::data_type::GAUGE : seastar::metrics::impl::data_type::COUNTER));
    BOOST_CHECK(registered.info().d.str() == seastar::sstring{descriptor.help});
    BOOST_REQUIRE_EQUAL(registered.info().aggregate_labels.size(), 1U);
    BOOST_CHECK_EQUAL(registered.info().aggregate_labels.front(), "shard");
    BOOST_CHECK_EQUAL(registered.size(), instances);
    for (const auto& entry : registered) {
        const auto& labels = entry.first.labels();
        BOOST_REQUIRE(labels.contains("shard"));
        BOOST_CHECK(
          labels.size()
          == (descriptor.labels == kwaque::metric_label_domain::workload ? 2U : 1U));
        for (const auto& label : labels) {
            BOOST_CHECK(
              label.first == "shard"
              || (descriptor.labels == kwaque::metric_label_domain::workload
                  && label.first
                       == seastar::sstring{kwaque::metric_workload_label}));
        }
    }
}

std::uint64_t metric_value(kwaque::metric_id id) {
    const auto& registered = family(id);
    BOOST_REQUIRE_EQUAL(registered.size(), 1U);
    return registered.begin()->second->get_function()().ui();
}

std::uint64_t
workload_metric_value(kwaque::metric_id id, std::string_view workload) {
    for (const auto& entry : family(id)) {
        const auto& labels = entry.first.labels();
        if (
          labels.at(seastar::sstring{kwaque::metric_workload_label}).value()
          == seastar::sstring{workload}) {
            return entry.second->get_function()().ui();
        }
    }
    BOOST_FAIL("workload metric instance was not registered");
    return 0;
}

} // namespace

SEASTAR_TEST_CASE(resource_metrics_have_exact_fixed_workload_cardinality) {
    for (const auto classification : kwaque::resource::all_workload_classes) {
        const auto index = kwaque::resource::workload_index(classification);
        BOOST_CHECK(
          kwaque::resource::descriptor_for(classification).metric_name
          == kwaque::metric_workload_label_values[index]);
    }
    kwaque::resource::resource_registry registry;
    co_await registry.start(config());
    kwaque::resource::resource_manager manager{registry.handles()};
    co_await manager.start();

    for (std::uint16_t value = static_cast<std::uint16_t>(
           kwaque::metric_id::memory_configured_bytes);
         value <= static_cast<std::uint16_t>(kwaque::metric_id::memory_waiters);
         ++value) {
        require_exact_family(
          static_cast<kwaque::metric_id>(value),
          kwaque::metric_workload_values);
    }

    std::set<std::string> workloads;
    for (const auto& entry :
         family(kwaque::metric_id::memory_configured_bytes)) {
        const auto& labels = entry.first.labels();
        const auto& value
          = labels.at(seastar::sstring{kwaque::metric_workload_label}).value();
        workloads.emplace(value.data(), value.size());
    }
    BOOST_CHECK_EQUAL(workloads.size(), kwaque::metric_workload_values);
    for (const auto value : kwaque::metric_workload_label_values) {
        BOOST_CHECK(workloads.contains(std::string{value}));
    }

    constexpr auto classification = kwaque::resource::workload_class::metadata;
    constexpr auto workload = std::string_view{"metadata"};
    const auto budget = manager.hard_budget(classification).value();
    BOOST_CHECK_EQUAL(
      workload_metric_value(
        kwaque::metric_id::memory_configured_bytes, workload),
      budget);
    BOOST_CHECK_EQUAL(
      workload_metric_value(kwaque::metric_id::memory_used_bytes, workload),
      0U);
    {
        auto handle = manager.acquire_workload(classification);
        auto units = seastar::try_get_units(handle.memory_admission(), 16);
        BOOST_REQUIRE(units.has_value());
        BOOST_CHECK_EQUAL(
          workload_metric_value(kwaque::metric_id::memory_used_bytes, workload),
          16U);
        BOOST_CHECK_EQUAL(
          workload_metric_value(
            kwaque::metric_id::memory_available_bytes, workload),
          budget - 16U);
    }
    BOOST_CHECK_EQUAL(
      workload_metric_value(kwaque::metric_id::memory_used_bytes, workload),
      0U);
    {
        auto handle = manager.acquire_workload(classification);
        auto units = seastar::try_get_units(handle.memory_admission(), budget);
        BOOST_REQUIRE(units.has_value());
        seastar::abort_source abort_source;
        auto waiting = seastar::get_units(
          handle.memory_admission(), 1, abort_source);
        co_await seastar::yield();
        BOOST_CHECK(!waiting.available());
        BOOST_CHECK_EQUAL(
          workload_metric_value(kwaque::metric_id::memory_waiters, workload),
          1U);
        abort_source.request_abort();
        bool aborted = false;
        try {
            static_cast<void>(co_await std::move(waiting));
        } catch (const seastar::abort_requested_exception&) {
            aborted = true;
        }
        BOOST_REQUIRE(aborted);
        BOOST_CHECK_EQUAL(
          workload_metric_value(kwaque::metric_id::memory_waiters, workload),
          0U);
    }

    co_await manager.stop();
    for (std::uint16_t value = static_cast<std::uint16_t>(
           kwaque::metric_id::memory_configured_bytes);
         value <= static_cast<std::uint16_t>(kwaque::metric_id::memory_waiters);
         ++value) {
        BOOST_CHECK(!seastar::metrics::impl::get_value_map().contains(
          full_name(static_cast<kwaque::metric_id>(value))));
    }
    co_await registry.stop();
    co_return;
}

SEASTAR_TEST_CASE(queue_metrics_sum_one_compile_time_queue_set) {
    using queue_type = kwaque::resource::bounded_work_queue<int>;
    queue_type first{kwaque::resource::bounded_work_queue_config{
      .maximum_items = kwaque::item_count{1},
      .maximum_bytes = kwaque::byte_count{2},
      .maximum_producer_waiters = 0,
    }};
    queue_type second{kwaque::resource::bounded_work_queue_config{
      .maximum_items = kwaque::item_count{1},
      .maximum_bytes = kwaque::byte_count{2},
      .maximum_producer_waiters = 0,
    }};
    kwaque::resource::bounded_work_queue_metrics metrics{first, second};
    static_assert(!std::is_copy_constructible_v<decltype(metrics)>);
    static_assert(!std::is_move_constructible_v<decltype(metrics)>);

    metrics.start();
    for (std::uint16_t value = static_cast<std::uint16_t>(
           kwaque::metric_id::queue_items);
         value <= static_cast<std::uint16_t>(
           kwaque::metric_id::queue_handler_failures_suppressed_total);
         ++value) {
        require_exact_family(static_cast<kwaque::metric_id>(value), 1U);
    }

    seastar::abort_source abort_source;
    BOOST_REQUIRE((co_await first.push(1, kwaque::byte_count{1}, abort_source))
                    .has_value());
    BOOST_REQUIRE((co_await second.push(2, kwaque::byte_count{1}, abort_source))
                    .has_value());
    BOOST_CHECK(!(co_await first.push(3, kwaque::byte_count{1}, abort_source))
                   .has_value());
    BOOST_CHECK_EQUAL(metric_value(kwaque::metric_id::queue_items), 2U);
    BOOST_CHECK_EQUAL(metric_value(kwaque::metric_id::queue_bytes), 2U);
    BOOST_CHECK_EQUAL(
      metric_value(kwaque::metric_id::queue_push_accepted_total), 2U);
    BOOST_CHECK_EQUAL(
      metric_value(kwaque::metric_id::queue_push_rejected_total), 1U);

    BOOST_REQUIRE((co_await first.pop(abort_source)).has_value());
    BOOST_REQUIRE((co_await second.pop(abort_source)).has_value());
    co_await first.close(kwaque::resource::queue_close_mode::drain);
    co_await second.close(kwaque::resource::queue_close_mode::drain);
    metrics.stop();
    for (std::uint16_t value = static_cast<std::uint16_t>(
           kwaque::metric_id::queue_items);
         value <= static_cast<std::uint16_t>(
           kwaque::metric_id::queue_handler_failures_suppressed_total);
         ++value) {
        BOOST_CHECK(!seastar::metrics::impl::get_value_map().contains(
          full_name(static_cast<kwaque::metric_id>(value))));
    }
    co_return;
}
