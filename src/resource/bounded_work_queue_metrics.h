#ifndef KWAQUE_SRC_RESOURCE_BOUNDED_WORK_QUEUE_METRICS_H_
#define KWAQUE_SRC_RESOURCE_BOUNDED_WORK_QUEUE_METRICS_H_

#include "src/base/invariant.h"
#include "src/base/metric_schema.h"
#include "src/runtime/shard_affinity.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_registration.hh>

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <tuple>
#include <utility>
#include <vector>

namespace kwaque::resource {

template<typename Queue>
concept bounded_queue_metric_source = requires(const Queue& queue) {
    { queue.size() } -> std::convertible_to<std::size_t>;
    { queue.bytes().value() } -> std::convertible_to<std::uint64_t>;
    { queue.waiting_producers() } -> std::convertible_to<std::size_t>;
    { queue.waiting_consumers() } -> std::convertible_to<std::size_t>;
    { queue.active_handlers() } -> std::convertible_to<std::size_t>;
    { queue.accepted_pushes() } -> std::convertible_to<std::uint64_t>;
    { queue.rejected_pushes() } -> std::convertible_to<std::uint64_t>;
    { queue.reported_errors() } -> std::convertible_to<std::uint64_t>;
    { queue.suppressed_errors() } -> std::convertible_to<std::uint64_t>;
    { queue.owner() } -> std::same_as<runtime::owner_shard>;
};

// Registers one fixed aggregate over a compile-time queue set. Queue hot paths
// retain direct owner-local integer updates; scrape callbacks only read and sum
// the explicitly bound queues. Every bound queue must outlive this owner and
// its registration interval.
template<bounded_queue_metric_source... Queues>
requires(sizeof...(Queues) > 0)
class bounded_work_queue_metrics final : public runtime::shard_affine {
public:
    explicit bounded_work_queue_metrics(Queues&... queues)
      : queues_(&queues...) {
        KWAQUE_INVARIANT(
          invariant_id{"KQ-QUEUE-METRIC-SHARD"},
          ((queues.owner() == owner()) && ...),
          "queue metric sources have different owners");
    }

    bounded_work_queue_metrics(const bounded_work_queue_metrics&) = delete;
    bounded_work_queue_metrics&
    operator=(const bounded_work_queue_metrics&) = delete;
    bounded_work_queue_metrics(bounded_work_queue_metrics&&) = delete;
    bounded_work_queue_metrics&
    operator=(bounded_work_queue_metrics&&) = delete;

    void start() {
        assert_current();
        if (metrics_) {
            throw std::logic_error("queue metrics are already registered");
        }
        namespace metrics = seastar::metrics;
        try {
            metrics_.emplace();
            std::vector<metrics::metric_definition> definitions;
            definitions.reserve(9);
            const std::vector<metrics::label> aggregate{metrics::shard_label};
            const auto add_gauge = [&definitions,
                                    &aggregate](metric_id id, auto value) {
                const auto& descriptor = *descriptor_for(id);
                definitions.emplace_back(
                  metrics::make_gauge(
                    seastar::sstring{descriptor.name},
                    std::move(value),
                    metrics::description(seastar::sstring{descriptor.help}))
                    .aggregate(aggregate));
            };
            const auto add_counter = [&definitions,
                                      &aggregate](metric_id id, auto value) {
                const auto& descriptor = *descriptor_for(id);
                definitions.emplace_back(
                  metrics::make_counter(
                    seastar::sstring{descriptor.name},
                    std::move(value),
                    metrics::description(seastar::sstring{descriptor.help}))
                    .aggregate(aggregate));
            };
            add_gauge(metric_id::queue_items, [this] {
                return sum([](const auto& queue) { return queue.size(); });
            });
            add_gauge(metric_id::queue_bytes, [this] {
                return sum(
                  [](const auto& queue) { return queue.bytes().value(); });
            });
            add_gauge(metric_id::queue_producer_waiters, [this] {
                return sum(
                  [](const auto& queue) { return queue.waiting_producers(); });
            });
            add_gauge(metric_id::queue_consumer_waiters, [this] {
                return sum(
                  [](const auto& queue) { return queue.waiting_consumers(); });
            });
            add_gauge(metric_id::queue_active_handlers, [this] {
                return sum(
                  [](const auto& queue) { return queue.active_handlers(); });
            });
            add_counter(metric_id::queue_push_accepted_total, [this] {
                return sum(
                  [](const auto& queue) { return queue.accepted_pushes(); });
            });
            add_counter(metric_id::queue_push_rejected_total, [this] {
                return sum(
                  [](const auto& queue) { return queue.rejected_pushes(); });
            });
            add_counter(
              metric_id::queue_handler_failures_reported_total, [this] {
                  return sum(
                    [](const auto& queue) { return queue.reported_errors(); });
              });
            add_counter(
              metric_id::queue_handler_failures_suppressed_total, [this] {
                  return sum([](const auto& queue) {
                      return queue.suppressed_errors();
                  });
              });
            metrics_->add_group(
              seastar::sstring{descriptor_for(metric_id::queue_items)->group},
              definitions);
        } catch (...) {
            metrics_.reset();
            throw;
        }
    }

    void stop() noexcept {
        assert_current();
        metrics_.reset();
    }

    [[nodiscard]] bool registered() const noexcept {
        assert_current();
        return metrics_.has_value();
    }

private:
    template<typename Read>
    [[nodiscard]] std::uint64_t sum(Read read) const {
        return std::apply(
          [&read](const auto*... queues) {
              return (
                std::uint64_t{0} + ...
                + static_cast<std::uint64_t>(read(*queues)));
          },
          queues_);
    }

    std::tuple<Queues*...> queues_;
    std::optional<seastar::metrics::metric_groups> metrics_;
};

} // namespace kwaque::resource

#endif // KWAQUE_SRC_RESOURCE_BOUNDED_WORK_QUEUE_METRICS_H_
