#include "src/storage/metrics.h"

#include "src/base/metric_schema.h"

#include <seastar/core/metrics.hh>

#include <stdexcept>
#include <vector>

namespace kwaque::storage {
storage_metrics::storage_metrics(std::span<const metric_source> sources) {
    if (sources.empty() || sources.size() > sources_.size())
        throw std::invalid_argument("invalid storage metric source count");
    for (const auto& source : sources) {
        if (
          !source.statistics || !source.budget
          || source.statistics->owner() != owner()
          || source.budget->owner() != owner())
            throw std::invalid_argument("invalid storage metric source");
        for (std::size_t i = 0; i < count_; ++i)
            if (
              sources_[i].statistics == source.statistics
              || sources_[i].budget == source.budget)
                throw std::invalid_argument("duplicate storage metric source");
        sources_[count_++] = source;
    }
}
void storage_metrics::stop() noexcept {
    assert_current();
    metrics_.reset();
}
void storage_metrics::start() {
    assert_current();
    if (metrics_)
        throw std::logic_error("storage metrics are already registered");
    namespace metrics = seastar::metrics;
    try {
        metrics_.emplace();
        std::vector<metrics::metric_definition> definitions;
        definitions.reserve(11);
        const std::vector<metrics::label> aggregate{metrics::shard_label};
        const auto gauge = [&](metric_id id, auto read) {
            const auto& descriptor = *descriptor_for(id);
            definitions.emplace_back(
              metrics::make_gauge(
                seastar::sstring{descriptor.name},
                [this, read] { return sum(read); },
                metrics::description(seastar::sstring{descriptor.help}))
                .aggregate(aggregate));
        };
        const auto counter = [&](metric_id id, auto read) {
            const auto& descriptor = *descriptor_for(id);
            definitions.emplace_back(
              metrics::make_counter(
                seastar::sstring{descriptor.name},
                [this, read] { return sum(read); },
                metrics::description(seastar::sstring{descriptor.help}))
                .aggregate(aggregate));
        };
        gauge(metric_id::storage_active, [](const auto& source) {
            return source.statistics->snapshot().operations.active;
        });
        counter(metric_id::storage_accepted_total, [](const auto& source) {
            return source.statistics->snapshot().operations.accepted;
        });
        counter(metric_id::storage_completed_total, [](const auto& source) {
            return source.statistics->snapshot().operations.completed;
        });
        counter(metric_id::storage_rejected_total, [](const auto& source) {
            return source.statistics->snapshot().operations.rejected;
        });
        counter(metric_id::storage_succeeded_total, [](const auto& source) {
            return source.statistics->snapshot().succeeded;
        });
        counter(metric_id::storage_failed_total, [](const auto& source) {
            return source.statistics->snapshot().failed;
        });
        counter(metric_id::storage_uncertain_total, [](const auto& source) {
            return source.statistics->snapshot().uncertain;
        });
        counter(metric_id::storage_durable_total, [](const auto& source) {
            return source.statistics->snapshot().durable;
        });
        gauge(metric_id::storage_reserved_tasks, [](const auto& source) {
            return source.budget->snapshot().tasks;
        });
        gauge(metric_id::storage_reserved_bytes, [](const auto& source) {
            return source.budget->snapshot().bytes;
        });
        gauge(metric_id::storage_reserved_handles, [](const auto& source) {
            return source.budget->snapshot().handles;
        });
        metrics_->add_group(
          seastar::sstring{descriptor_for(metric_id::storage_active)->group},
          definitions);
    } catch (...) {
        metrics_.reset();
        throw;
    }
}
} // namespace kwaque::storage
