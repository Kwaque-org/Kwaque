#include "src/storage/metrics.h"

#include "src/base/metric_schema.h"

#include <seastar/core/metrics.hh>

#include <cstddef>
#include <stdexcept>
#include <utility>
#include <vector>

namespace kwaque::storage {
storage_metrics::storage_metrics(
  std::span<const metric_source> sources, placement_reader placement)
  : placement_(std::move(placement)) {
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
        definitions.reserve(placement_ ? 30 : 11);
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
        if (placement_) {
            const auto placed = [&](metric_id id, auto read) {
                const auto& descriptor = *descriptor_for(id);
                const seastar::sstring name{descriptor.name};
                const auto help = metrics::description(
                  seastar::sstring{descriptor.help});
                if (descriptor.kind == metric_value_kind::counter)
                    definitions.emplace_back(
                      metrics::make_counter(
                        name, [this, read] { return read(placed_now()); }, help)
                        .aggregate(aggregate));
                else
                    definitions.emplace_back(
                      metrics::make_gauge(
                        name, [this, read] { return read(placed_now()); }, help)
                        .aggregate(aggregate));
            };
            constexpr auto wal = static_cast<std::size_t>(
              local_append_placement::wal);
            constexpr auto segment = static_cast<std::size_t>(
              local_append_placement::segment);
            placed(
              metric_id::storage_wal_placed_portions_total,
              [](const auto& placed) { return placed.portions[wal]; });
            placed(
              metric_id::storage_segment_placed_portions_total,
              [](const auto& placed) { return placed.portions[segment]; });
            placed(
              metric_id::storage_wal_placed_bytes_total,
              [](const auto& placed) { return placed.portion_bytes[wal]; });
            placed(
              metric_id::storage_segment_placed_bytes_total,
              [](const auto& placed) { return placed.portion_bytes[segment]; });
            placed(
              metric_id::storage_held_portions_total,
              [](const auto& placed) { return placed.held_portions; });
            placed(
              metric_id::storage_held_portion_bytes_total,
              [](const auto& placed) { return placed.held_portion_bytes; });
            const auto runs = [&](metric_id id, segment_run_close why) {
                placed(id, [why](const auto& placed) {
                    return placed.runs[static_cast<std::size_t>(why)];
                });
            };
            runs(
              metric_id::storage_runs_filled_total, segment_run_close::filled);
            runs(
              metric_id::storage_runs_wanted_total, segment_run_close::wanted);
            runs(
              metric_id::storage_runs_limited_total,
              segment_run_close::limited);
            runs(
              metric_id::storage_runs_synced_total, segment_run_close::synced);
            runs(
              metric_id::storage_runs_pressed_total,
              segment_run_close::pressed);
            placed(
              metric_id::storage_segment_barriers_total,
              [](const auto& placed) { return placed.barriers; });
            placed(
              metric_id::storage_segment_syncs_total,
              [](const auto& placed) { return placed.syncs; });
            placed(
              metric_id::storage_sync_rounds_total,
              [](const auto& placed) { return placed.sync_rounds; });
            placed(
              metric_id::storage_round_syncs_total,
              [](const auto& placed) { return placed.round_syncs; });
            placed(metric_id::storage_held_bytes, [](const auto& placed) {
                return placed.held_bytes;
            });
            placed(metric_id::storage_held_runs, [](const auto& placed) {
                return placed.held_runs;
            });
            placed(metric_id::storage_unsynced_bytes, [](const auto& placed) {
                return placed.unsynced_bytes;
            });
            placed(
              metric_id::storage_unsynced_wal_files,
              [](const auto& placed) { return placed.unsynced_files; });
        }
        metrics_->add_group(
          seastar::sstring{descriptor_for(metric_id::storage_active)->group},
          definitions);
    } catch (...) {
        metrics_.reset();
        throw;
    }
}
} // namespace kwaque::storage
