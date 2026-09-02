#include "src/simulation/metrics.h"

#include "src/base/invariant.h"
#include "src/base/metric_schema.h"
#include "src/simulation/event_trace.h"
#include "src/simulation/fake_dns.h"
#include "src/simulation/fake_file.h"
#include "src/simulation/fake_network.h"
#include "src/simulation/fault_schedule.h"
#include "src/simulation/scheduler.h"

#include <seastar/core/metrics.hh>

#include <stdexcept>
#include <utility>
#include <vector>

namespace kwaque::simulation {

namespace {

const metric_descriptor& metric(metric_id id) { return *descriptor_for(id); }

} // namespace

simulation_metrics::simulation_metrics(
  scheduler& events,
  event_trace& trace,
  fault_schedule& faults,
  fake_file_system& files,
  fake_network& network,
  fake_dns& dns)
  : events_(&events)
  , trace_(&trace)
  , faults_(&faults)
  , files_(&files)
  , network_(&network)
  , dns_(&dns) {
    const auto owner = events.owner();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-SIMULATION-METRIC-SHARD"},
      events.uses_trace(trace) && faults.owner() == owner
        && files.owner() == owner && network.owner() == owner
        && dns.owner() == owner,
      "simulation metric sources have different owners");
}

void simulation_metrics::start() {
    assert_current();
    if (metrics_) {
        throw std::logic_error("simulation metrics are already registered");
    }
    namespace metrics = seastar::metrics;
    try {
        metrics_.emplace();
        std::vector<metrics::metric_definition> definitions;
        definitions.reserve(9);
        const std::vector<metrics::label> aggregate{metrics::shard_label};
        const auto add_gauge = [&definitions,
                                &aggregate](metric_id id, auto value) {
            const auto& descriptor = metric(id);
            definitions.emplace_back(
              metrics::make_gauge(
                seastar::sstring{descriptor.name},
                std::move(value),
                metrics::description(seastar::sstring{descriptor.help}))
                .aggregate(aggregate));
        };
        const auto add_counter = [&definitions,
                                  &aggregate](metric_id id, auto value) {
            const auto& descriptor = metric(id);
            definitions.emplace_back(
              metrics::make_counter(
                seastar::sstring{descriptor.name},
                std::move(value),
                metrics::description(seastar::sstring{descriptor.help}))
                .aggregate(aggregate));
        };
        add_gauge(metric_id::scheduler_pending_events, [this] {
            return events_->pending_events();
        });
        add_counter(metric_id::scheduler_executed_events_total, [this] {
            return events_->executed_events();
        });
        add_gauge(metric_id::trace_entries, [this] {
            return trace_->entries().size();
        });
        add_gauge(
          metric_id::fault_rules, [this] { return faults_->rules().size(); });
        add_counter(metric_id::fault_evaluations_total, [this] {
            return faults_->evaluations();
        });
        add_counter(metric_id::fault_decisions_applied_total, [this] {
            return faults_->applied_decisions();
        });
        add_gauge(metric_id::fake_file_active, [this] {
            return files_->pending_operations();
        });
        add_gauge(metric_id::fake_network_active, [this] {
            return network_->active_operations();
        });
        add_gauge(metric_id::fake_dns_active, [this] {
            return dns_->pending_queries();
        });
        metrics_->add_group(
          seastar::sstring{metric(metric_id::scheduler_pending_events).group},
          definitions);
    } catch (...) {
        metrics_.reset();
        throw;
    }
}

void simulation_metrics::stop() noexcept {
    assert_current();
    metrics_.reset();
}

} // namespace kwaque::simulation
