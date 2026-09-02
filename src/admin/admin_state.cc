#include "src/admin/admin_state.h"

#include <stdexcept>
#include <vector>

namespace kwaque::admin {

void admin_state::register_metrics() {
    assert_current();
    if (metrics_) {
        throw std::logic_error("admin metrics are already registered");
    }
    namespace metrics = seastar::metrics;
    try {
        metrics_.emplace();
        std::vector<metrics::metric_definition> definitions;
        if (owner().value() == 0) {
            definitions.emplace_back(
              metrics::make_gauge(
                "process_readiness",
                [this] { return ready() ? 1U : 0U; },
                metrics::description(
                  "Whether the broker is ready for traffic")));
            definitions.emplace_back(
              metrics::make_gauge(
                "shard_count",
                [this] { return shard_count(); },
                metrics::description("Configured reactor shard count")));
            definitions.emplace_back(
              metrics::make_gauge(
                "startup_duration_seconds",
                [this] { return startup_duration_seconds(); },
                metrics::description(
                  "Time from application start to readiness")));
            definitions.emplace_back(
              metrics::make_counter(
                "shutdown_total",
                [this] { return shutdown_count(); },
                metrics::description("Number of initiated broker shutdowns")));
        }
        definitions.emplace_back(
          metrics::make_counter(
            "http_requests_total",
            [this] { return request_count(); },
            metrics::description("Administrative HTTP requests"))
            .aggregate(std::vector<metrics::label>{metrics::shard_label}));
        metrics_->add_group("broker", definitions);
    } catch (...) {
        metrics_.reset();
        throw;
    }
}

void admin_state::listener_started(unsigned shard_count) {
    assert_current();
    shard_count_ = shard_count;
    lifecycle_ = lifecycle::live;
}

void admin_state::mark_ready(double startup_duration_seconds) {
    assert_current();
    startup_duration_seconds_ = startup_duration_seconds;
    if (lifecycle_ == lifecycle::live) {
        lifecycle_ = lifecycle::ready;
    }
}

void admin_state::begin_shutdown() {
    assert_current();
    if (lifecycle_ == lifecycle::live || lifecycle_ == lifecycle::ready) {
        lifecycle_ = lifecycle::draining;
        ++shutdown_count_;
    }
}

seastar::future<> admin_state::stop() {
    assert_current();
    lifecycle_ = lifecycle::stopped;
    metrics_.reset();
    return seastar::make_ready_future<>();
}

void admin_state::record_request() {
    assert_current();
    ++request_count_;
}

bool admin_state::live() const {
    assert_current();
    return lifecycle_ == lifecycle::live || lifecycle_ == lifecycle::ready;
}

bool admin_state::ready() const {
    assert_current();
    return lifecycle_ == lifecycle::ready;
}

unsigned admin_state::shard_count() const {
    assert_current();
    return shard_count_;
}

double admin_state::startup_duration_seconds() const {
    assert_current();
    return startup_duration_seconds_;
}

std::uint64_t admin_state::shutdown_count() const {
    assert_current();
    return shutdown_count_;
}

std::uint64_t admin_state::request_count() const {
    assert_current();
    return request_count_;
}

} // namespace kwaque::admin
