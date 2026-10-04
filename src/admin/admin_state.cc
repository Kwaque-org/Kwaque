#include "src/admin/admin_state.h"

#include "src/base/build_info.h"
#include "src/base/metric_schema.h"

#include <chrono>
#include <stdexcept>
#include <string>
#include <vector>

namespace kwaque::admin {

namespace {

const std::chrono::system_clock::time_point process_started
  = std::chrono::system_clock::now();

const metric_descriptor& metric(metric_id id) {
    const auto* descriptor = descriptor_for(id);
    if (descriptor == nullptr) {
        throw std::logic_error("admin metric descriptor is missing");
    }
    return *descriptor;
}

} // namespace

double process_start_time_seconds() noexcept {
    return std::chrono::duration<double>(process_started.time_since_epoch())
      .count();
}

void admin_state::register_metrics() {
    assert_current();
    if (metrics_) {
        throw std::logic_error("admin metrics are already registered");
    }
    namespace metrics = seastar::metrics;
    try {
        metrics_.emplace();
        if (owner().value() != 0) {
            return;
        }
        // Process-wide values have one owner. Aggregating the shard label
        // exposes them without a misleading shard identity.
        const std::vector<metrics::label> aggregate{metrics::shard_label};
        std::vector<metrics::metric_definition> definitions;
        definitions.reserve(5);
        const auto gauge = [&definitions, &aggregate](metric_id id, auto read) {
            const auto& descriptor = metric(id);
            definitions.emplace_back(
              metrics::make_gauge(
                seastar::sstring{descriptor.name},
                std::move(read),
                metrics::description(seastar::sstring{descriptor.help}))
                .aggregate(aggregate));
        };
        gauge(metric_id::broker_process_readiness, [this] {
            return ready() ? 1U : 0U;
        });
        gauge(
          metric_id::broker_draining, [this] { return draining() ? 1U : 0U; });
        gauge(metric_id::broker_shards, [this] { return shard_count(); });
        gauge(metric_id::broker_startup_duration_seconds, [this] {
            return startup_duration_seconds();
        });
        gauge(metric_id::broker_start_time_seconds, [] {
            return process_start_time_seconds();
        });
        metrics_->add_group(
          seastar::sstring{metric(metric_id::broker_process_readiness).group},
          definitions);

        // The conventional information metric: one series whose labels name
        // the running build and whose value is always 1.
        const metrics::label version{"version"};
        const metrics::label revision{"revision"};
        const metrics::label build_mode{"build_mode"};
        const metrics::label dirty{"dirty"};
        metrics_->add_group(
          "build",
          {metrics::make_gauge(
             "info",
             [] { return 1U; },
             metrics::description(
               "Build identity of the running broker; always 1"),
             {version(std::string{build_info::version()}),
              revision(std::string{build_info::git_revision()}),
              build_mode(std::string{build_info::build_mode()}),
              dirty(build_info::git_dirty() ? "true" : "false")})
             .aggregate(aggregate)});
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
    }
}

seastar::future<> admin_state::stop() {
    assert_current();
    lifecycle_ = lifecycle::stopped;
    metrics_.reset();
    return seastar::make_ready_future<>();
}

bool admin_state::live() const {
    assert_current();
    return lifecycle_ == lifecycle::live || lifecycle_ == lifecycle::ready
           || lifecycle_ == lifecycle::draining;
}

bool admin_state::ready() const {
    assert_current();
    return lifecycle_ == lifecycle::ready;
}

bool admin_state::draining() const {
    assert_current();
    return lifecycle_ == lifecycle::draining;
}

unsigned admin_state::shard_count() const {
    assert_current();
    return shard_count_;
}

double admin_state::startup_duration_seconds() const {
    assert_current();
    return startup_duration_seconds_;
}

} // namespace kwaque::admin
