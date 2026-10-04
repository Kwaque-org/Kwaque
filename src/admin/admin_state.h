#pragma once

#include "src/runtime/shard_affinity.h"

#include <seastar/core/future.hh>
#include <seastar/core/metrics.hh>

#include <cstdint>
#include <optional>

namespace kwaque::admin {

class admin_state final : public runtime::shard_affine {
public:
    void register_metrics();
    void listener_started(unsigned shard_count);
    void mark_ready(double startup_duration_seconds);
    void begin_shutdown();
    [[nodiscard]] seastar::future<> stop();

    // Liveness means the process should keep running. A draining broker is
    // still live; only readiness reports that it is leaving service.
    [[nodiscard]] bool live() const;
    [[nodiscard]] bool ready() const;
    [[nodiscard]] bool draining() const;
    [[nodiscard]] unsigned shard_count() const;
    [[nodiscard]] double startup_duration_seconds() const;

private:
    enum class lifecycle : std::uint8_t { stopped, live, ready, draining };

    lifecycle lifecycle_{lifecycle::stopped};
    unsigned shard_count_{0};
    double startup_duration_seconds_{0.0};
    std::optional<seastar::metrics::metric_groups> metrics_;
};

// Wall-clock time at which this process initialized its static storage.
[[nodiscard]] double process_start_time_seconds() noexcept;

} // namespace kwaque::admin
