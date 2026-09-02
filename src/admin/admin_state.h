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
    void record_request();

    [[nodiscard]] bool live() const;
    [[nodiscard]] bool ready() const;
    [[nodiscard]] unsigned shard_count() const;
    [[nodiscard]] double startup_duration_seconds() const;
    [[nodiscard]] std::uint64_t shutdown_count() const;
    [[nodiscard]] std::uint64_t request_count() const;

private:
    enum class lifecycle : std::uint8_t { stopped, live, ready, draining };

    lifecycle lifecycle_{lifecycle::stopped};
    unsigned shard_count_{0};
    double startup_duration_seconds_{0.0};
    std::uint64_t shutdown_count_{0};
    std::uint64_t request_count_{0};
    std::optional<seastar::metrics::metric_groups> metrics_;
};

} // namespace kwaque::admin
