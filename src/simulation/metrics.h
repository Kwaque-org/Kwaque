#ifndef KWAQUE_SRC_SIMULATION_METRICS_H_
#define KWAQUE_SRC_SIMULATION_METRICS_H_

#include "src/runtime/shard_affinity.h"

#include <seastar/core/metrics_registration.hh>

#include <optional>

namespace kwaque::simulation {

class event_trace;
class fake_dns;
class fake_file_system;
class fake_network;
class fault_schedule;
class scheduler;

class simulation_metrics final : public runtime::shard_affine {
public:
    simulation_metrics(
      scheduler& events,
      event_trace& trace,
      fault_schedule& faults,
      fake_file_system& files,
      fake_network& network,
      fake_dns& dns);

    void start();
    void stop() noexcept;
    [[nodiscard]] bool registered() const noexcept {
        assert_current();
        return metrics_.has_value();
    }

private:
    scheduler* events_;
    event_trace* trace_;
    fault_schedule* faults_;
    fake_file_system* files_;
    fake_network* network_;
    fake_dns* dns_;
    std::optional<seastar::metrics::metric_groups> metrics_;
};

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_METRICS_H_
