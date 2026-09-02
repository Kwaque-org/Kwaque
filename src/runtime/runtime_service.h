#pragma once

#include "src/runtime/shard_affinity.h"
#include "src/runtime/task_scope.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/shard_id.hh>

#include <functional>
#include <optional>

namespace kwaque::runtime {

// Shard-local runtime root. All mutable access must occur on owner(); stop()
// requests cancellation and drains every task accepted through tasks().
class runtime_service final : public shard_affine {
public:
    runtime_service() noexcept = default;
    explicit runtime_service(
      std::reference_wrapper<seastar::abort_source> parent_abort);

    [[nodiscard]] seastar::future<> start();
    void request_abort();
    [[nodiscard]] seastar::future<> stop();

    [[nodiscard]] seastar::shard_id shard() const noexcept;
    [[nodiscard]] bool ready() const;
    [[nodiscard]] bool abort_requested() const;
    [[nodiscard]] task_scope& tasks();

private:
    void register_metrics();

    task_scope tasks_;
    std::optional<seastar::metrics::metric_groups> metrics_;
    bool ready_{false};
};

} // namespace kwaque::runtime
