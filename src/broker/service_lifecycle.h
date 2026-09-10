#pragma once

#include "src/broker/shutdown_watchdog.h"
#include "src/runtime/shard_affinity.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>

#include <cstddef>
#include <functional>
#include <string_view>
#include <vector>

namespace kwaque::broker {

enum class service_lifecycle_state {
    open,
    stopping,
    stopped,
};

class service_lifecycle final : public runtime::shard_affine {
public:
    using action = std::function<seastar::future<>()>;

    // An owner with additional abort broadcasts can retain rollback until
    // stop().
    explicit service_lifecycle(
      seastar::abort_source& abort_source,
      bool rollback_on_start_failure = true) noexcept;
    ~service_lifecycle();

    [[nodiscard]] seastar::future<> start_step(action start, action stop);
    [[nodiscard]] seastar::future<>
    start_step(std::string_view name, action start, action stop);
    [[nodiscard]] seastar::future<> stop();

    [[nodiscard]] service_lifecycle_state state() const;
    [[nodiscard]] std::size_t running_steps() const;

private:
    struct stop_step final {
        shutdown_stage_name name;
        action stop;
    };

    [[nodiscard]] seastar::future<> stop_once();

    seastar::abort_source& abort_source_;
    std::vector<stop_step> started_;
    seastar::shared_promise<> stop_done_;
    service_lifecycle_state state_{service_lifecycle_state::open};
    bool operation_active_{false};
    bool rollback_on_start_failure_;
};

} // namespace kwaque::broker
