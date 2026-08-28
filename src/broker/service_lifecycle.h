#pragma once

#include "src/runtime/shard_affinity.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>

#include <cstddef>
#include <functional>
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

    explicit service_lifecycle(seastar::abort_source& abort_source) noexcept;
    ~service_lifecycle();

    [[nodiscard]] seastar::future<> start_step(action start, action stop);
    [[nodiscard]] seastar::future<> stop();

    [[nodiscard]] service_lifecycle_state state() const;
    [[nodiscard]] std::size_t running_steps() const;

private:
    [[nodiscard]] seastar::future<> stop_once();

    seastar::abort_source& abort_source_;
    std::vector<action> started_;
    seastar::shared_promise<> stop_done_;
    service_lifecycle_state state_{service_lifecycle_state::open};
    bool operation_active_{false};
};

} // namespace kwaque::broker
