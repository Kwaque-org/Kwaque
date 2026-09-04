#pragma once

#include "src/broker/application_internal.h"

#include <seastar/core/future.hh>

#include <chrono>
#include <cstddef>
#include <stdexcept>
#include <utility>

namespace kwaque::broker::detail {

class application_start_checkpoint_failure final : public std::runtime_error {
public:
    application_start_checkpoint_failure()
      : std::runtime_error("injected broker startup checkpoint failure") {}
};

class application_test_access final {
public:
    static constexpr std::size_t start_boundary_count{6};

    static void configure(
      application_state& target, config::bootstrap_config configuration) {
        target.configuration_ = std::move(configuration);
        target.startup_started_at_ = std::chrono::steady_clock::now();
    }

    [[nodiscard]] static seastar::future<> fail_at_start_boundary(
      application_state& target, std::size_t failure_point) {
        return target.start_services_with([failure_point](std::size_t current) {
            if (current == failure_point) {
                throw application_start_checkpoint_failure{};
            }
        });
    }

    [[nodiscard]] static bool
    services_released(const application_state& target) noexcept {
        return target.stop_signal_ == nullptr && target.lifecycle_ == nullptr
               && target.pid_file_ == nullptr && target.admin_server_ == nullptr
               && target.resource_registry_ == nullptr
               && target.environments_ == nullptr;
    }
};

} // namespace kwaque::broker::detail
