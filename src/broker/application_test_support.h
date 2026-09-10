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
    static constexpr std::size_t start_boundary_count{8};

    static void configure(
      application_state& target, config::bootstrap_config configuration) {
        target.configuration_identity_ = identify_configuration(
          config::render_config(configuration));
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
               && target.environments_ == nullptr
               && target.crash_limiter_ == nullptr
               && target.crash_recorder_ == nullptr;
    }

    [[nodiscard]] static const configuration_identity&
    identity(const application_state& target) {
        return target.configuration_identity_.value();
    }

    [[nodiscard]] static const config::bootstrap_config&
    configuration(const application_state& target) {
        return target.configuration_.value();
    }

    static void request_stop(application_state& target) {
        target.stop_signal_->request_stop();
    }

    [[nodiscard]] static seastar::future<>
    stop_at_start_boundary(application_state& target, std::size_t stop_point) {
        return target.start_services_with(
          [&target, stop_point](std::size_t current) {
              if (current == stop_point) {
                  target.stop_signal_->request_stop();
                  target.stop_signal_->request_stop();
              }
          });
    }

    [[nodiscard]] static seastar::future<> shutdown_with_readiness(
      application_state& target, service_lifecycle::action notification) {
        return target.shutdown_with(std::move(notification));
    }

    [[nodiscard]] static seastar::future<>
    begin_admin_shutdown(application_state& target) {
        return target.admin_server_->begin_shutdown();
    }

    [[nodiscard]] static seastar::future<>
    add_cleanup(application_state& target, service_lifecycle::action cleanup) {
        return target.lifecycle_->start_step(
          "test_cleanup",
          [] { return seastar::make_ready_future<>(); },
          std::move(cleanup));
    }

    [[nodiscard]] static bool
    shutdown_failed(const application_state& target) noexcept {
        return target.shutdown_failed_;
    }

    template<typename Function, typename... Args>
    [[nodiscard]] static seastar::future<> on_environments(
      application_state& target, Function function, Args... args) {
        return target.environments_->invoke_on_all(
          std::move(function), std::move(args)...);
    }
};

} // namespace kwaque::broker::detail
