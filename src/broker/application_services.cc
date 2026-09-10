#include "src/broker/application_internal.h"
#include "src/broker/shutdown_watchdog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>

#include <exception>
#include <stdexcept>
#include <utility>

namespace kwaque::broker::detail {

namespace {

template<typename Function>
seastar::future<> preserve_shutdown_failure(
  std::exception_ptr& first_failure, std::string_view name, Function function) {
    shutdown_watchdog<> watchdog{shutdown_stage_name{name}};
    try {
        co_await function();
        watchdog.finish();
    } catch (...) {
        watchdog.fail();
        if (!first_failure) {
            first_failure = std::current_exception();
        }
    }
}

} // namespace

void application_state::capture_or_assert_owner() {
    if (!owner_) {
        owner_.emplace();
    }
    owner_->assert_current();
}

void application_state::assert_owner() const {
    if (!owner_) {
        throw std::logic_error("application reactor owner is not captured");
    }
    owner_->assert_current();
}

void application_state::initialize_stop_signal(bool install_signal_handlers) {
    capture_or_assert_owner();
    if (stop_signal_ != nullptr) {
        throw std::logic_error(
          "application stop signal is already initialized");
    }
    install_signal_handlers_ = install_signal_handlers;
    stop_signal_ = std::make_unique<runtime::stop_signal>(
      install_signal_handlers);
}

void application_state::construct_services(bool install_signal_handlers) {
    capture_or_assert_owner();
    if (services_constructed()) {
        throw std::logic_error("application services are already constructed");
    }
    if (stop_signal_ == nullptr) {
        initialize_stop_signal(install_signal_handlers);
    }
    stop_signal_->abort_source().check();
    lifecycle_ = std::make_unique<service_lifecycle>(
      stop_signal_->abort_source(), false);
    admin_server_ = std::make_unique<admin::admin_server>();
    resource_registry_ = std::make_unique<resource::resource_registry>();
    environments_ = std::make_unique<runtime::production::environment_owner>(
      seastar::default_smp_service_group());
}

seastar::future<> application_state::request_service_abort() {
    assert_owner();
    if (!environments_) {
        co_return;
    }
    co_await environments_->request_abort();
}

seastar::future<> application_state::shutdown() {
    return shutdown_with([this] {
        return admin_server_ ? admin_server_->begin_shutdown()
                             : seastar::make_ready_future<>();
    });
}

seastar::future<> application_state::shutdown_after_start(
  service_lifecycle::action readiness_notification) {
    stop_signal_->request_stop();
    co_await start_finished_.get_shared_future();
    co_await shutdown_with(std::move(readiness_notification));
}

seastar::future<> application_state::shutdown_with(
  service_lifecycle::action readiness_notification) {
    assert_owner();
    if (start_active_) {
        return shutdown_after_start(std::move(readiness_notification));
    }
    if (shutdown_started_) {
        return shutdown_finished_.get_shared_future();
    }
    shutdown_started_ = true;
    auto completion
      = shutdown_once(std::move(readiness_notification))
          .then_wrapped([this](seastar::future<> stopped) noexcept {
              try {
                  stopped.get();
                  shutdown_finished_.set_value();
              } catch (...) {
                  shutdown_failed_ = true;
                  shutdown_finished_.set_exception(std::current_exception());
              }
          });
    static_cast<void>(completion);
    return shutdown_finished_.get_shared_future();
}

seastar::future<> application_state::shutdown_once(
  service_lifecycle::action readiness_notification) {
    std::exception_ptr first_failure;
    // Both notifications are issued before awaiting either acknowledgement.
    // Their owners and diagnostics stay alive until both futures settle.
    auto readiness = preserve_shutdown_failure(
      first_failure, "readiness", std::move(readiness_notification));
    auto abort = preserve_shutdown_failure(
      first_failure, "runtime_abort", [this] {
          return request_service_abort();
      });
    co_await std::move(readiness);
    co_await std::move(abort);
    if (stop_signal_) {
        stop_signal_->request_stop();
    }
    if (lifecycle_) {
        co_await preserve_shutdown_failure(
          first_failure, "services", [this] { return lifecycle_->stop(); });
    }

    // HTTP callbacks are unregistered before runtime metrics and their targets
    // disappear. PID ownership covers all cleanup and persistent bookkeeping.
    admin_server_.reset();
    environments_.reset();
    resource_registry_.reset();
    lifecycle_.reset();
    if (crash_recorder_) {
        co_await preserve_shutdown_failure(
          first_failure, "crash_recorder", [this] {
              return crash_recorder_->stop();
          });
        crash_recorder_.reset();
    }
    if (crash_limiter_ && fully_started_ && !first_failure) {
        co_await preserve_shutdown_failure(
          first_failure, "restart_tracker", [this] {
              return crash_limiter_->record_clean_shutdown();
          });
    }
    crash_limiter_.reset();
    pid_file_.reset();
    stop_signal_.reset();

    if (first_failure) {
        std::rethrow_exception(first_failure);
    }
}

bool application_state::services_constructed() const noexcept {
    return stop_signal_ != nullptr && lifecycle_ != nullptr
           && admin_server_ != nullptr && resource_registry_ != nullptr
           && environments_ != nullptr;
}

bool application_state::runtime_started() const {
    assert_owner();
    return environments_ != nullptr
           && environments_->state() == runtime::sharded_service_state::started;
}

const service_lifecycle* application_state::lifecycle() const noexcept {
    return lifecycle_.get();
}

} // namespace kwaque::broker::detail
