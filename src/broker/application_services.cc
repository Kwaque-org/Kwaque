#include "src/broker/application_internal.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>

#include <exception>
#include <stdexcept>
#include <utility>

namespace kwaque::broker::detail {

namespace {

template<typename Function>
seastar::future<> preserve_shutdown_failure(
  std::exception_ptr& first_failure, Function function) {
    try {
        co_await function();
    } catch (...) {
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

void application_state::construct_services(bool install_signal_handlers) {
    capture_or_assert_owner();
    if (services_constructed()) {
        throw std::logic_error("application services are already constructed");
    }
    stop_signal_ = std::make_unique<runtime::stop_signal>(
      install_signal_handlers);
    lifecycle_ = std::make_unique<service_lifecycle>(
      stop_signal_->abort_source());
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
    assert_owner();
    std::exception_ptr first_failure;
    if (admin_server_) {
        co_await preserve_shutdown_failure(
          first_failure, [this] { return admin_server_->begin_shutdown(); });
        co_await preserve_shutdown_failure(
          first_failure, [this] { return admin_server_->stop(); });
    }
    co_await preserve_shutdown_failure(
      first_failure, [this] { return request_service_abort(); });
    if (lifecycle_) {
        co_await preserve_shutdown_failure(
          first_failure, [this] { return lifecycle_->stop(); });
    }

    admin_server_.reset();
    environments_.reset();
    resource_registry_.reset();
    pid_file_.reset();
    lifecycle_.reset();
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
