#include "src/broker/application_internal.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>

#include <exception>
#include <stdexcept>
#include <utility>

namespace kwaque::broker::detail {

void application_state::construct_services(bool install_signal_handlers) {
    if (services_constructed()) {
        throw std::logic_error("application services are already constructed");
    }
    stop_signal_ = std::make_unique<runtime::stop_signal>(
      install_signal_handlers);
    lifecycle_ = std::make_unique<service_lifecycle>(
      stop_signal_->abort_source());
    admin_server_ = std::make_unique<admin::admin_server>();
    runtime_service_
      = std::make_unique<runtime::sharded_service<runtime::runtime_service>>(
        seastar::default_smp_service_group());
}

seastar::future<> application_state::request_service_abort() {
    if (!runtime_service_ || !runtime_started_) {
        co_return;
    }
    co_await runtime_service_->request_abort();
}

seastar::future<> application_state::shutdown() {
    std::exception_ptr failure;
    if (lifecycle_) {
        try {
            co_await lifecycle_->stop();
        } catch (...) {
            failure = std::current_exception();
        }
    }

    runtime_started_ = false;
    runtime_service_.reset();
    pid_file_.reset();
    admin_server_.reset();
    lifecycle_.reset();
    stop_signal_.reset();

    if (failure) {
        std::rethrow_exception(failure);
    }
}

bool application_state::services_constructed() const noexcept {
    return stop_signal_ != nullptr && lifecycle_ != nullptr
           && admin_server_ != nullptr && runtime_service_ != nullptr;
}

bool application_state::runtime_started() const noexcept {
    return runtime_started_;
}

const service_lifecycle* application_state::lifecycle() const noexcept {
    return lifecycle_.get();
}

} // namespace kwaque::broker::detail
