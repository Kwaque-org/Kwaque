#include "src/runtime/stop_signal.h"

#include "src/base/invariant.h"

#include <seastar/core/signal.hh>

#include <csignal>
#include <exception>
#include <stdexcept>

namespace kwaque::runtime {

namespace {

stop_signal* active_handler_owner = nullptr;

} // namespace

stop_signal::stop_signal(bool install_handlers)
  : handlers_installed_(install_handlers) {
    if (handlers_installed_) {
        if (owner().value() != 0) {
            throw std::logic_error(
              "process signal handlers must be owned by shard zero");
        }
        if (active_handler_owner != nullptr) {
            throw std::logic_error(
              "process signal handlers already have an owner");
        }
        active_handler_owner = this;
        bool interrupt_installed = false;
        try {
            seastar::handle_signal(SIGINT, [this] { request_stop(); });
            interrupt_installed = true;
            seastar::handle_signal(SIGTERM, [this] { request_stop(); });
        } catch (...) {
            if (interrupt_installed) {
                try {
                    seastar::handle_signal(SIGINT, [] {});
                } catch (...) {
                    std::terminate();
                }
            }
            active_handler_owner = nullptr;
            throw;
        }
    }
}

stop_signal::~stop_signal() {
    assert_current();
    if (handlers_installed_) {
        KWAQUE_INVARIANT(
          invariant_id{"KQ-STOP-SIGNAL-OWNER"},
          active_handler_owner == this,
          "process signal handler ownership changed");
        seastar::handle_signal(SIGINT, [] {});
        seastar::handle_signal(SIGTERM, [] {});
        active_handler_owner = nullptr;
    }
    request_stop();
}

seastar::future<> stop_signal::wait() {
    assert_current();
    return condition_.wait([this] { return stopping(); });
}

bool stop_signal::stopping() const {
    assert_current();
    return abort_source_.abort_requested();
}

seastar::abort_source& stop_signal::abort_source() {
    assert_current();
    return abort_source_;
}

void stop_signal::request_stop() noexcept {
    assert_current();
    if (!abort_source_.abort_requested()) {
        abort_source_.request_abort();
    }
    condition_.broadcast();
}

} // namespace kwaque::runtime
