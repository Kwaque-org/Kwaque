#include "src/runtime/stop_signal.h"

#include <seastar/core/signal.hh>

#include <csignal>

namespace kwaque::runtime {

stop_signal::stop_signal(bool install_handlers)
  : handlers_installed_(install_handlers) {
    if (handlers_installed_) {
        seastar::handle_signal(SIGINT, [this] { request_stop(); });
        seastar::handle_signal(SIGTERM, [this] { request_stop(); });
    }
}

stop_signal::~stop_signal() {
    if (handlers_installed_) {
        seastar::handle_signal(SIGINT, [] {});
        seastar::handle_signal(SIGTERM, [] {});
    }
    request_stop();
}

seastar::future<> stop_signal::wait() {
    return condition_.wait([this] { return stopping(); });
}

bool stop_signal::stopping() const noexcept {
    return abort_source_.abort_requested();
}

seastar::abort_source& stop_signal::abort_source() noexcept {
    return abort_source_;
}

void stop_signal::request_stop() noexcept {
    if (!abort_source_.abort_requested()) {
        abort_source_.request_abort();
    }
    condition_.broadcast();
}

} // namespace kwaque::runtime
