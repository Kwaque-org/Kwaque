#include "src/runtime/production/backend.h"

#include "src/base/invariant.h"

#include <seastar/core/coroutine.hh>

#include <exception>
#include <new>
#include <stdexcept>
#include <system_error>
#include <utility>

namespace kwaque::runtime::production {

namespace {

constexpr invariant_id backend_stopped_invariant{"KQ-BACKEND-STOPPED"};

void throw_if_failed(const result<void>& outcome) {
    if (!outcome) {
        throw std::system_error(make_error_code(outcome.error().code()));
    }
}

} // namespace

backend::backend(backend_dependencies dependencies)
  : parent_abort_(dependencies.parent_abort_)
  , dns_options_(std::move(dependencies.dns_options_)) {}

backend::~backend() {
    assert_current();
    KWAQUE_INVARIANT(
      backend_stopped_invariant,
      state_ == backend_state::constructed || state_ == backend_state::stopped,
      "production backend destroyed while active");
}

seastar::future<> backend::start() {
    return start_with([](std::size_t) noexcept {});
}

void backend::request_abort_unchecked() noexcept {
    if (abort_requested_) {
        return;
    }
    abort_requested_ = true;
    if (timer_) {
        timer_->request_abort();
    }
    if (dns_) {
        dns_->request_abort();
    }
}

void backend::request_abort() {
    assert_current();
    request_abort_unchecked();
}

seastar::future<> backend::stop() {
    assert_current();
    if (state_ == backend_state::stopping) {
        return stop_done_->get_shared_future();
    }
    if (state_ == backend_state::stopped) {
        return stop_done_ && stop_done_->available()
                 ? stop_done_->get_shared_future()
                 : seastar::make_ready_future<>();
    }
    if (state_ == backend_state::starting) {
        return seastar::make_exception_future<>(
          std::logic_error("production backend startup is in progress"));
    }

    try {
        stop_done_.emplace();
    } catch (...) {
        return seastar::current_exception_as_future();
    }
    state_ = backend_state::stopping;
    request_abort_unchecked();
    auto completion = stop_once().then_wrapped(
      [this](seastar::future<> stopped) noexcept {
          state_ = backend_state::stopped;
          try {
              stopped.get();
              stop_done_->set_value();
          } catch (...) {
              stop_done_->set_exception(std::current_exception());
          }
      });
    static_cast<void>(completion);
    return stop_done_->get_shared_future();
}

seastar::future<> backend::stop_once() {
    std::exception_ptr first_failure;
    try {
        co_await lifetime_.close();
    } catch (...) {
        first_failure = std::current_exception();
    }

    if (dns_) {
        try {
            throw_if_failed(co_await dns_->stop());
        } catch (...) {
            if (!first_failure) {
                first_failure = std::current_exception();
            }
        }
        dns_.reset();
    }
    network_.reset();
    file_system_.reset();
    if (timer_) {
        try {
            throw_if_failed(co_await timer_->stop());
        } catch (...) {
            if (!first_failure) {
                first_failure = std::current_exception();
            }
        }
        timer_.reset();
    }
    random_.reset();
    dns_options_.reset();
    parent_subscription_ = {};

    if (first_failure) {
        std::rethrow_exception(first_failure);
    }
}

seastar::future<> backend::rollback_start() noexcept {
    try {
        request_abort_unchecked();
        co_await stop_once();
    } catch (...) {
    }
}

backend_state backend::state() const {
    assert_current();
    return state_;
}

bool backend::abort_requested() const {
    assert_current();
    return abort_requested_;
}

} // namespace kwaque::runtime::production
