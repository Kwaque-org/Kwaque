#include "src/runtime/environment.h"

#include "src/base/invariant.h"

#include <utility>

namespace kwaque::runtime {

namespace {

constexpr invariant_id runtime_lifetime_invariant{"KQ-RUNTIME-LIFETIME-CLOSED"};

} // namespace

runtime_lifetime::~runtime_lifetime() {
    assert_current();
    KWAQUE_INVARIANT(
      runtime_lifetime_invariant,
      (state_ == runtime_lifetime_state::closed && leases_.get_count() == 0)
        || (state_ == runtime_lifetime_state::open && !activated_ && leases_.get_count() == 0),
      "runtime lifetime destroyed before explicit close completed");
}

std::optional<seastar::gate::holder> runtime_lifetime::acquire() {
    assert_current();
    if (state_ != runtime_lifetime_state::open) {
        return std::nullopt;
    }
    activated_ = true;
    return leases_.try_hold();
}

seastar::future<> runtime_lifetime::close() {
    assert_current();
    if (state_ == runtime_lifetime_state::closing) {
        return close_done_->get_shared_future();
    }
    if (state_ == runtime_lifetime_state::closed) {
        return close_done_ && close_done_->available()
                 ? close_done_->get_shared_future()
                 : seastar::make_ready_future<>();
    }
    if (!activated_ && leases_.get_count() == 0) {
        state_ = runtime_lifetime_state::closed;
        return seastar::make_ready_future<>();
    }

    try {
        close_done_.emplace();
    } catch (...) {
        return seastar::current_exception_as_future();
    }
    state_ = runtime_lifetime_state::closing;
    auto completion = leases_.close().then_wrapped(
      [this](seastar::future<> closed) {
          state_ = runtime_lifetime_state::closed;
          try {
              closed.get();
              close_done_->set_value();
          } catch (...) {
              close_done_->set_exception(std::current_exception());
          }
      });
    static_cast<void>(completion);
    return close_done_->get_shared_future();
}

runtime_lifetime_state runtime_lifetime::state() const {
    assert_current();
    return state_;
}

std::size_t runtime_lifetime::leases() const {
    assert_current();
    return leases_.get_count();
}

} // namespace kwaque::runtime
