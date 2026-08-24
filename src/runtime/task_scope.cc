#include "src/runtime/task_scope.h"

#include <seastar/core/coroutine.hh>

#include <utility>

namespace kwaque::runtime {

task_scope::task_scope(seastar::abort_source& parent) {
    if (parent.abort_requested()) {
        request_abort_unchecked();
        return;
    }

    parent_subscription_ = parent.subscribe(
      [this] noexcept { request_abort_unchecked(); });
    if (!parent_subscription_ && parent.abort_requested()) {
        request_abort_unchecked();
    }
}

task_scope::~task_scope() {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-TASK-SCOPE-CLOSED"},
      close_done_.available() && gate_.is_closed() && gate_.get_count() == 0,
      "task scope destroyed before close completed");
}

void task_scope::request_abort_unchecked() noexcept {
    if (!abort_source_.abort_requested()) {
        abort_source_.request_abort();
    }
}

void task_scope::request_abort() {
    assert_current();
    request_abort_unchecked();
}

seastar::future<> task_scope::close_once() {
    request_abort_unchecked();
    parent_subscription_ = {};
    if (!gate_.is_closed()) {
        co_await gate_.close();
    }
    if (first_failure_) {
        std::rethrow_exception(first_failure_);
    }
}

seastar::future<> task_scope::close() {
    assert_current();
    if (!closing_) {
        closing_ = true;
        auto completion = close_once().then_wrapped(
          [this](seastar::future<> closed) noexcept {
              try {
                  closed.get();
                  close_done_.set_value();
              } catch (...) {
                  close_done_.set_exception(std::current_exception());
              }
          });
        static_cast<void>(completion);
    }
    return close_done_.get_shared_future();
}

bool task_scope::abort_requested() const {
    assert_current();
    return abort_source_.abort_requested();
}

bool task_scope::closed() const {
    assert_current();
    return gate_.is_closed();
}

std::size_t task_scope::task_count() const {
    assert_current();
    return gate_.get_count();
}

seastar::abort_source& task_scope::abort_source() {
    assert_current();
    return abort_source_;
}

} // namespace kwaque::runtime
