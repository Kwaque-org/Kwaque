#include "src/runtime/task_scope.h"

#include <seastar/core/coroutine.hh>

#include <utility>

namespace kwaque::runtime {

task_scope::task_scope(failure_notifier notifier)
  : failure_notifier_(std::move(notifier)) {}

task_scope::task_scope(seastar::abort_source& parent)
  : task_scope(parent, {}) {}

task_scope::task_scope(seastar::abort_source& parent, failure_notifier notifier)
  : failure_notifier_(std::move(notifier)) {
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

void task_scope::complete_task(
  seastar::future<> completion, task_lifetime lifetime) noexcept {
    ++statistics_.completed;
    std::exception_ptr failure;
    try {
        completion.get();
        KWAQUE_INVARIANT(
          invariant_id{"KQ-TASK-UNEXPECTED-EXIT"},
          lifetime == task_lifetime::finite || abort_source_.abort_requested(),
          "required task returned before cancellation");
        return;
    } catch (const seastar::abort_requested_exception&) {
        if (
          lifetime == task_lifetime::until_abort
          && abort_source_.abort_requested()) {
            return;
        }
        failure = std::current_exception();
    } catch (...) {
        failure = std::current_exception();
    }
    ++statistics_.failed;
    KWAQUE_INVARIANT(
      invariant_id{"KQ-TASK-UNHANDLED-FAILURE"},
      lifetime == task_lifetime::finite || static_cast<bool>(failure_notifier_),
      "required task failed without an owner notification action");
    if (first_failure_) {
        return;
    }
    first_failure_ = std::move(failure);
    if (failure_notifier_) {
        try {
            failure_notifier_(first_failure_);
        } catch (...) {
            invariant_failed(
              invariant_id{"KQ-TASK-FAILURE-NOTIFIER"},
              "failure notifier must not throw",
              "task owner failed while receiving first failure");
        }
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
        ++statistics_.abort_requests;
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

bool task_scope::admission_closed() const {
    assert_current();
    return gate_.is_closed();
}

std::size_t task_scope::task_count() const {
    assert_current();
    return gate_.get_count();
}

task_scope_statistics task_scope::statistics() const {
    assert_current();
    return statistics_;
}

seastar::abort_source& task_scope::abort_source() {
    assert_current();
    return abort_source_;
}

} // namespace kwaque::runtime
