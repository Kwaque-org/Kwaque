#include "src/runtime/runtime_service.h"

#include <seastar/core/coroutine.hh>

#include <exception>
#include <stdexcept>

namespace kwaque::runtime {

runtime_service::runtime_service(
  std::reference_wrapper<seastar::abort_source> parent_abort)
  : tasks_(parent_abort.get()) {}

seastar::future<> runtime_service::start() {
    assert_current();
    if (ready_ || tasks_.closed()) {
        throw std::logic_error("runtime service cannot be started");
    }
    ready_ = true;
    return seastar::make_ready_future<>();
}

void runtime_service::request_abort() {
    assert_current();
    tasks_.request_abort();
}

seastar::future<> runtime_service::stop() {
    assert_current();
    request_abort();
    std::exception_ptr failure;
    try {
        co_await tasks_.close();
    } catch (...) {
        failure = std::current_exception();
    }
    ready_ = false;
    if (failure) {
        std::rethrow_exception(failure);
    }
}

seastar::shard_id runtime_service::shard() const noexcept {
    return owner().value();
}

bool runtime_service::ready() const {
    assert_current();
    return ready_;
}

bool runtime_service::abort_requested() const {
    assert_current();
    return tasks_.abort_requested();
}

task_scope& runtime_service::tasks() {
    assert_current();
    return tasks_;
}

} // namespace kwaque::runtime
