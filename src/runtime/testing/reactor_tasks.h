#pragma once

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/idle_cpu_handler.hh>

namespace kwaque::runtime::testing {
namespace reactor_tasks_detail {
// One caller waiting for this shard's reactor to go idle, linked to the
// waiter it started inside.
struct drain_waiter final {
    seastar::promise<> idle;
    drain_waiter* outer;
};
inline thread_local drain_waiter* innermost_drain = nullptr;
} // namespace reactor_tasks_detail

// Waits until this reactor has no runnable tasks, including other scheduling
// groups. It does not step a simulation scheduler. Waiters nest: a drain
// started while another waits, such as one inside a callback of the call its
// caller is driving, is released first, and each idle point releases one.
inline seastar::future<> drain_reactor_tasks() {
    reactor_tasks_detail::drain_waiter waiting{
      {}, reactor_tasks_detail::innermost_drain};
    reactor_tasks_detail::innermost_drain = &waiting;
    seastar::set_idle_cpu_handler([](seastar::work_waiting_on_reactor) {
        auto* released = reactor_tasks_detail::innermost_drain;
        if (released == nullptr) {
            return seastar::idle_cpu_handler_result::no_more_work;
        }
        reactor_tasks_detail::innermost_drain = released->outer;
        released->idle.set_value();
        return seastar::idle_cpu_handler_result::
          interrupted_by_higher_priority_task;
    });
    co_await waiting.idle.get_future();
}
} // namespace kwaque::runtime::testing
