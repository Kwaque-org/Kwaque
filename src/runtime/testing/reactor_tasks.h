#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/idle_cpu_handler.hh>

#include <optional>
#include <utility>

namespace kwaque::runtime::testing {
// A single test driver waits until this reactor has no runnable tasks,
// including other scheduling groups. This replaces its idle handler; callers
// must await it before starting another drain. It does not step a simulation
// scheduler.
inline seastar::future<> drain_reactor_tasks() {
    std::optional<seastar::promise<>> pending{std::in_place};
    auto ready = pending->get_future();
    seastar::set_idle_cpu_handler(
      [pending = std::move(pending)](seastar::work_waiting_on_reactor) mutable {
          if (!pending) return seastar::idle_cpu_handler_result::no_more_work;
          pending->set_value();
          pending.reset();
          return seastar::idle_cpu_handler_result::
            interrupted_by_higher_priority_task;
      });
    return ready;
}
} // namespace kwaque::runtime::testing
