#ifndef KWAQUE_SRC_SIMULATION_SCHEDULER_DRIVER_H_
#define KWAQUE_SRC_SIMULATION_SCHEDULER_DRIVER_H_

#include "src/base/error.h"
#include "src/runtime/testing/reactor_tasks.h"
#include "src/simulation/scheduler.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <system_error>
#include <type_traits>
#include <utility>

namespace kwaque::simulation::testing {

inline constexpr std::uint64_t scheduler_driver_batch_size{64};

class scheduler_liveness_error final : public std::runtime_error {
public:
    scheduler_liveness_error()
      : std::runtime_error(
          "simulation future remained pending without a scheduler event") {}
};

class scheduler_watchdog_error final : public std::runtime_error {
public:
    scheduler_watchdog_error()
      : std::runtime_error("simulation environment watchdog expired") {}
};

namespace scheduler_driver_detail {

inline void run_next_batch(scheduler& events, std::uint64_t maximum_batch) {
    if (!events.has_ready_events()) {
        const auto advanced = events.advance_to_next();
        if (!advanced) {
            throw std::system_error(make_error_code(advanced.error().code()));
        }
        if (!*advanced) {
            throw scheduler_liveness_error{};
        }
    }

    const auto ran = events.run_ready_batch(maximum_batch);
    if (!ran) {
        throw std::system_error(make_error_code(ran.error().code()));
    }
    if (*ran == 0U) {
        throw scheduler_liveness_error{};
    }
}

} // namespace scheduler_driver_detail

// Every pump tests its future, reports a stall and steps virtual time only
// once the reactor has nothing left to run. The continuations a batch woke
// are then complete, in whatever order and however many tasks they took, so
// a pump's steps and its return do not follow reactor task order. Pumps
// nest: one started inside a callback of the call another is driving steps
// the scheduler until it returns, and the outer one resumes after it.
template<typename T>
seastar::future<>
pump_deterministic_until(scheduler& events, seastar::future<T>& waiting) {
    const auto maximum_batch = std::min<std::uint64_t>(
      scheduler_driver_batch_size, events.limits().events_per_pump());
    for (;;) {
        co_await runtime::testing::drain_reactor_tasks();
        if (waiting.available()) {
            break;
        }
        if (events.pending_events() == 0U) {
            throw scheduler_liveness_error{};
        }
        scheduler_driver_detail::run_next_batch(events, maximum_batch);
    }
}

template<typename T>
seastar::future<> pump_until(scheduler& events, seastar::future<T>& waiting) {
    constexpr auto watchdog = std::chrono::seconds{10};
    const auto deadline = seastar::lowres_clock::now() + watchdog;
    const auto maximum_batch = std::min<std::uint64_t>(
      scheduler_driver_batch_size, events.limits().events_per_pump());
    for (;;) {
        co_await runtime::testing::drain_reactor_tasks();
        if (waiting.available()) {
            break;
        }
        if (seastar::lowres_clock::now() >= deadline) {
            throw scheduler_watchdog_error{};
        }
        if (events.pending_events() == 0U) {
            continue;
        }
        scheduler_driver_detail::run_next_batch(events, maximum_batch);
    }
}

template<typename T, typename NativeOwner>
seastar::future<> pump_registered_until(
  scheduler& events, seastar::future<T>& waiting, NativeOwner& native_owner) {
    constexpr auto watchdog = std::chrono::seconds{10};
    const auto deadline = seastar::lowres_clock::now() + watchdog;
    const auto maximum_batch = std::min<std::uint64_t>(
      scheduler_driver_batch_size, events.limits().events_per_pump());
    for (;;) {
        co_await runtime::testing::drain_reactor_tasks();
        if (waiting.available()) {
            break;
        }
        if (seastar::lowres_clock::now() >= deadline) {
            throw scheduler_watchdog_error{};
        }
        if (events.pending_events() == 0U) {
            if (native_owner.active_tasks() == 0U) {
                throw scheduler_liveness_error{};
            }
            continue;
        }
        scheduler_driver_detail::run_next_batch(events, maximum_batch);
    }
}

class scheduler_driver final {
public:
    explicit scheduler_driver(scheduler& events) noexcept
      : events_(&events) {}

    template<typename T>
    seastar::future<T> lifecycle(seastar::future<T> waiting) const {
        co_await pump_until(*events_, waiting);
        if constexpr (std::is_void_v<T>) {
            co_await std::move(waiting);
            co_return;
        } else {
            co_return co_await std::move(waiting);
        }
    }

    template<typename T>
    seastar::future<T> operation(seastar::future<T> waiting) const {
        co_await pump_deterministic_until(*events_, waiting);
        if constexpr (std::is_void_v<T>) {
            co_await std::move(waiting);
            co_return;
        } else {
            co_return co_await std::move(waiting);
        }
    }

    template<typename T, typename NativeOwner>
    seastar::future<T>
    operation(seastar::future<T> waiting, NativeOwner& native_owner) const {
        co_await pump_registered_until(*events_, waiting, native_owner);
        if constexpr (std::is_void_v<T>) {
            co_await std::move(waiting);
            co_return;
        } else {
            co_return co_await std::move(waiting);
        }
    }

private:
    scheduler* events_;
};

} // namespace kwaque::simulation::testing

#endif // KWAQUE_SRC_SIMULATION_SCHEDULER_DRIVER_H_
