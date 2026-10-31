#pragma once

#include "src/runtime/alarm.h"
#include "src/runtime/time.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/timer.hh>

namespace kwaque::runtime::production {

// A Seastar low-resolution timer. The reactor refreshes that clock on every
// loop and checks these timers as it does, so the alarm rings within about a
// task quota of its time. Its callback is set once, here; arming moves only
// the time.
class alarm final {
public:
    explicit alarm(alarm_callback callback);

    alarm(const alarm&) = delete;
    alarm& operator=(const alarm&) = delete;
    alarm(alarm&&) = delete;
    alarm& operator=(alarm&&) = delete;
    ~alarm() = default;

    void arm(monotonic_time at) noexcept;
    void cancel() noexcept { timer_.cancel(); }

private:
    alarm_callback callback_;
    seastar::timer<seastar::lowres_clock> timer_;
};

static_assert(alarm_service<alarm>);

} // namespace kwaque::runtime::production
