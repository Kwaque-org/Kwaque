#ifndef KWAQUE_SRC_SIMULATION_ALARM_H_
#define KWAQUE_SRC_SIMULATION_ALARM_H_

#include "src/runtime/alarm.h"
#include "src/runtime/time.h"
#include "src/simulation/scheduler.h"

namespace kwaque::simulation {

// One scheduler event at a time: arming cancels the pending event and
// schedules another, so the alarm rings in the scheduler's deterministic
// order. A time already past rings at the scheduler's present.
class alarm final {
public:
    alarm(runtime::alarm_callback callback, scheduler& events) noexcept;

    alarm(const alarm&) = delete;
    alarm& operator=(const alarm&) = delete;
    alarm(alarm&&) = delete;
    alarm& operator=(alarm&&) = delete;
    ~alarm();

    void arm(runtime::monotonic_time at) noexcept;
    void cancel() noexcept;

    [[nodiscard]] bool armed() const noexcept { return pending_.valid(); }

private:
    runtime::alarm_callback callback_;
    scheduler* events_;
    event_id pending_;
};

static_assert(runtime::alarm_service<alarm>);

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_ALARM_H_
