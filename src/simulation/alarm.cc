#include "src/simulation/alarm.h"

#include "src/base/invariant.h"

#include <algorithm>
#include <utility>

namespace kwaque::simulation {

alarm::alarm(runtime::alarm_callback callback, scheduler& events) noexcept
  : callback_(std::move(callback))
  , events_(&events) {}

alarm::~alarm() { cancel(); }

void alarm::arm(runtime::monotonic_time at) noexcept {
    cancel();
    // A failed run rings nothing, and nothing rings after a run's last time.
    if (
      events_->trace_failure() != nullptr
      || at > events_->limits().maximum_deadline()) {
        return;
    }
    auto scheduled = events_->schedule(
      std::max(at, events_->now()), event_priority::normal(), [this] noexcept {
          pending_ = event_id{};
          callback_();
      });
    KWAQUE_INVARIANT(
      invariant_id{"KQ-SIM-ALARM-SCHEDULE"},
      scheduled.has_value(),
      "the scheduler refused an alarm");
    pending_ = *scheduled;
}

void alarm::cancel() noexcept {
    if (!pending_.valid()) {
        return;
    }
    if (events_->trace_failure() != nullptr) {
        // The failed run's queue is discarded with the event in it.
        pending_ = event_id{};
        return;
    }
    const auto cancelled = events_->cancel(pending_);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-SIM-ALARM-CANCEL"},
      cancelled.has_value(),
      "the scheduler refused to cancel an alarm");
    pending_ = event_id{};
}

} // namespace kwaque::simulation
