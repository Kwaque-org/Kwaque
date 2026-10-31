#include "src/runtime/production/alarm.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <utility>

namespace kwaque::runtime::production {

alarm::alarm(alarm_callback callback)
  : callback_(std::move(callback))
  // One pointer: the timer's callback is stored inline.
  , timer_([this] { callback_(); }) {}

void alarm::arm(monotonic_time at) noexcept {
    // The production monotonic clock is the low-resolution clock's own count;
    // a later time than that count can hold rings at its last.
    const auto count = std::min<std::uint64_t>(
      at.nanoseconds(), std::numeric_limits<seastar::lowres_clock::rep>::max());
    timer_.rearm(
      seastar::lowres_clock::time_point{seastar::lowres_clock::duration{
        static_cast<seastar::lowres_clock::rep>(count)}});
}

} // namespace kwaque::runtime::production
