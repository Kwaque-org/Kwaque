#pragma once

#include "src/runtime/time.h"

#include <seastar/util/noncopyable_function.hh>

#include <concepts>

namespace kwaque::runtime {

// One re-armable wake-up. It is made with the callback it calls when the
// time it is armed for comes; arming again moves that time, and cancel stops
// it. Neither allocates.
using alarm_callback = seastar::noncopyable_function<void() noexcept>;

template<typename Alarm>
concept alarm_service = requires(Alarm& alarm, monotonic_time at) {
    { alarm.arm(at) } noexcept -> std::same_as<void>;
    { alarm.cancel() } noexcept -> std::same_as<void>;
};

} // namespace kwaque::runtime
