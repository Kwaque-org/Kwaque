#pragma once

#include "src/runtime/alarm.h"
#include "src/runtime/deadline_wheel.h"
#include "src/runtime/shard_affinity.h"
#include "src/runtime/time.h"
#include "src/runtime/waiting.h"

#include <cstddef>
#include <optional>
#include <utility>

namespace kwaque::runtime {

// A shard's deadlines: its wheel and the one alarm that drives it. The alarm
// is armed for the wheel's next bucket, moved only when an insert lands
// earlier, and stopped when the wheel empties. When it rings, the wheel ends
// or re-places at most `touches_per_task` entries; if collected entries are
// left, the alarm is armed for now, so other work runs before the rest,
// allocating nothing. The rest is handled in a later task, never in a loop
// that keeps other work waiting.
template<alarm_service Alarm, monotonic_clock Clock>
class basic_deadlines final : public shard_affine {
public:
    using precision = deadline_wheel::precision;
    using insertion = deadline_wheel::insertion;

    static constexpr std::size_t default_touches_per_task{256};

    template<typename... AlarmArguments>
    explicit basic_deadlines(
      std::size_t touches_per_task, AlarmArguments&&... alarm_arguments)
      : wheel_(Clock::now())
      , alarm_(
          [this]() noexcept { ring(); },
          std::forward<AlarmArguments>(alarm_arguments)...)
      , touches_per_task_(touches_per_task) {
        KWAQUE_INVARIANT(
          invariant_id{"KQ-DEADLINES-BUDGET"},
          touches_per_task_ > 0,
          "a deadline wheel needs a positive touch budget");
        wheel_.on_emptied(&disarm_wheel, this);
        // The shard's exceptions exist before any deadline fires or any
        // operation aborts: an abort source builds its default exception
        // even when it is given one.
        static_cast<void>(deadline_exceeded());
        static_cast<void>(operation_aborted());
    }

    basic_deadlines(const basic_deadlines&) = delete;
    basic_deadlines& operator=(const basic_deadlines&) = delete;
    basic_deadlines(basic_deadlines&&) = delete;
    basic_deadlines& operator=(basic_deadlines&&) = delete;

    ~basic_deadlines() {
        assert_current();
        disarm();
    }

    // Links `entry` to end at `deadline`; `due` when it has already passed,
    // and nothing was linked.
    [[nodiscard]] insertion
    arm(wheel_entry& entry, monotonic_time deadline, precision kind) noexcept {
        assert_current();
        const auto now = Clock::now();
        const auto inserted = wheel_.insert(entry, deadline, kind, now);
        if (inserted == insertion::linked) {
            schedule(now);
        }
        return inserted;
    }

    bool cancel(wheel_entry& entry) noexcept {
        assert_current();
        return wheel_.cancel(entry);
    }

    [[nodiscard]] std::size_t size() const noexcept { return wheel_.size(); }
    [[nodiscard]] bool armed() const noexcept { return armed_.has_value(); }
    [[nodiscard]] std::optional<monotonic_time> armed_at() const noexcept {
        return armed_;
    }
    [[nodiscard]] std::size_t touches_per_task() const noexcept {
        return touches_per_task_;
    }
    [[nodiscard]] Alarm& alarm() noexcept { return alarm_; }

private:
    void schedule(monotonic_time now) noexcept {
        const auto next = wheel_.next_expiry(now);
        if (!next) {
            disarm();
            return;
        }
        if (armed_ && *armed_ <= *next) {
            return;
        }
        armed_ = *next;
        alarm_.arm(*next);
    }

    void ring() noexcept {
        armed_.reset();
        const auto now = Clock::now();
        static_cast<void>(wheel_.expire(now, touches_per_task_));
        schedule(now);
    }

    void disarm() noexcept {
        if (armed_) {
            armed_.reset();
            alarm_.cancel();
        }
    }

    static void disarm_wheel(void* self) noexcept {
        static_cast<basic_deadlines*>(self)->disarm();
    }

    deadline_wheel wheel_;
    Alarm alarm_;
    std::size_t touches_per_task_;
    std::optional<monotonic_time> armed_;
};

} // namespace kwaque::runtime
