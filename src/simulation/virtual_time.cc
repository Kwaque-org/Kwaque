#include "src/simulation/virtual_time.h"

#include "src/base/invariant.h"

#include <bit>
#include <limits>
#include <utility>

namespace kwaque::simulation {

namespace {

constexpr invariant_id clock_bound_invariant{"KQ-CLOCK-BOUND"};
constexpr invariant_id clock_binding_invariant{"KQ-CLOCK-BINDING"};
constexpr invariant_id virtual_time_drained_invariant{"KQ-TIME-DRAINED"};
constexpr invariant_id virtual_time_config_invariant{"KQ-TIME-CONFIG"};
[[maybe_unused]] constexpr invariant_id wall_range_invariant{"KQ-WALL-RANGE"};

[[nodiscard]] runtime::operation_error clock_error(errc code) noexcept {
    return runtime::operation_error{code, runtime::operation_kind::clock};
}

[[nodiscard]] runtime::operation_error
clock_error(const runtime::operation_error& source) noexcept {
    auto translated = clock_error(source.code());
    for (std::size_t index = 0; index < source.context_size(); ++index) {
        const auto field = source.context_at(index);
        static_cast<void>(translated.add_context(field->key, field->value));
    }
    return translated;
}

} // namespace

thread_local virtual_time* virtual_time::active_ = nullptr;

runtime::result<virtual_time_config> virtual_time_config::make(
  const scheduler_limits& limits, virtual_time_config_values values) noexcept {
    const auto maximum_adjustment
      = values.maximum_wall_adjustment.nanoseconds();
    if (maximum_adjustment == 0) {
        return runtime::failure(clock_error(errc::invalid_argument));
    }
    if (maximum_adjustment > maximum_wall_adjustment_absolute.nanoseconds()) {
        return runtime::failure(clock_error(errc::out_of_range));
    }

    const auto epoch = static_cast<__int128_t>(values.epoch.unix_nanoseconds());
    const auto progress = static_cast<__int128_t>(
      limits.maximum_deadline().nanoseconds());
    const auto adjustment = static_cast<__int128_t>(maximum_adjustment);
    const auto minimum = static_cast<__int128_t>(
      std::numeric_limits<runtime::wall_time::rep>::min());
    const auto maximum = static_cast<__int128_t>(
      std::numeric_limits<runtime::wall_time::rep>::max());
    if (
      epoch - adjustment < minimum || epoch + progress + adjustment > maximum) {
        return runtime::failure(clock_error(errc::out_of_range));
    }
    return virtual_time_config{values, limits.maximum_deadline()};
}

virtual_time::virtual_time(scheduler& target, virtual_time_config config)
  : scheduler_(&target)
  , config_(config) {
    assert_current();
    KWAQUE_INVARIANT(
      virtual_time_config_invariant,
      config_.maximum_deadline() == scheduler_->limits().maximum_deadline(),
      "virtual time and scheduler limits differ");
    const auto capacity = static_cast<std::size_t>(
      scheduler_->limits().pending_events());
    adjustments_.reserve(capacity);
    for (std::size_t index = 0; index < capacity; ++index) {
        adjustments_.emplace_back(
          index + 1U == capacity ? no_adjustment : index + 1U);
    }
    free_adjustment_ = 0;
}

virtual_time::~virtual_time() {
    assert_current();
    if (scheduler_->trace_failed()) {
        static_cast<void>(scheduler_->discard_failed());
    }
    KWAQUE_INVARIANT(
      virtual_time_drained_invariant,
      active_ != this && first_adjustment_ == no_adjustment
        && pending_adjustments_ == 0,
      "virtual time destroyed while active");
}

runtime::monotonic_time virtual_time::monotonic_now() const {
    return scheduler_->now();
}

runtime::wall_time virtual_time::wall_now() const {
    const auto value
      = static_cast<__int128_t>(config_.epoch().unix_nanoseconds())
        + static_cast<__int128_t>(scheduler_->now().nanoseconds())
        + static_cast<__int128_t>(offset_.nanoseconds());
    KWAQUE_DEBUG_ASSERT(
      wall_range_invariant,
      value >= static_cast<__int128_t>(
        std::numeric_limits<runtime::wall_time::rep>::min())
        && value <= static_cast<__int128_t>(
             std::numeric_limits<runtime::wall_time::rep>::max()),
      "validated virtual wall time overflowed");
    return runtime::wall_time{static_cast<runtime::wall_time::rep>(value)};
}

wall_offset virtual_time::offset() const {
    assert_current();
    return offset_;
}

std::size_t virtual_time::pending_adjustments() const {
    assert_current();
    return pending_adjustments_;
}

runtime::result<void> virtual_time::schedule_wall_offset(
  runtime::monotonic_time deadline, wall_offset offset) {
    assert_current();
    if (stopped_) {
        return runtime::failure(clock_error(errc::closed));
    }
    if (offset.magnitude() > config_.maximum_wall_adjustment().nanoseconds()) {
        auto error = clock_error(errc::out_of_range);
        static_cast<void>(error.add_context(
          runtime::operation_context_key::items, offset.magnitude()));
        static_cast<void>(error.add_context(
          runtime::operation_context_key::limit,
          config_.maximum_wall_adjustment().nanoseconds()));
        return runtime::failure(std::move(error));
    }
    KWAQUE_INVARIANT(
      virtual_time_drained_invariant,
      free_adjustment_ != no_adjustment,
      "virtual wall adjustment capacity diverged from scheduler capacity");
    const auto index = free_adjustment_;
    auto& adjustment = adjustments_[index];
    free_adjustment_ = adjustment.next_free;
    adjustment.offset = offset;
    adjustment.next_active = first_adjustment_;
    adjustment.previous_active = no_adjustment;
    adjustment.active = true;
    if (first_adjustment_ != no_adjustment) {
        adjustments_[first_adjustment_].previous_active = index;
    }
    first_adjustment_ = index;
    ++pending_adjustments_;
    try {
        auto scheduled = scheduler_->schedule(
          deadline,
          event_priority::normal(),
          [this, index] noexcept {
              finish_adjustment(index, !scheduler_->discarding_failed_event());
          },
          trace_event_descriptor{
            .kind = trace_event_kind::wall_adjustment,
            .value = std::bit_cast<std::uint64_t>(offset.nanoseconds()),
            .effect = trace_action::wall_adjusted,
          },
          event_cleanup_policy::invoke);
        if (!scheduled) {
            release_adjustment(index);
            return runtime::failure(clock_error(scheduled.error()));
        }
        adjustment.event = *scheduled;
    } catch (...) {
        release_adjustment(index);
        throw;
    }
    return {};
}

runtime::result<void> virtual_time::stop() noexcept {
    assert_current();
    if (stopped_) {
        return {};
    }
    if (scheduler_->trace_failed()) {
        static_cast<void>(scheduler_->discard_failed());
        if (pending_adjustments_ != 0) {
            return runtime::failure(clock_error(errc::replay_divergence));
        }
        stopped_ = true;
        return {};
    }
    while (first_adjustment_ != no_adjustment) {
        const auto index = first_adjustment_;
        const auto canceled = scheduler_->cancel(adjustments_[index].event);
        if (!canceled) {
            return runtime::failure(clock_error(canceled.error()));
        }
        KWAQUE_INVARIANT(
          virtual_time_drained_invariant,
          *canceled,
          "virtual wall adjustment was not pending during stop");
        release_adjustment(index);
    }
    stopped_ = true;
    return {};
}

virtual_time& virtual_time::active() noexcept {
    KWAQUE_INVARIANT(
      clock_bound_invariant,
      active_ != nullptr,
      "simulation clock has no active time source");
    return *active_;
}

void virtual_time::finish_adjustment(std::size_t index, bool apply) noexcept {
    assert_current();
    KWAQUE_INVARIANT(
      virtual_time_drained_invariant,
      index < adjustments_.size() && adjustments_[index].active,
      "virtual wall adjustment count underflow");
    if (apply) {
        offset_ = adjustments_[index].offset;
    }
    release_adjustment(index);
}

void virtual_time::release_adjustment(std::size_t index) noexcept {
    auto& adjustment = adjustments_[index];
    if (adjustment.previous_active == no_adjustment) {
        first_adjustment_ = adjustment.next_active;
    } else {
        adjustments_[adjustment.previous_active].next_active
          = adjustment.next_active;
    }
    if (adjustment.next_active != no_adjustment) {
        adjustments_[adjustment.next_active].previous_active
          = adjustment.previous_active;
    }
    adjustment.active = false;
    adjustment.next_active = no_adjustment;
    adjustment.previous_active = no_adjustment;
    adjustment.next_free = free_adjustment_;
    free_adjustment_ = index;
    --pending_adjustments_;
}

bool clock_binding::available() noexcept {
    return virtual_time::active_ == nullptr;
}

clock_binding::clock_binding(virtual_time& time)
  : time_(&time) {
    time_->assert_current();
    KWAQUE_INVARIANT(
      clock_binding_invariant,
      virtual_time::active_ == nullptr,
      "simulation clock already bound");
    virtual_time::active_ = time_;
}

clock_binding::~clock_binding() {
    time_->assert_current();
    KWAQUE_INVARIANT(
      clock_binding_invariant,
      virtual_time::active_ == time_,
      "simulation clock binding changed");
    virtual_time::active_ = nullptr;
}

} // namespace kwaque::simulation
