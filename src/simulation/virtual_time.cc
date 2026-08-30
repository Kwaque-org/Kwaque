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

virtual_time::virtual_time(
  scheduler& target, virtual_time_config config) noexcept
  : scheduler_(&target)
  , config_(config) {
    assert_current();
    KWAQUE_INVARIANT(
      virtual_time_config_invariant,
      config_.maximum_deadline() == scheduler_->limits().maximum_deadline(),
      "virtual time and scheduler limits differ");
}

virtual_time::~virtual_time() {
    assert_current();
    if (scheduler_->trace_failed()) {
        static_cast<void>(scheduler_->discard_failed());
    }
    KWAQUE_INVARIANT(
      virtual_time_drained_invariant,
      active_ != this && pending_adjustments_ == 0,
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
    if (offset.magnitude() > config_.maximum_wall_adjustment().nanoseconds()) {
        auto error = clock_error(errc::out_of_range);
        static_cast<void>(error.add_context(
          runtime::operation_context_key::items, offset.magnitude()));
        static_cast<void>(error.add_context(
          runtime::operation_context_key::limit,
          config_.maximum_wall_adjustment().nanoseconds()));
        return runtime::failure(std::move(error));
    }
    auto scheduled = scheduler_->schedule(
      deadline,
      event_priority::normal(),
      [this, offset] noexcept {
          if (scheduler_->discarding_failed_event()) [[unlikely]] {
              discard_wall_adjustment();
          } else {
              apply_wall_adjustment(offset);
          }
      },
      trace_event_descriptor{
        .kind = trace_event_kind::wall_adjustment,
        .value = std::bit_cast<std::uint64_t>(offset.nanoseconds()),
        .effect = trace_action::wall_adjusted,
      },
      event_cleanup_policy::invoke);
    if (!scheduled) {
        return runtime::failure(clock_error(scheduled.error()));
    }
    ++pending_adjustments_;
    return {};
}

virtual_time& virtual_time::active() noexcept {
    KWAQUE_INVARIANT(
      clock_bound_invariant,
      active_ != nullptr,
      "simulation clock has no active time source");
    return *active_;
}

void virtual_time::apply_wall_adjustment(wall_offset offset) noexcept {
    assert_current();
    KWAQUE_INVARIANT(
      virtual_time_drained_invariant,
      pending_adjustments_ != 0,
      "virtual wall adjustment count underflow");
    offset_ = offset;
    --pending_adjustments_;
}

void virtual_time::discard_wall_adjustment() noexcept {
    assert_current();
    KWAQUE_INVARIANT(
      virtual_time_drained_invariant,
      pending_adjustments_ != 0,
      "virtual wall adjustment discard count underflow");
    --pending_adjustments_;
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
