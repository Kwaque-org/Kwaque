#include "src/simulation/event_sink.h"

namespace kwaque::simulation {

namespace {

[[nodiscard]] runtime::operation_error sink_error(errc code) noexcept {
    return runtime::operation_error{
      code, runtime::operation_kind::observability};
}

[[nodiscard]] std::uint64_t
event_kind_value(const observability::event* value) noexcept {
    return value == nullptr ? 0U : static_cast<std::uint16_t>(value->kind());
}

} // namespace

event_log_sink::event_log_sink(
  observability::event_sink_identity identity,
  observability::event_log_limits limits,
  std::unique_ptr<observability::event_log> expected)
  : sequence_(identity)
  , events_(identity, limits)
  , expected_(std::move(expected)) {}

runtime::result<std::unique_ptr<event_log_sink>> event_log_sink::replay(
  observability::event_sink_identity identity,
  observability::event_log_limits limits,
  std::unique_ptr<observability::event_log> expected) {
    if (
      expected == nullptr || expected->identity() != identity
      || expected->entries().size() > limits.entries()
      || expected->encoded_bytes() > limits.encoded_bytes()) {
        return runtime::failure(sink_error(errc::replay_divergence));
    }
    return std::unique_ptr<event_log_sink>{
      new event_log_sink{identity, limits, std::move(expected)}};
}

runtime::result<void>
event_log_sink::emit(const observability::event_request& request) noexcept {
    return emit_with(request, nullptr);
}

runtime::result<void> event_log_sink::emit_reserved(
  const observability::event_request& request,
  observability::event_log::reservation& reservation) noexcept {
    return emit_with(request, &reservation);
}

runtime::result<void> event_log_sink::emit_with(
  const observability::event_request& request,
  observability::event_log::reservation* reservation) noexcept {
    assert_current();
    if (stopped_) {
        return runtime::failure(sink_error(errc::closed));
    }
    if (failure_) {
        return runtime::failure(*failure_);
    }
    auto prepared = sequence_.prepare(request);
    if (!prepared) {
        return runtime::failure(prepared.error());
    }
    if (auto compared = compare_next(prepared->value()); !compared) {
        return runtime::failure(compared.error());
    }
    const auto appended = reservation == nullptr
                            ? events_.append(prepared->value())
                            : events_.append(prepared->value(), *reservation);
    if (!appended) {
        return runtime::failure(appended.error());
    }
    prepared->commit();
    if (expected_) {
        ++replay_index_;
    }
    return {};
}

runtime::result<void>
event_log_sink::compare_next(const observability::event& actual) noexcept {
    if (!expected_) {
        return {};
    }
    if (replay_index_ == expected_->entries().size()) {
        return remember_failure(
          nullptr, &actual, event_replay_difference::expected_missing);
    }
    const auto& expected = expected_->entries()[replay_index_];
    if (expected != actual) {
        return remember_failure(
          &expected, &actual, event_replay_difference::value);
    }
    return {};
}

runtime::result<void> event_log_sink::remember_failure(
  const observability::event* expected,
  const observability::event* actual,
  event_replay_difference difference) noexcept {
    assert_current();
    if (!failure_) {
        auto error = sink_error(errc::replay_divergence);
        static_cast<void>(error.add_context(
          runtime::operation_context_key::sequence, replay_index_ + 1U));
        static_cast<void>(error.add_context(
          runtime::operation_context_key::detail,
          static_cast<std::uint8_t>(difference)));
        static_cast<void>(error.add_context(
          runtime::operation_context_key::expected,
          event_kind_value(expected)));
        static_cast<void>(error.add_context(
          runtime::operation_context_key::actual, event_kind_value(actual)));
        failure_.emplace(std::move(error));
    }
    return runtime::failure(*failure_);
}

runtime::result<void> event_log_sink::stop() noexcept {
    assert_current();
    stopped_ = true;
    return {};
}

runtime::result<void> event_log_sink::finish_replay() noexcept {
    assert_current();
    if (failure_) {
        return runtime::failure(*failure_);
    }
    if (!expected_ || replay_index_ == expected_->entries().size()) {
        return {};
    }
    return remember_failure(
      &expected_->entries()[replay_index_],
      nullptr,
      event_replay_difference::actual_missing);
}

} // namespace kwaque::simulation
