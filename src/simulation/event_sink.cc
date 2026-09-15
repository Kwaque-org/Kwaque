#include "src/simulation/event_sink.h"

#include <array>
#include <bit>

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

struct event_difference final {
    event_replay_difference field;
    std::uint64_t expected;
    std::uint64_t actual;
};

std::uint64_t
field_value(const observability::event_field_value& value) noexcept {
    using observability::event_field_type;
    switch (value.type()) {
    case event_field_type::signed_integer:
        return std::bit_cast<std::uint64_t>(*value.as_signed());
    case event_field_type::unsigned_integer:
        return *value.as_unsigned();
    case event_field_type::boolean:
        return *value.as_boolean();
    case event_field_type::stable_id:
        return value.as_stable_id()->value();
    case event_field_type::bounded_string:
        for (const auto& text : observability::event_text_descriptors()) {
            if (
              text.role == *value.text_role()
              && text.value == *value.as_text()) {
                return static_cast<std::uint16_t>(text.id);
            }
        }
        return 0;
    }
    return 0;
}

event_difference first_difference(
  const observability::event& expected,
  const observability::event& actual) noexcept {
    const std::array differences{
      event_difference{
        event_replay_difference::kind,
        event_kind_value(&expected),
        event_kind_value(&actual)},
      event_difference{
        event_replay_difference::severity,
        static_cast<std::uint8_t>(expected.severity()),
        static_cast<std::uint8_t>(actual.severity())},
      event_difference{
        event_replay_difference::monotonic,
        expected.monotonic().nanoseconds(),
        actual.monotonic().nanoseconds()},
      event_difference{
        event_replay_difference::wall,
        std::bit_cast<std::uint64_t>(expected.wall().unix_nanoseconds()),
        std::bit_cast<std::uint64_t>(actual.wall().unix_nanoseconds())},
      event_difference{
        event_replay_difference::shard,
        expected.shard().value(),
        actual.shard().value()},
      event_difference{
        event_replay_difference::workload,
        static_cast<std::uint8_t>(expected.workload()),
        static_cast<std::uint8_t>(actual.workload())},
      event_difference{
        event_replay_difference::sequence,
        expected.sequence(),
        actual.sequence()},
      event_difference{
        event_replay_difference::field_count,
        expected.fields().size(),
        actual.fields().size()},
    };
    for (const auto& candidate : differences) {
        if (candidate.expected != candidate.actual) {
            return candidate;
        }
    }
    for (std::size_t index = 0; index < expected.fields().size(); ++index) {
        const auto& left = expected.fields()[index];
        const auto& right = actual.fields()[index];
        const std::array fields{
          event_difference{
            event_replay_difference::field_key_0,
            static_cast<std::uint16_t>(left.key),
            static_cast<std::uint16_t>(right.key)},
          event_difference{
            event_replay_difference::field_type_0,
            static_cast<std::uint8_t>(left.value.type()),
            static_cast<std::uint8_t>(right.value.type())},
          event_difference{
            event_replay_difference::field_value_0,
            field_value(left.value),
            field_value(right.value)},
        };
        for (auto candidate : fields) {
            if (candidate.expected != candidate.actual) {
                candidate.field = static_cast<event_replay_difference>(
                  static_cast<std::uint8_t>(candidate.field) + index * 3U);
                return candidate;
            }
        }
    }
    return {
      event_replay_difference::value,
      event_kind_value(&expected),
      event_kind_value(&actual)};
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
        const auto context = expected != nullptr && actual != nullptr
                               ? first_difference(*expected, *actual)
                               : event_difference{
                                   difference,
                                   event_kind_value(expected),
                                   event_kind_value(actual)};
        auto error = sink_error(errc::replay_divergence);
        static_cast<void>(error.add_context(
          runtime::operation_context_key::sequence, replay_index_ + 1U));
        static_cast<void>(error.add_context(
          runtime::operation_context_key::detail,
          static_cast<std::uint8_t>(context.field)));
        static_cast<void>(error.add_context(
          runtime::operation_context_key::expected, context.expected));
        static_cast<void>(error.add_context(
          runtime::operation_context_key::actual, context.actual));
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
