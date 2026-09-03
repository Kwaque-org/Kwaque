#include "src/observability/event_sink.h"

#include <fmt/format.h>

#include <cstdint>
#include <string_view>
#include <utility>

namespace kwaque::observability {

namespace {

[[nodiscard]] runtime::operation_error sink_error(errc code) noexcept {
    return runtime::operation_error{
      code, runtime::operation_kind::observability};
}

[[nodiscard]] constexpr seastar::log_level
native_level(event_severity severity) noexcept {
    switch (severity) {
    case event_severity::trace:
        return seastar::log_level::trace;
    case event_severity::debug:
        return seastar::log_level::debug;
    case event_severity::info:
        return seastar::log_level::info;
    case event_severity::warning:
        return seastar::log_level::warn;
    case event_severity::error:
        return seastar::log_level::error;
    }
    return seastar::log_level::error;
}

using log_iterator = seastar::internal::log_buf::inserter_iterator;

[[nodiscard]] log_iterator
append_text(log_iterator output, std::string_view value) noexcept {
    for (const char character : value) {
        *output++ = character;
    }
    return output;
}

[[nodiscard]] log_iterator
append_value(log_iterator output, const event_field_value& value) {
    switch (value.type()) {
    case event_field_type::signed_integer:
        return fmt::format_to(output, "{}", *value.as_signed());
    case event_field_type::unsigned_integer:
        return fmt::format_to(output, "{}", *value.as_unsigned());
    case event_field_type::boolean:
        return append_text(output, *value.as_boolean() ? "true" : "false");
    case event_field_type::bounded_string:
        return append_text(output, *value.as_text());
    case event_field_type::stable_id:
        return fmt::format_to(output, "{}", value.as_stable_id()->value());
    }
    return output;
}

[[nodiscard]] log_iterator
append_event(log_iterator output, const event& value, event_sink_epoch epoch) {
    output = append_text(output, "event=");
    output = append_text(output, value.name());
    output = fmt::format_to(
      output,
      " schema={} epoch={} monotonic_ns={} wall_ns={} shard={} workload={} "
      "sequence={}",
      value.schema_version(),
      epoch.value(),
      value.monotonic().nanoseconds(),
      value.wall().unix_nanoseconds(),
      value.shard().value(),
      resource::to_string(value.workload()),
      value.sequence());
    for (const auto& field : value.fields()) {
        const auto* descriptor = descriptor_for(field.key);
        *output++ = ' ';
        output = append_text(output, descriptor->name);
        *output++ = '=';
        output = append_value(output, field.value);
    }
    return output;
}

} // namespace

runtime::result<void>
production_event_sink::emit(const event_request& request) noexcept {
    assert_current();
    if (stopped_) {
        return runtime::failure(sink_error(errc::closed));
    }
    auto prepared = sequence_.prepare(request);
    if (!prepared) {
        return runtime::failure(prepared.error());
    }
    const auto& value = prepared->value();
    const auto level = native_level(value.severity());
    const auto epoch = sequence_.identity().epoch;
    auto append = [&value, epoch](log_iterator output) {
        return append_event(output, value, epoch);
    };
    seastar::logger::lambda_log_writer<decltype(append)> writer{
      std::move(append)};
    logger_->log(level, writer);
    prepared->commit();
    return {};
}

runtime::result<void> production_event_sink::stop() noexcept {
    assert_current();
    stopped_ = true;
    return {};
}

} // namespace kwaque::observability
