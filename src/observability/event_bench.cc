#include "src/observability/event.h"
#include "src/observability/event_identity.h"
#include "src/observability/event_log.h"
#include "src/observability/event_sequence.h"
#include "src/observability/event_sink.h"
#include "src/observability/testing/capture_event_sink.h"
#include "src/resource/workload_class.h"
#include "src/runtime/time.h"

#include <seastar/testing/perf_tests.hh>
#include <seastar/util/log.hh>

#include <array>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <utility>

namespace kwaque::observability {

namespace {

constexpr std::size_t inner_iterations = 4'096;

event_sink_identity identity() {
    auto epoch = event_sink_epoch::make(1);
    if (!epoch) {
        throw std::logic_error("event benchmark epoch");
    }
    return event_sink_identity{
      .epoch = *epoch,
      .configuration_digest = {},
    };
}

event_text text(event_public_text value) {
    auto made = event_text::make(value);
    if (!made) {
        throw std::logic_error("event benchmark text");
    }
    return *made;
}

struct event_construction_fixture {
    event_request_context context{
      .kind = event_kind::runtime_state_changed,
      .severity = event_severity::info,
      .monotonic = runtime::monotonic_time{17},
      .wall = runtime::wall_time{23},
      .workload = resource::workload_class::metadata,
    };
    std::array<event_field, 2> fields{
      event_field{
        .key = event_field_key::state,
        .value = event_field_value::from_text(
          text(event_public_text::state_ready))},
      event_field{
        .key = event_field_key::operation,
        .value = event_field_value::from_text(
          text(event_public_text::operation_environment_start))},
    };
};

event_request request() {
    event_construction_fixture fixture;
    auto made = event_request::make(fixture.context, fixture.fields);
    if (!made) {
        throw std::logic_error("event benchmark request");
    }
    return std::move(*made);
}

} // namespace

PERF_TEST_F(event_construction_fixture, validated_request) {
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        auto made = event_request::make(context, fields);
        if (!made) [[unlikely]] {
            throw std::logic_error("event benchmark construction");
        }
        perf_tests::do_not_optimize(made->encoded_size());
    }
    return inner_iterations;
}

PERF_TEST(structured_event, reserved_capture) {
    testing::capture_event_sink sink{identity(), event_log_limits::defaults()};
    const auto value = request();
    perf_tests::start_measuring_time();
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        if (!sink.emit(value)) [[unlikely]] {
            throw std::logic_error("event benchmark capture");
        }
    }
    perf_tests::stop_measuring_time();
    perf_tests::do_not_optimize(sink.events().entries().size());
    static_cast<void>(sink.stop());
    return inner_iterations;
}

PERF_TEST(structured_event, disabled_native_log) {
    seastar::logger logger{"kwaque-event-benchmark"};
    logger.set_level(seastar::log_level::error);
    production_event_sink sink{logger, identity()};
    const auto value = request();
    perf_tests::start_measuring_time();
    for (std::size_t index = 0; index < inner_iterations; ++index) {
        if (!sink.emit(value)) [[unlikely]] {
            throw std::logic_error("event benchmark disabled log");
        }
    }
    perf_tests::stop_measuring_time();
    perf_tests::do_not_optimize(sink.last_sequence());
    static_cast<void>(sink.stop());
    return inner_iterations;
}

} // namespace kwaque::observability
