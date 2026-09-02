#include "src/observability/event.h"
#include "src/observability/event_log.h"
#include "src/observability/event_sequence.h"
#include "src/observability/event_sink.h"
#include "src/observability/testing/capture_event_sink.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/log.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <sstream>
#include <streambuf>
#include <string>

namespace {

using kwaque::observability::event_configuration_digest;
using kwaque::observability::event_field;
using kwaque::observability::event_field_key;
using kwaque::observability::event_field_value;
using kwaque::observability::event_kind;
using kwaque::observability::event_log_limit_values;
using kwaque::observability::event_log_limits;
using kwaque::observability::event_public_text;
using kwaque::observability::event_request;
using kwaque::observability::event_request_context;
using kwaque::observability::event_severity;
using kwaque::observability::event_sink_epoch;
using kwaque::observability::event_sink_identity;
using kwaque::observability::event_text;
using kwaque::observability::production_event_sink;
using kwaque::observability::testing::capture_event_sink;

event_text text(event_public_text value) {
    auto made = event_text::make(value);
    BOOST_REQUIRE(made.has_value());
    return *made;
}

event_sink_identity identity(std::uint64_t value = 7) {
    auto epoch = event_sink_epoch::make(value);
    BOOST_REQUIRE(epoch.has_value());
    event_configuration_digest digest{};
    digest[0] = 0x42;
    return event_sink_identity{
      .epoch = *epoch,
      .configuration_digest = digest,
    };
}

event_request make_event() {
    const std::array fields{
      event_field{
        .key = event_field_key::state,
        .value = event_field_value::from_text(
          text(event_public_text::state_ready))},
      event_field{
        .key = event_field_key::operation,
        .value = event_field_value::from_text(
          text(event_public_text::operation_environment_start))},
    };
    auto made = event_request::make(
      event_request_context{
        .kind = event_kind::runtime_state_changed,
        .severity = event_severity::info,
        .monotonic = kwaque::runtime::monotonic_time{11},
        .wall = kwaque::runtime::wall_time{-4},
        .workload = kwaque::resource::workload_class::metadata,
      },
      fields);
    BOOST_REQUIRE(made.has_value());
    return *made;
}

event_log_limits limits(std::uint32_t entries) {
    auto made = event_log_limits::make(
      event_log_limit_values{
        .entries = entries,
        .encoded_bytes = 4'096,
      });
    BOOST_REQUIRE(made.has_value());
    return *made;
}

class ostream_guard final {
public:
    explicit ostream_guard(std::ostream& output) noexcept {
        seastar::logger::set_ostream(output);
        seastar::logger::set_ostream_enabled(true);
        seastar::logger::set_syslog_enabled(false);
    }

    ~ostream_guard() { seastar::logger::set_ostream(std::cerr); }
};

class null_stream_buffer final : public std::streambuf {
protected:
    std::streamsize xsputn(const char*, std::streamsize count) override {
        return count;
    }

    int_type overflow(int_type character) override {
        return traits_type::not_eof(character);
    }
};

static_assert(kwaque::observability::event_sink<production_event_sink>);
static_assert(kwaque::observability::event_sink<capture_event_sink>);

} // namespace

SEASTAR_TEST_CASE(production_sink_uses_native_writer_and_level_gate) {
    std::ostringstream output;
    ostream_guard restore_output{output};
    seastar::logger logger{"kwaque-event-test"};
    logger.set_level(seastar::log_level::trace);
    production_event_sink sink{logger, identity()};
    const auto value = make_event();

    BOOST_REQUIRE(sink.emit(value).has_value());
    const std::string rendered = output.str();
    BOOST_CHECK(
      rendered.find("event=runtime_state_changed") != std::string::npos);
    BOOST_CHECK(rendered.find("schema=1") != std::string::npos);
    BOOST_CHECK(rendered.find("epoch=7") != std::string::npos);
    BOOST_CHECK(rendered.find("monotonic_ns=11") != std::string::npos);
    BOOST_CHECK(rendered.find("wall_ns=-4") != std::string::npos);
    BOOST_CHECK(rendered.find("workload=metadata") != std::string::npos);
    BOOST_CHECK(rendered.find("sequence=1") != std::string::npos);
    const auto state_position = rendered.find("state=ready");
    const auto operation_position = rendered.find(
      "operation=environment_start");
    BOOST_CHECK(state_position != std::string::npos);
    BOOST_CHECK(operation_position != std::string::npos);
    BOOST_CHECK(state_position < operation_position);

    null_stream_buffer null_buffer;
    std::ostream null_output{&null_buffer};
    seastar::logger::set_ostream(null_output);
    std::size_t enabled_attempts = 0;
    bool enabled_emit_succeeded = false;
    seastar::memory::with_allocation_failures([&] {
        ++enabled_attempts;
        enabled_emit_succeeded = sink.emit(value).has_value();
    });
    BOOST_CHECK(enabled_emit_succeeded);
    BOOST_CHECK(enabled_attempts == 1U);

    seastar::logger::set_ostream(output);
    output.str(std::string{});
    output.clear();
    logger.set_level(seastar::log_level::error);
    std::size_t disabled_attempts = 0;
    bool disabled_emit_succeeded = false;
    seastar::memory::with_allocation_failures([&] {
        ++disabled_attempts;
        disabled_emit_succeeded = sink.emit(value).has_value();
    });
    BOOST_CHECK(disabled_emit_succeeded);
    BOOST_CHECK(disabled_attempts == 1U);
    BOOST_CHECK(output.str().empty());
    BOOST_CHECK(sink.last_sequence() == 3U);

    BOOST_REQUIRE(sink.stop().has_value());
    BOOST_REQUIRE(sink.stop().has_value());
    BOOST_CHECK(sink.stopped());
    const auto after_stop = sink.emit(value);
    BOOST_REQUIRE(!after_stop.has_value());
    BOOST_CHECK(after_stop.error().code() == kwaque::errc::closed);
    co_return;
}

SEASTAR_TEST_CASE(capture_sink_is_reserved_bounded_and_stoppable) {
    const auto sink_identity = identity();
    bool construction_completed = false;
    seastar::memory::with_allocation_failures([&] {
        capture_event_sink constructed{sink_identity, limits(2)};
        construction_completed = constructed.stop().has_value();
    });
    BOOST_CHECK(construction_completed);

    capture_event_sink sink{sink_identity, limits(2)};
    const auto value = make_event();
    BOOST_REQUIRE(sink.emit(value).has_value());
    std::size_t emit_attempts = 0;
    bool second_emit_succeeded = false;
    seastar::memory::with_allocation_failures([&] {
        ++emit_attempts;
        second_emit_succeeded = sink.emit(value).has_value();
    });
    BOOST_CHECK(second_emit_succeeded);
    BOOST_CHECK(emit_attempts == 1U);
    BOOST_CHECK(sink.events().entries().size() == 2U);
    BOOST_CHECK(sink.events().identity() == sink_identity);
    BOOST_CHECK(sink.events().entries()[0].sequence() == 1U);
    BOOST_CHECK(sink.events().entries()[1].sequence() == 2U);
    BOOST_CHECK(sink.events().entries()[0].kind() == value.kind());
    BOOST_CHECK(
      std::ranges::equal(sink.events().entries()[1].fields(), value.fields()));
    const auto saturated = sink.emit(value);
    BOOST_REQUIRE(!saturated.has_value());
    BOOST_CHECK(saturated.error().code() == kwaque::errc::resource_exhausted);
    BOOST_CHECK(sink.last_sequence() == 2U);

    const auto encoded = sink.events().encode();
    BOOST_REQUIRE(encoded.has_value());
    const auto decoded = kwaque::observability::event_log::decode(
      *encoded, sink.events().limits());
    BOOST_REQUIRE(decoded.has_value());
    BOOST_CHECK((*decoded)->entries()[0] == sink.events().entries()[0]);

    BOOST_REQUIRE(sink.stop().has_value());
    BOOST_REQUIRE(sink.stop().has_value());
    const auto after_stop = sink.emit(value);
    BOOST_REQUIRE(!after_stop.has_value());
    BOOST_CHECK(after_stop.error().code() == kwaque::errc::closed);
    co_return;
}
