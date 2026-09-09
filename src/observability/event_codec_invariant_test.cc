#include "src/observability/event.h"
#include "src/observability/event_codec.h"
#include "src/observability/event_identity.h"
#include "src/observability/event_log.h"
#include "src/observability/event_sequence.h"
#include "src/observability/testing/event_sequence_test_access.h"
#include "src/resource/workload_class.h"
#include "src/runtime/time.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <span>
#include <stdexcept>

namespace {

using namespace kwaque::observability;

event_sink_identity identity() {
    return {
      .epoch = event_sink_epoch::make(1).value(), .configuration_digest = {}};
}

event make_event(std::span<const event_field> fields) {
    const auto request = event_request::make(
      event_request_context{
        .kind = event_kind::file_completion,
        .severity = event_severity::info,
        .monotonic = kwaque::runtime::monotonic_time{0},
        .wall = kwaque::runtime::wall_time{0},
        .workload = kwaque::resource::workload_class::metadata,
      },
      fields);
    if (!request) {
        throw std::runtime_error("event fixture request is invalid");
    }
    auto sequence = event_sequence_test_access::make(identity());
    auto reserved = event_sequence_test_access::prepare(*sequence, *request);
    if (!reserved) {
        throw std::runtime_error("event fixture reservation failed");
    }
    auto value = reserved->value();
    reserved->commit();
    return value;
}

constexpr std::array byte_field{
  make_event_field<event_kind::file_completion, event_field_key::bytes>(
    std::uint64_t{1})};

TEST(EventCodecInvariantDeathTest, RejectsCorruptValidatedFieldDescriptor) {
    auto value = make_event(byte_field);
    // Simulate corruption after validation without adding a mutable public API.
    const_cast<event_field&>(value.fields().front()).key
      = static_cast<event_field_key>(0);
    EXPECT_DEATH(
      static_cast<void>(encode_event(value)), "id=KQ-EVENT-ENCODE-FIELD");
}

TEST(EventCodecInvariantDeathTest, RejectsCorruptValidatedFieldSize) {
    auto value = make_event(byte_field);
    const_cast<event_field&>(value.fields().front()).value
      = event_field_value::from_boolean(true);
    EXPECT_DEATH(
      static_cast<void>(encode_event(value)), "id=KQ-EVENT-ENCODE-SIZE");
}

TEST(EventCodecInvariantDeathTest, RejectsCorruptLogByteAccounting) {
    event_log log{identity(), event_log_limits::defaults()};
    ASSERT_TRUE(log.append(make_event({})).has_value());
    // Each event is valid, but replacing an admitted entry invalidates the
    // log's owned byte accounting.
    const_cast<event&>(log.entries()[0]) = make_event(byte_field);
    EXPECT_DEATH(
      static_cast<void>(log.encode()), "id=KQ-EVENT-LOG-ENCODE-SIZE");
}

} // namespace
