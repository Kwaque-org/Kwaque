#include "src/protocol/control_schema.h"

#include <array>
#include <cstddef>
#include <limits>

namespace kwaque::protocol::detail {
namespace {
using message = control_message;
using kind = control_field_kind;
constexpr auto profile = codec::limits::defaults().config();
constexpr auto u16_max = std::numeric_limits<std::uint16_t>::max();
constexpr auto u64_max = std::numeric_limits<std::uint64_t>::max();

constexpr control_field scalar(
  std::uint32_t n, kind type, std::uint64_t minimum, std::uint64_t maximum) {
    return {
      .number = n,
      .kind = type,
      .required = true,
      .minimum_value = minimum,
      .maximum_value = maximum};
}
constexpr control_field enumeration(std::uint32_t n, std::uint64_t values) {
    return {
      .number = n,
      .kind = kind::enumeration,
      .required = true,
      .enum_values = values};
}
constexpr control_field identity(std::uint32_t n, bool required = true) {
    return {
      .number = n,
      .kind = kind::identity,
      .required = required,
      .minimum_bytes = control_identity_bytes,
      .maximum_bytes = control_identity_bytes};
}
constexpr control_field text(
  std::uint32_t n,
  byte_count maximum,
  bool required = false,
  byte_count minimum = {}) {
    return {
      .number = n,
      .kind = kind::text,
      .required = required,
      .minimum_bytes = minimum,
      .maximum_bytes = maximum};
}
constexpr control_field
embedded(std::uint32_t n, message child, bool required = true) {
    return {
      .number = n, .kind = kind::message, .required = required, .child = child};
}
constexpr control_field integers(
  std::uint32_t n,
  std::uint64_t minimum,
  std::uint64_t maximum,
  item_count count) {
    return {
      .number = n,
      .kind = kind::uint32,
      .repeated = true,
      .minimum_elements = item_count{1},
      .maximum_elements = count,
      .minimum_value = minimum,
      .maximum_value = maximum};
}
constexpr std::array build_fields{
  text(1, control_build_info_bytes),
  text(2, control_build_info_bytes),
  text(3, control_build_info_bytes)};
constexpr std::array routing_fields{
  identity(1),
  identity(2),
  scalar(3, kind::uint64, 1, u64_max),
  identity(4),
  scalar(5, kind::uint64, 1, u64_max)};
constexpr std::array format_fields{
  scalar(1, kind::uint32, 1, u16_max),
  scalar(2, kind::uint32, 1, u16_max),
  scalar(3, kind::uint32, 1, u16_max),
  scalar(4, kind::fixed64, 0, u64_max)};
constexpr std::array capability_fields{
  integers(1, 1, u16_max, control_protocol_versions),
  control_field{
    .number = 2,
    .kind = kind::message,
    .repeated = true,
    .minimum_elements = item_count{1},
    .maximum_elements = control_format_capabilities,
    .child = message::format_capability},
  integers(
    3, 0, std::numeric_limits<std::uint8_t>::max(), control_compression_codecs),
  scalar(4, kind::uint32, 1, profile.max_encoded_body_bytes.value()),
  scalar(5, kind::uint32, 1, profile.max_expanded_batch_bytes.value()),
  scalar(6, kind::uint32, 48, profile.max_header_bytes.value()),
  scalar(7, kind::uint32, 1, profile.max_record_bytes.value()),
  scalar(8, kind::uint32, 1, profile.max_original_records.value())};
constexpr std::array error_fields{
  enumeration(1, 0x3fe),
  text(2, control_reason_bytes),
  embedded(3, message::routing_context, false)};
constexpr std::array request_fields{
  enumeration(1, (1U << 1U) | (1U << 2U)),
  identity(2, false),
  identity(3, false),
  embedded(4, message::build_info),
  embedded(5, message::capabilities)};
constexpr std::array response_fields{
  identity(1),
  identity(2),
  embedded(3, message::build_info),
  embedded(4, message::capabilities)};
constexpr std::array endpoint_fields{
  text(1, control_host_bytes, true, byte_count{1}),
  scalar(2, kind::uint32, 1, u16_max)};
constexpr std::array redirect_fields{
  embedded(1, message::routing_context),
  identity(2),
  embedded(3, message::endpoint),
  enumeration(4, (1U << 3U) | (1U << 4U) | (1U << 9U))};

constexpr std::array schemas{
  control_schema{
    message::build_info,
    "kwaque.common.v1.BuildInfo",
    build_fields,
    control_build_info_bytes,
    true},
  control_schema{
    message::routing_context,
    "kwaque.common.v1.RoutingContext",
    routing_fields,
    profile.max_control_bytes},
  control_schema{
    message::format_capability,
    "kwaque.common.v1.FormatCapability",
    format_fields,
    profile.max_control_bytes},
  control_schema{
    message::capabilities,
    "kwaque.common.v1.Capabilities",
    capability_fields,
    profile.max_control_bytes},
  control_schema{
    message::error,
    "kwaque.common.v1.Error",
    error_fields,
    profile.max_control_bytes},
  control_schema{
    message::handshake_request,
    "kwaque.control.v1.HandshakeRequest",
    request_fields,
    profile.max_control_bytes},
  control_schema{
    message::handshake_response,
    "kwaque.control.v1.HandshakeResponse",
    response_fields,
    profile.max_control_bytes},
  control_schema{
    message::endpoint,
    "kwaque.control.v1.Endpoint",
    endpoint_fields,
    profile.max_control_bytes},
  control_schema{
    message::redirect,
    "kwaque.control.v1.Redirect",
    redirect_fields,
    profile.max_control_bytes}};
static_assert([] {
    for (std::size_t i = 0; i < schemas.size(); ++i) {
        if (schemas[i].fields.size() > control_scope_fields) return false;
        if (static_cast<std::size_t>(schemas[i].message) != i) return false;
        std::uint32_t previous = 0;
        for (const auto& field : schemas[i].fields) {
            if (field.number <= previous || field.maximum_elements.value() == 0)
                return false;
            // Owning-value sizing counts one byte per known field tag.
            // Unknown wire fields retain their full 29-bit number domain.
            if (field.number >= 16) return false;
            // New graph/storage shapes must extend the allocation proof.
            if (
              field.kind == kind::message
              && static_cast<std::size_t>(field.child) >= i)
                return false;
            if (
              field.repeated && field.kind != kind::uint32
              && field.kind != kind::message)
                return false;
            previous = field.number;
        }
    }
    return true;
}());
} // namespace

const control_schema* schema_for(control_message message) noexcept {
    const auto index = static_cast<std::size_t>(message);
    return index < schemas.size() ? &schemas[index] : nullptr;
}
const control_schema* schema_for(frame_kind kind) noexcept {
    switch (kind) {
    case frame_kind::handshake_request:
        return schema_for(message::handshake_request);
    case frame_kind::handshake_response:
        return schema_for(message::handshake_response);
    case frame_kind::redirect:
        return schema_for(message::redirect);
    case frame_kind::error:
        return schema_for(message::error);
    case frame_kind::submitted_batch:
    case frame_kind::assigned_batch:
        return nullptr;
    }
    return nullptr;
}
const control_field*
field_for(const control_schema& schema, std::uint32_t number) noexcept {
    for (const auto& field : schema.fields)
        if (field.number == number) return &field;
    return nullptr;
}
} // namespace kwaque::protocol::detail
