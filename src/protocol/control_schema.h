#pragma once

#include "src/codec/limits.h"
#include "src/protocol/frame.h"

#include <cstddef>
#include <cstdint>
#include <span>

namespace kwaque::protocol::detail {

enum class control_message : std::uint8_t {
    build_info,
    routing_context,
    format_capability,
    capabilities,
    error,
    handshake_request,
    handshake_response,
    endpoint,
    redirect,
};

enum class control_field_kind : std::uint8_t {
    uint32,
    uint64,
    fixed64,
    enumeration,
    identity,
    // Known text must pass bounded UTF-8 validation before generated parsing.
    // Unknown length-delimited fields remain opaque bytes.
    text,
    message,
};

inline constexpr byte_count control_identity_bytes{16};
inline constexpr byte_count control_build_info_bytes{4096};
inline constexpr byte_count control_reason_bytes{1024};
inline constexpr byte_count control_host_bytes{253};
inline constexpr item_count control_protocol_versions{16};
inline constexpr item_count control_format_capabilities{32};
inline constexpr item_count control_compression_codecs{16};
inline constexpr std::size_t control_scope_fields = 8;

// Immutable schema policy, independent of generated message objects. Byte and
// element ceilings intersect the caller's limits; they grant no new allowance.
// Advertised numeric limits describe the peer and retain their absolute field
// domains rather than being rewritten to the receiver's smaller limits.
// Conditional broker/observed context, nonnil IDs, host rules, ordered
// sets and independent expectations still require semantic validation.
struct control_field final {
    std::uint32_t number;
    control_field_kind kind;
    bool required{false};
    bool repeated{false};
    item_count minimum_elements;
    item_count maximum_elements{1};
    byte_count minimum_bytes;
    byte_count maximum_bytes;
    std::uint64_t minimum_value{0};
    std::uint64_t maximum_value{0};
    // Bit n permits enum number n. Unknown enums remain visible to validation;
    // generated proto3 enum membership alone does not establish this domain.
    std::uint64_t enum_values{0};
    control_message child{control_message::build_info};
};

struct control_schema final {
    control_message message;
    const char* full_name;
    std::span<const control_field> fields;
    byte_count maximum_wire_bytes;
    // Legacy informational strings merge, and other wire types are unknowns.
    // New schemas instead reject duplicate singulars and known wrong types.
    bool informational_merge{false};
};

[[nodiscard]] const control_schema* schema_for(control_message) noexcept;
[[nodiscard]] const control_schema* schema_for(frame_kind) noexcept;
[[nodiscard]] const control_field*
field_for(const control_schema&, std::uint32_t number) noexcept;

} // namespace kwaque::protocol::detail
