#pragma once

#include "src/codec/error.h"
#include "src/model/transport_identity.h"

#include <array>
#include <cstdint>

namespace kwaque::protocol {

inline constexpr std::uint16_t frame_protocol_version = 1;

enum class frame_kind : std::uint16_t {
    handshake_request = 1,
    handshake_response = 2,
    redirect = 3,
    error = 4,
    submitted_batch = 16,
    assigned_batch = 17,
};

enum class frame_payload_kind : std::uint8_t {
    control,
    submitted_batch,
    assigned_batch,
};

// Describes framing policy only. It neither validates a payload nor activates
// a protocol operation. Connection-control kinds use stream zero, raw kinds
// use nonzero streams. Every currently defined kind has flags zero.
struct frame_descriptor final {
    frame_kind kind;
    frame_payload_kind payload;

    [[nodiscard]] constexpr bool connection_control() const noexcept {
        return payload == frame_payload_kind::control;
    }
};

namespace detail {
inline constexpr std::array frame_descriptors{
  frame_descriptor{frame_kind::handshake_request, frame_payload_kind::control},
  frame_descriptor{frame_kind::handshake_response, frame_payload_kind::control},
  frame_descriptor{frame_kind::redirect, frame_payload_kind::control},
  frame_descriptor{frame_kind::error, frame_payload_kind::control},
  frame_descriptor{
    frame_kind::submitted_batch, frame_payload_kind::submitted_batch},
  frame_descriptor{
    frame_kind::assigned_batch, frame_payload_kind::assigned_batch}};
static_assert([] {
    std::uint16_t previous = 0;
    for (const auto descriptor : frame_descriptors) {
        const auto raw = static_cast<std::uint16_t>(descriptor.kind);
        if (raw <= previous) return false;
        previous = raw;
    }
    return true;
}());
} // namespace detail

[[nodiscard]] constexpr codec::result<frame_descriptor> lookup_frame_kind(
  std::uint16_t raw,
  codec::error anchor = codec::error{errc::success}) noexcept {
    for (const auto descriptor : detail::frame_descriptors) {
        if (static_cast<std::uint16_t>(descriptor.kind) == raw)
            return descriptor;
    }
    return codec::failure(
      codec::error{
        raw == 0 ? errc::malformed_data : errc::unsupported_format,
        anchor.family(),
        anchor.field(),
        anchor.byte_offset()});
}

// Plain scalar values, checked when written/read. Correlation and sequence
// retain their complete u64 domains. Matching/progression requires the
// connection owner's state, independently of producer stream identity.
struct frame_metadata final {
    frame_kind kind;
    model::transport_stream_id stream;
    model::correlation_id correlation;
    model::frame_sequence sequence;

    bool operator==(const frame_metadata&) const noexcept = default;
};

} // namespace kwaque::protocol
