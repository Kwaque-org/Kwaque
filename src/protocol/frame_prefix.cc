#include "src/base/error.h"
#include "src/base/invariant.h"
#include "src/protocol/frame_codec.h"

#include <seastar/core/byteorder.hh>

#include <algorithm>
#include <cstdint>
#include <limits>

namespace kwaque::protocol {
using codec::error;
using codec::field_context;
using codec::input_boundary;
using codec::limits;
using codec::result;
namespace {

constexpr std::uint32_t frame_magic = 0x4657514bU;

error prefix_error(
  errc code,
  field_context context,
  frame_field field,
  std::uint64_t offset) noexcept {
    return error{
      code, context.family, static_cast<std::uint16_t>(field), offset};
}

result<void> validate_prefix_work(
  const limits& policy, field_context context, std::uint64_t start) noexcept {
    const auto config = policy.config();
    if (
      frame_prefix_work_bytes > config.max_work_bytes
      || frame_prefix_work_items > config.max_work_items) {
        return codec::failure(prefix_error(
          errc::resource_exhausted, context, frame_field::magic, start));
    }
    return {};
}

// The fixed prefix is already present/representable. No kind lookup or
// variable input access occurs during this admission calculation.
result<void> validate_prefix_extents(
  std::uint16_t header_bytes,
  std::uint32_t payload_bytes,
  const limits& policy,
  frame_extent_limits owner_limits,
  field_context context,
  std::uint64_t start,
  errc arithmetic_error) noexcept {
    if (header_bytes < frame_prefix_bytes) {
        return codec::failure(prefix_error(
          errc::malformed_data, context, frame_field::header_bytes, start + 8));
    }
    const auto config = policy.config();
    if (header_bytes > config.max_header_bytes.value()) {
        return codec::failure(prefix_error(
          errc::resource_exhausted,
          context,
          frame_field::header_bytes,
          start + 8));
    }
    if (
      payload_bytes
      > std::min(config.max_encoded_body_bytes, owner_limits.max_payload_bytes)
          .value()) {
        return codec::failure(prefix_error(
          errc::resource_exhausted,
          context,
          frame_field::payload_bytes,
          start + 12));
    }
    const auto encoded_bytes = byte_count{header_bytes}.checked_add(
      byte_count{payload_bytes});
    if (
      !encoded_bytes
      || encoded_bytes->value()
           > std::numeric_limits<std::uint64_t>::max() - start) {
        return codec::failure(prefix_error(
          arithmetic_error, context, frame_field::encoded_bytes, start + 12));
    }
    if (
      *encoded_bytes
      > std::min(config.max_retained_bytes, owner_limits.max_encoded_bytes)) {
        return codec::failure(prefix_error(
          errc::resource_exhausted,
          context,
          frame_field::encoded_bytes,
          start + 12));
    }
    return {};
}

} // namespace

result<unverified_frame_prefix> peek_frame_prefix(
  const bytes::fragmented_buffer_parser& input,
  const limits& policy,
  frame_extent_limits owner_limits,
  field_context context,
  input_boundary boundary) {
    const auto start = codec::detail::integer_read_start(
      input, context, boundary);
    if (!start) {
        return codec::failure(start.error());
    }
    if (auto valid = validate_prefix_work(policy, context, *start); !valid) {
        return codec::failure(valid.error());
    }
    if (input.bytes_remaining().value() < frame_prefix_bytes) {
        return codec::failure(
          error{
            boundary == input_boundary::complete ? errc::malformed_data
                                                 : errc::truncated_data,
            context.family,
            static_cast<std::uint16_t>(frame_field::magic),
            context.origin + input.total_bytes().value()});
    }
    encoded_frame_prefix encoded{};
    const auto copied = input.peek_to(encoded);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-FRAME-PREFIX-PEEK"},
      copied.has_value(),
      "complete fixed prefix could not be inspected");
    if (seastar::read_le<std::uint32_t>(encoded.data()) != frame_magic) {
        return codec::failure(prefix_error(
          errc::malformed_data, context, frame_field::magic, *start));
    }
    const unverified_frame_prefix prefix{
      .protocol_version = seastar::read_le<std::uint16_t>(encoded.data() + 4),
      .kind = seastar::read_le<std::uint16_t>(encoded.data() + 6),
      .header_bytes = seastar::read_le<std::uint16_t>(encoded.data() + 8),
      .flags = seastar::read_le<std::uint16_t>(encoded.data() + 10),
      .payload_bytes = seastar::read_le<std::uint32_t>(encoded.data() + 12),
      .stream = seastar::read_le<std::uint64_t>(encoded.data() + 16),
      .correlation = seastar::read_le<std::uint64_t>(encoded.data() + 24),
      .sequence = seastar::read_le<std::uint64_t>(encoded.data() + 32),
      .header_crc32c = seastar::read_le<std::uint32_t>(
        encoded.data() + frame_header_crc_offset),
      .payload_crc32c = seastar::read_le<std::uint32_t>(
        encoded.data() + frame_payload_crc_offset)};
    if (
      auto valid = validate_prefix_extents(
        prefix.header_bytes,
        prefix.payload_bytes,
        policy,
        owner_limits,
        context,
        *start,
        errc::malformed_data);
      !valid) {
        return codec::failure(valid.error());
    }
    return prefix;
}

result<encoded_frame_prefix> encode_frame_prefix(
  frame_prefix_fields fields,
  const limits& policy,
  frame_extent_limits owner_limits,
  field_context context) {
    constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
    if (context.origin > maximum - frame_prefix_bytes) {
        return codec::failure(prefix_error(
          errc::invalid_argument, context, frame_field::magic, context.origin));
    }
    if (
      auto valid = validate_prefix_work(policy, context, context.origin);
      !valid) {
        return codec::failure(valid.error());
    }
    // Forged enum values and illegal stream combinations are caller errors.
    const auto kind = static_cast<std::uint16_t>(fields.metadata.kind);
    const auto descriptor = lookup_frame_kind(kind);
    if (!descriptor) {
        return codec::failure(prefix_error(
          errc::invalid_argument,
          context,
          frame_field::kind,
          context.origin + 6));
    }
    if (
      fields.payload_bytes.value()
      > std::numeric_limits<std::uint32_t>::max()) {
        return codec::failure(prefix_error(
          errc::invalid_argument,
          context,
          frame_field::payload_bytes,
          context.origin + 12));
    }
    const auto payload_bytes = static_cast<std::uint32_t>(
      fields.payload_bytes.value());
    if (
      auto valid = validate_prefix_extents(
        frame_prefix_bytes,
        payload_bytes,
        policy,
        owner_limits,
        context,
        context.origin,
        errc::invalid_argument);
      !valid) {
        return codec::failure(valid.error());
    }
    if (
      descriptor->connection_control()
      && fields.payload_bytes > policy.config().max_control_bytes) {
        return codec::failure(prefix_error(
          errc::resource_exhausted,
          context,
          frame_field::payload_bytes,
          context.origin + 12));
    }
    if (
      descriptor->connection_control()
      != (fields.metadata.stream.value() == 0)) {
        return codec::failure(prefix_error(
          errc::invalid_argument,
          context,
          frame_field::stream,
          context.origin + 16));
    }
    encoded_frame_prefix encoded{};
    seastar::write_le(encoded.data(), frame_magic);
    seastar::write_le(encoded.data() + 4, frame_protocol_version);
    seastar::write_le(encoded.data() + 6, kind);
    seastar::write_le(
      encoded.data() + 8, static_cast<std::uint16_t>(frame_prefix_bytes));
    seastar::write_le(encoded.data() + 10, std::uint16_t{0});
    seastar::write_le(encoded.data() + 12, payload_bytes);
    seastar::write_le(encoded.data() + 16, fields.metadata.stream.value());
    seastar::write_le(encoded.data() + 24, fields.metadata.correlation.value());
    seastar::write_le(encoded.data() + 32, fields.metadata.sequence.value());
    seastar::write_le(
      encoded.data() + frame_header_crc_offset, fields.header_crc32c);
    seastar::write_le(
      encoded.data() + frame_payload_crc_offset, fields.payload_crc32c);
    return encoded;
}

} // namespace kwaque::protocol
