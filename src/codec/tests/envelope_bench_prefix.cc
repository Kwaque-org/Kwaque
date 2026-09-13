#include "src/codec/tests/envelope_bench_native.h"

namespace kwaque::codec::bench::checked {

namespace {

constexpr std::uint32_t envelope_magic = 0x4642514bU;

error prefix_error(
  errc code,
  field_context context,
  envelope_field field,
  std::uint64_t offset) noexcept {
    return error{
      code, context.family, static_cast<std::uint16_t>(field), offset};
}

result<void> validate_prefix_work(
  const limits& policy, field_context context, std::uint64_t start) noexcept {
    const auto config = policy.config();
    if (
      envelope_prefix_work_bytes > config.max_work_bytes
      || envelope_prefix_work_items > config.max_work_items) {
        return codec::failure(prefix_error(
          errc::resource_exhausted, context, envelope_field::magic, start));
    }
    return {};
}

// The fixed prefix is already present/representable. No family lookup or
// variable input access occurs during this admission calculation.
result<void> validate_prefix_extents(
  std::uint16_t header_bytes,
  std::uint32_t body_bytes,
  const limits& policy,
  envelope_extent_limits owner_limits,
  field_context context,
  std::uint64_t start,
  errc arithmetic_error) noexcept {
    if (header_bytes < envelope_prefix_bytes) {
        return codec::failure(prefix_error(
          errc::malformed_data,
          context,
          envelope_field::header_bytes,
          start + 10));
    }
    const auto config = policy.config();
    if (header_bytes > config.max_header_bytes.value()) {
        return codec::failure(prefix_error(
          errc::resource_exhausted,
          context,
          envelope_field::header_bytes,
          start + 10));
    }
    if (
      body_bytes
      > std::min(config.max_encoded_body_bytes, owner_limits.max_body_bytes)
          .value()) {
        return codec::failure(prefix_error(
          errc::resource_exhausted,
          context,
          envelope_field::body_bytes,
          start + 12));
    }
    const auto encoded_bytes = byte_count{header_bytes}.checked_add(
      byte_count{body_bytes});
    if (
      !encoded_bytes
      || encoded_bytes->value()
           > std::numeric_limits<std::uint64_t>::max() - start) {
        return codec::failure(prefix_error(
          arithmetic_error,
          context,
          envelope_field::encoded_bytes,
          start + 12));
    }
    if (
      *encoded_bytes
      > std::min(config.max_retained_bytes, owner_limits.max_encoded_bytes)) {
        return codec::failure(prefix_error(
          errc::resource_exhausted,
          context,
          envelope_field::encoded_bytes,
          start + 12));
    }
    return {};
}

} // namespace

result<unverified_envelope_prefix> native_prefix_read(
  const bytes::fragmented_buffer_parser& input,
  const limits& policy,
  envelope_extent_limits owner_limits,
  field_context context,
  input_boundary boundary) {
    const auto start = detail::integer_read_start(input, context, boundary);
    if (!start) {
        return codec::failure(start.error());
    }
    if (auto valid = validate_prefix_work(policy, context, *start); !valid) {
        return codec::failure(valid.error());
    }
    if (input.bytes_remaining().value() < envelope_prefix_bytes) {
        return codec::failure(
          error{
            boundary == input_boundary::complete ? errc::malformed_data
                                                 : errc::truncated_data,
            context.family,
            static_cast<std::uint16_t>(envelope_field::magic),
            context.origin + input.total_bytes().value()});
    }
    encoded_envelope_prefix encoded{};
    const auto copied = input.peek_to(encoded);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-ENVELOPE-PREFIX-PEEK"},
      copied.has_value(),
      "complete fixed prefix could not be inspected");
    if (native_load<std::uint32_t>(encoded.data()) != envelope_magic) {
        return codec::failure(prefix_error(
          errc::malformed_data, context, envelope_field::magic, *start));
    }
    const unverified_envelope_prefix prefix{
      .family = native_load<std::uint16_t>(encoded.data() + 4),
      .writer_version = native_load<std::uint16_t>(encoded.data() + 6),
      .minimum_reader_version = native_load<std::uint16_t>(encoded.data() + 8),
      .header_bytes = native_load<std::uint16_t>(encoded.data() + 10),
      .body_bytes = native_load<std::uint32_t>(encoded.data() + 12),
      .required_features = native_load<std::uint64_t>(encoded.data() + 16),
      .body_crc32c = native_load<std::uint32_t>(encoded.data() + 24),
      .header_crc32c = native_load<std::uint32_t>(encoded.data() + 28)};
    if (
      auto valid = validate_prefix_extents(
        prefix.header_bytes,
        prefix.body_bytes,
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

result<encoded_envelope_prefix> native_prefix_write(
  envelope_prefix_fields fields,
  const limits& policy,
  envelope_extent_limits owner_limits,
  field_context context) {
    constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
    if (context.origin > maximum - envelope_prefix_bytes) {
        return codec::failure(prefix_error(
          errc::invalid_argument,
          context,
          envelope_field::magic,
          context.origin));
    }
    if (
      auto valid = validate_prefix_work(policy, context, context.origin);
      !valid) {
        return codec::failure(valid.error());
    }
    // A forged enum value is a caller error, not permission to emit an unknown
    // family. Read-side compatibility is deliberately absent from this leaf.
    const auto family = static_cast<std::uint16_t>(fields.family);
    const auto descriptor = lookup_format(family);
    if (!descriptor) {
        return codec::failure(prefix_error(
          errc::invalid_argument,
          context,
          envelope_field::family,
          context.origin + 4));
    }
    if (fields.body_bytes.value() > std::numeric_limits<std::uint32_t>::max()) {
        return codec::failure(prefix_error(
          errc::invalid_argument,
          context,
          envelope_field::body_bytes,
          context.origin + 12));
    }
    const auto body_bytes = static_cast<std::uint32_t>(
      fields.body_bytes.value());
    if (
      auto valid = validate_prefix_extents(
        envelope_prefix_bytes,
        body_bytes,
        policy,
        owner_limits,
        context,
        context.origin,
        errc::invalid_argument);
      !valid) {
        return codec::failure(valid.error());
    }
    encoded_envelope_prefix encoded{};
    native_store(encoded.data(), envelope_magic);
    native_store(encoded.data() + 4, family);
    native_store(encoded.data() + 6, descriptor->writer());
    native_store(encoded.data() + 8, descriptor->minimum_reader());
    native_store(
      encoded.data() + 10, static_cast<std::uint16_t>(envelope_prefix_bytes));
    native_store(encoded.data() + 12, body_bytes);
    native_store(encoded.data() + 16, std::uint64_t{0});
    native_store(encoded.data() + 24, fields.body_crc32c);
    native_store(encoded.data() + 28, fields.header_crc32c);
    return encoded;
}

} // namespace kwaque::codec::bench::checked
