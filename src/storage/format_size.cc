#include "src/storage/format_size.h"

#include "src/base/error.h"

#include <algorithm>

namespace kwaque::storage {
namespace {
result<void>
validate_header(byte_count bytes, const codec::limits& policy) noexcept {
    if (bytes.value() < codec::envelope_prefix_bytes)
        return failure(errc::invalid_argument);
    if (bytes > policy.config().max_header_bytes)
        return failure(errc::resource_exhausted);
    return {};
}
byte_count body_limit(
  const codec::limits& policy, codec::envelope_extent_limits owner) noexcept {
    return std::min(
      policy.config().max_encoded_body_bytes, owner.max_body_bytes);
}
} // namespace

byte_count
minimum_padding(byte_count bytes, storage_alignment alignment) noexcept {
    const auto multiple = alignment.bytes().value();
    const auto remainder = bytes.value() % multiple;
    return byte_count{remainder == 0 ? 0 : multiple - remainder};
}

result<byte_count> end_padding(
  runtime::file_position position,
  byte_count unpadded,
  storage_alignment alignment) noexcept {
    const auto end = byte_count{position.value()}.checked_add(unpadded);
    if (!end) return failure(errc::out_of_range);
    const auto padding = minimum_padding(*end, alignment);
    // The padded end must itself be a representable position.
    if (!end->checked_add(padding)) return failure(errc::out_of_range);
    return padding;
}

result<aligned_envelope_layout> aligned_envelope_layout::make(
  envelope_parts parts,
  storage_alignment alignment,
  const codec::limits& policy,
  codec::envelope_extent_limits owner_limits) noexcept {
    const auto unpadded = unpadded_bytes(parts, policy);
    if (!unpadded) return failure(unpadded.error());
    return checked(
      parts,
      *unpadded,
      minimum_padding(*unpadded, alignment),
      alignment,
      policy,
      owner_limits,
      std::nullopt);
}

result<aligned_envelope_layout> aligned_envelope_layout::make(
  envelope_parts parts,
  runtime::file_position position,
  envelope_placement placement,
  storage_alignment alignment,
  const codec::limits& policy,
  codec::envelope_extent_limits owner_limits) noexcept {
    const auto unpadded = unpadded_bytes(parts, policy);
    if (!unpadded) return failure(unpadded.error());
    // Placed envelopes are found by position, so their end must be one.
    if (!byte_count{position.value()}.checked_add(*unpadded))
        return failure(errc::out_of_range);
    byte_count padding{};
    if (placement == envelope_placement::ends_span) {
        const auto ending = end_padding(position, *unpadded, alignment);
        if (!ending) return failure(ending.error());
        padding = *ending;
    }
    return checked(
      parts, *unpadded, padding, alignment, policy, owner_limits, position);
}

result<byte_count> aligned_envelope_layout::unpadded_bytes(
  envelope_parts parts, const codec::limits& policy) noexcept {
    if (auto valid = validate_header(parts.header_bytes, policy); !valid)
        return failure(valid.error());
    const auto meaningful_body = parts.fixed_body_bytes.checked_add(
      parts.tail_bytes);
    if (!meaningful_body) return failure(errc::out_of_range);
    const auto unpadded = parts.header_bytes.checked_add(*meaningful_body);
    if (!unpadded) return failure(errc::out_of_range);
    return *unpadded;
}

result<aligned_envelope_layout> aligned_envelope_layout::checked(
  envelope_parts parts,
  byte_count unpadded,
  byte_count padding,
  storage_alignment alignment,
  const codec::limits& policy,
  codec::envelope_extent_limits owner_limits,
  std::optional<runtime::file_position> position) noexcept {
    const auto encoded = unpadded.checked_add(padding);
    if (!encoded) return failure(errc::out_of_range);
    // encoded >= header, so this subtraction is already proved safe.
    const byte_count body{encoded->value() - parts.header_bytes.value()};
    if (
      body > body_limit(policy, owner_limits)
      || *encoded > owner_limits.max_encoded_bytes)
        return failure(errc::resource_exhausted);
    return aligned_envelope_layout{
      parts.header_bytes, body, padding, alignment, position};
}

result<model::file_byte_span>
aligned_envelope_layout::at(runtime::file_position position) const noexcept {
    if (position_ ? position != *position_ : !alignment_.aligned(position))
        return failure(errc::invalid_argument);
    return model::file_byte_span::from_size(position, encoded_bytes());
}

result<byte_count> max_child_bytes(
  byte_count header_bytes,
  byte_count fixed_body_bytes,
  storage_alignment alignment,
  const codec::limits& policy,
  codec::envelope_extent_limits owner_limits) noexcept {
    if (auto valid = validate_header(header_bytes, policy); !valid)
        return failure(valid.error());
    const auto fixed = header_bytes.checked_add(fixed_body_bytes);
    if (!fixed) return failure(errc::out_of_range);
    const auto maximum = header_bytes.checked_add(
      body_limit(policy, owner_limits));
    if (!maximum) return failure(errc::out_of_range);
    const auto limit
      = std::min(*maximum, owner_limits.max_encoded_bytes).value();
    const auto aligned_limit = limit - limit % alignment.bytes().value();
    if (aligned_limit <= fixed->value())
        return failure(errc::resource_exhausted);
    return byte_count{aligned_limit - fixed->value()};
}

result<byte_count> max_child_bytes(
  byte_count header_bytes,
  byte_count fixed_body_bytes,
  envelope_placement placement,
  storage_alignment alignment,
  const codec::limits& policy,
  codec::envelope_extent_limits owner_limits) noexcept {
    if (auto valid = validate_header(header_bytes, policy); !valid)
        return failure(valid.error());
    auto fixed = header_bytes.checked_add(fixed_body_bytes);
    if (!fixed) return failure(errc::out_of_range);
    if (placement == envelope_placement::ends_span) {
        fixed = fixed->checked_add(byte_count{alignment.bytes().value() - 1U});
        if (!fixed) return failure(errc::out_of_range);
    }
    const auto maximum = header_bytes.checked_add(
      body_limit(policy, owner_limits));
    if (!maximum) return failure(errc::out_of_range);
    const auto limit
      = std::min(*maximum, owner_limits.max_encoded_bytes).value();
    if (limit <= fixed->value()) return failure(errc::resource_exhausted);
    return byte_count{limit - fixed->value()};
}

result<void> validate_zero_padding(
  std::span<const char> bytes, const codec::limits& policy) noexcept {
    if (bytes.size() > policy.config().max_work_bytes.value())
        return failure(errc::resource_exhausted);
    if (!std::ranges::all_of(bytes, [](char byte) { return byte == 0; }))
        return failure(errc::malformed_data);
    return {};
}

} // namespace kwaque::storage
