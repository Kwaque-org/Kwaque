#include "src/storage/local_metadata_layout.h"

#include <algorithm>

namespace kwaque::storage {
result<local_metadata_descriptor>
local_metadata_descriptor_for(std::uint16_t raw) noexcept {
    if (raw == 0) return failure(errc::invalid_argument);
    for (const auto descriptor : local_metadata_descriptors)
        if (static_cast<std::uint16_t>(descriptor.kind) == raw)
            return descriptor;
    return failure(errc::unsupported_format);
}
result<aligned_envelope_layout> local_metadata_layout(
  local_metadata_kind kind,
  byte_count payload,
  byte_count header,
  storage_alignment alignment,
  const codec::limits& policy) noexcept {
    const auto descriptor = local_metadata_descriptor_for(
      static_cast<std::uint16_t>(kind));
    if (!descriptor) return failure(descriptor.error());
    if (payload.value() < descriptor->minimum_payload_bytes)
        return failure(errc::invalid_argument);
    const auto maximum = static_cast<std::uint64_t>(
                           descriptor->maximum_fixed_payload_bytes)
                         + static_cast<std::uint64_t>(descriptor->entry_bytes)
                             * descriptor->maximum_entries;
    if (payload.value() > maximum)
        return failure(
          descriptor->entry_bytes == 0 ? errc::invalid_argument
                                       : errc::resource_exhausted);
    const auto bytes = payload.value();
    bool shape = false;
    switch (kind) {
    case local_metadata_kind::shard_control:
        shape = bytes == 48 || bytes == 96 || bytes == 108 || bytes == 156;
        break;
    case local_metadata_kind::wal_descriptor:
        shape = bytes == 44 || bytes == 68;
        break;
    case local_metadata_kind::object_publication:
        shape
          = ((bytes - 88) % 60 == 0 && (bytes - 88) / 60 <= 4)
            || (bytes >= 136 && (bytes - 136) % 60 == 0 && (bytes - 136) / 60 <= 4);
        break;
    case local_metadata_kind::boundary_evidence:
        shape = bytes == 188 || bytes == 248;
        break;
    default:
        if (descriptor->entry_bytes == 0)
            shape = bytes == descriptor->minimum_payload_bytes;
        else {
            const auto tail = bytes - descriptor->minimum_payload_bytes;
            const bool nonempty
              = kind == local_metadata_kind::checkpoint_page
                || kind == local_metadata_kind::completed_retry_page
                || kind == local_metadata_kind::deletion_intent;
            shape = tail % descriptor->entry_bytes == 0
                    && (!nonempty || tail != 0);
        }
        break;
    }
    if (!shape) return failure(errc::invalid_argument);
    const auto cap = std::min(
      local_metadata_max_bytes, policy.config().max_page_bytes);
    return aligned_envelope_layout::make(
      {header, local_metadata_prefix_bytes, payload},
      alignment,
      policy,
      {cap, cap});
}
result<std::uint32_t> local_metadata_page_capacity(
  local_metadata_kind kind,
  byte_count header,
  storage_alignment alignment,
  const codec::limits& policy) noexcept {
    if (
      kind != local_metadata_kind::checkpoint_page
      && kind != local_metadata_kind::completed_retry_page)
        return failure(errc::invalid_argument);
    const auto descriptor
      = local_metadata_descriptor_for(static_cast<std::uint16_t>(kind)).value();
    const auto cap = std::min(
      local_metadata_max_bytes, policy.config().max_page_bytes);
    auto remaining = max_child_bytes(
      header,
      byte_count{
        local_metadata_prefix_bytes.value() + descriptor.minimum_payload_bytes},
      alignment,
      policy,
      {cap, cap});
    if (!remaining) return failure(remaining.error());
    const auto count = std::min<std::uint64_t>(
      remaining->value() / descriptor.entry_bytes,
      policy.config().max_object_entries.value());
    if (count == 0) return failure(errc::resource_exhausted);
    return static_cast<std::uint32_t>(count);
}
} // namespace kwaque::storage
