#pragma once

#include "src/storage/format_size.h"

#include <array>
#include <cstdint>

namespace kwaque::storage {
inline constexpr byte_count local_metadata_prefix_bytes{72};
inline constexpr byte_count local_metadata_max_bytes{65536};
inline constexpr byte_count local_root_reference_bytes{60};
inline constexpr byte_count local_footer_reference_bytes{48};
inline constexpr byte_count local_wal_cursor_bytes{24};
inline constexpr byte_count local_checkpoint_entry_bytes{164};
inline constexpr byte_count local_deletion_object_bytes{12};
inline constexpr std::uint32_t local_deletion_objects_max = 512;

enum class local_metadata_kind : std::uint16_t {
    store_identity = 1,
    shard_control = 2,
    wal_descriptor = 3,
    segment_descriptor = 4,
    object_publication = 5,
    checkpoint_root = 6,
    checkpoint_page = 7,
    deletion_intent = 8,
    recovery_decision = 9,
    boundary_evidence = 10,
    completed_retry_root = 11,
    completed_retry_page = 12,
};
struct local_metadata_descriptor final {
    local_metadata_kind kind;
    // Payload excludes the common prefix and padding. Presence fields can
    // extend the fixed part; repeated fields use the exact entry width.
    std::uint32_t minimum_payload_bytes;
    std::uint32_t maximum_fixed_payload_bytes;
    std::uint32_t entry_bytes;
    std::uint32_t maximum_entries;
};
inline constexpr std::array local_metadata_descriptors{
  local_metadata_descriptor{local_metadata_kind::store_identity, 16, 16, 0, 0},
  local_metadata_descriptor{local_metadata_kind::shard_control, 48, 156, 0, 0},
  local_metadata_descriptor{local_metadata_kind::wal_descriptor, 44, 68, 0, 0},
  local_metadata_descriptor{
    local_metadata_kind::segment_descriptor, 120, 120, 0, 0},
  local_metadata_descriptor{
    local_metadata_kind::object_publication, 88, 136, 60, 4},
  local_metadata_descriptor{
    local_metadata_kind::checkpoint_root, 56, 56, 48, 256},
  local_metadata_descriptor{
    local_metadata_kind::checkpoint_page, 20, 20, 164, 65536},
  local_metadata_descriptor{
    local_metadata_kind::deletion_intent, 96, 96, 12, 512},
  local_metadata_descriptor{
    local_metadata_kind::recovery_decision, 148, 148, 0, 0},
  local_metadata_descriptor{
    local_metadata_kind::boundary_evidence, 188, 248, 0, 0},
  local_metadata_descriptor{
    local_metadata_kind::completed_retry_root, 128, 128, 48, 256},
  local_metadata_descriptor{
    local_metadata_kind::completed_retry_page, 92, 92, 160, 65536},
};
[[nodiscard]] result<local_metadata_descriptor>
local_metadata_descriptor_for(std::uint16_t) noexcept;
[[nodiscard]] result<aligned_envelope_layout> local_metadata_layout(
  local_metadata_kind,
  byte_count payload_bytes,
  byte_count header_bytes,
  storage_alignment,
  const codec::limits&) noexcept;
[[nodiscard]] result<std::uint32_t> local_metadata_page_capacity(
  local_metadata_kind,
  byte_count header_bytes,
  storage_alignment,
  const codec::limits&) noexcept;
} // namespace kwaque::storage
