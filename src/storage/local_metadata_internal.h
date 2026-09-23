#pragma once

#include "src/storage/local_metadata.h"
#include "src/storage/page_internal.h"
#include "src/storage/retry_entry_internal.h"

namespace kwaque::storage::detail {
inline constexpr auto local_family = codec::format_family::local_storage;
[[nodiscard]] result<byte_count>
local_payload_bytes(const local_metadata_payload&) noexcept;
[[nodiscard]] std::optional<segment_context>
local_payload_segment(const local_metadata_payload&) noexcept;
[[nodiscard]] codec::result<void> check_local_root_counts(
  std::uint32_t total,
  std::size_t pages,
  const codec::limits&,
  codec::field_context);
[[nodiscard]] codec::result<void> check_local_page_fields(
  local_publication_generation,
  local_object_sequence,
  page_ordinal,
  std::uint32_t first,
  std::size_t count,
  const codec::limits&,
  codec::field_context);
[[nodiscard]] seastar::future<codec::result<void>> validate_local_payload(
  local_metadata_header,
  const local_metadata_payload&,
  aligned_envelope_layout,
  std::optional<storage_alignment> segment_alignment,
  std::optional<storage_alignment> data_metadata_alignment,
  codec::cooperative_work&,
  codec::field_context,
  bool writing = false);
[[nodiscard]] codec::result<void> validate_local_expectation(
  const local_metadata_expectation&, codec::field_context);
[[nodiscard]] codec::result<void> match_local_expectation(
  const local_metadata_expectation&,
  local_metadata_header,
  const local_metadata_payload&,
  byte_count,
  codec::field_context);

// Preserve unsupported scalar formats; invalid wire values remain malformed.
inline codec::result<void> local_wire(
  result<void> value, codec::field_context c, std::uint64_t offset = 0) {
    if (value) return {};
    const auto reason = value.error() == errc::unsupported_format
                          ? errc::unsupported_format
                        : value.error() == errc::resource_exhausted
                          ? errc::resource_exhausted
                          : errc::malformed_data;
    return codec::failure(page_error(reason, c, offset));
}
template<typename T>
codec::result<T>
local_wire(result<T> value, codec::field_context c, std::uint64_t offset = 0) {
    if (!value && value.error() == errc::unsupported_format)
        return codec::failure(page_error(errc::unsupported_format, c, offset));
    return page_wire(std::move(value), c, offset);
}
template<std::size_t Offset, std::size_t N>
codec::result<segment_context>
read_local_segment(const std::array<char, N>& raw, codec::field_context c) {
    const auto cluster = read_id<Offset, model::cluster_id>(raw, c);
    const auto topic = read_id<Offset + 16, model::topic_id>(raw, c);
    const auto range = read_id<Offset + 32, model::range_id>(raw, c);
    const auto segment = read_id<Offset + 48, model::segment_id>(raw, c);
    const auto generation = page_wire(
      model::segment_generation::make(load<Offset + 64, std::uint64_t>(raw)),
      c,
      Offset + 64);
    if (!cluster) return codec::failure(cluster.error());
    if (!topic) return codec::failure(topic.error());
    if (!range) return codec::failure(range.error());
    if (!segment) return codec::failure(segment.error());
    if (!generation) return codec::failure(generation.error());
    return page_wire(
      segment_context::make(*cluster, *topic, *range, *segment, *generation),
      c,
      Offset);
}
template<std::size_t Offset, std::size_t N>
codec::result<local_wal_cursor>
read_local_cursor(const std::array<char, N>& raw, codec::field_context c) {
    auto id = read_id<Offset, model::wal_incarnation_id>(raw, c);
    if (!id) return codec::failure(id.error());
    return page_wire(
      local_wal_cursor::make(
        *id, runtime::file_position{load<Offset + 16, std::uint64_t>(raw)}),
      c,
      Offset);
}
template<std::size_t Offset, std::size_t N>
void write_local_cursor(
  std::array<char, N>& out, local_wal_cursor cursor) noexcept {
    write_id<Offset>(out, cursor.incarnation());
    store<Offset + 16>(out, cursor.position().value());
}
[[nodiscard]] codec::result<local_root_reference>
read_local_root(const std::array<char, 60>&, codec::field_context);
void write_local_root(
  std::array<char, 60>&, const local_root_reference&) noexcept;
template<std::size_t Offset, std::size_t N>
void write_local_footer(
  std::array<char, N>& out, const local_footer_reference& footer) noexcept {
    store<Offset>(out, footer.position().value());
    store<Offset + 8>(out, static_cast<std::uint32_t>(footer.bytes().value()));
    store<Offset + 12>(out, footer.family());
    write_digest<Offset + 16>(out, footer.digest());
}
template<std::size_t Offset, std::size_t N>
codec::result<local_footer_reference>
read_local_footer(const std::array<char, N>& raw, codec::field_context c) {
    if (load<Offset + 14, std::uint16_t>(raw) != 0)
        return codec::failure(page_error(errc::malformed_data, c, Offset + 14));
    return local_wire(
      local_footer_reference::make(
        runtime::file_position{load<Offset, std::uint64_t>(raw)},
        byte_count{load<Offset + 8, std::uint32_t>(raw)},
        load<Offset + 12, std::uint16_t>(raw),
        codec::immutable_object_digest{read_digest<Offset + 16>(raw)}),
      c,
      Offset);
}
[[nodiscard]] codec::result<local_checkpoint_entry>
read_local_checkpoint_entry(const std::array<char, 164>&, codec::field_context);
void write_local_checkpoint_entry(
  std::array<char, 164>&, const local_checkpoint_entry&) noexcept;
[[nodiscard]] codec::result<local_deletion_object>
read_local_deletion_object(const std::array<char, 12>&, codec::field_context);
void write_local_deletion_object(
  std::array<char, 12>&, const local_deletion_object&) noexcept;
} // namespace kwaque::storage::detail
