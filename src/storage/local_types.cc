#include "src/storage/local_types.h"

#include <algorithm>

namespace kwaque::storage {
result<local_wal_high>
local_wal_high::make(std::span<const std::uint8_t> bytes) noexcept {
    if (bytes.size() != 16) return failure(errc::invalid_argument);
    local_wal_high value;
    std::copy(bytes.begin(), bytes.end(), value.bytes_.begin());
    return value;
}
result<local_wal_high>
local_wal_high::from_incarnation(model::wal_incarnation_id id) noexcept {
    if (id.is_nil()) return failure(errc::invalid_argument);
    return make(id.bytes());
}
bool local_wal_high::empty() const noexcept {
    return std::all_of(
      bytes_.begin(), bytes_.end(), [](auto b) { return b == 0; });
}
result<model::wal_incarnation_id> local_wal_high::incarnation() const noexcept {
    return model::wal_incarnation_id::make(bytes_);
}
result<local_wal_high>
local_wal_high::checked_advance(std::uint64_t count) const noexcept {
    if (count == 0) return failure(errc::invalid_argument);
    auto next = *this;
    for (std::size_t at = next.bytes_.size(); at != 0; --at) {
        const auto sum = static_cast<std::uint16_t>(next.bytes_[at - 1])
                         + (count & 0xffU);
        next.bytes_[at - 1] = static_cast<std::uint8_t>(sum);
        count = (count >> 8U) + (sum >> 8U);
    }
    if (count != 0) return failure(errc::out_of_range);
    return next;
}
result<local_store_context> local_store_context::make(
  model::cluster_id cluster,
  model::broker_id broker,
  device_store_id device,
  std::uint32_t shard) noexcept {
    if (cluster.is_nil() || broker.is_nil() || device.is_nil())
        return failure(errc::invalid_argument);
    return local_store_context{cluster, broker, device, shard};
}
result<void> local_store_context::validate_expected(
  const local_store_context& other) const noexcept {
    if (*this != other) return failure(errc::wrong_context);
    return {};
}
result<local_wal_cursor> local_wal_cursor::make(
  model::wal_incarnation_id id, runtime::file_position position) noexcept {
    if (id.is_nil() || position.value() == 0 || position.value() % 512U != 0)
        return failure(errc::invalid_argument);
    return local_wal_cursor{id, position};
}
result<std::strong_ordering> local_wal_cursor::compare(
  const local_store_context& owner,
  const local_wal_cursor& other,
  const local_store_context& other_owner) const noexcept {
    if (owner.store_wide() || other_owner.store_wide())
        return failure(errc::invalid_argument);
    if (owner != other_owner) return failure(errc::wrong_context);
    if (incarnation_.canonical_less(other.incarnation_))
        return std::strong_ordering::less;
    if (other.incarnation_.canonical_less(incarnation_))
        return std::strong_ordering::greater;
    return position_ <=> other.position_;
}
result<local_root_kind> parse_local_root_kind(std::uint16_t value) noexcept {
    if (value == 0) return failure(errc::invalid_argument);
    if (value > 5) return failure(errc::unsupported_format);
    return static_cast<local_root_kind>(value);
}
result<local_root_reference> local_root_reference::make(
  local_root_kind kind,
  local_object_sequence sequence,
  runtime::file_position position,
  byte_count bytes,
  page_count pages,
  codec::immutable_object_digest digest) noexcept {
    auto valid = parse_local_root_kind(static_cast<std::uint16_t>(kind));
    if (!valid) return failure(valid.error());
    if (!sequence.is_valid() || bytes.value() < 32)
        return failure(errc::invalid_argument);
    if (bytes.value() > 65536 || !position.checked_add(bytes))
        return failure(errc::out_of_range);
    if ((kind == local_root_kind::sealed_retry) != (position.value() != 0))
        return failure(errc::invalid_argument);
    return local_root_reference{kind, sequence, position, bytes, pages, digest};
}
result<void> local_root_reference::validate_alignment(
  storage_alignment alignment) const noexcept {
    if (
      !alignment.aligned(position_)
      || bytes_.value() % alignment.bytes().value() != 0)
        return failure(errc::invalid_argument);
    return {};
}
result<local_footer_reference> local_footer_reference::make(
  runtime::file_position position,
  byte_count bytes,
  std::uint16_t family,
  codec::immutable_object_digest digest) noexcept {
    if (family == 0) return failure(errc::invalid_argument);
    if (family != 6 && family != 7) return failure(errc::unsupported_format);
    if (bytes.value() < 32 || position.value() == 0)
        return failure(errc::invalid_argument);
    if (bytes.value() > 65536 || !position.checked_add(bytes))
        return failure(errc::out_of_range);
    return local_footer_reference{position, bytes, family, digest};
}
result<void> local_footer_reference::validate_alignment(
  storage_alignment alignment) const noexcept {
    if (
      !alignment.aligned(position_)
      || bytes_.value() % alignment.bytes().value() != 0)
        return failure(errc::invalid_argument);
    return {};
}
} // namespace kwaque::storage
