#pragma once

#include "src/codec/xxh3.h"
#include "src/storage/local_types.h"

#include <array>
#include <cstdint>
#include <optional>

namespace kwaque::storage::detail {

// The canonical framing of recovery identities: integers as eight
// little-endian bytes, identifiers and digests as their bytes, and a presence
// word before every optional value.
inline void
hash_u64(codec::xxh3_128_hasher& hash, std::uint64_t value) noexcept {
    std::array<unsigned char, 8> bytes{};
    for (std::size_t i = 0; i < bytes.size(); ++i)
        bytes[i] = static_cast<unsigned char>(value >> (8U * i));
    hash.update(bytes.data(), bytes.size());
}
template<typename Id>
void hash_id(codec::xxh3_128_hasher& hash, const Id& id) noexcept {
    const auto bytes = id.bytes();
    hash.update(bytes.data(), bytes.size());
}
inline void hash_digest(
  codec::xxh3_128_hasher& hash, codec::content_digest digest) noexcept {
    hash.update(digest.data(), digest.size());
}
inline void hash_segment(
  codec::xxh3_128_hasher& hash, const segment_context& segment) noexcept {
    const auto cluster = segment.cluster();
    const auto topic = segment.topic();
    const auto range = segment.range();
    const auto id = segment.segment();
    hash_id(hash, cluster);
    hash_id(hash, topic);
    hash_id(hash, range);
    hash_id(hash, id);
    hash_u64(hash, segment.generation().value());
}
inline void hash_footer(
  codec::xxh3_128_hasher& hash,
  const std::optional<local_footer_reference>& footer) noexcept {
    hash_u64(hash, footer ? 1U : 0U);
    if (!footer) return;
    hash_u64(hash, footer->position().value());
    hash_u64(hash, footer->bytes().value());
    hash_u64(hash, footer->family());
    hash_digest(hash, footer->digest().bytes());
}
inline void hash_cursor(
  codec::xxh3_128_hasher& hash,
  const std::optional<local_wal_cursor>& cursor) noexcept {
    hash_u64(hash, cursor ? 1U : 0U);
    if (!cursor) return;
    const auto incarnation = cursor->incarnation();
    hash_id(hash, incarnation);
    hash_u64(hash, cursor->position().value());
}

} // namespace kwaque::storage::detail
