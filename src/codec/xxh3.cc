#include "src/codec/xxh3.h"

#include "src/base/invariant.h"

#define XXH_STATIC_LINKING_ONLY
#include <xxhash/xxhash.h>
#if defined(__x86_64__)
// Routes one-shot hashing and updates to the widest supported kernel, at most
// AVX2, selected once while the program loads.
#include <xxhash/xxh_x86dispatch.h>
#endif

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <memory>
#include <new>
#include <span>
#include <type_traits>
#include <utility>

namespace kwaque::codec {
namespace {

static_assert(sizeof(XXH3_state_t) <= detail::xxh3_state_storage::state_size);
static_assert(
  alignof(XXH3_state_t) == detail::xxh3_state_storage::state_alignment);
static_assert(std::is_trivially_destructible_v<XXH3_state_t>);
static_assert(xxh3_64_short_bytes == XXH3_MIDSIZE_MAX);

// The storage has room for the state at every possible alignment offset.
void* align(std::span<std::byte> storage) noexcept {
    void* aligned = storage.data();
    auto space = storage.size();
    return std::align(
      alignof(XXH3_state_t), sizeof(XXH3_state_t), aligned, space);
}

XXH3_state_t* create(detail::xxh3_state_storage& storage) noexcept {
    return ::new (align(storage.bytes())) XXH3_state_t;
}

XXH3_state_t* state_of(detail::xxh3_state_storage& storage) noexcept {
    return std::launder(static_cast<XXH3_state_t*>(align(storage.bytes())));
}

void require_updated(XXH_errorcode updated) noexcept {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CODEC-XXH3-UPDATE"},
      updated == XXH_OK,
      "XXH3 rejected a hash update");
}

content_digest canonical(XXH128_hash_t hash) noexcept {
    XXH128_canonical_t value;
    XXH128_canonicalFromHash(&value, hash);
    content_digest digest{};
    std::copy(std::begin(value.digest), std::end(value.digest), digest.begin());
    return digest;
}

} // namespace

xxh3_128_hasher::xxh3_128_hasher() noexcept {
    // The default secret needs no XXH3_INITSTATE before this reset.
    XXH3_128bits_reset(create(storage_));
}

xxh3_128_hasher&
xxh3_128_hasher::update(const void* data, std::size_t size) noexcept {
    require_updated(XXH3_128bits_update(state_of(storage_), data, size));
    return *this;
}

content_digest xxh3_128_hasher::final() && noexcept {
    return canonical(XXH3_128bits_digest(state_of(storage_)));
}

content_digest xxh3_128(const void* data, std::size_t size) noexcept {
    return canonical(XXH3_128bits(data, size));
}

std::uint64_t xxh3_64(const void* data, std::size_t size) noexcept {
    return XXH3_64bits(data, size);
}

xxh3_64_hasher::xxh3_64_hasher() noexcept {
    // The default secret needs no XXH3_INITSTATE before this reset.
    XXH3_64bits_reset(create(storage_));
}

xxh3_64_hasher&
xxh3_64_hasher::update(const void* data, std::size_t size) noexcept {
    require_updated(XXH3_64bits_update(state_of(storage_), data, size));
    return *this;
}

std::uint64_t xxh3_64_hasher::final() && noexcept {
    return XXH3_64bits_digest(state_of(storage_));
}

std::uint64_t
xxh3_64_pieces(std::span<const std::span<const std::byte>> pieces) noexcept {
    if (pieces.size() == 1) {
        return xxh3_64(pieces.front().data(), pieces.front().size());
    }
    std::size_t total = 0;
    for (const auto piece : pieces) {
        KWAQUE_INVARIANT(
          invariant_id{"KQ-CODEC-XXH3-PIECES"},
          piece.size() <= SIZE_MAX - total,
          "XXH3 pieces exceed the address space");
        total += piece.size();
    }
    if (total <= xxh3_64_short_bytes) {
        std::array<std::byte, xxh3_64_short_bytes> joined;
        std::size_t at = 0;
        for (const auto piece : pieces) {
            if (!piece.empty()) {
                std::memcpy(joined.data() + at, piece.data(), piece.size());
                at += piece.size();
            }
        }
        return xxh3_64(joined.data(), total);
    }
    xxh3_64_hasher hasher;
    for (const auto piece : pieces) {
        hasher.update(piece.data(), piece.size());
    }
    return std::move(hasher).final();
}

std::uint64_t
xxh3_64_tagged(std::uint8_t tag, const void* data, std::size_t size) noexcept {
    const std::byte prefix{tag};
    const std::array pieces{
      std::span<const std::byte>{&prefix, 1},
      std::span<const std::byte>{static_cast<const std::byte*>(data), size}};
    return xxh3_64_pieces(pieces);
}

} // namespace kwaque::codec
