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
#include <iterator>
#include <memory>
#include <new>
#include <span>
#include <type_traits>

namespace kwaque::codec {
namespace {

// The storage has room for the state at every possible alignment offset.
void* align(std::span<std::byte> storage) noexcept {
    void* aligned = storage.data();
    auto space = storage.size();
    return std::align(
      alignof(XXH3_state_t), sizeof(XXH3_state_t), aligned, space);
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
    static_assert(sizeof(XXH3_state_t) <= state_size);
    static_assert(alignof(XXH3_state_t) == state_alignment);
    static_assert(std::is_trivially_destructible_v<XXH3_state_t>);
    // The default secret needs no XXH3_INITSTATE before this reset.
    XXH3_128bits_reset(::new (align(storage_)) XXH3_state_t);
}

XXH3_state_t* xxh3_128_hasher::state() noexcept {
    return std::launder(static_cast<XXH3_state_t*>(align(storage_)));
}

xxh3_128_hasher&
xxh3_128_hasher::update(const void* data, std::size_t size) noexcept {
    const auto updated = XXH3_128bits_update(state(), data, size);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CODEC-XXH3-UPDATE"},
      updated == XXH_OK,
      "XXH3 rejected a hash update");
    return *this;
}

content_digest xxh3_128_hasher::final() && noexcept {
    return canonical(XXH3_128bits_digest(state()));
}

content_digest xxh3_128(const void* data, std::size_t size) noexcept {
    return canonical(XXH3_128bits(data, size));
}

} // namespace kwaque::codec
