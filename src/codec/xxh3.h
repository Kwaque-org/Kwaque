#pragma once

#include "src/codec/digest.h"

#include <array>
#include <cstddef>

struct XXH3_state_s;

namespace kwaque::codec {

// Streaming XXH3-128 with seed 0 and the default secret, whose output xxHash
// has kept stable since 0.8.0. It is a content identity: it detects any
// accidental change but authenticates nothing. The digest is xxHash's
// canonical form, the high 64 bits and then the low 64 bits, each big-endian.
class xxh3_128_hasher final {
public:
    // The native state lives inside the hasher; construction never allocates.
    xxh3_128_hasher() noexcept;
    xxh3_128_hasher(const xxh3_128_hasher&) = delete;
    xxh3_128_hasher& operator=(const xxh3_128_hasher&) = delete;
    xxh3_128_hasher(xxh3_128_hasher&&) = delete;
    xxh3_128_hasher& operator=(xxh3_128_hasher&&) = delete;
    ~xxh3_128_hasher() = default;

    xxh3_128_hasher& update(const void* data, std::size_t size) noexcept;
    [[nodiscard]] content_digest final() && noexcept;

private:
    [[nodiscard]] XXH3_state_s* state() noexcept;

    // The state needs 64-byte alignment, which neither coroutine frames nor
    // allocators guarantee to members, so it is aligned within this storage.
    static constexpr std::size_t state_size{576};
    static constexpr std::size_t state_alignment{64};
    std::array<std::byte, state_size + state_alignment - 1> storage_;
};

// The digest of empty input, the identity of an empty extent.
inline constexpr content_digest xxh3_128_empty{
  0x99,
  0xaa,
  0x06,
  0xd3,
  0x01,
  0x47,
  0x98,
  0xd8,
  0x60,
  0x01,
  0xc3,
  0x24,
  0x46,
  0x8d,
  0x49,
  0x7f};

// One contiguous input; needs no native state.
[[nodiscard]] content_digest
xxh3_128(const void* data, std::size_t size) noexcept;

} // namespace kwaque::codec
