#pragma once

#include "src/codec/digest.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

namespace kwaque::codec {

namespace detail {

// Room for one native XXH3 streaming state. The state needs 64-byte
// alignment, which neither coroutine frames nor allocators guarantee to
// members, so it is placed at an aligned offset inside this storage. No
// xxHash type is named here: a unit may also include xxh3_64_inline.h, which
// gives those names another meaning.
class xxh3_state_storage final {
public:
    static constexpr std::size_t state_size{576};
    static constexpr std::size_t state_alignment{64};

    [[nodiscard]] std::span<std::byte> bytes() noexcept { return storage_; }

private:
    std::array<std::byte, state_size + state_alignment - 1> storage_;
};

} // namespace detail

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
    detail::xxh3_state_storage storage_;
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

// XXH3-64 with seed 0 and the default secret. A record key's routing point
// is this value of the key's bytes, the same for every topic and cluster, so
// its output must never change; the checked-in vectors pin it. Nothing here
// allocates or suspends.
[[nodiscard]] std::uint64_t
xxh3_64(const void* data, std::size_t size) noexcept;

// The value of empty input, which is an ordinary key.
inline constexpr std::uint64_t xxh3_64_empty{0x2d06800538d394c2};

// XXH3 hashes inputs up to this length in a scalar path whose work does not
// depend on the processor's vector width; longer inputs use a vector kernel:
// on x86-64 the widest the processor has up to AVX2, selected at load, and
// NEON on aarch64.
inline constexpr std::size_t xxh3_64_short_bytes{240};

// Streaming XXH3-64; the same value as xxh3_64 over the concatenated input.
class xxh3_64_hasher final {
public:
    xxh3_64_hasher() noexcept;
    xxh3_64_hasher(const xxh3_64_hasher&) = delete;
    xxh3_64_hasher& operator=(const xxh3_64_hasher&) = delete;
    xxh3_64_hasher(xxh3_64_hasher&&) = delete;
    xxh3_64_hasher& operator=(xxh3_64_hasher&&) = delete;
    ~xxh3_64_hasher() = default;

    xxh3_64_hasher& update(const void* data, std::size_t size) noexcept;
    [[nodiscard]] std::uint64_t final() && noexcept;

private:
    detail::xxh3_state_storage storage_;
};

// XXH3-64 of one input held in several pieces, in order. Up to
// xxh3_64_short_bytes in all are copied to the stack and hashed in one call,
// which is cheaper than streaming: XXH3 buffers such input before hashing it
// whole anyway. Longer input streams.
[[nodiscard]] std::uint64_t
xxh3_64_pieces(std::span<const std::span<const std::byte>> pieces) noexcept;

// XXH3-64 of a one-byte tag followed by the input. Internal identities hash
// through this, each use with its own tag, so two uses of one value hash
// different inputs. A record key's routing point is never tagged.
[[nodiscard]] std::uint64_t
xxh3_64_tagged(std::uint8_t tag, const void* data, std::size_t size) noexcept;

} // namespace kwaque::codec
