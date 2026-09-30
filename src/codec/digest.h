#pragma once

#include <array>
#include <cstddef>

namespace kwaque::codec {

// A 128-bit non-cryptographic content identity (XXH3-128, canonical form).
inline constexpr std::size_t content_digest_bytes{16};
using content_digest = std::array<unsigned char, content_digest_bytes>;
// Encoded digests occupy a slot of this size: the content digest followed by
// zero bytes that decoders reject when set. A wider identity can later use
// the same slot without moving any field.
inline constexpr std::size_t digest_slot_bytes{32};

namespace detail {

// Domain tags distinguish values; they never change the bytes being hashed.
template<typename Domain>
class digest_value final {
public:
    explicit constexpr digest_value(content_digest bytes) noexcept
      : bytes_(bytes) {}

    [[nodiscard]] constexpr content_digest bytes() const noexcept {
        return bytes_;
    }

    bool operator==(const digest_value&) const noexcept = default;

private:
    content_digest bytes_;
};

struct semantic_batch_digest_tag;
struct checkpoint_digest_tag;
struct immutable_object_digest_tag;
struct extent_digest_tag;

} // namespace detail

// Explicit bytes, including all-zero bytes, are valid values. Construction does
// not verify content or authority; absence is represented separately by owners.
using semantic_batch_digest
  = detail::digest_value<detail::semantic_batch_digest_tag>;
using checkpoint_digest = detail::digest_value<detail::checkpoint_digest_tag>;
using immutable_object_digest
  = detail::digest_value<detail::immutable_object_digest_tag>;
using extent_digest = detail::digest_value<detail::extent_digest_tag>;

// These arrays contain exactly one terminating zero byte, which is included in
// the semantic projection. The owning model codec supplies the specified
// fields.
inline constexpr auto semantic_batch_domain = std::to_array("KQ/BATCH/1");
inline constexpr auto checkpoint_domain = std::to_array("KQ/CHECKPOINT/1");

// Exact immutable-object and extent projections have no added domain prefix or
// field framing. Their value tags alone must never add data to the hash stream.

} // namespace kwaque::codec
