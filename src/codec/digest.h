#pragma once

#include <array>
#include <cstddef>

namespace kwaque::codec {

inline constexpr std::size_t sha256_digest_bytes{32};
using sha256_digest = std::array<unsigned char, sha256_digest_bytes>;

namespace detail {

// Domain tags distinguish values; they never change the bytes being hashed.
template<typename Domain>
class digest_value final {
public:
    explicit constexpr digest_value(sha256_digest bytes) noexcept
      : bytes_(bytes) {}

    [[nodiscard]] constexpr sha256_digest bytes() const noexcept {
        return bytes_;
    }

    bool operator==(const digest_value&) const noexcept = default;

private:
    sha256_digest bytes_;
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
