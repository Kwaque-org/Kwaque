#pragma once

#include "src/codec/envelope.h"
#include "src/codec/limits.h"
#include "src/storage/format_context.h"

#include <span>

namespace kwaque::storage {

// Header includes optional extensions. Tail is either complete nested-envelope
// bytes or entry bytes; neither includes this envelope's minimal zero padding.
struct envelope_parts final {
    byte_count header_bytes;
    byte_count fixed_body_bytes;
    byte_count tail_bytes;
};

[[nodiscard]] byte_count minimum_padding(
  byte_count unpadded_bytes, storage_alignment alignment) noexcept;

// Checked logical geometry only. No buffer allocation, byte encoding, integrity
// or complete-object validation is performed. Zero owner limits are real zero
// allowances; larger owner limits cannot widen the supplied codec policy.
class aligned_envelope_layout final {
public:
    [[nodiscard]] static result<aligned_envelope_layout> make(
      envelope_parts parts,
      storage_alignment alignment,
      const codec::limits& policy,
      codec::envelope_extent_limits owner_limits) noexcept;
    [[nodiscard]] byte_count header_bytes() const noexcept {
        return header_bytes_;
    }
    // Includes counted padding; both header and body count in encoded_bytes().
    [[nodiscard]] byte_count body_bytes() const noexcept { return body_bytes_; }
    [[nodiscard]] byte_count padding_bytes() const noexcept {
        return padding_bytes_;
    }
    [[nodiscard]] byte_count encoded_bytes() const noexcept {
        // Construction checked this sum before publication.
        return byte_count{header_bytes_.value() + body_bytes_.value()};
    }
    [[nodiscard]] storage_alignment alignment() const noexcept {
        return alignment_;
    }
    [[nodiscard]] result<model::file_byte_span>
    at(runtime::file_position position) const noexcept;
    bool operator==(const aligned_envelope_layout&) const noexcept = default;

private:
    aligned_envelope_layout(
      byte_count header,
      byte_count body,
      byte_count padding,
      storage_alignment alignment) noexcept
      : header_bytes_(header)
      , body_bytes_(body)
      , padding_bytes_(padding)
      , alignment_(alignment) {}
    byte_count header_bytes_;
    byte_count body_bytes_;
    byte_count padding_bytes_;
    storage_alignment alignment_;
};

// Largest positive complete child that can fit after fixed_body_bytes. This is
// an allowance, not validation of the child's own minimum size/grammar/caps.
// The final chosen layout must still be checked with make().
[[nodiscard]] result<byte_count> max_child_bytes(
  byte_count header_bytes,
  byte_count fixed_body_bytes,
  storage_alignment alignment,
  const codec::limits& policy,
  codec::envelope_extent_limits owner_limits) noexcept;

// Synchronous borrowed leaf: the owner first admits bytes.size() byte work and
// one item against its shared work account, and keeps the span alive through
// this call. Oversized pieces reject before inspection. The owner checks total
// declared padding against the layout and visits all its pieces; this function
// proves only that the supplied bounded piece is zero. It never moves a cursor.
[[nodiscard]] result<void> validate_zero_padding(
  std::span<const char> bytes, const codec::limits& policy) noexcept;

} // namespace kwaque::storage
