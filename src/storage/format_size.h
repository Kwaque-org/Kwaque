#pragma once

#include "src/codec/envelope.h"
#include "src/codec/limits.h"
#include "src/storage/format_context.h"

#include <cstdint>
#include <optional>
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

// A write is the bytes one gathered write puts in a file. It begins on an
// alignment boundary and its envelopes are contiguous; only an envelope that
// ends a padded span pads, to the next boundary. So no write shares an
// alignment unit with another, and a barrier never certifies a unit that a
// later write changes.
enum class envelope_placement : std::uint8_t {
    // Another envelope of the same span follows: no padding.
    continues,
    // Ends a padded span: padding takes its end to the next boundary.
    ends_span,
};

// The padding that ends an envelope of unpadded_bytes at position on the next
// alignment boundary, or none when it already ends on one.
[[nodiscard]] result<byte_count> end_padding(
  runtime::file_position position,
  byte_count unpadded_bytes,
  storage_alignment alignment) noexcept;

// Checked logical geometry only. No buffer allocation, byte encoding, integrity
// or complete-object validation is performed. Zero owner limits are real zero
// allowances; larger owner limits cannot widen the supplied codec policy.
class aligned_envelope_layout final {
public:
    // A start-aligned object: it begins on a boundary and ends its own span,
    // so its padding is the minimum for its size, wherever it lies.
    [[nodiscard]] static result<aligned_envelope_layout> make(
      envelope_parts parts,
      storage_alignment alignment,
      const codec::limits& policy,
      codec::envelope_extent_limits owner_limits) noexcept;
    // An envelope at position inside a write. Its padding depends on both, and
    // at() accepts only that position.
    [[nodiscard]] static result<aligned_envelope_layout> make(
      envelope_parts parts,
      runtime::file_position position,
      envelope_placement placement,
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
    [[nodiscard]] static result<byte_count>
    unpadded_bytes(envelope_parts parts, const codec::limits& policy) noexcept;
    [[nodiscard]] static result<aligned_envelope_layout> checked(
      envelope_parts parts,
      byte_count unpadded,
      byte_count padding,
      storage_alignment alignment,
      const codec::limits& policy,
      codec::envelope_extent_limits owner_limits,
      std::optional<runtime::file_position> position) noexcept;
    aligned_envelope_layout(
      byte_count header,
      byte_count body,
      byte_count padding,
      storage_alignment alignment,
      std::optional<runtime::file_position> position) noexcept
      : header_bytes_(header)
      , body_bytes_(body)
      , padding_bytes_(padding)
      , alignment_(alignment)
      , position_(position) {}
    byte_count header_bytes_;
    byte_count body_bytes_;
    byte_count padding_bytes_;
    storage_alignment alignment_;
    // Set for an envelope placed inside a write.
    std::optional<runtime::file_position> position_;
};

// Largest positive complete child that can fit after fixed_body_bytes. This is
// an allowance, not validation of the child's own minimum size/grammar/caps.
// The final chosen layout must still be checked with make(). This one is for
// a start-aligned object.
[[nodiscard]] result<byte_count> max_child_bytes(
  byte_count header_bytes,
  byte_count fixed_body_bytes,
  storage_alignment alignment,
  const codec::limits& policy,
  codec::envelope_extent_limits owner_limits) noexcept;
// The same allowance for an envelope at any position inside a write. One that
// may end a span keeps room for padding of up to one unit less a byte.
[[nodiscard]] result<byte_count> max_child_bytes(
  byte_count header_bytes,
  byte_count fixed_body_bytes,
  envelope_placement placement,
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
