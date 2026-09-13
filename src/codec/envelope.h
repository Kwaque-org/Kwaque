#pragma once

#include "src/base/units.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/error.h"
#include "src/codec/format_registry.h"
#include "src/codec/integer.h"
#include "src/codec/limits.h"

#include <array>
#include <cstddef>
#include <cstdint>

namespace kwaque::codec {

inline constexpr std::size_t envelope_prefix_bytes = 32;
using encoded_envelope_prefix = std::array<char, envelope_prefix_bytes>;

// Conservative fixed-leaf costs include local initialization/copying, scalar
// access, validation and at most one fragment visit per prefix byte. An owning
// cooperative operation admits these costs against its shared residual first.
inline constexpr byte_count envelope_prefix_work_bytes{
  4 * envelope_prefix_bytes};
inline constexpr item_count envelope_prefix_work_items{
  2 * envelope_prefix_bytes};

enum class envelope_field : std::uint16_t {
    magic = 1,
    family = 2,
    writer_version = 3,
    minimum_reader_version = 4,
    header_bytes = 5,
    body_bytes = 6,
    required_features = 7,
    body_crc32c = 8,
    header_crc32c = 9,
    encoded_bytes = 10,
};

// Raw fixed-prefix values only. Successful extraction proves neither header
// integrity nor family/version/feature support, body availability or semantics.
// In particular family remains a raw integer until integrity permits lookup.
struct unverified_envelope_prefix final {
    std::uint16_t family;
    std::uint16_t writer_version;
    std::uint16_t minimum_reader_version;
    std::uint16_t header_bytes;
    std::uint32_t body_bytes;
    std::uint64_t required_features;
    std::uint32_t body_crc32c;
    std::uint32_t header_crc32c;

    [[nodiscard]] constexpr byte_count encoded_bytes() const noexcept {
        return byte_count{
          static_cast<std::uint64_t>(header_bytes) + body_bytes};
    }

    bool operator==(const unverified_envelope_prefix&) const noexcept = default;
};

// Independently supplied owner ceilings. Zero is an actual zero allowance.
// Larger values cannot widen the codec policy; these are logical extents, not
// reservations for backing, descriptors, native engines or coroutine frames.
struct envelope_extent_limits final {
    byte_count max_body_bytes;
    byte_count max_encoded_bytes;
};

// Only the current, extension-free writer profile is exposed. These checksums
// are already computed by the owning writer; this fixed leaf only encodes them.
struct envelope_prefix_fields final {
    format_family family;
    byte_count body_bytes;
    std::uint32_t body_crc32c;
    std::uint32_t header_crc32c;
};

// Inspect exactly the fixed prefix without consuming input, acquiring a mark,
// sharing backing or making a variable-sized request. Existing caller marks
// remain untouched even when the stack is full. Check magic and safe declared
// extents only; complete-header and body validation belongs to the owning
// codec. Coordinates describe the entire supplied parser, as for integer
// primitives.
[[nodiscard]] result<unverified_envelope_prefix> peek_envelope_prefix(
  const bytes::fragmented_buffer_parser& input,
  const limits& policy,
  envelope_extent_limits owner_limits,
  field_context context = {},
  input_boundary boundary = input_boundary::open);

// Returns an inline, owning 32-byte prefix. No builder, payload or partial
// envelope is published. The context origin names this prefix's first output
// byte and its family is a trusted diagnostic ID; this codec selects the field.
// No caller-controlled versions, feature bits, header extensions or checksum
// algorithm are accepted. Success does not verify the supplied CRC values.
[[nodiscard]] result<encoded_envelope_prefix> encode_envelope_prefix(
  envelope_prefix_fields fields,
  const limits& policy,
  envelope_extent_limits owner_limits,
  field_context context = {});

} // namespace kwaque::codec
