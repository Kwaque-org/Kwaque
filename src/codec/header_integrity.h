#pragma once

#include "src/bytes/fragmented_buffer.h"
#include "src/codec/cooperative.h"
#include "src/codec/integer.h"

#include <cstddef>
#include <cstdint>

namespace kwaque::codec::detail {

// Trusted constants supplied only by the two format wrappers. The CRC32C
// algorithm and four-byte mask are fixed; this is not a wire-selected policy.
struct header_crc_layout final {
    std::size_t prefix_bytes;
    std::size_t header_length_offset;
    std::size_t checksum_offset;
    std::uint16_t header_length_field;
    std::uint16_t checksum_field;
};

// Borrow an exact header alias already admitted by its owner. Validate bounds
// and coordinates, then hash every byte with the checksum slot replaced by
// zeros. No share, parser mark or semantic validation is performed here.
// Header/work/abort stay alive, unmoved and exclusive through completion.
[[nodiscard]] seastar::future<result<void>> verify_header_crc(
  const bytes::fragmented_buffer& header,
  cooperative_work& work,
  field_context context,
  header_crc_layout layout);

} // namespace kwaque::codec::detail
