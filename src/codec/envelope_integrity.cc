#include "src/codec/envelope_integrity.h"

#include "src/codec/envelope.h"
#include "src/codec/header_integrity.h"

namespace kwaque::codec {

seastar::future<result<void>> verify_envelope_header_crc(
  const bytes::fragmented_buffer& header,
  cooperative_work& work,
  field_context context) {
    return detail::verify_header_crc(
      header,
      work,
      context,
      {envelope_prefix_bytes,
       10,
       28,
       static_cast<std::uint16_t>(envelope_field::header_bytes),
       static_cast<std::uint16_t>(envelope_field::header_crc32c)});
}

} // namespace kwaque::codec
