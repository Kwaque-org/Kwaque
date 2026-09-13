#pragma once

#include "src/base/units.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/cooperative.h"
#include "src/codec/error.h"
#include "src/codec/integer.h"

#include <seastar/core/future.hh>

#include <cstddef>
#include <cstdint>

namespace kwaque::codec {

inline constexpr std::size_t header_extension_prefix_bytes = 8;
// Includes inline initialization/copy/read work and the worst-case visit count
// when each prefix octet occupies its own fragment.
inline constexpr byte_count header_extension_prefix_work_bytes{32};
inline constexpr item_count header_extension_prefix_work_items{16};

enum class header_extension_field : std::uint16_t {
    region = 32,
    count = 33,
    tag = 34,
    flags = 35,
    value_length = 36,
    value = 37,
};

// Scan only after the enclosing header's integrity has been checked, with the
// parser positioned at its extension region. fixed_header_bytes is the already
// consumed fixed prefix (at least 32 bytes); its sum with extension_bytes must
// fit the shared header cap. The complete region is required, and following
// body/adjacent-object bytes are never part of a value's declared boundary.
//
// The caller has already acquired a parser transaction. This helper acquires,
// commits and rolls back NO marks; failure may leave speculative advancement.
// The caller rolls back on any error/exception/abort and performs its own final
// abort poll and commit. An eighth caller-owned mark is therefore usable.
// Parser, work and abort source stay alive, unmoved and exclusively accessed
// through completion. Their backing, frame and native costs are already
// admitted by the enclosing owner; this scanner creates no shares or payload
// owners. Only unknown optional tags are currently accepted, and their values
// are skipped without copying. No extension tag is assigned for production.
[[nodiscard]] seastar::future<result<item_count>>
scan_header_extensions_in_transaction(
  bytes::fragmented_buffer_parser& input,
  byte_count extension_bytes,
  byte_count fixed_header_bytes,
  cooperative_work& work,
  field_context context = {});

} // namespace kwaque::codec
