#pragma once

#include "src/bytes/fragmented_buffer.h"
#include "src/codec/cooperative.h"
#include "src/codec/error.h"
#include "src/codec/integer.h"

#include <seastar/core/future.hh>

namespace kwaque::codec {

// Verify only the raw header checksum. The enclosing reader first checks the
// fixed prefix, declared extents and complete header availability, then
// supplies an exact header-only alias. It admits that alias's retained
// backing/share bookkeeping and this operation's native CRC/frame costs before
// calling. No body alias or body decoder is needed to verify the header.
//
// The header remains alive, unmoved and unmodified until completion. This
// helper borrows its existing fragments, creates no shares/copies and never
// changes a parser or its marks. It hashes all raw bytes with positions 28..31
// replaced by four zero bytes, including the stored body CRC and unknown
// extensions. Success establishes this integrity check only: magic,
// declared-header-size agreement, versions, family, features, extensions and
// body/context validity remain the enclosing reader's checks, in their
// specified order.
//
// context.origin is the first header byte. The enclosing owner releases its
// temporary aliases and polls again before any final parser commit/publication.
// Native exceptions propagate; shared work and abort ownership remain with it.
[[nodiscard]] seastar::future<result<void>> verify_envelope_header_crc(
  const bytes::fragmented_buffer& header,
  cooperative_work& work,
  field_context context = {});

void verify_envelope_header_crc(
  const bytes::fragmented_buffer&&,
  cooperative_work&,
  field_context = {}) = delete;

} // namespace kwaque::codec
