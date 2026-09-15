#pragma once

#include "src/codec/cooperative.h"
#include "src/codec/transaction.h"

#include <cstdint>

namespace kwaque::compression {

// LZ4 writes one standard frame: independent 64-KiB blocks, compression level
// zero, autoFlush off, block/content checksums on, original content size, no
// dictionary, and zero reserved fields. Source/destination stability and
// checksum-skipping options remain off. Readers accept raw or compressed blocks
// with that wire profile; level and flush policy are not wire-visible. The
// dictionary-ID field must be absent, even for ID zero. Content size must equal
// the expected expansion; omission is permitted only for a complete empty
// frame. Empty encoded input, extra frames, and trailing bytes are not empty
// frames.
enum class codec_id : std::uint8_t { none = 0, lz4 = 1 };
inline constexpr codec_id default_codec = codec_id::none;

[[nodiscard]] codec::result<codec_id>
parse_codec_id(std::uint8_t value, codec::field_context context = {}) noexcept;

struct owned_result final {
    bytes::fragmented_buffer value;
    // Complete retained capacity, including unused descriptor slots and
    // possible sharing controls. Existing input backing is never implicitly
    // refunded.
    bytes::buffer_allocation_cost retained;
    // Residual after ADDITIONAL output allocations. For none this is unchanged:
    // the already-reserved input storage now belongs to value. For newly
    // decoded backing its reservation must survive destruction of the encoded
    // input.
    codec::decode_budget remaining;
};

// Transfers input before the first await, including on rejection/cancellation.
// Failure to allocate the outer coroutine frame precedes transfer. Input must
// already be admitted, including backing/descriptors/promotion; memory excludes
// that reservation and other live/frame/opaque costs. charge is the same
// stable, verified allocator profile used for the input. Do not refund its
// reservation while the result or any alias lives. No payload, descriptor or
// native context is allocated here. Generic empty regions are valid; batch
// nonemptiness is a separate model contract. Encoded and expanded lengths must
// match exactly. work/abort stay alive, unmoved and exclusive until joined
// completion. Cleanup precedes the final abort poll and successful ownership
// publication.
[[nodiscard]] seastar::future<codec::result<owned_result>> transfer_none(
  bytes::fragmented_buffer&& input,
  byte_count expanded_bytes,
  codec::cooperative_work& work,
  codec::decode_budget memory,
  codec::field_context context = {});

// Same consuming input, shared work and pre-reserved input/opaque/frame
// contract as transfer_none. Each operation owns a fresh LZ4 context until
// joined cleanup. Only completed immutable output is returned; remaining
// reserves its additional backing/descriptors/promotion after temporary scratch
// has been released. The caller retains the OLD input reservation until all of
// its aliases die. encoded_limit bounds the actual frame, including its header
// and footer, and is intersected with the configured encoded-body cap. There is
// no fallback. LZ4 requires a 64-KiB byte-work quantum and at least eight work
// items.
[[nodiscard]] seastar::future<codec::result<owned_result>> compress_lz4(
  bytes::fragmented_buffer&& input,
  byte_count encoded_limit,
  codec::cooperative_work& work,
  codec::decode_budget memory,
  codec::field_context context = {});

// Input is one complete encoded region. Incomplete frames and expansion
// disagreement are malformed_data; native checksum failures are corrupt_data;
// valid unsupported profiles are unsupported_format. Success requires exactly
// one completed frame, all encoded bytes consumed and exactly expanded_bytes.
// Diagnostic offsets refer to encoded input (native errors use the call start),
// never to expanded byte positions. No intermediate output escapes on failure.
[[nodiscard]] seastar::future<codec::result<owned_result>> decompress_lz4(
  bytes::fragmented_buffer&& input,
  byte_count expanded_bytes,
  codec::cooperative_work& work,
  codec::decode_budget memory,
  codec::field_context context = {});

} // namespace kwaque::compression
