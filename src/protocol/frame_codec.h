#pragma once

#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/cooperative.h"
#include "src/codec/integer.h"
#include "src/codec/transaction.h"
#include "src/protocol/frame.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <variant>

namespace kwaque::protocol {

inline constexpr std::size_t frame_prefix_bytes = 48;
using encoded_frame_prefix = std::array<char, frame_prefix_bytes>;
// Includes local initialization/copy/scalar work and one fragment per octet.
inline constexpr byte_count frame_prefix_work_bytes{4 * frame_prefix_bytes};
inline constexpr item_count frame_prefix_work_items{2 * frame_prefix_bytes};

enum class frame_field : std::uint16_t {
    magic = 1,
    protocol_version = 2,
    kind = 3,
    header_bytes = 4,
    flags = 5,
    payload_bytes = 6,
    stream = 7,
    correlation = 8,
    sequence = 9,
    header_crc32c = 10,
    payload_crc32c = 11,
    encoded_bytes = 12,
};

// Extracted scalar bytes only: no integrity, kind/profile, stream or payload
// assertion follows from this value. In particular kind remains a raw integer.
struct unverified_frame_prefix final {
    std::uint16_t protocol_version;
    std::uint16_t kind;
    std::uint16_t header_bytes;
    std::uint16_t flags;
    std::uint32_t payload_bytes;
    std::uint64_t stream;
    std::uint64_t correlation;
    std::uint64_t sequence;
    std::uint32_t header_crc32c;
    std::uint32_t payload_crc32c;

    [[nodiscard]] constexpr byte_count encoded_bytes() const noexcept {
        return byte_count{
          static_cast<std::uint64_t>(header_bytes) + payload_bytes};
    }
    bool operator==(const unverified_frame_prefix&) const noexcept = default;
};

// Independent logical ceilings, intersected with policy. Zero is a real zero
// allowance. A raw payload includes its complete inner batch envelope.
struct frame_extent_limits final {
    byte_count max_payload_bytes;
    byte_count max_encoded_bytes;
};

struct frame_prefix_fields final {
    frame_metadata metadata;
    byte_count payload_bytes;
    std::uint32_t header_crc32c;
    std::uint32_t payload_crc32c;
};

// Descriptive header result, not a reusable validation capability. Payload
// availability, checksum, grammar and expected context are not established.
struct frame_header final {
    frame_metadata metadata;
    byte_count header_bytes;
    byte_count payload_bytes;
    std::uint32_t header_crc32c;
    std::uint32_t payload_crc32c;

    bool operator==(const frame_header&) const noexcept = default;
};

// An owning framing handoff, not an accepted batch/control message. Its owner
// must retain input backing reservations until all resulting aliases die.
struct framed_payload final {
    frame_header header;
    bytes::fragmented_buffer payload;
    codec::decode_budget remaining;
};

// Additional bytes needed to reach the next check boundary, always positive
// in returned values. This owns no input and carries no validation capability.
// Empty open input needs a prefix; it does not establish transport EOF.
struct need_more final {
    byte_count additional_bytes;
    bool operator==(const need_more&) const noexcept = default;
};

template<typename T>
using frame_read_result = codec::result<std::variant<need_more, T>>;

// Decode exactly one frame under one caller mark. On success, consume H+P
// bytes and return an owning payload with header/payload integrity checked,
// but no nested batch/control grammar or authorization asserted. Following
// bytes remain unread. Errors, need_more, exceptions and observed cancellation
// restore the entry cursor and marks; private owners are drained first.
//
// Open input reports 48-N, H-N or H+P-N at the respective next check boundary.
// Complete-parent shortage is malformed. Header errors precede missing/corrupt
// payload, and no incomplete attempt shares or checksums the partial payload.
// Retry on a longer immutable parser snapshot only after the prior call ends;
// this function owns no receive queue and retains no state between calls.
//
// Reserve this finite parser's input ONCE with reserve_decode_input. memory is
// the residual after native CRC/frame/callback/other live costs. Child aliases
// debit their descriptors only. remaining excludes just the returned payload's
// new descriptors; parent backing/promoted controls remain reserved while any
// resulting alias lives, including after parent destruction. No automatic
// resource reservation/refund service is created. Parent/work/abort remain
// alive, unmoved and exclusive through completion; final poll/commit/transfer
// do not suspend. Generic framing success never replaces a typed decoder.
[[nodiscard]] seastar::future<frame_read_result<framed_payload>> decode_frame(
  bytes::fragmented_buffer_parser& input,
  frame_extent_limits owner_limits,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context = {},
  codec::input_boundary boundary = codec::input_boundary::open);

// Fixed inline extraction, without marks, shares or allocation. Rejects bad
// magic/impossible extents/cap excess before waiting for variable-sized input.
// All caller marks remain untouched. Open shortage is truncated_data; complete
// shortage is malformed_data. context describes the whole supplied parser.
[[nodiscard]] codec::result<unverified_frame_prefix> peek_frame_prefix(
  const bytes::fragmented_buffer_parser& input,
  const codec::limits& policy,
  frame_extent_limits owner_limits,
  codec::field_context context = {},
  codec::input_boundary boundary = codec::input_boundary::open);

// Inline, extension-free v1 bytes; supplied CRCs are not verified by this leaf.
// The owning writer computes them from its actual payload. Invalid kinds or
// stream combinations supplied to a writer are caller errors.
[[nodiscard]] codec::result<encoded_frame_prefix> encode_frame_prefix(
  frame_prefix_fields fields,
  const codec::limits& policy,
  frame_extent_limits owner_limits,
  codec::field_context context = {});

// Raw integrity only, over an exact admitted header alias. Checks all bytes
// with 40..43 zeroed, including payload CRC and optional extensions. Header,
// work and abort source stay alive/unmoved/exclusive until completion. The
// enclosing owner handles semantic checks, cleanup and final polling.
[[nodiscard]] seastar::future<codec::result<void>> verify_frame_header_crc(
  const bytes::fragmented_buffer& header,
  codec::cooperative_work& work,
  codec::field_context context = {});
void verify_frame_header_crc(
  const bytes::fragmented_buffer&&,
  codec::cooperative_work&,
  codec::field_context = {}) = delete;

// Verify one complete bounded header, including kind/profile/stream and TLVs,
// without inspecting payload bytes. Restores position/marks on success too.
// Requires one free caller mark. Before entry, reserve_decode_input has
// admitted this finite parent's backing/descriptors/promotion ONCE. memory
// excludes verified native/frame/other live costs; only temporary header
// aliases are charged here. Persistent promotion remains reserved even after
// failure. Parent/work/abort stay alive, unmoved and exclusive until
// completion. Native allocation exceptions propagate after joined cleanup. No
// header value can authorize skipping checks on a later operation or different
// bytes.
[[nodiscard]] seastar::future<codec::result<frame_header>> inspect_frame_header(
  bytes::fragmented_buffer_parser& input,
  frame_extent_limits owner_limits,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context = {},
  codec::input_boundary boundary = codec::input_boundary::open);

// Consumes payload before the first await, including entered rejection,
// exception and abort. Native coroutine-frame allocation failure precedes
// transfer. Derives length, payload CRC then header CRC; publishes immutable
// output only after joined cleanup and final polling. Does not validate nested
// payload grammar. There is no outer compression or extension emission.
// other_live/parent_remaining exclude this payload and new framing/assembly
// staging; native CRC/frame/opaque costs are included/excluded once by the
// caller. charge is a verified monotone nonallocating served-capacity bound.
// Assembly retains the same payload backing reservation, adding header and
// descriptor costs. Work/abort remain exclusively alive and unmoved throughout.
[[nodiscard]] seastar::future<codec::result<bytes::fragmented_buffer>>
encode_frame(
  bytes::fragmented_buffer&& payload,
  frame_metadata metadata,
  codec::cooperative_work& work,
  frame_extent_limits owner_limits,
  codec::operation_usage other_live,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context context = {});

} // namespace kwaque::protocol
