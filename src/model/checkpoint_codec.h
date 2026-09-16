#pragma once

#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/cooperative.h"
#include "src/codec/digest.h"
#include "src/codec/transaction.h"
#include "src/model/checkpoint.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/future.hh>

#include <cstdint>
#include <span>

namespace kwaque::model {

inline constexpr byte_count checkpoint_fixed_bytes{20};
inline constexpr byte_count checkpoint_cursor_bytes{24};

enum class checkpoint_field : std::uint16_t {
    topic = 128,
    cursor_count = 129,
    range = 130,
    next = 131,
};

struct constructed_read_checkpoint final {
    read_checkpoint value;
    codec::decode_budget remaining;
};

struct decoded_read_checkpoint final {
    read_checkpoint value;
    codec::checkpoint_digest fingerprint;
    codec::decode_budget remaining;
};

struct encoded_read_checkpoint final {
    bytes::fragmented_buffer bytes;
    codec::checkpoint_digest fingerprint;
};

// Copies strictly ordered scalar entries into admitted private storage. Never
// sorts borrowed input. memory excludes input storage, verified frame/native
// costs and other live owners; the result deducts only its retained vector.
// Topic must be nonnil. Count/byte/served-capacity limits apply before copying.
// Input, work and abort stay alive, unmoved and exclusive through completion.
// The fixed copy/comparison leaf needs at least 128 work bytes and 8 items.
[[nodiscard]] seastar::future<codec::result<constructed_read_checkpoint>>
make_read_checkpoint(
  topic_id topic,
  std::span<const range_cursor> sorted,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context = {});

// Takes ownership before the first suspension. Frame allocation failure,
// missing charge profile, reserve-created free chunks or an impossible cleanup
// quantum leave the donor untouched. Later rejection/exception/cancellation
// drains consumed ownership. The count cap precedes bounded run sorting.
// memory INCLUDES native source chunks, unlike the borrowed sorted path. The
// sorter accounts source/runs/heap/output, and conversion admits FIFO/vector
// overlap. Only the final vector remains deducted on successful return.
// Conversion needs 128 work bytes/8 items; the bounded sorter also requires
// its count-dependent heap quantum and scratch reservation.
[[nodiscard]] seastar::future<codec::result<constructed_read_checkpoint>>
make_read_checkpoint_from_unordered(
  topic_id topic,
  seastar::chunked_fifo<range_cursor, 16>&& source,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context = {});

// Borrows a stable checked value. parent_remaining excludes that owner and
// verified SHA/CRC/frame/native costs; new body/envelope staging is admitted
// here. The writer emits an extension-free envelope with a fixed u32 count.
// Fingerprint covers the domain and canonical body only. Cleanup and final
// abort polling precede publication; rejection preserves the input value.
// Fixed wire leaves need at least 256 work bytes and 64 items.
[[nodiscard]] seastar::future<codec::result<encoded_read_checkpoint>>
encode_read_checkpoint(
  const read_checkpoint& value,
  codec::cooperative_work& work,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context context = {});

seastar::future<codec::result<encoded_read_checkpoint>> encode_read_checkpoint(
  const read_checkpoint&&,
  codec::cooperative_work&,
  byte_count,
  bytes::allocation_charge_fn,
  codec::field_context = {}) = delete;

// One parent transaction: integrity, exact fixed-u32 body, independent topic
// expectation, strict key order and semantic fingerprint all precede commit.
// Caller first reserves the input once with reserve_decode_input. memory is
// its residual after verified frame/native SHA/CRC costs and other live owners.
// Returned residuals retain only the cursor vector; parent backing and share
// promotion remain reserved by the caller even after a failed decode.
// The parser, work and abort source stay alive and exclusive through the call.
// Fixed wire leaves need at least 256 work bytes and 64 items.
[[nodiscard]] seastar::future<codec::result<decoded_read_checkpoint>>
decode_read_checkpoint(
  bytes::fragmented_buffer_parser& input,
  topic_id expected_topic,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context = {},
  codec::input_boundary boundary = codec::input_boundary::open);

} // namespace kwaque::model
