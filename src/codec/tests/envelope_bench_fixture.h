#pragma once

#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/cooperative.h"
#include "src/codec/envelope.h"
#include "src/codec/error.h"
#include "src/codec/format_registry.h"
#include "src/codec/integer.h"
#include "src/codec/limits.h"
#include "src/codec/transaction.h"

#include <seastar/core/future.hh>

#include <cstdint>

namespace kwaque::codec::bench {

struct envelope_expected_body final {
    std::uint64_t object;
    std::uint64_t generation;
};

struct envelope_decoded_body final {
    std::uint64_t object;
    std::uint64_t generation;
    bytes::fragmented_buffer payload;
};

// Both complete readers invoke this same statically bound body grammar. Its
// returned payload owns its bytes and is separately admitted before sharing.
struct envelope_body_reader final {
    envelope_expected_body expected;

    [[nodiscard]] seastar::future<result<envelope_decoded_body>> operator()(
      bytes::fragmented_buffer_parser&,
      field_context,
      input_boundary,
      decode_budget,
      cooperative_work&) const;
};

// Both sides cross this translation-unit boundary. The checked baseline keeps
// the same ownership, validation, work, admission and cleanup responsibilities,
// using native scalar loads/stores and the configured comparison CRC engine.
// It is an alternative checked mechanism, not a whole-system performance floor.
[[nodiscard]] result<unverified_envelope_prefix> checked_prefix_read(
  const bytes::fragmented_buffer_parser&,
  const limits&,
  envelope_extent_limits,
  field_context,
  input_boundary);
[[nodiscard]] result<unverified_envelope_prefix> codec_prefix_read(
  const bytes::fragmented_buffer_parser&,
  const limits&,
  envelope_extent_limits,
  field_context,
  input_boundary);
[[nodiscard]] result<encoded_envelope_prefix> checked_prefix_write(
  envelope_prefix_fields, const limits&, envelope_extent_limits, field_context);
[[nodiscard]] result<encoded_envelope_prefix> codec_prefix_write(
  envelope_prefix_fields, const limits&, envelope_extent_limits, field_context);

[[nodiscard]] seastar::future<result<item_count>> checked_extensions(
  bytes::fragmented_buffer_parser&,
  byte_count,
  byte_count,
  cooperative_work&,
  field_context);
[[nodiscard]] seastar::future<result<item_count>> codec_extensions(
  bytes::fragmented_buffer_parser&,
  byte_count,
  byte_count,
  cooperative_work&,
  field_context);

[[nodiscard]] seastar::future<result<envelope_decoded_body>>
checked_owned_decode(
  bytes::fragmented_buffer_parser&,
  format_family,
  envelope_extent_limits,
  decode_budget,
  cooperative_work&,
  envelope_expected_body,
  field_context,
  input_boundary);
[[nodiscard]] seastar::future<result<envelope_decoded_body>> codec_owned_decode(
  bytes::fragmented_buffer_parser&,
  format_family,
  envelope_extent_limits,
  decode_budget,
  cooperative_work&,
  envelope_expected_body,
  field_context,
  input_boundary);

[[nodiscard]] seastar::future<result<bytes::fragmented_buffer>>
checked_owned_encode(
  bytes::fragmented_buffer&&,
  format_family,
  cooperative_work&,
  envelope_extent_limits,
  operation_usage,
  byte_count,
  bytes::allocation_charge_fn,
  field_context);
[[nodiscard]] seastar::future<result<bytes::fragmented_buffer>>
codec_owned_encode(
  bytes::fragmented_buffer&&,
  format_family,
  cooperative_work&,
  envelope_extent_limits,
  operation_usage,
  byte_count,
  bytes::allocation_charge_fn,
  field_context);

} // namespace kwaque::codec::bench
