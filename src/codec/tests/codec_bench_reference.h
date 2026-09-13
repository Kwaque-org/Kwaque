#pragma once

#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/cooperative.h"
#include "src/codec/error.h"
#include "src/codec/integer.h"

#include <seastar/core/future.hh>

#include <cstdint>

namespace kwaque::codec::bench {

// Both sides cross a translation-unit boundary so inlining and coroutine
// lifetime visibility are equivalent at the benchmark call site.
[[nodiscard]] result<std::uint64_t> native_fixed_read(
  bytes::fragmented_buffer_parser&, field_context, input_boundary);
[[nodiscard]] result<std::uint64_t> codec_fixed_read(
  bytes::fragmented_buffer_parser&, field_context, input_boundary);
[[nodiscard]] result<std::uint64_t> baseline_varuint_read(
  bytes::fragmented_buffer_parser&, field_context, input_boundary);
[[nodiscard]] result<std::uint64_t> codec_varuint_read(
  bytes::fragmented_buffer_parser&, field_context, input_boundary);

[[nodiscard]] result<void> native_fixed_write(
  bytes::fragmented_buffer_builder&, std::uint64_t, field_context);
[[nodiscard]] result<void> codec_fixed_write(
  bytes::fragmented_buffer_builder&, std::uint64_t, field_context);
[[nodiscard]] result<void> baseline_varuint_write(
  bytes::fragmented_buffer_builder&, std::uint64_t, field_context);
[[nodiscard]] result<void> codec_varuint_write(
  bytes::fragmented_buffer_builder&, std::uint64_t, field_context);

// Same input ownership, validation, work schedule and cleanup as the production
// CRC driver, with the configured comparison engine as its only alternative.
[[nodiscard]] seastar::future<result<std::uint32_t>>
google_crc32c_cooperatively(
  bytes::fragmented_buffer, cooperative_work&, std::uint32_t, error);

} // namespace kwaque::codec::bench
