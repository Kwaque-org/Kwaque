#pragma once

#include "src/bytes/test_allocation_profile.h"
#include "src/codec/tests/envelope_decode_test_support.h"
#include "src/protocol/frame_codec.h"

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace kwaque::protocol::testing::frame_fixture {

using bytes::testing::charge;
using codec::testing::envelope_fixture::crc32c;
using codec::testing::envelope_fixture::fragmented;
using codec::testing::envelope_fixture::put_u16;
using codec::testing::envelope_fixture::put_u32;
using codec::testing::envelope_fixture::put_u64;
using codec::testing::envelope_fixture::split_at;

inline constexpr codec::field_context context{
  .origin = 71, .family = 91, .field = 92};
inline constexpr frame_extent_limits bounds{
  byte_count{16'777'216}, byte_count{16'781'312}};
// The other half of the operation budget remains unavailable for native/frame
// costs. This fixture does not infer frame sizes or claim a shard RSS bound.
inline constexpr byte_count parent_budget{33'554'432};
inline constexpr frame_metadata metadata{
  frame_kind::submitted_batch,
  model::transport_stream_id{0x0807060504030201ULL},
  model::correlation_id{0x1817161514131211ULL},
  model::frame_sequence{0x2827262524232221ULL}};

inline void repair(std::string& data) {
    const auto size = static_cast<std::size_t>(
      static_cast<unsigned char>(data.at(8))
      | (static_cast<unsigned>(static_cast<unsigned char>(data.at(9))) << 8U));
    if (size < 48 || size > data.size())
        throw std::invalid_argument("incomplete fixture header");
    put_u32(data, 40, 0);
    put_u32(data, 40, crc32c(std::string_view{data}.substr(0, size)));
}

// Independent fixed-header builder; large fixtures need no contiguous body.
inline std::string header(
  std::uint32_t payload_bytes,
  std::uint32_t payload_crc,
  std::string_view extensions = {},
  frame_metadata fields = metadata) {
    if (extensions.size() > 4096 - 48 || payload_bytes > 16'777'216)
        throw std::invalid_argument("oversized fixture");
    std::string data(48, '\0');
    data.replace(0, 4, "KQWF");
    put_u16(data, 4, 1);
    put_u16(data, 6, static_cast<std::uint16_t>(fields.kind));
    put_u16(data, 8, static_cast<std::uint16_t>(48 + extensions.size()));
    put_u32(data, 12, payload_bytes);
    put_u64(data, 16, fields.stream.value());
    put_u64(data, 24, fields.correlation.value());
    put_u64(data, 32, fields.sequence.value());
    put_u32(data, 44, payload_crc);
    data.append(extensions);
    repair(data);
    return data;
}

inline std::string
wire(std::string_view payload = "abc", std::string_view extensions = {}) {
    if (payload.size() > 16'777'216)
        throw std::invalid_argument("oversized fixture");
    auto data = header(
      static_cast<std::uint32_t>(payload.size()), crc32c(payload), extensions);
    data.append(payload);
    return data;
}

inline std::string extension(
  std::uint16_t tag = 0x7ffe,
  std::uint16_t flags = 0,
  std::string_view value = "x") {
    std::string result(8, '\0');
    put_u16(result, 0, tag);
    put_u16(result, 2, flags);
    put_u32(result, 4, static_cast<std::uint32_t>(value.size()));
    result.append(value);
    return result;
}

// Bounded, independent large-byte fixture shared by qualification and timing.
// The header is constructed directly; generic byte assembly does no framing.
struct patterned_payload {
    bytes::fragmented_buffer bytes;
    std::uint32_t checksum;
};
seastar::future<patterned_payload> make_payload(
  std::size_t size, std::size_t width, codec::cooperative_work& work);
seastar::future<bytes::fragmented_buffer> wrap(
  patterned_payload&& payload,
  codec::cooperative_work& work,
  std::string_view extensions = {},
  frame_metadata fields = metadata);

inline codec::result<frame_header> inspect(
  bytes::fragmented_buffer_parser& input,
  codec::cooperative_work& work,
  frame_extent_limits owner_limits = bounds,
  codec::field_context origin = context,
  codec::input_boundary boundary = codec::input_boundary::open) {
    auto memory = codec::reserve_decode_input(
      input,
      work.policy(),
      {parent_budget, work.policy().config().max_metadata_bytes, charge},
      origin,
      boundary);
    if (!memory) return codec::failure(memory.error());
    return inspect_frame_header(
             input, owner_limits, *memory, work, origin, boundary)
      .get();
}

inline codec::result<bytes::fragmented_buffer> encode(
  bytes::fragmented_buffer&& input,
  codec::cooperative_work& work,
  frame_metadata fields = metadata,
  byte_count budget = parent_budget) {
    return encode_frame(
             std::move(input),
             fields,
             work,
             bounds,
             {},
             budget,
             charge,
             context)
      .get();
}

} // namespace kwaque::protocol::testing::frame_fixture
