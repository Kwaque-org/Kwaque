#pragma once

#include "src/bytes/test_allocation_profile.h"
#include "src/codec/cooperative.h"
#include "src/codec/tests/envelope_decode_test_support.h"
#include "src/protocol/control_codec.h"

#include <seastar/core/abort_source.hh>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace kwaque::protocol::testing::control_fixture {
using bytes::testing::charge;
using codec::testing::envelope_fixture::fragmented;
using codec::testing::envelope_fixture::split_at;
inline constexpr codec::field_context context{
  .origin = 97, .family = 73, .field = 71};

inline constexpr std::size_t control_fixture_fragment_limit = 128;

// Keep small fixtures fragmented at the requested width; grow that width for
// larger inputs so parent/child descriptors leave room for native parse state.
// Exact split-point and deliberately oversized layouts use fragmented/split_at.
inline bytes::fragmented_buffer
bounded_fragmented(std::string_view wire, std::size_t requested_width = 7) {
    if (requested_width == 0)
        throw std::invalid_argument("fixture fragment size is zero");
    const auto minimum_width
      = wire.empty() ? std::size_t{1}
                     : 1U + (wire.size() - 1U) / control_fixture_fragment_limit;
    return fragmented(wire, std::max(requested_width, minimum_width));
}

// Independent literal wire construction; production serialization is not an
// oracle for these fixtures. Native parsing is checked separately.
inline std::string varint(std::uint64_t value) {
    std::string out;
    while (value >= 128) {
        out.push_back(static_cast<char>((value & 127U) | 128U));
        value >>= 7U;
    }
    out.push_back(static_cast<char>(value));
    return out;
}
inline std::string scalar(std::uint32_t field, std::uint64_t value) {
    return varint(static_cast<std::uint64_t>(field) << 3U) + varint(value);
}
inline std::string blob(std::uint32_t field, std::string_view value) {
    return varint((static_cast<std::uint64_t>(field) << 3U) | 2U)
           + varint(value.size()) + std::string{value};
}
inline std::string format(std::uint16_t family = 65000) {
    return scalar(1, family) + scalar(2, 1) + scalar(3, 9)
           + std::string{"\x21\x00\x00\x00\x00\x00\x00\x00\x80", 9};
}
inline std::string capabilities(std::string versions = blob(1, "\x01\x02")) {
    return versions + blob(2, format()) + blob(3, std::string{"\0\xff\x01", 3})
           + scalar(4, 16'777'216) + scalar(5, 8'388'608) + scalar(6, 4096)
           + scalar(7, 1'048'576) + scalar(8, 4096);
}
inline std::string routing() {
    return blob(1, std::string(16, 't')) + blob(2, std::string(16, 'r'))
           + scalar(3, UINT64_MAX) + blob(4, std::string(16, 's'))
           + scalar(5, 1);
}
inline std::string
request(std::string build = {}, std::string caps = capabilities()) {
    return scalar(1, 2) + blob(4, build) + blob(5, caps);
}
inline std::string response(std::string build = {}) {
    return blob(1, std::string(16, 'c')) + blob(2, std::string(16, 'b'))
           + blob(3, build) + blob(4, capabilities());
}
inline std::string
redirect(std::string host = "localhost", std::uint32_t port = 65535) {
    return blob(1, routing()) + blob(2, std::string(16, 'b'))
           + blob(3, blob(1, host) + scalar(2, port)) + scalar(4, 3);
}
inline codec::decode_budget reserve(
  const bytes::fragmented_buffer_parser& input,
  const codec::cooperative_work& work,
  codec::field_context origin = context) {
    auto memory = codec::reserve_decode_input(
      input,
      work.policy(),
      {byte_count{33'554'432},
       work.policy().config().max_metadata_bytes,
       charge},
      origin);
    if (!memory) throw std::runtime_error("fixture input reservation failed");
    return *memory;
}
inline codec::decode_budget budget() {
    return {
      byte_count{33'554'432},
      codec::limits::defaults().config().max_metadata_bytes,
      charge};
}
inline control read(std::string_view wire, frame_kind kind) {
    bytes::fragmented_buffer_parser input{bounded_fragmented(wire)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto decoded = decode_control(
                     input, kind, {}, reserve(input, work), work, context)
                     .get();
    if (!decoded) throw std::runtime_error("invalid control fixture");
    return std::move(decoded->value);
}
inline std::string flatten(const bytes::fragmented_buffer& input) {
    std::string value(static_cast<std::size_t>(input.size().value()), '\0');
    if (!input.copy_to(std::span<char>{value}))
        throw std::runtime_error("fixture copy failed");
    return value;
}
} // namespace kwaque::protocol::testing::control_fixture
