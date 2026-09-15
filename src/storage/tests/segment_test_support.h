#pragma once

#include "src/bytes/fragmented_buffer_builder.h"
#include "src/bytes/test_allocation_profile.h"
#include "src/storage/segment_format.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/thread.hh>

#include <algorithm>
#include <array>
#include <bit>
#include <charconv>
#include <limits>
#include <lz4frame.h>
#include <stdexcept>
#include <string>
#include <string_view>

namespace kwaque::storage::testing {
using kwaque::bytes::testing::charge;
inline codec::decode_budget budget() {
    return {byte_count{32U << 20U}, byte_count{1U << 20U}, charge};
}
template<typename Id>
Id id(std::uint8_t value) {
    std::array<std::uint8_t, 16> bytes{};
    bytes.fill(value);
    return Id::make(bytes).value();
}
inline segment_context
sc(std::uint8_t segment = 0x30, std::uint64_t generation = 1) {
    return segment_context::make(
             id<model::cluster_id>(0x50),
             id<model::topic_id>(0x10),
             id<model::range_id>(0x20),
             id<model::segment_id>(segment),
             model::segment_generation::make(generation).value())
      .value();
}
inline storage_alignment alignment(std::uint64_t value = 512) {
    return storage_alignment::make(byte_count{value}).value();
}
inline segment_header header() {
    return segment_header::make(
             sc(), model::range_logical_end{100}, alignment())
      .value();
}
inline model::batch_decode_expectation batch_expected() {
    return {id<model::topic_id>(0x10), id<model::range_id>(0x20)};
}
inline segment_block_expectation block_expected(
  std::uint8_t segment = 0x30,
  std::uint64_t generation = 1,
  std::uint64_t position = 512,
  std::uint64_t physical = 0,
  std::uint64_t multiple = 512) {
    return {
      segment_write_context::make(
        sc(segment, generation),
        alignment(multiple),
        model::segment_relative_end{physical},
        runtime::file_position{position})
        .value(),
      runtime::file_position{multiple},
      batch_expected()};
}
inline std::string hex(std::string_view value) {
    std::string result;
    result.reserve(value.size() / 2U);
    for (std::size_t at = 0; at < value.size(); at += 2U) {
        unsigned byte = 0;
        const auto parsed = std::from_chars(
          value.data() + at, value.data() + at + 2U, byte, 16);
        if (parsed.ec != std::errc{} || parsed.ptr != value.data() + at + 2U)
            throw std::invalid_argument("invalid fixture hex");
        result.push_back(std::bit_cast<char>(static_cast<std::uint8_t>(byte)));
    }
    return result;
}
inline void
put(std::string& bytes, std::size_t at, std::uint64_t value, std::size_t size) {
    for (std::size_t i = 0; i < size; ++i)
        bytes.at(at + i) = std::bit_cast<char>(
          static_cast<std::uint8_t>(value >> (8U * i)));
}
inline std::uint64_t
get(std::string_view bytes, std::size_t at, std::size_t size) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < size; ++i)
        value |= std::uint64_t{static_cast<unsigned char>(bytes.at(at + i))}
                 << (8U * i);
    return value;
}
inline std::uint32_t crc(std::string_view bytes) {
    std::uint32_t value = 0xffffffffU;
    for (char byte : bytes) {
        value ^= static_cast<unsigned char>(byte);
        for (unsigned bit = 0; bit < 8; ++bit)
            value = (value >> 1U) ^ ((value & 1U) ? 0x82f63b78U : 0U);
    }
    return value ^ 0xffffffffU;
}
inline void repair(std::string& wire) {
    const auto h = static_cast<std::size_t>(get(wire, 10, 2));
    put(wire, 24, crc(std::string_view{wire}.substr(h)), 4);
    put(wire, 28, 0, 4);
    put(wire, 28, crc(std::string_view{wire}.substr(0, h)), 4);
}
inline std::string
frame(std::string body, std::uint16_t family, std::size_t h = 32) {
    std::string wire(h, '\0');
    wire.replace(0, 4, "KQBF");
    put(wire, 4, family, 2);
    put(wire, 6, h == 32 ? 1U : 2U, 2);
    put(wire, 8, 1, 2);
    put(wire, 10, h, 2);
    put(wire, 12, body.size(), 4);
    if (h > 32) {
        put(wire, 32, 0x7ffe, 2);
        put(wire, 36, h - 40U, 4);
        std::fill(wire.begin() + 40, wire.end(), 't');
    }
    wire += body;
    repair(wire);
    return wire;
}
inline void put_sc(std::string& body, segment_context context) {
    const auto copy = [&](auto value, std::size_t offset) {
        for (std::size_t i = 0; i < 16; ++i)
            body[offset + i] = std::bit_cast<char>(value.bytes()[i]);
    };
    copy(context.cluster(), 0);
    copy(context.topic(), 16);
    copy(context.range(), 32);
    copy(context.segment(), 48);
    put(body, 64, context.generation().value(), 8);
}
inline std::string
header_wire(segment_header value = header(), std::size_t h = 32) {
    std::string body(96, '\0');
    put_sc(body, value.context());
    put(body, 72, value.logical_origin().value(), 8);
    put(body, 80, 1, 2);
    put(body, 82, 1, 2);
    const auto a = value.alignment().bytes().value();
    put(body, 84, a, 4);
    const auto pad = (a - (h + body.size()) % a) % a;
    put(body, 88, pad, 4);
    body.append(pad, '\0');
    return frame(std::move(body), 3, h);
}
inline std::string assigned_wire(
  bool compressed = false, std::size_t h = 32, bool sparse = false) {
    std::string body(184, '\0');
    body.replace(0, 16, 16, '\x40');
    put(body, 16, 1, 8);
    put(body, 24, 1, 8);
    body.replace(40, 16, 16, '\x10');
    body.replace(56, 16, 16, '\x20');
    put(body, 72, 1, 8);
    body.replace(80, 16, 16, '\x30');
    put(body, 96, 1, 8);
    const auto digest = hex(
      sparse
        ? "1ca1f5939c787dc18baed7e6a9869863589d8164d20bf7511b779f928fab5c64"
        : "faf92b853fd9029baa6091b240ff60cbd8e82375f8d26d28e65160c5b88b8a02");
    body.replace(104, 32, digest);
    const auto raw = hex(
      sparse ? "0600010101000006000303010000" : "06000000010000");
    put(body, 144, sparse ? 5U : 1U, 4);
    put(body, 148, sparse ? 2U : 1U, 4);
    put(body, 156, compressed ? 1U : 0U, 1);
    put(body, 158, 1, 2);
    put(body, 164, raw.size(), 4);
    put(body, 168, 100, 8);
    put(body, 176, sparse ? 105U : 101U, 8);
    auto records = raw;
    if (compressed) {
        LZ4F_preferences_t prefs{};
        prefs.frameInfo.blockSizeID = LZ4F_max64KB;
        prefs.frameInfo.blockMode = LZ4F_blockIndependent;
        prefs.frameInfo.contentChecksumFlag = LZ4F_contentChecksumEnabled;
        prefs.frameInfo.blockChecksumFlag = LZ4F_blockChecksumEnabled;
        prefs.frameInfo.contentSize = raw.size();
        std::array<char, 128> output{};
        const auto size = LZ4F_compressFrame(
          output.data(), output.size(), raw.data(), raw.size(), &prefs);
        if (LZ4F_isError(size))
            throw std::runtime_error(LZ4F_getErrorName(size));
        records.assign(output.data(), size);
    }
    put(body, 160, records.size(), 4);
    body += records;
    return frame(std::move(body), 2, h);
}
inline std::string block_wire(
  std::string child = assigned_wire(),
  segment_block_expectation expected = block_expected(),
  std::size_t h = 32) {
    std::string body(120, '\0');
    put_sc(body, expected.location.segment());
    put(body, 72, expected.location.position().value(), 8);
    put(body, 80, expected.location.physical_begin().value(), 8);
    const auto inner = static_cast<std::size_t>(get(child, 10, 2));
    put(
      body,
      88,
      expected.location.physical_begin().value() + get(child, inner + 148, 4),
      8);
    put(body, 96, get(child, inner + 168, 8), 8);
    put(body, 104, get(child, inner + 176, 8), 8);
    put(body, 112, child.size(), 4);
    const auto a = expected.location.alignment().bytes().value();
    const auto pad = (a - (h + body.size() + child.size()) % a) % a;
    put(body, 116, pad, 4);
    body += child;
    body.append(pad, '\0');
    return frame(std::move(body), 4, h);
}
inline bytes::fragmented_buffer
buffer(std::string_view value, std::size_t width = 67) {
    const auto count = value.empty() ? 0U : (value.size() + width - 1U) / width;
    if (count > 1024)
        throw std::invalid_argument("fixture fragmentation limit");
    bytes::fragmented_buffer_builder builder{
      {.initial_fragment_bytes = byte_count{width},
       .max_fragment_bytes = byte_count{width},
       .max_total_bytes = byte_count{std::max(width, value.size())},
       .max_retained_bytes
       = byte_count{std::max(std::size_t{1}, count) * width},
       .max_fragments = std::max(std::size_t{1}, count)}};
    builder.reserve_fragments(item_count{count}).value();
    while (!value.empty()) {
        const auto size = std::min({value.size(), width, std::size_t{4096}});
        builder.append(std::span{value.data(), size}).value();
        value.remove_prefix(size);
        seastar::thread::maybe_yield();
    }
    return builder.finish().value();
}
inline std::string flat(const bytes::fragmented_buffer& value) {
    if (value.size().value() >= 131072)
        throw std::invalid_argument("fixture flattening limit");
    std::string out;
    out.reserve(value.size().value());
    for (auto fragment : value)
        out.append(fragment.data(), fragment.size());
    return out;
}
inline codec::decode_budget reserve(
  const bytes::fragmented_buffer_parser& input,
  codec::cooperative_work& work,
  codec::field_context context = {}) {
    return codec::reserve_decode_input(input, work.policy(), budget(), context)
      .value();
}
inline codec::decode_budget
reserve(const bytes::fragmented_buffer& input, codec::cooperative_work& work) {
    const auto cost = input.allocation_cost(charge).value();
    return codec::detail::consume_decode_budget(
             work.policy(),
             budget(),
             cost.backing,
             *cost.descriptors.checked_add(cost.share_controls),
             {},
             0)
      .value();
}
inline encoded_assigned_batch checked_child(
  codec::cooperative_work& work,
  bool compressed = false,
  std::size_t h = 32,
  bool sparse = false) {
    auto input = buffer(assigned_wire(compressed, h, sparse));
    const auto memory = reserve(input, work);
    return validate_encoded_assigned_batch(
             std::move(input), batch_expected(), memory, work)
      .get()
      .value();
}
} // namespace kwaque::storage::testing
