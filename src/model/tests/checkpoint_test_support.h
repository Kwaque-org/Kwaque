#pragma once

#include "src/bytes/fragmented_buffer.h"
#include "src/codec/digest.h"
#include "src/codec/limits.h"
#include "src/model/checkpoint.h"

#include <seastar/core/temporary_buffer.hh>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace kwaque::model::testing::checkpoint_fixture {
namespace model = kwaque::model;
namespace codec = kwaque::codec;
using bytes::fragmented_buffer;
inline constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();

template<typename Id>
Id object(std::uint8_t first) {
    std::array<std::uint8_t, 16> raw{};
    for (std::size_t i = 0; i < raw.size(); ++i)
        raw[i] = static_cast<std::uint8_t>(first + i);
    return Id::make(raw).value();
}
inline model::topic_id topic() { return object<model::topic_id>(1); }
inline auto golden_cursors() {
    return std::array{
      model::range_cursor::make(
        object<model::range_id>(33),
        model::range_logical_end{0x0102030405060708})
        .value(),
      model::range_cursor::make(
        object<model::range_id>(129), model::range_logical_end{maximum})
        .value()};
}
inline model::range_cursor numbered(std::uint32_t index) {
    std::array<std::uint8_t, 16> raw{};
    raw[12] = static_cast<std::uint8_t>(index >> 24U);
    raw[13] = static_cast<std::uint8_t>(index >> 16U);
    raw[14] = static_cast<std::uint8_t>(index >> 8U);
    raw[15] = static_cast<std::uint8_t>(index);
    return model::range_cursor::make(
             model::range_id::make(raw).value(),
             model::range_logical_end{index - 1U})
      .value();
}
inline std::string unhex(std::string_view hex) {
    std::string result;
    result.reserve(hex.size() / 2U);
    const auto digit = [](char c) { return c <= '9' ? c - '0' : c - 'a' + 10; };
    for (std::size_t i = 0; i < hex.size(); i += 2)
        result.push_back(
          static_cast<char>(16 * digit(hex[i]) + digit(hex[i + 1])));
    return result;
}
inline std::string digest_bytes(codec::checkpoint_digest digest) {
    const auto raw = digest.bytes();
    return {raw.begin(), raw.end()};
}
inline std::string flatten(const fragmented_buffer& buffer) {
    std::string result;
    result.reserve(buffer.size().value());
    for (const auto part : buffer)
        result.append(part.data(), part.size());
    return result;
}
inline fragmented_buffer
fragmented(std::string_view bytes, std::size_t width = 7) {
    std::vector<seastar::temporary_buffer<char>> parts;
    parts.reserve((bytes.size() + width - 1U) / width);
    while (!bytes.empty()) {
        const auto count = std::min(width, bytes.size());
        parts.emplace_back(bytes.data(), count);
        bytes.remove_prefix(count);
    }
    return fragmented_buffer::copy_from_fragments(parts).value();
}
// Independent fixed-offset byte and bitwise checksum fixture writers.
inline void put(
  std::string& bytes,
  std::size_t offset,
  std::uint64_t value,
  std::size_t width) {
    for (std::size_t i = 0; i < width; ++i) {
        bytes[offset + i] = static_cast<char>(value & 0xffU);
        value >>= 8U;
    }
}
inline std::uint32_t crc(std::string_view input) {
    std::uint32_t value = 0xffffffffU;
    for (const char c : input) {
        value ^= static_cast<unsigned char>(c);
        for (unsigned i = 0; i < 8; ++i)
            value = (value >> 1U) ^ ((value & 1U) != 0 ? 0x82f63b78U : 0U);
    }
    return value ^ 0xffffffffU;
}
inline void repair(std::string& wire, std::size_t header = 32) {
    put(wire, 24, crc(std::string_view{wire}.substr(header)), 4);
    put(wire, 28, 0, 4);
    put(wire, 28, crc(std::string_view{wire}.substr(0, header)), 4);
}
inline std::string extended(std::string wire, std::size_t header) {
    std::string extension(header - 32, '\0');
    put(extension, 0, 77, 2);
    put(extension, 4, header - 40, 4);
    wire.insert(32, extension);
    put(wire, 10, header, 2);
    repair(wire, header);
    return wire;
}

// Fixed-offset reference grammar and complete fixtures. No checkpoint or
// envelope encoder/decoder is called. Inputs/owners stay bounded to 4097
// cursors and 128 KiB; callers bracket large setup outside measured work.
struct probe_result final {
    errc error{errc::success};
    std::size_t used{0};
    std::size_t header{0};
    std::size_t count{0};
};
std::uint64_t little(std::string_view wire, std::size_t at, std::size_t width);
std::string wire(std::uint32_t count, std::size_t header = 32);
std::string body_from_value(const read_checkpoint& value);
codec::checkpoint_digest digest(std::string_view body);
probe_result probe(std::string_view wire, topic_id expected, bool complete);

} // namespace kwaque::model::testing::checkpoint_fixture
