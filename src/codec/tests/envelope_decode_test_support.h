#pragma once

#include "src/bytes/fragmented_buffer.h"

#include <seastar/core/temporary_buffer.hh>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace kwaque::codec::testing::envelope_fixture {

using namespace std::literals;

inline constexpr auto fixed_body
  = "\x08\x07\x06\x05\x04\x03\x02\x01"
    "\x18\x17\x16\x15\x14\x13\x12\x11"
    "\x28\x27\x26\x25\x24\x23\x22\x21"
    "\x03\x00\x00\x00\x01\x00\x00\x00\x61\x62\x63\x00"sv;
inline constexpr auto fixed_prefix
  = "\x4b\x51\x42\x46\x01\x00\x01\x00\x01\x00\x20\x00\x24\x00\x00\x00"
    "\x00\x00\x00\x00\x00\x00\x00\x00\xa2\x49\x09\xdd\x07\x4f\xff\x9c"sv;
static_assert(fixed_body.size() == 36);
static_assert(fixed_prefix.size() == 32);

// Independent bitwise oracle over finalized CRC values. Fixtures never call
// the production checksum or prefix writer to construct their expected bytes.
[[nodiscard]] inline std::uint32_t crc32c(std::string_view bytes) noexcept {
    std::uint32_t state = 0xffffffffU;
    for (const char octet : bytes) {
        state ^= static_cast<unsigned char>(octet);
        for (unsigned bit = 0; bit < 8; ++bit) {
            state = (state >> 1U) ^ ((state & 1U) ? 0x82f63b78U : 0U);
        }
    }
    return state ^ 0xffffffffU;
}

inline void
put_u16(std::string& bytes, std::size_t offset, std::uint16_t value) {
    for (std::size_t index = 0; index < 2; ++index) {
        bytes.at(offset + index) = static_cast<char>(value & 0xffU);
        value >>= 8U;
    }
}

inline void
put_u32(std::string& bytes, std::size_t offset, std::uint32_t value) {
    for (std::size_t index = 0; index < 4; ++index) {
        bytes.at(offset + index) = static_cast<char>(value & 0xffU);
        value >>= 8U;
    }
}

inline void
put_u64(std::string& bytes, std::size_t offset, std::uint64_t value) {
    for (std::size_t index = 0; index < 8; ++index) {
        bytes.at(offset + index) = static_cast<char>(value & 0xffU);
        value >>= 8U;
    }
}

[[nodiscard]] inline std::uint16_t header_bytes(std::string_view encoded) {
    return static_cast<std::uint16_t>(
      static_cast<unsigned char>(encoded.at(10))
      | (static_cast<std::uint16_t>(static_cast<unsigned char>(encoded.at(11))) << 8U));
}

inline void repair_header_crc(std::string& encoded) {
    const auto size = header_bytes(encoded);
    if (size < 32 || size > encoded.size()) {
        throw std::invalid_argument("fixture has no complete declared header");
    }
    put_u32(encoded, 28, 0);
    put_u32(encoded, 28, crc32c(std::string_view{encoded}.substr(0, size)));
}

[[nodiscard]] inline std::string make_envelope(
  std::string_view body = fixed_body,
  std::string_view extensions = {},
  std::uint16_t family = 1,
  std::uint16_t writer = 1,
  std::uint16_t minimum_reader = 1,
  std::uint64_t features = 0) {
    if (
      extensions.size() > std::numeric_limits<std::uint16_t>::max() - 32U
      || body.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::invalid_argument("fixture exceeds fixed envelope widths");
    }
    std::string encoded(32, '\0');
    encoded.replace(0, 4, "KQBF");
    put_u16(encoded, 4, family);
    put_u16(encoded, 6, writer);
    put_u16(encoded, 8, minimum_reader);
    put_u16(encoded, 10, static_cast<std::uint16_t>(32 + extensions.size()));
    put_u32(encoded, 12, static_cast<std::uint32_t>(body.size()));
    put_u64(encoded, 16, features);
    put_u32(encoded, 24, crc32c(body));
    encoded.append(extensions);
    repair_header_crc(encoded);
    encoded.append(body);
    return encoded;
}

[[nodiscard]] inline bytes::fragmented_buffer
split_at(std::string_view encoded, std::size_t cut) {
    std::vector<seastar::temporary_buffer<char>> fragments;
    for (const auto part : {encoded.substr(0, cut), encoded.substr(cut)}) {
        seastar::temporary_buffer<char> fragment{part.size()};
        std::ranges::copy(part, fragment.get_write());
        fragments.push_back(std::move(fragment));
    }
    return bytes::fragmented_buffer::copy_from_fragments(fragments).value();
}

[[nodiscard]] inline bytes::fragmented_buffer
fragmented(std::string_view encoded, std::size_t fragment_size) {
    if (fragment_size == 0) {
        throw std::invalid_argument("fixture fragment size is zero");
    }
    std::vector<seastar::temporary_buffer<char>> fragments;
    for (std::size_t offset = 0; offset < encoded.size();
         offset += fragment_size) {
        const auto part = encoded.substr(offset, fragment_size);
        seastar::temporary_buffer<char> fragment{part.size()};
        std::ranges::copy(part, fragment.get_write());
        fragments.push_back(std::move(fragment));
    }
    return bytes::fragmented_buffer::copy_from_fragments(fragments).value();
}

} // namespace kwaque::codec::testing::envelope_fixture
