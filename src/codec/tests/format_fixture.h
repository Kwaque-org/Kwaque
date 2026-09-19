#pragma once

#include <array>
#include <bit>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <stdexcept>
#include <string>
#include <string_view>

namespace kwaque::codec::testing::format_fixture {
// Small checked-in artifacts only. Large qualification keeps fragmented owners.
inline std::string read(std::string_view name) {
    const auto* directory = std::getenv("TEST_SRCDIR");
    const auto* workspace = std::getenv("TEST_WORKSPACE");
    const auto root = directory && workspace
                        ? std::string{directory} + "/" + workspace + "/"
                        : std::string{};
    std::ifstream file{
      root + "src/codec/tests/testdata/formats/" + std::string{name} + ".hex"};
    if (!file) throw std::runtime_error("cannot open format fixture");
    std::string result;
    result.reserve(4096);
    std::string token;
    while (file >> token) {
        if (token.size() % 2 != 0 || token.size() > 8192)
            throw std::runtime_error("invalid format fixture hex");
        for (std::size_t at = 0; at < token.size(); at += 2) {
            unsigned octet = 0;
            const auto parsed = std::from_chars(
              token.data() + at, token.data() + at + 2, octet, 16);
            if (
              parsed.ec != std::errc{} || parsed.ptr != token.data() + at + 2
              || result.size() == 4096)
                throw std::runtime_error("invalid format fixture extent");
            result.push_back(
              std::bit_cast<char>(static_cast<std::uint8_t>(octet)));
        }
    }
    if (!file.eof()) throw std::runtime_error("cannot read format fixture");
    return result;
}

inline std::array<unsigned char, 32> digest(std::string_view name) {
    const auto wire = read(name);
    if (wire.size() != 32) throw std::runtime_error("invalid digest fixture");
    std::array<unsigned char, 32> result{};
    for (std::size_t i = 0; i < result.size(); ++i)
        result[i] = static_cast<unsigned char>(wire[i]);
    return result;
}
} // namespace kwaque::codec::testing::format_fixture
