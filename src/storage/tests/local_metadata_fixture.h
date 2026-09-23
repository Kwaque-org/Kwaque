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

namespace kwaque::storage::testing::local_fixture {
// Small checked-in artifacts only. Large qualification keeps fragmented owners.
inline std::string read(std::string_view name) {
    const auto* directory = std::getenv("TEST_SRCDIR");
    const auto* workspace = std::getenv("TEST_WORKSPACE");
    const auto root = directory && workspace
                        ? std::string{directory} + "/" + workspace + "/"
                        : std::string{};
    std::ifstream file{
      root + "src/storage/tests/testdata/local_metadata/" + std::string{name}
      + ".hex"};
    if (!file) throw std::runtime_error("cannot open format fixture");
    std::string result;
    result.reserve(65536);
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
              || result.size() == 65536)
                throw std::runtime_error("invalid format fixture extent");
            result.push_back(
              std::bit_cast<char>(static_cast<std::uint8_t>(octet)));
        }
    }
    if (!file.eof()) throw std::runtime_error("cannot read format fixture");
    return result;
}

} // namespace kwaque::storage::testing::local_fixture
