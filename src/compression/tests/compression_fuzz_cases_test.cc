#include "src/compression/tests/compression_fuzz_cases.h"
#include "src/compression/tests/lz4_test_support.h"

#include <seastar/core/thread.hh>

#include <gtest/gtest.h>

#include <array>
#include <vector>

namespace kwaque::compression::testing {
TEST(CompressionFuzzCasesTest, ModesAdmissionAndFragmentation) {
    for (std::uint8_t mode = 0; mode < 4; ++mode) {
        for (std::uint8_t flags = 0; flags < 16; ++flags) {
            for (std::uint8_t mutation = 0; mutation < 6; ++mutation) {
                std::array<std::uint8_t, 12> input{
                  mode,
                  mutation,
                  flags,
                  mutation,
                  3,
                  0,
                  0,
                  0,
                  'a',
                  'b',
                  'c',
                  'd'};
                exercise_compression_case(input);
                seastar::thread::maybe_yield();
            }
        }
    }
}

TEST(CompressionFuzzCasesTest, IndependentFramesAndEveryTruncation) {
    for (const auto hex : {raw_abc_hex, compressed_abc_hex, empty_hex}) {
        const auto frame = from_hex(hex);
        for (std::size_t length = 0; length <= frame.size(); ++length) {
            std::vector<std::uint8_t> input{
              2,
              0,
              0,
              0,
              static_cast<std::uint8_t>(hex == empty_hex ? 0 : 3),
              0,
              0,
              0};
            input.insert(
              input.end(),
              frame.begin(),
              frame.begin() + static_cast<std::ptrdiff_t>(length));
            exercise_compression_case(input);
        }
    }
}

TEST(CompressionFuzzCasesTest, MaximumScriptAndNativeChecksumCanary) {
    exercise_compression_case({});
    std::vector<std::uint8_t> input(compression_fuzz_max_input);
    for (std::size_t i = 8; i < input.size(); ++i)
        input[i] = static_cast<std::uint8_t>((i * 113U + i / 7U) % 256U);
    for (std::uint8_t mode = 0; mode < 4; ++mode) {
        input[0] = mode;
        input[1] = 0;
        input[3] = 5;
        exercise_compression_case(input);
    }
}
} // namespace kwaque::compression::testing
