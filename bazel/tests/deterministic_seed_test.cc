#include <gtest/gtest.h>

#include <array>
#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <random>
#include <string>
#include <string_view>
#include <system_error>

namespace {

std::uint64_t test_seed() {
    const char* value = std::getenv("KWAQUE_TEST_SEED");
    if (value == nullptr) {
        ADD_FAILURE() << "KWAQUE_TEST_SEED is not set";
        return 1;
    }

    std::uint64_t seed = 0;
    const std::string_view encoded(value);
    const auto [end, error] = std::from_chars(
      encoded.data(), encoded.data() + encoded.size(), seed);
    if (error != std::errc{} || end != encoded.data() + encoded.size()) {
        ADD_FAILURE() << "invalid KWAQUE_TEST_SEED=" << encoded;
        return 1;
    }
    return seed;
}

std::array<std::uint64_t, 8> randomized_trace(std::uint64_t seed) {
    std::mt19937_64 generator(seed);
    std::array<std::uint64_t, 8> trace{};
    for (auto& value : trace) {
        value = generator();
    }
    return trace;
}

TEST(DeterministicSeedTest, ReproducesRandomizedTrace) {
    const std::uint64_t seed = test_seed();
    SCOPED_TRACE("KWAQUE_TEST_SEED=" + std::to_string(seed));
    EXPECT_EQ(randomized_trace(seed), randomized_trace(seed));
    EXPECT_NE(randomized_trace(seed), randomized_trace(seed + 1));
}

} // namespace
