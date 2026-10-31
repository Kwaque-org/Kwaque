// The inline short path against the out-of-line call, for the key lengths a
// record carries. Each iteration hashes keys_per_run keys.
#include "src/codec/xxh3.h"
#include "src/codec/xxh3_64_inline.h"

#include <seastar/testing/perf_tests.hh>

#include <array>
#include <cstddef>
#include <cstdint>
#include <stdexcept>

namespace kwaque::codec::bench {
namespace {

constexpr std::size_t keys_per_run = 1024;
constexpr std::size_t material_bytes = 64 * 1024;

[[gnu::always_inline]] inline void clobber_memory() {
    // NOLINTNEXTLINE(portability-no-assembler)
    asm volatile("" : : : "memory");
}

class key_fixture {
public:
    key_fixture() {
        std::uint64_t state = 0x6b77617175650002ULL;
        for (auto& octet : material_) {
            state = state * 6364136223846793005ULL + 1442695040888963407ULL;
            octet = static_cast<std::byte>(state >> 56U);
        }
        for (std::size_t at = 0; at < keys_per_run; ++at) {
            state = state * 6364136223846793005ULL + 1442695040888963407ULL;
            // 1-64 bytes: the range most record keys fall in.
            mixed_lengths_[at] = static_cast<std::size_t>(state >> 58U) + 1;
            offsets_[at] = static_cast<std::size_t>(
              (state >> 20U) % (material_bytes - 256));
        }
    }

protected:
    // Independent keys: the processor may overlap the hashes.
    template<bool Inline>
    std::size_t throughput(std::size_t fixed_length) {
        std::uint64_t sum = 0;
        perf_tests::start_measuring_time();
        for (std::size_t at = 0; at < keys_per_run; ++at) {
            clobber_memory();
            const auto length = fixed_length != 0 ? fixed_length
                                                  : mixed_lengths_[at];
            sum += hash<Inline>(material_.data() + offsets_[at], length);
        }
        perf_tests::do_not_optimize(sum);
        perf_tests::stop_measuring_time();
        return keys_per_run;
    }

    // Each key's position depends on the previous hash: one at a time.
    template<bool Inline>
    std::size_t latency(std::size_t fixed_length) {
        std::uint64_t value = 0;
        perf_tests::start_measuring_time();
        for (std::size_t at = 0; at < keys_per_run; ++at) {
            const auto offset = (offsets_[at] + (value & 63U))
                                % (material_bytes - 256);
            const auto length = fixed_length != 0 ? fixed_length
                                                  : mixed_lengths_[at];
            value = hash<Inline>(material_.data() + offset, length);
        }
        perf_tests::do_not_optimize(value);
        perf_tests::stop_measuring_time();
        if (value == 0) throw std::runtime_error("unexpected zero hash");
        return keys_per_run;
    }

private:
    template<bool Inline>
    static std::uint64_t hash(const std::byte* data, std::size_t size) {
        if constexpr (Inline) {
            return xxh3_64_inline(data, size);
        } else {
            return xxh3_64(data, size);
        }
    }

    std::array<std::byte, material_bytes> material_{};
    std::array<std::size_t, keys_per_run> mixed_lengths_{};
    std::array<std::size_t, keys_per_run> offsets_{};
};

#define XXH3_64_BENCH(Name, Length)                                            \
    PERF_TEST_F(key_fixture, Name##_throughput_call) {                         \
        return throughput<false>(Length);                                      \
    }                                                                          \
    PERF_TEST_F(key_fixture, Name##_throughput_inline) {                       \
        return throughput<true>(Length);                                       \
    }                                                                          \
    PERF_TEST_F(key_fixture, Name##_latency_call) {                            \
        return latency<false>(Length);                                         \
    }                                                                          \
    PERF_TEST_F(key_fixture, Name##_latency_inline) {                          \
        return latency<true>(Length);                                          \
    }

XXH3_64_BENCH(mixed_1_to_64, 0)
XXH3_64_BENCH(fixed_8, 8)
XXH3_64_BENCH(fixed_16, 16)
XXH3_64_BENCH(fixed_17, 17)
XXH3_64_BENCH(fixed_36, 36)
XXH3_64_BENCH(fixed_128, 128)
XXH3_64_BENCH(fixed_240, 240)

} // namespace
} // namespace kwaque::codec::bench
