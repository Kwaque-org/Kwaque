#ifndef KWAQUE_SRC_RUNTIME_PRODUCTION_RANDOM_H_
#define KWAQUE_SRC_RUNTIME_PRODUCTION_RANDOM_H_

#include "src/runtime/error.h"
#include "src/runtime/random.h"

#include <array>
#include <bit>
#include <concepts>
#include <cstdint>
#include <limits>
#include <new>

namespace kwaque::runtime::production {

namespace detail {

template<typename Entropy>
concept seed_entropy = requires(Entropy& entropy) {
    { entropy() } -> std::unsigned_integral;
};

template<seed_entropy Entropy>
[[nodiscard]] result<std::uint64_t> read_entropy_seed(Entropy& entropy) {
    using draw_type = decltype(entropy());
    constexpr auto draw_bits = std::numeric_limits<draw_type>::digits;
    static_assert(draw_bits > 0 && draw_bits <= 64);

    try {
        std::uint64_t seed = 0;
        unsigned offset = 0;
        while (offset < 64U) {
            const auto draw = static_cast<std::uint64_t>(entropy());
            const auto remaining = 64U - offset;
            const auto used = static_cast<unsigned>(
              draw_bits < remaining ? draw_bits : remaining);
            const auto mask = used == 64U
                                ? std::numeric_limits<std::uint64_t>::max()
                                : (std::uint64_t{1} << used) - 1U;
            seed |= (draw & mask) << offset;
            offset += used;
        }
        return seed;
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        return failure(
          operation_error{errc::unavailable, operation_kind::random});
    }
}

} // namespace detail

class random_source final {
public:
    explicit random_source(std::uint64_t seed) noexcept
      : state_(expand_seed(seed)) {}

    random_source(random_source&&) noexcept = default;
    random_source& operator=(random_source&&) noexcept = default;
    random_source(const random_source&) = delete;
    random_source& operator=(const random_source&) = delete;

    [[nodiscard]] static result<random_source> make();

    [[nodiscard]] std::uint64_t next_u64() noexcept {
        const auto result = std::rotl(state_[0] + state_[3], 23) + state_[0];
        const auto shifted = state_[1] << 17U;

        state_[2] ^= state_[0];
        state_[3] ^= state_[1];
        state_[1] ^= state_[2];
        state_[0] ^= state_[3];
        state_[2] ^= shifted;
        state_[3] = std::rotl(state_[3], 45);

        return result;
    }

private:
    [[nodiscard]] static constexpr std::uint64_t
    split_mix_64(std::uint64_t& seed) noexcept {
        seed += UINT64_C(0x9e3779b97f4a7c15);
        auto mixed = seed;
        mixed = (mixed ^ (mixed >> 30U)) * UINT64_C(0xbf58476d1ce4e5b9);
        mixed = (mixed ^ (mixed >> 27U)) * UINT64_C(0x94d049bb133111eb);
        return mixed ^ (mixed >> 31U);
    }

    [[nodiscard]] static constexpr std::array<std::uint64_t, 4>
    expand_seed(std::uint64_t seed) noexcept {
        return {
          split_mix_64(seed),
          split_mix_64(seed),
          split_mix_64(seed),
          split_mix_64(seed),
        };
    }

    std::array<std::uint64_t, 4> state_;
};

static_assert(kwaque::runtime::random_source<random_source>);

} // namespace kwaque::runtime::production

#endif // KWAQUE_SRC_RUNTIME_PRODUCTION_RANDOM_H_
