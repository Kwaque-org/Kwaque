#ifndef KWAQUE_SRC_RUNTIME_PRODUCTION_RANDOM_H_
#define KWAQUE_SRC_RUNTIME_PRODUCTION_RANDOM_H_

#include "src/base/invariant.h"
#include "src/runtime/error.h"
#include "src/runtime/random.h"

#include <sys/random.h>

#include <array>
#include <bit>
#include <cerrno>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <system_error>

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
    } catch (const std::system_error&) {
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

namespace detail {

// One call of the kernel's random source: the bytes written, or -1 with
// errno set.
template<typename Call>
concept kernel_random_call = requires(
  Call& call, void* buffer, std::size_t size, unsigned flags) {
    { call(buffer, size, flags) } noexcept -> std::same_as<long>;
};

// Asks, without blocking, whether the kernel's pool is ready. Before it is,
// a read would block the reactor, so the start refuses instead.
template<kernel_random_call Call>
[[nodiscard]] result<void> probe_identity_entropy(Call& call) noexcept {
    std::array<std::uint8_t, identity_entropy_bytes> octets{};
    for (;;) {
        const auto read = call(octets.data(), octets.size(), GRND_NONBLOCK);
        if (read == static_cast<long>(octets.size())) {
            return {};
        }
        if (read < 0 && errno == EINTR) {
            continue;
        }
        return failure(
          operation_error{errc::unavailable, operation_kind::random});
    }
}

// Repeats a call a signal interrupted and continues a short read where it
// stopped. Once the pool is ready the kernel neither fails nor interrupts a
// read of this size, so any other failure is a broken invariant.
template<kernel_random_call Call>
void fill_identity_octets(
  Call& call, std::span<std::uint8_t, identity_entropy_bytes> octets) noexcept {
    std::size_t offset = 0;
    while (offset < octets.size()) {
        const auto read = call(
          octets.data() + offset, octets.size() - offset, 0U);
        if (read < 0) {
            const auto error = errno;
            KWAQUE_INVARIANT(
              invariant_id{"KQ-RUNTIME-IDENTITY-ENTROPY"},
              error == EINTR,
              "the kernel random source failed after its start probe");
            continue;
        }
        offset += static_cast<std::size_t>(read);
    }
}

} // namespace detail

// Octets for object identities, one kernel call (getrandom) per identity. The
// shard's random_source is never used for identities: its 64-bit seed bounds
// the distinct streams a cluster can draw, and a cloned machine repeats one.
class identity_entropy final {
public:
    // Fails with unavailable while the kernel's pool is not ready.
    [[nodiscard]] static result<identity_entropy> make();

    identity_entropy(identity_entropy&&) noexcept = default;
    identity_entropy& operator=(identity_entropy&&) noexcept = default;
    identity_entropy(const identity_entropy&) = delete;
    identity_entropy& operator=(const identity_entropy&) = delete;

    void fill(std::span<std::uint8_t, identity_entropy_bytes> octets) noexcept;

private:
    identity_entropy() noexcept = default;
};

static_assert(kwaque::runtime::identity_entropy_source<identity_entropy>);

} // namespace kwaque::runtime::production

#endif // KWAQUE_SRC_RUNTIME_PRODUCTION_RANDOM_H_
