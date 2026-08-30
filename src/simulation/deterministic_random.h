#ifndef KWAQUE_SRC_SIMULATION_DETERMINISTIC_RANDOM_H_
#define KWAQUE_SRC_SIMULATION_DETERMINISTIC_RANDOM_H_

#include "src/runtime/error.h"
#include "src/runtime/random.h"
#include "src/simulation/event_trace.h"
#include "src/simulation/philox.h"

#include <array>
#include <cstddef>
#include <cstdint>

namespace kwaque::simulation {

class deterministic_random_test_access;

inline constexpr std::uint32_t deterministic_random_algorithm_version{1};
inline constexpr std::uint32_t deterministic_random_coordinate_version{1};
inline constexpr std::uint32_t deterministic_random_derivation_tag{
  UINT32_C(0x314b514b)};

enum class random_domain : std::uint32_t {
    runtime_stream = UINT32_C(0x00000001),
    fault_decision = UINT32_C(0x00000002),
    storage_decision = UINT32_C(0x00000003),
    network_decision = UINT32_C(0x00000004),
    dns_decision = UINT32_C(0x00000005),
};

inline constexpr std::array registered_random_domains{
  random_domain::runtime_stream,
  random_domain::fault_decision,
  random_domain::storage_decision,
  random_domain::network_decision,
  random_domain::dns_decision,
};

namespace detail {

template<std::size_t Size>
[[nodiscard]] consteval bool
random_domains_are_unique(const std::array<random_domain, Size>& domains) {
    for (std::size_t left = 0; left < domains.size(); ++left) {
        for (std::size_t right = left + 1; right < domains.size(); ++right) {
            if (domains[left] == domains[right]) {
                return false;
            }
        }
    }
    return true;
}

[[nodiscard]] constexpr bool is_registered(random_domain domain) noexcept {
    for (const auto registered : registered_random_domains) {
        if (domain == registered) {
            return true;
        }
    }
    return false;
}

[[nodiscard]] constexpr std::array<std::uint32_t, 2>
split_u64(std::uint64_t value) noexcept {
    return {
      static_cast<std::uint32_t>(value),
      static_cast<std::uint32_t>(value >> 32U),
    };
}

} // namespace detail

static_assert(detail::random_domains_are_unique(registered_random_domains));

class random_coordinate final {
public:
    [[nodiscard]] static runtime::result<random_coordinate> make(
      random_domain domain,
      std::uint64_t stable_id,
      std::uint64_t occurrence) noexcept;

    [[nodiscard]] constexpr random_domain domain() const noexcept {
        return domain_;
    }
    [[nodiscard]] constexpr std::uint64_t stable_id() const noexcept {
        return stable_id_;
    }
    [[nodiscard]] constexpr std::uint64_t occurrence() const noexcept {
        return occurrence_;
    }

    bool operator==(const random_coordinate&) const = default;

private:
    constexpr random_coordinate(
      random_domain domain,
      std::uint64_t stable_id,
      std::uint64_t occurrence) noexcept
      : domain_(domain)
      , stable_id_(stable_id)
      , occurrence_(occurrence) {}

    random_domain domain_;
    std::uint64_t stable_id_;
    std::uint64_t occurrence_;
};

class sequential_random_source final {
public:
    [[nodiscard]] std::uint64_t next_u64() noexcept;
    void reset(std::uint64_t occurrence) noexcept;

    [[nodiscard]] random_domain domain() const noexcept { return domain_; }
    [[nodiscard]] std::uint64_t stable_id() const noexcept {
        return stable_id_;
    }
    [[nodiscard]] std::uint64_t occurrence() const noexcept {
        return occurrence_;
    }
    [[nodiscard]] std::uint64_t draw_index() const noexcept {
        return next_draw_;
    }
    [[nodiscard]] bool exhausted() const noexcept { return exhausted_; }

private:
    friend class deterministic_random;
    friend class deterministic_random_test_access;

    sequential_random_source(
      random_domain domain,
      std::uint64_t stable_id,
      std::uint64_t occurrence,
      philox_key key) noexcept;

    random_domain domain_;
    std::uint64_t stable_id_;
    std::uint64_t occurrence_;
    std::uint64_t next_draw_{0};
    std::uint64_t cached_block_index_{0};
    philox_key key_;
    philox_counter cached_block_{};
    bool cache_valid_{false};
    bool exhausted_{false};
};

class deterministic_random final {
public:
    explicit constexpr deterministic_random(std::uint64_t master_seed) noexcept
      : master_seed_(master_seed) {}

    [[nodiscard]] runtime::result<sequential_random_source> stream(
      random_domain domain,
      std::uint64_t stable_id,
      std::uint64_t occurrence = 0) const noexcept;
    [[nodiscard]] sequential_random_source
    stream(random_coordinate coordinate) const noexcept;
    [[nodiscard]] sequential_random_source cursor(
      random_coordinate coordinate, std::uint64_t draw_index) const noexcept;
    [[nodiscard]] std::uint64_t word_at(
      random_coordinate coordinate, std::uint64_t draw_index) const noexcept;
    [[nodiscard]] runtime::result<std::uint64_t> recorded_word_at(
      event_trace& trace,
      runtime::monotonic_time time,
      random_coordinate coordinate,
      std::uint64_t draw_index) const noexcept;

    [[nodiscard]] constexpr std::uint64_t master_seed() const noexcept {
        return master_seed_;
    }

private:
    [[nodiscard]] philox_key
    derive_key(random_domain domain, std::uint64_t stable_id) const noexcept;

    std::uint64_t master_seed_;
};

static_assert(runtime::random_source<sequential_random_source>);

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_DETERMINISTIC_RANDOM_H_
