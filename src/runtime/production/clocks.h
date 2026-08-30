#ifndef KWAQUE_SRC_RUNTIME_PRODUCTION_CLOCKS_H_
#define KWAQUE_SRC_RUNTIME_PRODUCTION_CLOCKS_H_

#include "src/runtime/time.h"

#include <seastar/core/lowres_clock.hh>

#include <chrono>
#include <concepts>
#include <cstdint>
#include <limits>
#include <ratio>

namespace kwaque::runtime::production {

namespace detail {

template<typename Clock>
inline constexpr bool has_canonical_nanosecond_period
  = std::ratio_equal_v<typename Clock::period, std::nano>;

using native_monotonic_rep = seastar::lowres_clock::rep;
using native_wall_rep = seastar::lowres_system_clock::rep;
using wall_nanosecond_scale
  = std::ratio_divide<seastar::lowres_system_clock::period, std::nano>;

static_assert(has_canonical_nanosecond_period<seastar::lowres_clock>);
static_assert(std::signed_integral<native_monotonic_rep>);
static_assert(std::signed_integral<native_wall_rep>);
static_assert(sizeof(native_monotonic_rep) == sizeof(monotonic_time::rep));
static_assert(
  std::numeric_limits<native_monotonic_rep>::digits + 1
  == std::numeric_limits<monotonic_time::rep>::digits);
static_assert(sizeof(native_wall_rep) == sizeof(wall_time::rep));
static_assert(
  std::numeric_limits<native_wall_rep>::digits
  == std::numeric_limits<wall_time::rep>::digits);
static_assert(wall_nanosecond_scale::num > 0);
static_assert(wall_nanosecond_scale::den == 1);

} // namespace detail

class monotonic_clock final {
public:
    [[nodiscard]] static monotonic_time now() noexcept {
        const auto native
          = seastar::lowres_clock::now().time_since_epoch().count();
        return monotonic_time{static_cast<monotonic_time::rep>(native)};
    }
};

class wall_clock final {
public:
    [[nodiscard]] static wall_time now() noexcept {
        const auto native
          = seastar::lowres_system_clock::now().time_since_epoch().count();
        const auto nanoseconds = static_cast<__int128_t>(native)
                                 * detail::wall_nanosecond_scale::num;
        return wall_time{static_cast<wall_time::rep>(nanoseconds)};
    }
};

static_assert(kwaque::runtime::monotonic_clock<monotonic_clock>);
static_assert(kwaque::runtime::wall_clock<wall_clock>);

} // namespace kwaque::runtime::production

#endif // KWAQUE_SRC_RUNTIME_PRODUCTION_CLOCKS_H_
