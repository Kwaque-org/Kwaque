/*
Copyright 2010-2011, D. E. Shaw Research.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above copyright
  notice, this list of conditions, and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions, and the following disclaimer in the
  documentation and/or other materials provided with the distribution.

* Neither the name of D. E. Shaw Research nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

/*
This file includes code from Random123:
https://github.com/DEShawResearch/random123
*/

#ifndef KWAQUE_SRC_SIMULATION_PHILOX_H_
#define KWAQUE_SRC_SIMULATION_PHILOX_H_

#include <array>
#include <cstdint>

namespace kwaque::simulation {

using philox_counter = std::array<std::uint32_t, 4>;
using philox_key = std::array<std::uint32_t, 2>;

namespace detail {

inline constexpr std::uint32_t philox_multiplier_0{UINT32_C(0xd2511f53)};
inline constexpr std::uint32_t philox_multiplier_1{UINT32_C(0xcd9e8d57)};
inline constexpr std::uint32_t philox_weyl_0{UINT32_C(0x9e3779b9)};
inline constexpr std::uint32_t philox_weyl_1{UINT32_C(0xbb67ae85)};

[[nodiscard]] constexpr philox_counter
philox_round(philox_counter counter, philox_key key) noexcept {
    const auto product_0 = static_cast<std::uint64_t>(philox_multiplier_0)
                           * counter[0];
    const auto product_1 = static_cast<std::uint64_t>(philox_multiplier_1)
                           * counter[2];
    return philox_counter{
      static_cast<std::uint32_t>(product_1 >> 32U) ^ counter[1] ^ key[0],
      static_cast<std::uint32_t>(product_1),
      static_cast<std::uint32_t>(product_0 >> 32U) ^ counter[3] ^ key[1],
      static_cast<std::uint32_t>(product_0),
    };
}

[[nodiscard]] constexpr philox_key philox_bump_key(philox_key key) noexcept {
    key[0] += philox_weyl_0;
    key[1] += philox_weyl_1;
    return key;
}

} // namespace detail

[[nodiscard]] constexpr philox_counter
philox4x32_10(philox_counter counter, philox_key key) noexcept {
    counter = detail::philox_round(counter, key);
    key = detail::philox_bump_key(key);
    counter = detail::philox_round(counter, key);
    key = detail::philox_bump_key(key);
    counter = detail::philox_round(counter, key);
    key = detail::philox_bump_key(key);
    counter = detail::philox_round(counter, key);
    key = detail::philox_bump_key(key);
    counter = detail::philox_round(counter, key);
    key = detail::philox_bump_key(key);
    counter = detail::philox_round(counter, key);
    key = detail::philox_bump_key(key);
    counter = detail::philox_round(counter, key);
    key = detail::philox_bump_key(key);
    counter = detail::philox_round(counter, key);
    key = detail::philox_bump_key(key);
    counter = detail::philox_round(counter, key);
    key = detail::philox_bump_key(key);
    return detail::philox_round(counter, key);
}

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_PHILOX_H_
