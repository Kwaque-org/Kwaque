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
This file includes known-answer test material from Random123:
https://github.com/DEShawResearch/random123
*/

#include "src/simulation/philox.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>

namespace {

struct known_answer final {
    kwaque::simulation::philox_counter counter;
    kwaque::simulation::philox_key key;
    kwaque::simulation::philox_counter expected;
};

constexpr std::array known_answers{
  known_answer{
    .counter = {0, 0, 0, 0},
    .key = {0, 0},
    .expected = {
      UINT32_C(0x6627e8d5),
      UINT32_C(0xe169c58d),
      UINT32_C(0xbc57ac4c),
      UINT32_C(0x9b00dbd8),
    },
  },
  known_answer{
    .counter = {
      UINT32_MAX,
      UINT32_MAX,
      UINT32_MAX,
      UINT32_MAX,
    },
    .key = {UINT32_MAX, UINT32_MAX},
    .expected = {
      UINT32_C(0x408f276d),
      UINT32_C(0x41c83b0e),
      UINT32_C(0xa20bc7c6),
      UINT32_C(0x6d5451fd),
    },
  },
  known_answer{
    .counter = {
      UINT32_C(0x243f6a88),
      UINT32_C(0x85a308d3),
      UINT32_C(0x13198a2e),
      UINT32_C(0x03707344),
    },
    .key = {UINT32_C(0xa4093822), UINT32_C(0x299f31d0)},
    .expected = {
      UINT32_C(0xd16cfe09),
      UINT32_C(0x94fdcceb),
      UINT32_C(0x5001e420),
      UINT32_C(0x24126ea1),
    },
  },
};

static_assert(
  kwaque::simulation::philox4x32_10(
    known_answers[0].counter, known_answers[0].key)
  == known_answers[0].expected);

} // namespace

TEST(PhiloxTest, MatchesEverySelectedTenRoundKnownAnswer) {
    for (const auto& test_case : known_answers) {
        EXPECT_EQ(
          kwaque::simulation::philox4x32_10(test_case.counter, test_case.key),
          test_case.expected);
    }
}
