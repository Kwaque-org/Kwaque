#include "src/runtime/testing/seastar_fuzz.h"
#include "src/simulation/tests/fuzz_cases.h"

#include <array>
#include <cstdint>
#include <cstdlib>
#include <utility>

int main() {
    constexpr std::array<std::uint8_t, 12> input{
      0x51, 0x4b, 0x43, 0x41, 0x4e, 0x41, 0x52, 0x59, 1, 2, 3, 4};
    kwaque::runtime::testing::run_fuzz_input([input] {
        auto result = kwaque::simulation::testing::execute_fuzz_case(
          kwaque::simulation::testing::fuzz_harness::semantic_canary, input);
        if (!result) {
            std::abort();
        }
        kwaque::simulation::testing::report_fuzz_failure(std::move(*result));
    });
}
