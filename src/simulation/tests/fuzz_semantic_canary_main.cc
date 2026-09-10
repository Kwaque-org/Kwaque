#include "src/runtime/testing/seastar_fuzz.h"
#include "src/simulation/tests/fuzz_cases.h"

#include <sys/prctl.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <utility>

int main() {
    // The expected crash is captured by the reproduction envelope. Disable
    // kernel core collection, including piped collectors that ignore
    // RLIMIT_CORE.
    if (::prctl(PR_SET_DUMPABLE, 0UL, 0UL, 0UL, 0UL) != 0) {
        std::fputs("fuzz canary: cannot disable core dumps\n", stderr);
        return EXIT_FAILURE;
    }
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
