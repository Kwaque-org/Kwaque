#include "src/simulation/tests/fuzz_cases.h"

#include <cstddef>
#include <cstdint>

extern "C" int
LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    kwaque::simulation::testing::run_fuzz_case(
      kwaque::simulation::testing::fuzz_harness::fake_file, data, size);
    return 0;
}
