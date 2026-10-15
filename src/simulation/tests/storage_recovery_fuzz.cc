#include "src/runtime/testing/seastar_fuzz.h"
#include "src/simulation/tests/storage_recovery_fuzz_cases.h"

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

extern "C" int
LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    if (size > kwaque::simulation::testing::storage_recovery_fuzz_max_input)
        return 0;
    std::vector<std::uint8_t> owned;
    if (size != 0) owned.assign(data, data + size);
    kwaque::runtime::testing::run_fuzz_input([owned = std::move(owned)] {
        kwaque::simulation::testing::fuzz_recovery_case(owned);
    });
    return 0;
}
