#include "src/base/units.h"

#include <cstddef>
#include <cstdint>

using kwaque::literals::operator""_KiB;

extern "C" int
LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    constexpr std::size_t max_input_size = 4_KiB;
    if (size > max_input_size) {
        return 0;
    }

    std::uint8_t checksum = 0;
    for (std::size_t index = 0; index < size; ++index) {
        checksum ^= data[index];
    }
    (void)checksum;
    return 0;
}
