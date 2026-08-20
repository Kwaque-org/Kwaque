#include "src/config/bootstrap_config.h"

#include <cstddef>
#include <cstdint>
#include <string_view>

extern "C" int
LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    constexpr std::size_t max_input_size = 4096;
    if (size > max_input_size) {
        return 0;
    }

    const auto input = std::string_view(
      reinterpret_cast<const char*>(data), size);
    static_cast<void>(kwaque::config::parse_bootstrap_config(input));
    return 0;
}
