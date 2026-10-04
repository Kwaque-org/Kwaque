#include "src/config/bootstrap_config.h"

#include <cstddef>
#include <cstdint>
#include <string_view>

extern "C" int
LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    // Inputs past the cap exercise its rejection without parsing.
    if (size > kwaque::config::max_bootstrap_config_bytes + 1) {
        return 0;
    }

    const auto input = std::string_view(
      reinterpret_cast<const char*>(data), size);
    static_cast<void>(kwaque::config::parse_bootstrap_config(input));
    return 0;
}
