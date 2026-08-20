#include "proto/kwaque/common/v1/build_info_codec.h"

#include <cstddef>
#include <cstdint>
#include <string_view>

extern "C" int
LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    const auto input = std::string_view(
      reinterpret_cast<const char*>(data), size);
    static_cast<void>(kwaque::common::v1::parse_build_info(input));
    return 0;
}
