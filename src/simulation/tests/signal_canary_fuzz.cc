#include "src/runtime/testing/seastar_fuzz.h"

#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdio>

extern "C" int
LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    if (
      size < 4 || data[0] != 'K' || data[1] != 'Q' || data[2] != 'S'
      || data[3] != '1') {
        return 0;
    }
    kwaque::runtime::testing::run_fuzz_input([] {
        std::fputs(
          "KQFUZZ HARNESS=signal_canary VERSION=1 INPUT=4b515331\n", stderr);
        std::fflush(stderr);
        std::raise(SIGABRT);
    });
    return 0;
}
