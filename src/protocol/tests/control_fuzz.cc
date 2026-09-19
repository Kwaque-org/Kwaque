#include "src/protocol/tests/control_fuzz_cases.h"
#include "src/runtime/testing/seastar_fuzz.h"

#include <seastar/util/defer.hh>

#include <openssl/crypto.h>

#include <utility>
#include <vector>

extern "C" int
LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    if (size > kwaque::protocol::testing::control_fuzz_max_input) return 0;
    std::vector<std::uint8_t> owned;
    if (size != 0) owned.assign(data, data + size);
    kwaque::runtime::testing::run_fuzz_input([owned = std::move(owned)] {
        auto cleanup = seastar::defer([] noexcept { OPENSSL_thread_stop(); });
        kwaque::protocol::testing::exercise_control_case(owned);
    });
    return 0;
}
