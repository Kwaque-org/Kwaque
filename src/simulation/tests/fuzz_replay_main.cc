#include "src/runtime/error.h"
#include "src/runtime/testing/seastar_fuzz.h"
#include "src/simulation/tests/fuzz_cases.h"
#include "src/simulation/tests/fuzz_reproduction.h"

#include <seastar/util/defer.hh>

#include <openssl/crypto.h>

#include <iostream>

int main() {
    auto replayed = kwaque::runtime::result<void>{kwaque::runtime::failure(
      kwaque::runtime::operation_error{
        kwaque::errc::unavailable, kwaque::runtime::operation_kind::runtime})};
    int exit_code = 1;
    kwaque::runtime::testing::run_fuzz_input([&] {
        // Release this worker's crypto state before process-wide cleanup runs.
        auto cleanup = seastar::defer([] { OPENSSL_thread_stop(); });
        auto reproduction = kwaque::simulation::testing::read_fuzz_reproduction(
          std::cin);
        if (!reproduction) {
            exit_code = 2;
            return;
        }
        replayed = kwaque::simulation::testing::replay_fuzz_case(*reproduction);
        exit_code = replayed ? 0 : 1;
    });
    if (exit_code == 1 && !replayed) {
        std::cerr << replayed.error().render() << '\n';
    }
    return exit_code;
}
