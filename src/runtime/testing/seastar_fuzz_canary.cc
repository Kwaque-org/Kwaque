#include "src/runtime/testing/seastar_fuzz.h"

#include <seastar/core/future.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_runner.hh>

#include <array>
#include <cstdio>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {

void require(bool condition) {
    if (!condition) {
        std::abort();
    }
}

void configure_native_runner(bool fail_startup) {
    // The native runner owns only pointers to these arguments until lazy
    // startup. Keep them alive through every bridge call and finalization.
    static std::array<std::string, 7> arguments{
      "kwaque-fuzz-canary",
      "--smp=1",
      "--memory=128MiB",
      "--reactor-backend=epoll",
      "--overprovisioned",
      "--random-seed=1",
      "--invalid-canary-option",
    };
    static std::array<char*, arguments.size()> pointers{
      arguments[0].data(),
      arguments[1].data(),
      arguments[2].data(),
      arguments[3].data(),
      arguments[4].data(),
      arguments[5].data(),
      arguments[6].data(),
    };
    require(
      seastar::testing::global_test_runner().start(
        static_cast<int>(pointers.size() - (fail_startup ? 0U : 1U)),
        pointers.data()));
}

void input_marker() {
    std::fputs("CANARY input completed\n", stderr);
    std::fflush(stderr);
}

} // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        return 2;
    }
    const std::string_view mode{argv[1]};
    configure_native_runner(mode == "startup_failure");

    if (mode == "skipped_callback") {
        // Stop the same successfully started runner. Its next run_sync is a
        // documented no-op even though finalization itself was successful.
        seastar::testing::global_test_runner().run_sync(
          [] { return seastar::make_ready_future<>(); });
        require(seastar::testing::global_test_runner().finalize() == 0);
    }

    if (mode == "startup_failure" || mode == "skipped_callback") {
        kwaque::runtime::testing::run_fuzz_input(input_marker);
        std::fputs("CANARY incorrectly accepted skipped input\n", stderr);
        return 0;
    }
    if (mode == "finalize_failure") {
        kwaque::runtime::testing::run_fuzz_input([] {
            // The actual native runner reports this abandoned failure only
            // when its reactor exits. The bridge must retain that exit code.
            seastar::promise<> abandoned;
            static_cast<void>(abandoned.get_future());
            abandoned.set_exception(
              std::runtime_error("intentional canary future failure"));
            input_marker();
        });
        std::fputs("CANARY main returned success\n", stderr);
        std::fflush(stderr);
        return 0;
    }
    if (mode == "body_exception") {
        bool caught = false;
        try {
            kwaque::runtime::testing::run_fuzz_input([] {
                throw std::runtime_error("intentional canary body failure");
            });
        } catch (const std::runtime_error& error) {
            caught = std::string_view{error.what()}
                     == "intentional canary body failure";
        }
        require(caught);
        kwaque::runtime::testing::run_fuzz_input(input_marker);
        return 0;
    }
    if (mode != "normal") {
        return 2;
    }
    unsigned completed = 0;
    for (unsigned invocation = 0; invocation < 2; ++invocation) {
        kwaque::runtime::testing::run_fuzz_input([&completed] {
            require(seastar::thread::running_in_thread());
            require(seastar::make_ready_future<unsigned>(17).get() == 17);
            ++completed;
            input_marker();
        });
        require(completed == invocation + 1U);
    }
    return 0;
}
