#include "src/runtime/testing/seastar_fuzz.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/test_runner.hh>

#include <array>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <pthread.h>
#include <string>
#include <utility>

namespace kwaque::runtime::testing {

namespace {

struct signal_handler final {
    int signal;
    struct sigaction action{};
};

std::array<signal_handler, 5>& crash_handlers() {
    static std::array<signal_handler, 5> handlers{
      signal_handler{.signal = SIGILL},
      signal_handler{.signal = SIGABRT},
      signal_handler{.signal = SIGSEGV},
      signal_handler{.signal = SIGBUS},
      signal_handler{.signal = SIGFPE},
    };
    return handlers;
}

[[noreturn]] void fail_bridge(const char* reason) noexcept {
    std::fprintf(stderr, "fuzz reactor bridge failure: %s\n", reason);
    std::fflush(stderr);
    std::abort();
}

void save_crash_handlers() noexcept {
    for (auto& handler : crash_handlers()) {
        if (::sigaction(handler.signal, nullptr, &handler.action) != 0) {
            fail_bridge("cannot inspect crash handler");
        }
    }
}

void restore_crash_handlers() noexcept {
    for (const auto& handler : crash_handlers()) {
        if (::sigaction(handler.signal, &handler.action, nullptr) != 0) {
            fail_bridge("cannot restore crash handler");
        }
    }
}

void unblock_fuzzer_signals() noexcept {
    sigset_t signals;
    sigemptyset(&signals);
    for (const auto& handler : crash_handlers()) {
        sigaddset(&signals, handler.signal);
    }
    for (const int signal : {SIGTRAP, SIGXFSZ, SIGUSR1, SIGUSR2}) {
        sigaddset(&signals, signal);
    }
    if (::pthread_sigmask(SIG_UNBLOCK, &signals, nullptr) != 0) {
        fail_bridge("cannot unblock fuzz signals");
    }
}

std::string reactor_backend_argument() {
    // The test macros export the reactor backend selected for this build.
    // Left to itself the reactor picks linux-aio wherever it works, whatever
    // the build selected.
    const char* backend = std::getenv("KWAQUE_REACTOR_BACKEND");
    return std::string{"--reactor-backend="}
           + (backend != nullptr ? backend : "epoll");
}

void ensure_runner_started() {
    static std::once_flag once;
    std::call_once(once, [] {
        save_crash_handlers();
        // A linux-aio shard is bounded as the test macros bound it: a fuzz
        // input opens no socket, and ten thousand control blocks a process
        // are a sixth of what a host allows all of its processes together.
        static std::array<std::string, 5> arguments{
          "kwaque-fuzz",
          "-c1",
          "--overprovisioned",
          reactor_backend_argument(),
          "--max-networking-io-control-blocks=1000"};
        static std::array<char*, 5> argument_pointers{
          arguments[0].data(),
          arguments[1].data(),
          arguments[2].data(),
          arguments[3].data(),
          arguments[4].data()};
        if (!seastar::testing::global_test_runner().start(
              static_cast<int>(argument_pointers.size()),
              argument_pointers.data())) {
            fail_bridge("cannot configure reactor runner");
        }
        if (
          std::atexit([] {
              const auto status
                = seastar::testing::global_test_runner().finalize();
              if (status != 0) {
                  std::fprintf(
                    stderr, "fuzz reactor shutdown failed: %d\n", status);
                  std::fflush(stderr);
                  // The reactor has already joined. An exit callback cannot
                  // return a process status, and must not recursively exit.
                  std::_Exit(
                    status > 0 && status <= 255 ? status : EXIT_FAILURE);
              }
          })
          != 0) {
            fail_bridge("cannot register reactor shutdown");
        }
    });
}

} // namespace

void run_fuzz_input(std::function<void()> function) {
    ensure_runner_started();
    bool completed = false;
    seastar::testing::global_test_runner().run_sync(
      [function = std::move(function),
       &completed] mutable -> seastar::future<> {
          static std::once_flag once;
          std::call_once(once, restore_crash_handlers);
          unblock_fuzzer_signals();
          return seastar::async([function = std::move(function), &completed] {
              function();
              completed = true;
          });
      });
    // Native run_sync returns without invoking its task after startup failure.
    // Its synchronous exchanger joins every executed task, including this
    // acknowledgement, before the caller may read or destroy the stack value.
    if (!completed) {
        fail_bridge("reactor did not execute the input");
    }
}

} // namespace kwaque::runtime::testing
