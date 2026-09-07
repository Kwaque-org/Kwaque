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

void ensure_runner_started() {
    static std::once_flag once;
    std::call_once(once, [] {
        save_crash_handlers();
        static std::array<std::string, 3> arguments{
          "kwaque-fuzz", "-c1", "--overprovisioned"};
        static std::array<char*, 3> argument_pointers{
          arguments[0].data(), arguments[1].data(), arguments[2].data()};
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
