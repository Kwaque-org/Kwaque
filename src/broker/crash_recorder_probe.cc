#include "src/base/invariant.h"
#include "src/broker/crash_recorder.h"
#include "src/broker/startup_policy.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/later.hh>

#include <boost/program_options.hpp>

#include <array>
#include <atomic>
#include <csignal>
#include <cstdio>
#include <filesystem>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unistd.h>
#include <utility>

namespace {

std::atomic<unsigned> arrived{0};

void prior_handler(int, siginfo_t*, void*) {
    constexpr std::string_view marker = "prior fatal handler\n";
    static_cast<void>(::write(STDERR_FILENO, marker.data(), marker.size()));
    ::_exit(73);
}

void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

[[gnu::noinline]] void* allocate_block() {
    auto* block = ::operator new(64U * 1024U);
    static_cast<volatile char*>(block)[0] = 1;
    return block;
}

seastar::future<int>
exercise(std::string_view scenario, std::filesystem::path path) {
#ifdef SEASTAR_ASAN_ENABLED
    constexpr bool asan = true;
#else
    constexpr bool asan = false;
#endif
#ifdef SEASTAR_DEFAULT_ALLOCATOR
    constexpr bool system_allocator = true;
#else
    constexpr bool system_allocator = false;
#endif
#ifdef SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION
    constexpr bool injection = true;
#else
    constexpr bool injection = false;
#endif
    std::printf(
      "asan=%s allocator=%s injection=%s\n",
      asan ? "true" : "false",
      system_allocator ? "system" : "native",
      injection ? "true" : "false");
    std::fflush(stdout);
    if (scenario == "preinit") {
        ::raise(SIGABRT);
        co_return 90;
    }
    if (scenario == "prior") {
        struct sigaction action{};
        action.sa_sigaction = prior_handler;
        action.sa_flags = SA_SIGINFO | SA_RESTART;
        ::sigfillset(&action.sa_mask);
        require(
          ::sigaction(SIGILL, &action, nullptr) == 0, "install prior handler");
    }
    struct sigaction original_segv{};
    require(
      ::sigaction(SIGSEGV, nullptr, &original_segv) == 0,
      "query original segv handler");
    kwaque::broker::crash_recorder recorder;
    seastar::abort_source abort;
    co_await recorder.start(path, abort);
    struct sigaction current_segv{};
    require(
      ::sigaction(SIGSEGV, nullptr, &current_segv) == 0,
      "query crash segv handler");
    if (asan) {
        require(
          current_segv.sa_sigaction == original_segv.sa_sigaction
            && current_segv.sa_flags == original_segv.sa_flags,
          "sanitizer segv handler changed");
    } else {
        require(
          (current_segv.sa_flags & SA_ONSTACK) != 0,
          "segv wrapper lost alternate stack");
    }
    if (scenario == "clean" || scenario == "capabilities") {
        co_await recorder.stop();
        struct sigaction restored{};
        require(
          ::sigaction(SIGSEGV, nullptr, &restored) == 0,
          "query restored segv handler");
        require(
          restored.sa_sigaction == original_segv.sa_sigaction,
          "segv handler not restored");
        co_return 0;
    }
    if (scenario == "startup") {
        // The diagnostic deliberately receives no arbitrary exception text.
        const auto failure = std::make_exception_ptr(
          std::runtime_error("secret-token-never-record"));
        static_cast<void>(failure);
        recorder.record_startup_failure();
        co_await recorder.stop();
        co_return 1;
    }
    if (scenario == "concurrent") {
        require(
          seastar::this_smp_shard_count() == 2,
          "concurrent crash needs two shards");
        co_await seastar::smp::invoke_on_all([] -> seastar::future<> {
            arrived.fetch_add(1);
            while (arrived.load() != 2) {
                // Local invocation can run inline before remote dispatch.
                // Suspend and poll so both shards reach the barrier.
                co_await seastar::check_for_io_immediately();
            }
            ::raise(seastar::this_shard_id() == 0 ? SIGABRT : SIGILL);
        });
        co_return 91;
    }
    if (scenario == "allocation") {
#ifdef SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION
        seastar::memory::local_failure_injector().fail_after(0);
        ::raise(SIGABRT);
        co_return 92;
#else
        co_await recorder.stop();
        co_return 77;
#endif
    }
    if (scenario == "oom") {
        if (system_allocator) {
            co_await recorder.stop();
            co_return 77;
        }
        require(
          seastar::memory::is_abort_on_allocation_failure(), "OOM must abort");
        require(
          seastar::this_smp_shard_count() == 1
            && seastar::memory::stats().total_memory() <= 96U * 1024U * 1024U,
          "OOM probe requires one bounded managed heap");
        std::array<void*, 2048> blocks{};
        for (auto& block : blocks) {
            block = allocate_block();
        }
        for (auto* block : blocks) {
            ::operator delete(block);
        }
        throw std::runtime_error("managed heap did not exhaust");
    }
    if (scenario == "invariant") {
        kwaque::invariant_failed(
          kwaque::invariant_id{"KQ-CRASH-RECORDER-PROBE"},
          "false",
          "fatal recording probe");
    }
    if (scenario == "abrt") {
        ::raise(SIGABRT);
    } else if (scenario == "ill" || scenario == "prior") {
        ::raise(SIGILL);
    } else if (scenario == "segv") {
        ::raise(SIGSEGV);
    } else {
        throw std::runtime_error("unknown crash recorder probe scenario");
    }
    co_return 93;
}

} // namespace

int main(int argc, char** argv) {
    seastar::app_template::seastar_options options;
    kwaque::broker::detail::configure_allocation_failure_policy(options);
    seastar::app_template app(std::move(options));
    app.add_options()(
      "scenario", boost::program_options::value<std::string>()->required())(
      "directory", boost::program_options::value<std::string>()->required());
    return app.run(argc, argv, [&app] {
        return exercise(
          app.configuration()["scenario"].as<std::string>(),
          std::filesystem::path{
            app.configuration()["directory"].as<std::string>()});
    });
}
