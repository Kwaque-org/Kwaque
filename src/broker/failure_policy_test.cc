#include "src/base/error.h"
#include "src/base/invariant.h"
#include "src/base/units.h"
#include "src/broker/startup_policy.h"
#include "src/resource/bounded_work_queue.h"
#include "src/runtime/shard_affinity.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <boost/program_options.hpp>

#include <array>
#include <cstddef>
#include <cstdio>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace {

[[gnu::noinline]] void* allocate_block() {
    auto* block = ::operator new(64U * 1024U);
    static_cast<volatile char*>(block)[0] = 1;
    return block;
}

void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

seastar::future<int> exercise(std::string_view scenario) {
    const bool abort_enabled
      = seastar::memory::is_abort_on_allocation_failure();
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    constexpr bool native_allocator = false;
#else
    constexpr bool native_allocator = true;
#endif
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    constexpr bool injection_available = true;
#else
    constexpr bool injection_available = false;
#endif
    require(
      abort_enabled == native_allocator, "unexpected effective OOM policy");
    std::printf(
      "allocator=%s abort=%s injection=%s\n",
      native_allocator ? "native" : "system",
      abort_enabled ? "true" : "false",
      injection_available ? "true" : "false");
    std::fflush(stdout);

    if (scenario == "effective") {
        co_return 0;
    }
    if (scenario == "oom") {
        if (!native_allocator) {
            co_return 77;
        }
        require(
          seastar::memory::stats().total_memory() <= 64U * 1024U * 1024U,
          "OOM probe requires a heap no larger than 64 MiB");
        // Retain bounded blocks until the fixed native heap cannot satisfy one.
        // This branch never runs with the unbounded system allocator.
        std::array<void*, 2048> blocks{};
        for (auto& block : blocks) {
            block = allocate_block();
        }
        for (auto* block : blocks) {
            ::operator delete(block);
        }
        throw std::runtime_error("managed heap did not exhaust");
    }
    if (scenario == "injection") {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
        auto& injector = seastar::memory::local_failure_injector();
        injector.fail_after(0);
        void* block = nullptr;
        bool caught = false;
        try {
            block = allocate_block();
        } catch (const std::bad_alloc&) {
            caught = true;
        } catch (...) {
            injector.cancel();
            throw;
        }
        const bool injected = injector.failed();
        injector.cancel();
        ::operator delete(block);
        require(caught && injected, "allocation injection did not throw");
        require(
          seastar::memory::is_abort_on_allocation_failure(),
          "injection disabled real OOM abort");
        co_return 0;
#else
        co_return 77;
#endif
    }
    if (scenario == "invariant") {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
        seastar::memory::local_failure_injector().fail_after(0);
#endif
        kwaque::invariant_failed(
          kwaque::invariant_id{"KQ-FAILURE-POLICY-PROBE"},
          "false",
          "fatal diagnostic needs no allocation");
    }
    if (scenario == "wrong-shard") {
        require(seastar::this_smp_shard_count() == 2, "two shards required");
        kwaque::runtime::owner_shard owner;
        co_await seastar::smp::submit_to(
          1, [owner] { owner.assert_current(); });
        throw std::runtime_error("foreign shard access returned");
    }
    if (scenario == "admission") {
        using namespace kwaque;
        resource::bounded_work_queue<int> queue{
          resource::bounded_work_queue_config{
            .maximum_items = item_count{1},
            .maximum_bytes = byte_count{2},
            .maximum_producer_waiters = 0}};
        seastar::abort_source abort;
        const auto accepted = co_await queue.push(1, byte_count{2}, abort);
        const auto full = co_await queue.push(2, byte_count{1}, abort);
        const auto oversized = co_await queue.push(3, byte_count{3}, abort);
        co_await queue.close(resource::queue_close_mode::abort);
        require(accepted.has_value(), "initial admission failed");
        require(
          !full && full.error().code() == errc::resource_exhausted,
          "full queue did not reject with a typed resource error");
        require(
          !oversized && oversized.error().code() == errc::out_of_range,
          "oversized item did not reject with a typed size error");
        require(
          seastar::memory::is_abort_on_allocation_failure() == abort_enabled,
          "admission changed the OOM policy");
        co_return 0;
    }
    throw std::runtime_error("unknown failure-policy scenario");
}

} // namespace

int main(int argc, char** argv) {
    seastar::app_template::seastar_options options;
    kwaque::broker::detail::configure_allocation_failure_policy(options);
    seastar::app_template app(std::move(options));
    app.set_configuration_reader(
      kwaque::broker::detail::validate_runtime_configuration);
    app.add_options()(
      "scenario", boost::program_options::value<std::string>()->required());
    return app.run(argc, argv, [&app] {
        return exercise(app.configuration()["scenario"].as<std::string>());
    });
}
