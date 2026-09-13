#include "src/base/units.h"
#include "src/resource/resource_config.h"
#include "src/resource/resource_manager.h"
#include "src/resource/resource_registry.h"
#include "src/runtime/sharded_service.h"
#include "src/runtime/task_scope.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/when_any.hh>

#include <sys/resource.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <functional>
#include <memory>
#include <string_view>
#include <tuple>
#include <utility>

namespace {

class pending_service {
public:
    explicit pending_service(
      std::reference_wrapper<seastar::promise<>> entered) noexcept
      : entered_(entered) {}

    seastar::future<> start() {
        auto pending = release_.get_future();
        // The notification belongs to the probe's coordinator shard. Await
        // its delivery, then remain suspended on the service-owned promise.
        co_await seastar::smp::submit_to(
          0, [entered = entered_] { entered.get().set_value(); });
        co_await std::move(pending);
    }
    void request_abort() {}
    seastar::future<> stop() { return seastar::make_ready_future<>(); }

private:
    std::reference_wrapper<seastar::promise<>> entered_;
    seastar::promise<> release_;
};

seastar::future<int> run_probe(std::string_view scenario) {
    if (seastar::this_smp_shard_count() < 2) {
        co_return 80;
    }
    if (scenario == "scope-foreign") {
        auto scope = std::make_unique<kwaque::runtime::task_scope>();
        co_await scope->close();
        co_await seastar::smp::submit_to(
          1, [scope = std::move(scope)] mutable { scope.reset(); });
    } else if (
      scenario == "service-foreign" || scenario == "service-starting") {
        auto service
          = std::make_unique<kwaque::runtime::sharded_service<pending_service>>(
            seastar::default_smp_service_group());
        if (scenario == "service-starting") {
            seastar::promise<> entered;
            auto notification = entered.get_future();
            auto starting = service->start(std::ref(entered));
            // Await an actual lifecycle event, not a number of reactor turns.
            // Retain both futures and detect unexpected startup completion.
            auto checkpoint = co_await seastar::when_any(
              std::move(notification), std::move(starting));
            auto& startup = std::get<1>(checkpoint.futures);
            if (checkpoint.index != 0 || startup.available()) {
                if (startup.available()) {
                    try {
                        startup.get();
                        std::fputs(
                          "probe: startup completed unexpectedly\n", stderr);
                    } catch (const std::exception& error) {
                        std::fprintf(
                          stderr,
                          "probe: startup failed: %.256s\n",
                          error.what());
                    } catch (...) {
                        std::fputs(
                          "probe: startup failed with an unknown exception\n",
                          stderr);
                    }
                }
                std::_Exit(81);
            }
            std::get<0>(checkpoint.futures).get();
            service.reset();
        } else {
            co_await service->stop();
            co_await seastar::smp::submit_to(
              1, [service = std::move(service)] mutable { service.reset(); });
        }
    } else if (scenario == "registry-foreign") {
        auto registry = std::make_unique<kwaque::resource::resource_registry>();
        co_await seastar::smp::submit_to(
          1, [registry = std::move(registry)] mutable { registry.reset(); });
    } else if (scenario == "manager-foreign") {
        const auto config
          = kwaque::resource::resource_config::from_total_memory(
            kwaque::byte_count{static_cast<std::uint64_t>(
              seastar::memory::stats().total_memory())});
        if (!config) {
            co_return 82;
        }
        kwaque::resource::resource_registry registry;
        co_await registry.start(*config);
        auto manager = std::make_unique<kwaque::resource::resource_manager>(
          registry.handles());
        co_await manager->start();
        co_await manager->stop();
        co_await seastar::smp::submit_to(
          1, [manager = std::move(manager)] mutable { manager.reset(); });
        co_await registry.stop();
    } else {
        co_return 83;
    }
    // A missed guard must not turn into a different destructor's failure and
    // accidentally satisfy the subprocess test.
    std::_Exit(84);
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        return 85;
    }
    const rlimit no_core{0, 0};
    if (::setrlimit(RLIMIT_CORE, &no_core) != 0) {
        return 86;
    }
    const std::string_view scenario{argv[1]};
    argv[1] = argv[0];
    seastar::app_template app;
    return app.run(
      argc - 1, argv + 1, [scenario] { return run_probe(scenario); });
}
