#include "src/base/units.h"
#include "src/resource/bounded_work_queue.h"
#include "src/resource/resource_config.h"
#include "src/resource/resource_manager.h"
#include "src/resource/resource_registry.h"
#include "src/resource/workload_class.h"
#include "src/runtime/task_scope.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/later.hh>

#include <sys/resource.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace {

class optional_item_failure final : public std::exception {};

seastar::future<> fail_after_suspension() {
    co_await seastar::yield();
    throw std::logic_error("unexpected suspended item failure");
}

seastar::future<int> observe_scope_failure(std::string_view scenario) {
    kwaque::runtime::task_scope::failure_notifier notifier;
    if (scenario == "scope-notifier-throw") {
        notifier = [](std::exception_ptr) {
            throw std::logic_error("notification failure");
        };
    }
    kwaque::runtime::task_scope scope{std::move(notifier)};
    if (scenario == "scope-finite-failure-first") {
        static_cast<void>(scope.spawn([] {
            return seastar::make_exception_future<>(
              std::runtime_error("finite task failure"));
        }));
        while (scope.statistics().completed == 0) {
            co_await seastar::yield();
        }
        if (scope.statistics().failed != 1) {
            std::_Exit(88);
        }
    }
    const auto accepted = scope.spawn(
      [scenario] {
          if (
            scenario == "scope-unrequested-abort"
            || scenario == "scope-finite-failure-first") {
              return seastar::make_exception_future<>(
                seastar::abort_requested_exception{});
          }
          if (scenario == "scope-notifier-throw") {
              return seastar::make_exception_future<>(
                std::runtime_error("task failure"));
          }
          return seastar::make_ready_future<>();
      },
      kwaque::runtime::task_lifetime::until_abort);
    if (!accepted) {
        std::_Exit(80);
    }
    co_await seastar::sleep(std::chrono::seconds{1});
    // This exit deliberately bypasses teardown: only a failure observed before
    // close can produce the fatal status and diagnostic asserted by the test.
    std::_Exit(81);
}

seastar::future<int> observe_queue_failure(std::string_view scenario) {
    using namespace kwaque;
    using namespace kwaque::resource;

    auto config = resource_config::from_total_memory(
      byte_count{
        static_cast<std::uint64_t>(seastar::memory::stats().total_memory())});
    if (!config) {
        co_return 82;
    }
    resource_registry registry;
    co_await registry.start(*config);
    resource_manager manager{registry.handles()};
    co_await manager.start();
    bounded_work_queue<int> queue{
      bounded_work_queue_config{
        .maximum_items = item_count{3},
        .maximum_bytes = byte_count{32},
      },
      manager,
      workload_class::maintenance};
    seastar::abort_source admission_abort;
    for (int item = 1; item <= 3; ++item) {
        if (!(co_await queue.push(item, byte_count{1}, admission_abort))) {
            std::_Exit(83);
        }
    }

    std::uint64_t reported = 0;
    std::uint64_t classified = 0;
    seastar::promise<> processed;
    auto processing_done = processed.get_future();
    queue.start_workers(
      bounded_queue_worker_config{
        .workers = 1,
        .maximum_error_reports = scenario == "queue-disabled-reports" ? 0U : 1U,
      },
      [scenario](int item) -> seastar::future<> {
          if (scenario == "queue-unknown-suspended") {
              return fail_after_suspension();
          }
          if (
            scenario == "queue-unknown-ready"
            || scenario == "queue-disabled-reports"
            || (scenario == "queue-exhausted-reports" && item == 3)) {
              throw std::logic_error("unexpected item failure");
          }
          throw optional_item_failure{};
      },
      [scenario, &reported](std::exception_ptr) {
          if (scenario == "queue-reporter-throw") {
              throw std::logic_error("reporter failure");
          }
          ++reported;
      },
      [scenario, &classified, &processed](const std::exception_ptr& failure) {
          if (scenario == "queue-classifier-throw") {
              throw std::logic_error("classifier failure");
          }
          try {
              std::rethrow_exception(failure);
          } catch (const optional_item_failure&) {
              ++classified;
              if (classified == 3) {
                  processed.set_value();
              }
              return true;
          } catch (...) {
              return false;
          }
      });
    if (scenario != "queue-optional-limited") {
        co_await seastar::sleep(std::chrono::seconds{1});
        std::_Exit(84);
    }

    co_await std::move(processing_done);
    co_await queue.close(queue_close_mode::drain);
    const bool limits_preserved = reported == 1 && queue.reported_errors() == 1
                                  && queue.suppressed_errors() == 2
                                  && queue.bytes().value() == 0;
    co_await manager.stop();
    co_await registry.stop();
    co_return limits_preserved ? 0 : 85;
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        return 86;
    }
    const rlimit no_core{0, 0};
    if (::setrlimit(RLIMIT_CORE, &no_core) != 0) {
        return 87;
    }
    const std::string_view scenario{argv[1]};
    argv[1] = argv[0];
    seastar::app_template app;
    return app.run(argc - 1, argv + 1, [scenario] {
        return scenario.starts_with("scope-") ? observe_scope_failure(scenario)
                                              : observe_queue_failure(scenario);
    });
}
