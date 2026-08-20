#include "src/runtime/runtime_service.h"
#include "src/runtime/stop_signal.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sharded.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <atomic>
#include <functional>
#include <memory>

namespace {

class abort_source_service final {
public:
    [[nodiscard]] seastar::abort_source& get() noexcept { return source_; }
    [[nodiscard]] seastar::future<> stop() {
        if (!source_.abort_requested()) {
            source_.request_abort();
        }
        return seastar::make_ready_future<>();
    }

private:
    seastar::abort_source source_;
};

} // namespace

SEASTAR_TEST_CASE(runtime_service_reports_every_shard_ready) {
    seastar::sharded<abort_source_service> abort_sources;
    seastar::sharded<kwaque::runtime::runtime_service> services;
    std::atomic<unsigned> ready_count{0};

    co_await abort_sources.start();
    auto source = seastar::sharded_parameter(
      [](abort_source_service& local) { return std::ref(local.get()); },
      std::ref(abort_sources));
    co_await services.start(std::move(source));
    co_await services.invoke_on_all(
      [&ready_count](kwaque::runtime::runtime_service& service) {
          return service.start().then([&ready_count, &service] {
              BOOST_CHECK(service.ready());
              BOOST_CHECK_EQUAL(service.shard(), seastar::this_shard_id());
              ready_count.fetch_add(1, std::memory_order_relaxed);
          });
      });

    BOOST_CHECK_EQUAL(
      ready_count.load(std::memory_order_relaxed),
      seastar::this_smp_shard_count());
    co_await abort_sources.invoke_on_all(
      [](abort_source_service& local) { local.get().request_abort(); });
    co_await services.invoke_on_all(
      [](kwaque::runtime::runtime_service& service) {
          BOOST_CHECK(service.abort_requested());
      });
    co_await services.stop();
    co_await abort_sources.stop();
}

SEASTAR_TEST_CASE(stop_signal_requests_abort_once) {
    kwaque::runtime::stop_signal signal(false);
    unsigned callbacks = 0;
    auto subscription = signal.abort_source().subscribe(
      [&callbacks] noexcept { ++callbacks; });

    signal.request_stop();
    signal.request_stop();
    co_await signal.wait();

    BOOST_CHECK(signal.stopping());
    BOOST_CHECK_EQUAL(callbacks, 1U);
    BOOST_CHECK(!subscription);
}

SEASTAR_TEST_CASE(stop_signal_destructor_requests_abort) {
    auto signal = std::make_unique<kwaque::runtime::stop_signal>(false);
    unsigned callbacks = 0;
    auto subscription = signal->abort_source().subscribe(
      [&callbacks] noexcept { ++callbacks; });

    signal.reset();

    BOOST_CHECK_EQUAL(callbacks, 1U);
    BOOST_CHECK(!subscription);
    co_return;
}
