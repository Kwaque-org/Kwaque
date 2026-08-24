#include "src/runtime/runtime_service.h"
#include "src/runtime/sharded_service.h"
#include "src/runtime/stop_signal.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <memory>
#include <stdexcept>
#include <utility>

static_assert(
  !noexcept(std::declval<kwaque::runtime::runtime_service&>().request_abort()));

SEASTAR_TEST_CASE(runtime_service_reports_every_shard_ready) {
    kwaque::runtime::sharded_service<kwaque::runtime::runtime_service> services{
      seastar::default_smp_service_group()};

    co_await services.start();
    const auto owners = co_await services.invoke_on_all(
      [](kwaque::runtime::runtime_service& service) {
          if (!service.ready() || service.shard() != seastar::this_shard_id()) {
              throw std::logic_error("runtime service has the wrong owner");
          }
          return service.owner();
      });

    BOOST_CHECK_EQUAL(owners.size(), seastar::this_smp_shard_count());
    co_await services.request_abort();
    co_await services.invoke_on_all(
      [](kwaque::runtime::runtime_service& service) {
          if (!service.abort_requested()) {
              throw std::logic_error("runtime service was not aborted");
          }
      });
    co_await services.stop();
    co_await services.stop();
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
