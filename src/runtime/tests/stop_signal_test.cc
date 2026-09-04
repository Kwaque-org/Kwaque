#include "src/runtime/stop_signal.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <memory>

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
