#include "src/broker/application_internal.h"
#include "src/broker/service_lifecycle.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/reactor.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <stdexcept>
#include <string>
#include <vector>

namespace {

seastar::future<> ready() { return seastar::make_ready_future<>(); }

} // namespace

SEASTAR_TEST_CASE(lifecycle_starts_in_order_and_stops_in_reverse) {
  seastar::abort_source abort_source;
  kwaque::broker::service_lifecycle lifecycle(abort_source);
  std::vector<std::string> events;

  co_await lifecycle.start_step(
      "first",
      [&events] {
        events.emplace_back("start:first");
        return ready();
      },
      [&events] {
        events.emplace_back("stop:first");
        return ready();
      });
  co_await lifecycle.start_step(
      "second",
      [&events] {
        events.emplace_back("start:second");
        return ready();
      },
      [&events] {
        events.emplace_back("stop:second");
        return ready();
      });
  co_await lifecycle.stop();

  const std::vector<std::string> expected{
      "start:first", "start:second", "stop:second", "stop:first"};
  BOOST_CHECK(events == expected);
  BOOST_CHECK_EQUAL(lifecycle.running_steps(), 0U);
}

SEASTAR_TEST_CASE(lifecycle_rolls_back_after_start_failure) {
  seastar::abort_source abort_source;
  kwaque::broker::service_lifecycle lifecycle(abort_source);
  std::vector<std::string> events;

  co_await lifecycle.start_step(
      "first",
      [&events] {
        events.emplace_back("start:first");
        return ready();
      },
      [&events] {
        events.emplace_back("stop:first");
        return ready();
      });

  bool failed = false;
  try {
    co_await lifecycle.start_step(
        "second",
        [&events] {
          events.emplace_back("start:second");
          return seastar::make_exception_future<>(
              std::runtime_error("injected startup failure"));
        },
        [&events] {
          events.emplace_back("stop:second");
          return ready();
        });
  } catch (const std::runtime_error &) {
    failed = true;
  }

  BOOST_REQUIRE(failed);
  const std::vector<std::string> expected{
      "start:first", "start:second", "stop:first"};
  BOOST_CHECK(events == expected);
  BOOST_CHECK_EQUAL(lifecycle.running_steps(), 0U);
}

SEASTAR_TEST_CASE(lifecycle_waits_for_held_gate_operation) {
  seastar::abort_source abort_source;
  kwaque::broker::service_lifecycle lifecycle(abort_source);
  seastar::promise<> release;

  auto held = seastar::with_gate(lifecycle.gate(), [&release] {
    return release.get_future();
  });
  auto stopping = lifecycle.stop();
  co_await seastar::yield();
  BOOST_CHECK(!stopping.available());

  release.set_value();
  co_await std::move(held);
  co_await std::move(stopping);
}

SEASTAR_TEST_CASE(lifecycle_rolls_back_when_startup_is_interrupted) {
  seastar::abort_source abort_source;
  kwaque::broker::service_lifecycle lifecycle(abort_source);
  std::vector<std::string> events;

  co_await lifecycle.start_step(
      "first",
      [&events] {
        events.emplace_back("start:first");
        return ready();
      },
      [&events] {
        events.emplace_back("stop:first");
        return ready();
      });
  abort_source.request_abort();

  bool interrupted = false;
  try {
    co_await lifecycle.start_step(
        "second",
        [&events] {
          events.emplace_back("start:second");
          return ready();
        },
        [&events] {
          events.emplace_back("stop:second");
          return ready();
        });
  } catch (const seastar::abort_requested_exception &) {
    interrupted = true;
  }

  BOOST_REQUIRE(interrupted);
  const std::vector<std::string> expected{"start:first", "stop:first"};
  BOOST_CHECK(events == expected);
  BOOST_CHECK_EQUAL(lifecycle.running_steps(), 0U);
}

SEASTAR_TEST_CASE(application_constructs_services_without_starting_them) {
  kwaque::broker::detail::application_state state;
  state.construct_services(false);

  BOOST_CHECK(state.services_constructed());
  BOOST_CHECK(!state.runtime_started());
  BOOST_REQUIRE(state.lifecycle() != nullptr);
  BOOST_CHECK_EQUAL(state.lifecycle()->running_steps(), 0U);

  co_await state.shutdown();
}
