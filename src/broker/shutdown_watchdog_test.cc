#include "src/broker/service_lifecycle.h"
#include "src/broker/shutdown_watchdog.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <chrono>
#include <cstddef>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace {

using namespace std::chrono_literals;
using kwaque::broker::shutdown_stage_name;
using kwaque::broker::shutdown_watchdog_event;
using watchdog = kwaque::broker::shutdown_watchdog<seastar::manual_clock>;

struct observations final {
    std::array<shutdown_watchdog_event, 8> events{};
    std::size_t count{};
    bool names_match{true};

    void record(shutdown_watchdog_event event, std::string_view name) noexcept {
        names_match = names_match && name == "runtime-tasks";
        if (count < events.size()) {
            events[count] = event;
        }
        ++count;
    }

    watchdog::observer observer() {
        return [this](
                 shutdown_watchdog_event event,
                 std::string_view name) noexcept { record(event, name); };
    }
};

seastar::future<> advance(seastar::manual_clock::duration duration) {
    seastar::manual_clock::advance(duration);
    co_await seastar::yield();
}

seastar::future<>
wait_for_shutdown(seastar::promise<>& release, watchdog::observer report) {
    watchdog guard{shutdown_stage_name{"runtime-tasks"}, std::move(report)};
    co_await release.get_future();
    guard.finish();
}

} // namespace

SEASTAR_TEST_CASE(
  shutdown_watchdog_warns_at_both_thresholds_and_keeps_waiting) {
    observations observed;
    seastar::promise<> release;
    seastar::abort_source subsystem_abort;
    auto waiting = wait_for_shutdown(release, observed.observer());
    subsystem_abort.request_abort();
    BOOST_CHECK(!waiting.available());
    BOOST_CHECK_EQUAL(observed.count, 1U);
    BOOST_CHECK(observed.events[0] == shutdown_watchdog_event::begin);

    co_await advance(15s - 1ns);
    BOOST_CHECK_EQUAL(observed.count, 1U);
    co_await advance(1ns);
    BOOST_CHECK_EQUAL(observed.count, 2U);
    BOOST_CHECK(observed.events[1] == shutdown_watchdog_event::information);
    BOOST_CHECK(!waiting.available());

    co_await advance(105s - 1ns);
    BOOST_CHECK_EQUAL(observed.count, 2U);
    co_await advance(1ns);
    BOOST_CHECK_EQUAL(observed.count, 3U);
    BOOST_CHECK(observed.events[2] == shutdown_watchdog_event::error);
    BOOST_CHECK(!waiting.available());
    co_await advance(1h);
    BOOST_CHECK_EQUAL(observed.count, 3U);
    BOOST_CHECK(!waiting.available());

    release.set_value();
    co_await std::move(waiting);
    BOOST_CHECK_EQUAL(observed.count, 4U);
    BOOST_CHECK(observed.events[3] == shutdown_watchdog_event::complete);
    BOOST_CHECK(observed.names_match);
}

SEASTAR_TEST_CASE(shutdown_watchdog_disarms_after_completion_or_failure) {
    for (const bool fail : {false, true}) {
        observations observed;
        seastar::promise<> release;
        auto waiting = wait_for_shutdown(release, observed.observer());
        co_await advance(14s);
        if (fail) {
            release.set_exception(std::runtime_error("shutdown failure"));
        } else {
            release.set_value();
        }
        bool failed = false;
        try {
            co_await std::move(waiting);
        } catch (const std::runtime_error&) {
            failed = true;
        }
        BOOST_CHECK_EQUAL(failed, fail);
        BOOST_CHECK_EQUAL(observed.count, 2U);
        BOOST_CHECK(
          observed.events[1]
          == (fail ? shutdown_watchdog_event::failed : shutdown_watchdog_event::complete));
        co_await advance(121s);
        BOOST_CHECK_EQUAL(observed.count, 2U);
        BOOST_CHECK(observed.names_match);
    }
}

SEASTAR_TEST_CASE(shutdown_watchdog_owns_names_and_callback_until_disarmed) {
    auto observed = std::make_shared<observations>();
    const std::weak_ptr<observations> lifetime{observed};
    {
        std::string name{"runtime-tasks"};
        watchdog guard{
          shutdown_stage_name{name},
          [owner = observed](
            shutdown_watchdog_event event, std::string_view stage) noexcept {
              owner->record(event, stage);
          }};
        name.assign("changed-name");
        observed.reset();
        co_await advance(15s);
        BOOST_CHECK(!lifetime.expired());
        const auto retained = lifetime.lock();
        BOOST_CHECK_EQUAL(retained->count, 2U);
        BOOST_CHECK(retained->names_match);
        guard.finish();
        guard.finish();
        guard.fail();
        BOOST_CHECK_EQUAL(retained->count, 3U);
    }
    BOOST_CHECK(lifetime.expired());
    co_await advance(121s);
}

SEASTAR_TEST_CASE(shutdown_watchdog_destruction_cancels_pending_expiry) {
    observations observed;
    {
        watchdog guard{
          shutdown_stage_name{"runtime-tasks"}, observed.observer()};
        // Advancing the clock can invoke expired callbacks before returning.
        // Destroy the guard while both deadlines are still pending.
        co_await advance(15s - 1ns);
        BOOST_CHECK_EQUAL(observed.count, 1U);
    }
    BOOST_CHECK_EQUAL(observed.count, 2U);
    BOOST_CHECK(observed.events[0] == shutdown_watchdog_event::begin);
    BOOST_CHECK(observed.events[1] == shutdown_watchdog_event::failed);
    co_await advance(105s + 1ns);
    BOOST_CHECK_EQUAL(observed.count, 2U);
}

SEASTAR_TEST_CASE(shutdown_stage_names_are_bounded_and_single_line) {
    for (const std::string_view invalid : {"", "two words", "line\nbreak"}) {
        BOOST_CHECK_THROW(shutdown_stage_name{invalid}, std::invalid_argument);
    }
    const std::string maximum(shutdown_stage_name::max_size, 'a');
    BOOST_CHECK_EQUAL(shutdown_stage_name{maximum}.value(), maximum);
    const std::string oversized(shutdown_stage_name::max_size + 1U, 'a');
    BOOST_CHECK_THROW(shutdown_stage_name{oversized}, std::invalid_argument);
    co_return;
}

SEASTAR_TEST_CASE(
  lifecycle_can_retain_failed_start_for_owner_ordered_rollback) {
    seastar::abort_source abort_source;
    kwaque::broker::service_lifecycle lifecycle{abort_source, false};
    std::array<unsigned, 2> stopped{};
    std::size_t count{};
    co_await lifecycle.start_step(
      "first",
      [] { return seastar::make_ready_future<>(); },
      [&] {
          stopped[count++] = 1;
          return seastar::make_ready_future<>();
      });
    const auto failure = std::make_exception_ptr(std::runtime_error("start"));
    std::exception_ptr observed;
    try {
        co_await lifecycle.start_step(
          "second",
          [failure] { return seastar::make_exception_future<>(failure); },
          [&] {
              stopped[count++] = 2;
              return seastar::make_ready_future<>();
          });
    } catch (...) {
        observed = std::current_exception();
    }
    BOOST_CHECK(observed == failure);
    BOOST_CHECK_EQUAL(count, 0U);
    BOOST_CHECK_EQUAL(lifecycle.running_steps(), 2U);
    abort_source.request_abort();
    co_await lifecycle.stop();
    BOOST_CHECK_EQUAL(count, 2U);
    BOOST_CHECK_EQUAL(stopped[0], 2U);
    BOOST_CHECK_EQUAL(stopped[1], 1U);
}
