#include "src/runtime/timer.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <cassert>
#include <exception>
#include <optional>
#include <utility>

namespace {

class test_clock final {
public:
    static kwaque::runtime::monotonic_time now() noexcept { return now_; }
    static void set(kwaque::runtime::monotonic_time value) noexcept {
        now_ = value;
    }

private:
    static inline kwaque::runtime::monotonic_time now_{};
};

class contract_timer_service final {
public:
    ~contract_timer_service() { assert(!pending_ || completed_); }

    seastar::future<kwaque::runtime::result<void>> sleep_until(
      kwaque::runtime::monotonic_time deadline,
      seastar::abort_source& abort_source) {
        ++submissions_;
        deadline_ = deadline;
        pending_ = true;
        auto subscription = abort_source.subscribe(
          [this](const std::optional<std::exception_ptr>&) noexcept {
              complete_error(kwaque::errc::aborted);
          });
        if (subscription) {
            subscription_ = std::move(*subscription);
        } else {
            complete_error(kwaque::errc::aborted);
        }
        return completion_.get_future();
    }

    void request_abort() noexcept { complete_error(kwaque::errc::aborted); }

    seastar::future<kwaque::runtime::result<void>> stop() {
        complete_error(kwaque::errc::closed);
        return seastar::make_ready_future<kwaque::runtime::result<void>>(
          kwaque::runtime::result<void>{});
    }

    void fire() noexcept { complete(kwaque::runtime::result<void>{}); }

    [[nodiscard]] kwaque::runtime::monotonic_time deadline() const noexcept {
        return deadline_;
    }
    [[nodiscard]] unsigned submissions() const noexcept { return submissions_; }

private:
    void complete_error(kwaque::errc code) noexcept {
        kwaque::runtime::result<void> outcome = kwaque::runtime::failure(
          kwaque::runtime::operation_error{
            code, kwaque::runtime::operation_kind::timer});
        complete(std::move(outcome));
    }

    void complete(kwaque::runtime::result<void> outcome) noexcept {
        if (!pending_ || completed_) {
            return;
        }
        completed_ = true;
        completion_.set_value(std::move(outcome));
    }

    seastar::promise<kwaque::runtime::result<void>> completion_;
    std::optional<seastar::abort_source::subscription> subscription_;
    kwaque::runtime::monotonic_time deadline_;
    unsigned submissions_{0};
    bool pending_{false};
    bool completed_{false};
};

class concurrent_timer_service final {
public:
    ~concurrent_timer_service() { assert(completed_count_ == submissions_); }

    seastar::future<kwaque::runtime::result<void>>
    sleep_until(kwaque::runtime::monotonic_time, seastar::abort_source&) {
        assert(submissions_ < completions_.size());
        return completions_[submissions_++].get_future();
    }

    void request_abort() noexcept {
        for (std::size_t index = 0; index < submissions_; ++index) {
            if (completed_[index]) {
                continue;
            }
            kwaque::runtime::result<void> outcome = kwaque::runtime::failure(
              kwaque::runtime::operation_error{
                kwaque::errc::aborted, kwaque::runtime::operation_kind::timer});
            completions_[index].set_value(std::move(outcome));
            completed_[index] = true;
            ++completed_count_;
        }
    }

    seastar::future<kwaque::runtime::result<void>> stop() {
        request_abort();
        return seastar::make_ready_future<kwaque::runtime::result<void>>(
          kwaque::runtime::result<void>{});
    }

    void fire(std::size_t index) {
        assert(index < submissions_ && !completed_[index]);
        completions_[index].set_value(kwaque::runtime::result<void>{});
        completed_[index] = true;
        ++completed_count_;
    }

private:
    std::array<seastar::promise<kwaque::runtime::result<void>>, 2> completions_;
    std::array<bool, 2> completed_{};
    std::size_t submissions_{0};
    std::size_t completed_count_{0};
};

static_assert(kwaque::runtime::monotonic_clock<test_clock>);
static_assert(kwaque::runtime::timer_service<contract_timer_service>);
static_assert(kwaque::runtime::timer_service<concurrent_timer_service>);

} // namespace

SEASTAR_TEST_CASE(timer_sleep_for_checks_and_delegates_one_deadline) {
    test_clock::set(kwaque::runtime::monotonic_time{100});
    contract_timer_service service;
    seastar::abort_source abort_source;

    auto waiting = kwaque::runtime::sleep_for<test_clock>(
      service, kwaque::runtime::monotonic_duration{25}, abort_source);
    BOOST_CHECK(!waiting.available());
    BOOST_CHECK_EQUAL(service.deadline().nanoseconds(), 125U);
    BOOST_CHECK_EQUAL(service.submissions(), 1U);
    service.fire();
    const auto outcome = co_await std::move(waiting);
    BOOST_REQUIRE(outcome.has_value());
}

SEASTAR_TEST_CASE(timer_sleep_for_rejects_overflow_before_submission) {
    test_clock::set(kwaque::runtime::monotonic_time::maximum());
    contract_timer_service service;
    seastar::abort_source abort_source;

    const auto outcome = co_await kwaque::runtime::sleep_for<test_clock>(
      service, kwaque::runtime::monotonic_duration{1}, abort_source);
    BOOST_REQUIRE(!outcome.has_value());
    BOOST_CHECK(outcome.error().code() == kwaque::errc::out_of_range);
    BOOST_CHECK_EQUAL(service.submissions(), 0U);
}

SEASTAR_TEST_CASE(timer_past_deadline_remains_pending_until_backend_event) {
    contract_timer_service service;
    seastar::abort_source abort_source;
    auto waiting = service.sleep_until(
      kwaque::runtime::monotonic_time{1}, abort_source);

    BOOST_CHECK(!waiting.available());
    service.fire();
    const auto outcome = co_await std::move(waiting);
    BOOST_REQUIRE(outcome.has_value());
}

SEASTAR_TEST_CASE(timer_abort_and_deadline_race_has_one_terminal_outcome) {
    contract_timer_service aborted_service;
    seastar::abort_source abort_source;
    auto aborted = aborted_service.sleep_until(
      kwaque::runtime::monotonic_time{10}, abort_source);
    abort_source.request_abort();
    aborted_service.request_abort();
    aborted_service.fire();
    const auto aborted_outcome = co_await std::move(aborted);
    BOOST_REQUIRE(!aborted_outcome.has_value());
    BOOST_CHECK(aborted_outcome.error().code() == kwaque::errc::aborted);

    contract_timer_service fired_service;
    seastar::abort_source later_abort;
    auto fired = fired_service.sleep_until(
      kwaque::runtime::monotonic_time{10}, later_abort);
    fired_service.fire();
    later_abort.request_abort();
    const auto fired_outcome = co_await std::move(fired);
    BOOST_REQUIRE(fired_outcome.has_value());
}

SEASTAR_TEST_CASE(timer_abort_before_submission_and_stop_are_typed) {
    contract_timer_service preaborted_service;
    seastar::abort_source preaborted;
    preaborted.request_abort();
    const auto preaborted_outcome = co_await preaborted_service.sleep_until(
      kwaque::runtime::monotonic_time{10}, preaborted);
    BOOST_REQUIRE(!preaborted_outcome.has_value());
    BOOST_CHECK(preaborted_outcome.error().code() == kwaque::errc::aborted);

    contract_timer_service stopped_service;
    seastar::abort_source active;
    auto waiting = stopped_service.sleep_until(
      kwaque::runtime::monotonic_time{20}, active);
    const auto stopped = co_await stopped_service.stop();
    BOOST_REQUIRE(stopped.has_value());
    active.request_abort();
    stopped_service.request_abort();
    const auto waiting_outcome = co_await std::move(waiting);
    BOOST_REQUIRE(!waiting_outcome.has_value());
    BOOST_CHECK(waiting_outcome.error().code() == kwaque::errc::closed);
}

SEASTAR_TEST_CASE(timer_service_accepts_independent_concurrent_waits) {
    concurrent_timer_service service;
    seastar::abort_source abort_source;
    auto first = service.sleep_until(
      kwaque::runtime::monotonic_time{10}, abort_source);
    auto second = service.sleep_until(
      kwaque::runtime::monotonic_time{20}, abort_source);
    BOOST_CHECK(!first.available());
    BOOST_CHECK(!second.available());

    service.fire(1);
    const auto second_result = co_await std::move(second);
    BOOST_REQUIRE(second_result.has_value());
    BOOST_CHECK(!first.available());

    service.request_abort();
    const auto first_result = co_await std::move(first);
    BOOST_REQUIRE(!first_result.has_value());
    BOOST_CHECK(first_result.error().code() == kwaque::errc::aborted);
    const auto stopped = co_await service.stop();
    BOOST_REQUIRE(stopped.has_value());
}
