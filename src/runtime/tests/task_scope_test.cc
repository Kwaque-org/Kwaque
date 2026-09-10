#include "src/runtime/task_scope.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <exception>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace {

static_assert(
  !noexcept(std::declval<kwaque::runtime::task_scope&>().request_abort()));
static_assert(!std::is_nothrow_constructible_v<
              kwaque::runtime::task_scope,
              kwaque::runtime::task_scope::failure_notifier>);

class delayed_task final {
public:
    delayed_task(seastar::future<> release, unsigned& completions) noexcept
      : release_(std::move(release))
      , completions_(completions) {}

    delayed_task(delayed_task&&) noexcept = default;
    delayed_task(const delayed_task&) = delete;
    delayed_task& operator=(const delayed_task&) = delete;

    seastar::future<> operator()() {
        co_await std::move(release_);
        ++completions_;
    }

private:
    seastar::future<> release_;
    unsigned& completions_;
};

} // namespace

SEASTAR_TEST_CASE(task_scope_aborts_rejects_and_drains_owned_temporary_work) {
    kwaque::runtime::task_scope scope;
    seastar::promise<> release;
    unsigned completions = 0;

    const auto accepted = scope.spawn(
      delayed_task{release.get_future(), completions});
    BOOST_REQUIRE(accepted.has_value());
    BOOST_CHECK_EQUAL(scope.task_count(), 1U);
    BOOST_CHECK_EQUAL(scope.statistics().accepted, 1U);
    BOOST_CHECK_EQUAL(scope.statistics().completed, 0U);

    bool closed_during_abort = false;
    auto subscription = scope.abort_source().subscribe(
      [&] noexcept { closed_during_abort = scope.admission_closed(); });
    auto closing = scope.close();
    BOOST_CHECK(closed_during_abort);
    co_await seastar::yield();
    BOOST_CHECK(scope.abort_requested());
    BOOST_CHECK_EQUAL(scope.statistics().abort_requests, 1U);
    BOOST_CHECK(scope.admission_closed());
    BOOST_CHECK(!closing.available());

    const auto rejected = scope.spawn(
      [] { return seastar::make_ready_future<>(); });
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == kwaque::errc::closed);
    BOOST_CHECK_EQUAL(scope.statistics().accepted, 1U);

    release.set_value();
    co_await std::move(closing);
    BOOST_CHECK_EQUAL(completions, 1U);
    BOOST_CHECK_EQUAL(scope.task_count(), 0U);
    BOOST_CHECK_EQUAL(scope.statistics().completed, 1U);
    BOOST_CHECK_EQUAL(scope.statistics().failed, 0U);
    co_await scope.close();
}

SEASTAR_TEST_CASE(task_scope_closes_admission_before_separate_abort_and_drain) {
    kwaque::runtime::task_scope scope;
    seastar::promise<> release;
    unsigned completions = 0;
    BOOST_REQUIRE(
      scope.spawn(delayed_task{release.get_future(), completions}).has_value());
    unsigned abort_notifications = 0;
    unsigned rejected_invocations = 0;
    bool rejected_during_abort = false;
    auto subscription = scope.abort_source().subscribe([&] noexcept {
        ++abort_notifications;
        const auto admitted = scope.spawn([&] {
            ++rejected_invocations;
            return seastar::make_ready_future<>();
        });
        rejected_during_abort = !admitted.has_value()
                                && admitted.error().code()
                                     == kwaque::errc::closed;
    });

    scope.close_admission();
    scope.close_admission();
    BOOST_CHECK(scope.admission_closed());
    BOOST_CHECK(!scope.abort_requested());
    BOOST_CHECK_EQUAL(scope.task_count(), 1U);
    BOOST_CHECK_EQUAL(completions, 0U);
    scope.request_abort();
    scope.request_abort();
    BOOST_CHECK_EQUAL(abort_notifications, 1U);
    BOOST_CHECK(rejected_during_abort);
    BOOST_CHECK_EQUAL(rejected_invocations, 0U);

    auto closing = scope.close();
    co_await seastar::yield();
    BOOST_CHECK(!closing.available());
    release.set_value();
    co_await std::move(closing);
    BOOST_CHECK_EQUAL(completions, 1U);
    BOOST_CHECK_EQUAL(scope.statistics().accepted, 1U);
    BOOST_CHECK_EQUAL(scope.statistics().completed, 1U);
    BOOST_CHECK_EQUAL(scope.statistics().abort_requests, 1U);
}

SEASTAR_TEST_CASE(task_scope_abort_alone_preserves_task_admission) {
    kwaque::runtime::task_scope scope;
    bool admitted_during_abort = false;
    bool closed_during_abort = true;
    unsigned abort_work = 0;
    auto subscription = scope.abort_source().subscribe([&] noexcept {
        closed_during_abort = scope.admission_closed();
        admitted_during_abort = scope
                                  .spawn([&] {
                                      ++abort_work;
                                      return seastar::make_ready_future<>();
                                  })
                                  .has_value();
    });
    scope.request_abort();
    BOOST_CHECK(!closed_during_abort);
    BOOST_CHECK(admitted_during_abort);
    BOOST_CHECK_EQUAL(abort_work, 1U);
    BOOST_CHECK(!scope.admission_closed());

    seastar::promise<> release;
    unsigned completions = 0;
    BOOST_REQUIRE(
      scope.spawn(delayed_task{release.get_future(), completions}).has_value());
    auto closing = scope.close();
    co_await seastar::yield();
    BOOST_CHECK(!closing.available());
    release.set_value();
    co_await std::move(closing);
    BOOST_CHECK_EQUAL(completions, 1U);
    BOOST_CHECK_EQUAL(scope.statistics().accepted, 2U);
    BOOST_CHECK_EQUAL(scope.statistics().completed, 2U);
}

SEASTAR_TEST_CASE(
  task_scope_propagates_parent_abort_and_unlinks_both_lifetimes) {
    auto parent = std::make_unique<seastar::abort_source>();
    auto child = std::make_unique<kwaque::runtime::task_scope>(*parent);

    parent->request_abort();
    BOOST_CHECK(child->abort_requested());
    co_await child->close();
    child.reset();
    parent.reset();

    auto second_parent = std::make_unique<seastar::abort_source>();
    auto second_child = std::make_unique<kwaque::runtime::task_scope>(
      *second_parent);
    co_await second_child->close();
    second_child.reset();
    second_parent->request_abort();
    second_parent.reset();

    auto third_parent = std::make_unique<seastar::abort_source>();
    auto third_child = std::make_unique<kwaque::runtime::task_scope>(
      *third_parent);
    third_parent.reset();
    BOOST_CHECK(!third_child->abort_requested());
    co_await third_child->close();
    third_child.reset();
}

SEASTAR_TEST_CASE(task_scope_reports_the_first_background_failure_once_closed) {
    kwaque::runtime::task_scope scope;
    const auto accepted = scope.spawn([] {
        return seastar::make_exception_future<>(
          std::runtime_error("background failure"));
    });
    BOOST_REQUIRE(accepted.has_value());

    bool failed = false;
    try {
        co_await scope.close();
    } catch (const std::runtime_error&) {
        failed = true;
    }
    BOOST_REQUIRE(failed);
    BOOST_CHECK_EQUAL(scope.statistics().accepted, 1U);
    BOOST_CHECK_EQUAL(scope.statistics().completed, 1U);
    BOOST_CHECK_EQUAL(scope.statistics().failed, 1U);

    failed = false;
    try {
        co_await scope.close();
    } catch (const std::runtime_error&) {
        failed = true;
    }
    BOOST_REQUIRE(failed);
}

SEASTAR_TEST_CASE(task_scope_notifies_first_failure_before_close) {
    seastar::promise<> notified;
    auto notification = notified.get_future();
    unsigned calls = 0;
    std::exception_ptr observed;
    const auto expected = std::make_exception_ptr(std::runtime_error("first"));
    kwaque::runtime::task_scope scope{[&](std::exception_ptr failure) noexcept {
        observed = std::move(failure);
        ++calls;
        notified.set_value();
    }};
    BOOST_REQUIRE(scope
                    .spawn([expected]() -> seastar::future<> {
                        std::rethrow_exception(expected);
                    })
                    .has_value());
    co_await std::move(notification);
    BOOST_CHECK(observed == expected);
    BOOST_CHECK_EQUAL(calls, 1U);
    BOOST_CHECK(!scope.admission_closed());
    BOOST_REQUIRE(scope
                    .spawn([] {
                        return seastar::make_exception_future<>(
                          std::logic_error("second"));
                    })
                    .has_value());
    try {
        co_await scope.close();
        BOOST_FAIL("close must retain the first failure");
    } catch (...) {
        BOOST_CHECK(std::current_exception() == expected);
    }
    BOOST_CHECK_EQUAL(calls, 1U);
    BOOST_CHECK_EQUAL(scope.statistics().failed, 2U);
}

SEASTAR_TEST_CASE(task_scope_stop_does_not_hide_a_suspended_failure) {
    seastar::promise<> release;
    seastar::promise<> notified;
    auto notification = notified.get_future();
    const auto expected = std::make_exception_ptr(std::logic_error("worker"));
    std::exception_ptr observed;
    kwaque::runtime::task_scope scope{[&](std::exception_ptr failure) noexcept {
        observed = std::move(failure);
        notified.set_value();
    }};
    BOOST_REQUIRE(scope
                    .spawn(
                      [pending = release.get_future(),
                       expected]() mutable -> seastar::future<> {
                          co_await std::move(pending);
                          std::rethrow_exception(expected);
                      },
                      kwaque::runtime::task_lifetime::until_abort)
                    .has_value());
    scope.request_abort();
    BOOST_CHECK(!notification.available());
    release.set_value();
    co_await std::move(notification);
    BOOST_CHECK(observed == expected);
    BOOST_CHECK(!scope.admission_closed());
    try {
        co_await scope.close();
        BOOST_FAIL("stop must not replace an unrelated worker failure");
    } catch (...) {
        BOOST_CHECK(std::current_exception() == expected);
    }
}

SEASTAR_TEST_CASE(
  task_scope_required_task_accepts_only_requested_cancellation) {
    kwaque::runtime::task_scope scope;
    seastar::promise<> release;
    BOOST_REQUIRE(
      scope
        .spawn(
          [pending = release.get_future()]() mutable -> seastar::future<> {
              co_await std::move(pending);
              throw seastar::abort_requested_exception{};
          },
          kwaque::runtime::task_lifetime::until_abort)
        .has_value());
    scope.request_abort();
    release.set_value();
    co_await scope.close();
    BOOST_CHECK_EQUAL(scope.statistics().failed, 0U);

    kwaque::runtime::task_scope normally_stopped;
    seastar::promise<> finish;
    BOOST_REQUIRE(normally_stopped
                    .spawn(
                      [pending = finish.get_future()] mutable {
                          return std::move(pending);
                      },
                      kwaque::runtime::task_lifetime::until_abort)
                    .has_value());
    normally_stopped.request_abort();
    finish.set_value();
    co_await normally_stopped.close();
    BOOST_CHECK_EQUAL(normally_stopped.statistics().failed, 0U);
}

SEASTAR_TEST_CASE(task_scope_retains_notification_owner_until_destruction) {
    auto owner = std::make_shared<unsigned>(0);
    std::weak_ptr<unsigned> lifetime = owner;
    auto scope = std::make_unique<kwaque::runtime::task_scope>(
      [retained = std::move(owner)](std::exception_ptr) noexcept {
          ++*retained;
      });
    BOOST_CHECK(!lifetime.expired());
    BOOST_REQUIRE(scope
                    ->spawn([] {
                        return seastar::make_exception_future<>(
                          std::runtime_error("failure"));
                    })
                    .has_value());
    try {
        co_await scope->close();
        BOOST_FAIL("close must expose task failure");
    } catch (const std::runtime_error&) {
    }
    BOOST_REQUIRE(!lifetime.expired());
    BOOST_CHECK_EQUAL(*lifetime.lock(), 1U);
    scope.reset();
    BOOST_CHECK(lifetime.expired());
}

SEASTAR_TEST_CASE(task_scope_notifier_construction_failure_is_recoverable) {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    auto owner = std::make_shared<unsigned>(0);
    std::weak_ptr<unsigned> lifetime = owner;
    kwaque::runtime::task_scope::failure_notifier notifier{
      [retained = std::move(owner)](std::exception_ptr) noexcept {
          ++*retained;
      }};
    std::optional<kwaque::runtime::task_scope> scope;
    bool caught = false;
    auto& injector = seastar::memory::local_failure_injector();
    injector.fail_after(0);
    try {
        scope.emplace(std::move(notifier));
    } catch (const std::bad_alloc&) {
        caught = true;
    }
    const bool injected = injector.failed();
    injector.cancel();
    if (scope) {
        co_await scope->close();
        scope.reset();
    }
    BOOST_CHECK(injected);
    BOOST_CHECK(caught);
    BOOST_CHECK(lifetime.expired());
#endif
    co_return;
}
