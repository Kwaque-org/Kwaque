#include "src/runtime/task_scope.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <memory>
#include <stdexcept>
#include <utility>

namespace {

static_assert(
  !noexcept(std::declval<kwaque::runtime::task_scope&>().request_abort()));

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

    auto closing = scope.close();
    co_await seastar::yield();
    BOOST_CHECK(scope.abort_requested());
    BOOST_CHECK(scope.closed());
    BOOST_CHECK(!closing.available());

    const auto rejected = scope.spawn(
      [] { return seastar::make_ready_future<>(); });
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == kwaque::errc::closed);

    release.set_value();
    co_await std::move(closing);
    BOOST_CHECK_EQUAL(completions, 1U);
    BOOST_CHECK_EQUAL(scope.task_count(), 0U);
    co_await scope.close();
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

    failed = false;
    try {
        co_await scope.close();
    } catch (const std::runtime_error&) {
        failed = true;
    }
    BOOST_REQUIRE(failed);
}
