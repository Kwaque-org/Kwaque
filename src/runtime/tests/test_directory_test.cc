#include "src/runtime/error.h"
#include "src/runtime/testing/contracts/cleanup.h"
#include "src/runtime/testing/test_directory.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/seastar.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/tmp_file.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <exception>
#include <filesystem>
#include <stdexcept>
#include <string_view>
#include <system_error>

SEASTAR_TEST_CASE(test_directory_is_below_test_root_and_removed_after_success) {
    std::filesystem::path created;
    const auto path_template
      = kwaque::runtime::testing::test_directory_template();
    const auto inspect = [&created, &path_template](
                           seastar::tmp_dir& directory) -> seastar::future<> {
        created = directory.get_path();
        BOOST_CHECK(created.parent_path() == path_template.parent_path());
        const auto exists = co_await seastar::file_exists(created.string());
        BOOST_CHECK(exists);
    };
    co_await seastar::tmp_dir::do_with(
      path_template,
      [&inspect](seastar::tmp_dir& directory) { return inspect(directory); });
    const auto exists = co_await seastar::file_exists(created.string());
    BOOST_CHECK(!exists);
}

SEASTAR_TEST_CASE(test_directory_is_removed_after_body_failure) {
    std::filesystem::path created;
    bool failed = false;
    try {
        co_await seastar::tmp_dir::do_with(
          kwaque::runtime::testing::test_directory_template(),
          [&created](seastar::tmp_dir& directory) -> seastar::future<> {
              created = directory.get_path();
              return seastar::make_exception_future<>(
                std::runtime_error("injected body failure"));
          });
    } catch (const std::runtime_error& error) {
        failed = std::string_view{error.what()} == "injected body failure";
    }
    BOOST_CHECK(failed);
    const auto exists = co_await seastar::file_exists(created.string());
    BOOST_CHECK(!exists);
}

SEASTAR_TEST_CASE(test_directory_removal_failure_is_propagated) {
    // Remove the owned directory early so final cleanup encounters ENOENT.
    bool failed = false;
    try {
        co_await seastar::tmp_dir::do_with(
          kwaque::runtime::testing::test_directory_template(),
          [](seastar::tmp_dir& directory) {
              return seastar::remove_file(directory.get_path().string());
          });
    } catch (const std::system_error& error) {
        failed = error.code() == std::errc::no_such_file_or_directory;
    }
    BOOST_CHECK(failed);
}

SEASTAR_TEST_CASE(typed_cleanup_failure_is_not_discarded_by_native_finally) {
    bool failed = false;
    try {
        co_await seastar::make_ready_future<>().finally([] {
            return seastar::make_ready_future<kwaque::runtime::result<void>>(
                     kwaque::runtime::failure(
                       kwaque::runtime::operation_error{
                         kwaque::errc::io_failure,
                         kwaque::runtime::operation_kind::file}))
              .then(kwaque::runtime::testing::require_cleanup);
        });
    } catch (const std::runtime_error&) {
        failed = true;
    }
    BOOST_CHECK(failed);
}

SEASTAR_TEST_CASE(cleanup_matches_native_finally_exception_order) {
    const auto body = std::make_exception_ptr(
      std::runtime_error("body failure"));
    const auto cleanup = std::make_exception_ptr(
      std::runtime_error("cleanup failure"));
    std::exception_ptr native;
    try {
        co_await seastar::make_exception_future<>(body).finally(
          [cleanup] { return seastar::make_exception_future<>(cleanup); });
    } catch (...) {
        native = std::current_exception();
    }
    auto retained = body;
    try {
        std::rethrow_exception(cleanup);
    } catch (...) {
        kwaque::runtime::testing::retain_cleanup_failure(retained);
    }
    for (const auto& failure : std::array{native, retained}) {
        BOOST_REQUIRE(failure != nullptr);
        try {
            std::rethrow_exception(failure);
        } catch (const seastar::nested_exception& error) {
            BOOST_CHECK(error.inner == cleanup);
            BOOST_CHECK(error.outer == body);
            try {
                error.rethrow_nested();
            } catch (const std::runtime_error& original) {
                BOOST_CHECK(
                  std::string_view{original.what()} == "body failure");
            }
        }
    }
}
