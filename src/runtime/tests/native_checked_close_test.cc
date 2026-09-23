#include "src/runtime/file.h"
#include "src/runtime/testing/test_directory.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/posix.hh>
#include <seastar/testing/file-impl.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/tmp_file.hh>

#include <boost/test/unit_test.hpp>

#include <atomic>
#include <cerrno>
#include <exception>
#include <fcntl.h>
#include <optional>
#include <system_error>
#include <unistd.h>

namespace {
std::atomic<int> watched_fd{-1};
std::atomic<int> close_errno{0};
std::atomic<int> truncate_errno{0};
std::atomic<unsigned> close_calls{0};
} // namespace

extern "C" int __real_close(int fd);
extern "C" int __real_ftruncate(int fd, off_t length);
extern "C" int __wrap_close(int fd) {
    const int result = __real_close(fd);
    if (fd == watched_fd.load()) {
        ++close_calls;
        if (const int failure = close_errno.load()) {
            errno = failure;
            return -1;
        }
    }
    return result;
}
extern "C" int __wrap_ftruncate(int fd, off_t length) {
    if (fd == watched_fd.load())
        if (const int failure = truncate_errno.load()) {
            errno = failure;
            return -1;
        }
    return __real_ftruncate(fd, length);
}

namespace seastar::testing {
class append_challenged_posix_file_test {
public:
    static void set_slack(append_challenged_posix_file_impl& impl) {
        impl._logical_size = 1;
        impl._committed_size = 4096;
    }
};
} // namespace seastar::testing

SEASTAR_TEST_CASE(
  native_checked_close_reports_errors_after_one_descriptor_release) {
    co_await seastar::tmp_dir::do_with(
      kwaque::runtime::testing::test_directory_template(),
      [](auto& dir) -> seastar::future<> {
          for (const bool append : {false, true}) {
              for (const bool checked : {false, true}) {
                  int fd = ::open(
                    (dir.get_path() / "close").c_str(), O_RDWR | O_CREAT, 0600);
                  BOOST_REQUIRE_GE(fd, 0);
                  auto guard = seastar::defer([fd] { ::close(fd); });
                  seastar::shared_ptr<seastar::file_impl> impl;
                  if (append) {
                      guard.cancel();
                      auto selected
                        = seastar::testing::make_append_challenged_posix_file(
                          fd, 0, true, std::nullopt);
                      seastar::testing::append_challenged_posix_file_test::
                        set_slack(*selected);
                      impl = std::move(selected);
                  } else {
                      struct stat status{};
                      BOOST_REQUIRE_EQUAL(::fstat(fd, &status), 0);
                      seastar::file_open_options options;
                      options.append_is_unlikely = true;
                      guard.cancel();
                      impl = co_await seastar::make_file_impl(
                        fd, options, O_RDWR, status);
                  }
                  const int number = fd;
                  guard.cancel();
                  seastar::file native{std::move(impl)};
                  close_calls = 0;
                  close_errno = EIO;
                  truncate_errno = append ? ENOSPC : 0;
                  watched_fd = number;
                  auto unwatch = seastar::defer([] { watched_fd = -1; });
                  std::exception_ptr failure;
                  try {
                      if (checked)
                          co_await native.close_checked();
                      else
                          co_await native.close();
                  } catch (...) {
                      failure = std::current_exception();
                  }
                  BOOST_CHECK_EQUAL(close_calls.load(), 1U);
                  BOOST_CHECK_EQUAL(::fcntl(number, F_GETFD), -1);
                  BOOST_CHECK_EQUAL(errno, EBADF);
                  BOOST_CHECK_EQUAL(bool(failure), checked);
                  if (failure) {
                      try {
                          std::rethrow_exception(failure);
                      } catch (const std::system_error& error) {
                          BOOST_CHECK(
                            error.code()
                            == std::error_code(
                              append ? ENOSPC : EIO, std::system_category()));
                      }
                  }
                  if (checked) {
                      const int replacement = ::open(
                        (dir.get_path() / "close").c_str(), O_RDONLY);
                      BOOST_REQUIRE_GE(replacement, 0);
                      co_await native.close_checked();
                      const bool still_open = ::fcntl(replacement, F_GETFD)
                                              != -1;
                      watched_fd = -1;
                      ::close(replacement);
                      BOOST_CHECK(still_open);
                  }
                  watched_fd = -1;
                  BOOST_CHECK_EQUAL(close_calls.load(), 1U);
              }
          }
      });
}

SEASTAR_TEST_CASE(native_directory_factory_allocation_failure_is_exceptional) {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    co_await seastar::tmp_dir::do_with(
      kwaque::runtime::testing::test_directory_template(),
      [](auto& dir) -> seastar::future<> {
          int fd = ::open(dir.get_path().c_str(), O_RDONLY | O_DIRECTORY);
          BOOST_REQUIRE_GE(fd, 0);
          struct stat status{};
          BOOST_REQUIRE_EQUAL(::fstat(fd, &status), 0);
          auto& injector = seastar::memory::local_failure_injector();
          injector.fail_after(0);
          auto opening = seastar::make_file_impl(fd, {}, O_RDONLY, status);
          const bool injected = injector.failed();
          injector.cancel();
          bool failed = false;
          try {
              static_cast<void>(co_await std::move(opening));
          } catch (const std::bad_alloc&) {
              failed = true;
          }
          BOOST_CHECK(injected && failed);
          BOOST_CHECK_EQUAL(::fcntl(fd, F_GETFD), -1);
          BOOST_CHECK_EQUAL(errno, EBADF);
      });
#endif
    co_return;
}

SEASTAR_TEST_CASE(
  native_checked_close_submission_oom_still_releases_the_descriptor) {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    co_await seastar::tmp_dir::do_with(
      kwaque::runtime::testing::test_directory_template(),
      [](auto& dir) -> seastar::future<> {
          for (const bool checked : {false, true}) {
              int fd = ::open(
                (dir.get_path() / "submit").c_str(), O_RDWR | O_CREAT, 0600);
              BOOST_REQUIRE_GE(fd, 0);
              auto guard = seastar::defer([fd] { ::close(fd); });
              struct stat status{};
              BOOST_REQUIRE_EQUAL(::fstat(fd, &status), 0);
              seastar::file_open_options options;
              options.append_is_unlikely = true;
              guard.cancel();
              auto impl = co_await seastar::make_file_impl(
                fd, options, O_RDWR, status);
              seastar::file file{std::move(impl)};
              close_calls = 0;
              close_errno = 0;
              watched_fd = fd;
              auto unwatch = seastar::defer([] { watched_fd = -1; });
              auto& injector = seastar::memory::local_failure_injector();
              injector.fail_after(0);
              auto closing = checked ? file.close_checked() : file.close();
              const bool injected = injector.failed();
              injector.cancel();
              bool failed = false;
              try {
                  co_await std::move(closing);
              } catch (const std::bad_alloc&) {
                  failed = true;
              }
              watched_fd = -1;
              BOOST_CHECK(injected);
              BOOST_CHECK_EQUAL(failed, checked);
              BOOST_CHECK_EQUAL(close_calls.load(), 1U);
              BOOST_CHECK_EQUAL(::fcntl(fd, F_GETFD), -1);
              BOOST_CHECK_EQUAL(errno, EBADF);
          }
      });
#endif
    co_return;
}

SEASTAR_TEST_CASE(
  native_regular_factory_allocation_failure_releases_owned_descriptor) {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    co_await seastar::tmp_dir::do_with(
      kwaque::runtime::testing::test_directory_template(),
      [](auto& dir) -> seastar::future<> {
          bool saw_failure = false, reached_success = false;
          for (unsigned allocation = 0; allocation < 256 && !reached_success;
               ++allocation) {
              int fd = ::open(
                (dir.get_path() / "construct").c_str(), O_RDWR | O_CREAT, 0600);
              BOOST_REQUIRE_GE(fd, 0);
              struct stat status{};
              BOOST_REQUIRE_EQUAL(::fstat(fd, &status), 0);
              watched_fd = fd;
              auto unwatch = seastar::defer([] { watched_fd = -1; });
              close_calls = 0;
              close_errno = 0;
              truncate_errno = 0;
              auto& injector = seastar::memory::local_failure_injector();
              injector.fail_after(allocation);
              seastar::shared_ptr<seastar::file_impl> impl;
              try {
                  impl = co_await seastar::make_file_impl(
                    fd, {}, O_RDWR, status);
              } catch (const std::bad_alloc&) {
                  saw_failure = true;
              }
              const bool injected = injector.failed();
              injector.cancel();
              if (impl) {
                  // Successful publication transferred destructor ownership.
                  impl = {};
                  reached_success = !injected;
              } else {
                  BOOST_CHECK(injected);
              }
              BOOST_CHECK_EQUAL(close_calls.load(), 1U);
              BOOST_CHECK_EQUAL(::fcntl(fd, F_GETFD), -1);
              BOOST_CHECK_EQUAL(errno, EBADF);
          }
          BOOST_CHECK(saw_failure && reached_success);
      });
#endif
    co_return;
}

SEASTAR_TEST_CASE(
  native_partial_construction_failure_releases_the_borrowed_fd_once) {
    int descriptors[2];
    BOOST_REQUIRE_EQUAL(::pipe(descriptors), 0);
    auto writer = seastar::defer([fd = descriptors[1]] { ::close(fd); });
    watched_fd = descriptors[0];
    auto unwatch = seastar::defer([] { watched_fd = -1; });
    close_errno = 0;
    truncate_errno = 0;
    close_calls = 0;
    bool failed = false;
    try {
        // An append implementation's seek fails after its POSIX base was
        // constructed. That base must leave release to the factory guard.
        auto impl = seastar::testing::make_append_challenged_posix_file(
          descriptors[0], 0, true, std::nullopt);
    } catch (const std::system_error& error) {
        failed = error.code()
                 == std::error_code(ESPIPE, std::system_category());
    }
    BOOST_CHECK(failed);
    BOOST_CHECK_EQUAL(close_calls.load(), 1U);
    BOOST_CHECK_EQUAL(::fcntl(descriptors[0], F_GETFD), -1);
    BOOST_CHECK_EQUAL(errno, EBADF);
    co_return;
}
