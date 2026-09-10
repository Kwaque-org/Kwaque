#include "src/broker/application_internal.h"
#include "src/broker/application_test_support.h"
#include "src/broker/pid_file.h"
#include "src/runtime/production/environment.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/metrics_api.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/api.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/later.hh>
#include <seastar/util/tmp_file.hh>

#include <boost/test/unit_test.hpp>

#include <cstdint>
#include <exception>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <utility>

namespace {

using kwaque::broker::detail::application_state;
using access = kwaque::broker::detail::application_test_access;

std::uint16_t available_port() {
    seastar::listen_options options;
    options.reuse_address = false;
    auto socket = seastar::listen(
      seastar::socket_address{seastar::net::inet_address{"127.0.0.1"}, 0},
      options);
    const auto port = socket.local_address().port();
    socket.abort_accept();
    return port;
}

kwaque::config::bootstrap_config
configuration(const std::filesystem::path& directory, std::uint16_t port) {
    kwaque::config::bootstrap_config result;
    result.data_directory = directory;
    result.admin_port = port;
    result.diagnostic_memory_per_shard_bytes = 192U * 1024U * 1024U;
    return result;
}

void require_aborted(kwaque::runtime::production::environment& environment) {
    if (
      !environment.abort_requested() || !environment.tasks().admission_closed()
      || !environment.tasks().abort_requested()
      || environment.lifetime().acquire()) {
        throw std::runtime_error(
          "runtime admission must close before cancellation");
    }
}

seastar::future<std::string>
health_response(std::uint16_t port, std::string path) {
    auto connection = co_await seastar::connect(
      seastar::socket_address{seastar::net::inet_address{"127.0.0.1"}, port});
    auto input = connection.input();
    auto output = connection.output();
    const auto request
      = "GET " + path
        + " HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    co_await output.write(request);
    co_await output.flush();
    std::string response;
    while (auto bytes = co_await input.read_up_to(4096)) {
        response.append(bytes.get(), bytes.size());
        if (response.size() > 16384) {
            throw std::runtime_error("health response exceeded test bound");
        }
    }
    co_await output.close();
    co_await input.close();
    co_return response;
}

} // namespace

SEASTAR_TEST_CASE(
  application_cancellation_preserves_unclean_start_at_each_boundary) {
    seastar::tmp_dir root;
    co_await root.create(
      std::filesystem::temp_directory_path() / "kwaque-stop-boundaries-XXXXXX");
    for (std::size_t point = 0; point < access::start_boundary_count; ++point) {
        const auto path = root.get_path() / std::to_string(point);
        application_state app;
        access::configure(app, configuration(path, available_port()));
        app.construct_services(false);
        bool cancelled = false;
        try {
            co_await access::stop_at_start_boundary(app, point);
        } catch (const seastar::abort_requested_exception&) {
            cancelled = true;
        }
        BOOST_CHECK(cancelled);
        BOOST_CHECK(access::services_released(app));
        BOOST_CHECK(!std::filesystem::exists(path / "kwaque.pid"));
        BOOST_CHECK_EQUAL(
          std::filesystem::exists(path / ".kwaque-crash-loop"), point >= 3);
    }
    co_await root.remove();
}

SEASTAR_TEST_CASE(
  application_abort_does_not_wait_for_readiness_acknowledgements) {
    seastar::tmp_dir root;
    co_await root.create(
      std::filesystem::temp_directory_path() / "kwaque-readiness-drain-XXXXXX");
    const auto port = available_port();
    application_state app;
    access::configure(app, configuration(root.get_path(), port));
    app.construct_services(false);
    co_await app.start_services();
    seastar::promise<> release;
    seastar::promise<> entered;
    auto stopping = access::shutdown_with_readiness(
      app, [&] -> seastar::future<> {
          co_await access::begin_admin_shutdown(app);
          entered.set_value();
          co_await release.get_future();
      });
    co_await entered.get_future();
    co_await access::on_environments(app, require_aborted);
    BOOST_CHECK(!stopping.available());
    BOOST_CHECK_THROW(
      kwaque::broker::pid_file{root.get_path() / "kwaque.pid"},
      kwaque::broker::pid_file_locked);
    for (const auto* route : {"/v1/health/ready", "/v1/health/live"}) {
        const auto response = co_await health_response(port, route);
        BOOST_CHECK(response.starts_with("HTTP/1.1 503"));
    }
    BOOST_CHECK(
      seastar::metrics::impl::get_value_map().contains(
        seastar::sstring{"runtime_task_active"}));
    auto also_stopping = app.shutdown();
    BOOST_CHECK(!also_stopping.available());
    release.set_value();
    co_await std::move(stopping);
    co_await std::move(also_stopping);
    BOOST_CHECK(access::services_released(app));
    BOOST_CHECK(
      !std::filesystem::exists(root.get_path() / ".kwaque-crash-loop"));
    BOOST_CHECK(!seastar::metrics::impl::get_value_map().contains(
      seastar::sstring{"runtime_task_active"}));
    co_await root.remove();
}

SEASTAR_TEST_CASE(application_abort_precedes_parked_dependent_cleanup) {
    seastar::tmp_dir root;
    co_await root.create(
      std::filesystem::temp_directory_path() / "kwaque-parked-drain-XXXXXX");
    application_state app;
    access::configure(app, configuration(root.get_path(), available_port()));
    app.construct_services(false);
    co_await app.start_services();
    seastar::promise<> release;
    seastar::promise<> entered;
    co_await access::add_cleanup(app, [&] -> seastar::future<> {
        entered.set_value();
        co_await release.get_future();
    });
    auto stopping = app.shutdown();
    co_await entered.get_future();
    co_await access::on_environments(app, require_aborted);
    BOOST_CHECK(
      std::filesystem::exists(root.get_path() / ".kwaque-crash-loop"));
    release.set_value();
    co_await std::move(stopping);
    BOOST_CHECK(
      !std::filesystem::exists(root.get_path() / ".kwaque-crash-loop"));
    co_await root.remove();
}

SEASTAR_TEST_CASE(
  application_failed_readiness_and_cleanup_keep_first_failure_and_restart_evidence) {
    seastar::tmp_dir root;
    co_await root.create(
      std::filesystem::temp_directory_path() / "kwaque-failed-drain-XXXXXX");
    application_state app;
    access::configure(app, configuration(root.get_path(), available_port()));
    app.construct_services(false);
    co_await app.start_services();
    bool cleaned = false;
    co_await access::add_cleanup(app, [&] -> seastar::future<> {
        co_await access::on_environments(app, require_aborted);
        cleaned = true;
        throw std::runtime_error("later cleanup failure");
    });
    const auto expected = std::make_exception_ptr(
      std::runtime_error("first readiness failure"));
    for (unsigned attempt = 0; attempt < 2; ++attempt) {
        auto stopping = attempt == 0
                          ? access::shutdown_with_readiness(
                              app,
                              [&] -> seastar::future<> {
                                  co_await access::begin_admin_shutdown(app);
                                  std::rethrow_exception(expected);
                              })
                          : app.shutdown();
        try {
            co_await std::move(stopping);
            BOOST_FAIL("shutdown must report the first failure");
        } catch (...) {
            BOOST_CHECK(std::current_exception() == expected);
        }
    }
    BOOST_CHECK(cleaned);
    BOOST_CHECK(access::shutdown_failed(app));
    BOOST_CHECK(access::services_released(app));
    BOOST_CHECK(
      std::filesystem::exists(root.get_path() / ".kwaque-crash-loop"));
    BOOST_CHECK(!std::filesystem::exists(root.get_path() / "kwaque.pid"));
    co_await root.remove();
}

SEASTAR_TEST_CASE(application_concurrent_stop_waits_for_startup_rollback) {
    seastar::tmp_dir root;
    co_await root.create(
      std::filesystem::temp_directory_path() / "kwaque-start-stop-XXXXXX");
    application_state app;
    access::configure(app, configuration(root.get_path(), available_port()));
    app.construct_services(false);
    auto starting = app.start_services();
    BOOST_CHECK(!starting.available());
    auto stopping = app.shutdown();
    auto also_stopping = app.shutdown();
    bool cancelled = false;
    try {
        co_await std::move(starting);
    } catch (const seastar::abort_requested_exception&) {
        cancelled = true;
    }
    co_await std::move(stopping);
    co_await std::move(also_stopping);
    BOOST_CHECK(cancelled);
    BOOST_CHECK(access::services_released(app));
    BOOST_CHECK(!access::shutdown_failed(app));
    co_await root.remove();
}

SEASTAR_TEST_CASE(
  application_startup_cancellation_does_not_hide_cleanup_failure) {
    seastar::tmp_dir root;
    co_await root.create(
      std::filesystem::temp_directory_path()
      / "kwaque-cancel-cleanup-failure-XXXXXX");
    application_state app;
    access::configure(app, configuration(root.get_path(), available_port()));
    app.construct_services(false);
    const auto expected = std::make_exception_ptr(
      std::runtime_error("mandatory cleanup failed"));
    co_await access::add_cleanup(
      app, [expected] { return seastar::make_exception_future<>(expected); });
    bool cancelled = false;
    try {
        co_await access::stop_at_start_boundary(
          app, access::start_boundary_count - 1);
    } catch (const seastar::abort_requested_exception&) {
        cancelled = true;
    }
    BOOST_CHECK(cancelled);
    BOOST_CHECK(access::shutdown_failed(app));
    BOOST_CHECK(access::services_released(app));
    try {
        co_await app.shutdown();
        BOOST_FAIL(
          "cleanup failure must remain visible after startup cancellation");
    } catch (...) {
        BOOST_CHECK(std::current_exception() == expected);
    }
    BOOST_CHECK(
      std::filesystem::exists(root.get_path() / ".kwaque-crash-loop"));
    co_await root.remove();
}
