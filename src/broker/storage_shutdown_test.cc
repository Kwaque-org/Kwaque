#include "src/broker/application_internal.h"
#include "src/broker/application_test_support.h"
#include "src/runtime/cross_shard.h"
#include "src/runtime/production/environment.h"
#include "src/runtime/testing/test_directory.h"
#include "src/storage/tests/completion_contract.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/net/api.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/tmp_file.hh>

#include <boost/test/unit_test.hpp>

#include <cstdint>
#include <exception>
#include <filesystem>
#include <memory>
#include <optional>
#include <string_view>
#include <type_traits>

namespace kwaque::broker::test_types {
struct storage_start_request final {
    std::int64_t fail_at;
};
} // namespace kwaque::broker::test_types

template<>
struct kwaque::runtime::enable_cross_shard_value<
  kwaque::broker::test_types::storage_start_request> : std::true_type {};

namespace {
using namespace kwaque;
using app_type = broker::detail::application_state;
using access = broker::detail::application_test_access;
using backend = runtime::production::environment;
using broker::test_types::storage_start_request;
using consumer = storage::testing::completion_contract<backend>;
using storage::testing::direct_driver;
using storage::testing::require;
using storage::testing::take;

thread_local std::unique_ptr<consumer> current_storage;
thread_local std::optional<seastar::tmp_dir> current_directory;
thread_local bool directory_created = false;
thread_local bool storage_started = false;

std::uint16_t available_port() {
    seastar::listen_options options;
    options.reuse_address = false;
    auto listener = seastar::listen(
      seastar::socket_address{seastar::net::inet_address{"127.0.0.1"}, 0},
      options);
    const auto port = listener.local_address().port();
    listener.abort_accept();
    return port;
}

seastar::future<>
start_storage(backend& environment, storage_start_request request) {
    const auto fail_at = request.fail_at;
    current_directory.emplace();
    co_await current_directory->create(
      runtime::testing::test_directory_template());
    directory_created = true;
    current_storage = std::make_unique<consumer>(
      environment,
      take(
        runtime::file_path::make(
          (current_directory->get_path() / "accepted").string())));
    const auto checkpoint = fail_at < 0 ? std::optional<std::size_t>{}
                                        : std::optional<std::size_t>{
                                            static_cast<std::size_t>(fail_at)};
    co_await current_storage->start(environment, direct_driver{}, checkpoint);
    storage_started = true;
}

seastar::future<> stop_storage(backend& environment) {
    std::exception_ptr first;
    if (current_storage) {
        try {
            require(
              environment.abort_requested(),
              "broker cleanup preceded runtime abort");
            if (storage_started)
                current_storage->check_early_abort(environment);
        } catch (...) {
            first = std::current_exception();
        }
        try {
            take(co_await current_storage->stop());
            current_storage->check_stopped(storage_started);
            if (storage_started)
                co_await current_storage->verify_contents(direct_driver{});
        } catch (...) {
            if (!first) first = std::current_exception();
        }
        current_storage.reset();
    }
    storage_started = false;
    if (directory_created) {
        try {
            co_await current_directory->remove();
        } catch (...) {
            if (!first) first = std::current_exception();
        }
    }
    current_directory.reset();
    directory_created = false;
    if (first) std::rethrow_exception(first);
}

seastar::future<> exercise(std::int64_t fail_at) {
    seastar::tmp_dir root;
    co_await root.create(runtime::testing::test_directory_template());
    app_type app;
    config::bootstrap_config config;
    config.data_directory = root.get_path();
    config.admin_port = available_port();
    config.diagnostic_memory_per_shard_bytes = 192U * 1024U * 1024U;
    access::configure(app, std::move(config));
    app.construct_services(false);
    bool failed = false;
    std::exception_ptr first;
    try {
        co_await app.start_services();
        co_await access::start_dependent_service(
          app,
          [&app, fail_at] {
              return access::on_environments(
                app, start_storage, storage_start_request{fail_at});
          },
          [&app] { return access::on_environments(app, stop_storage); });
    } catch (const std::runtime_error& error) {
        if (
          std::string_view{error.what()} == "injected storage startup failure")
            failed = true;
        else
            first = std::current_exception();
    } catch (...) {
        first = std::current_exception();
    }
    try {
        co_await app.shutdown();
    } catch (...) {
        if (!first) first = std::current_exception();
    }
    BOOST_CHECK_EQUAL(failed, fail_at >= 0);
    BOOST_CHECK(access::services_released(app));
    co_await root.remove();
    if (first) std::rethrow_exception(first);
}
} // namespace

SEASTAR_TEST_CASE(
  broker_storage_completion_drains_under_early_abort_and_pressure) {
    co_await exercise(-1);
}

SEASTAR_TEST_CASE(
  broker_storage_partial_start_uses_registered_reverse_cleanup) {
    for (std::size_t point = 0; point < consumer::start_boundaries; ++point)
        co_await exercise(static_cast<std::int64_t>(point));
}
