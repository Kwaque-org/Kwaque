#include "src/broker/application_internal.h"
#include "src/broker/application_test_support.h"
#include "src/broker/service_lifecycle.h"

#include <seastar/core/abort_source.hh>
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

#include <array>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {

seastar::future<> ready() { return seastar::make_ready_future<>(); }

std::uint16_t reserve_loopback_port() {
    seastar::listen_options options;
    options.reuse_address = false;
    auto listener = seastar::listen(
      seastar::socket_address{seastar::net::inet_address{"127.0.0.1"}, 0},
      options);
    const auto port = listener.local_address().port();
    listener.abort_accept();
    return port;
}

void require_loopback_port_available(std::uint16_t port) {
    seastar::listen_options options;
    options.reuse_address = false;
    auto listener = seastar::listen(
      seastar::socket_address{seastar::net::inet_address{"127.0.0.1"}, port},
      options);
    listener.abort_accept();
}

bool metric_registered(const char* name) {
    return seastar::metrics::impl::get_value_map().contains(
      seastar::sstring{name});
}

void require_no_application_metrics() {
    for (const auto name : {
           "broker_process_readiness",
           "broker_http_requests_total",
           "runtime_task_active",
           "runtime_timer_active",
           "resource_manager_memory_configured_bytes",
         }) {
        BOOST_CHECK(!metric_registered(name));
    }
}

kwaque::config::bootstrap_config
application_config(std::filesystem::path data_directory, std::uint16_t port) {
    auto result = kwaque::config::bootstrap_config{};
    result.data_directory = std::move(data_directory);
    result.admin_port = port;
    return result;
}

} // namespace

SEASTAR_TEST_CASE(resource_configuration_uses_smallest_shard_allocator) {
    constexpr std::uint64_t mebibyte{1'024U * 1'024U};
    const std::array observed{
      kwaque::byte_count{96U * mebibyte},
      kwaque::byte_count{64U * mebibyte},
      kwaque::byte_count{80U * mebibyte},
    };
    auto minimum = kwaque::byte_count{
      std::numeric_limits<std::uint64_t>::max()};
    for (const auto shard_memory : observed) {
        minimum = kwaque::broker::detail::reduce_minimum_shard_memory(
          minimum, shard_memory);
    }

    const auto configured = kwaque::broker::detail::production_resource_config(
      minimum);
    BOOST_CHECK_EQUAL(configured.total_memory().value(), 64U * mebibyte);
    BOOST_CHECK_EQUAL(
      configured.reactor_headroom().value(),
      kwaque::resource::resource_config::default_reactor_headroom().value());
    co_return;
}

SEASTAR_TEST_CASE(application_rolls_back_every_global_start_boundary) {
    seastar::tmp_dir root;
    co_await root.create(
      std::filesystem::temp_directory_path()
      / "kwaque-application-checkpoints-XXXXXX");

    for (std::size_t point = 0; point < kwaque::broker::detail::
                                  application_test_access::start_boundary_count;
         ++point) {
        const auto port = reserve_loopback_port();
        const auto data_directory = root.get_path()
                                    / ("boundary-" + std::to_string(point));
        kwaque::broker::detail::application_state target;
        kwaque::broker::detail::application_test_access::configure(
          target, application_config(data_directory, port));
        target.construct_services(false);

        bool injected = false;
        try {
            co_await kwaque::broker::detail::application_test_access::
              fail_at_start_boundary(target, point);
        } catch (
          const kwaque::broker::detail::application_start_checkpoint_failure&) {
            injected = true;
        }
        BOOST_REQUIRE(injected);
        BOOST_CHECK(
          kwaque::broker::detail::application_test_access::services_released(
            target));
        BOOST_CHECK(!target.services_constructed());
        BOOST_CHECK(!std::filesystem::exists(data_directory / "kwaque.pid"));
        require_loopback_port_available(port);
        require_no_application_metrics();
    }

    const auto port = reserve_loopback_port();
    const auto data_directory = root.get_path() / "successful-replacement";
    kwaque::broker::detail::application_state replacement;
    kwaque::broker::detail::application_test_access::configure(
      replacement, application_config(data_directory, port));
    replacement.construct_services(false);
    co_await replacement.start_services();
    BOOST_CHECK(replacement.runtime_started());
    co_await replacement.shutdown();
    BOOST_CHECK(
      kwaque::broker::detail::application_test_access::services_released(
        replacement));
    BOOST_CHECK(!std::filesystem::exists(data_directory / "kwaque.pid"));
    require_loopback_port_available(port);
    require_no_application_metrics();

    co_await root.remove();
}

SEASTAR_TEST_CASE(lifecycle_starts_in_order_and_stops_in_reverse) {
    seastar::abort_source abort_source;
    kwaque::broker::service_lifecycle lifecycle(abort_source);
    std::vector<std::string> events;

    co_await lifecycle.start_step(
      [&events] {
          events.emplace_back("start:first");
          return ready();
      },
      [&events] {
          events.emplace_back("stop:first");
          return ready();
      });
    co_await lifecycle.start_step(
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
          [&events] {
              events.emplace_back("start:second");
              return seastar::make_exception_future<>(
                std::runtime_error("injected startup failure"));
          },
          [&events] {
              events.emplace_back("stop:second");
              return ready();
          });
    } catch (const std::runtime_error&) {
        failed = true;
    }

    BOOST_REQUIRE(failed);
    const std::vector<std::string> expected{
      "start:first", "start:second", "stop:second", "stop:first"};
    BOOST_CHECK(events == expected);
    BOOST_CHECK_EQUAL(lifecycle.running_steps(), 0U);
}

SEASTAR_TEST_CASE(lifecycle_stop_is_idempotent_while_cleanup_is_pending) {
    seastar::abort_source abort_source;
    kwaque::broker::service_lifecycle lifecycle(abort_source);
    seastar::promise<> release;
    unsigned stops = 0;

    co_await lifecycle.start_step(
      [] { return ready(); },
      [&release, &stops] -> seastar::future<> {
          ++stops;
          co_await release.get_future();
      });
    auto first_stop = lifecycle.stop();
    auto second_stop = lifecycle.stop();
    co_await seastar::yield();
    BOOST_CHECK(!first_stop.available());
    BOOST_CHECK(!second_stop.available());
    BOOST_CHECK_EQUAL(stops, 1U);

    release.set_value();
    co_await std::move(first_stop);
    co_await std::move(second_stop);
    BOOST_CHECK(
      lifecycle.state() == kwaque::broker::service_lifecycle_state::stopped);
}

SEASTAR_TEST_CASE(lifecycle_rolls_back_when_startup_is_interrupted) {
    seastar::abort_source abort_source;
    kwaque::broker::service_lifecycle lifecycle(abort_source);
    std::vector<std::string> events;

    co_await lifecycle.start_step(
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
          [&events] {
              events.emplace_back("start:second");
              return ready();
          },
          [&events] {
              events.emplace_back("stop:second");
              return ready();
          });
    } catch (const seastar::abort_requested_exception&) {
        interrupted = true;
    }

    BOOST_REQUIRE(interrupted);
    const std::vector<std::string> expected{"start:first", "stop:first"};
    BOOST_CHECK(events == expected);
    BOOST_CHECK_EQUAL(lifecycle.running_steps(), 0U);
}

SEASTAR_TEST_CASE(lifecycle_preserves_first_stop_failure_and_finishes_cleanup) {
    seastar::abort_source abort_source;
    kwaque::broker::service_lifecycle lifecycle(abort_source);
    std::vector<std::string> events;

    co_await lifecycle.start_step(
      [] { return ready(); },
      [&events] {
          events.emplace_back("stop:first");
          return ready();
      });
    co_await lifecycle.start_step(
      [] { return ready(); },
      [&events] {
          events.emplace_back("stop:second");
          return seastar::make_exception_future<>(
            std::runtime_error("injected stop failure"));
      });

    for (unsigned attempt = 0; attempt < 2; ++attempt) {
        bool failed = false;
        try {
            co_await lifecycle.stop();
        } catch (const std::runtime_error&) {
            failed = true;
        }
        BOOST_REQUIRE(failed);
    }

    const std::vector<std::string> expected{"stop:second", "stop:first"};
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
    co_await state.shutdown();
}
