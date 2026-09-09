#include "src/base/build_info.h"
#include "src/base/invariant.h"
#include "src/base/logging.h"
#include "src/base/units.h"
#include "src/broker/application_internal.h"
#include "src/broker/data_directory.h"
#include "src/observability/event_identity.h"
#include "src/resource/resource_config.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/map_reduce.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/smp.hh>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace kwaque::broker::detail {

resource::resource_config
production_resource_config(byte_count minimum_shard_memory) {
    auto configured = resource::resource_config::from_total_memory(
      minimum_shard_memory);
    if (!configured) {
        throw std::system_error(configured.error());
    }
    return *configured;
}

namespace {

observability::event_sink_identity production_event_identity() {
    auto epoch = observability::event_sink_epoch::make(1);
    if (!epoch) {
        throw std::system_error(make_error_code(epoch.error().code()));
    }
    return observability::event_sink_identity{
      .epoch = *epoch,
      .configuration_digest = {},
    };
}

} // namespace

seastar::future<byte_count> application_state::observe_minimum_shard_memory() {
    log::broker().info("build {}", build_info::version_line());
    const unsigned shard_count = seastar::this_smp_shard_count();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-BROKER-REACTOR-SHARDS"},
      shard_count != 0,
      "running reactor has no shards");
    const byte_count minimum_shard_memory = co_await seastar::map_reduce(
      seastar::this_smp_all_shards(),
      [](unsigned shard) {
          return seastar::smp::submit_to(shard, [] {
              return byte_count{static_cast<std::uint64_t>(
                seastar::memory::stats().total_memory())};
          });
      },
      byte_count{std::numeric_limits<std::uint64_t>::max()},
      reduce_minimum_shard_memory);
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    constexpr std::string_view allocator_stats = "synthetic";
#else
    constexpr std::string_view allocator_stats = "native";
#endif
    log::broker().info(
      "runtime shards={} minimum_shard_memory_bytes={} reactor_backend={} "
      "allocator_stats={}",
      shard_count,
      minimum_shard_memory.value(),
      seastar::engine().get_backend_name(),
      allocator_stats);
    co_return minimum_shard_memory;
}

seastar::future<> application_state::start_data_directory() {
    co_await lifecycle_->start_step(
      [this] {
          return prepare_data_directory(
            configuration_->data_directory, &stop_signal_->abort_source());
      },
      [] { return seastar::make_ready_future<>(); });
    log::broker().info("startup stage=data_directory state=ready");
}

seastar::future<> application_state::start_pid_file() {
    co_await lifecycle_->start_step(
      [this] {
          pid_file_ = std::make_unique<pid_file>(
            configuration_->data_directory / "kwaque.pid");
          return seastar::make_ready_future<>();
      },
      [this] {
          pid_file_.reset();
          return seastar::make_ready_future<>();
      });
    log::broker().info("startup stage=pid_file state=ready");
}

seastar::future<> application_state::start_resource_registry(
  resource::resource_config configuration) {
    co_await lifecycle_->start_step(
      [this, configuration = std::move(configuration)] {
          return resource_registry_->start(configuration);
      },
      [this] { return resource_registry_->stop(); });
    log::broker().info("startup stage=resource_registry state=ready");
}

seastar::future<> application_state::start_environments() {
    co_await lifecycle_->start_step(
      [this] {
          return environments_->start(
            runtime::production::environment_dependencies{
              resource_registry_->handles(),
              log::broker(),
              production_event_identity()});
      },
      [this] { return environments_->stop(); });
    co_await environments_->invoke_on_all(
      [](runtime::production::environment&) {
          log::broker().info(
            "runtime environment ready shard={}", seastar::this_shard_id());
      });
    log::broker().info("startup stage=runtime_environment state=ready");
}

seastar::future<> application_state::start_admin() {
    co_await lifecycle_->start_step(
      [this] {
          return admin_server_->start(
            configuration_->admin_address,
            configuration_->admin_port,
            seastar::this_smp_shard_count(),
            &stop_signal_->abort_source());
      },
      [this] { return admin_server_->stop(); });
    stop_signal_->abort_source().check();
    co_await admin_server_->mark_ready(
      std::chrono::steady_clock::now() - startup_started_at_);
    stop_signal_->abort_source().check();
    log::broker().info(
      "startup stage=admin state=ready address={} port={}",
      configuration_->admin_address,
      configuration_->admin_port);
}

seastar::future<> application_state::start_services() {
    return start_services_with([](std::size_t) noexcept {});
}

seastar::future<> application_state::start_services(
  const seastar::app_template::seastar_options& runtime_options) {
    return start_services_with([](std::size_t) noexcept {}, &runtime_options);
}

void application_state::freeze_startup_policy(
  const seastar::app_template::seastar_options& runtime_options,
  const resource::resource_config& resources) {
    if (!configuration_ || !configuration_identity_ || startup_policy_) {
        throw std::logic_error(
          "startup policy requires one loaded configuration");
    }
    startup_policy_ = render_startup_policy(
      runtime_options,
      *configuration_,
      *configuration_identity_,
      resources,
      seastar::this_smp_shard_count(),
      seastar::engine().get_backend_name());
    log::broker().info("startup policy {}", *startup_policy_);
    if (!admin_is_loopback(configuration_->admin_address)) {
        const std::array address{config::config_value{
          "address",
          configuration_->admin_address,
          config::config_visibility::safe}};
        log::broker().warn(
          "admin API is exposed without authentication or TLS when the "
          "configured non-loopback listener starts: {} port={}",
          config::render_config(address),
          configuration_->admin_port);
    }
}

int application_state::execute(
  const boost::program_options::variables_map& options,
  const seastar::app_template::seastar_options& runtime_options) {
    capture_or_assert_owner();
    int exit_code = 1;
    startup_started_at_ = std::chrono::steady_clock::now();
    try {
        initialize_stop_signal();
        load_configuration(options).get();
        stop_signal_->abort_source().check();
        validate_broker_profile(*configuration_);
        construct_services();
        start_services(runtime_options).get();
        stop_signal_->wait().get();
        log::broker().info("shutdown requested");
        shutdown().get();
        log::broker().info("shutdown complete");
        return 0;
    } catch (const seastar::abort_requested_exception&) {
        log::broker().info("startup interrupted; rolling back");
        exit_code = 0;
    } catch (const std::exception& error) {
        log::broker().error("broker failure: {}", error.what());
    } catch (...) {
        log::broker().error("broker failure: unknown exception");
    }

    try {
        shutdown().get();
    } catch (const std::exception& error) {
        log::broker().error("broker shutdown failure: {}", error.what());
    } catch (...) {
        log::broker().error("broker shutdown failure: unknown exception");
    }
    return exit_code;
}

} // namespace kwaque::broker::detail
