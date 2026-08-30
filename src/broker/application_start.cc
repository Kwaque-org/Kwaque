#include "src/base/build_info.h"
#include "src/base/logging.h"
#include "src/broker/application_internal.h"
#include "src/broker/data_directory.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/map_reduce.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/smp.hh>

#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <stdexcept>
#include <string>

namespace kwaque::broker::detail {

seastar::future<> application_state::start_services() {
    assert_owner();
    if (!configuration_ || !services_constructed()) {
        throw std::logic_error(
          "configuration and services must be ready before startup");
    }

    log::broker().info("build {}", build_info::version_line());
    const unsigned shard_count = seastar::this_smp_shard_count();
    const std::uint64_t total_memory = co_await seastar::map_reduce(
      seastar::this_smp_all_shards(),
      [](unsigned shard) {
          return seastar::smp::submit_to(shard, [] {
              return static_cast<std::uint64_t>(
                seastar::memory::stats().total_memory());
          });
      },
      std::uint64_t{0},
      std::plus<>{});
    log::broker().info(
      "runtime shards={} memory_bytes={} reactor_backend={}",
      shard_count,
      total_memory,
      seastar::engine().get_backend_name());

    co_await lifecycle_->start_step(
      [this] { return prepare_data_directory(configuration_->data_directory); },
      [] { return seastar::make_ready_future<>(); });
    log::broker().info("startup stage=data_directory state=ready");

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

    co_await lifecycle_->start_step(
      [this] { return runtime_service_->start(); },
      [this] { return runtime_service_->stop(); });

    co_await runtime_service_->invoke_on_all(
      [](runtime::runtime_service& service) {
          log::broker().info("runtime service ready shard={}", service.shard());
      });
    log::broker().info("startup stage=runtime_service state=ready");

    co_await lifecycle_->start_step(
      [this] {
          return runtime::production::start_backends(
            *production_backends_, *runtime_service_);
      },
      [this] { return production_backends_->stop(); });
    log::broker().info("startup stage=runtime_backend state=ready");

    co_await lifecycle_->start_step(
      [this] {
          return admin_server_->start(
            configuration_->admin_address,
            configuration_->admin_port,
            seastar::this_smp_shard_count());
      },
      [this] { return admin_server_->stop(); });
    co_await admin_server_->mark_ready(
      std::chrono::steady_clock::now() - startup_started_at_);
    log::broker().info(
      "startup stage=admin state=ready address={} port={}",
      configuration_->admin_address,
      configuration_->admin_port);
}

int application_state::execute(
  const boost::program_options::variables_map& options) {
    capture_or_assert_owner();
    int exit_code = 1;
    startup_started_at_ = std::chrono::steady_clock::now();
    try {
        load_configuration(options).get();
        construct_services();
        start_services().get();
        stop_signal_->wait().get();
        admin_server_->begin_shutdown().get();
        request_service_abort().get();
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
        if (admin_server_) {
            admin_server_->begin_shutdown().get();
        }
        request_service_abort().get();
        shutdown().get();
    } catch (const std::exception& error) {
        log::broker().error("broker shutdown failure: {}", error.what());
    } catch (...) {
        log::broker().error("broker shutdown failure: unknown exception");
    }
    return exit_code;
}

} // namespace kwaque::broker::detail
