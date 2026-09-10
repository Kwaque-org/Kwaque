#include "src/admin/admin_server.h"

#include "src/admin/admin_responses.h"
#include "src/admin/admin_state.h"
#include "src/base/invariant.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/prometheus.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/net/inet_address.hh>

#include <chrono>
#include <exception>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>

namespace kwaque::admin {

namespace {

using reply_status = seastar::http::reply::status_type;

void put_json_route(
  seastar::httpd::routes& routes,
  seastar::sstring path,
  seastar::httpd::handle_function handler) {
    auto owned_handler = std::make_unique<seastar::httpd::function_handler>(
      std::move(handler), "application/json");
    routes.put(
      seastar::httpd::operation_type::GET,
      std::move(path),
      owned_handler.get());
    static_cast<void>(owned_handler.release());
}

void register_routes(
  seastar::httpd::routes& routes,
  admin_state& state,
  const std::string& version_json) {
    auto* local_state = &state;
    put_json_route(
      routes,
      "/v1/health/live",
      [local_state](seastar::httpd::const_req, seastar::http::reply& reply) {
          local_state->record_request();
          auto response = liveness_response(local_state->live());
          reply.set_status(static_cast<reply_status>(response.status));
          return seastar::sstring(std::move(response.body));
      });
    put_json_route(
      routes,
      "/v1/health/ready",
      [local_state](seastar::httpd::const_req, seastar::http::reply& reply) {
          local_state->record_request();
          auto response = readiness_response(local_state->ready());
          reply.set_status(static_cast<reply_status>(response.status));
          return seastar::sstring(std::move(response.body));
      });
    put_json_route(
      routes,
      "/v1/version",
      [local_state,
       version_json](seastar::httpd::const_req, seastar::http::reply&) {
          local_state->record_request();
          return seastar::sstring(version_json);
      });
}

} // namespace

class admin_server::impl final {
public:
    impl()
      : version_json_(build_info_json(current_build_info())) {}

    enum class lifecycle { constructed, starting, started, stopping, stopped };

    seastar::sharded<admin_state> states_;
    seastar::httpd::http_server_control server_;
    std::string version_json_;
    seastar::shared_promise<> stop_done_;
    seastar::gate operations_;
    lifecycle state_{lifecycle::constructed};
    bool states_started_{false};
    bool server_started_{false};
    bool operation_active_{false};
    bool draining_{false};
    std::optional<seastar::scheduling_group> scheduling_group_;
};

admin_server::admin_server()
  : impl_(std::make_unique<impl>()) {}

seastar::httpd::http_server_control& admin_server::native_server_for_testing() {
    assert_current();
    return impl_->server_;
}

admin_server::~admin_server() {
    assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-ADMIN-SERVER-STOPPED"},
      impl_->state_ == impl::lifecycle::constructed
        || impl_->state_ == impl::lifecycle::stopped,
      "admin server destroyed while active");
}

seastar::future<> admin_server::start(
  std::string address,
  std::uint16_t port,
  unsigned shard_count,
  const seastar::abort_source* startup_abort,
  std::uint64_t reservation_bytes) {
    assert_current();
    const auto check_abort = [startup_abort] {
        if (startup_abort != nullptr) {
            startup_abort->check();
        }
    };
    check_abort();
    if (
      reservation_bytes < admin_reservation_bytes
      || shard_count != seastar::this_smp_shard_count()
      || shard_count > max_scrape_shards) {
        throw std::invalid_argument(
          "admin reservation or scrape shard limit is insufficient");
    }
    if (
      impl_->state_ != impl::lifecycle::constructed
      || impl_->operation_active_) {
        throw std::logic_error("admin server cannot be started");
    }
    impl_->state_ = impl::lifecycle::starting;
    impl_->operation_active_ = true;

    std::exception_ptr startup_failure;
    try {
        impl_->scheduling_group_ = co_await seastar::create_scheduling_group(
          "admin", scheduling_shares);
        check_abort();
        co_await impl_->states_.start();
        impl_->states_started_ = true;
        check_abort();
        co_await impl_->states_.invoke_on_all(
          [](admin_state& state) { state.register_metrics(); });
        check_abort();

        co_await seastar::with_scheduling_group(
          *impl_->scheduling_group_,
          [this] { return impl_->server_.start("kwaque-admin"); });
        impl_->server_started_ = true;
        check_abort();
        const auto group = *impl_->scheduling_group_;
        co_await impl_->server_.server().invoke_on_all(
          [group](seastar::httpd::http_server& server) {
              server.set_request_limits(
                seastar::httpd::request_limits{
                  .connections = connections_per_shard,
                  .request_line_bytes = request_line_bytes,
                  .header_bytes = header_bytes,
                  .header_count = header_count,
                  .header_timeout = header_timeout,
                  .exchange_timeout = exchange_timeout,
                });
              server.set_content_streaming(true);
              server.set_content_length_limit(0);
              server.set_request_scheduling_group(group);
              server.set_keepalive_parameters(
                seastar::net::tcp_keepalive_params{
                  .idle = std::chrono::seconds{120},
                  .interval = std::chrono::seconds{60},
                  .count = 3,
                });
          });
        check_abort();
        auto* states = &impl_->states_;
        const auto version_json = impl_->version_json_;
        co_await impl_->server_.set_routes(
          [states, version_json](seastar::httpd::routes& routes) {
              register_routes(routes, states->local(), version_json);
          });
        check_abort();

        seastar::prometheus::config prometheus_config;
        prometheus_config.prefix = "kwaque";
        prometheus_config.snapshot_bounds.emplace();
        prometheus_config.snapshot_bounds->value_bytes = metrics_snapshot_bytes;
        prometheus_config.max_response_bytes = metrics_response_bytes;
        prometheus_config.max_scrape_shards = max_scrape_shards;
        co_await seastar::prometheus::start(
          impl_->server_, std::move(prometheus_config));
        check_abort();

        seastar::listen_options options;
        options.reuse_address = true;
        co_await seastar::with_scheduling_group(
          *impl_->scheduling_group_, [this, &address, port, options] {
              return impl_->server_.listen(
                seastar::socket_address{
                  seastar::net::inet_address(address), port},
                options);
          });
        check_abort();
        co_await impl_->states_.invoke_on_all(
          [shard_count](admin_state& state) {
              state.listener_started(shard_count);
          });
        check_abort();
    } catch (...) {
        startup_failure = std::current_exception();
    }
    if (startup_failure) {
        if (impl_->server_started_) {
            try {
                co_await impl_->server_.stop();
            } catch (...) {
            }
            impl_->server_started_ = false;
        }
        if (impl_->states_started_) {
            try {
                co_await impl_->states_.stop();
            } catch (...) {
            }
            impl_->states_started_ = false;
        }
        if (impl_->scheduling_group_) {
            try {
                co_await seastar::destroy_scheduling_group(
                  *impl_->scheduling_group_);
            } catch (...) {
            }
            impl_->scheduling_group_.reset();
        }
        impl_->state_ = impl::lifecycle::stopped;
        impl_->operation_active_ = false;
        std::rethrow_exception(startup_failure);
    }
    impl_->state_ = impl::lifecycle::started;
    impl_->operation_active_ = false;
}

seastar::future<>
admin_server::mark_ready(std::chrono::steady_clock::duration startup_duration) {
    assert_current();
    if (impl_->state_ != impl::lifecycle::started || impl_->draining_) {
        return seastar::make_exception_future<>(
          std::logic_error("admin server is not started"));
    }
    const auto seconds
      = std::chrono::duration<double>(startup_duration).count();
    return seastar::with_gate(impl_->operations_, [this, seconds] {
        return impl_->states_.invoke_on_all(
          [seconds](admin_state& state) { state.mark_ready(seconds); });
    });
}

seastar::future<> admin_server::begin_shutdown() {
    assert_current();
    if (!impl_->states_started_ || impl_->state_ != impl::lifecycle::started) {
        return seastar::make_ready_future<>();
    }
    impl_->draining_ = true;
    // Publish local health before waiting for acknowledgements from other
    // shards.
    impl_->states_.local().begin_shutdown();
    return seastar::with_gate(impl_->operations_, [this] {
        return impl_->states_.invoke_on_all(
          [](admin_state& state) { state.begin_shutdown(); });
    });
}

seastar::future<> admin_server::stop() {
    assert_current();
    if (impl_->state_ == impl::lifecycle::stopping) {
        return impl_->stop_done_.get_shared_future();
    }
    if (impl_->state_ == impl::lifecycle::stopped) {
        return impl_->stop_done_.available()
                 ? impl_->stop_done_.get_shared_future()
                 : seastar::make_ready_future<>();
    }
    if (impl_->operation_active_) {
        return seastar::make_exception_future<>(
          std::logic_error("admin server operation is in progress"));
    }
    if (impl_->state_ == impl::lifecycle::constructed) {
        impl_->state_ = impl::lifecycle::stopped;
        return seastar::make_ready_future<>();
    }

    impl_->state_ = impl::lifecycle::stopping;
    auto completion = stop_once().then_wrapped(
      [this](seastar::future<> stopped) noexcept {
          impl_->state_ = impl::lifecycle::stopped;
          try {
              stopped.get();
              impl_->stop_done_.set_value();
          } catch (...) {
              impl_->stop_done_.set_exception(std::current_exception());
          }
      });
    static_cast<void>(completion);
    return impl_->stop_done_.get_shared_future();
}

seastar::future<> admin_server::stop_once() {
    std::exception_ptr failure;
    try {
        if (!impl_->operations_.is_closed()) {
            co_await impl_->operations_.close();
        }
    } catch (...) {
        failure = std::current_exception();
    }
    try {
        co_await impl_->states_.invoke_on_all(
          [](admin_state& state) { state.begin_shutdown(); });
    } catch (...) {
        if (!failure) {
            failure = std::current_exception();
        }
    }
    if (impl_->server_started_) {
        try {
            co_await impl_->server_.stop();
        } catch (...) {
            if (!failure) {
                failure = std::current_exception();
            }
        }
        impl_->server_started_ = false;
    }
    if (impl_->states_started_) {
        try {
            co_await impl_->states_.stop();
        } catch (...) {
            if (!failure) {
                failure = std::current_exception();
            }
        }
        impl_->states_started_ = false;
    }
    if (impl_->scheduling_group_) {
        try {
            co_await seastar::destroy_scheduling_group(
              *impl_->scheduling_group_);
        } catch (...) {
            if (!failure) {
                failure = std::current_exception();
            }
        }
        impl_->scheduling_group_.reset();
    }
    if (failure) {
        std::rethrow_exception(failure);
    }
}

} // namespace kwaque::admin
