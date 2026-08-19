#include "src/admin/admin_server.h"

#include "src/admin/admin_responses.h"
#include "src/admin/admin_state.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/prometheus.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/net/inet_address.hh>

#include <chrono>
#include <exception>
#include <memory>
#include <stdexcept>
#include <utility>

namespace kwaque::admin {

namespace {

using reply_status = seastar::http::reply::status_type;

void put_json_route(seastar::httpd::routes &routes, seastar::sstring path,
                    seastar::httpd::handle_function handler) {
  auto owned_handler = std::make_unique<seastar::httpd::function_handler>(
      std::move(handler), "application/json");
  routes.put(seastar::httpd::operation_type::GET, std::move(path),
             owned_handler.get());
  static_cast<void>(owned_handler.release());
}

} // namespace

class admin_server::impl final {
public:
  impl()
      : state_(std::make_shared<admin_state>()),
        version_json_(build_info_json(current_build_info())) {}

  void register_metrics() {
    namespace metrics = seastar::metrics;
    metrics_.add_group(
        "broker",
        {metrics::make_gauge(
             "process_readiness",
             [state = state_] { return state->ready() ? 1U : 0U; },
             metrics::description("Whether the broker is ready for traffic")),
         metrics::make_gauge(
             "shard_count", [state = state_] { return state->shard_count(); },
             metrics::description("Configured reactor shard count")),
         metrics::make_gauge(
             "startup_duration_seconds",
             [state = state_] { return state->startup_duration_seconds(); },
             metrics::description("Time from application start to readiness")),
         metrics::make_counter(
             "shutdown_total",
             [state = state_] { return state->shutdown_count(); },
             metrics::description("Number of initiated broker shutdowns")),
         metrics::make_counter(
             "http_requests_total",
             [state = state_] { return state->request_count(); },
             metrics::description("Administrative HTTP requests"))});
  }

  void register_routes(seastar::httpd::routes &routes) {
    const auto state = state_;
    put_json_route(
        routes, "/v1/health/live",
        [state](seastar::httpd::const_req, seastar::http::reply &reply) {
          state->record_request();
          auto response = liveness_response(state->live());
          reply.set_status(static_cast<reply_status>(response.status));
          return seastar::sstring(std::move(response.body));
        });
    put_json_route(
        routes, "/v1/health/ready",
        [state](seastar::httpd::const_req, seastar::http::reply &reply) {
          state->record_request();
          auto response = readiness_response(state->ready());
          reply.set_status(static_cast<reply_status>(response.status));
          return seastar::sstring(std::move(response.body));
        });
    put_json_route(
        routes, "/v1/version",
        [state, version = version_json_](seastar::httpd::const_req,
                                         seastar::http::reply &) {
          state->record_request();
          return seastar::sstring(version);
        });
  }

  std::shared_ptr<admin_state> state_;
  std::string version_json_;
  seastar::httpd::http_server_control server_;
  seastar::metrics::metric_groups metrics_;
  bool started_{false};
};

admin_server::admin_server() : impl_(std::make_unique<impl>()) {}

admin_server::~admin_server() = default;

seastar::future<> admin_server::start(std::string address, std::uint16_t port,
                                      unsigned shard_count) {
  if (impl_->started_) {
    throw std::logic_error("admin server is already started");
  }

  co_await impl_->server_.start("kwaque-admin");
  std::exception_ptr startup_failure;
  try {
    impl_->register_metrics();
    co_await impl_->server_.set_routes(
        [this](seastar::httpd::routes &routes) {
          impl_->register_routes(routes);
        });

    seastar::prometheus::config prometheus_config;
    prometheus_config.prefix = "kwaque";
    co_await seastar::prometheus::start(impl_->server_,
                                       std::move(prometheus_config));

    impl_->state_->listener_started(shard_count);
    seastar::listen_options options;
    options.reuse_address = true;
    co_await impl_->server_.listen(
        seastar::socket_address{seastar::net::inet_address(address), port},
        options);
    impl_->started_ = true;
  } catch (...) {
    startup_failure = std::current_exception();
  }
  if (startup_failure) {
    impl_->state_->stopped();
    co_await impl_->server_.stop();
    impl_->metrics_.clear();
    std::rethrow_exception(startup_failure);
  }
}

void admin_server::mark_ready(
    std::chrono::steady_clock::duration startup_duration) noexcept {
  impl_->state_->mark_ready(
      std::chrono::duration<double>(startup_duration).count());
}

void admin_server::begin_shutdown() noexcept {
  impl_->state_->begin_shutdown();
}

seastar::future<> admin_server::stop() {
  if (!impl_->started_) {
    co_return;
  }
  begin_shutdown();
  co_await impl_->server_.stop();
  impl_->started_ = false;
  impl_->metrics_.clear();
  impl_->state_->stopped();
}

bool admin_server::live() const noexcept { return impl_->state_->live(); }

bool admin_server::ready() const noexcept { return impl_->state_->ready(); }

} // namespace kwaque::admin
