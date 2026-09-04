#include "src/runtime/production/environment.h"

#include "src/base/invariant.h"
#include "src/base/metric_schema.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/metrics.hh>

#include <array>
#include <exception>
#include <new>
#include <system_error>
#include <utility>
#include <vector>

namespace kwaque::runtime::production {

namespace {

constexpr invariant_id environment_stopped_invariant{
  "KQ-PROD-ENVIRONMENT-STOPPED"};
static_assert(environment_stopped_invariant.valid());

const metric_descriptor& metric(metric_id id) { return *descriptor_for(id); }

template<typename Function>
seastar::future<>
preserve_cleanup_failure(std::exception_ptr& first_failure, Function function) {
    try {
        co_await function();
        co_return;
    } catch (...) {
        if (!first_failure) {
            first_failure = std::current_exception();
        }
    }
    try {
        co_await function();
    } catch (...) {
    }
}

template<typename Function>
seastar::future<>
preserve_cleanup_result(std::exception_ptr& first_failure, Function function) {
    try {
        auto outcome = co_await function();
        if (!outcome) {
            throw std::system_error(make_error_code(outcome.error().code()));
        }
        co_return;
    } catch (...) {
        if (!first_failure) {
            first_failure = std::current_exception();
        }
    }
    try {
        static_cast<void>(co_await function());
    } catch (...) {
    }
}

[[nodiscard]] result<observability::event_request> make_lifecycle_event(
  monotonic_time monotonic,
  wall_time wall,
  observability::event_public_text state,
  observability::event_public_text operation,
  observability::event_severity severity) noexcept {
    using observability::event_field_key;
    using observability::event_kind;
    auto state_field = observability::make_event_field<
      event_kind::runtime_state_changed,
      event_field_key::state>(state);
    auto operation_field = observability::make_event_field<
      event_kind::runtime_state_changed,
      event_field_key::operation>(operation);
    if (!state_field || !operation_field) {
        return failure(
          !state_field ? state_field.error() : operation_field.error());
    }
    const std::array fields{*state_field, *operation_field};
    return observability::event_request::make(
      observability::event_request_context{
        .kind = event_kind::runtime_state_changed,
        .severity = severity,
        .monotonic = monotonic,
        .wall = wall,
        .workload = resource::workload_class::maintenance,
      },
      fields);
}

} // namespace

environment::environment(environment_dependencies dependencies)
  : resource_manager_(std::move(dependencies.resources_))
  , event_sink_(*dependencies.logger_, dependencies.event_identity_)
  , dns_options_(std::move(dependencies.dns_options_))
  , stop_done_(std::in_place) {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-PRODUCTION-ENVIRONMENT-LOGGER"},
      dependencies.logger_ != nullptr,
      "production environment has no event logger");
}

environment::~environment() {
    assert_current();
    KWAQUE_INVARIANT(
      environment_stopped_invariant,
      state_ == environment_state::constructed
        || state_ == environment_state::stopped,
      "production environment destroyed before explicit stop completed");
}

seastar::future<> environment::start() {
    return start_with([](std::size_t) noexcept {});
}

void environment::assert_runtime_available(bool owner_present) const {
    const auto lifetime_state = lifetime_.state();
    if (
      !owner_present
      || (state_ != environment_state::started && state_ != environment_state::stopping)
      || (lifetime_state != runtime_lifetime_state::open && lifetime_state != runtime_lifetime_state::closing)) {
        throw std::logic_error("production environment runtime is not ready");
    }
}

void environment::request_abort_unchecked() noexcept {
    if (abort_requested_) {
        return;
    }
    abort_requested_ = true;
    if (tasks_) {
        tasks_->request_abort();
    }
    if (timer_) {
        timer_->request_abort();
    }
    if (dns_) {
        dns_->request_abort();
    }
}

void environment::request_abort() {
    assert_current();
    request_abort_unchecked();
}

seastar::future<> environment::stop() {
    assert_current();
    if (state_ == environment_state::stopping) {
        return stop_done_->get_shared_future();
    }
    if (state_ == environment_state::stopped) {
        return stop_done_ && stop_done_->available()
                 ? stop_done_->get_shared_future()
                 : seastar::make_ready_future<>();
    }
    if (state_ == environment_state::starting) {
        return seastar::make_exception_future<>(
          std::logic_error("production environment startup is in progress"));
    }
    state_ = environment_state::stopping;
    auto completion = stop_once(false).then_wrapped(
      [this](seastar::future<> stopped) noexcept {
          state_ = environment_state::stopped;
          try {
              stopped.get();
              stop_done_->set_value();
          } catch (...) {
              stop_done_->set_exception(std::current_exception());
          }
      });
    static_cast<void>(completion);
    return stop_done_->get_shared_future();
}

seastar::future<> environment::stop_once(bool start_failed) {
    std::exception_ptr first_failure;
    if (!start_failed) {
        try {
            throw_if_failed(emit_lifecycle(
              observability::event_public_text::state_stopping,
              observability::event_public_text::operation_environment_stop,
              observability::event_severity::info));
        } catch (...) {
            first_failure = std::current_exception();
        }
    }

    request_abort_unchecked();
    if (tasks_) {
        co_await preserve_cleanup_failure(
          first_failure, [this] { return tasks_->close(); });
    }
    co_await preserve_cleanup_failure(
      first_failure, [this] { return lifetime_.close(); });
    if (dns_) {
        co_await preserve_cleanup_result(
          first_failure, [this] { return dns_->stop(); });
        dns_.reset();
    }
    network_.reset();
    file_system_.reset();
    if (timer_) {
        co_await preserve_cleanup_result(
          first_failure, [this] { return timer_->stop(); });
        timer_.reset();
    }
    random_.reset();
    dns_options_.reset();
    metrics_.reset();
    tasks_.reset();

    try {
        throw_if_failed(emit_lifecycle(
          start_failed ? observability::event_public_text::state_failed
                       : observability::event_public_text::state_stopped,
          start_failed
            ? observability::event_public_text::operation_environment_start
            : observability::event_public_text::operation_environment_stop,
          start_failed ? observability::event_severity::error
                       : observability::event_severity::info));
        if (start_failed) {
            throw_if_failed(emit_lifecycle(
              observability::event_public_text::state_stopped,
              observability::event_public_text::operation_environment_start,
              observability::event_severity::info));
        }
    } catch (...) {
        if (!first_failure) {
            first_failure = std::current_exception();
        }
    }
    try {
        throw_if_failed(event_sink_.stop());
    } catch (...) {
        if (!first_failure) {
            first_failure = std::current_exception();
        }
    }
    co_await preserve_cleanup_failure(
      first_failure, [this] { return resource_manager_.stop(); });
    if (first_failure) {
        std::rethrow_exception(first_failure);
    }
}

seastar::future<> environment::rollback_start() noexcept {
    try {
        co_await stop_once(true);
    } catch (...) {
    }
}

void environment::throw_if_failed(const result<void>& outcome) {
    if (!outcome) {
        throw std::system_error(make_error_code(outcome.error().code()));
    }
}

result<void> environment::emit_lifecycle(
  observability::event_public_text state,
  observability::event_public_text operation,
  observability::event_severity severity) noexcept {
    auto request = make_lifecycle_event(
      monotonic_clock::now(), wall_clock::now(), state, operation, severity);
    if (!request) {
        return failure(request.error());
    }
    return event_sink_.emit(*request);
}

environment_state environment::state() const {
    assert_current();
    return state_;
}

bool environment::abort_requested() const {
    assert_current();
    return abort_requested_;
}

task_scope& environment::tasks() {
    assert_current();
    if (!tasks_) {
        throw std::logic_error(
          "production environment task scope is not ready");
    }
    return *tasks_;
}

resource::resource_manager& environment::resource_manager() {
    assert_current();
    return resource_manager_;
}

observability::production_event_sink& environment::event_sink() {
    assert_current();
    return event_sink_;
}

void environment::register_metrics() {
    namespace metrics = seastar::metrics;
    try {
        metrics_.emplace();
        const std::vector<metrics::label> aggregate{metrics::shard_label};

        std::vector<metrics::metric_definition> task_definitions;
        task_definitions.reserve(5);
        const auto& active = metric(metric_id::task_active);
        task_definitions.emplace_back(
          metrics::make_gauge(
            seastar::sstring{active.name},
            [this] { return tasks_->task_count(); },
            metrics::description(seastar::sstring{active.help}))
            .aggregate(aggregate));
        const auto add_task_counter = [&task_definitions, &aggregate, this](
                                        metric_id id, auto value) {
            const auto& descriptor = metric(id);
            task_definitions.emplace_back(
              metrics::make_counter(
                seastar::sstring{descriptor.name},
                [this, value] { return value(tasks_->statistics()); },
                metrics::description(seastar::sstring{descriptor.help}))
                .aggregate(aggregate));
        };
        add_task_counter(
          metric_id::task_accepted_total,
          [](task_scope_statistics value) { return value.accepted; });
        add_task_counter(
          metric_id::task_completed_total,
          [](task_scope_statistics value) { return value.completed; });
        add_task_counter(
          metric_id::task_failed_total,
          [](task_scope_statistics value) { return value.failed; });
        add_task_counter(
          metric_id::task_abort_requests_total,
          [](task_scope_statistics value) { return value.abort_requests; });
        metrics_->add_group(seastar::sstring{active.group}, task_definitions);

        const auto add_group = [this, &aggregate](
                                 operation_statistics_owner& statistics,
                                 metric_id active_id,
                                 metric_id accepted_id,
                                 metric_id completed_id,
                                 metric_id rejected_id,
                                 std::optional<metric_id> bytes_id) {
            const auto& active_descriptor = metric(active_id);
            const auto& accepted = metric(accepted_id);
            const auto& completed = metric(completed_id);
            const auto& rejected = metric(rejected_id);
            auto* values = &statistics.get();
            std::vector<metrics::metric_definition> definitions;
            definitions.reserve(bytes_id ? 5U : 4U);
            definitions.emplace_back(
              metrics::make_gauge(
                seastar::sstring{active_descriptor.name},
                [values] { return values->active(); },
                metrics::description(seastar::sstring{active_descriptor.help}))
                .aggregate(aggregate));
            definitions.emplace_back(
              metrics::make_counter(
                seastar::sstring{accepted.name},
                [values] { return values->accepted(); },
                metrics::description(seastar::sstring{accepted.help}))
                .aggregate(aggregate));
            definitions.emplace_back(
              metrics::make_counter(
                seastar::sstring{completed.name},
                [values] { return values->completed(); },
                metrics::description(seastar::sstring{completed.help}))
                .aggregate(aggregate));
            definitions.emplace_back(
              metrics::make_counter(
                seastar::sstring{rejected.name},
                [values] { return values->rejected(); },
                metrics::description(seastar::sstring{rejected.help}))
                .aggregate(aggregate));
            if (bytes_id) {
                const auto& bytes = metric(*bytes_id);
                definitions.emplace_back(
                  metrics::make_counter(
                    seastar::sstring{bytes.name},
                    [values] { return values->completed_bytes(); },
                    metrics::description(seastar::sstring{bytes.help}))
                    .aggregate(aggregate));
            }
            metrics_->add_group(
              seastar::sstring{active_descriptor.group}, definitions);
        };
        add_group(
          timer_statistics_,
          metric_id::timer_active,
          metric_id::timer_accepted_total,
          metric_id::timer_completed_total,
          metric_id::timer_rejected_total,
          std::nullopt);
        add_group(
          file_statistics_,
          metric_id::file_active,
          metric_id::file_accepted_total,
          metric_id::file_completed_total,
          metric_id::file_rejected_total,
          metric_id::file_completed_bytes_total);
        add_group(
          network_statistics_,
          metric_id::network_active,
          metric_id::network_accepted_total,
          metric_id::network_completed_total,
          metric_id::network_rejected_total,
          metric_id::network_completed_bytes_total);
        add_group(
          dns_statistics_,
          metric_id::dns_active,
          metric_id::dns_accepted_total,
          metric_id::dns_completed_total,
          metric_id::dns_rejected_total,
          std::nullopt);
    } catch (...) {
        metrics_.reset();
        throw;
    }
}

} // namespace kwaque::runtime::production
