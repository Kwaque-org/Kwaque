#include "src/runtime/runtime_service.h"

#include "src/base/metric_schema.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/metrics.hh>

#include <exception>
#include <stdexcept>
#include <vector>

namespace kwaque::runtime {

namespace {

const metric_descriptor& metric(metric_id id) { return *descriptor_for(id); }

} // namespace

runtime_service::runtime_service(
  std::reference_wrapper<seastar::abort_source> parent_abort)
  : tasks_(parent_abort.get()) {}

seastar::future<> runtime_service::start() {
    assert_current();
    if (ready_ || tasks_.admission_closed()) {
        throw std::logic_error("runtime service cannot be started");
    }
    register_metrics();
    ready_ = true;
    return seastar::make_ready_future<>();
}

void runtime_service::request_abort() {
    assert_current();
    tasks_.request_abort();
}

seastar::future<> runtime_service::stop() {
    assert_current();
    request_abort();
    std::exception_ptr failure;
    try {
        co_await tasks_.close();
    } catch (...) {
        failure = std::current_exception();
    }
    ready_ = false;
    metrics_.reset();
    if (failure) {
        std::rethrow_exception(failure);
    }
}

void runtime_service::register_metrics() {
    namespace metrics = seastar::metrics;
    try {
        metrics_.emplace();
        std::vector<metrics::metric_definition> definitions;
        definitions.reserve(5);
        const std::vector<metrics::label> aggregate{metrics::shard_label};
        const auto& active = metric(metric_id::task_active);
        definitions.emplace_back(
          metrics::make_gauge(
            seastar::sstring{active.name},
            [this] { return tasks_.task_count(); },
            metrics::description(seastar::sstring{active.help}))
            .aggregate(aggregate));
        const auto add_counter = [&definitions, &aggregate, this](
                                   metric_id id, auto value) {
            const auto& descriptor = metric(id);
            definitions.emplace_back(
              metrics::make_counter(
                seastar::sstring{descriptor.name},
                [this, value] { return value(tasks_.statistics()); },
                metrics::description(seastar::sstring{descriptor.help}))
                .aggregate(aggregate));
        };
        add_counter(
          metric_id::task_accepted_total,
          [](task_scope_statistics value) { return value.accepted; });
        add_counter(
          metric_id::task_completed_total,
          [](task_scope_statistics value) { return value.completed; });
        add_counter(
          metric_id::task_failed_total,
          [](task_scope_statistics value) { return value.failed; });
        add_counter(
          metric_id::task_abort_requests_total,
          [](task_scope_statistics value) { return value.abort_requests; });
        metrics_->add_group(seastar::sstring{active.group}, definitions);
    } catch (...) {
        metrics_.reset();
        throw;
    }
}

seastar::shard_id runtime_service::shard() const noexcept {
    return owner().value();
}

bool runtime_service::ready() const {
    assert_current();
    return ready_;
}

bool runtime_service::abort_requested() const {
    assert_current();
    return tasks_.abort_requested();
}

task_scope& runtime_service::tasks() {
    assert_current();
    return tasks_;
}

} // namespace kwaque::runtime
