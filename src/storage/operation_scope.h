#ifndef KWAQUE_SRC_STORAGE_OPERATION_SCOPE_H_
#define KWAQUE_SRC_STORAGE_OPERATION_SCOPE_H_

#include "src/base/invariant.h"
#include "src/runtime/first_failure.h"
#include "src/runtime/task_scope.h"
#include "src/storage/statistics.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/util/noncopyable_function.hh>

#include <concepts>
#include <exception>
#include <functional>
#include <optional>
#include <type_traits>
#include <utility>

namespace kwaque::storage {

// Owns accepted work through drain. Cleanup is constructed at startup and must
// use its already held resources. Required work finishes before task_scope's
// close requests abort. One close caller owns cleanup; repeats observe its
// cache.
class operation_scope final : public runtime::shard_affine {
public:
    using cleanup_type
      = seastar::noncopyable_function<seastar::future<runtime::result<void>>()>;
    using wake_type = seastar::noncopyable_function<void()>;
    operation_scope(workload_budget& budget, cleanup_type cleanup)
      : budget_(budget)
      , cleanup_(std::move(cleanup)) {}
    operation_scope(const operation_scope&) = delete;
    operation_scope& operator=(const operation_scope&) = delete;
    ~operation_scope() {
        assert_current();
        KWAQUE_INVARIANT(
          invariant_id{"KQ-STORAGE-SCOPE-DRAINED"},
          closed_,
          "storage work destroyed before joined close");
    }

    template<typename Func>
    requires std::is_nothrow_move_constructible_v<Func>
             && std::same_as<
               std::invoke_result_t<Func&>,
               seastar::future<runtime::result<void>>>
    [[nodiscard]] runtime::result<void> spawn(byte_count retained, Func task) {
        static_assert(
          sizeof(Func) <= 8192,
          "large task captures require a bounded payload owner");
        assert_current();
        if (admission_closed_ || closing_ || first_.failed()) {
            statistics_.reject();
            return runtime::failure(
              runtime::operation_error{
                errc::closed, runtime::operation_kind::resource});
        }
        if (retained.value() < sizeof(Func)) {
            statistics_.reject();
            return runtime::failure(
              runtime::operation_error{
                errc::invalid_argument, runtime::operation_kind::resource});
        }
        auto reservation = budget_.try_reserve(retained);
        if (!reservation) {
            statistics_.reject();
            return runtime::failure(reservation.error());
        }
        auto holder = admitted_.hold();
        auto metric = statistics_.accept();
        return tasks_.spawn(
          [this,
           task = std::move(task),
           reservation = std::move(*reservation),
           holder = std::move(holder),
           metric = std::move(metric)]() mutable -> seastar::future<> {
              static_cast<void>(reservation);
              static_cast<void>(holder);
              try {
                  // The owning task retains captures until its future
                  // completes.
                  auto outcome = co_await seastar::with_scheduling_group(
                    budget_.scheduling_group(), std::ref(task));
                  first_.observe(outcome);
                  metric.finish(
                    outcome ? storage_outcome::success
                            : storage_outcome::error);
              } catch (...) {
                  first_.observe(std::current_exception());
                  metric.finish(storage_outcome::uncertain);
                  throw;
              }
          });
    }

    // Bind at startup to the environment's early stop notification. Accepted
    // work retains this scope's independent task lifetime and file intents.
    // Wake must synchronously notify the already-running consumer; it must not
    // launch replacement workers or acquire a new runtime/workload lease.
    [[nodiscard]] runtime::result<void>
    bind_shutdown(seastar::abort_source& source, wake_type wake) {
        assert_current();
        if (
          shutdown_bound_ || admission_closed_ || closing_ || closed_ || !wake)
            return runtime::failure(
              runtime::operation_error{
                errc::invalid_argument, runtime::operation_kind::resource});
        wake_ = std::move(wake);
        if (source.abort_requested()) {
            shutdown_bound_ = true;
            request_stop();
            return {};
        }
        shutdown_subscription_ = source.subscribe(
          [this] noexcept { request_stop(); });
        shutdown_bound_ = true;
        if (!shutdown_subscription_ && source.abort_requested()) request_stop();
        return {};
    }

    void close_admission() noexcept {
        assert_current();
        admission_closed_ = true;
        tasks_.close_admission();
    }

    void request_stop() noexcept {
        assert_current();
        close_admission();
        if (stop_requested_) return;
        stop_requested_ = true;
        try {
            if (wake_) wake_();
        } catch (...) {
            first_.observe(std::current_exception());
        }
    }

    [[nodiscard]] bool admission_closed() const noexcept {
        assert_current();
        return admission_closed_;
    }

    [[nodiscard]] seastar::future<runtime::result<void>> close() {
        assert_current();
        if (closed_) {
            if (first_.exception())
                return seastar::make_exception_future<runtime::result<void>>(
                  first_.exception());
            return seastar::make_ready_future<runtime::result<void>>(
              first_.outcome());
        }
        if (closing_)
            return seastar::make_ready_future<runtime::result<void>>(
              runtime::failure(
                runtime::operation_error{
                  errc::queue_full, runtime::operation_kind::resource}));
        return close_once();
    }
    [[nodiscard]] const storage_statistics& statistics() const noexcept {
        assert_current();
        return statistics_;
    }
    // Call only at an independently established storage barrier/uncertain
    // publication transition; terminal task completion is not such a barrier.
    void observe_durable() noexcept {
        assert_current();
        statistics_.observe_durable();
    }
    void observe_uncertain() noexcept {
        assert_current();
        statistics_.observe_uncertain();
    }

private:
    seastar::future<runtime::result<void>> close_once() {
        closing_ = true;
        request_stop();
        co_await admitted_.close();
        try {
            if (cleanup_) {
                first_.observe(
                  co_await seastar::with_scheduling_group(
                    budget_.scheduling_group(), [this] { return cleanup_(); }));
            }
        } catch (...) {
            first_.observe(std::current_exception());
        }
        try {
            co_await tasks_.close();
        } catch (...) {
            first_.observe(std::current_exception());
        }
        shutdown_subscription_ = std::nullopt;
        closed_ = true;
        co_return first_.outcome();
    }
    workload_budget& budget_;
    cleanup_type cleanup_;
    wake_type wake_;
    storage_statistics statistics_;
    runtime::first_failure first_;
    runtime::task_scope tasks_;
    seastar::gate admitted_;
    seastar::optimized_optional<seastar::abort_source::subscription>
      shutdown_subscription_;
    bool admission_closed_{false}, stop_requested_{false},
      shutdown_bound_{false};
    bool closing_{false}, closed_{false};
};

} // namespace kwaque::storage
#endif // KWAQUE_SRC_STORAGE_OPERATION_SCOPE_H_
