#pragma once

#include "src/bytes/fragmented_buffer_builder.h"
#include "src/bytes/test_allocation_profile.h"
#include "src/runtime/environment.h"
#include "src/runtime/first_failure.h"
#include "src/runtime/timer.h"
#include "src/storage/completion_resources.h"
#include "src/storage/operation_scope.h"

#include <seastar/core/chunked_vector.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>

#include <cstddef>
#include <exception>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>

namespace kwaque::storage::testing {

inline void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

template<typename T>
T take(runtime::result<T> value) {
    if (!value) throw std::runtime_error(value.error().render());
    return std::move(*value);
}
inline void take(runtime::result<void> value) {
    if (!value) throw std::runtime_error(value.error().render());
}
template<typename T>
T take(kwaque::result<T> value) {
    if (!value) throw std::system_error(value.error());
    if constexpr (!std::is_void_v<T>) return std::move(*value);
}

struct direct_driver final {
    template<typename T>
    seastar::future<T> operation(seastar::future<T> operation) const {
        return operation;
    }
    template<typename T>
    seastar::future<T> lifecycle(seastar::future<T> operation) const {
        return operation;
    }
};

template<runtime::runtime_backend Backend>
class completion_contract final {
    using view_type = runtime::basic_runtime_view<
      Backend,
      runtime::runtime_capability::file_system,
      runtime::runtime_capability::timer>;

    static view_type acquire(Backend& backend) {
        runtime::basic_runtime<Backend> root{backend};
        return take(root.template view<
                    runtime::runtime_capability::file_system,
                    runtime::runtime_capability::timer>());
    }

public:
    static constexpr std::size_t start_boundaries = 5;
    completion_contract(Backend& backend, runtime::file_path path)
      : view_(acquire(backend))
      , path_(std::move(path))
      , observer_(backend.resource_manager().acquire_workload(
          resource::workload_class::metadata))
      , budget_(
          backend.resource_manager().acquire_workload(
            resource::workload_class::metadata),
          {.tasks = 4, .bytes = byte_count{128U * 1024U}, .handles = 2},
          bytes::testing::charge)
      , scope_(budget_, [this] { return finish(); }) {}

    template<typename Driver>
    seastar::future<> start(
      Backend& backend,
      Driver drive,
      std::optional<std::size_t> fail_at = std::nullopt) {
        const auto checkpoint = [fail_at](std::size_t current) {
            if (fail_at == current)
                throw std::runtime_error("injected storage startup failure");
        };
        checkpoint(0);
        file_.emplace(take(
          co_await drive.operation(view_.file_system().open(
            path_,
            {.access = runtime::file_access::read_write,
             .create = true,
             .exclusive = true,
             .close_policy = runtime::file_close_policy::checked}))));
        if (backend.abort_requested())
            throw seastar::abort_requested_exception{};
        checkpoint(1);
        completion_.emplace(take(completion_resources::make(budget_, *file_)));
        bytes::fragmented_buffer_builder builder;
        take(builder.append(std::string_view{"accepted-before-shutdown"}));
        payload_ = take(builder.finish());
        checkpoint(2);
        take(scope_.bind_shutdown(backend.tasks().abort_source(), [this] {
            batch_abort_.request_abort();
            wake_.set_value();
            ++wakes_;
        }));
        take(scope_.spawn(
          byte_count{4096}, [this] -> seastar::future<runtime::result<void>> {
              co_await wake_.get_future();
              if (batch_timer_) {
                  auto waiting = std::move(*batch_timer_);
                  batch_timer_.reset();
                  const auto tick = co_await std::move(waiting);
                  timer_cancelled_ = !tick
                                     && tick.error().code() == errc::aborted;
                  if (!tick && !timer_cancelled_)
                      co_return runtime::failure(tick.error());
              }
              completion_->scratch().front() = 'k';
              const auto written = co_await file_->write(
                runtime::file_position{}, std::move(payload_));
              if (!written) co_return runtime::failure(written.error());
              worker_finished_ = true;
              co_return runtime::result<void>{};
          }));
        worker_started_ = true;
        checkpoint(3);

        // Occupy every ordinary metadata unit; the completion unit stays owned.
        metadata_pressure_.reserve(file_->limits().pending_metadata_operations);
        while (auto slot = file_->try_reserve_metadata()) {
            metadata_pressure_.push_back(std::move(*slot));
        }
        const auto ordinary = co_await drive.operation(file_->size());
        require(
          !ordinary && ordinary.error().code() == errc::queue_full,
          "ordinary metadata admission was not saturated");
        if (scope_.admission_closed())
            throw seastar::abort_requested_exception{};

        // Keep ordinary task/handle credits and remaining workload bytes held
        // until after the protected barrier has completed.
        while (auto reservation = budget_.try_reserve(byte_count{512})) {
            if (task_pressure_.empty())
                take(reservation->try_acquire_handles(1));
            task_pressure_.push_back(std::move(*reservation));
        }
        memory_pressure_ = seastar::try_get_units(
          observer_.memory_admission(), observer_.memory_admission().current());
        require(
          memory_pressure_.has_value(),
          "failed to occupy ordinary workload bytes");
        require(
          budget_.snapshot().tasks == 4 && budget_.snapshot().handles == 2,
          "ordinary task or handle capacity was not saturated");
        pressure_ready_ = true;
        batch_timer_.emplace(
          runtime::sleep_for<typename Backend::monotonic_clock>(
            view_.timer(),
            runtime::monotonic_duration{86'400'000'000'000},
            batch_abort_));
        checkpoint(4);
    }

    void check_early_abort(Backend& backend) {
        require(
          backend.abort_requested(), "storage stop preceded environment abort");
        require(
          scope_.admission_closed(),
          "early abort did not close storage admission");
        require(
          !backend.lifetime().acquire(),
          "early abort admitted a runtime lease");
        bool invoked = false;
        const auto rejected = scope_.spawn(byte_count{4096}, [&invoked] {
            invoked = true;
            return seastar::make_ready_future<runtime::result<void>>(
              runtime::result<void>{});
        });
        require(
          !rejected && rejected.error().code() == errc::closed && !invoked,
          "early abort admitted replacement storage work");
        if (file_)
            require(
              !file_->abort_requested(),
              "early abort cancelled the file intent");
    }

    seastar::future<runtime::result<void>> stop() { return scope_.close(); }

    template<typename Driver>
    seastar::future<> verify_contents(Driver drive) {
        // Native file continuations can remain queued after the final fake I/O
        // event. Use the lifecycle driver to join the complete verification.
        auto file = take(
          co_await drive.lifecycle(view_.file_system().open(
            path_,
            {.access = runtime::file_access::read_only,
             .close_policy = runtime::file_close_policy::checked})));
        runtime::first_failure first;
        try {
            const auto read = take(
              co_await drive.lifecycle(
                file.read(runtime::file_position{}, byte_count{64})));
            require(
              read.eof()
                && read.data().content_equals("accepted-before-shutdown"),
              "completion changed the accepted file payload");
        } catch (...) {
            first.observe(std::current_exception());
        }
        try {
            first.observe(co_await drive.lifecycle(file.close()));
        } catch (...) {
            first.observe(std::current_exception());
        }
        take(first.outcome());
    }

    void
    check_stopped(bool complete_start, bool barrier_expected = true) const {
        require(
          !file_ && !completion_, "storage cleanup retained file resources");
        require(
          budget_.snapshot().tasks == 0 && budget_.snapshot().bytes == 0
            && budget_.snapshot().handles == 0,
          "storage cleanup retained workload capacity");
        require(
          scope_.statistics().snapshot().operations.active == 0,
          "storage work did not drain");
        if (complete_start) {
            require(
              worker_finished_ && timer_cancelled_
                && barrier_completed_ == barrier_expected && pressure_observed_
                && wakes_ == 1,
              "protected completion did not finish under shutdown pressure");
        }
    }

private:
    seastar::future<runtime::result<void>> finish() {
        runtime::first_failure first;
        if (file_ && completion_) {
            pressure_observed_
              = pressure_ready_ && budget_.snapshot().tasks == 3
                && budget_.snapshot().handles == 2
                && file_->pending_metadata_operations()
                     == file_->limits().pending_metadata_operations;
            try {
                require(
                  seastar::current_scheduling_group()
                    == budget_.scheduling_group(),
                  "completion used the wrong workload scheduling group");
                if (worker_started_)
                    require(
                      worker_finished_ && completion_->scratch().front() == 'k',
                      "cleanup overtook the accepted worker");
                const auto flushed = co_await file_->flush(
                  completion_->metadata());
                first.observe(flushed);
                barrier_completed_ = flushed.has_value();
            } catch (...) {
                first.observe(std::current_exception());
            }
            completion_->release_metadata();
        }
        metadata_pressure_.clear();
        if (file_) {
            try {
                first.observe(co_await file_->close());
            } catch (...) {
                first.observe(std::current_exception());
            }
            file_.reset();
        }
        payload_ = bytes::fragmented_buffer{};
        completion_.reset();
        task_pressure_.clear();
        memory_pressure_.reset();
        co_return first.outcome();
    }

    view_type view_;
    runtime::file_path path_;
    resource::workload_handle observer_;
    workload_budget budget_;
    std::optional<runtime::file> file_;
    std::optional<completion_resources> completion_;
    bytes::fragmented_buffer payload_;
    seastar::chunked_vector<runtime::file::metadata_reservation>
      metadata_pressure_;
    seastar::chunked_vector<workload_reservation> task_pressure_;
    std::optional<seastar::semaphore_units<>> memory_pressure_;
    seastar::promise<> wake_;
    seastar::abort_source batch_abort_;
    std::optional<seastar::future<runtime::result<void>>> batch_timer_;
    bool timer_cancelled_{false};
    bool worker_started_{false}, worker_finished_{false};
    bool pressure_ready_{false}, pressure_observed_{false},
      barrier_completed_{false};
    unsigned wakes_{0};
    operation_scope scope_;
};

} // namespace kwaque::storage::testing
