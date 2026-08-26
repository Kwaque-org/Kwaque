#pragma once

#include "src/base/invariant.h"
#include "src/base/result.h"
#include "src/base/units.h"
#include "src/resource/memory_budget.h"
#include "src/resource/resource_manager.h"
#include "src/resource/workload_class.h"
#include "src/runtime/error.h"
#include "src/runtime/shard_affinity.h"
#include "src/runtime/task_scope.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/util/noncopyable_function.hh>

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <expected>
#include <limits>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace kwaque::resource {

struct bounded_work_queue_config final {
    item_count maximum_items;
    byte_count maximum_bytes;
    std::size_t maximum_producer_waiters{0};

    [[nodiscard]] result<void> validate() const noexcept {
        // Every admitted item costs at least one byte, so an item bound above
        // the byte bound could never be the binding constraint.
        if (
          maximum_items.value() == 0 || maximum_bytes.value() < 2
          || maximum_bytes.value() > seastar::semaphore::max_counter()
          || maximum_items.value() > maximum_bytes.value()) {
            return failure(errc::invalid_argument);
        }
        return {};
    }

    bool operator==(const bounded_work_queue_config&) const = default;
};

enum class queue_failure_kind {
    closed,
    aborted,
    invalid_cost,
    oversized,
    producer_waiters_exhausted,
};

struct queue_failure final {
    queue_failure_kind kind;
    runtime::operation_error detail;

    bool operator==(const queue_failure&) const = default;
};

template<typename T>
using queue_result = std::expected<T, queue_failure>;

enum class queue_close_mode {
    drain,
    abort,
};

enum class bounded_work_queue_state {
    open,
    draining,
    aborting,
    closed,
};

// FIFO queue admission is serialized so an expensive producer cannot be
// bypassed by later cheap producers. Item, byte, and producer-waiter storage
// are bounded independently. At most maximum_producer_waiters producers are
// suspended at once; one of those slots is reserved for the producer holding
// the admission turn, so a later arrival can never displace the producer
// already at the head of the admission order.
template<typename T>
requires std::is_nothrow_move_constructible_v<T>
class bounded_work_queue final : public runtime::shard_affine {
public:
    explicit bounded_work_queue(bounded_work_queue_config config)
      : config_(validate_or_throw(config))
      , bytes_(memory_config(config_.maximum_bytes)) {
        producer_turn_.ensure_space_for_waiters(max_queued_producers());
    }

    ~bounded_work_queue() {
        assert_current();
        KWAQUE_INVARIANT(
          invariant_id{"KQ-WORK-QUEUE-CLOSED"},
          state_ == bounded_work_queue_state::closed,
          "bounded work queue destroyed before close completed");
        KWAQUE_INVARIANT(
          invariant_id{"KQ-WORK-QUEUE-DRAINED"},
          items_.empty() && waiting_producers() == 0 && waiting_consumers_ == 0
            && active_producers_ == 0 && active_consumers_ == 0
            && bytes_.used().value() == 0 && producer_turn_.waiters() == 0
            && producer_turn_.current() == 1,
          "bounded work queue destroyed with retained work or waiters");
    }

    [[nodiscard]] const bounded_work_queue_config& config() const {
        assert_current();
        return config_;
    }

    [[nodiscard]] bounded_work_queue_state state() const {
        assert_current();
        return state_;
    }

    [[nodiscard]] std::size_t size() const {
        assert_current();
        return items_.size();
    }

    [[nodiscard]] byte_count bytes() const {
        assert_current();
        return bytes_.used();
    }

    [[nodiscard]] std::size_t waiting_producers() const {
        assert_current();
        return queued_producers_ + admitting_producers_;
    }

    // Producers suspended waiting for their turn to attempt admission.
    [[nodiscard]] std::size_t queued_producers() const {
        assert_current();
        return queued_producers_;
    }

    // At most one: the producer holding the turn, suspended waiting for item or
    // byte capacity.
    [[nodiscard]] std::size_t admitting_producers() const {
        assert_current();
        return admitting_producers_;
    }

    [[nodiscard]] std::size_t waiting_consumers() const {
        assert_current();
        return waiting_consumers_;
    }

    [[nodiscard]] seastar::future<queue_result<void>>
    push(T item, byte_count cost, seastar::abort_source& abort_source) {
        assert_current();
        operation_token operation{*this, active_producers_};
        if (auto invalid = validate_cost(cost)) {
            co_return std::unexpected(std::move(*invalid));
        }
        if (state_ != bounded_work_queue_state::open) {
            co_return std::unexpected(
              failure(queue_failure_kind::closed, errc::closed, cost));
        }
        if (abort_source.abort_requested()) {
            co_return std::unexpected(
              failure(queue_failure_kind::aborted, errc::aborted, cost));
        }

        auto turn = seastar::try_get_units(producer_turn_, 1);
        producer_wait_token waiting{*this};
        if (!turn) {
            if (!waiting.try_engage_queued()) {
                co_return std::unexpected(failure(
                  queue_failure_kind::producer_waiters_exhausted,
                  errc::resource_exhausted,
                  cost));
            }
            try {
                turn = co_await seastar::get_units(
                  producer_turn_, 1, abort_source);
            } catch (...) {
                if (abort_source.abort_requested()) {
                    co_return std::unexpected(failure(
                      queue_failure_kind::aborted, errc::aborted, cost));
                }
                throw;
            }
        }

        while (true) {
            if (state_ != bounded_work_queue_state::open) {
                co_return std::unexpected(
                  failure(queue_failure_kind::closed, errc::closed, cost));
            }
            if (abort_source.abort_requested()) {
                co_return std::unexpected(
                  failure(queue_failure_kind::aborted, errc::aborted, cost));
            }
            if (can_admit(cost)) {
                auto units = bytes_.try_acquire(cost);
                KWAQUE_INVARIANT(
                  invariant_id{"KQ-WORK-QUEUE-BYTE-ADMISSION"},
                  units.has_value(),
                  "queue byte availability disagreed with admission");
                items_.push_back(
                  admitted_item{
                    .units = std::move(*units),
                    .item = std::move(item),
                  });
                consumer_condition_.signal();
                co_return queue_result<void>{};
            }

            // This producer holds the turn, so it takes the slot reserved for
            // the head of the admission order. Only a queue that permits no
            // suspended producers at all can refuse it.
            if (!waiting.engage_admitting()) {
                co_return std::unexpected(failure(
                  queue_failure_kind::producer_waiters_exhausted,
                  errc::resource_exhausted,
                  cost));
            }
            auto subscription = abort_source.subscribe(
              [this]() noexcept { producer_condition_.broadcast(); });
            if (abort_source.abort_requested()) {
                continue;
            }
            co_await producer_condition_.wait([this, cost, &abort_source] {
                return state_ != bounded_work_queue_state::open
                       || abort_source.abort_requested() || can_admit(cost);
            });
        }
    }

    [[nodiscard]] seastar::future<queue_result<T>>
    pop(seastar::abort_source& abort_source) {
        assert_current();
        operation_token operation{*this, active_consumers_};
        consumer_wait_token waiting{*this};
        while (true) {
            if (state_ != bounded_work_queue_state::open && items_.empty()) {
                co_return std::unexpected(failure(
                  queue_failure_kind::closed, errc::closed, byte_count{}));
            }
            if (abort_source.abort_requested()) {
                co_return std::unexpected(failure(
                  queue_failure_kind::aborted, errc::aborted, byte_count{}));
            }
            if (!items_.empty()) {
                auto admitted = std::move(items_.front());
                items_.pop_front();
                auto item = std::move(admitted.item);
                static_cast<void>(admitted.units.release());
                producer_condition_.signal();
                maybe_finish_close();
                co_return queue_result<T>{std::move(item)};
            }

            waiting.engage();
            auto subscription = abort_source.subscribe(
              [this]() noexcept { consumer_condition_.broadcast(); });
            if (abort_source.abort_requested()) {
                continue;
            }
            co_await consumer_condition_.wait([this, &abort_source] {
                return !items_.empty()
                       || state_ != bounded_work_queue_state::open
                       || abort_source.abort_requested();
            });
        }
    }

    [[nodiscard]] seastar::future<> close(queue_close_mode mode) {
        assert_current();
        if (state_ == bounded_work_queue_state::closed) {
            return close_done_.get_shared_future();
        }
        if (state_ == bounded_work_queue_state::open) {
            state_ = mode == queue_close_mode::drain
                       ? bounded_work_queue_state::draining
                       : bounded_work_queue_state::aborting;
        } else if (mode == queue_close_mode::abort) {
            state_ = bounded_work_queue_state::aborting;
        }

        if (state_ == bounded_work_queue_state::aborting) {
            items_.clear();
        }
        producer_condition_.broadcast();
        consumer_condition_.broadcast();
        maybe_finish_close();
        return close_done_.get_shared_future();
    }

private:
    struct admitted_item final {
        memory_units units;
        T item;
    };

    // This token outlives every later coroutine local, so close cannot become
    // ready until the operation has returned semaphore units/subscriptions and
    // finished touching the queue.
    class operation_token final {
    public:
        operation_token(bounded_work_queue& queue, std::size_t& active) noexcept
          : queue_(queue)
          , active_(active) {
            KWAQUE_INVARIANT(
              invariant_id{"KQ-WORK-QUEUE-ACTIVE-CAPACITY"},
              active_ != std::numeric_limits<std::size_t>::max(),
              "queue operation counter overflow");
            ++active_;
        }
        operation_token(const operation_token&) = delete;
        operation_token& operator=(const operation_token&) = delete;
        ~operation_token() {
            KWAQUE_INVARIANT(
              invariant_id{"KQ-WORK-QUEUE-ACTIVE-OPERATION"},
              active_ != 0,
              "queue operation counter underflow");
            --active_;
            queue_.maybe_finish_close();
        }

    private:
        bounded_work_queue& queue_;
        std::size_t& active_;
    };

    enum class producer_wait_role {
        queued,
        admitting,
    };

    // Holds exactly one producer-waiter slot for the lifetime of one push, in
    // whichever of the two populations that push is currently suspended in.
    // Promotion from queued to admitting moves the slot without releasing it,
    // so the combined population never exceeds its configured bound and the
    // producer that won the turn never has to re-compete for a slot.
    class producer_wait_token final {
    public:
        explicit producer_wait_token(bounded_work_queue& queue) noexcept
          : queue_(queue) {}
        producer_wait_token(const producer_wait_token&) = delete;
        producer_wait_token& operator=(const producer_wait_token&) = delete;
        ~producer_wait_token() { disengage(); }

        [[nodiscard]] bool try_engage_queued() noexcept {
            if (engaged_) {
                return true;
            }
            if (queue_.queued_producers_ >= queue_.max_queued_producers()) {
                return false;
            }
            ++queue_.queued_producers_;
            role_ = producer_wait_role::queued;
            engaged_ = true;
            return true;
        }

        [[nodiscard]] bool engage_admitting() noexcept {
            if (engaged_ && role_ == producer_wait_role::admitting) {
                return true;
            }
            if (queue_.config_.maximum_producer_waiters == 0) {
                return false;
            }
            if (engaged_) {
                KWAQUE_INVARIANT(
                  invariant_id{"KQ-WORK-QUEUE-QUEUED-WAITER"},
                  queue_.queued_producers_ != 0,
                  "queued producer counter underflow");
                --queue_.queued_producers_;
            }
            KWAQUE_INVARIANT(
              invariant_id{"KQ-WORK-QUEUE-ADMITTING-UNIQUE"},
              queue_.admitting_producers_ == 0,
              "more than one producer holds the admission turn");
            ++queue_.admitting_producers_;
            role_ = producer_wait_role::admitting;
            engaged_ = true;
            return true;
        }

    private:
        void disengage() noexcept {
            if (!engaged_) {
                return;
            }
            engaged_ = false;
            if (role_ == producer_wait_role::admitting) {
                KWAQUE_INVARIANT(
                  invariant_id{"KQ-WORK-QUEUE-PRODUCER-WAITER"},
                  queue_.admitting_producers_ != 0,
                  "admitting producer counter underflow");
                --queue_.admitting_producers_;
            } else {
                KWAQUE_INVARIANT(
                  invariant_id{"KQ-WORK-QUEUE-QUEUED-WAITER"},
                  queue_.queued_producers_ != 0,
                  "queued producer counter underflow");
                --queue_.queued_producers_;
            }
            queue_.maybe_finish_close();
        }

        bounded_work_queue& queue_;
        producer_wait_role role_{producer_wait_role::queued};
        bool engaged_{false};
    };

    class consumer_wait_token final {
    public:
        explicit consumer_wait_token(bounded_work_queue& queue) noexcept
          : queue_(queue) {}
        consumer_wait_token(const consumer_wait_token&) = delete;
        consumer_wait_token& operator=(const consumer_wait_token&) = delete;
        ~consumer_wait_token() {
            if (engaged_) {
                KWAQUE_INVARIANT(
                  invariant_id{"KQ-WORK-QUEUE-CONSUMER-WAITER"},
                  queue_.waiting_consumers_ != 0,
                  "consumer waiter counter underflow");
                --queue_.waiting_consumers_;
                queue_.maybe_finish_close();
            }
        }

        void engage() noexcept {
            if (!engaged_) {
                ++queue_.waiting_consumers_;
                engaged_ = true;
            }
        }

    private:
        bounded_work_queue& queue_;
        bool engaged_{false};
    };

    [[nodiscard]] static bounded_work_queue_config
    validate_or_throw(bounded_work_queue_config config) {
        if (auto valid = config.validate(); !valid) {
            throw std::invalid_argument("invalid bounded work queue config");
        }
        return config;
    }

    [[nodiscard]] static memory_budget_config
    memory_config(byte_count capacity) {
        auto config = memory_budget_config::with_defaults(capacity, 0);
        if (!config) {
            throw std::invalid_argument("invalid queue byte capacity");
        }
        return *config;
    }

    [[nodiscard]] std::optional<queue_failure>
    validate_cost(byte_count cost) const noexcept {
        if (cost.value() == 0) {
            return failure(
              queue_failure_kind::invalid_cost, errc::invalid_argument, cost);
        }
        if (cost > config_.maximum_bytes) {
            return failure(
              queue_failure_kind::oversized, errc::out_of_range, cost);
        }
        return std::nullopt;
    }

    [[nodiscard]] queue_failure failure(
      queue_failure_kind kind, errc code, byte_count cost) const noexcept {
        runtime::operation_error detail{
          code, runtime::operation_kind::resource};
        static_cast<void>(detail.add_context(
          runtime::operation_context_key::bytes, cost.value()));
        static_cast<void>(detail.add_context(
          runtime::operation_context_key::items,
          static_cast<std::uint64_t>(items_.size())));
        return queue_failure{kind, std::move(detail)};
    }

    // One slot of the producer-waiter bound is reserved for the turn holder, so
    // the queue for the turn is bounded one lower. A queue configured to permit
    // no suspended producers permits none in either population.
    [[nodiscard]] std::size_t max_queued_producers() const noexcept {
        return config_.maximum_producer_waiters == 0
                 ? 0
                 : config_.maximum_producer_waiters - 1;
    }

    [[nodiscard]] bool can_admit(byte_count cost) const {
        return items_.size() < config_.maximum_items.value()
               && bytes_.available() >= cost;
    }

    void maybe_finish_close() noexcept {
        if (
          state_ != bounded_work_queue_state::open
          && state_ != bounded_work_queue_state::closed && items_.empty()
          && queued_producers_ == 0 && admitting_producers_ == 0
          && waiting_consumers_ == 0 && active_producers_ == 0
          && active_consumers_ == 0) {
            state_ = bounded_work_queue_state::closed;
            if (!close_done_.available()) {
                close_done_.set_value();
            }
        }
    }

    const bounded_work_queue_config config_;
    memory_budget bytes_;
    std::deque<admitted_item> items_;
    seastar::semaphore producer_turn_{1};
    seastar::condition_variable producer_condition_;
    seastar::condition_variable consumer_condition_;
    seastar::shared_promise<> close_done_;
    bounded_work_queue_state state_{bounded_work_queue_state::open};
    std::size_t queued_producers_{0};
    std::size_t admitting_producers_{0};
    std::size_t waiting_consumers_{0};
    std::size_t active_producers_{0};
    std::size_t active_consumers_{0};
};

struct bounded_queue_worker_config final {
    std::size_t workers;
    std::uint64_t maximum_error_reports;

    [[nodiscard]] result<void> validate() const noexcept {
        return workers == 0 ? failure(errc::invalid_argument) : result<void>{};
    }

    bool operator==(const bounded_queue_worker_config&) const = default;
};

template<typename T>
requires std::is_nothrow_move_constructible_v<T>
class bounded_queue_workers final : public runtime::shard_affine {
public:
    using handler_type = seastar::noncopyable_function<seastar::future<>(T)>;
    using reporter_type
      = seastar::noncopyable_function<void(std::exception_ptr) noexcept>;

    bounded_queue_workers(
      bounded_work_queue<T>& queue,
      resource_manager& manager,
      workload_class classification,
      bounded_queue_worker_config config,
      handler_type handler,
      reporter_type reporter)
      : queue_(queue)
      , manager_(manager)
      , classification_(classification)
      , config_(validate_or_throw(config))
      , handler_(std::move(handler))
      , reporter_(std::move(reporter)) {
        if (!handler_ || !reporter_) {
            throw std::invalid_argument(
              "queue worker callbacks must be present");
        }
    }

    ~bounded_queue_workers() {
        assert_current();
        KWAQUE_INVARIANT(
          invariant_id{"KQ-QUEUE-WORKERS-CLOSED"},
          !started_ || close_done_.available(),
          "queue workers destroyed before close completed");
        KWAQUE_INVARIANT(
          invariant_id{"KQ-QUEUE-WORKERS-DRAINED"},
          active_workers_ == 0 && active_handlers_ == 0,
          "queue workers destroyed with active fibers or handlers");
    }

    void start() {
        assert_current();
        if (started_) {
            throw std::logic_error("queue workers already started");
        }
        if (!manager_.ready()) {
            throw std::logic_error("resource manager is not ready");
        }
        tasks_.emplace();
        started_ = true;
        for (std::size_t index = 0; index < config_.workers; ++index) {
            const auto accepted = tasks_->spawn(
              [this] { return worker_loop(); });
            KWAQUE_INVARIANT(
              invariant_id{"KQ-QUEUE-WORKER-SPAWNED"},
              accepted.has_value(),
              "fresh worker task scope rejected a worker");
        }
    }

    [[nodiscard]] seastar::future<> close(queue_close_mode mode) {
        assert_current();
        if (!started_) {
            throw std::logic_error("queue workers were not started");
        }
        if (!closing_) {
            closing_ = true;
            auto completion = close_once(mode).then_wrapped(
              [this](seastar::future<> closed) noexcept {
                  try {
                      closed.get();
                      close_done_.set_value();
                  } catch (...) {
                      close_done_.set_exception(std::current_exception());
                  }
              });
            static_cast<void>(completion);
        } else if (mode == queue_close_mode::abort) {
            static_cast<void>(queue_.close(queue_close_mode::abort));
        }
        return close_done_.get_shared_future();
    }

    [[nodiscard]] std::size_t configured_workers() const {
        assert_current();
        return config_.workers;
    }

    [[nodiscard]] std::size_t active_workers() const {
        assert_current();
        return active_workers_;
    }

    [[nodiscard]] std::size_t active_handlers() const {
        assert_current();
        return active_handlers_;
    }

    [[nodiscard]] std::size_t maximum_active_handlers() const {
        assert_current();
        return maximum_active_handlers_;
    }

    [[nodiscard]] std::uint64_t reported_errors() const {
        assert_current();
        return reported_errors_;
    }

    [[nodiscard]] std::uint64_t suppressed_errors() const {
        assert_current();
        return suppressed_errors_;
    }

private:
    class count_guard final {
    public:
        explicit count_guard(std::size_t& value) noexcept
          : value_(value) {
            ++value_;
        }
        count_guard(const count_guard&) = delete;
        count_guard& operator=(const count_guard&) = delete;
        ~count_guard() { --value_; }

    private:
        std::size_t& value_;
    };

    [[nodiscard]] static bounded_queue_worker_config
    validate_or_throw(bounded_queue_worker_config config) {
        if (auto valid = config.validate(); !valid) {
            throw std::invalid_argument("invalid queue worker config");
        }
        return config;
    }

    static void increment(std::uint64_t& value) {
        KWAQUE_INVARIANT(
          invariant_id{"KQ-QUEUE-WORKER-COUNTER"},
          value != std::numeric_limits<std::uint64_t>::max(),
          "queue worker counter overflow");
        ++value;
    }

    [[nodiscard]] seastar::future<> worker_loop() {
        count_guard worker{active_workers_};
        while (true) {
            KWAQUE_INVARIANT(
              invariant_id{"KQ-QUEUE-WORKER-SCOPE"},
              tasks_.has_value(),
              "queue worker has no task scope");
            auto popped = co_await queue_.pop(tasks_->abort_source());
            if (!popped) {
                if (
                  popped.error().kind == queue_failure_kind::closed
                  || popped.error().kind == queue_failure_kind::aborted) {
                    co_return;
                }
                throw std::logic_error("queue worker received a pop failure");
            }

            try {
                co_await manager_.with_workload_class(
                  classification_,
                  [this,
                   item = std::move(*popped)]() mutable -> seastar::future<> {
                      count_guard handler{active_handlers_};
                      maximum_active_handlers_ = std::max(
                        maximum_active_handlers_, active_handlers_);
                      co_await handler_(std::move(item));
                  });
            } catch (...) {
                if (reported_errors_ < config_.maximum_error_reports) {
                    increment(reported_errors_);
                    reporter_(std::current_exception());
                } else {
                    increment(suppressed_errors_);
                }
            }
        }
    }

    [[nodiscard]] seastar::future<> close_once(queue_close_mode mode) {
        co_await queue_.close(mode);
        KWAQUE_INVARIANT(
          invariant_id{"KQ-QUEUE-WORKER-CLOSE-SCOPE"},
          tasks_.has_value(),
          "started queue workers have no task scope");
        co_await tasks_->close();
        tasks_.reset();
    }

    bounded_work_queue<T>& queue_;
    resource_manager& manager_;
    workload_class classification_;
    const bounded_queue_worker_config config_;
    handler_type handler_;
    reporter_type reporter_;
    std::optional<runtime::task_scope> tasks_;
    seastar::shared_promise<> close_done_;
    std::size_t active_workers_{0};
    std::size_t active_handlers_{0};
    std::size_t maximum_active_handlers_{0};
    std::uint64_t reported_errors_{0};
    std::uint64_t suppressed_errors_{0};
    bool started_{false};
    bool closing_{false};
};

} // namespace kwaque::resource
