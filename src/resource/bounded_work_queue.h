#pragma once

#include "src/base/invariant.h"
#include "src/base/result.h"
#include "src/base/units.h"
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
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/util/optimized_optional.hh>

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
    static constexpr std::size_t maximum_supported_producer_waiters = 256;

    item_count maximum_items;
    byte_count maximum_bytes;
    std::size_t maximum_producer_waiters{0};

    [[nodiscard]] result<void> validate() const noexcept {
        // Every admitted item costs at least one byte, so an item bound above
        // the byte bound could never be the binding constraint.
        if (
          maximum_items.value() == 0 || maximum_bytes.value() < 2
          || maximum_bytes.value() > seastar::semaphore::max_counter()
          || maximum_items.value() > maximum_bytes.value()
          || maximum_producer_waiters > maximum_supported_producer_waiters) {
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
    byte_count requested_bytes;
    item_count queued_items;

    [[nodiscard]] errc code() const noexcept {
        switch (kind) {
        case queue_failure_kind::closed:
            return errc::closed;
        case queue_failure_kind::aborted:
            return errc::aborted;
        case queue_failure_kind::invalid_cost:
            return errc::invalid_argument;
        case queue_failure_kind::oversized:
            return errc::out_of_range;
        case queue_failure_kind::producer_waiters_exhausted:
            return errc::resource_exhausted;
        }
        return errc::invariant_violation;
    }

    [[nodiscard]] runtime::operation_error detail() const noexcept {
        runtime::operation_error error{
          code(), runtime::operation_kind::resource};
        static_cast<void>(error.add_context(
          runtime::operation_context_key::bytes, requested_bytes.value()));
        static_cast<void>(error.add_context(
          runtime::operation_context_key::items, queued_items.value()));
        return error;
    }

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

struct bounded_queue_worker_config final {
    static constexpr std::size_t maximum_supported_workers = 64;

    std::size_t workers;
    std::uint64_t maximum_error_reports;

    [[nodiscard]] result<void> validate() const noexcept {
        return workers == 0 || workers > maximum_supported_workers
                 ? failure(errc::invalid_argument)
                 : result<void>{};
    }

    bool operator==(const bounded_queue_worker_config&) const = default;
};

// FIFO queue admission is serialized so an expensive producer cannot be
// bypassed by later cheap producers. Item, local byte, and producer-waiter
// storage are bounded independently. A manager-backed queue additionally draws
// every byte from its workload class budget. At most maximum_producer_waiters
// producers are suspended at once; one of those slots is reserved for the
// producer holding the admission turn, so a later arrival can never displace
// the producer already at the head of the admission order.
template<typename T>
requires std::is_nothrow_move_constructible_v<T>
class bounded_work_queue final : public runtime::shard_affine {
public:
    using handler_type = seastar::noncopyable_function<seastar::future<>(T)>;
    using reporter_type
      = seastar::noncopyable_function<void(std::exception_ptr) noexcept>;

    explicit bounded_work_queue(bounded_work_queue_config config)
      : config_(validate_or_throw(config))
      , owned_admission_(std::in_place, config_.maximum_bytes.value())
      , memory_admission_(&*owned_admission_) {
        producer_turn_.ensure_space_for_waiters(max_queued_producers());
    }

    bounded_work_queue(
      bounded_work_queue_config config,
      resource_manager& manager,
      workload_class classification)
      : config_(validate_or_throw(config))
      , manager_(&manager)
      , workload_(manager.acquire_workload(classification))
      , memory_admission_(&workload_->memory_admission()) {
        if (config_.maximum_bytes > manager_->hard_budget(classification)) {
            throw std::invalid_argument(
              "queue byte capacity exceeds its workload budget");
        }
        producer_turn_.ensure_space_for_waiters(max_queued_producers());
    }

    void start_workers(
      bounded_queue_worker_config config,
      handler_type handler,
      reporter_type reporter) {
        assert_current();
        if (workers_started_) {
            throw std::logic_error("queue workers already started");
        }
        if (state_ != bounded_work_queue_state::open || closing_) {
            throw std::logic_error("queue is closing");
        }
        if (manager_ == nullptr || !workload_) {
            throw std::logic_error(
              "standalone queue cannot start managed workers");
        }
        if (!manager_->ready()) {
            throw std::logic_error("resource manager is not ready");
        }
        if (active_consumers_ != 0 || waiting_consumers_ != 0) {
            throw std::logic_error("queue has active manual consumers");
        }
        if (auto valid = config.validate(); !valid) {
            throw std::invalid_argument("invalid queue worker config");
        }
        if (!handler || !reporter) {
            throw std::invalid_argument(
              "queue worker callbacks must be present");
        }

        worker_config_ = config;
        handler_ = std::move(handler);
        reporter_ = std::move(reporter);
        tasks_.emplace();
        workers_started_ = true;
        for (std::size_t index = 0; index < config.workers; ++index) {
            const auto accepted = tasks_->spawn(
              [this] { return worker_loop(); });
            KWAQUE_INVARIANT(
              invariant_id{"KQ-QUEUE-WORKER-SPAWNED"},
              accepted.has_value(),
              "fresh worker task scope rejected a worker");
        }
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
            && bytes_held_.value() == 0 && producer_turn_.waiters() == 0
            && producer_turn_.current() == 1 && consumer_turn_.waiters() == 0
            && consumer_turn_.current() == 1,
          "bounded work queue destroyed with retained work or waiters");
        KWAQUE_INVARIANT(
          invariant_id{"KQ-QUEUE-WORKERS-DRAINED"},
          active_workers_ == 0 && active_handlers_ == 0 && !tasks_,
          "bounded work queue destroyed with active workers");
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
        return bytes_held_;
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

    [[nodiscard]] std::size_t configured_workers() const {
        assert_current();
        return worker_config_ ? worker_config_->workers : 0;
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

    [[nodiscard]] seastar::future<queue_result<void>>
    push(T item, byte_count cost, seastar::abort_source& abort_source) {
        assert_current();
        operation_token operation{*this, active_producers_};
        if (auto invalid = validate_cost(cost)) {
            co_return std::unexpected(std::move(*invalid));
        }
        if (state_ != bounded_work_queue_state::open) {
            co_return std::unexpected(
              failure(queue_failure_kind::closed, cost));
        }
        if (abort_source.abort_requested()) {
            co_return std::unexpected(
              failure(queue_failure_kind::aborted, cost));
        }

        std::optional<producer_cancellation> cancellation;
        auto cancellation_source = [&]() -> seastar::abort_source& {
            if (!cancellation) {
                cancellation.emplace(*this, abort_source);
            }
            return cancellation->source();
        };

        auto turn = seastar::try_get_units(producer_turn_, 1);
        producer_wait_token waiting{*this};
        if (!turn) {
            if (!waiting.try_engage_queued()) {
                co_return std::unexpected(failure(
                  queue_failure_kind::producer_waiters_exhausted, cost));
            }
            auto& push_abort = cancellation_source();
            try {
                turn = co_await seastar::coroutine::without_preemption_check(
                  seastar::get_units(producer_turn_, 1, push_abort));
            } catch (...) {
                if (push_abort.abort_requested()) {
                    co_return std::unexpected(
                      interruption_failure(abort_source, cost));
                }
                throw;
            }
        }

        while (true) {
            if (state_ != bounded_work_queue_state::open) {
                co_return std::unexpected(
                  failure(queue_failure_kind::closed, cost));
            }
            if (abort_source.abort_requested()) {
                co_return std::unexpected(
                  failure(queue_failure_kind::aborted, cost));
            }
            if (can_admit_locally(cost)) {
                break;
            }

            // This producer holds the turn, so it takes the slot reserved for
            // the head of the admission order. Only a queue that permits no
            // suspended producers at all can refuse it.
            if (!waiting.engage_admitting()) {
                co_return std::unexpected(failure(
                  queue_failure_kind::producer_waiters_exhausted, cost));
            }
            auto& push_abort = cancellation_source();
            if (push_abort.abort_requested()) {
                continue;
            }
            co_await producer_condition_.wait([this, cost, &push_abort] {
                return state_ != bounded_work_queue_state::open
                       || push_abort.abort_requested()
                       || can_admit_locally(cost);
            });
        }

        KWAQUE_INVARIANT(
          invariant_id{"KQ-WORK-QUEUE-MEMORY-ADMISSION"},
          memory_admission_ != nullptr,
          "queue has no memory admission handle");
        auto acquired = seastar::try_get_units(
          *memory_admission_, cost.value());
        if (!acquired) {
            if (!waiting.engage_admitting()) {
                co_return std::unexpected(failure(
                  queue_failure_kind::producer_waiters_exhausted, cost));
            }
            auto& push_abort = cancellation_source();
            try {
                acquired
                  = co_await seastar::coroutine::without_preemption_check(
                    seastar::get_units(
                      *memory_admission_, cost.value(), push_abort));
            } catch (...) {
                if (push_abort.abort_requested()) {
                    co_return std::unexpected(
                      interruption_failure(abort_source, cost));
                }
                throw;
            }
        }
        if (
          state_ != bounded_work_queue_state::open
          || abort_source.abort_requested()) {
            co_return std::unexpected(interruption_failure(abort_source, cost));
        }

        items_.push_back(
          admitted_item{
            .units = std::move(*acquired),
            .item = std::move(item),
          });
        const auto held = bytes_held_.checked_add(cost);
        KWAQUE_INVARIANT(
          invariant_id{"KQ-WORK-QUEUE-BYTE-ADMISSION"},
          held.has_value() && *held <= config_.maximum_bytes,
          "queue byte admission exceeded its local capacity");
        bytes_held_ = *held;
        consumer_condition_.signal();
        co_return queue_result<void>{};
    }

    [[nodiscard]] seastar::future<queue_result<T>>
    pop(seastar::abort_source& abort_source) {
        assert_current();
        if (workers_started_) {
            throw std::logic_error(
              "manual pop is unavailable after workers start");
        }
        auto admitted = co_await pop_admitted(abort_source);
        if (!admitted) {
            co_return std::unexpected(std::move(admitted.error()));
        }
        auto item = std::move(admitted->item);
        release_admitted(admitted->units);
        co_return queue_result<T>{std::move(item)};
    }

private:
    struct admitted_item final {
        seastar::semaphore_units<> units;
        T item;
    };

    [[nodiscard]] seastar::future<queue_result<admitted_item>>
    pop_admitted(seastar::abort_source& abort_source) {
        assert_current();
        operation_token operation{*this, active_consumers_};
        consumer_wait_token waiting{*this};
        auto turn = seastar::try_get_units(consumer_turn_, 1);
        if (!turn) {
            waiting.engage();
            try {
                turn = co_await seastar::coroutine::without_preemption_check(
                  seastar::get_units(consumer_turn_, 1, abort_source));
            } catch (...) {
                if (abort_source.abort_requested()) {
                    co_return std::unexpected(
                      interruption_failure(abort_source, byte_count{}));
                }
                throw;
            }
        }
        while (true) {
            if (state_ != bounded_work_queue_state::open && items_.empty()) {
                co_return std::unexpected(
                  failure(queue_failure_kind::closed, byte_count{}));
            }
            if (abort_source.abort_requested()) {
                co_return std::unexpected(
                  failure(queue_failure_kind::aborted, byte_count{}));
            }
            if (!items_.empty()) {
                auto admitted = std::move(items_.front());
                items_.pop_front();
                co_return queue_result<admitted_item>{std::move(admitted)};
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

public:
    [[nodiscard]] seastar::future<> close(queue_close_mode mode) {
        assert_current();
        if (!closing_) {
            closing_ = true;
            begin_close(mode);
            auto completion = close_once().then_wrapped(
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
            begin_close(queue_close_mode::abort);
        }
        return close_done_.get_shared_future();
    }

private:
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

    class count_guard final {
    public:
        explicit count_guard(std::size_t& value) noexcept
          : value_(value) {
            KWAQUE_INVARIANT(
              invariant_id{"KQ-QUEUE-WORKER-COUNT"},
              value_ != std::numeric_limits<std::size_t>::max(),
              "queue worker counter overflow");
            ++value_;
        }
        count_guard(const count_guard&) = delete;
        count_guard& operator=(const count_guard&) = delete;
        ~count_guard() { --value_; }

    private:
        std::size_t& value_;
    };

    class processing_item final {
    public:
        processing_item(bounded_work_queue& queue, admitted_item item) noexcept
          : queue_(&queue)
          , item_(std::move(item)) {}
        processing_item(processing_item&& other) noexcept
          : queue_(std::exchange(other.queue_, nullptr))
          , item_(std::move(other.item_)) {}
        processing_item& operator=(processing_item&&) = delete;
        processing_item(const processing_item&) = delete;
        processing_item& operator=(const processing_item&) = delete;
        ~processing_item() {
            if (queue_ != nullptr) {
                queue_->release_admitted(item_.units);
            }
        }

        [[nodiscard]] T&& take() noexcept { return std::move(item_.item); }

    private:
        bounded_work_queue* queue_;
        admitted_item item_;
    };

    class producer_cancellation final {
    public:
        producer_cancellation(
          bounded_work_queue& queue, seastar::abort_source& caller)
          : queue_(queue)
          , caller_subscription_(caller.subscribe(
              [this]() noexcept { combined_.request_abort(); }))
          , queue_subscription_(queue_.producer_abort_.subscribe(
              [this]() noexcept { combined_.request_abort(); }))
          , wakeup_subscription_(combined_.subscribe(
              [this]() noexcept { queue_.producer_condition_.broadcast(); })) {
            if (
              caller.abort_requested()
              || queue_.producer_abort_.abort_requested()) {
                combined_.request_abort();
            }
        }

        producer_cancellation(const producer_cancellation&) = delete;
        producer_cancellation& operator=(const producer_cancellation&) = delete;

        [[nodiscard]] seastar::abort_source& source() noexcept {
            return combined_;
        }

    private:
        bounded_work_queue& queue_;
        seastar::abort_source combined_;
        seastar::optimized_optional<seastar::abort_source::subscription>
          caller_subscription_;
        seastar::optimized_optional<seastar::abort_source::subscription>
          queue_subscription_;
        seastar::optimized_optional<seastar::abort_source::subscription>
          wakeup_subscription_;
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

    [[nodiscard]] std::optional<queue_failure>
    validate_cost(byte_count cost) const noexcept {
        if (cost.value() == 0) {
            return failure(queue_failure_kind::invalid_cost, cost);
        }
        if (cost > config_.maximum_bytes) {
            return failure(queue_failure_kind::oversized, cost);
        }
        return std::nullopt;
    }

    [[nodiscard]] queue_failure
    failure(queue_failure_kind kind, byte_count cost) const noexcept {
        return queue_failure{
          .kind = kind,
          .requested_bytes = cost,
          .queued_items = item_count{static_cast<std::uint64_t>(items_.size())},
        };
    }

    [[nodiscard]] queue_failure interruption_failure(
      const seastar::abort_source& caller_abort,
      byte_count cost) const noexcept {
        return state_ == bounded_work_queue_state::open
                   && caller_abort.abort_requested()
                 ? failure(queue_failure_kind::aborted, cost)
                 : failure(queue_failure_kind::closed, cost);
    }

    // One slot of the producer-waiter bound is reserved for the turn holder, so
    // the queue for the turn is bounded one lower. A queue configured to permit
    // no suspended producers permits none in either population.
    [[nodiscard]] std::size_t max_queued_producers() const noexcept {
        return config_.maximum_producer_waiters == 0
                 ? 0
                 : config_.maximum_producer_waiters - 1;
    }

    [[nodiscard]] bool can_admit_locally(byte_count cost) const {
        const auto remaining = config_.maximum_bytes.checked_sub(bytes_held_);
        return items_.size() < config_.maximum_items.value()
               && remaining.has_value() && *remaining >= cost;
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
            auto popped = co_await pop_admitted(tasks_->abort_source());
            if (!popped) {
                if (
                  popped.error().kind == queue_failure_kind::closed
                  || popped.error().kind == queue_failure_kind::aborted) {
                    co_return;
                }
                throw std::logic_error("queue worker received a pop failure");
            }

            processing_item processing{*this, std::move(*popped)};
            try {
                KWAQUE_INVARIANT(
                  invariant_id{"KQ-QUEUE-WORKER-HANDLE"},
                  workload_.has_value(),
                  "queue worker lost its workload handle");
                co_await seastar::with_scheduling_group(
                  workload_->scheduling_group(), [this, &processing] {
                      return invoke_handler(processing.take());
                  });
            } catch (...) {
                KWAQUE_INVARIANT(
                  invariant_id{"KQ-QUEUE-WORKER-REPORTER"},
                  static_cast<bool>(reporter_),
                  "started queue workers have no reporter");
                if (reported_errors_ < worker_config_->maximum_error_reports) {
                    increment(reported_errors_);
                    reporter_(std::current_exception());
                } else {
                    increment(suppressed_errors_);
                }
            }
        }
    }

    [[nodiscard]] seastar::future<> invoke_handler(T item) {
        count_guard handler{active_handlers_};
        maximum_active_handlers_ = std::max(
          maximum_active_handlers_, active_handlers_);
        co_await handler_(std::move(item));
    }

    void release_admitted(seastar::semaphore_units<>& units) noexcept {
        const auto remaining = bytes_held_.checked_sub(
          byte_count{units.count()});
        KWAQUE_INVARIANT(
          invariant_id{"KQ-WORK-QUEUE-BYTE-RELEASE"},
          remaining.has_value(),
          "queue released more bytes than it held");
        bytes_held_ = *remaining;
        units.return_all();
        producer_condition_.signal();
        maybe_finish_close();
    }

    void begin_close(queue_close_mode mode) {
        if (state_ == bounded_work_queue_state::closed) {
            return;
        }
        if (state_ == bounded_work_queue_state::open) {
            state_ = mode == queue_close_mode::drain
                       ? bounded_work_queue_state::draining
                       : bounded_work_queue_state::aborting;
        } else if (mode == queue_close_mode::abort) {
            state_ = bounded_work_queue_state::aborting;
        }

        if (!producer_abort_.abort_requested()) {
            producer_abort_.request_abort();
        }
        if (state_ == bounded_work_queue_state::aborting) {
            while (!items_.empty()) {
                auto& admitted = items_.front();
                release_admitted(admitted.units);
                items_.pop_front();
            }
        }
        producer_condition_.broadcast();
        consumer_condition_.broadcast();
        maybe_finish_close();
    }

    [[nodiscard]] seastar::future<> close_once() {
        co_await drained_done_.get_shared_future();
        std::exception_ptr failure;
        if (tasks_) {
            try {
                co_await tasks_->close();
            } catch (...) {
                failure = std::current_exception();
            }
            tasks_.reset();
        }
        workload_.reset();
        if (failure) {
            std::rethrow_exception(failure);
        }
    }

    void maybe_finish_close() noexcept {
        if (
          state_ != bounded_work_queue_state::open
          && state_ != bounded_work_queue_state::closed && items_.empty()
          && queued_producers_ == 0 && admitting_producers_ == 0
          && waiting_consumers_ == 0 && active_producers_ == 0
          && active_consumers_ == 0 && bytes_held_.value() == 0) {
            state_ = bounded_work_queue_state::closed;
            if (!drained_done_.available()) {
                drained_done_.set_value();
            }
        }
    }

    const bounded_work_queue_config config_;
    std::optional<seastar::semaphore> owned_admission_;
    resource_manager* manager_{nullptr};
    std::optional<workload_handle> workload_;
    seastar::semaphore* memory_admission_{nullptr};
    std::deque<admitted_item> items_;
    seastar::semaphore producer_turn_{1};
    seastar::semaphore consumer_turn_{1};
    seastar::condition_variable producer_condition_;
    seastar::condition_variable consumer_condition_;
    seastar::abort_source producer_abort_;
    seastar::shared_promise<> drained_done_;
    seastar::shared_promise<> close_done_;
    std::optional<bounded_queue_worker_config> worker_config_;
    handler_type handler_;
    reporter_type reporter_;
    std::optional<runtime::task_scope> tasks_;
    byte_count bytes_held_;
    bounded_work_queue_state state_{bounded_work_queue_state::open};
    std::size_t queued_producers_{0};
    std::size_t admitting_producers_{0};
    std::size_t waiting_consumers_{0};
    std::size_t active_producers_{0};
    std::size_t active_consumers_{0};
    std::size_t active_workers_{0};
    std::size_t active_handlers_{0};
    std::size_t maximum_active_handlers_{0};
    std::uint64_t reported_errors_{0};
    std::uint64_t suppressed_errors_{0};
    bool workers_started_{false};
    bool closing_{false};
};

} // namespace kwaque::resource
