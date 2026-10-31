#pragma once

#include "src/base/invariant.h"
#include "src/runtime/deadline_wheel.h"
#include "src/runtime/error.h"
#include "src/runtime/shard_affinity.h"
#include "src/runtime/time.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/util/optimized_optional.hh>

#include <boost/intrusive/list.hpp>

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <optional>
#include <utility>

namespace kwaque::runtime {

// The shard's two exceptions for ending an operation: made once per thread
// and compared by pointer, so ending a wait never allocates or throws. A
// Seastar abort source builds its default exception even when it is given
// one (abort_source.hh 131), and a semaphore wait aborted without one makes
// its own; neither is used on these paths.
[[nodiscard]] const std::exception_ptr& deadline_exceeded() noexcept;
[[nodiscard]] const std::exception_ptr& operation_aborted() noexcept;

// An operation's abort source. Aborting it plainly reports the operation
// aborted; expire() reports its deadline passed. Both use the shard's cached
// exceptions, so neither allocates.
class deadline_abort_source final : public seastar::abort_source {
public:
    [[nodiscard]] std::exception_ptr
    get_default_exception() const noexcept override {
        return operation_aborted();
    }

    void expire() noexcept { request_abort_ex(deadline_exceeded()); }
};

// What a wait ended by `source` reports.
[[nodiscard]] operation_error
ended_by(const seastar::abort_source& source) noexcept;
// The same, from the exception an abort hands its subscribers: none for a
// plain abort.
[[nodiscard]] operation_error
ended_by(const std::optional<std::exception_ptr>& reason) noexcept;

// One operation's deadline: a wheel entry that expires the operation's abort
// source, which every wait the operation makes follows. An abort of `parent`
// (a caller's or the shard's stop) aborts it too.
class operation_deadline final : public wheel_entry {
public:
    operation_deadline() noexcept
      : wheel_entry(&on_expiry) {}

    operation_deadline(const operation_deadline&) = delete;
    operation_deadline& operator=(const operation_deadline&) = delete;
    operation_deadline(operation_deadline&&) = delete;
    operation_deadline& operator=(operation_deadline&&) = delete;
    ~operation_deadline() = default;

    // Arms the deadline; one already past expires the source now.
    template<typename Deadlines>
    void arm(Deadlines& deadlines, monotonic_time at) noexcept {
        if (
          deadlines.arm(*this, at, deadline_wheel::precision::exact)
          == deadline_wheel::insertion::due) {
            source_.expire();
        }
    }

    // Follows `parent`: its abort aborts this operation.
    void follow(seastar::abort_source& parent) {
        parent_subscription_ = parent.subscribe(
          [this](const std::optional<std::exception_ptr>&) noexcept {
              source_.request_abort();
          });
        if (parent.abort_requested()) {
            source_.request_abort();
        }
    }

    // Disarms the deadline and stops following; the operation ended.
    void finish() noexcept {
        static_cast<void>(cancel());
        parent_subscription_ = {};
    }

    [[nodiscard]] deadline_abort_source& source() noexcept { return source_; }

private:
    static void on_expiry(wheel_entry& entry) noexcept {
        static_cast<operation_deadline&>(entry).source_.expire();
    }

    deadline_abort_source source_;
    seastar::optimized_optional<seastar::abort_source::subscription>
      parent_subscription_;
};

// The shape of every waiter: an object in the waiting caller's frame,
// linked into its owner's list, optionally into the shard's deadline wheel,
// and subscribed to an abort source with a callback that captures one
// pointer. Whoever takes it off its list first tells it, exactly once:
// tell() unlinks it from the list, the wheel and the abort source, then
// resolves the caller's future, and every later teller finds nothing to do.
// Nothing here allocates, and tell() never suspends.
template<typename T>
class waiter
  : public wheel_entry
  , public boost::intrusive::list_base_hook<
      boost::intrusive::link_mode<boost::intrusive::auto_unlink>> {
public:
    using outcome = result<T>;

    // Called after the waiter left on its own, by an abort or its deadline,
    // so its owner can reconsider the waiters behind it.
    using departure = void (*)(waiter&, void*) noexcept;

    waiter() noexcept
      : wheel_entry(&on_deadline) {}

    waiter(const waiter&) = delete;
    waiter& operator=(const waiter&) = delete;
    waiter(waiter&&) = delete;
    waiter& operator=(waiter&&) = delete;

    ~waiter() {
        KWAQUE_INVARIANT(
          invariant_id{"KQ-WAITER-WAITING"},
          state_ != state::waiting,
          "a waiter was destroyed while waiting");
    }

    // Starts the wait: checks the abort and the deadline before linking
    // anything, then links into `owner`, arms the deadline and subscribes to
    // the abort source. The caller has already checked the condition it
    // waits for. Returns the future the caller awaits.
    template<typename List, typename Deadlines>
    [[nodiscard]] seastar::future<outcome> enlist(
      List& owner,
      seastar::abort_source* abort,
      Deadlines* deadlines,
      std::optional<monotonic_time> deadline,
      deadline_wheel::precision kind = deadline_wheel::precision::exact) {
        KWAQUE_INVARIANT(
          invariant_id{"KQ-WAITER-REUSED"},
          state_ == state::idle,
          "a waiter was enlisted twice");
        state_ = state::waiting;
        auto told = promise_.get_future();
        if (abort != nullptr && abort->abort_requested()) {
            static_cast<void>(tell(failure(ended_by(*abort))));
            return told;
        }
        if (deadline) {
            KWAQUE_INVARIANT(
              invariant_id{"KQ-WAITER-DEADLINES"},
              deadlines != nullptr,
              "a waiter's deadline has no wheel");
            if (
              deadlines->arm(*this, *deadline, kind)
              == deadline_wheel::insertion::due) {
                static_cast<void>(tell(failure(
                  operation_error{errc::timed_out, operation_kind::runtime})));
                return told;
            }
        }
        owner.push_back(static_cast<typename List::value_type&>(*this));
        if (abort != nullptr) {
            subscription_ = abort->subscribe(
              [this](const std::optional<std::exception_ptr>& reason) noexcept {
                  on_abort(reason);
              });
        }
        return told;
    }

    // Ends the wait with `told`; false if it already ended.
    bool tell(outcome told) noexcept {
        if (state_ != state::waiting) {
            return false;
        }
        state_ = state::told;
        if (this->is_linked()) {
            this->unlink();
        }
        static_cast<void>(cancel());
        if (!aborting_) {
            subscription_ = {};
        }
        promise_.set_value(std::move(told));
        return true;
    }

    void on_departure(departure callback, void* context) noexcept {
        departure_ = callback;
        departure_context_ = context;
    }

    [[nodiscard]] bool waiting() const noexcept {
        return state_ == state::waiting;
    }

private:
    enum class state : std::uint8_t {
        idle,
        waiting,
        told,
    };

    void on_abort(const std::optional<std::exception_ptr>& reason) noexcept {
        // The subscription is running this callback and must outlive it.
        aborting_ = true;
        if (tell(failure(ended_by(reason)))) {
            depart();
        }
    }

    static void on_deadline(wheel_entry& entry) noexcept {
        auto& self = static_cast<waiter&>(entry);
        if (
          self.tell(failure(
            operation_error{errc::timed_out, operation_kind::runtime}))) {
            self.depart();
        }
    }

    void depart() noexcept {
        if (departure_ != nullptr) {
            departure_(*this, departure_context_);
        }
    }

    seastar::promise<outcome> promise_;
    seastar::optimized_optional<seastar::abort_source::subscription>
      subscription_;
    departure departure_{nullptr};
    void* departure_context_{nullptr};
    state state_{state::idle};
    bool aborting_{false};
};

// The type a waiter with no deadline names for its wheel.
struct no_deadlines final {
    deadline_wheel::insertion
    arm(wheel_entry&, monotonic_time, deadline_wheel::precision) noexcept {
        return deadline_wheel::insertion::due;
    }
};

// A FIFO wait for units with a native semaphore's rules: try_wait succeeds
// only when nobody is queued, grants go in arrival order, a wait ends on an
// abort or a deadline, and a broken queue fails every wait. Each waiter is an
// object in its caller's frame: unlike a native semaphore wait, which
// allocates its queue's first entry (abortable_fifo.hh 89-92, 213-219), and
// an empty abort that allocates its exception (semaphore.hh 171-196), no wait
// here allocates. Unlike the native semaphore, a waiter that leaves by abort
// or deadline lets the waiters behind it take units that are free, so a
// large request that gave up never strands smaller ones.
class unit_queue final : public shard_affine {
public:
    class units;

    // A queued wait for `count` units.
    struct queued final : waiter<void> {
        std::size_t count{0};
    };

    explicit unit_queue(std::size_t available) noexcept
      : available_(available) {}

    unit_queue(const unit_queue&) = delete;
    unit_queue& operator=(const unit_queue&) = delete;
    unit_queue(unit_queue&&) = delete;
    unit_queue& operator=(unit_queue&&) = delete;
    ~unit_queue();

    [[nodiscard]] bool try_wait(std::size_t count = 1) noexcept;
    void signal(std::size_t count = 1) noexcept;
    // Fails every queued wait and every later one with `reason`; later
    // signals are ignored.
    void broken(errc reason = errc::closed) noexcept;

    [[nodiscard]] std::size_t available_units() const noexcept {
        return available_;
    }
    [[nodiscard]] std::size_t waiters() const noexcept;
    [[nodiscard]] bool is_broken() const noexcept {
        return broken_.has_value();
    }

    // One wait for units; co_await it. It lives in the caller's frame while
    // it waits.
    template<typename Deadlines>
    class basic_awaiter final {
    public:
        basic_awaiter(const basic_awaiter&) = delete;
        basic_awaiter& operator=(const basic_awaiter&) = delete;
        basic_awaiter(basic_awaiter&&) = delete;
        basic_awaiter& operator=(basic_awaiter&&) = delete;
        ~basic_awaiter() = default;

        auto operator co_await() {
            return seastar::internal::awaiter<false, result<void>>(
              queue_->start(node_, abort_, deadlines_, deadline_));
        }

    private:
        friend class unit_queue;

        basic_awaiter(
          unit_queue& queue,
          std::size_t count,
          seastar::abort_source* abort,
          Deadlines* deadlines,
          std::optional<monotonic_time> deadline) noexcept
          : queue_(&queue)
          , abort_(abort)
          , deadlines_(deadlines)
          , deadline_(deadline) {
            node_.count = count;
        }

        unit_queue* queue_;
        seastar::abort_source* abort_;
        Deadlines* deadlines_;
        std::optional<monotonic_time> deadline_;
        queued node_;
    };

    [[nodiscard]] basic_awaiter<no_deadlines>
    wait(std::size_t count, seastar::abort_source* abort = nullptr) noexcept {
        return basic_awaiter<no_deadlines>{
          *this, count, abort, nullptr, std::nullopt};
    }

    template<typename Deadlines>
    [[nodiscard]] basic_awaiter<Deadlines> wait(
      std::size_t count,
      seastar::abort_source* abort,
      Deadlines& deadlines,
      monotonic_time deadline) noexcept {
        return basic_awaiter<Deadlines>{
          *this, count, abort, &deadlines, deadline};
    }

private:
    using list = boost::intrusive::
      list<queued, boost::intrusive::constant_time_size<false>>;

    template<typename Deadlines>
    seastar::future<result<void>> start(
      queued& node,
      seastar::abort_source* abort,
      Deadlines* deadlines,
      std::optional<monotonic_time> deadline) {
        assert_current();
        if (broken_) {
            return seastar::make_ready_future<result<void>>(
              failure(operation_error{*broken_, operation_kind::runtime}));
        }
        if (try_wait(node.count)) {
            return seastar::make_ready_future<result<void>>();
        }
        node.on_departure(&departed, this);
        return node.enlist(waiters_, abort, deadlines, deadline);
    }

    // Grants queued waits in order while the first one fits.
    void grant() noexcept;

    static void departed(waiter<void>&, void* self) noexcept {
        static_cast<unit_queue*>(self)->grant();
    }

    list waiters_;
    std::size_t available_;
    std::optional<errc> broken_;
};

// Units taken from a unit_queue, returned when dropped.
class unit_queue::units final {
public:
    units() noexcept = default;
    units(unit_queue& queue, std::size_t count) noexcept
      : queue_(&queue)
      , count_(count) {}
    units(units&& other) noexcept
      : queue_(other.queue_)
      , count_(std::exchange(other.count_, 0)) {}
    units& operator=(units&& other) noexcept {
        if (this != &other) {
            return_all();
            queue_ = other.queue_;
            count_ = std::exchange(other.count_, 0);
        }
        return *this;
    }
    units(const units&) = delete;
    units& operator=(const units&) = delete;
    ~units() { return_all(); }

    [[nodiscard]] std::size_t count() const noexcept { return count_; }

    void return_all() noexcept {
        if (count_ != 0) {
            queue_->signal(std::exchange(count_, 0));
        }
    }

private:
    unit_queue* queue_{nullptr};
    std::size_t count_{0};
};

// Takes `count` units now if the queue allows it.
[[nodiscard]] std::optional<unit_queue::units>
try_get_units(unit_queue& queue, std::size_t count) noexcept;

} // namespace kwaque::runtime
