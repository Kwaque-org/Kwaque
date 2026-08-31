#ifndef KWAQUE_SRC_SIMULATION_SCHEDULER_H_
#define KWAQUE_SRC_SIMULATION_SCHEDULER_H_

#include "src/runtime/error.h"
#include "src/runtime/shard_affinity.h"
#include "src/runtime/time.h"
#include "src/simulation/event_trace.h"

#include <seastar/util/noncopyable_function.hh>

#include <algorithm>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <optional>
#include <type_traits>
#include <vector>

namespace kwaque::simulation {

class scheduler;
class scheduler_test_access;

class event_id final {
public:
    constexpr event_id() noexcept = default;

    [[nodiscard]] constexpr std::uint64_t value() const noexcept {
        return value_;
    }
    [[nodiscard]] constexpr bool valid() const noexcept { return value_ != 0; }

    auto operator<=>(const event_id&) const = default;

private:
    friend class scheduler;

    constexpr explicit event_id(std::uint64_t value) noexcept
      : value_(value) {}

    std::uint64_t value_{0};
};

class event_priority final {
public:
    constexpr event_priority() noexcept = default;
    constexpr explicit event_priority(std::uint8_t value) noexcept
      : value_(value) {}

    [[nodiscard]] constexpr std::uint8_t value() const noexcept {
        return value_;
    }

    [[nodiscard]] static constexpr event_priority highest() noexcept {
        return event_priority{0};
    }
    [[nodiscard]] static constexpr event_priority normal() noexcept {
        return event_priority{128};
    }
    [[nodiscard]] static constexpr event_priority lowest() noexcept {
        return event_priority{std::numeric_limits<std::uint8_t>::max()};
    }

    auto operator<=>(const event_priority&) const = default;

private:
    std::uint8_t value_{128};
};

enum class event_cleanup_policy : std::uint8_t {
    drop,
    invoke,
};

struct scheduler_limit_values final {
    std::uint32_t pending_events{65'536};
    std::uint64_t events_per_pump{1'024};
    std::uint64_t total_events{100'000};
    runtime::monotonic_time maximum_deadline{
      std::uint64_t{31'536'000'000'000'000}};
};

class scheduler_limits final {
public:
    static constexpr std::uint32_t pending_events_absolute{1'048'576};
    static constexpr std::uint64_t events_per_pump_absolute{1'000'000};
    static constexpr std::uint64_t total_events_absolute{1'000'000};
    static constexpr runtime::monotonic_time maximum_deadline_absolute{
      static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())};

    [[nodiscard]] static runtime::result<scheduler_limits>
    make(scheduler_limit_values values) noexcept;
    [[nodiscard]] static constexpr scheduler_limits defaults() noexcept {
        return scheduler_limits{scheduler_limit_values{}};
    }

    [[nodiscard]] constexpr std::uint32_t pending_events() const noexcept {
        return values_.pending_events;
    }
    [[nodiscard]] constexpr std::uint64_t events_per_pump() const noexcept {
        return values_.events_per_pump;
    }
    [[nodiscard]] constexpr std::uint64_t total_events() const noexcept {
        return values_.total_events;
    }
    [[nodiscard]] constexpr runtime::monotonic_time
    maximum_deadline() const noexcept {
        return values_.maximum_deadline;
    }

private:
    constexpr explicit scheduler_limits(scheduler_limit_values values) noexcept
      : values_(values) {}

    scheduler_limit_values values_;
};

class scheduler final : public runtime::shard_affine {
public:
    using callback = seastar::noncopyable_function<void() noexcept>;

    class event_id_reservation final {
    public:
        event_id_reservation() noexcept = default;
        ~event_id_reservation();

        event_id_reservation(const event_id_reservation&) = delete;
        event_id_reservation& operator=(const event_id_reservation&) = delete;
        event_id_reservation(event_id_reservation&& other) noexcept;
        event_id_reservation& operator=(event_id_reservation&& other) noexcept;

        [[nodiscard]] bool active() const noexcept { return owner_ != nullptr; }
        void release() noexcept;

    private:
        friend class scheduler;

        explicit event_id_reservation(scheduler& owner) noexcept
          : owner_(&owner) {}

        scheduler* owner_{nullptr};
    };

    explicit scheduler(scheduler_limits limits, event_trace* trace = nullptr);
    ~scheduler();

    scheduler(const scheduler&) = delete;
    scheduler& operator=(const scheduler&) = delete;
    scheduler(scheduler&&) = delete;
    scheduler& operator=(scheduler&&) = delete;

    [[nodiscard]] runtime::result<event_id> schedule(
      runtime::monotonic_time deadline,
      event_priority priority,
      callback&& completion,
      trace_event_descriptor descriptor = {},
      event_cleanup_policy cleanup = event_cleanup_policy::drop,
      event_trace::reservation trace_reservation = {});
    [[nodiscard]] runtime::result<event_id_reservation> reserve_event_id();
    [[nodiscard]] runtime::result<void> can_schedule(
      runtime::monotonic_time deadline,
      trace_event_descriptor descriptor = {},
      event_cleanup_policy cleanup = event_cleanup_policy::drop) const noexcept;
    [[nodiscard]] runtime::result<event_trace::reservation>
    reserve_trace(trace_event_descriptor descriptor = {});
    [[nodiscard]] runtime::result<bool> cancel(event_id id) noexcept;

    [[nodiscard]] runtime::result<bool> step();
    // Executes at most `maximum_events` ready events and returns normally when
    // the batch is exhausted. Callers that need reactor fairness can yield
    // explicitly between batches without changing simulation event order.
    [[nodiscard]] runtime::result<std::uint64_t>
    run_ready_batch(std::uint64_t maximum_events);
    [[nodiscard]] runtime::result<std::uint64_t> run_ready();
    [[nodiscard]] runtime::result<std::optional<runtime::monotonic_time>>
    advance_to_next();
    // Advances and executes through `target` in caller-sized batches. If the
    // batch fills first, time remains at the last executed deadline; callers
    // may yield and invoke it again.
    [[nodiscard]] runtime::result<std::uint64_t> run_until_batch(
      runtime::monotonic_time target, std::uint64_t maximum_events);
    [[nodiscard]] runtime::result<std::uint64_t>
    run_until(runtime::monotonic_time target);
    // Drops all queued work after sticky trace failure. Cleanup callbacks run
    // only for events admitted with event_cleanup_policy::invoke.
    [[nodiscard]] bool discard_failed() noexcept;

    [[nodiscard]] runtime::monotonic_time now() const;
    [[nodiscard]] std::size_t pending_events() const;
    [[nodiscard]] bool has_ready_events() const;
    [[nodiscard]] std::uint64_t executed_events() const;
    [[nodiscard]] bool trace_failed() const;
    [[nodiscard]] const runtime::operation_error* trace_failure() const;
    [[nodiscard]] bool uses_trace(const event_trace& trace) const;
    // True only while a poisoned replay invokes an explicitly admitted
    // teardown callback. Such a callback must release ownership and must not
    // apply the event's normal simulated effect.
    [[nodiscard]] bool discarding_failed_event() const;
    [[nodiscard]] const scheduler_limits& limits() const {
        assert_current();
        return limits_;
    }

private:
    friend class scheduler_test_access;

    struct event final {
        runtime::monotonic_time deadline;
        event_priority priority;
        event_cleanup_policy cleanup;
        event_id id;
        callback completion;
        trace_event_descriptor descriptor;
        event_trace::reservation trace_reservation;
    };

    static_assert(sizeof(callback) == 40);
    static_assert(sizeof(event) == 136);
    static_assert(std::is_nothrow_move_constructible_v<event>);
    static_assert(std::is_nothrow_move_assignable_v<event>);

    class event_storage final {
    public:
        explicit event_storage(std::size_t capacity);

        [[nodiscard]] std::size_t size() const noexcept { return size_; }
        [[nodiscard]] std::size_t capacity() const noexcept {
            return capacity_;
        }
        [[nodiscard]] event& operator[](std::size_t index) noexcept;
        [[nodiscard]] const event& operator[](std::size_t index) const noexcept;
        [[nodiscard]] event& back() noexcept { return (*this)[size_ - 1U]; }

        void push_back(event value) noexcept;
        void pop_back() noexcept;

    private:
        using slot = std::optional<event>;
        static constexpr std::size_t entries_per_chunk = std::max<std::size_t>(
          1, maximum_contiguous_allocation_bytes / sizeof(slot));

        std::deque<std::vector<slot>> chunks_;
        std::size_t size_{0};
        std::size_t capacity_{0};
    };

    class event_index final {
    public:
        explicit event_index(std::size_t maximum_entries);

        [[nodiscard]] std::size_t size() const noexcept { return size_; }
        [[nodiscard]] std::size_t* find(std::uint64_t key) noexcept;
        [[nodiscard]] const std::size_t* find(std::uint64_t key) const noexcept;
        [[nodiscard]] std::size_t& at(std::uint64_t key) noexcept;
        [[nodiscard]] bool
        try_emplace(std::uint64_t key, std::size_t value) noexcept;
        [[nodiscard]] bool erase(std::uint64_t key) noexcept;

    private:
        struct slot final {
            std::uint64_t key{0};
            std::size_t value{0};
            // Zero is empty; one means the ideal bucket. Larger values are the
            // Robin-Hood probe distance plus one.
            std::uint32_t distance{0};
        };

        static constexpr std::size_t entries_per_chunk = std::max<std::size_t>(
          1, maximum_contiguous_allocation_bytes / sizeof(slot));

        [[nodiscard]] static std::uint64_t hash(std::uint64_t key) noexcept;
        [[nodiscard]] std::size_t bucket(std::uint64_t key) const noexcept;
        [[nodiscard]] slot& slot_at(std::size_t index) noexcept;
        [[nodiscard]] const slot& slot_at(std::size_t index) const noexcept;

        std::deque<std::vector<slot>> chunks_;
        std::size_t capacity_{0};
        std::size_t mask_{0};
        std::size_t size_{0};
    };

    class pump_scope final {
    public:
        explicit pump_scope(bool& pumping) noexcept
          : pumping_(&pumping) {
            pumping = true;
        }
        ~pump_scope() { *pumping_ = false; }

        pump_scope(const pump_scope&) = delete;
        pump_scope& operator=(const pump_scope&) = delete;
        pump_scope(pump_scope&&) = delete;
        pump_scope& operator=(pump_scope&&) = delete;

    private:
        bool* pumping_;
    };

    class discard_scope final {
    public:
        explicit discard_scope(bool& discarding) noexcept
          : discarding_(&discarding) {
            discarding = true;
        }
        ~discard_scope() { *discarding_ = false; }

        discard_scope(const discard_scope&) = delete;
        discard_scope& operator=(const discard_scope&) = delete;
        discard_scope(discard_scope&&) = delete;
        discard_scope& operator=(discard_scope&&) = delete;

    private:
        bool* discarding_;
    };

    [[nodiscard]] static bool
    earlier(const event& left, const event& right) noexcept;
    void swap_events(std::size_t left, std::size_t right) noexcept;
    void sift_up(std::size_t index) noexcept;
    void sift_down(std::size_t index) noexcept;
    [[nodiscard]] event remove_at(std::size_t index) noexcept;

    [[nodiscard]] bool has_ready_event() const noexcept;
    [[nodiscard]] runtime::result<void> execute_ready_unchecked() noexcept;
    [[nodiscard]] runtime::result<void> check_lifetime_budget() const;
    [[nodiscard]] runtime::result<void> check_control_entry() const;
    [[nodiscard]] runtime::result<void> check_trace_failure() const;
    [[nodiscard]] runtime::result<void> validate_descriptor(
      trace_event_descriptor descriptor,
      event_cleanup_policy cleanup) const noexcept;
    [[nodiscard]] bool event_id_available() const noexcept;
    void release_reserved_event_id() noexcept;
    [[nodiscard]] runtime::result<void> observe_event(
      trace_action action,
      const event& selected,
      event_trace::reservation* reservation) noexcept;
    [[nodiscard]] runtime::result<void>
    observe_time_advance(runtime::monotonic_time target) noexcept;
    [[nodiscard]] runtime::operation_error
    limit_error(std::uint64_t observed, std::uint64_t limit) const noexcept;

    scheduler_limits limits_;
    event_trace* trace_;
    event_storage heap_;
    event_index indices_;
    runtime::monotonic_time now_{};
    std::uint64_t next_event_id_{1};
    std::uint64_t executed_events_{0};
    std::uint64_t reserved_event_ids_{0};
    bool event_ids_exhausted_{false};
    bool pumping_{false};
    bool discarding_failed_event_{false};
};

[[nodiscard]] constexpr trace_scheduler_budget
trace_budget(const scheduler_limits& limits) noexcept {
    return trace_scheduler_budget{
      .pending_events = limits.pending_events(),
      .events_per_pump = limits.events_per_pump(),
      .total_events = limits.total_events(),
      .maximum_deadline = limits.maximum_deadline().nanoseconds(),
    };
}

static_assert(sizeof(event_id) == sizeof(std::uint64_t));
static_assert(sizeof(event_priority) == sizeof(std::uint8_t));
static_assert(sizeof(event_cleanup_policy) == sizeof(std::uint8_t));

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_SCHEDULER_H_
