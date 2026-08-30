#ifndef KWAQUE_SRC_SIMULATION_TIMER_H_
#define KWAQUE_SRC_SIMULATION_TIMER_H_

#include "src/runtime/error.h"
#include "src/runtime/shard_affinity.h"
#include "src/runtime/time.h"
#include "src/runtime/timer.h"
#include "src/simulation/scheduler.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>

#include <cstddef>
#include <cstdint>
#include <list>
#include <optional>
#include <utility>

namespace kwaque::simulation {

enum class timer_state : std::uint8_t {
    open,
    stopping,
    stopped,
};

class timer final : public runtime::shard_affine {
public:
    explicit timer(scheduler& target) noexcept;
    ~timer();

    timer(const timer&) = delete;
    timer& operator=(const timer&) = delete;
    timer(timer&&) = delete;
    timer& operator=(timer&&) = delete;

    [[nodiscard]] seastar::future<runtime::result<void>> sleep_until(
      runtime::monotonic_time deadline, seastar::abort_source& caller_abort);
    void request_abort() noexcept;
    [[nodiscard]] seastar::future<runtime::result<void>> stop();

    [[nodiscard]] timer_state state() const;
    [[nodiscard]] std::size_t pending_waits() const;

private:
    enum class wait_phase : std::uint8_t {
        waiting,
        terminal_scheduled,
        discard_pending,
    };

    struct wait_state final {
        wait_state(
          scheduler::event_id_reservation id_reservation,
          event_trace::reservation trace_reservation)
          : terminal_id(std::move(id_reservation))
          , terminal_trace(std::move(trace_reservation)) {}

        seastar::promise<runtime::result<void>> completion;
        std::optional<seastar::abort_source::subscription> caller_subscription;
        scheduler::event_id_reservation terminal_id;
        event_trace::reservation terminal_trace;
        std::list<wait_state>::iterator position;
        event_id event;
        wait_phase phase{wait_phase::waiting};
        errc outcome{errc::success};
    };

    [[nodiscard]] static seastar::future<runtime::result<void>>
    ready_failure(runtime::operation_error error);
    void schedule_abort(wait_state& wait, errc outcome) noexcept;
    void complete_wait(wait_state& wait) noexcept;
    void discard_wait(
      wait_state& wait, const runtime::operation_error& failure) noexcept;
    void discard_remaining(const runtime::operation_error& failure) noexcept;
    void finish_stop() noexcept;

    scheduler* scheduler_;
    std::list<wait_state> waits_;
    std::optional<seastar::shared_promise<runtime::result<void>>> stop_done_;
    std::optional<runtime::operation_error> stop_failure_;
    timer_state state_{timer_state::open};
    bool abort_requested_{false};
    bool activated_{false};
};

static_assert(runtime::timer_service<timer>);

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_TIMER_H_
