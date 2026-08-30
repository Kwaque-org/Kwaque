#ifndef KWAQUE_SRC_RUNTIME_PRODUCTION_TIMER_H_
#define KWAQUE_SRC_RUNTIME_PRODUCTION_TIMER_H_

#include "src/runtime/error.h"
#include "src/runtime/shard_affinity.h"
#include "src/runtime/time.h"
#include "src/runtime/timer.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>

#include <cstdint>
#include <optional>

namespace kwaque::runtime::production {

enum class timer_state : std::uint8_t {
    open,
    stopping,
    stopped,
};

class timer final {
public:
    timer() noexcept = default;
    ~timer();

    timer(const timer&) = delete;
    timer& operator=(const timer&) = delete;
    timer(timer&&) = delete;
    timer& operator=(timer&&) = delete;

    [[nodiscard]] seastar::future<result<void>>
    sleep_until(monotonic_time deadline, seastar::abort_source& caller_abort);
    void request_abort() noexcept;
    [[nodiscard]] seastar::future<result<void>> stop();

    [[nodiscard]] timer_state state() const;
    [[nodiscard]] owner_shard owner() const noexcept { return owner_; }

private:
    [[nodiscard]] seastar::future<result<void>> stop_once();

    owner_shard owner_;
    seastar::abort_source owner_abort_;
    seastar::gate waiters_;
    std::optional<seastar::shared_promise<result<void>>> stop_done_;
    timer_state state_{timer_state::open};
    bool abort_requested_{false};
    bool activated_{false};
};

static_assert(kwaque::runtime::timer_service<timer>);

} // namespace kwaque::runtime::production

#endif // KWAQUE_SRC_RUNTIME_PRODUCTION_TIMER_H_
