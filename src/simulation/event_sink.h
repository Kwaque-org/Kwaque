#ifndef KWAQUE_SRC_SIMULATION_EVENT_SINK_H_
#define KWAQUE_SRC_SIMULATION_EVENT_SINK_H_

#include "src/observability/event_log.h"
#include "src/observability/event_sequence.h"
#include "src/observability/event_sink_concept.h"
#include "src/runtime/error.h"
#include "src/runtime/shard_affinity.h"

namespace kwaque::simulation {

class event_log_sink final : public runtime::shard_affine {
public:
    event_log_sink(
      observability::event_sink_identity identity,
      observability::event_log_limits limits)
      : sequence_(identity)
      , events_(identity, limits) {}

    [[nodiscard]] runtime::result<void>
    emit(const observability::event_request& request) noexcept;
    [[nodiscard]] runtime::result<void> stop() noexcept;

    [[nodiscard]] const observability::event_log& events() const noexcept {
        assert_current();
        return events_;
    }
    [[nodiscard]] std::uint64_t last_sequence() const noexcept {
        assert_current();
        return sequence_.last_sequence();
    }

private:
    observability::event_sequence sequence_;
    observability::event_log events_;
    bool stopped_{false};
};

static_assert(observability::event_sink<event_log_sink>);

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_EVENT_SINK_H_
