#ifndef KWAQUE_SRC_OBSERVABILITY_EVENT_SINK_H_
#define KWAQUE_SRC_OBSERVABILITY_EVENT_SINK_H_

#include "src/observability/event.h"
#include "src/observability/event_sequence.h"
#include "src/observability/event_sink_concept.h"
#include "src/runtime/error.h"
#include "src/runtime/shard_affinity.h"

#include <seastar/util/log.hh>

namespace kwaque::observability {

class production_event_sink final : public runtime::shard_affine {
public:
    production_event_sink(
      seastar::logger& logger, event_sink_identity identity) noexcept
      : logger_(&logger)
      , sequence_(identity) {}

    [[nodiscard]] runtime::result<void>
    emit(const event_request& request) noexcept;
    [[nodiscard]] runtime::result<void> stop() noexcept;
    [[nodiscard]] bool stopped() const noexcept {
        assert_current();
        return stopped_;
    }
    [[nodiscard]] const event_sink_identity& identity() const noexcept {
        assert_current();
        return sequence_.identity();
    }
    [[nodiscard]] std::uint64_t last_sequence() const noexcept {
        assert_current();
        return sequence_.last_sequence();
    }

private:
    seastar::logger* logger_;
    event_sequence sequence_;
    bool stopped_{false};
};

static_assert(event_sink<production_event_sink>);

} // namespace kwaque::observability

#endif // KWAQUE_SRC_OBSERVABILITY_EVENT_SINK_H_
