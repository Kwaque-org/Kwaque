#ifndef KWAQUE_SRC_OBSERVABILITY_TESTING_CAPTURE_EVENT_SINK_H_
#define KWAQUE_SRC_OBSERVABILITY_TESTING_CAPTURE_EVENT_SINK_H_

#include "src/observability/event_log.h"
#include "src/observability/event_sequence.h"
#include "src/observability/event_sink_concept.h"
#include "src/runtime/error.h"
#include "src/runtime/shard_affinity.h"

#include <utility>

namespace kwaque::observability::testing {

class capture_event_sink final : public runtime::shard_affine {
public:
    capture_event_sink(event_sink_identity identity, event_log_limits limits)
      : sequence_(identity)
      , events_(identity, limits) {}

    [[nodiscard]] runtime::result<void>
    emit(const event_request& request) noexcept {
        assert_current();
        if (stopped_) {
            return runtime::failure(error(errc::closed));
        }
        auto prepared = sequence_.prepare(request);
        if (!prepared) {
            return runtime::failure(prepared.error());
        }
        if (auto appended = events_.append(prepared->value()); !appended) {
            return runtime::failure(appended.error());
        }
        prepared->commit();
        return {};
    }

    [[nodiscard]] runtime::result<void> stop() noexcept {
        assert_current();
        stopped_ = true;
        return {};
    }

    [[nodiscard]] const event_log& events() const noexcept {
        assert_current();
        return events_;
    }
    [[nodiscard]] std::uint64_t last_sequence() const noexcept {
        assert_current();
        return sequence_.last_sequence();
    }

private:
    [[nodiscard]] static runtime::operation_error error(errc code) noexcept {
        return runtime::operation_error{
          code, runtime::operation_kind::observability};
    }

    event_sequence sequence_;
    event_log events_;
    bool stopped_{false};
};

static_assert(event_sink<capture_event_sink>);

} // namespace kwaque::observability::testing

#endif // KWAQUE_SRC_OBSERVABILITY_TESTING_CAPTURE_EVENT_SINK_H_
