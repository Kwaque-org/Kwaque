#ifndef KWAQUE_SRC_SIMULATION_EVENT_SINK_H_
#define KWAQUE_SRC_SIMULATION_EVENT_SINK_H_

#include "src/observability/event_log.h"
#include "src/observability/event_sequence.h"
#include "src/observability/event_sink_concept.h"
#include "src/runtime/error.h"
#include "src/runtime/shard_affinity.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>

namespace kwaque::simulation {

enum class event_replay_difference : std::uint8_t {
    expected_missing = 1,
    actual_missing = 2,
    value = 3,
};

class event_log_sink final : public runtime::shard_affine {
public:
    event_log_sink(
      observability::event_sink_identity identity,
      observability::event_log_limits limits)
      : sequence_(identity)
      , events_(identity, limits) {}

    [[nodiscard]] static runtime::result<std::unique_ptr<event_log_sink>>
    replay(
      observability::event_sink_identity identity,
      observability::event_log_limits limits,
      std::unique_ptr<observability::event_log> expected);

    [[nodiscard]] runtime::result<void>
    emit(const observability::event_request& request) noexcept;
    [[nodiscard]] runtime::result<void> emit_reserved(
      const observability::event_request& request,
      observability::event_log::reservation& reservation) noexcept;
    [[nodiscard]] runtime::result<observability::event_log::reservation>
    reserve(std::uint32_t entries, std::uint64_t encoded_bytes) noexcept {
        assert_current();
        if (stopped_) {
            return runtime::failure(
              runtime::operation_error{
                errc::closed, runtime::operation_kind::observability});
        }
        if (failure_) {
            return runtime::failure(*failure_);
        }
        return events_.reserve(entries, encoded_bytes);
    }
    [[nodiscard]] runtime::result<void> stop() noexcept;
    [[nodiscard]] runtime::result<void> finish_replay() noexcept;

    [[nodiscard]] const observability::event_log& events() const noexcept {
        assert_current();
        return events_;
    }
    [[nodiscard]] std::uint64_t last_sequence() const noexcept {
        assert_current();
        return sequence_.last_sequence();
    }
    [[nodiscard]] bool replaying() const noexcept {
        assert_current();
        return expected_ != nullptr;
    }
    [[nodiscard]] const runtime::operation_error*
    replay_failure() const noexcept {
        assert_current();
        return failure_ ? &*failure_ : nullptr;
    }

private:
    event_log_sink(
      observability::event_sink_identity identity,
      observability::event_log_limits limits,
      std::unique_ptr<observability::event_log> expected);
    [[nodiscard]] runtime::result<void> emit_with(
      const observability::event_request& request,
      observability::event_log::reservation* reservation) noexcept;
    [[nodiscard]] runtime::result<void>
    compare_next(const observability::event& actual) noexcept;
    [[nodiscard]] runtime::result<void> remember_failure(
      const observability::event* expected,
      const observability::event* actual,
      event_replay_difference difference) noexcept;

    observability::event_sequence sequence_;
    observability::event_log events_;
    std::unique_ptr<observability::event_log> expected_;
    std::optional<runtime::operation_error> failure_;
    std::size_t replay_index_{0};
    bool stopped_{false};
};

static_assert(observability::event_sink<event_log_sink>);

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_EVENT_SINK_H_
