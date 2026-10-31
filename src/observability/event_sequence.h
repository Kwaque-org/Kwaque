#ifndef KWAQUE_SRC_OBSERVABILITY_EVENT_SEQUENCE_H_
#define KWAQUE_SRC_OBSERVABILITY_EVENT_SEQUENCE_H_

#include "src/observability/event.h"
#include "src/observability/event_identity.h"
#include "src/runtime/error.h"
#include "src/runtime/gcra.h"
#include "src/runtime/shard_affinity.h"
#include "src/runtime/time.h"

#include <array>
#include <cstdint>
#include <optional>
#include <utility>

namespace kwaque::simulation {
class event_log_sink;
}

namespace kwaque::observability {

class event_sequence_test_access;
class production_event_sink;
namespace testing {
class capture_event_sink;
}

// A rate-bounded kind admits one event a second on a shard, with no burst:
// the per-site rate a shard's request paths log at, applied per kind so a storm
// of one kind cannot crowd out another's.
inline constexpr runtime::monotonic_duration event_rate_bound_interval{
  1'000'000'000};
inline constexpr std::uint32_t event_rate_bound_burst{1};

// Assigns one shard's event sequence numbers, gapless. It also bounds the rate
// of each rate-bounded kind, on the time the event carries (the environment's
// clock, so a simulated run does not depend on host time), before a sequence
// number is taken: a dropped event takes none, and the next admitted event of
// its kind carries how many were dropped.
class event_sequence final : public runtime::shard_affine {
public:
    class reservation final {
    public:
        ~reservation();

        reservation(const reservation&) = delete;
        reservation& operator=(const reservation&) = delete;
        reservation(reservation&& other) noexcept;
        reservation& operator=(reservation&&) = delete;

        [[nodiscard]] const event& value() const noexcept {
            if (owner_ != nullptr) {
                owner_->assert_current();
            }
            return value_;
        }
        [[nodiscard]] bool active() const noexcept { return owner_ != nullptr; }
        void commit() noexcept;

    private:
        friend class event_sequence;

        reservation(event_sequence& owner, event value) noexcept
          : owner_(&owner)
          , value_(std::move(value)) {}

        event_sequence* owner_;
        event value_;
    };

    ~event_sequence();

    event_sequence(const event_sequence&) = delete;
    event_sequence& operator=(const event_sequence&) = delete;
    event_sequence(event_sequence&&) = delete;
    event_sequence& operator=(event_sequence&&) = delete;

    [[nodiscard]] const event_sink_identity& identity() const noexcept {
        assert_current();
        return identity_;
    }
    [[nodiscard]] std::uint64_t last_sequence() const noexcept {
        assert_current();
        return last_sequence_;
    }
    // Rate-bounded events dropped on this shard.
    [[nodiscard]] std::uint64_t suppressed_total() const noexcept {
        assert_current();
        return suppressed_total_;
    }

private:
    friend class event_sequence_test_access;
    friend class production_event_sink;
    friend class testing::capture_event_sink;
    friend class ::kwaque::simulation::event_log_sink;

    struct rate_bound final {
        runtime::gcra word;
        std::uint64_t pending{0};
    };

    explicit event_sequence(event_sink_identity identity) noexcept
      : identity_(identity) {}
    // No reservation means the rate bound dropped the event.
    [[nodiscard]] runtime::result<std::optional<reservation>>
    prepare(const event_request& request) noexcept;

    void finish(const event& value, bool committed) noexcept;

    event_sink_identity identity_;
    std::array<rate_bound, event_rate_bounded_kinds> bounds_{};
    std::uint64_t last_sequence_{0};
    std::uint64_t suppressed_total_{0};
    bool reserved_{false};
};

} // namespace kwaque::observability

#endif // KWAQUE_SRC_OBSERVABILITY_EVENT_SEQUENCE_H_
