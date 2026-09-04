#ifndef KWAQUE_SRC_SIMULATION_VIRTUAL_TIME_H_
#define KWAQUE_SRC_SIMULATION_VIRTUAL_TIME_H_

#include "src/runtime/error.h"
#include "src/runtime/shard_affinity.h"
#include "src/runtime/time.h"
#include "src/simulation/scheduler.h"

#include <seastar/core/chunked_vector.hh>

#include <compare>
#include <cstddef>
#include <cstdint>
#include <limits>

namespace kwaque::simulation {

class clock_binding;
class monotonic_clock;
class virtual_time_test_access;
class wall_clock;

class wall_offset final {
public:
    using rep = std::int64_t;

    constexpr wall_offset() noexcept = default;
    constexpr explicit wall_offset(rep nanoseconds) noexcept
      : nanoseconds_(nanoseconds) {}

    [[nodiscard]] constexpr rep nanoseconds() const noexcept {
        return nanoseconds_;
    }
    [[nodiscard]] constexpr std::uint64_t magnitude() const noexcept {
        if (nanoseconds_ >= 0) {
            return static_cast<std::uint64_t>(nanoseconds_);
        }
        return static_cast<std::uint64_t>(-(nanoseconds_ + 1)) + 1U;
    }

    auto operator<=>(const wall_offset&) const = default;

private:
    rep nanoseconds_{0};
};

struct virtual_time_config_values final {
    runtime::wall_time epoch{};
    runtime::monotonic_duration maximum_wall_adjustment{
      std::uint64_t{604'800'000'000'000}};
};

class virtual_time_config final {
public:
    static constexpr runtime::monotonic_duration
      maximum_wall_adjustment_absolute{std::uint64_t{31'536'000'000'000'000}};

    [[nodiscard]] static runtime::result<virtual_time_config> make(
      const scheduler_limits& limits,
      virtual_time_config_values values = {}) noexcept;

    [[nodiscard]] constexpr runtime::wall_time epoch() const noexcept {
        return values_.epoch;
    }
    [[nodiscard]] constexpr runtime::monotonic_duration
    maximum_wall_adjustment() const noexcept {
        return values_.maximum_wall_adjustment;
    }
    [[nodiscard]] constexpr runtime::monotonic_time
    maximum_deadline() const noexcept {
        return maximum_deadline_;
    }

private:
    constexpr virtual_time_config(
      virtual_time_config_values values,
      runtime::monotonic_time maximum_deadline) noexcept
      : values_(values)
      , maximum_deadline_(maximum_deadline) {}

    virtual_time_config_values values_;
    runtime::monotonic_time maximum_deadline_;
};

class virtual_time final : public runtime::shard_affine {
public:
    virtual_time(scheduler& target, virtual_time_config config);
    ~virtual_time();

    virtual_time(const virtual_time&) = delete;
    virtual_time& operator=(const virtual_time&) = delete;
    virtual_time(virtual_time&&) = delete;
    virtual_time& operator=(virtual_time&&) = delete;

    [[nodiscard]] runtime::monotonic_time monotonic_now() const;
    [[nodiscard]] runtime::wall_time wall_now() const;
    [[nodiscard]] wall_offset offset() const;
    [[nodiscard]] std::size_t pending_adjustments() const;

    [[nodiscard]] runtime::result<void>
    schedule_wall_offset(runtime::monotonic_time deadline, wall_offset offset);
    [[nodiscard]] runtime::result<void> stop() noexcept;

private:
    friend class clock_binding;
    friend class monotonic_clock;
    friend class virtual_time_test_access;
    friend class wall_clock;

    static constexpr std::size_t no_adjustment
      = std::numeric_limits<std::size_t>::max();

    struct adjustment_state final {
        explicit adjustment_state(std::size_t next) noexcept
          : next_free(next) {}

        wall_offset offset;
        event_id event;
        std::size_t next_free;
        std::size_t next_active{no_adjustment};
        std::size_t previous_active{no_adjustment};
        bool active{false};
    };

    [[nodiscard]] static virtual_time& active() noexcept;
    void finish_adjustment(std::size_t index, bool apply) noexcept;
    void release_adjustment(std::size_t index) noexcept;

    static thread_local virtual_time* active_;

    scheduler* scheduler_;
    virtual_time_config config_;
    wall_offset offset_{};
    seastar::chunked_vector<adjustment_state> adjustments_;
    std::size_t first_adjustment_{no_adjustment};
    std::size_t free_adjustment_{no_adjustment};
    std::size_t pending_adjustments_{0};
    bool stopped_{false};
};

class clock_binding final {
public:
    [[nodiscard]] static bool available() noexcept;

    explicit clock_binding(virtual_time& time);
    ~clock_binding();

    clock_binding(const clock_binding&) = delete;
    clock_binding& operator=(const clock_binding&) = delete;
    clock_binding(clock_binding&&) = delete;
    clock_binding& operator=(clock_binding&&) = delete;

private:
    virtual_time* time_;
};

class monotonic_clock final {
public:
    [[nodiscard]] static runtime::monotonic_time now() noexcept {
        return virtual_time::active().monotonic_now();
    }
};

class wall_clock final {
public:
    [[nodiscard]] static runtime::wall_time now() noexcept {
        return virtual_time::active().wall_now();
    }
};

static_assert(runtime::monotonic_clock<monotonic_clock>);
static_assert(runtime::wall_clock<wall_clock>);
static_assert(sizeof(wall_offset) == sizeof(std::int64_t));

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_VIRTUAL_TIME_H_
