#ifndef KWAQUE_SRC_RUNTIME_GCRA_H_
#define KWAQUE_SRC_RUNTIME_GCRA_H_

#include "src/runtime/time.h"

#include <algorithm>
#include <cstdint>
#include <limits>

namespace kwaque::runtime {

// A rate bound in one word: the generic cell rate algorithm's virtual
// scheduling form (ITU-T I.371), which decides exactly as its continuous-state
// leaky bucket form does. The word holds the theoretical arrival time of the
// next request. A request at `now` is granted when it is no more than
// (burst - 1) intervals early, and a grant moves the arrival time one interval
// past the later of itself and `now`. So `burst` requests are granted at once
// and then one each interval, and a refused request learns the exact time it
// would be granted. It is the token bucket a shard's I/O scheduler keeps
// (replenished lazily from elapsed time, a shortfall telling when to return)
// reduced to one owner and kept in exact time: that bucket rounds elapsed time
// to whole tokens and drops the remainder at each replenish, so it is exact
// only at whole intervals. No timer, no allocation, nothing to replenish.
class gcra final {
public:
    struct decision final {
        bool granted;
        // When refused, the earliest time the same request is granted.
        monotonic_time retry_at;

        bool operator==(const decision&) const = default;
    };

    constexpr gcra() noexcept = default;

    // A burst of zero is taken as one. A zero interval spends no time per
    // grant, so a word only ever used with one grants every request. Times,
    // intervals and (burst - 1) intervals are meant to stay far below 2^64 ns
    // (about 584 years): there the word saturates, and a tolerance that
    // saturates grants every request.
    [[nodiscard]] constexpr decision try_take(
      monotonic_time now,
      monotonic_duration interval,
      std::uint32_t burst) noexcept {
        const monotonic_duration tolerance{saturating_multiply(
          interval.nanoseconds(), std::max<std::uint32_t>(burst, 1U) - 1U)};
        if (
          now.checked_add(tolerance).value_or(monotonic_time::maximum())
          < arrival_) {
            return decision{
              .granted = false,
              .retry_at = monotonic_time{
                arrival_.nanoseconds() - tolerance.nanoseconds()}};
        }
        arrival_ = std::max(arrival_, now)
                     .checked_add(interval)
                     .value_or(monotonic_time::maximum());
        return decision{.granted = true, .retry_at = now};
    }

    // Returns a grant that was not used. A word that has been idle is already
    // full, so a refund never lets more than the burst through.
    constexpr void refund(monotonic_duration interval) noexcept {
        arrival_ = arrival_.checked_sub(interval).value_or(monotonic_time{});
    }

    [[nodiscard]] constexpr monotonic_time
    theoretical_arrival() const noexcept {
        return arrival_;
    }

private:
    [[nodiscard]] static constexpr std::uint64_t
    saturating_multiply(std::uint64_t value, std::uint64_t count) noexcept {
        constexpr auto max = std::numeric_limits<std::uint64_t>::max();
        return count != 0 && value > max / count ? max : value * count;
    }

    monotonic_time arrival_{};
};

static_assert(sizeof(gcra) == sizeof(std::uint64_t));

} // namespace kwaque::runtime

#endif // KWAQUE_SRC_RUNTIME_GCRA_H_
