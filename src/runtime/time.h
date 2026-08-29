#ifndef KWAQUE_SRC_RUNTIME_TIME_H_
#define KWAQUE_SRC_RUNTIME_TIME_H_

#include <bit>
#include <compare>
#include <concepts>
#include <cstdint>
#include <limits>
#include <optional>

namespace kwaque::runtime {

// A non-negative duration in canonical nanoseconds. It is intentionally not a
// std::chrono duration: runtime boundaries must make unit conversion explicit.
class monotonic_duration final {
public:
    using rep = std::uint64_t;

    constexpr monotonic_duration() noexcept = default;
    constexpr explicit monotonic_duration(rep nanoseconds) noexcept
      : nanoseconds_(nanoseconds) {}

    [[nodiscard]] constexpr rep nanoseconds() const noexcept {
        return nanoseconds_;
    }
    [[nodiscard]] static constexpr monotonic_duration maximum() noexcept {
        return monotonic_duration{std::numeric_limits<rep>::max()};
    }

    [[nodiscard]] constexpr std::optional<monotonic_duration>
    checked_add(monotonic_duration other) const noexcept {
        if (other.nanoseconds_ > maximum().nanoseconds_ - nanoseconds_) {
            return std::nullopt;
        }
        return monotonic_duration{nanoseconds_ + other.nanoseconds_};
    }

    [[nodiscard]] constexpr std::optional<monotonic_duration>
    checked_sub(monotonic_duration other) const noexcept {
        if (other.nanoseconds_ > nanoseconds_) {
            return std::nullopt;
        }
        return monotonic_duration{nanoseconds_ - other.nanoseconds_};
    }

    auto operator<=>(const monotonic_duration&) const = default;

private:
    rep nanoseconds_{0};
};

// Monotonic values are meaningful only within the clock/environment that
// produced them. Their integer form exists for checked scheduling and trace
// coordinates, not for persistence or comparison across process lifetimes.
class monotonic_time final {
public:
    using rep = std::uint64_t;

    constexpr monotonic_time() noexcept = default;
    constexpr explicit monotonic_time(rep nanoseconds) noexcept
      : nanoseconds_(nanoseconds) {}

    [[nodiscard]] constexpr rep nanoseconds() const noexcept {
        return nanoseconds_;
    }
    [[nodiscard]] static constexpr monotonic_time maximum() noexcept {
        return monotonic_time{std::numeric_limits<rep>::max()};
    }

    [[nodiscard]] constexpr std::optional<monotonic_time>
    checked_add(monotonic_duration duration) const noexcept {
        if (duration.nanoseconds() > maximum().nanoseconds_ - nanoseconds_) {
            return std::nullopt;
        }
        return monotonic_time{nanoseconds_ + duration.nanoseconds()};
    }

    [[nodiscard]] constexpr std::optional<monotonic_time>
    checked_sub(monotonic_duration duration) const noexcept {
        if (duration.nanoseconds() > nanoseconds_) {
            return std::nullopt;
        }
        return monotonic_time{nanoseconds_ - duration.nanoseconds()};
    }

    [[nodiscard]] constexpr std::optional<monotonic_duration>
    checked_elapsed_since(monotonic_time earlier) const noexcept {
        if (earlier.nanoseconds_ > nanoseconds_) {
            return std::nullopt;
        }
        return monotonic_duration{nanoseconds_ - earlier.nanoseconds_};
    }

    auto operator<=>(const monotonic_time&) const = default;

private:
    rep nanoseconds_{0};
};

// Signed Unix-epoch nanoseconds. Wall time is deliberately a different type
// from monotonic time and must never drive scheduler ordering.
class wall_time final {
public:
    using rep = std::int64_t;

    constexpr wall_time() noexcept = default;
    constexpr explicit wall_time(rep unix_nanoseconds) noexcept
      : unix_nanoseconds_(unix_nanoseconds) {}

    [[nodiscard]] constexpr rep unix_nanoseconds() const noexcept {
        return unix_nanoseconds_;
    }

    [[nodiscard]] constexpr std::optional<wall_time>
    checked_add(monotonic_duration duration) const noexcept {
        const auto ordered = ordered_rep(unix_nanoseconds_);
        if (
          duration.nanoseconds()
          > std::numeric_limits<std::uint64_t>::max() - ordered) {
            return std::nullopt;
        }
        return wall_time{signed_rep(ordered + duration.nanoseconds())};
    }

    [[nodiscard]] constexpr std::optional<wall_time>
    checked_sub(monotonic_duration duration) const noexcept {
        const auto ordered = ordered_rep(unix_nanoseconds_);
        if (duration.nanoseconds() > ordered) {
            return std::nullopt;
        }
        return wall_time{signed_rep(ordered - duration.nanoseconds())};
    }

    auto operator<=>(const wall_time&) const = default;

private:
    static constexpr std::uint64_t sign_bit = std::uint64_t{1} << 63U;

    [[nodiscard]] static constexpr std::uint64_t
    ordered_rep(rep value) noexcept {
        return std::bit_cast<std::uint64_t>(value) ^ sign_bit;
    }

    [[nodiscard]] static constexpr rep
    signed_rep(std::uint64_t value) noexcept {
        return std::bit_cast<rep>(value ^ sign_bit);
    }

    rep unix_nanoseconds_{0};
};

template<typename Clock>
concept monotonic_clock = requires {
    { Clock::now() } noexcept -> std::same_as<monotonic_time>;
};

template<typename Clock>
concept wall_clock = requires {
    { Clock::now() } noexcept -> std::same_as<wall_time>;
};

template<typename Backend>
concept clock_backend = requires {
    typename Backend::monotonic_clock;
    typename Backend::wall_clock;
} && monotonic_clock<typename Backend::monotonic_clock> && wall_clock<typename Backend::wall_clock>;

} // namespace kwaque::runtime

#endif // KWAQUE_SRC_RUNTIME_TIME_H_
