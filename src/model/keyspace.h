#pragma once

#include "src/base/error.h"
#include "src/base/result.h"

#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>

namespace kwaque::model {

// An ordinary u64 boundary or the exclusive mathematical endpoint 2^64.
// The full endpoint is not a hash point and has no ordinary numeric value.
class keyspace_boundary final {
public:
    [[nodiscard]] static constexpr keyspace_boundary
    ordinary(std::uint64_t value) noexcept {
        return keyspace_boundary{value, false};
    }

    [[nodiscard]] static constexpr keyspace_boundary full_end() noexcept {
        return keyspace_boundary{0, true};
    }

    [[nodiscard]] constexpr bool is_full() const noexcept { return full_; }

    [[nodiscard]] constexpr std::optional<std::uint64_t>
    value() const noexcept {
        if (full_) {
            return std::nullopt;
        }
        return value_;
    }

    constexpr std::strong_ordering
    operator<=>(const keyspace_boundary& other) const noexcept {
        if (full_ != other.full_) {
            return full_ <=> other.full_;
        }
        return value_ <=> other.value_;
    }

    bool operator==(const keyspace_boundary&) const = default;

private:
    constexpr keyspace_boundary(std::uint64_t value, bool full) noexcept
      : value_(value)
      , full_(full) {}

    std::uint64_t value_;
    bool full_;
};

// A nonempty, aligned half-open interval in the hashed u64 keyspace. Its high
// prefix bits are significant; the unused suffix bits must all be zero.
// Geometry carries neither range identity nor topology-change authority.
class keyspace_interval final {
public:
    [[nodiscard]] static result<keyspace_interval>
    make(std::uint64_t prefix, std::uint64_t depth) noexcept {
        if (depth > 64U) {
            return failure(errc::invalid_argument);
        }
        if ((prefix & ~prefix_mask(depth)) != 0) {
            return failure(errc::invalid_argument);
        }
        return keyspace_interval{prefix, static_cast<std::uint8_t>(depth)};
    }

    [[nodiscard]] static constexpr keyspace_interval root() noexcept {
        return keyspace_interval{0, 0};
    }

    [[nodiscard]] constexpr std::uint64_t prefix() const noexcept {
        return prefix_;
    }
    [[nodiscard]] constexpr std::uint8_t depth() const noexcept {
        return depth_;
    }

    [[nodiscard]] constexpr keyspace_boundary begin() const noexcept {
        return keyspace_boundary::ordinary(prefix_);
    }

    [[nodiscard]] constexpr keyspace_boundary end() const noexcept {
        const auto last = prefix_ | ~prefix_mask(depth_);
        return last == std::numeric_limits<std::uint64_t>::max()
                 ? keyspace_boundary::full_end()
                 : keyspace_boundary::ordinary(last + 1U);
    }

    // A hash point must already be a u64; reject implicit scalar conversions.
    template<std::same_as<std::uint64_t> Point>
    [[nodiscard]] constexpr bool contains(Point point) const noexcept {
        return (point & prefix_mask(depth_)) == prefix_;
    }

    [[nodiscard]] constexpr bool
    contains(keyspace_interval other) const noexcept {
        return depth_ <= other.depth_
               && (other.prefix_ & prefix_mask(depth_)) == prefix_;
    }

    [[nodiscard]] constexpr bool
    overlaps(keyspace_interval other) const noexcept {
        return other.begin() < end() && other.end() > begin();
    }

    [[nodiscard]] constexpr bool
    adjacent_to(keyspace_interval other) const noexcept {
        return end() == other.begin() || other.end() == begin();
    }

    [[nodiscard]] constexpr bool
    is_buddy_of(keyspace_interval other) const noexcept {
        if (depth_ == 0 || depth_ != other.depth_ || prefix_ == other.prefix_) {
            return false;
        }
        const auto parent_mask = prefix_mask(
          static_cast<std::uint64_t>(depth_) - 1U);
        return (prefix_ & parent_mask) == (other.prefix_ & parent_mask);
    }

    bool operator==(const keyspace_interval&) const = default;

private:
    constexpr keyspace_interval(
      std::uint64_t prefix, std::uint8_t depth) noexcept
      : prefix_(prefix)
      , depth_(depth) {}

    // The caller has already checked depth <= 64. Guard zero before shifting.
    [[nodiscard]] static constexpr std::uint64_t
    prefix_mask(std::uint64_t depth) noexcept {
        std::uint64_t mask = std::numeric_limits<std::uint64_t>::max();
        if (depth == 0) {
            mask = 0;
        } else {
            mask <<= 64U - depth;
        }
        return mask;
    }

    std::uint64_t prefix_;
    std::uint8_t depth_;
};

// Constant work per entry; the caller owns collection bounds and scheduling.
// An invalid append is sticky. An early finish only queries the current state.
class ordered_keyspace_coverage final {
public:
    constexpr explicit ordered_keyspace_coverage(
      keyspace_interval target) noexcept
      : next_(target.begin())
      , end_(target.end()) {}

    [[nodiscard]] result<void> append(keyspace_interval interval) noexcept {
        if (failed_ || interval.begin() != next_ || interval.end() > end_) {
            failed_ = true;
            return failure(errc::invalid_argument);
        }
        next_ = interval.end();
        return {};
    }

    [[nodiscard]] result<void> finish() const noexcept {
        if (failed_ || next_ != end_) {
            return failure(errc::invalid_argument);
        }
        return {};
    }

private:
    keyspace_boundary next_;
    keyspace_boundary end_;
    bool failed_{false};
};

inline constexpr std::size_t max_unordered_keyspace_intervals{64};

// Synchronous, non-mutating convenience check. Only bounded pointers into the
// input are sorted; none escape this call. Larger owners feed the ordered scan
// under their own work budgets instead of expanding this leaf's input limit.
[[nodiscard]] result<void> validate_keyspace_coverage(
  std::span<const keyspace_interval> intervals,
  keyspace_interval target) noexcept;

} // namespace kwaque::model
