#pragma once

#include <compare>
#include <cstdint>
#include <limits>
#include <optional>

namespace kwaque {

namespace detail {

template<typename Tag>
class strong_count final {
public:
    using value_type = std::uint64_t;

    constexpr strong_count() noexcept = default;
    constexpr explicit strong_count(value_type value) noexcept
      : value_(value) {}

    [[nodiscard]] constexpr value_type value() const noexcept { return value_; }

    [[nodiscard]] constexpr std::optional<strong_count>
    checked_add(strong_count other) const noexcept {
        if (other.value_ > std::numeric_limits<value_type>::max() - value_) {
            return std::nullopt;
        }
        return strong_count{value_ + other.value_};
    }

    [[nodiscard]] constexpr std::optional<strong_count>
    checked_sub(strong_count other) const noexcept {
        if (other.value_ > value_) {
            return std::nullopt;
        }
        return strong_count{value_ - other.value_};
    }

    auto operator<=>(const strong_count&) const = default;

private:
    value_type value_{0};
};

struct byte_count_tag;
struct item_count_tag;

} // namespace detail

using byte_count = detail::strong_count<detail::byte_count_tag>;
using item_count = detail::strong_count<detail::item_count_tag>;

} // namespace kwaque
