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

    [[nodiscard]] constexpr std::optional<strong_count>
    checked_mul(value_type factor) const noexcept {
        value_type product = 0;
        if (__builtin_mul_overflow(value_, factor, &product)) {
            return std::nullopt;
        }
        return strong_count{product};
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

// NOLINTBEGIN(google-runtime-int)
namespace detail {

// Deliberately not constexpr: an overflowing size literal calls it during
// constant evaluation, so the literal does not compile and the diagnostic
// names this function.
inline void size_literal_overflows_64_bits() noexcept {}

consteval std::uint64_t
scaled_size(unsigned long long value, unsigned shift) noexcept {
    if (value > std::numeric_limits<std::uint64_t>::max() >> shift) {
        size_literal_overflows_64_bits();
    }
    return value << shift;
}

} // namespace detail

// Binary size literals in 64-bit arithmetic. An out-of-range literal does not
// compile, so a size constant can never wrap. The literals are noexcept, so a
// default member initializer that uses one keeps its class nothrow
// constructible. The namespace is inline, as std::literals is: code in kwaque
// uses the literals directly, and other code imports them with
// `using kwaque::literals::operator""_KiB` and its siblings.
inline namespace literals {

consteval std::uint64_t operator""_KiB(unsigned long long value) noexcept {
    return detail::scaled_size(value, 10U);
}

consteval std::uint64_t operator""_MiB(unsigned long long value) noexcept {
    return detail::scaled_size(value, 20U);
}

consteval std::uint64_t operator""_GiB(unsigned long long value) noexcept {
    return detail::scaled_size(value, 30U);
}

consteval std::uint64_t operator""_TiB(unsigned long long value) noexcept {
    return detail::scaled_size(value, 40U);
}

} // namespace literals
// NOLINTEND(google-runtime-int)

} // namespace kwaque
