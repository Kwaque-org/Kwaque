#pragma once

#include <array>
#include <cstddef>
#include <source_location>
#include <string_view>

namespace kwaque {

class invariant_id final {
public:
    static constexpr std::size_t max_size = 32;

    constexpr explicit invariant_id(std::string_view value) noexcept {
        if (value.empty() || value.size() > max_size) {
            return;
        }
        for (const char character : value) {
            const bool upper = character >= 'A' && character <= 'Z';
            const bool digit = character >= '0' && character <= '9';
            if (!upper && !digit && character != '-' && character != '_') {
                return;
            }
        }
        for (std::size_t index = 0; index < value.size(); ++index) {
            value_[index] = value[index];
        }
        size_ = value.size();
        valid_ = true;
    }

    [[nodiscard]] constexpr std::string_view value() const noexcept {
        return {value_.data(), size_};
    }

    [[nodiscard]] constexpr bool valid() const noexcept { return valid_; }

    bool operator==(const invariant_id&) const = default;

private:
    std::array<char, max_size> value_{};
    std::size_t size_{0};
    bool valid_{false};
};

inline constexpr std::size_t max_invariant_expression_size = 128;
inline constexpr std::size_t max_invariant_context_size = 160;
inline constexpr std::size_t max_invariant_diagnostic_size = 2048;

[[noreturn]] void invariant_failed(
  invariant_id id,
  std::string_view expression,
  std::string_view context,
  std::source_location location = std::source_location::current());

} // namespace kwaque

#define KWAQUE_INVARIANT(id, expression, context)                              \
    do {                                                                       \
        if (!static_cast<bool>(expression)) [[unlikely]] {                     \
            ::kwaque::invariant_failed(                                        \
              (id), #expression, (context), std::source_location::current());  \
        }                                                                      \
    } while (false)

#ifndef KWAQUE_ENABLE_DEBUG_ASSERTIONS
#define KWAQUE_ENABLE_DEBUG_ASSERTIONS 1
#endif

#if KWAQUE_ENABLE_DEBUG_ASSERTIONS
#define KWAQUE_DEBUG_ASSERT(id, expression, context)                           \
    KWAQUE_INVARIANT((id), expression, (context))
#else
#define KWAQUE_DEBUG_ASSERT(id, expression, context)                           \
    do {                                                                       \
    } while (false)
#endif
