#ifndef KWAQUE_SRC_BASE_NAMES_H_
#define KWAQUE_SRC_BASE_NAMES_H_

#include <cstddef>
#include <string_view>

namespace kwaque {

// A name that reads as one lowercase word: a letter, then letters, digits and
// underscores, at most `maximum_bytes` long. Event, metric, outcome, BUGGIFY
// site and coverage mark names follow it, so each prints as itself in a
// report line, a label or a structured field.
[[nodiscard]] constexpr bool
is_lowercase_name(std::string_view name, std::size_t maximum_bytes) noexcept {
    if (name.empty() || name.size() > maximum_bytes) {
        return false;
    }
    for (std::size_t index = 0; index < name.size(); ++index) {
        const char character = name[index];
        const bool letter = character >= 'a' && character <= 'z';
        const bool digit = character >= '0' && character <= '9';
        if (
          (!letter && !digit && character != '_') || (index == 0 && !letter)) {
            return false;
        }
    }
    return true;
}

} // namespace kwaque

#endif // KWAQUE_SRC_BASE_NAMES_H_
