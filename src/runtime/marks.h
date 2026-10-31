#ifndef KWAQUE_SRC_RUNTIME_MARKS_H_
#define KWAQUE_SRC_RUNTIME_MARKS_H_

#include "src/base/names.h"

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace kwaque::runtime {

// A simulation keeps one counter for each mark, so the catalog is bounded.
inline constexpr std::size_t maximum_coverage_marks{256};
inline constexpr std::size_t maximum_coverage_mark_name_bytes{48};

// A coverage mark names a path a test or a campaign must reach. A campaign
// fails when it never reaches a required mark; a rare one is only listed.
struct coverage_mark_descriptor final {
    std::uint16_t id;
    std::string_view name;
    bool rare{false};

    bool operator==(const coverage_mark_descriptor&) const = default;
};

// The closed catalog. A mark is appended with the code that hits it and
// keeps its ID; its enumerator is its index.
enum class coverage_mark : std::uint16_t {};

inline constexpr std::array<coverage_mark_descriptor, 0> coverage_marks{};

// Names become report lines, so they are lowercase words. The `buggify_`
// prefix belongs to the reports of BUGGIFY sites.
[[nodiscard]] constexpr bool
coverage_mark_name_is_valid(std::string_view name) noexcept {
    return !name.starts_with("buggify_")
           && is_lowercase_name(name, maximum_coverage_mark_name_bytes);
}

[[nodiscard]] constexpr bool
valid_coverage_marks(std::span<const coverage_mark_descriptor> marks) noexcept {
    if (marks.size() > maximum_coverage_marks) {
        return false;
    }
    for (std::size_t left = 0; left < marks.size(); ++left) {
        if (
          marks[left].id != left + 1U
          || !coverage_mark_name_is_valid(marks[left].name)) {
            return false;
        }
        for (std::size_t right = left + 1; right < marks.size(); ++right) {
            if (marks[left].name == marks[right].name) {
                return false;
            }
        }
    }
    return true;
}

static_assert(valid_coverage_marks(coverage_marks));

template<typename Counters>
concept coverage_mark_counters = requires(
  Counters& counters, std::size_t index) {
    { counters.hit(index) } noexcept -> std::same_as<void>;
};

template<typename Backend>
concept marks_backend_contract
  = requires {
        requires std::same_as<decltype(Backend::marks_enabled), const bool>;
    } && (!Backend::marks_enabled || requires(Backend& backend) {
        typename Backend::mark_counters_type;
        requires coverage_mark_counters<typename Backend::mark_counters_type>;
        {
            backend.marks()
        } noexcept -> std::same_as<typename Backend::mark_counters_type&>;
    });

// Records that this path ran. Marks never change behaviour. A backend without
// marks discards the call, so its build names no catalog and no counter.
template<typename Backend>
void mark(Backend& backend, coverage_mark which) noexcept {
    if constexpr (!Backend::marks_enabled) {
        static_cast<void>(backend);
        static_cast<void>(which);
    } else {
        backend.marks().hit(static_cast<std::size_t>(which));
    }
}

} // namespace kwaque::runtime

#endif // KWAQUE_SRC_RUNTIME_MARKS_H_
