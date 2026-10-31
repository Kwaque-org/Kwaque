#ifndef KWAQUE_SRC_RUNTIME_BUGGIFY_H_
#define KWAQUE_SRC_RUNTIME_BUGGIFY_H_

#include "src/base/names.h"

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace kwaque::runtime {

// A simulation keeps an activation bit, a first-evaluation bit and a firing
// count for each site, so the table is bounded.
inline constexpr std::size_t maximum_buggify_sites{256};
inline constexpr std::size_t maximum_buggify_site_name_bytes{48};

// What a firing may change. A neutral site changes only timing, a size or a
// choice every outcome tolerates, so a run's answers are the same whether it
// fires or not. An outcome site injects a refusal its caller may retry, so
// answers may differ; runs whose answers are compared turn those off.
enum class buggify_class : std::uint8_t {
    neutral,
    outcome,
};

struct buggify_site_descriptor final {
    std::uint16_t id;
    std::string_view name;
    // An active site fires with this probability at each evaluation.
    std::uint32_t fire_numerator{1};
    std::uint32_t fire_denominator{4};
    buggify_class kind{buggify_class::neutral};

    bool operator==(const buggify_site_descriptor&) const = default;
};

// The closed site table. A site is appended with the code that evaluates it
// and keeps its ID; its enumerator is its index.
enum class buggify_site : std::uint16_t {};

inline constexpr std::array<buggify_site_descriptor, 0> buggify_sites{};

// Names become report lines (`buggify_<name>`), so they are lowercase words.
[[nodiscard]] constexpr bool
buggify_name_is_valid(std::string_view name) noexcept {
    return is_lowercase_name(name, maximum_buggify_site_name_bytes);
}

[[nodiscard]] constexpr bool
valid_buggify_sites(std::span<const buggify_site_descriptor> sites) noexcept {
    if (sites.size() > maximum_buggify_sites) {
        return false;
    }
    for (std::size_t left = 0; left < sites.size(); ++left) {
        const auto& site = sites[left];
        if (
          site.id != left + 1U || !buggify_name_is_valid(site.name)
          || site.fire_numerator == 0 || site.fire_denominator == 0
          || site.fire_numerator > site.fire_denominator) {
            return false;
        }
        for (std::size_t right = left + 1; right < sites.size(); ++right) {
            if (site.name == sites[right].name) {
                return false;
            }
        }
    }
    return true;
}

static_assert(valid_buggify_sites(buggify_sites));

[[nodiscard]] constexpr const buggify_site_descriptor&
buggify_descriptor(buggify_site site) noexcept {
    return buggify_sites[static_cast<std::size_t>(site)];
}

// The caller's coordinates for one evaluation. A firing decision is a
// function of the run's seed, the site and these, so it is the same in every
// run that reaches them, whatever ran before.
struct buggify_key final {
    // A stable fold of the object the site acts on, such as the low word of
    // its identity, or zero when the site acts on none.
    std::uint64_t object{0};
    // A sequence the caller already keeps for that object, such as a
    // generation or an ordinal within one.
    std::uint64_t sequence{0};
};

template<typename Evaluator>
concept buggify_evaluator_contract = requires(
  Evaluator& evaluator, const buggify_site_descriptor& site, buggify_key key) {
    { evaluator.evaluate(site, key) } noexcept -> std::same_as<bool>;
};

template<typename Backend>
concept buggify_backend_contract
  = requires {
        requires std::same_as<decltype(Backend::buggify_enabled), const bool>;
    } && (!Backend::buggify_enabled || requires(Backend& backend) {
        typename Backend::buggify_evaluator_type;
        requires buggify_evaluator_contract<
          typename Backend::buggify_evaluator_type>;
        {
            backend.buggify()
        } noexcept -> std::same_as<typename Backend::buggify_evaluator_type&>;
    });

// Whether `site` fires here. A backend without BUGGIFY answers false through
// a discarded branch, so its build names no site table and no evaluator.
template<typename Backend>
[[nodiscard]] bool
buggify(Backend& backend, buggify_site site, buggify_key key) noexcept {
    if constexpr (!Backend::buggify_enabled) {
        static_cast<void>(backend);
        static_cast<void>(site);
        static_cast<void>(key);
        return false;
    } else {
        return backend.buggify().evaluate(buggify_descriptor(site), key);
    }
}

} // namespace kwaque::runtime

#endif // KWAQUE_SRC_RUNTIME_BUGGIFY_H_
