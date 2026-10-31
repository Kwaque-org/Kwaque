#include "src/simulation/tests/fuzz_marks.h"

#include "src/base/invariant.h"

#include <cstddef>

namespace kwaque::simulation::testing {

namespace {

template<std::size_t Size>
[[nodiscard]] bool
first_report(std::bitset<Size>& reported, std::size_t index) noexcept {
    if (reported[index]) {
        return false;
    }
    reported[index] = true;
    return true;
}

} // namespace

fuzz_mark_report::fuzz_mark_report(
  std::span<const runtime::coverage_mark_descriptor> owned,
  std::span<const runtime::buggify_site_descriptor> sites) noexcept
  : owned_(owned)
  , sites_(sites) {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-SIM-MARK-REPORT"},
      owned_.size() <= runtime::maximum_coverage_marks
        && sites_.size() <= runtime::maximum_buggify_sites,
      "a fuzz campaign owns more marks than a catalog holds");
}

void fuzz_mark_report::print_catalog(std::FILE* output) const {
    for (const auto& owned : owned_) {
        std::fprintf(
          output,
          "kwaque-mark-catalog %.*s %s\n",
          static_cast<int>(owned.name.size()),
          owned.name.data(),
          owned.rare ? "rare" : "required");
    }
    for (const auto& site : sites_) {
        std::fprintf(
          output,
          "kwaque-mark-catalog buggify_%.*s rare\n",
          static_cast<int>(site.name.size()),
          site.name.data());
    }
    std::fflush(output);
}

void fuzz_mark_report::report(
  const mark_counters& marks,
  const buggify_evaluator& buggify,
  std::FILE* output) {
    for (const auto& owned : owned_) {
        // An owned mark is a catalog entry, so its index is its ID less one.
        const std::size_t index = owned.id - 1U;
        KWAQUE_INVARIANT(
          invariant_id{"KQ-SIM-MARK-REPORT"},
          index < marks.catalog().size() && marks.catalog()[index] == owned,
          "a fuzz campaign owns a mark its run does not count");
        if (marks.hits(index) != 0 && first_report(marks_reported_, index)) {
            std::fprintf(
              output,
              "kwaque-mark-hit %.*s\n",
              static_cast<int>(owned.name.size()),
              owned.name.data());
        }
    }
    KWAQUE_INVARIANT(
      invariant_id{"KQ-SIM-MARK-REPORT"},
      buggify.sites().data() == sites_.data()
        && buggify.sites().size() == sites_.size(),
      "a fuzz campaign reports another site table than its run evaluates");
    for (std::size_t index = 0; index < sites_.size(); ++index) {
        if (
          buggify.firings(index) != 0 && first_report(sites_reported_, index)) {
            std::fprintf(
              output,
              "kwaque-mark-hit buggify_%.*s\n",
              static_cast<int>(sites_[index].name.size()),
              sites_[index].name.data());
        }
    }
    std::fflush(output);
}

} // namespace kwaque::simulation::testing
