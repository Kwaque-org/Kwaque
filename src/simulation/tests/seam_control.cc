#include "src/runtime/buggify.h"
#include "src/runtime/marks.h"
#include "src/simulation/buggify.h"
#include "src/simulation/deterministic_random.h"

#include <cstdint>
#include <cstdio>

// The release-seam check's positive control: a binary built as the
// simulation's are, holding the BUGGIFY evaluator's keyed firing draw, both
// tables and every site and mark name. It prints the names the check looks
// for in the release broker.
int main(int argc, char** /*argv*/) {
    namespace runtime = kwaque::runtime;
    for (const auto& site : runtime::buggify_sites) {
        std::printf(
          "buggify_site %.*s\n",
          static_cast<int>(site.name.size()),
          site.name.data());
    }
    for (const auto& mark : runtime::coverage_marks) {
        std::printf(
          "coverage_mark %.*s\n",
          static_cast<int>(mark.name.size()),
          mark.name.data());
    }
    // A keyed draw through the evaluator, so its code is in this binary as it
    // is in every simulation.
    constexpr runtime::buggify_site_descriptor control{
      .id = 1, .name = "control"};
    const kwaque::simulation::deterministic_random random{
      static_cast<std::uint64_t>(argc)};
    std::printf(
      "control_fires %d\n",
      kwaque::simulation::buggify_evaluator::fires(random, control, {}) ? 1
                                                                        : 0);
    return 0;
}
