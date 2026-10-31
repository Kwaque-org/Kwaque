#ifndef KWAQUE_SRC_SIMULATION_TESTS_FUZZ_MARKS_H_
#define KWAQUE_SRC_SIMULATION_TESTS_FUZZ_MARKS_H_

#include "src/runtime/buggify.h"
#include "src/runtime/marks.h"
#include "src/simulation/buggify.h"
#include "src/simulation/marks.h"

#include <bitset>
#include <cstdint>
#include <cstdio>
#include <span>

namespace kwaque::simulation::testing {

// What a fuzz campaign reached, on the lines its wrapper reads. At start the
// target prints the marks its campaign owns, each required or rare, and every
// BUGGIFY site as a rare mark `buggify_<site>`; after each input it prints
// each of them hit for the first time in this process. Nothing waits for the
// campaign to end: libFuzzer leaves through _Exit after a crash, a timeout or
// running out of memory, and every line printed before stays in the log.
class fuzz_mark_report final {
public:
    fuzz_mark_report(
      std::span<const runtime::coverage_mark_descriptor> owned,
      std::span<const runtime::buggify_site_descriptor> sites
      = runtime::buggify_sites) noexcept;

    void print_catalog(std::FILE* output) const;
    // Prints the owned marks and sites the run hit that no earlier run did.
    void report(
      const mark_counters& marks,
      const buggify_evaluator& buggify,
      std::FILE* output);

private:
    std::span<const runtime::coverage_mark_descriptor> owned_;
    std::span<const runtime::buggify_site_descriptor> sites_;
    std::bitset<runtime::maximum_coverage_marks> marks_reported_;
    std::bitset<runtime::maximum_buggify_sites> sites_reported_;
};

} // namespace kwaque::simulation::testing

#endif // KWAQUE_SRC_SIMULATION_TESTS_FUZZ_MARKS_H_
