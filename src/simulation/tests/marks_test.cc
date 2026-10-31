#include "src/runtime/buggify.h"
#include "src/runtime/marks.h"
#include "src/simulation/buggify.h"
#include "src/simulation/deterministic_random.h"
#include "src/simulation/event_trace.h"
#include "src/simulation/marks.h"
#include "src/simulation/scheduler.h"
#include "src/simulation/tests/fuzz_marks.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <utility>

namespace {

namespace runtime = kwaque::runtime;
namespace simulation = kwaque::simulation;
using runtime::coverage_mark_descriptor;
using simulation::mark_counters;
using simulation::mark_window;

// Marks for the tests: the product catalog holds only marks with call sites.
constexpr std::array test_marks{
  coverage_mark_descriptor{.id = 1, .name = "reached"},
  coverage_mark_descriptor{.id = 2, .name = "seldom", .rare = true},
  coverage_mark_descriptor{.id = 3, .name = "unreached"},
};
static_assert(runtime::valid_coverage_marks(test_marks));
static_assert(!runtime::valid_coverage_marks(
  std::array{coverage_mark_descriptor{.id = 2, .name = "misnumbered"}}));
static_assert(!runtime::valid_coverage_marks(
  std::array{
    coverage_mark_descriptor{.id = 1, .name = "same"},
    coverage_mark_descriptor{.id = 2, .name = "same"}}));
static_assert(!runtime::valid_coverage_marks(
  std::array{coverage_mark_descriptor{.id = 1, .name = "Upper"}}));
static_assert(!runtime::valid_coverage_marks(
  std::array{coverage_mark_descriptor{.id = 1, .name = ""}}));
// Report lines for BUGGIFY sites own the prefix.
static_assert(!runtime::valid_coverage_marks(
  std::array{coverage_mark_descriptor{.id = 1, .name = "buggify_roll"}}));

// A backend that counts marks, as the simulation does.
class counting_backend final {
public:
    using mark_counters_type = mark_counters;
    static constexpr bool marks_enabled = true;

    mark_counters_type& marks() noexcept { return counters_; }

private:
    mark_counters counters_{test_marks};
};

struct production_like final {
    static constexpr bool marks_enabled = false;
};

static_assert(runtime::marks_backend_contract<counting_backend>);
static_assert(runtime::marks_backend_contract<production_like>);

template<typename Backend>
concept can_check_marks = requires(Backend& backend) {
    simulation::check_mark(backend, runtime::coverage_mark{});
};
// A test cannot ask a backend without marks whether one was hit.
static_assert(can_check_marks<counting_backend>);
static_assert(!can_check_marks<production_like>);

TEST(MarksTest, EachRunCountsItsOwnHits) {
    mark_counters first{test_marks};
    mark_counters second{test_marks};
    first.hit(0);
    first.hit(0);
    second.hit(1);
    EXPECT_EQ(first.hits(0), 2U);
    EXPECT_EQ(first.hits(1), 0U);
    EXPECT_EQ(second.hits(0), 0U);
    EXPECT_EQ(second.hits(1), 1U);
    EXPECT_EQ(first.hits(test_marks.size()), 0U);
}

TEST(MarksTest, AWindowSeesOnlyHitsAfterItOpens) {
    mark_counters counters{test_marks};
    counters.hit(0);
    const mark_window window{counters, 0};
    EXPECT_TRUE(window.expect_not_hit());
    EXPECT_FALSE(window.expect_hit());
    counters.hit(1);
    EXPECT_TRUE(window.expect_not_hit());
    counters.hit(0);
    EXPECT_TRUE(window.expect_hit());
    EXPECT_FALSE(window.expect_not_hit());
    EXPECT_EQ(window.hits(), 1U);
    EXPECT_EQ(window.name(), "reached");
}

TEST(MarksTest, AMarkReachesTheBackendsCountersAndProductionIgnoresIt) {
    counting_backend backend;
    const auto window = simulation::check_mark(
      backend, static_cast<runtime::coverage_mark>(1));
    runtime::mark(backend, static_cast<runtime::coverage_mark>(1));
    EXPECT_TRUE(window.expect_hit());
    EXPECT_EQ(backend.marks().hits(0), 0U);
    EXPECT_EQ(backend.marks().hits(1), 1U);

    production_like production;
    runtime::mark(production, static_cast<runtime::coverage_mark>(0));
}

// A campaign's report, read back from the file it was printed to.
class report_output final {
public:
    report_output()
      : file_(std::tmpfile()) {
        EXPECT_NE(file_, nullptr);
    }
    report_output(const report_output&) = delete;
    report_output& operator=(const report_output&) = delete;
    ~report_output() { std::fclose(file_); }

    [[nodiscard]] std::FILE* file() const noexcept { return file_; }
    // What was printed since the last take.
    [[nodiscard]] std::string take() {
        std::string text;
        EXPECT_EQ(std::fseek(file_, read_, SEEK_SET), 0);
        for (int character = std::fgetc(file_); character != EOF;
             character = std::fgetc(file_)) {
            text.push_back(static_cast<char>(character));
        }
        read_ = std::ftell(file_);
        return text;
    }

private:
    std::FILE* file_;
    long read_{0};
};

constexpr std::array report_sites{
  runtime::buggify_site_descriptor{
    .id = 1, .name = "early_roll", .fire_numerator = 1, .fire_denominator = 1},
};

// A seed under which the site is active, so it fires at every evaluation.
std::uint64_t active_seed() {
    for (std::uint64_t seed = 1;; ++seed) {
        if (
          simulation::buggify_activation_word_is_active(
            simulation::buggify_evaluator::activation_word(
              simulation::deterministic_random{seed}, 1))) {
            return seed;
        }
    }
}

TEST(MarksTest, ACampaignPrintsItsCatalogThenEachFirstHitOnce) {
    const auto seed = active_seed();
    auto scheduler_budget = simulation::scheduler_limits::make(
      simulation::scheduler_limit_values{
        .pending_events = 8,
        .events_per_pump = 8,
        .total_events = 64,
        .maximum_deadline = runtime::monotonic_time{1'000},
      });
    ASSERT_TRUE(scheduler_budget.has_value());
    auto trace_budget = simulation::trace_limits::make(
      simulation::trace_limit_values{
        .entries = 8,
        .encoded_bytes = simulation::canonical_header_encoded_size
                         + 8U * simulation::canonical_entry_encoded_size,
        .line_bytes = 1'024,
      });
    ASSERT_TRUE(trace_budget.has_value());

    simulation::testing::fuzz_mark_report report{test_marks, report_sites};
    report_output output;
    report.print_catalog(output.file());
    EXPECT_EQ(
      output.take(),
      "kwaque-mark-catalog reached required\n"
      "kwaque-mark-catalog seldom rare\n"
      "kwaque-mark-catalog unreached required\n"
      "kwaque-mark-catalog buggify_early_roll rare\n");

    // Each input is a run of its own; a mark is reported the first time any
    // run hits it, and never again.
    for (int input = 0; input < 3; ++input) {
        simulation::event_trace trace{
          simulation::trace_header::current(
            seed,
            simulation::deterministic_random_algorithm_version,
            simulation::deterministic_random_coordinate_version,
            simulation::trace_scheduler_budget{
              .pending_events = scheduler_budget->pending_events(),
              .events_per_pump = scheduler_budget->events_per_pump(),
              .total_events = scheduler_budget->total_events(),
              .maximum_deadline
              = scheduler_budget->maximum_deadline().nanoseconds(),
            },
            *trace_budget,
            simulation::trace_digest{},
            simulation::trace_digest{}),
          *trace_budget};
        simulation::scheduler events{*scheduler_budget, &trace};
        auto buggify = simulation::buggify_evaluator::make(
          simulation::buggify_config{.enabled = input == 1},
          simulation::deterministic_random{seed},
          trace,
          events,
          report_sites);
        ASSERT_TRUE(buggify.has_value());
        mark_counters marks{test_marks};
        if (input >= 1) {
            marks.hit(0);
        }
        if (input == 2) {
            marks.hit(1);
        }
        static_cast<void>((*buggify)->evaluate(report_sites[0], {}));
        report.report(marks, **buggify, output.file());
        const auto printed = output.take();
        switch (input) {
        case 0:
            EXPECT_EQ(printed, "");
            break;
        case 1:
            EXPECT_EQ(
              printed,
              "kwaque-mark-hit reached\nkwaque-mark-hit buggify_early_roll\n");
            break;
        default:
            EXPECT_EQ(printed, "kwaque-mark-hit seldom\n");
            break;
        }
    }
}

} // namespace
