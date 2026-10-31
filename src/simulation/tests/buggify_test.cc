#include "src/runtime/buggify.h"
#include "src/runtime/random.h"
#include "src/simulation/buggify.h"
#include "src/simulation/deterministic_random.h"
#include "src/simulation/event_trace.h"
#include "src/simulation/scheduler.h"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <span>
#include <utility>
#include <vector>

namespace {

namespace runtime = kwaque::runtime;
using kwaque::errc;
using kwaque::simulation::buggify_activation_trace_domain;
using kwaque::simulation::buggify_activation_word_is_active;
using kwaque::simulation::buggify_config;
using kwaque::simulation::buggify_evaluator;
using kwaque::simulation::buggify_firing;
using kwaque::simulation::canonical_entry_encoded_size;
using kwaque::simulation::canonical_header_encoded_size;
using kwaque::simulation::deterministic_random;
using kwaque::simulation::event_trace;
using kwaque::simulation::random_domain;
using kwaque::simulation::scheduler;
using kwaque::simulation::scheduler_limit_values;
using kwaque::simulation::scheduler_limits;
using kwaque::simulation::trace_action;
using kwaque::simulation::trace_digest;
using kwaque::simulation::trace_entry;
using kwaque::simulation::trace_event_kind;
using kwaque::simulation::trace_header;
using kwaque::simulation::trace_limit_values;
using kwaque::simulation::trace_limits;
using kwaque::simulation::trace_scheduler_budget;

using runtime::buggify_class;
using runtime::buggify_key;
using runtime::buggify_site_descriptor;

constexpr std::uint64_t seed{UINT64_C(0x5eed00000000b06)};

// Sites for the tests: the product table holds only sites with call sites.
constexpr std::array test_sites{
  buggify_site_descriptor{.id = 1, .name = "first"},
  buggify_site_descriptor{
    .id = 2, .name = "second", .fire_numerator = 3, .fire_denominator = 4},
  buggify_site_descriptor{
    .id = 3,
    .name = "refusal",
    .fire_numerator = 1,
    .fire_denominator = 10,
    .kind = buggify_class::outcome},
  buggify_site_descriptor{.id = 4, .name = "fourth"},
  buggify_site_descriptor{.id = 5, .name = "fifth"},
  buggify_site_descriptor{.id = 6, .name = "sixth"},
  buggify_site_descriptor{.id = 7, .name = "seventh"},
  buggify_site_descriptor{.id = 8, .name = "eighth"},
};
static_assert(runtime::valid_buggify_sites(test_sites));

scheduler_limits test_scheduler_limits() {
    auto limits = scheduler_limits::make(
      scheduler_limit_values{
        .pending_events = 8,
        .events_per_pump = 8,
        .total_events = 64,
        .maximum_deadline = runtime::monotonic_time{1'000},
      });
    EXPECT_TRUE(limits.has_value());
    return *limits;
}

trace_limits test_trace_limits(std::uint32_t entries = 64) {
    auto limits = trace_limits::make(
      trace_limit_values{
        .entries = entries,
        .encoded_bytes = canonical_header_encoded_size
                         + std::uint64_t{entries}
                             * canonical_entry_encoded_size,
        .line_bytes = 1'024,
      });
    EXPECT_TRUE(limits.has_value());
    return *limits;
}

trace_header test_header(
  std::uint64_t master_seed,
  scheduler_limits scheduler_budget,
  trace_limits trace_budget) {
    return trace_header::current(
      master_seed,
      kwaque::simulation::deterministic_random_algorithm_version,
      kwaque::simulation::deterministic_random_coordinate_version,
      trace_scheduler_budget{
        .pending_events = scheduler_budget.pending_events(),
        .events_per_pump = scheduler_budget.events_per_pump(),
        .total_events = scheduler_budget.total_events(),
        .maximum_deadline = scheduler_budget.maximum_deadline().nanoseconds(),
      },
      trace_budget,
      trace_digest{},
      trace_digest{});
}

// A run's trace, clock and evaluator.
struct run final {
    explicit run(
      buggify_config config,
      std::uint64_t master_seed = seed,
      std::uint32_t trace_entries = 64)
      : trace_budget(test_trace_limits(trace_entries))
      , trace(
          test_header(master_seed, scheduler_budget, trace_budget),
          trace_budget)
      , events(scheduler_budget, &trace) {
        auto made = buggify_evaluator::make(
          config, deterministic_random{master_seed}, trace, events, test_sites);
        EXPECT_TRUE(made.has_value());
        evaluator = std::move(*made);
    }

    std::vector<trace_entry> first_evaluations() const {
        std::vector<trace_entry> found;
        for (const auto& entry : trace.entries()) {
            if (entry.action == trace_action::buggify_evaluated) {
                found.push_back(entry);
            }
        }
        return found;
    }

    scheduler_limits scheduler_budget{test_scheduler_limits()};
    trace_limits trace_budget;
    event_trace trace;
    scheduler events;
    std::unique_ptr<buggify_evaluator> evaluator;
};

// A source that yields one fixed word, then fails the test if drawn again.
struct one_word final {
    std::uint64_t next_u64() noexcept {
        EXPECT_EQ(draws++, 0U);
        return word;
    }
    std::uint64_t word;
    unsigned draws{0};
};

std::uint64_t active_set(const buggify_evaluator& evaluator) {
    std::uint64_t set = 0;
    for (std::size_t index = 0; index < test_sites.size(); ++index) {
        if (evaluator.active(index)) {
            set |= std::uint64_t{1} << index;
        }
    }
    return set;
}

// A master seed under which every one of `sites` is active.
std::uint64_t seed_with_active(std::initializer_list<std::uint16_t> sites) {
    for (std::uint64_t candidate = 1;; ++candidate) {
        const deterministic_random random{candidate};
        bool all = true;
        for (const auto site : sites) {
            all = all
                  && buggify_activation_word_is_active(
                    buggify_evaluator::activation_word(random, site));
        }
        if (all) {
            return candidate;
        }
    }
}

TEST(BuggifyTest, ActivationIsTheSpecifiedQuarterChanceOfOneKeyedWord) {
    // The activation draw is chance(word, 1/4): the top two bits are zero.
    for (const std::uint64_t word :
         {std::uint64_t{0},
          (std::uint64_t{1} << 62U) - 1U,
          std::uint64_t{1} << 62U,
          std::uint64_t{3} << 62U,
          UINT64_MAX,
          UINT64_C(0x123456789abcdef0)}) {
        one_word source{.word = word};
        const auto quarter = runtime::probability_ratio::make(1, 4);
        ASSERT_TRUE(quarter.has_value());
        EXPECT_EQ(
          runtime::chance(source, *quarter),
          buggify_activation_word_is_active(word))
          << word;
    }
    // The word is the first of the site's coordinate in its own domain.
    const deterministic_random random{seed};
    const auto coordinate = kwaque::simulation::random_coordinate::make(
      random_domain::buggify_activation, 3, 0);
    ASSERT_TRUE(coordinate.has_value());
    EXPECT_EQ(
      buggify_evaluator::activation_word(random, 3),
      random.word_at(*coordinate, 0));
}

// Activation and firing words computed from the counter-based generator's
// definition and its key derivation alone, not from this code: they pin the
// two domains, each draw's coordinate and each site's draw offset.
TEST(BuggifyTest, ActivationAndFiringMatchKnownAnswers) {
    const deterministic_random random{seed};
    EXPECT_EQ(
      buggify_evaluator::activation_word(random, 1),
      UINT64_C(0xbd455b3ca8b68090));
    EXPECT_EQ(
      buggify_evaluator::activation_word(random, 2),
      UINT64_C(0x37f9acea7cc650c6));
    EXPECT_EQ(
      buggify_evaluator::activation_word(random, 3),
      UINT64_C(0x98f081b07c454fde));
    struct expected_firing final {
        buggify_key key;
        bool first;
        bool second;
    };
    for (const auto& [key, first, second] : std::array{
           expected_firing{{.object = 0, .sequence = 0}, false, true},
           expected_firing{{.object = 1, .sequence = 0}, false, true},
           expected_firing{{.object = 7, .sequence = 3}, false, false},
           expected_firing{{.object = 0xdeadbeef, .sequence = 42}, true, false},
         }) {
        EXPECT_EQ(buggify_evaluator::fires(random, test_sites[0], key), first)
          << key.object;
        EXPECT_EQ(buggify_evaluator::fires(random, test_sites[1], key), second)
          << key.object;
    }
}

TEST(BuggifyTest, TheActiveSetDependsOnTheSeedAndSiteAlone) {
    // Activation is drawn when the evaluator is made, from keyed words of the
    // run seed and the site: two runs of one seed agree, whatever they then
    // evaluate.
    run plain{buggify_config{.enabled = true}};
    run evaluated{buggify_config{.enabled = true}};
    for (std::size_t index = test_sites.size(); index-- > 0;) {
        static_cast<void>(evaluated.evaluator->evaluate(
          test_sites[index], buggify_key{.object = index, .sequence = 9}));
    }
    EXPECT_EQ(active_set(*plain.evaluator), active_set(*evaluated.evaluator));
    // And a different seed gives a different set.
    std::uint64_t differs = 0;
    for (std::uint64_t other_seed = seed + 1; other_seed < seed + 16;
         ++other_seed) {
        run changed{buggify_config{.enabled = true}, other_seed};
        differs += active_set(*changed.evaluator)
                       != active_set(*plain.evaluator)
                     ? 1U
                     : 0U;
    }
    EXPECT_GT(differs, 0U);
}

TEST(BuggifyTest, ActivationRateOverSeedsIsOneQuarter) {
    constexpr std::uint64_t seeds = std::uint64_t{1} << 20U;
    std::uint64_t active = 0;
    for (std::uint64_t master = 0; master < seeds; ++master) {
        active += buggify_activation_word_is_active(
                    buggify_evaluator::activation_word(
                      deterministic_random{master}, 1))
                    ? 1U
                    : 0U;
    }
    const double rate = static_cast<double>(active) / seeds;
    EXPECT_NEAR(rate, 0.25, 0.005);
}

TEST(BuggifyTest, FiringRateAtKeysIsTheSiteRatio) {
    constexpr std::uint64_t keys = std::uint64_t{1} << 20U;
    const deterministic_random random{seed};
    for (const auto& site : {test_sites[0], test_sites[1], test_sites[2]}) {
        std::uint64_t fired = 0;
        for (std::uint64_t key = 0; key < keys; ++key) {
            fired += buggify_evaluator::fires(
                       random,
                       site,
                       buggify_key{.object = key * 7U, .sequence = key})
                       ? 1U
                       : 0U;
        }
        const double rate = static_cast<double>(fired) / keys;
        EXPECT_NEAR(
          rate,
          static_cast<double>(site.fire_numerator) / site.fire_denominator,
          0.005)
          << site.name;
    }
}

TEST(BuggifyTest, AFiringAtAKeyIsTheSameInEveryRunThatReachesIt) {
    const auto always = seed_with_active({1, 2});
    run first{buggify_config{.enabled = true}, always};
    run second{buggify_config{.enabled = true}, always};
    std::vector<bool> answers;
    for (std::uint64_t sequence = 0; sequence < 64; ++sequence) {
        answers.push_back(first.evaluator->evaluate(
          test_sites[0], buggify_key{.object = 11, .sequence = sequence}));
    }
    // The second run reaches the keys in reverse, between other evaluations.
    for (std::uint64_t sequence = 64; sequence-- > 0;) {
        static_cast<void>(second.evaluator->evaluate(
          test_sites[1], buggify_key{.object = sequence, .sequence = 3}));
        EXPECT_EQ(
          second.evaluator->evaluate(
            test_sites[0], buggify_key{.object = 11, .sequence = sequence}),
          answers[sequence])
          << sequence;
    }
    // Two sites at one key draw different words.
    std::uint64_t same = 0;
    for (std::uint64_t sequence = 0; sequence < 256; ++sequence) {
        const buggify_key key{.object = 5, .sequence = sequence};
        same += buggify_evaluator::fires(
                  deterministic_random{always}, test_sites[0], key)
                    == buggify_evaluator::fires(
                      deterministic_random{always}, test_sites[3], key)
                  ? 1U
                  : 0U;
    }
    EXPECT_LT(same, 256U);
}

TEST(BuggifyTest, DisabledItDrawsTracesAndCountsNothing) {
    run disabled{buggify_config{}};
    for (std::size_t index = 0; index < test_sites.size(); ++index) {
        EXPECT_FALSE(disabled.evaluator->evaluate(test_sites[index], {}));
        EXPECT_FALSE(disabled.evaluator->active(index));
        EXPECT_FALSE(disabled.evaluator->evaluated(index));
        EXPECT_EQ(disabled.evaluator->firings(index), 0U);
    }
    EXPECT_TRUE(disabled.trace.entries().empty());
    // Nothing was reserved: the whole budget is still free.
    auto everything = disabled.trace.reserve(
      disabled.trace_budget.entries(),
      std::uint64_t{disabled.trace_budget.entries()}
        * canonical_entry_encoded_size);
    EXPECT_TRUE(everything.has_value());
}

TEST(BuggifyTest, EachSitesFirstEvaluationIsTracedOnceActiveOrNot) {
    run traced{buggify_config{.enabled = true}};
    for (int round = 0; round < 3; ++round) {
        for (std::size_t index = 0; index < test_sites.size(); ++index) {
            static_cast<void>(traced.evaluator->evaluate(
              test_sites[index],
              buggify_key{
                .object = index,
                .sequence = static_cast<std::uint64_t>(round)}));
        }
    }
    const auto entries = traced.first_evaluations();
    ASSERT_EQ(entries.size(), test_sites.size());
    const deterministic_random random{seed};
    for (std::size_t index = 0; index < entries.size(); ++index) {
        const auto& entry = entries[index];
        EXPECT_EQ(entry.kind, trace_event_kind::keyed_random);
        EXPECT_EQ(entry.domain, buggify_activation_trace_domain);
        EXPECT_EQ(entry.stable_id, test_sites[index].id);
        EXPECT_EQ(
          entry.value,
          buggify_evaluator::activation_word(random, test_sites[index].id));
        EXPECT_EQ(entry.result, traced.evaluator->active(index) ? 1U : 0U);
        EXPECT_TRUE(traced.evaluator->evaluated(index));
    }
    // The trace encodes and decodes: the validator accepts every entry.
    const auto encoded = traced.trace.encode();
    ASSERT_TRUE(encoded.has_value());
    EXPECT_TRUE(event_trace::decode(*encoded, traced.trace_budget).has_value());
}

TEST(BuggifyTest, ARunReplaysAndADifferentOrderOfFirstEvaluationsDoesNot) {
    run recorded{buggify_config{.enabled = true}};
    for (const auto index : {2U, 0U, 1U}) {
        static_cast<void>(recorded.evaluator->evaluate(test_sites[index], {}));
    }
    const auto encoded = recorded.trace.encode();
    ASSERT_TRUE(encoded.has_value());

    const auto replay = [&](std::initializer_list<unsigned> order) {
        auto decoded = event_trace::decode(*encoded, recorded.trace_budget);
        EXPECT_TRUE(decoded.has_value());
        auto trace = event_trace::replay(
          recorded.trace.header(), recorded.trace_budget, std::move(*decoded));
        EXPECT_TRUE(trace.has_value());
        scheduler events{recorded.scheduler_budget, trace->get()};
        {
            auto evaluator = buggify_evaluator::make(
              buggify_config{.enabled = true},
              deterministic_random{seed},
              **trace,
              events,
              test_sites);
            EXPECT_TRUE(evaluator.has_value());
            for (const auto index : order) {
                static_cast<void>(
                  (*evaluator)->evaluate(test_sites[index], {}));
            }
        }
        return (*trace)->finish_replay().has_value();
    };
    EXPECT_TRUE(replay({2U, 0U, 1U}));
    EXPECT_FALSE(replay({0U, 2U, 1U}));
}

TEST(BuggifyTest, FiringPoliciesNarrowWhichActiveSitesFire) {
    // Both sites active and certain to fire when allowed.
    constexpr std::array certain{
      buggify_site_descriptor{
        .id = 1,
        .name = "neutral_site",
        .fire_numerator = 1,
        .fire_denominator = 1},
      buggify_site_descriptor{
        .id = 2,
        .name = "outcome_site",
        .fire_numerator = 1,
        .fire_denominator = 1,
        .kind = buggify_class::outcome},
    };
    const auto both = seed_with_active({1, 2});
    const auto limits = test_trace_limits();
    const auto scheduler_budget = test_scheduler_limits();
    for (const auto firing :
         {buggify_firing::every_site,
          buggify_firing::neutral_sites,
          buggify_firing::none}) {
        event_trace trace{test_header(both, scheduler_budget, limits), limits};
        scheduler events{scheduler_budget, &trace};
        auto evaluator = buggify_evaluator::make(
          buggify_config{.enabled = true, .firing = firing},
          deterministic_random{both},
          trace,
          events,
          certain);
        ASSERT_TRUE(evaluator.has_value());
        const bool neutral = (*evaluator)->evaluate(certain[0], {});
        const bool outcome = (*evaluator)->evaluate(certain[1], {});
        EXPECT_EQ(neutral, firing != buggify_firing::none);
        EXPECT_EQ(outcome, firing == buggify_firing::every_site);
        EXPECT_EQ((*evaluator)->firings(0), neutral ? 1U : 0U);
        // Activation and the trace do not depend on the policy.
        EXPECT_TRUE((*evaluator)->active(0) && (*evaluator)->active(1));
        std::size_t traced = 0;
        for (const auto& entry : trace.entries()) {
            traced += entry.action == trace_action::buggify_evaluated ? 1U : 0U;
        }
        EXPECT_EQ(traced, 2U);
    }
    // A healed phase turns firing off mid-run.
    event_trace trace{test_header(both, scheduler_budget, limits), limits};
    scheduler events{scheduler_budget, &trace};
    auto evaluator = buggify_evaluator::make(
      buggify_config{.enabled = true},
      deterministic_random{both},
      trace,
      events,
      certain);
    ASSERT_TRUE(evaluator.has_value());
    EXPECT_TRUE((*evaluator)->evaluate(certain[0], {}));
    (*evaluator)->set_firing(buggify_firing::none);
    EXPECT_FALSE((*evaluator)->evaluate(certain[0], {}));
    EXPECT_EQ((*evaluator)->firings(0), 1U);
}

TEST(BuggifyTest, MakingRefusesBadTablesAndPoliciesAndReservesFirst) {
    const auto limits = test_trace_limits();
    const auto scheduler_budget = test_scheduler_limits();
    event_trace trace{test_header(seed, scheduler_budget, limits), limits};
    scheduler events{scheduler_budget, &trace};
    constexpr std::array misnumbered{
      buggify_site_descriptor{.id = 2, .name = "misnumbered"}};
    constexpr std::array duplicated{
      buggify_site_descriptor{.id = 1, .name = "same"},
      buggify_site_descriptor{.id = 2, .name = "same"}};
    constexpr std::array never{
      buggify_site_descriptor{.id = 1, .name = "never", .fire_numerator = 0}};
    constexpr std::array uppercase{
      buggify_site_descriptor{.id = 1, .name = "Upper"}};
    for (const std::span<const buggify_site_descriptor> sites :
         {std::span<const buggify_site_descriptor>{misnumbered},
          std::span<const buggify_site_descriptor>{duplicated},
          std::span<const buggify_site_descriptor>{never},
          std::span<const buggify_site_descriptor>{uppercase}}) {
        auto made = buggify_evaluator::make(
          buggify_config{.enabled = true},
          deterministic_random{seed},
          trace,
          events,
          sites);
        ASSERT_FALSE(made.has_value());
        EXPECT_EQ(made.error().code(), errc::invalid_argument);
    }
    auto unknown = buggify_evaluator::make(
      buggify_config{.enabled = true, .firing = static_cast<buggify_firing>(3)},
      deterministic_random{seed},
      trace,
      events,
      test_sites);
    ASSERT_FALSE(unknown.has_value());
    EXPECT_EQ(unknown.error().code(), errc::invalid_argument);

    // A trace without room for every first evaluation refuses the run.
    run small{buggify_config{}, seed, 4};
    auto cramped = buggify_evaluator::make(
      buggify_config{.enabled = true},
      deterministic_random{seed},
      small.trace,
      small.events,
      test_sites);
    ASSERT_FALSE(cramped.has_value());
}

TEST(BuggifyTest, TheTraceRefusesAFirstEvaluationThatIsNotWellFormed) {
    const auto limits = test_trace_limits();
    const auto scheduler_budget = test_scheduler_limits();
    const deterministic_random random{seed};
    const auto word = buggify_evaluator::activation_word(random, 1);
    const trace_entry valid{
      .action = trace_action::buggify_evaluated,
      .kind = trace_event_kind::keyed_random,
      .domain = buggify_activation_trace_domain,
      .stable_id = 1,
      .value = word,
      .result = buggify_activation_word_is_active(word) ? 1U : 0U,
    };
    {
        event_trace trace{test_header(seed, scheduler_budget, limits), limits};
        EXPECT_TRUE(trace.observe(valid).has_value());
    }
    auto altered = [&](auto change) {
        auto entry = valid;
        change(entry);
        event_trace trace{test_header(seed, scheduler_budget, limits), limits};
        const auto observed = trace.observe(entry);
        return !observed.has_value()
               && observed.error().code() == errc::malformed_data;
    };
    EXPECT_TRUE(altered([](trace_entry& e) { e.result ^= 1U; }));
    EXPECT_TRUE(altered([](trace_entry& e) { e.result = 2; }));
    EXPECT_TRUE(altered([](trace_entry& e) { e.stable_id = 0; }));
    EXPECT_TRUE(altered([](trace_entry& e) {
        e.stable_id = runtime::maximum_buggify_sites + 1U;
    }));
    EXPECT_TRUE(altered([](trace_entry& e) { e.domain = 7; }));
    EXPECT_TRUE(altered([](trace_entry& e) { e.coordinate_a = 1; }));
    EXPECT_TRUE(altered([](trace_entry& e) { e.coordinate_b = 1; }));
    EXPECT_TRUE(
      altered([](trace_entry& e) { e.kind = trace_event_kind::generic; }));
    EXPECT_TRUE(altered([](trace_entry& e) { e.event_id = 1; }));
}

struct production_like final {
    static constexpr bool buggify_enabled = false;
};
static_assert(runtime::buggify_backend_contract<production_like>);

TEST(BuggifyTest, ProductionAnswersFalseWithoutNamingASite) {
    production_like backend;
    EXPECT_FALSE(
      runtime::buggify(
        backend, static_cast<runtime::buggify_site>(0), buggify_key{}));
}

} // namespace
