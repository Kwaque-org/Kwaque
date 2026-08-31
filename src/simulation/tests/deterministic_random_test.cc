#include "src/simulation/deterministic_random.h"
#include "src/simulation/deterministic_random_test_support.h"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace {

using kwaque::simulation::deterministic_random;
using kwaque::simulation::deterministic_random_test_access;
using kwaque::simulation::random_coordinate;
using kwaque::simulation::random_domain;

static_assert(kwaque::simulation::deterministic_random_algorithm_version == 1);
static_assert(kwaque::simulation::deterministic_random_coordinate_version == 1);
static_assert(
  kwaque::simulation::deterministic_random_derivation_tag
  == UINT32_C(0x314b514b));
static_assert(!kwaque::simulation::detail::random_domains_are_unique(
  std::array{random_domain::runtime_stream, random_domain::runtime_stream}));

random_coordinate coordinate(
  random_domain domain, std::uint64_t stable_id, std::uint64_t occurrence) {
    auto result = random_coordinate::make(domain, stable_id, occurrence);
    EXPECT_TRUE(result.has_value());
    return *result;
}

} // namespace

TEST(DeterministicRandomTest, RejectsUnregisteredDomains) {
    const auto invalid = random_coordinate::make(
      static_cast<random_domain>(UINT32_C(0x10203040)), 0, 0);
    ASSERT_FALSE(invalid.has_value());
    EXPECT_EQ(invalid.error().code(), kwaque::errc::invalid_argument);
}

TEST(DeterministicRandomTest, MatchesCanonicalCoordinateAndLaneVectors) {
    const deterministic_random zero{0};
    const auto zero_coordinate = coordinate(
      random_domain::runtime_stream, 0, 0);
    EXPECT_EQ(zero.word_at(zero_coordinate, 0), UINT64_C(0xf45349b04aeb3720));
    EXPECT_EQ(zero.word_at(zero_coordinate, 1), UINT64_C(0x0a37d583d317c57a));
    EXPECT_EQ(zero.word_at(zero_coordinate, 2), UINT64_C(0x64134d08e6594940));

    const deterministic_random seeded{UINT64_C(0x0123456789abcdef)};
    const auto stream_coordinate = coordinate(
      random_domain::runtime_stream,
      UINT64_C(0xfedcba9876543210),
      UINT64_C(0x8877665544332211));
    EXPECT_EQ(
      seeded.word_at(stream_coordinate, 0), UINT64_C(0x4407e9ebc66dd045));
    EXPECT_EQ(
      seeded.word_at(stream_coordinate, 1), UINT64_C(0x088a0cfe5afe6f12));

    const auto fault_coordinate = coordinate(
      random_domain::fault_decision,
      UINT64_C(0xfedcba9876543210),
      UINT64_C(0x8877665544332211));
    EXPECT_EQ(
      seeded.word_at(fault_coordinate, 0), UINT64_C(0x758943ca89fd6c49));

    const deterministic_random maximum_seed{UINT64_MAX};
    const auto maximum_coordinate = coordinate(
      random_domain::dns_decision, UINT64_MAX, UINT64_MAX);
    EXPECT_EQ(
      maximum_seed.word_at(maximum_coordinate, UINT64_MAX),
      UINT64_C(0xafd853882ddb4f3e));
}

TEST(DeterministicRandomTest, NamedStreamsAreIndependentAndResettable) {
    const deterministic_random random{UINT64_C(0x0123456789abcdef)};
    const auto decision_coordinate = coordinate(
      random_domain::fault_decision, 99, 3);
    const auto decision_before = random.word_at(decision_coordinate, 0);
    auto stream_a = random.stream(random_domain::runtime_stream, 11, 7);
    auto stream_b = random.stream(random_domain::runtime_stream, 12, 7);
    auto fresh_a = random.stream(random_domain::runtime_stream, 11, 7);
    ASSERT_TRUE(stream_a && stream_b && fresh_a);

    const auto a_first = stream_a->next_u64();
    static_cast<void>(stream_b->next_u64());
    static_cast<void>(stream_b->next_u64());
    const auto a_second = stream_a->next_u64();
    EXPECT_EQ(random.word_at(decision_coordinate, 0), decision_before);
    EXPECT_EQ(a_first, fresh_a->next_u64());
    EXPECT_EQ(a_second, fresh_a->next_u64());

    stream_a->reset(9);
    auto reset_reference = random.stream(random_domain::runtime_stream, 11, 9);
    ASSERT_TRUE(reset_reference.has_value());
    EXPECT_EQ(stream_a->draw_index(), 0U);
    EXPECT_EQ(stream_a->occurrence(), 9U);
    EXPECT_EQ(stream_a->next_u64(), reset_reference->next_u64());
}

TEST(DeterministicRandomTest, SequentialExhaustionIsDetectedBeforeReuse) {
    const deterministic_random random{UINT64_MAX};
    const auto source_coordinate = coordinate(
      random_domain::dns_decision, UINT64_MAX, UINT64_MAX);
    auto source = random.stream(source_coordinate);
    deterministic_random_test_access::set_next_draw(source, UINT64_MAX);

    EXPECT_EQ(source.next_u64(), random.word_at(source_coordinate, UINT64_MAX));
    EXPECT_TRUE(source.exhausted());
    EXPECT_DEATH(
      { static_cast<void>(source.next_u64()); }, "id=KQ-RANDOM-EXHAUSTED");
    source.reset(UINT64_MAX);
    EXPECT_FALSE(source.exhausted());
    EXPECT_EQ(source.draw_index(), 0U);
}

TEST(
  DeterministicRandomDeathTest,
  RejectionCursorExhaustionFailsBeforeCoordinateReuse) {
    const deterministic_random random{0};
    const auto source_coordinate = coordinate(
      random_domain::runtime_stream, 0, 0);
    auto cursor = random.cursor(source_coordinate, UINT64_MAX);
    EXPECT_DEATH(
      {
          static_cast<void>(
            kwaque::runtime::uniform_u64(cursor, UINT64_C(0x8000000000000001)));
      },
      "id=KQ-RANDOM-EXHAUSTED");
}

TEST(DeterministicRandomTest, KeyedCursorReusesIntegerOnlyRuntimeAlgorithms) {
    const deterministic_random random{0};
    const auto source_coordinate = coordinate(
      random_domain::runtime_stream, 0, 0);
    auto cursor = random.cursor(source_coordinate, 0);

    const auto bounded = kwaque::runtime::uniform_u64(
      cursor, UINT64_C(0x800000000000000b));
    ASSERT_TRUE(bounded.has_value());
    EXPECT_EQ(*bounded, UINT64_C(0x66545b75e382738b));
    EXPECT_EQ(cursor.draw_index(), 4U);

    const auto probability = kwaque::runtime::probability_ratio::make(1, 3);
    ASSERT_TRUE(probability.has_value());
    static_cast<void>(kwaque::runtime::chance(cursor, *probability));
    EXPECT_GE(cursor.draw_index(), 5U);

    std::array<std::byte, 9> bytes{};
    const auto before_fill = cursor.draw_index();
    kwaque::runtime::fill_bytes(cursor, bytes);
    EXPECT_EQ(cursor.draw_index(), before_fill + 2U);
}

TEST(DeterministicRandomTest, EverySmallExclusiveBoundRemainsInRange) {
    const deterministic_random random{UINT64_C(0x0123456789abcdef)};
    for (std::uint64_t upper = 2; upper <= 257; ++upper) {
        auto source = random.stream(random_domain::runtime_stream, upper, 0);
        ASSERT_TRUE(source.has_value());
        bool in_range = true;
        for (std::size_t draw = 0; draw < 1'024; ++draw) {
            const auto selected = kwaque::runtime::uniform_u64(*source, upper);
            if (!selected || *selected >= upper) {
                in_range = false;
                break;
            }
        }
        EXPECT_TRUE(in_range) << "upper=" << upper;
    }
}

TEST(DeterministicRandomTest, LargeSequentialFillCreatesNoTraceEntries) {
    const auto limits = kwaque::simulation::trace_limits::make(
      kwaque::simulation::trace_limit_values{
        .entries = 8,
        .encoded_bytes = 4'096,
        .line_bytes = 1'024,
      });
    ASSERT_TRUE(limits.has_value());
    kwaque::simulation::event_trace trace{
      kwaque::simulation::trace_header::current(
        0,
        kwaque::simulation::deterministic_random_algorithm_version,
        kwaque::simulation::deterministic_random_coordinate_version,
        kwaque::simulation::trace_scheduler_budget{
          .pending_events = 1,
          .events_per_pump = 1,
          .total_events = 1,
          .maximum_deadline = 1,
        },
        *limits,
        kwaque::simulation::trace_digest{},
        kwaque::simulation::trace_digest{}),
      *limits};
    const deterministic_random random{0};
    auto source = random.stream(random_domain::runtime_stream, 1, 0);
    ASSERT_TRUE(source.has_value());
    std::vector<std::byte> output(64 * 1'024);
    kwaque::runtime::fill_bytes(*source, output);
    EXPECT_TRUE(trace.entries().empty());
}

TEST(DeterministicRandomTest, RecordedDecisionsRequireTheTraceMasterSeed) {
    const auto limits = kwaque::simulation::trace_limits::make(
      kwaque::simulation::trace_limit_values{
        .entries = 8,
        .encoded_bytes = 4'096,
        .line_bytes = 1'024,
      });
    ASSERT_TRUE(limits.has_value());
    kwaque::simulation::event_trace trace{
      kwaque::simulation::trace_header::current(
        7,
        kwaque::simulation::deterministic_random_algorithm_version,
        kwaque::simulation::deterministic_random_coordinate_version,
        kwaque::simulation::trace_scheduler_budget{
          .pending_events = 1,
          .events_per_pump = 1,
          .total_events = 1,
          .maximum_deadline = 1,
        },
        *limits,
        kwaque::simulation::trace_digest{},
        kwaque::simulation::trace_digest{}),
      *limits};
    const deterministic_random random{8};
    const auto source_coordinate = coordinate(
      random_domain::runtime_stream, 1, 1);
    const auto rejected = random.recorded_word_at(
      trace, kwaque::runtime::monotonic_time{}, source_coordinate, 0);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().code(), kwaque::errc::invalid_argument);
    EXPECT_TRUE(trace.entries().empty());
}
