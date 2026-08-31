#include "src/runtime/fault.h"
#include "src/simulation/event_trace.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <type_traits>
#include <utility>

namespace {

using kwaque::simulation::canonical_entry_encoded_size;
using kwaque::simulation::canonical_header_encoded_size;
using kwaque::simulation::decoded_event_trace;
using kwaque::simulation::event_trace;
using kwaque::simulation::trace_action;
using kwaque::simulation::trace_context_key;
using kwaque::simulation::trace_difference_field;
using kwaque::simulation::trace_digest;
using kwaque::simulation::trace_entry;
using kwaque::simulation::trace_event_kind;
using kwaque::simulation::trace_header;
using kwaque::simulation::trace_limit_values;
using kwaque::simulation::trace_limits;
using kwaque::simulation::trace_scheduler_budget;

trace_limits test_limits(
  std::uint32_t entries = 16,
  std::uint64_t bytes = 16'384,
  std::uint32_t line_bytes = 1'024) {
    auto limits = trace_limits::make(
      trace_limit_values{
        .entries = entries,
        .encoded_bytes = bytes,
        .line_bytes = line_bytes,
      });
    EXPECT_TRUE(limits.has_value());
    return *limits;
}

trace_header test_header(
  trace_limits limits,
  std::uint64_t seed = 0,
  trace_digest configuration = {},
  trace_digest input = {}) {
    return trace_header::current(
      seed,
      kwaque::simulation::deterministic_random_algorithm_version,
      kwaque::simulation::deterministic_random_coordinate_version,
      trace_scheduler_budget{
        .pending_events = 4,
        .events_per_pump = 4,
        .total_events = 8,
        .maximum_deadline = 100,
      },
      limits,
      configuration,
      input);
}

trace_entry keyed_entry(std::uint64_t value = 5) {
    return trace_entry{
      .time = kwaque::runtime::monotonic_time{2},
      .action = trace_action::keyed_decision,
      .kind = trace_event_kind::keyed_random,
      .domain = 1,
      .stable_id = 2,
      .coordinate_a = 3,
      .coordinate_b = 4,
      .value = value,
    };
}

trace_entry fault_entry(
  std::uint64_t value = std::numeric_limits<std::uint64_t>::max(),
  std::uint64_t draws = 0,
  std::uint32_t outcome = 1) {
    return trace_entry{
      .time = kwaque::runtime::monotonic_time{2},
      .action = trace_action::fault_evaluated,
      .kind = trace_event_kind::fault,
      .domain = kwaque::runtime::descriptor_for(
                  kwaque::runtime::builtin_fault_point::file_read)
                  ->id.value(),
      .stable_id = 7,
      .coordinate_a = 3,
      .coordinate_b = draws,
      .value = value,
      .result = static_cast<std::uint8_t>(kwaque::runtime::fault_action::error)
                | (outcome << 8U),
    };
}

constexpr std::string_view golden_trace
  = "KQTR 03 0000000000000000 00000001 00000001 00000001 00000004 "
    "0000000000000004 0000000000000008 0000000000000064 00000004 "
    "0000000000001000 00000400 0000000000000001 "
    "0000000000000000000000000000000000000000000000000000000000000000 "
    "0000000000000000000000000000000000000000000000000000000000000000\n"
    "E 0000000000000001 0000000000000002 0000000000000000 06 03 "
    "0000000000000000 00 00000001 0000000000000002 0000000000000003 "
    "0000000000000004 0000000000000005 00000000 00 000000000000000000 "
    "000000000000000000 000000000000000000 000000000000000000\n";

static_assert(canonical_header_encoded_size == 294);
static_assert(canonical_entry_encoded_size == 244);
static_assert(golden_trace.size() == 538);
static_assert(!std::is_move_constructible_v<event_trace>);
static_assert(noexcept(
  std::declval<event_trace&>().reserve(1, canonical_entry_encoded_size)));
static_assert(noexcept(std::declval<event_trace&>().observe(trace_entry{})));

void replace_token(
  std::string& artifact,
  std::size_t line_start,
  std::size_t token_index,
  std::string_view replacement) {
    auto begin = line_start;
    for (std::size_t index = 0; index < token_index; ++index) {
        begin = artifact.find(' ', begin);
        ASSERT_NE(begin, std::string::npos);
        ++begin;
    }
    const auto end = artifact.find_first_of(" \n", begin);
    ASSERT_NE(end, std::string::npos);
    ASSERT_EQ(end - begin, replacement.size());
    artifact.replace(begin, replacement.size(), replacement);
}

} // namespace

TEST(EventTraceTest, ValidatesEveryConfiguredBound) {
    auto zero = trace_limits::make(trace_limit_values{});
    ASSERT_TRUE(zero.has_value());

    auto invalid = trace_limit_values{};
    invalid.entries = 0;
    EXPECT_EQ(
      trace_limits::make(invalid).error().code(),
      kwaque::errc::invalid_argument);

    auto short_line = trace_limit_values{};
    short_line.line_bytes = canonical_entry_encoded_size - 1;
    EXPECT_EQ(
      trace_limits::make(short_line).error().code(),
      kwaque::errc::out_of_range);

    auto oversized = trace_limit_values{};
    oversized.entries = trace_limits::entries_absolute + 1;
    EXPECT_EQ(
      trace_limits::make(oversized).error().code(), kwaque::errc::out_of_range);

    const auto absolute = trace_limits::make(
      trace_limit_values{
        .entries = trace_limits::entries_absolute,
        .encoded_bytes = trace_limits::encoded_bytes_absolute,
        .line_bytes = trace_limits::line_bytes_absolute,
      });
    EXPECT_TRUE(absolute.has_value());
}

TEST(EventTraceTest, EncodingAndParsingMatchTheCanonicalGoldenFixture) {
    const auto limits = test_limits(4, 4'096);
    event_trace trace{test_header(limits), limits};
    ASSERT_TRUE(trace.observe(keyed_entry()).has_value());

    const auto encoded = trace.encode();
    ASSERT_TRUE(encoded.has_value());
    const auto encoded_text = encoded->to_string();
    ASSERT_TRUE(encoded_text.has_value());
    EXPECT_EQ(*encoded_text, golden_trace);
    EXPECT_EQ(encoded->size(), trace.encoded_bytes());
    EXPECT_EQ(encoded_text->find("/home/"), std::string::npos);
    EXPECT_EQ(encoded_text->find('\n'), canonical_header_encoded_size - 1U);

    auto version_one = *encoded_text;
    version_one[6] = '1';
    EXPECT_FALSE(event_trace::decode(version_one, limits).has_value());

    const auto decoded = event_trace::decode(*encoded, limits);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->header, trace.header());
    EXPECT_TRUE(std::ranges::equal(decoded->entries, trace.entries()));
    EXPECT_EQ(decoded->encoded_bytes, encoded->size());
}

TEST(EventTraceTest, LargeArtifactsRequireTheCooperativeCodec) {
    constexpr std::uint32_t entry_count{1'025};
    constexpr std::uint64_t encoded_bytes = canonical_header_encoded_size
                                            + static_cast<std::uint64_t>(
                                                entry_count)
                                                * canonical_entry_encoded_size;
    const auto limits = test_limits(entry_count, encoded_bytes);
    event_trace captured{test_header(limits), limits};
    for (std::uint64_t value = 0; value < entry_count; ++value) {
        ASSERT_TRUE(captured.observe(keyed_entry(value)).has_value());
    }

    const auto encoded = captured.encode();
    ASSERT_FALSE(encoded.has_value());
    EXPECT_EQ(encoded.error().code(), kwaque::errc::resource_exhausted);
}

TEST(EventTraceTest, ParserRejectsEveryNoncanonicalBoundary) {
    const auto limits = test_limits(4, 4'096);
    event_trace trace{test_header(limits), limits};
    ASSERT_TRUE(trace.observe(keyed_entry()).has_value());
    const auto encoded_result = trace.encode();
    ASSERT_TRUE(encoded_result.has_value());
    const auto encoded_text = encoded_result->to_string();
    ASSERT_TRUE(encoded_text.has_value());

    auto uppercase = *encoded_text;
    uppercase[5] = 'A';
    EXPECT_FALSE(event_trace::decode(uppercase, limits).has_value());

    auto crlf = *encoded_text;
    crlf.insert(crlf.find('\n'), 1, '\r');
    EXPECT_FALSE(event_trace::decode(crlf, limits).has_value());

    auto truncated = *encoded_text;
    truncated.pop_back();
    EXPECT_FALSE(event_trace::decode(truncated, limits).has_value());

    auto trailing = *encoded_text;
    trailing.append("x\n");
    EXPECT_FALSE(event_trace::decode(trailing, limits).has_value());

    auto invalid_action = *encoded_text;
    replace_token(invalid_action, canonical_header_encoded_size, 4, "ff");
    EXPECT_FALSE(event_trace::decode(invalid_action, limits).has_value());

    auto unknown_schema = *encoded_text;
    replace_token(unknown_schema, 0, 1, "04");
    EXPECT_FALSE(event_trace::decode(unknown_schema, limits).has_value());

    auto unknown_random = *encoded_text;
    replace_token(unknown_random, 0, 3, "00000002");
    EXPECT_FALSE(event_trace::decode(unknown_random, limits).has_value());
    auto unknown_coordinate = *encoded_text;
    replace_token(unknown_coordinate, 0, 4, "00000002");
    EXPECT_FALSE(event_trace::decode(unknown_coordinate, limits).has_value());

    auto invalid_kind = *encoded_text;
    replace_token(invalid_kind, canonical_header_encoded_size, 5, "ff");
    EXPECT_FALSE(event_trace::decode(invalid_kind, limits).has_value());

    auto duplicate_context = *encoded_text;
    replace_token(duplicate_context, canonical_header_encoded_size, 14, "02");
    replace_token(
      duplicate_context,
      canonical_header_encoded_size,
      15,
      "040000000000000001");
    replace_token(
      duplicate_context,
      canonical_header_encoded_size,
      16,
      "040000000000000002");
    EXPECT_FALSE(event_trace::decode(duplicate_context, limits).has_value());

    auto oversized_count = *encoded_text;
    replace_token(oversized_count, 0, 13, "0000000000400001");
    EXPECT_FALSE(event_trace::decode(oversized_count, limits).has_value());

    auto overlong_line = *encoded_text;
    overlong_line.insert(overlong_line.size() - 1, 1, '0');
    EXPECT_FALSE(event_trace::decode(overlong_line, limits).has_value());

    const auto tiny_parser = test_limits(4, golden_trace.size() - 1);
    EXPECT_FALSE(event_trace::decode(*encoded_result, tiny_parser).has_value());
}

TEST(EventTraceTest, ReservationsProtectTerminalCapacity) {
    const auto bytes = canonical_header_encoded_size
                       + 2 * canonical_entry_encoded_size;
    const auto limits = test_limits(2, bytes);
    event_trace trace{test_header(limits), limits};
    auto reservation = trace.reserve(2, 2 * canonical_entry_encoded_size);
    ASSERT_TRUE(reservation.has_value());

    EXPECT_EQ(
      trace.observe(keyed_entry()).error().code(),
      kwaque::errc::resource_exhausted);
    ASSERT_TRUE(trace.observe(keyed_entry(), &*reservation).has_value());
    ASSERT_TRUE(trace.observe(keyed_entry(6), &*reservation).has_value());
    EXPECT_FALSE(reservation->active());
    EXPECT_EQ(trace.entries().size(), 2U);
}

TEST(EventTraceTest, RejectsDuplicateAndOutOfRangeEntryFields) {
    const auto limits = test_limits();
    event_trace trace{test_header(limits), limits};
    auto duplicate = keyed_entry();
    duplicate.context_size = 2;
    duplicate.context[0] = {
      .key = trace_context_key::detail,
      .value = 1,
    };
    duplicate.context[1] = {
      .key = trace_context_key::detail,
      .value = 2,
    };
    EXPECT_EQ(
      trace.observe(duplicate).error().code(), kwaque::errc::malformed_data);

    auto invalid_action = keyed_entry();
    invalid_action.action = static_cast<trace_action>(255);
    EXPECT_EQ(
      trace.observe(invalid_action).error().code(),
      kwaque::errc::malformed_data);

    auto keyed_with_deadline = keyed_entry();
    keyed_with_deadline.deadline = kwaque::runtime::monotonic_time{2};
    EXPECT_EQ(
      trace.observe(keyed_with_deadline).error().code(),
      kwaque::errc::malformed_data);
    EXPECT_EQ(
      trace
        .observe(
          trace_entry{
            .time = kwaque::runtime::monotonic_time{2},
            .deadline = kwaque::runtime::monotonic_time{3},
            .action = trace_action::selected,
            .kind = trace_event_kind::generic,
            .event_id = 1,
          })
        .error()
        .code(),
      kwaque::errc::malformed_data);
    EXPECT_EQ(
      trace
        .observe(
          trace_entry{
            .action = trace_action::time_advanced,
            .kind = trace_event_kind::generic,
            .coordinate_b = 1,
            .value = 1,
          })
        .error()
        .code(),
      kwaque::errc::malformed_data);
}

TEST(EventTraceTest, ValidatesFaultAndFileLifecycleVocabulary) {
    const auto limits = test_limits();
    event_trace trace{test_header(limits), limits};
    EXPECT_TRUE(trace.observe(fault_entry()).has_value());
    EXPECT_TRUE(trace.observe(fault_entry(2, 1, 2)).has_value());

    auto inconsistent_sample = fault_entry(2, 0);
    EXPECT_EQ(
      trace.observe(inconsistent_sample).error().code(),
      kwaque::errc::malformed_data);
    auto unknown_outcome = fault_entry();
    unknown_outcome.result = static_cast<std::uint8_t>(
      kwaque::runtime::fault_action::error);
    EXPECT_EQ(
      trace.observe(unknown_outcome).error().code(),
      kwaque::errc::malformed_data);
    auto illegal_action = fault_entry();
    illegal_action.domain = kwaque::runtime::descriptor_for(
                              kwaque::runtime::builtin_fault_point::timer)
                              ->id.value();
    illegal_action.result = static_cast<std::uint8_t>(
                              kwaque::runtime::fault_action::corrupt)
                            | UINT32_C(0x100);
    EXPECT_EQ(
      trace.observe(illegal_action).error().code(),
      kwaque::errc::malformed_data);

    EXPECT_TRUE(trace
                  .observe(
                    trace_entry{
                      .time = kwaque::runtime::monotonic_time{2},
                      .deadline = kwaque::runtime::monotonic_time{2},
                      .action = trace_action::operation_discarded,
                      .kind = trace_event_kind::file,
                      .event_id = 1,
                    })
                  .has_value());
    EXPECT_TRUE(trace
                  .observe(
                    trace_entry{
                      .time = kwaque::runtime::monotonic_time{2},
                      .deadline = kwaque::runtime::monotonic_time{2},
                      .action = trace_action::crash_applied,
                      .kind = trace_event_kind::filesystem,
                      .event_id = 2,
                    })
                  .has_value());
    EXPECT_EQ(
      trace
        .observe(
          trace_entry{
            .time = kwaque::runtime::monotonic_time{2},
            .deadline = kwaque::runtime::monotonic_time{2},
            .action = trace_action::operation_discarded,
            .kind = trace_event_kind::generic,
            .event_id = 3,
          })
        .error()
        .code(),
      kwaque::errc::malformed_data);
}

TEST(EventTraceTest, ReplayDetectsTheFirstDifferenceAndTrailingEntries) {
    const auto limits = test_limits();
    event_trace captured{test_header(limits, 7), limits};
    ASSERT_TRUE(captured.observe(keyed_entry()).has_value());
    ASSERT_TRUE(captured.observe(keyed_entry(6)).has_value());
    const auto encoded = captured.encode();
    ASSERT_TRUE(encoded.has_value());

    auto decoded = event_trace::decode(*encoded, limits);
    ASSERT_TRUE(decoded.has_value());
    auto replay = event_trace::replay(
      captured.header(), limits, std::move(*decoded));
    ASSERT_TRUE(replay.has_value());
    EXPECT_TRUE((*replay)->observe(keyed_entry()).has_value());
    EXPECT_TRUE((*replay)->observe(keyed_entry(6)).has_value());
    EXPECT_TRUE((*replay)->finish_replay().has_value());

    auto divergent_decoded = event_trace::decode(*encoded, limits);
    ASSERT_TRUE(divergent_decoded.has_value());
    auto divergent = event_trace::replay(
      captured.header(), limits, std::move(*divergent_decoded));
    ASSERT_TRUE(divergent.has_value());
    const auto mismatch = (*divergent)->observe(keyed_entry(99));
    ASSERT_FALSE(mismatch.has_value());
    EXPECT_EQ(mismatch.error().code(), kwaque::errc::replay_divergence);
    EXPECT_EQ(
      mismatch.error().context_at(0)->key,
      kwaque::runtime::operation_context_key::sequence);
    EXPECT_EQ(mismatch.error().context_at(0)->value, 1U);
    EXPECT_EQ(
      mismatch.error().context_at(1)->key,
      kwaque::runtime::operation_context_key::detail);
    EXPECT_EQ(
      mismatch.error().context_at(1)->value,
      static_cast<std::uint8_t>(trace_difference_field::value));
    EXPECT_EQ(
      mismatch.error().context_at(2)->key,
      kwaque::runtime::operation_context_key::expected);
    EXPECT_EQ(mismatch.error().context_at(2)->value, 5U);
    EXPECT_EQ(
      mismatch.error().context_at(3)->key,
      kwaque::runtime::operation_context_key::actual);
    EXPECT_EQ(mismatch.error().context_at(3)->value, 99U);

    auto trailing_decoded = event_trace::decode(*encoded, limits);
    ASSERT_TRUE(trailing_decoded.has_value());
    auto trailing = event_trace::replay(
      captured.header(), limits, std::move(*trailing_decoded));
    ASSERT_TRUE(trailing.has_value());
    EXPECT_TRUE((*trailing)->observe(keyed_entry()).has_value());
    const auto trailing_error = (*trailing)->finish_replay();
    ASSERT_FALSE(trailing_error.has_value());
    EXPECT_EQ(trailing_error.error().code(), kwaque::errc::replay_divergence);
    EXPECT_EQ(
      trailing_error.error().context_at(1)->value,
      static_cast<std::uint8_t>(trace_difference_field::actual_missing));
    EXPECT_EQ(
      trailing_error.error().context_at(2)->value,
      static_cast<std::uint8_t>(trace_action::keyed_decision));
    EXPECT_EQ(trailing_error.error().context_at(3)->value, 0U);

    auto deleted_decoded = event_trace::decode(*encoded, limits);
    ASSERT_TRUE(deleted_decoded.has_value());
    deleted_decoded->entries.pop_back();
    deleted_decoded->encoded_bytes -= canonical_entry_encoded_size;
    auto deleted = event_trace::replay(
      captured.header(), limits, std::move(*deleted_decoded));
    ASSERT_TRUE(deleted.has_value());
    ASSERT_TRUE((*deleted)->observe(keyed_entry()).has_value());
    const auto missing_expected = (*deleted)->observe(keyed_entry(6));
    ASSERT_FALSE(missing_expected.has_value());
    EXPECT_EQ(missing_expected.error().code(), kwaque::errc::replay_divergence);
    EXPECT_EQ(
      missing_expected.error().context_at(1)->value,
      static_cast<std::uint8_t>(trace_difference_field::expected_missing));
    EXPECT_EQ(missing_expected.error().context_at(2)->value, 0U);
    EXPECT_EQ(
      missing_expected.error().context_at(3)->value,
      static_cast<std::uint8_t>(trace_action::keyed_decision));

    auto inserted_decoded = event_trace::decode(*encoded, limits);
    ASSERT_TRUE(inserted_decoded.has_value());
    auto inserted_entry = keyed_entry(99);
    inserted_entry.sequence = 2;
    const auto displaced_entry = inserted_decoded->entries.back();
    inserted_decoded->entries.push_back(displaced_entry);
    inserted_decoded->entries[1] = inserted_entry;
    inserted_decoded->entries.back().sequence = 3;
    inserted_decoded->encoded_bytes += canonical_entry_encoded_size;
    auto inserted = event_trace::replay(
      captured.header(), limits, std::move(*inserted_decoded));
    ASSERT_TRUE(inserted.has_value());
    ASSERT_TRUE((*inserted)->observe(keyed_entry()).has_value());
    const auto insertion = (*inserted)->observe(keyed_entry(6));
    ASSERT_FALSE(insertion.has_value());
    EXPECT_EQ(insertion.error().context_at(0)->value, 2U);

    auto reordered_decoded = event_trace::decode(*encoded, limits);
    ASSERT_TRUE(reordered_decoded.has_value());
    std::swap(reordered_decoded->entries[0], reordered_decoded->entries[1]);
    reordered_decoded->entries[0].sequence = 1;
    reordered_decoded->entries[1].sequence = 2;
    auto reordered = event_trace::replay(
      captured.header(), limits, std::move(*reordered_decoded));
    ASSERT_TRUE(reordered.has_value());
    const auto reordered_outcome = (*reordered)->observe(keyed_entry());
    ASSERT_FALSE(reordered_outcome.has_value());
    EXPECT_EQ(
      reordered_outcome.error().code(), kwaque::errc::replay_divergence);

    auto mismatched_header = captured.header();
    ++mismatched_header.master_seed;
    auto header_decoded = event_trace::decode(*encoded, limits);
    ASSERT_TRUE(header_decoded.has_value());
    auto header_mismatch = event_trace::replay(
      mismatched_header, limits, std::move(*header_decoded));
    ASSERT_FALSE(header_mismatch.has_value());
    EXPECT_EQ(header_mismatch.error().code(), kwaque::errc::replay_divergence);
}

TEST(EventTraceTest, ReplayRejectsEveryHeaderMismatchBeforeObservation) {
    const auto limits = test_limits();
    event_trace captured{test_header(limits, 7), limits};
    ASSERT_TRUE(captured.observe(keyed_entry()).has_value());
    const auto encoded = captured.encode();
    ASSERT_TRUE(encoded.has_value());

    auto expect_rejected = [&](trace_header actual) {
        auto decoded = event_trace::decode(*encoded, limits);
        ASSERT_TRUE(decoded.has_value());
        const auto replay = event_trace::replay(
          std::move(actual), limits, std::move(*decoded));
        EXPECT_FALSE(replay.has_value());
    };

    auto changed = captured.header();
    ++changed.schema_version;
    expect_rejected(changed);
    changed = captured.header();
    ++changed.master_seed;
    expect_rejected(changed);
    changed = captured.header();
    ++changed.random_algorithm_version;
    expect_rejected(changed);
    changed = captured.header();
    ++changed.coordinate_schema_version;
    expect_rejected(changed);
    changed = captured.header();
    ++changed.ordering_version;
    expect_rejected(changed);
    changed = captured.header();
    ++changed.scheduler_budget.pending_events;
    expect_rejected(changed);
    changed = captured.header();
    ++changed.trace_budget.entries;
    expect_rejected(changed);
    changed = captured.header();
    changed.configuration_digest[0] ^= 1U;
    expect_rejected(changed);
    changed = captured.header();
    changed.input_digest[0] ^= 1U;
    expect_rejected(changed);
}
