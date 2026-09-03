#include "src/base/allocation.h"
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
  std::uint32_t outcome = 1,
  kwaque::runtime::builtin_fault_point point
  = kwaque::runtime::builtin_fault_point::file_read,
  kwaque::runtime::fault_action decision
  = kwaque::runtime::fault_action::error) {
    return trace_entry{
      .time = kwaque::runtime::monotonic_time{2},
      .action = trace_action::fault_evaluated,
      .kind = trace_event_kind::fault,
      .domain = kwaque::runtime::descriptor_for(point)->id.value(),
      .stable_id = 7,
      .coordinate_a = 3,
      .coordinate_b = draws,
      .value = value,
      .result = static_cast<std::uint8_t>(decision) | (outcome << 8U),
    };
}

constexpr std::string_view golden_trace
  = "KQTR 05 0000000000000000 00000001 00000001 00000001 00000004 "
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
static_assert(kwaque::simulation::event_trace_schema_version == 5);
static_assert(kwaque::runtime::builtin_fault_points.size() == 27);
static_assert(
  kwaque::runtime::fault_action::partial_resize
  == static_cast<kwaque::runtime::fault_action>(13));
static_assert(trace_action::stop_terminal == static_cast<trace_action>(22));
static_assert(trace_event_kind::dns == static_cast<trace_event_kind>(10));
static_assert(
  trace_context_key::digest_word_3 == static_cast<trace_context_key>(8));
static_assert(
  kwaque::simulation::network_trace_phase::stop
  == static_cast<kwaque::simulation::network_trace_phase>(13));
static_assert(
  kwaque::simulation::bandwidth_trace_phase::transfer_done
  == static_cast<kwaque::simulation::bandwidth_trace_phase>(3));
static_assert(
  kwaque::simulation::network_control_trace_phase::ingress_limit
  == static_cast<kwaque::simulation::network_control_trace_phase>(7));
static_assert(
  kwaque::simulation::dns_trace_phase::stop
  == static_cast<kwaque::simulation::dns_trace_phase>(5));
static_assert(kwaque::simulation::trace_event_domain_is_valid(
  kwaque::simulation::trace_event_kind::network,
  static_cast<std::uint32_t>(kwaque::simulation::network_trace_phase::stop)));
static_assert(kwaque::simulation::trace_event_domain_is_valid(
  kwaque::simulation::trace_event_kind::bandwidth,
  static_cast<std::uint32_t>(
    kwaque::simulation::bandwidth_trace_phase::transfer_done)));
static_assert(kwaque::simulation::trace_event_domain_is_valid(
  kwaque::simulation::trace_event_kind::network_control,
  static_cast<std::uint32_t>(
    kwaque::simulation::network_control_trace_phase::ingress_limit)));
static_assert(kwaque::simulation::trace_event_domain_is_valid(
  kwaque::simulation::trace_event_kind::dns,
  static_cast<std::uint32_t>(kwaque::simulation::dns_trace_phase::stop)));
static_assert(!kwaque::simulation::trace_event_domain_is_valid(
  kwaque::simulation::trace_event_kind::network, 0));
static_assert(!kwaque::simulation::trace_event_domain_is_valid(
  kwaque::simulation::trace_event_kind::network,
  static_cast<std::uint32_t>(kwaque::simulation::network_trace_phase::stop)
    + 1U));
static_assert(kwaque::simulation::trace_event_domain_is_valid(
  kwaque::simulation::trace_event_kind::generic,
  std::numeric_limits<std::uint32_t>::max()));
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

    for (const char version : {'1', '2', '3', '4'}) {
        auto old_version = *encoded_text;
        old_version[6] = version;
        EXPECT_FALSE(event_trace::decode(old_version, limits).has_value());
    }

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

TEST(EventTraceTest, ArtifactUnderestimatedHintStaysChunkBounded) {
    kwaque::simulation::trace_artifact artifact{64};
    const std::string prefix(32, 'a');
    artifact.append(prefix);
    const std::string remaining(
      kwaque::maximum_contiguous_allocation_bytes, 'b');
    artifact.append(remaining);
    EXPECT_EQ(
      artifact.size(),
      kwaque::maximum_contiguous_allocation_bytes + prefix.size());
    ASSERT_GT(artifact.chunks().size(), 1U);
    for (const auto& chunk : artifact.chunks()) {
        EXPECT_LE(
          chunk.capacity(), kwaque::maximum_contiguous_allocation_bytes);
        EXPECT_LE(chunk.size(), chunk.capacity());
    }
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
    replace_token(unknown_schema, 0, 1, "06");
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

    for (const auto [point, action] : {
           std::pair{
             kwaque::runtime::builtin_fault_point::environment_start,
             kwaque::runtime::fault_action::error},
           std::pair{
             kwaque::runtime::builtin_fault_point::environment_start,
             kwaque::runtime::fault_action::delay},
           std::pair{
             kwaque::runtime::builtin_fault_point::resource_group_create,
             kwaque::runtime::fault_action::error},
           std::pair{
             kwaque::runtime::builtin_fault_point::queue_admission,
             kwaque::runtime::fault_action::error},
           std::pair{
             kwaque::runtime::builtin_fault_point::queue_admission,
             kwaque::runtime::fault_action::delay},
           std::pair{
             kwaque::runtime::builtin_fault_point::environment_stop,
             kwaque::runtime::fault_action::error},
           std::pair{
             kwaque::runtime::builtin_fault_point::environment_stop,
             kwaque::runtime::fault_action::delay},
         }) {
        EXPECT_TRUE(
          trace
            .observe(fault_entry(
              std::numeric_limits<std::uint64_t>::max(), 0, 1, point, action))
            .has_value());
    }

    auto unknown_point = fault_entry();
    unknown_point.domain = 28;
    EXPECT_EQ(
      trace.observe(unknown_point).error().code(),
      kwaque::errc::malformed_data);
    auto unsupported_resource_delay = fault_entry(
      std::numeric_limits<std::uint64_t>::max(),
      0,
      1,
      kwaque::runtime::builtin_fault_point::resource_group_create,
      kwaque::runtime::fault_action::delay);
    EXPECT_EQ(
      trace.observe(unsupported_resource_delay).error().code(),
      kwaque::errc::malformed_data);
    auto unsupported_environment_crash = fault_entry(
      std::numeric_limits<std::uint64_t>::max(),
      0,
      1,
      kwaque::runtime::builtin_fault_point::environment_start,
      kwaque::runtime::fault_action::crash);
    EXPECT_EQ(
      trace.observe(unsupported_environment_crash).error().code(),
      kwaque::errc::malformed_data);
    auto fault_context = fault_entry(
      std::numeric_limits<std::uint64_t>::max(),
      0,
      1,
      kwaque::runtime::builtin_fault_point::queue_admission);
    fault_context.context_size = 1;
    fault_context.context[0] = {
      .key = trace_context_key::detail,
      .value = 1,
    };
    EXPECT_EQ(
      trace.observe(fault_context).error().code(),
      kwaque::errc::malformed_data);
    auto fault_deadline = fault_entry(
      std::numeric_limits<std::uint64_t>::max(),
      0,
      1,
      kwaque::runtime::builtin_fault_point::environment_start);
    fault_deadline.deadline = kwaque::runtime::monotonic_time{1};
    EXPECT_EQ(
      trace.observe(fault_deadline).error().code(),
      kwaque::errc::malformed_data);
    auto fault_trace_action = fault_entry(
      std::numeric_limits<std::uint64_t>::max(),
      0,
      1,
      kwaque::runtime::builtin_fault_point::environment_start);
    fault_trace_action.action = trace_action::keyed_decision;
    EXPECT_EQ(
      trace.observe(fault_trace_action).error().code(),
      kwaque::errc::malformed_data);
    auto fault_kind = fault_entry(
      std::numeric_limits<std::uint64_t>::max(),
      0,
      1,
      kwaque::runtime::builtin_fault_point::environment_start);
    fault_kind.kind = trace_event_kind::generic;
    EXPECT_EQ(
      trace.observe(fault_kind).error().code(), kwaque::errc::malformed_data);
    auto fault_event_id = fault_entry(
      std::numeric_limits<std::uint64_t>::max(),
      0,
      1,
      kwaque::runtime::builtin_fault_point::environment_start);
    fault_event_id.event_id = 1;
    EXPECT_EQ(
      trace.observe(fault_event_id).error().code(),
      kwaque::errc::malformed_data);
    auto fault_priority = fault_entry(
      std::numeric_limits<std::uint64_t>::max(),
      0,
      1,
      kwaque::runtime::builtin_fault_point::environment_start);
    fault_priority.priority = 1;
    EXPECT_EQ(
      trace.observe(fault_priority).error().code(),
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

TEST(EventTraceTest, ReplayRejectsEveryValidLifecycleFaultMutation) {
    const auto limits = test_limits();
    const auto original = fault_entry(
      std::numeric_limits<std::uint64_t>::max(),
      0,
      1,
      kwaque::runtime::builtin_fault_point::environment_start,
      kwaque::runtime::fault_action::error);
    event_trace captured{test_header(limits), limits};
    ASSERT_TRUE(captured.observe(original).has_value());
    const auto encoded = captured.encode();
    ASSERT_TRUE(encoded.has_value());

    enum class mutation {
        time,
        domain,
        stable_id,
        coordinate_a,
        sample,
        result,
    };
    struct mutation_case final {
        mutation selected;
        trace_difference_field expected;
    };
    constexpr std::array mutations{
      mutation_case{mutation::time, trace_difference_field::time},
      mutation_case{mutation::domain, trace_difference_field::domain},
      mutation_case{mutation::stable_id, trace_difference_field::stable_id},
      mutation_case{
        mutation::coordinate_a, trace_difference_field::coordinate_a},
      mutation_case{mutation::sample, trace_difference_field::coordinate_b},
      mutation_case{mutation::result, trace_difference_field::result},
    };

    for (const auto& test_case : mutations) {
        auto decoded = event_trace::decode(*encoded, limits);
        ASSERT_TRUE(decoded.has_value());
        auto replay = event_trace::replay(
          captured.header(), limits, std::move(*decoded));
        ASSERT_TRUE(replay.has_value());
        auto changed = original;
        switch (test_case.selected) {
        case mutation::time:
            changed.time = kwaque::runtime::monotonic_time{3};
            break;
        case mutation::domain:
            changed.domain
              = kwaque::runtime::descriptor_for(
                  kwaque::runtime::builtin_fault_point::queue_admission)
                  ->id.value();
            break;
        case mutation::stable_id:
            ++changed.stable_id;
            break;
        case mutation::coordinate_a:
            ++changed.coordinate_a;
            break;
        case mutation::sample:
            changed.coordinate_b = 1;
            changed.value = 0;
            break;
        case mutation::result:
            changed.result = static_cast<std::uint8_t>(
                               kwaque::runtime::fault_action::delay)
                             | UINT32_C(0x100);
            break;
        }

        const auto mismatch = (*replay)->observe(changed);
        ASSERT_FALSE(mismatch.has_value());
        EXPECT_EQ(mismatch.error().code(), kwaque::errc::replay_divergence);
        ASSERT_TRUE(mismatch.error().context_at(1).has_value());
        EXPECT_EQ(
          mismatch.error().context_at(1)->value,
          static_cast<std::uint8_t>(test_case.expected));
    }
}

TEST(EventTraceTest, ValidatesReservedSchemaFiveVocabulary) {
    const auto limits = test_limits(32, 32'768);
    event_trace trace{test_header(limits), limits};
    const auto event_effect = [](
                                trace_action action,
                                trace_event_kind kind,
                                std::uint32_t domain,
                                std::uint64_t stable_id = 1) {
        return trace_entry{
          .time = kwaque::runtime::monotonic_time{2},
          .deadline = kwaque::runtime::monotonic_time{2},
          .action = action,
          .kind = kind,
          .event_id = 1,
          .priority = 128,
          .domain = domain,
          .stable_id = stable_id,
        };
    };

    auto partial = event_effect(
      trace_action::partial_resize_applied,
      trace_event_kind::file,
      kwaque::runtime::descriptor_for(
        kwaque::runtime::builtin_fault_point::file_truncate)
        ->id.value());
    partial.coordinate_a = 7;
    partial.value = 10;
    partial.result = static_cast<std::uint8_t>(
                       kwaque::runtime::fault_action::partial_resize)
                     | UINT32_C(0x100);
    EXPECT_TRUE(trace.observe(partial).has_value());
    EXPECT_TRUE(trace
                  .observe(event_effect(
                    trace_action::network_operation_applied,
                    trace_event_kind::network,
                    static_cast<std::uint32_t>(
                      kwaque::simulation::network_trace_phase::bind)))
                  .has_value());
    EXPECT_TRUE(
      trace
        .observe(event_effect(
          trace_action::network_operation_applied,
          trace_event_kind::network,
          static_cast<std::uint32_t>(
            kwaque::simulation::network_trace_phase::sequence_release)))
        .has_value());

    auto flow = event_effect(
      trace_action::flow_started,
      trace_event_kind::bandwidth,
      static_cast<std::uint32_t>(
        kwaque::simulation::bandwidth_trace_phase::flow_start));
    flow.event_id = 0;
    flow.deadline = kwaque::runtime::monotonic_time{};
    flow.priority = 0;
    EXPECT_TRUE(trace.observe(flow).has_value());

    auto rebalance = trace_entry{
      .time = kwaque::runtime::monotonic_time{2},
      .action = trace_action::bandwidth_rebalanced,
      .kind = trace_event_kind::bandwidth,
      .domain = static_cast<std::uint32_t>(
        kwaque::simulation::bandwidth_trace_phase::rebalance),
      .stable_id = 2,
      .coordinate_a = 3,
      .coordinate_b = 4,
      .value = std::numeric_limits<std::uint64_t>::max(),
      .result = 5,
      .context = {
        kwaque::simulation::trace_context_field{
          .key = trace_context_key::digest_word_0, .value = 1},
        kwaque::simulation::trace_context_field{
          .key = trace_context_key::digest_word_1, .value = 2},
        kwaque::simulation::trace_context_field{
          .key = trace_context_key::digest_word_2, .value = 3},
        kwaque::simulation::trace_context_field{
          .key = trace_context_key::digest_word_3, .value = 4},
      },
      .context_size = 4,
    };
    EXPECT_TRUE(trace.observe(rebalance).has_value());
    EXPECT_TRUE(
      trace
        .observe(event_effect(
          trace_action::transfer_completed,
          trace_event_kind::bandwidth,
          static_cast<std::uint32_t>(
            kwaque::simulation::bandwidth_trace_phase::transfer_done)))
        .has_value());
    for (const auto action : {
           trace_action::packet_delivered,
           trace_action::packet_dropped,
         }) {
        EXPECT_TRUE(trace
                      .observe(event_effect(
                        action,
                        trace_event_kind::network,
                        static_cast<std::uint32_t>(
                          kwaque::simulation::network_trace_phase::delivery)))
                      .has_value());
    }
    EXPECT_TRUE(trace
                  .observe(event_effect(
                    trace_action::fin_delivered,
                    trace_event_kind::network,
                    static_cast<std::uint32_t>(
                      kwaque::simulation::network_trace_phase::fin)))
                  .has_value());
    EXPECT_TRUE(trace
                  .observe(event_effect(
                    trace_action::reset_applied,
                    trace_event_kind::network,
                    static_cast<std::uint32_t>(
                      kwaque::simulation::network_trace_phase::reset)))
                  .has_value());
    EXPECT_TRUE(
      trace
        .observe(event_effect(
          trace_action::network_control_applied,
          trace_event_kind::network_control,
          static_cast<std::uint32_t>(
            kwaque::simulation::network_control_trace_phase::partition)))
        .has_value());
    EXPECT_TRUE(trace
                  .observe(event_effect(
                    trace_action::dns_result_applied,
                    trace_event_kind::dns,
                    static_cast<std::uint32_t>(
                      kwaque::simulation::dns_trace_phase::numeric)))
                  .has_value());
    for (const auto [kind, domain] : {
           std::pair{
             trace_event_kind::network,
             static_cast<std::uint32_t>(
               kwaque::simulation::network_trace_phase::parked)},
           std::pair{
             trace_event_kind::dns,
             static_cast<std::uint32_t>(
               kwaque::simulation::dns_trace_phase::parked)},
         }) {
        EXPECT_TRUE(
          trace
            .observe(event_effect(trace_action::operation_parked, kind, domain))
            .has_value());
    }
    for (const auto [kind, domain] : {
           std::pair{
             trace_event_kind::network,
             static_cast<std::uint32_t>(
               kwaque::simulation::network_trace_phase::stop)},
           std::pair{
             trace_event_kind::dns,
             static_cast<std::uint32_t>(
               kwaque::simulation::dns_trace_phase::stop)},
         }) {
        EXPECT_TRUE(
          trace.observe(event_effect(trace_action::stop_terminal, kind, domain))
            .has_value());
    }

    auto missing_digest = rebalance;
    missing_digest.context_size = 3;
    EXPECT_FALSE(trace.observe(missing_digest).has_value());
    auto digest_on_partial = partial;
    digest_on_partial.context_size = 1;
    digest_on_partial.context[0] = {
      .key = trace_context_key::digest_word_0,
      .value = 1,
    };
    EXPECT_FALSE(trace.observe(digest_on_partial).has_value());
    auto nonintermediate_partial = partial;
    nonintermediate_partial.coordinate_a = nonintermediate_partial.value;
    EXPECT_FALSE(trace.observe(nonintermediate_partial).has_value());
    auto wrong_delivery_domain = event_effect(
      trace_action::packet_delivered,
      trace_event_kind::network,
      static_cast<std::uint32_t>(kwaque::simulation::network_trace_phase::fin));
    EXPECT_FALSE(trace.observe(wrong_delivery_domain).has_value());
    auto unknown_phase = event_effect(
      trace_action::scheduled, trace_event_kind::network, 99);
    EXPECT_FALSE(trace.observe(unknown_phase).has_value());
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
