#include "src/base/error.h"
#include "src/base/result.h"
#include "src/model/segment_state.h"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace {

using kwaque::model::append_state;
using kwaque::model::completion_state;
using kwaque::model::is_valid;
using kwaque::model::remote_state;
using kwaque::model::replica_state;
using kwaque::model::retention_state;
using kwaque::model::segment_completion_facts;
using kwaque::model::validate_segment_state;

constexpr auto unknown = completion_state::unknown;
constexpr auto incomplete = completion_state::incomplete;
constexpr auto complete = completion_state::complete;
constexpr segment_completion_facts all_complete{
  complete, complete, complete, complete};

constexpr std::array fact_members{
  &segment_completion_facts::offload_commit,
  &segment_completion_facts::expiration_commit,
  &segment_completion_facts::remote_cleanup,
  &segment_completion_facts::local_cleanup};

constexpr auto fact_values(const segment_completion_facts& facts) noexcept {
    return std::array{
      facts.offload_commit,
      facts.expiration_commit,
      facts.remote_cleanup,
      facts.local_cleanup};
}

void expect_invalid(const kwaque::result<void>& result) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), kwaque::errc::invalid_argument);
}

static_assert(std::is_scoped_enum_v<completion_state>);
static_assert(sizeof(completion_state) == sizeof(std::uint8_t));
static_assert(
  std::is_same_v<std::underlying_type_t<completion_state>, std::uint8_t>);
static_assert(static_cast<std::uint8_t>(unknown) == 0U);
static_assert(static_cast<std::uint8_t>(incomplete) == 1U);
static_assert(static_cast<std::uint8_t>(complete) == 2U);
static_assert(std::is_trivially_copyable_v<segment_completion_facts>);
static_assert(std::is_standard_layout_v<segment_completion_facts>);
static_assert(sizeof(segment_completion_facts) == 4 * sizeof(completion_state));
static_assert(std::is_nothrow_copy_constructible_v<segment_completion_facts>);
static_assert(std::is_nothrow_destructible_v<segment_completion_facts>);
static_assert(segment_completion_facts{}.offload_commit == unknown);
static_assert(segment_completion_facts{}.expiration_commit == unknown);
static_assert(segment_completion_facts{}.remote_cleanup == unknown);
static_assert(segment_completion_facts{}.local_cleanup == unknown);
static_assert(is_valid(unknown));
static_assert(is_valid(incomplete));
static_assert(is_valid(complete));
static_assert(noexcept(validate_segment_state(
  append_state::sealed,
  retention_state::retained,
  remote_state::not_offloaded,
  replica_state::assigned,
  segment_completion_facts{})));

struct expected_requirements {
    retention_state retention;
    remote_state remote;
    bool requires_sealed;
    // offload commit, expiration commit, remote cleanup, local cleanup.
    std::array<bool, 4> requires_complete;
    bool markers_consistent;
    bool permits_readable;
};

// Literal requirements, independent of the validator's branching or helpers.
constexpr std::array requirements{
  expected_requirements{
    retention_state::retained,
    remote_state::not_offloaded,
    false,
    {false, false, false, false},
    true,
    true},
  expected_requirements{
    retention_state::retained,
    remote_state::offloading,
    true,
    {false, false, false, false},
    true,
    true},
  expected_requirements{
    retention_state::retained,
    remote_state::offloaded,
    true,
    {true, false, false, false},
    true,
    true},
  expected_requirements{
    retention_state::retained,
    remote_state::remote_delete_pending,
    true,
    {true, false, false, false},
    true,
    true},
  expected_requirements{
    retention_state::retained,
    remote_state::remote_deleted,
    true,
    {true, false, true, false},
    true,
    true},
  expected_requirements{
    retention_state::expiring,
    remote_state::not_offloaded,
    false,
    {false, true, false, false},
    true,
    true},
  expected_requirements{
    retention_state::expiring,
    remote_state::offloading,
    true,
    {false, true, false, false},
    true,
    true},
  expected_requirements{
    retention_state::expiring,
    remote_state::offloaded,
    true,
    {true, true, false, false},
    true,
    true},
  expected_requirements{
    retention_state::expiring,
    remote_state::remote_delete_pending,
    true,
    {true, true, false, false},
    true,
    true},
  expected_requirements{
    retention_state::expiring,
    remote_state::remote_deleted,
    true,
    {true, true, true, false},
    true,
    true},
  expected_requirements{
    retention_state::deleted,
    remote_state::not_offloaded,
    false,
    {false, true, true, true},
    true,
    false},
  expected_requirements{
    retention_state::deleted,
    remote_state::offloading,
    true,
    {false, true, true, true},
    true,
    false},
  expected_requirements{
    retention_state::deleted,
    remote_state::offloaded,
    true,
    {true, true, true, true},
    false,
    false},
  expected_requirements{
    retention_state::deleted,
    remote_state::remote_delete_pending,
    true,
    {true, true, true, true},
    true,
    false},
  expected_requirements{
    retention_state::deleted,
    remote_state::remote_deleted,
    true,
    {true, true, true, true},
    true,
    false}};

struct append_case {
    append_state state;
    bool sealed;
};
constexpr std::array appends{
  append_case{append_state::creating, false},
  append_case{append_state::active, false},
  append_case{append_state::sealing, false},
  append_case{append_state::sealed, true}};

struct replica_case {
    replica_state state;
    bool readable;
};
constexpr std::array replicas{
  replica_case{replica_state::assigned, false},
  replica_case{replica_state::copying, false},
  replica_case{replica_state::readable, true},
  replica_case{replica_state::lost, false},
  replica_case{replica_state::removing, false},
  replica_case{replica_state::removed, false}};

struct completion_case {
    completion_state state;
    bool completed;
};
constexpr std::array completions{
  completion_case{unknown, false},
  completion_case{incomplete, false},
  completion_case{complete, true}};

struct fact_case {
    segment_completion_facts facts;
    std::array<bool, 4> completed;
};

constexpr auto all_fact_cases() noexcept {
    std::array<fact_case, 81> cases{};
    std::size_t index = 0;
    for (const auto offload : completions) {
        for (const auto expiration : completions) {
            for (const auto remote : completions) {
                for (const auto local : completions) {
                    cases[index++] = {
                      {offload.state,
                       expiration.state,
                       remote.state,
                       local.state},
                      {offload.completed,
                       expiration.completed,
                       remote.completed,
                       local.completed}};
                }
            }
        }
    }
    return cases;
}

consteval bool covers_numeric_domain(
  const auto& entries, std::uint64_t first, std::uint64_t last) {
    if (entries.size() != last - first + 1U) {
        return false;
    }
    for (auto code = first; code <= last; ++code) {
        std::size_t matches = 0;
        for (const auto& entry : entries) {
            if (static_cast<std::uint64_t>(entry.state) == code) {
                ++matches;
            }
        }
        if (matches != 1U) {
            return false;
        }
    }
    return true;
}

consteval bool covers_every_retention_remote_pair() {
    std::array<bool, 15> seen{};
    for (const auto& rule : requirements) {
        const auto retention = static_cast<std::uint64_t>(rule.retention);
        const auto remote = static_cast<std::uint64_t>(rule.remote);
        if (retention < 1U || retention > 3U || remote < 1U || remote > 5U) {
            return false;
        }
        const auto index = (retention - 1U) * 5U + remote - 1U;
        if (seen[index]) {
            return false;
        }
        seen[index] = true;
    }
    for (const auto present : seen) {
        if (!present) {
            return false;
        }
    }
    return true;
}

consteval bool covers_every_fact_tuple_and_field() {
    for (std::size_t left = 0; left < fact_members.size(); ++left) {
        if (fact_members[left] == nullptr) {
            return false;
        }
        for (std::size_t right = left + 1U; right < fact_members.size();
             ++right) {
            if (fact_members[left] == fact_members[right]) {
                return false;
            }
        }
    }
    std::array<bool, 81> seen{};
    for (const auto& input : all_fact_cases()) {
        const auto values = fact_values(input.facts);
        std::size_t index = 0;
        for (std::size_t field = 0; field < values.size(); ++field) {
            const auto code = static_cast<std::size_t>(values[field]);
            if (code > 2U || input.completed[field] != (code == 2U)) {
                return false;
            }
            index = index * 3U + code;
        }
        if (seen[index]) {
            return false;
        }
        seen[index] = true;
    }
    for (const auto present : seen) {
        if (!present) {
            return false;
        }
    }
    return true;
}

// Counts alone cannot detect a repeated fixture replacing an omitted tuple.
// Numeric indices here check coverage, not lifecycle ordering or acceptance.
static_assert(covers_numeric_domain(appends, 1, 4));
static_assert(covers_numeric_domain(replicas, 1, 6));
static_assert(covers_numeric_domain(completions, 0, 2));
static_assert(covers_every_retention_remote_pair());
static_assert(covers_every_fact_tuple_and_field());

TEST(
  SegmentStateValidationTest,
  CompletionFactsHaveThreeValidCodesAndDefaultToUnknown) {
    for (std::uint64_t raw = 0; raw < 256; ++raw) {
        EXPECT_EQ(is_valid(static_cast<completion_state>(raw)), raw <= 2U)
          << raw;
    }
    const segment_completion_facts defaults;
    EXPECT_EQ(
      fact_values(defaults), (std::array{unknown, unknown, unknown, unknown}));
    EXPECT_TRUE(validate_segment_state(
                  append_state::active,
                  retention_state::retained,
                  remote_state::not_offloaded,
                  replica_state::assigned,
                  defaults)
                  .has_value());
    expect_invalid(validate_segment_state(
      append_state::sealed,
      retention_state::deleted,
      remote_state::not_offloaded,
      replica_state::removed,
      defaults));
}

TEST(
  SegmentStateValidationTest,
  AllStateAndFactCombinationsMatchLiteralRequirements) {
    constexpr auto facts = all_fact_cases();
    std::size_t checked = 0;
    for (const auto& rule : requirements) {
        for (const auto append : appends) {
            for (const auto replica : replicas) {
                for (const auto& input : facts) {
                    bool expected
                      = rule.markers_consistent
                        && (!rule.requires_sealed || append.sealed)
                        && (rule.permits_readable || !replica.readable);
                    for (std::size_t field = 0;
                         field < rule.requires_complete.size();
                         ++field) {
                        expected
                          = expected
                            && (!rule.requires_complete[field] || input.completed[field]);
                    }
                    const auto saved = fact_values(input.facts);
                    const auto valid = validate_segment_state(
                      append.state,
                      rule.retention,
                      rule.remote,
                      replica.state,
                      input.facts);
                    ASSERT_EQ(valid.has_value(), expected)
                      << static_cast<unsigned>(append.state) << ':'
                      << static_cast<unsigned>(rule.retention) << ':'
                      << static_cast<unsigned>(rule.remote) << ':'
                      << static_cast<unsigned>(replica.state) << " case "
                      << checked;
                    if (!expected) {
                        EXPECT_EQ(
                          valid.error(), kwaque::errc::invalid_argument);
                    }
                    EXPECT_EQ(fact_values(input.facts), saved);
                    ++checked;
                }
            }
        }
    }
    EXPECT_EQ(checked, 29160U);
}

TEST(SegmentStateValidationTest, RejectsUnknownFactCodesEvenInUnusedFields) {
    for (std::uint64_t raw = 3; raw < 256; ++raw) {
        SCOPED_TRACE(raw);
        for (const auto member : fact_members) {
            segment_completion_facts unused;
            unused.*member = static_cast<completion_state>(raw);
            const auto saved_unused = fact_values(unused);
            expect_invalid(validate_segment_state(
              append_state::active,
              retention_state::retained,
              remote_state::not_offloaded,
              replica_state::assigned,
              unused));
            EXPECT_EQ(fact_values(unused), saved_unused);

            auto required = all_complete;
            required.*member = static_cast<completion_state>(raw);
            const auto saved_required = fact_values(required);
            expect_invalid(validate_segment_state(
              append_state::sealed,
              retention_state::deleted,
              remote_state::remote_deleted,
              replica_state::removed,
              required));
            EXPECT_EQ(fact_values(required), saved_required);
        }
    }
}

TEST(SegmentStateValidationTest, RejectsEveryUnknownOrUninitializedAxisCode) {
    for (std::uint64_t raw = 0; raw < 256; ++raw) {
        SCOPED_TRACE(raw);
        if (raw == 0 || raw > 4) {
            expect_invalid(validate_segment_state(
              static_cast<append_state>(raw),
              retention_state::retained,
              remote_state::not_offloaded,
              replica_state::assigned,
              all_complete));
        }
        if (raw == 0 || raw > 3) {
            expect_invalid(validate_segment_state(
              append_state::sealed,
              static_cast<retention_state>(raw),
              remote_state::not_offloaded,
              replica_state::assigned,
              all_complete));
        }
        if (raw == 0 || raw > 5) {
            expect_invalid(validate_segment_state(
              append_state::sealed,
              retention_state::retained,
              static_cast<remote_state>(raw),
              replica_state::assigned,
              all_complete));
        }
        if (raw == 0 || raw > 6) {
            expect_invalid(validate_segment_state(
              append_state::sealed,
              retention_state::retained,
              remote_state::not_offloaded,
              static_cast<replica_state>(raw),
              all_complete));
        }
    }
}

TEST(
  SegmentStateValidationTest,
  UnknownMetadataOutcomeCannotCertifyOffloadCommit) {
    for (const auto not_committed : {unknown, incomplete}) {
        auto facts = all_complete;
        facts.offload_commit = not_committed;
        EXPECT_TRUE(validate_segment_state(
                      append_state::sealed,
                      retention_state::retained,
                      remote_state::offloading,
                      replica_state::readable,
                      facts)
                      .has_value());
        for (const auto remote :
             {remote_state::offloaded,
              remote_state::remote_delete_pending,
              remote_state::remote_deleted}) {
            expect_invalid(validate_segment_state(
              append_state::sealed,
              retention_state::retained,
              remote,
              replica_state::readable,
              facts));
        }
    }
    EXPECT_TRUE(validate_segment_state(
                  append_state::sealed,
                  retention_state::retained,
                  remote_state::offloaded,
                  replica_state::readable,
                  all_complete)
                  .has_value());
}

TEST(
  SegmentStateValidationTest,
  RepairDebtAndHistoricalCommitDoNotRequireCurrentReadability) {
    EXPECT_TRUE(validate_segment_state(
                  append_state::sealed,
                  retention_state::retained,
                  remote_state::not_offloaded,
                  replica_state::lost,
                  {})
                  .has_value());
    const segment_completion_facts committed{
      complete, unknown, unknown, unknown};
    for (const auto replica :
         {replica_state::assigned,
          replica_state::copying,
          replica_state::readable,
          replica_state::lost,
          replica_state::removing,
          replica_state::removed}) {
        EXPECT_TRUE(validate_segment_state(
                      append_state::sealed,
                      retention_state::retained,
                      remote_state::offloaded,
                      replica,
                      committed)
                      .has_value());
    }
}

TEST(
  SegmentStateValidationTest,
  DeletedRequiresAggregateCleanupAndConsistentCurrentMarkers) {
    expect_invalid(validate_segment_state(
      append_state::sealed,
      retention_state::deleted,
      remote_state::offloaded,
      replica_state::removed,
      all_complete));
    expect_invalid(validate_segment_state(
      append_state::sealed,
      retention_state::deleted,
      remote_state::remote_deleted,
      replica_state::readable,
      all_complete));
    EXPECT_TRUE(validate_segment_state(
                  append_state::sealed,
                  retention_state::deleted,
                  remote_state::remote_deleted,
                  replica_state::removed,
                  all_complete)
                  .has_value());

    for (const auto not_complete : {unknown, incomplete}) {
        auto remote_partial = all_complete;
        remote_partial.remote_cleanup = not_complete;
        expect_invalid(validate_segment_state(
          append_state::sealed,
          retention_state::deleted,
          remote_state::not_offloaded,
          replica_state::removed,
          remote_partial));
        auto local_partial = all_complete;
        local_partial.local_cleanup = not_complete;
        expect_invalid(validate_segment_state(
          append_state::sealed,
          retention_state::deleted,
          remote_state::remote_deleted,
          replica_state::lost,
          local_partial));
    }
}

TEST(
  SegmentStateValidationTest,
  ConfirmedLocalOnlyCleanupDoesNotRequireSealingOrAnOffload) {
    const segment_completion_facts cleaned{
      unknown, complete, complete, complete};
    for (const auto append :
         {append_state::creating,
          append_state::active,
          append_state::sealing,
          append_state::sealed}) {
        EXPECT_TRUE(validate_segment_state(
                      append,
                      retention_state::deleted,
                      remote_state::not_offloaded,
                      replica_state::removed,
                      cleaned)
                      .has_value());
    }
    expect_invalid(validate_segment_state(
      append_state::active,
      retention_state::deleted,
      remote_state::offloading,
      replica_state::removed,
      cleaned));
}

TEST(
  SegmentStateValidationTest,
  RetainedLocalDataCanRemainReadableAfterRemoteDeletion) {
    const segment_completion_facts remote_gone{
      complete, unknown, complete, unknown};
    EXPECT_TRUE(validate_segment_state(
                  append_state::sealed,
                  retention_state::retained,
                  remote_state::remote_deleted,
                  replica_state::readable,
                  remote_gone)
                  .has_value());
    auto expiring = remote_gone;
    expiring.expiration_commit = complete;
    EXPECT_TRUE(validate_segment_state(
                  append_state::sealed,
                  retention_state::expiring,
                  remote_state::remote_deleted,
                  replica_state::readable,
                  expiring)
                  .has_value());
    expect_invalid(validate_segment_state(
      append_state::sealed,
      retention_state::deleted,
      remote_state::remote_deleted,
      replica_state::readable,
      expiring));
}

} // namespace
