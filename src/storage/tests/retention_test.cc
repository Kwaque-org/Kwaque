#include "src/base/units.h"
#include "src/codec/digest.h"
#include "src/model/identity.h"
#include "src/model/position.h"
#include "src/runtime/file_position.h"
#include "src/runtime/time.h"
#include "src/storage/format_context.h"
#include "src/storage/retention.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <optional>
#include <vector>

namespace kwaque::storage {
namespace {
template<typename Id>
Id id(std::uint8_t value) {
    std::array<std::uint8_t, 16> bytes{};
    bytes.fill(value);
    return Id::make(bytes).value();
}
segment_context segment(std::uint64_t generation) {
    return segment_context::make(
             id<model::cluster_id>(1),
             id<model::topic_id>(4),
             id<model::range_id>(5),
             id<model::segment_id>(6),
             model::segment_generation::make(generation).value())
      .value();
}
// Ten sealed segments of ten positions each, sealed a second apart, each
// holding 100 bytes that nothing local needs.
struct range final {
    range() {
        for (std::uint64_t i = 0; i != 10; ++i) {
            segments.push_back(
              {segment(i + 1),
               model::range_logical_end{10 * (i + 1)},
               runtime::wall_time{static_cast<std::int64_t>(1'000 * (i + 1))}});
            local.push_back({byte_count{100}, retention_need::none});
        }
    }
    [[nodiscard]] retention_candidates select(
      std::optional<std::int64_t> cutoff,
      std::optional<std::uint64_t> target,
      std::uint64_t bound = 100) const {
        return select_retention(
          {segments,
           cutoff ? std::optional{runtime::wall_time{*cutoff}} : std::nullopt,
           target ? std::optional{byte_count{*target}} : std::nullopt,
           model::range_logical_end{bound}},
          local);
    }
    std::vector<retention_segment> segments;
    std::vector<retention_local> local;
};
segment_immutable_expectation extent(std::uint8_t mark, std::uint64_t records) {
    codec::content_digest bytes{};
    bytes.fill(mark);
    return {
      coverage{
        model::range_logical_span::make(
          model::range_logical_end{100},
          model::range_logical_end{100 + records})
          .value(),
        model::segment_relative_span::make(
          model::segment_relative_end{}, model::segment_relative_end{records})
          .value(),
        model::file_byte_span::make(
          runtime::file_position{4096}, runtime::file_position{8192})
          .value()},
      codec::extent_digest{bytes},
      runtime::file_position{12288}};
}

// The walk goes oldest first and stops at the first segment that may not
// go: what it answers is a prefix.
TEST(RetentionTest, ByAgeTheWalkStopsAtTheFirstSegmentThatIsNotOldEnough) {
    const range ten;
    // Sealed before the cutoff is strictly before it.
    EXPECT_EQ(
      ten.select(6'001, std::nullopt),
      (retention_candidates{6, byte_count{600}, retention_stop::kept}));
    EXPECT_EQ(
      ten.select(6'000, std::nullopt),
      (retention_candidates{5, byte_count{500}, retention_stop::kept}));
    EXPECT_EQ(ten.select(1'000, std::nullopt).count, 0U);
    EXPECT_EQ(
      ten.select(1'000'000, std::nullopt),
      (retention_candidates{10, byte_count{1000}, retention_stop::exhausted}));
    // With no rule nothing goes.
    EXPECT_EQ(
      ten.select(std::nullopt, std::nullopt),
      (retention_candidates{0, {}, retention_stop::kept}));
    // A young segment between old ones ends the walk: only a prefix goes.
    range gap;
    gap.segments[2].sealed = runtime::wall_time{50'000};
    EXPECT_EQ(gap.select(9'000, std::nullopt).count, 2U);
}

// Nothing that ends past the bound goes, whatever the rules say.
TEST(RetentionTest, NothingPastTheBoundIsSelected) {
    const range ten;
    for (std::uint64_t bound = 0; bound <= 100; ++bound) {
        const auto selected = ten.select(1'000'000, 0, bound);
        EXPECT_EQ(selected.count, bound / 10);
        EXPECT_EQ(
          selected.stop,
          bound == 100 ? retention_stop::exhausted : retention_stop::bound);
        for (std::size_t i = 0; i != selected.count; ++i)
            EXPECT_LE(ten.segments[i].end.value(), bound);
    }
}

// By size the oldest go while the range holds more than its target, and
// never so many that it falls below it. No time is needed.
TEST(RetentionTest, BySizeTheRangeNeverFallsBelowItsTarget) {
    range ten;
    for (auto& held : ten.segments)
        held.sealed.reset();
    EXPECT_EQ(
      ten.select(std::nullopt, 400),
      (retention_candidates{6, byte_count{600}, retention_stop::kept}));
    // A segment that would take the range below its target stays.
    EXPECT_EQ(ten.select(std::nullopt, 450).count, 5U);
    EXPECT_EQ(ten.select(std::nullopt, 1000).count, 0U);
    EXPECT_EQ(ten.select(std::nullopt, 5000).count, 0U);
    EXPECT_EQ(
      ten.select(std::nullopt, 0),
      (retention_candidates{10, byte_count{1000}, retention_stop::exhausted}));
    // With no time an age rule selects nothing, and size still does.
    EXPECT_EQ(ten.select(1'000'000, std::nullopt).count, 0U);
    EXPECT_EQ(ten.select(1'000'000, 900).count, 1U);
    // Uneven segments: the first that does not fit what is over ends it.
    ten.local[1].bytes = byte_count{500};
    EXPECT_EQ(ten.select(std::nullopt, 1000).count, 1U);
    EXPECT_EQ(ten.select(std::nullopt, 800).count, 2U);
}

// Either rule lets a segment go, and what age frees counts toward the
// target.
TEST(RetentionTest, EitherRuleSelectsAndAgeCountsTowardTheTarget) {
    const range ten;
    // Three by age; the target of 600 lets one more go by size.
    EXPECT_EQ(ten.select(3'001, 600).count, 4U);
    // The target alone lets two go; age takes three.
    EXPECT_EQ(ten.select(3'001, 800).count, 3U);
}

// Something local that still needs a segment ends the walk there, and the
// answer says what.
TEST(RetentionTest, WhatIsStillNeededHereIsNotSelected) {
    range ten;
    ten.local[4].need = retention_need::read;
    EXPECT_EQ(
      ten.select(1'000'000, 0),
      (retention_candidates{
        4, byte_count{400}, retention_stop::needed, retention_need::read}));
    ten.local[4].need = retention_need::none;
    ten.local[9] = {byte_count{40}, retention_need::unsealed};
    EXPECT_EQ(
      ten.select(1'000'000, 0),
      (retention_candidates{
        9, byte_count{900}, retention_stop::needed, retention_need::unsealed}));
    // What the shard was told nothing of is not selected either.
    ten.local.resize(3);
    EXPECT_EQ(
      ten.select(1'000'000, std::nullopt),
      (retention_candidates{
        3, byte_count{300}, retention_stop::needed, retention_need::unknown}));
    // The bound is looked at before what is needed here.
    ten.local[2].need = retention_need::read;
    EXPECT_EQ(ten.select(1'000'000, 0, 25).stop, retention_stop::bound);
}

// The rules are asked first. A segment neither selects ends the walk as
// kept, though it ends past the bound or is read: nothing holds retention
// back there. One a rule selects says what does.
TEST(RetentionTest, TheReasonSaysWhetherRetentionIsHeldBack) {
    range ten;
    ten.local[3].need = retention_need::read;
    // Three are old enough; the fourth is read and is not.
    EXPECT_EQ(
      ten.select(3'001, std::nullopt),
      (retention_candidates{3, byte_count{300}, retention_stop::kept}));
    // And past the bound as well.
    EXPECT_EQ(ten.select(3'001, std::nullopt, 35).stop, retention_stop::kept);
    // Old enough and read: the reader holds it.
    EXPECT_EQ(
      ten.select(4'001, std::nullopt),
      (retention_candidates{
        3, byte_count{300}, retention_stop::needed, retention_need::read}));
    // The segment that is appended to, once a rule wants it.
    ten.local[3].need = retention_need::none;
    ten.local[9] = {byte_count{100}, retention_need::unsealed};
    ten.segments[9].sealed.reset();
    EXPECT_EQ(ten.select(1'000'000, std::nullopt).stop, retention_stop::kept);
    EXPECT_EQ(
      ten.select(std::nullopt, 0),
      (retention_candidates{
        9, byte_count{900}, retention_stop::needed, retention_need::unsealed}));
}

// Nothing goes by size while the range holds less than its target, a
// segment of no bytes neither.
TEST(RetentionTest, NothingGoesBySizeUnderTheTarget) {
    range ten;
    for (auto& held : ten.segments)
        held.sealed.reset();
    ten.local[0].bytes = byte_count{};
    EXPECT_EQ(
      ten.select(std::nullopt, 5000),
      (retention_candidates{0, {}, retention_stop::kept}));
    // At its target or over it, one that frees nothing may go.
    EXPECT_EQ(ten.select(std::nullopt, 900).count, 1U);
    EXPECT_EQ(ten.select(std::nullopt, 800).count, 2U);
}

// A segment that is being removed already is offered again whatever the
// rules and the bound say of it, and the walk goes on behind it.
TEST(RetentionTest, ASegmentAlreadyGoingIsCountedWhateverTheRulesSay) {
    range ten;
    ten.segments[0].sealed.reset();
    ten.local[0] = {byte_count{}, retention_need::none, true};
    EXPECT_EQ(
      ten.select(3'001, std::nullopt),
      (retention_candidates{3, byte_count{200}, retention_stop::kept}));
    EXPECT_EQ(ten.select(std::nullopt, std::nullopt, 0).count, 1U);
    EXPECT_EQ(
      ten.select(std::nullopt, std::nullopt, 0).stop, retention_stop::kept);
}

// An authorization names a segment and what its owner holds it to be. It
// is accepted only for exactly that.
TEST(DeletionAuthorizationTest, OnlyWhatTheShardHoldsIsAuthorized) {
    const auto a = segment(1);
    const auto sealed = extent(7, 3);
    const deletion_authorization exact{a, sealed};
    const deletion_authorization discard{a, std::nullopt};
    const segment_held held{a, segment_holding::sealed, sealed, std::nullopt};
    EXPECT_EQ(
      authorize_deletion(exact, held), authorization_verdict::authorized);
    // A sealed segment named as unsealed.
    EXPECT_EQ(
      authorize_deletion(discard, held), authorization_verdict::sealed_here);
    // Another identity: one field of the extent differs.
    auto other = sealed;
    other.file_end = runtime::file_position{16384};
    EXPECT_EQ(
      authorize_deletion({a, other}, held),
      authorization_verdict::wrong_identity);
    EXPECT_EQ(
      authorize_deletion({a, extent(8, 3)}, held),
      authorization_verdict::wrong_identity);
    EXPECT_EQ(
      authorize_deletion({a, extent(7, 4)}, held),
      authorization_verdict::wrong_identity);
    // Another context.
    EXPECT_EQ(
      authorize_deletion({segment(2), sealed}, held),
      authorization_verdict::wrong_context);
}

TEST(DeletionAuthorizationTest, AnAttachedSegmentIsRefusedUntilDetached) {
    const auto a = segment(1);
    const auto sealed = extent(7, 3);
    segment_held held{a, segment_holding::attached, std::nullopt, std::nullopt};
    EXPECT_EQ(
      authorize_deletion({a, std::nullopt}, held),
      authorization_verdict::attached);
    EXPECT_EQ(
      authorize_deletion({a, sealed}, held), authorization_verdict::attached);
    // Detached and not sealed: it may be discarded, and only as that.
    held.holding = segment_holding::unsealed;
    EXPECT_EQ(
      authorize_deletion({a, std::nullopt}, held),
      authorization_verdict::authorized);
    EXPECT_EQ(
      authorize_deletion({a, sealed}, held),
      authorization_verdict::wrong_identity);
    held.holding = segment_holding::unknown;
    EXPECT_EQ(
      authorize_deletion({a, sealed}, held), authorization_verdict::unknown);
}

// A repeat for a segment that is already being removed is the first answer
// again; anything else is refused.
TEST(DeletionAuthorizationTest, ARepeatIsTheFirstAnswerAgain) {
    const auto a = segment(1);
    const auto sealed = extent(7, 3);
    const deletion_authorization first{a, sealed};
    const segment_held held{a, segment_holding::deleting, sealed, first};
    EXPECT_EQ(authorize_deletion(first, held), authorization_verdict::repeated);
    EXPECT_EQ(
      authorize_deletion({a, extent(8, 3)}, held),
      authorization_verdict::wrong_identity);
    EXPECT_EQ(
      authorize_deletion({a, std::nullopt}, held),
      authorization_verdict::wrong_identity);
    const deletion_authorization discarded{a, std::nullopt};
    const segment_held dropped{
      a, segment_holding::deleting, std::nullopt, discarded};
    EXPECT_EQ(
      authorize_deletion(discarded, dropped), authorization_verdict::repeated);
    // One found under its deleting name with nothing accepted takes the
    // first authorization that names it, and no other segment's.
    const segment_held marked{
      a, segment_holding::deleting, std::nullopt, std::nullopt};
    EXPECT_EQ(
      authorize_deletion(first, marked), authorization_verdict::authorized);
    EXPECT_EQ(
      authorize_deletion(discarded, marked), authorization_verdict::authorized);
    EXPECT_EQ(
      authorize_deletion({segment(2), sealed}, marked),
      authorization_verdict::wrong_context);
}
} // namespace
} // namespace kwaque::storage
