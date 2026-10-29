#include "src/storage/local_append_verdict.h"

#include <gtest/gtest.h>

#include <map>
#include <string_view>

namespace kwaque::storage {
namespace {
using P = local_append_placement;
using W = local_wal_state;
using S = local_segment_state;
using E = local_earlier_state;
using V = local_append_verdict;

// Every combination of placement, WAL state, segment state and how the
// earlier groups of its segment were published matches exactly one case,
// and every case is reached.
TEST(LocalAppendVerdictTest, EveryStateMatchesOneCase) {
    std::map<std::string_view, unsigned> hits;
    unsigned impossible = 0;
    for (std::size_t p = 0; p < local_append_placements; ++p)
        for (std::size_t w = 0; w < local_wal_states; ++w)
            for (std::size_t s = 0; s < local_segment_states; ++s)
                for (std::size_t e = 0; e < local_earlier_states; ++e) {
                    const local_verdict_case* matched = nullptr;
                    for (const auto& candidate : local_verdict_cases()) {
                        if (!candidate.matches(
                              static_cast<P>(p),
                              static_cast<W>(w),
                              static_cast<S>(s),
                              static_cast<E>(e)))
                            continue;
                        ASSERT_EQ(matched, nullptr)
                          << p << w << s << e << ' ' << candidate.label;
                        matched = &candidate;
                    }
                    ASSERT_NE(matched, nullptr) << p << w << s << e;
                    if (matched->verdict == V::impossible)
                        ++impossible;
                    else
                        ++hits[matched->label];
                }
    unsigned possible = 0;
    for (const auto& candidate : local_verdict_cases()) {
        if (candidate.verdict == V::impossible) continue;
        EXPECT_GT(hits[candidate.label], 0U) << candidate.label;
        possible += hits[candidate.label];
    }
    EXPECT_EQ(possible, 54U);
    EXPECT_EQ(impossible, 114U);
}

V verdict(P placement, W wal, S segment, E earlier = E::durable) {
    return classify_local_append(placement, wal, segment, earlier).verdict;
}

// Either place makes a WAL-placed group durable, whichever comes first.
TEST(LocalAppendVerdictTest, WalPlacedTakesTheFirstBarrier) {
    EXPECT_EQ(verdict(P::wal, W::durable, S::frozen), V::durable);
    EXPECT_EQ(verdict(P::wal, W::durable, S::failed), V::durable);
    EXPECT_EQ(verdict(P::wal, W::pending, S::durable), V::durable);
    EXPECT_EQ(verdict(P::wal, W::failed, S::durable), V::durable);
    EXPECT_EQ(verdict(P::wal, W::pending, S::submitted), V::wait);
    EXPECT_EQ(verdict(P::wal, W::pending, S::failed), V::wait);
    EXPECT_EQ(verdict(P::wal, W::failed, S::submitted), V::wait);
    EXPECT_EQ(verdict(P::wal, W::failed, S::failed), V::uncertain);
    EXPECT_EQ(verdict(P::wal, W::failed, S::dropped), V::uncertain);
}

// A segment-placed group has only its segment's barrier.
TEST(LocalAppendVerdictTest, SegmentPlacedTakesItsBarrier) {
    EXPECT_EQ(verdict(P::segment, W::none, S::frozen), V::wait);
    EXPECT_EQ(verdict(P::segment, W::none, S::submitted), V::wait);
    EXPECT_EQ(verdict(P::segment, W::none, S::durable), V::durable);
    EXPECT_EQ(verdict(P::segment, W::none, S::failed), V::uncertain);
    EXPECT_EQ(verdict(P::segment, W::none, S::dropped), V::not_written);
}

// Without the WAL's answer nothing is decided; a proven refusal is
// not_written, an unproven one uncertain.
TEST(LocalAppendVerdictTest, UnacceptedWaitsForTheAnswer) {
    EXPECT_EQ(verdict(P::wal, W::deciding, S::frozen), V::wait);
    EXPECT_EQ(verdict(P::wal, W::refused, S::frozen), V::wait);
    EXPECT_EQ(verdict(P::wal, W::refused, S::dropped), V::not_written);
    EXPECT_EQ(verdict(P::wal, W::unknown, S::dropped), V::uncertain);
    EXPECT_EQ(verdict(P::wal, W::none, S::unfrozen), V::not_written);
    EXPECT_EQ(verdict(P::segment, W::none, S::unfrozen), V::not_written);
}

// After an earlier failure in its segment nothing is durable, even with its
// own WAL barrier passed: restart could not fill the place before it.
TEST(LocalAppendVerdictTest, NothingDurablePastAFailure) {
    EXPECT_EQ(
      verdict(P::wal, W::durable, S::submitted, E::failed), V::uncertain);
    EXPECT_EQ(verdict(P::wal, W::pending, S::frozen, E::failed), V::uncertain);
    EXPECT_EQ(
      verdict(P::segment, W::none, S::durable, E::failed), V::uncertain);
    EXPECT_EQ(
      verdict(P::segment, W::none, S::dropped, E::failed), V::not_written);
    EXPECT_EQ(verdict(P::segment, W::none, S::frozen, E::failed), V::wait);
    EXPECT_EQ(
      verdict(P::wal, W::refused, S::dropped, E::failed), V::not_written);
}
} // namespace
} // namespace kwaque::storage
