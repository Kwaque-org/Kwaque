#include "src/storage/tests/engine_ledger.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace {
using kwaque::storage::testing::engine_ledger;
using verdict = engine_ledger::verdict;
using name = engine_ledger::name;
using listing = engine_ledger::listing;
using batch = engine_ledger::read_batch;

// One segment with three requests at 100, 101 and 102: the first two are
// reported durable, the third is accepted and has no result.
struct history final {
    history() {
        ledger.create_asked(1);
        ledger.created(1);
        for (std::uint64_t logical = 100; logical != 103; ++logical) {
            const auto id = ledger.offer(
              1, logical, "child-" + std::to_string(logical));
            ledger.accepted(id);
            ids.push_back(id);
        }
        ledger.durable(ids[0], 4096, "block-100");
        ledger.durable(ids[1], 8192, "block-101");
    }
    engine_ledger ledger;
    std::vector<engine_ledger::request_id> ids;
};
const std::string a = "child-100", b = "child-101", c = "child-102";

TEST(EngineLedgerTest, WhatWasReportedDurableIsReadBackInOrder) {
    history told;
    const std::array whole{batch{100, a}, batch{101, b}};
    EXPECT_EQ(told.ledger.read_back(1, whole, 200, true), verdict::holds);
    // A request with no result may be there too.
    const std::array more{batch{100, a}, batch{101, b}, batch{102, c}};
    EXPECT_EQ(told.ledger.read_back(1, more, 200, true), verdict::holds);
    // A read that stopped early says nothing of what follows it.
    const std::array part{batch{100, a}};
    EXPECT_EQ(told.ledger.read_back(1, part, 200, false), verdict::holds);
    EXPECT_EQ(told.ledger.first(), verdict::holds);
    EXPECT_EQ(told.ledger.acknowledged(1), 2U);
}

TEST(EngineLedgerTest, AMissingAcknowledgedRequestIsLost) {
    history told;
    const std::array short_read{batch{100, a}};
    EXPECT_EQ(told.ledger.read_back(1, short_read, 200, true), verdict::lost);
    EXPECT_EQ(told.ledger.first(), verdict::lost);
    history stepped;
    const std::array over{batch{101, b}};
    EXPECT_EQ(stepped.ledger.read_back(1, over, 200, false), verdict::lost);
}

TEST(EngineLedgerTest, SwappedRequestsAreReordered) {
    history told;
    const std::array swapped{batch{101, b}, batch{100, a}};
    // The first step over an acknowledged request is already a loss; the
    // order alone is told apart on requests with no result.
    EXPECT_EQ(told.ledger.read_back(1, swapped, 200, false), verdict::lost);
    engine_ledger ledger;
    ledger.create_asked(1);
    ledger.created(1);
    for (std::uint64_t logical = 100; logical != 102; ++logical)
        ledger.accepted(ledger.offer(1, logical, std::to_string(logical)));
    const std::string first = "100", second = "101";
    const std::array out_of_order{batch{101, second}, batch{100, first}};
    EXPECT_EQ(
      ledger.read_back(1, out_of_order, 200, false), verdict::reordered);
}

TEST(EngineLedgerTest, ACandidateBeyondTheBoundIsExposed) {
    history told;
    const std::array beyond{batch{100, a}, batch{101, b}, batch{102, c}};
    EXPECT_EQ(told.ledger.read_back(1, beyond, 102, true), verdict::exposed);
    // Below the bound the acknowledged ones are still owed.
    history cut;
    const std::array below{batch{100, a}};
    EXPECT_EQ(cut.ledger.read_back(1, below, 101, true), verdict::holds);
}

TEST(EngineLedgerTest, ARefusedRequestOrAStrangerIsForeign) {
    history told;
    const auto refused = told.ledger.offer(1, 103, "child-103");
    told.ledger.refused(refused);
    const std::string d = "child-103", stranger = "child-900";
    const std::array with_refused{batch{100, a}, batch{101, b}, batch{103, d}};
    EXPECT_EQ(
      told.ledger.read_back(1, with_refused, 200, true), verdict::foreign);
    history other;
    const std::array with_stranger{batch{100, a}, batch{900, stranger}};
    EXPECT_EQ(
      other.ledger.read_back(1, with_stranger, 1000, false), verdict::foreign);
}

TEST(EngineLedgerTest, ARemovedSegmentThatIsReadIsResurrected) {
    history told;
    told.ledger.removal_asked(1);
    told.ledger.removed(1);
    EXPECT_EQ(told.ledger.listed(1), listing::none);
    EXPECT_EQ(told.ledger.read_back(1, {}, 200, true), verdict::holds);
    EXPECT_EQ(told.ledger.image_missing(1), verdict::holds);
    const std::array back{batch{100, a}};
    EXPECT_EQ(told.ledger.read_back(1, back, 200, false), verdict::resurrected);
    history named;
    named.ledger.removal_asked(1);
    named.ledger.removed(1);
    EXPECT_EQ(named.ledger.named(1, name::own), verdict::resurrected);
}

TEST(EngineLedgerTest, WrongBytesAreAltered) {
    history told;
    const std::string wrong = "child-10x";
    const std::array changed{batch{100, a}, batch{101, wrong}};
    EXPECT_EQ(told.ledger.read_back(1, changed, 200, true), verdict::altered);
}

TEST(EngineLedgerTest, TheImageHoldsEveryAcknowledgedBlockWhereItWasReported) {
    history told;
    std::string file(16384, '\0');
    file.replace(4096, 9, "block-100");
    file.replace(8192, 9, "block-101");
    EXPECT_EQ(told.ledger.image_back(1, file), verdict::holds);
    auto changed = file;
    changed[8195] = 'X';
    EXPECT_EQ(told.ledger.image_back(1, changed), verdict::altered);
    history cut;
    EXPECT_EQ(cut.ledger.image_back(1, file.substr(0, 8195)), verdict::lost);
    history gone;
    EXPECT_EQ(gone.ledger.image_missing(1), verdict::lost);
    // Blocks reported against the segment's order.
    engine_ledger ledger;
    ledger.create_asked(1);
    ledger.created(1);
    const auto early = ledger.offer(1, 100, "x");
    const auto late = ledger.offer(1, 101, "y");
    ledger.accepted(early);
    ledger.accepted(late);
    ledger.durable(late, 4096, "block-101");
    ledger.durable(early, 8192, "block-100");
    std::string crossed(16384, '\0');
    crossed.replace(4096, 9, "block-101");
    crossed.replace(8192, 9, "block-100");
    EXPECT_EQ(ledger.image_back(1, crossed), verdict::reordered);
}

TEST(EngineLedgerTest, AResultThatArrivesInACrashIsNotAResult) {
    history told;
    told.ledger.crash();
    told.ledger.durable(told.ids[2], 12288, "block-102");
    EXPECT_EQ(told.ledger.acknowledged(1), 2U);
    // What was offered and never answered may have been accepted.
    const auto unanswered = told.ledger.offer(1, 103, "child-103");
    static_cast<void>(unanswered);
    told.ledger.restart();
    const std::string d = "child-103";
    const std::array with_both{
      batch{100, a}, batch{101, b}, batch{102, c}, batch{103, d}};
    EXPECT_EQ(told.ledger.read_back(1, with_both, 200, true), verdict::holds);
    const std::array without{batch{100, a}, batch{101, b}};
    EXPECT_EQ(told.ledger.read_back(1, without, 200, true), verdict::holds);
    EXPECT_EQ(told.ledger.first(), verdict::holds);
}

TEST(EngineLedgerTest, ASegmentThatTakesNoMoreIsFenced) {
    history sealed;
    sealed.ledger.seal_asked(1);
    sealed.ledger.accepted(sealed.ledger.offer(1, 103, "late"));
    EXPECT_EQ(sealed.ledger.first(), verdict::unfenced);

    history failed;
    failed.ledger.failed(failed.ids[2]);
    failed.ledger.refused(failed.ledger.offer(1, 103, "late"));
    EXPECT_EQ(failed.ledger.first(), verdict::holds);
    failed.ledger.accepted(failed.ledger.offer(1, 104, "later"));
    EXPECT_EQ(failed.ledger.first(), verdict::unfenced);

    history found;
    found.ledger.crash();
    found.ledger.restart();
    found.ledger.accepted(found.ledger.offer(1, 103, "late"));
    EXPECT_EQ(found.ledger.first(), verdict::unfenced);
}

TEST(EngineLedgerTest, ANameIsOneItsHistoryAllows) {
    engine_ledger ledger;
    // A creation with no answer: there or not, and discarded by a restart.
    ledger.create_asked(1);
    EXPECT_EQ(ledger.listed(1), listing::expiring);
    ledger.crash();
    ledger.created(1);
    ledger.restart();
    EXPECT_EQ(ledger.listed(1), listing::expiring);
    EXPECT_EQ(ledger.named(1, name::absent), verdict::holds);
    EXPECT_EQ(ledger.named(1, name::own), verdict::misnamed);

    // One that made nothing is not listed at all.
    engine_ledger nothing;
    nothing.create_asked(1);
    nothing.create_refused(1, true);
    EXPECT_EQ(nothing.listed(1), listing::none);
    EXPECT_EQ(nothing.named(1, name::absent), verdict::holds);

    // A created segment is under its own name until its removal is asked.
    history kept;
    EXPECT_EQ(kept.ledger.listed(1), listing::kept);
    EXPECT_TRUE(kept.ledger.unsealed(1));
    EXPECT_EQ(kept.ledger.named(1, name::deleting), verdict::misnamed);
    history asked;
    asked.ledger.removal_asked(1);
    EXPECT_EQ(asked.ledger.listed(1), listing::expiring);
    EXPECT_EQ(asked.ledger.named(1, name::deleting), verdict::holds);
    // Seen under its deleting name, it does not come back under its own.
    asked.ledger.restart();
    EXPECT_EQ(asked.ledger.named(1, name::absent), verdict::holds);
    EXPECT_EQ(asked.ledger.named(1, name::own), verdict::misnamed);
}

TEST(EngineLedgerTest, WithNoFaultEveryRequestHasItsResult) {
    history told;
    EXPECT_EQ(told.ledger.progressed(), verdict::stalled);
    history faulted;
    faulted.ledger.faulted();
    EXPECT_EQ(faulted.ledger.progressed(), verdict::holds);
    history crashed;
    crashed.ledger.crash();
    EXPECT_EQ(crashed.ledger.progressed(), verdict::holds);
    // The next life owes nothing for the one before it.
    crashed.ledger.restart();
    EXPECT_EQ(crashed.ledger.progressed(), verdict::holds);
    history done;
    done.ledger.durable(done.ids[2], 12288, "block-102");
    EXPECT_EQ(done.ledger.progressed(), verdict::holds);
}

TEST(EngineLedgerTest, TheDriversOwnContradictionsAreMisuse) {
    engine_ledger twice;
    twice.create_asked(1);
    twice.create_asked(1);
    EXPECT_EQ(twice.first(), verdict::misuse);
    engine_ledger nowhere;
    static_cast<void>(nowhere.offer(7, 100, "x"));
    EXPECT_EQ(nowhere.first(), verdict::misuse);
    history falling;
    static_cast<void>(falling.ledger.offer(1, 101, "again"));
    EXPECT_EQ(falling.ledger.first(), verdict::misuse);
    history unaccepted;
    const auto id = unaccepted.ledger.offer(1, 103, "x");
    unaccepted.ledger.durable(id, 12288, "block");
    EXPECT_EQ(unaccepted.ledger.first(), verdict::misuse);
}

// Everything one ledger is told, written down as it is told.
struct recorded final {
    recorded() {
        ledger.record(
          [this](const std::string& text) { lines.push_back(text); });
        ledger.create_asked(1);
        ledger.created(1);
        ledger.create_asked(2);
        ledger.create_refused(2, true);
        const auto first = ledger.offer(1, 100, a);
        ledger.accepted(first);
        ledger.durable(first, 4096, std::string{"\0block\xff", 7});
        const auto second = ledger.offer(1, 101, "");
        ledger.refused(second);
        ledger.note("the driver's own");
        const auto third = ledger.offer(1, 102, c);
        ledger.accepted(third);
        ledger.seal_asked(1);
        ledger.sealed(1);
        ledger.removal_asked(1);
    }
    engine_ledger ledger;
    std::vector<std::string> lines;
};

TEST(EngineLedgerTest, ALedgerThatHearsTheSameLinesKnowsTheSame) {
    recorded told;
    ASSERT_EQ(told.ledger.first(), verdict::holds);
    engine_ledger heard;
    std::size_t notes = 0;
    for (const auto& text : told.lines) {
        const auto was = heard.hear(text);
        if (was == engine_ledger::line::note) {
            ++notes;
        } else {
            EXPECT_EQ(was, engine_ledger::line::told) << text;
        }
    }
    EXPECT_EQ(notes, 1U);
    EXPECT_EQ(heard.first(), verdict::holds);
    for (auto* ledger : {&told.ledger, &heard}) {
        ledger->crash();
        ledger->restart();
        EXPECT_EQ(ledger->segments(), (std::vector<std::uint64_t>{1, 2}));
        EXPECT_EQ(ledger->listed(1), listing::expiring);
        EXPECT_EQ(ledger->listed(2), listing::none);
        EXPECT_EQ(ledger->acknowledged(1), 1U);
        EXPECT_FALSE(ledger->unsealed(1));
        // The block is where it was reported, every byte of it.
        std::string file(4096, 'x');
        file += std::string{"\0block\xff", 7};
        EXPECT_EQ(ledger->image_back(1, file), verdict::holds);
        file[4096] = 'y';
        EXPECT_EQ(ledger->image_back(1, file), verdict::altered);
    }
}

TEST(EngineLedgerTest, AnAnswerThatIsWithheldWasNeverHeard) {
    recorded told;
    engine_ledger heard;
    for (const auto& text : told.lines) {
        const auto was = heard.hear(text, true);
        EXPECT_NE(was, engine_ledger::line::unknown) << text;
    }
    heard.crash();
    heard.restart();
    // Neither creation was answered: each is an interrupted one. Nothing
    // was reported durable, so nothing is owed.
    EXPECT_EQ(heard.listed(1), listing::expiring);
    EXPECT_EQ(heard.listed(2), listing::expiring);
    EXPECT_EQ(heard.acknowledged(1), 0U);
    EXPECT_EQ(heard.image_missing(1), verdict::holds);
    // What was offered may still be read.
    const std::array read{batch{100, a}, batch{102, c}};
    EXPECT_EQ(heard.read_back(1, read, 200, true), verdict::holds);
    EXPECT_EQ(heard.first(), verdict::holds);
}

TEST(EngineLedgerTest, AVerdictHasAWordOfItsOwn) {
    using kwaque::storage::testing::verdict_name;
    EXPECT_EQ(verdict_name(verdict::holds), "holds");
    EXPECT_EQ(verdict_name(verdict::lost), "lost");
    std::vector<std::string_view> words;
    for (std::uint8_t value = 0;
         value <= static_cast<std::uint8_t>(verdict::misuse);
         ++value) {
        const auto word = verdict_name(static_cast<verdict>(value));
        EXPECT_NE(word, "unknown");
        EXPECT_EQ(std::ranges::count(words, word), 0);
        words.push_back(word);
    }
    EXPECT_EQ(words.size(), 11U);
}

TEST(EngineLedgerTest, ALineTheLedgerDoesNotWriteIsNotHeard) {
    for (const auto* text :
         {"",
          "mark 3",
          "created",
          "created x",
          "created 1 2",
          "offer 1 100",
          "offer 1 100 0",
          "offer 1 100 zz",
          "durable 0 4096",
          "create_refused 1 2",
          "crash 1",
          "Created 1"}) {
        engine_ledger heard;
        EXPECT_EQ(heard.hear(text), engine_ledger::line::unknown) << text;
        EXPECT_EQ(heard.first(), verdict::holds) << text;
        EXPECT_TRUE(heard.segments().empty()) << text;
    }
}
} // namespace
