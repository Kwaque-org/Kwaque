#include "src/storage/tests/choice_stack.h"
#include "src/storage/tests/engine_history.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <set>
#include <stdexcept>
#include <vector>

namespace {
using namespace kwaque::storage::testing;

engine_history sample() {
    return {
      {history_survival::names, 5},
      {{history_op::create, 0, 0, 0},
       {history_op::append, 1, 2, 0x80},
       {history_op::settle, 0, 0, 0},
       {history_op::crash, 0, 0, 0}}};
}

TEST(EngineHistoryTest, AHistoryIsItsBytes) {
    const auto history = sample();
    const auto bytes = encode_history(history);
    ASSERT_EQ(bytes.size(), history_header_bytes + 4 * history_operation_bytes);
    EXPECT_EQ(decode_history(bytes), history);
}

TEST(EngineHistoryTest, EveryByteStringIsAHistory) {
    std::vector<std::uint8_t> bytes;
    for (std::size_t length = 0; length != 64; ++length) {
        const auto decoded = decode_history(bytes);
        const auto whole = length > history_header_bytes
                             ? (length - history_header_bytes)
                                 / history_operation_bytes
                             : 0;
        EXPECT_EQ(decoded.operations.size(), whole);
        for (const auto& operation : decoded.operations)
            EXPECT_LT(
              static_cast<std::uint8_t>(operation.op), history_op_count);
        EXPECT_LT(static_cast<std::uint8_t>(decoded.header.survival), 4U);
        // What it decodes to encodes to a history that decodes the same.
        EXPECT_EQ(decode_history(encode_history(decoded)), decoded);
        bytes.push_back(static_cast<std::uint8_t>(251U * (length + 1U)));
    }
    // Bytes that are missing are zero.
    EXPECT_EQ(decode_history({}), engine_history{});
}

TEST(EngineHistoryTest, TakingBytesAwayTakesOperationsAway) {
    const auto history = sample();
    const auto bytes = encode_history(history);
    for (std::size_t gone = 0; gone != history.operations.size(); ++gone) {
        auto shorter = bytes;
        const auto from = shorter.begin()
                          + static_cast<std::ptrdiff_t>(
                            history_header_bytes
                            + gone * history_operation_bytes);
        shorter.erase(
          from, from + static_cast<std::ptrdiff_t>(history_operation_bytes));
        auto expected = history;
        expected.operations.erase(
          expected.operations.begin() + static_cast<std::ptrdiff_t>(gone));
        EXPECT_EQ(decode_history(shorter), expected);
    }
    // The end cut anywhere inside an operation drops that operation alone.
    for (std::size_t cut = 1; cut != history_operation_bytes; ++cut) {
        auto torn = bytes;
        torn.resize(torn.size() - cut);
        auto expected = history;
        expected.operations.pop_back();
        EXPECT_EQ(decode_history(torn), expected);
    }
}

TEST(EngineHistoryTest, NoMoreOperationsThanTheCap) {
    const std::vector<std::uint8_t> bytes(
      history_header_bytes
        + (maximum_history_operations + 7) * history_operation_bytes,
      std::uint8_t{3});
    const auto decoded = decode_history(bytes);
    EXPECT_EQ(decoded.operations.size(), maximum_history_operations);
    EXPECT_EQ(decoded.operations.back().op, history_op::create);
}

TEST(ChoiceStackTest, EverySequenceOfChoicesIsMadeOnce) {
    choice_stack choices;
    std::set<std::vector<int>> seen;
    std::size_t runs = 0;
    do {
        const int first = choices.choose_up_to(2);
        // How many choices follow depends on the one before.
        if (first != 1) static_cast<void>(choices.choose_bool());
        EXPECT_TRUE(seen.insert(choices.answers()).second);
        ++runs;
    } while (choices.rewind());
    // Two answers after 0 and after 2, none after 1.
    EXPECT_EQ(runs, 5U);
    EXPECT_TRUE(seen.contains({0, 0}));
    EXPECT_TRUE(seen.contains({0, 1}));
    EXPECT_TRUE(seen.contains({1}));
    EXPECT_TRUE(seen.contains({2, 0}));
    EXPECT_TRUE(seen.contains({2, 1}));
}

TEST(ChoiceStackTest, ARunWithNoChoiceIsMadeOnce) {
    choice_stack choices;
    std::size_t runs = 0;
    do {
        ++runs;
    } while (choices.rewind());
    EXPECT_EQ(runs, 1U);
    EXPECT_THROW(choices.mark_last_choice(), std::logic_error);
}

TEST(ChoiceStackTest, AChoiceOfUnknownWidthEndsWhereItIsMarked) {
    choice_stack choices;
    std::vector<int> seen;
    do {
        const int value = choices.choose();
        seen.push_back(value);
        if (value == 3) choices.mark_last_choice();
    } while (choices.rewind());
    EXPECT_EQ(seen, (std::vector<int>{0, 1, 2, 3}));
}
} // namespace
