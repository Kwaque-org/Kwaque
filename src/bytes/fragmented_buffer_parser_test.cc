#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_parser.h"

#include <seastar/core/temporary_buffer.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace kwaque::bytes {

namespace {

using fragment_type = seastar::temporary_buffer<char>;

fragment_type fragment_of(std::string_view bytes) {
    fragment_type fragment{bytes.size()};
    std::ranges::copy(bytes, fragment.get_write());
    return fragment;
}

// Splits `bytes` into fragments at the given cut points so a test can place a
// fragment boundary at any byte.
fragmented_buffer
split_at(std::string_view bytes, const std::vector<std::size_t>& cuts) {
    std::vector<fragment_type> fragments;
    std::size_t previous = 0;
    for (const auto cut : cuts) {
        fragments.push_back(
          fragment_of(bytes.substr(previous, cut - previous)));
        previous = cut;
    }
    fragments.push_back(fragment_of(bytes.substr(previous)));
    auto buffer = fragmented_buffer::copy_from_fragments(fragments);
    EXPECT_TRUE(buffer.has_value());
    return std::move(*buffer);
}

// Every way to cut a string of `length` bytes into consecutive fragments,
// including cuts that produce empty fragments.
std::vector<std::vector<std::size_t>> all_cut_sets(std::size_t length) {
    std::vector<std::vector<std::size_t>> sets;
    const auto positions = length + 1;
    for (std::uint32_t mask = 0; mask < (1U << positions); ++mask) {
        std::vector<std::size_t> cuts;
        for (std::size_t position = 0; position < positions; ++position) {
            if ((mask & (1U << position)) != 0) {
                cuts.push_back(position);
            }
        }
        sets.push_back(std::move(cuts));
    }
    return sets;
}

std::string contents(const fragmented_buffer& buffer) {
    std::string out;
    out.resize(buffer.size().value());
    const auto copied = buffer.copy_to(out);
    EXPECT_TRUE(copied.has_value());
    return out;
}

template<fixed_width_unsigned_integer T>
void expect_integer_orders(
  std::string_view bytes, T little_endian, T big_endian) {
    for (std::size_t cut = 0; cut <= bytes.size(); ++cut) {
        fragmented_buffer_parser little_parser{split_at(bytes, {cut})};
        const auto little = little_parser.read_le<T>();
        ASSERT_TRUE(little.has_value()) << "cut=" << cut;
        EXPECT_EQ(*little, little_endian) << "cut=" << cut;
        EXPECT_TRUE(little_parser.at_end()) << "cut=" << cut;

        fragmented_buffer_parser big_parser{split_at(bytes, {cut})};
        const auto big = big_parser.read_be<T>();
        ASSERT_TRUE(big.has_value()) << "cut=" << cut;
        EXPECT_EQ(*big, big_endian) << "cut=" << cut;
        EXPECT_TRUE(big_parser.at_end()) << "cut=" << cut;
    }
}

static_assert(!std::is_copy_constructible_v<fragmented_buffer_parser>);
static_assert(!std::is_copy_assignable_v<fragmented_buffer_parser>);
static_assert(std::is_nothrow_move_constructible_v<fragmented_buffer_parser>);
static_assert(std::is_nothrow_move_assignable_v<fragmented_buffer_parser>);
static_assert(fixed_width_unsigned_integer<std::uint8_t>);
static_assert(fixed_width_unsigned_integer<std::uint16_t>);
static_assert(fixed_width_unsigned_integer<std::uint32_t>);
static_assert(fixed_width_unsigned_integer<std::uint64_t>);
static_assert(!fixed_width_unsigned_integer<bool>);
static_assert(!fixed_width_unsigned_integer<char16_t>);

} // namespace

TEST(BufferParser, TracksPositionAcrossFragments) {
    fragmented_buffer_parser parser{split_at("abcdefgh", {3, 5})};
    EXPECT_EQ(parser.total_bytes().value(), 8U);
    EXPECT_EQ(parser.bytes_consumed().value(), 0U);
    EXPECT_EQ(parser.bytes_remaining().value(), 8U);
    EXPECT_FALSE(parser.at_end());
    EXPECT_EQ(parser.peek_current_fragment().bytes(), "abc");

    std::array<char, 2> two{};
    ASSERT_TRUE(parser.read_to(two).has_value());
    EXPECT_EQ(std::string_view(two.data(), 2), "ab");
    EXPECT_EQ(parser.bytes_consumed().value(), 2U);
    EXPECT_EQ(parser.peek_current_fragment().bytes(), "c");

    // A read that spans a boundary leaves the cursor on the next fragment.
    std::array<char, 3> three{};
    ASSERT_TRUE(parser.read_to(three).has_value());
    EXPECT_EQ(std::string_view(three.data(), 3), "cde");
    EXPECT_EQ(parser.bytes_consumed().value(), 5U);
    EXPECT_EQ(parser.peek_current_fragment().bytes(), "fgh");

    ASSERT_TRUE(parser.skip(byte_count{3}).has_value());
    EXPECT_TRUE(parser.at_end());
    EXPECT_EQ(parser.bytes_remaining().value(), 0U);
    EXPECT_TRUE(parser.peek_current_fragment().empty());
}

TEST(BufferParser, MoveTransfersPositionAndCheckpointsAndClearsSource) {
    fragmented_buffer_parser source{split_at("abcdefgh", {3, 5})};
    ASSERT_TRUE(source.skip(byte_count{2}).has_value());
    ASSERT_TRUE(source.push_checkpoint().has_value());
    ASSERT_TRUE(source.skip(byte_count{2}).has_value());

    auto moved = std::move(source);
    // The parser guarantees a canonical, usable moved-from state.
    // NOLINTBEGIN(bugprone-use-after-move)
    EXPECT_EQ(source.total_bytes().value(), 0U);
    EXPECT_EQ(source.bytes_consumed().value(), 0U);
    EXPECT_EQ(source.bytes_remaining().value(), 0U);
    EXPECT_TRUE(source.at_end());
    EXPECT_TRUE(source.peek_current_fragment().empty());
    EXPECT_EQ(source.checkpoint_depth(), 0U);
    // NOLINTEND(bugprone-use-after-move)

    EXPECT_EQ(moved.total_bytes().value(), 8U);
    EXPECT_EQ(moved.bytes_consumed().value(), 4U);
    EXPECT_EQ(moved.peek_current_fragment().bytes(), "e");
    EXPECT_EQ(moved.checkpoint_depth(), 1U);
    ASSERT_TRUE(moved.rollback().has_value());
    EXPECT_EQ(moved.bytes_consumed().value(), 2U);
    EXPECT_EQ(moved.peek_current_fragment().bytes(), "c");

    ASSERT_TRUE(moved.push_checkpoint().has_value());
    ASSERT_TRUE(moved.skip(byte_count{3}).has_value());
    fragmented_buffer_parser assigned;
    assigned = std::move(moved);
    // Move assignment provides the same canonical source state.
    // NOLINTBEGIN(bugprone-use-after-move)
    EXPECT_EQ(moved.total_bytes().value(), 0U);
    EXPECT_EQ(moved.bytes_consumed().value(), 0U);
    EXPECT_TRUE(moved.at_end());
    EXPECT_TRUE(moved.peek_current_fragment().empty());
    EXPECT_EQ(moved.checkpoint_depth(), 0U);
    // NOLINTEND(bugprone-use-after-move)

    EXPECT_EQ(assigned.total_bytes().value(), 8U);
    EXPECT_EQ(assigned.bytes_consumed().value(), 5U);
    EXPECT_EQ(assigned.peek_current_fragment().bytes(), "fgh");
    EXPECT_EQ(assigned.checkpoint_depth(), 1U);
    ASSERT_TRUE(assigned.rollback().has_value());
    EXPECT_EQ(assigned.bytes_consumed().value(), 2U);
}

TEST(BufferParser, FailedReadsLeaveTheCursorUntouched) {
    fragmented_buffer_parser parser{split_at("abcdef", {2, 4})};
    ASSERT_TRUE(parser.skip(byte_count{4}).has_value());
    EXPECT_EQ(parser.bytes_consumed().value(), 4U);

    // Overrun by exactly one byte.
    std::array<char, 3> three{};
    const auto truncated = parser.read_to(three);
    ASSERT_FALSE(truncated.has_value());
    EXPECT_EQ(truncated.error(), make_error_code(errc::truncated_data));
    EXPECT_EQ(parser.bytes_consumed().value(), 4U);

    const auto skipped = parser.skip(byte_count{3});
    ASSERT_FALSE(skipped.has_value());
    EXPECT_EQ(skipped.error(), make_error_code(errc::out_of_range));
    EXPECT_EQ(parser.bytes_consumed().value(), 4U);

    const auto shared = parser.read_buffer(byte_count{3});
    ASSERT_FALSE(shared.has_value());
    EXPECT_EQ(shared.error(), make_error_code(errc::truncated_data));
    EXPECT_EQ(parser.bytes_consumed().value(), 4U);

    // A failed peek never advances either.
    const auto peeked = parser.peek_to(three);
    ASSERT_FALSE(peeked.has_value());
    EXPECT_EQ(peeked.error(), make_error_code(errc::truncated_data));
    EXPECT_EQ(parser.bytes_consumed().value(), 4U);

    // The exact remainder still reads.
    std::array<char, 2> two{};
    ASSERT_TRUE(parser.read_to(two).has_value());
    EXPECT_EQ(std::string_view(two.data(), 2), "ef");
    EXPECT_TRUE(parser.at_end());
}

TEST(BufferParser, PeekDoesNotAdvanceAndReadDoes) {
    fragmented_buffer_parser parser{split_at("abcdef", {1, 3})};
    std::array<char, 4> peeked{};
    ASSERT_TRUE(parser.peek_to(peeked).has_value());
    EXPECT_EQ(std::string_view(peeked.data(), 4), "abcd");
    EXPECT_EQ(parser.bytes_consumed().value(), 0U);

    auto looked = parser.peek_buffer(byte_count{4});
    ASSERT_TRUE(looked.has_value());
    EXPECT_EQ(contents(*looked), "abcd");
    EXPECT_EQ(parser.bytes_consumed().value(), 0U);

    auto taken = parser.read_buffer(byte_count{4});
    ASSERT_TRUE(taken.has_value());
    EXPECT_EQ(contents(*taken), "abcd");
    EXPECT_EQ(parser.bytes_consumed().value(), 4U);

    // A zero-length request is legal and yields the canonical empty buffer.
    auto nothing = parser.read_buffer(byte_count{0});
    ASSERT_TRUE(nothing.has_value());
    EXPECT_TRUE(nothing->empty());
    EXPECT_EQ(parser.bytes_consumed().value(), 4U);
}

TEST(BufferParser, SubBuffersOutliveParserAndSource) {
    fragmented_buffer slice;
    {
        auto source = split_at("abcdefghij", {2, 6});
        auto shared = source.share();
        fragmented_buffer_parser parser{std::move(shared)};
        ASSERT_TRUE(parser.skip(byte_count{1}).has_value());
        auto taken = parser.read_buffer(byte_count{7});
        ASSERT_TRUE(taken.has_value());
        slice = std::move(*taken);
        // Both the parser and the buffer it came from go out of scope here.
    }
    EXPECT_EQ(contents(slice), "bcdefgh");
}

TEST(BufferParser, FixedWidthEndianReadsAtEveryBoundary) {
    const std::string bytes{"\x01\x02\x03\x04\x05\x06\x07\x08", 8};
    for (std::size_t cut = 0; cut <= 8; ++cut) {
        fragmented_buffer_parser parser{split_at(bytes, {cut})};

        const auto u8 = parser.read_le<std::uint8_t>();
        ASSERT_TRUE(u8.has_value()) << "cut=" << cut;
        EXPECT_EQ(*u8, 0x01U);

        const auto u16 = parser.read_be<std::uint16_t>();
        ASSERT_TRUE(u16.has_value()) << "cut=" << cut;
        EXPECT_EQ(*u16, 0x0203U);

        const auto u32 = parser.read_le<std::uint32_t>();
        ASSERT_TRUE(u32.has_value()) << "cut=" << cut;
        EXPECT_EQ(*u32, 0x07060504U);

        const auto tail = parser.read_le<std::uint8_t>();
        ASSERT_TRUE(tail.has_value()) << "cut=" << cut;
        EXPECT_EQ(*tail, 0x08U);
        EXPECT_TRUE(parser.at_end());
    }

    // Both orders over the full width, and truncation past the end.
    {
        fragmented_buffer_parser parser{split_at(bytes, {1, 3, 5, 7})};
        const auto be = parser.read_be<std::uint64_t>();
        ASSERT_TRUE(be.has_value());
        EXPECT_EQ(*be, 0x0102030405060708ULL);
        EXPECT_FALSE(parser.read_le<std::uint8_t>().has_value());
    }
    {
        fragmented_buffer_parser parser{split_at(bytes, {1, 3, 5, 7})};
        const auto le = parser.read_le<std::uint64_t>();
        ASSERT_TRUE(le.has_value());
        EXPECT_EQ(*le, 0x0807060504030201ULL);
    }
    {
        fragmented_buffer_parser parser{split_at(bytes.substr(0, 3), {1})};
        const auto truncated = parser.read_be<std::uint32_t>();
        ASSERT_FALSE(truncated.has_value());
        EXPECT_EQ(truncated.error(), make_error_code(errc::truncated_data));
        EXPECT_EQ(parser.bytes_consumed().value(), 0U);
    }
}

TEST(BufferParser, EveryFixedWidthSupportsBothByteOrders) {
    expect_integer_orders<std::uint8_t>(
      std::string_view{"\x01", 1}, 0x01U, 0x01U);
    expect_integer_orders<std::uint16_t>(
      std::string_view{"\x01\x02", 2}, 0x0201U, 0x0102U);
    expect_integer_orders<std::uint32_t>(
      std::string_view{"\x01\x02\x03\x04", 4}, 0x04030201U, 0x01020304U);
    expect_integer_orders<std::uint64_t>(
      std::string_view{"\x01\x02\x03\x04\x05\x06\x07\x08", 8},
      0x0807060504030201ULL,
      0x0102030405060708ULL);
}

TEST(BufferParser, CheckpointsRollBackAndCommitWithinTheirBound) {
    fragmented_buffer_parser parser{split_at("abcdefgh", {3, 5})};

    ASSERT_TRUE(parser.push_checkpoint().has_value());
    EXPECT_EQ(parser.checkpoint_depth(), 1U);
    ASSERT_TRUE(parser.skip(byte_count{4}).has_value());
    EXPECT_EQ(parser.bytes_consumed().value(), 4U);
    ASSERT_TRUE(parser.rollback().has_value());
    EXPECT_EQ(parser.checkpoint_depth(), 0U);
    EXPECT_EQ(parser.bytes_consumed().value(), 0U);
    EXPECT_EQ(parser.peek_current_fragment().bytes(), "abc");

    ASSERT_TRUE(parser.push_checkpoint().has_value());
    ASSERT_TRUE(parser.skip(byte_count{4}).has_value());
    ASSERT_TRUE(parser.commit().has_value());
    EXPECT_EQ(parser.checkpoint_depth(), 0U);
    EXPECT_EQ(parser.bytes_consumed().value(), 4U);

    // Nested rollback restores the innermost mark, then the outer one.
    ASSERT_TRUE(parser.push_checkpoint().has_value());
    ASSERT_TRUE(parser.skip(byte_count{1}).has_value());
    ASSERT_TRUE(parser.push_checkpoint().has_value());
    ASSERT_TRUE(parser.skip(byte_count{1}).has_value());
    EXPECT_EQ(parser.bytes_consumed().value(), 6U);
    ASSERT_TRUE(parser.rollback().has_value());
    EXPECT_EQ(parser.bytes_consumed().value(), 5U);
    ASSERT_TRUE(parser.rollback().has_value());
    EXPECT_EQ(parser.bytes_consumed().value(), 4U);

    EXPECT_FALSE(parser.rollback().has_value());
    EXPECT_FALSE(parser.commit().has_value());

    // The stack is hard-bounded.
    for (std::size_t depth = 0; depth < max_parser_checkpoints; ++depth) {
        ASSERT_TRUE(parser.push_checkpoint().has_value());
    }
    const auto overflowed = parser.push_checkpoint();
    ASSERT_FALSE(overflowed.has_value());
    EXPECT_EQ(overflowed.error(), make_error_code(errc::resource_exhausted));
    EXPECT_EQ(parser.checkpoint_depth(), max_parser_checkpoints);
    for (std::size_t depth = 0; depth < max_parser_checkpoints; ++depth) {
        ASSERT_TRUE(parser.commit().has_value());
    }
}

// Compares every parser operation against a contiguous oracle for every
// fragmentation of an eight-byte payload, including empty fragments and cursors
// that land exactly on the end.
TEST(BufferParser, ExhaustiveFragmentationMatchesContiguousOracle) {
    constexpr std::size_t length = 8;
    const std::string oracle = "ABCDEFGH";
    ASSERT_EQ(oracle.size(), length);

    std::size_t compositions = 0;
    for (const auto& cuts : all_cut_sets(length)) {
        auto buffer = split_at(oracle, cuts);
        ASSERT_EQ(buffer.size().value(), length);
        ASSERT_EQ(contents(buffer), oracle);
        ++compositions;

        for (std::size_t start = 0; start <= length; ++start) {
            for (std::size_t span = 0; span + start <= length; ++span) {
                auto shared = buffer.share();
                fragmented_buffer_parser parser{std::move(shared)};
                ASSERT_TRUE(parser.skip(byte_count{start}).has_value());

                std::vector<char> destination(span);
                ASSERT_TRUE(parser.peek_to(destination).has_value());
                EXPECT_EQ(
                  std::string(destination.begin(), destination.end()),
                  oracle.substr(start, span));
                EXPECT_EQ(parser.bytes_consumed().value(), start);

                auto sliced = parser.read_buffer(byte_count{span});
                ASSERT_TRUE(sliced.has_value());
                EXPECT_EQ(contents(*sliced), oracle.substr(start, span));
                EXPECT_EQ(parser.bytes_consumed().value(), start + span);
                EXPECT_EQ(
                  parser.bytes_remaining().value(), length - start - span);

                // Overrun by exactly one from wherever the cursor now sits.
                std::vector<char> overrun(length - start - span + 1);
                const auto truncated = parser.read_to(overrun);
                ASSERT_FALSE(truncated.has_value());
                EXPECT_EQ(
                  truncated.error(), make_error_code(errc::truncated_data));
                EXPECT_EQ(parser.bytes_consumed().value(), start + span);
            }
        }
    }
    // 2^(length+1) compositions, so the matrix really is exhaustive.
    EXPECT_EQ(compositions, std::size_t{1} << (length + 1));
}

TEST(BufferParser, EmptyAndExhaustedInputsBehaveConsistently) {
    fragmented_buffer_parser empty;
    EXPECT_EQ(empty.total_bytes().value(), 0U);
    EXPECT_TRUE(empty.at_end());
    EXPECT_TRUE(empty.peek_current_fragment().empty());
    EXPECT_TRUE(empty.skip(byte_count{0}).has_value());
    EXPECT_FALSE(empty.skip(byte_count{1}).has_value());
    const auto empty_read = empty.read_le<std::uint8_t>();
    ASSERT_FALSE(empty_read.has_value());
    EXPECT_EQ(empty_read.error(), make_error_code(errc::truncated_data));
    auto nothing = empty.read_buffer(byte_count{0});
    ASSERT_TRUE(nothing.has_value());
    EXPECT_TRUE(nothing->empty());

    // A buffer made only of empty fragments parses as empty.
    fragmented_buffer_parser sparse{split_at("", {0, 0, 0})};
    EXPECT_EQ(sparse.total_bytes().value(), 0U);
    EXPECT_TRUE(sparse.at_end());
    const auto sparse_read = sparse.read_le<std::uint8_t>();
    ASSERT_FALSE(sparse_read.has_value());
    EXPECT_EQ(sparse_read.error(), make_error_code(errc::truncated_data));
}

} // namespace kwaque::bytes
