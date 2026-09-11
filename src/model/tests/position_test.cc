#include "src/base/error.h"
#include "src/base/units.h"
#include "src/model/position.h"
#include "src/runtime/file_position.h"

#include <gtest/gtest.h>

#include <array>
#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <tuple>
#include <type_traits>
#include <utility>

namespace {

using kwaque::byte_count;
using kwaque::item_count;
using kwaque::model::file_byte_span;
using kwaque::model::range_logical_count;
using kwaque::model::range_logical_end;
using kwaque::model::range_logical_offset;
using kwaque::model::range_logical_span;
using kwaque::model::segment_record_count;
using kwaque::model::segment_relative_end;
using kwaque::model::segment_relative_offset;
using kwaque::model::segment_relative_span;
using kwaque::runtime::file_position;

constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();

template<typename Left, typename Right>
concept comparison_expression
  = requires(const Left& lhs, const Right& rhs) { lhs == rhs; }
    || requires(const Left& lhs, const Right& rhs) { lhs != rhs; }
    || requires(const Left& lhs, const Right& rhs) { lhs < rhs; }
    || requires(const Left& lhs, const Right& rhs) { lhs <= rhs; }
    || requires(const Left& lhs, const Right& rhs) { lhs > rhs; }
    || requires(const Left& lhs, const Right& rhs) { lhs >= rhs; }
    || requires(const Left& lhs, const Right& rhs) { lhs <=> rhs; };

template<typename Value>
concept ordering_expression
  = requires(const Value& lhs, const Value& rhs) { lhs < rhs; }
    || requires(const Value& lhs, const Value& rhs) { lhs <= rhs; }
    || requires(const Value& lhs, const Value& rhs) { lhs > rhs; }
    || requires(const Value& lhs, const Value& rhs) { lhs >= rhs; }
    || requires(const Value& lhs, const Value& rhs) { lhs <=> rhs; };

template<typename Left, typename Right>
concept unchecked_arithmetic_expression = requires(Left lhs, const Right& rhs) {
    lhs + rhs;
} || requires(Left lhs, const Right& rhs) {
    lhs - rhs;
} || requires(Left lhs, const Right& rhs) {
    lhs += rhs;
} || requires(Left lhs, const Right& rhs) { lhs -= rhs; };

template<typename Value>
concept increment_expression = requires(Value value) { ++value; }
                               || requires(Value value) { value++; }
                               || requires(Value value) { --value; }
                               || requires(Value value) { value--; };

template<typename Left, typename Right>
concept checked_add_expression = requires(const Left& lhs, const Right& rhs) {
    lhs.checked_add(rhs);
};

template<typename Left, typename Right>
concept checked_sub_expression = requires(const Left& lhs, const Right& rhs) {
    lhs.checked_sub(rhs);
};

template<typename Value>
inline constexpr bool count_type = std::same_as<Value, range_logical_count>
                                   || std::same_as<Value, segment_record_count>
                                   || std::same_as<Value, byte_count>
                                   || std::same_as<Value, item_count>;

template<typename Left, typename Right>
consteval bool position_pair_contract() {
    constexpr bool allowed_count
      = (count_type<Left> && std::same_as<Left, Right>)
        || ((std::same_as<Left, range_logical_offset> || std::same_as<Left, range_logical_end>) && std::same_as<Right, range_logical_count>)
        || ((std::same_as<Left, segment_relative_offset> || std::same_as<Left, segment_relative_end>) && std::same_as<Right, segment_record_count>)
        || (std::same_as<Left, file_position> && std::same_as<Right, byte_count>);
    constexpr bool arithmetic_matches
      = checked_add_expression<Left, Right> == allowed_count
        && checked_sub_expression<Left, Right>
             == (allowed_count && !std::same_as<Left, file_position>)
        && !unchecked_arithmetic_expression<Left, Right>;
    if constexpr (std::same_as<Left, Right>) {
        return arithmetic_matches&&
        requires(const Left & lhs, const Right & rhs)
        {
            {lhs == rhs}->std::same_as<bool>;
            {lhs <=> rhs}->std::same_as<std::strong_ordering>;
        };
    } else {
        return arithmetic_matches && !comparison_expression<Left, Right>
               && !std::constructible_from<Left, Right>
               && !std::constructible_from<Left, const Right&>
               && !std::convertible_to<Left, Right>;
    }
}

template<typename Left, typename... Right>
consteval bool position_peer_contract(std::tuple<Right...>*) {
    constexpr auto occurrences = (std::size_t{std::same_as<Left, Right>} + ...);
    return occurrences == 1U && (position_pair_contract<Left, Right>() && ...)
           && !std::convertible_to<Left, std::uint64_t>
           && !std::convertible_to<std::uint64_t, Left>
           && !comparison_expression<Left, std::uint64_t>
           && !comparison_expression<std::uint64_t, Left>
           && !checked_add_expression<Left, std::uint64_t>
           && !checked_sub_expression<Left, std::uint64_t>
           && !unchecked_arithmetic_expression<Left, std::uint64_t>
           && !unchecked_arithmetic_expression<std::uint64_t, Left>
           && !increment_expression<Left>;
}

template<typename... Value>
consteval bool position_type_contract(std::tuple<Value...>* types) {
    return (position_peer_contract<Value>(types) && ...);
}

using position_types = std::tuple<
  range_logical_count,
  segment_record_count,
  byte_count,
  item_count,
  range_logical_offset,
  range_logical_end,
  segment_relative_offset,
  segment_relative_end,
  file_position>;
static_assert(position_type_contract(static_cast<position_types*>(nullptr)));

template<typename Span, typename Point>
concept contains_expression = requires(const Span& span, Point point) {
    span.contains(point);
};

template<typename Span, typename End, typename Count>
concept from_count_expression = requires(End begin, Count count) {
    Span::from_count(begin, count);
};

template<typename Span, typename End, typename Count>
concept from_size_expression = requires(End begin, Count count) {
    Span::from_size(begin, count);
};

static_assert(
  !comparison_expression<range_logical_span, segment_relative_span>);
static_assert(!comparison_expression<range_logical_span, file_byte_span>);
static_assert(!comparison_expression<segment_relative_span, file_byte_span>);
static_assert(!ordering_expression<range_logical_span>);
static_assert(!ordering_expression<segment_relative_span>);
static_assert(!ordering_expression<file_byte_span>);
static_assert(!contains_expression<range_logical_span, range_logical_end>);
static_assert(
  !contains_expression<range_logical_span, segment_relative_offset>);
static_assert(
  !contains_expression<segment_relative_span, segment_relative_end>);
static_assert(
  !contains_expression<segment_relative_span, range_logical_offset>);
static_assert(!contains_expression<file_byte_span, range_logical_offset>);
static_assert(!contains_expression<file_byte_span, segment_relative_offset>);
static_assert(!from_count_expression<
              range_logical_span,
              range_logical_offset,
              range_logical_count>);
static_assert(!from_count_expression<
              range_logical_span,
              range_logical_end,
              segment_record_count>);
static_assert(
  !from_count_expression<range_logical_span, range_logical_end, byte_count>);
static_assert(!from_count_expression<
              segment_relative_span,
              segment_relative_offset,
              segment_record_count>);
static_assert(!from_count_expression<
              segment_relative_span,
              segment_relative_end,
              range_logical_count>);
static_assert(!from_count_expression<
              segment_relative_span,
              segment_relative_end,
              byte_count>);
static_assert(
  !from_size_expression<file_byte_span, file_position, range_logical_count>);
static_assert(
  !from_size_expression<file_byte_span, file_position, segment_record_count>);

struct logical_domain {
    using offset = range_logical_offset;
    using end = range_logical_end;
    using count = range_logical_count;
    using span = range_logical_span;
};

struct physical_domain {
    using offset = segment_relative_offset;
    using end = segment_relative_end;
    using count = segment_record_count;
    using span = segment_relative_span;
};

template<typename Value>
consteval bool compact_value_contract(std::size_t bytes) {
    return sizeof(Value) == bytes && std::is_standard_layout_v<Value>
           && std::is_trivially_copyable_v<Value>
           && std::is_nothrow_copy_constructible_v<Value>
           && std::is_nothrow_copy_assignable_v<Value>
           && std::is_nothrow_move_constructible_v<Value>
           && std::is_nothrow_move_assignable_v<Value>
           && std::is_nothrow_destructible_v<Value>;
}

template<typename Domain>
class RecordPositionTest : public ::testing::Test {
    using Offset = Domain::offset;
    using End = Domain::end;
    using Count = Domain::count;
    using Span = Domain::span;

    static_assert(compact_value_contract<Offset>(8));
    static_assert(compact_value_contract<End>(8));
    static_assert(compact_value_contract<Count>(8));
    static_assert(compact_value_contract<Span>(16));
    static_assert(std::is_nothrow_default_constructible_v<Offset>);
    static_assert(std::is_nothrow_default_constructible_v<End>);
    static_assert(std::is_nothrow_default_constructible_v<Count>);
    static_assert(!std::constructible_from<Offset, std::uint64_t>);
    static_assert(std::is_nothrow_constructible_v<End, std::uint64_t>);
    static_assert(!std::default_initializable<Span>);
    static_assert(!std::constructible_from<Span, End, End>);
    static_assert(
      std::same_as<decltype(std::declval<const Span&>().begin()), End>);
    static_assert(std::same_as<decltype(std::declval<Span>().end()), End>);
    static_assert(
      std::same_as<decltype(std::declval<const Span>().count()), Count>);
    static_assert(
      std::same_as<decltype(std::declval<const Offset&>().as_end()), End>);
    static_assert(
      std::same_as<decltype(std::declval<const Offset&>().end_after()), End>);
    static_assert(std::same_as<
                  decltype(std::declval<const Offset&>().checked_add(Count{})),
                  std::optional<Offset>>);
    static_assert(std::same_as<
                  decltype(std::declval<const End&>().checked_sub(Count{})),
                  std::optional<End>>);
    static_assert(noexcept(Offset::make(0)));
    static_assert(noexcept(Span::make(End{}, End{})));
    static_assert(noexcept(Span::from_count(End{}, Count{})));
    static_assert(End{maximum}.value() == maximum);
    static_assert(!End{maximum}.checked_add(Count{1}).has_value());
    static_assert(!End{}.checked_sub(Count{1}).has_value());
    static_assert(End{maximum}.checked_add(Count{})->value() == maximum);
    static_assert(End{}.checked_add(Count{maximum})->value() == maximum);
};

using record_domains = ::testing::Types<logical_domain, physical_domain>;
TYPED_TEST_SUITE(RecordPositionTest, record_domains);

TYPED_TEST(RecordPositionTest, DistinguishesActualRecordsFromExclusiveEnds) {
    using Offset = TypeParam::offset;
    using End = TypeParam::end;
    EXPECT_EQ(Offset{}.value(), 0U);
    for (const auto raw : {std::uint64_t{0}, std::uint64_t{1}, maximum - 1U}) {
        const auto point = Offset::make(raw);
        ASSERT_TRUE(point.has_value()) << raw;
        EXPECT_EQ(point->value(), raw);
        EXPECT_EQ(point->as_end(), End{raw});
        EXPECT_EQ(point->end_after(), End{raw + 1U});
    }
    const auto invalid = Offset::make(maximum);
    ASSERT_FALSE(invalid.has_value());
    EXPECT_EQ(invalid.error(), kwaque::errc::out_of_range);
    EXPECT_EQ(End{maximum}.value(), maximum);
}

TYPED_TEST(RecordPositionTest, PointArithmeticRejectsNonRecordCoordinates) {
    using Offset = TypeParam::offset;
    using Count = TypeParam::count;
    const Offset zero;
    const auto first = zero.checked_add(Count{1});
    const auto last = zero.checked_add(Count{maximum - 1U});
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(last.has_value());
    EXPECT_EQ(first->value(), 1U);
    EXPECT_EQ(last->value(), maximum - 1U);
    EXPECT_EQ(zero.value(), 0U);
    EXPECT_FALSE(zero.checked_sub(Count{1}).has_value());
    EXPECT_FALSE(zero.checked_add(Count{maximum}).has_value());
    EXPECT_FALSE(last->checked_add(Count{1}).has_value());
    EXPECT_FALSE(last->checked_add(Count{2}).has_value());
    EXPECT_FALSE(last->checked_sub(Count{maximum}).has_value());
    const auto unchanged = last->checked_add(Count{});
    const auto back_to_zero = last->checked_sub(Count{maximum - 1U});
    ASSERT_TRUE(unchanged.has_value());
    ASSERT_TRUE(back_to_zero.has_value());
    EXPECT_EQ(*unchanged, *last);
    EXPECT_EQ(*back_to_zero, zero);
    EXPECT_EQ(last->value(), maximum - 1U);
}

TYPED_TEST(
  RecordPositionTest, BoundaryArithmeticPreservesTheFullUnsignedDomain) {
    using End = TypeParam::end;
    using Count = TypeParam::count;
    const End last{maximum};
    const auto unchanged = last.checked_add(Count{});
    const auto from_zero = End{}.checked_add(Count{maximum});
    const auto previous = last.checked_sub(Count{1});
    const auto to_zero = last.checked_sub(Count{maximum});
    ASSERT_TRUE(unchanged.has_value());
    ASSERT_TRUE(from_zero.has_value());
    ASSERT_TRUE(previous.has_value());
    ASSERT_TRUE(to_zero.has_value());
    EXPECT_EQ(*unchanged, last);
    EXPECT_EQ(*from_zero, last);
    EXPECT_EQ(previous->value(), maximum - 1U);
    EXPECT_EQ(to_zero->value(), 0U);
    EXPECT_FALSE(last.checked_add(Count{1}).has_value());
    EXPECT_FALSE(End{}.checked_sub(Count{1}).has_value());
    EXPECT_EQ(last.value(), maximum);
    EXPECT_LT(End{1}, End{256});
    EXPECT_LT(
      End{(std::uint64_t{1} << 63U) - 1U}, End{std::uint64_t{1} << 63U});
}

TYPED_TEST(RecordPositionTest, AcceptsEmptyTerminalAndFullDomainSpans) {
    using Offset = TypeParam::offset;
    using End = TypeParam::end;
    using Count = TypeParam::count;
    using Span = TypeParam::span;
    struct span_case {
        std::uint64_t begin;
        std::uint64_t end;
        std::uint64_t count;
        bool contains_zero;
        bool contains_last;
    };
    const std::array cases{
      span_case{0, 0, 0, false, false},
      span_case{maximum, maximum, 0, false, false},
      span_case{maximum - 1U, maximum, 1, false, true},
      span_case{0, maximum, maximum, true, true}};
    const auto last_record = Offset::make(maximum - 1U);
    ASSERT_TRUE(last_record.has_value());
    for (const auto& input : cases) {
        const auto span = Span::make(End{input.begin}, End{input.end});
        const auto from_count = Span::from_count(
          End{input.begin}, Count{input.count});
        ASSERT_TRUE(span.has_value());
        ASSERT_TRUE(from_count.has_value());
        EXPECT_EQ(*span, *from_count);
        EXPECT_EQ(span->begin(), End{input.begin});
        EXPECT_EQ(span->end(), End{input.end});
        EXPECT_EQ(span->count(), Count{input.count});
        EXPECT_EQ(span->empty(), input.count == 0U);
        EXPECT_EQ(span->contains(Offset{}), input.contains_zero);
        EXPECT_EQ(span->contains(*last_record), input.contains_last);
    }
}

TYPED_TEST(
  RecordPositionTest, RejectsReversedSpansAndOverflowBeforePublication) {
    using End = TypeParam::end;
    using Count = TypeParam::count;
    using Span = TypeParam::span;
    for (const auto& [begin, end] :
         {std::pair{std::uint64_t{1}, std::uint64_t{0}},
          std::pair{maximum, maximum - 1U}}) {
        const auto reversed = Span::make(End{begin}, End{end});
        ASSERT_FALSE(reversed.has_value());
        EXPECT_EQ(reversed.error(), kwaque::errc::invalid_argument);
    }
    for (const auto& [begin, count] :
         {std::pair{maximum, std::uint64_t{1}},
          std::pair{std::uint64_t{1}, maximum},
          std::pair{maximum - 1U, std::uint64_t{2}}}) {
        const auto overflow = Span::from_count(End{begin}, Count{count});
        ASSERT_FALSE(overflow.has_value());
        EXPECT_EQ(overflow.error(), kwaque::errc::out_of_range);
    }
}

TYPED_TEST(
  RecordPositionTest, ContainsOnlyHalfOpenRecordsAndReturnsOwnedEndpoints) {
    using Offset = TypeParam::offset;
    using End = TypeParam::end;
    using Span = TypeParam::span;
    const auto span = Span::make(End{4}, End{7});
    ASSERT_TRUE(span.has_value());
    for (const auto& [raw, contained] :
         {std::pair{4U, true},
          std::pair{6U, true},
          std::pair{3U, false},
          std::pair{7U, false}}) {
        const auto point = Offset::make(raw);
        ASSERT_TRUE(point.has_value());
        EXPECT_EQ(span->contains(*point), contained) << raw;
    }
    const auto begin = Span{*span}.begin();
    const auto end = decltype(span){*span}->end();
    EXPECT_EQ(begin, End{4});
    EXPECT_EQ(end, End{7});
    const auto wider = Span::make(End{4}, End{8});
    ASSERT_TRUE(wider.has_value());
    EXPECT_NE(*span, *wider);
}

static_assert(compact_value_contract<file_byte_span>(16));
static_assert(!std::default_initializable<file_byte_span>);
static_assert(
  !std::constructible_from<file_byte_span, file_position, file_position>);
static_assert(std::same_as<
              decltype(std::declval<const file_byte_span&>().begin()),
              file_position>);
static_assert(
  std::same_as<decltype(std::declval<file_byte_span>().end()), file_position>);
static_assert(std::same_as<
              decltype(std::declval<const file_byte_span>().size()),
              byte_count>);
static_assert(
  file_position{maximum}.checked_add(byte_count{})->value() == maximum);
static_assert(!file_position{maximum}.checked_add(byte_count{1}).has_value());

TEST(FileByteSpanTest, PreservesExistingPositionDomainAndExactByteLengths) {
    struct span_case {
        std::uint64_t begin;
        std::uint64_t end;
        std::uint64_t bytes;
    };
    const std::array cases{
      span_case{0, 0, 0},
      span_case{maximum, maximum, 0},
      span_case{maximum - 1U, maximum, 1},
      span_case{0, maximum, maximum},
      span_case{4, 7, 3}};
    for (const auto& input : cases) {
        const auto span = file_byte_span::make(
          file_position{input.begin}, file_position{input.end});
        const auto from_size = file_byte_span::from_size(
          file_position{input.begin}, byte_count{input.bytes});
        ASSERT_TRUE(span.has_value());
        ASSERT_TRUE(from_size.has_value());
        EXPECT_EQ(*span, *from_size);
        EXPECT_EQ(span->begin(), file_position{input.begin});
        EXPECT_EQ(span->end(), file_position{input.end});
        EXPECT_EQ(span->size(), byte_count{input.bytes});
        EXPECT_EQ(span->empty(), input.bytes == 0U);
        EXPECT_FALSE(span->contains(file_position{input.end}));
        EXPECT_EQ(
          span->contains(file_position{input.begin}), input.bytes != 0U);
    }
}

TEST(FileByteSpanTest, RejectsReversalAndOverflowAndExcludesTheEnd) {
    const auto reversed = file_byte_span::make(
      file_position{1}, file_position{0});
    ASSERT_FALSE(reversed.has_value());
    EXPECT_EQ(reversed.error(), kwaque::errc::invalid_argument);
    for (const auto& [begin, bytes] :
         {std::pair{maximum, std::uint64_t{1}},
          std::pair{std::uint64_t{1}, maximum},
          std::pair{maximum - 1U, std::uint64_t{2}}}) {
        const auto overflow = file_byte_span::from_size(
          file_position{begin}, byte_count{bytes});
        ASSERT_FALSE(overflow.has_value());
        EXPECT_EQ(overflow.error(), kwaque::errc::out_of_range);
    }
    const auto span = file_byte_span::make(file_position{4}, file_position{7});
    ASSERT_TRUE(span.has_value());
    EXPECT_FALSE(span->contains(file_position{3}));
    EXPECT_TRUE(span->contains(file_position{4}));
    EXPECT_TRUE(span->contains(file_position{6}));
    EXPECT_FALSE(span->contains(file_position{7}));
    const auto begin = file_byte_span{*span}.begin();
    const auto end = decltype(span){*span}->end();
    EXPECT_EQ(begin, file_position{4});
    EXPECT_EQ(end, file_position{7});
}

} // namespace
