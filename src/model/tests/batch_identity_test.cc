#include "src/base/error.h"
#include "src/model/batch_identity.h"
#include "src/model/epoch.h"
#include "src/model/identity.h"
#include "src/model/transport_identity.h"

#include <absl/hash/hash.h>
#include <absl/hash/hash_testing.h>
#include <gtest/gtest.h>

#include <array>
#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <tuple>
#include <type_traits>
#include <utility>

namespace {

using kwaque::model::batch_id;
using kwaque::model::batch_sequence;
using kwaque::model::correlation_id;
using kwaque::model::frame_sequence;
using kwaque::model::lease_epoch;
using kwaque::model::producer_epoch;
using kwaque::model::producer_id;
using kwaque::model::producer_stream_id;
using kwaque::model::range_manifest_generation;
using kwaque::model::range_routing_epoch;
using kwaque::model::segment_generation;
using kwaque::model::transport_stream_id;

constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
constexpr std::array<std::uint8_t, 16> producer_bytes{
  0x01,
  0x23,
  0x45,
  0x67,
  0x89,
  0xab,
  0xcd,
  0xef,
  0xfe,
  0xdc,
  0xba,
  0x98,
  0x76,
  0x54,
  0x32,
  0x10};

template<typename Left, typename Right>
concept equality_expression = requires(const Left& lhs, const Right& rhs) {
    lhs == rhs;
} || requires(const Left& lhs, const Right& rhs) { lhs != rhs; };

template<typename Left, typename Right>
concept ordering_expression
  = requires(const Left& lhs, const Right& rhs) { lhs < rhs; }
    || requires(const Left& lhs, const Right& rhs) { lhs <= rhs; }
    || requires(const Left& lhs, const Right& rhs) { lhs > rhs; }
    || requires(const Left& lhs, const Right& rhs) { lhs >= rhs; }
    || requires(const Left& lhs, const Right& rhs) { lhs <=> rhs; };

template<typename Left, typename Right>
concept arithmetic_expression = requires(Left lhs, const Right& rhs) {
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
consteval bool scalar_pair_contract() {
    if constexpr (std::same_as<Left, Right>) {
        return requires(const Left& lhs, const Right& rhs) {
            { lhs == rhs } -> std::same_as<bool>;
            { lhs != rhs } -> std::same_as<bool>;
            { lhs <=> rhs } -> std::same_as<std::strong_ordering>;
        } && !arithmetic_expression<Left, Right>;
    } else {
        return !equality_expression<Left, Right>
               && !ordering_expression<Left, Right>
               && !arithmetic_expression<Left, Right>
               && !std::constructible_from<Left, Right>
               && !std::constructible_from<Left, const Right&>
               && !std::convertible_to<Left, Right>;
    }
}

template<typename Left, typename... Right>
consteval bool scalar_peer_contract(std::tuple<Right...>*) {
    constexpr auto occurrences = (std::size_t{std::same_as<Left, Right>} + ...);
    return occurrences == 1U && (scalar_pair_contract<Left, Right>() && ...)
           && !equality_expression<Left, std::uint64_t>
           && !equality_expression<std::uint64_t, Left>
           && !ordering_expression<Left, std::uint64_t>
           && !ordering_expression<std::uint64_t, Left>
           && !arithmetic_expression<Left, std::uint64_t>
           && !arithmetic_expression<std::uint64_t, Left>
           && !increment_expression<Left>
           && !std::convertible_to<Left, std::uint64_t>
           && !std::convertible_to<std::uint64_t, Left>;
}

template<typename... Value>
consteval bool scalar_type_contract(std::tuple<Value...>* types) {
    return (scalar_peer_contract<Value>(types) && ...);
}

using scalar_types = std::tuple<
  segment_generation,
  range_routing_epoch,
  lease_epoch,
  producer_epoch,
  range_manifest_generation,
  producer_stream_id,
  batch_sequence,
  transport_stream_id,
  correlation_id,
  frame_sequence>;
static_assert(scalar_type_contract(static_cast<scalar_types*>(nullptr)));

template<typename Value>
consteval bool scalar_layout_contract() {
    return sizeof(Value) == sizeof(std::uint64_t)
           && std::is_standard_layout_v<Value>
           && std::is_trivially_copyable_v<Value>
           && std::is_nothrow_default_constructible_v<Value>
           && std::is_nothrow_copy_constructible_v<Value>
           && std::is_nothrow_copy_assignable_v<Value>
           && std::is_nothrow_move_constructible_v<Value>
           && std::is_nothrow_move_assignable_v<Value>
           && std::is_nothrow_destructible_v<Value>;
}

static_assert(scalar_layout_contract<producer_stream_id>());
static_assert(scalar_layout_contract<batch_sequence>());
static_assert(!std::constructible_from<producer_stream_id, std::uint64_t>);
static_assert(!producer_stream_id{}.is_valid());
static_assert(producer_stream_id{}.value() == 0U);
static_assert(batch_sequence{}.value() == 0U);
static_assert(batch_sequence{maximum}.value() == maximum);
static_assert(std::is_nothrow_constructible_v<batch_sequence, std::uint64_t>);
static_assert(noexcept(producer_stream_id::make(1)));
static_assert(noexcept(producer_stream_id{}.checked_successor()));
static_assert(noexcept(batch_sequence{}.checked_successor()));

TEST(
  ProducerStreamIdentityTest, ZeroIsInvalidAndMaximumExhaustsWithoutWrapping) {
    const producer_stream_id staging;
    const auto zero = producer_stream_id::make(0);
    const auto invalid_successor = staging.checked_successor();
    ASSERT_FALSE(zero.has_value());
    ASSERT_FALSE(invalid_successor.has_value());
    EXPECT_EQ(zero.error(), kwaque::errc::invalid_argument);
    EXPECT_EQ(invalid_successor.error(), kwaque::errc::invalid_argument);

    const auto first = producer_stream_id::make(1);
    const auto penultimate = producer_stream_id::make(maximum - 1U);
    const auto last = producer_stream_id::make(maximum);
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(penultimate.has_value());
    ASSERT_TRUE(last.has_value());
    const auto second = first->checked_successor();
    const auto last_successor = penultimate->checked_successor();
    const auto exhausted = last->checked_successor();
    ASSERT_TRUE(second.has_value());
    ASSERT_TRUE(last_successor.has_value());
    ASSERT_FALSE(exhausted.has_value());
    EXPECT_EQ(second->value(), 2U);
    EXPECT_EQ(first->value(), 1U);
    EXPECT_EQ(*last_successor, *last);
    EXPECT_EQ(penultimate->value(), maximum - 1U);
    EXPECT_EQ(last->value(), maximum);
    EXPECT_EQ(exhausted.error(), kwaque::errc::out_of_range);
    EXPECT_LT(*first, *penultimate);
    EXPECT_LT(*penultimate, *last);
    EXPECT_TRUE(
      absl::VerifyTypeImplementsAbslHashCorrectly(
        std::array{staging, *first, *first, *second, *penultimate, *last}));
}

TEST(BatchSequenceTest, ZeroIsValidAndSuccessorChecksTheExclusiveExhaustion) {
    for (const auto raw : {std::uint64_t{0}, std::uint64_t{1}, maximum - 1U}) {
        const batch_sequence current{raw};
        const auto next = current.checked_successor();
        ASSERT_TRUE(next.has_value()) << raw;
        EXPECT_EQ(next->value(), raw + 1U);
        EXPECT_GT(*next, current);
        EXPECT_EQ(current.value(), raw);
    }
    const batch_sequence last{maximum};
    const auto exhausted = last.checked_successor();
    ASSERT_FALSE(exhausted.has_value());
    EXPECT_EQ(exhausted.error(), kwaque::errc::out_of_range);
    EXPECT_EQ(last.value(), maximum);
    EXPECT_LT(batch_sequence{1}, batch_sequence{256});
    EXPECT_LT(
      batch_sequence{(std::uint64_t{1} << 63U) - 1U},
      batch_sequence{std::uint64_t{1} << 63U});
}

TEST(BatchSequenceTest, HashExpansionCoversEveryValueBit) {
    std::array<batch_sequence, 68> cases{};
    cases[1] = batch_sequence{1};
    cases[2] = batch_sequence{1};
    for (std::size_t bit = 0; bit < 64; ++bit) {
        cases[bit + 3] = batch_sequence{std::uint64_t{1} << bit};
    }
    cases.back() = batch_sequence{maximum};
    EXPECT_TRUE(absl::VerifyTypeImplementsAbslHashCorrectly(cases));
    EXPECT_EQ(
      absl::Hash<batch_sequence>{}(cases[1]),
      absl::Hash<batch_sequence>{}(cases[2]));
}

template<typename Batch>
consteval bool owns_returned_components() {
    return std::same_as<decltype(std::declval<Batch>().producer()), producer_id>
           && std::
             same_as<decltype(std::declval<Batch>().epoch()), producer_epoch>
           && std::same_as<
             decltype(std::declval<Batch>().stream()),
             producer_stream_id>
           && std::same_as<
             decltype(std::declval<Batch>().sequence()),
             batch_sequence>;
}

static_assert(sizeof(batch_id) == 40);
static_assert(std::is_standard_layout_v<batch_id>);
static_assert(std::is_trivially_copyable_v<batch_id>);
static_assert(std::is_nothrow_copy_constructible_v<batch_id>);
static_assert(std::is_nothrow_copy_assignable_v<batch_id>);
static_assert(std::is_nothrow_move_constructible_v<batch_id>);
static_assert(std::is_nothrow_move_assignable_v<batch_id>);
static_assert(std::is_nothrow_destructible_v<batch_id>);
static_assert(!std::default_initializable<batch_id>);
static_assert(!std::constructible_from<
              batch_id,
              producer_id,
              producer_epoch,
              producer_stream_id,
              batch_sequence>);
static_assert(!ordering_expression<batch_id, batch_id>);
static_assert(!arithmetic_expression<batch_id, batch_id>);
static_assert(!increment_expression<batch_id>);
static_assert(owns_returned_components<batch_id&>());
static_assert(owns_returned_components<const batch_id&>());
static_assert(owns_returned_components<batch_id>());
static_assert(owns_returned_components<const batch_id>());
static_assert(noexcept(batch_id::make(
  producer_id{}, producer_epoch{}, producer_stream_id{}, batch_sequence{})));
static_assert(noexcept(std::declval<const batch_id&>().producer()));
static_assert(noexcept(std::declval<const batch_id&>().epoch()));
static_assert(noexcept(std::declval<const batch_id&>().stream()));
static_assert(noexcept(std::declval<const batch_id&>().sequence()));

class BatchIdentityTest : public ::testing::Test {
protected:
    void SetUp() override {
        const auto producer = producer_id::make(producer_bytes);
        const auto epoch = producer_epoch::make(1);
        const auto stream = producer_stream_id::make(1);
        ASSERT_TRUE(producer.has_value());
        ASSERT_TRUE(epoch.has_value());
        ASSERT_TRUE(stream.has_value());
        producer_ = *producer;
        epoch_ = *epoch;
        stream_ = *stream;
    }

    producer_id producer_;
    producer_epoch epoch_;
    producer_stream_id stream_;
};

TEST_F(BatchIdentityTest, RejectsEachInvalidComponentAndAcceptsZeroSequence) {
    const std::array invalid_cases{
      batch_id::make(producer_id{}, epoch_, stream_, batch_sequence{}),
      batch_id::make(producer_, producer_epoch{}, stream_, batch_sequence{}),
      batch_id::make(
        producer_, epoch_, producer_stream_id{}, batch_sequence{})};
    for (const auto& invalid : invalid_cases) {
        ASSERT_FALSE(invalid.has_value());
        EXPECT_EQ(invalid.error(), kwaque::errc::invalid_argument);
    }

    const auto first = batch_id::make(
      producer_, epoch_, stream_, batch_sequence{});
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->producer(), producer_);
    EXPECT_EQ(first->epoch(), epoch_);
    EXPECT_EQ(first->stream(), stream_);
    EXPECT_EQ(first->sequence(), batch_sequence{0});

    const auto last_epoch = producer_epoch::make(maximum);
    const auto last_stream = producer_stream_id::make(maximum);
    ASSERT_TRUE(last_epoch.has_value());
    ASSERT_TRUE(last_stream.has_value());
    const auto last = batch_id::make(
      producer_, *last_epoch, *last_stream, batch_sequence{maximum});
    ASSERT_TRUE(last.has_value());
    EXPECT_EQ(last->epoch().value(), maximum);
    EXPECT_EQ(last->stream().value(), maximum);
    EXPECT_EQ(last->sequence().value(), maximum);
}

TEST_F(BatchIdentityTest, ComponentsOutliveTemporaryBatchesAndResults) {
    const auto original = batch_id::make(
      producer_, epoch_, stream_, batch_sequence{9});
    ASSERT_TRUE(original.has_value());
    const auto copied = *original;
    const auto producer = batch_id{copied}.producer();
    const auto epoch = batch_id{copied}.epoch();
    const auto stream = batch_id{copied}.stream();
    const auto sequence = batch_id{copied}.sequence();
    const auto temporary_result_producer
      = decltype(original){copied}->producer();
    EXPECT_EQ(producer, producer_);
    EXPECT_EQ(epoch, epoch_);
    EXPECT_EQ(stream, stream_);
    EXPECT_EQ(sequence, batch_sequence{9});
    EXPECT_EQ(temporary_result_producer, producer_);
    EXPECT_EQ(copied, *original);
}

TEST_F(BatchIdentityTest, EqualityAndHashExpansionUseAllFourComponents) {
    auto changed_bytes = producer_bytes;
    changed_bytes.back() ^= 0xff;
    const auto other_producer = producer_id::make(changed_bytes);
    const auto other_epoch = producer_epoch::make(2);
    const auto other_stream = producer_stream_id::make(2);
    ASSERT_TRUE(other_producer.has_value());
    ASSERT_TRUE(other_epoch.has_value());
    ASSERT_TRUE(other_stream.has_value());
    const auto original = batch_id::make(
      producer_, epoch_, stream_, batch_sequence{});
    ASSERT_TRUE(original.has_value());
    const std::array variants{
      batch_id::make(*other_producer, epoch_, stream_, batch_sequence{}),
      batch_id::make(producer_, *other_epoch, stream_, batch_sequence{}),
      batch_id::make(producer_, epoch_, *other_stream, batch_sequence{}),
      batch_id::make(producer_, epoch_, stream_, batch_sequence{1})};
    for (const auto& variant : variants) {
        ASSERT_TRUE(variant.has_value());
        EXPECT_NE(*original, *variant);
    }
    const std::array cases{
      *original,
      *original,
      *variants[0],
      *variants[1],
      *variants[2],
      *variants[3]};
    EXPECT_TRUE(absl::VerifyTypeImplementsAbslHashCorrectly(cases));
    EXPECT_EQ(
      absl::Hash<batch_id>{}(cases[0]), absl::Hash<batch_id>{}(cases[1]));
}

TEST_F(
  BatchIdentityTest, CanonicalOrderUsesProducerOctetsThenNumericComponents) {
    auto low_bytes = producer_bytes;
    auto high_bytes = producer_bytes;
    low_bytes[0] = 0x7f;
    high_bytes[0] = 0x80;
    const auto low = producer_id::make(low_bytes);
    const auto high = producer_id::make(high_bytes);
    const auto epoch256 = producer_epoch::make(256);
    const auto stream256 = producer_stream_id::make(256);
    const auto last_epoch = producer_epoch::make(maximum);
    const auto last_stream = producer_stream_id::make(maximum);
    ASSERT_TRUE(low.has_value());
    ASSERT_TRUE(high.has_value());
    ASSERT_TRUE(epoch256.has_value());
    ASSERT_TRUE(stream256.has_value());
    ASSERT_TRUE(last_epoch.has_value());
    ASSERT_TRUE(last_stream.has_value());
    const std::array ordered_pairs{
      std::pair{
        batch_id::make(
          *low, *last_epoch, *last_stream, batch_sequence{maximum}),
        batch_id::make(*high, epoch_, stream_, batch_sequence{})},
      std::pair{
        batch_id::make(
          producer_, epoch_, *last_stream, batch_sequence{maximum}),
        batch_id::make(producer_, *epoch256, stream_, batch_sequence{})},
      std::pair{
        batch_id::make(producer_, epoch_, stream_, batch_sequence{maximum}),
        batch_id::make(producer_, epoch_, *stream256, batch_sequence{})},
      std::pair{
        batch_id::make(producer_, epoch_, stream_, batch_sequence{1}),
        batch_id::make(producer_, epoch_, stream_, batch_sequence{256})}};
    for (const auto& [earlier, later] : ordered_pairs) {
        ASSERT_TRUE(earlier.has_value());
        ASSERT_TRUE(later.has_value());
        EXPECT_TRUE(earlier->canonical_less(*later));
        EXPECT_FALSE(later->canonical_less(*earlier));
        EXPECT_FALSE(earlier->canonical_less(*earlier));
        EXPECT_NE(*earlier, *later);
    }
}

} // namespace
