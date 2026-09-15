#include "src/model/batch_identity.h"
#include "src/storage/format_context.h"

#include <gtest/gtest.h>

#include <array>
#include <concepts>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <utility>

namespace {
namespace model = kwaque::model;
namespace storage = kwaque::storage;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::runtime::file_position;
using storage::coverage;
using storage::segment_context;
using storage::segment_write_context;
using storage::storage_alignment;
using storage::wal_write_context;
constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();

template<typename T>
consteval bool checked_value() {
    return std::is_trivially_copyable_v<T>
           && std::is_nothrow_move_constructible_v<T>
           && std::is_nothrow_destructible_v<T> && !std::is_aggregate_v<T>
           && !std::default_initializable<T>;
}
static_assert(checked_value<storage_alignment>());
static_assert(checked_value<segment_context>());
static_assert(checked_value<segment_write_context>());
static_assert(checked_value<wal_write_context>());
static_assert(checked_value<coverage>());
static_assert(
  !std::convertible_to<model::producer_stream_binding, segment_context>);
static_assert(!std::convertible_to<wal_write_context, segment_write_context>);
static_assert(
  !std::convertible_to<storage::storage_profile, storage::replay_profile>);
static_assert(!std::constructible_from<storage_alignment, byte_count>);
static_assert(std::same_as<
              decltype(std::declval<coverage>().logical()),
              model::range_logical_span>);
static_assert(std::same_as<
              decltype(std::declval<coverage>().physical()),
              model::segment_relative_span>);
static_assert(std::same_as<
              decltype(std::declval<coverage>().bytes()),
              model::file_byte_span>);

template<typename Physical, typename Position>
concept segment_location = requires(
  segment_context context,
  storage_alignment alignment,
  Physical physical,
  Position position) {
    segment_write_context::make(context, alignment, physical, position);
};
static_assert(segment_location<model::segment_relative_end, file_position>);
static_assert(!segment_location<model::range_logical_end, file_position>);
static_assert(!segment_location<file_position, model::segment_relative_end>);
static_assert(!segment_location<model::segment_relative_end, byte_count>);

template<typename Id>
Id id(std::uint8_t byte) {
    std::array<std::uint8_t, 16> bytes;
    bytes.fill(byte);
    return Id::make(bytes).value();
}
storage_alignment alignment(std::uint64_t bytes = 512) {
    return storage_alignment::make(byte_count{bytes}).value();
}
segment_context context(std::uint8_t changed = 0) {
    return segment_context::make(
             id<model::cluster_id>(changed == 1 ? 11 : 1),
             id<model::topic_id>(changed == 2 ? 12 : 2),
             id<model::range_id>(changed == 3 ? 13 : 3),
             id<model::segment_id>(changed == 4 ? 14 : 4),
             model::segment_generation::make(changed == 5 ? 15 : 5).value())
      .value();
}
void expect_error(const auto& result, errc error) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), error);
}

TEST(StorageContextTest, AlignmentAcceptsOnlyTheBoundedPowerOfTwoDomain) {
    for (unsigned shift = 9; shift <= 16; ++shift) {
        const auto bytes = std::uint64_t{1} << shift;
        const auto value = storage_alignment::make(byte_count{bytes});
        ASSERT_TRUE(value.has_value());
        EXPECT_EQ(value->bytes(), byte_count{bytes});
        EXPECT_TRUE(value->aligned(file_position{}));
        EXPECT_TRUE(value->aligned(file_position{3 * bytes}));
        EXPECT_FALSE(value->aligned(file_position{bytes + 1}));
        EXPECT_FALSE(value->aligned(file_position{maximum}));
    }
    for (const auto bytes : std::array<std::uint64_t, 9>{
           0, 1, 511, 513, 768, 32769, 65535, 131072, maximum})
        expect_error(
          storage_alignment::make(byte_count{bytes}), errc::invalid_argument);
}

TEST(StorageContextTest, ProfilesDistinguishInvalidZeroFromUnsupportedValues) {
    EXPECT_EQ(
      storage::parse_storage_profile(1).value(), storage::storage_profile::v1);
    EXPECT_EQ(
      storage::parse_replay_profile(1).value(), storage::replay_profile::v1);
    expect_error(storage::parse_storage_profile(0), errc::invalid_argument);
    expect_error(storage::parse_replay_profile(0), errc::invalid_argument);
    for (const auto value : std::array<std::uint16_t, 3>{2, 256, 65535}) {
        expect_error(
          storage::parse_storage_profile(value), errc::unsupported_format);
        expect_error(
          storage::parse_replay_profile(value), errc::unsupported_format);
    }
}

TEST(StorageContextTest, EverySegmentIdentityComponentIsCheckedAndCompared) {
    const auto expected = context();
    EXPECT_EQ(expected.cluster(), id<model::cluster_id>(1));
    EXPECT_EQ(expected.topic(), id<model::topic_id>(2));
    EXPECT_EQ(expected.range(), id<model::range_id>(3));
    EXPECT_EQ(expected.segment(), id<model::segment_id>(4));
    EXPECT_EQ(expected.generation().value(), 5U);
    EXPECT_TRUE(expected.validate_expected(context()));
    for (std::uint8_t field = 1; field <= 5; ++field) {
        const auto different = context(field);
        EXPECT_NE(different, expected);
        expect_error(
          different.validate_expected(expected), errc::wrong_context);
    }
    for (unsigned field = 0; field < 5; ++field) {
        expect_error(
          segment_context::make(
            field == 0 ? model::cluster_id{} : expected.cluster(),
            field == 1 ? model::topic_id{} : expected.topic(),
            field == 2 ? model::range_id{} : expected.range(),
            field == 3 ? model::segment_id{} : expected.segment(),
            field == 4 ? model::segment_generation{} : expected.generation()),
          errc::invalid_argument);
    }
    const auto greatest = segment_context::make(
      id<model::cluster_id>(255),
      id<model::topic_id>(255),
      id<model::range_id>(255),
      id<model::segment_id>(255),
      model::segment_generation::make(maximum).value());
    ASSERT_TRUE(greatest.has_value());
    EXPECT_EQ(greatest->generation().value(), maximum);
}

TEST(StorageContextTest, CurrentLocationCannotReplaceOriginalProducerBinding) {
    const auto original = model::producer_stream_binding::make(
                            id<model::topic_id>(2),
                            id<model::range_id>(3),
                            model::range_routing_epoch::make(9).value(),
                            id<model::segment_id>(7),
                            model::segment_generation::make(11).value())
                            .value();
    const auto current = segment_write_context::make(
                           context(),
                           alignment(4096),
                           model::segment_relative_end{17},
                           file_position{8192})
                           .value();
    EXPECT_EQ(current.segment().topic(), original.topic());
    EXPECT_EQ(current.segment().range(), original.range());
    EXPECT_NE(current.segment().segment(), original.segment());
    EXPECT_NE(current.segment().generation(), original.generation());
    EXPECT_EQ(original.routing_epoch().value(), 9U);
    EXPECT_EQ(original.segment(), id<model::segment_id>(7));
    EXPECT_EQ(original.generation().value(), 11U);
    EXPECT_EQ(current.physical_begin(), model::segment_relative_end{17});
    EXPECT_EQ(current.position(), file_position{8192});
}

TEST(
  StorageContextTest,
  SegmentExpectedPositionIncludesAlignmentAndPhysicalOrdinal) {
    const auto expected = segment_write_context::make(
                            context(),
                            alignment(),
                            model::segment_relative_end{17},
                            file_position{4096})
                            .value();
    EXPECT_TRUE(expected.validate_expected(expected));
    const std::array mismatches{
      segment_write_context::make(
        context(4),
        alignment(),
        model::segment_relative_end{17},
        file_position{4096})
        .value(),
      segment_write_context::make(
        context(),
        alignment(4096),
        model::segment_relative_end{17},
        file_position{4096})
        .value(),
      segment_write_context::make(
        context(),
        alignment(),
        model::segment_relative_end{18},
        file_position{4096})
        .value(),
      segment_write_context::make(
        context(),
        alignment(),
        model::segment_relative_end{17},
        file_position{4608})
        .value()};
    for (const auto& different : mismatches)
        expect_error(
          different.validate_expected(expected), errc::wrong_context);
    expect_error(
      segment_write_context::make(
        context(),
        alignment(),
        model::segment_relative_end{},
        file_position{513}),
      errc::invalid_argument);
    const auto terminal = segment_write_context::make(
      context(),
      alignment(),
      model::segment_relative_end{maximum},
      file_position{});
    ASSERT_TRUE(terminal.has_value());
    EXPECT_EQ(terminal->physical_begin().value(), maximum);
}

TEST(
  StorageContextTest, WalIncarnationAndPositionAreIndependentOfTheTargetFile) {
    const auto expected = wal_write_context::make(
                            id<model::wal_incarnation_id>(6),
                            alignment(),
                            file_position{512})
                            .value();
    EXPECT_TRUE(expected.validate_expected(expected));
    EXPECT_EQ(expected.incarnation(), id<model::wal_incarnation_id>(6));
    EXPECT_EQ(expected.position(), file_position{512});
    const auto segment = segment_write_context::make(
                           context(),
                           alignment(4096),
                           model::segment_relative_end{3},
                           file_position{8192})
                           .value();
    EXPECT_NE(expected.alignment(), segment.alignment());
    EXPECT_NE(expected.position(), segment.position());
    expect_error(
      wal_write_context::make(
        id<model::wal_incarnation_id>(7), alignment(), file_position{512})
        .value()
        .validate_expected(expected),
      errc::wrong_context);
    expect_error(
      wal_write_context::make(
        expected.incarnation(), alignment(), file_position{1024})
        .value()
        .validate_expected(expected),
      errc::wrong_context);
    const auto aligned_wal = wal_write_context::make(
                               expected.incarnation(),
                               alignment(),
                               file_position{4096})
                               .value();
    expect_error(
      wal_write_context::make(
        expected.incarnation(), alignment(4096), file_position{4096})
        .value()
        .validate_expected(aligned_wal),
      errc::wrong_context);
    expect_error(
      wal_write_context::make({}, alignment(), file_position{}),
      errc::invalid_argument);
    expect_error(
      wal_write_context::make(
        expected.incarnation(), alignment(), file_position{1}),
      errc::invalid_argument);
    EXPECT_TRUE(
      wal_write_context::make(
        expected.incarnation(), alignment(), file_position{}));
}

TEST(
  StorageContextTest, CoveragePreservesSparseEmptyAndIndependentSpanDomains) {
    const auto logical = model::range_logical_span::make(
                           model::range_logical_end{100},
                           model::range_logical_end{10000})
                           .value();
    const auto physical = model::segment_relative_span::make(
                            model::segment_relative_end{7},
                            model::segment_relative_end{9})
                            .value();
    const auto bytes = model::file_byte_span::make(
                         file_position{512}, file_position{1536})
                         .value();
    const coverage sparse{logical, physical, bytes};
    EXPECT_EQ(sparse.logical(), logical);
    EXPECT_EQ(sparse.physical(), physical);
    EXPECT_EQ(sparse.bytes(), bytes);
    const coverage removed{
      logical,
      model::segment_relative_span::make(
        model::segment_relative_end{9}, model::segment_relative_end{9})
        .value(),
      model::file_byte_span::make(file_position{1536}, file_position{1536})
        .value()};
    EXPECT_FALSE(removed.logical().empty());
    EXPECT_TRUE(removed.physical().empty());
    EXPECT_TRUE(removed.bytes().empty());
    EXPECT_NE(sparse, removed);
    const coverage other_logical{
      model::range_logical_span::make(
        model::range_logical_end{101}, model::range_logical_end{10000})
        .value(),
      removed.physical(),
      removed.bytes()};
    EXPECT_NE(other_logical, removed);
    EXPECT_NE((coverage{logical, physical, removed.bytes()}), sparse);
    EXPECT_NE((coverage{logical, removed.physical(), bytes}), sparse);
    const coverage terminal{
      model::range_logical_span::make(
        model::range_logical_end{maximum}, model::range_logical_end{maximum})
        .value(),
      model::segment_relative_span::make(
        model::segment_relative_end{maximum},
        model::segment_relative_end{maximum})
        .value(),
      model::file_byte_span::make(
        file_position{maximum}, file_position{maximum})
        .value()};
    EXPECT_TRUE(terminal.logical().empty());
    EXPECT_TRUE(terminal.physical().empty());
    EXPECT_TRUE(terminal.bytes().empty());
}
} // namespace
