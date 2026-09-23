#include "src/storage/local_metadata_layout.h"
#include "src/storage/local_types.h"

#include <gtest/gtest.h>

#include <array>
#include <type_traits>

namespace kwaque::storage {
namespace {
template<typename Id>
Id identity(std::uint8_t byte) {
    std::array<std::uint8_t, 16> bytes{};
    bytes.fill(byte);
    return Id::make(bytes).value();
}
local_store_context owner(std::uint8_t device = 3, std::uint32_t shard = 0) {
    return local_store_context::make(
             identity<model::cluster_id>(1),
             identity<model::broker_id>(2),
             identity<device_store_id>(device),
             shard)
      .value();
}
static_assert(
  !std::is_convertible_v<local_object_sequence, local_decision_sequence>);
static_assert(!std::is_convertible_v<local_object_high, local_object_sequence>);
static_assert(
  !std::is_convertible_v<local_wal_high, model::wal_incarnation_id>);
static_assert(!std::is_default_constructible_v<local_root_reference>);

TEST(
  LocalTypesTest, AllocationMarksDistinguishZeroFromUsableIdsAndCheckOverflow) {
    local_object_high zero;
    EXPECT_FALSE(zero.sequence());
    EXPECT_FALSE(zero.checked_advance(0));
    const auto reserved = zero.checked_advance(16).value();
    EXPECT_EQ(reserved.sequence()->value(), 16U);
    EXPECT_EQ(zero.value(), 0U);
    EXPECT_FALSE(local_object_high{UINT64_MAX}.checked_advance(1));
    EXPECT_EQ(
      local_object_high{UINT64_MAX - 1}.checked_advance(1)->value(),
      UINT64_MAX);
    EXPECT_FALSE(local_publication_generation::make(0));
    EXPECT_FALSE(
      local_publication_generation::make(UINT64_MAX)->checked_successor());
}
TEST(LocalTypesTest, WalArithmeticCarriesAcrossBothWordsWithoutWrapping) {
    local_wal_high zero;
    EXPECT_TRUE(zero.empty());
    EXPECT_FALSE(zero.incarnation());
    EXPECT_FALSE(zero.checked_advance(0));
    auto one = zero.checked_advance(1).value();
    EXPECT_EQ(one.bytes().back(), 1U);
    auto top_low = zero.checked_advance(UINT64_MAX).value();
    auto carried = top_low.checked_advance(1).value();
    for (std::size_t i = 0; i < 16; ++i)
        EXPECT_EQ(carried.bytes()[i], i == 7 ? 1U : 0U);
    EXPECT_TRUE(carried > top_low);
    EXPECT_EQ(
      local_wal_high::from_incarnation(carried.incarnation().value()), carried);
    std::array<std::uint8_t, 16> maximum;
    maximum.fill(0xff);
    EXPECT_FALSE(local_wal_high::make(maximum)->checked_advance(1));
    EXPECT_FALSE(local_wal_high::make(std::span{maximum}.first(15)));
}
TEST(LocalTypesTest, CursorComparisonRequiresExactStoreAndAllowsBurnedGaps) {
    auto first_id = local_wal_high{}.checked_advance(1)->incarnation().value();
    auto next_id = local_wal_high{}.checked_advance(9)->incarnation().value();
    auto first
      = local_wal_cursor::make(first_id, runtime::file_position{8192}).value();
    auto next
      = local_wal_cursor::make(next_id, runtime::file_position{4096}).value();
    EXPECT_TRUE(first.compare(owner(), next, owner()).value() < 0);
    EXPECT_FALSE(first.compare(owner(), next, owner(4)));
    EXPECT_FALSE(first.compare(owner(), next, owner(3, 1)));
    EXPECT_FALSE(first.compare(
      owner(3, local_store_shard), next, owner(3, local_store_shard)));
    EXPECT_FALSE(local_wal_cursor::make({}, runtime::file_position{4096}));
    EXPECT_FALSE(local_wal_cursor::make(first_id, runtime::file_position{}));
    EXPECT_FALSE(local_wal_cursor::make(first_id, runtime::file_position{1}));
    EXPECT_FALSE(
      local_store_context::make(
        {}, identity<model::broker_id>(2), identity<device_store_id>(3), 0));
}
TEST(LocalTypesTest, RootAndFooterKindsKeepDifferentPlacementRules) {
    const auto sequence = local_object_sequence::make(1).value();
    const auto pages = page_count::make(0).value();
    const auto alignment = storage_alignment::make(byte_count{4096}).value();
    codec::immutable_object_digest hash{codec::sha256_digest{}};
    EXPECT_FALSE(
      local_root_reference::make(
        local_root_kind::sealed_retry,
        sequence,
        runtime::file_position{},
        byte_count{4096},
        pages,
        hash));
    EXPECT_FALSE(
      local_footer_reference::make(
        runtime::file_position{}, byte_count{4096}, 7, hash));
    EXPECT_FALSE(
      local_root_reference::make(
        local_root_kind::index,
        sequence,
        runtime::file_position{4096},
        byte_count{4096},
        pages,
        hash));
    auto retry = local_root_reference::make(
                   local_root_kind::sealed_retry,
                   sequence,
                   runtime::file_position{4096},
                   byte_count{4096},
                   pages,
                   hash)
                   .value();
    EXPECT_TRUE(retry.validate_alignment(alignment));
    EXPECT_FALSE(
      local_root_reference::make(
        local_root_kind::sealed_retry,
        sequence,
        runtime::file_position{UINT64_MAX},
        byte_count{4096},
        pages,
        hash));
    EXPECT_FALSE(
      local_root_reference::make(
        local_root_kind::index,
        {},
        runtime::file_position{},
        byte_count{4096},
        pages,
        hash));
    EXPECT_EQ(parse_local_root_kind(6).error(), errc::unsupported_format);
    EXPECT_FALSE(
      local_footer_reference::make(
        runtime::file_position{4096}, byte_count{4096}, 0, hash));
    EXPECT_EQ(
      local_footer_reference::make(
        runtime::file_position{4096}, byte_count{4096}, 8, hash)
        .error(),
      errc::unsupported_format);
    EXPECT_TRUE(
      local_footer_reference::make(
        runtime::file_position{4096}, byte_count{4096}, 6, hash));
    EXPECT_TRUE(
      local_footer_reference::make(
        runtime::file_position{4096}, byte_count{4096}, 7, hash));
}
TEST(LocalTypesTest, LayoutUsesActualHeaderAndIndependentPageEquations) {
    auto alignment = storage_alignment::make(byte_count{4096}).value();
    auto policy = codec::limits::defaults();
    EXPECT_EQ(
      local_metadata_page_capacity(
        local_metadata_kind::checkpoint_page, byte_count{32}, alignment, policy)
        .value(),
      398U);
    EXPECT_EQ(
      local_metadata_page_capacity(
        local_metadata_kind::completed_retry_page,
        byte_count{32},
        alignment,
        policy)
        .value(),
      408U);
    EXPECT_EQ(
      local_metadata_layout(
        local_metadata_kind::checkpoint_page,
        byte_count{20 + 399 * 164},
        byte_count{32},
        alignment,
        policy)
        .error(),
      errc::resource_exhausted);
    struct invalid_fixed_payload final {
        local_metadata_kind kind;
        std::uint64_t bytes;
    };
    for (const auto invalid : std::array{
           invalid_fixed_payload{local_metadata_kind::store_identity, 17},
           invalid_fixed_payload{local_metadata_kind::shard_control, 157},
           invalid_fixed_payload{local_metadata_kind::wal_descriptor, 69},
           invalid_fixed_payload{local_metadata_kind::segment_descriptor, 121},
           invalid_fixed_payload{local_metadata_kind::recovery_decision, 149},
           invalid_fixed_payload{
             local_metadata_kind::boundary_evidence, 249}}) {
        EXPECT_EQ(
          local_metadata_layout(
            invalid.kind,
            byte_count{invalid.bytes},
            byte_count{32},
            alignment,
            policy)
            .error(),
          errc::invalid_argument);
    }
    EXPECT_EQ(
      local_metadata_layout(
        local_metadata_kind::deletion_intent,
        byte_count{96 + 513 * 12},
        byte_count{32},
        alignment,
        policy)
        .error(),
      errc::resource_exhausted);
    auto narrowed = policy.config();
    narrowed.max_page_bytes = byte_count{512};
    EXPECT_EQ(
      local_metadata_layout(
        local_metadata_kind::store_identity,
        byte_count{16},
        byte_count{32},
        alignment,
        codec::limits::make(narrowed).value())
        .error(),
      errc::resource_exhausted);
    for (const auto payload : {48U, 96U, 108U, 156U}) {
        const auto layout = local_metadata_layout(
                              local_metadata_kind::shard_control,
                              byte_count{payload},
                              byte_count{32},
                              alignment,
                              policy)
                              .value();
        EXPECT_EQ(layout.encoded_bytes().value(), 4096U);
        EXPECT_EQ(layout.padding_bytes().value(), 4096U - 32U - 72U - payload);
    }
    EXPECT_FALSE(local_metadata_descriptor_for(0));
    EXPECT_FALSE(local_metadata_layout(
      local_metadata_kind::shard_control,
      byte_count{49},
      byte_count{32},
      alignment,
      policy));
    EXPECT_FALSE(local_metadata_layout(
      local_metadata_kind::checkpoint_page,
      byte_count{20},
      byte_count{32},
      alignment,
      policy));
    EXPECT_EQ(
      local_metadata_descriptor_for(13).error(), errc::unsupported_format);
    EXPECT_EQ(local_metadata_descriptors.size(), 12U);
}
} // namespace
} // namespace kwaque::storage
