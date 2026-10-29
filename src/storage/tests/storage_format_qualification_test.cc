#include "src/base/units.h"
#include "src/codec/tests/benchmark_buffer.h"
#include "src/codec/tests/qualification_profile.h"
#include "src/storage/tests/storage_format_fixture.h"
#include "src/storage/tests/storage_large_fixture.h"

#include <gtest/gtest.h>

namespace kwaque::storage::testing {
namespace {
using bytes::fragmented_buffer_parser;

std::string shortened_child(const storage_fixture& fixture, std::size_t count) {
    const bool wal = fixture.kind == storage_case::wal;
    const std::size_t fixed = wal ? 168U : 144U;
    auto body = fixture.wire.substr(32, fixed);
    put(body, wal ? 160U : 136U, count, 4);
    body += fixture.child.substr(0, count);
    // The aligned PREPARE ends its write; a block never pads.
    const auto padding = wal ? (512U - (32U + body.size()) % 512U) % 512U : 0U;
    put(body, wal ? 164U : 140U, padding, 4);
    body.append(padding, '\0');
    return frame(std::move(body), wal ? std::uint16_t{5} : std::uint16_t{4});
}
} // namespace

TEST(
  StorageQualificationTest, CorruptionAndRepairedContextReachSeparateLayers) {
    for (std::uint8_t kind = 0; kind < 6; ++kind) {
        const storage_fixture fixture{static_cast<storage_case>(kind)};
        const std::size_t context = kind == 2 ? 24U : kind >= 4 ? 4U : 0U;
        auto corrupt = fixture.wire;
        corrupt[32 + context] ^= 1;
        const auto damaged = observe_storage(fixture, corrupt, 7);
        ASSERT_TRUE(damaged.error);
        EXPECT_EQ(damaged.error->code(), errc::corrupt_data);
        repair(corrupt);
        const auto misplaced = observe_storage(fixture, corrupt, 7);
        ASSERT_TRUE(misplaced.error);
        EXPECT_EQ(misplaced.error->code(), errc::wrong_context);
        EXPECT_EQ(misplaced.consumed, byte_count{});
        auto padding = fixture.wire;
        // A block never pads, so its padding count is what breaks.
        if (fixture.kind == storage_case::block)
            put(padding, 32 + 140, 1, 4);
        else
            padding.back() ^= 1;
        repair(padding);
        const auto nonzero = observe_storage(fixture, padding, 7);
        ASSERT_TRUE(nonzero.error);
        EXPECT_EQ(nonzero.error->code(), errc::malformed_data);
        const auto joined = observe_storage(
          fixture, fixture.wire + fixture.wire, 7);
        EXPECT_FALSE(joined.error);
        EXPECT_EQ(joined.consumed.value(), fixture.wire.size());
    }
}

TEST(
  StorageQualificationTest,
  EveryInnerCutRejectsEvenWithRepairedOuterIntegrity) {
    for (const auto kind : {storage_case::block, storage_case::wal}) {
        for (const bool compressed : {false, true}) {
            const storage_fixture fixture{kind, compressed};
            for (std::size_t cut = 0; cut < fixture.child.size(); ++cut) {
                SCOPED_TRACE(cut);
                const auto result = observe_storage(
                  fixture, shortened_child(fixture, cut), 7);
                ASSERT_TRUE(result.error);
                EXPECT_EQ(result.consumed, byte_count{});
                EXPECT_NE(result.error->code(), errc::truncated_data);
                seastar::thread::maybe_yield();
            }
            const auto valid = observe_storage(
              fixture, shortened_child(fixture, fixture.child.size()), 7);
            EXPECT_FALSE(valid.error);
        }
    }
}

TEST(StorageQualificationTest, RecycledObjectsDoNotSkipToALaterValidEnvelope) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto current = block_expected(0x30, 2);
    const auto old_block = block_wire();
    const auto new_block = block_wire(assigned_wire(), current);
    fragmented_buffer_parser input{buffer(old_block + new_block, 7)};
    const auto memory = reserve(input, work);
    const auto stale = decode_segment_block(input, current, memory, work).get();
    ASSERT_FALSE(stale.has_value());
    EXPECT_EQ(stale.error().code(), errc::wrong_context);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
    input.skip(byte_count{old_block.size()}).value();
    const auto next = decode_segment_block(input, current, memory, work).get();
    ASSERT_TRUE(next.has_value());
    EXPECT_TRUE(input.at_end());
    auto context = wal_expected();
    context.wal = wal_write_context::make(
                    id<model::wal_incarnation_id>(0x71),
                    alignment(),
                    context.wal_data_start)
                    .value();
    const auto old_wal = wal_wire();
    fragmented_buffer_parser wal{
      buffer(old_wal + wal_wire(assigned_wire(), context), 7)};
    const auto wal_memory = reserve(wal, work);
    const auto rejected
      = decode_wal_prepare(wal, context, wal_memory, work).get();
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().code(), errc::wrong_context);
    EXPECT_EQ(wal.bytes_consumed(), byte_count{});
    wal.skip(byte_count{old_wal.size()}).value();
    EXPECT_TRUE(decode_wal_prepare(wal, context, wal_memory, work).get());
    EXPECT_TRUE(wal.at_end());
}

TEST(StorageQualificationTest, ExactAndOneShortRootPageAndWrapperLimits) {
    for (const auto kind :
         {storage_case::header,
          storage_case::block,
          storage_case::wal,
          storage_case::durable,
          storage_case::sealed,
          storage_case::retry}) {
        SCOPED_TRACE(static_cast<unsigned>(kind));
        const storage_fixture fixture{kind};
        const std::uint64_t exact = fixture.wire.size() - 32U;
        for (const std::uint64_t body : {exact - 1U, exact}) {
            SCOPED_TRACE(body);
            auto config = codec::limits_config{};
            config.max_encoded_body_bytes = byte_count{body};
            const auto seen = observe_storage(
              fixture, fixture.wire, 512, codec::limits::make(config).value());
            EXPECT_EQ(seen.error.has_value(), body < exact);
            if (seen.error) {
                EXPECT_EQ(seen.error->code(), errc::resource_exhausted);
                EXPECT_EQ(seen.consumed, byte_count{});
            } else {
                EXPECT_EQ(seen.consumed.value(), fixture.wire.size());
            }
        }
        if (kind == storage_case::sealed || kind == storage_case::retry) {
            for (const std::uint64_t cap : {511U, 512U}) {
                SCOPED_TRACE(cap);
                auto config = codec::limits_config{};
                config.max_page_bytes = byte_count{cap};
                const auto seen = observe_storage(
                  fixture,
                  fixture.wire,
                  512,
                  codec::limits::make(config).value());
                EXPECT_EQ(seen.error.has_value(), cap == 511);
                if (seen.error) {
                    EXPECT_EQ(seen.error->code(), errc::resource_exhausted);
                    EXPECT_EQ(seen.consumed, byte_count{});
                } else {
                    EXPECT_EQ(seen.consumed.value(), fixture.wire.size());
                }
            }
        }
    }
}

TEST(StorageQualificationTest, ThreeGroupsIncludeEachEarlierFooterExactlyOnce) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    // Each footer is packed after its block and pads to the next group's
    // start, so the extent ends with the third block.
    const auto end = 2560U + data_block(102, 2, 2560).size();
    auto verifier = extent_verifier::make(
                      history(),
                      scope(100, 103, 0, 3, 512, end),
                      work.policy(),
                      extent_layout_kind::initial_append,
                      {},
                      extent_integrity::crc32c_and_digest)
                      .value();
    std::string supplied;
    for (std::uint64_t group = 0; group < 3; ++group) {
        const auto position = 512U + 1024U * group;
        const auto block = data_block(100U + group, group, position);
        ASSERT_TRUE(feed_block(verifier, block, work));
        supplied += block;
        if (group < 2) {
            const auto prefix = verifier.checkpoint(work).value();
            EXPECT_EQ(prefix.boundary().block_count, group + 1U);
            const auto footer = footer_wire(
              prefix.boundary(),
              {history(), runtime::file_position{position + block.size()}});
            ASSERT_EQ(
              position + block.size() + footer.size(), position + 1024U);
            ASSERT_TRUE(feed_footer(verifier, footer, work, prefix));
            supplied += footer;
        }
    }
    const auto proof = verifier.finish(work).value();
    EXPECT_EQ(proof.boundary().block_count, 3U);
    EXPECT_EQ(proof.boundary().last_block, scope(102, 103, 2, 3, 2560, end));
    EXPECT_EQ(proof.boundary().data_crc32c, crc(supplied));
    EXPECT_EQ(proof.digest()->bytes(), exact_digest(supplied));
}

TEST(
  StorageQualificationTest, MaximumAssignedRegionSurvivesBothPhysicalWrappers) {
    using codec::bench::capacity_bound;
    codec::bench::qualify_allocator();
    // The original child, its conservatively charged wrapper alias and the
    // expanded records overlap during LZ4 decode. Reserve one MiB for native
    // engines/frames and use the remaining operation allowance for these
    // owners.
    constexpr byte_count operation_budget{codec::testing::residual};
    for (const bool compressed : {false, true}) {
        SCOPED_TRACE(compressed ? "lz4" : "none");
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto built = large_child(compressed, work, operation_budget).get();
        ASSERT_TRUE(built.has_value())
          << "build child: code=" << static_cast<int>(built.error().code())
          << " family=" << built.error().family()
          << " field=" << built.error().field()
          << " offset=" << built.error().byte_offset();
        auto child = std::move(*built);
        const auto facts = child.info();
        ASSERT_EQ(facts.context.retained_count().value(), 8U);
        for (const bool wal : {false, true}) {
            SCOPED_TRACE(wal ? "WAL" : "segment");
            const auto cost
              = child.bytes().allocation_cost(capacity_bound).value();
            const auto remaining = operation_budget.checked_sub(cost.backing)
                                     .value()
                                     .checked_sub(cost.descriptors)
                                     .value()
                                     .checked_sub(cost.share_controls)
                                     .value();
            auto shared
              = child
                  .share({remaining, byte_count{1_MiB}, capacity_bound}, work)
                  .get();
            ASSERT_TRUE(shared.has_value())
              << "share child: code="
              << static_cast<int>(shared.error().code());
            auto copy = std::move(*shared);
            bytes::fragmented_buffer wire;
            if (wal) {
                const auto expected = wal_expected(4096, 512);
                auto encoded = encode_wal_prepare(
                                 std::move(copy),
                                 expected,
                                 {true,
                                  runtime::wall_time{1},
                                  expected.target_data_start,
                                  expected.wal_data_start},
                                 work,
                                 remaining,
                                 capacity_bound)
                                 .get();
                ASSERT_TRUE(encoded.has_value())
                  << "encode WAL: code="
                  << static_cast<int>(encoded.error().code())
                  << " field=" << encoded.error().field();
                wire = std::move(*encoded);
            } else {
                const auto expected = block_expected();
                auto block = encode_segment_block(
                               std::move(copy),
                               expected,
                               {runtime::wall_time{1}, expected.data_start},
                               work,
                               remaining,
                               capacity_bound)
                               .get();
                ASSERT_TRUE(block.has_value())
                  << "encode segment: code="
                  << static_cast<int>(block.error().code())
                  << " field=" << block.error().field();
                wire = std::move(*block).release_bytes();
            }
            EXPECT_GT(wire.size().value(), 8_MiB);
            EXPECT_LE(wire.fragment_count(), 1024U);
            // Each nested child contributes its complete envelope to its
            // wrapper.
            auto bytes = wire.share();
            fragmented_buffer_parser input{std::move(bytes)};
            const auto admitted = codec::reserve_decode_input(
              input,
              work.policy(),
              {remaining, byte_count{1_MiB}, capacity_bound});
            ASSERT_TRUE(admitted.has_value())
              << "reserve wrapper: code="
              << static_cast<int>(admitted.error().code());
            const auto memory = *admitted;
            if (wal) {
                const auto read
                  = decode_wal_prepare(
                      input, wal_expected(4096, 512), memory, work)
                      .get();
                ASSERT_TRUE(read.has_value())
                  << "decode WAL: code="
                  << static_cast<int>(read.error().code())
                  << " field=" << read.error().field();
                EXPECT_EQ(read->value.batch().info(), facts);
                EXPECT_TRUE(
                  codec::bench::buffers_equal(
                    read->value.batch().bytes(), child.bytes(), work)
                    .get());
            } else {
                const auto read = decode_segment_block(
                                    input, block_expected(), memory, work)
                                    .get();
                ASSERT_TRUE(read.has_value())
                  << "decode segment: code="
                  << static_cast<int>(read.error().code())
                  << " field=" << read.error().field();
                EXPECT_EQ(read->value.descriptor().batch(), facts);
                EXPECT_EQ(
                  read->value.descriptor()
                    .coverage()
                    .physical()
                    .count()
                    .value(),
                  8U);
                auto body
                  = wire.share(byte_count{176}, child.bytes().size()).value();
                EXPECT_TRUE(
                  codec::bench::buffers_equal(body, child.bytes(), work).get());
            }
            EXPECT_TRUE(input.at_end());
        }
    }
}
} // namespace kwaque::storage::testing
