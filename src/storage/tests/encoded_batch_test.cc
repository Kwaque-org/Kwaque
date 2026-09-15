#include "src/storage/tests/segment_test_support.h"

#include <gtest/gtest.h>

namespace kwaque::storage {
namespace {
using namespace testing;
using bytes::fragmented_buffer;
using bytes::fragmented_buffer_parser;

TEST(EncodedBatchTest, ExactValidationRetainsBytesAndCheckedOriginalFacts) {
    for (const bool compressed : {false, true}) {
        for (const bool sparse : {false, true}) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            const auto wire = assigned_wire(compressed, 40, sparse);
            auto input = buffer(wire, 7);
            const auto original = input.fragment_at(0)->data();
            const auto memory = reserve(input, work);
            auto result = validate_encoded_assigned_batch(
                            std::move(input), batch_expected(), memory, work)
                            .get();
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(flat(result->bytes()), wire);
            EXPECT_EQ(result->bytes().fragment_at(0)->data(), original);
            // Consuming entry empties the source by contract.
            // NOLINTNEXTLINE(bugprone-use-after-move)
            EXPECT_TRUE(input.empty());
            const auto info = result->info();
            EXPECT_EQ(
              info.context.submitted().id().producer(),
              id<model::producer_id>(0x40));
            EXPECT_EQ(info.context.submitted().id().epoch().value(), 1U);
            EXPECT_EQ(info.context.submitted().id().stream().value(), 1U);
            EXPECT_EQ(info.context.submitted().id().sequence().value(), 0U);
            EXPECT_EQ(
              info.context.submitted().binding().segment(),
              id<model::segment_id>(0x30));
            EXPECT_EQ(info.context.logical_span().begin().value(), 100U);
            EXPECT_EQ(
              info.context.logical_span().end().value(), sparse ? 105U : 101U);
            EXPECT_EQ(info.context.retained_count().value(), sparse ? 2U : 1U);
            EXPECT_EQ(
              info.context.submitted().original_count().value(),
              sparse ? 5U : 1U);
            EXPECT_EQ(info.header_count, item_count{});
            EXPECT_EQ(
              info.verification,
              sparse ? model::batch_fingerprint_verification::carried
                     : model::batch_fingerprint_verification::recomputed);
        }
    }
}

TEST(EncodedBatchTest, ReuseNeedsNoNewAliasAndNarrowerPolicyIsRevalidated) {
    seastar::abort_source abort;
    codec::cooperative_work prepare{codec::limits::defaults(), abort};
    auto child = checked_child(prepare, true, 40);
    const auto original = flat(child.bytes());
    const codec::decode_budget no_new_allocations{{}, {}, charge};
    EXPECT_TRUE(
      child.validate(batch_expected(), no_new_allocations, prepare).get());
    auto wrong = batch_expected();
    wrong.topic = id<model::topic_id>(0x11);
    const auto mismatch
      = child.validate(wrong, no_new_allocations, prepare).get();
    ASSERT_FALSE(mismatch.has_value());
    EXPECT_EQ(mismatch.error().code(), errc::wrong_context);
    auto config = codec::limits_config{};
    config.max_record_bytes = byte_count{7};
    codec::cooperative_work exact{codec::limits::make(config).value(), abort};
    EXPECT_TRUE(child.validate(batch_expected(), budget(), exact).get());
    EXPECT_TRUE(
      child.validate(batch_expected(), no_new_allocations, exact).get());
    config.max_record_bytes = byte_count{6};
    codec::cooperative_work narrow{codec::limits::make(config).value(), abort};
    const auto rejected
      = child.validate(batch_expected(), budget(), narrow).get();
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().code(), errc::resource_exhausted);
    EXPECT_EQ(flat(child.bytes()), original);
    EXPECT_TRUE(
      child.validate(batch_expected(), no_new_allocations, prepare).get());
    config = codec::limits_config{};
    config.max_header_bytes = byte_count{32};
    codec::cooperative_work short_header{
      codec::limits::make(config).value(), abort};
    const auto extension
      = child.validate(batch_expected(), budget(), short_header).get();
    ASSERT_FALSE(extension.has_value());
    EXPECT_EQ(extension.error().code(), errc::resource_exhausted);
}

TEST(
  EncodedBatchTest, EncodingRawOwnerDoesNotClaimAnUnprovedNarrowRecordLimit) {
    for (const auto encoding :
         {compression::codec_id::none, compression::codec_id::lz4}) {
        for (const bool narrow : {false, true}) {
            seastar::abort_source abort;
            codec::cooperative_work prepare{codec::limits::defaults(), abort};
            fragmented_buffer_parser input{buffer(assigned_wire())};
            auto raw
              = model::decode_assigned_batch(
                  input, batch_expected(), reserve(input, prepare), prepare)
                  .get()
                  .value();
            auto config = codec::limits_config{};
            if (narrow) config.max_record_bytes = byte_count{6};
            codec::cooperative_work work{
              codec::limits::make(config).value(), abort};
            auto encoded = make_encoded_assigned_batch(
                             std::move(raw.value),
                             encoding,
                             work,
                             budget().operation_remaining,
                             charge)
                             .get();
            if (narrow) {
                ASSERT_FALSE(encoded.has_value());
                EXPECT_EQ(encoded.error().code(), errc::resource_exhausted);
            } else {
                ASSERT_TRUE(encoded.has_value());
                EXPECT_TRUE(
                  encoded->validate(batch_expected(), {{}, {}, charge}, work)
                    .get());
                const auto wire = flat(encoded->bytes());
                EXPECT_EQ(
                  get(wire, 32 + 156, 1), static_cast<std::uint8_t>(encoding));
                if (encoding == compression::codec_id::none)
                    EXPECT_EQ(wire, assigned_wire());
            }
        }
    }
}

TEST(EncodedBatchTest, AdmittedShareRetainsExactBytesAfterOriginalDies) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto shared = [&] {
        auto child = checked_child(work, true, 40);
        const auto failed = child.share({{}, {}, charge}, work).get();
        EXPECT_FALSE(failed.has_value());
        return child.share(budget(), work).get().value();
    }();
    EXPECT_EQ(flat(shared.bytes()), assigned_wire(true, 40));
    const auto extracted = std::move(shared).release_bytes();
    // Extraction leaves a readable, empty donor; it cannot be reused as data.
    // NOLINTBEGIN(bugprone-use-after-move)
    const auto rejected
      = shared.validate(batch_expected(), budget(), work).get();
    // NOLINTEND(bugprone-use-after-move)
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().code(), errc::invalid_argument);
    EXPECT_EQ(flat(extracted), assigned_wire(true, 40));
}

TEST(
  EncodedBatchTest, ExtraEnvelopeTruncationAndCancellationCannotCertifyBytes) {
    for (int mode = 0; mode < 4; ++mode) {
        auto wire = assigned_wire(true);
        if (mode == 0) wire += assigned_wire();
        if (mode == 1) wire.pop_back();
        if (mode == 2) wire.clear();
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto input = buffer(wire);
        const auto memory = reserve(input, work);
        if (mode == 3) abort.request_abort();
        const auto result = validate_encoded_assigned_batch(
                              std::move(input), batch_expected(), memory, work)
                              .get();
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(
          result.error().code(),
          mode == 3 ? errc::aborted : errc::malformed_data);
        // NOLINTNEXTLINE(bugprone-use-after-move)
        EXPECT_TRUE(input.empty());
    }
}
} // namespace
} // namespace kwaque::storage
