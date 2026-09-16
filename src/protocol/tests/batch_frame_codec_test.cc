#include "src/codec/crc32c.h"
#include "src/model/tests/record_fuzz_oracle.h"
#include "src/protocol/batch_frame_codec.h"
#include "src/protocol/tests/batch_frame_test_support.h"
#include "src/protocol/tests/frame_test_support.h"

#include <seastar/core/abort_source.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <gtest/gtest.h>

#include <array>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <variant>

namespace {
using namespace kwaque;
namespace protocol = kwaque::protocol;
namespace fixture = protocol::testing::frame_fixture;
namespace oracle = model::testing;
using bytes::fragmented_buffer_parser;

using protocol::testing::batch_frame_fixture::batch_wire;
using protocol::testing::batch_frame_fixture::binding;
using protocol::testing::batch_frame_fixture::expected;
using protocol::testing::batch_frame_fixture::frame;
using protocol::testing::batch_frame_fixture::id;
using protocol::testing::batch_frame_fixture::records;

codec::decode_budget
reserve(const fragmented_buffer_parser& input, codec::cooperative_work& work) {
    return codec::reserve_decode_input(
             input,
             work.policy(),
             {fixture::parent_budget,
              work.policy().config().max_metadata_bytes,
              fixture::charge},
             fixture::context)
      .value();
}
auto submitted(
  fragmented_buffer_parser& input,
  codec::cooperative_work& work,
  model::batch_decode_expectation expectation = expected()) {
    return protocol::decode_submitted_frame(
             input,
             std::move(expectation),
             fixture::bounds,
             reserve(input, work),
             work,
             fixture::context)
      .get();
}
auto assigned(
  fragmented_buffer_parser& input,
  codec::cooperative_work& work,
  model::batch_decode_expectation expectation = expected()) {
    return protocol::decode_assigned_frame(
             input,
             std::move(expectation),
             fixture::bounds,
             reserve(input, work),
             work,
             fixture::context)
      .get();
}
void expect_error(const auto& result, errc code) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), code);
}

TEST(
  BatchFrameCodecTest,
  SubmittedNoneAndLz4RetainTheOriginalIdentityAndOwnRecords) {
    for (bool compressed : {false, true}) {
        const auto batch = batch_wire(false, compressed);
        ASSERT_EQ(
          oracle::probe_batch(batch, false, true, false).error, errc::success);
        const auto wire = frame(batch);
        for (std::size_t split = 0; split <= wire.size(); ++split) {
            std::optional<protocol::decoded_submitted_frame> owner;
            {
                fragmented_buffer_parser input{
                  fixture::split_at("pre" + wire + "next", split)};
                ASSERT_TRUE(input.skip(byte_count{3}).has_value());
                seastar::abort_source abort;
                codec::cooperative_work work{codec::limits::defaults(), abort};
                auto result = submitted(input, work);
                ASSERT_TRUE(result.has_value());
                ASSERT_TRUE(
                  std::holds_alternative<protocol::decoded_submitted_frame>(
                    *result));
                owner.emplace(
                  std::get<protocol::decoded_submitted_frame>(
                    std::move(*result)));
                EXPECT_EQ(input.bytes_consumed(), byte_count{3 + wire.size()});
                EXPECT_EQ(input.bytes_remaining(), byte_count{4});
            }
            ASSERT_TRUE(owner.has_value());
            EXPECT_TRUE(owner->batch.records().content_equals(records()));
            EXPECT_EQ(owner->batch.context().binding(), binding());
            EXPECT_EQ(owner->batch.context().id().stream().value(), 3U);
            EXPECT_EQ(owner->header.metadata.stream, fixture::metadata.stream);
        }
    }
}

TEST(
  BatchFrameCodecTest,
  AssignedDenseAndSparsePreserveSpanAndVerificationMeaning) {
    for (bool compressed : {false, true}) {
        for (bool sparse : {false, true}) {
            const auto batch = batch_wire(true, compressed, sparse);
            ASSERT_EQ(
              oracle::probe_batch(batch, true, true, false).error,
              errc::success);
            fragmented_buffer_parser input{
              fixture::fragmented(frame(batch, true), 7)};
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto result = assigned(input, work);
            ASSERT_TRUE(result.has_value());
            auto* ready = std::get_if<protocol::decoded_assigned_frame>(
              &*result);
            ASSERT_NE(ready, nullptr);
            EXPECT_TRUE(input.at_end());
            EXPECT_TRUE(ready->batch.records().content_equals(records(sparse)));
            EXPECT_EQ(ready->batch.context().submitted().binding(), binding());
            EXPECT_EQ(
              ready->batch.context().logical_span().begin().value(), 10U);
            EXPECT_EQ(ready->batch.context().logical_span().end().value(), 13U);
            EXPECT_EQ(
              ready->batch.context().retained_count().value(),
              sparse ? 2U : 3U);
            EXPECT_EQ(
              ready->fingerprint_verification,
              sparse ? model::batch_fingerprint_verification::carried
                     : model::batch_fingerprint_verification::recomputed);
        }
    }
}

TEST(
  BatchFrameCodecTest,
  ExactChildRejectsTwoBatchesTrailingBytesAndInnerOverruns) {
    const auto batch = batch_wire(false);
    auto oversized = batch;
    oracle::put(oversized, 12, oracle::little(oversized, 12, 4) + 1, 4);
    oracle::repair_crc(oversized);
    const std::array candidates{
      batch + batch,
      batch + "x",
      oversized,
      batch.substr(0, batch.size() - 1),
      std::string{}};
    for (const auto& body : candidates) {
        fragmented_buffer_parser input{
          fixture::fragmented("pre" + frame(body) + frame(batch), 7)};
        for (unsigned i = 0; i < 7; ++i)
            ASSERT_TRUE(input.push_checkpoint().has_value());
        ASSERT_TRUE(input.skip(byte_count{3}).has_value());
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        expect_error(submitted(input, work), errc::malformed_data);
        EXPECT_EQ(input.bytes_consumed(), byte_count{3});
        EXPECT_EQ(input.checkpoint_depth(), 7U);
    }
}

TEST(BatchFrameCodecTest, WrongFrameKindFamilyAndExpectedContextNeverCommit) {
    const auto batch = batch_wire(false);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    fragmented_buffer_parser wrong_kind{
      fixture::fragmented(frame(batch, true), 7)};
    expect_error(submitted(wrong_kind, work), errc::wrong_context);
    EXPECT_EQ(wrong_kind.bytes_consumed(), byte_count{});
    fragmented_buffer_parser wrong_family{
      fixture::fragmented(frame(batch_wire(true)), 7)};
    expect_error(submitted(wrong_family, work), errc::wrong_context);
    EXPECT_EQ(wrong_family.bytes_consumed(), byte_count{});
    auto unknown = batch;
    oracle::put(unknown, 4, 11, 2);
    oracle::repair_crc(unknown);
    fragmented_buffer_parser unknown_family{
      fixture::fragmented(frame(unknown), 7)};
    expect_error(submitted(unknown_family, work), errc::unsupported_format);
    fragmented_buffer_parser wrong_topic{fixture::fragmented(frame(batch), 7)};
    auto expectation = expected();
    expectation.topic = id<model::topic_id>(34);
    expect_error(
      submitted(wrong_topic, work, expectation), errc::wrong_context);
    EXPECT_EQ(wrong_topic.bytes_consumed(), byte_count{});
    fragmented_buffer_parser wrong_binding{
      fixture::fragmented(frame(batch), 7)};
    expectation = expected();
    expectation.original_binding = binding(98);
    expect_error(
      submitted(wrong_binding, work, expectation), errc::wrong_context);
    EXPECT_EQ(wrong_binding.bytes_consumed(), byte_count{});
    fragmented_buffer_parser wrong_identity{
      fixture::fragmented(frame(batch), 7)};
    expectation = expected();
    expectation.id = model::batch_id::make(
                       id<model::producer_id>(2),
                       model::producer_epoch::make(2).value(),
                       model::producer_stream_id::make(3).value(),
                       model::batch_sequence{4})
                       .value();
    expect_error(
      submitted(wrong_identity, work, expectation), errc::wrong_context);
    fragmented_buffer_parser wrong_digest{
      fixture::fragmented(frame(batch_wire(true, true, true), true), 7)};
    expectation = expected();
    expectation.fingerprint = codec::semantic_batch_digest{
      codec::sha256_digest{}};
    expect_error(
      assigned(wrong_digest, work, expectation), errc::wrong_context);
    EXPECT_EQ(wrong_digest.bytes_consumed(), byte_count{});
}

TEST(
  BatchFrameCodecTest,
  FramingSuccessDoesNotAuthorizeInvalidRawOrControlPayloads) {
    const std::string proto_bytes{"\x08\x01", 2};
    const auto wire = frame(proto_bytes);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    fragmented_buffer_parser raw{fixture::fragmented(wire, 1)};
    const auto framed
      = protocol::decode_frame(
          raw, fixture::bounds, reserve(raw, work), work, fixture::context)
          .get();
    ASSERT_TRUE(framed.has_value());
    ASSERT_TRUE(std::holds_alternative<protocol::framed_payload>(*framed));
    fragmented_buffer_parser typed{fixture::fragmented(wire, 1)};
    expect_error(submitted(typed, work), errc::malformed_data);
    EXPECT_EQ(typed.bytes_consumed(), byte_count{});
    auto control = frame(batch_wire(false));
    fixture::put_u16(control, 6, 1);
    fixture::put_u64(control, 16, 0);
    fixture::repair(control);
    fragmented_buffer_parser wrong_type{fixture::fragmented(control, 7)};
    expect_error(submitted(wrong_type, work), errc::wrong_context);
}

TEST(BatchFrameCodecTest, NilExpectationsRejectEvenBeforeAnIncompleteFrame) {
    fragmented_buffer_parser input;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto expectation = expected();
    expectation.topic = model::topic_id{};
    expect_error(submitted(input, work, expectation), errc::invalid_argument);
    abort.request_abort();
    expect_error(submitted(input, work, expectation), errc::aborted);
}

TEST(
  BatchFrameCodecTest,
  TypedIncompletePayloadReturnsAnExactHintAndUsesOnlyOneParentMark) {
    const auto wire = frame(batch_wire(true, true, true), true);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    for (std::size_t cut :
         {std::size_t{0},
          std::size_t{47},
          std::size_t{48},
          wire.size() - 1,
          wire.size()}) {
        fragmented_buffer_parser input{
          fixture::fragmented(wire.substr(0, cut), 7)};
        for (unsigned i = 0; i < 7; ++i)
            ASSERT_TRUE(input.push_checkpoint().has_value());
        const auto result = assigned(input, work);
        ASSERT_TRUE(result.has_value());
        if (cut == wire.size()) {
            EXPECT_TRUE(
              std::holds_alternative<protocol::decoded_assigned_frame>(
                *result));
            EXPECT_TRUE(input.at_end());
        } else {
            ASSERT_TRUE(std::holds_alternative<protocol::need_more>(*result));
            EXPECT_EQ(
              std::get<protocol::need_more>(*result).additional_bytes,
              byte_count{(cut < 48 ? 48 : wire.size()) - cut});
            EXPECT_EQ(input.bytes_consumed(), byte_count{});
        }
        EXPECT_EQ(input.checkpoint_depth(), 7U);
    }
}

TEST(
  BatchFrameCodecTest, TemporaryPayloadDescriptorsAreRefundedAfterBatchDecode) {
    for (bool compressed : {false, true}) {
        fragmented_buffer_parser input{
          fixture::fragmented(frame(batch_wire(false, compressed)), 7)};
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto memory = reserve(input, work);
        auto result = protocol::decode_submitted_frame(
                        input,
                        expected(),
                        fixture::bounds,
                        memory,
                        work,
                        fixture::context)
                        .get();
        ASSERT_TRUE(result.has_value());
        auto* ready = std::get_if<protocol::decoded_submitted_frame>(&*result);
        ASSERT_NE(ready, nullptr);
        // Compare the model decoder on the identical fragment boundaries.
        // Its residual includes its own conservative retained charges; the
        // frame adapter must preserve those and remove only its outer alias.
        const auto inner = batch_wire(false, compressed);
        fragmented_buffer_parser direct_parent{
          fixture::fragmented(frame(inner), 7)};
        const auto direct_memory = reserve(direct_parent, work);
        ASSERT_TRUE(direct_parent.skip(byte_count{48}).has_value());
        const auto alias_cost = direct_parent
                                  .next_buffer_allocation_cost(
                                    byte_count{inner.size()}, fixture::charge)
                                  .value();
        const auto child_memory = codec::detail::consume_decode_budget(
                                    work.policy(),
                                    direct_memory,
                                    byte_count{},
                                    alias_cost.descriptors,
                                    fixture::context,
                                    fixture::context.origin)
                                    .value();
        fragmented_buffer_parser child{
          direct_parent.read_buffer(byte_count{inner.size()}).value()};
        auto direct = model::decode_submitted_batch(
                        child, expected(), child_memory, work)
                        .get();
        ASSERT_TRUE(direct.has_value());
        EXPECT_EQ(
          memory.metadata_remaining.value()
            - ready->remaining.metadata_remaining.value(),
          child_memory.metadata_remaining.value()
            - direct->remaining.metadata_remaining.value());
        EXPECT_EQ(
          memory.operation_remaining.value()
            - ready->remaining.operation_remaining.value(),
          child_memory.operation_remaining.value()
            - direct->remaining.operation_remaining.value());
    }
}

TEST(
  BatchFrameCodecTest,
  NativeAllocationFailureInsideTheBatchRollsBackTheOuterFrame) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation failure injection is disabled";
#else
    // Fixture construction initializes providers before injection. Fresh SHA
    // contexts still allocate on each decode and report failure through the
    // native hash exception channel, not necessarily std::bad_alloc.
    const auto wire = frame(batch_wire(false));
    static_cast<void>(kwaque::error_category());
    codec::crc32c warm;
    const std::array<char, 65> warm_bytes{};
    warm.extend(warm_bytes);
    unsigned failures = 0;
    unsigned hash_failures = 0;
    bool completed = false;
    for (std::uint64_t ordinal = 0; ordinal < 512; ++ordinal) {
        fragmented_buffer_parser input{fixture::fragmented("pre" + wire, 7)};
        ASSERT_TRUE(input.skip(byte_count{3}).has_value());
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto memory = reserve(input, work);
        std::optional<
          protocol::frame_read_result<protocol::decoded_submitted_frame>>
          result;
        bool allocation_failed = false;
        auto& injector = seastar::memory::local_failure_injector();
        injector.fail_after(ordinal);
        try {
            result.emplace(
              protocol::decode_submitted_frame(
                input,
                expected(),
                fixture::bounds,
                memory,
                work,
                fixture::context)
                .get());
        } catch (const std::bad_alloc&) {
            allocation_failed = true;
        } catch (const std::runtime_error&) {
            if (!injector.failed()) {
                injector.cancel();
                throw;
            }
            ++hash_failures;
            allocation_failed = true;
        } catch (...) {
            injector.cancel();
            throw;
        }
        const bool injected = injector.failed();
        injector.cancel();
        EXPECT_EQ(input.checkpoint_depth(), 0U);
        if (injected) {
            ++failures;
            EXPECT_TRUE(allocation_failed);
            EXPECT_FALSE(result.has_value());
            EXPECT_EQ(input.bytes_consumed(), byte_count{3});
        } else {
            ASSERT_TRUE(result.has_value());
            ASSERT_TRUE(result->has_value());
            ASSERT_TRUE(
              std::holds_alternative<protocol::decoded_submitted_frame>(
                **result));
            EXPECT_EQ(input.bytes_consumed(), byte_count{3 + wire.size()});
            completed = true;
            break;
        }
    }
    EXPECT_GT(failures, 0U);
    EXPECT_GT(hash_failures, 0U);
    EXPECT_TRUE(completed);
#endif
}

} // namespace
