#include "src/bytes/fragmented_buffer_test_support.h"
#include "src/storage/tests/footer_test_support.h"

#include <seastar/core/preempt.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <type_traits>

namespace kwaque::storage {
namespace {
using namespace testing;
using bytes::fragmented_buffer_parser;
static_assert(!std::is_copy_constructible_v<extent_verifier>);
static_assert(!std::is_move_assignable_v<extent_verifier>);
static_assert(sizeof(extent_verifier) < 2048);

TEST(
  ExtentVerifierTest, TwoGroupsHashEarlierFooterBytesOnceAndCountOnlyBlocks) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto verifier = extent_verifier::make(
                      history(),
                      scope(100, 102, 0, 2, 512, 2048),
                      work.policy())
                      .value();
    const auto first = data_block();
    ASSERT_TRUE(feed_block(verifier, first, work));
    const auto first_evidence = verifier.checkpoint(work).value();
    const auto middle = footer_wire(
      first_evidence.boundary(), {history(), runtime::file_position{1024}});
    ASSERT_TRUE(feed_footer(verifier, middle, work, first_evidence));
    const auto middle_evidence = verifier.checkpoint(work).value();
    EXPECT_EQ(middle_evidence.boundary().block_count, 1U);
    EXPECT_EQ(middle_evidence.boundary().coverage.physical().end().value(), 1U);
    EXPECT_EQ(
      middle_evidence.boundary().last_block->bytes().end().value(), 1024U);
    EXPECT_EQ(middle_evidence.boundary().coverage.bytes().end().value(), 1536U);
    EXPECT_EQ(middle_evidence.boundary().data_crc32c, crc(first + middle));
    // A last block may precede an included earlier footer at the data end.
    EXPECT_TRUE(encode_durable_footer(
                  middle_evidence,
                  {history(), runtime::file_position{1536}},
                  work,
                  budget().operation_remaining,
                  charge)
                  .get());
    const auto second = data_block(101, 1, 1536);
    ASSERT_TRUE(feed_block(verifier, second, work));
    const auto evidence = verifier.finish(work).value();
    EXPECT_TRUE(verifier.closed());
    EXPECT_EQ(evidence.boundary().block_count, 2U);
    EXPECT_EQ(
      evidence.boundary().last_block,
      std::optional{scope(101, 102, 1, 2, 1536, 2048)});
    EXPECT_EQ(evidence.boundary().data_crc32c, crc(first + middle + second));
    EXPECT_NE(evidence.boundary().data_crc32c, crc(first + second));
    const footer_expectation expected{history(), runtime::file_position{2560}};
    fragmented_buffer_parser input{
      buffer(footer_wire(evidence.boundary(), expected))};
    const auto footer = decode_durable_footer(
                          input, expected, reserve(input, work), work)
                          .get()
                          .value();
    EXPECT_TRUE(validate_durable_footer(footer, evidence));
}

TEST(
  ExtentVerifierTest,
  SkippedDuplicatedReorderedAndPartialObjectsCloseTheVerifier) {
    for (int mode = 0; mode < 6; ++mode) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto verifier = extent_verifier::make(
                          history(),
                          scope(100, 102, 0, 2, 512, 1536),
                          work.policy())
                          .value();
        if (mode == 0) {
            EXPECT_FALSE(feed_block(verifier, data_block(101, 1, 1024), work));
        } else {
            ASSERT_TRUE(feed_block(verifier, data_block(), work));
            auto next = data_block(101, 1, 1024);
            if (mode == 1) next = data_block();
            if (mode == 2) next = data_block(102, 1, 1024);
            if (mode == 3) next = data_block(101, 2, 1024);
            if (mode == 4) next.pop_back();
            if (mode == 5) next = data_block(101, 1, 1024, false, 0x30, 2);
            EXPECT_FALSE(feed_block(verifier, next, work));
        }
        EXPECT_TRUE(verifier.closed());
        const auto result = verifier.finish(work);
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code(), errc::closed);
    }
}

TEST(ExtentVerifierTest, ClaimedSpanCannotOmitTailOrAcceptExtraObjects) {
    for (const bool extra : {false, true}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto verifier = extent_verifier::make(
                          history(),
                          scope(
                            100,
                            extra ? 101U : 102U,
                            0,
                            extra ? 1U : 2U,
                            512,
                            extra ? 1024U : 1536U),
                          work.policy())
                          .value();
        ASSERT_TRUE(feed_block(verifier, data_block(), work));
        if (extra)
            EXPECT_FALSE(feed_block(verifier, data_block(101, 1, 1024), work));
        const auto proof = verifier.finish(work);
        EXPECT_FALSE(proof.has_value());
        EXPECT_TRUE(verifier.closed());
    }
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto joined = extent_verifier::make(
                    history(), scope(100, 102, 0, 2, 512, 1536), work.policy())
                    .value();
    EXPECT_FALSE(
      feed_block(joined, data_block() + data_block(101, 1, 1024), work));
    EXPECT_TRUE(joined.closed());
}

TEST(ExtentVerifierTest, OriginalAndRewriteHistoriesHaveDifferentLogicalRules) {
    for (const bool rewrite : {false, true}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto verifier = extent_verifier::make(
                          history(0x80, 2),
                          scope(100, 115, 0, 4, 512, 1536),
                          work.policy(),
                          rewrite ? extent_layout_kind::rewrite
                                  : extent_layout_kind::initial_append)
                          .value();
        const auto first = data_block(102, 0, 512, true, 0x80, 2, true);
        const auto accepted = feed_block(verifier, first, work);
        if (!rewrite) {
            EXPECT_FALSE(accepted.has_value());
            EXPECT_TRUE(verifier.closed());
            continue;
        }
        ASSERT_TRUE(accepted.has_value());
        ASSERT_TRUE(
          feed_block(verifier, data_block(109, 2, 1024, true, 0x80, 2), work));
        const auto proof = verifier.finish(work).value();
        EXPECT_EQ(proof.boundary().coverage, scope(100, 115, 0, 4, 512, 1536));
        EXPECT_EQ(proof.boundary().block_count, 2U);
        EXPECT_EQ(proof.boundary().last_block->logical().end().value(), 114U);
        // Trailing original coverage belongs to a rewritten extent, not a
        // fabricated last retained batch or an originally empty boundary.
        EXPECT_FALSE(encode_durable_footer(
                       proof,
                       {history(0x80, 2), runtime::file_position{1536}},
                       work,
                       budget().operation_remaining,
                       charge)
                       .get());
    }
}

TEST(
  ExtentVerifierTest, RewriteCannotInferRemovedCoverageOrEscapeSuppliedBounds) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto verifier = extent_verifier::make(
                      history(0x80, 2),
                      scope(100, 105, 0, 2, 512, 1024),
                      work.policy(),
                      extent_layout_kind::rewrite)
                      .value();
    EXPECT_FALSE(
      feed_block(verifier, data_block(101, 0, 512, true, 0x80, 2), work));
    EXPECT_TRUE(verifier.closed());
    auto empty = extent_verifier::make(
                   history(),
                   scope(100, 105, 0, 0, 512, 512),
                   work.policy(),
                   extent_layout_kind::rewrite)
                   .value();
    const auto proof = empty.finish(work).value();
    EXPECT_EQ(proof.boundary().coverage.logical().count().value(), 5U);
    EXPECT_FALSE(proof.boundary().last_block.has_value());
    EXPECT_EQ(proof.boundary().block_count, 0U);
    EXPECT_EQ(proof.boundary().data_crc32c, 0U);
    EXPECT_FALSE(
      extent_verifier::make(
        history(), scope(100, 105, 0, 0, 512, 512), work.policy()));
}

TEST(ExtentVerifierTest, EarlierFooterReferencesCannotWidenCurrentEvidence) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto previous = single_evidence(work);
    auto claim = previous.boundary();
    claim.data_crc32c ^= 1U;
    const auto literal = footer_wire(
      claim, {history(), runtime::file_position{1024}});
    // This walk supplies the footer object, not its earlier data interval.
    auto structural = extent_verifier::make(
                        history(),
                        scope(100, 100, 0, 0, 1024, 1536),
                        work.policy())
                        .value();
    ASSERT_TRUE(feed_footer(structural, literal, work));
    const auto proof = structural.finish(work).value();
    EXPECT_EQ(proof.boundary().coverage.bytes().begin().value(), 1024U);
    EXPECT_EQ(proof.boundary().block_count, 0U);
    EXPECT_EQ(proof.boundary().data_crc32c, crc(literal));
    auto checked = extent_verifier::make(
                     history(),
                     scope(100, 100, 0, 0, 1024, 1536),
                     work.policy())
                     .value();
    const auto failed = feed_footer(checked, literal, work, previous);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code(), errc::corrupt_data);
    EXPECT_TRUE(checked.closed());
}

TEST(ExtentVerifierTest, KnownPrefixFooterCannotLieAboutSuppliedHistory) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto verifier = extent_verifier::make(
                      history(),
                      scope(100, 101, 0, 1, 512, 1536),
                      work.policy())
                      .value();
    ASSERT_TRUE(feed_block(verifier, data_block(), work));
    auto claim = verifier.checkpoint(work).value().boundary();
    claim.data_crc32c ^= 1U;
    EXPECT_FALSE(feed_footer(
      verifier,
      footer_wire(claim, {history(), runtime::file_position{1024}}),
      work));
    EXPECT_TRUE(verifier.closed());
}

TEST(
  ExtentVerifierTest, SnapshotIsStableAndMovingOrClosingLeavesNoUsableSource) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto verifier = extent_verifier::make(
                      history(),
                      scope(100, 102, 0, 2, 512, 1536),
                      work.policy())
                      .value();
    ASSERT_TRUE(feed_block(verifier, data_block(), work));
    const auto prefix = verifier.checkpoint(work).value();
    auto moved = std::move(verifier);
    // A moved verifier is explicitly closed by contract.
    // NOLINTNEXTLINE(bugprone-use-after-move)
    EXPECT_TRUE(verifier.closed());
    ASSERT_TRUE(feed_block(moved, data_block(101, 1, 1024), work));
    const auto all = moved.finish(work).value();
    EXPECT_EQ(all.boundary().block_count, 2U);
    EXPECT_EQ(prefix.boundary().block_count, 1U);
    EXPECT_EQ(prefix.boundary().coverage.bytes().end().value(), 1024U);
    EXPECT_FALSE(moved.checkpoint(work));
}

TEST(
  ExtentVerifierTest, AdmissionFailureAndRealSuspensionCancellationCloseState) {
    for (const bool cancel : {false, true}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto verifier = extent_verifier::make(
                          history(),
                          scope(100, 101, 0, 1, 512, 1024),
                          work.policy(),
                          extent_layout_kind::initial_append,
                          {},
                          extent_integrity::crc32c_and_sha256)
                          .value();
        auto input = buffer(data_block(), 1);
        auto memory = reserve(input, work);
        if (!cancel) memory.metadata_remaining = {};
        if (cancel) {
            const auto deadline = std::chrono::steady_clock::now()
                                  + std::chrono::seconds{2};
            while (!seastar::need_preempt()
                   && std::chrono::steady_clock::now() < deadline) {
            }
            ASSERT_TRUE(seastar::need_preempt());
        }
        auto pending = verifier.add_block(
          std::move(input), batch_expected(), memory, work);
        const bool suspended = !pending.available();
        if (cancel) abort.request_abort();
        const auto result = pending.get();
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(
          result.error().code(),
          cancel ? errc::aborted : errc::resource_exhausted);
        if (cancel) EXPECT_TRUE(suspended);
        EXPECT_TRUE(verifier.closed());
        EXPECT_FALSE(verifier.finish(work));
    }
}

TEST(ExtentVerifierTest, AllocationFailuresCloseEvenAfterAValidPrefix) {
#ifndef SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION
    GTEST_SKIP() << "allocation failure injection is unavailable";
#else
    bool completed = false;
    std::size_t failures = 0;
    for (std::uint64_t ordinal = 0; ordinal < 384 && !completed; ++ordinal) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto verifier = extent_verifier::make(
                          history(),
                          scope(100, 102, 0, 2, 512, 1536),
                          work.policy(),
                          extent_layout_kind::initial_append,
                          {},
                          extent_integrity::crc32c_and_sha256)
                          .value();
        ASSERT_TRUE(feed_block(verifier, data_block(), work));
        auto bytes = buffer(data_block(101, 1, 1024, false, 0x30, 1, true));
        const auto memory = reserve(bytes, work);
        auto& injector = seastar::memory::local_failure_injector();
        bool threw = false, succeeded = false;
        injector.fail_after(ordinal);
        try {
            succeeded = verifier
                          .add_block(
                            std::move(bytes), batch_expected(), memory, work)
                          .get()
                          .has_value();
        } catch (const std::bad_alloc&) {
            threw = true;
        } catch (const std::runtime_error&) {
            if (!injector.failed()) {
                injector.cancel();
                throw;
            }
            threw = true;
        } catch (...) {
            injector.cancel();
            throw;
        }
        const bool reached = injector.failed();
        injector.cancel();
        if (threw) {
            EXPECT_TRUE(reached);
            ++failures;
            // A failure before coroutine-frame entry leaves the donor and
            // verifier untouched. Once transfer occurred, failure closes it.
            // NOLINTNEXTLINE(bugprone-use-after-move)
            if (bytes.empty()) EXPECT_TRUE(verifier.closed());
        } else {
            ASSERT_TRUE(succeeded);
            EXPECT_TRUE(verifier.finish(work));
            completed = !reached;
        }
    }
    EXPECT_TRUE(completed);
    EXPECT_GT(failures, 0U);
#endif
}

TEST(
  ExtentVerifierTest, AbortDuringFinalInputCleanupPreventsPrefixPublication) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto verifier = extent_verifier::make(
                      history(),
                      scope(100, 101, 0, 1, 512, 1024),
                      work.policy(),
                      extent_layout_kind::initial_append,
                      {},
                      extent_integrity::crc32c_and_sha256)
                      .value();
    const auto wire = data_block();
    bool released = false;
    auto allocation = std::make_unique<char[]>(wire.size());
    std::copy(wire.begin(), wire.end(), allocation.get());
    auto* pointer = allocation.get();
    auto deleter = seastar::make_deleter(
      [allocation = std::move(allocation), &abort, &released] {
          static_cast<void>(allocation);
          released = true;
          abort.request_abort();
      });
    auto storage = seastar::temporary_buffer<char>::maybe_unsafe_from_deleter(
      pointer, wire.size(), std::move(deleter));
    auto input = bytes::fragmented_buffer_test_access::adopt_fragment(
                   std::move(storage), byte_count{wire.size()})
                   .value();
    const auto memory = reserve(input, work);
    const auto result
      = verifier.add_block(std::move(input), batch_expected(), memory, work)
          .get();
    EXPECT_TRUE(released);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), errc::aborted);
    EXPECT_TRUE(verifier.closed());
    EXPECT_FALSE(verifier.checkpoint(work));
}

TEST(
  ExtentVerifierTest, LongExtentExceedsOperationBytesWithoutRetainingHistory) {
    constexpr std::uint64_t objects = 1025, width = 65536;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto h = history(0x30, 1, width);
    auto verifier
      = extent_verifier::make(
          h,
          scope(100, 100 + objects, 0, objects, width, (objects + 1) * width),
          work.policy(),
          extent_layout_kind::initial_append,
          {},
          extent_integrity::crc32c_and_sha256)
          .value();
    codec::sha256_hasher expected_sha;
    for (std::uint64_t i = 0; i < objects; ++i) {
        const auto wire = data_block(
          100 + i, i, (i + 1) * width, false, 0x30, 1, false, width);
        ASSERT_EQ(wire.size(), width);
        expected_sha.update(wire.data(), wire.size());
        ASSERT_TRUE(feed_block(verifier, wire, work, width));
        seastar::thread::maybe_yield();
    }
    const auto proof = verifier.finish(work).value();
    EXPECT_EQ(proof.boundary().block_count, objects);
    ASSERT_TRUE(proof.digest());
    EXPECT_EQ(proof.digest()->bytes(), std::move(expected_sha).final());
    EXPECT_GT(
      proof.boundary().coverage.bytes().size(),
      work.policy().config().max_operation_bytes);
}
} // namespace
} // namespace kwaque::storage
