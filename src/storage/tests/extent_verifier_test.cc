#include "src/base/units.h"
#include "src/bytes/fragmented_buffer_test_support.h"
#include "src/codec/xxh3.h"
#include "src/model/record_scan.h"
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
                          extent_integrity::crc32c_and_digest)
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
                          extent_integrity::crc32c_and_digest)
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
                      extent_integrity::crc32c_and_digest)
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
    constexpr std::uint64_t objects = 1025, width = 64_KiB;
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
          extent_integrity::crc32c_and_digest)
          .value();
    codec::xxh3_128_hasher expected_hasher;
    for (std::uint64_t i = 0; i < objects; ++i) {
        const auto wire = data_block(
          100 + i, i, (i + 1) * width, false, 0x30, 1, false, width);
        ASSERT_EQ(wire.size(), width);
        expected_hasher.update(wire.data(), wire.size());
        ASSERT_TRUE(feed_block(verifier, wire, work, width));
        seastar::thread::maybe_yield();
    }
    const auto proof = verifier.finish(work).value();
    EXPECT_EQ(proof.boundary().block_count, objects);
    ASSERT_TRUE(proof.digest());
    EXPECT_EQ(proof.digest()->bytes(), std::move(expected_hasher).final());
    EXPECT_GT(
      proof.boundary().coverage.bytes().size(),
      work.policy().config().max_operation_bytes);
}

TEST(ExtentVerifierTest, FragmentAndWorkBoundariesKeepTheCompleteDigest) {
    // Raw block validation scans records, whose fixed layout needs this much
    // work in one quantum. A smaller quantum rejects before any digest input.
    constexpr std::uint64_t minimum = 4U * sizeof(model::record_layout);
    for (const auto quantum : {minimum - 1U, minimum, std::uint64_t{65536}}) {
        for (const auto width : {17U, 257U, 1023U, 1024U, 1025U, 4096U}) {
            seastar::abort_source abort;
            auto config = codec::limits::defaults().config();
            config.max_work_bytes = byte_count{quantum};
            codec::cooperative_work work{
              codec::limits::make(config).value(), abort};
            const auto h = history(0x30, 1, 8192);
            auto verifier = extent_verifier::make(
                              h,
                              scope(100, 101, 0, 1, 8192, 16384),
                              work.policy(),
                              extent_layout_kind::initial_append,
                              {},
                              extent_integrity::crc32c_and_digest)
                              .value();
            const auto wire = data_block(
              100, 0, 8192, false, 0x30, 1, false, 8192);
            codec::xxh3_128_hasher expected;
            expected.update(wire.data(), wire.size());
            const auto fed = feed_block(verifier, wire, work, width);
            if (quantum < minimum) {
                ASSERT_FALSE(fed);
                EXPECT_EQ(fed.error().code(), errc::resource_exhausted);
                EXPECT_TRUE(verifier.closed());
                continue;
            }
            ASSERT_TRUE(fed);
            const auto proof = verifier.finish(work).value();
            ASSERT_TRUE(proof.digest());
            EXPECT_EQ(proof.digest()->bytes(), std::move(expected).final());
            EXPECT_EQ(proof.boundary().data_crc32c, crc(wire));
        }
    }
}

TEST(
  ExtentVerifierTest, TypedAndRawBlocksProduceTheSameIndependentCrcAndDigest) {
    for (const bool compressed : {false, true}) {
        for (const std::size_t header : {32U, 4096U}) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            const auto wire = block_wire(
              assigned_wire(compressed, header), block_expected(), header);
            fragmented_buffer_parser input{buffer(wire)};
            auto block = decode_segment_block(
                           input, block_expected(), reserve(input, work), work)
                           .get()
                           .value();
            const auto expected = block.value.descriptor().coverage();
            auto typed = extent_verifier::make(
                           history(),
                           expected,
                           work.policy(),
                           extent_layout_kind::initial_append,
                           {},
                           extent_integrity::crc32c_and_digest)
                           .value();
            auto raw = extent_verifier::make(
                         history(),
                         expected,
                         work.policy(),
                         extent_layout_kind::initial_append,
                         {},
                         extent_integrity::crc32c_and_digest)
                         .value();
            // The qualified path has no parser/alias metadata allocation. The
            // source remains separately admitted; hash state is inline.
            const codec::decode_budget no_temporaries{{}, {}, charge};
            const auto accepted
              = typed
                  .add_block(
                    block.value, batch_expected(), no_temporaries, work)
                  .get();
            ASSERT_TRUE(accepted);
            ASSERT_TRUE(feed_block(raw, wire, work));
            const auto a = typed.finish(work).value();
            const auto b = raw.finish(work).value();
            EXPECT_EQ(a.boundary(), b.boundary());
            EXPECT_EQ(a.boundary().data_crc32c, crc(wire));
            codec::xxh3_128_hasher hasher;
            hasher.update(wire.data(), wire.size());
            ASSERT_TRUE(a.digest());
            EXPECT_EQ(a.digest()->bytes(), std::move(hasher).final());
            EXPECT_EQ(a.digest(), b.digest());
            EXPECT_EQ(flat(block.value.bytes()), wire);
        }
    }
}

TEST(ExtentVerifierTest, DeferredDigestMatchesTheEagerWalkOverExactBytes) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto wire = block_wire(
      assigned_wire(false, 32), block_expected(), 32);
    fragmented_buffer_parser input{buffer(wire)};
    const auto expected = decode_segment_block(
                            input, block_expected(), reserve(input, work), work)
                            .get()
                            .value()
                            .value.descriptor()
                            .coverage();
    const auto make = [&](extent_integrity integrity) {
        return extent_verifier::make(
                 history(),
                 expected,
                 work.policy(),
                 extent_layout_kind::initial_append,
                 {},
                 integrity)
          .value();
    };
    constexpr auto deferred = extent_integrity::crc32c_and_deferred_digest;
    auto eager = make(extent_integrity::crc32c_and_digest);
    ASSERT_TRUE(feed_block(eager, wire, work));
    const auto reference = eager.finish(work).value();

    // The digest may trail the CRC walk, in any split, and catch up later.
    // One aligned block can be a single sector, so split inside it.
    ASSERT_GE(wire.size(), 2U);
    const auto half = wire.size() / 2;
    auto verifier = make(deferred);
    auto digest = verifier.deferred_digest().value();
    ASSERT_TRUE(feed_block(verifier, wire, work));
    ASSERT_TRUE(digest.add(buffer(wire.substr(0, half)), work).get());
    ASSERT_TRUE(digest.add(buffer(wire.substr(half), 67), work).get());
    EXPECT_EQ(digest.end(), expected.bytes().end());
    const auto proof = verifier.finish(work, std::move(digest)).value();
    EXPECT_EQ(proof.boundary(), reference.boundary());
    ASSERT_TRUE(proof.digest());
    EXPECT_EQ(proof.digest(), reference.digest());

    {
        // Deferred evidence has no digest without its walk.
        auto missing = make(deferred);
        ASSERT_TRUE(feed_block(missing, wire, work));
        EXPECT_FALSE(missing.finish(work));
        EXPECT_TRUE(missing.closed());
    }
    {
        auto trailing = make(deferred);
        auto walk = trailing.deferred_digest().value();
        ASSERT_TRUE(feed_block(trailing, wire, work));
        ASSERT_TRUE(walk.add(buffer(wire.substr(0, half)), work).get());
        EXPECT_FALSE(trailing.finish(work, std::move(walk)));
        EXPECT_TRUE(trailing.closed());
    }
    {
        // Same length, other bytes: the walk's CRC exposes the substitution.
        auto substituted = make(deferred);
        auto walk = substituted.deferred_digest().value();
        ASSERT_TRUE(feed_block(substituted, wire, work));
        auto changed = wire;
        changed.back() = static_cast<char>(changed.back() ^ 1);
        ASSERT_TRUE(walk.add(buffer(changed), work).get());
        EXPECT_FALSE(substituted.finish(work, std::move(walk)));
    }
    {
        auto twice = make(deferred);
        ASSERT_TRUE(twice.deferred_digest());
        EXPECT_FALSE(twice.deferred_digest());
        EXPECT_TRUE(twice.closed());
        auto late = make(deferred);
        ASSERT_TRUE(feed_block(late, wire, work));
        EXPECT_FALSE(late.deferred_digest());
        auto eager_only = make(extent_integrity::crc32c_and_digest);
        EXPECT_FALSE(eager_only.deferred_digest());
    }
    {
        auto aborted = make(deferred);
        auto walk = aborted.deferred_digest().value();
        seastar::abort_source stop;
        codec::cooperative_work stopped{codec::limits::defaults(), stop};
        stop.request_abort();
        EXPECT_FALSE(walk.add(buffer(wire), stopped).get());
        EXPECT_TRUE(walk.closed());
        EXPECT_FALSE(walk.add(buffer(wire), work).get());
    }
}

TEST(
  ExtentVerifierTest, TypedBlocksRevalidateChangedPolicyAndPreserveTheOwner) {
    seastar::abort_source abort;
    codec::cooperative_work original{codec::limits::defaults(), abort};
    auto block = encode_segment_block(
                   checked_child(original, true),
                   block_expected(),
                   original,
                   budget().operation_remaining,
                   charge)
                   .get()
                   .value();
    const auto wire = flat(block.bytes());
    for (const bool too_narrow : {false, true}) {
        auto config = original.policy().config();
        config.max_record_bytes = byte_count{too_narrow ? 1U : 65536U};
        codec::cooperative_work work{
          codec::limits::make(config).value(), abort};
        auto verifier = extent_verifier::make(
                          history(),
                          block.descriptor().coverage(),
                          work.policy())
                          .value();
        const auto accepted
          = verifier.add_block(block, batch_expected(), budget(), work).get();
        if (too_narrow) {
            EXPECT_FALSE(accepted);
            EXPECT_TRUE(verifier.closed());
        } else {
            ASSERT_TRUE(accepted);
            EXPECT_EQ(
              verifier.finish(work).value().boundary().data_crc32c, crc(wire));
        }
        EXPECT_EQ(flat(block.bytes()), wire);
    }
    auto config = original.policy().config();
    config.max_record_bytes = byte_count{64_KiB};
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    auto verifier = extent_verifier::make(
                      history(), block.descriptor().coverage(), work.policy())
                      .value();
    const auto rejected
      = verifier.add_block(block, batch_expected(), {{}, {}, charge}, work)
          .get();
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code(), errc::resource_exhausted);
    EXPECT_TRUE(verifier.closed());
}

TEST(
  ExtentVerifierTest,
  TypedBlocksRejectWrongIdentityPlacementExtractionAndAbort) {
    for (unsigned mode = 0; mode != 5; ++mode) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto block = encode_segment_block(
                       checked_child(work),
                       block_expected(),
                       work,
                       budget().operation_remaining,
                       charge)
                       .get()
                       .value();
        auto expected = batch_expected();
        auto context = history();
        auto bounds = block.descriptor().coverage();
        if (mode == 0) expected.topic = id<model::topic_id>(0x99);
        if (mode == 1) context = history(0x99);
        if (mode == 2) bounds = scope(100, 101, 0, 1, 1024, 1536);
        bytes::fragmented_buffer extracted;
        if (mode == 3) extracted = std::move(block).release_bytes();
        auto verifier
          = extent_verifier::make(context, bounds, work.policy()).value();
        if (mode == 4) abort.request_abort();
        // Extraction deliberately leaves readable scalar metadata but no bytes.
        // NOLINTBEGIN(bugprone-use-after-move)
        const auto rejected
          = verifier.add_block(block, expected, budget(), work).get();
        // NOLINTEND(bugprone-use-after-move)
        EXPECT_FALSE(rejected);
        EXPECT_TRUE(verifier.closed());
    }
}

TEST(
  ExtentVerifierTest,
  GrowingGroupsMatchFiniteCrcAndDigestIncludingTheirFooters) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto growing = extent_verifier::make(
                     history(),
                     scope(100, 100, 0, 0, 512, 512),
                     work.policy(),
                     extent_layout_kind::initial_append,
                     {},
                     extent_integrity::crc32c_and_digest)
                     .value();
    auto finite = extent_verifier::make(
                    history(),
                    scope(100, 102, 0, 2, 512, 2560),
                    work.policy(),
                    extent_layout_kind::initial_append,
                    {},
                    extent_integrity::crc32c_and_digest)
                    .value();
    ASSERT_TRUE(growing.extend_expected(scope(100, 100, 0, 0, 512, 512), work));
    EXPECT_FALSE(growing.checkpoint(work).value().digest());
    std::string all;
    for (std::uint64_t group = 0; group != 2; ++group) {
        const auto begin = 512 + group * 1024;
        const auto expected = scope(
          100, 101 + group, 0, 1 + group, 512, begin + 1024);
        ASSERT_TRUE(growing.extend_expected(expected, work));
        const auto block = data_block(
          100 + group, group, begin, false, 0x30, 1, group != 0);
        ASSERT_TRUE(feed_block(growing, block, work));
        ASSERT_TRUE(feed_block(finite, block, work));
        all += block;
        const auto prefix = growing.checkpoint(work).value();
        EXPECT_EQ(
          prefix.boundary(), finite.checkpoint(work).value().boundary());
        EXPECT_EQ(prefix.boundary().data_crc32c, crc(all));
        const auto footer = footer_wire(
          prefix.boundary(), {history(), runtime::file_position{begin + 512}});
        ASSERT_TRUE(feed_footer(growing, footer, work, prefix));
        ASSERT_TRUE(feed_footer(finite, footer, work, prefix));
        all += footer;
        const auto after = growing.checkpoint(work).value();
        EXPECT_EQ(after.boundary().block_count, group + 1);
        EXPECT_EQ(after.boundary().data_crc32c, crc(all));
        ASSERT_TRUE(growing.extend_expected(expected, work));
        EXPECT_EQ(
          growing.checkpoint(work).value().boundary(), after.boundary());
    }
    const auto a = growing.finish(work).value();
    const auto b = finite.finish(work).value();
    EXPECT_EQ(a.boundary(), b.boundary());
    EXPECT_EQ(a.digest(), b.digest());
    codec::xxh3_128_hasher hasher;
    hasher.update(all.data(), all.size());
    ASSERT_TRUE(a.digest());
    EXPECT_EQ(a.digest()->bytes(), std::move(hasher).final());
}

TEST(
  ExtentVerifierTest,
  GrowthPermitsEqualAndFooterOnlyBoundsWithoutResettingHistory) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto walk = extent_verifier::make(
                  history(), scope(100, 101, 0, 1, 512, 1024), work.policy())
                  .value();
    const auto block = data_block();
    ASSERT_TRUE(feed_block(walk, block, work));
    const auto prefix = walk.checkpoint(work).value();
    ASSERT_TRUE(walk.extend_expected(prefix.boundary().coverage, work));
    ASSERT_TRUE(walk.extend_expected(scope(100, 101, 0, 1, 512, 1536), work));
    const auto footer = footer_wire(
      prefix.boundary(), {history(), runtime::file_position{1024}});
    ASSERT_TRUE(feed_footer(walk, footer, work, prefix));
    const auto final = walk.finish(work).value();
    EXPECT_EQ(final.boundary().block_count, 1U);
    EXPECT_EQ(
      final.boundary().coverage.logical(),
      prefix.boundary().coverage.logical());
    EXPECT_EQ(
      final.boundary().coverage.physical(),
      prefix.boundary().coverage.physical());
    EXPECT_EQ(final.boundary().data_crc32c, crc(block + footer));
    EXPECT_FALSE(walk.extend_expected(final.boundary().coverage, work));
}

TEST(
  ExtentVerifierTest,
  InvalidOrIncompleteGrowthClosesWithoutPublishingAnotherPrefix) {
    for (unsigned mode = 0; mode != 13; ++mode) {
        SCOPED_TRACE(mode);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto walk = extent_verifier::make(
                      history(),
                      scope(100, 101, 0, 1, 512, 1024),
                      work.policy(),
                      mode == 8 ? extent_layout_kind::rewrite
                                : extent_layout_kind::initial_append)
                      .value();
        if (mode != 7 && mode != 12)
            ASSERT_TRUE(feed_block(walk, data_block(), work));
        auto next = scope(100, 102, 0, 2, 512, 1536);
        if (mode == 0) next = scope(99, 102, 0, 3, 512, 1536);
        if (mode == 1) next = scope(100, 102, 1, 3, 512, 1536);
        if (mode == 2) next = scope(100, 102, 0, 2, 0, 1536);
        if (mode == 3) next = scope(100, 100, 0, 0, 512, 1536);
        if (mode == 4) next = scope(100, 101, 0, 1, 512, 512);
        if (mode == 5) next = scope(100, 102, 0, 2, 512, 1537);
        if (mode == 6) next = scope(100, 103, 0, 2, 512, 1536);
        if (mode == 9) next = scope(100, 102, 0, 2, 512, 1024);
        if (mode == 10) abort.request_abort();
        if (mode == 12) next = scope(100, 101, 0, 1, 512, 1024);
        auto narrower = work.policy().config();
        narrower.max_record_bytes = byte_count{64_KiB};
        codec::cooperative_work other{
          codec::limits::make(narrower).value(), abort};
        const auto rejected = walk.extend_expected(
          next, mode == 11 ? other : work);
        ASSERT_FALSE(rejected);
        EXPECT_EQ(
          rejected.error().code(),
          mode == 10 ? errc::aborted : errc::invalid_argument);
        EXPECT_TRUE(walk.closed());
        EXPECT_FALSE(walk.checkpoint(work));
    }
}

TEST(ExtentVerifierTest, GrowthDuringAnActiveAddCannotDestroyItsHashState) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto walk = extent_verifier::make(
                  history(),
                  scope(100, 101, 0, 1, 512, 1024),
                  work.policy(),
                  extent_layout_kind::initial_append,
                  {},
                  extent_integrity::crc32c_and_digest)
                  .value();
    auto source = buffer(data_block(), 1);
    auto memory = reserve(source, work);
    const auto deadline = std::chrono::steady_clock::now()
                          + std::chrono::seconds{2};
    while (!seastar::need_preempt()
           && std::chrono::steady_clock::now() < deadline) {
    }
    ASSERT_TRUE(seastar::need_preempt());
    auto adding = walk.add_block(
      std::move(source), batch_expected(), memory, work);
    const bool pending = !adding.available();
    const auto grown = walk.extend_expected(
      scope(100, 102, 0, 2, 512, 1536), work);
    const bool kept_open = !walk.closed();
    const auto added = adding.get();
    ASSERT_TRUE(pending);
    EXPECT_FALSE(grown);
    EXPECT_TRUE(kept_open);
    ASSERT_TRUE(added);
    EXPECT_TRUE(walk.finish(work));
}

TEST(ExtentVerifierTest, GrowingToTheTerminalLogicalEndDoesNotOverflow) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto h = history();
    h.logical_origin = model::range_logical_end{UINT64_MAX - 1};
    auto walk = extent_verifier::make(
                  h,
                  scope(UINT64_MAX - 1, UINT64_MAX - 1, 0, 0, 512, 512),
                  work.policy())
                  .value();
    auto child = assigned_wire();
    put(child, 32 + 168, UINT64_MAX - 1, 8);
    put(child, 32 + 176, UINT64_MAX, 8);
    repair(child);
    const auto wire = block_wire(child);
    ASSERT_TRUE(walk.extend_expected(
      scope(UINT64_MAX - 1, UINT64_MAX, 0, 1, 512, 1024), work));
    ASSERT_TRUE(feed_block(walk, wire, work));
    const auto result = walk.finish(work).value();
    EXPECT_EQ(result.boundary().coverage.logical().end().value(), UINT64_MAX);
    EXPECT_EQ(result.boundary().coverage.physical().end().value(), 1U);
}

TEST(
  ExtentVerifierTest, TypedFooterReusesValidationAndMatchesIndependentBytes) {
    for (const auto integrity :
         {extent_integrity::crc32c, extent_integrity::crc32c_and_digest}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto typed = extent_verifier::make(
                       history(),
                       scope(100, 100, 0, 0, 512, 1024),
                       work.policy(),
                       extent_layout_kind::initial_append,
                       {},
                       integrity)
                       .value();
        auto raw = extent_verifier::make(
                     history(),
                     scope(100, 100, 0, 0, 512, 1024),
                     work.policy(),
                     extent_layout_kind::initial_append,
                     {},
                     integrity)
                     .value();
        const auto prefix = typed.checkpoint(work).value();
        auto footer = encode_durable_footer(
                        prefix,
                        {history(), runtime::file_position{512}},
                        work,
                        budget().operation_remaining,
                        charge)
                        .get()
                        .value();
        const auto wire = footer_wire(
          prefix.boundary(), footer.descriptor().location());
        EXPECT_EQ(flat(footer.bytes()), wire);
        // No parser descriptors or alias promotion can fit this budget.
        const codec::decode_budget no_temporaries{{}, {}, charge};
        const auto accepted
          = typed.add_footer(footer, no_temporaries, work, prefix).get();
        ASSERT_TRUE(accepted);
        auto raw_bytes = buffer(wire, 1);
        const auto raw_memory = reserve(raw_bytes, work);
        const auto decoded
          = raw.add_footer(std::move(raw_bytes), raw_memory, work, prefix)
              .get();
        ASSERT_TRUE(decoded);
        const auto a = typed.finish(work).value();
        const auto b = raw.finish(work).value();
        EXPECT_EQ(a.boundary(), b.boundary());
        EXPECT_EQ(a.boundary().data_crc32c, crc(wire));
        EXPECT_EQ(a.digest(), b.digest());
        EXPECT_EQ(flat(footer.bytes()), wire);
        if (a.digest()) {
            codec::xxh3_128_hasher hasher;
            hasher.update(wire.data(), wire.size());
            EXPECT_EQ(a.digest()->bytes(), std::move(hasher).final());
        }
    }
}

TEST(ExtentVerifierTest, TypedFooterCannotBypassPlacementPolicyOrCancellation) {
    for (int mode = 0; mode != 6; ++mode) {
        seastar::abort_source abort;
        codec::cooperative_work original{codec::limits::defaults(), abort};
        auto empty = extent_verifier::make(
                       history(),
                       scope(100, 100, 0, 0, 512, 512),
                       original.policy())
                       .value();
        const auto prefix = empty.finish(original).value();
        auto footer = encode_durable_footer(
                        prefix,
                        {history(), runtime::file_position{512}},
                        original,
                        budget().operation_remaining,
                        charge)
                        .get()
                        .value();
        auto config = original.policy().config();
        if (mode == 0 || mode == 1)
            config.max_record_bytes = byte_count{64_KiB};
        codec::cooperative_work work{
          codec::limits::make(config).value(), abort};
        auto h = mode == 2 ? history(0x99) : history();
        const auto start = mode == 5 ? 1024U : 512U;
        auto walk = extent_verifier::make(
                      h,
                      scope(100, 100, 0, 0, start, start + 512),
                      work.policy())
                      .value();
        bytes::fragmented_buffer extracted;
        if (mode == 3) extracted = std::move(footer).release_bytes();
        if (mode == 4) abort.request_abort();
        const auto memory = mode == 0 ? codec::decode_budget{{}, {}, charge}
                                      : budget();
        // Extraction empties only the byte owner, leaving diagnostic metadata.
        // NOLINTBEGIN(bugprone-use-after-move)
        const auto accepted
          = walk.add_footer(footer, memory, work, prefix).get();
        EXPECT_EQ(accepted.has_value(), mode == 1);
        if (!accepted) EXPECT_TRUE(walk.closed());
        if (mode != 3) EXPECT_FALSE(footer.bytes().empty());
        // NOLINTEND(bugprone-use-after-move)
    }
}

TEST(ExtentVerifierTest, TypedFooterStillChecksTheActualHistoryCrc) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto walk = extent_verifier::make(
                  history(), scope(100, 101, 0, 1, 512, 1536), work.policy())
                  .value();
    ASSERT_TRUE(feed_block(walk, data_block(), work));
    const auto prefix = walk.checkpoint(work).value();
    auto other = extent_verifier::make(
                   history(), scope(100, 101, 0, 1, 512, 1024), work.policy())
                   .value();
    ASSERT_TRUE(
      feed_block(other, data_block(100, 0, 512, false, 0x30, 1, true), work));
    const auto different = other.finish(work).value();
    ASSERT_NE(prefix.boundary().data_crc32c, different.boundary().data_crc32c);
    auto footer = encode_durable_footer(
                    different,
                    {history(), runtime::file_position{1024}},
                    work,
                    budget().operation_remaining,
                    charge)
                    .get()
                    .value();
    const codec::decode_budget no_temporaries{{}, {}, charge};
    const auto rejected
      = walk.add_footer(footer, no_temporaries, work, prefix).get();
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code(), errc::corrupt_data);
    EXPECT_TRUE(walk.closed());
    EXPECT_FALSE(footer.bytes().empty());
}

codec::immutable_object_digest identity(std::string_view bytes) {
    codec::xxh3_128_hasher hash;
    hash.update(bytes.data(), bytes.size());
    return codec::immutable_object_digest{std::move(hash).final()};
}
codec::result<resumed_extent> resume_after(
  const std::string& footer,
  std::uint64_t position,
  codec::immutable_object_digest pinned,
  storage::coverage expected,
  codec::cooperative_work& work) {
    auto bytes = buffer(footer, 67);
    const auto memory = reserve(bytes, work);
    return extent_verifier::resume(
             {history(), runtime::file_position{position}},
             pinned,
             expected,
             std::move(bytes),
             memory,
             work)
      .get();
}

// A walk that knows only its end byte finishes at the prefix it accepted with
// exactly the evidence and digest of a walk supplied that extent.
TEST(ExtentVerifierTest, FinishAtThePrefixMatchesTheSuppliedExtent) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto first = data_block();
    const auto second = data_block(101, 1, 1024);
    auto exact = extent_verifier::make(
                   history(),
                   scope(100, 102, 0, 2, 512, 1536),
                   work.policy(),
                   extent_layout_kind::initial_append,
                   {},
                   extent_integrity::crc32c_and_digest)
                   .value();
    ASSERT_TRUE(feed_block(exact, first, work));
    ASSERT_TRUE(feed_block(exact, second, work));
    const auto whole = exact.finish(work).value();
    // Room for more than was supplied: only the accepted prefix counts.
    auto open = extent_verifier::make(
                  history(),
                  scope(100, 110, 0, 10, 512, 4096),
                  work.policy(),
                  extent_layout_kind::initial_append,
                  {},
                  extent_integrity::crc32c_and_digest)
                  .value();
    ASSERT_TRUE(feed_block(open, first, work));
    ASSERT_TRUE(feed_block(open, second, work));
    const auto prefix = open.finish_prefix(work).value();
    EXPECT_EQ(prefix.boundary(), whole.boundary());
    ASSERT_TRUE(prefix.digest());
    EXPECT_EQ(prefix.digest(), whole.digest());
    const auto bytes = first + second;
    EXPECT_EQ(
      prefix.digest()->bytes(), codec::xxh3_128(bytes.data(), bytes.size()));
    // Closed by finishing.
    EXPECT_FALSE(feed_block(open, data_block(102, 2, 1536), work));
    // A rewrite's coverage is never inferred from a prefix.
    auto rewrite = extent_verifier::make(
                     history(),
                     scope(100, 102, 0, 2, 512, 1536),
                     work.policy(),
                     extent_layout_kind::rewrite)
                     .value();
    const auto refused = rewrite.finish_prefix(work);
    ASSERT_FALSE(refused);
    EXPECT_EQ(refused.error().code(), errc::invalid_argument);
}

TEST(ExtentVerifierTest, ResumeAfterAPinnedFooterMatchesTheFullWalk) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto expected = scope(100, 102, 0, 2, 512, 2560);
    auto full
      = extent_verifier::make(history(), expected, work.policy()).value();
    const auto first = data_block();
    ASSERT_TRUE(feed_block(full, first, work));
    const auto pinned = full.checkpoint(work).value();
    const auto middle = footer_wire(
      pinned.boundary(), {history(), runtime::file_position{1024}});
    ASSERT_TRUE(feed_footer(full, middle, work, pinned));
    const auto second = data_block(101, 1, 1536);
    ASSERT_TRUE(feed_block(full, second, work));
    const auto prefix = full.checkpoint(work).value();
    const auto last = footer_wire(
      prefix.boundary(), {history(), runtime::file_position{2048}});
    ASSERT_TRUE(feed_footer(full, last, work, prefix));
    const auto whole = full.finish(work).value();

    // Only the pinned footer and later objects are supplied.
    auto resumed
      = resume_after(middle, 1024, identity(middle), expected, work).value();
    EXPECT_EQ(resumed.footer.boundary(), pinned.boundary());
    EXPECT_EQ(resumed.footer.location().position.value(), 1024U);
    auto& walk = resumed.verifier;
    ASSERT_TRUE(feed_block(walk, second, work));
    EXPECT_EQ(walk.checkpoint(work).value().boundary(), prefix.boundary());
    ASSERT_TRUE(feed_footer(walk, last, work));
    const auto finished = walk.finish(work).value();
    EXPECT_EQ(finished.boundary(), whole.boundary());
    EXPECT_EQ(
      finished.boundary().data_crc32c, crc(first + middle + second + last));
    EXPECT_FALSE(finished.digest());
}

TEST(ExtentVerifierTest, ResumeAcceptsOnlyThePinnedFooterNamingItsPrefix) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto expected = scope(100, 102, 0, 2, 512, 2560);
    auto full
      = extent_verifier::make(history(), expected, work.policy()).value();
    ASSERT_TRUE(feed_block(full, data_block(), work));
    const auto pinned = full.checkpoint(work).value();
    const auto middle = footer_wire(
      pinned.boundary(), {history(), runtime::file_position{1024}});
    // Other bytes than the pinned ones are never decoded.
    auto other = middle;
    other[100] ^= 1;
    const auto changed = resume_after(
      other, 1024, identity(middle), expected, work);
    ASSERT_FALSE(changed);
    EXPECT_EQ(changed.error().code(), errc::corrupt_data);
    // The pinned bytes at another position name another footer.
    const auto moved = resume_after(
      middle, 1536, identity(middle), expected, work);
    ASSERT_FALSE(moved);
    EXPECT_EQ(moved.error().code(), errc::wrong_context);
    // A footer after the prefix it names leaves its gap unverified.
    const auto gapped = footer_wire(
      pinned.boundary(), {history(), runtime::file_position{1536}});
    const auto gap = resume_after(
      gapped, 1536, identity(gapped), expected, work);
    ASSERT_FALSE(gap);
    EXPECT_EQ(gap.error().code(), errc::malformed_data);
    // The independent extent must contain the pinned footer.
    const auto outside = resume_after(
      middle, 1024, identity(middle), scope(100, 101, 0, 1, 512, 1024), work);
    ASSERT_FALSE(outside);
    EXPECT_EQ(outside.error().code(), errc::invalid_argument);
}

} // namespace
} // namespace kwaque::storage
