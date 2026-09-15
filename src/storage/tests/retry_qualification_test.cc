#include "src/storage/tests/retry_test_support.h"

#include <seastar/core/preempt.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <gtest/gtest.h>

#include <chrono>

namespace kwaque::storage {
namespace {
using namespace testing;
using bytes::fragmented_buffer_parser;

std::vector<completed_retry>
page_entries(std::uint32_t first, std::uint32_t count) {
    std::vector<completed_retry> entries;
    entries.reserve(count);
    // The same original returned span is valid for distinct producer requests;
    // a summary key order makes no assertion about execution/logical ordering.
    for (std::uint32_t i = 0; i < count; ++i)
        entries.push_back(retry(first + i));
    return entries;
}

TEST(RetryQualificationTest, CompleteMaximumSummaryKeepsOnlyOnePage) {
    for (const std::size_t header : {32U, 4096U}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto cap = retry_page_capacity(
                           byte_count{header}, alignment(), work.policy())
                           .value();
        std::vector<page_ref> refs;
        refs.reserve((65536U + cap - 1U) / cap);
        for (std::uint32_t first = 0; first < 65536;) {
            const auto count = std::min(cap, 65536U - first);
            const auto entries = page_entries(first, count);
            const auto ordinal = static_cast<std::uint32_t>(refs.size());
            const auto wire = retry_page_wire(
              entries, root_location(), ordinal, first, header);
            refs.push_back(reference(wire, ordinal, first, count));
            first += count;
            seastar::thread::maybe_yield();
        }
        EXPECT_EQ(refs.size(), header == 32 ? 161U : 172U);
        const auto root = pin_root(
          one_root(refs, root_location(), header), work);
        EXPECT_EQ(root.retry_count(), 65536U);
        retry_summary_verifier summary{root, work.policy()};
        std::uint32_t total = 0;
        for (const auto& ref : refs) {
            const auto entries = page_entries(
              ref.first_entry(), ref.entry_count());
            const auto wire = retry_page_wire(
              entries,
              root_location(),
              ref.ordinal().value(),
              ref.first_entry(),
              header);
            fragmented_buffer_parser input{buffer(wire, 4096)};
            auto memory = reserve_page(input, root, work);
            // Independent fixture entries and refs remain live while the
            // production decoder owns its one-page metadata array.
            const auto fixture_cost
              = charge(byte_count{entries.capacity() * sizeof(completed_retry)})
                  .checked_add(
                    charge(byte_count{refs.capacity() * sizeof(page_ref)}))
                  .value();
            memory.operation_remaining = byte_count{
              memory.operation_remaining.value() - fixture_cost.value()};
            memory.metadata_remaining = byte_count{
              memory.metadata_remaining.value() - fixture_cost.value()};
            const auto page = summary.next(input, memory, work).get();
            ASSERT_TRUE(page.has_value());
            EXPECT_TRUE(std::ranges::equal(page->value.entries(), entries));
            EXPECT_LE(page->value.entry_capacity(), cap);
            const auto retained = charge(
              byte_count{
                page->value.entry_capacity() * sizeof(completed_retry)});
            EXPECT_EQ(
              memory.operation_remaining.value()
                - page->remaining.operation_remaining.value(),
              retained.value());
            EXPECT_EQ(
              memory.metadata_remaining.value()
                - page->remaining.metadata_remaining.value(),
              retained.value());
            total += ref.entry_count();
        }
        const auto proof = summary.finish(work);
        ASSERT_TRUE(proof.has_value());
        EXPECT_EQ(proof->entry_count(), total);
        EXPECT_EQ(total, 65536U);
        auto over = one_root(refs);
        put(over, 32 + 220, 65537, 4);
        repair(over);
        fragmented_buffer_parser input{buffer(over)};
        const auto result = decode_sealed_footer(
                              input,
                              root_location(),
                              codec::immutable_object_digest{exact_sha(over)},
                              reserve(input, work),
                              work)
                              .get();
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code(), errc::resource_exhausted);
    }
}

TEST(RetryQualificationTest, OneOverPageAndNarrowerRootPolicyRejectExplicitly) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto entries = page_entries(0, 409);
    const auto result = encode_retry_page(
                          entries,
                          root_location(),
                          page_ordinal::make(0).value(),
                          0,
                          work,
                          budget().operation_remaining,
                          charge)
                          .get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), errc::resource_exhausted);
    const auto page = retry_page_wire(std::span{entries}.first(2));
    const std::array refs{reference(page, 0, 0, 2)};
    const auto root = pin_root(one_root(refs), work);
    auto config = work.policy().config();
    config.max_object_entries = item_count{1};
    codec::cooperative_work narrow{codec::limits::make(config).value(), abort};
    retry_summary_verifier summary{root, narrow.policy()};
    const auto final = summary.finish(narrow);
    ASSERT_FALSE(final.has_value());
    EXPECT_EQ(final.error().code(), errc::resource_exhausted);
    EXPECT_TRUE(summary.closed());
}

TEST(
  RetryQualificationTest, AllocationFailuresRestorePageAndCloseEnteredSummary) {
#ifndef SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION
    GTEST_SKIP() << "allocation failure injection is unavailable";
#else
    const std::array entries{retry(0), retry(1)};
    const auto wire = retry_page_wire(entries);
    const std::array refs{reference(wire, 0, 0, 2)};
    seastar::abort_source root_abort;
    codec::cooperative_work root_work{codec::limits::defaults(), root_abort};
    const auto root = pin_root(one_root(refs), root_work);
    for (const bool encode : {false, true}) {
        std::size_t failures = 0;
        bool completed = false;
        for (std::uint64_t ordinal = 0; ordinal < 256 && !completed;
             ++ordinal) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            fragmented_buffer_parser input{buffer("p" + wire, 7)};
            input.skip(byte_count{1}).value();
            input.push_checkpoint().value();
            const auto memory = reserve_page(input, root, work);
            retry_summary_verifier summary{root, work.policy()};
            auto& injector = seastar::memory::local_failure_injector();
            bool succeeded = false, entered = false;
            injector.fail_after(ordinal);
            try {
                if (encode)
                    succeeded = encode_retry_page(
                                  entries,
                                  root_location(),
                                  page_ordinal::make(0).value(),
                                  0,
                                  work,
                                  budget().operation_remaining,
                                  charge)
                                  .get()
                                  .has_value();
                else {
                    auto pending = summary.next(input, memory, work);
                    entered = true;
                    succeeded = pending.get().has_value();
                }
            } catch (const std::bad_alloc&) {
            } catch (const std::runtime_error&) {
                if (!injector.failed()) {
                    injector.cancel();
                    throw;
                }
            } catch (...) {
                injector.cancel();
                throw;
            }
            const bool reached = injector.failed();
            injector.cancel();
            if (!succeeded) {
                EXPECT_TRUE(reached);
                ++failures;
                EXPECT_EQ(input.bytes_consumed().value(), 1U);
                // A failing outer frame allocation precedes entry. Every
                // failure after next() returns its future has entered the
                // owner.
                if (!encode && entered) EXPECT_TRUE(summary.closed());
            } else {
                if (!encode) EXPECT_TRUE(summary.finish(work));
                completed = !reached;
            }
            EXPECT_EQ(input.checkpoint_depth(), 1U);
        }
        EXPECT_TRUE(completed);
        EXPECT_GT(failures, 0U);
    }
#endif
}

TEST(RetryQualificationTest, SuspendedPageCancellationRestoresCallerMarks) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto entries = page_entries(0, 100);
    const auto wire = retry_page_wire(entries);
    const std::array refs{reference(wire, 0, 0, 100)};
    const auto root = pin_root(one_root(refs), work);
    fragmented_buffer_parser input{buffer("p" + wire, 67)};
    input.skip(byte_count{1}).value();
    input.push_checkpoint().value();
    const auto memory = reserve_page(input, root, work);
    retry_summary_verifier summary{root, work.policy()};
    const auto deadline = std::chrono::steady_clock::now()
                          + std::chrono::seconds{2};
    while (!seastar::need_preempt()
           && std::chrono::steady_clock::now() < deadline) {
    }
    ASSERT_TRUE(seastar::need_preempt());
    auto pending = summary.next(input, memory, work);
    const bool suspended = !pending.available();
    abort.request_abort();
    const auto result = pending.get();
    ASSERT_FALSE(result.has_value());
    EXPECT_TRUE(suspended);
    EXPECT_EQ(result.error().code(), errc::aborted);
    EXPECT_TRUE(summary.closed());
    EXPECT_FALSE(summary.finish(work));
    EXPECT_EQ(input.bytes_consumed().value(), 1U);
    EXPECT_EQ(input.checkpoint_depth(), 1U);
}
} // namespace
} // namespace kwaque::storage
