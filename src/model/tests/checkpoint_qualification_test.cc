#include "src/bytes/test_allocation_profile.h"
#include "src/codec/collection.h"
#include "src/codec/tests/prepared_abort_source.h"
#include "src/model/checkpoint_codec.h"
#include "src/model/fingerprint.h"
#include "src/model/tests/checkpoint_fuzz_cases.h"
#include "src/model/tests/checkpoint_test_support.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/preempt.hh>
#include <seastar/core/reactor.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <exception>
#include <malloc.h>
#include <optional>
#include <ranges>
#include <string>
#include <utility>
#include <vector>

namespace {
namespace model = kwaque::model;
namespace codec = kwaque::codec;
namespace fixture = model::testing::checkpoint_fixture;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::item_count;
using kwaque::bytes::fragmented_buffer_parser;
using kwaque::bytes::testing::charge;
using source_type = seastar::chunked_fifo<model::range_cursor, 16>;
codec::decode_budget memory() {
    return {byte_count{32U << 20U}, byte_count{1U << 20U}, charge};
}
std::vector<model::range_cursor> entries(std::uint32_t count) {
    std::vector<model::range_cursor> values;
    values.reserve(count);
    for (std::uint32_t i = 1; i <= count; ++i)
        values.push_back(fixture::numbered(i));
    return values;
}

TEST(CheckpointQualificationTest, MaximumAndOneOverCountAtEveryHeaderEndpoint) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    for (const auto count : {4096U, 4097U}) {
        for (const auto header : {32U, 41U, 4096U}) {
            const auto raw = fixture::wire(count, header);
            const auto reference = fixture::probe(raw, fixture::topic(), true);
            for (const auto width : {128U, 4096U}) {
                fragmented_buffer_parser input{fixture::fragmented(raw, width)};
                const auto budget = codec::reserve_decode_input(
                                      input, work.policy(), memory())
                                      .value();
                auto decoded = model::decode_read_checkpoint(
                                 input, fixture::topic(), budget, work)
                                 .get();
                if (count == 4097) {
                    ASSERT_FALSE(decoded);
                    EXPECT_EQ(decoded.error().code(), errc::resource_exhausted);
                    EXPECT_EQ(input.bytes_consumed(), byte_count{});
                    EXPECT_EQ(reference.error, errc::resource_exhausted);
                    continue;
                }
                ASSERT_TRUE(decoded);
                EXPECT_TRUE(input.at_end());
                EXPECT_EQ(reference.error, errc::success);
                EXPECT_EQ(raw.size(), header + 20U + 24U * count);
                const auto body = fixture::body_from_value(decoded->value);
                EXPECT_EQ(body, std::string_view{raw}.substr(header));
                EXPECT_EQ(decoded->fingerprint, fixture::digest(body));
                const auto reserved = charge(
                  byte_count{
                    decoded->value.cursor_capacity().value()
                    * sizeof(model::range_cursor)});
                EXPECT_LE(reserved.value(), 131072U);
                EXPECT_LE(
                  malloc_usable_size(
                    const_cast<model::range_cursor*>(
                      decoded->value.cursors().data())),
                  reserved.value());
                EXPECT_EQ(
                  decoded->remaining.operation_remaining,
                  budget.operation_remaining.checked_sub(reserved).value());
                EXPECT_EQ(
                  decoded->remaining.metadata_remaining,
                  budget.metadata_remaining.checked_sub(reserved).value());
            }
        }
    }
    const auto oversized = entries(4097);
    const auto rejected = model::make_read_checkpoint(
                            fixture::topic(), oversized, memory(), work)
                            .get();
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code(), errc::resource_exhausted);
}

TEST(
  CheckpointQualificationTest,
  LowerLimitsAndTerminalCoordinatesRemainTransactional) {
    const auto raw = fixture::wire(4096, 4096);
    for (unsigned limit = 0; limit < 8; ++limit) {
        auto config = codec::limits::defaults().config();
        if (limit == 0) config.max_checkpoint_cursors = item_count{4095};
        if (limit == 1) config.max_object_entries = item_count{4095};
        if (limit == 2)
            config.max_checkpoint_bytes = byte_count{raw.size() - 1U};
        if (limit == 3)
            config.max_allocation_bytes = byte_count{
              charge(byte_count{4096U * sizeof(model::range_cursor)}).value()
              - 1U};
        if (limit == 4) config.max_work_items = item_count{63};
        if (limit == 7) config.max_work_bytes = byte_count{255};
        seastar::abort_source abort;
        codec::cooperative_work work{
          codec::limits::make(config).value(), abort};
        fragmented_buffer_parser input{fixture::fragmented(raw, 4096)};
        input.push_checkpoint().value();
        auto budget
          = codec::reserve_decode_input(input, work.policy(), memory()).value();
        if (limit == 5)
            budget.metadata_remaining = byte_count{
              charge(byte_count{4096U * sizeof(model::range_cursor)}).value()
              - 1U};
        if (limit == 6) budget.operation_remaining = {};
        const auto result = model::decode_read_checkpoint(
                              input, fixture::topic(), budget, work)
                              .get();
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().code(), errc::resource_exhausted);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
        EXPECT_EQ(input.checkpoint_depth(), 1);
    }
    for (const bool overflow : {false, true}) {
        fragmented_buffer_parser input{fixture::fragmented(raw, 4096)};
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const codec::field_context context{
          .origin = UINT64_MAX - raw.size() + (overflow ? 1U : 0U),
          .family = 10};
        const auto budget = codec::reserve_decode_input(
          input, work.policy(), memory(), context);
        if (overflow) {
            ASSERT_FALSE(budget);
            EXPECT_EQ(budget.error().code(), errc::invalid_argument);
        } else {
            ASSERT_TRUE(budget);
            const auto result
              = model::decode_read_checkpoint(
                  input, fixture::topic(), *budget, work, context)
                  .get();
            ASSERT_TRUE(result);
            EXPECT_TRUE(input.at_end());
        }
    }
}

TEST(
  CheckpointQualificationTest,
  NativePartialFifoChunksAndConversionKeepTheirReservations) {
    source_type source;
    for (std::uint32_t i = 1; i <= 4097; ++i)
        source.push_back(fixture::numbered(i));
    const auto chunk = charge(
      byte_count{
        codec::detail::collection_chunk_bytes<model::range_cursor, 16>()});
    std::size_t index = 0;
    for (auto& cursor : source) {
        // Native chunk items begin at allocation offset zero. Inspect only
        // the first slot of fresh chunks, before changing the front index.
        if (index++ % 16U == 0)
            EXPECT_LE(malloc_usable_size(&cursor), chunk.value());
    }
    source.pop_front();
    ASSERT_EQ(source.size(), 4096);
    ASSERT_EQ(source.nfree_chunks(), 0);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto result = model::make_read_checkpoint_from_unordered(
                          fixture::topic(), std::move(source), memory(), work)
                          .get();
    ASSERT_TRUE(result);
    EXPECT_EQ(result->value.cursors().front(), fixture::numbered(2));
    EXPECT_EQ(result->value.cursors().back(), fixture::numbered(4097));
    const auto retained = charge(
      byte_count{
        result->value.cursor_capacity().value() * sizeof(model::range_cursor)});
    EXPECT_EQ(
      result->remaining.metadata_remaining,
      memory().metadata_remaining.checked_sub(retained).value());
    EXPECT_EQ(
      result->remaining.operation_remaining,
      memory().operation_remaining.checked_sub(retained).value());
}

TEST(
  CheckpointQualificationTest,
  AllocationFailuresInSortedHashAndEncodePreserveBorrowedInput) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation injection is disabled in this verified profile";
#else
    const auto source = entries(65);
    seastar::abort_source setup_abort;
    codec::cooperative_work setup{codec::limits::defaults(), setup_abort};
    auto built = model::make_read_checkpoint(
                   fixture::topic(), source, memory(), setup)
                   .get()
                   .value();
    const auto warm
      = model::compute_checkpoint_fingerprint(built.value, setup).get().value();
    const auto exercise = [&](auto start) {
        bool completed = false;
        std::size_t failures = 0;
        for (std::uint64_t ordinal = 0; ordinal < 512 && !completed;
             ++ordinal) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            std::optional<typename decltype(start(work))::value_type> outcome;
            std::exception_ptr exception;
            auto& injector = seastar::memory::local_failure_injector();
            injector.fail_after(ordinal);
            try {
                outcome.emplace(start(work).get());
            } catch (...) {
                exception = std::current_exception();
            }
            const bool injected = injector.failed();
            injector.cancel();
            if (exception) {
                EXPECT_TRUE(injected);
                ++failures;
            } else {
                ASSERT_TRUE(outcome);
                ASSERT_TRUE(*outcome);
                completed = true;
            }
            EXPECT_TRUE(std::ranges::equal(source, built.value.cursors()));
        }
        EXPECT_TRUE(completed);
        EXPECT_GT(failures, 0);
    };
    exercise([&](codec::cooperative_work& work) {
        return model::make_read_checkpoint(
          fixture::topic(), source, memory(), work);
    });
    exercise([&](codec::cooperative_work& work) {
        return model::compute_checkpoint_fingerprint(built.value, work);
    });
    exercise([&](codec::cooperative_work& work) {
        return model::encode_read_checkpoint(
          built.value, work, memory().operation_remaining, charge);
    });
    EXPECT_EQ(
      model::compute_checkpoint_fingerprint(built.value, setup).get().value(),
      warm);
#endif
}

seastar::future<> control_progress(
  seastar::abort_source& stop,
  std::uint64_t& ticks,
  std::chrono::steady_clock::duration& largest_gap) {
    auto previous = std::chrono::steady_clock::now();
    while (!stop.abort_requested()) {
        co_await seastar::yield();
        if (!stop.abort_requested()) {
            const auto now = std::chrono::steady_clock::now();
            largest_gap = std::max(largest_gap, now - previous);
            previous = now;
            ++ticks;
        }
    }
}

thread_local seastar::abort_source* admission_abort = nullptr;
thread_local std::size_t admission_ordinal = 0;
thread_local std::size_t admission_calls = 0;
byte_count aborting_charge(byte_count request) noexcept {
    // Preserve the verified monotone capacity bound, while observing actual
    // codec admission points. No allocator or scheduling hook is installed.
    const auto served = charge(request);
    if (admission_abort != nullptr && admission_calls++ == admission_ordinal)
        admission_abort->request_abort();
    return served;
}
TEST(
  CheckpointQualificationTest,
  AdmissionCancellationPreservesChargeWithoutAllocation) {
    codec::testing::prepared_abort_source abort;
    admission_abort = &abort;
    admission_ordinal = 0;
    admission_calls = 0;
    auto reset = seastar::defer([] { admission_abort = nullptr; });
    const byte_count request{4096};
    const auto expected = charge(request);
#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
    const auto before = seastar::memory::stats().mallocs();
#endif
    const auto served = aborting_charge(request);
#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
    const auto allocations = seastar::memory::stats().mallocs() - before;
    EXPECT_EQ(allocations, 0U);
#endif
    EXPECT_TRUE(abort.abort_requested());
    EXPECT_EQ(served, expected);
    EXPECT_EQ(admission_calls, 1U);
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    GTEST_SKIP() << "semantic cancellation passed; native allocation counters "
                    "are unavailable";
#endif
}

TEST(
  CheckpointQualificationTest,
  CancellationAtAdmissionAndPublicationDrainsOwnedStaging) {
    const auto source = entries(65);
    seastar::abort_source setup_abort;
    codec::cooperative_work setup{codec::limits::defaults(), setup_abort};
    auto built = model::make_read_checkpoint(
                   fixture::topic(), source, memory(), setup)
                   .get()
                   .value();
    const auto raw = fixture::wire(65);
    const auto exercise = [&](auto start) {
        bool completed = false;
        std::size_t cancellations = 0;
        for (std::size_t ordinal = 0; ordinal < 512 && !completed; ++ordinal) {
            codec::testing::prepared_abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto budget = memory();
            budget.charge = aborting_charge;
            admission_ordinal = ordinal;
            admission_calls = 0;
            admission_abort = &abort;
            auto reset = seastar::defer([] { admission_abort = nullptr; });
            const auto result = start(work, budget);
            admission_abort = nullptr;
            if (abort.abort_requested()) {
                ASSERT_FALSE(result);
                EXPECT_EQ(result.error().code(), errc::aborted);
                ++cancellations;
            } else {
                ASSERT_TRUE(result);
                completed = true;
            }
            EXPECT_TRUE(std::ranges::equal(source, built.value.cursors()));
        }
        EXPECT_TRUE(completed);
        EXPECT_GT(cancellations, 0);
    };
    exercise([&](codec::cooperative_work& work, codec::decode_budget budget) {
        return model::make_read_checkpoint(
                 fixture::topic(), source, budget, work)
          .get();
    });
    exercise([&](codec::cooperative_work& work, codec::decode_budget budget) {
        source_type owned;
        for (const auto& cursor : std::views::reverse(source))
            owned.push_back(cursor);
        return model::make_read_checkpoint_from_unordered(
                 fixture::topic(), std::move(owned), budget, work)
          .get();
    });
    exercise([&](codec::cooperative_work& work, codec::decode_budget budget) {
        return model::encode_read_checkpoint(
                 built.value, work, budget.operation_remaining, budget.charge)
          .get();
    });
    exercise([&](codec::cooperative_work& work, codec::decode_budget budget) {
        fragmented_buffer_parser input{fixture::fragmented(raw, 67)};
        // The stable input's admission precedes the operation under test.
        auto reserved = codec::reserve_decode_input(
                          input, setup.policy(), memory())
                          .value();
        reserved.charge = budget.charge;
        auto result = model::decode_read_checkpoint(
                        input, fixture::topic(), reserved, work)
                        .get();
        if (!result) EXPECT_EQ(input.bytes_consumed(), byte_count{});
        return result;
    });
}
TEST(
  CheckpointQualificationTest,
  EveryMaximumOperationAllowsControlProgressAndReportsNativeCounters) {
    const auto source = entries(4096);
    seastar::abort_source setup_abort;
    codec::cooperative_work setup{codec::limits::defaults(), setup_abort};
    const auto raw = fixture::wire(4096, 4096);
    const auto source_budget
      = codec::detail::consume_decode_budget(
          setup.policy(),
          memory(),
          charge(byte_count{raw.capacity() + 1U}),
          charge(byte_count{source.capacity() * sizeof(model::range_cursor)}),
          {},
          0)
          .value();
    const auto exercise = [&](const char* name, auto start) {
        seastar::abort_source abort, stop;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto deadline = std::chrono::steady_clock::now()
                              + std::chrono::seconds{2};
        while (!seastar::need_preempt()
               && std::chrono::steady_clock::now() < deadline) {
        }
        ASSERT_TRUE(seastar::need_preempt());
        std::uint64_t ticks = 0;
        std::chrono::steady_clock::duration gap{};
        auto observer = control_progress(stop, ticks, gap);
        auto joined = seastar::defer([&] {
            stop.request_abort();
            observer.get();
        });
        const auto allocations = seastar::memory::stats().mallocs();
        const auto tasks = seastar::engine().get_sched_stats().tasks_processed;
        auto pending = start(work);
        const bool suspended = !pending.available();
        const auto result = pending.get();
        const auto allocated = seastar::memory::stats().mallocs() - allocations;
        const auto processed
          = seastar::engine().get_sched_stats().tasks_processed - tasks;
        ASSERT_TRUE(result);
        EXPECT_TRUE(suspended);
        EXPECT_GT(ticks, 0U);
        RecordProperty(
          std::string{name} + "_allocations", std::to_string(allocated));
        RecordProperty(std::string{name} + "_tasks", std::to_string(processed));
        RecordProperty(
          std::string{name} + "_control_ticks", std::to_string(ticks));
        RecordProperty(
          std::string{name} + "_largest_gap_ns",
          std::to_string(
            std::chrono::duration_cast<std::chrono::nanoseconds>(gap).count()));
    };
    exercise("sorted", [&](codec::cooperative_work& work) {
        return model::make_read_checkpoint(
          fixture::topic(), source, source_budget, work);
    });
    source_type unordered;
    for (std::uint32_t i = 4096; i != 0; --i)
        unordered.push_back(fixture::numbered(i));
    exercise("unordered", [&](codec::cooperative_work& work) {
        return model::make_read_checkpoint_from_unordered(
          fixture::topic(), std::move(unordered), source_budget, work);
    });
    auto built = model::make_read_checkpoint(
                   fixture::topic(), source, source_budget, setup)
                   .get()
                   .value();
    exercise("fingerprint", [&](codec::cooperative_work& work) {
        return model::compute_checkpoint_fingerprint(built.value, work);
    });
    exercise("encode", [&](codec::cooperative_work& work) {
        return model::encode_read_checkpoint(
          built.value, work, built.remaining.operation_remaining, charge);
    });
    fragmented_buffer_parser input{fixture::fragmented(raw, 4096)};
    const auto reserved = codec::reserve_decode_input(
                            input, setup.policy(), built.remaining)
                            .value();
    exercise("decode", [&](codec::cooperative_work& work) {
        return model::decode_read_checkpoint(
          input, fixture::topic(), reserved, work);
    });
}

TEST(
  CheckpointQualificationTest,
  StructuredFuzzCasesAndRawGoldensReachIndependentOracle) {
    for (unsigned mutation = 0; mutation < 20; ++mutation)
        for (unsigned header = 0; header < 3; ++header)
            for (unsigned boundary = 0; boundary < 2; ++boundary) {
                const std::array<std::uint8_t, 8> test{
                  1,
                  static_cast<std::uint8_t>(mutation),
                  2,
                  static_cast<std::uint8_t>(header),
                  1,
                  static_cast<std::uint8_t>(boundary),
                  0,
                  67};
                model::testing::exercise_checkpoint_case(test);
            }
    for (const unsigned flags : {2U, 4U, 8U, 16U, 32U, 64U}) {
        const std::array<std::uint8_t, 8> test{
          1, 0, 2, 1, 0, static_cast<std::uint8_t>(flags), 0, 0};
        model::testing::exercise_checkpoint_case(test);
    }
    for (std::uint8_t depth = 0; depth <= 8; ++depth) {
        const std::array<std::uint8_t, 8> test{1, 0, 2, 1, 1, 0, depth, 0};
        model::testing::exercise_checkpoint_case(test);
    }
    for (std::uint8_t mode = 0; mode < 3; ++mode)
        for (const unsigned flags : {0U, 4U, 8U, 16U, 32U, 64U, 128U}) {
            const std::array<std::uint8_t, 8> test{
              2, mode, 128, 0, 0, static_cast<std::uint8_t>(flags), 0, 17};
            model::testing::exercise_checkpoint_case(test);
        }
    for (const auto header : {32U, 41U, 4096U}) {
        const auto wire = fixture::wire(2, header);
        std::vector<std::uint8_t> test(8, 0);
        test[5] = 1;
        test.insert(test.end(), wire.begin(), wire.end());
        model::testing::exercise_checkpoint_case(test);
    }
}
TEST(
  CheckpointQualificationTest,
  MixedFuzzControlsKeepFaultPrecedenceAndRejectForcedSuccess) {
    for (unsigned flags = 0; flags < 256; ++flags) {
        for (const std::uint8_t depth : {std::uint8_t{0}, std::uint8_t{8}}) {
            // Intact frames distinguish a forced failure from a grammar
            // rejection; owning duplicates also exercise competing failures.
            std::array<std::uint8_t, 8> test{
              1, 0, 2, 1, 1, static_cast<std::uint8_t>(flags), depth, 0};
            model::testing::exercise_checkpoint_case(test);
            test[0] = 2;
            test[1] = 2;
            model::testing::exercise_checkpoint_case(test);
        }
    }
}

} // namespace
