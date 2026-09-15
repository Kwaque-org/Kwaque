#include "src/compression/lz4.h"
#include "src/compression/tests/test_support.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>

#include <algorithm>
#include <array>
#include <cstddef>
#include <malloc.h>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <type_traits>

namespace kwaque::compression {
namespace {
using namespace testing;
using namespace detail;

std::array<char, 128> small_frame(std::size_t& length) {
    std::array<char, 128> frame{};
    const auto prefs = writer_preferences(byte_count{1});
    length = LZ4F_compressFrame(frame.data(), frame.size(), "x", 1, &prefs);
    if (LZ4F_isError(length))
        throw std::runtime_error{LZ4F_getErrorName(length)};
    return frame;
}

codec::result<lz4_plan> plan(
  lz4_direction direction,
  codec::cooperative_work& work,
  codec::decode_budget memory = budget(),
  byte_count expanded = byte_count{1}) {
    return admit_lz4(
      direction, expanded, byte_count{16U << 20U}, work, memory, context);
}

std::size_t live_count(const lz4_memory& memory) {
    return static_cast<std::size_t>(std::ranges::count_if(
      memory.allocations(),
      [](const auto& entry) { return entry.address != nullptr; }));
}

void verify_allocations(const lz4_memory& memory, byte_count admitted) {
    byte_count total;
    for (const auto& entry : memory.allocations()) {
        if (entry.address == nullptr) continue;
        const auto observed = ::malloc_usable_size(entry.address);
        EXPECT_GE(observed, entry.requested.value());
        EXPECT_LE(observed, entry.charged.value());
        EXPECT_LE(entry.charged.value(), maximum_contiguous_allocation_bytes);
        total = *total.checked_add(entry.charged);
    }
    EXPECT_EQ(total, memory.live_bytes());
    EXPECT_LE(total, admitted);
}

TEST(Lz4ContextTest, FixedWriterPreferencesAndCallBounds) {
    const auto prefs = writer_preferences(byte_count{65537});
    EXPECT_EQ(prefs.frameInfo.blockSizeID, LZ4F_max64KB);
    EXPECT_EQ(prefs.frameInfo.blockMode, LZ4F_blockIndependent);
    EXPECT_EQ(prefs.frameInfo.contentChecksumFlag, LZ4F_contentChecksumEnabled);
    EXPECT_EQ(prefs.frameInfo.blockChecksumFlag, LZ4F_blockChecksumEnabled);
    EXPECT_EQ(prefs.frameInfo.frameType, LZ4F_frame);
    EXPECT_EQ(prefs.frameInfo.dictID, 0U);
    EXPECT_EQ(prefs.frameInfo.contentSize, 65537U);
    EXPECT_EQ(prefs.compressionLevel, 0);
    EXPECT_EQ(prefs.autoFlush, 0U);
    EXPECT_EQ(prefs.favorDecSpeed, 0U);
    for (const auto value : prefs.reserved)
        EXPECT_EQ(value, 0U);
    EXPECT_EQ(LZ4F_compressBound(1, &prefs), 65552U);
    EXPECT_EQ(LZ4F_compressBound(65536, &prefs), 65552U);
    EXPECT_EQ(LZ4F_compressBound(0, &prefs), 65551U);
}

TEST(Lz4ContextTest, AdmissionBoundsScratchOutputAndParentMetadata) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    for (const auto direction :
         {lz4_direction::compress, lz4_direction::decompress}) {
        const auto admitted = plan(
          direction, work, budget(), byte_count{8U << 20U});
        ASSERT_TRUE(admitted.has_value());
        EXPECT_LE(admitted->scratch_bytes.value(), 1U << 20U);
        EXPECT_LE(admitted->bounce_bytes.value(), 128U << 10U);
        EXPECT_EQ(
          admitted->output_limit.value(),
          direction == lz4_direction::compress ? 8389659U : 8388608U);
        EXPECT_LE(admitted->output_fragments.value(), 1024U);
        auto exact = budget();
        exact.operation_remaining = byte_count{
          admitted->scratch_bytes.value() + admitted->output_backing.value()
          + admitted->output_metadata.value()};
        exact.metadata_remaining = admitted->output_metadata;
        EXPECT_TRUE(plan(direction, work, exact, byte_count{8U << 20U}));
        if (direction == lz4_direction::decompress) {
            exact.operation_remaining = *exact.operation_remaining.checked_sub(
              byte_count{1});
            expect_error(
              plan(direction, work, exact, byte_count{8U << 20U}),
              errc::resource_exhausted);
        }
        exact = budget();
        exact.metadata_remaining = *admitted->output_metadata.checked_sub(
          byte_count{1});
        expect_error(
          plan(direction, work, exact, byte_count{8U << 20U}),
          errc::resource_exhausted);
    }
}

TEST(Lz4ContextTest, AdmissionRejectsBudgetAllocationAndWorkLimitsBeforeEntry) {
    for (auto member :
         {&codec::limits_config::max_scratch_bytes,
          &codec::limits_config::max_allocation_bytes,
          &codec::limits_config::max_work_bytes}) {
        auto config = codec::limits_config{};
        config.*member = byte_count{1};
        seastar::abort_source abort;
        codec::cooperative_work work{
          codec::limits::make(config).value(), abort};
        expect_error(
          plan(lz4_direction::compress, work), errc::resource_exhausted);
    }
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto memory = budget();
    memory.metadata_remaining = {};
    expect_error(
      plan(lz4_direction::compress, work, memory), errc::resource_exhausted);
    memory = budget();
    memory.operation_remaining = {};
    expect_error(
      plan(lz4_direction::compress, work, memory), errc::resource_exhausted);
    memory = budget();
    memory.charge = nullptr;
    expect_error(
      plan(lz4_direction::compress, work, memory), errc::invalid_argument);
    abort.request_abort();
    expect_error(plan(lz4_direction::compress, work), errc::aborted);
}

TEST(Lz4ContextTest, ThreeSlotsAreBoundedZeroedReusableAndBalanced) {
    lz4_memory memory{codec::limits::defaults(), byte_count{1U << 20U}, charge};
    const auto callbacks = memory.callbacks();
    std::array<void*, 3> pointers{};
    const auto cleanup = seastar::defer([&] {
        for (auto* pointer : pointers)
            callbacks.customFree(callbacks.opaqueState, pointer);
    });
    for (auto& pointer : pointers) {
        pointer = callbacks.customCalloc(callbacks.opaqueState, 256);
        ASSERT_NE(pointer, nullptr);
        const auto content = std::span{
          static_cast<const unsigned char*>(pointer), 256U};
        EXPECT_TRUE(
          std::ranges::all_of(content, [](auto value) { return value == 0; }));
    }
    EXPECT_EQ(live_count(memory), 3U);
    verify_allocations(memory, byte_count{1U << 20U});
    callbacks.customFree(callbacks.opaqueState, pointers[1]);
    pointers[1] = nullptr;
    EXPECT_EQ(live_count(memory), 2U);
    pointers[1] = callbacks.customAlloc(callbacks.opaqueState, 512);
    ASSERT_NE(pointers[1], nullptr);
    EXPECT_EQ(live_count(memory), 3U);
    EXPECT_EQ(callbacks.customAlloc(callbacks.opaqueState, 1), nullptr);
    expect_error(memory.status(context), errc::resource_exhausted);
    callbacks.customFree(callbacks.opaqueState, nullptr);
}

TEST(Lz4ContextTest, CallbacksRejectOversizedServedCapacityAndInvalidCharges) {
    {
        auto config = codec::limits_config{};
        config.max_allocation_bytes = byte_count{131071};
        lz4_memory memory{
          codec::limits::make(config).value(), byte_count{1U << 20U}, charge};
        auto callbacks = memory.callbacks();
        // The request fits, but its served-capacity bound is one byte too big.
        EXPECT_EQ(callbacks.customAlloc(callbacks.opaqueState, 65540), nullptr);
        expect_error(memory.status(context), errc::resource_exhausted);
    }
    for (const byte_count reserved : {byte_count{1}, byte_count{1U << 20U}}) {
        lz4_memory memory{codec::limits::defaults(), reserved, charge};
        auto callbacks = memory.callbacks();
        const auto request = reserved.value() == 1 ? 2U : 131073U;
        EXPECT_EQ(
          callbacks.customAlloc(callbacks.opaqueState, request), nullptr);
        expect_error(memory.status(context), errc::resource_exhausted);
        EXPECT_EQ(memory.live_bytes(), byte_count{});
    }
    lz4_memory memory{
      codec::limits::defaults(),
      byte_count{1U << 20U},
      +[](byte_count request) noexcept {
          return byte_count{request.value() - 1U};
      }};
    auto callbacks = memory.callbacks();
    EXPECT_EQ(callbacks.customAlloc(callbacks.opaqueState, 10), nullptr);
    expect_error(memory.status(context), errc::invalid_argument);
}

TEST(Lz4ContextTest, NativeContextsReachExactlyThreeLiveAllocations) {
    static_assert(!std::is_move_constructible_v<lz4_context>);
    static_assert(!std::is_move_constructible_v<lz4_memory>);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    std::size_t frame_size = 0;
    const auto frame = small_frame(frame_size);
    for (const auto direction :
         {lz4_direction::compress, lz4_direction::decompress}) {
        auto admitted = plan(direction, work).value();
        lz4_context owner{admitted};
        ASSERT_TRUE(owner.memory().status());
        ASSERT_EQ(live_count(owner.memory()), 1U);
        EXPECT_LE(owner.memory().allocations()[0].requested.value(), 4096U);
        lz4_staging staging{admitted};
        EXPECT_LE(
          ::malloc_usable_size(staging.bounce.get_write()),
          charge(admitted.bounce_bytes).value());
        if (direction == lz4_direction::compress) {
            const auto prefs = writer_preferences(byte_count{1});
            const auto result = LZ4F_compressBegin(
              owner.compressor(),
              staging.bounce.get_write(),
              staging.bounce.size(),
              &prefs);
            EXPECT_FALSE(LZ4F_isError(result));
        } else {
            LZ4F_frameInfo_t info{};
            auto header_size = LZ4F_headerSize(frame.data(), frame_size);
            ASSERT_FALSE(LZ4F_isError(header_size));
            const auto header = LZ4F_getFrameInfo(
              owner.decompressor(), &info, frame.data(), &header_size);
            ASSERT_FALSE(LZ4F_isError(header));
            EXPECT_EQ(live_count(owner.memory()), 1U);
            auto source_size = frame_size - header_size;
            auto output_size = staging.bounce.size();
            const auto result = LZ4F_decompress(
              owner.decompressor(),
              staging.bounce.get_write(),
              &output_size,
              frame.data() + header_size,
              &source_size,
              nullptr);
            EXPECT_EQ(result, 0U);
            EXPECT_EQ(output_size, 1U);
            EXPECT_EQ(staging.bounce.get()[0], 'x');
        }
        ASSERT_TRUE(owner.memory().status());
        EXPECT_EQ(live_count(owner.memory()), 3U);
        verify_allocations(owner.memory(), admitted.native_bytes);
        std::size_t served = 0, largest = 0;
        for (const auto& allocation : owner.memory().allocations()) {
            if (allocation.address == nullptr) continue;
            const auto capacity = ::malloc_usable_size(allocation.address);
            served += capacity;
            largest = std::max(largest, capacity);
        }
        const auto prefix = direction == lz4_direction::compress
                              ? "compress_"
                              : "decompress_";
        ::testing::Test::RecordProperty(
          std::string{prefix} + "native_served_bytes", std::to_string(served));
        ::testing::Test::RecordProperty(
          std::string{prefix} + "largest_native_allocation",
          std::to_string(largest));
        ::testing::Test::RecordProperty(
          std::string{prefix} + "scratch_charge_upper",
          std::to_string(admitted.scratch_bytes.value()));
    }
}

TEST(Lz4ContextTest, NativeBudgetDenialPreservesPartialContextCleanup) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    std::size_t frame_size = 0;
    const auto frame = small_frame(frame_size);
    for (const auto direction :
         {lz4_direction::compress, lz4_direction::decompress}) {
        auto admitted = plan(direction, work).value();
        // Narrow only the native subreservation to cover context creation.
        admitted.native_bytes = charge(byte_count{4096});
        lz4_context owner{admitted};
        ASSERT_TRUE(owner.memory().status());
        lz4_staging staging{admitted};
        std::size_t result;
        if (direction == lz4_direction::compress) {
            const auto prefs = writer_preferences(byte_count{1});
            result = LZ4F_compressBegin(
              owner.compressor(),
              staging.bounce.get_write(),
              staging.bounce.size(),
              &prefs);
        } else {
            auto source_size = frame_size;
            auto output_size = staging.bounce.size();
            result = LZ4F_decompress(
              owner.decompressor(),
              staging.bounce.get_write(),
              &output_size,
              frame.data(),
              &source_size,
              nullptr);
        }
        EXPECT_TRUE(LZ4F_isError(result));
        expect_error(owner.memory().status(context), errc::resource_exhausted);
        EXPECT_EQ(live_count(owner.memory()), 1U);
    }
}

TEST(Lz4ContextTest, StagingTransfersReservedCapacityEvenWhenMostlyEmpty) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto admitted
      = plan(lz4_direction::decompress, work, budget(), byte_count{65537})
          .value();
    lz4_staging staging{admitted};
    ASSERT_TRUE(staging.output.append(std::string_view{"x"}));
    auto output = staging.output.finish().value();
    const auto cost = output.allocation_cost(charge).value();
    EXPECT_LE(cost.backing, admitted.output_backing);
    EXPECT_LE(
      cost.descriptors.value() + cost.share_controls.value(),
      admitted.output_metadata.value());
    EXPECT_EQ(cost.fragments, item_count{1});
    EXPECT_GE(
      cost.descriptors.value(),
      2U * bytes::fragmented_buffer::fragment_descriptor_size());
    auto empty_plan
      = plan(lz4_direction::decompress, work, budget(), byte_count{}).value();
    lz4_staging empty{empty_plan};
    auto empty_output = empty.output.finish().value();
    EXPECT_EQ(empty_output.allocation_cost(charge)->descriptors, byte_count{});
}

TEST(
  Lz4ContextTest, EveryNativeAllocationFailureIsReportedAfterReturningFromC) {
#ifndef SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION
    GTEST_SKIP() << "allocation failure injection is unavailable";
#else
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    std::size_t frame_size = 0;
    const auto frame = small_frame(frame_size);
    for (const auto direction :
         {lz4_direction::compress, lz4_direction::decompress}) {
        const auto admitted = plan(direction, work).value();
        lz4_staging staging{admitted};
        for (std::uint64_t index = 0; index < 3; ++index) {
            std::optional<lz4_context> owner;
            auto& injector = seastar::memory::local_failure_injector();
            bool threw = false;
            injector.fail_after(index);
            try {
                owner.emplace(admitted);
                (void)owner->memory().status();
                if (direction == lz4_direction::compress) {
                    const auto prefs = writer_preferences(byte_count{1});
                    (void)LZ4F_compressBegin(
                      owner->compressor(),
                      staging.bounce.get_write(),
                      staging.bounce.size(),
                      &prefs);
                } else {
                    auto source_size = frame_size;
                    auto output_size = staging.bounce.size();
                    (void)LZ4F_decompress(
                      owner->decompressor(),
                      staging.bounce.get_write(),
                      &output_size,
                      frame.data(),
                      &source_size,
                      nullptr);
                }
                (void)owner->memory().status();
            } catch (const std::bad_alloc&) {
                threw = true;
            } catch (...) {
                injector.cancel();
                throw;
            }
            injector.cancel();
            EXPECT_TRUE(injector.failed());
            EXPECT_TRUE(threw);
            // Destruction's ledger invariant verifies all partial allocations
            // were freed while their callback state was still alive.
            owner.reset();
        }
        lz4_context healthy{admitted};
        EXPECT_TRUE(healthy.memory().status());
    }
#endif
}

TEST(Lz4ContextTest, ScratchAndDescriptorFailureLeaveContextOwnedUntilCleanup) {
#ifndef SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION
    GTEST_SKIP() << "allocation failure injection is unavailable";
#else
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto admitted = plan(lz4_direction::compress, work).value();
    for (std::uint64_t index = 0; index < 2; ++index) {
        lz4_context owner{admitted};
        ASSERT_TRUE(owner.memory().status());
        auto& injector = seastar::memory::local_failure_injector();
        bool failed = false;
        injector.fail_after(index);
        try {
            lz4_staging staging{admitted};
        } catch (const std::bad_alloc&) {
            failed = true;
        } catch (...) {
            injector.cancel();
            throw;
        }
        injector.cancel();
        EXPECT_TRUE(injector.failed());
        EXPECT_TRUE(failed);
        EXPECT_EQ(live_count(owner.memory()), 1U);
        EXPECT_TRUE(owner.memory().status());
    }
#endif
}

seastar::future<codec::result<void>> cancel_context(
  const lz4_plan& admitted,
  codec::cooperative_work& work,
  const lz4_context& other) {
    codec::result<void> result;
    {
        lz4_context owner{admitted};
        const auto created = owner.memory().status(context);
        if (!created) co_return created;
        const auto* address = owner.decompressor();
        co_await seastar::yield();
        EXPECT_EQ(owner.decompressor(), address);
        EXPECT_NE(owner.decompressor(), other.decompressor());
        result = work.poll(
          codec::error{
            errc::success, context.family, context.field, context.origin});
    }
    co_return result;
}

TEST(Lz4ContextTest, ContextStateSurvivesSuspensionAndCanceledOwnerCleanup) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto admitted = plan(lz4_direction::decompress, work).value();
    lz4_context other{admitted};
    ASSERT_TRUE(other.memory().status());
    auto pending = cancel_context(admitted, work, other);
    ASSERT_FALSE(pending.available());
    abort.request_abort();
    expect_error(pending.get(), errc::aborted);
    EXPECT_TRUE(other.memory().status());
    EXPECT_EQ(live_count(other.memory()), 1U);
}

} // namespace
} // namespace kwaque::compression
