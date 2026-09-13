#include "src/model/record_scan.h"
#include "src/model/tests/model_bench_fixture.h"
#include "src/model/tests/model_bench_reference.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/preempt.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/later.hh>

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <exception>
#include <malloc.h>
#include <optional>
#include <string>
#include <utility>

namespace {
namespace model = kwaque::model;
namespace bench = model::bench;
namespace codec = kwaque::codec;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::item_count;
using kwaque::bytes::fragmented_buffer;
using kwaque::bytes::fragmented_buffer_parser;

TEST(ModelQualificationTest, CheckedFramingAndModelPathsAgreeOnCompleteBodies) {
    for (const auto width : std::array<std::size_t, 3>{0, 7, 67}) {
        bench::model_fixture fixture{8, 64, width};
        fixture.initialize().get();
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        EXPECT_EQ(
          bench::checked_size(*fixture.value, work.policy()),
          bench::codec_size(*fixture.value, work.policy()));
        for (int kind = 0; kind < 3; ++kind) {
            auto& wire = kind == 0   ? fixture.submitted_wire
                         : kind == 1 ? fixture.assigned_wire
                                     : fixture.sparse_wire;
            fragmented_buffer_parser left{wire.share()}, right{wire.share()};
            const auto left_budget = codec::reserve_decode_input(
                                       left, work.policy(), fixture.memory())
                                       .value();
            const auto right_budget = codec::reserve_decode_input(
                                        right, work.policy(), fixture.memory())
                                        .value();
            if (kind == 0) {
                auto a = bench::checked_decode_submitted(
                           left, bench::expected_context(), left_budget, work)
                           .get();
                auto b = bench::codec_decode_submitted(
                           right, bench::expected_context(), right_budget, work)
                           .get();
                ASSERT_TRUE(a.has_value());
                ASSERT_TRUE(b.has_value());
                EXPECT_EQ(a->value.context(), b->value.context());
                EXPECT_EQ(a->remaining, b->remaining);
                auto x = bench::checked_encode_submitted(
                           std::move(a->value),
                           work,
                           fixture.remaining,
                           bench::capacity_bound)
                           .get();
                auto y = bench::codec_encode_submitted(
                           std::move(b->value),
                           work,
                           fixture.remaining,
                           bench::capacity_bound)
                           .get();
                ASSERT_TRUE(x.has_value());
                ASSERT_TRUE(y.has_value());
                EXPECT_TRUE(x->content_equals(*y));
                EXPECT_TRUE(x->content_equals(wire));
            } else {
                auto a = bench::checked_decode_assigned(
                           left, bench::expected_context(), left_budget, work)
                           .get();
                auto b = bench::codec_decode_assigned(
                           right, bench::expected_context(), right_budget, work)
                           .get();
                ASSERT_TRUE(a.has_value());
                ASSERT_TRUE(b.has_value());
                EXPECT_EQ(a->value.context(), b->value.context());
                EXPECT_EQ(a->remaining, b->remaining);
                EXPECT_EQ(
                  a->fingerprint_verification, b->fingerprint_verification);
                EXPECT_EQ(a->value.fingerprint(), b->value.fingerprint());
                auto x = bench::checked_encode_assigned(
                           std::move(a->value),
                           work,
                           fixture.remaining,
                           bench::capacity_bound)
                           .get();
                auto y = bench::codec_encode_assigned(
                           std::move(b->value),
                           work,
                           fixture.remaining,
                           bench::capacity_bound)
                           .get();
                ASSERT_TRUE(x.has_value());
                ASSERT_TRUE(y.has_value());
                EXPECT_TRUE(x->content_equals(*y));
                EXPECT_TRUE(x->content_equals(wire));
            }
            EXPECT_TRUE(left.at_end());
            EXPECT_TRUE(right.at_end());
        }
    }
}

TEST(ModelQualificationTest, CheckedFramingMatchesErrorsAndCallerRollback) {
    bench::model_fixture fixture{8, 64, 7};
    fixture.initialize().get();
    for (int mutation = 0; mutation < 5; ++mutation) {
        std::string raw;
        for (auto part : fixture.submitted_wire)
            raw.append(part.data(), part.size());
        if (mutation == 0) raw[0] ^= 1;
        if (mutation == 1) raw[28] ^= 1;
        if (mutation == 2) raw.back() ^= 1;
        if (mutation == 3) raw.resize(raw.size() - 1U);
        fragmented_buffer_parser left{fragmented_buffer::copy_of(raw).value()},
          right{fragmented_buffer::copy_of(raw).value()};
        auto expected = bench::expected_context();
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto a_memory = codec::reserve_decode_input(
                                left, work.policy(), fixture.memory())
                                .value();
        const auto b_memory = codec::reserve_decode_input(
                                right, work.policy(), fixture.memory())
                                .value();
        if (mutation == 4) abort.request_abort();
        auto a = bench::checked_decode_submitted(left, expected, a_memory, work)
                   .get();
        auto b = bench::codec_decode_submitted(right, expected, b_memory, work)
                   .get();
        ASSERT_FALSE(a.has_value());
        ASSERT_FALSE(b.has_value());
        EXPECT_EQ(a.error(), b.error());
        EXPECT_EQ(left.bytes_consumed(), byte_count{});
        EXPECT_EQ(right.bytes_consumed(), byte_count{});
        EXPECT_EQ(left.checkpoint_depth(), 0U);
        EXPECT_EQ(right.checkpoint_depth(), 0U);
    }
}

TEST(ModelQualificationTest, ThousandsOfHeadersFitActualDescriptorBudget) {
    bench::model_fixture fixture{4096, 0, 65536};
    fixture.initialize().get();
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    fragmented_buffer_parser input{fixture.assigned_wire.share()};
    const auto input_cost
      = input.allocation_cost(bench::capacity_bound).value();
    const auto budget = codec::reserve_decode_input(
                          input, work.policy(), fixture.memory())
                          .value();
    const auto allocations = seastar::memory::stats().mallocs();
    const auto tasks = seastar::engine().get_sched_stats().tasks_processed;
    auto decoded = model::decode_assigned_batch(
                     input, bench::expected_context(), budget, work)
                     .get();
    const auto allocation_count = seastar::memory::stats().mallocs()
                                  - allocations;
    const auto task_count = seastar::engine().get_sched_stats().tasks_processed
                            - tasks;
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->value.header_count(), item_count{4096});
    EXPECT_EQ(decoded->value.context().retained_count(), item_count{4096});
    const auto charged = input_cost.descriptors.value()
                         + input_cost.share_controls.value()
                         + budget.metadata_remaining.value()
                         - decoded->remaining.metadata_remaining.value()
                         + sizeof(model::record_layout) + sizeof(*decoded);
    EXPECT_LT(charged, 1U << 20U);
    RecordProperty(
      "decoded_metadata_capacity_bound_bytes", std::to_string(charged));
    RecordProperty(
      "record_layout_inline_bytes",
      std::to_string(sizeof(model::record_layout)));
    RecordProperty(
      "native_decode_allocations", std::to_string(allocation_count));
    RecordProperty("native_decode_tasks", std::to_string(task_count));
}

TEST(
  ModelQualificationTest, MaximumFragmentCountUsesBoundedMetadataAllocations) {
    // One header and this value length give an exact 32768-byte assigned
    // envelope. Splitting at 32 bytes reaches the substrate's 1024 fragments.
    bench::model_fixture fixture{1, 32539, 0};
    fixture.initialize().get();
    ASSERT_EQ(fixture.assigned_wire.size(), byte_count{32768});
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto source
      = bench::copy_layout(
          fixture.assigned_wire.share(), 32, work, fixture.remaining, 1024)
          .get();
    ASSERT_EQ(source.fragment_count(), 1024U);
    const auto cost = source.allocation_cost(bench::capacity_bound).value();
    EXPECT_LE(
      cost.largest_allocation, work.policy().config().max_allocation_bytes);
    fragmented_buffer_parser input{std::move(source)};
    auto decoded = model::decode_assigned_batch(
                     input,
                     bench::expected_context(),
                     codec::reserve_decode_input(
                       input, work.policy(), fixture.memory())
                       .value(),
                     work)
                     .get();
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(input.at_end());
    EXPECT_LE(
      decoded->value.records()
        .allocation_cost(bench::capacity_bound)
        ->largest_allocation,
      work.policy().config().max_allocation_bytes);
}

TEST(ModelQualificationTest, TinyViewsKeepBackingAndPromotionReservations) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    std::string text(65536, 'x');
    auto owner = fragmented_buffer::copy_of(text).value();
    const auto base = (*owner.begin()).data();
    const auto observed = ::malloc_usable_size(const_cast<char*>(base));
    const auto before = owner.allocation_cost(bench::capacity_bound).value();
    auto tiny = owner.share(byte_count{101}, byte_count{1}).value();
    owner = fragmented_buffer{};
    EXPECT_EQ(tiny.size(), byte_count{1});
    EXPECT_GE(tiny.retained_bytes(), byte_count{65536});
    const auto after = tiny.allocation_cost(bench::capacity_bound).value();
    EXPECT_GE(after.backing.value(), observed);
    EXPECT_EQ(after.backing, before.backing);
    EXPECT_GT(after.share_controls.value(), 0U);
    RecordProperty(
      "tiny_view_observed_backing_bytes", std::to_string(observed));
    RecordProperty(
      "tiny_view_reserved_backing_bytes",
      std::to_string(after.backing.value()));
}

TEST(
  ModelQualificationTest,
  NativeCapacityProfileSurvivesWarmAndChurnedAllocations) {
    bench::qualify_allocator();
    std::array<seastar::temporary_buffer<char>, 64> live;
    constexpr std::array<std::size_t, 5> sizes{32, 1024, 8192, 16384, 65536};
    for (int round = 0; round < 3; ++round) {
        for (std::size_t i = 0; i < live.size(); ++i) {
            const auto size = sizes[i % sizes.size()];
            live[i] = seastar::temporary_buffer<char>{size};
            EXPECT_LE(
              ::malloc_usable_size(live[i].get_write()),
              bench::capacity_bound(byte_count{size}).value());
            if (i % 16U == 15U) seastar::thread::maybe_yield();
        }
        for (std::size_t i = 0; i < live.size(); i += 2)
            live[i] = seastar::temporary_buffer<char>{};
        bench::qualify_allocator();
    }
}

seastar::future<> observe(bool& active, std::uint64_t& ticks) {
    while (active) {
        co_await seastar::yield();
        if (active) ++ticks;
    }
}
TEST(
  ModelQualificationTest,
  MaximumDecodeAllowsControlProgressAndReportsKnownBacking) {
    bench::model_fixture fixture{8, 1048565, 65536};
    fixture.initialize().get();
    std::uint64_t observed_backing = 0;
    // copy_layout created these exact allocation bases; never measure an
    // interior pointer from a returned slice with malloc_usable_size.
    for (const auto fragment : fixture.assigned_wire)
        observed_backing += ::malloc_usable_size(
          const_cast<char*>(fragment.data()));
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    fragmented_buffer_parser input{fixture.assigned_wire.share()};
    const auto budget = codec::reserve_decode_input(
                          input, work.policy(), fixture.memory())
                          .value();
    const auto deadline = std::chrono::steady_clock::now()
                          + std::chrono::seconds{2};
    while (!seastar::need_preempt()
           && std::chrono::steady_clock::now() < deadline) {
    }
    ASSERT_TRUE(seastar::need_preempt());
    bool active = true;
    std::uint64_t ticks = 0;
    auto observer = observe(active, ticks);
    std::optional<codec::result<model::decoded_assigned_batch>> result;
    std::exception_ptr exception;
    bool pending = false;
    try {
        auto decoded = model::decode_assigned_batch(
          input, bench::expected_context(), budget, work);
        pending = !decoded.available();
        result.emplace(decoded.get());
    } catch (...) {
        exception = std::current_exception();
    }
    // The suspended observer reads this flag before get() joins it.

    // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
    active = false;
    observer.get();
    if (exception) std::rethrow_exception(exception);
    EXPECT_TRUE(pending);
    EXPECT_GT(ticks, 0U);
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->has_value());
    EXPECT_EQ((*result)->value.records().size(), byte_count{8U << 20U});
    EXPECT_GE(
      (*result)
        ->value.records()
        .allocation_cost(bench::capacity_bound)
        ->backing.value(),
      observed_backing);
    RecordProperty("maximum_decode_control_ticks", std::to_string(ticks));
    RecordProperty(
      "maximum_decode_known_backing_bytes", std::to_string(observed_backing));
    // Observed backing and capacity equations are independent of
    // task/allocation counters. None is a measurement of transient process RSS
    // or frame peaks.
}
} // namespace
