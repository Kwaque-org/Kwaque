#include "src/bytes/fragmented_buffer_builder.h"
#include "src/bytes/fragmented_buffer_test_support.h"
#include "src/compression/tests/test_support.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/deleter.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <array>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <type_traits>
#include <utility>

namespace kwaque::compression {
namespace {
using namespace testing;
using bytes::fragmented_buffer;

TEST(CompressionTest, CodecIdsAndDefault) {
    EXPECT_EQ(default_codec, codec_id::none);
    EXPECT_EQ(parse_codec_id(0).value(), codec_id::none);
    EXPECT_EQ(parse_codec_id(1).value(), codec_id::lz4);
    for (unsigned value = 2; value < 256; ++value)
        expect_error(
          parse_codec_id(static_cast<std::uint8_t>(value), context),
          errc::unsupported_format);
    static_assert(!std::is_copy_constructible_v<owned_result>);
    static_assert(std::is_nothrow_move_constructible_v<owned_result>);
}

TEST(CompressionTest, NoneTransfersBackingAndAllDescriptorCapacity) {
    bytes::fragmented_buffer_builder builder;
    ASSERT_TRUE(builder.reserve_fragments(item_count{100}));
    ASSERT_TRUE(builder.append(std::string_view{"payload"}));
    auto input = builder.finish().value();
    const auto cost = input.allocation_cost(charge).value();
    const auto* address = (*input.begin()).data();
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    // No extra allocation budget is needed: the input is already reserved.
    const codec::decode_budget memory{{}, {}, charge};
    auto result
      = transfer_none(std::move(input), byte_count{7}, work, memory).get();
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->value.content_equals("payload"));
    EXPECT_EQ((*result->value.begin()).data(), address);
    EXPECT_EQ(result->retained, cost);
    EXPECT_EQ(result->remaining, memory);
    EXPECT_GT(
      cost.descriptors.value(),
      100U * fragmented_buffer::fragment_descriptor_size() - 1U);
    // Consuming entry leaves the donor empty by contract.
    // NOLINTNEXTLINE(bugprone-use-after-move)
    EXPECT_TRUE(input.empty());
}

TEST(CompressionTest, EmptyNoneKeepsReservedDescriptorsWithNarrowWork) {
    bytes::fragmented_buffer_builder builder;
    ASSERT_TRUE(builder.reserve_fragments(item_count{33}));
    auto input = builder.finish().value();
    const auto cost = input.allocation_cost(charge).value();
    ASSERT_GT(cost.descriptors.value(), 0U);
    auto config = codec::limits_config{};
    config.max_work_bytes = byte_count{1};
    config.max_work_items = item_count{1};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    auto result = transfer_none(std::move(input), {}, work, budget()).get();
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->value.empty());
    EXPECT_EQ(result->retained, cost);
    EXPECT_EQ(result->value.allocation_cost(charge).value(), cost);
}

TEST(CompressionTest, TinySliceKeepsLargeBackingAndSurvivesOriginal) {
    auto input = fragmented_buffer::copy_of(std::string(65536, 'x')).value();
    auto slice = input.share(byte_count{111}, byte_count{1}).value();
    input = fragmented_buffer{};
    const auto cost = slice.allocation_cost(charge).value();
    ASSERT_GE(cost.backing.value(), 65536U);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const codec::field_context near_end{
      .origin = std::numeric_limits<std::uint64_t>::max() - 1U};
    auto result = transfer_none(
                    std::move(slice), byte_count{1}, work, budget(), near_end)
                    .get();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->retained, cost);
    EXPECT_TRUE(result->value.content_equals("x"));
}

TEST(CompressionTest, NonePreservesEveryFragmentAtTheStructuralLimit) {
    bytes::fragmented_buffer_builder builder{
      {.initial_fragment_bytes = byte_count{1},
       .max_fragment_bytes = byte_count{1},
       .max_total_bytes = byte_count{1024},
       .max_retained_bytes = byte_count{1024},
       .max_fragments = 1024}};
    ASSERT_TRUE(builder.reserve_fragments(item_count{1024}));
    for (std::size_t index = 0; index < 1024; ++index) {
        ASSERT_TRUE(builder.append(std::string_view{"x"}));
        if (index % 128U == 127U) seastar::thread::maybe_yield();
    }
    auto input = builder.finish().value();
    const auto cost = input.allocation_cost(charge).value();
    const auto first = input.fragment_at(0)->data();
    const auto last = input.fragment_at(1023)->data();
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto result
      = transfer_none(std::move(input), byte_count{1024}, work, budget()).get();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->retained, cost);
    EXPECT_EQ(result->value.fragment_count(), 1024U);
    EXPECT_EQ(result->value.fragment_at(0)->data(), first);
    EXPECT_EQ(result->value.fragment_at(1023)->data(), last);
}

TEST(CompressionTest, NoneRejectsUnequalLengthsAndNarrowedCaps) {
    for (const auto expected : {0U, 2U, 4U}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto input
          = fragmented_buffer::copy_of(std::string_view{"abc"}).value();
        expect_error(
          transfer_none(
            std::move(input), byte_count{expected}, work, budget(), context)
            .get(),
          errc::malformed_data);
    }
    for (auto member :
         {&codec::limits_config::max_expanded_batch_bytes,
          &codec::limits_config::max_encoded_body_bytes,
          &codec::limits_config::max_retained_bytes}) {
        auto config = codec::limits_config{};
        config.*member = byte_count{2};
        seastar::abort_source abort;
        codec::cooperative_work work{
          codec::limits::make(config).value(), abort};
        auto input
          = fragmented_buffer::copy_of(std::string_view{"abc"}).value();
        expect_error(
          transfer_none(
            std::move(input), byte_count{3}, work, budget(), context)
            .get(),
          errc::resource_exhausted);
    }
}

TEST(CompressionTest, NoneOwnsWitnessThroughReturnAndCancellationCleanup) {
    for (const bool cancel : {false, true}) {
        auto witness = std::make_shared<bool>(false);
        auto memory = std::make_unique<char[]>(1);
        memory[0] = 'x';
        auto* raw = memory.get();
        auto deleter = seastar::make_deleter(
          [memory = std::move(memory), witness] {
              static_cast<void>(memory);
              *witness = true;
          });
        auto storage
          = seastar::temporary_buffer<char>::maybe_unsafe_from_deleter(
            raw, 1, std::move(deleter));
        auto input = bytes::fragmented_buffer_test_access::adopt_fragment(
                       std::move(storage), byte_count{1})
                       .value();
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        if (cancel) abort.request_abort();
        {
            auto result
              = transfer_none(
                  std::move(input), byte_count{1}, work, budget(), context)
                  .get();
            if (cancel) {
                expect_error(result, errc::aborted);
                EXPECT_TRUE(*witness);
            } else {
                ASSERT_TRUE(result.has_value());
                EXPECT_FALSE(*witness);
            }
        }
        EXPECT_TRUE(*witness);
    }
}

TEST(CompressionTest, NoneRejectsMissingAndUnderchargingProfiles) {
    const auto undercharge = +[](byte_count) noexcept { return byte_count{}; };
    for (auto accounting :
         {bytes::allocation_charge_fn{nullptr}, undercharge}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto input
          = fragmented_buffer::copy_of(std::string_view{"abc"}).value();
        auto memory = budget();
        memory.charge = accounting;
        expect_error(
          transfer_none(std::move(input), byte_count{3}, work, memory, context)
            .get(),
          errc::invalid_argument);
    }
}

} // namespace
} // namespace kwaque::compression
