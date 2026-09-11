#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/codec/error.h"
#include "src/codec/integer.h"
#include "src/codec/limits.h"
#include "src/codec/staging.h"

#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <new>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using namespace std::literals;
namespace codec = kwaque::codec;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::item_count;
using kwaque::bytes::allocation_charge_fn;
using kwaque::bytes::fragmented_buffer;

constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
constexpr byte_count parent_budget{64U * 1024U * 1024U};
constexpr codec::field_context context{.origin = 123, .family = 7, .field = 9};

template<typename Prefix>
concept assembly_prefix = requires(
  Prefix&& prefix,
  fragmented_buffer payload,
  const codec::limits& policy,
  const codec::operation_usage& live,
  allocation_charge_fn charge) {
    codec::assemble_buffer(
      std::forward<Prefix>(prefix),
      std::move(payload),
      policy,
      byte_count{},
      live,
      byte_count{},
      charge);
};

static_assert(assembly_prefix<std::span<const char>>);
static_assert(assembly_prefix<std::string_view>);
static_assert(assembly_prefix<std::array<char, 4>&>);
static_assert(!assembly_prefix<const char (&)[4]>);

// A conservative synthetic allocation profile for these bounded fixtures.
byte_count test_charge(byte_count requested) noexcept {
    const auto bytes = std::max(requested.value(), std::uint64_t{16});
    if (bytes > (std::uint64_t{1} << 62U)) {
        return byte_count{maximum};
    }
    return byte_count{2U * std::bit_ceil(bytes)};
}

std::uint64_t charge_calls{0};

byte_count counting_charge(byte_count requested) noexcept {
    ++charge_calls;
    return test_charge(requested);
}

byte_count undercharge(byte_count requested) noexcept {
    return byte_count{requested.value() == 0 ? 0 : requested.value() - 1U};
}

byte_count overflowing_charge(byte_count) noexcept {
    return byte_count{maximum};
}

fragmented_buffer fragments(std::initializer_list<std::string_view> values) {
    std::vector<seastar::temporary_buffer<char>> storage;
    for (const auto value : values) {
        seastar::temporary_buffer<char> fragment{value.size()};
        std::ranges::copy(value, fragment.get_write());
        storage.push_back(std::move(fragment));
    }
    return fragmented_buffer::copy_from_fragments(storage).value();
}

fragmented_buffer repeated_fragments(std::size_t count) {
    std::vector<seastar::temporary_buffer<char>> storage;
    for (std::size_t index = 0; index < count; ++index) {
        seastar::temporary_buffer<char> fragment{1};
        fragment.get_write()[0] = 'x';
        storage.push_back(std::move(fragment));
    }
    return fragmented_buffer::copy_from_fragments(storage).value();
}

codec::limits policy_with(codec::limits_config config) {
    return codec::limits::make(config).value();
}

byte_count total_cost(const kwaque::bytes::buffer_allocation_cost& cost) {
    const auto first = cost.backing.checked_add(cost.descriptors).value();
    return first.checked_add(cost.share_controls).value();
}

void expect_error(const auto& result, errc reason) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), reason);
    EXPECT_EQ(result.error().family(), context.family);
    EXPECT_EQ(result.error().field(), context.field);
    EXPECT_EQ(result.error().byte_offset(), context.origin);
}

codec::result<fragmented_buffer> assemble(
  std::string_view prefix,
  fragmented_buffer payload,
  const codec::limits& policy,
  byte_count logical_cap,
  const codec::operation_usage& other_live = {},
  byte_count remaining = parent_budget,
  allocation_charge_fn charge = test_charge) {
    return codec::assemble_buffer(
      std::span<const char>{prefix},
      std::move(payload),
      policy,
      logical_cap,
      other_live,
      remaining,
      charge,
      context);
}

TEST(CodecStagingTest, EmptyAssemblyNeedsNeitherBudgetNorChargeCalls) {
    charge_calls = 0;
    const auto made = assemble(
      ""sv,
      fragmented_buffer{},
      codec::limits::defaults(),
      byte_count{},
      {},
      byte_count{},
      counting_charge);
    ASSERT_TRUE(made.has_value());
    EXPECT_TRUE(made->empty());
    EXPECT_EQ(made->fragment_count(), 0U);
    EXPECT_EQ(charge_calls, 0U);
    expect_error(
      assemble(
        ""sv,
        fragmented_buffer{},
        codec::limits::defaults(),
        byte_count{},
        {},
        byte_count{},
        nullptr),
      errc::invalid_argument);
}

TEST(CodecStagingTest, PrefixAndSlicedPayloadPublishOnlyExactOwnedBytes) {
    auto parent = fragments({"xxpay"sv, "loadzz"sv});
    auto payload = parent.share(byte_count{2}, byte_count{7}).value();
    parent = fragmented_buffer{};
    std::string prefix{"head:"};
    codec::operation_usage other_live;
    other_live.retained_input = test_charge(byte_count{prefix.capacity() + 1U});
    auto made = assemble(
      prefix,
      std::move(payload),
      codec::limits::defaults(),
      byte_count{12},
      other_live);
    ASSERT_TRUE(made.has_value());
    prefix.assign(prefix.size(), 'x');
    EXPECT_EQ(made->size(), byte_count{12});
    EXPECT_TRUE(made->content_equals("head:payload"sv));
}

TEST(CodecStagingTest, ExistingLargeDonatedBackingIsNotANewAllocation) {
    auto payload = fragmented_buffer::copy_of(std::string(4097, 'z')).value();
    const auto* original = payload.fragment_at(0)->data();
    const auto cost = payload.allocation_cost(test_charge).value();
    codec::limits_config config;
    config.max_allocation_bytes = byte_count{512};
    ASSERT_GT(cost.largest_allocation, config.max_allocation_bytes);
    auto made = assemble(
      ""sv, std::move(payload), policy_with(config), byte_count{4097});
    ASSERT_TRUE(made.has_value());
    EXPECT_TRUE(made->content_equals(std::string(4097, 'z')));
    ASSERT_EQ(made->fragment_count(), 1U);
    EXPECT_EQ(made->fragment_at(0)->data(), original);
}

TEST(CodecStagingTest, NewCopiedOutputClampsServedAllocationsToTheCallerCap) {
    codec::limits_config config;
    config.max_allocation_bytes = byte_count{512};
    const std::string prefix(300, 'p');
    codec::operation_usage other_live;
    other_live.retained_input = test_charge(byte_count{prefix.capacity() + 1U});
    auto made = assemble(
      prefix,
      fragmented_buffer{},
      policy_with(config),
      byte_count{prefix.size()},
      other_live);
    ASSERT_TRUE(made.has_value());
    EXPECT_TRUE(made->content_equals(prefix));
    const auto cost = made->allocation_cost(test_charge);
    ASSERT_TRUE(cost.has_value());
    EXPECT_LE(cost->largest_allocation, config.max_allocation_bytes);
    EXPECT_LE(cost->backing, config.max_retained_bytes);
    for (const auto fragment : *made) {
        EXPECT_LE(
          test_charge(byte_count{fragment.size()}),
          config.max_allocation_bytes);
    }
}

TEST(CodecStagingTest, SlicedPayloadCannotHideItsRetainedBacking) {
    auto parent = fragmented_buffer::copy_of(std::string(4096, 'x')).value();
    auto payload = parent.share(byte_count{4095}, byte_count{1}).value();
    parent = fragmented_buffer{};
    const auto cost = payload.allocation_cost(test_charge).value();
    ASSERT_GT(cost.backing, payload.size());
    codec::limits_config config;
    config.max_retained_bytes = byte_count{cost.backing.value() - 1U};
    expect_error(
      assemble(""sv, std::move(payload), policy_with(config), byte_count{1}),
      errc::resource_exhausted);
}

TEST(CodecStagingTest, TrimmedDescriptorHistoryStillConsumesParentBudget) {
    auto payload = repeated_fragments(32);
    const auto before = payload.allocation_cost(test_charge).value();
    ASSERT_TRUE(payload.trim_front(byte_count{31}).has_value());
    const auto after = payload.allocation_cost(test_charge).value();
    ASSERT_EQ(payload.fragment_count(), 1U);
    EXPECT_EQ(after.descriptors, before.descriptors);
    ASSERT_GT(after.descriptors, payload.size());
    const auto remaining = byte_count{after.descriptors.value() - 1U};
    expect_error(
      assemble(
        ""sv,
        std::move(payload),
        codec::limits::defaults(),
        byte_count{1},
        {},
        remaining),
      errc::resource_exhausted);

    auto emptied = repeated_fragments(2);
    ASSERT_TRUE(emptied.trim_front(byte_count{2}).has_value());
    const auto empty_cost = emptied.allocation_cost(test_charge).value();
    ASSERT_TRUE(emptied.empty());
    ASSERT_GT(empty_cost.descriptors.value(), 0U);
    expect_error(
      assemble(
        ""sv,
        std::move(emptied),
        codec::limits::defaults(),
        byte_count{},
        {},
        byte_count{}),
      errc::resource_exhausted);
}

TEST(CodecStagingTest, LogicalAndResidualLimitsRejectBeforePublishing) {
    expect_error(
      assemble(
        "h"sv, fragments({"data"sv}), codec::limits::defaults(), byte_count{4}),
      errc::resource_exhausted);
    auto payload = fragments({"x"sv});
    const auto input_cost = payload.allocation_cost(test_charge).value();
    expect_error(
      assemble(
        ""sv,
        std::move(payload),
        codec::limits::defaults(),
        byte_count{1},
        {},
        byte_count{total_cost(input_cost).value() - 1U}),
      errc::resource_exhausted);

    codec::limits_config config;
    config.max_metadata_bytes = byte_count{16};
    codec::operation_usage metadata;
    metadata.decoded_metadata = byte_count{17};
    expect_error(
      assemble(
        ""sv, fragmented_buffer{}, policy_with(config), byte_count{}, metadata),
      errc::resource_exhausted);

    codec::operation_usage overflow;
    overflow.retained_input = byte_count{maximum};
    overflow.staged_output = byte_count{1};
    expect_error(
      assemble(
        ""sv,
        fragmented_buffer{},
        codec::limits::defaults(),
        byte_count{},
        overflow),
      errc::out_of_range);
}

TEST(CodecStagingTest, WorkAndFragmentLimitsBoundSynchronousAssembly) {
    codec::limits_config config;
    config.max_work_bytes = byte_count{3};
    expect_error(
      assemble(
        "head"sv, fragmented_buffer{}, policy_with(config), byte_count{4}),
      errc::resource_exhausted);
    config = codec::limits_config{};
    config.max_work_bytes = byte_count{3};
    expect_error(
      assemble(
        "h"sv,
        fragments({"a"sv, "b"sv, "c"sv}),
        policy_with(config),
        byte_count{4}),
      errc::resource_exhausted);
    config = codec::limits_config{};
    config.max_work_items = item_count{8};
    expect_error(
      assemble(""sv, repeated_fragments(2), policy_with(config), byte_count{2}),
      errc::resource_exhausted);
    config = codec::limits_config{};
    config.max_buffer_fragments = item_count{1};
    expect_error(
      assemble(""sv, repeated_fragments(2), policy_with(config), byte_count{2}),
      errc::resource_exhausted);
    config = codec::limits_config{};
    config.max_allocation_bytes = byte_count{1};
    expect_error(
      assemble("x"sv, fragmented_buffer{}, policy_with(config), byte_count{1}),
      errc::resource_exhausted);
}

TEST(CodecStagingTest, InvalidOrOverflowingAllocationChargesRemainTyped) {
    expect_error(
      assemble(
        ""sv,
        fragments({"x"sv}),
        codec::limits::defaults(),
        byte_count{1},
        {},
        parent_budget,
        undercharge),
      errc::invalid_argument);
    expect_error(
      assemble(
        ""sv,
        fragments({"x"sv}),
        codec::limits::defaults(),
        byte_count{1},
        {},
        parent_budget,
        overflowing_charge),
      errc::out_of_range);
}

TEST(CodecStagingTest, PreflightRejectionsNeedNoAllocation) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation failure injection is not enabled";
#else
    // The armed interval measures staging after error-category initialization.
    static_cast<void>(kwaque::error_category());
    struct rejection_case {
        byte_count logical_cap;
        byte_count remaining;
        allocation_charge_fn charge;
        errc reason;
    };
    const std::array cases{
      rejection_case{
        byte_count{}, parent_budget, test_charge, errc::resource_exhausted},
      rejection_case{
        byte_count{1}, byte_count{}, test_charge, errc::resource_exhausted},
      rejection_case{
        byte_count{1}, parent_budget, undercharge, errc::invalid_argument},
      rejection_case{
        byte_count{1}, parent_budget, overflowing_charge, errc::out_of_range}};
    for (const auto& test_case : cases) {
        auto payload = fragments({"x"sv});
        std::optional<codec::result<fragmented_buffer>> made;
        auto& injector = seastar::memory::local_failure_injector();
        const auto before = injector.alloc_count();
        injector.fail_after(0);
        try {
            made.emplace(assemble(
              ""sv,
              std::move(payload),
              codec::limits::defaults(),
              test_case.logical_cap,
              {},
              test_case.remaining,
              test_case.charge));
        } catch (...) {
            injector.cancel();
            throw;
        }
        const bool injected = injector.failed();
        const auto after = injector.alloc_count();
        injector.cancel();
        EXPECT_FALSE(injected);
        EXPECT_EQ(after, before);
        ASSERT_TRUE(made.has_value());
        expect_error(*made, test_case.reason);
    }
#endif
}

TEST(CodecStagingTest, AllocationFailuresPublishNothingAndPropagate) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation failure injection is not enabled";
#else
    const std::string large(4097, 'z');
    const std::string expected = "head:" + large + "end";
    std::size_t failures = 0;
    bool completed = false;
    for (std::uint64_t fail_after = 0; fail_after < 64; ++fail_after) {
        auto payload = fragments({large, "end"sv});
        std::optional<codec::result<fragmented_buffer>> made;
        bool threw = false;
        auto& injector = seastar::memory::local_failure_injector();
        injector.fail_after(fail_after);
        try {
            made.emplace(assemble(
              "head:"sv,
              std::move(payload),
              codec::limits::defaults(),
              byte_count{expected.size()}));
        } catch (const std::bad_alloc&) {
            threw = true;
        } catch (...) {
            injector.cancel();
            throw;
        }
        const bool injected = injector.failed();
        injector.cancel();
        if (injected) {
            ++failures;
            EXPECT_TRUE(threw);
            EXPECT_FALSE(made.has_value());
        } else {
            EXPECT_FALSE(threw);
            ASSERT_TRUE(made.has_value());
            ASSERT_TRUE(made->has_value());
            EXPECT_TRUE((*made)->content_equals(expected));
            completed = true;
            break;
        }
    }
    EXPECT_GT(failures, 0U);
    EXPECT_TRUE(completed);
#endif
}

} // namespace
