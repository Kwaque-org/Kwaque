#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_test_support.h"
#include "src/codec/cooperative.h"
#include "src/codec/staging_cooperative.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/deleter.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace {

namespace codec = kwaque::codec;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::item_count;
using kwaque::bytes::fragmented_buffer;
using native_fragment = seastar::temporary_buffer<char>;
using namespace std::literals;

constexpr byte_count parent_budget{64U * 1024U * 1024U};
constexpr codec::field_context context{.origin = 71, .family = 4, .field = 8};

// Synthetic conservative served-capacity bound for these bounded fixtures.
byte_count charge(byte_count request) noexcept {
    if (request.value() > (std::uint64_t{1} << 62U)) {
        return byte_count{std::numeric_limits<std::uint64_t>::max()};
    }
    return byte_count{
      2U * std::bit_ceil(std::max(request.value(), std::uint64_t{16}))};
}

fragmented_buffer text(std::string_view value) {
    return fragmented_buffer::copy_of(value).value();
}

codec::result<fragmented_buffer> assemble(
  fragmented_buffer&& prefix,
  fragmented_buffer&& payload,
  codec::cooperative_work& work,
  byte_count cap,
  byte_count remaining = parent_budget,
  kwaque::bytes::allocation_charge_fn accounting = charge) {
    return codec::assemble_buffer_cooperatively(
             std::move(prefix),
             std::move(payload),
             work,
             cap,
             {},
             remaining,
             accounting,
             context)
      .get();
}

void expect_error(const auto& value, errc code) {
    ASSERT_FALSE(value.has_value());
    EXPECT_EQ(
      value.error(),
      codec::error(code, context.family, context.field, context.origin));
}

TEST(CooperativeStagingTest, EmptyInputsNeedNoEntryCapacityOrMemory) {
    codec::limits_config config;
    config.max_work_bytes = byte_count{1};
    config.max_work_items = item_count{1};
    config.max_allocation_bytes = byte_count{1};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    auto prefix = fragmented_buffer{};
    auto payload = fragmented_buffer{};
    auto result = assemble(
      std::move(prefix), std::move(payload), work, byte_count{}, byte_count{});
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->empty());
    expect_error(
      assemble(
        fragmented_buffer{},
        fragmented_buffer{},
        work,
        byte_count{},
        byte_count{},
        nullptr),
      errc::invalid_argument);
}

TEST(CooperativeStagingTest, RejectsTheSameOwnerBeforeMovingIt) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto input = text("data"sv);
    expect_error(
      assemble(std::move(input), std::move(input), work, byte_count{8}),
      errc::invalid_argument);
    // Aliased rvalue references reject before either input is moved.
    // NOLINTNEXTLINE(bugprone-use-after-move)
    EXPECT_TRUE(input.content_equals("data"sv));
}

TEST(CooperativeStagingTest, EmptyChildrenConsumeTheirSharedResidualWork) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    ASSERT_TRUE(assemble(
                  fragmented_buffer{},
                  fragmented_buffer{},
                  work,
                  byte_count{},
                  byte_count{})
                  .has_value());
    const auto after_first = work.items_remaining();
    EXPECT_LT(after_first, work.item_quantum());
    ASSERT_TRUE(assemble(
                  fragmented_buffer{},
                  fragmented_buffer{},
                  work,
                  byte_count{},
                  byte_count{})
                  .has_value());
    EXPECT_LT(work.items_remaining(), after_first);
}

TEST(
  CooperativeStagingTest, PrefixAndPayloadTransferExactBytesWithNarrowQuanta) {
    codec::limits_config config;
    config.max_work_bytes = byte_count{8};
    config.max_work_items = item_count{8};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    auto prefix = text("header:"sv);
    auto payload = text("abcdefghijklmnopqrstuvw"sv);
    const auto result = assemble(
      std::move(prefix), std::move(payload), work, byte_count{30});
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->content_equals("header:abcdefghijklmnopqrstuvw"sv));
    // Assembly transfers both buffers, whose move contract empties the sources.
    // NOLINTBEGIN(bugprone-use-after-move)
    EXPECT_TRUE(prefix.empty());
    EXPECT_TRUE(payload.empty());
    // NOLINTEND(bugprone-use-after-move)
}

TEST(
  CooperativeStagingTest, LargeDonationsPreserveBackingUnderSmallCopyLimits) {
    native_fragment storage{128U * 1024U};
    std::fill_n(storage.get_write(), storage.size(), 'z');
    auto payload = fragmented_buffer::copy_from_fragment(storage).value();
    const auto* original = payload.fragment_at(0)->data();
    codec::limits_config config;
    config.max_work_bytes = byte_count{2};
    config.max_work_items = item_count{8};
    config.max_allocation_bytes = byte_count{512};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    const auto result = assemble(
      fragmented_buffer{},
      std::move(payload),
      work,
      byte_count{storage.size()});
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->fragment_count(), 1U);
    EXPECT_EQ(result->fragment_at(0)->data(), original);
    EXPECT_EQ(result->size(), byte_count{128U * 1024U});
}

TEST(CooperativeStagingTest, MaximumPackingLayoutDoesNotBecomeOneLargeCopy) {
    std::vector<native_fragment> fragments;
    fragments.reserve(1024);
    for (std::size_t index = 0; index < 1024; ++index) {
        native_fragment fragment{4096};
        std::fill_n(
          fragment.get_write(),
          fragment.size(),
          static_cast<char>(index % 127U));
        fragments.push_back(std::move(fragment));
    }
    auto payload = fragmented_buffer::copy_from_fragments(fragments).value();
    fragments.clear();
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto result = assemble(
      fragmented_buffer{},
      std::move(payload),
      work,
      byte_count{4U * 1024U * 1024U});
    ASSERT_TRUE(result.has_value());
    EXPECT_LE(result->fragment_count(), 1024U);
    EXPECT_EQ(result->size(), byte_count{4U * 1024U * 1024U});
    std::size_t offset = 0;
    bool matched = true;
    for (const auto fragment : *result) {
        for (const auto value : fragment.bytes()) {
            matched = matched
                      && value == static_cast<char>((offset / 4096U) % 127U);
            ++offset;
        }
        seastar::thread::maybe_yield();
    }
    EXPECT_TRUE(matched);
}

TEST(CooperativeStagingTest, LogicalBackingDescriptorAndParentCapsIntersect) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    expect_error(
      assemble(text("head"sv), text("data"sv), work, byte_count{7}),
      errc::resource_exhausted);
    expect_error(
      assemble(
        text("x"sv), fragmented_buffer{}, work, byte_count{1}, byte_count{}),
      errc::resource_exhausted);
    auto source = text("x"sv);
    ASSERT_TRUE(source.trim_front(byte_count{1}).has_value());
    expect_error(
      assemble(
        fragmented_buffer{},
        std::move(source),
        work,
        byte_count{},
        byte_count{}),
      errc::resource_exhausted);
    codec::limits_config config;
    config.max_allocation_bytes = byte_count{1};
    codec::cooperative_work narrow{codec::limits::make(config).value(), abort};
    expect_error(
      assemble(text("x"sv), fragmented_buffer{}, narrow, byte_count{1}),
      errc::resource_exhausted);
}

TEST(CooperativeStagingTest, AbortWithAnArbitraryExceptionIsATypedFailure) {
    seastar::abort_source abort;
    abort.request_abort_ex(
      std::make_exception_ptr(std::runtime_error("external abort")));
    codec::cooperative_work work{codec::limits::defaults(), abort};
    expect_error(
      assemble(text("head"sv), text("data"sv), work, byte_count{8}),
      errc::aborted);
}

TEST(CooperativeStagingTest, AbortFromConsumedInputReleasePreventsPublication) {
    seastar::abort_source abort;
    bool released = false;
    auto memory = std::make_unique<char[]>(1);
    memory[0] = 'x';
    auto* raw = memory.get();
    auto deleter = seastar::make_deleter(
      [memory = std::move(memory), &abort, &released] {
          static_cast<void>(memory);
          released = true;
          abort.request_abort();
      });
    auto storage = native_fragment::maybe_unsafe_from_deleter(
      raw, 1, std::move(deleter));
    auto input = kwaque::bytes::fragmented_buffer_test_access::adopt_fragment(
                   std::move(storage), byte_count{1})
                   .value();
    ASSERT_FALSE(released);
    ASSERT_FALSE(abort.abort_requested());
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto result = assemble(
      fragmented_buffer{}, std::move(input), work, byte_count{1});
    EXPECT_TRUE(released);
    expect_error(result, errc::aborted);
}

TEST(CooperativeStagingTest, EveryObservedAllocationFailurePublishesNothing) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation failure injection is disabled";
#else
    static_cast<void>(kwaque::error_category());
    bool completed = false;
    std::size_t failures = 0;
    for (std::uint64_t ordinal = 0; ordinal < 128; ++ordinal) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto prefix = text("header:"sv);
        auto payload = text("payload"sv);
        std::optional<codec::result<fragmented_buffer>> produced;
        bool bad_alloc = false;
        auto& injector = seastar::memory::local_failure_injector();
        injector.fail_after(ordinal);
        try {
            produced.emplace(assemble(
              std::move(prefix), std::move(payload), work, byte_count{14}));
        } catch (const std::bad_alloc&) {
            bad_alloc = true;
        } catch (...) {
            injector.cancel();
            throw;
        }
        const bool injected = injector.failed();
        injector.cancel();
        if (injected) {
            ++failures;
            EXPECT_TRUE(bad_alloc);
            EXPECT_FALSE(produced.has_value());
        } else {
            ASSERT_TRUE(produced.has_value());
            ASSERT_TRUE(produced->has_value());
            EXPECT_TRUE((**produced).content_equals("header:payload"sv));
            completed = true;
            break;
        }
    }
    EXPECT_GT(failures, 0U);
    EXPECT_TRUE(completed);
#endif
}

} // namespace
