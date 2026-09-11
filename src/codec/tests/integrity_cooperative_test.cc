#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_test_support.h"
#include "src/codec/cooperative.h"
#include "src/codec/crc32c.h"
#include "src/codec/crc32c_cooperative.h"
#include "src/codec/digest.h"
#include "src/codec/error.h"
#include "src/codec/limits.h"
#include "src/codec/sha256.h"
#include "src/codec/sha256_cooperative.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/deleter.hh>
#include <seastar/core/preempt.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/later.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using namespace std::literals;
namespace codec = kwaque::codec;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::item_count;
using kwaque::bytes::fragmented_buffer;

constexpr codec::error anchor{errc::success, 2, 9, 42};
constexpr codec::sha256_digest empty_sha{
  0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4,
  0xc8, 0x99, 0x6f, 0xb9, 0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b,
  0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55};
constexpr codec::sha256_digest abc_sha{
  0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40,
  0xde, 0x5d, 0xae, 0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17,
  0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61, 0xf2, 0x00, 0x15, 0xad};

codec::limits policy_with(std::uint64_t bytes, std::uint64_t items) {
    codec::limits_config config;
    config.max_work_bytes = byte_count{bytes};
    config.max_work_items = item_count{items};
    return codec::limits::make(config).value();
}

fragmented_buffer
fragmented(std::span<const char> bytes, std::size_t fragment_size) {
    std::vector<seastar::temporary_buffer<char>> storage;
    for (std::size_t offset = 0; offset < bytes.size();
         offset += fragment_size) {
        const auto part = bytes.subspan(
          offset, std::min(fragment_size, bytes.size() - offset));
        seastar::temporary_buffer<char> fragment{part.size()};
        std::ranges::copy(part, fragment.get_write());
        storage.push_back(std::move(fragment));
    }
    return fragmented_buffer::copy_from_fragments(storage).value();
}

void expect_error(const auto& result, errc code) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), (codec::error{code, 2, 9, 42}));
}

TEST(
  IntegrityCooperativeTest, EmptyInputsPreserveSeedsAndChargeSharedEmptyWork) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto crc
      = codec::crc32c_cooperatively({}, work, 0x12345678U, anchor).get();
    ASSERT_TRUE(crc.has_value());
    EXPECT_EQ(*crc, 0x12345678U);
    const auto sha = codec::sha256_cooperatively({}, work, anchor).get();
    ASSERT_TRUE(sha.has_value());
    EXPECT_EQ(*sha, empty_sha);
    EXPECT_EQ(work.bytes_remaining(), work.byte_quantum());
    EXPECT_EQ(work.items_remaining(), item_count{252});
}

TEST(
  IntegrityCooperativeTest, EmptyDriverChildrenCannotResetTheirParentsBudget) {
    seastar::abort_source abort;
    codec::cooperative_work work{policy_with(8, 5), abort};
    for (std::uint64_t invocation = 1; invocation <= 20; ++invocation) {
        if (invocation % 2U == 0) {
            const auto result = codec::sha256_cooperatively({}, work).get();
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(*result, empty_sha);
        } else {
            const auto result = codec::crc32c_cooperatively({}, work).get();
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(*result, 0U);
        }
        EXPECT_EQ(work.bytes_remaining(), byte_count{8});
        EXPECT_EQ(
          work.items_remaining(),
          item_count{(5U - (2U * invocation) % 5U) % 5U});
    }
}

TEST(
  IntegrityCooperativeTest,
  AlgorithmsUseTheirParentsPolicyAndFinishBoundedCleanup) {
    seastar::abort_source abort;
    codec::cooperative_work work{policy_with(16, 32), abort};
    const auto crc
      = codec::crc32c_cooperatively(fragmented("abc"sv, 3), work).get();
    ASSERT_TRUE(crc.has_value());
    codec::crc32c expected;
    expected.extend("abc"sv);
    EXPECT_EQ(*crc, expected.value());
    const auto sha
      = codec::sha256_cooperatively(fragmented("abc"sv, 3), work).get();
    ASSERT_TRUE(sha.has_value());
    EXPECT_EQ(*sha, abc_sha);
    EXPECT_EQ(work.bytes_remaining(), byte_count{});
    EXPECT_EQ(work.items_remaining(), item_count{});
}

TEST(
  IntegrityCooperativeTest,
  FragmentShapesAndNarrowQuantaMatchSynchronousDigests) {
    struct shape {
        std::size_t bytes;
        std::size_t fragment_bytes;
    };
    const std::array shapes{
      shape{1, 1},
      shape{17, 1},
      shape{1024, 1},
      shape{4096, 7},
      shape{131072, 131072}};
    for (const auto shape : shapes) {
        std::vector<char> bytes(shape.bytes);
        for (std::size_t index = 0; index < bytes.size(); ++index) {
            bytes[index] = std::bit_cast<char>(
              static_cast<std::uint8_t>(index % 251U));
        }
        codec::crc32c expected_crc{0x13579bdfU};
        expected_crc.extend(std::span<const char>{bytes});
        codec::sha256_hasher expected_sha;
        expected_sha.update(bytes.data(), bytes.size());
        const auto sha_digest = std::move(expected_sha).final();
        for (const std::uint64_t quantum : {1U, 31U, 4096U, 65536U}) {
            SCOPED_TRACE(
              ::testing::Message()
              << shape.bytes << ':' << shape.fragment_bytes << ':' << quantum);
            seastar::abort_source abort;
            codec::cooperative_work work{policy_with(quantum, 3), abort};
            auto crc_input = fragmented(bytes, shape.fragment_bytes);
            const auto crc = codec::crc32c_cooperatively(
                               std::move(crc_input), work, 0x13579bdfU, anchor)
                               .get();
            // The buffer move contract guarantees an empty source.
            // NOLINTNEXTLINE(bugprone-use-after-move)
            EXPECT_TRUE(crc_input.empty());
            ASSERT_TRUE(crc.has_value());
            EXPECT_EQ(*crc, expected_crc.value());
            auto sha_input = fragmented(bytes, shape.fragment_bytes);
            const auto sha = codec::sha256_cooperatively(
                               std::move(sha_input), work, anchor)
                               .get();
            // The buffer move contract guarantees an empty source.
            // NOLINTNEXTLINE(bugprone-use-after-move)
            EXPECT_TRUE(sha_input.empty());
            ASSERT_TRUE(sha.has_value());
            EXPECT_EQ(*sha, sha_digest);
        }
    }
}

TEST(IntegrityCooperativeTest, OwnedSharedSlicesKeepTheirOriginalPresentation) {
    auto original = fragmented("xxabczz"sv, 2);
    auto crc_input = original.share(byte_count{2}, byte_count{3}).value();
    auto sha_input = original.share(byte_count{2}, byte_count{3}).value();
    ASSERT_TRUE(original.trim_front(byte_count{7}).has_value());
    seastar::abort_source abort;
    codec::cooperative_work work{policy_with(1, 1), abort};
    const auto crc
      = codec::crc32c_cooperatively(std::move(crc_input), work).get();
    ASSERT_TRUE(crc.has_value());
    codec::crc32c expected;
    expected.extend("abc"sv);
    EXPECT_EQ(*crc, expected.value());
    const auto sha
      = codec::sha256_cooperatively(std::move(sha_input), work).get();
    ASSERT_TRUE(sha.has_value());
    EXPECT_EQ(*sha, abc_sha);
}

TEST(
  IntegrityCooperativeTest,
  AlreadyRequestedAbortPrecedesWorkAndNativeConstruction) {
    seastar::abort_source abort;
    codec::cooperative_work work{policy_with(1, 1), abort};
    abort.request_abort_ex(
      std::make_exception_ptr(std::runtime_error{"custom abort"}));
    expect_error(
      codec::crc32c_cooperatively(fragmented("abc"sv, 1), work, 9, anchor)
        .get(),
      errc::aborted);
    expect_error(
      codec::sha256_cooperatively(fragmented("abc"sv, 1), work, anchor).get(),
      errc::aborted);
    EXPECT_EQ(work.bytes_remaining(), byte_count{});
    EXPECT_EQ(work.items_remaining(), item_count{});
}

TEST(
  IntegrityCooperativeTest,
  RecordedInputLimitsRejectBeforeHashingAndStillDrain) {
    codec::limits_config config;
    config.max_buffer_fragments = item_count{1};
    config.max_retained_bytes = byte_count{8};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    expect_error(
      codec::crc32c_cooperatively(fragmented("ab"sv, 1), work, 0, anchor).get(),
      errc::resource_exhausted);
    expect_error(
      codec::sha256_cooperatively(fragmented("123456789"sv, 9), work, anchor)
        .get(),
      errc::resource_exhausted);
    EXPECT_EQ(work.bytes_remaining(), byte_count{});
    EXPECT_EQ(work.items_remaining(), item_count{});
}

TEST(IntegrityCooperativeTest, PublishedSuccessIsNotChangedByALaterAbort) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto crc
      = codec::crc32c_cooperatively(fragmented("123456789"sv, 9), work).get();
    const auto sha
      = codec::sha256_cooperatively(fragmented("abc"sv, 3), work).get();
    ASSERT_TRUE(crc.has_value());
    ASSERT_TRUE(sha.has_value());
    abort.request_abort();
    EXPECT_EQ(*crc, 0xe3069283U);
    EXPECT_EQ(*sha, abc_sha);
    EXPECT_FALSE(work.poll().has_value());
}

fragmented_buffer abort_on_release(
  seastar::abort_source& abort, bool& released, std::size_t size) {
    auto memory = std::make_unique<char[]>(size);
    std::fill_n(memory.get(), size, 'x');
    auto* raw = memory.get();
    auto deleter = seastar::make_deleter(
      [memory = std::move(memory), &abort, &released] {
          static_cast<void>(memory);
          released = true;
          abort.request_abort();
      });
    auto storage = seastar::temporary_buffer<char>::maybe_unsafe_from_deleter(
      raw, size, std::move(deleter));
    return kwaque::bytes::fragmented_buffer_test_access::adopt_fragment(
             std::move(storage), byte_count{size})
      .value();
}

TEST(
  IntegrityCooperativeTest,
  AbortDuringFinalInputReleasePreventsEitherDigestPublication) {
    for (const bool sha : {false, true}) {
        seastar::abort_source abort;
        bool released = false;
        auto input = abort_on_release(abort, released, 1);
        codec::cooperative_work work{codec::limits::defaults(), abort};
        ASSERT_FALSE(released);
        ASSERT_FALSE(abort.abort_requested());
        if (sha) {
            expect_error(
              codec::sha256_cooperatively(std::move(input), work, anchor).get(),
              errc::aborted);
        } else {
            expect_error(
              codec::crc32c_cooperatively(std::move(input), work, 0, anchor)
                .get(),
              errc::aborted);
        }
        EXPECT_TRUE(released);
        EXPECT_TRUE(abort.abort_requested());
    }
}

TEST(
  IntegrityCooperativeTest,
  InputReleaseAbortDoesNotReplaceAnEarlierLimitFailure) {
    seastar::abort_source abort;
    bool released = false;
    auto input = abort_on_release(abort, released, 2);
    codec::limits_config config;
    config.max_retained_bytes = byte_count{1};
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    ASSERT_FALSE(released);
    ASSERT_FALSE(abort.abort_requested());
    expect_error(
      codec::crc32c_cooperatively(std::move(input), work, 0, anchor).get(),
      errc::resource_exhausted);
    EXPECT_TRUE(released);
    EXPECT_TRUE(abort.abort_requested());
}

struct abort_observation {
    bool pending;
    bool observed;
    bool aborted;
};

seastar::future<abort_observation> cancel_pending_hash(bool sha) {
    std::vector<char> bytes(65536, 'x');
    auto input = fragmented(bytes, bytes.size());
    seastar::abort_source abort;
    codec::cooperative_work work{policy_with(1, 1), abort};
    // Wait only for the public quota signal; do not alter the reactor monitor.
    const auto deadline = std::chrono::steady_clock::now()
                          + std::chrono::seconds(2);
    while (!seastar::need_preempt()
           && std::chrono::steady_clock::now() < deadline) {
    }
    if (!seastar::need_preempt()) {
        throw std::runtime_error("reactor quota did not request preemption");
    }
    bool observed = false;
    auto observer = seastar::yield().then([&abort, &observed] {
        observed = true;
        abort.request_abort();
    });
    bool pending = false;
    bool aborted = false;
    std::exception_ptr failure;
    try {
        if (sha) {
            auto completion = codec::sha256_cooperatively(
              std::move(input), work, anchor);
            pending = !completion.available();
            const auto result = co_await std::move(completion);
            aborted = !result
                      && result.error()
                           == codec::error{errc::aborted, 2, 9, 42};
        } else {
            auto completion = codec::crc32c_cooperatively(
              std::move(input), work, 0, anchor);
            pending = !completion.available();
            const auto result = co_await std::move(completion);
            aborted = !result
                      && result.error()
                           == codec::error{errc::aborted, 2, 9, 42};
        }
    } catch (...) {
        failure = std::current_exception();
    }
    const bool during_work = observed;
    co_await std::move(observer);
    if (failure) {
        std::rethrow_exception(failure);
    }
    co_return abort_observation{pending, during_work, aborted};
}

TEST(IntegrityCooperativeTest, QueuedAbortInterruptsPendingCrcWork) {
    const auto observed = cancel_pending_hash(false).get();
    EXPECT_TRUE(observed.pending);
    EXPECT_TRUE(observed.observed);
    EXPECT_TRUE(observed.aborted);
}

TEST(IntegrityCooperativeTest, QueuedAbortInterruptsPendingShaWork) {
    const auto observed = cancel_pending_hash(true).get();
    EXPECT_TRUE(observed.pending);
    EXPECT_TRUE(observed.observed);
    EXPECT_TRUE(observed.aborted);
}

TEST(IntegrityCooperativeTest, WarmedCrcHasNoInjectableAllocationPoints) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation failure injection is not enabled";
#else
    // Warm native engine initialization before observing the driver. Coroutine
    // frames use critical allocations, which the injector neither counts nor
    // fails; this check does not claim that the driver allocates no frames.
    codec::crc32c warmup;
    const std::array<char, 65> warm_bytes{};
    warmup.extend(warm_bytes);
    EXPECT_NE(warmup.value(), 0U);
    codec::crc32c expected;
    expected.extend("abc"sv);
    seastar::abort_source warm_abort;
    codec::cooperative_work warm_work{policy_with(2, 2), warm_abort};
    ASSERT_TRUE(
      codec::crc32c_cooperatively(fragmented("abc"sv, 1), warm_work)
        .get()
        .has_value());
    seastar::abort_source abort;
    codec::cooperative_work work{policy_with(2, 2), abort};
    auto input = fragmented("abc"sv, 1);
    std::optional<codec::result<codec::crc32c::value_type>> result;
    bool threw = false;
    auto& injector = seastar::memory::local_failure_injector();
    const auto before = injector.alloc_count();
    injector.fail_after(0);
    try {
        result.emplace(
          codec::crc32c_cooperatively(std::move(input), work, 0, anchor).get());
    } catch (const std::bad_alloc&) {
        threw = true;
    } catch (...) {
        injector.cancel();
        throw;
    }
    const auto after = injector.alloc_count();
    const bool injected = injector.failed();
    injector.cancel();
    EXPECT_FALSE(threw);
    EXPECT_FALSE(injected);
    EXPECT_EQ(after, before);
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->has_value());
    EXPECT_EQ(**result, expected.value());
#endif
}

} // namespace
