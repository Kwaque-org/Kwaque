#include "src/base/error.h"
#include "src/base/units.h"
#include "src/codec/cooperative.h"
#include "src/codec/error.h"
#include "src/codec/limits.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/later.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <new>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace {

namespace codec = kwaque::codec;
using codec::cooperative_work;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::item_count;

constexpr codec::error anchor{errc::success, 7, 13, 101};

static_assert(!std::is_copy_constructible_v<cooperative_work>);
static_assert(!std::is_move_constructible_v<cooperative_work>);
static_assert(!std::is_copy_assignable_v<cooperative_work>);
static_assert(!std::is_move_assignable_v<cooperative_work>);
static_assert(std::same_as<
              decltype(std::declval<const cooperative_work&>().policy()),
              codec::limits>);
static_assert(
  std::same_as<
    decltype(std::declval<const cooperative_work&>().bytes_remaining()),
    byte_count>);
static_assert(
  std::same_as<
    decltype(std::declval<const cooperative_work&>().items_remaining()),
    item_count>);
static_assert(noexcept(
  std::declval<cooperative_work&>().drain_inline(byte_count{}, item_count{})));
using cleanup_admission
  = decltype(std::declval<cooperative_work&>().drain_inline(
    byte_count{}, item_count{}));
static_assert(!std::convertible_to<cleanup_admission, bool>);
static_assert(!std::is_copy_constructible_v<cleanup_admission>);
static_assert(!seastar::is_future<cleanup_admission>::value);

codec::limits policy_with(std::uint64_t bytes, std::uint64_t items) {
    codec::limits_config config;
    config.max_work_bytes = byte_count{bytes};
    config.max_work_items = item_count{items};
    return codec::limits::make(config).value();
}

void expect_error(const codec::result<void>& value, errc code) {
    ASSERT_FALSE(value.has_value());
    EXPECT_EQ(value.error(), (codec::error{code, 7, 13, 101}));
}

TEST(CooperativeWorkTest, OwnsItsImmutablePolicyAndResidualCounts) {
    seastar::abort_source abort;
    auto policy = policy_with(13, 3);
    cooperative_work work{policy, abort};
    policy = codec::limits::defaults();
    auto snapshot = work.policy().config();
    snapshot.max_work_bytes = byte_count{1};
    EXPECT_EQ(work.policy().config().max_work_bytes, byte_count{13});
    EXPECT_EQ(work.byte_quantum(), byte_count{13});
    EXPECT_EQ(work.item_quantum(), item_count{3});
    EXPECT_EQ(work.bytes_remaining(), byte_count{13});
    EXPECT_EQ(work.items_remaining(), item_count{3});
    EXPECT_TRUE(work.poll(anchor).has_value());
    EXPECT_NE(snapshot, work.policy().config());
    EXPECT_NE(policy, work.policy());
}

TEST(CooperativeWorkTest, FittingAdmissionIsReadyAndConsumesOnlyItsCost) {
    seastar::abort_source abort;
    cooperative_work work{policy_with(11, 5), abort};
    auto first = work.admit(byte_count{3}, item_count{2}, anchor);
    ASSERT_TRUE(first.available());
    ASSERT_TRUE(first.get().has_value());
    EXPECT_EQ(work.bytes_remaining(), byte_count{8});
    EXPECT_EQ(work.items_remaining(), item_count{3});
    auto second = work.admit(byte_count{8}, item_count{3}, anchor);
    ASSERT_TRUE(second.available());
    ASSERT_TRUE(second.get().has_value());
    EXPECT_EQ(work.bytes_remaining(), byte_count{});
    EXPECT_EQ(work.items_remaining(), item_count{});
}

TEST(CooperativeWorkTest, EmptySequentialChildrenShareTheSameBudget) {
    seastar::abort_source abort;
    cooperative_work work{policy_with(8, 3), abort};
    const auto child = [](cooperative_work& shared) {
        return shared.admit(byte_count{}, item_count{}, anchor);
    };
    for (std::uint64_t remaining = 2;; --remaining) {
        auto admitted = child(work);
        ASSERT_TRUE(admitted.available());
        ASSERT_TRUE(admitted.get().has_value());
        EXPECT_EQ(work.items_remaining(), item_count{remaining});
        EXPECT_EQ(work.bytes_remaining(), byte_count{8});
        if (remaining == 0) {
            break;
        }
    }
    ASSERT_TRUE(child(work).get().has_value());
    EXPECT_EQ(work.items_remaining(), item_count{2});
    EXPECT_EQ(work.bytes_remaining(), byte_count{8});
}

TEST(CooperativeWorkTest, EitherDimensionCheckpointsBeforeTheNextLeaf) {
    seastar::abort_source abort;
    cooperative_work work{policy_with(8, 4), abort};
    ASSERT_TRUE(work.admit(byte_count{6}, item_count{1}).get().has_value());
    ASSERT_TRUE(work.admit(byte_count{3}, item_count{1}).get().has_value());
    EXPECT_EQ(work.bytes_remaining(), byte_count{5});
    EXPECT_EQ(work.items_remaining(), item_count{3});
    ASSERT_TRUE(work.admit(byte_count{1}, item_count{3}).get().has_value());
    EXPECT_EQ(work.bytes_remaining(), byte_count{4});
    EXPECT_EQ(work.items_remaining(), item_count{});
    ASSERT_TRUE(work.admit(byte_count{1}, item_count{1}).get().has_value());
    EXPECT_EQ(work.bytes_remaining(), byte_count{7});
    EXPECT_EQ(work.items_remaining(), item_count{3});
}

TEST(CooperativeWorkTest, OversizedLeavesRejectWithoutCharging) {
    seastar::abort_source abort;
    cooperative_work work{policy_with(8, 4), abort};
    ASSERT_TRUE(work.admit(byte_count{3}, item_count{1}).get().has_value());
    expect_error(
      work.admit(byte_count{9}, item_count{1}, anchor).get(),
      errc::resource_exhausted);
    expect_error(
      work.admit(byte_count{1}, item_count{5}, anchor).get(),
      errc::resource_exhausted);
    EXPECT_EQ(work.bytes_remaining(), byte_count{5});
    EXPECT_EQ(work.items_remaining(), item_count{3});
}

TEST(CooperativeWorkTest, SeparatePassesConsumeSeparateByteWork) {
    seastar::abort_source abort;
    cooperative_work work{policy_with(8, 4), abort};
    ASSERT_TRUE(work.admit(byte_count{4}, item_count{1}).get().has_value());
    ASSERT_TRUE(work.admit(byte_count{4}, item_count{1}).get().has_value());
    EXPECT_EQ(work.bytes_remaining(), byte_count{});
    EXPECT_EQ(work.items_remaining(), item_count{2});
    ASSERT_TRUE(work.admit(byte_count{1}, item_count{1}).get().has_value());
    EXPECT_EQ(work.bytes_remaining(), byte_count{7});
    EXPECT_EQ(work.items_remaining(), item_count{3});
}

TEST(
  CooperativeWorkTest,
  AbortPollingReturnsDiagnosticsInsteadOfRethrowingItsPayload) {
    seastar::abort_source abort;
    cooperative_work work{policy_with(8, 4), abort};
    abort.request_abort_ex(std::make_exception_ptr(137));
    expect_error(work.poll(anchor), errc::aborted);
    expect_error(
      work.admit(byte_count{99}, item_count{99}, anchor).get(), errc::aborted);
    expect_error(work.checkpoint(anchor).get(), errc::aborted);
    EXPECT_EQ(work.bytes_remaining(), byte_count{8});
    EXPECT_EQ(work.items_remaining(), item_count{4});
}

TEST(CooperativeWorkTest, CleanupIgnoresAbortAndRetainsWorkBounds) {
    seastar::abort_source abort;
    cooperative_work work{policy_with(4, 2), abort};
    ASSERT_TRUE(work.admit(byte_count{4}, item_count{2}).get().has_value());
    abort.request_abort();
    work.drain(byte_count{2}, item_count{1}).get();
    EXPECT_EQ(work.bytes_remaining(), byte_count{2});
    EXPECT_EQ(work.items_remaining(), item_count{1});
    work.drain(byte_count{}, item_count{}).get();
    EXPECT_EQ(work.items_remaining(), item_count{});
    work.drain_checkpoint().get();
    EXPECT_EQ(work.bytes_remaining(), byte_count{4});
    EXPECT_EQ(work.items_remaining(), item_count{2});
    expect_error(work.poll(anchor), errc::aborted);
}

TEST(CooperativeWorkTest, ExplicitLeafCheckpointResetsBothResiduals) {
    seastar::abort_source abort;
    cooperative_work work{policy_with(7, 3), abort};
    ASSERT_TRUE(work.admit(byte_count{2}, item_count{2}).get().has_value());
    ASSERT_TRUE(work.checkpoint(anchor).get().has_value());
    ASSERT_TRUE(work.poll(anchor).has_value());
    EXPECT_EQ(work.bytes_remaining(), byte_count{7});
    EXPECT_EQ(work.items_remaining(), item_count{3});
}

seastar::future<item_count> inline_cleanup_after_exhaustion() {
    seastar::abort_source abort;
    cooperative_work work{policy_with(8, 4), abort};
    abort.request_abort();
    co_await work.drain_inline(byte_count{8}, item_count{4});
    co_await work.drain_inline(byte_count{}, item_count{});
    co_return work.items_remaining();
}

TEST(CooperativeWorkTest, InlineCleanupOwnsItsCheckpointAndResumesAdmission) {
    EXPECT_EQ(inline_cleanup_after_exhaustion().get(), item_count{3});
}

TEST(CooperativeWorkTest, ReadyAdmissionDoesNotAllocateACoroutineFrame) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation failure injection is not enabled";
#else
    seastar::abort_source abort;
    cooperative_work work{policy_with(8, 4), abort};
    std::optional<seastar::future<codec::result<void>>> admitted;
    auto& injector = seastar::memory::local_failure_injector();
    const auto before = injector.alloc_count();
    const auto mallocs_before = seastar::memory::stats().mallocs();
    bool threw = false;
    bool cleanup_admitted = false;
    injector.fail_after(0);
    try {
        admitted.emplace(work.admit(byte_count{3}, item_count{1}, anchor));
        auto cleanup = work.drain_inline(byte_count{2}, item_count{1});
        cleanup_admitted = cleanup.await_ready();
        if (cleanup_admitted) {
            cleanup.await_resume();
        }
    } catch (const std::bad_alloc&) {
        threw = true;
    } catch (...) {
        injector.cancel();
        throw;
    }
    const auto after = injector.alloc_count();
    const auto mallocs_after = seastar::memory::stats().mallocs();
    const bool injected = injector.failed();
    injector.cancel();
    ASSERT_FALSE(threw);
    EXPECT_FALSE(injected);
    EXPECT_EQ(after, before);
    // Critical coroutine allocations are invisible to the injector, but are
    // included in the native allocator's actual allocation counter.
    EXPECT_EQ(mallocs_after, mallocs_before);
    EXPECT_TRUE(cleanup_admitted);
    ASSERT_TRUE(admitted.has_value());
    ASSERT_TRUE(admitted->available());
    EXPECT_TRUE(admitted->get().has_value());
    EXPECT_EQ(work.bytes_remaining(), byte_count{3});
#endif
}

struct progress_observation {
    bool during_work;
    std::uint64_t iterations;
};

seastar::future<progress_observation> observe_progress(bool cleanup) {
    seastar::abort_source abort;
    cooperative_work work{policy_with(8, 4), abort};
    if (cleanup) {
        abort.request_abort();
    }
    bool observed = false;
    auto observer = seastar::yield().then([&observed] { observed = true; });
    std::uint64_t iterations = 0;
    std::exception_ptr failure;
    try {
        const auto deadline = std::chrono::steady_clock::now()
                              + std::chrono::seconds(2);
        while (!observed && iterations < 1'000'000
               && std::chrono::steady_clock::now() < deadline) {
            if (cleanup) {
                co_await work.drain_inline(byte_count{}, item_count{});
            } else {
                const auto admitted
                  = co_await seastar::coroutine::without_preemption_check(
                    work.admit(byte_count{}, item_count{}, anchor));
                if (!admitted || !work.poll(anchor)) {
                    throw std::runtime_error(
                      "unexpected abort during work observation");
                }
            }
            ++iterations;
        }
    } catch (...) {
        failure = std::current_exception();
    }
    const bool during_work = observed;
    co_await std::move(observer);
    if (failure) {
        std::rethrow_exception(failure);
    }
    co_return progress_observation{during_work, iterations};
}

TEST(CooperativeWorkTest, EmptyWorkActuallyAllowsAnObserverToRun) {
    const auto observed = observe_progress(false).get();
    EXPECT_TRUE(observed.during_work);
    EXPECT_GT(observed.iterations, 0U);
}

TEST(CooperativeWorkTest, CleanupAfterAbortActuallyAllowsAnObserverToRun) {
    const auto observed = observe_progress(true).get();
    EXPECT_TRUE(observed.during_work);
    EXPECT_GT(observed.iterations, 0U);
}

} // namespace
