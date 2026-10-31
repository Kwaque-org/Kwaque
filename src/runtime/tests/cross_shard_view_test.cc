#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/runtime/cross_shard.h"
#include "src/runtime/error.h"
#include "src/runtime/shard_affinity.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

using kwaque::bytes::fragmented_buffer;
using kwaque::runtime::cross_shard_borrow_invocation;
using kwaque::runtime::cross_shard_value;
using kwaque::runtime::cross_shard_view_referent;
using kwaque::runtime::lendable;
using kwaque::runtime::owner_shard;
using view = kwaque::runtime::cross_shard_view<const fragmented_buffer>;
using kwaque::literals::operator""_KiB;
using kwaque::literals::operator""_MiB;

struct not_opted_in final {};

struct affine_referent final : kwaque::runtime::shard_affine {};

} // namespace

template<>
struct kwaque::runtime::enable_cross_shard_view<affine_referent>
  : std::true_type {};

namespace {

static_assert(cross_shard_view_referent<fragmented_buffer>);
static_assert(!cross_shard_view_referent<not_opted_in>);
// Opting in a shard-affine type is still refused.
static_assert(!cross_shard_view_referent<affine_referent>);
static_assert(!std::is_copy_constructible_v<view>);
static_assert(!std::is_copy_assignable_v<view>);
static_assert(!std::is_move_assignable_v<view>);
static_assert(!std::is_default_constructible_v<view>);
static_assert(!std::is_constructible_v<view, const fragmented_buffer&>);
static_assert(std::is_trivially_destructible_v<view>);
static_assert(!cross_shard_value<view>);
static_assert(!cross_shard_value<const view&>);
static_assert(!std::is_copy_constructible_v<lendable<fragmented_buffer>>);
static_assert(!std::is_move_constructible_v<lendable<fragmented_buffer>>);
// The lender reads its own referent and never changes it while it is lent.
static_assert(
  std::is_same_v<
    decltype(std::declval<const lendable<fragmented_buffer>&>().value()),
    const fragmented_buffer&>);

// A target reads through the const reference it was given, never through an
// rvalue it could keep from, and no result can carry a view out.
template<typename View>
concept readable_as_rvalue = requires(View&& lent) { std::move(lent).get(); };
template<typename View>
concept readable_as_lvalue = requires(const View& lent) { lent.get(); };
static_assert(readable_as_lvalue<view>);
static_assert(!readable_as_rvalue<view>);
static_assert(!cross_shard_value<std::optional<view>>);
static_assert(!cross_shard_value<kwaque::runtime::result<view>>);

using outcome = kwaque::runtime::result<void>;

// The byte at position i of every test referent.
char pattern(std::uint64_t at) noexcept {
    return static_cast<char>((at * 131U + 7U) & 0xffU);
}

fragmented_buffer make_buffer(std::uint64_t size) {
    kwaque::bytes::fragmented_buffer_builder builder;
    std::vector<char> chunk(128_KiB);
    for (std::uint64_t at = 0; at < size;) {
        const auto count = std::min<std::uint64_t>(chunk.size(), size - at);
        for (std::uint64_t index = 0; index < count; ++index)
            chunk[index] = pattern(at + index);
        if (!builder.append(std::span<const char>{chunk.data(), count}))
            throw std::runtime_error("cannot build a test referent");
        at += count;
    }
    auto finished = builder.finish();
    if (!finished) throw std::runtime_error("cannot finish a test referent");
    return std::move(*finished);
}

bool holds_pattern(const fragmented_buffer& buffer) {
    std::uint64_t at = 0;
    for (const auto& fragment : buffer) {
        for (const auto octet : fragment.bytes()) {
            if (octet != pattern(at++)) return false;
        }
    }
    return at == buffer.size().value();
}

// Reads the referent and copies it into the target's memory, as an owner
// does, then drops the copy there.
outcome copy_on_target(const view& lent) {
    if (!holds_pattern(lent.get())) {
        return kwaque::runtime::failure(
          kwaque::runtime::operation_error{
            kwaque::errc::corrupt_data,
            kwaque::runtime::operation_kind::runtime});
    }
    const auto copied = lent.get().copy();
    if (copied.size() != lent.get().size()) {
        return kwaque::runtime::failure(
          kwaque::runtime::operation_error{
            kwaque::errc::corrupt_data,
            kwaque::runtime::operation_kind::runtime});
    }
    return {};
}

outcome refuse_on_target(const view&) {
    return kwaque::runtime::failure(
      kwaque::runtime::operation_error{
        kwaque::errc::wrong_context, kwaque::runtime::operation_kind::runtime});
}

seastar::future<> throw_on_target(const view&) {
    throw std::runtime_error("planted target failure");
}

std::atomic<unsigned> reads{0};

seastar::future<outcome> count_read(const view& lent) {
    co_await seastar::sleep(std::chrono::milliseconds(1));
    if (!holds_pattern(lent.get())) {
        co_return kwaque::runtime::failure(
          kwaque::runtime::operation_error{
            kwaque::errc::corrupt_data,
            kwaque::runtime::operation_kind::runtime});
    }
    reads.fetch_add(1);
    co_return outcome{};
}

std::atomic<unsigned> delayed_done{0};

// The last shard fails at once; every other shard is still reading.
seastar::future<> fail_once_others_delayed(const view& lent) {
    if (seastar::this_shard_id() + 1 == seastar::this_smp_shard_count()) {
        throw std::runtime_error("planted fast failure");
    }
    co_await seastar::sleep(std::chrono::milliseconds(20));
    if (holds_pattern(lent.get())) delayed_done.fetch_add(1);
}

// Target signatures the borrowing helpers must refuse.
using takes_view_by_value = void (*)(view);
using takes_view_by_rvalue = void (*)(view&&);
struct unopted_result final {};
using returns_unopted = unopted_result (*)(const view&);

static_assert(
  cross_shard_borrow_invocation<decltype(&copy_on_target), fragmented_buffer>);
static_assert(
  !cross_shard_borrow_invocation<takes_view_by_value, fragmented_buffer>);
static_assert(
  !cross_shard_borrow_invocation<takes_view_by_rvalue, fragmented_buffer>);
static_assert(
  !cross_shard_borrow_invocation<returns_unopted, fragmented_buffer>);
static_assert(
  !cross_shard_borrow_invocation<decltype(&copy_on_target), not_opted_in>);
inline constexpr auto capturing = [value = 1](const view&) {
    static_cast<void>(value);
    return outcome{};
};
static_assert(
  !cross_shard_borrow_invocation<decltype(capturing), fragmented_buffer>);

seastar::future<owner_shard> remote_owner() {
    const auto target = (seastar::this_shard_id() + 1)
                        % seastar::this_smp_shard_count();
    co_return co_await seastar::smp::submit_to(
      target, [] { return owner_shard{}; });
}

#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
// Only the native allocator counts frees per shard.
seastar::future<std::vector<std::uint64_t>> cross_cpu_frees_by_shard() {
    std::vector<std::uint64_t> frees;
    for (seastar::shard_id shard = 0; shard < seastar::this_smp_shard_count();
         ++shard) {
        frees.push_back(co_await seastar::smp::submit_to(shard, [] {
            return seastar::memory::stats().cross_cpu_frees();
        }));
    }
    co_return frees;
}
#endif

} // namespace

SEASTAR_TEST_CASE(borrow_on_owner_reads_on_another_shard_under_an_open_loan) {
    BOOST_REQUIRE_GE(seastar::this_smp_shard_count(), 2U);
    const auto target = co_await remote_owner();
    BOOST_REQUIRE(!target.is_current());
    lendable<fragmented_buffer> lent{make_buffer(1_MiB)};
    auto call = kwaque::runtime::borrow_on_owner(
      target, seastar::default_smp_service_group(), &copy_on_target, lent);
    BOOST_CHECK_EQUAL(lent.loans(), 1U);
    const auto copied = co_await std::move(call);
    BOOST_CHECK(copied.has_value());
    BOOST_CHECK_EQUAL(lent.loans(), 0U);

    const auto refused = co_await kwaque::runtime::borrow_on_owner(
      target, seastar::default_smp_service_group(), &refuse_on_target, lent);
    BOOST_REQUIRE(!refused.has_value());
    BOOST_CHECK(refused.error().code() == kwaque::errc::wrong_context);
    BOOST_CHECK_EQUAL(lent.loans(), 0U);

    BOOST_CHECK_THROW(
      co_await kwaque::runtime::borrow_on_owner(
        target, seastar::default_smp_service_group(), &throw_on_target, lent),
      std::runtime_error);
    BOOST_CHECK_EQUAL(lent.loans(), 0U);

    // The lender's own shard runs the call inline.
    const auto local = co_await kwaque::runtime::borrow_on_owner(
      owner_shard{},
      seastar::default_smp_service_group(),
      &copy_on_target,
      lent);
    BOOST_CHECK(local.has_value());
    const auto released = std::move(lent).release();
    BOOST_CHECK(released.content_equals(make_buffer(1_MiB)));
}

SEASTAR_TEST_CASE(borrow_on_all_reads_on_every_shard) {
    lendable<fragmented_buffer> lent{make_buffer(300_KiB)};
    reads.store(0);
    const auto results = co_await kwaque::runtime::borrow_on_all(
      seastar::default_smp_service_group(), &count_read, lent);
    BOOST_CHECK_EQUAL(results.size(), seastar::this_smp_shard_count());
    for (const auto& result : results)
        BOOST_CHECK(result.has_value());
    BOOST_CHECK_EQUAL(reads.load(), seastar::this_smp_shard_count());
    BOOST_CHECK_EQUAL(lent.loans(), 0U);
}

SEASTAR_TEST_CASE(borrow_on_all_resolves_only_after_every_call_returns) {
    BOOST_REQUIRE_GE(seastar::this_smp_shard_count(), 2U);
    lendable<fragmented_buffer> lent{make_buffer(64_KiB)};
    delayed_done.store(0);
    BOOST_CHECK_THROW(
      co_await kwaque::runtime::borrow_on_all(
        seastar::default_smp_service_group(), &fail_once_others_delayed, lent),
      std::runtime_error);
    // Every delayed shard finished reading before the failure was reported,
    // so the loan never closed under a reader.
    BOOST_CHECK_EQUAL(delayed_done.load(), seastar::this_smp_shard_count() - 1);
    BOOST_CHECK_EQUAL(lent.loans(), 0U);
}

SEASTAR_TEST_CASE(
  borrowing_copies_on_the_owner_and_frees_nothing_across_shards) {
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    // The system allocator keeps no per-shard counts.
    co_return;
#else
    const auto target = co_await remote_owner();
    std::vector<std::uint64_t> lender_growth;
    for (const std::uint64_t size :
         {std::uint64_t{1}, 128_KiB, 1_MiB, 16_MiB}) {
        lendable<fragmented_buffer> lent{make_buffer(size)};
        // Warm the paths once so first-use allocations are not counted.
        static_cast<void>(co_await kwaque::runtime::borrow_on_owner(
          target,
          seastar::default_smp_service_group(),
          &refuse_on_target,
          lent));
        const auto frees_before = co_await cross_cpu_frees_by_shard();
        const auto before = seastar::memory::stats().total_bytes_allocated();
        const auto copied = co_await kwaque::runtime::borrow_on_owner(
          target, seastar::default_smp_service_group(), &copy_on_target, lent);
        const auto refused = co_await kwaque::runtime::borrow_on_owner(
          target,
          seastar::default_smp_service_group(),
          &refuse_on_target,
          lent);
        lender_growth.push_back(
          seastar::memory::stats().total_bytes_allocated() - before);
        const auto frees_after = co_await cross_cpu_frees_by_shard();
        BOOST_CHECK(copied.has_value());
        BOOST_CHECK(!refused.has_value());
        BOOST_CHECK(frees_before == frees_after);
    }
    // The payload is never copied on the lender: its allocations do not grow
    // with the referent's size.
    for (const auto growth : lender_growth)
        BOOST_CHECK_EQUAL(growth, lender_growth.front());
#endif
}
