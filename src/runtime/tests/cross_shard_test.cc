#include "src/bytes/fragmented_buffer.h"
#include "src/runtime/cross_shard.h"

#include <seastar/core/checked_ptr.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <vector>

namespace kwaque::runtime::test_types {

class scalar_id final {
public:
    constexpr explicit scalar_id(std::uint64_t value) noexcept
      : value_(value) {}

    [[nodiscard]] constexpr std::uint64_t value() const noexcept {
        return value_;
    }

    bool operator==(const scalar_id&) const = default;

private:
    std::uint64_t value_;
};

} // namespace kwaque::runtime::test_types

template<>
struct kwaque::runtime::enable_cross_shard_value<
  kwaque::runtime::test_types::scalar_id> : std::true_type {};

template<>
struct kwaque::runtime::enable_cross_shard_value<int*> : std::true_type {};

template<>
struct kwaque::runtime::enable_cross_shard_value<int&> : std::true_type {};

template<>
struct kwaque::runtime::enable_cross_shard_value<std::unique_ptr<int>>
  : std::true_type {};

template<>
struct kwaque::runtime::enable_cross_shard_value<std::shared_ptr<int>>
  : std::true_type {};

template<>
struct kwaque::runtime::enable_cross_shard_value<std::reference_wrapper<int>>
  : std::true_type {};

template<>
struct kwaque::runtime::enable_cross_shard_value<std::span<const std::byte>>
  : std::true_type {};

template<>
struct kwaque::runtime::enable_cross_shard_value<std::string_view>
  : std::true_type {};

template<>
struct kwaque::runtime::enable_cross_shard_value<seastar::lw_shared_ptr<int>>
  : std::true_type {};

template<>
struct kwaque::runtime::enable_cross_shard_value<seastar::weak_ptr<int>>
  : std::true_type {};

template<>
struct kwaque::runtime::enable_cross_shard_value<seastar::checked_ptr<int*>>
  : std::true_type {};

template<>
struct kwaque::runtime::enable_cross_shard_value<
  seastar::foreign_ptr<std::unique_ptr<int>>> : std::true_type {};

namespace {

using kwaque::runtime::cross_shard_value;
using kwaque::runtime::test_types::scalar_id;

scalar_id echo_id(scalar_id value) { return value; }
seastar::future<scalar_id> delayed_echo_id(scalar_id value) {
    co_await seastar::yield();
    co_return value;
}
seastar::future<scalar_id> delayed_echo_reference(const scalar_id& value) {
    co_await seastar::yield();
    co_return value;
}
seastar::future<kwaque::runtime::result<scalar_id>>
delayed_result(scalar_id value) {
    co_await seastar::yield();
    co_return value;
}
seastar::future<kwaque::runtime::result<scalar_id>>
delayed_result_failure(scalar_id) {
    co_await seastar::yield();
    kwaque::runtime::operation_error error{
      kwaque::errc::timed_out, kwaque::runtime::operation_kind::resource};
    static_cast<void>(
      error.add_context(kwaque::runtime::operation_context_key::attempt, 7));
    co_return kwaque::runtime::failure(std::move(error));
}
using consume_id_function = void (*)(scalar_id);
using echo_pointer_function = int* (*)(int*);

constexpr std::size_t max_test_shards = 16;
std::array<std::atomic<unsigned>, max_test_shards> void_fanout_calls{};
std::array<std::atomic<unsigned>, max_test_shards> value_fanout_calls{};

seastar::future<> record_void_fanout() {
    const auto shard = seastar::this_shard_id();
    void_fanout_calls[shard].fetch_add(1, std::memory_order_relaxed);
    if (shard == 0) {
        throw std::runtime_error("injected void fan-out failure");
    }
    return seastar::make_ready_future<>();
}

seastar::future<kwaque::runtime::owner_shard> record_value_fanout() {
    const auto shard = seastar::this_shard_id();
    value_fanout_calls[shard].fetch_add(1, std::memory_order_relaxed);
    if (shard == 0) {
        throw std::runtime_error("injected value fan-out failure");
    }
    return seastar::make_ready_future<kwaque::runtime::owner_shard>();
}

static_assert(cross_shard_value<kwaque::runtime::owner_shard>);
static_assert(cross_shard_value<kwaque::runtime::cross_shard_bytes>);
static_assert(
  kwaque::runtime::cross_shard_bytes::max_size
  == kwaque::maximum_contiguous_allocation_bytes);
static_assert(cross_shard_value<kwaque::runtime::operation_error>);
static_assert(cross_shard_value<scalar_id>);
static_assert(cross_shard_value<kwaque::runtime::result<void>>);
static_assert(cross_shard_value<kwaque::runtime::result<scalar_id>>);
static_assert(!cross_shard_value<kwaque::runtime::result<int>>);
static_assert(!cross_shard_value<kwaque::runtime::result<int*>>);
static_assert(!cross_shard_value<
              kwaque::runtime::result<kwaque::bytes::fragmented_buffer>>);
static_assert(
  !std::is_copy_constructible_v<kwaque::runtime::cross_shard_bytes>);
static_assert(!cross_shard_value<int>);
static_assert(!cross_shard_value<int&>);
static_assert(!cross_shard_value<int*>);
static_assert(!cross_shard_value<std::unique_ptr<int>>);
static_assert(!cross_shard_value<std::shared_ptr<int>>);
static_assert(!cross_shard_value<std::weak_ptr<int>>);
static_assert(!cross_shard_value<std::reference_wrapper<int>>);
static_assert(!cross_shard_value<std::span<const std::byte>>);
static_assert(!cross_shard_value<std::string_view>);
static_assert(!cross_shard_value<seastar::lw_shared_ptr<int>>);
static_assert(!cross_shard_value<seastar::weak_ptr<int>>);
static_assert(!cross_shard_value<seastar::checked_ptr<int*>>);
static_assert(!cross_shard_value<seastar::foreign_ptr<std::unique_ptr<int>>>);
// Fragmented buffers own shard-local storage and are deny-by-default here, so a
// caller that needs bytes on another shard must go through the bounded deep
// copy above rather than transferring the buffer itself.
static_assert(!cross_shard_value<kwaque::bytes::fragmented_buffer>);
static_assert(!cross_shard_value<seastar::temporary_buffer<char>>);
static_assert(
  kwaque::runtime::cross_shard_invocation<decltype(&echo_id), scalar_id>);
static_assert(
  kwaque::runtime::cross_shard_invocation<consume_id_function, scalar_id>);
static_assert(
  !kwaque::runtime::cross_shard_invocation<echo_pointer_function, int*>);

} // namespace

SEASTAR_TEST_CASE(cross_shard_bytes_are_bounded_and_deeply_owned) {
    std::array source{std::byte{0x01}, std::byte{0x02}, std::byte{0x03}};
    auto copied = kwaque::runtime::cross_shard_bytes::copy(source);
    BOOST_REQUIRE(copied.has_value());
    source[0] = std::byte{0xff};
    BOOST_CHECK(copied->bytes()[0] == std::byte{0x01});

    const std::vector<std::byte> oversized(
      kwaque::runtime::cross_shard_bytes::max_size + 1);
    const auto rejected = kwaque::runtime::cross_shard_bytes::copy(oversized);
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == kwaque::errc::resource_exhausted);
    co_return;
}

SEASTAR_TEST_CASE(cross_shard_wrappers_return_owned_values_from_every_shard) {
    const auto group = seastar::default_smp_service_group();
    const auto owners = co_await kwaque::runtime::invoke_on_all(
      group, [] { return kwaque::runtime::owner_shard{}; });

    BOOST_REQUIRE_EQUAL(owners.size(), seastar::this_smp_shard_count());
    for (std::size_t index = 0; index < owners.size(); ++index) {
        BOOST_CHECK_EQUAL(owners[index].value(), index);
    }

    const scalar_id expected{42};
    const auto echoed = co_await kwaque::runtime::invoke_on_owner(
      owners.back(), group, &echo_id, expected);
    BOOST_CHECK(echoed == expected);
    const auto delayed = co_await kwaque::runtime::invoke_on_owner(
      owners.back(), group, &delayed_echo_id, expected);
    BOOST_CHECK(delayed == expected);
    const auto referenced = co_await kwaque::runtime::invoke_on_owner(
      owners.back(), group, &delayed_echo_reference, expected);
    BOOST_CHECK(referenced == expected);
    const auto result = co_await kwaque::runtime::invoke_on_owner(
      owners.back(), group, &delayed_result, expected);
    BOOST_REQUIRE(result.has_value());
    BOOST_CHECK(*result == expected);
    const auto failure = co_await kwaque::runtime::invoke_on_owner(
      owners.back(), group, &delayed_result_failure, expected);
    BOOST_REQUIRE(!failure.has_value());
    BOOST_CHECK(failure.error().code() == kwaque::errc::timed_out);
    BOOST_CHECK_EQUAL(failure.error().context_size(), 1U);
}

SEASTAR_TEST_CASE(cross_shard_fanout_waits_for_all_shards_before_failing) {
    const auto shard_count = seastar::this_smp_shard_count();
    BOOST_REQUIRE_LE(shard_count, max_test_shards);
    const auto group = seastar::default_smp_service_group();

    bool failed = false;
    try {
        co_await kwaque::runtime::invoke_on_all(group, &record_void_fanout);
    } catch (const std::runtime_error&) {
        failed = true;
    }
    BOOST_REQUIRE(failed);
    for (unsigned shard = 0; shard < shard_count; ++shard) {
        BOOST_CHECK_EQUAL(
          void_fanout_calls[shard].load(std::memory_order_relaxed), 1U);
    }

    failed = false;
    try {
        co_await kwaque::runtime::invoke_on_all(group, &record_value_fanout);
    } catch (const std::runtime_error&) {
        failed = true;
    }
    BOOST_REQUIRE(failed);
    for (unsigned shard = 0; shard < shard_count; ++shard) {
        BOOST_CHECK_EQUAL(
          value_fanout_calls[shard].load(std::memory_order_relaxed), 1U);
    }
}
