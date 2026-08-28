#include "src/bytes/fragmented_buffer.h"
#include "src/runtime/dns.h"
#include "src/runtime/network.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <optional>
#include <span>
#include <utility>

SEASTAR_TEST_CASE(network_write_admission_bounds_operations_and_bytes) {
    kwaque::runtime::network_write_admission admission{
      kwaque::runtime::network_connection_limits{
        .pending_write_bytes = kwaque::byte_count{8},
        .pending_writes = 2,
      }};

    auto first_result = admission.try_acquire(kwaque::byte_count{5});
    BOOST_REQUIRE(first_result.has_value());
    std::optional first{std::move(*first_result)};
    auto second_result = admission.try_acquire(kwaque::byte_count{3});
    BOOST_REQUIRE(second_result.has_value());
    std::optional second{std::move(*second_result)};
    BOOST_CHECK_EQUAL(admission.pending_bytes().value(), 8U);
    BOOST_CHECK_EQUAL(admission.pending_writes(), 2U);

    const auto operation_saturated = admission.try_acquire(
      kwaque::byte_count{1});
    BOOST_CHECK(!operation_saturated.has_value());

    first.reset();
    BOOST_CHECK_EQUAL(admission.pending_bytes().value(), 3U);
    BOOST_CHECK_EQUAL(admission.pending_writes(), 1U);

    const auto byte_saturated = admission.try_acquire(kwaque::byte_count{6});
    BOOST_CHECK(!byte_saturated.has_value());
    BOOST_CHECK_EQUAL(admission.pending_writes(), 1U);

    second.reset();
    auto complete_capacity = admission.try_acquire(kwaque::byte_count{8});
    BOOST_REQUIRE(complete_capacity.has_value());
    BOOST_CHECK_EQUAL(complete_capacity->bytes().value(), 8U);
    std::optional complete{std::move(*complete_capacity)};
    complete.reset();

    BOOST_CHECK_EQUAL(admission.pending_bytes().value(), 0U);
    BOOST_CHECK_EQUAL(admission.pending_writes(), 0U);
    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(network_write_validation_rejects_invalid_requests) {
    const kwaque::runtime::network_connection_limits limits{
      .pending_write_bytes = kwaque::byte_count{8},
      .pending_writes = 1,
    };
    kwaque::bytes::fragmented_buffer empty;
    const auto empty_result = kwaque::runtime::validate_network_write(
      empty, limits);
    BOOST_REQUIRE(!empty_result.has_value());
    BOOST_CHECK(empty_result.error().code() == kwaque::errc::invalid_argument);

    auto over_connection_limit = kwaque::bytes::fragmented_buffer::copy_of(
      std::span<const char>{"123456789", 9});
    const auto rejected = kwaque::runtime::validate_network_write(
      over_connection_limit, limits);
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == kwaque::errc::out_of_range);
    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(network_write_admission_moves_before_first_use) {
    kwaque::runtime::network_write_admission original{
      kwaque::runtime::network_connection_limits{
        .pending_write_bytes = kwaque::byte_count{8},
        .pending_writes = 1,
      }};
    auto moved = std::move(original);
    auto acquired = moved.try_acquire(kwaque::byte_count{8});
    BOOST_REQUIRE(acquired.has_value());
    std::optional reservation{std::move(*acquired)};
    reservation.reset();
    BOOST_CHECK_EQUAL(moved.pending_bytes().value(), 0U);
    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(dns_admission_serializes_and_bounds_waiters) {
    kwaque::runtime::dns_admission admission{kwaque::runtime::dns_config{
      .maximum_waiters = 1,
      .maximum_results = 8,
    }};
    seastar::abort_source first_abort;
    auto first_result = co_await admission.acquire(first_abort);
    BOOST_REQUIRE(first_result.has_value());
    std::optional first{std::move(*first_result)};
    BOOST_CHECK(admission.active());

    seastar::abort_source waiting_abort;
    auto waiting = admission.acquire(waiting_abort);
    BOOST_CHECK(!waiting.available());
    BOOST_CHECK_EQUAL(admission.waiters(), 1U);

    seastar::abort_source saturated_abort;
    const auto saturated = co_await admission.acquire(saturated_abort);
    BOOST_REQUIRE(!saturated.has_value());
    BOOST_CHECK(saturated.error().code() == kwaque::errc::queue_full);

    waiting_abort.request_abort();
    const auto aborted = co_await std::move(waiting);
    BOOST_REQUIRE(!aborted.has_value());
    BOOST_CHECK(aborted.error().code() == kwaque::errc::aborted);
    BOOST_CHECK_EQUAL(admission.waiters(), 0U);

    first.reset();
    BOOST_CHECK(!admission.active());
    co_return;
}

SEASTAR_TEST_CASE(dns_admission_hands_off_after_active_query) {
    kwaque::runtime::dns_admission admission{kwaque::runtime::dns_config{
      .maximum_waiters = 1,
      .maximum_results = 8,
    }};
    seastar::abort_source abort_source;
    auto first_result = co_await admission.acquire(abort_source);
    BOOST_REQUIRE(first_result.has_value());
    std::optional first{std::move(*first_result)};
    auto waiting = admission.acquire(abort_source);
    BOOST_CHECK(!waiting.available());

    first.reset();
    auto second_result = co_await std::move(waiting);
    BOOST_REQUIRE(second_result.has_value());
    std::optional second{std::move(*second_result)};
    BOOST_CHECK(admission.active());
    second.reset();
    BOOST_CHECK(!admission.active());
    co_return;
}

SEASTAR_TEST_CASE(dns_admission_abort_rejects_new_and_wakes_queued_work) {
    kwaque::runtime::dns_admission admission{kwaque::runtime::dns_config{
      .maximum_waiters = 1,
      .maximum_results = 8,
    }};
    seastar::abort_source abort_source;
    auto active_result = co_await admission.acquire(abort_source);
    BOOST_REQUIRE(active_result.has_value());
    std::optional active{std::move(*active_result)};
    auto waiting = admission.acquire(abort_source);
    BOOST_CHECK(!waiting.available());

    admission.request_abort();
    const auto aborted = co_await std::move(waiting);
    BOOST_REQUIRE(!aborted.has_value());
    BOOST_CHECK(aborted.error().code() == kwaque::errc::aborted);
    const auto rejected = co_await admission.acquire(abort_source);
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == kwaque::errc::closed);

    active.reset();
    BOOST_CHECK(!admission.active());
    co_return;
}
