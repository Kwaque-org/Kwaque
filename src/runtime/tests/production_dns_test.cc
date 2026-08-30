#include "src/runtime/dns.h"
#include "src/runtime/production/dns.h"
#include "src/runtime/testing/contracts/dns_test_server.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/api.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace {

using kwaque::runtime::testing::dns_resolver_options;
using kwaque::runtime::testing::make_dns_listener;
using kwaque::runtime::testing::make_dns_query;
using kwaque::runtime::testing::serve_dns_queries;

} // namespace

SEASTAR_TEST_CASE(production_dns_numeric_bypasses_native_resolution) {
    seastar::net::dns_resolver::options options;
    options.servers = std::vector<seastar::net::inet_address>{
      seastar::net::inet_address{"127.0.0.1"}};
    options.use_tcp_query = true;
    options.tcp_port = 1;
    options.timeout = std::chrono::milliseconds{1};
    kwaque::runtime::production::resolver resolver{{}, options};

    seastar::abort_source abort_source;
    const auto resolved = co_await resolver.resolve(
      make_dns_query("127.0.0.42", 12000), abort_source);
    BOOST_REQUIRE(resolved.has_value());
    BOOST_REQUIRE_EQUAL(resolved->answers().size(), 1U);
    BOOST_CHECK_EQUAL(resolved->answers()[0].endpoint.port(), 12000U);
    BOOST_CHECK(
      resolved->answers()[0].endpoint.address().bytes()[3] == std::byte{42});
    BOOST_CHECK(resolved->answers()[0].ttl == kwaque::runtime::maximum_dns_ttl);
    BOOST_CHECK(!resolver.active());
    BOOST_CHECK_EQUAL(resolver.waiters(), 0U);

    seastar::abort_source preaborted;
    preaborted.request_abort();
    const auto aborted = co_await resolver.resolve(
      make_dns_query("127.0.0.43", 12000), preaborted);
    BOOST_REQUIRE(!aborted.has_value());
    BOOST_CHECK(aborted.error().code() == kwaque::errc::aborted);

    const auto stopped = co_await resolver.stop();
    BOOST_REQUIRE(stopped.has_value());
}

SEASTAR_TEST_CASE(production_dns_preserves_answer_order_ttl_and_split_input) {
    auto listener = make_dns_listener();
    auto serving = serve_dns_queries(
      listener,
      {
        {.address = {127, 0, 0, 42}, .ttl = 7},
        {.address = {127, 0, 0, 43}, .ttl = 11},
      },
      true);
    kwaque::runtime::production::resolver resolver{
      kwaque::runtime::dns_config{
        .maximum_waiters = 1,
        .maximum_results = 2,
      },
      dns_resolver_options(listener)};

    seastar::abort_source abort_source;
    const auto resolved = co_await resolver.resolve(
      make_dns_query("ordered.test", 9988), abort_source);
    BOOST_REQUIRE(resolved.has_value());
    BOOST_REQUIRE_EQUAL(resolved->answers().size(), 2U);
    BOOST_CHECK(
      resolved->answers()[0].endpoint.address().bytes()[3] == std::byte{42});
    BOOST_CHECK(
      resolved->answers()[1].endpoint.address().bytes()[3] == std::byte{43});
    BOOST_CHECK(
      resolved->answers()[0].ttl
      == kwaque::runtime::monotonic_duration{7'000'000'000});
    BOOST_CHECK(
      resolved->answers()[1].ttl
      == kwaque::runtime::monotonic_duration{11'000'000'000});

    const auto stopped = co_await resolver.stop();
    BOOST_REQUIRE(stopped.has_value());
    co_await std::move(serving);
    listener.abort_accept();
}

SEASTAR_TEST_CASE(production_dns_stop_drains_active_and_wakes_waiter) {
    auto listener = make_dns_listener();
    seastar::promise<> query_received;
    auto received = query_received.get_future();
    seastar::promise<> release_response;
    auto serving = serve_dns_queries(
      listener,
      {{.address = {127, 0, 0, 50}, .ttl = 19}},
      false,
      &query_received,
      release_response.get_future());
    kwaque::runtime::production::resolver resolver{
      kwaque::runtime::dns_config{
        .maximum_waiters = 1,
        .maximum_results = 2,
      },
      dns_resolver_options(listener)};

    seastar::abort_source active_abort;
    auto active = resolver.resolve(make_dns_query("active.test"), active_abort);
    co_await std::move(received);
    BOOST_CHECK(resolver.active());

    seastar::abort_source waiting_abort;
    auto waiting = resolver.resolve(
      make_dns_query("waiting.test"), waiting_abort);
    BOOST_CHECK(!waiting.available());
    BOOST_CHECK_EQUAL(resolver.waiters(), 1U);

    seastar::abort_source saturated_abort;
    const auto saturated = co_await resolver.resolve(
      make_dns_query("saturated.test"), saturated_abort);
    BOOST_REQUIRE(!saturated.has_value());
    BOOST_CHECK(saturated.error().code() == kwaque::errc::queue_full);

    waiting_abort.request_abort();
    const auto caller_aborted = co_await std::move(waiting);
    BOOST_REQUIRE(!caller_aborted.has_value());
    BOOST_CHECK(caller_aborted.error().code() == kwaque::errc::aborted);
    BOOST_CHECK_EQUAL(resolver.waiters(), 0U);

    seastar::abort_source stop_waiter_abort;
    auto stop_waiter = resolver.resolve(
      make_dns_query("stop-waiter.test"), stop_waiter_abort);
    BOOST_CHECK(!stop_waiter.available());
    BOOST_CHECK_EQUAL(resolver.waiters(), 1U);

    auto stopping = resolver.stop();
    const auto rejected = co_await std::move(stop_waiter);
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == kwaque::errc::aborted);
    BOOST_CHECK(!stopping.available());
    BOOST_CHECK(resolver.active());

    release_response.set_value();
    const auto resolved = co_await std::move(active);
    BOOST_REQUIRE(resolved.has_value());
    const auto stopped = co_await std::move(stopping);
    BOOST_REQUIRE(stopped.has_value());
    BOOST_CHECK(!resolver.active());
    BOOST_CHECK_EQUAL(resolver.waiters(), 0U);

    co_await std::move(serving);
    listener.abort_accept();
}

SEASTAR_TEST_CASE(production_dns_maps_server_error_and_result_bound) {
    {
        auto listener = make_dns_listener();
        auto serving = serve_dns_queries(listener, {}, false, nullptr, {}, 3);
        kwaque::runtime::production::resolver resolver{
          {}, dns_resolver_options(listener)};
        seastar::abort_source abort_source;
        const auto failed = co_await resolver.resolve(
          make_dns_query("missing.test"), abort_source);
        BOOST_REQUIRE(!failed.has_value());
        BOOST_CHECK(failed.error().code() == kwaque::errc::dns_failure);
        const auto stopped = co_await resolver.stop();
        BOOST_REQUIRE(stopped.has_value());
        co_await std::move(serving);
        listener.abort_accept();
    }
    {
        auto listener = make_dns_listener();
        auto serving = serve_dns_queries(
          listener,
          {
            {.address = {127, 0, 0, 1}, .ttl = 1},
            {.address = {127, 0, 0, 2}, .ttl = 2},
          },
          false);
        kwaque::runtime::production::resolver resolver{
          kwaque::runtime::dns_config{
            .maximum_waiters = 0,
            .maximum_results = 1,
          },
          dns_resolver_options(listener)};
        seastar::abort_source abort_source;
        const auto bounded = co_await resolver.resolve(
          make_dns_query("bounded.test"), abort_source);
        BOOST_REQUIRE(!bounded.has_value());
        BOOST_CHECK(bounded.error().code() == kwaque::errc::resource_exhausted);
        const auto stopped = co_await resolver.stop();
        BOOST_REQUIRE(stopped.has_value());
        co_await std::move(serving);
        listener.abort_accept();
    }
}
