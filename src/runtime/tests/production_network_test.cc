#include "src/runtime/network.h"
#include "src/runtime/production/network.h"
#include "src/runtime/production/network_connect_internal.h"
#include "src/runtime/production/network_test_support.h"
#include "src/runtime/testing/contracts/network_contract.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <chrono>
#include <cstddef>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace {

constexpr auto loopback_address = kwaque::runtime::network_address::ipv4(
  {std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}});

kwaque::bytes::fragmented_buffer bytes(std::string_view value) {
    auto copied = kwaque::bytes::fragmented_buffer::copy_of(
      std::span<const char>{value.data(), value.size()});
    if (!copied) {
        throw std::runtime_error("test payload exceeds buffer limits");
    }
    return std::move(*copied);
}

seastar::future<> run_shared_contract(
  seastar::lw_shared_ptr<kwaque::runtime::production::network> backend) {
    co_await kwaque::runtime::testing::run_network_contract(*backend);
}

struct controlled_connect_socket final {
    void shutdown() noexcept { ++shutdowns; }

    unsigned shutdowns{0};
};

} // namespace

SEASTAR_TEST_CASE(production_network_connect_abort_guard_owns_subscription) {
    controlled_connect_socket socket;
    seastar::abort_source caller_abort;
    {
        kwaque::runtime::production::connect_detail::connect_abort_guard guard{
          socket, caller_abort};
        BOOST_REQUIRE(guard.armed());
        caller_abort.request_abort();
        BOOST_CHECK_EQUAL(socket.shutdowns, 1U);
    }
    caller_abort.request_abort();
    BOOST_CHECK_EQUAL(socket.shutdowns, 1U);
    co_return;
}

SEASTAR_TEST_CASE(production_network_shared_contract) {
    auto backend
      = seastar::make_lw_shared<kwaque::runtime::production::network>();
    co_await seastar::with_timeout(
      seastar::lowres_clock::now() + std::chrono::seconds{60},
      run_shared_contract(std::move(backend)));
}

SEASTAR_TEST_CASE(
  production_listener_retains_statistics_after_factory_destruction) {
    std::optional<kwaque::runtime::production::listener> listening;
    {
        kwaque::runtime::production::network backend;
        auto result = co_await backend.listen(
          kwaque::runtime::network_endpoint{loopback_address, 0}, {});
        BOOST_REQUIRE(result.has_value());
        listening.emplace(std::move(*result));
    }

    const auto closed = co_await listening->close();
    BOOST_REQUIRE(closed.has_value());
    BOOST_CHECK(
      listening->statistics()
      == (kwaque::runtime::operation_statistics_snapshot{
        .active = 0,
        .accepted = 2,
        .completed = 2,
      }));
    listening.reset();
}

SEASTAR_TEST_CASE(production_network_moves_idle_native_owners_after_use) {
    kwaque::runtime::production::network backend;
    auto original_listener = co_await backend.listen(
      kwaque::runtime::network_endpoint{loopback_address, 0}, {});
    BOOST_REQUIRE(original_listener.has_value());

    seastar::abort_source accept_abort;
    seastar::abort_source connect_abort;
    auto accepting = original_listener->accept(accept_abort);
    auto original_client = co_await backend.connect(
      original_listener->local_endpoint(),
      std::nullopt,
      kwaque::runtime::network_connection_limits{},
      connect_abort);
    auto original_server = co_await std::move(accepting);
    BOOST_REQUIRE(original_client.has_value());
    BOOST_REQUIRE(original_server.has_value());

    seastar::abort_source write_abort;
    seastar::abort_source read_abort;
    const auto written = co_await original_client->write(
      bytes("m"), write_abort);
    const auto read = co_await original_server->read(
      kwaque::byte_count{1}, read_abort);
    BOOST_REQUIRE(written.has_value());
    BOOST_REQUIRE(read.has_value());
    BOOST_CHECK(read->data().content_equals("m"));

    kwaque::runtime::production::connection client{std::move(*original_client)};
    kwaque::runtime::production::connection server{std::move(*original_server)};
    kwaque::runtime::production::listener listener{
      std::move(*original_listener)};

    const auto client_closed = co_await client.close();
    const auto server_closed = co_await server.close();
    const auto listener_closed = co_await listener.close();
    BOOST_REQUIRE(client_closed.has_value());
    BOOST_REQUIRE(server_closed.has_value());
    BOOST_REQUIRE(listener_closed.has_value());
    BOOST_CHECK(
      backend.statistics()
      == (kwaque::runtime::operation_statistics_snapshot{
        .active = 0,
        .accepted = 8,
        .completed = 8,
        .completed_bytes = 2,
      }));
}

SEASTAR_TEST_CASE(
  production_network_canceled_last_writer_flushes_the_preceding_batch) {
    kwaque::runtime::production::network backend;
    auto listening = co_await backend.listen(
      kwaque::runtime::network_endpoint{loopback_address, 0}, {});
    BOOST_REQUIRE(listening.has_value());

    seastar::abort_source accept_abort;
    seastar::abort_source connect_abort;
    auto accepting = listening->accept(accept_abort);
    auto connected = co_await backend.connect(
      listening->local_endpoint(),
      std::nullopt,
      kwaque::runtime::network_connection_limits{},
      connect_abort);
    auto accepted = co_await std::move(accepting);
    BOOST_REQUIRE(connected.has_value());
    BOOST_REQUIRE(accepted.has_value());

    auto serialization
      = kwaque::runtime::production::network_test_access::hold_write_serializer(
        *connected);
    BOOST_REQUIRE(serialization.has_value());
    kwaque::runtime::production::network_test_access::set_unflushed_bytes(
      *connected, 1);

    seastar::abort_source caller_abort;
    auto queued = connected->write(bytes("canceled"), caller_abort);
    BOOST_CHECK(!queued.available());
    serialization.reset();
    caller_abort.request_abort();

    const auto canceled = co_await std::move(queued);
    BOOST_REQUIRE(!canceled.has_value());
    BOOST_CHECK(canceled.error().code() == kwaque::errc::aborted);
    BOOST_CHECK_EQUAL(
      kwaque::runtime::production::network_test_access::unflushed_bytes(
        *connected),
      0U);

    const auto client_closed = co_await connected->close();
    const auto server_closed = co_await accepted->close();
    const auto listener_closed = co_await listening->close();
    BOOST_REQUIRE(client_closed.has_value());
    BOOST_REQUIRE(server_closed.has_value());
    BOOST_REQUIRE(listener_closed.has_value());
}
