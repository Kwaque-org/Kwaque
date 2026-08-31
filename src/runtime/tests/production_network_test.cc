#include "src/bytes/fragmented_buffer_builder.h"
#include "src/runtime/network.h"
#include "src/runtime/production/network.h"
#include "src/runtime/production/network_connect_internal.h"
#include "src/runtime/production/network_test_support.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstring>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
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

kwaque::bytes::fragmented_buffer repeated_bytes(std::size_t size, char value) {
    kwaque::bytes::fragmented_buffer_builder builder;
    std::array<char, 4096> chunk{};
    chunk.fill(value);
    while (size != 0) {
        const auto count = std::min(size, chunk.size());
        const auto appended = builder.append(
          std::span<const char>{chunk}.first(count));
        if (!appended) {
            throw std::runtime_error("test payload exceeds buffer limits");
        }
        size -= count;
    }
    auto result = builder.finish();
    if (!result) {
        throw std::runtime_error("test payload publication failed");
    }
    return std::move(*result);
}

seastar::future<std::string> read_exactly(
  kwaque::runtime::production::connection& connection,
  std::size_t bytes,
  seastar::abort_source& abort_source) {
    std::string output;
    output.reserve(bytes);
    while (output.size() < bytes) {
        auto read = co_await connection.read(
          kwaque::byte_count{bytes - output.size()}, abort_source);
        if (!read || read->eof() || read->data().empty()) {
            throw std::runtime_error("connection ended before expected bytes");
        }
        const auto offset = output.size();
        output.resize(offset + read->data().size().value());
        const auto copied = read->data().copy_to(
          std::span<char>{output}.subspan(offset));
        if (!copied) {
            throw std::runtime_error("failed to copy received test bytes");
        }
    }
    co_return output;
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

SEASTAR_TEST_CASE(production_network_loopback_preserves_ownership_and_eof) {
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
    BOOST_CHECK_EQUAL(
      connected->remote_endpoint().port(), listening->local_endpoint().port());

    seastar::abort_source read_abort;
    auto first_read = accepted->read(kwaque::byte_count{64}, read_abort);
    const auto concurrent = co_await accepted->read(
      kwaque::byte_count{64}, read_abort);
    BOOST_REQUIRE(!concurrent.has_value());
    BOOST_CHECK(concurrent.error().code() == kwaque::errc::unavailable);

    seastar::abort_source write_abort;
    const auto wrote = co_await connected->write(bytes("hello"), write_abort);
    const auto received = co_await std::move(first_read);
    BOOST_REQUIRE(wrote.has_value());
    BOOST_REQUIRE(received.has_value());
    BOOST_CHECK(!received->eof());
    BOOST_CHECK(received->data().content_equals("hello"));
    BOOST_CHECK_EQUAL(
      received->data().retained_bytes().value(),
      kwaque::maximum_contiguous_allocation_bytes);
    const auto retained_rejection = kwaque::runtime::validate_network_write(
      received->data(),
      kwaque::runtime::network_connection_limits{
        .pending_write_bytes
        = kwaque::byte_count{kwaque::maximum_contiguous_allocation_bytes - 1U},
        .pending_writes = 1,
      });
    BOOST_REQUIRE(!retained_rejection.has_value());
    BOOST_CHECK(
      retained_rejection.error().code() == kwaque::errc::out_of_range);

    const auto replied = co_await accepted->write(bytes("world"), write_abort);
    const auto reply = co_await connected->read(
      kwaque::byte_count{64}, read_abort);
    BOOST_REQUIRE(replied.has_value());
    BOOST_REQUIRE(reply.has_value());
    BOOST_CHECK(reply->data().content_equals("world"));

    auto first_ordered = connected->write(bytes("first-"), write_abort);
    auto second_ordered = connected->write(bytes("second"), write_abort);
    const auto first_ordered_result = co_await std::move(first_ordered);
    const auto second_ordered_result = co_await std::move(second_ordered);
    const auto ordered = co_await read_exactly(*accepted, 12, read_abort);
    BOOST_REQUIRE(first_ordered_result.has_value());
    BOOST_REQUIRE(second_ordered_result.has_value());
    BOOST_CHECK_EQUAL(ordered, "first-second");

    std::array<seastar::temporary_buffer<char>, 3> fragments;
    const std::array<std::string_view, 3> fragment_bytes{
      "multi-", "fragment-", "write"};
    for (std::size_t index = 0; index < fragments.size(); ++index) {
        fragments[index] = seastar::temporary_buffer<char>(
          fragment_bytes[index].size());
        std::memcpy(
          fragments[index].get_write(),
          fragment_bytes[index].data(),
          fragment_bytes[index].size());
    }
    auto fragmented = kwaque::bytes::fragmented_buffer::copy_from_fragments(
      std::span<const seastar::temporary_buffer<char>>{fragments});
    BOOST_REQUIRE(fragmented.has_value());
    const auto fragmented_write = co_await connected->write(
      std::move(*fragmented), write_abort);
    const auto fragmented_read = co_await accepted->read(
      kwaque::byte_count{64}, read_abort);
    BOOST_REQUIRE(fragmented_write.has_value());
    BOOST_REQUIRE(fragmented_read.has_value());
    BOOST_CHECK(fragmented_read->data().content_equals("multi-fragment-write"));

    const auto output_shutdown = connected->shutdown_output();
    BOOST_REQUIRE(output_shutdown.has_value());
    const auto eof = co_await accepted->read(
      kwaque::byte_count{64}, read_abort);
    BOOST_REQUIRE(eof.has_value());
    BOOST_CHECK(eof->eof());
    BOOST_CHECK(eof->data().empty());

    const auto reverse_output_shutdown = accepted->shutdown_output();
    BOOST_REQUIRE(reverse_output_shutdown.has_value());
    const auto reverse_eof = co_await connected->read(
      kwaque::byte_count{64}, read_abort);
    BOOST_REQUIRE(reverse_eof.has_value());
    BOOST_CHECK(reverse_eof->eof());
    const auto input_shutdown = connected->shutdown_input();
    const auto reverse_input_shutdown = accepted->shutdown_input();
    BOOST_REQUIRE(input_shutdown.has_value());
    BOOST_REQUIRE(reverse_input_shutdown.has_value());

    const auto client_closed = co_await connected->close();
    const auto client_closed_again = co_await connected->close();
    const auto server_closed = co_await accepted->close();
    const auto listener_closed = co_await listening->close();
    const auto listener_closed_again = co_await listening->close();
    BOOST_REQUIRE(client_closed.has_value());
    BOOST_REQUIRE(client_closed_again.has_value());
    BOOST_REQUIRE(server_closed.has_value());
    BOOST_REQUIRE(listener_closed.has_value());
    BOOST_REQUIRE(listener_closed_again.has_value());
}

SEASTAR_TEST_CASE(production_network_abort_boundaries_are_typed) {
    kwaque::runtime::production::network backend;
    auto refused_listener = co_await backend.listen(
      kwaque::runtime::network_endpoint{loopback_address, 0}, {});
    BOOST_REQUIRE(refused_listener.has_value());
    const auto refused_endpoint = refused_listener->local_endpoint();
    const auto refused_listener_closed = co_await refused_listener->close();
    BOOST_REQUIRE(refused_listener_closed.has_value());

    seastar::abort_source refused_abort;
    const auto refused = co_await backend.connect(
      refused_endpoint,
      std::nullopt,
      kwaque::runtime::network_connection_limits{},
      refused_abort);
    BOOST_REQUIRE(!refused.has_value());
    BOOST_CHECK(refused.error().code() == kwaque::errc::network_failure);

    auto listening = co_await backend.listen(
      kwaque::runtime::network_endpoint{loopback_address, 0}, {});
    BOOST_REQUIRE(listening.has_value());

    seastar::abort_source preaborted;
    preaborted.request_abort();
    const auto rejected_connect = co_await backend.connect(
      listening->local_endpoint(),
      std::nullopt,
      kwaque::runtime::network_connection_limits{},
      preaborted);
    BOOST_REQUIRE(!rejected_connect.has_value());
    BOOST_CHECK(rejected_connect.error().code() == kwaque::errc::aborted);

    seastar::abort_source accept_abort;
    auto accepting = listening->accept(accept_abort);
    listening->request_abort();
    const auto aborted_accept = co_await std::move(accepting);
    BOOST_REQUIRE(!aborted_accept.has_value());
    BOOST_CHECK(aborted_accept.error().code() == kwaque::errc::aborted);

    const auto listener_closed = co_await listening->close();
    BOOST_REQUIRE(listener_closed.has_value());
}

SEASTAR_TEST_CASE(production_network_rejects_a_concurrent_accept) {
    kwaque::runtime::production::network backend;
    auto listening = co_await backend.listen(
      kwaque::runtime::network_endpoint{loopback_address, 0}, {});
    BOOST_REQUIRE(listening.has_value());

    seastar::abort_source first_abort;
    auto first = listening->accept(first_abort);
    BOOST_CHECK(!first.available());

    seastar::abort_source second_abort;
    const auto second = co_await listening->accept(second_abort);
    BOOST_REQUIRE(!second.has_value());
    BOOST_CHECK(second.error().code() == kwaque::errc::unavailable);
    BOOST_CHECK(!first.available());

    listening->request_abort();
    const auto first_result = co_await std::move(first);
    BOOST_REQUIRE(!first_result.has_value());
    BOOST_CHECK(first_result.error().code() == kwaque::errc::aborted);
    const auto closed = co_await listening->close();
    BOOST_REQUIRE(closed.has_value());
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

SEASTAR_TEST_CASE(production_network_owner_abort_ends_active_io) {
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

    seastar::abort_source read_abort;
    auto reading = accepted->read(kwaque::byte_count{64}, read_abort);
    BOOST_CHECK(!reading.available());
    accepted->request_abort();
    const auto aborted = co_await std::move(reading);
    BOOST_REQUIRE(!aborted.has_value());
    BOOST_CHECK(aborted.error().code() == kwaque::errc::aborted);

    const auto server_closed = co_await accepted->close();
    const auto client_closed = co_await connected->close();
    const auto listener_closed = co_await listening->close();
    BOOST_REQUIRE(server_closed.has_value());
    BOOST_REQUIRE(client_closed.has_value());
    BOOST_REQUIRE(listener_closed.has_value());
}

SEASTAR_TEST_CASE(production_network_bounds_and_aborts_a_queued_writer) {
    kwaque::runtime::production::network backend;
    const kwaque::runtime::network_connection_limits limits{
      .pending_write_bytes = kwaque::byte_count{2U * 1024U * 1024U},
      .pending_writes = 2,
    };
    auto listening = co_await backend.listen(
      kwaque::runtime::network_endpoint{loopback_address, 0},
      {.backlog = 8,
       .receive_buffer_bytes = kwaque::byte_count{4096},
       .send_buffer_bytes = kwaque::byte_count{4096},
       .reuse_address = true,
       .connection_limits = limits});
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

    seastar::abort_source active_abort;
    auto active = accepted->write(
      repeated_bytes(1024U * 1024U, 'a'), active_abort);
    BOOST_CHECK(!active.available());

    seastar::abort_source queued_abort;
    auto queued = accepted->write(bytes("q"), queued_abort);
    BOOST_CHECK(!queued.available());

    seastar::abort_source saturated_abort;
    const auto saturated = co_await accepted->write(
      bytes("s"), saturated_abort);
    BOOST_REQUIRE(!saturated.has_value());
    BOOST_CHECK(saturated.error().code() == kwaque::errc::queue_full);

    queued_abort.request_abort();
    const auto queued_result = co_await std::move(queued);
    BOOST_REQUIRE(!queued_result.has_value());
    BOOST_CHECK(queued_result.error().code() == kwaque::errc::aborted);

    accepted->request_abort();
    const auto active_result = co_await std::move(active);
    BOOST_REQUIRE(!active_result.has_value());
    BOOST_CHECK(active_result.error().code() == kwaque::errc::aborted);

    const auto server_closed = co_await accepted->close();
    const auto client_closed = co_await connected->close();
    const auto listener_closed = co_await listening->close();
    BOOST_REQUIRE(server_closed.has_value());
    BOOST_REQUIRE(client_closed.has_value());
    BOOST_REQUIRE(listener_closed.has_value());
}
