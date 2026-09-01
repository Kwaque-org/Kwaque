#ifndef KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_NETWORK_CONTRACT_H_
#define KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_NETWORK_CONTRACT_H_

#include "src/bytes/fragmented_buffer_builder.h"
#include "src/runtime/network.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace kwaque::runtime::testing {

inline constexpr std::size_t network_contract_clients{3};
inline constexpr std::size_t network_contract_stream_chunks{1'000};
inline constexpr std::size_t network_contract_stream_chunk_bytes{8U * 1024U};

namespace network_contract_detail {

inline constexpr auto loopback_address = network_address::ipv4(
  {std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}});

[[noreturn]] inline void contract_failure(std::string message) {
    std::fprintf(stderr, "network contract failure: %s\n", message.c_str());
    std::fflush(stderr);
    throw std::runtime_error(std::move(message));
}

inline void require(bool condition, std::string_view message) {
    if (!condition) {
        contract_failure(std::string{message});
    }
}

template<typename T>
T require_value(result<T> outcome, std::string_view operation) {
    if (!outcome) {
        contract_failure(
          std::string{operation} + " failed: " + outcome.error().render());
    }
    return std::move(*outcome);
}

inline void require_value(result<void> outcome, std::string_view operation) {
    if (!outcome) {
        contract_failure(
          std::string{operation} + " failed: " + outcome.error().render());
    }
}

inline bytes::fragmented_buffer make_bytes(std::string_view value) {
    auto copied = bytes::fragmented_buffer::copy_of(
      std::span<const char>{value.data(), value.size()});
    if (!copied) {
        contract_failure("network contract payload was rejected");
    }
    return std::move(*copied);
}

inline bytes::fragmented_buffer repeated_bytes(std::size_t size, char value) {
    bytes::fragmented_buffer_builder builder;
    std::array<char, 4'096> chunk{};
    chunk.fill(value);
    while (size != 0) {
        const auto count = std::min(size, chunk.size());
        auto appended = builder.append(
          std::span<const char>{chunk}.first(count));
        if (!appended) {
            contract_failure("network contract payload exceeded bounds");
        }
        size -= count;
    }
    auto result = builder.finish();
    if (!result) {
        contract_failure("network contract payload publication failed");
    }
    return std::move(*result);
}

inline bytes::fragmented_buffer fragmented_payload() {
    constexpr std::array<std::string_view, 3> contents{
      "multi-", "fragment-", "write"};
    std::array<seastar::temporary_buffer<char>, contents.size()> fragments;
    for (std::size_t index = 0; index < fragments.size(); ++index) {
        fragments[index] = seastar::temporary_buffer<char>(
          contents[index].size());
        std::memcpy(
          fragments[index].get_write(),
          contents[index].data(),
          contents[index].size());
    }
    auto result = bytes::fragmented_buffer::copy_from_fragments(
      std::span<const seastar::temporary_buffer<char>>{fragments});
    if (!result) {
        contract_failure("fragmented network payload was rejected");
    }
    return std::move(*result);
}

template<typename Connection>
seastar::future<std::string> read_exactly(
  Connection& connection,
  std::size_t expected,
  seastar::abort_source& abort_source) {
    std::string output;
    output.reserve(expected);
    while (output.size() < expected) {
        auto received = require_value(
          co_await connection.read(
            byte_count{expected - output.size()}, abort_source),
          "network read");
        require(
          !received.eof() && !received.data().empty(),
          "network stream ended before the expected bytes");
        const auto offset = output.size();
        output.resize(offset + received.data().size().value());
        require(
          received.data()
            .copy_to(std::span<char>{output}.subspan(offset))
            .has_value(),
          "network read could not copy its bounded result");
    }
    co_return output;
}

template<typename Connection>
seastar::future<> require_exact_bytes(
  Connection& connection,
  std::string_view expected,
  seastar::abort_source& read_abort) {
    std::size_t offset = 0;
    while (offset < expected.size()) {
        auto received = require_value(
          co_await connection.read(
            byte_count{expected.size() - offset}, read_abort),
          "network read");
        require(
          !received.eof() && !received.data().empty(),
          "network stream ended before the expected bytes");
        const auto size = static_cast<std::size_t>(
          received.data().size().value());
        require(
          received.data().content_equals(expected.substr(offset, size)),
          "network read changed the expected byte stream");
        offset += size;
    }
}

template<typename Connection>
seastar::future<> echo_exact_bytes(
  Connection& connection,
  std::string_view expected,
  seastar::abort_source& read_abort,
  seastar::abort_source& write_abort) {
    std::size_t offset = 0;
    while (offset < expected.size()) {
        auto received = require_value(
          co_await connection.read(
            byte_count{expected.size() - offset}, read_abort),
          "echo read");
        require(
          !received.eof() && !received.data().empty(),
          "echo stream ended before the expected bytes");
        const auto size = static_cast<std::size_t>(
          received.data().size().value());
        require(
          received.data().content_equals(expected.substr(offset, size)),
          "echo input changed the expected byte stream");
        require_value(
          co_await connection.write(
            std::move(received).take_data(), write_abort),
          "echo write");
        offset += size;
    }
}

inline std::string indexed_payload(std::size_t index, std::size_t size) {
    std::string result(size, static_cast<char>('a' + index % 26U));
    const auto stable_index = static_cast<std::uint64_t>(index);
    for (std::size_t byte = 0;
         byte < sizeof(stable_index) && byte < result.size();
         ++byte) {
        const auto encoded = static_cast<unsigned char>(
          (stable_index >> (byte * 8U)) & 0xffU);
        std::memcpy(result.data() + byte, &encoded, sizeof(encoded));
    }
    return result;
}

template<network_backend Backend>
seastar::future<> round_trip(Backend& backend) {
    auto listener = require_value(
      co_await backend.listen(network_endpoint{loopback_address, 0}, {}),
      "network listen");
    seastar::abort_source accept_abort;
    seastar::abort_source connect_abort;
    auto accepting = listener.accept(accept_abort);
    auto client = require_value(
      co_await backend.connect(
        listener.local_endpoint(),
        std::nullopt,
        network_connection_limits{},
        connect_abort),
      "network connect");
    auto server = require_value(
      co_await std::move(accepting), "network accept");
    require(
      client.remote_endpoint() == listener.local_endpoint(),
      "client remote endpoint differs from the listener");
    require(
      client.local_endpoint() == server.remote_endpoint()
        && server.local_endpoint() == client.remote_endpoint(),
      "accepted connection endpoints are not reciprocal");

    seastar::abort_source read_abort;
    seastar::abort_source write_abort;
    auto first_read = server.read(byte_count{64}, read_abort);
    const auto concurrent = co_await server.read(byte_count{64}, read_abort);
    require(
      !concurrent && concurrent.error().code() == errc::unavailable,
      "network backend admitted concurrent reads");
    require_value(
      co_await client.write(make_bytes("hello"), write_abort), "client write");
    auto received = require_value(
      co_await std::move(first_read), "server read");
    require(
      !received.eof() && received.data().content_equals("hello"),
      "round-trip request bytes changed");
    const auto retained = received.data().retained_bytes();
    require(
      retained.value() >= received.data().size().value(),
      "received data underreported retained backing");
    const auto retained_rejection = validate_network_write(
      received.data(),
      network_connection_limits{
        .pending_write_bytes = byte_count{retained.value() - 1U},
        .pending_writes = 1,
      });
    require(
      !retained_rejection
        && retained_rejection.error().code() == errc::out_of_range,
      "write admission ignored retained backing");

    require_value(
      co_await server.write(make_bytes("world"), write_abort), "server write");
    const auto reply = co_await read_exactly(client, 5, read_abort);
    require(reply == "world", "round-trip response bytes changed");

    auto first = client.write(make_bytes("first-"), write_abort);
    auto second = client.write(make_bytes("second"), write_abort);
    require_value(co_await std::move(first), "first ordered write");
    require_value(co_await std::move(second), "second ordered write");
    const auto ordered = co_await read_exactly(server, 12, read_abort);
    require(ordered == "first-second", "serialized writes changed order");

    require_value(
      co_await client.write(fragmented_payload(), write_abort),
      "fragmented write");
    const auto fragmented = co_await read_exactly(server, 20, read_abort);
    require(
      fragmented == "multi-fragment-write",
      "fragmented write changed the byte stream");

    require_value(client.shutdown_output(), "client output shutdown");
    auto eof = require_value(
      co_await server.read(byte_count{64}, read_abort), "server EOF read");
    require(eof.eof() && eof.data().empty(), "peer EOF was not explicit");
    require_value(server.shutdown_output(), "server output shutdown");
    auto reverse_eof = require_value(
      co_await client.read(byte_count{64}, read_abort), "client EOF read");
    require(
      reverse_eof.eof() && reverse_eof.data().empty(),
      "reverse peer EOF was not explicit");
    require_value(client.shutdown_input(), "client input shutdown");
    require_value(server.shutdown_input(), "server input shutdown");

    require_value(co_await client.close(), "client close");
    require_value(co_await client.close(), "repeated client close");
    require_value(co_await server.close(), "server close");
    require_value(co_await listener.close(), "listener close");
    require_value(co_await listener.close(), "repeated listener close");
}

template<network_backend Backend>
seastar::future<> connection_errors(Backend& backend) {
    auto refused_listener = require_value(
      co_await backend.listen(network_endpoint{loopback_address, 0}, {}),
      "refused-listener setup");
    const auto refused_endpoint = refused_listener.local_endpoint();
    require_value(co_await refused_listener.close(), "refused-listener close");
    seastar::abort_source refused_abort;
    auto refused = co_await backend.connect(
      refused_endpoint,
      std::nullopt,
      network_connection_limits{},
      refused_abort);
    if (refused) {
        auto unexpected = std::move(*refused);
        require_value(
          co_await unexpected.close(), "unexpected refused connection close");
        throw std::runtime_error("missing listener accepted a connection");
    }
    require(
      refused.error().code() == errc::network_failure,
      "missing listener did not fail as a network error");

    auto listener = require_value(
      co_await backend.listen(network_endpoint{loopback_address, 0}, {}),
      "duplicate-listener setup");
    auto duplicate = co_await backend.listen(listener.local_endpoint(), {});
    if (duplicate) {
        auto unexpected = std::move(*duplicate);
        require_value(
          co_await unexpected.close(), "unexpected duplicate close");
        require_value(co_await listener.close(), "duplicate-listener cleanup");
        throw std::runtime_error("duplicate listener bind succeeded");
    }
    require(
      duplicate.error().code() == errc::network_failure,
      "duplicate listener bind did not return a network error");

    seastar::abort_source preaborted;
    preaborted.request_abort();
    auto aborted_connect = co_await backend.connect(
      listener.local_endpoint(),
      std::nullopt,
      network_connection_limits{},
      preaborted);
    if (aborted_connect) {
        auto unexpected = std::move(*aborted_connect);
        require_value(
          co_await unexpected.close(), "unexpected aborted connection close");
        require_value(
          co_await listener.close(), "pre-aborted listener cleanup");
        throw std::runtime_error("pre-aborted connect succeeded");
    }
    require(
      aborted_connect.error().code() == errc::aborted,
      "pre-aborted connect was not rejected");

    seastar::abort_source first_abort;
    auto first_accept = listener.accept(first_abort);
    seastar::abort_source second_abort;
    const auto second_accept = co_await listener.accept(second_abort);
    require(
      !second_accept && second_accept.error().code() == errc::unavailable,
      "listener admitted concurrent accepts");
    listener.request_abort();
    const auto aborted_accept = co_await std::move(first_accept);
    require(
      !aborted_accept && aborted_accept.error().code() == errc::aborted,
      "listener abort did not terminate accept");
    require_value(co_await listener.close(), "aborted listener close");
}

template<network_backend Backend>
seastar::future<> multiple_clients(Backend& backend) {
    using connection = typename Backend::connection_type;
    auto listener = require_value(
      co_await backend.listen(
        network_endpoint{loopback_address, 0},
        network_listen_options{.backlog = 8}),
      "multi-client listen");
    using connect_future = decltype(backend.connect(
      listener.local_endpoint(),
      std::nullopt,
      network_connection_limits{},
      std::declval<seastar::abort_source&>()));
    std::array<seastar::abort_source, network_contract_clients> connect_aborts;
    std::array<std::optional<connect_future>, network_contract_clients>
      connects;
    for (std::size_t index = 0; index < connects.size(); ++index) {
        connects[index].emplace(backend.connect(
          listener.local_endpoint(),
          std::nullopt,
          network_connection_limits{},
          connect_aborts[index]));
    }

    std::array<seastar::abort_source, network_contract_clients> accept_aborts;
    std::array<std::optional<connection>, network_contract_clients> clients;
    std::array<std::optional<connection>, network_contract_clients> servers;
    for (std::size_t index = 0; index < clients.size(); ++index) {
        auto accepting = listener.accept(accept_aborts[index]);
        clients[index].emplace(require_value(
          co_await std::move(*connects[index]), "multi-client connect"));
        connects[index].reset();
        servers[index].emplace(
          require_value(co_await std::move(accepting), "multi-client accept"));
    }

    std::array<std::string, network_contract_clients> payloads;
    std::array<seastar::abort_source, network_contract_clients> write_aborts;
    std::array<seastar::abort_source, network_contract_clients> read_aborts;
    for (std::size_t index = 0; index < clients.size(); ++index) {
        payloads[index] = indexed_payload(index, 64);
        require_value(
          co_await clients[index]->write(
            make_bytes(payloads[index]), write_aborts[index]),
          "multi-client write");
    }
    for (std::size_t index = 0; index < servers.size(); ++index) {
        co_await echo_exact_bytes(
          *servers[index],
          payloads[index],
          read_aborts[index],
          write_aborts[index]);
    }
    for (std::size_t index = 0; index < clients.size(); ++index) {
        co_await require_exact_bytes(
          *clients[index], payloads[index], read_aborts[index]);
    }
    for (std::size_t index = 0; index < clients.size(); ++index) {
        require_value(co_await clients[index]->close(), "multi-client close");
        require_value(co_await servers[index]->close(), "multi-server close");
    }
    require_value(co_await listener.close(), "multi-client listener close");
}

template<network_backend Backend>
seastar::future<> long_stream(Backend& backend) {
    auto listener = require_value(
      co_await backend.listen(network_endpoint{loopback_address, 0}, {}),
      "stream listen");
    seastar::abort_source accept_abort;
    seastar::abort_source connect_abort;
    auto accepting = listener.accept(accept_abort);
    auto client = require_value(
      co_await backend.connect(
        listener.local_endpoint(),
        std::nullopt,
        network_connection_limits{},
        connect_abort),
      "stream connect");
    auto server = require_value(co_await std::move(accepting), "stream accept");
    seastar::abort_source write_abort;
    seastar::abort_source read_abort;
    for (std::size_t index = 0; index < network_contract_stream_chunks;
         ++index) {
        const auto expected = indexed_payload(
          index, network_contract_stream_chunk_bytes);
        require_value(
          co_await client.write(make_bytes(expected), write_abort),
          "stream chunk write");
        co_await echo_exact_bytes(server, expected, read_abort, write_abort);
        co_await require_exact_bytes(client, expected, read_abort);
    }
    require_value(co_await client.close(), "stream client close");
    require_value(co_await server.close(), "stream server close");
    require_value(co_await listener.close(), "stream listener close");
}

template<network_backend Backend>
seastar::future<> active_read_abort(Backend& backend) {
    auto listener = require_value(
      co_await backend.listen(network_endpoint{loopback_address, 0}, {}),
      "abort-read listen");
    seastar::abort_source accept_abort;
    seastar::abort_source connect_abort;
    auto accepting = listener.accept(accept_abort);
    auto client = require_value(
      co_await backend.connect(
        listener.local_endpoint(),
        std::nullopt,
        network_connection_limits{},
        connect_abort),
      "abort-read connect");
    auto server = require_value(
      co_await std::move(accepting), "abort-read accept");

    seastar::abort_source read_abort;
    auto reading = server.read(byte_count{64}, read_abort);
    require(!reading.available(), "active read completed before owner abort");
    server.request_abort();
    const auto aborted = co_await std::move(reading);
    require(
      !aborted && aborted.error().code() == errc::aborted,
      "owner abort did not terminate the active read");
    require_value(co_await server.close(), "abort-read server close");
    require_value(co_await client.close(), "abort-read client close");
    require_value(co_await listener.close(), "abort-read listener close");
}

template<network_backend Backend>
seastar::future<> saturation_and_abort(Backend& backend) {
    const network_connection_limits limits{
      .pending_write_bytes = byte_count{2U * 1024U * 1024U},
      .pending_writes = 2,
    };
    auto listener = require_value(
      co_await backend.listen(
        network_endpoint{loopback_address, 0},
        network_listen_options{
          .backlog = 8,
          .receive_buffer_bytes = byte_count{4'096},
          .send_buffer_bytes = byte_count{4'096},
          .reuse_address = true,
          .connection_limits = limits,
        }),
      "saturation listen");
    seastar::abort_source accept_abort;
    seastar::abort_source connect_abort;
    auto accepting = listener.accept(accept_abort);
    auto client = require_value(
      co_await backend.connect(
        listener.local_endpoint(),
        std::nullopt,
        network_connection_limits{},
        connect_abort),
      "saturation connect");
    auto server = require_value(
      co_await std::move(accepting), "saturation accept");

    seastar::abort_source active_abort;
    auto active = server.write(
      repeated_bytes(1024U * 1024U, 'a'), active_abort);
    require(!active.available(), "saturation active write completed too early");
    seastar::abort_source queued_abort;
    auto queued = server.write(make_bytes("q"), queued_abort);
    require(!queued.available(), "saturation queued write completed too early");
    seastar::abort_source rejected_abort;
    auto rejecting = server.write(make_bytes("s"), rejected_abort);
    require(
      rejecting.available(), "write saturation did not reject synchronously");
    const auto rejected = rejecting.get();
    require(
      !rejected && rejected.error().code() == errc::queue_full,
      "write saturation did not return queue_full");

    queued_abort.request_abort();
    const auto canceled = co_await std::move(queued);
    require(
      !canceled && canceled.error().code() == errc::aborted,
      "queued write cancellation was not typed");
    server.request_abort();
    const auto aborted = co_await std::move(active);
    require(
      !aborted && aborted.error().code() == errc::aborted,
      "owner abort did not terminate active write");
    require_value(co_await server.close(), "saturation server close");
    require_value(co_await client.close(), "saturation client close");
    require_value(co_await listener.close(), "saturation listener close");
}

} // namespace network_contract_detail

template<network_backend Backend>
seastar::future<> run_network_contract(Backend& backend) {
    co_await network_contract_detail::round_trip(backend);
    co_await network_contract_detail::connection_errors(backend);
    co_await network_contract_detail::multiple_clients(backend);
    co_await network_contract_detail::long_stream(backend);
    co_await network_contract_detail::active_read_abort(backend);
    co_await network_contract_detail::saturation_and_abort(backend);
}

} // namespace kwaque::runtime::testing

#endif // KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_NETWORK_CONTRACT_H_
