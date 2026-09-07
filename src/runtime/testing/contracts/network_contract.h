#ifndef KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_NETWORK_CONTRACT_H_
#define KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_NETWORK_CONTRACT_H_

#include "src/bytes/fragmented_buffer_builder.h"
#include "src/runtime/network.h"
#include "src/runtime/testing/contracts/cleanup.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/optimized_optional.hh>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <exception>
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

// A watchdog cancels the scenario and joins its original future. Reporting a
// timeout never detaches the resource-owning coroutine or its cleanup.
class network_contract_watchdog final {
public:
    explicit network_contract_watchdog(seastar::abort_source& cancel)
      : cancel_(cancel)
      , timer_([this] noexcept {
          expired_ = true;
          cancel_.request_abort();
      }) {}

    void arm(seastar::lowres_clock::time_point deadline) {
        timer_.rearm(deadline);
    }

    network_contract_watchdog(const network_contract_watchdog&) = delete;
    network_contract_watchdog&
    operator=(const network_contract_watchdog&) = delete;
    network_contract_watchdog(network_contract_watchdog&&) = delete;
    network_contract_watchdog& operator=(network_contract_watchdog&&) = delete;

    seastar::future<> join(seastar::future<> operation) {
        std::exception_ptr failure;
        try {
            co_await std::move(operation);
        } catch (...) {
            failure = std::current_exception();
        }
        timer_.cancel();
        if (expired_) {
            auto timeout = std::make_exception_ptr(seastar::timed_out_error{});
            if (failure) {
                throw seastar::nested_exception{
                  std::move(failure), std::move(timeout)};
            }
            std::rethrow_exception(timeout);
        }
        if (failure) {
            std::rethrow_exception(failure);
        }
    }

    [[nodiscard]] bool expired() const noexcept { return expired_; }

private:
    seastar::abort_source& cancel_;
    seastar::timer<seastar::lowres_clock> timer_;
    bool expired_{false};
};

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

template<typename Resource>
class owned_resource final {
public:
    void start(seastar::future<result<Resource>> acquisition) {
        require(!pending_ && !value_, "resource slot is already occupied");
        pending_.emplace(std::move(acquisition));
    }

    seastar::future<result<void>> finish() {
        require(pending_.has_value(), "resource slot has no acquisition");
        auto acquisition = std::move(*pending_);
        pending_.reset();
        auto acquired = co_await seastar::coroutine::without_preemption_check(
          std::move(acquisition));
        if (!acquired) {
            co_return failure(acquired.error());
        }
        value_.emplace(std::move(*acquired));
        if (aborted_) {
            value_->request_abort();
        }
        co_return result<void>{};
    }

    [[nodiscard]] Resource& get() {
        require(value_.has_value(), "resource slot has no owner");
        return *value_;
    }

    [[nodiscard]] bool pending() const noexcept { return pending_.has_value(); }
    void request_abort() {
        aborted_ = true;
        if (value_) {
            value_->request_abort();
        }
    }

    seastar::future<> close(std::exception_ptr& failure) {
        if (!value_) {
            co_return;
        }
        // A failed allocation may precede the native owner's closing state.
        // Retry once so a transient failure cannot leave that owner open;
        // retain both the initiating error and any later cleanup failure.
        for (unsigned attempt = 0; attempt < 2; ++attempt) {
            try {
                require_cleanup(co_await value_->close());
                co_return;
            } catch (...) {
                retain_cleanup_failure(failure);
            }
        }
    }

private:
    std::optional<seastar::future<result<Resource>>> pending_;
    std::optional<Resource> value_;
    bool aborted_{false};
};

template<typename T>
seastar::future<result<T>>
take_future(std::optional<seastar::future<result<T>>>& pending) {
    require(pending.has_value(), "operation slot is empty");
    auto operation = std::move(*pending);
    pending.reset();
    return operation;
}

template<typename T>
seastar::future<result<T>>
take_pending(std::optional<seastar::future<result<T>>>& pending) {
    co_return co_await take_future(pending);
}

template<typename T>
void require_drained(const result<T>& outcome) {
    if (
      !outcome && outcome.error().code() != errc::aborted
      && outcome.error().code() != errc::closed) {
        contract_failure(
          "network cleanup operation failed: " + outcome.error().render());
    }
}

// Every scenario owns its acquisitions and parked operations before starting
// concurrent work. A failed assertion cannot unwind an open native owner, and
// cancellation reaches handles that arrive after the cancellation request.
template<network_backend Backend>
class scenario_owner final {
public:
    explicit scenario_owner(seastar::abort_source& cancel)
      : subscription_(cancel.subscribe([this] noexcept { request_abort(); })) {
        if (cancel.abort_requested()) {
            request_abort();
        }
    }

    void request_abort() noexcept {
        for (auto& source : accept_aborts) {
            source.request_abort();
        }
        for (auto& source : connect_aborts) {
            source.request_abort();
        }
        for (auto& source : read_aborts) {
            source.request_abort();
        }
        for (auto& source : write_aborts) {
            source.request_abort();
        }
        const auto abort_owner = [this](auto& resource) noexcept {
            try {
                resource.request_abort();
            } catch (...) {
                if (!abort_failure_) {
                    abort_failure_ = std::current_exception();
                }
            }
        };
        for (auto& resource : listeners) {
            abort_owner(resource);
        }
        for (auto& resource : clients) {
            abort_owner(resource);
        }
        for (auto& resource : servers) {
            abort_owner(resource);
        }
    }

    seastar::future<> close(std::exception_ptr& failure) {
        request_abort();
        subscription_ = {};
        if (abort_failure_) {
            try {
                std::rethrow_exception(abort_failure_);
            } catch (...) {
                retain_cleanup_failure(failure);
            }
        }
        for (auto& resource : listeners) {
            co_await drain_acquisition(resource, failure);
        }
        for (auto& resource : clients) {
            co_await drain_acquisition(resource, failure);
        }
        for (auto& resource : servers) {
            co_await drain_acquisition(resource, failure);
        }
        for (auto& operation : reads) {
            co_await drain_operation(operation, failure);
        }
        for (auto& operation : writes) {
            co_await drain_operation(operation, failure);
        }
        for (auto& resource : servers) {
            co_await resource.close(failure);
        }
        for (auto& resource : clients) {
            co_await resource.close(failure);
        }
        for (auto& resource : listeners) {
            co_await resource.close(failure);
        }
    }

    std::array<owned_resource<typename Backend::listener_type>, 3> listeners;
    std::array<
      owned_resource<typename Backend::connection_type>,
      network_contract_clients>
      clients;
    std::array<
      owned_resource<typename Backend::connection_type>,
      network_contract_clients>
      servers;
    std::array<seastar::abort_source, network_contract_clients> accept_aborts;
    std::array<seastar::abort_source, network_contract_clients> connect_aborts;
    std::array<seastar::abort_source, network_contract_clients> read_aborts;
    std::array<seastar::abort_source, network_contract_clients> write_aborts;
    std::array<std::optional<seastar::future<result<network_read_result>>>, 2>
      reads;
    std::array<std::optional<seastar::future<result<void>>>, 3> writes;

private:
    template<typename Resource>
    static seastar::future<> drain_acquisition(
      owned_resource<Resource>& resource, std::exception_ptr& failure) {
        if (resource.pending()) {
            try {
                require_drained(co_await resource.finish());
            } catch (...) {
                retain_cleanup_failure(failure);
            }
        }
    }

    template<typename T>
    static seastar::future<> drain_operation(
      std::optional<seastar::future<result<T>>>& pending,
      std::exception_ptr& failure) {
        if (pending) {
            try {
                require_drained(co_await take_pending(pending));
            } catch (...) {
                retain_cleanup_failure(failure);
            }
        }
    }

    std::exception_ptr abort_failure_;
    seastar::optimized_optional<seastar::abort_source::subscription>
      subscription_;
};

template<network_backend Backend, typename Function>
seastar::future<>
run_scenario(Backend& backend, seastar::abort_source& cancel, Function body) {
    scenario_owner<Backend> owner{cancel};
    std::exception_ptr failure;
    try {
        cancel.check();
        co_await body(backend, owner);
        cancel.check();
    } catch (...) {
        failure = std::current_exception();
    }
    co_await owner.close(failure);
    if (failure) {
        std::rethrow_exception(failure);
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
seastar::future<>
round_trip_body(Backend& backend, scenario_owner<Backend>& owner) {
    owner.listeners[0].start(
      backend.listen(network_endpoint{loopback_address, 0}, {}));
    require_value(co_await owner.listeners[0].finish(), "network listen");
    auto& listener = owner.listeners[0].get();
    auto& accept_abort = owner.accept_aborts[0];
    auto& connect_abort = owner.connect_aborts[0];
    owner.servers[0].start(listener.accept(accept_abort));
    owner.clients[0].start(backend.connect(
      listener.local_endpoint(),
      std::nullopt,
      network_connection_limits{},
      connect_abort));
    require_value(co_await owner.clients[0].finish(), "network connect");
    require_value(co_await owner.servers[0].finish(), "network accept");
    auto& client = owner.clients[0].get();
    auto& server = owner.servers[0].get();
    require(
      client.remote_endpoint() == listener.local_endpoint(),
      "client remote endpoint differs from the listener");
    require(
      client.local_endpoint() == server.remote_endpoint()
        && server.local_endpoint() == client.remote_endpoint(),
      "accepted connection endpoints are not reciprocal");

    auto& read_abort = owner.read_aborts[0];
    auto& write_abort = owner.write_aborts[0];
    owner.reads[0].emplace(server.read(byte_count{64}, read_abort));
    const auto concurrent = co_await server.read(byte_count{64}, read_abort);
    require(
      !concurrent && concurrent.error().code() == errc::unavailable,
      "network backend admitted concurrent reads");
    require_value(
      co_await client.write(make_bytes("hello"), write_abort), "client write");
    auto received = require_value(
      co_await take_pending(owner.reads[0]), "server read");
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

    owner.writes[0].emplace(client.write(make_bytes("first-"), write_abort));
    owner.writes[1].emplace(client.write(make_bytes("second"), write_abort));
    require_value(
      co_await take_pending(owner.writes[0]), "first ordered write");
    require_value(
      co_await take_pending(owner.writes[1]), "second ordered write");
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
seastar::future<>
connection_errors_body(Backend& backend, scenario_owner<Backend>& owner) {
    owner.listeners[0].start(
      backend.listen(network_endpoint{loopback_address, 0}, {}));
    require_value(
      co_await owner.listeners[0].finish(), "refused-listener setup");
    auto& refused_listener = owner.listeners[0].get();
    const auto refused_endpoint = refused_listener.local_endpoint();
    require_value(co_await refused_listener.close(), "refused-listener close");
    auto& refused_abort = owner.connect_aborts[0];
    owner.clients[0].start(backend.connect(
      refused_endpoint,
      std::nullopt,
      network_connection_limits{},
      refused_abort));
    const auto refused = co_await owner.clients[0].finish();
    if (refused) {
        throw std::runtime_error("missing listener accepted a connection");
    }
    require(
      refused.error().code() == errc::network_failure,
      "missing listener did not fail as a network error");

    owner.listeners[1].start(
      backend.listen(network_endpoint{loopback_address, 0}, {}));
    require_value(
      co_await owner.listeners[1].finish(), "duplicate-listener setup");
    auto& listener = owner.listeners[1].get();
    owner.listeners[2].start(backend.listen(listener.local_endpoint(), {}));
    const auto duplicate = co_await owner.listeners[2].finish();
    if (duplicate) {
        throw std::runtime_error("duplicate listener bind succeeded");
    }
    require(
      duplicate.error().code() == errc::network_failure,
      "duplicate listener bind did not return a network error");

    auto& preaborted = owner.connect_aborts[1];
    preaborted.request_abort();
    owner.clients[1].start(backend.connect(
      listener.local_endpoint(),
      std::nullopt,
      network_connection_limits{},
      preaborted));
    const auto aborted_connect = co_await owner.clients[1].finish();
    if (aborted_connect) {
        throw std::runtime_error("pre-aborted connect succeeded");
    }
    require(
      aborted_connect.error().code() == errc::aborted,
      "pre-aborted connect was not rejected");

    auto& first_abort = owner.accept_aborts[0];
    owner.servers[0].start(listener.accept(first_abort));
    auto& second_abort = owner.accept_aborts[1];
    owner.servers[1].start(listener.accept(second_abort));
    const auto second_accept = co_await owner.servers[1].finish();
    require(
      !second_accept && second_accept.error().code() == errc::unavailable,
      "listener admitted concurrent accepts");
    listener.request_abort();
    const auto aborted_accept = co_await owner.servers[0].finish();
    require(
      !aborted_accept && aborted_accept.error().code() == errc::aborted,
      "listener abort did not terminate accept");
    require_value(co_await listener.close(), "aborted listener close");
}

template<network_backend Backend>
seastar::future<>
multiple_clients_body(Backend& backend, scenario_owner<Backend>& owner) {
    owner.listeners[0].start(backend.listen(
      network_endpoint{loopback_address, 0},
      network_listen_options{.backlog = 8}));
    require_value(co_await owner.listeners[0].finish(), "multi-client listen");
    auto& listener = owner.listeners[0].get();
    for (std::size_t index = 0; index < owner.clients.size(); ++index) {
        owner.clients[index].start(backend.connect(
          listener.local_endpoint(),
          std::nullopt,
          network_connection_limits{},
          owner.connect_aborts[index]));
    }

    for (std::size_t index = 0; index < owner.clients.size(); ++index) {
        owner.servers[index].start(listener.accept(owner.accept_aborts[index]));
        require_value(
          co_await owner.clients[index].finish(), "multi-client connect");
        require_value(
          co_await owner.servers[index].finish(), "multi-client accept");
    }

    std::array<std::string, network_contract_clients> payloads;
    for (std::size_t index = 0; index < owner.clients.size(); ++index) {
        payloads[index] = indexed_payload(index, 64);
        require_value(
          co_await owner.clients[index].get().write(
            make_bytes(payloads[index]), owner.write_aborts[index]),
          "multi-client write");
    }
    for (std::size_t index = 0; index < owner.servers.size(); ++index) {
        co_await echo_exact_bytes(
          owner.servers[index].get(),
          payloads[index],
          owner.read_aborts[index],
          owner.write_aborts[index]);
    }
    for (std::size_t index = 0; index < owner.clients.size(); ++index) {
        co_await require_exact_bytes(
          owner.clients[index].get(),
          payloads[index],
          owner.read_aborts[index]);
    }
    for (std::size_t index = 0; index < owner.clients.size(); ++index) {
        require_value(
          co_await owner.clients[index].get().close(), "multi-client close");
        require_value(
          co_await owner.servers[index].get().close(), "multi-server close");
    }
    require_value(co_await listener.close(), "multi-client listener close");
}

template<network_backend Backend>
seastar::future<>
long_stream_body(Backend& backend, scenario_owner<Backend>& owner) {
    owner.listeners[0].start(
      backend.listen(network_endpoint{loopback_address, 0}, {}));
    require_value(co_await owner.listeners[0].finish(), "stream listen");
    auto& listener = owner.listeners[0].get();
    owner.servers[0].start(listener.accept(owner.accept_aborts[0]));
    owner.clients[0].start(backend.connect(
      listener.local_endpoint(),
      std::nullopt,
      network_connection_limits{},
      owner.connect_aborts[0]));
    require_value(co_await owner.clients[0].finish(), "stream connect");
    require_value(co_await owner.servers[0].finish(), "stream accept");
    auto& client = owner.clients[0].get();
    auto& server = owner.servers[0].get();
    auto& write_abort = owner.write_aborts[0];
    auto& read_abort = owner.read_aborts[0];
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
seastar::future<>
active_read_abort_body(Backend& backend, scenario_owner<Backend>& owner) {
    owner.listeners[0].start(
      backend.listen(network_endpoint{loopback_address, 0}, {}));
    require_value(co_await owner.listeners[0].finish(), "abort-read listen");
    auto& listener = owner.listeners[0].get();
    owner.servers[0].start(listener.accept(owner.accept_aborts[0]));
    owner.clients[0].start(backend.connect(
      listener.local_endpoint(),
      std::nullopt,
      network_connection_limits{},
      owner.connect_aborts[0]));
    require_value(co_await owner.clients[0].finish(), "abort-read connect");
    require_value(co_await owner.servers[0].finish(), "abort-read accept");
    auto& client = owner.clients[0].get();
    auto& server = owner.servers[0].get();

    owner.reads[0].emplace(server.read(byte_count{64}, owner.read_aborts[0]));
    require(
      !owner.reads[0]->available(), "active read completed before owner abort");
    server.request_abort();
    const auto aborted = co_await take_pending(owner.reads[0]);
    require(
      !aborted && aborted.error().code() == errc::aborted,
      "owner abort did not terminate the active read");
    require_value(co_await server.close(), "abort-read server close");
    require_value(co_await client.close(), "abort-read client close");
    require_value(co_await listener.close(), "abort-read listener close");
}

template<network_backend Backend>
seastar::future<>
saturation_and_abort_body(Backend& backend, scenario_owner<Backend>& owner) {
    const network_connection_limits limits{
      .pending_write_bytes = byte_count{2U * 1024U * 1024U},
      .pending_writes = 2,
    };
    owner.listeners[0].start(backend.listen(
      network_endpoint{loopback_address, 0},
      network_listen_options{
        .backlog = 8,
        .receive_buffer_bytes = byte_count{4'096},
        .send_buffer_bytes = byte_count{4'096},
        .reuse_address = true,
        .connection_limits = limits,
      }));
    require_value(co_await owner.listeners[0].finish(), "saturation listen");
    auto& listener = owner.listeners[0].get();
    owner.servers[0].start(listener.accept(owner.accept_aborts[0]));
    owner.clients[0].start(backend.connect(
      listener.local_endpoint(),
      std::nullopt,
      network_connection_limits{},
      owner.connect_aborts[0]));
    require_value(co_await owner.clients[0].finish(), "saturation connect");
    require_value(co_await owner.servers[0].finish(), "saturation accept");
    auto& client = owner.clients[0].get();
    auto& server = owner.servers[0].get();

    owner.writes[0].emplace(
      server.write(repeated_bytes(1024U * 1024U, 'a'), owner.write_aborts[0]));
    require(
      !owner.writes[0]->available(),
      "saturation active write completed too early");
    auto& queued_abort = owner.write_aborts[1];
    owner.writes[1].emplace(server.write(make_bytes("q"), queued_abort));
    require(
      !owner.writes[1]->available(),
      "saturation queued write completed too early");
    owner.writes[2].emplace(
      server.write(make_bytes("s"), owner.write_aborts[2]));
    require(
      owner.writes[2]->available(),
      "write saturation did not reject synchronously");
    const auto rejected = take_future(owner.writes[2]).get();
    require(
      !rejected && rejected.error().code() == errc::queue_full,
      "write saturation did not return queue_full");

    queued_abort.request_abort();
    const auto canceled = co_await take_pending(owner.writes[1]);
    require(
      !canceled && canceled.error().code() == errc::aborted,
      "queued write cancellation was not typed");
    require(
      !owner.writes[0]->available(),
      "active write was not backpressured through queued cancellation");
    server.request_abort();
    const auto aborted = co_await take_pending(owner.writes[0]);
    require(
      !aborted && aborted.error().code() == errc::aborted,
      "owner abort did not terminate active write");
    require_value(co_await server.close(), "saturation server close");
    require_value(co_await client.close(), "saturation client close");
    require_value(co_await listener.close(), "saturation listener close");
}

template<network_backend Backend>
seastar::future<>
round_trip(Backend& backend, seastar::abort_source* cancel = nullptr) {
    seastar::abort_source local_cancel;
    co_await run_scenario(
      backend, cancel ? *cancel : local_cancel, round_trip_body<Backend>);
}

template<network_backend Backend>
seastar::future<>
connection_errors(Backend& backend, seastar::abort_source* cancel = nullptr) {
    seastar::abort_source local_cancel;
    co_await run_scenario(
      backend,
      cancel ? *cancel : local_cancel,
      connection_errors_body<Backend>);
}

template<network_backend Backend>
seastar::future<>
multiple_clients(Backend& backend, seastar::abort_source* cancel = nullptr) {
    seastar::abort_source local_cancel;
    co_await run_scenario(
      backend, cancel ? *cancel : local_cancel, multiple_clients_body<Backend>);
}

template<network_backend Backend>
seastar::future<>
long_stream(Backend& backend, seastar::abort_source* cancel = nullptr) {
    seastar::abort_source local_cancel;
    co_await run_scenario(
      backend, cancel ? *cancel : local_cancel, long_stream_body<Backend>);
}

template<network_backend Backend>
seastar::future<>
active_read_abort(Backend& backend, seastar::abort_source* cancel = nullptr) {
    seastar::abort_source local_cancel;
    co_await run_scenario(
      backend,
      cancel ? *cancel : local_cancel,
      active_read_abort_body<Backend>);
}

template<network_backend Backend>
seastar::future<> saturation_and_abort(
  Backend& backend, seastar::abort_source* cancel = nullptr) {
    seastar::abort_source local_cancel;
    co_await run_scenario(
      backend,
      cancel ? *cancel : local_cancel,
      saturation_and_abort_body<Backend>);
}

} // namespace network_contract_detail

template<network_backend Backend>
seastar::future<>
run_network_contract(Backend& backend, seastar::abort_source& cancel) {
    co_await network_contract_detail::round_trip(backend, &cancel);
    co_await network_contract_detail::connection_errors(backend, &cancel);
    co_await network_contract_detail::multiple_clients(backend, &cancel);
    co_await network_contract_detail::long_stream(backend, &cancel);
    co_await network_contract_detail::active_read_abort(backend, &cancel);
    co_await network_contract_detail::saturation_and_abort(backend, &cancel);
}

template<network_backend Backend>
seastar::future<> run_network_contract(Backend& backend) {
    seastar::abort_source cancel;
    co_await run_network_contract(backend, cancel);
}

} // namespace kwaque::runtime::testing

#endif // KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_NETWORK_CONTRACT_H_
