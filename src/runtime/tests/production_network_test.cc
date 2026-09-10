#include "src/runtime/network.h"
#include "src/runtime/production/network.h"
#include "src/runtime/production/network_connect_internal.h"
#include "src/runtime/production/network_test_support.h"
#include "src/runtime/testing/contracts/network_contract.h"
#include "src/runtime/testing/contracts/network_contract_failure_cases.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/net/stack.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <chrono>
#include <cstddef>
#include <exception>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <system_error>
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

struct controlled_connect_socket final {
    void shutdown() noexcept { ++shutdowns; }

    unsigned shutdowns{0};
};

struct close_probe_state final {
    unsigned attempts{0};
    bool closed{false};
    bool destroyed{false};
};

class transient_close_resource final {
public:
    explicit transient_close_resource(close_probe_state& state) noexcept
      : state_(&state) {}
    transient_close_resource(transient_close_resource&& other) noexcept
      : state_(std::exchange(other.state_, nullptr)) {}
    transient_close_resource(const transient_close_resource&) = delete;
    ~transient_close_resource() {
        if (state_ != nullptr) {
            if (!state_->closed) {
                std::terminate();
            }
            state_->destroyed = true;
        }
    }
    void request_abort() noexcept {}
    seastar::future<kwaque::runtime::result<void>> close() {
        if (++state_->attempts == 1U) {
            return seastar::make_exception_future<
              kwaque::runtime::result<void>>(std::bad_alloc{});
        }
        state_->closed = true;
        return seastar::make_ready_future<kwaque::runtime::result<void>>(
          kwaque::runtime::result<void>{});
    }

private:
    close_probe_state* state_;
};

struct transport_failure_probe final {
    std::exception_ptr read_failure;
    std::exception_ptr write_failure;
    std::exception_ptr input_close_failure;
    std::exception_ptr output_close_failure;
    std::optional<seastar::promise<seastar::temporary_buffer<char>>>
      pending_read;
    unsigned input_closes{0};
    unsigned output_closes{0};
    bool socket_destroyed{false};
};

class failing_source final : public seastar::data_source_impl {
public:
    explicit failing_source(transport_failure_probe& probe) noexcept
      : probe_(probe) {}

    seastar::future<seastar::temporary_buffer<char>> get() final {
        if (probe_.pending_read) {
            return probe_.pending_read->get_future();
        }
        if (probe_.read_failure) {
            return seastar::make_exception_future<
              seastar::temporary_buffer<char>>(probe_.read_failure);
        }
        return seastar::make_ready_future<seastar::temporary_buffer<char>>();
    }
    seastar::future<> close() final {
        ++probe_.input_closes;
        return probe_.input_close_failure
                 ? seastar::make_exception_future<>(probe_.input_close_failure)
                 : seastar::make_ready_future<>();
    }

private:
    transport_failure_probe& probe_;
};

class failing_sink final : public seastar::data_sink_impl {
public:
    explicit failing_sink(transport_failure_probe& probe) noexcept
      : probe_(probe) {}

    seastar::future<>
    put(std::span<seastar::temporary_buffer<char>> buffers) final {
        for (auto& buffer : buffers) {
            buffer = {};
        }
        return probe_.write_failure
                 ? seastar::make_exception_future<>(probe_.write_failure)
                 : seastar::make_ready_future<>();
    }
    seastar::future<> close() final {
        ++probe_.output_closes;
        return probe_.output_close_failure
                 ? seastar::make_exception_future<>(probe_.output_close_failure)
                 : seastar::make_ready_future<>();
    }
    std::size_t buffer_size() const noexcept final { return 4096; }

private:
    transport_failure_probe& probe_;
};

class failing_socket final : public seastar::net::connected_socket_impl {
public:
    explicit failing_socket(transport_failure_probe& probe) noexcept
      : probe_(probe) {}
    ~failing_socket() final { probe_.socket_destroyed = true; }
    seastar::data_source source() final {
        return seastar::data_source{std::make_unique<failing_source>(probe_)};
    }
    seastar::data_sink sink() final {
        return seastar::data_sink{std::make_unique<failing_sink>(probe_)};
    }
    void shutdown_input() final {}
    void shutdown_output() final {}
    void set_nodelay(bool) final {}
    bool get_nodelay() const final { return true; }
    void set_keepalive(bool) final {}
    bool get_keepalive() const final { return false; }
    void set_keepalive_parameters(const seastar::net::keepalive_params&) final {
    }
    seastar::net::keepalive_params get_keepalive_parameters() const final {
        return seastar::net::tcp_keepalive_params{
          std::chrono::seconds{0}, std::chrono::seconds{0}, 0};
    }
    void set_sockopt(int, int, const void*, std::size_t) final {
        throw std::system_error(
          std::make_error_code(std::errc::operation_not_supported));
    }
    int get_sockopt(int, int, void*, std::size_t) const final {
        throw std::system_error(
          std::make_error_code(std::errc::operation_not_supported));
    }
    seastar::socket_address local_address() const final {
        return seastar::make_ipv4_address({0x7f000001U, 1});
    }
    seastar::socket_address remote_address() const final {
        return seastar::make_ipv4_address({0x7f000001U, 2});
    }
    seastar::future<> wait_input_shutdown() final {
        return seastar::make_ready_future<>();
    }

private:
    transport_failure_probe& probe_;
};

kwaque::runtime::production::connection
make_failing_connection(transport_failure_probe& probe) {
    return kwaque::runtime::production::network_test_access::make_connection(
      seastar::connected_socket{std::make_unique<failing_socket>(probe)},
      kwaque::runtime::network_endpoint{loopback_address, 1},
      kwaque::runtime::network_endpoint{loopback_address, 2});
}

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

SEASTAR_TEST_CASE(
  production_network_preserves_unknown_read_failure_after_abort) {
    transport_failure_probe probe;
    probe.pending_read.emplace();
    auto connection = make_failing_connection(probe);
    seastar::abort_source caller_abort;
    auto reading = connection.read(kwaque::byte_count{8}, caller_abort);
    BOOST_CHECK(!reading.available());
    const auto failure = std::make_exception_ptr(
      std::logic_error("unexpected native read failure"));
    connection.request_abort();
    probe.pending_read->set_exception(failure);
    std::exception_ptr observed;
    try {
        static_cast<void>(co_await std::move(reading));
    } catch (...) {
        observed = std::current_exception();
    }
    const auto closed = co_await connection.close();
    BOOST_CHECK(observed == failure);
    BOOST_REQUIRE(closed.has_value());
    BOOST_CHECK_EQUAL(connection.statistics().active, 0U);
    BOOST_CHECK(probe.socket_destroyed);
}

SEASTAR_TEST_CASE(production_network_keeps_native_system_errors_operational) {
    const std::array cases{
      std::pair{std::errc::no_buffer_space, kwaque::errc::resource_exhausted},
      std::pair{std::errc::timed_out, kwaque::errc::timed_out},
      std::pair{std::errc::connection_reset, kwaque::errc::network_failure},
    };
    for (const auto& [native, expected] : cases) {
        transport_failure_probe probe;
        probe.read_failure = std::make_exception_ptr(
          std::system_error(std::make_error_code(native)));
        auto connection = make_failing_connection(probe);
        seastar::abort_source caller_abort;
        const auto read = co_await connection.read(
          kwaque::byte_count{8}, caller_abort);
        const auto closed = co_await connection.close();
        BOOST_REQUIRE(!read.has_value());
        BOOST_CHECK(read.error().code() == expected);
        BOOST_REQUIRE(closed.has_value());
    }
}

SEASTAR_TEST_CASE(production_network_preserves_unclassified_write_failures) {
    transport_failure_probe probe;
    probe.write_failure = std::make_exception_ptr(
      std::runtime_error("unexpected native write failure"));
    auto connection = make_failing_connection(probe);
    seastar::abort_source caller_abort;
    std::exception_ptr observed;
    try {
        static_cast<void>(
          co_await connection.write(bytes("payload"), caller_abort));
    } catch (...) {
        observed = std::current_exception();
    }
    const auto closed = co_await connection.close();
    BOOST_CHECK(observed == probe.write_failure);
    BOOST_REQUIRE(closed.has_value());
    BOOST_CHECK_EQUAL(connection.statistics().active, 0U);
}

SEASTAR_TEST_CASE(production_network_preserves_unknown_write_waiter_failure) {
    transport_failure_probe probe;
    auto connection = make_failing_connection(probe);
    auto serialization
      = kwaque::runtime::production::network_test_access::hold_write_serializer(
        connection);
    BOOST_REQUIRE(serialization.has_value());
    seastar::abort_source caller_abort;
    auto writing = connection.write(bytes("payload"), caller_abort);
    BOOST_CHECK(!writing.available());
    const auto failure = std::make_exception_ptr(
      std::logic_error("unexpected write admission failure"));
    caller_abort.request_abort_ex(failure);
    std::exception_ptr observed;
    try {
        static_cast<void>(co_await std::move(writing));
    } catch (...) {
        observed = std::current_exception();
    }
    serialization.reset();
    const auto closed = co_await connection.close();
    BOOST_CHECK(observed == failure);
    BOOST_REQUIRE(closed.has_value());
    BOOST_CHECK_EQUAL(connection.statistics().active, 0U);
}

SEASTAR_TEST_CASE(production_network_close_cleans_both_streams_before_rethrow) {
    const auto operational = std::make_exception_ptr(
      std::system_error(std::make_error_code(std::errc::connection_reset)));
    const auto unexpected = std::make_exception_ptr(
      std::logic_error("unexpected stream close failure"));
    const auto allocation = std::make_exception_ptr(std::bad_alloc{});
    struct close_case final {
        std::exception_ptr output_failure;
        std::exception_ptr input_failure;
        std::exception_ptr expected;
    };
    const std::array cases{
      close_case{unexpected, operational, unexpected},
      close_case{operational, unexpected, unexpected},
      close_case{allocation, operational, allocation},
    };
    for (const auto& [output_failure, input_failure, expected] : cases) {
        transport_failure_probe probe;
        probe.output_close_failure = output_failure;
        probe.input_close_failure = input_failure;
        auto connection = make_failing_connection(probe);
        for (unsigned attempt = 0; attempt < 2; ++attempt) {
            std::exception_ptr observed;
            try {
                static_cast<void>(co_await connection.close());
            } catch (...) {
                observed = std::current_exception();
            }
            BOOST_CHECK(observed == expected);
        }
        BOOST_CHECK_EQUAL(probe.output_closes, 1U);
        BOOST_CHECK_EQUAL(probe.input_closes, 1U);
        BOOST_CHECK(probe.socket_destroyed);
        BOOST_CHECK_EQUAL(connection.statistics().active, 0U);
    }
}

SEASTAR_TEST_CASE(network_contract_retries_failure_before_native_close_state) {
    using namespace kwaque::runtime::testing;
    close_probe_state state;
    auto failure = std::make_exception_ptr(injected_network_body_failure{});
    {
        network_contract_detail::owned_resource<transient_close_resource> owned;
        owned.start(
          seastar::make_ready_future<
            kwaque::runtime::result<transient_close_resource>>(
            kwaque::runtime::result<transient_close_resource>{
              std::in_place, state}));
        const auto acquired = co_await owned.finish();
        if (!acquired) {
            std::terminate();
        }
        owned.request_abort();
        co_await owned.close(failure);
    }
    BOOST_CHECK_EQUAL(state.attempts, 2U);
    BOOST_CHECK(state.closed);
    BOOST_CHECK(state.destroyed);
    BOOST_CHECK(
      contains_network_failure<injected_network_body_failure>(failure));
    BOOST_CHECK(contains_network_failure<std::bad_alloc>(failure));
}

SEASTAR_TEST_CASE(production_network_shared_contract) {
    kwaque::runtime::production::network backend;
    seastar::abort_source cancel;
    kwaque::runtime::testing::network_contract_watchdog watchdog{cancel};
    watchdog.arm(seastar::lowres_clock::now() + std::chrono::seconds{60});
    co_await watchdog.join(
      kwaque::runtime::testing::run_network_contract(backend, cancel));
}

SEASTAR_TEST_CASE(production_network_contract_drains_every_failure_boundary) {
    using namespace kwaque::runtime::testing;
    constexpr std::array cases{
      network_cleanup_case::listener_acquired,
      network_cleanup_case::connect_failure,
      network_cleanup_case::typed_connect_failure,
      network_cleanup_case::accept_pending,
      network_cleanup_case::client_acquired,
      network_cleanup_case::invalid_result,
      network_cleanup_case::read_pending,
      network_cleanup_case::writes_pending,
      network_cleanup_case::body_and_cleanup_failure,
    };
    for (const auto selected : cases) {
        kwaque::runtime::production::network backend;
        seastar::abort_source cancel;
        network_contract_watchdog watchdog{cancel};
        watchdog.arm(seastar::lowres_clock::now() + std::chrono::seconds{60});
        std::exception_ptr failure;
        try {
            co_await watchdog.join(
              run_network_cleanup_case(backend, selected, cancel));
        } catch (...) {
            failure = std::current_exception();
        }
        BOOST_REQUIRE(failure != nullptr);
        BOOST_CHECK(!watchdog.expired());
        if (selected == network_cleanup_case::read_pending) {
            BOOST_CHECK(contains_network_failure<std::bad_alloc>(failure));
        } else if (
          selected == network_cleanup_case::invalid_result
          || selected == network_cleanup_case::typed_connect_failure) {
            const auto expected = selected
                                      == network_cleanup_case::invalid_result
                                    ? "injected invalid network result"
                                    : "injected connect failure";
            try {
                std::rethrow_exception(failure);
            } catch (const std::runtime_error& error) {
                BOOST_CHECK(
                  std::string_view{error.what()}.find(expected)
                  != std::string_view::npos);
            }
        } else {
            BOOST_CHECK(
              contains_network_failure<injected_network_body_failure>(failure));
        }
        if (selected == network_cleanup_case::body_and_cleanup_failure) {
            BOOST_CHECK(
              contains_network_failure<injected_network_cleanup_failure>(
                failure));
        }
        BOOST_CHECK_EQUAL(backend.statistics().active, 0U);
        BOOST_CHECK_EQUAL(
          backend.statistics().accepted, backend.statistics().completed);
    }
}

SEASTAR_TEST_CASE(production_network_watchdog_aborts_and_joins_parked_read) {
    using namespace kwaque::runtime::testing;
    kwaque::runtime::production::network backend;
    seastar::abort_source cancel;
    network_contract_watchdog watchdog{cancel};
    watchdog.arm(seastar::lowres_clock::now() + std::chrono::seconds{60});
    seastar::promise<> parked;
    auto ready = parked.get_future();
    auto operation = run_network_cleanup_case(
      backend, network_cleanup_case::watchdog_read, cancel, &parked);
    while (!ready.available() && !operation.available()) {
        co_await seastar::yield();
    }
    const bool read_parked = ready.available() && !watchdog.expired();
    if (read_parked) {
        ready.get();
        watchdog.arm(seastar::lowres_clock::now());
    }
    std::exception_ptr failure;
    try {
        co_await watchdog.join(std::move(operation));
    } catch (...) {
        failure = std::current_exception();
    }
    BOOST_REQUIRE(read_parked);
    BOOST_CHECK(watchdog.expired());
    BOOST_CHECK(contains_network_failure<seastar::timed_out_error>(failure));
    BOOST_CHECK_EQUAL(backend.statistics().active, 0U);
    BOOST_CHECK_EQUAL(
      backend.statistics().accepted, backend.statistics().completed);
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
