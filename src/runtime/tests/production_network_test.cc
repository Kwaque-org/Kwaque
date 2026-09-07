#include "src/runtime/network.h"
#include "src/runtime/production/network.h"
#include "src/runtime/production/network_connect_internal.h"
#include "src/runtime/production/network_test_support.h"
#include "src/runtime/testing/contracts/network_contract.h"
#include "src/runtime/testing/contracts/network_contract_failure_cases.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <chrono>
#include <cstddef>
#include <exception>
#include <new>
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
