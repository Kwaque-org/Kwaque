#include "src/runtime/environment.h"
#include "src/runtime/testing/contracts/contract_backends.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <optional>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

using production_backend = kwaque::runtime::testing::production_shaped_backend;
using deterministic_backend
  = kwaque::runtime::testing::deterministic_shaped_backend;

template<typename Backend>
concept exposes_fault_injector = requires(Backend& backend) {
    backend.faults();
};

template<typename View>
concept exposes_random = requires(View& view) { view.random(); };

using timer_only_view = kwaque::runtime::basic_runtime_view<
  production_backend,
  kwaque::runtime::runtime_capability::timer>;

static_assert(kwaque::runtime::runtime_backend<production_backend>);
static_assert(kwaque::runtime::runtime_backend<deterministic_backend>);
static_assert(!exposes_fault_injector<production_backend>);
static_assert(exposes_fault_injector<deterministic_backend>);
static_assert(!exposes_random<timer_only_view>);
static_assert(sizeof(timer_only_view) <= 32);
static_assert(sizeof(kwaque::runtime::basic_runtime<production_backend>) <= 32);

template<kwaque::runtime::runtime_backend Backend>
seastar::future<> exercise_contract(Backend& backend) {
    kwaque::runtime::basic_runtime root{backend};
    auto acquired = root.template view<
      kwaque::runtime::runtime_capability::timer,
      kwaque::runtime::runtime_capability::random,
      kwaque::runtime::runtime_capability::file_system,
      kwaque::runtime::runtime_capability::network,
      kwaque::runtime::runtime_capability::dns,
      kwaque::runtime::runtime_capability::fault>();
    BOOST_REQUIRE(acquired.has_value());
    auto view = std::move(*acquired);

    static_cast<void>(Backend::monotonic_clock::now());
    static_cast<void>(Backend::wall_clock::now());
    static_cast<void>(view.random().next_u64());
    seastar::abort_source abort_source;
    const auto timer_result = co_await view.timer().sleep_until(
      kwaque::runtime::monotonic_time{101}, abort_source);
    BOOST_REQUIRE(!timer_result.has_value());

    auto path = kwaque::runtime::file_path::make("contract-path");
    BOOST_REQUIRE(path.has_value());
    const auto exists = co_await view.file_system().exists(std::move(*path));
    BOOST_REQUIRE(exists.has_value());
    BOOST_CHECK(!*exists);

    const auto endpoint = kwaque::runtime::testing::detail::loopback(33145);
    auto connected = co_await view.network().connect(
      endpoint,
      std::nullopt,
      kwaque::runtime::network_connection_limits{},
      abort_source);
    BOOST_REQUIRE(connected.has_value());
    BOOST_CHECK(
      connected->state() == kwaque::runtime::network_connection_state::open);
    BOOST_CHECK(
      connected->limits() == kwaque::runtime::network_connection_limits{});
    BOOST_REQUIRE(connected->shutdown_input().has_value());
    BOOST_CHECK(
      connected->input_state()
      == kwaque::runtime::network_half_state::shut_down);
    BOOST_CHECK(
      connected->output_state() == kwaque::runtime::network_half_state::open);
    auto payload = kwaque::bytes::fragmented_buffer::copy_of(
      std::span<const char>{"x", 1});
    BOOST_REQUIRE(payload.has_value());
    const auto write_result = co_await connected->write(
      std::move(*payload), abort_source);
    BOOST_REQUIRE(write_result.has_value());
    const auto read_result = co_await connected->read(
      kwaque::byte_count{1}, abort_source);
    BOOST_REQUIRE(!read_result.has_value());
    BOOST_CHECK(read_result.error().code() == kwaque::errc::closed);
    const auto connection_closed = co_await connected->close();
    BOOST_REQUIRE(connection_closed.has_value());
    BOOST_CHECK(
      connected->state() == kwaque::runtime::network_connection_state::closed);
    const auto connection_closed_again = co_await connected->close();
    BOOST_REQUIRE(connection_closed_again.has_value());
    const auto rejected_shutdown = connected->shutdown_output();
    BOOST_REQUIRE(!rejected_shutdown.has_value());
    BOOST_CHECK(rejected_shutdown.error().code() == kwaque::errc::closed);

    auto host = kwaque::runtime::dns_name::make("127.0.0.1");
    BOOST_REQUIRE(host.has_value());
    auto resolved = co_await view.dns().resolve(
      {.host = std::move(*host), .port = 33145}, abort_source);
    BOOST_REQUIRE(resolved.has_value());
    BOOST_REQUIRE_EQUAL(resolved->answers().size(), 1U);

    const auto* descriptor = kwaque::runtime::descriptor_for(
      kwaque::runtime::builtin_fault_point::timer);
    BOOST_REQUIRE(descriptor != nullptr);
    const kwaque::runtime::fault_request request{
      .point = descriptor->id,
      .occurrence = kwaque::runtime::fault_occurrence::first(),
      .object = kwaque::runtime::fault_object_key::none(),
    };
    auto decision = view.evaluate_fault(request);
    BOOST_REQUIRE(decision.has_value());
    BOOST_CHECK(decision->action() == kwaque::runtime::fault_action::none);
}

} // namespace

SEASTAR_TEST_CASE(runtime_consumer_instantiates_for_both_backend_shapes) {
    production_backend production;
    co_await exercise_contract(production);

    deterministic_backend deterministic;
    co_await exercise_contract(deterministic);
}

SEASTAR_TEST_CASE(
  network_contract_serializes_owning_io_and_drains_before_close) {
    production_backend backend;
    seastar::abort_source abort_source;
    const auto endpoint = kwaque::runtime::testing::detail::loopback(33146);
    auto connected = co_await backend.network().connect(
      endpoint,
      std::nullopt,
      kwaque::runtime::network_connection_limits{
        .pending_write_bytes = kwaque::byte_count{8},
        .pending_writes = 2,
      },
      abort_source);
    BOOST_REQUIRE(connected.has_value());
    connected->enable_controlled_io();

    auto first_payload = kwaque::bytes::fragmented_buffer::copy_of(
      std::span<const char>{"abc", 3});
    BOOST_REQUIRE(first_payload.has_value());
    auto first_write = connected->write(
      std::move(*first_payload), abort_source);
    BOOST_CHECK(!first_write.available());

    std::optional<seastar::future<kwaque::runtime::result<void>>> second_write;
    {
        auto second_payload = kwaque::bytes::fragmented_buffer::copy_of(
          std::span<const char>{"defg", 4});
        BOOST_REQUIRE(second_payload.has_value());
        second_write.emplace(
          connected->write(std::move(*second_payload), abort_source));
    }
    BOOST_CHECK(!second_write->available());
    BOOST_CHECK_EQUAL(connected->pending_write_count(), 2U);
    BOOST_CHECK_EQUAL(connected->pending_write_bytes().value(), 7U);
    BOOST_CHECK(connected->pending_write_content_equals(0, "abc"));
    BOOST_CHECK(connected->pending_write_content_equals(1, "defg"));

    auto rejected_payload = kwaque::bytes::fragmented_buffer::copy_of(
      std::span<const char>{"z", 1});
    BOOST_REQUIRE(rejected_payload.has_value());
    const auto rejected_write = co_await connected->write(
      std::move(*rejected_payload), abort_source);
    BOOST_REQUIRE(!rejected_write.has_value());
    BOOST_CHECK(rejected_write.error().code() == kwaque::errc::queue_full);

    auto pending_read = connected->read(kwaque::byte_count{8}, abort_source);
    BOOST_CHECK(!pending_read.available());
    BOOST_CHECK(connected->read_pending());
    const auto concurrent_read = co_await connected->read(
      kwaque::byte_count{8}, abort_source);
    BOOST_REQUIRE(!concurrent_read.has_value());
    BOOST_CHECK(concurrent_read.error().code() == kwaque::errc::unavailable);

    auto read_payload = kwaque::bytes::fragmented_buffer::copy_of(
      std::span<const char>{"hi", 2});
    BOOST_REQUIRE(read_payload.has_value());
    const bool read_completed = connected->complete_read(
      std::move(*read_payload), false);
    BOOST_REQUIRE(read_completed);
    const auto completed_read = co_await std::move(pending_read);
    BOOST_REQUIRE(completed_read.has_value());
    BOOST_CHECK(!completed_read->eof());
    BOOST_CHECK(completed_read->data().content_equals("hi"));
    auto close_interrupted_read = connected->read(
      kwaque::byte_count{8}, abort_source);
    BOOST_CHECK(!close_interrupted_read.available());

    BOOST_REQUIRE(connected->complete_next_write());
    const auto first_result = co_await std::move(first_write);
    BOOST_REQUIRE(first_result.has_value());
    BOOST_CHECK(!second_write->available());
    BOOST_CHECK_EQUAL(connected->pending_write_count(), 1U);

    const auto closed = co_await connected->close();
    BOOST_REQUIRE(closed.has_value());
    const auto second_result = co_await std::move(*second_write);
    BOOST_REQUIRE(!second_result.has_value());
    BOOST_CHECK(second_result.error().code() == kwaque::errc::closed);
    const auto read_result = co_await std::move(close_interrupted_read);
    BOOST_REQUIRE(!read_result.has_value());
    BOOST_CHECK(read_result.error().code() == kwaque::errc::closed);
    BOOST_CHECK_EQUAL(connected->pending_write_count(), 0U);
    BOOST_CHECK_EQUAL(connected->pending_write_bytes().value(), 0U);
    BOOST_CHECK(!connected->read_pending());
    BOOST_CHECK(!connected->complete_next_write());
    BOOST_CHECK(
      !connected->complete_read(kwaque::bytes::fragmented_buffer{}, true));

    const auto closed_again = co_await connected->close();
    BOOST_REQUIRE(closed_again.has_value());
}

SEASTAR_TEST_CASE(network_contract_abort_resolves_every_accepted_operation) {
    production_backend backend;
    seastar::abort_source abort_source;
    const auto endpoint = kwaque::runtime::testing::detail::loopback(33147);
    auto connected = co_await backend.network().connect(
      endpoint,
      std::nullopt,
      kwaque::runtime::network_connection_limits{
        .pending_write_bytes = kwaque::byte_count{8},
        .pending_writes = 1,
      },
      abort_source);
    BOOST_REQUIRE(connected.has_value());
    connected->enable_controlled_io();

    auto pending_read = connected->read(kwaque::byte_count{8}, abort_source);
    auto payload = kwaque::bytes::fragmented_buffer::copy_of(
      std::span<const char>{"abc", 3});
    BOOST_REQUIRE(payload.has_value());
    auto pending_write = connected->write(std::move(*payload), abort_source);
    BOOST_CHECK(!pending_read.available());
    BOOST_CHECK(!pending_write.available());

    connected->request_abort();
    const auto read_result = co_await std::move(pending_read);
    const auto write_result = co_await std::move(pending_write);
    BOOST_REQUIRE(!read_result.has_value());
    BOOST_REQUIRE(!write_result.has_value());
    BOOST_CHECK(read_result.error().code() == kwaque::errc::aborted);
    BOOST_CHECK(write_result.error().code() == kwaque::errc::aborted);
    BOOST_CHECK(!connected->read_pending());
    BOOST_CHECK_EQUAL(connected->pending_write_count(), 0U);

    const auto rejected_read = co_await connected->read(
      kwaque::byte_count{8}, abort_source);
    BOOST_REQUIRE(!rejected_read.has_value());
    BOOST_CHECK(rejected_read.error().code() == kwaque::errc::aborted);
    const auto closed = co_await connected->close();
    BOOST_REQUIRE(closed.has_value());
}

SEASTAR_TEST_CASE(runtime_lifetime_waits_for_root_and_component_views) {
    production_backend backend;
    std::optional<seastar::future<>> closing;
    {
        kwaque::runtime::basic_runtime root{backend};
        auto acquired
          = root.view<kwaque::runtime::runtime_capability::random>();
        BOOST_REQUIRE(acquired.has_value());
        std::optional view{std::move(*acquired)};
        BOOST_CHECK_EQUAL(backend.lifetime().leases(), 2U);

        closing.emplace(backend.lifetime().close());
        BOOST_CHECK(!closing->available());
        BOOST_CHECK(
          backend.lifetime().state()
          == kwaque::runtime::runtime_lifetime_state::closing);

        view.reset();
        BOOST_CHECK(!closing->available());
        BOOST_CHECK_EQUAL(backend.lifetime().leases(), 1U);
    }

    co_await std::move(*closing);
    BOOST_CHECK(
      backend.lifetime().state()
      == kwaque::runtime::runtime_lifetime_state::closed);
    BOOST_CHECK_EQUAL(backend.lifetime().leases(), 0U);
}

SEASTAR_TEST_CASE(runtime_rejects_new_views_after_backend_close_begins) {
    production_backend backend;
    std::optional<seastar::future<>> closing;
    {
        kwaque::runtime::basic_runtime root{backend};
        closing.emplace(backend.lifetime().close());

        const auto rejected
          = root.view<kwaque::runtime::runtime_capability::random>();
        BOOST_REQUIRE(!rejected.has_value());
        BOOST_CHECK(rejected.error().code() == kwaque::errc::closed);
    }

    co_await std::move(*closing);
}

SEASTAR_TEST_CASE(runtime_never_activated_close_completes_inline) {
    production_backend backend;
    auto closing = backend.lifetime().close();
    BOOST_CHECK(closing.available());
    co_await std::move(closing);
    BOOST_CHECK(
      backend.lifetime().state()
      == kwaque::runtime::runtime_lifetime_state::closed);
}

SEASTAR_TEST_CASE(component_gate_closes_before_runtime_backend_completion) {
    production_backend backend;
    std::optional<seastar::future<>> backend_closing;
    std::vector<unsigned> order;
    {
        kwaque::runtime::basic_runtime root{backend};
        auto acquired
          = root.view<kwaque::runtime::runtime_capability::network>();
        BOOST_REQUIRE(acquired.has_value());
        std::optional view{std::move(*acquired)};

        seastar::gate component_gate;
        std::optional work{component_gate.hold()};
        auto component_closing = component_gate.close().then([&] {
            order.push_back(1);
            view.reset();
        });
        backend_closing.emplace(backend.lifetime().close());
        BOOST_CHECK(!component_closing.available());
        BOOST_CHECK(!backend_closing->available());

        work.reset();
        co_await std::move(component_closing);
        BOOST_REQUIRE_EQUAL(order.size(), 1U);
        BOOST_CHECK(!backend_closing->available());
    }

    co_await std::move(*backend_closing);
    order.push_back(2);
    BOOST_REQUIRE_EQUAL(order.size(), 2U);
    BOOST_CHECK_EQUAL(order[0], 1U);
    BOOST_CHECK_EQUAL(order[1], 2U);
}
