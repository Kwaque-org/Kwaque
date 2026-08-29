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
    const auto write_result = co_await connected->write(
      std::move(payload), abort_source);
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
