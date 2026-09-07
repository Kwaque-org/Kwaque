#include "src/bytes/fragmented_buffer.h"
#include "src/runtime/error.h"
#include "src/runtime/network.h"
#include "src/simulation/event_trace.h"
#include "src/simulation/fake_network.h"
#include "src/simulation/scheduler.h"
#include "src/simulation/tests/fuzz_cases.h"
#include "src/simulation/tests/fuzz_network_cases.h"
#include "src/simulation/tests/fuzz_reproduction.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using kwaque::simulation::testing::fuzz_case_detail::wait_for;
using kwaque::simulation::testing::fuzz_case_detail::with_cleanup;

std::vector<std::uint8_t> network_script(
  std::uint8_t count,
  std::uint8_t options,
  std::uint8_t action,
  std::uint8_t controls = 0) {
    std::vector<std::uint8_t> script(8, 0);
    script.insert(
      script.end(),
      {
        count,
        options,
        action,
        17,
        controls,
        0,
        123,
        0,
        3,
        1,
        57,
        0,
        7,
        2,
        0,
        0,
        11,
      });
    return script;
}

bool check_received_stream(std::string_view actual, std::string_view expected) {
    using namespace kwaque::simulation;
    using kwaque::runtime::network_address;
    using kwaque::runtime::network_endpoint;
    const auto budget = scheduler_limits::make({
      .pending_events = 256,
      .events_per_pump = 64,
      .total_events = 4'096,
      .maximum_deadline = kwaque::runtime::monotonic_time{65'535},
    });
    BOOST_REQUIRE(budget.has_value());
    scheduler events{*budget};
    fake_network_config config;
    config.maximum_listeners = 1;
    config.maximum_connection_pairs = 1;
    config.maximum_pending_connects = 1;
    config.maximum_backlog_entries = 1;
    config.maximum_operations = 4;
    config.maximum_parked_operations = 1;
    config.maximum_packets = 4;
    config.maximum_direction_packets = 2;
    config.maximum_links = 2;
    config.maximum_active_flows = 1;
    config.stop_batch = 8;
    auto made = fake_network::make(config, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);
    std::optional<fake_listener> listener;
    std::optional<fake_connection> client;
    std::optional<fake_connection> server;
    seastar::abort_source abort_source;
    return with_cleanup(
      [&] {
          const auto address = network_address::ipv4(
            {std::byte{127}, std::byte{0}, std::byte{0}, std::byte{2}});
          auto bound = wait_for(
            events, network->listen(network_endpoint{address, 0}, {}));
          BOOST_REQUIRE(bound.has_value());
          listener.emplace(std::move(*bound));
          auto accepting = listener->accept(abort_source);
          auto connecting = network->connect(
            listener->local_endpoint(), std::nullopt, {}, abort_source);
          auto connected = wait_for(events, std::move(connecting));
          BOOST_REQUIRE(connected.has_value());
          client.emplace(std::move(*connected));
          auto accepted = wait_for(events, std::move(accepting));
          BOOST_REQUIRE(accepted.has_value());
          server.emplace(std::move(*accepted));
          if (!actual.empty()) {
              auto payload = kwaque::bytes::fragmented_buffer::copy_of(
                std::span<const char>{actual.data(), actual.size()});
              BOOST_REQUIRE(payload.has_value());
              const auto written = wait_for(
                events, client->write(std::move(*payload), abort_source));
              BOOST_REQUIRE(written.has_value());
          }
          BOOST_REQUIRE(client->shutdown_output().has_value());
          return kwaque::simulation::testing::network_stream_is_exact(
            events, *server, expected, abort_source);
      },
      [&] {
          const auto stopped = wait_for(events, network->stop());
          BOOST_REQUIRE(stopped.has_value());
          client.reset();
          server.reset();
          listener.reset();
          BOOST_CHECK_EQUAL(network->active_operations(), 0U);
          BOOST_CHECK_EQUAL(events.pending_events(), 0U);
      });
}

} // namespace

SEASTAR_TEST_CASE(
  fuzz_network_reconciliation_rejects_surplus_missing_and_changed_bytes) {
    co_await seastar::async([] {
        struct row final {
            std::string_view actual;
            std::string_view expected;
            bool matches;
        };
        constexpr std::array cases{
          row{"payload", "payload", true},
          row{"payload-extra", "payload", false},
          row{"payload", "payload-extra", false},
          row{"payloae", "payload", false},
          row{"x", "", false},
          row{"", "", true},
        };
        for (const auto& test : cases) {
            BOOST_CHECK_EQUAL(
              check_received_stream(test.actual, test.expected), test.matches);
        }
    });
}

SEASTAR_TEST_CASE(
  fuzz_network_runs_actual_concurrent_flow_bounds_in_both_directions) {
    co_await seastar::async([] {
        using namespace kwaque::simulation;
        using namespace kwaque::simulation::testing;
        constexpr std::array counts{1U, 8U, 32U, 96U};
        for (std::uint8_t selector = 0; selector < counts.size(); ++selector) {
            for (std::uint8_t topology = 0; topology < 2; ++topology) {
                const auto script = network_script(
                  selector, topology, 0, selector % 4U);
                const auto captured = execute_fuzz_case(
                  fuzz_harness::fake_network, script);
                BOOST_REQUIRE(captured.has_value());
                BOOST_REQUIRE(
                  captured->outcome().code == kwaque::errc::success);
                auto decoded = decode_fuzz_trace(
                                 captured->trace(), fuzz_harness::fake_network)
                                 .get();
                BOOST_REQUIRE(decoded.has_value());
                std::uint64_t maximum_active = 0;
                std::size_t rebalances = 0;
                for (const auto& entry : decoded->entries) {
                    if (entry.action == trace_action::bandwidth_rebalanced) {
                        maximum_active = std::max(
                          maximum_active, entry.coordinate_a);
                        ++rebalances;
                    }
                }
                BOOST_CHECK_EQUAL(maximum_active, counts[selector]);
                BOOST_CHECK_GT(rebalances, counts[selector]);
                BOOST_REQUIRE(replay_fuzz_case(*captured).has_value());
            }
        }
    });
}

SEASTAR_TEST_CASE(
  fuzz_network_fault_profiles_and_mixed_maximum_history_replay) {
    co_await seastar::async([] {
        using namespace kwaque::simulation;
        using namespace kwaque::simulation::testing;
        for (std::uint8_t action = 1; action <= 9; ++action) {
            const auto captured = execute_fuzz_case(
              fuzz_harness::fake_network,
              network_script(0, action % 2U, action));
            BOOST_REQUIRE(captured.has_value());
            BOOST_REQUIRE(captured->outcome().code == kwaque::errc::success);
            BOOST_REQUIRE(replay_fuzz_case(*captured).has_value());
        }
        for (const auto& script :
             {network_script(3, 3, 2), network_script(3, 0, 6)}) {
            const auto captured = execute_fuzz_case(
              fuzz_harness::fake_network, script);
            BOOST_REQUIRE(captured.has_value());
            BOOST_REQUIRE(captured->outcome().code == kwaque::errc::success);
            BOOST_REQUIRE(replay_fuzz_case(*captured).has_value());
        }
    });
}
