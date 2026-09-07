#include "src/runtime/fault.h"
#include "src/simulation/tests/fuzz_cases.h"
#include "src/simulation/tests/fuzz_reproduction.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <cstdint>
#include <vector>

SEASTAR_TEST_CASE(
  file_histories_reconcile_each_io_profile_and_durability_choice) {
    co_await seastar::async([] {
        using namespace kwaque::simulation;
        using namespace kwaque::simulation::testing;
        for (std::uint8_t profile = 0; profile < 13; ++profile) {
            for (std::uint8_t durability = 0; durability < 4; ++durability) {
                std::vector<std::uint8_t> script(8, 0);
                script[0] = static_cast<std::uint8_t>(17U + profile);
                script.insert(
                  script.end(), {'F', profile, 41, 3, 7, durability});
                const auto captured = execute_fuzz_case(
                  fuzz_harness::fake_file, script);
                BOOST_REQUIRE(captured.has_value());
                BOOST_REQUIRE_MESSAGE(
                  captured->outcome().code == kwaque::errc::success,
                  "file history profile=" << static_cast<unsigned>(profile)
                                          << " durability="
                                          << static_cast<unsigned>(durability));
                BOOST_CHECK_LE(
                  captured->trace().size(),
                  fuzz_trace_budget(fuzz_harness::fake_file).encoded_bytes);
                const auto decoded = decode_fuzz_trace(
                                       captured->trace(),
                                       fuzz_harness::fake_file)
                                       .get();
                BOOST_REQUIRE(decoded.has_value());
                BOOST_CHECK(
                  std::ranges::any_of(decoded->entries, [](const auto& entry) {
                      return entry.action == trace_action::fault_evaluated
                             && entry.stable_id == 1 && entry.result != 0;
                  }));
                BOOST_CHECK(
                  std::ranges::any_of(decoded->entries, [](const auto& entry) {
                      return entry.action == trace_action::crash_applied;
                  }));
                BOOST_REQUIRE(replay_fuzz_case(*captured).has_value());
                const auto repeated = execute_fuzz_case(
                  fuzz_harness::fake_file, script);
                BOOST_REQUIRE(repeated.has_value());
                BOOST_CHECK(
                  repeated->terminal_digest() == captured->terminal_digest());
                BOOST_CHECK(repeated->trace() == captured->trace());
                BOOST_CHECK(repeated->events() == captured->events());
            }
        }
    });
}
