#include "src/simulation/fake_dns.h"
#include "src/simulation/fake_network.h"
#include "src/simulation/scheduler.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <utility>

namespace {

kwaque::simulation::scheduler_limits network_scheduler_limits() {
    auto limits = kwaque::simulation::scheduler_limits::make(
      kwaque::simulation::scheduler_limit_values{
        .pending_events = 65'536,
        .events_per_pump = 4'096,
        .total_events = 100'000,
        .maximum_deadline = kwaque::runtime::monotonic_time{10'000'000'000ULL},
      });
    BOOST_REQUIRE(limits.has_value());
    return *limits;
}

} // namespace

SEASTAR_TEST_CASE(fake_network_has_one_nontransportable_shard_owner) {
    kwaque::simulation::scheduler events{network_scheduler_limits()};
    auto made = kwaque::simulation::fake_network::make({}, events);
    BOOST_REQUIRE(made.has_value());
    auto network = std::move(*made);
    const auto owner = network->owner();
    BOOST_REQUIRE(owner.is_current());
    BOOST_CHECK(owner == events.owner());
    BOOST_REQUIRE_GE(seastar::this_smp_shard_count(), 2U);

    const auto rejected = co_await seastar::smp::submit_to(
      1, [owner] { return !owner.is_current(); });
    BOOST_REQUIRE(rejected);
}

SEASTAR_TEST_CASE(fake_dns_has_one_nontransportable_shard_owner) {
    kwaque::simulation::scheduler events{network_scheduler_limits()};
    auto made = kwaque::simulation::fake_dns::make({}, events);
    BOOST_REQUIRE(made.has_value());
    auto resolver = std::move(*made);
    const auto owner = resolver->owner();
    BOOST_REQUIRE(owner.is_current());
    BOOST_CHECK(owner == events.owner());
    BOOST_REQUIRE_GE(seastar::this_smp_shard_count(), 2U);

    const auto rejected = co_await seastar::smp::submit_to(
      1, [owner] { return !owner.is_current(); });
    BOOST_REQUIRE(rejected);
}
