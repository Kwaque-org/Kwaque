#include "src/base/invariant_test_observer.h"
#include "src/simulation/scheduler.h"
#include "src/simulation/virtual_time.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <exception>
#include <string>
#include <string_view>

namespace {

class observed_invariant final : public std::exception {};

thread_local std::string observed_diagnostic;

void observe_and_throw(std::string_view diagnostic) {
    observed_diagnostic.assign(diagnostic);
    throw observed_invariant{};
}

} // namespace

SEASTAR_TEST_CASE(virtual_clock_rejects_a_second_active_binding) {
    const auto limits = kwaque::simulation::scheduler_limits::defaults();
    kwaque::simulation::scheduler target{limits};
    const auto config = kwaque::simulation::virtual_time_config::make(limits);
    BOOST_REQUIRE(config.has_value());
    kwaque::simulation::virtual_time time{target, *config};
    kwaque::simulation::clock_binding first{time};

    observed_diagnostic.clear();
    kwaque::testing::scoped_invariant_observer observer{observe_and_throw};
    try {
        kwaque::simulation::clock_binding duplicate{time};
        static_cast<void>(duplicate);
    } catch (const observed_invariant&) {
        BOOST_REQUIRE(
          observed_diagnostic.find("id=KQ-CLOCK-BINDING") != std::string::npos);
        BOOST_REQUIRE(observed_diagnostic.find("/home/") == std::string::npos);
        co_return;
    }
    BOOST_FAIL("second simulation clock binding was accepted");
}
