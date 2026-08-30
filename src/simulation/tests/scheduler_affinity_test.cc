#include "src/base/invariant_test_observer.h"
#include "src/simulation/scheduler.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>
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

SEASTAR_TEST_CASE(scheduler_rejects_foreign_shard_access) {
    kwaque::simulation::scheduler target{
      kwaque::simulation::scheduler_limits::defaults()};
    BOOST_REQUIRE_GE(seastar::this_smp_shard_count(), 2U);

    const bool observed = co_await seastar::smp::submit_to(1, [&target] {
        observed_diagnostic.clear();
        kwaque::testing::scoped_invariant_observer observer{observe_and_throw};
        try {
            static_cast<void>(target.pending_events());
        } catch (const observed_invariant&) {
            return observed_diagnostic.find("id=KQ-WRONG-SHARD-ACCESS")
                     != std::string::npos
                   && observed_diagnostic.find("expected=0 current=1")
                        != std::string::npos
                   && observed_diagnostic.find("/home/") == std::string::npos;
        }
        return false;
    });

    BOOST_REQUIRE(observed);
}
