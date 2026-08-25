#include "src/base/invariant_test_observer.h"
#include "src/runtime/shard_affinity.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <exception>
#include <string>
#include <string_view>
#include <type_traits>

namespace {

class observed_invariant final : public std::exception {};

thread_local std::string observed_diagnostic;

void observe_and_throw(std::string_view diagnostic) {
    observed_diagnostic.assign(diagnostic);
    throw observed_invariant{};
}

class affine_probe final : public kwaque::runtime::shard_affine {};

static_assert(!std::is_copy_constructible_v<affine_probe>);
static_assert(!std::is_copy_assignable_v<affine_probe>);
static_assert(!std::is_move_constructible_v<affine_probe>);
static_assert(!std::is_move_assignable_v<affine_probe>);

} // namespace

SEASTAR_TEST_CASE(owner_shard_rejects_foreign_access_with_stable_identity) {
    kwaque::runtime::owner_shard owner;
    BOOST_REQUIRE(owner.is_current());
    owner.assert_current();
    BOOST_REQUIRE_GE(seastar::this_smp_shard_count(), 2U);

    const bool observed = co_await seastar::smp::submit_to(1, [owner] {
        observed_diagnostic.clear();
        kwaque::testing::scoped_invariant_observer observer{observe_and_throw};
        try {
            owner.assert_current();
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
