#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

SEASTAR_TEST_CASE(reactor_runs_a_coroutine) {
    BOOST_REQUIRE_EQUAL(seastar::this_shard_id(), 0U);
    co_return;
}
