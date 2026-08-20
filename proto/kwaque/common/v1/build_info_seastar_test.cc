#include "proto/kwaque/common/v1/build_info_seastar_compat.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

SEASTAR_TEST_CASE(generated_build_info_crosses_a_seastar_future) {
    kwaque::common::v1::BuildInfo expected;
    expected.set_version("0.1.0");
    expected.set_revision("0123456789abcdef");
    expected.set_build_mode("release");

    const auto actual = co_await kwaque::common::v1::make_ready_build_info(
      expected);
    BOOST_REQUIRE_EQUAL(actual.version(), expected.version());
    BOOST_REQUIRE_EQUAL(actual.revision(), expected.revision());
    BOOST_REQUIRE_EQUAL(actual.build_mode(), expected.build_mode());
}
