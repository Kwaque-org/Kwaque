#include "src/config/bootstrap_config.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <cstddef>
#include <cstdint>
#include <new>
#include <optional>
#include <string>

SEASTAR_TEST_CASE(bootstrap_config_allocation_failures_do_not_terminate) {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    const std::string document = "kwaque:\n  schema_version: 1\n  node_id: 7\n"
                                 "  data_directory: "
                                 + std::string(256, 'p') + "\n";
    const auto expected = kwaque::config::parse_bootstrap_config(document);
    BOOST_REQUIRE(expected.has_value());
    bool completed = false;
    std::size_t failures = 0;
    std::size_t exceptions = 0;
    for (std::uint64_t fail_after = 0; fail_after < 4'096; ++fail_after) {
        std::optional<kwaque::config::bootstrap_config_result> parsed;
        auto& injector = seastar::memory::local_failure_injector();
        injector.fail_after(fail_after);
        try {
            parsed.emplace(kwaque::config::parse_bootstrap_config(document));
        } catch (const std::bad_alloc&) {
            ++exceptions;
        } catch (...) {
            injector.cancel();
            throw;
        }
        const bool injected = injector.failed();
        injector.cancel();
        if (parsed && parsed->has_value()) {
            BOOST_CHECK(**parsed == *expected);
        }
        if (!injected) {
            BOOST_REQUIRE(parsed.has_value());
            BOOST_REQUIRE(parsed->has_value());
            completed = true;
            break;
        }
        ++failures;
        // Dependencies may translate an allocation failure into a parse error.
        // It must never terminate the process or corrupt the next parse.
        const auto retried = kwaque::config::parse_bootstrap_config(document);
        BOOST_REQUIRE(retried.has_value());
        BOOST_CHECK(*retried == *expected);
        if ((fail_after + 1U) % 16U == 0) {
            co_await seastar::yield();
        }
    }
    BOOST_CHECK(completed);
    BOOST_CHECK_GT(failures, 0U);
    BOOST_CHECK_GT(exceptions, 0U);
#endif
    co_return;
}
