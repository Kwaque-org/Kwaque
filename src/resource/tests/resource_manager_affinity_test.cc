#include "src/base/invariant_test_observer.h"
#include "src/base/units.h"
#include "src/resource/memory_budget.h"
#include "src/resource/reclaimer_registry.h"
#include "src/resource/resource_config.h"
#include "src/resource/resource_manager.h"
#include "src/resource/resource_registry.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/smp.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <cstdint>
#include <exception>
#include <stdexcept>
#include <string>
#include <string_view>

namespace kwaque::resource {

namespace {

class observed_invariant final : public std::exception {};

thread_local std::string observed_diagnostic;

void observe_and_throw(std::string_view diagnostic) {
    observed_diagnostic.assign(diagnostic);
    throw observed_invariant{};
}

resource_config local_memory_config() {
    auto config = resource_config::from_total_memory(
      byte_count{
        static_cast<std::uint64_t>(seastar::memory::stats().total_memory())});
    if (!config) {
        throw std::runtime_error("test resource configuration was rejected");
    }
    return *config;
}

} // namespace

SEASTAR_TEST_CASE(resource_manager_rejects_foreign_shard_access) {
    BOOST_REQUIRE_GE(seastar::this_smp_shard_count(), 2U);
    resource_registry registry;
    co_await registry.start(local_memory_config());
    resource_manager manager{registry.handles()};
    co_await manager.start();

    resource_manager* manager_address = &manager;
    const bool observed = co_await seastar::smp::submit_to(
      1, [manager_address] {
          observed_diagnostic.clear();
          testing::scoped_invariant_observer observer{observe_and_throw};
          try {
              static_cast<void>(manager_address->ready());
          } catch (const observed_invariant&) {
              return observed_diagnostic.find("id=KQ-WRONG-SHARD-ACCESS")
                       != std::string::npos
                     && observed_diagnostic.find("expected=0 current=1")
                          != std::string::npos;
          }
          return false;
      });

    BOOST_CHECK(observed);
    co_await manager.stop();
    co_await registry.stop();
}

SEASTAR_TEST_CASE(memory_resources_reject_foreign_shard_access) {
    BOOST_REQUIRE_GE(seastar::this_smp_shard_count(), 2U);
    memory_budget budget{memory_budget_config{
      .capacity = byte_count{100},
      .soft_watermark = byte_count{50},
      .high_watermark = byte_count{80},
      .max_waiters = 1,
    }};
    reclaimer_registry reclaimers{1};
    reclaimers.start();

    auto* budget_address = &budget;
    auto* registry_address = &reclaimers;
    const auto observed = co_await seastar::smp::submit_to(
      1, [budget_address, registry_address] {
          std::array<bool, 2> failures{};
          testing::scoped_invariant_observer observer{observe_and_throw};
          try {
              static_cast<void>(budget_address->capacity());
          } catch (const observed_invariant&) {
              failures[0] = observed_diagnostic.find("id=KQ-WRONG-SHARD-ACCESS")
                            != std::string::npos;
          }
          try {
              static_cast<void>(
                registry_address->request_reclaim(byte_count{1}));
          } catch (const observed_invariant&) {
              failures[1] = observed_diagnostic.find("id=KQ-WRONG-SHARD-ACCESS")
                            != std::string::npos;
          }
          return failures;
      });

    BOOST_CHECK(observed[0]);
    BOOST_CHECK(observed[1]);
    reclaimers.stop();
}

} // namespace kwaque::resource
