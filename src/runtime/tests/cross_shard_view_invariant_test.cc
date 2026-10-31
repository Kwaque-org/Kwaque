#include "src/bytes/fragmented_buffer.h"
#include "src/runtime/cross_shard.h"
#include "src/runtime/shard_affinity.h"

#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>

#include <gtest/gtest.h>

#include <memory>
#include <utility>

namespace {

using kwaque::bytes::fragmented_buffer;
using kwaque::runtime::lendable;
using view = kwaque::runtime::cross_shard_view<const fragmented_buffer>;

seastar::promise<>& never() {
    static seastar::promise<> pending;
    return pending;
}

// Reads nothing and never returns, so the loan stays open.
seastar::future<> hold_the_loan(const view&) { return never().get_future(); }

TEST(CrossShardViewInvariantDeathTest, RefusesDestroyingALentReferent) {
    EXPECT_DEATH(
      {
          auto lent = std::make_unique<lendable<fragmented_buffer>>(
            fragmented_buffer{});
          auto pending = kwaque::runtime::borrow_on_owner(
            kwaque::runtime::owner_shard{},
            seastar::default_smp_service_group(),
            &hold_the_loan,
            *lent);
          lent.reset();
      },
      "id=KQ-CROSS-SHARD-LENT");
}

TEST(CrossShardViewInvariantDeathTest, RefusesReleasingALentReferent) {
    EXPECT_DEATH(
      {
          lendable<fragmented_buffer> lent{fragmented_buffer{}};
          auto pending = kwaque::runtime::borrow_on_owner(
            kwaque::runtime::owner_shard{},
            seastar::default_smp_service_group(),
            &hold_the_loan,
            lent);
          static_cast<void>(std::move(lent).release());
      },
      "id=KQ-CROSS-SHARD-LENT");
}

} // namespace
