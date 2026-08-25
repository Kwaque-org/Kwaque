#include "src/runtime/runtime_service.h"

#include <seastar/core/future.hh>

#include <gtest/gtest.h>

#include <cstdlib>
#include <utility>

namespace {

TEST(
  RuntimeServiceInvariantDeathTest,
  RejectsDestructionWhilePendingWorkKeepsTaskScopeOpen) {
    EXPECT_DEATH(
      {
          seastar::promise<> release;
          kwaque::runtime::runtime_service service;
          service.start().get();
          const auto accepted = service.tasks().spawn(
            [pending = release.get_future()]() mutable {
                return std::move(pending);
            });
          if (!accepted.has_value()) {
              std::abort();
          }
      },
      "id=KQ-TASK-SCOPE-CLOSED");
}

} // namespace
