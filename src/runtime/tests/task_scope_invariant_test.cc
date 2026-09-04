#include "src/runtime/task_scope.h"

#include <seastar/core/future.hh>

#include <gtest/gtest.h>

#include <cstdlib>
#include <utility>

namespace {

TEST(
  TaskScopeInvariantDeathTest,
  RejectsDestructionWhilePendingWorkKeepsScopeOpen) {
    EXPECT_DEATH(
      {
          seastar::promise<> release;
          kwaque::runtime::task_scope scope;
          const auto accepted = scope.spawn(
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
