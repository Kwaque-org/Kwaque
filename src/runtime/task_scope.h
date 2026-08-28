#pragma once

#include "src/runtime/error.h"
#include "src/runtime/shard_affinity.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/util/optimized_optional.hh>

#include <concepts>
#include <cstddef>
#include <exception>
#include <functional>
#include <type_traits>
#include <utility>

namespace kwaque::runtime {

// Owns background work on one shard. Await close() before destroying the
// owner; close first requests cancellation, then waits for every accepted task.
class task_scope final : public shard_affine {
public:
    task_scope() noexcept = default;
    // Parent and scope may be destroyed in either order. Destroying the parent
    // first removes propagation without requesting abort on this scope.
    explicit task_scope(seastar::abort_source& parent);

    ~task_scope();

    template<typename Func>
    requires std::constructible_from<std::remove_cvref_t<Func>, Func&&>
             && std::invocable<std::remove_cvref_t<Func>&>
             && std::same_as<
               seastar::futurize_t<
                 std::invoke_result_t<std::remove_cvref_t<Func>&>>,
               seastar::future<>>
    [[nodiscard]] result<void> spawn(Func&& task) {
        assert_current();
        auto holder = gate_.try_hold();
        if (!holder) {
            return failure(
              operation_error{errc::closed, operation_kind::resource});
        }

        auto tracked
          = invoke_owned(std::forward<Func>(task))
              .then_wrapped([this, holder = std::move(*holder)](
                              seastar::future<> completion) mutable noexcept {
                  static_cast<void>(holder);
                  try {
                      completion.get();
                  } catch (...) {
                      if (!first_failure_) {
                          first_failure_ = std::current_exception();
                      }
                  }
              });
        static_cast<void>(tracked);
        return {};
    }

    void request_abort();
    [[nodiscard]] seastar::future<> close();

    [[nodiscard]] bool abort_requested() const;
    // True once new task admission has closed. Existing accepted tasks may
    // still be draining; the future returned by close() is the completion
    // signal.
    [[nodiscard]] bool admission_closed() const;
    [[nodiscard]] std::size_t task_count() const;
    [[nodiscard]] seastar::abort_source& abort_source();

private:
    template<typename Func>
    static seastar::future<> invoke_owned(Func task) {
        co_await seastar::futurize_invoke(task);
    }

    void request_abort_unchecked() noexcept;
    [[nodiscard]] seastar::future<> close_once();

    seastar::abort_source abort_source_;
    seastar::optimized_optional<seastar::abort_source::subscription>
      parent_subscription_;
    seastar::gate gate_;
    seastar::shared_promise<> close_done_;
    std::exception_ptr first_failure_;
    bool closing_{false};
};

} // namespace kwaque::runtime
