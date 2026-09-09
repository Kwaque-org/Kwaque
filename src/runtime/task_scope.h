#pragma once

#include "src/runtime/error.h"
#include "src/runtime/shard_affinity.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/util/optimized_optional.hh>

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <type_traits>
#include <utility>

namespace kwaque::runtime {

struct task_scope_statistics final {
    std::uint64_t accepted{0};
    std::uint64_t completed{0};
    std::uint64_t failed{0};
    std::uint64_t abort_requests{0};

    bool operator==(const task_scope_statistics&) const = default;
};

enum class task_lifetime {
    finite,
    // Returning successfully requires a prior request on this scope's abort
    // source. Only native abort_requested_exception after that request is an
    // expected terminal exception; other failures still reach the owner.
    until_abort,
};

// Owns background work on one shard. Await close() before destroying the
// owner; close first requests cancellation, then waits for every accepted task.
class task_scope final : public shard_affine {
public:
    using failure_notifier
      = seastar::noncopyable_function<void(std::exception_ptr)>;

    task_scope() = default;
    // The notifier is retained until scope destruction and runs on the owner
    // shard at the first escaping failure, before close(). It must not throw.
    // Essential owners supply a terminal action; finite owners may report the
    // failure and retain close() as their completion boundary.
    explicit task_scope(failure_notifier notifier);
    // Parent and scope may be destroyed in either order. Destroying the parent
    // first removes propagation without requesting abort on this scope.
    explicit task_scope(seastar::abort_source& parent);
    task_scope(seastar::abort_source& parent, failure_notifier notifier);

    ~task_scope();

    template<typename Func>
    requires std::constructible_from<std::remove_cvref_t<Func>, Func&&>
             && std::invocable<std::remove_cvref_t<Func>&>
             && std::same_as<
               seastar::futurize_t<
                 std::invoke_result_t<std::remove_cvref_t<Func>&>>,
               seastar::future<>>
    [[nodiscard]] result<void>
    spawn(Func&& task, task_lifetime lifetime = task_lifetime::finite) {
        assert_current();
        auto holder = gate_.try_hold();
        if (!holder) {
            return failure(
              operation_error{errc::closed, operation_kind::resource});
        }

        auto tracked = invoke_owned(std::forward<Func>(task))
                         .then_wrapped(
                           [this, lifetime, holder = std::move(*holder)](
                             seastar::future<> completion) mutable noexcept {
                               static_cast<void>(holder);
                               complete_task(std::move(completion), lifetime);
                           });
        ++statistics_.accepted;
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
    [[nodiscard]] task_scope_statistics statistics() const;
    [[nodiscard]] seastar::abort_source& abort_source();

private:
    template<typename Func>
    static seastar::future<> invoke_owned(Func task) {
        co_await seastar::futurize_invoke(task);
    }

    void request_abort_unchecked() noexcept;
    void complete_task(
      seastar::future<> completion, task_lifetime lifetime) noexcept;
    [[nodiscard]] seastar::future<> close_once();

    seastar::abort_source abort_source_;
    seastar::optimized_optional<seastar::abort_source::subscription>
      parent_subscription_;
    seastar::gate gate_;
    seastar::shared_promise<> close_done_;
    failure_notifier failure_notifier_;
    std::exception_ptr first_failure_;
    task_scope_statistics statistics_;
    bool closing_{false};
};

} // namespace kwaque::runtime
