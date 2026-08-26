#pragma once

#include "src/base/invariant.h"
#include "src/base/result.h"
#include "src/base/units.h"
#include "src/resource/resource_registry.h"
#include "src/resource/workload_class.h"
#include "src/runtime/error.h"
#include "src/runtime/shard_affinity.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/with_scheduling_group.hh>

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace kwaque::resource {

class resource_manager_test_access;

namespace detail {

template<typename Future>
struct future_value;

template<typename Value>
struct future_value<seastar::future<Value>> {
    using type = Value;
};

template<typename Func>
concept smp_group_invocable
  = std::constructible_from<std::remove_cvref_t<Func>, Func&&>
    && std::invocable<std::remove_cvref_t<Func>&, seastar::smp_service_group>;

} // namespace detail

struct workload_counters final {
    std::uint64_t queued{0};
    std::uint64_t executing{0};
    std::uint64_t rejected{0};
    std::uint64_t completed{0};
    std::uint64_t failed{0};
    std::uint64_t bytes_reserved{0};
    std::uint64_t reclaim_attempts{0};

    bool operator==(const workload_counters&) const = default;
};

enum class resource_manager_state {
    constructed,
    starting,
    started,
    stopping,
    stopped,
};

// Shard-local resource access. Work accepted by with_workload_class() holds the
// manager gate through asynchronous completion, so stop() drains every caller
// before global handles can be destroyed by the registry owner.
class resource_manager final : public runtime::shard_affine {
public:
    explicit resource_manager(resource_handle_set handles) noexcept;
    ~resource_manager();

    [[nodiscard]] seastar::future<> start();
    void request_abort();
    [[nodiscard]] seastar::future<> stop();

    [[nodiscard]] bool ready() const;
    [[nodiscard]] resource_manager_state state() const;
    [[nodiscard]] byte_count hard_budget(workload_class classification) const;
    [[nodiscard]] seastar::scheduling_group
    scheduling_group(workload_class classification) const;
    [[nodiscard]] unsigned
    smp_admission_limit(workload_class classification) const;
    [[nodiscard]] workload_counters
    counters(workload_class classification) const;
    [[nodiscard]] seastar::abort_source& abort_source();

    [[nodiscard]] result<void>
    reserve_bytes(workload_class classification, byte_count bytes);
    void release_bytes(workload_class classification, byte_count bytes);
    void record_reclaim_attempt(workload_class classification);

    // Bounds caller-side outstanding cross-shard work without adding semaphore
    // waiters. The callable receives the class's global service-group handle.
    // Calls using the same service group must not nest, and nested groups must
    // form an acyclic dependency graph.
    template<typename Func>
    requires detail::smp_group_invocable<Func>
    [[nodiscard]] auto
    try_with_smp_service_group(workload_class classification, Func&& function) {
        using function_type = std::remove_cvref_t<Func>;
        using value_type = smp_invocation_value<function_type>;
        using admission_result = runtime::result<value_type>;

        assert_current();
        const auto index = checked_index(classification);
        if (state_ != resource_manager_state::started) {
            increment(counters_[index].rejected);
            return seastar::make_exception_future<admission_result>(
              std::logic_error("resource manager is not ready"));
        }

        auto holder = work_.try_hold();
        if (!holder) {
            increment(counters_[index].rejected);
            return seastar::make_exception_future<admission_result>(
              std::logic_error("resource manager is stopping"));
        }

        KWAQUE_INVARIANT(
          invariant_id{"KQ-RESOURCE-SMP-ADMISSION-READY"},
          smp_admission_[index].has_value(),
          "started resource manager has no SMP admission controller");
        // Resolve the global handle before touching any counter so a stale
        // handle cannot leave this class permanently accounted.
        auto service_group = handles_.smp_service_group(classification);
        auto units = seastar::try_get_units(*smp_admission_[index], 1);
        if (!units) {
            increment(counters_[index].rejected);
            runtime::operation_error error{
              errc::resource_exhausted, runtime::operation_kind::resource};
            static_cast<void>(error.add_context(
              runtime::operation_context_key::items,
              descriptor_for(classification).max_nonlocal_requests));
            return seastar::make_ready_future<admission_result>(
              runtime::failure(std::move(error)));
        }

        auto owned_function = std::make_unique<function_type>(
          std::forward<Func>(function));
        auto invoked = invoke_smp_owned(
          std::move(owned_function), service_group);
        return std::move(invoked).then_wrapped(
          [holder = std::move(*holder), units = std::move(*units)](
            seastar::future<admission_result> completion) mutable
            -> seastar::future<admission_result> {
              static_cast<void>(holder);
              static_cast<void>(units);
              return std::move(completion);
          });
    }

    template<typename Func>
    requires std::constructible_from<std::remove_cvref_t<Func>, Func&&>
             && std::invocable<std::remove_cvref_t<Func>&>
    [[nodiscard]] auto
    with_workload_class(workload_class classification, Func&& function) {
        using function_type = std::remove_cvref_t<Func>;
        using result_type = std::invoke_result_t<function_type&>;
        using futurator = seastar::futurize<result_type>;
        using future_type = typename futurator::type;

        assert_current();
        const auto index = checked_index(classification);
        if (state_ != resource_manager_state::started) {
            increment(counters_[index].rejected);
            return futurator::make_exception_future(
              std::logic_error("resource manager is not ready"));
        }

        auto holder = work_.try_hold();
        if (!holder) {
            increment(counters_[index].rejected);
            return futurator::make_exception_future(
              std::logic_error("resource manager is stopping"));
        }

        // Resolve the global handle before touching any counter. It rejects a
        // stale registry generation by throwing, and a counter incremented
        // ahead of that throw would never be balanced by a completion.
        auto group = handles_.scheduling_group(classification);
        auto owned_function = std::make_unique<function_type>(
          std::forward<Func>(function));
        increment(counters_[index].queued);
        auto scheduled = seastar::with_scheduling_group(
          group,
          [this, index, owned_function = std::move(owned_function)]() mutable
            -> future_type {
              KWAQUE_INVARIANT(
                invariant_id{"KQ-RESOURCE-QUEUED-NONZERO"},
                counters_[index].queued != 0,
                "workload queued counter underflow");
              --counters_[index].queued;
              increment(counters_[index].executing);
              return invoke_owned(std::move(owned_function));
          });

        return std::move(scheduled).then_wrapped(
          [this, index, holder = std::move(*holder)](
            future_type completion) mutable -> future_type {
              static_cast<void>(holder);
              KWAQUE_INVARIANT(
                invariant_id{"KQ-RESOURCE-EXECUTING-NONZERO"},
                counters_[index].executing != 0,
                "workload executing counter underflow");
              --counters_[index].executing;
              if (completion.failed()) {
                  increment(counters_[index].failed);
              } else {
                  increment(counters_[index].completed);
              }
              return std::move(completion);
          });
    }

private:
    friend class resource_manager_test_access;

    template<typename Func>
    using invocation_value = typename detail::future_value<
      seastar::futurize_t<std::invoke_result_t<Func&>>>::type;

    template<typename Func>
    using smp_invocation_value =
      typename detail::future_value<seastar::futurize_t<
        std::invoke_result_t<Func&, seastar::smp_service_group>>>::type;

    template<typename Func>
    static seastar::future<invocation_value<Func>>
    invoke_owned(std::unique_ptr<Func> function) {
        if constexpr (std::same_as<invocation_value<Func>, void>) {
            co_await seastar::futurize_invoke([&function]() -> decltype(auto) {
                return std::invoke(*function);
            });
        } else {
            co_return co_await seastar::futurize_invoke(
              [&function]() -> decltype(auto) {
                  return std::invoke(*function);
              });
        }
    }

    template<typename Func>
    static seastar::future<runtime::result<smp_invocation_value<Func>>>
    invoke_smp_owned(
      std::unique_ptr<Func> function,
      seastar::smp_service_group service_group) {
        if constexpr (std::same_as<smp_invocation_value<Func>, void>) {
            co_await seastar::futurize_invoke(
              [&function, service_group]() -> decltype(auto) {
                  return std::invoke(*function, service_group);
              });
            co_return runtime::result<void>{};
        } else {
            co_return runtime::result<smp_invocation_value<Func>>{
              co_await seastar::futurize_invoke(
                [&function, service_group]() -> decltype(auto) {
                    return std::invoke(*function, service_group);
                })};
        }
    }

    static void increment(std::uint64_t& value);
    [[nodiscard]] static std::size_t
    checked_index(workload_class classification);
    void assert_ready() const;
    void register_metrics();
    void rollback_start();
    [[nodiscard]] seastar::future<> stop_once();

    resource_handle_set handles_;
    std::array<workload_counters, workload_class_count> counters_{};
    std::array<bool, workload_class_count> initialized_{};
    std::array<std::optional<seastar::semaphore>, workload_class_count>
      smp_admission_{};
    seastar::abort_source abort_source_;
    seastar::gate work_;
    seastar::metrics::metric_groups metrics_;
    seastar::shared_promise<> stop_done_;
    resource_manager_state state_{resource_manager_state::constructed};
    std::optional<std::size_t> fail_before_start_point_;
    bool registry_lease_acquired_{false};
};

} // namespace kwaque::resource
