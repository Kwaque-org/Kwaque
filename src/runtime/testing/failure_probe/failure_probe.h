#ifndef KWAQUE_SRC_RUNTIME_TESTING_FAILURE_PROBE_FAILURE_PROBE_H_
#define KWAQUE_SRC_RUNTIME_TESTING_FAILURE_PROBE_FAILURE_PROBE_H_

#include "src/runtime/fault.h"
#include "src/runtime/shard_affinity.h"
#include "src/runtime/timer.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <utility>

namespace kwaque::runtime::testing {

inline constexpr std::array logical_failure_points{
  builtin_fault_point::environment_start,
  builtin_fault_point::resource_group_create,
  builtin_fault_point::queue_admission,
  builtin_fault_point::environment_stop,
};

[[nodiscard]] constexpr std::optional<std::size_t>
failure_probe_point_index(builtin_fault_point point) noexcept {
    for (std::size_t index = 0; index < logical_failure_points.size();
         ++index) {
        if (logical_failure_points[index] == point) {
            return index;
        }
    }
    return std::nullopt;
}

static_assert(logical_failure_points.size() == 4U);
static_assert(
  failure_probe_point_index(builtin_fault_point::environment_start).value()
  == 0U);
static_assert(
  failure_probe_point_index(builtin_fault_point::resource_group_create).value()
  == 1U);
static_assert(
  failure_probe_point_index(builtin_fault_point::queue_admission).value()
  == 2U);
static_assert(
  failure_probe_point_index(builtin_fault_point::environment_stop).value()
  == 3U);
static_assert(!failure_probe_point_index(builtin_fault_point::timer));

class failure_probe;
class failure_probe_test_access;

// A complete probe result is bounded data. The optional deadline is present
// only for a validated delay action and is computed before the occurrence is
// committed.
class failure_evaluation final {
public:
    [[nodiscard]] owner_shard owner() const noexcept { return owner_; }
    void assert_current() const { owner_.assert_current(); }
    [[nodiscard]] constexpr const fault_request& request() const noexcept {
        return request_;
    }
    [[nodiscard]] constexpr fault_decision decision() const noexcept {
        return decision_;
    }
    [[nodiscard]] constexpr std::optional<monotonic_time>
    deadline() const noexcept {
        return deadline_;
    }

    bool operator==(const failure_evaluation&) const = default;

private:
    friend class failure_probe;

    failure_evaluation(
      owner_shard owner,
      fault_request request,
      fault_decision decision,
      std::optional<monotonic_time> deadline) noexcept
      : owner_(owner)
      , request_(request)
      , decision_(decision)
      , deadline_(deadline) {}

    owner_shard owner_;
    fault_request request_;
    fault_decision decision_;
    std::optional<monotonic_time> deadline_;
};

// Owns only four fixed occurrence counters. The injector is supplied to each
// evaluation and is never stored, and no callback or action implementation can
// enter the probe's state.
class failure_probe final : public shard_affine {
public:
    failure_probe() noexcept = default;

    failure_probe(const failure_probe&) = delete;
    failure_probe& operator=(const failure_probe&) = delete;
    failure_probe(failure_probe&&) = delete;
    failure_probe& operator=(failure_probe&&) = delete;

    template<fault_injector Injector>
    [[nodiscard]] result<failure_evaluation> evaluate(
      Injector& injector,
      builtin_fault_point point,
      monotonic_time now,
      monotonic_time maximum_deadline,
      fault_object_key object = fault_object_key::none()) noexcept {
        assert_current();
        if (now > maximum_deadline) {
            return failure(probe_error(errc::invalid_argument));
        }
        auto candidate = prepare(point, object);
        if (!candidate) {
            return failure(candidate.error());
        }
        auto decision = injector.evaluate(candidate->request);
        if (!decision) {
            return failure(decision.error());
        }
        if (
          auto valid = validate_fault_decision(candidate->request, *decision);
          !valid) {
            return failure(valid.error());
        }

        std::optional<monotonic_time> deadline;
        switch (decision->action()) {
        case fault_action::none:
        case fault_action::error:
            break;
        case fault_action::delay:
            deadline = now.checked_add(*decision->delay());
            if (!deadline || *deadline > maximum_deadline) {
                return failure(probe_error(errc::out_of_range));
            }
            break;
        default:
            return failure(probe_error(errc::invalid_argument));
        }

        if (auto committed = commit(*candidate); !committed) {
            return failure(committed.error());
        }
        return failure_evaluation{
          owner(), candidate->request, *decision, std::move(deadline)};
    }

    [[nodiscard]] result<std::uint64_t>
    occurrences(builtin_fault_point point) const noexcept;

private:
    friend class failure_probe_test_access;

    struct occurrence_candidate final {
        fault_request request;
        std::size_t index;
        std::uint64_t previous;
    };

    [[nodiscard]] static operation_error probe_error(errc code) noexcept;
    [[nodiscard]] result<occurrence_candidate>
    prepare(builtin_fault_point point, fault_object_key object) const noexcept;
    [[nodiscard]] result<void>
    commit(const occurrence_candidate& candidate) noexcept;

    std::array<std::uint64_t, logical_failure_points.size()> occurrences_{};
};

template<timer_service Timer>
[[nodiscard]] seastar::future<result<void>> apply_failure_evaluation(
  const failure_evaluation& evaluation,
  Timer& timer,
  seastar::abort_source& abort_source) {
    evaluation.assert_current();
    switch (evaluation.decision().action()) {
    case fault_action::none:
        return seastar::make_ready_future<result<void>>(result<void>{});
    case fault_action::error: {
        operation_error error{errc::fault_injected, operation_kind::fault};
        static_cast<void>(error.add_context(
          operation_context_key::detail, evaluation.request().point.value()));
        static_cast<void>(error.add_context(
          operation_context_key::occurrence,
          evaluation.request().occurrence.value()));
        return seastar::make_ready_future<result<void>>(
          runtime::failure(std::move(error)));
    }
    case fault_action::delay:
        if (!evaluation.deadline()) {
            return seastar::make_ready_future<result<void>>(runtime::failure(
              operation_error{
                errc::invariant_violation, operation_kind::fault}));
        }
        return timer.sleep_until(*evaluation.deadline(), abort_source);
    default:
        return seastar::make_ready_future<result<void>>(runtime::failure(
          operation_error{errc::invalid_argument, operation_kind::fault}));
    }
}

} // namespace kwaque::runtime::testing

#endif // KWAQUE_SRC_RUNTIME_TESTING_FAILURE_PROBE_FAILURE_PROBE_H_
