#ifndef KWAQUE_SRC_SIMULATION_FAULT_SCHEDULE_H_
#define KWAQUE_SRC_SIMULATION_FAULT_SCHEDULE_H_

#include "src/runtime/fault.h"
#include "src/runtime/random.h"
#include "src/runtime/shard_affinity.h"
#include "src/simulation/deterministic_random.h"
#include "src/simulation/event_trace.h"
#include "src/simulation/scheduler.h"

#include <seastar/core/chunked_vector.hh>

#include <array>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>

namespace kwaque::simulation {

class fault_schedule;

inline constexpr std::uint32_t default_fault_schedule_rules{4'096};
inline constexpr std::uint32_t maximum_fault_schedule_rules{65'536};
inline constexpr std::uint64_t fault_trace_no_sample{
  std::numeric_limits<std::uint64_t>::max()};

class fault_rule_id final {
public:
    [[nodiscard]] static runtime::result<fault_rule_id>
    make(std::uint64_t value) noexcept;

    [[nodiscard]] constexpr std::uint64_t value() const noexcept {
        return value_;
    }

    auto operator<=>(const fault_rule_id&) const = default;

private:
    constexpr explicit fault_rule_id(std::uint64_t value) noexcept
      : value_(value) {}

    std::uint64_t value_;
};

enum class fault_selector_kind : std::uint8_t {
    once,
    bounded_range,
    every_n,
    rational,
};

class fault_selector final {
public:
    [[nodiscard]] static constexpr fault_selector once() noexcept {
        return fault_selector{fault_selector_kind::once, 0, 0, 1};
    }
    [[nodiscard]] static constexpr fault_selector bounded_range() noexcept {
        return fault_selector{fault_selector_kind::bounded_range, 0, 0, 1};
    }
    [[nodiscard]] static runtime::result<fault_selector>
    every_n(std::uint64_t period) noexcept;
    [[nodiscard]] static constexpr fault_selector
    rational(runtime::probability_ratio probability) noexcept {
        return fault_selector{
          fault_selector_kind::rational,
          0,
          probability.numerator(),
          probability.denominator()};
    }

    [[nodiscard]] constexpr fault_selector_kind kind() const noexcept {
        return kind_;
    }
    [[nodiscard]] constexpr std::uint64_t period() const noexcept {
        return period_;
    }
    [[nodiscard]] constexpr std::uint64_t numerator() const noexcept {
        return numerator_;
    }
    [[nodiscard]] constexpr std::uint64_t denominator() const noexcept {
        return denominator_;
    }

    bool operator==(const fault_selector&) const = default;

private:
    constexpr fault_selector(
      fault_selector_kind kind,
      std::uint64_t period,
      std::uint64_t numerator,
      std::uint64_t denominator) noexcept
      : kind_(kind)
      , period_(period)
      , numerator_(numerator)
      , denominator_(denominator) {}

    fault_selector_kind kind_;
    std::uint64_t period_;
    std::uint64_t numerator_;
    std::uint64_t denominator_;
};

class fault_rule final {
public:
    [[nodiscard]] static runtime::result<fault_rule> make(
      fault_rule_id id,
      runtime::builtin_fault_point point,
      std::optional<runtime::fault_object_key> object,
      runtime::fault_occurrence first,
      runtime::fault_occurrence last,
      fault_selector selector,
      runtime::fault_decision decision) noexcept;

    [[nodiscard]] constexpr fault_rule_id id() const noexcept { return id_; }
    [[nodiscard]] constexpr runtime::builtin_fault_point
    point() const noexcept {
        return point_;
    }
    [[nodiscard]] constexpr const std::optional<runtime::fault_object_key>&
    object() const noexcept {
        return object_;
    }
    [[nodiscard]] constexpr runtime::fault_occurrence first() const noexcept {
        return first_;
    }
    [[nodiscard]] constexpr runtime::fault_occurrence last() const noexcept {
        return last_;
    }
    [[nodiscard]] constexpr const fault_selector& selector() const noexcept {
        return selector_;
    }
    [[nodiscard]] constexpr runtime::fault_decision decision() const noexcept {
        return decision_;
    }

    bool operator==(const fault_rule&) const = default;

private:
    constexpr fault_rule(
      fault_rule_id id,
      runtime::builtin_fault_point point,
      std::optional<runtime::fault_object_key> object,
      runtime::fault_occurrence first,
      runtime::fault_occurrence last,
      fault_selector selector,
      runtime::fault_decision decision) noexcept
      : id_(id)
      , point_(point)
      , object_(object)
      , first_(first)
      , last_(last)
      , selector_(selector)
      , decision_(decision) {}

    fault_rule_id id_;
    runtime::builtin_fault_point point_;
    std::optional<runtime::fault_object_key> object_;
    runtime::fault_occurrence first_;
    runtime::fault_occurrence last_;
    fault_selector selector_;
    runtime::fault_decision decision_;
};

class fault_schedule_limits final {
public:
    [[nodiscard]] static runtime::result<fault_schedule_limits>
    make(std::uint32_t rules) noexcept;
    [[nodiscard]] static constexpr fault_schedule_limits defaults() noexcept {
        return fault_schedule_limits{default_fault_schedule_rules};
    }

    [[nodiscard]] constexpr std::uint32_t rules() const noexcept {
        return rules_;
    }

private:
    constexpr explicit fault_schedule_limits(std::uint32_t rules) noexcept
      : rules_(rules) {}

    std::uint32_t rules_;
};

class prepared_fault_evaluation final {
public:
    prepared_fault_evaluation(const prepared_fault_evaluation&) = delete;
    prepared_fault_evaluation&
    operator=(const prepared_fault_evaluation&) = delete;
    prepared_fault_evaluation(prepared_fault_evaluation&& other) noexcept;
    prepared_fault_evaluation&
    operator=(prepared_fault_evaluation&& other) noexcept;
    ~prepared_fault_evaluation() = default;

    [[nodiscard]] bool matched() const noexcept { return matched_; }
    [[nodiscard]] bool applied() const noexcept { return applied_; }
    [[nodiscard]] runtime::fault_decision preview() const noexcept {
        return applied_ ? decision_ : runtime::fault_decision{};
    }
    [[nodiscard]] runtime::fault_decision configured() const noexcept {
        return matched_ ? decision_ : runtime::fault_decision{};
    }
    [[nodiscard]] runtime::result<std::uint64_t>
    draw_bounded(std::uint64_t upper_exclusive) noexcept;
    [[nodiscard]] std::uint64_t draws_consumed() const noexcept {
        return next_draw_;
    }
    [[nodiscard]] runtime::result<runtime::fault_decision> commit() noexcept;

private:
    friend class fault_schedule;

    prepared_fault_evaluation() noexcept = default;
    prepared_fault_evaluation(
      fault_schedule& owner,
      runtime::fault_decision decision,
      bool applied,
      trace_entry entry,
      event_trace::reservation reservation,
      std::uint64_t master_seed,
      std::uint64_t next_draw) noexcept;

    fault_schedule* owner_{nullptr};
    runtime::fault_decision decision_{};
    trace_entry entry_{};
    event_trace::reservation reservation_{};
    bool matched_{false};
    bool applied_{false};
    bool committed_{false};
    std::uint64_t master_seed_{0};
    std::uint64_t rule_id_{0};
    std::uint64_t occurrence_{0};
    std::uint64_t next_draw_{0};
};

class fault_schedule final : public runtime::shard_affine {
public:
    [[nodiscard]] static runtime::result<std::unique_ptr<fault_schedule>> make(
      scheduler& event_scheduler,
      event_trace& trace,
      std::uint64_t master_seed,
      seastar::chunked_vector<fault_rule> rules,
      fault_schedule_limits limits = fault_schedule_limits::defaults());

    fault_schedule(const fault_schedule&) = delete;
    fault_schedule& operator=(const fault_schedule&) = delete;
    fault_schedule(fault_schedule&&) = delete;
    fault_schedule& operator=(fault_schedule&&) = delete;
    ~fault_schedule() = default;

    [[nodiscard]] runtime::result<prepared_fault_evaluation>
    prepare(const runtime::fault_request& request) noexcept;
    [[nodiscard]] runtime::result<prepared_fault_evaluation> prepare(
      const runtime::fault_request& request,
      runtime::monotonic_time now,
      runtime::monotonic_time maximum_deadline) noexcept;
    [[nodiscard]] runtime::result<runtime::fault_decision>
    evaluate(const runtime::fault_request& request) noexcept;

    [[nodiscard]] const seastar::chunked_vector<fault_rule>& rules() const {
        assert_current();
        return rules_;
    }
    [[nodiscard]] std::uint64_t master_seed() const noexcept {
        assert_current();
        return random_.master_seed();
    }
    [[nodiscard]] std::uint64_t evaluations() const noexcept {
        assert_current();
        return evaluations_;
    }
    [[nodiscard]] std::uint64_t applied_decisions() const noexcept {
        assert_current();
        return applied_decisions_;
    }

private:
    friend class prepared_fault_evaluation;

    struct rule_group final {
        runtime::builtin_fault_point point;
        std::optional<runtime::fault_object_key> object;
        std::size_t begin;
        std::size_t end;
    };

    struct group_range final {
        std::size_t begin{0};
        std::size_t end{0};
    };

    struct selector_result final {
        std::uint64_t sample{fault_trace_no_sample};
        std::uint64_t draws{0};
        bool applied{false};
    };

    fault_schedule(
      scheduler& event_scheduler,
      event_trace& trace,
      std::uint64_t master_seed,
      seastar::chunked_vector<fault_rule> rules,
      seastar::chunked_vector<rule_group> groups,
      std::array<group_range, runtime::builtin_fault_points.size()>
        ranges) noexcept;

    [[nodiscard]] const fault_rule* find_rule(
      const runtime::fault_request& request,
      runtime::builtin_fault_point point) const noexcept;
    [[nodiscard]] const rule_group* find_group(
      runtime::builtin_fault_point point,
      const std::optional<runtime::fault_object_key>& object) const noexcept;
    [[nodiscard]] const fault_rule* find_occurrence(
      const rule_group& group,
      runtime::fault_occurrence occurrence) const noexcept;
    [[nodiscard]] selector_result select(
      const fault_rule& rule,
      runtime::fault_occurrence occurrence) const noexcept;

    scheduler* scheduler_;
    event_trace* trace_;
    deterministic_random random_;
    seastar::chunked_vector<fault_rule> rules_;
    seastar::chunked_vector<rule_group> groups_;
    std::array<group_range, runtime::builtin_fault_points.size()> ranges_;
    std::uint64_t evaluations_{0};
    std::uint64_t applied_decisions_{0};
};

static_assert(runtime::fault_injector<fault_schedule>);

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_FAULT_SCHEDULE_H_
