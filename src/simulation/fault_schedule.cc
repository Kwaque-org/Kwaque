#include "src/simulation/fault_schedule.h"

#include "src/base/invariant.h"

#include <algorithm>
#include <limits>
#include <utility>

namespace kwaque::simulation {

namespace {

constexpr std::uint32_t selector_applied = UINT32_C(1);
constexpr std::uint32_t selector_skipped = UINT32_C(2);
constexpr invariant_id fault_coordinate_invariant{"KQ-FAULT-COORDINATE"};

[[nodiscard]] runtime::operation_error fault_error(errc code) noexcept {
    return runtime::operation_error{code, runtime::operation_kind::fault};
}

[[nodiscard]] bool object_less(
  const std::optional<runtime::fault_object_key>& left,
  const std::optional<runtime::fault_object_key>& right) noexcept {
    if (!left || !right) {
        return !left && right.has_value();
    }
    return *left < *right;
}

[[nodiscard]] bool
canonical_rule_less(const fault_rule& left, const fault_rule& right) noexcept {
    if (left.point() != right.point()) {
        return left.point() < right.point();
    }
    if (left.object() != right.object()) {
        return object_less(left.object(), right.object());
    }
    if (left.first() != right.first()) {
        return left.first() < right.first();
    }
    if (left.last() != right.last()) {
        return left.last() < right.last();
    }
    return left.id() < right.id();
}

[[nodiscard]] bool
intervals_overlap(const fault_rule& left, const fault_rule& right) noexcept {
    return left.first() <= right.last() && right.first() <= left.last();
}

} // namespace

runtime::result<fault_rule_id>
fault_rule_id::make(std::uint64_t value) noexcept {
    if (value == 0) {
        return runtime::failure(fault_error(errc::invalid_argument));
    }
    return fault_rule_id{value};
}

runtime::result<fault_selector>
fault_selector::every_n(std::uint64_t period) noexcept {
    if (period == 0) {
        return runtime::failure(fault_error(errc::invalid_argument));
    }
    return fault_selector{fault_selector_kind::every_n, period, 0, 1};
}

runtime::result<fault_rule> fault_rule::make(
  fault_rule_id id,
  runtime::builtin_fault_point point,
  std::optional<runtime::fault_object_key> object,
  runtime::fault_occurrence first,
  runtime::fault_occurrence last,
  fault_selector selector,
  runtime::fault_decision decision) noexcept {
    const auto* descriptor = runtime::descriptor_for(point);
    if (
      descriptor == nullptr || first > last
      || decision.action() == runtime::fault_action::none
      || !descriptor->permitted_actions.contains(decision.action())
      || (selector.kind() == fault_selector_kind::once && first != last)
      || (selector.kind() == fault_selector_kind::every_n
          && selector.period() == 0)
      || (selector.kind() == fault_selector_kind::rational
          && (selector.denominator() == 0
              || selector.numerator() > selector.denominator()))) {
        return runtime::failure(fault_error(errc::invalid_argument));
    }
    const runtime::fault_request request{
      .point = descriptor->id,
      .occurrence = first,
      .object = object.value_or(runtime::fault_object_key::none()),
    };
    if (
      auto valid = runtime::validate_fault_decision(request, decision);
      !valid) {
        return runtime::failure(valid.error());
    }
    return fault_rule{id, point, object, first, last, selector, decision};
}

runtime::result<fault_schedule_limits>
fault_schedule_limits::make(std::uint32_t rules) noexcept {
    if (rules == 0) {
        return runtime::failure(fault_error(errc::invalid_argument));
    }
    if (rules > maximum_fault_schedule_rules) {
        return runtime::failure(fault_error(errc::out_of_range));
    }
    return fault_schedule_limits{rules};
}

prepared_fault_evaluation::prepared_fault_evaluation(
  fault_schedule& owner,
  runtime::fault_decision decision,
  bool applied,
  trace_entry entry,
  event_trace::reservation reservation,
  std::uint64_t master_seed,
  std::uint64_t next_draw) noexcept
  : owner_(&owner)
  , decision_(decision)
  , entry_(entry)
  , reservation_(std::move(reservation))
  , matched_(true)
  , applied_(applied)
  , master_seed_(master_seed)
  , rule_id_(entry.stable_id)
  , occurrence_(entry.coordinate_a)
  , next_draw_(next_draw) {}

prepared_fault_evaluation::prepared_fault_evaluation(
  prepared_fault_evaluation&& other) noexcept
  : owner_(std::exchange(other.owner_, nullptr))
  , decision_(other.decision_)
  , entry_(other.entry_)
  , reservation_(std::move(other.reservation_))
  , matched_(std::exchange(other.matched_, false))
  , applied_(std::exchange(other.applied_, false))
  , committed_(std::exchange(other.committed_, true))
  , master_seed_(other.master_seed_)
  , rule_id_(other.rule_id_)
  , occurrence_(other.occurrence_)
  , next_draw_(other.next_draw_) {}

prepared_fault_evaluation& prepared_fault_evaluation::operator=(
  prepared_fault_evaluation&& other) noexcept {
    if (this != &other) {
        owner_ = std::exchange(other.owner_, nullptr);
        decision_ = other.decision_;
        entry_ = other.entry_;
        reservation_ = std::move(other.reservation_);
        matched_ = std::exchange(other.matched_, false);
        applied_ = std::exchange(other.applied_, false);
        committed_ = std::exchange(other.committed_, true);
        master_seed_ = other.master_seed_;
        rule_id_ = other.rule_id_;
        occurrence_ = other.occurrence_;
        next_draw_ = other.next_draw_;
    }
    return *this;
}

runtime::result<std::uint64_t> prepared_fault_evaluation::draw_bounded(
  std::uint64_t upper_exclusive) noexcept {
    if (!matched_ || committed_ || upper_exclusive == 0) {
        return runtime::failure(fault_error(errc::invalid_argument));
    }
    const auto coordinate = random_coordinate::make(
      random_domain::fault_decision, rule_id_, occurrence_);
    KWAQUE_INVARIANT(
      fault_coordinate_invariant,
      coordinate.has_value(),
      "prepared fault evaluation lost its random coordinate");
    deterministic_random random{master_seed_};
    auto cursor = random.cursor(*coordinate, next_draw_);
    auto selected = runtime::uniform_u64(cursor, upper_exclusive);
    if (!selected) {
        return runtime::failure(selected.error());
    }
    next_draw_ = cursor.draw_index();
    return selected;
}

runtime::result<runtime::fault_decision>
prepared_fault_evaluation::commit() noexcept {
    if (committed_) {
        return runtime::failure(fault_error(errc::invalid_argument));
    }
    committed_ = true;
    if (!matched_) {
        return runtime::fault_decision{};
    }
    if (auto observed = reservation_.observe(entry_); !observed) {
        return runtime::failure(observed.error());
    }
    if (applied_) {
        ++owner_->applied_decisions_;
    }
    return preview();
}

fault_schedule::fault_schedule(
  scheduler& event_scheduler,
  event_trace& trace,
  std::uint64_t master_seed,
  seastar::chunked_vector<fault_rule> rules,
  seastar::chunked_vector<rule_group> groups,
  std::array<group_range, runtime::builtin_fault_points.size()> ranges) noexcept
  : scheduler_(&event_scheduler)
  , trace_(&trace)
  , random_(master_seed)
  , rules_(std::move(rules))
  , groups_(std::move(groups))
  , ranges_(ranges) {}

runtime::result<std::unique_ptr<fault_schedule>> fault_schedule::make(
  scheduler& event_scheduler,
  event_trace& trace,
  std::uint64_t master_seed,
  seastar::chunked_vector<fault_rule> rules,
  fault_schedule_limits limits) {
    event_scheduler.assert_current();
    if (
      rules.size() > limits.rules()
      || rules.size() > maximum_fault_schedule_rules) {
        return runtime::failure(fault_error(errc::out_of_range));
    }
    if (
      trace.header().master_seed != master_seed
      || !event_scheduler.uses_trace(trace)) {
        return runtime::failure(fault_error(errc::invalid_argument));
    }

    seastar::chunked_vector<std::uint64_t> ids;
    ids.reserve(rules.size());
    for (const auto& rule : rules) {
        ids.push_back(rule.id().value());
    }
    std::sort(ids.begin(), ids.end());
    if (std::adjacent_find(ids.begin(), ids.end()) != ids.end()) {
        return runtime::failure(fault_error(errc::invalid_argument));
    }

    std::sort(rules.begin(), rules.end(), canonical_rule_less);
    seastar::chunked_vector<rule_group> groups;
    groups.reserve(rules.size());
    for (std::size_t begin = 0; begin < rules.size();) {
        std::size_t end = begin + 1U;
        while (end < rules.size() && rules[end].point() == rules[begin].point()
               && rules[end].object() == rules[begin].object()) {
            ++end;
        }
        for (std::size_t index = begin + 1U; index < end; ++index) {
            if (intervals_overlap(rules[index - 1U], rules[index])) {
                return runtime::failure(fault_error(errc::invalid_argument));
            }
        }
        groups.push_back(
          rule_group{
            .point = rules[begin].point(),
            .object = rules[begin].object(),
            .begin = begin,
            .end = end,
          });
        begin = end;
    }

    std::array<group_range, runtime::builtin_fault_points.size()> ranges{};
    std::size_t group_index = 0;
    for (std::size_t point_index = 0; point_index < ranges.size();
         ++point_index) {
        const auto point = static_cast<runtime::builtin_fault_point>(
          point_index);
        const auto begin = group_index;
        while (group_index < groups.size()
               && groups[group_index].point == point) {
            ++group_index;
        }
        ranges[point_index] = group_range{.begin = begin, .end = group_index};

        if (begin == group_index || groups[begin].object.has_value()) {
            continue;
        }
        const auto& wildcard = groups[begin];
        for (std::size_t exact_index = begin + 1U; exact_index < group_index;
             ++exact_index) {
            const auto& exact = groups[exact_index];
            std::size_t wildcard_rule = wildcard.begin;
            std::size_t exact_rule = exact.begin;
            while (wildcard_rule < wildcard.end && exact_rule < exact.end) {
                if (
                  intervals_overlap(rules[wildcard_rule], rules[exact_rule])) {
                    return runtime::failure(
                      fault_error(errc::invalid_argument));
                }
                if (rules[wildcard_rule].last() < rules[exact_rule].first()) {
                    ++wildcard_rule;
                } else {
                    ++exact_rule;
                }
            }
        }
    }

    return std::unique_ptr<fault_schedule>{new fault_schedule{
      event_scheduler,
      trace,
      master_seed,
      std::move(rules),
      std::move(groups),
      ranges}};
}

const fault_schedule::rule_group* fault_schedule::find_group(
  runtime::builtin_fault_point point,
  const std::optional<runtime::fault_object_key>& object) const noexcept {
    const auto point_index = static_cast<std::size_t>(point);
    if (point_index >= ranges_.size()) {
        return nullptr;
    }
    const auto range = ranges_[point_index];
    const auto first = groups_.begin()
                       + static_cast<std::ptrdiff_t>(range.begin);
    const auto last = groups_.begin() + static_cast<std::ptrdiff_t>(range.end);
    const auto found = std::lower_bound(
      first,
      last,
      object,
      [](
        const rule_group& group,
        const std::optional<runtime::fault_object_key>& key) {
          return object_less(group.object, key);
      });
    return found != last && found->object == object ? &*found : nullptr;
}

const fault_rule* fault_schedule::find_occurrence(
  const rule_group& group,
  runtime::fault_occurrence occurrence) const noexcept {
    const auto first = rules_.begin()
                       + static_cast<std::ptrdiff_t>(group.begin);
    const auto last = rules_.begin() + static_cast<std::ptrdiff_t>(group.end);
    const auto after = std::upper_bound(
      first,
      last,
      occurrence,
      [](runtime::fault_occurrence value, const fault_rule& rule) {
          return value < rule.first();
      });
    if (after == first) {
        return nullptr;
    }
    const auto candidate = std::prev(after);
    return occurrence <= candidate->last() ? &*candidate : nullptr;
}

const fault_rule* fault_schedule::find_rule(
  const runtime::fault_request& request,
  runtime::builtin_fault_point point) const noexcept {
    const std::optional<runtime::fault_object_key> exact{request.object};
    if (const auto* group = find_group(point, exact)) {
        if (const auto* rule = find_occurrence(*group, request.occurrence)) {
            return rule;
        }
    }
    if (const auto* group = find_group(point, std::nullopt)) {
        return find_occurrence(*group, request.occurrence);
    }
    return nullptr;
}

fault_schedule::selector_result fault_schedule::select(
  const fault_rule& rule, runtime::fault_occurrence occurrence) const noexcept {
    switch (rule.selector().kind()) {
    case fault_selector_kind::once:
    case fault_selector_kind::bounded_range:
        return selector_result{.applied = true};
    case fault_selector_kind::every_n:
        return selector_result{
          .applied = (occurrence.value() - rule.first().value())
                       % rule.selector().period()
                     == 0};
    case fault_selector_kind::rational:
        if (rule.selector().numerator() == 0) {
            return selector_result{.applied = false};
        }
        if (rule.selector().numerator() == rule.selector().denominator()) {
            return selector_result{.applied = true};
        }
        const auto coordinate = random_coordinate::make(
          random_domain::fault_decision, rule.id().value(), occurrence.value());
        KWAQUE_INVARIANT(
          fault_coordinate_invariant,
          coordinate.has_value(),
          "validated fault rule produced an invalid random coordinate");
        auto cursor = random_.cursor(*coordinate, 0);
        const auto sample = runtime::uniform_u64(
          cursor, rule.selector().denominator());
        KWAQUE_INVARIANT(
          fault_coordinate_invariant,
          sample.has_value(),
          "validated fault ratio produced an invalid random bound");
        return selector_result{
          .sample = *sample,
          .draws = cursor.draw_index(),
          .applied = *sample < rule.selector().numerator(),
        };
    }
    return {};
}

runtime::result<prepared_fault_evaluation>
fault_schedule::prepare(const runtime::fault_request& request) noexcept {
    assert_current();
    const auto descriptor = runtime::validate_fault_request(request);
    if (!descriptor) {
        return runtime::failure(descriptor.error());
    }
    const auto* rule = find_rule(request, (**descriptor).point);
    const auto selected = rule != nullptr ? select(*rule, request.occurrence)
                                          : selector_result{};
    if (
      rule != nullptr && selected.applied
      && rule->decision().action() == runtime::fault_action::delay) {
        const auto deadline = scheduler_->now().checked_add(
          *rule->decision().delay());
        if (!deadline || *deadline > scheduler_->limits().maximum_deadline()) {
            auto error = fault_error(errc::out_of_range);
            static_cast<void>(error.add_context(
              runtime::operation_context_key::limit,
              scheduler_->limits().maximum_deadline().nanoseconds()));
            return runtime::failure(std::move(error));
        }
    }
    ++evaluations_;
    if (rule == nullptr) {
        return prepared_fault_evaluation{};
    }
    auto reservation = trace_->reserve(1, canonical_entry_encoded_size);
    if (!reservation) {
        return runtime::failure(reservation.error());
    }
    const auto outcome = selected.applied ? selector_applied : selector_skipped;
    return prepared_fault_evaluation{
      *this,
      rule->decision(),
      selected.applied,
      trace_entry{
        .time = scheduler_->now(),
        .action = trace_action::fault_evaluated,
        .kind = trace_event_kind::fault,
        .domain = request.point.value(),
        .stable_id = rule->id().value(),
        .coordinate_a = request.occurrence.value(),
        .coordinate_b = selected.draws,
        .value = selected.sample,
        .result = static_cast<std::uint32_t>(rule->decision().action())
                  | (outcome << 8U),
      },
      std::move(*reservation),
      random_.master_seed(),
      selected.draws};
}

runtime::result<runtime::fault_decision>
fault_schedule::evaluate(const runtime::fault_request& request) noexcept {
    auto prepared = prepare(request);
    if (!prepared) {
        return runtime::failure(prepared.error());
    }
    return prepared->commit();
}

} // namespace kwaque::simulation
