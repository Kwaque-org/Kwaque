#include "src/simulation/tests/fuzz_cases.h"

#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/observability/event.h"
#include "src/observability/event_log.h"
#include "src/resource/workload_class.h"
#include "src/runtime/fault.h"
#include "src/runtime/file.h"
#include "src/runtime/network.h"
#include "src/runtime/testing/seastar_fuzz.h"
#include "src/simulation/bandwidth.h"
#include "src/simulation/determinism_version.h"
#include "src/simulation/deterministic_random.h"
#include "src/simulation/event_sink.h"
#include "src/simulation/fake_file.h"
#include "src/simulation/fake_file_test_support.h"
#include "src/simulation/fake_network.h"
#include "src/simulation/fault_schedule.h"
#include "src/simulation/scheduler.h"
#include "src/simulation/sha256.h"
#include "src/simulation/tests/fake_file_model.h"
#include "src/simulation/tests/fuzz_network_cases.h"
#include "src/simulation/tests/network_oracle.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_vector.hh>
#include <seastar/core/future.hh>
#include <seastar/core/thread.hh>

#include <openssl/crypto.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

namespace kwaque::simulation::testing {

namespace {

constexpr std::uint32_t file_commands_max{32};
constexpr std::uint64_t fake_file_capacity{64U * 1'024U};

[[nodiscard]] runtime::operation_error case_error(errc code) noexcept {
    return runtime::operation_error{code, runtime::operation_kind::runtime};
}

[[nodiscard]] fuzz_case_outcome success() noexcept { return {}; }

[[nodiscard]] fuzz_case_outcome
mismatch(runtime::operation_kind operation) noexcept {
    return {.code = errc::invariant_violation, .operation = operation};
}

class input_cursor final {
public:
    explicit input_cursor(std::span<const std::uint8_t> input) noexcept
      : input_(input)
      , position_(std::min<std::size_t>(sizeof(std::uint64_t), input.size())) {}

    [[nodiscard]] bool empty() const noexcept {
        return position_ >= input_.size();
    }

    [[nodiscard]] std::size_t remaining() const noexcept {
        return input_.size() - position_;
    }

    [[nodiscard]] std::uint8_t byte() noexcept {
        return empty() ? 0 : input_[position_++];
    }

    [[nodiscard]] std::uint16_t u16() noexcept {
        std::uint16_t value = byte();
        value |= static_cast<std::uint16_t>(byte()) << 8U;
        return value;
    }

    [[nodiscard]] std::uint64_t
    bounded(std::uint64_t upper_exclusive) noexcept {
        return upper_exclusive == 0 ? 0 : u16() % upper_exclusive;
    }

private:
    std::span<const std::uint8_t> input_;
    std::size_t position_;
};

[[nodiscard]] scheduler_limits case_scheduler_limits(fuzz_harness harness) {
    const auto budget = fuzz_scheduler_budget(harness);
    auto made = scheduler_limits::make(
      scheduler_limit_values{
        .pending_events = budget.pending_events,
        .events_per_pump = budget.events_per_pump,
        .total_events = budget.total_events,
        .maximum_deadline = runtime::monotonic_time{budget.maximum_deadline},
      });
    if (!made) {
        throw std::logic_error("invalid fuzz scheduler limits");
    }
    return *made;
}

[[nodiscard]] trace_limits case_trace_limits(fuzz_harness harness) {
    auto made = trace_limits::make(fuzz_trace_budget(harness));
    if (!made) {
        throw std::logic_error("invalid fuzz trace limits");
    }
    return *made;
}

[[nodiscard]] observability::event_log_limits case_event_limits() {
    auto made = observability::event_log_limits::make(
      observability::event_log_limit_values{
        .entries = fuzz_event_entries_max,
        .encoded_bytes = fuzz_artifact_bytes_max,
      });
    if (!made) {
        throw std::logic_error("invalid fuzz event limits");
    }
    return *made;
}

[[nodiscard]] observability::event_sink_identity
case_identity(fuzz_harness harness, const fuzz_digest& configuration_digest) {
    auto epoch = observability::event_sink_epoch::make(
      fuzz_event_epoch(harness));
    if (!epoch) {
        throw std::logic_error("invalid fuzz event epoch");
    }
    return {.epoch = *epoch, .configuration_digest = configuration_digest};
}

[[nodiscard]] trace_header
case_trace_header(fuzz_harness harness, std::span<const std::uint8_t> input) {
    return trace_header::current(
      fuzz_master_seed(input),
      deterministic_random_algorithm_version,
      deterministic_random_coordinate_version,
      fuzz_scheduler_budget(harness),
      case_trace_limits(harness),
      digest_bytes(fuzz_configuration(harness)),
      digest_bytes(input));
}

[[nodiscard]] observability::event_public_text
case_operation(fuzz_harness harness) noexcept {
    using observability::event_public_text;
    switch (harness) {
    case fuzz_harness::scheduler:
        return event_public_text::operation_queue_admission;
    case fuzz_harness::fault_schedule:
        return event_public_text::operation_fault_evaluate;
    case fuzz_harness::fake_file:
        return event_public_text::operation_file_write;
    case fuzz_harness::fake_network:
        return event_public_text::operation_network_delivery;
    case fuzz_harness::semantic_canary:
        return event_public_text::operation_fault_evaluate;
    }
    return event_public_text::operation_fault_evaluate;
}

class terminal_hasher final {
public:
    template<typename Integer>
    void integer(Integer value) {
        using unsigned_type = std::make_unsigned_t<Integer>;
        static_assert(sizeof(Integer) <= sizeof(std::uint64_t));
        auto encoded = static_cast<std::uint64_t>(
          static_cast<unsigned_type>(value));
        std::array<std::uint8_t, sizeof(Integer)> bytes{};
        for (auto& byte : bytes) {
            byte = static_cast<std::uint8_t>(encoded & 0xffU);
            encoded >>= 8U;
        }
        hasher_.update(bytes.data(), bytes.size());
    }

    void bytes(std::span<const std::uint8_t> value) {
        hasher_.update(value.data(), value.size());
    }

    [[nodiscard]] fuzz_digest finish() && { return std::move(hasher_).final(); }

private:
    sha256_hasher hasher_;
};

class fuzz_case_context final {
public:
    fuzz_case_context(
      fuzz_harness harness,
      std::span<const std::uint8_t> input,
      event_trace& trace,
      event_log_sink& events)
      : harness_(harness)
      , seed_(fuzz_master_seed(input))
      , configuration_(fuzz_configuration(harness))
      , input_(input.begin(), input.end())
      , configuration_digest_(digest_bytes(configuration_))
      , input_digest_(digest_bytes(input_))
      , trace_(trace)
      , scheduler_(case_scheduler_limits(harness), &trace_)
      , events_(events) {}

    [[nodiscard]] scheduler& event_scheduler() noexcept { return scheduler_; }
    [[nodiscard]] event_trace& trace() noexcept { return trace_; }
    [[nodiscard]] std::uint64_t seed() const noexcept { return seed_; }
    void command_completed() noexcept { ++commands_; }
    template<typename Integer>
    void terminal_integer(Integer value) {
        terminal_.integer(value);
    }
    void terminal_bytes(std::span<const std::uint8_t> bytes) {
        terminal_.bytes(bytes);
    }

    [[nodiscard]] runtime::result<fuzz_reproduction>
    finish(fuzz_case_outcome outcome) {
        // Every scheduled effect precedes the terminal event. Keep the first
        // trace mismatch, including a missing suffix, ahead of that
        // publication.
        if (auto finished = trace_.finish_replay(); !finished) {
            return runtime::failure(finished.error());
        }
        using observability::event_field_key;
        using observability::event_kind;
        using observability::event_public_text;
        const auto outcome_text = outcome.code == errc::success
                                    ? event_public_text::outcome_completed
                                    : event_public_text::outcome_failed;
        auto outcome_field = observability::make_event_field<
          event_kind::fault_decision,
          event_field_key::outcome>(outcome_text);
        auto operation_field = observability::make_event_field<
          event_kind::fault_decision,
          event_field_key::operation>(case_operation(harness_));
        auto stable_id = observability::event_stable_id::make(
          static_cast<std::uint8_t>(harness_));
        if (!outcome_field || !operation_field || !stable_id) {
            return runtime::failure(case_error(errc::invariant_violation));
        }
        std::array<observability::event_field, 5> fields{
          *outcome_field,
          *operation_field,
          observability::make_event_field<
            event_kind::fault_decision,
            event_field_key::occurrence>(commands_),
          observability::make_event_field<
            event_kind::fault_decision,
            event_field_key::stable_id>(*stable_id),
        };
        std::size_t field_count = 4;
        if (outcome.code != errc::success) {
            const auto reason = observability::event_reason_for(outcome.code);
            if (!reason) {
                return runtime::failure(case_error(errc::invariant_violation));
            }
            auto reason_field = observability::make_event_field<
              event_kind::fault_decision,
              event_field_key::reason>(*reason);
            if (!reason_field) {
                return runtime::failure(reason_field.error());
            }
            fields[field_count++] = *reason_field;
        }
        auto request = observability::event_request::make(
          observability::event_request_context{
            .kind = event_kind::fault_decision,
            .severity = outcome.code == errc::success
                          ? observability::event_severity::info
                          : observability::event_severity::error,
            .monotonic = scheduler_.now(),
            .wall = runtime::wall_time{static_cast<runtime::wall_time::rep>(
              scheduler_.now().nanoseconds())},
            .workload = resource::workload_class::maintenance,
          },
          std::span{fields}.first(field_count));
        if (!request) {
            return runtime::failure(request.error());
        }
        if (auto emitted = events_.emit(*request); !emitted) {
            return runtime::failure(emitted.error());
        }
        if (auto finished = events_.finish_replay(); !finished) {
            return runtime::failure(finished.error());
        }
        if (scheduler_.pending_events() != 0) {
            return runtime::failure(case_error(errc::invariant_violation));
        }
        auto encoded_trace = trace_.encode_cooperatively(64).get();
        auto encoded_events = events_.events().encode();
        if (!encoded_trace || !encoded_events) {
            return runtime::failure(
              !encoded_trace ? encoded_trace.error() : encoded_events.error());
        }
        if (auto stopped = events_.stop(); !stopped) {
            return runtime::failure(stopped.error());
        }
        terminal_.integer(static_cast<std::uint8_t>(harness_));
        terminal_.integer(seed_);
        terminal_.integer(commands_);
        terminal_.integer(static_cast<std::uint32_t>(outcome.code));
        terminal_.integer(static_cast<std::uint8_t>(outcome.operation));
        const auto terminal_digest = std::move(terminal_).finish();
        return fuzz_reproduction::make(
          harness_,
          fuzz_harness_version,
          seed_,
          events_.events().identity().epoch.value(),
          std::move(configuration_),
          std::move(input_),
          configuration_digest_,
          input_digest_,
          terminal_digest,
          outcome,
          std::move(*encoded_trace),
          std::move(*encoded_events));
    }

private:
    fuzz_harness harness_;
    std::uint64_t seed_;
    std::vector<std::uint8_t> configuration_;
    std::vector<std::uint8_t> input_;
    fuzz_digest configuration_digest_;
    fuzz_digest input_digest_;
    event_trace& trace_;
    scheduler scheduler_;
    event_log_sink& events_;
    std::uint64_t commands_{0};
    terminal_hasher terminal_;
};

using fuzz_case_detail::wait_for;
using fuzz_case_detail::with_cleanup;

void cooperate(std::size_t command) {
    if ((command + 1U) % fuzz_scheduler_batch_max == 0) {
        seastar::thread::yield();
    }
}

struct scheduler_model_event final {
    std::uint64_t deadline;
    std::uint8_t priority;
    std::uint64_t id;
    std::uint64_t marker;
    bool active{true};
};

class scheduler_model final {
public:
    scheduler_model() {
        events_.reserve(fuzz_scheduler_pending_max);
        observed_.reserve(fuzz_scheduler_pending_max);
    }

    void schedule(
      std::uint64_t deadline,
      std::uint8_t priority,
      std::uint64_t id,
      std::uint64_t marker) {
        events_.push_back({deadline, priority, id, marker});
    }

    [[nodiscard]] bool cancel(std::uint64_t id) noexcept {
        for (auto& event : events_) {
            if (event.id == id && event.active) {
                event.active = false;
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] bool step() {
        const auto selected = earliest_ready();
        if (!selected) {
            return false;
        }
        execute(*selected);
        return true;
    }

    [[nodiscard]] bool advance_to_next() {
        if (earliest_ready()) {
            return false;
        }
        const auto selected = earliest_active();
        if (!selected) {
            return false;
        }
        now_ = events_[*selected].deadline;
        return true;
    }

    [[nodiscard]] std::uint64_t run_ready(std::uint64_t maximum) {
        std::uint64_t count = 0;
        while (count < maximum && step()) {
            ++count;
        }
        return count;
    }

    [[nodiscard]] std::uint64_t
    run_until(std::uint64_t target, std::uint64_t maximum) {
        std::uint64_t count = 0;
        while (count < maximum) {
            const auto selected = earliest_active();
            if (!selected || events_[*selected].deadline > target) {
                break;
            }
            now_ = events_[*selected].deadline;
            execute(*selected);
            ++count;
        }
        const auto selected = earliest_active();
        if (!selected || events_[*selected].deadline > target) {
            now_ = target;
        }
        return count;
    }

    [[nodiscard]] std::uint64_t now() const noexcept { return now_; }

    [[nodiscard]] std::size_t pending() const noexcept {
        return static_cast<std::size_t>(
          std::count_if(events_.begin(), events_.end(), [](const auto& event) {
              return event.active;
          }));
    }

    [[nodiscard]] const std::vector<std::uint64_t>& observed() const noexcept {
        return observed_;
    }

private:
    [[nodiscard]] static bool earlier(
      const scheduler_model_event& left,
      const scheduler_model_event& right) noexcept {
        if (left.deadline != right.deadline) {
            return left.deadline < right.deadline;
        }
        if (left.priority != right.priority) {
            return left.priority < right.priority;
        }
        return left.id < right.id;
    }

    [[nodiscard]] std::optional<std::size_t> earliest_active() const noexcept {
        std::optional<std::size_t> selected;
        for (std::size_t index = 0; index < events_.size(); ++index) {
            if (
              events_[index].active
              && (!selected || earlier(events_[index], events_[*selected]))) {
                selected = index;
            }
        }
        return selected;
    }

    [[nodiscard]] std::optional<std::size_t> earliest_ready() const noexcept {
        const auto selected = earliest_active();
        return selected && events_[*selected].deadline <= now_ ? selected
                                                               : std::nullopt;
    }

    void execute(std::size_t index) {
        events_[index].active = false;
        observed_.push_back(events_[index].marker);
    }

    std::vector<scheduler_model_event> events_;
    std::vector<std::uint64_t> observed_;
    std::uint64_t now_{0};
};

[[nodiscard]] bool scheduler_matches(
  const scheduler& actual,
  const scheduler_model& expected,
  const std::vector<std::uint64_t>& observed) {
    return actual.now().nanoseconds() == expected.now()
           && actual.pending_events() == expected.pending()
           && observed == expected.observed();
}

class scheduled_event_cleanup final {
public:
    scheduled_event_cleanup(scheduler& target, const std::vector<event_id>& ids)
      : target_(&target)
      , ids_(&ids) {}

    ~scheduled_event_cleanup() {
        if (released_) {
            return;
        }
        if (target_->trace_failed()) {
            static_cast<void>(target_->discard_failed());
            return;
        }
        for (const auto id : *ids_) {
            static_cast<void>(target_->cancel(id));
        }
    }

    void release() noexcept { released_ = true; }

private:
    scheduler* target_;
    const std::vector<event_id>* ids_;
    bool released_{false};
};

class crypto_thread_cleanup final {
public:
    ~crypto_thread_cleanup() { OPENSSL_thread_stop(); }
};

[[nodiscard]] fuzz_case_outcome run_scheduler_case(
  fuzz_case_context& context, std::span<const std::uint8_t> input) {
    input_cursor cursor{input};
    scheduler_model model;
    std::vector<std::uint64_t> observed;
    observed.reserve(fuzz_scheduler_pending_max);
    std::vector<event_id> ids;
    ids.reserve(fuzz_scheduler_pending_max);
    auto& target = context.event_scheduler();
    scheduled_event_cleanup cleanup{target, ids};

    // This corpus-selected path fills the bounded queue exactly, verifies that
    // the next admission is rejected, and drains in caller-sized batches.  It
    // consumes exactly the trace budget: one admission and one selection entry
    // for each accepted event.
    if (
      input.size() > sizeof(std::uint64_t)
      && input[sizeof(std::uint64_t)] == 'S') {
        for (std::uint64_t marker = 1; marker <= fuzz_scheduler_pending_max;
             ++marker) {
            auto scheduled = target.schedule(
              target.now(),
              event_priority::normal(),
              [&observed, marker] noexcept { observed.push_back(marker); },
              trace_event_descriptor{
                .kind = trace_event_kind::generic,
                .stable_id = marker,
              });
            if (!scheduled) {
                return mismatch(runtime::operation_kind::scheduler);
            }
            ids.push_back(*scheduled);
            model.schedule(
              target.now().nanoseconds(),
              event_priority::normal().value(),
              scheduled->value(),
              marker);
            context.command_completed();
        }
        auto rejected = target.schedule(
          target.now(),
          event_priority::normal(),
          [] noexcept {},
          trace_event_descriptor{
            .kind = trace_event_kind::generic,
            .stable_id = fuzz_scheduler_pending_max + 1U,
          });
        context.command_completed();
        if (rejected || rejected.error().code() != errc::queue_full) {
            return mismatch(runtime::operation_kind::scheduler);
        }
        while (model.pending() != 0) {
            const auto expected = model.run_ready(fuzz_scheduler_batch_max);
            const auto actual = target.run_ready_batch(
              fuzz_scheduler_batch_max);
            if (
              !actual || *actual != expected
              || !scheduler_matches(target, model, observed)) {
                return mismatch(runtime::operation_kind::scheduler);
            }
            seastar::thread::yield();
        }
        cleanup.release();
        context.terminal_integer(model.now());
        context.terminal_integer(model.pending());
        context.terminal_integer(observed.size());
        for (const auto marker : observed) {
            context.terminal_integer(marker);
        }
        return success();
    }

    for (std::size_t command = 0;
         command < fuzz_commands_max && !cursor.empty();
         ++command) {
        // Existing events own their terminal credits. Keep enough free trace
        // capacity for the next admission and all possible time advances;
        // reaching the campaign budget ends a valid prefix, not a model
        // failure.
        const auto headroom = static_cast<std::uint32_t>(model.pending() + 3U);
        auto trace_room = context.trace().reserve(
          headroom, headroom * canonical_entry_encoded_size);
        if (!trace_room) {
            if (trace_room.error().code() != errc::resource_exhausted) {
                return mismatch(runtime::operation_kind::scheduler);
            }
            for (const auto id : ids) {
                const auto expected = model.cancel(id.value());
                const auto canceled = target.cancel(id);
                if (!canceled || *canceled != expected) {
                    return mismatch(runtime::operation_kind::scheduler);
                }
            }
            if (!scheduler_matches(target, model, observed)) {
                return mismatch(runtime::operation_kind::scheduler);
            }
            context.terminal_integer(std::uint8_t{1});
            break;
        }
        trace_room->release();
        const auto operation = cursor.byte() % 5U;
        if (operation == 0 || ids.empty()) {
            if (
              target.pending_events() < fuzz_scheduler_pending_max
              && ids.size() < fuzz_scheduler_pending_max) {
                const auto deadline = target.now().nanoseconds()
                                      + cursor.bounded(33);
                const auto priority = cursor.byte();
                const auto marker = static_cast<std::uint64_t>(ids.size()) + 1U;
                auto scheduled = target.schedule(
                  runtime::monotonic_time{deadline},
                  event_priority{priority},
                  [&observed, marker] noexcept { observed.push_back(marker); },
                  trace_event_descriptor{
                    .kind = trace_event_kind::generic,
                    .stable_id = marker,
                  });
                if (!scheduled) {
                    return mismatch(runtime::operation_kind::scheduler);
                }
                ids.push_back(*scheduled);
                model.schedule(deadline, priority, scheduled->value(), marker);
            }
        } else if (operation == 1) {
            const auto index = cursor.bounded(ids.size());
            const auto expected = model.cancel(ids[index].value());
            const auto actual = target.cancel(ids[index]);
            if (!actual || *actual != expected) {
                return mismatch(runtime::operation_kind::scheduler);
            }
        } else if (operation == 2) {
            const auto expected = model.step();
            const auto actual = target.step();
            if (!actual || *actual != expected) {
                return mismatch(runtime::operation_kind::scheduler);
            }
        } else if (operation == 3) {
            if (target.has_ready_events()) {
                const auto expected = model.step();
                const auto actual = target.step();
                if (!actual || *actual != expected) {
                    return mismatch(runtime::operation_kind::scheduler);
                }
            } else {
                const auto expected = model.advance_to_next();
                const auto actual = target.advance_to_next();
                if (expected) {
                    if (
                      !actual || !*actual
                      || (*actual)->nanoseconds() != model.now()) {
                        return mismatch(runtime::operation_kind::scheduler);
                    }
                } else if (!actual || *actual) {
                    return mismatch(runtime::operation_kind::scheduler);
                }
            }
        } else {
            const auto deadline = std::min<std::uint64_t>(
              fuzz_scheduler_deadline_max, model.now() + cursor.bounded(33));
            const auto maximum = 1U + cursor.bounded(fuzz_scheduler_batch_max);
            const auto expected = model.run_until(deadline, maximum);
            const auto actual = target.run_until_batch(
              runtime::monotonic_time{deadline}, maximum);
            if (!actual || *actual != expected) {
                return mismatch(runtime::operation_kind::scheduler);
            }
        }
        context.command_completed();
        if (!scheduler_matches(target, model, observed)) {
            return mismatch(runtime::operation_kind::scheduler);
        }
        cooperate(command);
    }

    while (model.pending() != 0) {
        if (!target.has_ready_events()) {
            const auto expected = model.advance_to_next();
            const auto actual = target.advance_to_next();
            if (
              !expected || !actual || !*actual
              || (*actual)->nanoseconds() != model.now()) {
                return mismatch(runtime::operation_kind::scheduler);
            }
        }
        const auto expected = model.run_ready(fuzz_scheduler_batch_max);
        const auto actual = target.run_ready_batch(fuzz_scheduler_batch_max);
        if (!actual || *actual != expected) {
            return mismatch(runtime::operation_kind::scheduler);
        }
        if (!scheduler_matches(target, model, observed)) {
            return mismatch(runtime::operation_kind::scheduler);
        }
        seastar::thread::yield();
    }
    cleanup.release();
    context.terminal_integer(model.now());
    context.terminal_integer(model.pending());
    context.terminal_integer(observed.size());
    for (const auto marker : observed) {
        context.terminal_integer(marker);
    }
    return success();
}

[[nodiscard]] runtime::fault_decision fault_decision_for(std::uint8_t value) {
    switch (value % 6U) {
    case 0:
        return runtime::fault_decision::make_error();
    case 1:
        return runtime::fault_decision::make_delay(
          runtime::monotonic_duration{1U + value % 31U});
    case 2:
        return runtime::fault_decision::make_short_operation(
          byte_count{1U + value % 64U});
    case 3:
        return runtime::fault_decision::make_corrupt();
    case 4:
        return runtime::fault_decision::make_drop_completion();
    default:
        return runtime::fault_decision::make_crash();
    }
}

struct fault_model_rule final {
    std::uint64_t id;
    std::uint64_t first;
    std::uint64_t last;
    fault_selector_kind selector;
    std::uint64_t period;
    std::uint64_t numerator;
    std::uint64_t denominator;
    runtime::fault_decision decision;
};

[[nodiscard]] bool fault_applies(
  const fault_model_rule& rule, std::uint64_t occurrence, std::uint64_t seed) {
    switch (rule.selector) {
    case fault_selector_kind::once:
    case fault_selector_kind::bounded_range:
        return true;
    case fault_selector_kind::every_n:
        return (occurrence - rule.first) % rule.period == 0;
    case fault_selector_kind::rational: {
        if (rule.numerator == 0) {
            return false;
        }
        if (rule.numerator == rule.denominator) {
            return true;
        }
        const auto coordinate = random_coordinate::make(
          random_domain::fault_decision, rule.id, occurrence);
        if (!coordinate) {
            return false;
        }
        auto cursor = deterministic_random{seed}.cursor(*coordinate, 0);
        const auto selected = runtime::uniform_u64(cursor, rule.denominator);
        return selected && *selected < rule.numerator;
    }
    }
    return false;
}

[[nodiscard]] fuzz_case_outcome run_fault_case(
  fuzz_case_context& context, std::span<const std::uint8_t> input) {
    input_cursor cursor{input};
    seastar::chunked_vector<fault_rule> rules;
    std::vector<fault_model_rule> model;
    model.reserve(fuzz_fault_rules_max);
    std::uint64_t next_occurrence = 1;
    const bool saturating = input.size() > sizeof(std::uint64_t)
                            && input[sizeof(std::uint64_t)] == 'F';
    const auto rule_count = saturating ? std::size_t{fuzz_fault_rules_max}
                                       : std::min<std::size_t>(
                                           fuzz_fault_rules_max,
                                           cursor.remaining() / 6U);
    for (std::size_t index = 0; index < rule_count; ++index) {
        const auto selector_value = static_cast<std::uint8_t>(
          saturating ? 1U : cursor.byte() % 4U);
        const auto span = saturating || selector_value == 0
                            ? (saturating ? 4U : 1U)
                            : 1U + cursor.byte() % 4U;
        const auto first = next_occurrence;
        const auto last = first + span - 1U;
        next_occurrence = last + 1U;
        fault_selector selector = fault_selector::bounded_range();
        std::uint64_t period = 1;
        std::uint64_t numerator = 1;
        std::uint64_t denominator = 1;
        if (selector_value == 0) {
            selector = fault_selector::once();
        } else if (selector_value == 2) {
            period = 1U + cursor.byte() % span;
            auto made = fault_selector::every_n(period);
            if (!made) {
                return mismatch(runtime::operation_kind::fault);
            }
            selector = *made;
        } else if (selector_value == 3) {
            denominator = 1U + cursor.byte() % 16U;
            numerator = cursor.byte() % (denominator + 1U);
            auto probability = runtime::probability_ratio::make(
              numerator, denominator);
            if (!probability) {
                return mismatch(runtime::operation_kind::fault);
            }
            selector = fault_selector::rational(*probability);
        }
        const auto decision = fault_decision_for(
          saturating ? static_cast<std::uint8_t>(index) : cursor.byte());
        auto id = fault_rule_id::make(index + 1U);
        auto first_value = runtime::fault_occurrence::make(first);
        auto last_value = runtime::fault_occurrence::make(last);
        if (!id || !first_value || !last_value) {
            return mismatch(runtime::operation_kind::fault);
        }
        auto made = fault_rule::make(
          *id,
          runtime::builtin_fault_point::file_read,
          std::nullopt,
          *first_value,
          *last_value,
          selector,
          decision);
        if (!made) {
            return mismatch(runtime::operation_kind::fault);
        }
        rules.push_back(*made);
        model.push_back(
          fault_model_rule{
            .id = index + 1U,
            .first = first,
            .last = last,
            .selector = selector.kind(),
            .period = period,
            .numerator = numerator,
            .denominator = denominator,
            .decision = decision,
          });
    }

    auto limits = fault_schedule_limits::make(fuzz_fault_rules_max);
    if (!limits) {
        return mismatch(runtime::operation_kind::fault);
    }
    auto schedule = fault_schedule::make(
      context.event_scheduler(),
      context.trace(),
      context.seed(),
      std::move(rules),
      *limits);
    if (!schedule) {
        return mismatch(runtime::operation_kind::fault);
    }
    const auto evaluations = std::min<std::uint64_t>(
      fuzz_fault_evaluations_max,
      std::max<std::uint64_t>(1, next_occurrence + cursor.remaining()));
    std::uint64_t expected_applied = 0;
    for (std::uint64_t occurrence = 1; occurrence <= evaluations;
         ++occurrence) {
        runtime::fault_decision expected;
        for (const auto& rule : model) {
            if (occurrence >= rule.first && occurrence <= rule.last) {
                if (fault_applies(rule, occurrence, context.seed())) {
                    expected = rule.decision;
                    ++expected_applied;
                }
                break;
            }
        }
        const auto occurrence_value = runtime::fault_occurrence::make(
          occurrence);
        if (!occurrence_value) {
            return mismatch(runtime::operation_kind::fault);
        }
        auto actual = (*schedule)->evaluate(
          runtime::fault_request{
            .point = runtime::descriptor_for(
                       runtime::builtin_fault_point::file_read)
                       ->id,
            .occurrence = *occurrence_value,
            .object = runtime::fault_object_key::none(),
          });
        if (!actual || *actual != expected) {
            return mismatch(runtime::operation_kind::fault);
        }
        context.command_completed();
        cooperate(occurrence - 1U);
    }
    if (
      (*schedule)->evaluations() != evaluations
      || (*schedule)->applied_decisions() != expected_applied) {
        return mismatch(runtime::operation_kind::fault);
    }
    context.terminal_integer((*schedule)->evaluations());
    context.terminal_integer((*schedule)->applied_decisions());
    for (const auto& entry : context.trace().entries()) {
        if (entry.action == trace_action::fault_evaluated) {
            context.terminal_integer(entry.stable_id);
            context.terminal_integer(entry.coordinate_a);
            context.terminal_integer(entry.value);
            context.terminal_integer(entry.result);
        }
    }
    return success();
}

[[nodiscard]] runtime::file_path fuzz_file_path(std::uint8_t slot) {
    auto made = runtime::file_path::make(
      slot == 0 ? "/kwaque/data/alpha" : "/kwaque/data/beta");
    if (!made) {
        throw std::logic_error("invalid fuzz file path");
    }
    return std::move(*made);
}

[[nodiscard]] runtime::file_path fuzz_data_path() {
    auto made = runtime::file_path::make("/kwaque/data");
    if (!made) {
        throw std::logic_error("invalid fuzz data path");
    }
    return std::move(*made);
}

[[nodiscard]] runtime::file_path fuzz_root_path() {
    auto made = runtime::file_path::make("/kwaque");
    if (!made) {
        throw std::logic_error("invalid fuzz root path");
    }
    return std::move(*made);
}

[[nodiscard]] bytes::fragmented_buffer
fuzz_file_payload(std::size_t size, std::byte value) {
    const std::string payload(
      size, static_cast<char>(std::to_integer<unsigned>(value)));
    auto made = bytes::fragmented_buffer::copy_of(
      std::span<const char>{payload.data(), payload.size()});
    if (!made) {
        throw std::runtime_error("fuzz file payload allocation failed");
    }
    return std::move(*made);
}

[[nodiscard]] storage_outcome
storage_outcome_for(const runtime::operation_error& error) noexcept {
    switch (error.code()) {
    case errc::not_found:
        return storage_outcome::not_found;
    case errc::io_failure:
        return storage_outcome::io_failure;
    case errc::aborted:
        return storage_outcome::aborted;
    default:
        return storage_outcome::io_failure;
    }
}

template<typename T>
[[nodiscard]] storage_outcome
storage_outcome_for(const runtime::result<T>& result) {
    return result ? storage_outcome::success
                  : storage_outcome_for(result.error());
}

struct file_command_observation final {
    storage_outcome outcome;
    bool value_matches{true};
};

[[nodiscard]] file_command_observation execute_file_command(
  scheduler& events,
  fake_file_system& files,
  const dense_storage_model& model,
  const storage_command& command) {
    if (command.kind == storage_command_kind::sync_directory) {
        return {storage_outcome_for(
          wait_for(events, files.sync_directory(fuzz_data_path())))};
    }
    if (command.kind == storage_command_kind::crash) {
        return {storage_outcome_for(wait_for(events, files.crash()))};
    }
    if (command.kind == storage_command_kind::rename) {
        return {storage_outcome_for(wait_for(
          events,
          files.rename(
            fuzz_file_path(command.source),
            fuzz_file_path(command.destination))))};
    }
    if (command.kind == storage_command_kind::remove) {
        return {storage_outcome_for(
          wait_for(events, files.remove_file(fuzz_file_path(command.source))))};
    }

    auto opened = wait_for(
      events,
      files.open(
        fuzz_file_path(command.source),
        {.access = runtime::file_access::read_write,
         .create = command.kind == storage_command_kind::write}));
    if (!opened) {
        return {storage_outcome_for(opened.error())};
    }
    auto file = std::move(*opened);
    return with_cleanup(
      [&] {
          storage_outcome outcome = storage_outcome::success;
          bool value_matches = true;
          if (command.kind == storage_command_kind::write) {
              const auto written = wait_for(
                events,
                file.write(
                  runtime::file_position{command.position},
                  fuzz_file_payload(command.length, command.value)));
              outcome = storage_outcome_for(written);
              value_matches = !written || written->value() == command.length;
          } else if (command.kind == storage_command_kind::truncate) {
              outcome = storage_outcome_for(
                wait_for(events, file.truncate(command.length)));
          } else if (command.kind == storage_command_kind::flush) {
              outcome = storage_outcome_for(wait_for(events, file.flush()));
          } else {
              auto read = wait_for(
                events,
                file.read(
                  runtime::file_position{command.position},
                  byte_count{command.length}));
              outcome = storage_outcome_for(read);
              if (read) {
                  const auto* visible = model.visible_bytes(command.source);
                  if (visible == nullptr) {
                      value_matches = false;
                  } else {
                      const auto begin = std::min<std::size_t>(
                        command.position, visible->size());
                      const auto count = std::min<std::size_t>(
                        command.length, visible->size() - begin);
                      std::string expected(count, '\0');
                      std::transform(
                        visible->begin() + static_cast<std::ptrdiff_t>(begin),
                        visible->begin()
                          + static_cast<std::ptrdiff_t>(begin + count),
                        expected.begin(),
                        [](std::byte byte) {
                            return static_cast<char>(
                              std::to_integer<unsigned char>(byte));
                        });
                      value_matches = read->data().content_equals(expected)
                                      && read->eof()
                                           == (count < command.length);
                  }
              }
          }
          return file_command_observation{outcome, value_matches};
      },
      [&] {
          const auto closed = wait_for(events, file.close());
          if (!closed) {
              throw std::system_error(make_error_code(closed.error().code()));
          }
      });
}

[[nodiscard]] bool storage_matches(
  const fake_file_system& files,
  const dense_storage_model& model,
  std::uint32_t open_handles = 0,
  std::uint32_t pending_operations = 0,
  std::uint64_t pending_bytes = 0) {
    const auto actual = fake_file_test_access::snapshot(files);
    if (!actual) {
        return false;
    }
    const auto expected = model.snapshot();
    if (
      actual->objects.size() != expected.objects.size()
      || actual->retained_capacity != expected.retained_capacity
      || actual->generation != expected.generation
      || actual->open_handles != open_handles
      || actual->pending_operations != pending_operations
      || actual->pending_bytes != pending_bytes) {
        return false;
    }
    for (std::size_t index = 0; index < expected.objects.size(); ++index) {
        const auto& observed = actual->objects[index];
        const auto& wanted = expected.objects[index];
        if (
          observed.id != wanted.id
          || (observed.kind == fake_file_kind::directory) != wanted.directory
          || observed.visible_links != wanted.visible_links
          || observed.durable_links != wanted.durable_links
          || observed.visible_bytes != wanted.visible_bytes
          || observed.durable_bytes != wanted.durable_bytes
          || !std::equal(
            observed.visible_entries.begin(),
            observed.visible_entries.end(),
            wanted.visible_entries.begin(),
            wanted.visible_entries.end())
          || !std::equal(
            observed.durable_entries.begin(),
            observed.durable_entries.end(),
            wanted.durable_entries.begin(),
            wanted.durable_entries.end())) {
            return false;
        }
    }
    return true;
}

[[nodiscard]] fuzz_case_outcome run_file_limit_case(
  fuzz_case_context& context,
  fake_file_system& files,
  dense_storage_model& model) {
    constexpr std::uint16_t half_capacity{32U * 1'024U};
    constexpr std::uint64_t read_size{2U * 1'024U};
    constexpr std::size_t reads{32};
    bool matches = true;

    const std::array writes{
      storage_command{
        .kind = storage_command_kind::write,
        .source = 0,
        .position = 0,
        .length = half_capacity,
        .value = static_cast<std::byte>('a'),
      },
      storage_command{
        .kind = storage_command_kind::write,
        .source = 0,
        .position = half_capacity,
        .length = half_capacity,
        .value = static_cast<std::byte>('b'),
      },
    };
    for (const auto& command : writes) {
        const auto observed = execute_file_command(
          context.event_scheduler(), files, model, command);
        if (
          !observed.value_matches || !model.reconcile(command, observed.outcome)
          || !storage_matches(files, model)) {
            return mismatch(runtime::operation_kind::file);
        }
        context.command_completed();
    }
    if (model.snapshot().retained_capacity != fake_file_capacity) {
        return mismatch(runtime::operation_kind::file);
    }

    std::vector<seastar::future<runtime::result<bool>>> metadata;
    metadata.reserve(64);
    for (std::size_t index = 0; index < 64; ++index) {
        auto pending = files.exists(fuzz_file_path(0));
        matches = !pending.available() && matches;
        metadata.push_back(std::move(pending));
        context.command_completed();
    }
    matches = files.pending_operations() == 64
              && files.pending_bytes().value() == 0 && matches;
    auto metadata_overflow = files.exists(fuzz_file_path(0));
    context.command_completed();
    matches = metadata_overflow.available() && matches;
    if (metadata_overflow.available()) {
        const auto rejected = std::move(metadata_overflow).get();
        matches = !rejected && rejected.error().code() == errc::queue_full
                  && matches;
    } else {
        matches = false;
        static_cast<void>(
          wait_for(context.event_scheduler(), std::move(metadata_overflow)));
    }
    for (auto& pending : metadata) {
        const auto result = wait_for(
          context.event_scheduler(), std::move(pending));
        matches = result && *result && matches;
    }
    matches = storage_matches(files, model) && matches;

    auto opened = wait_for(
      context.event_scheduler(),
      files.open(
        fuzz_file_path(0), {.access = runtime::file_access::read_write}));
    if (!opened) {
        return mismatch(runtime::operation_kind::file);
    }
    auto file = std::move(*opened);
    matches = with_cleanup(
      [&] {
          std::vector<
            seastar::future<runtime::result<runtime::file_read_result>>>
            reading;
          reading.reserve(reads);
          for (std::size_t index = 0; index < reads; ++index) {
              reading.push_back(file.read(
                runtime::file_position{index * read_size},
                byte_count{read_size}));
              context.command_completed();
          }
          matches = files.pending_operations() == reads
                    && files.pending_reads() == reads
                    && files.pending_bytes().value() == fake_file_capacity
                    && matches;
          auto read_overflow = file.read(
            runtime::file_position{0}, byte_count{1});
          context.command_completed();
          for (std::size_t turn = 0; turn < 64 && !read_overflow.available();
               ++turn) {
              seastar::thread::yield();
          }
          if (read_overflow.available()) {
              const auto rejected = std::move(read_overflow).get();
              matches = !rejected
                        && rejected.error().code() == errc::resource_exhausted
                        && files.pending_operations() == reads
                        && files.pending_reads() == reads
                        && files.pending_bytes().value() == fake_file_capacity
                        && matches;
          } else {
              matches = false;
              static_cast<void>(
                wait_for(context.event_scheduler(), std::move(read_overflow)));
          }
          for (std::size_t index = 0; index < reading.size(); ++index) {
              auto result = wait_for(
                context.event_scheduler(), std::move(reading[index]));
              const char expected = index < reading.size() / 2U ? 'a' : 'b';
              matches = result && !result->eof()
                        && result->data().content_equals(
                          std::string(
                            static_cast<std::size_t>(read_size), expected))
                        && matches;
          }
          matches = files.pending_operations() == 0
                    && files.pending_reads() == 0
                    && files.pending_bytes().value() == 0 && matches;
          return matches;
      },
      [&] {
          const auto closed = wait_for(context.event_scheduler(), file.close());
          if (!closed) {
              throw std::system_error(make_error_code(closed.error().code()));
          }
      });
    matches = storage_matches(files, model) && matches;
    return matches ? success() : mismatch(runtime::operation_kind::file);
}

void hash_storage(
  fuzz_case_context& context, const dense_storage_model& model) {
    const auto snapshot = model.snapshot();
    context.terminal_integer(snapshot.generation);
    context.terminal_integer(snapshot.retained_capacity);
    context.terminal_integer(snapshot.objects.size());
    for (const auto& object : snapshot.objects) {
        context.terminal_integer(object.id);
        context.terminal_integer(static_cast<std::uint8_t>(object.directory));
        context.terminal_integer(object.visible_links);
        context.terminal_integer(object.durable_links);
        context.terminal_integer(object.visible_bytes.size());
        context.terminal_bytes(
          std::span{
            reinterpret_cast<const std::uint8_t*>(object.visible_bytes.data()),
            object.visible_bytes.size()});
        context.terminal_integer(object.durable_bytes.size());
        context.terminal_bytes(
          std::span{
            reinterpret_cast<const std::uint8_t*>(object.durable_bytes.data()),
            object.durable_bytes.size()});
        context.terminal_integer(object.visible_entries.size());
        for (const auto& [name, id] : object.visible_entries) {
            context.terminal_integer(name.size());
            context.terminal_bytes(
              std::span{
                reinterpret_cast<const std::uint8_t*>(name.data()),
                name.size()});
            context.terminal_integer(id);
        }
        context.terminal_integer(object.durable_entries.size());
        for (const auto& [name, id] : object.durable_entries) {
            context.terminal_integer(name.size());
            context.terminal_bytes(
              std::span{
                reinterpret_cast<const std::uint8_t*>(name.data()),
                name.size()});
            context.terminal_integer(id);
        }
    }
}

[[nodiscard]] fault_rule file_history_rule(std::uint8_t profile) {
    auto point = runtime::builtin_fault_point::file_write;
    auto decision = runtime::fault_decision::make_error();
    std::uint64_t occurrence = 2;
    switch (profile) {
    case 1:
    case 12:
        decision = runtime::fault_decision::make_delay(
          runtime::monotonic_duration{31});
        break;
    case 2:
        decision = runtime::fault_decision::make_short_operation(byte_count{8});
        break;
    case 3:
        decision = runtime::fault_decision::make_corrupt();
        break;
    case 4:
        decision = runtime::fault_decision::make_torn_write();
        break;
    case 5:
        decision = runtime::fault_decision::make_misdirect();
        break;
    case 6:
        decision = runtime::fault_decision::make_drop_completion();
        break;
    case 7:
        decision = runtime::fault_decision::make_crash();
        break;
    case 8:
        point = runtime::builtin_fault_point::file_truncate;
        occurrence = 1;
        decision = runtime::fault_decision::make_partial_resize();
        break;
    case 9:
    case 10:
    case 11:
        point = runtime::builtin_fault_point::file_read;
        occurrence = 1;
        decision = profile == 9 ? runtime::fault_decision::make_corrupt()
                   : profile == 10
                     ? runtime::fault_decision::make_short_operation(
                         byte_count{8})
                     : runtime::fault_decision::make_error();
        break;
    default:
        break;
    }
    auto made = fault_rule::make(
      *fault_rule_id::make(1),
      point,
      std::nullopt,
      *runtime::fault_occurrence::make(occurrence),
      *runtime::fault_occurrence::make(occurrence),
      fault_selector::once(),
      decision);
    if (!made) {
        throw std::logic_error("invalid file history rule");
    }
    return *made;
}

[[nodiscard]] std::vector<storage_fault_rule>
file_history_model_rules(std::uint8_t profile) {
    if (profile == 12) {
        profile = 1;
    }
    if (profile >= 9) {
        return {};
    }
    constexpr std::array actions{
      storage_fault_action::error,
      storage_fault_action::delay,
      storage_fault_action::short_operation,
      storage_fault_action::corrupt,
      storage_fault_action::torn_write,
      storage_fault_action::misdirect,
      storage_fault_action::drop_completion,
      storage_fault_action::crash,
      storage_fault_action::partial_resize};
    return {
      {.id = 1,
       .point = profile == 8 ? storage_command_kind::truncate
                             : storage_command_kind::write,
       .first = profile == 8 ? 1U : 2U,
       .last = profile == 8 ? 1U : 2U,
       .action = actions[profile],
       .payload = profile == 1 ? 31U : 8U}};
}

[[nodiscard]] fuzz_case_outcome run_file_history(
  fuzz_case_context& context,
  fake_file_system& files,
  dense_storage_model& model,
  std::span<const std::uint8_t> input,
  std::uint8_t profile) {
    auto& events = context.event_scheduler();
    const auto parameter = [&](std::size_t offset, std::uint8_t fallback) {
        return offset < input.size() ? input[offset] : fallback;
    };
    for (std::uint8_t slot = 0; slot < 2; ++slot) {
        for (const auto command : std::array{
               storage_command{
                 .kind = storage_command_kind::write,
                 .source = slot,
                 .length = 256,
                 .value = static_cast<std::byte>('a' + slot)},
               storage_command{
                 .kind = storage_command_kind::flush, .source = slot}}) {
            const auto observed = execute_file_command(
              events, files, model, command);
            if (
              !observed.value_matches
              || !model.reconcile(command, observed.outcome)
              || !storage_matches(files, model)) {
                return mismatch(runtime::operation_kind::file);
            }
            context.command_completed();
        }
    }
    const storage_command sync{.kind = storage_command_kind::sync_directory};
    const auto synced = execute_file_command(events, files, model, sync);
    if (!model.reconcile(sync, synced.outcome)) {
        return mismatch(runtime::operation_kind::file);
    }

    std::array<std::optional<runtime::file>, 2> handles;
    std::array<std::optional<seastar::future<runtime::result<byte_count>>>, 2>
      writes;
    std::array<std::optional<seastar::future<runtime::result<void>>>, 2>
      resizes;
    const auto result = with_cleanup(
      [&] {
          for (std::uint8_t slot = 0; slot < 2; ++slot) {
              auto opened = wait_for(
                events,
                files.open(
                  fuzz_file_path(profile == 12 ? 0 : slot),
                  {.access = runtime::file_access::read_write}));
              if (!opened) {
                  return mismatch(runtime::operation_kind::file);
              }
              handles[slot].emplace(std::move(*opened));
          }
          const std::uint8_t count = profile == 7 ? 1 : 2;
          std::array<storage_command, 2> commands;
          for (std::uint8_t slot = 0; slot < count; ++slot) {
              commands[slot] = {
                .kind = profile == 8 ? storage_command_kind::truncate
                                     : storage_command_kind::write,
                .source = static_cast<std::uint8_t>(profile == 12 ? 0 : slot),
                .position = 32,
                .length = static_cast<std::uint16_t>(
                  profile == 8 ? 16U + parameter(10, 17) % 112U : 32U),
                .value = static_cast<std::byte>(
                  'k' + parameter(11 + slot, slot) % 12U)};
              if (profile == 8) {
                  resizes[slot].emplace(
                    handles[slot]->truncate(commands[slot].length));
              } else {
                  writes[slot].emplace(
                    handles[slot]->write(
                      runtime::file_position{32},
                      fuzz_file_payload(32, commands[slot].value)));
              }
              context.command_completed();
          }
          if (profile == 6) {
              // Both effects finish while their completions remain parked.
              // Check the volatile image before an explicit crash settles the
              // futures and restores the durable image.
              wait_for(
                events,
                fake_file_test_access::wait_submitted(
                  files, fake_submission_kind::write, 4));
              while (events.pending_events() != 0) {
                  if (!events.has_ready_events()) {
                      const auto advanced = events.advance_to_next();
                      if (!advanced || !*advanced) {
                          throw std::system_error(make_error_code(
                            advanced ? errc::invariant_violation
                                     : advanced.error().code()));
                      }
                  }
                  const auto ran = events.run_ready_batch(
                    fuzz_scheduler_batch_max);
                  if (!ran) {
                      throw std::system_error(
                        make_error_code(ran.error().code()));
                  }
                  seastar::thread::yield();
              }
              if (writes[0]->available() || writes[1]->available()) {
                  return mismatch(runtime::operation_kind::file);
              }
              for (const auto& command : commands) {
                  if (!model.reconcile(command, storage_outcome::success)) {
                      return mismatch(runtime::operation_kind::file);
                  }
              }
              if (!storage_matches(files, model, 2, 2, 64)) {
                  return mismatch(runtime::operation_kind::file);
              }
              const auto crashed = wait_for(events, files.crash());
              if (
                !crashed
                || !model.reconcile(
                  storage_command{.kind = storage_command_kind::crash},
                  storage_outcome::success)) {
                  return mismatch(runtime::operation_kind::file);
              }
              context.command_completed();
          }
          if (profile == 12) {
              fake_file_test_access::wait_submitted(
                files, fake_submission_kind::write, 4)
                .get();
              if (
                fake_file_test_access::submitted(
                  files, fake_submission_kind::write)
                  != 4U
                || files.pending_writes() != 2U) {
                  return mismatch(runtime::operation_kind::file);
              }
              // Select only the overtaking native completion. Its public
              // result still crosses native continuations; pumping until that
              // result is ready could also select the delayed predecessor.
              if (!events.has_ready_events()) {
                  const auto advanced = events.advance_to_next();
                  if (!advanced) {
                      throw std::system_error(
                        make_error_code(advanced.error().code()));
                  }
                  if (!*advanced) {
                      return mismatch(runtime::operation_kind::file);
                  }
              }
              const auto selected = events.step();
              if (!selected) {
                  throw std::system_error(
                    make_error_code(selected.error().code()));
              }
              if (!*selected) {
                  return mismatch(runtime::operation_kind::file);
              }
          }
          for (std::uint8_t order = 0; order < count; ++order) {
              const auto slot = static_cast<std::uint8_t>(
                profile == 12 ? 1U - order : order);
              storage_outcome observed;
              if (resizes[slot]) {
                  auto pending = std::move(*resizes[slot]);
                  resizes[slot].reset();
                  observed = storage_outcome_for(
                    wait_for(events, std::move(pending)));
              } else {
                  auto pending = std::move(*writes[slot]);
                  writes[slot].reset();
                  const auto written = profile == 12 && order == 0
                                         ? std::move(pending).get()
                                         : wait_for(events, std::move(pending));
                  if (profile == 6) {
                      if (written || written.error().code() != errc::aborted) {
                          return mismatch(runtime::operation_kind::file);
                      }
                      observed = storage_outcome::success;
                  } else {
                      if (
                        written && written->value() != commands[slot].length) {
                          return mismatch(runtime::operation_kind::file);
                      }
                      observed = storage_outcome_for(written);
                  }
              }
              if (profile != 6 && !model.reconcile(commands[slot], observed)) {
                  return mismatch(runtime::operation_kind::file);
              }
              if (
                profile == 12 && order == 0
                && (writes[0]->available() || !storage_matches(files, model, 2, 1, 32))) {
                  return mismatch(runtime::operation_kind::file);
              }
          }
          if (profile >= 9 && profile <= 11) {
              for (std::uint8_t slot = 0; slot < 2; ++slot) {
                  auto read = wait_for(
                    events,
                    handles[slot]->read(
                      runtime::file_position{32}, byte_count{32}));
                  if (profile == 11) {
                      if (read || read.error().code() != errc::io_failure) {
                          return mismatch(runtime::operation_kind::file);
                      }
                      continue;
                  }
                  if (!read) {
                      return mismatch(runtime::operation_kind::file);
                  }
                  const auto* stored = model.visible_bytes(slot);
                  const std::size_t count_bytes = profile == 10 ? 8 : 32;
                  std::vector<std::byte> expected(
                    stored->begin() + 32,
                    stored->begin() + 32
                      + static_cast<std::ptrdiff_t>(count_bytes));
                  if (profile == 9) {
                      auto random = deterministic_random{context.seed()}.stream(
                        *random_coordinate::make(
                          random_domain::fault_decision, 1, 1));
                      const auto offset = *runtime::uniform_u64(random, 32);
                      const auto bit = *runtime::uniform_u64(random, 8);
                      expected[offset] ^= std::byte{
                        static_cast<std::uint8_t>(1U << bit)};
                  }
                  std::array<char, 32> actual{};
                  auto copied = read->data().copy_to(
                    std::span{actual}.first(count_bytes));
                  if (
                    !copied || copied->value() != count_bytes
                    || read->data().size().value() != count_bytes
                    || read->eof() != (profile == 10)) {
                      return mismatch(runtime::operation_kind::file);
                  }
                  for (std::size_t index = 0; index < count_bytes; ++index) {
                      if (
                        static_cast<std::byte>(actual[index])
                        != expected[index]) {
                          return mismatch(runtime::operation_kind::file);
                      }
                  }
                  context.command_completed();
              }
          }
          return success();
      },
      [&] {
          std::exception_ptr failure;
          for (auto& handle : handles) {
              if (handle) {
                  handle->request_abort();
              }
          }
          if (
            events.trace_failed()
            || (profile == 6 && (writes[0] || writes[1]))) {
              // A failed assertion may interrupt the explicit restart that owns
              // parked completions. Stop still settles every admitted future
              // before any runtime file gate or handle can be destroyed.
              try {
                  const auto stopped = wait_for(events, files.stop());
                  if (!stopped) {
                      throw std::system_error(
                        make_error_code(stopped.error().code()));
                  }
              } catch (...) {
                  runtime::testing::retain_cleanup_failure(failure);
              }
          }
          for (std::size_t slot = 0; slot < handles.size(); ++slot) {
              try {
                  if (writes[slot]) {
                      static_cast<void>(
                        wait_for(events, std::move(*writes[slot])));
                      writes[slot].reset();
                  }
                  if (resizes[slot]) {
                      static_cast<void>(
                        wait_for(events, std::move(*resizes[slot])));
                      resizes[slot].reset();
                  }
              } catch (...) {
                  runtime::testing::retain_cleanup_failure(failure);
              }
              if (handles[slot]) {
                  try {
                      auto closed = wait_for(events, handles[slot]->close());
                      if (!closed) {
                          throw std::system_error(
                            make_error_code(closed.error().code()));
                      }
                  } catch (...) {
                      runtime::testing::retain_cleanup_failure(failure);
                  }
              }
          }
          if (failure) {
              std::rethrow_exception(failure);
          }
      });
    if (result.code != errc::success || !storage_matches(files, model)) {
        return mismatch(runtime::operation_kind::file);
    }
    // Independently choose which new bytes cross a durability barrier before
    // restart. The model, not the observed file, supplies the recovered image.
    for (std::uint8_t slot = 0; slot < 2; ++slot) {
        if ((parameter(13, 1) & (1U << slot)) != 0U) {
            const storage_command flush{
              .kind = storage_command_kind::flush, .source = slot};
            const auto observed = execute_file_command(
              events, files, model, flush);
            if (!model.reconcile(flush, observed.outcome)) {
                return mismatch(runtime::operation_kind::file);
            }
        }
    }
    const storage_command crash{.kind = storage_command_kind::crash};
    const auto restarted = execute_file_command(events, files, model, crash);
    if (
      !model.reconcile(crash, restarted.outcome)
      || !storage_matches(files, model)) {
        return mismatch(runtime::operation_kind::file);
    }
    context.command_completed();
    return success();
}

[[nodiscard]] fuzz_case_outcome
run_file_case(fuzz_case_context& context, std::span<const std::uint8_t> input) {
    auto fault_limits = fault_schedule_limits::make(fuzz_fault_rules_max);
    if (!fault_limits) {
        return mismatch(runtime::operation_kind::file);
    }
    const bool history = input.size() > 9 && input[8] == 'F';
    const auto profile = history ? static_cast<std::uint8_t>(input[9] % 13U)
                                 : std::uint8_t{0};
    seastar::chunked_vector<fault_rule> rules;
    if (history) {
        rules.push_back(file_history_rule(profile));
    }
    auto faults = fault_schedule::make(
      context.event_scheduler(),
      context.trace(),
      context.seed(),
      std::move(rules),
      *fault_limits);
    if (!faults) {
        return mismatch(runtime::operation_kind::file);
    }
    fake_file_system_config config;
    config.logical_capacity = byte_count{fake_file_capacity};
    config.maximum_objects = 16;
    config.maximum_operation_bytes = byte_count{fake_file_capacity};
    config.maximum_retained_path_bytes = byte_count{4'096};
    config.maximum_open_handles = 4;
    config.maximum_pending_operations = 64;
    config.maximum_pending_bytes = byte_count{fake_file_capacity};
    config.maximum_pending_reads = 32;
    config.maximum_pending_writes = 4;
    config.memory_dma_alignment = 1;
    config.disk_read_dma_alignment = 1;
    config.disk_write_dma_alignment = 1;
    config.disk_overwrite_dma_alignment = 1;
    config.native_max_length = static_cast<std::uint32_t>(fake_file_capacity);
    auto made = fake_file_system::make(
      std::move(config), context.event_scheduler(), **faults);
    if (!made) {
        return mismatch(runtime::operation_kind::file);
    }
    auto files = std::move(*made);
    return with_cleanup(
      [&] {
          if (!wait_for(
                context.event_scheduler(),
                files->create_directories(fuzz_data_path()))) {
              return mismatch(runtime::operation_kind::file);
          }
          // Persist the new data-directory entry in its parent. Syncing the
          // empty child does not make that parent entry durable.
          if (!wait_for(
                context.event_scheduler(),
                files->sync_directory(fuzz_root_path()))) {
              return mismatch(runtime::operation_kind::file);
          }

          dense_storage_model model{
            history ? file_history_model_rules(profile)
                    : std::vector<storage_fault_rule>{},
            context.seed()};
          if (!storage_matches(*files, model)) {
              return mismatch(runtime::operation_kind::file);
          }
          if (history) {
              const auto outcome = run_file_history(
                context, *files, model, input, profile);
              hash_storage(context, model);
              return outcome;
          }
          if (
            input.size() > sizeof(std::uint64_t)
            && input[sizeof(std::uint64_t)] == 'L') {
              auto outcome = run_file_limit_case(context, *files, model);
              hash_storage(context, model);
              return outcome;
          }
          input_cursor cursor{input};
          const auto commands = std::min<std::size_t>(
            file_commands_max, cursor.remaining() / 7U);
          auto outcome = success();
          for (std::size_t index = 0; index < commands; ++index) {
              storage_command command{
                .kind = static_cast<storage_command_kind>(cursor.byte() % 8U),
                .source = static_cast<std::uint8_t>(cursor.byte() % 2U),
              };
              command.destination = command.source ^ 1U;
              command.position = static_cast<std::uint16_t>(
                cursor.bounded(1'024));
              command.length = static_cast<std::uint16_t>(
                1U + cursor.bounded(64));
              command.value = static_cast<std::byte>(cursor.byte());
              if (
                command.kind != storage_command_kind::write
                && command.kind != storage_command_kind::sync_directory
                && command.kind != storage_command_kind::crash
                && !model.exists(command.source)) {
                  command.kind = storage_command_kind::write;
              }
              const auto observed = execute_file_command(
                context.event_scheduler(), *files, model, command);
              if (
                !observed.value_matches
                || !model.reconcile(command, observed.outcome)
                || !storage_matches(*files, model)) {
                  outcome = mismatch(runtime::operation_kind::file);
                  break;
              }
              context.command_completed();
              cooperate(index);
          }
          hash_storage(context, model);
          return outcome;
      },
      [&] {
          const auto stopped = wait_for(
            context.event_scheduler(), files->stop());
          if (!stopped) {
              throw std::system_error(make_error_code(stopped.error().code()));
          }
          const auto closed = wait_for(
            context.event_scheduler(), files->exists(fuzz_file_path(0)));
          if (closed || closed.error().code() != errc::closed) {
              throw std::logic_error("stopped file owner admitted a lookup");
          }
      });
}

} // namespace

bool network_stream_is_exact(
  scheduler& events,
  fake_connection& receiver,
  std::string_view expected,
  seastar::abort_source& abort_source) {
    if (expected.size() >= runtime::maximum_network_operation_bytes.value()) {
        return false;
    }
    std::size_t consumed = 0;
    while (true) {
        auto received = fuzz_case_detail::wait_for(
          events,
          receiver.read(
            byte_count{expected.size() - consumed + 1U}, abort_source));
        if (!received) {
            return false;
        }
        if (received->eof()) {
            return received->data().empty() && consumed == expected.size();
        }
        const auto count = received->data().size().value();
        if (
          count == 0 || count > expected.size() - consumed
          || !received->data().content_equals(
            expected.substr(consumed, count))) {
            return false;
        }
        consumed += static_cast<std::size_t>(count);
    }
}

namespace {

constexpr auto network_source = runtime::network_address::ipv4(
  {std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}});
constexpr auto network_target = runtime::network_address::ipv4(
  {std::byte{127}, std::byte{0}, std::byte{0}, std::byte{2}});
constexpr std::uint64_t network_fault_rule_id{0x4e45544641554c54};
constexpr std::uint64_t network_nanoseconds_per_second{1'000'000'000};

[[nodiscard]] runtime::network_address
network_numbered_address(std::size_t index) {
    return runtime::network_address::ipv4(
      {std::byte{127},
       std::byte{0},
       std::byte{1},
       static_cast<std::byte>(index + 1U)});
}

[[nodiscard]] bytes::fragmented_buffer
network_payload(std::size_t size, std::uint8_t pattern) {
    const std::string source(size, static_cast<char>(pattern));
    auto made = bytes::fragmented_buffer::copy_of(
      std::span<const char>{source.data(), source.size()});
    if (!made) {
        throw std::runtime_error("fuzz network payload allocation failed");
    }
    return std::move(*made);
}

void drain_scheduler(scheduler& events) {
    while (events.pending_events() != 0) {
        if (!events.has_ready_events()) {
            const auto advanced = events.advance_to_next();
            if (!advanced) {
                throw std::system_error(
                  make_error_code(advanced.error().code()));
            }
            if (!*advanced) {
                throw std::runtime_error(
                  "fuzz scheduler drain could not advance");
            }
        }
        const auto ran = events.run_ready_batch(fuzz_scheduler_batch_max);
        if (!ran) {
            throw std::system_error(make_error_code(ran.error().code()));
        }
        if (*ran == 0) {
            throw std::runtime_error("fuzz scheduler drain made no progress");
        }
        seastar::thread::yield();
    }
}

[[nodiscard]] runtime::fault_action
network_fault_action(std::uint8_t selector) {
    constexpr std::array actions{
      runtime::fault_action::none,
      runtime::fault_action::error,
      runtime::fault_action::delay,
      runtime::fault_action::short_operation,
      runtime::fault_action::drop,
      runtime::fault_action::duplicate,
      runtime::fault_action::reorder,
      runtime::fault_action::disconnect,
      runtime::fault_action::corrupt,
      runtime::fault_action::drop_completion,
    };
    return actions[selector % actions.size()];
}

[[nodiscard]] runtime::fault_decision
network_fault_decision(runtime::fault_action action) {
    switch (action) {
    case runtime::fault_action::error:
        return runtime::fault_decision::make_error();
    case runtime::fault_action::delay:
        return runtime::fault_decision::make_delay(
          runtime::monotonic_duration{1'000'000});
    case runtime::fault_action::short_operation:
        return runtime::fault_decision::make_short_operation(byte_count{2});
    case runtime::fault_action::drop:
        return runtime::fault_decision::make_drop();
    case runtime::fault_action::duplicate:
        return runtime::fault_decision::make_duplicate();
    case runtime::fault_action::reorder:
        return runtime::fault_decision::make_reorder();
    case runtime::fault_action::disconnect:
        return runtime::fault_decision::make_disconnect();
    case runtime::fault_action::corrupt:
        return runtime::fault_decision::make_corrupt();
    case runtime::fault_action::drop_completion:
        return runtime::fault_decision::make_drop_completion();
    default:
        return {};
    }
}

[[nodiscard]] fault_rule make_network_fault_rule(
  std::uint64_t id,
  std::optional<std::size_t> connection,
  std::uint64_t occurrence,
  runtime::fault_decision decision) {
    const auto rule_id = fault_rule_id::make(id);
    const auto selected = runtime::fault_occurrence::make(occurrence);
    if (!rule_id || !selected) {
        throw std::logic_error("invalid fuzz network fault identity");
    }
    std::optional<runtime::fault_object_key> object;
    if (connection) {
        std::array<std::byte, 9> encoded{};
        auto pair = static_cast<std::uint64_t>(*connection + 1U);
        for (std::size_t index = 0; index < sizeof(pair); ++index) {
            encoded[index] = static_cast<std::byte>(pair & 0xffU);
            pair >>= 8U;
        }
        const auto made = runtime::fault_object_key::from_bytes(encoded);
        if (!made) {
            throw std::logic_error("invalid fuzz network fault object");
        }
        object = *made;
    }
    auto made = fault_rule::make(
      *rule_id,
      runtime::builtin_fault_point::network_write,
      object,
      *selected,
      *selected,
      fault_selector::once(),
      decision);
    if (!made) {
        throw std::logic_error("invalid fuzz network fault rule");
    }
    return *made;
}

// The dense byte-stream model remains independent of the fake network. This
// small timeline adds only elapsed-progress bookkeeping around the existing
// independent exact-rate solver; it never reads fake planner state.
class network_transfer_model final {
public:
    explicit network_transfer_model(std::size_t count)
      : flows_(count) {}

    void start(std::size_t index, std::uint64_t bytes, std::uint64_t now) {
        advance(now);
        auto& flow = flows_[index];
        flow.started = true;
        flow.active = true;
        flow.remaining = oracle_fraction{bytes};
        rebalance(capacity_);
    }

    void advance(std::uint64_t now) {
        if (now < last_update_) {
            throw std::logic_error("fuzz network model time moved backward");
        }
        const auto elapsed = now - last_update_;
        for (auto& flow : flows_) {
            if (!flow.active) {
                continue;
            }
            if (flow.rate.unlimited) {
                flow.remaining = oracle_fraction{};
            } else {
                const auto transferred
                  = flow.rate.finite.multiply(elapsed).divide(
                    network_nanoseconds_per_second);
                flow.remaining = transferred.compare(flow.remaining)
                                     == std::strong_ordering::less
                                   ? flow.remaining.subtract(transferred)
                                   : oracle_fraction{};
            }
            if (flow.remaining.zero()) {
                flow.active = false;
                flow.finished_at = now;
            }
        }
        last_update_ = now;
    }

    void rebalance(std::uint64_t capacity) {
        capacity_ = capacity;
        std::vector<oracle_flow> active;
        active.reserve(flows_.size());
        for (std::size_t index = 0; index < flows_.size(); ++index) {
            if (flows_[index].active) {
                active.push_back(
                  oracle_flow{
                    .id = index + 1U,
                    .bytes = 1,
                    .constraints = {oracle_constraint{
                      .resource = 1,
                      .capacity = oracle_capacity::finite(capacity)}},
                    .constraint_count = 1,
                  });
            }
        }
        auto solved = solve_bandwidth_oracle(active);
        if (!solved) {
            throw std::runtime_error("fuzz network oracle allocation failed");
        }
        solution_ = std::move(*solved);
        for (const auto& allocation : solution_.allocations) {
            flows_[allocation.flow - 1U].rate = allocation.rate;
        }
    }

    [[nodiscard]] std::optional<std::uint64_t> next_deadline() const {
        using integer = oracle_fraction::integer;
        std::optional<std::uint64_t> earliest;
        for (const auto& flow : flows_) {
            if (
              !flow.active
              || (!flow.rate.unlimited && flow.rate.finite.zero())) {
                continue;
            }
            std::uint64_t duration = 0;
            if (!flow.rate.unlimited) {
                const integer numerator = flow.remaining.numerator()
                                          * flow.rate.finite.denominator()
                                          * network_nanoseconds_per_second;
                const integer denominator = flow.remaining.denominator()
                                            * flow.rate.finite.numerator();
                const integer rounded = (numerator + denominator - 1)
                                        / denominator;
                duration = rounded.convert_to<std::uint64_t>();
            }
            const auto deadline = last_update_ + duration;
            earliest = !earliest || deadline < *earliest ? deadline : *earliest;
        }
        return earliest;
    }

    [[nodiscard]] std::size_t active() const noexcept {
        return solution_.allocations.size();
    }
    [[nodiscard]] bool started(std::size_t index) const noexcept {
        return flows_[index].started;
    }
    [[nodiscard]] bool completed(std::size_t index) const noexcept {
        return flows_[index].started && !flows_[index].active;
    }
    [[nodiscard]] std::uint64_t finished_at(std::size_t index) const noexcept {
        return flows_[index].finished_at;
    }
    [[nodiscard]] const oracle_digest& digest() const noexcept {
        return solution_.digest;
    }
    [[nodiscard]] std::uint64_t capacity() const noexcept { return capacity_; }

private:
    struct flow final {
        oracle_fraction remaining;
        oracle_rate rate;
        std::uint64_t finished_at{0};
        bool started{false};
        bool active{false};
    };
    std::vector<flow> flows_;
    oracle_solution solution_;
    std::uint64_t capacity_{0};
    std::uint64_t last_update_{0};
};

struct network_fixture final {
    std::unique_ptr<fault_schedule> faults;
    std::unique_ptr<fake_network> network;
    std::vector<fake_listener> listeners;
    std::vector<fake_connection> clients;
    std::vector<fake_connection> servers;
    std::vector<seastar::future<runtime::result<void>>> writes;
    std::vector<errc> write_outcomes;
    std::vector<runtime::fault_action> actions;
    std::vector<dense_network_oracle> oracles;
    std::vector<std::string> expected;
    std::vector<std::size_t> consumed;
    std::vector<std::size_t> sizes;
    std::vector<std::uint8_t> patterns;
    network_transfer_model transfer;
    seastar::abort_source accept_abort;
    seastar::abort_source connect_abort;
    seastar::abort_source write_abort;
    seastar::abort_source read_abort;
    std::size_t flow_count;
    bool many_to_one;

    network_fixture(
      std::size_t count, std::uint8_t fault, bool many, bool mixed)
      : expected(count)
      , consumed(count)
      , sizes(count)
      , patterns(count)
      , transfer(count)
      , flow_count(count)
      , many_to_one(many) {
        listeners.reserve(count);
        clients.reserve(count);
        servers.reserve(count);
        writes.reserve(2U * count);
        write_outcomes.reserve(2U * count);
        actions.reserve(count);
        oracles.reserve(count);
        for (std::size_t index = 0; index < count; ++index) {
            oracles.emplace_back(2);
            actions.push_back(network_fault_action(
              static_cast<std::uint8_t>(
                fault % 10U + (mixed ? index % 10U : 0U))));
        }
    }

    [[nodiscard]] runtime::network_address source(std::size_t index) const {
        return many_to_one ? network_numbered_address(index) : network_source;
    }
    [[nodiscard]] runtime::network_address target(std::size_t index) const {
        return many_to_one ? network_target : network_numbered_address(index);
    }
};

[[nodiscard]] bool
abort_network(fuzz_case_context& context, network_fixture& fixture) {
    fixture.network->request_abort();
    const auto stopped = wait_for(
      context.event_scheduler(), fixture.network->stop());
    fixture.writes.clear();
    fixture.write_outcomes.clear();
    fixture.clients.clear();
    fixture.servers.clear();
    fixture.listeners.clear();
    if (!stopped && stopped.error().code() == errc::replay_divergence) {
        throw std::system_error(make_error_code(stopped.error().code()));
    }
    return stopped.has_value()
           && context.event_scheduler().pending_events() == 0;
}

[[nodiscard]] bool
open_network(fuzz_case_context& context, network_fixture& fixture) {
    seastar::chunked_vector<fault_rule> rules;
    const bool all_reordered = std::ranges::all_of(
      fixture.actions,
      [](auto action) { return action == runtime::fault_action::reorder; });
    if (all_reordered) {
        rules.push_back(make_network_fault_rule(
          network_fault_rule_id,
          std::nullopt,
          2,
          runtime::fault_decision::make_delay(
            runtime::monotonic_duration{1'000'000'000})));
        rules.push_back(make_network_fault_rule(
          network_fault_rule_id + 1U,
          std::nullopt,
          3,
          runtime::fault_decision::make_reorder()));
    }
    for (std::size_t index = 0; !all_reordered && index < fixture.flow_count;
         ++index) {
        const auto action = fixture.actions[index];
        const auto id = network_fault_rule_id + 2U * index;
        if (action == runtime::fault_action::reorder) {
            rules.push_back(make_network_fault_rule(
              id,
              index,
              2,
              runtime::fault_decision::make_delay(
                runtime::monotonic_duration{1'000'000'000})));
            rules.push_back(make_network_fault_rule(
              id + 1U, index, 3, runtime::fault_decision::make_reorder()));
        } else if (action != runtime::fault_action::none) {
            rules.push_back(make_network_fault_rule(
              id, index, 2, network_fault_decision(action)));
        }
    }
    const auto limits = fault_schedule_limits::make(fuzz_fault_rules_max);
    if (!limits) {
        return false;
    }
    auto faults = fault_schedule::make(
      context.event_scheduler(),
      context.trace(),
      context.seed(),
      std::move(rules),
      *limits);
    if (!faults) {
        return false;
    }
    fixture.faults = std::move(*faults);
    const auto count = static_cast<std::uint32_t>(fixture.flow_count);
    fake_network_config config;
    config.maximum_listeners = count;
    config.maximum_connection_pairs = count;
    config.maximum_pending_connects = count;
    config.maximum_backlog_entries = count;
    config.maximum_operations = 2U * count + 8U;
    config.maximum_parked_operations = count;
    config.maximum_direction_bytes = byte_count{4'096};
    config.maximum_packets = 2U * count;
    config.maximum_packet_logical_bytes = byte_count{4'096U * count};
    config.maximum_packet_retained_bytes = byte_count{4'096U * count};
    config.maximum_direction_packets = 2;
    config.maximum_links = count + 1U;
    config.maximum_address_entries = 2U * count + 4U;
    config.maximum_active_flows = count;
    config.maximum_controls = 8;
    config.stop_batch = 64;
    config.latency_seed = context.seed();
    config.latency_min = runtime::monotonic_duration{17};
    config.latency_mean_parameter = config.latency_min;
    config.interframe_gap = runtime::monotonic_duration{3};
    config.reorder_window = runtime::monotonic_duration{1};
    auto made = fake_network::make(
      std::move(config), context.event_scheduler(), fixture.faults.get());
    if (!made) {
        return false;
    }
    fixture.network = std::move(*made);
    for (std::size_t index = 0; index < fixture.flow_count; ++index) {
        if (!fixture.many_to_one || index == 0) {
            auto bound = wait_for(
              context.event_scheduler(),
              fixture.network->listen(
                runtime::network_endpoint{fixture.target(index), 0}, {}));
            if (!bound) {
                static_cast<void>(abort_network(context, fixture));
                return false;
            }
            fixture.listeners.push_back(std::move(*bound));
        }
        auto& listener = fixture.many_to_one ? fixture.listeners.front()
                                             : fixture.listeners.back();
        auto accepting = listener.accept(fixture.accept_abort);
        auto connecting = fixture.network->connect(
          listener.local_endpoint(),
          runtime::network_endpoint{fixture.source(index), 0},
          runtime::network_connection_limits{
            .pending_write_bytes = byte_count{8'192}, .pending_writes = 2},
          fixture.connect_abort);
        auto connected = wait_for(
          context.event_scheduler(), std::move(connecting));
        if (connected) {
            fixture.clients.push_back(std::move(*connected));
        }
        if (!connected || context.event_scheduler().trace_failed()) {
            static_cast<void>(abort_network(context, fixture));
            return false;
        }
        auto accepted = wait_for(
          context.event_scheduler(), std::move(accepting));
        if (accepted) {
            fixture.servers.push_back(std::move(*accepted));
        }
        if (!accepted) {
            static_cast<void>(abort_network(context, fixture));
            return false;
        }
        for (const auto step :
             {oracle_step{
                .kind = oracle_step_kind::bind_exact,
                .source = 1,
                .port = listener.local_endpoint().port()},
              oracle_step{
                .kind = oracle_step_kind::connect_explicit,
                .source = 0,
                .target = 1},
              oracle_step{
                .kind = oracle_step_kind::accept, .source = 0, .target = 1}}) {
            if (!fixture.oracles[index].apply(step)) {
                return false;
            }
        }
        cooperate(index);
    }
    return true;
}

[[nodiscard]] bool set_network_capacity(
  fuzz_case_context& context,
  network_fixture& fixture,
  std::uint64_t capacity) {
    auto& events = context.event_scheduler();
    fixture.transfer.advance(events.now().nanoseconds());
    auto changed = fixture.many_to_one
                     ? fixture.network->set_ingress_capacity(
                         network_target, bandwidth_capacity::finite(capacity))
                     : fixture.network->set_egress_capacity(
                         network_source, bandwidth_capacity::finite(capacity));
    if (!wait_for(events, std::move(changed))) {
        return false;
    }
    fixture.transfer.rebalance(capacity);
    context.command_completed();
    return fixture.network->allocation_digest().words
           == fixture.transfer.digest().words;
}

[[nodiscard]] bool transfer_matches(network_fixture& fixture) {
    if (
      fixture.network->allocation_digest().words
        != fixture.transfer.digest().words
      || fixture.network->active_operations() != fixture.transfer.active()) {
        return false;
    }
    for (std::size_t index = 0; index < fixture.writes.size(); ++index) {
        if (
          fixture.writes[index].available()
          != fixture.transfer.completed(index)) {
            return false;
        }
    }
    return true;
}

[[nodiscard]] bool submit_initial_flow(
  fuzz_case_context& context, network_fixture& fixture, std::size_t index) {
    const oracle_step write{
      .kind = oracle_step_kind::write,
      .source = 0,
      .target = 1,
      .value = fixture.sizes[index],
      .pattern = fixture.patterns[index]};
    if (!fixture.oracles[index].apply(write)) {
        return false;
    }
    fixture.transfer.start(
      index,
      fixture.sizes[index],
      context.event_scheduler().now().nanoseconds());
    fixture.writes.push_back(fixture.clients[index].write(
      network_payload(fixture.sizes[index], fixture.patterns[index]),
      fixture.write_abort));
    context.command_completed();
    return transfer_matches(fixture);
}

[[nodiscard]] bool advance_fractional(
  fuzz_case_context& context, network_fixture& fixture, std::uint64_t delta) {
    auto& events = context.event_scheduler();
    const auto next = events.now().checked_add(
      runtime::monotonic_duration{delta});
    if (!next) {
        return false;
    }
    const auto advanced = events.run_until_batch(
      *next, fuzz_scheduler_batch_max);
    if (!advanced || *advanced != 0) {
        return false;
    }
    fixture.transfer.advance(next->nanoseconds());
    return transfer_matches(fixture);
}

[[nodiscard]] bool
drain_transfers(fuzz_case_context& context, network_fixture& fixture) {
    auto& events = context.event_scheduler();
    while (fixture.transfer.active() != 0) {
        const auto expected = fixture.transfer.next_deadline();
        if (!expected || events.pending_events() == 0) {
            return false;
        }
        if (!events.has_ready_events()) {
            const auto advanced = events.advance_to_next();
            if (
              !advanced || !*advanced
              || (*advanced)->nanoseconds() > *expected) {
                return false;
            }
        }
        const auto now = events.now().nanoseconds();
        fixture.transfer.advance(now);
        fixture.transfer.rebalance(fixture.transfer.capacity());
        while (events.has_ready_events()) {
            const auto ran = events.run_ready_batch(fuzz_scheduler_batch_max);
            if (!ran || *ran == 0) {
                return false;
            }
            seastar::thread::yield();
        }
        if (!transfer_matches(fixture)) {
            return false;
        }
    }
    for (std::size_t index = 0; index < fixture.writes.size(); ++index) {
        if (!std::move(fixture.writes[index]).get()) {
            return false;
        }
        context.terminal_integer(fixture.transfer.finished_at(index));
    }
    fixture.writes.clear();
    drain_scheduler(events);
    return true;
}

[[nodiscard]] bool network_link_control(
  fuzz_case_context& context, network_fixture& fixture, oracle_step_kind kind) {
    const auto source = fixture.source(0);
    const auto target = fixture.target(0);
    std::optional<seastar::future<runtime::result<void>>> changed;
    switch (kind) {
    case oracle_step_kind::clog:
        changed.emplace(fixture.network->clog(source, target));
        break;
    case oracle_step_kind::unclog:
        changed.emplace(fixture.network->unclog(source, target));
        break;
    case oracle_step_kind::partition:
        changed.emplace(fixture.network->partition(source, target));
        break;
    case oracle_step_kind::heal:
        changed.emplace(fixture.network->heal(source, target));
        break;
    default:
        return false;
    }
    if (
      !fixture.oracles.front().apply(
        oracle_step{.kind = kind, .source = 0, .target = 1})
      || !wait_for(context.event_scheduler(), std::move(*changed))) {
        return false;
    }
    context.command_completed();
    return true;
}

[[nodiscard]] bool consume_network_prefix(
  fuzz_case_context& context, network_fixture& fixture, std::size_t index) {
    const auto expected = fixture.oracles[index].snapshot().visible[1];
    while (fixture.consumed[index] < expected.size()) {
        const auto remaining = expected.size() - fixture.consumed[index];
        auto received = wait_for(
          context.event_scheduler(),
          fixture.servers[index].read(
            byte_count{remaining}, fixture.read_abort));
        if (
          !received || received->eof() || received->data().empty()
          || received->data().size().value() > remaining
          || !received->data().content_equals(
            std::string_view{expected}.substr(
              fixture.consumed[index], received->data().size().value()))) {
            return false;
        }
        fixture.consumed[index] += received->data().size().value();
    }
    context.command_completed();
    return true;
}

[[nodiscard]] bool run_concurrent_transfers(
  fuzz_case_context& context, network_fixture& fixture, input_cursor& cursor) {
    const auto variation = cursor.byte();
    const auto controls = cursor.byte();
    const bool clogged = (controls & 1U) != 0;
    const bool partitioned = (controls & 2U) != 0;
    for (std::size_t index = 0; index < fixture.flow_count; ++index) {
        fixture.sizes[index] = index == 0 ? 1U
                               : index + 1U == fixture.flow_count
                                 ? 257U
                                 : 1U + (index * 29U + variation) % 257U;
        fixture.patterns[index] = static_cast<std::uint8_t>(
          'a' + (variation + index) % 26U);
    }
    if (
      clogged
      && !network_link_control(context, fixture, oracle_step_kind::clog)) {
        return false;
    }
    if (
      partitioned
      && !network_link_control(context, fixture, oracle_step_kind::partition)) {
        return false;
    }
    if (!set_network_capacity(context, fixture, 0)) {
        return false;
    }
    const auto initial = fixture.flow_count == 1 ? 1 : fixture.flow_count - 1U;
    for (std::size_t index = 0; index < initial; ++index) {
        if (!submit_initial_flow(context, fixture, index)) {
            return false;
        }
        cooperate(index);
    }
    if (context.event_scheduler().pending_events() != 0) {
        return false;
    }
    const auto capacity = 30'001U + variation;
    if (
      !set_network_capacity(context, fixture, capacity)
      || !advance_fractional(context, fixture, 1)) {
        return false;
    }
    if (
      initial != fixture.flow_count
      && !submit_initial_flow(context, fixture, initial)) {
        return false;
    }
    if (
      fixture.transfer.active() != fixture.flow_count
      || fixture.network->active_operations() != fixture.flow_count) {
        return false;
    }
    context.terminal_integer(fixture.flow_count);
    if (
      !set_network_capacity(context, fixture, 0)
      || context.event_scheduler().pending_events() != 0
      || !set_network_capacity(context, fixture, capacity + 137U)
      || !advance_fractional(context, fixture, 7)
      || !set_network_capacity(context, fixture, capacity * 2U + 1U)) {
        return false;
    }
    for (std::size_t command = 0; command < 8U && !cursor.empty(); ++command) {
        const auto kind = cursor.byte() % 3U;
        const auto changed_capacity = capacity + cursor.bounded(1'001);
        const auto elapsed = 1U + cursor.byte() % 32U;
        if (!advance_fractional(context, fixture, elapsed)) {
            return false;
        }
        if (kind == 1U && !set_network_capacity(context, fixture, 0)) {
            return false;
        }
        if (
          kind != 2U
          && !set_network_capacity(context, fixture, changed_capacity)) {
            return false;
        }
    }
    if (!drain_transfers(context, fixture)) {
        return false;
    }
    if (
      partitioned
      && !network_link_control(context, fixture, oracle_step_kind::heal)) {
        return false;
    }
    if (
      clogged
      && !network_link_control(context, fixture, oracle_step_kind::unclog)) {
        return false;
    }
    drain_scheduler(context.event_scheduler());
    for (std::size_t index = 0; index < fixture.flow_count; ++index) {
        if (!consume_network_prefix(context, fixture, index)) {
            return false;
        }
        cooperate(index);
    }
    return true;
}

[[nodiscard]] bool
run_network_faults(fuzz_case_context& context, network_fixture& fixture) {
    for (std::size_t index = 0; index < fixture.flow_count; ++index) {
        const auto action = fixture.actions[index];
        fixture.writes.push_back(fixture.clients[index].write(
          network_payload(4, 'A'), fixture.write_abort));
        fixture.write_outcomes.push_back(
          action == runtime::fault_action::drop_completion ? errc::unavailable
          : action == runtime::fault_action::error      ? errc::fault_injected
          : action == runtime::fault_action::disconnect ? errc::network_failure
                                                        : errc::success);
        if (action != runtime::fault_action::reorder) {
            const auto modeled = fixture.oracles[index].apply(
              oracle_step{
                .kind = oracle_step_kind::write,
                .source = 0,
                .target = 1,
                .value = 4,
                .pattern = 'A',
                .action = action == runtime::fault_action::corrupt
                            ? runtime::fault_action::none
                            : action});
            const auto expected = action == runtime::fault_action::error
                                    ? errc::fault_injected
                                  : action == runtime::fault_action::disconnect
                                    ? errc::network_failure
                                    : errc::success;
            if (
              (modeled ? errc::success : modeled.error().code()) != expected) {
                return false;
            }
        }
        context.command_completed();
        cooperate(index);
    }
    for (std::size_t index = 0; index < fixture.flow_count; ++index) {
        if (fixture.actions[index] != runtime::fault_action::reorder) {
            continue;
        }
        fixture.writes.push_back(fixture.clients[index].write(
          network_payload(4, 'B'), fixture.write_abort));
        fixture.write_outcomes.push_back(errc::success);
        // The one-second delay on the predecessor exceeds every four-byte
        // transfer here. The explicitly reordered successor therefore arrives
        // first, then the predecessor releases the recorded sequence gap.
        for (const auto pattern : {'B', 'A'}) {
            if (!fixture.oracles[index].apply(
                  oracle_step{
                    .kind = oracle_step_kind::write,
                    .source = 0,
                    .target = 1,
                    .value = 4,
                    .pattern = static_cast<std::uint8_t>(pattern)})) {
                return false;
            }
        }
        context.command_completed();
        cooperate(index);
    }
    drain_scheduler(context.event_scheduler());
    for (std::size_t index = 0; index < fixture.writes.size(); ++index) {
        auto& writing = fixture.writes[index];
        const auto expected = fixture.write_outcomes[index];
        if (expected == errc::unavailable) {
            if (writing.available()) {
                return false;
            }
            continue;
        }
        if (!writing.available()) {
            return false;
        }
        const auto written = std::move(writing).get();
        if ((written ? errc::success : written.error().code()) != expected) {
            return false;
        }
        context.terminal_integer(static_cast<std::uint32_t>(expected));
    }
    for (std::size_t index = 0; index < fixture.flow_count; ++index) {
        const auto bytes = fixture.oracles[index].snapshot().visible[1];
        fixture.expected[index] = bytes.substr(fixture.consumed[index]);
        if (fixture.actions[index] == runtime::fault_action::corrupt) {
            const auto coordinate = random_coordinate::make(
              random_domain::fault_decision,
              network_fault_rule_id + 2U * index,
              2);
            if (!coordinate) {
                return false;
            }
            auto random = deterministic_random{context.seed()}.cursor(
              *coordinate, 0);
            const auto byte = runtime::uniform_u64(random, 4);
            const auto bit = runtime::uniform_u64(random, 8);
            if (!byte || !bit || fixture.expected[index].size() != 4) {
                return false;
            }
            fixture.expected[index][*byte] ^= static_cast<char>(
              std::uint8_t{1} << *bit);
        }
    }
    return true;
}

[[nodiscard]] bool
finish_network_streams(fuzz_case_context& context, network_fixture& fixture) {
    for (std::size_t index = 0; index < fixture.flow_count; ++index) {
        if (fixture.actions[index] == runtime::fault_action::disconnect) {
            continue;
        }
        if (!fixture.clients[index].shutdown_output()) {
            return false;
        }
        context.command_completed();
    }
    for (std::size_t index = 0; index < fixture.flow_count; ++index) {
        if (fixture.actions[index] == runtime::fault_action::disconnect) {
            const auto read = wait_for(
              context.event_scheduler(),
              fixture.servers[index].read(byte_count{1}, fixture.read_abort));
            if (read || read.error().code() != errc::network_failure) {
                return false;
            }
        } else if (!network_stream_is_exact(
                     context.event_scheduler(),
                     fixture.servers[index],
                     fixture.expected[index],
                     fixture.read_abort)) {
            return false;
        }
        context.terminal_integer(
          static_cast<std::uint8_t>(fixture.actions[index]));
        context.terminal_integer(fixture.expected[index].size());
        context.terminal_bytes(
          std::span{
            reinterpret_cast<const std::uint8_t*>(
              fixture.expected[index].data()),
            fixture.expected[index].size()});
        cooperate(index);
    }
    return true;
}

[[nodiscard]] bool
close_network(fuzz_case_context& context, network_fixture& fixture) {
    bool clean = true;
    std::vector<seastar::future<runtime::result<void>>> closing;
    closing.reserve(
      fixture.clients.size() + fixture.servers.size()
      + fixture.listeners.size());
    for (auto& connection : fixture.clients) {
        closing.push_back(connection.close());
    }
    for (auto& connection : fixture.servers) {
        closing.push_back(connection.close());
    }
    for (auto& listener : fixture.listeners) {
        closing.push_back(listener.close());
    }
    for (auto& future : closing) {
        clean
          = wait_for(context.event_scheduler(), std::move(future)).has_value()
            && clean;
    }
    const auto stopped = wait_for(
      context.event_scheduler(), fixture.network->stop());
    for (std::size_t index = 0; index < fixture.writes.size(); ++index) {
        if (fixture.write_outcomes[index] != errc::unavailable) {
            continue;
        }
        auto& writing = fixture.writes[index];
        if (!writing.available()) {
            clean = false;
            continue;
        }
        const auto result = std::move(writing).get();
        clean = !result && result.error().code() == errc::aborted && clean;
    }
    if (stopped && !fixture.clients.empty()) {
        const auto closed = wait_for(
          context.event_scheduler(),
          fixture.clients.front().write(
            network_payload(1, 'q'), fixture.write_abort));
        clean = !closed && closed.error().code() == errc::closed && clean;
    }
    fixture.writes.clear();
    fixture.write_outcomes.clear();
    fixture.clients.clear();
    fixture.servers.clear();
    fixture.listeners.clear();
    return clean && stopped.has_value()
           && fixture.network->active_operations() == 0
           && context.event_scheduler().pending_events() == 0;
}

[[nodiscard]] fuzz_case_outcome run_network_case(
  fuzz_case_context& context, std::span<const std::uint8_t> input) {
    input_cursor cursor{input};
    constexpr std::array counts{1U, 8U, 32U, 96U};
    const auto count = counts[cursor.byte() % counts.size()];
    const auto options = cursor.byte();
    const auto action = cursor.byte();
    network_fixture fixture{
      count, action, (options & 1U) != 0, (options & 2U) != 0};
    return with_cleanup(
      [&] {
          if (
            !open_network(context, fixture)
            || !run_concurrent_transfers(context, fixture, cursor)
            || !run_network_faults(context, fixture)
            || !finish_network_streams(context, fixture)) {
              return mismatch(runtime::operation_kind::network);
          }
          context.terminal_integer(action);
          context.terminal_integer(
            static_cast<std::uint8_t>(fixture.many_to_one));
          return close_network(context, fixture)
                   ? success()
                   : mismatch(runtime::operation_kind::network);
      },
      [&] {
          if (
            fixture.network
            && fixture.network->state() != fake_network_state::stopped
            && !abort_network(context, fixture)) {
              throw std::runtime_error("fuzz network cleanup failed");
          }
      });
}

[[nodiscard]] bool reproductions_equal(
  const fuzz_reproduction& left, const fuzz_reproduction& right) {
    return left.harness() == right.harness()
           && left.harness_version() == right.harness_version()
           && left.master_seed() == right.master_seed()
           && left.event_epoch() == right.event_epoch()
           && std::equal(
             left.configuration().begin(),
             left.configuration().end(),
             right.configuration().begin(),
             right.configuration().end())
           && std::equal(
             left.input().begin(),
             left.input().end(),
             right.input().begin(),
             right.input().end())
           && left.configuration_digest() == right.configuration_digest()
           && left.input_digest() == right.input_digest()
           && left.terminal_digest() == right.terminal_digest()
           && left.outcome() == right.outcome() && left.trace() == right.trace()
           && left.events() == right.events();
}

[[nodiscard]] bool is_replay_divergence(
  const std::exception_ptr& failure, std::size_t remaining_depth = 8) {
    if (!failure || remaining_depth == 0) {
        return false;
    }
    try {
        std::rethrow_exception(failure);
    } catch (const std::system_error& error) {
        return error.code() == make_error_code(errc::replay_divergence);
    } catch (const seastar::nested_exception& error) {
        return is_replay_divergence(error.inner, remaining_depth - 1)
               && is_replay_divergence(error.outer, remaining_depth - 1);
    } catch (...) {
        return false;
    }
}

[[nodiscard]] runtime::result<fuzz_reproduction> execute_case(
  fuzz_case_context& context,
  fuzz_harness harness,
  std::span<const std::uint8_t> input) {
    fuzz_case_outcome outcome;
    try {
        switch (harness) {
        case fuzz_harness::scheduler:
            outcome = run_scheduler_case(context, input);
            break;
        case fuzz_harness::fault_schedule:
            outcome = run_fault_case(context, input);
            break;
        case fuzz_harness::fake_file:
            outcome = run_file_case(context, input);
            break;
        case fuzz_harness::fake_network:
            outcome = run_network_case(context, input);
            break;
        case fuzz_harness::semantic_canary:
            context.terminal_integer(UINT64_C(0x43414e415259));
            outcome = mismatch(runtime::operation_kind::trace);
            break;
        default:
            return runtime::failure(case_error(errc::invalid_argument));
        }
    } catch (...) {
        // Owners unwind and drain before returning the sticky, fully typed
        // trace error. An unrelated exception must remain an unexpected
        // failure.
        const auto* failure = context.trace().failure();
        if (
          failure != nullptr
          && is_replay_divergence(std::current_exception())) {
            return runtime::failure(*failure);
        }
        throw;
    }
    return context.finish(outcome);
}

} // namespace

runtime::result<fuzz_reproduction>
execute_fuzz_case(fuzz_harness harness, std::span<const std::uint8_t> input) {
    if (input.size() > fuzz_input_bytes_max) {
        return runtime::failure(case_error(errc::out_of_range));
    }
    const auto header = case_trace_header(harness, input);
    event_trace trace{header, case_trace_limits(harness)};
    event_log_sink events{
      case_identity(harness, header.configuration_digest), case_event_limits()};
    fuzz_case_context context{harness, input, trace, events};
    return execute_case(context, harness, input);
}

runtime::result<void> replay_fuzz_case(const fuzz_reproduction& expected) {
    auto decoded_trace
      = decode_fuzz_trace(expected.trace(), expected.harness()).get();
    if (!decoded_trace) {
        return runtime::failure(decoded_trace.error());
    }
    auto decoded_events = observability::event_log::decode(
      expected.events(), case_event_limits());
    if (!decoded_events) {
        return runtime::failure(decoded_events.error());
    }
    const auto header = case_trace_header(expected.harness(), expected.input());
    auto trace = event_trace::replay(
      header, case_trace_limits(expected.harness()), std::move(*decoded_trace));
    if (!trace) {
        return runtime::failure(trace.error());
    }
    auto events = event_log_sink::replay(
      case_identity(expected.harness(), header.configuration_digest),
      case_event_limits(),
      std::move(*decoded_events));
    if (!events) {
        return runtime::failure(events.error());
    }
    fuzz_case_context context{
      expected.harness(), expected.input(), **trace, **events};
    auto actual = execute_case(context, expected.harness(), expected.input());
    if (!actual) {
        return runtime::failure(actual.error());
    }
    if (!reproductions_equal(*actual, expected)) {
        return runtime::failure(
          runtime::operation_error{
            errc::replay_divergence, runtime::operation_kind::trace});
    }
    return {};
}

void run_fuzz_case(
  fuzz_harness harness, const std::uint8_t* data, std::size_t size) {
    if (size > fuzz_input_bytes_max) {
        return;
    }
    std::vector<std::uint8_t> input;
    if (size != 0) {
        input.assign(data, data + size);
    }
    runtime::testing::run_fuzz_input(
      [harness, input = std::move(input)] mutable {
          crypto_thread_cleanup cleanup;
          auto result = execute_fuzz_case(harness, input);
          if (!result) {
              throw std::system_error(make_error_code(result.error().code()));
          }
          if (result->outcome().code != errc::success) {
              report_fuzz_failure(std::move(*result));
          }
      });
}

[[noreturn]] void report_fuzz_failure(fuzz_reproduction value) {
    static_cast<void>(write_fuzz_reproduction(std::cerr, value));
    std::cerr.flush();
    __builtin_trap();
}

} // namespace kwaque::simulation::testing
