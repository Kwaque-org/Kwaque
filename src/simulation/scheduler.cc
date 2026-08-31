#include "src/simulation/scheduler.h"

#include "src/base/invariant.h"

#include <algorithm>
#include <bit>
#include <limits>
#include <utility>

namespace kwaque::simulation {

namespace {

constexpr invariant_id scheduler_drained_invariant{"KQ-SCHEDULER-DRAINED"};
constexpr invariant_id scheduler_index_invariant{"KQ-SCHEDULER-INDEX"};
constexpr invariant_id scheduler_reservation_invariant{
  "KQ-SCHEDULER-ID-RESERVATION"};
constexpr invariant_id scheduler_cleanup_invariant{"KQ-SCHEDULER-CLEANUP"};
constexpr invariant_id scheduler_trace_invariant{"KQ-SCHEDULER-TRACE"};
constexpr invariant_id scheduler_storage_invariant{"KQ-SCHEDULER-STORAGE"};
constexpr std::size_t scheduler_heap_root_index{1};
constexpr std::uint64_t scheduler_sentinel_event_id{0};

[[nodiscard]] runtime::operation_error scheduler_error(errc code) noexcept {
    return runtime::operation_error{code, runtime::operation_kind::scheduler};
}

[[nodiscard]] runtime::result<scheduler_limits>
invalid_limits(errc code) noexcept {
    return runtime::failure(scheduler_error(code));
}

} // namespace

scheduler::event_storage::event_storage(std::size_t capacity)
  : capacity_(capacity) {
    auto remaining = capacity;
    while (remaining != 0) {
        const auto count = std::min(remaining, entries_per_chunk);
        chunks_.emplace_back(count);
        remaining -= count;
    }
}

scheduler::event&
scheduler::event_storage::operator[](std::size_t index) noexcept {
    KWAQUE_INVARIANT(
      scheduler_storage_invariant,
      index < size_,
      "scheduler event storage index out of range");
    return *chunks_[index / entries_per_chunk][index % entries_per_chunk];
}

const scheduler::event&
scheduler::event_storage::operator[](std::size_t index) const noexcept {
    KWAQUE_INVARIANT(
      scheduler_storage_invariant,
      index < size_,
      "scheduler event storage index out of range");
    return *chunks_[index / entries_per_chunk][index % entries_per_chunk];
}

void scheduler::event_storage::push_back(event value) noexcept {
    KWAQUE_INVARIANT(
      scheduler_storage_invariant,
      size_ < capacity_,
      "scheduler event storage exceeded its fixed capacity");
    chunks_[size_ / entries_per_chunk][size_ % entries_per_chunk].emplace(
      std::move(value));
    ++size_;
}

void scheduler::event_storage::pop_back() noexcept {
    KWAQUE_INVARIANT(
      scheduler_storage_invariant,
      size_ != 0,
      "scheduler event storage popped while empty");
    --size_;
    chunks_[size_ / entries_per_chunk][size_ % entries_per_chunk].reset();
}

scheduler::event_index::event_index(std::size_t maximum_entries) {
    const auto requested = maximum_entries + maximum_entries / 2U + 1U;
    capacity_ = std::bit_ceil(std::max<std::size_t>(requested, 2U));
    mask_ = capacity_ - 1U;
    auto remaining = capacity_;
    while (remaining != 0) {
        const auto count = std::min(remaining, entries_per_chunk);
        chunks_.emplace_back(count);
        remaining -= count;
    }
}

std::uint64_t scheduler::event_index::hash(std::uint64_t key) noexcept {
    key += UINT64_C(0x9e3779b97f4a7c15);
    key = (key ^ (key >> 30U)) * UINT64_C(0xbf58476d1ce4e5b9);
    key = (key ^ (key >> 27U)) * UINT64_C(0x94d049bb133111eb);
    return key ^ (key >> 31U);
}

std::size_t scheduler::event_index::bucket(std::uint64_t key) const noexcept {
    return static_cast<std::size_t>(hash(key)) & mask_;
}

scheduler::event_index::slot&
scheduler::event_index::slot_at(std::size_t index) noexcept {
    return chunks_[index / entries_per_chunk][index % entries_per_chunk];
}

const scheduler::event_index::slot&
scheduler::event_index::slot_at(std::size_t index) const noexcept {
    return chunks_[index / entries_per_chunk][index % entries_per_chunk];
}

std::size_t* scheduler::event_index::find(std::uint64_t key) noexcept {
    auto position = bucket(key);
    std::uint32_t distance = 1;
    for (std::size_t probed = 0; probed < capacity_; ++probed) {
        auto& candidate = slot_at(position);
        if (candidate.distance < distance) {
            return nullptr;
        }
        if (candidate.key == key) {
            return &candidate.value;
        }
        ++distance;
        position = (position + 1U) & mask_;
    }
    return nullptr;
}

const std::size_t*
scheduler::event_index::find(std::uint64_t key) const noexcept {
    auto position = bucket(key);
    std::uint32_t distance = 1;
    for (std::size_t probed = 0; probed < capacity_; ++probed) {
        const auto& candidate = slot_at(position);
        if (candidate.distance < distance) {
            return nullptr;
        }
        if (candidate.key == key) {
            return &candidate.value;
        }
        ++distance;
        position = (position + 1U) & mask_;
    }
    return nullptr;
}

std::size_t& scheduler::event_index::at(std::uint64_t key) noexcept {
    auto* found = find(key);
    KWAQUE_INVARIANT(
      scheduler_index_invariant,
      found != nullptr,
      "scheduler event index lost a live event");
    return *found;
}

bool scheduler::event_index::try_emplace(
  std::uint64_t key, std::size_t value) noexcept {
    slot incoming{
      .key = key,
      .value = value,
      .distance = 1,
    };
    bool seeking_key = true;
    auto position = bucket(key);
    for (std::size_t probed = 0; probed < capacity_; ++probed) {
        auto& candidate = slot_at(position);
        if (candidate.distance == 0) {
            candidate = incoming;
            ++size_;
            return true;
        }
        if (seeking_key && candidate.key == key) {
            return false;
        }
        if (incoming.distance > candidate.distance) {
            std::swap(incoming, candidate);
            seeking_key = false;
        }
        ++incoming.distance;
        position = (position + 1U) & mask_;
    }
    KWAQUE_INVARIANT(
      scheduler_index_invariant,
      false,
      "scheduler event index exceeded its fixed capacity");
    return false;
}

bool scheduler::event_index::erase(std::uint64_t key) noexcept {
    auto position = bucket(key);
    std::uint32_t distance = 1;
    for (std::size_t probed = 0; probed < capacity_; ++probed) {
        const auto& candidate = slot_at(position);
        if (candidate.distance < distance) {
            return false;
        }
        if (candidate.key == key) {
            auto hole = position;
            auto next = (hole + 1U) & mask_;
            while (slot_at(next).distance > 1) {
                slot_at(hole) = slot_at(next);
                --slot_at(hole).distance;
                hole = next;
                next = (next + 1U) & mask_;
            }
            slot_at(hole) = slot{};
            --size_;
            return true;
        }
        ++distance;
        position = (position + 1U) & mask_;
    }
    return false;
}

runtime::result<scheduler_limits>
scheduler_limits::make(scheduler_limit_values values) noexcept {
    if (
      values.pending_events == 0 || values.events_per_pump == 0
      || values.total_events == 0 || values.maximum_deadline.nanoseconds() == 0
      || values.events_per_pump > values.total_events) {
        return invalid_limits(errc::invalid_argument);
    }
    if (
      values.pending_events > pending_events_absolute
      || values.events_per_pump > events_per_pump_absolute
      || values.total_events > total_events_absolute
      || values.maximum_deadline > maximum_deadline_absolute) {
        return invalid_limits(errc::out_of_range);
    }
    return scheduler_limits{values};
}

scheduler::scheduler(scheduler_limits limits, event_trace* trace)
  : limits_(limits)
  , trace_(trace)
  , heap_(static_cast<std::size_t>(limits.pending_events()) + 1U)
  , indices_(static_cast<std::size_t>(limits.pending_events()) + 1U) {
    heap_.push_back(event{});
    const auto inserted = indices_.try_emplace(scheduler_sentinel_event_id, 0U);
    KWAQUE_INVARIANT(
      scheduler_index_invariant,
      inserted && indices_.at(scheduler_sentinel_event_id) == 0U,
      "scheduler sentinel insertion failed");
    KWAQUE_INVARIANT(
      scheduler_trace_invariant,
      trace_ == nullptr
        || trace_->header().scheduler_budget == trace_budget(limits_),
      "scheduler and trace budgets differ");
    if (trace_ != nullptr) {
        trace_->attach_scheduler();
    }
}

scheduler::event_id_reservation::~event_id_reservation() { release(); }

scheduler::event_id_reservation::event_id_reservation(
  event_id_reservation&& other) noexcept
  : owner_(std::exchange(other.owner_, nullptr)) {}

scheduler::event_id_reservation& scheduler::event_id_reservation::operator=(
  event_id_reservation&& other) noexcept {
    if (this != &other) {
        release();
        owner_ = std::exchange(other.owner_, nullptr);
    }
    return *this;
}

void scheduler::event_id_reservation::release() noexcept {
    if (owner_ == nullptr) {
        return;
    }
    auto* owner = std::exchange(owner_, nullptr);
    owner->release_reserved_event_id();
}

scheduler::~scheduler() {
    assert_current();
    if (trace_failed()) {
        for (std::size_t index = scheduler_heap_root_index;
             index < heap_.size();
             ++index) {
            KWAQUE_INVARIANT(
              scheduler_cleanup_invariant,
              heap_[index].cleanup != event_cleanup_policy::invoke,
              "failed scheduler destroyed before owner cleanup");
        }
        while (heap_.size() > scheduler_heap_root_index) {
            static_cast<void>(remove_at(heap_.size() - 1U));
        }
    }
    const auto* sentinel = indices_.find(scheduler_sentinel_event_id);
    KWAQUE_INVARIANT(
      scheduler_drained_invariant,
      heap_.size() == scheduler_heap_root_index && indices_.size() == 1U
        && sentinel != nullptr && *sentinel == 0U && reserved_event_ids_ == 0
        && !pumping_ && !discarding_failed_event_,
      "scheduler destroyed with pending work");
    if (trace_ != nullptr) {
        trace_->detach_scheduler();
    }
}

runtime::result<event_id> scheduler::schedule(
  runtime::monotonic_time deadline,
  event_priority priority,
  callback&& completion,
  trace_event_descriptor descriptor,
  event_cleanup_policy cleanup,
  event_trace::reservation trace_reservation) {
    assert_current();
    if (!completion) {
        return runtime::failure(scheduler_error(errc::invalid_argument));
    }
    if (
      auto available = can_schedule(deadline, descriptor, cleanup);
      !available) {
        return runtime::failure(available.error());
    }
    if (trace_ == nullptr && trace_reservation.active()) {
        return runtime::failure(scheduler_error(errc::invalid_argument));
    }
    if (trace_ != nullptr) {
        const std::uint32_t entries = descriptor.effect == trace_action::none
                                        ? 2U
                                        : 3U;
        if (
          trace_reservation.active()
          && trace_reservation.entries() != entries) {
            return runtime::failure(
              runtime::operation_error{
                errc::invalid_argument, runtime::operation_kind::trace});
        }
    }

    const event_id id{next_event_id_};
    if (trace_ != nullptr) {
        if (!trace_reservation.active()) {
            auto reserved = reserve_trace(descriptor);
            if (!reserved) {
                return runtime::failure(reserved.error());
            }
            trace_reservation = std::move(*reserved);
        }
        event pending{
          .deadline = deadline,
          .priority = priority,
          .cleanup = cleanup,
          .id = id,
          .completion = callback{},
          .descriptor = descriptor,
          .trace_reservation = {},
        };
        if (
          auto observed = observe_event(
            trace_action::scheduled, pending, &trace_reservation);
          !observed) {
            return runtime::failure(observed.error());
        }
    }
    heap_.push_back(
      event{
        .deadline = deadline,
        .priority = priority,
        .cleanup = cleanup,
        .id = id,
        .completion = std::move(completion),
        .descriptor = descriptor,
        .trace_reservation = std::move(trace_reservation),
      });
    const auto inserted = indices_.try_emplace(id.value(), heap_.size() - 1U);
    KWAQUE_INVARIANT(
      scheduler_index_invariant, inserted, "duplicate scheduler event id");
    sift_up(heap_.size() - 1);

    if (next_event_id_ == std::numeric_limits<std::uint64_t>::max()) {
        event_ids_exhausted_ = true;
    } else {
        ++next_event_id_;
    }
    return id;
}

runtime::result<void> scheduler::can_schedule(
  runtime::monotonic_time deadline,
  trace_event_descriptor descriptor,
  event_cleanup_policy cleanup) const noexcept {
    assert_current();
    if (auto healthy = check_trace_failure(); !healthy) {
        return runtime::failure(healthy.error());
    }
    if (auto valid = validate_descriptor(descriptor, cleanup); !valid) {
        return runtime::failure(valid.error());
    }
    if (deadline < now_) {
        auto error = scheduler_error(errc::invalid_argument);
        static_cast<void>(error.add_context(
          runtime::operation_context_key::deadline_ns, deadline.nanoseconds()));
        static_cast<void>(error.add_context(
          runtime::operation_context_key::limit, now_.nanoseconds()));
        return runtime::failure(std::move(error));
    }
    if (deadline > limits_.maximum_deadline()) {
        auto error = scheduler_error(errc::out_of_range);
        static_cast<void>(error.add_context(
          runtime::operation_context_key::deadline_ns, deadline.nanoseconds()));
        static_cast<void>(error.add_context(
          runtime::operation_context_key::limit,
          limits_.maximum_deadline().nanoseconds()));
        return runtime::failure(std::move(error));
    }
    if (pending_events() >= limits_.pending_events()) {
        auto error = scheduler_error(errc::queue_full);
        static_cast<void>(error.add_context(
          runtime::operation_context_key::items, pending_events()));
        static_cast<void>(error.add_context(
          runtime::operation_context_key::limit, limits_.pending_events()));
        return runtime::failure(std::move(error));
    }
    if (!event_id_available()) {
        auto error = scheduler_error(errc::out_of_range);
        static_cast<void>(error.add_context(
          runtime::operation_context_key::sequence,
          std::numeric_limits<std::uint64_t>::max()));
        return runtime::failure(std::move(error));
    }
    return {};
}

runtime::result<scheduler::event_id_reservation> scheduler::reserve_event_id() {
    assert_current();
    if (auto healthy = check_trace_failure(); !healthy) {
        return runtime::failure(healthy.error());
    }
    if (!event_id_available()) {
        auto error = scheduler_error(errc::out_of_range);
        static_cast<void>(error.add_context(
          runtime::operation_context_key::sequence,
          std::numeric_limits<std::uint64_t>::max()));
        return runtime::failure(std::move(error));
    }
    ++reserved_event_ids_;
    return event_id_reservation{*this};
}

runtime::result<event_trace::reservation>
scheduler::reserve_trace(trace_event_descriptor descriptor) {
    assert_current();
    if (auto healthy = check_trace_failure(); !healthy) {
        return runtime::failure(healthy.error());
    }
    if (
      auto valid = validate_descriptor(
        descriptor,
        descriptor.kind == trace_event_kind::timer
            || descriptor.kind == trace_event_kind::wall_adjustment
            || descriptor.kind == trace_event_kind::file
            || descriptor.kind == trace_event_kind::filesystem
          ? event_cleanup_policy::invoke
          : event_cleanup_policy::drop);
      !valid) {
        return runtime::failure(valid.error());
    }
    if (trace_ == nullptr) {
        return event_trace::reservation{};
    }
    const std::uint32_t entries = descriptor.effect == trace_action::none ? 2U
                                                                          : 3U;
    return trace_->reserve(
      entries,
      static_cast<std::uint64_t>(entries) * canonical_entry_encoded_size);
}

runtime::result<bool> scheduler::cancel(event_id id) noexcept {
    assert_current();
    if (auto healthy = check_trace_failure(); !healthy) {
        return runtime::failure(healthy.error());
    }
    if (!id.valid()) {
        return false;
    }
    const auto* found = indices_.find(id.value());
    if (found == nullptr) {
        return false;
    }
    auto& selected = heap_[*found];
    if (
      auto observed = observe_event(
        trace_action::canceled,
        selected,
        selected.trace_reservation.active() ? &selected.trace_reservation
                                            : nullptr);
      !observed) {
        return runtime::failure(observed.error());
    }
    static_cast<void>(remove_at(*found));
    return true;
}

runtime::result<bool> scheduler::step() {
    assert_current();
    if (auto healthy = check_trace_failure(); !healthy) {
        return runtime::failure(healthy.error());
    }
    if (auto allowed = check_control_entry(); !allowed) {
        return runtime::failure(allowed.error());
    }
    pump_scope scope{pumping_};
    if (!has_ready_event()) {
        return false;
    }
    if (auto available = check_lifetime_budget(); !available) {
        return runtime::failure(available.error());
    }
    if (auto executed = execute_ready_unchecked(); !executed) {
        return runtime::failure(executed.error());
    }
    return true;
}

runtime::result<std::uint64_t> scheduler::run_ready() {
    assert_current();
    if (auto healthy = check_trace_failure(); !healthy) {
        return runtime::failure(healthy.error());
    }
    if (auto allowed = check_control_entry(); !allowed) {
        return runtime::failure(allowed.error());
    }
    pump_scope scope{pumping_};

    std::uint64_t executed = 0;
    while (has_ready_event()) {
        if (executed == limits_.events_per_pump()) {
            return runtime::failure(
              limit_error(executed, limits_.events_per_pump()));
        }
        if (auto available = check_lifetime_budget(); !available) {
            return runtime::failure(available.error());
        }
        if (auto completed = execute_ready_unchecked(); !completed) {
            return runtime::failure(completed.error());
        }
        ++executed;
    }
    return executed;
}

runtime::result<std::uint64_t>
scheduler::run_ready_batch(std::uint64_t maximum_events) {
    assert_current();
    if (maximum_events == 0) {
        return runtime::failure(scheduler_error(errc::invalid_argument));
    }
    if (maximum_events > limits_.events_per_pump()) {
        return runtime::failure(
          limit_error(maximum_events, limits_.events_per_pump()));
    }
    if (auto healthy = check_trace_failure(); !healthy) {
        return runtime::failure(healthy.error());
    }
    if (auto allowed = check_control_entry(); !allowed) {
        return runtime::failure(allowed.error());
    }
    pump_scope scope{pumping_};

    std::uint64_t executed = 0;
    while (has_ready_event() && executed < maximum_events) {
        if (auto available = check_lifetime_budget(); !available) {
            return runtime::failure(available.error());
        }
        if (auto completed = execute_ready_unchecked(); !completed) {
            return runtime::failure(completed.error());
        }
        ++executed;
    }
    return executed;
}

runtime::result<std::optional<runtime::monotonic_time>>
scheduler::advance_to_next() {
    assert_current();
    if (auto healthy = check_trace_failure(); !healthy) {
        return runtime::failure(healthy.error());
    }
    if (auto allowed = check_control_entry(); !allowed) {
        return runtime::failure(allowed.error());
    }
    if (heap_.size() == scheduler_heap_root_index) {
        return std::optional<runtime::monotonic_time>{};
    }
    if (has_ready_event()) {
        return runtime::failure(scheduler_error(errc::unavailable));
    }
    if (
      auto observed = observe_time_advance(
        heap_[scheduler_heap_root_index].deadline);
      !observed) {
        return runtime::failure(observed.error());
    }
    now_ = heap_[scheduler_heap_root_index].deadline;
    return std::optional<runtime::monotonic_time>{now_};
}

runtime::result<std::uint64_t>
scheduler::run_until(runtime::monotonic_time target) {
    assert_current();
    if (auto healthy = check_trace_failure(); !healthy) {
        return runtime::failure(healthy.error());
    }
    if (auto allowed = check_control_entry(); !allowed) {
        return runtime::failure(allowed.error());
    }
    if (target < now_) {
        auto error = scheduler_error(errc::invalid_argument);
        static_cast<void>(error.add_context(
          runtime::operation_context_key::deadline_ns, target.nanoseconds()));
        static_cast<void>(error.add_context(
          runtime::operation_context_key::limit, now_.nanoseconds()));
        return runtime::failure(std::move(error));
    }
    if (target > limits_.maximum_deadline()) {
        auto error = scheduler_error(errc::out_of_range);
        static_cast<void>(error.add_context(
          runtime::operation_context_key::deadline_ns, target.nanoseconds()));
        static_cast<void>(error.add_context(
          runtime::operation_context_key::limit,
          limits_.maximum_deadline().nanoseconds()));
        return runtime::failure(std::move(error));
    }

    pump_scope scope{pumping_};
    std::uint64_t executed = 0;
    while (heap_.size() > scheduler_heap_root_index
           && heap_[scheduler_heap_root_index].deadline <= target) {
        if (executed == limits_.events_per_pump()) {
            return runtime::failure(
              limit_error(executed, limits_.events_per_pump()));
        }
        if (auto available = check_lifetime_budget(); !available) {
            return runtime::failure(available.error());
        }
        if (heap_[scheduler_heap_root_index].deadline != now_) {
            if (
              auto observed = observe_time_advance(
                heap_[scheduler_heap_root_index].deadline);
              !observed) {
                return runtime::failure(observed.error());
            }
            now_ = heap_[scheduler_heap_root_index].deadline;
        }
        if (auto completed = execute_ready_unchecked(); !completed) {
            return runtime::failure(completed.error());
        }
        ++executed;
    }
    if (target != now_) {
        if (auto observed = observe_time_advance(target); !observed) {
            return runtime::failure(observed.error());
        }
        now_ = target;
    }
    return executed;
}

runtime::result<std::uint64_t> scheduler::run_until_batch(
  runtime::monotonic_time target, std::uint64_t maximum_events) {
    assert_current();
    if (maximum_events == 0) {
        return runtime::failure(scheduler_error(errc::invalid_argument));
    }
    if (maximum_events > limits_.events_per_pump()) {
        return runtime::failure(
          limit_error(maximum_events, limits_.events_per_pump()));
    }
    if (auto healthy = check_trace_failure(); !healthy) {
        return runtime::failure(healthy.error());
    }
    if (auto allowed = check_control_entry(); !allowed) {
        return runtime::failure(allowed.error());
    }
    if (target < now_) {
        auto error = scheduler_error(errc::invalid_argument);
        static_cast<void>(error.add_context(
          runtime::operation_context_key::deadline_ns, target.nanoseconds()));
        static_cast<void>(error.add_context(
          runtime::operation_context_key::limit, now_.nanoseconds()));
        return runtime::failure(std::move(error));
    }
    if (target > limits_.maximum_deadline()) {
        auto error = scheduler_error(errc::out_of_range);
        static_cast<void>(error.add_context(
          runtime::operation_context_key::deadline_ns, target.nanoseconds()));
        static_cast<void>(error.add_context(
          runtime::operation_context_key::limit,
          limits_.maximum_deadline().nanoseconds()));
        return runtime::failure(std::move(error));
    }

    pump_scope scope{pumping_};
    std::uint64_t executed = 0;
    while (heap_.size() > scheduler_heap_root_index
           && heap_[scheduler_heap_root_index].deadline <= target
           && executed < maximum_events) {
        if (auto available = check_lifetime_budget(); !available) {
            return runtime::failure(available.error());
        }
        if (heap_[scheduler_heap_root_index].deadline != now_) {
            if (
              auto observed = observe_time_advance(
                heap_[scheduler_heap_root_index].deadline);
              !observed) {
                return runtime::failure(observed.error());
            }
            now_ = heap_[scheduler_heap_root_index].deadline;
        }
        if (auto completed = execute_ready_unchecked(); !completed) {
            return runtime::failure(completed.error());
        }
        ++executed;
    }
    const bool work_remains = heap_.size() > scheduler_heap_root_index
                              && heap_[scheduler_heap_root_index].deadline
                                   <= target;
    if (!work_remains && target != now_) {
        if (auto observed = observe_time_advance(target); !observed) {
            return runtime::failure(observed.error());
        }
        now_ = target;
    }
    return executed;
}

bool scheduler::discard_failed() noexcept {
    assert_current();
    if (!trace_failed() || pumping_ || discarding_failed_event_) [[unlikely]] {
        return false;
    }
    while (heap_.size() > scheduler_heap_root_index) {
        auto discarded = remove_at(scheduler_heap_root_index);
        if (discarded.cleanup == event_cleanup_policy::invoke) {
            discard_scope scope{discarding_failed_event_};
            discarded.completion();
        }
    }
    return true;
}

runtime::monotonic_time scheduler::now() const {
    assert_current();
    return now_;
}

std::size_t scheduler::pending_events() const {
    assert_current();
    return heap_.size() - scheduler_heap_root_index;
}

bool scheduler::has_ready_events() const {
    assert_current();
    return has_ready_event();
}

std::uint64_t scheduler::executed_events() const {
    assert_current();
    return executed_events_;
}

bool scheduler::trace_failed() const {
    assert_current();
    return trace_ != nullptr && trace_->failed();
}

const runtime::operation_error* scheduler::trace_failure() const {
    assert_current();
    return trace_ == nullptr ? nullptr : trace_->failure();
}

bool scheduler::uses_trace(const event_trace& trace) const {
    assert_current();
    return trace_ == &trace;
}

bool scheduler::discarding_failed_event() const {
    assert_current();
    return discarding_failed_event_;
}

bool scheduler::earlier(const event& left, const event& right) noexcept {
    if (left.deadline != right.deadline) {
        return left.deadline < right.deadline;
    }
    if (left.priority != right.priority) {
        return left.priority < right.priority;
    }
    return left.id < right.id;
}

void scheduler::swap_events(std::size_t left, std::size_t right) noexcept {
    if (left == right) {
        return;
    }
    std::swap(heap_[left], heap_[right]);
    indices_.at(heap_[left].id.value()) = left;
    indices_.at(heap_[right].id.value()) = right;
}

void scheduler::sift_up(std::size_t index) noexcept {
    while (index > scheduler_heap_root_index) {
        const auto parent = index / 2U;
        if (!earlier(heap_[index], heap_[parent])) {
            break;
        }
        swap_events(index, parent);
        index = parent;
    }
}

void scheduler::sift_down(std::size_t index) noexcept {
    while (true) {
        const auto left = index * 2U;
        if (left >= heap_.size()) {
            return;
        }
        const auto right = left + 1;
        auto selected = left;
        if (right < heap_.size() && earlier(heap_[right], heap_[left])) {
            selected = right;
        }
        if (!earlier(heap_[selected], heap_[index])) {
            return;
        }
        swap_events(index, selected);
        index = selected;
    }
}

scheduler::event scheduler::remove_at(std::size_t index) noexcept {
    KWAQUE_INVARIANT(
      scheduler_index_invariant,
      index >= scheduler_heap_root_index && index < heap_.size(),
      "scheduler heap index out of range");
    const auto removed_id = heap_[index].id.value();
    const auto last = heap_.size() - 1;
    if (index != last) {
        swap_events(index, last);
    }
    event removed = std::move(heap_.back());
    heap_.pop_back();
    const auto erased = indices_.erase(removed_id);
    KWAQUE_INVARIANT(
      scheduler_index_invariant,
      erased,
      "scheduler event index lost the removed event");

    if (index < heap_.size()) {
        if (
          index > scheduler_heap_root_index
          && earlier(heap_[index], heap_[index / 2U])) {
            sift_up(index);
        } else {
            sift_down(index);
        }
    }
    return removed;
}

bool scheduler::has_ready_event() const noexcept {
    return heap_.size() > scheduler_heap_root_index
           && heap_[scheduler_heap_root_index].deadline <= now_;
}

runtime::result<void> scheduler::execute_ready_unchecked() noexcept {
    auto& pending = heap_[scheduler_heap_root_index];
    auto* reservation = pending.trace_reservation.active()
                          ? &pending.trace_reservation
                          : nullptr;
    if (
      auto observed = observe_event(
        trace_action::selected, pending, reservation);
      !observed) {
        return runtime::failure(observed.error());
    }
    if (pending.descriptor.effect != trace_action::none) {
        if (
          auto observed = observe_event(
            pending.descriptor.effect, pending, reservation);
          !observed) {
            return runtime::failure(observed.error());
        }
    }
    auto selected = remove_at(scheduler_heap_root_index);
    ++executed_events_;
    selected.completion();
    if (auto healthy = check_trace_failure(); !healthy) {
        return runtime::failure(healthy.error());
    }
    return {};
}

runtime::result<void> scheduler::check_lifetime_budget() const {
    if (executed_events_ != limits_.total_events()) {
        return {};
    }
    return runtime::failure(
      limit_error(executed_events_, limits_.total_events()));
}

runtime::result<void> scheduler::check_control_entry() const {
    if (!pumping_) {
        return {};
    }
    return runtime::failure(scheduler_error(errc::unavailable));
}

runtime::result<void> scheduler::check_trace_failure() const {
    if (trace_ == nullptr || trace_->failure() == nullptr) [[likely]] {
        return {};
    }
    return runtime::failure(*trace_->failure());
}

runtime::result<void> scheduler::validate_descriptor(
  trace_event_descriptor descriptor,
  event_cleanup_policy cleanup) const noexcept {
    const auto kind = static_cast<std::uint8_t>(descriptor.kind);
    const auto effect = static_cast<std::uint8_t>(descriptor.effect);
    const auto cleanup_value = static_cast<std::uint8_t>(cleanup);
    if (
      kind > static_cast<std::uint8_t>(trace_event_kind::filesystem)
      || effect > static_cast<std::uint8_t>(trace_action::crash_applied)
      || cleanup_value
           > static_cast<std::uint8_t>(event_cleanup_policy::invoke)
      || descriptor.kind == trace_event_kind::keyed_random
      || descriptor.kind == trace_event_kind::fault
      || ((descriptor.kind == trace_event_kind::timer
           || descriptor.kind == trace_event_kind::wall_adjustment
           || descriptor.kind == trace_event_kind::file
           || descriptor.kind == trace_event_kind::filesystem)
          && cleanup != event_cleanup_policy::invoke)
      || (descriptor.kind == trace_event_kind::wall_adjustment
          && descriptor.effect != trace_action::wall_adjusted)
      || (descriptor.effect != trace_action::none
          && !(
            (descriptor.effect == trace_action::wall_adjusted
             && descriptor.kind == trace_event_kind::wall_adjustment)
            || (descriptor.effect == trace_action::operation_discarded
                && (descriptor.kind == trace_event_kind::file
                    || descriptor.kind == trace_event_kind::filesystem))
            || (descriptor.effect == trace_action::crash_applied
                && descriptor.kind == trace_event_kind::filesystem))))
      [[unlikely]] {
        return runtime::failure(scheduler_error(errc::invalid_argument));
    }
    return {};
}

bool scheduler::event_id_available() const noexcept {
    if (event_ids_exhausted_) {
        return false;
    }
    const auto remaining = std::numeric_limits<std::uint64_t>::max()
                           - next_event_id_ + 1U;
    return reserved_event_ids_ < remaining;
}

void scheduler::release_reserved_event_id() noexcept {
    assert_current();
    KWAQUE_INVARIANT(
      scheduler_reservation_invariant,
      reserved_event_ids_ != 0,
      "scheduler event-id reservation underflow");
    --reserved_event_ids_;
}

runtime::result<void> scheduler::observe_event(
  trace_action action,
  const event& selected,
  event_trace::reservation* reservation) noexcept {
    if (trace_ == nullptr) {
        return {};
    }
    return trace_->observe(
      trace_entry{
        .time = now_,
        .deadline = selected.deadline,
        .action = action,
        .kind = selected.descriptor.kind,
        .event_id = selected.id.value(),
        .priority = selected.priority.value(),
        .domain = selected.descriptor.domain,
        .stable_id = selected.descriptor.stable_id,
        .coordinate_a = selected.descriptor.coordinate_a,
        .coordinate_b = selected.descriptor.coordinate_b,
        .value = selected.descriptor.value,
        .result = selected.descriptor.result,
      },
      reservation);
}

runtime::result<void>
scheduler::observe_time_advance(runtime::monotonic_time target) noexcept {
    if (trace_ == nullptr) {
        return {};
    }
    return trace_->observe(
      trace_entry{
        .time = now_,
        .action = trace_action::time_advanced,
        .kind = trace_event_kind::generic,
        .coordinate_a = now_.nanoseconds(),
        .value = target.nanoseconds(),
      });
}

runtime::operation_error scheduler::limit_error(
  std::uint64_t observed, std::uint64_t limit) const noexcept {
    auto error = scheduler_error(errc::resource_exhausted);
    static_cast<void>(error.add_context(
      runtime::operation_context_key::deadline_ns, now_.nanoseconds()));
    if (heap_.size() > scheduler_heap_root_index) {
        static_cast<void>(error.add_context(
          runtime::operation_context_key::sequence,
          heap_[scheduler_heap_root_index].id.value()));
    }
    static_cast<void>(
      error.add_context(runtime::operation_context_key::items, observed));
    static_cast<void>(
      error.add_context(runtime::operation_context_key::limit, limit));
    return error;
}

} // namespace kwaque::simulation
