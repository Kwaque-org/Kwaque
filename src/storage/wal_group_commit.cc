#include "src/storage/wal_group_commit.h"

#include "src/base/invariant.h"

#include <seastar/core/abort_on_expiry.hh>
#include <seastar/core/abortable_fifo.hh>

#include <type_traits>

namespace kwaque::storage {
static_assert(std::is_nothrow_move_constructible_v<wal_submission>);
static_assert(std::is_nothrow_copy_constructible_v<wal_commit_ticket>);
static_assert(std::is_nothrow_copy_constructible_v<wal_flush_capture>);
static_assert(std::is_nothrow_copy_constructible_v<wal_commit_result>);
static_assert(std::is_nothrow_copy_constructible_v<runtime::first_failure>);
namespace {
runtime::operation_error commit_error(errc code) noexcept {
    return runtime::operation_error{code, runtime::operation_kind::file};
}

// Conservative layout bounds for the native shared-future peer storage.
// Include the timer even though this API does not install timed peers. Native
// FIFO chunks contain 128 entries; every potential chunk is admitted up front.
// These types describe allocation geometry only, not another wait queue.
template<typename Value>
struct shared_peer_value_layout final {
    seastar::promise<Value> promise;
    std::optional<seastar::abort_on_expiry<seastar::lowres_clock>> timer;
};
template<typename Value>
struct shared_peer_layout final {
    std::optional<shared_peer_value_layout<Value>> value;
    seastar::optimized_optional<seastar::abort_source::subscription>
      subscription;
};
static_assert(
  std::is_same_v<
    seastar::chunked_fifo<shared_peer_layout<wal_commit_result>>,
    seastar::chunked_fifo<shared_peer_layout<wal_commit_result>, 128, 0>>);
template<typename Value>
runtime::result<byte_count>
observer_storage(workload_budget& budget, std::uint32_t maximum) {
    // The layout entry is noncopyable. Model the empty, reference-taking
    // expiry callback; this queue type is used only for sizeof below.
    using peers = seastar::internal::abortable_fifo<
      shared_peer_value_layout<Value>,
      seastar::internal::noop_aborter<shared_peer_value_layout<Value>&>>;
    const auto state = budget.allocation_charge(
      byte_count{
        sizeof(seastar::task) + sizeof(seastar::future<Value>)
        + sizeof(seastar::lw_shared_ptr<detail::wal_commit_result_state>)
        + sizeof(peers) + 64});
    const auto front = budget.allocation_charge(
      byte_count{sizeof(shared_peer_layout<Value>) + 64});
    if (!state) return runtime::failure(state.error());
    if (!front) return runtime::failure(front.error());
    const auto chunks = (maximum - 1U + 127U) / 128U;
    if (chunks == 0) return byte_count{state->value() + front->value()};
    const auto chunk = budget.allocation_charge(
      byte_count{128U * sizeof(shared_peer_layout<Value>) + 64});
    if (!chunk) return runtime::failure(chunk.error());
    return byte_count{
      state->value() + front->value() + chunks * chunk->value()};
}
} // namespace

namespace detail {
wal_commit_lifetime::wal_commit_lifetime(
  workload_reservation held,
  std::uint32_t maximum,
  std::uint32_t maximum_observers)
  : held(std::move(held))
  , slots(maximum)
  , observers(maximum_observers)
  , maximum(maximum) {}
wal_commit_lifetime::~wal_commit_lifetime() { assert_current(); }
wal_commit_result_state::wal_commit_result_state(
  seastar::lw_shared_ptr<wal_commit_lifetime> lifetime,
  seastar::semaphore_units<> slot,
  seastar::semaphore_units<> observers,
  workload_reservation held)
  : lifetime(std::move(lifetime))
  , slot(std::move(slot))
  , observers(std::move(observers))
  , held(std::move(held)) {}
wal_commit_result_state::~wal_commit_result_state() { assert_current(); }
wal_commit_node::wal_commit_node(
  seastar::lw_shared_ptr<wal_commit_result_state> result,
  std::uint32_t members,
  runtime::monotonic_time deadline)
  : result(std::move(result))
  , members(members)
  , deadline(deadline) {}
wal_commit_node::~wal_commit_node() {
    assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-WAL-COHORT-JOINED"},
      !submission || (joined && result->settled),
      "accepted cohort write observer destroyed before join");
}
} // namespace detail

runtime::result<void> wal_group_commit_config::validate() const noexcept {
    if (
      target_members == 0 || target_members > maximum_members
      || maximum_members > maximum_wal_cohort_members
      || target_bytes.value() == 0 || target_bytes > maximum_bytes
      || maximum_bytes > runtime::maximum_file_io_bytes
      || outstanding_groups == 0
      || outstanding_groups > maximum_wal_cohort_groups
      || maximum_observers == 0
      || maximum_observers > maximum_wal_commit_observers
      || maximum_observers < outstanding_groups
      || execution_bytes.value() < 4096
      || execution_bytes.value() > maximum_contiguous_allocation_bytes)
        return runtime::failure(commit_error(errc::invalid_argument));
    return {};
}

wal_commit_result::wal_commit_result(
  seastar::lw_shared_ptr<const detail::wal_commit_result_state> state) noexcept
  : state_(std::move(state)) {}
const detail::wal_commit_result_state&
wal_commit_result::value() const noexcept {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-WAL-RESULT-SETTLED"},
      state_ && state_->settled && state_->requested,
      "access through an empty or unsettled WAL result");
    state_->assert_current();
    return *state_;
}
const wal_captured_boundary& wal_commit_result::boundary() const& noexcept {
    return *value().requested;
}
const runtime::first_failure& wal_commit_result::failure() const& noexcept {
    return value().outcome.failure;
}
const std::optional<wal_durable_receipt>&
wal_commit_result::receipt() const& noexcept {
    return value().outcome.receipt;
}

wal_commit_ticket::wal_commit_ticket(detail::wal_commit_node_ptr node) noexcept
  : node_(std::move(node)) {}
const wal_captured_boundary& wal_commit_ticket::boundary() const& noexcept {
    return value().submission->boundary;
}
byte_count wal_commit_ticket::encoded_bytes() const noexcept {
    return value().submission->extent.size();
}
std::uint32_t wal_commit_ticket::members() const noexcept {
    return value().members;
}
runtime::result<seastar::future<wal_commit_result>>
wal_commit_ticket::observe() const {
    const auto& node = value();
    auto& state = *node.result;
    if (state.settled)
        return seastar::make_ready_future<wal_commit_result>(
          wal_commit_result{node.result});
    if (auto admitted = admit_observer(state); !admitted)
        return runtime::failure(admitted.error());
    // Native registration uses critical allocations. Its complete bounded
    // storage was charged before acceptance; it is not recoverable frame OOM.
    return node.durable.get_shared_future();
}
runtime::result<void> wal_commit_ticket::admit_observer(
  detail::wal_commit_result_state& state) noexcept {
    if (state.registrations != 0) {
        auto slot = seastar::try_get_units(state.lifetime->observers, 1);
        if (!slot) return runtime::failure(commit_error(errc::queue_full));
        state.observers.adopt(std::move(*slot));
    }
    ++state.registrations;
    return {};
}
const detail::wal_commit_node& wal_commit_ticket::value() const noexcept {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-WAL-COMMIT-TICKET-LIVE"},
      node_ && node_->submission,
      "access through an empty cohort ticket");
    node_->assert_current();
    return *node_;
}
const wal_captured_boundary& wal_flush_capture::boundary() const& noexcept {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-WAL-CAPTURE-LIVE"},
      groups_ != 0 && nodes_[groups_ - 1],
      "access through an empty cohort capture");
    nodes_[groups_ - 1]->assert_current();
    return nodes_[groups_ - 1]->submission->boundary;
}

runtime::operation_error wal_group_commit::error(errc code) noexcept {
    return commit_error(code);
}
runtime::result<std::unique_ptr<wal_group_commit>> wal_group_commit::make_bound(
  workload_budget& budget,
  wal_captured_boundary scope,
  wal_group_commit_config config,
  bool* registration) {
    if (auto valid = config.validate(); !valid)
        return runtime::failure(valid.error());
    auto instance = budget.allocation_charge(
      byte_count{sizeof(wal_group_commit)});
    auto lifetime = budget.allocation_charge(
      byte_count{sizeof(detail::wal_commit_lifetime) + 64});
    auto chunk = budget.allocation_charge(
      byte_count{8 * sizeof(detail::wal_commit_node_ptr) + 64});
    auto execution = budget.allocation_charge(config.execution_bytes);
    auto close_peers = observer_storage<runtime::first_failure>(
      budget, maximum_close_waiters);
    if (!instance) return runtime::failure(instance.error());
    if (!lifetime) return runtime::failure(lifetime.error());
    if (!chunk) return runtime::failure(chunk.error());
    if (!execution) return runtime::failure(execution.error());
    if (!close_peers) return runtime::failure(close_peers.error());
    // Control chains and each admitted observer's wrapper/deadline/backend
    // timer work have separate held allowances. They never reacquire ordinary
    // workload admission while draining a full queue.
    auto held = budget.try_reserve(
      byte_count{
        instance->value() + lifetime->value() + 2 * chunk->value()
        + close_peers->value()
        + (8U + 3U * config.maximum_observers) * execution->value()});
    if (!held) return runtime::failure(held.error());
    auto shared = seastar::make_lw_shared<detail::wal_commit_lifetime>(
      std::move(*held), config.outstanding_groups, config.maximum_observers);
    return std::unique_ptr<wal_group_commit>{new wal_group_commit(
      budget, std::move(scope), config, std::move(shared), registration)};
}
wal_group_commit::wal_group_commit(
  workload_budget& budget,
  wal_captured_boundary scope,
  wal_group_commit_config config,
  seastar::lw_shared_ptr<detail::wal_commit_lifetime> lifetime,
  bool* registration)
  : budget_(budget)
  , scope_(std::move(scope))
  , config_(config)
  , lifetime_(std::move(lifetime))
  , registration_(registration) {
    queue_.reserve(16);
}
wal_group_commit::~wal_group_commit() {
    assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-WAL-COHORT-DRAINED"},
      closed_ && queue_.empty() && !timer_wake_ && !registration_
        && !flush_worker_ && !service_worker_
        && (!close_worker_ || close_worker_->available())
        && operations_.get_count() == 0
        && lifetime_->observer_work.get_count() == 0,
      "cohort owner destroyed before joined close");
    if (close_worker_) close_worker_->get();
}
runtime::result<detail::wal_commit_node_ptr> wal_group_commit::reserve(
  std::size_t members,
  runtime::monotonic_time origin,
  runtime::monotonic_time now) {
    if (members == 0 || origin > now)
        return runtime::failure(error(errc::invalid_argument));
    if (members > config_.maximum_members)
        return runtime::failure(error(errc::resource_exhausted));
    const auto deadline = origin.checked_add(config_.maximum_wait);
    if (!deadline) return runtime::failure(error(errc::out_of_range));
    auto slot = seastar::try_get_units(lifetime_->slots, 1);
    if (!slot) return runtime::failure(error(errc::queue_full));
    auto observer = seastar::try_get_units(lifetime_->observers, 1);
    if (!observer) return runtime::failure(error(errc::queue_full));
    auto object = budget_.allocation_charge(
      byte_count{sizeof(detail::wal_commit_node) + 64});
    auto result = budget_.allocation_charge(
      byte_count{sizeof(detail::wal_commit_result_state) + 64});
    auto observers = observer_storage<wal_commit_result>(
      budget_, config_.maximum_observers);
    auto execution = budget_.allocation_charge(config_.execution_bytes);
    if (!object) return runtime::failure(object.error());
    if (!result) return runtime::failure(result.error());
    if (!observers) return runtime::failure(observers.error());
    if (!execution) return runtime::failure(execution.error());
    auto held = budget_.try_reserve(
      byte_count{
        object->value() + result->value() + observers->value()
        + execution->value()});
    if (!held) return runtime::failure(held.error());
    auto state = seastar::make_lw_shared<detail::wal_commit_result_state>(
      lifetime_, std::move(*slot), std::move(*observer), std::move(*held));
    return seastar::make_lw_shared<detail::wal_commit_node>(
      std::move(state), static_cast<std::uint32_t>(members), *deadline);
}
void wal_group_commit::install(
  detail::wal_commit_node_ptr node,
  wal_submission submission,
  runtime::monotonic_time now) noexcept {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-WAL-COHORT-TICKET"},
      submission.members == node->members
        && submission.extent.begin() == scope_.cursor().position()
        && submission.extent.end() == submission.boundary.cursor().position()
        && submission.boundary.cursor().incarnation()
             == scope_.cursor().incarnation()
        && submission.extent.size() <= config_.maximum_bytes,
      "writer returned an inconsistent cohort ticket");
    const auto bytes = submission.extent.size();
    const auto previous_deadline = queue_.empty()
                                     ? std::optional<runtime::monotonic_time>{}
                                     : std::optional{next_deadline()};
    bool sealed = false;
    if (forming_tail_
        && (forming_members_ + node->members > config_.target_members
            || forming_bytes_.value() + bytes.value() > config_.target_bytes.value()
            || (config_.maximum_wait.nanoseconds() != 0 && now >= forming_deadline_))) {
        forming_tail_->sealed = true;
        forming_tail_ = {};
        sealed = true;
    }
    if (!forming_tail_) {
        forming_members_ = 0;
        forming_bytes_ = {};
        forming_deadline_ = node->deadline;
    }
    forming_members_ += node->members;
    forming_bytes_ = byte_count{forming_bytes_.value() + bytes.value()};
    forming_deadline_ = std::min(forming_deadline_, node->deadline);
    scope_ = submission.boundary;
    node->result->requested.emplace(submission.boundary);
    node->submission.emplace(std::move(submission));
    // Native reserved FIFO insertion and shared-pointer copy cannot allocate.
    queue_.push_back(node);
    forming_tail_ = std::move(node);
    if (
      forming_members_ >= config_.target_members
      || forming_bytes_ >= config_.target_bytes
      || lifetime_->slots.current() == 0 || stopped_ || rotating_) {
        // A full retained-group budget prevents this cohort from growing.
        // Seal only the forming tail; an active capture remains immutable.
        forming_tail_->sealed = true;
        forming_tail_ = {};
        sealed = true;
    }
    // A later arrival in the same open cohort does not cancel/reallocate its
    // unchanged timer. Wake only for new work, an earlier deadline or a cut.
    if (!previous_deadline || sealed || forming_deadline_ < *previous_deadline)
        wake();
}
std::optional<wal_flush_capture>
wal_group_commit::capture(runtime::monotonic_time now) {
    assert_current();
    if (active_) return active_;
    if (queue_.empty()) return std::nullopt;
    wal_flush_capture selected;
    selected.deadline_ = queue_.front()->deadline;
    bool sealed = false;
    for (const auto& node : queue_) {
        selected.nodes_[selected.groups_++] = node;
        selected.members_ += node->members;
        selected.bytes_ = byte_count{
          selected.bytes_.value() + node->submission->extent.size().value()};
        selected.deadline_ = std::min(selected.deadline_, node->deadline);
        if (node->sealed) {
            sealed = true;
            break;
        }
    }
    if (!sealed && !stopped_ && !rotating_ && now < selected.deadline_)
        return std::nullopt;
    // Membership is now frozen. Its final complete ticket remains owned by
    // the selected last node, rather than a separately sampled writer cursor.
    auto& last = selected.nodes_[selected.groups_ - 1];
    last->sealed = true;
    if (forming_tail_ == last) forming_tail_ = {};
    active_.emplace(std::move(selected));
    wake();
    return active_;
}
runtime::monotonic_time wal_group_commit::next_deadline() const noexcept {
    auto deadline = queue_.front()->deadline;
    for (const auto& node : queue_) {
        deadline = std::min(deadline, node->deadline);
        if (node->sealed) break;
    }
    return deadline;
}
void wal_group_commit::wake() noexcept {
    changed_.broadcast();
    if (timer_wake_) timer_wake_->request_abort();
}
void wal_group_commit::force() noexcept {
    assert_current();
    if (forming_tail_) {
        forming_tail_->sealed = true;
        forming_tail_ = {};
    }
    wake();
}
void wal_group_commit::request_stop() noexcept {
    assert_current();
    stopped_ = true;
    force();
}
bool wal_group_commit::is_active(
  const wal_flush_capture& capture) const noexcept {
    if (!active_ || capture.groups_ != active_->groups_) return false;
    for (std::uint32_t i = 0; i != capture.groups_; ++i)
        if (capture.nodes_[i] != active_->nodes_[i]) return false;
    return true;
}
bool wal_group_commit::belongs(
  const wal_flush_capture& capture) const noexcept {
    if (capture.groups_ == 0 || capture.groups_ > maximum_wal_cohort_groups)
        return false;
    for (std::uint32_t i = 0; i != capture.groups_; ++i)
        if (
          !capture.nodes_[i]
          || capture.nodes_[i]->result->lifetime != lifetime_)
            return false;
    return true;
}
void wal_group_commit::remember(
  const runtime::first_failure& failure) noexcept {
    if (failure.exception()) {
        storage_failure_.observe(failure.exception());
        first_.observe(failure.exception());
    }
    if (failure.error()) {
        storage_failure_.observe(*failure.error());
        first_.observe(*failure.error());
    }
    if (failure.failed()) request_stop();
}
void wal_group_commit::publish(detail::wal_commit_node& node) noexcept {
    node.durable.set_value(wal_commit_result{node.result});
}
void wal_group_commit::finish_flush(
  const wal_flush_capture& capture,
  const wal_barrier_outcome& outcome) noexcept {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-WAL-FLUSH-CAPTURE"},
      is_active(capture) && flushing_ && joining_,
      "flush completion lost its active membership");
    for (std::uint32_t i = 0; i != capture.groups_; ++i) {
        auto& node = *capture.nodes_[i];
        KWAQUE_INVARIANT(
          invariant_id{"KQ-WAL-RESULT-ONCE"},
          node.joined && !node.result->settled,
          "WAL result settled before join or settled twice");
        node.result->outcome = outcome;
        node.result->settled = true;
        queue_.pop_front();
    }
    active_.reset();
    flushing_ = joining_ = false;
    wake();
    // Every result and reusable slot is consistent before any continuation.
    // The captured nodes and operation gate stay alive through notification.
    for (std::uint32_t i = 0; i != capture.groups_; ++i)
        publish(*capture.nodes_[i]);
}
void wal_group_commit::abandon(detail::wal_commit_node& node) noexcept {
    if (node.result->settled) return;
    node.result->outcome.failure = first_;
    if (!node.result->outcome.failure.failed())
        node.result->outcome.failure.observe(error(errc::closed));
    node.result->settled = true;
}
seastar::future<>
wal_group_commit::join_node(detail::wal_commit_node_ptr node) {
    if (node->joined) co_return;
    try {
        node->completion.emplace(co_await std::move(node->submission->written));
        node->failure = node->completion->failure;
    } catch (...) {
        node->failure.observe(std::current_exception());
    }
    node->joined = true;
    remember(node->failure);
}
seastar::future<runtime::first_failure>
wal_group_commit::join_written(wal_flush_capture capture) {
    assert_current();
    if (
      closing_ || closed_ || service_enabled_ || rotating_ || joining_
      || waiting_ || !is_active(capture)) {
        runtime::first_failure rejected;
        rejected.observe(error(
          closing_ || closed_ ? errc::closed
          : service_enabled_ || rotating_ || joining_ || waiting_
            ? errc::queue_full
            : errc::wrong_context));
        co_return rejected;
    }
    auto holder = operations_.hold();
    joining_ = true;
    auto done = seastar::defer([this] noexcept {
        joining_ = false;
        wake();
    });
    // Never skip a later observer when a lower write fails. Every native
    // operation is still owned and must finish before its node can retire.
    for (std::uint32_t i = 0; i != capture.groups_; ++i)
        co_await join_node(capture.nodes_[i]);
    co_return first_;
}
runtime::result<void>
wal_group_commit::retire_written(const wal_flush_capture& capture) noexcept {
    assert_current();
    if (closing_ || closed_) return runtime::failure(error(errc::closed));
    if (service_enabled_ || rotating_ || joining_ || waiting_)
        return runtime::failure(error(errc::queue_full));
    if (!is_active(capture))
        return runtime::failure(error(errc::wrong_context));
    for (std::uint32_t i = 0; i != capture.groups_; ++i)
        if (!capture.nodes_[i]->joined)
            return runtime::failure(error(errc::queue_full));
    for (std::uint32_t i = 0; i != capture.groups_; ++i)
        abandon(*capture.nodes_[i]);
    for (std::uint32_t i = 0; i != capture.groups_; ++i)
        queue_.pop_front();
    active_.reset();
    wake();
    for (std::uint32_t i = 0; i != capture.groups_; ++i)
        publish(*capture.nodes_[i]);
    return {};
}
runtime::result<void>
wal_group_commit::bind_shutdown(seastar::abort_source& abort) {
    assert_current();
    if (shutdown_bound_ || service_enabled_ || stopped_ || closing_ || closed_)
        return runtime::failure(error(errc::invalid_argument));
    if (abort.abort_requested()) {
        shutdown_bound_ = true;
        request_stop();
        return {};
    }
    shutdown_subscription_ = abort.subscribe(
      [this] noexcept { request_stop(); });
    shutdown_bound_ = true;
    if (!shutdown_subscription_ && abort.abort_requested()) request_stop();
    return {};
}
seastar::future<runtime::result<std::optional<wal_flush_capture>>>
wal_group_commit::rejected_capture(errc code) {
    return seastar::make_ready_future<
      runtime::result<std::optional<wal_flush_capture>>>(
      runtime::failure(error(code)));
}
seastar::future<runtime::result<std::optional<wal_flush_capture>>>
wal_group_commit::wait_capture_guarded(
  void* timer, sleep_fn sleep, now_fn now, seastar::gate::holder holder) {
    static_cast<void>(holder);
    co_return co_await wait_capture_owned(timer, sleep, now);
}
seastar::future<std::optional<wal_flush_capture>>
wal_group_commit::wait_capture_owned(void* timer, sleep_fn sleep, now_fn now) {
    waiting_ = true;
    auto done = seastar::defer([this] noexcept {
        waiting_ = false;
        changed_.broadcast();
    });
    while (true) {
        if (auto ready = capture(now())) co_return ready;
        if ((stopped_ || rotating_) && queue_.empty() && !submitting_)
            co_return std::nullopt;
        if (queue_.empty()) {
            co_await changed_.when();
            continue;
        }
        seastar::abort_source wake;
        timer_wake_ = &wake;
        auto unlink = seastar::defer(
          [this] noexcept { timer_wake_ = nullptr; });
        try {
            const auto result = co_await sleep(timer, next_deadline(), wake);
            if (
              !result
              && !(
                result.error().code() == errc::aborted
                && wake.abort_requested())) {
                if (
                  result.error().code() != errc::aborted
                  && result.error().code() != errc::closed) {
                    coordinator_failure_.observe(result.error());
                    first_.observe(result.error());
                }
                request_stop();
            }
        } catch (...) {
            coordinator_failure_.observe(std::current_exception());
            first_.observe(std::current_exception());
            request_stop();
        }
    }
}
runtime::result<void> wal_group_commit::start_flush(wal_flush_capture capture) {
    if (flush_worker_) {
        if (!flush_worker_->available())
            return runtime::failure(error(errc::queue_full));
        flush_worker_->get();
        flush_worker_.reset();
    }
    auto holder = operations_.hold();
    flush_worker_.emplace(flush_owned(std::move(capture), std::move(holder)));
    return {};
}
seastar::future<> wal_group_commit::flush_owned(
  wal_flush_capture capture, seastar::gate::holder holder) {
    // Manual execution holds the gate through publication. The automatic
    // service and joined drain already own their worker and pass an empty
    // holder.
    static_cast<void>(holder);
    flushing_ = joining_ = true;
    wal_barrier_outcome outcome;
    try {
        // A coordinator/timer failure fences certification, but does not skip
        // the accepted storage work. Only the writer's storage failure stops
        // new syncs.
        if (!storage_failure_.failed())
            outcome = co_await barrier_(writer_, capture.boundary());
    } catch (...) {
        outcome.failure.observe(std::current_exception());
    }
    remember(outcome.failure);
    for (std::uint32_t i = 0; i != capture.groups_; ++i)
        co_await join_node(capture.nodes_[i]);
    if (
      !first_.failed()
      && (!outcome.receipt || outcome.receipt->boundary() != capture.boundary())) {
        coordinator_failure_.observe(error(errc::wrong_context));
        first_.observe(error(errc::wrong_context));
        request_stop();
    }
    outcome.failure = first_;
    if (outcome.failure.failed()) outcome.receipt.reset();
    finish_flush(capture, outcome);
}
seastar::future<> wal_group_commit::service_loop() {
    try {
        while (
          auto selected = co_await wait_capture_owned(timer_, sleep_, now_))
            co_await flush_owned(std::move(*selected));
        co_return;
    } catch (...) {
        coordinator_failure_.observe(std::current_exception());
        first_.observe(std::current_exception());
        request_stop();
    }
    co_await drain_queue();
}
seastar::future<> wal_group_commit::drain_pending() {
    force();
    if (service_worker_) {
        co_await std::move(*service_worker_);
        service_worker_.reset();
    }
    co_await drain_queue();
}
seastar::future<> wal_group_commit::drain_queue() {
    // A previously entered manual consumer or writer preflight must finish
    // before this control operation becomes the sole consumer of the queue.
    while (submitting_ || waiting_ || joining_)
        co_await changed_.when();
    if (flush_worker_) {
        co_await std::move(*flush_worker_);
        flush_worker_.reset();
    }
    while (!queue_.empty()) {
        force();
        auto selected = capture(runtime::monotonic_time{});
        KWAQUE_INVARIANT(
          invariant_id{"KQ-WAL-DRAIN-CAPTURE"},
          selected.has_value(),
          "forced WAL cohort was not captured");
        co_await flush_owned(std::move(*selected));
    }
}
seastar::future<runtime::result<void>> wal_group_commit::close() {
    assert_current();
    if (closed_) {
        if (first_.exception())
            return seastar::make_exception_future<runtime::result<void>>(
              first_.exception());
        return seastar::make_ready_future<runtime::result<void>>(
          first_.outcome());
    }
    // Control waiters are bounded separately from data observer slots, so a
    // full data budget cannot prevent the one joined close operation.
    if (close_waiters_ == maximum_close_waiters)
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::failure(error(errc::queue_full)));
    ++close_waiters_;
    auto ready = close_done_.get_shared_future();
    if (!closing_) {
        closing_ = true;
        request_stop();
        close_worker_.emplace(close_once());
    }
    return std::move(ready).then(
      [lifetime = lifetime_](runtime::first_failure failure) {
          static_cast<void>(lifetime);
          return failure.outcome();
      });
}
seastar::future<> wal_group_commit::close_once() {
    co_await operations_.close();
    co_await drain_pending();
    co_await lifetime_->observer_work.close();
    active_.reset();
    forming_tail_ = {};
    shutdown_subscription_ = {};
    *registration_ = false;
    registration_ = nullptr;
    writer_ = nullptr;
    timer_ = nullptr;
    closed_ = true;
    closing_ = false;
    close_done_.set_value(first_);
}
std::size_t wal_group_commit::queued_groups() const noexcept {
    assert_current();
    return queue_.size();
}
std::uint32_t wal_group_commit::retained_groups() const noexcept {
    assert_current();
    return lifetime_->maximum
           - static_cast<std::uint32_t>(lifetime_->slots.current());
}
} // namespace kwaque::storage
