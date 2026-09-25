#pragma once

#include "src/runtime/timer.h"
#include "src/storage/wal_writer.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/util/defer.hh>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>

namespace kwaque::storage {

inline constexpr std::uint32_t maximum_wal_cohort_groups = 8;
inline constexpr std::uint32_t maximum_wal_cohort_members = 128;
inline constexpr std::uint32_t maximum_wal_commit_observers = 256;

struct wal_group_commit_config final {
    std::uint32_t target_members{32};
    std::uint32_t maximum_members{maximum_wal_cohort_members};
    byte_count target_bytes{4U * 1024U * 1024U};
    byte_count maximum_bytes{runtime::maximum_file_io_bytes};
    std::uint32_t outstanding_groups{2};
    // Includes one pre-admitted observer per group; must be at least
    // outstanding_groups. Additional observers share this pool.
    std::uint32_t maximum_observers{64};
    runtime::monotonic_duration maximum_wait{1'000'000};
    byte_count execution_bytes{4096};
    [[nodiscard]] runtime::result<void> validate() const noexcept;
};

namespace detail {
struct wal_commit_lifetime final : runtime::shard_affine {
    wal_commit_lifetime(workload_reservation, std::uint32_t, std::uint32_t);
    ~wal_commit_lifetime();
    workload_reservation held;
    seastar::semaphore slots;
    seastar::semaphore observers;
    seastar::gate observer_work;
    std::uint32_t maximum;
};
struct wal_commit_result_state final : runtime::shard_affine {
    wal_commit_result_state(
      seastar::lw_shared_ptr<wal_commit_lifetime>,
      seastar::semaphore_units<>,
      seastar::semaphore_units<>,
      workload_reservation);
    ~wal_commit_result_state();
    seastar::lw_shared_ptr<wal_commit_lifetime> lifetime;
    seastar::semaphore_units<> slot, observers;
    workload_reservation held;
    std::optional<wal_captured_boundary> requested;
    wal_barrier_outcome outcome;
    std::uint32_t registrations{0};
    bool settled{false};
};
} // namespace detail

// Immutable WAL-only outcome. Keep this owner through downstream work: copying
// the small receipt alone does not retain the group's admission. The result
// owns no open file and is never a segment barrier or producer acknowledgement.
class wal_commit_result final {
public:
    [[nodiscard]] const wal_captured_boundary& boundary() const& noexcept;
    const wal_captured_boundary& boundary() const&& = delete;
    [[nodiscard]] const runtime::first_failure& failure() const& noexcept;
    const runtime::first_failure& failure() const&& = delete;
    [[nodiscard]] const std::optional<wal_durable_receipt>&
    receipt() const& noexcept;
    const std::optional<wal_durable_receipt>& receipt() const&& = delete;

private:
    friend class wal_group_commit;
    friend class wal_commit_ticket;
    explicit wal_commit_result(
      seastar::lw_shared_ptr<const detail::wal_commit_result_state>) noexcept;
    [[nodiscard]] const detail::wal_commit_result_state& value() const noexcept;
    seastar::lw_shared_ptr<const detail::wal_commit_result_state> state_;
};

namespace detail {
struct wal_commit_node final : runtime::shard_affine {
    wal_commit_node(
      seastar::lw_shared_ptr<wal_commit_result_state>,
      std::uint32_t,
      runtime::monotonic_time);
    ~wal_commit_node();
    // Result storage and promise state are admitted before writer acceptance.
    // Results never point back to the node/promise, avoiding an ownership
    // cycle.
    seastar::lw_shared_ptr<wal_commit_result_state> result;
    seastar::shared_promise<wal_commit_result> durable;
    std::optional<wal_submission> submission;
    std::optional<wal_write_completion> completion;
    runtime::first_failure failure;
    std::uint32_t members;
    runtime::monotonic_time deadline;
    bool sealed{false}, joined{false};
};
using wal_commit_node_ptr = seastar::lw_shared_ptr<wal_commit_node>;
} // namespace detail

// An accepted group and its retained admission. This is not written or durable
// success. Copies retain the same admission until the last owner is released.
class wal_commit_ticket final {
public:
    [[nodiscard]] const wal_captured_boundary& boundary() const& noexcept;
    const wal_captured_boundary& boundary() const&& = delete;
    [[nodiscard]] byte_count encoded_bytes() const noexcept;
    [[nodiscard]] std::uint32_t members() const noexcept;
    // The first observer uses its pre-admitted slot. Extra pending observers
    // acquire bounded interest; rejection never cancels accepted execution.
    [[nodiscard]] runtime::result<seastar::future<wal_commit_result>>
    observe() const;

    // Cancellation/deadline ends only this interest. The timer, caller abort
    // source and owner must outlive the returned future (or joined close).
    // Registration credits remain charged until the common result is released.
    template<runtime::monotonic_clock Clock, runtime::timer_service Timer>
    [[nodiscard]] seastar::future<runtime::result<wal_commit_result>> observe(
      Timer& timer,
      seastar::abort_source& caller,
      std::optional<runtime::monotonic_time> deadline = std::nullopt) const {
        auto& state = *value().result;
        if (state.settled)
            return seastar::make_ready_future<
              runtime::result<wal_commit_result>>(
              wal_commit_result{node_->result});
        if (caller.abort_requested() || (deadline && Clock::now() >= *deadline))
            return seastar::make_ready_future<
              runtime::result<wal_commit_result>>(runtime::failure(
              runtime::operation_error{
                caller.abort_requested() ? errc::aborted : errc::timed_out,
                runtime::operation_kind::timer}));
        if (auto admitted = admit_observer(state); !admitted)
            return seastar::make_ready_future<
              runtime::result<wal_commit_result>>(
              runtime::failure(admitted.error()));
        return observe_owned(node_, timer, caller, deadline);
    }

private:
    friend class wal_group_commit;
    explicit wal_commit_ticket(detail::wal_commit_node_ptr node) noexcept;
    [[nodiscard]] const detail::wal_commit_node& value() const noexcept;
    static runtime::result<void>
    admit_observer(detail::wal_commit_result_state&) noexcept;
    template<runtime::timer_service Timer>
    static seastar::future<> observer_deadline(
      Timer& timer,
      runtime::monotonic_time deadline,
      seastar::abort_source& timer_abort,
      seastar::abort_source& peer_abort,
      runtime::first_failure& reason) {
        try {
            const auto result = co_await timer.sleep_until(
              deadline, timer_abort);
            if (timer_abort.abort_requested()) co_return;
            if (result)
                reason.observe(
                  runtime::operation_error{
                    errc::timed_out, runtime::operation_kind::timer});
            else
                reason.observe(result.error());
        } catch (...) {
            if (timer_abort.abort_requested()) co_return;
            reason.observe(std::current_exception());
        }
        peer_abort.request_abort();
    }
    template<runtime::timer_service Timer>
    static seastar::future<runtime::result<wal_commit_result>> observe_owned(
      detail::wal_commit_node_ptr node,
      Timer& timer,
      seastar::abort_source& caller,
      std::optional<runtime::monotonic_time> deadline) {
        auto holder = node->result->lifetime->observer_work.hold();
        runtime::first_failure reason;
        seastar::abort_source peer_abort, timer_abort;
        // Install fallible subscription storage before creating the native
        // peer.
        auto subscription = caller.subscribe([&reason, &peer_abort] noexcept {
            reason.observe(
              runtime::operation_error{
                errc::aborted, runtime::operation_kind::timer});
            peer_abort.request_abort();
        });
        if (!subscription) {
            reason.observe(
              runtime::operation_error{
                errc::aborted, runtime::operation_kind::timer});
            peer_abort.request_abort();
        }
        auto peer = node->durable.get_shared_future(peer_abort);
        std::optional<seastar::future<>> timeout;
        if (deadline)
            timeout.emplace(observer_deadline(
              timer, *deadline, timer_abort, peer_abort, reason));
        std::optional<wal_commit_result> result;
        std::exception_ptr exception;
        try {
            result.emplace(co_await std::move(peer));
        } catch (...) {
            exception = std::current_exception();
        }
        timer_abort.request_abort();
        if (timeout) co_await std::move(*timeout);
        // The native peer installs its terminal state once. A callback after
        // that installation cannot replace a successful value with detachment.
        if (result) co_return std::move(*result);
        if (reason.failed()) {
            auto failed = reason.outcome();
            co_return runtime::failure(failed.error());
        }
        std::rethrow_exception(exception);
    }
    detail::wal_commit_node_ptr node_;
};

// Immutable, complete membership and the final writer-issued cut. Retaining
// this value retains group admission, never an open file or the queue owner.
class wal_flush_capture final {
public:
    [[nodiscard]] const wal_captured_boundary& boundary() const& noexcept;
    const wal_captured_boundary& boundary() const&& = delete;
    [[nodiscard]] std::uint32_t groups() const noexcept { return groups_; }
    [[nodiscard]] std::uint32_t members() const noexcept { return members_; }
    [[nodiscard]] byte_count encoded_bytes() const noexcept { return bytes_; }
    [[nodiscard]] runtime::monotonic_time deadline() const noexcept {
        return deadline_;
    }

private:
    friend class wal_group_commit;
    wal_flush_capture() = default;
    std::array<detail::wal_commit_node_ptr, maximum_wal_cohort_groups> nodes_;
    std::uint32_t groups_{0}, members_{0};
    byte_count bytes_{};
    runtime::monotonic_time deadline_{};
};

// A shard-local cohort owner. Submission and capture use the same file
// lifetime; providers outlive joined close. It owns write observers and
// batching. start() runs the single automatic barrier consumer. close() forces
// every accepted cohort, joins observers and unregisters this owner before the
// writer may close its file. Providers outlive close; results may outlive
// providers.
class wal_group_commit final : public runtime::shard_affine {
public:
    template<runtime::file_system_backend Backend, typename Owner>
    [[nodiscard]] static runtime::result<std::unique_ptr<wal_group_commit>>
    make(
      wal_writer<Backend, Owner>& writer,
      workload_budget& budget,
      wal_group_commit_config config = {}) {
        writer.assert_current();
        if (writer.group_commit_active_)
            return runtime::failure(error(errc::queue_full));
        auto scope = writer.capture();
        if (!scope) return runtime::failure(scope.error());
        auto owner = make_bound(
          budget, std::move(*scope), config, &writer.group_commit_active_);
        if (owner) {
            (*owner)->writer_ = &writer;
            (*owner)->barrier_ = [](void* context, wal_captured_boundary cut) {
                return static_cast<wal_writer<Backend, Owner>*>(context)
                  ->barrier(std::move(cut));
            };
            writer.group_commit_active_ = true;
        }
        return owner;
    }
    wal_group_commit(const wal_group_commit&) = delete;
    wal_group_commit& operator=(const wal_group_commit&) = delete;
    ~wal_group_commit();

    // Local admission rejection preserves the offer. Once the writer is
    // entered, its consuming contract applies, including writer rejection.
    // The origin is an earlier acceptance timestamp in this clock domain,
    // never a caller timeout. Join the future before destroying admission.
    template<
      runtime::monotonic_clock Clock,
      runtime::file_system_backend Backend,
      typename Owner>
    [[nodiscard]] seastar::future<runtime::result<wal_commit_ticket>> submit(
      wal_writer<Backend, Owner>& writer,
      wal_group&& offered,
      codec::cooperative_work& admission,
      std::optional<runtime::monotonic_time> origin = std::nullopt) {
        assert_current();
        if (stopped_ || closing_ || closed_ || first_.failed())
            return reject_submission(error(errc::closed));
        if (submitting_ || rotating_)
            return reject_submission(error(errc::queue_full));
        try {
            if (auto valid = writer.validate_capture(scope_); !valid)
                return reject_submission(valid.error());
            auto current = writer.positions();
            if (!current) return reject_submission(current.error());
            if (current->reserved != scope_.cursor())
                return reject_submission(error(errc::wrong_context));
            const auto now = Clock::now();
            auto node = reserve(offered.size(), origin.value_or(now), now);
            if (!node) return reject_submission(node.error());
            return submit_owned<Clock>(
              writer,
              std::move(offered),
              admission,
              std::move(*node),
              operations_.hold());
        } catch (...) {
            return seastar::current_exception_as_future<
              runtime::result<wal_commit_ticket>>();
        }
    }

    // Synchronous selection: freeze registered membership before taking its
    // final boundary. Repeated calls retain the same active capture.
    [[nodiscard]] std::optional<wal_flush_capture>
    capture(runtime::monotonic_time now);

    // Explicit consumer for low-level callers. Automatic service and lifecycle
    // operations exclude this path so a selected capture has exactly one owner.
    template<runtime::monotonic_clock Clock, runtime::timer_service Timer>
    [[nodiscard]] seastar::future<
      runtime::result<std::optional<wal_flush_capture>>>
    wait_capture(Timer& timer) {
        assert_current();
        if (closing_ || closed_) return rejected_capture(errc::closed);
        if (service_enabled_ || rotating_ || waiting_ || joining_)
            return rejected_capture(errc::queue_full);
        return wait_capture_guarded(
          &timer, sleep_function<Timer>, Clock::now, operations_.hold());
    }

    // Bind startup-held capabilities. No environment task is spawned during
    // drain, and a timer-service abort removes batching delay without aborting
    // the writer.
    template<runtime::monotonic_clock Clock, runtime::timer_service Timer>
    [[nodiscard]] runtime::result<void> start(Timer& timer) {
        assert_current();
        if (stopped_ || closing_ || closed_)
            return runtime::failure(error(errc::closed));
        if (service_enabled_ || waiting_ || joining_ || rotating_ || active_)
            return runtime::failure(error(errc::queue_full));
        timer_ = &timer;
        sleep_ = sleep_function<Timer>;
        now_ = Clock::now;
        service_enabled_ = true;
        service_worker_.emplace(service_loop());
        return {};
    }
    [[nodiscard]] runtime::result<void> bind_shutdown(seastar::abort_source&);

    template<runtime::file_system_backend Backend, typename Owner>
    [[nodiscard]] seastar::future<runtime::result<void>> rotate(
      wal_writer<Backend, Owner>& writer,
      byte_count required_bytes,
      codec::cooperative_work& admission) {
        assert_current();
        if (stopped_ || closing_ || closed_)
            co_return runtime::failure(error(errc::closed));
        if (writer_ != &writer)
            co_return runtime::failure(error(errc::wrong_context));
        if (rotating_) co_return runtime::failure(error(errc::queue_full));
        auto holder = operations_.hold();
        rotating_ = true;
        auto done = seastar::defer([this] noexcept {
            rotating_ = false;
            if (service_enabled_ && !stopped_ && !closing_)
                service_worker_.emplace(service_loop());
            wake();
        });
        force();
        co_await drain_pending();
        if (first_.failed()) co_return first_.outcome();
        if (stopped_) co_return runtime::failure(error(errc::closed));
        std::optional<runtime::result<void>> rotated;
        try {
            rotated.emplace(
              co_await writer.rotate_checked(
                scope_, required_bytes, admission));
        } catch (...) {
            remember(writer.failure());
            if (!writer.failure().failed()) {
                coordinator_failure_.observe(std::current_exception());
                first_.observe(std::current_exception());
                request_stop();
            }
            throw;
        }
        remember(writer.failure());
        if (!*rotated) co_return std::move(*rotated);
        auto scope = writer.capture();
        if (!scope) {
            coordinator_failure_.observe(scope.error());
            first_.observe(scope.error());
            request_stop();
            co_return runtime::failure(scope.error());
        }
        scope_ = std::move(*scope);
        co_return runtime::result<void>{};
    }

    void force() noexcept;
    void request_stop() noexcept;
    // Start or join the same captured operation without creating another I/O
    // path. Completion is observed through the admitted tickets. A completed
    // same-owner capture is reusable without another barrier.
    template<runtime::file_system_backend Backend, typename Owner>
    [[nodiscard]] runtime::result<void>
    flush(wal_writer<Backend, Owner>& writer, wal_flush_capture capture) {
        assert_current();
        if (closing_ || closed_) return runtime::failure(error(errc::closed));
        if (registration_ != &writer.group_commit_active_ || !belongs(capture))
            return runtime::failure(error(errc::wrong_context));
        if (capture.nodes_[capture.groups_ - 1]->result->settled) return {};
        if (!is_active(capture))
            return runtime::failure(error(errc::wrong_context));
        if (service_enabled_ || rotating_)
            return runtime::failure(error(errc::queue_full));
        if (flushing_) return {};
        if (joining_ || waiting_)
            return runtime::failure(error(errc::queue_full));
        return start_flush(std::move(capture));
    }
    [[nodiscard]] seastar::future<runtime::first_failure>
      join_written(wal_flush_capture);
    [[nodiscard]] runtime::result<void>
    retire_written(const wal_flush_capture&) noexcept;
    // Up to eight concurrent callers join the same close. Further pending
    // interests reject; calls after completion return the cached outcome.
    [[nodiscard]] seastar::future<runtime::result<void>> close();
    [[nodiscard]] std::size_t queued_groups() const noexcept;
    [[nodiscard]] std::uint32_t retained_groups() const noexcept;
    [[nodiscard]] const runtime::first_failure& failure() const& noexcept {
        assert_current();
        return first_;
    }
    const runtime::first_failure& failure() const&& = delete;
    [[nodiscard]] const runtime::first_failure&
    storage_failure() const& noexcept {
        assert_current();
        return storage_failure_;
    }
    const runtime::first_failure& storage_failure() const&& = delete;
    [[nodiscard]] const runtime::first_failure&
    coordinator_failure() const& noexcept {
        assert_current();
        return coordinator_failure_;
    }
    const runtime::first_failure& coordinator_failure() const&& = delete;

private:
    static constexpr std::uint32_t maximum_close_waiters = 8;
    using sleep_fn = seastar::future<runtime::result<void>> (*)(
      void*, runtime::monotonic_time, seastar::abort_source&);
    using now_fn = runtime::monotonic_time (*)();
    template<runtime::timer_service Timer>
    static seastar::future<runtime::result<void>> sleep_function(
      void* timer,
      runtime::monotonic_time deadline,
      seastar::abort_source& abort) {
        return static_cast<Timer*>(timer)->sleep_until(deadline, abort);
    }
    static seastar::future<runtime::result<std::optional<wal_flush_capture>>>
      rejected_capture(errc);
    seastar::future<runtime::result<std::optional<wal_flush_capture>>>
    wait_capture_guarded(void*, sleep_fn, now_fn, seastar::gate::holder);
    seastar::future<std::optional<wal_flush_capture>>
    wait_capture_owned(void*, sleep_fn, now_fn);
    seastar::future<> service_loop();
    seastar::future<> drain_pending();
    seastar::future<> drain_queue();
    runtime::result<void> start_flush(wal_flush_capture);
    seastar::future<>
      flush_owned(wal_flush_capture, seastar::gate::holder = {});
    template<
      runtime::monotonic_clock Clock,
      runtime::file_system_backend Backend,
      typename Owner>
    seastar::future<runtime::result<wal_commit_ticket>> submit_owned(
      wal_writer<Backend, Owner>& writer,
      wal_group&& offered,
      codec::cooperative_work& admission,
      detail::wal_commit_node_ptr node,
      seastar::gate::holder holder) {
        static_cast<void>(holder);
        submitting_ = true;
        auto idle = seastar::defer([this] noexcept {
            submitting_ = false;
            changed_.broadcast();
        });
        // The writer consumes before this coroutine's first suspension and
        // owns all payload cleanup, including its rejected consuming paths.
        auto submission = co_await writer.submit_entered(
          std::move(offered),
          admission,
          {std::min(config_.maximum_members, maximum_wal_group_members),
           config_.maximum_bytes});
        if (!submission) co_return runtime::failure(submission.error());
        // Node/FIFO capacity already exists. Caller abort cannot undo this
        // accepted ticket, and installation requires no fallible allocation.
        install(node, std::move(*submission), Clock::now());
        co_return wal_commit_ticket{std::move(node)};
    }
    static seastar::future<runtime::result<wal_commit_ticket>>
    reject_submission(runtime::operation_error failure) {
        return seastar::make_ready_future<runtime::result<wal_commit_ticket>>(
          runtime::failure(failure));
    }
    [[nodiscard]] static runtime::result<std::unique_ptr<wal_group_commit>>
    make_bound(
      workload_budget&, wal_captured_boundary, wal_group_commit_config, bool*);
    wal_group_commit(
      workload_budget&,
      wal_captured_boundary,
      wal_group_commit_config,
      seastar::lw_shared_ptr<detail::wal_commit_lifetime>,
      bool*);
    static runtime::operation_error error(errc) noexcept;
    [[nodiscard]] runtime::result<detail::wal_commit_node_ptr>
    reserve(std::size_t, runtime::monotonic_time, runtime::monotonic_time);
    void install(
      detail::wal_commit_node_ptr,
      wal_submission,
      runtime::monotonic_time) noexcept;
    [[nodiscard]] runtime::monotonic_time next_deadline() const noexcept;
    [[nodiscard]] bool is_active(const wal_flush_capture&) const noexcept;
    [[nodiscard]] bool belongs(const wal_flush_capture&) const noexcept;
    void remember(const runtime::first_failure&) noexcept;
    void
    finish_flush(const wal_flush_capture&, const wal_barrier_outcome&) noexcept;
    void abandon(detail::wal_commit_node&) noexcept;
    static void publish(detail::wal_commit_node&) noexcept;
    seastar::future<> join_node(detail::wal_commit_node_ptr);
    seastar::future<> close_once();
    void wake() noexcept;

    workload_budget& budget_;
    wal_captured_boundary scope_;
    wal_group_commit_config config_;
    seastar::lw_shared_ptr<detail::wal_commit_lifetime> lifetime_;
    // At most eight live nodes and a partial front chunk: two retained chunks
    // suffice. Reserve both at construction, before any write can be accepted.
    seastar::chunked_fifo<detail::wal_commit_node_ptr, 8, 2> queue_;
    std::optional<wal_flush_capture> active_;
    std::optional<seastar::future<>> flush_worker_, service_worker_,
      close_worker_;
    seastar::shared_promise<runtime::first_failure> close_done_;
    std::uint32_t close_waiters_{0};
    detail::wal_commit_node_ptr forming_tail_;
    std::uint32_t forming_members_{0};
    byte_count forming_bytes_{};
    runtime::monotonic_time forming_deadline_{};
    seastar::gate operations_;
    seastar::condition_variable changed_;
    seastar::abort_source* timer_wake_{nullptr};
    runtime::first_failure first_, storage_failure_, coordinator_failure_;
    void* writer_{nullptr};
    seastar::future<wal_barrier_outcome> (*barrier_)(
      void*, wal_captured_boundary){nullptr};
    void* timer_{nullptr};
    sleep_fn sleep_{nullptr};
    now_fn now_{nullptr};
    seastar::optimized_optional<seastar::abort_source::subscription>
      shutdown_subscription_;
    bool shutdown_bound_{false}, service_enabled_{false}, rotating_{false};
    bool* registration_;
    bool submitting_{false}, waiting_{false}, joining_{false}, flushing_{false};
    bool stopped_{false}, closing_{false}, closed_{false};
};
} // namespace kwaque::storage
