#pragma once

#include "src/base/units.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/codec/transaction.h"
#include "src/storage/completion_resources.h"
#include "src/storage/local_id_allocator.h"
#include "src/storage/wal_child.h"
#include "src/storage/wal_inventory.h"
#include "src/storage/wal_writer_state.h"
#include "src/storage/zero_fill.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/with_scheduling_group.hh>

#include <array>
#include <exception>
#include <span>

namespace kwaque::storage {

enum class wal_start_intent : std::uint8_t {
    // The store never activated a WAL: bootstrap() creates the first head.
    known_unactivated,
    // The control pins a head that recovery classified after a restart:
    // activate_recovered() closes it with a successor and never resumes it.
    recovered_head
};
struct wal_writer_config final {
    storage_alignment alignment;
    // Logical bound. Without preallocation, files grow through writes; no
    // file is reused.
    byte_count capacity_bytes;
    // Preparing a file writes zeros this far past its header (capped by the
    // capacity) and makes them durable once. Writes inside the zero-written
    // range overwrite allocated blocks inside the file size, so their
    // durability needs no allocation metadata. The file then ends in zeros
    // past its last frame, which never decode as a frame. Zero disables it.
    byte_count preallocation_bytes{};
    // Keeps the zero-written range ahead of the reservations: whenever less
    // than this remains past the reserved end, zero-write this much more in
    // the background. Writes past its start wait for it. Nonzero requires
    // preallocation.
    byte_count preallocation_extension_bytes{};
    // A gathered write no larger than this, wholly inside the zero-written
    // range, uses a second handle opened for synchronized writes: its
    // completion is durable, so a barrier covering only such writes needs no
    // flush. Larger writes use the ordinary handle and the barrier's flush.
    byte_count synchronous_write_bytes{byte_count{1_MiB}};
    replay_profile profile{replay_profile::v1};
    std::uint32_t maximum_descriptors{64};
    byte_count maximum_pending_bytes{runtime::maximum_file_io_bytes};
    completion_resource_limits completion{};
    wal_child_limits children{};
};
struct wal_writer_statistics final {
    std::uint64_t accepted_groups{0}, encoded_groups{0}, write_calls{0},
      gathered_groups{0}, flush_calls{0}, rotations{0};
    // Writes durable at completion, and background zero-written extensions.
    std::uint64_t synchronized_writes{0}, extensions{0};
};

// A size observation is neither an allocation guarantee nor a complete end.
// The native adapter may extend EOF before a write completes.
struct wal_file_extent final {
    byte_count capacity_bytes;
    runtime::file_position observed_eof;
    wal_writer_positions positions;
};

// One stable, shard-affine owner per control. Providers outlive joined close.
// Bootstrap is explicit: a missing head in a previously activated store is a
// recovery error, never authorization to create a replacement here.
// Written and WAL-durable results are separate from segment durability and ACK.
template<runtime::file_system_backend Backend, typename Owner>
class wal_writer final : public runtime::shard_affine {
    friend class wal_group_commit;
    bool group_commit_active_{false};
    static_assert(local_directory_owner<Owner>);
    // Metadata reservations point into their file owner. Switch slot identity,
    // never move a file that has installed its completion reserve.
    struct file_slot final {
        std::optional<workload_reservation> preparation;
        std::optional<runtime::file_path> path;
        std::optional<local_wal_descriptor> descriptor;
        std::optional<local_wal_head> head;
        std::optional<runtime::file> file;
        std::optional<completion_resources> completion;
        std::optional<local_file_publisher<Backend>> publisher;
        local_publication_outcome header_publication, head_publication;
        // Synchronized-write handle for [zero_begin, zeroed_end); extensions
        // begin at or after issued_end. A flush is needed only after an
        // ordinary write completed since the last flush began.
        std::optional<runtime::file> sync;
        std::uint64_t zero_begin{0}, zeroed_end{0}, issued_end{0};
        std::uint64_t plain_writes{0}, flushed_plain_writes{0};
        bool ancestors_ready{false}, verified{false}, ready{false};
        bool retryable{false}, abandoned{false};
    };
    struct rotation_state final {
        wal_captured_boundary cut;
        local_wal_head old_head;
        byte_count required_bytes;
        codec::limits policy;
        std::optional<wal_durable_receipt> barrier;
        bool old_closed{false};
    };

public:
    using control_type = local_control_owner<Backend, Owner>;
    using allocator_type = local_id_allocator<Backend, Owner>;
    [[nodiscard]] static runtime::result<std::unique_ptr<wal_writer>> make(
      control_type& control,
      allocator_type& ids,
      workload_budget& budget,
      wal_writer_config config,
      wal_start_intent intent) {
        control.assert_current();
        if (
          &ids.control_ != &control || ids.closing_ || ids.closed_
          || (intent != wal_start_intent::known_unactivated
              && intent != wal_start_intent::recovered_head)
          || config.maximum_descriptors == 0
          || config.maximum_descriptors > runtime::maximum_queued_file_writes
          || config.maximum_pending_bytes.value() == 0
          || config.maximum_pending_bytes > runtime::maximum_file_io_bytes
          || config.capacity_bytes <= config.alignment.bytes()
          || config.capacity_bytes.value() % config.alignment.bytes().value()
               != 0
          || config.preallocation_bytes > config.capacity_bytes
          || config.preallocation_extension_bytes > config.capacity_bytes
          || (config.preallocation_extension_bytes.value() != 0
              && config.preallocation_bytes.value() == 0)
          || config.synchronous_write_bytes > runtime::maximum_file_io_bytes)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        if (
          auto profile = parse_replay_profile(
            static_cast<std::uint16_t>(config.profile));
          !profile)
            return runtime::failure(
              detail::path_error(
                profile.error() == errc::unsupported_format
                  ? errc::unsupported_format
                  : errc::invalid_argument));
        auto current = control.snapshot();
        if (!current) return runtime::failure(current.error());
        if (
          intent == wal_start_intent::known_unactivated
            ? current->fields.wal_head || current->fields.checkpoint
            : !current->fields.wal_head)
            return runtime::failure(detail::path_error(errc::wrong_context));
        if (control.wal_writer_active_)
            return runtime::failure(detail::path_error(errc::already_exists));
        if (!config.children.charge)
            config.children.charge = control.limits_.charge;
        // Reject configuration before registering an owner or allowing any
        // durable bootstrap effect. Submission relies on these bounded sums.
        if (auto valid = config.children.validate(); !valid)
            return runtime::failure(valid.error());
        if (
          config.children.execution_bytes
          < detail::wal_write_descriptor::minimum_execution_bytes)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        if (auto valid = config.completion.validate(); !valid)
            return runtime::failure(valid.error());
        // Object/path storage and retained lifecycle frames. Per-operation
        // codec, publication, alias and native completion costs are separate.
        auto instance = budget.allocation_charge(
          byte_count{sizeof(wal_writer)});
        auto path = budget.allocation_charge(
          byte_count{runtime::maximum_file_path_bytes + 1});
        auto token = budget.allocation_charge(
          byte_count{sizeof(wal_captured_boundary::lifetime) + 64});
        // The write queue's one emptied chunk kept for reuse.
        auto chunk = budget.allocation_charge(
          byte_count{16 * sizeof(detail::wal_write_descriptor::pointer) + 64});
        // Select the largest power-of-two chunk whose allocator-served charge
        // fits the ceiling. Install this bound on each file before activation;
        // reserving a smaller amount alone would not bound native staging.
        auto write_allocation = byte_count{maximum_contiguous_allocation_bytes};
        auto staging = budget.allocation_charge(write_allocation);
        while (!staging && write_allocation > config.alignment.bytes()) {
            write_allocation = byte_count{write_allocation.value() / 2};
            staging = budget.allocation_charge(write_allocation);
        }
        auto gather = budget.allocation_charge(
          byte_count{
            bytes::max_buffer_fragments
            * bytes::fragmented_buffer::fragment_descriptor_size()});
        if (!instance) return runtime::failure(instance.error());
        if (!path) return runtime::failure(path.error());
        if (!token) return runtime::failure(token.error());
        if (!chunk) return runtime::failure(chunk.error());
        if (!staging) return runtime::failure(staging.error());
        if (!gather) return runtime::failure(gather.error());
        // A zero-written file also owns a synchronized-write handle with its
        // own staging window, and the retained zero chunk and its lists. The
        // chunk uses the same qualified size as staging; writes repeat it.
        const std::uint64_t handles = config.preallocation_bytes.value() != 0
                                        ? 2
                                        : 1;
        std::uint64_t zero = 0;
        if (handles == 2) {
            auto lists = budget.allocation_charge(
              byte_count{
                2 * detail::zero_write_chunks
                * bytes::fragmented_buffer::fragment_descriptor_size()});
            if (!lists) return runtime::failure(lists.error());
            zero = staging->value() + lists->value();
        }
        auto held = budget.try_reserve(
          byte_count{
            instance->value() + 16 * path->value() + chunk->value() + 64_KiB
            + handles * 2 * runtime::maximum_file_write_concurrency
                * staging->value()
            + gather->value() + zero});
        if (!held) return runtime::failure(held.error());
        auto token_held = budget.try_reserve(*token);
        if (!token_held) return runtime::failure(token_held.error());
        auto lifetime
          = seastar::make_lw_shared<wal_captured_boundary::lifetime>(
            std::move(*token_held));
        auto value = std::unique_ptr<wal_writer>{new wal_writer(
          control,
          ids,
          budget,
          std::move(config),
          write_allocation,
          std::move(*held),
          std::move(lifetime))};
        value->intent_ = intent;
        control.wal_writer_active_ = true;
        return value;
    }
    wal_writer(const wal_writer&) = delete;
    wal_writer& operator=(const wal_writer&) = delete;
    ~wal_writer() {
        assert_current();
        KWAQUE_INVARIANT(
          invariant_id{"KQ-WAL-DRAINED"},
          closed_ && inflight_.empty() && !rotation_ && !group_commit_active_,
          "WAL owner destroyed before joined close");
    }

    // One attempt. Failures retain publication disposition and namespace debt
    // on this owner until close/destruction. Do not retry a partially started
    // bootstrap or activate an existing tail through this entry point.
    [[nodiscard]] seastar::future<runtime::result<void>>
    bootstrap(codec::cooperative_work& admission) {
        assert_current();
        if (intent_ != wal_start_intent::known_unactivated)
            return reject(errc::invalid_argument);
        if (started_ || admission_stopped_ || closing_ || closed_)
            return reject(errc::closed);
        if (auto ready = admission.poll(); !ready)
            return reject(ready.error().code());
        return bootstrap_owned(admission.policy(), operations_.hold());
    }
    // Activates a successor of the recovered head the control pins, never
    // resuming the head. The head is opened read-only and flushed once while
    // the successor, naming `content_end`, the head's classified content end,
    // as its predecessor cursor, is prepared; the control pins the successor
    // as the head only after both, so the head's surviving bytes seed it only
    // after that fresh barrier. Bytes after the content end stay in the old
    // file as slack, never written or truncated. One attempt, as for
    // bootstrap(): a failure is latched and never retried here.
    [[nodiscard]] seastar::future<runtime::result<void>> activate_recovered(
      local_wal_cursor content_end, codec::cooperative_work& admission) {
        assert_current();
        if (intent_ != wal_start_intent::recovered_head)
            return reject(errc::invalid_argument);
        if (started_ || admission_stopped_ || closing_ || closed_)
            return reject(errc::closed);
        if (auto ready = admission.poll(); !ready)
            return reject(ready.error().code());
        return activate_owned(
          content_end, admission.policy(), operations_.hold());
    }
    // Bind before admission starts. Early environment stop only closes input
    // and wakes the existing dispatcher; accepted work owns independent abort
    // state and the file's checked completion path.
    [[nodiscard]] runtime::result<void>
    bind_shutdown(seastar::abort_source& source) {
        assert_current();
        if (
          shutdown_bound_ || started_ || admission_stopped_ || closing_
          || closed_)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        shutdown_bound_ = true;
        if (source.abort_requested()) {
            request_stop();
            return {};
        }
        shutdown_subscription_ = source.subscribe(
          [this] noexcept { request_stop(); });
        if (!shutdown_subscription_ && source.abort_requested()) request_stop();
        return {};
    }
    void request_stop() noexcept {
        assert_current();
        admission_stopped_ = true;
        dispatcher_stopping_ = true;
        changed_.broadcast();
    }
    [[nodiscard]] bool admission_stopped() const noexcept {
        assert_current();
        return admission_stopped_;
    }
    [[nodiscard]] runtime::result<wal_writer_positions> positions() const {
        assert_current();
        if (!active_ || first_.failed() || closing_ || closed_)
            return runtime::failure(detail::path_error(errc::closed));
        auto control = control_.snapshot();
        if (!control || control->fields.wal_head != current_file().head)
            return runtime::failure(detail::path_error(errc::wrong_context));
        return *positions_;
    }
    [[nodiscard]] runtime::result<wal_captured_boundary> capture() const {
        auto current = positions();
        if (!current) return runtime::failure(current.error());
        return wal_captured_boundary{
          lifetime_, current->owner, current->reserved, reserved_members_};
    }
    [[nodiscard]] runtime::result<void>
    validate_capture(const wal_captured_boundary& cut) const {
        auto current = positions();
        if (!current) return runtime::failure(current.error());
        if (
          cut.token_ != lifetime_ || cut.owner_ != current->owner
          || cut.cursor_.incarnation() != current->reserved.incarnation()
          || cut.cursor_.position() < current_file().descriptor->data_start
          || cut.cursor_.position() > current->reserved.position()
          || cut.members_ > reserved_members_
          || ((cut.members_ == 0) != (cut.cursor_.position() == current_file().descriptor->data_start)))
            return runtime::failure(detail::path_error(errc::wrong_context));
        return {};
    }
    // Observation of installed facts, including after failure/close. This does
    // not authorize admission or create a captured boundary.
    [[nodiscard]] std::optional<wal_writer_positions>
    progress() const noexcept {
        assert_current();
        return positions_;
    }
    [[nodiscard]] wal_writer_statistics statistics() const noexcept {
        assert_current();
        return statistics_;
    }
    // The limits children are prepared under, and the replay profile their
    // expectations must name, for callers that prepare before submission.
    [[nodiscard]] const wal_child_limits& child_limits() const& noexcept {
        assert_current();
        return config_.children;
    }
    const wal_child_limits& child_limits() const&& = delete;
    [[nodiscard]] replay_profile profile() const noexcept {
        assert_current();
        return config_.profile;
    }
    [[nodiscard]] const local_publication_outcome&
    header_publication() const& noexcept {
        assert_current();
        return current_file().header_publication;
    }
    [[nodiscard]] const local_publication_outcome&
    head_publication() const& noexcept {
        assert_current();
        return current_file().head_publication;
    }
    [[nodiscard]] std::optional<local_wal_head> prepared_head() const noexcept {
        assert_current();
        return current_file().head;
    }
    [[nodiscard]] const runtime::first_failure& failure() const& noexcept {
        assert_current();
        return first_;
    }
    // Joined close must precede handing the namespace to recovery. This is
    // only a selected control snapshot; file EOF/known write facts cannot grant
    // resumed append permission. A fenced control needs independent reload.
    [[nodiscard]] runtime::result<wal_inventory_snapshot>
    inventory_snapshot() const {
        assert_current();
        if (!closed_)
            return runtime::failure(detail::path_error(errc::queue_full));
        auto control = control_.snapshot();
        if (!control) return runtime::failure(control.error());
        return wal_inventory_snapshot{
          control_.spec_.shard_owner(control_.shard_).value(), *control};
    }

    // Inspect visible extent independently of the complete write/durable ends.
    // A concurrent native write may change size; this observation grants no
    // append, recovery or truncation authority.
    [[nodiscard]] seastar::future<runtime::result<wal_file_extent>>
    inspect_extent() {
        auto before = positions();
        if (!before) co_return runtime::failure(before.error());
        if (rotation_ || preflight_busy_)
            co_return runtime::failure(detail::path_error(errc::queue_full));
        auto holder = operations_.hold();
        preflight_busy_ = true;
        auto idle = seastar::defer(
          [this] noexcept { preflight_busy_ = false; });
        auto eof = co_await current_file().file->size();
        if (!eof) co_return runtime::failure(eof.error());
        auto after = positions();
        if (!after) co_return runtime::failure(after.error());
        co_return wal_file_extent{
          current_file().descriptor->capacity_bytes,
          runtime::file_position{*eof},
          *after};
    }

    [[nodiscard]] bool rotation_pending() const noexcept {
        assert_current();
        return rotation_.has_value();
    }
    [[nodiscard]] std::optional<local_wal_head>
    successor_head() const noexcept {
        assert_current();
        return successor_file().head;
    }
    [[nodiscard]] const local_publication_outcome&
    successor_header_publication() const& noexcept {
        assert_current();
        return successor_file().header_publication;
    }
    [[nodiscard]] const local_publication_outcome&
    rotation_publication() const& noexcept {
        assert_current();
        return successor_file().head_publication;
    }

    // Freeze the exact final group boundary. No next group is accepted here.
    // required_bytes is its aligned encoded size; an impossible or empty-file
    // rotation rejects before ID allocation. Only proven untouched pressure
    // permits another attempt with the same cut/size and one retained
    // successor.
    [[nodiscard]] seastar::future<runtime::result<void>> rotate(
      wal_captured_boundary cut,
      byte_count required_bytes,
      codec::cooperative_work& admission) {
        assert_current();
        if (group_commit_active_) return reject(errc::queue_full);
        return rotate_checked(std::move(cut), required_bytes, admission);
    }

private:
    seastar::future<runtime::result<void>> rotate_checked(
      wal_captured_boundary cut,
      byte_count required_bytes,
      codec::cooperative_work& admission) {
        assert_current();
        if (admission_stopped_) return reject(errc::closed);
        if (auto valid = validate_capture(cut); !valid)
            return seastar::make_ready_future<runtime::result<void>>(
              runtime::failure(valid.error()));
        if (rotation_busy_ || barrier_busy_ || preflight_busy_)
            return reject(errc::queue_full);
        if (
          cut.cursor_ != positions_->reserved
          || cut.members_ != reserved_members_
          || (rotation_ && (rotation_->cut.cursor_ != cut.cursor_ || rotation_->required_bytes != required_bytes)))
            return reject(errc::wrong_context);
        if (auto ready = admission.poll(); !ready)
            return reject(ready.error().code());
        const auto layout = local_metadata_layout(
          local_metadata_kind::wal_descriptor,
          byte_count{68},
          byte_count{codec::envelope_prefix_bytes},
          config_.alignment,
          admission.policy());
        if (!layout)
            return reject(
              codec::detail::allocation_cost_error(layout.error(), {}, 0)
                .code());
        const auto end = layout->encoded_bytes().checked_add(required_bytes);
        if (
          required_bytes.value() == 0
          || required_bytes.value() % config_.alignment.bytes().value() != 0
          || reserved_members_ == 0)
            return reject(errc::invalid_argument);
        if (
          !end || *end > config_.capacity_bytes
          || required_bytes > config_.maximum_pending_bytes)
            return reject(errc::resource_exhausted);
        return rotate_owned(
          std::move(cut),
          required_bytes,
          admission.policy(),
          operations_.hold());
    }

public:
    // One nonwaiting preflight entrance. Consumes an entered offer, including
    // rejection; there is no reserved-cursor effect. The caller joins this
    // future or close before destroying the writer/work object.
    [[nodiscard]] seastar::future<runtime::result<wal_prepared_children>>
    prepare(
      admitted_wal_batch&& source,
      wal_child_expectation expected,
      codec::cooperative_work& work) {
        auto input = std::move(source);
        assert_current();
        if (admission_stopped_)
            co_return runtime::failure(detail::path_error(errc::closed));
        auto before = positions();
        if (!before) co_return runtime::failure(before.error());
        if (preflight_busy_ || rotation_)
            co_return runtime::failure(detail::path_error(errc::queue_full));
        if (
          auto valid = validate_wal_child_context(
            input.batch().info(), expected);
          !valid)
            co_return runtime::failure(
              detail::path_error(valid.error().code()));
        if (
          expected.target.segment().cluster() != before->owner.cluster()
          || expected.profile != config_.profile)
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        auto holder = operations_.hold();
        preflight_busy_ = true;
        auto idle = seastar::defer(
          [this] noexcept { preflight_busy_ = false; });
        auto prepared = co_await prepare_wal_children(
          std::move(input),
          std::move(expected),
          budget_,
          config_.children,
          work);
        if (!prepared) co_return runtime::failure(prepared.error());
        auto after = positions();
        auto control = control_.snapshot();
        if (
          admission_stopped_ || !after || !control
          || control->fields.wal_head != current_file().head
          || after->reserved != before->reserved) {
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            co_return runtime::failure(detail::path_error(errc::closed));
        }
        if (auto ready = work.poll(); !ready) {
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            co_return runtime::failure(
              detail::path_error(ready.error().code()));
        }
        co_return std::move(*prepared);
    }

    // Consume before the first await, including entered rejection. A returned
    // error means no WAL coordinates were accepted. A returned ticket always
    // denotes accepted coordinates, even if encoding/I/O already failed; its
    // independent observer carries that failure. Dropping it cannot cancel I/O.
    // Direct submission consumes and rejects with queue_full while a
    // group-commit coordinator is registered.
    [[nodiscard]] seastar::future<runtime::result<wal_submission>> submit(
      wal_group&& offered,
      codec::cooperative_work& admission,
      wal_submission_limits limits = {}) {
        assert_current();
        if (group_commit_active_) {
            std::optional<wal_group> consumed{
              std::in_place, std::move(offered)};
            return seastar::make_ready_future<runtime::result<wal_submission>>(
              runtime::failure(detail::path_error(errc::queue_full)));
        }
        return submit_entered(std::move(offered), admission, limits);
    }

    // Measures a later submission of exactly these members from the current
    // reserved end, with the same checks the submission applies before its
    // awaited validation. Nothing is reserved or accepted.
    [[nodiscard]] runtime::result<wal_admission_measure> measure_submission(
      std::span<const wal_admission_member> members,
      const codec::limits& policy,
      wal_submission_limits limits = {}) const {
        auto measured = measure_group(members, policy, limits);
        if (!measured) return runtime::failure(measured.error());
        return measured->measure;
    }

private:
    // The bounds a submitted group accumulates member by member.
    struct group_bounds final {
        byte_count input, additional, retained;
        std::size_t fragments{0};
        [[nodiscard]] runtime::result<void> add(
          const detail::wal_member_memory& memory,
          const codec::limits& policy) {
            auto next_input = input.checked_add(memory.input);
            auto next_additional = additional.checked_add(memory.additional);
            auto next_retained = retained.checked_add(memory.retained);
            if (!next_input || !next_additional || !next_retained)
                return runtime::failure(detail::path_error(errc::out_of_range));
            input = *next_input;
            additional = *next_additional;
            retained = *next_retained;
            fragments += static_cast<std::size_t>(memory.fragments.value());
            if (
              fragments > bytes::max_buffer_fragments
              || fragments > policy.config().max_buffer_fragments.value()
              || retained > runtime::maximum_file_io_bytes)
                return runtime::failure(
                  detail::path_error(errc::resource_exhausted));
            return {};
        }
    };
    struct member_cost final {
        aligned_envelope_layout layout;
        detail::wal_member_memory memory;
    };
    // One member at end: complete layout, owner context and live memory. The
    // submission repeats this with final targets before accepting.
    [[nodiscard]] runtime::result<member_cost> measure_member(
      const encoded_assigned_batch& child,
      const wal_prepare_expectation& reserved,
      const codec::limits& policy,
      const local_store_context& owner) const {
        auto layout = preflight_wal_prepare(
          child,
          reserved,
          policy,
          {config_.maximum_pending_bytes, config_.maximum_pending_bytes},
          {reserved.wal.position().value()});
        if (!layout)
            return runtime::failure(detail::path_error(layout.error().code()));
        if (
          reserved.target.segment().cluster() != owner.cluster()
          || reserved.profile != config_.profile)
            return runtime::failure(detail::path_error(errc::wrong_context));
        auto memory = detail::wal_prepare_memory(
          child, *layout, policy, config_.children.charge);
        if (!memory) return runtime::failure(memory.error());
        return member_cost{*layout, *memory};
    }
    static wal_prepare_expectation reserve_member(
      const wal_child_expectation& e,
      local_wal_cursor cursor,
      storage_alignment alignment,
      runtime::file_position at) {
        return {
          wal_write_context::make(cursor.incarnation(), alignment, at).value(),
          e.target,
          e.target_data_start,
          e.routing_epoch,
          e.batch,
          e.profile,
          e.target_profile};
    }
    struct group_measure final {
        wal_admission_measure measure;
        group_bounds bounds;
    };
    runtime::result<group_measure> measure_group(
      std::span<const wal_admission_member> members,
      const codec::limits& policy,
      wal_submission_limits limits) const {
        auto before = positions();
        if (!before) return runtime::failure(before.error());
        if (
          members.empty() || limits.members == 0
          || limits.members > maximum_wal_group_members
          || limits.encoded_bytes.value() == 0
          || limits.encoded_bytes > runtime::maximum_file_io_bytes)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        group_measure output;
        if (members.size() > limits.members) return output;
        const auto start = before->reserved.position();
        auto end = start;
        for (const auto& member : members) {
            if (!member.child || !member.expected)
                return runtime::failure(
                  detail::path_error(errc::invalid_argument));
            auto cost = measure_member(
              *member.child,
              reserve_member(
                *member.expected, before->reserved, config_.alignment, end),
              policy,
              before->owner);
            if (!cost) return runtime::failure(cost.error());
            if (!output.bounds.add(cost->memory, policy)) return output;
            end = cost->layout.at(end)->end();
        }
        const auto extent = model::file_byte_span::make(start, end).value();
        output.measure.encoded_bytes = extent.size();
        if (
          extent.size() > config_.maximum_pending_bytes
          || extent.size() > limits.encoded_bytes
          || members.size() > UINT64_MAX - reserved_members_)
            return output;
        if (end.value() <= config_.capacity_bytes.value()) {
            output.measure.decision = wal_admission_decision::fits;
            return output;
        }
        // The same fresh-file check rotation applies before allocating an ID.
        const auto header = local_metadata_layout(
          local_metadata_kind::wal_descriptor,
          byte_count{68},
          byte_count{codec::envelope_prefix_bytes},
          config_.alignment,
          policy);
        if (!header)
            return runtime::failure(
              detail::path_error(
                codec::detail::allocation_cost_error(header.error(), {}, 0)
                  .code()));
        const auto fresh = header->encoded_bytes().checked_add(extent.size());
        if (fresh && *fresh <= config_.capacity_bytes && reserved_members_ != 0)
            output.measure.decision = wal_admission_decision::rotate_required;
        return output;
    }

    // Reserves what the submission of a measured group would reserve after
    // its awaits, in one synchronous step. Only ordinary pressure (queue_full)
    // or a changed owner can reject it; neither leaves any effect.
    runtime::result<wal_admission_outcome> admit_entered(
      std::span<const wal_admission_member> members,
      const codec::limits& policy,
      wal_submission_limits limits) {
        assert_current();
        if (admission_stopped_ || closing_ || closed_)
            return runtime::failure(detail::path_error(errc::closed));
        if (rotation_ || preflight_busy_)
            return runtime::failure(detail::path_error(errc::queue_full));
        auto measured = measure_group(members, policy, limits);
        if (!measured) return runtime::failure(measured.error());
        wal_admission_outcome output{measured->measure, std::nullopt};
        if (output.measure.decision != wal_admission_decision::fits)
            return output;
        const auto extent = output.measure.encoded_bytes;
        const auto queued = inflight_.bytes().checked_add(extent);
        const auto kept = inflight_.retained_bytes().checked_add(
          measured->bounds.retained);
        if (
          inflight_.size() >= config_.maximum_descriptors || !queued
          || *queued > config_.maximum_pending_bytes || !kept
          || *kept > config_.maximum_pending_bytes)
            return runtime::failure(detail::path_error(errc::queue_full));
        auto working = budget_.try_reserve(
          byte_count{
            config_.children.working_bytes.value()
            + config_.children.execution_bytes.value()});
        if (!working) return runtime::failure(working.error());
        const auto descriptors = budget_.allocation_charge(
          byte_count{
            measured->bounds.fragments
            * bytes::fragmented_buffer::fragment_descriptor_size()});
        if (!descriptors) return runtime::failure(descriptors.error());
        const auto assembly = measured->bounds.additional.checked_add(
          *descriptors);
        if (!assembly)
            return runtime::failure(detail::path_error(errc::out_of_range));
        auto encoded = budget_.try_reserve(*assembly);
        if (!encoded) return runtime::failure(encoded.error());
        const auto node = detail::wal_write_descriptor::charge(
          budget_, config_.children.execution_bytes);
        if (!node) return runtime::failure(node.error());
        auto descriptor = budget_.try_reserve(*node);
        if (!descriptor) return runtime::failure(descriptor.error());
        output.admission.emplace(
          wal_submission_admission{
            *capture(),
            std::move(*working),
            std::move(*encoded),
            std::move(*descriptor),
            static_cast<std::uint32_t>(members.size()),
            extent,
            measured->bounds.retained});
        return output;
    }

    seastar::future<runtime::result<wal_submission>> submit_entered(
      wal_group&& offered,
      codec::cooperative_work& admission,
      wal_submission_limits limits,
      std::optional<wal_submission_admission> admitted = std::nullopt) {
        std::optional<wal_group> group{std::in_place, std::move(offered)};
        assert_current();
        if (admission_stopped_ || closing_ || closed_)
            co_return runtime::failure(detail::path_error(errc::closed));
        auto holder = operations_.hold();
        std::optional<runtime::result<wal_submission>> result;
        std::exception_ptr exception;
        try {
            result.emplace(
              co_await submit_owned(*group, admission, limits, admitted));
        } catch (...) {
            exception = std::current_exception();
        }
        co_await admission.drain_inline(
          admission.byte_quantum(), admission.item_quantum());
        group.reset();
        if (exception) std::rethrow_exception(exception);
        co_return std::move(*result);
    }

public:
    [[nodiscard]] seastar::future<wal_barrier_outcome>
    barrier(wal_captured_boundary cut) {
        assert_current();
        if (auto valid = validate_capture(cut); !valid) {
            wal_barrier_outcome rejected;
            rejected.failure = first_;
            if (!rejected.failure.failed())
                rejected.failure.observe(valid.error());
            return seastar::make_ready_future<wal_barrier_outcome>(
              std::move(rejected));
        }
        if (
          barrier_busy_ || rotation_busy_
          || (rotation_ && rotation_->old_closed)) {
            wal_barrier_outcome rejected;
            rejected.failure.observe(detail::path_error(errc::queue_full));
            return seastar::make_ready_future<wal_barrier_outcome>(
              std::move(rejected));
        }
        return barrier_owned(std::move(cut), operations_.hold());
    }

    // Stop admission, join preflight/barriers and accepted encoders/writes,
    // cover the final accepted cut, then release metadata and checked-close.
    [[nodiscard]] seastar::future<runtime::result<void>> close() {
        assert_current();
        if (group_commit_active_) return reject(errc::queue_full);
        if (closed_) {
            if (first_.exception())
                return seastar::make_exception_future<runtime::result<void>>(
                  first_.exception());
            return seastar::make_ready_future<runtime::result<void>>(
              first_.outcome());
        }
        if (closing_) return reject(errc::queue_full);
        return close_once();
    }

private:
    seastar::future<runtime::result<void>> close_once() {
        closing_ = true;
        active_ = false;
        request_stop();
        if (!operations_closed_) {
            co_await operations_.close();
            operations_closed_ = true;
        }
        if (dispatcher_) {
            try {
                co_await std::move(*dispatcher_);
            } catch (...) {
                first_.observe(std::current_exception());
            }
            dispatcher_.reset();
        }
        if (positions_ && current_file().completion && !first_.failed()) {
            try {
                auto outcome = co_await flush_cut(
                  wal_captured_boundary{
                    lifetime_,
                    positions_->owner,
                    positions_->reserved,
                    reserved_members_});
                remember(outcome.failure);
            } catch (...) {
                first_.observe(std::current_exception());
            }
        }
        try {
            co_await close_file();
        } catch (...) {
            first_.observe(std::current_exception());
        }
        try {
            co_await close_slot(successor_file());
        } catch (...) {
            first_.observe(std::current_exception());
        }
        if (
          current_file().file || current_file().sync || current_file().publisher
          || successor_file().file || successor_file().sync
          || successor_file().publisher) {
            // A close-frame allocation failure precedes native close. Keep
            // ownership for a later joined attempt; never destroy an open file.
            closing_ = false;
            co_return first_.outcome();
        }
        rotation_.reset();
        zero_ = {};
        shutdown_subscription_ = std::nullopt;
        control_.wal_writer_active_ = false;
        closed_ = true;
        co_return first_.outcome();
    }

    seastar::future<runtime::result<wal_submission>> submit_owned(
      wal_group& group,
      codec::cooperative_work& admission,
      wal_submission_limits limits,
      std::optional<wal_submission_admission>& admitted) {
        auto before = positions();
        if (!before) co_return runtime::failure(before.error());
        if (
          group.size() == 0 || limits.members == 0
          || limits.members > maximum_wal_group_members
          || limits.encoded_bytes.value() == 0
          || limits.encoded_bytes > runtime::maximum_file_io_bytes)
            co_return runtime::failure(
              detail::path_error(errc::invalid_argument));
        if (group.size() > limits.members)
            co_return runtime::failure(
              detail::path_error(errc::resource_exhausted));
        if (
          rotation_ || preflight_busy_
          || inflight_.size() >= config_.maximum_descriptors)
            co_return runtime::failure(detail::path_error(errc::queue_full));
        // An admission is bound to the reserved end it measured.
        if (
          admitted
          && (admitted->start_ != *capture() || group.size() > admitted->members_))
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        if (auto ready = admission.poll(); !ready)
            co_return runtime::failure(
              detail::path_error(ready.error().code()));
        preflight_busy_ = true;
        auto idle = seastar::defer(
          [this] noexcept { preflight_busy_ = false; });
        std::optional<workload_reservation> working;
        if (admitted)
            working.emplace(std::move(admitted->working_));
        else {
            auto held = budget_.try_reserve(
              byte_count{
                config_.children.working_bytes.value()
                + config_.children.execution_bytes.value()});
            if (!held) co_return runtime::failure(held.error());
            working.emplace(std::move(*held));
        }
        const auto start = before->reserved.position();
        auto end = start;
        group_bounds bounds;
        for (auto& member : group.members_) {
            const auto& e = member.expected;
            member.reserved.emplace(
              reserve_member(e, before->reserved, config_.alignment, end));
            auto cost = measure_member(
              member.input.batch,
              *member.reserved,
              admission.policy(),
              before->owner);
            if (!cost) co_return runtime::failure(cost.error());
            auto valid = co_await member.input.batch.validate(
              e.batch,
              {config_.children.working_bytes,
               config_.children.metadata_bytes,
               config_.children.charge},
              admission);
            if (!valid)
                co_return runtime::failure(
                  detail::path_error(valid.error().code()));
            if (auto ready = admission.poll(); !ready)
                co_return runtime::failure(
                  detail::path_error(ready.error().code()));
            member.memory = cost->memory;
            end = cost->layout.at(end)->end();
            if (end.value() > config_.capacity_bytes.value())
                co_return runtime::failure(
                  detail::path_error(errc::resource_exhausted));
            if (
              auto added = bounds.add(cost->memory, admission.policy()); !added)
                co_return runtime::failure(added.error());
        }
        group.input_bytes_ = bounds.input;
        group.additional_bytes_ = bounds.additional;
        group.retained_bound_ = bounds.retained;
        group.fragment_bound_ = bounds.fragments;
        const auto extent = model::file_byte_span::make(start, end).value();
        if (
          extent.size() > config_.maximum_pending_bytes
          || extent.size() > limits.encoded_bytes
          || group.size() > UINT64_MAX - reserved_members_)
            co_return runtime::failure(
              detail::path_error(errc::resource_exhausted));
        const auto descriptors = budget_.allocation_charge(
          byte_count{
            group.fragment_bound_
            * bytes::fragmented_buffer::fragment_descriptor_size()});
        if (!descriptors) co_return runtime::failure(descriptors.error());
        auto assembly = group.additional_bytes_.checked_add(*descriptors);
        if (!assembly)
            co_return runtime::failure(detail::path_error(errc::out_of_range));
        std::optional<detail::wal_write_descriptor::pointer> descriptor;
        if (admitted) {
            // The final targets change no geometry or cost, so the admitted
            // allowances cover the (possibly smaller) submitted group.
            if (
              extent.size() > admitted->extent_
              || group.retained_bound_ > admitted->retained_
              || *assembly > admitted->encoded_.retained_bytes())
                co_return runtime::failure(
                  detail::path_error(errc::wrong_context));
            group.encoding_.emplace(std::move(admitted->encoded_));
            descriptor.emplace(
              seastar::make_lw_shared<detail::wal_write_descriptor>(
                std::move(admitted->descriptor_), extent, admission.policy()));
        } else {
            auto encoded = budget_.try_reserve(*assembly);
            if (!encoded) co_return runtime::failure(encoded.error());
            group.encoding_.emplace(std::move(*encoded));
            auto made = detail::wal_write_descriptor::make(
              budget_,
              extent,
              admission.policy(),
              config_.children.execution_bytes);
            if (!made) co_return runtime::failure(made.error());
            descriptor.emplace(std::move(*made));
        }
        auto node = std::move(*descriptor);
        const auto members = group.size();
        node->group_.emplace(std::move(group));
        auto after = positions();
        if (admission_stopped_ || !after || after->reserved != before->reserved)
            co_return runtime::failure(detail::path_error(errc::closed));
        if (auto ready = admission.poll(); !ready)
            co_return runtime::failure(
              detail::path_error(ready.error().code()));
        auto linked = inflight_.push(node);
        if (!linked) co_return runtime::failure(linked.error());
        // Acceptance: no suspension or fallible allocation from linking through
        // cursor/membership installation and observer attachment.
        positions_->reserved
          = local_wal_cursor::make(before->reserved.incarnation(), end).value();
        reserved_members_ += members;
        ++statistics_.accepted_groups;
        wal_submission ticket{
          wal_captured_boundary{
            lifetime_, before->owner, positions_->reserved, reserved_members_},
          node->observe(),
          extent,
          static_cast<std::uint32_t>(members)};
        node->encoding_.emplace(
          seastar::with_scheduling_group(
            budget_.scheduling_group(),
            [this, node]() noexcept -> seastar::future<> {
                try {
                    return encode_group(node);
                } catch (...) {
                    node->fail(std::current_exception());
                    remember(node->failure());
                    changed_.broadcast();
                    return seastar::make_ready_future<>();
                }
            }));
        co_return std::move(ticket);
    }

    void remember(const runtime::first_failure& failure) noexcept {
        if (failure.exception()) first_.observe(failure.exception());
        if (failure.error()) first_.observe(*failure.error());
    }
    void fail_node(detail::wal_write_descriptor& node) noexcept {
        if (first_.exception())
            node.fail(first_.exception());
        else if (first_.error())
            node.fail(*first_.error());
    }
    void check_selected_head() {
        const auto current = control_.snapshot();
        if (!current)
            first_.observe(current.error());
        else if (current->fields.wal_head != current_file().head)
            first_.observe(detail::path_error(errc::wrong_context));
    }
    seastar::future<> encode_group(detail::wal_write_descriptor::pointer node) {
        auto& work = node->work();
        std::optional<bytes::fragmented_buffer_builder> builder;
        std::optional<codec::result<bytes::fragmented_buffer>> record;
        try {
            do {
                if (first_.failed()) {
                    fail_node(*node);
                    break;
                }
                auto& group = *node->group_;
                if (group.members_.size() != 1) {
                    builder.emplace(
                      bytes::fragmented_buffer_builder_config{
                        .initial_fragment_bytes = byte_count{1},
                        .max_fragment_bytes = byte_count{1},
                        .max_total_bytes = node->extent().size(),
                        .max_retained_bytes = group.retained_bound_,
                        .max_fragments = group.fragment_bound_});
                    auto reserved = builder->reserve_fragments(
                      item_count{group.fragment_bound_});
                    if (!reserved) {
                        node->fail(
                          detail::path_error(errc::resource_exhausted));
                        break;
                    }
                }
                for (auto& member : group.members_) {
                    if (first_.failed()) {
                        fail_node(*node);
                        break;
                    }
                    record.emplace(
                      co_await encode_wal_prepare(
                        std::move(member.input.batch),
                        *member.reserved,
                        work,
                        member.memory->codec_bytes,
                        config_.children.charge,
                        {member.reserved->wal.position().value()}));
                    if (!*record) {
                        node->fail(detail::path_error(record->error().code()));
                        break;
                    }
                    if (first_.failed()) {
                        fail_node(*node);
                        break;
                    }
                    auto ready = co_await work.checkpoint();
                    if (!ready) {
                        node->fail(detail::path_error(ready.error().code()));
                        break;
                    }
                    if (builder) {
                        auto appended = builder->append_buffer(
                          std::move(**record));
                        if (!appended) {
                            node->fail(
                              detail::path_error(errc::resource_exhausted));
                            break;
                        }
                        record.reset();
                    }
                }
                if (node->state() == detail::wal_write_state::done) break;
                if (first_.failed()) {
                    fail_node(*node);
                    break;
                }
                bytes::fragmented_buffer encoded;
                if (builder) {
                    auto result = builder->finish();
                    if (!result) {
                        node->fail(
                          detail::path_error(errc::resource_exhausted));
                        break;
                    }
                    encoded = std::move(*result);
                } else {
                    // A singleton already owns its complete encoded envelope.
                    // Transfer that owner without rebuilding its descriptors.
                    encoded = std::move(**record);
                }
                auto queued = node->encoded_group(
                  std::move(encoded), config_.children.charge);
                if (!queued)
                    node->fail(queued.error());
                else
                    ++statistics_.encoded_groups;
            } while (false);
        } catch (...) {
            node->fail(std::current_exception());
        }
        remember(node->failure());
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        record.reset();
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        builder.reset();
        // No suspension after wakeup: the owned future is complete before a
        // dispatcher can join it and retire the node.
        changed_.broadcast();
    }
    seastar::future<> dispatch() {
        dispatcher_started_.set_value();
        while (!dispatcher_stopping_ || !inflight_.empty() || extension_) {
            if (extension_ && extension_->available()) {
                extension_->get();
                extension_.reset();
                changed_.broadcast();
                continue;
            }
            maybe_extend();
            if (
              inflight_.empty()
              || inflight_.front()->state()
                   == detail::wal_write_state::encoding) {
                co_await changed_.when();
                continue;
            }
            if (
              inflight_.front()->state() == detail::wal_write_state::done
              || first_.failed()) {
                auto node = inflight_.front();
                if (node->state() != detail::wal_write_state::done)
                    fail_node(*node);
                if (node->encoding_) {
                    try {
                        co_await std::move(*node->encoding_);
                    } catch (...) {
                        node->fail(std::current_exception());
                    }
                    node->encoding_.reset();
                }
                co_await dispatch_work_.drain_inline(
                  dispatch_work_.byte_quantum(), dispatch_work_.item_quantum());
                node = inflight_.pop_completed_front();
                remember(node->failure());
                if (node->failure().failed())
                    prefix_failed_ = true;
                else if (!prefix_failed_)
                    positions_->write_complete
                      = local_wal_cursor::make(
                          positions_->reserved.incarnation(),
                          node->extent().end())
                          .value();
                changed_.broadcast();
                node->notify();
                continue;
            }
            std::array<
              detail::wal_write_descriptor::pointer,
              runtime::maximum_queued_file_writes>
              selected{};
            const auto prefix = inflight_.ready_prefix();
            const auto count = prefix.groups, fragments = prefix.fragments;
            const auto logical = prefix.logical, retained = prefix.retained;
            const auto position = inflight_.front()->extent().begin();
            // Small writes inside the zero-written range are pure overwrites:
            // synchronized completion makes them durable without a flush.
            // Anything else needs the barrier's, and waits for an extension
            // that may be zero-writing its range.
            auto& slot = current_file();
            const auto end = position.value() + logical.value();
            const bool synchronous = slot.sync
                                     && logical
                                          <= config_.synchronous_write_bytes
                                     && position.value() >= slot.zero_begin
                                     && end <= slot.zeroed_end;
            if (!synchronous && extending_) {
                // No further extension starts until this write is issued.
                plain_waiting_ = true;
                co_await changed_.when();
                continue;
            }
            plain_waiting_ = false;
            slot.issued_end = std::max(slot.issued_end, end);
            std::size_t index = 0;
            for (const auto& node : inflight_) {
                if (index == count) break;
                selected[index++] = node;
                node->dispatch();
            }
            KWAQUE_INVARIANT(
              invariant_id{"KQ-WAL-GATHER-ADMITTED"},
              count != 0,
              "admitted WAL group cannot fit one runtime write");
            runtime::first_failure failed;
            std::optional<bytes::fragmented_buffer_builder> gather;
            bytes::fragmented_buffer data;
            try {
                if (count != 1) {
                    gather.emplace(
                      bytes::fragmented_buffer_builder_config{
                        .initial_fragment_bytes = byte_count{1},
                        .max_fragment_bytes = byte_count{1},
                        .max_total_bytes = logical,
                        .max_retained_bytes = retained,
                        .max_fragments = fragments});
                    auto ready = gather->reserve_fragments(
                      item_count{fragments});
                    if (!ready)
                        failed.observe(
                          detail::path_error(errc::resource_exhausted));
                }
                for (std::size_t i = 0; i < count; ++i) {
                    auto& node = *selected[i];
                    if (node.encoding_) {
                        auto execution = std::move(*node.encoding_);
                        node.encoding_.reset();
                        co_await std::move(execution);
                    }
                    co_await dispatch_work_.drain_inline(
                      dispatch_work_.byte_quantum(),
                      dispatch_work_.item_quantum());
                    if (failed.failed() || first_.failed()) break;
                    if (gather) {
                        auto appended = gather->append_buffer(
                          std::move(node.payload_));
                        if (!appended)
                            failed.observe(
                              detail::path_error(errc::resource_exhausted));
                    } else {
                        // One group already owns the complete write buffer.
                        data = std::move(node.payload_);
                    }
                }
                if (!failed.failed() && !first_.failed()) {
                    if (gather) {
                        auto complete = gather->finish();
                        if (!complete)
                            failed.observe(
                              detail::path_error(errc::resource_exhausted));
                        else
                            data = std::move(*complete);
                    }
                    if (!failed.failed()) {
                        ++statistics_.write_calls;
                        statistics_.gathered_groups += count - 1;
                        auto written = co_await (
                                         synchronous ? *slot.sync : *slot.file)
                                         .write(position, std::move(data));
                        failed.observe(written);
                        if (written && *written != logical)
                            failed.observe(
                              detail::path_error(errc::io_failure));
                        if (!failed.failed()) {
                            if (synchronous)
                                ++statistics_.synchronized_writes;
                            else
                                ++slot.plain_writes;
                        }
                    }
                } else if (!failed.failed())
                    failed = first_;
            } catch (...) {
                failed.observe(std::current_exception());
            }
            remember(failed);
            // Every native operation above is joined, including its exceptional
            // result, before any gather member is settled or released.
            for (std::size_t i = 0; i < count; ++i) {
                if (failed.exception())
                    selected[i]->fail(failed.exception());
                else if (failed.error())
                    selected[i]->fail(*failed.error());
                else
                    selected[i]->complete(selected[i]->extent().size());
            }
            co_await dispatch_work_.drain_inline(
              dispatch_work_.byte_quantum(), dispatch_work_.item_quantum());
            data = bytes::fragmented_buffer{};
            gather.reset();
        }
    }
    seastar::future<wal_barrier_outcome> flush_cut(wal_captured_boundary cut) {
        wal_barrier_outcome result;
        try {
            while (!first_.failed()
                   && positions_->write_complete.position()
                        < cut.cursor_.position())
                co_await changed_.when();
            if (!first_.failed()) check_selected_head();
            if (
              !first_.failed()
              && positions_->durable.position() < cut.cursor_.position()) {
                auto& slot = current_file();
                // Synchronized writes were durable when they completed. A
                // flush covers every ordinary write completed before it began,
                // so a zero-written file flushes only if one completed since.
                const auto covered = slot.plain_writes;
                if (!slot.sync || covered != slot.flushed_plain_writes) {
                    ++statistics_.flush_calls;
                    first_.observe(
                      co_await slot.file->flush(slot.completion->metadata()));
                    if (!first_.failed())
                        slot.flushed_plain_writes = std::max(
                          slot.flushed_plain_writes, covered);
                }
                if (!first_.failed()) check_selected_head();
                if (!first_.failed()) positions_->durable = cut.cursor_;
            }
            if (!first_.failed())
                result.receipt = wal_durable_receipt{std::move(cut)};
        } catch (...) {
            first_.observe(std::current_exception());
        }
        result.failure = first_;
        changed_.broadcast();
        co_return result;
    }
    seastar::future<wal_barrier_outcome>
    barrier_owned(wal_captured_boundary cut, seastar::gate::holder holder) {
        barrier_busy_ = true;
        auto idle = seastar::defer([this] noexcept { barrier_busy_ = false; });
        static_cast<void>(holder);
        try {
            co_return co_await flush_cut(std::move(cut));
        } catch (...) {
            first_.observe(std::current_exception());
        }
        changed_.broadcast();
        co_return wal_barrier_outcome{first_, {}};
    }
    wal_writer(
      control_type& control,
      allocator_type& ids,
      workload_budget& budget,
      wal_writer_config config,
      byte_count write_allocation,
      workload_reservation held,
      seastar::lw_shared_ptr<wal_captured_boundary::lifetime> lifetime)
      : held_(std::move(held))
      , control_(control)
      , ids_(ids)
      , budget_(budget)
      , config_(std::move(config))
      , write_allocation_(write_allocation)
      , lifetime_(std::move(lifetime))
      , inflight_(config_.maximum_descriptors, config_.maximum_pending_bytes) {}
    static seastar::future<runtime::result<void>> reject(errc code) {
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::failure(detail::path_error(code)));
    }
    static runtime::file_path parent_of(const runtime::file_path& path) {
        return runtime::file_path::make(
                 path.value().substr(0, path.value().rfind('/')))
          .value();
    }
    static runtime::file_name leaf_of(const runtime::file_path& path) {
        return runtime::file_name::make(
                 path.value().substr(path.value().rfind('/') + 1))
          .value();
    }
    bool published(const local_publication_outcome& outcome) {
        if (outcome.failure.exception())
            first_.observe(outcome.failure.exception());
        if (outcome.failure.error()) first_.observe(*outcome.failure.error());
        if (outcome.disposition != local_publication_disposition::durable)
            first_.observe(detail::path_error(errc::io_failure));
        return !first_.failed();
    }
    file_slot& current_file() noexcept { return slots_[active_slot_]; }
    const file_slot& current_file() const noexcept {
        return slots_[active_slot_];
    }
    file_slot& successor_file() noexcept { return slots_[1 - active_slot_]; }
    const file_slot& successor_file() const noexcept {
        return slots_[1 - active_slot_];
    }
    seastar::future<> close_file() { return close_slot(current_file()); }
    seastar::future<> close_slot(file_slot& slot) {
        if (slot.publisher) {
            try {
                auto closed = co_await slot.publisher->close();
                first_.observe(closed);
                if (closed) slot.publisher.reset();
            } catch (...) {
                first_.observe(std::current_exception());
            }
        }
        if (slot.completion) slot.completion->release_metadata();
        if (slot.sync) {
            try {
                first_.observe(co_await slot.sync->close());
            } catch (...) {
                first_.observe(std::current_exception());
            }
            if (slot.sync->state() == runtime::file_state::closed)
                slot.sync.reset();
        }
        if (slot.file) {
            try {
                first_.observe(co_await slot.file->close());
            } catch (...) {
                first_.observe(std::current_exception());
            }
            if (slot.file->state() == runtime::file_state::closed)
                slot.file.reset();
        }
        if (!slot.file) slot.completion.reset();
        if (!slot.file && !slot.sync && !slot.publisher)
            slot.preparation.reset();
    }
    static bool admission_pressure(runtime::operation_error error) noexcept {
        return detail::publication_admission_pressure(error);
    }
    static bool
    untouched_pressure(const local_publication_outcome& result) noexcept {
        return result.admission_rejected
               && result.disposition == local_publication_disposition::untouched
               && result.stage == local_publication_stage::none
               && !result.temporary_may_exist && !result.failure.exception()
               && result.failure.error()
               && admission_pressure(*result.failure.error());
    }
    static runtime::result<void>
    publication_result(const local_publication_outcome& result) {
        auto status = result.failure.outcome();
        if (!status) return status;
        if (result.disposition != local_publication_disposition::durable)
            return runtime::failure(detail::path_error(errc::io_failure));
        return {};
    }
    // Reused by initial creation and the single frozen successor. Each retry
    // resumes at a proved completed step; immutable bytes and served IDs are
    // never rewritten/reissued. Publication admission remains independently
    // fallible, even while the predecessor holds its completion reserve.
    bool stop_preparation(file_slot& slot) noexcept {
        if (!admission_stopped_) return false;
        slot.abandoned = true;
        return true;
    }
    seastar::future<runtime::result<void>> prepare_file(
      file_slot& slot,
      std::optional<local_wal_cursor> predecessor,
      codec::cooperative_work& work) {
        slot.retryable = false;
        slot.abandoned = false;
        if (stop_preparation(slot))
            co_return runtime::failure(detail::path_error(errc::closed));
        if (slot.ready) co_return runtime::result<void>{};
        const auto& spec = control_.spec_;
        const auto owner = spec.shard_owner(control_.shard_).value();
        const auto io = control_.limits_;
        auto& files = control_.files_;
        if (!slot.preparation) {
            auto held = budget_.try_reserve(
              byte_count{
                io.operation_bytes.value() + io.execution_bytes.value()});
            if (!held) {
                slot.retryable = admission_pressure(held.error());
                co_return runtime::failure(held.error());
            }
            auto handles = held->try_acquire_handles(
              config_.preallocation_bytes.value() != 0 ? 2U : 1U);
            if (!handles) {
                slot.retryable = admission_pressure(handles.error());
                co_return runtime::failure(handles.error());
            }
            slot.preparation.emplace(std::move(*held));
        }
        auto checked = co_await control_.owner_.validate(spec);
        if (!checked) co_return checked;
        if (stop_preparation(slot))
            co_return runtime::failure(detail::path_error(errc::closed));
        if (!slot.descriptor) {
            const auto layout = local_metadata_layout(
              local_metadata_kind::wal_descriptor,
              byte_count{predecessor ? 68U : 44U},
              byte_count{codec::envelope_prefix_bytes},
              config_.alignment,
              work.policy());
            if (!layout || layout->encoded_bytes() >= config_.capacity_bytes)
                co_return runtime::failure(
                  detail::path_error(errc::invalid_argument));
            const auto before = control_.snapshot();
            if (!before) co_return runtime::failure(before.error());
            if (ids_.busy_ || (ids_.wal_.remaining == 0 && control_.busy_)) {
                slot.retryable = true;
                co_return runtime::failure(
                  runtime::make_file_error(
                    errc::queue_full,
                    runtime::file_failure_detail::admission_not_dispatched));
            }
            auto id = co_await ids_.allocate_wal(work);
            if (!id) {
                const auto after = control_.snapshot();
                // Only an unchanged, unfenced control plus an explicit
                // admission failure permits another reservation attempt.
                slot.retryable = admission_pressure(id.error()) && after
                                 && before->generation == after->generation;
                co_return runtime::failure(id.error());
            }
            slot.descriptor.emplace(
              *id,
              predecessor,
              config_.alignment,
              config_.profile,
              runtime::file_position{layout->encoded_bytes().value()},
              config_.capacity_bytes);
            auto path = local_paths::make(spec.root).value().wal(
              control_.shard_, *id);
            if (!path) co_return runtime::failure(path.error());
            slot.path = std::move(*path);
        }
        if (stop_preparation(slot))
            co_return runtime::failure(detail::path_error(errc::closed));
        const auto parent = parent_of(*slot.path);
        if (!slot.ancestors_ready) {
            checked = co_await detail::ensure_local_directory(
              files, spec, parent_of(parent), leaf_of(parent), work);
            if (!checked) co_return checked;
            slot.ancestors_ready = true;
        }
        if (stop_preparation(slot))
            co_return runtime::failure(detail::path_error(errc::closed));
        const auto header = local_metadata_header::make(
                              local_metadata_kind::wal_descriptor,
                              owner,
                              local_publication_generation::make(1).value())
                              .value();
        if (
          slot.header_publication.disposition
          != local_publication_disposition::durable) {
            // A prior safe rejection has no namespace debt. Release its
            // diagnostic reservation before attempting nested admission again.
            slot.header_publication = local_publication_outcome{};
            const local_metadata_payload payload{*slot.descriptor};
            auto encoded = co_await encode_local_metadata(
              {header, config_.alignment},
              payload,
              work,
              io.operation_bytes,
              io.charge);
            if (!encoded)
                co_return runtime::failure(
                  detail::path_error(encoded.error().code()));
            slot.head.emplace(slot.descriptor->incarnation, encoded->digest);
            if (stop_preparation(slot))
                co_return runtime::failure(detail::path_error(errc::closed));
            checked = co_await control_.owner_.validate(spec);
            if (!checked) co_return checked;
            if (stop_preparation(slot))
                co_return runtime::failure(detail::path_error(errc::closed));
            slot.publisher.emplace(
              files,
              budget_,
              local_publication_target{
                owner,
                spec.root,
                parent,
                leaf_of(*slot.path),
                runtime::file_rename_policy::no_replace,
                {}});
            slot.header_publication = co_await slot.publisher->publish(
              {owner, header.generation(), {}},
              std::move(encoded->bytes),
              work);
            runtime::first_failure failed = slot.header_publication.failure;
            const auto closed = co_await slot.publisher->close();
            failed.observe(closed);
            if (closed) slot.publisher.reset();
            slot.retryable = closed
                             && untouched_pressure(slot.header_publication);
            if (failed.failed()) co_return failed.outcome();
            checked = publication_result(slot.header_publication);
            if (!checked) co_return checked;
        }
        if (stop_preparation(slot))
            co_return runtime::failure(detail::path_error(errc::closed));
        if (!slot.file) {
            auto opened = co_await files.open(
              *slot.path,
              {.access = runtime::file_access::read_write,
               .close_policy = runtime::file_close_policy::checked});
            if (!opened) {
                slot.retryable = admission_pressure(opened.error());
                co_return runtime::failure(opened.error());
            }
            slot.file.emplace(std::move(*opened));
            auto limited = slot.file->limit_write_allocation(write_allocation_);
            if (!limited) co_return runtime::failure(limited.error());
            auto geometry = slot.file->geometry();
            if (
              !geometry
              || !geometry->supports_disk_alignment(config_.alignment.bytes()))
                co_return runtime::failure(
                  detail::path_error(errc::invalid_argument));
        }
        if (stop_preparation(slot))
            co_return runtime::failure(detail::path_error(errc::closed));
        if (!slot.verified) {
            checked = co_await verify_open_file(slot, header, work);
            if (!checked) {
                slot.retryable = admission_pressure(checked.error());
                co_return checked;
            }
            slot.verified = true;
        }
        if (stop_preparation(slot))
            co_return runtime::failure(detail::path_error(errc::closed));
        if (!slot.completion) {
            auto completion = completion_resources::make(
              budget_, *slot.file, config_.completion);
            if (!completion) {
                slot.retryable = admission_pressure(completion.error());
                co_return runtime::failure(completion.error());
            }
            slot.completion.emplace(std::move(*completion));
        }
        if (stop_preparation(slot))
            co_return runtime::failure(detail::path_error(errc::closed));
        if (config_.preallocation_bytes.value() != 0 && !slot.sync) {
            checked = co_await preallocate(slot, work);
            if (!checked) {
                slot.retryable = admission_pressure(checked.error())
                                 && slot.zeroed_end == 0;
                co_return checked;
            }
        }
        slot.preparation.reset();
        slot.ready = true;
        co_return runtime::result<void>{};
    }
    static void clear_slot(file_slot& slot) {
        KWAQUE_INVARIANT(
          invariant_id{"KQ-WAL-SLOT-CLOSED"},
          !slot.file && !slot.sync && !slot.completion && !slot.publisher,
          "reusing WAL slot before joined close");
        slot.preparation.reset();
        slot.path.reset();
        slot.descriptor.reset();
        slot.head.reset();
        slot.header_publication = local_publication_outcome{};
        slot.head_publication = local_publication_outcome{};
        slot.zero_begin = slot.zeroed_end = slot.issued_end = 0;
        slot.plain_writes = slot.flushed_plain_writes = 0;
        slot.ancestors_ready = slot.verified = slot.ready = slot.retryable
          = slot.abandoned = false;
    }
    seastar::future<runtime::result<void>> rotate_owned(
      wal_captured_boundary cut,
      byte_count required_bytes,
      codec::limits policy,
      seastar::gate::holder holder) {
        static_cast<void>(holder);
        rotation_busy_ = true;
        auto idle = seastar::defer([this] noexcept { rotation_busy_ = false; });
        if (!rotation_)
            rotation_.emplace(
              std::move(cut), *current_file().head, required_bytes, policy);
        seastar::abort_source abort;
        codec::cooperative_work work{rotation_->policy, abort};
        try {
            do {
                auto& next = successor_file();
                auto prepared = co_await prepare_file(
                  next, rotation_->cut.cursor_, work);
                if (!prepared) {
                    if ((next.retryable || next.abandoned) && !first_.failed())
                        co_return prepared;
                    first_.observe(prepared);
                    break;
                }
                if (first_.failed()) break;
                if (admission_stopped_)
                    co_return runtime::failure(
                      detail::path_error(errc::closed));
                if (!rotation_->barrier) {
                    auto durable = co_await flush_cut(rotation_->cut);
                    remember(durable.failure);
                    if (first_.failed()) break;
                    if (
                      !durable.receipt
                      || durable.receipt->boundary().token_ != lifetime_
                      || durable.receipt->boundary().cursor_
                           != rotation_->cut.cursor_
                      || durable.receipt->boundary().members_
                           != rotation_->cut.members_
                      || !inflight_.empty()) {
                        first_.observe(detail::path_error(errc::wrong_context));
                        break;
                    }
                    rotation_->barrier.emplace(std::move(*durable.receipt));
                }
                if (!rotation_->old_closed) {
                    // No extension starts while a rotation is pending.
                    while (extending_)
                        co_await changed_.when();
                    co_await close_file();
                    if (first_.failed()) break;
                    rotation_->old_closed = true;
                }
                if (admission_stopped_)
                    co_return runtime::failure(
                      detail::path_error(errc::closed));
                next.head_publication = local_publication_outcome{};
                next.head_publication = co_await control_.update(
                  [old = rotation_->old_head, head = *next.head](
                    local_shard_control& fields) -> runtime::result<void> {
                      if (fields.wal_head != old)
                          return runtime::failure(
                            detail::path_error(errc::wrong_context));
                      fields.wal_head = head;
                      return {};
                  },
                  [this](
                    this auto,
                    const local_control_snapshot& before,
                    const local_shard_control& after,
                    codec::cooperative_work&)
                    -> seastar::future<runtime::result<void>> {
                      const auto& next = successor_file();
                      if (
                        !rotation_ || !rotation_->barrier
                        || !rotation_->old_closed || current_file().file
                        || !next.ready || !next.completion
                        || next.header_publication.failure.failed()
                        || next.header_publication.disposition
                             != local_publication_disposition::durable
                        || before.fields.wal_head != rotation_->old_head
                        || after.wal_head != next.head
                        || rotation_->barrier->boundary().cursor_
                             != rotation_->cut.cursor_)
                          co_return runtime::failure(
                            detail::path_error(errc::wrong_context));
                      co_return runtime::result<void>{};
                  },
                  work);
                if (
                  untouched_pressure(next.head_publication)
                  && !control_.fenced() && !first_.failed())
                    co_return next.head_publication.failure.outcome();
                if (!published(next.head_publication)) break;
                const auto selected = control_.snapshot();
                if (!selected || selected->fields.wal_head != next.head) {
                    first_.observe(detail::path_error(errc::wrong_context));
                    break;
                }
                if (admission_stopped_)
                    co_return runtime::failure(
                      detail::path_error(errc::closed));
                // All publication and close effects succeeded. Install the
                // empty successor facts and switch only the stable slot index.
                const auto cursor = local_wal_cursor::make(
                                      next.descriptor->incarnation,
                                      next.descriptor->data_start)
                                      .value();
                positions_ = wal_writer_positions{
                  positions_->owner, cursor, cursor, cursor};
                reserved_members_ = 0;
                active_slot_ = 1 - active_slot_;
                rotation_.reset();
                clear_slot(successor_file());
                ++statistics_.rotations;
                co_return runtime::result<void>{};
            } while (false);
        } catch (...) {
            first_.observe(std::current_exception());
        }
        changed_.broadcast();
        co_return first_.outcome();
    }
    seastar::future<runtime::result<void>>
    bootstrap_owned(codec::limits policy, seastar::gate::holder holder) {
        started_ = true;
        static_cast<void>(holder);
        seastar::abort_source execution_abort;
        codec::cooperative_work work{policy, execution_abort};
        const auto& spec = control_.spec_;
        const auto owner = spec.shard_owner(control_.shard_).value();
        bool stopped = false;
        try {
            do {
                auto current = control_.snapshot();
                if (
                  !current || current->fields.wal_head
                  || current->fields.checkpoint) {
                    first_.observe(detail::path_error(errc::wrong_context));
                    break;
                }
                if (admission_stopped_) {
                    stopped = true;
                    break;
                }
                auto prepared = co_await prepare_file(
                  current_file(), std::nullopt, work);
                if (!prepared) {
                    if (
                      current_file().abandoned
                      || (admission_stopped_ && current_file().retryable))
                        stopped = true;
                    else
                        first_.observe(prepared);
                    break;
                }
                if (admission_stopped_) {
                    stopped = true;
                    break;
                }
                // The dependency validator owns this gate-protected readiness
                // capture through publication. Only this completed sequence can
                // reach it; a read of an existing header cannot create it.
                current_file().head_publication = co_await control_.update(
                  [head = *current_file().head](
                    local_shard_control& fields) -> runtime::result<void> {
                      if (fields.wal_head || fields.checkpoint)
                          return runtime::failure(
                            detail::path_error(errc::wrong_context));
                      fields.wal_head = head;
                      return {};
                  },
                  [this](
                    this auto,
                    const local_control_snapshot& before,
                    const local_shard_control& after,
                    codec::cooperative_work&)
                    -> seastar::future<runtime::result<void>> {
                      if (
                        before.fields.wal_head || before.fields.checkpoint
                        || !current_file().completion
                        || current_file().header_publication.failure.failed()
                        || current_file().header_publication.disposition
                             != local_publication_disposition::durable
                        || after.wal_head != current_file().head)
                          co_return runtime::failure(
                            detail::path_error(errc::wrong_context));
                      co_return runtime::result<void>{};
                  },
                  work);
                if (!published(current_file().head_publication)) break;
                current = control_.snapshot();
                if (
                  !current || current->fields.wal_head != current_file().head) {
                    first_.observe(detail::path_error(errc::wrong_context));
                    break;
                }
                if (admission_stopped_) {
                    stopped = true;
                    break;
                }
                if (!co_await start_current(owner)) {
                    stopped = admission_stopped_;
                    break;
                }
            } while (false);
        } catch (...) {
            first_.observe(std::current_exception());
        }
        if (first_.failed() || stopped) {
            active_ = false;
            try {
                co_await close_file();
            } catch (...) {
                first_.observe(std::current_exception());
            }
        }
        if (first_.failed()) co_return first_.outcome();
        if (stopped)
            co_return runtime::failure(detail::path_error(errc::closed));
        co_return runtime::result<void>{};
    }
    // Installs the current file's empty positions and starts its dispatcher;
    // false when a failure or a stop came first.
    seastar::future<bool> start_current(local_store_context owner) {
        const auto cursor = local_wal_cursor::make(
                              current_file().descriptor->incarnation,
                              current_file().descriptor->data_start)
                              .value();
        positions_.emplace(owner, cursor, cursor, cursor);
        dispatcher_.emplace(
          seastar::with_scheduling_group(
            budget_.scheduling_group(), [this]() noexcept -> seastar::future<> {
                try {
                    return dispatch();
                } catch (...) {
                    first_.observe(std::current_exception());
                    dispatcher_started_.set_value();
                    return seastar::make_ready_future<>();
                }
            }));
        co_await dispatcher_started_.get_future();
        if (first_.failed() || admission_stopped_) {
            dispatcher_stopping_ = true;
            changed_.broadcast();
            co_return false;
        }
        active_ = true;
        co_return true;
    }
    // One new flush of the recovered head through a read-only handle: the
    // head is never written again.
    seastar::future<runtime::result<void>> flush_recovered(
      model::wal_incarnation_id head,
      runtime::file_position content_end,
      codec::cooperative_work& work) {
        const auto& spec = control_.spec_;
        auto& files = control_.files_;
        auto held = budget_.try_reserve(control_.limits_.execution_bytes);
        if (!held) co_return runtime::failure(held.error());
        if (auto handles = held->try_acquire_handles(1); !handles)
            co_return runtime::failure(handles.error());
        auto path = local_paths::make(spec.root).value().wal(
          control_.shard_, head);
        if (!path) co_return runtime::failure(path.error());
        if (
          auto inspected = co_await inspect_local_path(
            files, spec.root, *path, runtime::file_kind::regular, work);
          !inspected)
            co_return inspected;
        auto opened = co_await files.open(
          *path,
          {.access = runtime::file_access::read_only,
           .close_policy = runtime::file_close_policy::checked});
        if (!opened) co_return runtime::failure(opened.error());
        auto file = std::move(*opened);
        runtime::first_failure failed;
        try {
            do {
                auto size = co_await file.size();
                if (!size) {
                    failed.observe(size);
                    break;
                }
                // The classified content must still be there.
                if (*size < content_end.value()) {
                    failed.observe(detail::path_error(errc::truncated_data));
                    break;
                }
                auto unit = file.try_reserve_metadata();
                if (!unit) {
                    failed.observe(unit);
                    break;
                }
                failed.observe(co_await file.flush(*unit));
            } while (false);
        } catch (...) {
            failed.observe(std::current_exception());
        }
        try {
            failed.observe(co_await file.close());
        } catch (...) {
            failed.observe(std::current_exception());
        }
        co_return failed.outcome();
    }
    seastar::future<runtime::result<void>> activate_owned(
      local_wal_cursor content_end,
      codec::limits policy,
      seastar::gate::holder holder) {
        started_ = true;
        static_cast<void>(holder);
        seastar::abort_source execution_abort;
        codec::cooperative_work work{policy, execution_abort};
        const auto& spec = control_.spec_;
        const auto owner = spec.shard_owner(control_.shard_).value();
        bool stopped = false;
        try {
            do {
                auto current = control_.snapshot();
                if (
                  !current || !current->fields.wal_head
                  || current->fields.wal_head->incarnation
                       != content_end.incarnation()) {
                    first_.observe(detail::path_error(errc::wrong_context));
                    break;
                }
                const auto old = *current->fields.wal_head;
                if (admission_stopped_) {
                    stopped = true;
                    break;
                }
                // The head's own descriptor bounds the cursor.
                auto loaded = co_await load_local_wal_head(
                  control_.files_,
                  spec,
                  control_.shard_,
                  old,
                  budget_,
                  control_.limits_,
                  work);
                if (!loaded) {
                    first_.observe(loaded);
                    break;
                }
                const auto& head = std::get<local_wal_descriptor>(
                  loaded->value.payload());
                const auto at = content_end.position();
                if (
                  at < head.data_start || !head.alignment.aligned(at)
                  || at.value() > head.capacity_bytes.value()
                  || at.value() > loaded->file_bytes) {
                    first_.observe(detail::path_error(errc::wrong_context));
                    break;
                }
                // The successor is prepared while the head's fresh barrier
                // runs: nothing names it until the control update below,
                // after both succeeded. A failed barrier leaves a prepared
                // successor unnamed, as a failed rotation does.
                codec::cooperative_work barrier_work{policy, execution_abort};
                auto [fresh, successor] = co_await seastar::when_all(
                  flush_recovered(old.incarnation, at, barrier_work),
                  prepare_file(current_file(), content_end, work));
                std::exception_ptr thrown;
                runtime::result<void> flushed{}, prepared{};
                try {
                    flushed = fresh.get();
                } catch (...) {
                    thrown = std::current_exception();
                }
                try {
                    prepared = successor.get();
                } catch (...) {
                    if (!thrown) thrown = std::current_exception();
                }
                if (thrown) std::rethrow_exception(thrown);
                if (!flushed) {
                    first_.observe(flushed);
                    break;
                }
                recovered_flushed_ = true;
                // A preparation refused admission while the barrier held its
                // share changed nothing; it runs once more, alone, as an
                // untouched rotation preparation may.
                if (
                  !prepared && current_file().retryable
                  && !current_file().abandoned && !admission_stopped_)
                    prepared = co_await prepare_file(
                      current_file(), content_end, work);
                if (!prepared) {
                    if (
                      current_file().abandoned
                      || (admission_stopped_ && current_file().retryable))
                        stopped = true;
                    else
                        first_.observe(prepared);
                    break;
                }
                if (admission_stopped_) {
                    stopped = true;
                    break;
                }
                current_file().head_publication = co_await control_.update(
                  [old, head = *current_file().head](
                    local_shard_control& fields) -> runtime::result<void> {
                      if (fields.wal_head != old)
                          return runtime::failure(
                            detail::path_error(errc::wrong_context));
                      fields.wal_head = head;
                      return {};
                  },
                  [this, old](
                    this auto,
                    const local_control_snapshot& before,
                    const local_shard_control& after,
                    codec::cooperative_work&)
                    -> seastar::future<runtime::result<void>> {
                      if (
                        !recovered_flushed_ || before.fields.wal_head != old
                        || !current_file().completion
                        || current_file().header_publication.failure.failed()
                        || current_file().header_publication.disposition
                             != local_publication_disposition::durable
                        || after.wal_head != current_file().head)
                          co_return runtime::failure(
                            detail::path_error(errc::wrong_context));
                      co_return runtime::result<void>{};
                  },
                  work);
                if (!published(current_file().head_publication)) break;
                current = control_.snapshot();
                if (
                  !current || current->fields.wal_head != current_file().head) {
                    first_.observe(detail::path_error(errc::wrong_context));
                    break;
                }
                if (admission_stopped_) {
                    stopped = true;
                    break;
                }
                if (!co_await start_current(owner)) {
                    stopped = admission_stopped_;
                    break;
                }
            } while (false);
        } catch (...) {
            first_.observe(std::current_exception());
        }
        if (first_.failed() || stopped) {
            active_ = false;
            try {
                co_await close_file();
            } catch (...) {
                first_.observe(std::current_exception());
            }
        }
        if (first_.failed()) co_return first_.outcome();
        if (stopped)
            co_return runtime::failure(detail::path_error(errc::closed));
        co_return runtime::result<void>{};
    }
    seastar::future<runtime::result<void>> verify_open_file(
      file_slot& slot,
      local_metadata_header header,
      codec::cooperative_work& work) {
        const auto io = control_.limits_;
        auto size = co_await slot.file->size();
        if (!size) co_return runtime::failure(size.error());
        if (*size != slot.descriptor->data_start.value())
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        auto raw = co_await slot.file->read(
          runtime::file_position{}, byte_count{*size});
        if (!raw) co_return runtime::failure(raw.error());
        bytes::fragmented_buffer_parser input{std::move(*raw).take_data()};
        auto memory = detail::metadata_file_budget(input, io, work);
        if (!memory)
            co_return runtime::failure(
              detail::path_error(memory.error().code()));
        local_metadata_expectation expected{header, config_.alignment};
        expected.wal_incarnation = slot.descriptor->incarnation;
        expected.digest = slot.head->header_digest;
        expected.encoded_bytes = byte_count{*size};
        auto decoded = co_await decode_local_metadata(
          input, expected, *memory, work, {}, codec::input_boundary::complete);
        if (!decoded)
            co_return runtime::failure(
              detail::path_error(decoded.error().code()));
        if (
          !input.at_end()
          || std::get<local_wal_descriptor>(decoded->value.payload())
               != *slot.descriptor)
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        auto after = co_await slot.file->size();
        if (!after) co_return runtime::failure(after.error());
        if (*after != *size)
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        co_return runtime::result<void>{};
    }

    // Zero-writes [data start, window end) of a prepared file through its
    // ordinary handle and makes it durable once, then opens its
    // synchronized-write handle. A retry resumes after completed zeroing.
    seastar::future<runtime::result<void>>
    preallocate(file_slot& slot, codec::cooperative_work& work) {
        const auto alignment = config_.alignment.bytes().value();
        const auto start = slot.descriptor->data_start.value();
        const auto limit = config_.capacity_bytes.value();
        if (slot.zeroed_end == 0) {
            if (start >= limit) co_return runtime::result<void>{};
            const auto window
              = std::min(config_.preallocation_bytes.value(), limit - start)
                / alignment * alignment;
            if (window == 0) co_return runtime::result<void>{};
            if (zero_.empty())
                zero_ = detail::make_zero_chunk(write_allocation_);
            // Reserve the whole range first, then zero-write it: otherwise
            // the zero writes fragment the file into many small extents.
            auto reserved = co_await slot.file->allocate(
              runtime::file_position{start}, byte_count{window});
            if (!reserved) co_return reserved;
            auto zeroed = co_await detail::zero_fill(
              *slot.file, zero_, start, start + window, work);
            if (!zeroed) co_return zeroed;
            auto synced = co_await slot.file->flush(
              slot.completion->metadata());
            if (!synced) co_return synced;
            slot.zero_begin = start;
            slot.zeroed_end = start + window;
        }
        auto opened = co_await control_.files_.open(
          *slot.path,
          {.access = runtime::file_access::read_write,
           .close_policy = runtime::file_close_policy::checked,
           .synchronous = true});
        if (!opened) co_return runtime::failure(opened.error());
        slot.sync.emplace(std::move(*opened));
        if (
          auto limited = slot.sync->limit_write_allocation(write_allocation_);
          !limited)
            co_return runtime::failure(limited.error());
        co_return runtime::result<void>{};
    }

    // Starts zero-writing the next extension of the current file when its
    // zero-written range no longer reaches far enough past the reservations.
    // It begins after every write already issued, so it never overwrites
    // frames; ordinary writes wait for it.
    void maybe_extend() noexcept {
        const auto step = config_.preallocation_extension_bytes.value();
        if (
          step == 0 || extension_ || extending_ || plain_waiting_ || rotation_
          || !positions_ || dispatcher_stopping_ || admission_stopped_
          || first_.failed())
            return;
        auto& slot = current_file();
        if (!slot.sync || !slot.file) return;
        const auto alignment = config_.alignment.bytes().value();
        const auto limit = config_.capacity_bytes.value();
        const auto reserved = positions_->reserved.position().value();
        if (slot.zeroed_end > reserved && slot.zeroed_end - reserved >= step)
            return;
        const auto begin = std::max(slot.zeroed_end, slot.issued_end);
        if (begin >= limit) return;
        const auto length = std::max(step / alignment, std::uint64_t{1})
                            * alignment;
        const auto end = std::min(limit, begin + std::min(length, limit));
        extending_ = true;
        try {
            extension_.emplace(extend_window(slot, begin, end));
        } catch (...) {
            // Only speed depends on the extension; try again later.
            extending_ = false;
        }
    }

    // Zero-writes [begin, end), makes it durable and only then admits
    // synchronized writes there. Durability of the zeros is only for speed:
    // a synchronized write makes its own blocks durable either way.
    seastar::future<>
    extend_window(file_slot& slot, std::uint64_t begin, std::uint64_t end) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        runtime::first_failure failed;
        try {
            failed.observe(
              co_await detail::zero_fill(*slot.file, zero_, begin, end, work));
            // Each handle sizes the file once, when opened, and resizes it
            // to the end of any write it believes extends the file. Set the
            // synchronized handle's size to the new end first, while no
            // write is past it, so it never cuts what this handle wrote.
            if (!failed.failed())
                failed.observe(co_await slot.sync->truncate(end));
            if (!failed.failed()) {
                auto synced = co_await slot.file->flush();
                if (!synced && synced.error().code() != errc::queue_full)
                    failed.observe(synced);
            }
        } catch (...) {
            failed.observe(std::current_exception());
        }
        if (failed.exception())
            first_.observe(failed.exception());
        else if (failed.error())
            first_.observe(*failed.error());
        else {
            if (begin > slot.zeroed_end) slot.zero_begin = begin;
            slot.zeroed_end = end;
            ++statistics_.extensions;
        }
        extending_ = false;
        changed_.broadcast();
    }

    workload_reservation held_;
    control_type& control_;
    allocator_type& ids_;
    workload_budget& budget_;
    wal_writer_config config_;
    byte_count write_allocation_;
    seastar::lw_shared_ptr<wal_captured_boundary::lifetime> lifetime_;
    detail::wal_descriptor_queue inflight_;
    std::array<file_slot, 2> slots_;
    std::size_t active_slot_{0};
    std::optional<rotation_state> rotation_;
    std::optional<wal_writer_positions> positions_;
    runtime::first_failure first_;
    wal_writer_statistics statistics_;
    std::uint64_t reserved_members_{0};
    seastar::abort_source dispatch_abort_;
    codec::cooperative_work dispatch_work_{
      codec::limits::defaults(), dispatch_abort_};
    seastar::condition_variable changed_;
    seastar::promise<> dispatcher_started_;
    std::optional<seastar::future<>> dispatcher_;
    // Background extension of the current file's zero-written range, joined
    // by the dispatcher; it holds the ordinary-write slot while running.
    std::optional<seastar::future<>> extension_;
    bool extending_{false}, plain_waiting_{false};
    bytes::fragmented_buffer zero_;
    seastar::gate operations_;
    bool started_{false}, active_{false}, preflight_busy_{false};
    bool closing_{false}, closed_{false}, operations_closed_{false};
    bool rotation_busy_{false};
    seastar::optimized_optional<seastar::abort_source::subscription>
      shutdown_subscription_;
    bool shutdown_bound_{false}, admission_stopped_{false};
    bool dispatcher_stopping_{false}, barrier_busy_{false},
      prefix_failed_{false};
    wal_start_intent intent_{wal_start_intent::known_unactivated};
    // The recovered head's fresh barrier completed.
    bool recovered_flushed_{false};
};
} // namespace kwaque::storage
