#pragma once

#include "src/base/units.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/codec/format_registry.h"
#include "src/storage/completion_resources.h"
#include "src/storage/extent_verifier.h"
#include "src/storage/local_generation.h"
#include "src/storage/segment_group.h"
#include "src/storage/segment_seal.h"
#include "src/storage/zero_fill.h"

#include <seastar/core/shared_future.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/util/defer.hh>

#include <algorithm>
#include <array>
#include <exception>
#include <memory>
#include <optional>
#include <span>
#include <utility>

namespace kwaque::storage {
struct segment_writer_config final {
    // Required for creation; independently reserved by the local ID owner,
    // never derived from SC. Metadata-only open needs no future object ID.
    std::optional<local_object_sequence> retry_object;
    local_store_io_limits metadata;
    segment_admission_limits admission{};
    completion_resource_limits completion{};
    std::uint32_t maximum_groups{64};
    byte_count maximum_pending_bytes{runtime::maximum_file_io_bytes};
    // Creation writes zeros this far past the header (capped by the data
    // limit) and makes them durable once. Group writes inside that range then
    // overwrite blocks that are already allocated and inside the file size,
    // so their durability needs no allocation metadata. Zero disables it.
    // Sealing truncates the file back to its exact end.
    byte_count preallocation_bytes{};
    // Keeps the zero-written range ahead of the reservations: whenever less
    // than this remains past the reserved end, the owner zero-writes this
    // much more in the background and makes it durable before admitting
    // synchronized writes there. Groups past its start wait for it. Zero
    // keeps the creation window fixed; nonzero requires a window.
    byte_count preallocation_extension_bytes{};
    // A gathered write no larger than this, wholly inside the zero-written
    // range, uses a second handle opened for synchronized writes: its
    // completion is durable, so a barrier covering only such writes needs no
    // flush. Larger writes use the ordinary handle and the barrier's flush.
    byte_count synchronous_write_bytes{byte_count{1_MiB}};
    // Synchronized writes in flight at once. Each group is written as soon
    // as it is assembled instead of waiting for the previous write; results
    // still apply in file order. Ordinary writes keep one in flight.
    std::uint32_t concurrent_writes{4};
    // One policy for this owner's growing extent. Inputs validated elsewhere
    // are revalidated under it before freezing; it never changes during growth.
    codec::limits policy{codec::limits::defaults()};
};
struct segment_creation_progress final {
    local_publication_outcome descriptor, data, publication;
};

// Owns one supplied SC/descriptor and its handles. Directory ownership,
// backend and budget outlive joined close. Creation never resumes an existing
// name. Recovered active and recovering opens verify metadata; sealed opens
// also require the independently pinned complete extent. Neither entrance
// grants append authority. A recovered segment is never appended again: only
// a durable decision's recovered seal changes it.
template<
  runtime::file_system_backend Backend,
  typename Owner,
  runtime::monotonic_clock Clock>
class segment_writer final : public runtime::shard_affine {
    static_assert(local_directory_owner<Owner>);
    friend class segment_writer_test_access;

public:
    [[nodiscard]] static runtime::result<std::unique_ptr<segment_writer>>
    make_new(
      Backend& files,
      Owner& owner,
      const local_device_spec& spec,
      std::uint32_t shard,
      local_segment_descriptor descriptor,
      workload_budget& budget,
      segment_writer_config config) {
        if (descriptor.layout != local_layout_kind::initial)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        return make(
          files,
          owner,
          spec,
          shard,
          std::move(descriptor),
          budget,
          config,
          true);
    }
    segment_writer(const segment_writer&) = delete;
    segment_writer& operator=(const segment_writer&) = delete;
    ~segment_writer() {
        assert_current();
        KWAQUE_INVARIANT(
          invariant_id{"KQ-SEGMENT-DRAINED"},
          closed_ && inflight_.empty() && digesting_.empty(),
          "segment owner destroyed before joined close");
    }

    [[nodiscard]] seastar::future<runtime::result<void>>
    create_new(codec::cooperative_work& work) {
        assert_current();
        if (entered_ || closing_ || closed_)
            co_return runtime::failure(detail::path_error(errc::closed));
        if (auto ready = work.poll(); !ready)
            co_return runtime::failure(
              detail::path_error(ready.error().code()));
        entered_ = true;
        auto holder = operations_.hold();
        try {
            co_await create_owned(work);
        } catch (...) {
            lifetime_->failure.observe(std::current_exception());
        }
        if (closing_ && !lifetime_->failure.failed())
            co_return runtime::failure(detail::path_error(errc::closed));
        co_return lifetime_->failure.outcome();
    }

    [[nodiscard]] static seastar::future<
      runtime::result<std::unique_ptr<segment_writer>>>
    open_existing(
      Backend& files,
      Owner& owner,
      const local_device_spec& spec,
      std::uint32_t shard,
      const local_generation_expectation& expected,
      workload_budget& budget,
      segment_writer_config config,
      codec::cooperative_work& work,
      std::optional<segment_immutable_expectation> immutable = std::nullopt) {
        if (
          (expected.publication.state == local_object_state::sealed)
          != immutable.has_value())
            co_return runtime::failure(
              detail::path_error(errc::invalid_argument));
        if (expected.publication.roots.size() > 4 || expected.roots.size() > 4)
            co_return runtime::failure(
              detail::path_error(errc::invalid_argument));
        auto made = make(
          files,
          owner,
          spec,
          shard,
          expected.descriptor,
          budget,
          config,
          false);
        if (!made) co_return runtime::failure(made.error());
        auto output = std::move(*made);
        output->entered_ = true;
        output->recovered_ = true;
        output->immutable_ = immutable;
        try {
            output->read_publication_ = expected.publication;
            output->read_roots_.assign(
              expected.roots.begin(), expected.roots.end());
            auto generation = co_await local_generation_owner::open(
              files,
              owner,
              spec,
              shard,
              expected,
              budget,
              config.metadata,
              work);
            if (!generation)
                output->lifetime_->failure.observe(generation);
            else {
                output->generation_ = std::move(*generation);
                if (
                  expected.publication.state != local_object_state::active
                  && expected.publication.state
                       != local_object_state::recovering
                  && expected.publication.state != local_object_state::sealed)
                    output->lifetime_->failure.observe(
                      detail::path_error(errc::wrong_context));
                else {
                    output->publication_ = expected.generation;
                    output->append_ = expected.publication.state
                                          == local_object_state::sealed
                                        ? model::append_state::sealed
                                        : model::append_state::active;
                    output->lifetime_->failure.observe(
                      co_await output->open_data(false, work));
                    if (!output->lifetime_->failure.failed() && immutable)
                        output->lifetime_->failure.observe(
                          co_await output->verify_immutable(work));
                }
            }
        } catch (...) {
            output->lifetime_->failure.observe(std::current_exception());
        }
        if (output->lifetime_->failure.failed()) {
            // Cleanup all owned handles before returning either error channel.
            const auto failed = output->lifetime_->failure;
            try {
                static_cast<void>(co_await output->close());
            } catch (...) {
            }
            output.reset();
            auto result = failed.outcome();
            co_return runtime::failure(result.error());
        }
        co_return std::move(output);
    }

    // Opens a recovered segment only to execute a durable recovered-seal
    // decision. The current publication is selected by its fixed path and must
    // be active or recovering. Its pinned boundary and roots are not opened:
    // the decision supersedes them, and the seal verifies the whole extent
    // below its end. A seal interrupted after its root replaced a pinned
    // footer therefore still resumes. config.retry_object is reserved afresh
    // for every attempt.
    [[nodiscard]] static seastar::future<
      runtime::result<std::unique_ptr<segment_writer>>>
    open_recovered_seal(
      Backend& files,
      Owner& owner,
      const local_device_spec& spec,
      std::uint32_t shard,
      local_segment_descriptor descriptor,
      workload_budget& budget,
      segment_writer_config config,
      codec::cooperative_work& work) {
        if (
          !config.retry_object || !config.retry_object->is_valid()
          || config.metadata.operation_bytes < byte_count{64_KiB})
            co_return runtime::failure(
              detail::path_error(errc::invalid_argument));
        auto made = make(
          files,
          owner,
          spec,
          shard,
          std::move(descriptor),
          budget,
          config,
          false);
        if (!made) co_return runtime::failure(made.error());
        auto output = std::move(*made);
        output->entered_ = true;
        output->recovered_ = true;
        output->recovered_seal_ = true;
        try {
            output->lifetime_->failure.observe(
              co_await output->select_recovered(work));
            if (!output->lifetime_->failure.failed())
                output->lifetime_->failure.observe(
                  co_await output->open_data(false, work));
        } catch (...) {
            output->lifetime_->failure.observe(std::current_exception());
        }
        if (output->lifetime_->failure.failed()) {
            const auto failed = output->lifetime_->failure;
            try {
                static_cast<void>(co_await output->close());
            } catch (...) {
            }
            output.reset();
            auto result = failed.outcome();
            co_return runtime::failure(result.error());
        }
        co_return std::move(output);
    }

    [[nodiscard]] model::append_state append_state() const noexcept {
        assert_current();
        return append_;
    }
    [[nodiscard]] segment_handle_state handle_state() const noexcept {
        assert_current();
        return handle_;
    }
    [[nodiscard]] std::optional<runtime::monotonic_time>
    first_acceptance() const noexcept {
        assert_current();
        return first_acceptance_;
    }
    // Control observation only; rolling and the next generation belong to the
    // caller. An untouched empty lifetime has no age, including near overflow.
    [[nodiscard]] runtime::result<bool> roll_required() const {
        assert_current();
        if (
          closing_ || closed_ || lifetime_->failure.failed()
          || append_ != model::append_state::active)
            return runtime::failure(detail::path_error(errc::closed));
        if (recovered_) return true;
        if (!first_acceptance_) return false;
        return age_expired(Clock::now());
    }
    [[nodiscard]] bool recovered() const noexcept {
        assert_current();
        return recovered_;
    }
    [[nodiscard]] bool closed() const noexcept {
        assert_current();
        return closed_;
    }
    // Decoded metadata only; this is not complete-extent or durability proof.
    [[nodiscard]] std::optional<runtime::file_position>
    header_end() const noexcept {
        assert_current();
        if (!data_start_.value()) return std::nullopt;
        return data_start_;
    }
    [[nodiscard]] const runtime::first_failure& failure() const& noexcept {
        assert_current();
        return lifetime_->failure;
    }
    const runtime::first_failure& failure() const&& = delete;
    [[nodiscard]] const segment_creation_progress& creation() const& noexcept {
        assert_current();
        return creation_;
    }
    const segment_creation_progress& creation() const&& = delete;
    [[nodiscard]] const std::optional<segment_writer_positions>&
    progress() const& noexcept {
        assert_current();
        return positions_;
    }
    const std::optional<segment_writer_positions>& progress() const&& = delete;
    [[nodiscard]] std::optional<local_publication_generation>
    publication_generation() const noexcept {
        assert_current();
        return publication_;
    }
    [[nodiscard]] runtime::result<segment_captured_boundary> capture() const {
        assert_current();
        if (
          !positions_ || append_ != model::append_state::active || recovered_
          || lifetime_->failure.failed() || closing_ || closed_)
            return runtime::failure(detail::path_error(errc::closed));
        return segment_captured_boundary{
          lifetime_,
          {descriptor_.segment,
           descriptor_.alignment,
           data_start_,
           descriptor_.logical_origin,
           descriptor_.physical_origin,
           descriptor_.profile},
          positions_->reserved,
          reserved_footer_};
    }
    [[nodiscard]] runtime::result<void>
    validate_capture(const segment_captured_boundary& cut) const {
        auto current = capture();
        if (!current) return runtime::failure(current.error());
        if (
          cut.lifetime_ != lifetime_ || cut.history_ != current->history_
          || cut.end_.bytes > positions_->reserved.bytes
          || cut.end_.logical > positions_->reserved.logical
          || cut.end_.physical > positions_->reserved.physical
          || cut.end_.blocks > positions_->reserved.blocks
          || cut.end_.footers > positions_->reserved.footers)
            return runtime::failure(detail::path_error(errc::wrong_context));
        return {};
    }

    // No coordinate/byte mutation and no I/O. A successful preparation holds
    // future working/retry metadata; ordinary queue pressure stays a typed
    // resource error, distinct from impossible or roll-required geometry.
    // Input ownership stays with the caller. Freezing must consume and validate
    // those children; this preparation alone does not authorize execution.
    [[nodiscard]] runtime::result<segment_group_preparation> prepare_group(
      std::span<const encoded_assigned_batch> batches,
      codec::cooperative_work& work) {
        assert_current();
        if (batches.size() > maximum_segment_group_blocks)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        std::array<const encoded_assigned_batch*, maximum_segment_group_blocks>
          input{};
        for (std::size_t i = 0; i < batches.size(); ++i)
            input[i] = &batches[i];
        return prepare_group(std::span{input}.first(batches.size()), work);
    }
    // The same preparation for children owned elsewhere, e.g. by separate
    // requests. Each pointer is borrowed for this call only.
    [[nodiscard]] runtime::result<segment_group_preparation> prepare_group(
      std::span<const encoded_assigned_batch* const> batches,
      codec::cooperative_work& work) {
        assert_current();
        if (
          lifetime_->failure.failed() || closing_ || closed_
          || append_ != model::append_state::active)
            return runtime::failure(detail::path_error(errc::closed));
        if (batches.empty() || batches.size() > maximum_segment_group_blocks)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        if (recovered_)
            return segment_group_preparation{
              segment_capacity_decision::roll_required, {}};
        if (work.policy() != config_.policy)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        if (auto ready = work.poll(); !ready)
            return runtime::failure(detail::path_error(ready.error().code()));
        for (const auto* batch : batches)
            if (!batch)
                return runtime::failure(
                  detail::path_error(errc::invalid_argument));
        auto fixed = capacity();
        if (!fixed) return runtime::failure(fixed.error());
        auto plan = detail::plan_segment_capacity(
          **fixed, positions_->reserved, batches);
        if (!plan) return runtime::failure(plan.error());
        if (plan->decision != segment_capacity_decision::fits)
            return segment_group_preparation{plan->decision, {}};
        byte_count metadata{};
        // These are distinct bounded allocations, not one large vector.
        // Admit the donor's ordinary geometric spare capacity as well.
        for (const auto request : std::array{
               sizeof(segment_prepared_group),
               batches.size() * sizeof(detail::segment_plan_input),
               batches.size() * sizeof(segment_block_layout),
               batches.size() * sizeof(admitted_wal_batch::contents),
               batches.size() * sizeof(segment_block),
               std::min<std::size_t>(
                 maximum_segment_group_blocks, 2 * batches.size())
                 * sizeof(admitted_wal_batch)}) {
            const auto cost = budget_.allocation_charge(byte_count{request});
            if (!cost)
                return segment_group_preparation{
                  segment_capacity_decision::impossible, {}};
            metadata = metadata.checked_add(*cost).value();
        }
        auto required = std::optional<byte_count>{metadata};
        if (required) required = required->checked_add(plan->staging_bytes);
        if (required)
            required = required->checked_add(config_.admission.working_bytes);
        if (required)
            required = required->checked_add(config_.admission.execution_bytes);
        if (!required)
            return runtime::failure(detail::path_error(errc::out_of_range));
        std::size_t fragments = 3;
        byte_count retained = plan->staging_bytes;
        for (const auto* batch : batches) {
            fragments += batch->bytes().fragment_count() + 3;
            const auto next = retained.checked_add(
              batch->bytes().retained_bytes());
            if (!next)
                return runtime::failure(detail::path_error(errc::out_of_range));
            retained = *next;
        }
        const auto control = node_charge(fragments);
        if (!control)
            return segment_group_preparation{
              segment_capacity_decision::impossible, {}};
        if (!group_fits_budget(*required, plan->retry_metadata, *control))
            return segment_group_preparation{
              segment_capacity_decision::impossible, {}};
        auto aged = age_expired(Clock::now());
        if (!aged) return runtime::failure(aged.error());
        if (*aged)
            return segment_group_preparation{
              segment_capacity_decision::roll_required, {}};
        if (!group_fits_budget(*required, plan->retry_metadata, *control, true))
            return segment_group_preparation{
              segment_capacity_decision::roll_required, {}};
        auto held = budget_.try_reserve(*required);
        if (!held) {
            if (held.error().code() == errc::out_of_range)
                return segment_group_preparation{
                  segment_capacity_decision::impossible, {}};
            return runtime::failure(held.error());
        }
        auto retries = budget_.try_reserve(plan->retry_metadata);
        if (!retries) return runtime::failure(retries.error());
        auto node = budget_.try_reserve(*control);
        if (!node) return runtime::failure(node.error());
        if (auto ready = work.poll(); !ready)
            return runtime::failure(detail::path_error(ready.error().code()));
        std::vector<detail::segment_plan_input> inputs;
        inputs.reserve(batches.size());
        for (const auto* batch : batches)
            inputs.push_back(detail::plan_input(*batch));
        return segment_group_preparation{
          segment_capacity_decision::fits,
          segment_prepared_group{
            capture().value(),
            std::move(*plan),
            std::move(inputs),
            std::move(*held),
            std::move(*retries),
            std::move(*node),
            retained,
            work.policy()}};
    }

    // Whether freeze_group of this current preparation would now pass its
    // queue, pending-byte and age admission, without effect. Pressure is
    // queue_full and a crossed age limit timed_out. Child validation, native
    // allocation and a later state change can still reject the freeze.
    [[nodiscard]] runtime::result<void>
    freeze_admission(const segment_prepared_group& prepared) const {
        assert_current();
        auto before = capture();
        if (!before) return runtime::failure(before.error());
        if (
          prepared.base_ != *before || prepared.policy_ != config_.policy
          || !prepared.plan_.end)
            return runtime::failure(detail::path_error(errc::wrong_context));
        if (
          freezing_ || inflight_.size() >= config_.maximum_groups
          || digesting_.size() >= runtime::maximum_queued_file_writes)
            return runtime::failure(detail::path_error(errc::queue_full));
        const auto extent = model::file_byte_span::make(
          before->end().bytes, prepared.plan_.end->bytes);
        if (!extent)
            return runtime::failure(detail::path_error(errc::wrong_context));
        const auto pending = pending_bytes_.checked_add(extent->size());
        const auto retained = pending_retained_.checked_add(prepared.retained_);
        if (
          !pending || *pending > config_.maximum_pending_bytes || !retained
          || *retained > config_.maximum_pending_bytes)
            return runtime::failure(detail::path_error(errc::queue_full));
        auto aged = age_expired(Clock::now());
        if (!aged) return runtime::failure(aged.error());
        if (*aged) return runtime::failure(detail::path_error(errc::timed_out));
        return {};
    }

    // Consumes admitted exact children. All validation and fallible allocation
    // precede one synchronous installation of the complete block/footer cut.
    // No payload encoding, WAL operation or file write occurs here.
    [[nodiscard]] seastar::future<runtime::result<segment_frozen_group>>
    freeze_group(
      segment_prepared_group&& offered,
      std::vector<admitted_wal_batch>&& offered_children,
      codec::cooperative_work& work) {
        auto prepared = std::move(offered);
        auto children = std::move(offered_children);
        std::vector<admitted_wal_batch::contents> owned;
        std::optional<runtime::result<segment_frozen_group>> result;
        std::exception_ptr exception;
        assert_current();
        std::optional<seastar::gate::holder> gate;
        try {
            if (closing_ || closed_)
                result.emplace(
                  runtime::failure(detail::path_error(errc::closed)));
            else {
                gate.emplace(operations_.hold());
                result.emplace(
                  co_await freeze_owned(prepared, children, owned, work));
            }
        } catch (...) {
            exception = std::current_exception();
        }
        while (!owned.empty()) {
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            owned.pop_back();
        }
        while (!children.empty()) {
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            children.pop_back();
        }
        if (exception) std::rethrow_exception(exception);
        co_return std::move(*result);
    }

private:
    seastar::future<runtime::result<segment_frozen_group>> freeze_owned(
      segment_prepared_group& prepared,
      std::vector<admitted_wal_batch>& children,
      std::vector<admitted_wal_batch::contents>& owned,
      codec::cooperative_work& work) {
        auto before = capture();
        if (!before) co_return runtime::failure(before.error());
        if (
          freezing_ || inflight_.size() >= config_.maximum_groups
          || digesting_.size() >= runtime::maximum_queued_file_writes)
            co_return runtime::failure(detail::path_error(errc::queue_full));
        if (
          children.empty() || children.size() > maximum_segment_group_blocks
          || children.capacity() > maximum_segment_group_blocks
          || children.capacity() > 2 * children.size()
          || prepared.base_ != *before || prepared.policy_ != work.policy()
          || !budget_.owns(prepared.held_) || !prepared.held_.exclusive()
          || !budget_.owns(prepared.retries_) || !prepared.retries_.exclusive()
          || !budget_.owns(prepared.control_) || !prepared.control_.exclusive())
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        freezing_ = true;
        auto idle = seastar::defer([this] noexcept { freezing_ = false; });
        owned.reserve(children.size());
        for (auto& child : children)
            owned.push_back(std::move(child).release());
        std::array<const encoded_assigned_batch*, maximum_segment_group_blocks>
          inputs{};
        for (std::size_t i = 0; i < owned.size(); ++i) {
            auto& member = owned[i];
            const auto info = member.batch.info();
            const auto original = info.context.submitted();
            auto checked = co_await member.batch.validate(
              {descriptor_.segment.topic(),
               descriptor_.segment.range(),
               original.id(),
               original.binding(),
               info.fingerprint},
              {config_.admission.working_bytes,
               config_.admission.metadata_bytes,
               config_.metadata.charge},
              work);
            if (!checked)
                co_return runtime::failure(
                  detail::path_error(checked.error().code()));
            const auto cost = budget_.buffer_charge(member.batch.bytes());
            const auto allowance = member.backing.retained_bytes().checked_add(
              member.aliases ? member.aliases->retained_bytes() : byte_count{});
            if (!cost || !allowance || *cost > *allowance)
                co_return runtime::failure(
                  detail::path_error(errc::resource_exhausted));
            inputs[i] = &member.batch;
        }
        // The base is unchanged (checked above), so children equal to the
        // prepared inputs plan exactly as prepared; only others plan again.
        bool same = prepared.inputs_.size() == owned.size();
        for (std::size_t i = 0; same && i < owned.size(); ++i)
            same = prepared.inputs_[i] == detail::plan_input(owned[i].batch);
        std::optional<segment_capacity_plan> plan;
        if (same)
            plan = prepared.plan_;
        else {
            auto fixed = capacity();
            if (!fixed) co_return runtime::failure(fixed.error());
            auto replanned = detail::plan_segment_capacity(
              **fixed, before->end(), std::span{inputs}.first(owned.size()));
            if (!replanned) co_return runtime::failure(replanned.error());
            plan = *replanned;
        }
        if (
          plan->decision != segment_capacity_decision::fits
          || plan->end != prepared.plan_.end
          || plan->staging_bytes > prepared.plan_.staging_bytes
          || plan->retry_metadata > prepared.plan_.retry_metadata)
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        const auto extent = model::file_byte_span::make(
                              before->end().bytes, plan->end->bytes)
                              .value();
        const auto pending = pending_bytes_.checked_add(extent.size());
        if (!pending || *pending > config_.maximum_pending_bytes)
            co_return runtime::failure(detail::path_error(errc::queue_full));
        std::vector<segment_block_layout> blocks;
        blocks.reserve(owned.size());
        auto position = before->end().bytes;
        auto physical = before->end().physical;
        std::size_t fragments = 3; // complete footer prefix and padding
        byte_count retained = plan->staging_bytes;
        const auto policy = work.policy().config();
        for (const auto& member : owned) {
            const auto& batch = member.batch;
            const auto geometry = aligned_envelope_layout::make(
                                    {byte_count{codec::envelope_prefix_bytes},
                                     segment_block_fixed_bytes,
                                     batch.bytes().size()},
                                    descriptor_.alignment,
                                    work.policy(),
                                    {policy.max_encoded_body_bytes,
                                     byte_count{
                                       policy.max_encoded_body_bytes.value()
                                       + policy.max_header_bytes.value()}})
                                    .value();
            const auto records
              = model::segment_relative_span::from_count(
                  physical,
                  model::segment_record_count{
                    batch.info().context.retained_count().value()})
                  .value();
            const auto bytes = geometry.at(position).value();
            blocks.push_back(
              segment_block_layout{
                storage::coverage{
                  batch.info().context.logical_span(), records, bytes},
                geometry});
            position = bytes.end();
            physical = records.end();
            fragments += batch.bytes().fragment_count() + 3;
            retained
              = retained.checked_add(batch.bytes().retained_bytes()).value();
        }
        if (
          fragments > bytes::max_buffer_fragments
          || fragments > policy.max_buffer_fragments.value()
          || retained > runtime::maximum_file_io_bytes)
            co_return runtime::failure(
              detail::path_error(errc::resource_exhausted));
        const auto pending_retained = pending_retained_.checked_add(retained);
        if (
          !pending_retained
          || *pending_retained > config_.maximum_pending_bytes)
            co_return runtime::failure(detail::path_error(errc::queue_full));
        auto control = node_charge(fragments);
        if (!control) co_return runtime::failure(control.error());
        // The preparation holds this node's allowance; only children other
        // than the prepared ones can need a fresh one.
        std::optional<workload_reservation> held;
        if (*control <= prepared.control_.retained_bytes())
            held.emplace(std::move(prepared.control_));
        else {
            auto fresh = budget_.try_reserve(*control);
            if (!fresh) co_return runtime::failure(fresh.error());
            held.emplace(std::move(*fresh));
        }
        const auto footer
          = model::file_byte_span::make(position, plan->end->bytes).value();
        const segment_captured_boundary cut{
          lifetime_, before->history(), *plan->end, footer};
        segment_group_layout layout{
          cut, std::move(blocks), prepared.held_.share()};
        auto node = seastar::make_lw_shared<detail::segment_write_descriptor>(
          std::move(*held),
          std::move(prepared.held_),
          lifetime_,
          *before,
          std::move(layout),
          std::move(owned),
          work.policy());
        node->blocks.reserve(node->children.size());
        node->retained_bound = retained;
        node->gather_credit
          = budget_
              .allocation_charge(
                byte_count{
                  fragments
                  * bytes::fragmented_buffer::fragment_descriptor_size()})
              .value();
        auto current = capture();
        if (!current || *current != *before)
            co_return runtime::failure(detail::path_error(errc::closed));
        if (auto ready = work.poll(); !ready)
            co_return runtime::failure(
              detail::path_error(ready.error().code()));
        const auto now = Clock::now();
        auto aged = age_expired(now);
        if (!aged) co_return runtime::failure(aged.error());
        // Reject a preparation that crossed the age boundary before any
        // coordinates escape. Existing accepted groups retain their authority.
        if (*aged)
            co_return runtime::failure(detail::path_error(errc::timed_out));
        inflight_.push_back(node);
        // Retain retry capacity until finalization/close in one reservation,
        // even after write completion releases the group's larger workspace.
        // Adopting returns this grant's task unit: accepted groups never
        // accumulate workload task units.
        if (retry_memory_) {
            const auto adopted = retry_memory_->adopt(
              std::move(prepared.retries_));
            KWAQUE_INVARIANT(
              invariant_id{"KQ-SEGMENT-RETRY-GRANT"},
              adopted.has_value(),
              "checked retry grant was not adopted");
        } else {
            retry_memory_.emplace(std::move(prepared.retries_));
        }
        if (!first_acceptance_) first_acceptance_ = now;
        node->linked = true;
        positions_->reserved = *plan->end;
        reserved_footer_ = footer;
        pending_bytes_ = *pending;
        pending_retained_ = *pending_retained;
        retry_page_entries_ = plan->retry_page_entries;
        // No await or allocation after linking through returning the handoff.
        co_return segment_frozen_group{std::move(node)};
    }

public:
    [[nodiscard]] seastar::future<runtime::result<void>>
    encode_group(segment_frozen_group& group, codec::cooperative_work& work) {
        assert_current();
        auto node = group.node_;
        if (!node || node->lifetime != lifetime_)
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        if (
          closing_ || closed_ || append_ != model::append_state::active
          || lifetime_->failure.failed())
            co_return runtime::failure(detail::path_error(errc::closed));
        if (node->state != detail::segment_write_state::frozen)
            co_return runtime::failure(detail::path_error(errc::queue_full));
        auto gate = operations_.hold();
        node->state = detail::segment_write_state::encoding;
        auto execution = node->execution.hold();
        co_return co_await seastar::with_scheduling_group(
          budget_.scheduling_group(),
          [this, node, &work] { return encode_owned(node, work); });
    }

private:
    seastar::future<runtime::result<void>> encode_owned(
      detail::segment_write_descriptor::pointer node,
      codec::cooperative_work& work) {
        try {
            if (work.policy() != node->policy) {
                node->fail(detail::path_error(errc::invalid_argument));
            } else
                for (std::size_t i = 0; i < node->children.size(); ++i) {
                    if (closing_ || lifetime_->failure.failed()) {
                        node->fail(detail::path_error(errc::closed));
                        break;
                    }
                    auto& child = node->children[i];
                    const auto layout = node->layout.blocks()[i];
                    const auto info = child.batch.info();
                    const auto original = info.context.submitted();
                    const auto location = segment_write_context::make(
                                            descriptor_.segment,
                                            descriptor_.alignment,
                                            layout.records.physical().begin(),
                                            layout.records.bytes().begin())
                                            .value();
                    const auto cost = child.batch.bytes()
                                        .allocation_cost(
                                          config_.metadata.charge)
                                        .value();
                    const byte_count allowance{
                      cost.backing.value() + cost.descriptors.value()
                      + cost.share_controls.value()
                      + config_.admission.working_bytes.value()};
                    auto encoded = co_await encode_segment_block(
                      std::move(child.batch),
                      {location,
                       data_start_,
                       {descriptor_.segment.topic(),
                        descriptor_.segment.range(),
                        original.id(),
                        original.binding(),
                        info.fingerprint},
                       descriptor_.profile},
                      work,
                      allowance,
                      config_.metadata.charge);
                    if (!encoded) {
                        node->fail(detail::path_error(encoded.error().code()));
                        break;
                    }
                    if (encoded->descriptor().coverage() != layout.records) {
                        node->fail(detail::path_error(errc::wrong_context));
                        break;
                    }
                    node->blocks.push_back(std::move(*encoded));
                }
        } catch (...) {
            node->fail(std::current_exception());
        }
        // Only the caller awaits encoding; failure has already woken the
        // owner through the node.
        if (!node->failure.failed())
            node->state = detail::segment_write_state::encoded;
        co_return node->failure.outcome();
    }

public:
    // Transfers execution authority after the caller's required ordering
    // barrier. Acceptance is synchronous; the owner subsequently hashes and
    // assembles the footer in file order using its own work/abort lifetime.
    [[nodiscard]] runtime::result<segment_submission>
    submit(segment_frozen_group&& offered, codec::cooperative_work& admission) {
        auto group = std::move(offered);
        assert_current();
        auto node = group.node_;
        if (!node || node->lifetime != lifetime_)
            return runtime::failure(detail::path_error(errc::wrong_context));
        if (
          closing_ || closed_ || append_ != model::append_state::active
          || lifetime_->failure.failed())
            return runtime::failure(detail::path_error(errc::closed));
        if (node->state != detail::segment_write_state::encoded)
            return runtime::failure(detail::path_error(errc::queue_full));
        if (admission.policy() != node->policy)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        if (auto ready = admission.poll(); !ready)
            return runtime::failure(detail::path_error(ready.error().code()));
        segment_submission result{
          node->layout.boundary(), node->completion.get_future()};
        node->observed = true;
        node->state = detail::segment_write_state::authorized;
        group.node_ = nullptr;
        lifetime_->changed.broadcast();
        return result;
    }

    [[nodiscard]] seastar::future<segment_barrier_outcome>
    barrier(segment_captured_boundary cut) {
        assert_current();
        if (auto valid = validate_capture(cut); !valid) {
            segment_barrier_outcome rejected{lifetime_->failure, {}};
            if (!rejected.failure.failed())
                rejected.failure.observe(valid.error());
            return seastar::make_ready_future<segment_barrier_outcome>(
              std::move(rejected));
        }
        if (!cut.footer()) {
            segment_barrier_outcome rejected;
            rejected.failure.observe(
              detail::path_error(errc::invalid_argument));
            return seastar::make_ready_future<segment_barrier_outcome>(
              std::move(rejected));
        }
        // A frozen handoff still belongs to its caller. Waiting for it here
        // could deadlock close and cannot invent the missing authorization.
        bool unsubmitted = false;
        for (const auto& node : inflight_) {
            if (node->extent().begin() >= cut.end().bytes) break;
            unsubmitted |= !node->observed;
        }
        if (barrier_busy_ || unsubmitted) {
            segment_barrier_outcome rejected;
            rejected.failure.observe(detail::path_error(errc::queue_full));
            return seastar::make_ready_future<segment_barrier_outcome>(
              std::move(rejected));
        }
        return barrier_owned(std::move(cut), operations_.hold());
    }

    // Observation only: waits until every written group is included in the
    // extent digest, or the owner failed. Receipts never wait for this;
    // sealing joins the digest itself.
    [[nodiscard]] seastar::future<runtime::result<void>> digest_caught_up() {
        assert_current();
        if (closing_ || closed_)
            co_return runtime::failure(detail::path_error(errc::closed));
        std::optional<seastar::gate::holder> holder;
        try {
            holder.emplace(operations_.hold());
        } catch (const seastar::gate_closed_exception&) {
            co_return runtime::failure(detail::path_error(errc::closed));
        }
        ++digest_waiters_;
        auto waiter = seastar::defer([this] noexcept { --digest_waiters_; });
        while (!digesting_.empty() && !lifetime_->failure.failed())
            co_await lifetime_->changed.when();
        co_return lifetime_->failure.outcome();
    }

    [[nodiscard]] const segment_seal_progress& seal_progress() const& noexcept {
        assert_current();
        return sealing_;
    }
    const segment_seal_progress& seal_progress() const&& = delete;

    // Source is moved into the joined attempt and owns/pins an independently
    // completed immutable snapshot. read(first,count,work) returns exactly
    // that slice as result<vector<completed_retry>>, without fresh workload
    // admission, and can replay it. Its retained facts are already charged.
    // Future completions remain with the supplied snapshot/WAL-retention owner.
    template<typename Source>
    [[nodiscard]] seastar::future<segment_seal_outcome> seal(
      Source source,
      std::uint32_t completed,
      std::uint32_t unresolved,
      codec::cooperative_work& admission) {
        static_assert(sizeof(Source) <= 8_KiB);
        assert_current();
        if (seal_done_) co_return seal_result_;
        if (seal_waiters_ == maximum_control_waiters) {
            segment_seal_outcome rejected;
            rejected.failure.observe(detail::path_error(errc::queue_full));
            co_return rejected;
        }
        ++seal_waiters_;
        auto waiter = seastar::defer([this] noexcept { --seal_waiters_; });
        if (seal_started_) {
            while (!seal_done_)
                co_await lifetime_->changed.when();
            co_return seal_result_;
        }
        segment_seal_outcome rejected;
        if (
          closing_ || closed_ || recovered_ || !positions_
          || append_ != model::append_state::active
          || lifetime_->failure.failed()) {
            rejected.failure = lifetime_->failure;
            rejected.failure.observe(detail::path_error(errc::closed));
            co_return rejected;
        }
        if (
          admission.policy() != config_.policy
          || std::uint64_t{completed} + unresolved
               != positions_->reserved.retry_entries) {
            rejected.failure.observe(
              detail::path_error(errc::invalid_argument));
            co_return rejected;
        }
        if (auto ready = admission.poll(); !ready) {
            rejected.failure.observe(detail::path_error(ready.error().code()));
            co_return rejected;
        }
        // One shard-local stop point. No later preparation or handoff can
        // authorize work; entered encoders finish before abandoned nodes drain.
        seal_started_ = true;
        append_ = model::append_state::sealing;
        seal_result_.unresolved = unresolved;
        try {
            co_await operations_.close();
            co_await stop_execution();
            if (!lifetime_->failure.failed()) {
                seastar::abort_source abort;
                codec::cooperative_work work{config_.policy, abort};
                lifetime_->failure.observe(
                  co_await seastar::with_scheduling_group(
                    budget_.scheduling_group(),
                    [this, &source, completed, &work] {
                        return seal_owned(source, completed, work);
                    }));
            }
        } catch (...) {
            lifetime_->failure.observe(std::current_exception());
        }
        seal_result_.failure = lifetime_->failure;
        seal_done_ = true;
        lifetime_->changed.broadcast();
        co_return seal_result_;
    }

    // Writes one object a recovered seal reconstructed below its decided end,
    // before seal_recovered(): only on an owner opened by
    // open_recovered_seal, once its decision is durable and the caller's walk
    // verified the object in place. The writable handle replaces every read
    // handle; the bytes stay unflushed until the seal's one flush.
    [[nodiscard]] seastar::future<runtime::result<void>> write_recovered(
      runtime::file_position at,
      bytes::fragmented_buffer object,
      codec::cooperative_work& work) {
        assert_current();
        if (
          closing_ || closed_ || !recovered_seal_ || seal_started_ || positions_
          || append_ != model::append_state::active
          || lifetime_->failure.failed())
            co_return runtime::failure(detail::path_error(errc::closed));
        if (read_busy_ || read_operations_.get_count() != 0)
            co_return runtime::failure(detail::path_error(errc::queue_full));
        const auto length = object.size();
        const auto end = at.checked_add(length);
        if (
          object.empty() || at < data_start_
          || !descriptor_.alignment.aligned(at) || !end
          || !descriptor_.alignment.aligned(*end)
          || end->value() > descriptor_.maximum_data_bytes.value())
            co_return runtime::failure(
              detail::path_error(errc::invalid_argument));
        if (auto ready = work.poll(); !ready)
            co_return runtime::failure(
              detail::path_error(ready.error().code()));
        auto holder = read_operations_.hold();
        read_busy_ = true;
        auto idle = seastar::defer([this] noexcept { read_busy_ = false; });
        try {
            if (!writable_) {
                co_await close_handles();
                if (!lifetime_->failure.failed())
                    lifetime_->failure.observe(
                      co_await open_data(true, work, true));
            }
            if (!lifetime_->failure.failed()) {
                auto written = co_await data_->write(at, std::move(object));
                lifetime_->failure.observe(written);
                if (written && *written != length)
                    lifetime_->failure.observe(
                      detail::path_error(errc::io_failure));
            }
        } catch (...) {
            lifetime_->failure.observe(std::current_exception());
        }
        co_return lifetime_->failure.outcome();
    }

    // Seals a recovered segment at the end of `extent`, the complete surviving
    // prefix a durable recovered-seal decision fixed. The caller made that
    // decision durable and walked the extent from its data start with a
    // digest; this owner never appends to it. In order: the retry bundle, the
    // sealed root at the end, removal of every byte after the root, one flush
    // covering the surviving extent and the root, then the sealed
    // publication. Nothing else is published until that last step, so a
    // failure or crash anywhere leaves the earlier publication and every byte
    // below the end, and the same decision resumes the seal. No reader may
    // overlap it. Source and the counts are as for seal().
    template<typename Source>
    [[nodiscard]] seastar::future<segment_seal_outcome> seal_recovered(
      verified_extent extent,
      Source source,
      std::uint32_t completed,
      std::uint32_t unresolved,
      codec::cooperative_work& admission) {
        static_assert(sizeof(Source) <= 8_KiB);
        assert_current();
        if (seal_done_) co_return seal_result_;
        if (seal_waiters_ == maximum_control_waiters) {
            segment_seal_outcome rejected;
            rejected.failure.observe(detail::path_error(errc::queue_full));
            co_return rejected;
        }
        ++seal_waiters_;
        auto waiter = seastar::defer([this] noexcept { --seal_waiters_; });
        if (seal_started_) {
            while (!seal_done_)
                co_await lifetime_->changed.when();
            co_return seal_result_;
        }
        segment_seal_outcome rejected;
        // Only an owner opened to execute a durable decision seals: any other
        // recovered owner holds no decision that authorizes the truncation.
        if (
          closing_ || closed_ || !recovered_ || !recovered_seal_ || positions_
          || append_ != model::append_state::active
          || lifetime_->failure.failed()) {
            rejected.failure = lifetime_->failure;
            rejected.failure.observe(detail::path_error(errc::closed));
            co_return rejected;
        }
        if (read_busy_ || read_operations_.get_count() != 0) {
            rejected.failure.observe(detail::path_error(errc::queue_full));
            co_return rejected;
        }
        if (admission.policy() != config_.policy || !config_.retry_object) {
            rejected.failure.observe(
              detail::path_error(errc::invalid_argument));
            co_return rejected;
        }
        if (auto ready = admission.poll(); !ready) {
            rejected.failure.observe(detail::path_error(ready.error().code()));
            co_return rejected;
        }
        // From here no read entrance admits work.
        seal_started_ = true;
        append_ = model::append_state::sealing;
        seal_result_.unresolved = unresolved;
        try {
            seastar::abort_source abort;
            codec::cooperative_work work{config_.policy, abort};
            lifetime_->failure.observe(
              co_await seastar::with_scheduling_group(
                budget_.scheduling_group(),
                [this, &source, &extent, completed, &work] {
                    return seal_recovered_owned(
                      source, extent, completed, work);
                }));
        } catch (...) {
            lifetime_->failure.observe(std::current_exception());
        }
        seal_result_.failure = lifetime_->failure;
        seal_done_ = true;
        lifetime_->changed.broadcast();
        co_return seal_result_;
    }

    // After a restart classified this recovered segment: one new flush of its
    // data file, then a recovering publication pinning `boundary`, the last
    // footer the restart verified, or nothing for a segment without one.
    // Surviving bytes prove no earlier flush, so the flush comes first and the
    // publication only after it succeeded. The boundary never lowers the
    // current pin, and an unchanged recovering publication is not published
    // again. Any failure fails this owner, which no later flush makes healthy;
    // the segment then stays as published before. It stays roll-required
    // either way and nothing is appended. No reader may overlap it.
    [[nodiscard]] seastar::future<runtime::result<void>> publish_recovering(
      std::optional<local_footer_reference> boundary,
      codec::cooperative_work& work) {
        assert_current();
        if (
          closing_ || closed_ || !recovered_ || recovered_seal_ || immutable_
          || seal_started_ || positions_ || !publication_ || !read_publication_
          || append_ != model::append_state::active
          || lifetime_->failure.failed())
            co_return runtime::failure(detail::path_error(errc::closed));
        if (read_busy_ || read_operations_.get_count() != 0)
            co_return runtime::failure(detail::path_error(errc::queue_full));
        const auto& current = *read_publication_;
        if (
          current.boundary
          && (!boundary || boundary->position() < current.boundary->position()
              || (boundary->position() == current.boundary->position() && *boundary != *current.boundary)))
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        if (
          (current.state == local_object_state::recovering
           && current.boundary == boundary)
          || (boundary
              && (boundary->family() != static_cast<std::uint16_t>(codec::format_family::durable_boundary_footer)
                  || boundary->position() < data_start_
                  || !boundary->validate_alignment(descriptor_.alignment))))
            co_return runtime::failure(
              detail::path_error(errc::invalid_argument));
        if (auto ready = work.poll(); !ready)
            co_return runtime::failure(
              detail::path_error(ready.error().code()));
        auto held = budget_.try_reserve(
          byte_count{
            config_.metadata.operation_bytes.value()
            + config_.metadata.execution_bytes.value()});
        if (!held) co_return runtime::failure(held.error());
        auto holder = read_operations_.hold();
        read_busy_ = true;
        auto idle = seastar::defer([this] noexcept { read_busy_ = false; });
        try {
            lifetime_->failure.observe(
              co_await publish_recovering_owned(boundary, work));
        } catch (...) {
            lifetime_->failure.observe(std::current_exception());
        }
        co_await close_publishers();
        co_return lifetime_->failure.outcome();
    }

    // Only fully verified immutable owners expose data reads. Returned bytes
    // own their reservation; no borrowed descriptor escapes this operation.
    [[nodiscard]] seastar::future<runtime::result<local_metadata_file>>
    read_immutable(
      runtime::file_position position,
      byte_count length,
      codec::cooperative_work& work) {
        assert_current();
        if (
          !immutable_ || closing_ || closed_ || lifetime_->failure.failed()
          || append_ != model::append_state::sealed)
            co_return runtime::failure(detail::path_error(errc::closed));
        if (
          read_busy_
          || read_operations_.get_count()
               >= runtime::maximum_pending_file_reads)
            co_return runtime::failure(detail::path_error(errc::queue_full));
        auto end = position.checked_add(length);
        if (
          !length.value() || !end || *end > immutable_->file_end
          || length > byte_count{config_.admission.working_bytes.value() / 2})
            co_return runtime::failure(
              detail::path_error(errc::invalid_argument));
        if (auto ready = work.poll(); !ready)
            co_return runtime::failure(
              detail::path_error(ready.error().code()));
        auto held = budget_.try_reserve(
          byte_count{
            config_.admission.working_bytes.value()
            + config_.admission.execution_bytes.value()});
        if (!held) co_return runtime::failure(held.error());
        auto holder = read_operations_.hold();
        try {
            if (!data_) {
                read_busy_ = true;
                auto idle = seastar::defer(
                  [this] noexcept { read_busy_ = false; });
                lifetime_->failure.observe(co_await reopen_read(work));
            }
            if (!lifetime_->failure.failed()) {
                auto read = co_await data_->read(position, length);
                lifetime_->failure.observe(read);
                if (read) {
                    if (read->data().size() != length)
                        lifetime_->failure.observe(
                          detail::path_error(errc::truncated_data));
                    else {
                        auto cost = budget_.buffer_charge(read->data());
                        if (!cost || *cost > config_.admission.working_bytes)
                            lifetime_->failure.observe(
                              detail::path_error(errc::resource_exhausted));
                        else
                            co_return local_metadata_file{
                              std::move(*held),
                              std::move(*read).take_data(),
                              immutable_->file_end.value()};
                    }
                }
            }
        } catch (...) {
            lifetime_->failure.observe(std::current_exception());
        }
        auto failed = lifetime_->failure.outcome();
        co_return runtime::failure(failed.error());
    }

    // Idle read eviction serializes with read/reopen. Active writable handles
    // and their startup completion reservation are never evicted.
    [[nodiscard]] seastar::future<runtime::result<void>> evict_read_handle() {
        assert_current();
        if (
          closing_ || closed_ || lifetime_->failure.failed() || writable_
          || (!immutable_ && !(recovered_ && append_ == model::append_state::active)))
            co_return runtime::failure(detail::path_error(errc::closed));
        if (read_busy_ || read_operations_.get_count() != 0)
            co_return runtime::failure(detail::path_error(errc::queue_full));
        auto holder = read_operations_.hold();
        read_busy_ = true;
        auto idle = seastar::defer([this] noexcept { read_busy_ = false; });
        co_await close_handles();
        handle_ = lifetime_->failure.failed() ? segment_handle_state::closed
                                              : segment_handle_state::evicted;
        co_return lifetime_->failure.outcome();
    }

    // Revalidates the retained publication on a new read-only descriptor.
    // Recovered active state stays metadata-only and roll-required.
    [[nodiscard]] seastar::future<runtime::result<void>>
    reopen_read_handle(codec::cooperative_work& work) {
        assert_current();
        if (
          closing_ || closed_ || lifetime_->failure.failed() || writable_
          || (!immutable_ && !(recovered_ && append_ == model::append_state::active)))
            co_return runtime::failure(detail::path_error(errc::closed));
        if (read_busy_)
            co_return runtime::failure(detail::path_error(errc::queue_full));
        if (auto ready = work.poll(); !ready)
            co_return runtime::failure(
              detail::path_error(ready.error().code()));
        if (data_) co_return runtime::result<void>{};
        auto held = budget_.try_reserve(
          byte_count{
            (immutable_ ? config_.admission.working_bytes
                        : config_.metadata.operation_bytes)
              .value()
            + config_.admission.execution_bytes.value()});
        if (!held) co_return runtime::failure(held.error());
        auto holder = read_operations_.hold();
        read_busy_ = true;
        auto idle = seastar::defer([this] noexcept { read_busy_ = false; });
        try {
            lifetime_->failure.observe(co_await reopen_read(work));
        } catch (...) {
            lifetime_->failure.observe(std::current_exception());
        }
        co_return lifetime_->failure.outcome();
    }

    // At most eight pending callers join one terminal cleanup. Extra interest
    // rejects without changing the owner; completed calls reuse its outcome.
    [[nodiscard]] seastar::future<runtime::result<void>> close() {
        assert_current();
        if (closed_) co_return lifetime_->failure.outcome();
        if (close_waiters_ == maximum_control_waiters)
            co_return runtime::failure(detail::path_error(errc::queue_full));
        ++close_waiters_;
        auto waiter = seastar::defer([this] noexcept { --close_waiters_; });
        if (closing_) {
            while (!closed_)
                co_await lifetime_->changed.when();
            co_return lifetime_->failure.outcome();
        }
        closing_ = true;
        // Seal owns the same finite execution and finalizer state. Close joins
        // it before releasing any child, even when the seal caller detaches.
        while (seal_started_ && !seal_done_)
            co_await lifetime_->changed.when();
        if (!execution_stopped_) co_await operations_.close();
        co_await read_operations_.close();
        co_await stop_execution();
        if (extent_) {
            extent_->close();
            extent_.reset();
        }
        digest_.reset();
        zero_ = {};
        zero_memory_.reset();
        retry_memory_.reset();
        co_await close_publishers();
        co_await close_handles();
        bundle_memory_.reset();
        seal_memory_.reset();
        handle_ = segment_handle_state::closed;
        closed_ = true;
        closing_ = false;
        lifetime_->changed.broadcast();
        co_return lifetime_->failure.outcome();
    }

private:
    runtime::result<bool> age_expired(runtime::monotonic_time now) const {
        if (first_acceptance_ && now < *first_acceptance_)
            return runtime::failure(detail::path_error(errc::wrong_context));
        const auto first = first_acceptance_.value_or(now);
        const auto deadline = first.checked_add(descriptor_.maximum_lifetime);
        if (!deadline)
            return runtime::failure(detail::path_error(errc::out_of_range));
        return first_acceptance_ && now >= *deadline;
    }

    seastar::future<> stop_execution() {
        if (execution_stopped_) co_return;
        execution_stopped_ = true;
        for (const auto& node : inflight_)
            node->abandon();
        lifetime_->changed.broadcast();
        if (dispatcher_) {
            try {
                co_await std::move(*dispatcher_);
            } catch (...) {
                lifetime_->failure.observe(std::current_exception());
            }
            dispatcher_.reset();
        }
        // Sealing needs every written byte hashed; close only releases.
        digest_stopped_ = true;
        lifetime_->changed.broadcast();
        if (digester_) {
            try {
                co_await std::move(*digester_);
            } catch (...) {
                lifetime_->failure.observe(std::current_exception());
            }
            digester_.reset();
        }
        while (!digesting_.empty()) {
            release_group(*digesting_.front());
            digesting_.pop_front();
        }
    }

    void release_pending(detail::segment_write_descriptor& node) noexcept {
        pending_bytes_ = byte_count{
          pending_bytes_.value() - node.extent().size().value()};
        pending_retained_ = byte_count{
          pending_retained_.value() - node.retained_bound.value()};
    }
    void release_group(detail::segment_write_descriptor& node) noexcept {
        node.digest = {};
        node.children.clear();
        node.working.reset();
        release_pending(node);
    }

    // Hashes written groups in file order, off the durability path: it runs
    // while barriers flush, and a group's memory is released only after its
    // bytes are hashed. Failure fences the owner; receipts already issued
    // stay valid, but the extent can no longer be sealed.
    seastar::future<> digest_groups() {
        seastar::abort_source abort;
        codec::cooperative_work work{config_.policy, abort};
        while (!digest_stopped_ || !digesting_.empty()) {
            if (digesting_.empty()) {
                co_await lifetime_->changed.when();
                continue;
            }
            auto node = digesting_.front();
            if (!closing_ && !lifetime_->failure.failed()) {
                try {
                    auto hashed = co_await digest_->add(node->digest, work);
                    if (!hashed)
                        lifetime_->failure.observe(
                          detail::path_error(hashed.error().code()));
                } catch (...) {
                    lifetime_->failure.observe(std::current_exception());
                }
            }
            node->digest = {};
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            release_group(*node);
            digesting_.pop_front();
            // Wake others only for a failure or a waiter for this drain.
            if (
              lifetime_->failure.failed()
              || (digesting_.empty() && digest_waiters_ != 0))
                lifetime_->changed.broadcast();
        }
    }

    seastar::future<> close_publishers() {
        if (bundle_publisher_) {
            try {
                lifetime_->failure.observe(co_await bundle_publisher_->close());
            } catch (...) {
                lifetime_->failure.observe(std::current_exception());
            }
            bundle_publisher_.reset();
        }
        if (pointer_publisher_) {
            try {
                lifetime_->failure.observe(
                  co_await pointer_publisher_->close());
            } catch (...) {
                lifetime_->failure.observe(std::current_exception());
            }
            pointer_publisher_.reset();
        }
        co_return;
    }

    seastar::future<> close_handles() {
        if (completion_) completion_->release_metadata();
        if (sync_) {
            try {
                lifetime_->failure.observe(co_await sync_->close());
            } catch (...) {
                lifetime_->failure.observe(std::current_exception());
            }
            sync_.reset();
        }
        if (data_) {
            try {
                lifetime_->failure.observe(co_await data_->close());
            } catch (...) {
                lifetime_->failure.observe(std::current_exception());
            }
            data_.reset();
        }
        completion_.reset();
        writable_ = false;
        if (generation_) {
            try {
                lifetime_->failure.observe(co_await generation_->close());
            } catch (...) {
                lifetime_->failure.observe(std::current_exception());
            }
            generation_.reset();
        }
    }

    seastar::future<runtime::result<void>>
    verify_immutable(codec::cooperative_work& work) {
        auto held = budget_.try_reserve(
          byte_count{
            config_.admission.working_bytes.value()
            + config_.admission.execution_bytes.value()});
        if (!held) co_return runtime::failure(held.error());
        co_return co_await verify_immutable_reserved(work);
    }
    seastar::future<runtime::result<void>>
    verify_immutable_reserved(codec::cooperative_work& work) {
        auto pin = generation_->pin();
        if (!pin) co_return runtime::failure(pin.error());
        const auto& boundary = pin->boundary();
        const auto* root = boundary ? std::get_if<sealed_footer>(&*boundary)
                                    : nullptr;
        if (!root || !immutable_)
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        auto actual = co_await detail::read_local_boundary(
          *data_,
          root->location().history,
          *read_publication_->boundary,
          config_.metadata,
          work);
        if (!actual) co_return runtime::failure(actual.error());
        const auto* current = std::get_if<sealed_footer>(&*actual);
        if (!current || current->digest() != root->digest())
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        auto verified = co_await verify_segment_immutable(
          *data_,
          *current,
          *immutable_,
          descriptor_.layout,
          {config_.admission.working_bytes,
           config_.admission.metadata_bytes,
           config_.metadata.charge},
          work);
        if (!verified) co_return runtime::failure(verified.error());
        co_return runtime::result<void>{};
    }

    seastar::future<runtime::result<void>>
    reopen_read(codec::cooperative_work& work) {
        handle_ = segment_handle_state::reopening;
        const local_generation_expectation expected{
          *publication_, *read_publication_, descriptor_, read_roots_};
        auto generation = co_await local_generation_owner::open(
          files_,
          owner_,
          spec_,
          shard_,
          expected,
          budget_,
          config_.metadata,
          work);
        if (!generation) co_return runtime::failure(generation.error());
        generation_ = std::move(*generation);
        auto opened = co_await open_data(false, work);
        if (!opened) co_return opened;
        if (immutable_) co_return co_await verify_immutable_reserved(work);
        co_return runtime::result<void>{};
    }

    template<typename Source>
    seastar::future<runtime::result<void>> seal_owned(
      Source& source, std::uint32_t completed, codec::cooperative_work& work) {
        const auto end = positions_->reserved;
        if (positions_->written != end)
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        // No live groups or earlier captured barrier remain. Empty data has
        // already passed creation's file/namespace barriers.
        if (positions_->durable.bytes < end.bytes) {
            const auto covered = plain_writes_;
            if (covered != flushed_plain_writes_) {
                auto synced = co_await data_->flush(completion_->metadata());
                if (!synced) co_return runtime::failure(synced.error());
                flushed_plain_writes_ = covered;
            }
            positions_->durable = end;
        }
        sealing_.data_synced = true;
        // Execution has stopped, so the digest has hashed every written
        // byte; finish checks that against this walk.
        if (!digest_)
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        auto finished = extent_->finish(work, std::move(*digest_));
        digest_.reset();
        if (!finished)
            co_return runtime::failure(
              detail::path_error(finished.error().code()));
        sealing_.extent = *finished;
        extent_.reset();
        auto dependencies = [this, end](this auto, codec::cooperative_work&)
          -> seastar::future<runtime::result<void>> {
            if (lifetime_->failure.failed())
                co_return lifetime_->failure.outcome();
            if (!sealing_.data_synced || positions_->durable != end)
                co_return runtime::failure(
                  detail::path_error(errc::wrong_context));
            co_return runtime::result<void>{};
        };
        co_return co_await seal_root(
          source,
          *finished,
          end.bytes,
          completed,
          prezeroed_end_,
          std::move(dependencies),
          work);
    }

    // Selects the recovered segment's current publication by its fixed path:
    // it must name this segment and be active or recovering.
    seastar::future<runtime::result<void>>
    select_recovered(codec::cooperative_work& work) {
        auto selected = co_await load_local_metadata_file(
          files_,
          spec_,
          path(local_segment_file::published),
          budget_,
          config_.metadata,
          work,
          local_metadata_extent::exact_file,
          [owner = spec_.shard_owner(shard_).value(),
           segment = descriptor_.segment,
           metadata = spec_.identity.metadata_alignment,
           data = descriptor_.alignment](
            auto& input, byte_count, codec::decode_budget memory, auto& work) {
              return decode_selected_object_publication(
                input,
                owner,
                segment,
                metadata,
                data,
                memory,
                work,
                {},
                codec::input_boundary::complete);
          });
        if (!selected) co_return runtime::failure(selected.error());
        const auto& publication = std::get<local_object_publication>(
          selected->value.payload());
        if (
          publication.segment != descriptor_.segment
          || (publication.state != local_object_state::active && publication.state != local_object_state::recovering))
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        publication_ = selected->value.header().generation();
        read_publication_ = publication;
        append_ = model::append_state::active;
        co_return runtime::result<void>{};
    }

    seastar::future<runtime::result<void>> publish_recovering_owned(
      std::optional<local_footer_reference> boundary,
      codec::cooperative_work& work) {
        if (!data_) {
            if (auto reopened = co_await reopen_read(work); !reopened)
                co_return reopened;
        }
        auto size = co_await data_->size();
        if (!size) co_return runtime::failure(size.error());
        // The bytes the restart classified must still be there.
        const auto certified = boundary ? boundary->position().checked_add(
                                            boundary->bytes())
                                        : std::optional{data_start_};
        if (!certified)
            co_return runtime::failure(detail::path_error(errc::out_of_range));
        if (*size < certified->value())
            co_return runtime::failure(
              detail::path_error(errc::truncated_data));
        if (publication_->value() == UINT64_MAX)
            co_return runtime::failure(detail::path_error(errc::out_of_range));
        auto next = local_publication_generation::make(
          publication_->value() + 1);
        if (!next)
            co_return runtime::failure(detail::path_error(errc::out_of_range));
        const auto metadata_header = local_metadata_header::make(
                                       local_metadata_kind::object_publication,
                                       spec_.shard_owner(shard_).value(),
                                       *next)
                                       .value();
        const local_object_publication publication{
          descriptor_.segment,
          local_object_state::recovering,
          boundary,
          read_publication_->roots};
        // The fresh flush runs while the publication's temporary is written
        // and flushed; only the rename that makes it current waits for the
        // flush, so a failed flush still publishes nothing. The flush is
        // joined, and its reservation returned, before any handle closes.
        runtime::result<void> published{};
        {
            auto unit = data_->try_reserve_metadata();
            if (!unit) co_return runtime::failure(unit.error());
            // Allocated before the flush starts, and the flush is always
            // joined below: nothing it borrows can go before it ends.
            seastar::shared_promise<runtime::result<void>> flushed;
            auto flushing =
              [](
                runtime::file& data,
                runtime::file::metadata_reservation& unit,
                seastar::shared_promise<runtime::result<void>>& flushed)
              -> seastar::future<> {
                try {
                    flushed.set_value(co_await data.flush(unit));
                } catch (...) {
                    flushed.set_exception(std::current_exception());
                }
            }(*data_, *unit, flushed);
            std::exception_ptr thrown;
            try {
                do {
                    auto pointer = co_await encode_local_metadata(
                      {metadata_header,
                       spec_.identity.metadata_alignment,
                       descriptor_.alignment},
                      local_metadata_payload{publication},
                      work,
                      config_.metadata.operation_bytes,
                      config_.metadata.charge);
                    if (!pointer) {
                        published = runtime::failure(
                          detail::path_error(pointer.error().code()));
                        break;
                    }
                    pointer_publisher_.emplace(
                      files_,
                      budget_,
                      target(
                        path(local_segment_file::published),
                        runtime::file_rename_policy::replace,
                        publication_));
                    published = co_await pointer_publisher_->prepare(
                      config_.metadata.operation_bytes, work);
                    if (!published) break;
                    published = co_await owner_.validate(spec_);
                    if (!published) break;
                    auto outcome = co_await pointer_publisher_->publish(
                      {spec_.shard_owner(shard_).value(), *next, publication_},
                      std::move(pointer->bytes),
                      work,
                      [&flushed] { return flushed.get_shared_future(); });
                    if (outcome.failure.failed())
                        published = outcome.failure.outcome();
                    else if (
                      outcome.disposition
                      != local_publication_disposition::durable)
                        published = runtime::failure(
                          detail::path_error(errc::io_failure));
                } while (false);
            } catch (...) {
                thrown = std::current_exception();
            }
            co_await std::move(flushing);
            runtime::result<void> synced{};
            try {
                synced = co_await flushed.get_shared_future();
            } catch (...) {
                if (!thrown) thrown = std::current_exception();
            }
            if (thrown) std::rethrow_exception(thrown);
            if (!synced) co_return runtime::failure(synced.error());
        }
        if (!published) co_return published;
        publication_ = *next;
        read_publication_ = publication;
        // Later readers reopen under the new publication.
        co_await close_handles();
        handle_ = lifetime_->failure.failed() ? segment_handle_state::closed
                                              : segment_handle_state::evicted;
        co_return runtime::result<void>{};
    }

    template<typename Source>
    seastar::future<runtime::result<void>> seal_recovered_owned(
      Source& source,
      const verified_extent& extent,
      std::uint32_t completed,
      codec::cooperative_work& work) {
        // The seal's writable handle replaces every read handle.
        co_await close_handles();
        if (lifetime_->failure.failed()) co_return lifetime_->failure.outcome();
        const segment_history_context history{
          descriptor_.segment,
          descriptor_.alignment,
          data_start_,
          descriptor_.logical_origin,
          descriptor_.physical_origin,
          descriptor_.profile};
        const auto covered = extent.boundary().coverage;
        if (
          extent.context() != history || !extent.digest()
          || covered.bytes().begin() != data_start_)
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        const auto end = covered.bytes().end();
        auto fixed = capacity();
        if (!fixed) co_return runtime::failure(fixed.error());
        const auto entries = (*fixed)->seal_page_entries(completed);
        if (!entries)
            co_return runtime::failure(
              detail::path_error(errc::resource_exhausted));
        retry_page_entries_ = *entries;
        // The grants and publishers creation would have prepared.
        auto bundle = budget_.try_reserve(
          byte_count{
            2 * config_.metadata.operation_bytes.value()
            + config_.metadata.execution_bytes.value() + 32_KiB});
        if (!bundle) co_return runtime::failure(bundle.error());
        bundle_memory_.emplace(std::move(*bundle));
        const auto object = local_paths::make(spec_.root)
                              ->object(
                                shard_,
                                {descriptor_.segment.segment(),
                                 descriptor_.segment.generation()},
                                *config_.retry_object);
        if (!object) co_return runtime::failure(object.error());
        bundle_publisher_.emplace(
          files_,
          budget_,
          target(*object, runtime::file_rename_policy::no_replace));
        if (
          auto prepared = co_await bundle_publisher_->prepare(
            config_.metadata.operation_bytes, work);
          !prepared)
            co_return prepared;
        pointer_publisher_.emplace(
          files_,
          budget_,
          target(
            path(local_segment_file::published),
            runtime::file_rename_policy::replace,
            publication_));
        if (
          auto prepared = co_await pointer_publisher_->prepare(
            config_.metadata.operation_bytes, work);
          !prepared)
            co_return prepared;
        if (auto opened = co_await open_data(true, work, true); !opened)
            co_return opened;
        auto size = co_await data_->size();
        if (!size) co_return runtime::failure(size.error());
        // Every byte the walk verified must still be there.
        if (*size < end.value())
            co_return runtime::failure(
              detail::path_error(errc::truncated_data));
        sealing_.extent = extent;
        // The one flush after the root covers the surviving extent as well.
        auto dependencies = [this](this auto, codec::cooperative_work&)
          -> seastar::future<runtime::result<void>> {
            if (lifetime_->failure.failed())
                co_return lifetime_->failure.outcome();
            co_return runtime::result<void>{};
        };
        co_return co_await seal_root(
          source, extent, end, completed, *size, std::move(dependencies), work);
    }

    // The retry bundle, then the sealed root at `end`; every byte past the
    // root is removed in the same durable step, one flush covers the extent
    // and the root, and only then is the sealed publication the first
    // published change. `file_bytes` is the file's size before the root,
    // through any zero-written or recovered tail.
    template<typename Source, typename Dependencies>
    seastar::future<runtime::result<void>> seal_root(
      Source& source,
      const verified_extent& finished,
      runtime::file_position end,
      std::uint32_t completed,
      std::uint64_t file_bytes,
      Dependencies dependencies,
      codec::cooperative_work& work) {
        const footer_expectation location{
          {descriptor_.segment,
           descriptor_.alignment,
           data_start_,
           descriptor_.logical_origin,
           descriptor_.physical_origin,
           descriptor_.profile},
          end};
        const auto pages = (std::uint64_t{completed} + retry_page_entries_ - 1)
                           / retry_page_entries_;
        if (
          pages > config_.admission.maximum_retry_pages
          || pages > maximum_object_pages)
            co_return runtime::failure(
              detail::path_error(errc::resource_exhausted));
        const auto refs_charge = pages
                                   ? budget_.allocation_charge(
                                       byte_count{pages * sizeof(page_ref)})
                                   : runtime::result<byte_count>{byte_count{}};
        if (!refs_charge) co_return runtime::failure(refs_charge.error());
        if (
          *refs_charge > config_.metadata.metadata_bytes
          || *refs_charge > config_.metadata.operation_bytes)
            co_return runtime::failure(
              detail::path_error(errc::resource_exhausted));
        std::vector<page_ref> refs;
        refs.reserve(pages);
        detail::segment_retry_pages<Source> first_pass{
          &source,
          location,
          completed,
          retry_page_entries_,
          config_.metadata,
          *refs_charge};
        for (std::uint32_t i = 0; i < pages; ++i) {
            auto page = co_await first_pass.next(work);
            if (!page) co_return runtime::failure(page.error());
            if (!*page || !first_pass.last)
                co_return runtime::failure(
                  detail::path_error(errc::wrong_context));
            refs.push_back(*first_pass.last);
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
        }
        auto encoded = co_await encode_sealed_footer(
          finished,
          location,
          completed,
          refs,
          work,
          config_.metadata.operation_bytes.checked_sub(*refs_charge).value(),
          config_.metadata.charge);
        if (!encoded)
            co_return runtime::failure(
              detail::path_error(encoded.error().code()));
        const auto file_end = end.checked_add(encoded->bytes.size());
        if (
          !file_end
          || file_end->value() > descriptor_.maximum_data_bytes.value())
            co_return runtime::failure(
              detail::path_error(errc::resource_exhausted));
        const auto reference = local_root_reference::make(
          local_root_kind::sealed_retry,
          *config_.retry_object,
          end,
          encoded->bytes.size(),
          page_count::make(static_cast<std::uint32_t>(pages)).value(),
          encoded->digest);
        if (!reference)
            co_return runtime::failure(
              detail::path_error(errc::invalid_argument));
        const auto footer = local_footer_reference::make(
          end,
          encoded->bytes.size(),
          static_cast<std::uint16_t>(codec::format_family::sealed_extent),
          encoded->digest);
        if (!footer)
            co_return runtime::failure(
              detail::path_error(errc::invalid_argument));
        sealing_.boundary_candidate = *footer;
        sealing_.retry_candidate = *reference;
        // Keep the exact root bytes in the seal grant for the later data write.
        // The bundle reader decodes an admitted alias and owns the page-only
        // file.
        auto bundle = co_await local_bundle::make(
          *reference,
          location,
          encoded->bytes.share(),
          budget_,
          config_.metadata,
          work,
          std::move(bundle_memory_));
        bundle_memory_.reset();
        if (!bundle) co_return runtime::failure(bundle.error());
        detail::segment_retry_pages<Source> second_pass{
          &source,
          location,
          completed,
          retry_page_entries_,
          config_.metadata,
          *refs_charge};
        auto published = co_await publish_local_bundle(
          files_,
          owner_,
          spec_,
          shard_,
          std::move(*bundle),
          std::move(second_pass),
          std::move(dependencies),
          budget_,
          work,
          &*bundle_publisher_);
        sealing_.retry = std::move(published.publication);
        if (sealing_.retry.failure.failed())
            co_return sealing_.retry.failure.outcome();
        if (!published.reference)
            co_return runtime::failure(detail::path_error(errc::io_failure));
        auto written = co_await data_->write(end, std::move(encoded->bytes));
        if (!written) co_return runtime::failure(written.error());
        if (*written != reference->bytes())
            co_return runtime::failure(detail::path_error(errc::io_failure));
        sealing_.root_written = true;
        // A sealed extent ends exactly after its root; drop the zero-written
        // or recovered tail in the same durable step.
        if (file_bytes > file_end->value()) {
            auto trimmed = co_await data_->truncate(file_end->value());
            if (!trimmed) co_return runtime::failure(trimmed.error());
        }
        auto synced = co_await data_->flush(completion_->metadata());
        if (!synced) co_return runtime::failure(synced.error());
        sealing_.root_synced = true;
        if (publication_->value() == UINT64_MAX)
            co_return runtime::failure(detail::path_error(errc::out_of_range));
        auto next = local_publication_generation::make(
          publication_->value() + 1);
        if (!next)
            co_return runtime::failure(detail::path_error(errc::out_of_range));
        const auto metadata_header = local_metadata_header::make(
                                       local_metadata_kind::object_publication,
                                       spec_.shard_owner(shard_).value(),
                                       *next)
                                       .value();
        const local_object_publication publication{
          descriptor_.segment,
          local_object_state::sealed,
          *footer,
          {*reference}};
        auto pointer = co_await encode_local_metadata(
          {metadata_header,
           spec_.identity.metadata_alignment,
           descriptor_.alignment},
          local_metadata_payload{publication},
          work,
          config_.metadata.operation_bytes,
          config_.metadata.charge);
        if (!pointer)
            co_return runtime::failure(
              detail::path_error(pointer.error().code()));
        auto valid = co_await owner_.validate(spec_);
        if (!valid) co_return valid;
        sealing_.pointer = co_await pointer_publisher_->publish(
          {spec_.shard_owner(shard_).value(), *next, publication_},
          std::move(pointer->bytes),
          work);
        if (sealing_.pointer.failure.failed())
            co_return sealing_.pointer.failure.outcome();
        if (
          sealing_.pointer.disposition
          != local_publication_disposition::durable)
            co_return runtime::failure(detail::path_error(errc::io_failure));
        publication_ = *next;
        append_ = model::append_state::sealed;
        immutable_ = segment_immutable_expectation{
          finished.boundary().coverage, *finished.digest(), *file_end};
        read_publication_ = publication;
        read_roots_.assign(1, location);
        co_await close_publishers();
        co_await close_handles();
        handle_ = lifetime_->failure.failed() ? segment_handle_state::closed
                                              : segment_handle_state::evicted;
        if (lifetime_->failure.failed()) co_return lifetime_->failure.outcome();
        seal_result_.boundary = *footer;
        seal_result_.retry = *reference;
        seal_result_.extent = immutable_;
        co_return runtime::result<void>{};
    }

    void fail_node(detail::segment_write_descriptor& node) noexcept {
        if (lifetime_->failure.exception())
            node.fail(lifetime_->failure.exception());
        else if (lifetime_->failure.error())
            node.fail(*lifetime_->failure.error());
    }

    seastar::future<> assemble_group(
      detail::segment_write_descriptor::pointer node,
      codec::cooperative_work& work) {
        node->state = detail::segment_write_state::assembling;
        std::optional<encoded_durable_footer> footer;
        try {
            do {
                const auto cut = node->layout.boundary();
                const auto coverage = cut.covered();
                const storage::coverage complete{
                  coverage.logical(),
                  coverage.physical(),
                  model::file_byte_span::make(data_start_, cut.end().bytes)
                    .value()};
                auto extended = extent_->extend_expected(complete, work);
                if (!extended) {
                    node->fail(detail::path_error(extended.error().code()));
                    break;
                }
                const codec::decode_budget memory{
                  config_.admission.working_bytes,
                  config_.admission.metadata_bytes,
                  config_.metadata.charge};
                for (std::size_t i = 0; i < node->blocks.size(); ++i) {
                    if (lifetime_->failure.failed()) {
                        fail_node(*node);
                        break;
                    }
                    const auto info = node->children[i].batch.info();
                    const auto original = info.context.submitted();
                    auto added = co_await extent_->add_block(
                      node->blocks[i],
                      {descriptor_.segment.topic(),
                       descriptor_.segment.range(),
                       original.id(),
                       original.binding(),
                       info.fingerprint},
                      memory,
                      work);
                    if (!added) {
                        node->fail(detail::path_error(added.error().code()));
                        break;
                    }
                }
                if (lifetime_->failure.failed()) {
                    fail_node(*node);
                    break;
                }
                auto prefix = extent_->checkpoint(work);
                if (!prefix) {
                    node->fail(detail::path_error(prefix.error().code()));
                    break;
                }
                if (
                  prefix->boundary().coverage != cut.covered()
                  || prefix->boundary().block_count != cut.end().blocks
                  || prefix->boundary().last_block
                       != std::optional{node->layout.blocks().back().records}) {
                    node->fail(detail::path_error(errc::wrong_context));
                    break;
                }
                auto encoded = co_await encode_durable_footer(
                  *prefix,
                  {cut.history(), cut.footer()->begin()},
                  work,
                  config_.admission.working_bytes,
                  config_.metadata.charge);
                if (!encoded) {
                    node->fail(detail::path_error(encoded.error().code()));
                    break;
                }
                footer.emplace(std::move(*encoded));
                if (footer->bytes().size() != cut.footer()->size()) {
                    node->fail(detail::path_error(errc::wrong_context));
                    break;
                }
                // The source's staging/promotion is covered by preparation.
                // Feed this footer once, retaining its exact bytes for I/O.
                auto included = co_await extent_->add_footer(
                  *footer, memory, work, *prefix);
                if (!included) {
                    node->fail(detail::path_error(included.error().code()));
                    break;
                }
                if (lifetime_->failure.failed()) {
                    fail_node(*node);
                    break;
                }
                std::size_t fragments = footer->bytes().fragment_count();
                auto retained = footer->bytes().retained_bytes();
                for (const auto& block : node->blocks) {
                    fragments += block.bytes().fragment_count();
                    retained = retained
                                 .checked_add(block.bytes().retained_bytes())
                                 .value();
                }
                const auto assembly = budget_.allocation_charge(
                  byte_count{
                    fragments
                    * bytes::fragmented_buffer::fragment_descriptor_size()});
                if (
                  fragments > bytes::max_buffer_fragments
                  || retained > node->retained_bound || !assembly
                  || *assembly > node->gather_credit) {
                    node->fail(detail::path_error(errc::resource_exhausted));
                    break;
                }
                bytes::fragmented_buffer_builder builder{
                  {.initial_fragment_bytes = byte_count{1},
                   .max_fragment_bytes = byte_count{1},
                   .max_total_bytes = node->extent().size(),
                   .max_retained_bytes = retained,
                   .max_fragments = fragments}};
                builder.reserve_fragments(item_count{fragments}).value();
                for (auto& block : node->blocks) {
                    builder.append_buffer(std::move(block).release_bytes())
                      .value();
                    co_await work.drain_inline(
                      work.byte_quantum(), work.item_quantum());
                }
                builder.append_buffer(std::move(*footer).release_bytes())
                  .value();
                node->payload = builder.finish().value();
                if (lifetime_->failure.failed()) {
                    fail_node(*node);
                    break;
                }
                if (auto ready = work.poll(); !ready) {
                    node->fail(detail::path_error(ready.error().code()));
                    break;
                }
                node->state = detail::segment_write_state::queued;
            } while (false);
        } catch (...) {
            node->fail(std::current_exception());
        }
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        footer.reset();
        // The dispatcher awaits this directly; failure already woke others.
        if (node->failure.failed()) extent_->close();
    }

    seastar::future<segment_barrier_outcome>
    barrier_owned(segment_captured_boundary cut, seastar::gate::holder holder) {
        static_cast<void>(holder);
        barrier_busy_ = true;
        auto idle = seastar::defer([this] noexcept { barrier_busy_ = false; });
        segment_barrier_outcome result;
        try {
            while (!lifetime_->failure.failed()
                   && positions_->written.bytes < cut.end().bytes)
                co_await lifetime_->changed.when();
            if (
              !lifetime_->failure.failed()
              && positions_->durable.bytes < cut.end().bytes) {
                // Synchronized writes were durable when they completed. A
                // flush covers every ordinary write completed before it
                // began, so flush only if one completed after the last.
                const auto covered = plain_writes_;
                if (covered != flushed_plain_writes_) {
                    lifetime_->failure.observe(
                      co_await data_->flush(completion_->metadata()));
                    if (!lifetime_->failure.failed())
                        flushed_plain_writes_ = std::max(
                          flushed_plain_writes_, covered);
                }
                if (!lifetime_->failure.failed())
                    positions_->durable = cut.end();
            }
            if (!lifetime_->failure.failed())
                result.receipt = segment_durable_receipt{std::move(cut)};
        } catch (...) {
            lifetime_->failure.observe(std::current_exception());
        }
        result.failure = lifetime_->failure;
        // Only a failure changes what other waiters observe.
        if (result.failure.failed()) lifetime_->changed.broadcast();
        co_return result;
    }

    // Groups are written in file order but not one at a time: a gathered
    // write starts as soon as its groups are assembled and a slot is free,
    // and its completion is recorded on its first group. Outcomes, written
    // positions and notifications still apply strictly in file order.
    seastar::future<> dispatch() {
        using state = detail::segment_write_state;
        using pointer = detail::segment_write_descriptor::pointer;
        seastar::abort_source abort;
        codec::cooperative_work work{config_.policy, abort};
        dispatcher_started_.set_value();
        while (!execution_stopped_ || !inflight_.empty() || extension_) {
            if (extension_ && extension_->available()) {
                extension_->get();
                extension_.reset();
                continue;
            }
            maybe_extend();
            if (inflight_.empty()) {
                co_await lifetime_->changed.when();
                continue;
            }
            auto front = inflight_.front();
            if (
              front->state == state::dispatched && front->write
              && front->write->available()) {
                front->write->get();
                front->write.reset();
                const auto outcome = front->write_failure;
                const auto covered = std::exchange(front->gathered, 0U);
                std::uint32_t applied = 0;
                for (const auto& node : inflight_) {
                    if (applied++ == covered) break;
                    KWAQUE_INVARIANT(
                      invariant_id{"KQ-SEGMENT-WRITE-ORDER"},
                      node->state == state::dispatched,
                      "gathered segment write outcome crossed its groups");
                    if (outcome.exception())
                        node->fail(outcome.exception());
                    else if (outcome.error())
                        node->fail(*outcome.error());
                    else
                        node->state = state::done;
                }
                continue;
            }
            // A write in flight still owns its bytes; it is never failed
            // early, only after it has completed.
            if (
              lifetime_->failure.failed() && front->state != state::done
              && front->state != state::dispatched)
                fail_node(*front);
            if (front->state == state::done) {
                // Error notification may precede the end of an encoder's
                // cleanup. Join it before releasing any of its children.
                co_await front->execution.close();
                co_await work.drain_inline(
                  work.byte_quantum(), work.item_quantum());
                // Bytes written behind a failed group are not part of the
                // extent, even when their own write succeeded.
                if (prefix_failed_ && !front->failure.failed())
                    fail_node(*front);
                // A written group stays charged, with its children, until the
                // digest has hashed its bytes; the barrier does not wait.
                bool digest = front->observed && !front->failure.failed()
                              && !prefix_failed_ && !closing_
                              && !front->digest.empty();
                if (digest) {
                    try {
                        digesting_.push_back(front);
                    } catch (...) {
                        lifetime_->failure.observe(std::current_exception());
                        digest = false;
                    }
                }
                if (front->observed) {
                    front->payload = {};
                    front->blocks.clear();
                    if (!digest) {
                        front->digest = {};
                        front->children.clear();
                        front->working.reset();
                    }
                }
                // A caller may still borrow an unsubmitted handoff's encoded
                // blocks. Close fences the reservation but must not destroy
                // that caller's byte owner; its own release ends the borrow.
                if (front->failure.failed())
                    prefix_failed_ = true;
                else if (!prefix_failed_)
                    positions_->written = front->layout.boundary().end();
                if (!digest) release_pending(*front);
                inflight_.pop_front();
                // State and the reusable slot are visible before notification.
                lifetime_->changed.broadcast();
                front->notify();
                continue;
            }
            // Past the writes in flight: gather the queued run into one write
            // when its handle has a free slot, otherwise assemble the next
            // authorized group so its encoding overlaps the writes. Groups
            // stay owned by the queue, which only this loop pops, so the run
            // is borrowed; only the write's first group is shared with it.
            std::array<
              detail::segment_write_descriptor*,
              runtime::maximum_queued_file_writes>
              selected{};
            pointer lead;
            std::size_t count = 0, fragments = 0;
            byte_count logical{}, retained{}, credit{};
            auto first = inflight_.begin();
            while (first != inflight_.end()
                   && (*first)->state == state::dispatched)
                ++first;
            auto rest = first;
            while (rest != inflight_.end() && (*rest)->state == state::queued)
                ++rest;
            bool synchronous = false;
            std::uint64_t run_end = 0;
            if (!lifetime_->failure.failed() && first != rest) {
                const auto start = (*first)->extent().begin();
                auto end = start;
                // A run that can be one synchronized write is not grown past
                // the synchronized size; the remainder is the next write.
                const bool window = sync_ && start.value() >= zero_begin_
                                    && start.value() < prezeroed_end_;
                for (auto it = first; it != rest; ++it) {
                    const auto& node = *it;
                    auto next = logical.checked_add(node->payload.size());
                    auto backing = retained.checked_add(
                      node->payload.retained_bytes());
                    if (
                      !next || !backing
                      || *next > runtime::maximum_file_io_bytes
                      || *backing > runtime::maximum_file_io_bytes
                      || fragments + node->payload.fragment_count()
                           > bytes::max_buffer_fragments
                      || (window && count != 0
                          && logical <= config_.synchronous_write_bytes
                          && *next > config_.synchronous_write_bytes))
                        break;
                    auto next_credit = credit.checked_add(node->gather_credit);
                    const auto assembly = budget_.allocation_charge(
                      byte_count{
                        (fragments + node->payload.fragment_count())
                        * bytes::fragmented_buffer::
                          fragment_descriptor_size()});
                    if (!next_credit || !assembly || *assembly > *next_credit)
                        break;
                    KWAQUE_INVARIANT(
                      invariant_id{"KQ-SEGMENT-ADJACENT"},
                      node->extent().begin() == end,
                      "segment gather crossed a reservation hole");
                    if (count == 0) lead = node;
                    selected[count++] = node.get();
                    end = node->extent().end();
                    logical = *next;
                    retained = *backing;
                    credit = *next_credit;
                    fragments += node->payload.fragment_count();
                }
                KWAQUE_INVARIANT(
                  invariant_id{"KQ-SEGMENT-GATHER"},
                  count != 0,
                  "admitted segment group cannot fit one runtime write");
                // Small writes inside the zero-written range are pure
                // overwrites: synchronized completion makes them durable
                // without a flush. Anything else needs the barrier's.
                synchronous = sync_
                              && logical <= config_.synchronous_write_bytes
                              && start.value() >= zero_begin_
                              && end.value() <= prezeroed_end_;
                run_end = end.value();
                if (
                  synchronous ? sync_writes_ >= config_.concurrent_writes
                              : plain_writing_) {
                    // Held behind an extension: no further extension
                    // starts until this run has been issued.
                    plain_waiting_ |= !synchronous && extension_.has_value();
                    count = 0;
                }
            }
            if (count == 0) {
                // One group at a time, in file order: the next pass can
                // write it before the one after it is assembled.
                if (
                  !lifetime_->failure.failed() && rest != inflight_.end()
                  && (*rest)->state == state::authorized) {
                    auto next = *rest;
                    co_await assemble_group(std::move(next), work);
                } else
                    co_await lifetime_->changed.when();
                continue;
            }
            for (std::size_t i = 0; i < count; ++i)
                selected[i]->state = state::dispatched;
            // Extensions start past every issued write.
            issued_end_ = run_end;
            plain_waiting_ = false;
            runtime::first_failure failed;
            bytes::fragmented_buffer payload;
            try {
                // The digest later hashes these exact immutable bytes.
                for (std::size_t i = 0; i < count; ++i)
                    selected[i]->digest = selected[i]->payload.share();
                if (count == 1)
                    payload = std::move(selected[0]->payload);
                else {
                    bytes::fragmented_buffer_builder gather{
                      {.initial_fragment_bytes = byte_count{1},
                       .max_fragment_bytes = byte_count{1},
                       .max_total_bytes = logical,
                       .max_retained_bytes = retained,
                       .max_fragments = fragments}};
                    gather.reserve_fragments(item_count{fragments}).value();
                    for (std::size_t i = 0; i < count; ++i) {
                        gather.append_buffer(std::move(selected[i]->payload))
                          .value();
                        co_await work.drain_inline(
                          work.byte_quantum(), work.item_quantum());
                    }
                    payload = gather.finish().value();
                }
                if (lifetime_->failure.failed()) failed = lifetime_->failure;
            } catch (...) {
                failed.observe(std::current_exception());
            }
            if (!failed.failed()) {
                auto& head = *lead;
                head.gathered = static_cast<std::uint32_t>(count);
                if (synchronous)
                    ++sync_writes_;
                else
                    plain_writing_ = true;
                try {
                    head.write.emplace(write_gathered(
                      std::move(lead),
                      std::move(payload),
                      logical,
                      synchronous));
                } catch (...) {
                    // The frame could not be created; nothing was submitted.
                    head.gathered = 0;
                    if (synchronous)
                        --sync_writes_;
                    else
                        plain_writing_ = false;
                    failed.observe(std::current_exception());
                }
            }
            if (failed.failed())
                for (std::size_t i = 0; i < count; ++i) {
                    if (failed.exception())
                        selected[i]->fail(failed.exception());
                    else
                        selected[i]->fail(*failed.error());
                }
        }
    }

    // One gathered write, joined through its first group. It may complete
    // before earlier writes: slots and flush evidence follow completion,
    // while the dispatcher applies the outcome in file order.
    seastar::future<> write_gathered(
      detail::segment_write_descriptor::pointer lead,
      bytes::fragmented_buffer payload,
      byte_count logical,
      bool synchronous) {
        auto& failed = lead->write_failure;
        try {
            auto written = co_await (synchronous ? *sync_ : *data_)
                             .write(lead->extent().begin(), std::move(payload));
            failed.observe(written);
            if (written && *written != logical)
                failed.observe(detail::path_error(errc::io_failure));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        if (synchronous)
            --sync_writes_;
        else {
            plain_writing_ = false;
            // A flush covers this write only if it begins after now.
            if (!failed.failed()) ++plain_writes_;
        }
        // Stop dispatching later groups at once.
        if (failed.exception())
            lifetime_->failure.observe(failed.exception());
        else if (failed.error())
            lifetime_->failure.observe(*failed.error());
        lifetime_->changed.broadcast();
    }

    // The owner's descriptor, data start, limits, policy and alignments never
    // change once its data start is known, so their part of every group's
    // plan is computed once.
    runtime::result<detail::segment_capacity_constants*> capacity() {
        if (!capacity_) {
            auto limits = config_.admission;
            limits.metadata_bytes = std::min(
              limits.metadata_bytes, config_.metadata.metadata_bytes);
            auto made = detail::segment_capacity_constants::make(
              descriptor_,
              data_start_,
              limits,
              config_.policy,
              config_.metadata.charge,
              spec_.identity.metadata_alignment,
              config_.metadata.operation_bytes);
            if (!made) return runtime::failure(made.error());
            capacity_.emplace(std::move(*made));
        }
        return &*capacity_;
    }

    runtime::result<byte_count> node_charge(std::size_t fragments) const {
        if (fragments > bytes::max_buffer_fragments)
            return runtime::failure(
              detail::path_error(errc::resource_exhausted));
        const auto assembly = budget_.allocation_charge(
          byte_count{
            fragments * bytes::fragmented_buffer::fragment_descriptor_size()});
        const auto control = budget_.allocation_charge(
          byte_count{sizeof(detail::segment_write_descriptor) + 64});
        const auto chunk = budget_.allocation_charge(
          byte_count{queue_chunk_bytes});
        if (!assembly) return runtime::failure(assembly.error());
        if (!control) return runtime::failure(control.error());
        if (!chunk) return runtime::failure(chunk.error());
        // Descriptor lists: the group payload, the gathered write and the
        // digest's shared view of the same bytes.
        return byte_count{
          control->value() + chunk->value() + 3 * assembly->value()
          + config_.admission.execution_bytes.value()};
    }

    bool group_fits_budget(
      byte_count working,
      byte_count retry_facts,
      byte_count control,
      bool include_retained = false) const {
        // These are distinct, startup-held leases. Exclude other callers and
        // outstanding preparations: those cause temporary queue pressure.
        const std::array fixed{
          held_.bytes(),
          lifetime_->held.bytes(),
          completion_->charged_bytes(),
          seal_memory_->bytes(),
          bundle_memory_->bytes(),
          bundle_publisher_->prepared_bytes(),
          pointer_publisher_->prepared_bytes(),
          creation_.descriptor.charged_bytes(),
          creation_.data.charged_bytes(),
          creation_.publication.charged_bytes(),
          zero_memory_ ? zero_memory_->bytes() : byte_count{}};
        const bool retained = include_retained && retry_memory_;
        byte_count total = retained ? retry_memory_->bytes() : byte_count{};
        std::uint64_t tasks = 3 + (retained ? 1 : 0);
        for (auto amount : fixed) {
            const auto next = total.checked_add(amount);
            if (!next) return false;
            total = *next;
            if (amount.value() != 0) ++tasks;
        }
        for (auto amount : {working, retry_facts, control}) {
            const auto charged = budget_.reservation_charge(amount);
            if (!charged) return false;
            const auto next = total.checked_add(*charged);
            if (!next) return false;
            total = *next;
        }
        const auto limits = budget_.limits();
        return total <= limits.bytes && tasks <= limits.tasks;
    }

    static runtime::result<std::unique_ptr<segment_writer>> make(
      Backend& files,
      Owner& owner,
      const local_device_spec& spec,
      std::uint32_t shard,
      local_segment_descriptor descriptor,
      workload_budget& budget,
      segment_writer_config config,
      bool fresh) {
        auto header = segment_header::make(
          descriptor.segment,
          descriptor.logical_origin,
          descriptor.alignment,
          descriptor.profile);
        if (!header)
            return runtime::failure(
              detail::path_error(
                header.error() == errc::unsupported_format
                  ? errc::unsupported_format
                  : errc::invalid_argument));
        if (
          auto valid = validate_local_segment(spec, shard, descriptor, *header);
          !valid)
            return runtime::failure(valid.error());
        if (auto valid = config.metadata.validate(); !valid)
            return runtime::failure(valid.error());
        if (auto valid = config.admission.validate(); !valid)
            return runtime::failure(valid.error());
        if (fresh) {
            if (
              config.policy.config().max_work_bytes < byte_count{1_KiB}
              || config.policy.config().max_work_items < item_count{64})
                return runtime::failure(
                  detail::path_error(errc::invalid_argument));
            if (
              !config.maximum_groups
              || config.maximum_groups > runtime::maximum_queued_file_writes
              || !config.maximum_pending_bytes.value()
              || config.maximum_pending_bytes > runtime::maximum_file_io_bytes)
                return runtime::failure(
                  detail::path_error(errc::invalid_argument));
            if (auto valid = config.completion.validate(); !valid)
                return runtime::failure(valid.error());
            if (
              !config.retry_object || !config.retry_object->is_valid()
              || config.metadata.operation_bytes < byte_count{64_KiB})
                return runtime::failure(
                  detail::path_error(errc::invalid_argument));
            if (
              config.preallocation_bytes > descriptor.maximum_data_bytes
              || config.preallocation_extension_bytes
                   > descriptor.maximum_data_bytes
              || (config.preallocation_extension_bytes.value() != 0
                  && config.preallocation_bytes.value() == 0)
              || config.synchronous_write_bytes > runtime::maximum_file_io_bytes
              || config.concurrent_writes == 0
              || config.concurrent_writes
                   > runtime::maximum_file_write_concurrency)
                return runtime::failure(
                  detail::path_error(errc::invalid_argument));
            auto limits = config.admission;
            limits.metadata_bytes = std::min(
              limits.metadata_bytes, config.metadata.metadata_bytes);
            if (
              auto valid = detail::validate_empty_segment_seal(
                descriptor.alignment,
                limits,
                config.policy,
                config.metadata.charge,
                config.metadata.operation_bytes);
              !valid)
                return runtime::failure(valid.error());
        }
        auto instance = budget.allocation_charge(
          byte_count{sizeof(segment_writer)});
        auto path = budget.allocation_charge(
          byte_count{runtime::maximum_file_path_bytes + 1});
        auto token = budget.allocation_charge(
          byte_count{sizeof(segment_captured_boundary::lifetime) + 64});
        // Node charges cover the chunks holding groups; each of the two
        // queues also keeps one emptied chunk for reuse.
        auto chunk = budget.allocation_charge(byte_count{queue_chunk_bytes});
        if (!instance) return runtime::failure(instance.error());
        if (!path) return runtime::failure(path.error());
        if (!token) return runtime::failure(token.error());
        if (!chunk) return runtime::failure(chunk.error());
        // A zero-written segment also owns a synchronized-write handle with
        // its own staging window.
        const std::uint32_t handles
          = fresh && config.preallocation_bytes.value() != 0 ? 2U : 1U;
        auto held = budget.try_reserve(
          byte_count{
            instance->value() + 16 * path->value() + 2 * chunk->value()
            + handles * runtime::maximum_file_write_buffer_bytes.value()
            + config.metadata.operation_bytes.value()
            + config.metadata.execution_bytes.value()});
        if (!held) return runtime::failure(held.error());
        if (auto acquired = held->try_acquire_handles(handles); !acquired)
            return runtime::failure(acquired.error());
        if (fresh) {
            const auto paths = local_paths::make(spec.root).value();
            const local_segment_name identity{
              descriptor.segment.segment(), descriptor.segment.generation()};
            // Qualify every final and longest temporary path before any create
            // effect. The future retry object is deeper than the data/header
            // path.
            for (const auto& selected :
                 {paths.segment_file(
                    shard, identity, local_segment_file::descriptor),
                  paths.segment_file(shard, identity, local_segment_file::data),
                  paths.segment_file(
                    shard, identity, local_segment_file::published),
                  paths.object(shard, identity, *config.retry_object)}) {
                if (!selected) return runtime::failure(selected.error());
                const auto slash = selected->value().rfind('/');
                const auto parent = runtime::file_path::make(
                                      selected->value().substr(0, slash))
                                      .value();
                const auto leaf = runtime::file_name::make(
                                    selected->value().substr(slash + 1))
                                    .value();
                const auto temporary = local_temporary_name(
                  leaf,
                  local_publication_generation::make(UINT64_MAX).value(),
                  63);
                if (!temporary) return runtime::failure(temporary.error());
                if (auto valid = local_child_path(parent, *temporary); !valid)
                    return runtime::failure(valid.error());
            }
        }
        auto token_held = budget.try_reserve(*token);
        if (!token_held) return runtime::failure(token_held.error());
        auto lifetime
          = seastar::make_lw_shared<segment_captured_boundary::lifetime>(
            std::move(*token_held));
        return std::unique_ptr<segment_writer>{new segment_writer(
          files,
          owner,
          spec,
          shard,
          std::move(descriptor),
          budget,
          config,
          *header,
          std::move(*held),
          std::move(lifetime))};
    }
    segment_writer(
      Backend& files,
      Owner& owner,
      local_device_spec spec,
      std::uint32_t shard,
      local_segment_descriptor descriptor,
      workload_budget& budget,
      segment_writer_config config,
      segment_header header,
      workload_reservation held,
      seastar::lw_shared_ptr<segment_captured_boundary::lifetime> lifetime)
      : files_(files)
      , owner_(owner)
      , spec_(std::move(spec))
      , shard_(shard)
      , descriptor_(std::move(descriptor))
      , budget_(budget)
      , config_(config)
      , header_(header)
      , held_(std::move(held))
      , lifetime_(std::move(lifetime)) {}

    runtime::file_path path(local_segment_file kind) const {
        return local_paths::make(spec_.root)
          ->segment_file(
            shard_,
            {descriptor_.segment.segment(), descriptor_.segment.generation()},
            kind)
          .value();
    }
    local_publication_target target(
      runtime::file_path path,
      runtime::file_rename_policy policy,
      std::optional<local_publication_generation> current
      = std::nullopt) const {
        const auto split = path.value().rfind('/');
        return {
          spec_.shard_owner(shard_).value(),
          spec_.root,
          runtime::file_path::make(path.value().substr(0, split)).value(),
          runtime::file_name::make(path.value().substr(split + 1)).value(),
          policy,
          current};
    }
    seastar::future<local_publication_outcome> publish_new(
      local_segment_file kind,
      bytes::fragmented_buffer payload,
      codec::cooperative_work& work) {
        local_publication_outcome output;
        auto valid = co_await owner_.validate(spec_);
        if (!valid) {
            output.failure.observe(valid);
            co_return output;
        }
        local_file_publisher<Backend> publisher{
          files_,
          budget_,
          target(path(kind), runtime::file_rename_policy::no_replace)};
        try {
            output = co_await publisher.publish(
              {spec_.shard_owner(shard_).value(),
               local_publication_generation::make(1).value(),
               {}},
              std::move(payload),
              work);
        } catch (...) {
            output.failure.observe(std::current_exception());
        }
        try {
            output.failure.observe(co_await publisher.close());
        } catch (...) {
            output.failure.observe(std::current_exception());
        }
        co_return output;
    }
    // A writable handle normally opens a file holding only its header; a
    // recovered seal's opens the existing extent.
    seastar::future<runtime::result<void>> open_data(
      bool writable, codec::cooperative_work& work, bool existing = false) {
        auto loaded = co_await load_local_segment(
          files_,
          owner_,
          spec_,
          shard_,
          descriptor_,
          header_,
          budget_,
          config_.metadata,
          work);
        if (!loaded) co_return runtime::failure(loaded.error());
        if (data_start_ != loaded->header.bytes.end()) capacity_.reset();
        data_start_ = loaded->header.bytes.end();
        auto opened = co_await files_.open(
          path(local_segment_file::data),
          {.access = writable ? runtime::file_access::read_write
                              : runtime::file_access::read_only,
           .close_policy = runtime::file_close_policy::checked});
        if (!opened) co_return runtime::failure(opened.error());
        data_.emplace(std::move(*opened));
        handle_ = segment_handle_state::open;
        writable_ = writable;
        if (writable) {
            // The largest power of two whose allocator-served charge fits the
            // contiguous ceiling bounds staging on every writable handle and
            // sizes the zero chunk.
            auto cap = byte_count{maximum_contiguous_allocation_bytes};
            while (!budget_.allocation_charge(cap)
                   && cap > descriptor_.alignment.bytes())
                cap = byte_count{cap.value() / 2};
            if (auto qualified = budget_.allocation_charge(cap); !qualified)
                co_return runtime::failure(qualified.error());
            if (auto limited = data_->limit_write_allocation(cap); !limited)
                co_return limited;
            write_allocation_ = cap;
        }
        auto geometry = data_->geometry();
        if (
          !geometry || !budget_.allocation_charge(geometry->memory_alignment())
          || (writable && !geometry->supports_disk_alignment(descriptor_.alignment.bytes())))
            co_return runtime::failure(
              detail::path_error(errc::invalid_argument));
        auto size = co_await data_->size();
        if (!size) co_return runtime::failure(size.error());
        if (
          *size < data_start_.value()
          || *size > descriptor_.maximum_data_bytes.value()
          || (writable && !existing && *size != data_start_.value()))
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        auto read = co_await data_->read({}, byte_count{data_start_.value()});
        if (!read) co_return runtime::failure(read.error());
        if (read->data().size().value() != data_start_.value())
            co_return runtime::failure(
              detail::path_error(errc::truncated_data));
        bytes::fragmented_buffer_parser input{std::move(*read).take_data()};
        auto memory = detail::metadata_file_budget(
          input, config_.metadata, work);
        if (!memory)
            co_return runtime::failure(
              detail::path_error(memory.error().code()));
        auto decoded = co_await decode_segment_header(
          input,
          header_,
          {},
          *memory,
          work,
          {},
          codec::input_boundary::complete);
        if (!decoded)
            co_return runtime::failure(
              detail::path_error(decoded.error().code()));
        if (!input.at_end() || decoded->bytes.end() != data_start_)
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        if (writable) {
            auto completion = completion_resources::make(
              budget_, *data_, config_.completion);
            if (!completion) co_return runtime::failure(completion.error());
            completion_.emplace(std::move(*completion));
        }
        co_return co_await owner_.validate(spec_);
    }
    seastar::future<> create_owned(codec::cooperative_work& work) {
        lifetime_->failure.observe(co_await owner_.validate(spec_));
        if (lifetime_->failure.failed()) co_return;
        const auto data_path = path(local_segment_file::data);
        const auto directory = runtime::file_path::make(
                                 data_path.value().substr(
                                   0, data_path.value().rfind('/')))
                                 .value();
        auto existing = co_await files_.stat(directory);
        if (existing) {
            lifetime_->failure.observe(
              detail::path_error(errc::already_exists));
            co_return;
        }
        if (existing.error().code() != errc::not_found) {
            lifetime_->failure.observe(existing);
            co_return;
        }
        creation_.descriptor = co_await publish_local_segment_descriptor(
          files_,
          owner_,
          spec_,
          shard_,
          descriptor_,
          header_,
          budget_,
          config_.metadata,
          work);
        lifetime_->failure.observe(creation_.descriptor.failure.outcome());
        if (lifetime_->failure.failed()) co_return;
        auto header = co_await encode_segment_header(
          header_,
          work,
          config_.metadata.operation_bytes,
          config_.metadata.charge);
        if (!header) {
            lifetime_->failure.observe(
              detail::path_error(header.error().code()));
            co_return;
        }
        creation_.data = co_await publish_new(
          local_segment_file::data, std::move(*header), work);
        lifetime_->failure.observe(creation_.data.failure.outcome());
        if (lifetime_->failure.failed()) co_return;
        lifetime_->failure.observe(co_await open_data(true, work));
        if (lifetime_->failure.failed()) co_return;
        if (config_.preallocation_bytes.value() != 0) {
            lifetime_->failure.observe(co_await preallocate(work));
            if (lifetime_->failure.failed()) co_return;
        }
        // Actual completion memory stays held. These grants are later consumed
        // by the existing bundle and codec/publication entrances, without a
        // fresh reservation from ordinary admission.
        auto seal = budget_.try_reserve(
          byte_count{
            config_.metadata.operation_bytes.value()
            + config_.metadata.execution_bytes.value()});
        if (!seal) {
            lifetime_->failure.observe(seal);
            co_return;
        }
        seal_memory_.emplace(std::move(*seal));
        auto bundle = budget_.try_reserve(
          byte_count{
            2 * config_.metadata.operation_bytes.value()
            + config_.metadata.execution_bytes.value() + 32_KiB});
        if (!bundle) {
            lifetime_->failure.observe(bundle);
            co_return;
        }
        bundle_memory_.emplace(std::move(*bundle));
        const auto object = local_paths::make(spec_.root)
                              ->object(
                                shard_,
                                {descriptor_.segment.segment(),
                                 descriptor_.segment.generation()},
                                *config_.retry_object);
        if (!object) {
            lifetime_->failure.observe(object);
            co_return;
        }
        bundle_publisher_.emplace(
          files_,
          budget_,
          target(*object, runtime::file_rename_policy::no_replace));
        lifetime_->failure.observe(
          co_await bundle_publisher_->prepare(
            config_.metadata.operation_bytes, work));
        if (lifetime_->failure.failed()) co_return;
        const auto generation = local_publication_generation::make(1).value();
        pointer_publisher_.emplace(
          files_,
          budget_,
          target(
            path(local_segment_file::published),
            runtime::file_rename_policy::replace,
            generation));
        lifetime_->failure.observe(
          co_await pointer_publisher_->prepare(
            config_.metadata.operation_bytes, work));
        if (lifetime_->failure.failed()) co_return;
        const auto metadata_header = local_metadata_header::make(
                                       local_metadata_kind::object_publication,
                                       spec_.shard_owner(shard_).value(),
                                       generation)
                                       .value();
        const local_metadata_payload payload{local_object_publication{
          descriptor_.segment, local_object_state::active, {}, {}}};
        auto encoded = co_await encode_local_metadata(
          {metadata_header,
           spec_.identity.metadata_alignment,
           descriptor_.alignment},
          payload,
          work,
          config_.metadata.operation_bytes,
          config_.metadata.charge);
        if (!encoded) {
            lifetime_->failure.observe(
              detail::path_error(encoded.error().code()));
            co_return;
        }
        creation_.publication = co_await publish_new(
          local_segment_file::published, std::move(encoded->bytes), work);
        lifetime_->failure.observe(creation_.publication.failure.outcome());
        if (lifetime_->failure.failed() || closing_) co_return;
        const segment_writer_position initial{
          descriptor_.logical_origin, descriptor_.physical_origin, data_start_};
        positions_.emplace(segment_writer_positions{initial, initial, initial});
        const segment_history_context history{
          descriptor_.segment,
          descriptor_.alignment,
          data_start_,
          descriptor_.logical_origin,
          descriptor_.physical_origin,
          descriptor_.profile};
        auto extent = extent_verifier::make(
          history,
          storage::coverage{
            model::range_logical_span::make(initial.logical, initial.logical)
              .value(),
            model::segment_relative_span::make(
              initial.physical, initial.physical)
              .value(),
            model::file_byte_span::make(data_start_, data_start_).value()},
          config_.policy,
          extent_layout_kind::initial_append,
          {},
          extent_integrity::crc32c_and_deferred_digest);
        if (!extent) {
            lifetime_->failure.observe(
              detail::path_error(extent.error().code()));
            co_return;
        }
        extent_.emplace(std::move(*extent));
        auto digest = extent_->deferred_digest();
        if (!digest) {
            lifetime_->failure.observe(
              detail::path_error(digest.error().code()));
            co_return;
        }
        digest_.emplace(std::move(*digest));
        publication_ = generation;
        dispatcher_.emplace(
          seastar::with_scheduling_group(
            budget_.scheduling_group(), [this]() noexcept -> seastar::future<> {
                try {
                    return dispatch();
                } catch (...) {
                    lifetime_->failure.observe(std::current_exception());
                    dispatcher_started_.set_value();
                    return seastar::make_ready_future<>();
                }
            }));
        co_await dispatcher_started_.get_future();
        digester_.emplace(
          seastar::with_scheduling_group(
            budget_.scheduling_group(), [this]() noexcept -> seastar::future<> {
                try {
                    return digest_groups();
                } catch (...) {
                    lifetime_->failure.observe(std::current_exception());
                    return seastar::make_ready_future<>();
                }
            }));
        if (!lifetime_->failure.failed() && !closing_)
            append_ = model::append_state::active;
    }

    // Zero-write [data start, window end) through the ordinary handle and
    // make it durable once, then open the synchronized-write handle. Each
    // bounded runtime write repeats one shared zero-filled chunk, which is
    // kept while the range can still be extended.
    seastar::future<runtime::result<void>>
    preallocate(codec::cooperative_work& work) {
        const auto alignment = descriptor_.alignment.bytes().value();
        const auto start = data_start_.value();
        const auto limit = descriptor_.maximum_data_bytes.value() / alignment
                           * alignment;
        if (start >= limit) co_return runtime::result<void>{};
        const auto window
          = std::min(config_.preallocation_bytes.value(), limit - start)
            / alignment * alignment;
        if (window == 0) co_return runtime::result<void>{};
        const auto end = start + window;
        // The zero chunk shares the writable handles' staging bound. Writes
        // repeat the chunk; its size is not the write size.
        const auto cap = write_allocation_;
        const auto zero_charge = budget_.allocation_charge(cap);
        const auto list_charge = budget_.allocation_charge(
          byte_count{
            2 * detail::zero_write_chunks
            * bytes::fragmented_buffer::fragment_descriptor_size()});
        if (!zero_charge) co_return runtime::failure(zero_charge.error());
        if (!list_charge) co_return runtime::failure(list_charge.error());
        auto transient = budget_.try_reserve(
          byte_count{zero_charge->value() + list_charge->value()});
        if (!transient) co_return runtime::failure(transient.error());
        // Reserve the whole range first, then zero-write it: otherwise the
        // concurrent zero writes fragment the file into many small extents.
        auto reserved = co_await data_->allocate(
          runtime::file_position{start}, byte_count{window});
        if (!reserved) co_return reserved;
        zero_ = detail::make_zero_chunk(cap);
        auto zeroed = co_await detail::zero_fill(
          *data_, zero_, start, end, work);
        if (!zeroed) co_return zeroed;
        auto synced = co_await data_->flush(completion_->metadata());
        if (!synced) co_return synced;
        auto opened = co_await files_.open(
          path(local_segment_file::data),
          {.access = runtime::file_access::read_write,
           .close_policy = runtime::file_close_policy::checked,
           .synchronous = true});
        if (!opened) co_return runtime::failure(opened.error());
        sync_.emplace(std::move(*opened));
        if (auto limited = sync_->limit_write_allocation(cap); !limited)
            co_return limited;
        if (
          auto shared = sync_->allow_concurrent_writes(
            config_.concurrent_writes);
          !shared)
            co_return shared;
        zero_begin_ = start;
        prezeroed_end_ = end;
        if (config_.preallocation_extension_bytes.value() != 0)
            zero_memory_.emplace(std::move(*transient));
        else
            zero_ = {};
        co_return runtime::result<void>{};
    }

    // Starts zero-writing the next extension when the zero-written range no
    // longer reaches far enough past the reservations. It begins after every
    // write already issued, so it never overwrites group bytes; later groups
    // wait for it through the ordinary-write slot it occupies.
    void maybe_extend() noexcept {
        const auto step = config_.preallocation_extension_bytes.value();
        if (
          !sync_ || step == 0 || extension_ || plain_writing_ || plain_waiting_
          || execution_stopped_ || closing_ || lifetime_->failure.failed())
            return;
        const auto alignment = descriptor_.alignment.bytes().value();
        const auto limit = descriptor_.maximum_data_bytes.value() / alignment
                           * alignment;
        const auto reserved = positions_->reserved.bytes.value();
        if (prezeroed_end_ > reserved && prezeroed_end_ - reserved >= step)
            return;
        const auto begin = std::max(prezeroed_end_, issued_end_);
        const auto length = std::max(step / alignment, std::uint64_t{1})
                            * alignment;
        if (begin >= limit) return;
        const auto end = std::min(limit, begin + std::min(length, limit));
        plain_writing_ = true;
        try {
            extension_.emplace(extend_window(begin, end));
        } catch (...) {
            // Only speed depends on the extension; try again later.
            plain_writing_ = false;
        }
    }

    // Zero-writes [begin, end), makes it durable and only then admits
    // synchronized writes there. Durability of the zeros is only for speed:
    // a synchronized write makes its own blocks durable either way.
    seastar::future<> extend_window(std::uint64_t begin, std::uint64_t end) {
        seastar::abort_source abort;
        codec::cooperative_work work{config_.policy, abort};
        runtime::first_failure failed;
        try {
            failed.observe(
              co_await detail::zero_fill(*data_, zero_, begin, end, work));
            // Each handle sizes the file once, when opened, and resizes it
            // to the end of any write it believes extends the file. Set the
            // synchronized handle's size to the new end first, while no
            // write is past it, so it never cuts what this handle wrote.
            if (!failed.failed()) failed.observe(co_await sync_->truncate(end));
            if (!failed.failed()) {
                auto synced = co_await data_->flush();
                if (!synced && synced.error().code() != errc::queue_full)
                    failed.observe(synced);
            }
        } catch (...) {
            failed.observe(std::current_exception());
        }
        plain_writing_ = false;
        if (failed.exception())
            lifetime_->failure.observe(failed.exception());
        else if (failed.error())
            lifetime_->failure.observe(*failed.error());
        else {
            if (begin > prezeroed_end_) zero_begin_ = begin;
            prezeroed_end_ = end;
        }
        lifetime_->changed.broadcast();
    }

    Backend& files_;
    Owner& owner_;
    local_device_spec spec_;
    std::uint32_t shard_;
    local_segment_descriptor descriptor_;
    workload_budget& budget_;
    segment_writer_config config_;
    segment_header header_;
    workload_reservation held_;
    seastar::lw_shared_ptr<segment_captured_boundary::lifetime> lifetime_;
    std::optional<runtime::file> data_;
    std::optional<completion_resources> completion_;
    std::optional<workload_reservation> seal_memory_, bundle_memory_;
    std::optional<local_file_publisher<Backend>> bundle_publisher_,
      pointer_publisher_;
    std::unique_ptr<local_generation_owner> generation_;
    segment_creation_progress creation_;
    std::optional<local_publication_generation> publication_;
    // The startup seal execution allowance covers the native hash state for
    // this walk's lifetime; per-group workspace covers parser/codec
    // temporaries.
    std::optional<extent_verifier> extent_;
    std::optional<segment_writer_positions> positions_;
    std::optional<model::file_byte_span> reserved_footer_;
    // Both queues empty at every idle point; each keeps its emptied chunk
    // instead of freeing and reallocating it on the next group.
    using group_queue
      = seastar::chunked_fifo<detail::segment_write_descriptor::pointer, 16, 1>;
    static constexpr std::size_t queue_chunk_bytes
      = 16 * sizeof(detail::segment_write_descriptor::pointer) + 64;
    group_queue inflight_;
    // Written groups waiting for the digest, in file order; each keeps its
    // children and workspace charged until hashed.
    group_queue digesting_;
    std::optional<extent_digest_walk> digest_;
    std::optional<seastar::future<>> digester_;
    // Synchronized-write handle for the zero-written range, and progress of
    // ordinary writes relative to the last flush.
    std::optional<runtime::file> sync_;
    std::uint64_t prezeroed_end_{0}, plain_writes_{0}, flushed_plain_writes_{0};
    // Gathered writes in flight on each handle; an extension occupies the
    // ordinary slot. Synchronized writes go only to [zero_begin_,
    // prezeroed_end_); extensions begin at or after issued_end_.
    std::uint32_t sync_writes_{0};
    bool plain_writing_{false};
    std::uint64_t zero_begin_{0}, issued_end_{0};
    bool plain_waiting_{false};
    std::optional<seastar::future<>> extension_;
    bytes::fragmented_buffer zero_;
    std::optional<workload_reservation> zero_memory_;
    // Staging bound installed on writable handles; also the zero chunk size.
    byte_count write_allocation_{};
    bool digest_stopped_{false};
    // Every accepted group's retry grant, adopted into one reservation.
    std::optional<workload_reservation> retry_memory_;
    byte_count pending_bytes_{}, pending_retained_{};
    std::optional<seastar::future<>> dispatcher_;
    seastar::promise<> dispatcher_started_;
    static constexpr std::uint32_t maximum_control_waiters = 8;
    std::uint32_t close_waiters_{0}, seal_waiters_{0}, digest_waiters_{0};
    std::uint32_t retry_page_entries_{1};
    bool execution_stopped_{false}, seal_started_{false}, seal_done_{false};
    bool read_busy_{false}, writable_{false};
    segment_seal_progress sealing_;
    segment_seal_outcome seal_result_;
    std::optional<segment_immutable_expectation> immutable_;
    std::optional<local_object_publication> read_publication_;
    std::vector<local_bundle_context> read_roots_;
    bool freezing_{false}, prefix_failed_{false}, barrier_busy_{false};
    std::optional<runtime::monotonic_time> first_acceptance_;
    runtime::file_position data_start_{};
    std::optional<detail::segment_capacity_constants> capacity_;
    seastar::gate operations_, read_operations_;
    model::append_state append_{model::append_state::creating};
    segment_handle_state handle_{segment_handle_state::closed};
    bool entered_{false}, recovered_{false}, closing_{false}, closed_{false};
    // Opened only to execute a recovered seal's decision.
    bool recovered_seal_{false};
};
} // namespace kwaque::storage
