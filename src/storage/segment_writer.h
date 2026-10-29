#pragma once

#include "src/base/units.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/codec/format_registry.h"
#include "src/storage/completion_resources.h"
#include "src/storage/extent_verifier.h"
#include "src/storage/local_generation.h"
#include "src/storage/local_paths.h"
#include "src/storage/local_publication.h"
#include "src/storage/local_spares.h"
#include "src/storage/segment_data.h"
#include "src/storage/segment_group.h"
#include "src/storage/segment_seal.h"
#include "src/storage/workload_budget.h"
#include "src/storage/zero_fill.h"

#include <seastar/core/shared_future.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/util/defer.hh>

#include <algorithm>
#include <array>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <new>
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
    // Creation writes zeros this far past the header, capped by the data
    // limit, and makes them durable once, so that the range can be written
    // with O_DSYNC. Group writes inside that range then overwrite blocks
    // that are already allocated and inside the file size, so their
    // durability needs no allocation metadata. Zero disables it. Sealing
    // truncates the file back to its exact end.
    byte_count preallocation_bytes{8_MiB};
    // Keeps the zero-written range ahead of the reservations: whenever less
    // than this remains past the reserved end, the owner zero-writes this
    // much more in the background and makes it durable before admitting
    // synchronized writes there. Groups past its start wait for it. Zero
    // keeps the creation window fixed; nonzero requires a window.
    byte_count preallocation_extension_bytes{8_MiB};
    // A gathered write no larger than this, wholly inside the zero-written
    // range, uses a second handle opened for synchronized writes: its
    // completion is durable, so a barrier covering only such writes needs no
    // flush. Larger writes use the ordinary handle and the barrier's flush.
    // A record of 1 MiB, the largest one takes, fits with its framing.
    byte_count synchronous_write_bytes{byte_count{1_MiB + 256_KiB}};
    // Synchronized writes in flight at once. Each group is written as soon
    // as it is assembled instead of waiting for the previous write; results
    // still apply in file order. Ordinary writes keep one in flight.
    std::uint32_t concurrent_writes{4};
    // One policy for this owner's growing extent. Inputs validated elsewhere
    // are revalidated under it before freezing; it never changes during growth.
    codec::limits policy{codec::limits::defaults()};
};
// The handle credits one writable segment holds while it is open: its data
// file's, and a second for the synchronized-write handle of a zero-written
// one, and its completion reserve's. Its seal's publications are a seal
// slot's (segment_seal_grants). A segment opened instead of created holds
// fewer.
[[nodiscard]] constexpr std::uint32_t
segment_writer_handles(const segment_writer_config& config) noexcept {
    return (config.preallocation_bytes.value() != 0 ? 2U : 1U)
           + completion_handles;
}

// What one creation holds beyond that while its files are made: the one
// publication it makes at a time.
inline constexpr std::uint32_t segment_creation_handles = publication_handles;
// What one seal slot's grants hold: its two publications'.
inline constexpr std::uint32_t segment_seal_handles = 2 * publication_handles;

// What a shard asks of the budget that funds its segment writers.
struct segment_handle_demand final {
    // Segments open at once.
    std::uint32_t segments{0};
    // How many of them may be in creation at once.
    std::uint32_t creating{1};
    // Handle credits for whatever else draws on the budget: the WAL writer,
    // restart work, readers.
    std::uint32_t reserved{0};
    // Seals that may run at once, each holding its slot's grants.
    std::uint32_t seals{0};
};

// Whether the budget can hold that demand. A shard checks its configured
// counts with this before it creates any owner. A count the budget cannot
// hold is then a configuration error at start, carrying the limit and what
// the configuration expects of it, and not a refusal at the first segment
// that no longer fits.
[[nodiscard]] inline runtime::result<void> validate_segment_handles(
  const workload_budget& budget,
  const segment_writer_config& config,
  segment_handle_demand demand) noexcept {
    const auto limit = std::uint64_t{budget.limits().handles};
    const auto expected
      = std::uint64_t{demand.segments} * segment_writer_handles(config)
        + std::uint64_t{std::min(demand.creating, demand.segments)}
            * segment_creation_handles
        + std::uint64_t{demand.seals} * segment_seal_handles + demand.reserved;
    if (expected <= limit) return {};
    runtime::operation_error refused{
      errc::resource_exhausted, runtime::operation_kind::resource};
    static_cast<void>(
      refused.add_context(runtime::operation_context_key::limit, limit));
    static_cast<void>(
      refused.add_context(runtime::operation_context_key::expected, expected));
    return runtime::failure(refused);
}

// What one seal holds while it runs: its own allowance, its retry bundle's,
// and its two publications' (memory and handle credits), which it prepares
// on its segment's own directories. A shard keeps one set for each seal
// that may run at once (segment_table_config::maximum_seals), reserved
// before any segment is written, so that a seal waits only for a slot and
// never fails for want of the budget, and a writable segment holds none of
// it. A seal takes the grants for its run and gives them back; what its
// bundle used goes with the bundle's publication, and fill() reserves it
// again. Each grant is what its user would reserve for itself.
struct segment_seal_grants final {
    std::optional<workload_reservation> seal, bundle;
    std::array<std::optional<workload_reservation>, 2> publications;

    // Reserves, for writers with the metadata limits `limits`, whatever is
    // missing or smaller than they need.
    [[nodiscard]] runtime::result<void>
    fill(workload_budget& budget, const local_store_io_limits& limits) {
        const auto top_up = [&budget](
                              std::optional<workload_reservation>& grant,
                              byte_count bytes,
                              std::uint32_t handles) -> runtime::result<void> {
            if (grant && grant->retained_bytes() >= bytes) return {};
            auto held = budget.try_reserve(bytes);
            if (!held) return runtime::failure(held.error());
            if (handles != 0)
                if (
                  auto acquired = held->try_acquire_handles(handles); !acquired)
                    return acquired;
            grant.emplace(std::move(*held));
            return {};
        };
        if (
          auto made = top_up(
            seal,
            byte_count{
              limits.operation_bytes.value() + limits.execution_bytes.value()},
            0);
          !made)
            return made;
        if (
          auto made = top_up(
            bundle,
            byte_count{
              2 * limits.operation_bytes.value()
              + limits.execution_bytes.value() + 32_KiB},
            0);
          !made)
            return made;
        const auto publication = local_publication_charge(
          budget, limits.operation_bytes);
        if (!publication) return runtime::failure(publication.error());
        for (auto& grant : publications)
            if (
              auto made = top_up(grant, *publication, publication_handles);
              !made)
                return made;
        return {};
    }
};
namespace detail {
// Whether `completed` is every fact `source` holds, for a source that says
// how many it has.
template<typename Source>
[[nodiscard]] bool
seals_whole_source(const Source& source, std::uint32_t completed) noexcept {
    if constexpr (requires {
                      {
                          source.completed()
                      } -> std::convertible_to<std::uint64_t>;
                  })
        return std::uint64_t{source.completed()} == completed;
    return true;
}
} // namespace detail

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
    // The spares a new segment's data file may be made of: see
    // local_spares.
    using spare_pool = local_spares<Backend, local_segment_name>;
    // No batch the segment takes may have an append time below
    // `append_floor`: the newest its range's earlier segments took, as the
    // range's owner knows it. A range's first segment has none.
    [[nodiscard]] static runtime::result<std::unique_ptr<segment_writer>>
    make_new(
      Backend& files,
      Owner& owner,
      const local_device_spec& spec,
      std::uint32_t shard,
      local_segment_descriptor descriptor,
      model::range_logical_end logical_origin,
      workload_budget& budget,
      segment_writer_config config,
      runtime::wall_time append_floor = runtime::wall_time{1}) {
        if (
          descriptor.layout != local_layout_kind::initial
          || append_floor < runtime::wall_time{1})
            return runtime::failure(detail::path_error(errc::invalid_argument));
        auto made = make(
          files,
          owner,
          spec,
          shard,
          std::move(descriptor),
          logical_origin,
          budget,
          config,
          true);
        if (made) (*made)->append_floor_ = append_floor;
        return made;
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

    // Makes the segment's files and opens it to be written: prepare_files()
    // and activate_prepared() in one.
    [[nodiscard]] seastar::future<runtime::result<void>>
    create_new(codec::cooperative_work& work, spare_pool* spares = nullptr) {
        return entered(
          work, [this, &work, spares](this auto) -> seastar::future<> {
              lifetime_->failure.observe(co_await make_files(work, spares));
              if (lifetime_->failure.failed() || closing_) co_return;
              co_await activate_owned(work);
          });
    }
    // Makes the segment's files only, ahead of the request that opens it:
    // its directory, descriptor, data file with its header and the window
    // of zeros, and its first pointer, all closed again. It takes no append:
    // its origin is bound when an owner made with it opens it through
    // activate_prepared(). Answers what its data file then takes on its
    // device. This owner is closed after it, whatever it answers.
    [[nodiscard]] seastar::future<runtime::result<byte_count>>
    prepare_files(codec::cooperative_work& work, spare_pool* spares = nullptr) {
        auto made = co_await entered(
          work, [this, &work, spares](this auto) -> seastar::future<> {
              lifetime_->failure.observe(co_await make_files(work, spares));
          });
        if (!made) co_return runtime::failure(made.error());
        co_return byte_count{std::max(prezeroed_end_, data_start_.value())};
    }
    // What of its data file a spare held when it was made: space its
    // device had given before. Zero for a new file.
    [[nodiscard]] byte_count spare_bytes() const noexcept {
        assert_current();
        return spare_bytes_;
    }
    // Opens a segment whose files prepare_files() made, to be written from
    // the origin this owner was made with: the files are read and nothing is
    // written or synced. Its data file reaches past its header by what is
    // written there already, zeros or a spare's old bytes, and synchronized
    // writes are admitted below that.
    [[nodiscard]] seastar::future<runtime::result<void>>
    activate_prepared(codec::cooperative_work& work) {
        return entered(work, [this, &work](this auto) -> seastar::future<> {
            co_await activate_owned(work);
        });
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
          expected.logical_origin,
          budget,
          config,
          false);
        if (!made) co_return runtime::failure(made.error());
        auto output = std::move(*made);
        output->entered_ = true;
        output->recovered_ = true;
        output->immutable_ = immutable;
        // The metadata work of opening it.
        auto metadata = budget.try_reserve(
          byte_count{
            config.metadata.operation_bytes.value()
            + config.metadata.execution_bytes.value()});
        if (!metadata)
            output->lifetime_->failure.observe(metadata.error());
        else
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
                      && expected.publication.state
                           != local_object_state::sealed)
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
      model::range_logical_end logical_origin,
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
          logical_origin,
          budget,
          config,
          false);
        if (!made) co_return runtime::failure(made.error());
        auto output = std::move(*made);
        output->entered_ = true;
        output->recovered_ = true;
        output->recovered_seal_ = true;
        // The metadata work of opening it.
        auto metadata = budget.try_reserve(
          byte_count{
            config.metadata.operation_bytes.value()
            + config.metadata.execution_bytes.value()});
        if (!metadata)
            output->lifetime_->failure.observe(metadata.error());
        else
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
    // What is left of it before its next group, for whoever decides when
    // its successor is made. Nothing is left of a segment a restart found,
    // which has no age either.
    [[nodiscard]] runtime::result<segment_left> left() {
        assert_current();
        if (
          closing_ || closed_ || lifetime_->failure.failed()
          || append_ != model::append_state::active)
            return runtime::failure(detail::path_error(errc::closed));
        segment_left output;
        if (recovered_ || !positions_) return output;
        if (first_acceptance_) {
            output.deadline = first_acceptance_->checked_add(
              descriptor_.maximum_lifetime);
            if (!output.deadline)
                return runtime::failure(detail::path_error(errc::out_of_range));
        }
        auto fixed = capacity();
        if (!fixed) return runtime::failure(fixed.error());
        const auto& reserved = positions_->reserved;
        const auto& limits = (*fixed)->limits;
        const auto facts = std::min<std::uint64_t>(
          limits.maximum_retry_entries,
          config_.policy.config().max_object_entries.value());
        // As its next group's plan counts them: the footer after its blocks,
        // padded to the boundary, and the root of a seal holding one entry
        // more while it can take one, which begins on that boundary. A
        // segment whose seal takes no more entries still takes batches of
        // the streams it holds the depth of.
        const auto entries = std::uint64_t{reserved.retry_entries};
        const auto root = (*fixed)->sealed_root(
          entries < facts ? entries + 1 : entries);
        if (
          !root || !(*fixed)->footer_bytes
          || reserved.footers == std::numeric_limits<std::uint32_t>::max()
          || reserved.blocks >= limits.maximum_blocks
          || *root >= descriptor_.maximum_data_bytes)
            return output;
        const auto alignment = descriptor_.alignment.bytes().value();
        const auto room = (descriptor_.maximum_data_bytes.value()
                           - root->value())
                          / alignment * alignment;
        // A run held open is closed by its own footer first, which pads to
        // the boundary after it.
        auto start = std::optional{reserved.bytes};
        if (run_) {
            const auto unit = descriptor_.alignment.bytes().value();
            start = reserved.bytes.checked_add(
              byte_count{(*fixed)->footer_bytes->value() + unit - 1});
            if (start)
                start = runtime::file_position{start->value() / unit * unit};
        }
        const auto taken = start ? start->checked_add(*(*fixed)->footer_bytes)
                                 : std::nullopt;
        if (!taken || taken->value() >= room) return output;
        output.bytes = byte_count{room - taken->value()};
        output.blocks = limits.maximum_blocks - reserved.blocks;
        output.retry_entries = static_cast<std::uint32_t>(
          entries < facts ? facts - entries : 0);
        return output;
    }
    // What an idle writable segment's owner holds of its budget once it is
    // opened to be written: its own instance, a path and its two queue
    // chunks, its handles' credits, its lifetime token, and its data file's
    // completion reserve, each a reservation of its own. A group, a seal and a
    // metadata operation reserve what they take for themselves.
    struct idle_cost final {
        byte_count bytes;
        std::uint32_t reservations{0};
    };
    [[nodiscard]] static runtime::result<idle_cost> idle_charge(
      const workload_budget& budget, const segment_writer_config& config) {
        const auto charge = [&budget](std::size_t bytes) {
            return budget.allocation_charge(byte_count{bytes});
        };
        const auto instance = charge(sizeof(segment_writer));
        const auto path = charge(runtime::maximum_file_path_bytes + 1);
        const auto chunk = charge(queue_chunk_bytes);
        const auto token = charge(
          sizeof(segment_captured_boundary::lifetime) + 64);
        const auto scratch = charge(config.completion.scratch_bytes.value());
        if (!instance) return runtime::failure(instance.error());
        if (!path) return runtime::failure(path.error());
        if (!chunk) return runtime::failure(chunk.error());
        if (!token) return runtime::failure(token.error());
        if (!scratch) return runtime::failure(scratch.error());
        byte_count total;
        for (const auto held :
             {byte_count{
                instance->value() + path->value() + 2 * chunk->value()},
              byte_count{},
              *token,
              byte_count{
                scratch->value()
                + config.completion.execution_bytes.value()}}) {
            const auto reserved = budget.reservation_charge(held);
            if (!reserved) return runtime::failure(reserved.error());
            const auto sum = total.checked_add(*reserved);
            if (!sum)
                return runtime::failure(detail::path_error(errc::out_of_range));
            total = *sum;
        }
        return idle_cost{total, 4};
    }
    // The most a segment made with this descriptor and configuration may
    // come to hold on its device; see segment_disk_bound.
    [[nodiscard]] static runtime::result<byte_count> disk_bound(
      const local_segment_descriptor& descriptor,
      const segment_writer_config& config) {
        auto limits = config.admission;
        limits.metadata_bytes = std::min(
          limits.metadata_bytes, config.metadata.metadata_bytes);
        return segment_disk_bound(
          descriptor,
          limits,
          config.policy,
          config.metadata.charge,
          config.metadata.operation_bytes);
    }
    // What this segment may still come to write on its device: its bound
    // less what its data file already takes there, written or zero-written.
    // Nothing once it is sealed, closed or failed, since it writes no more,
    // and nothing for a segment a restart found.
    [[nodiscard]] byte_count owed() const noexcept {
        assert_current();
        if (
          closed_ || recovered_ || !positions_ || lifetime_->failure.failed()
          || append_ == model::append_state::sealed)
            return {};
        const auto bound = disk_bound(descriptor_, config_);
        if (!bound) return {};
        const auto held = std::max(
          positions_->written.bytes.value(), prezeroed_end_);
        return byte_count{bound->value() > held ? bound->value() - held : 0};
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
    // The data device this segment was opened on.
    [[nodiscard]] device_store_id device() const noexcept {
        assert_current();
        return spec_.owner.device();
    }
    // The budget that funds this owner.
    [[nodiscard]] const workload_budget& budget() const noexcept {
        assert_current();
        return budget_;
    }
    // How many completed facts its seal keeps of each stream.
    [[nodiscard]] std::uint8_t retry_depth() const noexcept {
        assert_current();
        return descriptor_.retry_depth;
    }
    // What it was made with: its descriptor, header, origin and
    // configuration.
    [[nodiscard]] const local_segment_descriptor& descriptor() const& noexcept {
        assert_current();
        return descriptor_;
    }
    const local_segment_descriptor& descriptor() const&& = delete;
    [[nodiscard]] const segment_header& header() const& noexcept {
        assert_current();
        return header_;
    }
    const segment_header& header() const&& = delete;
    [[nodiscard]] model::range_logical_end logical_origin() const noexcept {
        assert_current();
        return logical_origin_;
    }
    [[nodiscard]] const segment_writer_config& config() const& noexcept {
        assert_current();
        return config_;
    }
    const segment_writer_config& config() const&& = delete;
    // Refuses a request because the segment holds as many streams as its
    // retry depth allows, which its writer cannot see: whoever keeps their
    // facts can. Its owner is told, as for a group its plan refuses.
    [[nodiscard]] segment_group_preparation refuse_streams() {
        assert_current();
        return rolls(segment_roll_reasons{segment_roll_reason::retry_state});
    }
    // Reports this segment's blocks to `observer` from now on. It is bound
    // once and before the first group freezes, so that no block is missed; a
    // recovered segment is never written and has none to report. Whatever
    // the observer refers to outlives this owner's joined close.
    [[nodiscard]] runtime::result<void>
    observe_blocks(segment_block_observer observer) {
        assert_current();
        if (closing_ || closed_)
            return runtime::failure(detail::path_error(errc::closed));
        // Whoever takes the bytes says when it wants them.
        if (
          !observer.blocks || !observer.durable
          || (observer.wanted != nullptr) != static_cast<bool>(observer.fed))
            return runtime::failure(detail::path_error(errc::invalid_argument));
        if (observer_ || recovered_ || first_acceptance_)
            return runtime::failure(detail::path_error(errc::wrong_context));
        observer_.emplace(std::move(observer));
        return {};
    }
    // Tells `observer` when this segment's lifetime begins to run, and each
    // time a request is refused as roll-required, with why. It is bound
    // once and before the first group freezes, so that the lifetime's
    // beginning is not missed. Whatever the observer refers to outlives
    // this owner's joined close.
    [[nodiscard]] runtime::result<void>
    observe_roll(segment_roll_observer observer) {
        assert_current();
        if (closing_ || closed_)
            return runtime::failure(detail::path_error(errc::closed));
        if (!observer.aging || !observer.refused)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        if (roll_observer_ || first_acceptance_)
            return runtime::failure(detail::path_error(errc::wrong_context));
        roll_observer_.emplace(std::move(observer));
        return {};
    }
    [[nodiscard]] runtime::result<segment_captured_boundary> capture() const {
        assert_current();
        if (
          !positions_ || append_ != model::append_state::active || recovered_
          || lifetime_->failure.failed() || closing_ || closed_)
            return runtime::failure(detail::path_error(errc::closed));
        return reserved_cut();
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
    // future working metadata; ordinary queue pressure stays a typed
    // resource error, distinct from impossible or roll-required geometry.
    // Input ownership stays with the caller. Freezing must consume and validate
    // those children; this preparation alone does not authorize execution.
    // `retry_entries` is how many completed-retry entries the group adds to
    // the seal, which whoever keeps its streams' facts knows: a batch adds
    // one while its stream holds fewer than the retry depth. Absent, each
    // batch adds one.
    [[nodiscard]] runtime::result<segment_group_preparation> prepare_group(
      std::span<const encoded_assigned_batch> batches,
      codec::cooperative_work& work,
      std::optional<std::uint32_t> retry_entries = std::nullopt) {
        assert_current();
        if (batches.size() > maximum_segment_group_blocks)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        std::array<const encoded_assigned_batch*, maximum_segment_group_blocks>
          input{};
        for (std::size_t i = 0; i < batches.size(); ++i)
            input[i] = &batches[i];
        return prepare_group(
          std::span{input}.first(batches.size()), work, retry_entries);
    }
    // The same preparation for children owned elsewhere, e.g. by separate
    // requests. Each pointer is borrowed for this call only.
    [[nodiscard]] runtime::result<segment_group_preparation> prepare_group(
      std::span<const encoded_assigned_batch* const> batches,
      codec::cooperative_work& work,
      std::optional<std::uint32_t> retry_entries = std::nullopt) {
        assert_current();
        if (
          lifetime_->failure.failed() || closing_ || closed_
          || append_ != model::append_state::active)
            return runtime::failure(detail::path_error(errc::closed));
        if (batches.empty() || batches.size() > maximum_segment_group_blocks)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        if (recovered_)
            return rolls(segment_roll_reasons{segment_roll_reason::recovered});
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
        const auto entries = retry_entries.value_or(
          static_cast<std::uint32_t>(batches.size()));
        auto plan = detail::plan_segment_capacity(
          **fixed, positions_->reserved, batches, run_.has_value(), entries);
        if (!plan) return runtime::failure(plan.error());
        if (plan->decision == segment_capacity_decision::roll_required)
            return rolls(plan->reasons);
        if (plan->decision != segment_capacity_decision::fits)
            return segment_group_preparation{plan->decision, {}, {}};
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
                  segment_capacity_decision::impossible, {}, {}};
            metadata = metadata.checked_add(*cost).value();
        }
        // What the runtime may stage to write the group, and its footer.
        const auto staging = write_staging(
          plan->end->bytes.value() - positions_->reserved.bytes.value());
        if (!staging)
            return segment_group_preparation{
              segment_capacity_decision::impossible, {}, {}};
        auto required = metadata.checked_add(*staging);
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
              segment_capacity_decision::impossible, {}, {}};
        if (!group_fits_budget(*required, *control))
            return segment_group_preparation{
              segment_capacity_decision::impossible, {}, {}};
        auto aged = age_expired(Clock::now());
        if (!aged) return runtime::failure(aged.error());
        if (*aged) return rolls(segment_roll_reasons{segment_roll_reason::age});
        auto held = budget_.try_reserve(*required);
        if (!held) {
            if (held.error().code() == errc::out_of_range)
                return segment_group_preparation{
                  segment_capacity_decision::impossible, {}, {}};
            return runtime::failure(held.error());
        }
        auto node = budget_.try_reserve(*control);
        if (!node) return runtime::failure(node.error());
        // Whoever keeps its blocks admits what they will take: an index
        // refuses the group, never an anchor, when the budget has no room.
        if (observer_ && observer_->admit)
            if (
              auto admitted = observer_->admit(
                static_cast<std::uint32_t>(batches.size()));
              !admitted)
                return runtime::failure(admitted.error());
        if (auto ready = work.poll(); !ready)
            return runtime::failure(detail::path_error(ready.error().code()));
        std::vector<detail::segment_plan_input> inputs;
        inputs.reserve(batches.size());
        for (const auto* batch : batches)
            inputs.push_back(detail::plan_input(*batch));
        auto base = capture().value();
        // Over a run held open, the plan counts the footer that closes it;
        // whether this group or close_run() freezes that footer, the group
        // then freezes from where it ends.
        std::optional<segment_captured_boundary> closed;
        if (run_) {
            auto end = base.end();
            const auto footer = model::file_byte_span::make(
              end.bytes, end.bytes.checked_add(plan->closing).value());
            end.bytes = footer->end();
            ++end.footers;
            closed.emplace(
              segment_captured_boundary{
                lifetime_, base.history(), end, *footer});
        }
        return segment_group_preparation{
          segment_capacity_decision::fits,
          segment_prepared_group{
            std::move(base),
            std::move(closed),
            std::move(*plan),
            std::move(inputs),
            std::move(*held),
            std::move(*node),
            retained,
            work.policy(),
            entries},
          {}};
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
          (prepared.base_ != *before && prepared.closed_ != *before)
          || prepared.policy_ != config_.policy || !prepared.plan_.end)
            return runtime::failure(detail::path_error(errc::wrong_context));
        if (
          freezing_ || inflight_.size() >= config_.maximum_groups
          || digesting_.size() >= runtime::maximum_queued_file_writes)
            return runtime::failure(detail::path_error(errc::queue_full));
        const auto pending = pending_bytes_.checked_add(
          prepared.extent().size());
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
    //
    // `hold` is how far the group may be held: zero writes it as soon as it
    // is assembled, closed by its own footer, and its durability is wanted
    // now. Otherwise it joins the run held open at the reserved end, the
    // blocks frozen since the last footer, and is frozen with a footer of
    // its own only once that run reaches `hold` bytes with it. A held group
    // is never written alone: at assembly its bytes are copied into its run,
    // everything else of it is let go and it completes with nothing written,
    // and the footer that closes the run writes the run with it. A run is
    // closed by its own footer before a group that wants durability now, or
    // that would take it past one write, and by close_run().
    [[nodiscard]] seastar::future<runtime::result<segment_frozen_group>>
    freeze_group(
      segment_prepared_group&& offered,
      std::vector<admitted_wal_batch>&& offered_children,
      codec::cooperative_work& work,
      byte_count hold = {}) {
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
                  co_await freeze_owned(prepared, children, owned, hold, work));
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

    // Bytes of the run held open at the reserved end: frozen blocks that no
    // footer closes yet, none of them written.
    [[nodiscard]] byte_count held() const noexcept {
        assert_current();
        return run_
                 ? byte_count{positions_->reserved.bytes.value() - run_->begin.value()}
                 : byte_count{};
    }

    // Closes the run held open at the reserved end with a footer of its own:
    // a group of no blocks, frozen and authorized at once and paid from the
    // allowance the run reserved when it opened, so no admission is needed.
    // `wanted`: the run is closed for a barrier, so its write may go through
    // the synchronized handle. Returns its cut, which a barrier may take
    // once the groups before it are submitted. With no run open, nothing
    // changes and the reserved end's cut is returned. Refused while a group
    // is freezing. A run whose closing group cannot be made is never
    // written here, and the owner fails with it.
    [[nodiscard]] runtime::result<segment_captured_boundary>
    close_run(bool wanted = true) {
        assert_current();
        auto before = capture();
        if (!before) return runtime::failure(before.error());
        if (!run_) return before;
        if (freezing_)
            return runtime::failure(detail::path_error(errc::queue_full));
        try {
            auto closing = make_closing(*before, wanted);
            if (!closing) return unclosed(closing.error());
            inflight_.reserve(inflight_.size() + 1);
            link_closing(*closing);
            lifetime_->changed.broadcast();
            return closing->cut;
        } catch (...) {
            return unclosed(detail::recognized_or_rethrown());
        }
    }

    // Closes the run held open, if there is one, and resolves once its write
    // completed, or the owner failed. A barrier on the cut it closed then
    // makes the run durable; this only writes it.
    [[nodiscard]] seastar::future<runtime::result<void>> materialize() {
        assert_current();
        auto cut = close_run(false);
        if (!cut) co_return runtime::failure(cut.error());
        auto holder = operations_.try_hold();
        if (!holder)
            co_return runtime::failure(detail::path_error(errc::closed));
        while (!lifetime_->failure.failed()
               && positions_->written.bytes < cut->end().bytes)
            co_await lifetime_->changed.when();
        co_return lifetime_->failure.outcome();
    }

private:
    // A run that cannot be closed is never written here: what it holds is
    // durable in the WAL alone. The owner fails with it, so that whoever
    // appends settles what it accepted, and a seal writes the run from the
    // WAL.
    runtime::result<segment_captured_boundary>
    unclosed(runtime::operation_error why) noexcept {
        lifetime_->failure.observe(why);
        lifetime_->changed.broadcast();
        return runtime::failure(why);
    }
    // The cut at the reserved end, whatever the owner's state.
    [[nodiscard]] segment_captured_boundary reserved_cut() const {
        return segment_captured_boundary{
          lifetime_,
          {descriptor_.segment,
           descriptor_.alignment,
           data_start_,
           logical_origin_,
           descriptor_.physical_origin,
           descriptor_.profile},
          positions_->reserved,
          reserved_footer_};
    }
    // A group of no blocks whose footer closes the run held open at
    // `before`, made but not linked, and the cut it ends at. Its allowance
    // is the run's.
    struct closing_group final {
        detail::segment_write_descriptor::pointer node;
        segment_captured_boundary cut;
    };
    runtime::result<closing_group>
    make_closing(const segment_captured_boundary& before, bool wanted) {
        auto fixed = capacity();
        if (!fixed) return runtime::failure(fixed.error());
        const auto policy = config_.policy.config();
        auto layout = aligned_envelope_layout::make(
          {byte_count{codec::envelope_prefix_bytes},
           durable_footer_fixed_bytes,
           {}},
          before.end().bytes,
          envelope_placement::ends_span,
          descriptor_.alignment,
          config_.policy,
          {policy.max_page_bytes, policy.max_page_bytes});
        if (!layout || !(*fixed)->closing_staging)
            return runtime::failure(detail::path_error(errc::out_of_range));
        const auto footer = layout->at(before.end().bytes);
        if (!footer)
            return runtime::failure(detail::path_error(errc::out_of_range));
        auto end = before.end();
        end.bytes = footer->end();
        ++end.footers;
        const segment_captured_boundary cut{
          lifetime_, before.history(), end, *footer};
        const auto credit = budget_.allocation_charge(
          byte_count{3 * bytes::fragmented_buffer::fragment_descriptor_size()});
        if (!credit) return runtime::failure(credit.error());
        auto& paid = run_->memory;
        auto node = seastar::make_lw_shared<detail::segment_write_descriptor>(
          paid.share(),
          paid.share(),
          lifetime_,
          before,
          segment_group_layout{
            cut, {}, positions_->durable.bytes, paid.share()},
          std::vector<admitted_wal_batch::contents>{},
          config_.policy);
        node->retained_bound = *(*fixed)->closing_staging;
        node->gather_credit = *credit;
        node->state = detail::segment_write_state::authorized;
        node->observed = true;
        node->wanted = wanted;
        return closing_group{std::move(node), cut};
    }
    // Links a closing group made at the reserved end, whose queue room is
    // reserved: nothing here allocates or fails.
    void link_closing(closing_group& closing) noexcept {
        inflight_.push_back(closing.node);
        closing.node->linked = true;
        pending_bytes_ = byte_count{
          pending_bytes_.value() + closing.node->extent().size().value()};
        pending_retained_ = byte_count{
          pending_retained_.value() + closing.node->retained_bound.value()};
        positions_->reserved = closing.cut.end();
        reserved_footer_ = closing.cut.footer();
        run_.reset();
    }

    seastar::future<runtime::result<segment_frozen_group>> freeze_owned(
      segment_prepared_group& prepared,
      std::vector<admitted_wal_batch>& children,
      std::vector<admitted_wal_batch::contents>& owned,
      byte_count hold,
      codec::cooperative_work& work) {
        auto before = capture();
        if (!before) co_return runtime::failure(before.error());
        if (
          freezing_ || inflight_.size() >= config_.maximum_groups
          || digesting_.size() >= runtime::maximum_queued_file_writes)
            co_return runtime::failure(detail::path_error(errc::queue_full));
        if (hold > maximum_segment_hold_bytes)
            co_return runtime::failure(
              detail::path_error(errc::invalid_argument));
        if (
          children.empty() || children.size() > maximum_segment_group_blocks
          || children.capacity() > maximum_segment_group_blocks
          || children.capacity() > 2 * children.size()
          || (prepared.base_ != *before && prepared.closed_ != *before)
          || prepared.policy_ != work.policy() || !budget_.owns(prepared.held_)
          || !prepared.held_.exclusive() || !budget_.owns(prepared.control_)
          || !prepared.control_.exclusive())
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        freezing_ = true;
        auto idle = seastar::defer([this] noexcept { freezing_ = false; });
        owned.reserve(children.size());
        for (auto& child : children)
            owned.push_back(std::move(child).release());
        // Append times never go back along the segment: none is below its
        // floor, the newest of its accepted groups' or an earlier member's.
        auto newest = append_floor_;
        for (const auto& member : owned) {
            if (member.append_time < newest)
                co_return runtime::failure(
                  detail::path_error(errc::invalid_argument));
            newest = member.append_time;
        }
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
        // The base is unchanged, or a run held open there was closed since
        // (checked above). Children equal to the prepared inputs plan
        // exactly as prepared from that base; only others plan again.
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
              **fixed,
              prepared.base_.end(),
              std::span{inputs}.first(owned.size()),
              prepared.closed_.has_value(),
              prepared.retry_entries_);
            if (!replanned) co_return runtime::failure(replanned.error());
            plan = *replanned;
        }
        if (
          plan->decision != segment_capacity_decision::fits
          || plan->end != prepared.plan_.end
          || plan->staging_bytes > prepared.plan_.staging_bytes)
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        const auto policy = work.policy().config();
        // A block placed at `at`. Blocks never pad, so their sizes do not
        // depend on where they lie: each is sized at the base, then placed
        // where it lands.
        const auto block_at = [&](std::size_t i, runtime::file_position at) {
            return aligned_envelope_layout::make(
              {byte_count{codec::envelope_prefix_bytes},
               segment_block_fixed_bytes,
               owned[i].batch.bytes().size()},
              at,
              envelope_placement::continues,
              descriptor_.alignment,
              work.policy(),
              {policy.max_encoded_body_bytes,
               byte_count{
                 policy.max_encoded_body_bytes.value()
                 + policy.max_header_bytes.value()}});
        };
        std::array<
          std::optional<aligned_envelope_layout>,
          maximum_segment_group_blocks>
          geometry{};
        std::uint64_t own_bytes = 0;
        std::size_t fragments = 3; // complete footer prefix and padding
        byte_count retained = plan->staging_bytes;
        for (std::size_t i = 0; i < owned.size(); ++i) {
            const auto& batch = owned[i].batch;
            geometry[i] = block_at(i, before->end().bytes).value();
            own_bytes += geometry[i]->encoded_bytes().value();
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
        // Whether the run held open here is closed first, and whether this
        // group ends with a footer. The run's copies lie in chunks, which
        // its write carries before this group's bytes.
        auto fixed = capacity();
        if (!fixed) co_return runtime::failure(fixed.error());
        const auto unit = descriptor_.alignment.bytes().value();
        const auto footer_bound
          = (*fixed)->footer_bytes.value_or(byte_count{}).value() + unit;
        bool close_first = false;
        bool footed = hold.value() == 0;
        if (run_) {
            const auto run_bytes = before->end().bytes.value()
                                   - run_->begin.value();
            close_first = footed
                          || run_bytes + own_bytes + footer_bound
                               > runtime::maximum_file_io_bytes.value()
                          || run_bytes / held_chunk_bytes + 1 + fragments
                               > bytes::max_buffer_fragments;
        }
        const auto held_before = run_ && !close_first
                                   ? before->end().bytes.value()
                                       - run_->begin.value()
                                   : std::uint64_t{0};
        if (!footed) footed = held_before + own_bytes >= hold.value();
        // A run that opens here reserves at once what it will hold and the
        // footer that closes it, so neither waits for admission later;
        // without that room the group is not held.
        std::optional<workload_reservation> opened;
        if (!footed && (!run_ || close_first)) {
            const auto charge = run_charge(hold);
            if (charge) {
                auto taken = budget_.try_reserve(*charge);
                if (taken) opened.emplace(std::move(*taken));
            }
            if (!opened) footed = true;
        }
        std::optional<closing_group> closer;
        if (close_first) {
            auto made = make_closing(*before, hold.value() == 0);
            if (!made) co_return runtime::failure(made.error());
            closer.emplace(std::move(*made));
        }
        const auto& start = closer ? closer->cut : *before;
        std::vector<segment_block_layout> blocks;
        blocks.reserve(owned.size());
        auto position = start.end().bytes;
        auto physical = start.end().physical;
        for (std::size_t i = 0; i < owned.size(); ++i) {
            const auto& batch = owned[i].batch;
            const auto records
              = model::segment_relative_span::from_count(
                  physical,
                  model::segment_record_count{
                    batch.info().context.retained_count().value()})
                  .value();
            geometry[i] = block_at(i, position).value();
            const auto bytes = geometry[i]->at(position).value();
            blocks.push_back(
              segment_block_layout{
                storage::coverage{
                  batch.info().context.logical_span(), records, bytes},
                *geometry[i]});
            position = bytes.end();
            physical = records.end();
        }
        std::optional<model::file_byte_span> footer;
        if (footed) {
            auto padded = aligned_envelope_layout::make(
              {byte_count{codec::envelope_prefix_bytes},
               durable_footer_fixed_bytes,
               {}},
              position,
              envelope_placement::ends_span,
              descriptor_.alignment,
              work.policy(),
              {policy.max_page_bytes, policy.max_page_bytes});
            if (!padded)
                co_return runtime::failure(
                  detail::path_error(errc::wrong_context));
            footer = padded->at(position).value();
            position = footer->end();
        }
        const segment_writer_position end{
          plan->end->logical,
          physical,
          position,
          start.end().blocks + static_cast<std::uint32_t>(owned.size()),
          start.end().footers + (footed ? 1U : 0U),
          start.end().retry_entries + prepared.retry_entries_};
        // Whichever way it froze, it takes no more than its plan counted.
        if (
          end.physical != plan->end->physical || end.bytes > plan->end->bytes
          || end.footers > plan->end->footers || end.blocks != plan->end->blocks
          || end.retry_entries != plan->end->retry_entries)
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        const auto extent
          = model::file_byte_span::make(before->end().bytes, end.bytes).value();
        const auto pending = pending_bytes_.checked_add(extent.size());
        if (!pending || *pending > config_.maximum_pending_bytes)
            co_return runtime::failure(detail::path_error(errc::queue_full));
        const auto closing_retained = closer ? closer->node->retained_bound
                                             : byte_count{};
        const auto pending_retained = pending_retained_.checked_add(
          retained.checked_add(closing_retained).value());
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
        const segment_captured_boundary cut{
          lifetime_, start.history(), end, footer};
        segment_group_layout layout{
          cut,
          std::move(blocks),
          positions_->durable.bytes,
          prepared.held_.share()};
        auto node = seastar::make_lw_shared<detail::segment_write_descriptor>(
          std::move(*held),
          std::move(prepared.held_),
          lifetime_,
          start,
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
        node->wanted = hold.value() == 0;
        node->held = !footed;
        if (closer)
            node->closed_run = hold.value() == 0 ? segment_run_close::wanted
                                                 : segment_run_close::limited;
        else if (run_ && footed)
            node->closed_run = segment_run_close::filled;
        if (!footed) {
            node->run_memory.emplace(
              opened ? opened->share() : run_->memory.share());
            node->run_chunks = static_cast<std::uint32_t>(held_chunks(hold));
        }
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
        inflight_.reserve(inflight_.size() + (closer ? 2 : 1));
        if (closer) link_closing(*closer);
        inflight_.push_back(node);
        if (!first_acceptance_) {
            first_acceptance_ = now;
            // Checked above: its deadline is representable.
            if (
              const auto ends = now.checked_add(descriptor_.maximum_lifetime);
              ends && roll_observer_)
                roll_observer_->aging(*ends);
        }
        node->linked = true;
        positions_->reserved = end;
        append_floor_ = newest;
        reserved_footer_ = footer;
        pending_bytes_ = byte_count{
          pending_bytes_.value() + node->extent().size().value()};
        pending_retained_ = byte_count{
          pending_retained_.value() + retained.value()};
        retry_page_entries_ = plan->retry_page_entries;
        if (footed)
            run_.reset();
        else if (opened)
            run_.emplace(open_run{node->extent().begin(), std::move(*opened)});
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
                      {child.append_time, node->layout.certified_end()},
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

    // The append owner published everything before `cut` as durable in at
    // least one place, its WAL included: readers may be given that much
    // while it is held. It never moves back; a cut not of this owner, or
    // past what was reserved, is refused.
    [[nodiscard]] runtime::result<void>
    report(const segment_captured_boundary& cut) {
        assert_current();
        if (auto valid = validate_capture(cut); !valid) return valid;
        if (cut.end().bytes <= positions_->reported.bytes) return {};
        positions_->reported = cut.end();
        tell_reported();
        return {};
    }

    // The bytes from `position` to `end` while they are only in this
    // owner's memory: of groups made and not written yet, held ones
    // included. As shares, which keep them where they are for as long as
    // whoever takes them lives, however this owner moves on. Refused with
    // out_of_range when any of them is not held here, because it is in the
    // file by now or was not made yet, and with closed once this owner
    // failed or is closing.
    [[nodiscard]] runtime::result<bytes::fragmented_buffer>
    held_bytes(runtime::file_position position, runtime::file_position end) {
        assert_current();
        if (closing_ || closed_ || !positions_ || lifetime_->failure.failed())
            return runtime::failure(detail::path_error(errc::closed));
        if (
          end <= position || position < positions_->written.bytes
          || made_ < end)
            return runtime::failure(detail::path_error(errc::out_of_range));
        try {
            bytes::fragmented_buffer_builder joined{
              {.initial_fragment_bytes = byte_count{1},
               .max_fragment_bytes = byte_count{1},
               .max_total_bytes = byte_count{end.value() - position.value()},
               .max_retained_bytes = bytes::max_builder_total_bytes}};
            // Groups lie in file order from where the file's bytes end: a
            // queued one holds its bytes, one in flight a share for its
            // digest, and a held one's are with its run.
            auto at = position;
            const auto take = [&](
                                model::file_byte_span span,
                                auto& bytes) -> runtime::result<void> {
                if (span.end() <= at || at == end) return {};
                if (span.begin() > at)
                    return runtime::failure(
                      detail::path_error(errc::out_of_range));
                const auto upto = std::min(end, span.end());
                auto shared = bytes.share(
                  byte_count{at.value() - span.begin().value()},
                  byte_count{upto.value() - at.value()});
                if (!shared)
                    return runtime::failure(
                      detail::path_error(errc::resource_exhausted));
                if (!joined.append_buffer(std::move(*shared)))
                    return runtime::failure(
                      detail::path_error(errc::resource_exhausted));
                at = upto;
                return {};
            };
            for (const auto& node : inflight_) {
                auto& bytes = node->payload.empty() ? node->digest
                                                    : node->payload;
                if (bytes.empty()) continue;
                if (auto taken = take(node->extent(), bytes); !taken)
                    return runtime::failure(taken.error());
            }
            if (copies_) {
                const auto span = model::file_byte_span::make(
                                    copies_->begin,
                                    runtime::file_position{
                                      copies_->begin.value()
                                      + copies_->bytes.size().value()})
                                    .value();
                if (auto taken = take(span, copies_->bytes); !taken)
                    return runtime::failure(taken.error());
            }
            if (at != end)
                return runtime::failure(detail::path_error(errc::out_of_range));
            auto made = joined.finish();
            if (!made)
                return runtime::failure(
                  detail::path_error(errc::resource_exhausted));
            return std::move(*made);
        } catch (const std::bad_alloc&) {
            return runtime::failure(
              detail::path_error(errc::resource_exhausted));
        }
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

    // Names an index in a sealed segment's publication after its seal: for a
    // segment sealed without one, or one whose index was built again. The
    // source publishes its bundle as it does for a seal, under the sealed
    // coverage and digest, and only a durable bundle is named; the pointer
    // is then replaced once, an earlier index root giving way to this one.
    //
    // Reads go on meanwhile and keep the handles they use: the new
    // publication adds a root or replaces the index root, the data and every
    // other root stay as they are, and a later reopen finds the new
    // publication. Only a read that would have to reopen is asked to come
    // back. A refusal or a failed bundle changes nothing; after an uncertain
    // pointer effect this owner fails, as it does for its other publications.
    //
    // The bundle an earlier publication named is removed once this one is
    // durable, and this one when the pointer was not touched; a reader that
    // still has either open reads on to its end. A bundle whose removal
    // fails, or that was written and could not be named for another reason,
    // is named by nothing and is left for the next open's reconciliation.
    // The source's sequence is one nothing was given: under the sequence of
    // the root being replaced the bundle just named would be the one
    // removed, so that one is kept.
    template<typename Index>
    [[nodiscard]] seastar::future<runtime::result<local_root_reference>>
    publish_index(Index index, codec::cooperative_work& work) {
        static_assert(sizeof(Index) <= 8_KiB);
        assert_current();
        if (
          closing_ || closed_ || lifetime_->failure.failed() || !immutable_
          || !publication_ || !read_publication_
          || append_ != model::append_state::sealed)
            co_return runtime::failure(detail::path_error(errc::closed));
        if (publishing_ || read_busy_)
            co_return runtime::failure(detail::path_error(errc::queue_full));
        if (
          work.policy() != config_.policy
          || immutable_->coverage.physical().empty()
          || read_roots_.size() != read_publication_->roots.size())
            co_return runtime::failure(
              detail::path_error(errc::invalid_argument));
        if (publication_->value() == UINT64_MAX)
            co_return runtime::failure(detail::path_error(errc::out_of_range));
        const auto next = local_publication_generation::make(
          publication_->value() + 1);
        const auto context = index_scope(
          immutable_->coverage, immutable_->digest);
        if (!next || !context)
            co_return runtime::failure(detail::path_error(errc::out_of_range));
        if (auto ready = work.poll(); !ready)
            co_return runtime::failure(
              detail::path_error(ready.error().code()));
        auto held = budget_.try_reserve(
          byte_count{
            config_.metadata.operation_bytes.value()
            + config_.metadata.execution_bytes.value()});
        if (!held) co_return runtime::failure(held.error());
        // Joined by close; an eviction is refused meanwhile, as during a
        // read.
        auto holder = read_operations_.hold();
        publishing_ = true;
        auto idle = seastar::defer([this] noexcept { publishing_ = false; });
        // The pointer's publisher is prepared before the bundle is written,
        // as a seal's is: a pointer that cannot be admitted then costs no
        // bundle.
        pointer_publisher_.emplace(
          files_,
          budget_,
          target(
            path(local_segment_file::published),
            runtime::file_rename_policy::replace,
            publication_));
        runtime::first_failure refused;
        std::optional<local_root_reference> root;
        try {
            if (
              auto prepared = co_await pointer_publisher_->prepare(
                config_.metadata.operation_bytes, work);
              !prepared)
                refused.observe(prepared);
            else
                root = index_root(
                  co_await index.publish(
                    files_,
                    owner_,
                    spec_,
                    shard_,
                    *context,
                    budget_,
                    config_.metadata,
                    work),
                  refused);
        } catch (...) {
            refused.observe(std::current_exception());
        }
        if (!root) {
            co_await close_publishers();
            if (lifetime_->failure.failed()) lifetime_->changed.broadcast();
            co_return runtime::failure(
              refused.error() ? *refused.error()
                              : detail::path_error(errc::io_failure));
        }
        const auto& named = read_publication_->roots;
        const auto replaced = !named.empty()
                                  && named.front().kind()
                                       == local_root_kind::index
                                ? std::optional{named.front()}
                                : std::nullopt;
        runtime::result<void> published{};
        try {
            published = co_await publish_indexed(*root, *context, *next, work);
        } catch (...) {
            // Kept as the owner's failure; this entrance returns a result.
            lifetime_->failure.observe(std::current_exception());
            published = runtime::failure(detail::path_error(errc::io_failure));
        }
        co_await close_publishers();
        if (published) {
            // Only now that the publication replacing it is durable, and
            // never the bundle that publication names.
            if (replaced && replaced->sequence() != root->sequence())
                co_await remove_unnamed(*replaced);
        } else if (!lifetime_->failure.failed()) {
            // A pointer that was not touched leaves the new bundle named by
            // nothing, so it goes; after anything else it may be current.
            co_await remove_unnamed(*root);
        }
        if (lifetime_->failure.failed()) lifetime_->changed.broadcast();
        if (!published) co_return runtime::failure(published.error());
        co_return *root;
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

    // The segment's durable publication as this owner last made or found it:
    // its generation, the publication, the descriptor and each root's context
    // in the publication's order. A generation of the segment is opened
    // against it, and once the segment is sealed it is what covers the
    // segment's checkpoint entries and its pinned PREPAREs. Nothing for a
    // segment this owner created and has not sealed. The contexts are
    // borrowed until the next publication through this owner. A native
    // allocation failure throws.
    [[nodiscard]] std::optional<local_generation_expectation>
    published() const {
        assert_current();
        if (
          !publication_ || !read_publication_
          || read_roots_.size() != read_publication_->roots.size())
            return std::nullopt;
        return local_generation_expectation{
          *publication_,
          *read_publication_,
          descriptor_,
          logical_origin_,
          read_roots_};
    }

    // Source is moved into the joined attempt and owns an immutable summary:
    // read(first,count,work) returns exactly that slice as
    // result<vector<completed_retry>>, in batch identity order, without fresh
    // workload admission, and can replay it. Its retained facts are already
    // charged. `completed` is every fact it holds, the facts of the batches
    // its appends published: at most the entries their groups reserved.
    // `grants` are the seal slot's this seal runs in: see
    // segment_seal_grants. Without them the seal reserves its own when it
    // starts, and may be refused for want of the budget.
    template<typename Source, typename Index = segment_no_index>
    [[nodiscard]] seastar::future<segment_seal_outcome> seal(
      Source source,
      std::uint32_t completed,
      codec::cooperative_work& admission,
      Index index = {},
      segment_seal_grants* grants = nullptr) {
        static_assert(sizeof(Source) + sizeof(Index) <= 8_KiB);
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
        // The seal writes exactly `completed` facts: all the source holds.
        if (
          admission.policy() != config_.policy
          || completed > positions_->reserved.retry_entries
          || !detail::seals_whole_source(source, completed)) {
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
        try {
            co_await operations_.close();
            co_await stop_execution();
            if (!lifetime_->failure.failed()) {
                seastar::abort_source abort;
                codec::cooperative_work work{config_.policy, abort};
                segment_seal_grants own;
                auto& held = grants ? *grants : own;
                lifetime_->failure.observe(co_await take_grants(held, work));
                if (!lifetime_->failure.failed())
                    lifetime_->failure.observe(
                      co_await seastar::with_scheduling_group(
                        budget_.scheduling_group(),
                        [this, &source, completed, &index, &work] {
                            return seal_owned(source, completed, index, work);
                        }));
                co_await give_grants(held);
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
            // What the runtime may stage for this write.
            std::optional<workload_reservation> staged;
            if (!lifetime_->failure.failed()) {
                const auto staging = write_staging(length.value());
                auto held
                  = staging
                      ? budget_.try_reserve(*staging)
                      : runtime::result<workload_reservation>{runtime::failure(
                          detail::path_error(errc::invalid_argument))};
                if (!held)
                    lifetime_->failure.observe(held.error());
                else
                    staged.emplace(std::move(*held));
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
    // overlap it. Source and its count are as for seal(): the facts of every
    // batch the extent holds.
    template<typename Source, typename Index = segment_no_index>
    [[nodiscard]] seastar::future<segment_seal_outcome> seal_recovered(
      verified_extent extent,
      Source source,
      std::uint32_t completed,
      codec::cooperative_work& admission,
      Index index = {},
      segment_seal_grants* grants = nullptr) {
        static_assert(sizeof(Source) + sizeof(Index) <= 8_KiB);
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
        // The seal writes exactly `completed` facts: all the source holds.
        if (!detail::seals_whole_source(source, completed)) {
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
        try {
            seastar::abort_source abort;
            codec::cooperative_work work{config_.policy, abort};
            segment_seal_grants own;
            auto& held = grants ? *grants : own;
            // The seal's writable handle replaces every read handle first.
            co_await close_handles();
            if (!lifetime_->failure.failed())
                lifetime_->failure.observe(co_await take_grants(held, work));
            if (!lifetime_->failure.failed())
                lifetime_->failure.observe(
                  co_await seastar::with_scheduling_group(
                    budget_.scheduling_group(),
                    [this, &source, &extent, completed, &index, &work] {
                        return seal_recovered_owned(
                          source, extent, completed, index, work);
                    }));
            co_await give_grants(held);
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
        // A pointer publication in flight leaves open handles to their
        // readers; only a read that would reopen under it waits.
        if (
          read_busy_ || (publishing_ && !data_)
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

    // A reader's right to read this segment's data while it is appended to,
    // and how far it may read: everything below the durable position, as it
    // stands now. Readers share one read-only handle, apart from the handle
    // the writer writes through; it is opened when the first of them asks,
    // and takes one handle credit while it is open. A pin outlives a seal:
    // what lies below its reader's bound does not change. Close waits for
    // every pin.
    //
    // Whatever keeps the handle from opening is this request's outcome: the
    // segment is not failed by it, and the next request tries again. A
    // request that arrives while the handle is being opened is asked to come
    // back. A segment that was recovered, is sealed or is closing is read
    // through its publication instead, and is refused here.
    [[nodiscard]] seastar::future<runtime::result<segment_data_view>>
    pin_data(codec::cooperative_work& work) {
        using output = runtime::result<segment_data_view>;
        assert_current();
        const auto appended = [this] {
            return !closing_ && !closed_ && !recovered_ && !seal_started_
                   && positions_.has_value()
                   && append_ == model::append_state::active
                   && !lifetime_->failure.failed();
        };
        if (!appended())
            co_return output{
              runtime::failure(detail::path_error(errc::closed))};
        if (data_pin_busy_)
            co_return output{
              runtime::failure(detail::path_error(errc::queue_full))};
        if (auto ready = work.poll(); !ready)
            co_return output{
              runtime::failure(detail::path_error(ready.error().code()))};
        if (!readable_) {
            auto held = budget_.try_reserve(byte_count{512});
            if (!held) co_return output{runtime::failure(held.error())};
            if (auto credit = held->try_acquire_handles(1); !credit)
                co_return output{runtime::failure(credit.error())};
            auto holder = operations_.try_hold();
            if (!holder)
                co_return output{
                  runtime::failure(detail::path_error(errc::closed))};
            data_pin_busy_ = true;
            const auto idle = seastar::defer(
              [this] noexcept { data_pin_busy_ = false; });
            runtime::result<void> opened{};
            seastar::lw_shared_ptr<detail::segment_data_handle> made;
            try {
                // Made before the file is opened: once it is open nothing
                // here can fail and leave it without an owner.
                made = seastar::make_lw_shared<detail::segment_data_handle>(
                  std::move(*held));
                opened = co_await inspect_local_path(
                  files_,
                  spec_.root,
                  path(local_segment_file::data),
                  runtime::file_kind::regular,
                  work);
                if (opened) {
                    auto file = co_await files_.open(
                      path(local_segment_file::data),
                      {.access = runtime::file_access::read_only});
                    if (file)
                        made->file.emplace(std::move(*file));
                    else
                        opened = runtime::failure(file.error());
                }
            } catch (...) {
                opened = runtime::failure(detail::recognized_or_rethrown());
            }
            if (!opened) co_return output{runtime::failure(opened.error())};
            // Kept even when the segment moved on meanwhile: its close
            // closes the handle.
            readable_ = std::move(made);
        }
        if (!appended())
            co_return output{
              runtime::failure(detail::path_error(errc::closed))};
        co_return output{segment_data_view{
          readable_->pin(),
          {descriptor_.segment,
           descriptor_.alignment,
           data_start_,
           logical_origin_,
           descriptor_.physical_origin,
           descriptor_.profile},
          positions_->durable.bytes,
          positions_->written.bytes,
          std::min(positions_->reported.bytes, made_)}};
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
        if (read_busy_ || (publishing_ && !data_))
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

    // Lets go of what a failed owner writes with, once whoever submitted to
    // it has settled: its execution stops, and its writable handles, their
    // completion reserve, its held run's copies and its zero chunk go.
    // Readers keep the handle they share and read below the bound each took.
    // An owner whose own writes have not failed takes `why`, the failure its
    // table holds, as for a device reported failed under it: it lets go of
    // them all the same. Nothing again.
    [[nodiscard]] seastar::future<>
    release_writing(const runtime::first_failure& why) {
        assert_current();
        if (released_ || closing_ || closed_) co_return;
        if (!lifetime_->failure.failed()) {
            lifetime_->failure = why;
            if (!lifetime_->failure.failed()) co_return;
            lifetime_->changed.broadcast();
        }
        released_ = true;
        if (!execution_stopped_) co_await operations_.close();
        co_await stop_execution();
        zero_ = {};
        copies_.reset();
        run_.reset();
        co_await close_handles();
        credits_.reset();
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
        // A run held open is written with a footer of its own before the
        // queue stops; nothing is freezing any more.
        if (
          run_ && !execution_stopped_ && positions_
          && !lifetime_->failure.failed())
            try {
                auto closing = make_closing(reserved_cut(), true);
                if (closing) {
                    inflight_.reserve(inflight_.size() + 1);
                    link_closing(*closing);
                    lifetime_->changed.broadcast();
                }
            } catch (...) {
                lifetime_->failure.observe(std::current_exception());
            }
        co_await read_operations_.close();
        co_await stop_execution();
        if (extent_) {
            extent_->close();
            extent_.reset();
        }
        digest_.reset();
        zero_ = {};
        co_await close_publishers();
        if (readable_) {
            // Whoever still reads through it finishes first.
            co_await readable_->readers.close();
            try {
                lifetime_->failure.observe(co_await readable_->file->close());
            } catch (...) {
                lifetime_->failure.observe(std::current_exception());
            }
            readable_->closed = true;
            readable_ = nullptr;
        }
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
    // The one entrance of a new owner: `step` runs once, inside the owner's
    // operations, and its failure is the owner's.
    template<typename Step>
    seastar::future<runtime::result<void>>
    entered(codec::cooperative_work& work, Step step) {
        assert_current();
        if (entered_ || closing_ || closed_)
            co_return runtime::failure(detail::path_error(errc::closed));
        if (auto ready = work.poll(); !ready)
            co_return runtime::failure(
              detail::path_error(ready.error().code()));
        entered_ = true;
        auto holder = operations_.hold();
        // The metadata work of making or opening the files.
        auto metadata = budget_.try_reserve(
          byte_count{
            config_.metadata.operation_bytes.value()
            + config_.metadata.execution_bytes.value()});
        if (!metadata) {
            lifetime_->failure.observe(metadata.error());
            co_return lifetime_->failure.outcome();
        }
        try {
            co_await step();
        } catch (...) {
            lifetime_->failure.observe(std::current_exception());
        }
        if (closing_ && !lifetime_->failure.failed())
            co_return runtime::failure(detail::path_error(errc::closed));
        co_return lifetime_->failure.outcome();
    }
    // A request this segment takes no more: its owner is told why.
    [[nodiscard]] segment_group_preparation rolls(segment_roll_reasons why) {
        if (roll_observer_) roll_observer_->refused(why);
        return segment_group_preparation{
          segment_capacity_decision::roll_required, {}, why};
    }
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
        node.run_memory.reset();
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

    // What an index of this segment's sealed extent is bound to.
    std::optional<sparse_index_context> index_scope(
      const storage::coverage& coverage,
      codec::extent_digest digest) const noexcept {
        const auto context = sparse_index_context::make(
          descriptor_.segment,
          coverage,
          digest,
          descriptor_.alignment,
          descriptor_.profile);
        if (!context) return std::nullopt;
        return *context;
    }
    // The root an index source returned, when it can be named in this
    // segment's publication; otherwise why not.
    std::optional<local_root_reference> index_root(
      const runtime::result<local_root_reference>& published,
      runtime::first_failure& reason) const noexcept {
        if (!published) {
            reason.observe(published.error());
            return std::nullopt;
        }
        if (
          published->kind() != local_root_kind::index
          || !published->validate_alignment(descriptor_.alignment)) {
            reason.observe(detail::path_error(errc::wrong_context));
            return std::nullopt;
        }
        return *published;
    }

    // The encoded pointer of `publication` at generation `next`.
    seastar::future<runtime::result<bytes::fragmented_buffer>> encode_pointer(
      const local_object_publication& publication,
      local_publication_generation next,
      codec::cooperative_work& work) {
        const auto metadata_header = local_metadata_header::make(
                                       local_metadata_kind::object_publication,
                                       spec_.shard_owner(shard_).value(),
                                       next)
                                       .value();
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
        co_return std::move(pointer->bytes);
    }
    // What a pointer publication did. Anything but an untouched refusal that
    // is not durable leaves the pointer's durable state unknown: the owner
    // fails.
    runtime::result<void>
    pointer_outcome(const local_publication_outcome& outcome) {
        runtime::result<void> published{};
        if (outcome.failure.failed())
            published = outcome.failure.outcome();
        else if (outcome.disposition != local_publication_disposition::durable)
            published = runtime::failure(detail::path_error(errc::io_failure));
        if (
          !published
          && outcome.disposition != local_publication_disposition::untouched)
            lifetime_->failure.observe(published);
        return published;
    }
    // The sealed publication again, with `root` as its index root, through
    // the pointer's prepared publisher. Handles stay as they are: the data
    // and every other root are unchanged.
    seastar::future<runtime::result<void>> publish_indexed(
      local_root_reference root,
      sparse_index_context context,
      local_publication_generation next,
      codec::cooperative_work& work) {
        // Roots stay in kind order, and each keeps its context beside it.
        auto publication = *read_publication_;
        auto contexts = read_roots_;
        if (
          !publication.roots.empty()
          && publication.roots.front().kind() == local_root_kind::index) {
            publication.roots.front() = root;
            contexts.front() = context;
        } else {
            publication.roots.insert(publication.roots.begin(), root);
            contexts.insert(contexts.begin(), context);
        }
        if (publication.roots.size() > 4)
            co_return runtime::failure(
              detail::path_error(errc::resource_exhausted));
        auto pointer = co_await encode_pointer(publication, next, work);
        if (!pointer) co_return runtime::failure(pointer.error());
        if (auto valid = co_await owner_.validate(spec_); !valid)
            co_return valid;
        runtime::result<void> published{};
        {
            const auto outcome = co_await pointer_publisher_->publish(
              {spec_.shard_owner(shard_).value(), next, publication_},
              std::move(*pointer),
              work);
            published = pointer_outcome(outcome);
        }
        if (!published) co_return published;
        publication_ = next;
        read_publication_ = std::move(publication);
        read_roots_ = std::move(contexts);
        // What the segment's seal is asked for again names the index too.
        seal_result_.index = root;
        co_return lifetime_->failure.outcome();
    }
    // Unlinks a bundle this segment's publication does not name, with no
    // directory sync. One that survives is referenced by nothing.
    seastar::future<> remove_unnamed(local_root_reference bundle) {
        const auto object = local_paths::make(spec_.root)
                              ->object(
                                shard_,
                                {descriptor_.segment.segment(),
                                 descriptor_.segment.generation()},
                                bundle.sequence());
        if (!object) co_return;
        try {
            static_cast<void>(co_await files_.remove_file(*object));
        } catch (...) {
            // Left as debris for the next open's reconciliation.
        }
    }

    // Takes a seal's grants for its run: its own allowance, its bundle's,
    // and its two publications, prepared on this segment's directories:
    // the bundle's under its retry object, the pointer's over the
    // publication that stands.
    seastar::future<runtime::result<void>>
    take_grants(segment_seal_grants& grants, codec::cooperative_work& work) {
        if (auto filled = grants.fill(budget_, config_.metadata); !filled)
            co_return filled;
        if (!config_.retry_object)
            co_return runtime::failure(
              detail::path_error(errc::invalid_argument));
        seal_memory_ = std::move(grants.seal);
        grants.seal.reset();
        bundle_memory_ = std::move(grants.bundle);
        grants.bundle.reset();
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
        auto bundle = std::move(*grants.publications[0]);
        grants.publications[0].reset();
        if (
          auto prepared = co_await bundle_publisher_->prepare(
            std::move(bundle), config_.metadata.operation_bytes, work);
          !prepared)
            co_return prepared;
        pointer_publisher_.emplace(
          files_,
          budget_,
          target(
            path(local_segment_file::published),
            runtime::file_rename_policy::replace,
            publication_));
        auto pointer = std::move(*grants.publications[1]);
        grants.publications[1].reset();
        co_return co_await pointer_publisher_->prepare(
          std::move(pointer), config_.metadata.operation_bytes, work);
    }
    // Gives a seal's grants back once it ended, whatever its outcome: its
    // publications close and return theirs. What its bundle used left with
    // the bundle; the slot reserves it again.
    seastar::future<> give_grants(segment_seal_grants& grants) {
        if (seal_memory_) grants.seal = std::move(seal_memory_);
        seal_memory_.reset();
        if (bundle_memory_) grants.bundle = std::move(bundle_memory_);
        bundle_memory_.reset();
        for (auto [publisher, grant] :
             {std::pair{&bundle_publisher_, &grants.publications[0]},
              std::pair{&pointer_publisher_, &grants.publications[1]}}) {
            if (!*publisher) continue;
            try {
                lifetime_->failure.observe(co_await (*publisher)->close());
            } catch (...) {
                lifetime_->failure.observe(std::current_exception());
            }
            *grant = (*publisher)->release_prepared();
            publisher->reset();
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
          *publication_,
          *read_publication_,
          descriptor_,
          logical_origin_,
          read_roots_};
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

    template<typename Source, typename Index>
    seastar::future<runtime::result<void>> seal_owned(
      Source& source,
      std::uint32_t completed,
      Index& index,
      codec::cooperative_work& work) {
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
            if (observer_) observer_->durable(end.bytes);
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
          index,
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

    template<typename Source, typename Index>
    seastar::future<runtime::result<void>> seal_recovered_owned(
      Source& source,
      const verified_extent& extent,
      std::uint32_t completed,
      Index& index,
      codec::cooperative_work& work) {
        const segment_history_context history{
          descriptor_.segment,
          descriptor_.alignment,
          data_start_,
          logical_origin_,
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
          source,
          extent,
          end,
          completed,
          *size,
          std::move(dependencies),
          index,
          work);
    }

    // The retry bundle, then the sealed root at `end`; every byte past the
    // root is removed in the same durable step, one flush covers the extent
    // and the root, and only then is the sealed publication the first
    // published change. `file_bytes` is the file's size before the root,
    // through any zero-written or recovered tail. With the root durable an
    // index source publishes its bundle, and the publication names it too.
    template<typename Source, typename Dependencies, typename Index>
    seastar::future<runtime::result<void>> seal_root(
      Source& source,
      const verified_extent& finished,
      runtime::file_position end,
      std::uint32_t completed,
      std::uint64_t file_bytes,
      Dependencies dependencies,
      Index& index,
      codec::cooperative_work& work) {
        const footer_expectation location{
          {descriptor_.segment,
           descriptor_.alignment,
           data_start_,
           logical_origin_,
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
        // Derived state: whatever its publication does, the seal goes on.
        // An extent without a record has no index.
        std::optional<local_root_reference> indexed;
        std::optional<sparse_index_context> index_context;
        if constexpr (segment_seal_indexes<Index>) {
            if (!finished.boundary().coverage.physical().empty()) {
                try {
                    index_context = index_scope(
                      finished.boundary().coverage, *finished.digest());
                    if (!index_context)
                        sealing_.index.observe(
                          detail::path_error(errc::wrong_context));
                    else
                        indexed = index_root(
                          co_await index.publish(
                            files_,
                            owner_,
                            spec_,
                            shard_,
                            *index_context,
                            budget_,
                            config_.metadata,
                            work),
                          sealing_.index);
                } catch (...) {
                    sealing_.index.observe(std::current_exception());
                }
            }
        } else
            static_cast<void>(index);
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
        // Roots stay in kind order, and each keeps its context beside it.
        local_object_publication publication{
          descriptor_.segment, local_object_state::sealed, *footer, {}};
        std::vector<local_bundle_context> contexts;
        publication.roots.reserve(2);
        contexts.reserve(2);
        if (indexed) {
            publication.roots.push_back(*indexed);
            contexts.emplace_back(*index_context);
        }
        publication.roots.push_back(*reference);
        contexts.emplace_back(location);
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
        read_publication_ = std::move(publication);
        read_roots_ = std::move(contexts);
        co_await close_publishers();
        co_await close_handles();
        handle_ = lifetime_->failure.failed() ? segment_handle_state::closed
                                              : segment_handle_state::evicted;
        if (lifetime_->failure.failed()) co_return lifetime_->failure.outcome();
        seal_result_.boundary = *footer;
        seal_result_.retry = *reference;
        seal_result_.extent = immutable_;
        seal_result_.index = indexed;
        co_return runtime::result<void>{};
    }

    void fail_node(detail::segment_write_descriptor& node) noexcept {
        if (lifetime_->failure.exception())
            node.fail(lifetime_->failure.exception());
        else if (lifetime_->failure.error())
            node.fail(*lifetime_->failure.error());
    }

    // Adds a group to the extent walk in file order and builds what is
    // written for it: its blocks, and the footer after them when it has one.
    // Its blocks and bytes are final here, and the observer is told of them.
    // A held group's bytes are then copied into its run and the rest of it
    // is let go; a group with a footer takes the copies of the run it
    // closes, which its write carries before its own bytes.
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
                if (cut.footer()) {
                    auto prefix = extent_->checkpoint(work);
                    if (!prefix) {
                        node->fail(detail::path_error(prefix.error().code()));
                        break;
                    }
                    // A footer that only closes a run has no block of its
                    // own to name as the last.
                    if (
                      prefix->boundary().coverage != cut.covered()
                      || prefix->boundary().block_count != cut.end().blocks
                      || (!node->layout.blocks().empty()
                          && prefix->boundary().last_block
                               != std::optional{
                                 node->layout.blocks().back().records})) {
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
                    // The source's staging/promotion is covered by
                    // preparation. Feed this footer once, retaining its
                    // exact bytes for I/O.
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
                }
                // What its write carries before its own bytes: the copies of
                // the run its footer closes.
                const bool carries = cut.footer() && copies_;
                std::size_t fragments = footer
                                          ? footer->bytes().fragment_count()
                                          : 0;
                auto retained = footer ? footer->bytes().retained_bytes()
                                       : byte_count{};
                for (const auto& block : node->blocks) {
                    fragments += block.bytes().fragment_count();
                    retained = retained
                                 .checked_add(block.bytes().retained_bytes())
                                 .value();
                }
                if (carries) {
                    fragments += copies_->bytes.fragment_count() + 1;
                    retained = retained.checked_add(copies_->retained).value();
                    node->retained_bound = node->retained_bound
                                             .checked_add(copies_->retained)
                                             .value();
                    // The run reserved the whole list (carried_list).
                    node->gather_credit = node->gather_credit
                                            .checked_add(carried_list().value())
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
                const auto own = model::file_byte_span::make(
                                   node->before.end().bytes, cut.end().bytes)
                                   .value();
                bytes::fragmented_buffer_builder builder{
                  {.initial_fragment_bytes = byte_count{1},
                   .max_fragment_bytes = byte_count{1},
                   .max_total_bytes = own.size(),
                   .max_retained_bytes = retained,
                   .max_fragments = fragments}};
                builder.reserve_fragments(item_count{fragments}).value();
                for (auto& block : node->blocks) {
                    builder.append_buffer(std::move(block).release_bytes())
                      .value();
                    co_await work.drain_inline(
                      work.byte_quantum(), work.item_quantum());
                }
                if (footer)
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
                made_ = cut.end().bytes;
                if (observer_) {
                    observer_->blocks(node->layout.blocks());
                    if (
                      observer_->wanted != nullptr && *observer_->wanted
                      && node->working)
                        observer_->fed(
                          own.begin(), node->payload, *node->working);
                }
                tell_reported();
                if (node->held) {
                    keep_held(*node);
                    co_await work.drain_inline(
                      work.byte_quantum(), work.item_quantum());
                    break;
                }
                if (carries) {
                    auto held = std::move(*copies_);
                    copies_.reset();
                    auto prefix = held.bytes.finish().value();
                    bytes::fragmented_buffer_builder whole{
                      {.initial_fragment_bytes = byte_count{1},
                       .max_fragment_bytes = byte_count{1},
                       .max_total_bytes
                       = byte_count{prefix.size().value() + own.size().value()},
                       .max_retained_bytes = retained,
                       .max_fragments = fragments}};
                    whole.reserve_fragments(item_count{fragments}).value();
                    whole.append_buffer(std::move(prefix)).value();
                    whole.append_buffer(std::move(node->payload)).value();
                    node->payload = whole.finish().value();
                    node->run_begin = held.begin;
                    node->run_memory.emplace(std::move(held.memory));
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

    // Tells the observer how far readers may read now: what the owner
    // reported, as far as the groups made so far reach, so that every byte
    // a reader is let read is in the file or in memory.
    void tell_reported() noexcept {
        const auto readable = std::min(positions_->reported.bytes, made_);
        if (readable <= told_reported_) return;
        told_reported_ = readable;
        if (observer_ && observer_->reported) observer_->reported(readable);
    }

    // Copies a held group's bytes into the chunks its run reserved when it
    // opened, then lets go of the group: its blocks, children, payload and
    // allowance. It is then done, with nothing written of its own; the
    // group that closes the run writes these bytes.
    void keep_held(detail::segment_write_descriptor& node) {
        if (!copies_) {
            copies_.emplace(
              held_copies{
                node.extent().begin(),
                bytes::fragmented_buffer_builder{
                  {.initial_fragment_bytes = byte_count{held_chunk_bytes},
                   .max_fragment_bytes = byte_count{held_chunk_bytes}}},
                node.run_memory->share()});
            copies_->bytes.reserve_fragments(item_count{node.run_chunks})
              .value();
        }
        for (auto fragment : node.payload)
            copies_->bytes
              .append(std::span<const char>{fragment.data(), fragment.size()})
              .value();
        const auto kept = copies_->bytes.retained_bytes();
        pending_retained_ = byte_count{
          pending_retained_.value() - node.retained_bound.value() + kept.value()
          - copies_->retained.value()};
        copies_->retained = kept;
        node.retained_bound = {};
        node.payload = {};
        node.blocks = std::vector<segment_block>{};
        node.children = std::vector<admitted_wal_batch::contents>{};
        node.working.reset();
        node.run_memory.reset();
        node.absorbed = true;
        node.state = detail::segment_write_state::done;
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
                if (!lifetime_->failure.failed()) {
                    positions_->durable = cut.end();
                    if (observer_) observer_->durable(cut.end().bytes);
                }
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
                    // A held group's bytes went with the group that closed
                    // its run.
                    if (node->absorbed) continue;
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
            if (front->state == state::done && front->absorbed) {
                // Its bytes are with its run, which a later group writes.
                co_await front->execution.close();
                if (prefix_failed_) fail_node(*front);
                inflight_.pop_front();
                lifetime_->changed.broadcast();
                front->notify();
                continue;
            }
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
                        front->run_memory.reset();
                    }
                }
                // A caller may still borrow an unsubmitted handoff's encoded
                // blocks. Close fences the reservation but must not destroy
                // that caller's byte owner; its own release ends the borrow.
                if (front->failure.failed())
                    prefix_failed_ = true;
                else if (!prefix_failed_) {
                    positions_->written = front->layout.boundary().end();
                    if (observer_ && observer_->written)
                        observer_->written(positions_->written.bytes);
                }
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
            // Held groups are done once copied; their bytes are written with
            // the group that closes their run, so they lie between writes.
            const auto skipped = [](const pointer& node) {
                return node->state == state::dispatched || node->absorbed;
            };
            auto first = inflight_.begin();
            while (first != inflight_.end() && skipped(*first))
                ++first;
            auto rest = first;
            while (rest != inflight_.end()
                   && ((*rest)->state == state::queued || (*rest)->absorbed))
                ++rest;
            bool synchronous = false, wanted = false;
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
                    if (node->absorbed) continue;
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
                    wanted |= node->wanted;
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
                // without a flush. Anything else needs the barrier's. A
                // write of held runs alone is not wanted durable yet, so it
                // takes the ordinary handle and the flush of whatever
                // barrier later covers it, unless an extension holds that
                // handle for its zeros and their flush: only writes past the
                // extension's start wait for it, so inside the range such a
                // write goes through the synchronized handle instead.
                const bool overwrite = sync_
                                       && logical
                                            <= config_.synchronous_write_bytes
                                       && start.value() >= zero_begin_
                                       && end.value() <= prezeroed_end_;
                synchronous = overwrite && (wanted || extension_.has_value());
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

    // What a run reserves when it opens: a node of its own for the footer
    // that closes it, with the staging of a footer that pads the most;
    // copies of the held groups' bytes, which stay below `hold`: whole
    // chunks, and room to list them; and the list of the write that carries
    // them (carried_list).
    std::optional<byte_count> run_charge(byte_count hold) {
        auto fixed = capacity();
        if (!fixed || !(*fixed)->closing_staging) return std::nullopt;
        const auto control = node_charge(3, false);
        const auto chunk = budget_.allocation_charge(
          byte_count{held_chunk_bytes});
        const auto chunks = held_chunks(hold);
        const auto listed = budget_.allocation_charge(
          byte_count{
            chunks * bytes::fragmented_buffer::fragment_descriptor_size()});
        const auto carried = carried_list();
        if (!control || !chunk || !listed || !carried) return std::nullopt;
        // What the runtime may stage for the write that carries the run.
        const auto staging = write_staging(
          hold.value() + (*fixed)->footer_bytes.value_or(byte_count{}).value()
          + descriptor_.alignment.bytes().value());
        if (!staging) return std::nullopt;
        auto total = control->checked_add(*(*fixed)->closing_staging);
        if (total) total = total->checked_add(*listed);
        if (total) total = total->checked_add(*carried);
        if (total) total = total->checked_add(*staging);
        if (total)
            total = total->checked_add(byte_count{chunks * chunk->value()});
        return total;
    }
    // What the runtime may stage to write `bytes` of groups, through either
    // handle, which share one geometry: the staging of a write of twice as
    // many bytes, at a boundary or not, whichever stages more. A gathered
    // write stages no more than its groups' charges together: none of its
    // allocations exceeds twice its bytes, and it takes no more of them
    // than its groups would apart. Nothing for a segment not opened to be
    // written.
    std::optional<byte_count> write_staging(std::uint64_t bytes) const {
        if (!data_) return std::nullopt;
        const auto geometry = data_->geometry();
        if (!geometry) return std::nullopt;
        const byte_count length{2 * bytes};
        std::uint64_t most = 0;
        for (const auto position : {std::uint64_t{0}, std::uint64_t{1}}) {
            const auto layout = geometry->write_buffers(
              runtime::file_position{position}, length);
            if (!layout.allocations) continue;
            const auto charge = budget_.allocation_charge(
              layout.allocation_bytes);
            if (!charge) return std::nullopt;
            most = std::max(most, charge->value() * layout.allocations);
        }
        return byte_count{most};
    }
    // The write that closes a run lists the copies and its group's own
    // bytes in one allocation, which charges more than the two lists apart
    // when their sum crosses a size class. A freeze closes the run first
    // rather than let that list pass the most fragments a buffer holds, so
    // the run pays for a list of that length.
    runtime::result<byte_count> carried_list() const {
        return budget_.allocation_charge(
          byte_count{
            bytes::max_buffer_fragments
            * bytes::fragmented_buffer::fragment_descriptor_size()});
    }
    static std::uint64_t held_chunks(byte_count hold) noexcept {
        return (hold.value() + held_chunk_bytes - 1) / held_chunk_bytes;
    }

    // A group's node: its descriptor, its queue slot, the descriptor lists
    // of its bytes and, unless it is a run's closing footer, which encodes
    // nothing of its own, its execution frames.
    runtime::result<byte_count>
    node_charge(std::size_t fragments, bool executes = true) const {
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
          + (executes ? config_.admission.execution_bytes.value() : 0)};
    }

    bool group_fits_budget(byte_count working, byte_count control) const {
        // These are distinct, startup-held leases. Exclude other callers and
        // outstanding preparations: those cause temporary queue pressure.
        const std::array fixed{
          held_.bytes(),
          credits_ ? credits_->bytes() : byte_count{},
          lifetime_->held.bytes(),
          completion_->charged_bytes()};
        byte_count total{};
        std::uint64_t tasks = 2;
        for (auto amount : fixed) {
            const auto next = total.checked_add(amount);
            if (!next) return false;
            total = *next;
            if (amount.value() != 0) ++tasks;
        }
        for (auto amount : {working, control}) {
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
      model::range_logical_end logical_origin,
      workload_budget& budget,
      segment_writer_config config,
      bool fresh) {
        auto header = segment_header::make(
          descriptor.segment, descriptor.alignment, descriptor.profile);
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
            // A window or a step past the data limit is capped by it.
            if (
              (config.preallocation_extension_bytes.value() != 0
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
        // A zero-written segment also owns a synchronized-write handle. What
        // either handle stages for a write is the group's that writes it,
        // and metadata work is the operation's that does it: an idle
        // segment holds neither.
        const std::uint32_t handles
          = fresh && config.preallocation_bytes.value() != 0 ? 2U : 1U;
        auto held = budget.try_reserve(
          byte_count{instance->value() + path->value() + 2 * chunk->value()});
        if (!held) return runtime::failure(held.error());
        // The handles' credits are held apart, so that an owner that failed
        // gives them back when it lets go of its handles: see
        // release_writing().
        auto credits = budget.try_reserve(byte_count{});
        if (!credits) return runtime::failure(credits.error());
        if (auto acquired = credits->try_acquire_handles(handles); !acquired)
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
          logical_origin,
          budget,
          config,
          *header,
          std::move(*held),
          std::move(*credits),
          std::move(lifetime))};
    }
    segment_writer(
      Backend& files,
      Owner& owner,
      local_device_spec spec,
      std::uint32_t shard,
      local_segment_descriptor descriptor,
      model::range_logical_end logical_origin,
      workload_budget& budget,
      segment_writer_config config,
      segment_header header,
      workload_reservation held,
      workload_reservation credits,
      seastar::lw_shared_ptr<segment_captured_boundary::lifetime> lifetime)
      : files_(files)
      , owner_(owner)
      , spec_(std::move(spec))
      , shard_(shard)
      , descriptor_(std::move(descriptor))
      , logical_origin_(logical_origin)
      , budget_(budget)
      , config_(config)
      , header_(header)
      , held_(std::move(held))
      , credits_(std::move(credits))
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
    // Replaces a pointer publisher that fenced itself with a prepared one
    // for the same target, under the publication that still stands.
    seastar::future<runtime::result<void>>
    renew_pointer_publisher(codec::cooperative_work& work) {
        try {
            // Whatever its close reports, the failed publication reported.
            static_cast<void>(co_await pointer_publisher_->close());
            pointer_publisher_.emplace(
              files_,
              budget_,
              target(
                path(local_segment_file::published),
                runtime::file_rename_policy::replace,
                *publication_));
            co_return co_await pointer_publisher_->prepare(
              config_.metadata.operation_bytes, work);
        } catch (...) {
            co_return runtime::failure(detail::recognized_or_rethrown());
        }
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
                              : runtime::file_access::read_only});
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
    // Makes the segment's files: its directory, its descriptor, its data
    // file holding its header, the window of zeros after the header when one
    // is configured, written and made durable once, and its first pointer,
    // active and naming nothing. Every handle it opens is closed again, and
    // nothing of the segment's range is bound: its origin comes when it is
    // opened to be written (activate_owned). Creation never resumes an
    // existing name.
    seastar::future<runtime::result<void>>
    make_files(codec::cooperative_work& work, spare_pool* spares = nullptr) {
        if (auto valid = co_await owner_.validate(spec_); !valid)
            co_return valid;
        const auto data_path = path(local_segment_file::data);
        const auto directory = runtime::file_path::make(
                                 data_path.value().substr(
                                   0, data_path.value().rfind('/')))
                                 .value();
        auto existing = co_await files_.stat(directory);
        if (existing)
            co_return runtime::failure(
              detail::path_error(errc::already_exists));
        if (existing.error().code() != errc::not_found)
            co_return runtime::failure(existing.error());
        // A creation publication's outcome ends where its failure is taken:
        // kept, it would hold that publication's allowance, handle credits
        // included, for as long as the segment is open.
        if (
          auto published = (co_await publish_local_segment_descriptor(
                              files_,
                              owner_,
                              spec_,
                              shard_,
                              descriptor_,
                              header_,
                              budget_,
                              config_.metadata,
                              work))
                             .failure.outcome();
          !published)
            co_return published;
        auto header = co_await encode_segment_header(
          header_,
          work,
          config_.metadata.operation_bytes,
          config_.metadata.charge);
        if (!header)
            co_return runtime::failure(
              detail::path_error(header.error().code()));
        data_start_ = runtime::file_position{header->size().value()};
        // The data file is a spare when one is kept, else a new file.
        std::optional<byte_count> spare;
        if (spares) {
            auto taken = co_await take_spare(*spares, *header, work);
            if (!taken) co_return runtime::failure(taken.error());
            spare = *taken;
            if (spare) spare_bytes_ = *spare;
        }
        if (!spare) {
            if (
              auto published
              = (co_await publish_new(
                   local_segment_file::data, std::move(*header), work))
                  .failure.outcome();
              !published)
                co_return published;
        }
        if (config_.preallocation_bytes.value() != 0 || spare) {
            if (
              auto opened = co_await open_data(true, work, spare.has_value());
              !opened)
                co_return opened;
            auto zeroed = co_await zero_window(work);
            co_await close_handles();
            handle_ = segment_handle_state::closed;
            if (!zeroed) co_return zeroed;
            if (lifetime_->failure.failed())
                co_return lifetime_->failure.outcome();
        }
        const auto generation = local_publication_generation::make(1).value();
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
        if (!encoded)
            co_return runtime::failure(
              detail::path_error(encoded.error().code()));
        co_return (
          co_await publish_new(
            local_segment_file::published, std::move(encoded->bytes), work))
          .failure.outcome();
    }

    // Opens a segment whose files make_files() made to be written, at the
    // origin its owner gave: its writable handle, and the synchronized one
    // when its file reaches past its header, every byte of which is written,
    // by the window's zeros or by the file's earlier life; then its seal's
    // grants and publications, its positions and extent from the origin, and
    // its dispatcher. It writes nothing and syncs nothing.
    seastar::future<> activate_owned(codec::cooperative_work& work) {
        lifetime_->failure.observe(co_await open_data(true, work, true));
        if (lifetime_->failure.failed()) co_return;
        auto size = co_await data_->size();
        if (!size) {
            lifetime_->failure.observe(size);
            co_return;
        }
        const auto alignment = descriptor_.alignment.bytes().value();
        const auto written = *size / alignment * alignment;
        if (written > data_start_.value()) {
            lifetime_->failure.observe(co_await open_window(written));
            if (lifetime_->failure.failed()) co_return;
        }
        // Its seal's grants are a seal slot's, taken when it seals.
        const auto generation = local_publication_generation::make(1).value();
        if (lifetime_->failure.failed() || closing_) co_return;
        const segment_writer_position initial{
          logical_origin_, descriptor_.physical_origin, data_start_};
        positions_.emplace(
          segment_writer_positions{initial, initial, initial, initial});
        // Nothing is made, and readers are told nothing past the start.
        made_ = data_start_;
        told_reported_ = data_start_;
        const segment_history_context history{
          descriptor_.segment,
          descriptor_.alignment,
          data_start_,
          logical_origin_,
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

    // Zero-writes [data start, window end) through the ordinary handle and
    // makes it durable once. Each bounded runtime write repeats one shared
    // zero-filled chunk, which is let go once the window is written.
    seastar::future<runtime::result<void>>
    zero_window(codec::cooperative_work& work) {
        const auto alignment = descriptor_.alignment.bytes().value();
        const auto start = data_start_.value();
        const auto limit = descriptor_.maximum_data_bytes.value() / alignment
                           * alignment;
        // What a spare's earlier life wrote is written already.
        auto size = co_await data_->size();
        if (!size) co_return runtime::failure(size.error());
        const auto written = std::max(*size / alignment * alignment, start);
        prezeroed_end_ = written;
        if (start >= limit) co_return runtime::result<void>{};
        const auto end = start
                         + std::min(
                             config_.preallocation_bytes.value(), limit - start)
                             / alignment * alignment;
        if (end <= written) co_return runtime::result<void>{};
        auto transient = reserve_zero_chunk();
        if (!transient) co_return runtime::failure(transient.error());
        // Reserve the whole range first, then zero-write it: otherwise the
        // concurrent zero writes fragment the file into many small extents.
        auto reserved = co_await data_->allocate(
          runtime::file_position{written}, byte_count{end - written});
        if (!reserved) co_return reserved;
        zero_ = detail::make_zero_chunk(write_allocation_);
        auto zeroed = co_await detail::zero_fill(
          *data_, zero_, written, end, work);
        zero_ = {};
        if (!zeroed) co_return zeroed;
        auto synced = co_await data_->flush(completion_->metadata());
        if (!synced) co_return synced;
        prezeroed_end_ = end;
        co_return runtime::result<void>{};
    }
    // Makes a spare this segment's data file, where publish_new() would
    // make a new one: it is moved under a temporary name in the segment's
    // directory, cut to the data limit when it is longer, its first bytes
    // overwritten with the header and made durable, then it takes its name
    // and the directory is synced. None when no spare is kept. Its earlier
    // bytes after the header stay, never read: another segment's objects,
    // at which every scan of this segment stops.
    seastar::future<runtime::result<std::optional<byte_count>>> take_spare(
      spare_pool& spares,
      bytes::fragmented_buffer& header,
      codec::cooperative_work& work) {
        using output = runtime::result<std::optional<byte_count>>;
        const auto data = path(local_segment_file::data);
        const auto split = data.value().rfind('/');
        const auto directory
          = runtime::file_path::make(data.value().substr(0, split)).value();
        const auto name = local_temporary_name(
          runtime::file_name::make(data.value().substr(split + 1)).value(),
          local_publication_generation::make(1).value(),
          63);
        if (!name) co_return output{runtime::failure(name.error())};
        const auto temporary = local_child_path(directory, *name);
        if (!temporary) co_return output{runtime::failure(temporary.error())};
        std::optional<byte_count> taken;
        {
            const auto turn = co_await spares.turn();
            auto moved = co_await spares.take(*temporary, work);
            if (!moved) co_return output{runtime::failure(moved.error())};
            taken = *moved;
        }
        if (!taken) co_return output{std::optional<byte_count>{}};
        auto opened = co_await files_.open(
          *temporary, {.access = runtime::file_access::read_write});
        if (!opened) co_return output{runtime::failure(opened.error())};
        auto file = std::move(*opened);
        runtime::first_failure failed;
        try {
            do {
                if (*taken > descriptor_.maximum_data_bytes) {
                    failed.observe(
                      co_await file.truncate(
                        descriptor_.maximum_data_bytes.value()));
                    if (failed.failed()) break;
                    taken = descriptor_.maximum_data_bytes;
                }
                const auto bytes = header.size();
                auto written = co_await file.write(
                  runtime::file_position{}, header.share());
                if (!written) {
                    failed.observe(written);
                    break;
                }
                if (*written != bytes) {
                    failed.observe(detail::path_error(errc::io_failure));
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
        if (auto done = detail::settled(failed); !done)
            co_return output{runtime::failure(done.error())};
        if (
          auto renamed = co_await files_.rename(
            *temporary, data, runtime::file_rename_policy::no_replace);
          !renamed)
            co_return output{runtime::failure(renamed.error())};
        auto parent = co_await files_.open_directory(directory);
        if (!parent) co_return output{runtime::failure(parent.error())};
        auto held = std::move(*parent);
        try {
            failed.observe(co_await held.sync());
        } catch (...) {
            failed.observe(std::current_exception());
        }
        try {
            failed.observe(co_await held.close());
        } catch (...) {
            failed.observe(std::current_exception());
        }
        if (auto done = detail::settled(failed); !done)
            co_return output{runtime::failure(done.error())};
        co_return output{taken};
    }
    // The zero chunk, its write lists, and what the runtime may stage for
    // one zero write: writes repeat the chunk, and its size is not theirs.
    runtime::result<workload_reservation> reserve_zero_chunk() {
        const auto zero_charge = budget_.allocation_charge(write_allocation_);
        const auto list_charge = budget_.allocation_charge(
          byte_count{
            2 * detail::zero_write_chunks
            * bytes::fragmented_buffer::fragment_descriptor_size()});
        const auto staging = write_staging(
          detail::zero_write_chunks * write_allocation_.value() / 2);
        if (!zero_charge) return runtime::failure(zero_charge.error());
        if (!list_charge) return runtime::failure(list_charge.error());
        if (!staging)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        return budget_.try_reserve(
          byte_count{
            zero_charge->value() + list_charge->value() + staging->value()});
    }
    // Admits synchronized writes to [data start, `written`), every byte of
    // which is written already: the synchronized handle opens. No write, no
    // sync.
    seastar::future<runtime::result<void>> open_window(std::uint64_t written) {
        auto opened = co_await files_.open(
          path(local_segment_file::data),
          {.access = runtime::file_access::read_write, .synchronous = true});
        if (!opened) co_return runtime::failure(opened.error());
        sync_.emplace(std::move(*opened));
        if (
          auto limited = sync_->limit_write_allocation(write_allocation_);
          !limited)
            co_return limited;
        if (
          auto shared = sync_->allow_concurrent_writes(
            config_.concurrent_writes);
          !shared)
            co_return shared;
        zero_begin_ = data_start_.value();
        prezeroed_end_ = written;
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
    // a synchronized write makes its own blocks durable either way. The zero
    // chunk and what its writes may stage are taken for the extension and
    // let go after it, so an idle segment holds none of them; an extension
    // the budget cannot take now is tried again later.
    seastar::future<> extend_window(std::uint64_t begin, std::uint64_t end) {
        seastar::abort_source abort;
        codec::cooperative_work work{config_.policy, abort};
        runtime::first_failure failed;
        auto held = reserve_zero_chunk();
        if (held) {
            try {
                zero_ = detail::make_zero_chunk(write_allocation_);
            } catch (const std::bad_alloc&) {
                held = runtime::failure(
                  detail::path_error(errc::resource_exhausted));
            }
        }
        if (!held) {
            plain_writing_ = false;
            lifetime_->changed.broadcast();
            co_return;
        }
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
        zero_ = {};
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
    // Where the segment starts in its range, as its owner supplied it.
    model::range_logical_end logical_origin_;
    workload_budget& budget_;
    segment_writer_config config_;
    segment_header header_;
    workload_reservation held_;
    std::optional<workload_reservation> credits_;
    seastar::lw_shared_ptr<segment_captured_boundary::lifetime> lifetime_;
    std::optional<runtime::file> data_;
    std::optional<completion_resources> completion_;
    std::optional<workload_reservation> seal_memory_, bundle_memory_;
    std::optional<local_file_publisher<Backend>> bundle_publisher_,
      pointer_publisher_;
    std::unique_ptr<local_generation_owner> generation_;
    std::optional<local_publication_generation> publication_;
    // The startup seal execution allowance covers the native hash state for
    // this walk's lifetime; per-group workspace covers parser/codec
    // temporaries.
    std::optional<extent_verifier> extent_;
    std::optional<segment_writer_positions> positions_;
    std::optional<model::file_byte_span> reserved_footer_;
    // The run held open at the reserved end: blocks frozen since the last
    // footer, none of them written. Where it begins, and what it reserved
    // when it opened: the footer that will close it and copies of up to the
    // hold's bytes (see run_charge), which its groups share.
    struct open_run final {
        runtime::file_position begin;
        workload_reservation memory;
    };
    std::optional<open_run> run_;
    // A held group's bytes are copied into chunks of this size.
    static constexpr std::uint64_t held_chunk_bytes = 16_KiB;
    // The bytes of held groups assembled so far, in file order, waiting for
    // the group that closes their run: where they begin, the copies, and how
    // much of what the run reserved they retain.
    struct held_copies final {
        runtime::file_position begin;
        bytes::fragmented_buffer_builder bytes;
        workload_reservation memory;
        byte_count retained{};
    };
    std::optional<held_copies> copies_;
    // Where the groups made so far end: from here back to what is written,
    // their bytes are in memory. And what readers were told they may read:
    // what the owner reported, as far as that.
    runtime::file_position made_{}, told_reported_{};
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
    // Staging bound installed on writable handles; also the zero chunk size.
    byte_count write_allocation_{};
    // What a spare held of the data file when it was made.
    byte_count spare_bytes_{};
    // A failed owner let go of its writing resources.
    bool released_{false};
    bool digest_stopped_{false};
    byte_count pending_bytes_{}, pending_retained_{};
    std::optional<seastar::future<>> dispatcher_;
    seastar::promise<> dispatcher_started_;
    static constexpr std::uint32_t maximum_control_waiters = 8;
    std::uint32_t close_waiters_{0}, seal_waiters_{0}, digest_waiters_{0};
    std::uint32_t retry_page_entries_{1};
    bool execution_stopped_{false}, seal_started_{false}, seal_done_{false};
    bool read_busy_{false}, writable_{false};
    // An index publication in flight, named after the seal.
    bool publishing_{false};
    segment_seal_progress sealing_;
    segment_seal_outcome seal_result_;
    std::optional<segment_immutable_expectation> immutable_;
    std::optional<local_object_publication> read_publication_;
    std::vector<local_bundle_context> read_roots_;
    // The read-only handle readers of an appended segment share, once one
    // has asked, and whether it is being opened.
    seastar::lw_shared_ptr<detail::segment_data_handle> readable_;
    bool data_pin_busy_{false};
    bool freezing_{false}, prefix_failed_{false}, barrier_busy_{false};
    std::optional<segment_block_observer> observer_;
    std::optional<segment_roll_observer> roll_observer_;
    std::optional<runtime::monotonic_time> first_acceptance_;
    // No batch's append time is below this: the floor its creator gave,
    // then the newest of its accepted groups'.
    runtime::wall_time append_floor_{1};
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
