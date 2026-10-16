#pragma once

#include "src/base/error.h"
#include "src/base/invariant.h"
#include "src/codec/cooperative.h"
#include "src/codec/limits.h"
#include "src/runtime/error.h"
#include "src/runtime/file.h"
#include "src/runtime/first_failure.h"
#include "src/runtime/shard_affinity.h"
#include "src/runtime/time.h"
#include "src/runtime/timer.h"
#include "src/storage/checkpoint.h"
#include "src/storage/device_placement.h"
#include "src/storage/format_context.h"
#include "src/storage/local_append.h"
#include "src/storage/local_control.h"
#include "src/storage/local_id_allocator.h"
#include "src/storage/local_paths.h"
#include "src/storage/local_store_config.h"
#include "src/storage/page_cache.h"
#include "src/storage/recovery_decision.h"
#include "src/storage/recovery_inventory.h"
#include "src/storage/recovery_merge.h"
#include "src/storage/recovery_plan.h"
#include "src/storage/recovery_publication.h"
#include "src/storage/recovery_successor.h"
#include "src/storage/retention.h"
#include "src/storage/segment_lifecycle.h"
#include "src/storage/segment_writer.h"
#include "src/storage/segment_writer_state.h"
#include "src/storage/sparse_index.h"
#include "src/storage/sparse_index_owner.h"
#include "src/storage/wal_group_commit.h"
#include "src/storage/wal_reclaimer.h"
#include "src/storage/wal_writer.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>

#include <cstdint>
#include <exception>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <utility>
#include <variant>
#include <vector>

namespace kwaque::storage {

// Where one shard's storage is in its life.
enum class engine_state : std::uint8_t {
    constructed,
    // Opening the control record and whatever follows it.
    starting,
    // A store that was used before, read and not yet taken over; and the
    // state a start is left in when what it read permits no change: a
    // scope the restart plan stopped, or a seal decision to carry out
    // first. Nothing was changed, and whoever owns the segments decides.
    recovering,
    // Appends, readers and removals are served.
    ready,
    stopping,
    stopped,
};

struct local_engine_config final {
    // The WAL's geometry is its store's: nothing here has a default for it.
    wal_writer_config wal;
    wal_group_commit_config commit{};
    local_append_config append{};
    checkpoint_limits checkpoint{};
    segment_table_config table{};
    page_cache_config cache{};
    device_placement_config placement{};
    // What a segment's writer is made with, for the check of the handle
    // credits the appends' budget must hold, and for the fresh flush and
    // recovering publication of a segment a restart found.
    segment_writer_config writer{};
    local_store_io_limits metadata{};
    recovery_decision_limits decisions{};
    recovery_merge_limits merge{};
    recovered_publication_limits publications{};
    // Index roots the shard keeps open, and rebuilds it runs at once.
    std::uint32_t resident_index_roots{default_resident_index_roots};
    std::uint32_t index_rebuilds{default_sparse_index_rebuilds};
    // Identities taken from the control record at a time.
    std::uint32_t id_block{256};
    // Handle credits of the appends' budget that are not a segment's: the
    // WAL writer's files and its publications.
    std::uint32_t reserved_append_handles{8};
    codec::limits policy{codec::limits::defaults()};
};

// What a start found, for whoever owns the segments.
struct engine_start final {
    engine_state state{engine_state::stopped};
    // The store was used before: its WAL head was closed with a successor.
    bool restarted{false};
    // Unsealed segments the restart found and the table took in, each told
    // to the roll sink as recovered: none is appended to again here. And
    // those the table had no place for, which are not told: its bound on
    // segments that are not sealed is below what the catalog lists of
    // them. Whoever owns the segments removes some of the first and has
    // the table take in the rest.
    std::uint32_t recovered{0}, unadopted{0};
    // Removals the restart went on with: segments under their deleting
    // name that the catalog lists as expiring, and expiring ones found
    // whole and authorized. And segments under their deleting name that
    // the catalog lists as retained: held as they are, under that name,
    // until their owner says they expire.
    std::uint32_t resumed{0}, marked_retained{0};
    // Expiring segments no device holds, and that nothing durable here
    // names any more: removals that finished.
    std::uint32_t gone{0};
    // Removals that are not finished: of those gone on with, and of the
    // segments no device holds that the durable checkpoint or the WAL
    // still names. A request for each finishes it.
    std::uint32_t owed{0};
    // Expiring segments kept because their authorization does not name
    // what the device holds.
    std::uint32_t refused{0};
    // Segments with a durable seal decision that has not been carried
    // out. The shard stays recovering until one is.
    std::uint32_t sealing{0};
    // PREPAREs with no segment the catalog explains; the WAL that holds
    // them stays until they are resolved. And the files its chain has.
    std::uint64_t unresolved{0}, wal_files{0};
    // Why the shard is not ready, when it is not.
    runtime::first_failure failure;
};

// One shard's storage, composed: the control record and identities, the WAL
// and its group commit, the append owner, the checkpoint owner, the cache,
// the device table and the table of segment owners. It starts a store that
// was never used or restarts one that was, and stops them in the order each
// owner's contract asks for.
//
// A control record with no WAL head says the store was never used. It is
// believed only when the owner lists no segment and no WAL file is there
// that the record never handed out; otherwise the record is older than the
// store, and the start is refused before it writes anything.
//
// A restart reads everything before it changes anything: every durable
// decision, the segments its owner's catalog lists, the durable checkpoint
// and the WAL from the checkpoint's cutoff. Only a plan with no stopped
// scope lets it go on: each recovered segment gets a fresh flush and a
// recovering publication, the WAL head is closed with a successor and never
// resumed, what the restart pinned is restored into the append owner, and
// the checkpoint owner continues the durable table. The removals a restart
// found then go on, and every unsealed segment is told as recovered.
//
// The catalog is the segments that were writable, unresolved or expiring,
// and every sealed one a restart would still look for: the durable
// checkpoint carries an entry for a segment until a checkpoint cut after its
// seal is durable, the WAL above the cutoff holds its PREPAREs, and a start
// fails on an entry its catalog does not explain. The table says which they
// are, see segment_table::restart_names(); a clean stop leaves none but
// those a pinned PREPARE holds. Sealed history beyond that is not in the
// catalog and is opened when it is read, so a start does not grow with what
// the store retains.
//
// Every provider outlives joined stop().
template<
  runtime::file_system_backend Backend,
  local_directory_owner Owner,
  runtime::monotonic_clock Clock>
class local_engine final : public runtime::shard_affine {
public:
    using control_type = local_control_owner<Backend, Owner>;
    using allocator_type = local_id_allocator<Backend, Owner>;
    using wal_type = wal_writer<Backend, Owner>;
    using append_type = local_append<Backend, Owner, Clock>;
    using checkpoint_type = local_checkpoint<Backend, Owner, Clock>;
    using table_type = segment_table<Backend, Owner, Clock>;

    struct providers final {
        Backend& files;
        Owner& ownership;
        // Every configured device, and the one that holds this shard's
        // control record, WAL and checkpoints; borrowed until joined stop().
        std::span<const local_device_spec> devices;
        const local_device_spec& control;
        std::uint32_t shard;
        // The shard's budgets, cut from its workload classes: appends and
        // the table from the foreground; the cache, generations and runs
        // beside them; the control record and checkpoints from metadata,
        // which shares no admission with the appends; recovery, rebuilds
        // and removals from maintenance.
        workload_budget& appends;
        workload_budget& readers;
        workload_budget& metadata;
        workload_budget& maintenance;
    };
    // Told on the shard, each after the owner that reports it is
    // consistent. None may throw.
    struct sinks final {
        segment_roll_sink rolls;
        local_failure_sink failures;
        local_retention_sink pressure;
    };

    [[nodiscard]] static runtime::result<std::unique_ptr<local_engine>>
    make(providers given, local_engine_config config) {
        // Everything that can refuse the configuration is asked before a
        // store is opened: a start that fails later has changed it.
        for (const auto& valid :
             {config.table.validate(),
              config.placement.validate(),
              config.metadata.validate(),
              config.commit.validate(),
              config.append.validate(),
              config.checkpoint.validate(),
              config.cache.validate(),
              config.decisions.validate(),
              config.merge.validate(),
              config.publications.validate()})
            if (!valid) return runtime::failure(valid.error());
        // What must agree: one policy for the table, its writers and the
        // append owner; no more segments written than the append owner
        // attaches; and no more left unsealed or being removed than one
        // restart's catalog lists.
        if (
          config.table.policy != config.policy
          || config.writer.policy != config.policy
          || config.append.policy != config.policy
          || config.table.maximum_writable > config.append.maximum_segments
          || std::size_t{config.table.maximum_writable}
                 + config.table.maximum_deleting
               > maximum_recovery_catalog)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        if (
          !given.control.controls() || !given.control.shard_owner(given.shard))
            return runtime::failure(detail::path_error(errc::invalid_argument));
        // The checkpoint owner frees the appends' WAL: it may not wait for
        // what they hold.
        if (given.metadata.shares_admission_with(given.appends))
            return runtime::failure(detail::path_error(errc::invalid_argument));
        // Every writable segment open at once, beside the WAL.
        if (
          auto held = validate_segment_handles(
            given.appends,
            config.writer,
            config.table.handle_demand(config.reserved_append_handles));
          !held)
            return runtime::failure(held.error());
        try {
            return std::unique_ptr<local_engine>{
              new local_engine(given, std::move(config))};
        } catch (const std::bad_alloc&) {
            return runtime::failure(
              detail::path_error(errc::resource_exhausted));
        }
    }
    local_engine(const local_engine&) = delete;
    local_engine& operator=(const local_engine&) = delete;
    ~local_engine() {
        assert_current();
        KWAQUE_INVARIANT(
          invariant_id{"KQ-LOCAL-ENGINE-STOPPED"},
          state_ == engine_state::constructed
            || state_ == engine_state::stopped,
          "local engine destroyed before joined stop");
    }

    [[nodiscard]] engine_state state() const noexcept {
        assert_current();
        return state_;
    }
    // The owners a ready shard serves through. Nothing before it is ready.
    [[nodiscard]] append_type* append() const noexcept {
        assert_current();
        return state_ == engine_state::ready ? append_.get() : nullptr;
    }
    [[nodiscard]] table_type* table() const noexcept {
        assert_current();
        return state_ == engine_state::ready ? table_.get() : nullptr;
    }
    [[nodiscard]] checkpoint_type* checkpoint() const noexcept {
        assert_current();
        return state_ == engine_state::ready ? checkpoint_.get() : nullptr;
    }
    [[nodiscard]] device_table* placement() const noexcept {
        assert_current();
        return state_ == engine_state::ready ? placement_.get() : nullptr;
    }
    // Why the shard stopped serving, when a failure did it: the first
    // failure of the WAL, which is on the control device.
    [[nodiscard]] const runtime::first_failure& failure() const& noexcept {
        assert_current();
        return failed_;
    }
    const runtime::first_failure& failure() const&& = delete;

    // Starts the shard. `catalog` is what its owner of segments lists; it
    // is borrowed until this returns. A store that cannot be read, or whose
    // owners cannot be made, fails the start and leaves the shard stopped:
    // nothing is left open. A store whose restart plan permits no change
    // leaves it recovering, with nothing changed and nothing open: what
    // was read is in the report, and the shard is started again once its
    // owner of segments has decided, as a new engine. `timer` outlives
    // joined stop(), which follows this call's return: one made while the
    // start runs is refused.
    template<runtime::timer_service Timer>
    [[nodiscard]] seastar::future<engine_start> start(
      Timer& timer,
      std::span<const recovery_catalog_entry> catalog,
      sinks told) {
        assert_current();
        engine_start report;
        if (state_ != engine_state::constructed) {
            report.state = state_;
            report.failure.observe(detail::path_error(errc::closed));
            co_return report;
        }
        state_ = engine_state::starting;
        starting_ = true;
        failures_ = std::move(told.failures);
        try {
            report.failure.observe(
              co_await started(
                timer,
                catalog,
                std::move(told.rolls),
                std::move(told.pressure),
                report));
        } catch (...) {
            report.failure.observe(std::current_exception());
        }
        if (report.failure.failed() || state_ == engine_state::recovering) {
            // Nothing is served: what was opened is closed again, and what
            // that fails with is no part of why the start failed. A shard
            // left recovering changed nothing, and stays so.
            const bool recovering = state_ == engine_state::recovering
                                    && !report.failure.failed();
            try {
                static_cast<void>(co_await closed());
            } catch (...) {
            }
            state_ = recovering ? engine_state::recovering
                                : engine_state::stopped;
        }
        starting_ = false;
        report.state = state_;
        co_return report;
    }

    // The early notice of a stop: admission closes and what is forming
    // forms now. Nothing accepted is failed, and nothing is waited for.
    void stop_admission() noexcept {
        assert_current();
        abort_.request_abort();
    }

    // Stops the shard: accepted appends drain through both barriers and
    // each writable segment is closed, not sealed, so the next start finds
    // it unsealed; sessions are ended; lifetimes and removals stop where
    // they stand, with their marks on the device; one checkpoint is asked
    // for, which ends at the WAL's durable end; then the checkpoint owner,
    // the append owner, the group commit, the WAL, the identities, the
    // control record and the cache close, none skipped for another's
    // failure. There is no flag that says the stop was clean: the next
    // start reads the same store. Repeated calls join the first. Refused
    // with queue_full while a start runs: it follows that start's return.
    [[nodiscard]] seastar::future<runtime::result<void>> stop() {
        assert_current();
        if (stopping_) co_return co_await stopping_->get_shared_future();
        if (starting_)
            co_return runtime::failure(detail::path_error(errc::queue_full));
        if (
          state_ == engine_state::stopped || state_ == engine_state::constructed
          || state_ == engine_state::recovering) {
            state_ = engine_state::stopped;
            co_return runtime::result<void>{};
        }
        stopping_.emplace();
        state_ = engine_state::stopping;
        abort_.request_abort();
        runtime::first_failure kept;
        try {
            kept = co_await closed();
        } catch (...) {
            kept.observe(std::current_exception());
        }
        const auto outcome = detail::settled(kept);
        state_ = engine_state::stopped;
        auto ended = std::move(*stopping_);
        stopping_.reset();
        ended.set_value(outcome);
        co_return outcome;
    }

private:
    local_engine(providers given, local_engine_config config)
      : files_(given.files)
      , ownership_(given.ownership)
      , devices_(given.devices)
      , control_spec_(given.control)
      , shard_(given.shard)
      , appends_(given.appends)
      , readers_(given.readers)
      , metadata_(given.metadata)
      , maintenance_(given.maintenance)
      , config_(std::move(config))
      , residency_(config_.resident_index_roots)
      , rebuilds_(config_.index_rebuilds) {}

    // Feeds every merged item to the plan and keeps the obligation rows.
    struct restart_view final {
        recovery_planner* planner;
        std::vector<recovery_obligation>* rows;
        seastar::future<runtime::result<bool>>
        operator()(const recovery_item& item) const {
            if (auto observed = planner->observe(item); !observed)
                return seastar::make_ready_future<runtime::result<bool>>(
                  runtime::failure(observed.error()));
            if (const auto* row = std::get_if<recovery_obligation>(&item))
                rows->push_back(*row);
            return seastar::make_ready_future<runtime::result<bool>>(true);
        }
    };
    // What a restart read and established before the append owner exists.
    struct restarted final {
        recovery_inventory inventory;
        std::vector<recovery_obligation> rows;
        std::optional<loaded_checkpoint> durable;
        std::optional<recovered_publications> published;
    };

    template<runtime::timer_service Timer>
    seastar::future<runtime::result<void>> started(
      Timer& timer,
      std::span<const recovery_catalog_entry> catalog,
      segment_roll_sink rolls,
      local_retention_sink pressure,
      engine_start& report) {
        codec::cooperative_work work{config_.policy, work_abort_};
        auto control = co_await control_type::open(
          files_,
          ownership_,
          control_spec_,
          shard_,
          false,
          metadata_,
          config_.metadata,
          work);
        if (!control) co_return runtime::failure(control.error());
        control_ = std::move(*control);
        const auto opened = control_->snapshot();
        if (!opened) co_return runtime::failure(opened.error());
        auto ids = allocator_type::make(*control_, metadata_, config_.id_block);
        if (!ids) co_return runtime::failure(ids.error());
        ids_ = std::move(*ids);

        std::optional<restarted> recovered;
        if (!opened->fields.wal_head) {
            // Never used, unless the store says otherwise: a segment its
            // owner lists, or a WAL file the record never handed out, is a
            // store the record is older than. It is refused with nothing
            // written over it.
            if (!catalog.empty())
                co_return runtime::failure(
                  detail::path_error(errc::wrong_context));
            auto unallocated = co_await holds_unallocated_wal(
              files_,
              ownership_,
              control_spec_,
              shard_,
              opened->fields.wal_high,
              work);
            if (!unallocated) co_return runtime::failure(unallocated.error());
            if (*unallocated)
                co_return runtime::failure(
                  detail::path_error(errc::wrong_context));
            // The first WAL head is made here.
            auto writer = wal_type::make(
              *control_,
              *ids_,
              appends_,
              config_.wal,
              wal_start_intent::known_unactivated);
            if (!writer) co_return runtime::failure(writer.error());
            writer_ = std::move(*writer);
            if (auto made = co_await writer_->bootstrap(work); !made)
                co_return made;
        } else {
            report.restarted = true;
            state_ = engine_state::recovering;
            auto read = co_await restart(opened->fields, catalog, work, report);
            if (!read) co_return runtime::failure(read.error());
            // Nothing may be changed: the shard stays recovering.
            if (!*read) co_return runtime::result<void>{};
            recovered.emplace(std::move(**read));
            state_ = engine_state::starting;
            // What a checkpoint reclaimed and a crash brought back, before
            // any owner exists that could replace the head they are below.
            if (
              auto below = co_await remove_wal_below(
                files_, ownership_, control_spec_, shard_, *control_, work);
              !below)
                co_return runtime::failure(below.error());
            if (
              auto stale = co_await remove_stale_checkpoints(
                files_, ownership_, control_spec_, shard_, *control_, work);
              !stale)
                co_return runtime::failure(stale.error());
        }
        // Where the WAL's data begins in a file: the head holds nothing yet.
        const auto wal = writer_->progress();
        if (!wal) co_return runtime::failure(detail::path_error(errc::closed));
        const auto data_start = wal->reserved.position();

        auto commit = wal_group_commit::make(
          *writer_, appends_, config_.commit);
        if (!commit) co_return runtime::failure(commit.error());
        commit_ = std::move(*commit);
        if (auto begun = commit_->template start<Clock>(timer); !begun)
            co_return begun;
        auto append = append_type::make(
          appends_,
          *commit_,
          *writer_,
          config_.append,
          [this](const local_storage_failure& failure) noexcept {
              failed_storage(failure);
          },
          std::move(pressure));
        if (!append) co_return runtime::failure(append.error());
        append_ = std::move(*append);
        if (auto bound = append_->bind_shutdown(abort_); !bound)
            co_return bound;
        if (recovered) {
            // What the restart pinned holds its WAL file as a failed group
            // does, until a supplied resolution or a removal releases it.
            const auto pinned = fold_pinned_obligations(
              recovered->rows, wal->owner);
            if (!pinned) co_return runtime::failure(pinned.error());
            if (auto restored = append_->restore_pinned(*pinned); !restored)
                co_return restored;
        }

        // The unreclaimed WAL begins in its oldest file while no checkpoint
        // says otherwise.
        const auto oldest = append_->retained_wal().oldest();
        if (!oldest)
            co_return runtime::failure(detail::path_error(errc::closed));
        const auto origin = local_wal_cursor::make(*oldest, data_start);
        if (!origin)
            co_return runtime::failure(
              detail::path_error(errc::invariant_violation));
        auto checkpoint = checkpoint_type::make(
          files_,
          ownership_,
          devices_,
          control_spec_,
          shard_,
          *control_,
          *ids_,
          *append_,
          metadata_,
          config_.checkpoint,
          config_.policy,
          *origin);
        if (!checkpoint) co_return runtime::failure(checkpoint.error());
        checkpoint_ = std::move(*checkpoint);
        if (recovered) {
            if (recovered->durable)
                if (
                  auto continued = checkpoint_->restore(
                    std::move(*recovered->durable));
                  !continued)
                    co_return continued;
            if (
              auto covered = checkpoint_->recovered(*recovered->published);
              !covered)
                co_return covered;
            // A segment the catalog lists and its device holds sealed is
            // decided by its own publication: what the durable checkpoint
            // still carries of it leaves with the next one, and is not
            // carried on.
            for (const auto& entry : recovered->inventory.entries)
                if (
                  entry.state == recovery_entry_state::target
                  && entry.publication.state == local_object_state::sealed)
                    if (
                      auto covered = checkpoint_->cover(entry.publication);
                      !covered)
                        co_return covered;
        }
        if (auto bound = checkpoint_->bind_retention(); !bound) co_return bound;

        auto cache = page_cache::make(readers_, config_.cache);
        if (!cache) co_return runtime::failure(cache.error());
        cache_ = std::move(*cache);
        auto placement = device_table::make(
          devices_,
          config_.placement,
          [this](const device_report& changed) noexcept {
              if (table_) table_->device_changed(changed);
          });
        if (!placement) co_return runtime::failure(placement.error());
        placement_ = std::move(*placement);
        auto table = table_type::make(
          {files_,
           ownership_,
           devices_,
           shard_,
           *append_,
           checkpoint_.get(),
           *ids_,
           *cache_,
           residency_,
           rebuilds_,
           appends_,
           readers_,
           maintenance_,
           placement_.get()},
          config_.table);
        if (!table) co_return runtime::failure(table.error());
        table_ = std::move(*table);
        if (rolls)
            if (
              auto telling = table_->report_rolls(timer, std::move(rolls));
              !telling)
                co_return telling;

        if (recovered) {
            // One checkpoint now that the head has its successor: when
            // nothing is pinned its cutoff moves into the new head, the
            // files before it go, and the durable table lets go of what
            // the catalog lists sealed. One that cannot be taken costs the
            // removals below a wait and the next start a longer scan.
            try {
                static_cast<void>(co_await checkpoint_->let_go());
            } catch (...) {
            }
            co_await resume(recovered->inventory, report);
        }
        state_ = engine_state::ready;
        co_return runtime::result<void>{};
    }

    // Reads a store that was used and, when its plan permits, establishes
    // what a ready shard stands on. Nothing when the plan permits no
    // change.
    seastar::future<runtime::result<std::optional<restarted>>> restart(
      const local_shard_control& fields,
      std::span<const recovery_catalog_entry> catalog,
      codec::cooperative_work& work,
      engine_start& report) {
        using output = runtime::result<std::optional<restarted>>;
        std::vector<local_segment_descriptor> descriptors;
        descriptors.reserve(catalog.size());
        for (const auto& listed : catalog)
            descriptors.push_back(listed.descriptor);
        std::vector<recovery_decision_record> decided;
        const auto discovered = co_await discover_recovery_decisions(
          files_,
          ownership_,
          control_spec_,
          shard_,
          fields,
          std::span<const local_segment_descriptor>{descriptors},
          maintenance_,
          config_.decisions,
          work,
          [&decided](const recovery_decision_record& value) {
              try {
                  decided.push_back(value);
              } catch (const std::bad_alloc&) {
                  return seastar::make_ready_future<runtime::result<bool>>(
                    runtime::failure(
                      detail::path_error(errc::resource_exhausted)));
              }
              return seastar::make_ready_future<runtime::result<bool>>(true);
          });
        if (!discovered) co_return output{runtime::failure(discovered.error())};
        auto found = co_await open_recovery_inventory(
          files_,
          ownership_,
          devices_,
          shard_,
          catalog,
          std::span<const recovery_decision_record>{decided},
          maintenance_,
          config_.metadata,
          work);
        if (!found) co_return output{runtime::failure(found.error())};
        restarted read{std::move(*found), {}, {}, {}};
        read.rows.reserve(read.inventory.targets.size());
        for (const auto& entry : read.inventory.entries) {
            if (entry.state == recovery_entry_state::sealing) ++report.sealing;
            if (entry.refused) ++report.refused;
        }
        // The durable checkpoint, under the budget its owner continues it
        // with: each segment resumes at the higher of its own pin and the
        // checkpoint's.
        auto durable = co_await load_checkpoint(
          files_,
          ownership_,
          control_spec_,
          shard_,
          fields,
          metadata_,
          config_.metadata,
          work);
        if (!durable) co_return output{runtime::failure(durable.error())};
        std::optional<local_wal_cursor> cutoff;
        if (*durable) {
            const auto raised = resume_from_checkpoint(
              (*durable)->entries,
              read.inventory,
              std::span<const recovery_decision_record>{decided});
            if (!raised) co_return output{runtime::failure(raised.error())};
            cutoff = (*durable)->end;
            read.durable = std::move(*durable);
        }
        auto planner = recovery_planner::make(
          read.inventory.targets, maintenance_);
        if (!planner) co_return output{runtime::failure(planner.error())};
        const auto merged = co_await reconcile_local_recovery(
          files_,
          ownership_,
          devices_,
          control_spec_,
          shard_,
          fields,
          cutoff,
          std::span<const recovery_target>{read.inventory.targets},
          maintenance_,
          config_.merge,
          work,
          restart_view{&*planner, &read.rows});
        if (!merged) co_return output{runtime::failure(merged.error())};
        if (auto finished = planner->finish(*merged); !finished)
            co_return output{runtime::failure(finished.error())};
        report.unresolved = planner->wal().unresolved;
        report.wal_files = planner->wal().chain_files;
        // A stopped scope, or a seal decision to carry out first: nothing
        // is changed.
        if (!planner->ready() || report.sealing != 0)
            co_return output{std::optional<restarted>{}};

        auto writer = wal_type::make(
          *control_,
          *ids_,
          appends_,
          config_.wal,
          wal_start_intent::recovered_head);
        if (!writer) co_return output{runtime::failure(writer.error())};
        writer_ = std::move(*writer);
        // What a recovered segment's writer is made with names an object of
        // its own, as a created segment's does.
        auto recovering = config_.writer;
        {
            const auto reserved = co_await ids_->allocate_object(work);
            if (!reserved) co_return output{runtime::failure(reserved.error())};
            recovering.retry_object = *reserved;
        }
        // Each recovered segment's fresh flush and recovering publication,
        // and the head's successor.
        auto published = co_await establish_recovered_state<Clock>(
          files_,
          ownership_,
          devices_,
          shard_,
          *planner,
          read.inventory,
          std::span<const recovery_visibility>{},
          maintenance_,
          recovering,
          config_.publications,
          *writer_,
          config_.policy,
          work_abort_);
        if (!published) co_return output{runtime::failure(published.error())};
        if (!published->complete)
            co_return output{
              runtime::failure(detail::path_error(errc::io_failure))};
        read.published.emplace(std::move(*published));
        co_return output{std::optional<restarted>{std::move(read)}};
    }

    // After the table exists: the removals a restart found go on, a
    // removal that finished is counted, and every unsealed segment is told
    // as recovered and taken in, so that its owner of segments can remove
    // it as what it is.
    seastar::future<>
    resume(const recovery_inventory& inventory, engine_start& report) {
        for (const auto& entry : inventory.entries) {
            const auto& segment = entry.history.segment;
            switch (entry.state) {
            case recovery_entry_state::gone: {
                // On no device. What the checkpoint and the WAL still hold
                // of it is released, and it is gone once neither names it.
                const auto removed = co_await table_->remove(*entry.expiring);
                ++(removed.gone ? report.gone : report.owed);
                break;
            }
            case recovery_entry_state::deleting: {
                // Under its deleting name, and listed as retained: held as
                // it is. Removed, the same catalog would list a retained
                // segment no device holds, which the next start reads as
                // lost.
                if (entry.marked && !entry.expiring) {
                    ++report.marked_retained;
                    break;
                }
                ++report.resumed;
                const auto removed = co_await table_->resume_removal(
                  segment, entry.device, entry.expiring);
                if (!removed.gone) ++report.owed;
                break;
            }
            case recovery_entry_state::target:
                if (entry.publication.state == local_object_state::sealed)
                    break;
                ++(
                  table_->adopt_recovered(segment, entry.device)
                    ? report.recovered
                    : report.unadopted);
                break;
            case recovery_entry_state::sealing:
                break;
            }
        }
    }

    // The WAL's first failure ends what the shard can make durable: it is
    // kept, and whoever composes the process is told.
    void failed_storage(const local_storage_failure& failure) noexcept {
        if (failure.path == local_storage_path::wal && !failed_.failed())
            failed_ = failure.failure;
        // The segment's own failure first: its device's then fails the
        // segments beside it, and finds this one failed with what it said.
        if (table_ && failure.segment)
            table_->fail(*failure.segment, failure.failure);
        if (placement_ && failure.device)
            placement_->failed(*failure.device, failure.detail);
        if (failures_) failures_(failure);
    }

    // Closes whatever is open, in the order a stop takes: no step is
    // skipped for the failure of one before it, and the first is returned,
    // as it was.
    seastar::future<runtime::first_failure> closed() {
        runtime::first_failure kept;
        work_abort_.request_abort();
        if (table_) co_await joined(kept, table_->close());
        if (checkpoint_) {
            // An ordinary checkpoint at the WAL's durable end: the next
            // start scans what follows it, which is nothing. It is written
            // though the cutoff stands still when a segment was sealed
            // since the one before it, so that the durable table names no
            // sealed segment and the next catalog need not list one. One
            // that cannot be taken costs the next start a longer scan, and
            // a catalog that still lists them; it is no failure of the
            // stop.
            if (table_ && !failed_.failed()) {
                try {
                    static_cast<void>(co_await checkpoint_->let_go());
                } catch (...) {
                }
            }
            co_await joined(kept, checkpoint_->close());
        }
        if (append_) co_await joined(kept, append_->close());
        if (table_) {
            // An owner the append owner would not let go of while it ran
            // is let go now that it has drained and closed.
            runtime::first_failure again;
            co_await joined(again, table_->close());
            if (table_->size() != 0) {
                if (again.exception()) kept.observe(again.exception());
                if (again.error()) kept.observe(*again.error());
            }
        }
        table_.reset();
        checkpoint_.reset();
        placement_.reset();
        append_.reset();
        if (commit_) co_await joined(kept, commit_->close());
        commit_.reset();
        if (writer_) co_await joined(kept, writer_->close());
        writer_.reset();
        if (ids_) co_await joined(kept, ids_->close());
        ids_.reset();
        if (control_) co_await joined(kept, control_->close());
        control_.reset();
        if (cache_) {
            try {
                co_await cache_->stop();
            } catch (...) {
                kept.observe(std::current_exception());
            }
        }
        cache_.reset();
        co_return kept;
    }
    // One step of a stop: what it fails with is kept, and the next step
    // runs all the same.
    static seastar::future<> joined(
      runtime::first_failure& kept,
      seastar::future<runtime::result<void>> closing) {
        try {
            kept.observe(co_await std::move(closing));
        } catch (...) {
            kept.observe(std::current_exception());
        }
    }

    Backend& files_;
    Owner& ownership_;
    std::span<const local_device_spec> devices_;
    const local_device_spec& control_spec_;
    std::uint32_t shard_;
    workload_budget& appends_;
    workload_budget& readers_;
    workload_budget& metadata_;
    workload_budget& maintenance_;
    local_engine_config config_;
    engine_state state_{engine_state::constructed};
    // The early notice of a stop, and what ends the start's own reads.
    seastar::abort_source abort_, work_abort_;
    runtime::first_failure failed_;
    // start() runs: a stop is refused until it has returned.
    bool starting_{false};
    local_failure_sink failures_;
    sparse_index_residency residency_;
    sparse_index_rebuild_limit rebuilds_;
    std::unique_ptr<control_type> control_;
    std::unique_ptr<allocator_type> ids_;
    std::unique_ptr<wal_type> writer_;
    std::unique_ptr<wal_group_commit> commit_;
    std::unique_ptr<append_type> append_;
    std::unique_ptr<checkpoint_type> checkpoint_;
    std::unique_ptr<page_cache> cache_;
    std::unique_ptr<device_table> placement_;
    std::unique_ptr<table_type> table_;
    std::optional<seastar::shared_promise<runtime::result<void>>> stopping_;
};

} // namespace kwaque::storage
