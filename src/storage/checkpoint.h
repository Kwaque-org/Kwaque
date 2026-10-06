#pragma once

#include "src/base/invariant.h"
#include "src/base/units.h"
#include "src/runtime/task_scope.h"
#include "src/storage/local_append.h"
#include "src/storage/local_bundle.h"
#include "src/storage/local_generation.h"
#include "src/storage/recovery_decision.h"
#include "src/storage/recovery_inventory.h"
#include "src/storage/recovery_publication.h"
#include "src/storage/wal_reclaimer.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_vector.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/with_scheduling_group.hh>

#include <algorithm>
#include <array>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <vector>

namespace kwaque::storage {

// One pin per segment from the obligation rows a restart rebuilt: every
// PREPARE the segment has pinned across WAL files, at the oldest of them. Only
// a segment's oldest unresolved PREPARE bounds a checkpoint, and the append
// owner's table has room for one pinned row per segment. Rows that pin
// nothing are skipped.
[[nodiscard]] runtime::result<std::vector<local_pinned_obligation>>
fold_pinned_obligations(
  std::span<const recovery_obligation> rows, const local_store_context& owner);

// Where the next checkpoint ends: the WAL below it is no longer needed. It is
// the lowest of where the oldest open or pinned obligation begins, the
// WAL-durable end, and `held`. A group can be segment-durable before its WAL
// barrier completes, so the discharged prefix alone can lie past what the WAL
// made durable. `held` is where the oldest hold begins: a recovered seal
// whose durable decision has not finished executing, since its suffix plan is
// classified from the cutoff, or a WAL scan in progress. Each bound is a
// complete PREPARE boundary, and so is the result. After a rotation the WAL
// writer's positions are at the successor's data start, so once nothing in a
// closed file is open the cutoff moves into its successor instead of resting
// on that file's end. Positions are compared under their checked order; a
// result below `previous`, the durable checkpoint's end, is refused.
[[nodiscard]] runtime::result<local_wal_cursor> checkpoint_cutoff(
  const local_store_context& owner,
  const local_obligation_snapshot& obligations,
  const std::optional<local_wal_cursor>& held,
  const std::optional<local_wal_cursor>& previous) noexcept;

inline constexpr std::uint32_t maximum_checkpoint_segments
  = 2 * maximum_local_append_segments;
inline constexpr std::uint32_t maximum_checkpoint_decisions = 1024;
struct checkpoint_pin_limits final {
    // Unsealed segments pinned or waiting for a pin: every attachment, and
    // each one detached until its own publication covers it.
    std::uint32_t segments{128};
    // Discard decisions held until their segment's publication covers them.
    std::uint32_t decisions{maximum_checkpoint_decisions};
    [[nodiscard]] runtime::result<void> validate() const noexcept;
};

// A durable checkpoint as a reopen read it: the bundle the control's head
// names, its root under the control's digest and every page under the
// root's, with the table in canonical order.
struct loaded_checkpoint final {
    // Declared first: released after the table it admitted.
    workload_reservation held;
    local_root_reference reference;
    // The WAL below `end` is no longer needed: the cutoff a restart scans
    // from.
    local_wal_cursor begin, end;
    seastar::chunked_vector<local_checkpoint_entry> entries;
};

// What a durable checkpoint's table changed in a restart's inventory.
struct checkpoint_resume final {
    // Segments whose scan resumes at the checkpoint's pin, above the one
    // their own publication carries.
    std::uint32_t raised{0};
    // Entries of a segment its own publication or a pending seal decides.
    std::uint32_t superseded{0};
    // Discard entries matched to their durable decision.
    std::uint32_t discards{0};
};

// Hands a restart the pins of the durable checkpoint, before its merge reads
// any segment. Each active or recovering segment resumes at the higher of its
// publication's boundary and its checkpoint entry: the WAL below the cutoff
// is gone, so damage below that entry must be proven corruption, never a
// shorter boundary. The scan verifies the footer it resumes at against the
// pin. A segment's sealed or deleting publication, or a durable seal decision
// it has not carried out, supersedes its entries whatever end it chose. A
// boundary entry whose segment the catalog lacks, or names on another device,
// fails, and so does a discard entry whose decision is absent or differs.
// Nothing is read or changed but the inventory's pins.
[[nodiscard]] runtime::result<checkpoint_resume> resume_from_checkpoint(
  const seastar::chunked_vector<local_checkpoint_entry>& table,
  recovery_inventory& inventory,
  std::span<const recovery_decision_record> decisions);

// One cut of the pin table: where the checkpoint ends, the entries carried
// into it unchanged, the discards its end passed, and the footers still to
// be read. Once every footer's pin is installed, entries() is the
// checkpoint's table in canonical order.
class checkpoint_capture final {
public:
    checkpoint_capture(checkpoint_capture&&) noexcept = default;
    checkpoint_capture& operator=(checkpoint_capture&&) noexcept = default;
    checkpoint_capture(const checkpoint_capture&) = delete;
    checkpoint_capture& operator=(const checkpoint_capture&) = delete;

    [[nodiscard]] local_wal_cursor end() const noexcept { return end_; }
    [[nodiscard]] const seastar::chunked_vector<local_checkpoint_entry>&
    entries() const& noexcept {
        return entries_;
    }
    const seastar::chunked_vector<local_checkpoint_entry>&
    entries() const&& = delete;
    // Segments whose newest durable footer is not the one their entry pins,
    // in segment order. Each footer is read once.
    [[nodiscard]] const seastar::chunked_vector<local_durable_boundary>&
    refresh() const& noexcept {
        return refresh_;
    }
    const seastar::chunked_vector<local_durable_boundary>&
    refresh() const&& = delete;
    // How many of refresh() have their pin installed, in order.
    [[nodiscard]] std::size_t installed() const noexcept { return installed_; }
    [[nodiscard]] bool complete() const noexcept {
        return installed_ == refresh_.size();
    }
    // Installs the pin read for refresh()[installed()]: it replaces the
    // segment's carried boundary entry or adds its first. The reference must
    // name that footer's exact span. A native allocation failure throws,
    // with no effect.
    [[nodiscard]] runtime::result<void>
    install(const local_footer_reference& reference);

private:
    friend class checkpoint_pins;
    checkpoint_capture(
      workload_reservation held,
      local_wal_cursor end,
      std::uint64_t cut) noexcept
      : held_(std::move(held))
      , end_(end)
      , cut_(cut) {}
    // Declared first: released after the tables it admitted.
    workload_reservation held_;
    local_wal_cursor end_;
    std::uint64_t cut_;
    seastar::chunked_vector<local_checkpoint_entry> entries_;
    seastar::chunked_vector<local_durable_boundary> refresh_;
    std::size_t installed_{0};
};

// The pins a shard's checkpoints carry in place of reclaimed WAL: for each
// unsealed segment, the newest durable footer that covers its PREPAREs below
// the cutoff, and each discard decision the cutoff has passed. The table is
// carried forward. An entry whose segment made no progress stays as it is,
// and an entry leaves only when the segment's own durable publication covers
// it. A cut copies the table; adopting the cut, once the checkpoint that
// carries it is durable, makes the cut's copy the table. Nothing here reads
// or writes storage, and nothing is flushed for a pin: every reported append
// already made its segment bytes durable.
//
// No table is one large allocation. Each is held in fragments within the
// contiguous allocation bound and charged fragment by fragment: the footers
// and held discards for their limits when the owner is made, and the table
// itself with the cut that produced it.
class checkpoint_pins final {
public:
    [[nodiscard]] static runtime::result<checkpoint_pins>
    make(workload_budget& budget, checkpoint_pin_limits limits);
    checkpoint_pins(checkpoint_pins&&) noexcept = default;
    checkpoint_pins& operator=(checkpoint_pins&&) noexcept = default;
    checkpoint_pins(const checkpoint_pins&) = delete;
    checkpoint_pins& operator=(const checkpoint_pins&) = delete;

    // The table as the durable checkpoint carries it, less what a
    // publication has covered since, in canonical order.
    [[nodiscard]] const seastar::chunked_vector<local_checkpoint_entry>&
    entries() const& noexcept {
        return entries_;
    }
    const seastar::chunked_vector<local_checkpoint_entry>&
    entries() const&& = delete;
    // Segments with a durable footer newer than the one their entry pins.
    [[nodiscard]] std::size_t stale() const noexcept { return pending_.size(); }

    // Records an unsealed segment's newest durable footer: from the append
    // owner while it is attached, and its last one when it detaches. A
    // segment's footers only advance on one device; anything else is
    // rejected. The next cut pins it. A native allocation failure throws,
    // with no effect; so it does for hold().
    [[nodiscard]] runtime::result<void>
    observe(const local_durable_boundary& boundary);
    // Holds a durable discard decision. A cut carries it once its end passes
    // the PREPARE the decision names, and from then until the segment's
    // publication covers it. The same decision again changes nothing.
    [[nodiscard]] runtime::result<void>
    hold(const recovery_decision_record& decision);
    // The append owner is about to release the PREPARE a discard names or,
    // with `released`, has. Only a decision held here that has released
    // nothing yet may: one never held is refused, and so is one seen
    // released, which a cut may already carry.
    [[nodiscard]] runtime::result<void>
    discharge(const local_recovery_decision& decision, bool released) noexcept;
    // The segment's own durable publication takes over from its entries. A
    // sealed or deleting one releases its boundary and its discards; a
    // recovering one releases the boundary only when it pins at least the
    // segment's newest known footer. The caller holds the publication.
    [[nodiscard]] runtime::result<void>
    cover(const local_object_publication& publication);

    // Cuts the table for a checkpoint that ends at `end`, in one synchronous
    // step. Entries are copied unchanged; nothing is read here. The copy is
    // admitted and allocated here, where a refusal only means no checkpoint
    // now.
    [[nodiscard]] runtime::result<checkpoint_capture>
    capture(const local_store_context& owner, local_wal_cursor end);
    // The checkpoint carrying this cut is durable: the cut's entries become
    // the table, except those a publication covered meanwhile. Only the
    // latest cut, complete, is adopted, and only once. It takes over the
    // cut's memory and admission, so it allocates nothing and cannot be
    // refused for room.
    [[nodiscard]] runtime::result<void> adopt(checkpoint_capture&& cut);
    // After a restart: the table the durable checkpoint carries is the table,
    // before anything else is recorded here. Its entries are carried forward
    // like any other and leave as each segment's publication covers them. It
    // takes over the memory and admission the table was loaded under.
    [[nodiscard]] runtime::result<void> restore(loaded_checkpoint&& durable);
    // The durable checkpoint this table continues: the one restored or last
    // adopted, and none before the first. A cut is published only as the
    // successor of exactly that one.
    [[nodiscard]] const std::optional<local_root_reference>&
    continues() const& noexcept {
        return continues_;
    }
    const std::optional<local_root_reference>& continues() const&& = delete;
    void continue_from(const local_root_reference& durable) noexcept {
        continues_.emplace(durable);
    }

private:
    struct held_discard final {
        local_checkpoint_entry pin;
        // Where the PREPARE the decision discards begins.
        local_wal_cursor prepare;
        // The append owner released that PREPARE by this decision.
        bool released{false};
    };
    checkpoint_pins(
      workload_budget& budget,
      checkpoint_pin_limits limits,
      workload_reservation held) noexcept
      : held_(std::move(held))
      , budget_(&budget)
      , limits_(limits) {}

    // Declared first: released after the tables they admitted. The second
    // came with the adopted cut and covers the table.
    workload_reservation held_;
    std::optional<workload_reservation> table_held_;
    workload_budget* budget_;
    checkpoint_pin_limits limits_;
    seastar::chunked_vector<local_checkpoint_entry> entries_;
    // Newest durable footers not pinned yet, in segment order.
    seastar::chunked_vector<local_durable_boundary> pending_;
    // Held discards no cut has carried durably yet, in pin order.
    seastar::chunked_vector<held_discard> discards_;
    // Segments with a boundary entry or a pending footer, and discards held
    // or carried.
    std::uint32_t segments_{0}, decisions_{0};
    // The latest cut issued, and whether it is still to be adopted.
    std::uint64_t cut_{0};
    bool cut_open_{false};
    std::optional<local_root_reference> continues_;
};

namespace detail {
// Encodes a cut's table as checkpoint pages, one per call and in table order,
// then nothing. Encoding is deterministic: a first pass yields the
// references the root pins, and a second pass the same bytes for the file,
// so no page is kept in between.
struct checkpoint_pages final {
    const seastar::chunked_vector<local_checkpoint_entry>* entries;
    local_store_context owner;
    storage_alignment alignment;
    local_object_sequence sequence;
    // Entries one page holds.
    std::uint32_t capacity;
    local_store_io_limits limits;
    std::uint32_t first{0}, ordinal{0};
    // The reference of the page just encoded.
    std::optional<page_ref> last;

    seastar::future<runtime::result<std::optional<bytes::fragmented_buffer>>>
    next(codec::cooperative_work& work);
};
// A cut's root with the reference that pins it and the context a reader
// independently expects.
struct encoded_checkpoint_root final {
    local_root_reference reference;
    local_metadata_expectation expected;
    bytes::fragmented_buffer bytes;
};
// The root of a checkpoint from `begin` to `end` over the pages a first pass
// of `pages` yields. `pages` is left at its start for the second pass.
[[nodiscard]] seastar::future<runtime::result<encoded_checkpoint_root>>
encode_checkpoint_root(
  checkpoint_pages& pages,
  local_wal_cursor begin,
  local_wal_cursor end,
  codec::cooperative_work& work);
} // namespace detail

namespace detail {
// Reads a durable footer's exact bytes once and returns the reference that
// pins them. The same bytes must decode as the footer of this segment at
// this position, covering what the boundary says.
[[nodiscard]] seastar::future<runtime::result<local_footer_reference>>
pin_durable_footer(
  runtime::file& data,
  local_durable_boundary boundary,
  local_store_io_limits limits,
  codec::cooperative_work& work);
} // namespace detail

// Reads one segment's durable footer in place and returns the reference that
// pins its exact bytes. The digest comes from the bytes on the device, never
// from what the writer remembers writing. It runs outside the append path, on
// its own read-only handle, and reads the footer once.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<runtime::result<local_footer_reference>> read_durable_boundary(
  Backend& files,
  Owner& ownership,
  std::span<const local_device_spec> devices,
  std::uint32_t shard,
  local_durable_boundary boundary,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work) {
    if (auto valid = limits.validate(); !valid)
        co_return runtime::failure(valid.error());
    const auto device = std::find_if(
      devices.begin(), devices.end(), [&boundary](const auto& spec) {
          return spec.owner.device() == boundary.device;
      });
    if (device == devices.end() || !device->stores_data())
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    const auto& spec = *device;
    if (auto valid = co_await ownership.validate(spec); !valid)
        co_return runtime::failure(valid.error());
    const auto paths = local_paths::make(spec.root);
    if (!paths) co_return runtime::failure(paths.error());
    const auto& segment = boundary.history.segment;
    auto path = paths->segment_file(
      shard,
      {segment.segment(), segment.generation()},
      local_segment_file::data);
    if (!path) co_return runtime::failure(path.error());
    auto handle = budget.try_reserve(limits.execution_bytes);
    if (!handle) co_return runtime::failure(handle.error());
    if (auto held = handle->try_acquire_handles(1); !held)
        co_return runtime::failure(held.error());
    if (
      auto inspected = co_await inspect_local_path(
        files, spec.root, *path, runtime::file_kind::regular, work);
      !inspected)
        co_return runtime::failure(inspected.error());
    auto opened = co_await files.open(
      *path, {.close_policy = runtime::file_close_policy::checked});
    if (!opened) co_return runtime::failure(opened.error());
    auto data = std::move(*opened);
    runtime::first_failure failed;
    std::optional<local_footer_reference> pinned;
    try {
        auto read = co_await detail::pin_durable_footer(
          data, boundary, limits, work);
        if (!read)
            failed.observe(read);
        else
            pinned.emplace(*read);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        failed.observe(co_await data.close());
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (failed.failed()) {
        auto error = failed.outcome();
        co_return runtime::failure(error.error());
    }
    co_return *pinned;
}

// Reads every footer a cut still needs, one at a time in segment order, and
// installs its pin. The first failure stops it and leaves the cut incomplete:
// a checkpoint is never published from an incomplete cut. A footer that no
// longer reads back as written is corruption found while its WAL copy still
// exists.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<runtime::result<void>> pin_durable_boundaries(
  Backend& files,
  Owner& ownership,
  std::span<const local_device_spec> devices,
  std::uint32_t shard,
  checkpoint_capture& cut,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work) {
    while (!cut.complete()) {
        if (auto ready = work.poll(); !ready)
            co_return runtime::failure(
              detail::path_error(ready.error().code()));
        auto pinned = co_await read_durable_boundary(
          files,
          ownership,
          devices,
          shard,
          cut.refresh()[cut.installed()],
          budget,
          limits,
          work);
        if (!pinned) co_return runtime::failure(pinned.error());
        if (auto installed = cut.install(*pinned); !installed)
            co_return runtime::failure(installed.error());
    }
    co_return runtime::result<void>{};
}

namespace detail {
// Admits and reserves the table of the root a reopen loaded.
[[nodiscard]] runtime::result<loaded_checkpoint> admit_checkpoint_table(
  workload_budget& budget,
  const local_root_reference& reference,
  const local_checkpoint_root& root);
// One page's entries from its exact bytes, under the reference the root pins
// it with and after the entry the page before it ended with.
[[nodiscard]] seastar::future<
  runtime::result<std::vector<local_checkpoint_entry>>>
decode_checkpoint_page(
  bytes::fragmented_buffer raw,
  local_store_context owner,
  storage_alignment alignment,
  local_object_sequence sequence,
  page_ref reference,
  std::optional<local_checkpoint_entry> previous,
  local_store_io_limits limits,
  codec::cooperative_work& work);
} // namespace detail

// Loads the checkpoint the control's head names; nothing when the store has
// none. The caller selected `control` through the control owner, which
// confirms the record durable before anything depends on it. A missing or
// damaged root or page fails: there is no older checkpoint, scan from the
// chain's origin or empty table to fall back to. Reads only.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<runtime::result<std::optional<loaded_checkpoint>>>
load_checkpoint(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  std::uint32_t shard,
  const local_shard_control& control,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work) {
    using output = std::optional<loaded_checkpoint>;
    if (!control.checkpoint) co_return output{};
    const auto reference = *control.checkpoint;
    const auto owner = spec.shard_owner(shard);
    if (!owner || !spec.controls())
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    if (auto valid = co_await ownership.validate(spec); !valid)
        co_return runtime::failure(valid.error());
    auto root = co_await load_local_checkpoint_root(
      files, spec, shard, reference, budget, limits, work);
    if (!root) co_return runtime::failure(root.error());
    const auto& fields = std::get<local_checkpoint_root>(root->value.payload());
    auto loaded = detail::admit_checkpoint_table(budget, reference, fields);
    if (!loaded) co_return runtime::failure(loaded.error());
    if (fields.pages.empty()) {
        if (fields.entry_count != 0)
            co_return runtime::failure(
              detail::path_error(errc::malformed_data));
        co_return output{std::move(*loaded)};
    }
    const auto paths = local_paths::make(spec.root);
    if (!paths) co_return runtime::failure(paths.error());
    const auto path = paths->sequence_file(
      shard, local_sequence_file::checkpoint, reference.sequence().value());
    if (!path) co_return runtime::failure(path.error());
    auto handle = budget.try_reserve(limits.execution_bytes);
    if (!handle) co_return runtime::failure(handle.error());
    if (auto held = handle->try_acquire_handles(1); !held)
        co_return runtime::failure(held.error());
    auto opened = co_await files.open(
      *path, {.close_policy = runtime::file_close_policy::checked});
    if (!opened) co_return runtime::failure(opened.error());
    auto bundle = std::move(*opened);
    runtime::first_failure failed;
    try {
        auto position = runtime::file_position{reference.bytes().value()};
        std::optional<local_checkpoint_entry> previous;
        for (const auto& page : fields.pages) {
            auto raw = co_await detail::read_local_extent(
              bundle, position, page.encoded_bytes(), work);
            if (!raw) {
                failed.observe(raw);
                break;
            }
            auto entries = co_await detail::decode_checkpoint_page(
              std::move(*raw),
              *owner,
              spec.identity.metadata_alignment,
              reference.sequence(),
              page,
              previous,
              limits,
              work);
            if (!entries) {
                failed.observe(entries);
                break;
            }
            for (auto& entry : *entries)
                loaded->entries.push_back(std::move(entry));
            previous = loaded->entries.back();
            const auto next = position.checked_add(page.encoded_bytes());
            if (!next) {
                failed.observe(detail::path_error(errc::out_of_range));
                break;
            }
            position = *next;
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        failed.observe(co_await bundle.close());
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (failed.failed()) {
        auto error = failed.outcome();
        co_return runtime::failure(error.error());
    }
    if (loaded->entries.size() != fields.entry_count)
        co_return runtime::failure(detail::path_error(errc::malformed_data));
    co_return output{std::move(*loaded)};
}

// At reopen: removes every checkpoint bundle of the shard but the one the
// head names. A bundle below the head was replaced; one above it was
// published by a run whose head update never became durable. Nothing else
// references a bundle, and sequences are never used twice, so none of them
// can become the head. The head is read from the open control owner, which
// confirmed its record durable, at the moment of the sweep: a copy taken
// earlier would spare a bundle that is no longer the head and remove the one
// that is. Temporaries and anything unknown are left for their own owner.
// Returns how many bundles were removed; the first removal that fails is
// returned instead. Like every removal at reopen, it follows a ready plan.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<runtime::result<std::uint32_t>> remove_stale_checkpoints(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  std::uint32_t shard,
  const local_control_owner<Backend, Owner>& head,
  codec::cooperative_work& work) {
    if (!spec.shard_owner(shard) || !spec.controls())
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    const auto current = head.snapshot();
    if (!current) co_return runtime::failure(current.error());
    // Kept by value: the sweep suspends, and the head it spares is the one
    // that stood when it began. No run can replace it meanwhile, since the
    // owner that would is made after a reopen has finished.
    const auto kept = current->fields.checkpoint;
    if (auto valid = co_await ownership.validate(spec); !valid)
        co_return runtime::failure(valid.error());
    const auto paths = local_paths::make(spec.root);
    if (!paths) co_return runtime::failure(paths.error());
    const auto first = paths->sequence_file(
      shard, local_sequence_file::checkpoint, 1);
    if (!first) co_return runtime::failure(first.error());
    const auto directory = runtime::file_path::make(
      first->value().substr(0, first->value().rfind('/')));
    if (!directory) co_return runtime::failure(directory.error());
    if (
      auto inspected = co_await inspect_local_path(
        files, spec.root, *directory, runtime::file_kind::directory, work);
      !inspected)
        co_return runtime::failure(inspected.error());
    auto opened = co_await files.open_directory(
      *directory, runtime::file_close_policy::checked);
    if (!opened) co_return runtime::failure(opened.error());
    auto cursor = std::move(*opened);
    std::uint32_t removed = 0;
    runtime::first_failure failed;
    try {
        bool end = false;
        while (!end && !failed.failed()) {
            if (auto ready = co_await detail::path_checkpoint(work); !ready) {
                failed.observe(ready);
                break;
            }
            auto page = co_await cursor.next(
              {.maximum_entries = item_count{16},
               .maximum_name_bytes = byte_count{4_KiB}});
            if (!page) {
                failed.observe(page);
                break;
            }
            end = page->end();
            for (const auto& entry : page->entries()) {
                const auto sequence = parse_local_sequence_name(
                  entry.name.value(), false);
                if (
                  !sequence || entry.kind != runtime::file_kind::regular
                  || (kept && kept->sequence().value() == *sequence))
                    continue;
                auto path = local_child_path(*directory, entry.name);
                if (!path) {
                    failed.observe(path);
                    break;
                }
                auto gone = co_await files.remove_file(*path);
                if (!gone && gone.error().code() != errc::not_found) {
                    failed.observe(gone);
                    break;
                }
                ++removed;
            }
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        failed.observe(co_await cursor.close());
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (failed.failed()) {
        auto error = failed.outcome();
        co_return runtime::failure(error.error());
    }
    co_return removed;
}

struct checkpoint_limits final {
    checkpoint_pin_limits pins{};
    local_store_io_limits metadata{};
    [[nodiscard]] runtime::result<void> validate() const noexcept;
};

// The entries one checkpoint page carries: as many as its encoded size
// allows, and no more than a reader held to the same limits can decode.
// Decoding admits a page's entries as one array against the metadata budget,
// which also holds the page's own descriptors, and against the operation
// budget, which holds the page twice besides. So the array takes at most half
// of the first and a quarter of the second. The page is cut by the budget its
// reader has, not by the largest page the format can express; limits too
// small for one entry are refused.
[[nodiscard]] runtime::result<std::uint32_t> checkpoint_page_entries(
  const local_store_io_limits& limits,
  storage_alignment alignment,
  const codec::limits& policy) noexcept;

// What one checkpoint run did.
struct checkpoint_outcome final {
    // Where the durable checkpoint ends after the run: the WAL below it is
    // no longer needed.
    local_wal_cursor end;
    // False when the cutoff could not move: nothing was written.
    bool published{false};
    // Entries the published checkpoint carries.
    std::uint32_t entries{0};
    // Earlier bundles whose removal is still owed. One that survives is
    // referenced by nothing.
    std::uint32_t unretired{0};
    // WAL files below the cutoff's file removed by this run, and those still
    // there: debt the next run tries again. Removal is never reported as
    // durable; space is counted again from the surviving names at start.
    std::uint32_t wal_removed{0}, wal_owed{0};
    // Why the oldest of the files still there could not be removed.
    std::optional<runtime::operation_error> wal_failed;
};

// Keeps the cutoff where it is for as long as it lives. A WAL scan takes one
// for its duration, since it reads from the cutoff, and so does a recovered
// seal from the pass that classifies its suffix plan until its seal is
// published: that plan is identified from the cutoff. The owner outlives it.
class checkpoint_hold final {
public:
    checkpoint_hold(checkpoint_hold&& other) noexcept
      : holds_(std::exchange(other.holds_, nullptr))
      , from_(other.from_) {}
    checkpoint_hold& operator=(checkpoint_hold&&) noexcept = delete;
    checkpoint_hold(const checkpoint_hold&) = delete;
    checkpoint_hold& operator=(const checkpoint_hold&) = delete;
    ~checkpoint_hold() {
        if (holds_) --*holds_;
    }
    // The cutoff it holds: where a scan starts. Absent before the first
    // checkpoint, when the whole chain is still needed.
    [[nodiscard]] std::optional<local_wal_cursor> from() const noexcept {
        return from_;
    }

private:
    template<
      runtime::file_system_backend Backend,
      local_directory_owner Owner,
      runtime::monotonic_clock Clock>
    friend class local_checkpoint;
    checkpoint_hold(
      std::uint32_t& holds, std::optional<local_wal_cursor> from) noexcept
      : holds_(&holds)
      , from_(from) {
        ++holds;
    }
    std::uint32_t* holds_;
    std::optional<local_wal_cursor> from_;
};

// One shard's checkpoints. A run takes the cutoff from the append owner's
// obligations, pins each unsealed segment's newest durable footer, publishes
// the table as one immutable bundle and then replaces the control's
// checkpoint head. Nothing is flushed for it: every reported append already
// made its segment bytes durable, so a run costs one bundle and one control
// replacement whatever the number of segments.
//
// One run is in flight at a time, and requests made meanwhile are served
// together by one run after it. Whatever the old checkpoint alone kept stays
// until the new head is durable: only then does the cut become the pin
// table and the previous bundle go. A run that fails changes nothing durable
// that anything depends on. Then the WAL files below the cutoff's file go,
// oldest first. Removing a file or an old bundle waits for no directory sync:
// one that reappears after a crash is below the cutoff or the head,
// referenced by nothing, and removed again at the next open.
// An uncertain head update fences the control, and this owner with it.
//
// Runs are asked for, never timed. request() is an explicit demand, and a
// graceful stop is one made after the appends have drained: an ordinary
// checkpoint that ends at the WAL-durable end, so the next start scans
// nothing, and never a flag that skips recovery. Once bind_retention() is
// called, the append owner asks too: when a closed WAL file may go, and when
// a rotation meets the retained-WAL limit.
//
// The owner binds itself to the append owner when it is made, one per append
// owner. From then on a detaching segment's last footer reaches the pin
// table in the detach itself, and a discard decision releases its PREPARE
// only once hold() has been given it, so neither is left to call order.
//
// Every provider outlives joined close(), and close() precedes closing them.
template<
  runtime::file_system_backend Backend,
  local_directory_owner Owner,
  runtime::monotonic_clock Clock>
class local_checkpoint final : public runtime::shard_affine {
public:
    using control_type = local_control_owner<Backend, Owner>;
    using allocator_type = local_id_allocator<Backend, Owner>;
    using append_type = local_append<Backend, Owner, Clock>;
    using output = runtime::result<checkpoint_outcome>;

    // `devices` are the stores that can hold a pinned segment, and `control`
    // the one that holds this shard's control and checkpoints. `origin` is
    // where the unreclaimed WAL begins while no checkpoint exists: a cursor
    // in the oldest WAL file the shard holds. It is the first checkpoint's
    // beginning, so one that lies in a later file is refused. `devices` is
    // borrowed until joined close().
    [[nodiscard]] static runtime::result<std::unique_ptr<local_checkpoint>>
    make(
      Backend& files,
      Owner& ownership,
      std::span<const local_device_spec> devices,
      local_device_spec control,
      std::uint32_t shard,
      control_type& head,
      allocator_type& ids,
      append_type& append,
      workload_budget& budget,
      checkpoint_limits limits,
      codec::limits policy,
      local_wal_cursor origin) {
        if (auto valid = limits.validate(); !valid)
            return runtime::failure(valid.error());
        if (auto valid = validate_local_device_spec(control); !valid)
            return runtime::failure(valid.error());
        const auto owner = control.shard_owner(shard);
        if (!control.controls() || !owner)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        if (auto free = append.can_bind_checkpointer(); !free)
            return runtime::failure(free.error());
        if (!head.checkpoint_end()) {
            const auto oldest = append.retained_wal().oldest();
            if (!oldest || origin.incarnation() != *oldest)
                return runtime::failure(
                  detail::path_error(errc::invalid_argument));
        }
        const auto capacity = checkpoint_page_entries(
          limits.metadata, control.identity.metadata_alignment, policy);
        if (!capacity) return runtime::failure(capacity.error());
        // The table is bounded by its limits, and a full table must still
        // fit one bundle's pages: otherwise every run would fail once it
        // grew past them, and the WAL would never be reclaimed.
        if (
          std::uint64_t{*capacity} * maximum_object_pages
          < std::uint64_t{limits.pins.segments} + limits.pins.decisions)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        auto pins = checkpoint_pins::make(budget, limits.pins);
        if (!pins) return runtime::failure(pins.error());
        auto instance = budget.allocation_charge(
          byte_count{sizeof(local_checkpoint)});
        if (!instance) return runtime::failure(instance.error());
        auto held = budget.try_reserve(
          byte_count{
            instance->value() + limits.metadata.execution_bytes.value()});
        if (!held) return runtime::failure(held.error());
        std::unique_ptr<local_checkpoint> made{new local_checkpoint(
          files,
          ownership,
          devices,
          std::move(control),
          shard,
          *owner,
          *capacity,
          head,
          ids,
          append,
          budget,
          limits,
          policy,
          origin,
          std::move(*pins),
          std::move(*held))};
        // Checked above, and nothing ran since: the append owner now hands
        // this owner what a cut must carry as it happens.
        const auto bound = append.bind_checkpointer(
          local_checkpointer{
            [self = made.get()](
              const local_durable_boundary& boundary) noexcept {
                return self->take_boundary(boundary);
            },
            [self = made.get()](
              const local_recovery_decision& decision, bool released) noexcept {
                return self->pins_.discharge(decision, released);
            },
            {&budget, &head.budget()},
            [self = made.get()]() noexcept { self->demand(); },
            [self = made.get()]() noexcept {
                try {
                    return self->relieve();
                } catch (...) {
                    return seastar::current_exception_as_future<
                      runtime::result<void>>();
                }
            }});
        KWAQUE_INVARIANT(
          invariant_id{"KQ-CHECKPOINT-BOUND"},
          bound.has_value(),
          "an append owner refused the checkpoint owner it had room for");
        return made;
    }
    local_checkpoint(const local_checkpoint&) = delete;
    local_checkpoint& operator=(const local_checkpoint&) = delete;
    ~local_checkpoint() {
        assert_current();
        KWAQUE_INVARIANT(
          invariant_id{"KQ-CHECKPOINT-CLOSED"},
          closed_ && holds_ == 0,
          "checkpoint owner destroyed before joined close or with a hold");
    }

    // Where the durable checkpoint ends; absent before the first.
    [[nodiscard]] std::optional<local_wal_cursor> end() const noexcept {
        assert_current();
        return head_.checkpoint_end();
    }
    [[nodiscard]] const checkpoint_pins& pins() const& noexcept {
        assert_current();
        return pins_;
    }
    const checkpoint_pins& pins() const&& = delete;
    [[nodiscard]] bool running() const noexcept {
        assert_current();
        return running_;
    }
    // Scans and pending recovered seals holding the cutoff.
    [[nodiscard]] std::uint32_t holds() const noexcept {
        assert_current();
        return holds_;
    }
    // The first failure that ends this owner: a fenced control.
    [[nodiscard]] const runtime::first_failure& failure() const& noexcept {
        assert_current();
        return first_;
    }
    const runtime::first_failure& failure() const&& = delete;

    // The pin table's inputs; see checkpoint_pins.
    [[nodiscard]] runtime::result<void>
    observe(const local_durable_boundary& boundary) {
        assert_current();
        if (closing_ || closed_) return runtime::failure(error(errc::closed));
        return pins_.observe(boundary);
    }
    [[nodiscard]] runtime::result<void>
    hold(const recovery_decision_record& decision) {
        assert_current();
        if (closing_ || closed_) return runtime::failure(error(errc::closed));
        return pins_.hold(decision);
    }
    [[nodiscard]] runtime::result<void>
    cover(const local_object_publication& publication) {
        assert_current();
        if (closing_ || closed_) return runtime::failure(error(errc::closed));
        return pins_.cover(publication);
    }

    // After a restart, before the first request: recovery's publications
    // are durable. Each recovered segment's boundary is then pinned by its
    // own publication, which is what lets a cut pass the WAL that recovery
    // read; until this is called on a restarted shard, every run is refused.
    // An incomplete set is refused.
    [[nodiscard]] runtime::result<void>
    recovered(const recovered_publications& published) {
        assert_current();
        if (closing_ || closed_) return runtime::failure(error(errc::closed));
        if (!published.complete)
            return runtime::failure(error(errc::wrong_context));
        recovered_ = true;
        return {};
    }
    // The segment whose newest durable footer the latest run could not read
    // back and pin. Until that segment's own publication covers it, no cut
    // can be taken, so the WAL below it stays. Absent once a run pinned
    // every footer.
    [[nodiscard]] const std::optional<segment_context>&
    unpinned() const& noexcept {
        assert_current();
        return unpinned_;
    }
    const std::optional<segment_context>& unpinned() const&& = delete;

    // After a restart, before the first request: the pin table continues
    // from the checkpoint the control's head names, as a reopen loaded it.
    // A run is refused while the table is not the one the head's checkpoint
    // carries, so a restart that skips this publishes nothing.
    [[nodiscard]] runtime::result<void> restore(loaded_checkpoint&& durable) {
        assert_current();
        if (closing_ || closed_) return runtime::failure(error(errc::closed));
        const auto current = head_.snapshot();
        if (!current) return runtime::failure(current.error());
        if (
          running_ || current->fields.checkpoint != durable.reference
          || head_.checkpoint_end() != durable.end)
            return runtime::failure(error(errc::wrong_context));
        return pins_.restore(std::move(durable));
    }

    // Holds the cutoff. It is refused while a run is replacing the head:
    // the cutoff is then moving, and the caller asks again.
    [[nodiscard]] runtime::result<checkpoint_hold> hold_cutoff() {
        assert_current();
        if (closing_ || closed_ || first_.failed())
            return runtime::failure(error(errc::closed));
        if (publishing_) return runtime::failure(error(errc::queue_full));
        return checkpoint_hold{holds_, head_.checkpoint_end()};
    }

    // Runs a checkpoint, or joins the one that follows the run in flight.
    // The run takes its cut before this returns, in the caller's scheduling
    // group.
    [[nodiscard]] seastar::future<output> request() {
        assert_current();
        return ask(false);
    }

    // Lets this owner reclaim the append owner's WAL on its own. From then
    // on a run starts when the oldest obligation leaves a closed WAL file,
    // and a rotation that meets the WAL writer's retained limit waits for
    // one run before it is refused. Neither is a timer, and a run that
    // cannot move the cutoff writes nothing.
    //
    // A run is funded by the budget this owner was made with and, for its
    // head update, by the control owner's. Neither may share admission with
    // anything the appends draw on: with tasks, bytes and handle credits
    // that appends can exhaust, the one thing that frees their WAL could
    // not run while they wait for it. The append owner checks that, here
    // and for every segment attached later. close() ends it.
    [[nodiscard]] runtime::result<void> bind_retention() {
        assert_current();
        if (closing_ || closed_ || first_.failed())
            return runtime::failure(error(errc::closed));
        return append_.start_reclaiming();
    }

    // Stops taking requests, lets the runs already asked for finish, and
    // joins them.
    [[nodiscard]] seastar::future<runtime::result<void>> close() {
        assert_current();
        if (closed_) co_return runtime::result<void>{};
        closing_ = true;
        // The append owner asks for nothing more. A rotation waiting for a
        // run already asked for gets its answer when that run is joined.
        append_.unbind_checkpointer();
        try {
            co_await tasks_.close();
        } catch (...) {
            first_.observe(std::current_exception());
        }
        closed_ = true;
        co_return runtime::result<void>{};
    }

private:
    local_checkpoint(
      Backend& files,
      Owner& ownership,
      std::span<const local_device_spec> devices,
      local_device_spec control,
      std::uint32_t shard,
      local_store_context owner,
      std::uint32_t capacity,
      control_type& head,
      allocator_type& ids,
      append_type& append,
      workload_budget& budget,
      checkpoint_limits limits,
      codec::limits policy,
      local_wal_cursor origin,
      checkpoint_pins pins,
      workload_reservation held)
      : held_(std::move(held))
      , files_(files)
      , ownership_(ownership)
      , devices_(devices)
      , control_(std::move(control))
      , shard_(shard)
      , owner_(owner)
      , capacity_(capacity)
      , head_(head)
      , ids_(ids)
      , append_(append)
      , budget_(budget)
      , limits_(limits)
      , origin_(origin)
      , pins_(std::move(pins))
      , execution_(policy, abort_)
      , tasks_([this](std::exception_ptr failure) noexcept {
          first_.observe(std::move(failure));
      }) {}

    static runtime::operation_error error(errc code) noexcept {
        return detail::path_error(code);
    }
    // A detaching segment's final footer, from the append owner.
    runtime::result<void>
    take_boundary(const local_durable_boundary& boundary) noexcept {
        try {
            return pins_.observe(boundary);
        } catch (...) {
            return runtime::failure(error(errc::resource_exhausted));
        }
    }

    // request(), or with `apart` the same in this owner's budget's group:
    // see start().
    seastar::future<output> ask(bool apart) {
        if (closing_ || closed_ || first_.failed())
            return seastar::make_ready_future<output>(
              runtime::failure(error(errc::closed)));
        if (running_) {
            if (!next_) next_.emplace();
            return next_->get_shared_future();
        }
        serving_.emplace();
        auto served = serving_->get_shared_future();
        try {
            if (auto started = start(apart); !started)
                return seastar::make_ready_future<output>(
                  runtime::failure(started.error()));
        } catch (...) {
            return seastar::current_exception_as_future<output>();
        }
        return served;
    }

    // Starts the run that serves serving_. A run asked for explicitly takes
    // its cut before this returns, in the caller's scheduling group. One the
    // append path asked for runs `apart`, in this owner's budget's group, so
    // it is neither scheduled nor accounted as an append. A request that
    // joins a run in flight is served by the run after it, in whichever
    // group that first run started in.
    runtime::result<void> start(bool apart) {
        // Set first: the run may finish before spawn returns.
        running_ = true;
        runtime::result<void> spawned;
        try {
            spawned = tasks_.spawn([this, apart] {
                if (!apart) return serve();
                return seastar::with_scheduling_group(
                  budget_.scheduling_group(), [this] { return serve(); });
            });
        } catch (...) {
            serving_.reset();
            running_ = false;
            throw;
        }
        if (!spawned) {
            serving_.reset();
            running_ = false;
        }
        return spawned;
    }
    // A run nobody waits for: now, or after the one in flight. What it did
    // shows in end() and wal(), and in the append owner's retained count.
    void demand() noexcept {
        if (closing_ || closed_ || first_.failed()) return;
        try {
            if (running_) {
                if (!next_) next_.emplace();
                return;
            }
            serving_.emplace();
            // A scope that cannot take the run has put this back already.
            if (const auto started = start(true); !started) return;
        } catch (...) {
        }
    }
    // One run for a rotation held at the retained limit, which waits for
    // it. The run is started inside this owner's scope, so close() joins it,
    // and nothing here touches the owner after the wait. A failure is why a
    // file that could go is still there.
    seastar::future<runtime::result<void>> relieve() {
        const auto ran = co_await ask(true);
        if (!ran) co_return runtime::failure(ran.error());
        if (ran->wal_owed != 0)
            co_return runtime::failure(
              ran->wal_failed.value_or(error(errc::io_failure)));
        co_return runtime::result<void>{};
    }

    // Serves the requests of one run, then of the run that follows it,
    // until no request arrived during a run.
    seastar::future<> serve() {
        while (serving_) {
            output outcome = runtime::failure(error(errc::io_failure));
            std::exception_ptr thrown;
            try {
                outcome = co_await run();
            } catch (...) {
                thrown = std::current_exception();
            }
            publishing_ = false;
            auto served = std::move(*serving_);
            serving_.reset();
            if (next_) {
                serving_.emplace(std::move(*next_));
                next_.reset();
            } else {
                running_ = false;
            }
            if (thrown)
                served.set_exception(std::move(thrown));
            else
                served.set_value(std::move(outcome));
        }
    }

    seastar::future<output> run() {
        if (first_.failed()) co_return runtime::failure(error(errc::closed));
        // The cut, in one synchronous step: the cutoff, each attachment's
        // newest durable footer and the table as it stands.
        const auto obligations = append_.obligations(
          [](const local_obligation&) {});
        if (!obligations) co_return runtime::failure(obligations.error());
        const auto previous = head_.snapshot();
        if (!previous) co_return runtime::failure(previous.error());
        // A cut is published as the successor of the head's checkpoint and
        // retires it, so the table must be the one that checkpoint carries.
        // And on a restarted shard nothing is cut before recovery's
        // publications cover the WAL it read.
        if (
          previous->fields.checkpoint != pins_.continues()
          || (append_.wal_recovered() && !recovered_))
            co_return runtime::failure(error(errc::wrong_context));
        const auto begin = head_.checkpoint_end().value_or(origin_);
        std::optional<local_wal_cursor> held;
        if (holds_ != 0) held = begin;
        const auto end = checkpoint_cutoff(owner_, *obligations, held, begin);
        if (!end) co_return runtime::failure(end.error());
        runtime::first_failure observed;
        append_.durable_boundaries(
          [this, &observed](const local_durable_boundary& boundary) {
              observed.observe(pins_.observe(boundary));
          });
        if (observed.failed())
            co_return runtime::failure(observed.outcome().error());
        // The cutoff stands still: what the durable checkpoint pins already
        // covers everything below it. Removals still owed are tried again.
        if (*end == begin) {
            co_await settle_debts();
            checkpoint_outcome idle{begin, false, 0, unretired()};
            co_await reclaim(idle);
            co_return idle;
        }
        auto cut = pins_.capture(owner_, *end);
        if (!cut) co_return runtime::failure(cut.error());
        auto pinned = co_await pin_durable_boundaries(
          files_,
          ownership_,
          devices_,
          shard_,
          *cut,
          budget_,
          limits_.metadata,
          execution_);
        if (!pinned) {
            // Footers are pinned in order; the first one left is the one
            // that could not be read back.
            if (cut->installed() < cut->refresh().size())
                unpinned_.emplace(
                  cut->refresh()[cut->installed()].history.segment);
            co_return runtime::failure(pinned.error());
        }
        unpinned_.reset();

        auto sequence = co_await ids_.allocate_object(execution_);
        if (!sequence) co_return runtime::failure(sequence.error());
        detail::checkpoint_pages pages{
          &cut->entries(),
          owner_,
          control_.identity.metadata_alignment,
          *sequence,
          capacity_,
          limits_.metadata};
        auto root = co_await detail::encode_checkpoint_root(
          pages, begin, *end, execution_);
        if (!root) co_return runtime::failure(root.error());
        const auto reference = root->reference;
        auto bundle = co_await local_bundle::make(
          reference,
          std::move(root->expected),
          std::move(root->bytes),
          budget_,
          limits_.metadata,
          execution_);
        if (!bundle) co_return runtime::failure(bundle.error());
        // Every pinned footer was read back above: the file may be exposed.
        auto published = co_await publish_local_bundle(
          files_,
          ownership_,
          control_,
          shard_,
          std::move(*bundle),
          pages,
          [](codec::cooperative_work&) {
              return seastar::make_ready_future<runtime::result<void>>(
                runtime::result<void>{});
          },
          budget_,
          execution_);
        if (!published.reference) {
            // Its name may be there all the same, as after a rename whose
            // directory sync failed; nothing will ever reference it.
            owe(reference.sequence());
            if (!published.publication.failure.failed())
                co_return runtime::failure(error(errc::io_failure));
            co_return runtime::failure(
              published.publication.failure.outcome().error());
        }

        // The head moves only now that its bundle is durable, and only from
        // the head this cut was taken under. A hold taken meanwhile wins.
        const auto replaced = previous->fields.checkpoint;
        auto updated = co_await head_.update(
          [this, replaced, reference](
            local_shard_control& fields) -> runtime::result<void> {
              if (fields.checkpoint != replaced)
                  return runtime::failure(error(errc::wrong_context));
              if (holds_ != 0) return runtime::failure(error(errc::queue_full));
              fields.checkpoint = reference;
              publishing_ = true;
              return {};
          },
          [](
            const local_control_snapshot&,
            const local_shard_control&,
            codec::cooperative_work&) {
              return seastar::make_ready_future<runtime::result<void>>(
                runtime::result<void>{});
          },
          execution_);
        publishing_ = false;
        if (
          updated.failure.failed()
          || updated.disposition != local_publication_disposition::durable) {
            if (head_.fenced()) first_.observe(error(errc::closed));
            // An update that touched nothing leaves the bundle referenced by
            // nothing, so it goes; after anything else it may be the head.
            if (
              updated.disposition == local_publication_disposition::untouched
              && !head_.fenced() && !co_await retire(reference.sequence()))
                owe(reference.sequence());
            if (!updated.failure.failed())
                co_return runtime::failure(error(errc::io_failure));
            co_return runtime::failure(updated.failure.outcome().error());
        }

        // Durable: the cut is the table, and what only the previous
        // checkpoint kept may go.
        const auto entries = static_cast<std::uint32_t>(cut->entries().size());
        if (auto adopted = pins_.adopt(std::move(*cut)); !adopted)
            first_.observe(adopted.error());
        else
            pins_.continue_from(reference);
        if (replaced) owe(replaced->sequence());
        co_await settle_debts();
        checkpoint_outcome done{*end, true, entries, unretired()};
        co_await reclaim(done);
        co_return done;
    }

    // Removes the WAL files below the file the durable cutoff lies in. It
    // runs only after the head that no longer needs them is durable, and a
    // hold keeps that head where a scan reads from, so no scan reads a file
    // this removes. The files are the WAL writer's own names, and each
    // removal is taken off them as it happens.
    seastar::future<> reclaim(checkpoint_outcome& run) {
        const auto cutoff = head_.checkpoint_end();
        if (!cutoff) co_return;
        const auto reclaimed = co_await reclaim_wal_prefix(
          files_, control_, shard_, *cutoff, append_);
        run.wal_removed = reclaimed.removed;
        run.wal_owed = reclaimed.owed;
        run.wal_failed = reclaimed.failed;
    }

    // Bundles whose removal is owed, oldest first. The table is fixed: when
    // it is full the oldest debt is dropped, and the next open removes every
    // bundle below the head anyway.
    void owe(local_object_sequence sequence) noexcept {
        if (debts_ == debt_.size()) {
            std::move(debt_.begin() + 1, debt_.end(), debt_.begin());
            --debts_;
        }
        debt_[debts_++] = sequence.value();
    }
    [[nodiscard]] std::uint32_t unretired() const noexcept { return debts_; }
    seastar::future<> settle_debts() {
        std::uint32_t kept = 0;
        const auto owed = debts_;
        for (std::uint32_t i = 0; i != owed; ++i) {
            const auto sequence = local_object_sequence::make(debt_[i]);
            if (sequence && !co_await retire(*sequence))
                debt_[kept++] = debt_[i];
        }
        debts_ = kept;
    }
    // Unlinks one checkpoint bundle, with no directory sync. True when it
    // is gone.
    seastar::future<bool> retire(local_object_sequence sequence) {
        const auto paths = local_paths::make(control_.root);
        if (!paths) co_return false;
        const auto path = paths->sequence_file(
          shard_, local_sequence_file::checkpoint, sequence.value());
        if (!path) co_return false;
        try {
            auto removed = co_await files_.remove_file(*path);
            co_return removed.has_value()
              || removed.error().code() == errc::not_found;
        } catch (...) {
            co_return false;
        }
    }

    // Declared first: released after everything it admitted.
    workload_reservation held_;
    Backend& files_;
    Owner& ownership_;
    std::span<const local_device_spec> devices_;
    local_device_spec control_;
    std::uint32_t shard_;
    local_store_context owner_;
    std::uint32_t capacity_;
    control_type& head_;
    allocator_type& ids_;
    append_type& append_;
    workload_budget& budget_;
    checkpoint_limits limits_;
    local_wal_cursor origin_;
    checkpoint_pins pins_;
    // A run never uses a caller's abort source.
    seastar::abort_source abort_;
    codec::cooperative_work execution_;
    // The requests of the run in flight, and of the one that follows it.
    std::optional<seastar::shared_promise<output>> serving_, next_;
    std::array<std::uint64_t, 8> debt_{};
    std::uint32_t debts_{0}, holds_{0};
    runtime::first_failure first_;
    std::optional<segment_context> unpinned_;
    bool running_{false}, publishing_{false}, closing_{false}, closed_{false};
    // Recovery's publications were handed over; see recovered().
    bool recovered_{false};
    // The runs; joined by close().
    runtime::task_scope tasks_;
};

} // namespace kwaque::storage
