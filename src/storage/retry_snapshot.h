#pragma once

#include "src/base/invariant.h"
#include "src/base/units.h"
#include "src/runtime/task_scope.h"
#include "src/storage/checkpoint.h"
#include "src/storage/local_append.h"
#include "src/storage/local_bundle.h"
#include "src/storage/local_id_allocator.h"
#include "src/storage/retry_lookup.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_vector.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_future.hh>

#include <algorithm>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <vector>

namespace kwaque::storage {

// The format's bounds on one snapshot: its root pins at most this many pages
// and facts.
inline constexpr std::uint32_t maximum_retry_snapshot_pages = 256;
inline constexpr std::uint32_t maximum_retry_snapshot_facts = 65536;

// The facts one snapshot page carries: as many as its encoded size allows,
// and no more than a reader held to the same limits can decode. Decoding
// admits a page's facts as one array, so the page is cut by the budget its
// reader has; limits too small for one fact are refused.
[[nodiscard]] runtime::result<std::uint32_t> retry_snapshot_page_entries(
  const local_store_io_limits& limits,
  storage_alignment alignment,
  const codec::limits& policy) noexcept;

// Whether a demand cuts a snapshot. Nothing new means no cut, forced or not.
// A demand that is not forced cuts only once the facts no snapshot holds are
// at least as many as those the durable one does. Every cut writes all the
// facts again, so cutting at each doubling writes about twice the facts in
// total; cutting for every fact would write a number that grows with their
// square.
[[nodiscard]] constexpr bool retry_cut_due(
  std::uint32_t saved, std::uint32_t recorded, bool forced) noexcept {
    if (recorded <= saved) return false;
    return forced || recorded - saved >= saved;
}

// A durable snapshot merged with facts no snapshot holds yet, in batch
// identity order: the retry source of a seal, and of a snapshot that replaces
// another. The durable side is read a page at a time and the other is a
// table already in memory, so the merge keeps one page and nothing else. The
// two must share no identity; one found in both ends the read, and nothing is
// written from it. A slice can be read again and returns the same facts.
class merged_retry_source final {
public:
    // `durable` may be absent; both outlive this source.
    merged_retry_source(
      retry_root_reader* durable,
      const seastar::chunked_vector<completed_retry>* pending) noexcept
      : durable_(durable)
      , pending_(pending) {}
    // Facts the merge yields.
    [[nodiscard]] std::uint32_t completed() const noexcept {
        return (durable_ ? durable_->facts() : 0U)
               + static_cast<std::uint32_t>(pending_->size());
    }
    // The snapshot whose every fact this source yields, if it merges one.
    [[nodiscard]] std::optional<local_root_reference> merged_snapshot() const {
        if (!durable_) return std::nullopt;
        return durable_->reference();
    }
    [[nodiscard]] seastar::future<runtime::result<std::vector<completed_retry>>>
    read(std::uint32_t first, std::uint32_t count, codec::cooperative_work&);

private:
    retry_root_reader* durable_;
    const seastar::chunked_vector<completed_retry>* pending_;
    // Facts already yielded, and where each side continues.
    std::uint32_t position_{0}, page_{0}, within_{0}, taken_{0};
    std::optional<local_retry_page> loaded_;
};

namespace detail {
// Admission for a fragmented table of up to `count` facts.
[[nodiscard]] runtime::result<workload_reservation>
admit_retry_facts(workload_budget& budget, std::size_t count);

// A cut's own copy of the facts, taken in one synchronous step: facts recorded
// while the cut is written are not part of it.
struct retry_cut final {
    // Declared first: released after the table it admitted.
    workload_reservation held;
    seastar::chunked_vector<completed_retry> facts;
};
[[nodiscard]] runtime::result<retry_cut> copy_retry_facts(
  workload_budget& budget,
  const seastar::chunked_vector<completed_retry>& facts);

// Encodes a cut's facts as snapshot pages, one per call and in batch identity
// order, then nothing. Encoding is deterministic: a first pass yields the
// references the root pins, and a second pass the same bytes for the file.
struct retry_snapshot_pages final {
    const seastar::chunked_vector<completed_retry>* facts;
    local_store_context owner;
    segment_context segment;
    // Of the data device's metadata, and of the segment's data.
    storage_alignment alignment, segment_alignment;
    local_object_sequence sequence;
    // Facts one page holds.
    std::uint32_t capacity;
    local_store_io_limits limits;
    std::uint32_t first{0}, ordinal{0};
    // The reference of the page just encoded.
    std::optional<page_ref> last;
    // Set instead of `facts` when the pages come from a merge.
    merged_retry_source* merged{nullptr};

    [[nodiscard]] std::uint32_t total() const noexcept {
        return merged ? merged->completed()
                      : static_cast<std::uint32_t>(facts->size());
    }
    seastar::future<runtime::result<std::optional<bytes::fragmented_buffer>>>
    next(codec::cooperative_work& work);
};
// A cut's root with the reference that pins it and the context a reader
// independently expects.
struct encoded_retry_snapshot final {
    local_root_reference reference;
    local_metadata_expectation expected;
    bytes::fragmented_buffer bytes;
};
// The root of a snapshot cut against `footer`, a durable footer of the
// segment, over the pages a first pass of `pages` yields. `pages` is left at
// its start for the second pass.
[[nodiscard]] seastar::future<runtime::result<encoded_retry_snapshot>>
encode_retry_snapshot_root(
  retry_snapshot_pages& pages,
  local_footer_reference footer,
  codec::cooperative_work& work);
} // namespace detail

// What one demand did.
struct retry_snapshot_outcome final {
    // False when the demand was not due, or the segment has no durable footer
    // to pin a snapshot beside yet: nothing was written.
    bool cut{false};
    // The facts the segment's durable snapshot holds as the call returns.
    // After a cut that is every fact recorded before the demand was made,
    // and exactly those: one recorded while the cut was written is not in
    // it, and stays unsaved.
    std::uint32_t durable{0};
    // Facts recorded that no durable snapshot holds as the call returns. A
    // crash loses them; their owner supplies them again.
    std::uint32_t unsaved{0};
    // The durable snapshot and the footer it is pinned beside.
    std::optional<local_root_reference> snapshot;
    std::optional<local_footer_reference> boundary;
    // Facts this call wrote: what the cut cost.
    std::uint32_t written{0};
    // The snapshot this one replaced is still there. One that survives is
    // referenced by nothing.
    bool unretired{false};
};

// The completed-retry snapshots of one attached segment. A fact its owner
// supplied is held by the append owner and lost in a crash until a cut makes
// it durable: one immutable bundle beside the segment holding every fact
// recorded so far, then the segment's publication pointing at it. Nothing of
// it is on the append path or between its two barriers, and it gates no WAL
// reclamation: a fact never enters the WAL, so no WAL file is its recovery
// source.
//
// A cut happens only on demand; there is no timer and no task of its own.
// One cut is in flight at a time, and demands made meanwhile are served
// together by one cut after it, which is forced when any of them is. A demand
// therefore always returns the state of a cut taken after it was made. The
// snapshot a cut replaces is removed only after the pointer that replaces it
// is durable, with no directory sync awaited.
//
// Every provider outlives joined close(), and close() precedes closing them.
template<
  runtime::file_system_backend Backend,
  local_directory_owner Owner,
  runtime::monotonic_clock Clock>
class retry_snapshot final : public runtime::shard_affine {
public:
    using append_type = local_append<Backend, Owner, Clock>;
    using segment_type = segment_writer<Backend, Owner, Clock>;
    using allocator_type = local_id_allocator<Backend, Owner>;
    using output = runtime::result<retry_snapshot_outcome>;

    // `devices` are the stores that can hold the segment; `target` is its
    // attachment in `append`, and `segment` the owner that publishes its
    // pointer.
    [[nodiscard]] static runtime::result<std::unique_ptr<retry_snapshot>> make(
      Backend& files,
      Owner& ownership,
      std::span<const local_device_spec> devices,
      std::uint32_t shard,
      append_type& append,
      local_append_target target,
      segment_type& segment,
      allocator_type& ids,
      workload_budget& budget,
      local_store_io_limits limits,
      codec::limits policy) {
        if (auto valid = limits.validate(); !valid)
            return runtime::failure(valid.error());
        const auto device = std::find_if(
          devices.begin(), devices.end(), [&segment](const auto& spec) {
              return spec.owner.device() == segment.device();
          });
        if (device == devices.end() || !device->stores_data())
            return runtime::failure(detail::path_error(errc::wrong_context));
        const auto owner = device->shard_owner(shard);
        if (!owner)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        const auto capacity = retry_snapshot_page_entries(
          limits, device->identity.metadata_alignment, policy);
        if (!capacity) return runtime::failure(capacity.error());
        auto instance = budget.allocation_charge(
          byte_count{sizeof(retry_snapshot)});
        if (!instance) return runtime::failure(instance.error());
        auto held = budget.try_reserve(
          byte_count{instance->value() + limits.execution_bytes.value()});
        if (!held) return runtime::failure(held.error());
        return std::unique_ptr<retry_snapshot>{new retry_snapshot(
          files,
          ownership,
          devices,
          *device,
          shard,
          *owner,
          *capacity,
          append,
          target,
          segment,
          ids,
          budget,
          limits,
          policy,
          std::move(*held))};
    }
    retry_snapshot(const retry_snapshot&) = delete;
    retry_snapshot& operator=(const retry_snapshot&) = delete;
    ~retry_snapshot() {
        assert_current();
        KWAQUE_INVARIANT(
          invariant_id{"KQ-RETRY-SNAPSHOT-CLOSED"},
          closed_,
          "retry snapshot owner destroyed before joined close");
    }

    // Facts the durable snapshot holds; none before the first cut.
    [[nodiscard]] std::uint32_t durable() const noexcept {
        assert_current();
        return saved_;
    }
    [[nodiscard]] std::optional<local_root_reference>
    snapshot() const noexcept {
        assert_current();
        return snapshot_;
    }
    [[nodiscard]] bool running() const noexcept {
        assert_current();
        return running_;
    }

    // Cuts a snapshot when one is due, or joins the cut that follows the one
    // in flight. A forced demand cuts whenever a fact is unsaved.
    [[nodiscard]] seastar::future<output> request(bool forced) {
        assert_current();
        if (closing_ || closed_)
            return seastar::make_ready_future<output>(
              runtime::failure(detail::path_error(errc::closed)));
        if (running_) {
            if (!next_) next_.emplace();
            next_forced_ = next_forced_ || forced;
            return next_->get_shared_future();
        }
        serving_.emplace();
        serving_forced_ = forced;
        auto served = serving_->get_shared_future();
        // Set first: the cut may finish before spawn returns.
        running_ = true;
        runtime::result<void> spawned;
        try {
            spawned = tasks_.spawn([this] { return serve(); });
        } catch (...) {
            serving_.reset();
            running_ = false;
            return seastar::current_exception_as_future<output>();
        }
        if (!spawned) {
            serving_.reset();
            running_ = false;
            return seastar::make_ready_future<output>(
              runtime::failure(spawned.error()));
        }
        return served;
    }

    // Stops taking demands, lets the cuts already asked for finish, and
    // joins them.
    [[nodiscard]] seastar::future<runtime::result<void>> close() {
        assert_current();
        if (closed_) co_return runtime::result<void>{};
        closing_ = true;
        try {
            co_await tasks_.close();
        } catch (...) {
            failed_.observe(std::current_exception());
        }
        closed_ = true;
        co_return runtime::result<void>{};
    }

private:
    retry_snapshot(
      Backend& files,
      Owner& ownership,
      std::span<const local_device_spec> devices,
      local_device_spec device,
      std::uint32_t shard,
      local_store_context owner,
      std::uint32_t capacity,
      append_type& append,
      local_append_target target,
      segment_type& segment,
      allocator_type& ids,
      workload_budget& budget,
      local_store_io_limits limits,
      codec::limits policy,
      workload_reservation held)
      : held_(std::move(held))
      , files_(files)
      , ownership_(ownership)
      , devices_(devices)
      , device_(std::move(device))
      , shard_(shard)
      , owner_(owner)
      , capacity_(capacity)
      , append_(append)
      , target_(target)
      , segment_(segment)
      , ids_(ids)
      , budget_(budget)
      , limits_(limits)
      , execution_(policy, abort_)
      , tasks_([this](std::exception_ptr failure) noexcept {
          failed_.observe(std::move(failure));
      }) {}

    static runtime::operation_error error(errc code) noexcept {
        return detail::path_error(code);
    }

    // Serves the demands of one cut, then of the cut that follows it, until
    // no demand arrived during a cut.
    seastar::future<> serve() {
        while (serving_) {
            output outcome = runtime::failure(error(errc::io_failure));
            std::exception_ptr thrown;
            try {
                outcome = co_await run(serving_forced_);
            } catch (...) {
                thrown = std::current_exception();
            }
            auto served = std::move(*serving_);
            serving_.reset();
            if (next_) {
                serving_.emplace(std::move(*next_));
                serving_forced_ = next_forced_;
                next_.reset();
                next_forced_ = false;
            } else {
                running_ = false;
            }
            if (thrown)
                served.set_exception(std::move(thrown));
            else
                served.set_value(std::move(outcome));
        }
    }

    [[nodiscard]] retry_snapshot_outcome
    state(bool cut, std::uint32_t recorded, std::uint32_t written) const {
        return {
          cut,
          saved_,
          recorded - std::min(recorded, saved_),
          snapshot_,
          boundary_,
          written,
          debt_.has_value()};
    }

    seastar::future<output> run(bool forced) {
        // A failed segment takes no publication: nothing is written for a
        // pointer that cannot move.
        if (segment_.failure().failed())
            co_return runtime::failure(error(errc::closed));
        // The cut, in one synchronous step: the facts as they stand and the
        // segment's newest durable footer.
        const auto recorded_state = append_.completed_retries(target_);
        if (!recorded_state) co_return runtime::failure(recorded_state.error());
        const auto recorded = static_cast<std::uint32_t>(
          recorded_state->facts->size());
        if (
          !retry_cut_due(saved_, recorded, forced) || !recorded_state->boundary)
            co_return state(false, recorded, 0);
        // A snapshot is one bundle: what does not fit its pages needs a
        // roll of the segment, never a dropped fact.
        if (
          recorded > maximum_retry_snapshot_facts
          || (recorded + capacity_ - 1) / capacity_
               > maximum_retry_snapshot_pages)
            co_return runtime::failure(error(errc::resource_exhausted));
        const auto pinned = *recorded_state->boundary;
        auto cut = detail::copy_retry_facts(budget_, *recorded_state->facts);
        if (!cut) co_return runtime::failure(cut.error());

        // The footer's digest comes from the device, never from memory.
        const auto footer = co_await read_durable_boundary(
          files_,
          ownership_,
          devices_,
          shard_,
          pinned,
          budget_,
          limits_,
          execution_);
        if (!footer) co_return runtime::failure(footer.error());
        const auto sequence = co_await ids_.allocate_object(execution_);
        if (!sequence) co_return runtime::failure(sequence.error());
        detail::retry_snapshot_pages pages{
          &cut->facts,
          owner_,
          pinned.history.segment,
          device_.identity.metadata_alignment,
          pinned.history.alignment,
          *sequence,
          capacity_,
          limits_};
        auto root = co_await detail::encode_retry_snapshot_root(
          pages, *footer, execution_);
        if (!root) co_return runtime::failure(root.error());
        const auto reference = root->reference;
        auto bundle = co_await local_bundle::make(
          reference,
          std::move(root->expected),
          std::move(root->bytes),
          budget_,
          limits_,
          execution_);
        if (!bundle) co_return runtime::failure(bundle.error());
        auto published = co_await publish_local_bundle(
          files_,
          ownership_,
          device_,
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
            if (!published.publication.failure.failed())
                co_return runtime::failure(error(errc::io_failure));
            co_return runtime::failure(
              published.publication.failure.outcome().error());
        }

        // The pointer moves only now that its bundle is durable.
        const auto pointed = co_await segment_.publish_retry_snapshot(
          *footer, reference, recorded, execution_);
        if (!pointed) {
            // A pointer that was not touched leaves the bundle referenced by
            // nothing, so it goes; after anything else it may be current.
            if (!segment_.failure().failed())
                static_cast<void>(
                  co_await retire(pinned.history.segment, reference));
            co_return runtime::failure(pointed.error());
        }

        // Durable: these facts are saved, and what only the previous
        // snapshot held may go.
        const auto replaced = snapshot_;
        snapshot_ = reference;
        boundary_ = *footer;
        saved_ = recorded;
        if (debt_ && co_await retire(pinned.history.segment, *debt_))
            debt_.reset();
        if (replaced && !co_await retire(pinned.history.segment, *replaced))
            debt_ = replaced;
        // Facts recorded while the cut was written are unsaved.
        const auto now = append_.completed_retries(target_);
        co_return state(
          true,
          now ? static_cast<std::uint32_t>(now->facts->size()) : recorded,
          recorded);
    }

    // Unlinks one snapshot bundle, with no directory sync. True when it is
    // gone.
    seastar::future<bool>
    retire(segment_context segment, local_root_reference snapshot) {
        const auto paths = local_paths::make(device_.root);
        if (!paths) co_return false;
        const auto path = paths->object(
          shard_,
          {segment.segment(), segment.generation()},
          snapshot.sequence());
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
    local_device_spec device_;
    std::uint32_t shard_;
    local_store_context owner_;
    std::uint32_t capacity_;
    append_type& append_;
    local_append_target target_;
    segment_type& segment_;
    allocator_type& ids_;
    workload_budget& budget_;
    local_store_io_limits limits_;
    // The durable snapshot, the footer it is pinned beside and how many
    // facts it holds.
    std::optional<local_root_reference> snapshot_;
    std::optional<local_footer_reference> boundary_;
    std::uint32_t saved_{0};
    // A replaced snapshot whose removal failed.
    std::optional<local_root_reference> debt_;
    // A cut never uses a caller's abort source.
    seastar::abort_source abort_;
    codec::cooperative_work execution_;
    // The demands of the cut in flight, and of the one that follows it.
    std::optional<seastar::shared_promise<output>> serving_, next_;
    bool serving_forced_{false}, next_forced_{false};
    runtime::first_failure failed_;
    bool running_{false}, closing_{false}, closed_{false};
    // The cuts; joined by close().
    runtime::task_scope tasks_;
};

namespace detail {
// The fact with this identity in a table kept in batch identity order.
[[nodiscard]] const completed_retry* held_retry(
  const seastar::chunked_vector<completed_retry>& facts,
  const model::batch_id& id) noexcept;
// Adds a fact to such a table at its place. A native allocation failure
// throws, with no effect.
void insert_retry(
  seastar::chunked_vector<completed_retry>& facts, const completed_retry& fact);
// Puts the facts of `from` back into `into`, keeping its order.
void restore_retries(
  seastar::chunked_vector<completed_retry>& into,
  seastar::chunked_vector<completed_retry>& from);
} // namespace detail

// The completed-retry facts supplied for a segment that is no longer appended
// to: sealed, or recovering after a restart. Such a fact cannot join the
// segment's sealed summary, which is never rewritten or copied, and the
// append owner holds nothing for the segment. It is checked against what is
// already durable, the segment's snapshot and then its sealed summary, and
// only a fact neither holds is kept. A cut then writes a new snapshot beside
// the segment: the durable snapshot merged page by page with the kept facts.
// So a snapshot never repeats what the sealed summary holds, and a late fact
// costs a rewrite of the late facts alone.
//
// The segment's publication keeps its boundary and every other root; only
// the snapshot root is replaced, and the snapshot it replaces is removed
// after that publication is durable. Before a recovering segment is sealed,
// merged() is the retry source its recovered seal must use: the sealed
// summary then holds every fact, and the snapshot leaves the publication.
//
// Cuts happen on demand, one at a time, and demands made during a cut are
// served together by one cut after it. Every provider outlives joined
// close(), and close() precedes closing them.
template<
  runtime::file_system_backend Backend,
  local_directory_owner Owner,
  runtime::monotonic_clock Clock>
class late_retry_snapshot final : public runtime::shard_affine {
public:
    using segment_type = segment_writer<Backend, Owner, Clock>;
    using allocator_type = local_id_allocator<Backend, Owner>;
    using output = runtime::result<retry_snapshot_outcome>;

    // `publication` is the segment's current one, sealed or recovering, with
    // the boundary a snapshot is pinned beside. `summary` reads its sealed
    // summary and is required for a sealed segment. At most `capacity` facts
    // wait for a cut.
    [[nodiscard]] static seastar::future<
      runtime::result<std::unique_ptr<late_retry_snapshot>>>
    open(
      Backend& files,
      Owner& ownership,
      std::span<const local_device_spec> devices,
      std::uint32_t shard,
      segment_type& segment,
      const local_segment_descriptor& descriptor,
      const local_object_publication& publication,
      std::optional<retry_root_reader> summary,
      allocator_type& ids,
      workload_budget& budget,
      local_store_io_limits limits,
      codec::limits policy,
      std::uint32_t capacity) {
        if (auto valid = limits.validate(); !valid)
            co_return runtime::failure(valid.error());
        const bool sealed = publication.state == local_object_state::sealed;
        if (
          capacity == 0 || capacity > maximum_retry_snapshot_facts
          || publication.segment != descriptor.segment || !publication.boundary
          || (!sealed && publication.state != local_object_state::recovering)
          || sealed != summary.has_value())
            co_return runtime::failure(
              detail::path_error(errc::invalid_argument));
        const auto device = std::find_if(
          devices.begin(), devices.end(), [&segment](const auto& spec) {
              return spec.owner.device() == segment.device();
          });
        if (device == devices.end() || !device->stores_data())
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        const auto owner = device->shard_owner(shard);
        if (!owner)
            co_return runtime::failure(
              detail::path_error(errc::invalid_argument));
        const auto page = retry_snapshot_page_entries(
          limits, device->identity.metadata_alignment, policy);
        if (!page) co_return runtime::failure(page.error());
        auto instance = budget.allocation_charge(
          byte_count{sizeof(late_retry_snapshot)});
        if (!instance) co_return runtime::failure(instance.error());
        auto held = budget.try_reserve(
          byte_count{instance->value() + limits.execution_bytes.value()});
        if (!held) co_return runtime::failure(held.error());
        // The waiting facts and those a cut is writing are two tables.
        auto waiting = detail::admit_retry_facts(budget, capacity);
        if (!waiting) co_return runtime::failure(waiting.error());
        auto cutting = detail::admit_retry_facts(budget, capacity);
        if (!cutting) co_return runtime::failure(cutting.error());
        std::unique_ptr<late_retry_snapshot> made{new late_retry_snapshot(
          files,
          ownership,
          *device,
          shard,
          *owner,
          *page,
          capacity,
          segment,
          descriptor,
          *publication.boundary,
          std::move(summary),
          ids,
          budget,
          limits,
          policy,
          std::move(*held),
          std::move(*waiting),
          std::move(*cutting))};
        for (const auto& root : publication.roots) {
            if (root.kind() != local_root_kind::completed_retry_snapshot)
                continue;
            if (auto read = co_await made->read_snapshot(root); !read) {
                static_cast<void>(co_await made->close());
                co_return runtime::failure(read.error());
            }
        }
        co_return std::move(made);
    }
    late_retry_snapshot(const late_retry_snapshot&) = delete;
    late_retry_snapshot& operator=(const late_retry_snapshot&) = delete;
    ~late_retry_snapshot() {
        assert_current();
        KWAQUE_INVARIANT(
          invariant_id{"KQ-LATE-RETRIES-CLOSED"},
          closed_,
          "late retry owner destroyed before joined close");
    }

    // Facts the durable snapshot holds; none without one.
    [[nodiscard]] std::uint32_t durable() const noexcept {
        assert_current();
        return snapshot_ ? snapshot_->facts() : 0U;
    }
    // Facts kept that no snapshot holds yet.
    [[nodiscard]] std::uint32_t unsaved() const noexcept {
        assert_current();
        return static_cast<std::uint32_t>(pending_.size() + cutting_.size());
    }
    [[nodiscard]] std::optional<local_root_reference> snapshot() const {
        assert_current();
        if (!snapshot_) return std::nullopt;
        return snapshot_->reference();
    }
    [[nodiscard]] bool running() const noexcept {
        assert_current();
        return running_;
    }
    // The first failure that ends this owner.
    [[nodiscard]] const runtime::first_failure& failure() const& noexcept {
        assert_current();
        return failed_;
    }
    const runtime::first_failure& failure() const&& = delete;

    // Records a fact its owner supplied. A fact already durable, in the
    // snapshot or the sealed summary, or already kept, is not kept again:
    // the stored original is returned, and a differing one names its first
    // differing field. The identity is the full batch id; the fact's topic
    // and range must be the segment's. Nothing is evicted by time.
    [[nodiscard]] seastar::future<runtime::result<completed_retry_ingest>>
    record(completed_retry fact, codec::cooperative_work& work) {
        assert_current();
        using result = runtime::result<completed_retry_ingest>;
        if (closing_ || closed_ || lent_ || failed_.failed())
            co_return runtime::failure(error(errc::closed));
        const auto binding = fact.original_binding();
        if (
          binding.topic() != segment_context_.topic()
          || binding.range() != segment_context_.range())
            co_return runtime::failure(error(errc::wrong_context));
        // One at a time, and never while a finished cut swaps what is
        // durable: every lookup below sees one consistent state.
        std::optional<seastar::semaphore_units<>> turn;
        try {
            turn.emplace(co_await seastar::get_units(turn_, 1));
        } catch (...) {
            co_return runtime::failure(error(errc::closed));
        }
        if (closing_ || closed_ || failed_.failed())
            co_return runtime::failure(error(errc::closed));
        const auto kept = [this](const model::batch_id& id) {
            const auto* held = detail::held_retry(pending_, id);
            return held ? held : detail::held_retry(cutting_, id);
        };
        if (const auto* held = kept(fact.id()))
            co_return result{completed_retry_ingest{
              completed_retry_difference(fact, *held), *held}};
        for (auto* reader : {&snapshot_, &summary_}) {
            if (!*reader) continue;
            auto stored = co_await (*reader)->find(fact.id(), work);
            if (!stored) co_return runtime::failure(stored.error());
            if (*stored)
                co_return result{completed_retry_ingest{
                  completed_retry_difference(fact, **stored), **stored}};
        }
        if (pending_.size() + cutting_.size() >= capacity_)
            co_return runtime::failure(error(errc::resource_exhausted));
        detail::insert_retry(pending_, fact);
        co_return result{
          completed_retry_ingest{completed_retry_status::created, fact}};
    }

    // The retry source a recovered seal of this segment must use: the
    // durable snapshot merged with the facts kept here. It borrows this
    // owner's state, and the seal that reads it replaces the publication
    // this owner was opened under. So it is lent once, and from then on the
    // owner records and cuts nothing: it is only closed, after the seal.
    [[nodiscard]] runtime::result<merged_retry_source> merged() {
        assert_current();
        if (closing_ || closed_ || lent_ || failed_.failed() || running_)
            return runtime::failure(error(errc::closed));
        lent_ = true;
        return merged_retry_source{
          snapshot_ ? &*snapshot_ : nullptr, &pending_};
    }

    // Cuts a snapshot when one is due, or joins the cut that follows the one
    // in flight. A demand that is not forced cuts only once the kept facts
    // are at least as many as the snapshot's.
    [[nodiscard]] seastar::future<output> request(bool forced) {
        assert_current();
        if (closing_ || closed_ || lent_ || failed_.failed())
            return seastar::make_ready_future<output>(
              runtime::failure(error(errc::closed)));
        if (running_) {
            if (!next_) next_.emplace();
            next_forced_ = next_forced_ || forced;
            return next_->get_shared_future();
        }
        serving_.emplace();
        serving_forced_ = forced;
        auto served = serving_->get_shared_future();
        // Set first: the cut may finish before spawn returns.
        running_ = true;
        runtime::result<void> spawned;
        try {
            spawned = tasks_.spawn([this] { return serve(); });
        } catch (...) {
            serving_.reset();
            running_ = false;
            return seastar::current_exception_as_future<output>();
        }
        if (!spawned) {
            serving_.reset();
            running_ = false;
            return seastar::make_ready_future<output>(
              runtime::failure(spawned.error()));
        }
        return served;
    }

    // Stops taking facts and demands, lets the cuts already asked for
    // finish, joins them and releases what it read through.
    [[nodiscard]] seastar::future<runtime::result<void>> close() {
        assert_current();
        if (closed_) co_return runtime::result<void>{};
        closing_ = true;
        try {
            co_await tasks_.close();
        } catch (...) {
            failed_.observe(std::current_exception());
        }
        // A record in flight finishes before the readers go.
        try {
            const auto last = co_await seastar::get_units(turn_, 1);
        } catch (...) {
            failed_.observe(std::current_exception());
        }
        turn_.broken();
        summary_.reset();
        co_await release_snapshot();
        closed_ = true;
        co_return runtime::result<void>{};
    }

private:
    late_retry_snapshot(
      Backend& files,
      Owner& ownership,
      local_device_spec device,
      std::uint32_t shard,
      local_store_context owner,
      std::uint32_t page,
      std::uint32_t capacity,
      segment_type& segment,
      const local_segment_descriptor& descriptor,
      local_footer_reference boundary,
      std::optional<retry_root_reader> summary,
      allocator_type& ids,
      workload_budget& budget,
      local_store_io_limits limits,
      codec::limits policy,
      workload_reservation held,
      workload_reservation waiting,
      workload_reservation cutting)
      : held_(std::move(held))
      , waiting_held_(std::move(waiting))
      , cutting_held_(std::move(cutting))
      , files_(files)
      , ownership_(ownership)
      , device_(std::move(device))
      , shard_(shard)
      , owner_(owner)
      , page_(page)
      , capacity_(capacity)
      , segment_(segment)
      , segment_context_(descriptor.segment)
      , segment_alignment_(descriptor.alignment)
      , boundary_(boundary)
      , summary_(std::move(summary))
      , ids_(ids)
      , budget_(budget)
      , limits_(limits)
      , execution_(policy, abort_)
      , tasks_([this](std::exception_ptr failure) noexcept {
          failed_.observe(std::move(failure));
      }) {}

    static runtime::operation_error error(errc code) noexcept {
        return detail::path_error(code);
    }

    seastar::future<> serve() {
        while (serving_) {
            output outcome = runtime::failure(error(errc::io_failure));
            std::exception_ptr thrown;
            try {
                outcome = co_await run(serving_forced_);
            } catch (...) {
                thrown = std::current_exception();
            }
            auto served = std::move(*serving_);
            serving_.reset();
            if (next_) {
                serving_.emplace(std::move(*next_));
                serving_forced_ = next_forced_;
                next_.reset();
                next_forced_ = false;
            } else {
                running_ = false;
            }
            if (thrown)
                served.set_exception(std::move(thrown));
            else
                served.set_value(std::move(outcome));
        }
    }

    [[nodiscard]] retry_snapshot_outcome
    state(bool cut, std::uint32_t written) const {
        return {
          cut,
          snapshot_ ? snapshot_->facts() : 0U,
          static_cast<std::uint32_t>(pending_.size() + cutting_.size()),
          snapshot_ ? std::optional{snapshot_->reference()} : std::nullopt,
          boundary_,
          written,
          debt_.has_value()};
    }

    seastar::future<output> run(bool forced) {
        if (failed_.failed()) co_return runtime::failure(error(errc::closed));
        const auto saved = snapshot_ ? snapshot_->facts() : 0U;
        const auto kept = static_cast<std::uint32_t>(pending_.size());
        if (!retry_cut_due(saved, saved + kept, forced))
            co_return state(false, 0);
        const auto total = saved + kept;
        // A snapshot is one bundle; what does not fit it is refused, never
        // dropped.
        if (
          total > maximum_retry_snapshot_facts
          || (total + page_ - 1) / page_ > maximum_retry_snapshot_pages)
            co_return runtime::failure(error(errc::resource_exhausted));
        // The cut, in one synchronous step: the facts kept so far. One
        // recorded while it is written waits for the next.
        cutting_ = std::move(pending_);
        pending_ = {};
        runtime::result<void> written{};
        std::exception_ptr thrown;
        try {
            written = co_await write(total);
        } catch (...) {
            thrown = std::current_exception();
        }
        if (thrown || !written) {
            // Nothing became durable: the facts wait again.
            detail::restore_retries(pending_, cutting_);
            if (thrown) std::rethrow_exception(thrown);
            co_return runtime::failure(written.error());
        }
        co_return state(true, total);
    }

    // One cut: the bundle, then the pointer, then what is durable is read
    // through the new snapshot. Returns once the cut's facts are saved, or
    // with nothing changed.
    seastar::future<runtime::result<void>> write(std::uint32_t total) {
        const auto sequence = co_await ids_.allocate_object(execution_);
        if (!sequence) co_return runtime::failure(sequence.error());
        std::optional<local_root_reference> written;
        {
            merged_retry_source merged{
              snapshot_ ? &*snapshot_ : nullptr, &cutting_};
            detail::retry_snapshot_pages pages{
              nullptr,
              owner_,
              segment_context_,
              device_.identity.metadata_alignment,
              segment_alignment_,
              *sequence,
              page_,
              limits_};
            pages.merged = &merged;
            auto root = co_await detail::encode_retry_snapshot_root(
              pages, boundary_, execution_);
            if (!root) co_return runtime::failure(root.error());
            const auto reference = root->reference;
            auto bundle = co_await local_bundle::make(
              reference,
              std::move(root->expected),
              std::move(root->bytes),
              budget_,
              limits_,
              execution_);
            if (!bundle) co_return runtime::failure(bundle.error());
            auto published = co_await publish_local_bundle(
              files_,
              ownership_,
              device_,
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
                if (!published.publication.failure.failed())
                    co_return runtime::failure(error(errc::io_failure));
                co_return runtime::failure(
                  published.publication.failure.outcome().error());
            }
            written = reference;
        }
        // The pointer moves only now that its bundle is durable, and
        // carries the boundary and every other root as they were.
        const auto pointed = co_await segment_.publish_retry_snapshot(
          boundary_, *written, total, execution_);
        if (!pointed) {
            // A pointer that was not touched leaves the bundle referenced
            // by nothing, so it goes; after anything else it may be current.
            if (!segment_.failure().failed())
                static_cast<void>(co_await retire(*written));
            else
                failed_.observe(pointed.error());
            co_return runtime::failure(pointed.error());
        }
        // Durable. From here the cut cannot be undone: its facts are no
        // longer waiting, and a failure to read the new snapshot, returned
        // or thrown, ends this owner instead of failing the cut. An owner
        // left alive without that reader would take the next cut for the
        // first and publish it over the facts this one saved.
        cutting_ = {};
        try {
            const auto turn = co_await seastar::get_units(turn_, 1);
            const auto replaced = snapshot_
                                    ? std::optional{snapshot_->reference()}
                                    : std::nullopt;
            co_await release_snapshot();
            if (auto read = co_await read_snapshot(*written); !read)
                failed_.observe(read.error());
            if (debt_ && co_await retire(*debt_)) debt_.reset();
            if (replaced && !co_await retire(*replaced)) debt_ = replaced;
        } catch (...) {
            failed_.observe(std::current_exception());
        }
        co_return runtime::result<void>{};
    }

    // Opens a snapshot of this segment under the context a reader expects,
    // nothing of it taken from its bytes, and reads through it from now on.
    seastar::future<runtime::result<void>>
    read_snapshot(local_root_reference snapshot) {
        const auto generation = local_publication_generation::make(
          snapshot.sequence().value());
        if (!generation) co_return runtime::failure(error(errc::wrong_context));
        const auto header = local_metadata_header::make(
          local_metadata_kind::completed_retry_root, owner_, *generation);
        if (!header) co_return runtime::failure(error(errc::wrong_context));
        local_metadata_expectation expected{
          *header, device_.identity.metadata_alignment};
        expected.segment = segment_context_;
        expected.segment_alignment = segment_alignment_;
        expected.digest = snapshot.digest();
        expected.encoded_bytes = snapshot.bytes();
        auto opened = co_await local_root_owner::open(
          files_,
          ownership_,
          device_,
          shard_,
          snapshot,
          local_bundle_context{std::move(expected)},
          budget_,
          limits_,
          execution_);
        if (!opened) co_return runtime::failure(opened.error());
        snapshot_owner_ = std::move(*opened);
        auto pin = snapshot_owner_->pin();
        if (!pin) {
            const auto failed = pin.error();
            co_await release_snapshot();
            co_return runtime::failure(failed);
        }
        auto reader = co_await retry_root_reader::open(
          std::move(*pin), budget_, execution_);
        if (!reader) {
            co_await release_snapshot();
            co_return runtime::failure(reader.error());
        }
        snapshot_.emplace(std::move(*reader));
        co_return runtime::result<void>{};
    }
    // Stops reading through the current snapshot and closes its file.
    seastar::future<> release_snapshot() {
        snapshot_.reset();
        if (!snapshot_owner_) co_return;
        snapshot_owner_->retire();
        try {
            failed_.observe(co_await snapshot_owner_->close());
        } catch (...) {
            failed_.observe(std::current_exception());
        }
        snapshot_owner_.reset();
    }

    // Unlinks one snapshot bundle, with no directory sync. True when it is
    // gone.
    seastar::future<bool> retire(local_root_reference snapshot) {
        const auto paths = local_paths::make(device_.root);
        if (!paths) co_return false;
        const auto path = paths->object(
          shard_,
          {segment_context_.segment(), segment_context_.generation()},
          snapshot.sequence());
        if (!path) co_return false;
        try {
            auto removed = co_await files_.remove_file(*path);
            co_return removed.has_value()
              || removed.error().code() == errc::not_found;
        } catch (...) {
            co_return false;
        }
    }

    // Declared first: released after everything they admitted.
    workload_reservation held_, waiting_held_, cutting_held_;
    Backend& files_;
    Owner& ownership_;
    local_device_spec device_;
    std::uint32_t shard_;
    local_store_context owner_;
    // Facts one page holds, and facts that may wait for a cut.
    std::uint32_t page_, capacity_;
    segment_type& segment_;
    segment_context segment_context_;
    storage_alignment segment_alignment_;
    // The boundary the segment's publication pins; it never changes here.
    local_footer_reference boundary_;
    // Facts kept in batch identity order: those waiting for a cut, and
    // those the cut in flight is writing.
    seastar::chunked_vector<completed_retry> pending_, cutting_;
    // What is durable: the snapshot this owner opened and reads through,
    // and the sealed summary.
    std::unique_ptr<local_root_owner> snapshot_owner_;
    std::optional<retry_root_reader> snapshot_, summary_;
    allocator_type& ids_;
    workload_budget& budget_;
    local_store_io_limits limits_;
    // A replaced snapshot whose removal failed.
    std::optional<local_root_reference> debt_;
    // A record or the swap after a cut, one at a time.
    seastar::semaphore turn_{1};
    // A cut never uses a caller's abort source.
    seastar::abort_source abort_;
    codec::cooperative_work execution_;
    std::optional<seastar::shared_promise<output>> serving_, next_;
    bool serving_forced_{false}, next_forced_{false};
    runtime::first_failure failed_;
    // merged() lent this owner's state to a seal.
    bool lent_{false};
    bool running_{false}, closing_{false}, closed_{false};
    // The cuts; joined by close().
    runtime::task_scope tasks_;
};

// At reopen, before any owner of the segment runs: removes every bundle in
// the segment's objects directory that `publication`, its current one, does
// not name. Such a bundle is a snapshot whose pointer never moved, one a
// later snapshot or the seal replaced and whose removal a crash undid, or
// the retry bundle of a seal that did not finish. Only the segment's own
// publication references a bundle there, and sequences are never used
// twice, so none of them can become referenced. A deleting segment is left
// to its deletion; temporaries and anything unknown are left for their own
// owner. Returns how many bundles were removed; the first removal that fails
// is returned instead.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<runtime::result<std::uint32_t>> remove_unreferenced_objects(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  std::uint32_t shard,
  const local_object_publication& publication,
  codec::cooperative_work& work) {
    if (!spec.shard_owner(shard) || !spec.stores_data())
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    if (publication.state == local_object_state::deleting) co_return 0U;
    if (auto valid = co_await ownership.validate(spec); !valid)
        co_return runtime::failure(valid.error());
    const auto paths = local_paths::make(spec.root);
    if (!paths) co_return runtime::failure(paths.error());
    const auto first = paths->object(
      shard,
      {publication.segment.segment(), publication.segment.generation()},
      local_object_sequence::make(1).value());
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
                if (!sequence || entry.kind != runtime::file_kind::regular)
                    continue;
                const bool named = std::any_of(
                  publication.roots.begin(),
                  publication.roots.end(),
                  [&sequence](const local_root_reference& root) {
                      return root.sequence().value() == *sequence;
                  });
                if (named) continue;
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

} // namespace kwaque::storage
