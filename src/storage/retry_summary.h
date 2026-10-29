#pragma once

#include "src/codec/cooperative.h"
#include "src/codec/digest.h"
#include "src/model/batch_identity.h"
#include "src/model/epoch.h"
#include "src/model/position.h"
#include "src/runtime/error.h"
#include "src/storage/completed_retry.h"
#include "src/storage/encoded_batch.h"
#include "src/storage/format_context.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/chunked_hash_map.hh>
#include <seastar/core/chunked_vector.hh>
#include <seastar/core/future.hh>

#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace kwaque::storage {

// The newest batches of one prefix in one segment generation, oldest first,
// as many as the segment's retry depth: what its completed facts are made
// of. Its address is stable for the builder's lifetime, so an accepted
// request names its ring until it is published.
class retry_ring final {
public:
    retry_ring(const retry_ring&) = delete;
    retry_ring& operator=(const retry_ring&) = delete;
    [[nodiscard]] std::uint32_t size() const noexcept { return size_; }

private:
    friend class retry_summary_builder;
    friend class retry_summary_source;
    struct entry final {
        model::batch_sequence sequence;
        codec::semantic_batch_digest digest;
        model::range_logical_span span;
    };
    retry_ring(
      model::range_routing_epoch routing,
      std::vector<entry> entries,
      std::uint8_t depth) noexcept
      : routing_(routing)
      , entries_(std::move(entries))
      , depth_(depth) {}
    // The i-th oldest batch it holds.
    [[nodiscard]] const entry& at(std::uint32_t i) const noexcept {
        return entries_[(head_ + i) % depth_];
    }
    [[nodiscard]] const entry& newest() const noexcept { return at(size_ - 1); }
    // The completed fact of its i-th oldest batch, a batch of `prefix` in
    // `segment`: absent only if the batch could not be one.
    [[nodiscard]] std::optional<completed_retry> fact(
      segment_context segment,
      const retry_prefix& prefix,
      std::uint32_t i) const noexcept;
    // A stream's binding never changes, so its routing epoch is kept once.
    model::range_routing_epoch routing_;
    // Reserved for the depth's entries, then written in place once full.
    std::vector<entry> entries_;
    std::uint8_t depth_;
    // The newest sequence of its prefix whose group froze in the segment.
    std::optional<model::batch_sequence> frozen_;
    std::uint32_t size_{0}, head_{0};
    // Entries reserved for it in the segment's seal, at most the depth.
    std::uint32_t claimed_{0};
};

class retry_summary_source;

// Derives one segment generation's completed facts from its batches: for
// each prefix the last R published, R being the segment's retry depth. The
// append owner feeds it as it publishes each branch durable, in file order,
// and a recovered seal from the blocks its walk verifies, so a live seal and
// a recovered one of the same blocks hold the same facts. Nothing is
// evicted by time or by other prefixes: past ⌊maximum_object_entries / R⌋
// prefixes a new one is refused, which rolls the segment.
//
// Each prefix is admitted from the supplied budget when it is first seen, and
// a fact allocates nothing once its ring exists. The budget outlives the
// builder and the summary sealed from it.
class retry_summary_builder final {
public:
    [[nodiscard]] static runtime::result<retry_summary_builder>
    make(segment_context segment, std::uint8_t depth, workload_budget& budget);
    retry_summary_builder(retry_summary_builder&&) noexcept = default;
    retry_summary_builder&
    operator=(retry_summary_builder&&) noexcept = default;
    retry_summary_builder(const retry_summary_builder&) = delete;
    retry_summary_builder& operator=(const retry_summary_builder&) = delete;

    // The most prefixes one segment of this depth holds.
    [[nodiscard]] static std::uint32_t
    prefix_limit(std::uint8_t depth) noexcept;

    // The ring of `id`'s prefix, made with `routing` when the segment has
    // none yet: null when it already holds as many prefixes as its depth
    // allows. A prefix keeps the routing epoch it was made with
    // (wrong_context otherwise) and takes only sequences above the newest
    // whose group froze (invalid_argument otherwise); the caller orders the
    // ones not yet frozen. A new prefix's memory is admitted from the
    // budget; a refusal is the budget's error, and a native allocation
    // failure throws, with no effect. A prefix made here is the caller's
    // until keep() or drop(), and one is made at a time.
    [[nodiscard]] runtime::result<retry_ring*>
    admit(const model::batch_id& id, model::range_routing_epoch routing);
    // The prefix admit() made stays in the segment.
    void keep() noexcept;
    // The prefix admit() made, unless kept, goes with its memory: a request
    // refused after its admission leaves nothing.
    void drop() noexcept;
    // Whether one more batch of the ring's prefix, after `earlier` others of
    // it in the same group, takes an entry of the segment's seal: as long as
    // the prefix has fewer than the depth reserved.
    [[nodiscard]] bool
    opens_entry(const retry_ring& ring, std::uint32_t earlier) const noexcept {
        return ring.claimed_ + std::uint64_t{earlier} < depth_;
    }
    // A batch of the ring's prefix froze in the segment with `sequence`, in
    // file order, and its group reserved a seal entry for it when `entry`.
    void froze(retry_ring& ring, model::batch_sequence sequence, bool entry)
      const noexcept;
    // A batch of the ring's prefix was published durable, in file order: it
    // becomes the prefix's newest fact, and the oldest goes once the ring
    // holds the depth. Allocates nothing.
    void add(
      retry_ring& ring,
      model::batch_sequence sequence,
      codec::semantic_batch_digest digest,
      model::range_logical_span span) noexcept;

    // Feeds one verified block's batch, in file order. False when the
    // segment already holds as many prefixes as its depth allows. A batch
    // bound to another segment generation, a prefix's second routing epoch
    // and a sequence not above the prefix's newest are malformed_data.
    [[nodiscard]] runtime::result<bool>
    derive(const assigned_batch_info& batch);

    // The fact for `id`, if it is among its prefix's last R.
    [[nodiscard]] std::optional<completed_retry>
    find(const model::batch_id& id) const;
    // The newest fact of a prefix.
    [[nodiscard]] std::optional<completed_retry>
    last(const retry_prefix& prefix) const;
    [[nodiscard]] std::uint32_t prefixes() const noexcept {
        return static_cast<std::uint32_t>(rings_.size());
    }
    [[nodiscard]] std::uint32_t facts() const noexcept { return facts_; }
    [[nodiscard]] std::uint8_t depth() const noexcept { return depth_; }
    [[nodiscard]] segment_context segment() const noexcept { return segment_; }

    // Every fact, in batch identity order, for a seal. The prefixes are
    // sorted in place, a bounded step for each unit of work admitted, so no
    // count of them stalls the shard; nothing is allocated, and the builder
    // is used up.
    [[nodiscard]] static seastar::future<runtime::result<retry_summary_source>>
    seal(retry_summary_builder builder, codec::cooperative_work& work);

private:
    friend class retry_summary_source;
    using ring_ptr = std::unique_ptr<retry_ring>;
    using ring_map = seastar::chunked_hash_map<retry_prefix, ring_ptr>;
    retry_summary_builder(
      segment_context segment,
      std::uint8_t depth,
      workload_budget& budget) noexcept
      : segment_(segment)
      , budget_(&budget)
      , depth_(depth) {}
    [[nodiscard]] runtime::result<retry_ring*>
    open(const retry_prefix& prefix, model::range_routing_epoch routing);

    segment_context segment_;
    workload_budget* budget_;
    // Every kept ring's memory, grown one prefix at a time.
    std::optional<workload_reservation> held_;
    // The prefix admit() made and nobody has kept or dropped, with its memory.
    struct opening final {
        retry_prefix prefix;
        workload_reservation held;
    };
    std::optional<opening> opening_;
    ring_map rings_;
    std::uint32_t facts_{0};
    std::uint8_t depth_;
};

// A segment seal's completed facts, sealed from a builder: read(first,count)
// returns exactly that slice in batch identity order and replays it
// unchanged. It owns the builder's rings and their admitted memory.
class retry_summary_source final {
public:
    retry_summary_source(retry_summary_source&&) noexcept = default;
    retry_summary_source& operator=(retry_summary_source&&) noexcept = default;
    retry_summary_source(const retry_summary_source&) = delete;
    retry_summary_source& operator=(const retry_summary_source&) = delete;
    [[nodiscard]] std::uint32_t completed() const noexcept { return facts_; }
    // The returned vector's capacity is exactly `count`.
    [[nodiscard]] seastar::future<runtime::result<std::vector<completed_retry>>>
    read(std::uint32_t first, std::uint32_t count, codec::cooperative_work&);

private:
    friend class retry_summary_builder;
    using values = seastar::chunked_vector<
      std::pair<retry_prefix, std::unique_ptr<retry_ring>>>;
    retry_summary_source(
      segment_context segment,
      std::optional<workload_reservation> held,
      values sorted,
      std::uint32_t facts) noexcept
      : segment_(segment)
      , held_(std::move(held))
      , sorted_(std::move(sorted))
      , facts_(facts) {}

    segment_context segment_;
    std::optional<workload_reservation> held_;
    values sorted_;
    std::uint32_t facts_{0};
    // Where the next read continues: a prefix, a fact within its ring, and
    // that fact's place in the whole summary.
    std::size_t prefix_{0};
    std::uint32_t offset_{0}, next_{0};
};

} // namespace kwaque::storage
