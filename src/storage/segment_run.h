#pragma once

#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/codec/cooperative.h"
#include "src/codec/limits.h"
#include "src/model/position.h"
#include "src/runtime/error.h"
#include "src/runtime/file_position.h"
#include "src/runtime/first_failure.h"
#include "src/runtime/shard_affinity.h"
#include "src/runtime/time.h"
#include "src/storage/footer_format.h"
#include "src/storage/local_generation.h"
#include "src/storage/page_cache.h"
#include "src/storage/segment_data.h"
#include "src/storage/segment_writer_state.h"
#include "src/storage/sparse_index.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/util/noncopyable_function.hh>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <variant>
#include <vector>

namespace kwaque::storage {

// One block of a run: what it covers, and where in the run it and the encoded
// batch it carries lie.
struct segment_run_block final {
    model::range_logical_span logical;
    model::segment_relative_span physical;
    // From the run's first byte to the block's.
    std::uint64_t offset;
    // The block as it lies in the file, padding included.
    std::uint32_t bytes;
    // From the block's first byte to its batch's, and the batch's length.
    std::uint32_t batch_offset;
    std::uint32_t batch_bytes;
    // Which of the run's parts holds it.
    std::uint32_t part;
};

// The most blocks one run holds, so that its table stays one allocation
// within the contiguous ceiling: the index takes an anchor at least that
// often. And the most parts a run that is fed is built from.
inline constexpr std::uint32_t maximum_run_blocks = maximum_anchor_blocks;
inline constexpr std::uint32_t maximum_run_parts = 64;

// What reading and checking a run may take.
struct segment_run_limits final {
    // What the budget is charged for one allocation.
    bytes::allocation_charge_fn charge{nullptr};
    // The most bytes read or fed as one run, a multiple of the segment's
    // alignment. A longer stretch between two anchors is read in parts,
    // each for its reader alone.
    byte_count run_bytes{4_MiB};
    // What checking a run takes beyond the run: the codec's aliases of the
    // object it looks at, and their bookkeeping.
    byte_count working_bytes{256_KiB};
    byte_count metadata_bytes{64_KiB};
    codec::limits policy{codec::limits::defaults()};
    [[nodiscard]] runtime::result<void> validate() const noexcept;
};

// Where a run begins, and what the block there must be: the block at that
// position whose first record has that offset and that place among the
// segment's records. An anchor says the first two. The third follows from
// them in an extent that was appended to, where every block keeps every
// record: the extent's physical origin plus the distance from its logical
// origin.
struct segment_run_start final {
    runtime::file_position position;
    model::range_logical_offset base;
    model::segment_relative_end physical;
    bool operator==(const segment_run_start&) const noexcept = default;
};
// The start an anchor names in an appended extent. Nothing for an anchor
// that lies before the extent's origin.
[[nodiscard]] std::optional<segment_run_start> segment_run_at(
  const segment_history_context&, const sparse_index_entry& anchor) noexcept;

// The complete blocks of a segment, and the footers between them, from one
// block start to another: from an anchor to the next, when it is a run the
// cache keeps. Each block was checked where it lies when the run was read,
// or was written by the segment's own writer, and is not checked again.
//
// The bytes are the read's own buffers, or copies of what the writer wrote,
// and nothing else is kept: a batch handed out is a share of them. What a
// run is charged is what those buffers and its tables cost.
class segment_run final {
public:
    explicit segment_run(runtime::file_position begin) noexcept
      : begin_(begin) {}
    segment_run(segment_run&&) noexcept = default;
    segment_run& operator=(segment_run&&) noexcept = default;
    segment_run(const segment_run&) = delete;
    segment_run& operator=(const segment_run&) = delete;

    [[nodiscard]] runtime::file_position begin() const noexcept {
        return begin_;
    }
    // Where the last complete object it holds ends.
    [[nodiscard]] runtime::file_position end() const noexcept {
        return runtime::file_position{begin_.value() + size_};
    }
    [[nodiscard]] std::span<const segment_run_block> blocks() const noexcept {
        return blocks_;
    }
    // What the cache is charged for the value: see page_cache.
    [[nodiscard]] byte_count charge() const noexcept {
        return byte_count{charge_};
    }
    // The block a reader that wants `target` is given: the one that holds
    // it, or the first after it when no block does. blocks().size() when
    // every block ends at or before it.
    [[nodiscard]] std::size_t
    seek(model::range_logical_offset target) const noexcept;
    // Where block `at` begins, and where whatever follows it does: the next
    // block of this run, or the block that begins where the run ends.
    // Nothing when no offset can follow it.
    [[nodiscard]] segment_run_start at(std::size_t at) const noexcept;
    [[nodiscard]] std::optional<segment_run_start>
    after(std::size_t at) const noexcept;
    // The encoded batch of one block, whole, sharing the run's bytes.
    [[nodiscard]] runtime::result<bytes::fragmented_buffer>
    batch(std::size_t at) const;

private:
    friend class segment_run_builder;
    struct part final {
        std::uint64_t offset;
        // Shared when a batch is handed out; the bytes do not change.
        mutable bytes::fragmented_buffer bytes;
    };
    runtime::file_position begin_;
    std::uint64_t size_{0};
    std::uint64_t charge_{0};
    std::vector<part> parts_;
    std::vector<segment_run_block> blocks_;
};

// Builds a run from bytes that are there already, part by part in file
// order. What it holds is priced as it grows, so that whoever admits the run
// can take each part's cost before the next.
class segment_run_builder final {
public:
    segment_run_builder(
      runtime::file_position begin, const workload_budget& budget) noexcept
      : budget_(&budget)
      , run_(begin) {}
    [[nodiscard]] const segment_run& run() const& noexcept { return run_; }
    const segment_run& run() const&& = delete;
    // Appends `bytes` as the run's next part: they lie in the file where the
    // parts before them end. Its blocks are added with block() before the
    // next part.
    [[nodiscard]] runtime::result<void> part(bytes::fragmented_buffer bytes);
    // One block of the part appended last.
    [[nodiscard]] runtime::result<void> block(segment_run_block described);
    // The run ends at `size` bytes from its first: the end of the last
    // complete object in it.
    void end_at(std::uint64_t size) noexcept { run_.size_ = size; }
    [[nodiscard]] segment_run finish() && noexcept { return std::move(run_); }

private:
    // Prices what the run holds now.
    [[nodiscard]] runtime::result<void> price() noexcept;
    const workload_budget* budget_;
    segment_run run_;
    std::uint64_t buffers_{0};
};

// What a check of the bytes of a run leaves.
struct checked_segment_run final {
    segment_run run;
    // The bytes ended inside an object: that object's whole length. Nothing
    // when they ended where an object ends.
    std::optional<byte_count> cut;
};

// Checks `bytes`, read at `start`, as blocks and the footers between them,
// each where it lies, and makes the run of them. Every block is checked with
// check_segment_block(): nothing is decompressed and no record is walked. A
// footer is passed over once its header's checksum says how long it is.
//
// The first block must be the one `start` names. Every later block must
// begin where the one before it ended among the segment's records, and at or
// after where it ended in logical offsets. With `whole`, the bytes must
// begin with that block, end where an object ends and hold no more than
// maximum_run_blocks blocks: that is a run the cache keeps. Without it the
// walk stops at the last complete object that fits, for a reader that reads
// a stretch in parts; such a part may begin with the footer the part before
// it stopped at, and may hold nothing but footers.
//
// A first object that is not the block `start` names fails with
// wrong_context: what led there was wrong. A checksum that does not match is
// corrupt_data, and anything else the bytes fail is malformed_data. `budget`
// prices what the run holds; nothing is reserved here, and whoever calls has
// admitted the bytes and the check's own work.
[[nodiscard]] seastar::future<runtime::result<checked_segment_run>>
check_segment_run(
  bytes::fragmented_buffer bytes,
  segment_history_context history,
  segment_run_start start,
  bool whole,
  const segment_run_limits& limits,
  const workload_budget& budget,
  codec::cooperative_work& work);

// What a segment's data is read through: the pin of a generation that is
// published, or of the handle the readers of a segment that is still
// appended to share.
class segment_data_source final {
public:
    explicit segment_data_source(local_generation_pin pin) noexcept
      : pin_(std::move(pin)) {}
    explicit segment_data_source(segment_data_pin pin) noexcept
      : pin_(std::move(pin)) {}
    segment_data_source(segment_data_source&&) noexcept = default;
    segment_data_source& operator=(segment_data_source&&) = delete;
    segment_data_source(const segment_data_source&) = delete;
    segment_data_source& operator=(const segment_data_source&) = delete;
    // Another right to read through the same pin, for work that outlives
    // its caller.
    [[nodiscard]] runtime::result<segment_data_source> share() const;
    // As read_segment_data().
    [[nodiscard]] seastar::future<runtime::result<bytes::fragmented_buffer>>
    read(
      runtime::file_position position,
      byte_count length,
      runtime::file_position bound) const;

private:
    std::variant<local_generation_pin, segment_data_pin> pin_;
};

// How a reader of a segment that is still appended to reads what is durable
// in at least one place and not in the segment's file yet: the bytes from a
// position to an end, as shares of what the segment's writer holds of them.
// Refused with out_of_range when any of them is in the file by now, or was
// never made. Whatever it refers to outlives the session it is given to.
using segment_read_held
  = seastar::noncopyable_function<runtime::result<bytes::fragmented_buffer>(
    runtime::file_position, runtime::file_position)>;

// A run the writer fed that the cache does not answer with yet, as far as it
// may be read. The run is borrowed until its caller next waits; `held` keeps
// whatever is taken of it charged for as long as it lives, and is no way to
// the run: its entry has no value until the run is whole and readable.
struct segment_tail final {
    const segment_run* run;
    cache_hold<segment_run> held;
    // Blocks that end at or below this are durable in at least one place.
    runtime::file_position readable;
};

inline constexpr std::uint64_t default_feed_linger_nanoseconds
  = 2'000'000'000ULL;
// The most written groups that wait to be copied, and the most bytes they
// share: past either, what is written is not kept and is read from the
// device later.
inline constexpr std::uint32_t maximum_fed_groups = 64;
inline constexpr std::uint32_t maximum_fed_runs = 64;

struct segment_runs_config final {
    segment_run_limits limits;
    // The most bytes of written groups that wait to be copied.
    byte_count feed_bytes{8_MiB};
    // How long after it was last read a segment is still fed.
    std::uint64_t feed_linger_nanoseconds{default_feed_linger_nanoseconds};
    // The time, for that. Without it a segment is fed only while an owner
    // says a reader is established on it.
    seastar::noncopyable_function<runtime::monotonic_time() noexcept> now{};
    // Told of a read of the segment that failed for the segment's own
    // reasons, each kind once: the first time a read finds its committed
    // data not what a barrier made durable there, and the first time its
    // device fails a read (an I/O error, a read-only filesystem, a device
    // that is gone).
    seastar::noncopyable_function<void(
      const runtime::operation_error&) noexcept>
      failed{};
    // Whether what is fed is copied whole. Otherwise a fragment that fills
    // at least three quarters of the allocation it keeps alive is shared,
    // and only the others are copied.
    bool copy_fed{false};
};

// The cached runs of one segment, and the tail its writer feeds.
//
// A run is found by the logical offset its first block begins with, in this
// segment's own index and nowhere else. One the cache keeps reaches from one
// anchor to the next and is read with one admitted read, as the cache's own
// work, for everyone who misses it together. A stretch that is not such a
// run, or one the cache has no room for, is read for its reader alone under
// that reader's budget.
//
// While something reads the segment its writer's observer hands over a share
// of each group once its bytes are made. They are taken here, off the
// writer's path, into the run the segment is filling, which ends where the
// index chooses the next anchor: a fed run and one read back from the device
// are the same entry under the same key. Each part's cost is taken before it
// is made, and a run there is no room for is dropped and read later from the
// device or from the writer. A reader that comes before the feed's own work
// took what it wants takes it itself; see catch_up().
//
// What may be read is what is durable in at least one place: below where the
// segment's own barriers reached, or where its owner reported it durable,
// its WAL included. A fed run answers from the cache once that has passed
// it; until then, and while it is still filling, what lies below it is read
// through tail(). The device is read only below where the writer's bytes in
// the file end; what lies between there and what may be read is in the
// writer's memory.
//
// `cache` and `budget` outlive it, and it outlives the writer whose observer
// it gave. close() is joined before it is destroyed.
class segment_runs final : public runtime::shard_affine {
public:
    [[nodiscard]] static runtime::result<std::unique_ptr<segment_runs>> make(
      page_cache& cache,
      workload_budget& budget,
      segment_history_context history,
      segment_runs_config config);
    segment_runs(const segment_runs&) = delete;
    segment_runs& operator=(const segment_runs&) = delete;
    ~segment_runs();

    [[nodiscard]] const segment_history_context& history() const& noexcept {
        return history_;
    }
    const segment_history_context& history() const&& = delete;
    [[nodiscard]] const segment_run_limits& limits() const& noexcept {
        return config_.limits;
    }
    const segment_run_limits& limits() const&& = delete;

    // A reader's session on the segment: close() waits for whoever holds
    // one. Refused with closed once close() has begun.
    [[nodiscard]] runtime::result<seastar::gate::holder> enter() noexcept;

    // Something read the segment now: it is fed from here on, for as long
    // as the configured time after its last read.
    void touch() noexcept;
    // A read of the segment failed for `why`. Whoever the configuration
    // names is told when the failure is the data's or its device's:
    // everything a reader reads lies below its durable bound.
    void read_failed(const runtime::operation_error& why) noexcept;
    // Its owner says `readers` readers are established on the segment: it
    // is fed while that is not zero, and a fed run is kept for that many
    // reads. Zero hands the question back to touch().
    void establish(std::uint32_t readers) noexcept;
    [[nodiscard]] bool fed() const noexcept { return wanted_; }

    // Of a segment whose writer reports here: where what is durable in at
    // least one place ends, and where its bytes in the file end. Nothing is
    // readable of one that never had a writer report here, and all of what
    // is readable of it is in its file.
    [[nodiscard]] runtime::file_position durable_position() const noexcept {
        return readable_;
    }
    [[nodiscard]] runtime::file_position written_position() const noexcept {
        return written_.value_or(
          runtime::file_position{std::numeric_limits<std::uint64_t>::max()});
    }

    // The run the cache keeps that begins with the greatest offset at or
    // below `target`, held. This is a use of it unless `use` says it is
    // not: a reader that looks first and uses what it found only when the
    // run holds its offset says so, and marks its use itself.
    [[nodiscard]] cache_ref<segment_run>
    floor(model::range_logical_offset target, bool use = true) noexcept {
        assert_current();
        return runs_.floor(target, use);
    }
    // The run the writer fed that begins with the greatest offset at or
    // below `target` and is not answered by floor() yet.
    [[nodiscard]] std::optional<segment_tail>
    tail(model::range_logical_offset target) noexcept;
    // A reader that neither of those answered takes, before it reads
    // anything, what the writer handed over and the feed's own work has
    // not taken yet: in order and as that work would, up to the group that
    // holds `target`. It stops short at a group after one that was not kept,
    // at a lapse the feed's work has not handled, once the segment is
    // drained or its seal handled, and once one run's worth of bytes was
    // copied; the rest is the feed's own work. Nothing is taken when no
    // group waiting holds `target`. True when something was taken: the
    // reader looks again.
    bool catch_up(model::range_logical_offset target) noexcept;

    // The run from `start` to `end`, held: from the cache, from the load in
    // flight, or read once as the cache's own work for everyone who asks
    // meanwhile. `end` is where the next anchor's block begins, or where
    // the extent ends, and lies at or below `bound`. `readers` are the reads
    // known to be still to come. Refused with queue_full or out_of_range
    // when the cache cannot keep it.
    //
    // `lender` is the budget of a reader that was refused so: the run is
    // then read all the same, once for everyone who misses it together,
    // under a reservation of that budget, and kept by nothing once they let
    // go. Refused as before when that budget cannot hold it either, and its
    // reader then reads alone.
    [[nodiscard]] seastar::future<runtime::result<cache_ref<segment_run>>>
    fetch(
      segment_run_start start,
      runtime::file_position end,
      const segment_data_source& source,
      runtime::file_position bound,
      seastar::abort_source& caller,
      std::uint32_t readers,
      workload_budget* lender = nullptr);
    // The complete blocks from `start` that lie below `end`, as many as one
    // read holds, for one reader alone: read under a reservation of that
    // reader's `budget`, which lives as long as what is returned, and kept
    // by nothing. The run ends where its last complete block ends, which
    // may be before `end`: its reader goes on from there.
    [[nodiscard]] seastar::future<runtime::result<cache_ref<segment_run>>>
    read_alone(
      segment_run_start start,
      runtime::file_position end,
      const segment_data_source& source,
      runtime::file_position bound,
      workload_budget& budget,
      seastar::abort_source& caller);
    // As read_alone(), from what `held` gives of the writer's memory: for
    // what is durable in at least one place and not in the file yet. The
    // run keeps shares of the writer's bytes, and is charged what they keep
    // alive. Refused as `held` refuses, and with out_of_range when there is
    // no `held`, which a seal's move takes away.
    [[nodiscard]] seastar::future<runtime::result<cache_ref<segment_run>>>
    read_held(
      segment_run_start start,
      runtime::file_position end,
      const segment_read_held& held,
      workload_budget& budget,
      seastar::abort_source& caller);

    // Starts the read of the run from `start` to `end` for nobody in
    // particular: the run a reader expects to want next. As fetch() reads
    // it, and kept for `readers` reads. It takes a load slot and the run's
    // bytes without waiting or is dropped, and nobody waits for it, so a
    // reader that does not come for it owes nothing. True when a read was
    // started.
    bool prefetch(
      segment_run_start start,
      runtime::file_position end,
      const segment_data_source& source,
      runtime::file_position bound,
      std::uint32_t readers);
    // Reads started for nobody in particular.
    [[nodiscard]] std::uint64_t prefetches() const noexcept {
        return prefetched_;
    }
    // Reads of the device that began at or past where the oldest run the
    // writer is feeding begins: of a tail the feed holds, or should.
    [[nodiscard]] std::uint64_t tail_device_reads() const noexcept {
        return tail_device_reads_;
    }
    // Whether the run that begins with `base` is the cache's, is being
    // read, or is being fed. Not a use of it.
    [[nodiscard]] bool holds(model::range_logical_offset base) const noexcept {
        assert_current();
        return runs_.holds(base);
    }

    // The segment is sealed: nothing more is written to it, and all of it is
    // in its file. The run that was filling is whole, and answers from the
    // cache like any other once it is readable; nothing a reader holds
    // changes, and the runs the cache keeps are the sealed segment's runs.
    // Called once the seal is done.
    void seal() noexcept;

    // What the segment's writer is given to report to: `index` chooses the
    // anchors and is told what may be read, and what is made while the
    // segment is fed is handed over here.
    [[nodiscard]] segment_block_observer
    observer(active_sparse_index& index) noexcept;

    // Resolves once everything the writer handed over so far is taken, and
    // every run that is complete and readable answers from the cache.
    [[nodiscard]] seastar::future<> settled();
    // Runs the writer fed that the cache does not answer with yet.
    [[nodiscard]] std::size_t pending() const noexcept {
        return pending_.size();
    }
    // Groups the writer handed over that are not taken yet.
    [[nodiscard]] std::size_t waiting() const noexcept {
        return waiting_.size();
    }
    // Written groups that were not kept: there was no room for them, or
    // they could not be shared.
    [[nodiscard]] std::uint64_t dropped() const noexcept { return dropped_; }

    // The segment's readers are drained: for its close, and before it is
    // removed. From its first step no session is let in and no read is
    // asked for; a session that is in keeps what the cache holds and is
    // refused what is not there. A read asked for before that step ends,
    // also one that had not reached the device yet. Then the feed ends,
    // every session and every such read is waited for, and every run is let
    // go. A run something still holds lives on with its charge: what was
    // read is its holder's, and no file is needed for it.
    //
    // No step is skipped for the failure of one before it. What failed is
    // kept, and is what this returns and failure() says. Called once: a
    // call while a drain is in flight is refused, and one after it returns
    // what the drain kept.
    [[nodiscard]] seastar::future<runtime::result<void>> close();
    [[nodiscard]] const runtime::first_failure& failure() const& noexcept {
        return debt_;
    }
    const runtime::first_failure& failure() const&& = delete;

private:
    struct noted_block final {
        coverage records;
        aligned_envelope_layout envelope;
        bool chosen;
    };
    struct fed_group final {
        runtime::file_position begin;
        // A share of what the writer's group is charged to: released after
        // the bytes it pays for, once the group is copied or dropped.
        workload_reservation paid;
        bytes::fragmented_buffer bytes;
        std::vector<noted_block> blocks;
        // A group before this one was not kept.
        bool after_gap;
    };
    struct fed_run final {
        fed_run(
          cache_load<segment_run> led,
          model::range_logical_offset base,
          runtime::file_position begin,
          const workload_budget& budget) noexcept
          : load(std::move(led))
          , base(base)
          , building(begin, budget) {}
        cache_load<segment_run> load;
        model::range_logical_offset base;
        segment_run_builder building;
        // The next anchor was chosen: nothing more is added.
        bool closed{false};
    };

    segment_runs(
      page_cache& cache,
      workload_budget& budget,
      segment_history_context history,
      segment_runs_config config) noexcept;

    // What a run of `length` bytes is admitted for before it is read. It is
    // charged what it holds once it is there.
    [[nodiscard]] runtime::result<byte_count>
    estimate(std::uint64_t length) const noexcept;
    // What reading `length` bytes at `position` holds: the aligned span that
    // encloses them, which the runtime reads and keeps whole.
    [[nodiscard]] std::uint64_t enclosing(
      runtime::file_position position, std::uint64_t length) const noexcept {
        const auto unit = history_.alignment.bytes().value();
        const auto begin = position.value() - position.value() % unit;
        const auto end = position.value() + length;
        return (end + unit - 1) / unit * unit - begin;
    }
    seastar::future<runtime::result<segment_run>> load(
      const segment_data_source& source,
      segment_run_start start,
      runtime::file_position end,
      runtime::file_position bound,
      seastar::abort_source& stop);

    void note(
      std::span<const segment_block_layout> blocks,
      active_sparse_index& index) noexcept;
    void feed(
      runtime::file_position begin,
      bytes::fragmented_buffer& bytes,
      const workload_reservation& paid) noexcept;
    void drop() noexcept;
    // Wakes the feed's own work.
    void stir() noexcept;
    // What may be read reaches at least where the segment's own barriers
    // or its owner's reports have; `index` is told.
    void readable(active_sparse_index& index) noexcept;
    // Counts a read of the device from `at`; see tail_device_reads().
    void device_read(runtime::file_position at) noexcept;
    // The feed's own work: takes what the writer handed over, in order.
    seastar::future<> copy();
    // Takes the group the writer handed over first.
    void take_front() noexcept;
    void take(fed_group& group);
    // Takes `[from, until)` of a group, which holds `count` of its blocks
    // from `first`, into the run that is filling.
    [[nodiscard]] bool append(
      fed_group& group,
      std::size_t first,
      std::size_t count,
      std::uint64_t from,
      std::uint64_t until);
    void begin_run(const noted_block& block);
    // Gives up the run that is filling: its bytes are read from the device
    // by whoever wants them.
    void abandon() noexcept;
    // Hands the cache every run that is complete and durable.
    void publish() noexcept;

    page_cache& cache_;
    workload_budget& budget_;
    segment_history_context history_;
    segment_runs_config config_;
    cache_index<model::range_logical_offset, segment_run> runs_;
    seastar::gate loads_;

    // The feed. `wanted_` is what the writer looks at.
    bool wanted_{false};
    bool closing_{false};
    bool closed_{false};
    // Nothing more is written; and the run that was filling was settled.
    bool sealed_{false};
    bool finished_{false};
    std::uint64_t prefetched_{0};
    runtime::first_failure debt_;
    // Damage of its committed data, and a read its device failed, were told.
    bool damage_told_{false}, device_told_{false};
    // The run that is filling is missing bytes, or there is none: nothing
    // is copied until the next anchor.
    bool skipping_{true};
    bool gap_{false};
    // A group could not be noted whole, and is not kept.
    bool torn_{false};
    // The segment stopped being fed: the run that was filling is given up.
    bool lapsed_{false};
    std::uint32_t established_{0};
    runtime::monotonic_time read_{};
    // Where the segment's own barriers reached, where its owner reported,
    // the greater of the two, and where its bytes in the file end: nothing
    // until a writer reports here.
    runtime::file_position durable_{}, reported_{}, readable_{};
    std::optional<runtime::file_position> written_;
    std::uint64_t waiting_bytes_{0};
    std::uint64_t dropped_{0};
    // Bytes the feed copied rather than shared, and reads of the device
    // into a tail it feeds.
    std::uint64_t copied_{0};
    std::uint64_t tail_device_reads_{0};
    std::vector<noted_block> noted_;
    // The group the next feed is of was noted.
    bool noted_group_{false};
    seastar::circular_buffer<fed_group> waiting_;
    // Oldest first; only the last may still be filling.
    seastar::circular_buffer<std::unique_ptr<fed_run>> pending_;
    seastar::condition_variable wake_;
    // The feed's work was woken and has not gone back to waiting.
    bool stirring_{false};
    seastar::condition_variable idle_;
    std::optional<seastar::future<>> copier_;
};

} // namespace kwaque::storage
