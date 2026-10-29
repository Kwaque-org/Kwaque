#pragma once

#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/codec/envelope.h"
#include "src/model/segment_state.h"
#include "src/runtime/file.h"
#include "src/runtime/first_failure.h"
#include "src/runtime/time.h"
#include "src/storage/encoded_batch.h"
#include "src/storage/local_metadata.h"
#include "src/storage/retry_format.h"
#include "src/storage/segment_data.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/noncopyable_function.hh>

#include <array>
#include <cstdint>
#include <optional>
#include <span>
#include <utility>
#include <vector>

namespace kwaque::storage {
namespace detail {
struct segment_lifetime final {
    explicit segment_lifetime(workload_reservation value)
      : held(std::move(value)) {}
    workload_reservation held;
    runtime::first_failure failure;
    seastar::condition_variable changed;
};
class segment_write_descriptor;
} // namespace detail
template<
  runtime::file_system_backend Backend,
  typename Owner,
  runtime::monotonic_clock Clock>
class segment_writer;

inline constexpr std::uint32_t maximum_segment_group_blocks = 64;
// The most a run of held groups may hold before it is written: its copies
// then take at most half of one write's fragments.
inline constexpr byte_count maximum_segment_hold_bytes{8_MiB};

enum class segment_handle_state : std::uint8_t {
    closed,
    open,
    evicted,
    reopening
};

// Observations only. The byte end includes stored footers; record ends do not.
struct segment_writer_position final {
    model::range_logical_end logical;
    model::segment_relative_end physical;
    runtime::file_position bytes;
    std::uint32_t blocks{0}, footers{0}, retry_entries{0};
    bool operator==(const segment_writer_position&) const noexcept = default;
};
// reported is the end of what the append owner published as durable in at
// least one place, its WAL included: never past reserved, and possibly past
// written while its groups are held.
struct segment_writer_positions final {
    segment_writer_position reserved, written, durable, reported;
};

// What a reader of a segment that is still written is given: the right to
// read its data, the segment it is of, and how far it may read. `durable` is
// where the segment's durable bytes ended when this was asked for: a
// snapshot, which later appends do not move. Bytes below it never change,
// so they are read without their writer and stay readable when the segment
// is sealed. A reader that wants what was appended since asks again.
// `written` is where its bytes in the file ended then, and `reported` where
// what its owner published ended, as far as the groups made then reach:
// the bytes between the two are only in the writer's memory and its WAL.
struct segment_data_view final {
    segment_data_pin pin;
    segment_history_context history;
    runtime::file_position durable, written, reported;
};

// Captured by one owner, not constructible from caller-selected coordinates.
// The token is an in-memory lifetime, independent of both SC generation and
// the sequence used to publish metadata. Keeping it retains its admission.
class segment_captured_boundary final {
public:
    [[nodiscard]] segment_context context() const noexcept {
        return history_.segment;
    }
    [[nodiscard]] segment_history_context history() const noexcept {
        return history_;
    }
    // The named footer describes the prefix before itself, including earlier
    // stored footers. end().bytes instead includes this footer's own bytes.
    [[nodiscard]] storage::coverage covered() const noexcept {
        return storage::coverage{
          model::range_logical_span::make(history_.logical_origin, end_.logical)
            .value(),
          model::segment_relative_span::make(
            history_.physical_origin, end_.physical)
            .value(),
          model::file_byte_span::make(
            history_.data_start, footer_ ? footer_->begin() : end_.bytes)
            .value()};
    }
    [[nodiscard]] segment_writer_position end() const noexcept { return end_; }
    [[nodiscard]] const std::optional<model::file_byte_span>&
    footer() const& noexcept {
        return footer_;
    }
    const std::optional<model::file_byte_span>& footer() const&& = delete;
    bool operator==(const segment_captured_boundary&) const noexcept = default;

private:
    template<
      runtime::file_system_backend Backend,
      typename Owner,
      runtime::monotonic_clock Clock>
    friend class segment_writer;
    using lifetime = detail::segment_lifetime;
    segment_captured_boundary(
      seastar::lw_shared_ptr<lifetime> lifetime,
      segment_history_context history,
      segment_writer_position end,
      std::optional<model::file_byte_span> footer)
      : lifetime_(std::move(lifetime))
      , history_(history)
      , end_(end)
      , footer_(footer) {}
    seastar::lw_shared_ptr<lifetime> lifetime_;
    segment_history_context history_;
    segment_writer_position end_;
    std::optional<model::file_byte_span> footer_;
};

// Segment-only successful barrier evidence. Layout/metadata decoding and
// captured positions cannot construct a receipt or a producer acknowledgement.
class segment_durable_receipt final {
public:
    [[nodiscard]] const segment_captured_boundary& boundary() const& noexcept {
        return boundary_;
    }
    const segment_captured_boundary& boundary() const&& = delete;

private:
    template<
      runtime::file_system_backend Backend,
      typename Owner,
      runtime::monotonic_clock Clock>
    friend class segment_writer;
    explicit segment_durable_receipt(segment_captured_boundary value)
      : boundary_(std::move(value)) {}
    segment_captured_boundary boundary_;
};

struct segment_barrier_outcome final {
    runtime::first_failure failure;
    std::optional<segment_durable_receipt> receipt;
};

struct segment_block_layout final {
    coverage records;
    aligned_envelope_layout envelope;
};
// Follows a segment's blocks, on the owner's shard, for state derived from
// them. No call may throw and none can fail the segment: nothing a caller
// derives here is part of an append's result. All run inside the owner's
// own steps and must not call back into it.
struct segment_block_observer final {
    // The blocks of one group, in file order, once its bytes are final: they
    // may still be held for a later write, and may not be durable. Borrowed
    // for the call.
    seastar::noncopyable_function<void(
      std::span<const segment_block_layout>) noexcept>
      blocks;
    // Every block that begins below this position is durable.
    seastar::noncopyable_function<void(runtime::file_position) noexcept>
      durable;
    // The bytes of the group `blocks` was just told of, its blocks and the
    // footer after them if it has one, as they will lie in the file from the
    // position given, for whoever keeps them for those reading behind the
    // writer. Called only while `*wanted` is true, which costs a segment
    // nobody reads one branch. The bytes are borrowed for the call and may
    // be shared: nothing here waits, copies or fails. The reservation is
    // what the bytes are charged to: whoever keeps a share of them keeps a
    // share of it, so that they stay charged for as long as they live.
    seastar::noncopyable_function<void(
      runtime::file_position,
      bytes::fragmented_buffer&,
      const workload_reservation&) noexcept>
      fed{};
    const bool* wanted{nullptr};
    // Every byte below this position is in the file. Optional.
    seastar::noncopyable_function<void(runtime::file_position) noexcept>
      written{};
    // The owner published everything below this position as durable in at
    // least one place, and every group below it is made: its bytes are in
    // the file or in the writer's memory. Optional.
    seastar::noncopyable_function<void(runtime::file_position) noexcept>
      reported{};
    // Before a group of this many blocks freezes: admits what keeping them
    // will take, inside the group's own admission. A refusal refuses the
    // group. Optional.
    seastar::noncopyable_function<runtime::result<void>(std::uint32_t)> admit{};
};
// The eventual frozen handoff binds all blocks and the footer to one captured
// owner. A prospective capacity check does not mint this value.
class segment_group_layout final {
public:
    segment_group_layout(segment_group_layout&&) noexcept = default;
    segment_group_layout& operator=(segment_group_layout&&) = delete;
    segment_group_layout(const segment_group_layout&) = delete;
    segment_group_layout& operator=(const segment_group_layout&) = delete;
    [[nodiscard]] const segment_captured_boundary& boundary() const& noexcept {
        return boundary_;
    }
    const segment_captured_boundary& boundary() const&& = delete;
    [[nodiscard]] std::span<const segment_block_layout>
    blocks() const& noexcept {
        return blocks_;
    }
    std::span<const segment_block_layout> blocks() const&& = delete;
    // Where the segment's durable bytes ended when the group froze: the end
    // of the newest barrier's cut, or the data start. Each of its blocks
    // carries it, and so does each PREPARE made for them.
    [[nodiscard]] runtime::file_position certified_end() const noexcept {
        return certified_end_;
    }

private:
    friend class detail::segment_write_descriptor;
    template<
      runtime::file_system_backend Backend,
      typename Owner,
      runtime::monotonic_clock Clock>
    friend class segment_writer;
    segment_group_layout(
      segment_captured_boundary boundary,
      std::vector<segment_block_layout> blocks,
      runtime::file_position certified_end,
      workload_reservation held)
      : held_(std::move(held))
      , boundary_(std::move(boundary))
      , blocks_(std::move(blocks))
      , certified_end_(certified_end) {}
    workload_reservation held_;
    segment_captured_boundary boundary_;
    std::vector<segment_block_layout> blocks_;
    runtime::file_position certified_end_;
};

struct segment_admission_limits final {
    std::uint32_t maximum_blocks{maximum_object_entries};
    std::uint32_t maximum_retry_entries{maximum_object_entries};
    std::uint32_t maximum_retry_pages{maximum_object_pages};
    byte_count metadata_bytes{64_KiB};
    byte_count working_bytes{8_MiB};
    byte_count execution_bytes{64_KiB};
    [[nodiscard]] runtime::result<void> validate() const noexcept;
};
enum class segment_capacity_decision : std::uint8_t {
    fits,
    roll_required,
    impossible
};
// Why a segment that is appended to takes a request no more, so that
// whoever owns the segments follows it with another. A request an empty
// segment could not take either is impossible, and has none of these.
enum class segment_roll_reason : std::uint8_t {
    // Its extent has no room for the group, the footer after it and the
    // root its seal writes.
    size,
    // Its blocks, footers or records are as many as a segment holds.
    count,
    // Its seal can publish no more completed-retry entries, or it holds as
    // many streams as its retry depth allows.
    retry_state,
    // Its lifetime ended.
    age,
    // A restart found it: it is never appended again.
    recovered,
    // Its device is short of space, stalled or failed.
    device,
};
// Any of them together.
class segment_roll_reasons final {
public:
    constexpr segment_roll_reasons() noexcept = default;
    constexpr explicit segment_roll_reasons(segment_roll_reason one) noexcept
      : bits_(bit(one)) {}
    [[nodiscard]] constexpr bool empty() const noexcept { return bits_ == 0; }
    [[nodiscard]] constexpr bool has(segment_roll_reason one) const noexcept {
        return (bits_ & bit(one)) != 0;
    }
    constexpr segment_roll_reasons& add(segment_roll_reason one) noexcept {
        bits_ = static_cast<std::uint8_t>(bits_ | bit(one));
        return *this;
    }
    constexpr segment_roll_reasons& add(segment_roll_reasons more) noexcept {
        bits_ = static_cast<std::uint8_t>(bits_ | more.bits_);
        return *this;
    }
    // These, less those of `other`.
    [[nodiscard]] constexpr segment_roll_reasons
    without(segment_roll_reasons other) const noexcept {
        segment_roll_reasons rest;
        rest.bits_ = static_cast<std::uint8_t>(bits_ & ~other.bits_);
        return rest;
    }
    bool operator==(const segment_roll_reasons&) const noexcept = default;

private:
    [[nodiscard]] static constexpr std::uint8_t
    bit(segment_roll_reason one) noexcept {
        return static_cast<std::uint8_t>(1U << static_cast<unsigned>(one));
    }
    std::uint8_t bits_{0};
};
// What a segment that is appended to still has room for. Whoever owns the
// segments reads it to make the successor before the room is gone. Nothing
// here is reserved: a request is decided when it is made.
struct segment_left final {
    // Encoded bytes of blocks its next group may hold, after the footer
    // that follows them and the root its seal writes. A group of one block
    // of exactly this size is taken, and one byte more is not; a group of
    // more blocks may find less, since the root grows with their facts.
    byte_count bytes;
    // Blocks it may still take, by its counts; none when it has no byte for
    // one.
    std::uint32_t blocks{0};
    // Completed-retry entries its seal can still publish. A batch takes one
    // while its stream holds fewer than the retry depth, so a stream that
    // is new to the segment needs one.
    std::uint32_t retry_entries{0};
    // When its lifetime ends. None before its first accepted group.
    std::optional<runtime::monotonic_time> deadline;
};
// What a segment's owner is told of it that no append's outcome says.
// Neither call may throw, and neither may call back into the writer: both
// run inside its own steps.
struct segment_roll_observer final {
    // Its first group was accepted: its lifetime runs, and ends then.
    seastar::noncopyable_function<void(runtime::monotonic_time) noexcept> aging;
    // A request was refused as roll-required, for these.
    seastar::noncopyable_function<void(segment_roll_reasons) noexcept> refused;
};
struct segment_disk_obligation final {
    // Total projected data file, including its header, group footer and root.
    byte_count data;
    byte_count retry_bundle;
    // Replacement pointer overlaps its old version; the immutable bundle's
    // temporary becomes its final name and is counted once above.
    byte_count publication_overlap;
    byte_count descriptor;
};
struct segment_capacity_plan final {
    segment_capacity_decision decision{segment_capacity_decision::impossible};
    std::optional<segment_writer_position> end;
    // The footer that closes a run held open before the group, its blocks
    // and the footer after them.
    byte_count closing, blocks, footer, sealed_root;
    byte_count staging_bytes;
    std::uint32_t retry_pages{0}, retry_page_entries{0};
    segment_disk_obligation disk{};
    // Every reason a roll_required decision has; none otherwise.
    segment_roll_reasons reasons{};
};

// Pure, bounded lookahead. Borrow exact children only for this call. Neither
// this value nor its projected positions freeze coordinates, validate children
// under a new policy, or authorize an append. `retry_entries` is how many
// completed-retry entries the group adds to the seal, at most one per full
// batch/BID whatever records it retains; absent, each batch adds one.
[[nodiscard]] runtime::result<segment_capacity_plan> plan_segment_capacity(
  const local_segment_descriptor&,
  runtime::file_position data_start,
  segment_writer_position current,
  std::span<const encoded_assigned_batch>,
  segment_admission_limits,
  const codec::limits&,
  bytes::allocation_charge_fn,
  storage_alignment metadata_alignment,
  byte_count finalization_bytes,
  std::optional<std::uint32_t> retry_entries = std::nullopt);

// The most a segment of this descriptor may come to hold on its device: its
// whole extent, and the retry bundle of a seal that holds as many facts as
// its extent has room for blocks. A segment is admitted against this before
// it is created, so that the space it was promised is there when it writes.
// What else it writes is small and is the device's floor's: its descriptor,
// its pointers, its index bundle and temporaries.
[[nodiscard]] runtime::result<byte_count> segment_disk_bound(
  const local_segment_descriptor&,
  segment_admission_limits,
  const codec::limits&,
  bytes::allocation_charge_fn,
  byte_count finalization_bytes);

namespace detail {
// Uses the same served root/page projection as group admission. Even an
// untouched empty generation must be sealable under its startup allowance.
[[nodiscard]] runtime::result<void> validate_empty_segment_seal(
  storage_alignment,
  segment_admission_limits,
  const codec::limits&,
  bytes::allocation_charge_fn,
  byte_count finalization_bytes);

// Everything a plan reads from one batch. Equal inputs over an equal current
// position plan identically, so a group frozen from children equal to its
// prepared inputs keeps its prepared plan; any difference plans again.
struct segment_plan_input final {
    assigned_batch_info info;
    byte_count size;
    std::size_t fragments{0};
    bool operator==(const segment_plan_input&) const noexcept = default;
};
[[nodiscard]] segment_plan_input
plan_input(const encoded_assigned_batch&) noexcept;

// The part of a plan fixed by one owner's descriptor, data start, limits,
// policy, charge and alignments: the footer, publication-pointer, descriptor
// and retry-page shapes. It is computed once per owner instead of per plan.
// The retry projection of a fresh segment depends only on the batch count
// and is cached per count on first use.
struct segment_capacity_constants final {
    [[nodiscard]] static runtime::result<segment_capacity_constants> make(
      const local_segment_descriptor&,
      runtime::file_position data_start,
      segment_admission_limits,
      const codec::limits&,
      bytes::allocation_charge_fn,
      storage_alignment metadata_alignment,
      byte_count finalization_bytes);
    // Root bytes of a fresh segment's retry projection for this many batches,
    // absent when no projection fits.
    [[nodiscard]] std::optional<byte_count> fresh_root(std::size_t batches);
    // Entries per retry page when sealing this many completed facts, absent
    // when no projection fits.
    [[nodiscard]] std::optional<std::uint32_t>
    seal_page_entries(std::uint64_t entries) const;
    // Root bytes of a segment sealed with this many completed facts, absent
    // when no projection fits. The answer for the count last asked is kept.
    [[nodiscard]] std::optional<byte_count> sealed_root(std::uint64_t entries);

    local_segment_descriptor descriptor;
    runtime::file_position data_start;
    segment_admission_limits limits;
    codec::limits policy;
    bytes::allocation_charge_fn charge{nullptr};
    byte_count finalization_bytes;
    // Work limits admit no group at all.
    bool tiny{false};
    codec::envelope_extent_limits block_limits{};
    // A durable footer's bytes before the padding that ends its span; absent
    // when even one that begins on a boundary cannot be laid out or staged.
    std::optional<byte_count> footer_bytes;
    // The most a footer's staging costs, wherever it begins.
    std::optional<byte_count> closing_staging;
    // Absent unless every publication-pointer shape fits its workspace.
    std::optional<byte_count> pointer_bytes;
    std::optional<byte_count> descriptor_bytes;
    // Retry entries per page on the wire; zero when none fit.
    std::uint32_t retry_wire_capacity{0};
    std::array<std::optional<byte_count>, maximum_segment_group_blocks + 1>
      fresh_roots{};
    std::array<bool, maximum_segment_group_blocks + 1> fresh_known{};
    std::optional<std::uint64_t> root_asked;
    std::optional<byte_count> root_kept;
};
// `open`: blocks since the last footer end at `current`, so a footer that
// closes them is counted before the group. `retry_entries` as above.
[[nodiscard]] runtime::result<segment_capacity_plan> plan_segment_capacity(
  segment_capacity_constants&,
  segment_writer_position current,
  std::span<const encoded_assigned_batch* const>,
  bool open = false,
  std::optional<std::uint32_t> retry_entries = std::nullopt);
} // namespace detail

// A rejectable resource preparation. It owns prospective work/metadata units,
// but does not own a frozen byte range or a WAL authorization. Release without
// dispatch is therefore safe; the later frozen handoff has a separate type.
class segment_prepared_group final {
public:
    segment_prepared_group(segment_prepared_group&&) noexcept = default;
    segment_prepared_group& operator=(segment_prepared_group&&) = delete;
    [[nodiscard]] const segment_capacity_plan& plan() const& noexcept {
        return plan_;
    }
    const segment_capacity_plan& plan() const&& = delete;
    // The bytes its freeze would take: from the reserved end it was
    // measured at to the end of its footer's padding, with the footer that
    // closes a run held open there.
    [[nodiscard]] model::file_byte_span extent() const noexcept {
        return model::file_byte_span::make(base_.end().bytes, plan_.end->bytes)
          .value();
    }

private:
    template<
      runtime::file_system_backend Backend,
      typename Owner,
      runtime::monotonic_clock Clock>
    friend class segment_writer;
    segment_prepared_group(
      segment_captured_boundary base,
      std::optional<segment_captured_boundary> closed,
      segment_capacity_plan plan,
      std::vector<detail::segment_plan_input> inputs,
      workload_reservation held,
      workload_reservation control,
      byte_count retained,
      codec::limits policy,
      std::uint32_t retry_entries)
      : held_(std::move(held))
      , control_(std::move(control))
      , base_(std::move(base))
      , closed_(std::move(closed))
      , plan_(std::move(plan))
      , inputs_(std::move(inputs))
      , retained_(retained)
      , policy_(policy)
      , retry_entries_(retry_entries) {}
    workload_reservation held_;
    // The queue node the freeze installs, held so the freeze takes no fresh
    // ordinary admission after its awaits.
    workload_reservation control_;
    segment_captured_boundary base_;
    // Measured over a run held open: the base once that run is closed by
    // its own footer, from which the group freezes as planned.
    std::optional<segment_captured_boundary> closed_;
    segment_capacity_plan plan_;
    std::vector<detail::segment_plan_input> inputs_;
    // Staging plus the children's retained bytes the freeze adds as pending.
    byte_count retained_;
    codec::limits policy_;
    // Completed-retry entries the group adds to the seal.
    std::uint32_t retry_entries_;
};
struct segment_group_preparation final {
    segment_capacity_decision decision;
    std::optional<segment_prepared_group> prepared;
    // Every reason a roll_required decision has; none otherwise.
    segment_roll_reasons reasons{};
};
} // namespace kwaque::storage
