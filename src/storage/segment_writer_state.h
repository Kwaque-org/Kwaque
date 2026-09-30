#pragma once

#include "src/codec/envelope.h"
#include "src/model/segment_state.h"
#include "src/runtime/file.h"
#include "src/runtime/first_failure.h"
#include "src/runtime/time.h"
#include "src/storage/encoded_batch.h"
#include "src/storage/local_metadata.h"
#include "src/storage/retry_format.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/shared_ptr.hh>

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
struct segment_writer_positions final {
    segment_writer_position reserved, written, durable;
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
      workload_reservation held)
      : held_(std::move(held))
      , boundary_(std::move(boundary))
      , blocks_(std::move(blocks)) {}
    workload_reservation held_;
    segment_captured_boundary boundary_;
    std::vector<segment_block_layout> blocks_;
};

struct segment_admission_limits final {
    std::uint32_t maximum_blocks{maximum_object_entries};
    std::uint32_t maximum_retry_entries{maximum_object_entries};
    std::uint32_t maximum_retry_pages{maximum_object_pages};
    byte_count metadata_bytes{65536};
    byte_count working_bytes{8U * 1024U * 1024U};
    byte_count execution_bytes{65536};
    [[nodiscard]] runtime::result<void> validate() const noexcept;
};
enum class segment_capacity_decision : std::uint8_t {
    fits,
    roll_required,
    impossible
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
    byte_count blocks, footer, sealed_root;
    byte_count staging_bytes;
    std::uint32_t retry_pages{0}, retry_page_entries{0};
    // Only this group's new full-BID facts. Earlier groups retain their own
    // admission; finalizer PageRefs are covered by the held seal working set.
    byte_count retry_metadata;
    segment_disk_obligation disk{};
};

// Pure, bounded lookahead. Borrow exact children only for this call. Neither
// this value nor its projected positions freeze coordinates, validate children
// under a new policy, or authorize an append. Capacity is per full batch/BID,
// independently of the number of records retained by that batch.
[[nodiscard]] runtime::result<segment_capacity_plan> plan_segment_capacity(
  const local_segment_descriptor&,
  runtime::file_position data_start,
  segment_writer_position current,
  std::span<const encoded_assigned_batch>,
  segment_admission_limits,
  const codec::limits&,
  bytes::allocation_charge_fn,
  storage_alignment metadata_alignment,
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

    local_segment_descriptor descriptor;
    runtime::file_position data_start;
    segment_admission_limits limits;
    codec::limits policy;
    bytes::allocation_charge_fn charge{nullptr};
    byte_count finalization_bytes;
    // Work limits admit no group at all.
    bool tiny{false};
    codec::envelope_extent_limits block_limits{};
    // Absent when the durable footer cannot be laid out or staged.
    std::optional<byte_count> footer_bytes, footer_staging;
    // Absent unless every publication-pointer shape fits its workspace.
    std::optional<byte_count> pointer_bytes;
    std::optional<byte_count> descriptor_bytes;
    // Served grant node charge, absent when the charge is rejected.
    std::optional<byte_count> fact_control;
    // Retry entries per page on the wire; zero when none fit.
    std::uint32_t retry_wire_capacity{0};
    std::array<std::optional<byte_count>, maximum_segment_group_blocks + 1>
      fresh_roots{};
    std::array<bool, maximum_segment_group_blocks + 1> fresh_known{};
};
[[nodiscard]] runtime::result<segment_capacity_plan> plan_segment_capacity(
  segment_capacity_constants&,
  segment_writer_position current,
  std::span<const encoded_assigned_batch* const>);
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

private:
    template<
      runtime::file_system_backend Backend,
      typename Owner,
      runtime::monotonic_clock Clock>
    friend class segment_writer;
    segment_prepared_group(
      segment_captured_boundary base,
      segment_capacity_plan plan,
      std::vector<detail::segment_plan_input> inputs,
      workload_reservation held,
      workload_reservation retries,
      codec::limits policy)
      : held_(std::move(held))
      , retries_(std::move(retries))
      , base_(std::move(base))
      , plan_(std::move(plan))
      , inputs_(std::move(inputs))
      , policy_(policy) {}
    workload_reservation held_;
    // Retry facts can remain needed after the group's codec/write workspace
    // has been released. Never make their lifetime pin that larger allowance.
    workload_reservation retries_;
    segment_captured_boundary base_;
    segment_capacity_plan plan_;
    std::vector<detail::segment_plan_input> inputs_;
    codec::limits policy_;
};
struct segment_group_preparation final {
    segment_capacity_decision decision;
    std::optional<segment_prepared_group> prepared;
};
} // namespace kwaque::storage
