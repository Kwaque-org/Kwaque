#pragma once

#include "src/base/result.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/cooperative.h"
#include "src/codec/error.h"
#include "src/codec/integer.h"
#include "src/codec/limits.h"
#include "src/codec/transaction.h"
#include "src/model/record.h"
#include "src/runtime/time.h"

#include <seastar/core/future.hh>

#include <optional>
#include <span>
#include <vector>

namespace kwaque::model {

struct record_sizes final {
    byte_count body_bytes;
    byte_count encoded_bytes;
    byte_count header_payload_bytes;

    bool operator==(const record_sizes&) const noexcept = default;
};

// Synchronous metadata-only leaves: at most 64 header entries, no payload
// traversal, allocation, share or ownership transfer. Includes the minimal
// unsigned size prefix in encoded_bytes; header payload excludes its framing.
// The enclosing async owner accounts this bounded leaf's work separately.
[[nodiscard]] result<record_sizes> record_encoded_size(
  record_fields fields,
  const std::optional<bytes::fragmented_buffer>& key,
  const std::optional<bytes::fragmented_buffer>& value,
  std::span<const record_header> headers,
  const codec::limits& policy = codec::limits::defaults());

[[nodiscard]] result<record_sizes> record_encoded_size(
  const record& value, const codec::limits& policy = codec::limits::defaults());

// The same size calculation, admitting each bounded metadata group against
// shared work. At least 16 work items are required. The record remains alive,
// unmoved and immutable until completion; native/frame storage is pre-admitted.
[[nodiscard]] seastar::future<codec::result<record_sizes>>
record_encoded_size_cooperatively(
  const record& value,
  codec::cooperative_work& work,
  codec::error anchor = codec::error{errc::success});
void record_encoded_size_cooperatively(
  const record&&,
  codec::cooperative_work&,
  codec::error = codec::error{errc::success}) = delete;

// Validate before moving. Rejection/exception leaves the supplied owners
// unchanged; success transfers them without allocation or a payload copy.
// Distinct donor objects are required even when their backing is shared.
// The complete owner also fits the configured aggregate fragment count. These
// factories prove grammar/record-size bounds, not memory admission or batch
// membership. Input allocations are already owned/accounted by callers.
[[nodiscard]] result<record_header> make_record_header(
  bytes::fragmented_buffer&& name,
  std::optional<bytes::fragmented_buffer>&& value,
  const codec::limits& policy = codec::limits::defaults());

[[nodiscard]] result<record> make_record(
  record_fields fields,
  std::optional<bytes::fragmented_buffer>&& key,
  std::optional<bytes::fragmented_buffer>&& value,
  std::vector<record_header>&& headers,
  const codec::limits& policy = codec::limits::defaults());

struct record_memory_usage final {
    byte_count backing;
    // Header vector capacity plus every buffer's descriptor history and
    // possible share-control promotion. No per-field backing deduplication.
    byte_count metadata;
    byte_count largest_allocation;
    item_count fragments;

    bool operator==(const record_memory_usage&) const noexcept = default;
};

// Validate logical bounds and report conservative physical ownership costs
// through bounded native range queries. charge must be a verified, monotone,
// nonallocating served-capacity bound. No memory is allocated/shared/reserved
// by the cost traversal itself; native coroutine machinery is caller-admitted.
// Metadata groups require at least 16 work items in the complete quantum.
// Record, work and abort source remain alive, unmoved and exclusive until the
// returned future completes. Rvalue records are rejected at the interface.
[[nodiscard]] seastar::future<codec::result<record_memory_usage>>
record_allocation_cost(
  const record& value,
  codec::cooperative_work& work,
  bytes::allocation_charge_fn charge,
  codec::error anchor = codec::error{errc::success});
void record_allocation_cost(
  const record&&,
  codec::cooperative_work&,
  bytes::allocation_charge_fn,
  codec::error = codec::error{errc::success}) = delete;

// Reserve this independent input owner once, before further structural shares.
// Includes conservative existing-allocation/fragment/retained limits and the
// separate metadata ceiling. Returned residuals are snapshots, not refunds.
// Caller excludes record inline storage, opaque owners, other live objects and
// verified native/frame costs first; keep this reservation for the owner's
// lifetime, including after a failed operation that promoted share controls.
[[nodiscard]] seastar::future<codec::result<codec::decode_budget>>
reserve_record_input(
  const record& value,
  codec::cooperative_work& work,
  codec::decode_budget memory,
  codec::error anchor = codec::error{errc::success});
void reserve_record_input(
  const record&&,
  codec::cooperative_work&,
  codec::decode_budget,
  codec::error = codec::error{errc::success}) = delete;

// Complete semantic comparison: relative fields, nullable presence, payload
// bytes and ordered duplicate headers. Fragmentation and allocated capacity do
// not affect equality. Growing byte work is split; all input/native/frame
// reservations remain with the caller. No owning shares or payload copies.
// Both values and borrowed work remain alive, unmoved and unmodified through
// completion. Cancellation is checked even on an unequal/empty fast path.
// The metadata leaf needs eight work items; nonempty byte comparisons also
// need room for two bytes and two fragment visits per admitted chunk.
[[nodiscard]] seastar::future<codec::result<bool>> records_equal(
  const record& left,
  const record& right,
  codec::cooperative_work& work,
  codec::error anchor = codec::error{errc::success});
void records_equal(
  const record&&,
  const record&,
  codec::cooperative_work&,
  codec::error = codec::error{errc::success}) = delete;
void records_equal(
  const record&,
  const record&&,
  codec::cooperative_work&,
  codec::error = codec::error{errc::success}) = delete;

enum class record_field : std::uint16_t {
    body_bytes = 64,
    attributes = 65,
    timestamp_delta = 66,
    logical_delta = 67,
    key_length = 68,
    key = 69,
    value_length = 70,
    value = 71,
    header_count = 72,
    header_name_length = 73,
    header_name = 74,
    header_value_length = 75,
    header_value = 76,
};

// Encode the record's relative fields, copying in bounded chunks into private
// immutable output. No assignment or absolute timestamp claim is made here.
// value stays alive, unmoved and unmodified through completion; all paths leave
// it unchanged. parent_remaining excludes its already-reserved input backing,
// metadata/opaque owners, other live usage and verified native/frame costs.
// charge is the same verified served-capacity bound used for those
// reservations. New output backing and the single transferred descriptor
// allocation are admitted here. The returned owner's reservation persists until
// it is released. Scalar groups need at least 128 work bytes / 64 items.
// Growing copies split within fragments; cleanup precedes the final
// non-suspending poll/transfer.
[[nodiscard]] seastar::future<codec::result<bytes::fragmented_buffer>>
encode_record(
  const record& value,
  codec::cooperative_work& work,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context context = {});
void encode_record(
  const record&&,
  codec::cooperative_work&,
  byte_count,
  bytes::allocation_charge_fn,
  codec::field_context = {}) = delete;

struct record_decode_context final {
    runtime::wall_time timestamp_base;
    range_logical_count original_count;
    // Remaining aggregate batch header allowance; zero is an actual allowance.
    item_count headers_remaining;
};

struct decoded_record final {
    record value;
    // Excludes conservative reservations for this value's owning field
    // descriptors/header capacity. The temporary child reservation is released
    // only after child teardown. Pass this residual to subsequent live results.
    codec::decode_budget remaining;
};

// Decode exactly one length-delimited record. expected supplies the original
// timestamp base/slot bound and remaining aggregate header allowance; this
// validates each relative field, not dense batch ordering or physical identity.
// Success advances precisely one record. Errors, exceptions and observed abort
// restore the parent's cursor and entry checkpoint depth, preserving siblings.
//
// reserve_decode_input has reserved this parent's backing/descriptors/potential
// promotion once. memory excludes that reservation and all native/frame/opaque
// costs. Aliased output retains the original backing reservation even after the
// parser dies; do not refund it while any returned field remains alive. New
// field/header metadata is charged separately and reflected in remaining.
// A failed call returns no reservation; memory itself is passed by value.
// Before retaining another result, use remaining and subtract this value's
// header count from the batch owner's aggregate header allowance.
// Parent, work and abort source remain alive, unmoved and exclusively used
// throughout. Cleanup is joined before the final poll/commit/result transfer.
// The fixed metadata group needs 128 work bytes / 64 items. Scalar primitives
// remain synchronous; native bounded share/publication/release leaves retain
// their substrate ceiling and are bracketed by shared-work checkpoints.
[[nodiscard]] seastar::future<codec::result<decoded_record>> decode_record(
  bytes::fragmented_buffer_parser& input,
  record_decode_context expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context = {},
  codec::input_boundary boundary = codec::input_boundary::open);

} // namespace kwaque::model
