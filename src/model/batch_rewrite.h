#pragma once

#include "src/codec/cooperative.h"
#include "src/codec/transaction.h"
#include "src/model/batch.h"

#include <span>

namespace kwaque::model {

// Original identity and coverage after all current records have been removed.
// Contains no payload, retained count, envelope or completion/ACK evidence.
// Later extent/retry owners retain actual completed results independently.
class removed_batch_coverage final {
public:
    [[nodiscard]] submitted_batch_context submitted() const noexcept {
        return submitted_;
    }
    [[nodiscard]] range_logical_span logical_span() const noexcept {
        return logical_span_;
    }
    [[nodiscard]] codec::semantic_batch_digest fingerprint() const noexcept {
        return fingerprint_;
    }
    bool operator==(const removed_batch_coverage&) const noexcept = default;

private:
    friend seastar::future<codec::result<removed_batch_coverage>>
    remove_all_records(assigned_batch&&, codec::cooperative_work&);

    removed_batch_coverage(
      submitted_batch_context submitted,
      range_logical_span logical_span,
      codec::semantic_batch_digest fingerprint) noexcept
      : submitted_(submitted)
      , logical_span_(logical_span)
      , fingerprint_(fingerprint) {}

    submitted_batch_context submitted_;
    range_logical_span logical_span_;
    codec::semantic_batch_digest fingerprint_;
};

// Consume an already validated assigned owner and discard its current record
// bytes, retaining only original metadata. No storage operation or original
// fingerprint recomputation occurs. An empty/moved-from donor rejects.
//
// Transfer occurs before the first await, including cancellation/rejection;
// outer coroutine-frame allocation failure precedes transfer. Work/abort stay
// alive, unmoved and exclusive. The caller reserves that frame and bounds
// native/opaque destruction work; no new buffer or metadata container is
// allocated. Existing backing reservations follow any independent aliases.
// Cleanup is joined before the final abort poll and coverage publication.
[[nodiscard]] seastar::future<codec::result<removed_batch_coverage>>
remove_all_records(assigned_batch&& source, codec::cooperative_work& work);

// Select a nonempty, strictly increasing list of original logical deltas from
// this assigned owner's actual current survivors. Missing/duplicate/out-of-span
// selections reject; callers cannot supply replacement records or resurrect
// removed data. Partial selections copy complete canonical extents into bounded
// private staging. Keeping every current survivor reuses the original backing
// after verifying every match. Original context/span/timestamp base and digest
// are retained; only retained count, header total and record-region lengths
// change.
//
// The source transfers before the first await, including
// failure/abort/exception; outer coroutine-frame allocation failure precedes
// transfer. Selection storage is borrowed, immutable and alive until
// completion. An empty selection rejects without producing a data batch. This
// operation supplies no completion proof. Use remove_all_records() when the
// requested output has no survivors.
//
// memory includes source backing/descriptors/promotion plus all new parser and
// output storage. Exclude other live objects, selection storage and verified
// native/frame/opaque reservations first. Input is reserved once; each scan
// child also excludes all simultaneously live output. Partial selections return
// fresh backing and release discarded source allocations before publication;
// unchanged selections carry the source backing reservation into the result.
// Metadata uses only the current record layout, never an all-record index.
// Work/abort remain alive, unmoved and exclusive throughout. Fixed layout work
// needs 4*sizeof(record_layout) bytes / 64 items, as for record-region
// scanning. context.origin names the first source record-region byte.
// All cleanup is joined in the owning coroutine before final
// poll/transfer.
[[nodiscard]] seastar::future<codec::result<assigned_batch>>
rewrite_assigned_batch(
  assigned_batch&& source,
  std::span<const range_logical_count> selected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context = {});

} // namespace kwaque::model
