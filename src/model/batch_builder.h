#pragma once

#include "src/bytes/fragmented_buffer_builder.h"
#include "src/codec/cooperative.h"
#include "src/model/batch.h"
#include "src/model/batch_identity.h"
#include "src/model/record.h"
#include "src/runtime/time.h"

#include <optional>

namespace kwaque::model {

// Incremental original submission with no clock, logical base or placement.
// add() borrows the complete record's payload/headers; supplied relative fields
// are replaced by checked timestamp - first_timestamp and the next dense
// ordinal. Donors remain unchanged on every path. Count advances only after a
// complete append. Any add/finalize rejection, exception or abort closes the
// builder and drains staging; it cannot later publish a shortened prefix. A
// native failure to allocate the outer coroutine frame occurs before entry and
// changes nothing. finalize() succeeds at most once; close() joins abandonment
// cleanup.
//
// Each operation uses the same policy/allocator profile and exclusive live
// work/abort owner. The builder and borrowed record remain alive and unmoved
// until completion. parent_remaining excludes caller-owned input, verified
// native SHA/frame/opaque reservations and other live allocations, but INCLUDES
// this builder's entire staging allowance. Staging costs are checked again on
// each call, never given a fresh child budget. Returned byte reservations live
// until their owners are freed. Synchronous destruction requires
// caller-bounded native/opaque cleanup; close() is available for
// joined cleanup.
class batch_builder final {
public:
    [[nodiscard]] static codec::result<batch_builder> make(
      batch_id id,
      producer_stream_binding binding,
      codec::limits policy,
      bytes::allocation_charge_fn charge);

    batch_builder(const batch_builder&) = delete;
    batch_builder& operator=(const batch_builder&) = delete;
    batch_builder(batch_builder&&) noexcept;
    batch_builder& operator=(batch_builder&&) = delete;
    ~batch_builder() = default;

    [[nodiscard]] seastar::future<codec::result<void>> add(
      const record& value,
      runtime::wall_time timestamp,
      codec::cooperative_work& work,
      byte_count parent_remaining) &;
    seastar::future<codec::result<void>> add(
      const record&&,
      runtime::wall_time,
      codec::cooperative_work&,
      byte_count) & = delete;
    [[nodiscard]] seastar::future<codec::result<submitted_batch>>
    finalize(codec::cooperative_work& work, byte_count parent_remaining) &;
    [[nodiscard]] seastar::future<> close(codec::cooperative_work& work) &;
    [[nodiscard]] item_count record_count() const noexcept { return count_; }
    [[nodiscard]] bool closed() const noexcept { return closed_; }

private:
    batch_builder(
      batch_id id,
      producer_stream_binding binding,
      codec::limits policy,
      bytes::allocation_charge_fn charge,
      bytes::fragmented_buffer_builder_config config,
      byte_count descriptor_charge) noexcept;
    codec::result<void>
    admit_size(byte_count total, byte_count parent_remaining) const;
    seastar::future<codec::result<void>> append(
      const record& value,
      runtime::wall_time timestamp,
      codec::cooperative_work& work,
      byte_count parent_remaining);

    batch_id id_;
    producer_stream_binding binding_;
    codec::limits policy_;
    bytes::allocation_charge_fn charge_;
    bytes::fragmented_buffer_builder_config config_;
    byte_count descriptor_charge_;
    std::optional<bytes::fragmented_buffer_builder> output_;
    std::optional<runtime::wall_time> timestamp_base_;
    item_count count_;
    item_count headers_;
    bool closed_{false};
};

} // namespace kwaque::model
