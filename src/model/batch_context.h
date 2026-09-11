#pragma once

#include "src/base/error.h"
#include "src/base/result.h"
#include "src/base/units.h"
#include "src/codec/limits.h"
#include "src/model/batch_identity.h"
#include "src/model/position.h"
#include "src/runtime/time.h"

namespace kwaque::model {

// Original submission metadata, without assigned positions or payload storage.
// The record owner separately verifies actual contents, timestamps and counts.
class submitted_batch_context final {
public:
    [[nodiscard]] static result<submitted_batch_context> make(
      batch_id id,
      producer_stream_binding binding,
      range_logical_count original_count,
      runtime::wall_time original_timestamp_base) noexcept {
        if (original_count.value() == 0) {
            return failure(errc::invalid_argument);
        }
        if (original_count.value() > codec::absolute_max_original_records) {
            return failure(errc::resource_exhausted);
        }
        return submitted_batch_context{
          id, binding, original_count, original_timestamp_base};
    }

    [[nodiscard]] batch_id id() const noexcept { return id_; }
    [[nodiscard]] producer_stream_binding binding() const noexcept {
        return binding_;
    }
    [[nodiscard]] range_logical_count original_count() const noexcept {
        return original_count_;
    }
    [[nodiscard]] runtime::wall_time original_timestamp_base() const noexcept {
        return original_timestamp_base_;
    }

    bool operator==(const submitted_batch_context&) const noexcept = default;

private:
    submitted_batch_context(
      batch_id id,
      producer_stream_binding binding,
      range_logical_count original_count,
      runtime::wall_time original_timestamp_base) noexcept
      : id_(id)
      , binding_(binding)
      , original_count_(original_count)
      , original_timestamp_base_(original_timestamp_base) {}

    batch_id id_;
    producer_stream_binding binding_;
    range_logical_count original_count_;
    runtime::wall_time original_timestamp_base_;
};

// Dense and sparse data share the original logical span and submission
// metadata. Retained count alone does not verify which original records
// survived.
class assigned_batch_context final {
public:
    // Expected binding comes from independently pinned original append/retry
    // context, not from a replacement physical layout or the submitted bytes.
    [[nodiscard]] static result<assigned_batch_context> assign(
      submitted_batch_context submitted,
      range_logical_end base,
      const producer_stream_binding& expected_binding) noexcept {
        const auto binding = submitted.binding().validate_expected(
          expected_binding);
        if (!binding) {
            return failure(binding.error());
        }
        const auto span = range_logical_span::from_count(
          base, submitted.original_count());
        if (!span) {
            return failure(span.error());
        }
        return assigned_batch_context{
          submitted, *span, item_count{submitted.original_count().value()}};
    }

    [[nodiscard]] submitted_batch_context submitted() const noexcept {
        return submitted_;
    }
    [[nodiscard]] range_logical_span logical_span() const noexcept {
        return logical_span_;
    }
    [[nodiscard]] item_count retained_count() const noexcept {
        return retained_count_;
    }

    [[nodiscard]] result<assigned_batch_context>
    with_retained_count(item_count count) const noexcept {
        if (count.value() == 0 || count > retained_count_) {
            return failure(errc::invalid_argument);
        }
        return assigned_batch_context{submitted_, logical_span_, count};
    }

    bool operator==(const assigned_batch_context&) const noexcept = default;

private:
    assigned_batch_context(
      submitted_batch_context submitted,
      range_logical_span logical_span,
      item_count retained_count) noexcept
      : submitted_(submitted)
      , logical_span_(logical_span)
      , retained_count_(retained_count) {}

    submitted_batch_context submitted_;
    range_logical_span logical_span_;
    item_count retained_count_;
};

} // namespace kwaque::model
