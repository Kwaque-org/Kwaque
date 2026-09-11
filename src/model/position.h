#pragma once

#include "src/base/error.h"
#include "src/base/result.h"
#include "src/base/units.h"
#include "src/model/epoch.h"
#include "src/model/identity.h"
#include "src/runtime/file_position.h"
#include "src/runtime/time.h"

#include <bit>
#include <compare>
#include <cstdint>
#include <limits>
#include <optional>
#include <utility>

namespace kwaque::model {

namespace detail {

// Both endpoints of a record span are boundaries; even UINT64_MAX can be the
// beginning of an empty span. The tag fixes the logical or physical unit.
template<typename Tag>
class record_boundary final {
public:
    using rep = std::uint64_t;
    using count_type = kwaque::detail::strong_count<Tag>;

    constexpr record_boundary() noexcept = default;
    constexpr explicit record_boundary(rep value) noexcept
      : value_(value) {}

    [[nodiscard]] constexpr rep value() const noexcept {
        return value_.value();
    }

    [[nodiscard]] constexpr std::optional<record_boundary>
    checked_add(count_type count) const noexcept {
        const auto next = value_.checked_add(count);
        if (!next) {
            return std::nullopt;
        }
        return record_boundary{next->value()};
    }

    [[nodiscard]] constexpr std::optional<record_boundary>
    checked_sub(count_type count) const noexcept {
        const auto previous = value_.checked_sub(count);
        if (!previous) {
            return std::nullopt;
        }
        return record_boundary{previous->value()};
    }

    auto operator<=>(const record_boundary&) const = default;

private:
    count_type value_;
};

// An actual record coordinate excludes UINT64_MAX so its following boundary
// is always representable. Zero is a valid coordinate, not an absence sentinel.
template<typename Tag>
class record_offset final {
public:
    using rep = std::uint64_t;
    using count_type = kwaque::detail::strong_count<Tag>;

    constexpr record_offset() noexcept = default;

    [[nodiscard]] static result<record_offset> make(rep value) noexcept {
        if (value == std::numeric_limits<rep>::max()) {
            return failure(errc::out_of_range);
        }
        return record_offset{value};
    }

    [[nodiscard]] constexpr rep value() const noexcept {
        return value_.value();
    }
    // Convert the coordinate without advancing; end_after() advances one slot.
    [[nodiscard]] constexpr record_boundary<Tag> as_end() const noexcept {
        return record_boundary<Tag>{value()};
    }
    [[nodiscard]] constexpr record_boundary<Tag> end_after() const noexcept {
        return record_boundary<Tag>{value() + 1U};
    }

    [[nodiscard]] constexpr std::optional<record_offset>
    checked_add(count_type count) const noexcept {
        const auto next = value_.checked_add(count);
        if (!next || next->value() == std::numeric_limits<rep>::max()) {
            return std::nullopt;
        }
        return record_offset{next->value()};
    }

    [[nodiscard]] constexpr std::optional<record_offset>
    checked_sub(count_type count) const noexcept {
        const auto previous = value_.checked_sub(count);
        if (!previous) {
            return std::nullopt;
        }
        return record_offset{previous->value()};
    }

    auto operator<=>(const record_offset&) const = default;

private:
    constexpr explicit record_offset(rep value) noexcept
      : value_(value) {}

    count_type value_;
};

template<typename Tag>
class record_span final {
public:
    using boundary_type = record_boundary<Tag>;
    using count_type = kwaque::detail::strong_count<Tag>;

    [[nodiscard]] static result<record_span>
    make(boundary_type begin, boundary_type end) noexcept {
        if (end < begin) {
            return failure(errc::invalid_argument);
        }
        return record_span{begin, end};
    }

    [[nodiscard]] static result<record_span>
    from_count(boundary_type begin, count_type count) noexcept {
        const auto end = begin.checked_add(count);
        if (!end) {
            return failure(errc::out_of_range);
        }
        return record_span{begin, *end};
    }

    [[nodiscard]] constexpr boundary_type begin() const noexcept {
        return begin_;
    }
    [[nodiscard]] constexpr boundary_type end() const noexcept { return end_; }
    [[nodiscard]] constexpr count_type count() const noexcept {
        return count_type{end_.value() - begin_.value()};
    }
    [[nodiscard]] constexpr bool empty() const noexcept {
        return begin_ == end_;
    }
    [[nodiscard]] constexpr bool
    contains(record_offset<Tag> offset) const noexcept {
        return offset.value() >= begin_.value()
               && offset.value() < end_.value();
    }

    bool operator==(const record_span&) const noexcept = default;

private:
    constexpr explicit record_span(
      boundary_type begin, boundary_type end) noexcept
      : begin_(begin)
      , end_(end) {}

    boundary_type begin_;
    boundary_type end_;
};

struct range_logical_tag;
struct segment_record_tag;

} // namespace detail

// Logical slots within one topic/range, including holes left by a rewrite.
// Counts also express deltas in that unit. Callers retain the range context.
using range_logical_count
  = kwaque::detail::strong_count<detail::range_logical_tag>;
using range_logical_end = detail::record_boundary<detail::range_logical_tag>;
using range_logical_offset = detail::record_offset<detail::range_logical_tag>;
using range_logical_span = detail::record_span<detail::range_logical_tag>;

// Retained record ordinals within one segment/generation layout. These counts
// and coordinates cannot be used as logical offsets or file-byte positions.
using segment_record_count
  = kwaque::detail::strong_count<detail::segment_record_tag>;
using segment_relative_end
  = detail::record_boundary<detail::segment_record_tag>;
using segment_relative_offset
  = detail::record_offset<detail::segment_record_tag>;
using segment_relative_span = detail::record_span<detail::segment_record_tag>;

// Half-open byte coverage in a caller-supplied file object/incarnation.
class file_byte_span final {
public:
    [[nodiscard]] static result<file_byte_span>
    make(runtime::file_position begin, runtime::file_position end) noexcept {
        if (end < begin) {
            return failure(errc::invalid_argument);
        }
        return file_byte_span{begin, end};
    }

    [[nodiscard]] static result<file_byte_span>
    from_size(runtime::file_position begin, byte_count size) noexcept {
        const auto end = begin.checked_add(size);
        if (!end) {
            return failure(errc::out_of_range);
        }
        return file_byte_span{begin, *end};
    }

    [[nodiscard]] constexpr runtime::file_position begin() const noexcept {
        return begin_;
    }
    [[nodiscard]] constexpr runtime::file_position end() const noexcept {
        return end_;
    }
    [[nodiscard]] constexpr byte_count size() const noexcept {
        return byte_count{end_.value() - begin_.value()};
    }
    [[nodiscard]] constexpr bool empty() const noexcept {
        return begin_ == end_;
    }
    [[nodiscard]] constexpr bool
    contains(runtime::file_position position) const noexcept {
        return position >= begin_ && position < end_;
    }

    bool operator==(const file_byte_span&) const noexcept = default;

private:
    constexpr explicit file_byte_span(
      runtime::file_position begin, runtime::file_position end) noexcept
      : begin_(begin)
      , end_(end) {}

    runtime::file_position begin_;
    runtime::file_position end_;
};

// Stable logical identity. Physical movement never changes these components.
class record_id final {
public:
    [[nodiscard]] static result<record_id>
    make(topic_id topic, range_id range, range_logical_offset offset) noexcept {
        if (topic.is_nil() || range.is_nil()) {
            return failure(errc::invalid_argument);
        }
        return record_id{topic, range, offset};
    }

    [[nodiscard]] topic_id topic() const noexcept { return topic_; }
    [[nodiscard]] range_id range() const noexcept { return range_; }
    [[nodiscard]] range_logical_offset offset() const noexcept {
        return offset_;
    }

    bool operator==(const record_id&) const noexcept = default;

    // Collection order only; cross-range causality needs explicit lineage.
    [[nodiscard]] bool canonical_less(const record_id& other) const noexcept {
        if (topic_ != other.topic_) {
            return topic_.canonical_less(other.topic_);
        }
        if (range_ != other.range_) {
            return range_.canonical_less(other.range_);
        }
        return offset_ < other.offset_;
    }

    [[nodiscard]] result<std::strong_ordering>
    compare_in_range(const record_id& other) const noexcept {
        if (topic_ != other.topic_ || range_ != other.range_) {
            return failure(errc::invalid_argument);
        }
        return offset_ <=> other.offset_;
    }

    template<typename H>
    friend H AbslHashValue(H state, const record_id& value) {
        return H::combine(
          std::move(state), value.topic_, value.range_, value.offset_.value());
    }

private:
    explicit record_id(
      topic_id topic, range_id range, range_logical_offset offset) noexcept
      : topic_(topic)
      , range_(range)
      , offset_(offset) {}

    topic_id topic_;
    range_id range_;
    range_logical_offset offset_;
};

// A physical record coordinate. Equality alone does not establish matching
// generation, layout or content; those are supplied by the resolving owner.
class physical_address final {
public:
    [[nodiscard]] static result<physical_address>
    make(segment_id segment, segment_relative_offset offset) noexcept {
        if (segment.is_nil()) {
            return failure(errc::invalid_argument);
        }
        return physical_address{segment, offset};
    }

    [[nodiscard]] segment_id segment() const noexcept { return segment_; }
    [[nodiscard]] segment_relative_offset offset() const noexcept {
        return offset_;
    }

    bool operator==(const physical_address&) const noexcept = default;

    [[nodiscard]] bool
    canonical_less(const physical_address& other) const noexcept {
        if (segment_ != other.segment_) {
            return segment_.canonical_less(other.segment_);
        }
        return offset_ < other.offset_;
    }

    // Each generation must come from its address's independently pinned layout.
    [[nodiscard]] result<std::strong_ordering> compare_in_layout(
      const physical_address& other,
      segment_generation generation,
      segment_generation other_generation) const noexcept {
        if (
          !generation.is_valid() || !other_generation.is_valid()
          || segment_ != other.segment_ || generation != other_generation) {
            return failure(errc::invalid_argument);
        }
        return offset_ <=> other.offset_;
    }

    template<typename H>
    friend H AbslHashValue(H state, const physical_address& value) {
        return H::combine(
          std::move(state), value.segment_, value.offset_.value());
    }

private:
    explicit physical_address(
      segment_id segment, segment_relative_offset offset) noexcept
      : segment_(segment)
      , offset_(offset) {}

    segment_id segment_;
    segment_relative_offset offset_;
};

// value - base in signed nanoseconds, if that difference is representable.
// Ordered unsigned coordinates avoid signed overflow before the bounds check.
[[nodiscard]] constexpr std::optional<std::int64_t> checked_timestamp_delta(
  runtime::wall_time base, runtime::wall_time value) noexcept {
    constexpr auto sign_bit = std::uint64_t{1} << 63U;
    const auto base_ordered
      = std::bit_cast<std::uint64_t>(base.unix_nanoseconds()) ^ sign_bit;
    const auto value_ordered
      = std::bit_cast<std::uint64_t>(value.unix_nanoseconds()) ^ sign_bit;
    if (value_ordered >= base_ordered) {
        const auto magnitude = value_ordered - base_ordered;
        if (
          magnitude > static_cast<std::uint64_t>(
            std::numeric_limits<std::int64_t>::max())) {
            return std::nullopt;
        }
        return static_cast<std::int64_t>(magnitude);
    }
    const auto magnitude = base_ordered - value_ordered;
    if (magnitude > sign_bit) {
        return std::nullopt;
    }
    if (magnitude == sign_bit) {
        return std::numeric_limits<std::int64_t>::min();
    }
    return -static_cast<std::int64_t>(magnitude);
}

[[nodiscard]] constexpr std::optional<runtime::wall_time>
checked_timestamp_from_delta(
  runtime::wall_time base, std::int64_t delta) noexcept {
    if (delta >= 0) {
        return base.checked_add(
          runtime::monotonic_duration{static_cast<std::uint64_t>(delta)});
    }
    const auto magnitude = std::uint64_t{0} - static_cast<std::uint64_t>(delta);
    return base.checked_sub(runtime::monotonic_duration{magnitude});
}

} // namespace kwaque::model
