#pragma once

#include "src/base/error.h"
#include "src/base/result.h"
#include "src/base/units.h"
#include "src/model/epoch.h"
#include "src/model/identity.h"

#include <compare>
#include <cstdint>
#include <utility>

namespace kwaque::model {

namespace detail {

struct producer_stream_id_tag;
struct batch_sequence_tag;

} // namespace detail

// Nonzero and never reused within one producer/epoch. A numeric successor is
// only a candidate value; it does not allocate or bind a producer stream.
using producer_stream_id
  = detail::positive_counter<detail::producer_stream_id_tag>;

// Batch sequence within a producer/epoch/stream. Zero is the first value;
// maximum remains usable, but has no successor.
class batch_sequence final {
public:
    using rep = std::uint64_t;

    constexpr batch_sequence() noexcept = default;
    constexpr explicit batch_sequence(rep value) noexcept
      : value_(value) {}

    [[nodiscard]] constexpr rep value() const noexcept {
        return value_.value();
    }

    [[nodiscard]] result<batch_sequence> checked_successor() const noexcept {
        const auto next = value_.checked_add(count_type{1});
        if (!next) {
            return failure(errc::out_of_range);
        }
        return batch_sequence{next->value()};
    }

    auto operator<=>(const batch_sequence&) const = default;

    template<typename H>
    friend H AbslHashValue(H state, const batch_sequence& value) {
        return H::combine(std::move(state), value.value());
    }

private:
    using count_type = kwaque::detail::strong_count<detail::batch_sequence_tag>;

    count_type value_;
};

class batch_id final {
public:
    [[nodiscard]] static result<batch_id> make(
      producer_id producer,
      producer_epoch epoch,
      producer_stream_id stream,
      batch_sequence sequence) noexcept {
        if (producer.is_nil() || !epoch.is_valid() || !stream.is_valid()) {
            return failure(errc::invalid_argument);
        }
        return batch_id{producer, epoch, stream, sequence};
    }

    // Values are returned by copy so access through a temporary batch is safe.
    [[nodiscard]] producer_id producer() const noexcept { return producer_; }
    [[nodiscard]] producer_epoch epoch() const noexcept { return epoch_; }
    [[nodiscard]] producer_stream_id stream() const noexcept { return stream_; }
    [[nodiscard]] batch_sequence sequence() const noexcept { return sequence_; }

    bool operator==(const batch_id&) const noexcept = default;

    // Canonical collection order: opaque producer octets, then numeric fields.
    // It does not imply append order across producer streams or epochs.
    [[nodiscard]] bool canonical_less(const batch_id& other) const noexcept {
        if (producer_ != other.producer_) {
            return producer_.canonical_less(other.producer_);
        }
        if (epoch_ != other.epoch_) {
            return epoch_ < other.epoch_;
        }
        if (stream_ != other.stream_) {
            return stream_ < other.stream_;
        }
        return sequence_ < other.sequence_;
    }

    template<typename H>
    friend H AbslHashValue(H state, const batch_id& value) {
        return H::combine(
          std::move(state),
          value.producer_,
          value.epoch_,
          value.stream_,
          value.sequence_);
    }

private:
    explicit batch_id(
      producer_id producer,
      producer_epoch epoch,
      producer_stream_id stream,
      batch_sequence sequence) noexcept
      : producer_(producer)
      , epoch_(epoch)
      , stream_(stream)
      , sequence_(sequence) {}

    producer_id producer_;
    producer_epoch epoch_;
    producer_stream_id stream_;
    batch_sequence sequence_;
};

// Immutable context of an original producer stream. The associated BatchID
// supplies the producer/epoch/stream identity; this value supplies its binding.
class producer_stream_binding final {
public:
    [[nodiscard]] static result<producer_stream_binding> make(
      topic_id topic,
      range_id range,
      range_routing_epoch routing_epoch,
      segment_id segment,
      segment_generation generation) noexcept {
        if (
          topic.is_nil() || range.is_nil() || !routing_epoch.is_valid()
          || segment.is_nil() || !generation.is_valid()) {
            return failure(errc::invalid_argument);
        }
        return producer_stream_binding{
          topic, range, routing_epoch, segment, generation};
    }

    [[nodiscard]] topic_id topic() const noexcept { return topic_; }
    [[nodiscard]] range_id range() const noexcept { return range_; }
    [[nodiscard]] range_routing_epoch routing_epoch() const noexcept {
        return routing_epoch_;
    }
    [[nodiscard]] segment_id segment() const noexcept { return segment_; }
    [[nodiscard]] segment_generation generation() const noexcept {
        return generation_;
    }

    bool operator==(const producer_stream_binding&) const noexcept = default;

    // Expectations must come from the caller's intended stream context.
    // Matching two values does not prove metadata membership or write
    // authority.
    [[nodiscard]] result<void>
    validate_expected(const producer_stream_binding& expected) const noexcept {
        if (
          topic_ != expected.topic_ || range_ != expected.range_
          || routing_epoch_ != expected.routing_epoch_
          || segment_ != expected.segment_
          || generation_ != expected.generation_) {
            return failure(errc::invalid_argument);
        }
        return {};
    }

private:
    explicit producer_stream_binding(
      topic_id topic,
      range_id range,
      range_routing_epoch routing_epoch,
      segment_id segment,
      segment_generation generation) noexcept
      : topic_(topic)
      , range_(range)
      , routing_epoch_(routing_epoch)
      , segment_(segment)
      , generation_(generation) {}

    topic_id topic_;
    range_id range_;
    range_routing_epoch routing_epoch_;
    segment_id segment_;
    segment_generation generation_;
};

} // namespace kwaque::model
