#pragma once

#include "src/base/error.h"
#include "src/base/result.h"
#include "src/base/units.h"

#include <compare>
#include <cstdint>
#include <utility>

namespace kwaque::model {

namespace detail {

// Zero is uninitialized staging. Successor calculation changes only a value;
// publication and matching owner scope remain caller responsibilities.
template<typename Tag>
class positive_counter final {
public:
    using rep = std::uint64_t;

    constexpr positive_counter() noexcept = default;

    [[nodiscard]] static result<positive_counter> make(rep value) noexcept {
        if (value == 0) {
            return failure(errc::invalid_argument);
        }
        return positive_counter{value};
    }

    [[nodiscard]] constexpr rep value() const noexcept {
        return value_.value();
    }
    [[nodiscard]] constexpr bool is_valid() const noexcept {
        return value() != 0;
    }

    [[nodiscard]] result<positive_counter> checked_successor() const noexcept {
        if (!is_valid()) {
            return failure(errc::invalid_argument);
        }
        const auto next = value_.checked_add(count_type{1});
        if (!next) {
            return failure(errc::out_of_range);
        }
        return positive_counter{next->value()};
    }

    auto operator<=>(const positive_counter&) const = default;

    template<typename H>
    friend H AbslHashValue(H state, const positive_counter& value) {
        return H::combine(std::move(state), value.value());
    }

private:
    using count_type = kwaque::detail::strong_count<Tag>;

    constexpr explicit positive_counter(rep value) noexcept
      : value_(value) {}

    count_type value_;
};

struct segment_generation_tag;
struct range_routing_epoch_tag;
struct lease_epoch_tag;
struct producer_epoch_tag;
struct range_manifest_generation_tag;

} // namespace detail

// Each comparison requires the matching owner context; equal scalar values
// belonging to different objects do not share authority.
// One segment's immutable write/layout incarnation.
using segment_generation
  = detail::positive_counter<detail::segment_generation_tag>;
// Routing authority within one topic/range identity.
using range_routing_epoch
  = detail::positive_counter<detail::range_routing_epoch_tag>;
// Write-lease authority within one segment/generation.
using lease_epoch = detail::positive_counter<detail::lease_epoch_tag>;
// Fenced incarnation of one durable producer.
using producer_epoch = detail::positive_counter<detail::producer_epoch_tag>;
// Publication sequence of immutable manifests for one topic/range.
using range_manifest_generation
  = detail::positive_counter<detail::range_manifest_generation_tag>;

} // namespace kwaque::model
