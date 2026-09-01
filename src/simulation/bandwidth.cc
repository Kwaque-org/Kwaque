#include "src/simulation/bandwidth.h"

#include "src/base/invariant.h"
#include "src/simulation/sha256.h"

#include <seastar/core/chunked_vector.hh>

#include <boost/multiprecision/cpp_int/import_export.hpp>

#include <algorithm>
#include <array>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <utility>

namespace kwaque::simulation {

namespace {

constexpr invariant_id bandwidth_invariant{"KQ-BANDWIDTH-STATE"};

[[nodiscard]] runtime::operation_error bandwidth_error(errc code) noexcept {
    return runtime::operation_error{code, runtime::operation_kind::network};
}

[[nodiscard]] bandwidth_integer
gcd(bandwidth_integer left, bandwidth_integer right) {
    while (right != 0) {
        bandwidth_integer remainder = left % right;
        left = std::move(right);
        right = std::move(remainder);
    }
    return left;
}

[[nodiscard]] std::optional<std::uint64_t>
to_u64(const bandwidth_integer& value) {
    if (value > std::numeric_limits<std::uint64_t>::max()) {
        return std::nullopt;
    }
    return value.convert_to<std::uint64_t>();
}

[[nodiscard]] std::optional<std::uint64_t> ceil_ratio_u64(
  const bandwidth_integer& numerator, const bandwidth_integer& denominator) {
    const bandwidth_integer quotient = numerator / denominator;
    const bandwidth_integer remainder = numerator % denominator;
    const bandwidth_integer rounded = quotient + (remainder == 0 ? 0 : 1);
    return to_u64(rounded);
}

} // namespace

bandwidth_fraction::bandwidth_fraction(
  bandwidth_integer numerator, bandwidth_integer denominator)
  : numerator_(std::move(numerator))
  , denominator_(std::move(denominator)) {
    KWAQUE_INVARIANT(
      bandwidth_invariant,
      denominator_ != 0,
      "bandwidth fraction has zero denominator");
    if (numerator_ == 0) {
        denominator_ = 1;
        return;
    }
    const auto divisor = gcd(numerator_, denominator_);
    numerator_ /= divisor;
    denominator_ /= divisor;
}

bandwidth_fraction bandwidth_fraction::whole(std::uint64_t value) {
    return bandwidth_fraction{bandwidth_integer{value}, bandwidth_integer{1}};
}

runtime::result<bandwidth_fraction>
bandwidth_fraction::ratio(std::uint64_t numerator, std::uint64_t denominator) {
    if (denominator == 0) {
        return runtime::failure(bandwidth_error(errc::invalid_argument));
    }
    return bandwidth_fraction{
      bandwidth_integer{numerator}, bandwidth_integer{denominator}};
}

bool bandwidth_fraction::equals(
  std::uint64_t numerator, std::uint64_t denominator) const {
    const auto expected = ratio(numerator, denominator);
    return expected && *this == *expected;
}

std::optional<std::uint64_t> bandwidth_fraction::numerator_u64() const {
    return to_u64(numerator_);
}

std::optional<std::uint64_t> bandwidth_fraction::denominator_u64() const {
    return to_u64(denominator_);
}

bandwidth_fraction
bandwidth_fraction::add(const bandwidth_fraction& other) const {
    const auto common = gcd(denominator_, other.denominator_);
    const auto left_scale = other.denominator_ / common;
    const auto right_scale = denominator_ / common;
    return bandwidth_fraction{
      numerator_ * left_scale + other.numerator_ * right_scale,
      denominator_ * left_scale};
}

bandwidth_fraction
bandwidth_fraction::subtract(const bandwidth_fraction& other) const {
    KWAQUE_INVARIANT(
      bandwidth_invariant,
      compare(other) != std::strong_ordering::less,
      "bandwidth fraction subtraction underflow");
    const auto common = gcd(denominator_, other.denominator_);
    const auto left_scale = other.denominator_ / common;
    const auto right_scale = denominator_ / common;
    return bandwidth_fraction{
      numerator_ * left_scale - other.numerator_ * right_scale,
      denominator_ * left_scale};
}

bandwidth_fraction
bandwidth_fraction::multiply(const bandwidth_fraction& other) const {
    const auto left_cancel = gcd(numerator_, other.denominator_);
    const auto right_cancel = gcd(other.numerator_, denominator_);
    return bandwidth_fraction{
      (numerator_ / left_cancel) * (other.numerator_ / right_cancel),
      (denominator_ / right_cancel) * (other.denominator_ / left_cancel)};
}

bandwidth_fraction bandwidth_fraction::multiply(std::uint64_t value) const {
    if (value == 0 || numerator_ == 0) {
        return {};
    }
    bandwidth_integer multiplier{value};
    const auto cancel = gcd(multiplier, denominator_);
    return bandwidth_fraction{
      numerator_ * (multiplier / cancel), denominator_ / cancel};
}

bandwidth_fraction bandwidth_fraction::divide(std::uint64_t value) const {
    KWAQUE_INVARIANT(
      bandwidth_invariant, value != 0, "bandwidth fraction divided by zero");
    if (numerator_ == 0) {
        return {};
    }
    bandwidth_integer divisor{value};
    const auto cancel = gcd(numerator_, divisor);
    return bandwidth_fraction{
      numerator_ / cancel, denominator_ * (divisor / cancel)};
}

std::strong_ordering
bandwidth_fraction::compare(const bandwidth_fraction& other) const {
    const auto left = numerator_ * other.denominator_;
    const auto right = other.numerator_ * denominator_;
    if (left < right) {
        return std::strong_ordering::less;
    }
    if (left > right) {
        return std::strong_ordering::greater;
    }
    return std::strong_ordering::equal;
}

bandwidth_rate bandwidth_rate::unlimited() noexcept { return {}; }

bandwidth_rate bandwidth_rate::finite(bandwidth_fraction value) {
    bandwidth_rate result;
    result.kind_ = kind::finite;
    result.finite_ = std::move(value);
    return result;
}

bandwidth_capacity bandwidth_capacity::unlimited() noexcept { return {}; }

bandwidth_capacity
bandwidth_capacity::finite(std::uint64_t bytes_per_second) noexcept {
    bandwidth_capacity result;
    result.unlimited_ = false;
    result.bytes_per_second_ = bytes_per_second;
    return result;
}

bandwidth_resource_key bandwidth_resource_key::numeric(
  std::uint8_t domain, std::uint64_t value) noexcept {
    std::array<std::byte, encoded_bytes> encoded{};
    encoded[0] = static_cast<std::byte>(domain);
    for (std::size_t index = 0; index < sizeof(value); ++index) {
        encoded[1U + index] = static_cast<std::byte>(
          (value >> ((sizeof(value) - index - 1U) * 8U)) & 0xffU);
    }
    return bandwidth_resource_key{encoded};
}

class bandwidth_planner::impl final {
public:
    struct flow_slot final {
        bandwidth_flow input;
        std::array<std::uint16_t, maximum_bandwidth_constraints_per_flow>
          resources{};
        bandwidth_allocation allocation;
        std::uint8_t resource_count{0};
        bool limited{false};
        bool active{false};
    };

    struct resource_slot final {
        bandwidth_resource_key key;
        bandwidth_capacity capacity;
        bandwidth_fraction remaining;
        std::array<std::uint16_t, maximum_bandwidth_flows> members{};
        std::uint16_t member_count{0};
        std::uint16_t active{0};
    };

    explicit impl(std::uint32_t maximum)
      : maximum_flows(maximum) {
        flows.reserve(maximum_flows);
        for (std::uint32_t index = 0; index < maximum_flows; ++index) {
            flows.emplace_back();
        }
        const auto resource_capacity = maximum_flows
                                       * maximum_bandwidth_constraints_per_flow;
        resources.reserve(resource_capacity);
        for (std::uint32_t index = 0; index < resource_capacity; ++index) {
            resources.emplace_back();
        }
    }

    [[nodiscard]] runtime::result<std::optional<std::uint16_t>>
    constrain(bandwidth_constraint constraint) {
        if (constraint.capacity.is_unlimited()) {
            return std::optional<std::uint16_t>{};
        }
        for (std::uint16_t index = 0; index < resource_count; ++index) {
            auto& resource = resources[index];
            if (resource.key == constraint.resource) {
                if (resource.capacity != constraint.capacity) {
                    return runtime::failure(
                      bandwidth_error(errc::invalid_argument));
                }
                return std::optional<std::uint16_t>{index};
            }
        }
        KWAQUE_INVARIANT(
          bandwidth_invariant,
          resource_count
            < maximum_flows * maximum_bandwidth_constraints_per_flow,
          "bandwidth resource workspace exhausted after validated admission");
        const auto index = resource_count++;
        auto& resource = resources[index];
        resource.key = constraint.resource;
        resource.capacity = constraint.capacity;
        resource.remaining = bandwidth_fraction::whole(
          constraint.capacity.bytes_per_second());
        resource.member_count = 0;
        resource.active = 0;
        return std::optional<std::uint16_t>{index};
    }

    void attach(
      std::uint16_t resource_index,
      std::uint16_t flow_index,
      flow_slot& flow) noexcept {
        auto& resource = resources[resource_index];
        KWAQUE_INVARIANT(
          bandwidth_invariant,
          resource.member_count < maximum_bandwidth_flows,
          "bandwidth resource membership workspace exhausted");
        resource.members[resource.member_count++] = flow_index;
        ++resource.active;
        flow.resources[flow.resource_count++] = resource_index;
        flow.limited = true;
        ++membership_count;
    }

    void freeze(std::uint16_t resource_index) {
        const auto member_count = resources[resource_index].member_count;
        for (std::uint16_t member = 0; member < member_count; ++member) {
            const auto flow_index = resources[resource_index].members[member];
            auto& flow = flows[flow_index];
            if (!flow.active) {
                continue;
            }
            flow.allocation.rate = bandwidth_rate::finite(fill);
            flow.active = false;
            --active_flows;
            for (std::uint8_t index = 0; index < flow.resource_count; ++index) {
                auto& resource = resources[flow.resources[index]];
                if (resource.active != 0) {
                    --resource.active;
                }
            }
        }
    }

    std::uint32_t maximum_flows;
    seastar::chunked_vector<flow_slot> flows;
    seastar::chunked_vector<resource_slot> resources;
    std::uint16_t flow_count{0};
    std::uint16_t resource_count{0};
    std::uint16_t active_flows{0};
    std::uint16_t membership_count{0};
    std::array<std::uint16_t, maximum_bandwidth_flows> order{};
    bandwidth_fraction fill;
};

runtime::result<std::unique_ptr<bandwidth_planner>>
bandwidth_planner::make(std::uint32_t maximum_flows_value) {
    if (maximum_flows_value == 0) {
        return runtime::failure(bandwidth_error(errc::invalid_argument));
    }
    if (maximum_flows_value > maximum_bandwidth_flows) {
        return runtime::failure(bandwidth_error(errc::out_of_range));
    }
    auto implementation = std::make_unique<impl>(maximum_flows_value);
    return std::unique_ptr<bandwidth_planner>{
      new bandwidth_planner(std::move(implementation))};
}

bandwidth_planner::bandwidth_planner(
  std::unique_ptr<impl> implementation) noexcept
  : impl_(std::move(implementation)) {}

bandwidth_planner::~bandwidth_planner() = default;

void bandwidth_planner::reset() noexcept {
    impl_->flow_count = 0;
    impl_->resource_count = 0;
    impl_->active_flows = 0;
    impl_->membership_count = 0;
    impl_->fill = bandwidth_fraction{};
}

runtime::result<void>
bandwidth_planner::add_flow(bandwidth_flow flow) noexcept {
    if (
      flow.id == 0
      || flow.constraint_count > maximum_bandwidth_constraints_per_flow) {
        return runtime::failure(bandwidth_error(errc::invalid_argument));
    }
    if (impl_->flow_count == impl_->maximum_flows) {
        return runtime::failure(bandwidth_error(errc::queue_full));
    }
    for (std::uint16_t index = 0; index < impl_->flow_count; ++index) {
        if (impl_->flows[index].input.id == flow.id) {
            return runtime::failure(bandwidth_error(errc::invalid_argument));
        }
    }
    for (std::uint8_t left = 0; left < flow.constraint_count; ++left) {
        for (std::uint8_t right = left + 1U; right < flow.constraint_count;
             ++right) {
            if (
              flow.constraints[left].resource
              == flow.constraints[right].resource) {
                return runtime::failure(
                  bandwidth_error(errc::invalid_argument));
            }
        }
    }
    auto& slot = impl_->flows[impl_->flow_count++];
    slot.input = std::move(flow);
    slot.resource_count = 0;
    slot.limited = false;
    slot.active = false;
    slot.allocation = bandwidth_allocation{
      .flow = slot.input.id, .rate = bandwidth_rate::unlimited()};
    return {};
}

runtime::result<void> bandwidth_planner::solve() noexcept {
    try {
        impl_->resource_count = 0;
        impl_->active_flows = 0;
        impl_->membership_count = 0;
        impl_->fill = bandwidth_fraction{};
        for (std::uint16_t index = 0; index < impl_->flow_count; ++index) {
            impl_->order[index] = index;
        }
        std::sort(
          impl_->order.begin(),
          impl_->order.begin() + impl_->flow_count,
          [&](std::uint16_t left, std::uint16_t right) {
              return impl_->flows[left].input.id < impl_->flows[right].input.id;
          });
        for (std::uint16_t ordered = 0; ordered < impl_->flow_count;
             ++ordered) {
            const auto flow_index = impl_->order[ordered];
            auto& flow = impl_->flows[flow_index];
            flow.resource_count = 0;
            flow.limited = false;
            flow.active = false;
            flow.allocation = bandwidth_allocation{
              .flow = flow.input.id, .rate = bandwidth_rate::unlimited()};
            for (std::uint8_t constraint_index = 0;
                 constraint_index < flow.input.constraint_count;
                 ++constraint_index) {
                auto constrained = impl_->constrain(
                  flow.input.constraints[constraint_index]);
                if (!constrained) {
                    return runtime::failure(constrained.error());
                }
                if (*constrained) {
                    impl_->attach(**constrained, flow_index, flow);
                }
            }
            if (flow.limited) {
                flow.active = true;
                ++impl_->active_flows;
            }
        }

        while (impl_->active_flows != 0) {
            std::optional<bandwidth_fraction> minimum;
            for (std::uint16_t resource_index = 0;
                 resource_index < impl_->resource_count;
                 ++resource_index) {
                const auto& resource = impl_->resources[resource_index];
                if (resource.active == 0) {
                    continue;
                }
                const auto share = resource.remaining.divide(resource.active);
                if (
                  !minimum
                  || share.compare(*minimum) == std::strong_ordering::less) {
                    minimum = share;
                }
            }
            KWAQUE_INVARIANT(
              bandwidth_invariant,
              minimum.has_value(),
              "active bandwidth flows have no constrained resource");

            if (!minimum->zero()) {
                impl_->fill = impl_->fill.add(*minimum);
                for (std::uint16_t resource_index = 0;
                     resource_index < impl_->resource_count;
                     ++resource_index) {
                    auto& resource = impl_->resources[resource_index];
                    if (resource.active == 0) {
                        continue;
                    }
                    resource.remaining = resource.remaining.subtract(
                      minimum->multiply(resource.active));
                }
            }

            bool froze = false;
            for (std::uint16_t resource_index = 0;
                 resource_index < impl_->resource_count;
                 ++resource_index) {
                if (
                  impl_->resources[resource_index].active != 0
                  && impl_->resources[resource_index].remaining.zero()) {
                    impl_->freeze(resource_index);
                    froze = true;
                }
            }
            KWAQUE_INVARIANT(
              bandwidth_invariant,
              froze,
              "bandwidth progressive filling made no progress");
        }
        return {};
    } catch (const std::overflow_error&) {
        return runtime::failure(bandwidth_error(errc::out_of_range));
    }
}

std::size_t bandwidth_planner::allocation_count() const noexcept {
    return impl_->flow_count;
}

const bandwidth_allocation&
bandwidth_planner::allocation_at(std::size_t index) const noexcept {
    KWAQUE_INVARIANT(
      bandwidth_invariant,
      index < impl_->flow_count,
      "bandwidth allocation index is outside the solved flow set");
    return impl_->flows[impl_->order[index]].allocation;
}

std::size_t bandwidth_planner::resource_count() const noexcept {
    return impl_->resource_count;
}

std::size_t bandwidth_planner::membership_count() const noexcept {
    return impl_->membership_count;
}

std::uint32_t bandwidth_planner::maximum_flows() const noexcept {
    return impl_->maximum_flows;
}

bandwidth_allocation_digest bandwidth_planner::allocation_digest() const {
    sha256_hasher hasher;
    auto update = [&](const void* data, std::size_t size) {
        hasher.update(data, size);
    };
    auto update_u16 = [&](std::uint16_t value) {
        const std::array bytes{
          static_cast<unsigned char>(value >> 8U),
          static_cast<unsigned char>(value & 0xffU),
        };
        update(bytes.data(), bytes.size());
    };
    auto update_u64 = [&](std::uint64_t value) {
        std::array<unsigned char, 8> bytes{};
        for (std::size_t index = 0; index < bytes.size(); ++index) {
            bytes[index] = static_cast<unsigned char>(
              (value >> ((bytes.size() - index - 1U) * 8U)) & 0xffU);
        }
        update(bytes.data(), bytes.size());
    };
    auto update_integer = [&](const bandwidth_integer& value) {
        std::array<unsigned char, 512> bytes{};
        const auto bits
          = static_cast<std::size_t>(boost::multiprecision::msb(value)) + 1U;
        const auto size = (bits + 7U) / 8U;
        boost::multiprecision::export_bits(value, bytes.begin(), 8U, true);
        update_u16(static_cast<std::uint16_t>(size));
        update(bytes.data(), size);
    };

    const unsigned char version{0x01};
    update(&version, sizeof(version));
    update_u16(static_cast<std::uint16_t>(allocation_count()));
    for (std::size_t index = 0; index < allocation_count(); ++index) {
        const auto& allocation = allocation_at(index);
        update_u64(allocation.flow);
        if (allocation.rate.is_unlimited()) {
            const unsigned char tag{2};
            update(&tag, sizeof(tag));
        } else if (allocation.rate.finite_value().zero()) {
            const unsigned char tag{0};
            update(&tag, sizeof(tag));
        } else {
            const unsigned char tag{1};
            update(&tag, sizeof(tag));
            update_integer(allocation.rate.finite_value().numerator_);
            update_integer(allocation.rate.finite_value().denominator_);
        }
    }
    const auto bytes = std::move(hasher).final();
    bandwidth_allocation_digest result;
    for (std::size_t word = 0; word < result.words.size(); ++word) {
        for (std::size_t byte = 0; byte < 8U; ++byte) {
            result.words[word] = (result.words[word] << 8U)
                                 | bytes[word * 8U + byte];
        }
    }
    return result;
}

runtime::result<std::optional<runtime::monotonic_duration>> bandwidth_duration(
  const bandwidth_rate& rate, const bandwidth_fraction& remaining) noexcept {
    try {
        if (remaining.zero() || rate.is_unlimited()) {
            return std::optional<runtime::monotonic_duration>{
              runtime::monotonic_duration{}};
        }
        const auto& finite = rate.finite_value();
        if (finite.zero()) {
            return std::optional<runtime::monotonic_duration>{};
        }
        const bandwidth_integer numerator = remaining.numerator_
                                            * finite.denominator_
                                            * 1'000'000'000U;
        const bandwidth_integer denominator = remaining.denominator_
                                              * finite.numerator_;
        const auto rounded = ceil_ratio_u64(numerator, denominator);
        if (!rounded) {
            return runtime::failure(bandwidth_error(errc::out_of_range));
        }
        return std::optional<runtime::monotonic_duration>{
          runtime::monotonic_duration{*rounded}};
    } catch (const std::overflow_error&) {
        return runtime::failure(bandwidth_error(errc::out_of_range));
    }
}

bandwidth_fraction bandwidth_transfer(
  const bandwidth_rate& rate,
  runtime::monotonic_duration elapsed,
  const bandwidth_fraction& remaining) noexcept {
    if (remaining.zero()) {
        return remaining;
    }
    if (rate.is_unlimited()) {
        return {};
    }
    if (rate.finite_value().zero() || elapsed.nanoseconds() == 0) {
        return remaining;
    }
    try {
        const bandwidth_fraction elapsed_seconds{
          bandwidth_integer{elapsed.nanoseconds()},
          bandwidth_integer{1'000'000'000U}};
        const auto usage = rate.finite_value().multiply(elapsed_seconds);
        if (usage.compare(remaining) != std::strong_ordering::less) {
            return {};
        }
        return remaining.subtract(usage);
    } catch (const std::overflow_error&) {
        KWAQUE_INVARIANT(
          bandwidth_invariant,
          false,
          "validated bandwidth progress exceeded fixed exact arithmetic");
    }
}

runtime::result<std::optional<runtime::monotonic_duration>>
bit_rate_transmission_duration(
  byte_count bytes, std::uint64_t bits_per_second) noexcept {
    if (bits_per_second == 0) {
        return std::optional<runtime::monotonic_duration>{};
    }
    try {
        const bandwidth_integer numerator = bandwidth_integer{bytes.value()}
                                            * 8U * 1'000'000'000U;
        const bandwidth_integer denominator{bits_per_second};
        const auto rounded = ceil_ratio_u64(numerator, denominator);
        if (!rounded) {
            return runtime::failure(bandwidth_error(errc::out_of_range));
        }
        return std::optional<runtime::monotonic_duration>{
          runtime::monotonic_duration{*rounded}};
    } catch (const std::overflow_error&) {
        return runtime::failure(bandwidth_error(errc::out_of_range));
    }
}

bandwidth_rate bytes_per_second_from_bits(std::uint64_t bits_per_second) {
    auto value = bandwidth_fraction::ratio(bits_per_second, 8U);
    KWAQUE_INVARIANT(
      bandwidth_invariant,
      value.has_value(),
      "constant bit-to-byte rate denominator was rejected");
    return bandwidth_rate::finite(std::move(*value));
}

} // namespace kwaque::simulation
