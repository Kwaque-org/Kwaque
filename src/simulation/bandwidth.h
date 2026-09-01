#ifndef KWAQUE_SRC_SIMULATION_BANDWIDTH_H_
#define KWAQUE_SRC_SIMULATION_BANDWIDTH_H_

#include "src/base/units.h"
#include "src/runtime/error.h"
#include "src/runtime/time.h"

#include <boost/multiprecision/cpp_int.hpp>

#include <array>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>

namespace kwaque::simulation {

inline constexpr std::uint32_t maximum_bandwidth_flows{96};
inline constexpr std::uint32_t maximum_bandwidth_constraints_per_flow{3};
inline constexpr std::uint32_t maximum_bandwidth_resources{
  maximum_bandwidth_flows * maximum_bandwidth_constraints_per_flow};

using bandwidth_integer_backend = boost::multiprecision::cpp_int_backend<
  4'096,
  4'096,
  boost::multiprecision::unsigned_magnitude,
  boost::multiprecision::checked,
  void>;
using bandwidth_integer = boost::multiprecision::
  number<bandwidth_integer_backend, boost::multiprecision::et_off>;

class bandwidth_planner;
class bandwidth_rate;

class bandwidth_fraction final {
public:
    bandwidth_fraction() = default;

    [[nodiscard]] static bandwidth_fraction whole(std::uint64_t value);
    [[nodiscard]] static runtime::result<bandwidth_fraction>
    ratio(std::uint64_t numerator, std::uint64_t denominator);

    [[nodiscard]] bool zero() const noexcept { return numerator_ == 0; }
    [[nodiscard]] bool
    equals(std::uint64_t numerator, std::uint64_t denominator) const;
    [[nodiscard]] std::optional<std::uint64_t> numerator_u64() const;
    [[nodiscard]] std::optional<std::uint64_t> denominator_u64() const;

    [[nodiscard]] bandwidth_fraction add(const bandwidth_fraction& other) const;
    [[nodiscard]] bandwidth_fraction
    subtract(const bandwidth_fraction& other) const;
    [[nodiscard]] bandwidth_fraction
    multiply(const bandwidth_fraction& other) const;
    [[nodiscard]] bandwidth_fraction multiply(std::uint64_t value) const;
    [[nodiscard]] bandwidth_fraction divide(std::uint64_t value) const;
    [[nodiscard]] std::strong_ordering
    compare(const bandwidth_fraction& other) const;

    bool operator==(const bandwidth_fraction&) const = default;

private:
    friend class bandwidth_planner;
    friend class bandwidth_rate;
    friend runtime::result<std::optional<runtime::monotonic_duration>>
    bandwidth_duration(
      const bandwidth_rate&, const bandwidth_fraction&) noexcept;
    friend bandwidth_fraction bandwidth_transfer(
      const bandwidth_rate&,
      runtime::monotonic_duration,
      const bandwidth_fraction&) noexcept;

    bandwidth_fraction(
      bandwidth_integer numerator, bandwidth_integer denominator);

    bandwidth_integer numerator_{0};
    bandwidth_integer denominator_{1};
};

class bandwidth_rate final {
public:
    enum class kind : std::uint8_t {
        unlimited,
        finite,
    };

    bandwidth_rate() = default;

    [[nodiscard]] static bandwidth_rate unlimited() noexcept;
    [[nodiscard]] static bandwidth_rate finite(bandwidth_fraction value);

    [[nodiscard]] kind type() const noexcept { return kind_; }
    [[nodiscard]] bool is_unlimited() const noexcept {
        return kind_ == kind::unlimited;
    }
    [[nodiscard]] const bandwidth_fraction& finite_value() const noexcept {
        return finite_;
    }

    bool operator==(const bandwidth_rate&) const = default;

private:
    kind kind_{kind::unlimited};
    bandwidth_fraction finite_{};
};

class bandwidth_capacity final {
public:
    bandwidth_capacity() = default;

    [[nodiscard]] static bandwidth_capacity unlimited() noexcept;
    [[nodiscard]] static bandwidth_capacity
    finite(std::uint64_t bytes_per_second) noexcept;

    [[nodiscard]] bool is_unlimited() const noexcept { return unlimited_; }
    [[nodiscard]] std::uint64_t bytes_per_second() const noexcept {
        return bytes_per_second_;
    }

    bool operator==(const bandwidth_capacity&) const = default;

private:
    bool unlimited_{true};
    std::uint64_t bytes_per_second_{0};
};

class bandwidth_resource_key final {
public:
    static constexpr std::size_t encoded_bytes{48};

    constexpr bandwidth_resource_key() noexcept = default;
    constexpr explicit bandwidth_resource_key(
      std::array<std::byte, encoded_bytes> value) noexcept
      : value_(value) {}

    [[nodiscard]] static bandwidth_resource_key
    numeric(std::uint8_t domain, std::uint64_t value) noexcept;

    auto operator<=>(const bandwidth_resource_key&) const = default;

private:
    std::array<std::byte, encoded_bytes> value_{};
};

struct bandwidth_constraint final {
    bandwidth_resource_key resource;
    bandwidth_capacity capacity;
};

struct bandwidth_flow final {
    std::uint64_t id{0};
    std::array<bandwidth_constraint, maximum_bandwidth_constraints_per_flow>
      constraints{};
    std::uint8_t constraint_count{0};
};

struct bandwidth_allocation final {
    std::uint64_t flow{0};
    bandwidth_rate rate;
};

struct bandwidth_allocation_digest final {
    std::array<std::uint64_t, 4> words{};

    bool operator==(const bandwidth_allocation_digest&) const = default;
};

class bandwidth_planner final {
public:
    [[nodiscard]] static runtime::result<std::unique_ptr<bandwidth_planner>>
    make(std::uint32_t maximum_flows);

    bandwidth_planner(const bandwidth_planner&) = delete;
    bandwidth_planner& operator=(const bandwidth_planner&) = delete;
    bandwidth_planner(bandwidth_planner&&) = delete;
    bandwidth_planner& operator=(bandwidth_planner&&) = delete;
    ~bandwidth_planner();

    void reset() noexcept;
    [[nodiscard]] runtime::result<void> add_flow(bandwidth_flow flow) noexcept;
    [[nodiscard]] runtime::result<void> solve() noexcept;

    [[nodiscard]] std::size_t allocation_count() const noexcept;
    [[nodiscard]] const bandwidth_allocation&
    allocation_at(std::size_t index) const noexcept;
    [[nodiscard]] std::size_t resource_count() const noexcept;
    [[nodiscard]] std::size_t membership_count() const noexcept;
    [[nodiscard]] std::uint32_t maximum_flows() const noexcept;
    [[nodiscard]] bandwidth_allocation_digest allocation_digest() const;

private:
    class impl;

    explicit bandwidth_planner(std::unique_ptr<impl> implementation) noexcept;

    std::unique_ptr<impl> impl_;
};

[[nodiscard]] runtime::result<std::optional<runtime::monotonic_duration>>
bandwidth_duration(
  const bandwidth_rate& rate, const bandwidth_fraction& remaining) noexcept;

[[nodiscard]] bandwidth_fraction bandwidth_transfer(
  const bandwidth_rate& rate,
  runtime::monotonic_duration elapsed,
  const bandwidth_fraction& remaining) noexcept;

[[nodiscard]] runtime::result<std::optional<runtime::monotonic_duration>>
bit_rate_transmission_duration(
  byte_count bytes, std::uint64_t bits_per_second) noexcept;

[[nodiscard]] bandwidth_rate
bytes_per_second_from_bits(std::uint64_t bits_per_second);

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_BANDWIDTH_H_
