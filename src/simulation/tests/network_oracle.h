#ifndef KWAQUE_SRC_SIMULATION_TESTS_NETWORK_ORACLE_H_
#define KWAQUE_SRC_SIMULATION_TESTS_NETWORK_ORACLE_H_

#include "src/runtime/error.h"
#include "src/runtime/fault.h"

#include <boost/multiprecision/cpp_int.hpp>

#include <array>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace kwaque::simulation::testing {

inline constexpr std::size_t oracle_maximum_endpoints{3};
inline constexpr std::size_t oracle_maximum_flows{96};
inline constexpr std::size_t oracle_maximum_operations{512};
inline constexpr std::size_t oracle_maximum_packets{512};

class oracle_fraction final {
public:
    using integer = boost::multiprecision::cpp_int;

    oracle_fraction() = default;
    explicit oracle_fraction(std::uint64_t value);
    oracle_fraction(integer numerator, integer denominator);

    [[nodiscard]] const integer& numerator() const noexcept {
        return numerator_;
    }
    [[nodiscard]] const integer& denominator() const noexcept {
        return denominator_;
    }
    [[nodiscard]] bool zero() const noexcept { return numerator_ == 0; }
    [[nodiscard]] oracle_fraction add(const oracle_fraction& other) const;
    [[nodiscard]] oracle_fraction subtract(const oracle_fraction& other) const;
    [[nodiscard]] oracle_fraction multiply(std::uint64_t value) const;
    [[nodiscard]] oracle_fraction divide(std::uint64_t value) const;
    [[nodiscard]] std::strong_ordering
    compare(const oracle_fraction& other) const;
    [[nodiscard]] bool
    equals(std::uint64_t numerator, std::uint64_t denominator) const;

    bool operator==(const oracle_fraction&) const = default;

private:
    void normalize();

    integer numerator_{0};
    integer denominator_{1};
};

class oracle_capacity final {
public:
    constexpr oracle_capacity() noexcept = default;

    [[nodiscard]] static oracle_capacity unlimited() noexcept {
        return oracle_capacity{};
    }
    [[nodiscard]] static oracle_capacity finite(std::uint64_t value) noexcept {
        return oracle_capacity{false, value};
    }

    [[nodiscard]] bool is_unlimited() const noexcept { return unlimited_; }
    [[nodiscard]] std::uint64_t value() const noexcept { return value_; }

    bool operator==(const oracle_capacity&) const = default;

private:
    constexpr oracle_capacity(bool unlimited, std::uint64_t value) noexcept
      : value_(value)
      , unlimited_(unlimited) {}

    std::uint64_t value_{0};
    bool unlimited_{true};
};

struct oracle_constraint final {
    std::uint64_t resource{0};
    oracle_capacity capacity;
};

struct oracle_flow final {
    std::uint64_t id{0};
    std::uint64_t bytes{0};
    std::array<oracle_constraint, 3> constraints{};
    std::uint8_t constraint_count{0};
};

struct oracle_rate final {
    bool unlimited{true};
    oracle_fraction finite;
};

struct oracle_allocation final {
    std::uint64_t flow{0};
    oracle_rate rate;
};

struct oracle_digest final {
    std::array<std::uint64_t, 4> words{};

    bool operator==(const oracle_digest&) const = default;
};

struct oracle_solution final {
    std::vector<oracle_allocation> allocations;
    std::size_t resources{0};
    std::size_t memberships{0};
    oracle_digest digest;
};

[[nodiscard]] runtime::result<oracle_solution>
solve_bandwidth_oracle(std::span<const oracle_flow> flows);

enum class oracle_step_kind : std::uint8_t {
    bind_exact,
    bind_wildcard,
    connect_implicit,
    connect_explicit,
    accept,
    write,
    read,
    shutdown_output,
    reset,
    close,
    dns_record,
    dns_resolve,
    egress_finite,
    egress_zero,
    egress_unlimited,
    link_finite,
    link_zero,
    link_unlimited,
    ingress_finite,
    ingress_zero,
    ingress_unlimited,
    partition,
    heal,
    clog,
    unclog,
};

inline constexpr std::size_t oracle_step_kind_count
  = static_cast<std::size_t>(oracle_step_kind::unclog) + 1U;

struct oracle_step final {
    oracle_step_kind kind{oracle_step_kind::bind_exact};
    std::uint8_t source{0};
    std::uint8_t target{0};
    std::uint16_t port{0};
    std::uint64_t value{0};
    std::uint8_t pattern{0};
    runtime::fault_action action{runtime::fault_action::none};
};

class oracle_script final {
public:
    [[nodiscard]] static runtime::result<oracle_script>
    generate(std::uint64_t seed, std::size_t operations);

    [[nodiscard]] std::uint64_t seed() const noexcept { return seed_; }
    [[nodiscard]] std::span<const oracle_step> steps() const noexcept {
        return steps_;
    }
    [[nodiscard]] std::string render() const;

private:
    oracle_script(std::uint64_t seed, std::vector<oracle_step> steps) noexcept
      : seed_(seed)
      , steps_(std::move(steps)) {}

    std::uint64_t seed_{0};
    std::vector<oracle_step> steps_;
};

struct dense_oracle_snapshot final {
    std::array<std::string, oracle_maximum_endpoints> visible;
    std::array<std::uint64_t, oracle_maximum_endpoints> ingress{};
    std::array<std::uint64_t, oracle_maximum_endpoints> egress{};
    std::uint64_t live_packets{0};
    std::uint64_t logical_bytes{0};
    std::uint64_t tombstones{0};
    std::uint64_t dns_answers{0};
    std::uint64_t digest{0};

    bool operator==(const dense_oracle_snapshot&) const = default;
};

class dense_network_oracle final {
public:
    explicit dense_network_oracle(std::uint8_t endpoints = 3);

    [[nodiscard]] runtime::result<void> apply(const oracle_step& step);
    [[nodiscard]] dense_oracle_snapshot snapshot() const;

private:
    struct directed_link final {
        oracle_capacity capacity;
        bool partitioned{false};
        bool clogged{false};
    };

    struct packet final {
        std::uint64_t id{0};
        std::uint64_t sequence{0};
        std::uint8_t source{0};
        std::uint8_t target{0};
        std::string bytes;
        bool ready{false};
        bool retired{false};
    };

    [[nodiscard]] runtime::result<void> write(const oracle_step& step);
    void deliver_ready(std::uint8_t source, std::uint8_t target);
    void publish(packet& value);
    void recompute_digest() noexcept;

    std::array<
      std::array<directed_link, oracle_maximum_endpoints>,
      oracle_maximum_endpoints>
      links_{};
    std::array<oracle_capacity, oracle_maximum_endpoints> egress_{};
    std::array<oracle_capacity, oracle_maximum_endpoints> ingress_{};
    std::array<std::string, oracle_maximum_endpoints> visible_{};
    std::array<std::uint16_t, oracle_maximum_endpoints> exact_ports_{};
    std::array<bool, oracle_maximum_endpoints> wildcard_bound_{};
    std::array<bool, oracle_maximum_endpoints> connected_{};
    std::array<bool, oracle_maximum_endpoints> output_open_{};
    std::array<std::uint64_t, oracle_maximum_endpoints> dns_answers_{};
    std::vector<packet> packets_;
    std::uint64_t next_packet_id_{1};
    std::uint64_t next_sequence_{0};
    std::uint64_t tombstones_{0};
    std::uint64_t digest_{0};
    std::uint8_t endpoints_{0};
};

} // namespace kwaque::simulation::testing

#endif // KWAQUE_SRC_SIMULATION_TESTS_NETWORK_ORACLE_H_
