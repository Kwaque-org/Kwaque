#include "src/simulation/tests/network_oracle.h"

#include "src/simulation/sha256.h"

#include <boost/multiprecision/cpp_int/import_export.hpp>

#include <algorithm>
#include <array>
#include <charconv>
#include <compare>
#include <iterator>
#include <limits>
#include <map>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <utility>

namespace kwaque::simulation::testing {

namespace {

[[nodiscard]] runtime::operation_error oracle_error(errc code) noexcept {
    return runtime::operation_error{code, runtime::operation_kind::network};
}

[[nodiscard]] oracle_fraction::integer
absolute(oracle_fraction::integer value) {
    return value < 0 ? -value : value;
}

[[nodiscard]] oracle_fraction::integer
gcd(oracle_fraction::integer left, oracle_fraction::integer right) {
    left = absolute(std::move(left));
    right = absolute(std::move(right));
    while (right != 0) {
        oracle_fraction::integer remainder = left % right;
        left = std::move(right);
        right = std::move(remainder);
    }
    return left;
}

[[nodiscard]] std::uint64_t next_random(std::uint64_t& state) noexcept {
    state ^= state << 13U;
    state ^= state >> 7U;
    state ^= state << 17U;
    return state;
}

[[nodiscard]] std::uint64_t
mix(std::uint64_t state, std::uint64_t value) noexcept {
    state ^= value + UINT64_C(0x9e3779b97f4a7c15) + (state << 6U)
             + (state >> 2U);
    return state;
}

} // namespace

oracle_fraction::oracle_fraction(std::uint64_t value)
  : numerator_(value) {}

oracle_fraction::oracle_fraction(integer numerator, integer denominator)
  : numerator_(std::move(numerator))
  , denominator_(std::move(denominator)) {
    if (denominator_ <= 0 || numerator_ < 0) {
        throw std::invalid_argument("invalid oracle fraction");
    }
    normalize();
}

void oracle_fraction::normalize() {
    if (numerator_ == 0) {
        denominator_ = 1;
        return;
    }
    const auto divisor = gcd(numerator_, denominator_);
    numerator_ /= divisor;
    denominator_ /= divisor;
}

oracle_fraction oracle_fraction::add(const oracle_fraction& other) const {
    return oracle_fraction{
      numerator_ * other.denominator_ + other.numerator_ * denominator_,
      denominator_ * other.denominator_};
}

oracle_fraction oracle_fraction::subtract(const oracle_fraction& other) const {
    const auto left = numerator_ * other.denominator_;
    const auto right = other.numerator_ * denominator_;
    if (right > left) {
        throw std::underflow_error("oracle fraction subtraction underflow");
    }
    return oracle_fraction{left - right, denominator_ * other.denominator_};
}

oracle_fraction oracle_fraction::multiply(std::uint64_t value) const {
    return oracle_fraction{numerator_ * value, denominator_};
}

oracle_fraction oracle_fraction::divide(std::uint64_t value) const {
    if (value == 0) {
        throw std::invalid_argument("oracle fraction division by zero");
    }
    return oracle_fraction{numerator_, denominator_ * value};
}

std::strong_ordering
oracle_fraction::compare(const oracle_fraction& other) const {
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

bool oracle_fraction::equals(
  std::uint64_t numerator, std::uint64_t denominator) const {
    if (denominator == 0) {
        return false;
    }
    return compare(oracle_fraction{integer{numerator}, integer{denominator}})
           == std::strong_ordering::equal;
}

runtime::result<oracle_solution>
solve_bandwidth_oracle(std::span<const oracle_flow> flows) {
    if (flows.size() > oracle_maximum_flows) {
        return runtime::failure(oracle_error(errc::out_of_range));
    }

    struct resource_state final {
        oracle_fraction remaining;
        std::vector<std::size_t> members;
        std::size_t active{0};
    };
    struct flow_state final {
        std::vector<std::uint64_t> resources;
        bool limited{false};
        bool active{false};
        oracle_rate rate;
    };

    std::vector<std::size_t> order(flows.size());
    std::iota(order.begin(), order.end(), 0U);
    std::sort(order.begin(), order.end(), [&](auto left, auto right) {
        return flows[left].id < flows[right].id;
    });
    for (std::size_t index = 0; index < order.size(); ++index) {
        const auto& flow = flows[order[index]];
        if (
          flow.id == 0 || flow.constraint_count > flow.constraints.size()
          || (index != 0 && flow.id == flows[order[index - 1U]].id)) {
            return runtime::failure(oracle_error(errc::invalid_argument));
        }
    }

    std::map<std::uint64_t, resource_state> resources;
    std::map<std::uint64_t, oracle_capacity> capacities;
    std::vector<flow_state> states(flows.size());
    std::size_t memberships = 0;
    std::size_t active = 0;
    for (const auto flow_index : order) {
        const auto& flow = flows[flow_index];
        auto& state = states[flow_index];
        std::array<std::uint64_t, 3> seen{};
        std::size_t seen_count = 0;
        for (std::size_t index = 0; index < flow.constraint_count; ++index) {
            const auto& constraint = flow.constraints[index];
            if (
              constraint.resource == 0
              || std::find(
                   seen.begin(),
                   seen.begin() + static_cast<std::ptrdiff_t>(seen_count),
                   constraint.resource)
                   != seen.begin() + static_cast<std::ptrdiff_t>(seen_count)) {
                return runtime::failure(oracle_error(errc::invalid_argument));
            }
            seen[seen_count++] = constraint.resource;
            const auto known = capacities.find(constraint.resource);
            if (
              known != capacities.end()
              && known->second != constraint.capacity) {
                return runtime::failure(oracle_error(errc::invalid_argument));
            }
            capacities.emplace(constraint.resource, constraint.capacity);
            if (constraint.capacity.is_unlimited()) {
                continue;
            }
            auto [position, inserted] = resources.try_emplace(
              constraint.resource,
              resource_state{
                .remaining = oracle_fraction{constraint.capacity.value()}});
            static_cast<void>(inserted);
            position->second.members.push_back(flow_index);
            ++position->second.active;
            state.resources.push_back(constraint.resource);
            state.limited = true;
            ++memberships;
        }
        if (state.limited) {
            state.active = true;
            ++active;
        }
    }

    oracle_fraction fill;
    while (active != 0) {
        std::optional<oracle_fraction> minimum;
        std::vector<std::uint64_t> limiting;
        for (const auto& [id, resource] : resources) {
            if (resource.active == 0) {
                continue;
            }
            if (resource.remaining.zero()) {
                minimum = oracle_fraction{};
                limiting.clear();
                limiting.push_back(id);
                break;
            }
            const auto share = resource.remaining.divide(resource.active);
            if (!minimum) {
                minimum = share;
                limiting.clear();
                limiting.push_back(id);
                continue;
            }
            const auto order = share.compare(*minimum);
            if (order == std::strong_ordering::less) {
                minimum = share;
                limiting.clear();
                limiting.push_back(id);
            } else if (order == std::strong_ordering::equal) {
                limiting.push_back(id);
            }
        }
        if (!minimum || limiting.empty()) {
            return runtime::failure(oracle_error(errc::invariant_violation));
        }
        if (!minimum->zero()) {
            fill = fill.add(*minimum);
            for (auto& [id, resource] : resources) {
                static_cast<void>(id);
                if (resource.active != 0) {
                    resource.remaining = resource.remaining.subtract(
                      minimum->multiply(resource.active));
                }
            }
        }

        for (const auto id : limiting) {
            auto& resource = resources.at(id);
            for (const auto flow_index : resource.members) {
                auto& state = states[flow_index];
                if (!state.active) {
                    continue;
                }
                state.active = false;
                state.rate = oracle_rate{.unlimited = false, .finite = fill};
                --active;
                for (const auto resource_id : state.resources) {
                    auto& membership = resources.at(resource_id);
                    if (membership.active != 0) {
                        --membership.active;
                    }
                }
            }
        }
    }

    oracle_solution solution;
    solution.resources = resources.size();
    solution.memberships = memberships;
    solution.allocations.reserve(flows.size());
    for (const auto flow_index : order) {
        solution.allocations.push_back(
          oracle_allocation{
            .flow = flows[flow_index].id,
            .rate = states[flow_index].limited ? states[flow_index].rate
                                               : oracle_rate{},
          });
    }

    sha256_hasher hasher;
    auto update = [&](const void* data, std::size_t size) {
        hasher.update(data, size);
    };
    auto update_u16 = [&](std::uint16_t value) {
        const std::array<unsigned char, 2> bytes{
          static_cast<unsigned char>(value >> 8U),
          static_cast<unsigned char>(value & 0xffU),
        };
        update(bytes.data(), bytes.size());
    };
    auto update_u64 = [&](std::uint64_t value) {
        std::array<unsigned char, 8> bytes{};
        for (std::size_t index = 0; index < bytes.size(); ++index) {
            bytes[index] = static_cast<unsigned char>(
              value >> ((bytes.size() - index - 1U) * 8U));
        }
        update(bytes.data(), bytes.size());
    };
    auto update_integer = [&](const oracle_fraction::integer& value) {
        std::vector<unsigned char> bytes;
        boost::multiprecision::export_bits(
          value, std::back_inserter(bytes), 8U, true);
        update_u16(static_cast<std::uint16_t>(bytes.size()));
        update(bytes.data(), bytes.size());
    };
    const unsigned char version{1};
    update(&version, 1);
    update_u16(static_cast<std::uint16_t>(solution.allocations.size()));
    for (const auto& allocation : solution.allocations) {
        update_u64(allocation.flow);
        if (allocation.rate.unlimited) {
            const unsigned char tag{2};
            update(&tag, 1);
        } else if (allocation.rate.finite.zero()) {
            const unsigned char tag{0};
            update(&tag, 1);
        } else {
            const unsigned char tag{1};
            update(&tag, 1);
            update_integer(allocation.rate.finite.numerator());
            update_integer(allocation.rate.finite.denominator());
        }
    }
    const auto bytes = std::move(hasher).final();
    for (std::size_t word = 0; word < solution.digest.words.size(); ++word) {
        for (std::size_t byte = 0; byte < 8U; ++byte) {
            solution.digest.words[word] = (solution.digest.words[word] << 8U)
                                          | bytes[word * 8U + byte];
        }
    }
    return solution;
}

runtime::result<oracle_script>
oracle_script::generate(std::uint64_t seed, std::size_t operations) {
    using row = std::pair<oracle_step_kind, runtime::fault_action>;
    constexpr std::array actions{
      runtime::fault_action::error,
      runtime::fault_action::delay,
      runtime::fault_action::short_operation,
      runtime::fault_action::drop,
      runtime::fault_action::duplicate,
      runtime::fault_action::reorder,
      runtime::fault_action::disconnect,
      runtime::fault_action::corrupt,
      runtime::fault_action::drop_completion,
    };
    constexpr std::array fault_rows{
      row{oracle_step_kind::connect_implicit, runtime::fault_action::error},
      row{oracle_step_kind::connect_implicit, runtime::fault_action::delay},
      row{
        oracle_step_kind::connect_implicit, runtime::fault_action::disconnect},
      row{
        oracle_step_kind::connect_implicit,
        runtime::fault_action::drop_completion},
      row{oracle_step_kind::accept, runtime::fault_action::error},
      row{oracle_step_kind::accept, runtime::fault_action::delay},
      row{oracle_step_kind::accept, runtime::fault_action::disconnect},
      row{oracle_step_kind::accept, runtime::fault_action::drop_completion},
      row{oracle_step_kind::write, runtime::fault_action::error},
      row{oracle_step_kind::write, runtime::fault_action::delay},
      row{oracle_step_kind::write, runtime::fault_action::short_operation},
      row{oracle_step_kind::write, runtime::fault_action::drop},
      row{oracle_step_kind::write, runtime::fault_action::duplicate},
      row{oracle_step_kind::write, runtime::fault_action::reorder},
      row{oracle_step_kind::write, runtime::fault_action::disconnect},
      row{oracle_step_kind::write, runtime::fault_action::corrupt},
      row{oracle_step_kind::write, runtime::fault_action::drop_completion},
      row{oracle_step_kind::read, runtime::fault_action::error},
      row{oracle_step_kind::read, runtime::fault_action::delay},
      row{oracle_step_kind::read, runtime::fault_action::short_operation},
      row{oracle_step_kind::read, runtime::fault_action::drop},
      row{oracle_step_kind::read, runtime::fault_action::disconnect},
      row{oracle_step_kind::read, runtime::fault_action::corrupt},
      row{oracle_step_kind::read, runtime::fault_action::drop_completion},
      row{oracle_step_kind::close, runtime::fault_action::error},
      row{oracle_step_kind::close, runtime::fault_action::delay},
      row{oracle_step_kind::close, runtime::fault_action::drop_completion},
      row{oracle_step_kind::dns_resolve, runtime::fault_action::error},
      row{oracle_step_kind::dns_resolve, runtime::fault_action::delay},
      row{
        oracle_step_kind::dns_resolve, runtime::fault_action::drop_completion},
    };
    const auto required = oracle_step_kind_count + fault_rows.size();
    if (operations < required || operations > oracle_maximum_operations) {
        return runtime::failure(oracle_error(errc::out_of_range));
    }
    std::vector<oracle_step> steps;
    steps.reserve(operations);
    for (std::size_t index = 0; index < oracle_step_kind_count; ++index) {
        steps.push_back(
          oracle_step{
            .kind = static_cast<oracle_step_kind>(index),
            .source = static_cast<std::uint8_t>(index % 3U),
            .target = static_cast<std::uint8_t>((index + 1U) % 3U),
            .port = static_cast<std::uint16_t>(10'000U + index),
            .value = 1U + index,
          });
    }
    for (const auto& [kind, action] : fault_rows) {
        steps.push_back(
          oracle_step{
            .kind = kind,
            .source = 0,
            .target = 1,
            .value = 8,
            .action = action,
          });
    }
    auto random = seed == 0 ? UINT64_C(0x6a09e667f3bcc909) : seed;
    while (steps.size() < operations) {
        const auto value = next_random(random);
        const auto kind = static_cast<oracle_step_kind>(
          value % oracle_step_kind_count);
        steps.push_back(
          oracle_step{
            .kind = kind,
            .source = static_cast<std::uint8_t>((value >> 8U) % 3U),
            .target = static_cast<std::uint8_t>((value >> 16U) % 3U),
            .port = static_cast<std::uint16_t>(
              10'000U + ((value >> 24U) % 1'000U)),
            .value = 1U + ((value >> 40U) % 128U),
            .action = actions[(value >> 56U) % actions.size()],
          });
    }
    return oracle_script{seed, std::move(steps)};
}

std::string oracle_script::render() const {
    std::string result;
    result.reserve(64U + steps_.size() * 48U);
    result.append("seed=");
    result.append(std::to_string(seed_));
    result.append(" operations=");
    result.append(std::to_string(steps_.size()));
    result.push_back('\n');
    for (std::size_t index = 0; index < steps_.size(); ++index) {
        const auto& step = steps_[index];
        result.append(std::to_string(index));
        result.push_back(' ');
        result.append(std::to_string(static_cast<std::uint8_t>(step.kind)));
        result.push_back(' ');
        result.append(std::to_string(step.source));
        result.push_back(' ');
        result.append(std::to_string(step.target));
        result.push_back(' ');
        result.append(std::to_string(step.port));
        result.push_back(' ');
        result.append(std::to_string(step.value));
        result.push_back(' ');
        result.append(std::to_string(static_cast<std::uint8_t>(step.action)));
        result.push_back('\n');
    }
    return result;
}

dense_network_oracle::dense_network_oracle(std::uint8_t endpoints)
  : endpoints_(endpoints) {
    if (endpoints_ < 2 || endpoints_ > oracle_maximum_endpoints) {
        throw std::invalid_argument("invalid dense oracle endpoint count");
    }
    egress_.fill(oracle_capacity::unlimited());
    ingress_.fill(oracle_capacity::unlimited());
    output_open_.fill(true);
    packets_.reserve(oracle_maximum_packets);
}

runtime::result<void> dense_network_oracle::apply(const oracle_step& step) {
    if (step.source >= endpoints_ || step.target >= endpoints_) {
        return runtime::failure(oracle_error(errc::invalid_argument));
    }
    auto set_capacity = [&](oracle_capacity& target, oracle_capacity value) {
        target = value;
        for (std::uint8_t source = 0; source < endpoints_; ++source) {
            for (std::uint8_t destination = 0; destination < endpoints_;
                 ++destination) {
                deliver_ready(source, destination);
            }
        }
    };
    switch (step.kind) {
    case oracle_step_kind::bind_exact:
        if (exact_ports_[step.source] != 0 || wildcard_bound_[step.source]) {
            return runtime::failure(oracle_error(errc::already_exists));
        }
        exact_ports_[step.source] = step.port == 0 ? 10'000U + step.source
                                                   : step.port;
        break;
    case oracle_step_kind::bind_wildcard:
        if (exact_ports_[step.source] != 0 || wildcard_bound_[step.source]) {
            return runtime::failure(oracle_error(errc::already_exists));
        }
        wildcard_bound_[step.source] = true;
        break;
    case oracle_step_kind::connect_implicit:
    case oracle_step_kind::connect_explicit:
        if (step.action == runtime::fault_action::error) {
            return runtime::failure(oracle_error(errc::fault_injected));
        }
        if (step.action == runtime::fault_action::disconnect) {
            return runtime::failure(oracle_error(errc::network_failure));
        }
        if (exact_ports_[step.target] == 0 && !wildcard_bound_[step.target]) {
            return runtime::failure(oracle_error(errc::network_failure));
        }
        connected_[step.source] = true;
        output_open_[step.source] = true;
        break;
    case oracle_step_kind::accept:
        if (step.action == runtime::fault_action::error) {
            return runtime::failure(oracle_error(errc::fault_injected));
        }
        if (step.action == runtime::fault_action::disconnect) {
            connected_[step.source] = false;
            output_open_[step.source] = false;
            recompute_digest();
            return runtime::failure(oracle_error(errc::network_failure));
        }
        if (!connected_[step.source]) {
            return runtime::failure(oracle_error(errc::unavailable));
        }
        break;
    case oracle_step_kind::write:
        return write(step);
    case oracle_step_kind::read:
        if (step.action == runtime::fault_action::error) {
            return runtime::failure(oracle_error(errc::fault_injected));
        }
        if (step.action == runtime::fault_action::disconnect) {
            connected_[step.source] = false;
            output_open_[step.source] = false;
            recompute_digest();
            return runtime::failure(oracle_error(errc::network_failure));
        }
        for (auto& packet : packets_) {
            if (packet.target == step.source && !packet.ready) {
                packet.retired = true;
            }
        }
        break;
    case oracle_step_kind::shutdown_output:
        output_open_[step.source] = false;
        break;
    case oracle_step_kind::reset:
        connected_[step.source] = false;
        output_open_[step.source] = false;
        for (auto& packet : packets_) {
            if (!packet.retired && packet.source == step.source) {
                packet.retired = true;
                ++tombstones_;
            }
        }
        break;
    case oracle_step_kind::close:
        connected_[step.source] = false;
        output_open_[step.source] = false;
        recompute_digest();
        if (step.action == runtime::fault_action::error) {
            return runtime::failure(oracle_error(errc::fault_injected));
        }
        break;
    case oracle_step_kind::dns_record:
        dns_answers_[step.source] = step.value;
        break;
    case oracle_step_kind::dns_resolve:
        if (step.action == runtime::fault_action::error) {
            return runtime::failure(oracle_error(errc::fault_injected));
        }
        if (dns_answers_[step.source] == 0) {
            return runtime::failure(oracle_error(errc::dns_failure));
        }
        break;
    case oracle_step_kind::egress_finite:
        set_capacity(egress_[step.source], oracle_capacity::finite(step.value));
        break;
    case oracle_step_kind::egress_zero:
        set_capacity(egress_[step.source], oracle_capacity::finite(0));
        break;
    case oracle_step_kind::egress_unlimited:
        set_capacity(egress_[step.source], oracle_capacity::unlimited());
        break;
    case oracle_step_kind::link_finite:
        set_capacity(
          links_[step.source][step.target].capacity,
          oracle_capacity::finite(step.value));
        break;
    case oracle_step_kind::link_zero:
        set_capacity(
          links_[step.source][step.target].capacity,
          oracle_capacity::finite(0));
        break;
    case oracle_step_kind::link_unlimited:
        set_capacity(
          links_[step.source][step.target].capacity,
          oracle_capacity::unlimited());
        break;
    case oracle_step_kind::ingress_finite:
        set_capacity(
          ingress_[step.source], oracle_capacity::finite(step.value));
        break;
    case oracle_step_kind::ingress_zero:
        set_capacity(ingress_[step.source], oracle_capacity::finite(0));
        break;
    case oracle_step_kind::ingress_unlimited:
        set_capacity(ingress_[step.source], oracle_capacity::unlimited());
        break;
    case oracle_step_kind::partition:
        links_[step.source][step.target].partitioned = true;
        break;
    case oracle_step_kind::heal:
        links_[step.source][step.target].partitioned = false;
        break;
    case oracle_step_kind::clog:
        links_[step.source][step.target].clogged = true;
        break;
    case oracle_step_kind::unclog:
        links_[step.source][step.target].clogged = false;
        deliver_ready(step.source, step.target);
        break;
    }
    recompute_digest();
    return {};
}

runtime::result<void> dense_network_oracle::write(const oracle_step& step) {
    if (!connected_[step.source] || !output_open_[step.source]) {
        return runtime::failure(oracle_error(errc::closed));
    }
    if (step.action == runtime::fault_action::error) {
        return runtime::failure(oracle_error(errc::fault_injected));
    }
    if (step.action == runtime::fault_action::disconnect) {
        connected_[step.source] = false;
        output_open_[step.source] = false;
        recompute_digest();
        return runtime::failure(oracle_error(errc::network_failure));
    }
    const auto size = std::max<std::uint64_t>(1, step.value);
    std::string bytes(
      static_cast<std::size_t>(size), static_cast<char>('a' + step.source));
    if (step.action == runtime::fault_action::short_operation) {
        bytes.resize(std::max<std::size_t>(1, bytes.size() / 2U));
    }
    if (step.action == runtime::fault_action::corrupt) {
        bytes[0] ^= 1;
    }
    const auto copies = step.action == runtime::fault_action::duplicate ? 2U
                                                                        : 1U;
    const auto reusable = static_cast<std::size_t>(std::ranges::count_if(
      packets_, [](const packet& value) { return value.retired; }));
    const auto unused = oracle_maximum_packets - packets_.size();
    if (copies > reusable + unused) {
        return runtime::failure(oracle_error(errc::queue_full));
    }
    for (std::uint32_t copy = 0; copy < copies; ++copy) {
        packet replacement{
          .id = next_packet_id_++,
          .sequence = next_sequence_++,
          .source = step.source,
          .target = step.target,
          .bytes = bytes,
        };
        auto free = std::ranges::find_if(
          packets_, [](const packet& value) { return value.retired; });
        packet* inserted = nullptr;
        if (free == packets_.end()) {
            packets_.push_back(std::move(replacement));
            inserted = &packets_.back();
        } else {
            *free = std::move(replacement);
            inserted = &*free;
        }
        if (step.action == runtime::fault_action::drop) {
            inserted->retired = true;
            ++tombstones_;
        } else {
            publish(*inserted);
        }
    }
    recompute_digest();
    return {};
}

void dense_network_oracle::publish(packet& value) {
    auto& link = links_[value.source][value.target];
    const bool zero
      = (!egress_[value.source].is_unlimited()
         && egress_[value.source].value() == 0)
        || (!link.capacity.is_unlimited() && link.capacity.value() == 0)
        || (!ingress_[value.target].is_unlimited() && ingress_[value.target].value() == 0);
    if (link.clogged || zero) {
        value.ready = true;
        return;
    }
    if (link.partitioned) {
        value.retired = true;
        ++tombstones_;
        return;
    }
    visible_[value.target].append(value.bytes);
}

void dense_network_oracle::deliver_ready(
  std::uint8_t source, std::uint8_t target) {
    for (auto& packet : packets_) {
        if (
          packet.source != source || packet.target != target || packet.retired
          || !packet.ready) {
            continue;
        }
        packet.ready = false;
        publish(packet);
        if (packet.ready) {
            break;
        }
    }
    recompute_digest();
}

void dense_network_oracle::recompute_digest() noexcept {
    std::uint64_t state{UINT64_C(0xcbf29ce484222325)};
    state = mix(state, endpoints_);
    state = mix(state, next_packet_id_);
    state = mix(state, next_sequence_);
    state = mix(state, tombstones_);
    for (std::uint8_t endpoint = 0; endpoint < endpoints_; ++endpoint) {
        state = mix(state, exact_ports_[endpoint]);
        state = mix(state, wildcard_bound_[endpoint]);
        state = mix(state, connected_[endpoint]);
        state = mix(state, output_open_[endpoint]);
        state = mix(state, dns_answers_[endpoint]);
        for (const auto byte : visible_[endpoint]) {
            state = mix(state, static_cast<unsigned char>(byte));
        }
        for (std::uint8_t target = 0; target < endpoints_; ++target) {
            const auto& link = links_[endpoint][target];
            state = mix(state, link.partitioned);
            state = mix(state, link.clogged);
            state = mix(state, link.capacity.is_unlimited());
            state = mix(state, link.capacity.value());
        }
    }
    for (const auto& packet : packets_) {
        state = mix(state, packet.id);
        state = mix(state, packet.sequence);
        state = mix(state, packet.source);
        state = mix(state, packet.target);
        state = mix(state, packet.ready);
        state = mix(state, packet.retired);
    }
    digest_ = state;
}

dense_oracle_snapshot dense_network_oracle::snapshot() const {
    dense_oracle_snapshot result;
    result.visible = visible_;
    result.tombstones = tombstones_;
    result.digest = digest_;
    for (std::uint8_t endpoint = 0; endpoint < endpoints_; ++endpoint) {
        result.dns_answers += dns_answers_[endpoint];
        result.egress[endpoint] = egress_[endpoint].is_unlimited()
                                    ? std::numeric_limits<std::uint64_t>::max()
                                    : egress_[endpoint].value();
        result.ingress[endpoint] = ingress_[endpoint].is_unlimited()
                                     ? std::numeric_limits<std::uint64_t>::max()
                                     : ingress_[endpoint].value();
    }
    for (const auto& packet : packets_) {
        if (!packet.retired) {
            ++result.live_packets;
            result.logical_bytes += packet.bytes.size();
        }
    }
    return result;
}

} // namespace kwaque::simulation::testing
