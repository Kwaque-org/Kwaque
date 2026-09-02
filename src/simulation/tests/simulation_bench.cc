#include "src/bytes/fragmented_buffer.h"
#include "src/runtime/testing/contracts/network_contract.h"
#include "src/simulation/bandwidth.h"
#include "src/simulation/fake_network.h"
#include "src/simulation/scheduler.h"
#include "src/simulation/tests/network_oracle.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/later.hh>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace kwaque::simulation {

namespace {

constexpr std::uint32_t benchmark_flow_count = 96;
constexpr std::uint64_t benchmark_capacity = 96'000;
constexpr std::uint32_t integrated_flow_count = 8;
constexpr std::uint64_t integrated_payload_bytes = 4'096;
constexpr std::uint64_t benchmark_scheduler_restart_reserve = 65'536;

constexpr auto benchmark_loopback = runtime::network_address::ipv4(
  {std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}});
constexpr auto benchmark_target = runtime::network_address::ipv4(
  {std::byte{127}, std::byte{0}, std::byte{0}, std::byte{2}});

constexpr runtime::network_address benchmark_address(std::uint8_t last) {
    return runtime::network_address::ipv4(
      {std::byte{127}, std::byte{0}, std::byte{2}, std::byte{last}});
}

scheduler_limits benchmark_scheduler_limits() {
    auto made = scheduler_limits::make(
      scheduler_limit_values{
        .pending_events = 65'536,
        .events_per_pump = 4'096,
        .total_events = 1'000'000,
        .maximum_deadline = scheduler_limits::maximum_deadline_absolute,
      });
    if (!made) {
        throw std::runtime_error("network benchmark scheduler limits");
    }
    return *made;
}

struct bandwidth_fixture {
    std::unique_ptr<bandwidth_planner> planner;
    std::vector<bandwidth_flow> actual;
    std::vector<testing::oracle_flow> expected;

    explicit bandwidth_fixture(bool one_to_many) {
        auto made = bandwidth_planner::make(benchmark_flow_count);
        if (!made) {
            throw std::runtime_error(
              "bandwidth benchmark planner construction");
        }
        planner = std::move(*made);
        actual.reserve(benchmark_flow_count);
        expected.reserve(benchmark_flow_count);
        for (std::uint64_t index = 0; index < benchmark_flow_count; ++index) {
            const auto first_resource = one_to_many ? std::uint64_t{1}
                                                    : 100U + index;
            const auto second_resource = one_to_many ? 100U + index
                                                     : std::uint64_t{2};
            bandwidth_flow flow{.id = index + 1U, .constraint_count = 2};
            flow.constraints[0] = bandwidth_constraint{
              .resource = bandwidth_resource_key::numeric(1, first_resource),
              .capacity = bandwidth_capacity::finite(benchmark_capacity),
            };
            flow.constraints[1] = bandwidth_constraint{
              .resource = bandwidth_resource_key::numeric(2, second_resource),
              .capacity = bandwidth_capacity::finite(benchmark_capacity),
            };
            actual.push_back(std::move(flow));
            testing::oracle_flow oracle{
              .id = index + 1U, .bytes = 8'192, .constraint_count = 2};
            oracle.constraints[0] = testing::oracle_constraint{
              .resource = first_resource,
              .capacity = testing::oracle_capacity::finite(benchmark_capacity),
            };
            oracle.constraints[1] = testing::oracle_constraint{
              .resource = second_resource,
              .capacity = testing::oracle_capacity::finite(benchmark_capacity),
            };
            expected.push_back(std::move(oracle));
        }
    }

    std::size_t solve_fixed_workspace() {
        planner->reset();
        for (const auto& flow : actual) {
            if (!planner->add_flow(flow)) {
                throw std::runtime_error("bandwidth benchmark flow admission");
            }
        }
        if (!planner->solve()) {
            throw std::runtime_error("bandwidth benchmark solve");
        }
        perf_tests::do_not_optimize(planner->allocation_digest().words);
        return planner->allocation_count();
    }

    std::size_t solve_independent_oracle() const {
        auto solution = testing::solve_bandwidth_oracle(expected);
        if (!solution) {
            throw std::runtime_error("bandwidth benchmark oracle solve");
        }
        perf_tests::do_not_optimize(solution->digest.words);
        return solution->allocations.size();
    }
};

struct one_to_many_fixture : bandwidth_fixture {
    one_to_many_fixture()
      : bandwidth_fixture(true) {}
};

struct many_to_one_fixture : bandwidth_fixture {
    many_to_one_fixture()
      : bandwidth_fixture(false) {}
};

class integrated_network_fixture {
public:
    explicit integrated_network_fixture(bool many_to_one)
      : many_to_one_(many_to_one) {
        listeners_.reserve(integrated_flow_count);
        clients_.reserve(integrated_flow_count);
        servers_.reserve(integrated_flow_count);
        writes_.reserve(integrated_flow_count);
        source_ = runtime::testing::network_contract_detail::repeated_bytes(
          integrated_payload_bytes, 'b');
        create_environment();
        configure_environment().get();
    }

    ~integrated_network_fixture() {
        if (network_ != nullptr) {
            stop_environment().get();
        }
    }

    seastar::future<std::size_t> execute() {
        if (requires_fresh_environment()) {
            co_await restart_environment();
        }

        writes_.clear();
        perf_tests::start_measuring_time();
        for (auto& client : clients_) {
            writes_.push_back(client.write(source_.share(), write_abort_));
        }
        for (auto& writing : writes_) {
            require(co_await wait_asynchronously(std::move(writing)));
        }
        for (auto& server : servers_) {
            auto received = co_await wait_asynchronously(
              server.read(byte_count{integrated_payload_bytes}, read_abort_));
            if (
              !received || received->eof()
              || received->data().size().value() != integrated_payload_bytes) {
                throw std::runtime_error("network benchmark read mismatch");
            }
        }
        const auto completed = clients_.size();
        writes_.clear();
        perf_tests::stop_measuring_time();
        co_return completed;
    }

private:
    void create_environment() {
        events_ = std::make_unique<scheduler>(benchmark_scheduler_limits());
        auto config = fake_network_config{};
        config.maximum_listeners = integrated_flow_count;
        config.maximum_connection_pairs = integrated_flow_count;
        config.maximum_pending_connects = integrated_flow_count;
        config.maximum_backlog_entries = integrated_flow_count;
        config.maximum_operations = integrated_flow_count * 2U;
        config.maximum_packets = integrated_flow_count * 2U;
        config.maximum_direction_packets = integrated_flow_count * 2U;
        config.maximum_links = integrated_flow_count;
        config.maximum_address_entries = integrated_flow_count + 2U;
        config.maximum_active_flows = integrated_flow_count;
        config.maximum_controls = 4;
        config.latency_min = runtime::monotonic_duration{};
        config.latency_mean_parameter = config.latency_min;
        auto made = fake_network::make(config, *events_);
        if (!made) {
            throw std::runtime_error("network benchmark construction");
        }
        network_ = std::move(*made);
    }

    seastar::future<> configure_environment() {
        if (many_to_one_) {
            co_await add_many_to_one();
            require(
              co_await wait_asynchronously(network_->set_ingress_capacity(
                benchmark_target,
                bandwidth_capacity::finite(
                  integrated_flow_count * integrated_payload_bytes * 1'000U))));
        } else {
            co_await add_one_to_many();
            require(
              co_await wait_asynchronously(network_->set_egress_capacity(
                benchmark_loopback,
                bandwidth_capacity::finite(
                  integrated_flow_count * integrated_payload_bytes * 1'000U))));
        }
    }

    [[nodiscard]] bool requires_fresh_environment() const {
        return events_->executed_events()
               >= events_->limits().total_events()
                    - benchmark_scheduler_restart_reserve;
    }

    seastar::future<> restart_environment() {
        co_await stop_environment();
        create_environment();
        co_await configure_environment();
    }

    seastar::future<> stop_environment() {
        require(co_await wait_asynchronously(network_->stop()));
        servers_.clear();
        clients_.clear();
        listeners_.clear();
        network_.reset();
        events_.reset();
    }

    seastar::future<> add_many_to_one() {
        auto bound = co_await wait_asynchronously(
          network_->listen(runtime::network_endpoint{benchmark_target, 0}, {}));
        if (!bound) {
            throw std::runtime_error("network benchmark listen failed");
        }
        auto listener = std::move(*bound);
        for (std::size_t index = 0; index < integrated_flow_count; ++index) {
            auto accepting = listener.accept(accept_abort_);
            auto connected = co_await wait_asynchronously(network_->connect(
              listener.local_endpoint(),
              runtime::network_endpoint{
                benchmark_address(static_cast<std::uint8_t>(10U + index)), 0},
              runtime::network_connection_limits{},
              connect_abort_));
            auto accepted = co_await wait_asynchronously(std::move(accepting));
            if (!connected || !accepted) {
                throw std::runtime_error("network benchmark connect failed");
            }
            clients_.push_back(std::move(*connected));
            servers_.push_back(std::move(*accepted));
        }
        listeners_.push_back(std::move(listener));
    }

    seastar::future<> add_one_to_many() {
        for (std::size_t index = 0; index < integrated_flow_count; ++index) {
            auto bound = co_await wait_asynchronously(network_->listen(
              runtime::network_endpoint{
                benchmark_address(static_cast<std::uint8_t>(40U + index)), 0},
              {}));
            if (!bound) {
                throw std::runtime_error("network benchmark listen failed");
            }
            auto listener = std::move(*bound);
            auto accepting = listener.accept(accept_abort_);
            auto connected = co_await wait_asynchronously(network_->connect(
              listener.local_endpoint(),
              std::nullopt,
              runtime::network_connection_limits{},
              connect_abort_));
            auto accepted = co_await wait_asynchronously(std::move(accepting));
            if (!connected || !accepted) {
                throw std::runtime_error("network benchmark connect failed");
            }
            listeners_.push_back(std::move(listener));
            clients_.push_back(std::move(*connected));
            servers_.push_back(std::move(*accepted));
        }
    }

    template<typename T>
    seastar::future<T> wait_asynchronously(seastar::future<T> pending) {
        while (!pending.available()) {
            if (events_->pending_events() != 0U) {
                pump_scheduler();
            }
            if (!pending.available()) {
                co_await seastar::yield();
            }
        }
        co_return pending.get();
    }

    void pump_scheduler() {
        if (!events_->has_ready_events()) {
            const auto advanced = events_->advance_to_next();
            if (!advanced || !*advanced) {
                throw std::runtime_error("network benchmark could not advance");
            }
        }
        const auto ran = events_->run_ready();
        if (!ran) {
            throw std::runtime_error(
              "network benchmark pump failed: " + ran.error().render());
        }
    }

    static void require(const runtime::result<void>& result) {
        if (!result) {
            throw std::runtime_error(
              "network benchmark operation failed: " + result.error().render());
        }
    }

    bool many_to_one_;
    std::unique_ptr<scheduler> events_;
    std::unique_ptr<fake_network> network_;
    std::vector<fake_listener> listeners_;
    std::vector<fake_connection> clients_;
    std::vector<fake_connection> servers_;
    std::vector<seastar::future<runtime::result<void>>> writes_;
    bytes::fragmented_buffer source_;
    seastar::abort_source accept_abort_;
    seastar::abort_source connect_abort_;
    seastar::abort_source write_abort_;
    seastar::abort_source read_abort_;
};

struct integrated_many_to_one_fixture : integrated_network_fixture {
    integrated_many_to_one_fixture()
      : integrated_network_fixture(true) {}
};

struct integrated_one_to_many_fixture : integrated_network_fixture {
    integrated_one_to_many_fixture()
      : integrated_network_fixture(false) {}
};

} // namespace

PERF_TEST_F(one_to_many_fixture, fixed_workspace_96_flows) {
    return solve_fixed_workspace();
}

PERF_TEST_F(one_to_many_fixture, independent_oracle_96_flows) {
    return solve_independent_oracle();
}

PERF_TEST_F(many_to_one_fixture, fixed_workspace_96_flows) {
    return solve_fixed_workspace();
}

PERF_TEST_F(many_to_one_fixture, independent_oracle_96_flows) {
    return solve_independent_oracle();
}

PERF_TEST_CN(integrated_many_to_one_fixture, transmit_read_8x4096) {
    return execute();
}

PERF_TEST_CN(integrated_one_to_many_fixture, transmit_read_8x4096) {
    return execute();
}

} // namespace kwaque::simulation
