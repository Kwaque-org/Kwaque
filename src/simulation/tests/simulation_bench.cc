#include "src/bytes/fragmented_buffer.h"
#include "src/runtime/testing/contracts/network_contract.h"
#include "src/simulation/bandwidth.h"
#include "src/simulation/determinism_version.h"
#include "src/simulation/event_trace.h"
#include "src/simulation/fake_network.h"
#include "src/simulation/scheduler.h"
#include "src/simulation/scheduler_driver.h"
#include "src/simulation/tests/network_oracle.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/later.hh>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace kwaque::simulation {

namespace {

constexpr std::uint32_t benchmark_flow_count = 96;
constexpr std::uint64_t benchmark_capacity = 96'000;
constexpr std::uint32_t integrated_flow_count = 8;
constexpr std::uint64_t integrated_payload_bytes = 4'096;
constexpr std::uint64_t benchmark_scheduler_restart_reserve = 65'536;
constexpr std::uint64_t benchmark_batch = 64;

struct callback_measurements {
    std::uint64_t calls{0};
    std::uint64_t worst_nanoseconds{0};

    template<typename Function>
    auto measure(Function&& function) {
        // Includes scheduler dispatch, or owning write submission when the
        // rebalance is synchronous. This is an upper bound on callback work.
        const auto started = std::chrono::steady_clock::now();
        auto result = std::forward<Function>(function)();
        const auto elapsed
          = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - started);
        worst_nanoseconds = std::max(
          worst_nanoseconds, static_cast<std::uint64_t>(elapsed.count()));
        ++calls;
        return result;
    }

    void report(const char* operation, std::uint32_t flows) const noexcept {
        if (calls == 0) {
            return;
        }
        std::fprintf(
          stderr,
          "simulation_callbacks operation=%s flows=%u calls=%llu "
          "worst_dispatch_ns=%llu\n",
          operation,
          flows,
          static_cast<unsigned long long>(calls),
          static_cast<unsigned long long>(worst_nanoseconds));
    }
};

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

class scheduler_fixture {
public:
    std::size_t enqueue() {
        prepare();
        perf_tests::start_measuring_time();
        enqueue_batch();
        if (events_->pending_events() != benchmark_batch) {
            throw std::runtime_error("scheduler benchmark admission count");
        }
        perf_tests::stop_measuring_time();
        step_batch();
        return benchmark_batch;
    }

    std::size_t step() {
        prepare();
        enqueue_batch();
        perf_tests::start_measuring_time();
        step_batch();
        perf_tests::stop_measuring_time();
        return benchmark_batch;
    }

private:
    void prepare() {
        if (
          !events_
          || events_->executed_events() + benchmark_batch
               > events_->limits().total_events()) {
            events_ = std::make_unique<scheduler>(benchmark_scheduler_limits());
        }
        completed_ = 0;
        checksum_ = 0;
    }

    void enqueue_batch() {
        for (std::uint64_t index = 0; index < benchmark_batch; ++index) {
            auto submitted = events_->schedule(
              events_->now(), event_priority::normal(), [this, index] noexcept {
                  ++completed_;
                  checksum_ += index;
              });
            if (!submitted) {
                throw std::runtime_error("scheduler benchmark enqueue");
            }
        }
    }

    void step_batch() {
        for (std::uint64_t index = 0; index < benchmark_batch; ++index) {
            const auto selected = events_->step();
            if (!selected || !*selected) {
                throw std::runtime_error("scheduler benchmark step");
            }
        }
        if (
          completed_ != benchmark_batch
          || checksum_ != benchmark_batch * (benchmark_batch - 1U) / 2U
          || events_->pending_events() != 0) {
            throw std::runtime_error("scheduler benchmark completion count");
        }
    }

    std::unique_ptr<scheduler> events_;
    std::uint64_t completed_{0};
    std::uint64_t checksum_{0};
};

class trace_append_fixture {
public:
    std::size_t append() {
        const auto configured = trace_limits::make(
          trace_limit_values{
            .entries = static_cast<std::uint32_t>(benchmark_batch),
            .encoded_bytes = canonical_header_encoded_size
                             + benchmark_batch * canonical_entry_encoded_size,
            .line_bytes = 1'024,
          });
        if (!configured) {
            throw std::runtime_error("trace benchmark limits");
        }
        event_trace trace{
          trace_header::current(
            1,
            deterministic_random_algorithm_version,
            deterministic_random_coordinate_version,
            trace_budget(benchmark_scheduler_limits()),
            *configured,
            {},
            {}),
          *configured};
        perf_tests::start_measuring_time();
        for (std::uint64_t index = 0; index < benchmark_batch; ++index) {
            const auto appended = trace.observe(
              trace_entry{
                .time = runtime::monotonic_time{index},
                .action = trace_action::time_advanced,
                .kind = trace_event_kind::generic,
                .coordinate_a = index,
                .value = index + 1U,
              });
            if (!appended) {
                throw std::runtime_error("trace benchmark append");
            }
        }
        if (
          trace.entries().size() != benchmark_batch
          || trace.entries()[benchmark_batch - 1U].sequence
               != benchmark_batch) {
            throw std::runtime_error("trace benchmark sequence");
        }
        perf_tests::stop_measuring_time();
        return benchmark_batch;
    }
};

struct bandwidth_fixture {
    std::unique_ptr<bandwidth_planner> planner;
    std::vector<bandwidth_flow> actual;
    std::vector<testing::oracle_flow> expected;
    std::uint32_t flow_count;
    const char* topology;
    std::uint64_t add_flow_calls{0};
    std::uint64_t solve_calls{0};
    std::uint64_t oracle_solve_calls{0};

    explicit bandwidth_fixture(
      bool one_to_many, std::uint32_t flows = benchmark_flow_count)
      : flow_count(flows)
      , topology(one_to_many ? "one_to_many" : "many_to_one") {
        auto made = bandwidth_planner::make(flow_count);
        if (!made) {
            throw std::runtime_error(
              "bandwidth benchmark planner construction");
        }
        planner = std::move(*made);
        actual.reserve(flow_count);
        expected.reserve(flow_count);
        for (std::uint64_t index = 0; index < flow_count; ++index) {
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

    ~bandwidth_fixture() {
        std::fprintf(
          stderr,
          "simulation_planner_calls topology=%s flows=%u add_flow=%llu "
          "solve=%llu oracle_solve=%llu\n",
          topology,
          flow_count,
          static_cast<unsigned long long>(add_flow_calls),
          static_cast<unsigned long long>(solve_calls),
          static_cast<unsigned long long>(oracle_solve_calls));
    }

    std::size_t solve_fixed_workspace() {
        planner->reset();
        perf_tests::start_measuring_time();
        for (const auto& flow : actual) {
            if (!planner->add_flow(flow)) {
                throw std::runtime_error("bandwidth benchmark flow admission");
            }
        }
        if (!planner->solve()) {
            throw std::runtime_error("bandwidth benchmark solve");
        }
        perf_tests::do_not_optimize(planner->allocation_digest().words);
        const auto count = planner->allocation_count();
        if (count != flow_count) {
            throw std::runtime_error("bandwidth benchmark allocation count");
        }
        for (std::size_t index = 0; index < count; ++index) {
            const auto& allocation = planner->allocation_at(index);
            if (
              allocation.flow != index + 1U || allocation.rate.is_unlimited()
              || !allocation.rate.finite_value().equals(
                benchmark_capacity, flow_count)) {
                throw std::runtime_error("bandwidth benchmark fixed rate");
            }
        }
        perf_tests::stop_measuring_time();
        add_flow_calls += count;
        ++solve_calls;
        return count;
    }

    std::size_t solve_independent_oracle() {
        perf_tests::start_measuring_time();
        auto solution = testing::solve_bandwidth_oracle(expected);
        if (!solution) {
            throw std::runtime_error("bandwidth benchmark oracle solve");
        }
        perf_tests::do_not_optimize(solution->digest.words);
        const auto count = solution->allocations.size();
        if (count != flow_count) {
            throw std::runtime_error("bandwidth benchmark oracle count");
        }
        for (std::size_t index = 0; index < count; ++index) {
            const auto& allocation = solution->allocations[index];
            if (
              allocation.flow != index + 1U || allocation.rate.unlimited
              || !allocation.rate.finite.equals(
                benchmark_capacity, flow_count)) {
                throw std::runtime_error("bandwidth benchmark oracle rate");
            }
        }
        perf_tests::stop_measuring_time();
        ++oracle_solve_calls;
        return count;
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

template<std::uint32_t Flows>
struct bounded_bandwidth_fixture : bandwidth_fixture {
    static_assert(Flows > 0 && Flows <= maximum_bandwidth_flows);
    bounded_bandwidth_fixture()
      : bandwidth_fixture(true, Flows) {}
};

using bandwidth_1_fixture = bounded_bandwidth_fixture<1>;
using bandwidth_8_fixture = bounded_bandwidth_fixture<8>;
using bandwidth_32_fixture = bounded_bandwidth_fixture<32>;

class integrated_network_fixture {
public:
    explicit integrated_network_fixture(
      bool many_to_one, std::uint32_t flows = integrated_flow_count)
      : many_to_one_(many_to_one)
      , flow_count_(flows)
      , expected_contents_(integrated_payload_bytes, 'b') {
        listeners_.reserve(flow_count_);
        clients_.reserve(flow_count_);
        servers_.reserve(flow_count_);
        writes_.reserve(flow_count_);
        prepared_payloads_.reserve(flow_count_);
        source_ = runtime::testing::network_contract_detail::repeated_bytes(
          integrated_payload_bytes, 'b');
        try {
            create_environment();
            configure_environment().get();
        } catch (...) {
            const auto failure = std::current_exception();
            stop_environment().get();
            std::rethrow_exception(failure);
        }
    }

    ~integrated_network_fixture() {
        if (network_ != nullptr) {
            stop_environment().get();
        }
        measurements_.report(measurement_label_, flow_count_);
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
              || received->data().size().value() != integrated_payload_bytes
              || !received->data().content_equals(expected_contents_)) {
                throw std::runtime_error("network benchmark read mismatch");
            }
        }
        const auto completed = clients_.size();
        writes_.clear();
        perf_tests::stop_measuring_time();
        co_return completed;
    }

    seastar::future<std::size_t> start_rebalance() {
        measurement_label_ = "active_flow_start";
        co_await prepare_transition();
        for (std::size_t index = 0; index + 1U < flow_count_; ++index) {
            submit(index);
            if ((index + 1U) % benchmark_batch == 0) {
                co_await seastar::yield();
            }
        }
        advance_partial();
        perf_tests::start_measuring_time();
        writes_.push_back(measurements_.measure([this] {
            return clients_.back().write(
              std::move(prepared_payloads_.back()), write_abort_);
        }));
        if (
          writes_.back().available()
          || network_->active_operations() != flow_count_) {
            throw std::runtime_error("network benchmark start was not active");
        }
        perf_tests::stop_measuring_time();
        co_await finish_transition();
        co_return 1U;
    }

    seastar::future<std::size_t> finish_rebalance() {
        measurement_label_ = "active_flow_finish";
        co_await prepare_transition();
        for (std::size_t index = 0; index < flow_count_; ++index) {
            submit(index);
            if ((index + 1U) % benchmark_batch == 0) {
                co_await seastar::yield();
            }
        }
        const auto advanced = events_->advance_to_next();
        if (!advanced || !*advanced) {
            throw std::runtime_error("network benchmark finish deadline");
        }
        perf_tests::start_measuring_time();
        const auto selected = measurements_.measure(
          [this] { return events_->step(); });
        if (
          !selected || !*selected
          || network_->active_operations() != flow_count_ - 1U) {
            throw std::runtime_error("network benchmark finish dispatch");
        }
        perf_tests::stop_measuring_time();
        co_await finish_transition();
        co_return 1U;
    }

    seastar::future<std::size_t> capacity_rebalance(bool from_zero = false) {
        measurement_label_ = from_zero ? "zero_rate_resume"
                                       : "active_capacity_change";
        co_await prepare_transition(from_zero);
        for (std::size_t index = 0; index < flow_count_; ++index) {
            submit(index);
            if ((index + 1U) % benchmark_batch == 0) {
                co_await seastar::yield();
            }
        }
        if (from_zero) {
            if (events_->pending_events() != 0) {
                throw std::runtime_error(
                  "network benchmark zero rate scheduled progress");
            }
        } else {
            advance_partial();
        }
        perf_tests::start_measuring_time();
        measure_dispatch_ = true;
        require(
          co_await wait_asynchronously(network_->set_egress_capacity(
            benchmark_loopback,
            bandwidth_capacity::finite(shared_capacity() * 2U))));
        if (network_->active_operations() != flow_count_) {
            throw std::runtime_error(
              "network benchmark capacity lost active flow");
        }
        measure_dispatch_ = false;
        perf_tests::stop_measuring_time();
        co_await finish_transition();
        co_return 1U;
    }

private:
    [[nodiscard]] std::uint64_t shared_capacity() const noexcept {
        return flow_count_ * integrated_payload_bytes * 1'000U;
    }

    seastar::future<> prepare_transition(bool zero_capacity = false) {
        if (requires_fresh_environment()) {
            co_await restart_environment();
        }
        writes_.clear();
        prepared_payloads_.clear();
        require(
          co_await wait_asynchronously(network_->set_egress_capacity(
            benchmark_loopback,
            bandwidth_capacity::finite(
              zero_capacity ? 0 : shared_capacity()))));
        for (std::size_t index = 0; index < flow_count_; ++index) {
            auto payload = source_.share();
            if (index + 1U == flow_count_) {
                const auto trimmed = payload.trim_back(
                  byte_count{integrated_payload_bytes - 1U});
                if (!trimmed) {
                    throw std::system_error(
                      trimmed.error(), "network benchmark payload preparation");
                }
            }
            prepared_payloads_.push_back(std::move(payload));
            if ((index + 1U) % benchmark_batch == 0) {
                co_await seastar::yield();
            }
        }
    }

    void submit(std::size_t index) {
        writes_.push_back(
          clients_[index].write(
            std::move(prepared_payloads_[index]), write_abort_));
        if (
          writes_.back().available()
          || network_->active_operations() != writes_.size()) {
            throw std::runtime_error("network benchmark flow admission");
        }
    }

    void advance_partial() {
        const auto target = events_->now().checked_add(
          runtime::monotonic_duration{1});
        if (!target) {
            throw std::runtime_error("network benchmark partial deadline");
        }
        const auto advanced = events_->run_until_batch(
          *target, benchmark_batch);
        if (!advanced || *advanced != 0) {
            throw std::runtime_error(
              "network benchmark unexpected early completion");
        }
    }

    seastar::future<> finish_transition() {
        for (auto& writing : writes_) {
            require(co_await wait_asynchronously(std::move(writing)));
        }
        for (std::size_t index = 0; index < servers_.size(); ++index) {
            const auto expected = index + 1U == flow_count_
                                    ? std::uint64_t{1}
                                    : integrated_payload_bytes;
            std::uint64_t consumed = 0;
            while (consumed != expected) {
                auto received = co_await wait_asynchronously(
                  servers_[index].read(
                    byte_count{expected - consumed}, read_abort_));
                if (
                  !received || received->eof() || received->data().empty()
                  || received->data().size().value() > expected - consumed
                  || !received->data().content_equals(
                    std::string_view{expected_contents_}.substr(
                      0, received->data().size().value()))) {
                    throw std::runtime_error(
                      "network benchmark transition payload");
                }
                consumed += received->data().size().value();
            }
            if ((index + 1U) % benchmark_batch == 0) {
                co_await seastar::yield();
            }
        }
        while (events_->pending_events() != 0) {
            pump_scheduler();
            co_await seastar::yield();
        }
        if (network_->active_operations() != 0) {
            throw std::runtime_error(
              "network benchmark transition retained work");
        }
        writes_.clear();
        prepared_payloads_.clear();
    }

    void create_environment() {
        events_ = std::make_unique<scheduler>(benchmark_scheduler_limits());
        auto config = fake_network_config{};
        config.maximum_listeners = flow_count_;
        config.maximum_connection_pairs = flow_count_;
        config.maximum_pending_connects = flow_count_;
        config.maximum_backlog_entries = flow_count_;
        config.maximum_operations = flow_count_ * 2U;
        config.maximum_packets = flow_count_ * 2U;
        config.maximum_direction_packets = flow_count_ * 2U;
        config.maximum_links = flow_count_;
        config.maximum_address_entries = flow_count_ + 2U;
        config.maximum_active_flows = flow_count_;
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
                  flow_count_ * integrated_payload_bytes * 1'000U))));
        } else {
            co_await add_one_to_many();
            require(
              co_await wait_asynchronously(network_->set_egress_capacity(
                benchmark_loopback,
                bandwidth_capacity::finite(
                  flow_count_ * integrated_payload_bytes * 1'000U))));
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
        measure_dispatch_ = false;
        if (network_) {
            require(co_await wait_asynchronously(network_->stop()));
        }
        if (pending_accept_) {
            co_await testing::pump_until(*events_, *pending_accept_);
            // A successful accept may hold a now-closed connection. Consume
            // the future without moving that handle after owner shutdown.
            pending_accept_->ignore_ready_future();
        }
        pending_accept_.reset();
        for (auto& writing : writes_) {
            if (writing.available()) {
                writing.ignore_ready_future();
            }
        }
        servers_.clear();
        clients_.clear();
        listeners_.clear();
        writes_.clear();
        prepared_payloads_.clear();
        network_.reset();
        events_.reset();
    }

    seastar::future<> add_many_to_one() {
        auto bound = co_await wait_asynchronously(
          network_->listen(runtime::network_endpoint{benchmark_target, 0}, {}));
        if (!bound) {
            throw std::runtime_error("network benchmark listen failed");
        }
        listeners_.push_back(std::move(*bound));
        for (std::size_t index = 0; index < flow_count_; ++index) {
            pending_accept_.emplace(listeners_.back().accept(accept_abort_));
            auto connected = co_await wait_asynchronously(network_->connect(
              listeners_.back().local_endpoint(),
              runtime::network_endpoint{
                benchmark_address(static_cast<std::uint8_t>(10U + index)), 0},
              runtime::network_connection_limits{},
              connect_abort_));
            if (!connected) {
                throw std::runtime_error("network benchmark connect failed");
            }
            clients_.push_back(std::move(*connected));
            co_await testing::pump_until(*events_, *pending_accept_);
            auto accepted = pending_accept_->get();
            pending_accept_.reset();
            if (!accepted) {
                throw std::runtime_error("network benchmark accept failed");
            }
            servers_.push_back(std::move(*accepted));
        }
    }

    seastar::future<> add_one_to_many() {
        for (std::size_t index = 0; index < flow_count_; ++index) {
            auto bound = co_await wait_asynchronously(network_->listen(
              runtime::network_endpoint{
                benchmark_address(static_cast<std::uint8_t>(40U + index)), 0},
              {}));
            if (!bound) {
                throw std::runtime_error("network benchmark listen failed");
            }
            listeners_.push_back(std::move(*bound));
            pending_accept_.emplace(listeners_.back().accept(accept_abort_));
            auto connected = co_await wait_asynchronously(network_->connect(
              listeners_.back().local_endpoint(),
              std::nullopt,
              runtime::network_connection_limits{},
              connect_abort_));
            if (!connected) {
                throw std::runtime_error("network benchmark connect failed");
            }
            clients_.push_back(std::move(*connected));
            co_await testing::pump_until(*events_, *pending_accept_);
            auto accepted = pending_accept_->get();
            pending_accept_.reset();
            if (!accepted) {
                throw std::runtime_error("network benchmark accept failed");
            }
            servers_.push_back(std::move(*accepted));
        }
    }

    template<typename T>
    seastar::future<T> wait_asynchronously(seastar::future<T> pending) {
        if (!measure_dispatch_) {
            co_await testing::pump_until(*events_, pending);
            co_return co_await std::move(pending);
        }
        constexpr auto watchdog = std::chrono::seconds{10};
        const auto deadline = seastar::lowres_clock::now() + watchdog;
        while (!pending.available()) {
            co_await seastar::yield();
            if (pending.available()) {
                break;
            }
            if (seastar::lowres_clock::now() >= deadline) {
                throw testing::scheduler_watchdog_error{};
            }
            if (events_->pending_events() == 0U) {
                continue;
            }
            pump_scheduler();
        }
        co_return co_await std::move(pending);
    }

    void pump_scheduler() {
        if (!events_->has_ready_events()) {
            const auto advanced = events_->advance_to_next();
            if (!advanced || !*advanced) {
                throw std::runtime_error("network benchmark could not advance");
            }
        }
        if (measure_dispatch_) {
            const auto selected = measurements_.measure(
              [this] { return events_->step(); });
            if (!selected || !*selected) {
                throw std::runtime_error("network benchmark measured dispatch");
            }
            return;
        }
        const auto ran = events_->run_ready_batch(benchmark_batch);
        if (!ran || *ran == 0) {
            throw std::runtime_error("network benchmark pump failed");
        }
    }

    static void require(const runtime::result<void>& result) {
        if (!result) {
            throw std::runtime_error(
              "network benchmark operation failed: " + result.error().render());
        }
    }

    bool many_to_one_;
    std::uint32_t flow_count_;
    std::string expected_contents_;
    std::unique_ptr<scheduler> events_;
    std::unique_ptr<fake_network> network_;
    std::vector<fake_listener> listeners_;
    std::vector<fake_connection> clients_;
    std::vector<fake_connection> servers_;
    std::vector<seastar::future<runtime::result<void>>> writes_;
    std::optional<seastar::future<runtime::result<fake_connection>>>
      pending_accept_;
    std::vector<bytes::fragmented_buffer> prepared_payloads_;
    bytes::fragmented_buffer source_;
    callback_measurements measurements_;
    const char* measurement_label_{"transmit_read"};
    bool measure_dispatch_{false};
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

template<std::uint32_t Flows>
struct integrated_rebalance_fixture : integrated_network_fixture {
    static_assert(Flows > 0 && Flows <= maximum_bandwidth_flows);

    integrated_rebalance_fixture()
      : integrated_network_fixture(false, Flows) {}
};

using rebalance_1_fixture = integrated_rebalance_fixture<1>;
using rebalance_8_fixture = integrated_rebalance_fixture<8>;
using rebalance_32_fixture = integrated_rebalance_fixture<32>;
using rebalance_96_fixture = integrated_rebalance_fixture<96>;

} // namespace

PERF_TEST_F(scheduler_fixture, enqueue_64) { return enqueue(); }

PERF_TEST_F(scheduler_fixture, step_64) { return step(); }

PERF_TEST_F(trace_append_fixture, append_64) { return append(); }

PERF_TEST_F(bandwidth_1_fixture, fixed_workspace) {
    return solve_fixed_workspace();
}

PERF_TEST_F(bandwidth_1_fixture, independent_oracle) {
    return solve_independent_oracle();
}

PERF_TEST_F(bandwidth_8_fixture, fixed_workspace) {
    return solve_fixed_workspace();
}

PERF_TEST_F(bandwidth_8_fixture, independent_oracle) {
    return solve_independent_oracle();
}

PERF_TEST_F(bandwidth_32_fixture, fixed_workspace) {
    return solve_fixed_workspace();
}

PERF_TEST_F(bandwidth_32_fixture, independent_oracle) {
    return solve_independent_oracle();
}

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

PERF_TEST_CN(rebalance_1_fixture, active_flow_start) {
    return start_rebalance();
}

PERF_TEST_CN(rebalance_1_fixture, active_flow_finish) {
    return finish_rebalance();
}

PERF_TEST_CN(rebalance_1_fixture, active_capacity_change) {
    return capacity_rebalance();
}

PERF_TEST_CN(rebalance_1_fixture, zero_rate_resume) {
    return capacity_rebalance(true);
}

PERF_TEST_CN(rebalance_8_fixture, active_flow_start) {
    return start_rebalance();
}

PERF_TEST_CN(rebalance_8_fixture, active_flow_finish) {
    return finish_rebalance();
}

PERF_TEST_CN(rebalance_8_fixture, active_capacity_change) {
    return capacity_rebalance();
}

PERF_TEST_CN(rebalance_8_fixture, zero_rate_resume) {
    return capacity_rebalance(true);
}

PERF_TEST_CN(rebalance_32_fixture, active_flow_start) {
    return start_rebalance();
}

PERF_TEST_CN(rebalance_32_fixture, active_flow_finish) {
    return finish_rebalance();
}

PERF_TEST_CN(rebalance_32_fixture, active_capacity_change) {
    return capacity_rebalance();
}

PERF_TEST_CN(rebalance_32_fixture, zero_rate_resume) {
    return capacity_rebalance(true);
}

PERF_TEST_CN(rebalance_96_fixture, active_flow_start) {
    return start_rebalance();
}

PERF_TEST_CN(rebalance_96_fixture, active_flow_finish) {
    return finish_rebalance();
}

PERF_TEST_CN(rebalance_96_fixture, active_capacity_change) {
    return capacity_rebalance();
}

PERF_TEST_CN(rebalance_96_fixture, zero_rate_resume) {
    return capacity_rebalance(true);
}

} // namespace kwaque::simulation
