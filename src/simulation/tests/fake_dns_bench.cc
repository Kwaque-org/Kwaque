#include "src/runtime/dns.h"
#include "src/simulation/fake_dns.h"
#include "src/simulation/scheduler_driver.h"

#include <seastar/testing/perf_tests.hh>

#include <cstddef>
#include <memory>
#include <stdexcept>
#include <utility>

namespace kwaque::simulation {
namespace {

class dns_fixture {
public:
    dns_fixture() { create(); }
    ~dns_fixture() { stop().get(); }

    seastar::future<std::size_t> resolve(bool numeric) {
        co_await refresh();
        auto query = make_query(numeric);
        perf_tests::start_measuring_time();
        auto pending = dns_->resolve(std::move(query), abort_);
        co_await testing::pump_deterministic_until(*events_, pending);
        const auto answer = co_await std::move(pending);
        perf_tests::stop_measuring_time();
        require_answer(answer, numeric);
        co_return 1U;
    }

    seastar::future<std::size_t> reject_full_queue() {
        co_await refresh();
        auto active = dns_->resolve(make_query(false), abort_);
        auto queued = dns_->resolve(make_query(false), abort_);
        if (
          !dns_->active() || dns_->pending_queries() != 2
          || dns_->waiting_queries() != 1 || active.available()
          || queued.available()) {
            throw std::runtime_error("DNS benchmark queue setup");
        }
        auto query = make_query(false);
        perf_tests::start_measuring_time();
        auto third = dns_->resolve(std::move(query), abort_);
        const auto ready = third.available();
        const auto result = co_await std::move(third);
        perf_tests::stop_measuring_time();
        if (!ready || result || result.error().code() != errc::queue_full) {
            throw std::runtime_error("DNS benchmark exceeded query admission");
        }
        co_await stop_owner();
        if (!active.available() || !queued.available()) {
            throw std::runtime_error("DNS benchmark stop left a pending query");
        }
        const auto answer = co_await std::move(active);
        const auto canceled = co_await std::move(queued);
        require_answer(answer, false);
        if (canceled || canceled.error().code() != errc::aborted) {
            throw std::runtime_error("DNS benchmark queued terminal");
        }
        dns_.reset();
        events_.reset();
        co_return 1U;
    }

private:
    static void require_answer(
      const runtime::result<runtime::dns_result>& answer, bool numeric) {
        if (
          !answer || answer->answers().size() != 1
          || answer->answers()[0].endpoint.port() != 9'988
          || answer->answers()[0].endpoint.address() != address()
          || (!numeric && answer->answers()[0].ttl != runtime::monotonic_duration{30})) {
            throw std::runtime_error("DNS benchmark result mismatch");
        }
    }

    static runtime::network_address address() noexcept {
        return runtime::network_address::ipv4(
          {std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}});
    }
    static runtime::dns_query make_query(bool numeric) {
        auto name = runtime::dns_name::make(
          numeric ? "127.0.0.1" : "benchmark.test");
        if (!name) {
            throw std::runtime_error("DNS benchmark query");
        }
        return runtime::dns_query{
          .host = std::move(*name),
          .port = 9'988,
          .family = runtime::dns_address_family::ipv4};
    }
    void create() {
        auto limits = scheduler_limits::make(
          {.pending_events = 128,
           .events_per_pump = 64,
           .total_events = 100'000,
           .maximum_deadline = runtime::monotonic_time{1'000'000'000}});
        if (!limits) {
            throw std::runtime_error("DNS benchmark scheduler");
        }
        events_ = std::make_unique<scheduler>(*limits);
        fake_dns_config config;
        config.query_limits.maximum_waiters = 1;
        config.maximum_records = 1;
        config.maximum_answers = 1;
        config.maximum_name_bytes = byte_count{256};
        auto made = fake_dns::make(config, *events_);
        if (!made) {
            throw std::runtime_error("DNS benchmark owner");
        }
        dns_ = std::move(*made);
        auto added = dns_->add_record(
          fake_dns_record{
            .key = make_query(false),
            .answers = {runtime::dns_answer{
              .endpoint = runtime::network_endpoint{address(), 9'988},
              .ttl = runtime::monotonic_duration{30}}},
            .latency = runtime::monotonic_duration{1}});
        if (!added) {
            throw std::runtime_error("DNS benchmark record");
        }
    }
    seastar::future<> refresh() {
        if (events_ && events_->executed_events() >= 99'000) {
            co_await stop();
        }
        if (!dns_) {
            create();
        }
    }
    seastar::future<> stop_owner() {
        if (!dns_) {
            co_return;
        }
        auto stopping = dns_->stop();
        co_await testing::pump_deterministic_until(*events_, stopping);
        const auto stopped = co_await std::move(stopping);
        if (
          !stopped || events_->pending_events() != 0
          || dns_->pending_queries() != 0 || dns_->record_count() != 0) {
            throw std::runtime_error("DNS benchmark retained work");
        }
    }
    seastar::future<> stop() {
        co_await stop_owner();
        dns_.reset();
        events_.reset();
    }
    std::unique_ptr<scheduler> events_;
    std::unique_ptr<fake_dns> dns_;
    seastar::abort_source abort_;
};

} // namespace

PERF_TEST_CN(dns_fixture, named_lookup) { return resolve(false); }
PERF_TEST_CN(dns_fixture, numeric_bypass) { return resolve(true); }
PERF_TEST_CN(dns_fixture, queue_pressure) { return reject_full_queue(); }

} // namespace kwaque::simulation
