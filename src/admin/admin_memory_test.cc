#include "src/admin/admin_limits.h"
#include "src/admin/admin_server.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/metrics_api.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/api.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/later.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace {

#if !defined(SEASTAR_DEFAULT_ALLOCATOR)

class shard_pressure final {
public:
    seastar::future<> fill() {
        constexpr std::size_t chunk_size = 64U * 1024U;
        while (seastar::memory::stats().free_memory()
               > kwaque::admin::admin_reservation_bytes) {
            seastar::temporary_buffer<char> chunk{chunk_size};
            std::fill_n(chunk.get_write(), chunk.size(), 'x');
            ballast_.push_back(std::move(chunk));
            co_await seastar::maybe_yield();
        }
        const auto stats = seastar::memory::stats();
        failures_ = stats.failed_allocations();
        fallbacks_ = stats.fallback_allocations();
    }

    seastar::future<> fill_metric_families() {
        namespace metrics = seastar::metrics;
        metrics::impl::snapshot_limits limits;
        const auto initial_families = metrics::impl::get_value_map().size();
        if (initial_families >= limits.families) {
            throw std::runtime_error(
              "no metric family capacity remains for the boundary fixture");
        }
        // Register before sampling, then approach the byte boundary from above.
        // Rejected shapes fail preflight without building intermediate caches
        // whose vector sizes would seed unrelated native allocator free pools.
        for (std::size_t index = initial_families; index < limits.families;
             ++index) {
            boundary_groups_.emplace_back();
            boundary_groups_.back().add_group(
              "admin_boundary_" + seastar::to_sstring(index),
              {metrics::make_gauge("value", [] { return 1; })});
            co_await seastar::maybe_yield();
        }
        bool reached_limit = false;
        while (!boundary_groups_.empty()) {
            try {
                auto snapshot = metrics::impl::get_values(
                  metrics::impl::default_handle(), limits);
                co_await snapshot.destroy();
                break;
            } catch (const std::length_error& error) {
                const std::string_view reason{error.what()};
                if (
                  reason != "metrics metadata limit exceeded"
                  && reason != "metrics snapshot value limit exceeded") {
                    throw;
                }
                boundary_groups_.pop_back();
                reached_limit = true;
            }
            co_await seastar::maybe_yield();
        }
        if (!reached_limit || boundary_groups_.empty()) {
            throw std::runtime_error(
              "metric family byte boundary was not exercised");
        }
        // The first accepted shape was built under ballast. Repeat it under
        // the same cap to verify snapshot release and clean cache reuse.
        auto accepted = metrics::impl::get_values(
          metrics::impl::default_handle(), limits);
        co_await accepted.destroy();
        verify();
    }

    void verify() const {
        const auto stats = seastar::memory::stats();
        if (
          stats.failed_allocations() != failures_
          || stats.fallback_allocations() != fallbacks_) {
            throw std::runtime_error(
              "admin exceeded its native memory reservation");
        }
    }

    seastar::future<> stop() {
        boundary_groups_.clear();
        ballast_.clear();
        return seastar::make_ready_future<>();
    }

private:
    std::deque<seastar::temporary_buffer<char>> ballast_;
    std::deque<seastar::metrics::metric_groups> boundary_groups_;
    std::uint64_t failures_{0};
    std::uint64_t fallbacks_{0};
};

class connection final {
public:
    explicit connection(seastar::socket_address address)
      : socket_(seastar::connect(address).get())
      , input_(socket_.input())
      , output_(socket_.output()) {}

    ~connection() {
        socket_.shutdown_input();
        socket_.shutdown_output();
        try {
            input_.close().get();
        } catch (...) {
        }
        try {
            output_.close().get();
        } catch (...) {
        }
    }

    void send(std::string_view request) {
        output_.write(request.data(), request.size()).get();
        output_.flush().get();
    }

    bool complete_response(bool chunked) {
        std::string prefix;
        std::string tail;
        try {
            while (true) {
                auto buffer = input_.read().get();
                if (buffer.empty()) {
                    break;
                }
                const auto amount = std::min(
                  std::size_t{256} - prefix.size(), buffer.size());
                prefix.append(buffer.get(), amount);
                const auto ending = std::min(std::size_t{16}, buffer.size());
                tail.append(buffer.get() + buffer.size() - ending, ending);
                if (tail.size() > 16) {
                    tail.erase(0, tail.size() - 16);
                }
            }
        } catch (const std::system_error&) {
            return false;
        }
        return prefix.starts_with("HTTP/1.1 200 ")
               && (!chunked || tail.ends_with("0\r\n\r\n"));
    }

private:
    seastar::connected_socket socket_;
    seastar::input_stream<char> input_;
    seastar::output_stream<char> output_;
};

std::uint16_t unused_port() {
    seastar::listen_options options;
    options.reuse_address = false;
    auto probe = seastar::listen(
      seastar::socket_address{seastar::net::inet_address{"127.0.0.1"}, 0},
      options);
    return probe.local_address().port();
}

void check_live(seastar::socket_address address) {
    connection live{address};
    live.send("GET /v1/health/live HTTP/1.1\r\n\r\n");
    if (!live.complete_response(false)) {
        throw std::runtime_error(
          "admin liveness failed under native memory pressure");
    }
}

void exercise(seastar::socket_address address) {
    check_live(address);
    {
        connection scrape{address};
        scrape.send("GET /metrics?__aggregate__=false HTTP/1.1\r\n\r\n");
        if (!scrape.complete_response(true)) {
            throw std::runtime_error(
              "admin scrape failed under native memory pressure");
        }
    }
    {
        std::vector<std::unique_ptr<connection>> scrapes;
        for (std::size_t i = 0; i < kwaque::admin::connections_per_shard
                                      * seastar::this_smp_shard_count();
             ++i) {
            try {
                auto scrape = std::make_unique<connection>(address);
                scrape->send(
                  "GET /metrics?__aggregate__=false HTTP/1.1\r\n\r\n");
                scrapes.push_back(std::move(scrape));
            } catch (const std::system_error&) {
                continue;
            }
        }
        std::size_t completed = 0;
        for (auto& scrape : scrapes) {
            completed += scrape->complete_response(true);
        }
        if (!completed) {
            throw std::runtime_error("all admitted concurrent scrapes failed");
        }
    }
    check_live(address);
}

#endif

TEST(AdminMemoryTest, NativeReservationContainsStartupScrapesAndShutdown) {
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    GTEST_SKIP()
      << "Native allocator pressure requires native allocator statistics";
#else
    // Run alone in a fresh process. Ballast is retained on every source shard,
    // so transient extra page consumption cannot escape through sampling gaps.
    // Preexisting small-pool pages and the test-runner fiber are baseline.
    // Admin fiber stacks created after ballast use the constrained native heap.
    const auto port = unused_port();
    seastar::sharded<shard_pressure> pressure;
    pressure.start().get();
    kwaque::admin::admin_server server;
    std::exception_ptr failure;
    try {
        pressure.invoke_on_all(&shard_pressure::fill).get();
        server.start("127.0.0.1", port, seastar::this_smp_shard_count()).get();
        pressure.invoke_on_all(&shard_pressure::fill_metric_families).get();
        exercise(
          seastar::socket_address{
            seastar::net::inet_address{"127.0.0.1"}, port});
        // Leave partial headers parked while the actual admin owner drains its
        // listener, cross-shard state, and scheduling group under the same cap.
        std::vector<std::unique_ptr<connection>> idle;
        const auto address = seastar::socket_address{
          seastar::net::inet_address{"127.0.0.1"}, port};
        for (std::size_t i = 0; i < kwaque::admin::connections_per_shard
                                      * seastar::this_smp_shard_count();
             ++i) {
            try {
                auto pending = std::make_unique<connection>(address);
                pending->send("GET /metrics HTTP/1.1\r\nHeader: ");
                idle.push_back(std::move(pending));
            } catch (const std::system_error&) {
            }
        }
        server.begin_shutdown().get();
        server.stop().get();
    } catch (...) {
        failure = std::current_exception();
    }
    try {
        server.stop().get();
    } catch (...) {
        if (!failure) {
            failure = std::current_exception();
        }
    }
    try {
        pressure.invoke_on_all(&shard_pressure::verify).get();
    } catch (...) {
        if (!failure) {
            failure = std::current_exception();
        }
    }
    pressure.stop().get();
    if (failure) {
        std::rethrow_exception(failure);
    }
#endif
}

} // namespace
