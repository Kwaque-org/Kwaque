#include "src/admin/admin_limits.h"
#include "src/admin/admin_server.h"
#include "src/admin/admin_server_test_support.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/metrics_api.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/preempt.hh>
#include <seastar/core/prometheus.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/net/api.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/later.hh>

#include <gtest/gtest.h>
#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

struct prometheus_test_fixture {
    static seastar::future<> write(
      seastar::prometheus::config config,
      seastar::prometheus::details::write_body_args args,
      seastar::output_stream<char> output) {
        return seastar::prometheus::details::test_access{}.write_body(
          std::move(config), std::move(args), std::move(output));
    }
};

namespace {

using namespace std::chrono_literals;
namespace http = seastar::http;
namespace httpd = seastar::httpd;
namespace metrics = seastar::metrics;

class client final {
public:
    explicit client(seastar::socket_address address)
      : socket(seastar::connect(address).get())
      , input(socket.input())
      , output(socket.output()) {}

    ~client() {
        socket.shutdown_input();
        socket.shutdown_output();
        try {
            input.close().get();
        } catch (...) {
        }
        try {
            output.close().get();
        } catch (...) {
        }
    }

    void send(std::string_view bytes) {
        output.write(bytes.data(), bytes.size()).get();
        output.flush().get();
    }

    std::string response() {
        std::string result;
        while (true) {
            seastar::temporary_buffer<char> next;
            try {
                next = input.read().get();
            } catch (const std::system_error&) {
                if (!result.empty()) {
                    return result;
                }
                throw;
            }
            if (next.empty()) {
                return result;
            }
            if (result.size() + next.size() > 64U * 1024U) {
                throw std::length_error(
                  "test response exceeded expected bound");
            }
            result.append(next.get(), next.size());
        }
    }

    seastar::connected_socket socket;
    seastar::input_stream<char> input;
    seastar::output_stream<char> output;
};

class server_fixture final {
public:
    explicit server_fixture(httpd::request_limits limits = {}) {
        server.set_request_limits(limits);
        auto handler = std::make_unique<httpd::function_handler>(
          [this](httpd::const_req, http::reply&) {
              ++handled;
              return seastar::sstring{"ready"};
          },
          "txt");
        server._routes.put(httpd::GET, "/", handler.get());
        static_cast<void>(handler.release());
        // This fixture owns one HTTP server; other shards only supply metrics.
        seastar::listen_options options;
        options.reuse_address = true;
        options.set_fixed_cpu(seastar::this_shard_id());
        server
          .listen(
            seastar::socket_address{seastar::net::inet_address{"127.0.0.1"}, 0},
            options)
          .get();
        address = httpd::http_server_tester::listeners(server)
                    .front()
                    .local_address();
    }

    ~server_fixture() { stop(); }

    void stop() {
        if (!stopped) {
            server.stop().get();
            stopped = true;
        }
    }

    httpd::http_server server{"admin-resource-test"};
    seastar::socket_address address;
    unsigned handled{0};
    bool stopped{false};
};

void wait_for_connections(httpd::http_server& server, std::uint64_t count) {
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (server.current_connections() != count
           && std::chrono::steady_clock::now() < deadline) {
        seastar::sleep(1ms).get();
    }
    ASSERT_EQ(server.current_connections(), count);
}

std::size_t admin_group_metrics() {
    std::size_t count = 0;
    for (const auto& [name, family] : metrics::impl::get_value_map()) {
        for (const auto& [labels, metric] : family) {
            const auto group = labels.labels().find("group");
            if (
              group != labels.labels().end()
              && group->second.value() == "admin") {
                ++count;
            }
        }
    }
    return count;
}

httpd::request_limits short_deadlines() {
    httpd::request_limits limits;
    limits.header_timeout = 100ms;
    limits.exchange_timeout = 250ms;
    return limits;
}

seastar::future<> write_until_disconnected(
  seastar::output_stream<char> output, std::size_t& written) {
    const std::array<char, 8192> chunk{};
    std::exception_ptr failure;
    try {
        for (std::size_t index = 0; index < 8192; ++index) {
            co_await output.write(chunk.data(), chunk.size());
            written += chunk.size();
        }
    } catch (...) {
        failure = std::current_exception();
    }
    try {
        co_await output.close();
    } catch (...) {
        if (!failure) {
            failure = std::current_exception();
        }
    }
    if (failure) {
        std::rethrow_exception(failure);
    }
}

TEST(
  AdminResourceTest,
  RejectsOversizedLineHeadersAndRepeatedFieldsBeforeRouting) {
    server_fixture fixture;
    const std::array requests{
      "GET /" + std::string(kwaque::admin::request_line_bytes, 'x')
        + " HTTP/1.1\r\n\r\n",
      "GET / HTTP/1.1\r\nHeader: "
        + std::string(kwaque::admin::header_bytes, 'x') + "\r\n\r\n",
      [] {
          std::string request = "GET / HTTP/1.1\r\n";
          for (std::size_t i = 0; i <= kwaque::admin::header_count; ++i) {
              request += "Repeated: x\r\n";
          }
          return request + "\r\n";
      }(),
    };
    for (const auto& request : requests) {
        client connection(fixture.address);
        connection.send(request);
        EXPECT_NE(
          connection.response().find("400 Bad Request"), std::string::npos);
    }
    EXPECT_EQ(fixture.handled, 0U);
}

TEST(AdminResourceTest, RejectsBodyDeclarationsWithoutWaitingForTheirBodies) {
    server_fixture fixture;
    const std::array headers{
      "Content-Length: 1\r\n",
      "Content-Length: 0\r\nContent-Length: 0\r\n",
      "Content-Length: invalid\r\n",
      "Transfer-Encoding: chunked\r\n",
      "Transfer-Encoding: \r\n",
      "Content-Length: 99\r\nExpect: 100-continue\r\n",
    };
    for (const auto* header : headers) {
        client connection(fixture.address);
        connection.send("GET / HTTP/1.1\r\n" + std::string(header) + "\r\n");
        const auto response = connection.response();
        EXPECT_NE(response.find("413 Payload Too Large"), std::string::npos);
        EXPECT_EQ(response.find("100 Continue"), std::string::npos);
    }
    EXPECT_EQ(fixture.handled, 0U);
    for (const auto* length : {"0", "0000", " \t0000 \t"}) {
        client allowed(fixture.address);
        allowed.send(
          "GET / HTTP/1.1\r\nContent-Length: " + std::string(length)
          + "\r\n\r\n");
        EXPECT_NE(allowed.response().find("200 OK"), std::string::npos);
    }
    EXPECT_EQ(fixture.handled, 3U);
}

TEST(
  AdminResourceTest,
  ConnectionCapacityPrecedesProtocolConstructionAndShutdownReleasesIt) {
    server_fixture fixture;
    std::vector<std::unique_ptr<client>> connections;
    for (std::size_t index = 0; index < kwaque::admin::connections_per_shard;
         ++index) {
        connections.push_back(std::make_unique<client>(fixture.address));
    }
    wait_for_connections(fixture.server, kwaque::admin::connections_per_shard);
    const auto admitted = fixture.server.total_connections();
    try {
        client rejected(fixture.address);
        EXPECT_TRUE(rejected.response().empty());
    } catch (const std::system_error&) {
        // A reset or EOF both close an excess socket before protocol dispatch.
    }
    EXPECT_EQ(fixture.server.total_connections(), admitted);
    EXPECT_EQ(
      fixture.server.current_connections(),
      kwaque::admin::connections_per_shard);
    fixture.stop();
    EXPECT_EQ(fixture.server.current_connections(), 0U);
}

TEST(AdminResourceTest, TrickleCannotRenewAbsoluteHeaderLifetime) {
    server_fixture fixture(short_deadlines());
    client connection(fixture.address);
    connection.send("GET / HTTP/1.1\r\nHeader: ");
    const auto started = std::chrono::steady_clock::now();
    unsigned ticks = 0;
    while (std::chrono::steady_clock::now() - started < 400ms) {
        try {
            connection.send("x");
        } catch (const std::system_error&) {
        }
        ++ticks;
        seastar::sleep(20ms).get();
        if (fixture.server.current_connections() == 0) {
            break;
        }
    }
    EXPECT_GE(ticks, 2U);
    EXPECT_LT(std::chrono::steady_clock::now() - started, 400ms);
    EXPECT_EQ(fixture.server.current_connections(), 0U);
    EXPECT_EQ(fixture.handled, 0U);
}

TEST(
  AdminResourceTest,
  StalledResponseReaderLosesItsConnectionAndControlKeepsRunning) {
    std::size_t written = 0;
    server_fixture fixture(short_deadlines());
    auto handler = std::make_unique<httpd::function_handler>(
      [&written](httpd::const_req, http::reply& reply) {
          reply.write_body(
            "txt", [&written](seastar::output_stream<char>&& output) {
                return write_until_disconnected(std::move(output), written);
            });
          return seastar::sstring{};
      },
      "txt");
    fixture.server._routes.put(httpd::GET, "/large", handler.get());
    static_cast<void>(handler.release());
    client connection(fixture.address);
    const int receive_bytes = 1024;
    connection.socket.set_sockopt(
      SOL_SOCKET, SO_RCVBUF, &receive_bytes, sizeof(receive_bytes));
    connection.send("GET /large HTTP/1.1\r\n\r\n");
    wait_for_connections(fixture.server, 1);
    unsigned ticks = 0;
    const auto deadline = std::chrono::steady_clock::now() + 1s;
    while (fixture.server.current_connections()
           && std::chrono::steady_clock::now() < deadline) {
        seastar::sleep(10ms).get();
        ++ticks;
    }
    EXPECT_GT(ticks, 1U);
    EXPECT_GT(written, 0U);
    EXPECT_LT(written, 64U * 1024U * 1024U);
    EXPECT_EQ(fixture.server.current_connections(), 0U);
}

TEST(AdminResourceTest, ShutdownDrainsAStalledOutputBeforeItsDeadline) {
    std::size_t written = 0;
    server_fixture fixture;
    auto handler = std::make_unique<httpd::function_handler>(
      [&written](httpd::const_req, http::reply& reply) {
          reply.write_body(
            "txt", [&written](seastar::output_stream<char>&& output) {
                return write_until_disconnected(std::move(output), written);
            });
          return seastar::sstring{};
      },
      "txt");
    fixture.server._routes.put(httpd::GET, "/large", handler.get());
    static_cast<void>(handler.release());
    client connection(fixture.address);
    const int receive_bytes = 1024;
    connection.socket.set_sockopt(
      SOL_SOCKET, SO_RCVBUF, &receive_bytes, sizeof(receive_bytes));
    connection.send("GET /large HTTP/1.1\r\n\r\n");
    wait_for_connections(fixture.server, 1);
    while (!written && fixture.server.current_connections()) {
        seastar::sleep(1ms).get();
    }
    EXPECT_GT(written, 0U);
    fixture.stop();
    EXPECT_EQ(fixture.server.current_connections(), 0U);
}

TEST(
  AdminResourceTest,
  SnapshotAdmissionAndPreflightFailuresReleaseSourceOwnership) {
    metrics::impl::snapshot_limits limits;
    auto first = metrics::impl::get_values(
      metrics::impl::default_handle(), limits);
    EXPECT_THROW(
      metrics::impl::get_values(metrics::impl::default_handle(), limits),
      std::runtime_error);
    first.destroy().get();
    limits.families = 0;
    EXPECT_THROW(
      metrics::impl::get_values(metrics::impl::default_handle(), limits),
      std::length_error);
    limits.families = 256;
    limits.value_bytes = 1;
    EXPECT_THROW(
      metrics::impl::get_values(metrics::impl::default_handle(), limits),
      std::length_error);
    limits.value_bytes = kwaque::admin::metrics_snapshot_bytes;
    auto again = metrics::impl::get_values(
      metrics::impl::default_handle(), limits);
    again.destroy().get();
}

TEST(
  AdminResourceTest,
  ConcurrentScrapesStayWithinReservationAndReleaseSourceQuota) {
    const auto before = seastar::memory::stats().free_memory();
    server_fixture fixture;
    seastar::prometheus::config config;
    config.snapshot_bounds.emplace();
    seastar::prometheus::add_prometheus_routes(fixture.server, config).get();
    std::vector<std::unique_ptr<client>> clients;
    for (std::size_t index = 0; index < kwaque::admin::connections_per_shard;
         ++index) {
        clients.push_back(std::make_unique<client>(fixture.address));
        clients.back()->send(
          "GET /metrics?__aggregate__=false HTTP/1.1\r\n\r\n");
    }
    auto peak = before > seastar::memory::stats().free_memory()
                  ? before - seastar::memory::stats().free_memory()
                  : 0;
    for (auto& connection : clients) {
        // Stream and discard; a failed competing scrape may be a truncated
        // body.
        while (true) {
            auto buffer = connection->input.read().get();
            if (buffer.empty()) {
                break;
            }
            const auto remaining = seastar::memory::stats().free_memory();
            if (remaining < before) {
                peak = std::max(peak, before - remaining);
            }
        }
    }
    wait_for_connections(fixture.server, 0);
#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
    EXPECT_LT(peak, kwaque::admin::admin_reservation_bytes);
#endif
    std::cout << "admin saturation retained bytes=" << peak << '\n';
    auto snapshot = metrics::impl::get_values(
      metrics::impl::default_handle(), config.snapshot_bounds);
    snapshot.destroy().get();
}

TEST(AdminResourceTest, FragmentedParserAppliesLimitsAcrossReadBoundaries) {
    const std::array requests{
      std::string("GET / HTTP/1.1\r\nHeader: okay\r\n\r\n"),
      std::string("GET /01234567890123456789 HTTP/1.1\r\n\r\n"),
      std::string("GET /\na\n") + std::string(100, 'x') + " HTTP/1.1\r\n\r\n",
      std::string("GET /\ra\r") + std::string(100, 'x') + " HTTP/1.1\r\n\r\n",
      std::string("GET / HTTP/1.1\r\nA: b\r\nA: c\r\nA: d\r\n\r\n"),
    };
    for (std::size_t index = 0; index < requests.size(); ++index) {
        seastar::http_request_parser parser;
        parser.set_limits(24, 64, 2);
        parser.init();
        bool completed = false;
        for (char byte : requests[index]) {
            if (parser.parse(&byte, &byte + 1, nullptr) != nullptr) {
                completed = true;
                break;
            }
        }
        EXPECT_TRUE(completed);
        EXPECT_EQ(parser.failed(), index != 0);
    }
}

TEST(AdminResourceTest, MetadataIsRejectedBeforeCallingMetricProducers) {
    unsigned calls = 0;
    metrics::metric_groups group;
    group.add_group(
      "admin_limit_probe",
      {
        metrics::make_gauge(
          "value",
          [&calls] { return ++calls; },
          metrics::description(seastar::sstring(std::string(4097, 'x')))),
      });
    EXPECT_THROW(
      metrics::impl::get_values(
        metrics::impl::default_handle(), metrics::impl::snapshot_limits{}),
      std::length_error);
    EXPECT_EQ(calls, 0U);
}

TEST(AdminResourceTest, SnapshotByteCapChargesDequeBlocksBeforeSampling) {
    constexpr int handle = 17;
    unsigned calls = 0;
    metrics::metric_groups group{handle};
    for (unsigned index = 0; index < 32; ++index) {
        group.add_group(
          "capacity_" + seastar::to_sstring(index),
          {metrics::make_gauge("value", [&calls] { return ++calls; })});
    }
    metrics::impl::snapshot_limits limits;
    // Each family owns a value deque block even with only one scalar. These
    // blocks cannot fit into 64 KiB; rejecting them must precede callbacks.
    limits.value_bytes = 64U * 1024U;
    EXPECT_THROW(metrics::impl::get_values(handle, limits), std::length_error);
    EXPECT_EQ(calls, 0U);
    limits.value_bytes = kwaque::admin::metrics_snapshot_bytes;
    const auto before = seastar::memory::stats().allocated_memory();
    auto accepted = metrics::impl::get_values(handle, limits);
    EXPECT_EQ(calls, 32U);
#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
    const auto after = seastar::memory::stats().allocated_memory();
    EXPECT_LE(after - before, limits.metadata_bytes + limits.value_bytes);
#else
    static_cast<void>(before);
#endif
    accepted.destroy().get();
}

TEST(AdminResourceTest, BoundedSnapshotReplacesCleanUnboundedCacheCapacity) {
    constexpr int handle = 18;
    metrics::metric_groups group{handle};
    for (unsigned index = 0; index < 128; ++index) {
        group.add_group(
          "cache_history_" + seastar::to_sstring(index),
          {metrics::make_gauge("value", [] { return 1; })});
    }
    auto expanded = metrics::impl::get_values(handle);
    expanded.destroy().get();
    auto implementation = metrics::impl::get_local_impl(handle);
    const auto previous_capacity = implementation->functions().capacity();
    ASSERT_GE(previous_capacity, 128U);

    group.clear();
    group.add_group(
      "cache_history_small", {metrics::make_gauge("value", [] { return 1; })});
    auto smaller = metrics::impl::get_values(handle);
    smaller.destroy().get();
    ASSERT_EQ(implementation->functions().size(), 1U);
    ASSERT_EQ(implementation->functions().capacity(), previous_capacity);

    auto bounded = metrics::impl::get_values(
      handle, metrics::impl::snapshot_limits{});
    EXPECT_LT(implementation->functions().capacity(), previous_capacity);
    auto metadata = bounded->metadata;
    bounded.destroy().get();
    // A clean unbounded accessor does not rebuild or change the cache's mode.
    // Retain the metadata reference to make identity reuse deterministic.
    static_cast<void>(implementation->functions());
    auto repeated = metrics::impl::get_values(
      handle, metrics::impl::snapshot_limits{});
    EXPECT_EQ(repeated->metadata.get(), metadata.get());
    repeated.destroy().get();
}

TEST(AdminResourceTest, HistogramLimitFailureReleasesSnapshotCapacity) {
    metrics::metric_groups group;
    group.add_group(
      "admin_limit_probe",
      {
        metrics::make_histogram(
          "value",
          [] {
              metrics::histogram histogram;
              histogram.buckets.resize(129);
              return histogram;
          }),
      });
    EXPECT_THROW(
      metrics::impl::get_values(
        metrics::impl::default_handle(), metrics::impl::snapshot_limits{}),
      std::length_error);
    group.clear();
    auto snapshot = metrics::impl::get_values(
      metrics::impl::default_handle(), metrics::impl::snapshot_limits{});
    snapshot.destroy().get();
}

TEST(AdminResourceTest, MetricsRejectsRegexAndCapsStreamedOutput) {
    server_fixture fixture;
    seastar::prometheus::config config;
    config.snapshot_bounds.emplace();
    config.max_response_bytes = 64;
    seastar::prometheus::add_prometheus_routes(fixture.server, config).get();
    {
        client connection(fixture.address);
        connection.send("GET /metrics?shard=(a%2B)%2B HTTP/1.1\r\n\r\n");
        EXPECT_NE(
          connection.response().find("400 Bad Request"), std::string::npos);
    }
    {
        client connection(fixture.address);
        connection.send("GET /metrics HTTP/1.1\r\n\r\n");
        const auto response = connection.response();
        EXPECT_LT(response.size(), 4096U);
        EXPECT_NE(
          response.find("Transfer-Encoding: chunked"), std::string::npos);
        EXPECT_FALSE(response.ends_with("0\r\n\r\n"));
        EXPECT_GT(fixture.server.reply_errors(), 0U);
    }
    auto snapshot = metrics::impl::get_values(
      metrics::impl::default_handle(), config.snapshot_bounds);
    snapshot.destroy().get();
}

constexpr int aggregation_probe_handle = 7;

class discarded_output final : public seastar::data_sink_impl {
public:
    seastar::future<> put(std::span<seastar::temporary_buffer<char>>) override {
        return seastar::make_ready_future<>();
    }
    seastar::future<> close() override {
        return seastar::make_ready_future<>();
    }
    std::size_t buffer_size() const noexcept override { return 4096; }
};

class requested_preemption final {
public:
    requested_preemption()
      : previous_(seastar::internal::get_need_preempt_var()) {
        monitor_.head.store(1, std::memory_order_relaxed);
        monitor_.tail.store(0, std::memory_order_relaxed);
        seastar::internal::set_need_preempt_var(&monitor_);
    }
    ~requested_preemption() {
        seastar::internal::set_need_preempt_var(previous_);
    }

private:
    seastar::internal::preemption_monitor monitor_{};
    const seastar::internal::preemption_monitor* previous_;
};

class control_progress final {
public:
    ~control_progress() {
        preemption_.reset();
        if (pending_) {
            pending_->get();
        }
    }

    void request() {
        if (!pending_) {
            // Request cooperation only after formatting has started, so an
            // earlier cross-shard collection yield cannot satisfy this probe.
            preemption_.emplace();
            pending_.emplace(
              seastar::yield().then([this] { progressed_ = true; }));
        }
    }

    bool progressed() const noexcept { return progressed_; }

private:
    std::optional<requested_preemption> preemption_;
    std::optional<seastar::future<>> pending_;
    bool progressed_{false};
};

TEST(AdminResourceTest, FilteredFamiliesYieldWithoutWritingAnySeries) {
    metrics::metric_groups group{aggregation_probe_handle};
    for (unsigned index = 0; index < 64; ++index) {
        group.add_group(
          "filtered_" + seastar::to_sstring(index),
          {metrics::make_gauge("value", [] { return 1; })});
    }
    control_progress control;
    bool observed_progress = false;
    seastar::prometheus::config config;
    config.handle = aggregation_probe_handle;
    config.snapshot_bounds.emplace();
    prometheus_test_fixture::write(
      std::move(config),
      {.filter = [](const auto&) { return true; },
       .family_filter =
         [&](std::string_view) {
             control.request();
             observed_progress = observed_progress || control.progressed();
             return false;
         },
       .use_protobuf_format = false,
       .show_help = false,
       .enable_aggregation = false},
      seastar::output_stream<char>{
        seastar::data_sink{std::make_unique<discarded_output>()}})
      .get();
    EXPECT_TRUE(observed_progress);
}

TEST(AdminResourceTest, EmptySeriesYieldBeforeTheNextVisibleSeries) {
    metrics::metric_groups group{aggregation_probe_handle};
    const metrics::label series{"series"};
    for (unsigned index = 0; index < 128; ++index) {
        group.add_group(
          "empty_probe",
          {metrics::make_gauge(
             "value",
             [index] { return index == 127 ? 1 : 0; },
             metrics::description{},
             {series(
               index == 127 ? seastar::sstring{"z"}
                            : "a" + seastar::to_sstring(index))})
             .set_skip_when_empty()});
    }
    control_progress control;
    bool observed_progress = false;
    seastar::prometheus::config config;
    config.handle = aggregation_probe_handle;
    config.snapshot_bounds.emplace();
    prometheus_test_fixture::write(
      std::move(config),
      {.filter =
         [&](const auto&) {
             observed_progress = control.progressed();
             return true;
         },
       .family_filter =
         [&](std::string_view) {
             control.request();
             return true;
         },
       .use_protobuf_format = false,
       .show_help = false,
       .enable_aggregation = false},
      seastar::output_stream<char>{
        seastar::data_sink{std::make_unique<discarded_output>()}})
      .get();
    EXPECT_TRUE(observed_progress);
}

class snapshot_source final {
public:
    explicit snapshot_source(int handle)
      : handle_(handle)
      , group_(handle) {
        group_.add_group(
          "admin_snapshot_probe",
          {metrics::make_gauge("sample", [this] { return ++samples_; })});
    }

    void hold() {
        held_.emplace(
          metrics::impl::get_values(handle_, metrics::impl::snapshot_limits{}));
    }

    seastar::future<> release() {
        if (held_) {
            co_await held_->destroy();
            held_.reset();
        }
    }

    bool busy() {
        try {
            auto snapshot = metrics::impl::get_values(
              handle_, metrics::impl::snapshot_limits{});
            // This pointer was created on this shard; native destruction is
            // synchronous here. Never hold probe admission across a yield.
            snapshot.destroy().get();
            return false;
        } catch (const std::runtime_error& error) {
            if (
              std::string_view(error.what())
              == "metrics snapshot capacity exhausted") {
                return true;
            }
            throw;
        }
    }

    unsigned samples() const noexcept { return samples_; }

    void add_large_output() {
        const metrics::label series{"series"};
        // Each histogram is formatted before its first write. Keep one
        // 128-bucket sample below 64 KiB while the complete scrape still
        // exceeds the deliberately constrained socket buffers.
        for (unsigned index = 0; index < 8; ++index) {
            group_.add_group(
              "admin_parked_output",
              {metrics::make_histogram(
                "value",
                [] {
                    metrics::histogram histogram;
                    histogram.sample_count = 1;
                    histogram.sample_sum = 1;
                    histogram.buckets.resize(128);
                    for (std::size_t bucket = 0;
                         bucket < histogram.buckets.size();
                         ++bucket) {
                        histogram.buckets[bucket] = metrics::histogram_bucket{
                          1, static_cast<double>(bucket + 1)};
                    }
                    return histogram;
                },
                metrics::description{},
                {series(
                  seastar::sstring{std::string(256, 'x')}
                  + seastar::to_sstring(index))})});
        }
    }

    seastar::future<> stop() {
        co_await release();
        group_.clear();
    }

private:
    int handle_;
    metrics::metric_groups group_;
    std::optional<seastar::foreign_ptr<metrics::impl::values_reference>> held_;
    unsigned samples_{0};
};

TEST(AdminResourceTest, LaterShardRejectionReleasesEarlierCollectedSnapshot) {
    ASSERT_GE(seastar::this_smp_shard_count(), 2U);
    seastar::sharded<snapshot_source> sources;
    sources.start(aggregation_probe_handle).get();
    std::exception_ptr failure;
    try {
        sources.invoke_on(1, &snapshot_source::hold).get();
        const auto before = sources.local().samples();
        server_fixture fixture;
        seastar::prometheus::config config;
        config.handle = aggregation_probe_handle;
        config.snapshot_bounds.emplace();
        seastar::prometheus::add_prometheus_routes(fixture.server, config)
          .get();
        client connection(fixture.address);
        connection.send("GET /metrics HTTP/1.1\r\n\r\n");
        EXPECT_FALSE(connection.response().ends_with("0\r\n\r\n"));
        EXPECT_GT(sources.local().samples(), before);
        EXPECT_FALSE(sources.local().busy());
        EXPECT_TRUE(sources.invoke_on(1, &snapshot_source::busy).get());
        sources.invoke_on(1, &snapshot_source::release).get();
        EXPECT_FALSE(sources.invoke_on(1, &snapshot_source::busy).get());
    } catch (...) {
        failure = std::current_exception();
    }
    sources.stop().get();
    if (failure) {
        std::rethrow_exception(failure);
    }
}

TEST(AdminResourceTest, AdminStopDrainsParkedExporterAndForeignSnapshots) {
    ASSERT_GE(seastar::this_smp_shard_count(), 2U);
    seastar::sharded<snapshot_source> sources;
    sources.start(metrics::impl::default_handle()).get();
    kwaque::admin::admin_server server;
    std::exception_ptr failure;
    try {
        sources.invoke_on_all(&snapshot_source::add_large_output).get();
        seastar::listen_options probe_options;
        auto probe = seastar::listen(
          seastar::socket_address{seastar::net::inet_address{"127.0.0.1"}, 0},
          probe_options);
        const auto port = probe.local_address().port();
        probe = {};
        server.start("127.0.0.1", port, seastar::this_smp_shard_count()).get();
        client connection(
          seastar::socket_address{
            seastar::net::inet_address{"127.0.0.1"}, port});
        const int receive_bytes = 1024;
        connection.socket.set_sockopt(
          SOL_SOCKET, SO_RCVBUF, &receive_bytes, sizeof(receive_bytes));
        auto& native
          = kwaque::admin::detail::admin_server_test_access::native_server(
            server);
        const auto deadline = std::chrono::steady_clock::now() + 2s;
        std::uint64_t configured = 0;
        while (configured == 0 && std::chrono::steady_clock::now() < deadline) {
            configured
              = native.server()
                  .map_reduce0(
                    [](httpd::http_server& shard) {
                        httpd::http_server_tester::set_connection_send_buffer(
                          shard, 4096);
                        return shard.current_connections();
                    },
                    std::uint64_t{0},
                    std::plus<>{})
                  .get();
            seastar::sleep(1ms).get();
        }
        if (configured != 1) {
            throw std::runtime_error("admin output socket was not prepared");
        }
        connection.send("GET /metrics?__aggregate__=false HTTP/1.1\r\n\r\n");
        std::string prefix;
        while (prefix.find("admin_parked_output_value") == std::string::npos) {
            auto bytes = connection.input.read().get();
            if (bytes.empty() || prefix.size() + bytes.size() > 16U * 1024U) {
                throw std::runtime_error("exporter did not start its body");
            }
            prefix.append(bytes.get(), bytes.size());
        }
        bool all_busy = false;
        const auto busy_deadline = std::chrono::steady_clock::now() + 2s;
        while (!all_busy && std::chrono::steady_clock::now() < busy_deadline) {
            all_busy = sources
                         .map_reduce0(
                           &snapshot_source::busy, true, std::logical_and<>{})
                         .get();
            seastar::sleep(1ms).get();
        }
        if (!all_busy) {
            throw std::runtime_error(
              "exporter did not retain source snapshots");
        }
        // Both source shards remain admitted while output cannot progress.
        // The real owner's stop must release these before destroying its group.
        EXPECT_GT(
          sources
            .map_reduce0(
              [](snapshot_source&) { return admin_group_metrics(); },
              std::size_t{0},
              std::plus<>{})
            .get(),
          0U);
        server.begin_shutdown().get();
        server.stop().get();
        EXPECT_FALSE(
          sources
            .map_reduce0(&snapshot_source::busy, false, std::logical_or<>{})
            .get());
        EXPECT_EQ(
          sources
            .map_reduce0(
              [](snapshot_source&) { return admin_group_metrics(); },
              std::size_t{0},
              std::plus<>{})
            .get(),
          0U);
    } catch (...) {
        failure = std::current_exception();
    }
    server.stop().get();
    sources.stop().get();
    if (failure) {
        std::rethrow_exception(failure);
    }
}

void register_aggregation_probe(
  metrics::metric_groups& group,
  std::size_t series,
  std::size_t retained_groups) {
    const metrics::label source{"source"};
    const metrics::label bucket{"bucket"};
    const std::vector<metrics::label> aggregate{
      source, metrics::label{"shard"}};
    for (std::size_t index = 0; index < series; ++index) {
        group.add_group(
          "admin_aggregate_probe",
          {
            metrics::make_gauge(
              "value",
              [] { return 1.0; },
              metrics::description{},
              {source(index), bucket(index % retained_groups)})
              .aggregate(aggregate),
          });
    }
}

TEST(
  AdminResourceTest, AggregationChargesRetainedGroupsRatherThanRepeatedInputs) {
    metrics::metric_groups group{aggregation_probe_handle};
    // 512 input samples exceed 64 KiB when charged individually, but merge
    // into only eight retained groups, each containing the sum of 64 samples.
    register_aggregation_probe(group, 512, 8);
    server_fixture fixture;
    seastar::prometheus::config config;
    config.handle = aggregation_probe_handle;
    config.snapshot_bounds.emplace();
    seastar::prometheus::add_prometheus_routes(fixture.server, config).get();
    client connection(fixture.address);
    connection.send(
      "GET /metrics?__name__=admin_aggregate_probe_value HTTP/1.1\r\n\r\n");
    const auto response = connection.response();
    EXPECT_NE(response.find("200 OK"), std::string::npos);
    EXPECT_TRUE(response.ends_with("0\r\n\r\n"));
    EXPECT_EQ(fixture.server.reply_errors(), 0U);
    for (std::size_t bucket = 0; bucket < 8; ++bucket) {
        EXPECT_NE(
          response.find("bucket=\"" + std::to_string(bucket) + "\"} 64.000000"),
          std::string::npos);
    }
    EXPECT_EQ(response.find("source=\""), std::string::npos);
}

TEST(AdminResourceTest, AggregationChargesHistogramGrowthOncePerRetainedGroup) {
    metrics::metric_groups group{aggregation_probe_handle};
    const metrics::label source{"source"};
    const std::vector<metrics::label> aggregate{
      source, metrics::label{"shard"}};
    for (std::size_t index = 0; index < 96; ++index) {
        group.add_group(
          "admin_aggregate_probe",
          {
            metrics::make_histogram(
              "value",
              [index] {
                  metrics::histogram histogram;
                  histogram.sample_count = 1;
                  histogram.sample_sum = 1;
                  histogram.buckets.resize(index == 0 ? 1 : 128);
                  for (std::size_t bucket = 0;
                       bucket < histogram.buckets.size();
                       ++bucket) {
                      histogram.buckets[bucket] = metrics::histogram_bucket{
                        1, static_cast<double>(bucket + 1)};
                  }
                  return histogram;
              },
              metrics::description{},
              {source(index)})
              .aggregate(aggregate),
          });
    }
    server_fixture fixture;
    seastar::prometheus::config config;
    config.handle = aggregation_probe_handle;
    config.snapshot_bounds.emplace();
    seastar::prometheus::add_prometheus_routes(fixture.server, config).get();
    client connection(fixture.address);
    connection.send(
      "GET /metrics?__name__=admin_aggregate_probe_value HTTP/1.1\r\n\r\n");
    const auto response = connection.response();
    EXPECT_TRUE(response.ends_with("0\r\n\r\n"));
    EXPECT_EQ(fixture.server.reply_errors(), 0U);
    EXPECT_NE(
      response.find("admin_aggregate_probe_value_count{} 96"),
      std::string::npos);
}

TEST(AdminResourceTest, AggregationRejectsExcessDistinctRetainedGroups) {
    metrics::metric_groups group{aggregation_probe_handle};
    register_aggregation_probe(group, 256, 256);
    server_fixture fixture;
    seastar::prometheus::config config;
    config.handle = aggregation_probe_handle;
    config.snapshot_bounds.emplace();
    seastar::prometheus::add_prometheus_routes(fixture.server, config).get();
    client connection(fixture.address);
    connection.send(
      "GET /metrics?__name__=admin_aggregate_probe_value HTTP/1.1\r\n\r\n");
    const auto response = connection.response();
    EXPECT_FALSE(response.ends_with("0\r\n\r\n"));
    EXPECT_GT(fixture.server.reply_errors(), 0U);
    auto snapshot = metrics::impl::get_values(
      config.handle, config.snapshot_bounds);
    snapshot.destroy().get();
}

TEST(
  AdminResourceTest, LifecycleRejectsMissingReserveAndReleasesSchedulingGroup) {
    {
        kwaque::admin::admin_server invalid;
        EXPECT_THROW(
          invalid
            .start("127.0.0.1", 0, seastar::this_smp_shard_count(), nullptr, 0)
            .get(),
          std::invalid_argument);
        invalid.stop().get();
    }
    for (unsigned attempt = 0; attempt < 2; ++attempt) {
        EXPECT_EQ(admin_group_metrics(), 0U);
        kwaque::admin::admin_server server;
        server.start("127.0.0.1", 0, seastar::this_smp_shard_count()).get();
        EXPECT_GT(admin_group_metrics(), 0U);
        server.begin_shutdown().get();
        server.stop().get();
        EXPECT_EQ(admin_group_metrics(), 0U);
    }
}

} // namespace
