#include "src/admin/admin_limits.h"
#include "src/broker/application_internal.h"
#include "src/broker/application_test_support.h"
#include "src/config/bootstrap_config.h"
#include "src/resource/resource_config.h"
#include "src/resource/resource_manager.h"
#include "src/resource/workload_class.h"
#include "src/runtime/cross_shard.h"
#include "src/runtime/production/environment.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_api.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/later.hh>
#include <seastar/util/tmp_file.hh>

#include <arpa/inet.h>
#include <boost/test/unit_test.hpp>
#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <exception>
#include <filesystem>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <unistd.h>
#include <utility>

namespace kwaque::broker::test_types {

struct headroom_test_port final {
    std::uint16_t value;
};

} // namespace kwaque::broker::test_types

template<>
struct kwaque::runtime::enable_cross_shard_value<
  kwaque::broker::test_types::headroom_test_port> : std::true_type {};

namespace {

using kwaque::broker::test_types::headroom_test_port;
static_assert(kwaque::runtime::cross_shard_value<headroom_test_port>);

#ifndef SEASTAR_DEFAULT_ALLOCATOR
constexpr auto progress_timeout = std::chrono::seconds{3};

// These clients use fixed buffers and kernel sockets so their protocol stack
// does not consume the broker shard allocator. Kernel socket memory belongs
// to the separate host/process accounting, on both sides of the connection.
class raw_client final {
public:
    raw_client() = default;
    raw_client(const raw_client&) = delete;
    raw_client& operator=(const raw_client&) = delete;
    ~raw_client() { close(); }

    void close() noexcept {
        if (descriptor_ != -1) {
            static_cast<void>(::close(std::exchange(descriptor_, -1)));
        }
    }

    seastar::future<> connect(std::uint16_t port) {
        descriptor_ = ::socket(
          AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (descriptor_ == -1) {
            throw std::system_error(errno, std::generic_category());
        }
        const sockaddr_in address{
          .sin_family = AF_INET,
          .sin_port = htons(port),
          .sin_addr = {.s_addr = htonl(INADDR_LOOPBACK)},
          .sin_zero = {}};
        if (
          ::connect(
            descriptor_,
            reinterpret_cast<const sockaddr*>(&address),
            sizeof(address))
          == 0) {
            co_return;
        }
        if (errno != EINPROGRESS) {
            throw std::system_error(errno, std::generic_category());
        }
        const auto deadline = std::chrono::steady_clock::now()
                              + progress_timeout;
        while (true) {
            sockaddr_in peer{};
            socklen_t length = sizeof(peer);
            if (
              ::getpeername(
                descriptor_, reinterpret_cast<sockaddr*>(&peer), &length)
              == 0) {
                co_return;
            }
            int error = 0;
            length = sizeof(error);
            if (
              ::getsockopt(descriptor_, SOL_SOCKET, SO_ERROR, &error, &length)
              == -1) {
                throw std::system_error(errno, std::generic_category());
            }
            if (error != 0) {
                throw std::system_error(error, std::generic_category());
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                throw std::runtime_error("loopback connection timed out");
            }
            co_await seastar::sleep(std::chrono::milliseconds{1});
        }
    }

    seastar::future<> send(std::string_view bytes) {
        const auto deadline = std::chrono::steady_clock::now()
                              + progress_timeout;
        while (!bytes.empty()) {
            const auto amount = ::send(
              descriptor_, bytes.data(), bytes.size(), MSG_NOSIGNAL);
            if (amount > 0) {
                bytes.remove_prefix(static_cast<std::size_t>(amount));
                continue;
            }
            if (
              amount == -1 && errno != EAGAIN && errno != EWOULDBLOCK
              && errno != EINTR) {
                throw std::system_error(errno, std::generic_category());
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                throw std::runtime_error("loopback send timed out");
            }
            co_await seastar::sleep(std::chrono::milliseconds{1});
        }
    }

    template<typename Sample>
    seastar::future<> read_scrape(Sample sample) {
        const auto deadline = std::chrono::steady_clock::now()
                              + progress_timeout;
        std::array<char, 4096> buffer{};
        std::array<char, 32> prefix{};
        std::size_t prefix_bytes = 0;
        std::size_t total = 0;
        while (true) {
            sample();
            const auto amount = ::recv(
              descriptor_, buffer.data(), buffer.size(), 0);
            if (amount == 0) {
                break;
            }
            if (amount > 0) {
                const auto received = static_cast<std::size_t>(amount);
                const auto copy = std::min(
                  received, prefix.size() - prefix_bytes);
                std::copy_n(
                  buffer.begin(), copy, prefix.begin() + prefix_bytes);
                prefix_bytes += copy;
                total += received;
                if (
                  total > kwaque::admin::metrics_response_bytes + 16U * 1024U) {
                    throw std::runtime_error(
                      "scrape exceeded its response bound");
                }
                if (std::chrono::steady_clock::now() >= deadline) {
                    throw std::runtime_error("loopback scrape timed out");
                }
                co_await seastar::maybe_yield();
                continue;
            }
            if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
                throw std::system_error(errno, std::generic_category());
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                throw std::runtime_error("loopback scrape timed out");
            }
            co_await seastar::sleep(std::chrono::milliseconds{1});
        }
        BOOST_CHECK((std::string_view{prefix.data(), prefix_bytes}.starts_with(
          "HTTP/1.1 200")));
        BOOST_CHECK_GT(total, prefix_bytes);
    }

private:
    int descriptor_{-1};
};

std::uint16_t reserve_loopback_port() {
    const int descriptor = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (descriptor == -1) {
        throw std::system_error(errno, std::generic_category());
    }
    sockaddr_in address{
      .sin_family = AF_INET,
      .sin_port = 0,
      .sin_addr = {.s_addr = htonl(INADDR_LOOPBACK)},
      .sin_zero = {}};
    socklen_t length = sizeof(address);
    const auto bound = ::bind(
      descriptor, reinterpret_cast<const sockaddr*>(&address), sizeof(address));
    const auto inspected = bound == 0 ? ::getsockname(
                                          descriptor,
                                          reinterpret_cast<sockaddr*>(&address),
                                          &length)
                                      : -1;
    const auto error = errno;
    static_cast<void>(::close(descriptor));
    if (inspected == -1) {
        throw std::system_error(error, std::generic_category());
    }
    return ntohs(address.sin_port);
}

std::uint64_t connection_count() {
    const auto& metrics = seastar::metrics::impl::get_value_map();
    const auto family = metrics.find("httpd_connections_current");
    if (family == metrics.end() || family->second.size() != 1) {
        throw std::runtime_error("admin connection metric is unavailable");
    }
    return family->second.begin()->second->get_function()().ui();
}

seastar::future<> wait_for_connections(std::uint64_t expected) {
    const auto deadline = std::chrono::steady_clock::now() + progress_timeout;
    while (connection_count() != expected) {
        if (std::chrono::steady_clock::now() >= deadline) {
            throw std::runtime_error("admin connection count did not settle");
        }
        co_await seastar::sleep(std::chrono::milliseconds{1});
    }
}

struct resident_payload final {
    seastar::semaphore_units<> admission;
    seastar::temporary_buffer<char> bytes;
};

seastar::future<> saturate_broker(
  kwaque::runtime::production::environment& environment,
  headroom_test_port endpoint) {
    const auto port = endpoint.value;
    auto& manager = environment.resource_manager();
    const auto before = seastar::memory::stats();
    std::uint64_t peak_allocated = before.allocated_memory();
    std::uint64_t collections = 0;
    const auto sample = [&peak_allocated] {
        peak_allocated = std::max<std::uint64_t>(
          peak_allocated, seastar::memory::stats().allocated_memory());
    };
    std::array<
      std::optional<kwaque::resource::workload_handle>,
      kwaque::resource::workload_class_count>
      handles;
    std::deque<resident_payload> payloads;
    std::uint64_t charged = 0;
    for (const auto classification : kwaque::resource::all_workload_classes) {
        auto& workload
          = handles[kwaque::resource::workload_index(classification)];
        workload.emplace(manager.acquire_workload(classification));
        auto remaining = workload->hard_budget().value();
        while (remaining != 0) {
            const auto amount = std::min<std::uint64_t>(remaining, 64U * 1024U);
            auto units = co_await seastar::get_units(
              workload->memory_admission(), amount);
            seastar::temporary_buffer<char> bytes{amount};
            std::memset(bytes.get_write(), 0x5a, bytes.size());
            payloads.push_back(
              resident_payload{std::move(units), std::move(bytes)});
            remaining -= amount;
            charged += amount;
            co_await seastar::maybe_yield();
        }
        BOOST_CHECK_EQUAL(manager.memory_available(classification).value(), 0U);
    }
    const auto reserve
      = kwaque::resource::resource_config::default_reactor_headroom().value()
        + kwaque::admin::admin_reservation_bytes;
    BOOST_CHECK_EQUAL(charged + reserve, before.total_memory());

    std::exception_ptr failure;
    seastar::shared_promise<> release_task;
    const auto completed_before = environment.tasks().statistics().completed;
    bool task_accepted = false;
    {
        seastar::metrics::metric_groups sampler;
        // Sample while the native exporter owns its collected values, then
        // sample again during response reads. Fixture allocations remain in
        // the observation as conservative overhead; none are estimated away.
        sampler.add_group(
          "zzzz_headroom_test",
          {seastar::metrics::make_gauge(
            "allocated_bytes",
            [&] {
                ++collections;
                sample();
                return peak_allocated;
            },
            seastar::metrics::description(
              "Native allocator sample during the test scrape"))});
        std::array<raw_client, kwaque::admin::connections_per_shard> clients;
        try {
            task_accepted = environment.tasks()
                              .spawn([&release_task] {
                                  return release_task.get_shared_future();
                              })
                              .has_value();
            BOOST_CHECK(task_accepted);
            std::array<char, kwaque::admin::header_bytes - 256> header{};
            header.fill('x');
            constexpr std::string_view prefix{
              "GET /v1/health/live HTTP/1.1\r\nHost: localhost\r\nX-Padding: "};
            std::copy(prefix.begin(), prefix.end(), header.begin());
            for (std::size_t index = 0; index + 1 < clients.size(); ++index) {
                co_await clients[index].connect(port);
                co_await clients[index].send(
                  std::string_view{header.data(), header.size()});
            }
            co_await clients.back().connect(port);
            co_await wait_for_connections(clients.size());
            sample();
            co_await clients.back().send(
              "GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: "
              "close\r\n\r\n");
            co_await clients.back().read_scrape(sample);
            BOOST_CHECK_GT(collections, 0U);
            bool progressed = false;
            auto& control = handles[kwaque::resource::workload_index(
              kwaque::resource::workload_class::consensus_critical)];
            co_await seastar::with_scheduling_group(
              control->scheduling_group(),
              [&progressed] { progressed = true; });
            BOOST_CHECK(progressed);
            sample();
        } catch (...) {
            failure = std::current_exception();
        }
        for (auto& client : clients) {
            client.close();
        }
        release_task.set_value();
        if (task_accepted) {
            while (environment.tasks().statistics().completed
                   == completed_before) {
                co_await seastar::yield();
            }
        }
        co_await wait_for_connections(0);
    }
    BOOST_CHECK_GE(peak_allocated, charged);
    BOOST_CHECK_LE(peak_allocated - charged, reserve);
    BOOST_CHECK_EQUAL(
      seastar::memory::stats().failed_allocations(),
      before.failed_allocations());
    BOOST_CHECK_EQUAL(
      seastar::memory::stats().fallback_allocations(),
      before.fallback_allocations());
    BOOST_TEST_MESSAGE(
      "native broker headroom: before="
      << before.allocated_memory() << " peak=" << peak_allocated
      << " charged=" << charged << " uncharged=" << peak_allocated - charged
      << " combined_reserve=" << reserve
      << " scrape_collections=" << collections);
    if (failure) {
        std::rethrow_exception(failure);
    }
}
#endif

} // namespace

SEASTAR_TEST_CASE(broker_headroom_covers_live_foundation_payload_and_admin) {
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    BOOST_TEST_MESSAGE(
      "native broker headroom measurement unavailable with system allocator");
    co_return;
#else
    BOOST_REQUIRE_EQUAL(seastar::this_smp_shard_count(), 1U);
    seastar::tmp_dir temporary;
    co_await temporary.create(
      std::filesystem::temp_directory_path() / "kwaque-broker-headroom-XXXXXX");
    kwaque::config::bootstrap_config configuration;
    configuration.data_directory = temporary.get_path() / "data";
    configuration.admin_port = reserve_loopback_port();
    configuration.developer_mode = true;
    configuration.diagnostic_memory_per_shard_bytes = 192U * 1024U * 1024U;
    kwaque::broker::detail::application_state application;
    kwaque::broker::detail::application_test_access::configure(
      application, configuration);
    application.construct_services(false);
    std::exception_ptr failure;
    try {
        co_await application.start_services();
        co_await kwaque::broker::detail::application_test_access::
          on_environments(
            application,
            &saturate_broker,
            headroom_test_port{configuration.admin_port});
    } catch (...) {
        failure = std::current_exception();
    }
    co_await application.shutdown();
    BOOST_CHECK(
      kwaque::broker::detail::application_test_access::services_released(
        application));
    BOOST_CHECK(
      !std::filesystem::exists(configuration.data_directory / "kwaque.pid"));
    const auto& metrics = seastar::metrics::impl::get_value_map();
    for (const auto name : {
           "broker_process_readiness",
           "httpd_connections_current",
           "runtime_task_active",
           "resource_manager_memory_configured_bytes",
           "zzzz_headroom_test_allocated_bytes",
         }) {
        BOOST_CHECK(!metrics.contains(name));
    }
    const auto after = seastar::memory::stats().allocated_memory();
    BOOST_CHECK_LE(
      after,
      kwaque::resource::resource_config::default_reactor_headroom().value()
        + kwaque::admin::admin_reservation_bytes);
    BOOST_TEST_MESSAGE("native broker headroom after shutdown=" << after);
    co_await temporary.remove();
    if (failure) {
        std::rethrow_exception(failure);
    }
#endif
}
