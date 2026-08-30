#ifndef KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_REAL_BACKEND_CONFORMANCE_H_
#define KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_REAL_BACKEND_CONFORMANCE_H_

#include "src/bytes/fragmented_buffer.h"
#include "src/runtime/environment.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/with_timeout.hh>

#include <chrono>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace kwaque::runtime::testing {

inline constexpr auto real_backend_operation_timeout = std::chrono::seconds{5};

template<typename T>
T require_success(result<T> outcome, std::string_view operation) {
    if (!outcome) {
        throw std::runtime_error(std::string{operation} + " failed");
    }
    return std::move(*outcome);
}

inline void require_success(result<void> outcome, std::string_view operation) {
    if (!outcome) {
        throw std::runtime_error(std::string{operation} + " failed");
    }
}

template<typename T>
seastar::future<T> bounded(seastar::future<T> operation) {
    return seastar::with_timeout(
      seastar::lowres_clock::now() + real_backend_operation_timeout,
      std::move(operation));
}

inline bytes::fragmented_buffer conformance_bytes(std::string_view value) {
    auto copied = bytes::fragmented_buffer::copy_of(
      std::span<const char>{value.data(), value.size()});
    if (!copied) {
        throw std::runtime_error("conformance payload exceeds buffer limits");
    }
    return std::move(*copied);
}

inline network_address conformance_loopback() noexcept {
    return network_address::ipv4(
      {std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}});
}

struct real_backend_dns_expectation final {
    dns_query query;
    std::vector<dns_answer> answers;
};

template<runtime_backend Backend>
seastar::future<> run_real_backend_conformance(
  Backend& backend,
  file_path root_path,
  std::optional<real_backend_dns_expectation> dns_expectation = std::nullopt) {
    basic_runtime<Backend> runtime{backend};
    auto acquired = runtime.template view<
      runtime_capability::timer,
      runtime_capability::random,
      runtime_capability::file_system,
      runtime_capability::network,
      runtime_capability::dns>();
    if (!acquired) {
        throw std::runtime_error("runtime capabilities were unavailable");
    }
    auto capabilities = std::move(*acquired);

    using clock_type = typename Backend::monotonic_clock;
    seastar::abort_source timer_abort;
    require_success(
      co_await bounded(
        capabilities.timer().sleep_until(clock_type::now(), timer_abort)),
      "timer");
    static_cast<void>(capabilities.random().next_u64());

    auto data_path = file_path::make(root_path.value() + "/payload");
    if (!data_path) {
        throw std::runtime_error("conformance file path was rejected");
    }
    require_success(
      co_await bounded(
        capabilities.file_system().create_directories(root_path)),
      "directory creation");
    auto file = require_success(
      co_await bounded(capabilities.file_system().open(
        *data_path,
        {.access = file_access::read_write,
         .create = true,
         .exclusive = true,
         .permissions = 0600U})),
      "file open");
    constexpr std::string_view file_payload = "backend-contract";
    auto written = require_success(
      co_await bounded(
        file.write(file_position{0}, conformance_bytes(file_payload))),
      "file write");
    if (written.value() != file_payload.size()) {
        throw std::runtime_error("file write returned the wrong byte count");
    }
    require_success(co_await bounded(file.flush()), "file flush");
    auto read = require_success(
      co_await bounded(file.read(file_position{0}, byte_count{4096})),
      "file read");
    if (!read.eof() || !read.data().content_equals(file_payload)) {
        throw std::runtime_error("file read returned the wrong result");
    }
    require_success(co_await bounded(file.close()), "file close");
    auto listing = require_success(
      co_await bounded(capabilities.file_system().list(root_path, {})),
      "directory list");
    if (listing.entries().size() != 1) {
        throw std::runtime_error("directory listing returned the wrong size");
    }
    require_success(
      co_await bounded(capabilities.file_system().sync_directory(root_path)),
      "directory sync");

    auto listener = require_success(
      co_await bounded(capabilities.network().listen(
        network_endpoint{conformance_loopback(), 0}, {})),
      "network listen");
    seastar::abort_source accept_abort;
    seastar::abort_source connect_abort;
    auto accepting = listener.accept(accept_abort);
    auto client = require_success(
      co_await bounded(capabilities.network().connect(
        listener.local_endpoint(),
        std::nullopt,
        network_connection_limits{},
        connect_abort)),
      "network connect");
    auto server = require_success(
      co_await bounded(std::move(accepting)), "network accept");

    constexpr std::string_view network_payload = "loopback-contract";
    seastar::abort_source write_abort;
    seastar::abort_source read_abort;
    require_success(
      co_await bounded(
        client.write(conformance_bytes(network_payload), write_abort)),
      "network write");
    auto received = require_success(
      co_await bounded(server.read(byte_count{64}, read_abort)),
      "network read");
    if (received.eof() || !received.data().content_equals(network_payload)) {
        throw std::runtime_error("network read returned the wrong result");
    }
    require_success(client.shutdown_output(), "network output shutdown");
    auto eof = require_success(
      co_await bounded(server.read(byte_count{64}, read_abort)),
      "network EOF read");
    if (!eof.eof() || !eof.data().empty()) {
        throw std::runtime_error("network EOF was not explicit");
    }
    require_success(co_await bounded(client.close()), "client close");
    require_success(co_await bounded(server.close()), "server close");
    require_success(co_await bounded(listener.close()), "listener close");

    seastar::abort_source dns_abort;
    if (dns_expectation) {
        auto resolved = require_success(
          co_await bounded(capabilities.dns().resolve(
            std::move(dns_expectation->query), dns_abort)),
          "in-process DNS");
        if (resolved.answers() != dns_expectation->answers) {
            throw std::runtime_error(
              "in-process DNS returned the wrong result");
        }
    } else {
        auto name = dns_name::make("127.0.0.1");
        if (!name) {
            throw std::runtime_error("numeric DNS name was rejected");
        }
        auto resolved = require_success(
          co_await bounded(capabilities.dns().resolve(
            dns_query{
              .host = std::move(*name),
              .port = 33145,
              .family = dns_address_family::ipv4,
            },
            dns_abort)),
          "numeric DNS");
        if (
          resolved.answers().size() != 1
          || resolved.answers()[0].endpoint.port() != 33145) {
            throw std::runtime_error("numeric DNS returned the wrong result");
        }
    }

    require_success(
      co_await bounded(capabilities.file_system().remove_file(*data_path)),
      "file removal");
    require_success(
      co_await bounded(capabilities.file_system().remove_directory(root_path)),
      "directory removal");
}

} // namespace kwaque::runtime::testing

#endif // KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_REAL_BACKEND_CONFORMANCE_H_
