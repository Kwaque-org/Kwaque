#pragma once

#include "src/admin/admin_server.h"

#include <seastar/http/httpd.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>

#include <sys/socket.h>
#include <sys/un.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

namespace kwaque::admin::detail {

struct admin_server_test_access final {
    static seastar::httpd::http_server_control&
    native_server(admin_server& server) {
        return server.native_server_for_testing();
    }
};

struct leased_endpoint final {
    std::string address;
    std::uint16_t port;

    [[nodiscard]] seastar::socket_address socket() const {
        return seastar::socket_address{
          seastar::net::inet_address{address}, port};
    }
};

// Returns a loopback address that no concurrent test process uses, with a
// fixed port, so a server never binds a port a probe released. Linux routes
// all of 127.0.0.0/8 to loopback. The lease is an abstract Unix socket named
// after the address, shared with the Python smoke harness and released by the
// kernel at process exit. Candidates come from the process ID, not a random
// source, and 127.0.0.0/16 is left to developer brokers.
inline leased_endpoint lease_loopback_endpoint(std::uint16_t port = 9644) {
    static std::vector<int> leases;
    constexpr unsigned octets = 254U * 256U * 254U;
    const auto start = static_cast<unsigned>(::getpid()) * 7919U;
    for (unsigned attempt = 0; attempt < 4096U; ++attempt) {
        const unsigned index = (start + attempt) % octets;
        const std::string address
          = "127." + std::to_string(1U + index % 254U) + "."
            + std::to_string((index / 254U) % 256U) + "."
            + std::to_string(1U + (index / (254U * 256U)) % 254U);
        const std::string name = "kwaque-test-endpoint-" + address;
        sockaddr_un lease{};
        lease.sun_family = AF_UNIX;
        if (name.size() + 1U > sizeof(lease.sun_path)) {
            throw std::length_error("lease name exceeds the socket path");
        }
        std::memcpy(lease.sun_path + 1, name.data(), name.size());
        const int descriptor = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (descriptor < 0) {
            throw std::runtime_error("cannot create an endpoint lease");
        }
        const auto length = static_cast<socklen_t>(
          offsetof(sockaddr_un, sun_path) + 1U + name.size());
        if (
          ::bind(descriptor, reinterpret_cast<const sockaddr*>(&lease), length)
          == 0) {
            leases.push_back(descriptor);
            return {address, port};
        }
        ::close(descriptor);
    }
    throw std::runtime_error("unable to lease a loopback endpoint");
}

} // namespace kwaque::admin::detail
