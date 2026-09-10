#pragma once

#include "src/admin/admin_limits.h"
#include "src/runtime/shard_affinity.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

namespace seastar::httpd {
class http_server_control;
}

namespace kwaque::admin {

namespace detail {
struct admin_server_test_access;
}

class admin_server final : public runtime::shard_affine {
public:
    admin_server();
    ~admin_server();

    admin_server(const admin_server&) = delete;
    admin_server& operator=(const admin_server&) = delete;
    admin_server(admin_server&&) = delete;
    admin_server& operator=(admin_server&&) = delete;

    // A supplied abort source belongs to the calling shard and must outlive
    // the returned future.
    [[nodiscard]] seastar::future<> start(
      std::string address,
      std::uint16_t port,
      unsigned shard_count,
      const seastar::abort_source* startup_abort = nullptr,
      std::uint64_t reservation_bytes = admin_reservation_bytes);
    [[nodiscard]] seastar::future<>
    mark_ready(std::chrono::steady_clock::duration startup_duration);
    [[nodiscard]] seastar::future<> begin_shutdown();
    [[nodiscard]] seastar::future<> stop();

private:
    friend struct detail::admin_server_test_access;
    seastar::httpd::http_server_control& native_server_for_testing();
    [[nodiscard]] seastar::future<> stop_once();

    class impl;
    std::unique_ptr<impl> impl_;
};

} // namespace kwaque::admin
