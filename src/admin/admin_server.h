#pragma once

#include "src/admin/admin_limits.h"
#include "src/base/readiness.h"
#include "src/runtime/shard_affinity.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace seastar::httpd {
class http_server_control;
}

namespace kwaque::admin {

namespace detail {
struct admin_server_test_access;
// Every shard's admin state, owned by the server.
struct admin_shards;
} // namespace detail

// Holds or releases a readiness reason on the shard that calls it. The broker
// is to hand storage and the range engine a callback that calls one, so they
// never depend on admin.
class readiness_reporter final {
public:
    void operator()(readiness_reason reason, bool held) const noexcept;

private:
    friend class admin_server;

    explicit readiness_reporter(detail::admin_shards& shards) noexcept
      : shards_(&shards) {}

    detail::admin_shards* shards_;
};

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
    // The start finished: the broker is ready unless a reason is held.
    [[nodiscard]] seastar::future<>
    mark_started(std::chrono::steady_clock::duration startup_duration);
    [[nodiscard]] seastar::future<> begin_shutdown();

    // Valid on every shard from the start until the stop.
    [[nodiscard]] readiness_reporter reporter();
    // Called on this shard at each change of readiness or of the reasons held
    // across the broker, from the start until the stop.
    void observe_readiness(
      std::function<void(bool ready, readiness_reasons reasons)> observer);
    [[nodiscard]] seastar::future<> stop();

private:
    friend struct detail::admin_server_test_access;
    seastar::httpd::http_server_control& native_server_for_testing();
    // Each shard's readiness and the reasons it last heard, in shard order.
    [[nodiscard]] seastar::future<
      std::vector<std::pair<bool, readiness_reasons>>>
    readiness_by_shard_for_testing();
    [[nodiscard]] seastar::future<> stop_once();

    class impl;
    std::unique_ptr<impl> impl_;
};

} // namespace kwaque::admin
