#pragma once

#include <seastar/core/future.hh>

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

namespace kwaque::admin {

class admin_server final {
public:
    admin_server();
    ~admin_server();

    admin_server(const admin_server&) = delete;
    admin_server& operator=(const admin_server&) = delete;
    admin_server(admin_server&&) = delete;
    admin_server& operator=(admin_server&&) = delete;

    [[nodiscard]] seastar::future<>
    start(std::string address, std::uint16_t port, unsigned shard_count);
    void
    mark_ready(std::chrono::steady_clock::duration startup_duration) noexcept;
    void begin_shutdown() noexcept;
    [[nodiscard]] seastar::future<> stop();

    [[nodiscard]] bool live() const noexcept;
    [[nodiscard]] bool ready() const noexcept;

private:
    class impl;
    std::unique_ptr<impl> impl_;
};

} // namespace kwaque::admin
