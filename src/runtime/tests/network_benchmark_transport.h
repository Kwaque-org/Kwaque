#ifndef KWAQUE_SRC_RUNTIME_TESTS_NETWORK_BENCHMARK_TRANSPORT_H_
#define KWAQUE_SRC_RUNTIME_TESTS_NETWORK_BENCHMARK_TRANSPORT_H_

#include "src/base/allocation.h"

#include <seastar/core/iostream.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/api.hh>
#include <seastar/net/stack.hh>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <system_error>
#include <utility>

namespace kwaque::runtime::testing {

namespace controlled_transport_detail {

class direction final : public seastar::enable_lw_shared_from_this<direction> {
public:
    [[nodiscard]] seastar::future<> push(seastar::temporary_buffer<char> data) {
        if (aborted_ || output_closed_) {
            return seastar::make_exception_future<>(failure_);
        }
        return queue_.push_eventually(std::move(data))
          .finally(
            [keep = shared_from_this()] noexcept { static_cast<void>(keep); });
    }

    [[nodiscard]] seastar::future<seastar::temporary_buffer<char>> pop() {
        if (aborted_) {
            return seastar::make_exception_future<
              seastar::temporary_buffer<char>>(failure_);
        }
        if (eof_) {
            return seastar::make_ready_future<
              seastar::temporary_buffer<char>>();
        }
        return queue_.pop_eventually().then(
          [keep = shared_from_this()](seastar::temporary_buffer<char> data) {
              if (data.empty()) {
                  keep->eof_ = true;
                  keep->notify_shutdown();
              }
              return data;
          });
    }

    void abort() noexcept {
        if (aborted_) {
            return;
        }
        aborted_ = true;
        queue_.abort(failure_);
        notify_shutdown();
        if (close_done_ && !close_done_->available()) {
            close_done_->set_value();
        }
    }

    [[nodiscard]] seastar::future<> close() {
        if (aborted_) {
            return seastar::make_ready_future<>();
        }
        if (output_closed_) {
            return close_done_->get_shared_future();
        }
        close_done_.emplace();
        output_closed_ = true;
        auto closing = queue_.push_eventually(seastar::temporary_buffer<char>{})
                         .then_wrapped([keep = shared_from_this()](
                                         seastar::future<> pushed) noexcept {
                             try {
                                 pushed.get();
                                 if (!keep->close_done_->available()) {
                                     keep->close_done_->set_value();
                                 }
                             } catch (...) {
                                 if (!keep->close_done_->available()) {
                                     keep->close_done_->set_exception(
                                       std::current_exception());
                                 }
                             }
                         });
        static_cast<void>(closing);
        return close_done_->get_shared_future();
    }

    [[nodiscard]] seastar::future<> wait_input_shutdown() {
        if (aborted_ || eof_) {
            return seastar::make_ready_future<>();
        }
        if (!shutdown_done_) {
            shutdown_done_.emplace();
        }
        return shutdown_done_->get_shared_future();
    }

private:
    void notify_shutdown() noexcept {
        if (shutdown_done_ && !shutdown_done_->available()) {
            shutdown_done_->set_value();
        }
    }

    seastar::queue<seastar::temporary_buffer<char>> queue_{1};
    std::exception_ptr failure_ = std::make_exception_ptr(
      std::system_error(std::make_error_code(std::errc::broken_pipe)));
    std::optional<seastar::shared_promise<>> close_done_;
    std::optional<seastar::shared_promise<>> shutdown_done_;
    bool output_closed_{false};
    bool eof_{false};
    bool aborted_{false};
};

class source final : public seastar::data_source_impl {
public:
    explicit source(seastar::lw_shared_ptr<direction> incoming) noexcept
      : incoming_(std::move(incoming)) {}

    seastar::future<seastar::temporary_buffer<char>> get() override {
        return incoming_->pop();
    }
    seastar::future<> close() override {
        incoming_->abort();
        return seastar::make_ready_future<>();
    }

private:
    seastar::lw_shared_ptr<direction> incoming_;
};

class sink final : public seastar::data_sink_impl {
public:
    sink(
      seastar::lw_shared_ptr<direction> outgoing,
      seastar::lw_shared_ptr<direction> incoming) noexcept
      : outgoing_(std::move(outgoing))
      , incoming_(std::move(incoming)) {}

    seastar::future<>
    put(std::span<seastar::temporary_buffer<char>> buffers) override {
        std::size_t bytes = 0;
        for (const auto& buffer : buffers) {
            if (buffer.size() > maximum_contiguous_allocation_bytes - bytes) {
                throw std::length_error(
                  "controlled transport packet exceeds capacity");
            }
            bytes += buffer.size();
        }
        if (bytes == 0) {
            for (auto& buffer : buffers) {
                buffer = {};
            }
            return seastar::make_ready_future<>();
        }
        seastar::temporary_buffer<char> owned{bytes};
        std::size_t offset = 0;
        for (const auto& buffer : buffers) {
            if (!buffer.empty()) {
                std::memcpy(
                  owned.get_write() + offset, buffer.get(), buffer.size());
                offset += buffer.size();
            }
        }
        for (auto& buffer : buffers) {
            buffer = {};
        }
        return outgoing_->push(std::move(owned));
    }
    seastar::future<> close() override { return outgoing_->close(); }
    std::size_t buffer_size() const noexcept override {
        return maximum_contiguous_allocation_bytes;
    }
    bool can_batch_flushes() const noexcept override { return true; }
    void on_batch_flush_error() noexcept override { incoming_->abort(); }

private:
    seastar::lw_shared_ptr<direction> outgoing_;
    seastar::lw_shared_ptr<direction> incoming_;
};

class socket final : public seastar::net::connected_socket_impl {
public:
    socket(
      seastar::lw_shared_ptr<direction> outgoing,
      seastar::lw_shared_ptr<direction> incoming,
      seastar::socket_address local,
      seastar::socket_address remote) noexcept
      : outgoing_(std::move(outgoing))
      , incoming_(std::move(incoming))
      , local_(local)
      , remote_(remote) {}

    seastar::data_source source() override {
        return seastar::data_source{
          std::make_unique<controlled_transport_detail::source>(incoming_)};
    }
    seastar::data_sink sink() override {
        return seastar::data_sink{
          std::make_unique<controlled_transport_detail::sink>(
            outgoing_, incoming_)};
    }
    void shutdown_input() override { incoming_->abort(); }
    void shutdown_output() override { outgoing_->abort(); }
    void set_nodelay(bool value) override { nodelay_ = value; }
    bool get_nodelay() const override { return nodelay_; }
    void set_keepalive(bool value) override { keepalive_ = value; }
    bool get_keepalive() const override { return keepalive_; }
    void set_keepalive_parameters(
      const seastar::net::keepalive_params& value) override {
        keepalive_parameters_ = value;
    }
    seastar::net::keepalive_params get_keepalive_parameters() const override {
        return keepalive_parameters_;
    }
    void set_sockopt(int, int, const void*, std::size_t) override {
        throw std::system_error(
          std::make_error_code(std::errc::operation_not_supported));
    }
    int get_sockopt(int, int, void*, std::size_t) const override {
        throw std::system_error(
          std::make_error_code(std::errc::operation_not_supported));
    }
    seastar::socket_address local_address() const noexcept override {
        return local_;
    }
    seastar::socket_address remote_address() const noexcept override {
        return remote_;
    }
    seastar::future<> wait_input_shutdown() override {
        return incoming_->wait_input_shutdown();
    }

private:
    seastar::lw_shared_ptr<direction> outgoing_;
    seastar::lw_shared_ptr<direction> incoming_;
    seastar::socket_address local_;
    seastar::socket_address remote_;
    seastar::net::keepalive_params keepalive_parameters_
      = seastar::net::tcp_keepalive_params{
        std::chrono::seconds{0}, std::chrono::seconds{0}, 0};
    bool nodelay_{false};
    bool keepalive_{false};
};

} // namespace controlled_transport_detail

struct controlled_socket_pair final {
    seastar::connected_socket client;
    seastar::connected_socket server;
};

[[nodiscard]] inline controlled_socket_pair make_controlled_pair() {
    auto to_server
      = seastar::make_lw_shared<controlled_transport_detail::direction>();
    auto to_client
      = seastar::make_lw_shared<controlled_transport_detail::direction>();
    const auto client = seastar::make_ipv4_address({0x7f000001U, 1});
    const auto server = seastar::make_ipv4_address({0x7f000001U, 2});
    return {
      seastar::connected_socket{
        std::make_unique<controlled_transport_detail::socket>(
          to_server, to_client, client, server)},
      seastar::connected_socket{
        std::make_unique<controlled_transport_detail::socket>(
          to_client, to_server, server, client)},
    };
}

} // namespace kwaque::runtime::testing

#endif // KWAQUE_SRC_RUNTIME_TESTS_NETWORK_BENCHMARK_TRANSPORT_H_
