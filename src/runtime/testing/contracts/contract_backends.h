#ifndef KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_CONTRACT_BACKENDS_H_
#define KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_CONTRACT_BACKENDS_H_

#include "src/runtime/environment.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

namespace kwaque::runtime::testing {

namespace detail {

template<typename T>
seastar::future<result<T>> unavailable(operation_kind operation) {
    result<T> outcome = failure(operation_error{errc::unavailable, operation});
    return seastar::make_ready_future<result<T>>(std::move(outcome));
}

inline seastar::future<result<void>> success() {
    return seastar::make_ready_future<result<void>>(result<void>{});
}

inline network_endpoint loopback(std::uint16_t port) noexcept {
    return network_endpoint{
      network_address::ipv4(
        {std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}}),
      port};
}

} // namespace detail

struct contract_monotonic_clock final {
    static monotonic_time now() noexcept { return monotonic_time{100}; }
};

struct contract_wall_clock final {
    static wall_time now() noexcept { return wall_time{1'000}; }
};

class contract_timer final {
public:
    seastar::future<result<void>>
    sleep_until(monotonic_time, seastar::abort_source&) {
        return detail::unavailable<void>(operation_kind::timer);
    }
    void request_abort() noexcept {}
    seastar::future<result<void>> stop() { return detail::success(); }
};

class contract_random final {
public:
    std::uint64_t next_u64() noexcept { return next_++; }

private:
    std::uint64_t next_{1};
};

class contract_file_system final {
public:
    seastar::future<result<file>> open(file_path, file_open_options) {
        return detail::unavailable<file>(operation_kind::file);
    }
    seastar::future<result<bool>> exists(file_path) {
        return seastar::make_ready_future<result<bool>>(result<bool>{false});
    }
    seastar::future<result<file_status>> stat(file_path) {
        return seastar::make_ready_future<result<file_status>>(
          result<file_status>{file_status{file_kind::regular, byte_count{}}});
    }
    seastar::future<result<directory_listing>>
    list(file_path, directory_listing_limits limits) {
        auto listing = directory_listing::make(
          seastar::chunked_vector<directory_entry>{}, limits);
        return seastar::make_ready_future<result<directory_listing>>(
          std::move(listing));
    }
    seastar::future<result<void>> create_directories(file_path) {
        return detail::success();
    }
    seastar::future<result<void>> remove_file(file_path) {
        return detail::success();
    }
    seastar::future<result<void>> remove_directory(file_path) {
        return detail::success();
    }
    seastar::future<result<void>> rename(file_path, file_path) {
        return detail::success();
    }
    seastar::future<result<void>> sync_directory(file_path) {
        return detail::success();
    }
};

class contract_connection final {
public:
    contract_connection(
      network_endpoint local,
      network_endpoint remote,
      network_connection_limits limits) noexcept
      : local_(local)
      , remote_(remote)
      , limits_(limits)
      , write_admission_(limits) {}
    contract_connection(contract_connection&&) noexcept = default;
    contract_connection& operator=(contract_connection&&) = delete;
    contract_connection(const contract_connection&) = delete;
    contract_connection& operator=(const contract_connection&) = delete;

    seastar::future<result<network_read_result>>
    read(byte_count maximum_bytes, seastar::abort_source& abort_source) {
        if (auto valid = validate_network_read_limit(maximum_bytes); !valid) {
            result<network_read_result> outcome = failure(valid.error());
            return seastar::make_ready_future<result<network_read_result>>(
              std::move(outcome));
        }
        if (abort_source.abort_requested() || abort_requested_) {
            result<network_read_result> outcome = failure(
              operation_error{errc::aborted, operation_kind::network});
            return seastar::make_ready_future<result<network_read_result>>(
              std::move(outcome));
        }
        if (
          state_ != network_connection_state::open
          || input_state_ == network_half_state::shut_down) {
            result<network_read_result> outcome = failure(
              operation_error{errc::closed, operation_kind::network});
            return seastar::make_ready_future<result<network_read_result>>(
              std::move(outcome));
        }
        if (controlled_io_) {
            if (pending_read_) {
                result<network_read_result> outcome = failure(
                  operation_error{errc::unavailable, operation_kind::network});
                return seastar::make_ready_future<result<network_read_result>>(
                  std::move(outcome));
            }
            pending_read_.emplace();
            pending_read_limit_ = maximum_bytes;
            return pending_read_->get_future();
        }
        auto outcome = network_read_result::make(
          bytes::fragmented_buffer{}, true, maximum_bytes);
        return seastar::make_ready_future<result<network_read_result>>(
          std::move(outcome));
    }
    seastar::future<result<void>>
    write(bytes::fragmented_buffer data, seastar::abort_source& abort_source) {
        if (abort_source.abort_requested() || abort_requested_) {
            result<void> outcome = failure(
              operation_error{errc::aborted, operation_kind::network});
            return seastar::make_ready_future<result<void>>(std::move(outcome));
        }
        if (
          state_ != network_connection_state::open
          || output_state_ == network_half_state::shut_down) {
            result<void> outcome = failure(
              operation_error{errc::closed, operation_kind::network});
            return seastar::make_ready_future<result<void>>(std::move(outcome));
        }
        if (auto valid = validate_network_write(data, limits_); !valid) {
            result<void> outcome = failure(valid.error());
            return seastar::make_ready_future<result<void>>(std::move(outcome));
        }
        if (!controlled_io_) {
            return detail::success();
        }
        auto reservation = write_admission_.try_acquire(data.size());
        if (!reservation) {
            result<void> outcome = failure(
              operation_error{errc::queue_full, operation_kind::network});
            return seastar::make_ready_future<result<void>>(std::move(outcome));
        }
        pending_write pending{
          .data = std::move(data),
          .reservation = std::move(*reservation),
        };
        auto completion = pending.completion.get_future();
        pending_writes_.push_back(std::move(pending));
        return completion;
    }
    network_endpoint local_endpoint() const noexcept { return local_; }
    network_endpoint remote_endpoint() const noexcept { return remote_; }
    network_connection_state state() const noexcept { return state_; }
    network_half_state input_state() const noexcept { return input_state_; }
    network_half_state output_state() const noexcept { return output_state_; }
    const network_connection_limits& limits() const noexcept { return limits_; }
    owner_shard owner() const noexcept { return owner_; }
    result<void> shutdown_input() {
        if (state_ != network_connection_state::open) {
            return failure(
              operation_error{errc::closed, operation_kind::network});
        }
        input_state_ = network_half_state::shut_down;
        fail_pending_read(errc::closed);
        return {};
    }
    result<void> shutdown_output() {
        if (state_ != network_connection_state::open) {
            return failure(
              operation_error{errc::closed, operation_kind::network});
        }
        output_state_ = network_half_state::shut_down;
        fail_pending_writes(errc::closed);
        return {};
    }
    void request_abort() {
        abort_requested_ = true;
        fail_pending_read(errc::aborted);
        fail_pending_writes(errc::aborted);
    }
    seastar::future<result<void>> close() {
        if (state_ == network_connection_state::closed) {
            return detail::success();
        }
        state_ = network_connection_state::closed;
        input_state_ = network_half_state::shut_down;
        output_state_ = network_half_state::shut_down;
        fail_pending_read(errc::closed);
        fail_pending_writes(errc::closed);
        return detail::success();
    }

    void enable_controlled_io() noexcept { controlled_io_ = true; }
    [[nodiscard]] bool read_pending() const noexcept {
        return pending_read_.has_value();
    }
    [[nodiscard]] std::size_t pending_write_count() const noexcept {
        return pending_writes_.size();
    }
    [[nodiscard]] byte_count pending_write_bytes() const {
        return write_admission_.pending_bytes();
    }
    [[nodiscard]] bool pending_write_content_equals(
      std::size_t index, std::string_view expected) const noexcept {
        return index < pending_writes_.size()
               && pending_writes_[index].data.content_equals(expected);
    }
    [[nodiscard]] bool complete_read(bytes::fragmented_buffer data, bool eof) {
        if (!pending_read_) {
            return false;
        }
        auto outcome = network_read_result::make(
          std::move(data), eof, pending_read_limit_);
        auto completion = std::move(*pending_read_);
        pending_read_.reset();
        pending_read_limit_ = byte_count{};
        completion.set_value(std::move(outcome));
        return true;
    }
    [[nodiscard]] bool complete_next_write() {
        if (pending_writes_.empty()) {
            return false;
        }
        auto pending = std::move(pending_writes_.front());
        pending_writes_.pop_front();
        pending.completion.set_value(result<void>{});
        return true;
    }

private:
    struct pending_write final {
        bytes::fragmented_buffer data;
        network_write_admission::reservation reservation;
        seastar::promise<result<void>> completion;
    };

    void fail_pending_read(errc code) {
        if (!pending_read_) {
            return;
        }
        auto completion = std::move(*pending_read_);
        pending_read_.reset();
        pending_read_limit_ = byte_count{};
        result<network_read_result> outcome = failure(
          operation_error{code, operation_kind::network});
        completion.set_value(std::move(outcome));
    }

    void fail_pending_writes(errc code) {
        while (!pending_writes_.empty()) {
            auto pending = std::move(pending_writes_.front());
            pending_writes_.pop_front();
            result<void> outcome = failure(
              operation_error{code, operation_kind::network});
            pending.completion.set_value(std::move(outcome));
        }
    }

    owner_shard owner_;
    network_endpoint local_;
    network_endpoint remote_;
    network_connection_limits limits_;
    network_write_admission write_admission_;
    std::optional<seastar::promise<result<network_read_result>>> pending_read_;
    std::deque<pending_write> pending_writes_;
    byte_count pending_read_limit_{};
    network_connection_state state_{network_connection_state::open};
    network_half_state input_state_{network_half_state::open};
    network_half_state output_state_{network_half_state::open};
    bool abort_requested_{false};
    bool controlled_io_{false};
};

class contract_listener final {
public:
    contract_listener(
      network_endpoint local, network_connection_limits limits) noexcept
      : local_(local)
      , limits_(limits) {}
    contract_listener(contract_listener&&) noexcept = default;
    contract_listener& operator=(contract_listener&&) = delete;
    contract_listener(const contract_listener&) = delete;
    contract_listener& operator=(const contract_listener&) = delete;

    seastar::future<result<contract_connection>>
    accept(seastar::abort_source& abort_source) {
        if (closed_) {
            result<contract_connection> outcome = failure(
              operation_error{errc::closed, operation_kind::network});
            return seastar::make_ready_future<result<contract_connection>>(
              std::move(outcome));
        }
        if (abort_source.abort_requested() || abort_requested_) {
            result<contract_connection> outcome = failure(
              operation_error{errc::aborted, operation_kind::network});
            return seastar::make_ready_future<result<contract_connection>>(
              std::move(outcome));
        }
        return detail::unavailable<contract_connection>(
          operation_kind::network);
    }
    network_endpoint local_endpoint() const noexcept { return local_; }
    const network_connection_limits& connection_limits() const noexcept {
        return limits_;
    }
    owner_shard owner() const noexcept { return owner_; }
    void request_abort() { abort_requested_ = true; }
    seastar::future<result<void>> close() {
        closed_ = true;
        return detail::success();
    }

private:
    owner_shard owner_;
    network_endpoint local_;
    network_connection_limits limits_;
    bool abort_requested_{false};
    bool closed_{false};
};

class contract_network final {
public:
    using connection_type = contract_connection;
    using listener_type = contract_listener;

    seastar::future<result<connection_type>> connect(
      network_endpoint remote,
      std::optional<network_endpoint> local,
      network_connection_limits limits,
      seastar::abort_source& abort_source) {
        if (auto valid = limits.validate(); !valid) {
            result<connection_type> outcome = failure(valid.error());
            return seastar::make_ready_future<result<connection_type>>(
              std::move(outcome));
        }
        if (abort_source.abort_requested()) {
            result<connection_type> outcome = failure(
              operation_error{errc::aborted, operation_kind::network});
            return seastar::make_ready_future<result<connection_type>>(
              std::move(outcome));
        }
        return seastar::make_ready_future<result<connection_type>>(
          result<connection_type>{connection_type{
            local.value_or(detail::loopback(0)), remote, limits}});
    }
    seastar::future<result<listener_type>>
    listen(network_endpoint local, network_listen_options options) {
        if (auto valid = options.validate(); !valid) {
            result<listener_type> outcome = failure(valid.error());
            return seastar::make_ready_future<result<listener_type>>(
              std::move(outcome));
        }
        return seastar::make_ready_future<result<listener_type>>(
          result<listener_type>{
            listener_type{local, options.connection_limits}});
    }
    owner_shard owner() const noexcept { return owner_; }

private:
    owner_shard owner_;
};

class contract_dns final {
public:
    seastar::future<result<dns_result>>
    resolve(dns_query query, seastar::abort_source& abort_source) {
        if (stopped_) {
            result<dns_result> outcome = failure(
              operation_error{errc::closed, operation_kind::dns});
            return seastar::make_ready_future<result<dns_result>>(
              std::move(outcome));
        }
        if (abort_source.abort_requested() || abort_requested_) {
            result<dns_result> outcome = failure(
              operation_error{errc::aborted, operation_kind::dns});
            return seastar::make_ready_future<result<dns_result>>(
              std::move(outcome));
        }
        auto numeric = resolve_numeric(query);
        if (!numeric) {
            result<dns_result> outcome = failure(numeric.error());
            return seastar::make_ready_future<result<dns_result>>(
              std::move(outcome));
        }
        if (!*numeric) {
            return detail::unavailable<dns_result>(operation_kind::dns);
        }
        std::vector<dns_answer> answers;
        answers.push_back(
          dns_answer{.endpoint = **numeric, .ttl = monotonic_duration{0}});
        auto outcome = dns_result::make(std::move(answers), 1);
        return seastar::make_ready_future<result<dns_result>>(
          std::move(outcome));
    }
    owner_shard owner() const noexcept { return owner_; }
    void request_abort() { abort_requested_ = true; }
    seastar::future<result<void>> stop() {
        stopped_ = true;
        return detail::success();
    }

private:
    owner_shard owner_;
    bool abort_requested_{false};
    bool stopped_{false};
};

class contract_fault_injector final {
public:
    result<fault_decision> evaluate(const fault_request&) noexcept {
        return fault_decision{};
    }
};

class contract_backend_common {
public:
    using monotonic_clock = contract_monotonic_clock;
    using wall_clock = contract_wall_clock;
    using timer_type = contract_timer;
    using random_type = contract_random;
    using file_system_type = contract_file_system;
    using network_type = contract_network;
    using dns_type = contract_dns;

    ~contract_backend_common() {
        lifetime_.close().get();
        dns_.stop().get();
        timer_.stop().get();
    }

    owner_shard owner() const noexcept { return lifetime_.owner(); }
    runtime_lifetime& lifetime() noexcept { return lifetime_; }
    timer_type& timer() noexcept { return timer_; }
    random_type& random() noexcept { return random_; }
    file_system_type& file_system() noexcept { return file_system_; }
    network_type& network() noexcept { return network_; }
    dns_type& dns() noexcept { return dns_; }

private:
    runtime_lifetime lifetime_;
    timer_type timer_;
    random_type random_;
    file_system_type file_system_;
    network_type network_;
    dns_type dns_;
};

class production_shaped_backend final : public contract_backend_common {
public:
    static constexpr bool faults_enabled = false;
};

class deterministic_shaped_backend final : public contract_backend_common {
public:
    using fault_injector_type = contract_fault_injector;
    static constexpr bool faults_enabled = true;

    fault_injector_type& faults() noexcept { return faults_; }

private:
    fault_injector_type faults_;
};

static_assert(runtime_backend<production_shaped_backend>);
static_assert(runtime_backend<deterministic_shaped_backend>);

} // namespace kwaque::runtime::testing

#endif // KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_CONTRACT_BACKENDS_H_
