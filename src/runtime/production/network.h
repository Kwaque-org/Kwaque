#ifndef KWAQUE_SRC_RUNTIME_PRODUCTION_NETWORK_H_
#define KWAQUE_SRC_RUNTIME_PRODUCTION_NETWORK_H_

#include "src/runtime/network.h"
#include "src/runtime/operation_statistics.h"
#include "src/runtime/shard_affinity.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/net/api.hh>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>

namespace kwaque::runtime::production {

class network_test_access;

class connection final {
public:
    connection(connection&& other) noexcept;
    connection& operator=(connection&&) = delete;
    connection(const connection&) = delete;
    connection& operator=(const connection&) = delete;
    ~connection();

    [[nodiscard]] seastar::future<result<network_read_result>>
    read(byte_count maximum_bytes, seastar::abort_source& caller_abort);
    [[nodiscard]] seastar::future<result<void>>
    write(bytes::fragmented_buffer data, seastar::abort_source& caller_abort);

    [[nodiscard]] network_endpoint local_endpoint() const noexcept;
    [[nodiscard]] network_endpoint remote_endpoint() const noexcept;
    [[nodiscard]] network_connection_state state() const noexcept;
    [[nodiscard]] network_half_state input_state() const noexcept;
    [[nodiscard]] network_half_state output_state() const noexcept;
    [[nodiscard]] const network_connection_limits& limits() const noexcept;
    [[nodiscard]] owner_shard owner() const noexcept { return owner_; }
    [[nodiscard]] operation_statistics_snapshot statistics() const noexcept {
        owner_.assert_current();
        return statistics_->snapshot();
    }

    [[nodiscard]] result<void> shutdown_input();
    [[nodiscard]] result<void> shutdown_output();
    void request_abort();
    [[nodiscard]] seastar::future<result<void>> close();

private:
    friend class listener;
    friend class network;
    friend class network_test_access;

    connection(
      seastar::connected_socket native,
      network_endpoint local,
      network_endpoint remote,
      network_connection_limits limits,
      operation_statistics_owner statistics = {});

    [[nodiscard]] static owner_shard prepare_move(connection& other) noexcept;
    [[nodiscard]] std::optional<operation_error> input_rejection() const;
    [[nodiscard]] std::optional<operation_error> output_rejection() const;
    [[nodiscard]] seastar::future<result<void>> write_acquired(
      bytes::fragmented_buffer data,
      network_write_admission::reservation reservation,
      seastar::gate::holder holder,
      seastar::semaphore_units<> serialization,
      operation_statistics::reservation metric);
    [[nodiscard]] seastar::future<result<void>> write_general(
      bytes::fragmented_buffer data,
      seastar::abort_source& caller_abort,
      network_write_admission::reservation reservation,
      seastar::gate::holder holder,
      operation_statistics::reservation metric);
    [[nodiscard]] seastar::future<result<void>>
    flush_preceding_batch_after_cancellation();
    [[nodiscard]] seastar::future<result<void>> close_once();

    owner_shard owner_;
    operation_statistics_owner statistics_owner_;
    operation_statistics* statistics_;
    seastar::connected_socket native_;
    seastar::input_stream<char> input_;
    seastar::output_stream<char> output_;
    network_endpoint local_;
    network_endpoint remote_;
    network_connection_limits limits_;
    network_write_admission admission_;
    seastar::semaphore write_serializer_{1};
    seastar::gate input_operations_;
    seastar::gate output_operations_;
    std::optional<seastar::shared_promise<result<void>>> close_done_;
    std::uint64_t unflushed_bytes_{0};
    network_connection_state state_{network_connection_state::open};
    network_half_state input_state_{network_half_state::open};
    network_half_state output_state_{network_half_state::open};
    bool read_in_flight_{false};
    bool abort_requested_{false};
    bool moved_from_{false};
};

class listener final {
public:
    listener(listener&& other) noexcept;
    listener& operator=(listener&&) = delete;
    listener(const listener&) = delete;
    listener& operator=(const listener&) = delete;
    ~listener();

    [[nodiscard]] seastar::future<result<connection>>
    accept(seastar::abort_source& caller_abort);
    [[nodiscard]] network_endpoint local_endpoint() const noexcept;
    [[nodiscard]] const network_connection_limits&
    connection_limits() const noexcept;
    [[nodiscard]] owner_shard owner() const noexcept { return owner_; }
    [[nodiscard]] operation_statistics_snapshot statistics() const noexcept {
        owner_.assert_current();
        return statistics_->snapshot();
    }
    void request_abort();
    [[nodiscard]] seastar::future<result<void>> close();

private:
    friend class network;

    listener(
      seastar::server_socket native,
      network_endpoint local,
      network_connection_limits limits,
      operation_statistics_owner statistics = {});

    [[nodiscard]] static owner_shard prepare_move(listener& other) noexcept;

    owner_shard owner_;
    operation_statistics_owner statistics_owner_;
    operation_statistics* statistics_;
    seastar::server_socket native_;
    network_endpoint local_;
    network_connection_limits limits_;
    seastar::gate accepts_;
    std::optional<seastar::shared_promise<result<void>>> close_done_;
    bool aborted_{false};
    bool closing_{false};
    bool closed_{false};
    bool accept_in_flight_{false};
    bool moved_from_{false};
};

class network final : public shard_affine {
public:
    using connection_type = connection;
    using listener_type = listener;

    network()
      : statistics_(&statistics_owner_.get()) {}
    explicit network(operation_statistics_owner statistics) noexcept
      : statistics_owner_(std::move(statistics))
      , statistics_(&statistics_owner_.get()) {}

    [[nodiscard]] seastar::future<result<connection>> connect(
      network_endpoint endpoint,
      std::optional<network_endpoint> local_endpoint,
      network_connection_limits limits,
      seastar::abort_source& caller_abort);
    [[nodiscard]] seastar::future<result<listener>>
    listen(network_endpoint endpoint, network_listen_options options);

    [[nodiscard]] operation_statistics_snapshot statistics() const noexcept {
        assert_current();
        return statistics_->snapshot();
    }

private:
    operation_statistics_owner statistics_owner_;
    operation_statistics* statistics_;
};

static_assert(kwaque::runtime::network_backend<network>);

} // namespace kwaque::runtime::production

#endif // KWAQUE_SRC_RUNTIME_PRODUCTION_NETWORK_H_
