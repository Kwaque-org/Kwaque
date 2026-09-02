#ifndef KWAQUE_SRC_SIMULATION_FAKE_NETWORK_H_
#define KWAQUE_SRC_SIMULATION_FAKE_NETWORK_H_

#include "src/runtime/network.h"
#include "src/runtime/shard_affinity.h"
#include "src/runtime/time.h"
#include "src/simulation/bandwidth.h"
#include "src/simulation/scheduler.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <type_traits>

namespace kwaque::simulation {

class fault_schedule;

inline constexpr std::uint32_t default_fake_network_listeners{256};
inline constexpr std::uint32_t maximum_fake_network_listeners{4'096};
inline constexpr std::uint32_t default_fake_network_connection_pairs{1'024};
inline constexpr std::uint32_t maximum_fake_network_connection_pairs{16'384};
inline constexpr std::uint32_t default_fake_network_pending_connects{256};
inline constexpr std::uint32_t maximum_fake_network_pending_connects{4'096};
inline constexpr std::uint32_t default_fake_network_backlog_entries{1'024};
inline constexpr std::uint32_t maximum_fake_network_backlog_entries{16'384};
inline constexpr std::uint32_t default_fake_network_operations{4'096};
inline constexpr std::uint32_t maximum_fake_network_operations{65'536};
inline constexpr std::uint32_t default_fake_network_parked_operations{256};
inline constexpr std::uint32_t maximum_fake_network_parked_operations{4'096};
inline constexpr byte_count default_fake_network_direction_bytes{
  std::uint64_t{16} * 1024U * 1024U};
inline constexpr byte_count maximum_fake_network_direction_bytes{
  std::uint64_t{64} * 1024U * 1024U};
inline constexpr std::uint32_t default_fake_network_packets{8'192};
inline constexpr std::uint32_t maximum_fake_network_packets{65'536};
inline constexpr byte_count default_fake_network_packet_logical_bytes{
  std::uint64_t{256} * 1024U * 1024U};
inline constexpr byte_count maximum_fake_network_packet_logical_bytes{
  std::uint64_t{2} * 1024U * 1024U * 1024U};
inline constexpr byte_count default_fake_network_packet_retained_bytes{
  std::uint64_t{256} * 1024U * 1024U};
inline constexpr byte_count maximum_fake_network_packet_retained_bytes{
  std::uint64_t{2} * 1024U * 1024U * 1024U};
inline constexpr std::uint32_t default_fake_network_direction_packets{64};
inline constexpr std::uint32_t maximum_fake_network_direction_packets{1'024};
inline constexpr std::uint32_t default_fake_network_links{1'024};
inline constexpr std::uint32_t maximum_fake_network_links{16'384};
inline constexpr std::uint32_t default_fake_network_address_entries{1'024};
inline constexpr std::uint32_t maximum_fake_network_address_entries{16'384};
inline constexpr std::uint32_t default_fake_network_active_flows{32};
inline constexpr std::uint32_t default_fake_network_controls{256};
inline constexpr std::uint32_t maximum_fake_network_controls{4'096};
inline constexpr std::uint32_t default_fake_network_stop_batch{256};
inline constexpr std::uint32_t maximum_fake_network_stop_batch{1'024};

enum class fake_network_state : std::uint8_t {
    open,
    stopping,
    stopped,
};

struct fake_network_config final {
    std::optional<runtime::network_address> ipv4_source{
      runtime::network_address::ipv4(
        {std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}})};
    std::optional<runtime::network_address> ipv6_source{
      runtime::network_address::ipv6(
        {std::byte{},
         std::byte{},
         std::byte{},
         std::byte{},
         std::byte{},
         std::byte{},
         std::byte{},
         std::byte{},
         std::byte{},
         std::byte{},
         std::byte{},
         std::byte{},
         std::byte{},
         std::byte{},
         std::byte{},
         std::byte{1}})};
    std::uint16_t ephemeral_first{49'152};
    std::uint16_t ephemeral_last{65'535};
    std::uint32_t maximum_listeners{default_fake_network_listeners};
    std::uint32_t maximum_connection_pairs{
      default_fake_network_connection_pairs};
    std::uint32_t maximum_pending_connects{
      default_fake_network_pending_connects};
    std::uint32_t maximum_backlog_entries{default_fake_network_backlog_entries};
    std::uint32_t maximum_operations{default_fake_network_operations};
    std::uint32_t maximum_parked_operations{
      default_fake_network_parked_operations};
    byte_count maximum_direction_bytes{default_fake_network_direction_bytes};
    std::uint32_t maximum_packets{default_fake_network_packets};
    byte_count maximum_packet_logical_bytes{
      default_fake_network_packet_logical_bytes};
    byte_count maximum_packet_retained_bytes{
      default_fake_network_packet_retained_bytes};
    std::uint32_t maximum_direction_packets{
      default_fake_network_direction_packets};
    std::uint32_t maximum_links{default_fake_network_links};
    std::uint32_t maximum_address_entries{default_fake_network_address_entries};
    std::uint32_t maximum_active_flows{default_fake_network_active_flows};
    std::uint32_t maximum_controls{default_fake_network_controls};
    std::uint32_t stop_batch{default_fake_network_stop_batch};
    bandwidth_capacity egress_capacity{bandwidth_capacity::unlimited()};
    bandwidth_capacity link_capacity{bandwidth_capacity::unlimited()};
    bandwidth_capacity ingress_capacity{bandwidth_capacity::unlimited()};
    std::uint64_t latency_seed{0};
    runtime::monotonic_duration bind_latency{1};
    runtime::monotonic_duration connect_latency{1};
    runtime::monotonic_duration incoming_latency{1};
    runtime::monotonic_duration accept_latency{};
    runtime::monotonic_duration latency_min{1};
    runtime::monotonic_duration latency_mean_parameter{1};
    runtime::monotonic_duration interframe_gap{};
    runtime::monotonic_duration reorder_window{};
    runtime::monotonic_duration fin_latency{1};
    runtime::monotonic_duration close_latency{};
};

class fake_network;
class fake_network_test_access;

class fake_connection final {
public:
    fake_connection(fake_connection&& other) noexcept;
    fake_connection& operator=(fake_connection&&) = delete;
    fake_connection(const fake_connection&) = delete;
    fake_connection& operator=(const fake_connection&) = delete;
    ~fake_connection();

    [[nodiscard]] seastar::future<runtime::result<runtime::network_read_result>>
    read(byte_count maximum_bytes, seastar::abort_source& caller_abort);
    [[nodiscard]] seastar::future<runtime::result<void>>
    write(bytes::fragmented_buffer data, seastar::abort_source& caller_abort);

    [[nodiscard]] runtime::network_endpoint local_endpoint() const noexcept;
    [[nodiscard]] runtime::network_endpoint remote_endpoint() const noexcept;
    [[nodiscard]] runtime::network_connection_state state() const noexcept;
    [[nodiscard]] runtime::network_half_state input_state() const noexcept;
    [[nodiscard]] runtime::network_half_state output_state() const noexcept;
    [[nodiscard]] const runtime::network_connection_limits&
    limits() const noexcept {
        owner_.assert_current();
        return limits_;
    }
    [[nodiscard]] runtime::owner_shard owner() const noexcept { return owner_; }

    [[nodiscard]] runtime::result<void> shutdown_input();
    [[nodiscard]] runtime::result<void> shutdown_output();
    void request_abort();
    [[nodiscard]] seastar::future<runtime::result<void>> close();

private:
    friend class fake_network;

    fake_connection(
      fake_network& backend,
      std::uint64_t pair,
      std::uint8_t side,
      runtime::network_connection_limits limits);

    runtime::owner_shard owner_;
    fake_network* backend_{nullptr};
    std::uint64_t pair_{0};
    runtime::network_connection_limits limits_;
    runtime::network_write_admission admission_;
    std::uint8_t side_{0};
    bool moved_from_{false};
};

class fake_listener final {
public:
    fake_listener(fake_listener&& other) noexcept;
    fake_listener& operator=(fake_listener&&) = delete;
    fake_listener(const fake_listener&) = delete;
    fake_listener& operator=(const fake_listener&) = delete;
    ~fake_listener();

    [[nodiscard]] seastar::future<runtime::result<fake_connection>>
    accept(seastar::abort_source& caller_abort);
    [[nodiscard]] runtime::network_endpoint local_endpoint() const noexcept;
    [[nodiscard]] const runtime::network_connection_limits&
    connection_limits() const noexcept;
    [[nodiscard]] runtime::owner_shard owner() const noexcept { return owner_; }
    void request_abort();
    [[nodiscard]] seastar::future<runtime::result<void>> close();

private:
    friend class fake_network;

    fake_listener(fake_network& backend, std::uint64_t listener) noexcept;

    runtime::owner_shard owner_;
    fake_network* backend_{nullptr};
    std::uint64_t listener_{0};
    bool moved_from_{false};
};

class fake_network final : public runtime::shard_affine {
public:
    using connection_type = fake_connection;
    using listener_type = fake_listener;

    [[nodiscard]] static runtime::result<std::unique_ptr<fake_network>> make(
      fake_network_config config,
      scheduler& event_scheduler,
      fault_schedule* faults = nullptr);

    fake_network(const fake_network&) = delete;
    fake_network& operator=(const fake_network&) = delete;
    fake_network(fake_network&&) = delete;
    fake_network& operator=(fake_network&&) = delete;
    ~fake_network();

    [[nodiscard]] seastar::future<runtime::result<fake_connection>> connect(
      runtime::network_endpoint endpoint,
      std::optional<runtime::network_endpoint> local_endpoint,
      runtime::network_connection_limits limits,
      seastar::abort_source& caller_abort);
    [[nodiscard]] seastar::future<runtime::result<fake_listener>> listen(
      runtime::network_endpoint endpoint,
      runtime::network_listen_options options);
    [[nodiscard]] seastar::future<runtime::result<void>>
    partition(runtime::network_address source, runtime::network_address target);
    [[nodiscard]] seastar::future<runtime::result<void>>
    heal(runtime::network_address source, runtime::network_address target);
    [[nodiscard]] seastar::future<runtime::result<void>>
    clog(runtime::network_address source, runtime::network_address target);
    [[nodiscard]] seastar::future<runtime::result<void>>
    unclog(runtime::network_address source, runtime::network_address target);
    [[nodiscard]] seastar::future<runtime::result<void>> set_egress_capacity(
      runtime::network_address address, bandwidth_capacity capacity);
    [[nodiscard]] seastar::future<runtime::result<void>> set_link_capacity(
      runtime::network_address source,
      runtime::network_address target,
      bandwidth_capacity capacity);
    [[nodiscard]] seastar::future<runtime::result<void>> set_ingress_capacity(
      runtime::network_address address, bandwidth_capacity capacity);
    [[nodiscard]] bandwidth_allocation_digest
    allocation_digest() const noexcept;
    [[nodiscard]] std::size_t active_operations() const noexcept;
    void request_abort() noexcept;
    [[nodiscard]] seastar::future<runtime::result<void>> stop();
    [[nodiscard]] fake_network_state state() const noexcept;

private:
    friend class fake_connection;
    friend class fake_listener;
    friend class fake_network_test_access;

    class impl;

    fake_network(
      fake_network_config config,
      scheduler& event_scheduler,
      std::unique_ptr<impl> implementation) noexcept;

    [[nodiscard]] seastar::future<runtime::result<runtime::network_read_result>>
    read(
      std::uint64_t pair,
      std::uint8_t side,
      byte_count maximum_bytes,
      seastar::abort_source& caller_abort);
    [[nodiscard]] seastar::future<runtime::result<void>> write(
      std::uint64_t pair,
      std::uint8_t side,
      bytes::fragmented_buffer data,
      seastar::abort_source& caller_abort,
      runtime::network_write_admission& admission);
    [[nodiscard]] runtime::network_endpoint
    local_endpoint(std::uint64_t pair, std::uint8_t side) const noexcept;
    [[nodiscard]] runtime::network_endpoint
    remote_endpoint(std::uint64_t pair, std::uint8_t side) const noexcept;
    [[nodiscard]] runtime::network_connection_state
    connection_state(std::uint64_t pair, std::uint8_t side) const noexcept;
    [[nodiscard]] runtime::network_half_state
    input_state(std::uint64_t pair, std::uint8_t side) const noexcept;
    [[nodiscard]] runtime::network_half_state
    output_state(std::uint64_t pair, std::uint8_t side) const noexcept;
    [[nodiscard]] runtime::result<void>
    shutdown_input(std::uint64_t pair, std::uint8_t side);
    [[nodiscard]] runtime::result<void>
    shutdown_output(std::uint64_t pair, std::uint8_t side);
    void request_abort(std::uint64_t pair, std::uint8_t side) noexcept;
    [[nodiscard]] seastar::future<runtime::result<void>>
    close_connection(std::uint64_t pair, std::uint8_t side);
    void
    release_connection_handle(std::uint64_t pair, std::uint8_t side) noexcept;
    [[nodiscard]] bool
    connection_movable(std::uint64_t pair, std::uint8_t side) const noexcept;
    [[nodiscard]] bool owner_stopped() const noexcept;
    void
    force_discard_for_test(const runtime::operation_error& failure) noexcept;

    [[nodiscard]] seastar::future<runtime::result<fake_connection>>
    accept(std::uint64_t listener, seastar::abort_source& caller_abort);
    [[nodiscard]] runtime::network_endpoint
    listener_endpoint(std::uint64_t listener) const noexcept;
    [[nodiscard]] const runtime::network_connection_limits&
    listener_limits(std::uint64_t listener) const noexcept;
    void request_listener_abort(std::uint64_t listener) noexcept;
    [[nodiscard]] seastar::future<runtime::result<void>>
    close_listener(std::uint64_t listener);
    void release_listener_handle(std::uint64_t listener) noexcept;
    [[nodiscard]] bool listener_movable(std::uint64_t listener) const noexcept;

    fake_network_config config_;
    scheduler* scheduler_{nullptr};
    std::unique_ptr<impl> impl_;
};

static_assert(runtime::network_connection_contract<fake_connection>);
static_assert(
  runtime::network_listener_contract<fake_listener, fake_connection>);
static_assert(runtime::network_backend<fake_network>);
static_assert(!std::is_move_constructible_v<fake_network>);

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_FAKE_NETWORK_H_
