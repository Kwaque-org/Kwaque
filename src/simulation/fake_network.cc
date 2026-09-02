#include "src/simulation/fake_network.h"

#include "src/base/invariant.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/runtime/random.h"
#include "src/simulation/deterministic_random.h"
#include "src/simulation/fault_schedule.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/chunked_vector.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/util/optimized_optional.hh>

#include <absl/container/btree_map.h>
#include <absl/container/btree_set.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <exception>
#include <limits>
#include <memory>
#include <optional>
#include <utility>
#include <variant>
#include <vector>

namespace kwaque::simulation {

namespace {

constexpr invariant_id fake_network_drained_invariant{
  "KQ-FAKE-NETWORK-DRAINED"};
constexpr invariant_id fake_network_handle_invariant{"KQ-FAKE-NETWORK-HANDLE"};
constexpr invariant_id fake_network_state_invariant{"KQ-FAKE-NETWORK-STATE"};

[[nodiscard]] runtime::operation_error network_error(errc code) noexcept {
    return runtime::operation_error{code, runtime::operation_kind::network};
}

template<typename T>
[[nodiscard]] seastar::future<runtime::result<T>> ready_failure(errc code) {
    return seastar::make_ready_future<runtime::result<T>>(
      runtime::failure(network_error(code)));
}

[[nodiscard]] bool
is_unspecified(const runtime::network_address& address) noexcept {
    const auto bytes = address.family() == runtime::network_address_family::ipv4
                         ? std::size_t{4}
                         : address.bytes().size();
    return std::all_of(
      address.bytes().begin(),
      address.bytes().begin() + static_cast<std::ptrdiff_t>(bytes),
      [](std::byte value) { return value == std::byte{}; });
}

[[nodiscard]] runtime::network_address
wildcard_address(runtime::network_address_family family) noexcept {
    return family == runtime::network_address_family::ipv4
             ? runtime::network_address::ipv4(
                 {std::byte{}, std::byte{}, std::byte{}, std::byte{}})
             : runtime::network_address::ipv6({});
}

[[nodiscard]] runtime::result<runtime::monotonic_time> add_deadline(
  runtime::monotonic_time now,
  runtime::monotonic_duration latency,
  runtime::monotonic_time maximum) noexcept {
    if (latency.nanoseconds() > maximum.nanoseconds() - now.nanoseconds()) {
        return runtime::failure(network_error(errc::out_of_range));
    }
    return runtime::monotonic_time{now.nanoseconds() + latency.nanoseconds()};
}

[[nodiscard]] constexpr std::uint8_t other_side(std::uint8_t side) noexcept {
    return static_cast<std::uint8_t>(1U - side);
}

[[nodiscard]] std::array<trace_context_field, 4>
allocation_context(const bandwidth_allocation_digest& digest) noexcept {
    return {
      trace_context_field{
        .key = trace_context_key::digest_word_0,
        .value = digest.words[0],
      },
      trace_context_field{
        .key = trace_context_key::digest_word_1,
        .value = digest.words[1],
      },
      trace_context_field{
        .key = trace_context_key::digest_word_2,
        .value = digest.words[2],
      },
      trace_context_field{
        .key = trace_context_key::digest_word_3,
        .value = digest.words[3],
      },
    };
}

[[nodiscard]] runtime::fault_object_key
network_object_key(std::uint64_t owner, std::uint8_t side) noexcept {
    std::array<std::byte, 9> encoded{};
    for (std::size_t index = 0; index < sizeof(owner); ++index) {
        encoded[index] = static_cast<std::byte>(owner & 0xffU);
        owner >>= 8U;
    }
    encoded.back() = static_cast<std::byte>(side);
    auto key = runtime::fault_object_key::from_bytes(encoded);
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      key.has_value(),
      "fake network object key exceeded its fixed encoding");
    return *key;
}

[[nodiscard]] bytes::fragmented_buffer corrupt_network_payload(
  const bytes::fragmented_buffer& source,
  std::uint64_t selected_byte,
  std::uint8_t selected_bit) {
    bytes::fragmented_buffer_builder builder;
    std::uint64_t offset = 0;
    for (const auto fragment : source) {
        const auto begin = offset;
        const auto end = begin + fragment.size();
        if (selected_byte >= begin && selected_byte < end) {
            seastar::temporary_buffer<char> mutated(fragment.size());
            std::memcpy(mutated.get_write(), fragment.data(), fragment.size());
            mutated.get_write()[selected_byte - begin] ^= static_cast<char>(
              std::uint8_t{1} << selected_bit);
            auto appended = builder.append(
              std::span<const char>{mutated.get(), mutated.size()});
            KWAQUE_INVARIANT(
              fake_network_state_invariant,
              appended.has_value(),
              "bounded corrupt packet copy was rejected");
        } else {
            auto appended = builder.append(
              std::span<const char>{fragment.data(), fragment.size()});
            KWAQUE_INVARIANT(
              fake_network_state_invariant,
              appended.has_value(),
              "bounded packet copy was rejected");
        }
        offset = end;
    }
    auto result = builder.finish();
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      result.has_value(),
      "bounded corrupt packet publication was rejected");
    return std::move(*result);
}

} // namespace

class fake_network::impl final {
public:
    class parked_credit final {
    public:
        parked_credit() noexcept
          : used_(nullptr) {}
        explicit parked_credit(std::uint32_t& used) noexcept
          : used_(&used) {
            ++*used_;
        }
        parked_credit(const parked_credit&) = delete;
        parked_credit& operator=(const parked_credit&) = delete;
        parked_credit(parked_credit&& other) noexcept
          : used_(std::exchange(other.used_, nullptr)) {}
        parked_credit& operator=(parked_credit&& other) noexcept {
            if (this != &other) {
                release();
                used_ = std::exchange(other.used_, nullptr);
            }
            return *this;
        }
        ~parked_credit() { release(); }

        void release() noexcept {
            if (used_ == nullptr) {
                return;
            }
            KWAQUE_INVARIANT(
              fake_network_state_invariant,
              *used_ != 0,
              "fake parked-operation credit underflow");
            --*used_;
            used_ = nullptr;
        }

    private:
        std::uint32_t* used_;
    };

    enum class packet_phase : std::uint8_t {
        free,
        deferred_clone,
        queued,
        active,
        propagating,
        ready_clogged,
        arrived,
        delivered,
        retired,
    };

    enum class transmitter_state : std::uint8_t {
        ready,
        busy,
        interframe,
    };

    enum class control_kind : std::uint8_t {
        partition,
        heal,
        clog,
        unclog,
        egress,
        link,
        ingress,
    };

    enum class sequence_status : std::uint8_t {
        empty,
        live,
        delivered_early,
        retired,
    };

    struct packet_token final {
        std::uint64_t id{0};
        std::uint32_t slot{0};

        [[nodiscard]] bool valid() const noexcept { return id != 0; }
    };

    struct fin_token final {
        std::uint64_t pair{0};
        std::uint8_t side{0};
    };

    struct staged_flow_start final {
        packet_token packet;
        std::uint16_t flow_slot{0};
    };

    struct directed_link_key final {
        runtime::network_address source{runtime::network_address::ipv4(
          {std::byte{}, std::byte{}, std::byte{}, std::byte{}})};
        runtime::network_address target{runtime::network_address::ipv4(
          {std::byte{}, std::byte{}, std::byte{}, std::byte{}})};

        auto operator<=>(const directed_link_key&) const = default;
    };

    struct listener_state final {
        listener_state(
          std::uint64_t listener_id,
          runtime::network_endpoint bound_endpoint,
          runtime::network_listen_options listen_options,
          std::uint32_t global_backlog_limit)
          : id(listener_id)
          , endpoint(bound_endpoint)
          , options(listen_options) {
            backlog.reserve(
              std::min<std::uint32_t>(options.backlog, global_backlog_limit));
        }

        std::uint64_t id;
        runtime::network_endpoint endpoint;
        runtime::network_listen_options options;
        seastar::chunked_fifo<std::uint64_t, 128, 128> backlog;
        std::optional<std::uint64_t> accept_operation;
        std::optional<seastar::shared_promise<runtime::result<void>>>
          close_done;
        scheduler::event_slot_reservation close_event;
        event_trace::reservation close_trace;
        scheduler::event_slot_reservation stop_event;
        event_trace::reservation stop_trace;
        event_id close_event_id;
        std::uint32_t reserved_backlog{0};
        std::uint32_t captured_connects{0};
        bool bound{false};
        bool aborted{false};
        bool closing{false};
        bool closed{false};
        bool handle_owned{false};
        bool stop_prepared{false};
    };

    struct endpoint_state final {
        endpoint_state(
          runtime::network_endpoint local_endpoint,
          runtime::network_endpoint remote_endpoint,
          runtime::network_connection_limits connection_limits) noexcept
          : local(local_endpoint)
          , remote(remote_endpoint)
          , limits(connection_limits) {}

        runtime::network_endpoint local;
        runtime::network_endpoint remote;
        runtime::network_connection_limits limits;
        std::optional<std::uint64_t> read_operation;
        std::optional<seastar::shared_promise<runtime::result<void>>>
          close_done;
        scheduler::event_slot_reservation fin_event_reservation;
        event_trace::reservation fin_trace;
        event_trace::reservation fin_effect_trace;
        scheduler::event_slot_reservation fin_ready_event_reservation;
        event_trace::reservation fin_ready_trace;
        scheduler::event_slot_reservation close_event_reservation;
        event_trace::reservation close_trace;
        event_trace::reservation close_parked_trace;
        scheduler::event_slot_reservation stop_event_reservation;
        event_trace::reservation stop_trace;
        event_id fin_event;
        event_id close_event;
        runtime::fault_decision close_fault;
        runtime::network_connection_state state{
          runtime::network_connection_state::open};
        runtime::network_half_state input{runtime::network_half_state::open};
        runtime::network_half_state output{runtime::network_half_state::open};
        bool exposed{false};
        bool handle_owned{false};
        bool abort_requested{false};
        bool peer_reset{false};
        bool fin_scheduled{false};
        bool close_drop_completion{false};
        parked_credit close_parked_credit;
    };

    struct direction_state final {
        explicit direction_state(std::uint32_t maximum_packets) {
            transmit_queue.reserve(maximum_packets);
            delivered.reserve(maximum_packets);
            sequence_slots.resize(maximum_packets, packet_token{});
            sequence_statuses.resize(maximum_packets, sequence_status::empty);
            sequence_traces.resize(maximum_packets);
        }

        seastar::chunked_fifo<packet_token, 128, 8> transmit_queue;
        seastar::chunked_fifo<packet_token, 128, 8> delivered;
        std::vector<packet_token> sequence_slots;
        std::vector<sequence_status> sequence_statuses;
        std::vector<event_trace::reservation> sequence_traces;
        byte_count logical_bytes;
        std::optional<std::uint16_t> transmitter_slot;
        std::optional<std::uint32_t> current_packet;
        std::optional<packet_token> deferred_clone;
        std::optional<std::uint64_t> fin_sequence;
        event_id gap_event;
        std::uint64_t next_sequence{0};
        std::uint64_t expected_sequence{0};
        std::uint32_t packet_count{0};
        std::uint32_t sequence_entries{0};
        transmitter_state transmitter{transmitter_state::ready};
        bool gap_scheduled{false};
        bool fin_delivered{false};
        bool fin_arrived{false};
        bool fin_retired{false};
        bool sequence_exhausted{false};
    };

    struct pair_state final {
        pair_state(
          std::uint64_t pair_id,
          runtime::network_endpoint client_local,
          runtime::network_endpoint server_local,
          runtime::network_connection_limits client_limits,
          runtime::network_connection_limits server_limits,
          std::uint32_t maximum_direction_packets)
          : id(pair_id)
          , endpoints{endpoint_state{client_local, server_local, client_limits}, endpoint_state{server_local, client_local, server_limits}}
          , directions{direction_state{maximum_direction_packets}, direction_state{maximum_direction_packets}}
          , reserved_client_local(client_local) {}

        std::uint64_t id;
        std::array<endpoint_state, 2> endpoints;
        std::array<direction_state, 2> directions;
        runtime::network_endpoint reserved_client_local;
        std::optional<std::uint64_t> backlog_listener;
        event_trace::reservation reset_trace;
        bool reset_applied{false};
        std::uint8_t stop_terminals_pending{0};
        bool stop_prepared{false};
    };

    struct bind_operation final {
        std::uint64_t listener{0};
        seastar::promise<runtime::result<fake_listener>> done;
        event_id event;
    };

    struct connect_operation final {
        std::uint64_t pair{0};
        std::uint64_t listener{0};
        seastar::promise<runtime::result<fake_connection>> done;
        seastar::optimized_optional<seastar::abort_source::subscription>
          abort_subscription;
        scheduler::event_slot_reservation client_event_reservation;
        scheduler::event_slot_reservation incoming_event_reservation;
        event_id client_event;
        event_id incoming_event;
        scheduler::event_slot_reservation terminal_event;
        event_trace::reservation terminal_trace;
        event_trace::reservation parked_trace;
        runtime::fault_decision fault;
        bool client_done{false};
        bool incoming_done{false};
        bool aborting{false};
        bool parked{false};
    };

    struct accept_operation final {
        std::uint64_t listener{0};
        std::optional<std::uint64_t> pair;
        seastar::abort_source* caller_abort{nullptr};
        seastar::promise<runtime::result<fake_connection>> done;
        scheduler::event_slot_reservation event_reservation;
        event_trace::reservation trace;
        scheduler::event_slot_reservation terminal_event;
        event_trace::reservation terminal_trace;
        event_trace::reservation parked_trace;
        event_id event;
        runtime::fault_decision fault;
        bool scheduled{false};
        bool parked{false};
    };

    struct read_operation final {
        std::uint64_t pair{0};
        byte_count maximum_bytes;
        seastar::promise<runtime::result<runtime::network_read_result>> done;
        scheduler::event_slot_reservation event_reservation;
        event_trace::reservation trace;
        scheduler::event_slot_reservation terminal_event;
        event_trace::reservation terminal_trace;
        event_trace::reservation parked_trace;
        event_id event;
        runtime::fault_decision fault;
        std::optional<runtime::network_read_result> parked_result;
        std::uint64_t fault_a{0};
        std::uint64_t fault_b{0};
        std::uint8_t side{0};
        bool scheduled{false};
        bool parked{false};
    };

    struct write_operation final {
        std::uint64_t pair{0};
        packet_token packet;
        std::optional<runtime::network_write_admission::reservation> admission;
        seastar::promise<runtime::result<void>> done;
        seastar::optimized_optional<seastar::abort_source::subscription>
          abort_subscription;
        scheduler::event_slot_reservation terminal_event;
        event_trace::reservation terminal_trace;
        event_trace::reservation effect_trace;
        event_trace::reservation parked_trace;
        std::optional<runtime::operation_error> terminal_error;
        errc terminal_code{errc::success};
        std::uint8_t side{0};
        bool done_set{false};
        bool drop_completion{false};
        bool parked{false};
    };

    struct control_operation final {
        control_kind kind{control_kind::partition};
        runtime::network_address source{runtime::network_address::ipv4(
          {std::byte{}, std::byte{}, std::byte{}, std::byte{}})};
        runtime::network_address target{runtime::network_address::ipv4(
          {std::byte{}, std::byte{}, std::byte{}, std::byte{}})};
        bandwidth_capacity capacity;
        seastar::promise<runtime::result<void>> done;
        event_trace::reservation rebalance_trace;
        event_trace::reservation wake_trace;
        event_id event;
    };

    struct network_fault_key final {
        runtime::builtin_fault_point point{runtime::builtin_fault_point::timer};
        runtime::fault_object_key object;

        auto operator<=>(const network_fault_key&) const = default;
    };

    struct prepared_network_fault final {
        prepared_network_fault() = default;
        prepared_network_fault(const prepared_network_fault&) = delete;
        prepared_network_fault&
        operator=(const prepared_network_fault&) = delete;
        prepared_network_fault(prepared_network_fault&& other) noexcept;
        prepared_network_fault& operator=(prepared_network_fault&&) = delete;
        ~prepared_network_fault();

        impl* owner{nullptr};
        network_fault_key key;
        std::optional<prepared_fault_evaluation> prepared;
        runtime::fault_decision decision;
        std::uint64_t coordinate_a{0};
        std::uint64_t coordinate_b{0};
        bool inserted_occurrence{false};
        bool committed{false};
        bool applicable{false};
    };

    struct packet_trace_reservations final {
        event_trace::reservation flow_start;
        event_trace::reservation transfer;
        event_trace::reservation packet_effect;
        event_trace::reservation sequence;
        event_trace::reservation start_rebalance;
        event_trace::reservation finish_rebalance;
        event_trace::reservation start_wake;
        event_trace::reservation finish_wake;
    };

    struct stop_batch_reservation final {
        scheduler::event_slot_reservation event;
        event_trace::reservation trace;
        std::uint64_t id{0};
    };

    struct packet_state final {
        std::uint64_t id{0};
        std::uint32_t slot{0};
        std::uint64_t pair{0};
        std::uint64_t write_operation{0};
        std::optional<packet_token> clone;
        directed_link_key link;
        bytes::fragmented_buffer data;
        byte_count logical_charge;
        byte_count retained_charge;
        scheduler::event_slot_reservation delivery_event_reservation;
        event_trace::reservation delivery_trace;
        scheduler::event_slot_reservation gap_event_reservation;
        event_trace::reservation gap_trace;
        scheduler::event_slot_reservation ready_event_reservation;
        event_trace::reservation ready_trace;
        event_trace::reservation flow_start_trace;
        event_trace::reservation transfer_trace;
        event_trace::reservation packet_effect_trace;
        event_trace::reservation sequence_trace;
        event_trace::reservation start_rebalance_trace;
        event_trace::reservation finish_rebalance_trace;
        event_trace::reservation start_wake_trace;
        event_trace::reservation finish_wake_trace;
        event_id delivery_event;
        runtime::monotonic_time ready_at;
        runtime::monotonic_duration delivery_delay;
        std::uint64_t sequence{0};
        std::uint8_t side{0};
        packet_phase phase{packet_phase::free};
        bool delivery_scheduled{false};
        bool drop_delivery{false};
        bool reorder_delivery{false};
        bool packet_effect_observed{false};
    };

    struct flow_state final {
        std::uint32_t packet_slot{0};
        bandwidth_fraction remaining;
        bandwidth_rate rate;
        runtime::monotonic_time last_update;
        bool active{false};
    };

    struct link_state final {
        link_state(
          std::uint64_t link_id,
          directed_link_key link_key,
          bandwidth_capacity link_capacity,
          runtime::monotonic_duration minimum_latency,
          runtime::monotonic_duration mean_parameter) noexcept
          : id(link_id)
          , key(link_key)
          , capacity(link_capacity)
          , latency_min(minimum_latency)
          , latency_mean_parameter(mean_parameter) {}

        std::uint64_t id;
        directed_link_key key;
        bandwidth_capacity capacity;
        runtime::monotonic_duration latency_min;
        runtime::monotonic_duration latency_mean_parameter;
        seastar::chunked_fifo<packet_token, 128, 1> ready;
        seastar::chunked_fifo<fin_token, 128, 1> ready_fins;
        event_id ready_chain_event;
        std::uint32_t packets{0};
        bool partitioned{false};
        bool clogged{false};
        bool ready_chain_scheduled{false};
    };

    using operation_payload = std::variant<
      bind_operation,
      connect_operation,
      accept_operation,
      read_operation,
      write_operation,
      control_operation>;

    struct operation_state final {
        operation_state(
          std::uint64_t operation_id,
          operation_payload value,
          parked_credit parked_reservation = {})
          : id(operation_id)
          , payload(std::move(value))
          , parked(std::move(parked_reservation)) {}

        std::uint64_t id;
        operation_payload payload;
        parked_credit parked;
        scheduler::event_slot_reservation stop_event;
        event_trace::reservation stop_trace;
        bool stop_scheduled{false};
        bool stop_needs_completion{true};
    };

    struct port_selection final {
        runtime::network_endpoint endpoint;
        std::uint16_t next_cursor;
    };

    impl(
      fake_network& owner,
      fake_network_config config,
      scheduler& events,
      std::unique_ptr<bandwidth_planner> bandwidth,
      std::unique_ptr<bandwidth_planner> staged_bandwidth,
      scheduler::event_slot_reservation bandwidth_reservation,
      seastar::chunked_fifo<scheduler::event_slot_reservation, 32, 2>
        stop_capacity,
      fault_schedule* faults)
      : owner_(&owner)
      , config_(std::move(config))
      , scheduler_(&events)
      , random_(config_.latency_seed)
      , bandwidth_(std::move(bandwidth))
      , staged_bandwidth_(std::move(staged_bandwidth))
      , bandwidth_event_reservation_(std::move(bandwidth_reservation))
      , stop_event_capacity_(std::move(stop_capacity))
      , faults_(faults) {
        work_ids.reserve(config_.maximum_operations);
        packets.reserve(config_.maximum_packets);
        for (std::uint32_t slot = 0; slot < config_.maximum_packets; ++slot) {
            packets.emplace_back();
            packets[slot].slot = slot;
            free_packets.push_back(slot);
        }
        flows.reserve(config_.maximum_active_flows);
        for (std::uint16_t slot = 0; slot < config_.maximum_active_flows;
             ++slot) {
            flows.emplace_back();
            free_flows.push_back(slot);
        }
        bandwidth_->reset();
        const auto solved = bandwidth_->solve();
        staged_bandwidth_->reset();
        const auto staged_solved = staged_bandwidth_->solve();
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          solved.has_value() && staged_solved.has_value(),
          "empty bandwidth planner failed initialization");
        allocation_digest_ = bandwidth_->allocation_digest();
    }

    fake_network* owner_;
    fake_network_config config_;
    scheduler* scheduler_;
    absl::btree_map<std::uint64_t, std::unique_ptr<listener_state>> listeners;
    absl::btree_map<runtime::network_endpoint, std::uint64_t> listener_registry;
    absl::btree_map<std::uint64_t, std::unique_ptr<pair_state>> pairs;
    absl::btree_map<std::uint64_t, std::unique_ptr<operation_state>> operations;
    absl::btree_map<directed_link_key, std::unique_ptr<link_state>> links;
    absl::btree_map<runtime::network_address, bandwidth_capacity> egress_limits;
    absl::btree_map<runtime::network_address, bandwidth_capacity>
      ingress_limits;
    absl::btree_set<runtime::network_endpoint> connection_locals;
    absl::btree_map<runtime::network_address, std::uint16_t> port_cursors;
    std::vector<std::uint64_t> work_ids;
    seastar::chunked_vector<packet_state> packets;
    seastar::chunked_fifo<std::uint32_t, 128, 512> free_packets;
    seastar::chunked_vector<flow_state> flows;
    seastar::chunked_fifo<std::uint16_t, 128, 1> free_flows;
    deterministic_random random_;
    std::unique_ptr<bandwidth_planner> bandwidth_;
    std::unique_ptr<bandwidth_planner> staged_bandwidth_;
    scheduler::event_slot_reservation bandwidth_event_reservation_;
    seastar::chunked_fifo<scheduler::event_slot_reservation, 32, 2>
      stop_event_capacity_;
    event_id bandwidth_event_;
    runtime::monotonic_time bandwidth_deadline_;
    std::uint64_t bandwidth_flow_id_{0};
    bandwidth_allocation_digest allocation_digest_{};
    fault_schedule* faults_{nullptr};
    absl::btree_map<network_fault_key, std::uint64_t> fault_occurrences_;
    seastar::chunked_fifo<stop_batch_reservation, 32, 2> stop_batches_;
    std::optional<seastar::shared_promise<runtime::result<void>>> stop_done_;
    std::optional<runtime::operation_error> stop_failure_;
    std::uint64_t next_listener_id{1};
    std::uint64_t next_pair_id{1};
    std::uint64_t next_operation_id{1};
    std::uint64_t next_packet_id{1};
    std::uint64_t next_link_id{1};
    std::uint64_t next_stop_batch_id{1};
    std::uint32_t pending_connects{0};
    std::uint32_t backlog_entries{0};
    std::uint32_t live_packets{0};
    std::uint32_t active_controls{0};
    std::uint32_t parked_operations{0};
    byte_count packet_logical_bytes;
    byte_count packet_retained_bytes;
    bool listener_ids_exhausted{false};
    bool pair_ids_exhausted{false};
    bool operation_ids_exhausted{false};
    bool packet_ids_exhausted{false};
    bool link_ids_exhausted{false};
    bool bandwidth_scheduled_{false};
    fake_network_state state_{fake_network_state::open};
    bool abort_requested_{false};
    bool stop_batch_scheduled_{false};
    bool stop_resources_released_{false};
    bool forcing_discard_{false};
    bool activated_{false};

    [[nodiscard]] listener_state* find_listener(std::uint64_t id) noexcept {
        const auto found = listeners.find(id);
        return found == listeners.end() ? nullptr : found->second.get();
    }
    [[nodiscard]] const listener_state*
    find_listener(std::uint64_t id) const noexcept {
        const auto found = listeners.find(id);
        return found == listeners.end() ? nullptr : found->second.get();
    }
    [[nodiscard]] pair_state* find_pair(std::uint64_t id) noexcept {
        const auto found = pairs.find(id);
        return found == pairs.end() ? nullptr : found->second.get();
    }
    [[nodiscard]] const pair_state* find_pair(std::uint64_t id) const noexcept {
        const auto found = pairs.find(id);
        return found == pairs.end() ? nullptr : found->second.get();
    }
    [[nodiscard]] operation_state* find_operation(std::uint64_t id) noexcept {
        const auto found = operations.find(id);
        return found == operations.end() ? nullptr : found->second.get();
    }
    [[nodiscard]] packet_state* find_packet(packet_token token) noexcept {
        if (token.slot >= packets.size()) {
            return nullptr;
        }
        auto& packet = packets[token.slot];
        return packet.phase != packet_phase::free && packet.id == token.id
                 ? &packet
                 : nullptr;
    }
    [[nodiscard]] packet_state*
    find_packet(std::uint32_t slot, std::uint64_t id) noexcept {
        return find_packet(packet_token{.id = id, .slot = slot});
    }
    [[nodiscard]] link_state* find_link(const directed_link_key& key) noexcept {
        const auto found = links.find(key);
        return found == links.end() ? nullptr : found->second.get();
    }
    [[nodiscard]] bandwidth_capacity
    egress_capacity(const runtime::network_address& address) const noexcept;
    [[nodiscard]] bandwidth_capacity
    ingress_capacity(const runtime::network_address& address) const noexcept;
    [[nodiscard]] runtime::result<prepared_network_fault> prepare_fault(
      runtime::builtin_fault_point point, runtime::fault_object_key object);
    [[nodiscard]] runtime::result<void>
    commit_fault(prepared_network_fault& prepared) noexcept;

    [[nodiscard]] bool listener_conflicts(
      runtime::network_endpoint endpoint,
      std::optional<std::uint64_t> ignored = std::nullopt) const noexcept;
    [[nodiscard]] bool
    local_endpoint_busy(runtime::network_endpoint endpoint) const noexcept;
    [[nodiscard]] std::optional<std::uint64_t>
    lookup_listener(runtime::network_endpoint endpoint) const noexcept;
    [[nodiscard]] runtime::result<port_selection>
    select_port(runtime::network_address address) const noexcept;
    [[nodiscard]] runtime::result<void>
    prepare_cursor(const runtime::network_address& address) const noexcept;
    void commit_cursor(
      const runtime::network_address& address, std::uint16_t next_cursor);
    [[nodiscard]] std::size_t persistent_address_entries() const noexcept;
    [[nodiscard]] const runtime::network_address*
    default_source(runtime::network_address_family family) const noexcept;
    [[nodiscard]] runtime::result<runtime::network_endpoint> select_local(
      runtime::network_endpoint remote,
      std::optional<runtime::network_endpoint> requested,
      std::optional<port_selection>& selected_port) const noexcept;

    void issue_listener_id() noexcept;
    void issue_pair_id() noexcept;
    void issue_operation_id() noexcept;
    void issue_packet_id() noexcept;
    void issue_link_id() noexcept;
    [[nodiscard]] bool
    remove_backlog_pair(listener_state& listener, std::uint64_t pair) noexcept;
    [[nodiscard]] runtime::result<
      std::pair<scheduler::event_slot_reservation, event_trace::reservation>>
    reserve_terminal(
      trace_event_kind kind,
      std::uint32_t domain,
      std::uint64_t stable_id,
      trace_action effect = trace_action::none,
      std::uint64_t coordinate_a = 0,
      std::uint64_t coordinate_b = 0,
      std::uint64_t value = 0,
      std::uint32_t result = 0);
    [[nodiscard]] runtime::result<event_trace::reservation> reserve_event_trace(
      trace_event_kind kind,
      std::uint32_t domain,
      std::uint64_t stable_id,
      trace_action effect = trace_action::none,
      std::uint64_t coordinate_a = 0,
      std::uint64_t coordinate_b = 0,
      std::uint64_t value = 0,
      std::uint32_t result = 0);
    [[nodiscard]] runtime::result<packet_trace_reservations>
    reserve_packet_traces(
      std::uint64_t packet,
      std::uint64_t pair,
      std::uint8_t side,
      byte_count bytes);
    [[nodiscard]] runtime::result<parked_credit>
    reserve_parked(bool required) noexcept;

    void complete_bind(std::uint64_t operation) noexcept;
    void complete_connect_client(std::uint64_t operation) noexcept;
    void complete_incoming(std::uint64_t operation) noexcept;
    void abort_connect(std::uint64_t operation) noexcept;
    void complete_connect_abort(std::uint64_t operation) noexcept;
    void maybe_erase_connect(std::uint64_t operation) noexcept;

    [[nodiscard]] runtime::result<void>
    schedule_accept(std::uint64_t operation, runtime::monotonic_time deadline);
    void complete_accept(std::uint64_t operation) noexcept;
    void complete_accept_abort(std::uint64_t operation) noexcept;

    [[nodiscard]] runtime::result<void>
    schedule_read(std::uint64_t operation, runtime::monotonic_time deadline);
    void complete_read(std::uint64_t operation) noexcept;
    void complete_read_terminal(std::uint64_t operation) noexcept;

    [[nodiscard]] bandwidth_resource_key resource_key(
      std::uint8_t domain,
      const runtime::network_address& first,
      const runtime::network_address* second = nullptr) const noexcept;
    [[nodiscard]] runtime::monotonic_duration
    packet_latency(const link_state& link, const packet_state& packet) noexcept;
    [[nodiscard]] bool observe_packet_effect(
      packet_state& packet, trace_action action, errc result) noexcept;
    void start_next_packet(std::uint64_t pair, std::uint8_t direction) noexcept;
    [[nodiscard]] bool rebalance_bandwidth(
      event_trace::reservation trace = {},
      event_trace::reservation wake_trace = {},
      std::uint64_t stable_id = 0,
      const control_operation* staged_control = nullptr,
      const staged_flow_start* staged_start = nullptr) noexcept;
    void complete_bandwidth() noexcept;
    void finish_flow(std::uint16_t flow_slot) noexcept;
    void complete_gap(std::uint64_t pair, std::uint8_t direction) noexcept;
    void complete_delivery(std::uint32_t slot, std::uint64_t packet) noexcept;
    void release_sequences(std::uint64_t pair, std::uint8_t direction) noexcept;
    void retire_packet(std::uint32_t slot, errc write_code) noexcept;
    void destroy_packet(std::uint32_t slot) noexcept;
    void abort_write(std::uint64_t operation) noexcept;
    void complete_immediate_write(
      std::uint64_t operation, errc code, bool disconnect) noexcept;
    void complete_write_terminal(std::uint64_t operation) noexcept;
    void complete_parked_connection_operation(
      std::uint64_t operation, errc code) noexcept;
    void maybe_wake_read(std::uint64_t pair, std::uint8_t side) noexcept;
    [[nodiscard]] seastar::future<runtime::result<void>> submit_control(
      control_kind kind,
      runtime::network_address source,
      runtime::network_address target,
      bandwidth_capacity capacity);
    void complete_control(std::uint64_t operation) noexcept;
    void schedule_ready_delivery(link_state& link) noexcept;
    void complete_ready_delivery(
      directed_link_key link,
      std::uint32_t slot,
      std::uint64_t packet) noexcept;
    void complete_ready_fin(directed_link_key link, fin_token fin) noexcept;
    void apply_delivery(std::uint32_t slot, std::uint64_t packet) noexcept;

    void deliver_fin(std::uint64_t pair, std::uint8_t side) noexcept;
    void reset_pair(std::uint64_t pair, std::uint8_t initiator) noexcept;
    void fail_endpoint_operations(
      std::uint64_t pair, std::uint8_t side, errc code) noexcept;
    void discard_input(std::uint64_t pair, std::uint8_t side) noexcept;
    void
    complete_connection_close(std::uint64_t pair, std::uint8_t side) noexcept;
    void complete_listener_close(std::uint64_t listener) noexcept;
    void collect_pair(std::uint64_t pair) noexcept;
    void collect_listener(std::uint64_t listener) noexcept;
    [[nodiscard]] runtime::result<void> prepare_stop_batches();
    void schedule_stop_batch() noexcept;
    void run_stop_batch() noexcept;
    [[nodiscard]] bool has_stop_preparation_work() const noexcept;
    [[nodiscard]] operation_state* next_stop_operation() noexcept;
    [[nodiscard]] packet_state* next_stop_packet() noexcept;
    [[nodiscard]] pair_state* next_stop_pair() noexcept;
    [[nodiscard]] listener_state* next_stop_listener() noexcept;
    [[nodiscard]] link_state* next_stop_link() noexcept;
    void prepare_stop_operation(operation_state& operation) noexcept;
    void discard_stop_packet(packet_state& packet) noexcept;
    void prepare_stop_pair(pair_state& pair) noexcept;
    void prepare_stop_listener(listener_state& listener) noexcept;
    void discard_stop_link(link_state& link) noexcept;
    void complete_stop_operation(std::uint64_t operation) noexcept;
    void complete_stop_endpoint(std::uint64_t pair, std::uint8_t side) noexcept;
    void complete_stop_listener(std::uint64_t listener) noexcept;
    void maybe_finish_stop() noexcept;
    void force_discard_all(const runtime::operation_error& failure) noexcept;
};

fake_network::impl::prepared_network_fault::prepared_network_fault(
  prepared_network_fault&& other) noexcept
  : owner(std::exchange(other.owner, nullptr))
  , key(other.key)
  , prepared(std::move(other.prepared))
  , decision(other.decision)
  , coordinate_a(other.coordinate_a)
  , coordinate_b(other.coordinate_b)
  , inserted_occurrence(other.inserted_occurrence)
  , committed(other.committed)
  , applicable(other.applicable) {}

fake_network::impl::prepared_network_fault::~prepared_network_fault() {
    if (owner == nullptr || committed || !inserted_occurrence) {
        return;
    }
    const auto found = owner->fault_occurrences_.find(key);
    if (found != owner->fault_occurrences_.end() && found->second == 0) {
        owner->fault_occurrences_.erase(found);
    }
}

bool fake_network::impl::listener_conflicts(
  runtime::network_endpoint endpoint,
  std::optional<std::uint64_t> ignored) const noexcept {
    const bool requested_wildcard = is_unspecified(endpoint.address());
    for (const auto& [registered, id] : listener_registry) {
        if (ignored && id == *ignored) {
            continue;
        }
        if (
          registered.address().family() != endpoint.address().family()
          || registered.port() != endpoint.port()) {
            continue;
        }
        if (
          requested_wildcard || is_unspecified(registered.address())
          || registered.address() == endpoint.address()) {
            return true;
        }
    }
    return false;
}

bool fake_network::impl::local_endpoint_busy(
  runtime::network_endpoint endpoint) const noexcept {
    if (connection_locals.contains(endpoint) || listener_conflicts(endpoint)) {
        return true;
    }
    if (!is_unspecified(endpoint.address())) {
        return false;
    }
    return std::ranges::any_of(
      connection_locals, [&](const runtime::network_endpoint& local) {
          return local.address().family() == endpoint.address().family()
                 && local.port() == endpoint.port();
      });
}

std::optional<std::uint64_t> fake_network::impl::lookup_listener(
  runtime::network_endpoint endpoint) const noexcept {
    if (
      const auto exact = listener_registry.find(endpoint);
      exact != listener_registry.end()) {
        return exact->second;
    }
    const runtime::network_endpoint wildcard{
      wildcard_address(endpoint.address().family()), endpoint.port()};
    if (
      const auto found = listener_registry.find(wildcard);
      found != listener_registry.end()) {
        return found->second;
    }
    return std::nullopt;
}

runtime::result<fake_network::impl::port_selection>
fake_network::impl::select_port(
  runtime::network_address address) const noexcept {
    const auto found = port_cursors.find(address);
    auto candidate = found == port_cursors.end() ? config_.ephemeral_first
                                                 : found->second;
    const auto count = static_cast<std::uint32_t>(config_.ephemeral_last)
                       - config_.ephemeral_first + 1U;
    for (std::uint32_t scanned = 0; scanned < count; ++scanned) {
        const runtime::network_endpoint endpoint{address, candidate};
        const auto next = candidate == config_.ephemeral_last
                            ? config_.ephemeral_first
                            : static_cast<std::uint16_t>(candidate + 1U);
        if (!local_endpoint_busy(endpoint)) {
            return port_selection{.endpoint = endpoint, .next_cursor = next};
        }
        candidate = next;
    }
    return runtime::failure(network_error(errc::resource_exhausted));
}

void fake_network::impl::commit_cursor(
  const runtime::network_address& address, std::uint16_t next_cursor) {
    const auto [position, inserted] = port_cursors.try_emplace(
      address, next_cursor);
    if (!inserted) {
        position->second = next_cursor;
    }
}

runtime::result<void> fake_network::impl::prepare_cursor(
  const runtime::network_address& address) const noexcept {
    if (
      !port_cursors.contains(address)
      && persistent_address_entries() >= config_.maximum_address_entries) {
        return runtime::failure(network_error(errc::queue_full));
    }
    return {};
}

std::size_t fake_network::impl::persistent_address_entries() const noexcept {
    return egress_limits.size() + ingress_limits.size() + port_cursors.size();
}

const runtime::network_address* fake_network::impl::default_source(
  runtime::network_address_family family) const noexcept {
    const auto& selected = family == runtime::network_address_family::ipv4
                             ? config_.ipv4_source
                             : config_.ipv6_source;
    return selected ? &*selected : nullptr;
}

runtime::result<runtime::network_endpoint> fake_network::impl::select_local(
  runtime::network_endpoint remote,
  std::optional<runtime::network_endpoint> requested,
  std::optional<port_selection>& selected_port) const noexcept {
    runtime::network_address address = requested ? requested->address()
                                                 : remote.address();
    std::uint16_t port = 0;
    if (requested) {
        if (requested->address().family() != remote.address().family()) {
            return runtime::failure(network_error(errc::invalid_argument));
        }
        address = requested->address();
        port = requested->port();
    }
    if (!requested || is_unspecified(address)) {
        const auto* configured = default_source(remote.address().family());
        if (configured == nullptr) {
            return runtime::failure(network_error(errc::invalid_argument));
        }
        address = *configured;
    }
    if (port == 0) {
        auto selected = select_port(address);
        if (!selected) {
            return runtime::failure(selected.error());
        }
        selected_port = *selected;
        return selected->endpoint;
    }
    const runtime::network_endpoint endpoint{address, port};
    if (local_endpoint_busy(endpoint)) {
        return runtime::failure(network_error(errc::network_failure));
    }
    return endpoint;
}

void fake_network::impl::issue_listener_id() noexcept {
    if (next_listener_id == std::numeric_limits<std::uint64_t>::max()) {
        listener_ids_exhausted = true;
    } else {
        ++next_listener_id;
    }
}

void fake_network::impl::issue_pair_id() noexcept {
    if (next_pair_id == std::numeric_limits<std::uint64_t>::max()) {
        pair_ids_exhausted = true;
    } else {
        ++next_pair_id;
    }
}

void fake_network::impl::issue_operation_id() noexcept {
    if (next_operation_id == std::numeric_limits<std::uint64_t>::max()) {
        operation_ids_exhausted = true;
    } else {
        ++next_operation_id;
    }
}

void fake_network::impl::issue_packet_id() noexcept {
    if (next_packet_id == std::numeric_limits<std::uint64_t>::max()) {
        packet_ids_exhausted = true;
    } else {
        ++next_packet_id;
    }
}

void fake_network::impl::issue_link_id() noexcept {
    if (next_link_id == std::numeric_limits<std::uint64_t>::max()) {
        link_ids_exhausted = true;
    } else {
        ++next_link_id;
    }
}

bool fake_network::impl::remove_backlog_pair(
  listener_state& listener, std::uint64_t pair) noexcept {
    bool removed = false;
    const auto count = listener.backlog.size();
    for (std::size_t index = 0; index < count; ++index) {
        const auto current = listener.backlog.front();
        listener.backlog.pop_front();
        if (!removed && current == pair) {
            removed = true;
        } else {
            listener.backlog.push_back(current);
        }
    }
    return removed;
}

runtime::result<event_trace::reservation>
fake_network::impl::reserve_event_trace(
  trace_event_kind kind,
  std::uint32_t domain,
  std::uint64_t stable_id,
  trace_action effect,
  std::uint64_t coordinate_a,
  std::uint64_t coordinate_b,
  std::uint64_t value,
  std::uint32_t result) {
    return scheduler_->reserve_trace(
      trace_event_descriptor{
        .kind = kind,
        .domain = domain,
        .stable_id = stable_id,
        .coordinate_a = coordinate_a,
        .coordinate_b = coordinate_b,
        .value = value,
        .result = result,
        .effect = effect,
      });
}

runtime::result<
  std::pair<scheduler::event_slot_reservation, event_trace::reservation>>
fake_network::impl::reserve_terminal(
  trace_event_kind kind,
  std::uint32_t domain,
  std::uint64_t stable_id,
  trace_action effect,
  std::uint64_t coordinate_a,
  std::uint64_t coordinate_b,
  std::uint64_t value,
  std::uint32_t result) {
    auto event = scheduler_->reserve_event_slot();
    if (!event) {
        return runtime::failure(event.error());
    }
    auto trace = reserve_event_trace(
      kind,
      domain,
      stable_id,
      effect,
      coordinate_a,
      coordinate_b,
      value,
      result);
    if (!trace) {
        return runtime::failure(trace.error());
    }
    return std::pair{std::move(*event), std::move(*trace)};
}

runtime::result<fake_network::impl::packet_trace_reservations>
fake_network::impl::reserve_packet_traces(
  std::uint64_t packet,
  std::uint64_t pair,
  std::uint8_t side,
  byte_count bytes) {
    auto flow_start = scheduler_->reserve_effect(
      trace_event_descriptor{
        .kind = trace_event_kind::bandwidth,
        .domain = static_cast<std::uint32_t>(bandwidth_trace_phase::flow_start),
        .stable_id = packet,
        .coordinate_a = pair,
        .coordinate_b = side,
        .value = bytes.value(),
        .effect = trace_action::flow_started,
      });
    auto transfer = scheduler_->reserve_effect(
      trace_event_descriptor{
        .kind = trace_event_kind::bandwidth,
        .domain = static_cast<std::uint32_t>(
          bandwidth_trace_phase::transfer_done),
        .stable_id = packet,
        .coordinate_a = pair,
        .coordinate_b = side,
        .value = bytes.value(),
        .effect = trace_action::transfer_completed,
      });
    auto packet_effect = scheduler_->reserve_effect(
      trace_event_descriptor{
        .kind = trace_event_kind::network,
        .domain = static_cast<std::uint32_t>(network_trace_phase::delivery),
        .stable_id = packet,
        .coordinate_a = pair,
        .coordinate_b = side,
        .value = bytes.value(),
        .effect = trace_action::packet_delivered,
      });
    auto sequence = scheduler_->reserve_effect(
      trace_event_descriptor{
        .kind = trace_event_kind::network,
        .domain = static_cast<std::uint32_t>(
          network_trace_phase::sequence_release),
        .stable_id = packet,
        .coordinate_a = pair,
        .coordinate_b = side,
        .effect = trace_action::network_operation_applied,
      });
    const auto empty_context = allocation_context(
      bandwidth_allocation_digest{});
    auto start_rebalance = scheduler_->reserve_effect(
      trace_event_descriptor{
        .kind = trace_event_kind::bandwidth,
        .domain = static_cast<std::uint32_t>(bandwidth_trace_phase::rebalance),
        .stable_id = packet,
        .effect = trace_action::bandwidth_rebalanced,
      },
      empty_context);
    auto finish_rebalance = scheduler_->reserve_effect(
      trace_event_descriptor{
        .kind = trace_event_kind::bandwidth,
        .domain = static_cast<std::uint32_t>(bandwidth_trace_phase::rebalance),
        .stable_id = packet,
        .effect = trace_action::bandwidth_rebalanced,
      },
      empty_context);
    auto start_wake = scheduler_->reserve_trace(
      trace_event_descriptor{
        .kind = trace_event_kind::bandwidth,
        .domain = static_cast<std::uint32_t>(
          bandwidth_trace_phase::transfer_done),
        .stable_id = packet,
      });
    auto finish_wake = scheduler_->reserve_trace(
      trace_event_descriptor{
        .kind = trace_event_kind::bandwidth,
        .domain = static_cast<std::uint32_t>(
          bandwidth_trace_phase::transfer_done),
        .stable_id = packet,
      });
    if (
      !flow_start || !transfer || !packet_effect || !sequence
      || !start_rebalance || !finish_rebalance || !start_wake || !finish_wake) {
        const auto error = !flow_start         ? flow_start.error()
                           : !transfer         ? transfer.error()
                           : !packet_effect    ? packet_effect.error()
                           : !sequence         ? sequence.error()
                           : !start_rebalance  ? start_rebalance.error()
                           : !finish_rebalance ? finish_rebalance.error()
                           : !start_wake       ? start_wake.error()
                                               : finish_wake.error();
        return runtime::failure(error);
    }
    return packet_trace_reservations{
      .flow_start = std::move(*flow_start),
      .transfer = std::move(*transfer),
      .packet_effect = std::move(*packet_effect),
      .sequence = std::move(*sequence),
      .start_rebalance = std::move(*start_rebalance),
      .finish_rebalance = std::move(*finish_rebalance),
      .start_wake = std::move(*start_wake),
      .finish_wake = std::move(*finish_wake),
    };
}

runtime::result<fake_network::impl::parked_credit>
fake_network::impl::reserve_parked(bool required) noexcept {
    if (!required) {
        return parked_credit{};
    }
    if (parked_operations >= config_.maximum_parked_operations) {
        return runtime::failure(network_error(errc::queue_full));
    }
    return parked_credit{parked_operations};
}

namespace {

[[nodiscard]] runtime::result<void> validate_config(
  const fake_network_config& config, const scheduler& events) noexcept {
    if (
      config.ephemeral_first == 0
      || config.ephemeral_first > config.ephemeral_last
      || config.maximum_listeners == 0 || config.maximum_connection_pairs == 0
      || config.maximum_pending_connects == 0
      || config.maximum_backlog_entries == 0 || config.maximum_operations == 0
      || config.maximum_parked_operations == 0
      || config.maximum_direction_bytes.value() == 0
      || config.maximum_packets == 0
      || config.maximum_packet_logical_bytes.value() == 0
      || config.maximum_packet_retained_bytes.value() == 0
      || config.maximum_direction_packets == 0 || config.maximum_links == 0
      || config.maximum_address_entries == 0 || config.maximum_active_flows == 0
      || config.maximum_controls == 0 || config.stop_batch == 0) {
        return runtime::failure(network_error(errc::invalid_argument));
    }
    if (
      config.maximum_listeners > maximum_fake_network_listeners
      || config.maximum_connection_pairs > maximum_fake_network_connection_pairs
      || config.maximum_pending_connects > maximum_fake_network_pending_connects
      || config.maximum_backlog_entries > maximum_fake_network_backlog_entries
      || config.maximum_operations > maximum_fake_network_operations
      || config.maximum_parked_operations
           > maximum_fake_network_parked_operations
      || config.maximum_direction_bytes > maximum_fake_network_direction_bytes
      || config.maximum_packets > maximum_fake_network_packets
      || config.maximum_packet_logical_bytes
           > maximum_fake_network_packet_logical_bytes
      || config.maximum_packet_retained_bytes
           > maximum_fake_network_packet_retained_bytes
      || config.maximum_direction_packets
           > maximum_fake_network_direction_packets
      || config.maximum_links > maximum_fake_network_links
      || config.maximum_address_entries > maximum_fake_network_address_entries
      || config.maximum_active_flows > maximum_bandwidth_flows
      || config.maximum_controls > maximum_fake_network_controls
      || config.stop_batch > maximum_fake_network_stop_batch) {
        return runtime::failure(network_error(errc::out_of_range));
    }
    if (
      config.maximum_pending_connects > config.maximum_connection_pairs
      || config.maximum_pending_connects > config.maximum_backlog_entries
      || config.maximum_operations < config.maximum_pending_connects
      || config.maximum_operations < config.maximum_listeners
      || config.maximum_direction_packets > config.maximum_packets
      || config.maximum_active_flows > config.maximum_packets) {
        return runtime::failure(network_error(errc::invalid_argument));
    }
    if (
      (config.ipv4_source
       && (config.ipv4_source->family()
             != runtime::network_address_family::ipv4
           || is_unspecified(*config.ipv4_source)))
      || (config.ipv6_source
          && (config.ipv6_source->family()
                != runtime::network_address_family::ipv6
              || is_unspecified(*config.ipv6_source)))) {
        return runtime::failure(network_error(errc::invalid_argument));
    }
    const std::array latencies{
      config.bind_latency,
      config.connect_latency,
      config.incoming_latency,
      config.accept_latency,
      config.latency_min,
      config.latency_mean_parameter,
      config.interframe_gap,
      config.reorder_window,
      config.fin_latency,
      config.close_latency,
    };
    if (std::ranges::any_of(latencies, [&](runtime::monotonic_duration value) {
            return value.nanoseconds()
                   > events.limits().maximum_deadline().nanoseconds();
        })) {
        return runtime::failure(network_error(errc::out_of_range));
    }
    if (
      config.latency_mean_parameter.nanoseconds()
      > (std::numeric_limits<std::uint64_t>::max() - 1U) / 2U) {
        return runtime::failure(network_error(errc::out_of_range));
    }
    const auto maximum_sample = std::max(
      config.latency_min.nanoseconds(),
      config.latency_mean_parameter.nanoseconds() * 2U);
    if (maximum_sample > events.limits().maximum_deadline().nanoseconds()) {
        return runtime::failure(network_error(errc::out_of_range));
    }
    const std::array capacities{
      config.egress_capacity, config.link_capacity, config.ingress_capacity};
    const bool has_zero_capacity = std::ranges::any_of(
      capacities, [](bandwidth_capacity capacity) {
          return !capacity.is_unlimited() && capacity.bytes_per_second() == 0;
      });
    std::optional<std::uint64_t> minimum_capacity;
    for (const auto capacity : capacities) {
        if (
          !capacity.is_unlimited() && capacity.bytes_per_second() != 0
          && (!minimum_capacity || capacity.bytes_per_second() < *minimum_capacity)) {
            minimum_capacity = capacity.bytes_per_second();
        }
    }
    if (!has_zero_capacity && minimum_capacity) {
        const auto maximum_packet_bytes = std::min(
          {config.maximum_direction_bytes.value(),
           config.maximum_packet_logical_bytes.value(),
           runtime::maximum_network_operation_bytes.value()});
        auto minimum_rate = bandwidth_fraction::ratio(
          *minimum_capacity, config.maximum_active_flows);
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          minimum_rate.has_value(),
          "validated bandwidth flow count produced an invalid rate");
        auto maximum_duration = bandwidth_duration(
          bandwidth_rate::finite(std::move(*minimum_rate)),
          bandwidth_fraction::whole(maximum_packet_bytes));
        if (!maximum_duration || !*maximum_duration) {
            return runtime::failure(network_error(errc::out_of_range));
        }
        const auto maximum_tail = std::max(
          maximum_sample, config.interframe_gap.nanoseconds());
        if (
          (*maximum_duration)->nanoseconds()
          > events.limits().maximum_deadline().nanoseconds() - maximum_tail) {
            return runtime::failure(network_error(errc::out_of_range));
        }
    }
    const auto owners = static_cast<std::uint64_t>(config.maximum_operations)
                        + config.maximum_packets
                        + config.maximum_connection_pairs
                        + config.maximum_listeners + config.maximum_links + 1U;
    const auto stop_batches = (owners + config.stop_batch - 1U)
                              / config.stop_batch;
    const auto required_events
      = static_cast<std::uint64_t>(config.maximum_listeners) * 3U
        + static_cast<std::uint64_t>(config.maximum_connection_pairs) * 8U
        + static_cast<std::uint64_t>(config.maximum_operations) * 4U
        + static_cast<std::uint64_t>(config.maximum_packets) * 3U + stop_batches
        + 1U;
    if (required_events > events.limits().pending_events()) {
        return runtime::failure(network_error(errc::out_of_range));
    }
    return {};
}

} // namespace

fake_network::fake_network(
  fake_network_config config,
  scheduler& event_scheduler,
  std::unique_ptr<impl> implementation) noexcept
  : config_(std::move(config))
  , scheduler_(&event_scheduler)
  , impl_(std::move(implementation)) {}

runtime::result<std::unique_ptr<fake_network>> fake_network::make(
  fake_network_config config,
  scheduler& event_scheduler,
  fault_schedule* faults) {
    if (auto valid = validate_config(config, event_scheduler); !valid) {
        return runtime::failure(valid.error());
    }
    auto planner = bandwidth_planner::make(config.maximum_active_flows);
    if (!planner) {
        return runtime::failure(planner.error());
    }
    auto staged_planner = bandwidth_planner::make(config.maximum_active_flows);
    if (!staged_planner) {
        return runtime::failure(staged_planner.error());
    }
    auto bandwidth_event = event_scheduler.reserve_event_slot();
    if (!bandwidth_event) {
        return runtime::failure(bandwidth_event.error());
    }
    const auto stop_owners
      = static_cast<std::uint64_t>(config.maximum_operations)
        + config.maximum_packets + config.maximum_connection_pairs
        + config.maximum_listeners + config.maximum_links + 1U;
    const auto maximum_stop_batches = (stop_owners + config.stop_batch - 1U)
                                      / config.stop_batch;
    seastar::chunked_fifo<scheduler::event_slot_reservation, 32, 2>
      stop_capacity;
    stop_capacity.reserve(maximum_stop_batches);
    for (std::uint64_t index = 0; index < maximum_stop_batches; ++index) {
        auto reserved = event_scheduler.reserve_event_slot();
        if (!reserved) {
            return runtime::failure(reserved.error());
        }
        stop_capacity.push_back(std::move(*reserved));
    }
    auto owner = std::unique_ptr<fake_network>{
      new fake_network(config, event_scheduler, nullptr)};
    owner->impl_ = std::make_unique<impl>(
      *owner,
      config,
      event_scheduler,
      std::move(*planner),
      std::move(*staged_planner),
      std::move(*bandwidth_event),
      std::move(stop_capacity),
      faults);
    return owner;
}

seastar::future<runtime::result<void>> fake_network::partition(
  runtime::network_address source, runtime::network_address target) {
    assert_current();
    return impl_->submit_control(
      impl::control_kind::partition,
      source,
      target,
      bandwidth_capacity::unlimited());
}

seastar::future<runtime::result<void>> fake_network::heal(
  runtime::network_address source, runtime::network_address target) {
    assert_current();
    return impl_->submit_control(
      impl::control_kind::heal,
      source,
      target,
      bandwidth_capacity::unlimited());
}

seastar::future<runtime::result<void>> fake_network::clog(
  runtime::network_address source, runtime::network_address target) {
    assert_current();
    return impl_->submit_control(
      impl::control_kind::clog,
      source,
      target,
      bandwidth_capacity::unlimited());
}

seastar::future<runtime::result<void>> fake_network::unclog(
  runtime::network_address source, runtime::network_address target) {
    assert_current();
    return impl_->submit_control(
      impl::control_kind::unclog,
      source,
      target,
      bandwidth_capacity::unlimited());
}

seastar::future<runtime::result<void>> fake_network::set_egress_capacity(
  runtime::network_address address, bandwidth_capacity capacity) {
    assert_current();
    return impl_->submit_control(
      impl::control_kind::egress, address, address, capacity);
}

seastar::future<runtime::result<void>> fake_network::set_link_capacity(
  runtime::network_address source,
  runtime::network_address target,
  bandwidth_capacity capacity) {
    assert_current();
    return impl_->submit_control(
      impl::control_kind::link, source, target, capacity);
}

seastar::future<runtime::result<void>> fake_network::set_ingress_capacity(
  runtime::network_address address, bandwidth_capacity capacity) {
    assert_current();
    return impl_->submit_control(
      impl::control_kind::ingress, address, address, capacity);
}

bandwidth_allocation_digest fake_network::allocation_digest() const noexcept {
    assert_current();
    return impl_->allocation_digest_;
}

std::size_t fake_network::active_operations() const noexcept {
    assert_current();
    return impl_->operations.size();
}

void fake_network::request_abort() noexcept {
    assert_current();
    impl_->abort_requested_ = true;
    impl_->activated_ = true;
}

seastar::future<runtime::result<void>> fake_network::stop() {
    assert_current();
    if (impl_->state_ == fake_network_state::stopping) {
        return impl_->stop_done_->get_shared_future();
    }
    if (impl_->state_ == fake_network_state::stopped) {
        return impl_->stop_done_ && impl_->stop_done_->available()
                 ? impl_->stop_done_->get_shared_future()
                 : seastar::make_ready_future<runtime::result<void>>(
                     runtime::result<void>{});
    }
    try {
        impl_->stop_done_.emplace();
    } catch (...) {
        return seastar::current_exception_as_future<runtime::result<void>>();
    }
    try {
        if (auto prepared = impl_->prepare_stop_batches(); !prepared) {
            impl_->stop_done_.reset();
            return seastar::make_ready_future<runtime::result<void>>(
              runtime::failure(prepared.error()));
        }
    } catch (...) {
        impl_->stop_done_.reset();
        return seastar::current_exception_as_future<runtime::result<void>>();
    }
    impl_->state_ = fake_network_state::stopping;
    impl_->abort_requested_ = true;
    impl_->schedule_stop_batch();
    return impl_->stop_done_->get_shared_future();
}

fake_network_state fake_network::state() const noexcept {
    assert_current();
    return impl_->state_;
}

bool fake_network::owner_stopped() const noexcept {
    assert_current();
    return impl_->state_ == fake_network_state::stopped;
}

void fake_network::force_discard_for_test(
  const runtime::operation_error& failure) noexcept {
    assert_current();
    impl_->force_discard_all(failure);
}

seastar::future<runtime::result<void>> fake_network::impl::submit_control(
  control_kind kind,
  runtime::network_address source,
  runtime::network_address target,
  bandwidth_capacity capacity) {
    if (state_ != fake_network_state::open || abort_requested_) {
        return ready_failure<void>(errc::closed);
    }
    if (
      is_unspecified(source)
      || ((kind == control_kind::partition || kind == control_kind::heal
           || kind == control_kind::clog || kind == control_kind::unclog
           || kind == control_kind::link)
          && (is_unspecified(target) || source.family() != target.family()))) {
        return ready_failure<void>(errc::invalid_argument);
    }
    if (
      active_controls == config_.maximum_controls
      || operations.size() == config_.maximum_operations
      || operation_ids_exhausted) {
        return ready_failure<void>(errc::queue_full);
    }
    if (!capacity.is_unlimited() && capacity.bytes_per_second() != 0) {
        auto minimum_rate = bandwidth_fraction::ratio(
          capacity.bytes_per_second(), config_.maximum_active_flows);
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          minimum_rate.has_value(),
          "validated control flow count produced an invalid rate");
        auto maximum_duration = bandwidth_duration(
          bandwidth_rate::finite(std::move(*minimum_rate)),
          bandwidth_fraction::whole(config_.maximum_direction_bytes.value()));
        if (!maximum_duration || !*maximum_duration) {
            return ready_failure<void>(errc::out_of_range);
        }
        auto deadline = scheduler_->now().checked_add(**maximum_duration);
        if (!deadline || *deadline > scheduler_->limits().maximum_deadline()) {
            return ready_failure<void>(errc::out_of_range);
        }
    }

    const bool link_control = kind == control_kind::partition
                              || kind == control_kind::heal
                              || kind == control_kind::clog
                              || kind == control_kind::unclog
                              || kind == control_kind::link;
    const directed_link_key key{.source = source, .target = target};
    auto* existing_link = link_control ? find_link(key) : nullptr;
    if (
      link_control && existing_link == nullptr
      && (links.size() == config_.maximum_links || link_ids_exhausted)) {
        return ready_failure<void>(errc::queue_full);
    }
    if (
      (kind == control_kind::egress && capacity != config_.egress_capacity
       && !egress_limits.contains(source)
       && persistent_address_entries() >= config_.maximum_address_entries)
      || (kind == control_kind::ingress
          && capacity != config_.ingress_capacity
          && !ingress_limits.contains(source)
          && persistent_address_entries()
               >= config_.maximum_address_entries)) {
        return ready_failure<void>(errc::queue_full);
    }

    const auto operation_id = next_operation_id;
    const auto phase = [&] {
        switch (kind) {
        case control_kind::partition:
            return network_control_trace_phase::partition;
        case control_kind::heal:
            return network_control_trace_phase::heal;
        case control_kind::clog:
            return network_control_trace_phase::clog;
        case control_kind::unclog:
            return network_control_trace_phase::unclog;
        case control_kind::egress:
            return network_control_trace_phase::egress_limit;
        case control_kind::link:
            return network_control_trace_phase::link_limit;
        case control_kind::ingress:
            return network_control_trace_phase::ingress_limit;
        }
        return network_control_trace_phase::partition;
    }();
    auto main = reserve_terminal(
      trace_event_kind::network_control,
      static_cast<std::uint32_t>(phase),
      operation_id,
      trace_action::network_control_applied,
      static_cast<std::uint64_t>(kind),
      0,
      capacity.is_unlimited() ? 0U : capacity.bytes_per_second(),
      capacity.is_unlimited() ? 1U : 0U);
    if (!main) {
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::failure(main.error()));
    }
    auto stop_terminal = reserve_terminal(
      trace_event_kind::network,
      static_cast<std::uint32_t>(network_trace_phase::stop),
      operation_id,
      trace_action::stop_terminal,
      0,
      0,
      0,
      static_cast<std::uint32_t>(errc::aborted));
    if (!stop_terminal) {
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::failure(stop_terminal.error()));
    }
    runtime::result<event_trace::reservation> rebalance_trace{
      event_trace::reservation{}};
    runtime::result<event_trace::reservation> wake_trace{
      event_trace::reservation{}};
    if (
      kind == control_kind::egress || kind == control_kind::link
      || kind == control_kind::ingress) {
        const auto empty_context = allocation_context(
          bandwidth_allocation_digest{});
        rebalance_trace = scheduler_->reserve_effect(
          trace_event_descriptor{
            .kind = trace_event_kind::bandwidth,
            .domain = static_cast<std::uint32_t>(
              bandwidth_trace_phase::rebalance),
            .stable_id = operation_id,
            .effect = trace_action::bandwidth_rebalanced,
          },
          empty_context);
        if (!rebalance_trace) {
            return seastar::make_ready_future<runtime::result<void>>(
              runtime::failure(rebalance_trace.error()));
        }
        wake_trace = scheduler_->reserve_trace(
          trace_event_descriptor{
            .kind = trace_event_kind::bandwidth,
            .domain = static_cast<std::uint32_t>(
              bandwidth_trace_phase::transfer_done),
            .stable_id = operation_id,
          });
        if (!wake_trace) {
            return seastar::make_ready_future<runtime::result<void>>(
              runtime::failure(wake_trace.error()));
        }
    }
    bool inserted_link = false;
    bool inserted_address = false;
    try {
        if (link_control && existing_link == nullptr) {
            auto prepared = std::make_unique<link_state>(
              next_link_id,
              key,
              config_.link_capacity,
              config_.latency_min,
              config_.latency_mean_parameter);
            const auto inserted = links.emplace(key, std::move(prepared));
            KWAQUE_INVARIANT(
              fake_network_state_invariant,
              inserted.second,
              "fake control duplicated a prepared link");
            inserted_link = true;
        }
        if (
          kind == control_kind::egress && capacity != config_.egress_capacity) {
            inserted_address = egress_limits
                                 .try_emplace(source, config_.egress_capacity)
                                 .second;
        } else if (
          kind == control_kind::ingress
          && capacity != config_.ingress_capacity) {
            inserted_address = ingress_limits
                                 .try_emplace(source, config_.ingress_capacity)
                                 .second;
        }
        control_operation control{
          .kind = kind,
          .source = source,
          .target = target,
          .capacity = capacity,
          .rebalance_trace = std::move(*rebalance_trace),
          .wake_trace = std::move(*wake_trace),
        };
        auto waiting = control.done.get_future();
        auto operation = std::make_unique<operation_state>(
          operation_id,
          operation_payload{
            std::in_place_type<control_operation>, std::move(control)});
        operation->stop_event = std::move(stop_terminal->first);
        operation->stop_trace = std::move(stop_terminal->second);
        operations.emplace(operation_id, std::move(operation));
        main->first.release();
        auto scheduled = scheduler_->schedule(
          scheduler_->now(),
          event_priority::normal(),
          [this, operation_id] noexcept { complete_control(operation_id); },
          trace_event_descriptor{
            .kind = trace_event_kind::network_control,
            .domain = static_cast<std::uint32_t>(phase),
            .stable_id = operation_id,
            .coordinate_a = static_cast<std::uint64_t>(kind),
            .value = capacity.is_unlimited() ? 0U : capacity.bytes_per_second(),
            .result = capacity.is_unlimited() ? 1U : 0U,
            .effect = trace_action::network_control_applied,
          },
          event_cleanup_policy::invoke,
          std::move(main->second));
        if (!scheduled) {
            operations.erase(operation_id);
            if (inserted_link) {
                links.erase(key);
            }
            if (inserted_address) {
                if (kind == control_kind::egress) {
                    egress_limits.erase(source);
                } else {
                    ingress_limits.erase(source);
                }
            }
            return seastar::make_ready_future<runtime::result<void>>(
              runtime::failure(scheduled.error()));
        }
        std::get<control_operation>(find_operation(operation_id)->payload).event
          = *scheduled;
        ++active_controls;
        issue_operation_id();
        if (inserted_link) {
            issue_link_id();
        }
        activated_ = true;
        return waiting;
    } catch (...) {
        operations.erase(operation_id);
        if (inserted_link) {
            links.erase(key);
        }
        if (inserted_address) {
            if (kind == control_kind::egress) {
                egress_limits.erase(source);
            } else {
                ingress_limits.erase(source);
            }
        }
        return seastar::current_exception_as_future<runtime::result<void>>();
    }
}

void fake_network::impl::complete_control(std::uint64_t operation_id) noexcept {
    auto* operation = find_operation(operation_id);
    if (operation == nullptr) {
        return;
    }
    auto& control = std::get<control_operation>(operation->payload);
    auto done = std::move(control.done);
    if (scheduler_->discarding_failed_event()) {
        const auto* failure = scheduler_->trace_failure();
        operations.erase(operation_id);
        --active_controls;
        done.set_value(
          runtime::failure(
            failure != nullptr ? *failure
                               : network_error(errc::replay_divergence)));
        return;
    }
    const directed_link_key key{
      .source = control.source, .target = control.target};
    bool applied = true;
    switch (control.kind) {
    case control_kind::partition:
        find_link(key)->partitioned = true;
        break;
    case control_kind::heal:
        find_link(key)->partitioned = false;
        break;
    case control_kind::clog:
        find_link(key)->clogged = true;
        break;
    case control_kind::unclog: {
        auto* link = find_link(key);
        link->clogged = false;
        schedule_ready_delivery(*link);
        break;
    }
    case control_kind::egress:
        applied = rebalance_bandwidth(
          std::move(control.rebalance_trace),
          std::move(control.wake_trace),
          operation_id,
          &control);
        break;
    case control_kind::link:
        applied = rebalance_bandwidth(
          std::move(control.rebalance_trace),
          std::move(control.wake_trace),
          operation_id,
          &control);
        break;
    case control_kind::ingress:
        applied = rebalance_bandwidth(
          std::move(control.rebalance_trace),
          std::move(control.wake_trace),
          operation_id,
          &control);
        break;
    }
    operations.erase(operation_id);
    --active_controls;
    if (!applied) {
        const auto* failure = scheduler_->trace_failure();
        done.set_value(
          runtime::failure(
            failure != nullptr ? *failure
                               : network_error(errc::replay_divergence)));
    } else {
        done.set_value(runtime::result<void>{});
    }
}

fake_network::~fake_network() {
    assert_current();
    if (impl_ == nullptr) {
        return;
    }
    if (scheduler_->trace_failed()) {
        const auto* failure = scheduler_->trace_failure();
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          failure != nullptr,
          "failed network scheduler has no trace error");
        static_cast<void>(scheduler_->discard_failed());
        impl_->force_discard_all(*failure);
    }
    const bool links_idle = std::ranges::all_of(
      impl_->links,
      [](const auto& entry) { return entry.second->packets == 0; });
    KWAQUE_INVARIANT(
      fake_network_drained_invariant,
      (impl_->state_ == fake_network_state::stopped
       || (!impl_->activated_ && impl_->state_ == fake_network_state::open))
        && impl_->listeners.empty() && impl_->pairs.empty()
        && impl_->operations.empty() && impl_->listener_registry.empty()
        && impl_->connection_locals.empty() && impl_->pending_connects == 0
        && impl_->backlog_entries == 0 && links_idle && impl_->live_packets == 0
        && impl_->parked_operations == 0 && impl_->egress_limits.empty()
        && impl_->ingress_limits.empty() && impl_->port_cursors.empty()
        && impl_->fault_occurrences_.empty()
        && impl_->packet_logical_bytes.value() == 0
        && impl_->packet_retained_bytes.value() == 0
        && impl_->active_controls == 0
        && impl_->free_packets.size() == config_.maximum_packets
        && impl_->free_flows.size() == config_.maximum_active_flows
        && !impl_->bandwidth_scheduled_ && impl_->stop_batches_.empty()
        && !impl_->stop_batch_scheduled_
        && (!impl_->activated_ || impl_->stop_event_capacity_.empty())
        && impl_->bandwidth_->allocation_count() == 0
        && impl_->bandwidth_->resource_count() == 0
        && impl_->bandwidth_->membership_count() == 0
        && impl_->staged_bandwidth_->allocation_count() == 0
        && impl_->staged_bandwidth_->resource_count() == 0
        && impl_->staged_bandwidth_->membership_count() == 0,
      "fake network destroyed with live ownership");
    impl_->links.clear();
}

fake_connection::fake_connection(
  fake_network& backend,
  std::uint64_t pair,
  std::uint8_t side,
  runtime::network_connection_limits limits)
  : backend_(&backend)
  , pair_(pair)
  , limits_(limits)
  , admission_(limits)
  , side_(side) {}

fake_connection::fake_connection(fake_connection&& other) noexcept
  : owner_(other.owner_)
  , backend_(std::exchange(other.backend_, nullptr))
  , pair_(std::exchange(other.pair_, 0))
  , limits_(other.limits_)
  , admission_(std::move(other.admission_))
  , side_(other.side_)
  , moved_from_(other.moved_from_) {
    owner_.assert_current();
    KWAQUE_INVARIANT(
      fake_network_handle_invariant,
      !moved_from_ && backend_ != nullptr
        && backend_->connection_movable(pair_, side_),
      "fake connection moved after first use");
    other.moved_from_ = true;
}

fake_connection::~fake_connection() {
    owner_.assert_current();
    if (moved_from_) {
        return;
    }
    if (backend_ != nullptr && backend_->owner_stopped()) {
        return;
    }
    KWAQUE_INVARIANT(
      fake_network_handle_invariant,
      backend_ != nullptr
        && backend_->connection_state(pair_, side_)
             == runtime::network_connection_state::closed,
      "fake connection destroyed before close completed");
    backend_->release_connection_handle(pair_, side_);
}

seastar::future<runtime::result<runtime::network_read_result>>
fake_connection::read(
  byte_count maximum_bytes, seastar::abort_source& caller_abort) {
    owner_.assert_current();
    return backend_->read(pair_, side_, maximum_bytes, caller_abort);
}

seastar::future<runtime::result<void>> fake_connection::write(
  bytes::fragmented_buffer data, seastar::abort_source& caller_abort) {
    owner_.assert_current();
    return backend_->write(
      pair_, side_, std::move(data), caller_abort, admission_);
}

runtime::network_endpoint fake_connection::local_endpoint() const noexcept {
    owner_.assert_current();
    return backend_->local_endpoint(pair_, side_);
}

runtime::network_endpoint fake_connection::remote_endpoint() const noexcept {
    owner_.assert_current();
    return backend_->remote_endpoint(pair_, side_);
}

runtime::network_connection_state fake_connection::state() const noexcept {
    owner_.assert_current();
    return backend_->connection_state(pair_, side_);
}

runtime::network_half_state fake_connection::input_state() const noexcept {
    owner_.assert_current();
    return backend_->input_state(pair_, side_);
}

runtime::network_half_state fake_connection::output_state() const noexcept {
    owner_.assert_current();
    return backend_->output_state(pair_, side_);
}

runtime::result<void> fake_connection::shutdown_input() {
    owner_.assert_current();
    return backend_->shutdown_input(pair_, side_);
}

runtime::result<void> fake_connection::shutdown_output() {
    owner_.assert_current();
    return backend_->shutdown_output(pair_, side_);
}

void fake_connection::request_abort() {
    owner_.assert_current();
    backend_->request_abort(pair_, side_);
}

seastar::future<runtime::result<void>> fake_connection::close() {
    owner_.assert_current();
    return backend_->close_connection(pair_, side_);
}

fake_listener::fake_listener(
  fake_network& backend, std::uint64_t listener) noexcept
  : backend_(&backend)
  , listener_(listener) {}

fake_listener::fake_listener(fake_listener&& other) noexcept
  : owner_(other.owner_)
  , backend_(std::exchange(other.backend_, nullptr))
  , listener_(std::exchange(other.listener_, 0))
  , moved_from_(other.moved_from_) {
    owner_.assert_current();
    KWAQUE_INVARIANT(
      fake_network_handle_invariant,
      !moved_from_ && backend_ != nullptr
        && backend_->listener_movable(listener_),
      "fake listener moved after first use");
    other.moved_from_ = true;
}

fake_listener::~fake_listener() {
    owner_.assert_current();
    if (moved_from_) {
        return;
    }
    if (backend_ != nullptr && backend_->owner_stopped()) {
        return;
    }
    KWAQUE_INVARIANT(
      fake_network_handle_invariant,
      backend_ != nullptr
        && backend_->impl_->find_listener(listener_) != nullptr
        && backend_->impl_->find_listener(listener_)->closed,
      "fake listener destroyed before close completed");
    backend_->release_listener_handle(listener_);
}

seastar::future<runtime::result<fake_connection>>
fake_listener::accept(seastar::abort_source& caller_abort) {
    owner_.assert_current();
    return backend_->accept(listener_, caller_abort);
}

runtime::network_endpoint fake_listener::local_endpoint() const noexcept {
    owner_.assert_current();
    return backend_->listener_endpoint(listener_);
}

const runtime::network_connection_limits&
fake_listener::connection_limits() const noexcept {
    owner_.assert_current();
    return backend_->listener_limits(listener_);
}

void fake_listener::request_abort() {
    owner_.assert_current();
    backend_->request_listener_abort(listener_);
}

seastar::future<runtime::result<void>> fake_listener::close() {
    owner_.assert_current();
    return backend_->close_listener(listener_);
}

seastar::future<runtime::result<fake_listener>> fake_network::listen(
  runtime::network_endpoint endpoint, runtime::network_listen_options options) {
    assert_current();
    if (impl_->state_ != fake_network_state::open || impl_->abort_requested_) {
        return ready_failure<fake_listener>(errc::closed);
    }
    if (auto valid = options.validate(); !valid) {
        return seastar::make_ready_future<runtime::result<fake_listener>>(
          runtime::failure(valid.error()));
    }
    if (
      impl_->listeners.size() == config_.maximum_listeners
      || impl_->operations.size() == config_.maximum_operations
      || impl_->listener_ids_exhausted || impl_->operation_ids_exhausted) {
        return ready_failure<fake_listener>(errc::queue_full);
    }

    std::optional<impl::port_selection> selected_port;
    if (endpoint.port() == 0) {
        auto selected = impl_->select_port(endpoint.address());
        if (!selected) {
            return seastar::make_ready_future<runtime::result<fake_listener>>(
              runtime::failure(selected.error()));
        }
        selected_port = *selected;
        endpoint = selected->endpoint;
        try {
            if (
              auto prepared = impl_->prepare_cursor(endpoint.address());
              !prepared) {
                return seastar::make_ready_future<
                  runtime::result<fake_listener>>(
                  runtime::failure(prepared.error()));
            }
        } catch (...) {
            return seastar::current_exception_as_future<
              runtime::result<fake_listener>>();
        }
    }
    if (impl_->listener_conflicts(endpoint)) {
        return ready_failure<fake_listener>(errc::network_failure);
    }
    const auto deadline = add_deadline(
      scheduler_->now(),
      config_.bind_latency,
      scheduler_->limits().maximum_deadline());
    if (!deadline) {
        return seastar::make_ready_future<runtime::result<fake_listener>>(
          runtime::failure(deadline.error()));
    }

    const auto listener_id = impl_->next_listener_id;
    const auto operation_id = impl_->next_operation_id;
    auto close_terminal = impl_->reserve_terminal(
      trace_event_kind::network,
      static_cast<std::uint32_t>(network_trace_phase::close),
      listener_id,
      trace_action::network_operation_applied);
    auto stop_terminal = impl_->reserve_terminal(
      trace_event_kind::network,
      static_cast<std::uint32_t>(network_trace_phase::stop),
      listener_id,
      trace_action::stop_terminal,
      0,
      0,
      0,
      static_cast<std::uint32_t>(errc::aborted));
    if (!close_terminal || !stop_terminal) {
        return seastar::make_ready_future<runtime::result<fake_listener>>(
          runtime::failure(
            !close_terminal ? close_terminal.error() : stop_terminal.error()));
    }
    auto trace = impl_->reserve_event_trace(
      trace_event_kind::network,
      static_cast<std::uint32_t>(network_trace_phase::bind),
      operation_id,
      trace_action::network_operation_applied,
      listener_id,
      endpoint.port());
    auto bind_terminal = impl_->reserve_terminal(
      trace_event_kind::network,
      static_cast<std::uint32_t>(network_trace_phase::stop),
      operation_id,
      trace_action::stop_terminal,
      0,
      0,
      0,
      static_cast<std::uint32_t>(errc::aborted));
    if (!trace || !bind_terminal) {
        return seastar::make_ready_future<runtime::result<fake_listener>>(
          runtime::failure(!trace ? trace.error() : bind_terminal.error()));
    }

    try {
        auto listener = std::make_unique<impl::listener_state>(
          listener_id, endpoint, options, config_.maximum_backlog_entries);
        listener->close_event = std::move(close_terminal->first);
        listener->close_trace = std::move(close_terminal->second);
        listener->stop_event = std::move(stop_terminal->first);
        listener->stop_trace = std::move(stop_terminal->second);
        impl::bind_operation binding{.listener = listener_id};
        auto waiting = binding.done.get_future();
        auto operation = std::make_unique<impl::operation_state>(
          operation_id,
          impl::operation_payload{
            std::in_place_type<impl::bind_operation>, std::move(binding)});
        operation->stop_event = std::move(bind_terminal->first);
        operation->stop_trace = std::move(bind_terminal->second);
        const auto [listener_position, listener_inserted]
          = impl_->listeners.emplace(listener_id, std::move(listener));
        static_cast<void>(listener_position);
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          listener_inserted,
          "duplicate fake listener ID");
        try {
            const auto [registry_position, registry_inserted]
              = impl_->listener_registry.emplace(endpoint, listener_id);
            static_cast<void>(registry_position);
            KWAQUE_INVARIANT(
              fake_network_state_invariant,
              registry_inserted,
              "validated fake listener endpoint was already registered");
            try {
                const auto [operation_position, operation_inserted]
                  = impl_->operations.emplace(
                    operation_id, std::move(operation));
                static_cast<void>(operation_position);
                KWAQUE_INVARIANT(
                  fake_network_state_invariant,
                  operation_inserted,
                  "duplicate fake bind operation ID");
            } catch (...) {
                impl_->listener_registry.erase(endpoint);
                throw;
            }
        } catch (...) {
            impl_->listeners.erase(listener_id);
            throw;
        }

        auto scheduled = scheduler_->schedule(
          *deadline,
          event_priority::normal(),
          [this, operation_id] noexcept { impl_->complete_bind(operation_id); },
          trace_event_descriptor{
            .kind = trace_event_kind::network,
            .domain = static_cast<std::uint32_t>(network_trace_phase::bind),
            .stable_id = operation_id,
            .coordinate_a = listener_id,
            .coordinate_b = endpoint.port(),
            .effect = trace_action::network_operation_applied,
          },
          event_cleanup_policy::invoke,
          std::move(*trace));
        if (!scheduled) {
            impl_->operations.erase(operation_id);
            impl_->listener_registry.erase(endpoint);
            impl_->listeners.erase(listener_id);
            return seastar::make_ready_future<runtime::result<fake_listener>>(
              runtime::failure(scheduled.error()));
        }
        if (selected_port) {
            impl_->commit_cursor(
              selected_port->endpoint.address(), selected_port->next_cursor);
        }
        std::get<impl::bind_operation>(
          impl_->find_operation(operation_id)->payload)
          .event = *scheduled;
        impl_->issue_listener_id();
        impl_->issue_operation_id();
        impl_->activated_ = true;
        return waiting;
    } catch (...) {
        return seastar::current_exception_as_future<
          runtime::result<fake_listener>>();
    }
}

void fake_network::impl::complete_bind(std::uint64_t operation_id) noexcept {
    auto* operation = find_operation(operation_id);
    if (operation == nullptr) {
        return;
    }
    auto& bind = std::get<bind_operation>(operation->payload);
    auto* listener = find_listener(bind.listener);
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      listener != nullptr,
      "fake bind completion lost listener state");
    auto done = std::move(bind.done);
    operations.erase(operation_id);
    if (scheduler_->discarding_failed_event()) {
        listener_registry.erase(listener->endpoint);
        listeners.erase(listener->id);
        const auto* failure = scheduler_->trace_failure();
        done.set_value(
          runtime::failure(
            failure != nullptr ? *failure
                               : network_error(errc::replay_divergence)));
        return;
    }
    listener->bound = true;
    listener->handle_owned = true;
    done.set_value(fake_listener{*owner_, listener->id});
}

seastar::future<runtime::result<fake_connection>> fake_network::connect(
  runtime::network_endpoint endpoint,
  std::optional<runtime::network_endpoint> local_endpoint,
  runtime::network_connection_limits limits,
  seastar::abort_source& caller_abort) {
    assert_current();
    if (impl_->state_ != fake_network_state::open || impl_->abort_requested_) {
        return ready_failure<fake_connection>(errc::closed);
    }
    if (auto valid = limits.validate(); !valid) {
        return seastar::make_ready_future<runtime::result<fake_connection>>(
          runtime::failure(valid.error()));
    }
    if (caller_abort.abort_requested()) {
        return ready_failure<fake_connection>(errc::aborted);
    }
    if (endpoint.port() == 0) {
        return ready_failure<fake_connection>(errc::invalid_argument);
    }
    const auto listener_id = impl_->lookup_listener(endpoint);
    auto* listener = listener_id ? impl_->find_listener(*listener_id) : nullptr;
    if (
      listener == nullptr || !listener->bound || listener->aborted
      || listener->closing || listener->closed) {
        return ready_failure<fake_connection>(errc::network_failure);
    }
    if (
      impl_->pairs.size() == config_.maximum_connection_pairs
      || impl_->pending_connects == config_.maximum_pending_connects
      || impl_->operations.size() == config_.maximum_operations
      || impl_->backlog_entries == config_.maximum_backlog_entries
      || listener->reserved_backlog + listener->backlog.size()
           >= listener->options.backlog
      || impl_->pair_ids_exhausted || impl_->operation_ids_exhausted) {
        return ready_failure<fake_connection>(errc::queue_full);
    }

    std::optional<impl::port_selection> selected_port;
    auto selected_local = impl_->select_local(
      endpoint, local_endpoint, selected_port);
    if (!selected_local) {
        return seastar::make_ready_future<runtime::result<fake_connection>>(
          runtime::failure(selected_local.error()));
    }
    if (selected_port) {
        try {
            if (
              auto prepared = impl_->prepare_cursor(
                selected_port->endpoint.address());
              !prepared) {
                return seastar::make_ready_future<
                  runtime::result<fake_connection>>(
                  runtime::failure(prepared.error()));
            }
        } catch (...) {
            return seastar::current_exception_as_future<
              runtime::result<fake_connection>>();
        }
    }
    const auto pair_id = impl_->next_pair_id;
    auto prepared_fault = impl_->prepare_fault(
      runtime::builtin_fault_point::connect,
      runtime::fault_object_key::from_u64(pair_id));
    if (!prepared_fault) {
        return seastar::make_ready_future<runtime::result<fake_connection>>(
          runtime::failure(prepared_fault.error()));
    }
    auto connect_latency = config_.connect_latency;
    if (prepared_fault->decision.action() == runtime::fault_action::delay) {
        auto combined = connect_latency.checked_add(
          *prepared_fault->decision.delay());
        if (!combined) {
            return ready_failure<fake_connection>(errc::out_of_range);
        }
        connect_latency = *combined;
    }
    const auto client_deadline = add_deadline(
      scheduler_->now(),
      connect_latency,
      scheduler_->limits().maximum_deadline());
    auto incoming_deadline = add_deadline(
      scheduler_->now(),
      config_.incoming_latency,
      scheduler_->limits().maximum_deadline());
    if (
      prepared_fault->decision.action() == runtime::fault_action::error
      && client_deadline) {
        incoming_deadline = client_deadline;
    }
    if (!client_deadline || !incoming_deadline) {
        return seastar::make_ready_future<runtime::result<fake_connection>>(
          runtime::failure(
            !client_deadline ? client_deadline.error()
                             : incoming_deadline.error()));
    }

    const auto operation_id = impl_->next_operation_id;
    std::array<
      std::pair<scheduler::event_slot_reservation, event_trace::reservation>,
      4>
      endpoint_terminals;
    for (std::size_t index = 0; index < endpoint_terminals.size(); ++index) {
        const auto phase = index < 2U ? network_trace_phase::fin
                                      : network_trace_phase::close;
        auto terminal = impl_->reserve_terminal(
          trace_event_kind::network,
          static_cast<std::uint32_t>(phase),
          pair_id,
          phase == network_trace_phase::close
            ? trace_action::network_operation_applied
            : trace_action::none,
          index % 2U);
        if (!terminal) {
            return seastar::make_ready_future<runtime::result<fake_connection>>(
              runtime::failure(terminal.error()));
        }
        endpoint_terminals[index] = std::move(*terminal);
    }
    std::array<
      std::pair<scheduler::event_slot_reservation, event_trace::reservation>,
      2>
      fin_ready_terminals;
    std::array<
      std::pair<scheduler::event_slot_reservation, event_trace::reservation>,
      2>
      endpoint_stop_terminals;
    for (std::size_t side = 0; side < fin_ready_terminals.size(); ++side) {
        auto ready = impl_->reserve_terminal(
          trace_event_kind::network,
          static_cast<std::uint32_t>(network_trace_phase::fin),
          pair_id,
          trace_action::none,
          side);
        if (!ready) {
            return seastar::make_ready_future<runtime::result<fake_connection>>(
              runtime::failure(ready.error()));
        }
        fin_ready_terminals[side] = std::move(*ready);
        auto stop_terminal = impl_->reserve_terminal(
          trace_event_kind::network,
          static_cast<std::uint32_t>(network_trace_phase::stop),
          pair_id,
          trace_action::stop_terminal,
          side,
          0,
          0,
          static_cast<std::uint32_t>(errc::aborted));
        if (!stop_terminal) {
            return seastar::make_ready_future<runtime::result<fake_connection>>(
              runtime::failure(stop_terminal.error()));
        }
        endpoint_stop_terminals[side] = std::move(*stop_terminal);
    }
    std::array<event_trace::reservation, 2> fin_effects;
    std::array<event_trace::reservation, 2> close_parked_effects;
    for (std::size_t side = 0; side < fin_effects.size(); ++side) {
        auto fin_effect = scheduler_->reserve_effect(
          trace_event_descriptor{
            .kind = trace_event_kind::network,
            .domain = static_cast<std::uint32_t>(network_trace_phase::fin),
            .stable_id = pair_id,
            .coordinate_a = side,
            .effect = trace_action::fin_delivered,
          });
        auto close_parked = scheduler_->reserve_effect(
          trace_event_descriptor{
            .kind = trace_event_kind::network,
            .domain = static_cast<std::uint32_t>(network_trace_phase::parked),
            .stable_id = pair_id,
            .coordinate_a = side,
            .effect = trace_action::operation_parked,
          });
        if (!fin_effect || !close_parked) {
            return seastar::make_ready_future<runtime::result<fake_connection>>(
              runtime::failure(
                !fin_effect ? fin_effect.error() : close_parked.error()));
        }
        fin_effects[side] = std::move(*fin_effect);
        close_parked_effects[side] = std::move(*close_parked);
    }
    auto reset_trace = scheduler_->reserve_effect(
      trace_event_descriptor{
        .kind = trace_event_kind::network,
        .domain = static_cast<std::uint32_t>(network_trace_phase::reset),
        .stable_id = pair_id,
        .effect = trace_action::reset_applied,
      });
    if (!reset_trace) {
        return seastar::make_ready_future<runtime::result<fake_connection>>(
          runtime::failure(reset_trace.error()));
    }
    auto client_terminal = impl_->reserve_terminal(
      trace_event_kind::network,
      static_cast<std::uint32_t>(network_trace_phase::connect_client),
      operation_id,
      trace_action::network_operation_applied,
      pair_id,
      0,
      0,
      static_cast<std::uint32_t>(prepared_fault->decision.action()));
    auto incoming_terminal = impl_->reserve_terminal(
      trace_event_kind::network,
      static_cast<std::uint32_t>(network_trace_phase::incoming),
      operation_id,
      trace_action::network_operation_applied,
      pair_id);
    auto abort_terminal = impl_->reserve_terminal(
      trace_event_kind::network,
      static_cast<std::uint32_t>(network_trace_phase::connect_client),
      operation_id,
      trace_action::network_operation_applied,
      pair_id,
      0,
      0,
      static_cast<std::uint32_t>(errc::aborted));
    auto operation_stop_terminal = impl_->reserve_terminal(
      trace_event_kind::network,
      static_cast<std::uint32_t>(network_trace_phase::stop),
      operation_id,
      trace_action::stop_terminal,
      0,
      0,
      0,
      static_cast<std::uint32_t>(errc::aborted));
    runtime::result<event_trace::reservation> connect_parked_trace{
      event_trace::reservation{}};
    if (
      prepared_fault->decision.action()
      == runtime::fault_action::drop_completion) {
        connect_parked_trace = scheduler_->reserve_effect(
          trace_event_descriptor{
            .kind = trace_event_kind::network,
            .domain = static_cast<std::uint32_t>(network_trace_phase::parked),
            .stable_id = operation_id,
            .effect = trace_action::operation_parked,
          });
    }
    if (
      !client_terminal || !incoming_terminal || !abort_terminal
      || !operation_stop_terminal || !connect_parked_trace) {
        const auto error = !client_terminal     ? client_terminal.error()
                           : !incoming_terminal ? incoming_terminal.error()
                           : !abort_terminal    ? abort_terminal.error()
                           : !operation_stop_terminal
                             ? operation_stop_terminal.error()
                             : connect_parked_trace.error();
        return seastar::make_ready_future<runtime::result<fake_connection>>(
          runtime::failure(error));
    }
    auto parked_credit = impl_->reserve_parked(
      prepared_fault->decision.action()
      == runtime::fault_action::drop_completion);
    if (!parked_credit) {
        return seastar::make_ready_future<runtime::result<fake_connection>>(
          runtime::failure(parked_credit.error()));
    }

    try {
        auto pair = std::make_unique<impl::pair_state>(
          pair_id,
          *selected_local,
          endpoint,
          limits,
          listener->options.connection_limits,
          config_.maximum_direction_packets);
        pair->reserved_client_local = *selected_local;
        for (std::size_t side = 0; side < 2; ++side) {
            pair->endpoints[side].fin_event_reservation = std::move(
              endpoint_terminals[side].first);
            pair->endpoints[side].fin_trace = std::move(
              endpoint_terminals[side].second);
            pair->endpoints[side].fin_effect_trace = std::move(
              fin_effects[side]);
            pair->endpoints[side].fin_ready_event_reservation = std::move(
              fin_ready_terminals[side].first);
            pair->endpoints[side].fin_ready_trace = std::move(
              fin_ready_terminals[side].second);
            pair->endpoints[side].close_event_reservation = std::move(
              endpoint_terminals[side + 2U].first);
            pair->endpoints[side].close_trace = std::move(
              endpoint_terminals[side + 2U].second);
            pair->endpoints[side].close_parked_trace = std::move(
              close_parked_effects[side]);
            pair->endpoints[side].stop_event_reservation = std::move(
              endpoint_stop_terminals[side].first);
            pair->endpoints[side].stop_trace = std::move(
              endpoint_stop_terminals[side].second);
        }
        pair->reset_trace = std::move(*reset_trace);
        impl::connect_operation connecting{
          .pair = pair_id,
          .listener = listener->id,
          .client_event_reservation = std::move(client_terminal->first),
          .incoming_event_reservation = std::move(incoming_terminal->first),
          .terminal_event = std::move(abort_terminal->first),
          .terminal_trace = std::move(abort_terminal->second),
          .parked_trace = std::move(*connect_parked_trace),
          .fault = prepared_fault->decision,
        };
        auto waiting = connecting.done.get_future();
        auto operation = std::make_unique<impl::operation_state>(
          operation_id,
          impl::operation_payload{
            std::in_place_type<impl::connect_operation>, std::move(connecting)},
          std::move(*parked_credit));
        operation->stop_event = std::move(operation_stop_terminal->first);
        operation->stop_trace = std::move(operation_stop_terminal->second);

        impl_->connection_locals.insert(*selected_local);
        try {
            impl_->pairs.emplace(pair_id, std::move(pair));
            try {
                impl_->operations.emplace(operation_id, std::move(operation));
            } catch (...) {
                impl_->pairs.erase(pair_id);
                throw;
            }
        } catch (...) {
            impl_->connection_locals.erase(*selected_local);
            throw;
        }

        auto* inserted = impl_->find_operation(operation_id);
        auto& connect_state = std::get<impl::connect_operation>(
          inserted->payload);
        connect_state.abort_subscription = caller_abort.subscribe(
          [this, operation_id] noexcept {
              impl_->abort_connect(operation_id);
          });
        if (!connect_state.abort_subscription) {
            impl_->operations.erase(operation_id);
            impl_->pairs.erase(pair_id);
            impl_->connection_locals.erase(*selected_local);
            return ready_failure<fake_connection>(errc::aborted);
        }

        auto committed = impl_->commit_fault(*prepared_fault);
        if (!committed) {
            impl_->operations.erase(operation_id);
            impl_->pairs.erase(pair_id);
            impl_->connection_locals.erase(*selected_local);
            return seastar::make_ready_future<runtime::result<fake_connection>>(
              runtime::failure(committed.error()));
        }

        connect_state.client_event_reservation.release();
        auto client_event = scheduler_->schedule(
          *client_deadline,
          event_priority::normal(),
          [this, operation_id] noexcept {
              impl_->complete_connect_client(operation_id);
          },
          trace_event_descriptor{
            .kind = trace_event_kind::network,
            .domain = static_cast<std::uint32_t>(
              network_trace_phase::connect_client),
            .stable_id = operation_id,
            .coordinate_a = pair_id,
            .result = static_cast<std::uint32_t>(
              prepared_fault->decision.action()),
            .effect = trace_action::network_operation_applied,
          },
          event_cleanup_policy::invoke,
          std::move(client_terminal->second));
        if (!client_event) {
            impl_->operations.erase(operation_id);
            impl_->pairs.erase(pair_id);
            impl_->connection_locals.erase(*selected_local);
            return seastar::make_ready_future<runtime::result<fake_connection>>(
              runtime::failure(client_event.error()));
        }
        connect_state.client_event = *client_event;
        connect_state.incoming_event_reservation.release();
        auto incoming_event = scheduler_->schedule(
          *incoming_deadline,
          event_priority::normal(),
          [this, operation_id] noexcept {
              impl_->complete_incoming(operation_id);
          },
          trace_event_descriptor{
            .kind = trace_event_kind::network,
            .domain = static_cast<std::uint32_t>(network_trace_phase::incoming),
            .stable_id = operation_id,
            .coordinate_a = pair_id,
            .effect = trace_action::network_operation_applied,
          },
          event_cleanup_policy::invoke,
          std::move(incoming_terminal->second));
        if (!incoming_event) {
            static_cast<void>(scheduler_->cancel(*client_event));
            impl_->operations.erase(operation_id);
            impl_->pairs.erase(pair_id);
            impl_->connection_locals.erase(*selected_local);
            return seastar::make_ready_future<runtime::result<fake_connection>>(
              runtime::failure(incoming_event.error()));
        }
        connect_state.incoming_event = *incoming_event;
        ++listener->reserved_backlog;
        ++listener->captured_connects;
        ++impl_->pending_connects;
        ++impl_->backlog_entries;
        if (selected_port) {
            impl_->commit_cursor(
              selected_port->endpoint.address(), selected_port->next_cursor);
        }
        impl_->issue_pair_id();
        impl_->issue_operation_id();
        impl_->activated_ = true;
        return waiting;
    } catch (...) {
        impl_->operations.erase(operation_id);
        impl_->pairs.erase(pair_id);
        impl_->connection_locals.erase(*selected_local);
        return seastar::current_exception_as_future<
          runtime::result<fake_connection>>();
    }
}

void fake_network::impl::maybe_erase_connect(
  std::uint64_t operation_id) noexcept {
    auto* operation = find_operation(operation_id);
    if (operation == nullptr) {
        return;
    }
    auto& connect = std::get<connect_operation>(operation->payload);
    if (connect.client_done && connect.incoming_done && !connect.parked) {
        operations.erase(operation_id);
    }
}

void fake_network::impl::complete_connect_client(
  std::uint64_t operation_id) noexcept {
    auto* operation = find_operation(operation_id);
    if (operation == nullptr) {
        return;
    }
    auto& connect = std::get<connect_operation>(operation->payload);
    if (connect.aborting) {
        return;
    }
    auto* pair = find_pair(connect.pair);
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      pair != nullptr && pending_connects != 0,
      "fake connect completion lost pair or pending accounting");
    connect.abort_subscription = std::nullopt;
    --pending_connects;
    connect.client_done = true;
    if (
      connect.fault.action() == runtime::fault_action::error
      || connect.fault.action() == runtime::fault_action::disconnect) {
        if (!connect.incoming_done) {
            static_cast<void>(scheduler_->cancel(connect.incoming_event));
            auto* listener = find_listener(connect.listener);
            KWAQUE_INVARIANT(
              fake_network_state_invariant,
              listener != nullptr && listener->captured_connects != 0
                && listener->reserved_backlog != 0 && backlog_entries != 0,
              "faulted connect lost incoming reservations");
            --listener->captured_connects;
            --listener->reserved_backlog;
            --backlog_entries;
            connect.incoming_done = true;
        }
        auto done = std::move(connect.done);
        const auto pair_id = pair->id;
        const auto code = connect.fault.action() == runtime::fault_action::error
                            ? errc::fault_injected
                            : errc::network_failure;
        reset_pair(pair_id, 0);
        maybe_erase_connect(operation_id);
        done.set_value(runtime::failure(network_error(code)));
        collect_pair(pair_id);
        return;
    }
    if (scheduler_->discarding_failed_event()) {
        const auto* failure = scheduler_->trace_failure();
        connect.done.set_value(
          runtime::failure(
            failure != nullptr ? *failure
                               : network_error(errc::replay_divergence)));
        pair->endpoints[0].state = runtime::network_connection_state::closed;
        pair->endpoints[0].input = runtime::network_half_state::shut_down;
        pair->endpoints[0].output = runtime::network_half_state::shut_down;
        maybe_erase_connect(operation_id);
        collect_pair(pair->id);
        return;
    }
    if (pair->endpoints[0].peer_reset) {
        pair->endpoints[0].state = runtime::network_connection_state::closed;
        pair->endpoints[0].input = runtime::network_half_state::shut_down;
        pair->endpoints[0].output = runtime::network_half_state::shut_down;
        connect.done.set_value(
          runtime::failure(network_error(errc::network_failure)));
    } else {
        if (connect.fault.action() == runtime::fault_action::drop_completion) {
            auto observed = scheduler_->observe_effect(
              trace_event_descriptor{
                .kind = trace_event_kind::network,
                .domain = static_cast<std::uint32_t>(
                  network_trace_phase::parked),
                .stable_id = operation_id,
                .effect = trace_action::operation_parked,
              },
              {},
              connect.parked_trace);
            if (!observed) {
                return;
            }
            connect.parked = true;
            return;
        }
        pair->endpoints[0].exposed = true;
        pair->endpoints[0].handle_owned = true;
        connect.done.set_value(
          fake_connection{*owner_, pair->id, 0, pair->endpoints[0].limits});
    }
    const auto pair_id = pair->id;
    maybe_erase_connect(operation_id);
    collect_pair(pair_id);
}

void fake_network::impl::complete_incoming(
  std::uint64_t operation_id) noexcept {
    auto* operation = find_operation(operation_id);
    if (operation == nullptr) {
        return;
    }
    auto& connect = std::get<connect_operation>(operation->payload);
    if (connect.aborting) {
        return;
    }
    auto* listener = find_listener(connect.listener);
    auto* pair = find_pair(connect.pair);
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      listener != nullptr && pair != nullptr && listener->captured_connects != 0
        && listener->reserved_backlog != 0,
      "fake incoming completion lost captured ownership");
    --listener->captured_connects;
    --listener->reserved_backlog;
    connect.incoming_done = true;

    if (
      scheduler_->discarding_failed_event() || listener->aborted
      || listener->closing || listener->closed) {
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          backlog_entries != 0,
          "fake incoming rollback lost backlog accounting");
        --backlog_entries;
        const auto pair_id = pair->id;
        reset_pair(pair_id, 1);
        maybe_erase_connect(operation_id);
        collect_pair(pair_id);
        collect_listener(listener->id);
        return;
    }

    try {
        listener->backlog.push_back(pair->id);
        pair->backlog_listener = listener->id;
    } catch (...) {
        --backlog_entries;
        reset_pair(pair->id, 1);
    }
    if (listener->accept_operation && pair->backlog_listener) {
        const auto accept_id = *listener->accept_operation;
        auto* accepted_operation = find_operation(accept_id);
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          accepted_operation != nullptr,
          "fake incoming lost pending accept");
        auto& accept = std::get<accept_operation>(accepted_operation->payload);
        if (!accept.scheduled) {
            const auto selected_pair_id = listener->backlog.front();
            listener->backlog.pop_front();
            KWAQUE_INVARIANT(
              fake_network_state_invariant,
              backlog_entries != 0,
              "fake incoming pop lost backlog accounting");
            --backlog_entries;
            auto* selected_pair = find_pair(selected_pair_id);
            KWAQUE_INVARIANT(
              fake_network_state_invariant,
              selected_pair != nullptr,
              "fake incoming pop lost connection pair");
            selected_pair->backlog_listener.reset();
            accept.pair = selected_pair_id;
            auto accept_latency = config_.accept_latency;
            if (accept.fault.action() == runtime::fault_action::delay) {
                auto combined = accept_latency.checked_add(
                  *accept.fault.delay());
                if (!combined) {
                    accept.done.set_value(
                      runtime::failure(network_error(errc::out_of_range)));
                    listener->accept_operation.reset();
                    operations.erase(accept_id);
                    reset_pair(selected_pair_id, 1);
                    return;
                }
                accept_latency = *combined;
            }
            const auto deadline = add_deadline(
              scheduler_->now(),
              accept_latency,
              scheduler_->limits().maximum_deadline());
            if (!deadline || !schedule_accept(accept_id, *deadline)) {
                accept.done.set_value(
                  runtime::failure(
                    deadline ? network_error(errc::resource_exhausted)
                             : deadline.error()));
                listener->accept_operation.reset();
                operations.erase(accept_id);
                reset_pair(selected_pair_id, 1);
            }
        }
    }
    const auto pair_id = pair->id;
    maybe_erase_connect(operation_id);
    collect_pair(pair_id);
    collect_listener(listener->id);
}

void fake_network::impl::abort_connect(std::uint64_t operation_id) noexcept {
    auto* operation = find_operation(operation_id);
    if (operation == nullptr) {
        return;
    }
    auto& connect = std::get<connect_operation>(operation->payload);
    if (connect.client_done || connect.aborting) {
        return;
    }
    connect.aborting = true;
    if (!connect.client_done && connect.client_event.valid()) {
        static_cast<void>(scheduler_->cancel(connect.client_event));
    }
    if (!connect.incoming_done && connect.incoming_event.valid()) {
        static_cast<void>(scheduler_->cancel(connect.incoming_event));
    }
    connect.terminal_event.release();
    auto scheduled = scheduler_->schedule(
      scheduler_->now(),
      event_priority::highest(),
      [this, operation_id] noexcept { complete_connect_abort(operation_id); },
      trace_event_descriptor{
        .kind = trace_event_kind::network,
        .domain = static_cast<std::uint32_t>(
          network_trace_phase::connect_client),
        .stable_id = operation_id,
        .coordinate_a = connect.pair,
        .result = static_cast<std::uint32_t>(errc::aborted),
        .effect = trace_action::network_operation_applied,
      },
      event_cleanup_policy::invoke,
      std::move(connect.terminal_trace));
    if (!scheduled) {
        complete_connect_abort(operation_id);
    }
}

void fake_network::impl::complete_connect_abort(
  std::uint64_t operation_id) noexcept {
    auto* operation = find_operation(operation_id);
    if (operation == nullptr) {
        return;
    }
    auto& connect = std::get<connect_operation>(operation->payload);
    auto* listener = find_listener(connect.listener);
    auto* pair = find_pair(connect.pair);
    connect.abort_subscription = std::nullopt;
    if (!connect.client_done) {
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          pending_connects != 0,
          "fake connect abort lost pending accounting");
        --pending_connects;
        connect.client_done = true;
        const auto* failure = scheduler_->discarding_failed_event()
                                ? scheduler_->trace_failure()
                                : nullptr;
        connect.done.set_value(
          runtime::failure(
            failure != nullptr ? *failure : network_error(errc::aborted)));
    }
    if (!connect.incoming_done) {
        if (listener != nullptr) {
            KWAQUE_INVARIANT(
              fake_network_state_invariant,
              listener->captured_connects != 0
                && listener->reserved_backlog != 0,
              "fake connect abort lost listener reservation");
            --listener->captured_connects;
            --listener->reserved_backlog;
        }
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          backlog_entries != 0,
          "fake connect abort lost backlog reservation");
        --backlog_entries;
        connect.incoming_done = true;
    } else if (
      listener != nullptr && remove_backlog_pair(*listener, connect.pair)) {
        if (pair != nullptr) {
            pair->backlog_listener.reset();
        }
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          backlog_entries != 0,
          "fake connect abort lost visible backlog accounting");
        --backlog_entries;
    }
    if (pair != nullptr) {
        reset_pair(pair->id, 0);
    }
    const auto pair_id = connect.pair;
    operations.erase(operation_id);
    collect_pair(pair_id);
    if (listener != nullptr) {
        collect_listener(listener->id);
    }
}

seastar::future<runtime::result<fake_connection>> fake_network::accept(
  std::uint64_t listener_id, seastar::abort_source& caller_abort) {
    assert_current();
    if (impl_->state_ != fake_network_state::open || impl_->abort_requested_) {
        return ready_failure<fake_connection>(errc::closed);
    }
    auto* listener = impl_->find_listener(listener_id);
    if (listener == nullptr || listener->closed || listener->closing) {
        return ready_failure<fake_connection>(errc::closed);
    }
    if (caller_abort.abort_requested() || listener->aborted) {
        return ready_failure<fake_connection>(errc::aborted);
    }
    if (listener->accept_operation) {
        return ready_failure<fake_connection>(errc::unavailable);
    }
    if (
      impl_->operations.size() == config_.maximum_operations
      || impl_->operation_ids_exhausted) {
        return ready_failure<fake_connection>(errc::queue_full);
    }
    auto prepared_fault = impl_->prepare_fault(
      runtime::builtin_fault_point::accept,
      runtime::fault_object_key::from_u64(listener_id));
    if (!prepared_fault) {
        return seastar::make_ready_future<runtime::result<fake_connection>>(
          runtime::failure(prepared_fault.error()));
    }
    const auto operation_id = impl_->next_operation_id;
    auto main = impl_->reserve_terminal(
      trace_event_kind::network,
      static_cast<std::uint32_t>(network_trace_phase::accept),
      operation_id,
      trace_action::network_operation_applied,
      listener_id,
      0,
      0,
      static_cast<std::uint32_t>(prepared_fault->decision.action()));
    auto terminal = impl_->reserve_terminal(
      trace_event_kind::network,
      static_cast<std::uint32_t>(network_trace_phase::accept),
      operation_id,
      trace_action::network_operation_applied,
      listener_id,
      0,
      0,
      static_cast<std::uint32_t>(errc::aborted));
    auto operation_stop_terminal = impl_->reserve_terminal(
      trace_event_kind::network,
      static_cast<std::uint32_t>(network_trace_phase::stop),
      operation_id,
      trace_action::stop_terminal,
      0,
      0,
      0,
      static_cast<std::uint32_t>(errc::aborted));
    runtime::result<event_trace::reservation> accept_parked_trace{
      event_trace::reservation{}};
    if (
      prepared_fault->decision.action()
      == runtime::fault_action::drop_completion) {
        accept_parked_trace = scheduler_->reserve_effect(
          trace_event_descriptor{
            .kind = trace_event_kind::network,
            .domain = static_cast<std::uint32_t>(network_trace_phase::parked),
            .stable_id = operation_id,
            .effect = trace_action::operation_parked,
          });
    }
    if (
      !main || !terminal || !operation_stop_terminal || !accept_parked_trace) {
        const auto error = !main       ? main.error()
                           : !terminal ? terminal.error()
                           : !operation_stop_terminal
                             ? operation_stop_terminal.error()
                             : accept_parked_trace.error();
        return seastar::make_ready_future<runtime::result<fake_connection>>(
          runtime::failure(error));
    }
    auto parked_credit = impl_->reserve_parked(
      prepared_fault->decision.action()
      == runtime::fault_action::drop_completion);
    if (!parked_credit) {
        return seastar::make_ready_future<runtime::result<fake_connection>>(
          runtime::failure(parked_credit.error()));
    }
    try {
        impl::accept_operation accepting{
          .listener = listener_id,
          .caller_abort = &caller_abort,
          .event_reservation = std::move(main->first),
          .trace = std::move(main->second),
          .terminal_event = std::move(terminal->first),
          .terminal_trace = std::move(terminal->second),
          .parked_trace = std::move(*accept_parked_trace),
          .fault = prepared_fault->decision,
        };
        auto waiting = accepting.done.get_future();
        auto operation = std::make_unique<impl::operation_state>(
          operation_id,
          impl::operation_payload{
            std::in_place_type<impl::accept_operation>, std::move(accepting)},
          std::move(*parked_credit));
        operation->stop_event = std::move(operation_stop_terminal->first);
        operation->stop_trace = std::move(operation_stop_terminal->second);
        impl_->operations.emplace(operation_id, std::move(operation));
        listener->accept_operation = operation_id;
        auto committed = impl_->commit_fault(*prepared_fault);
        if (!committed) {
            listener->accept_operation.reset();
            impl_->operations.erase(operation_id);
            return seastar::make_ready_future<runtime::result<fake_connection>>(
              runtime::failure(committed.error()));
        }
        impl_->issue_operation_id();
        impl_->activated_ = true;
        if (prepared_fault->decision.action() == runtime::fault_action::error) {
            if (
              auto scheduled = impl_->schedule_accept(
                operation_id, scheduler_->now());
              !scheduled) {
                listener->accept_operation.reset();
                impl_->operations.erase(operation_id);
                return seastar::make_ready_future<
                  runtime::result<fake_connection>>(
                  runtime::failure(scheduled.error()));
            }
            return waiting;
        }
        if (!listener->backlog.empty()) {
            const auto pair = listener->backlog.front();
            listener->backlog.pop_front();
            KWAQUE_INVARIANT(
              fake_network_state_invariant,
              impl_->backlog_entries != 0,
              "fake accept lost backlog accounting");
            --impl_->backlog_entries;
            if (
              auto* selected_pair = impl_->find_pair(pair);
              selected_pair != nullptr) {
                selected_pair->backlog_listener.reset();
            }
            auto& inserted = std::get<impl::accept_operation>(
              impl_->find_operation(operation_id)->payload);
            inserted.pair = pair;
            auto accept_latency = config_.accept_latency;
            if (inserted.fault.action() == runtime::fault_action::delay) {
                auto combined = accept_latency.checked_add(
                  *inserted.fault.delay());
                if (!combined) {
                    listener->accept_operation.reset();
                    impl_->operations.erase(operation_id);
                    impl_->reset_pair(pair, 1);
                    return ready_failure<fake_connection>(errc::out_of_range);
                }
                accept_latency = *combined;
            }
            const auto deadline = add_deadline(
              scheduler_->now(),
              accept_latency,
              scheduler_->limits().maximum_deadline());
            if (!deadline) {
                listener->accept_operation.reset();
                impl_->operations.erase(operation_id);
                impl_->reset_pair(pair, 1);
                return seastar::make_ready_future<
                  runtime::result<fake_connection>>(
                  runtime::failure(deadline.error()));
            }
            if (
              auto scheduled = impl_->schedule_accept(operation_id, *deadline);
              !scheduled) {
                listener->accept_operation.reset();
                impl_->operations.erase(operation_id);
                impl_->reset_pair(pair, 1);
                return seastar::make_ready_future<
                  runtime::result<fake_connection>>(
                  runtime::failure(scheduled.error()));
            }
        }
        return waiting;
    } catch (...) {
        return seastar::current_exception_as_future<
          runtime::result<fake_connection>>();
    }
}

runtime::result<void> fake_network::impl::schedule_accept(
  std::uint64_t operation_id, runtime::monotonic_time deadline) {
    auto* operation = find_operation(operation_id);
    if (operation == nullptr) {
        return runtime::failure(network_error(errc::invalid_argument));
    }
    auto& accept = std::get<accept_operation>(operation->payload);
    if (accept.scheduled) {
        return runtime::failure(network_error(errc::unavailable));
    }
    accept.event_reservation.release();
    auto event = scheduler_->schedule(
      deadline,
      event_priority::normal(),
      [this, operation_id] noexcept { complete_accept(operation_id); },
      trace_event_descriptor{
        .kind = trace_event_kind::network,
        .domain = static_cast<std::uint32_t>(network_trace_phase::accept),
        .stable_id = operation_id,
        .coordinate_a = accept.listener,
        .result = static_cast<std::uint32_t>(accept.fault.action()),
        .effect = trace_action::network_operation_applied,
      },
      event_cleanup_policy::invoke,
      std::move(accept.trace));
    if (!event) {
        return runtime::failure(event.error());
    }
    accept.event = *event;
    accept.scheduled = true;
    return {};
}

void fake_network::impl::complete_accept(std::uint64_t operation_id) noexcept {
    auto* operation = find_operation(operation_id);
    if (operation == nullptr) {
        return;
    }
    auto& accept = std::get<accept_operation>(operation->payload);
    auto* listener = find_listener(accept.listener);
    auto* pair = accept.pair ? find_pair(*accept.pair) : nullptr;
    if (listener != nullptr && listener->accept_operation == operation_id) {
        listener->accept_operation.reset();
    }
    if (accept.fault.action() == runtime::fault_action::error) {
        auto done = std::move(accept.done);
        operations.erase(operation_id);
        done.set_value(runtime::failure(network_error(errc::fault_injected)));
        return;
    }
    if (
      accept.fault.action() == runtime::fault_action::disconnect
      && pair != nullptr) {
        auto done = std::move(accept.done);
        const auto selected_pair = pair->id;
        operations.erase(operation_id);
        reset_pair(selected_pair, 1);
        done.set_value(runtime::failure(network_error(errc::network_failure)));
        return;
    }
    if (
      accept.fault.action() == runtime::fault_action::drop_completion
      && pair != nullptr) {
        auto observed = scheduler_->observe_effect(
          trace_event_descriptor{
            .kind = trace_event_kind::network,
            .domain = static_cast<std::uint32_t>(network_trace_phase::parked),
            .stable_id = operation_id,
            .effect = trace_action::operation_parked,
          },
          {},
          accept.parked_trace);
        if (!observed) {
            return;
        }
        accept.parked = true;
        return;
    }
    auto done = std::move(accept.done);
    const bool rejected = scheduler_->discarding_failed_event()
                          || listener == nullptr || listener->aborted
                          || listener->closing || listener->closed
                          || accept.caller_abort == nullptr
                          || accept.caller_abort->abort_requested()
                          || pair == nullptr || pair->endpoints[1].peer_reset;
    const auto pair_id = accept.pair;
    operations.erase(operation_id);
    if (rejected) {
        if (pair_id) {
            reset_pair(*pair_id, 1);
        }
        const auto* failure = scheduler_->discarding_failed_event()
                                ? scheduler_->trace_failure()
                                : nullptr;
        done.set_value(
          runtime::failure(
            failure != nullptr ? *failure : network_error(errc::aborted)));
    } else {
        pair->endpoints[1].exposed = true;
        pair->endpoints[1].handle_owned = true;
        done.set_value(
          fake_connection{*owner_, pair->id, 1, pair->endpoints[1].limits});
    }
    if (listener != nullptr) {
        collect_listener(listener->id);
    }
}

void fake_network::impl::complete_accept_abort(
  std::uint64_t operation_id) noexcept {
    auto* operation = find_operation(operation_id);
    if (operation == nullptr) {
        return;
    }
    auto& accept = std::get<accept_operation>(operation->payload);
    auto* listener = find_listener(accept.listener);
    auto done = std::move(accept.done);
    const auto pair = accept.pair;
    if (listener != nullptr && listener->accept_operation == operation_id) {
        listener->accept_operation.reset();
    }
    operations.erase(operation_id);
    if (pair) {
        reset_pair(*pair, 1);
    }
    const auto* failure = scheduler_->discarding_failed_event()
                            ? scheduler_->trace_failure()
                            : nullptr;
    done.set_value(
      runtime::failure(
        failure != nullptr ? *failure : network_error(errc::aborted)));
    if (listener != nullptr) {
        collect_listener(listener->id);
    }
}

runtime::network_endpoint
fake_network::listener_endpoint(std::uint64_t listener) const noexcept {
    assert_current();
    const auto* state = impl_->find_listener(listener);
    KWAQUE_INVARIANT(
      fake_network_handle_invariant,
      state != nullptr,
      "fake listener endpoint lost state");
    return state->endpoint;
}

const runtime::network_connection_limits&
fake_network::listener_limits(std::uint64_t listener) const noexcept {
    assert_current();
    const auto* state = impl_->find_listener(listener);
    KWAQUE_INVARIANT(
      fake_network_handle_invariant,
      state != nullptr,
      "fake listener limits lost state");
    return state->options.connection_limits;
}

void fake_network::request_listener_abort(std::uint64_t listener_id) noexcept {
    assert_current();
    auto* listener = impl_->find_listener(listener_id);
    if (listener == nullptr || listener->aborted || listener->closed) {
        return;
    }
    listener->aborted = true;
    if (!listener->accept_operation) {
        return;
    }
    const auto operation_id = *listener->accept_operation;
    auto* operation = impl_->find_operation(operation_id);
    if (operation == nullptr) {
        return;
    }
    auto& accept = std::get<impl::accept_operation>(operation->payload);
    if (accept.scheduled) {
        static_cast<void>(scheduler_->cancel(accept.event));
    }
    accept.terminal_event.release();
    auto terminal = scheduler_->schedule(
      scheduler_->now(),
      event_priority::highest(),
      [this, operation_id] noexcept {
          impl_->complete_accept_abort(operation_id);
      },
      trace_event_descriptor{
        .kind = trace_event_kind::network,
        .domain = static_cast<std::uint32_t>(network_trace_phase::accept),
        .stable_id = operation_id,
        .coordinate_a = listener_id,
        .result = static_cast<std::uint32_t>(errc::aborted),
        .effect = trace_action::network_operation_applied,
      },
      event_cleanup_policy::invoke,
      std::move(accept.terminal_trace));
    if (!terminal) {
        impl_->complete_accept_abort(operation_id);
    }
}

seastar::future<runtime::result<void>>
fake_network::close_listener(std::uint64_t listener_id) {
    assert_current();
    auto* listener = impl_->find_listener(listener_id);
    if (listener == nullptr || listener->closed) {
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::result<void>{});
    }
    if (listener->closing) {
        return listener->close_done->get_shared_future();
    }
    try {
        listener->close_done.emplace();
    } catch (...) {
        return seastar::current_exception_as_future<runtime::result<void>>();
    }
    const auto deadline = add_deadline(
      scheduler_->now(),
      config_.close_latency,
      scheduler_->limits().maximum_deadline());
    if (!deadline) {
        listener->close_done.reset();
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::failure(deadline.error()));
    }
    listener->closing = true;
    request_listener_abort(listener_id);
    listener->close_event.release();
    auto closed = scheduler_->schedule(
      *deadline,
      event_priority::normal(),
      [this, listener_id] noexcept {
          impl_->complete_listener_close(listener_id);
      },
      trace_event_descriptor{
        .kind = trace_event_kind::network,
        .domain = static_cast<std::uint32_t>(network_trace_phase::close),
        .stable_id = listener_id,
        .effect = trace_action::network_operation_applied,
      },
      event_cleanup_policy::invoke,
      std::move(listener->close_trace));
    if (!closed) {
        listener->closing = false;
        listener->close_done->set_value(runtime::failure(closed.error()));
    } else {
        listener->close_event_id = *closed;
    }
    return listener->close_done->get_shared_future();
}

void fake_network::impl::complete_listener_close(
  std::uint64_t listener_id) noexcept {
    auto* listener = find_listener(listener_id);
    if (listener == nullptr) {
        return;
    }
    const auto registered = listener_registry.find(listener->endpoint);
    if (
      registered != listener_registry.end()
      && registered->second == listener_id) {
        listener_registry.erase(registered);
    }
    while (!listener->backlog.empty()) {
        const auto pair = listener->backlog.front();
        listener->backlog.pop_front();
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          backlog_entries != 0,
          "fake listener close lost backlog accounting");
        --backlog_entries;
        if (auto* selected_pair = find_pair(pair); selected_pair != nullptr) {
            selected_pair->backlog_listener.reset();
        }
        reset_pair(pair, 1);
    }
    work_ids.clear();
    for (const auto& [operation_id, operation] : operations) {
        static_cast<void>(operation_id);
        if (
          const auto* connect = std::get_if<connect_operation>(
            &operation->payload);
          connect != nullptr && connect->listener == listener_id
          && !connect->incoming_done) {
            work_ids.push_back(connect->pair);
        }
    }
    for (const auto pair : work_ids) {
        reset_pair(pair, 1);
    }
    work_ids.clear();
    listener->closed = true;
    listener->closing = false;
    const auto* failure = scheduler_->discarding_failed_event()
                            ? scheduler_->trace_failure()
                            : nullptr;
    if (failure != nullptr) {
        listener->close_done->set_value(runtime::failure(*failure));
    } else {
        listener->close_done->set_value(runtime::result<void>{});
    }
}

void fake_network::release_listener_handle(std::uint64_t listener_id) noexcept {
    assert_current();
    auto* listener = impl_->find_listener(listener_id);
    if (listener == nullptr && impl_->state_ == fake_network_state::stopped) {
        return;
    }
    KWAQUE_INVARIANT(
      fake_network_handle_invariant,
      listener != nullptr && listener->closed && listener->handle_owned,
      "fake listener handle release observed invalid state");
    listener->handle_owned = false;
    impl_->collect_listener(listener_id);
}

bool fake_network::listener_movable(std::uint64_t listener_id) const noexcept {
    assert_current();
    const auto* listener = impl_->find_listener(listener_id);
    return listener != nullptr && listener->bound && !listener->aborted
           && !listener->closing && !listener->closed
           && !listener->accept_operation;
}

void fake_network::impl::collect_listener(std::uint64_t listener_id) noexcept {
    auto* listener = find_listener(listener_id);
    if (
      listener == nullptr || listener->handle_owned || !listener->closed
      || listener->accept_operation || listener->captured_connects != 0
      || listener->reserved_backlog != 0 || !listener->backlog.empty()) {
        return;
    }
    fault_occurrences_.erase(
      network_fault_key{
        .point = runtime::builtin_fault_point::accept,
        .object = runtime::fault_object_key::from_u64(listener_id),
      });
    listeners.erase(listener_id);
}

runtime::network_endpoint fake_network::local_endpoint(
  std::uint64_t pair_id, std::uint8_t side) const noexcept {
    assert_current();
    const auto* pair = impl_->find_pair(pair_id);
    KWAQUE_INVARIANT(
      fake_network_handle_invariant,
      pair != nullptr && side < 2,
      "fake connection local endpoint lost state");
    return pair->endpoints[side].local;
}

runtime::network_endpoint fake_network::remote_endpoint(
  std::uint64_t pair_id, std::uint8_t side) const noexcept {
    assert_current();
    const auto* pair = impl_->find_pair(pair_id);
    KWAQUE_INVARIANT(
      fake_network_handle_invariant,
      pair != nullptr && side < 2,
      "fake connection remote endpoint lost state");
    return pair->endpoints[side].remote;
}

runtime::network_connection_state fake_network::connection_state(
  std::uint64_t pair_id, std::uint8_t side) const noexcept {
    assert_current();
    const auto* pair = impl_->find_pair(pair_id);
    KWAQUE_INVARIANT(
      fake_network_handle_invariant,
      pair != nullptr && side < 2,
      "fake connection lifecycle lost state");
    return pair->endpoints[side].state;
}

runtime::network_half_state fake_network::input_state(
  std::uint64_t pair_id, std::uint8_t side) const noexcept {
    assert_current();
    const auto* pair = impl_->find_pair(pair_id);
    KWAQUE_INVARIANT(
      fake_network_handle_invariant,
      pair != nullptr && side < 2,
      "fake connection input state was lost");
    return pair->endpoints[side].input;
}

runtime::network_half_state fake_network::output_state(
  std::uint64_t pair_id, std::uint8_t side) const noexcept {
    assert_current();
    const auto* pair = impl_->find_pair(pair_id);
    KWAQUE_INVARIANT(
      fake_network_handle_invariant,
      pair != nullptr && side < 2,
      "fake connection output state was lost");
    return pair->endpoints[side].output;
}

bool fake_network::connection_movable(
  std::uint64_t pair_id, std::uint8_t side) const noexcept {
    assert_current();
    const auto* pair = impl_->find_pair(pair_id);
    return pair != nullptr && side < 2
           && pair->endpoints[side].state
                == runtime::network_connection_state::open
           && !pair->endpoints[side].read_operation;
}

seastar::future<runtime::result<runtime::network_read_result>>
fake_network::read(
  std::uint64_t pair_id,
  std::uint8_t side,
  byte_count maximum_bytes,
  seastar::abort_source& caller_abort) {
    assert_current();
    if (impl_->state_ != fake_network_state::open || impl_->abort_requested_) {
        return ready_failure<runtime::network_read_result>(errc::closed);
    }
    if (
      auto valid = runtime::validate_network_read_limit(maximum_bytes);
      !valid) {
        return seastar::make_ready_future<
          runtime::result<runtime::network_read_result>>(
          runtime::failure(valid.error()));
    }
    if (caller_abort.abort_requested()) {
        return ready_failure<runtime::network_read_result>(errc::aborted);
    }
    auto* pair = impl_->find_pair(pair_id);
    if (pair == nullptr || side >= 2) {
        return ready_failure<runtime::network_read_result>(errc::closed);
    }
    auto& endpoint = pair->endpoints[side];
    if (endpoint.state != runtime::network_connection_state::open) {
        return ready_failure<runtime::network_read_result>(errc::closed);
    }
    if (endpoint.abort_requested) {
        return ready_failure<runtime::network_read_result>(errc::aborted);
    }
    if (endpoint.peer_reset) {
        return ready_failure<runtime::network_read_result>(
          errc::network_failure);
    }
    if (endpoint.input != runtime::network_half_state::open) {
        return ready_failure<runtime::network_read_result>(errc::closed);
    }
    if (endpoint.read_operation) {
        return ready_failure<runtime::network_read_result>(errc::unavailable);
    }
    if (
      impl_->operations.size() == config_.maximum_operations
      || impl_->operation_ids_exhausted) {
        return ready_failure<runtime::network_read_result>(errc::queue_full);
    }
    auto prepared_fault = impl_->prepare_fault(
      runtime::builtin_fault_point::network_read,
      network_object_key(pair_id, side));
    if (!prepared_fault) {
        return seastar::make_ready_future<
          runtime::result<runtime::network_read_result>>(
          runtime::failure(prepared_fault.error()));
    }
    auto decision = prepared_fault->decision;
    std::uint64_t fault_a = 0;
    std::uint64_t fault_b = 0;
    if (decision.action() == runtime::fault_action::short_operation) {
        maximum_bytes = byte_count{std::min(
          maximum_bytes.value(), decision.short_operation_bytes()->value())};
    } else if (decision.action() == runtime::fault_action::corrupt) {
        auto selected = prepared_fault->prepared->draw_bounded(
          maximum_bytes.value());
        auto bit = prepared_fault->prepared->draw_bounded(8U);
        if (!selected || !bit) {
            return seastar::make_ready_future<
              runtime::result<runtime::network_read_result>>(
              runtime::failure(!selected ? selected.error() : bit.error()));
        }
        fault_a = *selected;
        fault_b = *bit;
    }
    const auto operation_id = impl_->next_operation_id;
    auto main = impl_->reserve_terminal(
      trace_event_kind::network,
      static_cast<std::uint32_t>(network_trace_phase::read),
      operation_id,
      trace_action::network_operation_applied,
      pair_id,
      side,
      maximum_bytes.value(),
      static_cast<std::uint32_t>(decision.action()));
    auto terminal = impl_->reserve_terminal(
      trace_event_kind::network,
      static_cast<std::uint32_t>(network_trace_phase::read),
      operation_id,
      trace_action::network_operation_applied,
      pair_id,
      side,
      maximum_bytes.value(),
      static_cast<std::uint32_t>(errc::aborted));
    auto operation_stop_terminal = impl_->reserve_terminal(
      trace_event_kind::network,
      static_cast<std::uint32_t>(network_trace_phase::stop),
      operation_id,
      trace_action::stop_terminal,
      0,
      0,
      0,
      static_cast<std::uint32_t>(errc::aborted));
    runtime::result<event_trace::reservation> read_parked_trace{
      event_trace::reservation{}};
    if (
      decision.action() == runtime::fault_action::drop_completion
      || decision.action() == runtime::fault_action::drop) {
        read_parked_trace = scheduler_->reserve_effect(
          trace_event_descriptor{
            .kind = trace_event_kind::network,
            .domain = static_cast<std::uint32_t>(network_trace_phase::parked),
            .stable_id = operation_id,
            .effect = trace_action::operation_parked,
          });
    }
    if (!main || !terminal || !operation_stop_terminal || !read_parked_trace) {
        const auto error = !main       ? main.error()
                           : !terminal ? terminal.error()
                           : !operation_stop_terminal
                             ? operation_stop_terminal.error()
                             : read_parked_trace.error();
        return seastar::make_ready_future<
          runtime::result<runtime::network_read_result>>(
          runtime::failure(error));
    }
    auto parked_credit = impl_->reserve_parked(
      decision.action() == runtime::fault_action::drop
      || decision.action() == runtime::fault_action::drop_completion);
    if (!parked_credit) {
        return seastar::make_ready_future<
          runtime::result<runtime::network_read_result>>(
          runtime::failure(parked_credit.error()));
    }
    try {
        impl::read_operation reading{
          .pair = pair_id,
          .maximum_bytes = maximum_bytes,
          .event_reservation = std::move(main->first),
          .trace = std::move(main->second),
          .terminal_event = std::move(terminal->first),
          .terminal_trace = std::move(terminal->second),
          .parked_trace = std::move(*read_parked_trace),
          .fault = decision,
          .fault_a = fault_a,
          .fault_b = fault_b,
          .side = side,
        };
        auto waiting = reading.done.get_future();
        auto operation = std::make_unique<impl::operation_state>(
          operation_id,
          impl::operation_payload{
            std::in_place_type<impl::read_operation>, std::move(reading)},
          std::move(*parked_credit));
        operation->stop_event = std::move(operation_stop_terminal->first);
        operation->stop_trace = std::move(operation_stop_terminal->second);
        impl_->operations.emplace(operation_id, std::move(operation));
        endpoint.read_operation = operation_id;
        auto committed = impl_->commit_fault(*prepared_fault);
        if (!committed) {
            endpoint.read_operation.reset();
            impl_->operations.erase(operation_id);
            return seastar::make_ready_future<
              runtime::result<runtime::network_read_result>>(
              runtime::failure(committed.error()));
        }
        impl_->issue_operation_id();
        impl_->activated_ = true;
        const auto& incoming = pair->directions[other_side(side)];
        const bool readable = incoming.fin_delivered
                              || !incoming.delivered.empty();
        const bool immediate = decision.action() == runtime::fault_action::error
                               || decision.action()
                                    == runtime::fault_action::disconnect;
        if (readable || immediate) {
            auto deadline = scheduler_->now();
            if (decision.action() == runtime::fault_action::delay) {
                auto delayed = deadline.checked_add(*decision.delay());
                if (!delayed) {
                    endpoint.read_operation.reset();
                    impl_->operations.erase(operation_id);
                    return ready_failure<runtime::network_read_result>(
                      errc::out_of_range);
                }
                deadline = *delayed;
            }
            if (
              auto scheduled = impl_->schedule_read(operation_id, deadline);
              !scheduled) {
                endpoint.read_operation.reset();
                impl_->operations.erase(operation_id);
                return seastar::make_ready_future<
                  runtime::result<runtime::network_read_result>>(
                  runtime::failure(scheduled.error()));
            }
        }
        return waiting;
    } catch (...) {
        return seastar::current_exception_as_future<
          runtime::result<runtime::network_read_result>>();
    }
}

runtime::result<void> fake_network::impl::schedule_read(
  std::uint64_t operation_id, runtime::monotonic_time deadline) {
    auto* operation = find_operation(operation_id);
    if (operation == nullptr) {
        return runtime::failure(network_error(errc::invalid_argument));
    }
    auto& read = std::get<read_operation>(operation->payload);
    if (read.scheduled) {
        return {};
    }
    read.event_reservation.release();
    auto event = scheduler_->schedule(
      deadline,
      event_priority::normal(),
      [this, operation_id] noexcept { complete_read(operation_id); },
      trace_event_descriptor{
        .kind = trace_event_kind::network,
        .domain = static_cast<std::uint32_t>(network_trace_phase::read),
        .stable_id = operation_id,
        .coordinate_a = read.pair,
        .coordinate_b = read.side,
        .value = read.maximum_bytes.value(),
        .result = static_cast<std::uint32_t>(read.fault.action()),
        .effect = trace_action::network_operation_applied,
      },
      event_cleanup_policy::invoke,
      std::move(read.trace));
    if (!event) {
        return runtime::failure(event.error());
    }
    read.event = *event;
    read.scheduled = true;
    return {};
}

void fake_network::impl::complete_read(std::uint64_t operation_id) noexcept {
    auto* operation = find_operation(operation_id);
    if (operation == nullptr) {
        return;
    }
    auto& read = std::get<read_operation>(operation->payload);
    const auto maximum_bytes = read.maximum_bytes;
    auto* pair = find_pair(read.pair);
    if (pair == nullptr) {
        complete_read_terminal(operation_id);
        return;
    }
    auto& endpoint = pair->endpoints[read.side];
    if (
      scheduler_->discarding_failed_event() || endpoint.abort_requested
      || endpoint.peer_reset
      || endpoint.input != runtime::network_half_state::open) {
        auto done = std::move(read.done);
        endpoint.read_operation.reset();
        const auto* failure = scheduler_->discarding_failed_event()
                                ? scheduler_->trace_failure()
                                : nullptr;
        const auto code = endpoint.abort_requested ? errc::aborted
                                                   : errc::network_failure;
        operations.erase(operation_id);
        done.set_value(
          runtime::failure(
            failure != nullptr ? *failure : network_error(code)));
        return;
    }
    if (read.fault.action() == runtime::fault_action::error) {
        auto done = std::move(read.done);
        endpoint.read_operation.reset();
        operations.erase(operation_id);
        done.set_value(runtime::failure(network_error(errc::fault_injected)));
        return;
    }
    if (read.fault.action() == runtime::fault_action::disconnect) {
        auto done = std::move(read.done);
        endpoint.read_operation.reset();
        const auto pair_id = pair->id;
        const auto side = read.side;
        operations.erase(operation_id);
        reset_pair(pair_id, side);
        done.set_value(runtime::failure(network_error(errc::network_failure)));
        return;
    }

    const auto direction_index = other_side(read.side);
    auto& direction = pair->directions[direction_index];
    if (direction.delivered.empty()) {
        auto done = std::move(read.done);
        endpoint.read_operation.reset();
        operations.erase(operation_id);
        if (direction.fin_delivered) {
            auto result = runtime::network_read_result::make(
              bytes::fragmented_buffer{}, true, maximum_bytes);
            done.set_value(std::move(result));
        } else {
            done.set_value(runtime::failure(network_error(errc::unavailable)));
        }
        return;
    }
    const auto token = direction.delivered.front();
    auto* packet = find_packet(token);
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      packet != nullptr && packet->phase == packet_phase::delivered
        && packet->pair == pair->id && packet->side == direction_index,
      "fake read lost delivered packet state");
    try {
        const auto count = std::min(
          maximum_bytes.value(), packet->data.size().value());
        auto selected = packet->data.share(byte_count{}, byte_count{count});
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          selected.has_value(),
          "validated fake read failed to share delivered bytes");
        if (read.fault.action() == runtime::fault_action::corrupt) {
            *selected = corrupt_network_payload(
              *selected,
              read.fault_a % count,
              static_cast<std::uint8_t>(read.fault_b));
        }
        auto trimmed = packet->data.trim_front(byte_count{count});
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          trimmed.has_value(),
          "validated fake read failed to trim delivered bytes");
        direction.logical_bytes = *direction.logical_bytes.checked_sub(
          byte_count{count});
        if (packet->data.empty()) {
            direction.delivered.pop_front();
            destroy_packet(token.slot);
        }
        if (read.fault.action() == runtime::fault_action::drop) {
            read.scheduled = false;
            read.fault = runtime::fault_decision{};
            const bool readable = direction.fin_delivered
                                  || !direction.delivered.empty();
            if (readable) {
                if (
                  auto scheduled = schedule_read(
                    operation_id, scheduler_->now());
                  !scheduled) {
                    complete_read_terminal(operation_id);
                }
            } else {
                auto observed = scheduler_->observe_effect(
                  trace_event_descriptor{
                    .kind = trace_event_kind::network,
                    .domain = static_cast<std::uint32_t>(
                      network_trace_phase::parked),
                    .stable_id = operation_id,
                    .effect = trace_action::operation_parked,
                  },
                  {},
                  read.parked_trace);
                if (!observed) {
                    return;
                }
            }
            collect_pair(pair->id);
            return;
        }
        auto result = runtime::network_read_result::make(
          std::move(*selected), false, maximum_bytes);
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          result.has_value(),
          "validated fake read produced an invalid bounded result");
        if (read.fault.action() == runtime::fault_action::drop_completion) {
            auto observed = scheduler_->observe_effect(
              trace_event_descriptor{
                .kind = trace_event_kind::network,
                .domain = static_cast<std::uint32_t>(
                  network_trace_phase::parked),
                .stable_id = operation_id,
                .effect = trace_action::operation_parked,
              },
              {},
              read.parked_trace);
            if (!observed) {
                return;
            }
            read.parked_result.emplace(std::move(*result));
            read.parked = true;
            read.scheduled = false;
            endpoint.read_operation.reset();
            collect_pair(pair->id);
            return;
        }
        auto done = std::move(read.done);
        endpoint.read_operation.reset();
        operations.erase(operation_id);
        done.set_value(std::move(result));
        collect_pair(pair->id);
    } catch (...) {
        auto done = std::move(read.done);
        endpoint.read_operation.reset();
        operations.erase(operation_id);
        done.set_exception(std::current_exception());
    }
}

void fake_network::impl::complete_read_terminal(
  std::uint64_t operation_id) noexcept {
    auto* operation = find_operation(operation_id);
    if (operation == nullptr) {
        return;
    }
    auto& read = std::get<read_operation>(operation->payload);
    auto* pair = find_pair(read.pair);
    auto done = std::move(read.done);
    errc code{errc::network_failure};
    if (pair != nullptr) {
        auto& endpoint = pair->endpoints[read.side];
        endpoint.read_operation.reset();
        if (endpoint.abort_requested) {
            code = errc::aborted;
        }
    }
    const auto* failure = scheduler_->discarding_failed_event()
                            ? scheduler_->trace_failure()
                            : nullptr;
    operations.erase(operation_id);
    done.set_value(
      runtime::failure(failure != nullptr ? *failure : network_error(code)));
}

seastar::future<runtime::result<void>> fake_network::write(
  std::uint64_t pair_id,
  std::uint8_t side,
  bytes::fragmented_buffer data,
  seastar::abort_source& caller_abort,
  runtime::network_write_admission& admission) {
    assert_current();
    if (impl_->state_ != fake_network_state::open || impl_->abort_requested_) {
        return ready_failure<void>(errc::closed);
    }
    if (
      auto valid = runtime::validate_network_write(data, admission.limits());
      !valid) {
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::failure(valid.error()));
    }
    if (caller_abort.abort_requested()) {
        return ready_failure<void>(errc::aborted);
    }
    auto* pair = impl_->find_pair(pair_id);
    if (pair == nullptr || side >= 2) {
        return ready_failure<void>(errc::closed);
    }
    auto& endpoint = pair->endpoints[side];
    if (endpoint.state != runtime::network_connection_state::open) {
        return ready_failure<void>(errc::closed);
    }
    if (endpoint.abort_requested) {
        return ready_failure<void>(errc::aborted);
    }
    if (endpoint.peer_reset) {
        return ready_failure<void>(errc::network_failure);
    }
    if (endpoint.output != runtime::network_half_state::open) {
        return ready_failure<void>(errc::closed);
    }
    auto prepared_fault = impl_->prepare_fault(
      runtime::builtin_fault_point::network_write,
      network_object_key(pair_id, side));
    if (!prepared_fault) {
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::failure(prepared_fault.error()));
    }
    auto decision = prepared_fault->decision;
    if (decision.action() == runtime::fault_action::short_operation) {
        const auto cap = decision.short_operation_bytes()->value();
        if (cap >= data.size().value()) {
            decision = runtime::fault_decision{};
            prepared_fault->applicable = false;
        } else {
            auto trimmed = data.trim_back(
              byte_count{data.size().value() - cap});
            KWAQUE_INVARIANT(
              fake_network_state_invariant,
              trimmed.has_value(),
              "validated short network write failed to trim payload");
        }
    } else if (decision.action() == runtime::fault_action::corrupt) {
        auto selected_byte = prepared_fault->prepared->draw_bounded(
          data.size().value());
        auto selected_bit = prepared_fault->prepared->draw_bounded(8U);
        if (!selected_byte || !selected_bit) {
            return seastar::make_ready_future<runtime::result<void>>(
              runtime::failure(
                !selected_byte ? selected_byte.error() : selected_bit.error()));
        }
        prepared_fault->coordinate_a = *selected_byte;
        prepared_fault->coordinate_b = *selected_bit;
        data = corrupt_network_payload(
          data, *selected_byte, static_cast<std::uint8_t>(*selected_bit));
    }
    const bool immediate_fault = decision.action()
                                   == runtime::fault_action::error
                                 || decision.action()
                                      == runtime::fault_action::disconnect;
    if (immediate_fault) {
        if (
          impl_->operations.size() == config_.maximum_operations
          || impl_->operation_ids_exhausted) {
            return ready_failure<void>(errc::queue_full);
        }
        auto admitted = admission.try_acquire(data.retained_bytes());
        if (!admitted) {
            return ready_failure<void>(errc::queue_full);
        }
        const auto operation_id = impl_->next_operation_id;
        auto terminal = impl_->reserve_terminal(
          trace_event_kind::network,
          static_cast<std::uint32_t>(network_trace_phase::write),
          operation_id,
          trace_action::network_operation_applied,
          pair_id,
          side,
          data.size().value(),
          static_cast<std::uint32_t>(decision.action()));
        if (!terminal) {
            return seastar::make_ready_future<runtime::result<void>>(
              runtime::failure(terminal.error()));
        }
        auto operation_stop_terminal = impl_->reserve_terminal(
          trace_event_kind::network,
          static_cast<std::uint32_t>(network_trace_phase::stop),
          operation_id,
          trace_action::stop_terminal,
          0,
          0,
          0,
          static_cast<std::uint32_t>(errc::aborted));
        if (!operation_stop_terminal) {
            return seastar::make_ready_future<runtime::result<void>>(
              runtime::failure(operation_stop_terminal.error()));
        }
        try {
            impl::write_operation writing{
              .pair = pair_id,
              .admission = std::move(*admitted),
              .terminal_event = std::move(terminal->first),
              .terminal_trace = std::move(terminal->second),
              .side = side,
            };
            auto waiting = writing.done.get_future();
            auto operation = std::make_unique<impl::operation_state>(
              operation_id,
              impl::operation_payload{
                std::in_place_type<impl::write_operation>, std::move(writing)});
            operation->stop_event = std::move(operation_stop_terminal->first);
            operation->stop_trace = std::move(operation_stop_terminal->second);
            impl_->operations.emplace(operation_id, std::move(operation));
            auto committed = impl_->commit_fault(*prepared_fault);
            if (!committed) {
                impl_->operations.erase(operation_id);
                return seastar::make_ready_future<runtime::result<void>>(
                  runtime::failure(committed.error()));
            }
            auto& stored = std::get<impl::write_operation>(
              impl_->find_operation(operation_id)->payload);
            stored.terminal_event.release();
            const auto code = decision.action() == runtime::fault_action::error
                                ? errc::fault_injected
                                : errc::network_failure;
            auto scheduled = scheduler_->schedule(
              scheduler_->now(),
              event_priority::normal(),
              [this,
               operation_id,
               code,
               disconnect = decision.action()
                            == runtime::fault_action::disconnect] noexcept {
                  impl_->complete_immediate_write(
                    operation_id, code, disconnect);
              },
              trace_event_descriptor{
                .kind = trace_event_kind::network,
                .domain = static_cast<std::uint32_t>(
                  network_trace_phase::write),
                .stable_id = operation_id,
                .coordinate_a = pair_id,
                .coordinate_b = side,
                .value = data.size().value(),
                .result = static_cast<std::uint32_t>(decision.action()),
                .effect = trace_action::network_operation_applied,
              },
              event_cleanup_policy::invoke,
              std::move(stored.terminal_trace));
            KWAQUE_INVARIANT(
              fake_network_state_invariant,
              scheduled.has_value(),
              "prepared immediate write fault could not schedule");
            impl_->issue_operation_id();
            impl_->activated_ = true;
            return waiting;
        } catch (...) {
            impl_->operations.erase(operation_id);
            return seastar::current_exception_as_future<
              runtime::result<void>>();
        }
    }
    const std::uint32_t packet_copies = decision.action()
                                            == runtime::fault_action::duplicate
                                          ? 2U
                                          : 1U;
    const auto logical_required = data.size().value() * packet_copies;
    const auto retained_required = data.retained_bytes().value()
                                   * packet_copies;
    auto& direction = pair->directions[side];
    if (
      logical_required > config_.maximum_direction_bytes.value()
                           - direction.logical_bytes.value()) {
        return ready_failure<void>(errc::queue_full);
    }
    if (
      packet_copies > config_.maximum_direction_packets - direction.packet_count
      || packet_copies
           > config_.maximum_direction_packets - direction.sequence_entries
      || direction.sequence_exhausted
      || packet_copies > impl_->free_packets.size()
      || packet_copies > config_.maximum_packets - impl_->live_packets
      || impl_->packet_ids_exhausted) {
        return ready_failure<void>(errc::queue_full);
    }
    if (
      logical_required > config_.maximum_packet_logical_bytes.value()
                           - impl_->packet_logical_bytes.value()
      || retained_required > config_.maximum_packet_retained_bytes.value()
                               - impl_->packet_retained_bytes.value()) {
        return ready_failure<void>(errc::queue_full);
    }
    if (
      packet_copies == 2U
      && (impl_->next_packet_id == std::numeric_limits<std::uint64_t>::max()
          || direction.next_sequence
               == std::numeric_limits<std::uint64_t>::max())) {
        return ready_failure<void>(errc::out_of_range);
    }
    if (
      impl_->operations.size() == config_.maximum_operations
      || impl_->operation_ids_exhausted) {
        return ready_failure<void>(errc::queue_full);
    }
    const impl::directed_link_key link_key{
      .source = endpoint.local.address(), .target = endpoint.remote.address()};
    auto* link = impl_->find_link(link_key);
    if (
      link == nullptr
      && (impl_->links.size() == config_.maximum_links || impl_->link_ids_exhausted)) {
        return ready_failure<void>(errc::queue_full);
    }
    const bool needs_transmitter_slot = !direction.transmitter_slot;
    if (needs_transmitter_slot && impl_->free_flows.empty()) {
        return ready_failure<void>(errc::queue_full);
    }
    auto admitted = admission.try_acquire(data.retained_bytes());
    if (!admitted) {
        return ready_failure<void>(errc::queue_full);
    }

    const auto maximum_latency = std::max(
      config_.latency_min.nanoseconds(),
      config_.latency_mean_parameter.nanoseconds() * 2U);
    const auto injected_delay = decision.action()
                                    == runtime::fault_action::delay
                                  ? decision.delay()->nanoseconds()
                                : decision.action()
                                    == runtime::fault_action::reorder
                                  ? config_.reorder_window.nanoseconds()
                                  : 0U;
    if (
      injected_delay > scheduler_->limits().maximum_deadline().nanoseconds()
                         - maximum_latency) {
        return ready_failure<void>(errc::out_of_range);
    }
    const auto maximum_delivery_latency = maximum_latency + injected_delay;
    const auto delivery_limit = add_deadline(
      scheduler_->now(),
      runtime::monotonic_duration{maximum_delivery_latency},
      scheduler_->limits().maximum_deadline());
    const auto gap_limit = add_deadline(
      scheduler_->now(),
      config_.interframe_gap,
      scheduler_->limits().maximum_deadline());
    if (!delivery_limit || !gap_limit) {
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::failure(
            !delivery_limit ? delivery_limit.error() : gap_limit.error()));
    }
    const std::array capacities{
      impl_->egress_capacity(endpoint.local.address()),
      link == nullptr ? config_.link_capacity : link->capacity,
      decision.action() == runtime::fault_action::drop
        ? bandwidth_capacity::unlimited()
        : impl_->ingress_capacity(endpoint.remote.address())};
    const bool parked = std::ranges::any_of(
      capacities, [](bandwidth_capacity capacity) {
          return !capacity.is_unlimited() && capacity.bytes_per_second() == 0;
      });
    std::optional<std::uint64_t> minimum_capacity;
    for (const auto capacity : capacities) {
        if (
          !capacity.is_unlimited() && capacity.bytes_per_second() != 0
          && (!minimum_capacity || capacity.bytes_per_second() < *minimum_capacity)) {
            minimum_capacity = capacity.bytes_per_second();
        }
    }
    if (!parked && minimum_capacity) {
        auto minimum_rate = bandwidth_fraction::ratio(
          *minimum_capacity, config_.maximum_active_flows);
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          minimum_rate.has_value(),
          "validated bandwidth flow count produced an invalid write rate");
        auto duration = bandwidth_duration(
          bandwidth_rate::finite(std::move(*minimum_rate)),
          bandwidth_fraction::whole(data.size().value()));
        if (!duration || !*duration) {
            return seastar::make_ready_future<runtime::result<void>>(
              runtime::failure(
                duration ? network_error(errc::out_of_range)
                         : duration.error()));
        }
        auto transmit_limit = add_deadline(
          scheduler_->now(),
          **duration,
          scheduler_->limits().maximum_deadline());
        if (!transmit_limit) {
            return seastar::make_ready_future<runtime::result<void>>(
              runtime::failure(transmit_limit.error()));
        }
        const auto tail = std::max(
          maximum_delivery_latency, config_.interframe_gap.nanoseconds());
        if (
          tail > scheduler_->limits().maximum_deadline().nanoseconds()
                   - transmit_limit->nanoseconds()) {
            return ready_failure<void>(errc::out_of_range);
        }
    }
    const auto operation_id = impl_->next_operation_id;
    const auto packet_id = impl_->next_packet_id;
    auto delivery = impl_->reserve_terminal(
      trace_event_kind::network,
      static_cast<std::uint32_t>(network_trace_phase::delivery),
      packet_id);
    auto gap = impl_->reserve_terminal(
      trace_event_kind::bandwidth,
      static_cast<std::uint32_t>(bandwidth_trace_phase::transfer_done),
      packet_id);
    auto ready_delivery = impl_->reserve_terminal(
      trace_event_kind::network,
      static_cast<std::uint32_t>(network_trace_phase::delivery),
      packet_id);
    auto packet_traces = impl_->reserve_packet_traces(
      packet_id, pair_id, side, data.size());
    std::optional<
      std::pair<scheduler::event_slot_reservation, event_trace::reservation>>
      clone_delivery;
    std::optional<
      std::pair<scheduler::event_slot_reservation, event_trace::reservation>>
      clone_gap;
    std::optional<
      std::pair<scheduler::event_slot_reservation, event_trace::reservation>>
      clone_ready_delivery;
    std::optional<impl::packet_trace_reservations> clone_packet_traces;
    if (packet_copies == 2U) {
        auto reserved_delivery = impl_->reserve_terminal(
          trace_event_kind::network,
          static_cast<std::uint32_t>(network_trace_phase::delivery),
          packet_id + 1U);
        auto reserved_gap = impl_->reserve_terminal(
          trace_event_kind::bandwidth,
          static_cast<std::uint32_t>(bandwidth_trace_phase::transfer_done),
          packet_id + 1U);
        auto reserved_ready = impl_->reserve_terminal(
          trace_event_kind::network,
          static_cast<std::uint32_t>(network_trace_phase::delivery),
          packet_id + 1U);
        if (!reserved_delivery || !reserved_gap || !reserved_ready) {
            return seastar::make_ready_future<runtime::result<void>>(
              runtime::failure(
                !reserved_delivery ? reserved_delivery.error()
                : !reserved_gap    ? reserved_gap.error()
                                   : reserved_ready.error()));
        }
        clone_delivery.emplace(std::move(*reserved_delivery));
        clone_gap.emplace(std::move(*reserved_gap));
        clone_ready_delivery.emplace(std::move(*reserved_ready));
        auto reserved_traces = impl_->reserve_packet_traces(
          packet_id + 1U, pair_id, side, data.size());
        if (!reserved_traces) {
            return seastar::make_ready_future<runtime::result<void>>(
              runtime::failure(reserved_traces.error()));
        }
        clone_packet_traces.emplace(std::move(*reserved_traces));
    }
    auto terminal = impl_->reserve_terminal(
      trace_event_kind::network,
      static_cast<std::uint32_t>(network_trace_phase::write),
      operation_id,
      trace_action::network_operation_applied,
      pair_id,
      side,
      data.size().value(),
      static_cast<std::uint32_t>(decision.action()));
    auto write_stop_terminal = impl_->reserve_terminal(
      trace_event_kind::network,
      static_cast<std::uint32_t>(network_trace_phase::stop),
      operation_id,
      trace_action::stop_terminal,
      0,
      0,
      0,
      static_cast<std::uint32_t>(errc::aborted));
    auto write_effect = scheduler_->reserve_effect(
      trace_event_descriptor{
        .kind = trace_event_kind::network,
        .domain = static_cast<std::uint32_t>(network_trace_phase::write),
        .stable_id = operation_id,
        .coordinate_a = pair_id,
        .coordinate_b = side,
        .value = data.size().value(),
        .result = static_cast<std::uint32_t>(decision.action()),
        .effect = trace_action::network_operation_applied,
      });
    runtime::result<event_trace::reservation> parked_trace{
      event_trace::reservation{}};
    if (decision.action() == runtime::fault_action::drop_completion) {
        parked_trace = scheduler_->reserve_effect(
          trace_event_descriptor{
            .kind = trace_event_kind::network,
            .domain = static_cast<std::uint32_t>(network_trace_phase::parked),
            .stable_id = operation_id,
            .effect = trace_action::operation_parked,
          });
    }
    if (
      !delivery || !gap || !ready_delivery || !packet_traces || !terminal
      || !write_stop_terminal || !write_effect || !parked_trace) {
        const auto error = !delivery              ? delivery.error()
                           : !gap                 ? gap.error()
                           : !ready_delivery      ? ready_delivery.error()
                           : !packet_traces       ? packet_traces.error()
                           : !terminal            ? terminal.error()
                           : !write_stop_terminal ? write_stop_terminal.error()
                           : !write_effect        ? write_effect.error()
                                                  : parked_trace.error();
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::failure(error));
    }
    auto parked_credit = impl_->reserve_parked(
      decision.action() == runtime::fault_action::drop_completion);
    if (!parked_credit) {
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::failure(parked_credit.error()));
    }
    bool inserted_link = false;
    try {
        std::unique_ptr<impl::link_state> prepared_link;
        if (link == nullptr) {
            prepared_link = std::make_unique<impl::link_state>(
              impl_->next_link_id,
              link_key,
              config_.link_capacity,
              config_.latency_min,
              config_.latency_mean_parameter);
        }
        auto free = impl_->free_packets.begin();
        const auto packet_slot = *free;
        std::optional<std::uint32_t> clone_slot;
        std::optional<bytes::fragmented_buffer> clone_data;
        if (packet_copies == 2U) {
            ++free;
            clone_slot = *free;
            clone_data.emplace(data.share());
        }
        const impl::packet_token token{.id = packet_id, .slot = packet_slot};
        const impl::packet_token clone_token{
          .id = packet_copies == 2U ? packet_id + 1U : 0U,
          .slot = clone_slot.value_or(0U),
        };
        impl::write_operation writing{
          .pair = pair_id,
          .packet = token,
          .admission = std::move(*admitted),
          .terminal_event = std::move(terminal->first),
          .terminal_trace = std::move(terminal->second),
          .effect_trace = std::move(*write_effect),
          .parked_trace = std::move(*parked_trace),
          .side = side,
          .drop_completion = decision.action()
                             == runtime::fault_action::drop_completion,
        };
        auto waiting = writing.done.get_future();
        auto operation = std::make_unique<impl::operation_state>(
          operation_id,
          impl::operation_payload{
            std::in_place_type<impl::write_operation>, std::move(writing)},
          std::move(*parked_credit));
        operation->stop_event = std::move(write_stop_terminal->first);
        operation->stop_trace = std::move(write_stop_terminal->second);
        impl_->operations.emplace(operation_id, std::move(operation));
        if (prepared_link) {
            const auto inserted = impl_->links.emplace(
              link_key, std::move(prepared_link));
            KWAQUE_INVARIANT(
              fake_network_state_invariant,
              inserted.second,
              "fake network duplicated a prepared directed link");
            link = inserted.first->second.get();
            inserted_link = true;
        }
        auto* inserted = impl_->find_operation(operation_id);
        auto& write_state = std::get<impl::write_operation>(inserted->payload);
        write_state.abort_subscription = caller_abort.subscribe(
          [this, operation_id] noexcept { impl_->abort_write(operation_id); });
        if (!write_state.abort_subscription) {
            impl_->operations.erase(operation_id);
            if (inserted_link) {
                impl_->links.erase(link_key);
            }
            return ready_failure<void>(errc::aborted);
        }

        auto committed = impl_->commit_fault(*prepared_fault);
        if (!committed) {
            impl_->operations.erase(operation_id);
            if (inserted_link) {
                impl_->links.erase(link_key);
            }
            return seastar::make_ready_future<runtime::result<void>>(
              runtime::failure(committed.error()));
        }

        impl_->free_packets.pop_front();
        if (clone_slot) {
            impl_->free_packets.pop_front();
        }
        if (needs_transmitter_slot) {
            direction.transmitter_slot = impl_->free_flows.front();
            impl_->free_flows.pop_front();
        }
        auto& packet = impl_->packets[packet_slot];
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          packet.phase == impl::packet_phase::free,
          "fake packet free list returned an occupied slot");
        const auto logical_charge = data.size();
        const auto retained_charge = data.retained_bytes();
        packet.id = packet_id;
        packet.pair = pair_id;
        packet.write_operation = operation_id;
        if (clone_slot) {
            packet.clone = clone_token;
        }
        packet.link = link_key;
        packet.data = std::move(data);
        packet.logical_charge = logical_charge;
        packet.retained_charge = retained_charge;
        packet.delivery_event_reservation = std::move(delivery->first);
        packet.delivery_trace = std::move(delivery->second);
        packet.gap_event_reservation = std::move(gap->first);
        packet.gap_trace = std::move(gap->second);
        packet.ready_event_reservation = std::move(ready_delivery->first);
        packet.ready_trace = std::move(ready_delivery->second);
        packet.flow_start_trace = std::move(packet_traces->flow_start);
        packet.transfer_trace = std::move(packet_traces->transfer);
        packet.packet_effect_trace = std::move(packet_traces->packet_effect);
        packet.start_rebalance_trace = std::move(
          packet_traces->start_rebalance);
        packet.finish_rebalance_trace = std::move(
          packet_traces->finish_rebalance);
        packet.start_wake_trace = std::move(packet_traces->start_wake);
        packet.finish_wake_trace = std::move(packet_traces->finish_wake);
        packet.delivery_delay = runtime::monotonic_duration{injected_delay};
        packet.sequence = direction.next_sequence;
        packet.side = side;
        packet.phase = impl::packet_phase::queued;
        packet.delivery_scheduled = false;
        packet.drop_delivery = decision.action() == runtime::fault_action::drop;
        packet.reorder_delivery = decision.action()
                                  == runtime::fault_action::reorder;
        const auto sequence_index = static_cast<std::size_t>(
          packet.sequence % direction.sequence_slots.size());
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          !direction.sequence_slots[sequence_index].valid(),
          "fake packet sequence ring reused a live slot");
        direction.sequence_slots[sequence_index] = token;
        direction.sequence_statuses[sequence_index]
          = impl::sequence_status::live;
        direction.sequence_traces[sequence_index] = std::move(
          packet_traces->sequence);
        ++direction.sequence_entries;
        if (
          direction.next_sequence
          == std::numeric_limits<std::uint64_t>::max()) {
            direction.sequence_exhausted = true;
        } else {
            ++direction.next_sequence;
        }
        if (clone_slot) {
            auto& clone = impl_->packets[*clone_slot];
            KWAQUE_INVARIANT(
              fake_network_state_invariant,
              clone.phase == impl::packet_phase::free,
              "fake packet free list returned an occupied clone slot");
            clone.id = packet_id + 1U;
            clone.pair = pair_id;
            clone.link = link_key;
            clone.data = std::move(*clone_data);
            clone.logical_charge = logical_charge;
            clone.retained_charge = retained_charge;
            clone.delivery_event_reservation = std::move(clone_delivery->first);
            clone.delivery_trace = std::move(clone_delivery->second);
            clone.gap_event_reservation = std::move(clone_gap->first);
            clone.gap_trace = std::move(clone_gap->second);
            clone.ready_event_reservation = std::move(
              clone_ready_delivery->first);
            clone.ready_trace = std::move(clone_ready_delivery->second);
            clone.flow_start_trace = std::move(clone_packet_traces->flow_start);
            clone.transfer_trace = std::move(clone_packet_traces->transfer);
            clone.packet_effect_trace = std::move(
              clone_packet_traces->packet_effect);
            clone.start_rebalance_trace = std::move(
              clone_packet_traces->start_rebalance);
            clone.finish_rebalance_trace = std::move(
              clone_packet_traces->finish_rebalance);
            clone.start_wake_trace = std::move(clone_packet_traces->start_wake);
            clone.finish_wake_trace = std::move(
              clone_packet_traces->finish_wake);
            clone.sequence = direction.next_sequence;
            clone.side = side;
            clone.phase = impl::packet_phase::deferred_clone;
            const auto clone_sequence_index = static_cast<std::size_t>(
              clone.sequence % direction.sequence_slots.size());
            KWAQUE_INVARIANT(
              fake_network_state_invariant,
              !direction.sequence_slots[clone_sequence_index].valid(),
              "fake clone sequence ring reused a live slot");
            direction.sequence_slots[clone_sequence_index] = clone_token;
            direction.sequence_statuses[clone_sequence_index]
              = impl::sequence_status::live;
            direction.sequence_traces[clone_sequence_index] = std::move(
              clone_packet_traces->sequence);
            ++direction.sequence_entries;
            if (
              direction.next_sequence
              == std::numeric_limits<std::uint64_t>::max()) {
                direction.sequence_exhausted = true;
            } else {
                ++direction.next_sequence;
            }
        }
        direction.transmit_queue.push_back(token);
        direction.packet_count += packet_copies;
        direction.logical_bytes = *direction.logical_bytes.checked_add(
          byte_count{logical_required});
        link->packets += packet_copies;
        impl_->live_packets += packet_copies;
        impl_->packet_logical_bytes = *impl_->packet_logical_bytes.checked_add(
          byte_count{logical_required});
        impl_->packet_retained_bytes
          = *impl_->packet_retained_bytes.checked_add(
            byte_count{retained_required});
        impl_->issue_operation_id();
        impl_->issue_packet_id();
        if (clone_slot) {
            impl_->issue_packet_id();
        }
        if (inserted_link) {
            impl_->issue_link_id();
        }
        impl_->activated_ = true;
        if (direction.transmitter == impl::transmitter_state::ready) {
            impl_->start_next_packet(pair_id, side);
        }
        return waiting;
    } catch (...) {
        impl_->operations.erase(operation_id);
        if (inserted_link && link != nullptr && link->packets == 0) {
            impl_->links.erase(link_key);
        }
        return seastar::current_exception_as_future<runtime::result<void>>();
    }
}

bandwidth_resource_key fake_network::impl::resource_key(
  std::uint8_t domain,
  const runtime::network_address& first,
  const runtime::network_address* second) const noexcept {
    std::array<std::byte, bandwidth_resource_key::encoded_bytes> encoded{};
    auto append = [&](
                    std::size_t offset, const runtime::network_address& value) {
        encoded[offset] = static_cast<std::byte>(
          static_cast<std::uint8_t>(value.family()));
        std::copy(
          value.bytes().begin(),
          value.bytes().end(),
          encoded.begin() + offset + 1U);
        const auto scope = value.scope();
        for (std::size_t index = 0; index < sizeof(scope); ++index) {
            encoded[offset + 17U + index] = static_cast<std::byte>(
              (scope >> ((sizeof(scope) - index - 1U) * 8U)) & 0xffU);
        }
    };
    encoded[0] = static_cast<std::byte>(domain);
    append(1U, first);
    if (second != nullptr) {
        append(22U, *second);
    }
    return bandwidth_resource_key{encoded};
}

bandwidth_capacity fake_network::impl::egress_capacity(
  const runtime::network_address& address) const noexcept {
    const auto found = egress_limits.find(address);
    return found == egress_limits.end() ? config_.egress_capacity
                                        : found->second;
}

bandwidth_capacity fake_network::impl::ingress_capacity(
  const runtime::network_address& address) const noexcept {
    const auto found = ingress_limits.find(address);
    return found == ingress_limits.end() ? config_.ingress_capacity
                                         : found->second;
}

runtime::result<fake_network::impl::prepared_network_fault>
fake_network::impl::prepare_fault(
  runtime::builtin_fault_point point, runtime::fault_object_key object) {
    prepared_network_fault result;
    result.owner = this;
    result.key = network_fault_key{.point = point, .object = object};
    if (faults_ == nullptr) {
        result.committed = true;
        return result;
    }
    const auto [position, inserted] = fault_occurrences_.try_emplace(
      result.key, 0U);
    result.inserted_occurrence = inserted;
    const auto current = position->second;
    if (current == std::numeric_limits<std::uint64_t>::max()) {
        return runtime::failure(network_error(errc::out_of_range));
    }
    auto occurrence = runtime::fault_occurrence::make(current + 1U);
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      occurrence.has_value(),
      "fake network produced an invalid fault occurrence");
    auto prepared = faults_->prepare(
      runtime::fault_request{
        .point = runtime::descriptor_for(point)->id,
        .occurrence = *occurrence,
        .object = object,
      });
    if (!prepared) {
        if (inserted) {
            fault_occurrences_.erase(position);
            result.inserted_occurrence = false;
        }
        return runtime::failure(prepared.error());
    }
    result.decision = prepared->preview();
    result.applicable = result.decision.action() != runtime::fault_action::none;
    result.prepared.emplace(std::move(*prepared));
    return result;
}

runtime::result<void>
fake_network::impl::commit_fault(prepared_network_fault& prepared) noexcept {
    if (!prepared.prepared) {
        prepared.committed = true;
        return {};
    }
    auto committed = prepared.prepared->commit();
    if (!committed) {
        return runtime::failure(committed.error());
    }
    auto found = fault_occurrences_.find(prepared.key);
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      found != fault_occurrences_.end()
        && found->second != std::numeric_limits<std::uint64_t>::max(),
      "fake network fault occurrence disappeared before commit");
    ++found->second;
    prepared.committed = true;
    return {};
}

runtime::monotonic_duration fake_network::impl::packet_latency(
  const link_state& link, const packet_state& packet) noexcept {
    if (link.latency_min == link.latency_mean_parameter) {
        return link.latency_min;
    }
    auto source = random_.stream(
      random_domain::network_decision, link.id, packet.id);
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      source.has_value(),
      "fake network rejected a registered latency coordinate");
    const auto upper = link.latency_mean_parameter.nanoseconds() * 2U + 1U;
    auto sampled = runtime::uniform_u64(*source, upper);
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      sampled.has_value(),
      "fake network rejected a validated latency range");
    return runtime::monotonic_duration{
      std::max(link.latency_min.nanoseconds(), *sampled)};
}

bool fake_network::impl::observe_packet_effect(
  packet_state& packet, trace_action action, errc result) noexcept {
    if (packet.packet_effect_observed) {
        return true;
    }
    auto observed = scheduler_->observe_effect(
      trace_event_descriptor{
        .kind = trace_event_kind::network,
        .domain = static_cast<std::uint32_t>(network_trace_phase::delivery),
        .stable_id = packet.id,
        .coordinate_a = packet.sequence,
        .coordinate_b = packet.side,
        .value = packet.data.size().value(),
        .result = static_cast<std::uint32_t>(result),
        .effect = action,
      },
      {},
      packet.packet_effect_trace);
    if (!observed) {
        return false;
    }
    packet.packet_effect_observed = true;
    return true;
}

void fake_network::impl::start_next_packet(
  std::uint64_t pair_id, std::uint8_t direction_index) noexcept {
    auto* pair = find_pair(pair_id);
    if (pair == nullptr) {
        return;
    }
    if (pair->reset_applied) {
        return;
    }
    const auto& endpoint = pair->endpoints[direction_index];
    if (
      endpoint.state != runtime::network_connection_state::open
      || endpoint.output != runtime::network_half_state::open
      || endpoint.abort_requested || endpoint.peer_reset) {
        return;
    }
    auto& direction = pair->directions[direction_index];
    if (
      direction.transmitter != transmitter_state::ready
      || !direction.transmitter_slot) {
        return;
    }
    while (direction.deferred_clone || !direction.transmit_queue.empty()) {
        packet_token token;
        bool from_deferred = false;
        if (direction.deferred_clone) {
            token = *direction.deferred_clone;
            auto* deferred = find_packet(token);
            if (
              deferred != nullptr
              && deferred->phase == packet_phase::deferred_clone) {
                return;
            }
            from_deferred = true;
        } else {
            token = direction.transmit_queue.front();
        }
        auto* packet = find_packet(token);
        if (packet == nullptr || packet->phase == packet_phase::retired) {
            if (from_deferred) {
                direction.deferred_clone.reset();
            } else {
                direction.transmit_queue.pop_front();
            }
            continue;
        }
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          packet->phase == packet_phase::queued,
          "fake transmitter dequeued a non-queued packet");
        const auto flow_slot = *direction.transmitter_slot;
        auto& flow = flows[flow_slot];
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          !flow.active,
          "fake transmitter slot still owns an active flow");
        auto observed = scheduler_->observe_effect(
          trace_event_descriptor{
            .kind = trace_event_kind::bandwidth,
            .domain = static_cast<std::uint32_t>(
              bandwidth_trace_phase::flow_start),
            .stable_id = packet->id,
            .coordinate_a = packet->pair,
            .coordinate_b = packet->side,
            .value = packet->data.size().value(),
            .effect = trace_action::flow_started,
          },
          {},
          packet->flow_start_trace);
        if (!observed) {
            return;
        }
        auto rebalance_trace = std::move(packet->start_rebalance_trace);
        auto wake_trace = std::move(packet->start_wake_trace);
        const auto packet_id = packet->id;
        const staged_flow_start staged{.packet = token, .flow_slot = flow_slot};
        if (!rebalance_bandwidth(
              std::move(rebalance_trace),
              std::move(wake_trace),
              packet_id,
              nullptr,
              &staged)) {
            return;
        }
        if (from_deferred) {
            direction.deferred_clone.reset();
        } else {
            KWAQUE_INVARIANT(
              fake_network_state_invariant,
              !direction.transmit_queue.empty()
                && direction.transmit_queue.front().id == token.id,
              "fake transmitter queue changed during staged flow start");
            direction.transmit_queue.pop_front();
        }
        if (packet->clone) {
            direction.deferred_clone = *packet->clone;
        }
        return;
    }
    const auto flow_slot = *direction.transmitter_slot;
    flows[flow_slot] = flow_state{};
    free_flows.push_back(flow_slot);
    direction.transmitter_slot.reset();
}

bool fake_network::impl::rebalance_bandwidth(
  event_trace::reservation trace,
  event_trace::reservation wake_trace,
  std::uint64_t stable_id,
  const control_operation* staged_control,
  const staged_flow_start* staged_start) noexcept {
    const auto now = scheduler_->now();
    std::array<std::uint16_t, maximum_bandwidth_flows> active_slots{};
    std::array<std::uint32_t, maximum_bandwidth_flows> active_packet_slots{};
    std::array<std::uint16_t, maximum_bandwidth_flows> completed_slots{};
    std::array<bandwidth_fraction, maximum_bandwidth_flows> staged_remaining{};
    std::size_t active_count = 0;
    std::size_t completed_count = 0;
    for (std::uint16_t slot = 0; slot < flows.size(); ++slot) {
        auto& flow = flows[slot];
        if (!flow.active) {
            continue;
        }
        active_packet_slots[slot] = flow.packet_slot;
        const auto elapsed = now.checked_elapsed_since(flow.last_update);
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          elapsed.has_value(),
          "fake bandwidth time moved backwards");
        staged_remaining[slot] = bandwidth_transfer(
          flow.rate, *elapsed, flow.remaining);
        if (staged_remaining[slot].zero()) {
            completed_slots[completed_count++] = slot;
        } else {
            active_slots[active_count++] = slot;
        }
    }
    if (staged_start != nullptr) {
        auto& flow = flows[staged_start->flow_slot];
        auto* packet = find_packet(staged_start->packet);
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          !flow.active && packet != nullptr
            && packet->phase == packet_phase::queued,
          "fake bandwidth staging lost its queued flow");
        active_packet_slots[staged_start->flow_slot]
          = staged_start->packet.slot;
        staged_remaining[staged_start->flow_slot] = bandwidth_fraction::whole(
          packet->data.size().value());
        active_slots[active_count++] = staged_start->flow_slot;
    }
    std::sort(
      completed_slots.begin(),
      completed_slots.begin() + static_cast<std::ptrdiff_t>(completed_count),
      [&](std::uint16_t left, std::uint16_t right) {
          return packets[flows[left].packet_slot].id
                 < packets[flows[right].packet_slot].id;
      });
    for (std::size_t index = 0; index < completed_count; ++index) {
        auto& completed_flow = flows[completed_slots[index]];
        auto& completed_packet = packets[completed_flow.packet_slot];
        auto observed = scheduler_->observe_effect(
          trace_event_descriptor{
            .kind = trace_event_kind::bandwidth,
            .domain = static_cast<std::uint32_t>(
              bandwidth_trace_phase::transfer_done),
            .stable_id = completed_packet.id,
            .coordinate_a = completed_packet.pair,
            .coordinate_b = completed_packet.side,
            .value = completed_packet.data.size().value(),
            .effect = trace_action::transfer_completed,
          },
          {},
          completed_packet.transfer_trace);
        if (!observed) {
            return false;
        }
    }

    std::sort(
      active_slots.begin(),
      active_slots.begin() + static_cast<std::ptrdiff_t>(active_count),
      [&](std::uint16_t left, std::uint16_t right) {
          const auto& left_packet = packets[active_packet_slots[left]];
          const auto& right_packet = packets[active_packet_slots[right]];
          return left_packet.id < right_packet.id;
      });

    staged_bandwidth_->reset();
    for (std::size_t index = 0; index < active_count; ++index) {
        const auto flow_slot = active_slots[index];
        const auto& packet = packets[active_packet_slots[flow_slot]];
        auto* pair = find_pair(packet.pair);
        auto* link = find_link(packet.link);
        const bool prospective = staged_start != nullptr
                                 && staged_start->flow_slot == flow_slot
                                 && staged_start->packet.id == packet.id;
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          pair != nullptr && link != nullptr
            && (packet.phase == packet_phase::active || (prospective && packet.phase == packet_phase::queued)),
          "fake bandwidth planner lost an active packet path");
        const auto& endpoint = pair->endpoints[packet.side];
        bandwidth_flow input{.id = packet.id};
        const auto staged_egress = staged_control != nullptr
                                   && staged_control->kind
                                        == control_kind::egress
                                   && staged_control->source
                                        == endpoint.local.address();
        input.constraints[input.constraint_count++] = bandwidth_constraint{
          .resource = resource_key(1U, endpoint.local.address()),
          .capacity = staged_egress ? staged_control->capacity
                                    : egress_capacity(endpoint.local.address()),
        };
        const auto staged_link = staged_control != nullptr
                                 && staged_control->kind == control_kind::link
                                 && staged_control->source == packet.link.source
                                 && staged_control->target
                                      == packet.link.target;
        input.constraints[input.constraint_count++] = bandwidth_constraint{
          .resource = resource_key(2U, packet.link.source, &packet.link.target),
          .capacity = staged_link ? staged_control->capacity : link->capacity,
        };
        if (!packet.drop_delivery) {
            const auto staged_ingress = staged_control != nullptr
                                        && staged_control->kind
                                             == control_kind::ingress
                                        && staged_control->source
                                             == endpoint.remote.address();
            input.constraints[input.constraint_count++] = bandwidth_constraint{
              .resource = resource_key(3U, endpoint.remote.address()),
              .capacity = staged_ingress
                            ? staged_control->capacity
                            : ingress_capacity(endpoint.remote.address()),
            };
        }
        auto added = staged_bandwidth_->add_flow(std::move(input));
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          added.has_value(),
          "fake bandwidth planner rejected a validated active flow");
    }
    auto solved = staged_bandwidth_->solve();
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      solved.has_value(),
      "fake bandwidth fixed arithmetic exceeded its validated workspace");
    const auto allocation_digest = staged_bandwidth_->allocation_digest();

    std::optional<runtime::monotonic_duration> earliest;
    std::uint64_t earliest_flow = 0;
    for (std::size_t index = 0; index < staged_bandwidth_->allocation_count();
         ++index) {
        const auto& allocation = staged_bandwidth_->allocation_at(index);
        flow_state* selected = nullptr;
        std::uint16_t selected_slot = 0;
        for (std::size_t active_index = 0; active_index < active_count;
             ++active_index) {
            const auto candidate_slot = active_slots[active_index];
            if (
              packets[active_packet_slots[candidate_slot]].id
              == allocation.flow) {
                selected = &flows[candidate_slot];
                selected_slot = candidate_slot;
                break;
            }
        }
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          selected != nullptr,
          "fake bandwidth allocation lost its active flow");
        auto duration = bandwidth_duration(
          allocation.rate, staged_remaining[selected_slot]);
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          duration.has_value(),
          "fake bandwidth completion exceeded the scheduler time range");
        if (
          *duration
          && (!earliest || **duration < *earliest
              || (**duration == *earliest
                  && (earliest_flow == 0 || allocation.flow < earliest_flow)))) {
            earliest = **duration;
            earliest_flow = allocation.flow;
        }
    }
    std::optional<runtime::monotonic_time> deadline;
    if (earliest) {
        auto selected_deadline = add_deadline(
          now, *earliest, scheduler_->limits().maximum_deadline());
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          selected_deadline.has_value(),
          "fake bandwidth completion deadline exceeded validated time");
        deadline = *selected_deadline;
    }
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      stable_id != 0,
      "fake bandwidth rebalance lost its stable transaction ID");
    const auto context = allocation_context(allocation_digest);
    auto observed = scheduler_->observe_effect(
      trace_event_descriptor{
        .kind = trace_event_kind::bandwidth,
        .domain = static_cast<std::uint32_t>(bandwidth_trace_phase::rebalance),
        .stable_id = stable_id,
        .coordinate_a = active_count,
        .coordinate_b = staged_bandwidth_->resource_count(),
        .value = deadline ? deadline->nanoseconds()
                          : std::numeric_limits<std::uint64_t>::max(),
        .result = static_cast<std::uint32_t>(
          staged_bandwidth_->membership_count()),
        .effect = trace_action::bandwidth_rebalanced,
      },
      context,
      trace);
    if (!observed) {
        return false;
    }
    for (std::size_t index = 0; index < completed_count; ++index) {
        auto& packet = packets[flows[completed_slots[index]].packet_slot];
        if (packet.write_operation == 0) {
            continue;
        }
        auto* operation = find_operation(packet.write_operation);
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          operation != nullptr,
          "fake completed packet lost its accepted write terminal");
        auto& write = std::get<write_operation>(operation->payload);
        auto applied = scheduler_->observe_effect(
          trace_event_descriptor{
            .kind = trace_event_kind::network,
            .domain = static_cast<std::uint32_t>(network_trace_phase::write),
            .stable_id = packet.write_operation,
            .coordinate_a = write.pair,
            .coordinate_b = write.side,
            .value = packet.data.size().value(),
            .result = static_cast<std::uint32_t>(errc::success),
            .effect = trace_action::network_operation_applied,
          },
          {},
          write.effect_trace);
        if (!applied) {
            return false;
        }
        if (write.drop_completion) {
            auto parked = scheduler_->observe_effect(
              trace_event_descriptor{
                .kind = trace_event_kind::network,
                .domain = static_cast<std::uint32_t>(
                  network_trace_phase::parked),
                .stable_id = packet.write_operation,
                .effect = trace_action::operation_parked,
              },
              {},
              write.parked_trace);
            if (!parked) {
                return false;
            }
        }
    }
    if (bandwidth_scheduled_) {
        auto canceled = scheduler_->cancel(bandwidth_event_);
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          canceled.has_value() && *canceled,
          "fake bandwidth replacement lost its scheduled wake-up");
        bandwidth_scheduled_ = false;
        bandwidth_flow_id_ = 0;
        auto replacement = scheduler_->reserve_event_slot();
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          replacement.has_value(),
          "fake bandwidth replacement lost its reserved scheduler slot");
        bandwidth_event_reservation_ = std::move(*replacement);
    }
    if (staged_control != nullptr) {
        const directed_link_key staged_key{
          .source = staged_control->source, .target = staged_control->target};
        switch (staged_control->kind) {
        case control_kind::egress:
            if (staged_control->capacity == config_.egress_capacity) {
                egress_limits.erase(staged_control->source);
            } else {
                egress_limits.at(
                  staged_control->source) = staged_control->capacity;
            }
            break;
        case control_kind::link:
            find_link(staged_key)->capacity = staged_control->capacity;
            break;
        case control_kind::ingress:
            if (staged_control->capacity == config_.ingress_capacity) {
                ingress_limits.erase(staged_control->source);
            } else {
                ingress_limits.at(
                  staged_control->source) = staged_control->capacity;
            }
            break;
        case control_kind::partition:
        case control_kind::heal:
        case control_kind::clog:
        case control_kind::unclog:
            break;
        }
    }
    if (staged_start != nullptr) {
        auto& packet = packets[staged_start->packet.slot];
        auto* pair = find_pair(packet.pair);
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          pair != nullptr,
          "fake bandwidth commit lost its staged pair");
        auto& direction = pair->directions[packet.side];
        auto& flow = flows[staged_start->flow_slot];
        packet.phase = packet_phase::active;
        direction.current_packet = staged_start->packet.slot;
        direction.transmitter = transmitter_state::busy;
        flow.packet_slot = staged_start->packet.slot;
        flow.rate = bandwidth_rate::finite(bandwidth_fraction{});
        flow.active = true;
    }
    for (std::size_t index = 0; index < active_count; ++index) {
        const auto slot = active_slots[index];
        flows[slot].remaining = staged_remaining[slot];
        flows[slot].last_update = now;
    }
    for (std::size_t index = 0; index < completed_count; ++index) {
        const auto slot = completed_slots[index];
        flows[slot].remaining = staged_remaining[slot];
        flows[slot].last_update = now;
        finish_flow(slot);
    }
    bandwidth_.swap(staged_bandwidth_);
    staged_bandwidth_->reset();
    const auto staged_reset = staged_bandwidth_->solve();
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      staged_reset.has_value(),
      "fake bandwidth staging planner failed to reset");
    for (std::size_t index = 0; index < bandwidth_->allocation_count();
         ++index) {
        const auto& allocation = bandwidth_->allocation_at(index);
        for (std::size_t active_index = 0; active_index < active_count;
             ++active_index) {
            auto& selected = flows[active_slots[active_index]];
            if (
              packets[active_packet_slots[active_slots[active_index]]].id
              == allocation.flow) {
                selected.rate = allocation.rate;
                selected.last_update = now;
                break;
            }
        }
    }
    allocation_digest_ = allocation_digest;
    if (!earliest) {
        return true;
    }
    bandwidth_event_reservation_.release();
    auto scheduled = scheduler_->schedule(
      *deadline,
      event_priority::normal(),
      [this] noexcept { complete_bandwidth(); },
      trace_event_descriptor{
        .kind = trace_event_kind::bandwidth,
        .domain = static_cast<std::uint32_t>(
          bandwidth_trace_phase::transfer_done),
        .stable_id = earliest_flow,
      },
      event_cleanup_policy::invoke,
      std::move(wake_trace));
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      scheduled.has_value(),
      "fake bandwidth could not schedule its reserved wake-up");
    bandwidth_event_ = *scheduled;
    bandwidth_deadline_ = *deadline;
    bandwidth_flow_id_ = earliest_flow;
    bandwidth_scheduled_ = true;
    return true;
}

void fake_network::impl::complete_bandwidth() noexcept {
    bandwidth_scheduled_ = false;
    if (scheduler_->discarding_failed_event()) {
        return;
    }
    auto replacement = scheduler_->reserve_event_slot();
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      replacement.has_value(),
      "fake bandwidth wake lost its reserved scheduler slot");
    bandwidth_event_reservation_ = std::move(*replacement);
    const auto stable_id = std::exchange(bandwidth_flow_id_, 0U);
    event_trace::reservation trace;
    event_trace::reservation wake_trace;
    for (auto& flow : flows) {
        if (flow.active && packets[flow.packet_slot].id == stable_id) {
            trace = std::move(packets[flow.packet_slot].finish_rebalance_trace);
            wake_trace = std::move(packets[flow.packet_slot].finish_wake_trace);
            break;
        }
    }
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      stable_id != 0,
      "fake bandwidth wake lost its stable flow ID");
    static_cast<void>(
      rebalance_bandwidth(std::move(trace), std::move(wake_trace), stable_id));
}

void fake_network::impl::finish_flow(std::uint16_t flow_slot) noexcept {
    auto& flow = flows[flow_slot];
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      flow.active && flow.remaining.zero(),
      "fake bandwidth finished a non-complete flow");
    auto& packet = packets[flow.packet_slot];
    auto* pair = find_pair(packet.pair);
    auto* link = find_link(packet.link);
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      pair != nullptr && link != nullptr
        && packet.phase == packet_phase::active,
      "fake bandwidth completion lost packet ownership");
    auto& direction = pair->directions[packet.side];
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      direction.current_packet == flow.packet_slot
        && direction.transmitter == transmitter_state::busy,
      "fake transmitter current packet differs from completed flow");

    flow.active = false;
    flow.remaining = bandwidth_fraction{};
    flow.rate = bandwidth_rate::unlimited();
    packet.phase = packet_phase::propagating;
    direction.current_packet.reset();
    direction.transmitter = transmitter_state::interframe;

    const auto token = packet_token{.id = packet.id, .slot = flow.packet_slot};
    const auto latency = packet_latency(*link, packet);
    const auto combined_latency = latency.checked_add(packet.delivery_delay);
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      combined_latency.has_value(),
      "fake packet delivery delay exceeded validated duration");
    auto delivery_deadline = add_deadline(
      scheduler_->now(),
      *combined_latency,
      scheduler_->limits().maximum_deadline());
    auto gap_deadline = add_deadline(
      scheduler_->now(),
      config_.interframe_gap,
      scheduler_->limits().maximum_deadline());
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      delivery_deadline.has_value() && gap_deadline.has_value(),
      "fake packet completion exceeded validated deadlines");

    packet.gap_event_reservation.release();
    auto gap = scheduler_->schedule(
      *gap_deadline,
      event_priority::normal(),
      [this, pair_id = packet.pair, side = packet.side] noexcept {
          complete_gap(pair_id, side);
      },
      trace_event_descriptor{
        .kind = trace_event_kind::bandwidth,
        .domain = static_cast<std::uint32_t>(
          bandwidth_trace_phase::transfer_done),
        .stable_id = packet.id,
      },
      event_cleanup_policy::invoke,
      std::move(packet.gap_trace));
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      gap.has_value(),
      "fake transmitter could not schedule reserved interframe completion");
    direction.gap_event = *gap;
    direction.gap_scheduled = true;

    packet.delivery_event_reservation.release();
    auto delivery = scheduler_->schedule(
      *delivery_deadline,
      event_priority::normal(),
      [this, token] noexcept { complete_delivery(token.slot, token.id); },
      trace_event_descriptor{
        .kind = trace_event_kind::network,
        .domain = static_cast<std::uint32_t>(network_trace_phase::delivery),
        .stable_id = packet.id,
      },
      event_cleanup_policy::invoke,
      std::move(packet.delivery_trace));
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      delivery.has_value(),
      "fake packet could not schedule reserved delivery");
    packet.delivery_event = *delivery;
    packet.delivery_scheduled = true;

    const auto operation_id = std::exchange(packet.write_operation, 0U);
    if (operation_id == 0) {
        return;
    }
    auto* operation = find_operation(operation_id);
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      operation != nullptr,
      "fake packet lost its accepted write terminal");
    auto& write = std::get<write_operation>(operation->payload);
    if (write.drop_completion) {
        write.abort_subscription = std::nullopt;
        write.parked = true;
        return;
    }
    auto done = std::move(write.done);
    write.abort_subscription = std::nullopt;
    write.admission.reset();
    write.done_set = true;
    operations.erase(operation_id);
    done.set_value(runtime::result<void>{});
}

void fake_network::impl::complete_gap(
  std::uint64_t pair_id, std::uint8_t direction_index) noexcept {
    auto* pair = find_pair(pair_id);
    if (pair == nullptr) {
        return;
    }
    auto& direction = pair->directions[direction_index];
    direction.gap_scheduled = false;
    direction.transmitter = transmitter_state::ready;
    const auto& endpoint = pair->endpoints[direction_index];
    if (
      endpoint.state == runtime::network_connection_state::open
      && endpoint.output == runtime::network_half_state::open
      && !endpoint.abort_requested && !endpoint.peer_reset) {
        start_next_packet(pair_id, direction_index);
        return;
    }
    if (direction.transmitter_slot) {
        const auto slot = *direction.transmitter_slot;
        flows[slot] = flow_state{};
        free_flows.push_back(slot);
        direction.transmitter_slot.reset();
    }
}

void fake_network::impl::complete_delivery(
  std::uint32_t slot, std::uint64_t packet_id) noexcept {
    auto* packet = find_packet(slot, packet_id);
    if (packet == nullptr) {
        return;
    }
    packet->delivery_scheduled = false;
    packet->ready_at = scheduler_->now();
    auto* link = find_link(packet->link);
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      link != nullptr,
      "fake delivery lost its directed link");
    if (link->clogged) {
        packet->phase = packet_phase::ready_clogged;
        link->ready.push_back(packet_token{.id = packet_id, .slot = slot});
        return;
    }
    apply_delivery(slot, packet_id);
}

void fake_network::impl::apply_delivery(
  std::uint32_t slot, std::uint64_t packet_id) noexcept {
    auto* packet = find_packet(slot, packet_id);
    if (packet == nullptr) {
        return;
    }
    auto* pair = find_pair(packet->pair);
    auto* link = find_link(packet->link);
    if (
      pair == nullptr || link == nullptr || link->partitioned
      || packet->drop_delivery || scheduler_->discarding_failed_event()) {
        if (pair != nullptr) {
            if (
              !scheduler_->discarding_failed_event()
              && !observe_packet_effect(
                *packet, trace_action::packet_dropped, errc::network_failure)) {
                return;
            }
            retire_packet(slot, errc::network_failure);
        }
        return;
    }
    auto& target = pair->endpoints[other_side(packet->side)];
    if (
      target.input != runtime::network_half_state::open
      || target.abort_requested || target.peer_reset) {
        if (!observe_packet_effect(
              *packet, trace_action::packet_dropped, errc::network_failure)) {
            return;
        }
        retire_packet(slot, errc::network_failure);
        return;
    }
    if (!observe_packet_effect(
          *packet, trace_action::packet_delivered, errc::success)) {
        return;
    }
    const auto pair_id = packet->pair;
    const auto side = packet->side;
    if (packet->clone) {
        const auto clone_token = *packet->clone;
        packet->clone.reset();
        auto* clone = find_packet(clone_token);
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          clone != nullptr && clone->phase == packet_phase::deferred_clone,
          "duplicate packet lost its deferred clone");
        clone->phase = packet_phase::queued;
        auto& direction = pair->directions[side];
        direction.deferred_clone = clone_token;
        if (direction.transmitter == transmitter_state::ready) {
            start_next_packet(pair_id, side);
        }
    }
    if (packet->reorder_delivery) {
        auto& direction = pair->directions[side];
        const auto index = static_cast<std::size_t>(
          packet->sequence % direction.sequence_slots.size());
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          direction.sequence_statuses[index] == sequence_status::live,
          "reordered packet lost its sequence reservation");
        direction.sequence_statuses[index] = sequence_status::delivered_early;
        packet->phase = packet_phase::delivered;
        direction.delivered.push_back(
          packet_token{.id = packet_id, .slot = slot});
        maybe_wake_read(pair_id, other_side(side));
        release_sequences(pair_id, side);
        return;
    }
    packet->phase = packet_phase::arrived;
    release_sequences(pair_id, side);
    collect_pair(pair_id);
}

void fake_network::impl::schedule_ready_delivery(link_state& link) noexcept {
    if (link.clogged || link.ready_chain_scheduled) {
        return;
    }
    while (!link.ready.empty()) {
        const auto token = link.ready.front();
        auto* packet = find_packet(token);
        if (packet == nullptr || packet->phase != packet_phase::ready_clogged) {
            link.ready.pop_front();
            continue;
        }
        packet->ready_event_reservation.release();
        auto scheduled = scheduler_->schedule(
          scheduler_->now(),
          event_priority::normal(),
          [this, key = link.key, token] noexcept {
              complete_ready_delivery(key, token.slot, token.id);
          },
          trace_event_descriptor{
            .kind = trace_event_kind::network,
            .domain = static_cast<std::uint32_t>(network_trace_phase::delivery),
            .stable_id = token.id,
          },
          event_cleanup_policy::invoke,
          std::move(packet->ready_trace));
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          scheduled.has_value(),
          "fake unclog could not schedule bounded ready delivery");
        link.ready_chain_event = *scheduled;
        link.ready_chain_scheduled = true;
        return;
    }
    if (!link.ready_fins.empty()) {
        const auto fin = link.ready_fins.front();
        auto* pair = find_pair(fin.pair);
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          pair != nullptr,
          "fake ready FIN lost its connection pair");
        auto& endpoint = pair->endpoints[fin.side];
        endpoint.fin_ready_event_reservation.release();
        auto scheduled = scheduler_->schedule(
          scheduler_->now(),
          event_priority::normal(),
          [this, key = link.key, fin] noexcept {
              complete_ready_fin(key, fin);
          },
          trace_event_descriptor{
            .kind = trace_event_kind::network,
            .domain = static_cast<std::uint32_t>(network_trace_phase::fin),
            .stable_id = fin.pair,
            .coordinate_a = fin.side,
          },
          event_cleanup_policy::invoke,
          std::move(endpoint.fin_ready_trace));
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          scheduled.has_value(),
          "fake unclog could not schedule bounded ready FIN");
        link.ready_chain_event = *scheduled;
        link.ready_chain_scheduled = true;
    }
}

void fake_network::impl::complete_ready_delivery(
  directed_link_key key, std::uint32_t slot, std::uint64_t packet_id) noexcept {
    auto* link = find_link(key);
    if (link == nullptr) {
        return;
    }
    link->ready_chain_scheduled = false;
    if (link->clogged || link->ready.empty()) {
        return;
    }
    const auto token = link->ready.front();
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      token.slot == slot && token.id == packet_id,
      "fake ready-delivery chain changed order");
    link->ready.pop_front();
    apply_delivery(slot, packet_id);
    schedule_ready_delivery(*link);
}

void fake_network::impl::complete_ready_fin(
  directed_link_key key, fin_token fin) noexcept {
    auto* link = find_link(key);
    if (link == nullptr) {
        return;
    }
    link->ready_chain_scheduled = false;
    if (link->clogged || link->ready_fins.empty()) {
        return;
    }
    const auto front = link->ready_fins.front();
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      front.pair == fin.pair && front.side == fin.side,
      "fake ready FIN chain changed order");
    link->ready_fins.pop_front();
    deliver_fin(fin.pair, fin.side);
    schedule_ready_delivery(*link);
}

void fake_network::impl::release_sequences(
  std::uint64_t pair_id, std::uint8_t direction_index) noexcept {
    auto* pair = find_pair(pair_id);
    if (pair == nullptr) {
        return;
    }
    auto& direction = pair->directions[direction_index];
    bool delivered_any = false;
    while (true) {
        if (
          direction.fin_sequence
          && *direction.fin_sequence == direction.expected_sequence) {
            if (!direction.fin_arrived && !direction.fin_retired) {
                break;
            }
            if (direction.fin_arrived) {
                direction.fin_delivered = true;
                delivered_any = true;
            }
            direction.fin_sequence.reset();
            direction.fin_arrived = false;
            direction.fin_retired = false;
            if (
              direction.expected_sequence
              == std::numeric_limits<std::uint64_t>::max()) {
                break;
            }
            ++direction.expected_sequence;
            continue;
        }
        const auto index = static_cast<std::size_t>(
          direction.expected_sequence % direction.sequence_slots.size());
        const auto status = direction.sequence_statuses[index];
        if (status == sequence_status::empty) {
            break;
        }
        const auto token = direction.sequence_slots[index];
        packet_state* packet = nullptr;
        if (status == sequence_status::live) {
            packet = find_packet(token);
            KWAQUE_INVARIANT(
              fake_network_state_invariant,
              packet != nullptr && packet->pair == pair_id
                && packet->side == direction_index
                && packet->sequence == direction.expected_sequence,
              "fake packet sequence ring lost its expected owner");
            if (packet->phase != packet_phase::arrived) {
                break;
            }
        }
        auto observed = scheduler_->observe_effect(
          trace_event_descriptor{
            .kind = trace_event_kind::network,
            .domain = static_cast<std::uint32_t>(
              network_trace_phase::sequence_release),
            .stable_id = token.id,
            .coordinate_a = direction.expected_sequence,
            .coordinate_b = direction_index,
            .result = static_cast<std::uint32_t>(status),
            .effect = trace_action::network_operation_applied,
          },
          {},
          direction.sequence_traces[index]);
        if (!observed) {
            return;
        }
        if (packet != nullptr) {
            packet->phase = packet_phase::delivered;
            direction.delivered.push_back(token);
            delivered_any = true;
        }
        direction.sequence_slots[index] = packet_token{};
        direction.sequence_statuses[index] = sequence_status::empty;
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          direction.sequence_entries != 0,
          "fake sequence tombstone count underflow");
        --direction.sequence_entries;
        if (
          direction.expected_sequence
          == std::numeric_limits<std::uint64_t>::max()) {
            break;
        }
        ++direction.expected_sequence;
    }
    if (delivered_any) {
        maybe_wake_read(pair_id, other_side(direction_index));
    }
}

void fake_network::impl::destroy_packet(std::uint32_t slot) noexcept {
    auto& packet = packets[slot];
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      packet.phase != packet_phase::free,
      "fake packet released twice");
    auto* pair = find_pair(packet.pair);
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      pair != nullptr,
      "fake packet outlived its connection pair");
    auto& direction = pair->directions[packet.side];
    direction.logical_bytes = *direction.logical_bytes.checked_sub(
      packet.data.size());
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      direction.packet_count != 0 && live_packets != 0,
      "fake packet count underflow");
    --direction.packet_count;
    --live_packets;
    packet_logical_bytes = *packet_logical_bytes.checked_sub(
      packet.logical_charge);
    packet_retained_bytes = *packet_retained_bytes.checked_sub(
      packet.retained_charge);
    auto* link = find_link(packet.link);
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      link != nullptr && link->packets != 0,
      "fake packet lost its directed-link ownership");
    --link->packets;
    packet.delivery_event_reservation.release();
    packet.delivery_trace.release();
    packet.gap_event_reservation.release();
    packet.gap_trace.release();
    packet.ready_event_reservation.release();
    packet.ready_trace.release();
    packet.flow_start_trace.release();
    packet.transfer_trace.release();
    packet.packet_effect_trace.release();
    packet.sequence_trace.release();
    packet.start_rebalance_trace.release();
    packet.finish_rebalance_trace.release();
    packet.start_wake_trace.release();
    packet.finish_wake_trace.release();
    packet.data = bytes::fragmented_buffer{};
    const auto stable_slot = packet.slot;
    packet = packet_state{};
    packet.slot = stable_slot;
    free_packets.push_back(slot);
}

void fake_network::impl::retire_packet(
  std::uint32_t slot, errc write_code) noexcept {
    auto& packet = packets[slot];
    if (
      packet.phase == packet_phase::free
      || packet.phase == packet_phase::retired) {
        return;
    }
    if (
      !scheduler_->discarding_failed_event()
      && !observe_packet_effect(
        packet, trace_action::packet_dropped, write_code)) {
        return;
    }
    if (packet.delivery_scheduled) {
        static_cast<void>(scheduler_->cancel(packet.delivery_event));
        packet.delivery_scheduled = false;
    }
    if (packet.clone) {
        const auto clone = *packet.clone;
        packet.clone.reset();
        if (find_packet(clone) != nullptr) {
            retire_packet(clone.slot, write_code);
        }
    }
    if (packet.phase == packet_phase::active) {
        auto* pair = find_pair(packet.pair);
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          pair != nullptr
            && pair->directions[packet.side].transmitter_slot.has_value(),
          "fake active packet lost its transmitter slot");
        auto& direction = pair->directions[packet.side];
        auto& flow = flows[*direction.transmitter_slot];
        flow.active = false;
        direction.current_packet.reset();
        direction.transmitter = transmitter_state::ready;
    }
    if (packet.write_operation != 0) {
        auto* operation = find_operation(packet.write_operation);
        if (operation != nullptr) {
            auto& write = std::get<write_operation>(operation->payload);
            write.terminal_code = write_code;
            write.terminal_event.release();
            auto terminal = scheduler_->schedule(
              scheduler_->now(),
              event_priority::highest(),
              [this, operation_id = packet.write_operation] noexcept {
                  complete_write_terminal(operation_id);
              },
              trace_event_descriptor{
                .kind = trace_event_kind::network,
                .domain = static_cast<std::uint32_t>(
                  network_trace_phase::write),
                .stable_id = packet.write_operation,
                .effect = trace_action::network_operation_applied,
              },
              event_cleanup_policy::invoke,
              std::move(write.terminal_trace));
            if (!terminal) {
                complete_write_terminal(packet.write_operation);
            }
        }
        packet.write_operation = 0;
    }
    const auto pair_id = packet.pair;
    const auto side = packet.side;
    const auto sequence = packet.sequence;
    auto* pair = find_pair(pair_id);
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      pair != nullptr,
      "fake retired packet lost its pair");
    auto& direction = pair->directions[side];
    const auto sequence_index = static_cast<std::size_t>(
      sequence % direction.sequence_slots.size());
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      direction.sequence_statuses[sequence_index] == sequence_status::live,
      "fake retired packet lost its sequence reservation");
    direction.sequence_statuses[sequence_index] = sequence_status::retired;
    packet.phase = packet_phase::retired;
    destroy_packet(slot);
    release_sequences(pair_id, side);
}

void fake_network::impl::abort_write(std::uint64_t operation_id) noexcept {
    auto* operation = find_operation(operation_id);
    if (operation == nullptr) {
        return;
    }
    auto& write = std::get<write_operation>(operation->payload);
    auto* packet = find_packet(write.packet);
    if (packet == nullptr || packet->phase != packet_phase::queued) {
        return;
    }
    retire_packet(write.packet.slot, errc::aborted);
}

void fake_network::impl::complete_immediate_write(
  std::uint64_t operation_id, errc code, bool disconnect) noexcept {
    auto* operation = find_operation(operation_id);
    if (operation == nullptr) {
        return;
    }
    auto& write = std::get<write_operation>(operation->payload);
    const auto pair_id = write.pair;
    const auto side = write.side;
    auto done = std::move(write.done);
    write.admission.reset();
    operations.erase(operation_id);
    if (disconnect && find_pair(pair_id) != nullptr) {
        reset_pair(pair_id, side);
    }
    done.set_value(runtime::failure(network_error(code)));
}

void fake_network::impl::complete_parked_connection_operation(
  std::uint64_t operation_id, errc code) noexcept {
    auto* operation = find_operation(operation_id);
    if (operation == nullptr) {
        return;
    }
    if (auto* connect = std::get_if<connect_operation>(&operation->payload)) {
        auto done = std::move(connect->done);
        const auto pair_id = connect->pair;
        operations.erase(operation_id);
        done.set_value(runtime::failure(network_error(code)));
        collect_pair(pair_id);
        return;
    }
    auto& accept = std::get<accept_operation>(operation->payload);
    auto done = std::move(accept.done);
    const auto pair_id = accept.pair;
    operations.erase(operation_id);
    done.set_value(runtime::failure(network_error(code)));
    if (pair_id) {
        collect_pair(*pair_id);
    }
}

void fake_network::impl::complete_write_terminal(
  std::uint64_t operation_id) noexcept {
    auto* operation = find_operation(operation_id);
    if (operation == nullptr) {
        return;
    }
    auto& write = std::get<write_operation>(operation->payload);
    auto done = std::move(write.done);
    write.abort_subscription = std::nullopt;
    write.admission.reset();
    if (!write.done_set) {
        write.done_set = true;
        const auto* failure = scheduler_->discarding_failed_event()
                                ? scheduler_->trace_failure()
                                : nullptr;
        const auto error = failure != nullptr
                             ? *failure
                             : write.terminal_error.value_or(network_error(
                                 write.terminal_code == errc::success
                                   ? errc::network_failure
                                   : write.terminal_code));
        operations.erase(operation_id);
        done.set_value(runtime::failure(error));
        return;
    }
    operations.erase(operation_id);
}

void fake_network::impl::maybe_wake_read(
  std::uint64_t pair_id, std::uint8_t side) noexcept {
    auto* pair = find_pair(pair_id);
    if (pair == nullptr || !pair->endpoints[side].read_operation) {
        return;
    }
    const auto operation = *pair->endpoints[side].read_operation;
    auto* state = find_operation(operation);
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      state != nullptr,
      "fake read wake lost operation state");
    const auto& read = std::get<read_operation>(state->payload);
    auto deadline = scheduler_->now();
    if (read.fault.action() == runtime::fault_action::delay) {
        auto delayed = deadline.checked_add(*read.fault.delay());
        if (!delayed) {
            complete_read_terminal(operation);
            return;
        }
        deadline = *delayed;
    }
    if (auto scheduled = schedule_read(operation, deadline); !scheduled) {
        complete_read_terminal(operation);
    }
}

runtime::result<void>
fake_network::shutdown_input(std::uint64_t pair_id, std::uint8_t side) {
    assert_current();
    auto* pair = impl_->find_pair(pair_id);
    if (pair == nullptr && impl_->state_ == fake_network_state::stopped) {
        return {};
    }
    if (pair == nullptr || side >= 2) {
        return runtime::failure(network_error(errc::closed));
    }
    auto& endpoint = pair->endpoints[side];
    if (endpoint.state != runtime::network_connection_state::open) {
        return runtime::failure(network_error(errc::closed));
    }
    if (endpoint.abort_requested) {
        return runtime::failure(network_error(errc::aborted));
    }
    if (endpoint.peer_reset) {
        return runtime::failure(network_error(errc::network_failure));
    }
    if (endpoint.input != runtime::network_half_state::open) {
        return runtime::failure(network_error(errc::closed));
    }
    endpoint.input = runtime::network_half_state::shut_down;
    impl_->discard_input(pair_id, side);
    if (endpoint.read_operation) {
        const auto operation_id = *endpoint.read_operation;
        auto* operation = impl_->find_operation(operation_id);
        if (operation != nullptr) {
            auto& read = std::get<impl::read_operation>(operation->payload);
            if (read.scheduled) {
                static_cast<void>(scheduler_->cancel(read.event));
            }
            read.terminal_event.release();
            auto terminal = scheduler_->schedule(
              scheduler_->now(),
              event_priority::highest(),
              [this, operation_id] noexcept {
                  impl_->complete_read_terminal(operation_id);
              },
              trace_event_descriptor{
                .kind = trace_event_kind::network,
                .domain = static_cast<std::uint32_t>(network_trace_phase::read),
                .stable_id = operation_id,
                .effect = trace_action::network_operation_applied,
              },
              event_cleanup_policy::invoke,
              std::move(read.terminal_trace));
            if (!terminal) {
                impl_->complete_read_terminal(operation_id);
            }
        }
    }
    return {};
}

runtime::result<void>
fake_network::shutdown_output(std::uint64_t pair_id, std::uint8_t side) {
    assert_current();
    auto* pair = impl_->find_pair(pair_id);
    if (pair == nullptr || side >= 2) {
        return runtime::failure(network_error(errc::closed));
    }
    auto& endpoint = pair->endpoints[side];
    if (endpoint.state != runtime::network_connection_state::open) {
        return runtime::failure(network_error(errc::closed));
    }
    if (endpoint.abort_requested) {
        return runtime::failure(network_error(errc::aborted));
    }
    if (endpoint.peer_reset) {
        return runtime::failure(network_error(errc::network_failure));
    }
    if (endpoint.output != runtime::network_half_state::open) {
        return runtime::failure(network_error(errc::closed));
    }
    auto& direction = pair->directions[side];
    if (direction.sequence_exhausted) {
        return runtime::failure(network_error(errc::out_of_range));
    }
    const auto deadline = add_deadline(
      scheduler_->now(),
      config_.fin_latency,
      scheduler_->limits().maximum_deadline());
    if (!deadline) {
        return runtime::failure(deadline.error());
    }
    endpoint.fin_event_reservation.release();
    auto fin = scheduler_->schedule(
      *deadline,
      event_priority::normal(),
      [this, pair_id, side] noexcept { impl_->deliver_fin(pair_id, side); },
      trace_event_descriptor{
        .kind = trace_event_kind::network,
        .domain = static_cast<std::uint32_t>(network_trace_phase::fin),
        .stable_id = pair_id,
        .coordinate_a = side,
      },
      event_cleanup_policy::invoke,
      std::move(endpoint.fin_trace));
    if (!fin) {
        return runtime::failure(fin.error());
    }
    endpoint.fin_event = *fin;
    endpoint.fin_scheduled = true;
    endpoint.output = runtime::network_half_state::shut_down;
    direction.fin_sequence = direction.next_sequence;
    direction.fin_arrived = false;
    direction.fin_retired = false;
    if (direction.next_sequence == std::numeric_limits<std::uint64_t>::max()) {
        direction.sequence_exhausted = true;
    } else {
        ++direction.next_sequence;
    }
    bool removed_active = false;
    event_trace::reservation rebalance_trace;
    event_trace::reservation wake_trace;
    std::uint64_t rebalance_id = 0;
    for (const auto token : direction.sequence_slots) {
        auto* packet = impl_->find_packet(token);
        if (
          packet == nullptr || packet->write_operation == 0
          || (packet->phase != impl::packet_phase::queued && packet->phase != impl::packet_phase::active)) {
            continue;
        }
        const bool active = packet->phase == impl::packet_phase::active;
        removed_active = removed_active || active;
        if (active) {
            rebalance_id = packet->id;
            rebalance_trace = std::move(packet->finish_rebalance_trace);
            wake_trace = std::move(packet->finish_wake_trace);
        }
        impl_->retire_packet(
          token.slot, active ? errc::network_failure : errc::closed);
    }
    direction.transmit_queue.clear();
    direction.deferred_clone.reset();
    if (removed_active) {
        static_cast<void>(impl_->rebalance_bandwidth(
          std::move(rebalance_trace), std::move(wake_trace), rebalance_id));
    }
    if (!direction.gap_scheduled && direction.transmitter_slot) {
        const auto slot = *direction.transmitter_slot;
        impl_->flows[slot] = impl::flow_state{};
        impl_->free_flows.push_back(slot);
        direction.transmitter_slot.reset();
        direction.transmitter = impl::transmitter_state::ready;
    }
    return {};
}

void fake_network::impl::deliver_fin(
  std::uint64_t pair_id, std::uint8_t side) noexcept {
    auto* pair = find_pair(pair_id);
    if (pair == nullptr) {
        return;
    }
    auto& source = pair->endpoints[side];
    source.fin_scheduled = false;
    auto& direction = pair->directions[side];
    if (
      scheduler_->discarding_failed_event() || source.abort_requested
      || source.peer_reset) {
        direction.fin_retired = true;
        release_sequences(pair_id, side);
        return;
    }
    const directed_link_key key{
      .source = source.local.address(), .target = source.remote.address()};
    auto* link = find_link(key);
    if (link != nullptr && link->clogged) {
        link->ready_fins.push_back(fin_token{.pair = pair_id, .side = side});
        return;
    }
    if (link != nullptr && link->partitioned) {
        direction.fin_retired = true;
        release_sequences(pair_id, side);
        return;
    }
    auto& target = pair->endpoints[other_side(side)];
    if (
      target.input == runtime::network_half_state::open
      && !target.abort_requested && !target.peer_reset) {
        auto observed = scheduler_->observe_effect(
          trace_event_descriptor{
            .kind = trace_event_kind::network,
            .domain = static_cast<std::uint32_t>(network_trace_phase::fin),
            .stable_id = pair_id,
            .coordinate_a = side,
            .effect = trace_action::fin_delivered,
          },
          {},
          source.fin_effect_trace);
        if (!observed) {
            return;
        }
        direction.fin_arrived = true;
    } else {
        direction.fin_retired = true;
    }
    release_sequences(pair_id, side);
}

void fake_network::impl::discard_input(
  std::uint64_t pair_id, std::uint8_t side) noexcept {
    auto* pair = find_pair(pair_id);
    if (pair == nullptr) {
        return;
    }
    const auto incoming_index = other_side(side);
    auto& incoming = pair->directions[incoming_index];
    while (!incoming.delivered.empty()) {
        const auto token = incoming.delivered.front();
        incoming.delivered.pop_front();
        auto* packet = find_packet(token);
        if (packet != nullptr) {
            packet->phase = packet_phase::retired;
            destroy_packet(token.slot);
        }
    }
    for (const auto token : incoming.sequence_slots) {
        auto* packet = find_packet(token);
        if (packet != nullptr && packet->phase == packet_phase::arrived) {
            retire_packet(token.slot, errc::network_failure);
        }
    }
    if (incoming.fin_sequence && incoming.fin_arrived) {
        incoming.fin_arrived = false;
        incoming.fin_retired = true;
    }
    release_sequences(pair_id, incoming_index);
}

void fake_network::impl::fail_endpoint_operations(
  std::uint64_t pair_id, std::uint8_t side, errc code) noexcept {
    auto* pair = find_pair(pair_id);
    if (pair == nullptr) {
        return;
    }
    auto& endpoint = pair->endpoints[side];
    if (endpoint.read_operation) {
        const auto operation_id = *endpoint.read_operation;
        auto* operation = find_operation(operation_id);
        if (operation != nullptr) {
            auto& read = std::get<read_operation>(operation->payload);
            if (read.scheduled) {
                static_cast<void>(scheduler_->cancel(read.event));
            }
            read.terminal_event.release();
            auto terminal = scheduler_->schedule(
              scheduler_->now(),
              event_priority::highest(),
              [this, operation_id] noexcept {
                  complete_read_terminal(operation_id);
              },
              trace_event_descriptor{
                .kind = trace_event_kind::network,
                .domain = static_cast<std::uint32_t>(network_trace_phase::read),
                .stable_id = operation_id,
                .effect = trace_action::network_operation_applied,
              },
              event_cleanup_policy::invoke,
              std::move(read.terminal_trace));
            if (!terminal) {
                complete_read_terminal(operation_id);
            }
        }
    }
    auto& outgoing = pair->directions[side];
    bool removed_active = false;
    event_trace::reservation rebalance_trace;
    event_trace::reservation wake_trace;
    std::uint64_t rebalance_id = 0;
    for (const auto token : outgoing.sequence_slots) {
        auto* packet = find_packet(token);
        if (packet == nullptr) {
            continue;
        }
        const bool active = packet->phase == packet_phase::active;
        removed_active = removed_active || active;
        if (active) {
            rebalance_id = packet->id;
            rebalance_trace = std::move(packet->finish_rebalance_trace);
            wake_trace = std::move(packet->finish_wake_trace);
        }
        retire_packet(token.slot, code);
    }
    work_ids.clear();
    for (const auto& [operation_id, operation] : operations) {
        const auto* write = std::get_if<write_operation>(&operation->payload);
        const auto* read = std::get_if<read_operation>(&operation->payload);
        if (
          (write != nullptr && write->pair == pair_id && write->side == side
           && write->parked)
          || (read != nullptr && read->pair == pair_id && read->side == side
              && read->parked)) {
            work_ids.push_back(operation_id);
        }
    }
    for (const auto operation_id : work_ids) {
        auto* operation = find_operation(operation_id);
        if (operation == nullptr) {
            continue;
        }
        if (auto* read = std::get_if<read_operation>(&operation->payload)) {
            read->terminal_event.release();
            auto terminal = scheduler_->schedule(
              scheduler_->now(),
              event_priority::highest(),
              [this, operation_id] noexcept {
                  complete_read_terminal(operation_id);
              },
              trace_event_descriptor{
                .kind = trace_event_kind::network,
                .domain = static_cast<std::uint32_t>(network_trace_phase::read),
                .stable_id = operation_id,
                .effect = trace_action::network_operation_applied,
              },
              event_cleanup_policy::invoke,
              std::move(read->terminal_trace));
            if (!terminal) {
                complete_read_terminal(operation_id);
            }
            continue;
        }
        auto& write = std::get<write_operation>(operation->payload);
        write.terminal_code = code;
        write.terminal_event.release();
        auto terminal = scheduler_->schedule(
          scheduler_->now(),
          event_priority::highest(),
          [this, operation_id] noexcept {
              complete_write_terminal(operation_id);
          },
          trace_event_descriptor{
            .kind = trace_event_kind::network,
            .domain = static_cast<std::uint32_t>(network_trace_phase::write),
            .stable_id = operation_id,
            .effect = trace_action::network_operation_applied,
          },
          event_cleanup_policy::invoke,
          std::move(write.terminal_trace));
        if (!terminal) {
            complete_write_terminal(operation_id);
        }
    }
    outgoing.transmit_queue.clear();
    outgoing.deferred_clone.reset();
    if (outgoing.gap_scheduled) {
        static_cast<void>(scheduler_->cancel(outgoing.gap_event));
        outgoing.gap_scheduled = false;
    }
    if (outgoing.transmitter_slot) {
        const auto slot = *outgoing.transmitter_slot;
        flows[slot] = flow_state{};
        free_flows.push_back(slot);
        outgoing.transmitter_slot.reset();
    }
    outgoing.current_packet.reset();
    outgoing.transmitter = transmitter_state::ready;
    if (removed_active) {
        static_cast<void>(rebalance_bandwidth(
          std::move(rebalance_trace), std::move(wake_trace), rebalance_id));
    }
}

void fake_network::impl::reset_pair(
  std::uint64_t pair_id, std::uint8_t initiator) noexcept {
    auto* pair = find_pair(pair_id);
    if (pair == nullptr) {
        return;
    }
    if (pair->reset_applied) {
        return;
    }
    auto reset_observed = scheduler_->observe_effect(
      trace_event_descriptor{
        .kind = trace_event_kind::network,
        .domain = static_cast<std::uint32_t>(network_trace_phase::reset),
        .stable_id = pair_id,
        .coordinate_a = initiator,
        .effect = trace_action::reset_applied,
      },
      {},
      pair->reset_trace);
    if (!reset_observed) {
        return;
    }
    pair->reset_applied = true;
    auto& local = pair->endpoints[initiator];
    auto& peer = pair->endpoints[other_side(initiator)];
    peer.peer_reset = true;
    peer.input = runtime::network_half_state::shut_down;
    peer.output = runtime::network_half_state::shut_down;
    local.input = runtime::network_half_state::shut_down;
    local.output = runtime::network_half_state::shut_down;
    for (std::uint8_t side = 0; side < pair->endpoints.size(); ++side) {
        auto& endpoint = pair->endpoints[side];
        if (endpoint.fin_scheduled) {
            static_cast<void>(scheduler_->cancel(endpoint.fin_event));
            endpoint.fin_scheduled = false;
        }
        auto& direction = pair->directions[side];
        if (direction.fin_sequence) {
            direction.fin_retired = true;
        }
    }
    discard_input(pair_id, 0);
    discard_input(pair_id, 1);
    fail_endpoint_operations(pair_id, initiator, errc::aborted);
    fail_endpoint_operations(
      pair_id, other_side(initiator), errc::network_failure);
    work_ids.clear();
    for (const auto& [operation_id, operation] : operations) {
        const auto* connect = std::get_if<connect_operation>(
          &operation->payload);
        const auto* accept = std::get_if<accept_operation>(&operation->payload);
        if (
          (connect != nullptr && connect->pair == pair_id && connect->parked)
          || (accept != nullptr && accept->pair && *accept->pair == pair_id
              && accept->parked)) {
            work_ids.push_back(operation_id);
        }
    }
    for (const auto operation_id : work_ids) {
        auto* operation = find_operation(operation_id);
        if (operation == nullptr) {
            continue;
        }
        auto* connect = std::get_if<connect_operation>(&operation->payload);
        auto* accept = std::get_if<accept_operation>(&operation->payload);
        const auto owner_side = connect != nullptr ? std::uint8_t{0}
                                                   : std::uint8_t{1};
        const auto code = owner_side == initiator ? errc::aborted
                                                  : errc::network_failure;
        auto& terminal_event = connect != nullptr ? connect->terminal_event
                                                  : accept->terminal_event;
        auto& terminal_trace = connect != nullptr ? connect->terminal_trace
                                                  : accept->terminal_trace;
        terminal_event.release();
        auto terminal = scheduler_->schedule(
          scheduler_->now(),
          event_priority::highest(),
          [this, operation_id, code] noexcept {
              complete_parked_connection_operation(operation_id, code);
          },
          trace_event_descriptor{
            .kind = trace_event_kind::network,
            .domain = static_cast<std::uint32_t>(
              connect != nullptr ? network_trace_phase::connect_client
                                 : network_trace_phase::accept),
            .stable_id = operation_id,
            .effect = trace_action::network_operation_applied,
          },
          event_cleanup_policy::invoke,
          std::move(terminal_trace));
        if (!terminal) {
            complete_parked_connection_operation(operation_id, code);
        }
    }
    release_sequences(pair_id, 0);
    release_sequences(pair_id, 1);

    if (pair->backlog_listener) {
        if (
          auto* listener = find_listener(*pair->backlog_listener);
          listener != nullptr && remove_backlog_pair(*listener, pair_id)
          && backlog_entries != 0) {
            --backlog_entries;
        }
        pair->backlog_listener.reset();
    }
    collect_pair(pair_id);
}

void fake_network::request_abort(
  std::uint64_t pair_id, std::uint8_t side) noexcept {
    assert_current();
    auto* pair = impl_->find_pair(pair_id);
    if (pair == nullptr || side >= 2) {
        return;
    }
    auto& endpoint = pair->endpoints[side];
    if (
      endpoint.abort_requested
      || endpoint.state == runtime::network_connection_state::closed) {
        return;
    }
    endpoint.abort_requested = true;
    impl_->reset_pair(pair_id, side);
}

seastar::future<runtime::result<void>>
fake_network::close_connection(std::uint64_t pair_id, std::uint8_t side) {
    assert_current();
    auto* pair = impl_->find_pair(pair_id);
    if (pair == nullptr || side >= 2) {
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::result<void>{});
    }
    auto& endpoint = pair->endpoints[side];
    if (endpoint.state == runtime::network_connection_state::closed) {
        return endpoint.close_done && endpoint.close_done->available()
                 ? endpoint.close_done->get_shared_future()
                 : seastar::make_ready_future<runtime::result<void>>(
                     runtime::result<void>{});
    }
    if (endpoint.state == runtime::network_connection_state::closing) {
        return endpoint.close_done->get_shared_future();
    }
    try {
        endpoint.close_done.emplace();
    } catch (...) {
        return seastar::current_exception_as_future<runtime::result<void>>();
    }
    auto prepared_fault = impl_->prepare_fault(
      runtime::builtin_fault_point::close, network_object_key(pair_id, side));
    if (!prepared_fault) {
        endpoint.close_done.reset();
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::failure(prepared_fault.error()));
    }
    auto close_delay = config_.close_latency;
    if (prepared_fault->decision.action() == runtime::fault_action::delay) {
        auto combined = close_delay.checked_add(
          *prepared_fault->decision.delay());
        if (!combined) {
            endpoint.close_done.reset();
            return ready_failure<void>(errc::out_of_range);
        }
        close_delay = *combined;
    }
    const auto deadline = add_deadline(
      scheduler_->now(), close_delay, scheduler_->limits().maximum_deadline());
    if (!deadline) {
        endpoint.close_done.reset();
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::failure(deadline.error()));
    }
    auto parked_credit = impl_->reserve_parked(
      prepared_fault->decision.action()
      == runtime::fault_action::drop_completion);
    if (!parked_credit) {
        endpoint.close_done.reset();
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::failure(parked_credit.error()));
    }
    auto committed = impl_->commit_fault(*prepared_fault);
    if (!committed) {
        endpoint.close_done.reset();
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::failure(committed.error()));
    }
    endpoint.close_fault = prepared_fault->decision;
    endpoint.close_drop_completion = endpoint.close_fault.action()
                                     == runtime::fault_action::drop_completion;
    endpoint.close_parked_credit = std::move(*parked_credit);
    endpoint.state = runtime::network_connection_state::closing;
    request_abort(pair_id, side);
    endpoint.close_event_reservation.release();
    auto closed = scheduler_->schedule(
      *deadline,
      event_priority::normal(),
      [this, pair_id, side] noexcept {
          impl_->complete_connection_close(pair_id, side);
      },
      trace_event_descriptor{
        .kind = trace_event_kind::network,
        .domain = static_cast<std::uint32_t>(network_trace_phase::close),
        .stable_id = pair_id,
        .coordinate_a = side,
        .effect = trace_action::network_operation_applied,
      },
      event_cleanup_policy::invoke,
      std::move(endpoint.close_trace));
    if (!closed) {
        endpoint.state = runtime::network_connection_state::closed;
        endpoint.close_parked_credit.release();
        endpoint.close_done->set_value(runtime::failure(closed.error()));
    } else {
        endpoint.close_event = *closed;
    }
    return endpoint.close_done->get_shared_future();
}

void fake_network::impl::complete_connection_close(
  std::uint64_t pair_id, std::uint8_t side) noexcept {
    auto* pair = find_pair(pair_id);
    if (pair == nullptr) {
        return;
    }
    auto& endpoint = pair->endpoints[side];
    endpoint.state = runtime::network_connection_state::closed;
    endpoint.input = runtime::network_half_state::shut_down;
    endpoint.output = runtime::network_half_state::shut_down;
    const auto* failure = scheduler_->discarding_failed_event()
                            ? scheduler_->trace_failure()
                            : nullptr;
    if (failure != nullptr) {
        endpoint.close_parked_credit.release();
        endpoint.close_done->set_value(runtime::failure(*failure));
    } else if (endpoint.close_drop_completion) {
        auto observed = scheduler_->observe_effect(
          trace_event_descriptor{
            .kind = trace_event_kind::network,
            .domain = static_cast<std::uint32_t>(network_trace_phase::parked),
            .stable_id = pair_id,
            .coordinate_a = side,
            .effect = trace_action::operation_parked,
          },
          {},
          endpoint.close_parked_trace);
        if (!observed) {
            return;
        }
        return;
    } else if (endpoint.close_fault.action() == runtime::fault_action::error) {
        endpoint.close_parked_credit.release();
        endpoint.close_done->set_value(
          runtime::failure(network_error(errc::fault_injected)));
    } else {
        endpoint.close_parked_credit.release();
        endpoint.close_done->set_value(runtime::result<void>{});
    }
    collect_pair(pair_id);
}

void fake_network::release_connection_handle(
  std::uint64_t pair_id, std::uint8_t side) noexcept {
    assert_current();
    auto* pair = impl_->find_pair(pair_id);
    KWAQUE_INVARIANT(
      fake_network_handle_invariant,
      pair != nullptr && side < 2 && pair->endpoints[side].handle_owned
        && pair->endpoints[side].state
             == runtime::network_connection_state::closed,
      "fake connection handle release observed invalid state");
    pair->endpoints[side].handle_owned = false;
    impl_->collect_pair(pair_id);
}

void fake_network::impl::collect_pair(std::uint64_t pair_id) noexcept {
    auto* pair = find_pair(pair_id);
    if (pair == nullptr) {
        return;
    }
    for (const auto& endpoint : pair->endpoints) {
        if (
          endpoint.handle_owned || endpoint.read_operation
          || (endpoint.exposed && endpoint.state != runtime::network_connection_state::closed)) {
            return;
        }
    }
    if (
      pair->directions[0].packet_count != 0
      || pair->directions[1].packet_count != 0
      || pair->directions[0].sequence_entries != 0
      || pair->directions[1].sequence_entries != 0
      || pair->directions[0].transmitter_slot
      || pair->directions[1].transmitter_slot
      || pair->directions[0].gap_scheduled || pair->directions[1].gap_scheduled
      || pair->directions[0].fin_sequence || pair->directions[1].fin_sequence) {
        return;
    }
    for (const auto& [operation_id, operation] : operations) {
        static_cast<void>(operation_id);
        bool references = false;
        std::visit(
          [&](const auto& value) {
              using type = std::decay_t<decltype(value)>;
              if constexpr (
                std::same_as<type, connect_operation>
                || std::same_as<type, read_operation>
                || std::same_as<type, write_operation>) {
                  references = value.pair == pair_id;
              } else if constexpr (std::same_as<type, accept_operation>) {
                  references = value.pair && *value.pair == pair_id;
              }
          },
          operation->payload);
        if (references) {
            return;
        }
    }
    connection_locals.erase(pair->reserved_client_local);
    fault_occurrences_.erase(
      network_fault_key{
        .point = runtime::builtin_fault_point::connect,
        .object = runtime::fault_object_key::from_u64(pair_id),
      });
    for (const auto point :
         {runtime::builtin_fault_point::network_read,
          runtime::builtin_fault_point::network_write,
          runtime::builtin_fault_point::close}) {
        for (std::uint8_t side = 0; side < 2; ++side) {
            fault_occurrences_.erase(
              network_fault_key{
                .point = point,
                .object = network_object_key(pair_id, side),
              });
        }
    }
    pairs.erase(pair_id);
}

runtime::result<void> fake_network::impl::prepare_stop_batches() {
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      stop_batches_.empty(),
      "fake network retained stop-batch reservations while open");
    const auto owners = operations.size() + live_packets + pairs.size()
                        + listeners.size() + links.size() + 1U;
    const auto batches = std::max<std::size_t>(
      1U, (owners + config_.stop_batch - 1U) / config_.stop_batch);
    if (
      batches
      > std::numeric_limits<std::uint64_t>::max() - next_stop_batch_id + 1U) {
        return runtime::failure(network_error(errc::out_of_range));
    }
    if (batches > stop_event_capacity_.size()) {
        return runtime::failure(network_error(errc::queue_full));
    }
    seastar::chunked_fifo<stop_batch_reservation, 32, 2> prepared;
    prepared.reserve(batches);
    for (std::size_t index = 0; index < batches; ++index) {
        const auto id = next_stop_batch_id + index;
        const trace_event_descriptor descriptor{
          .kind = trace_event_kind::network,
          .domain = static_cast<std::uint32_t>(network_trace_phase::stop),
          .stable_id = id,
        };
        auto trace = scheduler_->reserve_trace(descriptor);
        if (!trace) {
            return runtime::failure(trace.error());
        }
        prepared.push_back(
          stop_batch_reservation{
            .trace = std::move(*trace),
            .id = id,
          });
    }
    for (auto& batch : prepared) {
        batch.event = std::move(stop_event_capacity_.front());
        stop_event_capacity_.pop_front();
    }
    stop_event_capacity_.clear();
    next_stop_batch_id += batches;
    stop_batches_ = std::move(prepared);
    return {};
}

void fake_network::impl::schedule_stop_batch() noexcept {
    if (stop_batch_scheduled_) {
        return;
    }
    if (stop_batches_.empty()) {
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          !has_stop_preparation_work(),
          "fake network exhausted stop batches before preparation drained");
        maybe_finish_stop();
        return;
    }
    auto batch = std::move(stop_batches_.front());
    stop_batches_.pop_front();
    batch.event.release();
    const auto id = batch.id;
    auto scheduled = scheduler_->schedule(
      scheduler_->now(),
      event_priority::highest(),
      [this] noexcept {
          stop_batch_scheduled_ = false;
          if (scheduler_->discarding_failed_event()) [[unlikely]] {
              const auto* failure = scheduler_->trace_failure();
              KWAQUE_INVARIANT(
                fake_network_state_invariant,
                failure != nullptr,
                "discarded network stop batch has no trace failure");
              force_discard_all(*failure);
          } else {
              run_stop_batch();
          }
      },
      trace_event_descriptor{
        .kind = trace_event_kind::network,
        .domain = static_cast<std::uint32_t>(network_trace_phase::stop),
        .stable_id = id,
      },
      event_cleanup_policy::invoke,
      std::move(batch.trace));
    if (!scheduled) {
        force_discard_all(scheduled.error());
        return;
    }
    stop_batch_scheduled_ = true;
}

fake_network::impl::operation_state*
fake_network::impl::next_stop_operation() noexcept {
    for (auto& [id, operation] : operations) {
        static_cast<void>(id);
        if (!operation->stop_scheduled) {
            return operation.get();
        }
    }
    return nullptr;
}

fake_network::impl::packet_state*
fake_network::impl::next_stop_packet() noexcept {
    packet_state* selected = nullptr;
    for (auto& packet : packets) {
        if (
          packet.phase != packet_phase::free
          && (selected == nullptr || packet.id < selected->id)) {
            selected = &packet;
        }
    }
    return selected;
}

fake_network::impl::pair_state* fake_network::impl::next_stop_pair() noexcept {
    for (auto& [id, pair] : pairs) {
        static_cast<void>(id);
        if (!pair->stop_prepared) {
            return pair.get();
        }
    }
    return nullptr;
}

fake_network::impl::listener_state*
fake_network::impl::next_stop_listener() noexcept {
    for (auto& [id, listener] : listeners) {
        static_cast<void>(id);
        if (!listener->stop_prepared) {
            return listener.get();
        }
    }
    return nullptr;
}

fake_network::impl::link_state* fake_network::impl::next_stop_link() noexcept {
    link_state* selected = nullptr;
    for (auto& [key, link] : links) {
        static_cast<void>(key);
        if (selected == nullptr || link->id < selected->id) {
            selected = link.get();
        }
    }
    return selected;
}

bool fake_network::impl::has_stop_preparation_work() const noexcept {
    if (!stop_resources_released_ || live_packets != 0 || !links.empty()) {
        return true;
    }
    return std::ranges::any_of(
             operations,
             [](const auto& entry) { return !entry.second->stop_scheduled; })
           || std::ranges::any_of(
             pairs,
             [](const auto& entry) { return !entry.second->stop_prepared; })
           || std::ranges::any_of(listeners, [](const auto& entry) {
                  return !entry.second->stop_prepared;
              });
}

void fake_network::impl::run_stop_batch() noexcept {
    std::uint32_t completed = 0;
    while (completed < config_.stop_batch) {
        if (state_ == fake_network_state::stopped) {
            return;
        }
        if (auto* operation = next_stop_operation(); operation != nullptr) {
            prepare_stop_operation(*operation);
        } else if (auto* packet = next_stop_packet(); packet != nullptr) {
            discard_stop_packet(*packet);
        } else if (auto* pair = next_stop_pair(); pair != nullptr) {
            prepare_stop_pair(*pair);
        } else if (auto* listener = next_stop_listener(); listener != nullptr) {
            prepare_stop_listener(*listener);
        } else if (auto* link = next_stop_link(); link != nullptr) {
            discard_stop_link(*link);
        } else if (!stop_resources_released_) {
            if (bandwidth_scheduled_) {
                static_cast<void>(scheduler_->cancel(bandwidth_event_));
                bandwidth_scheduled_ = false;
                bandwidth_flow_id_ = 0;
            }
            bandwidth_event_reservation_.release();
            free_flows.clear();
            for (std::uint16_t slot = 0; slot < flows.size(); ++slot) {
                flows[slot] = flow_state{};
                free_flows.push_back(slot);
            }
            bandwidth_->reset();
            const auto solved = bandwidth_->solve();
            staged_bandwidth_->reset();
            const auto staged_solved = staged_bandwidth_->solve();
            KWAQUE_INVARIANT(
              fake_network_state_invariant,
              solved.has_value() && staged_solved.has_value(),
              "empty planner failed during network stop");
            allocation_digest_ = bandwidth_->allocation_digest();
            egress_limits.clear();
            ingress_limits.clear();
            port_cursors.clear();
            fault_occurrences_.clear();
            stop_resources_released_ = true;
        } else {
            break;
        }
        ++completed;
    }
    if (has_stop_preparation_work()) {
        schedule_stop_batch();
    } else {
        stop_batches_.clear();
        maybe_finish_stop();
    }
}

void fake_network::impl::prepare_stop_operation(
  operation_state& operation) noexcept {
    auto cancel = [&](event_id id) {
        if (!id.valid()) {
            return;
        }
        const auto canceled = scheduler_->cancel(id);
        if (!canceled && !stop_failure_) {
            stop_failure_ = network_error(canceled.error().code());
        }
    };
    std::visit(
      [&](auto& value) {
          using type = std::decay_t<decltype(value)>;
          if constexpr (std::same_as<type, bind_operation>) {
              cancel(value.event);
          } else if constexpr (std::same_as<type, connect_operation>) {
              operation.stop_needs_completion = value.parked
                                                || !value.client_done;
              cancel(value.client_event);
              cancel(value.incoming_event);
              value.abort_subscription = std::nullopt;
              auto* listener = find_listener(value.listener);
              auto* pair = find_pair(value.pair);
              if (!value.client_done) {
                  KWAQUE_INVARIANT(
                    fake_network_state_invariant,
                    pending_connects != 0,
                    "network stop lost pending connect accounting");
                  --pending_connects;
                  value.client_done = true;
              }
              if (!value.incoming_done) {
                  if (listener != nullptr) {
                      KWAQUE_INVARIANT(
                        fake_network_state_invariant,
                        listener->captured_connects != 0
                          && listener->reserved_backlog != 0,
                        "network stop lost captured listener accounting");
                      --listener->captured_connects;
                      --listener->reserved_backlog;
                  }
                  KWAQUE_INVARIANT(
                    fake_network_state_invariant,
                    backlog_entries != 0,
                    "network stop lost reserved backlog accounting");
                  --backlog_entries;
                  value.incoming_done = true;
              } else if (
                listener != nullptr
                && remove_backlog_pair(*listener, value.pair)) {
                  if (pair != nullptr) {
                      pair->backlog_listener.reset();
                  }
                  KWAQUE_INVARIANT(
                    fake_network_state_invariant,
                    backlog_entries != 0,
                    "network stop lost visible backlog accounting");
                  --backlog_entries;
              }
          } else if constexpr (std::same_as<type, accept_operation>) {
              cancel(value.event);
              if (
                auto* listener = find_listener(value.listener);
                listener != nullptr
                && listener->accept_operation == operation.id) {
                  listener->accept_operation.reset();
              }
          } else if constexpr (std::same_as<type, read_operation>) {
              cancel(value.event);
              if (auto* pair = find_pair(value.pair); pair != nullptr) {
                  pair->endpoints[value.side].read_operation.reset();
              }
          } else if constexpr (std::same_as<type, write_operation>) {
              value.abort_subscription = std::nullopt;
              if (auto* packet = find_packet(value.packet); packet != nullptr) {
                  packet->write_operation = 0;
              }
          } else if constexpr (std::same_as<type, control_operation>) {
              cancel(value.event);
              KWAQUE_INVARIANT(
                fake_network_state_invariant,
                active_controls != 0,
                "network stop lost active control accounting");
              --active_controls;
          }
      },
      operation.payload);

    operation.stop_scheduled = true;
    operation.stop_event.release();
    const auto id = operation.id;
    auto terminal = scheduler_->schedule(
      scheduler_->now(),
      event_priority::normal(),
      [this, id] noexcept { complete_stop_operation(id); },
      trace_event_descriptor{
        .kind = trace_event_kind::network,
        .domain = static_cast<std::uint32_t>(network_trace_phase::stop),
        .stable_id = id,
        .result = static_cast<std::uint32_t>(errc::aborted),
        .effect = trace_action::stop_terminal,
      },
      event_cleanup_policy::invoke,
      std::move(operation.stop_trace));
    if (!terminal) {
        force_discard_all(terminal.error());
    }
}

void fake_network::impl::discard_stop_packet(packet_state& packet) noexcept {
    if (packet.delivery_scheduled) {
        static_cast<void>(scheduler_->cancel(packet.delivery_event));
        packet.delivery_scheduled = false;
    }
    auto* pair = find_pair(packet.pair);
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      pair != nullptr,
      "network stop packet lost its pair");
    auto& direction = pair->directions[packet.side];
    if (direction.gap_scheduled && packet.phase == packet_phase::propagating) {
        static_cast<void>(scheduler_->cancel(direction.gap_event));
        direction.gap_scheduled = false;
    }
    if (packet.phase == packet_phase::active && direction.transmitter_slot) {
        flows[*direction.transmitter_slot].active = false;
        direction.current_packet.reset();
        direction.transmitter = transmitter_state::ready;
    }
    if (direction.deferred_clone && direction.deferred_clone->id == packet.id) {
        direction.deferred_clone.reset();
    }
    const auto index = static_cast<std::size_t>(
      packet.sequence % direction.sequence_slots.size());
    if (direction.sequence_slots[index].id == packet.id) {
        direction.sequence_slots[index] = packet_token{};
        direction.sequence_statuses[index] = sequence_status::empty;
        direction.sequence_traces[index].release();
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          direction.sequence_entries != 0,
          "network stop sequence count underflow");
        --direction.sequence_entries;
    }
    packet.clone.reset();
    packet.write_operation = 0;
    packet.phase = packet_phase::retired;
    destroy_packet(packet.slot);
}

void fake_network::impl::prepare_stop_pair(pair_state& pair) noexcept {
    auto cancel = [&](event_id id) {
        if (id.valid()) {
            static_cast<void>(scheduler_->cancel(id));
        }
    };
    pair.stop_prepared = true;
    pair.stop_terminals_pending = 2;
    if (pair.backlog_listener) {
        if (
          auto* listener = find_listener(*pair.backlog_listener);
          listener != nullptr && remove_backlog_pair(*listener, pair.id)) {
            KWAQUE_INVARIANT(
              fake_network_state_invariant,
              backlog_entries != 0,
              "network stop pair backlog underflow");
            --backlog_entries;
        }
        pair.backlog_listener.reset();
    }
    connection_locals.erase(pair.reserved_client_local);
    for (std::uint8_t side = 0; side < 2; ++side) {
        auto& endpoint = pair.endpoints[side];
        if (endpoint.fin_scheduled) {
            cancel(endpoint.fin_event);
            endpoint.fin_scheduled = false;
        }
        if (endpoint.state == runtime::network_connection_state::closing) {
            cancel(endpoint.close_event);
        }
        endpoint.state = runtime::network_connection_state::closed;
        endpoint.input = runtime::network_half_state::shut_down;
        endpoint.output = runtime::network_half_state::shut_down;
        endpoint.handle_owned = false;
        endpoint.exposed = false;
        endpoint.read_operation.reset();
        auto& direction = pair.directions[side];
        if (direction.gap_scheduled) {
            cancel(direction.gap_event);
            direction.gap_scheduled = false;
        }
        direction.transmit_queue.clear();
        direction.delivered.clear();
        direction.current_packet.reset();
        direction.deferred_clone.reset();
        direction.fin_sequence.reset();
        direction.fin_arrived = false;
        direction.fin_retired = false;
        direction.fin_delivered = false;
        for (std::size_t index = 0; index < direction.sequence_slots.size();
             ++index) {
            direction.sequence_slots[index] = packet_token{};
            direction.sequence_statuses[index] = sequence_status::empty;
            direction.sequence_traces[index].release();
        }
        direction.sequence_entries = 0;
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          direction.packet_count == 0 && direction.logical_bytes.value() == 0,
          "network stop prepared pair before packets drained");
        if (direction.transmitter_slot) {
            const auto slot = *direction.transmitter_slot;
            flows[slot] = flow_state{};
            free_flows.push_back(slot);
            direction.transmitter_slot.reset();
        }
        direction.transmitter = transmitter_state::ready;

        endpoint.stop_event_reservation.release();
        auto terminal = scheduler_->schedule(
          scheduler_->now(),
          event_priority::normal(),
          [this, pair_id = pair.id, side] noexcept {
              complete_stop_endpoint(pair_id, side);
          },
          trace_event_descriptor{
            .kind = trace_event_kind::network,
            .domain = static_cast<std::uint32_t>(network_trace_phase::stop),
            .stable_id = pair.id,
            .coordinate_a = side,
            .result = static_cast<std::uint32_t>(errc::aborted),
            .effect = trace_action::stop_terminal,
          },
          event_cleanup_policy::invoke,
          std::move(endpoint.stop_trace));
        if (!terminal) {
            force_discard_all(terminal.error());
            return;
        }
    }
}

void fake_network::impl::prepare_stop_listener(
  listener_state& listener) noexcept {
    listener.stop_prepared = true;
    if (listener.close_event_id.valid()) {
        static_cast<void>(scheduler_->cancel(listener.close_event_id));
    }
    const auto registered = listener_registry.find(listener.endpoint);
    if (
      registered != listener_registry.end()
      && registered->second == listener.id) {
        listener_registry.erase(registered);
    }
    while (!listener.backlog.empty()) {
        listener.backlog.pop_front();
        KWAQUE_INVARIANT(
          fake_network_state_invariant,
          backlog_entries != 0,
          "network stop listener backlog underflow");
        --backlog_entries;
    }
    listener.reserved_backlog = 0;
    listener.captured_connects = 0;
    listener.accept_operation.reset();
    listener.handle_owned = false;
    listener.aborted = true;
    listener.closing = false;
    listener.closed = true;
    listener.stop_event.release();
    const auto id = listener.id;
    auto terminal = scheduler_->schedule(
      scheduler_->now(),
      event_priority::normal(),
      [this, id] noexcept { complete_stop_listener(id); },
      trace_event_descriptor{
        .kind = trace_event_kind::network,
        .domain = static_cast<std::uint32_t>(network_trace_phase::stop),
        .stable_id = id,
        .result = static_cast<std::uint32_t>(errc::aborted),
        .effect = trace_action::stop_terminal,
      },
      event_cleanup_policy::invoke,
      std::move(listener.stop_trace));
    if (!terminal) {
        force_discard_all(terminal.error());
    }
}

void fake_network::impl::discard_stop_link(link_state& link) noexcept {
    if (link.ready_chain_scheduled) {
        static_cast<void>(scheduler_->cancel(link.ready_chain_event));
        link.ready_chain_scheduled = false;
    }
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      link.packets == 0,
      "network stop discarded link before packets drained");
    link.ready.clear();
    link.ready_fins.clear();
    const auto key = link.key;
    links.erase(key);
}

void fake_network::impl::complete_stop_operation(
  std::uint64_t operation_id) noexcept {
    const auto found = operations.find(operation_id);
    if (found == operations.end()) {
        return;
    }
    auto operation = std::move(found->second);
    operations.erase(found);
    const auto* trace_failure = scheduler_->discarding_failed_event()
                                  ? scheduler_->trace_failure()
                                  : nullptr;
    const auto error = trace_failure != nullptr ? *trace_failure
                                                : network_error(errc::aborted);
    const bool needs_completion = operation->stop_needs_completion;
    std::visit(
      [&](auto& value) {
          using type = std::decay_t<decltype(value)>;
          auto done = std::move(value.done);
          if constexpr (std::same_as<type, connect_operation>) {
              if (needs_completion) {
                  done.set_value(runtime::failure(error));
              }
          } else {
              done.set_value(runtime::failure(error));
          }
      },
      operation->payload);
    maybe_finish_stop();
}

void fake_network::impl::complete_stop_endpoint(
  std::uint64_t pair_id, std::uint8_t side) noexcept {
    auto* pair = find_pair(pair_id);
    if (pair == nullptr) {
        return;
    }
    auto& endpoint = pair->endpoints[side];
    if (endpoint.close_done && !endpoint.close_done->available()) {
        const auto* failure = scheduler_->discarding_failed_event()
                                ? scheduler_->trace_failure()
                                : nullptr;
        endpoint.close_done->set_value(
          runtime::failure(
            failure != nullptr ? *failure : network_error(errc::aborted)));
    }
    KWAQUE_INVARIANT(
      fake_network_state_invariant,
      pair->stop_terminals_pending != 0,
      "network stop endpoint terminal underflow");
    --pair->stop_terminals_pending;
    if (pair->stop_terminals_pending == 0) {
        pairs.erase(pair_id);
    }
    maybe_finish_stop();
}

void fake_network::impl::complete_stop_listener(
  std::uint64_t listener_id) noexcept {
    auto* listener = find_listener(listener_id);
    if (listener == nullptr) {
        return;
    }
    if (listener->close_done && !listener->close_done->available()) {
        const auto* failure = scheduler_->discarding_failed_event()
                                ? scheduler_->trace_failure()
                                : nullptr;
        listener->close_done->set_value(
          runtime::failure(
            failure != nullptr ? *failure : network_error(errc::aborted)));
    }
    listeners.erase(listener_id);
    maybe_finish_stop();
}

void fake_network::impl::maybe_finish_stop() noexcept {
    if (
      state_ != fake_network_state::stopping || stop_batch_scheduled_
      || has_stop_preparation_work() || !operations.empty() || !pairs.empty()
      || !listeners.empty()) {
        return;
    }
    KWAQUE_INVARIANT(
      fake_network_drained_invariant,
      listener_registry.empty() && connection_locals.empty()
        && pending_connects == 0 && backlog_entries == 0 && live_packets == 0
        && packet_logical_bytes.value() == 0
        && packet_retained_bytes.value() == 0 && active_controls == 0
        && parked_operations == 0 && egress_limits.empty()
        && ingress_limits.empty() && port_cursors.empty()
        && fault_occurrences_.empty() && links.empty() && !bandwidth_scheduled_
        && free_packets.size() == config_.maximum_packets
        && free_flows.size() == config_.maximum_active_flows
        && bandwidth_->allocation_count() == 0
        && bandwidth_->resource_count() == 0
        && bandwidth_->membership_count() == 0
        && staged_bandwidth_->allocation_count() == 0
        && staged_bandwidth_->resource_count() == 0
        && staged_bandwidth_->membership_count() == 0,
      "fake network stop completed with retained ownership");
    state_ = fake_network_state::stopped;
    if (stop_failure_) {
        stop_done_->set_value(runtime::failure(*stop_failure_));
    } else {
        stop_done_->set_value(runtime::result<void>{});
    }
}

void fake_network::impl::force_discard_all(
  const runtime::operation_error& failure) noexcept {
    if (forcing_discard_) {
        return;
    }
    forcing_discard_ = true;
    stop_failure_ = network_error(failure.code());
    if (scheduler_->trace_failed()) {
        static_cast<void>(scheduler_->discard_failed());
    }
    while (!operations.empty()) {
        auto operation = std::move(operations.begin()->second);
        operations.erase(operations.begin());
        std::visit(
          [&](auto& value) {
              using type = std::decay_t<decltype(value)>;
              const bool needs_completion = [&] {
                  if constexpr (std::same_as<type, connect_operation>) {
                      return value.parked || !value.client_done;
                  }
                  return true;
              }();
              auto done = std::move(value.done);
              if (needs_completion) {
                  done.set_value(runtime::failure(*stop_failure_));
              }
          },
          operation->payload);
    }
    for (auto& packet : packets) {
        if (packet.phase != packet_phase::free) {
            discard_stop_packet(packet);
        }
    }
    for (auto& [id, pair] : pairs) {
        static_cast<void>(id);
        for (auto& endpoint : pair->endpoints) {
            if (endpoint.close_done && !endpoint.close_done->available()) {
                endpoint.close_done->set_value(
                  runtime::failure(*stop_failure_));
            }
        }
    }
    pairs.clear();
    for (auto& [id, listener] : listeners) {
        static_cast<void>(id);
        if (listener->close_done && !listener->close_done->available()) {
            listener->close_done->set_value(runtime::failure(*stop_failure_));
        }
    }
    listeners.clear();
    KWAQUE_INVARIANT(
      fake_network_drained_invariant,
      parked_operations == 0,
      "fake network discard retained parked-operation credits");
    links.clear();
    listener_registry.clear();
    connection_locals.clear();
    egress_limits.clear();
    ingress_limits.clear();
    port_cursors.clear();
    fault_occurrences_.clear();
    stop_batches_.clear();
    stop_event_capacity_.clear();
    free_flows.clear();
    for (std::uint16_t slot = 0; slot < flows.size(); ++slot) {
        flows[slot] = flow_state{};
        free_flows.push_back(slot);
    }
    bandwidth_->reset();
    static_cast<void>(bandwidth_->solve());
    staged_bandwidth_->reset();
    static_cast<void>(staged_bandwidth_->solve());
    allocation_digest_ = bandwidth_->allocation_digest();
    pending_connects = 0;
    backlog_entries = 0;
    active_controls = 0;
    bandwidth_scheduled_ = false;
    bandwidth_flow_id_ = 0;
    bandwidth_event_reservation_.release();
    stop_resources_released_ = true;
    stop_batch_scheduled_ = false;
    state_ = fake_network_state::stopped;
    forcing_discard_ = false;
    if (stop_done_ && !stop_done_->available()) {
        stop_done_->set_value(runtime::failure(*stop_failure_));
    }
}

} // namespace kwaque::simulation
