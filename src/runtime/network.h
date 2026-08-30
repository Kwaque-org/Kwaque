#ifndef KWAQUE_SRC_RUNTIME_NETWORK_H_
#define KWAQUE_SRC_RUNTIME_NETWORK_H_

#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/runtime/error.h"
#include "src/runtime/shard_affinity.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>

#include <array>
#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>

namespace kwaque::runtime {

enum class network_address_family : std::uint8_t {
    ipv4,
    ipv6,
};

enum class network_connection_state : std::uint8_t {
    open,
    closing,
    closed,
};

enum class network_half_state : std::uint8_t {
    open,
    shut_down,
};

class network_address final {
public:
    using storage_type = std::array<std::byte, 16>;

    [[nodiscard]] static constexpr network_address
    ipv4(std::array<std::byte, 4> bytes) noexcept {
        storage_type storage{};
        for (std::size_t index = 0; index < bytes.size(); ++index) {
            storage[index] = bytes[index];
        }
        return network_address{network_address_family::ipv4, storage, 0};
    }

    [[nodiscard]] static constexpr network_address
    ipv6(storage_type bytes, std::uint32_t scope = 0) noexcept {
        return network_address{network_address_family::ipv6, bytes, scope};
    }

    [[nodiscard]] static std::optional<network_address>
    try_parse_numeric(std::string_view value) noexcept;

    [[nodiscard]] constexpr network_address_family family() const noexcept {
        return family_;
    }
    [[nodiscard]] constexpr const storage_type& bytes() const noexcept {
        return bytes_;
    }
    [[nodiscard]] constexpr std::uint32_t scope() const noexcept {
        return scope_;
    }

    auto operator<=>(const network_address&) const = default;

private:
    constexpr network_address(
      network_address_family family,
      storage_type bytes,
      std::uint32_t scope) noexcept
      : bytes_(bytes)
      , scope_(scope)
      , family_(family) {}

    storage_type bytes_;
    std::uint32_t scope_;
    network_address_family family_;
};

class network_endpoint final {
public:
    constexpr network_endpoint(
      network_address address, std::uint16_t port) noexcept
      : address_(address)
      , port_(port) {}

    [[nodiscard]] constexpr const network_address& address() const noexcept {
        return address_;
    }
    [[nodiscard]] constexpr std::uint16_t port() const noexcept {
        return port_;
    }

    auto operator<=>(const network_endpoint&) const = default;

private:
    network_address address_;
    std::uint16_t port_;
};

inline constexpr byte_count maximum_network_operation_bytes{
  64U * 1024U * 1024U};
inline constexpr byte_count maximum_socket_buffer_bytes{16U * 1024U * 1024U};
inline constexpr byte_count maximum_pending_network_write_bytes{
  64U * 1024U * 1024U};
inline constexpr std::uint32_t maximum_pending_network_writes = 1024;
inline constexpr std::uint32_t maximum_listen_backlog = 65535;

struct network_connection_limits final {
    // The active write is included in both limits. Byte admission accounts for
    // retained backing rather than only the logical slice. A single write
    // larger than the byte capacity is out_of_range; temporary saturation fails
    // immediately with queue_full. Rejected payload is released rather than
    // retained in an unbounded waiter list.
    byte_count pending_write_bytes{16U * 1024U * 1024U};
    std::uint32_t pending_writes{256};

    [[nodiscard]] result<void> validate() const noexcept;

    bool operator==(const network_connection_limits&) const = default;
};

class network_write_admission final {
public:
    class reservation final {
    public:
        reservation(reservation&&) noexcept = default;
        reservation& operator=(reservation&&) noexcept = default;
        reservation(const reservation&) = delete;
        reservation& operator=(const reservation&) = delete;
        ~reservation() = default;

        [[nodiscard]] byte_count bytes() const noexcept {
            return byte_count{byte_units_.count()};
        }

    private:
        friend class network_write_admission;

        reservation(
          seastar::semaphore_units<> operation_units,
          seastar::semaphore_units<> byte_units) noexcept
          : operation_units_(std::move(operation_units))
          , byte_units_(std::move(byte_units)) {}

        seastar::semaphore_units<> operation_units_;
        seastar::semaphore_units<> byte_units_;
    };

    explicit network_write_admission(network_connection_limits limits);
    network_write_admission(const network_write_admission&) = delete;
    network_write_admission& operator=(const network_write_admission&) = delete;
    network_write_admission(network_write_admission&& other) noexcept;
    network_write_admission& operator=(network_write_admission&&) = delete;
    ~network_write_admission();

    // The owning connection validates the payload against limits() before this
    // hot non-blocking operation. A disengaged result means temporary
    // count/byte saturation and maps to queue_full at the connection boundary.
    [[nodiscard]] std::optional<reservation>
    try_acquire(byte_count bytes) noexcept {
        auto operation = seastar::try_get_units(operation_units_, 1);
        if (!operation) {
            return std::nullopt;
        }
        auto byte_reservation = seastar::try_get_units(
          byte_units_, bytes.value());
        if (!byte_reservation) {
            return std::nullopt;
        }
        return reservation{std::move(*operation), std::move(*byte_reservation)};
    }
    [[nodiscard]] byte_count pending_bytes() const;
    [[nodiscard]] std::uint32_t pending_writes() const;
    [[nodiscard]] const network_connection_limits& limits() const noexcept {
        return limits_;
    }

private:
    [[nodiscard]] static network_connection_limits
    prepare_move(network_write_admission& other) noexcept;

    network_connection_limits limits_;
    seastar::semaphore operation_units_;
    seastar::semaphore byte_units_;
    bool moved_from_{false};
};

struct network_listen_options final {
    std::uint32_t backlog{128};
    byte_count receive_buffer_bytes{};
    byte_count send_buffer_bytes{};
    bool reuse_address{true};
    network_connection_limits connection_limits{};

    [[nodiscard]] result<void> validate() const noexcept;

    bool operator==(const network_listen_options&) const = default;
};

class network_read_result final {
public:
    [[nodiscard]] static result<network_read_result> make(
      bytes::fragmented_buffer data,
      bool eof,
      byte_count maximum_bytes) noexcept;

    network_read_result(network_read_result&&) noexcept = default;
    network_read_result& operator=(network_read_result&&) noexcept = default;
    network_read_result(const network_read_result&) = delete;
    network_read_result& operator=(const network_read_result&) = delete;

    [[nodiscard]] const bytes::fragmented_buffer& data() const noexcept {
        return data_;
    }
    [[nodiscard]] bytes::fragmented_buffer take_data() && noexcept {
        return std::move(data_);
    }
    [[nodiscard]] bool eof() const noexcept { return eof_; }

private:
    network_read_result(bytes::fragmented_buffer data, bool eof) noexcept
      : data_(std::move(data))
      , eof_(eof) {}

    bytes::fragmented_buffer data_;
    bool eof_;
};

[[nodiscard]] result<void>
validate_network_read_limit(byte_count maximum_bytes) noexcept;
[[nodiscard]] result<void>
validate_network_write(const bytes::fragmented_buffer& data) noexcept;
// `limits` were validated when the connection was created.
[[nodiscard]] result<void> validate_network_write(
  const bytes::fragmented_buffer& data,
  network_connection_limits limits) noexcept;

// This is a bounded byte-stream contract only. A connection admits one read at
// a time and serializes writes. A concurrent read fails with unavailable;
// a write larger than its configured byte capacity is out_of_range and
// temporary count/byte saturation fails with queue_full. Accepted writes retain
// their owning payload and admission until native completion. Close rejects new
// work, resolves queued work exactly once, and drains accepted operations
// before destruction. Framing, correlation, retries, TLS policy, and protocol
// negotiation belong to higher-level components.
template<typename Connection>
concept network_connection_contract
  = std::is_nothrow_move_constructible_v<Connection>
    && !std::is_copy_constructible_v<Connection>
    && requires(
      Connection& connection,
      byte_count maximum_bytes,
      bytes::fragmented_buffer data,
      seastar::abort_source& abort_source) {
           {
               connection.read(maximum_bytes, abort_source)
           } -> std::same_as<seastar::future<result<network_read_result>>>;
           {
               connection.write(std::move(data), abort_source)
           } -> std::same_as<seastar::future<result<void>>>;
           {
               connection.local_endpoint()
           } noexcept -> std::same_as<network_endpoint>;
           {
               connection.remote_endpoint()
           } noexcept -> std::same_as<network_endpoint>;
           {
               connection.state()
           } noexcept -> std::same_as<network_connection_state>;
           {
               connection.input_state()
           } noexcept -> std::same_as<network_half_state>;
           {
               connection.output_state()
           } noexcept -> std::same_as<network_half_state>;
           {
               connection.limits()
           } noexcept -> std::same_as<const network_connection_limits&>;
           { connection.owner() } noexcept -> std::same_as<owner_shard>;
           { connection.shutdown_input() } -> std::same_as<result<void>>;
           { connection.shutdown_output() } -> std::same_as<result<void>>;
           { connection.request_abort() } -> std::same_as<void>;
           {
               connection.close()
           } -> std::same_as<seastar::future<result<void>>>;
       };

template<typename Listener, typename Connection>
concept network_listener_contract
  = network_connection_contract<Connection>
    && std::is_nothrow_move_constructible_v<Listener>
    && !std::is_copy_constructible_v<Listener>
    && requires(Listener& listener, seastar::abort_source& abort_source) {
           {
               listener.accept(abort_source)
           } -> std::same_as<seastar::future<result<Connection>>>;
           {
               listener.local_endpoint()
           } noexcept -> std::same_as<network_endpoint>;
           {
               listener.connection_limits()
           } noexcept -> std::same_as<const network_connection_limits&>;
           { listener.owner() } noexcept -> std::same_as<owner_shard>;
           { listener.request_abort() } -> std::same_as<void>;
           { listener.close() } -> std::same_as<seastar::future<result<void>>>;
       };

template<typename Backend>
concept network_backend = requires {
    typename Backend::connection_type;
    typename Backend::listener_type;
} && network_listener_contract<typename Backend::listener_type, typename Backend::connection_type> && requires(Backend& backend, network_endpoint endpoint, std::optional<network_endpoint> local_endpoint, network_connection_limits connection_limits, network_listen_options options, seastar::abort_source& abort_source) {
    {
        backend.connect(
          endpoint, local_endpoint, connection_limits, abort_source)
    }
    -> std::same_as<seastar::future<result<typename Backend::connection_type>>>;
    {
        backend.listen(endpoint, options)
    } -> std::same_as<seastar::future<result<typename Backend::listener_type>>>;
    { backend.owner() } noexcept -> std::same_as<owner_shard>;
};

} // namespace kwaque::runtime

#endif // KWAQUE_SRC_RUNTIME_NETWORK_H_
