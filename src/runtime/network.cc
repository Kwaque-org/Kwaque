#include "src/runtime/network.h"

#include "src/base/invariant.h"

#include <arpa/inet.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <cstring>
#include <stdexcept>
#include <system_error>

namespace kwaque::runtime {

namespace {

constexpr invariant_id network_admission_invariant{
  "KQ-NETWORK-WRITE-ADMISSION-RELEASED"};
constexpr invariant_id network_admission_move_invariant{
  "KQ-NETWORK-WRITE-ADMISSION-MOVE-BEFORE-USE"};

std::size_t
validated_pending_write_operations(network_connection_limits limits) {
    if (!limits.validate()) {
        throw std::invalid_argument("invalid network write admission limits");
    }
    return limits.pending_writes;
}

std::size_t validated_pending_write_bytes(network_connection_limits limits) {
    if (!limits.validate()) {
        throw std::invalid_argument("invalid network write admission limits");
    }
    return limits.pending_write_bytes.value();
}

} // namespace

result<void> network_connection_limits::validate() const noexcept {
    if (pending_write_bytes.value() == 0 || pending_writes == 0) {
        return failure(
          operation_error{errc::invalid_argument, operation_kind::network});
    }
    if (
      pending_write_bytes > maximum_pending_network_write_bytes
      || pending_writes > maximum_pending_network_writes) {
        return failure(
          operation_error{errc::out_of_range, operation_kind::network});
    }
    return {};
}

network_write_admission::network_write_admission(
  network_connection_limits limits)
  : limits_(limits)
  , operation_units_(validated_pending_write_operations(limits))
  , byte_units_(validated_pending_write_bytes(limits)) {}

network_connection_limits
network_write_admission::prepare_move(network_write_admission& other) noexcept {
    KWAQUE_INVARIANT(
      network_admission_move_invariant,
      !other.moved_from_
        && other.operation_units_.current() == other.limits_.pending_writes
        && other.byte_units_.current()
             == other.limits_.pending_write_bytes.value(),
      "network write admission moved with active reservations");
    return other.limits_;
}

network_write_admission::network_write_admission(
  network_write_admission&& other) noexcept
  : limits_(prepare_move(other))
  , operation_units_(std::move(other.operation_units_))
  , byte_units_(std::move(other.byte_units_)) {
    other.moved_from_ = true;
}

network_write_admission::~network_write_admission() {
    if (moved_from_) {
        return;
    }
    KWAQUE_INVARIANT(
      network_admission_invariant,
      operation_units_.current() == limits_.pending_writes
        && byte_units_.current() == limits_.pending_write_bytes.value(),
      "network write admission destroyed with live reservations");
}

byte_count network_write_admission::pending_bytes() const {
    KWAQUE_INVARIANT(
      network_admission_move_invariant,
      !moved_from_,
      "network write admission inspected after move");
    return byte_count{
      limits_.pending_write_bytes.value() - byte_units_.current()};
}

std::uint32_t network_write_admission::pending_writes() const {
    KWAQUE_INVARIANT(
      network_admission_move_invariant,
      !moved_from_,
      "network write admission inspected after move");
    return static_cast<std::uint32_t>(
      limits_.pending_writes - operation_units_.current());
}

std::optional<network_address>
network_address::try_parse_numeric(std::string_view value) noexcept {
    if (value.size() >= 2 && value.front() == '[' && value.back() == ']') {
        value.remove_prefix(1);
        value.remove_suffix(1);
    }
    if (value.empty() || value.find('\0') != std::string_view::npos) {
        return std::nullopt;
    }

    std::uint32_t scope = 0;
    bool has_scope = false;
    if (const auto separator = value.rfind('%'); separator != value.npos) {
        const auto scope_text = value.substr(separator + 1);
        if (scope_text.empty()) {
            return std::nullopt;
        }
        const auto [end, error] = std::from_chars(
          scope_text.data(), scope_text.data() + scope_text.size(), scope);
        if (
          error != std::errc{}
          || end != scope_text.data() + scope_text.size()) {
            return std::nullopt;
        }
        has_scope = true;
        value = value.substr(0, separator);
    }

    std::array<char, INET6_ADDRSTRLEN> text{};
    if (value.size() >= text.size()) {
        return std::nullopt;
    }
    std::copy(value.begin(), value.end(), text.begin());

    in_addr ipv4_address{};
    if (::inet_pton(AF_INET, text.data(), &ipv4_address) == 1) {
        if (has_scope) {
            return std::nullopt;
        }
        std::array<std::byte, 4> bytes{};
        std::memcpy(bytes.data(), &ipv4_address, bytes.size());
        return ipv4(bytes);
    }

    in6_addr ipv6_address{};
    if (::inet_pton(AF_INET6, text.data(), &ipv6_address) == 1) {
        storage_type bytes{};
        std::memcpy(bytes.data(), &ipv6_address, bytes.size());
        return ipv6(bytes, scope);
    }
    return std::nullopt;
}

result<void> network_listen_options::validate() const noexcept {
    if (backlog == 0) {
        return failure(
          operation_error{errc::invalid_argument, operation_kind::network});
    }
    if (
      backlog > maximum_listen_backlog
      || receive_buffer_bytes > maximum_socket_buffer_bytes
      || send_buffer_bytes > maximum_socket_buffer_bytes) {
        return failure(
          operation_error{errc::out_of_range, operation_kind::network});
    }
    return connection_limits.validate();
}

result<void> validate_network_read_limit(byte_count maximum_bytes) noexcept {
    if (maximum_bytes.value() == 0) {
        return failure(
          operation_error{errc::invalid_argument, operation_kind::network});
    }
    if (maximum_bytes > maximum_network_operation_bytes) {
        return failure(
          operation_error{errc::out_of_range, operation_kind::network});
    }
    return {};
}

result<void>
validate_network_write(const bytes::fragmented_buffer& data) noexcept {
    if (data.empty()) {
        return failure(
          operation_error{errc::invalid_argument, operation_kind::network});
    }
    if (
      data.size() > maximum_network_operation_bytes
      || data.retained_bytes() > maximum_network_operation_bytes) {
        return failure(
          operation_error{errc::out_of_range, operation_kind::network});
    }
    return {};
}

result<void> validate_network_write(
  const bytes::fragmented_buffer& data,
  network_connection_limits limits) noexcept {
    if (auto valid = validate_network_write(data); !valid) {
        return failure(valid.error());
    }
    if (data.retained_bytes() > limits.pending_write_bytes) {
        return failure(
          operation_error{errc::out_of_range, operation_kind::network});
    }
    return {};
}

result<network_read_result> network_read_result::make(
  bytes::fragmented_buffer data, bool eof, byte_count maximum_bytes) noexcept {
    if (auto valid = validate_network_read_limit(maximum_bytes); !valid) {
        return failure(valid.error());
    }
    if (data.size() > maximum_bytes) {
        return failure(
          operation_error{errc::out_of_range, operation_kind::network});
    }
    if (data.empty() && !eof) {
        return failure(
          operation_error{errc::invalid_argument, operation_kind::network});
    }
    return network_read_result{std::move(data), eof};
}

} // namespace kwaque::runtime
