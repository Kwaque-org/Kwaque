#include "src/runtime/production/network.h"

#include "src/base/invariant.h"
#include "src/runtime/fragmented_buffer_internal.h"
#include "src/runtime/production/network_connect_internal.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/defer.hh>

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <new>
#include <system_error>
#include <utility>

namespace kwaque::runtime::production {

namespace {

constexpr invariant_id connection_move_invariant{"KQ-NET-CONN-MOVE-IDLE"};
constexpr invariant_id connection_closed_invariant{"KQ-NET-CONN-CLOSED"};
constexpr invariant_id connection_gate_invariant{"KQ-NET-CONN-GATE"};
constexpr invariant_id listener_move_invariant{"KQ-NET-LISTEN-MOVE-IDLE"};
constexpr invariant_id listener_closed_invariant{"KQ-NET-LISTEN-CLOSED"};
constexpr std::uint64_t maximum_unflushed_bytes = 1024U * 1024U;
constexpr auto native_input_buffer_limit = static_cast<unsigned>(
  maximum_contiguous_allocation_bytes);
static_assert(
  static_cast<std::size_t>(native_input_buffer_limit)
  == maximum_contiguous_allocation_bytes);

operation_error network_error(errc code) noexcept {
    return operation_error{code, operation_kind::network};
}

errc map_network_system_error(const std::error_code& error) noexcept {
    if (error == std::errc::operation_canceled) {
        return errc::aborted;
    }
    if (error == std::errc::timed_out) {
        return errc::timed_out;
    }
    if (
      error == std::errc::no_buffer_space
      || error == std::errc::not_enough_memory
      || error == std::errc::resource_unavailable_try_again) {
        return errc::resource_exhausted;
    }
    if (error == std::errc::invalid_argument) {
        return errc::invalid_argument;
    }
    return errc::network_failure;
}

operation_error network_error_from_exception(std::exception_ptr exception) {
    try {
        std::rethrow_exception(std::move(exception));
    } catch (const std::bad_alloc&) {
        throw;
    } catch (const seastar::abort_requested_exception&) {
        return network_error(errc::aborted);
    } catch (const std::system_error& error) {
        return network_error(map_network_system_error(error.code()));
    } catch (...) {
        return network_error(errc::network_failure);
    }
}

seastar::socket_address native_endpoint(const network_endpoint& endpoint) {
    if (endpoint.address().family() == network_address_family::ipv4) {
        in_addr address{};
        std::memcpy(
          &address, endpoint.address().bytes().data(), sizeof(address));
        return seastar::socket_address(
          seastar::net::inet_address{address}, endpoint.port());
    }

    in6_addr address{};
    std::memcpy(&address, endpoint.address().bytes().data(), sizeof(address));
    return seastar::socket_address(
      seastar::ipv6_addr{address, endpoint.port()}, endpoint.address().scope());
}

network_endpoint kwaque_endpoint(const seastar::socket_address& endpoint) {
    const auto address = endpoint.addr();
    if (address.is_ipv4()) {
        std::array<std::byte, 4> bytes{};
        std::memcpy(bytes.data(), address.data(), bytes.size());
        return network_endpoint{network_address::ipv4(bytes), endpoint.port()};
    }

    network_address::storage_type bytes{};
    std::memcpy(bytes.data(), address.data(), bytes.size());
    return network_endpoint{
      network_address::ipv6(bytes, address.scope()), endpoint.port()};
}

} // namespace

connection::connection(
  seastar::connected_socket native,
  network_endpoint local,
  network_endpoint remote,
  network_connection_limits limits)
  : native_(std::move(native))
  , input_(native_.input(
      seastar::connected_socket_input_stream_config{
        .buffer_size = 8192,
        .min_buffer_size = 512,
        .max_buffer_size = native_input_buffer_limit,
      }))
  , output_(native_.output())
  , local_(local)
  , remote_(remote)
  , limits_(limits)
  , admission_(limits) {
    native_.set_nodelay(true);
}

owner_shard connection::prepare_move(connection& other) noexcept {
    other.owner_.assert_current();
    KWAQUE_INVARIANT(
      connection_move_invariant,
      other.state_ == network_connection_state::open && !other.read_in_flight_
        && other.input_operations_.get_count() == 0
        && other.output_operations_.get_count() == 0
        && other.admission_.pending_writes() == 0,
      "network connection moved after first use");
    return other.owner_;
}

connection::connection(connection&& other) noexcept
  : owner_(prepare_move(other))
  , native_(std::move(other.native_))
  , input_(std::move(other.input_))
  , output_(std::move(other.output_))
  , local_(other.local_)
  , remote_(other.remote_)
  , limits_(other.limits_)
  , admission_(std::move(other.admission_))
  , input_operations_(std::move(other.input_operations_))
  , output_operations_(std::move(other.output_operations_))
  , close_done_(std::move(other.close_done_))
  , unflushed_bytes_(other.unflushed_bytes_)
  , state_(other.state_)
  , input_state_(other.input_state_)
  , output_state_(other.output_state_)
  , read_in_flight_(other.read_in_flight_)
  , abort_requested_(other.abort_requested_) {
    other.state_ = network_connection_state::closed;
    other.input_state_ = network_half_state::shut_down;
    other.output_state_ = network_half_state::shut_down;
    other.moved_from_ = true;
}

connection::~connection() {
    owner_.assert_current();
    KWAQUE_INVARIANT(
      connection_closed_invariant,
      moved_from_
        || (state_ == network_connection_state::closed
            && input_operations_.get_count() == 0
            && output_operations_.get_count() == 0),
      "network connection destroyed before close completed");
}

std::optional<operation_error> connection::input_rejection() const {
    if (moved_from_ || state_ != network_connection_state::open) {
        return network_error(errc::closed);
    }
    if (abort_requested_) {
        return network_error(errc::aborted);
    }
    if (input_state_ != network_half_state::open) {
        return network_error(errc::closed);
    }
    return std::nullopt;
}

std::optional<operation_error> connection::output_rejection() const {
    if (moved_from_ || state_ != network_connection_state::open) {
        return network_error(errc::closed);
    }
    if (abort_requested_) {
        return network_error(errc::aborted);
    }
    if (output_state_ != network_half_state::open) {
        return network_error(errc::closed);
    }
    return std::nullopt;
}

seastar::future<result<network_read_result>> connection::read(
  byte_count maximum_bytes, seastar::abort_source& caller_abort) {
    owner_.assert_current();
    if (auto valid = validate_network_read_limit(maximum_bytes); !valid) {
        result<network_read_result> outcome = failure(valid.error());
        return seastar::make_ready_future<result<network_read_result>>(
          std::move(outcome));
    }
    if (caller_abort.abort_requested()) {
        result<network_read_result> outcome = failure(
          network_error(errc::aborted));
        return seastar::make_ready_future<result<network_read_result>>(
          std::move(outcome));
    }
    if (auto rejected = input_rejection()) {
        result<network_read_result> outcome = failure(std::move(*rejected));
        return seastar::make_ready_future<result<network_read_result>>(
          std::move(outcome));
    }
    if (read_in_flight_) {
        result<network_read_result> outcome = failure(
          network_error(errc::unavailable));
        return seastar::make_ready_future<result<network_read_result>>(
          std::move(outcome));
    }
    auto holder = input_operations_.try_hold();
    KWAQUE_INVARIANT(
      connection_gate_invariant,
      holder.has_value(),
      "open connection rejected input gate entry");
    read_in_flight_ = true;
    const auto physical_limit = static_cast<std::size_t>(
      std::min<std::uint64_t>(
        maximum_bytes.value(), maximum_contiguous_allocation_bytes));
    return input_.read_up_to(physical_limit)
      .then_wrapped(
        [this, holder = std::move(*holder), maximum_bytes](
          seastar::future<seastar::temporary_buffer<char>> completed) mutable
          -> result<network_read_result> {
            static_cast<void>(holder);
            auto reset = seastar::defer([this] { read_in_flight_ = false; });
            try {
                auto native = completed.get();
                if (abort_requested_) {
                    return failure(network_error(errc::aborted));
                }
                const bool eof = native.empty() && input_.eof();
                const auto retained
                  = native.empty()
                      ? byte_count{}
                      : byte_count{maximum_contiguous_allocation_bytes};
                auto data
                  = kwaque::runtime::detail::fragmented_buffer_io_access::adopt(
                    std::move(native), retained);
                return network_read_result::make(
                  std::move(data), eof, maximum_bytes);
            } catch (const std::bad_alloc&) {
                throw;
            } catch (...) {
                if (abort_requested_) {
                    return failure(network_error(errc::aborted));
                }
                return failure(
                  network_error_from_exception(std::current_exception()));
            }
        });
}

seastar::future<result<void>> connection::write(
  bytes::fragmented_buffer data, seastar::abort_source& caller_abort) {
    owner_.assert_current();
    if (auto valid = validate_network_write(data, limits_); !valid) {
        result<void> outcome = failure(valid.error());
        return seastar::make_ready_future<result<void>>(std::move(outcome));
    }
    if (caller_abort.abort_requested()) {
        result<void> outcome = failure(network_error(errc::aborted));
        return seastar::make_ready_future<result<void>>(std::move(outcome));
    }
    if (auto rejected = output_rejection()) {
        result<void> outcome = failure(std::move(*rejected));
        return seastar::make_ready_future<result<void>>(std::move(outcome));
    }
    auto reservation = admission_.try_acquire(data.retained_bytes());
    if (!reservation) {
        result<void> outcome = failure(network_error(errc::queue_full));
        return seastar::make_ready_future<result<void>>(std::move(outcome));
    }
    auto holder = output_operations_.try_hold();
    KWAQUE_INVARIANT(
      connection_gate_invariant,
      holder.has_value(),
      "open connection rejected output gate entry");

    auto serialization = seastar::try_get_units(write_serializer_, 1);
    if (serialization) [[likely]] {
        if (data.fragment_count() == 1) [[likely]] {
            const auto bytes = data.size().value();
            auto consumer
              = kwaque::runtime::detail::fragmented_buffer_io_access::consume(
                data);
            auto fragment = consumer.take_front();
            return output_.write(std::move(fragment))
              .then_wrapped(
                [this,
                 bytes,
                 reservation = std::move(*reservation),
                 holder = std::move(*holder),
                 serialization = std::move(*serialization)](
                  seastar::future<> completion) mutable
                  -> seastar::future<result<void>> {
                    try {
                        completion.get();
                        if (abort_requested_) {
                            result<void> outcome = failure(
                              network_error(errc::aborted));
                            return seastar::make_ready_future<result<void>>(
                              std::move(outcome));
                        }
                    } catch (const std::bad_alloc&) {
                        return seastar::current_exception_as_future<
                          result<void>>();
                    } catch (...) {
                        result<void> outcome = failure(
                          abort_requested_ ? network_error(errc::aborted)
                                           : network_error_from_exception(
                                               std::current_exception()));
                        return seastar::make_ready_future<result<void>>(
                          std::move(outcome));
                    }

                    unflushed_bytes_ += bytes;
                    if (
                      write_serializer_.waiters() != 0
                      && unflushed_bytes_ < maximum_unflushed_bytes) {
                        result<void> outcome;
                        return seastar::make_ready_future<result<void>>(
                          std::move(outcome));
                    }

                    return output_.flush().then_wrapped(
                      [this,
                       reservation = std::move(reservation),
                       holder = std::move(holder),
                       serialization = std::move(serialization)](
                        seastar::future<> flushed) mutable -> result<void> {
                          static_cast<void>(reservation);
                          static_cast<void>(holder);
                          static_cast<void>(serialization);
                          try {
                              flushed.get();
                              unflushed_bytes_ = 0;
                              if (abort_requested_) {
                                  return failure(network_error(errc::aborted));
                              }
                              return {};
                          } catch (const std::bad_alloc&) {
                              throw;
                          } catch (...) {
                              return failure(
                                abort_requested_ ? network_error(errc::aborted)
                                                 : network_error_from_exception(
                                                     std::current_exception()));
                          }
                      });
                });
        }
        return write_acquired(
          std::move(data),
          std::move(*reservation),
          std::move(*holder),
          std::move(*serialization));
    }
    return write_general(
      std::move(data),
      caller_abort,
      std::move(*reservation),
      std::move(*holder));
}

seastar::future<result<void>> connection::write_acquired(
  bytes::fragmented_buffer data,
  network_write_admission::reservation reservation,
  seastar::gate::holder holder,
  seastar::semaphore_units<> serialization) {
    static_cast<void>(reservation);
    static_cast<void>(holder);
    static_cast<void>(serialization);
    try {
        if (auto rejected = output_rejection()) {
            co_return failure(std::move(*rejected));
        }
        const auto bytes = data.size().value();
        auto consumer
          = kwaque::runtime::detail::fragmented_buffer_io_access::consume(data);
        while (auto fragment = consumer.take_front()) {
            co_await output_.write(std::move(fragment));
        }
        unflushed_bytes_ += bytes;
        if (
          write_serializer_.waiters() == 0
          || unflushed_bytes_ >= maximum_unflushed_bytes) {
            co_await output_.flush();
            unflushed_bytes_ = 0;
        }
        if (abort_requested_) {
            co_return failure(network_error(errc::aborted));
        }
        co_return result<void>{};
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        if (abort_requested_) {
            co_return failure(network_error(errc::aborted));
        }
        co_return failure(
          network_error_from_exception(std::current_exception()));
    }
}

seastar::future<result<void>> connection::write_general(
  bytes::fragmented_buffer data,
  seastar::abort_source& caller_abort,
  network_write_admission::reservation reservation,
  seastar::gate::holder holder) {
    static_cast<void>(reservation);
    static_cast<void>(holder);
    try {
        auto serialization
          = co_await seastar::coroutine::without_preemption_check(
            seastar::get_units(write_serializer_, 1, caller_abort));
        if (caller_abort.abort_requested()) {
            co_return co_await flush_preceding_batch_after_cancellation();
        }
        if (auto rejected = output_rejection()) {
            co_return failure(std::move(*rejected));
        }

        const auto bytes = data.size().value();
        auto consumer
          = kwaque::runtime::detail::fragmented_buffer_io_access::consume(data);
        while (auto fragment = consumer.take_front()) {
            co_await output_.write(std::move(fragment));
        }
        unflushed_bytes_ += bytes;
        if (
          write_serializer_.waiters() == 0
          || unflushed_bytes_ >= maximum_unflushed_bytes) {
            co_await output_.flush();
            unflushed_bytes_ = 0;
        }
        if (abort_requested_) {
            co_return failure(network_error(errc::aborted));
        }
        co_return result<void>{};
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        if (abort_requested_ || caller_abort.abort_requested()) {
            co_return failure(network_error(errc::aborted));
        }
        co_return failure(
          network_error_from_exception(std::current_exception()));
    }
}

seastar::future<result<void>>
connection::flush_preceding_batch_after_cancellation() {
    try {
        if (
          unflushed_bytes_ != 0 && write_serializer_.waiters() == 0
          && !abort_requested_) {
            co_await output_.flush();
            unflushed_bytes_ = 0;
        }
        co_return failure(network_error(errc::aborted));
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        if (abort_requested_) {
            co_return failure(network_error(errc::aborted));
        }
        co_return failure(
          network_error_from_exception(std::current_exception()));
    }
}

network_endpoint connection::local_endpoint() const noexcept {
    owner_.assert_current();
    return local_;
}

network_endpoint connection::remote_endpoint() const noexcept {
    owner_.assert_current();
    return remote_;
}

network_connection_state connection::state() const noexcept {
    owner_.assert_current();
    return state_;
}

network_half_state connection::input_state() const noexcept {
    owner_.assert_current();
    return input_state_;
}

network_half_state connection::output_state() const noexcept {
    owner_.assert_current();
    return output_state_;
}

const network_connection_limits& connection::limits() const noexcept {
    owner_.assert_current();
    return limits_;
}

result<void> connection::shutdown_input() {
    owner_.assert_current();
    if (auto rejected = input_rejection()) {
        return failure(std::move(*rejected));
    }
    try {
        input_state_ = network_half_state::shut_down;
        native_.shutdown_input();
        return {};
    } catch (...) {
        return failure(network_error_from_exception(std::current_exception()));
    }
}

result<void> connection::shutdown_output() {
    owner_.assert_current();
    if (auto rejected = output_rejection()) {
        return failure(std::move(*rejected));
    }
    try {
        output_state_ = network_half_state::shut_down;
        native_.shutdown_output();
        return {};
    } catch (...) {
        return failure(network_error_from_exception(std::current_exception()));
    }
}

void connection::request_abort() {
    owner_.assert_current();
    if (
      moved_from_ || state_ == network_connection_state::closed
      || abort_requested_) {
        return;
    }
    abort_requested_ = true;
    input_state_ = network_half_state::shut_down;
    output_state_ = network_half_state::shut_down;
    try {
        native_.shutdown_input();
    } catch (...) {
    }
    try {
        native_.shutdown_output();
    } catch (...) {
    }
}

seastar::future<result<void>> connection::close() {
    owner_.assert_current();
    if (moved_from_) {
        return seastar::make_ready_future<result<void>>(result<void>{});
    }
    if (state_ == network_connection_state::closing) {
        return close_done_->get_shared_future();
    }
    if (state_ == network_connection_state::closed) {
        return close_done_ && close_done_->available()
                 ? close_done_->get_shared_future()
                 : seastar::make_ready_future<result<void>>(result<void>{});
    }
    try {
        close_done_.emplace();
    } catch (...) {
        return seastar::current_exception_as_future<result<void>>();
    }
    state_ = network_connection_state::closing;
    request_abort();
    auto completion = close_once().then_wrapped(
      [this](seastar::future<result<void>> closed) {
          state_ = network_connection_state::closed;
          try {
              close_done_->set_value(closed.get());
          } catch (...) {
              close_done_->set_exception(std::current_exception());
          }
      });
    static_cast<void>(completion);
    return close_done_->get_shared_future();
}

seastar::future<result<void>> connection::close_once() {
    co_await input_operations_.close();
    co_await output_operations_.close();
    auto release_native = seastar::defer(
      [this] { native_ = seastar::connected_socket{}; });
    static_cast<void>(release_native);
    std::optional<operation_error> first_error;
    try {
        auto serialization
          = co_await seastar::coroutine::without_preemption_check(
            seastar::get_units(write_serializer_, 1));
        try {
            co_await output_.close();
        } catch (const std::bad_alloc&) {
            throw;
        } catch (...) {
            first_error = network_error_from_exception(
              std::current_exception());
        }
        try {
            co_await input_.close();
        } catch (const std::bad_alloc&) {
            throw;
        } catch (...) {
            if (!first_error) {
                first_error = network_error_from_exception(
                  std::current_exception());
            }
        }
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        first_error = network_error_from_exception(std::current_exception());
    }
    if (first_error) {
        co_return failure(std::move(*first_error));
    }
    co_return result<void>{};
}

listener::listener(
  seastar::server_socket native,
  network_endpoint local,
  network_connection_limits limits)
  : native_(std::move(native))
  , local_(local)
  , limits_(limits) {}

owner_shard listener::prepare_move(listener& other) noexcept {
    other.owner_.assert_current();
    KWAQUE_INVARIANT(
      listener_move_invariant,
      !other.aborted_ && !other.closing_ && !other.closed_
        && !other.accept_in_flight_ && other.accepts_.get_count() == 0,
      "network listener moved after first use");
    return other.owner_;
}

listener::listener(listener&& other) noexcept
  : owner_(prepare_move(other))
  , native_(std::move(other.native_))
  , local_(other.local_)
  , limits_(other.limits_)
  , accepts_(std::move(other.accepts_))
  , close_done_(std::move(other.close_done_))
  , aborted_(other.aborted_)
  , closing_(other.closing_)
  , closed_(other.closed_)
  , accept_in_flight_(other.accept_in_flight_) {
    other.aborted_ = true;
    other.closed_ = true;
    other.moved_from_ = true;
}

listener::~listener() {
    owner_.assert_current();
    KWAQUE_INVARIANT(
      listener_closed_invariant,
      moved_from_
        || (closed_ && !accept_in_flight_ && accepts_.get_count() == 0),
      "network listener destroyed before close completed");
}

seastar::future<result<connection>>
listener::accept(seastar::abort_source& caller_abort) {
    owner_.assert_current();
    if (caller_abort.abort_requested() || aborted_) {
        co_return failure(network_error(errc::aborted));
    }
    if (closing_ || closed_) {
        co_return failure(network_error(errc::closed));
    }
    if (accept_in_flight_) {
        co_return failure(network_error(errc::unavailable));
    }
    auto holder = accepts_.try_hold();
    KWAQUE_INVARIANT(
      connection_gate_invariant,
      holder.has_value(),
      "open listener rejected accept gate entry");
    accept_in_flight_ = true;
    auto reset_accept = seastar::defer([this] { accept_in_flight_ = false; });
    try {
        auto accepted = co_await native_.accept();
        if (caller_abort.abort_requested()) {
            accepted.connection.shutdown_input();
            accepted.connection.shutdown_output();
            co_return failure(network_error(errc::aborted));
        }
        const auto local = kwaque_endpoint(accepted.connection.local_address());
        const auto remote = kwaque_endpoint(accepted.remote_address);
        co_return connection{
          std::move(accepted.connection), local, remote, limits_};
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        if (aborted_ || caller_abort.abort_requested()) {
            co_return failure(network_error(errc::aborted));
        }
        co_return failure(
          network_error_from_exception(std::current_exception()));
    }
}

network_endpoint listener::local_endpoint() const noexcept {
    owner_.assert_current();
    return local_;
}

const network_connection_limits& listener::connection_limits() const noexcept {
    owner_.assert_current();
    return limits_;
}

void listener::request_abort() {
    owner_.assert_current();
    if (moved_from_ || aborted_) {
        return;
    }
    aborted_ = true;
    native_.abort_accept();
}

seastar::future<result<void>> listener::close() {
    owner_.assert_current();
    if (moved_from_ || closed_) {
        return close_done_ && close_done_->available()
                 ? close_done_->get_shared_future()
                 : seastar::make_ready_future<result<void>>(result<void>{});
    }
    if (closing_) {
        return close_done_->get_shared_future();
    }
    try {
        close_done_.emplace();
    } catch (...) {
        return seastar::current_exception_as_future<result<void>>();
    }
    closing_ = true;
    request_abort();
    auto completion = accepts_.close().then_wrapped(
      [this](seastar::future<> drained) {
          auto release_native = seastar::defer(
            [this] { native_ = seastar::server_socket{}; });
          static_cast<void>(release_native);
          try {
              drained.get();
              closed_ = true;
              closing_ = false;
              close_done_->set_value(result<void>{});
          } catch (...) {
              closed_ = true;
              closing_ = false;
              close_done_->set_exception(std::current_exception());
          }
      });
    static_cast<void>(completion);
    return close_done_->get_shared_future();
}

seastar::future<result<connection>> network::connect(
  network_endpoint endpoint,
  std::optional<network_endpoint> local_endpoint,
  network_connection_limits limits,
  seastar::abort_source& caller_abort) {
    assert_current();
    if (auto valid = limits.validate(); !valid) {
        co_return failure(valid.error());
    }
    if (caller_abort.abort_requested()) {
        co_return failure(network_error(errc::aborted));
    }
    auto socket = seastar::engine().net().socket();
    connect_detail::connect_abort_guard subscription{socket, caller_abort};
    if (!subscription.armed()) {
        co_return failure(network_error(errc::aborted));
    }
    try {
        auto native = co_await socket.connect(
          native_endpoint(endpoint),
          local_endpoint ? native_endpoint(*local_endpoint)
                         : seastar::socket_address{});
        if (caller_abort.abort_requested()) {
            native.shutdown_input();
            native.shutdown_output();
            co_return failure(network_error(errc::aborted));
        }
        const auto local = kwaque_endpoint(native.local_address());
        const auto remote = kwaque_endpoint(native.remote_address());
        co_return connection{std::move(native), local, remote, limits};
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        if (caller_abort.abort_requested()) {
            co_return failure(network_error(errc::aborted));
        }
        co_return failure(
          network_error_from_exception(std::current_exception()));
    }
}

seastar::future<result<listener>>
network::listen(network_endpoint endpoint, network_listen_options options) {
    assert_current();
    if (auto valid = options.validate(); !valid) {
        co_return failure(valid.error());
    }
    try {
        seastar::listen_options native_options;
        native_options.set_fixed_cpu(seastar::this_shard_id());
        native_options.reuse_address = options.reuse_address;
        native_options.listen_backlog = static_cast<int>(options.backlog);
        if (options.receive_buffer_bytes.value() != 0) {
            native_options.so_rcvbuf = static_cast<int>(
              options.receive_buffer_bytes.value());
        }
        if (options.send_buffer_bytes.value() != 0) {
            native_options.so_sndbuf = static_cast<int>(
              options.send_buffer_bytes.value());
        }
        auto native = seastar::listen(
          native_endpoint(endpoint), native_options);
        const auto local = kwaque_endpoint(native.local_address());
        co_return listener{std::move(native), local, options.connection_limits};
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        co_return failure(
          network_error_from_exception(std::current_exception()));
    }
}

} // namespace kwaque::runtime::production
