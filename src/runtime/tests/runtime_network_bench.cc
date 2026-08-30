#include "src/bytes/fragmented_buffer.h"
#include "src/runtime/fragmented_buffer_internal.h"
#include "src/runtime/network.h"
#include "src/runtime/production/network.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/when_all.hh>
#include <seastar/net/api.hh>
#include <seastar/testing/perf_tests.hh>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace kwaque::runtime {

namespace {

constexpr std::size_t network_benchmark_bytes = 4096;
constexpr std::uint64_t network_benchmark_max_unflushed_bytes = 1024U * 1024U;
const std::string network_benchmark_payload(network_benchmark_bytes, 'n');
constexpr std::size_t concurrent_writer_count = 8;
constexpr std::size_t concurrent_write_bytes = 64U * 1024U;
constexpr std::size_t concurrent_total_bytes = concurrent_writer_count
                                               * concurrent_write_bytes;
const std::string concurrent_benchmark_payload(concurrent_total_bytes, 'q');

seastar::socket_address native_loopback(std::uint16_t port = 0) {
    return seastar::make_ipv4_address({0x7f000001U, port});
}

network_endpoint kwaque_loopback(std::uint16_t port = 0) {
    return network_endpoint{
      network_address::ipv4(
        {std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}}),
      port};
}

seastar::future<result<void>> read_kwaque_exactly(
  production::connection& connection,
  seastar::abort_source& abort_source,
  std::string_view expected) {
    std::size_t offset = 0;
    while (offset < expected.size()) {
        auto received = co_await connection.read(
          byte_count{expected.size() - offset}, abort_source);
        if (
          !received || received->eof() || received->data().empty()
          || !received->data().content_equals(
            std::string_view{
              expected.data() + offset,
              static_cast<std::size_t>(received->data().size().value())})) {
            co_return failure(
              operation_error{errc::network_failure, operation_kind::network});
        }
        offset += static_cast<std::size_t>(received->data().size().value());
    }
    co_return result<void>{};
}

seastar::future<result<network_read_result>> read_native_up_to(
  seastar::input_stream<char>& input,
  seastar::gate& operations,
  byte_count maximum_bytes,
  seastar::abort_source& abort_source) {
    if (abort_source.abort_requested()) {
        result<network_read_result> outcome = failure(
          operation_error{errc::aborted, operation_kind::network});
        return seastar::make_ready_future<result<network_read_result>>(
          std::move(outcome));
    }
    auto holder = operations.hold();
    return input.read_up_to(static_cast<std::size_t>(maximum_bytes.value()))
      .then_wrapped(
        [&input, holder = std::move(holder), maximum_bytes](
          seastar::future<seastar::temporary_buffer<char>> completed) mutable
          -> result<network_read_result> {
            static_cast<void>(holder);
            try {
                auto native = completed.get();
                const bool eof = native.empty() && input.eof();
                auto data = detail::fragmented_buffer_io_access::adopt(
                  std::move(native));
                return network_read_result::make(
                  std::move(data), eof, maximum_bytes);
            } catch (const std::bad_alloc&) {
                throw;
            } catch (...) {
                return failure(
                  operation_error{
                    errc::network_failure, operation_kind::network});
            }
        });
}

seastar::future<result<void>> read_native_exactly(
  seastar::input_stream<char>& input,
  seastar::gate& operations,
  seastar::abort_source& abort_source,
  std::string_view expected) {
    std::size_t offset = 0;
    while (offset < expected.size()) {
        auto received = co_await read_native_up_to(
          input,
          operations,
          byte_count{expected.size() - offset},
          abort_source);
        if (
          !received || received->eof() || received->data().empty()
          || !received->data().content_equals(
            std::string_view{
              expected.data() + offset,
              static_cast<std::size_t>(received->data().size().value())})) {
            co_return failure(
              operation_error{errc::network_failure, operation_kind::network});
        }
        offset += static_cast<std::size_t>(received->data().size().value());
    }
    co_return result<void>{};
}

template<std::size_t FragmentCount>
bytes::fragmented_buffer make_payload(
  std::size_t total_bytes = network_benchmark_bytes, char value = 'n') {
    static_assert(FragmentCount > 0);
    if (total_bytes % FragmentCount != 0) {
        std::terminate();
    }
    const auto fragment_bytes = total_bytes / FragmentCount;
    std::array<seastar::temporary_buffer<char>, FragmentCount> fragments;
    for (auto& fragment : fragments) {
        fragment = seastar::temporary_buffer<char>(fragment_bytes);
        std::memset(fragment.get_write(), value, fragment.size());
    }
    auto payload = bytes::fragmented_buffer::copy_from_fragments(
      std::span<const seastar::temporary_buffer<char>>{fragments});
    if (!payload) {
        std::terminate();
    }
    return std::move(*payload);
}

class native_raw_network_fixture {
public:
    native_raw_network_fixture()
      : listener_(seastar::listen(native_loopback()))
      , source_(network_benchmark_bytes) {
        std::memset(source_.get_write(), 'n', source_.size());
        auto accepting = listener_.accept();
        client_ = seastar::connect(listener_.local_address()).get();
        server_ = std::move(accepting).get().connection;
        input_.emplace(server_.input());
        output_.emplace(client_.output());
    }

    ~native_raw_network_fixture() {
        output_->close().get();
        input_->close().get();
        listener_.abort_accept();
    }

    [[gnu::noinline]] seastar::future<> execute() {
        auto reading = input_->read_exactly(network_benchmark_bytes);
        return output_->write(source_.share())
          .then([this] { return output_->flush(); })
          .then([reading = std::move(reading)] mutable {
              return std::move(reading);
          })
          .then([](seastar::temporary_buffer<char> received) {
              if (
                received.size() != network_benchmark_bytes
                || received.get()[0] != 'n') {
                  std::terminate();
              }
          });
    }

private:
    seastar::server_socket listener_;
    seastar::connected_socket client_;
    seastar::connected_socket server_;
    std::optional<seastar::input_stream<char>> input_;
    std::optional<seastar::output_stream<char>> output_;
    seastar::temporary_buffer<char> source_;
};

template<std::size_t FragmentCount>
class native_batched_network_fixture {
public:
    native_batched_network_fixture()
      : listener_(seastar::listen(native_loopback()))
      , source_(make_payload<FragmentCount>()) {
        auto accepting = listener_.accept();
        client_ = seastar::connect(listener_.local_address()).get();
        server_ = std::move(accepting).get().connection;
        input_.emplace(server_.input());
        output_.emplace(client_.output());
    }

    ~native_batched_network_fixture() {
        operations_.close().get();
        input_operations_.close().get();
        auto serialization = seastar::get_units(serializer_, 1).get();
        output_->close().get();
        input_->close().get();
        listener_.abort_accept();
    }

    [[gnu::noinline]] seastar::future<> execute() {
        auto data = source_.share();
        owner_.assert_current();
        if (
          !validate_network_write(data, limits_)
          || write_abort_.abort_requested() || abort_requested_
          || state_ != network_connection_state::open
          || output_state_ != network_half_state::open) {
            std::terminate();
        }
        auto operation_reservation = seastar::try_get_units(
          operation_units_, 1);
        auto byte_reservation = seastar::try_get_units(
          byte_units_, data.retained_bytes().value());
        auto serialization = seastar::try_get_units(serializer_, 1);
        if (!operation_reservation || !byte_reservation || !serialization) {
            std::terminate();
        }
        auto holder = operations_.hold();
        auto reading = read_native_exactly(
          *input_, input_operations_, read_abort_, network_benchmark_payload);
        return write(
                 std::move(data),
                 std::move(*operation_reservation),
                 std::move(*byte_reservation),
                 std::move(*serialization),
                 std::move(holder))
          .then([reading = std::move(reading)](result<void> written) mutable {
              if (!written) {
                  std::terminate();
              }
              return std::move(reading);
          })
          .then([](result<void> received) {
              if (!received) {
                  std::terminate();
              }
          });
    }

private:
    seastar::future<result<void>> write(
      bytes::fragmented_buffer data,
      seastar::semaphore_units<> operation_reservation,
      seastar::semaphore_units<> byte_reservation,
      seastar::semaphore_units<> serialization,
      seastar::gate::holder holder) {
        static_cast<void>(operation_reservation);
        static_cast<void>(byte_reservation);
        static_cast<void>(serialization);
        static_cast<void>(holder);
        try {
            if (
              abort_requested_ || state_ != network_connection_state::open
              || output_state_ != network_half_state::open) {
                co_return failure(
                  operation_error{errc::aborted, operation_kind::network});
            }
            const auto bytes = data.size().value();
            auto consumer = detail::fragmented_buffer_io_access::consume(data);
            while (auto fragment = consumer.take_front()) {
                co_await output_->write(std::move(fragment));
            }
            unflushed_bytes_ += bytes;
            if (
              serializer_.waiters() == 0
              || unflushed_bytes_ >= network_benchmark_max_unflushed_bytes) {
                co_await output_->flush();
                unflushed_bytes_ = 0;
            }
            co_return result<void>{};
        } catch (const std::bad_alloc&) {
            throw;
        } catch (...) {
            co_return failure(
              operation_error{errc::network_failure, operation_kind::network});
        }
    }

    seastar::server_socket listener_;
    seastar::connected_socket client_;
    seastar::connected_socket server_;
    std::optional<seastar::input_stream<char>> input_;
    std::optional<seastar::output_stream<char>> output_;
    network_connection_limits limits_;
    seastar::semaphore operation_units_{limits_.pending_writes};
    seastar::semaphore byte_units_{limits_.pending_write_bytes.value()};
    seastar::semaphore serializer_{1};
    seastar::gate operations_;
    seastar::gate input_operations_;
    bytes::fragmented_buffer source_;
    owner_shard owner_;
    seastar::abort_source read_abort_;
    seastar::abort_source write_abort_;
    network_connection_state state_{network_connection_state::open};
    network_half_state output_state_{network_half_state::open};
    std::uint64_t unflushed_bytes_{0};
    bool abort_requested_{false};
};

template<std::size_t FragmentCount>
class kwaque_network_fixture {
public:
    kwaque_network_fixture()
      : source_(make_payload<FragmentCount>()) {
        auto listening = backend_.listen(kwaque_loopback(), {}).get();
        if (!listening) {
            std::terminate();
        }
        listener_.emplace(std::move(*listening));
        auto accepting = listener_->accept(accept_abort_);
        auto connected = backend_
                           .connect(
                             listener_->local_endpoint(),
                             std::nullopt,
                             network_connection_limits{},
                             connect_abort_)
                           .get();
        auto accepted = std::move(accepting).get();
        if (!connected || !accepted) {
            std::terminate();
        }
        client_.emplace(std::move(*connected));
        server_.emplace(std::move(*accepted));
    }

    ~kwaque_network_fixture() {
        const auto client_closed = client_->close().get();
        const auto server_closed = server_->close().get();
        const auto listener_closed = listener_->close().get();
        if (!client_closed || !server_closed || !listener_closed) {
            std::terminate();
        }
    }

    [[gnu::noinline]] seastar::future<> execute() {
        auto reading = read_kwaque_exactly(
          *server_, read_abort_, network_benchmark_payload);
        return client_->write(source_.share(), write_abort_)
          .then([reading = std::move(reading)](result<void> written) mutable {
              if (!written) {
                  std::terminate();
              }
              return std::move(reading);
          })
          .then([](result<void> received) {
              if (!received) {
                  std::terminate();
              }
          });
    }

private:
    production::network backend_;
    std::optional<production::listener> listener_;
    std::optional<production::connection> client_;
    std::optional<production::connection> server_;
    bytes::fragmented_buffer source_;
    seastar::abort_source accept_abort_;
    seastar::abort_source connect_abort_;
    seastar::abort_source read_abort_;
    seastar::abort_source write_abort_;
};

class native_concurrent_network_fixture {
public:
    native_concurrent_network_fixture()
      : listener_(make_listener())
      , source_(make_payload<1>(concurrent_write_bytes, 'q')) {
        auto accepting = listener_.accept();
        client_ = seastar::connect(listener_.local_address()).get();
        server_ = std::move(accepting).get().connection;
        input_.emplace(client_.input());
        output_.emplace(server_.output());
    }

    ~native_concurrent_network_fixture() {
        operations_.close().get();
        input_operations_.close().get();
        auto serialization = seastar::get_units(serializer_, 1).get();
        output_->close().get();
        input_->close().get();
        listener_.abort_accept();
    }

    [[gnu::noinline]] seastar::future<> execute() {
        auto reading = read_native_exactly(
          *input_,
          input_operations_,
          read_abort_,
          concurrent_benchmark_payload);
        std::vector<seastar::future<result<void>>> writers;
        writers.reserve(concurrent_writer_count);
        for (std::size_t index = 0; index < concurrent_writer_count; ++index) {
            auto data = source_.share();
            owner_.assert_current();
            if (
              !validate_network_write(data, limits_)
              || write_abort_.abort_requested() || abort_requested_
              || state_ != network_connection_state::open
              || output_state_ != network_half_state::open) {
                std::terminate();
            }
            auto operation = seastar::try_get_units(operation_units_, 1);
            auto byte_reservation = seastar::try_get_units(
              byte_units_, data.retained_bytes().value());
            if (!operation || !byte_reservation) {
                std::terminate();
            }
            writers.push_back(write_one(
              std::move(data),
              std::move(*operation),
              std::move(*byte_reservation),
              operations_.hold()));
        }
        return seastar::when_all_succeed(std::move(writers))
          .then([reading = std::move(reading)](
                  std::vector<result<void>> outcomes) mutable {
              for (const auto& outcome : outcomes) {
                  if (!outcome) {
                      std::terminate();
                  }
              }
              return std::move(reading);
          })
          .then([](result<void> received) {
              if (!received) {
                  std::terminate();
              }
          });
    }

private:
    static seastar::server_socket make_listener() {
        seastar::listen_options options;
        options.so_sndbuf = 4096;
        return seastar::listen(native_loopback(), options);
    }

    seastar::future<result<void>> write_one(
      bytes::fragmented_buffer data,
      seastar::semaphore_units<> operation_reservation,
      seastar::semaphore_units<> byte_reservation,
      seastar::gate::holder holder) {
        static_cast<void>(operation_reservation);
        static_cast<void>(byte_reservation);
        static_cast<void>(holder);
        try {
            auto serialization = co_await seastar::get_units(
              serializer_, 1, write_abort_);
            if (
              write_abort_.abort_requested() || abort_requested_
              || state_ != network_connection_state::open
              || output_state_ != network_half_state::open) {
                co_return failure(
                  operation_error{errc::aborted, operation_kind::network});
            }
            const auto bytes = data.size().value();
            auto consumer = detail::fragmented_buffer_io_access::consume(data);
            while (auto fragment = consumer.take_front()) {
                co_await output_->write(std::move(fragment));
            }
            unflushed_bytes_ += bytes;
            if (
              serializer_.waiters() == 0
              || unflushed_bytes_ >= network_benchmark_max_unflushed_bytes) {
                co_await output_->flush();
                unflushed_bytes_ = 0;
            }
            co_return result<void>{};
        } catch (const std::bad_alloc&) {
            throw;
        } catch (...) {
            co_return failure(
              operation_error{errc::network_failure, operation_kind::network});
        }
    }

    seastar::server_socket listener_;
    seastar::connected_socket client_;
    seastar::connected_socket server_;
    std::optional<seastar::input_stream<char>> input_;
    std::optional<seastar::output_stream<char>> output_;
    network_connection_limits limits_;
    seastar::semaphore operation_units_{limits_.pending_writes};
    seastar::semaphore byte_units_{limits_.pending_write_bytes.value()};
    seastar::semaphore serializer_{1};
    seastar::gate operations_;
    seastar::gate input_operations_;
    bytes::fragmented_buffer source_;
    owner_shard owner_;
    seastar::abort_source read_abort_;
    seastar::abort_source write_abort_;
    network_connection_state state_{network_connection_state::open};
    network_half_state output_state_{network_half_state::open};
    std::uint64_t unflushed_bytes_{0};
    bool abort_requested_{false};
};

class kwaque_concurrent_network_fixture {
public:
    kwaque_concurrent_network_fixture()
      : source_(make_payload<1>(concurrent_write_bytes, 'q')) {
        auto listening = backend_
                           .listen(
                             kwaque_loopback(),
                             {.send_buffer_bytes = byte_count{4096}})
                           .get();
        if (!listening) {
            std::terminate();
        }
        listener_.emplace(std::move(*listening));
        auto accepting = listener_->accept(accept_abort_);
        auto connected = backend_
                           .connect(
                             listener_->local_endpoint(),
                             std::nullopt,
                             network_connection_limits{},
                             connect_abort_)
                           .get();
        auto accepted = std::move(accepting).get();
        if (!connected || !accepted) {
            std::terminate();
        }
        client_.emplace(std::move(*connected));
        server_.emplace(std::move(*accepted));
    }

    ~kwaque_concurrent_network_fixture() {
        const auto server_closed = server_->close().get();
        const auto client_closed = client_->close().get();
        const auto listener_closed = listener_->close().get();
        if (!server_closed || !client_closed || !listener_closed) {
            std::terminate();
        }
    }

    [[gnu::noinline]] seastar::future<> execute() {
        auto reading = read_kwaque_exactly(
          *client_, read_abort_, concurrent_benchmark_payload);
        std::vector<seastar::future<result<void>>> writers;
        writers.reserve(concurrent_writer_count);
        for (std::size_t index = 0; index < concurrent_writer_count; ++index) {
            writers.push_back(server_->write(source_.share(), write_abort_));
        }
        return seastar::when_all_succeed(std::move(writers))
          .then([reading = std::move(reading)](
                  std::vector<result<void>> outcomes) mutable {
              for (const auto& outcome : outcomes) {
                  if (!outcome) {
                      std::terminate();
                  }
              }
              return std::move(reading);
          })
          .then([](result<void> received) {
              if (!received) {
                  std::terminate();
              }
          });
    }

private:
    production::network backend_;
    std::optional<production::listener> listener_;
    std::optional<production::connection> client_;
    std::optional<production::connection> server_;
    bytes::fragmented_buffer source_;
    seastar::abort_source accept_abort_;
    seastar::abort_source connect_abort_;
    seastar::abort_source read_abort_;
    seastar::abort_source write_abort_;
};

using native_batched_network_1_fixture = native_batched_network_fixture<1>;
using native_batched_network_64_fixture = native_batched_network_fixture<64>;
using kwaque_network_1_fixture = kwaque_network_fixture<1>;
using kwaque_network_64_fixture = kwaque_network_fixture<64>;

} // namespace

PERF_TEST_F(native_raw_network_fixture, write_read_4096) { return execute(); }

PERF_TEST_F(native_batched_network_1_fixture, write_read_4096) {
    return execute();
}

PERF_TEST_F(native_batched_network_64_fixture, write_read_4096) {
    return execute();
}

PERF_TEST_F(kwaque_network_1_fixture, write_read_4096) { return execute(); }

PERF_TEST_F(kwaque_network_64_fixture, write_read_4096) { return execute(); }

PERF_TEST_F(native_concurrent_network_fixture, concurrent_8x65536) {
    return execute();
}

PERF_TEST_F(kwaque_concurrent_network_fixture, concurrent_8x65536) {
    return execute();
}

} // namespace kwaque::runtime
