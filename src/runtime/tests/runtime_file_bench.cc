#include "src/runtime/file.h"
#include "src/runtime/fragmented_buffer_internal.h"

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/io_intent.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/testing/perf_tests.hh>

#include <sys/stat.h>
#include <sys/uio.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <functional>
#include <new>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace kwaque::runtime {

namespace {

constexpr unsigned dma_alignment = 4096;
constexpr std::size_t backing_size = 8192;

struct benchmark_file_state final {
    std::vector<char> storage = std::vector<char>(backing_size, 'b');
    std::uint64_t logical_size{backing_size};
    unsigned closes{0};
};

template<typename T>
[[noreturn]] T unexpected_file_call() {
    std::terminate();
}

class benchmark_file_impl final : public seastar::file_impl {
public:
    explicit benchmark_file_impl(benchmark_file_state& state) noexcept
      : state_(state) {
        _memory_dma_alignment = dma_alignment;
        _disk_read_dma_alignment = dma_alignment;
        _disk_write_dma_alignment = dma_alignment;
        _disk_overwrite_dma_alignment = dma_alignment;
    }

    seastar::future<std::size_t> write_dma(
      std::uint64_t position,
      const void* buffer,
      std::size_t size,
      seastar::io_intent*) final {
        const auto end = position + size;
        if (end > state_.storage.size()) {
            std::terminate();
        }
        std::memcpy(
          state_.storage.data() + static_cast<std::size_t>(position),
          buffer,
          size);
        state_.logical_size = std::max(state_.logical_size, end);
        return seastar::make_ready_future<std::size_t>(size);
    }

    seastar::future<std::size_t>
    write_dma(std::uint64_t, std::vector<iovec>, seastar::io_intent*) final {
        return unexpected_file_call<seastar::future<std::size_t>>();
    }

    seastar::future<std::size_t> read_dma(
      std::uint64_t position,
      void* buffer,
      std::size_t size,
      seastar::io_intent*) final {
        if (position >= state_.logical_size) {
            return seastar::make_ready_future<std::size_t>(0);
        }
        const auto available = state_.logical_size - position;
        const auto read = std::min<std::uint64_t>(available, size);
        std::memcpy(
          buffer,
          state_.storage.data() + static_cast<std::size_t>(position),
          static_cast<std::size_t>(read));
        return seastar::make_ready_future<std::size_t>(
          static_cast<std::size_t>(read));
    }

    seastar::future<std::size_t>
    read_dma(std::uint64_t, std::vector<iovec>, seastar::io_intent*) final {
        return unexpected_file_call<seastar::future<std::size_t>>();
    }

    seastar::future<seastar::temporary_buffer<std::uint8_t>> dma_read_bulk(
      std::uint64_t position, std::size_t size, seastar::io_intent*) final {
        const auto available = position < state_.logical_size
                                 ? state_.logical_size - position
                                 : std::uint64_t{0};
        const auto read = std::min<std::uint64_t>(available, size);
        seastar::temporary_buffer<std::uint8_t> result{
          static_cast<std::size_t>(read)};
        if (read != 0) {
            std::memcpy(
              result.get_write(),
              state_.storage.data() + static_cast<std::size_t>(position),
              static_cast<std::size_t>(read));
        }
        return seastar::make_ready_future<
          seastar::temporary_buffer<std::uint8_t>>(std::move(result));
    }

    seastar::future<> flush() final { return seastar::make_ready_future<>(); }

    seastar::future<struct stat> stat() final {
        struct stat status{};
        status.st_mode = S_IFREG | S_IRUSR | S_IWUSR;
        status.st_size = static_cast<off_t>(state_.logical_size);
        status.st_blksize = dma_alignment;
        return seastar::make_ready_future<struct stat>(status);
    }

    seastar::future<> truncate(std::uint64_t size) final {
        if (size > state_.storage.size()) {
            std::terminate();
        }
        state_.logical_size = size;
        return seastar::make_ready_future<>();
    }

    seastar::future<> discard(std::uint64_t, std::uint64_t) final {
        return unexpected_file_call<seastar::future<>>();
    }

    seastar::future<> allocate(std::uint64_t, std::uint64_t) final {
        return unexpected_file_call<seastar::future<>>();
    }

    seastar::future<std::uint64_t> size() final {
        return seastar::make_ready_future<std::uint64_t>(state_.logical_size);
    }

    seastar::future<> close() final {
        ++state_.closes;
        return seastar::make_ready_future<>();
    }

    seastar::subscription<seastar::directory_entry> list_directory(
      std::function<seastar::future<>(seastar::directory_entry)>) final {
        return unexpected_file_call<
          seastar::subscription<seastar::directory_entry>>();
    }

private:
    benchmark_file_state& state_;
};

seastar::file make_native_file(benchmark_file_state& state) {
    return seastar::file{seastar::make_shared<benchmark_file_impl>(state)};
}

seastar::temporary_buffer<char> make_source(std::size_t size) {
    auto source = seastar::temporary_buffer<char>::aligned(dma_alignment, size);
    std::memset(source.get_write(), 'w', source.size());
    // The first share promotes the raw free-deleter to a reference-counted
    // control block. Drop the transient clone so measured shares avoid setup
    // allocation while the fixture remains the sole current owner.
    static_cast<void>(source.share());
    return source;
}

class kwaque_file_fixture {
public:
    kwaque_file_fixture(
      std::size_t source_size,
      std::size_t source_offset,
      std::size_t write_size,
      std::uint64_t position)
      : owner_(make_native_file(state_))
      , source_(make_source(source_size))
      , source_offset_(source_offset)
      , write_size_(write_size)
      , position_(position) {}

    ~kwaque_file_fixture() {
        const auto outcome = owner_.close().get();
        if (!outcome || state_.closes != 1) {
            std::terminate();
        }
    }

    kwaque_file_fixture(const kwaque_file_fixture&) = delete;
    kwaque_file_fixture& operator=(const kwaque_file_fixture&) = delete;

    [[gnu::noinline]] seastar::future<> execute() {
        auto data = detail::fragmented_buffer_io_access::adopt(
          source_.share(source_offset_, write_size_));
        return owner_.write(file_position{position_}, std::move(data))
          .then([expected = write_size_](result<byte_count> outcome) {
              if (!outcome || outcome->value() != expected) {
                  std::terminate();
              }
          });
    }

private:
    benchmark_file_state state_;
    file owner_;
    seastar::temporary_buffer<char> source_;
    std::size_t source_offset_;
    std::size_t write_size_;
    std::uint64_t position_;
};

struct kwaque_aligned_file_fixture : kwaque_file_fixture {
    kwaque_aligned_file_fixture()
      : kwaque_file_fixture(4096, 0, 4096, 0) {}
};

struct kwaque_staged_file_fixture : kwaque_file_fixture {
    kwaque_staged_file_fixture()
      : kwaque_file_fixture(8192, 1, 4096, 0) {}
};

struct kwaque_rmw_file_fixture : kwaque_file_fixture {
    kwaque_rmw_file_fixture()
      : kwaque_file_fixture(4096, 0, 6, 101) {}
};

class fragmented_write_source final {
public:
    fragmented_write_source() {
        for (std::size_t index = 0; index < fragments_.size(); ++index) {
            fragments_[index] = seastar::temporary_buffer<char>(64);
            std::memset(
              fragments_[index].get_write(),
              static_cast<int>('a' + index % 26),
              fragments_[index].size());
        }
    }

    [[nodiscard]] bytes::fragmented_buffer make() const {
        auto data = bytes::fragmented_buffer::copy_from_fragments(
          std::span<const seastar::temporary_buffer<char>>{fragments_});
        if (!data || data->fragment_count() != fragments_.size()) {
            std::terminate();
        }
        return std::move(*data);
    }

private:
    std::array<seastar::temporary_buffer<char>, 64> fragments_;
};

class kwaque_fragmented_file_fixture {
public:
    kwaque_fragmented_file_fixture()
      : owner_(make_native_file(state_)) {}

    ~kwaque_fragmented_file_fixture() {
        const auto outcome = owner_.close().get();
        if (!outcome || state_.closes != 1) {
            std::terminate();
        }
    }

    [[gnu::noinline]] seastar::future<> execute() {
        return owner_.write(file_position{0}, source_.make())
          .then([](result<byte_count> outcome) {
              if (!outcome || outcome->value() != 4096) {
                  std::terminate();
              }
          });
    }

private:
    benchmark_file_state state_;
    file owner_;
    fragmented_write_source source_;
};

class native_fragmented_file_fixture {
public:
    native_fragmented_file_fixture()
      : native_(make_native_file(state_)) {}

    ~native_fragmented_file_fixture() {
        operations_.close().get();
        auto serialization = seastar::get_units(write_serialization_, 1).get();
        static_cast<void>(serialization);
        native_.close().get();
        if (state_.closes != 1) {
            std::terminate();
        }
    }

    [[gnu::noinline]] seastar::future<> execute() {
        auto data = source_.make();
        owner_.assert_current();
        if (
          owner_state_ != file_state::open || abort_requested_
          || !validate_file_write_request(file_position{0}, data)) {
            std::terminate();
        }
        auto holder = operations_.hold();
        auto serialization = seastar::try_get_units(write_serialization_, 1);
        if (!serialization) {
            std::terminate();
        }
        auto staging = seastar::temporary_buffer<char>::aligned(
          dma_alignment, 4096);
        auto consumer = detail::fragmented_buffer_io_access::consume(data);
        if (
          consumer.copy_front_to(
            std::span<char>{staging.get_write(), staging.size()})
            != staging.size()
          || !data.empty()) {
            std::terminate();
        }

        return native_.dma_write(0, staging.get(), staging.size(), &io_intent_)
          .then_wrapped(
            [data = std::move(data),
             staging = std::move(staging),
             holder = std::move(holder),
             serialization = std::move(*serialization)](
              seastar::future<std::size_t> completed) mutable
              -> result<byte_count> {
                static_cast<void>(data);
                static_cast<void>(staging);
                static_cast<void>(holder);
                static_cast<void>(serialization);
                try {
                    const auto written = completed.get();
                    if (written != 4096) {
                        return failure(
                          operation_error{
                            errc::io_failure, operation_kind::file});
                    }
                    return byte_count{written};
                } catch (const std::bad_alloc&) {
                    throw;
                } catch (...) {
                    return failure(
                      operation_error{errc::io_failure, operation_kind::file});
                }
            })
          .then([](result<byte_count> outcome) {
              if (!outcome || outcome->value() != 4096) {
                  std::terminate();
              }
          });
    }

private:
    benchmark_file_state state_;
    seastar::file native_;
    seastar::io_intent io_intent_;
    seastar::gate operations_;
    seastar::semaphore write_serialization_{1};
    fragmented_write_source source_;
    owner_shard owner_;
    file_state owner_state_{file_state::open};
    bool abort_requested_{false};
};

class native_aligned_file_fixture {
public:
    native_aligned_file_fixture()
      : native_(make_native_file(state_))
      , source_(make_source(4096))
      , memory_alignment_(
          std::max<std::uint64_t>(
            native_.memory_dma_alignment(), sizeof(void*)))
      , append_alignment_(native_.disk_write_dma_alignment())
      , native_limit_(
          std::min<std::uint64_t>(
            native_.disk_write_max_length(), maximum_file_io_bytes.value())
          & ~(append_alignment_ - 1U)) {
        if (
          !std::has_single_bit(memory_alignment_)
          || !std::has_single_bit(append_alignment_)) {
            std::terminate();
        }
        if (native_limit_ == 0) {
            native_limit_ = append_alignment_;
        }
    }

    ~native_aligned_file_fixture() {
        auto serialization = seastar::get_units(write_serialization_, 1).get();
        static_cast<void>(serialization);
        native_.close().get();
        if (state_.closes != 1) {
            std::terminate();
        }
    }

    native_aligned_file_fixture(const native_aligned_file_fixture&) = delete;
    native_aligned_file_fixture&
    operator=(const native_aligned_file_fixture&) = delete;

    [[gnu::noinline]] seastar::future<> execute() {
        auto data = detail::fragmented_buffer_io_access::adopt(
          source_.share(0, write_size_));
        const auto expected = data.size().value();
        return write_impl(file_position{position_}, std::move(data))
          .then([expected](result<byte_count> outcome) {
              if (!outcome || outcome->value() != expected) {
                  std::terminate();
              }
          });
    }

    [[gnu::noinline]] seastar::future<> execute_out_of_line() {
        auto data = detail::fragmented_buffer_io_access::adopt(
          source_.share(0, write_size_));
        const auto expected = data.size().value();
        return write_boundary(*this, file_position{position_}, std::move(data))
          .then([expected](result<byte_count> outcome) {
              if (!outcome || outcome->value() != expected) {
                  std::terminate();
              }
          });
    }

    [[gnu::always_inline]] seastar::future<result<byte_count>>
    write_impl(file_position position, bytes::fragmented_buffer data) {
        if (auto valid = validate_file_write_request(position, data); !valid) {
            std::terminate();
        }
        if (operation_rejection()) {
            std::terminate();
        }
        const auto data_size = data.size().value();
        const bool has_one_fragment = data.fragment_count() == 1;
        const auto source_fragment = has_one_fragment ? *data.begin()
                                                      : bytes::fragment_view{};
        if (
          !has_one_fragment
          || (position.value() & (append_alignment_ - 1U)) != 0
          || (data_size & (append_alignment_ - 1U)) != 0
          || data_size > native_limit_
          || (reinterpret_cast<std::uintptr_t>(source_fragment.data())
              & (memory_alignment_ - 1U))
               != 0) {
            std::terminate();
        }
        auto serialization = seastar::try_get_units(write_serialization_, 1);
        if (!serialization) {
            std::terminate();
        }
        auto consumer = detail::fragmented_buffer_io_access::consume(data);
        auto fragment = consumer.take_front();
        const auto expected = fragment.size();
        return native_
          .dma_write(position.value(), fragment.get(), expected, &io_intent_)
          .then_wrapped(
            [this,
             position = position.value(),
             fragment = std::move(fragment),
             data = std::move(data),
             serialization = std::move(*serialization),
             expected,
             append_alignment = append_alignment_,
             memory_alignment = memory_alignment_](
              seastar::future<std::size_t> completed) mutable
              -> seastar::future<result<byte_count>> {
                static_cast<void>(this);
                static_cast<void>(position);
                static_cast<void>(fragment);
                static_cast<void>(data);
                static_cast<void>(serialization);
                static_cast<void>(append_alignment);
                static_cast<void>(memory_alignment);
                try {
                    const auto written = completed.get();
                    if (written == 0 || written > expected) {
                        result<byte_count> outcome = failure(
                          operation_error{
                            errc::io_failure, operation_kind::file});
                        return seastar::make_ready_future<result<byte_count>>(
                          std::move(outcome));
                    }
                    if (written != expected) {
                        result<byte_count> outcome = failure(
                          operation_error{
                            errc::io_failure, operation_kind::file});
                        return seastar::make_ready_future<result<byte_count>>(
                          std::move(outcome));
                    }
                    result<byte_count> outcome = byte_count{written};
                    return seastar::make_ready_future<result<byte_count>>(
                      std::move(outcome));
                } catch (const std::bad_alloc&) {
                    return seastar::current_exception_as_future<
                      result<byte_count>>();
                } catch (...) {
                    result<byte_count> outcome = failure(
                      operation_error{errc::io_failure, operation_kind::file});
                    return seastar::make_ready_future<result<byte_count>>(
                      std::move(outcome));
                }
            });
    }

    [[gnu::noinline]] static seastar::future<result<byte_count>> write_boundary(
      native_aligned_file_fixture& fixture,
      file_position position,
      bytes::fragmented_buffer data) {
        return fixture.write_impl(position, std::move(data));
    }

private:
    [[gnu::noinline]] std::optional<operation_error>
    operation_rejection() const {
        owner_.assert_current();
        if (owner_state_ != file_state::open) {
            return operation_error{errc::closed, operation_kind::file};
        }
        if (abort_requested_) {
            return operation_error{errc::aborted, operation_kind::file};
        }
        return std::nullopt;
    }

    benchmark_file_state state_;
    seastar::file native_;
    seastar::io_intent io_intent_;
    seastar::temporary_buffer<char> source_;
    seastar::semaphore write_serialization_{1};
    owner_shard owner_;
    std::uint64_t memory_alignment_;
    std::uint64_t append_alignment_;
    std::uint64_t native_limit_;
    std::uint64_t position_{0};
    std::size_t write_size_{4096};
    file_state owner_state_{file_state::open};
    bool abort_requested_{false};
};

struct native_out_of_line_aligned_file_fixture : native_aligned_file_fixture {};

class native_raw_aligned_file_fixture {
public:
    native_raw_aligned_file_fixture()
      : native_(make_native_file(state_))
      , source_(make_source(4096)) {}

    ~native_raw_aligned_file_fixture() {
        native_.close().get();
        if (state_.closes != 1) {
            std::terminate();
        }
    }

    native_raw_aligned_file_fixture(const native_raw_aligned_file_fixture&)
      = delete;
    native_raw_aligned_file_fixture&
    operator=(const native_raw_aligned_file_fixture&) = delete;

    seastar::future<> execute() {
        auto source = source_.share();
        return native_.dma_write(0, source.get(), source.size())
          .then([source = std::move(source)](std::size_t written) mutable {
              static_cast<void>(source);
              if (written != 4096U) {
                  std::terminate();
              }
          });
    }

private:
    benchmark_file_state state_;
    seastar::file native_;
    seastar::temporary_buffer<char> source_;
};

class native_raw_file_read_fixture {
public:
    native_raw_file_read_fixture()
      : native_(make_native_file(state_)) {}

    ~native_raw_file_read_fixture() {
        native_.close().get();
        if (state_.closes != 1) {
            std::terminate();
        }
    }

    [[gnu::noinline]] seastar::future<> execute() {
        return native_.dma_read_bulk<char>(0, 4096).then(
          [](seastar::temporary_buffer<char> data) {
              if (data.size() != 4096 || data.get()[0] != 'b') {
                  std::terminate();
              }
          });
    }

private:
    benchmark_file_state state_;
    seastar::file native_;
};

class native_file_read_fixture {
public:
    native_file_read_fixture()
      : native_(make_native_file(state_)) {}

    ~native_file_read_fixture() {
        operations_.close().get();
        native_.close().get();
        if (state_.closes != 1) {
            std::terminate();
        }
    }

    [[gnu::noinline]] seastar::future<> execute() {
        owner_.assert_current();
        if (
          !validate_file_read_request(file_position{0}, byte_count{4096})
          || owner_state_ != file_state::open || abort_requested_
          || byte_count{4096} > limits_.pending_read_bytes) {
            std::terminate();
        }
        auto operation = seastar::try_get_units(read_operation_units_, 1);
        auto bytes = seastar::try_get_units(read_byte_units_, 4096);
        if (!operation || !bytes) {
            std::terminate();
        }
        auto holder = operations_.hold();
        return native_.dma_read_bulk<char>(0, 4096, &io_intent_)
          .then_wrapped(
            [operation = std::move(*operation),
             bytes = std::move(*bytes),
             holder = std::move(holder)](
              seastar::future<seastar::temporary_buffer<char>>
                completed) mutable -> result<file_read_result> {
                static_cast<void>(operation);
                static_cast<void>(bytes);
                static_cast<void>(holder);
                try {
                    auto native = completed.get();
                    auto data = detail::fragmented_buffer_io_access::adopt(
                      std::move(native));
                    return file_read_result::make(
                      std::move(data), false, byte_count{4096});
                } catch (const std::bad_alloc&) {
                    throw;
                } catch (...) {
                    return failure(
                      operation_error{errc::io_failure, operation_kind::file});
                }
            })
          .then([](result<file_read_result> outcome) {
              if (
                !outcome || outcome->eof()
                || outcome->data().size().value() != 4096
                || !outcome->data().content_equals(
                  std::string_view{benchmark_file_contents.data(), 4096})) {
                  std::terminate();
              }
          });
    }

private:
    static inline const std::string benchmark_file_contents = std::string(
      backing_size, 'b');

    benchmark_file_state state_;
    seastar::file native_;
    seastar::io_intent io_intent_;
    seastar::gate operations_;
    seastar::semaphore read_operation_units_{64};
    seastar::semaphore read_byte_units_{maximum_file_io_bytes.value()};
    file_io_limits limits_;
    owner_shard owner_;
    file_state owner_state_{file_state::open};
    bool abort_requested_{false};
};

class kwaque_file_read_fixture {
public:
    kwaque_file_read_fixture()
      : owner_(make_native_file(state_)) {}

    ~kwaque_file_read_fixture() {
        const auto closed = owner_.close().get();
        if (!closed || state_.closes != 1) {
            std::terminate();
        }
    }

    [[gnu::noinline]] seastar::future<> execute() {
        return owner_.read(file_position{0}, byte_count{4096})
          .then([](result<file_read_result> outcome) {
              if (
                !outcome || outcome->eof()
                || outcome->data().size().value() != 4096
                || !outcome->data().content_equals(
                  std::string_view{benchmark_file_contents.data(), 4096})) {
                  std::terminate();
              }
          });
    }

private:
    static inline const std::string benchmark_file_contents = std::string(
      backing_size, 'b');

    benchmark_file_state state_;
    file owner_;
};

} // namespace

PERF_TEST_F(native_raw_aligned_file_fixture, direct_aligned_4096) {
    return execute();
}

PERF_TEST_F(native_aligned_file_fixture, direct_aligned_4096) {
    return execute();
}

PERF_TEST_F(native_out_of_line_aligned_file_fixture, direct_aligned_4096) {
    return execute_out_of_line();
}

PERF_TEST_F(kwaque_aligned_file_fixture, direct_aligned_4096) {
    return execute();
}

PERF_TEST_F(kwaque_staged_file_fixture, staged_aligned_4096) {
    return execute();
}

PERF_TEST_F(kwaque_rmw_file_fixture, read_modify_write_6_at_101) {
    return execute();
}

PERF_TEST_F(native_fragmented_file_fixture, staged_64x64) { return execute(); }

PERF_TEST_F(kwaque_fragmented_file_fixture, staged_64x64) { return execute(); }

PERF_TEST_F(native_raw_file_read_fixture, direct_read_4096) {
    return execute();
}

PERF_TEST_F(native_file_read_fixture, validated_read_4096) { return execute(); }

PERF_TEST_F(kwaque_file_read_fixture, read_4096) { return execute(); }

} // namespace kwaque::runtime
