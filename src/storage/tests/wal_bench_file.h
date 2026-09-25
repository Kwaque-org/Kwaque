#pragma once

#include "src/runtime/production/file.h"

#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/defer.hh>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <functional>
#include <stdexcept>
#include <vector>

namespace kwaque::storage::testing::wal_bench_support {
inline std::uint64_t measurement_time() noexcept {
    return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch())
        .count());
}
struct flush_measurement final {
    std::uint64_t begin{0}, end{0};
};
struct io_sample final {
    bool measuring{false}, noop{false};
    std::uint64_t calls{0}, bytes{0}, flushes{0}, active{0}, depth{0};
    std::uint64_t minimum{UINT64_MAX}, maximum{0};
    bool measure_service_time{false};
    std::uint64_t write_service_ns{0}, flush_service_ns{0};
    // Wall time with at least one pending write, and the longest individual
    // write interval. Unlike the sum, busy time does not count overlap twice.
    std::uint64_t write_busy_ns{0}, write_max_service_ns{0},
      write_busy_start{0};
    std::array<flush_measurement, 128> flush_times{};
};
// The decorator observes the actual native boundary. Namespace, header reads,
// default native geometry and checked close remain the real file
// implementation. CPU-only cases discard appends/flushes explicitly; they
// cannot certify disk durability. Both compared owners use this same decorator.
class observed_file final : public seastar::file_impl {
public:
    observed_file(seastar::file native, io_sample& sample, std::uint64_t size)
      : file_(std::move(native))
      , sample_(sample)
      , logical_size_(size) {
        _memory_dma_alignment = static_cast<unsigned>(
          file_.memory_dma_alignment());
        _disk_read_dma_alignment = static_cast<unsigned>(
          file_.disk_read_dma_alignment());
        _disk_write_dma_alignment = static_cast<unsigned>(
          file_.disk_write_dma_alignment());
        _disk_overwrite_dma_alignment = static_cast<unsigned>(
          file_.disk_overwrite_dma_alignment());
        _read_max_length = static_cast<unsigned>(file_.disk_read_max_length());
        _write_max_length = static_cast<unsigned>(
          file_.disk_write_max_length());
    }
    seastar::future<std::size_t> write_dma(
      std::uint64_t pos,
      const void* data,
      std::size_t len,
      seastar::io_intent* intent) final {
        const bool observed = sample_.measuring;
        const bool timed = observed && sample_.measure_service_time;
        const auto started = timed ? measurement_time() : 0;
        if (observed) {
            ++sample_.calls;
            sample_.bytes += len;
            sample_.minimum = std::min(sample_.minimum, std::uint64_t{len});
            sample_.maximum = std::max(sample_.maximum, std::uint64_t{len});
            sample_.depth = std::max(sample_.depth, ++sample_.active);
            if (timed && sample_.active == 1)
                sample_.write_busy_start = started;
        }
        auto done = seastar::defer([&, observed] noexcept {
            if (observed) --sample_.active;
            if (timed) {
                const auto ended = measurement_time();
                const auto duration = ended - started;
                sample_.write_service_ns += duration;
                sample_.write_max_service_ns = std::max(
                  sample_.write_max_service_ns, duration);
                if (sample_.active == 0)
                    sample_.write_busy_ns += ended - sample_.write_busy_start;
            }
        });
        if (sample_.noop) {
            perf_tests::do_not_optimize(data);
            logical_size_ = std::max(logical_size_, pos + len);
            co_return len;
        }
        co_return co_await file_.dma_write(pos, data, len, intent);
    }
    seastar::future<std::size_t>
    write_dma(std::uint64_t, std::vector<iovec>, seastar::io_intent*) final {
        throw std::runtime_error(
          "unexpected vector write in scalar I/O profile");
    }
    seastar::future<std::size_t> read_dma(
      std::uint64_t pos,
      void* data,
      std::size_t len,
      seastar::io_intent* intent) final {
        return file_.dma_read(pos, data, len, intent);
    }
    seastar::future<std::size_t> read_dma(
      std::uint64_t pos,
      std::vector<iovec> data,
      seastar::io_intent* intent) final {
        return file_.dma_read(pos, std::move(data), intent);
    }
    seastar::future<seastar::temporary_buffer<std::uint8_t>> dma_read_bulk(
      std::uint64_t pos, std::size_t len, seastar::io_intent* intent) final {
        return file_.dma_read_bulk<std::uint8_t>(pos, len, intent);
    }
    seastar::future<> flush() final {
        if (!sample_.measuring || !sample_.measure_service_time) {
            if (sample_.measuring) ++sample_.flushes;
            return sample_.noop ? seastar::make_ready_future<>()
                                : file_.flush();
        }
        return measured_flush();
    }
    seastar::future<struct stat> stat() final { return file_.stat(); }
    seastar::future<> truncate(std::uint64_t size) final {
        return file_.truncate(size);
    }
    seastar::future<> discard(std::uint64_t pos, std::uint64_t len) final {
        return file_.discard(pos, len);
    }
    seastar::future<> allocate(std::uint64_t pos, std::uint64_t len) final {
        return file_.allocate(pos, len);
    }
    seastar::future<std::uint64_t> size() final {
        if (sample_.noop)
            return seastar::make_ready_future<std::uint64_t>(logical_size_);
        return file_.size();
    }
    seastar::future<> close() final { return file_.close(); }
    bool supports_checked_close() const noexcept final {
        return file_.supports_checked_close();
    }
    seastar::future<> close_checked() final { return file_.close_checked(); }
    seastar::subscription<seastar::directory_entry> list_directory(
      std::function<seastar::future<>(seastar::directory_entry)> next) final {
        return file_.list_directory(std::move(next));
    }

private:
    seastar::future<> measured_flush() {
        const auto index = sample_.flushes++;
        if (index >= sample_.flush_times.size())
            throw std::runtime_error("flush measurement exceeded its bound");
        auto& sample = sample_.flush_times[index];
        sample.begin = measurement_time();
        auto finished = seastar::defer([&] noexcept {
            sample.end = measurement_time();
            sample_.flush_service_ns += sample.end - sample.begin;
        });
        if (!sample_.noop) co_await file_.flush();
    }
    seastar::file file_;
    io_sample& sample_;
    std::uint64_t logical_size_{0};
};

class file_system final {
public:
    using directory_cursor_type = runtime::production::directory_cursor;
    explicit file_system(bool observe_io = true)
      : observe_io_(observe_io) {}
    io_sample sample;
    seastar::future<> prepare_extent(std::uint64_t size) {
        co_await native_.allocate(0, size);
        co_await native_.truncate(size);
        co_await native_.flush();
    }
    seastar::future<> finish_extent(std::uint64_t size) {
        co_await native_.truncate(size);
        co_await native_.flush();
    }
    seastar::future<runtime::result<runtime::file>>
    open(runtime::file_path path, runtime::file_open_options options) {
        // Only the writer's existing-file, read/write reopen is observed.
        // All temporary publication and reader opens use the production owner.
        if (
          options.access != runtime::file_access::read_write || options.create
          || options.truncate)
            co_return co_await files_.open(std::move(path), options);
        if (auto valid = options.validate(); !valid)
            co_return runtime::failure(valid.error());
        seastar::file_open_options native_options;
        native_options.durable = true;
        auto native = co_await seastar::open_file_dma(
          path.value(), seastar::open_flags::rw, native_options);
        std::exception_ptr failure;
        try {
            const auto size = co_await native.size();
            native_ = native;
            auto measured
              = observe_io_ ? seastar::file{seastar::make_shared<observed_file>(
                                native, sample, size)}
                            : native;
            co_return runtime::file{
              std::move(measured),
              runtime::file_io_limits{},
              runtime::operation_statistics_owner{},
              options.close_policy};
        } catch (...) {
            failure = std::current_exception();
        }
        try {
            co_await native.close_checked();
        } catch (...) {
        }
        std::rethrow_exception(failure);
    }
    auto open_directory(runtime::file_path p, runtime::file_close_policy c) {
        return files_.open_directory(std::move(p), c);
    }
    auto statistics() const noexcept { return files_.statistics(); }
    auto exists(runtime::file_path p) { return files_.exists(std::move(p)); }
    auto stat(runtime::file_path p) { return files_.stat(std::move(p)); }
    auto space(runtime::file_path p) { return files_.space(std::move(p)); }
    auto list(runtime::file_path p, runtime::directory_listing_limits l) {
        return files_.list(std::move(p), l);
    }
    auto create_directories(runtime::file_path p) {
        return files_.create_directories(std::move(p));
    }
    auto remove_file(runtime::file_path p) {
        return files_.remove_file(std::move(p));
    }
    auto remove_directory(runtime::file_path p) {
        return files_.remove_directory(std::move(p));
    }
    auto rename(
      runtime::file_path a,
      runtime::file_path b,
      runtime::file_rename_policy c) {
        return files_.rename(std::move(a), std::move(b), c);
    }
    auto sync_directory(runtime::file_path p, runtime::file_close_policy c) {
        return files_.sync_directory(std::move(p), c);
    }

private:
    runtime::production::file_system files_;
    seastar::file native_;
    bool observe_io_;
};
static_assert(runtime::file_system_backend<file_system>);
} // namespace kwaque::storage::testing::wal_bench_support
