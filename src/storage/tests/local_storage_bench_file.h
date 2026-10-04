#pragma once

#include "src/runtime/file.h"
#include "src/runtime/operation_statistics.h"
#include "src/runtime/production/file.h"
#include "src/storage/tests/wal_bench_file.h"

#include <seastar/core/seastar.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/defer.hh>

#include <algorithm>
#include <array>
#include <cstdint>
#include <exception>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace kwaque::storage::testing::local_storage_bench_support {
using wal_bench_support::measurement_time;

// Files are classified by their store names: a WAL file ends in ".wal" and a
// segment's data file is named "data". Metadata and control files are other.
enum class file_kind : std::uint8_t { wal, data, other };
constexpr std::size_t file_kinds = 3;
inline file_kind classify(const runtime::file_path& path) noexcept {
    const std::string_view value{path.value()};
    if (value.ends_with(".wal")) return file_kind::wal;
    const auto slash = value.rfind('/');
    const auto leaf = slash == std::string_view::npos ? value
                                                      : value.substr(slash + 1);
    return leaf == "data" ? file_kind::data : file_kind::other;
}

struct interval final {
    std::uint64_t begin{0}, end{0};
};
// One file kind's native work during the interval. Interval sums overlap and
// are not additive latency components; busy time counts overlap once.
struct kind_sample final {
    std::uint64_t calls{0}, bytes{0}, flushes{0};
    std::uint64_t active{0}, depth{0}, flushing{0}, flush_depth{0};
    std::uint64_t write_service_ns{0}, write_busy_ns{0}, write_busy_start{0};
    std::uint64_t flush_service_ns{0};
    // Flushes that waited for the device flush cap, and how long.
    std::uint64_t cap_waits{0}, cap_wait_ns{0};
    // Native reads as requested, and the files opened.
    std::uint64_t reads{0}, read_bytes{0}, read_minimum{UINT64_MAX},
      read_maximum{0}, opens{0};
    // Recorded only when sized before measurement; overflow is reported.
    std::vector<interval> flush_times;
    std::size_t flushes_recorded{0};
    bool flush_times_overflowed{false};
};
struct io_sample final {
    bool measuring{false};
    std::array<kind_sample, file_kinds> kinds{};
    // Namespace operations: directory opens, syncs and pages read, and path
    // status lookups.
    std::uint64_t directory_opens{0}, directory_syncs{0}, listings{0}, stats{0};
    [[nodiscard]] kind_sample& operator[](file_kind kind) noexcept {
        return kinds[static_cast<std::size_t>(kind)];
    }
    [[nodiscard]] const kind_sample& operator[](file_kind kind) const noexcept {
        return kinds[static_cast<std::size_t>(kind)];
    }
    [[nodiscard]] std::uint64_t active() const noexcept {
        std::uint64_t total = 0;
        for (const auto& kind : kinds)
            total += kind.active + kind.flushing;
        return total;
    }
};

// Observes one file's native reads, writes and flushes, and holds a device
// flush cap slot around each data flush. Namespace, native geometry and
// checked close remain the real file implementation.
class measured_file final : public seastar::file_impl {
public:
    measured_file(
      seastar::file native,
      io_sample* sample,
      file_kind kind,
      seastar::semaphore* cap)
      : file_(std::move(native))
      , sample_(sample)
      , kind_(kind)
      , cap_(cap) {
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

private:
    // Ends the write's observation when destroyed.
    [[nodiscard]] auto begin_write(std::size_t len) {
        const bool observed = sample_ && sample_->measuring;
        kind_sample* sample = observed ? &(*sample_)[kind_] : nullptr;
        const auto started = observed ? measurement_time() : 0;
        if (sample) {
            ++sample->calls;
            sample->bytes += len;
            sample->depth = std::max(sample->depth, ++sample->active);
            if (sample->active == 1) sample->write_busy_start = started;
        }
        return seastar::defer([sample, started] noexcept {
            if (!sample) return;
            const auto ended = measurement_time();
            --sample->active;
            sample->write_service_ns += ended - started;
            if (sample->active == 0)
                sample->write_busy_ns += ended - sample->write_busy_start;
        });
    }
    void observe_read(std::size_t len) noexcept {
        if (!sample_ || !sample_->measuring) return;
        auto& sample = (*sample_)[kind_];
        ++sample.reads;
        sample.read_bytes += len;
        sample.read_minimum = std::min(sample.read_minimum, std::uint64_t{len});
        sample.read_maximum = std::max(sample.read_maximum, std::uint64_t{len});
    }

public:
    seastar::future<std::size_t> write_dma(
      std::uint64_t pos,
      const void* data,
      std::size_t len,
      seastar::io_intent* intent) final {
        auto observed = begin_write(len);
        co_return co_await file_.dma_write(pos, data, len, intent);
    }
    seastar::future<std::size_t> write_dma(
      std::uint64_t pos,
      std::vector<iovec> data,
      seastar::io_intent* intent) final {
        std::size_t len = 0;
        for (const auto& part : data)
            len += part.iov_len;
        auto observed = begin_write(len);
        co_return co_await file_.dma_write(pos, std::move(data), intent);
    }
    seastar::future<std::size_t> read_dma(
      std::uint64_t pos,
      void* data,
      std::size_t len,
      seastar::io_intent* intent) final {
        observe_read(len);
        return file_.dma_read(pos, data, len, intent);
    }
    seastar::future<std::size_t> read_dma(
      std::uint64_t pos,
      std::vector<iovec> data,
      seastar::io_intent* intent) final {
        std::size_t len = 0;
        for (const auto& part : data)
            len += part.iov_len;
        observe_read(len);
        return file_.dma_read(pos, std::move(data), intent);
    }
    seastar::future<seastar::temporary_buffer<std::uint8_t>> dma_read_bulk(
      std::uint64_t pos, std::size_t len, seastar::io_intent* intent) final {
        observe_read(len);
        return file_.dma_read_bulk<std::uint8_t>(pos, len, intent);
    }
    seastar::future<> flush() final {
        const bool observed = sample_ && sample_->measuring;
        std::optional<seastar::semaphore_units<>> slot;
        if (cap_ && kind_ == file_kind::data) {
            if (auto ready = seastar::try_get_units(*cap_, 1))
                slot.emplace(std::move(*ready));
            else {
                const auto waited = measurement_time();
                slot.emplace(co_await seastar::get_units(*cap_, 1));
                if (observed) {
                    auto& sample = (*sample_)[kind_];
                    ++sample.cap_waits;
                    sample.cap_wait_ns += measurement_time() - waited;
                }
            }
        }
        if (!observed) {
            co_await file_.flush();
            co_return;
        }
        auto& sample = (*sample_)[kind_];
        ++sample.flushes;
        sample.flush_depth = std::max(sample.flush_depth, ++sample.flushing);
        const auto started = measurement_time();
        auto finished = seastar::defer([&sample, started] noexcept {
            --sample.flushing;
            const auto ended = measurement_time();
            sample.flush_service_ns += ended - started;
            if (sample.flushes_recorded < sample.flush_times.size())
                sample.flush_times[sample.flushes_recorded++] = {
                  started, ended};
            else
                sample.flush_times_overflowed = true;
        });
        co_await file_.flush();
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
    seastar::future<std::uint64_t> size() final { return file_.size(); }
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
    seastar::file file_;
    io_sample* sample_;
    file_kind kind_;
    seastar::semaphore* cap_;
};

// The production directory cursor, counting the syncs and pages it serves
// while measuring: storage lists and syncs directories through cursors.
class measured_cursor final {
public:
    measured_cursor(
      runtime::production::directory_cursor native, io_sample* sample) noexcept
      : native_(std::move(native))
      , sample_(sample) {}
    measured_cursor(measured_cursor&&) noexcept = default;
    measured_cursor& operator=(measured_cursor&&) noexcept = default;
    measured_cursor(const measured_cursor&) = delete;
    measured_cursor& operator=(const measured_cursor&) = delete;
    ~measured_cursor() = default;

    seastar::future<runtime::result<runtime::directory_page>>
    next(runtime::directory_page_limits limits) {
        if (sample_ && sample_->measuring) ++sample_->listings;
        return native_.next(limits);
    }
    seastar::future<runtime::result<void>> sync() {
        if (sample_ && sample_->measuring) ++sample_->directory_syncs;
        return native_.sync();
    }
    seastar::future<runtime::result<void>> close() { return native_.close(); }

private:
    runtime::production::directory_cursor native_;
    io_sample* sample_;
};

class file_system final {
public:
    using directory_cursor_type = measured_cursor;
    // flush_cap bounds concurrent data-file flushes on this device; zero is
    // no cap. Without observation, files are native unless capped.
    file_system(bool observe, std::uint32_t flush_cap)
      : files_(statistics_)
      , observe_(observe) {
        if (flush_cap != 0) cap_.emplace(flush_cap);
    }
    io_sample sample;
    // Sized before an interval; flushes beyond it are counted, not recorded.
    void reserve_flush_times(std::size_t per_kind) {
        for (auto& kind : sample.kinds)
            kind.flush_times.resize(per_kind);
    }

    seastar::future<runtime::result<runtime::file>>
    open(runtime::file_path path, runtime::file_open_options options) {
        if (auto valid = options.validate(); !valid)
            co_return runtime::failure(valid.error());
        [[maybe_unused]] auto metric = statistics_->accept();
        seastar::open_flags flags = seastar::open_flags::ro;
        switch (options.access) {
        case runtime::file_access::read_only:
            break;
        case runtime::file_access::write_only:
            flags = seastar::open_flags::wo;
            break;
        case runtime::file_access::read_write:
            flags = seastar::open_flags::rw;
            break;
        }
        if (options.create) flags |= seastar::open_flags::create;
        if (options.exclusive) flags |= seastar::open_flags::exclusive;
        if (options.truncate) flags |= seastar::open_flags::truncate;
        if (options.synchronous) flags |= seastar::open_flags::dsync;
        seastar::file_open_options native_options;
        native_options.create_permissions
          = static_cast<seastar::file_permissions>(options.permissions);
        native_options.durable = true;
        auto native = co_await seastar::open_file_dma(
          path.value(), flags, native_options);
        std::exception_ptr failure;
        try {
            const auto kind = classify(path);
            if (observe_ && sample.measuring) ++sample[kind].opens;
            auto* cap = cap_ ? &*cap_ : nullptr;
            auto file = observe_ || (cap && kind == file_kind::data)
                          ? seastar::file{seastar::make_shared<measured_file>(
                              native, observe_ ? &sample : nullptr, kind, cap)}
                          : native;
            co_return runtime::file{
              std::move(file),
              runtime::file_io_limits{},
              statistics_,
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
    seastar::future<runtime::result<measured_cursor>>
    open_directory(runtime::file_path path, runtime::file_close_policy c) {
        if (observe_ && sample.measuring) ++sample.directory_opens;
        auto opened = co_await files_.open_directory(std::move(path), c);
        if (!opened) co_return runtime::failure(opened.error());
        co_return measured_cursor{
          std::move(*opened), observe_ ? &sample : nullptr};
    }
    auto statistics() const noexcept { return statistics_->snapshot(); }
    auto exists(runtime::file_path path) {
        if (observe_ && sample.measuring) ++sample.stats;
        return files_.exists(std::move(path));
    }
    auto stat(runtime::file_path path) {
        if (observe_ && sample.measuring) ++sample.stats;
        return files_.stat(std::move(path));
    }
    auto space(runtime::file_path path) {
        return files_.space(std::move(path));
    }
    auto
    list(runtime::file_path path, runtime::directory_listing_limits limits) {
        if (observe_ && sample.measuring) ++sample.listings;
        return files_.list(std::move(path), limits);
    }
    auto create_directories(runtime::file_path path) {
        return files_.create_directories(std::move(path));
    }
    auto remove_file(runtime::file_path path) {
        return files_.remove_file(std::move(path));
    }
    auto remove_directory(runtime::file_path path) {
        return files_.remove_directory(std::move(path));
    }
    auto rename(
      runtime::file_path from,
      runtime::file_path to,
      runtime::file_rename_policy policy) {
        return files_.rename(std::move(from), std::move(to), policy);
    }
    auto
    sync_directory(runtime::file_path path, runtime::file_close_policy policy) {
        if (observe_ && sample.measuring) ++sample.directory_syncs;
        return files_.sync_directory(std::move(path), policy);
    }
    bool verified_directory(std::string_view path) const noexcept {
        return files_.verified_directory(path);
    }
    void remember_directory(std::string_view path) noexcept {
        files_.remember_directory(path);
    }

private:
    runtime::operation_statistics_owner statistics_;
    runtime::production::file_system files_;
    std::optional<seastar::semaphore> cap_;
    bool observe_;
};
static_assert(runtime::file_system_backend<file_system>);
static_assert(runtime::verified_directory_memo<file_system>);
} // namespace kwaque::storage::testing::local_storage_bench_support
