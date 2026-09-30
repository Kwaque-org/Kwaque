#pragma once

#include "src/runtime/first_failure.h"
#include "src/storage/tests/wal_bench_file.h"

namespace kwaque::storage::testing::segment_bench_support {
using wal_bench_support::io_sample;
using wal_bench_support::measurement_time;

class file_system final {
public:
    class directory_cursor final {
    public:
        directory_cursor(
          runtime::production::directory_cursor value,
          file_system& owner) noexcept
          : native_(std::move(value))
          , owner_(&owner) {}
        directory_cursor(directory_cursor&&) noexcept = default;
        directory_cursor& operator=(directory_cursor&&) noexcept = default;
        auto next(runtime::directory_page_limits limits) {
            return native_.next(limits);
        }
        seastar::future<runtime::result<void>> sync() {
            if (!owner_->observe_ || !owner_->sample.measuring)
                return native_.sync();
            return measured_sync();
        }
        auto close() { return native_.close(); }

    private:
        seastar::future<runtime::result<void>> measured_sync() {
            const auto started = measurement_time();
            ++owner_->directory_syncs;
            auto finished = seastar::defer([&] noexcept {
                owner_->directory_sync_ns += measurement_time() - started;
            });
            co_return co_await native_.sync();
        }
        runtime::production::directory_cursor native_;
        file_system* owner_;
    };
    using directory_cursor_type = directory_cursor;
    explicit file_system(bool observe)
      : files_(statistics_)
      , observe_(observe) {}
    io_sample sample;
    std::uint64_t directory_syncs{0}, directory_sync_ns{0};
    std::array<std::uint64_t, 6> native_geometry{};

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
            if (
              options.access == runtime::file_access::read_write
              && !options.create) {
                const std::array<std::uint64_t, 6> geometry{
                  native.memory_dma_alignment(),
                  native.disk_read_dma_alignment(),
                  native.disk_write_dma_alignment(),
                  native.disk_overwrite_dma_alignment(),
                  native.disk_read_max_length(),
                  native.disk_write_max_length()};
                if (native_geometry[0] && native_geometry != geometry)
                    throw std::runtime_error(
                      "benchmark native geometry changed between segment "
                      "owners");
                native_geometry = geometry;
                writable_ = native;
            }
            auto file = observe_ ? seastar::file{seastar::make_shared<
                                     wal_bench_support::observed_file>(
                                     native, sample, 0, options.synchronous)}
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
    seastar::future<runtime::result<directory_cursor>>
    open_directory(runtime::file_path path, runtime::file_close_policy policy) {
        auto opened = co_await files_.open_directory(std::move(path), policy);
        if (!opened) co_return runtime::failure(opened.error());
        co_return directory_cursor{std::move(*opened), *this};
    }
    auto statistics() const noexcept { return statistics_->snapshot(); }
    auto exists(runtime::file_path path) {
        return files_.exists(std::move(path));
    }
    auto stat(runtime::file_path path) { return files_.stat(std::move(path)); }
    auto space(runtime::file_path path) {
        return files_.space(std::move(path));
    }
    auto
    list(runtime::file_path path, runtime::directory_listing_limits limits) {
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
    seastar::future<runtime::result<void>>
    sync_directory(runtime::file_path path, runtime::file_close_policy policy) {
        auto opened = co_await open_directory(std::move(path), policy);
        if (!opened) co_return runtime::failure(opened.error());
        runtime::first_failure failed;
        try {
            failed.observe(co_await opened->sync());
        } catch (...) {
            failed.observe(std::current_exception());
        }
        try {
            failed.observe(co_await opened->close());
        } catch (...) {
            failed.observe(std::current_exception());
        }
        co_return failed.outcome();
    }
    // Benchmarks may preallocate only a fresh tail, before admitting any group.
    // Never zero an existing header or already frozen/written range.
    seastar::future<>
    prepare_extent(std::uint64_t prefix, std::uint64_t extent) {
        if (co_await writable_.size() != prefix || extent < prefix)
            throw std::runtime_error(
              "preallocation would overlap existing data");
        if (extent != prefix)
            co_await writable_.allocate(prefix, extent - prefix);
        co_await writable_.truncate(extent);
        co_await writable_.flush();
    }
    seastar::future<> finish_extent(std::uint64_t end) {
        co_await writable_.truncate(end);
        co_await writable_.flush();
    }
    seastar::future<std::uint64_t> allocated_bytes() {
        const auto status = co_await writable_.stat();
        if (status.st_blocks < 0)
            throw std::runtime_error(
              "native file returned invalid allocated-block accounting");
        co_return static_cast<std::uint64_t>(status.st_blocks) * 512U;
    }

private:
    runtime::operation_statistics_owner statistics_;
    runtime::production::file_system files_;
    seastar::file writable_;
    bool observe_;
};
static_assert(runtime::file_system_backend<file_system>);
} // namespace kwaque::storage::testing::segment_bench_support
