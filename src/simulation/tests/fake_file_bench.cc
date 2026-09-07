#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/runtime/file.h"
#include "src/simulation/event_trace.h"
#include "src/simulation/fake_file.h"
#include "src/simulation/fake_file_test_support.h"
#include "src/simulation/fault_schedule.h"
#include "src/simulation/scheduler.h"
#include "src/simulation/scheduler_driver.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/testing/perf_tests.hh>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>

namespace kwaque::simulation {

namespace {

constexpr std::size_t file_payload_bytes{4'096};
constexpr std::uint64_t file_benchmark_seed{71};
constexpr std::uint32_t file_trace_entries{1'024};
constexpr std::uint64_t file_restart_reserve{64};
constexpr std::uint32_t file_pending_operations{8};

enum class file_case { read, write, flush, crash };

runtime::file_path benchmark_path(std::string value) {
    auto made = runtime::file_path::make(std::move(value));
    if (!made) {
        throw std::runtime_error("invalid fake file benchmark path");
    }
    return std::move(*made);
}

bytes::fragmented_buffer benchmark_bytes(char value) {
    const std::string input(file_payload_bytes, value);
    auto made = bytes::fragmented_buffer::copy_of(
      std::span<const char>{input.data(), input.size()});
    if (!made) {
        throw std::runtime_error("fake file benchmark payload construction");
    }
    return std::move(*made);
}

scheduler_limits file_scheduler_limits() {
    auto made = scheduler_limits::make(
      scheduler_limit_values{
        .pending_events = 3U * file_pending_operations,
        .events_per_pump = 64,
        .total_events = 1'024,
        .maximum_deadline = runtime::monotonic_time{1'000'000},
      });
    if (!made) {
        throw std::runtime_error(
          "invalid fake file benchmark scheduler limits");
    }
    return *made;
}

trace_limits file_trace_limits() {
    auto made = trace_limits::make(
      trace_limit_values{
        .entries = file_trace_entries,
        .encoded_bytes = canonical_header_encoded_size
                         + file_trace_entries * canonical_entry_encoded_size,
        .line_bytes = 1'024,
      });
    if (!made) {
        throw std::runtime_error("invalid fake file benchmark trace limits");
    }
    return *made;
}

// A fixed one-file durability fixture. Every invocation begins with durable
// 'a' bytes; flush and crash receive the same volatile 'b' overwrite. Setup,
// state restoration, snapshot checks, and bounded trace renewal are excluded
// from time, allocation, and task measurements by the native timing regions.
template<file_case Selected>
class fake_file_fixture {
public:
    fake_file_fixture() {
        try {
            create_environment();
            initialize().get();
        } catch (...) {
            const auto failure = std::current_exception();
            stop_environment().get();
            std::rethrow_exception(failure);
        }
    }

    ~fake_file_fixture() { stop_environment().get(); }

    seastar::future<std::size_t> execute() {
        if (
          trace_->entries().size() + file_restart_reserve >= file_trace_entries
          || events_->executed_events() + file_restart_reserve
               >= events_->limits().total_events()) {
            co_await stop_environment();
            create_environment();
            co_await initialize();
        }
        if (!file_) {
            co_await open_file(false);
        }
        require_write(
          co_await wait(
            file_->write(runtime::file_position{0}, stable_.share())));
        require(co_await wait(file_->flush()));
        if constexpr (
          Selected == file_case::flush || Selected == file_case::crash) {
            require_write(
              co_await wait(
                file_->write(runtime::file_position{0}, changed_.share())));
        }
        if constexpr (Selected == file_case::crash) {
            require(co_await wait(file_->close()));
            file_.reset();
        }
        bytes::fragmented_buffer write_input;
        if constexpr (Selected == file_case::write) {
            write_input = changed_.share();
        }
        require_quiescent();

        std::exception_ptr failure;
        perf_tests::start_measuring_time();
        try {
            if constexpr (Selected == file_case::read) {
                const auto read = co_await wait(file_->read(
                  runtime::file_position{0}, byte_count{file_payload_bytes}));
                if (
                  !read || read->eof()
                  || !read->data().content_equals(stable_)) {
                    throw std::runtime_error(
                      "fake file benchmark read mismatch");
                }
                perf_tests::do_not_optimize(read->data().size());
            } else if constexpr (Selected == file_case::write) {
                require_write(
                  co_await wait(file_->write(
                    runtime::file_position{0}, std::move(write_input))));
            } else if constexpr (Selected == file_case::flush) {
                require(co_await wait(file_->flush()));
            } else {
                require(co_await wait(files_->crash()));
            }
            require_quiescent();
        } catch (...) {
            failure = std::current_exception();
        }
        perf_tests::stop_measuring_time();
        if (failure) {
            std::rethrow_exception(failure);
        }
        if constexpr (Selected == file_case::crash) {
            ++generation_;
        }
        verify_state();
        co_return std::size_t{1};
    }

private:
    template<typename T>
    seastar::future<T> wait(seastar::future<T> pending) {
        co_await testing::pump_until(*events_, pending);
        co_return co_await std::move(pending);
    }

    template<typename T>
    static void require(const runtime::result<T>& outcome) {
        if (!outcome) {
            throw std::runtime_error(
              "fake file benchmark operation failed: "
              + outcome.error().render());
        }
    }

    static void require_write(const runtime::result<byte_count>& written) {
        if (!written || written->value() != file_payload_bytes) {
            throw std::runtime_error("fake file benchmark write mismatch");
        }
    }

    void create_environment() {
        const auto scheduler_budget = file_scheduler_limits();
        const auto trace_budget = file_trace_limits();
        trace_ = std::make_unique<event_trace>(
          trace_header::current(
            file_benchmark_seed,
            deterministic_random_algorithm_version,
            deterministic_random_coordinate_version,
            kwaque::simulation::trace_budget(scheduler_budget),
            trace_budget,
            trace_digest{},
            trace_digest{}),
          trace_budget);
        events_ = std::make_unique<scheduler>(scheduler_budget, trace_.get());
        auto faults = fault_schedule::make(
          *events_, *trace_, file_benchmark_seed, {});
        require(faults);
        faults_ = std::move(*faults);
        fake_file_system_config config;
        config.logical_capacity = byte_count{file_payload_bytes};
        config.maximum_objects = 4;
        config.maximum_operation_bytes = byte_count{file_payload_bytes};
        config.maximum_retained_path_bytes = byte_count{256};
        config.maximum_open_handles = 1;
        config.maximum_pending_operations = file_pending_operations;
        config.maximum_pending_bytes = byte_count{file_payload_bytes * 2U};
        config.maximum_pending_reads = 8;
        config.maximum_pending_writes = 8;
        config.memory_dma_alignment = 1;
        config.disk_read_dma_alignment = 1;
        config.disk_write_dma_alignment = 1;
        config.disk_overwrite_dma_alignment = 1;
        config.native_max_length = file_payload_bytes;
        auto files = fake_file_system::make(
          std::move(config), *events_, *faults_);
        require(files);
        files_ = std::move(*files);
        generation_ = 1;
    }

    seastar::future<> open_file(bool create) {
        auto opened = co_await wait(files_->open(
          benchmark_path("/kwaque/data/file"),
          {.access = runtime::file_access::read_write, .create = create}));
        require(opened);
        file_.emplace(std::move(*opened));
    }

    seastar::future<> initialize() {
        require(
          co_await wait(
            files_->create_directories(benchmark_path("/kwaque/data"))));
        require(
          co_await wait(files_->sync_directory(benchmark_path("/kwaque"))));
        co_await open_file(true);
        require_write(
          co_await wait(
            file_->write(runtime::file_position{0}, stable_.share())));
        require(co_await wait(file_->flush()));
        require(
          co_await wait(
            files_->sync_directory(benchmark_path("/kwaque/data"))));
        require_quiescent();
    }

    seastar::future<> stop_environment() {
        if (file_) {
            require(co_await wait(file_->close()));
            file_.reset();
        }
        if (files_) {
            require(co_await wait(files_->stop()));
            files_.reset();
        }
        faults_.reset();
        events_.reset();
        trace_.reset();
    }

    void require_quiescent() const {
        if (
          files_->pending_operations() != 0 || files_->pending_reads() != 0
          || files_->pending_writes() != 0 || files_->pending_bytes().value() != 0
          || events_->pending_events() != 0
          || (file_ && (file_->pending_reads() != 0 || file_->pending_read_bytes().value() != 0
                       || file_->pending_metadata_operations() != 0 || file_->queued_writes() != 0
                       || file_->queued_write_bytes().value() != 0))) {
            throw std::runtime_error(
              "fake file benchmark retained pending work");
        }
    }

    void verify_state() const {
        const auto snapshot = fake_file_test_access::snapshot(
          *files_,
          fake_file_snapshot_limits{
            .maximum_objects = 4,
            .maximum_dense_bytes = byte_count{file_payload_bytes * 2U},
          });
        require(snapshot);
        const auto selected = std::ranges::find_if(
          snapshot->objects, [](const auto& inode) {
              return inode.kind == fake_file_kind::regular;
          });
        constexpr char visible = Selected == file_case::write
                                     || Selected == file_case::flush
                                   ? 'b'
                                   : 'a';
        constexpr char durable = Selected == file_case::flush ? 'b' : 'a';
        if (
          snapshot->objects.size() != 3 || selected == snapshot->objects.end()
          || snapshot->retained_capacity != file_payload_bytes
          || snapshot->generation != generation_
          || snapshot->open_handles != (file_ ? 1U : 0U)
          || selected->visible_links != 1 || selected->durable_links != 1
          || selected->visible_bytes.size() != file_payload_bytes
          || selected->durable_bytes.size() != file_payload_bytes
          || !std::ranges::all_of(
            selected->visible_bytes,
            [](std::byte value) {
                return value == static_cast<std::byte>(visible);
            })
          || !std::ranges::all_of(selected->durable_bytes, [](std::byte value) {
                 return value == static_cast<std::byte>(durable);
             })) {
            throw std::runtime_error("fake file benchmark durability mismatch");
        }
    }

    bytes::fragmented_buffer stable_{benchmark_bytes('a')};
    bytes::fragmented_buffer changed_{benchmark_bytes('b')};
    std::unique_ptr<event_trace> trace_;
    std::unique_ptr<scheduler> events_;
    std::unique_ptr<fault_schedule> faults_;
    std::unique_ptr<fake_file_system> files_;
    std::optional<runtime::file> file_;
    std::uint64_t generation_{1};
};

using fake_file_read_fixture = fake_file_fixture<file_case::read>;
using fake_file_write_fixture = fake_file_fixture<file_case::write>;
using fake_file_flush_fixture = fake_file_fixture<file_case::flush>;
using fake_file_crash_fixture = fake_file_fixture<file_case::crash>;

} // namespace

PERF_TEST_F(fake_file_read_fixture, read4096) { return execute(); }
PERF_TEST_F(fake_file_write_fixture, overwrite4096) { return execute(); }
PERF_TEST_F(fake_file_flush_fixture, flush_dirty4096) { return execute(); }
PERF_TEST_F(fake_file_crash_fixture, discard_volatile4096) { return execute(); }

} // namespace kwaque::simulation
