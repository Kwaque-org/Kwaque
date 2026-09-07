#include "src/runtime/file.h"
#include "src/runtime/file_test_support.h"
#include "src/runtime/fragmented_buffer_internal.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/deleter.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>
#include <sys/stat.h>
#include <sys/uio.h>

#include <array>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <functional>
#include <limits>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace {

struct file_probe final {
    struct io_call final {
        std::uint64_t position;
        std::size_t size;
        std::uintptr_t address;
    };

    std::optional<seastar::promise<>> delayed_flush;
    std::optional<seastar::promise<std::size_t>> delayed_write;
    std::optional<seastar::promise<seastar::temporary_buffer<std::uint8_t>>>
      delayed_bulk_read;
    std::vector<io_call> writes;
    std::vector<io_call> reads;
    std::vector<std::size_t> bulk_read_sizes;
    std::vector<std::size_t> bulk_read_allocations;
    std::vector<std::uintptr_t> bulk_read_addresses;
    std::vector<char> storage;
    std::uint64_t size{0};
    std::uintptr_t bulk_read_address{0};
    std::size_t maximum_write_result{std::numeric_limits<std::size_t>::max()};
    std::size_t delayed_write_index{0};
    unsigned memory_alignment{4096};
    unsigned read_alignment{4096};
    unsigned write_alignment{4096};
    unsigned overwrite_alignment{4096};
    unsigned flushes{0};
    unsigned truncates{0};
    unsigned sizes{0};
    unsigned bulk_reads{0};
    unsigned closes{0};
    bool fail_flush{false};
    bool fail_allocation{false};
    bool delayed_write_consumed{false};
};

template<typename T>
[[noreturn]] T unexpected_file_call() {
    std::abort();
}

class probe_file_impl final : public seastar::file_impl {
public:
    explicit probe_file_impl(file_probe& probe) noexcept
      : probe_(probe) {
        _memory_dma_alignment = probe.memory_alignment;
        _disk_read_dma_alignment = probe.read_alignment;
        _disk_write_dma_alignment = probe.write_alignment;
        _disk_overwrite_dma_alignment = probe.overwrite_alignment;
    }

    seastar::future<std::size_t> write_dma(
      std::uint64_t position,
      const void* buffer,
      std::size_t size,
      seastar::io_intent*) final {
        probe_.writes.push_back(
          file_probe::io_call{
            .position = position,
            .size = size,
            .address = reinterpret_cast<std::uintptr_t>(buffer),
          });
        if (
          probe_.delayed_write && !probe_.delayed_write_consumed
          && probe_.writes.size() == probe_.delayed_write_index + 1U) {
            probe_.delayed_write_consumed = true;
            return probe_.delayed_write->get_future();
        }
        const auto written = std::min(size, probe_.maximum_write_result);
        const auto end = position + written;
        if (probe_.storage.size() < end) {
            probe_.storage.resize(static_cast<std::size_t>(end), '\0');
        }
        std::memcpy(
          probe_.storage.data() + static_cast<std::size_t>(position),
          buffer,
          written);
        probe_.size = std::max(probe_.size, end);
        return seastar::make_ready_future<std::size_t>(written);
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
        probe_.reads.push_back(
          file_probe::io_call{
            .position = position,
            .size = size,
            .address = reinterpret_cast<std::uintptr_t>(buffer),
          });
        const auto available = position < probe_.size ? probe_.size - position
                                                      : std::uint64_t{0};
        const auto read = std::min<std::uint64_t>(available, size);
        if (read != 0) {
            std::memcpy(
              buffer,
              probe_.storage.data() + static_cast<std::size_t>(position),
              static_cast<std::size_t>(read));
        }
        return seastar::make_ready_future<std::size_t>(
          static_cast<std::size_t>(read));
    }

    seastar::future<std::size_t>
    read_dma(std::uint64_t, std::vector<iovec>, seastar::io_intent*) final {
        return unexpected_file_call<seastar::future<std::size_t>>();
    }

    seastar::future<seastar::temporary_buffer<std::uint8_t>> dma_read_bulk(
      std::uint64_t position, std::size_t size, seastar::io_intent*) final {
        ++probe_.bulk_reads;
        probe_.bulk_read_sizes.push_back(size);
        if (probe_.delayed_bulk_read) {
            return probe_.delayed_bulk_read->get_future();
        }
        const auto available = position < probe_.size ? probe_.size - position
                                                      : std::uint64_t{0};
        const auto read = std::min<std::uint64_t>(available, size);
        const auto front = position & (probe_.read_alignment - 1U);
        const auto allocation = static_cast<std::size_t>(
          (size + front + probe_.read_alignment - 1U)
          & ~(probe_.read_alignment - 1U));
        probe_.bulk_read_allocations.push_back(allocation);
        auto result = seastar::temporary_buffer<std::uint8_t>::aligned(
          probe_.memory_alignment, allocation);
        probe_.bulk_read_address = reinterpret_cast<std::uintptr_t>(
          result.get());
        probe_.bulk_read_addresses.push_back(probe_.bulk_read_address);
        if (read != 0) {
            std::memcpy(
              result.get_write(),
              probe_.storage.data() + static_cast<std::size_t>(position),
              static_cast<std::size_t>(read));
        }
        result.trim(static_cast<std::size_t>(read));
        return seastar::make_ready_future<
          seastar::temporary_buffer<std::uint8_t>>(std::move(result));
    }

    seastar::future<> flush() final {
        ++probe_.flushes;
        if (probe_.fail_allocation) {
            return seastar::make_exception_future<>(std::bad_alloc{});
        }
        if (probe_.fail_flush) {
            return seastar::make_exception_future<>(std::system_error(
              std::make_error_code(std::errc::no_space_on_device)));
        }
        if (probe_.delayed_flush) {
            return probe_.delayed_flush->get_future();
        }
        return seastar::make_ready_future<>();
    }

    seastar::future<struct stat> stat() final {
        struct stat status{};
        return seastar::make_ready_future<struct stat>(status);
    }

    seastar::future<> truncate(std::uint64_t size) final {
        ++probe_.truncates;
        probe_.size = size;
        probe_.storage.resize(static_cast<std::size_t>(size), '\0');
        return seastar::make_ready_future<>();
    }

    seastar::future<> discard(std::uint64_t, std::uint64_t) final {
        return unexpected_file_call<seastar::future<>>();
    }

    seastar::future<> allocate(std::uint64_t, std::uint64_t) final {
        return unexpected_file_call<seastar::future<>>();
    }

    seastar::future<std::uint64_t> size() final {
        ++probe_.sizes;
        return seastar::make_ready_future<std::uint64_t>(probe_.size);
    }

    seastar::future<> close() final {
        ++probe_.closes;
        return seastar::make_ready_future<>();
    }

    seastar::subscription<seastar::directory_entry> list_directory(
      std::function<seastar::future<>(seastar::directory_entry)>) final {
        return unexpected_file_call<
          seastar::subscription<seastar::directory_entry>>();
    }

private:
    file_probe& probe_;
};

kwaque::runtime::file
make_file(file_probe& probe, kwaque::runtime::file_io_limits limits = {}) {
    return kwaque::runtime::file{
      seastar::file{seastar::make_shared<probe_file_impl>(probe)}, limits};
}

kwaque::bytes::fragmented_buffer aligned_data(std::size_t size, char value) {
    auto storage = seastar::temporary_buffer<char>::aligned(4096, size);
    std::memset(storage.get_write(), value, storage.size());
    return kwaque::runtime::detail::fragmented_buffer_io_access::adopt(
      std::move(storage), kwaque::byte_count{size});
}

std::string staging_contents(std::size_t size) {
    return std::string(size / 2U, 'a') + std::string(size - size / 2U, 'z');
}

kwaque::bytes::fragmented_buffer
staging_data(bool fragmented, std::size_t size) {
    using access = kwaque::runtime::detail::fragmented_buffer_io_access;
    if (fragmented) {
        kwaque::bytes::fragmented_buffer data;
        for (std::size_t offset = 0; offset < size;) {
            const auto count = std::min<std::size_t>(2048, size - offset);
            seastar::temporary_buffer<char> fragment{count};
            std::memset(
              fragment.get_write(), offset < size / 2U ? 'a' : 'z', count);
            if (!access::append_adopted(
                  data, std::move(fragment), kwaque::byte_count{count})) {
                throw std::runtime_error("staging input construction failed");
            }
            offset += count;
        }
        return data;
    }
    auto storage = seastar::temporary_buffer<char>::aligned(4096, size + 4096U);
    const auto retained = kwaque::byte_count{storage.size()};
    storage.trim_front(1);
    storage.trim(size);
    std::memset(storage.get_write(), 'a', size / 2U);
    std::memset(storage.get_write() + size / 2U, 'z', size - size / 2U);
    return access::adopt(std::move(storage), retained);
}

void complete_delayed_write(file_probe& probe, std::size_t written) {
    const auto& submitted = probe.writes[probe.delayed_write_index];
    const auto end = submitted.position + written;
    if (probe.storage.size() < end) {
        probe.storage.resize(static_cast<std::size_t>(end), '\0');
    }
    std::memcpy(
      probe.storage.data() + static_cast<std::size_t>(submitted.position),
      reinterpret_cast<const void*>(submitted.address),
      written);
    probe.size = std::max(probe.size, end);
    probe.delayed_write->set_value(written);
}

} // namespace

SEASTAR_TEST_CASE(file_owner_is_move_only_and_closes_native_once) {
    file_probe probe;
    auto original = make_file(probe);
    auto moved = std::move(original);

    // The contract deliberately makes a moved-from owner canonically closed.
    const auto moved_from_close
      = co_await original.close(); // NOLINT(bugprone-use-after-move)
    const auto first_close = co_await moved.close();
    const auto second_close = co_await moved.close();

    BOOST_REQUIRE(moved_from_close.has_value());
    BOOST_REQUIRE(first_close.has_value());
    BOOST_REQUIRE(second_close.has_value());
    BOOST_CHECK_EQUAL(probe.closes, 1U);
    BOOST_CHECK(moved.state() == kwaque::runtime::file_state::closed);
}

SEASTAR_TEST_CASE(file_owner_moves_safely_after_completed_direct_use) {
    file_probe probe;
    auto original = make_file(probe);
    auto data = aligned_data(4096, 'm');
    const auto written = co_await original.write(
      kwaque::runtime::file_position{0}, std::move(data));
    BOOST_REQUIRE(written.has_value());

    auto moved = std::move(original);
    const auto closed = co_await moved.close();
    BOOST_REQUIRE(closed.has_value());
    BOOST_CHECK_EQUAL(probe.closes, 1U);
}

SEASTAR_TEST_CASE(file_owner_move_invariant_tracks_a_direct_write) {
    file_probe probe;
    probe.delayed_write.emplace();
    auto owner = make_file(probe);
    auto writing = owner.write(
      kwaque::runtime::file_position{0}, aligned_data(4096, 'p'));
    BOOST_CHECK(!writing.available());
    BOOST_CHECK(!kwaque::runtime::file_test_access::move_is_idle(owner));

    probe.delayed_write->set_value(4096);
    const auto written = co_await std::move(writing);
    BOOST_REQUIRE(written.has_value());
    BOOST_CHECK(kwaque::runtime::file_test_access::move_is_idle(owner));

    auto moved = std::move(owner);
    const auto closed = co_await moved.close();
    BOOST_REQUIRE(closed.has_value());
}

SEASTAR_TEST_CASE(file_close_drains_an_inflight_native_operation) {
    file_probe probe;
    probe.delayed_flush.emplace();
    auto owner = make_file(probe);

    auto flushing = owner.flush();
    auto closing = owner.close();
    const bool close_waited = !closing.available() && probe.closes == 0;

    probe.delayed_flush->set_value();
    const auto flush_result = co_await std::move(flushing);
    const auto close_result = co_await std::move(closing);

    BOOST_CHECK(close_waited);
    BOOST_REQUIRE(flush_result.has_value());
    BOOST_REQUIRE(close_result.has_value());
    BOOST_CHECK_EQUAL(probe.flushes, 1U);
    BOOST_CHECK_EQUAL(probe.closes, 1U);
}

SEASTAR_TEST_CASE(file_metadata_admission_bounds_native_operations) {
    file_probe probe;
    probe.delayed_flush.emplace();
    auto owner = make_file(
      probe,
      {.pending_read_bytes = kwaque::byte_count{4096},
       .pending_reads = 1,
       .pending_metadata_operations = 1,
       .queued_write_bytes = kwaque::byte_count{4096},
       .queued_writes = 1});

    auto active = owner.flush();
    BOOST_CHECK(!active.available());
    BOOST_CHECK_EQUAL(owner.pending_metadata_operations(), 1U);
    const auto saturated = co_await owner.size();
    BOOST_REQUIRE(!saturated.has_value());
    BOOST_CHECK(saturated.error().code() == kwaque::errc::queue_full);

    probe.delayed_flush->set_value();
    const auto completed = co_await std::move(active);
    BOOST_REQUIRE(completed.has_value());
    BOOST_CHECK_EQUAL(owner.pending_metadata_operations(), 0U);
    const auto closed = co_await owner.close();
    BOOST_REQUIRE(closed.has_value());
}

SEASTAR_TEST_CASE(file_close_drains_an_inflight_direct_write) {
    file_probe probe;
    probe.delayed_write.emplace();
    auto owner = make_file(probe);
    auto data = aligned_data(4096, 'w');

    auto writing = owner.write(
      kwaque::runtime::file_position{0}, std::move(data));
    auto closing = owner.close();
    const bool close_waited = !closing.available() && probe.closes == 0;

    probe.delayed_write->set_value(4096);
    const auto write_result = co_await std::move(writing);
    const auto close_result = co_await std::move(closing);

    BOOST_CHECK(close_waited);
    BOOST_REQUIRE(write_result.has_value());
    BOOST_REQUIRE(close_result.has_value());
    BOOST_CHECK_EQUAL(probe.closes, 1U);
}

SEASTAR_TEST_CASE(file_owner_translates_expected_dependency_failures) {
    file_probe probe;
    probe.fail_flush = true;
    auto owner = make_file(probe);

    const auto flush_result = co_await owner.flush();
    const auto close_result = co_await owner.close();

    BOOST_REQUIRE(!flush_result.has_value());
    BOOST_CHECK(
      flush_result.error().code() == kwaque::errc::resource_exhausted);
    BOOST_REQUIRE(close_result.has_value());
    BOOST_CHECK_EQUAL(probe.closes, 1U);
}

SEASTAR_TEST_CASE(file_owner_preserves_allocation_failure_as_exceptional) {
    file_probe probe;
    probe.fail_allocation = true;
    auto owner = make_file(probe);

    bool observed = false;
    try {
        static_cast<void>(co_await owner.flush());
    } catch (const std::bad_alloc&) {
        observed = true;
    }
    const auto close_result = co_await owner.close();

    BOOST_CHECK(observed);
    BOOST_REQUIRE(close_result.has_value());
    BOOST_CHECK_EQUAL(probe.closes, 1U);
}

SEASTAR_TEST_CASE(file_owner_supports_typed_metadata_operations) {
    file_probe probe;
    auto owner = make_file(probe);

    const auto truncated = co_await owner.truncate(8192);
    const auto observed_size = co_await owner.size();
    owner.request_abort();
    const auto rejected_size = co_await owner.size();
    const auto close_result = co_await owner.close();

    BOOST_REQUIRE(truncated.has_value());
    BOOST_REQUIRE(observed_size.has_value());
    BOOST_CHECK_EQUAL(*observed_size, 8192U);
    BOOST_REQUIRE(!rejected_size.has_value());
    BOOST_CHECK(rejected_size.error().code() == kwaque::errc::aborted);
    BOOST_REQUIRE(close_result.has_value());
    BOOST_CHECK_EQUAL(probe.truncates, 1U);
    BOOST_CHECK_EQUAL(probe.closes, 1U);
}

SEASTAR_TEST_CASE(file_read_adopts_exact_native_bulk_without_payload_copy) {
    file_probe probe;
    probe.storage.assign(4096, 'r');
    probe.size = probe.storage.size();
    std::optional<kwaque::runtime::file_read_result> retained;
    {
        auto owner = make_file(probe);
        auto read = co_await owner.read(
          kwaque::runtime::file_position{0}, kwaque::byte_count{4096});
        const auto close_result = co_await owner.close();

        BOOST_REQUIRE(read.has_value());
        BOOST_CHECK(!read->eof());
        BOOST_CHECK_EQUAL(read->data().size().value(), 4096U);
        const auto fragment = read->data().fragment_at(0);
        BOOST_REQUIRE(fragment.has_value());
        BOOST_CHECK_EQUAL(
          reinterpret_cast<std::uintptr_t>(fragment->data()),
          probe.bulk_read_address);
        BOOST_REQUIRE(close_result.has_value());
        retained.emplace(std::move(*read));
    }

    BOOST_REQUIRE(retained.has_value());
    BOOST_CHECK(retained->data().content_equals(std::string(4096, 'r')));
    BOOST_CHECK_EQUAL(probe.bulk_reads, 1U);
}

SEASTAR_TEST_CASE(file_read_reports_short_and_empty_eof_with_owned_bytes) {
    file_probe probe;
    probe.storage.assign(37, 'e');
    probe.size = probe.storage.size();
    auto owner = make_file(probe);

    auto short_read = co_await owner.read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{4096});
    auto empty_read = co_await owner.read(
      kwaque::runtime::file_position{probe.size}, kwaque::byte_count{4096});
    const auto close_result = co_await owner.close();

    BOOST_REQUIRE(short_read.has_value());
    BOOST_CHECK(short_read->eof());
    BOOST_CHECK(short_read->data().content_equals(std::string(37, 'e')));
    BOOST_REQUIRE(empty_read.has_value());
    BOOST_CHECK(empty_read->eof());
    BOOST_CHECK(empty_read->data().empty());
    BOOST_CHECK_EQUAL(probe.bulk_reads, 2U);
    BOOST_REQUIRE(close_result.has_value());
}

SEASTAR_TEST_CASE(file_read_chunks_large_logical_requests) {
    file_probe probe;
    const auto total = 2U * kwaque::maximum_contiguous_allocation_bytes + 37U;
    probe.storage.assign(total, 'c');
    probe.size = probe.storage.size();
    auto owner = make_file(probe);

    auto read = co_await owner.read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{total});
    const auto closed = co_await owner.close();

    BOOST_REQUIRE(read.has_value());
    BOOST_CHECK(!read->eof());
    BOOST_CHECK_EQUAL(read->data().size().value(), total);
    BOOST_CHECK_EQUAL(read->data().fragment_count(), 3U);
    BOOST_REQUIRE_EQUAL(probe.bulk_read_sizes.size(), 3U);
    BOOST_REQUIRE_EQUAL(probe.bulk_read_addresses.size(), 3U);
    for (std::size_t index = 0; index < probe.bulk_read_sizes.size(); ++index) {
        BOOST_CHECK_LE(
          probe.bulk_read_sizes[index],
          kwaque::maximum_contiguous_allocation_bytes);
        const auto fragment = read->data().fragment_at(index);
        BOOST_REQUIRE(fragment.has_value());
        BOOST_CHECK_EQUAL(
          reinterpret_cast<std::uintptr_t>(fragment->data()),
          probe.bulk_read_addresses[index]);
    }
    BOOST_REQUIRE(closed.has_value());
}

SEASTAR_TEST_CASE(
  file_read_caps_unaligned_native_allocations_and_tracks_backing) {
    file_probe probe;
    const auto limit = kwaque::maximum_contiguous_allocation_bytes;
    probe.storage.assign(limit + 1U, 'u');
    probe.size = probe.storage.size();
    auto owner = make_file(probe);

    auto read = co_await owner.read(
      kwaque::runtime::file_position{1}, kwaque::byte_count{limit});
    const auto closed = co_await owner.close();

    BOOST_REQUIRE(read.has_value());
    BOOST_CHECK(!read->eof());
    BOOST_CHECK_EQUAL(read->data().size().value(), limit);
    BOOST_CHECK_EQUAL(
      read->data().retained_bytes().value(), limit + probe.read_alignment);
    BOOST_REQUIRE_EQUAL(probe.bulk_read_sizes.size(), 2U);
    BOOST_REQUIRE_EQUAL(probe.bulk_read_allocations.size(), 2U);
    BOOST_CHECK_EQUAL(probe.bulk_read_sizes[0], limit - 1U);
    BOOST_CHECK_EQUAL(probe.bulk_read_sizes[1], 1U);
    for (const auto allocation : probe.bulk_read_allocations) {
        BOOST_CHECK_LE(allocation, limit);
    }
    BOOST_REQUIRE(closed.has_value());
}

SEASTAR_TEST_CASE(file_read_rejects_bounds_and_abort_before_native_dispatch) {
    file_probe probe;
    auto owner = make_file(probe);

    const auto empty_limit = co_await owner.read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{});
    const auto oversized = co_await owner.read(
      kwaque::runtime::file_position{0},
      kwaque::byte_count{kwaque::runtime::maximum_file_io_bytes.value() + 1});
    owner.request_abort();
    const auto aborted = co_await owner.read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{1});
    const auto close_result = co_await owner.close();

    BOOST_REQUIRE(!empty_limit.has_value());
    BOOST_CHECK(empty_limit.error().code() == kwaque::errc::invalid_argument);
    BOOST_REQUIRE(!oversized.has_value());
    BOOST_CHECK(oversized.error().code() == kwaque::errc::out_of_range);
    BOOST_REQUIRE(!aborted.has_value());
    BOOST_CHECK(aborted.error().code() == kwaque::errc::aborted);
    BOOST_CHECK_EQUAL(probe.bulk_reads, 0U);
    BOOST_REQUIRE(close_result.has_value());
}

SEASTAR_TEST_CASE(file_read_admission_bounds_count_and_requested_bytes) {
    file_probe probe;
    probe.delayed_bulk_read.emplace();
    auto owner = make_file(
      probe,
      {.pending_read_bytes = kwaque::byte_count{8},
       .pending_reads = 1,
       .queued_write_bytes = kwaque::byte_count{4096},
       .queued_writes = 1});

    auto active = owner.read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{8});
    BOOST_CHECK(!active.available());
    BOOST_CHECK_EQUAL(owner.pending_reads(), 1U);
    BOOST_CHECK_EQUAL(owner.pending_read_bytes().value(), 8U);

    const auto count_saturated = co_await owner.read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{1});
    const auto request_too_large = co_await owner.read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{9});
    BOOST_REQUIRE(!count_saturated.has_value());
    BOOST_CHECK(count_saturated.error().code() == kwaque::errc::queue_full);
    BOOST_REQUIRE(!request_too_large.has_value());
    BOOST_CHECK(request_too_large.error().code() == kwaque::errc::out_of_range);
    BOOST_CHECK_EQUAL(probe.bulk_reads, 1U);

    probe.delayed_bulk_read->set_value(
      seastar::temporary_buffer<std::uint8_t>{});
    const auto completed = co_await std::move(active);
    BOOST_REQUIRE(completed.has_value());
    BOOST_CHECK(completed->eof());
    BOOST_CHECK_EQUAL(owner.pending_reads(), 0U);
    BOOST_CHECK_EQUAL(owner.pending_read_bytes().value(), 0U);
    const auto closed = co_await owner.close();
    BOOST_REQUIRE(closed.has_value());
}

SEASTAR_TEST_CASE(file_write_dispatches_aligned_owned_storage_directly) {
    file_probe probe;
    auto owner = make_file(probe);
    auto data = aligned_data(4096, 'd');
    const auto source_fragment = data.fragment_at(0);
    BOOST_REQUIRE(source_fragment.has_value());
    const auto source_address = reinterpret_cast<std::uintptr_t>(
      source_fragment->data());

    const auto written = co_await owner.write(
      kwaque::runtime::file_position{0}, std::move(data));
    const auto close_result = co_await owner.close();

    BOOST_REQUIRE(written.has_value());
    BOOST_CHECK_EQUAL(written->value(), 4096U);
    BOOST_REQUIRE_EQUAL(probe.writes.size(), 1U);
    BOOST_CHECK_EQUAL(probe.writes.front().position, 0U);
    BOOST_CHECK_EQUAL(probe.writes.front().size, 4096U);
    BOOST_CHECK_EQUAL(probe.writes.front().address, source_address);
    BOOST_CHECK_EQUAL(probe.sizes, 0U);
    BOOST_CHECK(
      std::string_view(probe.storage.data(), probe.storage.size())
      == std::string(4096, 'd'));
    BOOST_REQUIRE(close_result.has_value());
}

SEASTAR_TEST_CASE(file_write_stages_only_the_unaligned_native_chunk) {
    file_probe probe;
    auto owner = make_file(probe);
    const std::string source(4097, 's');
    auto backing = kwaque::bytes::fragmented_buffer::copy_of(
      std::span<const char>{source});
    BOOST_REQUIRE(backing.has_value());
    auto unaligned = backing->share(
      kwaque::byte_count{1}, kwaque::byte_count{4096});
    BOOST_REQUIRE(unaligned.has_value());
    const auto source_fragment = unaligned->fragment_at(0);
    BOOST_REQUIRE(source_fragment.has_value());
    const auto source_address = reinterpret_cast<std::uintptr_t>(
      source_fragment->data());

    const auto written = co_await owner.write(
      kwaque::runtime::file_position{0}, std::move(*unaligned));
    const auto close_result = co_await owner.close();

    BOOST_REQUIRE(written.has_value());
    BOOST_REQUIRE_EQUAL(probe.writes.size(), 1U);
    BOOST_CHECK_EQUAL(probe.writes.front().size, 4096U);
    BOOST_CHECK_EQUAL(probe.writes.front().address % 4096U, 0U);
    BOOST_CHECK_NE(probe.writes.front().address, source_address);
    BOOST_CHECK_EQUAL(probe.sizes, 0U);
    BOOST_CHECK(
      std::string_view(probe.storage.data(), probe.storage.size())
      == std::string(4096, 's'));
    BOOST_REQUIRE(close_result.has_value());
}

SEASTAR_TEST_CASE(file_write_coalesces_fragment_batch_without_native_iovecs) {
    file_probe probe;
    auto owner = make_file(probe);
    std::array<seastar::temporary_buffer<char>, 64> fragments;
    for (std::size_t index = 0; index < fragments.size(); ++index) {
        fragments[index] = seastar::temporary_buffer<char>(64);
        std::memset(
          fragments[index].get_write(),
          static_cast<int>('a' + index % 26),
          fragments[index].size());
    }
    auto data = kwaque::bytes::fragmented_buffer::copy_from_fragments(
      std::span<const seastar::temporary_buffer<char>>{fragments});
    BOOST_REQUIRE(data.has_value());
    BOOST_CHECK_EQUAL(data->fragment_count(), 64U);

    const auto written = co_await owner.write(
      kwaque::runtime::file_position{0}, std::move(*data));
    const auto close_result = co_await owner.close();

    BOOST_REQUIRE(written.has_value());
    BOOST_REQUIRE_EQUAL(probe.writes.size(), 1U);
    BOOST_CHECK_EQUAL(probe.writes.front().size, 4096U);
    BOOST_CHECK_EQUAL(probe.sizes, 0U);
    BOOST_CHECK_EQUAL(probe.storage.size(), 4096U);
    BOOST_REQUIRE(close_result.has_value());
}

SEASTAR_TEST_CASE(file_aligned_fragments_keep_independent_native_dma_storage) {
    using access = kwaque::runtime::detail::fragmented_buffer_io_access;
    file_probe probe;
    probe.storage.assign(4096, 'p');
    probe.size = probe.storage.size();
    auto owner = make_file(probe);
    auto first = seastar::temporary_buffer<char>::aligned(4096, 4096);
    auto second = seastar::temporary_buffer<char>::aligned(4096, 4096);
    std::memset(first.get_write(), 'a', first.size());
    std::memset(second.get_write(), 'z', second.size());
    const auto first_address = reinterpret_cast<std::uintptr_t>(first.get());
    const auto second_address = reinterpret_cast<std::uintptr_t>(second.get());
    auto data = access::adopt(std::move(first), kwaque::byte_count{4096});
    const auto appended = access::append_adopted(
      data, std::move(second), kwaque::byte_count{4096});
    BOOST_REQUIRE(appended.has_value());
    const auto written = co_await owner.write(
      kwaque::runtime::file_position{4096}, std::move(data));
    const auto closed = co_await owner.close();
    BOOST_REQUIRE(written.has_value());
    BOOST_CHECK_EQUAL(written->value(), 8192U);
    BOOST_REQUIRE_EQUAL(probe.writes.size(), 2U);
    BOOST_CHECK_EQUAL(probe.writes[0].position, 4096U);
    BOOST_CHECK_EQUAL(probe.writes[1].position, 8192U);
    BOOST_CHECK_EQUAL(probe.writes[0].size, 4096U);
    BOOST_CHECK_EQUAL(probe.writes[1].size, 4096U);
    BOOST_CHECK_EQUAL(probe.writes[0].address, first_address);
    BOOST_CHECK_EQUAL(probe.writes[1].address, second_address);
    BOOST_CHECK(probe.reads.empty());
    BOOST_CHECK_EQUAL(probe.sizes, 0U);
    BOOST_CHECK_EQUAL(probe.truncates, 0U);
    BOOST_CHECK(
      std::string_view(probe.storage.data(), probe.storage.size())
      == std::string(4096, 'p') + staging_contents(8192));
    BOOST_REQUIRE(closed.has_value());
}

SEASTAR_TEST_CASE(file_staging_alignment_padding_is_not_written) {
    for (const bool fragmented : {false, true}) {
        file_probe probe;
        probe.memory_alignment = 8192;
        probe.storage.assign(12288, 'p');
        probe.size = probe.storage.size();
        auto owner = make_file(probe);
        const auto written = co_await owner.write(
          kwaque::runtime::file_position{4096}, staging_data(fragmented, 4096));
        const auto closed = co_await owner.close();
        BOOST_REQUIRE(written.has_value());
        BOOST_CHECK_EQUAL(written->value(), 4096U);
        BOOST_REQUIRE_EQUAL(probe.writes.size(), 1U);
        BOOST_CHECK_EQUAL(probe.writes[0].position, 4096U);
        BOOST_CHECK_EQUAL(probe.writes[0].size, 4096U);
        BOOST_CHECK_EQUAL(probe.writes[0].address % 8192U, 0U);
        BOOST_CHECK_EQUAL(probe.size, 12288U);
        BOOST_CHECK(probe.reads.empty());
        BOOST_CHECK_EQUAL(probe.sizes, 0U);
        BOOST_CHECK_EQUAL(probe.truncates, 0U);
        BOOST_CHECK_EQUAL(owner.statistics().completed_bytes, 4096U);
        BOOST_CHECK(
          std::string_view(probe.storage.data(), probe.storage.size())
          == std::string(4096, 'p') + staging_contents(4096)
               + std::string(4096, 'p'));
        BOOST_REQUIRE(closed.has_value());
    }
}

SEASTAR_TEST_CASE(file_staging_observes_abort_from_released_source_ownership) {
    file_probe probe;
    auto owner = make_file(probe);
    bool source_released = false;
    auto backing = seastar::temporary_buffer<char>::aligned(4096, 8192);
    std::memset(backing.get_write(), 's', backing.size());
    auto* source = backing.get_write() + 1;
    auto fragment = seastar::temporary_buffer<char>::maybe_unsafe_from_deleter(
      source,
      4096,
      seastar::make_deleter(
        [backing = std::move(backing), &owner, &source_released] noexcept {
            static_cast<void>(backing);
            source_released = true;
            owner.request_abort();
        }));
    auto data = kwaque::runtime::detail::fragmented_buffer_io_access::adopt(
      std::move(fragment), kwaque::byte_count{8192});
    const auto written = co_await owner.write(
      kwaque::runtime::file_position{0}, std::move(data));
    const auto statistics = owner.statistics();
    const bool source_aborted = owner.abort_requested();
    const bool admission_idle = kwaque::runtime::file_test_access::move_is_idle(
      owner);
    const auto closed = co_await owner.close();
    BOOST_CHECK(source_released);
    BOOST_CHECK(source_aborted);
    BOOST_REQUIRE(!written.has_value());
    BOOST_CHECK(written.error().code() == kwaque::errc::aborted);
    BOOST_CHECK(probe.writes.empty());
    BOOST_CHECK_EQUAL(statistics.accepted, 1U);
    BOOST_CHECK_EQUAL(statistics.completed, 1U);
    BOOST_CHECK_EQUAL(statistics.active, 0U);
    BOOST_CHECK_EQUAL(statistics.completed_bytes, 0U);
    BOOST_CHECK(admission_idle);
    BOOST_REQUIRE(closed.has_value());
    BOOST_CHECK_EQUAL(probe.closes, 1U);
}

SEASTAR_TEST_CASE(file_ready_staging_allocates_only_one_native_buffer) {
    for (const bool fragmented : {false, true}) {
        file_probe probe;
        probe.writes.reserve(1);
        probe.storage.assign(4096, '\0');
        auto owner = make_file(probe);
        auto data = staging_data(fragmented, 4096);
        const auto expected = staging_contents(4096);
#if !defined(SEASTAR_DEBUG) && !defined(SEASTAR_DEFAULT_ALLOCATOR)
        const auto before = seastar::memory::stats().mallocs();
#endif
        auto writing = owner.write(
          kwaque::runtime::file_position{0}, std::move(data));
#if !defined(SEASTAR_DEBUG) && !defined(SEASTAR_DEFAULT_ALLOCATOR)
        const auto after = seastar::memory::stats().mallocs();
        BOOST_CHECK_EQUAL(after - before, 1U);
#endif
#if !defined(SEASTAR_DEBUG)
        BOOST_CHECK(writing.available());
#endif
        const auto written = co_await std::move(writing);
        const auto closed = co_await owner.close();
        BOOST_REQUIRE(written.has_value());
        BOOST_CHECK_EQUAL(written->value(), 4096U);
        BOOST_REQUIRE_EQUAL(probe.writes.size(), 1U);
        BOOST_CHECK_EQUAL(probe.writes[0].size, 4096U);
        BOOST_CHECK_EQUAL(probe.writes[0].address % 4096U, 0U);
        BOOST_CHECK_EQUAL(probe.sizes, 0U);
        BOOST_CHECK_EQUAL(probe.truncates, 0U);
        BOOST_CHECK(
          std::string_view(probe.storage.data(), probe.storage.size())
          == expected);
        BOOST_REQUIRE(closed.has_value());
    }
}

SEASTAR_TEST_CASE(file_pending_staging_retains_bytes_and_serializes_truncate) {
    for (const bool fragmented : {false, true}) {
        file_probe probe;
        probe.delayed_write.emplace();
        probe.storage.assign(8192, 'p');
        probe.size = probe.storage.size();
        auto owner = make_file(probe);
        auto writing = owner.write(
          kwaque::runtime::file_position{4096}, staging_data(fragmented, 4096));
        auto truncating = owner.truncate(8192);
        const bool serialization_held
          = !writing.available() && !truncating.available()
            && probe.truncates == 0
            && !kwaque::runtime::file_test_access::move_is_idle(owner);
        co_await seastar::yield();
        BOOST_REQUIRE_EQUAL(probe.writes.size(), 1U);
        complete_delayed_write(probe, 4096);
        const auto written = co_await std::move(writing);
        const auto truncated = co_await std::move(truncating);
        const auto closed = co_await owner.close();
        BOOST_CHECK(serialization_held);
        BOOST_REQUIRE(written.has_value());
        BOOST_CHECK_EQUAL(written->value(), 4096U);
        BOOST_REQUIRE(truncated.has_value());
        BOOST_CHECK_EQUAL(probe.writes.size(), 1U);
        BOOST_CHECK_EQUAL(probe.truncates, 1U);
        BOOST_CHECK(
          std::string_view(probe.storage.data(), probe.storage.size())
          == std::string(4096, 'p') + staging_contents(4096));
        BOOST_REQUIRE(closed.has_value());
    }
}

SEASTAR_TEST_CASE(file_close_and_abort_drain_pending_staging) {
    for (const bool fragmented : {false, true}) {
        for (const bool abort : {false, true}) {
            file_probe probe;
            probe.delayed_write.emplace();
            auto owner = make_file(probe);
            auto writing = owner.write(
              kwaque::runtime::file_position{0},
              staging_data(fragmented, 4096));
            if (abort) {
                owner.request_abort();
                const auto rejected = co_await owner.size();
                BOOST_CHECK(!rejected.has_value());
                if (!rejected) {
                    BOOST_CHECK(
                      rejected.error().code() == kwaque::errc::aborted);
                }
            }
            auto closing = owner.close();
            const bool close_waited = !writing.available()
                                      && !closing.available()
                                      && probe.closes == 0;
            co_await seastar::yield();
            BOOST_REQUIRE_EQUAL(probe.writes.size(), 1U);
            complete_delayed_write(probe, 4096);
            const auto written = co_await std::move(writing);
            const auto closed = co_await std::move(closing);
            BOOST_CHECK(close_waited);
            BOOST_REQUIRE(written.has_value());
            BOOST_CHECK_EQUAL(written->value(), 4096U);
            BOOST_CHECK_EQUAL(owner.statistics().active, 0U);
            BOOST_CHECK_EQUAL(owner.statistics().completed_bytes, 4096U);
            BOOST_CHECK_EQUAL(probe.closes, 1U);
            BOOST_CHECK(
              std::string_view(probe.storage.data(), probe.storage.size())
              == staging_contents(4096));
            BOOST_REQUIRE(closed.has_value());
        }
    }
}

SEASTAR_TEST_CASE(
  file_staged_short_writes_recover_with_required_memory_alignment) {
    for (const bool fragmented : {false, true}) {
        for (const unsigned memory_alignment : {4096U, 8192U}) {
            file_probe probe;
            probe.memory_alignment = memory_alignment;
            probe.maximum_write_result = 4096;
            probe.delayed_write.emplace();
            probe.delayed_write_index = 1;
            probe.storage.assign(4096, 'p');
            probe.size = probe.storage.size();
            auto owner = make_file(probe);
            auto writing = owner.write(
              kwaque::runtime::file_position{4096},
              staging_data(fragmented, 8192));
            while (!probe.delayed_write_consumed && !writing.available()) {
                co_await seastar::yield();
            }
            auto closing = owner.close();
            const bool recovery_held = !writing.available()
                                       && !closing.available()
                                       && probe.closes == 0;
            if (probe.delayed_write_consumed) {
                complete_delayed_write(probe, 4096);
            }
            const auto written = co_await std::move(writing);
            const auto closed = co_await std::move(closing);
            BOOST_CHECK(recovery_held);
            BOOST_REQUIRE(written.has_value());
            BOOST_CHECK_EQUAL(written->value(), 8192U);
            BOOST_REQUIRE_EQUAL(probe.writes.size(), 2U);
            BOOST_CHECK_EQUAL(probe.writes[0].position, 4096U);
            BOOST_CHECK_EQUAL(probe.writes[0].size, 8192U);
            BOOST_CHECK_EQUAL(probe.writes[1].position, 8192U);
            BOOST_CHECK_EQUAL(probe.writes[1].size, 4096U);
            BOOST_CHECK_EQUAL(probe.writes[0].address % memory_alignment, 0U);
            BOOST_CHECK_EQUAL(probe.writes[1].address % memory_alignment, 0U);
            if (memory_alignment == 8192U) {
                BOOST_CHECK_NE(
                  probe.writes[1].address, probe.writes[0].address + 4096U);
            }
            BOOST_CHECK_EQUAL(probe.sizes, 0U);
            BOOST_CHECK_EQUAL(probe.truncates, 0U);
            BOOST_CHECK_EQUAL(owner.statistics().active, 0U);
            BOOST_CHECK_EQUAL(owner.statistics().completed_bytes, 8192U);
            BOOST_CHECK(
              std::string_view(probe.storage.data(), probe.storage.size())
              == std::string(4096, 'p') + staging_contents(8192));
            BOOST_REQUIRE(closed.has_value());
        }
    }
}

SEASTAR_TEST_CASE(
  file_staging_allocation_failure_releases_admission_for_retry) {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    for (const bool fragmented : {false, true}) {
        file_probe probe;
        auto owner = make_file(probe);
        auto data = staging_data(fragmented, 4096);
        std::optional<
          seastar::future<kwaque::runtime::result<kwaque::byte_count>>>
          writing;
        bool escaped = false;
        auto& injector = seastar::memory::local_failure_injector();
        injector.fail_after(0);
        try {
            writing.emplace(
              owner.write(kwaque::runtime::file_position{0}, std::move(data)));
        } catch (...) {
            escaped = true;
        }
        const bool injected = injector.failed();
        injector.cancel();
        BOOST_CHECK(injected);
        BOOST_CHECK(!escaped);
        if (writing) {
            BOOST_REQUIRE(writing->available());
            BOOST_CHECK_THROW(std::move(*writing).get(), std::bad_alloc);
        }
        BOOST_CHECK(probe.writes.empty());
        BOOST_CHECK(kwaque::runtime::file_test_access::move_is_idle(owner));
        BOOST_CHECK_EQUAL(owner.queued_writes(), 0U);
        BOOST_CHECK_EQUAL(owner.queued_write_bytes().value(), 0U);
        const auto statistics = owner.statistics();
        BOOST_CHECK_EQUAL(statistics.active, 0U);
        BOOST_CHECK_EQUAL(statistics.completed, statistics.accepted);
        const auto retried = co_await owner.write(
          kwaque::runtime::file_position{0}, staging_data(fragmented, 4096));
        const auto closed = co_await owner.close();
        BOOST_REQUIRE(retried.has_value());
        BOOST_CHECK_EQUAL(retried->value(), 4096U);
        BOOST_CHECK_EQUAL(probe.writes.size(), 1U);
        BOOST_CHECK(
          std::string_view(probe.storage.data(), probe.storage.size())
          == staging_contents(4096));
        BOOST_REQUIRE(closed.has_value());
    }
#endif
    co_return;
}

SEASTAR_TEST_CASE(file_write_staging_never_exceeds_contiguous_ceiling) {
    file_probe probe;
    auto owner = make_file(probe);
    std::array<seastar::temporary_buffer<char>, 1024> fragments;
    for (auto& fragment : fragments) {
        fragment = seastar::temporary_buffer<char>(256);
        std::memset(fragment.get_write(), 'b', fragment.size());
    }
    auto data = kwaque::bytes::fragmented_buffer::copy_from_fragments(
      std::span<const seastar::temporary_buffer<char>>{fragments});
    BOOST_REQUIRE(data.has_value());

    const auto written = co_await owner.write(
      kwaque::runtime::file_position{0}, std::move(*data));
    const auto closed = co_await owner.close();

    BOOST_REQUIRE(written.has_value());
    BOOST_REQUIRE_EQUAL(probe.writes.size(), 2U);
    for (const auto& write : probe.writes) {
        BOOST_CHECK_LE(write.size, kwaque::maximum_contiguous_allocation_bytes);
    }
    BOOST_REQUIRE(closed.has_value());
}

SEASTAR_TEST_CASE(file_write_preserves_bytes_around_unaligned_overwrite) {
    file_probe probe;
    probe.storage.assign(8192, 'p');
    probe.size = probe.storage.size();
    auto owner = make_file(probe);
    auto data = kwaque::bytes::fragmented_buffer::copy_of(
      std::span<const char>{"kwaque", 6});
    BOOST_REQUIRE(data.has_value());

    const auto written = co_await owner.write(
      kwaque::runtime::file_position{101}, std::move(*data));
    const auto close_result = co_await owner.close();

    BOOST_REQUIRE(written.has_value());
    BOOST_REQUIRE_EQUAL(probe.reads.size(), 1U);
    BOOST_REQUIRE_EQUAL(probe.writes.size(), 1U);
    BOOST_CHECK_EQUAL(probe.reads.front().position, 0U);
    BOOST_CHECK_EQUAL(probe.reads.front().size, 4096U);
    BOOST_CHECK_EQUAL(probe.writes.front().position, 0U);
    BOOST_CHECK_EQUAL(probe.writes.front().size, 4096U);
    BOOST_CHECK_EQUAL(probe.sizes, 1U);
    BOOST_CHECK_EQUAL(probe.truncates, 0U);
    BOOST_CHECK_EQUAL(probe.storage[100], 'p');
    BOOST_CHECK(std::string_view(probe.storage.data() + 101, 6) == "kwaque");
    BOOST_CHECK_EQUAL(probe.storage[107], 'p');
    BOOST_CHECK_EQUAL(probe.storage.size(), 8192U);
    BOOST_REQUIRE(close_result.has_value());
}

SEASTAR_TEST_CASE(file_write_rejects_an_overflowing_aligned_physical_span) {
    file_probe probe;
    auto owner = make_file(probe);
    auto data = kwaque::bytes::fragmented_buffer::copy_of(
      std::span<const char>{"x", 1});
    BOOST_REQUIRE(data.has_value());

    const auto written = co_await owner.write(
      kwaque::runtime::file_position{
        std::numeric_limits<std::uint64_t>::max() - 1U},
      std::move(*data));
    const auto closed = co_await owner.close();

    BOOST_REQUIRE(!written.has_value());
    BOOST_CHECK(written.error().code() == kwaque::errc::out_of_range);
    BOOST_CHECK(probe.reads.empty());
    BOOST_CHECK(probe.writes.empty());
    BOOST_CHECK_EQUAL(probe.sizes, 1U);
    BOOST_REQUIRE(closed.has_value());
}

SEASTAR_TEST_CASE(file_write_restores_logical_size_after_tail_extension) {
    file_probe probe;
    probe.storage.assign(5000, 'e');
    probe.size = probe.storage.size();
    auto owner = make_file(probe);
    auto data = kwaque::bytes::fragmented_buffer::copy_of(
      std::span<const char>{"tail", 4});
    BOOST_REQUIRE(data.has_value());

    const auto written = co_await owner.write(
      kwaque::runtime::file_position{4999}, std::move(*data));
    const auto close_result = co_await owner.close();

    BOOST_REQUIRE(written.has_value());
    BOOST_REQUIRE_EQUAL(probe.reads.size(), 1U);
    BOOST_REQUIRE_EQUAL(probe.writes.size(), 1U);
    BOOST_CHECK_EQUAL(probe.reads.front().position, 4096U);
    BOOST_CHECK_EQUAL(probe.writes.front().position, 4096U);
    BOOST_CHECK_EQUAL(probe.truncates, 1U);
    BOOST_CHECK_EQUAL(probe.storage.size(), 5003U);
    BOOST_CHECK_EQUAL(probe.storage[4998], 'e');
    BOOST_CHECK(std::string_view(probe.storage.data() + 4999, 4) == "tail");
    BOOST_REQUIRE(close_result.has_value());
}

SEASTAR_TEST_CASE(file_ready_native_write_rejects_zero_and_excess_progress) {
    for (const bool staged : {false, true}) {
        for (const auto reported : {std::size_t{0}, std::size_t{4097}}) {
            file_probe probe;
            probe.delayed_write.emplace();
            probe.delayed_write->set_value(reported);
            auto owner = make_file(probe);
            auto data = staged ? staging_data(true, 4096)
                               : aligned_data(4096, 'w');
            const auto written = co_await owner.write(
              kwaque::runtime::file_position{0}, std::move(data));
            const auto statistics = owner.statistics();
            const bool admission_idle
              = kwaque::runtime::file_test_access::move_is_idle(owner);
            const auto closed = co_await owner.close();
            BOOST_REQUIRE(!written.has_value());
            BOOST_CHECK(written.error().code() == kwaque::errc::io_failure);
            BOOST_CHECK_EQUAL(probe.writes.size(), 1U);
            BOOST_CHECK_EQUAL(statistics.accepted, 1U);
            BOOST_CHECK_EQUAL(statistics.completed, 1U);
            BOOST_CHECK_EQUAL(statistics.active, 0U);
            BOOST_CHECK_EQUAL(statistics.completed_bytes, 0U);
            BOOST_CHECK(admission_idle);
            BOOST_REQUIRE(closed.has_value());
            BOOST_CHECK_EQUAL(probe.closes, 1U);
        }
    }
}

SEASTAR_TEST_CASE(file_ready_native_write_preserves_dependency_failure_kinds) {
    for (const bool staged : {false, true}) {
        for (const bool allocation_failure : {false, true}) {
            file_probe probe;
            probe.delayed_write.emplace();
            if (allocation_failure) {
                probe.delayed_write->set_exception(std::bad_alloc{});
            } else {
                probe.delayed_write->set_exception(
                  std::system_error(
                    std::make_error_code(std::errc::no_space_on_device)));
            }
            auto owner = make_file(probe);
            auto data = staged ? staging_data(true, 4096)
                               : aligned_data(4096, 'w');
            std::optional<
              seastar::future<kwaque::runtime::result<kwaque::byte_count>>>
              writing;
            bool escaped = false;
            try {
                writing.emplace(owner.write(
                  kwaque::runtime::file_position{0}, std::move(data)));
            } catch (...) {
                escaped = true;
            }
            std::optional<kwaque::runtime::result<kwaque::byte_count>> written;
            bool allocation_observed = false;
            bool unexpected_exception = false;
            if (writing) {
                try {
                    written.emplace(co_await std::move(*writing));
                } catch (const std::bad_alloc&) {
                    allocation_observed = true;
                } catch (...) {
                    unexpected_exception = true;
                }
            }
            const auto statistics = owner.statistics();
            const bool admission_idle
              = kwaque::runtime::file_test_access::move_is_idle(owner);
            const auto closed = co_await owner.close();
            BOOST_CHECK(!escaped);
            BOOST_CHECK(!unexpected_exception);
            BOOST_CHECK_EQUAL(allocation_observed, allocation_failure);
            if (allocation_failure) {
                BOOST_CHECK(!written.has_value());
            } else {
                BOOST_REQUIRE(written.has_value());
                BOOST_REQUIRE(!written->has_value());
                BOOST_CHECK(
                  written->error().code() == kwaque::errc::resource_exhausted);
            }
            BOOST_CHECK_EQUAL(probe.writes.size(), 1U);
            BOOST_CHECK_EQUAL(statistics.accepted, 1U);
            BOOST_CHECK_EQUAL(statistics.completed, 1U);
            BOOST_CHECK_EQUAL(statistics.active, 0U);
            BOOST_CHECK_EQUAL(statistics.completed_bytes, 0U);
            BOOST_CHECK(admission_idle);
            BOOST_REQUIRE(closed.has_value());
            BOOST_CHECK_EQUAL(probe.closes, 1U);
        }
    }
}

SEASTAR_TEST_CASE(file_write_retries_aligned_short_native_writes) {
    file_probe probe;
    probe.maximum_write_result = 4096;
    auto owner = make_file(probe);
    auto data = aligned_data(8192, 'r');

    const auto written = co_await owner.write(
      kwaque::runtime::file_position{0}, std::move(data));
    const auto close_result = co_await owner.close();

    BOOST_REQUIRE(written.has_value());
    BOOST_REQUIRE_EQUAL(probe.writes.size(), 2U);
    BOOST_CHECK_EQUAL(probe.writes[0].position, 0U);
    BOOST_CHECK_EQUAL(probe.writes[1].position, 4096U);
    BOOST_CHECK_EQUAL(probe.storage.size(), 8192U);
    BOOST_REQUIRE(close_result.has_value());
}

SEASTAR_TEST_CASE(
  file_short_write_recovery_rejects_alignment_breaking_progress) {
    {
        file_probe probe;
        probe.maximum_write_result = 1024;
        auto owner = make_file(probe);

        const auto written = co_await owner.write(
          kwaque::runtime::file_position{0}, aligned_data(8192, 'd'));
        const auto closed = co_await owner.close();

        BOOST_REQUIRE(!written.has_value());
        BOOST_CHECK(written.error().code() == kwaque::errc::io_failure);
        BOOST_CHECK_EQUAL(probe.writes.size(), 1U);
        BOOST_REQUIRE(closed.has_value());
    }
    {
        file_probe probe;
        probe.maximum_write_result = 1024;
        auto owner = make_file(probe);
        const std::string source(4097, 's');
        auto backing = kwaque::bytes::fragmented_buffer::copy_of(
          std::span<const char>{source});
        BOOST_REQUIRE(backing.has_value());
        auto unaligned = backing->share(
          kwaque::byte_count{1}, kwaque::byte_count{4096});
        BOOST_REQUIRE(unaligned.has_value());

        const auto written = co_await owner.write(
          kwaque::runtime::file_position{0}, std::move(*unaligned));
        const auto closed = co_await owner.close();

        BOOST_REQUIRE(!written.has_value());
        BOOST_CHECK(written.error().code() == kwaque::errc::io_failure);
        BOOST_CHECK_EQUAL(probe.writes.size(), 1U);
        BOOST_REQUIRE(closed.has_value());
    }
}

SEASTAR_TEST_CASE(file_write_and_truncate_share_one_native_serializer) {
    file_probe probe;
    probe.delayed_write.emplace();
    auto owner = make_file(probe);
    auto data = aligned_data(4096, 'q');

    auto writing = owner.write(
      kwaque::runtime::file_position{0}, std::move(data));
    auto truncating = owner.truncate(2048);
    const bool truncate_waited = !truncating.available()
                                 && probe.truncates == 0;

    probe.delayed_write->set_value(4096);
    const auto written = co_await std::move(writing);
    const auto truncated = co_await std::move(truncating);
    const auto close_result = co_await owner.close();

    BOOST_CHECK(truncate_waited);
    BOOST_REQUIRE(written.has_value());
    BOOST_REQUIRE(truncated.has_value());
    BOOST_CHECK_EQUAL(probe.truncates, 1U);
    BOOST_REQUIRE(close_result.has_value());
}

SEASTAR_TEST_CASE(file_write_admission_bounds_retained_contenders) {
    file_probe probe;
    probe.delayed_write.emplace();
    auto owner = make_file(
      probe,
      {.pending_read_bytes = kwaque::byte_count{4096},
       .pending_reads = 1,
       .queued_write_bytes = kwaque::byte_count{8192},
       .queued_writes = 2});

    auto active = owner.write(
      kwaque::runtime::file_position{0}, aligned_data(4096, 'a'));
    BOOST_CHECK(!active.available());
    auto queued = owner.write(
      kwaque::runtime::file_position{4096}, aligned_data(4096, 'b'));
    BOOST_CHECK(!queued.available());
    BOOST_CHECK_EQUAL(owner.queued_writes(), 1U);
    BOOST_CHECK_EQUAL(owner.queued_write_bytes().value(), 4096U);

    const std::string retained_source(4097, 'r');
    auto retained_backing = kwaque::bytes::fragmented_buffer::copy_of(
      std::span<const char>{retained_source});
    BOOST_REQUIRE(retained_backing.has_value());
    auto retained_slice = retained_backing->share(
      kwaque::byte_count{}, kwaque::byte_count{4096});
    BOOST_REQUIRE(retained_slice.has_value());
    BOOST_CHECK_EQUAL(retained_slice->retained_bytes().value(), 4097U);
    const auto retained_saturated = co_await owner.write(
      kwaque::runtime::file_position{8192}, std::move(*retained_slice));
    auto second_queued = owner.write(
      kwaque::runtime::file_position{8192}, aligned_data(4096, 'c'));
    BOOST_CHECK(!second_queued.available());
    BOOST_CHECK_EQUAL(owner.queued_writes(), 2U);
    BOOST_CHECK_EQUAL(owner.queued_write_bytes().value(), 8192U);
    const auto saturated = co_await owner.write(
      kwaque::runtime::file_position{12288}, aligned_data(4096, 'd'));
    const auto saturated_truncate = co_await owner.truncate(4096);
    BOOST_REQUIRE(!retained_saturated.has_value());
    BOOST_CHECK(retained_saturated.error().code() == kwaque::errc::queue_full);
    BOOST_REQUIRE(!saturated.has_value());
    BOOST_CHECK(saturated.error().code() == kwaque::errc::queue_full);
    BOOST_REQUIRE(!saturated_truncate.has_value());
    BOOST_CHECK(saturated_truncate.error().code() == kwaque::errc::queue_full);

    probe.delayed_write->set_value(4096);
    const auto active_result = co_await std::move(active);
    const auto queued_result = co_await std::move(queued);
    const auto second_queued_result = co_await std::move(second_queued);
    BOOST_REQUIRE(active_result.has_value());
    BOOST_REQUIRE(queued_result.has_value());
    BOOST_REQUIRE(second_queued_result.has_value());
    BOOST_CHECK_EQUAL(owner.queued_writes(), 0U);
    BOOST_CHECK_EQUAL(owner.queued_write_bytes().value(), 0U);
    const auto closed = co_await owner.close();
    BOOST_REQUIRE(closed.has_value());
}
