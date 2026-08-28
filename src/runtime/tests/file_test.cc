#include "src/runtime/file.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>
#include <sys/stat.h>
#include <sys/uio.h>

#include <cstdlib>
#include <functional>
#include <new>
#include <optional>
#include <stdexcept>
#include <system_error>
#include <utility>
#include <vector>

namespace {

struct file_probe final {
    std::optional<seastar::promise<>> delayed_flush;
    std::uint64_t size{0};
    unsigned flushes{0};
    unsigned truncates{0};
    unsigned closes{0};
    bool fail_flush{false};
    bool fail_allocation{false};
};

template<typename T>
[[noreturn]] T unexpected_file_call() {
    std::abort();
}

class probe_file_impl final : public seastar::file_impl {
public:
    explicit probe_file_impl(file_probe& probe) noexcept
      : probe_(probe) {}

    seastar::future<std::size_t> write_dma(
      std::uint64_t, const void*, std::size_t, seastar::io_intent*) final {
        return unexpected_file_call<seastar::future<std::size_t>>();
    }

    seastar::future<std::size_t>
    write_dma(std::uint64_t, std::vector<iovec>, seastar::io_intent*) final {
        return unexpected_file_call<seastar::future<std::size_t>>();
    }

    seastar::future<std::size_t>
    read_dma(std::uint64_t, void*, std::size_t, seastar::io_intent*) final {
        return unexpected_file_call<seastar::future<std::size_t>>();
    }

    seastar::future<std::size_t>
    read_dma(std::uint64_t, std::vector<iovec>, seastar::io_intent*) final {
        return unexpected_file_call<seastar::future<std::size_t>>();
    }

    seastar::future<seastar::temporary_buffer<std::uint8_t>>
    dma_read_bulk(std::uint64_t, std::size_t, seastar::io_intent*) final {
        return unexpected_file_call<
          seastar::future<seastar::temporary_buffer<std::uint8_t>>>();
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
        return seastar::make_ready_future<>();
    }

    seastar::future<> discard(std::uint64_t, std::uint64_t) final {
        return unexpected_file_call<seastar::future<>>();
    }

    seastar::future<> allocate(std::uint64_t, std::uint64_t) final {
        return unexpected_file_call<seastar::future<>>();
    }

    seastar::future<std::uint64_t> size() final {
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

kwaque::runtime::file make_file(file_probe& probe) {
    return kwaque::runtime::file{
      seastar::file{seastar::make_shared<probe_file_impl>(probe)}};
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
