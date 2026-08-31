#include "src/runtime/file.h"
#include "src/runtime/fragmented_buffer_internal.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <chrono>
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>

namespace {

class temporary_directory final {
public:
    temporary_directory() {
        path_ = std::filesystem::temp_directory_path()
                / ("kwaque-runtime-file-test-" + std::to_string(::getpid())
                   + "-"
                   + std::to_string(
                     std::chrono::steady_clock::now()
                       .time_since_epoch()
                       .count()));
        std::filesystem::create_directories(path_);
    }

    ~temporary_directory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] std::filesystem::path file(std::string_view name) const {
        return path_ / std::string{name};
    }

private:
    std::filesystem::path path_;
};

kwaque::bytes::fragmented_buffer aligned_data(std::size_t size, char value) {
    auto storage = seastar::temporary_buffer<char>::aligned(4096, size);
    std::memset(storage.get_write(), value, storage.size());
    return kwaque::runtime::detail::fragmented_buffer_io_access::adopt(
      std::move(storage), kwaque::byte_count{size});
}

} // namespace

SEASTAR_TEST_CASE(production_file_owner_writes_flushes_truncates_and_reopens) {
    temporary_directory directory;
    const auto path = directory.file("owner.data").string();
    auto native = co_await seastar::open_file_dma(
      path,
      seastar::open_flags::rw | seastar::open_flags::create
        | seastar::open_flags::exclusive);
    kwaque::runtime::file owner{std::move(native)};

    const auto written = co_await owner.write(
      kwaque::runtime::file_position{0}, aligned_data(4096, 'k'));
    const auto flushed = co_await owner.flush();
    const auto truncated = co_await owner.truncate(2048);
    const auto truncate_flushed = co_await owner.flush();
    const auto observed_size = co_await owner.size();
    owner.request_abort();
    const auto rejected_flush = co_await owner.flush();
    const auto first_close = co_await owner.close();
    const auto second_close = co_await owner.close();

    BOOST_REQUIRE(written.has_value());
    BOOST_CHECK_EQUAL(written->value(), 4096U);
    BOOST_REQUIRE(flushed.has_value());
    BOOST_REQUIRE(truncated.has_value());
    BOOST_REQUIRE(truncate_flushed.has_value());
    BOOST_REQUIRE(observed_size.has_value());
    BOOST_CHECK_EQUAL(*observed_size, 2048U);
    BOOST_REQUIRE(!rejected_flush.has_value());
    BOOST_CHECK(rejected_flush.error().code() == kwaque::errc::aborted);
    BOOST_REQUIRE(first_close.has_value());
    BOOST_REQUIRE(second_close.has_value());

    auto reopened = co_await seastar::open_file_dma(
      path, seastar::open_flags::ro);
    auto contents = co_await reopened.dma_read_bulk<char>(0, 4096);
    co_await reopened.close();

    BOOST_CHECK_EQUAL(contents.size(), 2048U);
    BOOST_CHECK(
      std::string_view(contents.get(), contents.size())
      == std::string(2048, 'k'));
}
