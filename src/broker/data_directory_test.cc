#include "src/broker/data_directory.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>
#include <sys/stat.h>

#include <chrono>
#include <filesystem>
#include <string>
#include <unistd.h>

namespace {

class temporary_directory final {
public:
    temporary_directory() {
        path_ = std::filesystem::temp_directory_path() /
            ("kwaque-data-test-" + std::to_string(::getpid()) + "-" +
             std::to_string(std::chrono::steady_clock::now()
                                .time_since_epoch()
                                .count()));
    }

    ~temporary_directory() {
        std::error_code ignored;
        std::filesystem::permissions(
          path_,
          std::filesystem::perms::owner_all,
          std::filesystem::perm_options::add,
          ignored);
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
};

} // namespace

SEASTAR_TEST_CASE(data_directory_creates_missing_path_and_leaves_no_probe) {
    temporary_directory directory;
    const auto nested = directory.path() / "nested" / "data";

    co_await kwaque::broker::prepare_data_directory(nested);

    BOOST_REQUIRE(std::filesystem::is_directory(nested));
    for (const auto& entry : std::filesystem::directory_iterator(nested)) {
        BOOST_CHECK(
          entry.path().filename().string().find(".kwaque-write-probe-") != 0U);
    }
}

SEASTAR_TEST_CASE(data_directory_accepts_writable_existing_path) {
    temporary_directory directory;
    std::filesystem::create_directories(directory.path());
    co_await kwaque::broker::prepare_data_directory(directory.path());
    BOOST_CHECK(std::filesystem::is_directory(directory.path()));
}

SEASTAR_TEST_CASE(data_directory_rejects_read_only_path) {
    temporary_directory directory;
    std::filesystem::create_directories(directory.path());
    ::chmod(directory.path().c_str(), 0555);

    bool rejected = false;
    try {
        co_await kwaque::broker::prepare_data_directory(directory.path());
    } catch (const std::runtime_error&) {
        rejected = true;
    }
    BOOST_CHECK(rejected);
}
