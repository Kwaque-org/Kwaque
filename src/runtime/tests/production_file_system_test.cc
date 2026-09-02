#include "src/runtime/file.h"
#include "src/runtime/production/file.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <chrono>
#include <exception>
#include <filesystem>
#include <fstream>
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
                / ("kwaque-file-system-test-" + std::to_string(::getpid())
                   + "-"
                   + std::to_string(
                     std::chrono::steady_clock::now()
                       .time_since_epoch()
                       .count()));
        std::filesystem::create_directories(path_);
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

    [[nodiscard]] std::filesystem::path child(std::string_view name) const {
        return path_ / std::string{name};
    }

private:
    std::filesystem::path path_;
};

kwaque::runtime::file_path path_of(const std::filesystem::path& path) {
    auto made = kwaque::runtime::file_path::make(path.string());
    if (!made) {
        std::terminate();
    }
    return std::move(*made);
}

} // namespace

SEASTAR_TEST_CASE(production_file_system_creates_opens_stats_and_reopens) {
    temporary_directory directory;
    kwaque::runtime::production::file_system file_system;
    const auto nested = directory.child("nested/data");
    const auto file_path = nested / "owner.data";

    const auto created = co_await file_system.create_directories(
      path_of(nested));
    const auto directory_exists = co_await file_system.exists(path_of(nested));
    const auto directory_status = co_await file_system.stat(path_of(nested));
    auto opened = co_await file_system.open(
      path_of(file_path),
      {.access = kwaque::runtime::file_access::read_write,
       .create = true,
       .exclusive = true,
       .truncate = false,
       .permissions = 0600U});

    BOOST_REQUIRE(created.has_value());
    BOOST_REQUIRE(directory_exists.has_value());
    BOOST_CHECK(*directory_exists);
    BOOST_REQUIRE(directory_status.has_value());
    BOOST_CHECK(
      directory_status->kind == kwaque::runtime::file_kind::directory);
    BOOST_REQUIRE(opened.has_value());

    const auto second_open = co_await file_system.open(
      path_of(file_path),
      {.access = kwaque::runtime::file_access::read_write,
       .create = true,
       .exclusive = true,
       .truncate = false,
       .permissions = 0600U});
    BOOST_REQUIRE(!second_open.has_value());
    BOOST_CHECK(second_open.error().code() == kwaque::errc::already_exists);

    const auto flushed = co_await opened->flush();
    const auto closed = co_await opened->close();
    BOOST_REQUIRE(flushed.has_value());
    BOOST_REQUIRE(closed.has_value());

    auto reopened = co_await file_system.open(
      path_of(file_path), {.access = kwaque::runtime::file_access::read_only});
    BOOST_REQUIRE(reopened.has_value());
    const auto reopened_closed = co_await reopened->close();
    BOOST_REQUIRE(reopened_closed.has_value());
    BOOST_CHECK(
      file_system.statistics()
      == (kwaque::runtime::operation_statistics_snapshot{
        .active = 0,
        .accepted = 9,
        .completed = 9,
      }));
}

SEASTAR_TEST_CASE(
  production_file_system_lists_incrementally_and_bounds_results) {
    temporary_directory directory;
    kwaque::runtime::production::file_system file_system;
    std::filesystem::create_directories(directory.child("tree/subdir"));
    std::ofstream(directory.child("tree/a")) << "a";
    std::ofstream(directory.child("tree/bb")) << "b";
    std::filesystem::create_symlink("a", directory.child("tree/link"));

    auto listing = co_await file_system.list(
      path_of(directory.child("tree")),
      {.maximum_entries = kwaque::item_count{4},
       .maximum_name_bytes = kwaque::byte_count{64}});
    BOOST_REQUIRE(listing.has_value());
    BOOST_CHECK_EQUAL(listing->entries().size(), 4U);
    const auto link = std::find_if(
      listing->entries().begin(),
      listing->entries().end(),
      [](const kwaque::runtime::directory_entry& entry) {
          return entry.name.value() == "link";
      });
    BOOST_REQUIRE(link != listing->entries().end());
    BOOST_CHECK(link->kind == kwaque::runtime::file_kind::other);

    const auto count_limited = co_await file_system.list(
      path_of(directory.child("tree")),
      {.maximum_entries = kwaque::item_count{2},
       .maximum_name_bytes = kwaque::byte_count{64}});
    BOOST_REQUIRE(!count_limited.has_value());
    BOOST_CHECK(
      count_limited.error().code() == kwaque::errc::resource_exhausted);

    const auto bytes_limited = co_await file_system.list(
      path_of(directory.child("tree")),
      {.maximum_entries = kwaque::item_count{4},
       .maximum_name_bytes = kwaque::byte_count{3}});
    BOOST_REQUIRE(!bytes_limited.has_value());
    BOOST_CHECK(
      bytes_limited.error().code() == kwaque::errc::resource_exhausted);
}

SEASTAR_TEST_CASE(production_file_system_maps_missing_nonempty_and_rename) {
    temporary_directory directory;
    kwaque::runtime::production::file_system file_system;
    const auto source = directory.child("source");
    const auto destination = directory.child("destination");
    const auto nonempty = directory.child("nonempty");
    std::ofstream(source) << "source";
    std::ofstream(destination) << "destination";
    std::filesystem::create_directories(nonempty);
    std::ofstream(nonempty / "child") << "child";

    const auto missing = co_await file_system.stat(
      path_of(directory.child("missing")));
    BOOST_REQUIRE(!missing.has_value());
    BOOST_CHECK(missing.error().code() == kwaque::errc::not_found);

    const auto nonempty_remove = co_await file_system.remove_directory(
      path_of(nonempty));
    BOOST_REQUIRE(!nonempty_remove.has_value());
    BOOST_CHECK(
      nonempty_remove.error().code() == kwaque::errc::directory_not_empty);

    const auto renamed = co_await file_system.rename(
      path_of(source), path_of(destination));
    BOOST_REQUIRE(renamed.has_value());
    const auto source_exists = co_await file_system.exists(path_of(source));
    const auto destination_exists = co_await file_system.exists(
      path_of(destination));
    BOOST_REQUIRE(source_exists.has_value());
    BOOST_REQUIRE(destination_exists.has_value());
    BOOST_CHECK(!*source_exists);
    BOOST_CHECK(*destination_exists);

    const auto synced = co_await file_system.sync_directory(
      path_of(directory.child(".")));
    BOOST_REQUIRE(synced.has_value());

    const auto missing_sync = co_await file_system.sync_directory(
      path_of(directory.child("missing-directory")));
    BOOST_REQUIRE(!missing_sync.has_value());
    BOOST_CHECK(missing_sync.error().code() == kwaque::errc::not_found);

    const auto child_removed = co_await file_system.remove_file(
      path_of(nonempty / "child"));
    const auto directory_removed = co_await file_system.remove_directory(
      path_of(nonempty));
    BOOST_REQUIRE(child_removed.has_value());
    BOOST_REQUIRE(directory_removed.has_value());
}

SEASTAR_TEST_CASE(production_file_system_maps_object_kind_mismatches) {
    temporary_directory directory;
    kwaque::runtime::production::file_system file_system;
    const auto directory_path = directory.child("directory");
    const auto regular_path = directory.child("regular");
    std::filesystem::create_directories(directory_path);
    std::ofstream(regular_path) << "data";

    auto opened_directory = co_await file_system.open(
      path_of(directory_path),
      {.access = kwaque::runtime::file_access::write_only});
    if (opened_directory) {
        const auto closed = co_await opened_directory->close();
        BOOST_REQUIRE(closed.has_value());
    }
    BOOST_REQUIRE(!opened_directory.has_value());
    BOOST_CHECK(
      opened_directory.error().code() == kwaque::errc::is_a_directory);

    const auto listed_file = co_await file_system.list(
      path_of(regular_path), {});
    BOOST_REQUIRE(!listed_file.has_value());
    BOOST_CHECK(listed_file.error().code() == kwaque::errc::not_a_directory);
}

SEASTAR_TEST_CASE(production_file_system_maps_permission_denied) {
    temporary_directory directory;
    kwaque::runtime::production::file_system file_system;
    const auto restricted = directory.child("restricted");
    std::filesystem::create_directories(restricted);
    std::ofstream(restricted / "entry") << "data";
    std::filesystem::permissions(restricted, std::filesystem::perms::none);

    const auto restore_permissions = [&restricted] {
        std::error_code ignored;
        std::filesystem::permissions(
          restricted,
          std::filesystem::perms::owner_all,
          std::filesystem::perm_options::add,
          ignored);
    };
    try {
        const auto denied = co_await file_system.list(path_of(restricted), {});
        restore_permissions();
        if (::geteuid() == 0) {
            BOOST_REQUIRE(denied.has_value());
        } else {
            BOOST_REQUIRE(!denied.has_value());
            BOOST_CHECK(
              denied.error().code() == kwaque::errc::permission_denied);
        }
    } catch (...) {
        restore_permissions();
        throw;
    }
}
