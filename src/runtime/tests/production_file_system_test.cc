#include "src/runtime/file.h"
#include "src/runtime/production/file.h"
#include "src/runtime/testing/contracts/cleanup.h"
#include "src/runtime/testing/contracts/file_system_contract.h"
#include "src/runtime/testing/test_directory.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/tmp_file.hh>

#include <boost/test/unit_test.hpp>
#include <sys/statvfs.h>

#include <algorithm>
#include <exception>
#include <filesystem>
#include <fstream>
#include <new>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>

namespace {

struct native_file_driver {
    template<typename T>
    seastar::future<T> operator()(seastar::future<T> operation) const {
        return operation;
    }
};

kwaque::runtime::file_path path_of(const std::filesystem::path& path) {
    auto made = kwaque::runtime::file_path::make(path.string());
    if (!made) {
        std::terminate();
    }
    return std::move(*made);
}

} // namespace

SEASTAR_TEST_CASE(production_file_system_runs_shared_capability_contract) {
    return seastar::tmp_dir::do_with(
      kwaque::runtime::testing::test_directory_template(),
      [](seastar::tmp_dir& directory) -> seastar::future<> {
          kwaque::runtime::production::file_system files;
          co_await kwaque::runtime::testing::run_file_system_contract(
            files,
            path_of(directory.get_path() / "capabilities"),
            native_file_driver{});
      });
}

SEASTAR_TEST_CASE(production_file_system_creates_opens_stats_and_reopens) {
    return seastar::tmp_dir::do_with(
      kwaque::runtime::testing::test_directory_template(),
      [](seastar::tmp_dir& directory) -> seastar::future<> {
          kwaque::runtime::production::file_system file_system;
          const auto nested = (directory.get_path() / "nested/data");
          const auto file_path = nested / "owner.data";

          const auto created = co_await file_system.create_directories(
            path_of(nested));
          const auto directory_exists = co_await file_system.exists(
            path_of(nested));
          const auto directory_status = co_await file_system.stat(
            path_of(nested));
          auto opened = co_await file_system.open(
            path_of(file_path),
            {.access = kwaque::runtime::file_access::read_write,
             .create = true,
             .exclusive = true,
             .truncate = false,
             .permissions = 0600U});

          const auto exercise = [&] -> seastar::future<> {
              BOOST_REQUIRE(created.has_value());
              BOOST_REQUIRE(directory_exists.has_value());
              BOOST_CHECK(*directory_exists);
              BOOST_REQUIRE(directory_status.has_value());
              BOOST_CHECK(
                directory_status->kind
                == kwaque::runtime::file_kind::directory);
              BOOST_REQUIRE(opened.has_value());

              const auto second_open = co_await file_system.open(
                path_of(file_path),
                {.access = kwaque::runtime::file_access::read_write,
                 .create = true,
                 .exclusive = true,
                 .truncate = false,
                 .permissions = 0600U});
              BOOST_REQUIRE(!second_open.has_value());
              BOOST_CHECK(
                second_open.error().code() == kwaque::errc::already_exists);

              const auto flushed = co_await opened->flush();
              const auto closed = co_await opened->close();
              BOOST_REQUIRE(flushed.has_value());
              BOOST_REQUIRE(closed.has_value());

              auto reopened = co_await file_system.open(
                path_of(file_path),
                {.access = kwaque::runtime::file_access::read_only});
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
          };
          co_await exercise().finally([&opened] {
              if (opened) {
                  return opened->close().then(
                    kwaque::runtime::testing::require_cleanup);
              }
              return seastar::make_ready_future<>();
          });
          co_return;
      });
}

SEASTAR_TEST_CASE(
  production_file_retains_statistics_after_factory_destruction) {
    return seastar::tmp_dir::do_with(
      kwaque::runtime::testing::test_directory_template(),
      [](seastar::tmp_dir& directory) -> seastar::future<> {
          std::optional<kwaque::runtime::file> opened;
          {
              kwaque::runtime::production::file_system file_system;
              auto result = co_await file_system.open(
                path_of((directory.get_path() / "retained.data")),
                {.access = kwaque::runtime::file_access::read_write,
                 .create = true,
                 .permissions = 0600U});
              BOOST_REQUIRE(result.has_value());
              opened.emplace(std::move(*result));
          }

          const auto exercise = [&opened] -> seastar::future<> {
              const auto size = co_await opened->size();
              BOOST_REQUIRE(size.has_value());
              BOOST_CHECK_EQUAL(*size, 0U);
              const auto closed = co_await opened->close();
              BOOST_REQUIRE(closed.has_value());
              BOOST_CHECK(
                opened->statistics()
                == (kwaque::runtime::operation_statistics_snapshot{
                  .active = 0,
                  .accepted = 3,
                  .completed = 3,
                }));
          };
          co_await exercise().finally([&opened] {
              return opened->close().then(
                kwaque::runtime::testing::require_cleanup);
          });
          opened.reset();
          co_return;
      });
}

SEASTAR_TEST_CASE(
  production_file_system_lists_incrementally_and_bounds_results) {
    return seastar::tmp_dir::do_with(
      kwaque::runtime::testing::test_directory_template(),
      [](seastar::tmp_dir& directory) -> seastar::future<> {
          kwaque::runtime::production::file_system file_system;
          std::filesystem::create_directories(
            (directory.get_path() / "tree/subdir"));
          std::ofstream((directory.get_path() / "tree/a")) << "a";
          std::ofstream((directory.get_path() / "tree/bb")) << "b";
          std::filesystem::create_symlink(
            "a", (directory.get_path() / "tree/link"));

          auto listing = co_await file_system.list(
            path_of((directory.get_path() / "tree")),
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
            path_of((directory.get_path() / "tree")),
            {.maximum_entries = kwaque::item_count{2},
             .maximum_name_bytes = kwaque::byte_count{64}});
          BOOST_REQUIRE(!count_limited.has_value());
          BOOST_CHECK(
            count_limited.error().code() == kwaque::errc::resource_exhausted);

          const auto bytes_limited = co_await file_system.list(
            path_of((directory.get_path() / "tree")),
            {.maximum_entries = kwaque::item_count{4},
             .maximum_name_bytes = kwaque::byte_count{3}});
          BOOST_REQUIRE(!bytes_limited.has_value());
          BOOST_CHECK(
            bytes_limited.error().code() == kwaque::errc::resource_exhausted);
          co_return;
      });
}

SEASTAR_TEST_CASE(production_file_system_maps_missing_nonempty_and_rename) {
    return seastar::tmp_dir::do_with(
      kwaque::runtime::testing::test_directory_template(),
      [](seastar::tmp_dir& directory) -> seastar::future<> {
          kwaque::runtime::production::file_system file_system;
          const auto source = (directory.get_path() / "source");
          const auto destination = (directory.get_path() / "destination");
          const auto nonempty = (directory.get_path() / "nonempty");
          std::ofstream(source) << "source";
          std::ofstream(destination) << "destination";
          std::filesystem::create_directories(nonempty);
          std::ofstream(nonempty / "child") << "child";

          const auto missing = co_await file_system.stat(
            path_of((directory.get_path() / "missing")));
          BOOST_REQUIRE(!missing.has_value());
          BOOST_CHECK(missing.error().code() == kwaque::errc::not_found);

          const auto nonempty_remove = co_await file_system.remove_directory(
            path_of(nonempty));
          BOOST_REQUIRE(!nonempty_remove.has_value());
          BOOST_CHECK(
            nonempty_remove.error().code()
            == kwaque::errc::directory_not_empty);

          const auto renamed = co_await file_system.rename(
            path_of(source), path_of(destination));
          BOOST_REQUIRE(renamed.has_value());
          const auto source_exists = co_await file_system.exists(
            path_of(source));
          const auto destination_exists = co_await file_system.exists(
            path_of(destination));
          BOOST_REQUIRE(source_exists.has_value());
          BOOST_REQUIRE(destination_exists.has_value());
          BOOST_CHECK(!*source_exists);
          BOOST_CHECK(*destination_exists);

          const auto synced = co_await file_system.sync_directory(
            path_of((directory.get_path() / ".")));
          BOOST_REQUIRE(synced.has_value());

          const auto missing_sync = co_await file_system.sync_directory(
            path_of((directory.get_path() / "missing-directory")));
          BOOST_REQUIRE(!missing_sync.has_value());
          BOOST_CHECK(missing_sync.error().code() == kwaque::errc::not_found);

          const auto child_removed = co_await file_system.remove_file(
            path_of(nonempty / "child"));
          const auto directory_removed = co_await file_system.remove_directory(
            path_of(nonempty));
          BOOST_REQUIRE(child_removed.has_value());
          BOOST_REQUIRE(directory_removed.has_value());
          co_return;
      });
}

SEASTAR_TEST_CASE(production_file_system_maps_object_kind_mismatches) {
    return seastar::tmp_dir::do_with(
      kwaque::runtime::testing::test_directory_template(),
      [](seastar::tmp_dir& directory) -> seastar::future<> {
          kwaque::runtime::production::file_system file_system;
          const auto directory_path = (directory.get_path() / "directory");
          const auto regular_path = (directory.get_path() / "regular");
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

          const auto wrong_directory = co_await file_system.remove_directory(
            path_of(regular_path));
          BOOST_REQUIRE(!wrong_directory.has_value());
          BOOST_CHECK(
            wrong_directory.error().code() == kwaque::errc::not_a_directory);
          BOOST_CHECK(std::filesystem::exists(regular_path));
          const auto wrong_file = co_await file_system.remove_file(
            path_of(directory_path));
          BOOST_REQUIRE(!wrong_file.has_value());
          BOOST_CHECK(
            wrong_file.error().code() == kwaque::errc::is_a_directory);
          BOOST_CHECK(std::filesystem::exists(directory_path));
          const auto absent = directory.get_path() / "missing";
          const auto same_missing = co_await file_system.rename(
            path_of(absent), path_of(absent));
          BOOST_REQUIRE(!same_missing.has_value());
          BOOST_CHECK(same_missing.error().code() == kwaque::errc::not_found);
          const auto symlink = directory.get_path() / "link";
          std::filesystem::create_symlink(regular_path, symlink);
          const auto unlinked = co_await file_system.remove_file(
            path_of(symlink));
          BOOST_REQUIRE(unlinked.has_value());
          BOOST_CHECK(std::filesystem::exists(regular_path));

          const auto listed_file = co_await file_system.list(
            path_of(regular_path), {});
          BOOST_REQUIRE(!listed_file.has_value());
          BOOST_CHECK(
            listed_file.error().code() == kwaque::errc::not_a_directory);
          co_return;
      });
}

SEASTAR_TEST_CASE(production_file_system_maps_permission_denied) {
    return seastar::tmp_dir::do_with(
      kwaque::runtime::testing::test_directory_template(),
      [](seastar::tmp_dir& directory) -> seastar::future<> {
          kwaque::runtime::production::file_system file_system;
          const auto restricted = (directory.get_path() / "restricted");
          std::filesystem::create_directories(restricted);
          std::ofstream(restricted / "entry") << "data";
          std::filesystem::permissions(
            restricted, std::filesystem::perms::none);

          const auto exercise = [&] -> seastar::future<> {
              const auto denied = co_await file_system.list(
                path_of(restricted), {});
              if (::geteuid() == 0) {
                  BOOST_REQUIRE(denied.has_value());
              } else {
                  BOOST_REQUIRE(!denied.has_value());
                  BOOST_CHECK(
                    denied.error().code() == kwaque::errc::permission_denied);
              }
          };
          co_await exercise().finally([&restricted] {
              return seastar::chmod(
                restricted.string(),
                seastar::file_permissions::user_permissions);
          });
          co_return;
      });
}

SEASTAR_TEST_CASE(
  production_space_samples_native_statvfs_and_reports_missing_path) {
    return seastar::tmp_dir::do_with(
      kwaque::runtime::testing::test_directory_template(),
      [](seastar::tmp_dir& directory) -> seastar::future<> {
          kwaque::runtime::production::file_system files;
          const auto native = co_await seastar::engine().statvfs(
            directory.get_path().string());
          const auto sample = co_await files.space(
            path_of(directory.get_path()));
          BOOST_REQUIRE(sample.has_value());
          BOOST_CHECK_EQUAL(
            sample->capacity().value(), native.f_frsize * native.f_blocks);
          BOOST_CHECK(sample->available() <= sample->free());
          BOOST_CHECK(sample->free() <= sample->capacity());
          BOOST_CHECK_EQUAL(
            sample->read_only(), (native.f_flag & ST_RDONLY) != 0);
          // Free/available may change between calls on a shared filesystem.
          const auto missing = co_await files.space(
            path_of(directory.get_path() / "missing/child"));
          BOOST_REQUIRE(!missing);
          BOOST_CHECK(missing.error().code() == kwaque::errc::not_found);
          BOOST_CHECK_EQUAL(files.statistics().active, 0U);
          BOOST_CHECK_EQUAL(files.statistics().completed, 2U);
      });
}

SEASTAR_TEST_CASE(production_space_preserves_permission_failure) {
    if (::geteuid() == 0) co_return; // Root may bypass directory permissions.
    co_await seastar::tmp_dir::do_with(
      kwaque::runtime::testing::test_directory_template(),
      [](seastar::tmp_dir& directory) -> seastar::future<> {
          const auto denied = directory.get_path() / "denied";
          std::filesystem::create_directories(denied / "child");
          auto restore = seastar::defer([&denied] {
              std::filesystem::permissions(
                denied, std::filesystem::perms::owner_all);
          });
          std::filesystem::permissions(denied, std::filesystem::perms::none);
          kwaque::runtime::production::file_system files;
          const auto failed = co_await files.space(path_of(denied / "child"));
          BOOST_REQUIRE(!failed);
          BOOST_CHECK(failed.error().code() == kwaque::errc::permission_denied);
          BOOST_CHECK_EQUAL(files.statistics().active, 0U);
      });
}

SEASTAR_TEST_CASE(
  production_directory_cursor_joins_pending_read_and_classifies_links) {
    return seastar::tmp_dir::do_with(
      kwaque::runtime::testing::test_directory_template(),
      [](seastar::tmp_dir& directory) -> seastar::future<> {
          std::filesystem::create_symlink(
            "missing", directory.get_path() / "link");
          kwaque::runtime::production::file_system files;
          auto opened = co_await files.open_directory(
            path_of(directory.get_path()));
          BOOST_REQUIRE(opened.has_value());
          auto pending = opened->next({});
          auto moved = std::move(*opened);
          auto closing = moved.close();
          auto closed = co_await std::move(closing);
          auto page = co_await std::move(pending);
          BOOST_REQUIRE(closed.has_value());
          BOOST_REQUIRE(page.has_value());
          BOOST_REQUIRE_EQUAL(page->entries().size(), 1U);
          BOOST_CHECK_EQUAL(page->entries().front().name.value(), "link");
          BOOST_CHECK(
            page->entries().front().kind == kwaque::runtime::file_kind::other);
      });
}

SEASTAR_TEST_CASE(production_directory_cursor_can_close_before_first_next) {
    return seastar::tmp_dir::do_with(
      kwaque::runtime::testing::test_directory_template(),
      [](seastar::tmp_dir& directory) -> seastar::future<> {
          kwaque::runtime::production::file_system files;
          for (std::size_t count = 0; count < 80; ++count) {
              auto opened = co_await files.open_directory(
                path_of(directory.get_path()));
              BOOST_REQUIRE(opened.has_value());
              auto closed = co_await opened->close();
              BOOST_REQUIRE(closed.has_value());
          }
      });
}

SEASTAR_TEST_CASE(
  production_directory_cursor_crosses_native_buffers_with_small_pages) {
    return seastar::tmp_dir::do_with(
      kwaque::runtime::testing::test_directory_template(),
      [](seastar::tmp_dir& directory) -> seastar::future<> {
          for (std::size_t index = 0; index < 600; ++index)
              std::ofstream(
                directory.get_path()
                / (std::string(64, 'n') + std::to_string(index)))
                << "";
          kwaque::runtime::production::file_system files;
          auto opened = co_await files.open_directory(
            path_of(directory.get_path()));
          BOOST_REQUIRE(opened.has_value());
          std::set<std::string> names;
          std::exception_ptr failure;
          try {
              bool end = false;
              for (std::size_t pages = 0; !end; ++pages) {
                  BOOST_REQUIRE_LT(pages, 100U);
                  auto page = co_await opened->next(
                    {.maximum_entries = kwaque::item_count{17},
                     .maximum_name_bytes = kwaque::byte_count{1024}});
                  BOOST_REQUIRE(page.has_value());
                  BOOST_CHECK_LE(page->entries().size(), 17U);
                  std::size_t bytes = 0;
                  for (const auto& entry : page->entries()) {
                      bytes += entry.name.value().size();
                      BOOST_CHECK(names.insert(entry.name.value()).second);
                  }
                  BOOST_CHECK_LE(bytes, 1024U);
                  end = page->end();
              }
          } catch (...) {
              failure = std::current_exception();
          }
          const auto closed = co_await opened->close();
          if (failure) std::rethrow_exception(failure);
          BOOST_REQUIRE(closed.has_value());
          BOOST_CHECK_EQUAL(names.size(), 600U);
      });
}

SEASTAR_TEST_CASE(
  production_directory_cursor_open_allocation_failure_releases_admission) {
    return seastar::tmp_dir::do_with(
      kwaque::runtime::testing::test_directory_template(),
      [](seastar::tmp_dir& directory) -> seastar::future<> {
          kwaque::runtime::production::file_system files;
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
          auto directory_path = path_of(directory.get_path());
          auto& injector = seastar::memory::local_failure_injector();
          injector.fail_after(0);
          auto opening = files.open_directory(std::move(directory_path));
          const bool injected = injector.failed();
          injector.cancel();
          bool allocation_failure = false;
          try {
              auto opened = co_await std::move(opening);
              if (opened) static_cast<void>(co_await opened->close());
          } catch (const std::bad_alloc&) {
              allocation_failure = true;
          }
          BOOST_CHECK(injected && allocation_failure);
#endif
          std::optional<kwaque::runtime::production::directory_cursor> retained;
          {
              kwaque::runtime::production::file_system temporary;
              auto opened = co_await temporary.open_directory(
                path_of(directory.get_path()));
              BOOST_REQUIRE(opened.has_value());
              retained.emplace(std::move(*opened));
          }
          auto page = co_await retained->next({});
          auto closed = co_await retained->close();
          BOOST_REQUIRE(page.has_value());
          BOOST_CHECK(page->end());
          BOOST_REQUIRE(closed.has_value());
          auto reopened = co_await files.open_directory(
            path_of(directory.get_path()));
          BOOST_REQUIRE(reopened.has_value());
          BOOST_REQUIRE((co_await reopened->close()).has_value());
      });
}
