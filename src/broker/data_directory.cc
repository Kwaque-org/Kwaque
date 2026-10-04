#include "src/broker/data_directory.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shard_id.hh>

#include <chrono>
#include <exception>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unistd.h>

namespace kwaque::broker {

seastar::future<> prepare_data_directory(
  const std::filesystem::path& path,
  const seastar::abort_source* startup_abort,
  bool require_mount_marker) {
    const auto check_abort = [startup_abort] {
        if (startup_abort != nullptr) {
            startup_abort->check();
        }
    };
    check_abort();
    const std::string directory = path.string();
    if (require_mount_marker) {
        const auto marker = (path / ".kwaque_data_dir").string();
        const bool exists = co_await seastar::file_exists(marker);
        check_abort();
        if (!exists) {
            throw std::invalid_argument(
              "data directory mount marker is missing; verify the expected "
              "filesystem is mounted: "
              + marker);
        }
    }
    // A directory the broker creates is private to its owner; an existing
    // directory keeps the permissions its operator chose.
    co_await seastar::recursive_touch_directory(
      directory, seastar::file_permissions::user_permissions);
    check_abort();

    const seastar::stat_data status = co_await seastar::file_stat(directory);
    check_abort();
    if (status.type != seastar::directory_entry_type::directory) {
        throw std::runtime_error(
          "data directory path is not a directory: " + directory);
    }
    const auto nonce
      = std::chrono::steady_clock::now().time_since_epoch().count();
    const std::string probe
      = (path
         / (".kwaque-write-probe-" + std::to_string(::getpid()) + "-" + std::to_string(seastar::this_shard_id()) + "-" + std::to_string(nonce)))
          .string();

    // Creating a file is the writability test: permission bits alone do not
    // decide it, for example for a privileged process or a read-only mount.
    seastar::file file;
    try {
        file = co_await seastar::open_file_dma(
          probe,
          seastar::open_flags::wo | seastar::open_flags::create
            | seastar::open_flags::exclusive);
    } catch (const std::system_error& error) {
        throw std::runtime_error(
          "data directory is not writable: " + directory + " ("
          + error.code().message() + ")");
    }

    bool created = false;
    std::exception_ptr failure;
    try {
        // Preserve probe ownership across suspension points for failure
        // cleanup.
        created = true; // NOLINT(clang-analyzer-deadcode.DeadStores)
        co_await file.close();
        check_abort();
        co_await seastar::remove_file(probe);
        created = false;
        co_await seastar::sync_directory(directory);
        check_abort();
    } catch (...) {
        failure = std::current_exception();
    }
    if (failure) {
        if (created) {
            try {
                co_await seastar::remove_file(probe);
            } catch (...) {
            }
        }
        std::rethrow_exception(failure);
    }
}

} // namespace kwaque::broker
