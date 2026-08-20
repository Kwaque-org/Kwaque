#include "src/broker/data_directory.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shard_id.hh>

#include <sys/stat.h>

#include <chrono>
#include <exception>
#include <stdexcept>
#include <string>
#include <unistd.h>

namespace kwaque::broker {

seastar::future<> prepare_data_directory(const std::filesystem::path& path) {
    const std::string directory = path.string();
    co_await seastar::recursive_touch_directory(directory);

    const seastar::stat_data status = co_await seastar::file_stat(directory);
    if (status.type != seastar::directory_entry_type::directory) {
        throw std::runtime_error(
          "data directory path is not a directory: " + directory);
    }
    constexpr mode_t write_bits = S_IWUSR | S_IWGRP | S_IWOTH;
    if ((status.mode & write_bits) == 0) {
        throw std::runtime_error(
          "data directory is not writable: " + directory);
    }

    const auto nonce
      = std::chrono::steady_clock::now().time_since_epoch().count();
    const std::string probe
      = (path
         / (".kwaque-write-probe-" + std::to_string(::getpid()) + "-" + std::to_string(seastar::this_shard_id()) + "-" + std::to_string(nonce)))
          .string();

    bool created = false;
    std::exception_ptr failure;
    try {
        seastar::file file = co_await seastar::open_file_dma(
          probe,
          seastar::open_flags::wo | seastar::open_flags::create
            | seastar::open_flags::exclusive);
        // Preserve probe ownership across suspension points for failure
        // cleanup.
        created = true; // NOLINT(clang-analyzer-deadcode.DeadStores)
        co_await file.close();
        co_await seastar::remove_file(probe);
        created = false;
        co_await seastar::sync_directory(directory);
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
