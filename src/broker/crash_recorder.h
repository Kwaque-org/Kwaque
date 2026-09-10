#pragma once

#include "src/broker/crash_recorder_writer.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>

#include <cstddef>
#include <filesystem>

namespace kwaque::broker {

class crash_recorder final {
public:
    static constexpr std::size_t existing_reports_to_keep = 50;
    static constexpr std::size_t directory_entry_limit = 4096;

    crash_recorder() = default;
    ~crash_recorder();
    crash_recorder(const crash_recorder&) = delete;
    crash_recorder& operator=(const crash_recorder&) = delete;

    // Coroutine-safe. The caller must hold data-directory ownership throughout
    // start, fatal recording, and stop. Wrappers are installed only after the
    // prepared descriptor and buffers are published.
    [[nodiscard]] seastar::future<> start(
      const std::filesystem::path& data_directory,
      seastar::abort_source& abort,
      bool install_signal_handlers = true);
    // Concurrent calls serialize. An unlinked placeholder's directory sync
    // remains pending until it succeeds, including across failed stop calls.
    [[nodiscard]] seastar::future<> stop();

    // Records only a fixed classification, never exception/configuration text.
    // Before initialization, or after another recording, this is a no-op.
    void record_startup_failure() noexcept;

    [[nodiscard]] const std::filesystem::path& current_report() const noexcept {
        return report_path_;
    }

private:
    friend class crash_recorder_test_access;
    [[nodiscard]] static seastar::future<>
    sync_directory(const std::filesystem::path& directory);
    [[nodiscard]] seastar::future<> prune(seastar::abort_source& abort);

    detail::prepared_crash_writer writer_;
    std::filesystem::path directory_;
    std::filesystem::path report_path_;
    seastar::future<> (*sync_directory_)(const std::filesystem::path&)
      = sync_directory;
    seastar::semaphore cleanup_{1};
    bool placeholder_owned_{false};
    bool handlers_installed_{false};
    bool directory_sync_pending_{false};
};

} // namespace kwaque::broker
