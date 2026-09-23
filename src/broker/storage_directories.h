#pragma once

#include "src/broker/pid_file.h"
#include "src/storage/local_store_config.h"

#include <memory>
#include <span>
#include <vector>

namespace kwaque::broker {
// Startup composition. Every configured directory is checked and locked before
// storage inspection. A borrowed primary PID lock and this owner outlive all
// storage work; additional directory locks are retained here through teardown.
class storage_directories final {
public:
    [[nodiscard]] static seastar::future<
      runtime::result<std::unique_ptr<storage_directories>>>
    acquire(
      std::span<const storage::local_device_spec>,
      std::span<const pid_file* const> borrowed = {});
    storage_directories(const storage_directories&) = delete;
    storage_directories& operator=(const storage_directories&) = delete;
    [[nodiscard]] seastar::future<runtime::result<void>>
    validate(const storage::local_device_spec&);

private:
    struct entry final {
        storage::local_device_spec spec;
        std::unique_ptr<pid_file> owned;
        const pid_file* lock{nullptr};
    };
    storage_directories() = default;
    std::vector<entry> entries_;
};
static_assert(storage::local_directory_owner<storage_directories>);
} // namespace kwaque::broker
