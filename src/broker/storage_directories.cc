#include "src/broker/storage_directories.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>

#include <string>

namespace kwaque::broker {
namespace {
runtime::result<void> mismatch() {
    return runtime::failure(
      runtime::operation_error{
        errc::wrong_context, runtime::operation_kind::file});
}
seastar::future<runtime::result<void>>
check_directory(const storage::local_device_spec& spec) {
    const auto& root = spec.root.value();
    std::size_t end = 1;
    for (;;) {
        auto status = co_await seastar::file_stat(
          root.substr(0, end), seastar::follow_symlink::no);
        if (status.type != seastar::directory_entry_type::directory)
            co_return mismatch();
        if (end == root.size()) {
            if (
              status.device_id != spec.directory.device
              || status.inode_number != spec.directory.inode)
                co_return mismatch();
            break;
        }
        auto next = root.find('/', end + 1);
        end = next == std::string::npos ? root.size() : next;
    }
    if (spec.mount_marker) {
        auto marker = storage::local_child_path(spec.root, *spec.mount_marker);
        if (!marker) co_return runtime::failure(marker.error());
        auto status = co_await seastar::file_stat(
          marker->value(), seastar::follow_symlink::no);
        if (
          status.type != seastar::directory_entry_type::regular
          || status.device_id != spec.directory.device)
            co_return mismatch();
    }
    co_return runtime::result<void>{};
}
} // namespace
seastar::future<runtime::result<std::unique_ptr<storage_directories>>>
storage_directories::acquire(
  std::span<const storage::local_device_spec> specs,
  std::span<const pid_file* const> borrowed) {
    if (auto valid = storage::validate_local_device_set(specs); !valid)
        co_return runtime::failure(valid.error());
    if (borrowed.size() > storage::maximum_local_devices)
        co_return runtime::failure(
          runtime::operation_error{
            errc::invalid_argument, runtime::operation_kind::file});
    auto result = std::unique_ptr<storage_directories>{new storage_directories};
    result->entries_.reserve(specs.size());
    for (const auto& spec : specs) {
        if (!spec.allow_pid_file)
            co_return runtime::failure(
              runtime::operation_error{
                errc::invalid_argument, runtime::operation_kind::file});
        result->entries_.push_back(entry{spec, {}, {}});
    }
    // Check the complete device set before changing even broker-owned PID
    // files.
    for (const auto& entry : result->entries_) {
        auto checked = co_await check_directory(entry.spec);
        if (!checked) co_return runtime::failure(checked.error());
    }
    for (auto& entry : result->entries_) {
        const auto path = std::filesystem::path{entry.spec.root.value()}
                          / "kwaque.pid";
        for (const auto* existing : borrowed) {
            if (existing && existing->path() == path) {
                if (entry.lock)
                    co_return runtime::failure(
                      runtime::operation_error{
                        errc::invalid_argument, runtime::operation_kind::file});
                entry.lock = existing;
            }
        }
        if (!entry.lock) {
            entry.owned = std::make_unique<pid_file>(path);
            entry.lock = entry.owned.get();
        }
    }
    for (const auto& entry : result->entries_) {
        auto checked = co_await result->validate(entry.spec);
        if (!checked) co_return runtime::failure(checked.error());
    }
    co_return result;
}
seastar::future<runtime::result<void>>
storage_directories::validate(const storage::local_device_spec& spec) {
    for (const auto& entry : entries_) {
        if (entry.spec != spec) continue;
        auto checked = co_await check_directory(spec);
        if (!checked) co_return checked;
        const auto status = co_await seastar::file_stat(
          entry.lock->path().string(), seastar::follow_symlink::no);
        const auto acquired = entry.lock->identity();
        if (
          status.type != seastar::directory_entry_type::regular
          || status.device_id != acquired.device
          || status.inode_number != acquired.inode)
            co_return mismatch();
        co_return runtime::result<void>{};
    }
    co_return mismatch();
}
} // namespace kwaque::broker
