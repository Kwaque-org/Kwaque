#ifndef KWAQUE_SRC_RUNTIME_PRODUCTION_FILE_H_
#define KWAQUE_SRC_RUNTIME_PRODUCTION_FILE_H_

#include "src/runtime/file.h"
#include "src/runtime/operation_statistics.h"
#include "src/runtime/shard_affinity.h"

#include <utility>

namespace kwaque::runtime::production {

class file_system final : public shard_affine {
public:
    file_system()
      : statistics_(&statistics_owner_.get()) {}
    explicit file_system(operation_statistics_owner statistics) noexcept
      : statistics_owner_(std::move(statistics))
      , statistics_(&statistics_owner_.get()) {}

    file_system(const file_system&) = delete;
    file_system& operator=(const file_system&) = delete;
    file_system(file_system&&) = delete;
    file_system& operator=(file_system&&) = delete;

    [[nodiscard]] seastar::future<result<file>>
    open(file_path path, file_open_options options);
    [[nodiscard]] seastar::future<result<bool>> exists(file_path path);
    [[nodiscard]] seastar::future<result<file_status>> stat(file_path path);
    [[nodiscard]] seastar::future<result<directory_listing>>
    list(file_path path, directory_listing_limits limits);
    [[nodiscard]] seastar::future<result<void>>
    create_directories(file_path path);
    [[nodiscard]] seastar::future<result<void>> remove_file(file_path path);
    [[nodiscard]] seastar::future<result<void>>
    remove_directory(file_path path);
    [[nodiscard]] seastar::future<result<void>>
    rename(file_path source, file_path destination);
    [[nodiscard]] seastar::future<result<void>> sync_directory(file_path path);

    [[nodiscard]] operation_statistics_snapshot statistics() const noexcept {
        assert_current();
        return statistics_->snapshot();
    }

private:
    operation_statistics_owner statistics_owner_;
    operation_statistics* statistics_;
};

static_assert(kwaque::runtime::file_system_backend<file_system>);

} // namespace kwaque::runtime::production

#endif // KWAQUE_SRC_RUNTIME_PRODUCTION_FILE_H_
