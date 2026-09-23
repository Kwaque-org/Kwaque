#ifndef KWAQUE_SRC_RUNTIME_PRODUCTION_FILE_H_
#define KWAQUE_SRC_RUNTIME_PRODUCTION_FILE_H_

#include "src/runtime/file.h"
#include "src/runtime/operation_statistics.h"
#include "src/runtime/shard_affinity.h"

#include <memory>
#include <utility>

namespace kwaque::runtime::production {

// close() must be awaited before destruction. A move transfers stable state,
// including a pending next; it does not move the generator's borrowed frame.
// Close policy is fixed at open; checked close retains native failures.
// Concurrent close returns queue_full; the first caller owns the drain. After
// it settles, repeated close returns the cached outcome without another I/O.
class directory_cursor final {
public:
    directory_cursor(directory_cursor&&) noexcept;
    directory_cursor& operator=(directory_cursor&&) noexcept;
    directory_cursor(const directory_cursor&) = delete;
    directory_cursor& operator=(const directory_cursor&) = delete;
    ~directory_cursor();
    [[nodiscard]] seastar::future<result<directory_page>>
    next(directory_page_limits limits);
    // Uses the retained directory; next/sync are single-flight, close joins
    // both.
    [[nodiscard]] seastar::future<result<void>> sync();
    [[nodiscard]] seastar::future<result<void>> close();

private:
    friend class file_system;
    struct state;
    explicit directory_cursor(std::unique_ptr<state> state) noexcept;
    std::unique_ptr<state> state_;
};

class file_system final : public shard_affine {
public:
    using directory_cursor_type = directory_cursor;
    file_system()
      : statistics_(&statistics_owner_.get()) {}
    explicit file_system(operation_statistics_owner statistics)
      : statistics_owner_(std::move(statistics))
      , statistics_(&statistics_owner_.get()) {}

    file_system(const file_system&) = delete;
    file_system& operator=(const file_system&) = delete;
    file_system(file_system&&) = delete;
    file_system& operator=(file_system&&) = delete;

    [[nodiscard]] seastar::future<result<file>>
    open(file_path path, file_open_options options);
    [[nodiscard]] seastar::future<result<directory_cursor>> open_directory(
      file_path path, file_close_policy policy = file_close_policy::legacy);
    [[nodiscard]] seastar::future<result<bool>> exists(file_path path);
    [[nodiscard]] seastar::future<result<file_status>> stat(file_path path);
    [[nodiscard]] seastar::future<result<file_system_space>>
    space(file_path path);
    [[nodiscard]] seastar::future<result<directory_listing>>
    list(file_path path, directory_listing_limits limits);
    [[nodiscard]] seastar::future<result<void>>
    create_directories(file_path path);
    [[nodiscard]] seastar::future<result<void>> remove_file(file_path path);
    [[nodiscard]] seastar::future<result<void>>
    remove_directory(file_path path);
    [[nodiscard]] seastar::future<result<void>> rename(
      file_path source,
      file_path destination,
      file_rename_policy policy = file_rename_policy::replace);
    [[nodiscard]] seastar::future<result<void>> sync_directory(
      file_path path, file_close_policy policy = file_close_policy::legacy);

    [[nodiscard]] operation_statistics_snapshot statistics() const noexcept {
        assert_current();
        return statistics_->snapshot();
    }

private:
    operation_statistics_owner statistics_owner_;
    operation_statistics* statistics_;
    seastar::lw_shared_ptr<seastar::semaphore> cursor_slots_
      = seastar::make_lw_shared<seastar::semaphore>(64);
};

static_assert(kwaque::runtime::file_system_backend<file_system>);

} // namespace kwaque::runtime::production

#endif // KWAQUE_SRC_RUNTIME_PRODUCTION_FILE_H_
