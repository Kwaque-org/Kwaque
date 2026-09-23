#include "src/runtime/production/file.h"

#include "src/base/invariant.h"
#include "src/runtime/directory_cursor_internal.h"
#include "src/runtime/file_error_internal.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/defer.hh>

#include <sys/statvfs.h>

#include <algorithm>
#include <cstdint>
#include <new>
#include <optional>
#include <system_error>
#include <utility>

namespace kwaque::runtime::production {

namespace {

operation_error file_system_error(errc code) noexcept {
    return operation_error{code, operation_kind::file};
}

operation_error file_system_error_from_exception(std::exception_ptr exception) {
    try {
        std::rethrow_exception(std::move(exception));
    } catch (const kwaque::runtime::detail::file_operation_exception& error) {
        return error.error();
    } catch (const seastar::cancelled_error&) {
        return file_system_error(errc::aborted);
    } catch (const std::system_error& error) {
        return kwaque::runtime::detail::map_file_operation_error(error.code());
    }
}

seastar::open_flags native_open_flags(const file_open_options& options) {
    seastar::open_flags flags = seastar::open_flags::ro;
    switch (options.access) {
    case file_access::read_only:
        flags = seastar::open_flags::ro;
        break;
    case file_access::write_only:
        flags = seastar::open_flags::wo;
        break;
    case file_access::read_write:
        flags = seastar::open_flags::rw;
        break;
    }
    if (options.create) {
        flags |= seastar::open_flags::create;
    }
    if (options.exclusive) {
        flags |= seastar::open_flags::exclusive;
    }
    if (options.truncate) {
        flags |= seastar::open_flags::truncate;
    }
    return flags;
}

std::optional<file_kind>
native_file_kind(seastar::directory_entry_type type) noexcept {
    switch (type) {
    case seastar::directory_entry_type::regular:
        return file_kind::regular;
    case seastar::directory_entry_type::directory:
        return file_kind::directory;
    case seastar::directory_entry_type::unknown:
        return std::nullopt;
    case seastar::directory_entry_type::block_device:
    case seastar::directory_entry_type::char_device:
    case seastar::directory_entry_type::fifo:
    case seastar::directory_entry_type::link:
    case seastar::directory_entry_type::socket:
        return file_kind::other;
    }
    return std::nullopt;
}

} // namespace

struct directory_cursor::state final : shard_affine {
    state(
      seastar::lw_shared_ptr<detail::directory_cursor_memory> budget,
      operation_statistics_owner metrics)
      : memory(std::move(budget))
      , statistics(std::move(metrics)) {}

    ~state() {
        assert_current();
        KWAQUE_INVARIANT(
          invariant_id{"KQ-DIRECTORY-CLOSED"},
          !directory && !busy && !closing,
          "directory cursor destroyed before joined close");
    }

    seastar::future<result<directory_page>> read_page(
      directory_page_limits limits,
      seastar::semaphore_units<> reservation,
      seastar::gate::holder holder) {
        busy = true;
        [[maybe_unused]] auto metric = statistics.get().accept();
        auto idle = seastar::defer([this] noexcept { busy = false; });
        static_cast<void>(holder);
        try {
            seastar::chunked_vector<directory_entry> entries;
            std::uint64_t name_bytes = 0;
            while (!end && entries.size() < limits.maximum_entries.value()) {
                if (!lookahead) lookahead = co_await (*lister)();
                if (!lookahead) {
                    end = true;
                    break;
                }
                const auto current_name_bytes = lookahead->name.size();
                if (current_name_bytes > maximum_file_name_bytes) {
                    failed = file_system_error(errc::out_of_range);
                    co_return failure(*failed);
                }
                if (
                  current_name_bytes
                  > limits.maximum_name_bytes.value() - name_bytes) {
                    if (entries.empty())
                        co_return failure(
                          file_system_error(errc::resource_exhausted));
                    break;
                }
                auto name = file_name::make(
                  std::string_view{
                    lookahead->name.data(), lookahead->name.size()});
                if (!name) {
                    failed = name.error();
                    co_return failure(*failed);
                }
                auto kind = lookahead->type ? native_file_kind(*lookahead->type)
                                            : std::optional<file_kind>{};
                if (!kind) {
                    const auto status = co_await seastar::file_stat(
                      directory, lookahead->name, seastar::follow_symlink::no);
                    kind = native_file_kind(status.type);
                }
                if (!kind) {
                    failed = file_system_error(errc::io_failure);
                    co_return failure(*failed);
                }
                entries.push_back(
                  directory_entry{.name = std::move(*name), .kind = *kind});
                name_bytes += current_name_bytes;
                lookahead.reset();
            }
            auto listing = directory_listing::make(
              std::move(entries),
              {.maximum_entries = limits.maximum_entries,
               .maximum_name_bytes = limits.maximum_name_bytes});
            if (!listing) {
                failed = listing.error();
                co_return failure(*failed);
            }
            co_return directory_page{
              std::move(*listing), end, memory, std::move(reservation)};
        } catch (...) {
            try {
                failed = file_system_error_from_exception(
                  std::current_exception());
            } catch (...) {
                exception = std::current_exception();
                throw;
            }
            co_return failure(*failed);
        }
    }

    seastar::future<result<void>> sync_directory(seastar::gate::holder holder) {
        busy = true;
        auto idle = seastar::defer([this] noexcept { busy = false; });
        static_cast<void>(holder);
        [[maybe_unused]] auto metric = statistics.get().accept();
        try {
            co_await directory.flush();
        } catch (...) {
            try {
                failed = file_system_error_from_exception(
                  std::current_exception());
            } catch (...) {
                exception = std::current_exception();
                throw;
            }
            co_return failure(*failed);
        }
        co_return result<void>{};
    }

    seastar::future<result<void>> drain() {
        closing = true;
        [[maybe_unused]] auto metric = statistics.get().accept();
        co_await operations.close();
        // No generator awaiter or relative stat can still borrow this frame/fd.
        lister.reset();
        lookahead.reset();
        try {
            if (close_policy == file_close_policy::checked)
                co_await directory.close_checked();
            else
                co_await directory.close();
        } catch (...) {
            if (!exception && !failed) {
                try {
                    failed = file_system_error_from_exception(
                      std::current_exception());
                } catch (...) {
                    exception = std::current_exception();
                }
            }
        }
        directory = {};
        memory = {};
        closing = false;
        if (exception) std::rethrow_exception(exception);
        if (failed) co_return failure(*failed);
        co_return result<void>{};
    }

    seastar::lw_shared_ptr<detail::directory_cursor_memory> memory;
    operation_statistics_owner statistics;
    seastar::file directory;
    std::optional<seastar::list_directory_generator_type> lister;
    std::optional<seastar::directory_entry> lookahead;
    seastar::gate operations;
    std::optional<operation_error> failed;
    std::exception_ptr exception;
    file_close_policy close_policy{file_close_policy::legacy};
    bool busy{false}, closing{false}, end{false};
};

directory_cursor::directory_cursor(std::unique_ptr<state> value) noexcept
  : state_(std::move(value)) {}
directory_cursor::directory_cursor(directory_cursor&& other) noexcept {
    if (other.state_) other.state_->assert_current();
    state_ = std::move(other.state_);
}
directory_cursor&
directory_cursor::operator=(directory_cursor&& other) noexcept {
    if (state_) state_->assert_current();
    if (other.state_) other.state_->assert_current();
    if (this != &other) state_ = std::move(other.state_);
    return *this;
}
directory_cursor::~directory_cursor() = default;

seastar::future<result<directory_page>>
directory_cursor::next(directory_page_limits limits) {
    if (!state_)
        return seastar::make_ready_future<result<directory_page>>(
          failure(file_system_error(errc::closed)));
    auto& state = *state_;
    state.assert_current();
    auto reject = [&state](operation_error error) {
        state.statistics.get().reject();
        return seastar::make_ready_future<result<directory_page>>(
          failure(std::move(error)));
    };
    if (auto valid = limits.validate(); !valid) return reject(valid.error());
    if (!state.directory || state.closing)
        return reject(file_system_error(errc::closed));
    if (state.exception)
        return seastar::make_exception_future<result<directory_page>>(
          state.exception);
    if (state.failed) return reject(*state.failed);
    auto reservation = seastar::try_get_units(state.memory->page, 1);
    if (state.busy || !reservation)
        return reject(make_file_error(
          errc::queue_full, file_failure_detail::admission_not_dispatched));
    auto holder = state.operations.hold();
    return state.read_page(limits, std::move(*reservation), std::move(holder));
}

seastar::future<result<void>> directory_cursor::sync() {
    if (!state_)
        return seastar::make_ready_future<result<void>>(
          failure(file_system_error(errc::closed)));
    auto& state = *state_;
    state.assert_current();
    auto reject = [&state](operation_error error) {
        state.statistics.get().reject();
        return seastar::make_ready_future<result<void>>(
          failure(std::move(error)));
    };
    if (!state.directory || state.closing)
        return reject(file_system_error(errc::closed));
    if (state.exception)
        return seastar::make_exception_future<result<void>>(state.exception);
    if (state.failed) return reject(*state.failed);
    if (state.busy)
        return reject(make_file_error(
          errc::queue_full, file_failure_detail::admission_not_dispatched));
    return state.sync_directory(state.operations.hold());
}

seastar::future<result<void>> directory_cursor::close() {
    if (!state_)
        return seastar::make_ready_future<result<void>>(result<void>{});
    auto& state = *state_;
    state.assert_current();
    if (state.closing)
        return seastar::make_ready_future<result<void>>(failure(make_file_error(
          errc::queue_full, file_failure_detail::admission_not_dispatched)));
    if (!state.directory) {
        if (state.exception)
            return seastar::make_exception_future<result<void>>(
              state.exception);
        return seastar::make_ready_future<result<void>>(
          state.failed ? result<void>{failure(*state.failed)} : result<void>{});
    }
    return state.drain();
}

seastar::future<result<directory_cursor>>
file_system::open_directory(file_path path, file_close_policy policy) {
    if (
      policy != file_close_policy::legacy
      && policy != file_close_policy::checked)
        co_return failure(file_system_error(errc::invalid_argument));
    assert_current();
    auto slot = seastar::try_get_units(*cursor_slots_, 1);
    if (!slot) {
        statistics_->reject();
        co_return failure(make_file_error(
          errc::queue_full, file_failure_detail::admission_not_dispatched));
    }
    [[maybe_unused]] auto metric = statistics_->accept();
    auto memory = seastar::make_lw_shared<detail::directory_cursor_memory>(
      cursor_slots_, std::move(*slot));
    auto state = std::make_unique<directory_cursor::state>(
      std::move(memory), statistics_owner_);
    state->close_policy = policy;
    std::exception_ptr exception;
    try {
        state->directory = co_await seastar::open_directory(path.value());
        state->lister.emplace(state->directory.experimental_list_directory());
    } catch (...) {
        exception = std::current_exception();
    }
    if (exception) {
        if (state->directory) {
            try {
                if (policy == file_close_policy::checked)
                    co_await state->directory.close_checked();
                else
                    co_await state->directory.close();
            } catch (...) {
                // Construction failed first; cleanup must not replace it.
            }
            state->directory = {};
        }
        co_return failure(file_system_error_from_exception(exception));
    }
    co_return directory_cursor{std::move(state)};
}

seastar::future<result<file>>
file_system::open(file_path path, file_open_options options) {
    assert_current();
    if (auto valid = options.validate(); !valid) {
        statistics_->reject();
        co_return failure(valid.error());
    }
    [[maybe_unused]] auto metric = statistics_->accept();
    try {
        seastar::file_open_options native_options;
        native_options.create_permissions
          = static_cast<seastar::file_permissions>(options.permissions);
        native_options.durable = true;
        auto native = co_await seastar::open_file_dma(
          path.value(), native_open_flags(options), native_options);
        co_return file{
          std::move(native),
          file_io_limits{},
          statistics_owner_,
          options.close_policy};
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        co_return failure(
          file_system_error_from_exception(std::current_exception()));
    }
}

seastar::future<result<bool>> file_system::exists(file_path path) {
    assert_current();
    [[maybe_unused]] auto metric = statistics_->accept();
    try {
        co_return co_await seastar::file_exists(path.value());
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        co_return failure(
          file_system_error_from_exception(std::current_exception()));
    }
}

seastar::future<result<file_status>> file_system::stat(file_path path) {
    assert_current();
    [[maybe_unused]] auto metric = statistics_->accept();
    try {
        const auto native = co_await seastar::file_stat(
          path.value(), seastar::follow_symlink::no);
        const auto kind = native_file_kind(native.type);
        if (!kind) {
            co_return failure(file_system_error(errc::io_failure));
        }
        co_return file_status{.kind = *kind, .size = byte_count{native.size}};
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        co_return failure(
          file_system_error_from_exception(std::current_exception()));
    }
}

seastar::future<result<file_system_space>> file_system::space(file_path path) {
    assert_current();
    [[maybe_unused]] auto metric = statistics_->accept();
    try {
        const auto native = co_await seastar::engine().statvfs(path.value());
        co_return file_system_space::from_blocks(
          native.f_frsize,
          native.f_blocks,
          native.f_bfree,
          native.f_bavail,
          (native.f_flag & ST_RDONLY) != 0);
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        co_return failure(
          file_system_error_from_exception(std::current_exception()));
    }
}

seastar::future<result<directory_listing>>
file_system::list(file_path path, directory_listing_limits limits) {
    assert_current();
    if (auto valid = limits.validate(); !valid) {
        statistics_->reject();
        co_return failure(valid.error());
    }
    [[maybe_unused]] auto metric = statistics_->accept();

    seastar::file directory;
    seastar::chunked_vector<directory_entry> entries;
    std::optional<operation_error> rejected;
    std::optional<operation_error> operational_failure;
    std::exception_ptr exception;
    auto record_failure = [&](std::exception_ptr failure) noexcept {
        try {
            auto error = file_system_error_from_exception(failure);
            if (!operational_failure) {
                operational_failure = std::move(error);
            }
        } catch (...) {
            if (!exception) {
                exception = std::current_exception();
            }
        }
    };
    try {
        directory = co_await seastar::open_directory(path.value());
        std::uint64_t name_bytes = 0;
        {
            auto lister = directory.experimental_list_directory();
            while (auto native = co_await lister()) {
                if (entries.size() == limits.maximum_entries.value()) {
                    rejected = file_system_error(errc::resource_exhausted);
                    break;
                }
                const auto current_name_bytes = static_cast<std::uint64_t>(
                  native->name.size());
                if (current_name_bytes > maximum_file_name_bytes) {
                    rejected = file_system_error(errc::out_of_range);
                    break;
                }
                if (
                  current_name_bytes
                  > limits.maximum_name_bytes.value() - name_bytes) {
                    rejected = file_system_error(errc::resource_exhausted);
                    break;
                }

                auto name = file_name::make(
                  std::string_view{native->name.data(), native->name.size()});
                if (!name) {
                    rejected = name.error();
                    break;
                }

                auto kind = native->type ? native_file_kind(*native->type)
                                         : std::optional<file_kind>{};
                if (!kind) {
                    const auto status = co_await seastar::file_stat(
                      directory, native->name, seastar::follow_symlink::no);
                    kind = native_file_kind(status.type);
                }
                if (!kind) {
                    rejected = file_system_error(errc::io_failure);
                    break;
                }

                name_bytes += current_name_bytes;
                entries.push_back(
                  directory_entry{
                    .name = std::move(*name),
                    .kind = *kind,
                  });
            }
        }
    } catch (...) {
        record_failure(std::current_exception());
    }

    if (directory) {
        try {
            co_await directory.close();
        } catch (...) {
            record_failure(std::current_exception());
        }
    }
    if (exception) {
        std::rethrow_exception(exception);
    }
    if (operational_failure) {
        co_return failure(std::move(*operational_failure));
    }
    if (rejected) {
        co_return failure(std::move(*rejected));
    }
    co_return directory_listing::make(std::move(entries), limits);
}

seastar::future<result<void>> file_system::create_directories(file_path path) {
    assert_current();
    [[maybe_unused]] auto metric = statistics_->accept();
    try {
        co_await seastar::recursive_touch_directory(path.value());
        co_return result<void>{};
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        co_return failure(
          file_system_error_from_exception(std::current_exception()));
    }
}

seastar::future<result<void>> file_system::remove_file(file_path path) {
    assert_current();
    [[maybe_unused]] auto metric = statistics_->accept();
    try {
        co_await seastar::unlink_file(path.value());
        co_return result<void>{};
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        co_return failure(
          file_system_error_from_exception(std::current_exception()));
    }
}

seastar::future<result<void>> file_system::remove_directory(file_path path) {
    assert_current();
    [[maybe_unused]] auto metric = statistics_->accept();
    try {
        co_await seastar::remove_directory(path.value());
        co_return result<void>{};
    } catch (...) {
        co_return failure(
          file_system_error_from_exception(std::current_exception()));
    }
}

seastar::future<result<void>> file_system::rename(
  file_path source, file_path destination, file_rename_policy policy) {
    assert_current();
    [[maybe_unused]] auto metric = statistics_->accept();
    try {
        if (
          policy != file_rename_policy::replace
          && policy != file_rename_policy::no_replace)
            co_return failure(make_file_error(
              errc::invalid_argument,
              file_failure_detail::admission_not_dispatched));
        co_await seastar::rename_file(
          source.value(),
          destination.value(),
          policy == file_rename_policy::no_replace
            ? seastar::rename_flags::noreplace
            : seastar::rename_flags::none);
        co_return result<void>{};
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        co_return failure(
          file_system_error_from_exception(std::current_exception()));
    }
}

seastar::future<result<void>>
file_system::sync_directory(file_path path, file_close_policy policy) {
    assert_current();
    if (
      policy != file_close_policy::legacy
      && policy != file_close_policy::checked)
        co_return failure(file_system_error(errc::invalid_argument));
    [[maybe_unused]] auto metric = statistics_->accept();
    if (policy == file_close_policy::legacy) {
        try {
            co_await seastar::sync_directory(path.value());
            co_return result<void>{};
        } catch (...) {
            co_return failure(
              file_system_error_from_exception(std::current_exception()));
        }
    }
    first_failure failed;
    seastar::file directory;
    const auto remember = [&failed](std::exception_ptr exception) {
        if (failed.failed()) return;
        try {
            failed.observe(file_system_error_from_exception(exception));
        } catch (...) {
            failed.observe(std::current_exception());
        }
    };
    try {
        directory = co_await seastar::open_directory(path.value());
        co_await directory.flush();
    } catch (...) {
        remember(std::current_exception());
    }
    if (directory) {
        try {
            co_await directory.close_checked();
        } catch (...) {
            remember(std::current_exception());
        }
    }
    co_return failed.outcome();
}

} // namespace kwaque::runtime::production
