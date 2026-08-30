#include "src/runtime/production/file.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>

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

errc map_file_system_error(const std::error_code& error) noexcept {
    if (error == std::errc::no_such_file_or_directory) {
        return errc::not_found;
    }
    if (error == std::errc::file_exists) {
        return errc::already_exists;
    }
    if (
      error == std::errc::permission_denied
      || error == std::errc::operation_not_permitted
      || error == std::errc::read_only_file_system) {
        return errc::permission_denied;
    }
    if (error == std::errc::directory_not_empty) {
        return errc::directory_not_empty;
    }
    if (error == std::errc::operation_canceled) {
        return errc::aborted;
    }
    if (error == std::errc::timed_out) {
        return errc::timed_out;
    }
    if (
      error == std::errc::no_space_on_device
      || error == std::errc::too_many_files_open
      || error == std::errc::too_many_files_open_in_system) {
        return errc::resource_exhausted;
    }
    if (error == std::errc::file_too_large) {
        return errc::out_of_range;
    }
    if (error == std::errc::invalid_argument) {
        return errc::invalid_argument;
    }
    if (error == std::errc::is_a_directory) {
        return errc::is_a_directory;
    }
    if (error == std::errc::not_a_directory) {
        return errc::not_a_directory;
    }
    return errc::io_failure;
}

operation_error file_system_error_from_exception(std::exception_ptr exception) {
    try {
        std::rethrow_exception(std::move(exception));
    } catch (const std::bad_alloc&) {
        throw;
    } catch (const seastar::cancelled_error&) {
        return file_system_error(errc::aborted);
    } catch (const std::system_error& error) {
        return file_system_error(map_file_system_error(error.code()));
    } catch (...) {
        return file_system_error(errc::io_failure);
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

seastar::future<result<file>>
file_system::open(file_path path, file_open_options options) {
    assert_current();
    if (auto valid = options.validate(); !valid) {
        co_return failure(valid.error());
    }
    try {
        seastar::file_open_options native_options;
        native_options.create_permissions
          = static_cast<seastar::file_permissions>(options.permissions);
        native_options.durable = true;
        auto native = co_await seastar::open_file_dma(
          path.value(), native_open_flags(options), native_options);
        co_return file{std::move(native)};
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        co_return failure(
          file_system_error_from_exception(std::current_exception()));
    }
}

seastar::future<result<bool>> file_system::exists(file_path path) {
    assert_current();
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

seastar::future<result<directory_listing>>
file_system::list(file_path path, directory_listing_limits limits) {
    assert_current();
    if (auto valid = limits.validate(); !valid) {
        co_return failure(valid.error());
    }

    seastar::file directory;
    seastar::chunked_vector<directory_entry> entries;
    std::optional<operation_error> rejected;
    std::exception_ptr exception;
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
                  std::string{native->name.data(), native->name.size()});
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
    } catch (const std::bad_alloc&) {
        exception = std::current_exception();
    } catch (...) {
        exception = std::current_exception();
    }

    if (directory) {
        try {
            co_await directory.close();
        } catch (...) {
            if (!exception) {
                exception = std::current_exception();
            }
        }
    }
    if (exception) {
        try {
            std::rethrow_exception(exception);
        } catch (const std::bad_alloc&) {
            throw;
        } catch (...) {
            co_return failure(file_system_error_from_exception(exception));
        }
    }
    if (rejected) {
        co_return failure(std::move(*rejected));
    }
    co_return directory_listing::make(std::move(entries), limits);
}

seastar::future<result<void>> file_system::create_directories(file_path path) {
    assert_current();
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
    try {
        co_await seastar::remove_file(path.value());
        co_return result<void>{};
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        co_return failure(
          file_system_error_from_exception(std::current_exception()));
    }
}

seastar::future<result<void>> file_system::remove_directory(file_path path) {
    return remove_file(std::move(path));
}

seastar::future<result<void>>
file_system::rename(file_path source, file_path destination) {
    assert_current();
    try {
        co_await seastar::rename_file(source.value(), destination.value());
        co_return result<void>{};
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        co_return failure(
          file_system_error_from_exception(std::current_exception()));
    }
}

seastar::future<result<void>> file_system::sync_directory(file_path path) {
    assert_current();
    try {
        co_await seastar::sync_directory(path.value());
        co_return result<void>{};
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        co_return failure(
          file_system_error_from_exception(std::current_exception()));
    }
}

} // namespace kwaque::runtime::production
