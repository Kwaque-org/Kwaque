#ifndef KWAQUE_SRC_RUNTIME_FILE_ERROR_INTERNAL_H_
#define KWAQUE_SRC_RUNTIME_FILE_ERROR_INTERNAL_H_

#include "src/runtime/file_error.h"

#include <cerrno>
#include <exception>
#include <system_error>

namespace kwaque::runtime::detail {

// Owning, bounded transport through an exception-only native file interface.
// This deliberately carries no surrogate errno to be reclassified later.
class file_operation_exception final : public std::exception {
public:
    explicit file_operation_exception(operation_error error) noexcept
      : error_(std::move(error)) {}
    [[nodiscard]] const operation_error& error() const noexcept {
        return error_;
    }
    const char* what() const noexcept override {
        return "file operation failed";
    }

private:
    operation_error error_;
};

inline file_failure_detail
native_file_detail(const std::error_code& error) noexcept {
    if (error == std::errc::no_space_on_device)
        return file_failure_detail::no_space;
    if (
      error == std::errc::too_many_files_open
      || error == std::errc::too_many_files_open_in_system)
        return file_failure_detail::descriptor_limit;
    if (error == std::errc::read_only_file_system)
        return file_failure_detail::read_only;
    if (
      error == std::errc::permission_denied
      || error == std::errc::operation_not_permitted)
        return file_failure_detail::permission;
    if (error == std::errc::io_error) return file_failure_detail::device_io;
    if (error == std::error_condition{EDQUOT, std::generic_category()})
        return file_failure_detail::quota;
    return file_failure_detail::unknown;
}

inline errc map_file_system_error(const std::error_code& error) noexcept {
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
    if (error == std::error_condition{EDQUOT, std::generic_category()})
        return errc::resource_exhausted;
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

inline operation_error
map_file_operation_error(const std::error_code& error) noexcept {
    return make_file_error(
      map_file_system_error(error), native_file_detail(error));
}

} // namespace kwaque::runtime::detail

#endif // KWAQUE_SRC_RUNTIME_FILE_ERROR_INTERNAL_H_
