#ifndef KWAQUE_SRC_RUNTIME_FILE_ERROR_INTERNAL_H_
#define KWAQUE_SRC_RUNTIME_FILE_ERROR_INTERNAL_H_

#include "src/base/error.h"

#include <system_error>

namespace kwaque::runtime::detail {

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

} // namespace kwaque::runtime::detail

#endif // KWAQUE_SRC_RUNTIME_FILE_ERROR_INTERNAL_H_
