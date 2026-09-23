#ifndef KWAQUE_SRC_RUNTIME_FILE_ERROR_H_
#define KWAQUE_SRC_RUNTIME_FILE_ERROR_H_

#include "src/runtime/error.h"

#include <cstdint>

namespace kwaque::runtime {

// Stable file-domain detail, independent of platform errno values.
enum class file_failure_detail : std::uint8_t {
    unknown = 0,
    no_space = 1,
    descriptor_limit = 2,
    read_only = 3,
    permission = 4,
    device_io = 5,
    // No native attempt; this does not restore a consuming call's arguments.
    admission_not_dispatched = 6,
    quota = 7,
};

[[nodiscard]] inline result<file_failure_detail>
file_detail(const operation_error& error) noexcept {
    if (error.operation() != operation_kind::file)
        return failure(
          operation_error{errc::invalid_argument, operation_kind::file});
    for (std::size_t index = 0; index < error.context_size(); ++index) {
        const auto field = *error.context_at(index);
        if (field.key != operation_context_key::detail) continue;
        if (field.value > static_cast<std::uint8_t>(file_failure_detail::quota))
            return failure(
              operation_error{errc::malformed_data, operation_kind::file});
        return static_cast<file_failure_detail>(field.value);
    }
    return file_failure_detail::unknown;
}

[[nodiscard]] inline operation_error
make_file_error(errc code, file_failure_detail detail) noexcept {
    operation_error result{code, operation_kind::file};
    // Reserve the cause before optional diagnostic fields.
    static_cast<void>(result.add_context(
      operation_context_key::detail, static_cast<std::uint8_t>(detail)));
    return result;
}

[[nodiscard]] inline errc
file_failure_code(file_failure_detail detail) noexcept {
    switch (detail) {
    case file_failure_detail::no_space:
    case file_failure_detail::descriptor_limit:
    case file_failure_detail::quota:
        return errc::resource_exhausted;
    case file_failure_detail::read_only:
    case file_failure_detail::permission:
        return errc::permission_denied;
    case file_failure_detail::admission_not_dispatched:
        return errc::queue_full;
    case file_failure_detail::unknown:
    case file_failure_detail::device_io:
        return errc::io_failure;
    }
    return errc::io_failure;
}

} // namespace kwaque::runtime

#endif // KWAQUE_SRC_RUNTIME_FILE_ERROR_H_
