#include "src/base/error.h"

#include <string>

namespace kwaque {

namespace {

class kwaque_error_category final : public std::error_category {
public:
    [[nodiscard]] const char* name() const noexcept final { return "kwaque"; }

    [[nodiscard]] std::string message(int value) const final {
        switch (static_cast<errc>(value)) {
        case errc::success:
            return "success";
        case errc::invalid_argument:
            return "invalid argument";
        case errc::out_of_range:
            return "value out of range";
        case errc::malformed_data:
            return "malformed data";
        case errc::unavailable:
            return "service unavailable";
        case errc::aborted:
            return "operation aborted";
        case errc::closed:
            return "resource closed";
        case errc::timed_out:
            return "operation timed out";
        case errc::resource_exhausted:
            return "resource exhausted";
        case errc::queue_full:
            return "queue full";
        case errc::wrong_shard:
            return "wrong shard";
        case errc::io_failure:
            return "I/O failure";
        case errc::network_failure:
            return "network failure";
        case errc::dns_failure:
            return "DNS failure";
        case errc::fault_injected:
            return "fault injected";
        case errc::replay_divergence:
            return "replay divergence";
        case errc::invariant_violation:
            return "invariant violation";
        case errc::truncated_data:
            return "truncated data";
        case errc::not_found:
            return "not found";
        case errc::already_exists:
            return "already exists";
        case errc::permission_denied:
            return "permission denied";
        case errc::directory_not_empty:
            return "directory not empty";
        case errc::is_a_directory:
            return "is a directory";
        case errc::not_a_directory:
            return "not a directory";
        }
        return "unknown Kwaque error";
    }

    [[nodiscard]] std::error_condition
    default_error_condition(int value) const noexcept final {
        switch (static_cast<errc>(value)) {
        case errc::invalid_argument:
        case errc::malformed_data:
            return std::make_error_condition(std::errc::invalid_argument);
        case errc::out_of_range:
            return std::make_error_condition(std::errc::result_out_of_range);
        case errc::aborted:
            return std::make_error_condition(std::errc::operation_canceled);
        case errc::timed_out:
            return std::make_error_condition(std::errc::timed_out);
        case errc::queue_full:
            return std::make_error_condition(std::errc::no_buffer_space);
        case errc::io_failure:
            return std::make_error_condition(std::errc::io_error);
        case errc::not_found:
            return std::make_error_condition(
              std::errc::no_such_file_or_directory);
        case errc::already_exists:
            return std::make_error_condition(std::errc::file_exists);
        case errc::permission_denied:
            return std::make_error_condition(std::errc::permission_denied);
        case errc::directory_not_empty:
            return std::make_error_condition(std::errc::directory_not_empty);
        case errc::is_a_directory:
            return std::make_error_condition(std::errc::is_a_directory);
        case errc::not_a_directory:
            return std::make_error_condition(std::errc::not_a_directory);
        case errc::unavailable:
        case errc::closed:
        case errc::resource_exhausted:
        case errc::network_failure:
        case errc::dns_failure:
        case errc::wrong_shard:
        case errc::fault_injected:
        case errc::replay_divergence:
        case errc::invariant_violation:
        case errc::truncated_data:
            return {value, *this};
        case errc::success:
            return std::error_condition{};
        }
        return {value, *this};
    }
};

} // namespace

const std::error_category& error_category() noexcept {
    static const kwaque_error_category category;
    return category;
}

std::error_code make_error_code(errc error) noexcept {
    return {static_cast<int>(error), error_category()};
}

} // namespace kwaque
