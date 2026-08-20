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
        case errc::unavailable:
            return std::make_error_condition(
              std::errc::resource_unavailable_try_again);
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
