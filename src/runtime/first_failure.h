#ifndef KWAQUE_SRC_RUNTIME_FIRST_FAILURE_H_
#define KWAQUE_SRC_RUNTIME_FIRST_FAILURE_H_

#include "src/runtime/error.h"

#include <exception>
#include <optional>
#include <utility>

namespace kwaque::runtime {

// One owner-local first failure. Recording and copying allocate no diagnostic
// storage. An exception and an operational error preserve their original kind.
class first_failure final {
public:
    void observe(operation_error error) noexcept {
        if (!failed()) error_ = std::move(error);
    }
    void observe(std::exception_ptr error) noexcept {
        if (!failed()) exception_ = std::move(error);
    }
    template<typename T>
    void observe(const result<T>& value) noexcept {
        if (!value) observe(value.error());
    }
    [[nodiscard]] bool failed() const noexcept {
        return error_.has_value() || bool(exception_);
    }
    [[nodiscard]] const std::optional<operation_error>& error() const noexcept {
        return error_;
    }
    [[nodiscard]] std::exception_ptr exception() const noexcept {
        return exception_;
    }
    [[nodiscard]] result<void> outcome() const {
        if (exception_) std::rethrow_exception(exception_);
        if (error_) return failure(*error_);
        return {};
    }

private:
    std::optional<operation_error> error_;
    std::exception_ptr exception_;
};

} // namespace kwaque::runtime

#endif // KWAQUE_SRC_RUNTIME_FIRST_FAILURE_H_
