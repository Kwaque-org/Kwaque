#pragma once

#include <seastar/core/abort_source.hh>

#include <exception>

namespace kwaque::codec::testing {

// Prepare cancellation's exception before an observed operation or failure
// sweep. Requesting abort then performs no exception allocation. Supplying an
// exception to request_abort_ex alone is insufficient: the native source also
// evaluates its default exception while choosing the supplied value.
class prepared_abort_source final : public seastar::abort_source {
public:
    prepared_abort_source()
      : exception_(
          std::make_exception_ptr(seastar::abort_requested_exception{})) {}

    std::exception_ptr get_default_exception() const noexcept final {
        return exception_;
    }

private:
    std::exception_ptr exception_;
};

} // namespace kwaque::codec::testing
