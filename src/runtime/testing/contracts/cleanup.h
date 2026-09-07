#ifndef KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_CLEANUP_H_
#define KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_CLEANUP_H_

#include "src/runtime/error.h"

#include <seastar/core/future.hh>

#include <exception>
#include <stdexcept>
#include <utility>

namespace kwaque::runtime::testing {

// Native finally() propagates exceptions but discards successful future values.
// Convert a typed cleanup failure before passing that future to finally().
inline void require_cleanup(result<void> outcome) {
    if (!outcome) {
        throw std::runtime_error(outcome.error().render());
    }
}

inline void retain_cleanup_failure(std::exception_ptr& failure) {
    const auto cleanup_failure = std::current_exception();
    // Match native finally(): inner is cleanup; outer is the original failure.
    failure = failure ? std::make_exception_ptr(
                          seastar::nested_exception{
                            cleanup_failure, std::move(failure)})
                      : cleanup_failure;
}

} // namespace kwaque::runtime::testing

#endif // KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_CLEANUP_H_
