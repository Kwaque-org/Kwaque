#ifndef KWAQUE_SRC_SIMULATION_TESTS_FUZZ_CASES_H_
#define KWAQUE_SRC_SIMULATION_TESTS_FUZZ_CASES_H_

#include "src/runtime/error.h"
#include "src/runtime/testing/contracts/cleanup.h"
#include "src/simulation/scheduler_driver.h"
#include "src/simulation/tests/fuzz_reproduction.h"

#include <seastar/core/future.hh>

#include <cstddef>
#include <cstdint>
#include <exception>
#include <optional>
#include <span>
#include <system_error>
#include <type_traits>
#include <utility>

namespace kwaque::simulation::testing {

namespace fuzz_case_detail {

// Called only from a Seastar thread. A native file/socket continuation can
// still own progress after its last deterministic event has completed. The
// shared driver's external watchdog bounds that wait without making host time
// part of the simulated clock, ordering, or reproduction artifacts.
template<typename T>
T wait_for(scheduler& events, seastar::future<T> waiting) {
    try {
        pump_until(events, waiting).get();
    } catch (const std::system_error& error) {
        if (
          !events.trace_failed()
          || error.code() != make_error_code(errc::replay_divergence)) {
            throw;
        }
        // A rejected selection leaves admitted callbacks owned by the poisoned
        // scheduler. Discard them before awaiting their native continuations,
        // so a file/socket operation releases its gate before caller cleanup.
        static_cast<void>(events.discard_failed());
        pump_until(events, waiting).get();
    }
    if constexpr (std::is_void_v<T>) {
        // Consume the native completion so exceptions still propagate. Its
        // get() returns a storage sentinel even for a void future.
        static_cast<void>(waiting.get());
        return;
    } else {
        return std::move(waiting).get();
    }
}

// Both callbacks execute on the caller's Seastar-thread stack, so cleanup may
// drive and await owners before they are destroyed. Retain both failures with
// native finally() ordering when cleanup also fails; cleanup-only errors
// propagate.
template<typename Func, typename Cleanup>
auto with_cleanup(Func function, Cleanup cleanup) {
    std::optional<std::invoke_result_t<Func&>> value;
    std::exception_ptr failure;
    try {
        value.emplace(function());
    } catch (...) {
        failure = std::current_exception();
    }
    try {
        cleanup();
    } catch (...) {
        runtime::testing::retain_cleanup_failure(failure);
    }
    if (failure) {
        std::rethrow_exception(failure);
    }
    return std::move(*value);
}

} // namespace fuzz_case_detail

[[nodiscard]] runtime::result<fuzz_reproduction>
execute_fuzz_case(fuzz_harness harness, std::span<const std::uint8_t> input);

[[nodiscard]] runtime::result<void>
replay_fuzz_case(const fuzz_reproduction& expected);

void run_fuzz_case(
  fuzz_harness harness, const std::uint8_t* data, std::size_t size);

[[noreturn]] void report_fuzz_failure(fuzz_reproduction value);

} // namespace kwaque::simulation::testing

#endif // KWAQUE_SRC_SIMULATION_TESTS_FUZZ_CASES_H_
