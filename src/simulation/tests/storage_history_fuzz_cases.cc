#include "src/simulation/tests/storage_history_fuzz_cases.h"

#include "src/base/error.h"
#include "src/runtime/testing/fuzz_verdict.h"
#include "src/simulation/tests/engine_history_fixture.h"
#include "src/storage/tests/engine_history.h"
#include "src/storage/tests/engine_ledger.h"

#include <algorithm>
#include <exception>
#include <string>
#include <system_error>

namespace kwaque::simulation::testing {

history_case_result exercise_history_case(std::span<const std::uint8_t> input) {
    using verdict = storage::testing::engine_ledger::verdict;
    const auto history = storage::testing::decode_history(
      input.first(std::min(input.size(), storage_history_fuzz_max_input)));
    history_case_result result;
    try {
        const auto report
          = engine_history_fixture::run(history, {}, {}, true).get();
        result.lives = report.lives;
        if (report.verdict != verdict::holds) {
            result.end = history_case_end::verdict;
            result.word = storage::testing::verdict_name(report.verdict);
            result.about = report.about;
        }
    } catch (const std::system_error& failure) {
        // The scheduler says so when it has no event left to give, and so
        // does a helper that throws a store's answer of that kind.
        const bool spent = failure.code()
                           == make_error_code(errc::resource_exhausted);
        result.end = spent ? history_case_end::exhausted
                           : history_case_end::threw;
        result.word = spent ? "exhausted" : "threw";
        result.about = failure.what();
    } catch (const std::exception& failure) {
        result.end = history_case_end::threw;
        result.word = "threw";
        result.about = failure.what();
    }
    return result;
}

bool history_case_fails(
  const history_case_result& result, std::string_view required) noexcept {
    return result.end != history_case_end::held
           && runtime::testing::fuzz_verdict_fails(result.word, required);
}

void fuzz_history_case(std::span<const std::uint8_t> input) {
    const auto result = exercise_history_case(input);
    if (result.end == history_case_end::held) return;
    runtime::testing::fail_fuzz_verdict(
      result.word,
      result.about + " (lives " + std::to_string(result.lives) + ")");
}

} // namespace kwaque::simulation::testing
