#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>

namespace kwaque::simulation::testing {

// A header and sixty-four operations. A history may hold four times as many;
// a fuzz input is kept to what a run finishes well inside the simulation's
// own budget, whatever the operations are: the costliest found, a segment
// and a crash for every other operation, runs a third of the events the
// scheduler may.
inline constexpr std::size_t storage_history_fuzz_max_input = 8 + 4 * 64;

// How one input's run ended.
enum class history_case_end : std::uint8_t {
    // Everything the ledger was told held.
    held,
    // The ledger's first verdict that was not `holds`.
    verdict,
    // The driver or the store threw: a start that did not leave the shard
    // ready, an operation that stopped without a result, a segment under
    // both of its names.
    threw,
    // Something ran out: the simulation's own budget of events, or a store
    // answer of that kind that a helper threw. Never a pass.
    exhausted,
};
struct history_case_result final {
    history_case_end end{history_case_end::held};
    // One word for it: `holds`, the verdict's own, `threw` or `exhausted`.
    std::string word{"holds"};
    std::string about;
    // The shard's lives.
    std::uint32_t lives{0};
};

// Runs one input as a history of one shard's storage over a store of its own
// on the simulated file system, with a ledger of what the driver did and was
// told. Every input is a history: what is short of a whole operation is not
// read. Called on a Seastar thread.
[[nodiscard]] history_case_result
exercise_history_case(std::span<const std::uint8_t> input);

// Whether a result ends the process. One that held never does. With
// `required`, only a result of that word does: a reduction keeps an input
// only if it ends the way the first one ended.
[[nodiscard]] bool history_case_fails(
  const history_case_result& result, std::string_view required) noexcept;

// The fuzz entry's body: runs the input and, on a result that fails, writes
// the line a wrapper reads and ends the process.
void fuzz_history_case(std::span<const std::uint8_t> input);

} // namespace kwaque::simulation::testing
