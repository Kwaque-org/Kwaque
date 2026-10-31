#ifndef KWAQUE_SRC_SIMULATION_BUGGIFY_H_
#define KWAQUE_SRC_SIMULATION_BUGGIFY_H_

#include "src/runtime/buggify.h"
#include "src/runtime/error.h"
#include "src/runtime/shard_affinity.h"
#include "src/simulation/deterministic_random.h"
#include "src/simulation/event_trace.h"

#include <array>
#include <bitset>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>

namespace kwaque::simulation {

class scheduler;

enum class buggify_firing : std::uint8_t {
    // Every active site fires by its ratio.
    every_site,
    // Only neutral sites fire, so runs whose answers are compared agree.
    neutral_sites,
    // No site fires, as in a healed phase. Activation and the first
    // evaluations in the trace are unchanged.
    none,
};

struct buggify_config final {
    bool enabled{false};
    buggify_firing firing{buggify_firing::every_site};
};

// BUGGIFY for one simulated run. A site is active for the whole run with
// probability 1/4, and an active site fires at an evaluation with its own
// ratio. Both are keyed draws: activation is a function of the run seed and
// the site, a firing of the run seed, the site and the caller's key. No draw
// depends on what ran before, so every reduction of a history, every task
// order and every count of virtual shards has the same active sites, and a
// firing at a key is the same in every run that reaches it.
//
// Each site's first evaluation is traced, active or not, into an entry
// reserved when the evaluator is made, so evaluating never fails or
// allocates. Disabled, an evaluation answers false and draws, traces and
// counts nothing.
class buggify_evaluator final : public runtime::reactor_affine {
public:
    [[nodiscard]] static runtime::result<std::unique_ptr<buggify_evaluator>>
    make(
      buggify_config config,
      deterministic_random random,
      event_trace& trace,
      const scheduler& events,
      std::span<const runtime::buggify_site_descriptor> sites
      = runtime::buggify_sites);

    buggify_evaluator(const buggify_evaluator&) = delete;
    buggify_evaluator& operator=(const buggify_evaluator&) = delete;
    buggify_evaluator(buggify_evaluator&&) = delete;
    buggify_evaluator& operator=(buggify_evaluator&&) = delete;
    ~buggify_evaluator() = default;

    [[nodiscard]] bool evaluate(
      const runtime::buggify_site_descriptor& site,
      runtime::buggify_key key) noexcept;

    // Which sites may fire from now on; a healed phase sets none.
    void set_firing(buggify_firing firing) noexcept;

    [[nodiscard]] const buggify_config& config() const noexcept {
        return config_;
    }
    [[nodiscard]] std::span<const runtime::buggify_site_descriptor>
    sites() const noexcept {
        return sites_;
    }
    [[nodiscard]] bool active(std::size_t index) const noexcept;
    [[nodiscard]] bool evaluated(std::size_t index) const noexcept;
    [[nodiscard]] std::uint64_t firings(std::size_t index) const noexcept;

    // The word a site's activation is drawn from, and whether a site fires at
    // a key. Each is a pure function of the run seed and its arguments.
    [[nodiscard]] static std::uint64_t activation_word(
      const deterministic_random& random, std::uint16_t site) noexcept;
    [[nodiscard]] static bool fires(
      const deterministic_random& random,
      const runtime::buggify_site_descriptor& site,
      runtime::buggify_key key) noexcept;

private:
    buggify_evaluator(
      buggify_config config,
      deterministic_random random,
      const scheduler& events,
      std::span<const runtime::buggify_site_descriptor> sites,
      event_trace::reservation first_evaluations) noexcept;

    void trace_first_evaluation(
      const runtime::buggify_site_descriptor& site, bool active) noexcept;

    std::span<const runtime::buggify_site_descriptor> sites_;
    deterministic_random random_;
    const scheduler* events_;
    event_trace::reservation first_evaluations_;
    std::bitset<runtime::maximum_buggify_sites> active_;
    std::bitset<runtime::maximum_buggify_sites> evaluated_;
    std::array<std::uint64_t, runtime::maximum_buggify_sites> firings_{};
    buggify_config config_;
};

static_assert(runtime::buggify_evaluator_contract<buggify_evaluator>);

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_BUGGIFY_H_
