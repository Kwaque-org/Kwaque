#include "src/simulation/buggify.h"

#include "src/base/invariant.h"
#include "src/runtime/random.h"
#include "src/simulation/scheduler.h"

#include <utility>

namespace kwaque::simulation {

namespace {

constexpr invariant_id buggify_site_invariant{"KQ-SIM-BUGGIFY-SITE"};
constexpr invariant_id buggify_trace_invariant{"KQ-SIM-BUGGIFY-TRACE"};

[[nodiscard]] runtime::operation_error buggify_error(errc code) noexcept {
    return runtime::operation_error{code, runtime::operation_kind::fault};
}

[[nodiscard]] random_coordinate coordinate(
  random_domain domain, std::uint64_t stable_id, std::uint64_t at) noexcept {
    auto made = random_coordinate::make(domain, stable_id, at);
    KWAQUE_INVARIANT(
      buggify_site_invariant,
      made.has_value(),
      "a BUGGIFY domain is not registered");
    return *made;
}

} // namespace

runtime::result<std::unique_ptr<buggify_evaluator>> buggify_evaluator::make(
  buggify_config config,
  deterministic_random random,
  event_trace& trace,
  const scheduler& events,
  std::span<const runtime::buggify_site_descriptor> sites) {
    if (
      config.firing > buggify_firing::none
      || !runtime::valid_buggify_sites(sites)) {
        return runtime::failure(buggify_error(errc::invalid_argument));
    }
    event_trace::reservation first_evaluations;
    if (config.enabled && !sites.empty()) {
        const auto entries = static_cast<std::uint32_t>(sites.size());
        auto reserved = trace.reserve(
          entries, std::uint64_t{entries} * canonical_entry_encoded_size);
        if (!reserved) {
            return runtime::failure(reserved.error());
        }
        first_evaluations = std::move(*reserved);
    }
    return std::unique_ptr<buggify_evaluator>{new buggify_evaluator{
      config, random, events, sites, std::move(first_evaluations)}};
}

buggify_evaluator::buggify_evaluator(
  buggify_config config,
  deterministic_random random,
  const scheduler& events,
  std::span<const runtime::buggify_site_descriptor> sites,
  event_trace::reservation first_evaluations) noexcept
  : sites_(sites)
  , random_(random)
  , events_(&events)
  , first_evaluations_(std::move(first_evaluations))
  , config_(config) {
    if (!config_.enabled) {
        return;
    }
    for (std::size_t index = 0; index < sites_.size(); ++index) {
        if (
          buggify_activation_word_is_active(
            activation_word(random_, sites_[index].id))) {
            active_[index] = true;
        }
    }
}

std::uint64_t buggify_evaluator::activation_word(
  const deterministic_random& random, std::uint16_t site) noexcept {
    return random.word_at(
      coordinate(random_domain::buggify_activation, site, 0), 0);
}

bool buggify_evaluator::fires(
  const deterministic_random& random,
  const runtime::buggify_site_descriptor& site,
  runtime::buggify_key key) noexcept {
    // Each site has its own 2^32 draws at the key, so a rejected draw for one
    // ratio never reaches the word another site uses.
    auto cursor = random.cursor(
      coordinate(random_domain::buggify_decision, key.object, key.sequence),
      std::uint64_t{site.id} << 32U);
    const auto ratio = runtime::probability_ratio::make(
      site.fire_numerator, site.fire_denominator);
    KWAQUE_INVARIANT(
      buggify_site_invariant,
      ratio.has_value(),
      "a BUGGIFY site has an invalid fire ratio");
    return runtime::chance(cursor, *ratio);
}

bool buggify_evaluator::evaluate(
  const runtime::buggify_site_descriptor& site,
  runtime::buggify_key key) noexcept {
    assert_current();
    if (!config_.enabled) {
        return false;
    }
    const std::size_t index = site.id - 1U;
    KWAQUE_INVARIANT(
      buggify_site_invariant,
      site.id != 0 && index < sites_.size() && sites_[index] == site,
      "a BUGGIFY site is not in the evaluator's table");
    const bool active = active_[index];
    if (!evaluated_[index]) {
        evaluated_[index] = true;
        trace_first_evaluation(site, active);
    }
    if (!active) {
        return false;
    }
    switch (config_.firing) {
    case buggify_firing::every_site:
        break;
    case buggify_firing::neutral_sites:
        if (site.kind != runtime::buggify_class::neutral) {
            return false;
        }
        break;
    case buggify_firing::none:
        return false;
    }
    if (!fires(random_, site, key)) {
        return false;
    }
    ++firings_[index];
    return true;
}

void buggify_evaluator::trace_first_evaluation(
  const runtime::buggify_site_descriptor& site, bool active) noexcept {
    auto entry = first_evaluations_.split(1);
    KWAQUE_INVARIANT(
      buggify_trace_invariant,
      entry.has_value(),
      "a BUGGIFY site's first evaluation has no reserved trace entry");
    // A replay that diverges here is kept by the trace and reported when the
    // replay finishes; the site still answers from its activation.
    static_cast<void>(entry->observe(
      trace_entry{
        .time = events_->now(),
        .action = trace_action::buggify_evaluated,
        .kind = trace_event_kind::keyed_random,
        .domain = buggify_activation_trace_domain,
        .stable_id = site.id,
        .value = activation_word(random_, site.id),
        .result = active ? 1U : 0U,
      }));
}

void buggify_evaluator::set_firing(buggify_firing firing) noexcept {
    assert_current();
    KWAQUE_INVARIANT(
      buggify_site_invariant,
      firing <= buggify_firing::none,
      "an unknown BUGGIFY firing policy");
    config_.firing = firing;
}

bool buggify_evaluator::active(std::size_t index) const noexcept {
    return index < sites_.size() && active_[index];
}

bool buggify_evaluator::evaluated(std::size_t index) const noexcept {
    return index < sites_.size() && evaluated_[index];
}

std::uint64_t buggify_evaluator::firings(std::size_t index) const noexcept {
    return index < sites_.size() ? firings_[index] : 0;
}

} // namespace kwaque::simulation
