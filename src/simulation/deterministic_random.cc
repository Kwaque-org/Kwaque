#include "src/simulation/deterministic_random.h"

#include "src/base/invariant.h"

#include <limits>

namespace kwaque::simulation {

namespace {

constexpr invariant_id random_exhaustion_invariant{"KQ-RANDOM-EXHAUSTED"};

[[nodiscard]] runtime::operation_error random_error(errc code) noexcept {
    return runtime::operation_error{code, runtime::operation_kind::random};
}

[[nodiscard]] philox_counter draw_block(
  philox_key key,
  std::uint64_t occurrence,
  std::uint64_t block_index) noexcept {
    const auto occurrence_words = detail::split_u64(occurrence);
    const auto block_words = detail::split_u64(block_index);
    return philox4x32_10(
      philox_counter{
        occurrence_words[0],
        occurrence_words[1],
        block_words[0],
        block_words[1],
      },
      key);
}

[[nodiscard]] std::uint64_t
block_lane(const philox_counter& block, std::uint64_t draw_index) noexcept {
    const auto lane = static_cast<std::size_t>(draw_index & 1U) * 2U;
    return static_cast<std::uint64_t>(block[lane])
           | (static_cast<std::uint64_t>(block[lane + 1]) << 32U);
}

} // namespace

runtime::result<random_coordinate> random_coordinate::make(
  random_domain domain,
  std::uint64_t stable_id,
  std::uint64_t occurrence) noexcept {
    if (!detail::is_registered(domain)) {
        return runtime::failure(random_error(errc::invalid_argument));
    }
    return random_coordinate{domain, stable_id, occurrence};
}

sequential_random_source::sequential_random_source(
  random_domain domain,
  std::uint64_t stable_id,
  std::uint64_t occurrence,
  philox_key key) noexcept
  : domain_(domain)
  , stable_id_(stable_id)
  , occurrence_(occurrence)
  , key_(key) {}

std::uint64_t sequential_random_source::next_u64() noexcept {
    KWAQUE_INVARIANT(
      random_exhaustion_invariant,
      !exhausted_,
      "deterministic random draw index exhausted");
    const auto block_index = next_draw_ / 2U;
    if (!cache_valid_ || cached_block_index_ != block_index) {
        cached_block_ = draw_block(key_, occurrence_, block_index);
        cached_block_index_ = block_index;
        cache_valid_ = true;
    }
    const auto word = block_lane(cached_block_, next_draw_);
    if (next_draw_ == std::numeric_limits<std::uint64_t>::max()) {
        exhausted_ = true;
    } else {
        ++next_draw_;
    }
    return word;
}

void sequential_random_source::reset(std::uint64_t occurrence) noexcept {
    occurrence_ = occurrence;
    next_draw_ = 0;
    cached_block_index_ = 0;
    cache_valid_ = false;
    exhausted_ = false;
}

runtime::result<sequential_random_source> deterministic_random::stream(
  random_domain domain,
  std::uint64_t stable_id,
  std::uint64_t occurrence) const noexcept {
    auto coordinate = random_coordinate::make(domain, stable_id, occurrence);
    if (!coordinate) {
        return runtime::failure(coordinate.error());
    }
    return stream(*coordinate);
}

sequential_random_source
deterministic_random::stream(random_coordinate coordinate) const noexcept {
    return sequential_random_source{
      coordinate.domain(),
      coordinate.stable_id(),
      coordinate.occurrence(),
      derive_key(coordinate.domain(), coordinate.stable_id()),
    };
}

sequential_random_source deterministic_random::cursor(
  random_coordinate coordinate, std::uint64_t draw_index) const noexcept {
    auto source = stream(coordinate);
    source.next_draw_ = draw_index;
    return source;
}

std::uint64_t deterministic_random::word_at(
  random_coordinate coordinate, std::uint64_t draw_index) const noexcept {
    const auto block = draw_block(
      derive_key(coordinate.domain(), coordinate.stable_id()),
      coordinate.occurrence(),
      draw_index / 2U);
    return block_lane(block, draw_index);
}

runtime::result<std::uint64_t> deterministic_random::recorded_word_at(
  event_trace& trace,
  runtime::monotonic_time time,
  random_coordinate coordinate,
  std::uint64_t draw_index) const noexcept {
    const auto word = word_at(coordinate, draw_index);
    auto observed = trace.observe(
      trace_entry{
        .time = time,
        .action = trace_action::keyed_decision,
        .kind = trace_event_kind::keyed_random,
        .domain = static_cast<std::uint32_t>(coordinate.domain()),
        .stable_id = coordinate.stable_id(),
        .coordinate_a = coordinate.occurrence(),
        .coordinate_b = draw_index,
        .value = word,
      });
    if (!observed) {
        return runtime::failure(observed.error());
    }
    return word;
}

philox_key deterministic_random::derive_key(
  random_domain domain, std::uint64_t stable_id) const noexcept {
    const auto seed = detail::split_u64(master_seed_);
    const auto id = detail::split_u64(stable_id);
    const auto derived = philox4x32_10(
      philox_counter{
        deterministic_random_derivation_tag,
        static_cast<std::uint32_t>(domain),
        id[0],
        id[1],
      },
      philox_key{seed[0], seed[1]});
    return philox_key{derived[0], derived[1]};
}

} // namespace kwaque::simulation
