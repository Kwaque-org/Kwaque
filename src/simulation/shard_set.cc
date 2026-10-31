#include "src/simulation/shard_set.h"

#include <algorithm>

namespace kwaque::simulation {

namespace {

constexpr std::uint64_t slow_one_in{1'000};

} // namespace

shard_set::shard_set(
  scheduler& events,
  deterministic_random random,
  std::uint64_t stable_id,
  std::uint16_t shards,
  shard_set_config config)
  : events_(&events)
  , random_(random)
  , stable_id_(stable_id)
  , shards_(shards)
  , config_(config) {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-SIM-SHARD-COUNT"},
      shards_ >= 1 && shards_ <= shard_set_config::maximum_shards,
      "a simulation hosts one to eight virtual shards");
}

shard_set::~shard_set() {
    assert_current();
    for (auto& [key, pair] : pairs_) {
        const auto source = std::get<0>(key);
        // Each pair's queue belongs to its sender.
        const virtual_shard_turn turn{seastar::this_shard_id() + source};
        KWAQUE_INVARIANT(
          invariant_id{"KQ-SIM-SHARD-PENDING"},
          pair->units.waiters() == 0
            && pair->units.available_units() == config_.units_per_pair,
          "a cross-shard call was outstanding when the shard set ended");
        pair.reset();
    }
}

shard_set::pair_state&
shard_set::pair_of(std::uint16_t source, std::uint16_t target, unsigned group) {
    auto& slot = pairs_[std::tuple{source, target, group}];
    if (!slot) {
        // Made as the sender, whose queue it is.
        slot = std::make_unique<pair_state>(config_.units_per_pair);
    }
    return *slot;
}

runtime::monotonic_time shard_set::delivery_time(
  pair_state& pair,
  std::uint16_t source,
  std::uint16_t target,
  unsigned group) {
    auto coordinate = random_coordinate::make(
      random_domain::shard_delivery,
      stable_id_ * shard_set_config::maximum_shards
          * shard_set_config::maximum_shards
        + source * shard_set_config::maximum_shards + target,
      0);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-SIM-SHARD-DELAY"},
      coordinate.has_value(),
      "the delivery random domain is not registered");
    // Each group of a pair draws its own words.
    KWAQUE_INVARIANT(
      invariant_id{"KQ-SIM-SHARD-DELAY"},
      pair.sequence < (std::uint64_t{1} << 32U),
      "a pair's group made 2^32 calls");
    const auto word = random_.word_at(
      *coordinate, std::uint64_t{group} << 32U | pair.sequence++);
    const auto unit = static_cast<double>(word >> 11U) * 0x1.0p-53;
    const bool slow = word % slow_one_in == slow_one_in - 1U;
    const auto ceiling = slow ? config_.slow_nanoseconds
                              : config_.fast_nanoseconds;
    const auto delay
      = config_.minimum_nanoseconds
        + static_cast<std::uint64_t>(
          unit * static_cast<double>(ceiling - config_.minimum_nanoseconds));
    auto at = runtime::monotonic_time{events_->now().nanoseconds() + delay};
    if (!config_.reorder) {
        // Seastar's ring delivers one pair's admitted calls in order.
        at = std::max(at, pair.last_delivery);
    }
    pair.last_delivery = at;
    return at;
}

void shard_set::schedule_at(
  std::uint16_t shard,
  runtime::monotonic_time at,
  scheduler::callback&& callback) noexcept {
    const auto scheduled = events_->schedule_on(
      shard,
      std::max(at, events_->now()),
      event_priority::normal(),
      std::move(callback));
    KWAQUE_INVARIANT(
      invariant_id{"KQ-SIM-SHARD-SCHEDULE"},
      scheduled.has_value(),
      "the scheduler refused a cross-shard delivery");
}

} // namespace kwaque::simulation
