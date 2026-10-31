#ifndef KWAQUE_SRC_SIMULATION_SHARD_SET_H_
#define KWAQUE_SRC_SIMULATION_SHARD_SET_H_

#include "src/base/invariant.h"
#include "src/runtime/cross_shard.h"
#include "src/runtime/shard_affinity.h"
#include "src/runtime/time.h"
#include "src/runtime/waiting.h"
#include "src/simulation/deterministic_random.h"
#include "src/simulation/scheduler.h"
#include "src/simulation/virtual_shard.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>

#include <array>
#include <cstdint>
#include <exception>
#include <map>
#include <memory>
#include <optional>
#include <tuple>
#include <type_traits>
#include <utility>

namespace kwaque::simulation {

struct shard_set_config final {
    static constexpr std::uint16_t maximum_shards{8};

    // Seastar gives each shard pair max(max_nonlocal_requests, S - 1) /
    // (S - 1) units of a service group, taken on the sender and returned when
    // the reply is processed there (smp.cc 82-100, reactor.cc 3914-3934,
    // 4009-4015). The simulation gives each pair of each group this many.
    std::uint32_t units_per_pair{1'024};
    // Calls admitted on one pair may be delivered out of order, as the
    // sanitizer build's task shuffle can make them; otherwise they are
    // delivered in admission order, as Seastar's per-pair ring does.
    bool reorder{false};
    // Delays above minimum_nanoseconds: 999 in 1,000 uniform up to
    // fast_nanoseconds, the rest uniform up to slow_nanoseconds, so a rare
    // slow call exposes order a fast one hides.
    std::uint64_t minimum_nanoseconds{1'000};
    std::uint64_t fast_nanoseconds{5'000};
    std::uint64_t slow_nanoseconds{1'000'000};
};

// Cross-shard calls between the virtual shards of one simulated broker, with
// Seastar's semantics rather than a stronger order (smp.hh 211-279,
// reactor.cc 3893-3954): a call to the current shard runs inline; a remote
// call first takes one of its pair's units on the sender, in FIFO order, and
// a woken waiter is admitted in a later task, so a later call that finds a
// unit free overtakes it (semaphore.hh 406-417); it is delivered to the
// target after a keyed delay, its reply comes back to the sender as an event
// of the sender's, and the unit is returned then. No correctness may depend
// on the order of two separate calls on one pair.
class shard_set final : public runtime::reactor_affine {
public:
    shard_set(
      scheduler& events,
      deterministic_random random,
      std::uint64_t stable_id,
      std::uint16_t shards,
      shard_set_config config);

    shard_set(const shard_set&) = delete;
    shard_set& operator=(const shard_set&) = delete;
    shard_set(shard_set&&) = delete;
    shard_set& operator=(shard_set&&) = delete;
    ~shard_set();

    [[nodiscard]] seastar::shard_id current() const noexcept {
        return runtime::current_shard() - seastar::this_shard_id();
    }
    [[nodiscard]] unsigned count() const noexcept { return shards_; }

    // Runs `func(args...)` as virtual shard `target` and answers on the
    // calling shard.
    template<typename Func, typename... Args>
    auto invoke_on(
      seastar::shard_id target,
      seastar::smp_service_group group,
      Func func,
      Args... args) {
        assert_current();
        KWAQUE_INVARIANT(
          invariant_id{"KQ-SIM-SHARD-TARGET"},
          target < shards_,
          "a cross-shard call named a shard the simulation does not have");
        auto bound = runtime::detail::bind_arguments(
          std::move(func), std::move(args)...);
        if (target == current()) {
            return runtime::detail::invoke_inline(std::move(bound));
        }
        return invoke_remote(
          static_cast<std::uint16_t>(target),
          seastar::internal::smp_service_group_id(group),
          std::move(bound));
    }

    // The environment's own start and stop calls: no delay and no unit, so
    // they run once virtual time has stopped.
    template<typename Func>
    auto control(seastar::shard_id target, Func func)
      -> seastar::futurize_t<std::invoke_result_t<Func>> {
        assert_current();
        if (target == current()) {
            return runtime::detail::invoke_inline(std::move(func));
        }
        using futurator = seastar::futurize<std::invoke_result_t<Func>>;
        return send<futurator>(
          static_cast<std::uint16_t>(current()),
          static_cast<std::uint16_t>(target),
          nullptr,
          0,
          events_->now(),
          std::move(func));
    }

private:
    struct pair_state final {
        explicit pair_state(std::uint32_t units) noexcept
          : units(units) {}

        runtime::unit_queue units;
        runtime::monotonic_time last_delivery{};
        std::uint64_t sequence{0};
    };

    template<typename Futurator, typename Func>
    struct call_state final {
        Func func;
        typename Futurator::promise_type reply;
        std::optional<typename Futurator::type> answer;
    };

    pair_state&
    pair_of(std::uint16_t source, std::uint16_t target, unsigned group);
    runtime::monotonic_time delivery_time(
      pair_state& pair,
      std::uint16_t source,
      std::uint16_t target,
      unsigned group);
    void schedule_at(
      std::uint16_t shard,
      runtime::monotonic_time at,
      scheduler::callback&& callback) noexcept;

    template<typename Func>
    auto invoke_remote(std::uint16_t target, unsigned group, Func func)
      -> seastar::futurize_t<std::invoke_result_t<Func>> {
        using futurator = seastar::futurize<std::invoke_result_t<Func>>;
        const auto source = static_cast<std::uint16_t>(current());
        // Checks only the call's synchronous start: a target that suspends
        // and then calls in its own group is not seen.
        KWAQUE_INVARIANT(
          invariant_id{"KQ-SIM-SHARD-DAG"},
          running_group_[source] != group + 1U,
          "a cross-shard call waits on its own service group (Seastar's DAG "
          "rule, smp.hh 100-105)");
        auto& pair = pair_of(source, target, group);
        const auto admitted = co_await pair.units.wait(1);
        KWAQUE_INVARIANT(
          invariant_id{"KQ-SIM-SHARD-UNITS"},
          admitted.has_value(),
          "a cross-shard unit wait failed");
        co_return co_await send<futurator>(
          source,
          target,
          &pair,
          group,
          delivery_time(pair, source, target, group),
          std::move(func));
    }

    // Delivers `func` to `target` at `at` and awaits its answer. A call
    // with a pair holds one of its units until the answer is processed.
    template<typename Futurator, typename Func>
    auto send(
      std::uint16_t source,
      std::uint16_t target,
      pair_state* pair,
      unsigned group,
      runtime::monotonic_time at,
      Func func) -> typename Futurator::type {
        typename Futurator::promise_type reply;
        auto replied = reply.get_future();
        call_state<Futurator, Func> call{std::move(func), std::move(reply)};
        schedule_at(target, at, [this, &call, pair, source, group] noexcept {
            start_call(call, source, pair, group);
        });
        co_return co_await std::move(replied);
    }

    // Runs as the target: starts the call, and when its future resolves
    // sends the answer back as an event of the sender's. Each answer takes
    // the minimum delay, so answers return in completion order.
    template<typename Futurator, typename Func>
    void start_call(
      call_state<Futurator, Func>& call,
      std::uint16_t source,
      pair_state* pair,
      unsigned group) noexcept {
        const auto target = static_cast<std::uint16_t>(current());
        const auto previous = running_group_[target];
        running_group_[target] = pair != nullptr ? group + 1U : 0U;
        auto started = Futurator::invoke(std::move(call.func));
        running_group_[target] = previous;
        static_cast<void>(std::move(started).then_wrapped(
          [this, &call, source, pair](typename Futurator::type done) noexcept {
              call.answer.emplace(std::move(done));
              schedule_at(
                source,
                runtime::monotonic_time{
                  events_->now().nanoseconds() + config_.minimum_nanoseconds},
                [&call, pair]() noexcept {
                    // Seastar completes the call, then returns its unit
                    // (reactor.cc 4009-4015).
                    call.answer->forward_to(std::move(call.reply));
                    if (pair != nullptr) {
                        pair->units.signal(1);
                    }
                });
          }));
    }

    scheduler* events_;
    deterministic_random random_;
    std::uint64_t stable_id_;
    std::uint16_t shards_;
    shard_set_config config_;
    std::map<
      std::tuple<std::uint16_t, std::uint16_t, unsigned>,
      std::unique_ptr<pair_state>>
      pairs_;
    std::array<unsigned, shard_set_config::maximum_shards> running_group_{};
};

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_SHARD_SET_H_
