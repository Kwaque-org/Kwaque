#pragma once

#include "src/base/readiness.h"
#include "src/runtime/shard_affinity.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/sharded.hh>

#include <array>
#include <cstdint>
#include <functional>
#include <optional>
#include <span>
#include <vector>

namespace kwaque::admin {

// The broker-wide readiness reasons, kept by the owner on shard 0. Each shard
// states its whole set of held reasons with its own increasing sequence, and
// the owner keeps the last set of each shard and how many shards hold each
// reason. A statement no newer than its shard's last changes nothing, so
// duplicated and reordered statements cannot clear a reason a later one
// holds, and one shard or one reason cannot clear another's.
class readiness_owner final {
public:
    struct broadcast final {
        readiness_reasons reasons;
        std::uint64_t version;

        bool operator==(const broadcast&) const = default;
    };

    explicit readiness_owner(unsigned shard_count);

    // Applies one shard's statement; answers the new broker-wide set and its
    // version when the set changed.
    [[nodiscard]] std::optional<broadcast> apply(
      unsigned shard,
      readiness_reasons reasons,
      std::uint64_t sequence) noexcept;

    [[nodiscard]] readiness_reasons reasons() const noexcept {
        return reasons_;
    }
    [[nodiscard]] std::uint64_t version() const noexcept { return version_; }

private:
    static constexpr std::size_t reason_bits = 8;

    std::vector<readiness_reasons> held_;
    std::vector<std::uint64_t> sequences_;
    std::array<std::uint32_t, reason_bits> holders_{};
    readiness_reasons reasons_{0};
    std::uint64_t version_{0};
};

// One shard's view of the admin lifecycle and of the broker's readiness.
// The lifecycle runs stopped, live, started (the start finished), draining;
// the broker is ready while started with no reason held anywhere.
class admin_state final
  : public runtime::shard_affine
  , public seastar::peering_sharded_service<admin_state> {
public:
    // Called on shard 0 at each change of readiness or of the held reasons.
    using readiness_observer
      = std::function<void(bool ready, readiness_reasons reasons)>;

    admin_state();

    void register_metrics();
    void listener_started(unsigned shard_count);
    void mark_started(double startup_duration_seconds);
    void begin_shutdown();
    [[nodiscard]] seastar::future<> stop();

    // This shard holds or releases `reason`. The shard's new set goes to the
    // owner, which sends the broker-wide set back to every shard. Each side
    // keeps one message in flight and sends its latest set when that one is
    // answered, so a reason that flaps costs no more messages. Allocates the
    // messages; running out of memory there ends the process.
    void report(readiness_reason reason, bool held) noexcept;

    // A shard's latest statement of the reasons it holds.
    struct statement final {
        unsigned shard;
        readiness_reasons reasons;
        std::uint64_t sequence;
    };
    [[nodiscard]] statement latest_statement() const;
    // On shard 0, before any shard starts: applies every shard's latest
    // statement, whether or not its own message has arrived, and answers the
    // broker-wide set the start begins with.
    [[nodiscard]] readiness_owner::broadcast
    settle(std::span<const statement> statements);
    // On shard 0.
    void observe_readiness(readiness_observer observer);

    // Liveness means the process should keep running. A draining broker is
    // still live; only readiness reports that it is leaving service.
    [[nodiscard]] bool live() const;
    [[nodiscard]] bool started() const;
    [[nodiscard]] bool ready() const;
    [[nodiscard]] bool draining() const;
    // The broker-wide reasons this shard last heard.
    [[nodiscard]] readiness_reasons reasons() const;
    [[nodiscard]] std::uint64_t reasons_version() const;
    [[nodiscard]] unsigned shard_count() const;
    [[nodiscard]] double startup_duration_seconds() const;

    // The owner's side and every shard's side of the messages; public so
    // the messages and the tests can reach them.
    void apply_statement(
      unsigned shard, readiness_reasons reasons, std::uint64_t sequence);
    void apply_reasons(readiness_reasons reasons, std::uint64_t version);

private:
    enum class lifecycle : std::uint8_t { stopped, live, started, draining };

    void notify_if_changed();
    void send_statement() noexcept;
    void send_broadcast() noexcept;

    lifecycle lifecycle_{lifecycle::stopped};
    unsigned shard_count_{0};
    double startup_duration_seconds_{0.0};
    readiness_reasons held_{0};
    std::uint64_t statement_{0};
    bool statement_in_flight_{false};
    bool broadcast_in_flight_{false};
    readiness_reasons reasons_{0};
    std::uint64_t reasons_version_{0};
    std::optional<readiness_owner> owner_;
    readiness_observer observer_;
    std::optional<std::pair<bool, readiness_reasons>> observed_;
    seastar::gate messages_;
    std::optional<seastar::metrics::metric_groups> metrics_;
};

// Wall-clock time at which this process initialized its static storage.
[[nodiscard]] double process_start_time_seconds() noexcept;

} // namespace kwaque::admin
