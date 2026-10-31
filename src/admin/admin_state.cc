#include "src/admin/admin_state.h"

#include "src/base/build_info.h"
#include "src/base/invariant.h"
#include "src/base/metric_schema.h"

#include <seastar/core/smp.hh>

#include <chrono>
#include <exception>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace kwaque::admin {

namespace {

const std::chrono::system_clock::time_point process_started
  = std::chrono::system_clock::now();

// Both message paths check the gate before entering it, so the only failure
// left is an allocation, which the header promises ends the process.
[[noreturn]] void message_failed(const std::exception_ptr&) noexcept {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-ADMIN-READINESS-MESSAGE"},
      false,
      "a readiness message could not be sent");
    std::terminate();
}

const metric_descriptor& metric(metric_id id) {
    const auto* descriptor = descriptor_for(id);
    if (descriptor == nullptr) {
        throw std::logic_error("admin metric descriptor is missing");
    }
    return *descriptor;
}

} // namespace

readiness_owner::readiness_owner(unsigned shard_count)
  : held_(shard_count, 0)
  , sequences_(shard_count, 0) {}

std::optional<readiness_owner::broadcast> readiness_owner::apply(
  unsigned shard, readiness_reasons reasons, std::uint64_t sequence) noexcept {
    if (shard >= held_.size() || sequence <= sequences_[shard]) {
        return std::nullopt;
    }
    sequences_[shard] = sequence;
    const auto before = held_[shard];
    held_[shard] = reasons;
    readiness_reasons combined = 0;
    for (std::size_t bit = 0; bit < reason_bits; ++bit) {
        const auto mask = static_cast<readiness_reasons>(1U << bit);
        if ((before & mask) != 0 && (reasons & mask) == 0) {
            --holders_[bit];
        } else if ((before & mask) == 0 && (reasons & mask) != 0) {
            ++holders_[bit];
        }
        if (holders_[bit] != 0) {
            combined |= mask;
        }
    }
    if (combined == reasons_) {
        return std::nullopt;
    }
    reasons_ = combined;
    ++version_;
    return broadcast{.reasons = reasons_, .version = version_};
}

admin_state::admin_state() {
    // The owner keeps every shard's statement from construction, so a reason
    // held while the broker starts keeps it unready after the start.
    if (owner().value() == 0) {
        owner_.emplace(seastar::this_smp_shard_count());
    }
}

double process_start_time_seconds() noexcept {
    return std::chrono::duration<double>(process_started.time_since_epoch())
      .count();
}

void admin_state::register_metrics() {
    assert_current();
    if (metrics_) {
        throw std::logic_error("admin metrics are already registered");
    }
    namespace metrics = seastar::metrics;
    try {
        metrics_.emplace();
        if (owner().value() != 0) {
            return;
        }
        // Process-wide values have one owner. Aggregating the shard label
        // exposes them without a misleading shard identity.
        const std::vector<metrics::label> aggregate{metrics::shard_label};
        std::vector<metrics::metric_definition> definitions;
        definitions.reserve(5);
        const auto gauge = [&definitions, &aggregate](metric_id id, auto read) {
            const auto& descriptor = metric(id);
            definitions.emplace_back(
              metrics::make_gauge(
                seastar::sstring{descriptor.name},
                std::move(read),
                metrics::description(seastar::sstring{descriptor.help}))
                .aggregate(aggregate));
        };
        gauge(metric_id::broker_process_readiness, [this] {
            return ready() ? 1U : 0U;
        });
        gauge(
          metric_id::broker_draining, [this] { return draining() ? 1U : 0U; });
        gauge(metric_id::broker_shards, [this] { return shard_count(); });
        gauge(metric_id::broker_startup_duration_seconds, [this] {
            return startup_duration_seconds();
        });
        gauge(metric_id::broker_start_time_seconds, [] {
            return process_start_time_seconds();
        });
        metrics_->add_group(
          seastar::sstring{metric(metric_id::broker_process_readiness).group},
          definitions);

        // The conventional information metric: one series whose labels name
        // the running build and whose value is always 1.
        const metrics::label version{"version"};
        const metrics::label revision{"revision"};
        const metrics::label build_mode{"build_mode"};
        const metrics::label dirty{"dirty"};
        metrics_->add_group(
          "build",
          {metrics::make_gauge(
             "info",
             [] { return 1U; },
             metrics::description(
               "Build identity of the running broker; always 1"),
             {version(std::string{build_info::version()}),
              revision(std::string{build_info::git_revision()}),
              build_mode(std::string{build_info::build_mode()}),
              dirty(build_info::git_dirty() ? "true" : "false")})
             .aggregate(aggregate)});
    } catch (...) {
        metrics_.reset();
        throw;
    }
}

void admin_state::listener_started(unsigned shard_count) {
    assert_current();
    shard_count_ = shard_count;
    lifecycle_ = lifecycle::live;
}

void admin_state::mark_started(double startup_duration_seconds) {
    assert_current();
    startup_duration_seconds_ = startup_duration_seconds;
    if (lifecycle_ == lifecycle::live) {
        lifecycle_ = lifecycle::started;
    }
    notify_if_changed();
}

void admin_state::begin_shutdown() {
    assert_current();
    // Draining is final: no restored reason makes the broker ready again.
    if (lifecycle_ == lifecycle::live || lifecycle_ == lifecycle::started) {
        lifecycle_ = lifecycle::draining;
    }
    notify_if_changed();
}

seastar::future<> admin_state::stop() {
    assert_current();
    lifecycle_ = lifecycle::stopped;
    metrics_.reset();
    observer_ = nullptr;
    observed_.reset();
    if (messages_.is_closed()) {
        return seastar::make_ready_future<>();
    }
    // Statements and broadcasts in flight finish before the state goes.
    return messages_.close();
}

void admin_state::report(readiness_reason reason, bool held) noexcept {
    assert_current();
    const auto bit = readiness_bit(reason);
    const auto next = static_cast<readiness_reasons>(
      held ? held_ | bit : held_ & ~bit);
    if (next == held_) {
        return;
    }
    held_ = next;
    ++statement_;
    if (!statement_in_flight_) {
        send_statement();
    }
}

void admin_state::send_statement() noexcept {
    if (messages_.is_closed()) {
        return;
    }
    statement_in_flight_ = true;
    const auto shard = static_cast<unsigned>(owner().value());
    const auto reasons = held_;
    const auto sequence = statement_;
    static_cast<void>(
      seastar::with_gate(messages_, [this, shard, reasons, sequence] {
          return container()
            .invoke_on(
              0,
              [shard, reasons, sequence](admin_state& owner) {
                  owner.apply_statement(shard, reasons, sequence);
              })
            .then([this, sequence] {
                statement_in_flight_ = false;
                if (statement_ != sequence) {
                    send_statement();
                }
            });
      }).handle_exception(&message_failed));
}

admin_state::statement admin_state::latest_statement() const {
    assert_current();
    return statement{
      .shard = static_cast<unsigned>(owner().value()),
      .reasons = held_,
      .sequence = statement_};
}

readiness_owner::broadcast
admin_state::settle(std::span<const statement> statements) {
    assert_current();
    if (!owner_) {
        throw std::logic_error("readiness is settled on shard 0");
    }
    for (const auto& latest : statements) {
        static_cast<void>(
          owner_->apply(latest.shard, latest.reasons, latest.sequence));
    }
    return {.reasons = owner_->reasons(), .version = owner_->version()};
}

void admin_state::observe_readiness(readiness_observer observer) {
    assert_current();
    observer_ = std::move(observer);
    observed_ = std::pair{ready(), reasons_};
}

void admin_state::apply_statement(
  unsigned shard, readiness_reasons reasons, std::uint64_t sequence) {
    assert_current();
    if (
      !owner_ || !owner_->apply(shard, reasons, sequence)
      || broadcast_in_flight_) {
        return;
    }
    send_broadcast();
}

void admin_state::send_broadcast() noexcept {
    if (messages_.is_closed()) {
        return;
    }
    broadcast_in_flight_ = true;
    const readiness_owner::broadcast update{
      .reasons = owner_->reasons(), .version = owner_->version()};
    static_cast<void>(seastar::with_gate(messages_, [this, update] {
                          return container()
                            .invoke_on_all([update](admin_state& state) {
                                state.apply_reasons(
                                  update.reasons, update.version);
                            })
                            .then([this, version = update.version] {
                                broadcast_in_flight_ = false;
                                if (owner_->version() != version) {
                                    send_broadcast();
                                }
                            });
                      }).handle_exception(&message_failed));
}

void admin_state::apply_reasons(
  readiness_reasons reasons, std::uint64_t version) {
    assert_current();
    // A broadcast that arrives after a newer one changes nothing.
    if (version <= reasons_version_) {
        return;
    }
    reasons_ = reasons;
    reasons_version_ = version;
    notify_if_changed();
}

void admin_state::notify_if_changed() {
    if (!observer_) {
        return;
    }
    const std::pair current{ready(), reasons_};
    if (observed_ == current) {
        return;
    }
    observed_ = current;
    observer_(current.first, current.second);
}

bool admin_state::live() const {
    assert_current();
    return lifecycle_ == lifecycle::live || lifecycle_ == lifecycle::started
           || lifecycle_ == lifecycle::draining;
}

bool admin_state::started() const {
    assert_current();
    return lifecycle_ == lifecycle::started;
}

bool admin_state::ready() const {
    assert_current();
    return lifecycle_ == lifecycle::started && reasons_ == 0;
}

readiness_reasons admin_state::reasons() const {
    assert_current();
    return reasons_;
}

std::uint64_t admin_state::reasons_version() const {
    assert_current();
    return reasons_version_;
}

bool admin_state::draining() const {
    assert_current();
    return lifecycle_ == lifecycle::draining;
}

unsigned admin_state::shard_count() const {
    assert_current();
    return shard_count_;
}

double admin_state::startup_duration_seconds() const {
    assert_current();
    return startup_duration_seconds_;
}

} // namespace kwaque::admin
