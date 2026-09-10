#pragma once

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/noncopyable_function.hh>

#include <array>
#include <chrono>
#include <cstddef>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace kwaque::broker {

class shutdown_stage_name final {
public:
    static constexpr std::size_t max_size = 64;

    explicit shutdown_stage_name(std::string_view name);

    [[nodiscard]] std::string_view value() const noexcept {
        return {value_.data(), size_};
    }

private:
    std::array<char, max_size> value_{};
    std::size_t size_{};
};

enum class shutdown_watchdog_event {
    begin,
    information,
    error,
    complete,
    failed,
};

void report_shutdown_event(
  shutdown_watchdog_event event, std::string_view name) noexcept;

// These timers report a stalled shutdown; they never cancel or terminate it.
// The guard and its callbacks stay on one shard and cannot move while armed.
template<typename Clock = seastar::lowres_clock>
class shutdown_watchdog final {
public:
    static constexpr auto information_after = std::chrono::seconds{15};
    static constexpr auto error_after = std::chrono::seconds{120};
    using observer = seastar::noncopyable_function<void(
      shutdown_watchdog_event, std::string_view) noexcept>;

    explicit shutdown_watchdog(
      shutdown_stage_name name, observer report = report_shutdown_event)
      : name_(std::move(name))
      , report_(std::move(report))
      , information_timer_([this] noexcept {
          report_(shutdown_watchdog_event::information, name_.value());
      })
      , error_timer_([this] noexcept {
          report_(shutdown_watchdog_event::error, name_.value());
      }) {
        if (!report_) {
            throw std::invalid_argument("shutdown watchdog observer is absent");
        }
        const auto started = Clock::now();
        information_timer_.arm(started + information_after);
        error_timer_.arm(started + error_after);
        report_(shutdown_watchdog_event::begin, name_.value());
    }

    ~shutdown_watchdog() { fail(); }
    shutdown_watchdog(const shutdown_watchdog&) = delete;
    shutdown_watchdog(shutdown_watchdog&&) = delete;
    shutdown_watchdog& operator=(const shutdown_watchdog&) = delete;
    shutdown_watchdog& operator=(shutdown_watchdog&&) = delete;

    void finish() noexcept { disarm(shutdown_watchdog_event::complete); }
    void fail() noexcept { disarm(shutdown_watchdog_event::failed); }

private:
    void disarm(shutdown_watchdog_event event) noexcept {
        if (active_) {
            information_timer_.cancel();
            error_timer_.cancel();
            active_ = false;
            report_(event, name_.value());
        }
    }

    shutdown_stage_name name_;
    observer report_;
    seastar::timer<Clock> information_timer_;
    seastar::timer<Clock> error_timer_;
    bool active_{true};
};

} // namespace kwaque::broker
