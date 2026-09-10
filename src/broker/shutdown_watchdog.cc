#include "src/broker/shutdown_watchdog.h"

#include "src/base/logging.h"

namespace kwaque::broker {

shutdown_stage_name::shutdown_stage_name(std::string_view name) {
    if (name.empty() || name.size() > max_size) {
        throw std::invalid_argument("shutdown stage name has invalid size");
    }
    for (const char character : name) {
        const bool letter = (character >= 'a' && character <= 'z')
                            || (character >= 'A' && character <= 'Z');
        const bool digit = character >= '0' && character <= '9';
        if (
          !letter && !digit && character != '.' && character != '_'
          && character != '-') {
            throw std::invalid_argument("shutdown stage name is invalid");
        }
        value_[size_++] = character;
    }
}

void report_shutdown_event(
  shutdown_watchdog_event event, std::string_view name) noexcept {
    switch (event) {
    case shutdown_watchdog_event::begin:
        log::broker().info("Shutting down: {}", name);
        break;
    case shutdown_watchdog_event::information:
        log::broker().info(
          "Service {} is taking more than 15 seconds to shut down", name);
        break;
    case shutdown_watchdog_event::error:
        log::broker().error(
          "Service {} is taking more than 120 seconds to shut down", name);
        break;
    case shutdown_watchdog_event::complete:
        log::broker().info("Shutdown completed: {}", name);
        break;
    case shutdown_watchdog_event::failed:
        log::broker().error("Shutdown failed: {}", name);
        break;
    }
}

} // namespace kwaque::broker
