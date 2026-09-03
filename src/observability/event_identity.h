#ifndef KWAQUE_SRC_OBSERVABILITY_EVENT_IDENTITY_H_
#define KWAQUE_SRC_OBSERVABILITY_EVENT_IDENTITY_H_

#include "src/runtime/error.h"

#include <array>
#include <compare>
#include <cstdint>

namespace kwaque::observability {

using event_configuration_digest = std::array<std::uint8_t, 32>;

class event_sink_epoch final {
public:
    [[nodiscard]] static runtime::result<event_sink_epoch>
    make(std::uint64_t value) noexcept;

    [[nodiscard]] constexpr std::uint64_t value() const noexcept {
        return value_;
    }

    auto operator<=>(const event_sink_epoch&) const = default;

private:
    constexpr explicit event_sink_epoch(std::uint64_t value) noexcept
      : value_(value) {}

    std::uint64_t value_;
};

struct event_sink_identity final {
    event_sink_epoch epoch;
    event_configuration_digest configuration_digest;

    bool operator==(const event_sink_identity&) const = default;
};

} // namespace kwaque::observability

#endif // KWAQUE_SRC_OBSERVABILITY_EVENT_IDENTITY_H_
