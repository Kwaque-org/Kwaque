#ifndef KWAQUE_SRC_OBSERVABILITY_EVENT_SEQUENCE_H_
#define KWAQUE_SRC_OBSERVABILITY_EVENT_SEQUENCE_H_

#include "src/observability/event.h"
#include "src/runtime/error.h"

#include <array>
#include <compare>
#include <cstdint>
#include <utility>

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

class event_sequence_test_access;

class event_sequence final {
public:
    class reservation final {
    public:
        ~reservation();

        reservation(const reservation&) = delete;
        reservation& operator=(const reservation&) = delete;
        reservation(reservation&& other) noexcept;
        reservation& operator=(reservation&&) = delete;

        [[nodiscard]] const event& value() const noexcept { return value_; }
        [[nodiscard]] bool active() const noexcept { return owner_ != nullptr; }
        void commit() noexcept;

    private:
        friend class event_sequence;

        reservation(event_sequence& owner, event value) noexcept
          : owner_(&owner)
          , value_(std::move(value)) {}

        event_sequence* owner_;
        event value_;
    };

    explicit event_sequence(event_sink_identity identity) noexcept
      : identity_(identity) {}
    ~event_sequence();

    event_sequence(const event_sequence&) = delete;
    event_sequence& operator=(const event_sequence&) = delete;
    event_sequence(event_sequence&&) = delete;
    event_sequence& operator=(event_sequence&&) = delete;

    [[nodiscard]] runtime::result<reservation>
    prepare(const event_request& request, event_shard shard) noexcept;

    [[nodiscard]] constexpr const event_sink_identity&
    identity() const noexcept {
        return identity_;
    }
    [[nodiscard]] constexpr std::uint64_t last_sequence() const noexcept {
        return last_sequence_;
    }

private:
    friend class event_sequence_test_access;

    void release() noexcept { reserved_ = false; }

    event_sink_identity identity_;
    std::uint64_t last_sequence_{0};
    bool reserved_{false};
};

} // namespace kwaque::observability

#endif // KWAQUE_SRC_OBSERVABILITY_EVENT_SEQUENCE_H_
