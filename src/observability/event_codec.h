#ifndef KWAQUE_SRC_OBSERVABILITY_EVENT_CODEC_H_
#define KWAQUE_SRC_OBSERVABILITY_EVENT_CODEC_H_

#include "src/observability/event.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

namespace kwaque::observability {

class encoded_event final {
public:
    constexpr encoded_event() noexcept = default;

    [[nodiscard]] constexpr std::span<const std::uint8_t>
    bytes() const noexcept {
        return {storage_.data(), size_};
    }
    [[nodiscard]] constexpr std::size_t size() const noexcept { return size_; }
    [[nodiscard]] constexpr bool empty() const noexcept { return size_ == 0; }

    bool operator==(const encoded_event&) const = default;

private:
    friend struct event_encoding_writer;
    friend runtime::result<encoded_event> encode_event(const event&) noexcept;

    std::array<std::uint8_t, event_encoded_bytes_max> storage_{};
    std::uint16_t size_{0};
};

[[nodiscard]] runtime::result<encoded_event>
encode_event(const event& value) noexcept;
[[nodiscard]] runtime::result<event>
decode_event(std::span<const std::uint8_t> encoded) noexcept;

} // namespace kwaque::observability

#endif // KWAQUE_SRC_OBSERVABILITY_EVENT_CODEC_H_
