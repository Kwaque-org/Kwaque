#pragma once

#include <compare>
#include <cstdint>
#include <utility>

namespace kwaque::model {

namespace detail {

template<typename Tag>
class transport_value final {
public:
    using rep = std::uint64_t;

    constexpr transport_value() noexcept = default;
    constexpr explicit transport_value(rep value) noexcept
      : value_(value) {}

    [[nodiscard]] constexpr rep value() const noexcept { return value_; }

    auto operator<=>(const transport_value&) const = default;

    template<typename H>
    friend H AbslHashValue(H state, const transport_value& value) {
        return H::combine(std::move(state), value.value());
    }

private:
    rep value_{0};
};

struct transport_stream_id_tag;
struct correlation_id_tag;
struct frame_sequence_tag;

} // namespace detail

// Connection-local stream identity. Zero is reserved for connection control;
// raw data requires nonzero. This is independent of producer stream identity.
using transport_stream_id
  = detail::transport_value<detail::transport_stream_id_tag>;
// Connection-local request/reply correlation. Zero is representable; matching
// an outstanding request requires the connection owner's state.
using correlation_id = detail::transport_value<detail::correlation_id_tag>;
// Sequence within a connection/transport stream. Zero is representable;
// progression and replay validation belong to that stream's owner.
using frame_sequence = detail::transport_value<detail::frame_sequence_tag>;

} // namespace kwaque::model
