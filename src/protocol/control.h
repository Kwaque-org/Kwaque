#pragma once

#include "src/base/units.h"
#include "src/model/epoch.h"
#include "src/model/identity.h"

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace kwaque::protocol {

enum class peer_kind : std::uint8_t { broker = 1, client = 2 };
// Independent wire domain; these ordinals are never casts of runtime errors.
enum class control_error_code : std::uint8_t {
    invalid_request = 1,
    unsupported = 2,
    stale_routing = 3,
    stale_segment = 4,
    unavailable = 5,
    resource_limit = 6,
    timed_out = 7,
    aborted = 8,
    not_leader = 9,
};

struct routing_context final {
    model::topic_id topic;
    model::range_id range;
    model::range_routing_epoch routing_epoch;
    model::segment_id segment;
    model::segment_generation generation;
};
struct build_information final {
    std::string version;
    std::string revision;
    std::string build_mode;
};
struct format_capability final {
    std::uint16_t family{0};
    std::uint16_t oldest_readable{0};
    std::uint16_t current{0};
    std::uint64_t supported_features{0};
};
struct control_capabilities final {
    std::vector<std::uint16_t> protocol_versions;
    std::vector<format_capability> formats;
    std::vector<std::uint8_t> compression_codecs;
    byte_count max_frame_body_bytes;
    byte_count max_expanded_batch_bytes;
    byte_count max_header_bytes;
    byte_count max_record_bytes;
    item_count max_original_records;
};
struct control_endpoint final {
    std::string host;
    std::uint16_t port{0};
};
struct handshake_request final {
    peer_kind peer{};
    std::optional<model::cluster_id> expected_cluster;
    std::optional<model::broker_id> broker;
    build_information build;
    control_capabilities capabilities;
};
struct handshake_response final {
    model::cluster_id cluster;
    model::broker_id broker;
    build_information build;
    control_capabilities capabilities;
};
struct redirect_control final {
    routing_context destination;
    model::broker_id broker;
    control_endpoint endpoint;
    control_error_code reason{};
};
struct error_control final {
    control_error_code code{};
    std::optional<std::string> reason;
    std::optional<routing_context> observed;
};

using control_data = std::variant<
  handshake_request,
  handshake_response,
  redirect_control,
  error_control>;

namespace detail {
struct control_access;
}

// Validated, generated-free, owning observation. It grants no connection,
// routing, membership or negotiation authority. Future advertisement values
// remain data; optional presence survives conversion, unknown fields do not.
// Views returned by data() borrow this unmoved owner. A moved-from owner may
// only be destroyed or assigned; it is not an accepted control value.
class control final {
public:
    control(control&&) noexcept = default;
    control& operator=(control&&) noexcept = default;
    control(const control&) = delete;
    control& operator=(const control&) = delete;

    [[nodiscard]] const control_data& data() const& noexcept { return data_; }
    const control_data& data() const&& = delete;

private:
    friend struct detail::control_access;
    explicit control(control_data&& data) noexcept
      : data_(std::move(data)) {}
    control_data data_;
};

} // namespace kwaque::protocol
