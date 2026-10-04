#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace kwaque::admin {

inline constexpr std::size_t max_json_code_bytes = 64;
inline constexpr std::size_t max_json_detail_bytes = 256;
inline constexpr std::size_t max_json_build_field_bytes = 128;

inline constexpr std::string_view json_content_type = "application/json";
// RFC 9457 problem details; clients must ignore members they do not know.
inline constexpr std::string_view problem_content_type
  = "application/problem+json";

struct json_response final {
    std::uint16_t status;
    std::string_view content_type;
    std::string body;
};

[[nodiscard]] json_response liveness_response(bool live);
[[nodiscard]] json_response readiness_response(bool ready);
[[nodiscard]] json_response not_found_response();
[[nodiscard]] json_response method_not_allowed_response();

// The /v1/version fields, in response order.
struct build_identity final {
    std::string_view version;
    std::string_view revision;
    bool dirty;
    std::string_view build_timestamp;
    std::string_view build_mode;
};

[[nodiscard]] std::string build_info_json(const build_identity& build);
[[nodiscard]] std::string current_version_json();

// A problem document with the "about:blank" type, whose title is the HTTP
// status phrase, plus a stable machine-readable "code" extension member.
[[nodiscard]] std::string problem_json(
  std::uint16_t status, std::string_view code, std::string_view detail);

} // namespace kwaque::admin
