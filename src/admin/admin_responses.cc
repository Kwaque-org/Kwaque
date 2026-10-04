#include "src/admin/admin_responses.h"

#include "src/base/build_info.h"

#include <algorithm>
#include <string>

namespace kwaque::admin {

namespace {

void append_json_string(
  std::string& output, std::string_view value, std::size_t maximum_input) {
    output.push_back('"');
    const auto bounded_size = std::min(value.size(), maximum_input);
    for (const char raw_character : value.substr(0, bounded_size)) {
        const auto character = static_cast<unsigned char>(raw_character);
        switch (character) {
        case '"':
            output += "\\\"";
            break;
        case '\\':
            output += "\\\\";
            break;
        case '\b':
            output += "\\b";
            break;
        case '\f':
            output += "\\f";
            break;
        case '\n':
            output += "\\n";
            break;
        case '\r':
            output += "\\r";
            break;
        case '\t':
            output += "\\t";
            break;
        default:
            if (character < 0x20 || character > 0x7e) {
                constexpr char digits[] = "0123456789abcdef";
                output += "\\u00";
                output.push_back(digits[character >> 4]);
                output.push_back(digits[character & 0x0f]);
            } else {
                output.push_back(static_cast<char>(character));
            }
        }
    }
    if (value.size() > maximum_input) {
        output += "<truncated>";
    }
    output.push_back('"');
}

void append_member(
  std::string& output,
  std::string_view name,
  std::string_view value,
  std::size_t maximum_value) {
    append_json_string(output, name, 64);
    output.push_back(':');
    append_json_string(output, value, maximum_value);
}

std::string_view status_title(std::uint16_t status) noexcept {
    switch (status) {
    case 404:
        return "Not Found";
    case 405:
        return "Method Not Allowed";
    case 503:
        return "Service Unavailable";
    default:
        return "Error";
    }
}

} // namespace

json_response liveness_response(bool live) {
    if (!live) {
        return {
          .status = 503,
          .content_type = problem_content_type,
          .body = problem_json(503, "broker_not_live", "broker is not live")};
    }
    return {
      .status = 200,
      .content_type = json_content_type,
      .body = R"({"status":"live"})"};
}

json_response readiness_response(bool ready) {
    if (!ready) {
        return {
          .status = 503,
          .content_type = problem_content_type,
          .body = problem_json(503, "broker_not_ready", "broker is not ready")};
    }
    return {
      .status = 200,
      .content_type = json_content_type,
      .body = R"({"status":"ready"})"};
}

json_response not_found_response() {
    return {
      .status = 404,
      .content_type = problem_content_type,
      .body = problem_json(404, "not_found", "no such resource")};
}

json_response method_not_allowed_response() {
    return {
      .status = 405,
      .content_type = problem_content_type,
      .body = problem_json(
        405, "method_not_allowed", "the resource supports only GET and HEAD")};
}

std::string build_info_json(const build_identity& build) {
    std::string output;
    output.reserve(512);
    output.push_back('{');
    append_member(output, "version", build.version, max_json_build_field_bytes);
    output.push_back(',');
    append_member(
      output, "revision", build.revision, max_json_build_field_bytes);
    output += build.dirty ? R"(,"dirty":true,)" : R"(,"dirty":false,)";
    append_member(
      output,
      "build_timestamp",
      build.build_timestamp,
      max_json_build_field_bytes);
    output.push_back(',');
    append_member(
      output, "build_mode", build.build_mode, max_json_build_field_bytes);
    output.push_back('}');
    return output;
}

std::string current_version_json() {
    return build_info_json({
      .version = build_info::version(),
      .revision = build_info::git_revision(),
      .dirty = build_info::git_dirty(),
      .build_timestamp = build_info::build_timestamp(),
      .build_mode = build_info::build_mode(),
    });
}

std::string problem_json(
  std::uint16_t status, std::string_view code, std::string_view detail) {
    std::string output;
    output.reserve(512);
    output += R"({"type":"about:blank",)";
    append_member(output, "title", status_title(status), 64);
    output += R"(,"status":)";
    output += std::to_string(status);
    output.push_back(',');
    append_member(output, "detail", detail, max_json_detail_bytes);
    output.push_back(',');
    append_member(output, "code", code, max_json_code_bytes);
    output.push_back('}');
    return output;
}

} // namespace kwaque::admin
