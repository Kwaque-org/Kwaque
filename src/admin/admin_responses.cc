#include "src/admin/admin_responses.h"

#include "src/base/build_info.h"

#include <algorithm>

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

} // namespace

json_response liveness_response(bool live) {
    if (!live) {
        return {
          .status = 503,
          .body = error_json(
            "broker_not_live", "broker shutdown is in progress")};
    }
    return {.status = 200, .body = R"({"status":"live"})"};
}

json_response readiness_response(bool ready) {
    if (!ready) {
        return {
          .status = 503,
          .body = error_json("broker_not_ready", "broker is not ready")};
    }
    return {.status = 200, .body = R"({"status":"ready"})"};
}

kwaque::common::v1::BuildInfo current_build_info() {
    kwaque::common::v1::BuildInfo info;
    info.set_version(build_info::version());
    info.set_revision(build_info::git_revision());
    info.set_build_mode(build_info::build_mode());
    return info;
}

std::string build_info_json(const kwaque::common::v1::BuildInfo& info) {
    std::string output;
    output.reserve(512);
    output.push_back('{');
    append_member(
      output, "version", info.version(), max_json_build_field_bytes);
    output.push_back(',');
    append_member(
      output, "revision", info.revision(), max_json_build_field_bytes);
    output.push_back(',');
    append_member(
      output, "build_mode", info.build_mode(), max_json_build_field_bytes);
    output.push_back('}');
    return output;
}

std::string error_json(
  std::string_view code,
  std::string_view message,
  std::optional<std::string_view> correlation_id) {
    std::string output;
    output.reserve(512);
    output.push_back('{');
    append_member(output, "code", code, max_json_code_bytes);
    output.push_back(',');
    append_member(output, "message", message, max_json_message_bytes);
    output += ",\"correlation_id\":";
    if (correlation_id) {
        append_json_string(output, *correlation_id, max_json_correlation_bytes);
    } else {
        output += "null";
    }
    output.push_back('}');
    return output;
}

} // namespace kwaque::admin
