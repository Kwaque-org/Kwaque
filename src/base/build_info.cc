#include "src/base/build_info.h"

#include "src/base/build_stamp.h"

#include <string>
#include <string_view>

#ifndef KWAQUE_VERSION
#define KWAQUE_VERSION "unknown"
#endif

#ifndef KWAQUE_BUILD_MODE
#define KWAQUE_BUILD_MODE "unknown"
#endif

#ifndef KWAQUE_SEASTAR_VERSION
#define KWAQUE_SEASTAR_VERSION "unknown"
#endif

#ifndef KWAQUE_PROTOBUF_VERSION
#define KWAQUE_PROTOBUF_VERSION "unknown"
#endif

#define KWAQUE_STRINGIFY_INNER(value) #value
#define KWAQUE_STRINGIFY(value) KWAQUE_STRINGIFY_INNER(value)

namespace kwaque::build_info {

namespace {

#if defined(__clang__)
constexpr std::string_view compiler_value =
    "clang-" KWAQUE_STRINGIFY(__clang_major__) "." KWAQUE_STRINGIFY(
        __clang_minor__) "." KWAQUE_STRINGIFY(__clang_patchlevel__);
#elif defined(__GNUC__)
constexpr std::string_view compiler_value =
    "gcc-" KWAQUE_STRINGIFY(__GNUC__) "." KWAQUE_STRINGIFY(
        __GNUC_MINOR__) "." KWAQUE_STRINGIFY(__GNUC_PATCHLEVEL__);
#else
constexpr std::string_view compiler_value = "unknown";
#endif

void append_field(std::string &output, std::string_view name,
                  std::string_view value) {
  if (!output.empty()) {
    output.push_back('\t');
  }
  output.append(name);
  output.push_back('=');
  output.append(value);
}

} // namespace

std::string_view version() noexcept { return KWAQUE_VERSION; }

std::string_view git_revision() noexcept {
  return detail::stamped_git_revision;
}

bool git_dirty() noexcept { return detail::stamped_git_dirty; }

std::string_view build_timestamp() noexcept {
  return detail::stamped_build_timestamp;
}

std::string_view build_mode() noexcept { return KWAQUE_BUILD_MODE; }

std::string_view compiler() noexcept { return compiler_value; }

std::string_view protobuf_version() noexcept { return KWAQUE_PROTOBUF_VERSION; }

std::string_view seastar_version() noexcept { return KWAQUE_SEASTAR_VERSION; }

std::string version_line() {
  std::string output;
  output.reserve(256);
  append_field(output, "version", version());
  append_field(output, "revision", git_revision());
  append_field(output, "dirty", git_dirty() ? "true" : "false");
  append_field(output, "build_timestamp", build_timestamp());
  append_field(output, "build_mode", build_mode());
  append_field(output, "compiler", compiler());
  append_field(output, "protobuf", protobuf_version());
  append_field(output, "seastar", seastar_version());
  return output;
}

} // namespace kwaque::build_info

#undef KWAQUE_STRINGIFY
#undef KWAQUE_STRINGIFY_INNER
