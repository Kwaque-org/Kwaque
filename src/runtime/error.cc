#include "src/runtime/error.h"

#include <charconv>

namespace kwaque::runtime {

namespace {

void append_bounded(
  std::string& output, std::string_view value, std::size_t maximum) {
    if (output.size() >= maximum) {
        return;
    }
    const auto available = maximum - output.size();
    output.append(value.substr(0, available));
}

void append_number(
  std::string& output, std::uint64_t value, std::size_t maximum) {
    std::array<char, 32> digits{};
    const auto [end, error] = std::to_chars(
      digits.data(), digits.data() + digits.size(), value);
    if (error == std::errc{}) {
        append_bounded(
          output,
          std::string_view{
            digits.data(), static_cast<std::size_t>(end - digits.data())},
          maximum);
    }
}

} // namespace

operation_error::operation_error(errc code, operation_kind operation) noexcept
  : code_(code)
  , operation_(operation) {}

bool operation_error::add_context(
  operation_context_key key, std::uint64_t value) noexcept {
    if (
      context_size_ == max_context_fields
      || static_cast<std::uint8_t>(key)
           > static_cast<std::uint8_t>(operation_context_key::stable_id)) {
        return false;
    }
    for (std::size_t index = 0; index < context_size_; ++index) {
        if (context_keys_[index] == key) {
            return false;
        }
    }
    context_keys_[context_size_] = key;
    context_values_[context_size_] = value;
    ++context_size_;
    return true;
}

std::string operation_error::render() const {
    std::string output;
    output.reserve(max_rendered_size);
    append_bounded(output, "operation=", max_rendered_size);
    append_bounded(output, to_string(operation_), max_rendered_size);
    append_bounded(output, " error=kwaque:", max_rendered_size);
    append_number(output, static_cast<std::uint64_t>(code_), max_rendered_size);
    for (std::size_t index = 0; index < context_size_; ++index) {
        const auto field = context_at(index);
        append_bounded(output, " ", max_rendered_size);
        append_bounded(output, to_string(field.key), max_rendered_size);
        append_bounded(output, "=", max_rendered_size);
        append_number(output, field.value, max_rendered_size);
    }
    return output;
}

std::string_view to_string(operation_kind operation) noexcept {
    switch (operation) {
    case operation_kind::generic:
        return "generic";
    case operation_kind::timer:
        return "timer";
    case operation_kind::random:
        return "random";
    case operation_kind::file:
        return "file";
    case operation_kind::network:
        return "network";
    case operation_kind::dns:
        return "dns";
    case operation_kind::fault:
        return "fault";
    case operation_kind::resource:
        return "resource";
    }
    return "unknown";
}

std::string_view to_string(operation_context_key key) noexcept {
    switch (key) {
    case operation_context_key::shard:
        return "shard";
    case operation_context_key::peer:
        return "peer";
    case operation_context_key::attempt:
        return "attempt";
    case operation_context_key::bytes:
        return "bytes";
    case operation_context_key::items:
        return "items";
    case operation_context_key::occurrence:
        return "occurrence";
    case operation_context_key::deadline_ns:
        return "deadline_ns";
    case operation_context_key::stable_id:
        return "stable_id";
    }
    return "unknown";
}

} // namespace kwaque::runtime
