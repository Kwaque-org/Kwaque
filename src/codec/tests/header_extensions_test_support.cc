#include "src/codec/tests/header_extensions_test_support.h"

#include "src/base/error.h"
#include "src/codec/header_extensions.h"

#include <seastar/core/byteorder.hh>

#include <array>
#include <cstdint>
#include <limits>

namespace kwaque::codec::testing {
namespace {

error writer_error(
  errc code,
  field_context context,
  header_extension_field field,
  std::uint64_t offset) noexcept {
    return error{
      code, context.family, static_cast<std::uint16_t>(field), offset};
}

} // namespace

result<void> append_header_extensions(
  bytes::fragmented_buffer_builder& output,
  std::span<const header_extension> extensions,
  byte_count fixed_prefix_bytes,
  const limits& policy,
  field_context context) {
    const auto config = policy.config();
    const auto initial_size = output.size();
    constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
    if (
      fixed_prefix_bytes.value() < 32
      || initial_size.value() > maximum - context.origin) {
        return codec::failure(writer_error(
          errc::invalid_argument,
          context,
          header_extension_field::region,
          context.origin));
    }
    const auto start = context.origin + initial_size.value();
    if (
      fixed_prefix_bytes > config.max_header_bytes
      || extensions.size() > config.max_extensions.value()) {
        return codec::failure(writer_error(
          errc::resource_exhausted,
          context,
          header_extension_field::region,
          start));
    }
    std::uint16_t previous_tag = 0;
    auto header_bytes = fixed_prefix_bytes;
    for (const auto& extension : extensions) {
        const auto offset = context.origin + output.size().value();
        if (header_extension_prefix_bytes > maximum - offset) {
            return codec::failure(writer_error(
              errc::invalid_argument,
              context,
              header_extension_field::region,
              offset));
        }
        if (extension.tag == 0 || extension.tag <= previous_tag) {
            return codec::failure(writer_error(
              errc::malformed_data,
              context,
              header_extension_field::tag,
              offset));
        }
        if ((extension.flags & std::uint16_t{0xfffe}) != 0) {
            return codec::failure(writer_error(
              errc::unsupported_format,
              context,
              header_extension_field::flags,
              offset + 2));
        }
        if (
          extension.value.size() > std::numeric_limits<std::uint32_t>::max()) {
            return codec::failure(writer_error(
              errc::invalid_argument,
              context,
              header_extension_field::value_length,
              offset + 4));
        }
        const auto encoded_bytes = byte_count{
          header_extension_prefix_bytes + extension.value.size()};
        const auto next_header_bytes = header_bytes.checked_add(encoded_bytes);
        if (!next_header_bytes || encoded_bytes.value() > maximum - offset) {
            return codec::failure(writer_error(
              errc::invalid_argument,
              context,
              header_extension_field::value_length,
              offset + 4));
        }
        if (*next_header_bytes > config.max_header_bytes) {
            return codec::failure(writer_error(
              errc::resource_exhausted,
              context,
              header_extension_field::region,
              offset));
        }
        std::array<char, header_extension_prefix_bytes> prefix{};
        seastar::write_le(prefix.data(), extension.tag);
        seastar::write_le(prefix.data() + 2, extension.flags);
        seastar::write_le(
          prefix.data() + 4,
          static_cast<std::uint32_t>(extension.value.size()));
        if (
          auto written = detail::append_integer(output, prefix, context);
          !written) {
            return written;
        }
        if (
          auto written = detail::append_integer(
            output, extension.value, context);
          !written) {
            return written;
        }
        previous_tag = extension.tag;
        header_bytes = *next_header_bytes;
    }
    return {};
}

} // namespace kwaque::codec::testing
