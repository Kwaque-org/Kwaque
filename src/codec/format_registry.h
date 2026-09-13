#pragma once

#include "src/codec/error.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

namespace kwaque::codec {

enum class format_family : std::uint16_t {
    submitted_batch = 1,
    assigned_batch = 2,
    segment_header = 3,
    segment_batch_block = 4,
    wal_prepare = 5,
    durable_boundary_footer = 6,
    sealed_extent = 7,
    sparse_index = 8,
    range_manifest = 9,
    read_checkpoint = 10,
};

namespace detail {
struct format_registry_access;
struct format_registry_test_access;
} // namespace detail

// Registration describes framing compatibility, not an implemented body
// decoder, advertised capability, or permission to activate a writer.
class format_descriptor final {
public:
    [[nodiscard]] constexpr format_family family() const noexcept {
        return family_;
    }
    [[nodiscard]] constexpr std::uint16_t current() const noexcept {
        return current_;
    }
    [[nodiscard]] constexpr std::uint16_t oldest_readable() const noexcept {
        return oldest_readable_;
    }
    [[nodiscard]] constexpr std::uint16_t writer() const noexcept {
        return writer_;
    }
    [[nodiscard]] constexpr std::uint16_t minimum_reader() const noexcept {
        return minimum_reader_;
    }
    [[nodiscard]] constexpr std::uint64_t supported_features() const noexcept {
        return supported_features_;
    }

    bool operator==(const format_descriptor&) const noexcept = default;

private:
    friend struct detail::format_registry_access;
    friend struct detail::format_registry_test_access;

    constexpr format_descriptor(
      format_family family,
      std::uint16_t current,
      std::uint16_t oldest_readable,
      std::uint16_t writer,
      std::uint16_t minimum_reader,
      std::uint64_t supported_features) noexcept
      : family_(family)
      , current_(current)
      , oldest_readable_(oldest_readable)
      , writer_(writer)
      , minimum_reader_(minimum_reader)
      , supported_features_(supported_features) {}

    format_family family_;
    std::uint16_t current_;
    std::uint16_t oldest_readable_;
    std::uint16_t writer_;
    std::uint16_t minimum_reader_;
    std::uint64_t supported_features_;
};

namespace detail {

struct format_registry_access final {
    [[nodiscard]] static consteval std::array<format_descriptor, 10> make() {
        return {{
          {format_family::submitted_batch, 1, 1, 1, 1, 0},
          {format_family::assigned_batch, 1, 1, 1, 1, 0},
          {format_family::segment_header, 1, 1, 1, 1, 0},
          {format_family::segment_batch_block, 1, 1, 1, 1, 0},
          {format_family::wal_prepare, 1, 1, 1, 1, 0},
          {format_family::durable_boundary_footer, 1, 1, 1, 1, 0},
          {format_family::sealed_extent, 1, 1, 1, 1, 0},
          {format_family::sparse_index, 1, 1, 1, 1, 0},
          {format_family::range_manifest, 1, 1, 1, 1, 0},
          {format_family::read_checkpoint, 1, 1, 1, 1, 0},
        }};
    }
};

[[nodiscard]] consteval bool
valid_format_registry(std::span<const format_descriptor> descriptors) noexcept {
    if (descriptors.size() != 10) {
        return false;
    }
    std::array<bool, 10> seen{};
    for (const auto descriptor : descriptors) {
        const auto family = static_cast<std::uint16_t>(descriptor.family());
        if (family == 0 || family > seen.size() || seen[family - 1U]) {
            return false;
        }
        seen[family - 1U] = true;
        if (
          descriptor.oldest_readable() == 0
          || descriptor.oldest_readable() > descriptor.current()
          || descriptor.writer() == 0 || descriptor.minimum_reader() == 0
          || descriptor.minimum_reader() > descriptor.writer()) {
            return false;
        }
    }
    return true;
}

inline constexpr auto format_registry = format_registry_access::make();
static_assert(valid_format_registry(format_registry));

[[nodiscard]] constexpr error
registry_error(errc reason, error anchor) noexcept {
    return error{reason, anchor.family(), anchor.field(), anchor.byte_offset()};
}

// These errors are independent of family lookup. Framing readers apply them
// first after header integrity, before selecting a family's reader profile.
[[nodiscard]] constexpr result<void> validate_sender_version_values(
  std::uint16_t writer,
  std::uint16_t minimum_reader,
  error writer_anchor,
  error minimum_reader_anchor) noexcept {
    if (minimum_reader > writer) {
        return failure(
          registry_error(errc::malformed_data, minimum_reader_anchor));
    }
    if (writer == 0) {
        return failure(registry_error(errc::unsupported_format, writer_anchor));
    }
    if (minimum_reader == 0) {
        return failure(
          registry_error(errc::unsupported_format, minimum_reader_anchor));
    }
    return {};
}

} // namespace detail

// The anchor supplies trusted diagnostics. An unknown wire family is never
// promoted to a descriptor or used to select payload allocation policy.
[[nodiscard]] constexpr result<format_descriptor> lookup_format(
  std::uint16_t raw, error anchor = error{errc::unsupported_format}) noexcept {
    if (raw == 0) {
        return failure(detail::registry_error(errc::malformed_data, anchor));
    }
    for (const auto descriptor : detail::format_registry) {
        if (static_cast<std::uint16_t>(descriptor.family()) == raw) {
            return descriptor;
        }
    }
    return failure(detail::registry_error(errc::unsupported_format, anchor));
}

// This admits only numeric framing compatibility. A known body grammar and
// exact body consumption remain mandatory, including for a newer writer.
[[nodiscard]] constexpr result<void> validate_sender_versions(
  std::uint16_t writer,
  std::uint16_t minimum_reader,
  const format_descriptor& reader,
  error writer_anchor,
  error minimum_reader_anchor) noexcept {
    if (
      auto values = detail::validate_sender_version_values(
        writer, minimum_reader, writer_anchor, minimum_reader_anchor);
      !values) {
        return values;
    }
    if (minimum_reader > reader.current()) {
        return failure(
          detail::registry_error(
            errc::unsupported_format, minimum_reader_anchor));
    }
    if (writer < reader.oldest_readable()) {
        return failure(
          detail::registry_error(errc::unsupported_format, writer_anchor));
    }
    return {};
}

[[nodiscard]] constexpr result<void> validate_required_features(
  std::uint64_t required,
  const format_descriptor& reader,
  error anchor) noexcept {
    if ((required & ~reader.supported_features()) != 0) {
        return failure(
          detail::registry_error(errc::unsupported_format, anchor));
    }
    return {};
}

} // namespace kwaque::codec
