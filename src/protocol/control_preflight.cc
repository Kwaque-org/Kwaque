#include "src/protocol/control_preflight.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <utf8_range.h>

namespace kwaque::protocol::detail {
namespace {
using kind = control_field_kind;

struct scope final {
    const control_schema* schema{nullptr};
    std::size_t end{0};
    // The schema table checks this capacity. Counts also retain
    // seen singulars; empty packed segments deliberately do not add elements.
    std::array<std::uint64_t, control_scope_fields> counts{};
    std::array<std::uint64_t, control_scope_fields> values{};
};

class wire_reader final {
public:
    wire_reader(std::span<const char> wire, codec::field_context context)
      : wire_(wire)
      , context_(context) {}

    codec::error error(errc code, std::size_t at) const noexcept {
        return codec::error{code, context_.family, field, context_.origin + at};
    }

    // Unlike canonical raw integers, legal non-minimal encodings are accepted.
    // Check final bits before shifting and never read beyond the current
    // region.
    codec::result<std::uint64_t> varint(std::size_t end, unsigned bits) {
        std::uint64_t value = 0;
        for (unsigned shift = 0; shift < bits; shift += 7U) {
            if (position == end)
                return codec::failure(error(errc::malformed_data, position));
            const auto at = position++;
            const auto octet = static_cast<unsigned char>(wire_[at]);
            const auto remaining = bits - shift;
            if (remaining <= 7U && (octet >> remaining) != 0)
                return codec::failure(error(errc::malformed_data, at));
            value |= static_cast<std::uint64_t>(octet & 0x7fU) << shift;
            if ((octet & 0x80U) == 0) return value;
        }
        return codec::failure(error(errc::malformed_data, position));
    }

    std::size_t position{0};
    std::uint16_t field{0};

private:
    std::span<const char> wire_;
    codec::field_context context_;
};

codec::result<void> scalar(
  scope& scope,
  std::size_t index,
  std::uint64_t value,
  const codec::limits_config& limits,
  const wire_reader& reader,
  std::size_t at) {
    const auto& field = scope.schema->fields[index];
    if (field.kind == kind::enumeration) {
        if (value == 0)
            return codec::failure(reader.error(errc::malformed_data, at));
        if (
          value >= 64 || (field.enum_values & (std::uint64_t{1} << value)) == 0)
            return codec::failure(reader.error(errc::unsupported_format, at));
    } else if (value < field.minimum_value || value > field.maximum_value) {
        return codec::failure(reader.error(errc::malformed_data, at));
    }
    if (field.repeated) {
        if (
          scope.counts[index]
          >= std::min(field.maximum_elements, limits.max_control_repeated)
               .value())
            return codec::failure(reader.error(errc::resource_exhausted, at));
        if (scope.counts[index] != 0 && value <= scope.values[index])
            return codec::failure(reader.error(errc::malformed_data, at));
        ++scope.counts[index];
    }
    scope.values[index] = value;
    return {};
}

bool matches(const control_field& field, unsigned wire) noexcept {
    switch (field.kind) {
    case kind::uint32:
        return wire == 0 || (field.repeated && wire == 2);
    case kind::uint64:
    case kind::enumeration:
        return wire == 0;
    case kind::fixed64:
        return wire == 1;
    case kind::identity:
    case kind::text:
    case kind::message:
        return wire == 2;
    }
    return false;
}
} // namespace

seastar::future<codec::result<item_count>> preflight_control(
  std::span<const char> wire,
  control_message message,
  codec::cooperative_work& work,
  codec::field_context context) {
    const auto* schema = schema_for(message);
    const codec::error anchor{
      errc::success, context.family, context.field, context.origin};
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (!schema || context.origin > UINT64_MAX - wire.size())
        co_return codec::failure(
          codec::error{
            errc::invalid_argument,
            context.family,
            context.field,
            context.origin});
    const auto limits = work.policy().config();
    wire_reader reader{wire, context};
    reader.field = context.field;
    if (
      wire.size()
      > std::min(schema->maximum_wire_bytes, limits.max_control_bytes).value())
        co_return codec::failure(reader.error(errc::resource_exhausted, 0));
    constexpr auto maximum_depth
      = codec::limits::defaults().config().max_nesting_depth.value();
    std::array<scope, maximum_depth> stack{};
    stack[0] = scope{.schema = schema, .end = wire.size()};
    std::size_t depth = 1;
    std::uint64_t units = 0;
    while (depth != 0) {
        // Covers one tag, length/scalar and the fixed schema's presence scan.
        if (
          auto ready = co_await work.admit(
            byte_count{20}, item_count{1}, anchor);
          !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        auto& current = stack[depth - 1];
        if (reader.position == current.end) {
            for (std::size_t i = 0; i < current.schema->fields.size(); ++i) {
                const auto& field = current.schema->fields[i];
                if (
                  (field.required && current.counts[i] == 0)
                  || current.counts[i] < field.minimum_elements.value()) {
                    reader.field = static_cast<std::uint16_t>(field.number);
                    co_return codec::failure(
                      reader.error(errc::malformed_data, reader.position));
                }
            }
            if (current.schema->message == control_message::format_capability) {
                if (current.values[1] > current.values[2])
                    co_return codec::failure(
                      reader.error(errc::malformed_data, reader.position));
                // This child occurs only at Capabilities.formats, or as a
                // standalone private inspection root.
                if (depth > 1) {
                    auto& parent = stack[depth - 2];
                    if (current.values[0] <= parent.values[1])
                        co_return codec::failure(
                          reader.error(errc::malformed_data, reader.position));
                    parent.values[1] = current.values[0];
                }
            }
            --depth;
            continue;
        }
        reader.field = context.field;
        const auto at = reader.position;
        const auto tag = reader.varint(current.end, 32);
        if (!tag) co_return codec::failure(tag.error());
        const auto number = static_cast<std::uint32_t>(*tag >> 3U);
        const auto wire_type = static_cast<unsigned>(*tag & 7U);
        if (number == 0 || wire_type >= 6)
            co_return codec::failure(reader.error(errc::malformed_data, at));
        if (wire_type == 3 || wire_type == 4)
            co_return codec::failure(
              reader.error(errc::unsupported_format, at));
        if (++units > limits.max_control_fields.value())
            co_return codec::failure(
              reader.error(errc::resource_exhausted, at));
        const auto* field = field_for(*current.schema, number);
        if (field && !matches(*field, wire_type)) {
            if (!current.schema->informational_merge)
                co_return codec::failure(
                  reader.error(errc::malformed_data, at));
            field = nullptr;
        }
        std::size_t index = 0;
        if (field) {
            index = static_cast<std::size_t>(
              field - current.schema->fields.data());
            reader.field = static_cast<std::uint16_t>(field->number);
            if (!field->repeated) {
                if (
                  current.counts[index] != 0
                  && !current.schema->informational_merge)
                    co_return codec::failure(
                      reader.error(errc::malformed_data, at));
                ++current.counts[index];
            }
        }
        switch (wire_type) {
        case 0: {
            const auto value = reader.varint(current.end, 64);
            if (!value) co_return codec::failure(value.error());
            if (field) {
                const auto valid = scalar(
                  current, index, *value, limits, reader, at);
                if (!valid) co_return codec::failure(valid.error());
            }
            break;
        }
        case 1:
        case 5: {
            const std::size_t width = wire_type == 1 ? 8 : 4;
            if (width > current.end - reader.position)
                co_return codec::failure(
                  reader.error(errc::malformed_data, reader.position));
            reader.position += width;
            break;
        }
        case 2: {
            const auto length = reader.varint(current.end, 32);
            if (!length) co_return codec::failure(length.error());
            if (
              *length
                > static_cast<std::uint64_t>(std::numeric_limits<int>::max())
              || *length > current.end - reader.position)
                co_return codec::failure(
                  reader.error(errc::malformed_data, reader.position));
            const auto size = static_cast<std::size_t>(*length);
            const auto end = reader.position + size;
            if (field && field->kind == kind::message) {
                const auto* child = schema_for(field->child);
                if (
                  depth >= limits.max_nesting_depth.value()
                  || size
                       > std::min(
                           child->maximum_wire_bytes, limits.max_control_bytes)
                           .value())
                    co_return codec::failure(
                      reader.error(errc::resource_exhausted, at));
                if (
                  field->repeated
                  && ++current.counts[index]
                       > std::min(
                           field->maximum_elements, limits.max_control_repeated)
                           .value())
                    co_return codec::failure(
                      reader.error(errc::resource_exhausted, at));
                stack[depth++] = scope{.schema = child, .end = end};
            } else if (field && field->repeated) {
                while (reader.position < end) {
                    if (
                      auto ready = co_await work.admit(
                        byte_count{10}, item_count{1}, anchor);
                      !ready)
                        co_return codec::failure(ready.error());
                    if (auto ready = work.poll(anchor); !ready)
                        co_return codec::failure(ready.error());
                    if (++units > limits.max_control_fields.value())
                        co_return codec::failure(reader.error(
                          errc::resource_exhausted, reader.position));
                    const auto value = reader.varint(end, 64);
                    if (!value) co_return codec::failure(value.error());
                    const auto valid = scalar(
                      current, index, *value, limits, reader, at);
                    if (!valid) co_return codec::failure(valid.error());
                }
            } else {
                if (
                  field && field->kind == kind::identity
                  && size != control_identity_bytes.value())
                    co_return codec::failure(
                      reader.error(errc::malformed_data, at));
                if (
                  size > limits.max_control_field_bytes.value()
                  || (field && size > field->maximum_bytes.value()))
                    co_return codec::failure(
                      reader.error(errc::resource_exhausted, at));
                if (field && size < field->minimum_bytes.value())
                    co_return codec::failure(
                      reader.error(errc::malformed_data, at));
                if (field) {
                    // UTF-8 is an existing bounded native leaf. Admission also
                    // covers the identity/host scan before generated parsing.
                    if (
                      auto ready = co_await work.admit(
                        byte_count{2U * size}, item_count{1}, anchor);
                      !ready)
                        co_return codec::failure(ready.error());
                    if (auto ready = work.poll(anchor); !ready)
                        co_return codec::failure(ready.error());
                    const auto text = wire.subspan(reader.position, size);
                    if (field->kind == kind::identity) {
                        if (std::ranges::all_of(text, [](char c) {
                                return c == 0;
                            }))
                            co_return codec::failure(
                              reader.error(errc::malformed_data, at));
                    } else {
                        if (utf8_range_IsValid(text.data(), text.size()) == 0)
                            co_return codec::failure(
                              reader.error(errc::malformed_data, at));
                        if (
                          current.schema->message == control_message::endpoint
                          && std::ranges::any_of(text, [](char c) {
                                 const auto octet = static_cast<unsigned char>(
                                   c);
                                 return octet <= 0x20 || octet == 0x7f;
                             }))
                            co_return codec::failure(
                              reader.error(errc::malformed_data, at));
                    }
                    if (auto ready = work.poll(anchor); !ready)
                        co_return codec::failure(ready.error());
                }
                reader.position = end;
            }
            break;
        }
        default:
            co_return codec::failure(reader.error(errc::malformed_data, at));
        }
    }
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    co_return item_count{units};
}
} // namespace kwaque::protocol::detail
