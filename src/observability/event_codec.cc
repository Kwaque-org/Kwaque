#include "src/observability/event_codec.h"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <string_view>
#include <type_traits>
#include <utility>

namespace kwaque::observability {

struct event_codec_access final {
    [[nodiscard]] static runtime::result<event_text>
    decode_text(event_field_key role, std::string_view value) noexcept {
        return event_text::decode(role, value);
    }

    [[nodiscard]] static runtime::result<event> make_event(
      const event_request_context& context,
      std::uint32_t shard,
      std::uint64_t sequence,
      std::span<const event_field> fields) noexcept {
        return event::make(context, event_shard{shard}, sequence, fields);
    }
};

struct event_encoding_writer final {
    explicit event_encoding_writer(encoded_event& output) noexcept
      : output_(output) {}

    [[nodiscard]] bool append_byte(std::uint8_t value) noexcept {
        if (output_.size_ == output_.storage_.size()) {
            return false;
        }
        output_.storage_[output_.size_++] = value;
        return true;
    }

    template<typename Integer>
    [[nodiscard]] bool append_integer(Integer value) noexcept {
        using unsigned_type = std::make_unsigned_t<Integer>;
        auto encoded = static_cast<unsigned_type>(value);
        for (std::size_t index = 0; index < sizeof(Integer); ++index) {
            if (!append_byte(static_cast<std::uint8_t>(encoded & 0xffU))) {
                return false;
            }
            encoded >>= 8U;
        }
        return true;
    }

    [[nodiscard]] bool append_text(std::string_view value) noexcept {
        for (const char character : value) {
            if (!append_byte(static_cast<std::uint8_t>(character))) {
                return false;
            }
        }
        return true;
    }

private:
    encoded_event& output_;
};

namespace {

[[nodiscard]] runtime::operation_error codec_error(errc code) noexcept {
    return runtime::operation_error{
      code, runtime::operation_kind::observability};
}

class event_encoding_reader final {
public:
    explicit event_encoding_reader(std::span<const std::uint8_t> input) noexcept
      : input_(input) {}

    template<typename Integer>
    [[nodiscard]] bool read_integer(Integer& output) noexcept {
        using unsigned_type = std::make_unsigned_t<Integer>;
        if (remaining() < sizeof(Integer)) {
            return false;
        }
        unsigned_type value = 0;
        for (std::size_t index = 0; index < sizeof(Integer); ++index) {
            value |= static_cast<unsigned_type>(input_[offset_ + index])
                     << (index * 8U);
        }
        offset_ += sizeof(Integer);
        output = static_cast<Integer>(value);
        return true;
    }

    [[nodiscard]] std::optional<std::string_view>
    read_text(std::size_t size) noexcept {
        if (size > remaining()) {
            return std::nullopt;
        }
        const auto* data = reinterpret_cast<const char*>(
          input_.data() + offset_);
        offset_ += size;
        return std::string_view{data, size};
    }

    [[nodiscard]] constexpr std::size_t remaining() const noexcept {
        return input_.size() - offset_;
    }

private:
    std::span<const std::uint8_t> input_;
    std::size_t offset_{0};
};

[[nodiscard]] bool append_field_value(
  event_encoding_writer& writer, const event_field_value& value) noexcept {
    switch (value.type()) {
    case event_field_type::signed_integer:
        return writer.append_integer(
          std::bit_cast<std::uint64_t>(*value.as_signed()));
    case event_field_type::unsigned_integer:
        return writer.append_integer(*value.as_unsigned());
    case event_field_type::boolean:
        return writer.append_byte(
          static_cast<std::uint8_t>(*value.as_boolean()));
    case event_field_type::bounded_string:
        return writer.append_text(*value.as_text());
    case event_field_type::stable_id:
        return writer.append_integer(value.as_stable_id()->value());
    }
    return false;
}

[[nodiscard]] bool
append_field(event_encoding_writer& writer, const event_field& field) noexcept {
    const auto* descriptor = descriptor_for(field.key);
    if (descriptor == nullptr) {
        return false;
    }
    return writer.append_integer(static_cast<std::uint16_t>(field.key))
           && writer.append_byte(
             static_cast<std::uint8_t>(descriptor->name.size()))
           && writer.append_byte(static_cast<std::uint8_t>(field.value.type()))
           && writer.append_integer(
             static_cast<std::uint16_t>(field.value.encoded_payload_size()))
           && writer.append_text(descriptor->name)
           && append_field_value(writer, field.value);
}

[[nodiscard]] runtime::result<event_field_value> decode_field_value(
  event_encoding_reader& reader,
  event_field_key key,
  event_field_type type,
  std::uint16_t payload_size) noexcept {
    switch (type) {
    case event_field_type::signed_integer: {
        std::uint64_t value = 0;
        if (payload_size != sizeof(value) || !reader.read_integer(value)) {
            return runtime::failure(codec_error(errc::malformed_data));
        }
        return event_field_value::from_signed(
          std::bit_cast<std::int64_t>(value));
    }
    case event_field_type::unsigned_integer: {
        std::uint64_t value = 0;
        if (payload_size != sizeof(value) || !reader.read_integer(value)) {
            return runtime::failure(codec_error(errc::malformed_data));
        }
        return event_field_value::from_unsigned(value);
    }
    case event_field_type::boolean: {
        std::uint8_t value = 0;
        if (
          payload_size != sizeof(value) || !reader.read_integer(value)
          || value > 1U) {
            return runtime::failure(codec_error(errc::malformed_data));
        }
        return event_field_value::from_boolean(value != 0);
    }
    case event_field_type::bounded_string: {
        if (payload_size == 0 || payload_size > event_text_bytes_max) {
            return runtime::failure(codec_error(errc::malformed_data));
        }
        const auto text = reader.read_text(payload_size);
        if (!text) {
            return runtime::failure(codec_error(errc::malformed_data));
        }
        auto decoded = event_codec_access::decode_text(key, *text);
        if (!decoded) {
            return runtime::failure(codec_error(errc::malformed_data));
        }
        return event_field_value::from_text(std::move(*decoded));
    }
    case event_field_type::stable_id: {
        std::uint64_t value = 0;
        if (payload_size != sizeof(value) || !reader.read_integer(value)) {
            return runtime::failure(codec_error(errc::malformed_data));
        }
        auto stable = event_stable_id::make(value);
        if (!stable) {
            return runtime::failure(codec_error(errc::malformed_data));
        }
        return event_field_value::from_stable_id(*stable);
    }
    }
    return runtime::failure(codec_error(errc::malformed_data));
}

} // namespace

runtime::result<encoded_event> encode_event(const event& value) noexcept {
    encoded_event output;
    event_encoding_writer writer{output};
    const auto name = value.name();
    if (
      name.empty() || name.size() > std::numeric_limits<std::uint8_t>::max()
      || value.fields().size() > std::numeric_limits<std::uint8_t>::max()
      || !writer.append_integer(event_schema_version)
      || !writer.append_integer(static_cast<std::uint16_t>(value.kind()))
      || !writer.append_byte(static_cast<std::uint8_t>(name.size()))
      || !writer.append_text(name)
      || !writer.append_byte(static_cast<std::uint8_t>(value.severity()))
      || !writer.append_integer(value.monotonic().nanoseconds())
      || !writer.append_integer(
        std::bit_cast<std::uint64_t>(value.wall().unix_nanoseconds()))
      || !writer.append_integer(value.shard().value())
      || !writer.append_byte(static_cast<std::uint8_t>(value.workload()))
      || !writer.append_integer(value.sequence())
      || !writer.append_byte(
        static_cast<std::uint8_t>(value.fields().size()))) {
        return runtime::failure(codec_error(errc::invariant_violation));
    }
    for (const auto& field : value.fields()) {
        if (!append_field(writer, field)) {
            return runtime::failure(codec_error(errc::invariant_violation));
        }
    }
    if (output.size() != value.encoded_size()) {
        return runtime::failure(codec_error(errc::invariant_violation));
    }
    return output;
}

runtime::result<event>
decode_event(std::span<const std::uint8_t> encoded) noexcept {
    if (
      encoded.size() < canonical_event_fixed_encoded_size
      || encoded.size() > event_encoded_bytes_max) {
        return runtime::failure(codec_error(
          encoded.size() > event_encoded_bytes_max ? errc::out_of_range
                                                   : errc::malformed_data));
    }

    event_encoding_reader reader{encoded};
    std::uint32_t schema = 0;
    std::uint16_t kind_value = 0;
    std::uint8_t name_size = 0;
    if (
      !reader.read_integer(schema) || schema != event_schema_version
      || !reader.read_integer(kind_value) || !reader.read_integer(name_size)) {
        return runtime::failure(codec_error(errc::malformed_data));
    }
    const auto kind = static_cast<event_kind>(kind_value);
    const auto* event_descriptor = descriptor_for(kind);
    const auto name = reader.read_text(name_size);
    if (
      event_descriptor == nullptr || !name || *name != event_descriptor->name) {
        return runtime::failure(codec_error(errc::malformed_data));
    }

    std::uint8_t severity_value = 0;
    std::uint64_t monotonic = 0;
    std::uint64_t wall = 0;
    std::uint32_t shard = 0;
    std::uint8_t workload = 0;
    std::uint64_t sequence = 0;
    std::uint8_t field_count = 0;
    if (
      !reader.read_integer(severity_value) || !reader.read_integer(monotonic)
      || !reader.read_integer(wall) || !reader.read_integer(shard)
      || !reader.read_integer(workload) || !reader.read_integer(sequence)
      || !reader.read_integer(field_count) || field_count > event_fields_max) {
        return runtime::failure(codec_error(errc::malformed_data));
    }

    std::array<event_field, event_fields_max> fields{};
    std::uint16_t previous_key = 0;
    for (std::size_t index = 0; index < field_count; ++index) {
        std::uint16_t key_value = 0;
        std::uint8_t field_name_size = 0;
        std::uint8_t type_value = 0;
        std::uint16_t payload_size = 0;
        if (
          !reader.read_integer(key_value) || key_value <= previous_key
          || !reader.read_integer(field_name_size)
          || !reader.read_integer(type_value)
          || !reader.read_integer(payload_size)) {
            return runtime::failure(codec_error(errc::malformed_data));
        }
        previous_key = key_value;
        const auto key = static_cast<event_field_key>(key_value);
        const auto* field_descriptor = descriptor_for(key);
        const auto field_name = reader.read_text(field_name_size);
        const auto type = static_cast<event_field_type>(type_value);
        if (
          field_descriptor == nullptr || !field_name
          || *field_name != field_descriptor->name
          || type != field_descriptor->type
          || !event_field_is_allowed(kind, key)) {
            return runtime::failure(codec_error(errc::malformed_data));
        }
        auto value = decode_field_value(reader, key, type, payload_size);
        if (!value) {
            return runtime::failure(value.error());
        }
        fields[index] = event_field{.key = key, .value = std::move(*value)};
    }
    if (reader.remaining() != 0) {
        return runtime::failure(codec_error(errc::malformed_data));
    }

    const event_request_context context{
      .kind = kind,
      .severity = static_cast<event_severity>(severity_value),
      .monotonic = runtime::monotonic_time{monotonic},
      .wall = runtime::wall_time{std::bit_cast<std::int64_t>(wall)},
      .workload = static_cast<resource::workload_class>(workload),
    };
    auto decoded = event_codec_access::make_event(
      context,
      shard,
      sequence,
      std::span<const event_field>{fields.data(), field_count});
    if (!decoded || decoded->encoded_size() != encoded.size()) {
        return runtime::failure(codec_error(errc::malformed_data));
    }
    return decoded;
}

} // namespace kwaque::observability
