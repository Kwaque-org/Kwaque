#include "src/codec/integer.h"
#include "src/model/tests/model_bench_reference.h"

#include <algorithm>
#include <limits>

namespace kwaque::model::bench {
namespace {
using bytes::fragmented_buffer;
using nullable_bytes = std::optional<fragmented_buffer>;

constexpr std::uint64_t unsigned_size(std::uint64_t value) noexcept {
    std::uint64_t width = 1;
    while (value >= 128) {
        value /= 128;
        ++width;
    }
    return width;
}

result<void> add(byte_count& target, byte_count amount) noexcept {
    const auto sum = target.checked_add(amount);
    if (!sum) {
        return failure(errc::out_of_range);
    }
    target = *sum;
    return {};
}

byte_count visible_size(const nullable_bytes& value) noexcept {
    return value ? value->size() : byte_count{};
}

result<byte_count> nullable_encoded_size(const nullable_bytes& value) {
    const auto size = visible_size(value);
    if (size.value() > std::numeric_limits<std::int32_t>::max()) {
        return failure(errc::out_of_range);
    }
    const auto length = value ? static_cast<std::int32_t>(size.value()) : -1;
    return byte_count{
      unsigned_size(codec::encode_zigzag(length)) + size.value()};
}

result<byte_count> header_payload_size(
  const fragmented_buffer& name,
  const nullable_bytes& value,
  const codec::limits& policy) {
    if (name.size() > policy.config().max_header_name_bytes) {
        return failure(errc::resource_exhausted);
    }
    const auto total = name.size().checked_add(visible_size(value));
    if (!total) {
        return failure(errc::out_of_range);
    }
    if (*total > policy.config().max_record_header_bytes) {
        return failure(errc::resource_exhausted);
    }
    return *total;
}

result<record_sizes> initial_size(
  record_fields fields,
  const nullable_bytes& key,
  const nullable_bytes& value,
  std::size_t headers,
  const codec::limits& policy) {
    if (fields.attributes != 0) {
        return failure(errc::unsupported_format);
    }
    if (headers > policy.config().max_record_headers.value()) {
        return failure(errc::resource_exhausted);
    }
    const auto key_bytes = nullable_encoded_size(key);
    if (!key_bytes) {
        return failure(key_bytes.error());
    }
    const auto value_bytes = nullable_encoded_size(value);
    if (!value_bytes) {
        return failure(value_bytes.error());
    }
    record_sizes size{
      .body_bytes = byte_count{
        1U + unsigned_size(codec::encode_zigzag(fields.timestamp_delta))
        + unsigned_size(fields.logical_delta.value())
        + unsigned_size(headers)}};
    for (const auto amount : {*key_bytes, *value_bytes}) {
        if (auto added = add(size.body_bytes, amount); !added) {
            return failure(added.error());
        }
    }
    return size;
}

result<void> add_header_size(
  record_sizes& size,
  const record_header& header,
  const codec::limits& policy) {
    const auto payload = header_payload_size(
      header.name(), header.value(), policy);
    if (!payload) {
        return failure(payload.error());
    }
    if (auto added = add(size.header_payload_bytes, *payload); !added) {
        return added;
    }
    if (size.header_payload_bytes > policy.config().max_record_header_bytes) {
        return failure(errc::resource_exhausted);
    }
    const auto value = nullable_encoded_size(header.value());
    if (!value) {
        return failure(value.error());
    }
    // Name length is unsigned, whereas only the value length uses ZigZag.
    for (const auto amount :
         {byte_count{unsigned_size(header.name().size().value())},
          header.name().size(),
          *value}) {
        if (auto added = add(size.body_bytes, amount); !added) {
            return added;
        }
    }
    return {};
}

result<record_sizes>
finish_size(record_sizes size, const codec::limits& policy) {
    if (size.body_bytes.value() > std::numeric_limits<std::uint32_t>::max()) {
        return failure(errc::out_of_range);
    }
    const auto total = size.body_bytes.checked_add(
      byte_count{unsigned_size(size.body_bytes.value())});
    if (!total) {
        return failure(errc::out_of_range);
    }
    if (*total > policy.config().max_record_bytes) {
        return failure(errc::resource_exhausted);
    }
    size.encoded_bytes = *total;
    return size;
}

} // namespace
result<record_sizes>
checked_size(const record& value, const codec::limits& policy) {
    auto size = initial_size(
      value.fields(),
      value.key(),
      value.value(),
      value.headers().size(),
      policy);
    if (!size) return size;
    for (const auto& header : value.headers()) {
        if (auto added = add_header_size(*size, header, policy); !added)
            return failure(added.error());
    }
    return finish_size(*size, policy);
}
result<record_sizes>
codec_size(const record& value, const codec::limits& policy) {
    return record_encoded_size(value, policy);
}
} // namespace kwaque::model::bench
