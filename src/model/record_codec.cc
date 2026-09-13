#include "src/model/record_codec.h"

#include "src/base/invariant.h"
#include "src/codec/integer.h"
#include "src/model/record_encode.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <system_error>

namespace kwaque::model {
namespace {

using bytes::fragmented_buffer;
using nullable_bytes = std::optional<fragmented_buffer>;

constexpr std::uint64_t unsigned_size(std::uint64_t value) noexcept {
    std::uint64_t width = 1;
    while (value >= 128) {
        value >>= 7;
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

std::size_t fragment_count(const nullable_bytes& value) noexcept {
    return value ? value->fragment_count() : 0;
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

[[gnu::always_inline]] inline result<record_sizes> initial_size(
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

[[gnu::always_inline]] inline result<void> add_header_size(
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

// Fold the bounded metadata walk into either public entry point. Keep the
// named steps shared with cooperative sizing and its separate work admission.
[[gnu::always_inline]] inline result<record_sizes> calculate_size(
  record_fields fields,
  const nullable_bytes& key,
  const nullable_bytes& value,
  std::span<const record_header> headers,
  const codec::limits& policy) {
    auto size = initial_size(fields, key, value, headers.size(), policy);
    if (!size) {
        return size;
    }
    for (const auto& header : headers) {
        if (auto added = add_header_size(*size, header, policy); !added) {
            return failure(added.error());
        }
    }
    return finish_size(*size, policy);
}

// A field absent from the record owns nothing. Present-empty buffers are still
// visited, since they can retain descriptor capacity after prior operations.
const fragmented_buffer* field_buffer(const record& value, std::size_t index) {
    if (index == 0) {
        return value.key() ? std::addressof(*value.key()) : nullptr;
    }
    if (index == 1) {
        return value.value() ? std::addressof(*value.value()) : nullptr;
    }
    const auto& header = value.headers()[(index - 2U) / 2U];
    if (index % 2U == 0) {
        return std::addressof(header.name());
    }
    return header.value() ? std::addressof(*header.value()) : nullptr;
}

codec::error at(errc code, codec::error anchor) noexcept {
    return codec::error{
      code, anchor.family(), anchor.field(), anchor.byte_offset()};
}

codec::error at(std::error_code code, codec::error anchor) noexcept {
    for (const auto reason :
         {errc::invalid_argument,
          errc::out_of_range,
          errc::resource_exhausted,
          errc::unsupported_format}) {
        if (code == reason) {
            return at(reason, anchor);
        }
    }
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RECORD-ERROR"},
      false,
      "record accounting returned an unexpected error");
}

result<void> add_memory(
  record_memory_usage& total, const bytes::buffer_allocation_cost& part) {
    if (auto added = add(total.backing, part.backing); !added) {
        return added;
    }
    for (const auto amount : {part.descriptors, part.share_controls}) {
        if (auto added = add(total.metadata, amount); !added) {
            return added;
        }
    }
    if (!total.backing.checked_add(total.metadata)) {
        return failure(errc::out_of_range);
    }
    const auto fragments = total.fragments.checked_add(part.fragments);
    if (!fragments) {
        return failure(errc::out_of_range);
    }
    total.fragments = *fragments;
    total.largest_allocation = std::max(
      total.largest_allocation, part.largest_allocation);
    return {};
}

// Preserve the native allocation-cost range sequence: the first range charges
// descriptor history once; each range charges backing and possible promotion.
seastar::future<codec::result<void>> add_buffer_cost(
  record_memory_usage& total,
  const fragmented_buffer& input,
  codec::cooperative_work& work,
  bytes::allocation_charge_fn charge,
  codec::error anchor) {
    const auto quantum = work.item_quantum().value();
    if (!input.empty() && quantum < 6) {
        co_return codec::failure(at(errc::resource_exhausted, anchor));
    }
    const auto batch = quantum < 6 ? 1U : (quantum - 2U) / 4U;
    std::size_t first = 0;
    do {
        const auto count = std::min<std::size_t>(
          input.fragment_count() - first, batch);
        if (
          auto ready = co_await work.admit(
            byte_count{},
            item_count{count == 0 ? 1U : 4U * count + 2U},
            anchor);
          !ready) {
            co_return codec::failure(ready.error());
        }
        if (auto ready = work.poll(anchor); !ready) {
            co_return codec::failure(ready.error());
        }
        const auto part = input.allocation_cost(first, count, charge);
        if (!part) {
            co_return codec::failure(at(part.error(), anchor));
        }
        if (auto added = add_memory(total, *part); !added) {
            co_return codec::failure(at(added.error(), anchor));
        }
        first += count;
    } while (first != input.fragment_count());
    co_return codec::result<void>{};
}

seastar::future<codec::result<bool>> buffers_equal(
  const fragmented_buffer* left,
  const fragmented_buffer* right,
  codec::cooperative_work& work,
  codec::error anchor) {
    if (
      auto ready = co_await work.admit(byte_count{}, item_count{1}, anchor);
      !ready) {
        co_return codec::failure(ready.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    if (left == nullptr || right == nullptr) {
        co_return left == right;
    }
    if (left->size() != right->size()) {
        co_return false;
    }
    std::size_t left_index = 0;
    std::size_t right_index = 0;
    std::size_t left_offset = 0;
    std::size_t right_offset = 0;
    while (left_index < left->fragment_count()
           && right_index < right->fragment_count()) {
        const auto l = left->fragment_at(left_index).value();
        const auto r = right->fragment_at(right_index).value();
        const auto size = std::min(
          {l.size() - left_offset,
           r.size() - right_offset,
           static_cast<std::size_t>(work.byte_quantum().value() / 2U)});
        if (size == 0) {
            co_return codec::failure(at(errc::resource_exhausted, anchor));
        }
        if (
          auto ready = co_await work.admit(
            byte_count{2U * size}, item_count{2}, anchor);
          !ready) {
            co_return codec::failure(ready.error());
        }
        if (auto ready = work.poll(anchor); !ready) {
            co_return codec::failure(ready.error());
        }
        if (
          std::memcmp(l.data() + left_offset, r.data() + right_offset, size)
          != 0) {
            co_return false;
        }
        left_offset += size;
        right_offset += size;
        if (left_offset == l.size()) {
            ++left_index;
            left_offset = 0;
        }
        if (right_offset == r.size()) {
            ++right_index;
            right_offset = 0;
        }
    }
    co_return true;
}

} // namespace

result<record_sizes> record_encoded_size(
  record_fields fields,
  const nullable_bytes& key,
  const nullable_bytes& value,
  std::span<const record_header> headers,
  const codec::limits& policy) {
    return calculate_size(fields, key, value, headers, policy);
}

result<record_sizes>
record_encoded_size(const record& value, const codec::limits& policy) {
    return calculate_size(
      value.fields(), value.key(), value.value(), value.headers(), policy);
}

result<record_header> make_record_header(
  fragmented_buffer&& name,
  nullable_bytes&& value,
  const codec::limits& policy) {
    if (value && std::addressof(name) == std::addressof(*value)) {
        return failure(errc::invalid_argument);
    }
    if (auto valid = header_payload_size(name, value, policy); !valid) {
        return failure(valid.error());
    }
    if (
      name.fragment_count() + fragment_count(value)
      > policy.config().max_buffer_fragments.value()) {
        return failure(errc::resource_exhausted);
    }
    return record_header{std::move(name), std::move(value)};
}

result<record> make_record(
  record_fields fields,
  nullable_bytes&& key,
  nullable_bytes&& value,
  std::vector<record_header>&& headers,
  const codec::limits& policy) {
    if (std::addressof(key) == std::addressof(value)) {
        return failure(errc::invalid_argument);
    }
    if (
      auto valid = record_encoded_size(fields, key, value, headers, policy);
      !valid) {
        return failure(valid.error());
    }
    // Each buffer has the substrate's fixed fragment ceiling and there are at
    // most 64 headers. The sum fits size_t and bounds the complete owner too.
    auto fragments = fragment_count(key) + fragment_count(value);
    for (const auto& header : headers) {
        fragments += header.name().fragment_count()
                     + fragment_count(header.value());
    }
    if (fragments > policy.config().max_buffer_fragments.value()) {
        return failure(errc::resource_exhausted);
    }
    return record{fields, std::move(key), std::move(value), std::move(headers)};
}

seastar::future<codec::result<record_sizes>> detail::record_size_with_fields(
  record_fields fields,
  const record& value,
  codec::cooperative_work& work,
  codec::error anchor) {
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    if (
      auto ready = co_await work.admit(byte_count{}, item_count{16}, anchor);
      !ready) {
        co_return codec::failure(ready.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    const auto policy = work.policy();
    auto size = initial_size(
      fields, value.key(), value.value(), value.headers().size(), policy);
    if (!size) {
        co_return codec::failure(at(size.error(), anchor));
    }
    for (const auto& header : value.headers()) {
        if (
          auto ready = co_await work.admit(
            byte_count{}, item_count{16}, anchor);
          !ready) {
            co_return codec::failure(ready.error());
        }
        if (auto ready = work.poll(anchor); !ready) {
            co_return codec::failure(ready.error());
        }
        if (auto added = add_header_size(*size, header, policy); !added) {
            co_return codec::failure(at(added.error(), anchor));
        }
    }
    const auto valid = finish_size(*size, policy);
    if (!valid) {
        co_return codec::failure(at(valid.error(), anchor));
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    co_return *valid;
}

seastar::future<codec::result<record_sizes>> record_encoded_size_cooperatively(
  const record& value, codec::cooperative_work& work, codec::error anchor) {
    return detail::record_size_with_fields(value.fields(), value, work, anchor);
}

seastar::future<codec::result<record_memory_usage>> record_allocation_cost(
  const record& value,
  codec::cooperative_work& work,
  bytes::allocation_charge_fn charge,
  codec::error anchor) {
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    if (charge == nullptr) {
        co_return codec::failure(at(errc::invalid_argument, anchor));
    }
    if (
      auto size = co_await record_encoded_size_cooperatively(
        value, work, anchor);
      !size) {
        co_return codec::failure(size.error());
    }
    if (
      auto ready = co_await work.admit(byte_count{}, item_count{8}, anchor);
      !ready) {
        co_return codec::failure(ready.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    record_memory_usage total;
    if (
      value.header_capacity().value()
      > std::numeric_limits<std::uint64_t>::max() / sizeof(record_header)) {
        co_return codec::failure(at(errc::out_of_range, anchor));
    }
    const byte_count header_storage{
      value.header_capacity().value() * sizeof(record_header)};
    if (header_storage.value() != 0) {
        total.metadata = charge(header_storage);
        if (total.metadata < header_storage) {
            co_return codec::failure(at(errc::invalid_argument, anchor));
        }
        total.largest_allocation = total.metadata;
    }
    for (std::size_t index = 0; index < 2U + 2U * value.headers().size();
         ++index) {
        if (
          auto ready = co_await work.admit(byte_count{}, item_count{1}, anchor);
          !ready) {
            co_return codec::failure(ready.error());
        }
        if (auto ready = work.poll(anchor); !ready) {
            co_return codec::failure(ready.error());
        }
        if (const auto* input = field_buffer(value, index)) {
            if (
              auto added = co_await add_buffer_cost(
                total, *input, work, charge, anchor);
              !added) {
                co_return codec::failure(added.error());
            }
        }
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    co_return total;
}

seastar::future<codec::result<codec::decode_budget>> reserve_record_input(
  const record& value,
  codec::cooperative_work& work,
  codec::decode_budget memory,
  codec::error anchor) {
    const auto cost = co_await record_allocation_cost(
      value, work, memory.charge, anchor);
    if (!cost) {
        co_return codec::failure(cost.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    const auto policy = work.policy();
    if (
      cost->backing > policy.config().max_retained_bytes
      || cost->fragments > policy.config().max_buffer_fragments
      || !policy.validate_allocation(cost->largest_allocation)) {
        co_return codec::failure(at(errc::resource_exhausted, anchor));
    }
    co_return codec::detail::consume_decode_budget(
      policy,
      memory,
      cost->backing,
      cost->metadata,
      codec::field_context{
        .origin = anchor.byte_offset(),
        .family = anchor.family(),
        .field = anchor.field()},
      anchor.byte_offset());
}

seastar::future<codec::result<bool>> records_equal(
  const record& left,
  const record& right,
  codec::cooperative_work& work,
  codec::error anchor) {
    if (
      auto ready = co_await work.admit(byte_count{}, item_count{8}, anchor);
      !ready) {
        co_return codec::failure(ready.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    if (
      left.fields() != right.fields()
      || left.headers().size() != right.headers().size()) {
        co_return false;
    }
    for (std::size_t index = 0; index < 2U + 2U * left.headers().size();
         ++index) {
        const auto equal = co_await buffers_equal(
          field_buffer(left, index), field_buffer(right, index), work, anchor);
        if (!equal) {
            co_return codec::failure(equal.error());
        }
        if (auto ready = work.poll(anchor); !ready) {
            co_return codec::failure(ready.error());
        }
        if (!*equal) {
            co_return false;
        }
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    co_return true;
}

} // namespace kwaque::model
