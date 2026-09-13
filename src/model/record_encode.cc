#include "src/model/record_encode.h"

#include "src/base/invariant.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/model/record_codec.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <optional>
#include <utility>

namespace kwaque::model {
namespace {

using bytes::fragmented_buffer;
using bytes::fragmented_buffer_builder;

codec::field_context
field(codec::field_context context, record_field name) noexcept {
    context.field = static_cast<std::uint16_t>(name);
    return context;
}

codec::error at(errc reason, codec::field_context context) noexcept {
    return codec::error{reason, context.family, context.field, context.origin};
}

codec::result<byte_count>
multiply(byte_count bytes, std::uint64_t count, codec::field_context context) {
    if (
      count != 0
      && bytes.value() > std::numeric_limits<std::uint64_t>::max() / count) {
        return codec::failure(at(errc::out_of_range, context));
    }
    return byte_count{bytes.value() * count};
}

struct copy_shape final {
    byte_count fragment_bytes;
    item_count fragments;
    byte_count requested_backing;
};

// Uniform bounded tails make the exact worst-case allocation count independent
// of how fields are split. A fresh descriptor reservation avoids growth during
// emission; publication transfers that same storage without an allocation.
seastar::future<codec::result<copy_shape>> choose_shape(
  byte_count total,
  codec::cooperative_work& work,
  byte_count remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context context) {
    const auto policy = work.policy();
    const auto config = policy.config();
    auto width = std::min(
      {total.value(),
       std::uint64_t{65536},
       config.max_allocation_bytes.value(),
       remaining.value()});
    while (width != 0) {
        const auto anchor = at(errc::success, context);
        if (
          auto ready = co_await work.admit(byte_count{}, item_count{8}, anchor);
          !ready) {
            co_return codec::failure(ready.error());
        }
        if (auto ready = work.poll(anchor); !ready) {
            co_return codec::failure(ready.error());
        }
        const auto count = 1U + (total.value() - 1U) / width;
        if (count > config.max_buffer_fragments.value()) {
            break; // Smaller tails can only increase the count.
        }
        const byte_count request{
          count * fragmented_buffer::fragment_descriptor_size()};
        const auto descriptors = charge(request);
        const auto tail = charge(byte_count{width});
        if (descriptors < request || tail < byte_count{width}) {
            co_return codec::failure(at(errc::invalid_argument, context));
        }
        const auto backing = multiply(tail, count, context);
        if (!backing) co_return codec::failure(backing.error());
        const auto available = policy.remaining_operation_bytes(
          {.staged_output = *backing, .payload_bookkeeping = descriptors},
          remaining);
        if (!available && available.error() != errc::resource_exhausted) {
            co_return codec::failure(
              codec::detail::allocation_cost_error(
                available.error(), context, context.origin));
        }
        if (
          tail <= config.max_allocation_bytes
          && descriptors <= config.max_allocation_bytes
          && *backing <= config.max_retained_bytes && available) {
            co_return copy_shape{
              byte_count{width}, item_count{count}, byte_count{width * count}};
        }
        width /= 2U;
    }
    co_return codec::failure(at(errc::resource_exhausted, context));
}

seastar::future<codec::result<void>> append_payload(
  const fragmented_buffer& input,
  fragmented_buffer_builder& output,
  codec::cooperative_work& work,
  codec::field_context context) {
    const auto anchor = at(errc::success, context);
    if (
      auto ready = co_await work.admit(byte_count{}, item_count{1}, anchor);
      !ready) {
        co_return codec::failure(ready.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    for (const auto fragment : input) {
        for (std::size_t offset = 0; offset != fragment.size();) {
            const auto size = std::min(
              fragment.size() - offset,
              static_cast<std::size_t>(work.byte_quantum().value() / 2U));
            if (
              auto ready = co_await work.admit(
                byte_count{2U * size}, item_count{8}, anchor);
              !ready) {
                co_return codec::failure(ready.error());
            }
            if (auto ready = work.poll(anchor); !ready) {
                co_return codec::failure(ready.error());
            }
            const auto appended = output.append(
              std::span<const char>{fragment.data() + offset, size});
            if (!appended) {
                co_return codec::failure(
                  codec::detail::allocation_cost_error(
                    appended.error(),
                    context,
                    context.origin + output.size().value()));
            }
            offset += size;
        }
    }
    co_return codec::result<void>{};
}

std::optional<byte_count>
length(const std::optional<fragmented_buffer>& value) {
    return value ? std::optional{value->size()} : std::nullopt;
}

seastar::future<codec::result<fragmented_buffer>> encode_into(
  const record& value,
  std::optional<fragmented_buffer_builder>& output,
  codec::cooperative_work& work,
  byte_count remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context context) {
    const auto anchor = at(errc::success, context);
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (charge == nullptr)
        co_return codec::failure(at(errc::invalid_argument, context));
    if (work.byte_quantum().value() < 128 || work.item_quantum().value() < 64) {
        co_return codec::failure(at(errc::resource_exhausted, context));
    }
    const auto size = co_await record_encoded_size_cooperatively(
      value, work, anchor);
    if (!size) co_return codec::failure(size.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (
      size->encoded_bytes.value()
      > std::numeric_limits<std::uint64_t>::max() - context.origin) {
        co_return codec::failure(at(errc::invalid_argument, context));
    }
    const auto shape = co_await choose_shape(
      size->encoded_bytes, work, remaining, charge, context);
    if (!shape) co_return codec::failure(shape.error());
    if (auto ready = co_await work.checkpoint(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    bytes::fragmented_buffer_builder_config config;
    config.initial_fragment_bytes = shape->fragment_bytes;
    config.max_fragment_bytes = shape->fragment_bytes;
    config.max_total_bytes = size->encoded_bytes;
    config.max_retained_bytes = shape->requested_backing;
    config.max_fragments = shape->fragments.value();
    output.emplace(config);
    const auto reserved = output->reserve_fragments(shape->fragments);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RECORD-ENCODE-RESERVE"},
      reserved.has_value(),
      "admitted record descriptor reservation failed");
    const auto appended = co_await detail::append_record(
      value.fields(), value, *size, *output, work, context);
    if (!appended) co_return codec::failure(appended.error());
    if (auto ready = co_await work.checkpoint(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RECORD-ENCODE-SIZE"},
      output->size() == size->encoded_bytes,
      "record emission differs from checked accounting");
    auto published = output->finish();
    if (!published) {
        co_return codec::failure(
          codec::detail::allocation_cost_error(
            published.error(), context, context.origin));
    }
    co_return std::move(*published);
}

} // namespace

seastar::future<codec::result<void>> detail::append_record(
  record_fields fields,
  const record& value,
  record_sizes size,
  fragmented_buffer_builder& output,
  codec::cooperative_work& work,
  codec::field_context context) {
    const auto anchor = at(errc::success, context);
    const auto policy = work.policy();
    if (
      auto ready = co_await work.admit(byte_count{128}, item_count{64}, anchor);
      !ready) {
        co_return codec::failure(ready.error());
    }
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    // The fixed group preserves the wire's field order. Sizes were checked
    // before narrowing; the primitives retain canonical varint encodings.
    if (
      auto written = codec::write_varuint(
        output, static_cast<std::uint32_t>(size.body_bytes.value()), context);
      !written) {
        co_return codec::failure(written.error());
    }
    if (
      auto written = codec::write_le(
        output, fields.attributes, field(context, record_field::attributes));
      !written) {
        co_return codec::failure(written.error());
    }
    if (
      auto written = codec::write_varint(
        output,
        fields.timestamp_delta,
        field(context, record_field::timestamp_delta));
      !written) {
        co_return codec::failure(written.error());
    }
    if (
      auto written = codec::write_varuint(
        output,
        fields.logical_delta.value(),
        field(context, record_field::logical_delta));
      !written) {
        co_return codec::failure(written.error());
    }
    if (
      auto written = codec::write_nullable_length(
        output,
        length(value.key()),
        policy.config().max_record_bytes,
        field(context, record_field::key_length));
      !written) {
        co_return codec::failure(written.error());
    }
    if (value.key()) {
        if (
          auto written = co_await append_payload(
            *value.key(), output, work, field(context, record_field::key));
          !written) {
            co_return codec::failure(written.error());
        }
    }
    if (
      auto ready = co_await work.admit(byte_count{20}, item_count{10}, anchor);
      !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (
      auto written = codec::write_nullable_length(
        output,
        length(value.value()),
        policy.config().max_record_bytes,
        field(context, record_field::value_length));
      !written) {
        co_return codec::failure(written.error());
    }
    if (value.value()) {
        if (
          auto written = co_await append_payload(
            *value.value(), output, work, field(context, record_field::value));
          !written) {
            co_return codec::failure(written.error());
        }
    }
    if (
      auto ready = co_await work.admit(byte_count{20}, item_count{10}, anchor);
      !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (
      auto written = codec::write_varuint(
        output,
        static_cast<std::uint32_t>(value.headers().size()),
        field(context, record_field::header_count));
      !written) {
        co_return codec::failure(written.error());
    }
    for (const auto& header : value.headers()) {
        if (
          auto ready = co_await work.admit(
            byte_count{20}, item_count{10}, anchor);
          !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        if (
          auto written = codec::write_varuint(
            output,
            static_cast<std::uint32_t>(header.name().size().value()),
            field(context, record_field::header_name_length));
          !written) {
            co_return codec::failure(written.error());
        }
        if (
          auto written = co_await append_payload(
            header.name(),
            output,
            work,
            field(context, record_field::header_name));
          !written) {
            co_return codec::failure(written.error());
        }
        if (
          auto ready = co_await work.admit(
            byte_count{20}, item_count{10}, anchor);
          !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        if (
          auto written = codec::write_nullable_length(
            output,
            length(header.value()),
            policy.config().max_record_header_bytes,
            field(context, record_field::header_value_length));
          !written) {
            co_return codec::failure(written.error());
        }
        if (header.value()) {
            if (
              auto written = co_await append_payload(
                *header.value(),
                output,
                work,
                field(context, record_field::header_value));
              !written) {
                co_return codec::failure(written.error());
            }
        }
    }
    co_return codec::result<void>{};
}

seastar::future<codec::result<bytes::fragmented_buffer>> encode_record(
  const record& value,
  codec::cooperative_work& work,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context context) {
    context = field(context, record_field::body_bytes);
    std::optional<fragmented_buffer_builder> output;
    std::optional<codec::result<fragmented_buffer>> produced;
    std::exception_ptr exception;
    try {
        produced.emplace(
          co_await encode_into(
            value, output, work, parent_remaining, charge, context));
    } catch (...) {
        exception = std::current_exception();
    }
    if (output) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        output.reset();
    }
    if (exception) std::rethrow_exception(exception);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RECORD-ENCODE-OUTCOME"},
      produced.has_value(),
      "record encoder completed without an outcome");
    if (!produced->has_value()) co_return codec::failure(produced->error());
    if (auto ready = work.poll(at(errc::success, context)); !ready) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        produced.reset();
        co_return codec::failure(ready.error());
    }
    co_return std::move(**produced);
}

} // namespace kwaque::model
