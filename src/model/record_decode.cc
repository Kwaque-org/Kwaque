#include "src/base/invariant.h"
#include "src/model/record_codec.h"
#include "src/model/record_scan.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

namespace kwaque::model {
namespace {

using bytes::fragmented_buffer;
using bytes::fragmented_buffer_parser;

codec::field_context
field(codec::field_context context, record_field name) noexcept {
    context.field = static_cast<std::uint16_t>(name);
    return context;
}

codec::error
at(errc code, codec::field_context context, std::uint64_t offset) noexcept {
    return codec::error{code, context.family, context.field, offset};
}

// Parent backing/promotion is already reserved. This adds only descriptors for
// one exact alias; the same admission sequence is used by synchronous children.
codec::result<bytes::buffer_allocation_cost> slice_cost(
  const fragmented_buffer_parser& input,
  byte_count length,
  const codec::limits& policy,
  codec::decode_budget memory,
  codec::field_context context) {
    const auto start = context.origin + input.bytes_consumed().value();
    const auto cost = input.next_buffer_allocation_cost(length, memory.charge);
    if (!cost) {
        return codec::failure(
          codec::detail::allocation_cost_error(cost.error(), context, start));
    }
    if (
      auto valid = codec::detail::validate_decode_cost(
        length, length, *cost, policy, context, start);
      !valid) {
        return codec::failure(valid.error());
    }
    return *cost;
}

} // namespace

namespace detail {

// One concrete decode owner. Its private model construction follows complete
// field/extent validation; no public unchecked constructor or decoder registry
// is exposed. Partial field owners remain here so the outer transaction can
// drain them in its own frame even after allocation failure.
template<bool Scan>
class record_decoder final {
public:
    using field_type
      = std::conditional_t<Scan, record_byte_range, fragmented_buffer>;
    record_fields fields;
    std::conditional_t<Scan, record_layout, std::nullptr_t> layout{};
    byte_count body_offset;
    std::optional<field_type> key;
    std::optional<field_type> value;
    std::vector<record_header> headers;
    field_type name;
    std::optional<field_type> header_value;
    byte_count output_metadata;

    // Transfer the result fields, keeping this owner available for residual
    // accounting and cleanup of any remaining temporary fields.
    [[nodiscard]] auto release_result() & noexcept {
        if constexpr (Scan) {
            layout.fields = fields;
            layout.key = key;
            layout.value = value;
            return std::move(layout);
        } else {
            return record{
              fields, std::move(key), std::move(value), std::move(headers)};
        }
    }

    seastar::future<codec::result<void>> read(
      fragmented_buffer_parser& input,
      record_decode_context expected,
      codec::decode_budget remaining,
      codec::cooperative_work& work,
      codec::field_context context) {
        memory_ = remaining;
        const auto policy = work.policy();
        const auto anchor = at(errc::success, context, context.origin);
        if (
          auto ready = co_await work.admit(
            byte_count{128}, item_count{64}, anchor);
          !ready) {
            co_return codec::failure(ready.error());
        }
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        auto coordinates = field(context, record_field::attributes);
        const auto attributes = codec::read_le<std::uint8_t>(
          input, coordinates, codec::input_boundary::complete);
        if (!attributes) co_return codec::failure(attributes.error());
        if (*attributes != 0)
            co_return codec::failure(
              at(errc::unsupported_format, coordinates, context.origin));
        fields.attributes = *attributes;
        coordinates = field(context, record_field::timestamp_delta);
        const auto timestamp_at = context.origin
                                  + input.bytes_consumed().value();
        const auto timestamp = codec::read_varint<std::int64_t>(
          input, coordinates, codec::input_boundary::complete);
        if (!timestamp) co_return codec::failure(timestamp.error());
        if (!checked_timestamp_from_delta(
              expected.timestamp_base, *timestamp)) {
            co_return codec::failure(
              at(errc::malformed_data, coordinates, timestamp_at));
        }
        fields.timestamp_delta = *timestamp;
        coordinates = field(context, record_field::logical_delta);
        const auto logical_at = context.origin + input.bytes_consumed().value();
        const auto logical = codec::read_varuint<std::uint64_t>(
          input, coordinates, codec::input_boundary::complete);
        if (!logical) co_return codec::failure(logical.error());
        if (*logical >= expected.original_count.value()) {
            co_return codec::failure(
              at(errc::malformed_data, coordinates, logical_at));
        }
        fields.logical_delta = range_logical_count{*logical};

        if (
          auto read = co_await read_nullable(
            input,
            key,
            policy.config().max_record_bytes,
            record_field::key_length,
            record_field::key,
            work,
            context);
          !read) {
            co_return codec::failure(read.error());
        }
        if (
          auto read = co_await read_nullable(
            input,
            value,
            policy.config().max_record_bytes,
            record_field::value_length,
            record_field::value,
            work,
            context);
          !read) {
            co_return codec::failure(read.error());
        }
        coordinates = field(context, record_field::header_count);
        if (
          auto ready = co_await work.admit(
            byte_count{20}, item_count{10}, anchor);
          !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        const auto count_at = context.origin + input.bytes_consumed().value();
        const auto count = codec::read_varuint<std::uint32_t>(
          input, coordinates, codec::input_boundary::complete);
        if (!count) co_return codec::failure(count.error());
        const auto cap = std::min(
          {policy.config().max_record_headers,
           policy.config().max_batch_headers,
           expected.headers_remaining});
        if (*count > cap.value())
            co_return codec::failure(
              at(errc::resource_exhausted, coordinates, count_at));
        // Even empty names with null values require two framing bytes. This is
        // checked before allocating the one exact header descriptor array.
        if (*count > input.bytes_remaining().value() / 2U) {
            co_return codec::failure(at(
              errc::malformed_data,
              coordinates,
              context.origin + input.total_bytes().value()));
        }
        if constexpr (!Scan) {
            if (*count != 0) {
                const byte_count request{
                  static_cast<std::uint64_t>(*count) * sizeof(record_header)};
                const auto served = memory_.charge(request);
                if (served < request)
                    co_return codec::failure(
                      at(errc::invalid_argument, coordinates, count_at));
                if (!policy.validate_allocation(served))
                    co_return codec::failure(
                      at(errc::resource_exhausted, coordinates, count_at));
                if (
                  auto admitted = charge_metadata(
                    served, policy, coordinates, count_at);
                  !admitted) {
                    co_return codec::failure(admitted.error());
                }
                if (auto ready = work.poll(anchor); !ready)
                    co_return codec::failure(ready.error());
                headers.reserve(*count);
                KWAQUE_INVARIANT(
                  invariant_id{"KQ-RECORD-HEADER-CAPACITY"},
                  headers.capacity() <= served.value() / sizeof(record_header),
                  "header allocation exceeded its admitted served capacity");
            }
        }
        auto header_remaining = policy.config().max_record_header_bytes;
        for (std::uint32_t index = 0; index < *count; ++index) {
            coordinates = field(context, record_field::header_name_length);
            if (
              auto ready = co_await work.admit(
                byte_count{20}, item_count{10}, anchor);
              !ready)
                co_return codec::failure(ready.error());
            if (auto ready = work.poll(anchor); !ready)
                co_return codec::failure(ready.error());
            const auto name_at = context.origin
                                 + input.bytes_consumed().value();
            const auto length = codec::read_varuint<std::uint32_t>(
              input, coordinates, codec::input_boundary::complete);
            if (!length) co_return codec::failure(length.error());
            if (
              *length
              > std::min(
                  policy.config().max_header_name_bytes, header_remaining)
                  .value()) {
                co_return codec::failure(
                  at(errc::resource_exhausted, coordinates, name_at));
            }
            if (*length > input.bytes_remaining().value()) {
                co_return codec::failure(at(
                  errc::malformed_data,
                  field(context, record_field::header_name),
                  context.origin + input.total_bytes().value()));
            }
            if (
              auto read = co_await materialize(
                input,
                name,
                byte_count{*length},
                work,
                field(context, record_field::header_name));
              !read) {
                co_return codec::failure(read.error());
            }
            if (auto ready = work.poll(anchor); !ready)
                co_return codec::failure(ready.error());
            header_remaining = *header_remaining.checked_sub(
              byte_count{*length});
            if (
              auto read = co_await read_nullable(
                input,
                header_value,
                header_remaining,
                record_field::header_value_length,
                record_field::header_value,
                work,
                context);
              !read) {
                co_return codec::failure(read.error());
            }
            if (auto ready = work.poll(anchor); !ready)
                co_return codec::failure(ready.error());
            if (header_value)
                header_remaining = *header_remaining.checked_sub(
                  header_value->size());
            // Capacity was reserved before the loop; header names/values and
            // their combined fragment count have already been validated.
            if constexpr (Scan) {
                layout.header_storage[index] = record_header_range{
                  name, header_value};
                ++layout.header_count;
            } else {
                headers.push_back(
                  record_header{std::move(name), std::move(header_value)});
            }
            header_value.reset();
        }
        if (!input.at_end()) {
            co_return codec::failure(at(
              errc::malformed_data,
              field(context, record_field::body_bytes),
              context.origin + input.bytes_consumed().value()));
        }
        co_return work.poll(anchor);
    }

private:
    codec::decode_budget memory_;
    item_count fragments_;

    codec::result<void> charge_metadata(
      byte_count amount,
      const codec::limits& policy,
      codec::field_context context,
      std::uint64_t offset) {
        const auto reduced = codec::detail::consume_decode_budget(
          policy, memory_, byte_count{}, amount, context, offset);
        if (!reduced) return codec::failure(reduced.error());
        const auto spent = output_metadata.checked_add(amount);
        if (!spent)
            return codec::failure(at(errc::out_of_range, context, offset));
        memory_ = *reduced;
        output_metadata = *spent;
        return {};
    }

    seastar::future<codec::result<void>> materialize(
      fragmented_buffer_parser& input,
      field_type& output,
      byte_count length,
      codec::cooperative_work& work,
      codec::field_context context) {
        const auto start = context.origin + input.bytes_consumed().value();
        const auto anchor = at(errc::success, context, start);
        if constexpr (Scan) {
            output = record_byte_range{
              byte_count{body_offset.value() + input.bytes_consumed().value()},
              length};
            auto left = length.value();
            // Skip bounded portions even within one large fragment. No field
            // buffer, deleter promotion or header container is created here.
            do {
                const auto n = std::min(
                  {left,
                   work.byte_quantum().value(),
                   static_cast<std::uint64_t>(
                     input.peek_current_fragment().size())});
                KWAQUE_INVARIANT(
                  invariant_id{"KQ-RECORD-SCAN-FIELD"},
                  left == 0 || n != 0,
                  "validated field ended before its remaining bytes");
                if (
                  auto ready = co_await work.admit(
                    byte_count{n}, item_count{1}, anchor);
                  !ready)
                    co_return codec::failure(ready.error());
                if (auto ready = work.poll(anchor); !ready)
                    co_return codec::failure(ready.error());
                const auto skipped = input.skip(byte_count{n});
                KWAQUE_INVARIANT(
                  invariant_id{"KQ-RECORD-SCAN-SKIP"},
                  skipped.has_value(),
                  "validated field could not advance its exact child");
                left -= n;
            } while (left != 0);
            co_return codec::result<void>{};
        } else {
            if (length.value() == 0) {
                if (
                  auto ready = co_await work.admit(
                    byte_count{}, item_count{1}, anchor);
                  !ready)
                    co_return codec::failure(ready.error());
                co_return work.poll(anchor);
            }
            if (auto ready = co_await work.checkpoint(anchor); !ready)
                co_return codec::failure(ready.error());
            if (auto ready = work.poll(anchor); !ready)
                co_return codec::failure(ready.error());
            const auto cost = slice_cost(
              input, length, work.policy(), memory_, context);
            if (!cost) co_return codec::failure(cost.error());
            const auto count = fragments_.checked_add(cost->fragments);
            if (
              !count || *count > work.policy().config().max_buffer_fragments) {
                co_return codec::failure(
                  at(errc::resource_exhausted, context, start));
            }
            if (
              auto admitted = charge_metadata(
                cost->descriptors, work.policy(), context, start);
              !admitted) {
                co_return codec::failure(admitted.error());
            }
            if (auto ready = work.poll(anchor); !ready)
                co_return codec::failure(ready.error());
            auto shared = input.read_buffer(length);
            if (!shared)
                co_return codec::failure(
                  codec::detail::allocation_cost_error(
                    shared.error(), context, start));
            output = std::move(*shared);
            fragments_ = *count;
            co_return work.poll(anchor);
        }
    }

    // This helper owns a field's admission/transfer, not an integer coroutine.
    // The scalar itself is synchronous; partial ownership is kept in the
    // enclosing state before an awaited operation can fail or observe abort.
    seastar::future<codec::result<void>> read_nullable(
      fragmented_buffer_parser& input,
      std::optional<field_type>& output,
      byte_count maximum,
      record_field length_field,
      record_field value_field,
      codec::cooperative_work& work,
      codec::field_context context) {
        const auto coordinates = field(context, length_field);
        const auto anchor = at(
          errc::success,
          coordinates,
          context.origin + input.bytes_consumed().value());
        if (
          auto ready = co_await work.admit(
            byte_count{20}, item_count{10}, anchor);
          !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        const auto length = codec::read_nullable_length(
          input, maximum, coordinates, codec::input_boundary::complete);
        if (!length) co_return codec::failure(length.error());
        if (!length->has_value()) co_return codec::result<void>{};
        output.emplace();
        co_return co_await materialize(
          input, *output, **length, work, field(context, value_field));
    }
};

} // namespace detail

template<bool Scan>
seastar::future<
  codec::result<std::conditional_t<Scan, record_layout, decoded_record>>>
decode_record_impl(
  bytes::fragmented_buffer_parser& input,
  record_decode_context expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context,
  codec::input_boundary boundary) {
    context = field(context, record_field::body_bytes);
    const auto start = codec::detail::integer_read_start(
      input, context, boundary);
    if (!start) co_return codec::failure(start.error());
    const auto anchor = at(errc::success, context, *start);
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (memory.charge == nullptr || expected.original_count.value() == 0) {
        co_return codec::failure(at(errc::invalid_argument, context, *start));
    }
    if (
      expected.original_count.value()
        > work.policy().config().max_original_records.value()
      || work.byte_quantum().value() < 128
      || work.item_quantum().value() < 64) {
        co_return codec::failure(at(errc::resource_exhausted, context, *start));
    }
    const auto entry_depth = input.checkpoint_depth();
    const auto entry_position = input.bytes_consumed();
    const auto entry_extent = input.total_bytes();
    if (auto marked = input.push_checkpoint(); !marked) {
        co_return codec::failure(
          codec::detail::allocation_cost_error(
            marked.error(), context, *start));
    }
    codec::detail::parser_transaction_guard transaction{input, entry_depth};
    std::optional<fragmented_buffer_parser> child;
    detail::record_decoder<Scan> decoder;
    std::optional<std::conditional_t<Scan, record_layout, record>> produced;
    std::optional<codec::error> failed;
    std::exception_ptr exception;
    byte_count body_length;
    byte_count total_length;
    byte_count body_position;
    try {
        do {
            if (
              auto ready = co_await work.admit(
                byte_count{20}, item_count{10}, anchor);
              !ready) {
                failed = ready.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            const auto length = codec::read_varuint<std::uint32_t>(
              input, context, boundary);
            if (!length) {
                failed = length.error();
                break;
            }
            body_length = byte_count{*length};
            const auto prefix = input.bytes_consumed().value()
                                - entry_position.value();
            total_length = byte_count{prefix + *length};
            if (
              *length < 6
              || total_length.value()
                   > std::numeric_limits<std::uint64_t>::max() - *start) {
                failed = at(errc::malformed_data, context, *start);
                break;
            }
            if (total_length > work.policy().config().max_record_bytes) {
                failed = at(errc::resource_exhausted, context, *start);
                break;
            }
            if (body_length > input.bytes_remaining()) {
                failed = codec::detail::integer_shortage(
                  context,
                  boundary,
                  context.origin + input.total_bytes().value());
                break;
            }
            body_position = input.bytes_consumed();
            if (auto ready = co_await work.checkpoint(anchor); !ready) {
                failed = ready.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            const auto cost = slice_cost(
              input, body_length, work.policy(), memory, context);
            if (!cost) {
                failed = cost.error();
                break;
            }
            const auto reduced = codec::detail::consume_decode_budget(
              work.policy(),
              memory,
              byte_count{},
              cost->descriptors,
              context,
              *start + prefix);
            if (!reduced) {
                failed = reduced.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            auto shared = input.peek_buffer(body_length);
            if (!shared) {
                failed = codec::detail::allocation_cost_error(
                  shared.error(), context, *start + prefix);
                break;
            }
            child.emplace(std::move(*shared));
            const codec::field_context body_context{
              .origin = *start + prefix,
              .family = context.family,
              .field = context.field};
            decoder.body_offset = body_position;
            const auto read = co_await decoder.read(
              *child, expected, *reduced, work, body_context);
            if (!read) {
                failed = read.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            KWAQUE_INVARIANT(
              invariant_id{"KQ-RECORD-DECODE-CHILD"},
              child->total_bytes() == body_length && child->at_end()
                && child->checkpoint_depth() == 0,
              "record decoder did not preserve its complete child");
            if constexpr (Scan) {
                decoder.layout.encoded = record_byte_range{
                  entry_position, total_length};
            }
            produced.emplace(decoder.release_result());
        } while (false);
    } catch (...) {
        exception = std::current_exception();
    }
    // All teardown stays in this frame: allocation failure cannot prevent
    // cleanup admission, and an earlier typed/native failure is preserved.
    if (child) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        child.reset();
    }
    if constexpr (!Scan) {
        for (auto* owner :
             {&decoder.name,
              decoder.key ? &*decoder.key : nullptr,
              decoder.value ? &*decoder.value : nullptr,
              decoder.header_value ? &*decoder.header_value : nullptr}) {
            if (owner != nullptr) {
                const bool nonempty = owner->fragment_count() != 0;
                co_await work.drain_inline(
                  nonempty ? work.byte_quantum() : byte_count{},
                  nonempty ? work.item_quantum() : item_count{1});
                *owner = fragmented_buffer{};
            }
        }
        while (!decoder.headers.empty()) {
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            decoder.headers.pop_back();
        }
        co_await work.drain_inline(byte_count{}, item_count{1});
        std::vector<record_header>{}.swap(decoder.headers);
    }
    if (!failed && !exception) {
        if (auto ready = work.poll(anchor); !ready) failed = ready.error();
    }
    if (failed || exception) {
        if (produced) {
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            produced.reset();
        }
        if (exception) std::rethrow_exception(exception);
        co_return codec::failure(*failed);
    }
    // The exact-child descriptors are now gone. Only reservations attached to
    // returned fields/header storage reduce the caller's next residual.
    const auto remaining = codec::detail::consume_decode_budget(
      work.policy(),
      memory,
      byte_count{},
      decoder.output_metadata,
      context,
      *start);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RECORD-DECODE-BUDGET"},
      remaining.has_value(),
      "released child storage could not restore its temporary allowance");
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RECORD-DECODE-PARENT"},
      produced.has_value() && input.total_bytes() == entry_extent
        && input.bytes_consumed() == body_position
        && input.checkpoint_depth() == entry_depth + 1U,
      "record decode or cleanup changed its enclosing parser");
    const auto advanced = input.skip(body_length);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RECORD-DECODE-COMMIT"},
      advanced.has_value()
        && input.bytes_consumed().value() - entry_position.value()
             == total_length.value(),
      "validated record could not commit its exact extent");
    transaction.commit();
    if constexpr (Scan) {
        co_return std::move(*produced);
    } else {
        co_return decoded_record{std::move(*produced), *remaining};
    }
}

seastar::future<codec::result<decoded_record>> decode_record(
  bytes::fragmented_buffer_parser& input,
  record_decode_context expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context,
  codec::input_boundary boundary) {
    return decode_record_impl<false>(
      input, expected, memory, work, context, boundary);
}

seastar::future<codec::result<record_layout>> detail::scan_record(
  bytes::fragmented_buffer_parser& input,
  record_decode_context expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context) {
    return decode_record_impl<true>(
      input, expected, memory, work, context, codec::input_boundary::complete);
}

} // namespace kwaque::model
