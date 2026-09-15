#include "src/base/invariant.h"
#include "src/codec/envelope_decode.h"
#include "src/model/batch_codec.h"
#include "src/model/batch_decode_body.h"
#include "src/model/batch_wire.h"
#include "src/model/fingerprint.h"
#include "src/model/record_scan.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <optional>
#include <type_traits>
#include <utility>

namespace kwaque::model {
namespace {
using bytes::fragmented_buffer;
using bytes::fragmented_buffer_parser;

codec::error at(
  errc code,
  codec::field_context context,
  batch_field field,
  std::uint64_t offset) noexcept {
    return codec::error{
      code,
      context.family,
      static_cast<std::uint16_t>(field),
      context.origin + offset};
}

template<typename T>
codec::result<T> wire_value(
  result<T> value,
  codec::field_context context,
  batch_field field,
  std::uint64_t offset) {
    if (value) return std::move(*value);
    const auto code = value.error();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-BATCH-DECODE-VALUE"},
      code == errc::invalid_argument || code == errc::out_of_range
        || code == errc::resource_exhausted,
      "fixed model value returned an unexpected error category");
    return codec::failure(at(
      code == errc::resource_exhausted ? errc::resource_exhausted
                                       : errc::malformed_data,
      context,
      field,
      offset));
}

template<std::size_t Offset, typename Id, std::size_t N>
codec::result<Id> read_id(
  const std::array<char, N>& fixed,
  codec::field_context context,
  batch_field field) {
    static_assert(Offset <= N && Id::width <= N - Offset);
    std::array<std::uint8_t, Id::width> bytes{};
    for (std::size_t i = 0; i < bytes.size(); ++i)
        bytes[i] = static_cast<std::uint8_t>(fixed[Offset + i]);
    return wire_value(Id::make(bytes), context, field, Offset);
}

template<bool Assigned>
using decoded_type = std::
  conditional_t<Assigned, decoded_assigned_batch, decoded_submitted_batch>;
template<bool Assigned>
using context_type = std::
  conditional_t<Assigned, assigned_batch_context, submitted_batch_context>;

template<bool Assigned>
struct fixed_fields final {
    context_type<Assigned> context;
    codec::semantic_batch_digest digest;
    item_count headers;
    byte_count encoded_records;
    byte_count expanded_records;
    compression::codec_id encoding;
};

template<bool Assigned, std::size_t N>
codec::result<fixed_fields<Assigned>> parse_fixed(
  const std::array<char, N>& fixed,
  batch_decode_expectation expected,
  const codec::limits& policy,
  codec::field_context context) {
    using detail::batch_load;
    // Profile is checked before interpreting its identity/count/record grammar.
    const auto encoding = compression::parse_codec_id(
      batch_load<156, std::uint8_t>(fixed),
      {context.origin + 156U,
       context.family,
       static_cast<std::uint16_t>(batch_field::codec)});
    if (!encoding) return codec::failure(encoding.error());
    if (batch_load<157, std::uint8_t>(fixed) != 0)
        return codec::failure(
          at(errc::malformed_data, context, batch_field::reserved, 157));
    if (batch_load<158, std::uint16_t>(fixed) != 1)
        return codec::failure(at(
          errc::unsupported_format, context, batch_field::record_profile, 158));
    const auto producer = read_id<0, producer_id>(
      fixed, context, batch_field::producer);
    if (!producer) return codec::failure(producer.error());
    if (expected.id && *producer != expected.id->producer())
        return codec::failure(
          at(errc::wrong_context, context, batch_field::producer, 0));
    const auto epoch = wire_value(
      producer_epoch::make(batch_load<16, std::uint64_t>(fixed)),
      context,
      batch_field::producer_epoch,
      16);
    if (!epoch) return codec::failure(epoch.error());
    if (expected.id && *epoch != expected.id->epoch())
        return codec::failure(
          at(errc::wrong_context, context, batch_field::producer_epoch, 16));
    const auto stream = wire_value(
      producer_stream_id::make(batch_load<24, std::uint64_t>(fixed)),
      context,
      batch_field::producer_stream,
      24);
    if (!stream) return codec::failure(stream.error());
    if (expected.id && *stream != expected.id->stream())
        return codec::failure(
          at(errc::wrong_context, context, batch_field::producer_stream, 24));
    const batch_sequence sequence{batch_load<32, std::uint64_t>(fixed)};
    if (expected.id && sequence != expected.id->sequence())
        return codec::failure(
          at(errc::wrong_context, context, batch_field::sequence, 32));
    const auto id = wire_value(
      batch_id::make(*producer, *epoch, *stream, sequence),
      context,
      batch_field::producer,
      0);
    if (!id) return codec::failure(id.error());
    const auto topic = read_id<40, topic_id>(
      fixed, context, batch_field::topic);
    if (!topic) return codec::failure(topic.error());
    if (*topic != expected.topic)
        return codec::failure(
          at(errc::wrong_context, context, batch_field::topic, 40));
    const auto range = read_id<56, range_id>(
      fixed, context, batch_field::range);
    if (!range) return codec::failure(range.error());
    if (*range != expected.range)
        return codec::failure(
          at(errc::wrong_context, context, batch_field::range, 56));
    const auto routing = wire_value(
      range_routing_epoch::make(batch_load<72, std::uint64_t>(fixed)),
      context,
      batch_field::routing_epoch,
      72);
    if (!routing) return codec::failure(routing.error());
    if (
      expected.original_binding
      && *routing != expected.original_binding->routing_epoch())
        return codec::failure(
          at(errc::wrong_context, context, batch_field::routing_epoch, 72));
    const auto segment = read_id<80, segment_id>(
      fixed, context, batch_field::original_segment);
    if (!segment) return codec::failure(segment.error());
    if (
      expected.original_binding
      && *segment != expected.original_binding->segment())
        return codec::failure(
          at(errc::wrong_context, context, batch_field::original_segment, 80));
    const auto generation = wire_value(
      segment_generation::make(batch_load<96, std::uint64_t>(fixed)),
      context,
      batch_field::original_generation,
      96);
    if (!generation) return codec::failure(generation.error());
    if (
      expected.original_binding
      && *generation != expected.original_binding->generation())
        return codec::failure(at(
          errc::wrong_context, context, batch_field::original_generation, 96));
    const auto binding = wire_value(
      producer_stream_binding::make(
        *topic, *range, *routing, *segment, *generation),
      context,
      batch_field::topic,
      40);
    if (!binding) return codec::failure(binding.error());
    codec::sha256_digest digest_bytes{};
    for (std::size_t i = 0; i < digest_bytes.size(); ++i)
        digest_bytes[i] = static_cast<unsigned char>(fixed[104 + i]);
    const codec::semantic_batch_digest digest{digest_bytes};
    if (expected.fingerprint && digest != *expected.fingerprint)
        return codec::failure(
          at(errc::wrong_context, context, batch_field::fingerprint, 104));
    const runtime::wall_time timestamp{batch_load<136, std::int64_t>(fixed)};
    const auto original = batch_load<144, std::uint32_t>(fixed);
    const auto retained = batch_load<148, std::uint32_t>(fixed);
    const auto headers = batch_load<152, std::uint32_t>(fixed);
    const auto config = policy.config();
    if (original == 0)
        return codec::failure(
          at(errc::malformed_data, context, batch_field::original_count, 144));
    if (original > config.max_original_records.value())
        return codec::failure(at(
          errc::resource_exhausted, context, batch_field::original_count, 144));
    if (
      retained == 0 || retained > original
      || (!Assigned && retained != original))
        return codec::failure(
          at(errc::malformed_data, context, batch_field::retained_count, 148));
    if (
      headers > config.max_batch_headers.value()
      || headers > retained * config.max_record_headers.value())
        return codec::failure(at(
          errc::resource_exhausted, context, batch_field::header_count, 152));
    const auto encoded = batch_load<160, std::uint32_t>(fixed);
    const auto expanded = batch_load<164, std::uint32_t>(fixed);
    const auto encoded_limit = config.max_encoded_body_bytes.checked_sub(
      byte_count{N});
    if (!encoded_limit || encoded > encoded_limit->value()
        || (*encoding == compression::codec_id::none && encoded > config.max_expanded_batch_bytes.value()))
        return codec::failure(at(
          errc::resource_exhausted,
          context,
          batch_field::encoded_record_bytes,
          160));
    if (expanded > config.max_expanded_batch_bytes.value())
        return codec::failure(at(
          errc::resource_exhausted,
          context,
          batch_field::expanded_record_bytes,
          164));
    if (*encoding == compression::codec_id::none && encoded != expanded)
        return codec::failure(at(
          errc::malformed_data,
          context,
          batch_field::expanded_record_bytes,
          164));
    const auto submitted = wire_value(
      submitted_batch_context::make(
        *id, *binding, range_logical_count{original}, timestamp),
      context,
      batch_field::original_count,
      144);
    if (!submitted) return codec::failure(submitted.error());
    if constexpr (Assigned) {
        const auto assigned = assigned_batch_context::restore(
          *submitted,
          item_count{retained},
          range_logical_end{batch_load<168, std::uint64_t>(fixed)},
          range_logical_end{batch_load<176, std::uint64_t>(fixed)});
        if (!assigned)
            return codec::failure(at(
              errc::malformed_data,
              context,
              assigned.error() == errc::out_of_range
                ? batch_field::logical_begin
                : batch_field::logical_end,
              assigned.error() == errc::out_of_range ? 168 : 176));
        return fixed_fields<Assigned>{
          *assigned,
          digest,
          item_count{headers},
          byte_count{encoded},
          byte_count{expanded},
          *encoding};
    } else {
        return fixed_fields<Assigned>{
          *submitted,
          digest,
          item_count{headers},
          byte_count{encoded},
          byte_count{expanded},
          *encoding};
    }
}

template<bool Assigned>
seastar::future<codec::result<fixed_fields<Assigned>>> read_body(
  fragmented_buffer_parser& input,
  fragmented_buffer& records,
  std::optional<fragmented_buffer_parser>& expanded_input,
  byte_count& backing,
  byte_count& metadata,
  batch_decode_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context) {
    constexpr auto fixed_bytes = Assigned ? assigned_batch_fixed_bytes
                                          : submitted_batch_fixed_bytes;
    const auto anchor = at(errc::success, context, batch_field::fixed_body, 0);
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (input.bytes_remaining() < fixed_bytes)
        co_return codec::failure(at(
          errc::malformed_data,
          context,
          batch_field::fixed_body,
          input.total_bytes().value()));
    if (
      work.byte_quantum().value() < 4U * sizeof(record_layout)
      || work.item_quantum().value() < 64)
        co_return codec::failure(
          at(errc::resource_exhausted, context, batch_field::fixed_body, 0));
    if (
      auto ready = co_await work.admit(fixed_bytes, item_count{1}, anchor);
      !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    std::array<char, fixed_bytes.value()> fixed{};
    // The inline fixed body can itself cross many fragments. Read small groups
    // before performing its bounded scalar/factory checks without allocation.
    for (std::size_t offset = 0; offset < fixed.size();) {
        const auto size = std::min<std::size_t>(32, fixed.size() - offset);
        if (
          auto ready = co_await work.admit(
            byte_count{4U * size}, item_count{2U * size}, anchor);
          !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        const auto read = input.read_to(
          std::span<char>{fixed}.subspan(offset, size));
        KWAQUE_INVARIANT(
          invariant_id{"KQ-BATCH-DECODE-FIXED"},
          read.has_value(),
          "bounded fixed body read failed");
        offset += size;
    }
    if (
      auto ready = co_await work.admit(
        byte_count{2048}, item_count{64}, anchor);
      !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    const auto fields = parse_fixed<Assigned>(
      fixed, expected, work.policy(), context);
    if (!fields) co_return codec::failure(fields.error());
    const auto submitted = [&] {
        if constexpr (Assigned)
            return fields->context.submitted();
        else
            return fields->context;
    }();
    const auto retained = [&] {
        if constexpr (Assigned)
            return fields->context.retained_count();
        else
            return item_count{submitted.original_count().value()};
    }();
    if (fields->encoded_records != input.bytes_remaining())
        co_return codec::failure(at(
          errc::malformed_data,
          context,
          batch_field::encoded_record_bytes,
          160));
    if (retained.value() > fields->expanded_records.value() / 7U) {
        const bool raw = fields->encoding == compression::codec_id::none;
        co_return codec::failure(at(
          errc::malformed_data,
          context,
          raw ? batch_field::encoded_record_bytes
              : batch_field::expanded_record_bytes,
          raw ? 160U : 164U));
    }
    if (auto ready = co_await work.checkpoint(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    const auto cost = input.next_buffer_allocation_cost(
      fields->encoded_records, memory.charge);
    if (!cost)
        co_return codec::failure(
          codec::detail::allocation_cost_error(
            cost.error(), context, context.origin + fixed.size()));
    const auto valid = codec::detail::validate_decode_cost(
      fields->encoded_records,
      work.policy().config().max_encoded_body_bytes,
      *cost,
      work.policy(),
      context,
      context.origin + fixed.size());
    if (!valid) co_return codec::failure(valid.error());
    auto remaining = codec::detail::consume_decode_budget(
      work.policy(),
      memory,
      byte_count{},
      cost->descriptors,
      context,
      context.origin + fixed.size());
    if (!remaining) co_return codec::failure(remaining.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    auto shared = input.peek_buffer(fields->encoded_records);
    if (!shared)
        co_return codec::failure(
          codec::detail::allocation_cost_error(
            shared.error(), context, context.origin + fixed.size()));
    records = std::move(*shared);
    metadata = cost->descriptors;
    const bool compressed = fields->encoding == compression::codec_id::lz4;
    const codec::field_context region_context{
      context.origin + fixed.size(),
      context.family,
      static_cast<std::uint16_t>(batch_field::records)};
    if (compressed) {
        auto expanded = co_await compression::decompress_lz4(
          std::move(records),
          fields->expanded_records,
          work,
          *remaining,
          region_context);
        if (!expanded) co_return codec::failure(expanded.error());
        backing = expanded->retained.backing;
        metadata = *expanded->retained.descriptors.checked_add(
          expanded->retained.share_controls);
        records = std::move(expanded->value);
        // The consumed encoded alias is gone, but its parent backing is still
        // reserved. Reconcile that temporary alias, then reserve the raw owner
        // and its separate validation parser before creating the share.
        remaining = codec::detail::consume_decode_budget(
          work.policy(),
          memory,
          backing,
          metadata,
          region_context,
          region_context.origin);
        if (!remaining) co_return codec::failure(remaining.error());
        if (auto ready = co_await work.checkpoint(anchor); !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        const auto alias = records.slice_allocation_cost(
          byte_count{}, records.size(), memory.charge);
        if (!alias)
            co_return codec::failure(
              codec::detail::allocation_cost_error(
                alias.error(), region_context, region_context.origin));
        const auto valid_alias = codec::detail::validate_decode_cost(
          records.size(),
          work.policy().config().max_expanded_batch_bytes,
          *alias,
          work.policy(),
          region_context,
          region_context.origin);
        if (!valid_alias) co_return codec::failure(valid_alias.error());
        remaining = codec::detail::consume_decode_budget(
          work.policy(),
          *remaining,
          byte_count{},
          alias->descriptors,
          region_context,
          region_context.origin);
        if (!remaining) co_return codec::failure(remaining.error());
        auto shared_raw = records.share(byte_count{}, records.size());
        if (!shared_raw)
            co_return codec::failure(
              codec::detail::allocation_cost_error(
                shared_raw.error(), region_context, region_context.origin));
        expanded_input.emplace(std::move(*shared_raw));
    }
    auto& scan = compressed ? *expanded_input : input;
    // Expanded positions never enter wire-origin arithmetic, even when the
    // encoded input's last byte is near UINT64_MAX.
    const auto scan_context
      = compressed
          ? codec::
              field_context{0, context.family, static_cast<std::uint16_t>(batch_field::records)}
          : context;
    std::uint64_t headers = 0;
    std::optional<range_logical_count> previous;
    const bool dense = retained.value() == submitted.original_count().value();
    for (std::uint64_t index = 0; index < retained.value(); ++index) {
        if (
          auto ready = co_await work.admit(
            byte_count{4U * sizeof(record_layout)}, item_count{64}, anchor);
          !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        const auto record = co_await detail::scan_record(
          scan,
          {submitted.original_timestamp_base(),
           submitted.original_count(),
           item_count{
             work.policy().config().max_batch_headers.value() - headers}},
          *remaining,
          work,
          scan_context);
        if (!record)
            co_return codec::failure(
              compressed ? at(
                             record.error().code(),
                             context,
                             batch_field::records,
                             fixed.size())
                         : record.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        const auto delta = record->fields.logical_delta;
        if (
          (previous && delta <= *previous)
          || (dense && (delta.value() != index || (index == 0 && record->fields.timestamp_delta != 0))))
            co_return codec::failure(at(
              errc::malformed_data,
              context,
              batch_field::records,
              compressed ? fixed.size() : record->encoded.offset.value()));
        previous = delta;
        headers += record->headers().size();
        if (headers > fields->headers.value())
            co_return codec::failure(at(
              errc::malformed_data, context, batch_field::header_count, 152));
    }
    if (!scan.at_end())
        co_return codec::failure(at(
          errc::malformed_data,
          context,
          batch_field::records,
          compressed ? fixed.size() : scan.bytes_consumed().value()));
    if (headers != fields->headers.value())
        co_return codec::failure(
          at(errc::malformed_data, context, batch_field::header_count, 152));
    if (dense) {
        const auto fingerprint_anchor = at(
          errc::success, context, batch_field::fingerprint, 104);
        const auto computed = co_await compute_submitted_fingerprint(
          submitted, records, work, fingerprint_anchor);
        if (!computed) co_return codec::failure(computed.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        if (*computed != fields->digest)
            co_return codec::failure(
              at(errc::corrupt_data, context, batch_field::fingerprint, 104));
    }
    if (compressed) {
        const auto consumed = input.skip(fields->encoded_records);
        KWAQUE_INVARIANT(
          invariant_id{"KQ-BATCH-COMPRESSED-EXTENT"},
          consumed.has_value(),
          "complete encoded record region could not advance its body parser");
    }
    co_return *fields;
}
} // namespace

namespace detail {
template<bool Assigned>
class batch_decoder final {
public:
    batch_decoder(
      batch_decode_expectation expected, codec::decode_budget original) noexcept
      : expected_(std::move(expected))
      , original_(original) {}

    seastar::future<codec::result<decoded_type<Assigned>>> operator()(
      fragmented_buffer_parser& input,
      codec::field_context context,
      codec::input_boundary,
      codec::decode_budget memory,
      codec::cooperative_work& work) {
        return decode(input, expected_, original_, memory, work, context);
    }

    static seastar::future<codec::result<decoded_type<Assigned>>> decode(
      fragmented_buffer_parser& input,
      batch_decode_expectation expected,
      codec::decode_budget original,
      codec::decode_budget memory,
      codec::cooperative_work& work,
      codec::field_context context) {
        fragmented_buffer records;
        std::optional<fragmented_buffer_parser> expanded_input;
        byte_count backing;
        byte_count metadata;
        std::optional<codec::result<fixed_fields<Assigned>>> outcome;
        std::exception_ptr exception;
        try {
            outcome.emplace(
              co_await read_body<Assigned>(
                input,
                records,
                expanded_input,
                backing,
                metadata,
                expected,
                memory,
                work,
                context));
        } catch (...) {
            exception = std::current_exception();
        }
        if (expanded_input) {
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            expanded_input.reset();
        }
        if (!exception && outcome->has_value()) {
            if (
              auto ready = work.poll(
                at(errc::success, context, batch_field::records, 0));
              !ready)
                *outcome = codec::failure(ready.error());
        }
        if (exception || !outcome->has_value()) {
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            records = fragmented_buffer{};
            if (exception) std::rethrow_exception(exception);
            co_return codec::failure(outcome->error());
        }
        // This residual is prospective until the enclosing envelope destroys
        // its temporary body alias and commits. Nothing escapes this callback
        // early; the returned record region is the only new persistent owner.
        const auto remaining = codec::detail::consume_decode_budget(
          work.policy(), original, backing, metadata, context, context.origin);
        KWAQUE_INVARIANT(
          invariant_id{"KQ-BATCH-DECODE-RESIDUAL"},
          remaining.has_value(),
          "admitted record metadata exceeded its enclosing reservation");
        const auto& fields = **outcome;
        if constexpr (Assigned) {
            const auto verification
              = fields.context.retained_count().value()
                    == fields.context.submitted().original_count().value()
                  ? batch_fingerprint_verification::recomputed
                  : batch_fingerprint_verification::carried;
            co_return decoded_assigned_batch{
              assigned_batch{
                fields.context,
                fields.digest,
                fields.headers,
                std::move(records)},
              *remaining,
              verification};
        } else {
            co_return decoded_submitted_batch{
              submitted_batch{
                fields.context,
                fields.digest,
                fields.headers,
                std::move(records)},
              *remaining};
        }
    }

private:
    batch_decode_expectation expected_;
    codec::decode_budget original_;
};
seastar::future<codec::result<decoded_submitted_batch>> decode_submitted_body(
  fragmented_buffer_parser& input,
  batch_decode_expectation expected,
  codec::decode_budget original,
  codec::decode_budget remaining,
  codec::cooperative_work& work,
  codec::field_context context) {
    return batch_decoder<false>::decode(
      input, std::move(expected), original, remaining, work, context);
}
seastar::future<codec::result<decoded_assigned_batch>> decode_assigned_body(
  fragmented_buffer_parser& input,
  batch_decode_expectation expected,
  codec::decode_budget original,
  codec::decode_budget remaining,
  codec::cooperative_work& work,
  codec::field_context context) {
    return batch_decoder<true>::decode(
      input, std::move(expected), original, remaining, work, context);
}
} // namespace detail

namespace {
template<bool Assigned>
seastar::future<codec::result<decoded_type<Assigned>>> decode_batch(
  fragmented_buffer_parser& input,
  batch_decode_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context,
  codec::input_boundary boundary) {
    constexpr auto family = Assigned ? codec::format_family::assigned_batch
                                     : codec::format_family::submitted_batch;
    context.family = static_cast<std::uint16_t>(family);
    const auto start = codec::detail::integer_read_start(
      input, context, boundary);
    const auto fail = [](codec::error error) {
        return seastar::make_ready_future<
          codec::result<decoded_type<Assigned>>>(codec::failure(error));
    };
    if (!start) return fail(start.error());
    if (
      auto ready = work.poll(at(
        errc::success,
        context,
        batch_field::fixed_body,
        input.bytes_consumed().value()));
      !ready)
        return fail(ready.error());
    if (expected.topic.is_nil() || expected.range.is_nil() || (expected.original_binding
        && (expected.original_binding->topic() != expected.topic || expected.original_binding->range() != expected.range)))
        return fail(at(
          errc::invalid_argument,
          context,
          batch_field::fixed_body,
          input.bytes_consumed().value()));
    const auto body_cap = work.policy().config().max_encoded_body_bytes;
    const byte_count total_cap{
      body_cap.value() + work.policy().config().max_header_bytes.value()};
    return codec::decode_envelope<decoded_type<Assigned>>(
      input,
      family,
      {body_cap, total_cap},
      memory,
      work,
      detail::batch_decoder<Assigned>{std::move(expected), memory},
      context,
      boundary);
}
} // namespace

seastar::future<codec::result<decoded_submitted_batch>> decode_submitted_batch(
  fragmented_buffer_parser& input,
  batch_decode_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context,
  codec::input_boundary boundary) {
    return decode_batch<false>(
      input, std::move(expected), memory, work, context, boundary);
}

seastar::future<codec::result<decoded_assigned_batch>> decode_assigned_batch(
  fragmented_buffer_parser& input,
  batch_decode_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context,
  codec::input_boundary boundary) {
    return decode_batch<true>(
      input, std::move(expected), memory, work, context, boundary);
}

} // namespace kwaque::model
