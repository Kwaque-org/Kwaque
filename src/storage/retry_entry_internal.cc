#include "src/storage/retry_entry_internal.h"

#include "src/storage/page_internal.h"

namespace kwaque::storage::detail {
codec::result<completed_retry> read_retry_entry(
  const std::array<char, 160>& raw,
  segment_context expected,
  codec::field_context c,
  const std::optional<model::batch_id>& previous) {
    const auto producer = detail::read_id<0, model::producer_id>(raw, c);
    const auto epoch = detail::page_wire(
      model::producer_epoch::make(load<16, std::uint64_t>(raw)), c, 16);
    const auto stream = detail::page_wire(
      model::producer_stream_id::make(load<24, std::uint64_t>(raw)), c, 24);
    if (!producer) return codec::failure(producer.error());
    if (!epoch) return codec::failure(epoch.error());
    if (!stream) return codec::failure(stream.error());
    const auto id = model::batch_id::make(
                      *producer,
                      *epoch,
                      *stream,
                      model::batch_sequence{load<32, std::uint64_t>(raw)})
                      .value();
    // Reject the key before decoding its value or inserting the entry. The
    // summary walker supplies the preceding page's last full key as well.
    if (previous && !previous->canonical_less(id))
        return codec::failure(page_error(errc::malformed_data, c));
    const auto topic = detail::read_id<72, model::topic_id>(raw, c);
    const auto range = detail::read_id<88, model::range_id>(raw, c);
    const auto routing = detail::page_wire(
      model::range_routing_epoch::make(load<104, std::uint64_t>(raw)), c, 104);
    const auto segment = detail::read_id<112, model::segment_id>(raw, c);
    const auto generation = detail::page_wire(
      model::segment_generation::make(load<128, std::uint64_t>(raw)), c, 128);
    const auto ack = detail::page_wire(
      model::segment_generation::make(load<152, std::uint64_t>(raw)), c, 152);
    const auto logical = detail::page_wire(
      model::range_logical_span::make(
        model::range_logical_end{load<136, std::uint64_t>(raw)},
        model::range_logical_end{load<144, std::uint64_t>(raw)}),
      c,
      136);
    if (!topic) return codec::failure(topic.error());
    if (!range) return codec::failure(range.error());
    if (!routing) return codec::failure(routing.error());
    if (!segment) return codec::failure(segment.error());
    if (!generation) return codec::failure(generation.error());
    if (!ack) return codec::failure(ack.error());
    if (!logical) return codec::failure(logical.error());
    if (*topic != expected.topic() || *range != expected.range())
        return codec::failure(page_error(errc::wrong_context, c, 72));
    const auto binding = model::producer_stream_binding::make(
                           *topic, *range, *routing, *segment, *generation)
                           .value();
    return detail::page_wire(
      completed_retry::make(
        id,
        codec::semantic_batch_digest{detail::read_digest<40>(raw)},
        binding,
        *logical,
        *ack),
      c);
}
void write_retry_entry(
  std::array<char, 160>& out, const completed_retry& entry) noexcept {
    const auto id = entry.id();
    const auto binding = entry.original_binding();
    detail::write_id<0>(out, id.producer());
    store<16>(out, id.epoch().value());
    store<24>(out, id.stream().value());
    store<32>(out, id.sequence().value());
    detail::write_digest<40>(out, entry.submitted_digest());
    detail::write_id<72>(out, binding.topic());
    detail::write_id<88>(out, binding.range());
    store<104>(out, binding.routing_epoch().value());
    detail::write_id<112>(out, binding.segment());
    store<128>(out, binding.generation().value());
    store<136>(out, entry.returned_span().begin().value());
    store<144>(out, entry.returned_span().end().value());
    store<152>(out, entry.ack_generation().value());
}
} // namespace kwaque::storage::detail
