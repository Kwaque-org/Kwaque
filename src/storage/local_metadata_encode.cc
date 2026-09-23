#include "src/storage/local_metadata_internal.h"

#include <algorithm>
#include <array>
#include <span>

namespace kwaque::storage {
namespace {
using detail::store;
class metadata_writer final {
public:
    metadata_writer(
      local_metadata_header header,
      aligned_envelope_layout layout,
      codec::cooperative_work& work,
      byte_count remaining,
      bytes::allocation_charge_fn charge,
      codec::field_context context,
      byte_count payload)
      : layout_(layout)
      , work_(work)
      , remaining_(remaining)
      , charge_(charge)
      , context_(context) {
        store<0>(fixed_, static_cast<std::uint16_t>(header.kind()));
        detail::write_id<4>(fixed_, header.owner().cluster());
        detail::write_id<20>(fixed_, header.owner().broker());
        detail::write_id<36>(fixed_, header.owner().device());
        store<52>(fixed_, header.owner().shard());
        store<56>(fixed_, header.generation().value());
        store<64>(fixed_, static_cast<std::uint32_t>(payload.value()));
        store<68>(
          fixed_, static_cast<std::uint32_t>(layout.padding_bytes().value()));
    }
    auto operator()(const local_store_identity& value) {
        store<72>(fixed_, value.layout_version);
        store<74>(
          fixed_,
          static_cast<std::uint32_t>(value.metadata_alignment.bytes().value()));
        store<78>(fixed_, value.shard_count);
        store<82>(fixed_, static_cast<std::uint16_t>(value.role));
        return scalar<88>();
    }
    auto operator()(const local_shard_control& value) {
        const auto raw = value.wal_high.bytes();
        std::copy(raw.begin(), raw.end(), fixed_.begin() + 72);
        store<88>(fixed_, value.object_high.value());
        store<96>(fixed_, value.decision_high.value());
        store<104>(fixed_, value.deletion_high.value());
        store<112>(
          fixed_, static_cast<std::uint8_t>(value.checkpoint.has_value()));
        return value.checkpoint ? control<true>(value) : control<false>(value);
    }
    auto operator()(const local_wal_descriptor& value) {
        detail::write_id<72>(fixed_, value.incarnation);
        store<88>(
          fixed_, static_cast<std::uint8_t>(value.predecessor.has_value()));
        return value.predecessor ? wal<true>(value) : wal<false>(value);
    }
    auto operator()(const local_segment_descriptor& value) {
        detail::write_sc<72>(fixed_, value.segment);
        store<144>(fixed_, value.logical_origin.value());
        store<152>(fixed_, value.physical_origin.value());
        store<160>(
          fixed_, static_cast<std::uint32_t>(value.alignment.bytes().value()));
        store<164>(fixed_, static_cast<std::uint16_t>(value.profile));
        store<166>(fixed_, value.record_profile);
        store<168>(fixed_, static_cast<std::uint8_t>(value.layout));
        store<176>(fixed_, value.maximum_data_bytes.value());
        store<184>(fixed_, value.maximum_lifetime.nanoseconds());
        return scalar<192>();
    }
    auto operator()(const local_object_publication& value) {
        detail::write_sc<72>(fixed_, value.segment);
        store<144>(fixed_, static_cast<std::uint8_t>(value.state));
        store<152>(
          fixed_, static_cast<std::uint8_t>(value.boundary.has_value()));
        return value.boundary ? publication<true>(value)
                              : publication<false>(value);
    }
    auto operator()(const local_checkpoint_root& value) {
        detail::write_local_cursor<72>(fixed_, value.begin);
        detail::write_local_cursor<96>(fixed_, value.end);
        store<120>(fixed_, value.entry_count);
        store<124>(fixed_, static_cast<std::uint32_t>(value.pages.size()));
        return finish<128, 48>(
          std::span<const page_ref>{value.pages}, detail::write_page_ref);
    }
    auto operator()(const local_checkpoint_page& value) {
        store<72>(fixed_, value.sequence.value());
        store<80>(fixed_, value.ordinal.value());
        store<84>(fixed_, value.first_entry);
        store<88>(fixed_, static_cast<std::uint32_t>(value.entries.size()));
        return finish<92, 164>(
          std::span<const local_checkpoint_entry>{value.entries},
          detail::write_local_checkpoint_entry);
    }
    auto operator()(const local_deletion_intent& value) {
        detail::write_sc<72>(fixed_, value.segment);
        store<144>(fixed_, value.owner_decision_id);
        store<152>(fixed_, value.published_generation.value());
        store<160>(fixed_, static_cast<std::uint16_t>(value.reason));
        store<164>(fixed_, static_cast<std::uint32_t>(value.objects.size()));
        return finish<168, 12>(
          std::span<const local_deletion_object>{value.objects},
          detail::write_local_deletion_object);
    }
    auto operator()(const local_recovery_decision& value) {
        detail::write_local_cursor<72>(fixed_, value.prepare);
        detail::write_digest<96>(fixed_, value.prepare_digest);
        detail::write_sc<128>(fixed_, value.segment);
        store<200>(fixed_, value.target_position.value());
        store<208>(fixed_, static_cast<std::uint16_t>(value.action));
        store<212>(fixed_, value.owner_decision_id);
        return scalar<220>();
    }
    auto operator()(const local_boundary_evidence& value) {
        detail::write_sc<72>(fixed_, value.segment);
        detail::write_id<144>(fixed_, value.data_device);
        detail::write_local_footer<160>(fixed_, value.footer);
        detail::write_coverage<208>(fixed_, value.covered);
        store<256>(fixed_, static_cast<std::uint8_t>(value.retry.has_value()));
        if (value.retry) {
            embedded_root<260>(*value.retry);
            return scalar<320>();
        }
        return scalar<260>();
    }
    auto operator()(const local_completed_retry_root& value) {
        detail::write_sc<72>(fixed_, value.segment);
        detail::write_local_footer<144>(fixed_, value.footer);
        store<192>(fixed_, value.retry_count);
        store<196>(fixed_, static_cast<std::uint32_t>(value.pages.size()));
        return finish<200, 48>(
          std::span<const page_ref>{value.pages}, detail::write_page_ref);
    }
    auto operator()(const local_completed_retry_page& value) {
        detail::write_sc<72>(fixed_, value.segment);
        store<144>(fixed_, value.sequence.value());
        store<152>(fixed_, value.ordinal.value());
        store<156>(fixed_, value.first_entry);
        store<160>(fixed_, static_cast<std::uint32_t>(value.entries.size()));
        return finish<164, 160>(
          std::span<const completed_retry>{value.entries},
          detail::write_retry_entry);
    }

private:
    template<std::size_t Offset>
    void embedded_root(const local_root_reference& ref) {
        std::array<char, 60> raw{};
        detail::write_local_root(raw, ref);
        std::copy(raw.begin(), raw.end(), fixed_.begin() + Offset);
    }
    template<bool Checkpoint>
    seastar::future<codec::result<encoded_local_metadata>>
    control(const local_shard_control& value) {
        if constexpr (Checkpoint) embedded_root<116>(*value.checkpoint);
        constexpr auto at = Checkpoint ? 176U : 116U;
        store<at>(
          fixed_, static_cast<std::uint8_t>(value.wal_head.has_value()));
        if (value.wal_head) {
            detail::write_id<at + 4>(fixed_, value.wal_head->incarnation);
            detail::write_digest<at + 20>(
              fixed_, value.wal_head->header_digest);
            return scalar<at + 52>();
        }
        return scalar<at + 4>();
    }
    template<bool Predecessor>
    seastar::future<codec::result<encoded_local_metadata>>
    wal(const local_wal_descriptor& value) {
        if constexpr (Predecessor)
            detail::write_local_cursor<92>(fixed_, *value.predecessor);
        constexpr auto at = Predecessor ? 116U : 92U;
        store<at>(
          fixed_, static_cast<std::uint32_t>(value.alignment.bytes().value()));
        store<at + 4>(fixed_, static_cast<std::uint16_t>(value.profile));
        store<at + 8>(fixed_, value.data_start.value());
        store<at + 16>(fixed_, value.capacity_bytes.value());
        return scalar<at + 24>();
    }
    template<bool Boundary>
    seastar::future<codec::result<encoded_local_metadata>>
    publication(const local_object_publication& value) {
        if constexpr (Boundary)
            detail::write_local_footer<156>(fixed_, *value.boundary);
        constexpr auto at = Boundary ? 204U : 156U;
        store<at>(fixed_, static_cast<std::uint32_t>(value.roots.size()));
        return finish<at + 4, 60>(
          std::span<const local_root_reference>{value.roots},
          detail::write_local_root);
    }
    template<std::size_t N>
    seastar::future<codec::result<encoded_local_metadata>> scalar() {
        return finish<N, 1>(
          std::span<const char>{},
          [](std::array<char, 1>&, const char&) noexcept {});
    }
    template<std::size_t N, std::size_t Width, typename Entry, typename Write>
    seastar::future<codec::result<encoded_local_metadata>>
    finish(std::span<const Entry> entries, Write write) {
        std::array<char, N> fixed{};
        std::copy_n(fixed_.begin(), N, fixed.begin());
        auto encoded = co_await detail::encode_page_object<Width>(
          fixed,
          entries,
          write,
          layout_,
          detail::local_family,
          work_,
          remaining_,
          charge_,
          context_);
        if (!encoded) co_return codec::failure(encoded.error());
        co_return encoded_local_metadata{
          std::move(encoded->bytes), encoded->digest};
    }
    std::array<char, 512> fixed_{};
    aligned_envelope_layout layout_;
    codec::cooperative_work& work_;
    byte_count remaining_;
    bytes::allocation_charge_fn charge_;
    codec::field_context context_;
};
} // namespace

seastar::future<codec::result<encoded_local_metadata>> encode_local_metadata(
  local_metadata_encoding context,
  const local_metadata_payload& payload,
  codec::cooperative_work& work,
  byte_count remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context c) {
    c.family = static_cast<std::uint16_t>(detail::local_family);
    const auto size = detail::local_payload_bytes(payload);
    if (!size)
        co_return codec::failure(
          codec::detail::allocation_cost_error(size.error(), c, c.origin));
    if (!charge || (detail::local_payload_segment(payload).has_value() != context.segment_alignment.has_value())
        || (context.data_metadata_alignment && context.header.kind() != local_metadata_kind::boundary_evidence))
        co_return codec::failure(detail::page_error(errc::invalid_argument, c));
    const auto layout = local_metadata_layout(
      context.header.kind(),
      *size,
      byte_count{32},
      context.alignment,
      work.policy());
    if (!layout)
        co_return codec::failure(
          codec::detail::allocation_cost_error(layout.error(), c, c.origin));
    if (layout->encoded_bytes().value() > UINT64_MAX - c.origin)
        co_return codec::failure(detail::page_error(errc::out_of_range, c));
    if (
      auto valid = co_await detail::validate_local_payload(
        context.header,
        payload,
        *layout,
        context.segment_alignment,
        context.data_metadata_alignment,
        work,
        c,
        true);
      !valid)
        co_return codec::failure(valid.error());
    metadata_writer writer{
      context.header, *layout, work, remaining, charge, c, *size};
    auto output = co_await std::visit(
      [&writer](const auto& value) { return writer(value); }, payload);
    if (!output) co_return codec::failure(output.error());
    if (auto ready = work.poll(detail::page_error(errc::success, c)); !ready) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        output->bytes = bytes::fragmented_buffer{};
        co_return codec::failure(ready.error());
    }
    co_return output;
}
} // namespace kwaque::storage
