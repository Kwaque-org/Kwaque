#include "src/storage/local_metadata_internal.h"

#include <algorithm>
#include <type_traits>

namespace kwaque::storage {
result<local_metadata_header> local_metadata_header::make(
  local_metadata_kind kind,
  local_store_context owner,
  local_publication_generation generation) noexcept {
    auto descriptor = local_metadata_descriptor_for(
      static_cast<std::uint16_t>(kind));
    if (!descriptor) return failure(descriptor.error());
    if (
      !generation.is_valid()
      || owner.store_wide() != (kind == local_metadata_kind::store_identity))
        return failure(errc::invalid_argument);
    if (
      (kind == local_metadata_kind::store_identity
       || kind == local_metadata_kind::wal_descriptor
       || kind == local_metadata_kind::segment_descriptor)
      && generation.value() != 1)
        return failure(errc::invalid_argument);
    return local_metadata_header{kind, owner, generation};
}
namespace detail {
result<byte_count>
local_payload_bytes(const local_metadata_payload& payload) noexcept {
    if (payload.valueless_by_exception())
        return failure(errc::invalid_argument);
    return std::visit(
      [](const auto& value) -> result<byte_count> {
          using T = std::remove_cvref_t<decltype(value)>;
          std::uint64_t fixed = 0, width = 0, count = 0, cap = 0;
          if constexpr (std::is_same_v<T, local_store_identity>)
              fixed = 16;
          else if constexpr (std::is_same_v<T, local_shard_control>)
              fixed = 48U + (value.checkpoint ? 60U : 0U)
                      + (value.wal_head ? 48U : 0U);
          else if constexpr (std::is_same_v<T, local_wal_descriptor>)
              fixed = 44U + (value.predecessor ? 24U : 0U);
          else if constexpr (std::is_same_v<T, local_segment_descriptor>)
              fixed = 120;
          else if constexpr (std::is_same_v<T, local_object_publication>) {
              fixed = 88U + (value.boundary ? 48U : 0U);
              width = 60;
              count = value.roots.size();
              cap = 4;
          } else if constexpr (std::is_same_v<T, local_checkpoint_root>) {
              fixed = 56;
              width = 48;
              count = value.pages.size();
              cap = maximum_object_pages;
          } else if constexpr (std::is_same_v<T, local_checkpoint_page>) {
              fixed = 20;
              width = 164;
              count = value.entries.size();
              cap = maximum_object_entries;
          } else if constexpr (std::is_same_v<T, local_deletion_intent>) {
              fixed = 96;
              width = 12;
              count = value.objects.size();
              cap = local_deletion_objects_max;
          } else if constexpr (std::is_same_v<T, local_recovery_decision>)
              fixed = 148;
          else if constexpr (std::is_same_v<T, local_boundary_evidence>)
              fixed = 188U + (value.retry ? 60U : 0U);
          else if constexpr (std::is_same_v<T, local_completed_retry_root>) {
              fixed = 128;
              width = 48;
              count = value.pages.size();
              cap = maximum_object_pages;
          } else if constexpr (std::is_same_v<T, local_completed_retry_page>) {
              fixed = 92;
              width = 160;
              count = value.entries.size();
              cap = maximum_object_entries;
          }
          if (count > cap) return failure(errc::resource_exhausted);
          return byte_count{fixed + width * count};
      },
      payload);
}
std::optional<segment_context>
local_payload_segment(const local_metadata_payload& payload) noexcept {
    if (payload.valueless_by_exception()) return std::nullopt;
    return std::visit(
      [](const auto& value) -> std::optional<segment_context> {
          if constexpr (requires { value.segment; })
              return value.segment;
          else
              return std::nullopt;
      },
      payload);
}
codec::result<void> check_local_root_counts(
  std::uint32_t total,
  std::size_t pages,
  const codec::limits& policy,
  codec::field_context c) {
    if (
      total > maximum_object_entries
      || total > policy.config().max_object_entries.value()
      || pages > maximum_object_pages
      || pages > policy.config().max_object_pages.value())
        return codec::failure(page_error(errc::resource_exhausted, c));
    if ((total == 0) != (pages == 0) || pages > total)
        return codec::failure(page_error(errc::malformed_data, c));
    return {};
}
codec::result<void> check_local_page_fields(
  local_publication_generation generation,
  local_object_sequence sequence,
  page_ordinal ordinal,
  std::uint32_t first,
  std::size_t count,
  const codec::limits& policy,
  codec::field_context c) {
    if (
      !sequence.is_valid() || count == 0 || (ordinal.value() == 0 && first != 0)
      || first < ordinal.value())
        return codec::failure(page_error(errc::malformed_data, c));
    if (sequence.value() != generation.value())
        return codec::failure(page_error(errc::wrong_context, c));
    if (
      ordinal.value() >= policy.config().max_object_pages.value()
      || first > maximum_object_entries
      || count > maximum_object_entries - first
      || first + count > policy.config().max_object_entries.value())
        return codec::failure(page_error(errc::resource_exhausted, c));
    return {};
}
namespace {
codec::result<void> scalar_code(
  std::uint64_t value, std::uint64_t maximum, codec::field_context c) {
    if (value == 0) return codec::failure(page_error(errc::malformed_data, c));
    if (value > maximum)
        return codec::failure(page_error(errc::unsupported_format, c));
    return {};
}
codec::result<void> root_slot(
  const local_root_reference& ref,
  local_root_kind kind,
  storage_alignment alignment,
  codec::field_context c) {
    if (ref.kind() != kind)
        return codec::failure(page_error(errc::wrong_context, c));
    return local_wire(ref.validate_alignment(alignment), c);
}
bool footer_matches(
  const local_root_reference& root,
  const local_footer_reference& footer) noexcept {
    return footer.family() == 7 && root.position() == footer.position()
           && root.bytes() == footer.bytes()
           && root.digest() == footer.digest();
}
} // namespace

// One shared validator supplies encoder and decoder body semantics. Each
// dynamic entry is admitted against the same cumulative work account.
seastar::future<codec::result<void>> validate_local_payload(
  local_metadata_header header,
  const local_metadata_payload& payload,
  aligned_envelope_layout layout,
  std::optional<storage_alignment> segment_alignment,
  std::optional<storage_alignment> data_metadata_alignment,
  codec::cooperative_work& work,
  codec::field_context c,
  bool writing) {
    const auto anchor = page_error(errc::success, c);
    if (
      auto ready = co_await work.admit(
        byte_count{1024}, item_count{64}, anchor);
      !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (
      payload.valueless_by_exception()
      || payload.index() + 1 != static_cast<std::uint16_t>(header.kind()))
        co_return codec::failure(page_error(errc::wrong_context, c));
    const auto bad = [&] {
        return codec::failure(page_error(errc::malformed_data, c));
    };
    const auto wrong = [&] {
        return codec::failure(page_error(errc::wrong_context, c));
    };
    if (
      auto sc = local_payload_segment(payload);
      sc && sc->cluster() != header.owner().cluster())
        co_return wrong();
    const auto data_alignment = segment_alignment.value_or(
      storage_alignment::make(byte_count{512}).value());
    if (const auto* value = std::get_if<local_store_identity>(&payload)) {
        if (auto code = scalar_code(value->layout_version, 1, c); !code)
            co_return code;
        const auto role = static_cast<std::uint16_t>(value->role);
        if (role == 0) co_return bad();
        if (role != 2 && role != 5 && role != 7)
            co_return codec::failure(page_error(errc::unsupported_format, c));
        if (value->shard_count == 0) co_return bad();
        if (value->metadata_alignment != layout.alignment()) co_return wrong();
    } else if (const auto* value = std::get_if<local_shard_control>(&payload)) {
        if (value->checkpoint) {
            if (
              auto valid = root_slot(
                *value->checkpoint,
                local_root_kind::checkpoint,
                layout.alignment(),
                c);
              !valid)
                co_return valid;
            if (
              value->checkpoint->sequence().value()
              > value->object_high.value())
                co_return bad();
        }
        if (value->wal_head) {
            auto high = local_wal_high::from_incarnation(
              value->wal_head->incarnation);
            if (!high || *high > value->wal_high) co_return bad();
        }
    } else if (
      const auto* value = std::get_if<local_wal_descriptor>(&payload)) {
        if (
          auto code = scalar_code(
            static_cast<std::uint16_t>(value->profile), 1, c);
          !code)
            co_return code;
        if (
          value->incarnation.is_nil()
          || value->data_start.value() != layout.encoded_bytes().value()
          || value->alignment != layout.alignment()
          || value->capacity_bytes.value() < value->data_start.value()
          || value->capacity_bytes.value() - value->data_start.value()
               < value->alignment.bytes().value()
          || value->capacity_bytes.value() % value->alignment.bytes().value()
               != 0)
            co_return bad();
        if (
          value->predecessor
          && !value->predecessor->incarnation().canonical_less(
            value->incarnation))
            co_return bad();
    } else if (
      const auto* value = std::get_if<local_segment_descriptor>(&payload)) {
        if (
          auto code = scalar_code(
            static_cast<std::uint16_t>(value->profile), 1, c);
          !code)
            co_return code;
        if (auto code = scalar_code(value->record_profile, 1, c); !code)
            co_return code;
        if (
          auto code = scalar_code(
            static_cast<std::uint8_t>(value->layout), 2, c);
          !code)
            co_return code;
        if (segment_alignment && value->alignment != *segment_alignment)
            co_return wrong();
        if (value->maximum_lifetime.nanoseconds() == 0 || value->maximum_data_bytes.value()
            < value->alignment.bytes().value() * (value->layout == local_layout_kind::initial ? 4U : 2U)
            || (value->layout == local_layout_kind::initial && value->physical_origin.value() != 0))
            co_return bad();
    } else if (
      const auto* value = std::get_if<local_object_publication>(&payload)) {
        if (
          auto code = scalar_code(
            static_cast<std::uint8_t>(value->state), 4, c);
          !code)
            co_return code;
        if (value->roots.size() > 4)
            co_return codec::failure(page_error(errc::resource_exhausted, c));
        if (!value->boundary && !value->roots.empty()) co_return bad();
        if (value->boundary) {
            if (
              auto valid = local_wire(
                value->boundary->validate_alignment(data_alignment), c);
              !valid)
                co_return valid;
            if (
              value->state == local_object_state::active
              && value->boundary->family() != 6)
                co_return bad();
        }
        std::uint16_t previous = 0;
        bool sealed_retry = false;
        for (const auto& root : value->roots) {
            if (
              auto ready = co_await work.admit(
                byte_count{1024}, item_count{64}, anchor);
              !ready)
                co_return codec::failure(ready.error());
            if (auto ready = work.poll(anchor); !ready)
                co_return codec::failure(ready.error());
            const auto code = static_cast<std::uint16_t>(root.kind());
            if (code != 1 && code != 2 && code != 5) co_return wrong();
            if (code <= previous) co_return bad();
            previous = code;
            const auto alignment
              = root.kind() == local_root_kind::completed_retry_snapshot
                  ? layout.alignment()
                  : data_alignment;
            if (
              auto valid = local_wire(root.validate_alignment(alignment), c);
              !valid)
                co_return valid;
            if (root.kind() == local_root_kind::sealed_retry) {
                if (!value->boundary || !footer_matches(root, *value->boundary))
                    co_return wrong();
                sealed_retry = true;
            }
        }
        if (
          value->state == local_object_state::sealed
          && (!value->boundary || value->boundary->family() != 7 || !sealed_retry))
            co_return bad();
    } else if (
      const auto* value = std::get_if<local_deletion_intent>(&payload)) {
        if (
          auto code = scalar_code(
            static_cast<std::uint16_t>(value->reason), 3, c);
          !code)
            co_return code;
        if (
          !value->owner_decision_id || !value->published_generation.is_valid()
          || value->objects.empty())
            co_return bad();
        std::uint16_t previous_kind = 0;
        std::uint64_t previous_sequence = 0;
        for (const auto& object : value->objects) {
            if (
              auto ready = co_await work.admit(
                byte_count{1024}, item_count{64}, anchor);
              !ready)
                co_return codec::failure(ready.error());
            if (auto ready = work.poll(anchor); !ready)
                co_return codec::failure(ready.error());
            const auto code = static_cast<std::uint16_t>(object.kind);
            if (auto valid = scalar_code(code, 4, c); !valid) co_return valid;
            if (
              (code == 4) != (object.sequence != 0) || code < previous_kind
              || (code == previous_kind && object.sequence <= previous_sequence))
                co_return bad();
            previous_kind = code;
            previous_sequence = object.sequence;
        }
    } else if (
      const auto* value = std::get_if<local_recovery_decision>(&payload)) {
        if (
          auto code = scalar_code(
            static_cast<std::uint16_t>(value->action), 3, c);
          !code)
            co_return code;
        if (
          !value->owner_decision_id
          || !data_alignment.aligned(value->target_position))
            co_return bad();
    } else if (
      const auto* value = std::get_if<local_boundary_evidence>(&payload)) {
        const bool other_device = value->data_device != header.owner().device();
        if ((writing && other_device && !data_metadata_alignment)
            || (!other_device && data_metadata_alignment
                && *data_metadata_alignment != layout.alignment()))
            co_return codec::failure(page_error(errc::invalid_argument, c));
        // A discovery probe has no independent device configuration yet.
        // It verifies only the common minimum; contextual decode requires it.
        const auto retry_metadata_alignment
          = other_device ? data_metadata_alignment.value_or(
                             storage_alignment::make(byte_count{512}).value())
                         : layout.alignment();
        if (
          value->data_device.is_nil()
          || value->covered.bytes().end() != value->footer.position())
            co_return bad();
        if (
          auto valid = local_wire(
            value->footer.validate_alignment(data_alignment), c);
          !valid)
            co_return valid;
        if (value->retry) {
            if (
              value->retry->kind() != local_root_kind::sealed_retry
              && value->retry->kind()
                   != local_root_kind::completed_retry_snapshot)
                co_return wrong();
            const auto alignment = value->retry->kind()
                                       == local_root_kind::sealed_retry
                                     ? data_alignment
                                     : retry_metadata_alignment;
            if (
              auto valid = local_wire(
                value->retry->validate_alignment(alignment), c);
              !valid)
                co_return valid;
            if (
              value->retry->kind() == local_root_kind::sealed_retry
              && !footer_matches(*value->retry, value->footer))
                co_return wrong();
        }
    }
    const auto* checkpoint_root = std::get_if<local_checkpoint_root>(&payload);
    const auto* retry_root = std::get_if<local_completed_retry_root>(&payload);
    if (checkpoint_root || retry_root) {
        const auto& pages = checkpoint_root ? checkpoint_root->pages
                                            : retry_root->pages;
        const auto total = checkpoint_root ? checkpoint_root->entry_count
                                           : retry_root->retry_count;
        if (
          auto valid = check_local_root_counts(
            total, pages.size(), work.policy(), c);
          !valid)
            co_return valid;
        if (checkpoint_root) {
            auto order = checkpoint_root->begin.compare(
              header.owner(), checkpoint_root->end, header.owner());
            if (!order || *order > 0 || ((*order == 0) != (total == 0)))
                co_return bad();
        } else if (
          auto valid = local_wire(
            retry_root->footer.validate_alignment(data_alignment), c);
          !valid)
            co_return valid;
        std::uint32_t first = 0, ordinal = 0;
        for (const auto& ref : pages) {
            if (
              auto ready = co_await work.admit(
                byte_count{1024}, item_count{64}, anchor);
              !ready)
                co_return codec::failure(ready.error());
            if (auto ready = work.poll(anchor); !ready)
                co_return codec::failure(ready.error());
            auto valid = check_page_ref(
              ref,
              ordinal++,
              first,
              checkpoint_root ? 92 : 164,
              checkpoint_root ? 164 : 160,
              layout.alignment(),
              work.policy(),
              c);
            if (!valid) co_return valid;
            first += ref.entry_count();
            if (first > total) co_return bad();
        }
        if (first != total) co_return bad();
    }
    const auto* checkpoint = std::get_if<local_checkpoint_page>(&payload);
    const auto* retries = std::get_if<local_completed_retry_page>(&payload);
    if (checkpoint || retries) {
        const auto sequence = checkpoint ? checkpoint->sequence
                                         : retries->sequence;
        const auto ordinal = checkpoint ? checkpoint->ordinal
                                        : retries->ordinal;
        const auto first = checkpoint ? checkpoint->first_entry
                                      : retries->first_entry;
        const auto count = checkpoint ? checkpoint->entries.size()
                                      : retries->entries.size();
        if (
          auto valid = check_local_page_fields(
            header.generation(),
            sequence,
            ordinal,
            first,
            count,
            work.policy(),
            c);
          !valid)
            co_return valid;
        std::optional<local_wal_cursor> previous;
        std::optional<model::batch_id> previous_id;
        for (std::size_t i = 0; i < count; ++i) {
            if (
              auto ready = co_await work.admit(
                byte_count{1024}, item_count{64}, anchor);
              !ready)
                co_return codec::failure(ready.error());
            if (auto ready = work.poll(anchor); !ready)
                co_return codec::failure(ready.error());
            if (checkpoint) {
                const auto& entry = checkpoint->entries[i];
                if (
                  writing
                  && entry.disposition
                       == local_checkpoint_disposition::preserved_candidate)
                    co_return codec::failure(
                      page_error(errc::unsupported_format, c));
                if (
                  auto code = scalar_code(
                    static_cast<std::uint16_t>(entry.disposition), 3, c);
                  !code)
                    co_return code;
                auto order = entry.begin.compare(
                  header.owner(), entry.end, header.owner());
                if (
                  !order || *order >= 0 || !entry.evidence_sequence
                  || entry.segment.cluster() != header.owner().cluster())
                    co_return bad();
                if (previous) {
                    auto gap = previous->compare(
                      header.owner(), entry.begin, header.owner());
                    if (!gap || *gap > 0) co_return bad();
                }
                previous = entry.end;
            } else {
                const auto& entry = retries->entries[i];
                if (
                  entry.original_binding().topic() != retries->segment.topic()
                  || entry.original_binding().range()
                       != retries->segment.range())
                    co_return wrong();
                if (
                  (previous_id && !previous_id->canonical_less(entry.id()))
                  || entry.returned_span().count().value()
                       > work.policy().config().max_original_records.value())
                    co_return bad();
                previous_id = entry.id();
            }
        }
    }
    co_return work.poll(anchor);
}

codec::result<void> validate_local_expectation(
  const local_metadata_expectation& e, codec::field_context c) {
    const auto kind = e.header.kind();
    const bool segment = kind == local_metadata_kind::segment_descriptor
                         || kind == local_metadata_kind::object_publication
                         || kind == local_metadata_kind::deletion_intent
                         || kind == local_metadata_kind::recovery_decision
                         || kind == local_metadata_kind::boundary_evidence
                         || kind == local_metadata_kind::completed_retry_root
                         || kind == local_metadata_kind::completed_retry_page;
    const bool page = kind == local_metadata_kind::checkpoint_page
                      || kind == local_metadata_kind::completed_retry_page;
    const bool pinned = page || kind == local_metadata_kind::wal_descriptor
                        || kind == local_metadata_kind::checkpoint_root
                        || kind == local_metadata_kind::recovery_decision
                        || kind == local_metadata_kind::boundary_evidence
                        || kind == local_metadata_kind::completed_retry_root;
    if (
      segment != e.segment.has_value()
      || segment != e.segment_alignment.has_value()
      || (kind == local_metadata_kind::wal_descriptor)
           != e.wal_incarnation.has_value()
      || (e.wal_incarnation && e.wal_incarnation->is_nil())
      || (kind == local_metadata_kind::boundary_evidence)
           != e.data_device.has_value()
      || (e.data_device && e.data_device->is_nil())
      || (e.data_device && *e.data_device != e.header.owner().device() && !e.data_metadata_alignment)
      || (e.data_metadata_alignment && !e.data_device)
      || (e.data_device && *e.data_device == e.header.owner().device() && e.data_metadata_alignment && *e.data_metadata_alignment != e.alignment)
      || e.digest.has_value() != e.encoded_bytes.has_value()
      || (pinned && !e.digest) || page != e.page.has_value()
      || (e.previous_retry && kind != local_metadata_kind::completed_retry_page)
      || (e.previous_checkpoint_end && kind != local_metadata_kind::checkpoint_page))
        return codec::failure(page_error(errc::invalid_argument, c));
    if (e.segment && e.segment->cluster() != e.header.owner().cluster())
        return codec::failure(page_error(errc::invalid_argument, c));
    if (e.encoded_bytes && (e.encoded_bytes->value() < 32 || *e.encoded_bytes > local_metadata_max_bytes
        || e.encoded_bytes->value() % e.alignment.bytes().value() != 0))
        return codec::failure(page_error(errc::invalid_argument, c));
    if (e.page && (e.page->digest() != *e.digest || e.page->encoded_bytes() != *e.encoded_bytes
        || (e.page->ordinal().value() == 0 && (e.previous_retry || e.previous_checkpoint_end))))
        return codec::failure(page_error(errc::invalid_argument, c));
    return {};
}
codec::result<void> match_local_expectation(
  const local_metadata_expectation& e,
  local_metadata_header header,
  const local_metadata_payload& payload,
  byte_count encoded_bytes,
  codec::field_context c) {
    const auto wrong = [&] {
        return codec::failure(page_error(errc::wrong_context, c));
    };
    if (
      header != e.header
      || (e.encoded_bytes && *e.encoded_bytes != encoded_bytes))
        return wrong();
    if (e.segment != local_payload_segment(payload)) return wrong();
    if (
      e.wal_incarnation
      && std::get<local_wal_descriptor>(payload).incarnation
           != *e.wal_incarnation)
        return wrong();
    if (
      e.data_device
      && std::get<local_boundary_evidence>(payload).data_device
           != *e.data_device)
        return wrong();
    if (e.page) {
        if (header.kind() == local_metadata_kind::completed_retry_page) {
            const auto& retry = std::get<local_completed_retry_page>(payload);
            if (
              retry.ordinal != e.page->ordinal()
              || retry.first_entry != e.page->first_entry()
              || retry.entries.size() != e.page->entry_count())
                return wrong();
            if (
              e.previous_retry
              && !e.previous_retry->canonical_less(retry.entries.front().id()))
                return codec::failure(page_error(errc::malformed_data, c));
        } else {
            const auto& checkpoint = std::get<local_checkpoint_page>(payload);
            if (
              checkpoint.ordinal != e.page->ordinal()
              || checkpoint.first_entry != e.page->first_entry()
              || checkpoint.entries.size() != e.page->entry_count())
                return wrong();
            if (e.previous_checkpoint_end) {
                const auto order = e.previous_checkpoint_end->compare(
                  header.owner(),
                  checkpoint.entries.front().begin,
                  header.owner());
                if (!order || *order > 0)
                    return codec::failure(page_error(errc::malformed_data, c));
            }
        }
    }
    return {};
}
codec::result<local_root_reference>
read_local_root(const std::array<char, 60>& raw, codec::field_context c) {
    if (load<2, std::uint16_t>(raw) != 0)
        return codec::failure(page_error(errc::malformed_data, c, 2));
    auto kind = local_wire(
      parse_local_root_kind(load<0, std::uint16_t>(raw)), c);
    auto sequence = local_wire(
      local_object_sequence::make(load<4, std::uint64_t>(raw)), c, 4);
    auto pages = local_wire(
      page_count::make(load<24, std::uint32_t>(raw)), c, 24);
    if (!kind) return codec::failure(kind.error());
    if (!sequence) return codec::failure(sequence.error());
    if (!pages) return codec::failure(pages.error());
    return local_wire(
      local_root_reference::make(
        *kind,
        *sequence,
        runtime::file_position{load<12, std::uint64_t>(raw)},
        byte_count{load<20, std::uint32_t>(raw)},
        *pages,
        codec::immutable_object_digest{read_digest<28>(raw)}),
      c);
}
void write_local_root(
  std::array<char, 60>& out, const local_root_reference& ref) noexcept {
    store<0>(out, static_cast<std::uint16_t>(ref.kind()));
    store<4>(out, ref.sequence().value());
    store<12>(out, ref.position().value());
    store<20>(out, static_cast<std::uint32_t>(ref.bytes().value()));
    store<24>(out, ref.pages().value());
    write_digest<28>(out, ref.digest());
}
codec::result<local_checkpoint_entry> read_local_checkpoint_entry(
  const std::array<char, 164>& raw, codec::field_context c) {
    const auto begin = read_local_cursor<0>(raw, c);
    const auto end = read_local_cursor<24>(raw, c);
    const auto segment = read_local_segment<48>(raw, c);
    if (!begin) return codec::failure(begin.error());
    if (!end) return codec::failure(end.error());
    if (!segment) return codec::failure(segment.error());
    if (load<122, std::uint16_t>(raw) != 0)
        return codec::failure(page_error(errc::malformed_data, c, 122));
    return local_checkpoint_entry{
      *begin,
      *end,
      *segment,
      static_cast<local_checkpoint_disposition>(load<120, std::uint16_t>(raw)),
      load<124, std::uint64_t>(raw),
      codec::immutable_object_digest{read_digest<132>(raw)}};
}
void write_local_checkpoint_entry(
  std::array<char, 164>& out, const local_checkpoint_entry& entry) noexcept {
    write_local_cursor<0>(out, entry.begin);
    write_local_cursor<24>(out, entry.end);
    write_sc<48>(out, entry.segment);
    store<120>(out, static_cast<std::uint16_t>(entry.disposition));
    store<124>(out, entry.evidence_sequence);
    write_digest<132>(out, entry.evidence_digest);
}
codec::result<local_deletion_object> read_local_deletion_object(
  const std::array<char, 12>& raw, codec::field_context c) {
    if (load<2, std::uint16_t>(raw) != 0)
        return codec::failure(page_error(errc::malformed_data, c, 2));
    return local_deletion_object{
      static_cast<local_deletion_kind>(load<0, std::uint16_t>(raw)),
      load<4, std::uint64_t>(raw)};
}
void write_local_deletion_object(
  std::array<char, 12>& out, const local_deletion_object& object) noexcept {
    store<0>(out, static_cast<std::uint16_t>(object.kind));
    store<4>(out, object.sequence);
}
} // namespace detail
} // namespace kwaque::storage
