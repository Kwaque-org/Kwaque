#include "src/storage/local_metadata_internal.h"

#include <algorithm>
#include <array>
#include <exception>

namespace kwaque::storage {
namespace detail {
class local_metadata_codec final {
public:
    static local_metadata_record make(
      local_metadata_header header,
      local_metadata_payload&& payload,
      byte_count bytes) noexcept {
        return local_metadata_record{header, std::move(payload), bytes};
    }
};
} // namespace detail
namespace {
using detail::load;
using detail::local_wire;
using detail::page_error;
using detail::page_wire;
template<std::size_t Offset, std::size_t N>
codec::result<bool>
presence(const std::array<char, N>& raw, codec::field_context c) {
    static_assert(Offset + 4 <= N);
    if (
      static_cast<unsigned char>(raw[Offset]) > 1 || raw[Offset + 1] != 0
      || raw[Offset + 2] != 0 || raw[Offset + 3] != 0)
        return codec::failure(page_error(errc::malformed_data, c, Offset));
    return raw[Offset] != 0;
}
class payload_reader final {
public:
    payload_reader(
      bytes::fragmented_buffer_parser& input,
      codec::cooperative_work& work,
      codec::field_context context,
      codec::decode_budget memory,
      std::uint64_t end,
      std::optional<model::batch_id> previous)
      : input_(input)
      , work_(work)
      , context_(context)
      , memory_(memory)
      , end_(end)
      , previous_retry_(previous) {}
    [[nodiscard]] codec::field_context here() const noexcept {
        auto c = context_;
        c.origin += input_.bytes_consumed().value();
        return c;
    }
    template<std::size_t N>
    seastar::future<codec::result<std::array<char, N>>> read() {
        const auto c = here();
        if (
          input_.bytes_consumed().value() > end_
          || N > end_ - input_.bytes_consumed().value())
            co_return codec::failure(page_error(errc::malformed_data, c));
        std::array<char, N> raw{};
        if (
          auto result = co_await detail::read_fixed(input_, raw, work_, c);
          !result)
            co_return codec::failure(result.error());
        const auto anchor = page_error(errc::success, c);
        if (
          auto ready = co_await work_.admit(
            byte_count{1024}, item_count{64}, anchor);
          !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work_.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        co_return raw;
    }
    template<std::size_t Width, typename T, typename Read>
    seastar::future<codec::result<void>> entries(
      std::vector<T>& values,
      std::uint32_t count,
      std::uint32_t maximum,
      Read decode) {
        const auto c = here();
        if (count > maximum)
            co_return codec::failure(page_error(errc::resource_exhausted, c));
        if (
          input_.bytes_consumed().value() > end_
          || count > (end_ - input_.bytes_consumed().value()) / Width)
            co_return codec::failure(page_error(errc::malformed_data, c));
        const auto remaining = detail::reserve_entries(
          values, count, memory_, work_.policy(), c);
        if (!remaining) co_return codec::failure(remaining.error());
        memory_ = *remaining;
        for (std::uint32_t i = 0; i < count; ++i) {
            const auto field = here();
            auto raw = co_await read<Width>();
            if (!raw) co_return codec::failure(raw.error());
            auto value = decode(*raw, field);
            if (!value) co_return codec::failure(value.error());
            values.push_back(std::move(*value));
        }
        co_return codec::result<void>{};
    }
    seastar::future<codec::result<void>> decode(
      local_metadata_header header,
      const local_metadata_expectation* expected) {
        const auto c = here();
        const auto matches_page =
          [expected, c](
            page_ordinal ordinal,
            std::uint32_t first,
            std::uint32_t count) -> codec::result<void> {
            if (expected && expected->page && (ordinal != expected->page->ordinal()
                || first != expected->page->first_entry() || count != expected->page->entry_count()))
                return codec::failure(page_error(errc::wrong_context, c));
            return {};
        };
        const auto matches_segment =
          [expected, header, c](
            segment_context segment) -> codec::
                                       result<void> {
                                           if (segment.cluster() != header.owner().cluster()
                || (expected && expected->segment && segment != *expected->segment))
                                               return codec::failure(page_error(
                                                 errc::wrong_context, c));
                                           return {};
                                       };
        switch (header.kind()) {
        case local_metadata_kind::store_identity: {
            auto raw = co_await read<16>();
            if (!raw) co_return codec::failure(raw.error());
            auto alignment = page_wire(
              storage_alignment::make(byte_count{load<2, std::uint32_t>(*raw)}),
              c,
              2);
            if (!alignment) co_return codec::failure(alignment.error());
            if (load<12, std::uint32_t>(*raw) != 0)
                co_return codec::failure(
                  page_error(errc::malformed_data, c, 12));
            value_.emplace(
              local_store_identity{
                load<0, std::uint16_t>(*raw),
                *alignment,
                load<6, std::uint32_t>(*raw),
                static_cast<local_device_role>(load<10, std::uint16_t>(*raw))});
            break;
        }
        case local_metadata_kind::shard_control: {
            auto raw = co_await read<40>();
            if (!raw) co_return codec::failure(raw.error());
            std::array<std::uint8_t, 16> bytes{};
            std::copy_n(raw->begin(), 16, bytes.begin());
            std::optional<local_root_reference> checkpoint;
            std::optional<local_wal_head> head;
            auto flag_context = here();
            auto flag = co_await read<4>();
            if (!flag) co_return codec::failure(flag.error());
            auto present = presence<0>(*flag, flag_context);
            if (!present) co_return codec::failure(present.error());
            if (*present) {
                const auto field = here();
                auto root = co_await read<60>();
                if (!root) co_return codec::failure(root.error());
                auto decoded = detail::read_local_root(*root, field);
                if (!decoded) co_return codec::failure(decoded.error());
                checkpoint = *decoded;
            }
            flag_context = here();
            flag = co_await read<4>();
            if (!flag) co_return codec::failure(flag.error());
            present = presence<0>(*flag, flag_context);
            if (!present) co_return codec::failure(present.error());
            if (*present) {
                const auto field = here();
                auto raw_head = co_await read<48>();
                if (!raw_head) co_return codec::failure(raw_head.error());
                auto id = detail::read_id<0, model::wal_incarnation_id>(
                  *raw_head, field);
                if (!id) co_return codec::failure(id.error());
                head = local_wal_head{
                  *id,
                  codec::immutable_object_digest{
                    detail::read_digest<16>(*raw_head)}};
            }
            value_.emplace(
              local_shard_control{
                local_wal_high::make(bytes).value(),
                local_object_high{load<16, std::uint64_t>(*raw)},
                local_decision_high{load<24, std::uint64_t>(*raw)},
                local_deletion_high{load<32, std::uint64_t>(*raw)},
                checkpoint,
                head});
            break;
        }
        case local_metadata_kind::wal_descriptor: {
            auto raw = co_await read<20>();
            if (!raw) co_return codec::failure(raw.error());
            auto id = detail::read_id<0, model::wal_incarnation_id>(*raw, c);
            if (!id) co_return codec::failure(id.error());
            auto present = presence<16>(*raw, c);
            if (!present) co_return codec::failure(present.error());
            std::optional<local_wal_cursor> predecessor;
            if (*present) {
                const auto field = here();
                auto cursor = co_await read<24>();
                if (!cursor) co_return codec::failure(cursor.error());
                auto decoded = detail::read_local_cursor<0>(*cursor, field);
                if (!decoded) co_return codec::failure(decoded.error());
                predecessor = *decoded;
            }
            const auto field = here();
            auto tail = co_await read<24>();
            if (!tail) co_return codec::failure(tail.error());
            auto alignment = page_wire(
              storage_alignment::make(
                byte_count{load<0, std::uint32_t>(*tail)}),
              field);
            if (!alignment) co_return codec::failure(alignment.error());
            if (load<6, std::uint16_t>(*tail) != 0)
                co_return codec::failure(
                  page_error(errc::malformed_data, field, 6));
            value_.emplace(
              local_wal_descriptor{
                *id,
                predecessor,
                *alignment,
                static_cast<replay_profile>(load<4, std::uint16_t>(*tail)),
                runtime::file_position{load<8, std::uint64_t>(*tail)},
                byte_count{load<16, std::uint64_t>(*tail)}});
            break;
        }
        case local_metadata_kind::segment_descriptor: {
            auto raw = co_await read<120>();
            if (!raw) co_return codec::failure(raw.error());
            auto sc = detail::read_local_segment<0>(*raw, c);
            if (!sc) co_return codec::failure(sc.error());
            if (auto valid = matches_segment(*sc); !valid) co_return valid;
            auto alignment = page_wire(
              storage_alignment::make(
                byte_count{load<88, std::uint32_t>(*raw)}),
              c,
              88);
            if (!alignment) co_return codec::failure(alignment.error());
            if (!std::all_of(raw->begin() + 97, raw->begin() + 104, [](char b) {
                    return b == 0;
                }))
                co_return codec::failure(
                  page_error(errc::malformed_data, c, 97));
            value_.emplace(
              local_segment_descriptor{
                *sc,
                model::range_logical_end{load<72, std::uint64_t>(*raw)},
                model::segment_relative_end{load<80, std::uint64_t>(*raw)},
                *alignment,
                static_cast<storage_profile>(load<92, std::uint16_t>(*raw)),
                load<94, std::uint16_t>(*raw),
                static_cast<local_layout_kind>(load<96, std::uint8_t>(*raw)),
                byte_count{load<104, std::uint64_t>(*raw)},
                runtime::monotonic_duration{load<112, std::uint64_t>(*raw)}});
            break;
        }
        case local_metadata_kind::object_publication: {
            auto raw = co_await read<84>();
            if (!raw) co_return codec::failure(raw.error());
            auto sc = detail::read_local_segment<0>(*raw, c);
            if (!sc) co_return codec::failure(sc.error());
            if (auto valid = matches_segment(*sc); !valid) co_return valid;
            if (!std::all_of(raw->begin() + 73, raw->begin() + 80, [](char b) {
                    return b == 0;
                }))
                co_return codec::failure(
                  page_error(errc::malformed_data, c, 73));
            auto present = presence<80>(*raw, c);
            if (!present) co_return codec::failure(present.error());
            std::optional<local_footer_reference> boundary;
            if (*present) {
                const auto field = here();
                auto footer = co_await read<48>();
                if (!footer) co_return codec::failure(footer.error());
                auto decoded = detail::read_local_footer<0>(*footer, field);
                if (!decoded) co_return codec::failure(decoded.error());
                boundary = *decoded;
            }
            auto count = co_await read<4>();
            if (!count) co_return codec::failure(count.error());
            value_.emplace(
              local_object_publication{
                *sc,
                static_cast<local_object_state>(load<72, std::uint8_t>(*raw)),
                boundary,
                {}});
            co_return co_await entries<60>(
              std::get<local_object_publication>(*value_).roots,
              load<0, std::uint32_t>(*count),
              4,
              detail::read_local_root);
        }
        case local_metadata_kind::checkpoint_root: {
            auto raw = co_await read<56>();
            if (!raw) co_return codec::failure(raw.error());
            auto begin = detail::read_local_cursor<0>(*raw, c);
            if (!begin) co_return codec::failure(begin.error());
            auto end = detail::read_local_cursor<24>(*raw, c);
            if (!end) co_return codec::failure(end.error());
            if (
              auto valid = detail::check_local_root_counts(
                load<48, std::uint32_t>(*raw),
                load<52, std::uint32_t>(*raw),
                work_.policy(),
                c);
              !valid)
                co_return valid;
            value_.emplace(
              local_checkpoint_root{
                *begin, *end, load<48, std::uint32_t>(*raw), {}});
            co_return co_await entries<48>(
              std::get<local_checkpoint_root>(*value_).pages,
              load<52, std::uint32_t>(*raw),
              static_cast<std::uint32_t>(std::min<std::uint64_t>(
                maximum_object_pages,
                work_.policy().config().max_object_pages.value())),
              detail::read_page_ref);
        }
        case local_metadata_kind::checkpoint_page: {
            auto raw = co_await read<20>();
            if (!raw) co_return codec::failure(raw.error());
            auto sequence = page_wire(
              local_object_sequence::make(load<0, std::uint64_t>(*raw)), c);
            auto ordinal = page_wire(
              page_ordinal::make(load<8, std::uint32_t>(*raw)), c, 8);
            if (!sequence) co_return codec::failure(sequence.error());
            if (!ordinal) co_return codec::failure(ordinal.error());
            if (
              auto valid = detail::check_local_page_fields(
                header.generation(),
                *sequence,
                *ordinal,
                load<12, std::uint32_t>(*raw),
                load<16, std::uint32_t>(*raw),
                work_.policy(),
                c);
              !valid)
                co_return valid;
            if (
              auto valid = matches_page(
                *ordinal,
                load<12, std::uint32_t>(*raw),
                load<16, std::uint32_t>(*raw));
              !valid)
                co_return valid;
            value_.emplace(
              local_checkpoint_page{
                *sequence, *ordinal, load<12, std::uint32_t>(*raw), {}});
            co_return co_await entries<164>(
              std::get<local_checkpoint_page>(*value_).entries,
              load<16, std::uint32_t>(*raw),
              static_cast<std::uint32_t>(std::min<std::uint64_t>(
                maximum_object_entries,
                work_.policy().config().max_object_entries.value())),
              detail::read_local_checkpoint_entry);
        }
        case local_metadata_kind::deletion_intent: {
            auto raw = co_await read<96>();
            if (!raw) co_return codec::failure(raw.error());
            auto sc = detail::read_local_segment<0>(*raw, c);
            if (!sc) co_return codec::failure(sc.error());
            if (auto valid = matches_segment(*sc); !valid) co_return valid;
            auto generation = page_wire(
              local_publication_generation::make(load<80, std::uint64_t>(*raw)),
              c,
              80);
            if (!generation) co_return codec::failure(generation.error());
            if (load<90, std::uint16_t>(*raw) != 0)
                co_return codec::failure(
                  page_error(errc::malformed_data, c, 90));
            value_.emplace(
              local_deletion_intent{
                *sc,
                load<72, std::uint64_t>(*raw),
                *generation,
                static_cast<local_deletion_reason>(
                  load<88, std::uint16_t>(*raw)),
                {}});
            co_return co_await entries<12>(
              std::get<local_deletion_intent>(*value_).objects,
              load<92, std::uint32_t>(*raw),
              local_deletion_objects_max,
              detail::read_local_deletion_object);
        }
        case local_metadata_kind::recovery_decision: {
            auto raw = co_await read<148>();
            if (!raw) co_return codec::failure(raw.error());
            auto cursor = detail::read_local_cursor<0>(*raw, c);
            if (!cursor) co_return codec::failure(cursor.error());
            auto sc = detail::read_local_segment<56>(*raw, c);
            if (!sc) co_return codec::failure(sc.error());
            if (auto valid = matches_segment(*sc); !valid) co_return valid;
            if (load<138, std::uint16_t>(*raw) != 0)
                co_return codec::failure(
                  page_error(errc::malformed_data, c, 138));
            value_.emplace(
              local_recovery_decision{
                *cursor,
                codec::immutable_object_digest{detail::read_digest<24>(*raw)},
                *sc,
                runtime::file_position{load<128, std::uint64_t>(*raw)},
                static_cast<local_recovery_action>(
                  load<136, std::uint16_t>(*raw)),
                load<140, std::uint64_t>(*raw)});
            break;
        }
        case local_metadata_kind::boundary_evidence: {
            auto raw = co_await read<188>();
            if (!raw) co_return codec::failure(raw.error());
            auto sc = detail::read_local_segment<0>(*raw, c);
            if (!sc) co_return codec::failure(sc.error());
            if (auto valid = matches_segment(*sc); !valid) co_return valid;
            auto device = detail::read_id<72, device_store_id>(*raw, c);
            if (!device) co_return codec::failure(device.error());
            auto footer = detail::read_local_footer<88>(*raw, c);
            if (!footer) co_return codec::failure(footer.error());
            auto coverage = detail::read_coverage<136>(*raw, c);
            if (!coverage) co_return codec::failure(coverage.error());
            auto present = presence<184>(*raw, c);
            if (!present) co_return codec::failure(present.error());
            std::optional<local_root_reference> retry;
            if (*present) {
                const auto field = here();
                auto root = co_await read<60>();
                if (!root) co_return codec::failure(root.error());
                auto decoded = detail::read_local_root(*root, field);
                if (!decoded) co_return codec::failure(decoded.error());
                retry = *decoded;
            }
            value_.emplace(
              local_boundary_evidence{*sc, *device, *footer, *coverage, retry});
            break;
        }
        case local_metadata_kind::completed_retry_root: {
            auto raw = co_await read<128>();
            if (!raw) co_return codec::failure(raw.error());
            auto sc = detail::read_local_segment<0>(*raw, c);
            if (!sc) co_return codec::failure(sc.error());
            if (auto valid = matches_segment(*sc); !valid) co_return valid;
            auto footer = detail::read_local_footer<72>(*raw, c);
            if (!footer) co_return codec::failure(footer.error());
            if (
              auto valid = detail::check_local_root_counts(
                load<120, std::uint32_t>(*raw),
                load<124, std::uint32_t>(*raw),
                work_.policy(),
                c);
              !valid)
                co_return valid;
            value_.emplace(
              local_completed_retry_root{
                *sc, *footer, load<120, std::uint32_t>(*raw), {}});
            co_return co_await entries<48>(
              std::get<local_completed_retry_root>(*value_).pages,
              load<124, std::uint32_t>(*raw),
              static_cast<std::uint32_t>(std::min<std::uint64_t>(
                maximum_object_pages,
                work_.policy().config().max_object_pages.value())),
              detail::read_page_ref);
        }
        case local_metadata_kind::completed_retry_page: {
            auto raw = co_await read<92>();
            if (!raw) co_return codec::failure(raw.error());
            auto sc = detail::read_local_segment<0>(*raw, c);
            if (!sc) co_return codec::failure(sc.error());
            if (auto valid = matches_segment(*sc); !valid) co_return valid;
            auto sequence = page_wire(
              local_object_sequence::make(load<72, std::uint64_t>(*raw)),
              c,
              72);
            auto ordinal = page_wire(
              page_ordinal::make(load<80, std::uint32_t>(*raw)), c, 80);
            if (!sequence) co_return codec::failure(sequence.error());
            if (!ordinal) co_return codec::failure(ordinal.error());
            if (
              auto valid = detail::check_local_page_fields(
                header.generation(),
                *sequence,
                *ordinal,
                load<84, std::uint32_t>(*raw),
                load<88, std::uint32_t>(*raw),
                work_.policy(),
                c);
              !valid)
                co_return valid;
            if (
              auto valid = matches_page(
                *ordinal,
                load<84, std::uint32_t>(*raw),
                load<88, std::uint32_t>(*raw));
              !valid)
                co_return valid;
            value_.emplace(
              local_completed_retry_page{
                *sc, *sequence, *ordinal, load<84, std::uint32_t>(*raw), {}});
            co_return co_await entries<160>(
              std::get<local_completed_retry_page>(*value_).entries,
              load<88, std::uint32_t>(*raw),
              static_cast<std::uint32_t>(std::min<std::uint64_t>(
                maximum_object_entries,
                work_.policy().config().max_object_entries.value())),
              [segment = *sc, previous = previous_retry_](
                const std::array<char, 160>& entry,
                codec::field_context field) mutable {
                  auto decoded = detail::read_retry_entry(
                    entry, segment, field, previous);
                  if (decoded) previous = decoded->id();
                  return decoded;
              });
        }
        }
        co_return codec::result<void>{};
    }
    std::optional<local_metadata_payload> value_;
    [[nodiscard]] codec::decode_budget remaining() const noexcept {
        return memory_;
    }

private:
    bytes::fragmented_buffer_parser& input_;
    codec::cooperative_work& work_;
    codec::field_context context_;
    codec::decode_budget memory_;
    std::uint64_t end_;
    std::optional<model::batch_id> previous_retry_;
};
struct wal_descriptor_expectation final {
    local_metadata_header header;
    model::wal_incarnation_id incarnation;
};
struct body_reader final {
    std::optional<local_metadata_expectation> expected;
    std::optional<storage_alignment> alignment;
    std::uint64_t start;
    codec::decode_budget original;
    bool selected_mutable_generation{false};
    std::optional<wal_descriptor_expectation> wal_expected = std::nullopt;
    seastar::future<codec::result<decoded_local_metadata>> operator()(
      bytes::fragmented_buffer_parser& input,
      codec::field_context c,
      codec::input_boundary,
      codec::decode_budget memory,
      codec::cooperative_work& work) {
        const auto anchor = page_error(errc::success, c);
        std::array<char, 72> fixed{};
        if (
          auto read = co_await detail::read_fixed(input, fixed, work, c); !read)
            co_return codec::failure(read.error());
        if (
          auto ready = co_await work.admit(
            byte_count{1024}, item_count{64}, anchor);
          !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        if (load<2, std::uint16_t>(fixed) != 0)
            co_return codec::failure(page_error(errc::malformed_data, c, 2));
        const auto descriptor = local_wire(
          local_metadata_descriptor_for(load<0, std::uint16_t>(fixed)), c);
        const auto cluster = detail::read_id<4, model::cluster_id>(fixed, c);
        const auto broker = detail::read_id<20, model::broker_id>(fixed, c);
        const auto device = detail::read_id<36, device_store_id>(fixed, c);
        const auto generation = page_wire(
          local_publication_generation::make(load<56, std::uint64_t>(fixed)),
          c,
          56);
        if (!descriptor) co_return codec::failure(descriptor.error());
        if (!cluster) co_return codec::failure(cluster.error());
        if (!broker) co_return codec::failure(broker.error());
        if (!device) co_return codec::failure(device.error());
        if (!generation) co_return codec::failure(generation.error());
        const auto owner
          = local_store_context::make(
              *cluster, *broker, *device, load<52, std::uint32_t>(fixed))
              .value();
        const auto header = local_wire(
          local_metadata_header::make(descriptor->kind, owner, *generation), c);
        if (!header) co_return codec::failure(header.error());
        if (expected && selected_mutable_generation) {
            if (
              header->kind() != expected->header.kind()
              || header->owner() != expected->header.owner())
                co_return codec::failure(page_error(errc::wrong_context, c));
            expected->header = *header;
        }
        if (expected && expected->header != *header)
            co_return codec::failure(page_error(errc::wrong_context, c));
        if (wal_expected && wal_expected->header != *header)
            co_return codec::failure(page_error(errc::wrong_context, c));
        const auto payload_bytes = load<64, std::uint32_t>(fixed);
        const bool stored_wal_alignment
          = header->kind() == local_metadata_kind::wal_descriptor && !expected;
        std::optional<aligned_envelope_layout> layout;
        const auto check_layout =
          [&](storage_alignment selected) -> codec::result<void> {
            auto value = local_wire(
              local_metadata_layout(
                descriptor->kind,
                byte_count{payload_bytes},
                byte_count{c.origin - start},
                selected,
                work.policy()),
              c,
              64);
            if (!value) return codec::failure(value.error());
            if (
              input.total_bytes() != value->body_bytes()
              || load<68, std::uint32_t>(fixed)
                   != value->padding_bytes().value())
                return codec::failure(page_error(errc::malformed_data, c, 68));
            layout = *value;
            return {};
        };
        if (stored_wal_alignment) {
            // Fixed scalar payload only; no count-driven allocation precedes
            // geometry validation. The envelope already checked integrity.
            if (payload_bytes != 44 && payload_bytes != 68)
                co_return codec::failure(
                  page_error(errc::malformed_data, c, 64));
        } else {
            if (auto valid = check_layout(alignment.value()); !valid)
                co_return codec::failure(valid.error());
        }
        payload_reader reader{
          input,
          work,
          c,
          memory,
          local_metadata_prefix_bytes.value() + payload_bytes,
          expected ? expected->previous_retry : std::nullopt};
        std::optional<codec::error> failed;
        std::exception_ptr exception;
        try {
            do {
                auto decoded = co_await reader.decode(
                  *header, expected ? &*expected : nullptr);
                if (!decoded) {
                    failed = decoded.error();
                    break;
                }
                if (stored_wal_alignment) {
                    const auto& wal = std::get<local_wal_descriptor>(
                      *reader.value_);
                    if (
                      wal_expected
                      && wal.incarnation != wal_expected->incarnation) {
                        failed = page_error(errc::wrong_context, c);
                        break;
                    }
                    if (auto valid = check_layout(wal.alignment); !valid) {
                        failed = valid.error();
                        break;
                    }
                }
                if (
                  input.bytes_consumed().value()
                  != local_metadata_prefix_bytes.value() + payload_bytes) {
                    failed = page_error(errc::malformed_data, c, 64);
                    break;
                }
                if (
                  auto valid = co_await detail::validate_local_payload(
                    *header,
                    *reader.value_,
                    *layout,
                    expected ? expected->segment_alignment : std::nullopt,
                    expected ? expected->data_metadata_alignment : std::nullopt,
                    work,
                    c);
                  !valid) {
                    failed = valid.error();
                    break;
                }
                if (expected) {
                    auto matched = detail::match_local_expectation(
                      *expected,
                      *header,
                      *reader.value_,
                      layout->encoded_bytes(),
                      c);
                    if (!matched) {
                        failed = matched.error();
                        break;
                    }
                }
                if (
                  auto padding = co_await detail::read_padding(
                    input, layout->padding_bytes(), work, c);
                  !padding) {
                    failed = padding.error();
                    break;
                }
            } while (false);
        } catch (...) {
            exception = std::current_exception();
        }
        if (!failed && !exception) {
            if (auto ready = work.poll(anchor); !ready) failed = ready.error();
        }
        if (failed || exception) {
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            reader.value_.reset();
            if (exception) std::rethrow_exception(exception);
            co_return codec::failure(*failed);
        }
        const byte_count retained{
          memory.metadata_remaining.value()
          - reader.remaining().metadata_remaining.value()};
        const auto residual = codec::detail::consume_decode_budget(
          work.policy(), original, {}, retained, c, c.origin);
        KWAQUE_INVARIANT(
          invariant_id{"KQ-LOCAL-METADATA-RESIDUAL"},
          residual.has_value(),
          "admitted metadata exceeded its enclosing reservation");
        co_return decoded_local_metadata{
          detail::local_metadata_codec::make(
            *header, std::move(*reader.value_), layout->encoded_bytes()),
          *residual};
    }
};
} // namespace

seastar::future<codec::result<decoded_local_metadata>> decode_local_metadata(
  bytes::fragmented_buffer_parser& input,
  local_metadata_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context c,
  codec::input_boundary boundary) {
    c.family = static_cast<std::uint16_t>(detail::local_family);
    if (auto valid = detail::validate_local_expectation(expected, c); !valid)
        return seastar::make_ready_future<
          codec::result<decoded_local_metadata>>(codec::failure(valid.error()));
    const auto start = codec::detail::integer_read_start(input, c, boundary);
    if (!start)
        return seastar::make_ready_future<
          codec::result<decoded_local_metadata>>(codec::failure(start.error()));
    const auto alignment = expected.alignment;
    const auto digest = expected.digest;
    const auto exact = expected.encoded_bytes;
    body_reader reader{std::move(expected), alignment, *start, memory};
    if (digest)
        return detail::decode_pinned<decoded_local_metadata>(
          input,
          detail::local_family,
          *digest,
          memory,
          work,
          std::move(reader),
          c,
          boundary,
          exact);
    return codec::decode_envelope<decoded_local_metadata>(
      input,
      detail::local_family,
      {local_metadata_max_bytes, local_metadata_max_bytes},
      memory,
      work,
      std::move(reader),
      c,
      boundary);
}
seastar::future<codec::result<decoded_local_metadata>>
decode_selected_shard_control(
  bytes::fragmented_buffer_parser& input,
  local_store_context owner,
  storage_alignment alignment,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context c,
  codec::input_boundary boundary) {
    c.family = static_cast<std::uint16_t>(detail::local_family);
    const auto header = local_metadata_header::make(
      local_metadata_kind::shard_control,
      owner,
      local_publication_generation::make(1).value());
    if (!header)
        return seastar::make_ready_future<
          codec::result<decoded_local_metadata>>(
          codec::failure(detail::page_error(errc::invalid_argument, c)));
    const auto start = codec::detail::integer_read_start(input, c, boundary);
    if (!start)
        return seastar::make_ready_future<
          codec::result<decoded_local_metadata>>(codec::failure(start.error()));
    body_reader reader{
      local_metadata_expectation{*header, alignment},
      alignment,
      *start,
      memory,
      true};
    return codec::decode_envelope<decoded_local_metadata>(
      input,
      detail::local_family,
      {local_metadata_max_bytes, local_metadata_max_bytes},
      memory,
      work,
      std::move(reader),
      c,
      boundary);
}

seastar::future<codec::result<decoded_local_metadata>>
decode_selected_object_publication(
  bytes::fragmented_buffer_parser& input,
  local_store_context owner,
  segment_context segment,
  storage_alignment alignment,
  storage_alignment data_alignment,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context c,
  codec::input_boundary boundary) {
    c.family = static_cast<std::uint16_t>(detail::local_family);
    auto header = local_metadata_header::make(
      local_metadata_kind::object_publication,
      owner,
      local_publication_generation::make(1).value());
    auto reject = [](codec::error error) {
        return seastar::make_ready_future<
          codec::result<decoded_local_metadata>>(codec::failure(error));
    };
    if (!header) return reject(detail::page_error(errc::invalid_argument, c));
    local_metadata_expectation expected{
      *header, alignment, segment, data_alignment};
    if (auto valid = detail::validate_local_expectation(expected, c); !valid)
        return reject(valid.error());
    auto start = codec::detail::integer_read_start(input, c, boundary);
    if (!start) return reject(start.error());
    return codec::decode_envelope<decoded_local_metadata>(
      input,
      detail::local_family,
      {local_metadata_max_bytes, local_metadata_max_bytes},
      memory,
      work,
      body_reader{expected, alignment, *start, memory, true},
      c,
      boundary);
}
seastar::future<codec::result<decoded_local_metadata>>
decode_local_wal_descriptor(
  bytes::fragmented_buffer_parser& input,
  local_store_context owner,
  model::wal_incarnation_id incarnation,
  std::optional<codec::immutable_object_digest> digest,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context c,
  codec::input_boundary boundary) {
    c.family = static_cast<std::uint16_t>(detail::local_family);
    auto header = local_metadata_header::make(
      local_metadata_kind::wal_descriptor,
      owner,
      local_publication_generation::make(1).value());
    auto reject = [](codec::error error) {
        return seastar::make_ready_future<
          codec::result<decoded_local_metadata>>(codec::failure(error));
    };
    if (!header || incarnation.is_nil())
        return reject(detail::page_error(errc::invalid_argument, c));
    auto start = codec::detail::integer_read_start(input, c, boundary);
    if (!start) return reject(start.error());
    body_reader reader{
      std::nullopt,
      std::nullopt,
      *start,
      memory,
      false,
      wal_descriptor_expectation{*header, incarnation}};
    if (digest)
        return detail::decode_pinned<decoded_local_metadata>(
          input,
          detail::local_family,
          *digest,
          memory,
          work,
          std::move(reader),
          c,
          boundary);
    return codec::decode_envelope<decoded_local_metadata>(
      input,
      detail::local_family,
      {local_metadata_max_bytes, local_metadata_max_bytes},
      memory,
      work,
      std::move(reader),
      c,
      boundary);
}
seastar::future<codec::result<local_metadata_claims>> probe_local_metadata(
  bytes::fragmented_buffer_parser& input,
  storage_alignment alignment,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context c,
  codec::input_boundary boundary) {
    c.family = static_cast<std::uint16_t>(detail::local_family);
    const auto start = codec::detail::integer_read_start(input, c, boundary);
    if (!start) co_return codec::failure(start.error());
    const auto depth = input.checkpoint_depth();
    if (auto mark = input.push_checkpoint(); !mark)
        co_return codec::failure(
          codec::detail::allocation_cost_error(mark.error(), c, *start));
    codec::detail::parser_transaction_guard transaction{input, depth};
    std::optional<codec::result<decoded_local_metadata>> decoded;
    std::optional<local_metadata_claims> claims;
    std::optional<codec::error> failed;
    std::exception_ptr exception;
    try {
        decoded.emplace(
          co_await codec::decode_envelope<decoded_local_metadata>(
            input,
            detail::local_family,
            {local_metadata_max_bytes, local_metadata_max_bytes},
            memory,
            work,
            body_reader{std::nullopt, alignment, *start, memory},
            c,
            boundary));
        if (!*decoded)
            failed = decoded->error();
        else {
            const auto& record = decoded->value().value;
            std::optional<model::wal_incarnation_id> incarnation;
            auto record_alignment = alignment;
            if (
              const auto* wal = std::get_if<local_wal_descriptor>(
                &record.payload())) {
                incarnation = wal->incarnation;
                record_alignment = wal->alignment;
            }
            claims.emplace(
              local_metadata_claims{
                record.header(),
                detail::local_payload_segment(record.payload()),
                incarnation,
                record.encoded_bytes(),
                record_alignment});
        }
    } catch (...) {
        exception = std::current_exception();
    }
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    decoded.reset();
    if (!failed && !exception) {
        if (auto ready = work.poll(page_error(errc::success, c)); !ready)
            failed = ready.error();
    }
    if (exception) std::rethrow_exception(exception);
    if (failed) co_return codec::failure(*failed);
    transaction.commit();
    co_return *claims;
}
} // namespace kwaque::storage
