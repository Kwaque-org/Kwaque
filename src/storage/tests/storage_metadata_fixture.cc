#include "src/storage/tests/storage_metadata_fixture.h"

namespace kwaque::storage::testing {
namespace {
void require(bool valid) {
    if (!valid) __builtin_trap();
}
std::uint32_t total(std::uint32_t entries, std::uint32_t pages) {
    if (pages > 4 || entries > 4083 || (entries == 0) != (pages == 0))
        throw std::invalid_argument("invalid metadata fixture count");
    return entries * pages;
}
} // namespace
metadata_fixture::metadata_fixture(
  bool selected,
  std::uint32_t count,
  std::uint32_t page_count,
  std::size_t h,
  std::uint64_t a,
  bool sparse,
  bool wide)
  : is_manifest(selected)
  , entries(count)
  , pages(page_count)
  , header(h)
  , alignment(testing::alignment(a))
  , logical_base(wide ? (std::uint64_t{1} << 40U) : 100U)
  , position_base(wide ? (std::uint64_t{1} << 40U) : a)
  , stride(sparse ? 3U : 1U)
  , index_context(
      sparse_index_context::make(
        sc(),
        scope(
          logical_base,
          logical_base + stride * total(count, page_count),
          0,
          total(count, page_count),
          position_base,
          position_base + std::uint64_t{total(count, page_count)} * a),
        codec::extent_digest{index::sha(count == 0 ? "" : "extent")},
        alignment)
        .value())
  , manifest_header(
      manifest::root_header(
        total(count, page_count),
        page_count,
        manifest::mc(),
        logical_base,
        logical_base + stride * total(count, page_count))) {
    if (h != 32 && h != 40 && h != 4096)
        throw std::invalid_argument("invalid metadata fixture header");
    const auto capacity
      = selected ? range_manifest_page_capacity(
                     byte_count{h}, alignment, codec::limits::defaults())
                 : sparse_index_page_capacity(
                     byte_count{h}, alignment, codec::limits::defaults());
    if (!capacity || count > *capacity)
        throw std::invalid_argument("invalid metadata fixture size");
}
std::vector<sparse_index_entry>
metadata_fixture::index_entries(std::uint32_t ordinal) const {
    std::vector<sparse_index_entry> values;
    values.reserve(entries);
    for (std::uint32_t i = 0; i < entries; ++i) {
        const auto position = std::uint64_t{ordinal} * entries + i;
        values.push_back(
          index::entry(
            logical_base + stride * position,
            position_base + alignment.bytes().value() * position));
    }
    return values;
}
std::vector<range_manifest_entry>
metadata_fixture::manifest_entries(std::uint32_t ordinal) const {
    std::vector<range_manifest_entry> values;
    values.reserve(entries);
    for (std::uint32_t i = 0; i < entries; ++i) {
        const auto position = std::uint64_t{ordinal} * entries + i;
        values.push_back(
          manifest::item(
            logical_base + stride * position,
            logical_base + stride * (position + 1U),
            stride == 3 && (i % 2U) != 0,
            static_cast<std::uint8_t>(0x30U + i % 3U),
            7U + i % 3U));
    }
    return values;
}
range_manifest_page_header
metadata_fixture::page_header(std::uint32_t ordinal) const {
    const auto first = ordinal * entries;
    return manifest::page_header(
      entries,
      manifest_header.context(),
      logical_base + stride * first,
      logical_base + stride * (std::uint64_t{first} + entries),
      ordinal,
      first);
}
std::string metadata_fixture::page_wire(std::uint32_t ordinal) const {
    if (ordinal >= pages)
        throw std::invalid_argument("invalid fixture page ordinal");
    if (is_manifest)
        return manifest::expected_page(
          page_header(ordinal),
          manifest_entries(ordinal),
          alignment.bytes().value(),
          header);
    return index::page_wire(
      index_entries(ordinal),
      index_context,
      ordinal,
      ordinal * entries,
      header);
}
page_ref metadata_fixture::reference(
  std::string_view wire, std::uint32_t ordinal) const {
    return index::page_reference(wire, entries, ordinal, ordinal * entries);
}
std::string metadata_fixture::root_wire(std::span<const page_ref> refs) const {
    if (is_manifest)
        return manifest::expected_root(
          manifest_header, refs, alignment.bytes().value(), header);
    return index::root_wire(refs, index_context, header);
}

storage_observation observe_metadata(
  const metadata_fixture& fixture,
  bool is_root,
  std::string_view candidate,
  std::span<const page_ref> refs,
  codec::immutable_object_digest digest,
  std::size_t width,
  std::uint8_t restrictions,
  codec::input_boundary boundary) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    seastar::abort_source setup_abort;
    codec::cooperative_work setup{codec::limits::defaults(), setup_abort};
    std::optional<sparse_index_root> index_root;
    std::optional<range_manifest_root> manifest_root;
    if (!is_root) {
        const auto wire = fixture.root_wire(refs);
        if (fixture.is_manifest)
            manifest_root.emplace(
              manifest::pin(
                wire, fixture.manifest_header, setup, fixture.alignment));
        else
            index_root.emplace(index::pin(wire, fixture.index_context, setup));
    }
    std::string framed = "p";
    framed.append(candidate);
    width = std::max(width, (framed.size() + 999U) / 1000U);
    bytes::fragmented_buffer_parser input{buffer(framed, width)};
    input.skip(byte_count{1}).value();
    input.push_checkpoint().value();
    const codec::field_context c{
      .origin = (restrictions & 8U) != 0 ? UINT64_MAX - framed.size() : 71};
    // Reserve 32 MiB outside this budget for fixtures/pins/native/frame costs.
    auto memory = reserve(input, work, c);
    if ((restrictions & 1U) != 0) memory.operation_remaining = {};
    if ((restrictions & 2U) != 0) memory.metadata_remaining = {};
    if ((restrictions & 4U) != 0) abort.request_abort();
    if ((restrictions & 16U) != 0)
        while (input.checkpoint_depth() < bytes::max_parser_checkpoints)
            input.push_checkpoint().value();
    const auto depth = input.checkpoint_depth();
    storage_observation seen;
    const auto header = candidate.size() < 32
                          ? std::size_t{0}
                          : static_cast<std::size_t>(get(candidate, 10, 2));
    const auto record = [&](const auto& result) {
        if (!result) {
            seen.error = result.error();
            return false;
        }
        require(header == 32 || (header >= 40 && header <= 4096));
        return true;
    };
    const auto record_body = [&](const std::string& encoded) {
        const auto length = input.bytes_consumed().value() - 1U;
        require(length >= header && length <= candidate.size());
        require(encoded.size() == length);
        // Rebuild every body field from decoded values using independent
        // fixed-offset fixtures. Unknown optional envelope extensions need
        // not have a canonical representation, so compare the body only.
        require(
          std::string_view{encoded}.substr(header)
          == candidate.substr(header, length - header));
        seen.digest = index::sha(encoded);
    };
    if (fixture.is_manifest) {
        if (is_root) {
            const auto result = decode_range_manifest_root(
                                  input,
                                  fixture.manifest_header.context(),
                                  fixture.manifest_header.logical_span(),
                                  fixture.alignment,
                                  digest,
                                  memory,
                                  work,
                                  c,
                                  boundary)
                                  .get();
            if (record(result)) {
                require(
                  result->value.context() == fixture.manifest_header.context()
                  && result->value.logical_span()
                       == fixture.manifest_header.logical_span());
                seen.facts = {
                  result->value.entry_count(),
                  result->value.pages().size(),
                  result->value.logical_span().begin().value(),
                  result->value.logical_span().end().value(),
                  0,
                  0};
                record_body(
                  manifest::expected_root(
                    result->value.header(),
                    result->value.pages(),
                    fixture.alignment.bytes().value(),
                    header));
            }
        } else {
            const auto result = decode_range_manifest_page(
                                  input,
                                  *manifest_root,
                                  page_ordinal::make(0).value(),
                                  memory,
                                  work,
                                  c,
                                  boundary)
                                  .get();
            if (record(result)) {
                const auto logical = result->value.header().logical_span();
                require(
                  result->value.header().context()
                    == fixture.manifest_header.context()
                  && logical.begin()
                       >= fixture.manifest_header.logical_span().begin()
                  && logical.end()
                       <= fixture.manifest_header.logical_span().end());
                seen.facts = {
                  result->value.entries().size(),
                  result->value.reference().first_entry(),
                  result->value.header().logical_span().begin().value(),
                  result->value.header().logical_span().end().value(),
                  0,
                  0};
                require(
                  result->value.reference() == refs[0]
                  && result->value.entries().size() == refs[0].entry_count());
                record_body(
                  manifest::expected_page(
                    result->value.header(),
                    result->value.entries(),
                    fixture.alignment.bytes().value(),
                    header));
            }
        }
    } else {
        if (is_root) {
            const auto result = decode_sparse_index_root(
                                  input,
                                  fixture.index_context,
                                  digest,
                                  memory,
                                  work,
                                  c,
                                  boundary)
                                  .get();
            if (record(result)) {
                require(result->value.context() == fixture.index_context);
                seen.facts = {
                  result->value.entry_count(),
                  result->value.pages().size(),
                  0,
                  0,
                  0,
                  0};
                require(
                  result->value.entry_count()
                  == get(candidate, header + 156U, 4));
                record_body(
                  index::root_wire(
                    result->value.pages(), result->value.context(), header));
            }
        } else {
            const auto result = decode_sparse_index_page(
                                  input,
                                  *index_root,
                                  page_ordinal::make(0).value(),
                                  memory,
                                  work,
                                  c,
                                  boundary)
                                  .get();
            if (record(result)) {
                seen.facts = {
                  result->value.entries().size(),
                  result->value.reference().first_entry(),
                  0,
                  0,
                  0,
                  0};
                require(
                  result->value.reference() == refs[0]
                  && result->value.entries().size() == refs[0].entry_count());
                record_body(
                  index::page_wire(
                    result->value.entries(),
                    fixture.index_context,
                    0,
                    0,
                    header));
            }
        }
    }
    require(input.checkpoint_depth() == depth);
    if (seen.error) {
        require(input.bytes_consumed().value() == 1);
        require(
          seen.error->byte_offset() >= c.origin
          && seen.error->byte_offset() - c.origin <= framed.size());
    } else
        require(
          input.bytes_consumed().value() > 1
          && input.bytes_consumed().value() - 1U <= candidate.size());
    seen.consumed = byte_count{input.bytes_consumed().value() - 1U};
    return seen;
}
} // namespace kwaque::storage::testing
