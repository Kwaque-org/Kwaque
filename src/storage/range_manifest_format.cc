#include "src/storage/range_manifest_format.h"

#include "src/storage/footer_format.h"
#include "src/storage/page_internal.h"

namespace kwaque::storage {
namespace detail {
class range_manifest_codec final {
public:
    static range_manifest_root root(
      range_manifest_root_header header,
      storage_alignment alignment,
      codec::immutable_object_digest digest,
      std::vector<page_ref>&& pages,
      byte_count bytes) noexcept {
        return range_manifest_root{
          header, alignment, digest, std::move(pages), bytes};
    }
    static range_manifest_page page(
      range_manifest_page_header header,
      page_ref reference,
      std::vector<range_manifest_entry>&& entries) noexcept {
        return range_manifest_page{header, reference, std::move(entries)};
    }
};
} // namespace detail
namespace {
using detail::load;
using detail::page_error;
using detail::store;
constexpr auto manifest_family = codec::format_family::range_manifest;
constexpr codec::sha256_digest empty_sha{
  0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4,
  0xc8, 0x99, 0x6f, 0xb9, 0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b,
  0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55};

template<std::size_t N>
void write_scope(
  std::array<char, N>& fixed,
  manifest_context context,
  model::range_logical_span logical,
  std::uint16_t subkind) noexcept {
    store<0>(fixed, subkind);
    detail::write_id<4>(fixed, context.topic());
    detail::write_id<20>(fixed, context.range());
    detail::write_id<36>(fixed, context.manifest());
    store<52>(fixed, context.generation().value());
    store<60>(fixed, logical.begin().value());
    store<68>(fixed, logical.end().value());
}
void write_entry(
  std::array<char, 104>& fixed, const range_manifest_entry& entry) noexcept {
    const auto coverage = entry.coverage();
    store<0>(fixed, coverage.logical().begin().value());
    store<8>(fixed, coverage.logical().end().value());
    detail::write_id<16>(fixed, entry.segment());
    store<32>(fixed, entry.generation().value());
    store<40>(fixed, coverage.physical().begin().value());
    store<48>(fixed, coverage.physical().end().value());
    store<56>(fixed, coverage.bytes().begin().value());
    store<64>(fixed, coverage.bytes().end().value());
    detail::write_digest<72>(fixed, entry.digest());
}
codec::result<void> check_counts(
  range_manifest_root_header header,
  const codec::limits& policy,
  codec::field_context c) {
    if (
      header.entry_count() > policy.config().max_object_entries.value()
      || header.page_count().value() > policy.config().max_object_pages.value())
        return codec::failure(page_error(errc::resource_exhausted, c));
    return {};
}
template<std::size_t N>
codec::result<model::range_logical_span> read_scope(
  const std::array<char, N>& fixed,
  manifest_context expected,
  std::uint16_t subkind,
  codec::field_context c) {
    if (
      auto kind = detail::check_subkind(
        load<0, std::uint16_t>(fixed), subkind, c);
      !kind)
        return codec::failure(kind.error());
    if (load<2, std::uint16_t>(fixed) != 0)
        return codec::failure(page_error(errc::malformed_data, c, 2));
    const auto topic = detail::read_id<4, model::topic_id>(fixed, c);
    const auto range = detail::read_id<20, model::range_id>(fixed, c);
    const auto manifest = detail::read_id<36, model::manifest_id>(fixed, c);
    const auto generation = detail::page_wire(
      model::range_manifest_generation::make(load<52, std::uint64_t>(fixed)),
      c,
      52);
    if (!topic) return codec::failure(topic.error());
    if (!range) return codec::failure(range.error());
    if (!manifest) return codec::failure(manifest.error());
    if (!generation) return codec::failure(generation.error());
    if (
      *topic != expected.topic() || *range != expected.range()
      || *manifest != expected.manifest()
      || *generation != expected.generation())
        return codec::failure(page_error(errc::wrong_context, c, 4));
    return detail::page_wire(
      model::range_logical_span::make(
        model::range_logical_end{load<60, std::uint64_t>(fixed)},
        model::range_logical_end{load<68, std::uint64_t>(fixed)}),
      c,
      60);
}
codec::result<range_manifest_entry> read_entry(
  const std::array<char, 104>& raw,
  model::range_logical_end next,
  model::range_logical_end end,
  codec::field_context c) {
    const auto logical = detail::page_wire(
      model::range_logical_span::make(
        model::range_logical_end{load<0, std::uint64_t>(raw)},
        model::range_logical_end{load<8, std::uint64_t>(raw)}),
      c);
    if (!logical) return codec::failure(logical.error());
    // Positive length and adjacency check the ordered key before its value
    // can enter the staged vector. No sorting or duplicate replacement.
    if (logical->empty() || logical->begin() != next || logical->end() > end)
        return codec::failure(page_error(errc::malformed_data, c));
    const auto segment = detail::read_id<16, model::segment_id>(raw, c);
    const auto generation = detail::page_wire(
      model::segment_generation::make(load<32, std::uint64_t>(raw)), c, 32);
    const auto physical = detail::page_wire(
      model::segment_relative_span::make(
        model::segment_relative_end{load<40, std::uint64_t>(raw)},
        model::segment_relative_end{load<48, std::uint64_t>(raw)}),
      c,
      40);
    const auto bytes = detail::page_wire(
      model::file_byte_span::make(
        runtime::file_position{load<56, std::uint64_t>(raw)},
        runtime::file_position{load<64, std::uint64_t>(raw)}),
      c,
      56);
    if (!segment) return codec::failure(segment.error());
    if (!generation) return codec::failure(generation.error());
    if (!physical) return codec::failure(physical.error());
    if (!bytes) return codec::failure(bytes.error());
    return detail::page_wire(
      range_manifest_entry::make(
        *segment,
        *generation,
        storage::coverage{*logical, *physical, *bytes},
        codec::extent_digest{detail::read_digest<72>(raw)}),
      c);
}

struct root_reader final {
    manifest_context expected;
    model::range_logical_span logical;
    storage_alignment alignment;
    codec::immutable_object_digest digest;
    std::uint64_t start;
    codec::decode_budget original;
    std::vector<page_ref> pages;

    seastar::future<codec::result<decoded_range_manifest_root>> operator()(
      bytes::fragmented_buffer_parser& input,
      codec::field_context c,
      codec::input_boundary,
      codec::decode_budget memory,
      codec::cooperative_work& work) {
        const auto anchor = page_error(errc::success, c);
        std::array<char, 88> fixed{};
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
        const auto scope = read_scope(fixed, expected, 1, c);
        if (!scope) co_return codec::failure(scope.error());
        if (*scope != logical)
            co_return codec::failure(page_error(errc::wrong_context, c, 60));
        const auto total = load<76, std::uint32_t>(fixed);
        const auto count = detail::page_wire(
          page_count::make(load<80, std::uint32_t>(fixed)), c, 80);
        if (!count) co_return codec::failure(count.error());
        const auto header = detail::page_wire(
          range_manifest_root_header::make(expected, *scope, total, *count),
          c,
          76);
        if (!header) co_return codec::failure(header.error());
        if (auto valid = check_counts(*header, work.policy(), c); !valid)
            co_return codec::failure(valid.error());
        const auto layout = detail::page_wire(
          aligned_envelope_layout::make(
            {byte_count{c.origin - start},
             range_manifest_root_fixed_bytes,
             byte_count{static_cast<std::uint64_t>(count->value()) * 48U}},
            alignment,
            work.policy(),
            {work.policy().config().max_page_bytes,
             work.policy().config().max_page_bytes}),
          c,
          84);
        if (!layout) co_return codec::failure(layout.error());
        if (
          input.total_bytes() != layout->body_bytes()
          || load<84, std::uint32_t>(fixed) != layout->padding_bytes().value())
            co_return codec::failure(page_error(errc::malformed_data, c, 84));
        const auto remaining = detail::reserve_entries(
          pages, count->value(), memory, work.policy(), c);
        if (!remaining) co_return codec::failure(remaining.error());
        const byte_count retained_metadata{
          memory.metadata_remaining.value()
          - remaining->metadata_remaining.value()};
        std::uint32_t first = 0;
        for (std::uint32_t ordinal = 0; ordinal < count->value(); ++ordinal) {
            std::array<char, 48> raw{};
            auto entry_context = c;
            entry_context.origin += input.bytes_consumed().value();
            if (
              auto read = co_await detail::read_fixed(input, raw, work, c);
              !read)
                co_return codec::failure(read.error());
            if (
              auto ready = co_await work.admit(
                byte_count{1024}, item_count{64}, anchor);
              !ready)
                co_return codec::failure(ready.error());
            if (auto ready = work.poll(anchor); !ready)
                co_return codec::failure(ready.error());
            const auto ref = detail::read_page_ref(raw, entry_context);
            if (!ref) co_return codec::failure(ref.error());
            if (
              auto valid = detail::check_page_ref(
                *ref,
                ordinal,
                first,
                92,
                104,
                alignment,
                work.policy(),
                entry_context);
              !valid)
                co_return codec::failure(valid.error());
            first += ref->entry_count();
            if (first > total)
                co_return codec::failure(
                  page_error(errc::malformed_data, entry_context));
            pages.push_back(*ref);
        }
        if (first != total)
            co_return codec::failure(page_error(errc::malformed_data, c, 76));
        if (
          auto read = co_await detail::read_padding(
            input, layout->padding_bytes(), work, c);
          !read)
            co_return codec::failure(read.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        const auto retained = codec::detail::consume_decode_budget(
          work.policy(), original, {}, retained_metadata, c, c.origin);
        KWAQUE_INVARIANT(
          invariant_id{"KQ-MANIFEST-ROOT-RESIDUAL"},
          retained.has_value(),
          "admitted root metadata exceeded its enclosing reservation");
        co_return decoded_range_manifest_root{
          detail::range_manifest_codec::root(
            *header,
            alignment,
            digest,
            std::move(pages),
            layout->encoded_bytes()),
          *retained};
    }
};

struct page_reader final {
    manifest_context expected;
    model::range_logical_span root_logical;
    storage_alignment alignment;
    page_ref reference;
    std::uint64_t start;
    std::optional<model::range_logical_end> previous;
    codec::decode_budget original;
    std::vector<range_manifest_entry> entries;

    seastar::future<codec::result<decoded_range_manifest_page>> operator()(
      bytes::fragmented_buffer_parser& input,
      codec::field_context c,
      codec::input_boundary,
      codec::decode_budget memory,
      codec::cooperative_work& work) {
        const auto anchor = page_error(errc::success, c);
        std::array<char, 92> fixed{};
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
        const auto scope = read_scope(fixed, expected, 2, c);
        if (!scope) co_return codec::failure(scope.error());
        if (
          scope->begin() < root_logical.begin()
          || scope->end() > root_logical.end()
          || (previous && scope->begin() != *previous))
            co_return codec::failure(page_error(errc::malformed_data, c, 60));
        const auto count = load<84, std::uint32_t>(fixed);
        if (
          load<76, std::uint32_t>(fixed) != reference.ordinal().value()
          || load<80, std::uint32_t>(fixed) != reference.first_entry()
          || count != reference.entry_count())
            co_return codec::failure(page_error(errc::wrong_context, c, 76));
        const auto page_header = detail::page_wire(
          range_manifest_page_header::make(
            expected,
            *scope,
            reference.ordinal(),
            reference.first_entry(),
            count),
          c,
          76);
        if (!page_header) co_return codec::failure(page_header.error());
        const byte_count header{c.origin - start};
        const auto capacity = detail::page_wire(
          range_manifest_page_capacity(header, alignment, work.policy()),
          c,
          84);
        if (!capacity) co_return codec::failure(capacity.error());
        if (count > *capacity)
            co_return codec::failure(
              page_error(errc::resource_exhausted, c, 84));
        const auto layout = detail::page_wire(
          aligned_envelope_layout::make(
            {header,
             range_manifest_page_fixed_bytes,
             byte_count{static_cast<std::uint64_t>(count) * 104U}},
            alignment,
            work.policy(),
            {work.policy().config().max_page_bytes,
             work.policy().config().max_page_bytes}),
          c,
          88);
        if (!layout) co_return codec::failure(layout.error());
        if (
          layout->encoded_bytes() != reference.encoded_bytes()
          || input.total_bytes() != layout->body_bytes()
          || load<88, std::uint32_t>(fixed) != layout->padding_bytes().value())
            co_return codec::failure(page_error(errc::malformed_data, c, 88));
        const auto remaining = detail::reserve_entries(
          entries, count, memory, work.policy(), c);
        if (!remaining) co_return codec::failure(remaining.error());
        const byte_count retained_metadata{
          memory.metadata_remaining.value()
          - remaining->metadata_remaining.value()};
        auto next = scope->begin();
        for (std::uint32_t index = 0; index < count; ++index) {
            std::array<char, 104> raw{};
            auto entry_context = c;
            entry_context.origin += input.bytes_consumed().value();
            if (
              auto read = co_await detail::read_fixed(input, raw, work, c);
              !read)
                co_return codec::failure(read.error());
            if (
              auto ready = co_await work.admit(
                byte_count{1024}, item_count{64}, anchor);
              !ready)
                co_return codec::failure(ready.error());
            if (auto ready = work.poll(anchor); !ready)
                co_return codec::failure(ready.error());
            const auto entry = read_entry(
              raw, next, scope->end(), entry_context);
            if (!entry) co_return codec::failure(entry.error());
            next = entry->coverage().logical().end();
            entries.push_back(*entry);
        }
        if (next != scope->end())
            co_return codec::failure(page_error(errc::malformed_data, c, 68));
        if (
          auto read = co_await detail::read_padding(
            input, layout->padding_bytes(), work, c);
          !read)
            co_return codec::failure(read.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        const auto retained = codec::detail::consume_decode_budget(
          work.policy(), original, {}, retained_metadata, c, c.origin);
        KWAQUE_INVARIANT(
          invariant_id{"KQ-MANIFEST-PAGE-RESIDUAL"},
          retained.has_value(),
          "admitted page metadata exceeded its enclosing reservation");
        co_return decoded_range_manifest_page{
          detail::range_manifest_codec::page(
            *page_header, reference, std::move(entries)),
          *retained};
    }
};

seastar::future<codec::result<decoded_range_manifest_page>> decode_page(
  bytes::fragmented_buffer_parser& input,
  const range_manifest_root& root,
  page_ordinal ordinal,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context c,
  codec::input_boundary boundary,
  std::optional<model::range_logical_end> previous) {
    c.family = static_cast<std::uint16_t>(manifest_family);
    const auto fail = [](codec::error error) {
        return seastar::make_ready_future<
          codec::result<decoded_range_manifest_page>>(codec::failure(error));
    };
    const auto start = codec::detail::integer_read_start(input, c, boundary);
    if (!start) return fail(start.error());
    if (ordinal.value() >= root.pages().size())
        return fail(page_error(errc::invalid_argument, c));
    if (auto valid = check_counts(root.header(), work.policy(), c); !valid)
        return fail(valid.error());
    const auto ref = root.pages()[ordinal.value()];
    if (
      auto valid = detail::check_page_ref(
        ref,
        ordinal.value(),
        ref.first_entry(),
        92,
        104,
        root.alignment(),
        work.policy(),
        c);
      !valid)
        return fail(valid.error());
    return detail::decode_pinned<decoded_range_manifest_page>(
      input,
      manifest_family,
      ref.digest(),
      memory,
      work,
      page_reader{
        root.context(),
        root.logical_span(),
        root.alignment(),
        ref,
        *start,
        previous,
        memory,
        {}},
      c,
      boundary,
      ref.encoded_bytes());
}

} // namespace

result<manifest_context> manifest_context::make(
  model::topic_id topic,
  model::range_id range,
  model::manifest_id manifest,
  model::range_manifest_generation generation) noexcept {
    if (
      topic.is_nil() || range.is_nil() || manifest.is_nil()
      || !generation.is_valid())
        return failure(errc::invalid_argument);
    return manifest_context{topic, range, manifest, generation};
}
result<void> manifest_context::validate_expected(
  const manifest_context& expected) const noexcept {
    if (*this != expected) return failure(errc::wrong_context);
    return {};
}
result<range_manifest_entry> range_manifest_entry::make(
  model::segment_id segment,
  model::segment_generation generation,
  storage::coverage coverage,
  codec::extent_digest digest) noexcept {
    if (
      segment.is_nil() || !generation.is_valid() || coverage.logical().empty()
      || coverage.physical().count().value()
           > coverage.logical().count().value()
      || (!coverage.physical().empty() && coverage.bytes().empty())
      || (coverage.bytes().empty() && digest.bytes() != empty_sha))
        return failure(errc::invalid_argument);
    return range_manifest_entry{segment, generation, coverage, digest};
}
result<range_manifest_root_header> range_manifest_root_header::make(
  manifest_context context,
  model::range_logical_span logical,
  std::uint32_t count,
  storage::page_count pages) noexcept {
    if (count > maximum_object_entries)
        return failure(errc::resource_exhausted);
    if (
      (count == 0) != logical.empty() || (count == 0) != (pages.value() == 0)
      || pages.value() > count || count > logical.count().value())
        return failure(errc::invalid_argument);
    return range_manifest_root_header{context, logical, count, pages};
}
result<range_manifest_page_header> range_manifest_page_header::make(
  manifest_context context,
  model::range_logical_span logical,
  page_ordinal ordinal,
  std::uint32_t first,
  std::uint32_t count) noexcept {
    if (
      first > maximum_object_entries || count > maximum_object_entries - first)
        return failure(errc::resource_exhausted);
    if (
      count == 0 || logical.empty() || count > logical.count().value()
      || (ordinal.value() == 0 && first != 0) || ordinal.value() > first)
        return failure(errc::invalid_argument);
    return range_manifest_page_header{context, logical, ordinal, first, count};
}
result<std::uint32_t> range_manifest_page_capacity(
  byte_count header,
  storage_alignment alignment,
  const codec::limits& policy) noexcept {
    const auto tail = max_child_bytes(
      header,
      range_manifest_page_fixed_bytes,
      alignment,
      policy,
      {policy.config().max_page_bytes, policy.config().max_page_bytes});
    if (!tail) return failure(tail.error());
    return static_cast<std::uint32_t>(std::min(
      tail->value() / range_manifest_entry_wire_bytes.value(),
      policy.config().max_object_entries.value()));
}

seastar::future<codec::result<encoded_range_manifest_page>>
encode_range_manifest_page(
  range_manifest_page_header header,
  std::span<const range_manifest_entry> entries,
  storage_alignment alignment,
  codec::cooperative_work& work,
  byte_count remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context c) {
    c.family = static_cast<std::uint16_t>(manifest_family);
    const auto anchor = page_error(errc::success, c);
    if (
      auto ready = co_await work.admit(
        byte_count{1024}, item_count{64}, anchor);
      !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    const auto cap = range_manifest_page_capacity(
      byte_count{32}, alignment, work.policy());
    if (!cap)
        co_return codec::failure(
          codec::detail::allocation_cost_error(cap.error(), c, c.origin));
    if (
      header.entry_count() > *cap || entries.size() > *cap
      || static_cast<std::uint64_t>(header.first_entry()) + header.entry_count()
           > work.policy().config().max_object_entries.value()
      || header.ordinal().value()
           >= work.policy().config().max_object_pages.value())
        co_return codec::failure(page_error(errc::resource_exhausted, c));
    if (entries.size() != header.entry_count())
        co_return codec::failure(page_error(errc::invalid_argument, c));
    auto next = header.logical_span().begin();
    for (const auto& entry : entries) {
        if (
          auto ready = co_await work.admit(
            byte_count{1024}, item_count{64}, anchor);
          !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        const auto logical = entry.coverage().logical();
        // Positive entry lengths plus exact adjacency imply strict ascending
        // keys. Reject the edge before allocating any encoded tail.
        if (
          logical.begin() != next
          || logical.end() > header.logical_span().end())
            co_return codec::failure(page_error(errc::invalid_argument, c));
        next = logical.end();
    }
    if (next != header.logical_span().end())
        co_return codec::failure(page_error(errc::invalid_argument, c));
    const auto layout = aligned_envelope_layout::make(
                          {byte_count{32},
                           range_manifest_page_fixed_bytes,
                           byte_count{entries.size() * 104U}},
                          alignment,
                          work.policy(),
                          {work.policy().config().max_page_bytes,
                           work.policy().config().max_page_bytes})
                          .value();
    if (
      layout.encoded_bytes().value()
      > std::numeric_limits<std::uint64_t>::max() - c.origin)
        co_return codec::failure(page_error(errc::out_of_range, c));
    std::array<char, 92> fixed{};
    write_scope(fixed, header.context(), header.logical_span(), 2);
    store<76>(fixed, header.ordinal().value());
    store<80>(fixed, header.first_entry());
    store<84>(fixed, header.entry_count());
    store<88>(
      fixed, static_cast<std::uint32_t>(layout.padding_bytes().value()));
    auto output = co_await detail::encode_page_object<104>(
      fixed,
      entries,
      write_entry,
      layout,
      manifest_family,
      work,
      remaining,
      charge,
      c);
    if (!output) co_return codec::failure(output.error());
    if (auto ready = work.poll(anchor); !ready) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        output->bytes = bytes::fragmented_buffer{};
        co_return codec::failure(ready.error());
    }
    const auto ref = page_ref::make(
                       header.ordinal(),
                       header.first_entry(),
                       header.entry_count(),
                       output->bytes.size(),
                       output->digest)
                       .value();
    co_return encoded_range_manifest_page{std::move(output->bytes), ref};
}

seastar::future<codec::result<encoded_range_manifest_root>>
encode_range_manifest_root(
  range_manifest_root_header header,
  std::span<const page_ref> refs,
  storage_alignment alignment,
  codec::cooperative_work& work,
  byte_count remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context c) {
    c.family = static_cast<std::uint16_t>(manifest_family);
    const auto anchor = page_error(errc::success, c);
    if (
      auto ready = co_await work.admit(
        byte_count{1024}, item_count{64}, anchor);
      !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (
      header.entry_count() > work.policy().config().max_object_entries.value()
      || header.page_count().value()
           > work.policy().config().max_object_pages.value()
      || refs.size() > work.policy().config().max_object_pages.value())
        co_return codec::failure(page_error(errc::resource_exhausted, c));
    if (refs.size() != header.page_count().value())
        co_return codec::failure(page_error(errc::invalid_argument, c));
    std::uint32_t first = 0;
    for (std::uint32_t i = 0; i < refs.size(); ++i) {
        if (
          auto ready = co_await work.admit(
            byte_count{1024}, item_count{64}, anchor);
          !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        if (
          auto valid = detail::check_page_ref(
            refs[i], i, first, 92, 104, alignment, work.policy(), c);
          !valid)
            co_return codec::failure(valid.error());
        first += refs[i].entry_count();
        if (first > header.entry_count())
            co_return codec::failure(page_error(errc::invalid_argument, c));
    }
    if (first != header.entry_count())
        co_return codec::failure(page_error(errc::invalid_argument, c));
    const auto layout = aligned_envelope_layout::make(
      {byte_count{32},
       range_manifest_root_fixed_bytes,
       byte_count{refs.size() * 48U}},
      alignment,
      work.policy(),
      {work.policy().config().max_page_bytes,
       work.policy().config().max_page_bytes});
    if (!layout)
        co_return codec::failure(
          codec::detail::allocation_cost_error(layout.error(), c, c.origin));
    if (
      layout->encoded_bytes().value()
      > std::numeric_limits<std::uint64_t>::max() - c.origin)
        co_return codec::failure(page_error(errc::out_of_range, c));
    std::array<char, 88> fixed{};
    write_scope(fixed, header.context(), header.logical_span(), 1);
    store<76>(fixed, header.entry_count());
    store<80>(fixed, header.page_count().value());
    store<84>(
      fixed, static_cast<std::uint32_t>(layout->padding_bytes().value()));
    auto output = co_await detail::encode_page_object<48>(
      fixed,
      refs,
      detail::write_page_ref,
      *layout,
      manifest_family,
      work,
      remaining,
      charge,
      c);
    if (!output) co_return codec::failure(output.error());
    if (auto ready = work.poll(anchor); !ready) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        output->bytes = bytes::fragmented_buffer{};
        co_return codec::failure(ready.error());
    }
    co_return encoded_range_manifest_root{
      std::move(output->bytes), output->digest};
}
codec::result<void> validate_range_manifest_extent(
  manifest_context expected,
  model::cluster_id cluster,
  const range_manifest_entry& entry,
  const verified_extent& evidence,
  codec::field_context c) {
    c.family = static_cast<std::uint16_t>(manifest_family);
    if (cluster.is_nil())
        return codec::failure(page_error(errc::invalid_argument, c));
    const auto segment = evidence.context().segment;
    if (
      segment.cluster() != cluster || segment.topic() != expected.topic()
      || segment.range() != expected.range()
      || segment.segment() != entry.segment()
      || segment.generation() != entry.generation())
        return codec::failure(page_error(errc::wrong_context, c));
    if (!evidence.digest())
        return codec::failure(page_error(errc::invalid_argument, c));
    if (entry.coverage() != evidence.boundary().coverage)
        return codec::failure(page_error(errc::malformed_data, c));
    if (entry.digest() != *evidence.digest())
        return codec::failure(page_error(errc::corrupt_data, c));
    return {};
}

seastar::future<codec::result<decoded_range_manifest_root>>
decode_range_manifest_root(
  bytes::fragmented_buffer_parser& input,
  manifest_context expected,
  model::range_logical_span logical,
  storage_alignment alignment,
  codec::immutable_object_digest digest,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context c,
  codec::input_boundary boundary) {
    c.family = static_cast<std::uint16_t>(manifest_family);
    const auto start = codec::detail::integer_read_start(input, c, boundary);
    if (!start)
        return seastar::make_ready_future<
          codec::result<decoded_range_manifest_root>>(
          codec::failure(start.error()));
    return detail::decode_pinned<decoded_range_manifest_root>(
      input,
      manifest_family,
      digest,
      memory,
      work,
      root_reader{expected, logical, alignment, digest, *start, memory, {}},
      c,
      boundary);
}
seastar::future<codec::result<decoded_range_manifest_page>>
decode_range_manifest_page(
  bytes::fragmented_buffer_parser& input,
  const range_manifest_root& root,
  page_ordinal ordinal,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context c,
  codec::input_boundary boundary) {
    return decode_page(
      input, root, ordinal, memory, work, c, boundary, std::nullopt);
}
codec::result<void> range_manifest_verifier::ready(
  codec::cooperative_work& work, codec::field_context c) {
    if (state_ != state::open)
        return codec::failure(page_error(errc::closed, c));
    if (
      work.policy() != policy_
      || (root_.entry_count() == 0) != root_.pages().empty()) {
        state_ = state::closed;
        return codec::failure(page_error(errc::invalid_argument, c));
    }
    if (auto valid = check_counts(root_.header(), policy_, c); !valid) {
        state_ = state::closed;
        return valid;
    }
    if (auto valid = work.poll(page_error(errc::success, c)); !valid) {
        state_ = state::closed;
        return valid;
    }
    return {};
}
seastar::future<codec::result<decoded_range_manifest_page>>
range_manifest_verifier::next(
  bytes::fragmented_buffer_parser& input,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context c,
  codec::input_boundary boundary) {
    c.family = static_cast<std::uint16_t>(manifest_family);
    if (auto valid = ready(work, c); !valid)
        co_return codec::failure(valid.error());
    if (next_ == root_.pages().size()) {
        state_ = state::closed;
        co_return codec::failure(page_error(errc::malformed_data, c));
    }
    const auto depth = input.checkpoint_depth();
    if (auto mark = input.push_checkpoint(); !mark) {
        state_ = state::closed;
        co_return codec::failure(
          codec::detail::allocation_cost_error(mark.error(), c, c.origin));
    }
    codec::detail::parser_transaction_guard transaction{input, depth};
    state_ = state::active;
    std::optional<codec::result<decoded_range_manifest_page>> output;
    std::optional<codec::error> failed;
    std::exception_ptr exception;
    try {
        output.emplace(
          co_await decode_page(
            input,
            root_,
            page_ordinal::make(next_).value(),
            memory,
            work,
            c,
            boundary,
            end_));
        if (!output->has_value()) failed = output->error();
        if (!failed) {
            if (auto valid = work.poll(page_error(errc::success, c)); !valid)
                failed = valid.error();
        }
    } catch (...) {
        exception = std::current_exception();
    }
    if (failed || exception) {
        state_ = state::closed;
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        output.reset();
        if (exception) std::rethrow_exception(exception);
        co_return codec::failure(*failed);
    }
    end_ = (*output)->value.header().logical_span().end();
    ++next_;
    state_ = state::open;
    transaction.commit();
    co_return std::move(**output);
}
codec::result<verified_range_manifest_pages> range_manifest_verifier::finish(
  codec::cooperative_work& work, codec::field_context c) {
    c.family = static_cast<std::uint16_t>(manifest_family);
    if (auto valid = ready(work, c); !valid)
        return codec::failure(valid.error());
    state_ = state::closed;
    if (next_ != root_.pages().size() || end_ != root_.logical_span().end())
        return codec::failure(page_error(errc::malformed_data, c));
    return verified_range_manifest_pages{root_.digest(), root_.entry_count()};
}
} // namespace kwaque::storage
