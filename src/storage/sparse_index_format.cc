#include "src/storage/sparse_index_format.h"

#include "src/storage/footer_format.h"
#include "src/storage/page_internal.h"

namespace kwaque::storage {
namespace detail {
class sparse_index_codec final {
public:
    static sparse_index_root root(
      sparse_index_context context,
      codec::immutable_object_digest digest,
      std::uint32_t count,
      std::vector<page_ref>&& pages,
      byte_count bytes) noexcept {
        return sparse_index_root{
          context, digest, count, std::move(pages), bytes};
    }
    static sparse_index_page
    page(page_ref ref, std::vector<sparse_index_entry>&& entries) noexcept {
        return sparse_index_page{ref, std::move(entries)};
    }
};
} // namespace detail
namespace {
using detail::load;
using detail::page_error;
using detail::store;
constexpr auto index_family = codec::format_family::sparse_index;
constexpr codec::sha256_digest empty_sha{
  0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4,
  0xc8, 0x99, 0x6f, 0xb9, 0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b,
  0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55};

template<typename Span>
bool contains_span(Span outer, Span inner) noexcept {
    return outer.begin() <= inner.begin() && inner.end() <= outer.end();
}
bool valid_entry(
  const sparse_index_context& context,
  const sparse_index_entry& entry,
  const std::optional<sparse_index_entry>& previous) noexcept {
    const auto coverage = context.coverage();
    return coverage.logical().contains(entry.logical_anchor())
      && coverage.bytes().begin() <= entry.block_position()
      && entry.block_position() < coverage.bytes().end()
      && context.alignment().aligned(entry.block_position())
      && (!previous || (previous->logical_anchor() < entry.logical_anchor()
                        && previous->block_position() < entry.block_position()));
}
codec::result<void> check_counts(
  sparse_index_context context,
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
    if (
      (total == 0) != (pages == 0) || pages > total
      || (total == 0) != context.coverage().physical().empty()
      || total > context.coverage().physical().count().value())
        return codec::failure(page_error(errc::malformed_data, c));
    return {};
}
template<std::size_t N>
codec::result<void> read_scope(
  const std::array<char, N>& fixed,
  sparse_index_context expected,
  std::uint16_t subkind,
  codec::field_context c) {
    if (
      auto kind = detail::check_subkind(
        load<0, std::uint16_t>(fixed), subkind, c);
      !kind)
        return kind;
    if (load<2, std::uint16_t>(fixed) != 0)
        return codec::failure(page_error(errc::malformed_data, c, 2));
    if (auto sc = detail::read_sc<4>(fixed, expected.segment(), c); !sc)
        return sc;
    const auto coverage = detail::read_coverage<76>(fixed, c);
    if (!coverage) return codec::failure(coverage.error());
    if (*coverage != expected.coverage())
        return codec::failure(page_error(errc::wrong_context, c, 76));
    if (detail::read_digest<124>(fixed) != expected.digest().bytes())
        return codec::failure(page_error(errc::wrong_context, c, 124));
    return {};
}
template<std::size_t N>
void write_scope(
  std::array<char, N>& fixed,
  sparse_index_context context,
  std::uint16_t subkind) noexcept {
    store<0>(fixed, subkind);
    detail::write_sc<4>(fixed, context.segment());
    detail::write_coverage<76>(fixed, context.coverage());
    detail::write_digest<124>(fixed, context.digest());
}
void write_entry(
  std::array<char, 16>& out, const sparse_index_entry& entry) noexcept {
    store<0>(out, entry.logical_anchor().value());
    store<8>(out, entry.block_position().value());
}

struct root_reader final {
    sparse_index_context expected;
    codec::immutable_object_digest digest;
    std::uint64_t start;
    codec::decode_budget original;
    std::vector<page_ref> pages;

    seastar::future<codec::result<decoded_sparse_index_root>> operator()(
      bytes::fragmented_buffer_parser& input,
      codec::field_context c,
      codec::input_boundary,
      codec::decode_budget memory,
      codec::cooperative_work& work) {
        const auto anchor = page_error(errc::success, c);
        std::array<char, 168> fixed{};
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
        if (auto scope = read_scope(fixed, expected, 1, c); !scope)
            co_return codec::failure(scope.error());
        const auto total = load<156, std::uint32_t>(fixed);
        const auto count = detail::page_wire(
          page_count::make(load<160, std::uint32_t>(fixed)), c, 160);
        if (!count) co_return codec::failure(count.error());
        if (
          auto valid = check_counts(
            expected, total, count->value(), work.policy(), c);
          !valid)
            co_return codec::failure(valid.error());
        const auto layout = detail::page_wire(
          aligned_envelope_layout::make(
            {byte_count{c.origin - start},
             sparse_index_root_fixed_bytes,
             byte_count{static_cast<std::uint64_t>(count->value()) * 48U}},
            expected.alignment(),
            work.policy(),
            {work.policy().config().max_page_bytes,
             work.policy().config().max_page_bytes}),
          c,
          164);
        if (!layout) co_return codec::failure(layout.error());
        if (
          input.total_bytes() != layout->body_bytes()
          || load<164, std::uint32_t>(fixed) != layout->padding_bytes().value())
            co_return codec::failure(page_error(errc::malformed_data, c, 164));
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
                static_cast<std::uint32_t>(
                  sparse_index_page_fixed_bytes.value()),
                static_cast<std::uint32_t>(
                  sparse_index_entry_wire_bytes.value()),
                expected.alignment(),
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
            co_return codec::failure(page_error(errc::malformed_data, c, 156));
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
          invariant_id{"KQ-INDEX-ROOT-RESIDUAL"},
          retained.has_value(),
          "admitted root metadata exceeded its enclosing reservation");
        co_return decoded_sparse_index_root{
          detail::sparse_index_codec::root(
            expected, digest, total, std::move(pages), layout->encoded_bytes()),
          *retained};
    }
};

struct page_reader final {
    sparse_index_context expected;
    page_ref reference;
    std::uint64_t start;
    std::optional<sparse_index_entry> previous;
    codec::decode_budget original;
    std::vector<sparse_index_entry> entries;

    seastar::future<codec::result<decoded_sparse_index_page>> operator()(
      bytes::fragmented_buffer_parser& input,
      codec::field_context c,
      codec::input_boundary,
      codec::decode_budget memory,
      codec::cooperative_work& work) {
        const auto anchor = page_error(errc::success, c);
        std::array<char, 172> fixed{};
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
        if (auto scope = read_scope(fixed, expected, 2, c); !scope)
            co_return codec::failure(scope.error());
        const auto count = load<164, std::uint32_t>(fixed);
        if (
          load<156, std::uint32_t>(fixed) != reference.ordinal().value()
          || load<160, std::uint32_t>(fixed) != reference.first_entry()
          || count != reference.entry_count())
            co_return codec::failure(page_error(errc::wrong_context, c, 156));
        const byte_count header{c.origin - start};
        const auto capacity = detail::page_wire(
          sparse_index_page_capacity(
            header, expected.alignment(), work.policy()),
          c,
          164);
        if (!capacity) co_return codec::failure(capacity.error());
        if (count == 0)
            co_return codec::failure(page_error(errc::malformed_data, c, 164));
        if (count > *capacity)
            co_return codec::failure(
              page_error(errc::resource_exhausted, c, 164));
        const auto layout = detail::page_wire(
          aligned_envelope_layout::make(
            {header,
             sparse_index_page_fixed_bytes,
             byte_count{static_cast<std::uint64_t>(count) * 16U}},
            expected.alignment(),
            work.policy(),
            {work.policy().config().max_page_bytes,
             work.policy().config().max_page_bytes}),
          c,
          168);
        if (!layout) co_return codec::failure(layout.error());
        if (
          layout->encoded_bytes() != reference.encoded_bytes()
          || input.total_bytes() != layout->body_bytes()
          || load<168, std::uint32_t>(fixed) != layout->padding_bytes().value())
            co_return codec::failure(page_error(errc::malformed_data, c, 168));
        const auto remaining = detail::reserve_entries(
          entries, count, memory, work.policy(), c);
        if (!remaining) co_return codec::failure(remaining.error());
        const byte_count retained_metadata{
          memory.metadata_remaining.value()
          - remaining->metadata_remaining.value()};
        for (std::uint32_t index = 0; index < count; ++index) {
            std::array<char, 16> raw{};
            auto entry_context = c;
            entry_context.origin += input.bytes_consumed().value();
            if (
              auto read = co_await detail::read_fixed(input, raw, work, c);
              !read)
                co_return codec::failure(read.error());
            if (
              auto ready = co_await work.admit(
                byte_count{128}, item_count{8}, anchor);
              !ready)
                co_return codec::failure(ready.error());
            if (auto ready = work.poll(anchor); !ready)
                co_return codec::failure(ready.error());
            const auto logical = detail::page_wire(
              model::range_logical_offset::make(load<0, std::uint64_t>(raw)),
              entry_context);
            if (!logical) co_return codec::failure(logical.error());
            // Reject the key before publishing its value, including the last
            // key from the preceding page when called by the object walker.
            if (previous && previous->logical_anchor() >= *logical)
                co_return codec::failure(
                  page_error(errc::malformed_data, entry_context));
            const sparse_index_entry entry{
              *logical, runtime::file_position{load<8, std::uint64_t>(raw)}};
            if (!valid_entry(expected, entry, previous))
                co_return codec::failure(
                  page_error(errc::malformed_data, entry_context));
            previous = entry;
            entries.push_back(entry);
        }
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
          invariant_id{"KQ-INDEX-PAGE-RESIDUAL"},
          retained.has_value(),
          "admitted page metadata exceeded its enclosing reservation");
        co_return decoded_sparse_index_page{
          detail::sparse_index_codec::page(reference, std::move(entries)),
          *retained};
    }
};

seastar::future<codec::result<decoded_sparse_index_page>> decode_page(
  bytes::fragmented_buffer_parser& input,
  const sparse_index_root& root,
  page_ordinal ordinal,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context c,
  codec::input_boundary boundary,
  std::optional<sparse_index_entry> previous) {
    c.family = static_cast<std::uint16_t>(index_family);
    const auto fail = [](codec::error error) {
        return seastar::make_ready_future<
          codec::result<decoded_sparse_index_page>>(codec::failure(error));
    };
    const auto start = codec::detail::integer_read_start(input, c, boundary);
    if (!start) return fail(start.error());
    if (ordinal.value() >= root.pages().size())
        return fail(page_error(errc::invalid_argument, c));
    if (
      auto valid = check_counts(
        root.context(),
        root.entry_count(),
        root.pages().size(),
        work.policy(),
        c);
      !valid)
        return fail(valid.error());
    const auto ref = root.pages()[ordinal.value()];
    if (
      auto valid = detail::check_page_ref(
        ref,
        ordinal.value(),
        ref.first_entry(),
        static_cast<std::uint32_t>(sparse_index_page_fixed_bytes.value()),
        static_cast<std::uint32_t>(sparse_index_entry_wire_bytes.value()),
        root.context().alignment(),
        work.policy(),
        c);
      !valid)
        return fail(valid.error());
    return detail::decode_pinned<decoded_sparse_index_page>(
      input,
      index_family,
      ref.digest(),
      memory,
      work,
      page_reader{root.context(), ref, *start, previous, memory, {}},
      c,
      boundary,
      ref.encoded_bytes());
}

} // namespace

result<sparse_index_context> sparse_index_context::make(
  segment_context segment,
  storage::coverage coverage,
  codec::extent_digest digest,
  storage_alignment alignment,
  storage_profile profile) noexcept {
    const auto valid = parse_storage_profile(
      static_cast<std::uint16_t>(profile));
    if (!valid) return failure(valid.error());
    if (
      !alignment.aligned(coverage.bytes().begin())
      || !alignment.aligned(coverage.bytes().end())
      || coverage.physical().count().value()
           > coverage.logical().count().value()
      || (!coverage.physical().empty() && coverage.bytes().empty())
      || (coverage.bytes().empty() && digest.bytes() != empty_sha))
        return failure(errc::invalid_argument);
    return sparse_index_context{segment, coverage, digest, alignment, profile};
}
result<std::uint32_t> sparse_index_page_capacity(
  byte_count header,
  storage_alignment alignment,
  const codec::limits& policy) noexcept {
    const auto tail = max_child_bytes(
      header,
      sparse_index_page_fixed_bytes,
      alignment,
      policy,
      {policy.config().max_page_bytes, policy.config().max_page_bytes});
    if (!tail) return failure(tail.error());
    return static_cast<std::uint32_t>(std::min(
      tail->value() / 16U, policy.config().max_object_entries.value()));
}
codec::result<void> validate_sparse_index_anchor(
  const sparse_index_context& context,
  const sparse_index_entry& entry,
  const complete_block_descriptor& block,
  codec::field_context c) {
    c.family = static_cast<std::uint16_t>(index_family);
    if (block.context() != context.segment())
        return codec::failure(page_error(errc::wrong_context, c));
    const auto outer = context.coverage(), inner = block.coverage();
    if (
      !valid_entry(context, entry, std::nullopt)
      || entry.logical_anchor().as_end() != inner.logical().begin()
      || entry.block_position() != inner.bytes().begin()
      || !context.alignment().aligned(inner.bytes().end())
      || !contains_span(outer.logical(), inner.logical())
      || !contains_span(outer.physical(), inner.physical())
      || !contains_span(outer.bytes(), inner.bytes()))
        return codec::failure(page_error(errc::malformed_data, c));
    return {};
}
codec::result<void> validate_sparse_index_extent(
  const sparse_index_root& root,
  const verified_extent& evidence,
  codec::field_context c) {
    c.family = static_cast<std::uint16_t>(index_family);
    const auto context = root.context();
    if (
      context.segment() != evidence.context().segment
      || context.alignment() != evidence.context().alignment
      || context.profile() != evidence.context().profile)
        return codec::failure(page_error(errc::wrong_context, c));
    if (!evidence.digest())
        return codec::failure(page_error(errc::invalid_argument, c));
    if (
      context.coverage() != evidence.boundary().coverage
      || root.entry_count() > evidence.boundary().block_count)
        return codec::failure(page_error(errc::malformed_data, c));
    if (context.digest() != *evidence.digest())
        return codec::failure(page_error(errc::corrupt_data, c));
    return {};
}

seastar::future<codec::result<encoded_sparse_index_page>>
encode_sparse_index_page(
  std::span<const sparse_index_entry> entries,
  sparse_index_context context,
  page_ordinal ordinal,
  std::uint32_t first,
  codec::cooperative_work& work,
  byte_count remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context c) {
    c.family = static_cast<std::uint16_t>(index_family);
    const auto anchor = page_error(errc::success, c);
    if (
      auto ready = co_await work.admit(
        byte_count{1024}, item_count{64}, anchor);
      !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    const auto cap = sparse_index_page_capacity(
      byte_count{32}, context.alignment(), work.policy());
    if (!cap)
        co_return codec::failure(
          codec::detail::allocation_cost_error(cap.error(), c, c.origin));
    if (entries.empty())
        co_return codec::failure(page_error(errc::invalid_argument, c));
    if (
      entries.size() > *cap || first > maximum_object_entries
      || entries.size() > maximum_object_entries - first
      || first + entries.size()
           > work.policy().config().max_object_entries.value()
      || ordinal.value() >= work.policy().config().max_object_pages.value())
        co_return codec::failure(page_error(errc::resource_exhausted, c));
    if (
      (ordinal.value() == 0 && first != 0) || ordinal.value() > first
      || first + entries.size() > context.coverage().physical().count().value())
        co_return codec::failure(page_error(errc::invalid_argument, c));
    std::optional<sparse_index_entry> previous;
    for (const auto& entry : entries) {
        if (
          auto ready = co_await work.admit(
            byte_count{128}, item_count{8}, anchor);
          !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        if (!valid_entry(context, entry, previous))
            co_return codec::failure(page_error(errc::invalid_argument, c));
        previous = entry;
    }
    const auto layout = aligned_envelope_layout::make(
                          {byte_count{32},
                           sparse_index_page_fixed_bytes,
                           byte_count{entries.size() * 16U}},
                          context.alignment(),
                          work.policy(),
                          {work.policy().config().max_page_bytes,
                           work.policy().config().max_page_bytes})
                          .value();
    if (
      layout.encoded_bytes().value()
      > std::numeric_limits<std::uint64_t>::max() - c.origin)
        co_return codec::failure(page_error(errc::out_of_range, c));
    std::array<char, 172> fixed{};
    write_scope(fixed, context, 2);
    store<156>(fixed, ordinal.value());
    store<160>(fixed, first);
    store<164>(fixed, static_cast<std::uint32_t>(entries.size()));
    store<168>(
      fixed, static_cast<std::uint32_t>(layout.padding_bytes().value()));
    auto output = co_await detail::encode_page_object<16>(
      fixed,
      entries,
      write_entry,
      layout,
      index_family,
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
                       ordinal,
                       first,
                       static_cast<std::uint32_t>(entries.size()),
                       output->bytes.size(),
                       output->digest)
                       .value();
    co_return encoded_sparse_index_page{std::move(output->bytes), ref};
}
seastar::future<codec::result<encoded_sparse_index_root>>
encode_sparse_index_root(
  sparse_index_context context,
  std::uint32_t total,
  std::span<const page_ref> refs,
  codec::cooperative_work& work,
  byte_count remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context c) {
    c.family = static_cast<std::uint16_t>(index_family);
    const auto anchor = page_error(errc::success, c);
    if (
      auto ready = co_await work.admit(
        byte_count{1024}, item_count{64}, anchor);
      !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (
      auto valid = check_counts(context, total, refs.size(), work.policy(), c);
      !valid)
        co_return codec::failure(valid.error());
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
            refs[i],
            i,
            first,
            static_cast<std::uint32_t>(sparse_index_page_fixed_bytes.value()),
            static_cast<std::uint32_t>(sparse_index_entry_wire_bytes.value()),
            context.alignment(),
            work.policy(),
            c);
          !valid)
            co_return codec::failure(valid.error());
        first += refs[i].entry_count();
    }
    if (first != total)
        co_return codec::failure(page_error(errc::invalid_argument, c));
    const auto layout = aligned_envelope_layout::make(
      {byte_count{32},
       sparse_index_root_fixed_bytes,
       byte_count{refs.size() * 48U}},
      context.alignment(),
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
    std::array<char, 168> fixed{};
    write_scope(fixed, context, 1);
    store<156>(fixed, total);
    store<160>(fixed, static_cast<std::uint32_t>(refs.size()));
    store<164>(
      fixed, static_cast<std::uint32_t>(layout->padding_bytes().value()));
    auto output = co_await detail::encode_page_object<48>(
      fixed,
      refs,
      detail::write_page_ref,
      *layout,
      index_family,
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
    co_return encoded_sparse_index_root{
      std::move(output->bytes), output->digest};
}

seastar::future<codec::result<decoded_sparse_index_root>>
decode_sparse_index_root(
  bytes::fragmented_buffer_parser& input,
  sparse_index_context expected,
  codec::immutable_object_digest digest,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context c,
  codec::input_boundary boundary) {
    c.family = static_cast<std::uint16_t>(index_family);
    const auto start = codec::detail::integer_read_start(input, c, boundary);
    if (!start)
        return seastar::make_ready_future<
          codec::result<decoded_sparse_index_root>>(
          codec::failure(start.error()));
    return detail::decode_pinned<decoded_sparse_index_root>(
      input,
      index_family,
      digest,
      memory,
      work,
      root_reader{expected, digest, *start, memory, {}},
      c,
      boundary);
}
seastar::future<codec::result<decoded_sparse_index_page>>
decode_sparse_index_page(
  bytes::fragmented_buffer_parser& input,
  const sparse_index_root& root,
  page_ordinal ordinal,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context c,
  codec::input_boundary boundary) {
    return decode_page(
      input, root, ordinal, memory, work, c, boundary, std::nullopt);
}
codec::result<void> sparse_index_verifier::ready(
  codec::cooperative_work& work, codec::field_context c) {
    if (state_ != state::open)
        return codec::failure(page_error(errc::closed, c));
    if (
      work.policy() != policy_
      || (root_.entry_count() == 0) != root_.pages().empty()) {
        state_ = state::closed;
        return codec::failure(page_error(errc::invalid_argument, c));
    }
    if (
      auto valid = check_counts(
        root_.context(), root_.entry_count(), root_.pages().size(), policy_, c);
      !valid) {
        state_ = state::closed;
        return valid;
    }
    if (auto valid = work.poll(page_error(errc::success, c)); !valid) {
        state_ = state::closed;
        return valid;
    }
    return {};
}
seastar::future<codec::result<decoded_sparse_index_page>>
sparse_index_verifier::next(
  bytes::fragmented_buffer_parser& input,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context c,
  codec::input_boundary boundary) {
    c.family = static_cast<std::uint16_t>(index_family);
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
    std::optional<codec::result<decoded_sparse_index_page>> output;
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
            last_));
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
    last_ = (*output)->value.entries().back();
    ++next_;
    state_ = state::open;
    transaction.commit();
    co_return std::move(**output);
}
codec::result<verified_sparse_index_pages> sparse_index_verifier::finish(
  codec::cooperative_work& work, codec::field_context c) {
    c.family = static_cast<std::uint16_t>(index_family);
    if (auto valid = ready(work, c); !valid)
        return codec::failure(valid.error());
    state_ = state::closed;
    if (next_ != root_.pages().size())
        return codec::failure(page_error(errc::malformed_data, c));
    return verified_sparse_index_pages{root_.digest(), root_.entry_count()};
}
} // namespace kwaque::storage
