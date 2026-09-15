#include "src/storage/sealed_format.h"

#include "src/storage/footer_internal.h"
#include "src/storage/page_internal.h"

namespace kwaque::storage {
namespace detail {
class sealed_codec final {
public:
    static sealed_footer make(
      footer_expectation location,
      boundary_fields boundary,
      codec::extent_digest extent_digest,
      codec::immutable_object_digest digest,
      std::uint32_t count,
      std::vector<page_ref>&& pages,
      model::file_byte_span extent) noexcept {
        return sealed_footer{
          location,
          boundary,
          extent_digest,
          digest,
          count,
          std::move(pages),
          extent};
    }
};
} // namespace detail
namespace {
using detail::load;
using detail::page_error;
using detail::store;
constexpr codec::sha256_digest empty_sha{
  0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4,
  0xc8, 0x99, 0x6f, 0xb9, 0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b,
  0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55};

codec::result<void> check_counts(
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
struct sealed_reader final {
    footer_expectation expected;
    codec::immutable_object_digest digest;
    std::uint64_t start;
    codec::decode_budget original;
    std::vector<page_ref> pages;

    seastar::future<codec::result<decoded_sealed_footer>> operator()(
      bytes::fragmented_buffer_parser& input,
      codec::field_context c,
      codec::input_boundary,
      codec::decode_budget memory,
      codec::cooperative_work& work) {
        const auto anchor = page_error(errc::success, c);
        std::array<char, 232> fixed{};
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
        if (
          auto kind = detail::check_subkind(
            load<0, std::uint16_t>(fixed), 1, c);
          !kind)
            co_return codec::failure(kind.error());
        if (
          load<2, std::uint16_t>(fixed) != 0 || fixed[137] != 0
          || fixed[138] != 0 || fixed[139] != 0)
            co_return codec::failure(page_error(errc::malformed_data, c, 2));
        if (
          auto sc = detail::read_sc<4>(fixed, expected.history.segment, c); !sc)
            co_return codec::failure(sc.error());
        if (load<76, std::uint64_t>(fixed) != expected.position.value())
            co_return codec::failure(page_error(errc::wrong_context, c, 76));
        const auto coverage = detail::read_coverage<84>(fixed, c);
        if (!coverage) co_return codec::failure(coverage.error());
        const auto present = load<136, std::uint8_t>(fixed);
        if (present > 1)
            co_return codec::failure(page_error(errc::malformed_data, c, 136));
        std::optional<storage::coverage> last;
        if (present != 0) {
            const auto decoded = detail::read_coverage<140>(fixed, c);
            if (!decoded) co_return codec::failure(decoded.error());
            last = *decoded;
        } else if (!std::all_of(
                     fixed.begin() + 140, fixed.begin() + 188, [](char b) {
                         return b == 0;
                     }))
            co_return codec::failure(page_error(errc::malformed_data, c, 140));
        const boundary_fields boundary{
          *coverage, load<132, std::uint32_t>(fixed), last, 0};
        if (
          auto valid = detail::check_boundary(boundary, expected, c, true);
          !valid)
            co_return codec::failure(valid.error());
        const codec::extent_digest extent_digest{
          detail::read_digest<188>(fixed)};
        if (coverage->bytes().empty() && extent_digest.bytes() != empty_sha)
            co_return codec::failure(page_error(errc::malformed_data, c, 188));
        const auto total = load<220, std::uint32_t>(fixed);
        const auto count = detail::page_wire(
          page_count::make(load<224, std::uint32_t>(fixed)), c, 224);
        if (!count) co_return codec::failure(count.error());
        if (
          auto valid = check_counts(total, count->value(), work.policy(), c);
          !valid)
            co_return codec::failure(valid.error());
        const auto layout = detail::page_wire(
          aligned_envelope_layout::make(
            {byte_count{c.origin - start},
             sealed_footer_fixed_bytes,
             byte_count{static_cast<std::uint64_t>(count->value()) * 48U}},
            expected.history.alignment,
            work.policy(),
            {work.policy().config().max_page_bytes,
             work.policy().config().max_page_bytes}),
          c,
          228);
        if (!layout) co_return codec::failure(layout.error());
        if (
          input.total_bytes() != layout->body_bytes()
          || load<228, std::uint32_t>(fixed) != layout->padding_bytes().value())
            co_return codec::failure(page_error(errc::malformed_data, c, 228));
        const auto extent = detail::page_wire(
          layout->at(expected.position), c, 76);
        if (!extent) co_return codec::failure(extent.error());
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
              auto valid = detail::check_retry_ref(
                *ref,
                ordinal,
                first,
                expected.history.alignment,
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
            co_return codec::failure(page_error(errc::malformed_data, c, 220));
        if (
          auto read = co_await detail::read_padding(
            input, layout->padding_bytes(), work, c);
          !read)
            co_return codec::failure(read.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        // Prospective until the enclosing decoder drops its temporary body
        // alias and commits. The PageRef vector is the only retained charge.
        const auto retained = codec::detail::consume_decode_budget(
          work.policy(), original, {}, retained_metadata, c, c.origin);
        KWAQUE_INVARIANT(
          invariant_id{"KQ-SEALED-RESIDUAL"},
          retained.has_value(),
          "admitted root metadata exceeded its enclosing reservation");
        co_return decoded_sealed_footer{
          detail::sealed_codec::make(
            expected,
            boundary,
            extent_digest,
            digest,
            total,
            std::move(pages),
            *extent),
          *retained};
    }
};
} // namespace

codec::result<void> validate_sealed_footer(
  const sealed_footer& footer,
  const verified_extent& evidence,
  codec::field_context c) {
    c.family = static_cast<std::uint16_t>(detail::sealed_family);
    if (footer.location().history != evidence.context())
        return codec::failure(page_error(errc::wrong_context, c));
    if (!evidence.digest())
        return codec::failure(page_error(errc::invalid_argument, c));
    const auto fields = evidence.boundary();
    if (
      footer.coverage() != fields.coverage
      || footer.block_count() != fields.block_count
      || footer.last_block() != fields.last_block)
        return codec::failure(page_error(errc::malformed_data, c));
    if (footer.extent_digest() != *evidence.digest())
        return codec::failure(page_error(errc::corrupt_data, c));
    return {};
}
seastar::future<codec::result<decoded_sealed_footer>> decode_sealed_footer(
  bytes::fragmented_buffer_parser& input,
  footer_expectation expected,
  codec::immutable_object_digest digest,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context c,
  codec::input_boundary boundary) {
    c.family = static_cast<std::uint16_t>(detail::sealed_family);
    const auto fail = [](codec::error error) {
        return seastar::make_ready_future<codec::result<decoded_sealed_footer>>(
          codec::failure(error));
    };
    const auto start = codec::detail::integer_read_start(input, c, boundary);
    if (!start) return fail(start.error());
    if (auto valid = detail::validate_footer_location(expected, c); !valid)
        return fail(valid.error());
    return detail::decode_pinned<decoded_sealed_footer>(
      input,
      digest,
      memory,
      work,
      sealed_reader{expected, digest, *start, memory, {}},
      c,
      boundary);
}

seastar::future<codec::result<encoded_sealed_footer>> encode_sealed_footer(
  verified_extent evidence,
  footer_expectation expected,
  std::uint32_t total,
  std::span<const page_ref> refs,
  codec::cooperative_work& work,
  byte_count remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context c) {
    c.family = static_cast<std::uint16_t>(detail::sealed_family);
    const auto anchor = page_error(errc::success, c);
    if (
      auto ready = co_await work.admit(
        byte_count{1024}, item_count{64}, anchor);
      !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto valid = detail::validate_footer_location(expected, c); !valid)
        co_return codec::failure(valid.error());
    if (expected.history != evidence.context())
        co_return codec::failure(page_error(errc::wrong_context, c));
    if (
      !evidence.digest()
      || !detail::check_boundary(evidence.boundary(), expected, {}, true))
        co_return codec::failure(page_error(errc::invalid_argument, c));
    if (auto valid = check_counts(total, refs.size(), work.policy(), c); !valid)
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
          auto valid = detail::check_retry_ref(
            refs[i], i, first, expected.history.alignment, work.policy(), c);
          !valid)
            co_return codec::failure(valid.error());
        first += refs[i].entry_count();
    }
    if (first != total)
        co_return codec::failure(page_error(errc::invalid_argument, c));
    const auto layout = aligned_envelope_layout::make(
      {byte_count{32},
       sealed_footer_fixed_bytes,
       byte_count{refs.size() * 48U}},
      expected.history.alignment,
      work.policy(),
      {work.policy().config().max_page_bytes,
       work.policy().config().max_page_bytes});
    if (!layout)
        co_return codec::failure(
          codec::detail::allocation_cost_error(layout.error(), c, c.origin));
    if (
      !layout->at(expected.position)
      || layout->encoded_bytes().value()
           > std::numeric_limits<std::uint64_t>::max() - c.origin)
        co_return codec::failure(page_error(errc::out_of_range, c));
    std::array<char, 232> fixed{};
    store<0>(fixed, std::uint16_t{1});
    detail::write_sc<4>(fixed, expected.history.segment);
    store<76>(fixed, expected.position.value());
    detail::write_coverage<84>(fixed, evidence.boundary().coverage);
    store<132>(fixed, evidence.boundary().block_count);
    if (evidence.boundary().last_block) {
        store<136>(fixed, std::uint8_t{1});
        detail::write_coverage<140>(fixed, *evidence.boundary().last_block);
    }
    detail::write_digest<188>(fixed, *evidence.digest());
    store<220>(fixed, total);
    store<224>(fixed, static_cast<std::uint32_t>(refs.size()));
    store<228>(
      fixed, static_cast<std::uint32_t>(layout->padding_bytes().value()));
    bytes::fragmented_buffer tail, output;
    std::optional<codec::immutable_object_digest> digest;
    std::optional<codec::error> failed;
    std::exception_ptr exception;
    try {
        do {
            auto entries = co_await detail::encode_entries<48>(
              refs, detail::write_page_ref, work, remaining, charge, c);
            if (!entries) {
                failed = entries.error();
                break;
            }
            tail = std::move(*entries);
            auto encoded = co_await detail::encode_padded(
              fixed,
              std::move(tail),
              *layout,
              detail::sealed_family,
              work,
              remaining,
              charge,
              c);
            if (!encoded) {
                failed = encoded.error();
                break;
            }
            output = std::move(*encoded);
            const auto hash = co_await detail::hash_exact(output, work, c);
            if (!hash) {
                failed = hash.error();
                break;
            }
            digest = *hash;
        } while (false);
    } catch (...) {
        exception = std::current_exception();
    }
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    tail = bytes::fragmented_buffer{};
    if (!failed && !exception) {
        if (auto ready = work.poll(anchor); !ready) failed = ready.error();
    }
    if (failed || exception) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        output = bytes::fragmented_buffer{};
        if (exception) std::rethrow_exception(exception);
        co_return codec::failure(*failed);
    }
    co_return encoded_sealed_footer{std::move(output), *digest};
}
} // namespace kwaque::storage
