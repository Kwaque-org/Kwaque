#include "src/storage/footer_internal.h"
#include "src/storage/format_internal.h"

#include <limits>

namespace kwaque::storage {
namespace {
using detail::load;
using detail::store;
constexpr codec::envelope_extent_limits footer_limits{
  byte_count{65536}, byte_count{65536}};
codec::error at(
  errc code,
  codec::field_context context,
  footer_field field = footer_field::fixed_body,
  std::uint64_t offset = 0) noexcept {
    return codec::error{
      code,
      context.family,
      static_cast<std::uint16_t>(field),
      context.origin + offset};
}
template<typename T>
codec::result<T> from_wire(
  result<T> value,
  codec::field_context context,
  footer_field field,
  std::uint64_t offset) {
    if (value) return std::move(*value);
    return codec::failure(at(
      value.error() == errc::resource_exhausted ? errc::resource_exhausted
                                                : errc::malformed_data,
      context,
      field,
      offset));
}
template<std::size_t Offset, typename Id>
codec::result<void> check_id(
  const std::array<char, 192>& fixed,
  Id expected,
  codec::field_context context,
  footer_field field) {
    std::array<std::uint8_t, Id::width> raw{};
    static_assert(Offset + Id::width <= 192);
    for (std::size_t i = 0; i < raw.size(); ++i)
        raw[i] = static_cast<std::uint8_t>(fixed[Offset + i]);
    const auto value = from_wire(Id::make(raw), context, field, Offset);
    if (!value) return codec::failure(value.error());
    if (*value != expected)
        return codec::failure(at(errc::wrong_context, context, field, Offset));
    return {};
}
template<std::size_t Offset>
codec::result<storage::coverage> read_coverage(
  const std::array<char, 192>& fixed,
  codec::field_context context,
  footer_field field) {
    const auto logical = from_wire(
      model::range_logical_span::make(
        model::range_logical_end{load<Offset, std::uint64_t>(fixed)},
        model::range_logical_end{load<Offset + 8, std::uint64_t>(fixed)}),
      context,
      field,
      Offset);
    if (!logical) return codec::failure(logical.error());
    const auto physical = from_wire(
      model::segment_relative_span::make(
        model::segment_relative_end{load<Offset + 16, std::uint64_t>(fixed)},
        model::segment_relative_end{load<Offset + 24, std::uint64_t>(fixed)}),
      context,
      field,
      Offset + 16);
    if (!physical) return codec::failure(physical.error());
    const auto bytes = from_wire(
      model::file_byte_span::make(
        runtime::file_position{load<Offset + 32, std::uint64_t>(fixed)},
        runtime::file_position{load<Offset + 40, std::uint64_t>(fixed)}),
      context,
      field,
      Offset + 32);
    if (!bytes) return codec::failure(bytes.error());
    return storage::coverage{*logical, *physical, *bytes};
}
template<std::size_t Offset>
void write_coverage(
  std::array<char, 192>& fixed, const storage::coverage& value) {
    store<Offset>(fixed, value.logical().begin().value());
    store<Offset + 8>(fixed, value.logical().end().value());
    store<Offset + 16>(fixed, value.physical().begin().value());
    store<Offset + 24>(fixed, value.physical().end().value());
    store<Offset + 32>(fixed, value.bytes().begin().value());
    store<Offset + 40>(fixed, value.bytes().end().value());
}
bool contains(const auto& outer, const auto& inner) {
    return outer.begin() <= inner.begin() && inner.end() <= outer.end();
}

} // namespace

codec::result<void> detail::check_boundary(
  const boundary_fields& value,
  const footer_expectation& expected,
  codec::field_context context,
  bool sealed) {
    // The sealed fixed body inserts a four-byte subkind/reserved prefix.
    const auto shift = sealed ? 4U : 0U;
    const auto& scope = value.coverage;
    const auto& history = expected.history;
    if (
      scope.logical().begin() < history.logical_origin
      || scope.physical().begin() < history.physical_origin
      || scope.bytes().begin() < history.data_start
      || scope.bytes().end() > expected.position
      || !history.alignment.aligned(scope.bytes().begin())
      || !history.alignment.aligned(scope.bytes().end()))
        return codec::failure(at(
          errc::malformed_data, context, footer_field::coverage, 80 + shift));
    if (!sealed && scope.bytes().empty() && value.data_crc32c != 0)
        return codec::failure(
          at(errc::malformed_data, context, footer_field::data_crc32c, 184));
    if (value.block_count == 0) {
        if (
          value.last_block || !scope.physical().empty()
          || (!sealed && (!scope.logical().empty()
              || scope.logical().begin() != history.logical_origin
              || scope.physical().begin() != history.physical_origin)))
            return codec::failure(at(
              errc::malformed_data,
              context,
              footer_field::last_block,
              136 + shift));
        return {};
    }
    if (
      !value.last_block || scope.logical().empty() || scope.physical().empty()
      || scope.bytes().empty()
      || value.block_count > scope.physical().count().value()
      || scope.physical().count().value() > scope.logical().count().value())
        return codec::failure(at(
          errc::malformed_data,
          context,
          footer_field::block_count,
          128 + shift));
    const auto& last = *value.last_block;
    if (
      last.logical().empty() || last.physical().empty() || last.bytes().empty()
      || !contains(scope.logical(), last.logical())
      || !contains(scope.physical(), last.physical())
      || !contains(scope.bytes(), last.bytes())
      || last.physical().end() != scope.physical().end()
      || (!sealed && last.logical().end() != scope.logical().end())
      || last.logical().count().value() > codec::absolute_max_original_records
      || last.physical().count().value() > last.logical().count().value()
      || !history.alignment.aligned(last.bytes().begin())
      || !history.alignment.aligned(last.bytes().end()))
        return codec::failure(at(
          errc::malformed_data,
          context,
          footer_field::last_block,
          136 + shift));
    return {};
}

namespace detail {
codec::result<void> validate_history(
  const segment_history_context& history, codec::field_context context) {
    if (
      auto valid = parse_storage_profile(
        static_cast<std::uint16_t>(history.profile));
      !valid)
        return codec::failure(at(
          valid.error() == errc::unsupported_format ? errc::unsupported_format
                                                    : errc::invalid_argument,
          context));
    if (
      history.data_start.value() == 0
      || !history.alignment.aligned(history.data_start))
        return codec::failure(at(errc::invalid_argument, context));
    return {};
}
codec::result<void> validate_footer_location(
  const footer_expectation& expected, codec::field_context context) {
    if (auto valid = validate_history(expected.history, context); !valid)
        return valid;
    if (
      expected.position < expected.history.data_start
      || !expected.history.alignment.aligned(expected.position))
        return codec::failure(
          at(errc::invalid_argument, context, footer_field::position));
    return {};
}
class footer_codec final {
public:
    static durable_footer make(
      footer_expectation location,
      boundary_fields boundary,
      model::file_byte_span extent) noexcept {
        return durable_footer{location, boundary, extent};
    }
};
} // namespace detail

codec::result<void> validate_durable_footer(
  const durable_footer& footer,
  const verified_extent& evidence,
  codec::field_context context) {
    context.family = static_cast<std::uint16_t>(
      codec::format_family::durable_boundary_footer);
    if (footer.location().history != evidence.context())
        return codec::failure(at(errc::wrong_context, context));
    const auto actual = footer.boundary(), expected = evidence.boundary();
    if (actual.coverage != expected.coverage)
        return codec::failure(
          at(errc::malformed_data, context, footer_field::coverage));
    if (actual.block_count != expected.block_count)
        return codec::failure(
          at(errc::malformed_data, context, footer_field::block_count));
    if (actual.last_block != expected.last_block)
        return codec::failure(
          at(errc::malformed_data, context, footer_field::last_block));
    if (actual.data_crc32c != expected.data_crc32c)
        return codec::failure(
          at(errc::corrupt_data, context, footer_field::data_crc32c));
    return {};
}

seastar::future<codec::result<bytes::fragmented_buffer>> encode_durable_footer(
  verified_extent evidence,
  footer_expectation expected,
  codec::cooperative_work& work,
  byte_count remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context context) {
    context.family = static_cast<std::uint16_t>(
      codec::format_family::durable_boundary_footer);
    const auto anchor = at(errc::success, context);
    if (
      auto ready = co_await work.admit(
        byte_count{1024}, item_count{64}, anchor);
      !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (
      auto valid = detail::validate_footer_location(expected, context); !valid)
        co_return codec::failure(valid.error());
    if (expected.history != evidence.context())
        co_return codec::failure(at(errc::wrong_context, context));
    const auto boundary = evidence.boundary();
    // Structural errors here describe invalid placement/representation supplied
    // by the caller, not malformed wire input. Report the encoder's root
    // anchor.
    if (auto valid = detail::check_boundary(boundary, expected, {}); !valid)
        co_return codec::failure(at(errc::invalid_argument, context));
    const auto layout = aligned_envelope_layout::make(
      {byte_count{codec::envelope_prefix_bytes},
       durable_footer_fixed_bytes,
       {}},
      expected.history.alignment,
      work.policy(),
      footer_limits);
    if (!layout)
        co_return codec::failure(
          codec::detail::allocation_cost_error(
            layout.error(), context, context.origin));
    if (
      layout->encoded_bytes().value()
      > std::numeric_limits<std::uint64_t>::max() - context.origin)
        co_return codec::failure(at(errc::invalid_argument, context));
    if (auto extent = layout->at(expected.position); !extent)
        co_return codec::failure(
          codec::detail::allocation_cost_error(
            extent.error(), context, context.origin));
    std::array<char, 192> fixed{};
    const auto put = [&](auto id, std::size_t offset) {
        std::copy(
          id.bytes().begin(),
          id.bytes().end(),
          fixed.begin() + static_cast<std::ptrdiff_t>(offset));
    };
    const auto sc = expected.history.segment;
    put(sc.cluster(), 0);
    put(sc.topic(), 16);
    put(sc.range(), 32);
    put(sc.segment(), 48);
    store<64>(fixed, sc.generation().value());
    store<72>(fixed, expected.position.value());
    write_coverage<80>(fixed, boundary.coverage);
    store<128>(fixed, boundary.block_count);
    if (boundary.last_block) {
        store<132>(fixed, std::uint8_t{1});
        write_coverage<136>(fixed, *boundary.last_block);
    }
    store<184>(fixed, boundary.data_crc32c);
    store<188>(
      fixed, static_cast<std::uint32_t>(layout->padding_bytes().value()));
    auto output = co_await detail::encode_padded(
      fixed,
      bytes::fragmented_buffer{},
      *layout,
      codec::format_family::durable_boundary_footer,
      work,
      remaining,
      charge,
      context);
    if (!output) co_return codec::failure(output.error());
    if (auto ready = work.poll(anchor); !ready) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        output = bytes::fragmented_buffer{};
        co_return codec::failure(ready.error());
    }
    co_return std::move(*output);
}

namespace {
struct footer_reader final {
    footer_expectation expected;
    std::uint64_t start;
    seastar::future<codec::result<durable_footer>> operator()(
      bytes::fragmented_buffer_parser& input,
      codec::field_context context,
      codec::input_boundary,
      codec::decode_budget,
      codec::cooperative_work& work) {
        std::array<char, 192> fixed{};
        if (
          auto read = co_await detail::read_fixed(input, fixed, work, context);
          !read)
            co_return codec::failure(read.error());
        const auto anchor = at(errc::success, context);
        if (
          auto ready = co_await work.admit(
            byte_count{1024}, item_count{64}, anchor);
          !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        const auto sc = expected.history.segment;
        if (
          auto id = check_id<0>(
            fixed, sc.cluster(), context, footer_field::cluster);
          !id)
            co_return codec::failure(id.error());
        if (
          auto id = check_id<16>(
            fixed, sc.topic(), context, footer_field::topic);
          !id)
            co_return codec::failure(id.error());
        if (
          auto id = check_id<32>(
            fixed, sc.range(), context, footer_field::range);
          !id)
            co_return codec::failure(id.error());
        if (
          auto id = check_id<48>(
            fixed, sc.segment(), context, footer_field::segment);
          !id)
            co_return codec::failure(id.error());
        const auto generation = from_wire(
          model::segment_generation::make(load<64, std::uint64_t>(fixed)),
          context,
          footer_field::generation,
          64);
        if (!generation) co_return codec::failure(generation.error());
        if (*generation != sc.generation())
            co_return codec::failure(
              at(errc::wrong_context, context, footer_field::generation, 64));
        if (load<72, std::uint64_t>(fixed) != expected.position.value())
            co_return codec::failure(
              at(errc::wrong_context, context, footer_field::position, 72));
        const auto scope = read_coverage<80>(
          fixed, context, footer_field::coverage);
        if (!scope) co_return codec::failure(scope.error());
        const auto present = load<132, std::uint8_t>(fixed);
        if (present > 1)
            co_return codec::failure(at(
              errc::malformed_data, context, footer_field::last_present, 132));
        if (fixed[133] != 0 || fixed[134] != 0 || fixed[135] != 0)
            co_return codec::failure(
              at(errc::malformed_data, context, footer_field::reserved, 133));
        std::optional<storage::coverage> last;
        if (present != 0) {
            auto parsed = read_coverage<136>(
              fixed, context, footer_field::last_block);
            if (!parsed) co_return codec::failure(parsed.error());
            last = *parsed;
        } else if (!std::all_of(
                     fixed.begin() + 136, fixed.begin() + 184, [](char byte) {
                         return byte == 0;
                     }))
            co_return codec::failure(
              at(errc::malformed_data, context, footer_field::last_block, 136));
        const boundary_fields boundary{
          *scope,
          load<128, std::uint32_t>(fixed),
          last,
          load<184, std::uint32_t>(fixed)};
        if (
          auto valid = detail::check_boundary(boundary, expected, context);
          !valid)
            co_return codec::failure(valid.error());
        const auto layout = from_wire(
          aligned_envelope_layout::make(
            {byte_count{context.origin - start},
             durable_footer_fixed_bytes,
             {}},
            expected.history.alignment,
            work.policy(),
            footer_limits),
          context,
          footer_field::padding,
          188);
        if (!layout) co_return codec::failure(layout.error());
        if (
          input.total_bytes() != layout->body_bytes()
          || load<188, std::uint32_t>(fixed) != layout->padding_bytes().value())
            co_return codec::failure(
              at(errc::malformed_data, context, footer_field::padding, 188));
        const auto extent = from_wire(
          layout->at(expected.position), context, footer_field::position, 72);
        if (!extent) co_return codec::failure(extent.error());
        auto padding_context = context;
        padding_context.field = static_cast<std::uint16_t>(
          footer_field::padding);
        if (
          auto padding = co_await detail::read_padding(
            input, layout->padding_bytes(), work, padding_context);
          !padding)
            co_return codec::failure(padding.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        co_return detail::footer_codec::make(expected, boundary, *extent);
    }
};
} // namespace
seastar::future<codec::result<durable_footer>> decode_durable_footer(
  bytes::fragmented_buffer_parser& input,
  footer_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context,
  codec::input_boundary boundary) {
    context.family = static_cast<std::uint16_t>(
      codec::format_family::durable_boundary_footer);
    const auto start = codec::detail::integer_read_start(
      input, context, boundary);
    const auto fail = [](codec::error error) {
        return seastar::make_ready_future<codec::result<durable_footer>>(
          codec::failure(error));
    };
    if (!start) return fail(start.error());
    if (auto ready = work.poll(at(errc::success, context)); !ready)
        return fail(ready.error());
    if (
      auto valid = detail::validate_footer_location(expected, context); !valid)
        return fail(valid.error());
    return codec::decode_envelope<durable_footer>(
      input,
      codec::format_family::durable_boundary_footer,
      footer_limits,
      memory,
      work,
      footer_reader{expected, *start},
      context,
      boundary);
}
} // namespace kwaque::storage
