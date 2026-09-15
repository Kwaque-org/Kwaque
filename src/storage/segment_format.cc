#include "src/storage/segment_format.h"

#include "src/storage/format_internal.h"

#include <exception>
#include <limits>
#include <optional>

namespace kwaque::storage {
namespace {
using detail::load;
using detail::store;
using kwaque::bytes::fragmented_buffer;
using kwaque::bytes::fragmented_buffer_parser;
codec::error at(
  errc code,
  codec::field_context context,
  segment_field field = segment_field::fixed_body,
  std::uint64_t offset = 0) noexcept {
    return codec::error{
      code,
      context.family,
      static_cast<std::uint16_t>(field),
      context.origin + offset};
}
constexpr codec::envelope_extent_limits header_limits{
  byte_count{65536}, byte_count{65536}};
codec::envelope_extent_limits block_limits(const codec::limits& policy) {
    const auto config = policy.config();
    return {
      config.max_encoded_body_bytes,
      byte_count{
        config.max_encoded_body_bytes.value()
        + config.max_header_bytes.value()}};
}
template<typename T>
codec::result<T> from_wire(
  result<T> value,
  codec::field_context context,
  segment_field field,
  std::uint64_t offset) {
    if (value) return std::move(*value);
    const auto code = value.error();
    return codec::failure(at(
      code == errc::unsupported_format   ? errc::unsupported_format
      : code == errc::resource_exhausted ? errc::resource_exhausted
                                         : errc::malformed_data,
      context,
      field,
      offset));
}
template<std::size_t Offset, typename Id, std::size_t N>
codec::result<Id> read_id(
  const std::array<char, N>& fixed,
  codec::field_context context,
  segment_field field) {
    static_assert(Offset + Id::width <= N);
    std::array<std::uint8_t, Id::width> bytes{};
    for (std::size_t i = 0; i < bytes.size(); ++i)
        bytes[i] = static_cast<std::uint8_t>(fixed[Offset + i]);
    return from_wire(Id::make(bytes), context, field, Offset);
}
template<std::size_t N>
void write_context(std::array<char, N>& fixed, segment_context context) {
    const auto put = [&](auto id, std::size_t offset) {
        std::copy(
          id.bytes().begin(),
          id.bytes().end(),
          fixed.begin() + static_cast<std::ptrdiff_t>(offset));
    };
    put(context.cluster(), 0);
    put(context.topic(), 16);
    put(context.range(), 32);
    put(context.segment(), 48);
    store<64>(fixed, context.generation().value());
}
template<std::size_t N>
codec::result<void> check_context(
  const std::array<char, N>& fixed,
  segment_context expected,
  codec::field_context context) {
    const auto cluster = read_id<0, model::cluster_id>(
      fixed, context, segment_field::cluster);
    if (!cluster) return codec::failure(cluster.error());
    if (*cluster != expected.cluster())
        return codec::failure(
          at(errc::wrong_context, context, segment_field::cluster, 0));
    const auto topic = read_id<16, model::topic_id>(
      fixed, context, segment_field::topic);
    if (!topic) return codec::failure(topic.error());
    if (*topic != expected.topic())
        return codec::failure(
          at(errc::wrong_context, context, segment_field::topic, 16));
    const auto range = read_id<32, model::range_id>(
      fixed, context, segment_field::range);
    if (!range) return codec::failure(range.error());
    if (*range != expected.range())
        return codec::failure(
          at(errc::wrong_context, context, segment_field::range, 32));
    const auto segment = read_id<48, model::segment_id>(
      fixed, context, segment_field::segment);
    if (!segment) return codec::failure(segment.error());
    if (*segment != expected.segment())
        return codec::failure(
          at(errc::wrong_context, context, segment_field::segment, 48));
    const auto generation = from_wire(
      model::segment_generation::make(load<64, std::uint64_t>(fixed)),
      context,
      segment_field::generation,
      64);
    if (!generation) return codec::failure(generation.error());
    if (*generation != expected.generation())
        return codec::failure(
          at(errc::wrong_context, context, segment_field::generation, 64));
    return {};
}

codec::result<void> check_expectation(
  const segment_block_expectation& expected, codec::field_context context) {
    if (
      auto profile = parse_storage_profile(
        static_cast<std::uint16_t>(expected.profile));
      !profile)
        return codec::failure(at(
          profile.error() == errc::unsupported_format ? errc::unsupported_format
                                                      : errc::invalid_argument,
          context,
          segment_field::storage_profile));
    const auto& location = expected.location;
    if (expected.data_start.value() == 0 || !location.alignment().aligned(expected.data_start)
        || location.position() < expected.data_start
        || expected.batch.topic != location.segment().topic()
        || expected.batch.range != location.segment().range()
        || (expected.batch.original_binding
            && (expected.batch.original_binding->topic() != expected.batch.topic
                || expected.batch.original_binding->range() != expected.batch.range)))
        return codec::failure(at(errc::invalid_argument, context));
    return {};
}

struct header_reader final {
    segment_header expected;
    std::uint64_t start;
    seastar::future<codec::result<decoded_segment_header>> operator()(
      fragmented_buffer_parser& input,
      codec::field_context context,
      codec::input_boundary,
      codec::decode_budget,
      codec::cooperative_work& work) {
        std::array<char, 96> fixed{};
        if (
          auto read = co_await detail::read_fixed(input, fixed, work, context);
          !read)
            co_return codec::failure(read.error());
        const auto anchor = at(errc::success, context);
        if (
          auto ready = co_await work.admit(
            byte_count{512}, item_count{32}, anchor);
          !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        if (
          auto identity = check_context(fixed, expected.context(), context);
          !identity)
            co_return codec::failure(identity.error());
        const auto profile = from_wire(
          parse_storage_profile(load<80, std::uint16_t>(fixed)),
          context,
          segment_field::storage_profile,
          80);
        if (!profile) co_return codec::failure(profile.error());
        if (*profile != expected.profile())
            co_return codec::failure(at(
              errc::wrong_context,
              context,
              segment_field::storage_profile,
              80));
        const auto record_profile = load<82, std::uint16_t>(fixed);
        if (record_profile != 1)
            co_return codec::failure(at(
              record_profile == 0 ? errc::malformed_data
                                  : errc::unsupported_format,
              context,
              segment_field::record_profile,
              82));
        const auto alignment = from_wire(
          storage_alignment::make(byte_count{load<84, std::uint32_t>(fixed)}),
          context,
          segment_field::alignment,
          84);
        if (!alignment) co_return codec::failure(alignment.error());
        if (*alignment != expected.alignment())
            co_return codec::failure(
              at(errc::wrong_context, context, segment_field::alignment, 84));
        if (load<92, std::uint32_t>(fixed) != 0)
            co_return codec::failure(
              at(errc::malformed_data, context, segment_field::reserved, 92));
        if (load<72, std::uint64_t>(fixed) != expected.logical_origin().value())
            co_return codec::failure(at(
              errc::wrong_context, context, segment_field::logical_origin, 72));
        const auto layout = from_wire(
          aligned_envelope_layout::make(
            {byte_count{context.origin - start},
             segment_header_fixed_bytes,
             {}},
            *alignment,
            work.policy(),
            header_limits),
          context,
          segment_field::padding,
          88);
        if (!layout) co_return codec::failure(layout.error());
        if (
          load<88, std::uint32_t>(fixed) != layout->padding_bytes().value()
          || input.total_bytes() != layout->body_bytes())
            co_return codec::failure(
              at(errc::malformed_data, context, segment_field::padding, 88));
        auto padding_context = context;
        padding_context.field = static_cast<std::uint16_t>(
          segment_field::padding);
        if (
          auto padding = co_await detail::read_padding(
            input, layout->padding_bytes(), work, padding_context);
          !padding)
            co_return codec::failure(padding.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        co_return decoded_segment_header{expected, layout->at({}).value()};
    }
};
} // namespace

namespace detail {
class segment_codec final {
public:
    static complete_block_descriptor describe(
      segment_context context,
      model::segment_relative_span physical,
      model::file_byte_span bytes,
      assigned_batch_info child) noexcept {
        return complete_block_descriptor{context, physical, bytes, child};
    }
    static segment_block pair(
      fragmented_buffer bytes, complete_block_descriptor descriptor) noexcept {
        return segment_block{std::move(bytes), descriptor};
    }
};
} // namespace detail

result<segment_header> segment_header::make(
  segment_context context,
  model::range_logical_end origin,
  storage_alignment alignment,
  storage_profile profile) noexcept {
    if (
      auto valid = parse_storage_profile(static_cast<std::uint16_t>(profile));
      !valid)
        return failure(valid.error());
    return segment_header{context, origin, alignment, profile};
}

result<void> validate_initial_append(
  const encoded_assigned_batch& batch,
  const segment_write_context& current) noexcept {
    if (batch.bytes().empty()) return failure(errc::invalid_argument);
    const auto info = batch.info();
    const auto original = info.context.submitted();
    const auto binding = original.binding();
    if (
      info.context.retained_count().value()
      != original.original_count().value())
        return failure(errc::invalid_argument);
    if (
      binding.topic() != current.segment().topic()
      || binding.range() != current.segment().range()
      || binding.segment() != current.segment().segment()
      || binding.generation() != current.segment().generation())
        return failure(errc::wrong_context);
    return {};
}

seastar::future<codec::result<fragmented_buffer>> encode_segment_header(
  segment_header header,
  codec::cooperative_work& work,
  byte_count remaining,
  kwaque::bytes::allocation_charge_fn charge,
  codec::field_context context) {
    context.family = static_cast<std::uint16_t>(
      codec::format_family::segment_header);
    const auto anchor = at(errc::success, context);
    if (
      auto ready = co_await work.admit(byte_count{512}, item_count{32}, anchor);
      !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    const auto layout = aligned_envelope_layout::make(
      {byte_count{codec::envelope_prefix_bytes},
       segment_header_fixed_bytes,
       {}},
      header.alignment(),
      work.policy(),
      header_limits);
    if (!layout)
        co_return codec::failure(
          codec::detail::allocation_cost_error(
            layout.error(), context, context.origin));
    if (
      layout->encoded_bytes().value()
      > std::numeric_limits<std::uint64_t>::max() - context.origin)
        co_return codec::failure(at(errc::invalid_argument, context));
    std::array<char, 96> fixed{};
    write_context(fixed, header.context());
    store<72>(fixed, header.logical_origin().value());
    store<80>(fixed, static_cast<std::uint16_t>(header.profile()));
    store<82>(fixed, std::uint16_t{1});
    store<84>(
      fixed, static_cast<std::uint32_t>(header.alignment().bytes().value()));
    store<88>(
      fixed, static_cast<std::uint32_t>(layout->padding_bytes().value()));
    auto output = co_await detail::encode_padded(
      fixed,
      fragmented_buffer{},
      *layout,
      codec::format_family::segment_header,
      work,
      remaining,
      charge,
      context);
    if (!output) co_return codec::failure(output.error());
    if (auto ready = work.poll(anchor); !ready) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        output = fragmented_buffer{};
        co_return codec::failure(ready.error());
    }
    co_return std::move(*output);
}

seastar::future<codec::result<decoded_segment_header>> decode_segment_header(
  fragmented_buffer_parser& input,
  segment_header expected,
  runtime::file_position position,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context,
  codec::input_boundary boundary) {
    context.family = static_cast<std::uint16_t>(
      codec::format_family::segment_header);
    const auto start = codec::detail::integer_read_start(
      input, context, boundary);
    const auto fail = [](codec::error error) {
        return seastar::make_ready_future<
          codec::result<decoded_segment_header>>(codec::failure(error));
    };
    if (!start) return fail(start.error());
    if (auto ready = work.poll(at(errc::success, context)); !ready)
        return fail(ready.error());
    if (position.value() != 0)
        return fail(
          at(errc::wrong_context, context, segment_field::block_position));
    return codec::decode_envelope<decoded_segment_header>(
      input,
      codec::format_family::segment_header,
      header_limits,
      memory,
      work,
      header_reader{expected, *start},
      context,
      boundary);
}

namespace {
struct block_reader final {
    segment_block_expectation expected;
    std::uint64_t start;
    seastar::future<codec::result<complete_block_descriptor>> operator()(
      fragmented_buffer_parser& input,
      codec::field_context context,
      codec::input_boundary,
      codec::decode_budget memory,
      codec::cooperative_work& work) {
        fragmented_buffer child_bytes_owner;
        std::optional<codec::result<encoded_assigned_batch>> child;
        std::optional<complete_block_descriptor> result;
        std::optional<codec::error> failed;
        std::exception_ptr exception;
        const auto anchor = at(errc::success, context);
        try {
            do {
                std::array<char, 120> fixed{};
                if (
                  auto read = co_await detail::read_fixed(
                    input, fixed, work, context);
                  !read) {
                    failed = read.error();
                    break;
                }
                if (
                  auto ready = co_await work.admit(
                    byte_count{512}, item_count{32}, anchor);
                  !ready) {
                    failed = ready.error();
                    break;
                }
                if (auto ready = work.poll(anchor); !ready) {
                    failed = ready.error();
                    break;
                }
                if (
                  auto identity = check_context(
                    fixed, expected.location.segment(), context);
                  !identity) {
                    failed = identity.error();
                    break;
                }
                if (
                  load<72, std::uint64_t>(fixed)
                  != expected.location.position().value()) {
                    failed = at(
                      errc::wrong_context,
                      context,
                      segment_field::block_position,
                      72);
                    break;
                }
                if (
                  load<80, std::uint64_t>(fixed)
                  != expected.location.physical_begin().value()) {
                    failed = at(
                      errc::wrong_context,
                      context,
                      segment_field::physical_begin,
                      80);
                    break;
                }
                const byte_count child_bytes{load<112, std::uint32_t>(fixed)};
                if (child_bytes.value() == 0) {
                    failed = at(
                      errc::malformed_data,
                      context,
                      segment_field::assigned_bytes,
                      112);
                    break;
                }
                const auto layout = from_wire(
                  aligned_envelope_layout::make(
                    {byte_count{context.origin - start},
                     segment_block_fixed_bytes,
                     child_bytes},
                    expected.location.alignment(),
                    work.policy(),
                    block_limits(work.policy())),
                  context,
                  segment_field::assigned_bytes,
                  112);
                if (!layout) {
                    failed = layout.error();
                    break;
                }
                if (
                  layout->body_bytes() != input.total_bytes()
                  || load<116, std::uint32_t>(fixed)
                       != layout->padding_bytes().value()) {
                    failed = at(
                      errc::malformed_data,
                      context,
                      segment_field::padding,
                      116);
                    break;
                }
                const auto extent = from_wire(
                  layout->at(expected.location.position()),
                  context,
                  segment_field::block_position,
                  72);
                if (!extent) {
                    failed = extent.error();
                    break;
                }
                if (auto ready = co_await work.checkpoint(anchor); !ready) {
                    failed = ready.error();
                    break;
                }
                if (auto ready = work.poll(anchor); !ready) {
                    failed = ready.error();
                    break;
                }
                const codec::field_context child_context{
                  context.origin + 120U,
                  static_cast<std::uint16_t>(
                    codec::format_family::assigned_batch),
                  static_cast<std::uint16_t>(segment_field::assigned_batch)};
                auto alias_context = context;
                alias_context.field = static_cast<std::uint16_t>(
                  segment_field::assigned_batch);
                const auto remaining = codec::detail::admit_envelope_child(
                  input,
                  child_bytes,
                  child_bytes,
                  memory,
                  work.policy(),
                  alias_context);
                if (!remaining) {
                    failed = remaining.error();
                    break;
                }
                auto shared = input.read_buffer(child_bytes);
                if (!shared) {
                    failed = codec::detail::allocation_cost_error(
                      shared.error(), context, context.origin + 120U);
                    break;
                }
                child_bytes_owner = std::move(*shared);
                child.emplace(
                  co_await validate_encoded_assigned_batch(
                    std::move(child_bytes_owner),
                    expected.batch,
                    *remaining,
                    work,
                    child_context));
                if (!child->has_value()) {
                    failed = child->error();
                    break;
                }
                if (auto ready = work.poll(anchor); !ready) {
                    failed = ready.error();
                    break;
                }
                const auto info = (*child)->info();
                const auto physical = from_wire(
                  model::segment_relative_span::from_count(
                    expected.location.physical_begin(),
                    model::segment_record_count{
                      info.context.retained_count().value()}),
                  context,
                  segment_field::physical_end,
                  88);
                if (!physical) {
                    failed = physical.error();
                    break;
                }
                if (load<88, std::uint64_t>(fixed) != physical->end().value()) {
                    failed = at(
                      errc::malformed_data,
                      context,
                      segment_field::physical_end,
                      88);
                    break;
                }
                if (
                  load<96, std::uint64_t>(fixed)
                  != info.context.logical_span().begin().value()) {
                    failed = at(
                      errc::malformed_data,
                      context,
                      segment_field::logical_begin,
                      96);
                    break;
                }
                if (
                  load<104, std::uint64_t>(fixed)
                  != info.context.logical_span().end().value()) {
                    failed = at(
                      errc::malformed_data,
                      context,
                      segment_field::logical_end,
                      104);
                    break;
                }
                auto padding_context = context;
                padding_context.field = static_cast<std::uint16_t>(
                  segment_field::padding);
                if (
                  auto padding = co_await detail::read_padding(
                    input, layout->padding_bytes(), work, padding_context);
                  !padding) {
                    failed = padding.error();
                    break;
                }
                result.emplace(
                  detail::segment_codec::describe(
                    expected.location.segment(), *physical, *extent, info));
            } while (false);
        } catch (...) {
            exception = std::current_exception();
        }
        if (child) {
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            child.reset();
        }
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        child_bytes_owner = fragmented_buffer{};
        if (exception) std::rethrow_exception(exception);
        if (failed) co_return codec::failure(*failed);
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        KWAQUE_INVARIANT(
          invariant_id{"KQ-SEGMENT-BLOCK-DESCRIPTOR"},
          result.has_value(),
          "block validation completed without a descriptor");
        co_return *result;
    }
};
} // namespace

seastar::future<codec::result<segment_block>> encode_segment_block(
  encoded_assigned_batch&& source,
  segment_block_expectation expected,
  codec::cooperative_work& work,
  byte_count remaining,
  kwaque::bytes::allocation_charge_fn charge,
  codec::field_context context) {
    std::optional<encoded_assigned_batch> child{
      std::in_place, std::move(source)};
    fragmented_buffer payload;
    context.family = static_cast<std::uint16_t>(
      codec::format_family::segment_batch_block);
    const auto anchor = at(errc::success, context);
    std::optional<codec::result<fragmented_buffer>> encoded;
    std::optional<complete_block_descriptor> descriptor;
    std::optional<codec::error> failed;
    std::exception_ptr exception;
    try {
        do {
            if (auto ready = co_await work.checkpoint(anchor); !ready) {
                failed = ready.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            if (auto valid = check_expectation(expected, context); !valid) {
                failed = valid.error();
                break;
            }
            const auto layout = aligned_envelope_layout::make(
              {byte_count{codec::envelope_prefix_bytes},
               segment_block_fixed_bytes,
               child->bytes().size()},
              expected.location.alignment(),
              work.policy(),
              block_limits(work.policy()));
            if (!layout) {
                failed = codec::detail::allocation_cost_error(
                  layout.error(), context, context.origin);
                break;
            }
            if (
              layout->encoded_bytes().value()
              > std::numeric_limits<std::uint64_t>::max() - context.origin) {
                failed = at(errc::invalid_argument, context);
                break;
            }
            const auto extent = layout->at(expected.location.position());
            if (!extent) {
                failed = codec::detail::allocation_cost_error(
                  extent.error(), context, context.origin);
                break;
            }
            const auto info = child->info();
            const auto physical = model::segment_relative_span::from_count(
              expected.location.physical_begin(),
              model::segment_record_count{
                info.context.retained_count().value()});
            if (!physical) {
                failed = at(
                  errc::out_of_range, context, segment_field::physical_end);
                break;
            }
            const auto cost = child->bytes().allocation_cost(charge);
            if (!cost) {
                failed = codec::detail::allocation_cost_error(
                  cost.error(), context, context.origin);
                break;
            }
            const auto memory = codec::detail::consume_decode_budget(
              work.policy(),
              {remaining, work.policy().config().max_metadata_bytes, charge},
              cost->backing,
              *cost->descriptors.checked_add(cost->share_controls),
              context,
              context.origin);
            if (!memory) {
                failed = memory.error();
                break;
            }
            const codec::field_context child_context{
              context.origin + codec::envelope_prefix_bytes
                + segment_block_fixed_bytes.value(),
              static_cast<std::uint16_t>(codec::format_family::assigned_batch),
              static_cast<std::uint16_t>(segment_field::assigned_batch)};
            if (
              auto valid = co_await child->validate(
                expected.batch, *memory, work, child_context);
              !valid) {
                failed = valid.error();
                break;
            }
            if (
              auto ready = co_await work.admit(
                byte_count{512}, item_count{32}, anchor);
              !ready) {
                failed = ready.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            std::array<char, 120> fixed{};
            write_context(fixed, expected.location.segment());
            store<72>(fixed, expected.location.position().value());
            store<80>(fixed, physical->begin().value());
            store<88>(fixed, physical->end().value());
            store<96>(fixed, info.context.logical_span().begin().value());
            store<104>(fixed, info.context.logical_span().end().value());
            store<112>(
              fixed, static_cast<std::uint32_t>(child->bytes().size().value()));
            store<116>(
              fixed,
              static_cast<std::uint32_t>(layout->padding_bytes().value()));
            payload = std::move(*child).release_bytes();
            encoded.emplace(
              co_await detail::encode_padded(
                fixed,
                std::move(payload),
                *layout,
                codec::format_family::segment_batch_block,
                work,
                remaining,
                charge,
                context));
            if (!encoded->has_value()) {
                failed = encoded->error();
                break;
            }
            descriptor.emplace(
              detail::segment_codec::describe(
                expected.location.segment(), *physical, *extent, info));
        } while (false);
    } catch (...) {
        exception = std::current_exception();
    }
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    child.reset();
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    payload = fragmented_buffer{};
    if (!failed && !exception) {
        if (auto ready = work.poll(anchor); !ready) failed = ready.error();
    }
    if (failed || exception) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        encoded.reset();
        if (exception) std::rethrow_exception(exception);
        co_return codec::failure(*failed);
    }
    KWAQUE_INVARIANT(
      invariant_id{"KQ-SEGMENT-BLOCK-ENCODE"},
      encoded && encoded->has_value() && descriptor,
      "block encoding completed without an owning pair");
    co_return detail::segment_codec::pair(std::move(**encoded), *descriptor);
}

seastar::future<codec::result<decoded_segment_block>> decode_segment_block(
  fragmented_buffer_parser& input,
  segment_block_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context,
  codec::input_boundary boundary) {
    context.family = static_cast<std::uint16_t>(
      codec::format_family::segment_batch_block);
    const auto start = codec::detail::integer_read_start(
      input, context, boundary);
    if (!start) co_return codec::failure(start.error());
    const auto anchor = at(errc::success, context);
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto valid = check_expectation(expected, context); !valid)
        co_return codec::failure(valid.error());
    const auto depth = input.checkpoint_depth();
    if (auto marked = input.push_checkpoint(); !marked)
        co_return codec::failure(
          codec::detail::allocation_cost_error(
            marked.error(), context, *start));
    std::optional<codec::result<complete_block_descriptor>> descriptor;
    byte_count length;
    {
        // Validate once, then restore the cursor so the exact verified outer
        // envelope can be retained. The envelope decoder joins all inner
        // owners before returning.
        codec::detail::parser_transaction_guard rewind{input, depth};
        const auto initial = input.bytes_consumed();
        descriptor.emplace(
          co_await codec::decode_envelope<complete_block_descriptor>(
            input,
            codec::format_family::segment_batch_block,
            block_limits(work.policy()),
            memory,
            work,
            block_reader{std::move(expected), *start},
            context,
            boundary));
        length = *input.bytes_consumed().checked_sub(initial);
    }
    if (!descriptor->has_value()) co_return codec::failure(descriptor->error());
    if (auto ready = co_await work.checkpoint(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    const auto remaining = codec::detail::admit_envelope_child(
      input, length, length, memory, work.policy(), context);
    if (!remaining) co_return codec::failure(remaining.error());
    auto bytes = input.peek_buffer(length);
    if (!bytes)
        co_return codec::failure(
          codec::detail::allocation_cost_error(bytes.error(), context, *start));
    const auto cost = bytes->allocation_cost(memory.charge);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-SEGMENT-BLOCK-RETAINED"},
      cost.has_value(),
      "validated block alias has no allocation cost");
    const auto retained = codec::detail::consume_decode_budget(
      work.policy(), memory, {}, cost->descriptors, context, *start);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-SEGMENT-BLOCK-RETAINED"},
      retained.has_value(),
      "block alias exceeds admitted metadata");
    // No await separates alias creation, the final abort poll and advancement.
    if (auto ready = work.poll(anchor); !ready) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        bytes = fragmented_buffer{};
        co_return codec::failure(ready.error());
    }
    const auto advanced = input.skip(length);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-SEGMENT-BLOCK-COMMIT"},
      advanced.has_value(),
      "validated block could not advance");
    co_return decoded_segment_block{
      detail::segment_codec::pair(std::move(*bytes), **descriptor), *retained};
}
} // namespace kwaque::storage
