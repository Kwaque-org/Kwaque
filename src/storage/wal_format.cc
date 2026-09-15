#include "src/storage/wal_format.h"

#include "src/storage/format_internal.h"

#include <exception>
#include <limits>
#include <optional>

namespace kwaque::storage {
namespace {
using bytes::fragmented_buffer;
using bytes::fragmented_buffer_parser;
using detail::load;
using detail::store;

codec::error at(
  errc code,
  codec::field_context context,
  wal_field field = wal_field::fixed_body,
  std::uint64_t offset = 0) noexcept {
    return codec::error{
      code,
      context.family,
      static_cast<std::uint16_t>(field),
      context.origin + offset};
}
codec::envelope_extent_limits limits(const codec::limits& policy) {
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
  wal_field field,
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
template<std::size_t Offset, typename Id>
codec::result<void> check_id(
  const std::array<char, 136>& fixed,
  Id expected,
  codec::field_context context,
  wal_field field) {
    static_assert(Offset + Id::width <= 136);
    std::array<std::uint8_t, Id::width> raw{};
    for (std::size_t i = 0; i < raw.size(); ++i)
        raw[i] = static_cast<std::uint8_t>(fixed[Offset + i]);
    const auto id = from_wire(Id::make(raw), context, field, Offset);
    if (!id) return codec::failure(id.error());
    if (*id != expected)
        return codec::failure(at(errc::wrong_context, context, field, Offset));
    return {};
}
codec::result<void> check_expectation(
  const wal_prepare_expectation& expected, codec::field_context context) {
    if (
      auto valid = parse_replay_profile(
        static_cast<std::uint16_t>(expected.profile));
      !valid)
        return codec::failure(at(
          valid.error() == errc::unsupported_format ? errc::unsupported_format
                                                    : errc::invalid_argument,
          context,
          wal_field::replay_profile));
    if (
      auto valid = parse_storage_profile(
        static_cast<std::uint16_t>(expected.target_profile));
      !valid)
        return codec::failure(at(
          valid.error() == errc::unsupported_format ? errc::unsupported_format
                                                    : errc::invalid_argument,
          context));
    if (!expected.routing_epoch.is_valid()
        || expected.target_data_start.value() == 0
        || !expected.target.alignment().aligned(expected.target_data_start)
        || expected.target.position() < expected.target_data_start
        || expected.batch.topic != expected.target.segment().topic()
        || expected.batch.range != expected.target.segment().range()
        || (expected.batch.original_binding
            && (expected.batch.original_binding->topic() != expected.batch.topic
                || expected.batch.original_binding->range() != expected.batch.range
                || expected.batch.original_binding->routing_epoch() != expected.routing_epoch)))
        return codec::failure(at(errc::invalid_argument, context));
    return {};
}
codec::result<void> check_fixed(
  const std::array<char, 136>& fixed,
  const wal_prepare_expectation& expected,
  codec::field_context context) {
    if (
      auto id = check_id<0>(
        fixed, expected.wal.incarnation(), context, wal_field::incarnation);
      !id)
        return id;
    if (load<16, std::uint64_t>(fixed) != expected.wal.position().value())
        return codec::failure(
          at(errc::wrong_context, context, wal_field::wal_position, 16));
    const auto target = expected.target.segment();
    if (
      auto id = check_id<24>(
        fixed, target.cluster(), context, wal_field::cluster);
      !id)
        return id;
    if (
      auto id = check_id<40>(fixed, target.topic(), context, wal_field::topic);
      !id)
        return id;
    if (
      auto id = check_id<56>(fixed, target.range(), context, wal_field::range);
      !id)
        return id;
    if (
      auto id = check_id<72>(
        fixed, target.segment(), context, wal_field::segment);
      !id)
        return id;
    const auto generation = from_wire(
      model::segment_generation::make(load<88, std::uint64_t>(fixed)),
      context,
      wal_field::generation,
      88);
    if (!generation) return codec::failure(generation.error());
    if (*generation != target.generation())
        return codec::failure(
          at(errc::wrong_context, context, wal_field::generation, 88));
    const auto routing = from_wire(
      model::range_routing_epoch::make(load<96, std::uint64_t>(fixed)),
      context,
      wal_field::routing_epoch,
      96);
    if (!routing) return codec::failure(routing.error());
    if (*routing != expected.routing_epoch)
        return codec::failure(
          at(errc::wrong_context, context, wal_field::routing_epoch, 96));
    if (
      load<104, std::uint64_t>(fixed)
      != expected.target.physical_begin().value())
        return codec::failure(
          at(errc::wrong_context, context, wal_field::physical_begin, 104));
    if (load<112, std::uint64_t>(fixed) != expected.target.position().value())
        return codec::failure(
          at(errc::wrong_context, context, wal_field::segment_position, 112));
    const auto alignment = from_wire(
      storage_alignment::make(byte_count{load<120, std::uint32_t>(fixed)}),
      context,
      wal_field::alignment,
      120);
    if (!alignment) return codec::failure(alignment.error());
    if (*alignment != expected.wal.alignment())
        return codec::failure(
          at(errc::wrong_context, context, wal_field::alignment, 120));
    const auto profile = from_wire(
      parse_replay_profile(load<124, std::uint16_t>(fixed)),
      context,
      wal_field::replay_profile,
      124);
    if (!profile) return codec::failure(profile.error());
    if (*profile != expected.profile)
        return codec::failure(
          at(errc::wrong_context, context, wal_field::replay_profile, 124));
    if (load<126, std::uint16_t>(fixed) != 0)
        return codec::failure(
          at(errc::malformed_data, context, wal_field::reserved, 126));
    return {};
}

codec::result<void> check_child(
  assigned_batch_info info,
  const wal_prepare_expectation& expected,
  codec::field_context context,
  bool encoding) {
    if (
      info.context.submitted().binding().routing_epoch()
      != expected.routing_epoch)
        return codec::failure(at(
          encoding ? errc::wrong_context : errc::malformed_data,
          context,
          wal_field::routing_epoch,
          encoding ? 0U : 96U));
    if (!expected.target.physical_begin().checked_add(
          model::segment_record_count{info.context.retained_count().value()}))
        return codec::failure(at(
          encoding ? errc::out_of_range : errc::malformed_data,
          context,
          wal_field::physical_begin,
          encoding ? 0U : 104U));
    return {};
}
} // namespace

namespace detail {
class wal_codec final {
public:
    static wal_prepare make(
      const wal_prepare_expectation& expected,
      model::file_byte_span extent,
      encoded_assigned_batch child) noexcept {
        return wal_prepare{expected, extent, std::move(child)};
    }
};
} // namespace detail

seastar::future<codec::result<fragmented_buffer>> encode_wal_prepare(
  encoded_assigned_batch&& source,
  wal_prepare_expectation expected,
  codec::cooperative_work& work,
  byte_count remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context context) {
    std::optional<encoded_assigned_batch> child{
      std::in_place, std::move(source)};
    fragmented_buffer payload;
    std::optional<codec::result<fragmented_buffer>> output;
    std::optional<codec::error> failed;
    std::exception_ptr exception;
    context.family = static_cast<std::uint16_t>(
      codec::format_family::wal_prepare);
    const auto anchor = at(errc::success, context);
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
               wal_prepare_fixed_bytes,
               child->bytes().size()},
              expected.wal.alignment(),
              work.policy(),
              limits(work.policy()));
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
            if (auto extent = layout->at(expected.wal.position()); !extent) {
                failed = codec::detail::allocation_cost_error(
                  extent.error(), context, context.origin);
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
                + wal_prepare_fixed_bytes.value(),
              static_cast<std::uint16_t>(codec::format_family::assigned_batch),
              static_cast<std::uint16_t>(wal_field::assigned_batch)};
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
            if (
              auto valid = check_child(child->info(), expected, context, true);
              !valid) {
                failed = valid.error();
                break;
            }
            std::array<char, 136> fixed{};
            const auto put = [&](auto id, std::size_t offset) {
                std::copy(
                  id.bytes().begin(),
                  id.bytes().end(),
                  fixed.begin() + static_cast<std::ptrdiff_t>(offset));
            };
            put(expected.wal.incarnation(), 0);
            store<16>(fixed, expected.wal.position().value());
            const auto target = expected.target.segment();
            put(target.cluster(), 24);
            put(target.topic(), 40);
            put(target.range(), 56);
            put(target.segment(), 72);
            store<88>(fixed, target.generation().value());
            store<96>(fixed, expected.routing_epoch.value());
            store<104>(fixed, expected.target.physical_begin().value());
            store<112>(fixed, expected.target.position().value());
            store<120>(
              fixed,
              static_cast<std::uint32_t>(
                expected.wal.alignment().bytes().value()));
            store<124>(fixed, static_cast<std::uint16_t>(expected.profile));
            store<128>(
              fixed, static_cast<std::uint32_t>(child->bytes().size().value()));
            store<132>(
              fixed,
              static_cast<std::uint32_t>(layout->padding_bytes().value()));
            payload = std::move(*child).release_bytes();
            output.emplace(
              co_await detail::encode_padded(
                fixed,
                std::move(payload),
                *layout,
                codec::format_family::wal_prepare,
                work,
                remaining,
                charge,
                context));
            if (!output->has_value()) failed = output->error();
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
        output.reset();
        if (exception) std::rethrow_exception(exception);
        co_return codec::failure(*failed);
    }
    KWAQUE_INVARIANT(
      invariant_id{"KQ-WAL-ENCODE-OUTCOME"},
      output && output->has_value(),
      "WAL encoding completed without bytes");
    co_return std::move(**output);
}

namespace {
struct wal_reader final {
    wal_prepare_expectation expected;
    std::uint64_t start;
    codec::decode_budget original;
    seastar::future<codec::result<decoded_wal_prepare>> operator()(
      fragmented_buffer_parser& input,
      codec::field_context context,
      codec::input_boundary,
      codec::decode_budget memory,
      codec::cooperative_work& work) {
        fragmented_buffer child_bytes_owner;
        std::optional<codec::result<encoded_assigned_batch>> child;
        std::optional<model::file_byte_span> extent;
        std::optional<codec::error> failed;
        std::exception_ptr exception;
        const auto anchor = at(errc::success, context);
        try {
            do {
                std::array<char, 136> fixed{};
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
                  auto valid = check_fixed(fixed, expected, context); !valid) {
                    failed = valid.error();
                    break;
                }
                const byte_count child_bytes{load<128, std::uint32_t>(fixed)};
                if (child_bytes.value() == 0) {
                    failed = at(
                      errc::malformed_data,
                      context,
                      wal_field::assigned_bytes,
                      128);
                    break;
                }
                const auto layout = from_wire(
                  aligned_envelope_layout::make(
                    {byte_count{context.origin - start},
                     wal_prepare_fixed_bytes,
                     child_bytes},
                    expected.wal.alignment(),
                    work.policy(),
                    limits(work.policy())),
                  context,
                  wal_field::assigned_bytes,
                  128);
                if (!layout) {
                    failed = layout.error();
                    break;
                }
                if (
                  input.total_bytes() != layout->body_bytes()
                  || load<132, std::uint32_t>(fixed)
                       != layout->padding_bytes().value()) {
                    failed = at(
                      errc::malformed_data, context, wal_field::padding, 132);
                    break;
                }
                const auto position = from_wire(
                  layout->at(expected.wal.position()),
                  context,
                  wal_field::wal_position,
                  16);
                if (!position) {
                    failed = position.error();
                    break;
                }
                extent = *position;
                if (auto ready = co_await work.checkpoint(anchor); !ready) {
                    failed = ready.error();
                    break;
                }
                if (auto ready = work.poll(anchor); !ready) {
                    failed = ready.error();
                    break;
                }
                auto alias_context = context;
                alias_context.field = static_cast<std::uint16_t>(
                  wal_field::assigned_batch);
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
                      shared.error(), context, context.origin + 136U);
                    break;
                }
                child_bytes_owner = std::move(*shared);
                const codec::field_context child_context{
                  context.origin + 136U,
                  static_cast<std::uint16_t>(
                    codec::format_family::assigned_batch),
                  static_cast<std::uint16_t>(wal_field::assigned_batch)};
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
                if (
                  auto valid = check_child(
                    (*child)->info(), expected, context, false);
                  !valid) {
                    failed = valid.error();
                    break;
                }
                auto padding_context = context;
                padding_context.field = static_cast<std::uint16_t>(
                  wal_field::padding);
                if (
                  auto padding = co_await detail::read_padding(
                    input, layout->padding_bytes(), work, padding_context);
                  !padding) {
                    failed = padding.error();
                    break;
                }
            } while (false);
        } catch (...) {
            exception = std::current_exception();
        }
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        child_bytes_owner = fragmented_buffer{};
        if (!failed && !exception) {
            if (auto ready = work.poll(anchor); !ready) failed = ready.error();
        }
        if (failed || exception) {
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            child.reset();
            if (exception) std::rethrow_exception(exception);
            co_return codec::failure(*failed);
        }
        KWAQUE_INVARIANT(
          invariant_id{"KQ-WAL-DECODE-OUTCOME"},
          child && child->has_value() && extent,
          "WAL decoding completed without its child or extent");
        // Prospective until the envelope decoder releases its body alias and
        // commits. Only the returned child's descriptors remain additional.
        const auto cost = (*child)->bytes().allocation_cost(original.charge);
        KWAQUE_INVARIANT(
          invariant_id{"KQ-WAL-RETAINED"},
          cost.has_value(),
          "validated WAL child has no allocation cost");
        const auto remaining = codec::detail::consume_decode_budget(
          work.policy(),
          original,
          {},
          cost->descriptors,
          context,
          context.origin);
        KWAQUE_INVARIANT(
          invariant_id{"KQ-WAL-RETAINED"},
          remaining.has_value(),
          "WAL child exceeds admitted metadata");
        if (auto ready = work.poll(anchor); !ready) {
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            child.reset();
            co_return codec::failure(ready.error());
        }
        co_return decoded_wal_prepare{
          detail::wal_codec::make(expected, *extent, std::move(**child)),
          *remaining};
    }
};
} // namespace

seastar::future<codec::result<decoded_wal_prepare>> decode_wal_prepare(
  fragmented_buffer_parser& input,
  wal_prepare_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context,
  codec::input_boundary boundary) {
    context.family = static_cast<std::uint16_t>(
      codec::format_family::wal_prepare);
    const auto start = codec::detail::integer_read_start(
      input, context, boundary);
    const auto fail = [](codec::error error) {
        return seastar::make_ready_future<codec::result<decoded_wal_prepare>>(
          codec::failure(error));
    };
    if (!start) return fail(start.error());
    if (auto ready = work.poll(at(errc::success, context)); !ready)
        return fail(ready.error());
    if (auto valid = check_expectation(expected, context); !valid)
        return fail(valid.error());
    return codec::decode_envelope<decoded_wal_prepare>(
      input,
      codec::format_family::wal_prepare,
      limits(work.policy()),
      memory,
      work,
      wal_reader{std::move(expected), *start, memory},
      context,
      boundary);
}
} // namespace kwaque::storage
