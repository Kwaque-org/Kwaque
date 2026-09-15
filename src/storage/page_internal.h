#pragma once

#include "src/bytes/fragmented_buffer_builder.h"
#include "src/codec/sha256.h"
#include "src/storage/format_internal.h"
#include "src/storage/page_ref.h"

#include <seastar/core/deleter.hh>

#include <exception>
#include <limits>
#include <vector>

namespace kwaque::storage::detail {
inline constexpr auto sealed_family = codec::format_family::sealed_extent;
inline constexpr codec::envelope_extent_limits page_limits{
  byte_count{65536}, byte_count{65536}};
inline codec::error page_error(
  errc code, codec::field_context c, std::uint64_t offset = 0) noexcept {
    return codec::error{code, c.family, c.field, c.origin + offset};
}
template<typename T>
codec::result<T>
page_wire(result<T> value, codec::field_context c, std::uint64_t offset = 0) {
    if (value) return std::move(*value);
    return codec::failure(page_error(
      value.error() == errc::resource_exhausted ? errc::resource_exhausted
                                                : errc::malformed_data,
      c,
      offset));
}
template<std::size_t Offset, typename Id, std::size_t N>
codec::result<Id>
read_id(const std::array<char, N>& fixed, codec::field_context c) {
    static_assert(Offset + Id::width <= N);
    std::array<std::uint8_t, Id::width> raw{};
    for (std::size_t i = 0; i < raw.size(); ++i)
        raw[i] = static_cast<std::uint8_t>(fixed[Offset + i]);
    return page_wire(Id::make(raw), c, Offset);
}
template<std::size_t Offset, typename Id, std::size_t N>
void write_id(std::array<char, N>& out, Id id) noexcept {
    static_assert(Offset + Id::width <= N);
    std::copy(id.bytes().begin(), id.bytes().end(), out.begin() + Offset);
}
template<std::size_t Offset, std::size_t N>
codec::result<void> read_sc(
  const std::array<char, N>& fixed,
  segment_context expected,
  codec::field_context c) {
    const auto cluster = read_id<Offset, model::cluster_id>(fixed, c);
    const auto topic = read_id<Offset + 16, model::topic_id>(fixed, c);
    const auto range = read_id<Offset + 32, model::range_id>(fixed, c);
    const auto segment = read_id<Offset + 48, model::segment_id>(fixed, c);
    const auto generation = page_wire(
      model::segment_generation::make(load<Offset + 64, std::uint64_t>(fixed)),
      c,
      Offset + 64);
    if (!cluster) return codec::failure(cluster.error());
    if (!topic) return codec::failure(topic.error());
    if (!range) return codec::failure(range.error());
    if (!segment) return codec::failure(segment.error());
    if (!generation) return codec::failure(generation.error());
    if (
      *cluster != expected.cluster() || *topic != expected.topic()
      || *range != expected.range() || *segment != expected.segment()
      || *generation != expected.generation())
        return codec::failure(page_error(errc::wrong_context, c, Offset));
    return {};
}
template<std::size_t Offset, std::size_t N>
void write_sc(std::array<char, N>& out, segment_context sc) noexcept {
    write_id<Offset>(out, sc.cluster());
    write_id<Offset + 16>(out, sc.topic());
    write_id<Offset + 32>(out, sc.range());
    write_id<Offset + 48>(out, sc.segment());
    store<Offset + 64>(out, sc.generation().value());
}
template<std::size_t Offset, std::size_t N>
codec::result<coverage>
read_coverage(const std::array<char, N>& fixed, codec::field_context c) {
    const auto l = page_wire(
      model::range_logical_span::make(
        model::range_logical_end{load<Offset, std::uint64_t>(fixed)},
        model::range_logical_end{load<Offset + 8, std::uint64_t>(fixed)}),
      c,
      Offset);
    const auto p = page_wire(
      model::segment_relative_span::make(
        model::segment_relative_end{load<Offset + 16, std::uint64_t>(fixed)},
        model::segment_relative_end{load<Offset + 24, std::uint64_t>(fixed)}),
      c,
      Offset + 16);
    const auto b = page_wire(
      model::file_byte_span::make(
        runtime::file_position{load<Offset + 32, std::uint64_t>(fixed)},
        runtime::file_position{load<Offset + 40, std::uint64_t>(fixed)}),
      c,
      Offset + 32);
    if (!l) return codec::failure(l.error());
    if (!p) return codec::failure(p.error());
    if (!b) return codec::failure(b.error());
    return coverage{*l, *p, *b};
}
template<std::size_t Offset, std::size_t N>
void write_coverage(std::array<char, N>& out, coverage c) noexcept {
    store<Offset>(out, c.logical().begin().value());
    store<Offset + 8>(out, c.logical().end().value());
    store<Offset + 16>(out, c.physical().begin().value());
    store<Offset + 24>(out, c.physical().end().value());
    store<Offset + 32>(out, c.bytes().begin().value());
    store<Offset + 40>(out, c.bytes().end().value());
}
template<std::size_t Offset, std::size_t N>
codec::sha256_digest read_digest(const std::array<char, N>& fixed) noexcept {
    static_assert(Offset + codec::sha256_digest_bytes <= N);
    codec::sha256_digest out{};
    for (std::size_t i = 0; i < out.size(); ++i)
        out[i] = static_cast<unsigned char>(fixed[Offset + i]);
    return out;
}
template<std::size_t Offset, std::size_t N, typename Digest>
void write_digest(std::array<char, N>& out, Digest digest) noexcept {
    const auto raw = digest.bytes();
    static_assert(Offset + codec::sha256_digest_bytes <= N);
    std::copy(raw.begin(), raw.end(), out.begin() + Offset);
}

[[nodiscard]] codec::result<void> check_subkind(
  std::uint16_t actual, std::uint16_t expected, codec::field_context);
[[nodiscard]] codec::result<void> check_retry_ref(
  const page_ref&,
  std::uint32_t ordinal,
  std::uint32_t first,
  storage_alignment,
  const codec::limits&,
  codec::field_context);
[[nodiscard]] codec::result<page_ref>
read_page_ref(const std::array<char, 48>&, codec::field_context);
void write_page_ref(std::array<char, 48>&, const page_ref&) noexcept;
[[nodiscard]] seastar::future<codec::result<codec::immutable_object_digest>>
hash_exact(
  const bytes::fragmented_buffer&,
  codec::cooperative_work&,
  codec::field_context);
[[nodiscard]] seastar::future<codec::result<codec::immutable_object_digest>>
hash_exact(
  bytes::fragmented_buffer_parser&,
  byte_count,
  codec::cooperative_work&,
  codec::field_context);

// Same served-capacity admission as other decoded descriptor arrays. Callers
// bound count from the wire extent first and own this vector across awaits.
template<typename T>
codec::result<codec::decode_budget> reserve_entries(
  std::vector<T>& values,
  std::uint32_t count,
  codec::decode_budget memory,
  const codec::limits& policy,
  codec::field_context c) {
    if (count == 0) return memory;
    if (!memory.charge)
        return codec::failure(page_error(errc::invalid_argument, c));
    const byte_count request{static_cast<std::uint64_t>(count) * sizeof(T)};
    const auto served = memory.charge(request);
    if (served < request)
        return codec::failure(page_error(errc::invalid_argument, c));
    if (!policy.validate_allocation(served))
        return codec::failure(page_error(errc::resource_exhausted, c));
    const auto remaining = codec::detail::consume_decode_budget(
      policy, memory, {}, served, c, c.origin);
    if (!remaining) return codec::failure(remaining.error());
    values.reserve(count);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-PAGE-CAPACITY"},
      values.capacity() <= served.value() / sizeof(T),
      "page metadata allocation exceeded its admitted capacity");
    return *remaining;
}

// The outer mark retains the original cursor while the ordinary public
// envelope decoder checks the representation. Rewind that owned mark to hash
// precisely the verified envelope, including extensions/CRCs/padding. No hash
// alias or whole-buffer copy; neither parsed state nor cursor is published
// before the independently pinned digest and final abort poll pass.
template<typename T, typename Reader>
seastar::future<codec::result<T>> decode_pinned(
  bytes::fragmented_buffer_parser& input,
  codec::immutable_object_digest digest,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  Reader reader,
  codec::field_context c,
  codec::input_boundary boundary,
  std::optional<byte_count> exact_size = std::nullopt) {
    const auto start = codec::detail::integer_read_start(input, c, boundary);
    if (!start) co_return codec::failure(start.error());
    const auto anchor = page_error(errc::success, c);
    const auto depth = input.checkpoint_depth();
    const auto position = input.bytes_consumed();
    if (auto mark = input.push_checkpoint(); !mark)
        co_return codec::failure(
          codec::detail::allocation_cost_error(mark.error(), c, *start));
    codec::detail::parser_transaction_guard transaction{input, depth};
    std::optional<codec::result<T>> output;
    std::optional<codec::error> failed;
    std::exception_ptr exception;
    try {
        do {
            output.emplace(
              co_await codec::decode_envelope<T>(
                input,
                sealed_family,
                page_limits,
                memory,
                work,
                std::move(reader),
                c,
                boundary));
            if (!output->has_value()) {
                failed = output->error();
                break;
            }
            const byte_count length{
              input.bytes_consumed().value() - position.value()};
            if (exact_size && *exact_size != length) {
                failed = page_error(errc::malformed_data, c);
                break;
            }
            // Replacing our own mark at the same depth is infallible and does
            // not alter any caller mark. The guard owns the replacement too.
            input.rollback().value();
            input.push_checkpoint().value();
            const auto actual = co_await hash_exact(input, length, work, c);
            if (!actual) {
                failed = actual.error();
                break;
            }
            if (*actual != digest) {
                failed = page_error(errc::corrupt_data, c);
                break;
            }
            if (auto ready = work.poll(anchor); !ready) failed = ready.error();
        } while (false);
    } catch (...) {
        exception = std::current_exception();
    }
    if (failed || exception) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        output.reset();
        if (exception) std::rethrow_exception(exception);
        co_return codec::failure(*failed);
    }
    transaction.commit();
    co_return std::move(**output);
}

// One bounded allocation, filled a fixed entry at a time. The caller reserves
// borrowed entries; this helper admits the complete new tail and bookkeeping.
template<std::size_t Width, typename Entry, typename Write>
seastar::future<codec::result<bytes::fragmented_buffer>> encode_entries(
  std::span<const Entry> entries,
  Write write,
  codec::cooperative_work& work,
  byte_count remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context c) {
    if (entries.empty()) co_return bytes::fragmented_buffer{};
    const auto anchor = page_error(errc::success, c);
    std::optional<bytes::fragmented_buffer_builder> builder;
    std::optional<codec::error> failed;
    std::exception_ptr exception;
    std::optional<bytes::fragmented_buffer> output;
    try {
        do {
            if (!charge || entries.size() > 65536U / Width) {
                failed = page_error(errc::invalid_argument, c);
                break;
            }
            const byte_count size{entries.size() * Width};
            const auto backing = charge(size);
            const auto descriptor = charge(
              byte_count{bytes::fragmented_buffer::fragment_descriptor_size()});
            const auto control = charge(
              byte_count{sizeof(seastar::free_deleter_impl)});
            if (
              backing < size
              || descriptor < byte_count{bytes::fragmented_buffer::
                                           fragment_descriptor_size()}
              || control < byte_count{sizeof(seastar::free_deleter_impl)}) {
                failed = page_error(errc::invalid_argument, c);
                break;
            }
            if (
              !work.policy().validate_allocation(backing)
              || !work.policy().validate_allocation(descriptor)
              || !work.policy().validate_allocation(control)
              || backing > work.policy().config().max_retained_bytes
              || !work.policy().remaining_operation_bytes(
                {.staged_output = backing,
                 .payload_bookkeeping
                 = byte_count{descriptor.value() + control.value()}},
                remaining)) {
                failed = page_error(errc::resource_exhausted, c);
                break;
            }
            if (
              auto ready = co_await work.admit(
                byte_count{1024}, item_count{64}, anchor);
              !ready) {
                failed = ready.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            builder.emplace(
              bytes::fragmented_buffer_builder_config{
                .initial_fragment_bytes = size,
                .max_fragment_bytes = size,
                .max_total_bytes = size,
                .max_retained_bytes = size,
                .max_fragments = 1});
            builder->reserve_fragments(item_count{1}).value();
            for (const auto& entry : entries) {
                if (
                  auto ready = co_await work.admit(
                    byte_count{1024}, item_count{64}, anchor);
                  !ready) {
                    failed = ready.error();
                    break;
                }
                if (auto ready = work.poll(anchor); !ready) {
                    failed = ready.error();
                    break;
                }
                std::array<char, Width> fixed{};
                write(fixed, entry);
                builder->append(fixed).value();
            }
            if (!failed) output.emplace(builder->finish().value());
        } while (false);
    } catch (...) {
        exception = std::current_exception();
    }
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    builder.reset();
    if (!failed && !exception) {
        if (auto ready = work.poll(anchor); !ready) failed = ready.error();
    }
    if (failed || exception) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        output.reset();
        if (exception) std::rethrow_exception(exception);
        co_return codec::failure(*failed);
    }
    co_return std::move(*output);
}
} // namespace kwaque::storage::detail
