#include "src/protocol/tests/frame_fuzz_cases.h"

#include "src/codec/sha256.h"
#include "src/protocol/batch_frame_codec.h"
#include "src/protocol/tests/batch_frame_test_support.h"
#include "src/protocol/tests/frame_fuzz_oracle.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/thread.hh>

#include <algorithm>
#include <array>
#include <concepts>
#include <string>
#include <type_traits>
#include <utility>

namespace kwaque::protocol::testing {
namespace {
namespace fixture = frame_fixture;
namespace batch_fixture = batch_frame_fixture;
namespace oracle = model::testing;
using bytes::fragmented_buffer;
using bytes::fragmented_buffer_parser;
void require(bool condition) {
    if (!condition) __builtin_trap();
}

void repair(std::string& wire, bool payload) {
    if (wire.size() < 48) return;
    const auto header = oracle::little(wire, 8, 2),
               length = oracle::little(wire, 12, 4);
    if (header < 48 || header > wire.size() || header > 4096) return;
    if (payload && length <= wire.size() - header)
        fixture::put_u32(
          wire,
          44,
          fixture::crc32c(std::string_view{wire}.substr(header, length)));
    fixture::repair(wire);
}
void extensions(std::string& wire, std::string_view value) {
    const auto header = static_cast<std::size_t>(oracle::little(wire, 8, 2));
    wire.replace(48, header - 48, value);
    fixture::put_u16(wire, 8, static_cast<std::uint16_t>(48 + value.size()));
}

void mutate(
  std::string& wire, unsigned mutation, unsigned operand, std::uint8_t octet) {
    const auto header = static_cast<std::size_t>(oracle::little(wire, 8, 2));
    const auto payload = wire.substr(header);
    const auto bit = static_cast<char>(octet | 1U);
    switch (mutation) {
    case 0:
        return;
    case 1:
        wire.resize(operand % wire.size());
        return;
    case 2:
        wire[operand % wire.size()] ^= bit;
        return;
    case 3:
        wire[operand % wire.size()] ^= bit;
        repair(wire, false);
        return;
    case 4:
        wire[operand % wire.size()] ^= bit;
        break;
    case 5:
        fixture::put_u16(wire, 4, 2);
        break;
    case 6:
        fixture::put_u16(wire, 6, 0);
        break;
    case 7:
        fixture::put_u16(wire, 6, 42);
        break;
    case 8:
        fixture::put_u16(
          wire, 10, static_cast<std::uint16_t>(1U << (operand % 16U)));
        break;
    case 9:
        fixture::put_u64(wire, 16, 0);
        break;
    case 10:
        fixture::put_u16(wire, 8, 47);
        return;
    case 11:
        fixture::put_u16(wire, 8, 4097);
        return;
    case 12:
        fixture::put_u32(wire, 12, 16777217);
        return;
    case 13:
        wire[44] ^= 1;
        repair(wire, false);
        return;
    case 14:
        wire[40] ^= 1;
        return;
    case 15:
        extensions(wire, fixture::extension(7, 1));
        break;
    case 16:
        extensions(wire, fixture::extension(0));
        break;
    case 17:
        extensions(wire, fixture::extension(7, 2));
        break;
    case 18: {
        auto ext = fixture::extension();
        fixture::put_u32(ext, 4, UINT32_MAX);
        extensions(wire, ext);
        break;
    }
    case 19:
        extensions(wire, fixture::extension(7) + fixture::extension(7));
        break;
    case 20: {
        const auto next = wire;
        wire += next;
        return;
    }
    case 21:
        wire.push_back('\0');
        fixture::put_u32(
          wire, 12, static_cast<std::uint32_t>(payload.size() + 1));
        break;
    case 22:
        wire += payload;
        fixture::put_u32(
          wire, 12, static_cast<std::uint32_t>(2U * payload.size()));
        break;
    case 31:
        fixture::put_u16(wire, 6, 1);
        fixture::put_u64(wire, 16, 0);
        break;
    default: {
        auto inner = payload;
        if (inner.size() < 32 || inner.substr(0, 4) != "KQBF") return;
        const auto fixed = static_cast<std::size_t>(
          oracle::little(inner, 10, 2));
        if (fixed + 168 > inner.size()) return;
        switch (mutation) {
        case 23:
            oracle::put(inner, 4, operand % 12U, 2);
            break;
        case 24:
            inner[fixed + 157] = 1;
            break;
        case 25:
            oracle::put(inner, fixed + 144, 4097, 4);
            break;
        case 26:
            inner.back() ^= bit;
            break;
        case 27:
            inner[fixed + 156] = 2;
            break;
        case 28:
            oracle::put(inner, 6, 2, 2);
            break;
        case 29:
            oracle::put(inner, 12, oracle::little(inner, 12, 4) + 1, 4);
            break;
        case 30:
            inner[(static_cast<std::size_t>(operand) * 17U) % inner.size()]
              ^= bit;
            break;
        default:
            __builtin_trap();
        }
        oracle::repair_crc(inner);
        wire.replace(header, payload.size(), inner);
        break;
    }
    }
    repair(wire, true);
}

void check_header(
  const frame_header& header,
  std::string_view wire,
  const frame_probe& reference) {
    require(static_cast<unsigned>(header.metadata.kind) == reference.kind);
    require(
      header.header_bytes.value() == reference.header
      && header.payload_bytes.value() == reference.payload);
    require(header.metadata.stream.value() == oracle::little(wire, 16, 8));
    require(header.metadata.correlation.value() == oracle::little(wire, 24, 8));
    require(header.metadata.sequence.value() == oracle::little(wire, 32, 8));
    require(header.header_crc32c == oracle::little(wire, 40, 4));
    require(header.payload_crc32c == oracle::little(wire, 44, 4));
}

codec::sha256_digest record_digest(const fragmented_buffer& records) {
    codec::sha256_hasher hash;
    for (const auto fragment : records) {
        hash.update(fragment.data(), fragment.size());
        seastar::thread::maybe_yield();
    }
    return std::move(hash).final();
}

template<typename T>
void observe(
  const frame_read_result<T>& result,
  fragmented_buffer_parser& input,
  std::string_view wire,
  const frame_probe& reference,
  codec::decode_budget memory,
  unsigned depth,
  unsigned kind) {
    require(input.checkpoint_depth() == depth);
    if (result)
        require(reference.error == errc::success);
    else if (reference.batch.compression_body_error)
        require(reference.batch.matches_error(result.error().code()));
    else
        require(result.error().code() == reference.error);
    if (!result) {
        require(input.bytes_consumed() == byte_count{1});
        require(
          result.error().byte_offset() >= fixture::context.origin
          && result.error().byte_offset()
               <= fixture::context.origin + wire.size() + 1U);
        require(
          result.error().family() == fixture::context.family
          || (kind != 0 && result.error().family() == (kind == 17 ? 2 : 1)));
    } else if (const auto* more = std::get_if<need_more>(&*result)) {
        require(
          reference.needed != 0
          && more->additional_bytes.value() == reference.needed);
        require(input.bytes_consumed() == byte_count{1});
    } else {
        require(reference.needed == 0 && reference.used != 0);
        const auto& value = std::get<T>(*result);
        check_header(value.header, wire, reference);
        require(input.bytes_consumed().value() == reference.used + 1U);
        require(
          value.remaining.charge == memory.charge
          && value.remaining.operation_remaining <= memory.operation_remaining
          && value.remaining.metadata_remaining <= memory.metadata_remaining);
        if constexpr (std::same_as<T, framed_payload>) {
            require(value.payload.content_equals(
              wire.substr(reference.header, reference.payload)));
            const auto cost
              = value.payload.allocation_cost(fixture::charge).value();
            require(
              memory.operation_remaining.value()
                - value.remaining.operation_remaining.value()
              == cost.descriptors.value());
            require(
              memory.metadata_remaining.value()
                - value.remaining.metadata_remaining.value()
              == cost.descriptors.value());
        } else {
            const auto original = [&] {
                if constexpr (std::same_as<T, decoded_assigned_frame>)
                    return value.batch.context().submitted();
                else
                    return value.batch.context();
            }();
            const auto inner_header = oracle::little(
              wire, reference.header + 10, 2);
            const auto fixed = wire.substr(
              reference.header + inner_header,
              reference.payload - inner_header);
            const auto check_id = [&](const auto& id, std::size_t offset) {
                const auto bytes = id.bytes();
                for (std::size_t i = 0; i < bytes.size(); ++i)
                    require(
                      bytes[i]
                      == static_cast<unsigned char>(fixed[offset + i]));
            };
            const auto identity = original.id();
            const auto binding = original.binding();
            check_id(identity.producer(), 0);
            require(identity.epoch().value() == oracle::little(fixed, 16, 8));
            require(identity.stream().value() == oracle::little(fixed, 24, 8));
            require(
              identity.sequence().value() == oracle::little(fixed, 32, 8));
            check_id(binding.topic(), 40);
            check_id(binding.range(), 56);
            require(
              binding.routing_epoch().value() == oracle::little(fixed, 72, 8));
            check_id(binding.segment(), 80);
            require(
              binding.generation().value() == oracle::little(fixed, 96, 8));
            require(
              original.original_count().value() == reference.batch.original);
            require(
              original.original_timestamp_base().unix_nanoseconds()
              == reference.batch.timestamp);
            require(
              value.batch.header_count().value() == reference.batch.headers);
            require(
              value.batch.records().size().value()
              == reference.batch.record_bytes);
            require(
              value.batch.fingerprint().bytes() == reference.batch.digest);
            if (reference.batch.compressed)
                require(
                  record_digest(value.batch.records())
                  == reference.batch.record_digest);
            else
                require(value.batch.records().content_equals(wire.substr(
                  reference.header + reference.batch.records_at,
                  reference.batch.record_bytes)));
            if constexpr (std::same_as<T, decoded_assigned_frame>) {
                require(
                  value.batch.context().retained_count().value()
                  == reference.batch.retained);
                require(
                  value.batch.context().logical_span().begin().value()
                  == reference.batch.begin);
                require(
                  value.batch.context().logical_span().end().value()
                  == reference.batch.end);
                require(
                  value.fingerprint_verification
                  == (reference.batch.original == reference.batch.retained ? model::batch_fingerprint_verification::recomputed : model::batch_fingerprint_verification::carried));
            }
        }
    }
    const auto used = result && reference.needed == 0 ? reference.used : 0;
    std::string suffix(wire.size() - used, '\0');
    require(input.peek_to(suffix).has_value());
    require(suffix == wire.substr(used));
}

void decode_case(
  std::string_view wire,
  unsigned kind,
  unsigned flags,
  unsigned depth,
  std::size_t width) {
    const auto all = "p" + std::string{wire};
    fragmented_buffer_parser input{
      fixture::fragmented(all, std::max(width, (all.size() + 511U) / 512U))};
    input.skip(byte_count{1}).value();
    for (unsigned i = 0; i < depth; ++i)
        input.push_checkpoint().value();
    auto config = codec::limits_config{};
    if ((flags & deny_work) != 0) config.max_work_items = item_count{1};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    auto memory
      = codec::reserve_decode_input(
          input,
          work.policy(),
          {fixture::parent_budget, config.max_metadata_bytes, fixture::charge},
          fixture::context)
          .value();
    if ((flags & deny_operation) != 0) memory.operation_remaining = {};
    if ((flags & deny_metadata) != 0) memory.metadata_remaining = {};
    if ((flags & initial_abort) != 0) abort.request_abort();
    auto expected = batch_fixture::expected();
    if ((flags & wrong_topic) != 0)
        expected.topic = batch_fixture::id<model::topic_id>(34);
    if ((flags & nil_topic) != 0) expected.topic = {};
    const auto boundary = (flags & complete_input) != 0
                            ? codec::input_boundary::complete
                            : codec::input_boundary::open;
    const auto reference = probe_frame(wire, kind, flags, depth);
    if (kind == 16) {
        const auto result = decode_submitted_frame(
                              input,
                              expected,
                              fixture::bounds,
                              memory,
                              work,
                              fixture::context,
                              boundary)
                              .get();
        observe(result, input, wire, reference, memory, depth, kind);
    } else if (kind == 17) {
        const auto result = decode_assigned_frame(
                              input,
                              expected,
                              fixture::bounds,
                              memory,
                              work,
                              fixture::context,
                              boundary)
                              .get();
        observe(result, input, wire, reference, memory, depth, kind);
    } else {
        const auto result
          = decode_frame(
              input, fixture::bounds, memory, work, fixture::context, boundary)
              .get();
        observe(result, input, wire, reference, memory, depth, kind);
    }
}

} // namespace

void exercise_frame_case(std::span<const std::uint8_t> input) {
    if (input.size() > frame_fuzz_max_input) return;
    std::array<unsigned, 8> control{};
    for (std::size_t i = 0; i < std::min(control.size(), input.size()); ++i)
        control[i] = input[i];
    const auto tail = input.subspan(std::min(input.size(), control.size()));
    const auto shape = control[0] % 5U;
    constexpr std::array<unsigned, 3> raw_kinds{0, 16, 17};
    const unsigned kind = shape == 0
                            ? raw_kinds[(control[0] / 5U) % raw_kinds.size()]
                          : shape == 2 ? 16U
                          : shape >= 3 ? 17U
                                       : 0U;
    std::string wire;
    if (shape == 0) {
        if (!tail.empty())
            wire.assign(
              reinterpret_cast<const char*>(tail.data()), tail.size());
        const auto repairs = control[1] % 4U;
        if (repairs == 3 && wire.size() >= 48) {
            const auto h = static_cast<std::size_t>(oracle::little(wire, 8, 2));
            const auto n = static_cast<std::size_t>(
              oracle::little(wire, 12, 4));
            if (h >= 48 && h <= wire.size() && n <= wire.size() - h) {
                auto inner = wire.substr(h, n);
                oracle::repair_crc(inner);
                wire.replace(h, n, inner);
            }
        }
        if (repairs != 0) repair(wire, repairs >= 2);
    } else {
        std::string payload;
        if (shape == 1) {
            if (tail.empty())
                payload = "x";
            else
                payload.assign(
                  reinterpret_cast<const char*>(tail.data()),
                  std::min(tail.size(), std::size_t{8192}));
        } else
            payload = batch_fixture::batch_wire(
              shape >= 3, control[2] % 2U != 0, shape == 4);
        const auto header_choice = control[3] % 3U;
        const auto ext
          = header_choice == 0
              ? std::string{}
              : fixture::extension(
                  0x7ffe, 0, std::string(header_choice == 1 ? 1U : 4040U, 'x'));
        wire = fixture::wire(payload, ext);
        if (kind != 0)
            fixture::put_u16(wire, 6, static_cast<std::uint16_t>(kind));
        else {
            constexpr std::array<std::uint16_t, 6> kinds{1, 2, 3, 4, 16, 17};
            const auto selected = kinds[control[2] % kinds.size()];
            fixture::put_u16(wire, 6, selected);
            if (selected < 16) fixture::put_u64(wire, 16, 0);
        }
        repair(wire, true);
        mutate(
          wire,
          control[1] % 32U,
          control[7],
          tail.empty() ? std::uint8_t{1} : tail[control[7] % tail.size()]);
    }
    require(wire.size() <= 32768);
    constexpr std::array<std::size_t, 4> widths{1, 7, 67, 4096};
    const auto flags = control[5] & 127U;
    const auto base_flags = flags & (complete_input | wrong_topic);
    for (const auto width :
         {widths[control[4] % widths.size()], std::size_t{67}}) {
        decode_case(wire, kind, base_flags, 0, width);
        if (flags != base_flags || control[6] % 9U != 0)
            decode_case(wire, kind, flags, control[6] % 9U, width);
    }
}

} // namespace kwaque::protocol::testing
