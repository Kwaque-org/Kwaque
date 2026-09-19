#include "src/protocol/tests/control_fuzz_cases.h"

#include "src/protocol/control_frame_codec.h"
#include "src/protocol/tests/control_fuzz_oracle.h"
#include "src/protocol/tests/control_test_support.h"
#include "src/protocol/tests/frame_fuzz_oracle.h"
#include "src/protocol/tests/frame_test_support.h"

#include <seastar/core/thread.hh>

#include <algorithm>
#include <array>
#include <optional>
#include <string_view>

namespace kwaque::protocol::testing {
namespace {
namespace fixture = control_fixture;
namespace frame = frame_fixture;
using bytes::fragmented_buffer_parser;
using fixture::blob;
using fixture::scalar;
void require(bool condition) {
    if (!condition) __builtin_trap();
}

std::string wrap(frame_kind kind, std::string_view payload, unsigned mutation) {
    auto metadata = frame::metadata;
    metadata.kind = kind;
    metadata.stream = model::transport_stream_id{};
    const auto extension = mutation == 14
                             ? frame::extension(7, 0, std::string(4040, 'h'))
                             : std::string{};
    auto wire = frame::header(
      static_cast<std::uint32_t>(payload.size()),
      frame::crc32c(payload),
      extension,
      metadata);
    wire.reserve(wire.size() + payload.size() + 64);
    wire.append(payload);
    return wire;
}
void mutate(
  std::string& payload,
  unsigned mode,
  unsigned offset,
  unsigned octet,
  std::string_view tail) {
    payload.reserve(payload.size() + tail.size() + 128);
    switch (mode) {
    case 1:
        if (!payload.empty()) payload.resize(offset % payload.size());
        break;
    case 2:
        if (!payload.empty())
            payload[offset % payload.size()] ^= static_cast<char>(octet | 1U);
        break;
    case 3:
        payload += scalar(1, 1);
        break;
    case 4:
        payload += blob(99, tail);
        break;
    case 5:
        payload.push_back('\0');
        break;
    case 6:
        payload += fixture::varint((99U << 3U) | 3U);
        break;
    case 7:
        if (!payload.empty()) {
            payload[0] = static_cast<char>(
              static_cast<unsigned char>(payload[0]) | 0x80U);
            payload.insert(1, 1, '\0');
        }
        break;
    case 8:
        payload += std::string{"\x9a\x80\x80\x80\x80\x00\x00", 7};
        break;
    case 15:
        for (std::size_t i = 0;
             i < std::min<std::size_t>(64, tail.size()) && !payload.empty();
             ++i)
            payload[(offset + i * 257U) % payload.size()] ^= tail[i];
        break;
    default:
        break;
    }
}
template<typename Id>
Id expected_id(unsigned octet) {
    std::array<std::uint8_t, 16> raw{};
    raw.fill(static_cast<std::uint8_t>(octet | 1U));
    return Id::make(raw).value();
}
void verify_output(
  const control& value,
  frame_kind kind,
  const google::protobuf::Message& expected) {
    require(matches_control(expected, value.data()));
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto encoded = encode_control(
                     value,
                     work,
                     {},
                     fixture::budget().operation_remaining,
                     fixture::charge)
                     .get();
    require(encoded.has_value());
    auto normalized = probe_control(fixture::flatten(*encoded), kind);
    require(
      normalized && control_unknowns_empty(*normalized)
      && matches_control(*normalized, value.data()));
    // The oracle compares semantics and optional presence; it never demands
    // canonical bytes, received field order, or forwarding of unknowns.
}
} // namespace

void exercise_control_case(std::span<const std::uint8_t> data) {
    if (data.size() > control_fuzz_max_input) return;
    std::array<unsigned, 8> knobs{};
    for (std::size_t i = 0; i < std::min(data.size(), knobs.size()); ++i)
        knobs[i] = data[i];
    const auto kind = static_cast<frame_kind>(1U + (knobs[0] & 3U));
    const bool framed = (knobs[0] & 4U) != 0, raw = (knobs[0] & 8U) != 0;
    const unsigned mutation = knobs[2] % 16U, flags = knobs[3],
                   depth = knobs[5] % 9U;
    const std::string_view tail
      = data.size() <= 8
          ? std::string_view{}
          : std::string_view{
              reinterpret_cast<const char*>(data.data() + 8), data.size() - 8};
    auto payload = raw && !framed ? std::string{tail}
                                  : control_payload(kind, knobs[1] % 5U);
    if (!raw || !framed) mutate(payload, mutation, knobs[6], knobs[7], tail);
    auto wire = framed
                  ? (raw ? std::string{tail} : wrap(kind, payload, mutation))
                  : std::move(payload);
    if (framed) {
        if (mutation == 9 && !wire.empty()) wire.resize(knobs[6] % wire.size());
        if (mutation == 10 && wire.size() > 40) wire[40] ^= 1;
        if (mutation == 11) {
            wire.reserve(wire.size() + 64);
            wire += wrap(frame_kind::error, scalar(1, 1), 0);
        }
        if ((mutation == 12 || mutation == 13) && wire.size() >= 48) {
            if (mutation == 12) frame::put_u16(wire, 6, 16);
            frame::put_u64(wire, 16, 1);
            const auto h = model::testing::little(wire, 8, 2);
            if (h >= 48 && h <= 4096 && h <= wire.size()) frame::repair(wire);
        }
        // Raw repair cases reach payload validation behind intact outer CRCs.
        if (raw && mutation == 4 && wire.size() >= 48) {
            const auto h = model::testing::little(wire, 8, 2),
                       p = model::testing::little(wire, 12, 4);
            if (
              h >= 48 && h <= 4096 && h <= wire.size()
              && p <= wire.size() - h) {
                frame::put_u32(
                  wire, 44, frame::crc32c(std::string_view{wire}.substr(h, p)));
                frame::repair(wire);
            }
        }
    }
    const auto outer = framed ? probe_frame(
                                  wire, 0, (flags & 1U) ? complete_input : 0)
                              : frame_probe{};
    const auto body = !framed           ? std::string_view{wire}
                      : outer.used == 0 ? std::string_view{}
                                        : std::string_view{wire}.substr(
                                            outer.header, outer.payload);
    auto reference = probe_control(body, kind);
    control_expectation expected;
    if ((flags & 64U) != 0) {
        if (
          kind == frame_kind::handshake_request
          || kind == frame_kind::handshake_response)
            expected.cluster = expected_id<model::cluster_id>(knobs[7]);
        else
            expected.topic = expected_id<model::topic_id>(knobs[7]);
    }
    if ((flags & 128U) != 0) expected.topic = model::topic_id{};
    const bool altered_policy = (flags & (2U | 4U | 8U | 16U | 32U | 128U)) != 0
                                || depth == 8;
    std::optional<control> accepted;
    {
        const std::array<std::size_t, 4> widths{7, 67, 1024, 65536};
        fragmented_buffer_parser input{fixture::bounded_fragmented(
          "#" + wire, widths[knobs[4] % widths.size()])};
        input.skip(byte_count{1}).value();
        for (unsigned n = 0; n < depth; ++n)
            input.push_checkpoint().value();
        seastar::abort_source abort;
        auto config = codec::limits::defaults().config();
        if ((flags & 16U) != 0) config.max_nesting_depth = item_count{1};
        if ((flags & 32U) != 0) config.max_control_fields = item_count{1};
        codec::cooperative_work work{
          codec::limits::make(config).value(), abort};
        auto memory = fixture::reserve(input, work);
        if ((flags & 4U) != 0) memory.operation_remaining = byte_count{};
        if ((flags & 8U) != 0) memory.metadata_remaining = byte_count{};
        if ((flags & 2U) != 0) abort.request_abort();
        const bool expected_value = reference
                                    && matches_expectation(
                                      *reference, kind, expected);
        if (framed) {
            auto result = decode_control_frame(
                            input,
                            kind,
                            expected,
                            frame::bounds,
                            memory,
                            work,
                            fixture::context,
                            (flags & 1U) ? codec::input_boundary::complete
                                         : codec::input_boundary::open)
                            .get();
            auto* value = result ? std::get_if<decoded_control_frame>(&*result)
                                 : nullptr;
            if (!altered_policy) {
                require(
                  (value != nullptr)
                  == (outer.error == errc::success && outer.used != 0 && outer.kind == static_cast<unsigned>(kind) && expected_value));
                if (outer.needed != 0 && outer.error == errc::success) {
                    require(
                      result && std::holds_alternative<need_more>(*result));
                    require(
                      std::get<need_more>(*result).additional_bytes.value()
                      == outer.needed);
                }
            }
            if (value) {
                require(
                  reference && expected_value && outer.used != 0
                  && outer.kind == static_cast<unsigned>(kind));
                require(input.bytes_consumed().value() == 1U + outer.used);
                require(
                  value->header.metadata.kind == kind
                  && value->header.metadata.stream.value() == 0
                  && value->header.header_bytes.value() == outer.header
                  && value->header.payload_bytes.value() == outer.payload
                  && value->header.metadata.correlation.value()
                       == model::testing::little(wire, 24, 8)
                  && value->header.metadata.sequence.value()
                       == model::testing::little(wire, 32, 8));
                require(
                  value->remaining.operation_remaining
                    <= memory.operation_remaining
                  && value->remaining.metadata_remaining
                       <= memory.metadata_remaining);
                accepted.emplace(std::move(value->value));
            } else
                require(input.bytes_consumed() == byte_count{1});
        } else {
            auto result
              = decode_control(
                  input, kind, expected, memory, work, fixture::context)
                  .get();
            if (!altered_policy) require(result.has_value() == expected_value);
            if (result) {
                require(reference && expected_value && input.at_end());
                require(
                  result->remaining.operation_remaining
                    <= memory.operation_remaining
                  && result->remaining.metadata_remaining
                       <= memory.metadata_remaining);
                accepted.emplace(std::move(result->value));
            } else
                require(input.bytes_consumed() == byte_count{1});
        }
        if ((flags & (2U | 4U | 8U | 128U)) != 0 || depth == 8)
            require(!accepted);
        require(input.checkpoint_depth() == depth);
        for (unsigned n = 0; n < depth; ++n)
            input.rollback().value();
    }
    if (accepted) verify_output(*accepted, kind, *reference);
    seastar::thread::maybe_yield();
}
} // namespace kwaque::protocol::testing
