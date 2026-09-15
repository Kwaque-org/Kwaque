#include "src/compression/tests/compression_fuzz_cases.h"

#include "src/compression/tests/lz4_test_support.h"

#include <seastar/core/abort_source.hh>

#include <algorithm>
#include <array>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace kwaque::compression::testing {
namespace {
void require(bool value) {
    if (!value) __builtin_trap();
}

// Independent native context, wire preflight and overwritten destination. Only
// the algorithm is shared with the production reader, not its cursor/allocator
// or profile-validation helpers. Invalid profiles never allocate payload
// scratch.
std::optional<std::string>
reference_decode(std::string_view frame, std::size_t expected) {
    if (frame.size() < 5) return std::nullopt;
    const auto header = LZ4F_headerSize(frame.data(), 5);
    if (LZ4F_isError(header) || header > frame.size()) return std::nullopt;
    LZ4F_dctx* pointer = nullptr;
    native_ok(LZ4F_createDecompressionContext(&pointer, LZ4F_VERSION));
    const auto free = [](LZ4F_dctx* p) {
        (void)LZ4F_freeDecompressionContext(p);
    };
    std::unique_ptr<LZ4F_dctx, decltype(free)> owner{pointer, free};
    LZ4F_frameInfo_t info{};
    auto consumed = header;
    const auto read = LZ4F_getFrameInfo(
      owner.get(), &info, frame.data(), &consumed);
    if (LZ4F_isError(read)) return std::nullopt;
    const auto flags = static_cast<unsigned char>(frame[4]);
    if (
      info.frameType != LZ4F_frame || info.blockSizeID != LZ4F_max64KB
      || info.blockMode != LZ4F_blockIndependent
      || info.blockChecksumFlag != LZ4F_blockChecksumEnabled
      || info.contentChecksumFlag != LZ4F_contentChecksumEnabled
      || (flags & 1U) != 0 || info.dictID != 0
      || ((flags & 8U) == 0 && expected != 0) || info.contentSize != expected)
        return std::nullopt;
    require(consumed == header);
    frame.remove_prefix(consumed);
    std::string output;
    output.reserve(expected);
    std::array<char, 4098> bounce;
    const char sentinel = 0;
    for (;;) {
        const auto offered = std::min(frame.size(), std::size_t{67});
        consumed = offered;
        const auto capacity = std::min(
          std::size_t{4096},
          std::max(expected - output.size(), std::size_t{1}));
        auto produced = capacity;
        bounce.front() = 'a';
        bounce[capacity + 1] = 'z';
        const auto code = LZ4F_decompress(
          owner.get(),
          bounce.data() + 1,
          &produced,
          frame.empty() ? &sentinel : frame.data(),
          &consumed,
          nullptr);
        require(bounce.front() == 'a' && bounce[capacity + 1] == 'z');
        if (LZ4F_isError(code)) return std::nullopt;
        require(consumed <= offered && produced <= capacity);
        if (produced > expected - output.size()) return std::nullopt;
        frame.remove_prefix(consumed);
        output.append(bounce.data() + 1, produced);
        seastar::thread::maybe_yield();
        if (code == 0) {
            if (!frame.empty() || output.size() != expected)
                return std::nullopt;
            return output;
        }
        if (consumed == 0 && produced == 0) return std::nullopt;
    }
}

codec::decode_budget reserve(const bytes::fragmented_buffer& input) {
    const auto cost = input.allocation_cost(charge).value();
    // Keep 1 MiB for the bounded script, oracle, and coroutine/test owners.
    return codec::detail::consume_decode_budget(
             codec::limits::defaults(),
             {byte_count{63U << 20U}, byte_count{1U << 20U}, charge},
             cost.backing,
             cost.descriptors.checked_add(cost.share_controls).value(),
             {},
             0)
      .value();
}

void check_retention(
  const owned_result& result, codec::decode_budget before, bool additional) {
    require(result.retained == result.value.allocation_cost(charge).value());
    const auto metadata = result.retained.descriptors.value()
                          + result.retained.share_controls.value();
    require(
      before.operation_remaining.value()
        - result.remaining.operation_remaining.value()
      == (additional ? result.retained.backing.value() + metadata : 0));
    require(
      before.metadata_remaining.value()
        - result.remaining.metadata_remaining.value()
      == (additional ? metadata : 0));
}
} // namespace

void exercise_compression_case(std::span<const std::uint8_t> script) {
    if (script.size() > compression_fuzz_max_input) return;
    std::array<std::uint8_t, 8> control{};
    const auto prefix = std::min(script.size(), control.size());
    std::copy_n(script.begin(), prefix, control.begin());
    script = script.subspan(prefix);
    std::string payload;
    if (!script.empty())
        payload.assign(
          reinterpret_cast<const char*>(script.data()), script.size());
    const auto mode = control[0] % 4U;
    const auto flags = control[2];
    std::size_t expected = payload.size();
    if (mode == 2) {
        expected = (std::size_t{control[4]} | (std::size_t{control[5]} << 8U)
                    | (std::size_t{control[6]} << 16U))
                   % 65537U;
    } else if (mode == 3) {
        payload = native_frame(payload);
        require(!payload.empty());
        switch (control[3] % 6U) {
        case 0:
            break;
        case 1:
            payload.resize(control[4] % payload.size());
            break;
        case 2:
            payload[control[4] % payload.size()] ^= static_cast<char>(
              1U << (control[5] % 8U));
            break;
        case 3:
            payload += from_hex(empty_hex);
            break;
        case 4:
            ++expected;
            break;
        case 5:
            // This seeded case also detects a native build that bypasses
            // checksums.
            payload = from_hex(raw_abc_hex);
            payload.back() ^= 1;
            expected = 3;
            break;
        }
    } else if (mode == 0 && (control[3] & 1U) != 0) {
        ++expected;
    }
    const auto reference = mode >= 2 ? reference_decode(payload, expected)
                                     : std::optional<std::string>{};
    if (mode == 3 && control[3] % 6U == 5) require(!reference.has_value());
    constexpr std::array<std::size_t, 4> widths{1, 7, 67, 4096};
    const auto width = std::max(
      widths[control[1] % widths.size()], (payload.size() + 511U) / 512U);
    auto input = layout(payload, width);
    auto memory = reserve(input);
    if ((flags & 1U) != 0) memory.operation_remaining = {};
    auto config = codec::limits_config{};
    if ((flags & 2U) != 0) config.max_work_bytes = byte_count{65535};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    if ((flags & 4U) != 0) abort.request_abort();
    const codec::field_context anchor{
      .origin = (flags & 8U) != 0 ? UINT64_MAX - payload.size() : 71,
      .family = 4,
      .field = 8};
    auto result
      = mode == 0
          ? transfer_none(
              std::move(input), byte_count{expected}, work, memory, anchor)
              .get()
        : mode == 1
          ? compress_lz4(
              std::move(input), byte_count{131072}, work, memory, anchor)
              .get()
          : decompress_lz4(
              std::move(input), byte_count{expected}, work, memory, anchor)
              .get();
    // Every entry consumes its donor, including typed rejection.
    // NOLINTNEXTLINE(bugprone-use-after-move)
    require(input.empty());
    const bool admitted = mode == 0 || (flags & 3U) == 0;
    const bool valid = mode < 2 ? (mode == 1 || expected == payload.size())
                                : reference.has_value();
    require(
      result.has_value() == (admitted && valid && !abort.abort_requested()));
    if (!result) {
        const auto error = result.error();
        require(
          error.family() == anchor.family && error.field() == anchor.field);
        require(
          error.byte_offset() >= anchor.origin
          && error.byte_offset() - anchor.origin <= payload.size());
        require(
          error.code() == errc::aborted
          || error.code() == errc::resource_exhausted
          || error.code() == errc::malformed_data
          || error.code() == errc::corrupt_data
          || error.code() == errc::unsupported_format);
        return;
    }
    check_retention(*result, memory, mode != 0);
    if (mode == 0)
        require(result->value.content_equals(payload));
    else if (mode >= 2)
        require(result->value.content_equals(*reference));
    else {
        const auto encoded = flatten(result->value);
        require(native_decode(encoded, expected) == payload);
        // Independent encoder and a different input layout exercise the reverse
        // path.
        auto other = layout(
          native_frame(payload), std::max(width, std::size_t{7}));
        auto other_memory = reserve(other);
        auto decoded
          = decompress_lz4(
              std::move(other), byte_count{expected}, work, other_memory)
              .get();
        require(decoded.has_value() && decoded->value.content_equals(payload));
        check_retention(*decoded, other_memory, true);
    }
}
} // namespace kwaque::compression::testing
