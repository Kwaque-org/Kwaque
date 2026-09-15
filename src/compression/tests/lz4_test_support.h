#pragma once

#include "src/bytes/fragmented_buffer_builder.h"
#include "src/compression/tests/allocation_profile.h"

#include <seastar/core/thread.hh>

#include <array>
#include <bit>
#include <charconv>
#include <lz4frame.h>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>

namespace kwaque::compression::testing {

inline std::string from_hex(std::string_view value) {
    if (value.size() % 2U != 0)
        throw std::invalid_argument("odd hex fixture length");
    std::string result;
    result.reserve(value.size() / 2U);
    for (std::size_t offset = 0; offset < value.size(); offset += 2U) {
        unsigned byte = 0;
        const auto parsed = std::from_chars(
          value.data() + offset, value.data() + offset + 2U, byte, 16);
        if (
          parsed.ec != std::errc{} || parsed.ptr != value.data() + offset + 2U)
            throw std::invalid_argument("invalid hex fixture");
        result.push_back(std::bit_cast<char>(static_cast<std::uint8_t>(byte)));
    }
    return result;
}

inline constexpr std::string_view raw_abc_hex
  = "04224d187c4003000000000000007403000080616263ff53d13200000000ff53d132";
inline constexpr std::string_view compressed_abc_hex
  = "04224d187c400300000000000000740400000030616263c2cc790200000000ff53d132";
inline constexpr std::string_view empty_hex = "04224d187440bd00000000055dcc02";

inline bytes::fragmented_buffer
layout(std::string_view value, std::size_t width) {
    if (width == 0 || width > 65536)
        throw std::invalid_argument("invalid fixture fragment width");
    const auto fragments = value.empty() ? 0U
                                         : 1U + (value.size() - 1U) / width;
    if (fragments > 1024)
        throw std::invalid_argument("too many fixture fragments");
    const auto capacity = std::max(value.size(), width);
    bytes::fragmented_buffer_builder builder{
      {.initial_fragment_bytes = byte_count{width},
       .max_fragment_bytes = byte_count{width},
       .max_total_bytes = byte_count{capacity},
       .max_retained_bytes
       = byte_count{std::max(fragments, std::size_t{1}) * width},
       .max_fragments = std::max(fragments, std::size_t{1})}};
    if (!builder.reserve_fragments(item_count{fragments}))
        throw std::runtime_error("fixture reserve failed");
    while (!value.empty()) {
        const auto count = std::min({width, value.size(), std::size_t{4096}});
        if (!builder.append(std::span{value.data(), count}))
            throw std::runtime_error("fixture append failed");
        value.remove_prefix(count);
        seastar::thread::maybe_yield();
    }
    return builder.finish().value();
}

inline std::string flatten(const bytes::fragmented_buffer& value) {
    if (value.size().value() >= maximum_contiguous_allocation_bytes)
        throw std::invalid_argument("fixture flattening is bounded");
    std::string result;
    result.reserve(value.size().value());
    for (const auto fragment : value)
        result.append(fragment.data(), fragment.size());
    return result;
}

inline void native_ok(std::size_t code) {
    if (LZ4F_isError(code)) throw std::runtime_error{LZ4F_getErrorName(code)};
}

// An independent native writer with bounded input/output offers and ordinary
// native context allocation. Flush policy/level can differ on the wire.
inline std::string native_frame(
  std::string_view input,
  std::size_t chunk = 65536,
  unsigned auto_flush = 0,
  int level = 0) {
    if (
      chunk == 0 || chunk > 65536 || input.size() > 90000 || auto_flush > 1
      || (level != 0 && level != 1))
        throw std::invalid_argument("native fixture bounds exceeded");
    const auto retained = input.size() + 16U * (1U + input.size() / chunk)
                          + 64U;
    if (retained > 100000)
        throw std::invalid_argument("native fixture packing exceeds its bound");
    LZ4F_preferences_t prefs{};
    prefs.frameInfo.blockSizeID = LZ4F_max64KB;
    prefs.frameInfo.blockMode = LZ4F_blockIndependent;
    prefs.frameInfo.blockChecksumFlag = LZ4F_blockChecksumEnabled;
    prefs.frameInfo.contentChecksumFlag = LZ4F_contentChecksumEnabled;
    prefs.frameInfo.contentSize = input.size();
    prefs.autoFlush = auto_flush;
    prefs.compressionLevel = level;
    LZ4F_cctx* pointer = nullptr;
    native_ok(LZ4F_createCompressionContext(&pointer, LZ4F_VERSION));
    const auto free = [](LZ4F_cctx* p) {
        (void)LZ4F_freeCompressionContext(p);
    };
    std::unique_ptr<LZ4F_cctx, decltype(free)> owner{pointer, free};
    std::array<char, 65552> output;
    std::string encoded;
    encoded.reserve(retained);
    auto produced = LZ4F_compressBegin(
      owner.get(), output.data(), output.size(), &prefs);
    native_ok(produced);
    encoded.append(output.data(), produced);
    while (!input.empty()) {
        const auto count = std::min(input.size(), chunk);
        const auto bound = LZ4F_compressBound(count, &prefs);
        native_ok(bound);
        if (bound > output.size())
            throw std::runtime_error("native fixture output bound exceeded");
        produced = LZ4F_compressUpdate(
          owner.get(),
          output.data(),
          output.size(),
          input.data(),
          count,
          nullptr);
        native_ok(produced);
        encoded.append(output.data(), produced);
        input.remove_prefix(count);
        seastar::thread::maybe_yield();
    }
    produced = LZ4F_compressEnd(
      owner.get(), output.data(), output.size(), nullptr);
    native_ok(produced);
    encoded.append(output.data(), produced);
    return encoded;
}

inline std::string
native_decode(std::string_view encoded, std::size_t expected) {
    if (expected > 90000)
        throw std::invalid_argument(
          "native fixture expansion exceeds its bound");
    LZ4F_dctx* pointer = nullptr;
    native_ok(LZ4F_createDecompressionContext(&pointer, LZ4F_VERSION));
    const auto free = [](LZ4F_dctx* p) {
        (void)LZ4F_freeDecompressionContext(p);
    };
    std::unique_ptr<LZ4F_dctx, decltype(free)> owner{pointer, free};
    std::string result;
    result.reserve(expected);
    std::array<char, 4098> output;
    const char sentinel = 0;
    std::size_t code = 1;
    while (code != 0) {
        output.front() = 'a';
        output.back() = 'z';
        const auto offered = std::min(encoded.size(), std::size_t{67});
        auto consumed = offered;
        std::size_t produced = 4096;
        code = LZ4F_decompress(
          owner.get(),
          output.data() + 1,
          &produced,
          encoded.empty() ? &sentinel : encoded.data(),
          &consumed,
          nullptr);
        native_ok(code);
        if (
          consumed > offered || produced > 4096
          || produced > expected - result.size() || output.front() != 'a'
          || output.back() != 'z'
          || (code != 0 && consumed == 0 && produced == 0))
            throw std::runtime_error("invalid native fixture decode progress");
        encoded.remove_prefix(consumed);
        result.append(output.data() + 1, produced);
        seastar::thread::maybe_yield();
    }
    if (!encoded.empty() || result.size() != expected)
        throw std::runtime_error("incomplete native fixture decode");
    return result;
}

} // namespace kwaque::compression::testing
