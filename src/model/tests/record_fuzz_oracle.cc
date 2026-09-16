#include "src/model/tests/record_fuzz_oracle.h"

#include "src/codec/sha256.h"

#include <seastar/core/thread.hh>

#include <crc32c/crc32c.h>

#include <algorithm>
#include <array>
#include <bit>
#include <limits>
#include <memory>
#include <new>
#include <stdexcept>
#include <utility>
#include <vector>

#define LZ4F_STATIC_LINKING_ONLY
#include <lz4frame.h>

namespace kwaque::model::testing {
namespace {
constexpr std::uint64_t max64 = std::numeric_limits<std::uint64_t>::max();
std::uint32_t checksum(std::string_view input) {
    if (input.empty()) return 0;
    return ::crc32c::Extend(
      0, reinterpret_cast<const std::uint8_t*>(input.data()), input.size());
}
// Constant-width slabs keep both random scalar access and payload skipping
// independent of production parsers, without flattening expanded records.
struct expanded_bytes {
    static constexpr std::size_t width = 32768;
    std::vector<std::unique_ptr<std::array<char, width>>> slabs;
    std::size_t size{0};
    void append(const char* bytes, std::size_t count) {
        while (count != 0) {
            const auto offset = size % width;
            if (offset == 0)
                slabs.push_back(std::make_unique<std::array<char, width>>());
            const auto part = std::min(count, width - offset);
            std::copy_n(bytes, part, slabs.back()->data() + offset);
            bytes += part;
            count -= part;
            size += part;
        }
    }
};
struct oracle_view {
    std::string_view flat;
    const expanded_bytes* expanded{nullptr};
    std::size_t offset{0}, length{0};
    oracle_view(std::string_view value)
      : flat(value)
      , length(value.size()) {}
    oracle_view(const expanded_bytes& value)
      : expanded(&value)
      , length(value.size) {}
    std::size_t size() const { return length; }
    char operator[](std::size_t at) const {
        if (at >= length) __builtin_trap();
        if (!expanded) return flat[offset + at];
        const auto position = offset + at;
        return (*expanded->slabs[position / expanded_bytes::width])
          [position % expanded_bytes::width];
    }
    oracle_view substr(std::size_t at, std::size_t count = SIZE_MAX) const {
        if (at > length) __builtin_trap();
        auto result = *this;
        result.offset += at;
        result.length = std::min(count, length - at);
        return result;
    }
    void hash_into(codec::sha256_hasher& hash) const {
        for (std::size_t at = 0; at < length;) {
            const auto position = offset + at;
            const auto count = std::min(
              length - at,
              expanded_bytes::width - position % expanded_bytes::width);
            const char* source
              = expanded
                  ? expanded->slabs[position / expanded_bytes::width]->data()
                      + position % expanded_bytes::width
                  : flat.data() + position;
            hash.update(source, count);
            at += count;
            seastar::thread::maybe_yield();
        }
    }
};
errc native_error(std::size_t code) {
    if (!LZ4F_isError(code)) return errc::success;
    switch (LZ4F_getErrorCode(code)) {
    case LZ4F_ERROR_headerChecksum_invalid:
    case LZ4F_ERROR_blockChecksum_invalid:
    case LZ4F_ERROR_contentChecksum_invalid:
        return errc::corrupt_data;
    case LZ4F_ERROR_allocation_failed:
        throw std::bad_alloc{};
    default:
        return errc::malformed_data;
    }
}
errc expand_records(
  std::string_view frame, std::size_t expected, expanded_bytes& output) {
    if (frame.size() < 5) return errc::malformed_data;
    const auto header_size = LZ4F_headerSize(frame.data(), frame.size());
    if (const auto error = native_error(header_size); error != errc::success)
        return error;
    if (frame.size() < header_size) return errc::malformed_data;
    LZ4F_dctx* pointer = nullptr;
    const auto made = LZ4F_createDecompressionContext(&pointer, LZ4F_VERSION);
    if (const auto error = native_error(made); error != errc::success)
        return error;
    const auto free = [](LZ4F_dctx* p) {
        (void)LZ4F_freeDecompressionContext(p);
    };
    std::unique_ptr<LZ4F_dctx, decltype(free)> native{pointer, free};
    LZ4F_frameInfo_t info{};
    auto consumed = header_size;
    const auto read = LZ4F_getFrameInfo(
      native.get(), &info, frame.data(), &consumed);
    if (const auto error = native_error(read); error != errc::success)
        return error;
    const auto flags = static_cast<unsigned char>(frame[4]);
    if (
      info.frameType != LZ4F_frame || info.blockSizeID != LZ4F_max64KB
      || info.blockMode != LZ4F_blockIndependent || !info.blockChecksumFlag
      || !info.contentChecksumFlag || (flags & 1U) || info.dictID)
        return errc::unsupported_format;
    if (!(flags & 8U) && expected != 0) return errc::unsupported_format;
    if (info.contentSize != expected) return errc::malformed_data;
    frame.remove_prefix(consumed);
    auto bounce = std::make_unique<std::array<char, expanded_bytes::width>>();
    const char sentinel = 0;
    for (;;) {
        const auto offered = std::min(frame.size(), expanded_bytes::width);
        consumed = offered;
        const auto capacity = std::min(
          expanded_bytes::width,
          std::max(expected - output.size, std::size_t{1}));
        auto produced = capacity;
        const auto code = LZ4F_decompress(
          native.get(),
          bounce->data(),
          &produced,
          frame.empty() ? &sentinel : frame.data(),
          &consumed,
          nullptr);
        if (const auto error = native_error(code); error != errc::success)
            return error;
        if (
          consumed > offered || produced > capacity
          || produced > expected - output.size)
            return errc::malformed_data;
        frame.remove_prefix(consumed);
        output.append(bounce->data(), produced);
        seastar::thread::maybe_yield();
        if (code == 0)
            return frame.empty() && output.size == expected
                     ? errc::success
                     : errc::malformed_data;
        if (consumed == 0 && produced == 0) return errc::malformed_data;
    }
}
struct cursor {
    oracle_view bytes;
    std::size_t at{0};
    errc error{errc::success};
    bool complete{true};
    std::uint64_t number(unsigned width) {
        const auto maximum = width == 32 ? std::uint64_t{0xffffffffU} : max64;
        std::uint64_t value = 0, place = 1;
        for (unsigned digit_index = 0; digit_index < (width + 6U) / 7U;
             ++digit_index) {
            if (at == bytes.size()) {
                error = complete ? errc::malformed_data : errc::truncated_data;
                return 0;
            }
            const auto octet = static_cast<unsigned char>(bytes[at++]);
            const auto digit = static_cast<std::uint64_t>(octet % 128U);
            if (digit > (maximum - value) / place) {
                error = errc::malformed_data;
                return 0;
            }
            value += digit * place;
            if (octet < 128U) {
                if (digit_index != 0 && value < place)
                    error = errc::malformed_data;
                return value;
            }
            if (digit_index + 1U == (width + 6U) / 7U) {
                error = errc::malformed_data;
                return 0;
            }
            place *= 128U;
        }
        __builtin_trap();
    }
    bool skip(std::uint64_t count, std::uint64_t cap) {
        if (count > cap) {
            error = errc::resource_exhausted;
            return false;
        }
        if (count > bytes.size() - at) {
            error = errc::malformed_data;
            return false;
        }
        at += static_cast<std::size_t>(count);
        return true;
    }
    std::uint64_t nullable(std::uint64_t cap) {
        const auto bits = number(32);
        if (error != errc::success) return 0;
        const auto length = bits % 2U == 0
                              ? static_cast<std::int64_t>(bits / 2U)
                              : -static_cast<std::int64_t>(bits / 2U) - 1;
        if (length < -1) {
            error = errc::malformed_data;
            return 0;
        }
        if (length == -1) return 0;
        const auto size = static_cast<std::uint64_t>(length);
        skip(size, cap);
        return size;
    }
};
bool nil(std::string_view input, std::size_t offset) {
    return std::all_of(
      input.begin() + static_cast<std::ptrdiff_t>(offset),
      input.begin() + static_cast<std::ptrdiff_t>(offset + 16U),
      [](char c) { return c == 0; });
}
} // namespace
std::uint64_t little(std::string_view wire, std::size_t at, std::size_t width) {
    if (at > wire.size() || width > wire.size() - at) __builtin_trap();
    std::uint64_t value = 0;
    for (std::size_t i = width; i != 0; --i)
        value = value * 256U + static_cast<unsigned char>(wire[at + i - 1U]);
    return value;
}
void put(
  std::string& wire, std::size_t at, std::uint64_t value, std::size_t width) {
    if (at > wire.size() || width > wire.size() - at) __builtin_trap();
    for (std::size_t i = 0; i < width; ++i) {
        wire[at + i] = static_cast<char>(value % 256U);
        value /= 256U;
    }
}
std::string varuint(std::uint64_t value) {
    std::string result;
    do {
        auto digit = static_cast<unsigned char>(value % 128U);
        value /= 128U;
        result.push_back(static_cast<char>(digit + (value != 0 ? 128U : 0U)));
    } while (value != 0);
    return result;
}
std::string lz4_records(std::string_view records) {
    if (records.size() > 65536)
        throw std::invalid_argument("native fixture input is bounded");
    LZ4F_preferences_t prefs{};
    prefs.frameInfo.blockSizeID = LZ4F_max64KB;
    prefs.frameInfo.blockMode = LZ4F_blockIndependent;
    prefs.frameInfo.contentChecksumFlag = LZ4F_contentChecksumEnabled;
    prefs.frameInfo.blockChecksumFlag = LZ4F_blockChecksumEnabled;
    prefs.frameInfo.contentSize = records.size();
    const auto bound = LZ4F_compressFrameBound(records.size(), &prefs);
    if (LZ4F_isError(bound) || bound > 100000)
        throw std::runtime_error("invalid fixture frame bound");
    std::string result(bound, '\0');
    const char empty = 0;
    const auto size = LZ4F_compressFrame(
      result.data(),
      result.size(),
      records.empty() ? &empty : records.data(),
      records.size(),
      &prefs);
    if (LZ4F_isError(size)) throw std::runtime_error{LZ4F_getErrorName(size)};
    result.resize(size);
    return result;
}
codec::sha256_digest
fingerprint(std::string_view fixed, std::string_view records) {
    codec::sha256_hasher hash;
    hash.update(
      codec::semantic_batch_domain.data(), codec::semantic_batch_domain.size());
    hash.update(fixed.data(), 104);
    hash.update(fixed.data() + 136, 12);
    hash.update(records.data(), records.size());
    return std::move(hash).final();
}
void repair_crc(std::string& wire) {
    if (wire.size() < 32) return;
    const auto header = little(wire, 10, 2), body = little(wire, 12, 4);
    if (header < 32 || header > wire.size()) return;
    if (body <= wire.size() - header)
        put(wire, 24, checksum(std::string_view{wire}.substr(header, body)), 4);
    put(wire, 28, 0, 4);
    put(wire, 28, checksum(std::string_view{wire}.substr(0, header)), 4);
}
std::string identity_bytes() {
    std::string fixed(104, '\0');
    for (std::size_t i = 0; i < 16; ++i) {
        fixed[i] = static_cast<char>(1U + i);
        fixed[40 + i] = static_cast<char>(33U + i);
        fixed[56 + i] = static_cast<char>(65U + i);
        fixed[80 + i] = static_cast<char>(97U + i);
    }
    put(fixed, 16, 2, 8);
    put(fixed, 24, 3, 8);
    put(fixed, 32, 4, 8);
    put(fixed, 72, 7, 8);
    put(fixed, 96, 9, 8);
    return fixed;
}
std::string frame(std::string body, bool assigned, bool extension) {
    std::string wire(extension ? 40 : 32, '\0');
    wire.replace(0, 4, "KQBF");
    put(wire, 4, assigned ? 2U : 1U, 2);
    put(wire, 6, extension ? 2U : 1U, 2);
    put(wire, 8, 1, 2);
    put(wire, 10, wire.size(), 2);
    put(wire, 12, body.size(), 4);
    if (extension) put(wire, 32, 0x7ffe, 2);
    wire += body;
    repair_crc(wire);
    return wire;
}

namespace {
record_probe probe_record_bytes(
  oracle_view wire,
  std::int64_t base,
  std::uint64_t original,
  std::uint64_t headers,
  bool complete,
  std::uint64_t extent_limit) {
    cursor prefix{wire, 0, errc::success, complete};
    const auto length = prefix.number(32);
    if (prefix.error != errc::success) return {.error = prefix.error};
    if (length < 6 || length + prefix.at > extent_limit)
        return {.error = errc::malformed_data};
    if (length + prefix.at > 1048576U)
        return {.error = errc::resource_exhausted};
    if (length > wire.size() - prefix.at)
        return {
          .error = complete ? errc::malformed_data : errc::truncated_data};
    cursor body{wire.substr(prefix.at, length)};
    if (body.bytes[body.at++] != 0) return {.error = errc::unsupported_format};
    const auto bits = body.number(64);
    if (body.error != errc::success) return {.error = body.error};
    const auto timestamp = bits % 2U == 0
                             ? static_cast<std::int64_t>(bits / 2U)
                             : -static_cast<std::int64_t>(bits / 2U) - 1;
    if (
      (timestamp >= 0 && base > INT64_MAX - timestamp)
      || (timestamp < 0 && base < INT64_MIN - timestamp))
        return {.error = errc::malformed_data};
    const auto delta = body.number(64);
    if (body.error != errc::success) return {.error = body.error};
    if (delta >= original) return {.error = errc::malformed_data};
    body.nullable(1048576);
    if (body.error != errc::success) return {.error = body.error};
    body.nullable(1048576);
    if (body.error != errc::success) return {.error = body.error};
    const auto count = body.number(32);
    if (body.error != errc::success) return {.error = body.error};
    if (count > std::min<std::uint64_t>(64, headers))
        return {.error = errc::resource_exhausted};
    if (count > (body.bytes.size() - body.at) / 2U)
        return {.error = errc::malformed_data};
    std::uint64_t payload = 0;
    for (std::uint64_t i = 0; i < count; ++i) {
        const auto name = body.number(32);
        if (body.error != errc::success) return {.error = body.error};
        if (!body.skip(name, std::min<std::uint64_t>(4096, 65536 - payload)))
            return {.error = body.error};
        payload += name;
        payload += body.nullable(65536 - payload);
        if (body.error != errc::success) return {.error = body.error};
        if (i % 16U == 15U) seastar::thread::maybe_yield();
    }
    if (body.at != body.bytes.size()) return {.error = errc::malformed_data};
    return {
      .used = prefix.at + static_cast<std::size_t>(length),
      .delta = delta,
      .timestamp = timestamp,
      .headers = count};
}

} // namespace
record_probe probe_record(
  std::string_view wire,
  std::int64_t base,
  std::uint64_t original,
  std::uint64_t headers,
  bool complete,
  std::uint64_t extent_limit) {
    return probe_record_bytes(
      oracle_view{wire}, base, original, headers, complete, extent_limit);
}

batch_probe probe_batch(
  std::string_view wire,
  bool assigned,
  bool complete,
  bool wrong_context,
  std::uint64_t extent_limit,
  bool narrow_compression_work) {
    const auto short_error = complete ? errc::malformed_data
                                      : errc::truncated_data;
    const auto fixed = assigned ? 184U : 168U;
    if (wire.size() < 32) return {.error = short_error};
    if (wire.substr(0, 4) != "KQBF") return {.error = errc::malformed_data};
    const auto header = little(wire, 10, 2), body_size = little(wire, 12, 4);
    if (header < 32) return {.error = errc::malformed_data};
    if (header > 4096 || body_size > (16U << 20U))
        return {.error = errc::resource_exhausted};
    if (header + body_size > extent_limit)
        return {.error = errc::malformed_data};
    if (header > wire.size()) return {.error = short_error};
    std::string checked_header{wire.substr(0, header)};
    put(checked_header, 28, 0, 4);
    if (checksum(checked_header) != little(wire, 28, 4))
        return {.error = errc::corrupt_data};
    const auto writer = little(wire, 6, 2), reader = little(wire, 8, 2),
               family = little(wire, 4, 2);
    if (reader > writer) return {.error = errc::malformed_data};
    if (writer == 0 || reader == 0) return {.error = errc::unsupported_format};
    // Zero is an invalid family, not an unknown future family. Sender value
    // checks precede this; reader-profile compatibility and features follow it.
    if (family == 0) return {.error = errc::malformed_data};
    if (family > 10 || reader != 1 || little(wire, 16, 8) != 0)
        return {.error = errc::unsupported_format};
    std::size_t extension = 32;
    std::uint64_t previous_tag = 0, extensions = 0;
    while (extension < header) {
        if (header - extension < 8) return {.error = errc::malformed_data};
        if (++extensions > 64) return {.error = errc::resource_exhausted};
        const auto tag = little(wire, extension, 2),
                   flags = little(wire, extension + 2, 2),
                   size = little(wire, extension + 4, 4);
        if (tag == 0 || tag <= previous_tag)
            return {.error = errc::malformed_data};
        if ((flags & 0xfffeU) != 0) return {.error = errc::unsupported_format};
        extension += 8;
        if (size > header - extension) return {.error = errc::malformed_data};
        if ((flags & 1U) != 0) return {.error = errc::unsupported_format};
        previous_tag = tag;
        extension += static_cast<std::size_t>(size);
    }
    if (body_size > wire.size() - header) return {.error = short_error};
    const auto body = wire.substr(header, body_size);
    if (checksum(body) != little(wire, 24, 4))
        return {.error = errc::corrupt_data};
    if (family != (assigned ? 2U : 1U)) return {.error = errc::wrong_context};
    if (body.size() < fixed) return {.error = errc::malformed_data};
    const auto encoding = little(body, 156, 1);
    if (encoding > 1) return {.error = errc::unsupported_format};
    if (little(body, 157, 1) != 0) return {.error = errc::malformed_data};
    if (little(body, 158, 2) != 1) return {.error = errc::unsupported_format};
    if (nil(body, 0) || little(body, 16, 8) == 0 || little(body, 24, 8) == 0)
        return {.error = errc::malformed_data};
    const auto identity = identity_bytes();
    if (nil(body, 40)) return {.error = errc::malformed_data};
    if (
      wrong_context
      || body.substr(40, 16) != std::string_view{identity}.substr(40, 16))
        return {.error = errc::wrong_context};
    if (nil(body, 56)) return {.error = errc::malformed_data};
    if (body.substr(56, 16) != std::string_view{identity}.substr(56, 16))
        return {.error = errc::wrong_context};
    if (little(body, 72, 8) == 0 || nil(body, 80) || little(body, 96, 8) == 0)
        return {.error = errc::malformed_data};
    batch_probe result;
    result.timestamp = std::bit_cast<std::int64_t>(little(body, 136, 8));
    result.original = little(body, 144, 4);
    result.retained = little(body, 148, 4);
    result.headers = little(body, 152, 4);
    if (result.original == 0) return {.error = errc::malformed_data};
    if (result.original > 4096) return {.error = errc::resource_exhausted};
    if (
      result.retained == 0 || result.retained > result.original
      || (!assigned && result.retained != result.original))
        return {.error = errc::malformed_data};
    if (result.headers > 4096 || result.headers > 64U * result.retained)
        return {.error = errc::resource_exhausted};
    const auto encoded = little(body, 160, 4), expanded = little(body, 164, 4);
    if (
      encoded > (16U << 20U) - fixed || expanded > (8U << 20U)
      || (encoding == 0 && encoded > (8U << 20U)))
        return {.error = errc::resource_exhausted};
    if (encoding == 0 && encoded != expanded)
        return {.error = errc::malformed_data};
    if (assigned) {
        result.begin = little(body, 168, 8);
        result.end = little(body, 176, 8);
        if (
          result.begin > max64 - result.original
          || result.begin + result.original != result.end)
            return {.error = errc::malformed_data};
    }
    if (encoded != body.size() - fixed || result.retained > expanded / 7U)
        return {.error = errc::malformed_data};
    expanded_bytes expanded_records;
    if (encoding == 1) {
        if (narrow_compression_work) return {.error = errc::resource_exhausted};
        const auto error = expand_records(
          body.substr(fixed),
          static_cast<std::size_t>(expanded),
          expanded_records);
        if (error != errc::success) return {.error = error};
    }
    const auto records = encoding == 1 ? oracle_view{expanded_records}
                                       : oracle_view{body.substr(fixed)};
    std::size_t offset = 0;
    std::uint64_t headers = 0, previous = 0;
    for (std::uint64_t i = 0; i < result.retained; ++i) {
        auto record = probe_record_bytes(
          records.substr(offset),
          result.timestamp,
          result.original,
          4096 - headers,
          true,
          encoding == 1 ? max64 : extent_limit - header - fixed - offset);
        if (record.error != errc::success) return {.error = record.error};
        if ((i != 0 && record.delta <= previous) || (result.retained==result.original && (record.delta!=i || (i==0 && record.timestamp!=0))))
            return {.error = errc::malformed_data};
        previous = record.delta;
        headers += record.headers;
        offset += record.used;
        if (headers > result.headers) return {.error = errc::malformed_data};
        seastar::thread::maybe_yield();
    }
    if (offset != records.size() || headers != result.headers)
        return {.error = errc::malformed_data};
    for (std::size_t i = 0; i < result.digest.size(); ++i)
        result.digest[i] = static_cast<unsigned char>(body[104 + i]);
    if (encoding == 1) {
        codec::sha256_hasher raw_hash;
        records.hash_into(raw_hash);
        result.record_digest = std::move(raw_hash).final();
    }
    if (result.retained == result.original) {
        codec::sha256_hasher semantic;
        semantic.update(
          codec::semantic_batch_domain.data(),
          codec::semantic_batch_domain.size());
        semantic.update(body.data(), 104);
        semantic.update(body.data() + 136, 12);
        records.hash_into(semantic);
        if (std::move(semantic).final() != result.digest)
            return {.error = errc::corrupt_data};
    }
    result.used = static_cast<std::size_t>(header + body_size);
    result.records_at = static_cast<std::size_t>(header) + fixed;
    result.record_bytes = static_cast<std::size_t>(expanded);
    result.compressed = encoding == 1;
    result.canonical = encoding == 0 && writer == 1 && header == 32;
    return result;
}
} // namespace kwaque::model::testing
