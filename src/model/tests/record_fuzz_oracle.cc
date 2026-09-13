#include "src/model/tests/record_fuzz_oracle.h"

#include "src/codec/sha256.h"

#include <seastar/core/thread.hh>

#include <crc32c/crc32c.h>

#include <algorithm>
#include <array>
#include <bit>
#include <limits>
#include <utility>

namespace kwaque::model::testing {
namespace {
constexpr std::uint64_t max64 = std::numeric_limits<std::uint64_t>::max();
std::uint32_t checksum(std::string_view input) {
    if (input.empty()) return 0;
    return ::crc32c::Extend(
      0, reinterpret_cast<const std::uint8_t*>(input.data()), input.size());
}
struct cursor {
    std::string_view bytes;
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

record_probe probe_record(
  std::string_view wire,
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

batch_probe probe_batch(
  std::string_view wire,
  bool assigned,
  bool complete,
  bool wrong_context,
  std::uint64_t extent_limit) {
    const auto short_error = complete ? errc::malformed_data
                                      : errc::truncated_data;
    const auto fixed = assigned ? 184U : 168U;
    if (wire.size() < 32) return {.error = short_error};
    if (wire.substr(0, 4) != "KQBF") return {.error = errc::malformed_data};
    const auto header = little(wire, 10, 2), body_size = little(wire, 12, 4);
    if (header < 32) return {.error = errc::malformed_data};
    if (header > 4096 || body_size > (8U << 20U) + fixed)
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
    if (
      writer == 0 || reader != 1 || family == 0 || family > 10
      || little(wire, 16, 8) != 0)
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
    if (little(body, 156, 1) != 0) return {.error = errc::unsupported_format};
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
    if (encoded > (8U << 20U) || expanded > (8U << 20U))
        return {.error = errc::resource_exhausted};
    if (encoded != expanded) return {.error = errc::malformed_data};
    if (assigned) {
        result.begin = little(body, 168, 8);
        result.end = little(body, 176, 8);
        if (
          result.begin > max64 - result.original
          || result.begin + result.original != result.end)
            return {.error = errc::malformed_data};
    }
    if (encoded != body.size() - fixed || result.retained > encoded / 7U)
        return {.error = errc::malformed_data};
    std::size_t offset = fixed;
    std::uint64_t headers = 0, previous = 0;
    for (std::uint64_t i = 0; i < result.retained; ++i) {
        auto record = probe_record(
          body.substr(offset),
          result.timestamp,
          result.original,
          4096 - headers,
          true,
          extent_limit - header - offset);
        if (record.error != errc::success) return {.error = record.error};
        if ((i != 0 && record.delta <= previous) || (result.retained==result.original && (record.delta!=i || (i==0 && record.timestamp!=0))))
            return {.error = errc::malformed_data};
        previous = record.delta;
        headers += record.headers;
        offset += record.used;
        if (headers > result.headers) return {.error = errc::malformed_data};
        seastar::thread::maybe_yield();
    }
    if (offset != body.size() || headers != result.headers)
        return {.error = errc::malformed_data};
    for (std::size_t i = 0; i < result.digest.size(); ++i)
        result.digest[i] = static_cast<unsigned char>(body[104 + i]);
    if (
      result.retained == result.original
      && fingerprint(body, body.substr(fixed)) != result.digest)
        return {.error = errc::corrupt_data};
    result.used = static_cast<std::size_t>(header + body_size);
    result.records_at = static_cast<std::size_t>(header) + fixed;
    result.record_bytes = static_cast<std::size_t>(encoded);
    result.canonical = writer == 1 && header == 32;
    return result;
}
} // namespace kwaque::model::testing
