#include "src/model/tests/checkpoint_test_support.h"

#include "src/codec/sha256.h"

#include <crc32c/crc32c.h>

#include <cstring>
#include <utility>

namespace kwaque::model::testing::checkpoint_fixture {
namespace {
std::uint32_t checksum(std::string_view input) {
    if (input.empty()) return 0;
    return ::crc32c::Extend(
      0, reinterpret_cast<const std::uint8_t*>(input.data()), input.size());
}
bool nil(std::string_view raw) {
    return std::ranges::all_of(raw, [](char c) { return c == 0; });
}
} // namespace

std::uint64_t little(std::string_view wire, std::size_t at, std::size_t width) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < width; ++i)
        value |= std::uint64_t{static_cast<unsigned char>(wire[at + i])}
                 << (8U * i);
    return value;
}

std::string wire(std::uint32_t count, std::size_t header) {
    if (
      count > 4097 || header < 32 || header > 4096
      || (header != 32 && header < 40))
        __builtin_trap();
    std::string out(header + 20U + 24U * count, '\0');
    out.replace(0, 4, "KQBF");
    put(out, 4, 10, 2);
    put(out, 6, 1, 2);
    put(out, 8, 1, 2);
    put(out, 10, header, 2);
    put(out, 12, out.size() - header, 4);
    if (header != 32) {
        put(out, 32, 77, 2);
        put(out, 36, header - 40U, 4);
        std::fill(
          out.begin() + 40,
          out.begin() + static_cast<std::ptrdiff_t>(header),
          'x');
    }
    for (std::size_t i = 0; i < 16; ++i)
        out[header + i] = static_cast<char>(i + 1U);
    put(out, header + 16U, count, 4);
    for (std::uint32_t i = 1; i <= count; ++i) {
        const auto at = header + 20U + 24U * (i - 1U);
        // Opaque identity octets in unsigned order, independent of wire
        // u32/u64.
        for (std::size_t j = 0; j < 4; ++j)
            out[at + 15U - j] = static_cast<char>((i >> (8U * j)) & 0xffU);
        put(out, at + 16U, i - 1U, 8);
    }
    put(out, 24, checksum(std::string_view{out}.substr(header)), 4);
    put(out, 28, checksum(std::string_view{out}.substr(0, header)), 4);
    return out;
}

std::string body_from_value(const read_checkpoint& value) {
    const auto cursors = value.cursors();
    std::string body(20U + 24U * cursors.size(), '\0');
    const auto expected_topic = value.topic();
    std::copy(
      expected_topic.bytes().begin(),
      expected_topic.bytes().end(),
      body.begin());
    put(body, 16, cursors.size(), 4);
    for (std::size_t i = 0; i < cursors.size(); ++i) {
        const auto range = cursors[i].range();
        std::copy(
          range.bytes().begin(),
          range.bytes().end(),
          body.begin() + static_cast<std::ptrdiff_t>(20U + 24U * i));
        put(body, 36U + 24U * i, cursors[i].next().value(), 8);
    }
    return body;
}

codec::checkpoint_digest digest(std::string_view body) {
    // One terminating zero byte, independently specified in this projection.
    constexpr auto domain = std::to_array("KQ/CHECKPOINT/1");
    codec::sha256_hasher hash;
    hash.update(domain.data(), domain.size());
    hash.update(body.data(), body.size());
    return codec::checkpoint_digest{std::move(hash).final()};
}

probe_result probe(std::string_view wire, topic_id expected, bool complete) {
    if (expected.is_nil()) return {.error = errc::invalid_argument};
    const auto short_error = complete ? errc::malformed_data
                                      : errc::truncated_data;
    if (wire.size() < 32) return {.error = short_error};
    if (wire.substr(0, 4) != "KQBF") return {.error = errc::malformed_data};
    const auto header = static_cast<std::size_t>(little(wire, 10, 2));
    const auto body_size = static_cast<std::size_t>(little(wire, 12, 4));
    if (header < 32) return {.error = errc::malformed_data};
    if (header > 4096 || body_size > 131072 || header + body_size > 131072)
        return {.error = errc::resource_exhausted};
    if (header > wire.size()) return {.error = short_error};
    std::string checked_header{wire.substr(0, header)};
    put(checked_header, 28, 0, 4);
    if (checksum(checked_header) != little(wire, 28, 4))
        return {.error = errc::corrupt_data};
    const auto writer = little(wire, 6, 2), reader = little(wire, 8, 2),
               family = little(wire, 4, 2);
    if (reader > writer) return {.error = errc::malformed_data};
    if (writer == 0 || reader == 0) return {.error = errc::unsupported_format};
    if (family == 0) return {.error = errc::malformed_data};
    if (family > 11 || reader > 1 || little(wire, 16, 8) != 0)
        return {.error = errc::unsupported_format};
    std::size_t extension = 32;
    std::uint64_t previous = 0;
    std::size_t extensions = 0;
    while (extension < header) {
        if (header - extension < 8) return {.error = errc::malformed_data};
        if (++extensions > 64) return {.error = errc::resource_exhausted};
        const auto tag = little(wire, extension, 2),
                   flags = little(wire, extension + 2, 2),
                   length = little(wire, extension + 4, 4);
        if (tag == 0 || tag <= previous) return {.error = errc::malformed_data};
        if (flags > 1) return {.error = errc::unsupported_format};
        extension += 8;
        if (length > header - extension) return {.error = errc::malformed_data};
        if (flags == 1) return {.error = errc::unsupported_format};
        previous = tag;
        extension += static_cast<std::size_t>(length);
    }
    if (body_size > wire.size() - header) return {.error = short_error};
    const auto body = wire.substr(header, body_size);
    if (checksum(body) != little(wire, 24, 4))
        return {.error = errc::corrupt_data};
    if (family != 10) return {.error = errc::wrong_context};
    if (body.size() < 20 || nil(body.substr(0, 16)))
        return {.error = errc::malformed_data};
    for (std::size_t i = 0; i < 16; ++i)
        if (static_cast<unsigned char>(body[i]) != expected.bytes()[i])
            return {.error = errc::wrong_context};
    const auto count = static_cast<std::size_t>(little(body, 16, 4));
    if (count == 0) return {.error = errc::malformed_data};
    if (count > 4096) return {.error = errc::resource_exhausted};
    if (body.size() != 20U + 24U * count)
        return {.error = errc::malformed_data};
    for (std::size_t i = 0; i < count; ++i) {
        const auto key = body.substr(20U + 24U * i, 16);
        if (
          nil(key)
          || (i != 0 && std::memcmp(body.data() + 20U + 24U * (i - 1U), key.data(), 16) >= 0))
            return {.error = errc::malformed_data};
    }
    return {.used = header + body_size, .header = header, .count = count};
}
} // namespace kwaque::model::testing::checkpoint_fixture
