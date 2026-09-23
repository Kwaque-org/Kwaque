#include "src/protocol/tests/frame_fuzz_oracle.h"

#include "src/protocol/tests/frame_test_support.h"

#include <string>

namespace kwaque::protocol::testing {

frame_probe probe_frame(
  std::string_view wire,
  unsigned expected_kind,
  unsigned flags,
  unsigned depth) {
    namespace oracle = model::testing;
    const bool complete = (flags & complete_input) != 0;
    const auto shortage = [complete](std::size_t missing) -> frame_probe {
        return complete ? frame_probe{.error = errc::malformed_data}
                        : frame_probe{.needed = missing};
    };
    if ((flags & initial_abort) != 0) return {.error = errc::aborted};
    if (expected_kind != 0 && (flags & nil_topic) != 0)
        return {.error = errc::invalid_argument};
    if (depth == 8 || (flags & deny_work) != 0)
        return {.error = errc::resource_exhausted};
    if (wire.size() < 48) return shortage(48 - wire.size());
    if (wire.substr(0, 4) != "KQWF") return {.error = errc::malformed_data};
    const auto header = oracle::little(wire, 8, 2);
    const auto payload = oracle::little(wire, 12, 4);
    if (header < 48) return {.error = errc::malformed_data};
    if (header > 4096 || payload > 16777216)
        return {.error = errc::resource_exhausted};
    if (header > wire.size())
        return shortage(static_cast<std::size_t>(header - wire.size()));
    // This is the first variable-sized alias admission, before header CRC.
    if ((flags & (deny_operation | deny_metadata)) != 0)
        return {.error = errc::resource_exhausted};
    auto masked = std::string{wire.substr(0, header)};
    oracle::put(masked, 40, 0, 4);
    if (frame_fixture::crc32c(masked) != oracle::little(wire, 40, 4))
        return {.error = errc::corrupt_data};
    if (oracle::little(wire, 4, 2) != 1)
        return {.error = errc::unsupported_format};
    const auto kind = oracle::little(wire, 6, 2);
    if (kind == 0) return {.error = errc::malformed_data};
    if (kind > 4 && kind != 16 && kind != 17)
        return {.error = errc::unsupported_format};
    if (oracle::little(wire, 10, 2) != 0)
        return {.error = errc::unsupported_format};
    if (kind <= 4 && payload > 65536)
        return {.error = errc::resource_exhausted};
    if ((kind <= 4) != (oracle::little(wire, 16, 8) == 0))
        return {.error = errc::malformed_data};
    std::size_t offset = 48;
    std::uint64_t previous = 0;
    unsigned count = 0;
    while (offset < header) {
        if (header - offset < 8) return {.error = errc::malformed_data};
        if (++count > 64) return {.error = errc::resource_exhausted};
        const auto tag = oracle::little(wire, offset, 2);
        const auto extension_flags = oracle::little(wire, offset + 2, 2);
        const auto length = oracle::little(wire, offset + 4, 4);
        if (tag == 0 || tag <= previous) return {.error = errc::malformed_data};
        if ((extension_flags & 0xfffeU) != 0)
            return {.error = errc::unsupported_format};
        offset += 8;
        if (length > header - offset) return {.error = errc::malformed_data};
        if ((extension_flags & 1U) != 0)
            return {.error = errc::unsupported_format};
        previous = tag;
        offset += static_cast<std::size_t>(length);
    }
    if (payload > wire.size() - header)
        return shortage(
          static_cast<std::size_t>(header + payload - wire.size()));
    const auto body = wire.substr(header, payload);
    if (frame_fixture::crc32c(body) != oracle::little(wire, 44, 4))
        return {.error = errc::corrupt_data};
    if (expected_kind != 0 && kind != expected_kind)
        return {.error = errc::wrong_context};
    frame_probe result{
      .used = static_cast<std::size_t>(header + payload),
      .header = static_cast<std::size_t>(header),
      .payload = static_cast<std::size_t>(payload),
      .kind = static_cast<std::uint16_t>(kind)};
    if (expected_kind != 0) {
        result.batch = oracle::probe_batch(
          body, expected_kind == 17, true, (flags & wrong_topic) != 0);
        if (result.batch.error != errc::success)
            return {.error = result.batch.error, .batch = result.batch};
        if (result.batch.used != payload)
            return {.error = errc::malformed_data};
    }
    return result;
}

} // namespace kwaque::protocol::testing
