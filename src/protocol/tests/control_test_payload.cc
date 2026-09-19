#include "src/protocol/tests/control_test_payload.h"

#include "src/protocol/tests/control_test_support.h"

namespace kwaque::protocol::testing {
namespace {
namespace fixture = control_fixture;
using fixture::blob;
using fixture::scalar;
void require(bool value) {
    if (!value) __builtin_trap();
}
} // namespace
std::string control_payload(frame_kind kind, unsigned shape) {
    auto caps = fixture::capabilities();
    std::string build;
    if (shape == 1) build = blob(1, std::string(4093, 'v'));
    if (shape == 2 || shape == 3) {
        std::string versions, formats, codecs;
        for (unsigned i = 1; i <= 16; ++i)
            versions += fixture::varint(i);
        for (std::uint16_t i = 1; i <= 32; ++i)
            formats += blob(2, fixture::format(i));
        for (unsigned i = 0; i < 16; ++i)
            codecs += fixture::varint(i);
        caps = blob(1, versions) + formats + blob(3, codecs)
               + scalar(4, 16777216) + scalar(5, 8388608) + scalar(6, 4096)
               + scalar(7, 1048576) + scalar(8, 4096);
        build = blob(1, std::string(1360, 'v'))
                + blob(2, std::string(1360, 'r'))
                + blob(3, std::string(1340, 'b'));
    }
    if (shape == 4)
        build = blob(1, "old") + scalar(1, 9) + blob(1, "new")
                + blob(99, std::string{"\xff", 1});
    std::string payload;
    switch (kind) {
    case frame_kind::handshake_request:
        payload = shape >= 2 && shape <= 3
                    ? scalar(1, 1) + blob(2, std::string(16, 'c'))
                        + blob(3, std::string(16, 'b')) + blob(4, build)
                        + blob(5, caps)
                    : fixture::request(build, caps);
        break;
    case frame_kind::handshake_response:
        payload = blob(1, std::string(16, 'c')) + blob(2, std::string(16, 'b'))
                  + blob(3, build) + blob(4, caps);
        break;
    case frame_kind::redirect:
        payload = fixture::redirect(
          shape == 0 ? "host" : std::string(253, 'h'));
        break;
    case frame_kind::error:
        payload = shape == 0 ? scalar(1, 1)
                             : scalar(1, 3) + blob(2, std::string(1024, 'e'))
                                 + blob(3, fixture::routing());
        break;
    default:
        __builtin_trap();
    }
    if (shape == 3) {
        payload.reserve(65536);
        while (payload.size() + 4103 <= 65536)
            payload += blob(99, std::string(4096, 'u'));
        if (65536 - payload.size() > 4100) payload += blob(99, "");
        const auto remaining = 65536 - payload.size();
        bool filled = remaining == 0;
        for (std::size_t n = remaining > 4 ? remaining - 4 : 0;
             n <= 4096 && !filled;
             ++n) {
            const auto part = blob(99, std::string(n, 'u'));
            if (part.size() == remaining) {
                payload += part;
                filled = true;
            }
        }
        require(filled && payload.size() == 65536);
    }
    return payload;
}

} // namespace kwaque::protocol::testing
