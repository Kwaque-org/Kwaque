#pragma once

#include "src/model/batch_codec.h"
#include "src/model/tests/record_fuzz_oracle.h"
#include "src/protocol/tests/frame_test_support.h"

#include <array>
#include <cstdint>
#include <string>
#include <string_view>

namespace kwaque::protocol::testing::batch_frame_fixture {
namespace fixture = frame_fixture;
namespace oracle = model::testing;

template<typename Id>
inline Id id(std::uint8_t first) {
    std::array<std::uint8_t, 16> bytes{};
    for (std::size_t i = 0; i < bytes.size(); ++i)
        bytes[i] = static_cast<std::uint8_t>(first + i);
    return Id::make(bytes).value();
}
inline model::batch_decode_expectation expected() {
    return {id<model::topic_id>(33), id<model::range_id>(65)};
}
inline model::producer_stream_binding binding(std::uint8_t segment = 97) {
    return model::producer_stream_binding::make(
             id<model::topic_id>(33),
             id<model::range_id>(65),
             model::range_routing_epoch::make(7).value(),
             id<model::segment_id>(segment),
             model::segment_generation::make(9).value())
      .value();
}
inline std::string records(bool sparse = false) {
    std::string all;
    for (std::uint64_t delta = 0; delta < 3; ++delta) {
        if (sparse && delta == 1) continue;
        std::string body(1, '\0');
        body += oracle::varuint(2U * delta);
        body += oracle::varuint(delta);
        body += oracle::varuint(1);
        body += oracle::varuint(6);
        body += "xyz";
        body += oracle::varuint(0);
        all += oracle::varuint(body.size()) + body;
    }
    return all;
}
inline std::string
batch_wire(bool assigned, bool compressed = false, bool sparse = false) {
    const auto original = records();
    const auto raw = records(sparse);
    auto body = oracle::identity_bytes();
    body.resize(assigned ? 184 : 168, '\0');
    oracle::put(body, 136, 100, 8);
    oracle::put(body, 144, 3, 4);
    oracle::put(body, 148, sparse ? 2U : 3U, 4);
    oracle::put(body, 156, compressed ? 1U : 0U, 1);
    oracle::put(body, 158, 1, 2);
    const auto encoded = compressed ? oracle::lz4_records(raw) : raw;
    oracle::put(body, 160, encoded.size(), 4);
    oracle::put(body, 164, raw.size(), 4);
    if (assigned) {
        oracle::put(body, 168, 10, 8);
        oracle::put(body, 176, 13, 8);
    }
    const auto digest = oracle::fingerprint(body, original);
    for (std::size_t i = 0; i < digest.size(); ++i)
        body[104 + i] = static_cast<char>(digest[i]);
    return oracle::frame(body + encoded, assigned, false);
}
inline std::string frame(std::string_view batch, bool assigned = false) {
    auto wire = fixture::wire(batch);
    fixture::put_u16(wire, 6, assigned ? 17 : 16);
    fixture::repair(wire);
    return wire;
}
} // namespace kwaque::protocol::testing::batch_frame_fixture
