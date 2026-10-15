#pragma once

#include "src/codec/digest.h"
#include "src/storage/local_metadata.h"
#include "src/storage/tests/local_metadata_fixture_cases.h"
#include "src/storage/tests/retry_test_support.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string_view>

// What a caller supplies to decode one checked-in local metadata record: the
// owner, the scope and the pin its table row names. The fixture test and the
// fuzz cases decode under the same expectation.
namespace kwaque::storage::testing::local_fixture {
template<typename Id>
inline Id literal_id(std::string_view s) {
    const auto bytes = hex(s);
    std::array<std::uint8_t, 16> value{};
    if (bytes.size() != value.size())
        throw std::runtime_error("invalid identity literal");
    for (std::size_t i = 0; i < value.size(); ++i)
        value[i] = static_cast<std::uint8_t>(bytes[i]);
    return Id::make(value).value();
}
inline codec::immutable_object_digest literal_digest(std::string_view s) {
    const auto bytes = hex(s);
    codec::content_digest value{};
    if (bytes.size() != value.size())
        throw std::runtime_error("invalid digest literal");
    for (std::size_t i = 0; i < value.size(); ++i)
        value[i] = static_cast<std::uint8_t>(bytes[i]);
    return codec::immutable_object_digest{value};
}
inline local_metadata_expectation expectation(const local_fixture_case& test) {
    auto owner = local_store_context::make(
                   id<model::cluster_id>(0x11),
                   id<model::broker_id>(0x22),
                   id<device_store_id>(test.device),
                   test.shard)
                   .value();
    auto header = local_metadata_header::make(
                    static_cast<local_metadata_kind>(test.kind),
                    owner,
                    local_publication_generation::make(test.generation).value())
                    .value();
    local_metadata_expectation result{header, alignment(4096)};
    if (!test.segment.empty()) {
        const auto raw = hex(test.segment);
        result.segment
          = segment_context::make(
              literal_id<model::cluster_id>(test.segment.substr(0, 32)),
              literal_id<model::topic_id>(test.segment.substr(32, 32)),
              literal_id<model::range_id>(test.segment.substr(64, 32)),
              literal_id<model::segment_id>(test.segment.substr(96, 32)),
              model::segment_generation::make(get(raw, 64, 8)).value())
              .value();
        result.segment_alignment = alignment(4096);
    }
    if (!test.wal.empty())
        result.wal_incarnation = literal_id<model::wal_incarnation_id>(
          test.wal);
    if (test.data_device) {
        result.data_device = id<device_store_id>(test.data_device);
        result.data_metadata_alignment = alignment(4096);
    }
    result.digest = literal_digest(test.digest);
    result.encoded_bytes = byte_count{test.bytes};
    if (test.kind == 7 || test.kind == 12)
        result.page = page_ref::make(
                        page_ordinal::make(test.ordinal).value(),
                        test.first,
                        test.count,
                        byte_count{test.bytes},
                        *result.digest)
                        .value();
    return result;
}
} // namespace kwaque::storage::testing::local_fixture
