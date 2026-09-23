#pragma once

#include "src/codec/tests/benchmark_buffer.h"
#include "src/codec/tests/qualification_profile.h"
#include "src/model/batch_builder.h"
#include "src/model/batch_codec.h"
#include "src/model/record_codec.h"

#include <optional>
#include <vector>

namespace kwaque::model::bench {
using codec::bench::capacity_bound;
using codec::bench::copy_layout;
using codec::bench::qualify_allocator;
using codec::bench::require;
batch_decode_expectation expected_context();
batch_id fixture_id();
producer_stream_binding fixture_binding();

class model_fixture {
public:
    model_fixture(
      std::size_t count,
      std::size_t payload,
      std::size_t width,
      codec::bench::payload_pattern pattern
      = codec::bench::payload_pattern::compressible) noexcept
      : count(count)
      , payload_size(payload)
      , width(width)
      , pattern(pattern) {}
    seastar::future<> initialize(bool report = false);
    seastar::future<submitted_batch> build(codec::cooperative_work& work);
    seastar::future<submitted_batch> submitted(codec::cooperative_work& work);
    seastar::future<assigned_batch>
    assigned(codec::cooperative_work& work, bool sparse = false);
    codec::decode_budget memory() const noexcept {
        return {remaining, byte_count{1U << 20U}, capacity_bound};
    }
    static runtime::wall_time timestamp(std::size_t i) noexcept {
        return runtime::wall_time{100 + static_cast<std::int64_t>(i % 7U)};
    }
    std::size_t count;
    std::size_t payload_size;
    std::size_t width;
    codec::bench::payload_pattern pattern;
    std::optional<record> value;
    bytes::fragmented_buffer record_wire;
    bytes::fragmented_buffer submitted_wire;
    bytes::fragmented_buffer assigned_wire;
    bytes::fragmented_buffer sparse_wire;
    std::vector<range_logical_count> selected;
    std::optional<codec::semantic_batch_digest> digest;
    byte_count record_region_bytes;
    byte_count cache_charge;
    byte_count remaining{
      (64U << 20U) - codec::testing::execution_reservation.value()};

private:
    bool initialized_{false};
    seastar::future<> account(codec::cooperative_work& work);
};
} // namespace kwaque::model::bench
