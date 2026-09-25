#pragma once

#include "src/codec/tests/allocation_observer.h"
#include "src/codec/tests/benchmark_buffer.h"
#include "src/codec/tests/memory_qualification_support.h"
#include "src/model/batch_builder.h"
#include "src/model/record_codec.h"
#include "src/storage/tests/storage_large_fixture.h"
#include "src/storage/tests/wal_append_contract.h"

#include <algorithm>
#include <cstdlib>
#include <optional>
#include <stdexcept>

namespace kwaque::storage::testing::wal_bench_support {
using store_contract::require;
inline void initialize() {
    static const bool profile_reported = [] {
        codec::testing::report_profile();
        return true;
    }();
    static_cast<void>(profile_reported);
    if (std::getenv("KWAQUE_WAL_OBSERVE_ALLOCATIONS")) {
#if defined(KWAQUE_WAL_TIMING_ONLY)
        throw std::runtime_error("allocation observation requires wal_bench");
#else
        static const bool installed
          = codec::testing::install_crypto_allocation_observation();
        require(installed, "crypto allocation observation unavailable");
#endif
    }
}

inline seastar::future<encoded_assigned_batch>
child(std::size_t size, codec::cooperative_work& work) {
    if (size == 0) {
        // Match the independent small fixture and its exact-child validator.
        auto raw = co_await installation_contract::buffer_async(
          assigned_wire());
        co_return (co_await validate_encoded_assigned_batch(
                     std::move(raw),
                     batch_expected(),
                     {byte_count{32U << 20U}, byte_count{1U << 20U}, charge},
                     work))
          .value();
    }
    if (size == 8U << 20U)
        co_return (co_await large_child(false, work, byte_count{64U << 20U}))
          .value();
    const auto count = std::max<std::size_t>(1, size / (1U << 20U));
    auto payload = co_await codec::bench::patterned_buffer(
      size / count - 1024,
      65504,
      codec::bench::payload_pattern::incompressible,
      work,
      byte_count{64U << 20U});
    auto record
      = model::make_record(
          {}, {}, std::optional{std::move(payload)}, {}, work.policy())
          .value();
    const auto identity = model::batch_id::make(
                            id<model::producer_id>(0x40),
                            model::producer_epoch::make(1).value(),
                            model::producer_stream_id::make(1).value(),
                            model::batch_sequence{0})
                            .value();
    const auto binding = model::producer_stream_binding::make(
                           sc().topic(),
                           sc().range(),
                           model::range_routing_epoch::make(1).value(),
                           sc().segment(),
                           sc().generation())
                           .value();
    auto builder = model::batch_builder::make(
                     identity, binding, work.policy(), charge)
                     .value();
    for (std::size_t i = 0; i < count; ++i)
        (co_await builder.add(
           record,
           runtime::wall_time{static_cast<std::int64_t>(i)},
           work,
           byte_count{48U << 20U}))
          .value();
    auto submitted
      = (co_await builder.finalize(work, byte_count{48U << 20U})).value();
    auto assigned = model::assigned_batch::assign(
                      std::move(submitted),
                      model::range_logical_end{100},
                      binding)
                      .value();
    co_return (co_await make_encoded_assigned_batch(
                 std::move(assigned),
                 compression::codec_id::none,
                 work,
                 byte_count{48U << 20U},
                 charge))
      .value();
}

} // namespace kwaque::storage::testing::wal_bench_support
