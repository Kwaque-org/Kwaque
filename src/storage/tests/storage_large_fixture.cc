#include "src/storage/tests/storage_large_fixture.h"

#include "src/codec/tests/benchmark_buffer.h"
#include "src/model/batch_builder.h"
#include "src/model/record_codec.h"
#include "src/storage/tests/segment_test_support.h"

#include <seastar/core/coroutine.hh>

namespace kwaque::storage::testing {
seastar::future<codec::result<encoded_assigned_batch>> large_child(
  bool compressed, codec::cooperative_work& work, byte_count remaining) {
    using codec::bench::capacity_bound;
    auto payload = co_await codec::bench::patterned_buffer(
      1048565,
      65536,
      codec::bench::payload_pattern::incompressible,
      work,
      remaining);
    const auto cost = payload.allocation_cost(capacity_bound).value();
    const auto payload_cost = cost.backing.checked_add(cost.descriptors)
                                .value()
                                .checked_add(cost.share_controls)
                                .value();
    const auto allowance = remaining.checked_sub(payload_cost).value();
    const auto value
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
    auto made = model::batch_builder::make(
      identity, binding, work.policy(), capacity_bound);
    if (!made) co_return codec::failure(made.error());
    auto builder = std::move(*made);
    for (std::uint64_t i = 0; i < 8; ++i) {
        const auto added = co_await builder.add(
          value,
          runtime::wall_time{static_cast<std::int64_t>(i)},
          work,
          allowance);
        if (!added) co_return codec::failure(added.error());
    }
    auto finalized = co_await builder.finalize(work, allowance);
    if (!finalized) co_return codec::failure(finalized.error());
    auto submitted = std::move(*finalized);
    codec::bench::require(
      submitted.records().size() == byte_count{8U << 20U},
      "maximum record fixture has wrong expanded size");
    auto assigned = model::assigned_batch::assign(
                      std::move(submitted),
                      model::range_logical_end{100},
                      binding)
                      .value();
    co_return co_await make_encoded_assigned_batch(
      std::move(assigned),
      compressed ? compression::codec_id::lz4 : compression::codec_id::none,
      work,
      allowance,
      capacity_bound);
}
} // namespace kwaque::storage::testing
