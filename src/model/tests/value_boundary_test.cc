#include "src/base/result.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/codec/limits.h"
#include "src/model/batch_context.h"
#include "src/model/batch_identity.h"
#include "src/model/epoch.h"
#include "src/model/identity.h"
#include "src/model/keyspace.h"
#include "src/model/position.h"
#include "src/model/segment_state.h"
#include "src/model/topic_policy.h"
#include "src/model/transport_identity.h"
#include "src/runtime/cross_shard.h"
#include "src/runtime/error.h"
#include "src/runtime/file_position.h"
#include "src/runtime/shard_affinity.h"
#include "src/runtime/time.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <functional>
#include <memory>
#include <span>
#include <string_view>
#include <tuple>

namespace {

namespace model = kwaque::model;
using kwaque::runtime::cross_shard_value;
using kwaque::runtime::owner_shard;

template<typename... Value>
consteval bool model_values_stay_shard_local(std::tuple<Value...>*) {
    return (
      (!cross_shard_value<Value> && !cross_shard_value<const Value>
       && !cross_shard_value<Value&> && !cross_shard_value<const Value&>
       && !cross_shard_value<Value*>
       && !cross_shard_value<kwaque::result<Value>>
       && !cross_shard_value<kwaque::runtime::result<Value>>)
      && ...);
}

using model_values = std::tuple<
  model::cluster_id,
  model::broker_id,
  model::tenant_id,
  model::topic_id,
  model::range_id,
  model::segment_id,
  model::producer_id,
  model::control_transaction_id,
  model::manifest_id,
  model::wal_incarnation_id,
  model::vnode_index,
  model::segment_generation,
  model::range_routing_epoch,
  model::lease_epoch,
  model::producer_epoch,
  model::range_manifest_generation,
  model::producer_stream_id,
  model::batch_sequence,
  model::transport_stream_id,
  model::correlation_id,
  model::frame_sequence,
  model::batch_id,
  model::producer_stream_binding,
  model::range_logical_count,
  model::range_logical_offset,
  model::range_logical_end,
  model::range_logical_span,
  model::segment_record_count,
  model::segment_relative_offset,
  model::segment_relative_end,
  model::segment_relative_span,
  model::file_byte_span,
  model::record_id,
  model::physical_address,
  model::keyspace_boundary,
  model::keyspace_interval,
  model::ordered_keyspace_coverage,
  model::append_state,
  model::retention_state,
  model::remote_state,
  model::replica_state,
  model::completion_state,
  model::segment_completion_facts,
  model::topic_policy,
  model::submitted_batch_context,
  model::assigned_batch_context,
  kwaque::codec::limits_config,
  kwaque::codec::limits,
  kwaque::codec::operation_usage,
  kwaque::byte_count,
  kwaque::item_count,
  kwaque::runtime::file_position,
  kwaque::runtime::wall_time,
  kwaque::runtime::monotonic_duration,
  kwaque::runtime::monotonic_time>;

constexpr bool all_model_values_denied = model_values_stay_shard_local(
  static_cast<model_values*>(nullptr));
static_assert(all_model_values_denied);
static_assert(cross_shard_value<owner_shard>);
static_assert(cross_shard_value<kwaque::runtime::operation_error>);
static_assert(cross_shard_value<kwaque::runtime::result<void>>);
static_assert(cross_shard_value<kwaque::runtime::result<owner_shard>>);
static_assert(!cross_shard_value<kwaque::result<void>>);
static_assert(!cross_shard_value<kwaque::result<owner_shard>>);
static_assert(!cross_shard_value<owner_shard&>);
static_assert(!cross_shard_value<owner_shard*>);
static_assert(!cross_shard_value<std::reference_wrapper<owner_shard>>);
static_assert(!cross_shard_value<std::span<const owner_shard>>);
static_assert(!cross_shard_value<std::span<const std::byte>>);
static_assert(!cross_shard_value<std::string_view>);
static_assert(!cross_shard_value<kwaque::result<std::span<const std::byte>>>);
static_assert(
  !cross_shard_value<kwaque::runtime::result<std::span<const std::byte>>>);
static_assert(!cross_shard_value<kwaque::runtime::result<std::string_view>>);
static_assert(!cross_shard_value<std::unique_ptr<owner_shard>>);
static_assert(!cross_shard_value<std::shared_ptr<owner_shard>>);
static_assert(!cross_shard_value<kwaque::bytes::fragmented_buffer>);
static_assert(!cross_shard_value<
              kwaque::runtime::result<kwaque::bytes::fragmented_buffer>>);

TEST(ValueBoundaryTest, KeepsModelValuesDeniedWithoutChangingExistingOptIns) {
    EXPECT_TRUE(all_model_values_denied);
    EXPECT_TRUE(cross_shard_value<owner_shard>);
    EXPECT_TRUE(cross_shard_value<kwaque::runtime::result<owner_shard>>);
    EXPECT_FALSE(cross_shard_value<kwaque::result<owner_shard>>);
}

} // namespace
