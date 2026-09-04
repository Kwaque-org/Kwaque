#include "src/base/allocation.h"
#include "src/observability/testing/capture_event_sink.h"
#include "src/runtime/testing/contracts/contract_backends.h"
#include "src/runtime/testing/contracts/environment_component.h"

#include <gtest/gtest.h>

#include <concepts>
#include <cstdint>
#include <type_traits>

namespace {

using backend_type = kwaque::runtime::testing::production_shaped_backend;
using sink_type = kwaque::observability::testing::capture_event_sink;
using component_type
  = kwaque::runtime::testing::environment_component<backend_type, sink_type>;
using expected_view = kwaque::runtime::basic_runtime_view<
  backend_type,
  kwaque::runtime::runtime_capability::timer,
  kwaque::runtime::runtime_capability::random,
  kwaque::runtime::runtime_capability::file_system,
  kwaque::runtime::runtime_capability::network,
  kwaque::runtime::runtime_capability::dns,
  kwaque::runtime::runtime_capability::fault>;

template<typename View>
concept exposes_event_capability = requires(View& view) { view.event_sink(); };

template<typename View>
concept exposes_resource_capability = requires(View& view) {
    view.resource_manager();
};

template<typename Component>
concept exposes_broad_runtime = requires(Component& component) {
    component.runtime();
};

static_assert(std::same_as<component_type::view_type, expected_view>);
static_assert(
  std::same_as<component_type::monotonic_clock, backend_type::monotonic_clock>);
static_assert(
  std::same_as<component_type::wall_clock, backend_type::wall_clock>);
static_assert(std::same_as<component_type::file_type, kwaque::runtime::file>);
static_assert(std::same_as<
              component_type::listener_type,
              backend_type::network_type::listener_type>);
static_assert(std::same_as<
              component_type::connection_type,
              backend_type::network_type::connection_type>);
static_assert(std::constructible_from<
              component_type,
              expected_view,
              sink_type&,
              kwaque::resource::workload_handle>);
static_assert(!std::constructible_from<
              component_type,
              backend_type&,
              sink_type&,
              kwaque::resource::workload_handle>);
static_assert(!std::constructible_from<
              component_type,
              kwaque::runtime::basic_runtime<backend_type>&,
              sink_type&,
              kwaque::resource::workload_handle>);
static_assert(!std::constructible_from<
              component_type,
              expected_view,
              sink_type&,
              kwaque::resource::resource_manager&>);
static_assert(!exposes_event_capability<expected_view>);
static_assert(!exposes_resource_capability<expected_view>);
static_assert(!exposes_broad_runtime<component_type>);
static_assert(
  !kwaque::runtime::testing::exposes_component_environment<component_type>);
static_assert(!kwaque::runtime::testing::exposes_component_resource_manager<
              component_type>);
static_assert(!std::is_copy_constructible_v<component_type>);
static_assert(!std::is_move_constructible_v<component_type>);

TEST(EnvironmentComponentContractTest, KeepsFixedPayloadsSmall) {
    EXPECT_LT(
      kwaque::runtime::testing::environment_component_file_payload.size(),
      kwaque::maximum_contiguous_allocation_bytes);
    EXPECT_LT(
      kwaque::runtime::testing::environment_component_request_payload.size(),
      kwaque::maximum_contiguous_allocation_bytes);
    EXPECT_LT(
      kwaque::runtime::testing::environment_component_response_payload.size(),
      kwaque::maximum_contiguous_allocation_bytes);
}

} // namespace
