#include "src/storage/local_store_config.h"

#include <gtest/gtest.h>

#include <array>

namespace kwaque::storage {
namespace {
template<typename Id>
Id id(std::uint8_t value) {
    std::array<std::uint8_t, 16> bytes{};
    bytes.fill(value);
    return Id::make(bytes).value();
}
local_device_spec spec(
  const char* path,
  std::uint8_t device,
  local_device_role role,
  std::uint64_t inode) {
    return {
      runtime::file_path::make(path).value(),
      local_store_context::make(
        id<model::cluster_id>(1),
        id<model::broker_id>(2),
        id<device_store_id>(device),
        local_store_shard)
        .value(),
      {1, storage_alignment::make(byte_count{4096}).value(), 2, role},
      {7, inode}};
}
TEST(
  LocalStoreConfigTest,
  RequiresOneControlDeviceAndDistinctIndependentBindings) {
    const auto wal = spec("/store/wal", 3, local_device_role::wal_control, 10);
    const auto data = spec("/store/data", 4, local_device_role::data, 11);
    EXPECT_TRUE(validate_local_device_set(std::array{wal, data}));
    EXPECT_FALSE(validate_local_device_set(std::array{data}));
    auto second = spec("/store/other", 5, local_device_role::all, 12);
    EXPECT_FALSE(validate_local_device_set(std::array{wal, second}));
    second.identity.role = local_device_role::data;
    second.directory = wal.directory;
    EXPECT_FALSE(validate_local_device_set(std::array{wal, second}));
    second.directory = {7, 12};
    second.root = runtime::file_path::make("/store/wal/nested").value();
    EXPECT_FALSE(validate_local_device_set(std::array{wal, second}));
    second = data;
    second.identity.shard_count = 3;
    EXPECT_FALSE(validate_local_device_set(std::array{wal, second}));
    second = data;
    second.mount_marker = runtime::file_name::make("store.meta").value();
    EXPECT_FALSE(validate_local_device_spec(second));
}
TEST(LocalStoreConfigTest, ValidatesScopesAndLimitsBeforeFilesystemWork) {
    auto value = spec("/store", 3, local_device_role::all, 10);
    EXPECT_TRUE(value.shard_owner(1));
    EXPECT_FALSE(value.shard_owner(2));
    value.identity.shard_count = 0;
    EXPECT_FALSE(validate_local_device_spec(value));
    value.identity.shard_count = maximum_local_shards + 1;
    EXPECT_EQ(
      validate_local_device_spec(value).error().code(),
      errc::resource_exhausted);
    value.identity.shard_count = 2;
    value.identity.layout_version = 2;
    EXPECT_EQ(
      validate_local_device_spec(value).error().code(),
      errc::unsupported_format);
    EXPECT_FALSE(local_store_io_limits{}.validate());
}
} // namespace
} // namespace kwaque::storage
