#include "src/storage/local_cleanup.h"

#include <gtest/gtest.h>

namespace kwaque::storage {
namespace {
template<typename T>
T identity(std::uint8_t value) {
    std::array<std::uint8_t, 16> bytes{};
    bytes.fill(value);
    return T::make(bytes).value();
}
local_metadata_claims wal_claims() {
    auto owner = local_store_context::make(
                   identity<model::cluster_id>(1),
                   identity<model::broker_id>(2),
                   identity<device_store_id>(3),
                   0)
                   .value();
    return {
      local_metadata_header::make(
        local_metadata_kind::wal_descriptor,
        owner,
        local_publication_generation::make(1).value())
        .value(),
      {},
      local_wal_high{}.checked_advance(1)->incarnation().value(),
      byte_count{4096},
      storage_alignment::make(byte_count{4096}).value()};
}
local_namespace_entry entry(local_entry_kind kind) {
    return {
      runtime::file_path::make("/store/item").value(),
      runtime::file_kind::regular,
      kind};
}
TEST(LocalCleanupTest, ZeroReadersCannotDischargePersistedReferences) {
    auto path = entry(local_entry_kind::object);
    local_discovered_record object{path};
    EXPECT_EQ(
      classify_local_cleanup(object, {}).classification,
      local_cleanup_class::unresolved);
    EXPECT_EQ(
      classify_local_cleanup(
        object, {local_reference_status::referenced, false, false, true})
        .classification,
      local_cleanup_class::referenced);
    EXPECT_EQ(
      classify_local_cleanup(
        object, {local_reference_status::absent, true, false, true})
        .classification,
      local_cleanup_class::reader_pinned);
    // Even complete reference absence cannot classify opaque object bytes.
    EXPECT_EQ(
      classify_local_cleanup(
        object, {local_reference_status::absent, false, false, true})
        .classification,
      local_cleanup_class::unresolved);
}
TEST(
  LocalCleanupTest, UnpublishedWalTailRequiresRecoveryDespiteAbsentReferences) {
    auto path = entry(local_entry_kind::wal);
    local_discovered_record wal{path};
    wal.claims = wal_claims();
    wal.file_bytes = 8192;
    EXPECT_EQ(
      classify_local_cleanup(
        wal, {local_reference_status::absent, false, false, true})
        .classification,
      local_cleanup_class::recovery_required);
    wal.file_bytes = 4096;
    EXPECT_EQ(
      classify_local_cleanup(wal, {}).classification,
      local_cleanup_class::unresolved);
    EXPECT_EQ(
      classify_local_cleanup(
        wal, {local_reference_status::absent, false, false, true})
        .classification,
      local_cleanup_class::orphan_candidate);
    EXPECT_EQ(
      classify_local_cleanup(
        wal, {local_reference_status::absent, false, true, true})
        .classification,
      local_cleanup_class::recovery_required);
}
TEST(LocalCleanupTest, TemporarySpellingAndFutureFormatNeverAuthorizeRemoval) {
    auto path = entry(local_entry_kind::temporary);
    path.temporary_target = local_entry_kind::store;
    local_discovered_record temp{path};
    EXPECT_EQ(
      classify_local_cleanup(
        temp, {local_reference_status::absent, false, false, true})
        .classification,
      local_cleanup_class::unresolved);
    temp.damage = detail::path_error(errc::unsupported_format);
    EXPECT_EQ(
      classify_local_cleanup(
        temp, {local_reference_status::absent, false, false, true})
        .classification,
      local_cleanup_class::unsupported);
    temp.damage = detail::path_error(errc::malformed_data);
    EXPECT_EQ(
      classify_local_cleanup(
        temp, {local_reference_status::absent, false, false, true})
        .classification,
      local_cleanup_class::damaged);
    temp.damage.reset();
    path.kind = local_entry_kind::unknown;
    EXPECT_EQ(
      classify_local_cleanup(
        temp, {local_reference_status::absent, false, false, true})
        .classification,
      local_cleanup_class::unsupported);
}
} // namespace
} // namespace kwaque::storage

namespace kwaque::storage {
TEST(LocalDiscoveryContractTest, SelectedVersionDoesNotIgnoreExplicitPins) {
    auto store = local_store_context::make(
                   identity<model::cluster_id>(1),
                   identity<model::broker_id>(2),
                   identity<device_store_id>(3),
                   local_store_shard)
                   .value();
    local_device_spec spec{
      runtime::file_path::make("/store").value(),
      store,
      {1,
       storage_alignment::make(byte_count{4096}).value(),
       1,
       local_device_role::all},
      {1, 1}};
    auto path = entry(local_entry_kind::control);
    path.shard = 0;
    local_record_expectation expected{
      {local_metadata_header::make(
         local_metadata_kind::shard_control,
         spec.shard_owner(0).value(),
         local_publication_generation::make(1).value())
         .value(),
       spec.identity.metadata_alignment},
      true};
    ASSERT_TRUE(validate_discovery_expectation(spec, path, expected));
    expected.record.digest = codec::immutable_object_digest{
      codec::sha256_digest{}};
    expected.record.encoded_bytes = byte_count{4096};
    auto rejected = validate_discovery_expectation(spec, path, expected);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code(), errc::invalid_argument);
}
} // namespace kwaque::storage
