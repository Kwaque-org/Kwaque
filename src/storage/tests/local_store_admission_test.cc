#include "src/base/error.h"
#include "src/base/units.h"
#include "src/codec/digest.h"
#include "src/model/identity.h"
#include "src/runtime/file.h"
#include "src/storage/local_metadata.h"
#include "src/storage/local_store_admission.h"
#include "src/storage/local_store_config.h"
#include "src/storage/local_types.h"
#include "src/storage/recovery_inventory.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace kwaque::storage {
namespace {
template<typename Id>
Id id(std::uint8_t value) {
    std::array<std::uint8_t, 16> bytes{};
    bytes.fill(value);
    return Id::make(bytes).value();
}
device_store_id device(std::uint8_t value) {
    return id<device_store_id>(value);
}
local_device_spec spec(
  const char* path,
  std::uint8_t named,
  local_device_role role,
  std::uint64_t inode) {
    return {
      runtime::file_path::make(path).value(),
      local_store_context::make(
        id<model::cluster_id>(1),
        id<model::broker_id>(2),
        device(named),
        local_store_shard)
        .value(),
      {1, storage_alignment::make(byte_count{4096}).value(), 2, role},
      {7, inode}};
}
// A control device and two data devices.
std::vector<local_device_spec> three() {
    return {
      spec("/c", 9, local_device_role::wal_control, 90),
      spec("/a", 1, local_device_role::data, 10),
      spec("/b", 2, local_device_role::data, 20)};
}
recovery_store_report ready(std::size_t devices) {
    recovery_store_report report;
    report.inspected = devices;
    return report;
}
void find(
  recovery_store_report& report,
  std::size_t device,
  recovery_store_verdict verdict,
  errc why) {
    report.devices[device].verdict = verdict;
    report.devices[device].error = detail::path_error(why);
    report.verdict = std::max(report.verdict, verdict);
}
local_checkpoint_entry pin(std::uint8_t on) {
    codec::content_digest bytes{};
    return {
      segment_context::make(
        id<model::cluster_id>(1),
        id<model::topic_id>(4),
        id<model::range_id>(5),
        id<model::segment_id>(6),
        model::segment_generation::make(1).value())
        .value(),
      on == 0 ? local_checkpoint_disposition::authorized_discard
              : local_checkpoint_disposition::segment_boundary,
      on == 0 ? device_store_id{} : device(on),
      4096,
      byte_count{4096},
      1,
      codec::immutable_object_digest{bytes}};
}

// Only the store that was there is opened: every configured device ready.
TEST(StoreAdmissionTest, AStoreWhoseEveryDeviceIsReadyIsAdmitted) {
    const auto devices = three();
    const auto admitted = admit_local_store(devices, ready(3), {});
    EXPECT_TRUE(admitted.admitted());
    EXPECT_FALSE(admitted.device.has_value());
}

// A device that is missing, emptied, swapped or of another store refuses
// the start, and the refusal names it and says why.
TEST(StoreAdmissionTest, ADeviceThatIsNotTheStoresRefusesAndNamesItself) {
    const auto devices = three();
    struct expected final {
        recovery_store_verdict verdict;
        errc why;
        store_refusal refusal;
    };
    for (const auto& one :
         {expected{
            recovery_store_verdict::unavailable,
            errc::io_failure,
            store_refusal::missing},
          expected{
            recovery_store_verdict::lost,
            errc::not_found,
            store_refusal::emptied},
          expected{
            recovery_store_verdict::incomplete,
            errc::not_found,
            store_refusal::incomplete},
          expected{
            recovery_store_verdict::unsupported,
            errc::unsupported_format,
            store_refusal::unsupported},
          expected{
            recovery_store_verdict::corrupt,
            errc::wrong_context,
            store_refusal::foreign}}) {
        for (std::size_t at = 0; at != devices.size(); ++at) {
            auto report = ready(3);
            find(report, at, one.verdict, one.why);
            const auto refused = admit_local_store(devices, report, {});
            EXPECT_EQ(refused.refusal, one.refusal);
            ASSERT_TRUE(refused.device.has_value());
            EXPECT_EQ(*refused.device, devices[at].owner.device());
            ASSERT_TRUE(refused.error.has_value());
            EXPECT_EQ(refused.error->code(), one.why);
        }
    }
    // Several devices refuse: the most severe is named, the first of
    // equals.
    auto report = ready(3);
    find(report, 0, recovery_store_verdict::lost, errc::not_found);
    find(report, 1, recovery_store_verdict::corrupt, errc::wrong_context);
    find(report, 2, recovery_store_verdict::corrupt, errc::corrupt_data);
    const auto worst = admit_local_store(devices, report, {});
    EXPECT_EQ(worst.refusal, store_refusal::foreign);
    EXPECT_EQ(*worst.device, device(1));
    EXPECT_EQ(worst.error->code(), errc::wrong_context);
}

// Two configured devices that are one refuse before anything else is
// looked at.
TEST(StoreAdmissionTest, ADuplicatedDeviceRefuses) {
    auto devices = three();
    devices.push_back(spec("/d", 2, local_device_role::data, 30));
    auto refused = admit_local_store(devices, ready(4), {});
    EXPECT_EQ(refused.refusal, store_refusal::duplicated);
    EXPECT_EQ(*refused.device, device(2));
    // One directory under two identities.
    devices.back() = spec("/d", 3, local_device_role::data, 10);
    refused = admit_local_store(devices, ready(4), {});
    EXPECT_EQ(refused.refusal, store_refusal::duplicated);
    EXPECT_EQ(*refused.device, device(3));
    // An inspection of another set is no inspection of this one.
    const auto fewer = admit_local_store(three(), ready(2), {});
    EXPECT_EQ(fewer.refusal, store_refusal::missing);
    EXPECT_FALSE(fewer.device.has_value());
}

// A device the durable checkpoint names must be configured to hold data:
// a segment it pins there would otherwise look never written.
TEST(StoreAdmissionTest, EveryDeviceTheCheckpointNamesIsConfigured) {
    const auto devices = three();
    const std::vector<local_checkpoint_entry> known{pin(1), pin(2), pin(0)};
    EXPECT_TRUE(admit_local_store(devices, ready(3), known).admitted());
    const std::vector<local_checkpoint_entry> removed{pin(1), pin(4)};
    auto refused = admit_local_store(devices, ready(3), removed);
    EXPECT_EQ(refused.refusal, store_refusal::unconfigured);
    EXPECT_EQ(*refused.device, device(4));
    // The control device holds no segment.
    const std::vector<local_checkpoint_entry> control{pin(9)};
    refused = admit_local_store(devices, ready(3), control);
    EXPECT_EQ(refused.refusal, store_refusal::unconfigured);
    EXPECT_EQ(*refused.device, device(9));
    // A device that is not ready is named before the checkpoint is read.
    auto report = ready(3);
    find(report, 2, recovery_store_verdict::lost, errc::not_found);
    refused = admit_local_store(devices, report, removed);
    EXPECT_EQ(refused.refusal, store_refusal::emptied);
    EXPECT_EQ(*refused.device, device(2));
}

// A device the runtime has no queue for is reported and refuses nothing.
TEST(StoreAdmissionTest, ADeviceWithNoQueueIsReported) {
    const auto devices = three();
    const std::array queued{true, false, true};
    const auto admitted = admit_local_store(devices, ready(3), {}, queued);
    EXPECT_TRUE(admitted.admitted());
    EXPECT_FALSE(admitted.unqueued[0]);
    EXPECT_TRUE(admitted.unqueued[1]);
    EXPECT_FALSE(admitted.unqueued[2]);
    // Not asked: nothing is reported.
    EXPECT_FALSE(admit_local_store(devices, ready(3), {}).unqueued[1]);
}
} // namespace
} // namespace kwaque::storage
