#include "src/storage/local_paths.h"

#include <gtest/gtest.h>

#include <array>
#include <string>

namespace kwaque::storage {
namespace {
template<typename Id>
Id identity(std::uint8_t high, std::uint8_t low) {
    std::array<std::uint8_t, 16> value{};
    value.front() = high;
    value.back() = low;
    return Id::make(value).value();
}
TEST(LocalPathsTest, FixedNamesAndBothBucketRulesRoundTrip) {
    auto paths
      = local_paths::make(runtime::file_path::make("/data").value()).value();
    EXPECT_EQ(paths.store()->value(), "/data/store.meta");
    EXPECT_EQ(paths.control(2)->value(), "/data/shards/00000002/control");
    auto wal = identity<model::wal_incarnation_id>(0x12, 0xab);
    EXPECT_EQ(
      paths.wal(2, wal)->value(),
      "/data/shards/00000002/wal/ab/120000000000000000000000000000ab.wal");
    EXPECT_EQ(
      parse_local_wal_name("120000000000000000000000000000ab.wal", 0xab)
        .value(),
      wal);
    local_segment_name segment{
      identity<model::segment_id>(0x34, 0xcd),
      model::segment_generation::make(UINT64_MAX).value()};
    const std::string name
      = "340000000000000000000000000000cd-ffffffffffffffff";
    EXPECT_EQ(
      paths.segment(2, segment)->value(),
      "/data/shards/00000002/segments/34/" + name);
    EXPECT_EQ(parse_local_segment_name(name, 0x34).value(), segment);
    EXPECT_EQ(
      paths.segment_file(2, segment, local_segment_file::data)->value(),
      paths.segment(2, segment)->value() + "/data");
    EXPECT_EQ(
      paths.object(2, segment, local_object_sequence::make(7).value())->value(),
      paths.segment(2, segment)->value() + "/objects/0000000000000007.obj");
    EXPECT_EQ(
      paths.sequence_file(2, local_sequence_file::evidence, 7)->value(),
      "/data/shards/00000002/checkpoints/evidence/0000000000000007.meta");
    EXPECT_EQ(
      parse_local_sequence_name("ffffffffffffffff.obj", false).value(),
      UINT64_MAX);
    EXPECT_EQ(parse_local_shard_name("fffffffe").value(), UINT32_MAX - 1);
}
TEST(LocalPathsTest, NoncanonicalZeroWrongBucketAndTrailingBytesReject) {
    for (auto name :
         {"1.wal",
          "00000000000000000000000000000000.wal",
          "000000000000000000000000000000AB.wal",
          "000000000000000000000000000000ab.wal.tmp",
          "000000000000000000000000000000ab"})
        EXPECT_FALSE(parse_local_wal_name(name, 0xab));
    EXPECT_FALSE(
      parse_local_wal_name("000000000000000000000000000000ab.wal", 0));
    EXPECT_FALSE(parse_local_segment_name(
      "340000000000000000000000000000cd-0000000000000000", 0x34));
    EXPECT_FALSE(parse_local_segment_name(
      "340000000000000000000000000000cd-0000000000000001", 0xcd));
    EXPECT_FALSE(parse_local_sequence_name("0000000000000000.meta", true));
    EXPECT_FALSE(parse_local_sequence_name("0000000000000001.meta.more", true));
    EXPECT_FALSE(parse_local_shard_name("ffffffff"));
    EXPECT_FALSE(parse_local_shard_name("1"));
    EXPECT_FALSE(parse_local_bucket("AF"));
    EXPECT_FALSE(parse_local_bucket("f"));
    for (auto root :
         {"relative", "/data/", "/data//x", "/data/../x", "/data/./x"}) {
        auto path = runtime::file_path::make(root);
        if (path) EXPECT_FALSE(local_paths::make(*path));
    }
}
TEST(LocalPathsTest, TemporaryAttemptsAreBoundedAndIndependentOfAllocators) {
    auto target = runtime::file_name::make("control").value();
    auto generation = local_publication_generation::make(1).value();
    EXPECT_EQ(
      local_temporary_name(target, generation, 0)->value(),
      "control.tmp-0000000000000001-00");
    EXPECT_EQ(
      local_temporary_name(target, generation, 63)->value(),
      "control.tmp-0000000000000001-3f");
    EXPECT_FALSE(local_temporary_name(target, generation, 64));
    EXPECT_FALSE(local_temporary_name(target, {}, 0));
    auto parsed = parse_local_temporary_name(
      "control.tmp-ffffffffffffffff-3f", target);
    ASSERT_TRUE(parsed);
    EXPECT_EQ(parsed->generation.value(), UINT64_MAX);
    EXPECT_EQ(parsed->attempt, 63U);
    for (auto name :
         {"control.tmp-0000000000000000-00",
          "control.tmp-0000000000000001-40",
          "control.tmp-0000000000000001-0F",
          "control.tmp-1-00",
          "control.tmp-0000000000000001-00.extra"})
        EXPECT_FALSE(parse_local_temporary_name(name, target));
    EXPECT_TRUE(local_temporary_name(
      runtime::file_name::make(std::string(231, 'a')).value(), generation, 0));
    EXPECT_FALSE(local_temporary_name(
      runtime::file_name::make(std::string(232, 'a')).value(), generation, 0));
    auto parent
      = runtime::file_path::make("/" + std::string(4094, 'x')).value();
    EXPECT_FALSE(local_child_path(parent, target));
}
} // namespace
} // namespace kwaque::storage
