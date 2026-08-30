#include "src/runtime/file.h"

#include <seastar/core/future.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <string>
#include <type_traits>
#include <utility>

namespace {

struct contract_file_system final {
    seastar::future<kwaque::runtime::result<kwaque::runtime::file>>
      open(kwaque::runtime::file_path, kwaque::runtime::file_open_options);
    seastar::future<kwaque::runtime::result<bool>>
      exists(kwaque::runtime::file_path);
    seastar::future<kwaque::runtime::result<kwaque::runtime::file_status>>
      stat(kwaque::runtime::file_path);
    seastar::future<kwaque::runtime::result<kwaque::runtime::directory_listing>>
      list(
        kwaque::runtime::file_path, kwaque::runtime::directory_listing_limits);
    seastar::future<kwaque::runtime::result<void>>
      create_directories(kwaque::runtime::file_path);
    seastar::future<kwaque::runtime::result<void>>
      remove_file(kwaque::runtime::file_path);
    seastar::future<kwaque::runtime::result<void>>
      remove_directory(kwaque::runtime::file_path);
    seastar::future<kwaque::runtime::result<void>>
      rename(kwaque::runtime::file_path, kwaque::runtime::file_path);
    seastar::future<kwaque::runtime::result<void>>
      sync_directory(kwaque::runtime::file_path);
};

struct invalid_file_system final {
    void open(kwaque::runtime::file_path, kwaque::runtime::file_open_options);
};

static_assert(kwaque::runtime::file_system_backend<contract_file_system>);
static_assert(!kwaque::runtime::file_system_backend<invalid_file_system>);
static_assert(std::is_move_constructible_v<kwaque::runtime::file>);
static_assert(!std::is_move_assignable_v<kwaque::runtime::file>);
static_assert(!std::is_copy_constructible_v<kwaque::runtime::file>);

TEST(FileContractTest, ValidatesBoundedPathsAndDirectoryNames) {
    auto path = kwaque::runtime::file_path::make("data/segment.log");
    ASSERT_TRUE(path.has_value());
    EXPECT_EQ(path->value(), "data/segment.log");

    EXPECT_FALSE(kwaque::runtime::file_path::make("").has_value());
    const auto oversized = kwaque::runtime::file_path::make(
      std::string(kwaque::runtime::maximum_file_path_bytes + 1, 'x'));
    ASSERT_FALSE(oversized.has_value());
    EXPECT_EQ(oversized.error().code(), kwaque::errc::out_of_range);
    EXPECT_FALSE(
      kwaque::runtime::file_path::make(std::string{"a\0b", 3}).has_value());

    auto name = kwaque::runtime::file_name::make("segment.log");
    ASSERT_TRUE(name.has_value());
    EXPECT_EQ(name->value(), "segment.log");
    EXPECT_FALSE(kwaque::runtime::file_name::make(".").has_value());
    EXPECT_FALSE(kwaque::runtime::file_name::make("..").has_value());
    EXPECT_FALSE(kwaque::runtime::file_name::make("a/b").has_value());
}

TEST(FileContractTest, ValidatesOpenOptionCombinations) {
    kwaque::runtime::file_open_options valid{
      .access = kwaque::runtime::file_access::read_write,
      .create = true,
      .exclusive = true,
      .truncate = true,
      .permissions = 0600U,
    };
    EXPECT_TRUE(valid.validate().has_value());

    auto invalid = valid;
    invalid.create = false;
    EXPECT_FALSE(invalid.validate().has_value());

    invalid = valid;
    invalid.access = kwaque::runtime::file_access::read_only;
    EXPECT_FALSE(invalid.validate().has_value());

    invalid = valid;
    invalid.permissions = 01000U;
    EXPECT_FALSE(invalid.validate().has_value());

    invalid = valid;
    invalid.access = static_cast<kwaque::runtime::file_access>(255);
    EXPECT_FALSE(invalid.validate().has_value());

    kwaque::runtime::directory_listing_limits listing_limits;
    EXPECT_TRUE(listing_limits.validate().has_value());
    listing_limits.maximum_entries = kwaque::item_count{0};
    EXPECT_FALSE(listing_limits.validate().has_value());
    listing_limits.maximum_entries = kwaque::item_count{
      kwaque::runtime::maximum_directory_entries + 1};
    const auto excessive_entries = listing_limits.validate();
    ASSERT_FALSE(excessive_entries.has_value());
    EXPECT_EQ(excessive_entries.error().code(), kwaque::errc::out_of_range);

    listing_limits.maximum_entries = kwaque::item_count{1};
    listing_limits.maximum_name_bytes = kwaque::byte_count{};
    EXPECT_FALSE(listing_limits.validate().has_value());
    listing_limits.maximum_name_bytes = kwaque::byte_count{
      kwaque::runtime::maximum_directory_name_bytes.value() + 1};
    EXPECT_FALSE(listing_limits.validate().has_value());
}

TEST(FileContractTest, DirectoryListingBoundsEntriesAndAggregateNameBytes) {
    auto first_name = kwaque::runtime::file_name::make("a");
    auto second_name = kwaque::runtime::file_name::make("bc");
    ASSERT_TRUE(first_name.has_value());
    ASSERT_TRUE(second_name.has_value());
    seastar::chunked_vector<kwaque::runtime::directory_entry> entries;
    entries.push_back(
      {.name = std::move(*first_name),
       .kind = kwaque::runtime::file_kind::regular});
    entries.push_back(
      {.name = std::move(*second_name),
       .kind = kwaque::runtime::file_kind::regular});

    auto listing = kwaque::runtime::directory_listing::make(
      std::move(entries),
      {.maximum_entries = kwaque::item_count{2},
       .maximum_name_bytes = kwaque::byte_count{3}});
    ASSERT_TRUE(listing.has_value());
    EXPECT_EQ(listing->entries().size(), 2U);

    auto oversized_name = kwaque::runtime::file_name::make("ab");
    ASSERT_TRUE(oversized_name.has_value());
    seastar::chunked_vector<kwaque::runtime::directory_entry> oversized_entries;
    oversized_entries.push_back(
      {.name = std::move(*oversized_name),
       .kind = kwaque::runtime::file_kind::regular});
    const auto rejected = kwaque::runtime::directory_listing::make(
      std::move(oversized_entries),
      {.maximum_entries = kwaque::item_count{1},
       .maximum_name_bytes = kwaque::byte_count{1}});
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().code(), kwaque::errc::resource_exhausted);
}

TEST(FileContractTest, DirectoryListingCrossesChunkBoundaries) {
    const auto entry_count = std::min<std::size_t>(
      seastar::chunked_vector<
        kwaque::runtime::directory_entry>::elements_per_fragment()
        + 1U,
      kwaque::runtime::maximum_directory_entries);
    seastar::chunked_vector<kwaque::runtime::directory_entry> entries;
    for (std::size_t index = 0; index < entry_count; ++index) {
        auto name = kwaque::runtime::file_name::make("entry");
        ASSERT_TRUE(name.has_value());
        entries.push_back(
          {.name = std::move(*name),
           .kind = kwaque::runtime::file_kind::regular});
    }
    auto listing = kwaque::runtime::directory_listing::make(
      std::move(entries),
      {.maximum_entries = kwaque::item_count{entry_count},
       .maximum_name_bytes = kwaque::byte_count{entry_count * 5U}});
    ASSERT_TRUE(listing.has_value());
    EXPECT_EQ(listing->entries().size(), entry_count);
}

} // namespace
