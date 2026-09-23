#include "src/base/units.h"
#include "src/runtime/file.h"
#include "src/runtime/file_error_internal.h"
#include "src/runtime/file_position.h"

#include <seastar/core/future.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>

namespace {

struct contract_directory_cursor {
    contract_directory_cursor(contract_directory_cursor&&) noexcept = default;
    contract_directory_cursor&
    operator=(contract_directory_cursor&&) noexcept = default;
    contract_directory_cursor(const contract_directory_cursor&) = delete;
    contract_directory_cursor&
    operator=(const contract_directory_cursor&) = delete;
    seastar::future<kwaque::runtime::result<kwaque::runtime::directory_page>>
      next(kwaque::runtime::directory_page_limits);
    seastar::future<kwaque::runtime::result<void>> sync();
    seastar::future<kwaque::runtime::result<void>> close();
};

struct contract_file_system {
    using directory_cursor_type = contract_directory_cursor;
    seastar::future<kwaque::runtime::result<directory_cursor_type>>
      open_directory(
        kwaque::runtime::file_path,
        kwaque::runtime::file_close_policy
        = kwaque::runtime::file_close_policy::legacy);
    seastar::future<kwaque::runtime::result<kwaque::runtime::file>>
      open(kwaque::runtime::file_path, kwaque::runtime::file_open_options);
    seastar::future<kwaque::runtime::result<bool>>
      exists(kwaque::runtime::file_path);
    seastar::future<kwaque::runtime::result<kwaque::runtime::file_status>>
      stat(kwaque::runtime::file_path);
    seastar::future<kwaque::runtime::result<kwaque::runtime::file_system_space>>
      space(kwaque::runtime::file_path);
    seastar::future<kwaque::runtime::result<kwaque::runtime::directory_listing>>
      list(
        kwaque::runtime::file_path, kwaque::runtime::directory_listing_limits);
    seastar::future<kwaque::runtime::result<void>>
      create_directories(kwaque::runtime::file_path);
    seastar::future<kwaque::runtime::result<void>>
      remove_file(kwaque::runtime::file_path);
    seastar::future<kwaque::runtime::result<void>>
      remove_directory(kwaque::runtime::file_path);
    seastar::future<kwaque::runtime::result<void>> rename(
      kwaque::runtime::file_path,
      kwaque::runtime::file_path,
      kwaque::runtime::file_rename_policy
      = kwaque::runtime::file_rename_policy::replace);
    seastar::future<kwaque::runtime::result<void>> sync_directory(
      kwaque::runtime::file_path,
      kwaque::runtime::file_close_policy
      = kwaque::runtime::file_close_policy::legacy);
};

struct invalid_file_system final {
    void open(kwaque::runtime::file_path, kwaque::runtime::file_open_options);
};

struct missing_retained_sync : contract_directory_cursor {
    void sync();
};
struct missing_sync_backend : contract_file_system {
    using directory_cursor_type = missing_retained_sync;
    seastar::future<kwaque::runtime::result<directory_cursor_type>>
      open_directory(
        kwaque::runtime::file_path, kwaque::runtime::file_close_policy);
};
struct missing_rename_policy : contract_file_system {
    seastar::future<kwaque::runtime::result<void>>
      rename(kwaque::runtime::file_path, kwaque::runtime::file_path);
};
struct missing_directory_close_policy : contract_file_system {
    seastar::future<kwaque::runtime::result<directory_cursor_type>>
      open_directory(kwaque::runtime::file_path);
};
static_assert(!kwaque::runtime::file_system_backend<missing_sync_backend>);
static_assert(!kwaque::runtime::file_system_backend<missing_rename_policy>);
static_assert(
  !kwaque::runtime::file_system_backend<missing_directory_close_policy>);

static_assert(kwaque::runtime::file_system_backend<contract_file_system>);
static_assert(!kwaque::runtime::file_system_backend<invalid_file_system>);
static_assert(std::is_move_constructible_v<kwaque::runtime::file>);
static_assert(!std::is_move_assignable_v<kwaque::runtime::file>);
static_assert(!std::is_copy_constructible_v<kwaque::runtime::file>);

template<typename T>
concept exposes_preallocation = requires(T& owner) { owner.allocate(0, 4096); };
static_assert(!exposes_preallocation<kwaque::runtime::file>);

TEST(FileContractTest, DirectoryPagesHaveIndependentHardLimits) {
    using kwaque::runtime::directory_page_limits;
    EXPECT_TRUE(directory_page_limits{}.validate().has_value());
    EXPECT_TRUE((directory_page_limits{
      kwaque::item_count{1024}, kwaque::byte_count{256U * 1024U}}
                   .validate()
                   .has_value()));
    EXPECT_FALSE(
      (directory_page_limits{kwaque::item_count{}, kwaque::byte_count{1}}
         .validate()
         .has_value()));
    EXPECT_FALSE(
      (directory_page_limits{kwaque::item_count{1025}, kwaque::byte_count{1}}
         .validate()
         .has_value()));
    EXPECT_FALSE(
      (directory_page_limits{kwaque::item_count{1}, kwaque::byte_count{}}
         .validate()
         .has_value()));
    EXPECT_FALSE((directory_page_limits{
      kwaque::item_count{1}, kwaque::byte_count{256U * 1024U + 1U}}
                    .validate()
                    .has_value()));
}

TEST(FileContractTest, FilePositionPreservesItsExistingInterface) {
    using kwaque::runtime::file_position;
    constexpr file_position zero;
    constexpr file_position maximum{std::numeric_limits<std::uint64_t>::max()};
    constexpr auto next = zero.checked_add(kwaque::byte_count{17});
    constexpr auto terminal = maximum.checked_add(kwaque::byte_count{0});
    constexpr auto overflow = maximum.checked_add(kwaque::byte_count{1});

    static_assert(sizeof(file_position) == sizeof(std::uint64_t));
    static_assert(std::is_trivially_copyable_v<file_position>);
    static_assert(!std::is_convertible_v<std::uint64_t, file_position>);
    static_assert(next.has_value() && next->value() == 17);
    static_assert(terminal.has_value() && *terminal == maximum);
    static_assert(!overflow.has_value());
    EXPECT_LT(zero, *next);
    EXPECT_LT(*next, maximum);
}

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

TEST(FileContractTest, BoundedValuesDoNotRetainCallerCapacity) {
    using namespace kwaque;
    using namespace kwaque::runtime;
    std::string oversized;
    oversized.reserve(maximum_contiguous_allocation_bytes * 2);
    oversized = "entry";
    const auto name = file_name::make(oversized);
    const auto path = file_path::make(oversized);
    ASSERT_TRUE(name.has_value());
    ASSERT_TRUE(path.has_value());
    EXPECT_EQ(name->value(), "entry");
    EXPECT_LT(name->value().capacity(), oversized.capacity());
    EXPECT_LT(path->value().capacity(), oversized.capacity());

    seastar::chunked_vector<directory_entry> entries;
    entries.reserve(decltype(entries)::elements_per_fragment());
    entries.push_back({*name, file_kind::regular});
    auto listing = directory_listing::make(
      std::move(entries),
      {.maximum_entries = item_count{1}, .maximum_name_bytes = byte_count{5}});
    ASSERT_TRUE(listing.has_value());
    EXPECT_EQ(listing->entries().size(), 1U);
    EXPECT_EQ(listing->entries().capacity(), 1U);
    seastar::chunked_vector<directory_entry> invalid;
    invalid.push_back({*name, static_cast<file_kind>(255)});
    EXPECT_FALSE(directory_listing::make(std::move(invalid), {}).has_value());
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

TEST(FileContractTest, SpaceSamplesCheckUnitsSentinelsAndOrdering) {
    using kwaque::byte_count;
    using kwaque::runtime::file_system_space;
    static_assert(!std::is_default_constructible_v<file_system_space>);
    static_assert(std::is_trivially_copyable_v<file_system_space>);
    static_assert(
      !std::is_default_constructible_v<kwaque::runtime::file_geometry>);
    const auto sample = file_system_space::from_blocks(
      4096, 256, 128, 64, true);
    ASSERT_TRUE(sample.has_value());
    EXPECT_EQ(sample->capacity(), byte_count{1048576});
    EXPECT_EQ(sample->free(), byte_count{524288});
    EXPECT_EQ(sample->available(), byte_count{262144});
    EXPECT_TRUE(sample->read_only());
    EXPECT_TRUE(file_system_space::from_blocks(4096, 256, 0, 0, false));
    EXPECT_TRUE(file_system_space::make({}, {}, {}, true));
    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    EXPECT_FALSE(file_system_space::from_blocks(0, 1, 1, 1, false));
    EXPECT_FALSE(file_system_space::from_blocks(maximum, 0, 0, 0, false));
    EXPECT_FALSE(file_system_space::from_blocks(1, maximum, 0, 0, false));
    EXPECT_FALSE(file_system_space::from_blocks(1, 1, maximum, 0, false));
    EXPECT_FALSE(file_system_space::from_blocks(1, 1, 1, maximum, false));
    EXPECT_FALSE(file_system_space::from_blocks(4096, 1, 2, 0, false));
    EXPECT_FALSE(file_system_space::from_blocks(4096, 2, 1, 2, false));
    const auto overflow = file_system_space::from_blocks(
      4096, maximum / 4096 + 1, 0, 0, false);
    ASSERT_FALSE(overflow);
    EXPECT_EQ(overflow.error().code(), kwaque::errc::out_of_range);
    EXPECT_TRUE(file_system_space::from_blocks(1, maximum - 1, 0, 0, false));
    EXPECT_FALSE(file_system_space::make(byte_count{maximum}, {}, {}, false));
}

TEST(FileContractTest, FileFailureDetailsPreserveRealCausesWithoutGuessing) {
    using kwaque::runtime::file_failure_detail;
    using kwaque::runtime::detail::map_file_operation_error;
    struct sample {
        std::error_code native;
        kwaque::errc code;
        file_failure_detail detail;
    };
    const std::array cases{
      sample{
        std::make_error_code(std::errc::no_space_on_device),
        kwaque::errc::resource_exhausted,
        file_failure_detail::no_space},
      sample{
        std::make_error_code(std::errc::too_many_files_open),
        kwaque::errc::resource_exhausted,
        file_failure_detail::descriptor_limit},
      sample{
        std::make_error_code(std::errc::too_many_files_open_in_system),
        kwaque::errc::resource_exhausted,
        file_failure_detail::descriptor_limit},
      sample{
        std::make_error_code(std::errc::read_only_file_system),
        kwaque::errc::permission_denied,
        file_failure_detail::read_only},
      sample{
        std::make_error_code(std::errc::permission_denied),
        kwaque::errc::permission_denied,
        file_failure_detail::permission},
      sample{
        std::make_error_code(std::errc::io_error),
        kwaque::errc::io_failure,
        file_failure_detail::device_io},
      sample{
        std::error_code{EDQUOT, std::system_category()},
        kwaque::errc::resource_exhausted,
        file_failure_detail::quota},
      sample{
        std::error_code{123456, std::generic_category()},
        kwaque::errc::io_failure,
        file_failure_detail::unknown}};
    for (const auto& value : cases) {
        const auto mapped = map_file_operation_error(value.native);
        EXPECT_EQ(mapped.code(), value.code);
        const auto detail = kwaque::runtime::file_detail(mapped);
        ASSERT_TRUE(detail.has_value());
        EXPECT_EQ(*detail, value.detail);
    }
    const auto absent = kwaque::runtime::file_detail(
      kwaque::runtime::operation_error{
        kwaque::errc::resource_exhausted,
        kwaque::runtime::operation_kind::file});
    ASSERT_TRUE(absent.has_value());
    EXPECT_EQ(*absent, file_failure_detail::unknown);
    EXPECT_FALSE(
      kwaque::runtime::file_detail(
        kwaque::runtime::operation_error{
          kwaque::errc::io_failure, kwaque::runtime::operation_kind::network})
        .has_value());
    kwaque::runtime::operation_error invalid{
      kwaque::errc::io_failure, kwaque::runtime::operation_kind::file};
    ASSERT_TRUE(
      invalid.add_context(kwaque::runtime::operation_context_key::detail, 8));
    EXPECT_FALSE(kwaque::runtime::file_detail(invalid).has_value());
    auto bounded = kwaque::runtime::make_file_error(
      kwaque::errc::io_failure, file_failure_detail::device_io);
    EXPECT_TRUE(
      bounded.add_context(kwaque::runtime::operation_context_key::bytes, 11));
    EXPECT_TRUE(
      bounded.add_context(kwaque::runtime::operation_context_key::attempt, 2));
    EXPECT_TRUE(bounded.add_context(
      kwaque::runtime::operation_context_key::stable_id, 3));
    EXPECT_FALSE(
      bounded.add_context(kwaque::runtime::operation_context_key::shard, 4));
    EXPECT_EQ(
      bounded.context_at(0)->key,
      kwaque::runtime::operation_context_key::detail);
}
