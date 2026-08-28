#include "src/runtime/file.h"
#include "src/runtime/fragmented_buffer_internal.h"

#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>

#include <gtest/gtest.h>

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <string_view>
#include <utility>

namespace {

static_assert(requires(
  kwaque::runtime::file& owner,
  kwaque::runtime::file_position position,
  kwaque::byte_count maximum_bytes,
  kwaque::bytes::fragmented_buffer data) {
    {
        owner.read(position, maximum_bytes)
    } -> std::same_as<seastar::future<
      kwaque::runtime::result<kwaque::runtime::file_read_result>>>;
    {
        owner.write(position, std::move(data))
    } -> std::same_as<
      seastar::future<kwaque::runtime::result<kwaque::byte_count>>>;
});
static_assert(sizeof(kwaque::runtime::file_position) == sizeof(std::uint64_t));

TEST(FileIoContractTest, ValidatesReadAndWriteBoundsBeforeDispatch) {
    EXPECT_FALSE(
      kwaque::runtime::validate_file_read_request(
        kwaque::runtime::file_position{}, kwaque::byte_count{})
        .has_value());
    EXPECT_FALSE(
      kwaque::runtime::validate_file_read_request(
        kwaque::runtime::file_position{},
        kwaque::byte_count{kwaque::runtime::maximum_file_io_bytes.value() + 1})
        .has_value());
    EXPECT_FALSE(
      kwaque::runtime::validate_file_read_request(
        kwaque::runtime::file_position{
          std::numeric_limits<std::uint64_t>::max()},
        kwaque::byte_count{1})
        .has_value());

    kwaque::bytes::fragmented_buffer empty;
    EXPECT_FALSE(
      kwaque::runtime::validate_file_write_request(
        kwaque::runtime::file_position{}, empty)
        .has_value());
    auto one_byte = kwaque::bytes::fragmented_buffer::copy_of(
      std::span<const char>{"x", 1});
    EXPECT_FALSE(
      kwaque::runtime::validate_file_write_request(
        kwaque::runtime::file_position{
          std::numeric_limits<std::uint64_t>::max()},
        one_byte)
        .has_value());
}

TEST(FileIoContractTest, InternalConsumptionTransfersOwnershipCanonically) {
    seastar::temporary_buffer<char> first{"ab", 2};
    seastar::temporary_buffer<char> second{"cde", 3};
    std::array fragments{std::move(first), std::move(second)};
    auto copied = kwaque::bytes::fragmented_buffer::copy_from_fragments(
      std::span<const seastar::temporary_buffer<char>>{fragments});
    ASSERT_TRUE(copied.has_value());
    auto buffer = std::move(*copied);

    auto consumer
      = kwaque::runtime::detail::fragmented_buffer_io_access::consume(buffer);
    auto first_owned = consumer.take_front();
    EXPECT_EQ(first_owned.size(), 2U);
    EXPECT_EQ(std::string_view(first_owned.get(), first_owned.size()), "ab");
    EXPECT_EQ(buffer.size(), kwaque::byte_count{3});
    EXPECT_EQ(buffer.retained_bytes(), kwaque::byte_count{3});
    EXPECT_EQ(buffer.fragment_count(), 1U);

    auto second_owned = consumer.take_front();
    EXPECT_EQ(second_owned.size(), 3U);
    EXPECT_TRUE(buffer.empty());
    EXPECT_EQ(buffer.retained_bytes(), kwaque::byte_count{});
    EXPECT_EQ(buffer.fragment_count(), 0U);
    EXPECT_TRUE(consumer.take_front().empty());
}

TEST(FileIoContractTest, ReadResultCarriesExplicitEofAndOwningBytes) {
    auto data = kwaque::bytes::fragmented_buffer::copy_of(
      std::span<const char>{"short", 5});
    auto result = kwaque::runtime::file_read_result::make(
      std::move(data), true, kwaque::byte_count{5});
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->eof());
    EXPECT_EQ(result->data().size(), kwaque::byte_count{5});
    auto owned = std::move(*result).take_data();
    EXPECT_TRUE(owned.content_equals("short"));

    auto invalid = kwaque::runtime::file_read_result::make(
      kwaque::bytes::fragmented_buffer{}, false, kwaque::byte_count{1});
    EXPECT_FALSE(invalid.has_value());
}

} // namespace
