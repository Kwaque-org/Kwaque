#include "src/runtime/error.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>

namespace {

using kwaque::runtime::operation_context_field;
using kwaque::runtime::operation_context_key;
using kwaque::runtime::operation_error;
using kwaque::runtime::operation_kind;

class unsafe_category final : public std::error_category {
public:
    [[nodiscard]] const char* name() const noexcept final {
        return "unsafe/category\nname";
    }

    [[nodiscard]] std::string message(int) const final { return "unused"; }
};

static_assert(!std::is_convertible_v<kwaque::errc, operation_error>);
static_assert(!std::is_convertible_v<std::error_code, operation_error>);
static_assert(std::is_nothrow_move_constructible_v<operation_error>);
static_assert(std::is_nothrow_move_assignable_v<operation_error>);
static_assert(
  std::is_nothrow_move_constructible_v<kwaque::runtime::result<std::uint64_t>>);
static_assert(!std::is_constructible_v<
              operation_context_field,
              operation_context_key,
              std::string_view>);

TEST(OperationErrorTest, CarriesTypedBoundedNumericContext) {
    operation_error error{kwaque::errc::io_failure, operation_kind::file};
    EXPECT_TRUE(error.add_context(operation_context_key::shard, 3));
    EXPECT_TRUE(error.add_context(operation_context_key::bytes, 4096));
    EXPECT_FALSE(error.add_context(operation_context_key::shard, 7));
    EXPECT_TRUE(error.add_context(operation_context_key::attempt, 2));
    EXPECT_TRUE(error.add_context(operation_context_key::stable_id, 17));
    EXPECT_FALSE(error.add_context(operation_context_key::peer, 9));

    EXPECT_EQ(error.code(), kwaque::errc::io_failure);
    EXPECT_EQ(error.operation(), operation_kind::file);
    ASSERT_EQ(error.context().size(), operation_error::max_context_fields);
    EXPECT_EQ(
      error.context().front(),
      (operation_context_field{operation_context_key::shard, 3}));
}

TEST(OperationErrorTest, RendersOnlyBoundedAllowlistedFields) {
    operation_error error{
      kwaque::errc::network_failure, operation_kind::network};
    ASSERT_TRUE(error.add_context(operation_context_key::peer, 42));
    ASSERT_TRUE(error.add_context(operation_context_key::attempt, 3));

    const std::string rendered = error.render();
    EXPECT_EQ(rendered, "operation=network error=kwaque:12 peer=42 attempt=3");
    EXPECT_LE(rendered.size(), operation_error::max_rendered_size);
    EXPECT_EQ(rendered.find('\n'), std::string::npos);
    EXPECT_EQ(rendered.find('\r'), std::string::npos);
    EXPECT_EQ(rendered.find('/'), std::string::npos);
    EXPECT_EQ(rendered.find('\\'), std::string::npos);
}

TEST(OperationErrorTest, RuntimeResultSeparatesExpectedFailureFromExceptions) {
    kwaque::runtime::result<int> success{42};
    ASSERT_TRUE(success.has_value());
    EXPECT_EQ(*success, 42);

    kwaque::runtime::result<int> failure = std::unexpected(
      operation_error{kwaque::errc::timed_out, operation_kind::timer});
    ASSERT_FALSE(failure.has_value());
    EXPECT_EQ(failure.error().code(), kwaque::errc::timed_out);
    EXPECT_EQ(failure.error().operation(), operation_kind::timer);
}

TEST(OperationErrorTest, SanitizesForeignCategoryNames) {
    const unsafe_category category;
    const operation_error error{
      std::error_code{7, category}, operation_kind::generic};
    const std::string rendered = error.render();

    EXPECT_EQ(rendered, "operation=generic error=unsafe?category?name:7");
    EXPECT_EQ(rendered.find('\n'), std::string::npos);
    EXPECT_EQ(rendered.find('/'), std::string::npos);
    EXPECT_LE(rendered.size(), operation_error::max_rendered_size);
}

} // namespace
