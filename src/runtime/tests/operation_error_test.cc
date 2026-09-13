#include "src/runtime/error.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <type_traits>

namespace {

using kwaque::runtime::operation_context_field;
using kwaque::runtime::operation_context_key;
using kwaque::runtime::operation_error;
using kwaque::runtime::operation_kind;

static_assert(!std::is_convertible_v<kwaque::errc, operation_error>);
static_assert(std::is_nothrow_move_constructible_v<operation_error>);
static_assert(std::is_nothrow_move_assignable_v<operation_error>);
static_assert(std::is_trivially_copyable_v<operation_error>);
static_assert(sizeof(operation_error) <= 64);
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
    ASSERT_EQ(error.context_size(), operation_error::max_context_fields);
    const std::optional expected_context{
      operation_context_field{operation_context_key::shard, 3}};
    EXPECT_EQ(error.context_at(0), expected_context);
    EXPECT_FALSE(error.context_at(error.context_size()).has_value());
    EXPECT_FALSE(
      error.context_at(operation_error::max_context_fields).has_value());

    operation_error invalid_key{kwaque::errc::io_failure, operation_kind::file};
    EXPECT_FALSE(
      invalid_key.add_context(static_cast<operation_context_key>(255), 1));
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

TEST(OperationErrorTest, MaximumContextAndRejectedInsertionPreserveEveryField) {
    operation_error error{
      kwaque::errc::invariant_violation, operation_kind::observability};
    constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
    for (const auto key :
         {operation_context_key::deadline_ns,
          operation_context_key::occurrence,
          operation_context_key::stable_id,
          operation_context_key::sequence}) {
        ASSERT_TRUE(error.add_context(key, maximum));
        const auto snapshot = error;
        EXPECT_FALSE(error.add_context(key, 0));
        EXPECT_EQ(error, snapshot);
        EXPECT_FALSE(
          error.add_context(static_cast<operation_context_key>(255), 0));
        EXPECT_EQ(error, snapshot);
    }
    const auto before = error;
    for (const auto key :
         {operation_context_key::stable_id,
          operation_context_key::peer,
          static_cast<operation_context_key>(255)}) {
        EXPECT_FALSE(error.add_context(key, 0));
        EXPECT_EQ(error, before);
    }
    EXPECT_EQ(
      error.render(),
      "operation=observability error=kwaque:16"
      " deadline_ns=18446744073709551615 occurrence=18446744073709551615"
      " stable_id=18446744073709551615 sequence=18446744073709551615");
    EXPECT_LE(error.render().size(), operation_error::max_rendered_size);
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

TEST(OperationErrorTest, RuntimeLifetimeHasDistinctOperationKind) {
    operation_error error{kwaque::errc::closed, operation_kind::runtime};
    EXPECT_EQ(error.operation(), operation_kind::runtime);
    EXPECT_EQ(kwaque::runtime::to_string(error.operation()), "runtime");
}

TEST(OperationErrorTest, SchedulerBudgetVocabularyIsStableAndBounded) {
    static_assert(static_cast<std::uint8_t>(operation_kind::scheduler) == 9);
    static_assert(static_cast<std::uint8_t>(operation_kind::clock) == 10);
    static_assert(static_cast<std::uint8_t>(operation_kind::trace) == 11);
    static_assert(
      static_cast<std::uint8_t>(operation_kind::observability) == 12);
    static_assert(
      static_cast<std::uint8_t>(operation_context_key::sequence) == 8);
    static_assert(static_cast<std::uint8_t>(operation_context_key::limit) == 9);
    static_assert(
      static_cast<std::uint8_t>(operation_context_key::expected) == 10);
    static_assert(
      static_cast<std::uint8_t>(operation_context_key::actual) == 11);
    static_assert(
      static_cast<std::uint8_t>(operation_context_key::detail) == 12);
    EXPECT_EQ(kwaque::runtime::to_string(operation_kind::clock), "clock");
    EXPECT_EQ(kwaque::runtime::to_string(operation_kind::trace), "trace");
    EXPECT_EQ(
      kwaque::runtime::to_string(operation_kind::observability),
      "observability");

    operation_error error{
      kwaque::errc::resource_exhausted, operation_kind::scheduler};
    ASSERT_TRUE(error.add_context(operation_context_key::sequence, 41));
    ASSERT_TRUE(error.add_context(operation_context_key::limit, 40));
    EXPECT_EQ(
      error.render(),
      "operation=scheduler error=kwaque:8 sequence=41 limit=40");
}

} // namespace
