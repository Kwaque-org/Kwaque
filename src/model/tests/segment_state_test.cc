#include "src/base/error.h"
#include "src/model/segment_state.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <tuple>
#include <type_traits>
#include <utility>

namespace {

using kwaque::model::append_state;
using kwaque::model::append_state_from_raw;
using kwaque::model::completion_state;
using kwaque::model::is_valid;
using kwaque::model::remote_state;
using kwaque::model::remote_state_from_raw;
using kwaque::model::replica_state;
using kwaque::model::replica_state_from_raw;
using kwaque::model::retention_state;
using kwaque::model::retention_state_from_raw;

template<typename Left, typename Right>
concept comparison_expression = requires(Left lhs, Right rhs) { lhs == rhs; }
                                || requires(Left lhs, Right rhs) { lhs != rhs; }
                                || requires(Left lhs, Right rhs) { lhs < rhs; }
                                || requires(Left lhs, Right rhs) { lhs <= rhs; }
                                || requires(Left lhs, Right rhs) { lhs > rhs; }
                                || requires(Left lhs, Right rhs) { lhs >= rhs; }
                                || requires(Left lhs, Right rhs) {
                                       lhs <=> rhs;
                                   };

template<typename Value>
concept increment_expression = requires(Value value) { ++value; }
                               || requires(Value value) { value++; }
                               || requires(Value value) { --value; }
                               || requires(Value value) { value--; };

template<typename Left, typename Right>
consteval bool state_pair_contract() {
    if constexpr (std::same_as<Left, Right>) {
        return requires(Left lhs, Right rhs) {
            { lhs == rhs } -> std::same_as<bool>;
        };
    } else {
        return !comparison_expression<Left, Right>
               && !std::convertible_to<Left, Right>;
    }
}

template<typename State, typename... Other>
consteval bool state_peer_contract(std::tuple<Other...>*) {
    constexpr auto occurrences
      = (std::size_t{std::same_as<State, Other>} + ...);
    return occurrences == 1U && (state_pair_contract<State, Other>() && ...)
           && std::is_scoped_enum_v<State>
           && std::same_as<std::underlying_type_t<State>, std::uint8_t>
           && sizeof(State) == 1U && std::is_trivially_copyable_v<State>
           && std::is_nothrow_copy_constructible_v<State>
           && std::is_nothrow_copy_assignable_v<State>
           && std::is_nothrow_move_constructible_v<State>
           && std::is_nothrow_move_assignable_v<State>
           && std::is_nothrow_destructible_v<State>
           && !std::convertible_to<State, std::uint8_t>
           && !std::convertible_to<std::uint8_t, State>
           && !std::convertible_to<State, std::uint64_t>
           && !comparison_expression<State, std::uint64_t>
           && !comparison_expression<std::uint64_t, State>
           && !increment_expression<State>;
}

template<typename... State>
consteval bool state_type_contract(std::tuple<State...>* types) {
    return (state_peer_contract<State>(types) && ...);
}

using state_types = std::tuple<
  append_state,
  retention_state,
  remote_state,
  replica_state,
  completion_state>;
static_assert(state_type_contract(static_cast<state_types*>(nullptr)));
static_assert(!is_valid(append_state{}));
static_assert(!is_valid(retention_state{}));
static_assert(!is_valid(remote_state{}));
static_assert(!is_valid(replica_state{}));
static_assert(is_valid(completion_state{}));
static_assert(is_valid(append_state::sealed));
static_assert(is_valid(retention_state::deleted));
static_assert(is_valid(remote_state::remote_deleted));
static_assert(is_valid(replica_state::removed));
static_assert(noexcept(is_valid(append_state{})));
static_assert(noexcept(is_valid(retention_state{})));
static_assert(noexcept(is_valid(remote_state{})));
static_assert(noexcept(is_valid(replica_state{})));
static_assert(noexcept(append_state_from_raw(0)));
static_assert(noexcept(retention_state_from_raw(0)));
static_assert(noexcept(remote_state_from_raw(0)));
static_assert(noexcept(replica_state_from_raw(0)));

constexpr std::array append_codes{
  std::pair{std::uint64_t{1}, append_state::creating},
  std::pair{std::uint64_t{2}, append_state::active},
  std::pair{std::uint64_t{3}, append_state::sealing},
  std::pair{std::uint64_t{4}, append_state::sealed}};
constexpr std::array retention_codes{
  std::pair{std::uint64_t{1}, retention_state::retained},
  std::pair{std::uint64_t{2}, retention_state::expiring},
  std::pair{std::uint64_t{3}, retention_state::deleted}};
constexpr std::array remote_codes{
  std::pair{std::uint64_t{1}, remote_state::not_offloaded},
  std::pair{std::uint64_t{2}, remote_state::offloading},
  std::pair{std::uint64_t{3}, remote_state::offloaded},
  std::pair{std::uint64_t{4}, remote_state::remote_delete_pending},
  std::pair{std::uint64_t{5}, remote_state::remote_deleted}};
constexpr std::array replica_codes{
  std::pair{std::uint64_t{1}, replica_state::assigned},
  std::pair{std::uint64_t{2}, replica_state::copying},
  std::pair{std::uint64_t{3}, replica_state::readable},
  std::pair{std::uint64_t{4}, replica_state::lost},
  std::pair{std::uint64_t{5}, replica_state::removing},
  std::pair{std::uint64_t{6}, replica_state::removed}};

template<typename State, std::size_t Count, typename Parse>
void verify_numeric_domain(
  const std::array<std::pair<std::uint64_t, State>, Count>& codes,
  Parse parse) {
    EXPECT_EQ(static_cast<std::uint8_t>(State::uninitialized), 0U);
    EXPECT_EQ(State{}, State::uninitialized);
    for (std::uint64_t raw = 0; raw < 256; ++raw) {
        const auto expected = std::ranges::find_if(
          codes, [raw](const auto& entry) { return entry.first == raw; });
        const bool known = expected != codes.end();
        const auto parsed = parse(raw);
        EXPECT_EQ(is_valid(static_cast<State>(raw)), known) << raw;
        ASSERT_EQ(parsed.has_value(), known) << raw;
        if (known) {
            EXPECT_EQ(*parsed, expected->second) << raw;
            EXPECT_EQ(static_cast<std::uint8_t>(expected->second), raw);
        } else {
            EXPECT_EQ(parsed.error(), kwaque::errc::invalid_argument) << raw;
        }
    }

    const std::array wide_values{
      std::uint64_t{256},
      std::uint64_t{511},
      std::uint64_t{65536},
      std::uint64_t{1} << 32U,
      std::uint64_t{1} << 63U,
      std::numeric_limits<std::uint64_t>::max()};
    for (const auto raw : wide_values) {
        const auto invalid = parse(raw);
        ASSERT_FALSE(invalid.has_value()) << raw;
        EXPECT_EQ(invalid.error(), kwaque::errc::invalid_argument) << raw;
    }
    for (const auto& [raw, state] : codes) {
        EXPECT_TRUE(is_valid(state));
        for (const auto high_bits :
             {std::uint64_t{256},
              std::uint64_t{1} << 32U,
              std::uint64_t{1} << 63U}) {
            const auto invalid = parse(high_bits + raw);
            ASSERT_FALSE(invalid.has_value()) << high_bits + raw;
            EXPECT_EQ(invalid.error(), kwaque::errc::invalid_argument)
              << high_bits + raw;
        }
    }
}

TEST(SegmentStateTest, AppendCodesAndWideInputValidation) {
    verify_numeric_domain(append_codes, append_state_from_raw);
}

TEST(SegmentStateTest, RetentionCodesAndWideInputValidation) {
    verify_numeric_domain(retention_codes, retention_state_from_raw);
}

TEST(SegmentStateTest, RemoteCodesAndWideInputValidation) {
    verify_numeric_domain(remote_codes, remote_state_from_raw);
}

TEST(SegmentStateTest, ReplicaCodesAndWideInputValidation) {
    verify_numeric_domain(replica_codes, replica_state_from_raw);
}

} // namespace
