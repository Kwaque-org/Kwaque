#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/error.h"
#include "src/codec/integer.h"
#include "src/codec/limits.h"
#include "src/codec/transaction.h"

#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <new>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using namespace std::literals;
namespace codec = kwaque::codec;
using codec::decode_budget;
using codec::field_context;
using codec::input_boundary;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::item_count;
using kwaque::bytes::fragmented_buffer;
using kwaque::bytes::fragmented_buffer_parser;

constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
constexpr field_context context{.origin = 100, .family = 7, .field = 11};

// A deliberately conservative test charge, not a production allocator profile.
byte_count test_charge(byte_count requested) noexcept {
    if (requested.value() == 0) {
        return {};
    }
    const auto bytes = std::max(requested.value(), std::uint64_t{16});
    if (bytes > (std::uint64_t{1} << 62U)) {
        return byte_count{maximum};
    }
    return byte_count{2U * std::bit_ceil(bytes)};
}

byte_count undercharge(byte_count requested) noexcept {
    return byte_count{requested.value() == 0 ? 0 : requested.value() - 1U};
}

byte_count excessive_charge(byte_count requested) noexcept {
    return byte_count{requested.value() == 0 ? 0 : maximum};
}

decode_budget generous_budget() noexcept {
    return {
      byte_count{64U * 1024U * 1024U}, byte_count{1024U * 1024U}, test_charge};
}

seastar::temporary_buffer<char> fragment_of(std::string_view bytes) {
    seastar::temporary_buffer<char> fragment{bytes.size()};
    std::ranges::copy(bytes, fragment.get_write());
    return fragment;
}

fragmented_buffer
split_at(std::string_view bytes, const std::vector<std::size_t>& cuts = {}) {
    std::vector<seastar::temporary_buffer<char>> fragments;
    std::size_t previous = 0;
    for (const auto cut : cuts) {
        fragments.push_back(
          fragment_of(bytes.substr(previous, cut - previous)));
        previous = cut;
    }
    fragments.push_back(fragment_of(bytes.substr(previous)));
    return fragmented_buffer::copy_from_fragments(fragments).value();
}

std::string unread_bytes(const fragmented_buffer_parser& input) {
    std::string bytes(input.bytes_remaining().value(), '\0');
    input.peek_to(std::span<char>{bytes}).value();
    return bytes;
}

void expect_error(
  const auto& value,
  errc code,
  std::uint64_t offset,
  field_context diagnostic = context) {
    ASSERT_FALSE(value.has_value());
    EXPECT_EQ(value.error().code(), code);
    EXPECT_EQ(value.error().family(), diagnostic.family);
    EXPECT_EQ(value.error().field(), diagnostic.field);
    EXPECT_EQ(value.error().byte_offset(), offset);
}

decode_budget admitted(const fragmented_buffer_parser& input) {
    return codec::reserve_decode_input(
             input, codec::limits::defaults(), generous_budget(), context)
      .value();
}

template<typename Return>
struct transaction_callback {
    Return operator()(fragmented_buffer_parser&) const;
};

template<typename Return>
struct exact_callback {
    Return operator()(
      fragmented_buffer_parser&,
      field_context,
      input_boundary,
      decode_budget) const;
};

template<typename Return>
concept transaction_expression = requires(fragmented_buffer_parser& input) {
    codec::with_transaction(input, context, transaction_callback<Return>{});
};

template<typename Return>
concept exact_expression = requires(
  fragmented_buffer_parser& input,
  const codec::limits& bounds,
  decode_budget budget) {
    codec::decode_exact(
      input,
      byte_count{1},
      byte_count{1},
      bounds,
      budget,
      context,
      input_boundary::open,
      exact_callback<Return>{});
};

struct throwing_move {
    throwing_move(throwing_move&&) noexcept(false);
};

struct throwing_destructor {
    ~throwing_destructor() noexcept(false);
};

template<typename Return>
consteval bool rejected_callback_result() {
    return !transaction_expression<Return> && !exact_expression<Return>;
}

static_assert(transaction_expression<codec::result<void>>);
static_assert(transaction_expression<codec::result<std::uint64_t>>);
static_assert(transaction_expression<codec::result<fragmented_buffer>>);
static_assert(exact_expression<codec::result<void>>);
static_assert(exact_expression<codec::result<std::uint64_t>>);
static_assert(exact_expression<codec::result<fragmented_buffer>>);
static_assert(rejected_callback_result<std::uint64_t>());
static_assert(rejected_callback_result<codec::result<std::uint64_t>&>());
static_assert(rejected_callback_result<const codec::result<std::uint64_t>&>());
static_assert(rejected_callback_result<codec::result<char*>>());
static_assert(rejected_callback_result<codec::result<std::span<const char>>>());
static_assert(
  rejected_callback_result<codec::result<std::span<const char, 1>>>());
static_assert(rejected_callback_result<codec::result<std::string_view>>());
static_assert(
  rejected_callback_result<codec::result<std::reference_wrapper<int>>>());
static_assert(
  rejected_callback_result<codec::result<kwaque::bytes::fragment_view>>());
static_assert(
  rejected_callback_result<codec::result<fragmented_buffer_parser>>());
static_assert(rejected_callback_result<codec::result<throwing_move>>());
static_assert(rejected_callback_result<codec::result<throwing_destructor>>());
static_assert(
  rejected_callback_result<seastar::future<codec::result<std::uint64_t>>>());
static_assert(
  rejected_callback_result<codec::result<seastar::future<std::uint64_t>>>());

TEST(
  CodecTransactionTest,
  SuccessCommitsOnlyItsMarkAndFailurePreservesCallerMarks) {
    for (const bool success : {false, true}) {
        fragmented_buffer_parser input{split_at("abcdef", {1, 3})};
        ASSERT_TRUE(input.push_checkpoint().has_value());
        ASSERT_TRUE(input.skip(byte_count{1}).has_value());
        ASSERT_TRUE(input.push_checkpoint().has_value());
        ASSERT_TRUE(input.skip(byte_count{1}).has_value());
        const codec::error expected_error{errc::malformed_data, 17, 23, 104};
        const auto decoded = codec::with_transaction(
          input, context, [&](auto& parser) -> codec::result<std::uint16_t> {
              const auto value = codec::read_be<std::uint16_t>(parser, context);
              if (!value) {
                  return codec::failure(value.error());
              }
              return success ? value
                             : codec::result<std::uint16_t>{
                                 codec::failure(expected_error)};
          });
        if (success) {
            ASSERT_TRUE(decoded.has_value());
            EXPECT_EQ(*decoded, 0x6364U);
            EXPECT_EQ(input.bytes_consumed(), byte_count{4});
        } else {
            ASSERT_FALSE(decoded.has_value());
            EXPECT_EQ(decoded.error(), expected_error);
            EXPECT_EQ(input.bytes_consumed(), byte_count{2});
            EXPECT_EQ(unread_bytes(input), "cdef"sv);
        }
        EXPECT_EQ(input.checkpoint_depth(), 2U);
        ASSERT_TRUE(input.rollback().has_value());
        EXPECT_EQ(input.bytes_consumed(), byte_count{1});
        ASSERT_TRUE(input.rollback().has_value());
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
    }
}

struct callback_failure {
    int value;
};

TEST(CodecTransactionTest, ExceptionsRestoreTheEntryCursorAndDepth) {
    fragmented_buffer_parser input{split_at("abcdef", {2, 4})};
    ASSERT_TRUE(input.push_checkpoint().has_value());
    ASSERT_TRUE(input.skip(byte_count{1}).has_value());
    bool caught = false;
    try {
        static_cast<void>(codec::with_transaction(
          input, context, [](auto& parser) -> codec::result<void> {
              parser.skip(byte_count{3}).value();
              throw callback_failure{19};
          }));
    } catch (const callback_failure& failure) {
        EXPECT_EQ(failure.value, 19);
        caught = true;
    }
    EXPECT_TRUE(caught);
    EXPECT_EQ(input.bytes_consumed(), byte_count{1});
    EXPECT_EQ(input.checkpoint_depth(), 1U);
    EXPECT_EQ(unread_bytes(input), "bcdef"sv);
    ASSERT_TRUE(input.rollback().has_value());
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
}

TEST(
  CodecTransactionTest,
  NestedFailuresRecoverAndTheNinthMarkNeverInvokesItsCallback) {
    fragmented_buffer_parser input{split_at("abcd", {1, 2, 3})};
    for (std::size_t depth = 0; depth < 7; ++depth) {
        ASSERT_TRUE(input.push_checkpoint().has_value());
    }
    bool inner_called = false;
    std::optional<codec::error> inner_error;
    const auto decoded = codec::with_transaction(
      input, context, [&](auto& parser) -> codec::result<std::uint16_t> {
          const auto inner = codec::with_transaction(
            parser, context, [&](auto&) -> codec::result<void> {
                inner_called = true;
                return {};
            });
          if (!inner) {
              inner_error = inner.error();
          }
          return codec::read_be<std::uint16_t>(parser, context);
      });
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, 0x6162U);
    EXPECT_FALSE(inner_called);
    ASSERT_TRUE(inner_error.has_value());
    EXPECT_EQ(
      *inner_error, (codec::error{errc::resource_exhausted, 7, 11, 100}));
    EXPECT_EQ(input.checkpoint_depth(), 7U);
    EXPECT_EQ(input.bytes_consumed(), byte_count{2});
    ASSERT_TRUE(input.push_checkpoint().has_value());
    bool called = false;
    const auto full = codec::with_transaction(
      input, context, [&](auto&) -> codec::result<void> {
          called = true;
          return {};
      });
    expect_error(full, errc::resource_exhausted, 102);
    EXPECT_FALSE(called);
    EXPECT_EQ(input.checkpoint_depth(), 8U);
    EXPECT_EQ(input.bytes_consumed(), byte_count{2});
    ASSERT_TRUE(input.rollback().has_value());
    EXPECT_EQ(input.bytes_consumed(), byte_count{2});
    for (std::size_t depth = 0; depth < 7; ++depth) {
        ASSERT_TRUE(input.rollback().has_value());
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
    }
}

TEST(CodecTransactionTest, BalancedNestedTransactionsCanRollBackThenRecover) {
    fragmented_buffer_parser input{split_at("abcd", {1, 3})};
    const auto decoded = codec::with_transaction(
      input, context, [&](auto& parser) -> codec::result<std::uint16_t> {
          const auto rejected = codec::with_transaction(
            parser, context, [&](auto& nested) -> codec::result<void> {
                nested.skip(byte_count{3}).value();
                return codec::failure(
                  codec::error{errc::malformed_data, 7, 11, 102});
            });
          if (rejected) {
              return codec::failure(codec::error{errc::invariant_violation});
          }
          return codec::read_be<std::uint16_t>(parser, context);
      });
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, 0x6162U);
    EXPECT_EQ(input.bytes_consumed(), byte_count{2});
    EXPECT_EQ(input.checkpoint_depth(), 0U);
}

TEST(
  CodecTransactionTest, InvalidCoordinatesDoNotAcquireMarksOrInvokeCallbacks) {
    fragmented_buffer_parser input{split_at("ab")};
    ASSERT_TRUE(input.push_checkpoint().has_value());
    bool called = false;
    const field_context invalid{
      .origin = maximum - 1U, .family = 17, .field = 31};
    const auto decoded = codec::with_transaction(
      input, invalid, [&](auto&) -> codec::result<void> {
          called = true;
          return {};
      });
    expect_error(decoded, errc::invalid_argument, maximum - 1U, invalid);
    EXPECT_FALSE(called);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
    EXPECT_EQ(input.checkpoint_depth(), 1U);
    ASSERT_TRUE(input.rollback().has_value());
}

TEST(
  CodecSubparserTest,
  InputAdmissionChargesWholeBackingDescriptorsAndPotentialPromotions) {
    fragmented_buffer_parser input{split_at("abcdef", {2})};
    ASSERT_TRUE(input.skip(byte_count{2}).has_value());
    const auto cost = input.allocation_cost(test_charge);
    ASSERT_TRUE(cost.has_value());
    const auto metadata
      = cost->descriptors.checked_add(cost->share_controls).value();
    const auto total = cost->backing.checked_add(metadata).value();
    const decode_budget budget{
      total.checked_add(byte_count{17}).value(),
      metadata.checked_add(byte_count{13}).value(),
      test_charge};
    const auto saved = budget;
    const auto reserved = codec::reserve_decode_input(
      input, codec::limits::defaults(), budget, context);
    ASSERT_TRUE(reserved.has_value());
    EXPECT_EQ(reserved->operation_remaining, byte_count{17});
    EXPECT_EQ(reserved->metadata_remaining, byte_count{13});
    EXPECT_EQ(reserved->charge, test_charge);
    EXPECT_EQ(budget, saved);
    EXPECT_EQ(input.bytes_consumed(), byte_count{2});
    EXPECT_EQ(unread_bytes(input), "cdef"sv);

    expect_error(
      codec::reserve_decode_input(
        input,
        codec::limits::defaults(),
        {byte_count{total.value() - 1U}, metadata, test_charge},
        context),
      errc::resource_exhausted,
      102);
    expect_error(
      codec::reserve_decode_input(
        input,
        codec::limits::defaults(),
        {total, byte_count{metadata.value() - 1U}, test_charge},
        context),
      errc::resource_exhausted,
      102);
}

TEST(CodecSubparserTest, InputAdmissionRejectsBadProfilesAndLocalCaps) {
    fragmented_buffer_parser input{split_at("abcd", {2})};
    for (const auto charge : std::array<kwaque::bytes::allocation_charge_fn, 2>{
           nullptr, undercharge}) {
        auto budget = generous_budget();
        budget.charge = charge;
        expect_error(
          codec::reserve_decode_input(
            input, codec::limits::defaults(), budget, context),
          errc::invalid_argument,
          100);
    }
    auto overflowing = generous_budget();
    overflowing.charge = excessive_charge;
    expect_error(
      codec::reserve_decode_input(
        input, codec::limits::defaults(), overflowing, context),
      errc::out_of_range,
      100);
    const auto cost = input.allocation_cost(test_charge).value();
    for (unsigned mode = 0; mode < 3; ++mode) {
        codec::limits_config config;
        if (mode == 0) {
            config.max_retained_bytes = byte_count{cost.backing.value() - 1U};
        } else if (mode == 1) {
            config.max_buffer_fragments = item_count{1};
        } else {
            config.max_allocation_bytes = byte_count{
              cost.largest_allocation.value() - 1U};
        }
        const auto bounds = codec::limits::make(config).value();
        expect_error(
          codec::reserve_decode_input(
            input, bounds, generous_budget(), context),
          errc::resource_exhausted,
          100);
    }
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
    EXPECT_EQ(input.checkpoint_depth(), 0U);
}

TEST(
  CodecSubparserTest,
  ExactSuccessAtEverySplitProtectsAdjacentBytesAndCallerMarks) {
    const auto bytes = "p\x80\x01*"sv;
    for (std::size_t cut = 0; cut <= bytes.size(); ++cut) {
        fragmented_buffer_parser input{split_at(bytes, {cut})};
        ASSERT_TRUE(input.skip(byte_count{1}).has_value());
        for (std::size_t depth = 0; depth < 8; ++depth) {
            ASSERT_TRUE(input.push_checkpoint().has_value());
        }
        const auto budget = admitted(input);
        field_context seen;
        input_boundary seen_boundary = input_boundary::open;
        const auto decoded = codec::decode_exact(
          input,
          byte_count{2},
          byte_count{2},
          codec::limits::defaults(),
          budget,
          context,
          input_boundary::open,
          [&](
            auto& child,
            field_context child_context,
            input_boundary boundary,
            decode_budget) {
              seen = child_context;
              seen_boundary = boundary;
              return codec::read_varuint<std::uint32_t>(
                child, child_context, boundary);
          });
        ASSERT_TRUE(decoded.has_value());
        EXPECT_EQ(*decoded, 128U);
        EXPECT_EQ(seen.origin, 101U);
        EXPECT_EQ(seen.family, 7U);
        EXPECT_EQ(seen.field, 11U);
        EXPECT_EQ(seen_boundary, input_boundary::complete);
        EXPECT_EQ(input.checkpoint_depth(), 8U);
        EXPECT_EQ(input.bytes_consumed(), byte_count{3});
        const auto following = codec::read_le<std::uint8_t>(input);
        ASSERT_TRUE(following.has_value());
        EXPECT_EQ(*following, 42U);
        for (std::size_t depth = 0; depth < 8; ++depth) {
            ASSERT_TRUE(input.rollback().has_value());
            EXPECT_EQ(input.bytes_consumed(), byte_count{1});
        }
    }
}

TEST(
  CodecSubparserTest,
  ChildShortageAndTrailingBytesRejectWithoutRebasingDiagnostics) {
    fragmented_buffer_parser input{split_at("xy\x80\x80*"sv, {2, 3, 4})};
    ASSERT_TRUE(input.skip(byte_count{2}).has_value());
    const auto budget = admitted(input);
    const auto shortage = codec::decode_exact(
      input,
      byte_count{2},
      byte_count{2},
      codec::limits::defaults(),
      budget,
      context,
      input_boundary::open,
      [](
        auto& child,
        field_context child_context,
        input_boundary,
        decode_budget) {
          child_context.field = 55;
          // An older callback may report open-input shortage; the complete
          // structural parent translates its code, preserving absolute detail.
          return codec::read_varuint<std::uint32_t>(
            child, child_context, input_boundary::open);
      });
    expect_error(
      shortage,
      errc::malformed_data,
      104,
      {.origin = 102, .family = 7, .field = 55});
    EXPECT_EQ(input.bytes_consumed(), byte_count{2});
    const auto trailing = codec::decode_exact(
      input,
      byte_count{2},
      byte_count{2},
      codec::limits::defaults(),
      budget,
      context,
      input_boundary::open,
      [](
        auto& child,
        field_context child_context,
        input_boundary boundary,
        decode_budget) {
          return codec::read_le<std::uint8_t>(child, child_context, boundary);
      });
    expect_error(trailing, errc::malformed_data, 103);
    EXPECT_EQ(input.bytes_consumed(), byte_count{2});
    EXPECT_EQ(unread_bytes(input), "\x80\x80*"sv);
    EXPECT_EQ(input.checkpoint_depth(), 0U);
}

TEST(CodecSubparserTest, NestedChildOriginsAddOnlyEachLocalPosition) {
    for (const bool valid : {false, true}) {
        const auto bytes = valid ? "p\x01\x80\x01*"sv : "p\x01\x80\x00*"sv;
        fragmented_buffer_parser input{split_at(bytes, {1, 2, 3, 4})};
        ASSERT_TRUE(input.skip(byte_count{1}).has_value());
        const auto budget = admitted(input);
        const auto decoded = codec::decode_exact(
          input,
          byte_count{3},
          byte_count{3},
          codec::limits::defaults(),
          budget,
          context,
          input_boundary::open,
          [](
            auto& child,
            field_context child_context,
            input_boundary boundary,
            decode_budget child_budget) -> codec::result<std::uint32_t> {
              const auto first = codec::read_le<std::uint8_t>(
                child, child_context, boundary);
              if (!first) {
                  return codec::failure(first.error());
              }
              return codec::decode_exact(
                child,
                byte_count{2},
                byte_count{2},
                codec::limits::defaults(),
                child_budget,
                child_context,
                boundary,
                [](
                  auto& grandchild,
                  field_context inner_context,
                  input_boundary inner_boundary,
                  decode_budget) {
                    return codec::read_varuint<std::uint32_t>(
                      grandchild, inner_context, inner_boundary);
                });
          });
        if (valid) {
            ASSERT_TRUE(decoded.has_value());
            EXPECT_EQ(*decoded, 128U);
            EXPECT_EQ(input.bytes_consumed(), byte_count{4});
            EXPECT_EQ(unread_bytes(input), "*"sv);
        } else {
            expect_error(decoded, errc::malformed_data, 103);
            EXPECT_EQ(input.bytes_consumed(), byte_count{1});
            EXPECT_EQ(unread_bytes(input), "\x01\x80\x00*"sv);
        }
    }
}

TEST(CodecSubparserTest, DeclaredCapRejectsBeforeMissingBody) {
    for (const auto boundary :
         {input_boundary::open, input_boundary::complete}) {
        fragmented_buffer_parser input{split_at("pab"sv, {1, 2})};
        ASSERT_TRUE(input.skip(byte_count{1}).has_value());
        const auto budget = admitted(input);
        bool called = false;
        const auto decoded = codec::decode_exact(
          input,
          byte_count{5},
          byte_count{3},
          codec::limits::defaults(),
          budget,
          context,
          boundary,
          [&](auto&, field_context, input_boundary, decode_budget)
            -> codec::result<void> {
              called = true;
              return {};
          });
        expect_error(decoded, errc::resource_exhausted, 101);
        EXPECT_FALSE(called);
        EXPECT_EQ(input.bytes_consumed(), byte_count{1});
        EXPECT_EQ(input.checkpoint_depth(), 0U);
        EXPECT_EQ(unread_bytes(input), "ab"sv);
    }
}

TEST(CodecSubparserTest, MissingBodyAndInvalidContextFailBeforeCallback) {
    for (const auto boundary :
         {input_boundary::open, input_boundary::complete}) {
        fragmented_buffer_parser input{split_at("xyab")};
        ASSERT_TRUE(input.skip(byte_count{2}).has_value());
        bool called = false;
        const auto callback = [&](
                                auto&,
                                field_context,
                                input_boundary,
                                decode_budget) -> codec::result<void> {
            called = true;
            return {};
        };
        const auto budget = admitted(input);
        const auto missing = codec::decode_exact(
          input,
          byte_count{3},
          byte_count{3},
          codec::limits::defaults(),
          budget,
          context,
          boundary,
          callback);
        expect_error(
          missing,
          boundary == input_boundary::open ? errc::truncated_data
                                           : errc::malformed_data,
          104);
        const field_context invalid{
          .origin = maximum - 3U, .family = 17, .field = 31};
        expect_error(
          codec::decode_exact(
            input,
            byte_count{1},
            byte_count{1},
            codec::limits::defaults(),
            budget,
            invalid,
            boundary,
            callback),
          errc::invalid_argument,
          maximum - 3U,
          invalid);
        expect_error(
          codec::decode_exact(
            input,
            byte_count{1},
            byte_count{1},
            codec::limits::defaults(),
            budget,
            context,
            static_cast<input_boundary>(255),
            callback),
          errc::invalid_argument,
          100);
        auto no_profile = budget;
        no_profile.charge = nullptr;
        expect_error(
          codec::decode_exact(
            input,
            byte_count{1},
            byte_count{1},
            codec::limits::defaults(),
            no_profile,
            context,
            boundary,
            callback),
          errc::invalid_argument,
          102);
        EXPECT_FALSE(called);
        EXPECT_EQ(input.bytes_consumed(), byte_count{2});
        EXPECT_EQ(unread_bytes(input), "ab"sv);
    }
}

TEST(CodecSubparserTest, EmptyChildNeedsNoSharingBudgetAndIsNotAnAbsentValue) {
    fragmented_buffer_parser input{split_at("p*")};
    ASSERT_TRUE(input.skip(byte_count{1}).has_value());
    const auto reserved_parent = admitted(input);
    static_cast<void>(reserved_parent);
    const decode_budget no_remaining{byte_count{}, byte_count{}, test_charge};
    bool called = false;
    decode_budget observed;
    const auto decoded = codec::decode_exact(
      input,
      byte_count{},
      byte_count{},
      codec::limits::defaults(),
      no_remaining,
      context,
      input_boundary::open,
      [&](auto&, field_context, input_boundary, decode_budget child_budget)
        -> codec::result<std::uint32_t> {
          called = true;
          observed = child_budget;
          return 19U;
      });
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, 19U);
    EXPECT_TRUE(called);
    EXPECT_EQ(observed, no_remaining);
    EXPECT_EQ(input.bytes_consumed(), byte_count{1});
    const auto shortage = codec::decode_exact(
      input,
      byte_count{},
      byte_count{},
      codec::limits::defaults(),
      no_remaining,
      context,
      input_boundary::open,
      [](
        auto& child,
        field_context child_context,
        input_boundary boundary,
        decode_budget) {
          return codec::read_le<std::uint8_t>(child, child_context, boundary);
      });
    expect_error(shortage, errc::malformed_data, 101);
    EXPECT_EQ(unread_bytes(input), "*"sv);
}

TEST(
  CodecSubparserTest,
  ChildDescriptorsConsumeBothResidualsWithoutRechargingBacking) {
    fragmented_buffer_parser input{split_at("abcd", {2})};
    auto budget = admitted(input);
    const auto cost
      = input.next_buffer_allocation_cost(byte_count{4}, budget.charge).value();
    budget.operation_remaining = cost.descriptors;
    budget.metadata_remaining = cost.descriptors;
    const auto saved = budget;
    decode_budget observed;
    const auto decoded = codec::decode_exact(
      input,
      byte_count{4},
      byte_count{4},
      codec::limits::defaults(),
      budget,
      context,
      input_boundary::open,
      [&](
        auto& child, field_context, input_boundary, decode_budget child_budget)
        -> codec::result<void> {
          observed = child_budget;
          child.skip(byte_count{4}).value();
          return {};
      });
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(observed.operation_remaining, byte_count{});
    EXPECT_EQ(observed.metadata_remaining, byte_count{});
    EXPECT_EQ(observed.charge, test_charge);
    EXPECT_EQ(budget, saved);
    EXPECT_TRUE(input.at_end());
}

TEST(
  CodecSubparserTest, ChildCapsAndResidualFailuresDoNotShareOrInvokeCallbacks) {
    for (unsigned mode = 0; mode < 8; ++mode) {
        fragmented_buffer_parser input{split_at("abcd", {2})};
        auto budget = admitted(input);
        const auto cost
          = input.next_buffer_allocation_cost(byte_count{4}, test_charge)
              .value();
        codec::limits_config config;
        byte_count child_cap{4};
        if (mode == 0) {
            child_cap = byte_count{3};
        } else if (mode == 1) {
            budget.operation_remaining = byte_count{
              cost.descriptors.value() - 1U};
        } else if (mode == 2) {
            budget.metadata_remaining = byte_count{
              cost.descriptors.value() - 1U};
        } else if (mode == 3) {
            config.max_buffer_fragments = item_count{1};
        } else if (mode == 4) {
            config.max_retained_bytes = byte_count{cost.backing.value() - 1U};
        } else if (mode == 5) {
            config.max_allocation_bytes = byte_count{
              cost.largest_allocation.value() - 1U};
        } else if (mode == 6) {
            config.max_metadata_bytes = byte_count{
              cost.descriptors.value() - 1U};
        } else {
            config.max_operation_bytes = byte_count{
              cost.descriptors.value() - 1U};
        }
        const auto bounds = codec::limits::make(config).value();
        const auto saved = budget;
        bool called = false;
        const auto callback = [&](
                                auto&,
                                field_context,
                                input_boundary,
                                decode_budget) -> codec::result<void> {
            called = true;
            return {};
        };
        // Isolate sharing admission from first-use standard error registration.
        static_cast<void>(kwaque::make_error_code(errc::resource_exhausted));
        std::optional<codec::result<void>> decoded;
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
        auto& injector = seastar::memory::local_failure_injector();
        const auto allocations = injector.alloc_count();
        injector.fail_after(0);
        try {
            decoded.emplace(
              codec::decode_exact(
                input,
                byte_count{4},
                child_cap,
                bounds,
                budget,
                context,
                input_boundary::open,
                callback));
        } catch (...) {
            injector.cancel();
            throw;
        }
        const bool injected = injector.failed();
        const auto allocations_after = injector.alloc_count();
        injector.cancel();
        EXPECT_FALSE(injected);
        EXPECT_EQ(allocations_after, allocations);
#else
        decoded.emplace(
          codec::decode_exact(
            input,
            byte_count{4},
            child_cap,
            bounds,
            budget,
            context,
            input_boundary::open,
            callback));
#endif
        ASSERT_TRUE(decoded.has_value());
        expect_error(*decoded, errc::resource_exhausted, 100);
        EXPECT_FALSE(called);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
        EXPECT_EQ(input.checkpoint_depth(), 0U);
        EXPECT_EQ(unread_bytes(input), "abcd"sv);
        EXPECT_EQ(budget, saved);
    }
}

TEST(
  CodecSubparserTest,
  ExceptionAfterSharingPreservesParentAndItsReservedBudget) {
    fragmented_buffer_parser input{split_at("pabcd*", {1, 3, 5})};
    ASSERT_TRUE(input.skip(byte_count{1}).has_value());
    ASSERT_TRUE(input.push_checkpoint().has_value());
    const auto budget = admitted(input);
    const auto saved = budget;
    bool caught = false;
    try {
        static_cast<void>(codec::decode_exact(
          input,
          byte_count{4},
          byte_count{4},
          codec::limits::defaults(),
          budget,
          context,
          input_boundary::open,
          [](auto& child, field_context, input_boundary, decode_budget)
            -> codec::result<void> {
              child.skip(byte_count{2}).value();
              throw callback_failure{23};
          }));
    } catch (const callback_failure& failure) {
        EXPECT_EQ(failure.value, 23);
        caught = true;
    }
    EXPECT_TRUE(caught);
    EXPECT_EQ(input.bytes_consumed(), byte_count{1});
    EXPECT_EQ(input.checkpoint_depth(), 1U);
    EXPECT_EQ(unread_bytes(input), "abcd*"sv);
    EXPECT_EQ(budget, saved);
    ASSERT_TRUE(input.rollback().has_value());
    EXPECT_EQ(input.bytes_consumed(), byte_count{1});
}

TEST(CodecSubparserTest, OwningDecodedBytesOutliveEveryParser) {
    std::optional<fragmented_buffer> retained;
    {
        fragmented_buffer_parser input{split_at("powned*", {1, 3, 6})};
        ASSERT_TRUE(input.skip(byte_count{1}).has_value());
        auto budget = admitted(input);
        const auto result_cost
          = input.next_buffer_allocation_cost(byte_count{5}, test_charge)
              .value();
        // This callback makes its own owning slice. Reserve its extra
        // descriptor charge outside decode_exact, and keep that reservation
        // with the result.
        budget.operation_remaining = budget.operation_remaining
                                       .checked_sub(result_cost.descriptors)
                                       .value();
        budget.metadata_remaining = budget.metadata_remaining
                                      .checked_sub(result_cost.descriptors)
                                      .value();
        auto decoded = codec::decode_exact(
          input,
          byte_count{5},
          byte_count{5},
          codec::limits::defaults(),
          budget,
          context,
          input_boundary::open,
          [](
            auto& child,
            field_context child_context,
            input_boundary,
            decode_budget) -> codec::result<fragmented_buffer> {
              auto bytes = child.read_buffer(child.bytes_remaining());
              if (!bytes) {
                  return codec::failure(
                    codec::error{
                      errc::malformed_data,
                      child_context.family,
                      child_context.field,
                      child_context.origin});
              }
              return std::move(*bytes);
          });
        ASSERT_TRUE(decoded.has_value());
        retained.emplace(std::move(*decoded));
        EXPECT_EQ(input.bytes_consumed(), byte_count{6});
        EXPECT_EQ(unread_bytes(input), "*"sv);
    }
    ASSERT_TRUE(retained.has_value());
    EXPECT_TRUE(retained->content_equals("owned"sv));
}

TEST(
  CodecSubparserTest, SharingAllocationFailuresPreserveCursorAndCallerMarks) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation failure injection is not enabled";
#else
    std::size_t failures = 0;
    bool completed = false;
    for (std::uint64_t fail_after = 0; fail_after < 32; ++fail_after) {
        fragmented_buffer_parser input{
          split_at("pabcdef*", {1, 2, 3, 4, 5, 6, 7})};
        ASSERT_TRUE(input.skip(byte_count{1}).has_value());
        ASSERT_TRUE(input.push_checkpoint().has_value());
        const auto budget = admitted(input);
        const auto saved = budget;
        bool called = false;
        std::optional<codec::result<void>> decoded;
        bool threw = false;
        auto& injector = seastar::memory::local_failure_injector();
        injector.fail_after(fail_after);
        try {
            decoded.emplace(
              codec::decode_exact(
                input,
                byte_count{6},
                byte_count{6},
                codec::limits::defaults(),
                budget,
                context,
                input_boundary::open,
                [&](auto& child, field_context, input_boundary, decode_budget)
                  -> codec::result<void> {
                    called = true;
                    child.skip(byte_count{6}).value();
                    return {};
                }));
        } catch (const std::bad_alloc&) {
            threw = true;
        } catch (...) {
            injector.cancel();
            throw;
        }
        const bool injected = injector.failed();
        injector.cancel();
        if (injected) {
            ++failures;
            EXPECT_TRUE(threw);
            EXPECT_FALSE(called);
            EXPECT_FALSE(decoded.has_value());
            EXPECT_EQ(input.bytes_consumed(), byte_count{1});
            EXPECT_EQ(unread_bytes(input), "abcdef*"sv);
        } else {
            EXPECT_FALSE(threw);
            EXPECT_TRUE(called);
            ASSERT_TRUE(decoded.has_value());
            ASSERT_TRUE(decoded->has_value());
            EXPECT_EQ(input.bytes_consumed(), byte_count{7});
            EXPECT_EQ(unread_bytes(input), "*"sv);
            completed = true;
        }
        // Counted ownership may persist after an injected partial share; its
        // possible promotion cost remains reserved with this parent.
        EXPECT_EQ(budget, saved);
        EXPECT_EQ(input.checkpoint_depth(), 1U);
        ASSERT_TRUE(input.rollback().has_value());
        EXPECT_EQ(input.bytes_consumed(), byte_count{1});
        if (completed) {
            break;
        }
    }
    EXPECT_GT(failures, 0U);
    EXPECT_TRUE(completed);
#endif
}

} // namespace
