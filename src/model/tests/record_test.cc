#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/codec/cooperative.h"
#include "src/codec/limits.h"
#include "src/model/position.h"
#include "src/model/record.h"
#include "src/model/record_codec.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <malloc.h>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

using namespace std::literals;
namespace codec = kwaque::codec;
namespace model = kwaque::model;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::item_count;
using kwaque::bytes::fragmented_buffer;
using nullable_bytes = std::optional<fragmented_buffer>;

static_assert(!std::default_initializable<model::record>);
static_assert(!std::default_initializable<model::record_header>);
static_assert(!std::copy_constructible<model::record>);
static_assert(!std::copy_constructible<model::record_header>);
static_assert(std::is_nothrow_move_constructible_v<model::record>);
static_assert(std::is_nothrow_move_assignable_v<model::record>);
static_assert(std::is_nothrow_destructible_v<model::record>);
static_assert(std::is_nothrow_move_constructible_v<model::record_header>);
static_assert(std::is_nothrow_destructible_v<model::record_header>);
static_assert(
  64U * sizeof(model::record_header)
  <= kwaque::maximum_contiguous_allocation_bytes);
static_assert(!std::constructible_from<
              model::range_logical_count,
              model::segment_record_count>);

template<typename T>
concept borrowed_key = requires(T&& value) { std::forward<T>(value).key(); };
template<typename T>
concept borrowed_headers = requires(T&& value) {
    std::forward<T>(value).headers();
};
template<typename T>
concept borrowed_name = requires(T&& value) { std::forward<T>(value).name(); };
template<typename T>
concept extracting_key = requires(T&& value) {
    std::forward<T>(value).release_key();
};
template<typename T>
concept allocation_argument = requires(
  T&& value, codec::cooperative_work& work) {
    model::record_allocation_cost(std::forward<T>(value), work, nullptr);
};
template<typename L, typename R>
concept equality_arguments = requires(
  L&& left, R&& right, codec::cooperative_work& work) {
    model::records_equal(std::forward<L>(left), std::forward<R>(right), work);
};

static_assert(borrowed_key<const model::record&>);
static_assert(!borrowed_key<model::record>);
static_assert(!borrowed_headers<model::record>);
static_assert(borrowed_name<const model::record_header&>);
static_assert(!borrowed_name<model::record_header>);
static_assert(extracting_key<model::record>);
static_assert(!extracting_key<model::record&>);
static_assert(allocation_argument<const model::record&>);
static_assert(!allocation_argument<model::record>);
static_assert(equality_arguments<const model::record&, const model::record&>);
static_assert(!equality_arguments<model::record, const model::record&>);
static_assert(!equality_arguments<const model::record&, model::record>);

constexpr codec::error anchor{errc::success, 2, 91, 703};

byte_count charge(byte_count request) noexcept {
    if (request.value() == 0) {
        return {};
    }
    if (request.value() > (std::uint64_t{1} << 62U)) {
        return byte_count{std::numeric_limits<std::uint64_t>::max()};
    }
    return byte_count{
      2U * std::bit_ceil(std::max(request.value(), std::uint64_t{16}))};
}

codec::decode_budget memory() {
    // The other half remains unavailable for fixture/native/frame/opaque costs.
    return {byte_count{32U * 1024U * 1024U}, byte_count{1024U * 1024U}, charge};
}

fragmented_buffer text(std::string_view value) {
    return fragmented_buffer::copy_of(value).value();
}

fragmented_buffer payload(std::size_t length, std::size_t width = 65536) {
    std::vector<seastar::temporary_buffer<char>> fragments;
    fragments.reserve(length == 0 ? 0 : 1U + (length - 1U) / width);
    for (std::size_t offset = 0; offset < length; offset += width) {
        seastar::temporary_buffer<char> part{std::min(width, length - offset)};
        std::fill_n(part.get_write(), part.size(), 'x');
        fragments.push_back(std::move(part));
    }
    return fragmented_buffer::copy_from_fragments(fragments).value();
}

model::record_header
header(std::string_view name, std::optional<std::string_view> value) {
    return model::make_record_header(
             text(name), value ? nullable_bytes{text(*value)} : std::nullopt)
      .value();
}

model::record make(
  nullable_bytes key = std::nullopt,
  nullable_bytes value = std::nullopt,
  std::vector<model::record_header> headers = {},
  model::record_fields fields = {}) {
    return model::make_record(
             fields, std::move(key), std::move(value), std::move(headers))
      .value();
}

bool equal(const model::record& left, const model::record& right) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    return model::records_equal(left, right, work).get().value();
}

model::record_memory_usage cost(const model::record& value) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    return model::record_allocation_cost(value, work, charge).get().value();
}

void expect_base_error(const auto& value, errc reason) {
    ASSERT_FALSE(value.has_value());
    EXPECT_EQ(value.error(), reason);
}

void expect_codec_error(const auto& value, errc reason) {
    ASSERT_FALSE(value.has_value());
    EXPECT_EQ(
      value.error(),
      (codec::error{
        reason, anchor.family(), anchor.field(), anchor.byte_offset()}));
}

TEST(RecordTest, NullablePresenceAndTombstonesHaveDistinctSemantics) {
    for (unsigned key_kind = 0; key_kind != 3; ++key_kind) {
        for (unsigned value_kind = 0; value_kind != 3; ++value_kind) {
            const auto bytes = [](unsigned kind) -> nullable_bytes {
                if (kind == 0) {
                    return std::nullopt;
                }
                return text(kind == 1 ? ""sv : "abc"sv);
            };
            auto value = make(bytes(key_kind), bytes(value_kind));
            EXPECT_EQ(value.has_key(), key_kind != 0);
            EXPECT_EQ(value.has_value(), value_kind != 0);
            EXPECT_EQ(value.is_tombstone(), value_kind == 0);
            if (value.key()) {
                EXPECT_TRUE(
                  value.key()->content_equals(key_kind == 1 ? ""sv : "abc"sv));
            }
            if (value.value()) {
                EXPECT_TRUE(value.value()->content_equals(
                  value_kind == 1 ? ""sv : "abc"sv));
            }
            auto same = make(bytes(key_kind), bytes(value_kind));
            EXPECT_TRUE(equal(value, same));
        }
    }
    auto absent = make();
    auto empty_key = make(fragmented_buffer{});
    auto empty_value = make(std::nullopt, fragmented_buffer{});
    EXPECT_FALSE(equal(absent, empty_key));
    EXPECT_FALSE(equal(absent, empty_value));
    EXPECT_FALSE(equal(empty_key, empty_value));
}

TEST(RecordTest, OrderedDuplicateAndBinaryHeadersSurviveDonorDestruction) {
    std::optional<model::record> owned;
    {
        std::vector<model::record_header> headers;
        headers.push_back(header(""sv, std::nullopt));
        headers.push_back(header(""sv, ""sv));
        headers.push_back(header("\xff\0\x80"sv, "\0\xfe"sv));
        auto key = nullable_bytes{text("key"sv)};
        const char* address = key->fragment_at(0)->data();
        auto value = model::make_record(
          {.timestamp_delta = -2,
           .logical_delta = model::range_logical_count{3}},
          std::move(key),
          nullable_bytes{text("value"sv)},
          std::move(headers));
        ASSERT_TRUE(value.has_value());
        EXPECT_EQ(value->key()->fragment_at(0)->data(), address);
        owned.emplace(std::move(*value));
    }
    ASSERT_TRUE(owned.has_value());
    EXPECT_EQ(owned->timestamp_delta(), -2);
    EXPECT_EQ(owned->logical_delta(), model::range_logical_count{3});
    EXPECT_EQ(owned->attributes(), 0);
    ASSERT_EQ(owned->headers().size(), 3U);
    EXPECT_TRUE(owned->headers()[0].name().empty());
    EXPECT_FALSE(owned->headers()[0].value().has_value());
    ASSERT_TRUE(owned->headers()[1].value().has_value());
    EXPECT_TRUE(owned->headers()[1].value()->empty());
    EXPECT_TRUE(owned->headers()[2].name().content_equals("\xff\0\x80"sv));
    EXPECT_TRUE(owned->headers()[2].value()->content_equals("\0\xfe"sv));
    EXPECT_TRUE(owned->key()->content_equals("key"sv));
    EXPECT_TRUE(owned->value()->content_equals("value"sv));
}

TEST(RecordTest, EqualityIncludesRelativeFieldsAndEveryOrderedHeader) {
    const auto sample = [](unsigned change) {
        std::vector<model::record_header> headers;
        headers.push_back(header("same"sv, std::nullopt));
        headers.push_back(header(
          change == 3 ? "other"sv : "same"sv,
          change == 4 ? std::nullopt : std::optional{""sv}));
        if (change == 5) {
            std::swap(headers[0], headers[1]);
        }
        return make(
          text("key"sv),
          text(change == 6 ? "VALUE"sv : "value"sv),
          std::move(headers),
          {.timestamp_delta = change == 1 ? -1 : 0,
           .logical_delta = model::range_logical_count{change == 2 ? 2U : 1U}});
    };
    auto original = sample(0);
    for (unsigned change = 0; change <= 6; ++change) {
        auto other = sample(change);
        EXPECT_EQ(equal(original, other), change == 0) << change;
    }
}

TEST(RecordTest, ExplicitExtractionTransfersOwnersWithoutCopying) {
    std::vector<model::record_header> headers;
    headers.push_back(header("name"sv, "header"sv));
    auto value = make(text("key"sv), text("value"sv), std::move(headers));
    ASSERT_TRUE(value.key() && value.value());
    ASSERT_EQ(value.headers().size(), 1U);
    const auto* key_address = value.key()->fragment_at(0)->data();
    const auto* value_address = value.value()->fragment_at(0)->data();
    const auto* header_address = value.headers().data();
    const auto before = seastar::memory::stats().mallocs();
    // Each extraction clears only its named component; the record remains
    // available for the other extractions and the cleared-state assertions.
    // NOLINTBEGIN(bugprone-use-after-move)
    auto key = std::move(value).release_key();
    auto data = std::move(value).release_value();
    auto extracted_headers = std::move(value).release_headers();
    const auto after = seastar::memory::stats().mallocs();
    EXPECT_EQ(before, after);
    ASSERT_TRUE(key && data);
    EXPECT_EQ(key->fragment_at(0)->data(), key_address);
    EXPECT_EQ(data->fragment_at(0)->data(), value_address);
    EXPECT_EQ(extracted_headers.data(), header_address);
    ASSERT_EQ(extracted_headers.size(), 1U);
    EXPECT_FALSE(value.has_key());
    EXPECT_FALSE(value.has_value());
    EXPECT_TRUE(value.headers().empty());
    EXPECT_EQ(
      model::record_encoded_size(value).value().encoded_bytes, byte_count{7});
    auto name = std::move(extracted_headers[0]).release_name();
    auto header_value = std::move(extracted_headers[0]).release_value();
    // NOLINTEND(bugprone-use-after-move)
    extracted_headers.clear();
    EXPECT_TRUE(name.content_equals("name"sv));
    ASSERT_TRUE(header_value.has_value());
    EXPECT_TRUE(header_value->content_equals("header"sv));
}

TEST(RecordTest, EveryNonzeroAttributeRejectsBeforeOwnershipTransfer) {
    for (unsigned attribute = 1; attribute <= 255; ++attribute) {
        nullable_bytes key{text("key"sv)};
        nullable_bytes value{text("value"sv)};
        std::vector<model::record_header> headers;
        headers.push_back(header("name"sv, "value"sv));
        auto rejected = model::make_record(
          {.attributes = static_cast<std::uint8_t>(attribute)},
          std::move(key),
          std::move(value),
          std::move(headers));
        expect_base_error(rejected, errc::unsupported_format);
        // NOLINTBEGIN(bugprone-use-after-move)
        ASSERT_TRUE(key && value);
        EXPECT_TRUE(key->content_equals("key"sv));
        EXPECT_TRUE(value->content_equals("value"sv));
        ASSERT_EQ(headers.size(), 1U);
        EXPECT_TRUE(headers[0].name().content_equals("name"sv));
        // NOLINTEND(bugprone-use-after-move)
    }
}

TEST(RecordTest, SameDonorObjectsRejectButIndependentSharedBackingIsValid) {
    nullable_bytes donor{text("payload"sv)};
    expect_base_error(
      model::make_record({}, std::move(donor), std::move(donor), {}),
      errc::invalid_argument);
    // NOLINTNEXTLINE(bugprone-use-after-move)
    EXPECT_TRUE(donor->content_equals("payload"sv));
    nullable_bytes header_donor{text("payload"sv)};
    expect_base_error(
      model::make_record_header(
        std::move(*header_donor), std::move(header_donor)),
      errc::invalid_argument);
    // NOLINTNEXTLINE(bugprone-use-after-move)
    EXPECT_TRUE(header_donor->content_equals("payload"sv));
    auto backing = text("payload"sv);
    auto key = nullable_bytes{backing.share()};
    auto other = nullable_bytes{backing.share()};
    auto value = model::make_record({}, std::move(key), std::move(other), {});
    ASSERT_TRUE(value.has_value());
    EXPECT_TRUE(value->key()->content_equals(*value->value()));
    EXPECT_EQ(
      cost(*value).backing,
      charge(byte_count{7}).checked_add(charge(byte_count{7})).value());
}

TEST(RecordTest, IndependentRecordSizeFactsUseUnsignedAndNullableWidths) {
    struct sample {
        unsigned key;
        unsigned value;
        std::uint64_t body;
        std::uint64_t total;
    };
    for (const auto input : std::array{
           sample{0, 0, 6, 7},
           sample{3, 5, 14, 15},
           sample{0, 5, 11, 12},
           sample{3, 0, 9, 10},
           sample{63, 0, 69, 70},
           sample{64, 0, 71, 72},
           sample{0, 120, 127, 128},
           sample{0, 121, 128, 130}}) {
        auto value = make(
          input.key == 0 ? std::nullopt : nullable_bytes{payload(input.key)},
          input.value == 0 ? std::nullopt
                           : nullable_bytes{payload(input.value)});
        const auto size = model::record_encoded_size(value);
        ASSERT_TRUE(size.has_value());
        EXPECT_EQ(size->body_bytes, byte_count{input.body});
        EXPECT_EQ(size->encoded_bytes, byte_count{input.total});
        EXPECT_EQ(size->header_payload_bytes, byte_count{});
    }
    // This independently specified record has a six-byte body and one-byte
    // unsigned length prefix, unlike a signed-ZigZag record-size prefix.
    constexpr std::array<unsigned char, 7> literal{6, 0, 0, 0, 1, 1, 0};
    EXPECT_EQ(literal[0], literal.size() - 1U);
    auto null_record = make();
    EXPECT_EQ(
      model::record_encoded_size(null_record).value().encoded_bytes,
      byte_count{literal.size()});
}

TEST(RecordTest, FullWidthRelativeScalarsHaveCheckedIndependentSizes) {
    for (const auto [delta, width] :
         std::array<std::pair<std::int64_t, std::uint64_t>, 8>{
           {{0, 1},
            {-1, 1},
            {-64, 1},
            {63, 1},
            {-65, 2},
            {64, 2},
            {std::numeric_limits<std::int64_t>::min(), 10},
            {std::numeric_limits<std::int64_t>::max(), 10}}}) {
        for (const auto [logical, logical_width] :
             std::array<std::pair<std::uint64_t, std::uint64_t>, 20>{
               {{0, 1},
                {127, 1},
                {128, 2},
                {16'383, 2},
                {16'384, 3},
                {2'097'151, 3},
                {2'097'152, 4},
                {268'435'455, 4},
                {268'435'456, 5},
                {34'359'738'367, 5},
                {34'359'738'368, 6},
                {4'398'046'511'103, 6},
                {4'398'046'511'104, 7},
                {562'949'953'421'311, 7},
                {562'949'953'421'312, 8},
                {72'057'594'037'927'935, 8},
                {72'057'594'037'927'936, 9},
                {9'223'372'036'854'775'807ULL, 9},
                {9'223'372'036'854'775'808ULL, 10},
                {std::numeric_limits<std::uint64_t>::max(), 10}}}) {
            auto value = make(
              std::nullopt,
              std::nullopt,
              {},
              {.timestamp_delta = delta,
               .logical_delta = model::range_logical_count{logical}});
            const auto size = model::record_encoded_size(value).value();
            EXPECT_EQ(size.body_bytes, byte_count{4U + width + logical_width});
            EXPECT_EQ(
              size.encoded_bytes, byte_count{5U + width + logical_width});
        }
    }
}

TEST(RecordTest, HeaderFramingCountsAndPayloadHaveSeparateSizes) {
    for (const auto [name_bytes, body, total] :
         std::array<std::array<std::uint64_t, 3>, 2>{
           {{127, 135, 137}, {128, 137, 139}}}) {
        std::vector<model::record_header> headers;
        headers.push_back(
          model::make_record_header(payload(name_bytes), std::nullopt).value());
        auto value = make(std::nullopt, std::nullopt, std::move(headers));
        auto size = model::record_encoded_size(value).value();
        EXPECT_EQ(size.body_bytes, byte_count{body});
        EXPECT_EQ(size.encoded_bytes, byte_count{total});
        EXPECT_EQ(size.header_payload_bytes, byte_count{name_bytes});
    }
    std::vector<model::record_header> headers;
    for (unsigned index = 0; index < 64; ++index) {
        headers.push_back(header(""sv, std::nullopt));
    }
    auto value = make(std::nullopt, std::nullopt, std::move(headers));
    const auto size = model::record_encoded_size(value).value();
    EXPECT_EQ(size.body_bytes, byte_count{134});
    EXPECT_EQ(size.encoded_bytes, byte_count{136});
    EXPECT_EQ(size.header_payload_bytes, byte_count{});
}

TEST(RecordTest, RecordCapIncludesItsOwnPrefixAtMaximumAndNarrowBounds) {
    for (const auto [limit, bytes] :
         std::array<std::pair<std::uint64_t, std::size_t>, 2>{
           {{1048576, 1048565}, {64, 57}}}) {
        for (const bool oversized : {false, true}) {
            codec::limits_config config;
            config.max_record_bytes = byte_count{limit};
            nullable_bytes donor{payload(bytes + (oversized ? 1U : 0U))};
            auto value = model::make_record(
              {},
              std::nullopt,
              std::move(donor),
              {},
              codec::limits::make(config).value());
            if (oversized) {
                expect_base_error(value, errc::resource_exhausted);
                // NOLINTNEXTLINE(bugprone-use-after-move)
                EXPECT_EQ(donor->size(), byte_count{bytes + 1U});
            } else {
                ASSERT_TRUE(value.has_value());
                EXPECT_EQ(
                  model::record_encoded_size(*value).value().encoded_bytes,
                  byte_count{limit});
            }
        }
    }
    codec::limits_config config;
    config.max_record_bytes = byte_count{6};
    expect_base_error(
      model::make_record(
        {},
        std::nullopt,
        std::nullopt,
        {},
        codec::limits::make(config).value()),
      errc::resource_exhausted);
}

TEST(RecordTest, HeaderNamePayloadAndAggregateCapsRejectIndependently) {
    for (const bool oversized : {false, true}) {
        auto name = payload(4096U + (oversized ? 1U : 0U));
        auto result = model::make_record_header(std::move(name), std::nullopt);
        EXPECT_EQ(result.has_value(), !oversized);
        if (oversized) {
            expect_base_error(result, errc::resource_exhausted);
        }
        auto combined = model::make_record_header(
          payload(4096),
          nullable_bytes{payload(61440U + (oversized ? 1U : 0U))});
        EXPECT_EQ(combined.has_value(), !oversized);
        if (oversized) {
            expect_base_error(combined, errc::resource_exhausted);
        }
        std::vector<model::record_header> headers;
        headers.push_back(
          model::make_record_header(
            fragmented_buffer{}, nullable_bytes{payload(40000)})
            .value());
        headers.push_back(
          model::make_record_header(
            fragmented_buffer{},
            nullable_bytes{payload(25536U + (oversized ? 1U : 0U))})
            .value());
        auto value = model::make_record(
          {}, std::nullopt, std::nullopt, std::move(headers));
        EXPECT_EQ(value.has_value(), !oversized);
        if (value) {
            EXPECT_EQ(
              model::record_encoded_size(*value).value().header_payload_bytes,
              byte_count{65536});
        } else {
            expect_base_error(value, errc::resource_exhausted);
        }
    }
}

TEST(RecordTest, HeaderCountAndNarrowedHeaderPolicyPreserveDonors) {
    std::vector<model::record_header> headers;
    for (unsigned i = 0; i < 65; ++i) {
        headers.push_back(header(""sv, std::nullopt));
    }
    expect_base_error(
      model::make_record({}, std::nullopt, std::nullopt, std::move(headers)),
      errc::resource_exhausted);
    // NOLINTNEXTLINE(bugprone-use-after-move)
    EXPECT_EQ(headers.size(), 65U);
    for (unsigned choice = 0; choice < 3; ++choice) {
        codec::limits_config config;
        if (choice == 0) config.max_record_headers = item_count{1};
        if (choice == 1) config.max_header_name_bytes = byte_count{1};
        if (choice == 2) config.max_record_header_bytes = byte_count{3};
        std::vector<model::record_header> two;
        two.push_back(header("ab"sv, "cd"sv));
        two.push_back(header("ab"sv, "cd"sv));
        expect_base_error(
          model::make_record(
            {},
            std::nullopt,
            std::nullopt,
            std::move(two),
            codec::limits::make(config).value()),
          errc::resource_exhausted);
        // NOLINTNEXTLINE(bugprone-use-after-move)
        EXPECT_EQ(two.size(), 2U);
    }
}

TEST(RecordTest, CompleteOwnerFragmentCountIsBoundedAcrossFields) {
    for (const bool excess : {false, true}) {
        nullable_bytes key{payload(512, 1)};
        nullable_bytes value{payload(excess ? 513 : 512, 1)};
        auto built = model::make_record(
          {}, std::move(key), std::move(value), {});
        EXPECT_EQ(built.has_value(), !excess);
        if (!built) {
            expect_base_error(built, errc::resource_exhausted);
        } else {
            EXPECT_EQ(cost(*built).fragments, item_count{1024});
        }
    }
}

TEST(RecordTest, ConstructionAndSizeQueriesAllocateAndShareNothing) {
    nullable_bytes key{text("key"sv)};
    nullable_bytes value{text("value"sv)};
    std::vector<model::record_header> headers;
    headers.push_back(header("name"sv, "header"sv));
    const auto* address = key->fragment_at(0)->data();
    std::optional<kwaque::result<model::record>> built;
    auto& injector = seastar::memory::local_failure_injector();
    injector.fail_after(0);
    auto cancel = seastar::defer([&injector] noexcept { injector.cancel(); });
    const auto before = seastar::memory::stats().mallocs();
    built.emplace(
      model::make_record(
        {}, std::move(key), std::move(value), std::move(headers)));
    const auto size = model::record_encoded_size(built->value());
    const auto after = seastar::memory::stats().mallocs();
    const bool injected = injector.failed();
    injector.cancel();
    ASSERT_TRUE(built->has_value());
    ASSERT_TRUE(size.has_value());
    EXPECT_EQ(before, after);
    EXPECT_FALSE(injected);
    EXPECT_EQ(built->value().key()->fragment_at(0)->data(), address);
}

TEST(RecordTest, AllocationAccountingIncludesUnusedHeaderCapacity) {
    std::vector<model::record_header> reserved;
    reserved.reserve(64);
    reserved.push_back(header(""sv, std::nullopt));
    const auto capacity = reserved.capacity();
    auto wide = make(std::nullopt, std::nullopt, std::move(reserved));
    std::vector<model::record_header> exact;
    exact.push_back(header(""sv, std::nullopt));
    auto narrow = make(std::nullopt, std::nullopt, std::move(exact));
    const auto usage = cost(wide);
    EXPECT_EQ(wide.header_capacity(), item_count{capacity});
    EXPECT_EQ(usage.backing, byte_count{});
    EXPECT_EQ(
      usage.metadata,
      charge(byte_count{capacity * sizeof(model::record_header)}));
    EXPECT_EQ(usage.largest_allocation, usage.metadata);
    EXPECT_GT(usage.metadata, cost(narrow).metadata);
    EXPECT_TRUE(equal(wide, narrow));
    RecordProperty(
      "record_inline_bytes", std::to_string(sizeof(model::record)));
    RecordProperty(
      "record_header_inline_bytes",
      std::to_string(sizeof(model::record_header)));
#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
    // This vector starts at its allocator base; no payload slice is passed to
    // the native usable-size API. Inline record/frame storage is separate.
    const auto served = ::malloc_usable_size(
      const_cast<model::record_header*>(wide.headers().data()));
    EXPECT_GE(served, capacity * sizeof(model::record_header));
    EXPECT_LE(served, usage.metadata.value());
#endif
}

TEST(RecordTest, EmptyHeaderVectorStillOwnsItsReservedCapacity) {
    std::vector<model::record_header> headers;
    headers.reserve(8);
    const auto capacity = headers.capacity();
    auto value = make(std::nullopt, std::nullopt, std::move(headers));
    EXPECT_TRUE(value.headers().empty());
    EXPECT_EQ(
      model::record_encoded_size(value).value().encoded_bytes, byte_count{7});
    EXPECT_EQ(
      cost(value).metadata,
      charge(byte_count{capacity * sizeof(model::record_header)}));
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    expect_codec_error(
      model::reserve_record_input(
        value, work, {byte_count{}, byte_count{}, charge}, anchor)
        .get(),
      errc::resource_exhausted);
}

TEST(RecordTest, SmallVisibleValueRetainsBackingAndDescriptorHistoryCharges) {
    auto backing = payload(65536);
    ASSERT_TRUE(backing.trim_front(byte_count{65535}).has_value());
    const auto underlying = backing.allocation_cost(charge).value();
    auto value = make(std::nullopt, std::move(backing));
    const auto usage = cost(value);
    EXPECT_EQ(usage.backing, underlying.backing);
    EXPECT_EQ(
      usage.metadata,
      underlying.descriptors.checked_add(underlying.share_controls).value());
    EXPECT_EQ(usage.largest_allocation, underlying.largest_allocation);
    EXPECT_EQ(usage.fragments, item_count{1});
    EXPECT_EQ(
      model::record_encoded_size(value).value().encoded_bytes, byte_count{8});
    codec::limits_config config;
    config.max_retained_bytes = byte_count{4096};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    expect_codec_error(
      model::reserve_record_input(value, work, memory(), anchor).get(),
      errc::resource_exhausted);
    EXPECT_EQ(value.value()->size(), byte_count{1});
    EXPECT_EQ(value.value()->retained_bytes(), byte_count{65536});
}

TEST(RecordTest, ExactResidualReservationAndOneShortUseTheSameActualCosts) {
    std::vector<model::record_header> headers;
    headers.reserve(8);
    headers.push_back(header("name"sv, "value"sv));
    auto value = make(text("key"sv), payload(64, 1), std::move(headers));
    const auto usage = cost(value);
    const auto total = usage.backing.checked_add(usage.metadata).value();
    for (unsigned deficit = 0; deficit < 3; ++deficit) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto budget = memory();
        budget.operation_remaining = total;
        budget.metadata_remaining = usage.metadata;
        if (deficit == 1)
            budget.operation_remaining
              = total.checked_sub(byte_count{1}).value();
        if (deficit == 2)
            budget.metadata_remaining
              = usage.metadata.checked_sub(byte_count{1}).value();
        const auto original = budget;
        const auto remaining
          = model::reserve_record_input(value, work, budget, anchor).get();
        EXPECT_EQ(budget, original);
        if (deficit == 0) {
            ASSERT_TRUE(remaining.has_value());
            EXPECT_EQ(remaining->operation_remaining, byte_count{});
            EXPECT_EQ(remaining->metadata_remaining, byte_count{});
            EXPECT_EQ(remaining->charge, charge);
        } else {
            expect_codec_error(remaining, errc::resource_exhausted);
        }
    }
}

TEST(RecordTest, LocalMetadataAllocationAndFragmentCeilingsRemainIndependent) {
    auto value = make(std::nullopt, payload(64, 1));
    const auto usage = cost(value);
    for (unsigned choice = 0; choice < 3; ++choice) {
        codec::limits_config config;
        if (choice == 0) config.max_buffer_fragments = item_count{63};
        if (choice == 1)
            config.max_metadata_bytes
              = usage.metadata.checked_sub(byte_count{1}).value();
        if (choice == 2)
            config.max_allocation_bytes
              = usage.largest_allocation.checked_sub(byte_count{1}).value();
        seastar::abort_source abort;
        codec::cooperative_work work{
          codec::limits::make(config).value(), abort};
        expect_codec_error(
          model::reserve_record_input(value, work, memory(), anchor).get(),
          errc::resource_exhausted);
    }
}

TEST(RecordTest, InvalidChargeAndOverflowAreDistinctFromBudgetRejection) {
    auto value = make(text("key"sv));
    const auto short_charge = +[](byte_count request) noexcept {
        return byte_count{request.value() == 0 ? 0 : request.value() - 1U};
    };
    const auto overflowing = +[](byte_count) noexcept {
        return byte_count{std::numeric_limits<std::uint64_t>::max()};
    };
    for (const auto profile :
         std::array<kwaque::bytes::allocation_charge_fn, 3>{
           nullptr, short_charge, overflowing}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        expect_codec_error(
          model::record_allocation_cost(value, work, profile, anchor).get(),
          profile == overflowing ? errc::out_of_range : errc::invalid_argument);
    }
}

TEST(RecordTest, EmptyRecordsChargeNoAllocationsButConsumeCumulativeWork) {
    auto value = make();
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const codec::decode_budget zero{byte_count{}, byte_count{}, charge};
    const auto first
      = model::reserve_record_input(value, work, zero, anchor).get();
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(*first, zero);
    const auto after_first = work.items_remaining();
    const auto second
      = model::reserve_record_input(value, work, zero, anchor).get();
    ASSERT_TRUE(second.has_value());
    EXPECT_LT(work.items_remaining(), after_first);
    EXPECT_LT(after_first, work.item_quantum());
}

TEST(RecordTest, NarrowWorkSplitsEqualityAndAccountingWithoutCopies) {
    auto left = make(std::nullopt, payload(32768, 64));
    auto right = make(std::nullopt, payload(32768, 4096));
    codec::limits_config config;
    config.max_work_bytes = byte_count{8};
    config.max_work_items = item_count{16};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    const auto l
      = model::record_allocation_cost(left, work, charge, anchor).get();
    const auto r
      = model::record_allocation_cost(right, work, charge, anchor).get();
    ASSERT_TRUE(l.has_value());
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(l->fragments, item_count{512});
    EXPECT_EQ(r->fragments, item_count{8});
    EXPECT_TRUE(model::records_equal(left, right, work, anchor).get().value());
    EXPECT_EQ(work.byte_quantum(), byte_count{8});
    EXPECT_EQ(work.item_quantum(), item_count{16});
}

TEST(RecordTest, CancellationPrecedesEvenUnequalAndInvalidProfilePaths) {
    auto left = make();
    auto right = make(text("different"sv));
    seastar::abort_source abort;
    abort.request_abort();
    codec::cooperative_work work{codec::limits::defaults(), abort};
    expect_codec_error(
      model::records_equal(left, right, work, anchor).get(), errc::aborted);
    expect_codec_error(
      model::record_allocation_cost(left, work, nullptr, anchor).get(),
      errc::aborted);
    expect_codec_error(
      model::reserve_record_input(left, work, memory(), anchor).get(),
      errc::aborted);
}

TEST(RecordTest, InsufficientIndivisibleWorkRejectsWithoutEnlargingTheQuantum) {
    auto value = make(std::nullopt, text("x"sv));
    for (const bool accounting : {false, true}) {
        codec::limits_config config;
        config.max_work_bytes = byte_count{1};
        config.max_work_items = item_count{accounting ? 15U : 8U};
        seastar::abort_source abort;
        codec::cooperative_work work{
          codec::limits::make(config).value(), abort};
        if (accounting) {
            expect_codec_error(
              model::record_allocation_cost(value, work, charge, anchor).get(),
              errc::resource_exhausted);
        } else {
            expect_codec_error(
              model::records_equal(value, value, work, anchor).get(),
              errc::resource_exhausted);
        }
        EXPECT_EQ(work.byte_quantum(), config.max_work_bytes);
        EXPECT_EQ(work.item_quantum(), config.max_work_items);
        EXPECT_TRUE(value.value()->content_equals("x"sv));
    }
}

TEST(RecordTest, BorrowedTraversalsCreateNoOrdinaryAllocationOrPayloadShare) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "ordinary allocation injection is disabled";
#else
    auto left = make(std::nullopt, payload(4096, 8));
    auto right = make(std::nullopt, payload(4096, 64));
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto& injector = seastar::memory::local_failure_injector();
    injector.fail_after(0);
    auto cancel = seastar::defer([&injector] noexcept { injector.cancel(); });
    // Native critical coroutine/frame allocations are not observed by this
    // injector; the inputs, charge function and comparison need no new owners.
    const auto usage
      = model::record_allocation_cost(left, work, charge, anchor).get();
    const auto same = model::records_equal(left, right, work, anchor).get();
    const bool injected = injector.failed();
    injector.cancel();
    EXPECT_FALSE(injected);
    ASSERT_TRUE(usage.has_value());
    ASSERT_TRUE(same.has_value());
    EXPECT_TRUE(*same);
    EXPECT_EQ(left.value()->fragment_count(), 512U);
    EXPECT_EQ(right.value()->fragment_count(), 64U);
#endif
}

thread_local seastar::abort_source* abort_during_charge = nullptr;
thread_local unsigned observed_charges = 0;

byte_count cancelling_charge(byte_count request) noexcept {
    ++observed_charges;
    if (observed_charges == 9 && abort_during_charge != nullptr) {
        abort_during_charge->request_abort();
    }
    return charge(request);
}

TEST(RecordTest, AbortDuringActualCostTraversalNeverReturnsAReservation) {
    auto value = make(std::nullopt, payload(128, 1));
    seastar::abort_source abort;
    codec::limits_config config;
    config.max_work_items = item_count{16};
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    observed_charges = 0;
    abort_during_charge = &abort;
    auto reset = seastar::defer([] noexcept { abort_during_charge = nullptr; });
    auto budget = memory();
    budget.charge = cancelling_charge;
    expect_codec_error(
      model::reserve_record_input(value, work, budget, anchor).get(),
      errc::aborted);
    EXPECT_GE(observed_charges, 9U);
    EXPECT_LT(observed_charges, 257U);
    EXPECT_EQ(value.value()->size(), byte_count{128});
}

} // namespace
