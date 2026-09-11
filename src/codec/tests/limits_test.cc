#include "src/base/error.h"
#include "src/base/units.h"
#include "src/codec/limits.h"

#include <gtest/gtest.h>

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <utility>

namespace {

using kwaque::byte_count;
using kwaque::item_count;
using kwaque::codec::absolute_max_original_records;
using kwaque::codec::limits;
using kwaque::codec::limits_config;
using kwaque::codec::operation_usage;

constexpr std::uint64_t kib = 1024;
constexpr std::uint64_t mib = 1024 * kib;
constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();

template<typename Count>
struct field_case {
    const char* name;
    Count limits_config::* member;
    std::uint64_t absolute;
};

constexpr std::array byte_fields{
  field_case<byte_count>{
    "allocation", &limits_config::max_allocation_bytes, 128 * kib},
  field_case<byte_count>{"record", &limits_config::max_record_bytes, mib},
  field_case<byte_count>{
    "header name", &limits_config::max_header_name_bytes, 4 * kib},
  field_case<byte_count>{
    "record headers", &limits_config::max_record_header_bytes, 64 * kib},
  field_case<byte_count>{
    "expanded batch", &limits_config::max_expanded_batch_bytes, 8 * mib},
  field_case<byte_count>{"metadata", &limits_config::max_metadata_bytes, mib},
  field_case<byte_count>{
    "encoded body", &limits_config::max_encoded_body_bytes, 16 * mib},
  field_case<byte_count>{
    "retained backing", &limits_config::max_retained_bytes, 32 * mib},
  field_case<byte_count>{
    "operation", &limits_config::max_operation_bytes, 64 * mib},
  field_case<byte_count>{"scratch", &limits_config::max_scratch_bytes, mib},
  field_case<byte_count>{
    "envelope header", &limits_config::max_header_bytes, 4 * kib},
  field_case<byte_count>{
    "control wire", &limits_config::max_control_bytes, 64 * kib},
  field_case<byte_count>{
    "control field", &limits_config::max_control_field_bytes, 4 * kib},
  field_case<byte_count>{
    "checkpoint", &limits_config::max_checkpoint_bytes, 128 * kib},
  field_case<byte_count>{"page", &limits_config::max_page_bytes, 64 * kib},
  field_case<byte_count>{
    "work bytes", &limits_config::max_work_bytes, 64 * kib}};

constexpr std::array count_fields{
  field_case<item_count>{
    "fragments", &limits_config::max_buffer_fragments, 1024},
  field_case<item_count>{
    "record headers", &limits_config::max_record_headers, 64},
  field_case<item_count>{
    "original records", &limits_config::max_original_records, 4096},
  field_case<item_count>{
    "batch headers", &limits_config::max_batch_headers, 4096},
  field_case<item_count>{"extensions", &limits_config::max_extensions, 64},
  field_case<item_count>{"nesting", &limits_config::max_nesting_depth, 8},
  field_case<item_count>{
    "control fields", &limits_config::max_control_fields, 256},
  field_case<item_count>{
    "control repeated", &limits_config::max_control_repeated, 256},
  field_case<item_count>{
    "checkpoint cursors", &limits_config::max_checkpoint_cursors, 4096},
  field_case<item_count>{
    "object entries", &limits_config::max_object_entries, 65536},
  field_case<item_count>{"object pages", &limits_config::max_object_pages, 256},
  field_case<item_count>{"work items", &limits_config::max_work_items, 256}};

constexpr std::array usage_fields{
  &operation_usage::retained_input,
  &operation_usage::staged_output,
  &operation_usage::decoded_metadata,
  &operation_usage::scratch,
  &operation_usage::payload_bookkeeping,
  &operation_usage::payload_migration};

static_assert(absolute_max_original_records == 4096U);
static_assert(!std::default_initializable<limits>);
static_assert(!std::constructible_from<limits, limits_config>);
static_assert(std::is_trivially_copyable_v<limits>);
static_assert(std::is_nothrow_copy_constructible_v<limits>);
static_assert(std::is_nothrow_move_constructible_v<limits>);
static_assert(
  std::same_as<decltype(std::declval<limits&>().config()), limits_config>);
static_assert(std::same_as<
              decltype(std::declval<const limits&>().config()),
              limits_config>);
static_assert(
  std::same_as<decltype(std::declval<limits>().config()), limits_config>);
static_assert(
  std::same_as<decltype(std::declval<const limits>().config()), limits_config>);
static_assert(noexcept(limits::make(limits_config{})));
static_assert(noexcept(limits::defaults().intersect(limits::defaults())));
static_assert(
  limits::defaults().config().max_original_records.value() == 4096U);

void expect_error(const auto& result, kwaque::errc error) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), error);
}

limits validated(limits_config config) { return limits::make(config).value(); }

TEST(CodecLimitsTest, DefaultsMatchEveryIndependentProfileCeiling) {
    const auto defaults = limits::defaults();
    const auto config = defaults.config();
    for (const auto& field : byte_fields) {
        EXPECT_EQ((config.*field.member).value(), field.absolute) << field.name;
    }
    for (const auto& field : count_fields) {
        EXPECT_EQ((config.*field.member).value(), field.absolute) << field.name;
    }
    const auto checked = limits::make(config);
    ASSERT_TRUE(checked.has_value());
    EXPECT_EQ(*checked, defaults);
}

template<typename Count, std::size_t Size>
void verify_field_boundaries(
  const std::array<field_case<Count>, Size>& fields) {
    for (const auto& field : fields) {
        SCOPED_TRACE(field.name);
        for (const auto invalid :
             {std::uint64_t{0}, field.absolute + 1U, maximum}) {
            limits_config config;
            config.*field.member = Count{invalid};
            const auto saved = config;
            expect_error(limits::make(config), kwaque::errc::invalid_argument);
            EXPECT_EQ(config, saved);
        }
        limits_config narrow;
        narrow.*field.member = Count{1};
        const auto accepted = limits::make(narrow);
        ASSERT_TRUE(accepted.has_value());
        EXPECT_EQ(accepted->config(), narrow);
    }
}

TEST(CodecLimitsTest, RejectsEveryInvalidByteCeilingAndAcceptsNarrowValues) {
    verify_field_boundaries(byte_fields);
}

TEST(CodecLimitsTest, RejectsEveryInvalidCountCeilingAndAcceptsNarrowValues) {
    verify_field_boundaries(count_fields);
}

TEST(CodecLimitsTest, ValidatedValuesOwnTheirConfigurationSnapshots) {
    limits_config source;
    source.max_record_bytes = byte_count{321};
    const auto value = validated(source);
    source.max_record_bytes = byte_count{1};
    EXPECT_EQ(value.config().max_record_bytes, byte_count{321});
    auto snapshot = value.config();
    snapshot.max_record_bytes = byte_count{2};
    EXPECT_EQ(snapshot.max_record_bytes, byte_count{2});
    EXPECT_EQ(value.config().max_record_bytes, byte_count{321});
    const auto temporary_snapshot = limits{value}.config();
    EXPECT_EQ(temporary_snapshot.max_record_bytes, byte_count{321});
}

template<typename Count, std::size_t Size>
void set_intersection_inputs(
  const std::array<field_case<Count>, Size>& fields,
  limits_config& left,
  limits_config& right,
  limits_config& expected) {
    for (std::size_t index = 0; index < fields.size(); ++index) {
        const auto& field = fields[index];
        const auto minimum = static_cast<std::uint64_t>(index) + 1U;
        ASSERT_LE(minimum, field.absolute);
        left.*field.member = Count{index % 2U == 0 ? minimum : field.absolute};
        right.*field.member = Count{index % 2U == 0 ? field.absolute : minimum};
        expected.*field.member = Count{minimum};
    }
}

TEST(CodecLimitsTest, IntersectionNarrowsEveryFieldWithoutChangingEitherOwner) {
    limits_config left_config;
    limits_config right_config;
    limits_config expected;
    set_intersection_inputs(byte_fields, left_config, right_config, expected);
    set_intersection_inputs(count_fields, left_config, right_config, expected);
    const auto left = validated(left_config);
    const auto right = validated(right_config);
    const auto combined = left.intersect(right);
    EXPECT_EQ(combined.config(), expected);
    EXPECT_EQ(right.intersect(left), combined);
    EXPECT_EQ(left.intersect(left), left);
    EXPECT_EQ(left.intersect(limits::defaults()), left);
    EXPECT_EQ(right.intersect(limits::defaults()), right);
    EXPECT_EQ(left.config(), left_config);
    EXPECT_EQ(right.config(), right_config);
    EXPECT_TRUE(limits::make(combined.config()).has_value());
}

TEST(CodecLimitsTest, IndependentCeilingsDoNotNeedToFitSimultaneously) {
    limits_config narrow;
    for (const auto& field : byte_fields) {
        narrow.*field.member = byte_count{1};
    }
    for (const auto& field : count_fields) {
        narrow.*field.member = item_count{1};
    }
    EXPECT_TRUE(limits::make(narrow).has_value());

    limits_config crossing;
    crossing.max_operation_bytes = byte_count{1};
    crossing.max_retained_bytes = byte_count{2};
    crossing.max_record_bytes = byte_count{3};
    EXPECT_TRUE(limits::make(crossing).has_value());
    EXPECT_TRUE(limits::make(limits_config{}).has_value());
}

TEST(CodecLimitsTest, AllocationChecksUseTheSuppliedChargedCapacity) {
    const auto defaults = limits::defaults();
    EXPECT_TRUE(defaults.validate_allocation(byte_count{}).has_value());
    EXPECT_TRUE(
      defaults.validate_allocation(byte_count{128 * kib}).has_value());
    expect_error(
      defaults.validate_allocation(byte_count{128 * kib + 1U}),
      kwaque::errc::resource_exhausted);
    expect_error(
      defaults.validate_allocation(byte_count{maximum}),
      kwaque::errc::resource_exhausted);

    limits_config config;
    config.max_allocation_bytes = byte_count{100 * kib};
    const auto narrow = validated(config);
    EXPECT_TRUE(narrow.validate_allocation(byte_count{100 * kib}).has_value());
    expect_error(
      narrow.validate_allocation(byte_count{128 * kib}),
      kwaque::errc::resource_exhausted);
}

TEST(CodecLimitsTest, BufferChecksDistinguishEmptyStagingAndInvalidAccounting) {
    const auto value = limits::defaults();
    EXPECT_TRUE(
      value
        .validate_buffer(byte_count{}, byte_count{}, item_count{}, byte_count{})
        .has_value());
    EXPECT_TRUE(value
                  .validate_buffer(
                    byte_count{}, byte_count{512}, item_count{1}, byte_count{})
                  .has_value());
    EXPECT_TRUE(
      value
        .validate_buffer(
          byte_count{1}, byte_count{512}, item_count{1}, byte_count{1})
        .has_value());
    expect_error(
      value.validate_buffer(
        byte_count{2}, byte_count{1}, item_count{1}, byte_count{2}),
      kwaque::errc::invalid_argument);
    expect_error(
      value.validate_buffer(
        byte_count{}, byte_count{}, item_count{1}, byte_count{}),
      kwaque::errc::invalid_argument);
    expect_error(
      value.validate_buffer(
        byte_count{}, byte_count{1}, item_count{}, byte_count{}),
      kwaque::errc::invalid_argument);
    expect_error(
      value.validate_buffer(
        byte_count{2}, byte_count{2}, item_count{1}, byte_count{1}),
      kwaque::errc::resource_exhausted);
    EXPECT_TRUE(value
                  .validate_buffer(
                    byte_count{32 * mib},
                    byte_count{32 * mib},
                    item_count{256},
                    byte_count{32 * mib})
                  .has_value());
    expect_error(
      value.validate_buffer(
        byte_count{32 * mib},
        byte_count{32 * mib + 1U},
        item_count{257},
        byte_count{32 * mib}),
      kwaque::errc::resource_exhausted);
    EXPECT_TRUE(value
                  .validate_buffer(
                    byte_count{1024},
                    byte_count{1024},
                    item_count{1024},
                    byte_count{1025})
                  .has_value());
    expect_error(
      value.validate_buffer(
        byte_count{1025}, byte_count{1025}, item_count{1025}, byte_count{1025}),
      kwaque::errc::resource_exhausted);
}

TEST(
  CodecLimitsTest, BufferLogicalLimitIncludesHeadersAndHonorsNarrowObjectCaps) {
    const auto value = limits::defaults();
    const byte_count with_header{16 * mib + 4 * kib};
    EXPECT_TRUE(
      value
        .validate_buffer(with_header, with_header, item_count{129}, with_header)
        .has_value());
    expect_error(
      value.validate_buffer(
        with_header, with_header, item_count{129}, byte_count{16 * mib}),
      kwaque::errc::resource_exhausted);

    limits_config config;
    config.max_retained_bytes = byte_count{20};
    config.max_buffer_fragments = item_count{2};
    const auto narrow = validated(config);
    EXPECT_TRUE(narrow
                  .validate_buffer(
                    byte_count{4}, byte_count{20}, item_count{2}, byte_count{4})
                  .has_value());
    expect_error(
      narrow.validate_buffer(
        byte_count{4}, byte_count{21}, item_count{2}, byte_count{4}),
      kwaque::errc::resource_exhausted);
    expect_error(
      narrow.validate_buffer(
        byte_count{4}, byte_count{20}, item_count{3}, byte_count{4}),
      kwaque::errc::resource_exhausted);
}

TEST(CodecLimitsTest, DataBatchCountsRejectZeroAndGrowthWithoutWrapping) {
    const auto value = limits::defaults();
    for (const auto& counts : std::array{
           std::pair{item_count{}, item_count{}},
           std::pair{item_count{}, item_count{1}},
           std::pair{item_count{1}, item_count{}},
           std::pair{item_count{1}, item_count{2}}}) {
        expect_error(
          value.validate_batch_counts(
            counts.first, counts.second, item_count{}),
          kwaque::errc::invalid_argument);
    }
    EXPECT_TRUE(
      value.validate_batch_counts(item_count{1}, item_count{1}, item_count{})
        .has_value());
    EXPECT_TRUE(value
                  .validate_batch_counts(
                    item_count{4096}, item_count{4096}, item_count{4096})
                  .has_value());
    expect_error(
      value.validate_batch_counts(
        item_count{4097}, item_count{1}, item_count{}),
      kwaque::errc::resource_exhausted);
    expect_error(
      value.validate_batch_counts(
        item_count{maximum}, item_count{maximum}, item_count{}),
      kwaque::errc::resource_exhausted);
    expect_error(
      value.validate_batch_counts(
        item_count{4096}, item_count{4096}, item_count{maximum}),
      kwaque::errc::resource_exhausted);
}

TEST(CodecLimitsTest, HeaderCountsIntersectAggregateAndRetainedRecordCeilings) {
    const auto value = limits::defaults();
    EXPECT_TRUE(
      value
        .validate_batch_counts(item_count{4096}, item_count{1}, item_count{64})
        .has_value());
    expect_error(
      value.validate_batch_counts(
        item_count{4096}, item_count{1}, item_count{65}),
      kwaque::errc::resource_exhausted);
    EXPECT_TRUE(value
                  .validate_batch_counts(
                    item_count{4096}, item_count{64}, item_count{4096})
                  .has_value());
    expect_error(
      value.validate_batch_counts(
        item_count{4096}, item_count{63}, item_count{4096}),
      kwaque::errc::resource_exhausted);
    expect_error(
      value.validate_batch_counts(
        item_count{4096}, item_count{4096}, item_count{4097}),
      kwaque::errc::resource_exhausted);

    limits_config config;
    config.max_original_records = item_count{8};
    config.max_batch_headers = item_count{10};
    config.max_record_headers = item_count{3};
    const auto narrow = validated(config);
    EXPECT_TRUE(
      narrow.validate_batch_counts(item_count{8}, item_count{4}, item_count{10})
        .has_value());
    expect_error(
      narrow.validate_batch_counts(
        item_count{8}, item_count{4}, item_count{11}),
      kwaque::errc::resource_exhausted);
    expect_error(
      narrow.validate_batch_counts(
        item_count{8}, item_count{3}, item_count{10}),
      kwaque::errc::resource_exhausted);
    expect_error(
      narrow.validate_batch_counts(item_count{9}, item_count{9}, item_count{}),
      kwaque::errc::resource_exhausted);
}

TEST(CodecLimitsTest, ZeroUsageAndParentResidualsDoNotCreateFreshBudgets) {
    const auto value = limits::defaults();
    const operation_usage empty;
    const auto exhausted_parent = value.remaining_operation_bytes(
      empty, byte_count{});
    ASSERT_TRUE(exhausted_parent.has_value());
    EXPECT_EQ(*exhausted_parent, byte_count{});
    const auto small_parent = value.remaining_operation_bytes(
      empty, byte_count{7});
    ASSERT_TRUE(small_parent.has_value());
    EXPECT_EQ(*small_parent, byte_count{7});
    const auto large_parent = value.remaining_operation_bytes(
      empty, byte_count{maximum});
    ASSERT_TRUE(large_parent.has_value());
    EXPECT_EQ(*large_parent, byte_count{64 * mib});
    expect_error(
      value.remaining_operation_bytes(
        operation_usage{.retained_input = byte_count{1}}, byte_count{}),
      kwaque::errc::resource_exhausted);
}

TEST(CodecLimitsTest, EveryLiveUsageBucketIsChargedWithoutMutatingInputs) {
    const auto value = limits::defaults();
    for (const auto member : usage_fields) {
        operation_usage usage;
        usage.*member = byte_count{7};
        const auto saved = usage;
        const byte_count parent{7};
        const auto remaining = value.remaining_operation_bytes(usage, parent);
        ASSERT_TRUE(remaining.has_value());
        EXPECT_EQ(*remaining, byte_count{});
        expect_error(
          value.remaining_operation_bytes(usage, byte_count{6}),
          kwaque::errc::resource_exhausted);
        EXPECT_EQ(usage, saved);
        EXPECT_EQ(parent, byte_count{7});
    }
    const operation_usage usage{
      byte_count{11},
      byte_count{13},
      byte_count{17},
      byte_count{19},
      byte_count{23},
      byte_count{29}};
    const auto remaining = value.remaining_operation_bytes(
      usage, byte_count{128});
    ASSERT_TRUE(remaining.has_value());
    EXPECT_EQ(*remaining, byte_count{16});
    EXPECT_EQ(value, limits::defaults());
}

TEST(CodecLimitsTest, PerObjectBackingCapsDoNotBecomeAggregateOrPayloadCaps) {
    const auto value = limits::defaults();
    for (const auto logical : {byte_count{10 * mib}, byte_count{12 * mib}}) {
        ASSERT_TRUE(value
                      .validate_buffer(
                        logical,
                        byte_count{20 * mib},
                        item_count{160},
                        byte_count{16 * mib})
                      .has_value());
    }
    const operation_usage usage{
      .retained_input = byte_count{40 * mib},
      .payload_migration = byte_count{2 * mib}};
    const auto saved = usage;
    const auto remaining = value.remaining_operation_bytes(
      usage, byte_count{64 * mib});
    ASSERT_TRUE(remaining.has_value());
    EXPECT_EQ(*remaining, byte_count{22 * mib});
    EXPECT_EQ(usage, saved);
    EXPECT_EQ(value, limits::defaults());
}

TEST(CodecLimitsTest, ChildWorkUsesTheParentsResidualAndOnlyAdditionalCharges) {
    limits_config config;
    config.max_operation_bytes = byte_count{8 * mib};
    const auto parent = validated(config);
    const operation_usage parent_usage{.retained_input = byte_count{3 * mib}};
    const auto saved_parent = parent_usage;
    const auto parent_remaining = parent.remaining_operation_bytes(
      parent_usage, byte_count{8 * mib});
    ASSERT_TRUE(parent_remaining.has_value());
    EXPECT_EQ(*parent_remaining, byte_count{5 * mib});

    const auto child = parent.intersect(limits::defaults());
    EXPECT_EQ(child.config().max_operation_bytes, byte_count{8 * mib});
    // The parent input is already charged; the child adds disjoint output.
    const operation_usage child_usage{.staged_output = byte_count{2 * mib}};
    const auto saved_child = child_usage;
    const auto remaining = child.remaining_operation_bytes(
      child_usage, *parent_remaining);
    ASSERT_TRUE(remaining.has_value());
    EXPECT_EQ(*remaining, byte_count{3 * mib});

    const operation_usage too_large{.staged_output = byte_count{6 * mib}};
    const auto saved_large = too_large;
    expect_error(
      child.remaining_operation_bytes(too_large, *parent_remaining),
      kwaque::errc::resource_exhausted);
    EXPECT_EQ(parent_usage, saved_parent);
    EXPECT_EQ(child_usage, saved_child);
    EXPECT_EQ(too_large, saved_large);
    EXPECT_EQ(*parent_remaining, byte_count{5 * mib});
    EXPECT_EQ(parent.config(), config);
    EXPECT_EQ(child, parent);
}

TEST(
  CodecLimitsTest, MetadataAndScratchCeilingsCannotBeBypassedByParentHeadroom) {
    limits_config config;
    config.max_metadata_bytes = byte_count{10};
    config.max_scratch_bytes = byte_count{20};
    config.max_operation_bytes = byte_count{1000};
    const auto value = validated(config);
    const operation_usage usage{
      .decoded_metadata = byte_count{10},
      .scratch = byte_count{20},
      .payload_bookkeeping = byte_count{100},
      .payload_migration = byte_count{100}};
    const auto remaining = value.remaining_operation_bytes(
      usage, byte_count{1000});
    ASSERT_TRUE(remaining.has_value());
    EXPECT_EQ(*remaining, byte_count{770});
    auto too_much_metadata = usage;
    too_much_metadata.decoded_metadata = byte_count{11};
    expect_error(
      value.remaining_operation_bytes(too_much_metadata, byte_count{maximum}),
      kwaque::errc::resource_exhausted);
    auto too_much_scratch = usage;
    too_much_scratch.scratch = byte_count{21};
    expect_error(
      value.remaining_operation_bytes(too_much_scratch, byte_count{maximum}),
      kwaque::errc::resource_exhausted);
    expect_error(
      value.remaining_operation_bytes(usage, byte_count{229}),
      kwaque::errc::resource_exhausted);
    EXPECT_EQ(value.config(), config);
}

TEST(CodecLimitsTest, AggregateOverlapAndArithmeticOverflowHaveDistinctErrors) {
    const auto value = limits::defaults();
    const operation_usage exact{
      .retained_input = byte_count{32 * mib},
      .staged_output = byte_count{32 * mib}};
    const auto at_limit = value.remaining_operation_bytes(
      exact, byte_count{64 * mib});
    ASSERT_TRUE(at_limit.has_value());
    EXPECT_EQ(*at_limit, byte_count{});
    auto with_metadata = exact;
    with_metadata.decoded_metadata = byte_count{mib};
    with_metadata.scratch = byte_count{mib};
    const auto saved = with_metadata;
    expect_error(
      value.remaining_operation_bytes(with_metadata, byte_count{maximum}),
      kwaque::errc::resource_exhausted);
    EXPECT_EQ(with_metadata, saved);

    for (const auto member : usage_fields) {
        operation_usage overflow;
        overflow.*member = byte_count{maximum};
        if (member == &operation_usage::retained_input) {
            overflow.staged_output = byte_count{1};
        } else {
            overflow.retained_input = byte_count{1};
        }
        const auto original = overflow;
        expect_error(
          value.remaining_operation_bytes(overflow, byte_count{}),
          kwaque::errc::out_of_range);
        EXPECT_EQ(overflow, original);
    }
    expect_error(
      value.remaining_operation_bytes(
        operation_usage{.retained_input = byte_count{maximum}},
        byte_count{maximum}),
      kwaque::errc::resource_exhausted);
}

} // namespace
