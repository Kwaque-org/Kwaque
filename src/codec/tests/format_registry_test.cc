#include "src/base/error.h"
#include "src/codec/error.h"
#include "src/codec/format_registry.h"

#include <gtest/gtest.h>

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <type_traits>

namespace kwaque::codec::detail {

// Synthetic reader policies exist only in this test translation unit.
struct format_registry_test_access final {
    static constexpr format_descriptor make(
      format_family family,
      std::uint16_t current,
      std::uint16_t oldest_readable,
      std::uint16_t writer,
      std::uint16_t minimum_reader,
      std::uint64_t features = 0) noexcept {
        return format_descriptor{
          family, current, oldest_readable, writer, minimum_reader, features};
    }
};

} // namespace kwaque::codec::detail

namespace {

using kwaque::errc;
using kwaque::codec::error;
using kwaque::codec::format_descriptor;
using kwaque::codec::format_family;
using kwaque::codec::lookup_format;
using kwaque::codec::validate_required_features;
using kwaque::codec::validate_sender_versions;
using kwaque::codec::detail::format_registry;
using kwaque::codec::detail::format_registry_test_access;
using kwaque::codec::detail::valid_format_registry;

constexpr auto maximum_version = std::numeric_limits<std::uint16_t>::max();
constexpr error family_anchor{errc::aborted, 7, 11, 104};
constexpr error writer_anchor{errc::aborted, 7, 12, 106};
constexpr error minimum_anchor{errc::aborted, 7, 13, 108};
constexpr error features_anchor{errc::aborted, 7, 14, 116};

constexpr std::array families{
  format_family::submitted_batch,
  format_family::assigned_batch,
  format_family::segment_header,
  format_family::segment_batch_block,
  format_family::wal_prepare,
  format_family::durable_boundary_footer,
  format_family::sealed_extent,
  format_family::sparse_index,
  format_family::range_manifest,
  format_family::read_checkpoint,
  format_family::local_storage};

static_assert(
  std::same_as<std::underlying_type_t<format_family>, std::uint16_t>);
static_assert(sizeof(format_family) == 2);
static_assert(!std::default_initializable<format_descriptor>);
static_assert(!std::is_aggregate_v<format_descriptor>);
static_assert(!std::constructible_from<
              format_descriptor,
              format_family,
              std::uint16_t,
              std::uint16_t,
              std::uint16_t,
              std::uint16_t,
              std::uint64_t>);
static_assert(std::is_trivially_copyable_v<format_descriptor>);
static_assert(std::is_nothrow_copy_constructible_v<format_descriptor>);
static_assert(std::is_nothrow_move_constructible_v<format_descriptor>);
static_assert(std::is_nothrow_destructible_v<format_descriptor>);
static_assert(std::same_as<
              decltype(lookup_format(1)),
              kwaque::codec::result<format_descriptor>>);
static_assert(noexcept(lookup_format(1)));
static_assert(lookup_format(1).has_value());
static_assert(!lookup_format(0).has_value());
static_assert(!lookup_format(12).has_value());
static_assert(lookup_format(1)->current() == 1);
static_assert(lookup_format(10)->family() == format_family::read_checkpoint);

consteval bool family_codes_match() {
    for (std::size_t index = 0; index < families.size(); ++index) {
        if (static_cast<std::uint16_t>(families[index]) != index + 1U) {
            return false;
        }
    }
    return true;
}

consteval bool replacement_is_rejected(format_descriptor replacement) {
    auto descriptors = format_registry;
    descriptors[0] = replacement;
    return !valid_format_registry(descriptors);
}

static_assert(family_codes_match());
static_assert(valid_format_registry(format_registry));
static_assert(!valid_format_registry(
  std::span<const format_descriptor>{format_registry}.first(9)));
static_assert(!valid_format_registry(std::span<const format_descriptor>{}));
static_assert(replacement_is_rejected(format_registry[1]));
static_assert(replacement_is_rejected(
  format_registry_test_access::make(
    static_cast<format_family>(0), 1, 1, 1, 1)));
static_assert(replacement_is_rejected(
  format_registry_test_access::make(
    static_cast<format_family>(12), 1, 1, 1, 1)));
static_assert(replacement_is_rejected(
  format_registry_test_access::make(
    format_family::submitted_batch, 0, 0, 0, 0)));
static_assert(replacement_is_rejected(
  format_registry_test_access::make(
    format_family::submitted_batch, 1, 0, 1, 1)));
static_assert(replacement_is_rejected(
  format_registry_test_access::make(
    format_family::submitted_batch, 1, 2, 1, 1)));
static_assert(replacement_is_rejected(
  format_registry_test_access::make(
    format_family::submitted_batch, 1, 1, 0, 0)));
static_assert(replacement_is_rejected(
  format_registry_test_access::make(
    format_family::submitted_batch, 1, 1, 1, 0)));
static_assert(replacement_is_rejected(
  format_registry_test_access::make(
    format_family::submitted_batch, 1, 1, 1, 2)));

constexpr auto current_reader = *lookup_format(1);
static_assert(noexcept(validate_sender_versions(
  1, 1, current_reader, writer_anchor, minimum_anchor)));
static_assert(
  validate_sender_versions(1, 1, current_reader, writer_anchor, minimum_anchor)
    .has_value());
static_assert(
  validate_sender_versions(2, 1, current_reader, writer_anchor, minimum_anchor)
    .has_value());
static_assert(
  validate_sender_versions(0, 1, current_reader, writer_anchor, minimum_anchor)
    .error()
    .code()
  == errc::malformed_data);
static_assert(
  noexcept(validate_required_features(0, current_reader, features_anchor)));
static_assert(
  validate_required_features(0, current_reader, features_anchor).has_value());

TEST(FormatRegistryTest, RegisteredNamedFamiliesUseTheInitialProfile) {
    for (std::size_t index = 0; index < families.size(); ++index) {
        const auto selected = lookup_format(
          static_cast<std::uint16_t>(index + 1U), family_anchor);
        ASSERT_TRUE(selected.has_value());
        EXPECT_EQ(selected->family(), families[index]);
        EXPECT_EQ(selected->current(), 1U);
        EXPECT_EQ(selected->oldest_readable(), 1U);
        EXPECT_EQ(selected->writer(), 1U);
        EXPECT_EQ(selected->minimum_reader(), 1U);
        EXPECT_EQ(selected->supported_features(), 0U);
        EXPECT_TRUE(validate_sender_versions(
                      selected->writer(),
                      selected->minimum_reader(),
                      *selected,
                      writer_anchor,
                      minimum_anchor)
                      .has_value());
    }
}

TEST(FormatRegistryTest, EveryRawFamilyIsCheckedAndKeepsTrustedDiagnostics) {
    for (std::uint32_t raw = 0; raw <= maximum_version; ++raw) {
        const auto selected = lookup_format(
          static_cast<std::uint16_t>(raw), family_anchor);
        ASSERT_EQ(selected.has_value(), raw >= 1U && raw <= 11U) << raw;
        if (!selected) {
            EXPECT_EQ(
              selected.error(),
              (error{
                raw == 0 ? errc::malformed_data : errc::unsupported_format,
                7,
                11,
                104}))
              << raw;
        }
    }
    const auto zero = lookup_format(0);
    ASSERT_FALSE(zero.has_value());
    EXPECT_EQ(zero.error(), error{errc::malformed_data});
    const auto unknown = lookup_format(maximum_version);
    ASSERT_FALSE(unknown.has_value());
    EXPECT_EQ(unknown.error(), error{errc::unsupported_format});
}

TEST(FormatRegistryTest, LookupReturnsAnOwningDescriptorSnapshot) {
    auto first = *lookup_format(1);
    const auto second = *lookup_format(2);
    first = second;
    EXPECT_EQ(first.family(), format_family::assigned_batch);
    EXPECT_EQ(lookup_format(1)->family(), format_family::submitted_batch);
    EXPECT_EQ(second.family(), format_family::assigned_batch);
}

struct version_case final {
    std::uint16_t writer;
    std::uint16_t minimum;
    errc reason;
    std::uint16_t field;
};

TEST(FormatRegistryTest, VersionStructureAndZeroPrecedeReaderAdmission) {
    constexpr std::array cases{
      version_case{0, 0, errc::unsupported_format, 12},
      version_case{0, 1, errc::malformed_data, 13},
      version_case{0, maximum_version, errc::malformed_data, 13},
      version_case{1, 0, errc::unsupported_format, 13},
      version_case{1, 1, errc::success, 0},
      version_case{1, 2, errc::malformed_data, 13},
      version_case{1, maximum_version, errc::malformed_data, 13},
      version_case{2, 0, errc::unsupported_format, 13},
      version_case{2, 1, errc::success, 0},
      version_case{2, 2, errc::unsupported_format, 13},
      version_case{maximum_version, 0, errc::unsupported_format, 13},
      version_case{maximum_version, 1, errc::success, 0},
      version_case{maximum_version, 2, errc::unsupported_format, 13},
      version_case{
        maximum_version, maximum_version, errc::unsupported_format, 13}};
    for (const auto& input : cases) {
        const auto accepted = validate_sender_versions(
          input.writer,
          input.minimum,
          current_reader,
          writer_anchor,
          minimum_anchor);
        ASSERT_EQ(accepted.has_value(), input.reason == errc::success)
          << input.writer << ':' << input.minimum;
        if (!accepted) {
            EXPECT_EQ(
              accepted.error(),
              (error{
                input.reason,
                7,
                input.field,
                input.field == 12 ? 106U : 108U}));
        }
    }
}

TEST(
  FormatRegistryTest, ReaderOldestVersionRejectsAnOtherwiseCompatibleSender) {
    constexpr auto reader = format_registry_test_access::make(
      format_family::submitted_batch, 2, 2, 2, 2);
    const auto too_old = validate_sender_versions(
      1, 1, reader, writer_anchor, minimum_anchor);
    ASSERT_FALSE(too_old.has_value());
    EXPECT_EQ(too_old.error(), (error{errc::unsupported_format, 7, 12, 106}));
    EXPECT_TRUE(
      validate_sender_versions(2, 1, reader, writer_anchor, minimum_anchor)
        .has_value());
    EXPECT_TRUE(
      validate_sender_versions(2, 2, reader, writer_anchor, minimum_anchor)
        .has_value());
    EXPECT_TRUE(validate_sender_versions(
                  maximum_version, 2, reader, writer_anchor, minimum_anchor)
                  .has_value());
    const auto too_new = validate_sender_versions(
      maximum_version, maximum_version, reader, writer_anchor, minimum_anchor);
    ASSERT_FALSE(too_new.has_value());
    EXPECT_EQ(too_new.error(), (error{errc::unsupported_format, 7, 13, 108}));
    const auto malformed = validate_sender_versions(
      0, 1, reader, writer_anchor, minimum_anchor);
    ASSERT_FALSE(malformed.has_value());
    EXPECT_EQ(malformed.error(), (error{errc::malformed_data, 7, 13, 108}));
}

TEST(FormatRegistryTest, EveryUnassignedRequiredFeatureRejects) {
    for (const auto family : families) {
        const auto reader = lookup_format(static_cast<std::uint16_t>(family));
        ASSERT_TRUE(reader.has_value());
        EXPECT_TRUE(
          validate_required_features(0, *reader, features_anchor).has_value());
        for (unsigned bit = 0; bit < 64; ++bit) {
            const auto rejected = validate_required_features(
              std::uint64_t{1} << bit, *reader, features_anchor);
            ASSERT_FALSE(rejected.has_value()) << bit;
            EXPECT_EQ(
              rejected.error(), (error{errc::unsupported_format, 7, 14, 116}));
        }
        EXPECT_FALSE(
          validate_required_features(
            std::numeric_limits<std::uint64_t>::max(), *reader, features_anchor)
            .has_value());
    }
}

TEST(FormatRegistryTest, FeatureAdmissionChecksTheCompleteMask) {
    constexpr auto high_bit = std::uint64_t{1} << 63U;
    constexpr auto reader = format_registry_test_access::make(
      format_family::submitted_batch, 2, 1, 1, 1, high_bit | 1U);
    for (const auto required : std::array{
           std::uint64_t{0}, std::uint64_t{1}, high_bit, high_bit | 1U}) {
        EXPECT_TRUE(
          validate_required_features(required, reader, features_anchor)
            .has_value());
    }
    const auto rejected = validate_required_features(
      high_bit | 3U, reader, features_anchor);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), (error{errc::unsupported_format, 7, 14, 116}));
    EXPECT_EQ(lookup_format(1)->supported_features(), 0U);
}

} // namespace
