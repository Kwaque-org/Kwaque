#include "src/base/error.h"
#include "src/storage/tests/local_metadata_fixture.h"
#include "src/storage/tests/local_metadata_fixture_cases.h"
#include "src/storage/tests/local_metadata_fuzz_cases.h"
#include "src/storage/tests/segment_test_support.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <set>
#include <string>
#include <string_view>
#include <vector>

namespace kwaque::storage::testing {
namespace {
using mutation = local_metadata_mutation;

local_metadata_control control(
  std::size_t record,
  mutation what,
  std::size_t index = 0,
  std::uint8_t bit = 0,
  std::uint8_t layout = 3) {
    return {
      static_cast<std::uint8_t>(record),
      layout,
      0,
      static_cast<std::uint8_t>(what),
      static_cast<std::uint8_t>(index),
      static_cast<std::uint8_t>(index >> 8U),
      bit,
      0};
}
// What a record's frame says is left over after its payload.
std::size_t padding_of(const std::string& wire) {
    return get(wire, get(wire, 10, 2) + 68, 4);
}
// The first record of each kind that decodes and fills one small page: its
// place in the table.
std::vector<std::size_t> one_of_each_kind() {
    std::vector<std::size_t> records;
    std::set<std::uint16_t> kinds;
    for (std::size_t record = 0; record != std::size(local_fixture_cases);
         ++record) {
        const auto& test = local_fixture_cases[record];
        if (
          test.expected == errc::success && test.bytes == 4096
          && test.canonical == test.name && kinds.insert(test.kind).second)
            records.push_back(record);
    }
    return records;
}
std::string seed(std::string_view name) {
    const auto* directory = std::getenv("TEST_SRCDIR");
    const auto* workspace = std::getenv("TEST_WORKSPACE");
    const auto root = directory && workspace
                        ? std::string{directory} + "/" + workspace + "/"
                        : std::string{};
    std::ifstream file{
      root + "src/storage/tests/testdata/local_metadata_fuzz_corpus/"
        + std::string{name} + ".bin",
      std::ios::binary};
    return {std::istreambuf_iterator<char>(file), {}};
}

// The corpus is one script a record: its place in the table and nothing
// done to it. Each must answer as its row says, however its bytes are cut
// up, so no seed is ever a record the decoders are not held to.
TEST(LocalMetadataFuzzCasesTest, EverySeedIsItsRowsVerdict) {
    for (std::size_t record = 0; record != std::size(local_fixture_cases);
         ++record) {
        const auto& test = local_fixture_cases[record];
        SCOPED_TRACE(test.name);
        const std::string expected{
          static_cast<char>(record), 0, 0, 0, 0, 0, 0, 0};
        EXPECT_EQ(seed(test.name), expected);
        for (const auto layout : std::array<std::uint8_t, 4>{0, 1, 2, 3})
            EXPECT_EQ(
              check_local_metadata_case(
                control(record, mutation::none, 0, 0, layout), {}),
              "");
    }
}

TEST(LocalMetadataFuzzCasesTest, EveryKindHasARecord) {
    std::set<std::uint16_t> kinds;
    for (const auto record : one_of_each_kind())
        kinds.insert(local_fixture_cases[record].kind);
    EXPECT_EQ(kinds.size(), 10U);
}

// Every single bit of one record of each kind, flipped with nothing made
// again, the padding's bits among them: the decoder that knows no owner
// refuses each, so no bit of a record lies outside its checksums.
TEST(LocalMetadataFuzzCasesTest, EveryBitOfARecordIsUnderAChecksum) {
    for (const auto record : one_of_each_kind()) {
        const auto& test = local_fixture_cases[record];
        SCOPED_TRACE(test.name);
        auto wire = local_fixture::read(test.name);
        ASSERT_FALSE(local_metadata_probe_refuses(wire));
        std::size_t taken = 0, first = 0;
        for (std::size_t at = 0; at != wire.size() * 8; ++at) {
            const auto bit = static_cast<char>(1U << (at % 8));
            wire[at / 8] ^= bit;
            if (!local_metadata_probe_refuses(wire) && taken++ == 0) first = at;
            wire[at / 8] ^= bit;
        }
        EXPECT_EQ(taken, 0U) << "the first flipped bit taken is " << first;
    }
}

// A byte of the padding that is not zero is malformed, wherever in the
// padding it is and though the record is sealed again around it: for every
// padding byte of one record of each kind, and for the first and the last of
// every record.
TEST(LocalMetadataFuzzCasesTest, ASetPaddingByteIsMalformed) {
    const auto whole = one_of_each_kind();
    for (std::size_t record = 0; record != std::size(local_fixture_cases);
         ++record) {
        const auto& test = local_fixture_cases[record];
        if (test.bytes != 4096) continue;
        SCOPED_TRACE(test.name);
        const auto padding = padding_of(local_fixture::read(test.name));
        if (padding == 0 || padding > 4096) continue;
        const bool every = std::ranges::find(whole, record) != whole.end();
        for (std::size_t index = 0; index < padding;
             index += every || index + 1 == padding ? 1 : padding - 1)
            ASSERT_EQ(
              check_local_metadata_case(
                control(record, mutation::padded, index, index % 8), {}),
              "")
              << "padding byte " << index << " from the end";
    }
}

// One bit of a record's header or payload flipped and the record sealed
// again is another record or no record: whichever, the two layouts agree, a
// refusal reads nothing, and what is decoded encodes to bytes that decode
// to the same.
TEST(LocalMetadataFuzzCasesTest, AFlippedBitThatIsSealedBreaksNoRule) {
    for (const auto record : one_of_each_kind()) {
        const auto& test = local_fixture_cases[record];
        SCOPED_TRACE(test.name);
        const auto wire = local_fixture::read(test.name);
        const auto written = std::min<std::size_t>(
          wire.size() - padding_of(wire), 512);
        for (std::size_t at = 0; at != written; ++at)
            for (std::uint8_t bit = 0; bit != 8; ++bit)
                ASSERT_EQ(
                  check_local_metadata_case(
                    control(record, mutation::flipped_sealed, at, bit, 2), {}),
                  "")
                  << "byte " << at << " bit " << unsigned{bit};
    }
}

// The other mutations with a declared outcome, on every record: another
// device's record is refused as that, a record cut short anywhere is
// refused, and a flipped bit that nothing seals is refused by both decoders.
TEST(LocalMetadataFuzzCasesTest, EveryRecordHoldsWhatIsDeclared) {
    for (std::size_t record = 0; record != std::size(local_fixture_cases);
         ++record) {
        const auto& test = local_fixture_cases[record];
        SCOPED_TRACE(test.name);
        EXPECT_EQ(
          check_local_metadata_case(control(record, mutation::misdirected), {}),
          "");
        for (const std::size_t cut : {0U, 1U, 31U, 32U, 103U, 104U, 4095U})
            EXPECT_EQ(
              check_local_metadata_case(
                control(record, mutation::truncated, cut), {}),
              "")
              << "cut at " << cut;
        for (const std::size_t at : {0U, 4U, 10U, 24U, 28U, 32U, 68U, 4095U})
            EXPECT_EQ(
              check_local_metadata_case(
                control(record, mutation::flipped, at, at % 8), {}),
              "")
              << "flipped at " << at;
    }
}

// A script that writes over a record, and one that stands in a record's
// place: nothing, a record of another kind, and bytes that are no record.
TEST(LocalMetadataFuzzCasesTest, AnyScriptBreaksNoRule) {
    const auto other = local_fixture::read("descriptor_a");
    const std::vector<std::uint8_t> foreign{other.begin(), other.end()};
    const std::vector<std::uint8_t> junk(300, std::uint8_t{0xa5});
    const std::vector<std::uint8_t> edits{96, 0, 1, 32, 0, 9, 200, 0, 7};
    for (const auto record : one_of_each_kind()) {
        SCOPED_TRACE(local_fixture_cases[record].name);
        for (const auto flags : std::array<std::uint8_t, 2>{0, 1}) {
            auto raw = control(record, mutation::raw);
            raw.flags = flags;
            EXPECT_EQ(check_local_metadata_case(raw, {}), "");
            EXPECT_EQ(check_local_metadata_case(raw, foreign), "");
            EXPECT_EQ(check_local_metadata_case(raw, junk), "");
        }
        EXPECT_EQ(
          check_local_metadata_case(control(record, mutation::edited), edits),
          "");
    }
}
} // namespace
} // namespace kwaque::storage::testing
