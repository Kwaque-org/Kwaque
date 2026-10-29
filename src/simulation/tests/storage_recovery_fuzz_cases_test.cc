#include "src/base/error.h"
#include "src/simulation/tests/storage_recovery_fuzz_cases.h"
#include "src/storage/tests/engine_fixture_contract.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {
using namespace kwaque::simulation::testing;

constexpr std::size_t image_files = 11;

recovery_control control(
  std::size_t file,
  image_damage damage,
  std::size_t at = 0,
  std::uint8_t length = 0,
  std::uint8_t bit = 0) {
    return {
      static_cast<std::uint8_t>(file),
      static_cast<std::uint8_t>(damage),
      static_cast<std::uint8_t>(at),
      static_cast<std::uint8_t>(at >> 8U),
      length,
      bit,
      0,
      0};
}
std::string corpus() {
    const auto* directory = std::getenv("TEST_SRCDIR");
    const auto* workspace = std::getenv("TEST_WORKSPACE");
    return (directory && workspace
              ? std::string{directory} + "/" + workspace + "/"
              : std::string{})
           + "src/simulation/tests/testdata/storage_recovery_fuzz_corpus";
}
void say(std::string_view what, const recovery_case_result& result) {
    BOOST_TEST_MESSAGE(
      what << ": " << (result.seen.changed ? "changed" : "unchanged")
           << (result.seen.ready ? ", ready" : ", not ready") << ", recovered "
           << result.seen.recovered << ", unexplained "
           << result.seen.unresolved
           << (result.seen.untouched ? ", image as given" : ", image written")
           << (result.seen.failure ? ", " + *result.seen.failure
                                   : std::string{}));
}
} // namespace

// Each damage is what its name says, byte for byte.
BOOST_AUTO_TEST_CASE(storage_recovery_fuzz_a_damage_is_what_it_says) {
    const std::string page(4096, 'x');
    const auto file = page + page + page;
    const auto same = [&](recovery_control how) {
        return damaged_bytes(file, how, {}, "", "") == file;
    };
    BOOST_CHECK(same(control(0, image_damage::none, 100)));
    auto zeroed = damaged_bytes(
      file, control(0, image_damage::zeros, 4096, 1), {}, "", "");
    BOOST_CHECK_EQUAL(zeroed.size(), file.size());
    BOOST_CHECK(zeroed.substr(4096, 17) == std::string(17, '\0'));
    BOOST_CHECK(zeroed.substr(0, 4096) == page && zeroed[4096 + 17] == 'x');
    const std::array<std::uint8_t, 2> script{1, 2};
    const auto junk = damaged_bytes(
      file, control(0, image_damage::junk, 8), script, "", "");
    BOOST_CHECK(junk[8] == 1 && junk[7] == 'x' && junk[9] == 'x');
    const auto flipped = damaged_bytes(
      file, control(0, image_damage::bit, 5, 0, 3), {}, "", "");
    BOOST_CHECK_EQUAL(flipped[5], static_cast<char>('x' ^ 8));
    BOOST_CHECK_EQUAL(
      damaged_bytes(file, control(0, image_damage::cut, 4097), {}, "", "")
        .size(),
      4097U);
    // The second sector keeps its first 1024 bytes.
    const auto torn = damaged_bytes(
      file, control(0, image_damage::torn, 5000, 2), {}, "", "");
    BOOST_CHECK(torn.substr(0, 4096 + 1024) == file.substr(0, 4096 + 1024));
    BOOST_CHECK(torn.substr(4096 + 1024, 3072) == std::string(3072, '\0'));
    BOOST_CHECK(torn.substr(8192) == page);
    BOOST_CHECK(
      damaged_bytes(file, control(0, image_damage::foreign), {}, "other", "")
      == "other");
    BOOST_CHECK(
      damaged_bytes(file, control(0, image_damage::stale), {}, "", "older")
      == "older");
    const auto longer = damaged_bytes(
      file, control(0, image_damage::appended, 0, 1), {}, "", "");
    BOOST_CHECK_EQUAL(longer.size(), file.size() + 17);
    BOOST_CHECK(longer.substr(0, file.size()) == file);
    BOOST_CHECK(
      damaged_bytes({}, control(0, image_damage::appended), {}, "", "").size()
      == 1);
    // A file that is removed has no bytes to give.
    BOOST_CHECK(same(control(0, image_damage::removed)));
    // Nothing can stand in its place: it is left as it is.
    BOOST_CHECK(same(control(0, image_damage::foreign)));
    BOOST_CHECK(same(control(0, image_damage::stale)));
}

// An overlong record is one whose header says more than it holds and whose
// checksums are right for what the header says.
BOOST_AUTO_TEST_CASE(storage_recovery_fuzz_an_overlong_record_is_sealed) {
    namespace image = kwaque::storage::testing::engine_fixture_contract;
    const auto record = image::publication_b();
    BOOST_REQUIRE_EQUAL(record.size(), 4096U);
    BOOST_REQUIRE(image::sealed_at(record, 0));
    const auto file = record + std::string(4096, 'y');
    const auto said = [](const std::string& bytes) {
        return std::size_t{static_cast<unsigned char>(bytes[12])}
               | (std::size_t{static_cast<unsigned char>(bytes[13])} << 8U);
    };
    const auto longer = damaged_bytes(
      file, control(0, image_damage::overlong, 0, 1), {}, "", "");
    BOOST_REQUIRE_EQUAL(longer.size(), file.size());
    BOOST_CHECK_EQUAL(said(longer), said(file) + 1024);
    BOOST_CHECK(image::sealed_at(longer, 0));
    BOOST_CHECK(longer.substr(32) == file.substr(32));
    // A changed byte of the longer body is under the body's checksum.
    auto broken = longer;
    broken[4096 + 100] ^= 1;
    BOOST_CHECK(!image::sealed_at(broken, 0));
    // With no room for a longer body the record is left as it is.
    BOOST_CHECK(
      damaged_bytes(record, control(0, image_damage::overlong), {}, "", "")
      == record);
}

// Each rule holds for a start that keeps it and breaks for one that does
// not.
BOOST_AUTO_TEST_CASE(storage_recovery_fuzz_each_rule_can_break) {
    recovery_seen baseline;
    baseline.ready = baseline.restarted = true;
    baseline.recovered = 2;
    baseline.a_recovering = baseline.b_recovering = true;
    BOOST_CHECK(recovery_verdict(baseline).empty());
    auto fewer = baseline;
    fewer.recovered = 1;
    BOOST_CHECK_EQUAL(recovery_verdict(fewer), "baseline");
    auto unexplained = baseline;
    unexplained.unresolved = 1;
    BOOST_CHECK_EQUAL(recovery_verdict(unexplained), "baseline");

    recovery_seen refused;
    refused.changed = true;
    refused.untouched = true;
    BOOST_CHECK(recovery_verdict(refused).empty());
    refused.untouched = false;
    BOOST_CHECK_EQUAL(recovery_verdict(refused), "wrote");

    auto other = baseline;
    other.changed = true;
    other.device = 1;
    other.a_recovering = false;
    BOOST_CHECK(recovery_verdict(other).empty());
    other.b_recovering = false;
    BOOST_CHECK_EQUAL(recovery_verdict(other), "spared");
    other.device = 0;
    BOOST_CHECK(recovery_verdict(other).empty());
    other.device = 2;
    other.b_recovering = true;
    BOOST_CHECK_EQUAL(recovery_verdict(other), "spared");
    auto written_back = baseline;
    written_back.changed = written_back.dropped = true;
    written_back.device = 1;
    written_back.materialized = 2;
    BOOST_CHECK(recovery_verdict(written_back).empty());
    auto kept_out = written_back;
    kept_out.materialized = 0;
    BOOST_CHECK_EQUAL(recovery_verdict(kept_out), "lost");
    kept_out = written_back;
    kept_out.ready = false;
    kept_out.untouched = true;
    BOOST_CHECK_EQUAL(recovery_verdict(kept_out), "lost");
}

// Segment A's blocks dropped while the WAL held them are written back from
// their PREPAREs before the start publishes A.
SEASTAR_THREAD_TEST_CASE(storage_recovery_fuzz_dropped_blocks_are_written) {
    const auto result = exercise_recovery_case(
      control(0, image_damage::dropped), {});
    say("dropped", result);
    BOOST_CHECK_MESSAGE(
      result.word.empty(), result.word << " about " << result.about);
    BOOST_CHECK(result.seen.dropped && result.seen.ready);
    BOOST_CHECK_EQUAL(result.seen.materialized, 2U);
}

// With nothing done to it the image is started on as the engine's own case
// starts on it.
SEASTAR_THREAD_TEST_CASE(storage_recovery_fuzz_the_undamaged_image_starts) {
    const auto result = exercise_recovery_case(
      control(0, image_damage::none), {});
    say("undamaged", result);
    BOOST_CHECK_MESSAGE(
      result.word.empty(), result.word << " about " << result.about);
    BOOST_CHECK(!result.seen.changed && result.seen.ready);
    BOOST_CHECK_EQUAL(result.seen.recovered, 2U);
}

// Every seed is one damage, and the start on it breaks no rule.
SEASTAR_THREAD_TEST_CASE(storage_recovery_fuzz_every_seed_breaks_no_rule) {
    std::size_t seeds = 0;
    for (const auto& entry : std::filesystem::directory_iterator{corpus()}) {
        std::ifstream file{entry.path(), std::ios::binary};
        const std::string bytes{std::istreambuf_iterator<char>(file), {}};
        BOOST_REQUIRE_EQUAL(bytes.size(), 8U);
        const recovery_control seed{
          static_cast<std::uint8_t>(bytes[0]),
          static_cast<std::uint8_t>(bytes[1]),
          static_cast<std::uint8_t>(bytes[2]),
          static_cast<std::uint8_t>(bytes[3]),
          static_cast<std::uint8_t>(bytes[4]),
          static_cast<std::uint8_t>(bytes[5]),
          static_cast<std::uint8_t>(bytes[6]),
          static_cast<std::uint8_t>(bytes[7])};
        const auto result = exercise_recovery_case(seed, {});
        say(entry.path().filename().string(), result);
        BOOST_CHECK_MESSAGE(
          result.word.empty(),
          entry.path().filename().string()
            << ": " << result.word << " about " << result.about);
        ++seeds;
    }
    BOOST_CHECK_EQUAL(seeds, 34U);
}

// An earlier control record in the newer one's place says the store was
// never used, beside a WAL it never handed out and segments its owner lists.
// The start refuses it before it writes anything.
SEASTAR_THREAD_TEST_CASE(
  storage_recovery_fuzz_a_stale_control_record_is_refused) {
    const auto result = exercise_recovery_case(
      control(1, image_damage::stale), {});
    say("a stale control record", result);
    BOOST_CHECK(result.seen.changed);
    BOOST_CHECK_MESSAGE(
      result.word.empty(), result.word << " about " << result.about);
    BOOST_CHECK(!result.seen.ready && result.seen.untouched);
    BOOST_CHECK(
      result.seen.failure
      == make_error_code(kwaque::errc::wrong_context).message());
}

// A start that hangs, throws or runs something out is its own word and
// never a rule that held.
BOOST_AUTO_TEST_CASE(storage_recovery_fuzz_the_words_are_the_wrappers) {
    for (const std::string_view word :
         {"baseline", "wrote", "spared", "hang", "exhausted", "threw"}) {
        BOOST_CHECK(!word.empty() && word.size() <= 32);
        for (const char letter : word)
            BOOST_CHECK(letter == '_' || (letter >= 'a' && letter <= 'z'));
    }
}

// One bit flipped in every page of every file of the image, once inside
// what the page holds and once in its last byte: whatever the start makes
// of each, it breaks no rule.
SEASTAR_THREAD_TEST_CASE(storage_recovery_fuzz_a_flipped_bit_in_every_page) {
    for (std::size_t file = 0; file != image_files; ++file) {
        for (std::size_t page = 0; page != 4; ++page) {
            for (const std::size_t within : {40U, 4095U}) {
                const auto at = page * 4096 + within;
                const auto result = exercise_recovery_case(
                  control(file, image_damage::bit, at, 0, 1), {});
                BOOST_CHECK_MESSAGE(
                  result.word.empty(),
                  "file " << file << " byte " << at << ": " << result.word
                          << " about " << result.about);
            }
        }
    }
}
