#include "src/simulation/tests/storage_history_fuzz_cases.h"
#include "src/storage/tests/engine_history.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {
using namespace kwaque::simulation::testing;
using kwaque::storage::testing::decode_history;
using kwaque::storage::testing::encode_history;

constexpr std::array<std::string_view, 8> seeds{
  "appended",
  "rolled",
  "removed",
  "whole_life",
  "crashed",
  "rolled_crash_all",
  "removed_names",
  "appended_half"};

// A seed of the target's corpus, as the fuzzing engine is handed it.
std::vector<std::uint8_t> seed(std::string_view name) {
    const auto* directory = std::getenv("TEST_SRCDIR");
    const auto* workspace = std::getenv("TEST_WORKSPACE");
    const auto root = directory && workspace
                        ? std::string{directory} + "/" + workspace + "/"
                        : std::string{};
    std::ifstream file{
      root + "src/simulation/tests/testdata/storage_history_fuzz_corpus/"
        + std::string{name} + ".bin",
      std::ios::binary};
    if (!file) throw std::runtime_error("cannot open a history seed");
    const std::string bytes{std::istreambuf_iterator<char>(file), {}};
    return {bytes.begin(), bytes.end()};
}
} // namespace

// The corpus is the histories the tests name, as bytes: a change to how a
// history is encoded changes these files with it, or this says which.
BOOST_AUTO_TEST_CASE(storage_history_fuzz_the_seeds_are_the_fixed_histories) {
    using namespace kwaque::storage::testing;
    BOOST_CHECK(decode_history(seed("appended")) == appended_history());
    BOOST_CHECK(decode_history(seed("rolled")) == rolled_history());
    BOOST_CHECK(decode_history(seed("removed")) == removed_history());
    BOOST_CHECK(decode_history(seed("whole_life")) == whole_life_history());
    for (const auto name : seeds) {
        const auto bytes = seed(name);
        BOOST_TEST_CONTEXT(name) {
            BOOST_CHECK_LE(bytes.size(), storage_history_fuzz_max_input);
            BOOST_CHECK(!decode_history(bytes).operations.empty());
            // Nothing in a seed is left unread.
            BOOST_CHECK(encode_history(decode_history(bytes)) == bytes);
        }
    }
}

// Every seed is a history the store keeps its word through, and so is a
// second run of it on the reactor the first one left: as a fuzzing engine
// runs one input after another. The two have as many lives. What they leave
// on the device is not compared: requests that are offered together reach
// the WAL in the groups the reactor's task order makes of them, and that
// order follows the reactor's preemption timer.
SEASTAR_THREAD_TEST_CASE(storage_history_fuzz_every_seed_holds_and_repeats) {
    for (const auto name : seeds) {
        const auto bytes = seed(name);
        const auto once = exercise_history_case(bytes);
        const auto again = exercise_history_case(bytes);
        BOOST_TEST_CONTEXT(name) {
            BOOST_CHECK_MESSAGE(
              once.end == history_case_end::held,
              once.word << " about " << once.about);
            BOOST_CHECK_MESSAGE(
              again.end == history_case_end::held,
              again.word << " about " << again.about);
            BOOST_CHECK_GE(once.lives, 1U);
            BOOST_CHECK_EQUAL(once.lives, again.lives);
        }
    }
}

// The histories that ask the most of the fixture end by themselves. A
// segment and a crash for every other operation runs more scheduler events
// than any other history found, a third of what the scheduler may run. A
// segment rolled ten times, with a checkpoint and a restart after each
// roll, holds more files open than a device of the other tests gives.
SEASTAR_THREAD_TEST_CASE(storage_history_fuzz_the_longest_histories_end) {
    using namespace kwaque::storage::testing;
    engine_history crashes{{}, {{history_op::create}}};
    crashes.operations.resize(64, {history_op::crash});
    engine_history rolls{
      {history_survival::all},
      {{history_op::create}, {history_op::append, 0, 3}, {history_op::settle}}};
    for (unsigned roll = 0; roll != 10; ++roll)
        rolls.operations.insert(
          rolls.operations.end(),
          {{history_op::create},
           {history_op::append, 1, 3},
           {history_op::seal, 0},
           {history_op::settle},
           {history_op::checkpoint},
           {history_op::restart}});
    const auto crashed = exercise_history_case(encode_history(crashes));
    BOOST_CHECK_MESSAGE(
      crashed.end == history_case_end::held,
      crashed.word << " about " << crashed.about);
    BOOST_CHECK_EQUAL(crashed.lives, 64U);
    const auto rolled = exercise_history_case(encode_history(rolls));
    BOOST_CHECK_MESSAGE(
      rolled.end == history_case_end::held,
      rolled.word << " about " << rolled.about);
    BOOST_CHECK_EQUAL(rolled.lives, 11U);
}

// Every byte string is a history: nothing, a header cut short, an operation
// cut short, and operations on segments that are not there.
SEASTAR_THREAD_TEST_CASE(storage_history_fuzz_any_bytes_are_a_history) {
    const std::vector<std::vector<std::uint8_t>> inputs{
      {},
      {3},
      {0, 0, 0, 0, 0, 0, 0, 0, 3},
      std::vector<std::uint8_t>(72, std::uint8_t{0xff}),
      std::vector<std::uint8_t>(
        storage_history_fuzz_max_input, std::uint8_t{7})};
    for (const auto& input : inputs) {
        const auto result = exercise_history_case(input);
        BOOST_CHECK_MESSAGE(
          result.end == history_case_end::held,
          input.size() << " bytes: " << result.word << " about "
                       << result.about);
    }
}

// A result that held never ends the process, and under a reduction only a
// result of the word the first failure had does.
BOOST_AUTO_TEST_CASE(storage_history_fuzz_only_the_required_word_fails) {
    history_case_result held;
    history_case_result lost{history_case_end::verdict, "lost", "3"};
    history_case_result spent{history_case_end::exhausted, "exhausted", ""};
    BOOST_CHECK(!history_case_fails(held, ""));
    BOOST_CHECK(!history_case_fails(held, "holds"));
    BOOST_CHECK(history_case_fails(lost, ""));
    BOOST_CHECK(history_case_fails(lost, "lost"));
    BOOST_CHECK(!history_case_fails(lost, "altered"));
    BOOST_CHECK(history_case_fails(spent, ""));
    BOOST_CHECK(!history_case_fails(spent, "lost"));
}
