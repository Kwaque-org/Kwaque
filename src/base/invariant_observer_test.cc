#include "src/base/invariant.h"
#include "src/base/invariant_test_observer.h"

#include <gtest/gtest.h>

#include <array>
#include <exception>
#include <latch>
#include <string>
#include <string_view>
#include <thread>

namespace {

class observed_invariant final : public std::exception {};

thread_local std::string* observed_diagnostic = nullptr;

void observe_and_throw(std::string_view diagnostic) {
    if (observed_diagnostic != nullptr) {
        observed_diagnostic->assign(diagnostic);
    }
    throw observed_invariant{};
}

TEST(InvariantObserverTest, ObservesEscapedBoundedDiagnosticWithoutHostPath) {
    std::string diagnostic;
    observed_diagnostic = &diagnostic;
    kwaque::testing::scoped_invariant_observer observer{observe_and_throw};
    const std::string context(kwaque::max_invariant_context_size + 32, 'x');
    std::string unsafe_context{"line\nrow\rtab\tbyte\x01"};
    unsafe_context.push_back(static_cast<char>(0x80));
    unsafe_context += context;

    EXPECT_THROW(
      KWAQUE_INVARIANT(
        kwaque::invariant_id{"KQ-INVARIANT-OBSERVER-TEST"},
        false,
        unsafe_context),
      observed_invariant);
    observed_diagnostic = nullptr;

    EXPECT_NE(
      diagnostic.find("id=KQ-INVARIANT-OBSERVER-TEST"), std::string::npos);
    EXPECT_NE(diagnostic.find("\\n"), std::string::npos);
    EXPECT_NE(diagnostic.find("\\r"), std::string::npos);
    EXPECT_NE(diagnostic.find("\\t"), std::string::npos);
    EXPECT_NE(diagnostic.find("\\x01"), std::string::npos);
    EXPECT_NE(diagnostic.find("\\x80"), std::string::npos);
    EXPECT_NE(diagnostic.find("<truncated>"), std::string::npos);
    EXPECT_NE(
      diagnostic.find("source=invariant_observer_test.cc:"), std::string::npos);
    EXPECT_EQ(diagnostic.find('\n'), std::string::npos);
    EXPECT_EQ(diagnostic.find('\r'), std::string::npos);
    EXPECT_EQ(diagnostic.find("/home/"), std::string::npos);
    EXPECT_LE(diagnostic.size(), kwaque::max_invariant_diagnostic_size);
}

std::string capture(
  std::string_view expression,
  std::string_view context,
  kwaque::invariant_id id = kwaque::invariant_id{"KQ-BOUNDARY"}) {
    std::string diagnostic;
    observed_diagnostic = &diagnostic;
    kwaque::testing::scoped_invariant_observer observer{observe_and_throw};
    try {
        kwaque::invariant_failed(id, expression, context);
    } catch (const observed_invariant&) {
    }
    observed_diagnostic = nullptr;
    return diagnostic;
}

TEST(InvariantObserverTest, EveryByteHasAnIndependentEscapingExpectation) {
    constexpr std::array<std::string_view, 16> expected{
      "\\x00\\x01\\x02\\x03\\x04\\x05\\x06\\x07\\x08\\t\\n\\x0b\\x0c\\r\\x0e\\x"
      "0f",
      "\\x10\\x11\\x12\\x13\\x14\\x15\\x16\\x17\\x18\\x19\\x1a\\x1b\\x1c\\x1d\\"
      "x1e\\x1f",
      " !\"#$%&'()*+,-./",
      "0123456789:;<=>?",
      "@ABCDEFGHIJKLMNO",
      "PQRSTUVWXYZ[\\\\]^_",
      "`abcdefghijklmno",
      "pqrstuvwxyz{|}~\\x7f",
      "\\x80\\x81\\x82\\x83\\x84\\x85\\x86\\x87\\x88\\x89\\x8a\\x8b\\x8c\\x8d\\"
      "x8e\\x8f",
      "\\x90\\x91\\x92\\x93\\x94\\x95\\x96\\x97\\x98\\x99\\x9a\\x9b\\x9c\\x9d\\"
      "x9e\\x9f",
      "\\xa0\\xa1\\xa2\\xa3\\xa4\\xa5\\xa6\\xa7\\xa8\\xa9\\xaa\\xab\\xac\\xad\\"
      "xae\\xaf",
      "\\xb0\\xb1\\xb2\\xb3\\xb4\\xb5\\xb6\\xb7\\xb8\\xb9\\xba\\xbb\\xbc\\xbd\\"
      "xbe\\xbf",
      "\\xc0\\xc1\\xc2\\xc3\\xc4\\xc5\\xc6\\xc7\\xc8\\xc9\\xca\\xcb\\xcc\\xcd\\"
      "xce\\xcf",
      "\\xd0\\xd1\\xd2\\xd3\\xd4\\xd5\\xd6\\xd7\\xd8\\xd9\\xda\\xdb\\xdc\\xdd\\"
      "xde\\xdf",
      "\\xe0\\xe1\\xe2\\xe3\\xe4\\xe5\\xe6\\xe7\\xe8\\xe9\\xea\\xeb\\xec\\xed\\"
      "xee\\xef",
      "\\xf0\\xf1\\xf2\\xf3\\xf4\\xf5\\xf6\\xf7\\xf8\\xf9\\xfa\\xfb\\xfc\\xfd\\"
      "xfe\\xff",
    };
    for (std::size_t block = 0; block < expected.size(); ++block) {
        std::array<char, 16> input{};
        for (std::size_t index = 0; index < input.size(); ++index) {
            input[index] = static_cast<char>(block * 16 + index);
        }
        const auto diagnostic = capture("false", {input.data(), input.size()});
        EXPECT_NE(
          diagnostic.find(
            " context=" + std::string{expected[block]} + " source="),
          std::string::npos)
          << block;
    }
}

TEST(InvariantObserverTest, ExactInputLimitsAndInvalidIdentifiersAreExplicit) {
    for (const std::size_t excess : {0U, 1U}) {
        const std::string expression(
          kwaque::max_invariant_expression_size + excess, 'e');
        const std::string context(
          kwaque::max_invariant_context_size + excess, 'c');
        const auto diagnostic = capture(expression, context);
        const std::string marker = excess == 0 ? "" : "<truncated>";
        EXPECT_NE(
          diagnostic.find(
            " expression="
            + std::string(kwaque::max_invariant_expression_size, 'e') + marker
            + " context=" + std::string(kwaque::max_invariant_context_size, 'c')
            + marker + " source="),
          std::string::npos);
        EXPECT_LE(diagnostic.size(), kwaque::max_invariant_diagnostic_size);
    }
    const std::string maximum(kwaque::invariant_id::max_size, 'A');
    EXPECT_NE(
      capture("false", "", kwaque::invariant_id{maximum})
        .find("id=" + maximum + " "),
      std::string::npos);
    for (const auto& invalid :
         {std::string{}, maximum + "A", std::string{"bad id"}}) {
        EXPECT_NE(
          capture("false", "", kwaque::invariant_id{invalid})
            .find("id=INVALID "),
          std::string::npos);
    }
}

class first_observation final : public std::exception {};
class second_observation final : public std::exception {};
void first_observer(std::string_view) { throw first_observation{}; }
void second_observer(std::string_view) { throw second_observation{}; }
void returning_observer(std::string_view) {}

void trip() {
    kwaque::invariant_failed(
      kwaque::invariant_id{"KQ-OBSERVER-SCOPE"}, "false", "scope");
}

TEST(InvariantObserverTest, NestedObserverRestoresAfterExceptionUnwinding) {
    kwaque::testing::scoped_invariant_observer outer{first_observer};
    EXPECT_THROW(trip(), first_observation);
    EXPECT_THROW(
      [] {
          kwaque::testing::scoped_invariant_observer inner{second_observer};
          trip();
      }(),
      second_observation);
    EXPECT_THROW(trip(), first_observation);
}

TEST(InvariantObserverTest, ConcurrentThreadsKeepIndependentObservers) {
    kwaque::testing::scoped_invariant_observer outer{first_observer};
    std::latch installed{1};
    std::latch release{1};
    bool child_observed = false;
    std::jthread child{[&] {
        kwaque::testing::scoped_invariant_observer inner{second_observer};
        installed.count_down();
        release.wait();
        try {
            trip();
        } catch (const second_observation&) {
            child_observed = true;
        }
    }};
    installed.wait();
    EXPECT_THROW(trip(), first_observation);
    release.count_down();
    child.join();
    EXPECT_TRUE(child_observed);
    EXPECT_THROW(trip(), first_observation);
}

TEST(InvariantObserverDeathTest, ReturningObserverCannotReplaceTermination) {
    EXPECT_DEATH(
      {
          kwaque::testing::scoped_invariant_observer observer{
            returning_observer};
          trip();
      },
      "id=KQ-OBSERVER-SCOPE ");
}

} // namespace
