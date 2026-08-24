#include "src/base/invariant.h"
#include "src/base/invariant_test_observer.h"

#include <gtest/gtest.h>

#include <exception>
#include <string>
#include <string_view>

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

} // namespace
