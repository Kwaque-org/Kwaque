#include <seastar/core/future.hh>
#include <seastar/util/later.hh>

#include <gtest/gtest.h>

#include <exception>
#include <stdexcept>

namespace {

TEST(SeastarGtestRunnerTest, PendingFutureCanYieldAndReturnAValue) {
    seastar::promise<int> reply;
    auto result = reply.get_future();
    auto producer = seastar::yield().then([&reply] { reply.set_value(42); });
    EXPECT_FALSE(result.available());
    EXPECT_EQ(result.get(), 42);
    producer.get();
}

TEST(SeastarGtestRunnerTest, PendingFuturePreservesItsException) {
    const auto expected = std::make_exception_ptr(
      std::runtime_error("pending test failure"));
    seastar::promise<> reply;
    auto result = reply.get_future();
    auto producer = seastar::yield().then(
      [&reply, expected] { reply.set_exception(expected); });
    EXPECT_FALSE(result.available());
    std::exception_ptr observed;
    try {
        result.get();
    } catch (...) {
        observed = std::current_exception();
    }
    producer.get();
    EXPECT_EQ(observed, expected);
}

} // namespace
