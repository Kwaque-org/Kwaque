#include <gtest/gtest.h>

TEST(SanitizerFailureProbe, ReportsAnOutOfBoundsWrite) {
    auto* values = new int[1];
    values[1] = 42;
    delete[] values;
}
