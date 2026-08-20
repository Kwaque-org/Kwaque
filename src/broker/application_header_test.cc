#include "src/broker/application.h"

#include <gtest/gtest.h>

#include <type_traits>

static_assert(!std::is_copy_constructible_v<kwaque::broker::application>);
static_assert(!std::is_move_constructible_v<kwaque::broker::application>);

TEST(ApplicationHeaderTest, ExposesStableOwnerType) {
    EXPECT_TRUE((std::is_class_v<kwaque::broker::application>));
}
