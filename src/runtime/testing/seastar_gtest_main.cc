#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>

#include <gtest/gtest.h>

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);

    seastar::app_template app;
    return app.run(argc, argv, [] {
        return seastar::make_ready_future<int>(RUN_ALL_TESTS());
    });
}
