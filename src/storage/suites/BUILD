# The storage test suites, each one label across the packages its tests live
# in. Nothing depends on this package.
#
# Every suite, and every part of one in another package, is tagged manual:
# a wildcard such as //... runs each member already, and a suite it expanded
# would build its members past a tag filter. Name a suite to run it.

package(default_visibility = ["//visibility:public"])

# The storage unit tests outside the format suite.
test_suite(
    name = "local_storage_tests",
    tags = ["manual"],
    tests = ["//src/storage/tests:local_storage_tests"],
)

# The storage contracts and the engine on the production file system.
test_suite(
    name = "storage_backend_tests",
    tags = ["manual"],
    tests = [
        "//src/broker:storage_backend_tests",
        "//src/storage/tests:storage_backend_tests",
    ],
)

# The storage contracts, histories and crash sweeps on the simulated file
# system.
test_suite(
    name = "storage_crash_tests",
    tags = ["manual"],
    tests = ["//src/simulation/tests:storage_crash_tests"],
)

# The formats' goldens, the store a fixed history leaves and the engine on
# the checked-in store image, simulated and native.
test_suite(
    name = "storage_golden_tests",
    tags = ["manual"],
    tests = [
        "//src/broker:storage_golden_tests",
        "//src/simulation/tests:storage_golden_tests",
        "//src/storage/tests:storage_golden_tests",
    ],
)

# The engine at the writable bound with churn, simulated and native, and
# failed allocations swept through it.
test_suite(
    name = "storage_scale_tests",
    tags = ["manual"],
    tests = [
        "//src/broker:storage_scale_tests",
        "//src/simulation/tests:storage_scale_tests",
    ],
)
