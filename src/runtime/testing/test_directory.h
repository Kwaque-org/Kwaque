#ifndef KWAQUE_SRC_RUNTIME_TESTING_TEST_DIRECTORY_H_
#define KWAQUE_SRC_RUNTIME_TESTING_TEST_DIRECTORY_H_

#include <cstdlib>
#include <filesystem>
#include <stdexcept>

namespace kwaque::runtime::testing {

// Random suffixes avoid fixture collisions; they never enter semantic results.
inline std::filesystem::path test_directory_template() {
    const auto* value = std::getenv("TEST_TMPDIR");
    if (value == nullptr || *value == '\0') {
        throw std::runtime_error("file fixtures require TEST_TMPDIR");
    }
    std::filesystem::path root{value};
    if (!root.is_absolute()) {
        throw std::runtime_error("TEST_TMPDIR must be absolute");
    }
    return root / "kwaque-test-XXXXXXXXXXXX";
}

} // namespace kwaque::runtime::testing

#endif // KWAQUE_SRC_RUNTIME_TESTING_TEST_DIRECTORY_H_
