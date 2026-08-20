#include "src/broker/pid_file.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>
#include <unistd.h>

namespace {

class temporary_directory final {
public:
    temporary_directory() {
        path_ = std::filesystem::temp_directory_path() /
            ("kwaque-pid-test-" + std::to_string(::getpid()) + "-" +
             std::to_string(std::chrono::steady_clock::now()
                                .time_since_epoch()
                                .count()));
        std::filesystem::create_directories(path_);
    }

    ~temporary_directory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
};

TEST(PidFileTest, RejectsASecondOwnerAndRemovesOwnedFile) {
    temporary_directory directory;
    const auto path = directory.path() / "kwaque.pid";

    {
        kwaque::broker::pid_file owner(path);
        EXPECT_THROW(
          static_cast<void>(kwaque::broker::pid_file(path)),
          kwaque::broker::pid_file_locked);
        EXPECT_TRUE(std::filesystem::exists(path));
    }
    EXPECT_FALSE(std::filesystem::exists(path));
}

TEST(PidFileTest, ReplacesAStalePid) {
    temporary_directory directory;
    const auto path = directory.path() / "kwaque.pid";
    {
        std::ofstream stale(path);
        stale << "999999\n";
    }

    {
        kwaque::broker::pid_file owner(path);
        std::ifstream contents(path);
        std::string pid;
        std::getline(contents, pid);
        EXPECT_EQ(pid, std::to_string(::getpid()));
    }
    EXPECT_FALSE(std::filesystem::exists(path));
}

} // namespace
