#include "src/broker/pid_file.h"

#include <gtest/gtest.h>
#include <sys/file.h>
#include <sys/wait.h>

#include <cerrno>
#include <chrono>
#include <fcntl.h>
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
TEST(PidFileTest, ASecondProcessCannotAcquireTheOwnedInode) {
    temporary_directory directory;
    const auto path = directory.path() / "kwaque.pid";
    kwaque::broker::pid_file owner(path);
    const char* native_path = path.c_str();
    const auto child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        // Only syscall operations after fork; no allocator or test framework.
        const int descriptor = ::open(
          native_path, O_RDWR | O_CLOEXEC | O_NOFOLLOW);
        if (descriptor < 0) ::_exit(2);
        const int locked = ::flock(descriptor, LOCK_EX | LOCK_NB);
        const int reason = errno;
        ::close(descriptor);
        ::_exit(
          locked < 0 && (reason == EWOULDBLOCK || reason == EAGAIN) ? 0 : 3);
    }
    int status = 0;
    pid_t waited;
    do {
        waited = ::waitpid(child, &status, 0);
    } while (waited < 0 && errno == EINTR);
    ASSERT_EQ(waited, child);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
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

TEST(PidFileTest, DoesNotRemoveAFileWhoseContentsChanged) {
    temporary_directory directory;
    const auto path = directory.path() / "kwaque.pid";

    {
        kwaque::broker::pid_file owner(path);
        std::ofstream replaced(path, std::ios::trunc);
        replaced << ::getpid() << "unexpected\n";
    }

    EXPECT_TRUE(std::filesystem::exists(path));
}

} // namespace
