#include "src/broker/pid_file.h"

#include <sys/file.h>
#include <sys/stat.h>

#include <cerrno>
#include <cstdlib>
#include <fcntl.h>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>

namespace kwaque::broker {

namespace {

void throw_system_error(std::string_view operation) {
    throw std::system_error(
      errno, std::generic_category(), std::string(operation));
}

void write_all(int descriptor, std::string_view contents) {
    std::size_t offset = 0;
    while (offset < contents.size()) {
        const ssize_t written = ::write(
          descriptor, contents.data() + offset, contents.size() - offset);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw_system_error("write PID file");
        }
        if (written == 0) {
            throw std::runtime_error("write PID file made no progress");
        }
        offset += static_cast<std::size_t>(written);
    }
}

} // namespace

pid_file_locked::pid_file_locked(const std::filesystem::path& path)
  : std::runtime_error("PID file is already locked: " + path.string()) {}

pid_file::pid_file(std::filesystem::path path)
  : path_(std::move(path)) {
    descriptor_ = ::open(
      path_.c_str(), O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0644);
    if (descriptor_ < 0) {
        throw_system_error("open PID file");
    }

    try {
        if (::flock(descriptor_, LOCK_EX | LOCK_NB) < 0) {
            if (errno == EWOULDBLOCK || errno == EAGAIN) {
                throw pid_file_locked(path_);
            }
            throw_system_error("lock PID file");
        }

        struct stat status{};
        if (::fstat(descriptor_, &status) < 0) {
            throw_system_error("inspect PID file");
        }
        if (!S_ISREG(status.st_mode)) {
            throw std::runtime_error(
              "PID file is not a regular file: " + path_.string());
        }
        device_ = static_cast<unsigned long long>(status.st_dev);
        inode_ = static_cast<unsigned long long>(status.st_ino);

        if (::ftruncate(descriptor_, 0) < 0) {
            throw_system_error("truncate PID file");
        }
        const std::string contents = std::to_string(::getpid()) + "\n";
        write_all(descriptor_, contents);
        if (::fsync(descriptor_) < 0) {
            throw_system_error("sync PID file");
        }
    } catch (...) {
        ::close(descriptor_);
        descriptor_ = -1;
        throw;
    }
}

pid_file::~pid_file() {
    remove_if_owned();
    if (descriptor_ >= 0) {
        ::close(descriptor_);
    }
}

const std::filesystem::path& pid_file::path() const noexcept { return path_; }

void pid_file::remove_if_owned() noexcept {
    if (descriptor_ < 0) {
        return;
    }

    struct stat descriptor_status{};
    struct stat path_status{};
    if (
      ::fstat(descriptor_, &descriptor_status) < 0
      || ::lstat(path_.c_str(), &path_status) < 0) {
        return;
    }
    if (
      static_cast<unsigned long long>(descriptor_status.st_dev) != device_
      || static_cast<unsigned long long>(descriptor_status.st_ino) != inode_
      || descriptor_status.st_dev != path_status.st_dev
      || descriptor_status.st_ino != path_status.st_ino) {
        return;
    }

    if (::lseek(descriptor_, 0, SEEK_SET) < 0) {
        return;
    }
    char buffer[32]{};
    const ssize_t bytes = ::read(descriptor_, buffer, sizeof(buffer) - 1);
    if (bytes <= 0) {
        return;
    }
    char* end = nullptr;
    errno = 0;
    const long recorded_pid = std::strtol(buffer, &end, 10);
    if (errno != 0 || end == buffer || recorded_pid != ::getpid()) {
        return;
    }
    static_cast<void>(::unlink(path_.c_str()));
}

} // namespace kwaque::broker
