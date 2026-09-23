#pragma once

#include <cstdint>
#include <filesystem>
#include <stdexcept>

namespace kwaque::broker {
struct pid_file_identity final {
    std::uint64_t device, inode;
    bool operator==(const pid_file_identity&) const = default;
};

class pid_file_locked final : public std::runtime_error {
public:
    explicit pid_file_locked(const std::filesystem::path& path);
};

class pid_file final {
public:
    explicit pid_file(std::filesystem::path path);
    ~pid_file();

    pid_file(const pid_file&) = delete;
    pid_file& operator=(const pid_file&) = delete;
    pid_file(pid_file&&) = delete;
    pid_file& operator=(pid_file&&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const noexcept;
    // The descriptor that acquired flock remains owned by this object.
    [[nodiscard]] pid_file_identity identity() const noexcept {
        return {
          static_cast<std::uint64_t>(device_),
          static_cast<std::uint64_t>(inode_)};
    }

private:
    void remove_if_owned() noexcept;

    std::filesystem::path path_;
    int descriptor_{-1};
    unsigned long long device_{0};
    unsigned long long inode_{0};
};

} // namespace kwaque::broker
