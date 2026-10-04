#include "src/broker/service_manager.h"

#include <gtest/gtest.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <array>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <string>
#include <unistd.h>

namespace {

class ScopedNotifySocket final {
public:
    explicit ScopedNotifySocket(const char* value) {
        if (value == nullptr) {
            ::unsetenv("NOTIFY_SOCKET");
        } else {
            ::setenv("NOTIFY_SOCKET", value, 1);
        }
    }
    ScopedNotifySocket(const ScopedNotifySocket&) = delete;
    ScopedNotifySocket& operator=(const ScopedNotifySocket&) = delete;
    ~ScopedNotifySocket() { ::unsetenv("NOTIFY_SOCKET"); }
};

// A bound datagram socket standing in for the service manager.
class Receiver final {
public:
    explicit Receiver(const std::string& name) {
        descriptor_ = ::socket(AF_UNIX, SOCK_DGRAM | SOCK_CLOEXEC, 0);
        EXPECT_GE(descriptor_, 0);
        sockaddr_un address{};
        address.sun_family = AF_UNIX;
        std::memcpy(address.sun_path, name.data(), name.size());
        if (address.sun_path[0] == '@') {
            address.sun_path[0] = '\0';
        }
        const auto length = static_cast<socklen_t>(
          offsetof(sockaddr_un, sun_path) + name.size());
        EXPECT_EQ(
          ::bind(
            descriptor_, reinterpret_cast<const sockaddr*>(&address), length),
          0);
    }
    Receiver(const Receiver&) = delete;
    Receiver& operator=(const Receiver&) = delete;
    ~Receiver() { ::close(descriptor_); }

    std::string receive() const {
        std::array<char, 256> buffer{};
        const auto size = ::recv(
          descriptor_, buffer.data(), buffer.size(), MSG_DONTWAIT);
        return size < 0 ? std::string{}
                        : std::string(buffer.data(), static_cast<size_t>(size));
    }

private:
    int descriptor_ = -1;
};

std::string unique_name(const char* prefix) {
    return std::string(prefix) + std::to_string(::getpid());
}

TEST(ServiceManagerTest, WithoutANotifySocketNothingIsSent) {
    const ScopedNotifySocket unset{nullptr};
    EXPECT_TRUE(kwaque::broker::notify_service_manager("READY=1"));
    const ScopedNotifySocket empty{""};
    EXPECT_TRUE(kwaque::broker::notify_service_manager("READY=1"));
}

TEST(ServiceManagerTest, SendsEachStateToAnAbstractSocket) {
    const auto name = unique_name("@kwaque-notify-test-");
    const Receiver receiver{name};
    const ScopedNotifySocket socket{name.c_str()};

    EXPECT_TRUE(kwaque::broker::notify_service_manager("READY=1"));
    EXPECT_EQ(receiver.receive(), "READY=1");
    EXPECT_TRUE(kwaque::broker::notify_service_manager("STOPPING=1"));
    EXPECT_EQ(receiver.receive(), "STOPPING=1");
}

TEST(ServiceManagerTest, SendsToAPathSocket) {
    const auto name = unique_name("/tmp/kwaque-notify-test-");
    ::unlink(name.c_str());
    {
        const Receiver receiver{name};
        const ScopedNotifySocket socket{name.c_str()};
        EXPECT_TRUE(kwaque::broker::notify_service_manager("READY=1"));
        EXPECT_EQ(receiver.receive(), "READY=1");
    }
    ::unlink(name.c_str());
}

TEST(ServiceManagerTest, InvalidOrUnreachableSocketsReportFailure) {
    {
        const ScopedNotifySocket relative{"relative/socket"};
        EXPECT_FALSE(kwaque::broker::notify_service_manager("READY=1"));
    }
    {
        const std::string oversized(sizeof(sockaddr_un{}.sun_path) + 1, '/');
        const ScopedNotifySocket socket{oversized.c_str()};
        EXPECT_FALSE(kwaque::broker::notify_service_manager("READY=1"));
    }
    {
        const auto missing = unique_name("@kwaque-notify-missing-");
        const ScopedNotifySocket socket{missing.c_str()};
        EXPECT_FALSE(kwaque::broker::notify_service_manager("READY=1"));
    }
}

} // namespace
