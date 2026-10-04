#include "src/broker/service_manager.h"

#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <unistd.h>

namespace kwaque::broker {

bool notify_service_manager(std::string_view state) noexcept {
    const char* configured = std::getenv("NOTIFY_SOCKET");
    if (configured == nullptr || *configured == '\0') {
        return true;
    }
    const std::string_view name{configured};
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    if (
      (name.front() != '/' && name.front() != '@')
      || name.size() > sizeof(address.sun_path)) {
        return false;
    }
    std::memcpy(address.sun_path, name.data(), name.size());
    if (address.sun_path[0] == '@') {
        // An abstract socket name starts with a NUL byte.
        address.sun_path[0] = '\0';
    }
    const auto length = static_cast<socklen_t>(
      offsetof(sockaddr_un, sun_path) + name.size());

    // One small datagram to a local socket: the send does not wait on the
    // receiver, so this blocking call is brief even on a reactor thread.
    const int descriptor = ::socket(AF_UNIX, SOCK_DGRAM | SOCK_CLOEXEC, 0);
    if (descriptor < 0) {
        return false;
    }
    const auto sent = ::sendto(
      descriptor,
      state.data(),
      state.size(),
      MSG_NOSIGNAL,
      reinterpret_cast<const sockaddr*>(&address),
      length);
    ::close(descriptor);
    return sent == static_cast<ssize_t>(state.size());
}

} // namespace kwaque::broker
