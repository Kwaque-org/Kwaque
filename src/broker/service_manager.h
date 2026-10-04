#pragma once

#include <string_view>

namespace kwaque::broker {

// Reports a state change, such as "READY=1" or "STOPPING=1", to the service
// manager that started the process: one datagram to the socket named by
// NOTIFY_SOCKET, as sd_notify(3) sends it. A name beginning with '@' is an
// abstract socket. Returns true when the message was sent or no service
// manager asked for notifications, and false when sending failed.
[[nodiscard]] bool notify_service_manager(std::string_view state) noexcept;

} // namespace kwaque::broker
