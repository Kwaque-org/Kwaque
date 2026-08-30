#ifndef KWAQUE_SRC_RUNTIME_PRODUCTION_NETWORK_CONNECT_INTERNAL_H_
#define KWAQUE_SRC_RUNTIME_PRODUCTION_NETWORK_CONNECT_INTERNAL_H_

#include <seastar/core/abort_source.hh>
#include <seastar/util/optimized_optional.hh>

#include <exception>
#include <optional>

namespace kwaque::runtime::production::connect_detail {

template<typename Socket>
class connect_abort_guard final {
public:
    connect_abort_guard(Socket& socket, seastar::abort_source& abort_source)
      : subscription_(abort_source.subscribe(
          [&socket](const std::optional<std::exception_ptr>&) noexcept {
              try {
                  socket.shutdown();
              } catch (...) {
              }
          })) {}

    connect_abort_guard(const connect_abort_guard&) = delete;
    connect_abort_guard& operator=(const connect_abort_guard&) = delete;
    connect_abort_guard(connect_abort_guard&&) = delete;
    connect_abort_guard& operator=(connect_abort_guard&&) = delete;

    [[nodiscard]] bool armed() const noexcept {
        return static_cast<bool>(subscription_);
    }

private:
    seastar::optimized_optional<seastar::abort_source::subscription>
      subscription_;
};

} // namespace kwaque::runtime::production::connect_detail

#endif // KWAQUE_SRC_RUNTIME_PRODUCTION_NETWORK_CONNECT_INTERNAL_H_
