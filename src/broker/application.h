#pragma once

#include <memory>

namespace kwaque::broker {

namespace detail {
class application_state;
}

class application final {
public:
  application();
  ~application();

  application(const application &) = delete;
  application &operator=(const application &) = delete;
  application(application &&) = delete;
  application &operator=(application &&) = delete;

  int run(int argc, char **argv);

private:
  std::unique_ptr<detail::application_state> state_;
};

} // namespace kwaque::broker
