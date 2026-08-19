#include "src/base/build_info.h"
#include "src/base/logging.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shard_id.hh>

#include <cstdlib>
#include <iostream>
#include <string_view>

namespace {

bool version_requested(int argc, char **argv) noexcept {
  for (int index = 1; index < argc; ++index) {
    if (std::string_view(argv[index]) == "--version") {
      return true;
    }
  }
  return false;
}

seastar::future<> start_broker() {
  kwaque::log::broker.info("startup component=broker shard={}",
                           seastar::this_shard_id());
  return seastar::make_ready_future<>();
}

} // namespace

int main(int argc, char **argv) {
  if (version_requested(argc, argv)) {
    std::cout << kwaque::build_info::version_line() << '\n';
    return EXIT_SUCCESS;
  }

  seastar::app_template app;
  return app.run(argc, argv, start_broker);
}
