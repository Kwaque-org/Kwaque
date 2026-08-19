#include "src/broker/application.h"

#include "src/base/build_info.h"
#include "src/broker/application_internal.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>

#include <boost/program_options.hpp>

#include <cstdlib>
#include <iostream>
#include <string>
#include <string_view>

namespace kwaque::broker {

namespace {

bool version_requested(int argc, char **argv) noexcept {
  for (int index = 1; index < argc; ++index) {
    if (std::string_view(argv[index]) == "--version") {
      return true;
    }
  }
  return false;
}

} // namespace

application::application() : state_(std::make_unique<detail::application_state>()) {}

application::~application() = default;

int application::run(int argc, char **argv) {
  if (version_requested(argc, argv)) {
    std::cout << build_info::version_line() << '\n';
    return EXIT_SUCCESS;
  }

  seastar::app_template::config app_config;
  app_config.name = "Kwaque";
  app_config.description = "Kwaque distributed log broker";
  app_config.auto_handle_sigint_sigterm = false;
  seastar::app_template app(std::move(app_config));
  app.add_options()(
      "config",
      boost::program_options::value<std::string>()->default_value(
          "conf/kwaque.yaml"),
      "Path to the Kwaque bootstrap configuration file");

  return app.run(argc, argv, [this, &app] {
    return seastar::async(
        [this, &app] { return state_->execute(app.configuration()); });
  });
}

} // namespace kwaque::broker
