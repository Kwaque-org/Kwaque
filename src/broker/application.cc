#include "src/broker/application.h"

#include "src/base/build_info.h"
#include "src/broker/application_internal.h"
#include "src/broker/exit_code.h"
#include "src/broker/startup_policy.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>

#include <boost/program_options.hpp>
#include <sys/prctl.h>

#include <csignal>
#include <iostream>
#include <string>
#include <string_view>

namespace kwaque::broker {

namespace {

bool version_requested(int argc, char** argv) noexcept {
    for (int index = 1; index < argc; ++index) {
        if (std::string_view(argv[index]) == "--version") {
            return true;
        }
    }
    return false;
}

} // namespace

application::application()
  : state_(std::make_unique<detail::application_state>()) {}

application::~application() = default;

int application::run(int argc, char** argv) {
    if (version_requested(argc, argv)) {
        // A pipeline consumer must not mistake a failed write for a version.
        std::cout << build_info::version_line() << '\n' << std::flush;
        return std::cout ? KWAQUE_EXIT_SUCCESS : KWAQUE_EXIT_FAILURE;
    }

    // Keep core dumps possible. Granting the binary a file capability such as
    // CAP_SYS_NICE otherwise clears the dumpable flag.
    if (::prctl(PR_SET_DUMPABLE, 1, 0, 0, 0) != 0) {
        std::cerr << "kwaque: could not make the broker dumpable\n";
        return KWAQUE_EXIT_FAILURE;
    }

    // The configuration is static, so a hang-up has nothing to reload. Its
    // default action would end the broker without a drain, and a service
    // manager treats that as a clean exit. The disposition is process-wide,
    // so it holds whichever thread the kernel delivers the signal to.
    struct sigaction ignore_hangup{};
    ignore_hangup.sa_handler = SIG_IGN;
    ::sigemptyset(&ignore_hangup.sa_mask);
    if (::sigaction(SIGHUP, &ignore_hangup, nullptr) != 0) {
        std::cerr << "kwaque: could not ignore SIGHUP\n";
        return KWAQUE_EXIT_FAILURE;
    }

    seastar::app_template::seastar_options app_config;
    app_config.name = "Kwaque";
    app_config.description = "Kwaque distributed log broker";
    app_config.auto_handle_sigint_sigterm = false;
    detail::configure_allocation_failure_policy(app_config);
    seastar::app_template app(std::move(app_config));
    app.set_configuration_reader(detail::validate_runtime_configuration);
    app.add_options()(
      "config",
      boost::program_options::value<std::string>()->default_value(
        "conf/kwaque.yaml"),
      "Path to the Kwaque bootstrap configuration file");

    return app.run(argc, argv, [this, &app] {
        return seastar::async([this, &app] {
            return state_->execute(app.configuration(), app.options());
        });
    });
}

} // namespace kwaque::broker
