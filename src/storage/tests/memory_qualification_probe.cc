#include "src/storage/tests/memory_qualification_probe.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>

#include <boost/program_options.hpp>

#include <cstdio>
#include <stdexcept>
#include <string>

namespace {
int exercise(std::string_view name) {
    namespace q = kwaque::storage::qualification;
    q::observation::report_profile();
    std::printf("compiler=%s\n", __clang_version__);
    if (name == "capabilities") return 0;
    q::observation::require_effective_policy();
    if (name.starts_with("extent-"))
        q::extent_operation(name);
    else if (
      name.starts_with("index-") || name.starts_with("manifest-")
      || name.starts_with("retry-") || name.starts_with("sealed-"))
        q::metadata_operation(name);
    else
        q::format_operation(name);
    std::puts("status=ok");
    return 0;
}
} // namespace

int main(int argc, char** argv) {
    if (!kwaque::codec::testing::install_crypto_allocation_observation()) {
        std::fputs("crypto allocation hooks require a fresh process\n", stderr);
        return 1;
    }
    seastar::app_template app;
    app.set_configuration_reader([](boost::program_options::variables_map&) {});
    app.add_options()(
      "scenario", boost::program_options::value<std::string>()->required());
    return app.run(argc, argv, [&app] {
        return seastar::async([&app] {
            return exercise(app.configuration()["scenario"].as<std::string>());
        });
    });
}
