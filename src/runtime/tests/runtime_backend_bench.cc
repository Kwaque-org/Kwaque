#include "src/runtime/production/backend.h"
#include "src/runtime/runtime_service.h"
#include "src/runtime/sharded_service.h"

#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>
#include <seastar/testing/perf_tests.hh>

#include <memory>
#include <utility>

namespace kwaque::runtime {

namespace {

class backend_lifecycle_fixture {
public:
    backend_lifecycle_fixture()
      : runtimes_(seastar::default_smp_service_group()) {
        runtimes_.start().get();
    }

    ~backend_lifecycle_fixture() {
        if (backend_) {
            backend_->stop().get();
            backend_.reset();
        }
        runtimes_.stop().get();
    }

    [[gnu::noinline]] seastar::future<> execute() {
        if (backend_) {
            std::terminate();
        }
        backend_ = std::make_unique<production::backend_owner>(
          seastar::default_smp_service_group());
        return production::start_backends(*backend_, runtimes_)
          .then([this] { return backend_->stop(); })
          .then([this] { backend_.reset(); });
    }

private:
    sharded_service<runtime_service> runtimes_;
    std::unique_ptr<production::backend_owner> backend_;
};

} // namespace

PERF_TEST_F(backend_lifecycle_fixture, construct_start_stop) {
    return execute();
}

} // namespace kwaque::runtime
