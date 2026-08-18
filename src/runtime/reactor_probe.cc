#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/util/log.hh>

namespace {

seastar::logger runtime_log("kwaque-runtime");

seastar::future<> run_probe() {
    runtime_log.info("reactor ready on shard {}", seastar::this_shard_id());
    return seastar::make_ready_future<>();
}

} // namespace

int main(int argc, char** argv) {
    seastar::app_template app;
    return app.run(argc, argv, run_probe);
}
