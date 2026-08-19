#include "src/runtime/runtime_service.h"

#include <seastar/core/shard_id.hh>

namespace kwaque::runtime {

runtime_service::runtime_service(
    std::reference_wrapper<seastar::abort_source> abort_source) noexcept
    : abort_source_(abort_source.get()), shard_(seastar::this_shard_id()) {}

seastar::future<> runtime_service::start() {
  ready_ = true;
  return seastar::make_ready_future<>();
}

seastar::future<> runtime_service::stop() {
  ready_ = false;
  return seastar::make_ready_future<>();
}

seastar::shard_id runtime_service::shard() const noexcept { return shard_; }

bool runtime_service::ready() const noexcept { return ready_; }

bool runtime_service::abort_requested() const noexcept {
  return abort_source_.abort_requested();
}

} // namespace kwaque::runtime
