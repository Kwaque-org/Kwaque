#pragma once

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shard_id.hh>

#include <functional>

namespace kwaque::runtime {

class runtime_service final {
public:
  explicit runtime_service(
      std::reference_wrapper<seastar::abort_source> abort_source) noexcept;

  [[nodiscard]] seastar::future<> start();
  [[nodiscard]] seastar::future<> stop();

  [[nodiscard]] seastar::shard_id shard() const noexcept;
  [[nodiscard]] bool ready() const noexcept;
  [[nodiscard]] bool abort_requested() const noexcept;

private:
  seastar::abort_source &abort_source_;
  seastar::shard_id shard_;
  bool ready_{false};
};

} // namespace kwaque::runtime
