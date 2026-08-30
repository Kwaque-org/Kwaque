#ifndef KWAQUE_SRC_RUNTIME_PRODUCTION_BACKEND_H_
#define KWAQUE_SRC_RUNTIME_PRODUCTION_BACKEND_H_

#include "src/runtime/environment.h"
#include "src/runtime/production/clocks.h"
#include "src/runtime/production/dns.h"
#include "src/runtime/production/file.h"
#include "src/runtime/production/network.h"
#include "src/runtime/production/random.h"
#include "src/runtime/production/timer.h"
#include "src/runtime/runtime_service.h"
#include "src/runtime/sharded_service.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/util/optimized_optional.hh>

#include <cstddef>
#include <cstdint>
#include <exception>
#include <optional>
#include <stdexcept>
#include <system_error>
#include <utility>

namespace kwaque::runtime::production {

class backend_test_access;

enum class backend_state : std::uint8_t {
    constructed,
    starting,
    started,
    stopping,
    stopped,
};

class backend_dependencies final {
public:
    backend_dependencies(const backend_dependencies&) = default;
    backend_dependencies& operator=(const backend_dependencies&) = default;
    backend_dependencies(backend_dependencies&&) = default;
    backend_dependencies& operator=(backend_dependencies&&) = default;

private:
    friend class backend;
    friend class backend_test_access;
    friend auto backend_parameter(sharded_service<runtime_service>&);

    explicit backend_dependencies(
      seastar::abort_source& parent_abort,
      std::optional<seastar::net::dns_resolver::options> dns_options
      = std::nullopt)
      : parent_abort_(&parent_abort)
      , dns_options_(std::move(dns_options)) {}

    seastar::abort_source* parent_abort_;
    std::optional<seastar::net::dns_resolver::options> dns_options_;
};

class backend final : public shard_affine {
public:
    using monotonic_clock = production::monotonic_clock;
    using wall_clock = production::wall_clock;
    using timer_type = production::timer;
    using random_type = production::random_source;
    using file_system_type = production::file_system;
    using network_type = production::network;
    using dns_type = production::resolver;

    static constexpr bool faults_enabled = false;

    explicit backend(backend_dependencies dependencies);
    ~backend();

    backend(const backend&) = delete;
    backend& operator=(const backend&) = delete;
    backend(backend&&) = delete;
    backend& operator=(backend&&) = delete;

    [[nodiscard]] seastar::future<> start();
    void request_abort();
    [[nodiscard]] seastar::future<> stop();

    [[nodiscard]] runtime_lifetime& lifetime() noexcept { return lifetime_; }
    [[nodiscard]] timer_type& timer() noexcept { return *timer_; }
    [[nodiscard]] random_type& random() noexcept { return *random_; }
    [[nodiscard]] file_system_type& file_system() noexcept {
        return *file_system_;
    }
    [[nodiscard]] network_type& network() noexcept { return *network_; }
    [[nodiscard]] dns_type& dns() noexcept { return *dns_; }

    [[nodiscard]] backend_state state() const;
    [[nodiscard]] bool abort_requested() const;

private:
    friend class backend_test_access;

    void request_abort_unchecked() noexcept;
    template<typename Checkpoint>
    [[nodiscard]] seastar::future<> start_with(Checkpoint checkpoint);
    [[nodiscard]] seastar::future<> stop_once();
    [[nodiscard]] seastar::future<> rollback_start() noexcept;

    seastar::abort_source* parent_abort_;
    std::optional<seastar::net::dns_resolver::options> dns_options_;
    seastar::optimized_optional<seastar::abort_source::subscription>
      parent_subscription_;
    runtime_lifetime lifetime_;
    std::optional<random_type> random_;
    std::optional<timer_type> timer_;
    std::optional<file_system_type> file_system_;
    std::optional<network_type> network_;
    std::optional<dns_type> dns_;
    std::optional<seastar::shared_promise<>> stop_done_;
    backend_state state_{backend_state::constructed};
    bool abort_requested_{false};
};

using backend_owner = sharded_service<backend>;

[[nodiscard]] inline auto
backend_parameter(sharded_service<runtime_service>& runtimes) {
    return runtimes.local_parameter([](runtime_service& runtime) {
        return backend_dependencies{runtime.tasks().abort_source()};
    });
}

[[nodiscard]] inline seastar::future<> start_backends(
  backend_owner& backends, sharded_service<runtime_service>& runtimes) {
    return backends.start(backend_parameter(runtimes));
}

static_assert(kwaque::runtime::runtime_backend<backend>);

template<typename Checkpoint>
seastar::future<> backend::start_with(Checkpoint checkpoint) {
    assert_current();
    if (state_ != backend_state::constructed) {
        throw std::logic_error("production backend cannot be started");
    }
    if (parent_abort_ == nullptr) {
        throw std::logic_error("production backend dependencies are not ready");
    }

    state_ = backend_state::starting;
    if (parent_abort_->abort_requested()) {
        request_abort_unchecked();
    } else {
        parent_subscription_ = parent_abort_->subscribe(
          [this] noexcept { request_abort_unchecked(); });
        if (!parent_subscription_ && parent_abort_->abort_requested()) {
            request_abort_unchecked();
        }
    }

    std::exception_ptr failure;
    try {
        if (abort_requested_) {
            throw seastar::abort_requested_exception{};
        }
        checkpoint(0);
        auto source = random_type::make();
        if (!source) {
            throw std::system_error(make_error_code(source.error().code()));
        }
        random_.emplace(std::move(*source));
        checkpoint(1);
        timer_.emplace();
        checkpoint(2);
        file_system_.emplace();
        checkpoint(3);
        network_.emplace();
        checkpoint(4);
        if (dns_options_) {
            dns_.emplace(dns_config{}, *dns_options_);
        } else {
            dns_.emplace();
        }
        dns_options_.reset();
        if (abort_requested_) {
            throw seastar::abort_requested_exception{};
        }
    } catch (...) {
        failure = std::current_exception();
    }

    if (failure) {
        co_await rollback_start();
        state_ = backend_state::stopped;
        std::rethrow_exception(failure);
    }
    state_ = backend_state::started;
}

} // namespace kwaque::runtime::production

#endif // KWAQUE_SRC_RUNTIME_PRODUCTION_BACKEND_H_
