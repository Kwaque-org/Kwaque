#pragma once

#include "src/base/invariant.h"
#include "src/runtime/cross_shard.h"
#include "src/runtime/shard_affinity.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/when_all.hh>

#include <concepts>
#include <exception>
#include <functional>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace kwaque::runtime {

enum class sharded_service_state {
    constructed,
    started,
    stopping,
    stopped,
};

template<typename Service>
concept sharded_service_lifecycle = requires(Service& service) {
    { service.start() } -> std::same_as<seastar::future<>>;
    { service.request_abort() } -> std::same_as<void>;
    { service.stop() } -> std::same_as<seastar::future<>>;
};

// Process-scoped owner for one mutable Service instance per shard. Public
// lifecycle methods run on the constructing shard, and the supplied service
// group must outlive all calls. Service::request_abort() must be idempotent,
// and Service::stop() must drain its local task scope. Destruction is valid
// only before start or after stop has completed.
template<sharded_service_lifecycle Service>
class sharded_service final : public shard_affine {
private:
    class local_service final : public shard_affine {
    public:
        template<typename... Args>
        explicit local_service(Args&&... args)
          : service_(std::forward<Args>(args)...) {}

        ~local_service() {
            KWAQUE_INVARIANT(
              invariant_id{"KQ-SHARDED-LOCAL-STOPPED"},
              !started_,
              "local service destroyed while started");
        }

        seastar::future<> start() {
            assert_current();
            if (started_) {
                throw std::logic_error("local service is already started");
            }
            co_await service_.start();
            started_ = true;
        }

        void request_abort() {
            assert_current();
            if (started_) {
                service_.request_abort();
            }
        }

        seastar::future<> stop() {
            assert_current();
            if (!started_) {
                co_return;
            }
            service_.request_abort();
            std::exception_ptr failure;
            try {
                co_await service_.stop();
            } catch (...) {
                failure = std::current_exception();
            }
            started_ = false;
            if (failure) {
                std::rethrow_exception(failure);
            }
        }

        [[nodiscard]] Service& service() {
            assert_current();
            if (!started_) {
                throw std::logic_error("local service is not started");
            }
            return service_;
        }

    private:
        Service service_;
        bool started_{false};
    };

    template<typename Func, typename... Args>
    using invocation_result =
      typename detail::future_result<seastar::futurize_t<
        std::invoke_result_t<Func&, Service&, Args...>>>::type;

    template<typename Func, typename... Args>
    static consteval bool is_valid_invocation() {
        if constexpr (
          !detail::is_cross_shard_callable<Func>
          || !(cross_shard_value<Args> && ...)
          || !std::invocable<Func&, Service&, Args...>) {
            return false;
        } else {
            return detail::cross_shard_result_value<
              invocation_result<Func, Args...>>;
        }
    }

public:
    explicit sharded_service(seastar::smp_service_group service_group) noexcept
      : service_group_(service_group) {}

    ~sharded_service() {
        KWAQUE_INVARIANT(
          invariant_id{"KQ-SHARDED-OWNER-STOPPED"},
          state_ == sharded_service_state::constructed
            || (state_ == sharded_service_state::stopped && !container_started_),
          "sharded service owner destroyed while active");
    }

    // Constructor arguments are copied by the underlying shard container. Keep
    // them immutable and independently valid on every shard.
    template<typename... Args>
    requires(std::copy_constructible<Args> && ...)
    [[nodiscard]] seastar::future<> start(Args... args) {
        assert_current();
        if (state_ != sharded_service_state::constructed || operation_active_) {
            throw std::logic_error("sharded service cannot be started");
        }
        operation_active_ = true;

        std::exception_ptr startup_failure;
        try {
            co_await services_.start(std::move(args)...);
            container_started_ = true;
            for (const auto shard : seastar::this_smp_all_shards()) {
                co_await services_.invoke_on(
                  shard,
                  seastar::smp_submit_to_options{service_group_},
                  [](local_service& local) { return local.start(); });
            }
        } catch (...) {
            startup_failure = std::current_exception();
        }

        if (startup_failure) {
            if (container_started_) {
                try {
                    co_await request_abort_all();
                } catch (...) {
                }
                try {
                    co_await services_.stop();
                } catch (...) {
                }
                container_started_ = false;
            }
            state_ = sharded_service_state::stopped;
            operation_active_ = false;
            std::rethrow_exception(startup_failure);
        }

        state_ = sharded_service_state::started;
        operation_active_ = false;
    }

    [[nodiscard]] seastar::future<> request_abort() {
        assert_current();
        if (state_ != sharded_service_state::started) {
            co_return;
        }
        auto holder = invocations_.hold();
        co_await request_abort_all();
    }

    [[nodiscard]] seastar::future<> stop() {
        assert_current();
        if (state_ == sharded_service_state::stopping) {
            return stop_done_.get_shared_future();
        }
        if (state_ == sharded_service_state::stopped) {
            return stop_done_.available() ? stop_done_.get_shared_future()
                                          : seastar::make_ready_future<>();
        }
        if (operation_active_) {
            return seastar::make_exception_future<>(
              std::logic_error("sharded service operation is in progress"));
        }
        if (state_ == sharded_service_state::constructed) {
            state_ = sharded_service_state::stopped;
            return seastar::make_ready_future<>();
        }

        state_ = sharded_service_state::stopping;
        auto completion = stop_once().then_wrapped(
          [this](seastar::future<> stopped) noexcept {
              state_ = sharded_service_state::stopped;
              container_started_ = false;
              try {
                  stopped.get();
                  stop_done_.set_value();
              } catch (...) {
                  stop_done_.set_exception(std::current_exception());
              }
          });
        static_cast<void>(completion);
        return stop_done_.get_shared_future();
    }

    [[nodiscard]] sharded_service_state state() const {
        assert_current();
        return state_;
    }

    template<typename Selector>
    requires std::copy_constructible<Selector>
             && std::invocable<const Selector&, Service&>
    [[nodiscard]] auto local_parameter(Selector selector) {
        assert_parameter_source();
        return seastar::sharded_parameter(
          [selector = std::move(selector)](
            local_service& local) -> decltype(auto) {
              return std::invoke(selector, local.service());
          },
          std::ref(services_));
    }

    template<typename Func, typename... Args>
    requires(is_valid_invocation<Func, Args...>())
    [[nodiscard]] auto
    invoke_on_owner(owner_shard target, Func function, Args... args)
      -> seastar::future<invocation_result<Func, Args...>> {
        assert_invocable();
        auto holder = invocations_.hold();
        if constexpr (std::same_as<invocation_result<Func, Args...>, void>) {
            co_await invoke_one(
              target.value(), std::move(function), std::move(args)...);
        } else {
            co_return co_await invoke_one(
              target.value(), std::move(function), std::move(args)...);
        }
    }

    // Fan-out calls are submitted to every service instance before any result
    // is awaited. Completion waits for every call; if any fail, one failure is
    // propagated and value results are returned in shard order.
    template<typename Func, typename... Args>
    requires(
      is_valid_invocation<Func, Args...>()
      && std::same_as<invocation_result<Func, Args...>, void>
      && std::copy_constructible<Func>
      && (std::copy_constructible<Args> && ...))
    [[nodiscard]] seastar::future<> invoke_on_all(Func function, Args... args) {
        assert_invocable();
        auto holder = invocations_.hold();
        std::vector<seastar::future<>> invocations;
        invocations.reserve(seastar::this_smp_shard_count());
        for (const auto shard : seastar::this_smp_all_shards()) {
            invocations.push_back(seastar::futurize_invoke([&] {
                return invoke_one(shard, function, args...);
            }));
        }
        co_await seastar::when_all_succeed(std::move(invocations));
    }

    template<typename Func, typename... Args>
    requires(
      is_valid_invocation<Func, Args...>()
      && cross_shard_value<invocation_result<Func, Args...>>
      && std::copy_constructible<Func>
      && (std::copy_constructible<Args> && ...))
    [[nodiscard]] auto invoke_on_all(Func function, Args... args)
      -> seastar::future<std::vector<invocation_result<Func, Args...>>> {
        assert_invocable();
        auto holder = invocations_.hold();
        using result_type = invocation_result<Func, Args...>;
        std::vector<seastar::future<result_type>> invocations;
        invocations.reserve(seastar::this_smp_shard_count());
        for (const auto shard : seastar::this_smp_all_shards()) {
            invocations.push_back(seastar::futurize_invoke([&] {
                return invoke_one(shard, function, args...);
            }));
        }
        co_return co_await seastar::when_all_succeed(std::move(invocations));
    }

private:
    void assert_parameter_source() const {
        assert_current();
        if (state_ != sharded_service_state::started || operation_active_) {
            throw std::logic_error(
              "sharded service cannot provide local constructor parameters");
        }
    }

    void assert_invocable() const {
        assert_current();
        if (state_ != sharded_service_state::started || operation_active_) {
            throw std::logic_error("sharded service is not available");
        }
    }

    template<typename Func, typename... Args>
    static auto
    invoke_local_owned(local_service& local, Func function, Args... args)
      -> seastar::future<invocation_result<Func, Args...>> {
        if constexpr (std::same_as<invocation_result<Func, Args...>, void>) {
            co_await seastar::futurize_invoke(
              function, local.service(), std::move(args)...);
        } else {
            co_return co_await seastar::futurize_invoke(
              function, local.service(), std::move(args)...);
        }
    }

    template<typename Func, typename... Args>
    auto invoke_one(seastar::shard_id target, Func function, Args... args)
      -> seastar::future<invocation_result<Func, Args...>> {
        return services_.invoke_on(
          target,
          seastar::smp_submit_to_options{service_group_},
          [function = std::move(function),
           arguments = std::tuple<Args...>{std::move(args)...}](
            local_service& local) mutable {
              return std::apply(
                [&local, &function](Args&... values) {
                    return invoke_local_owned(
                      local, std::move(function), std::move(values)...);
                },
                arguments);
          });
    }

    [[nodiscard]] seastar::future<> request_abort_all() {
        if (!container_started_) {
            return seastar::make_ready_future<>();
        }
        return services_.invoke_on_all(
          seastar::smp_submit_to_options{service_group_},
          [](local_service& local) { local.request_abort(); });
    }

    [[nodiscard]] seastar::future<> stop_once() {
        std::exception_ptr failure;
        try {
            co_await request_abort_all();
        } catch (...) {
            failure = std::current_exception();
        }
        try {
            if (!invocations_.is_closed()) {
                co_await invocations_.close();
            }
        } catch (...) {
            if (!failure) {
                failure = std::current_exception();
            }
        }
        try {
            co_await services_.stop();
        } catch (...) {
            if (!failure) {
                failure = std::current_exception();
            }
        }
        container_started_ = false;
        if (failure) {
            std::rethrow_exception(failure);
        }
    }

    seastar::smp_service_group service_group_;
    seastar::sharded<local_service> services_;
    seastar::gate invocations_;
    seastar::shared_promise<> stop_done_;
    sharded_service_state state_{sharded_service_state::constructed};
    bool operation_active_{false};
    bool container_started_{false};
};

} // namespace kwaque::runtime
