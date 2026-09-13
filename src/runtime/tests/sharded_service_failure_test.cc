#include "src/runtime/sharded_service.h"
#include "src/runtime/task_scope.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <atomic>
#include <cstddef>
#include <exception>
#include <functional>
#include <limits>
#include <new>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace {

struct constructor_failure final : std::exception {};

struct construction_state {
    std::atomic<unsigned> copies{0};
    std::atomic<unsigned> fail_at{std::numeric_limits<unsigned>::max()};
    std::atomic<unsigned> constructed{0};
    std::atomic<unsigned> destroyed{0};
    std::atomic<unsigned> started{0};
    std::atomic<unsigned> stopped{0};
};

// Observable copy failures cover parameter storage and per-shard constructor
// copies. Callback moves inside native submission must remain nonthrowing.
struct construction_argument {
    construction_state& state;
    explicit construction_argument(construction_state& state)
      : state(state) {}
    construction_argument(const construction_argument& other)
      : state(other.state) {
        if (++state.copies == state.fail_at.load()) {
            throw constructor_failure{};
        }
    }
    construction_argument(construction_argument&& other) noexcept
      : state(other.state) {}
};

class construction_service {
public:
    explicit construction_service(construction_argument argument)
      : state_(argument.state) {
        ++state_.constructed;
    }
    ~construction_service() { ++state_.destroyed; }
    seastar::future<> start() {
        ++state_.started;
        return seastar::make_ready_future<>();
    }
    void request_abort() {}
    seastar::future<> stop() {
        ++state_.stopped;
        return seastar::make_ready_future<>();
    }

private:
    construction_state& state_;
};

struct throwing_move_argument {
    throwing_move_argument() = default;
    throwing_move_argument(const throwing_move_argument&) = default;
    throwing_move_argument(throwing_move_argument&&) noexcept(false) {}
};

template<typename Argument>
concept accepted_start_argument = requires(
  kwaque::runtime::sharded_service<construction_service>& service,
  Argument argument) { service.start(std::move(argument)); };

static_assert(!accepted_start_argument<throwing_move_argument>);
static_assert(accepted_start_argument<construction_argument>);

struct abort_state {
    std::atomic<unsigned> completed{0};
    std::atomic<unsigned> stopped{0};
    std::atomic<unsigned> destroyed{0};
    unsigned failed_shard;
};

class abort_service final : public kwaque::runtime::shard_affine {
public:
    explicit abort_service(std::reference_wrapper<abort_state> state)
      : state_(state) {}
    ~abort_service() { ++state_.get().destroyed; }
    seastar::future<> start() {
        tasks_.emplace();
        std::exception_ptr failure;
        try {
            const auto accepted = tasks_->spawn([this] -> seastar::future<> {
                co_await release_.get_future();
                ++state_.get().completed;
            });
            if (!accepted) {
                throw std::logic_error("task admission failed");
            }
        } catch (...) {
            failure = std::current_exception();
        }
        if (failure) {
            co_await tasks_->close();
            tasks_.reset();
            std::rethrow_exception(failure);
        }
    }
    void request_abort() {
        if (owner().value() == state_.get().failed_shard) {
            throw std::runtime_error("abort failed");
        }
        tasks_->request_abort();
    }
    seastar::future<> stop() {
        release_.set_value();
        co_await tasks_->close();
        tasks_.reset();
        ++state_.get().stopped;
        if (owner().value() == state_.get().failed_shard) {
            throw std::logic_error("later stop failed");
        }
    }

private:
    std::reference_wrapper<abort_state> state_;
    std::optional<kwaque::runtime::task_scope> tasks_;
    seastar::promise<> release_;
};

thread_local seastar::promise<>* pending_invocation = nullptr;

class invocation_service final : public kwaque::runtime::shard_affine {
public:
    explicit invocation_service(
      std::reference_wrapper<std::atomic<unsigned>> destroyed)
      : destroyed_(destroyed) {}
    ~invocation_service() { ++destroyed_.get(); }
    seastar::future<> start() { return seastar::make_ready_future<>(); }
    void request_abort() {}
    seastar::future<> stop() { return seastar::make_ready_future<>(); }
    seastar::future<> wait() {
        if (pending_invocation != nullptr) {
            throw std::logic_error("concurrent invocation on one shard");
        }
        pending_invocation = &release_;
        co_await release_.get_future();
        pending_invocation = nullptr;
    }

private:
    std::reference_wrapper<std::atomic<unsigned>> destroyed_;
    seastar::promise<> release_;
};

seastar::future<> verify_invocation_drain(unsigned target, bool fanout) {
    std::atomic<unsigned> destroyed{0};
    kwaque::runtime::sharded_service<invocation_service> services{
      seastar::default_smp_service_group()};
    co_await services.start(std::ref(destroyed));
    const auto owner = co_await seastar::smp::submit_to(
      target, [] { return kwaque::runtime::owner_shard{}; });
    auto invoking = fanout
                      ? services.invoke_on_all([](invocation_service& service) {
                            return service.wait();
                        })
                      : services
                          .invoke_on_owner(
                            owner,
                            [](
                              invocation_service& service,
                              const kwaque::runtime::owner_shard& expected)
                              -> seastar::future<kwaque::runtime::owner_shard> {
                                co_await service.wait();
                                co_return expected;
                            },
                            owner)
                          .discard_result();
    // The following message is ordered after submission to the same target.
    const bool entered = co_await seastar::smp::submit_to(
      target, [] { return pending_invocation != nullptr; });
    BOOST_REQUIRE(entered);
    auto stopping = services.stop();
    co_await seastar::yield();
    BOOST_CHECK(!stopping.available());
    BOOST_CHECK_EQUAL(destroyed.load(), 0U);
    bool rejected = false;
    try {
        co_await services.invoke_on_all([](invocation_service&) {});
    } catch (const std::logic_error&) {
        rejected = true;
    }
    BOOST_CHECK(rejected);
    co_await seastar::smp::invoke_on_all([] {
        if (pending_invocation != nullptr) {
            pending_invocation->set_value();
        }
    });
    co_await std::move(invoking);
    co_await std::move(stopping);
    BOOST_CHECK_EQUAL(destroyed.load(), seastar::this_smp_shard_count());
}

} // namespace

SEASTAR_TEST_CASE(sharded_construction_cleans_every_argument_failure_boundary) {
    unsigned boundaries = 0;
    {
        construction_state state;
        construction_argument argument{state};
        kwaque::runtime::sharded_service<construction_service> services{
          seastar::default_smp_service_group()};
        co_await services.start(argument);
        boundaries = state.copies.load();
        co_await services.stop();
    }
    BOOST_REQUIRE_GT(boundaries, 0U);
    BOOST_REQUIRE_LE(boundaries, 128U);
    for (unsigned boundary = 1; boundary <= boundaries; ++boundary) {
        construction_state state;
        state.fail_at.store(boundary);
        construction_argument argument{state};
        kwaque::runtime::sharded_service<construction_service> services{
          seastar::default_smp_service_group()};
        bool failed = false;
        try {
            co_await services.start(argument);
        } catch (const constructor_failure&) {
            failed = true;
        }
        co_await services.stop();
        BOOST_CHECK(failed);
        BOOST_CHECK_GE(state.copies.load(), boundary);
        BOOST_CHECK_EQUAL(state.constructed.load(), state.destroyed.load());
        BOOST_CHECK_EQUAL(state.started.load(), state.stopped.load());
    }
}

SEASTAR_TEST_CASE(
  sharded_abort_failure_still_drains_and_preserves_first_error) {
    for (unsigned shard = 0; shard < seastar::this_smp_shard_count(); ++shard) {
        abort_state state{.failed_shard = shard};
        kwaque::runtime::sharded_service<abort_service> services{
          seastar::default_smp_service_group()};
        co_await services.start(std::ref(state));
        std::exception_ptr first;
        for (unsigned attempt = 0; attempt < 2; ++attempt) {
            try {
                co_await services.stop();
            } catch (const std::runtime_error& error) {
                BOOST_CHECK(std::string_view{error.what()} == "abort failed");
                if (first) {
                    BOOST_CHECK(first == std::current_exception());
                } else {
                    first = std::current_exception();
                }
            }
        }
        BOOST_CHECK(first != nullptr);
        BOOST_CHECK_EQUAL(
          state.completed.load(), seastar::this_smp_shard_count());
        BOOST_CHECK_EQUAL(
          state.stopped.load(), seastar::this_smp_shard_count());
        BOOST_CHECK_EQUAL(
          state.destroyed.load(), seastar::this_smp_shard_count());
    }
}

SEASTAR_TEST_CASE(sharded_stop_waits_for_local_remote_and_fanout_invocations) {
    BOOST_REQUIRE_GE(seastar::this_smp_shard_count(), 2U);
    co_await verify_invocation_drain(0, false);
    co_await verify_invocation_drain(1, false);
    co_await verify_invocation_drain(1, true);
}

SEASTAR_TEST_CASE(sharded_construction_cleans_native_allocation_failures) {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    unsigned injected_failures = 0;
    bool reached_success = false;
    for (std::size_t point = 0; point < 256; ++point) {
        construction_state state;
        construction_argument argument{state};
        kwaque::runtime::sharded_service<construction_service> services{
          seastar::default_smp_service_group()};
        auto& injector = seastar::memory::local_failure_injector();
        bool failed = false;
        injector.fail_after(point);
        try {
            co_await services.start(argument);
        } catch (const std::bad_alloc&) {
            failed = true;
        }
        const bool injected = injector.failed();
        injector.cancel();
        co_await services.stop();
        BOOST_CHECK_EQUAL(state.constructed.load(), state.destroyed.load());
        BOOST_CHECK_EQUAL(state.started.load(), state.stopped.load());
        if (injected) {
            ++injected_failures;
            BOOST_CHECK(failed);
        } else {
            BOOST_CHECK(!failed);
            reached_success = true;
            break;
        }
    }
    BOOST_CHECK_GT(injected_failures, 0U);
    BOOST_CHECK(reached_success);
#endif
    co_return;
}
