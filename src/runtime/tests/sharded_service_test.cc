#include "src/runtime/sharded_service.h"
#include "src/runtime/task_scope.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

namespace {

constexpr unsigned no_failed_shard = std::numeric_limits<unsigned>::max();

struct lifecycle_counters final {
    explicit lifecycle_counters(std::size_t shard_count)
      : starts(shard_count)
      , aborts(shard_count)
      , stops(shard_count)
      , completions(shard_count)
      , destructions(shard_count) {}

    std::vector<std::atomic<unsigned>> starts;
    std::vector<std::atomic<unsigned>> aborts;
    std::vector<std::atomic<unsigned>> stops;
    std::vector<std::atomic<unsigned>> completions;
    std::vector<std::atomic<unsigned>> destructions;
};

class startup_probe final : public kwaque::runtime::shard_affine {
public:
    startup_probe(
      std::reference_wrapper<lifecycle_counters> counters,
      unsigned failed_shard) noexcept
      : counters_(counters.get())
      , failed_shard_(failed_shard) {}

    seastar::future<> start() {
        assert_current();
        counters_.starts[owner().value()].fetch_add(
          1, std::memory_order_relaxed);
        if (owner().value() == failed_shard_) {
            throw std::runtime_error("injected local startup failure");
        }
        return seastar::make_ready_future<>();
    }

    void request_abort() {
        assert_current();
        if (!aborted_) {
            aborted_ = true;
            counters_.aborts[owner().value()].fetch_add(
              1, std::memory_order_relaxed);
        }
    }

    seastar::future<> stop() {
        assert_current();
        counters_.stops[owner().value()].fetch_add(
          1, std::memory_order_relaxed);
        return seastar::make_ready_future<>();
    }

    void record_fanout() {
        assert_current();
        counters_.completions[owner().value()].fetch_add(
          1, std::memory_order_relaxed);
        if (owner().value() == 0) {
            throw std::runtime_error("injected void fan-out failure");
        }
    }

    kwaque::runtime::owner_shard record_value_fanout() {
        assert_current();
        counters_.completions[owner().value()].fetch_add(
          1, std::memory_order_relaxed);
        if (owner().value() == 0) {
            throw std::runtime_error("injected value fan-out failure");
        }
        return owner();
    }

private:
    lifecycle_counters& counters_;
    unsigned failed_shard_;
    bool aborted_{false};
};

class delayed_completion final {
public:
    delayed_completion(
      lifecycle_counters& counters, seastar::shard_id shard) noexcept
      : counters_(counters)
      , shard_(shard) {}

    seastar::future<> operator()() {
        using namespace std::chrono_literals;
        co_await seastar::sleep(20ms);
        counters_.completions[shard_].fetch_add(1, std::memory_order_relaxed);
    }

private:
    lifecycle_counters& counters_;
    seastar::shard_id shard_;
};

class delayed_service final : public kwaque::runtime::shard_affine {
public:
    explicit delayed_service(
      std::reference_wrapper<lifecycle_counters> counters) noexcept
      : counters_(counters.get()) {}

    ~delayed_service() {
        counters_.destructions[owner().value()].fetch_add(
          1, std::memory_order_relaxed);
    }

    seastar::future<> start() {
        assert_current();
        const auto spawned = tasks_.spawn(
          delayed_completion{counters_, owner().value()});
        if (!spawned) {
            throw std::logic_error("delayed task was rejected");
        }
        counters_.starts[owner().value()].fetch_add(
          1, std::memory_order_relaxed);
        return seastar::make_ready_future<>();
    }

    void request_abort() {
        assert_current();
        tasks_.request_abort();
    }

    seastar::future<> stop() {
        assert_current();
        co_await tasks_.close();
        counters_.stops[owner().value()].fetch_add(
          1, std::memory_order_relaxed);
    }

private:
    lifecycle_counters& counters_;
    kwaque::runtime::task_scope tasks_;
};

class failing_stop_service final : public kwaque::runtime::shard_affine {
public:
    failing_stop_service(
      std::reference_wrapper<lifecycle_counters> counters,
      unsigned failed_shard) noexcept
      : counters_(counters.get())
      , failed_shard_(failed_shard) {}

    ~failing_stop_service() {
        counters_.destructions[owner().value()].fetch_add(
          1, std::memory_order_relaxed);
    }

    seastar::future<> start() {
        assert_current();
        counters_.starts[owner().value()].fetch_add(
          1, std::memory_order_relaxed);
        return seastar::make_ready_future<>();
    }

    void request_abort() {
        assert_current();
        if (!aborted_) {
            aborted_ = true;
            counters_.aborts[owner().value()].fetch_add(
              1, std::memory_order_relaxed);
        }
    }

    seastar::future<> stop() {
        assert_current();
        counters_.stops[owner().value()].fetch_add(
          1, std::memory_order_relaxed);
        if (owner().value() == failed_shard_) {
            throw std::runtime_error("injected local stop failure");
        }
        return seastar::make_ready_future<>();
    }

private:
    lifecycle_counters& counters_;
    unsigned failed_shard_;
    bool aborted_{false};
};

unsigned load(const std::atomic<unsigned>& value) {
    return value.load(std::memory_order_relaxed);
}

} // namespace

SEASTAR_TEST_CASE(sharded_service_rolls_back_only_successful_local_starts) {
    const auto shard_count = seastar::this_smp_shard_count();

    for (unsigned failed_shard = 0; failed_shard < shard_count;
         ++failed_shard) {
        lifecycle_counters counters{shard_count};
        kwaque::runtime::sharded_service<startup_probe> services{
          seastar::default_smp_service_group()};

        bool failed = false;
        try {
            co_await services.start(std::ref(counters), failed_shard);
        } catch (const std::runtime_error&) {
            failed = true;
        }
        BOOST_REQUIRE(failed);
        BOOST_CHECK(
          services.state() == kwaque::runtime::sharded_service_state::stopped);

        for (unsigned shard = 0; shard < shard_count; ++shard) {
            BOOST_CHECK_EQUAL(
              load(counters.starts[shard]), shard <= failed_shard);
            BOOST_CHECK_EQUAL(
              load(counters.stops[shard]), shard < failed_shard);
            BOOST_CHECK_EQUAL(
              load(counters.aborts[shard]), shard < failed_shard);
        }
        co_await services.stop();
    }
}

SEASTAR_TEST_CASE(sharded_service_enforces_state_and_returns_owned_identities) {
    const auto shard_count = seastar::this_smp_shard_count();
    lifecycle_counters counters{shard_count};
    kwaque::runtime::sharded_service<startup_probe> services{
      seastar::default_smp_service_group()};
    BOOST_CHECK(
      services.state() == kwaque::runtime::sharded_service_state::constructed);

    co_await services.start(std::ref(counters), no_failed_shard);
    BOOST_CHECK(
      services.state() == kwaque::runtime::sharded_service_state::started);

    const auto owners = co_await services.invoke_on_all(
      [](startup_probe& service) { return service.owner(); });
    BOOST_REQUIRE_EQUAL(owners.size(), shard_count);
    for (std::size_t shard = 0; shard < owners.size(); ++shard) {
        BOOST_CHECK_EQUAL(owners[shard].value(), shard);
    }

    const auto owner = co_await services.invoke_on_owner(
      owners.back(), [](startup_probe& service) { return service.owner(); });
    BOOST_CHECK(owner == owners.back());

    bool rejected = false;
    try {
        co_await services.start(std::ref(counters), no_failed_shard);
    } catch (const std::logic_error&) {
        rejected = true;
    }
    BOOST_REQUIRE(rejected);

    auto aborting = services.request_abort();
    auto stopping = services.stop();
    co_await std::move(aborting);
    co_await std::move(stopping);
    co_await services.stop();
    BOOST_CHECK(
      services.state() == kwaque::runtime::sharded_service_state::stopped);

    rejected = false;
    try {
        co_await services.invoke_on_all(
          [](startup_probe& service) { return service.owner(); });
    } catch (const std::logic_error&) {
        rejected = true;
    }
    BOOST_REQUIRE(rejected);
}

SEASTAR_TEST_CASE(sharded_service_fanout_waits_for_all_shards_before_failing) {
    const auto shard_count = seastar::this_smp_shard_count();
    lifecycle_counters counters{shard_count};
    kwaque::runtime::sharded_service<startup_probe> services{
      seastar::default_smp_service_group()};
    co_await services.start(std::ref(counters), no_failed_shard);

    bool failed = false;
    try {
        co_await services.invoke_on_all(
          [](startup_probe& service) { service.record_fanout(); });
    } catch (const std::runtime_error&) {
        failed = true;
    }
    BOOST_REQUIRE(failed);
    for (unsigned shard = 0; shard < shard_count; ++shard) {
        BOOST_CHECK_EQUAL(load(counters.completions[shard]), 1U);
        counters.completions[shard].store(0, std::memory_order_relaxed);
    }

    failed = false;
    try {
        co_await services.invoke_on_all(
          [](startup_probe& service) { return service.record_value_fanout(); });
    } catch (const std::runtime_error&) {
        failed = true;
    }
    BOOST_REQUIRE(failed);
    for (unsigned shard = 0; shard < shard_count; ++shard) {
        BOOST_CHECK_EQUAL(load(counters.completions[shard]), 1U);
    }

    co_await services.stop();
}

SEASTAR_TEST_CASE(sharded_service_destroys_every_shard_after_stop_failure) {
    const auto shard_count = seastar::this_smp_shard_count();
    lifecycle_counters counters{shard_count};
    const auto failed_shard = shard_count - 1;
    kwaque::runtime::sharded_service<failing_stop_service> services{
      seastar::default_smp_service_group()};
    co_await services.start(std::ref(counters), failed_shard);

    for (unsigned attempt = 0; attempt < 2; ++attempt) {
        bool failed = false;
        try {
            co_await services.stop();
        } catch (const std::runtime_error&) {
            failed = true;
        }
        BOOST_REQUIRE(failed);
    }

    BOOST_CHECK(
      services.state() == kwaque::runtime::sharded_service_state::stopped);
    for (unsigned shard = 0; shard < shard_count; ++shard) {
        BOOST_CHECK_EQUAL(load(counters.starts[shard]), 1U);
        BOOST_CHECK_GE(load(counters.aborts[shard]), 1U);
        BOOST_CHECK_EQUAL(load(counters.stops[shard]), 1U);
        BOOST_CHECK_EQUAL(load(counters.destructions[shard]), 1U);
    }
}

SEASTAR_TEST_CASE(sharded_service_waits_for_delayed_work_before_destruction) {
    const auto shard_count = seastar::this_smp_shard_count();
    lifecycle_counters counters{shard_count};
    constexpr unsigned cycles = 3;

    for (unsigned cycle = 1; cycle <= cycles; ++cycle) {
        kwaque::runtime::sharded_service<delayed_service> services{
          seastar::default_smp_service_group()};
        co_await services.start(std::ref(counters));

        auto stopping = services.stop();
        BOOST_CHECK(!stopping.available());
        co_await std::move(stopping);

        for (unsigned shard = 0; shard < shard_count; ++shard) {
            BOOST_CHECK_EQUAL(load(counters.completions[shard]), cycle);
            BOOST_CHECK_EQUAL(load(counters.stops[shard]), cycle);
            BOOST_CHECK_EQUAL(load(counters.destructions[shard]), cycle);
        }
    }
}
