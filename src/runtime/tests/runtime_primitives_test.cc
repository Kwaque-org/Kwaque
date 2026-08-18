#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <atomic>
#include <exception>
#include <functional>

namespace {

class lifecycle_probe final {
public:
    lifecycle_probe(std::atomic<unsigned>& constructions, std::atomic<unsigned>& stops)
      : _constructions(constructions)
      , _stops(stops) {
        _constructions.fetch_add(1, std::memory_order_relaxed);
    }

    seastar::future<> stop() {
        _stops.fetch_add(1, std::memory_order_relaxed);
        return seastar::make_ready_future<>();
    }

private:
    std::atomic<unsigned>& _constructions;
    std::atomic<unsigned>& _stops;
};

} // namespace

SEASTAR_TEST_CASE(sharded_service_constructs_and_stops_on_every_shard) {
    const auto shard_count = seastar::this_smp_shard_count();
    std::atomic<unsigned> constructions{0};
    std::atomic<unsigned> stops{0};
    seastar::sharded<lifecycle_probe> service;

    co_await service.start(std::ref(constructions), std::ref(stops));
    std::exception_ptr failure;
    try {
        BOOST_REQUIRE_EQUAL(constructions.load(std::memory_order_relaxed), shard_count);
    } catch (...) {
        failure = std::current_exception();
    }

    co_await service.stop();
    if (failure) {
        std::rethrow_exception(failure);
    }
    BOOST_REQUIRE_EQUAL(stops.load(std::memory_order_relaxed), shard_count);
}

SEASTAR_TEST_CASE(scheduling_group_can_own_reactor_work) {
    auto group = co_await seastar::create_scheduling_group("kwaque-runtime-probe", 10.0F);
    std::exception_ptr failure;

    try {
        co_await seastar::with_scheduling_group(group, [group] {
            BOOST_REQUIRE(seastar::current_scheduling_group() == group);
        });
    } catch (...) {
        failure = std::current_exception();
    }

    co_await seastar::destroy_scheduling_group(group);
    if (failure) {
        std::rethrow_exception(failure);
    }
}
