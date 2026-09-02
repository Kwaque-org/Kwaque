#include "src/runtime/shard_affinity.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_api.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <boost/test/unit_test.hpp>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace {

constexpr const char* group_name{"kwaque_metric_lifecycle"};

bool registered(const char* metric_name) {
    auto full_name = seastar::sstring{group_name};
    full_name += seastar::sstring{"_"};
    full_name += seastar::sstring{metric_name};
    return seastar::metrics::impl::get_value_map().contains(full_name);
}

class owner_local_probe final : public kwaque::runtime::shard_affine {
public:
    void start(bool force_partial_duplicate = false) {
        assert_current();
        if (metrics_) {
            throw std::logic_error("probe metrics are already registered");
        }
        namespace metrics = seastar::metrics;
        try {
            metrics_.emplace();
            if (force_partial_duplicate) {
                metrics_->add_group(
                  seastar::sstring{group_name},
                  {metrics::make_gauge(
                    "first",
                    [this] { return value_; },
                    metrics::description("First lifecycle test value"))});
                metrics_->add_group(
                  seastar::sstring{group_name},
                  {metrics::make_gauge(
                    "duplicate",
                    [this] { return value_; },
                    metrics::description("Duplicate lifecycle test value"))});
            } else {
                metrics_->add_group(
                  seastar::sstring{group_name},
                  {metrics::make_gauge(
                    "value",
                    [this] { return value_; },
                    metrics::description("Lifecycle test value"))});
            }
        } catch (...) {
            metrics_.reset();
            throw;
        }
    }

    void stop() noexcept {
        assert_current();
        metrics_.reset();
    }

private:
    std::uint64_t value_{0};
    std::optional<seastar::metrics::metric_groups> metrics_;
};

static_assert(!std::is_copy_constructible_v<owner_local_probe>);
static_assert(!std::is_copy_assignable_v<owner_local_probe>);
static_assert(!std::is_move_constructible_v<owner_local_probe>);
static_assert(!std::is_move_assignable_v<owner_local_probe>);
static_assert(std::is_nothrow_destructible_v<seastar::metrics::metric_groups>);
static_assert(noexcept(
  std::declval<std::optional<seastar::metrics::metric_groups>&>().reset()));

} // namespace

SEASTAR_TEST_CASE(
  native_metric_group_destruction_unregisters_without_allocating) {
    for (std::size_t cycle = 0; cycle < 16; ++cycle) {
        owner_local_probe probe;
        probe.start();
        BOOST_CHECK(registered("value"));
        BOOST_CHECK_THROW(probe.start(), std::logic_error);

        std::size_t attempts = 0;
        seastar::memory::with_allocation_failures([&] {
            ++attempts;
            probe.stop();
        });
        BOOST_CHECK(attempts == 1U);
        BOOST_CHECK(!registered("value"));
        probe.stop();
    }

    {
        owner_local_probe probe;
        probe.start();
        BOOST_CHECK(registered("value"));
    }
    BOOST_CHECK(!registered("value"));
    co_return;
}

SEASTAR_TEST_CASE(
  partial_native_registration_failure_rolls_back_every_callback) {
    namespace metrics = seastar::metrics;
    std::optional<metrics::metric_groups> blocker;
    blocker.emplace();
    blocker->add_group(
      seastar::sstring{group_name},
      {metrics::make_gauge(
        "duplicate",
        [] { return 0U; },
        metrics::description("Duplicate registration blocker"))});
    BOOST_CHECK(registered("duplicate"));

    owner_local_probe probe;
    BOOST_CHECK_THROW(probe.start(true), metrics::double_registration);
    BOOST_CHECK(!registered("first"));
    BOOST_CHECK(registered("duplicate"));

    std::size_t attempts = 0;
    seastar::memory::with_allocation_failures([&] {
        ++attempts;
        blocker.reset();
    });
    BOOST_CHECK(attempts == 1U);
    BOOST_CHECK(!registered("duplicate"));
    probe.start();
    BOOST_CHECK(registered("value"));
    probe.stop();
    BOOST_CHECK(!registered("value"));
    co_return;
}
